//! `spky jobs` — operator overview of the outbox job system, with kill/retry.
//!
//! Reads jobs straight from the configured SurrealDB (like `spky flag`): the
//! per-row data and aggregate metrics come from `SELECT`s against each job table.
//! Mutations (`kill`/`retry`) cannot be done with a plain `UPDATE` — job pickup is
//! gated on CREATE inside the SSP — so they call the `fn::job::kill` /
//! `fn::job::retry` SurrealQL functions, which proxy to the SSP/scheduler over
//! HTTP. Those functions are installed by the schema deploy
//! (`functions_remote_singlenode.surql`).
//!
//! Job tables are discovered from `sp00ky.yml` (`apps.*` of type `backend` with an
//! `outbox` method), falling back to `job`. The CLI deliberately does not depend on
//! the `job-runner` crate (it pins a different surrealdb-core major), so the small
//! discovery parse is replicated here on top of the CLI's own config types.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Stdout;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Wrap};
use ratatui::{Frame, Terminal};

use crate::backend::{self, AppType, DEFAULT_CONFIG_PATH};
use crate::surreal_client::{MigrationDB, SurrealClient};
use crate::{ConnectionArgs, JobsCommands};

/// The clap default for `--url`. When `conn.url` still equals this, the user did
/// not override it, so we resolve the endpoint from the project config instead.

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const CYAN: &str = "\x1b[36m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

/// All four terminal/non-terminal job statuses, in display order.
const STATUS_ORDER: [&str; 4] = ["pending", "processing", "success", "failed"];

pub fn run(
    conn: ConnectionArgs,
    config: Option<PathBuf>,
    action: Option<JobsCommands>,
) -> Result<()> {
    let client = client_from(&conn, &config)?;
    let tables = discover_job_tables(&config);

    match action {
        None => run_tui(client, tables),
        Some(JobsCommands::List {
            status,
            table,
            limit,
            json,
        }) => list(&client, &tables, status, table, limit, json),
        Some(JobsCommands::Get { id, json }) => get(&client, &tables, id, json),
        Some(JobsCommands::Kill { id }) => kill(&client, &id),
        Some(JobsCommands::Retry { id }) => retry(&client, &id),
        Some(JobsCommands::Clear { all }) => clear(&client, &tables, all),
    }
}

// =============================================================
// Connection + job-table discovery
// =============================================================

fn client_from(conn: &ConnectionArgs, config: &Option<PathBuf>) -> Result<SurrealClient> {
    // `--cloud` resolves the deployment's SurrealDB URL + login from Sp00ky
    // Cloud; otherwise the local URL/ns/db come from sp00ky.yml (the dev
    // stack maps SurrealDB to localhost:8666).
    let c = conn.resolve(config)?;
    Ok(c.client())
}

/// Map of job table name -> backend app name, parsed from `sp00ky.yml`. Any
/// `backend` app with an `outbox` method that names a `table` is a job table.
/// Falls back to a single `job` table if none are configured.
fn discover_job_tables(config: &Option<PathBuf>) -> BTreeMap<String, String> {
    let config_file = config
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let cfg = backend::load_config(&config_file);

    let mut out = BTreeMap::new();
    for (name, app) in &cfg.apps {
        if app.app_type != AppType::Backend {
            continue;
        }
        if let Some(method) = &app.method {
            if let Some(table) = &method.table {
                out.insert(table.clone(), name.clone());
            }
        }
    }
    if out.is_empty() {
        out.insert("job".to_string(), "job".to_string());
    }
    out
}

// =============================================================
// SurrealQL helpers
// =============================================================

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Pull the array of rows out of a single SurrealDB statement response.
fn result_rows(result: Option<&Value>) -> Vec<Value> {
    match result {
        Some(Value::Array(arr)) => arr.clone(),
        Some(other) => vec![other.clone()],
        None => vec![],
    }
}

/// Read a statement result as a count.
///
/// A count comes back either as a bare number or wrapped — `count()` under
/// `GROUP ALL` yields `{ "count": n }` on some query plans and a plain int on
/// others, and which one you get depends on whether an index covers the
/// predicate. Accept both rather than silently reading zero.
fn result_count(result: &Value) -> Option<usize> {
    let first = match result {
        Value::Array(arr) => arr.first()?,
        other => other,
    };
    match first {
        Value::Object(map) => map.get("count").and_then(Value::as_u64).map(|n| n as usize),
        other => other.as_u64().map(|n| n as usize),
    }
}

// =============================================================
// Data model
// =============================================================

#[derive(Clone)]
struct JobRow {
    id: String,
    table: String,
    backend: String,
    status: String,
    path: String,
    retries: i64,
    max_retries: i64,
    retry_strategy: String,
    created_at: String,
    updated_at: String,
    errors: Vec<Value>,
    payload: Value,
}

impl JobRow {
    fn last_error(&self) -> Option<String> {
        self.errors.last().map(|e| {
            let reason = e.get("reason").and_then(Value::as_str).unwrap_or("");
            let code = e
                .get("code")
                .map(|c| match c {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default();
            if code.is_empty() {
                reason.to_string()
            } else {
                format!("[{}] {}", code, reason)
            }
        })
    }

    fn age(&self) -> Option<String> {
        parse_ts(&self.created_at).map(fmt_age)
    }
}

#[derive(Default, Clone)]
struct StatusCounts {
    pending: i64,
    processing: i64,
    success: i64,
    failed: i64,
    other: i64,
}

impl StatusCounts {
    fn add(&mut self, status: &str, n: i64) {
        match status {
            "pending" => self.pending += n,
            "processing" => self.processing += n,
            "success" => self.success += n,
            "failed" => self.failed += n,
            _ => self.other += n,
        }
    }

    fn total(&self) -> i64 {
        self.pending + self.processing + self.success + self.failed + self.other
    }

    /// Failed as a fraction of terminal jobs (success + failed) in the counting
    /// window — the last hour, not all of history. See `fetch_table`.
    fn fail_rate(&self) -> f64 {
        let terminal = self.success + self.failed;
        if terminal == 0 {
            0.0
        } else {
            self.failed as f64 / terminal as f64 * 100.0
        }
    }
}

struct Snapshot {
    rows: Vec<JobRow>,
    counts: StatusCounts,
    throughput_1m: i64,
    oldest_pending: Option<DateTime<Utc>>,
    errors: Vec<String>,
}

// =============================================================
// Fetching
// =============================================================

/// Query one job table in a single round-trip (rows + status counts + 1-minute
/// success throughput + oldest pending timestamp).
///
/// The status counts cover in-flight work plus the last hour of terminal work, not
/// all of history. Two reasons: an all-time `GROUP BY status` is an unbounded
/// aggregate over the whole table on every 1-second TUI refresh, and once retention
/// keeps failures longer than successes an all-time ratio reports a fail rate that
/// climbs as successes age out. A bounded window is the only honest denominator.
fn fetch_table(
    client: &SurrealClient,
    table: &str,
    backend_name: &str,
    limit: usize,
) -> Result<(Vec<JobRow>, StatusCounts, i64, Option<DateTime<Utc>>)> {
    let query = format!(
        "SELECT type::string(id) AS id, status, path, retries, max_retries, retry_strategy, \
         type::string(created_at) AS created_at, type::string(updated_at) AS updated_at, errors, payload \
         FROM {table} ORDER BY updated_at DESC LIMIT {limit}; \
         SELECT status, count() AS n FROM {table} \
         WHERE status IN ['pending', 'processing'] OR updated_at > time::now() - 1h \
         GROUP BY status; \
         SELECT count() AS n FROM {table} WHERE status = 'success' AND updated_at > time::now() - 1m GROUP ALL; \
         SELECT type::string(created_at) AS created_at FROM {table} WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1;",
        table = table,
        limit = limit
    );

    let responses = client
        .execute(&query)
        .with_context(|| format!("Failed to query job table '{}'", table))?;
    let results: Vec<Option<Value>> = responses.into_iter().map(|r| r.result).collect();

    let rows = result_rows(results.first().and_then(|r| r.as_ref()))
        .iter()
        .filter_map(|v| parse_job_row(v, table, backend_name))
        .collect::<Vec<_>>();

    let mut counts = StatusCounts::default();
    for v in result_rows(results.get(1).and_then(|r| r.as_ref())) {
        let status = v.get("status").and_then(Value::as_str).unwrap_or("");
        let n = v.get("n").and_then(Value::as_i64).unwrap_or(0);
        counts.add(status, n);
    }

    let throughput = result_rows(results.get(2).and_then(|r| r.as_ref()))
        .first()
        .and_then(|v| v.get("n"))
        .and_then(Value::as_i64)
        .unwrap_or(0);

    let oldest_pending = result_rows(results.get(3).and_then(|r| r.as_ref()))
        .first()
        .and_then(|v| v.get("created_at"))
        .and_then(Value::as_str)
        .and_then(parse_ts);

    Ok((rows, counts, throughput, oldest_pending))
}

/// Fetch and merge a snapshot across every configured job table. A failure on one
/// table is recorded but does not abort the others (e.g. a table that does not
/// exist yet on a fresh database).
fn fetch_snapshot(
    client: &SurrealClient,
    tables: &BTreeMap<String, String>,
    limit: usize,
) -> Snapshot {
    let mut snapshot = Snapshot {
        rows: Vec::new(),
        counts: StatusCounts::default(),
        throughput_1m: 0,
        oldest_pending: None,
        errors: Vec::new(),
    };

    for (table, backend_name) in tables {
        match fetch_table(client, table, backend_name, limit) {
            Ok((rows, counts, throughput, oldest)) => {
                snapshot.rows.extend(rows);
                snapshot.counts.pending += counts.pending;
                snapshot.counts.processing += counts.processing;
                snapshot.counts.success += counts.success;
                snapshot.counts.failed += counts.failed;
                snapshot.counts.other += counts.other;
                snapshot.throughput_1m += throughput;
                if let Some(o) = oldest {
                    snapshot.oldest_pending =
                        Some(snapshot.oldest_pending.map(|cur| cur.min(o)).unwrap_or(o));
                }
            }
            Err(e) => snapshot.errors.push(format!("{}: {:#}", table, e)),
        }
    }

    // Merge-sort by updated_at descending. ISO-8601 strings sort chronologically.
    snapshot
        .rows
        .sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
    snapshot
}

/// If *every* configured job table failed to query, the snapshot is meaningless
/// (an empty dashboard would falsely read as "0 jobs, all healthy"). Turn that
/// into a hard error, with a hint for the common connection-refused case.
fn total_failure(snapshot: &Snapshot, table_count: usize) -> Option<String> {
    if table_count == 0 || snapshot.errors.len() < table_count {
        return None;
    }
    let joined = snapshot.errors.join("; ");
    if joined.contains("Connection refused") || joined.contains("Failed to connect") {
        Some(format!(
            "{}\n\nCould not reach SurrealDB. Is the stack running? Start it with `spky dev`, \
             or point at the right instance with --url / SURREAL_URL.",
            joined
        ))
    } else {
        Some(joined)
    }
}

fn parse_job_row(v: &Value, table: &str, backend_name: &str) -> Option<JobRow> {
    let obj = v.as_object()?;
    let id = obj.get("id").and_then(Value::as_str)?.to_string();
    Some(JobRow {
        id,
        table: table.to_string(),
        backend: backend_name.to_string(),
        status: obj
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        path: obj
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        retries: obj.get("retries").and_then(Value::as_i64).unwrap_or(0),
        max_retries: obj.get("max_retries").and_then(Value::as_i64).unwrap_or(0),
        retry_strategy: obj
            .get("retry_strategy")
            .and_then(Value::as_str)
            .unwrap_or("linear")
            .to_string(),
        created_at: obj
            .get("created_at")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        updated_at: obj
            .get("updated_at")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        errors: obj
            .get("errors")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        payload: obj.get("payload").cloned().unwrap_or(Value::Null),
    })
}

// =============================================================
// Formatting helpers
// =============================================================

fn parse_ts(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Compact age from a past timestamp ("3s", "4m", "2h", "5d").
fn fmt_age(ts: DateTime<Utc>) -> String {
    fmt_secs((Utc::now() - ts).num_seconds().max(0))
}

fn fmt_secs(secs: i64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let kept: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{}…", kept)
    }
}

// =============================================================
// `spky jobs list`
// =============================================================

fn list(
    client: &SurrealClient,
    tables: &BTreeMap<String, String>,
    status: Option<String>,
    table: Option<String>,
    limit: usize,
    json: bool,
) -> Result<()> {
    let snapshot = fetch_snapshot(client, tables, limit);
    if let Some(err) = total_failure(&snapshot, tables.len()) {
        bail!(err);
    }

    let rows: Vec<&JobRow> = snapshot
        .rows
        .iter()
        .filter(|r| status.as_deref().map(|s| r.status == s).unwrap_or(true))
        .filter(|r| table.as_deref().map(|t| r.table == t).unwrap_or(true))
        .collect();

    if json {
        let arr: Vec<Value> = rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.id,
                    "table": r.table,
                    "backend": r.backend,
                    "status": r.status,
                    "path": r.path,
                    "retries": r.retries,
                    "max_retries": r.max_retries,
                    "retry_strategy": r.retry_strategy,
                    "created_at": r.created_at,
                    "updated_at": r.updated_at,
                    "errors": r.errors,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&Value::Array(arr))?);
        return Ok(());
    }

    // Table first, then the summary at the BOTTOM so it stays on screen without
    // scrolling back up past a long list.
    if rows.is_empty() {
        println!("{DIM}No jobs found.{RESET}");
    } else {
        println!(
            "{BOLD}{:<24} {:<10} {:<16} {:<12} {:>5}  {:>5}  {}{RESET}",
            "ID", "BACKEND", "PATH", "STATUS", "RETRY", "AGE", "LAST ERROR"
        );
        for r in rows {
            let (icon, color) = status_style(&r.status);
            println!(
                "{:<24} {:<10} {:<16} {}{} {:<10}{RESET} {:>5}  {:>5}  {DIM}{}{RESET}",
                truncate(&r.id, 24),
                truncate(&r.backend, 10),
                truncate(&r.path, 16),
                color,
                icon,
                r.status,
                format!("{}/{}", r.retries, r.max_retries),
                r.age().unwrap_or_else(|| "-".to_string()),
                truncate(&r.last_error().unwrap_or_default(), 50),
            );
        }
    }

    // Aggregate summary.
    let c = &snapshot.counts;
    let oldest = snapshot
        .oldest_pending
        .map(fmt_age)
        .unwrap_or_else(|| "-".to_string());
    println!();
    println!(
        "{BOLD}jobs{RESET}  {YELLOW}{} pending{RESET}  {CYAN}{} processing{RESET}  {GREEN}{} success{RESET}  {RED}{} failed{RESET}  {DIM}({} in last hour){RESET}",
        c.pending, c.processing, c.success, c.failed, c.total()
    );
    println!(
        "{DIM}fail rate {:.1}% (1h)   throughput {} ✓/min   oldest pending {}{RESET}",
        c.fail_rate(),
        snapshot.throughput_1m,
        oldest
    );
    for e in &snapshot.errors {
        println!("{YELLOW}warn:{RESET} {}", e);
    }
    Ok(())
}

/// Plain (non-ANSI) status icon for terminals and the TUI.
fn status_icon(status: &str) -> &'static str {
    match status {
        "pending" => "⏳",
        "processing" => "●",
        "success" => "✓",
        "failed" => "✗",
        _ => "·",
    }
}

fn status_style(status: &str) -> (&'static str, &'static str) {
    match status {
        "pending" => ("⏳", YELLOW),
        "processing" => ("●", CYAN),
        "success" => ("✓", GREEN),
        "failed" => ("✗", RED),
        _ => ("·", DIM),
    }
}

// =============================================================
// `spky jobs get`
// =============================================================

/// Resolve a user-supplied job reference to a concrete `(table, key)`. Accepts a
/// full `table:key`, a bare key, or a (table-qualified or bare) key *prefix* — so
/// the truncated ids shown in the list can be pasted directly. Errors if the
/// prefix matches zero or more than one job.
fn resolve_job_ref(
    client: &SurrealClient,
    tables: &BTreeMap<String, String>,
    input: &str,
) -> Result<(String, String)> {
    let (search_tables, prefix): (Vec<String>, &str) = match input.split_once(':') {
        Some((tb, key)) => (vec![tb.to_string()], key),
        None => (tables.keys().cloned().collect(), input),
    };
    if prefix.is_empty() {
        bail!("Job id must include a key, got '{}'", input);
    }

    // (table, key) pairs whose key starts with the given prefix.
    let mut matches: Vec<(String, String)> = Vec::new();
    for tb in &search_tables {
        let query = format!(
            "SELECT VALUE record::id(id) FROM {} WHERE string::starts_with(record::id(id), '{}') LIMIT 26;",
            tb,
            esc(prefix)
        );
        let responses = client
            .execute(&query)
            .with_context(|| format!("Failed to search job table '{}'", tb))?;
        let keys = responses
            .into_iter()
            .next()
            .and_then(|r| r.result)
            .map(|r| result_rows(Some(&r)))
            .unwrap_or_default();
        for v in keys {
            if let Some(k) = v.as_str() {
                matches.push((tb.clone(), k.to_string()));
            }
        }
    }

    match matches.len() {
        0 => bail!("No job matching '{}'", input),
        1 => Ok(matches.into_iter().next().unwrap()),
        n => {
            let preview: Vec<String> = matches
                .iter()
                .take(6)
                .map(|(tb, k)| format!("{}:{}", tb, k))
                .collect();
            bail!(
                "'{}' is ambiguous ({} matches): {}{}",
                input,
                n,
                preview.join(", "),
                if n > preview.len() { ", …" } else { "" }
            )
        }
    }
}

fn get(
    client: &SurrealClient,
    tables: &BTreeMap<String, String>,
    id: String,
    json: bool,
) -> Result<()> {
    let (tb, key) = resolve_job_ref(client, tables, &id)?;
    let tb = tb.as_str();
    let key = key.as_str();
    let backend_name = tables.get(tb).cloned().unwrap_or_else(|| tb.to_string());
    // `result` is selected here but NOT in the list query: it is the backend's response
    // body (capped at 64 KiB), which is worth showing for one job and far too much to
    // pull for every row of a listing. The docs have always promised `get` shows it.
    let query = format!(
        "SELECT type::string(id) AS id, status, path, retries, max_retries, retry_strategy, \
         type::string(created_at) AS created_at, type::string(updated_at) AS updated_at, \
         errors, payload, result \
         FROM type::record('{}', '{}');",
        esc(tb),
        esc(key)
    );
    let responses = client.execute(&query).context("Failed to load job")?;
    let row = responses
        .into_iter()
        .next()
        .and_then(|r| r.result)
        .and_then(|r| result_rows(Some(&r)).into_iter().next())
        .ok_or_else(|| anyhow!("Job '{}' not found", id))?;

    if json {
        println!("{}", serde_json::to_string_pretty(&row)?);
        return Ok(());
    }

    let job =
        parse_job_row(&row, tb, &backend_name).ok_or_else(|| anyhow!("Unexpected job shape"))?;
    let (icon, color) = status_style(&job.status);
    println!("{BOLD}{}{RESET}", job.id);
    println!("  status      : {}{} {}{RESET}", color, icon, job.status);
    println!("  backend     : {}", job.backend);
    println!("  path        : {}", job.path);
    println!(
        "  retries     : {}/{} ({})",
        job.retries, job.max_retries, job.retry_strategy
    );
    println!(
        "  created     : {} {DIM}({} ago){RESET}",
        job.created_at,
        job.age().unwrap_or_else(|| "-".into())
    );
    println!("  updated     : {}", job.updated_at);
    println!(
        "  payload     : {}",
        serde_json::to_string(&job.payload).unwrap_or_default()
    );
    // Only on success: on any other status there is nothing to show, and printing
    // `null` reads as "the backend returned nothing" rather than "it never ran".
    if job.status == "success" {
        match row.get("result") {
            Some(Value::Null) | None => println!("  result      : {DIM}none{RESET}"),
            Some(result) => println!(
                "  result      : {}",
                serde_json::to_string(result).unwrap_or_default()
            ),
        }
    }
    if job.errors.is_empty() {
        println!("  errors      : {DIM}none{RESET}");
    } else {
        println!("  errors      : {} attempt(s)", job.errors.len());
        for (i, e) in job.errors.iter().enumerate() {
            let code = e.get("code").map(|c| c.to_string()).unwrap_or_default();
            let reason = e.get("reason").and_then(Value::as_str).unwrap_or("");
            println!("    {DIM}#{} [{}]{RESET} {}", i + 1, code, reason);
        }
    }
    Ok(())
}

// =============================================================
// `spky jobs kill` / `spky jobs retry`
// =============================================================

fn kill(client: &SurrealClient, id: &str) -> Result<()> {
    let result = call_job_fn(client, "kill", id)?;
    print_action_result("Killed", id, &result);
    Ok(())
}

fn retry(client: &SurrealClient, id: &str) -> Result<()> {
    let result = call_job_fn(client, "retry", id)?;
    print_action_result("Retrying", id, &result);
    Ok(())
}

// =============================================================
// `spky jobs clear`
// =============================================================

/// Rows deleted per statement when clearing a job table.
///
/// Deleting a job row is not a local write: an outbox table is a synced user
/// table, so each DELETE fires the generated `_00_<table>_delete` event, whose body
/// removes the row's `_00_version` entry and `http::post`s the whole record to the
/// SSP's `/ingest`. One unbounded `DELETE` over a table with millions of rows is
/// therefore millions of HTTP round-trips and WAL appends inside a single
/// transaction, which is how this command used to stall a cluster. The cost per row
/// is a network round-trip, so keep this small.
const CLEAR_BATCH: usize = 500;

/// Delete jobs from all discovered job tables. By default only terminal jobs
/// (status `success` or `failed`) are removed — finished history, with no
/// in-flight pickup to coordinate with the SSP, so a plain `DELETE` is correct.
/// With `all`, pending (queued) jobs are also dropped. `processing` (in-flight)
/// jobs are NEVER deleted — killing a job mid-run is `spky jobs kill`'s domain.
///
/// Drains in bounded batches (see [`CLEAR_BATCH`]) rather than one statement.
fn clear(client: &SurrealClient, tables: &BTreeMap<String, String>, all: bool) -> Result<()> {
    let mut total = 0usize;
    for table in tables.keys() {
        // `table` is a config-derived identifier (same direct interpolation the
        // SELECTs use). `all` widens the terminal set to include pending, but always
        // spares `processing` so a running job is never yanked out from under the SSP.
        let cond = if all {
            "status != 'processing'"
        } else {
            "status IN ['success', 'failed']"
        };
        // One statement per batch, wrapped in a block so the result is a single
        // count. Projecting `id` instead of `RETURN BEFORE` keeps the response small:
        // a job row carries an uncapped `payload` and up to 64 KiB of `result`, and
        // we only ever wanted the number.
        let query = format!(
            "RETURN {{ \
             LET $doomed = (SELECT id FROM {table} WHERE {cond} LIMIT {CLEAR_BATCH}); \
             DELETE $doomed.id; \
             RETURN count($doomed); \
             }};"
        );
        let mut n = 0usize;
        loop {
            let responses = client
                .execute(&query)
                .with_context(|| format!("Failed to clear jobs in '{}'", table))?;
            let batch = responses
                .into_iter()
                .next()
                .and_then(|r| r.result)
                .and_then(|r| result_count(&r))
                .unwrap_or(0);
            n += batch;
            if batch > 0 {
                // Progress matters here: a large backlog takes many batches, and a
                // silent multi-minute command reads as a hang.
                print!("\r  {DIM}{table} : cleared {n} job(s)…{RESET}");
                let _ = std::io::Write::flush(&mut std::io::stdout());
            }
            if batch < CLEAR_BATCH {
                break;
            }
        }
        if n > 0 {
            println!("\r  {GREEN}✓{RESET} {table} : removed {n} job(s)          ");
        }
        total += n;
    }
    if total == 0 {
        if all {
            println!("No jobs to clear.");
        } else {
            println!("No failed or successful jobs to clear.");
        }
    } else {
        let kind = if all { "job(s)" } else { "terminal job(s)" };
        println!("\nCleared {GREEN}{total}{RESET} {kind}.");
    }
    Ok(())
}

/// Invoke `fn::job::<action>($id)` and return the (parsed) backend response value.
fn call_job_fn(client: &SurrealClient, action: &str, id: &str) -> Result<Value> {
    let query = format!("RETURN fn::job::{}('{}');", action, esc(id));
    let responses = client.execute(&query).map_err(|e| {
        let msg = e.to_string();
        if msg.contains("fn::job") && (msg.contains("not found") || msg.contains("does not exist"))
        {
            // The schema predates this feature: the proxy functions aren't installed.
            anyhow!(
                "{}\n\nThe job-control functions are not deployed. Run `spky dev` (or redeploy \
                 the schema) to install fn::job::kill / fn::job::retry, and make sure the \
                 SSP/scheduler are running.",
                msg
            )
        } else {
            anyhow!("Failed to {} job '{}': {}", action, id, msg)
        }
    })?;
    Ok(responses
        .into_iter()
        .next()
        .and_then(|r| r.result)
        .unwrap_or(Value::Null))
}

fn action_message(result: &Value) -> Option<String> {
    result
        .get("message")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn print_action_result(verb: &str, id: &str, result: &Value) {
    match action_message(result) {
        Some(msg) => println!("{GREEN}{} {}{RESET} — {}", verb, id, msg),
        None => println!("{GREEN}{} {}{RESET}", verb, id),
    }
}

// =============================================================
// Interactive TUI (`spky jobs`)
// =============================================================

const TICK: Duration = Duration::from_millis(200);
const REFRESH_INTERVAL: Duration = Duration::from_millis(1000);
/// How long a kill/retry status (or error) stays in the footer before clearing.
const STATUS_TTL: Duration = Duration::from_secs(5);
const TUI_LIMIT: usize = 200;

struct TuiApp {
    snapshot: Snapshot,
    /// `None` = show all; otherwise one of STATUS_ORDER.
    filter: Option<&'static str>,
    table_state: TableState,
    detail_open: bool,
    last_refresh: Instant,
    status_msg: Option<String>,
    /// When `status_msg` was set, so it can auto-expire.
    status_set_at: Option<Instant>,
}

impl TuiApp {
    fn new() -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));
        Self {
            snapshot: Snapshot {
                rows: Vec::new(),
                counts: StatusCounts::default(),
                throughput_1m: 0,
                oldest_pending: None,
                errors: Vec::new(),
            },
            filter: None,
            table_state,
            detail_open: false,
            last_refresh: Instant::now(),
            status_msg: None,
            status_set_at: None,
        }
    }

    fn set_status(&mut self, msg: String) {
        self.status_msg = Some(msg);
        self.status_set_at = Some(Instant::now());
    }

    /// Clear a kill/retry status once it has been on screen long enough.
    fn expire_status(&mut self) {
        if self
            .status_set_at
            .map(|t| t.elapsed() >= STATUS_TTL)
            .unwrap_or(false)
        {
            self.status_msg = None;
            self.status_set_at = None;
        }
    }

    fn refresh(&mut self, client: &SurrealClient, tables: &BTreeMap<String, String>) {
        self.snapshot = fetch_snapshot(client, tables, TUI_LIMIT);
        self.last_refresh = Instant::now();
        self.clamp_selection();
    }

    fn visible(&self) -> Vec<&JobRow> {
        self.snapshot
            .rows
            .iter()
            .filter(|r| self.filter.map(|f| r.status == f).unwrap_or(true))
            .collect()
    }

    fn clamp_selection(&mut self) {
        let len = self.visible().len();
        if len == 0 {
            self.table_state.select(None);
        } else {
            let sel = self.table_state.selected().unwrap_or(0).min(len - 1);
            self.table_state.select(Some(sel));
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.visible().len();
        if len == 0 {
            return;
        }
        let cur = self.table_state.selected().unwrap_or(0) as isize;
        let next = (cur + delta).clamp(0, len as isize - 1);
        self.table_state.select(Some(next as usize));
    }

    fn selected_id(&self) -> Option<String> {
        let visible = self.visible();
        self.table_state
            .selected()
            .and_then(|i| visible.get(i))
            .map(|r| r.id.clone())
    }

    fn cycle_filter(&mut self) {
        self.filter = match self.filter {
            None => Some(STATUS_ORDER[0]),
            Some(cur) => {
                let idx = STATUS_ORDER.iter().position(|s| *s == cur).unwrap_or(0);
                if idx + 1 < STATUS_ORDER.len() {
                    Some(STATUS_ORDER[idx + 1])
                } else {
                    None
                }
            }
        };
        self.clamp_selection();
    }
}

fn run_tui(client: SurrealClient, tables: BTreeMap<String, String>) -> Result<()> {
    // Probe connectivity first so we don't drop into an empty alt-screen on a
    // dead connection (which would misleadingly look like "0 jobs").
    let probe = fetch_snapshot(&client, &tables, TUI_LIMIT);
    if let Some(err) = total_failure(&probe, tables.len()) {
        bail!(err);
    }

    // Restore the terminal even if a panic unwinds through the draw loop.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(std::io::stderr(), LeaveAlternateScreen);
        original_hook(info);
    }));

    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen, crossterm::cursor::Hide)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let result = tui_loop(&mut terminal, &client, &tables);

    crossterm::terminal::disable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        crossterm::cursor::Show
    )?;

    result
}

fn tui_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    client: &SurrealClient,
    tables: &BTreeMap<String, String>,
) -> Result<()> {
    let mut app = TuiApp::new();
    app.refresh(client, tables);

    loop {
        terminal.draw(|f| render(f, &mut app))?;

        if event::poll(TICK)? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => {
                        if app.detail_open {
                            app.detail_open = false;
                        } else {
                            break;
                        }
                    }
                    KeyCode::Up => app.move_selection(-1),
                    KeyCode::Down => app.move_selection(1),
                    KeyCode::Enter => app.detail_open = !app.detail_open,
                    KeyCode::Char('f') => app.cycle_filter(),
                    KeyCode::Char('k') => {
                        if let Some(id) = app.selected_id() {
                            let msg = match call_job_fn(client, "kill", &id) {
                                Ok(v) => format!(
                                    "killed {} — {}",
                                    id,
                                    action_message(&v).unwrap_or_else(|| "ok".into())
                                ),
                                Err(e) => {
                                    format!("kill {} failed: {}", id, first_line(&e.to_string()))
                                }
                            };
                            app.set_status(msg);
                            app.refresh(client, tables);
                        }
                    }
                    KeyCode::Char('r') => {
                        if let Some(id) = app.selected_id() {
                            let msg = match call_job_fn(client, "retry", &id) {
                                Ok(v) => format!(
                                    "retrying {} — {}",
                                    id,
                                    action_message(&v).unwrap_or_else(|| "ok".into())
                                ),
                                Err(e) => {
                                    format!("retry {} failed: {}", id, first_line(&e.to_string()))
                                }
                            };
                            app.set_status(msg);
                            app.refresh(client, tables);
                        }
                    }
                    _ => {}
                }
            }
        }

        app.expire_status();

        if app.last_refresh.elapsed() >= REFRESH_INTERVAL {
            app.refresh(client, tables);
        }
    }

    Ok(())
}

/// First line of a (possibly multi-line) error, for one-line footer display.
fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s)
}

fn render(f: &mut Frame, app: &mut TuiApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4), // metrics
            Constraint::Min(0),    // table
            Constraint::Length(1), // footer
        ])
        .split(f.area());

    render_metrics(f, chunks[0], app);
    render_table(f, chunks[1], app);
    render_footer(f, chunks[2], app);

    if app.detail_open {
        render_detail(f, app);
    }
}

fn render_metrics(f: &mut Frame, area: Rect, app: &TuiApp) {
    let c = &app.snapshot.counts;
    let oldest = app
        .snapshot
        .oldest_pending
        .map(fmt_age)
        .unwrap_or_else(|| "-".to_string());

    let counts_line = Line::from(vec![
        Span::styled(
            format!("{} pending", c.pending),
            Style::default().fg(Color::Yellow),
        ),
        Span::raw("   "),
        Span::styled(
            format!("{} processing", c.processing),
            Style::default().fg(Color::Cyan),
        ),
        Span::raw("   "),
        Span::styled(
            format!("{} success", c.success),
            Style::default().fg(Color::Green),
        ),
        Span::raw("   "),
        Span::styled(
            format!("{} failed", c.failed),
            Style::default().fg(Color::Red),
        ),
        Span::styled(
            format!("   ({} in last hour)", c.total()),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    let detail_line = Line::from(Span::styled(
        format!(
            "fail rate {:.1}% (1h)    throughput {} ✓/min    oldest pending {}",
            c.fail_rate(),
            app.snapshot.throughput_1m,
            oldest
        ),
        Style::default().fg(Color::DarkGray),
    ));

    let filter_label = app.filter.unwrap_or("all");
    let block = Block::default().borders(Borders::ALL).title(Span::styled(
        format!(" spky jobs · filter: {} ", filter_label),
        Style::default().add_modifier(Modifier::BOLD),
    ));
    let para = Paragraph::new(vec![counts_line, detail_line]).block(block);
    f.render_widget(para, area);
}

fn render_table(f: &mut Frame, area: Rect, app: &mut TuiApp) {
    let visible = app.visible();
    if visible.is_empty() {
        let msg = if app.snapshot.rows.is_empty() {
            "No jobs found."
        } else {
            "No jobs match the current filter (press 'f' to cycle)."
        };
        let para = Paragraph::new(msg)
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center);
        f.render_widget(para, area);
        return;
    }

    let header = Row::new(vec![
        "ID",
        "BACKEND",
        "PATH",
        "STATUS",
        "RETRY",
        "AGE",
        "LAST ERROR",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD))
    .bottom_margin(0);

    let rows: Vec<Row> = visible
        .iter()
        .map(|r| {
            let color = match r.status.as_str() {
                "pending" => Color::Yellow,
                "processing" => Color::Cyan,
                "success" => Color::Green,
                "failed" => Color::Red,
                _ => Color::DarkGray,
            };
            Row::new(vec![
                Cell::from(truncate(&r.id, 22)),
                Cell::from(truncate(&r.backend, 10)),
                Cell::from(truncate(&r.path, 16)),
                Cell::from(Span::styled(
                    format!("{} {}", status_icon(&r.status), r.status),
                    Style::default().fg(color),
                )),
                Cell::from(format!("{}/{}", r.retries, r.max_retries)),
                Cell::from(r.age().unwrap_or_else(|| "-".to_string())),
                Cell::from(Span::styled(
                    truncate(&r.last_error().unwrap_or_default(), 40),
                    Style::default().fg(Color::DarkGray),
                )),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(22),
        Constraint::Length(10),
        Constraint::Length(16),
        Constraint::Length(13),
        Constraint::Length(6),
        Constraint::Length(5),
        Constraint::Min(10),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .row_highlight_style(
            Style::default()
                .bg(Color::Indexed(238))
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");

    f.render_stateful_widget(table, area, &mut app.table_state);
}

fn render_footer(f: &mut Frame, area: Rect, app: &TuiApp) {
    let text = if let Some(msg) = &app.status_msg {
        Line::from(Span::styled(
            format!(" {} ", msg),
            Style::default().fg(Color::White).bg(Color::Indexed(238)),
        ))
    } else if let Some(err) = app.snapshot.errors.first() {
        Line::from(Span::styled(
            format!(" ⚠ {} ", err),
            Style::default().fg(Color::Red),
        ))
    } else {
        Line::from(Span::styled(
            " ↑/↓ select   k kill   r retry   ⏎ details   f filter   q quit ",
            Style::default().fg(Color::DarkGray),
        ))
    };
    f.render_widget(Paragraph::new(text), area);
}

fn render_detail(f: &mut Frame, app: &TuiApp) {
    let visible = app.visible();
    let Some(job) = app.table_state.selected().and_then(|i| visible.get(i)) else {
        return;
    };

    let area = centered_rect(70, 70, f.area());
    f.render_widget(Clear, area);

    let mut lines = vec![
        kv("status", &job.status),
        kv("backend", &job.backend),
        kv("table", &job.table),
        kv("path", &job.path),
        kv(
            "retries",
            &format!(
                "{}/{} ({})",
                job.retries, job.max_retries, job.retry_strategy
            ),
        ),
        kv(
            "created",
            &format!(
                "{}  ({} ago)",
                job.created_at,
                job.age().unwrap_or_else(|| "-".into())
            ),
        ),
        kv("updated", &job.updated_at),
        kv(
            "payload",
            &truncate(
                &serde_json::to_string(&job.payload).unwrap_or_default(),
                200,
            ),
        ),
        Line::from(""),
        Line::from(Span::styled(
            format!("errors ({}):", job.errors.len()),
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];
    if job.errors.is_empty() {
        lines.push(Line::from(Span::styled(
            "  none",
            Style::default().fg(Color::DarkGray),
        )));
    }
    for (i, e) in job.errors.iter().enumerate() {
        let code = e.get("code").map(|c| c.to_string()).unwrap_or_default();
        let reason = e.get("reason").and_then(Value::as_str).unwrap_or("");
        lines.push(Line::from(format!("  #{} [{}] {}", i + 1, code, reason)));
    }

    let block = Block::default().borders(Borders::ALL).title(Span::styled(
        format!(" {} ", job.id),
        Style::default().add_modifier(Modifier::BOLD),
    ));
    let para = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    f.render_widget(para, area);
}

fn kv<'a>(key: &'a str, value: &str) -> Line<'a> {
    Line::from(vec![
        Span::styled(format!("{:<10}", key), Style::default().fg(Color::DarkGray)),
        Span::raw(value.to_string()),
    ])
}

/// Rect centered within `area`, sized as a percentage of it.
fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}
