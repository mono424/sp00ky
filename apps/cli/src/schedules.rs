//! `spky schedules` — operator surface for the server-side scheduler.
//!
//! Reads come straight from SurrealDB (`_00_schedule`, `_00_schedule_run`), like
//! `spky jobs` and `spky flag`. Writes are the interesting part: every operator
//! action is a field on the schedule row that the engine consumes on its next
//! sweep, not an RPC. `pause` sets `paused`, `trigger` sets
//! `trigger_requested_at`, and that is the whole mechanism — no new HTTP routes,
//! no proxy functions, and it behaves identically in singlenode and cluster mode
//! because both run the same engine against the same rows.
//!
//! The division of labour on this table is deliberate and worth preserving:
//! deploy owns the spec fields, the operator owns `paused` /
//! `trigger_requested_at`, and the engine owns `next_fire_at` / `last_*`. This
//! module writes only the middle group.

use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;

use crate::backend::{self, DEFAULT_CONFIG_PATH};
use crate::surreal_client::{MigrationDB, SurrealClient};
use crate::{ConnectionArgs, SchedulesCommands};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const CYAN: &str = "\x1b[36m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

pub fn run(
    conn: ConnectionArgs,
    config: Option<PathBuf>,
    action: SchedulesCommands,
) -> Result<()> {
    match action {
        SchedulesCommands::List { json } => {
            let client = client_from(&conn, &config)?;
            list(&client, json)
        }
        SchedulesCommands::Get { name, json } => {
            let client = client_from(&conn, &config)?;
            get(&client, &name, json)
        }
        SchedulesCommands::Pause { name } => {
            let client = client_from(&conn, &config)?;
            set_paused(&client, &name, true)
        }
        SchedulesCommands::Resume { name } => {
            let client = client_from(&conn, &config)?;
            set_paused(&client, &name, false)
        }
        SchedulesCommands::Trigger { name } => {
            let client = client_from(&conn, &config)?;
            trigger(&client, &name)
        }
        SchedulesCommands::Runs { name, status, limit, json } => {
            let client = client_from(&conn, &config)?;
            runs(&client, name.as_deref(), status.as_deref(), limit, json)
        }
        SchedulesCommands::Sync => sync(&conn, &config),
    }
}

fn client_from(conn: &ConnectionArgs, config: &Option<PathBuf>) -> Result<SurrealClient> {
    let c = conn.resolve(config)?;
    Ok(SurrealClient::new(
        &c.url,
        &c.namespace,
        &c.database,
        &c.username,
        &c.password,
    ))
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Rows out of one statement response.
fn result_rows(result: Option<&Value>) -> Vec<Value> {
    match result {
        Some(Value::Array(arr)) => arr.clone(),
        Some(other) => vec![other.clone()],
        None => vec![],
    }
}

fn query_rows(client: &SurrealClient, sql: &str) -> Result<Vec<Value>> {
    let responses = client.execute(sql).context("query failed")?;
    Ok(responses
        .into_iter()
        .next()
        .and_then(|r| r.result)
        .map(|r| result_rows(Some(&r)))
        .unwrap_or_default())
}

fn str_field(row: &Value, key: &str) -> String {
    row.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

/// Every field the list/get views read, with datetimes stringified so they
/// deserialize as plain JSON strings.
const SCHEDULE_FIELDS: &str = "name, kind, cron, every_ms, timezone, target_table, path, \
     for_each, concurrency, paused, config_disabled, last_error, last_run_status, \
     history_success_secs, history_failed_secs, \
     type::string(next_fire_at) AS next_fire_at, type::string(last_fire_at) AS last_fire_at, \
     type::string(last_run_at) AS last_run_at, \
     type::string(trigger_requested_at) AS trigger_requested_at";

// =============================================================
// `spky schedules list`
// =============================================================

fn list(client: &SurrealClient, json: bool) -> Result<()> {
    let rows = query_rows(
        client,
        &format!("SELECT {SCHEDULE_FIELDS} FROM _00_schedule ORDER BY name;"),
    )?;

    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("{DIM}No schedules deployed. Declare them under `schedules:` in sp00ky.yml.{RESET}");
        return Ok(());
    }

    println!(
        "{BOLD}{:<24} {:<9} {:<14} {:<9} {:<20} {:<10}{RESET}",
        "NAME", "KIND", "CADENCE", "STATE", "NEXT FIRE", "LAST RUN"
    );
    for row in &rows {
        let name = str_field(row, "name");
        let (state, color) = state_of(row);
        let last = row
            .get("last_run_status")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let (last_icon, last_color) = run_status_style(&last);
        println!(
            "{:<24} {:<9} {:<14} {color}{:<9}{RESET} {:<20} {last_color}{} {}{RESET}",
            truncate(&name, 24),
            str_field(row, "kind"),
            cadence_of(row),
            state,
            next_fire_display(row),
            last_icon,
            last,
        );
        if let Some(error) = row.get("last_error").and_then(Value::as_str) {
            println!("  {RED}last error{RESET}: {error}");
        }
    }
    Ok(())
}

fn state_of(row: &Value) -> (String, &'static str) {
    if row.get("paused").and_then(Value::as_bool) == Some(true) {
        return ("paused".into(), YELLOW);
    }
    if row.get("config_disabled").and_then(Value::as_bool) == Some(true) {
        return ("disabled".into(), DIM);
    }
    if row.get("last_error").and_then(Value::as_str).is_some() {
        return ("error".into(), RED);
    }
    if row.get("next_fire_at").and_then(Value::as_str).is_none() {
        return ("planning".into(), DIM);
    }
    ("active".into(), GREEN)
}

fn cadence_of(row: &Value) -> String {
    if let Some(cron) = row.get("cron").and_then(Value::as_str) {
        return truncate(cron, 14);
    }
    match row.get("every_ms").and_then(Value::as_i64) {
        Some(ms) => format!("every {}", fmt_ms(ms)),
        None => "-".to_string(),
    }
}

fn next_fire_display(row: &Value) -> String {
    match row.get("next_fire_at").and_then(Value::as_str) {
        Some(ts) => {
            let rel = crate::schedules::relative(ts);
            format!("{} {}", &ts[..ts.len().min(19)], rel)
        }
        None => "-".to_string(),
    }
}

// =============================================================
// `spky schedules get`
// =============================================================

fn get(client: &SurrealClient, name: &str, json: bool) -> Result<()> {
    let row = load_schedule(client, name)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&row)?);
        return Ok(());
    }

    let (state, color) = state_of(&row);
    println!("{BOLD}{}{RESET}", str_field(&row, "name"));
    println!("  kind        : {}", str_field(&row, "kind"));
    println!("  cadence     : {}", cadence_of(&row));
    if let Some(tz) = row.get("timezone").and_then(Value::as_str) {
        println!("  timezone    : {tz}");
    }
    println!("  state       : {color}{state}{RESET}");
    println!("  target      : {} {}", str_field(&row, "target_table"), str_field(&row, "path"));
    if let Some(fe) = row.get("for_each").and_then(Value::as_str) {
        println!("  forEach     : {fe}");
        println!("  concurrency : {}", str_field(&row, "concurrency"));
    }
    println!("  next fire   : {}", next_fire_display(&row));
    println!(
        "  last fire   : {}",
        row.get("last_fire_at").and_then(Value::as_str).unwrap_or("-")
    );
    if let Some(err) = row.get("last_error").and_then(Value::as_str) {
        println!("  {RED}last error  : {err}{RESET}");
    }

    print_rollup_totals(client, name)?;

    let recent = query_rows(
        client,
        &format!(
            "SELECT status, key, type::string(fire_at) AS fire_at, job_id \
             FROM _00_schedule_run WHERE schedule_name = '{}' ORDER BY fire_at DESC LIMIT 10;",
            esc(name)
        ),
    )?;
    if recent.is_empty() {
        println!("  runs        : {DIM}none yet{RESET}");
        return Ok(());
    }
    println!("  runs        : {} most recent", recent.len());
    for run in &recent {
        let status = str_field(run, "status");
        let (icon, color) = run_status_style(&status);
        let key = str_field(run, "key");
        let key = if key.is_empty() { String::new() } else { format!(" {DIM}{key}{RESET}") };
        println!(
            "    {color}{icon} {:<9}{RESET} {}{}",
            status,
            &str_field(run, "fire_at")[..19.min(str_field(run, "fire_at").len())],
            key
        );
    }
    Ok(())
}

/// Lifetime totals for a schedule: the retention rollup plus the runs that have
/// not been pruned yet.
///
/// The rollup is written as history is pruned, so on its own it covers runs whose
/// rows are long gone — which is the point of keeping short retention windows — but
/// says nothing about a bucket retention has not swept. Retention is routinely
/// asymmetric (successes for 30m, failures for the 30d default), so the rollup alone
/// reads `0 failed` for a month on a schedule that is visibly failing. Adding the
/// survivors is exact, not approximate: a run row is either still present or it was
/// folded when pruned, never both.
///
/// Still a LOWER bound, not an audit trail: rows removed by `spky jobs clear`, by an
/// application's own DELETE, or before the rollup existed were never counted.
fn print_rollup_totals(client: &SurrealClient, name: &str) -> Result<()> {
    let rows = query_rows(
        client,
        &format!(
            "SELECT math::sum(success) AS success, math::sum(failed) AS failed, \
             math::sum(skipped) AS skipped, math::sum(replaced) AS replaced, \
             math::sum(killed) AS killed \
             FROM _00_run_rollup WHERE scope = 'schedule' AND name = '{}' GROUP ALL;",
            esc(name)
        ),
    )?;
    let live = query_rows(
        client,
        &format!(
            "SELECT status, count() AS n FROM _00_schedule_run \
             WHERE schedule_name = '{}' \
             AND status IN ['success', 'failed', 'skipped', 'replaced', 'killed'] \
             GROUP BY status;",
            esc(name)
        ),
    )?;
    let surviving = |status: &str| -> i64 {
        live.iter()
            .find(|r| r.get("status").and_then(Value::as_str) == Some(status))
            .and_then(|r| r.get("n").and_then(Value::as_i64))
            .unwrap_or(0)
    };
    let row = rows.first().cloned().unwrap_or_default();
    let n = |key: &str| {
        row.get(key).and_then(Value::as_i64).unwrap_or(0) + surviving(key)
    };
    let (success, failed) = (n("success"), n("failed"));
    let (skipped, replaced, killed) = (n("skipped"), n("replaced"), n("killed"));
    if success + failed + skipped + replaced + killed == 0 {
        return Ok(());
    }

    let mut parts = vec![format!("{GREEN}{success} ok{RESET}")];
    if failed > 0 {
        parts.push(format!("{RED}{failed} failed{RESET}"));
    }
    for (n, label) in [(skipped, "skipped"), (replaced, "replaced"), (killed, "killed")] {
        if n > 0 {
            parts.push(format!("{DIM}{n} {label}{RESET}"));
        }
    }
    println!("  pruned      : {}", parts.join("  "));
    Ok(())
}

fn load_schedule(client: &SurrealClient, name: &str) -> Result<Value> {
    let rows = query_rows(
        client,
        &format!(
            "SELECT {SCHEDULE_FIELDS} FROM _00_schedule WHERE name = '{}' LIMIT 1;",
            esc(name)
        ),
    )?;
    rows.into_iter().next().ok_or_else(|| {
        anyhow!("No schedule named '{name}'. `spky schedules list` shows what is deployed.")
    })
}

// =============================================================
// `spky schedules pause` / `resume` / `trigger`
// =============================================================

fn set_paused(client: &SurrealClient, name: &str, paused: bool) -> Result<()> {
    // Confirm it exists first: a silent no-op on a typo'd name is worse than an
    // error, because the operator walks away believing the schedule is paused.
    load_schedule(client, name)?;
    client
        .execute(&format!(
            "UPDATE {} SET paused = {paused};",
            crate::schedule_sync::record_literal(name)
        ))
        .with_context(|| format!("failed to {} '{name}'", if paused { "pause" } else { "resume" }))?;

    if paused {
        println!("{YELLOW}Paused{RESET} {BOLD}{name}{RESET} — no further fires until resumed.");
        println!("{DIM}Runs already in flight are left to finish.{RESET}");
    } else {
        println!("{GREEN}Resumed{RESET} {BOLD}{name}{RESET} — the next fire is planned from now.");
    }
    Ok(())
}

fn trigger(client: &SurrealClient, name: &str) -> Result<()> {
    let row = load_schedule(client, name)?;
    if row.get("paused").and_then(Value::as_bool) == Some(true) {
        bail!(
            "'{name}' is paused, and pausing wins over a trigger. \
             Run `spky schedules resume {name}` first."
        );
    }
    client
        .execute(&format!(
            "UPDATE {} SET trigger_requested_at = time::now();",
            crate::schedule_sync::record_literal(name)
        ))
        .with_context(|| format!("failed to trigger '{name}'"))?;
    println!("{CYAN}Triggered{RESET} {BOLD}{name}{RESET} — it fires within a few seconds.");
    println!("{DIM}The cron clock is untouched: this run is extra, not instead.{RESET}");
    Ok(())
}

// =============================================================
// `spky schedules runs`
// =============================================================

fn runs(
    client: &SurrealClient,
    name: Option<&str>,
    status: Option<&str>,
    limit: usize,
    json: bool,
) -> Result<()> {
    let mut filters = Vec::new();
    if let Some(name) = name {
        filters.push(format!("schedule_name = '{}'", esc(name)));
    }
    if let Some(status) = status {
        filters.push(format!("status = '{}'", esc(status)));
    }
    let where_clause =
        if filters.is_empty() { String::new() } else { format!("WHERE {}", filters.join(" AND ")) };

    let rows = query_rows(
        client,
        &format!(
            "SELECT schedule_name, kind, status, key, trigger, job_id, \
             type::string(fire_at) AS fire_at, type::string(finished_at) AS finished_at \
             FROM _00_schedule_run {where_clause} ORDER BY fire_at DESC LIMIT {limit};"
        ),
    )?;

    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("{DIM}No runs recorded{}.{RESET}", name.map(|n| format!(" for '{n}'")).unwrap_or_default());
        return Ok(());
    }

    println!(
        "{BOLD}{:<22} {:<10} {:<20} {:<9} {:<22}{RESET}",
        "SCHEDULE", "STATUS", "FIRED", "TRIGGER", "KEY"
    );
    for row in &rows {
        let status = str_field(row, "status");
        let (icon, color) = run_status_style(&status);
        let fired = str_field(row, "fire_at");
        println!(
            "{:<22} {color}{} {:<8}{RESET} {:<20} {:<9} {:<22}",
            truncate(&str_field(row, "schedule_name"), 22),
            icon,
            status,
            &fired[..19.min(fired.len())],
            str_field(row, "trigger"),
            truncate(&str_field(row, "key"), 22),
        );
    }
    Ok(())
}

// =============================================================
// `spky schedules sync`
// =============================================================

/// Re-push the manifest's definitions without a full deploy.
///
/// Deploy already syncs, so this exists for the edit-while-`spky dev`-runs loop
/// (there is no config watcher) and to recover from a sync that failed after an
/// otherwise successful deploy.
fn sync(conn: &ConnectionArgs, config: &Option<PathBuf>) -> Result<()> {
    let config_path = config.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    if !config_path.exists() {
        bail!("Config file not found: {}", config_path.display());
    }
    let cfg = backend::load_config(&config_path);
    cfg.validate().context("sp00ky.yml is not valid")?;

    let mut processor = backend::BackendProcessor::new();
    processor
        .process(&config_path)
        .context("failed to resolve backends from sp00ky.yml")?;

    let base_dir = config_path.parent().unwrap_or(std::path::Path::new("."));
    let client = client_from(conn, config)?;
    let report = crate::schedule_sync::sync(&client, &cfg, &processor, base_dir)?;
    if report.is_noop() {
        println!("{DIM}Schedules already up to date ({}).{RESET}", report.summary());
    } else {
        println!("{GREEN}Synced schedules{RESET}: {}", report.summary());
    }
    Ok(())
}

// =============================================================
// Formatting
// =============================================================

fn run_status_style(status: &str) -> (&'static str, &'static str) {
    match status {
        "success" => ("✔", GREEN),
        "failed" => ("✖", RED),
        "running" => ("◐", YELLOW),
        "skipped" => ("⊘", DIM),
        "replaced" => ("⇄", DIM),
        "killed" => ("■", RED),
        _ => ("·", DIM),
    }
}

fn fmt_ms(ms: i64) -> String {
    let secs = ms / 1000;
    if secs % 3600 == 0 && secs >= 3600 {
        format!("{}h", secs / 3600)
    } else if secs % 60 == 0 && secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// `(in 4m)` / `(2h ago)` next to an absolute timestamp — the absolute value is
/// what you paste into a query, the relative one is what you actually read.
pub(crate) fn relative(ts: &str) -> String {
    let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(ts) else { return String::new() };
    let delta = parsed.with_timezone(&chrono::Utc) - chrono::Utc::now();
    let secs = delta.num_seconds();
    let magnitude = fmt_ms(secs.abs() * 1000);
    if secs >= 0 {
        format!("{DIM}(in {magnitude}){RESET}")
    } else {
        format!("{DIM}({magnitude} ago){RESET}")
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    format!("{}…", s.chars().take(max.saturating_sub(1)).collect::<String>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn state_reflects_the_field_that_matters_most() {
        // Paused beats everything: it is the operator's explicit intent.
        let paused = json!({ "paused": true, "next_fire_at": "2026-07-26T01:00:00Z" });
        assert_eq!(state_of(&paused).0, "paused");

        let disabled = json!({ "paused": false, "config_disabled": true });
        assert_eq!(state_of(&disabled).0, "disabled");

        let errored = json!({ "paused": false, "last_error": "invalid cron" });
        assert_eq!(state_of(&errored).0, "error");

        // No clock yet — the engine hasn't planned it.
        let unplanned = json!({ "paused": false });
        assert_eq!(state_of(&unplanned).0, "planning");

        let active = json!({ "paused": false, "next_fire_at": "2026-07-26T01:00:00Z" });
        assert_eq!(state_of(&active).0, "active");
    }

    #[test]
    fn cadence_reads_either_syntax() {
        assert_eq!(cadence_of(&json!({ "cron": "0 3 * * *" })), "0 3 * * *");
        assert_eq!(cadence_of(&json!({ "every_ms": 300_000 })), "every 5m");
        assert_eq!(cadence_of(&json!({ "every_ms": 3_600_000 })), "every 1h");
        assert_eq!(cadence_of(&json!({ "every_ms": 45_000 })), "every 45s");
        assert_eq!(cadence_of(&json!({})), "-");
    }

    #[test]
    fn record_literals_quote_names_with_hyphens() {
        // A bare `_00_schedule:game-sync` would be parsed as a subtraction.
        assert_eq!(
            crate::schedule_sync::record_literal("game-sync"),
            "_00_schedule:⟨game-sync⟩"
        );
    }

    #[test]
    fn every_run_status_has_a_distinct_glyph() {
        let mut seen = std::collections::HashSet::new();
        for status in ["success", "failed", "running", "skipped", "replaced", "killed"] {
            assert!(seen.insert(run_status_style(status).0), "duplicate glyph for {status}");
        }
    }
}

/// Every statement `spky schedules` and `spky workflows` can emit, parsed by the
/// real SurrealQL parser the CLI already links for schema work.
///
/// The docker-gated `schedules_e2e` test proves these run correctly against a
/// live v3 server, but that test is opt-in. This one runs on every `cargo test`
/// and catches the failure mode that hurts most: a statement that is syntactically
/// broken (a stray brace, an unquoted hyphenated record key) and therefore fails
/// only in an operator's hands.
#[cfg(test)]
mod statement_syntax_tests {
    use super::SCHEDULE_FIELDS;
    use surrealdb_core::dbs::Capabilities;
    use surrealdb_core::syn::parse_with_capabilities;

    fn assert_parses(label: &str, sql: &str) {
        let capabilities = Capabilities::all();
        if let Err(e) = parse_with_capabilities(sql, &capabilities) {
            panic!("{label} does not parse:\n  {sql}\n  → {e}");
        }
    }

    /// A hyphenated name is the common case (`game-sync`, `nightly-cleanup`), and
    /// a bare `_00_schedule:game-sync` parses as a subtraction, so the ⟨⟩ quoting
    /// in `record_literal` is load-bearing.
    #[test]
    fn operator_writes_parse_for_hyphenated_names() {
        let id = crate::schedule_sync::record_literal("game-sync");
        assert_parses("pause", &format!("UPDATE {id} SET paused = true;"));
        assert_parses("resume", &format!("UPDATE {id} SET paused = false;"));
        assert_parses("trigger", &format!("UPDATE {id} SET trigger_requested_at = time::now();"));
        assert_parses("delete", &format!("DELETE {id};"));
        assert_parses(
            "sync upsert",
            &format!(
                "UPSERT {id} MERGE {{ \"name\": \"game-sync\", \"kind\": \"job\" }}; \
                 UPDATE {id} SET next_fire_at = NONE;"
            ),
        );
    }

    #[test]
    fn schedule_reads_parse() {
        assert_parses(
            "list",
            &format!("SELECT {SCHEDULE_FIELDS} FROM _00_schedule ORDER BY name;"),
        );
        assert_parses(
            "get",
            &format!("SELECT {SCHEDULE_FIELDS} FROM _00_schedule WHERE name = 'game-sync' LIMIT 1;"),
        );
        assert_parses(
            "last run per schedule",
            "SELECT schedule_name, status, type::string(fire_at) AS fire_at \
             FROM _00_schedule_run ORDER BY fire_at DESC LIMIT 500;",
        );
        assert_parses(
            "recent runs for one schedule",
            "SELECT status, key, type::string(fire_at) AS fire_at, job_id \
             FROM _00_schedule_run WHERE schedule_name = 'game-sync' ORDER BY fire_at DESC LIMIT 10;",
        );
        assert_parses(
            "runs with both filters",
            "SELECT schedule_name, kind, status, key, trigger, job_id, \
             type::string(fire_at) AS fire_at, type::string(finished_at) AS finished_at \
             FROM _00_schedule_run WHERE schedule_name = 'game-sync' AND status = 'failed' \
             ORDER BY fire_at DESC LIMIT 30;",
        );
        assert_parses("stored hashes", "SELECT name, spec_hash FROM _00_schedule;");
    }

    /// Every datetime the CLI reads is wrapped in `type::string(...)`: a raw
    /// datetime column does not deserialize into the `String` these views expect.
    #[test]
    fn datetimes_are_read_as_strings() {
        for field in ["next_fire_at", "last_fire_at", "trigger_requested_at"] {
            assert!(
                SCHEDULE_FIELDS.contains(&format!("type::string({field})")),
                "{field} must be projected through type::string"
            );
        }
    }

    /// SurrealDB v3 rejects `ORDER BY` on a column that isn't in the projection,
    /// so every ordered read has to select what it sorts by.
    #[test]
    fn ordered_reads_project_their_sort_key() {
        let ordered = [
            ("name", format!("SELECT {SCHEDULE_FIELDS} FROM _00_schedule ORDER BY name;")),
            (
                "fire_at",
                "SELECT schedule_name, status, type::string(fire_at) AS fire_at \
                 FROM _00_schedule_run ORDER BY fire_at DESC LIMIT 500;"
                    .to_string(),
            ),
        ];
        for (key, sql) in ordered {
            let projection = &sql[..sql.find(" FROM ").expect("has a FROM")];
            assert!(
                projection.contains(key),
                "ORDER BY {key} is not in the projection of:\n  {sql}"
            );
        }
    }
}
