//! The operator surface of the job, schedule and workflow planes: what the
//! admin API (`/admin/api`) reads and does.
//!
//! One implementation for every host of that API: the cluster scheduler and
//! the SSP node (a Cloudflare project has no scheduler). Each function runs
//! over [`ScheduleDb`] and answers in the JSON shape the dashboard renders, so
//! a host only parses its request and frames the answer. Anything a host does
//! differently (a background sampler, a pool machine owning a job) stays in
//! the host.
//!
//! Every name that lands in a statement is either a validated identifier or an
//! escaped string literal; table names additionally pass the
//! `_00_retention.job_tables` allowlist, because the handle behind this
//! surface can read every table in the database.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::db::{rows, ScheduleDb};
use crate::ids;
use crate::sql::{is_plain_identifier, release_schedule_key, LEASE_LIVE};
use crate::{ScheduleEngine, WorkflowOpError};

/// Cap on `?limit=`, so one request cannot ask the database for everything.
pub const MAX_LIMIT: usize = 500;
pub const DEFAULT_LIMIT: usize = 50;

/// Rows deleted per clear statement. Matches `spky jobs clear`: one bounded
/// statement per round trip, looped until a short batch comes back, so a
/// backlog of a million rows is not one transaction.
const CLEAR_BATCH: usize = 500;

/// Ceiling on clear batches per request, so the call cannot outlive the
/// operator's patience. The answer says whether more is left.
const CLEAR_MAX_BATCHES: usize = 40;

/// The dispatcher's fallback when a table has no `_00_job_policy` row.
/// Mirrors `ssp-node`'s `DEFAULT_CONCURRENCY`.
pub const DEFAULT_CONCURRENCY: i64 = 1;

/// The outcome counters a run can land in, in the order the rollup and the
/// dashboard render them.
pub const RUN_OUTCOMES: [&str; 5] = ["success", "failed", "skipped", "replaced", "killed"];

/// Listing projection. Without `payload` and `result`: a payload is uncapped
/// and a result is up to 64 KiB, and fifty of each would make the list the
/// most expensive request in the dashboard. Both are on the detail.
pub const JOB_FIELDS: &str = "type::string(id) AS id, record::id(id) AS key, status, path, \
     retries, max_retries, retry_strategy, assignee, timeout, delay, errors, \
     type::string(lease_until) AS lease_until, \
     type::string(created_at) AS created_at, \
     type::string(updated_at) AS updated_at";

const RUN_FIELDS: &str = "type::string(id) AS id, workflow_name, schedule_name, status, \
     kill_requested, error, trigger, retry_count, \
     type::string(rerun_of) AS rerun_of, \
     type::string(created_at) AS created_at, \
     type::string(updated_at) AS updated_at, \
     type::string(finished_at) AS finished_at";

const SCHEDULE_FIELDS: &str = "name, kind, cron, every_ms, timezone, paused, config_disabled, \
     concurrency, max_retries, retry_strategy, timeout, target_table, path, \
     for_each, for_each_key, history_mode, last_run_status, last_error, \
     type::string(next_fire_at) AS next_fire_at, \
     type::string(last_fire_at) AS last_fire_at, \
     type::string(last_run_at) AS last_run_at, \
     type::string(created_at) AS created_at, \
     type::string(updated_at) AS updated_at";

/// A failed admin call: the HTTP status and the message the dashboard shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminError {
    pub status: u16,
    pub message: String,
}

impl AdminError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self { status, message: message.into() }
    }
}

pub type AdminResult<T> = Result<T, AdminError>;

/// Escape a value for a single-quoted SurrealQL string literal.
pub fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// The rows of one statement, or a 502 naming the database error. A table
/// that does not exist yet reads as empty: a project with no schedules has no
/// `_00_schedule` table, and its Schedules page is empty, not broken.
async fn query_rows(db: &dyn ScheduleDb, surql: &str) -> AdminResult<Vec<Value>> {
    match db.query(surql, &[]).await {
        Ok(results) => Ok(rows(results)),
        Err(e) if is_missing_table(&e.to_string()) => Ok(Vec::new()),
        Err(e) => Err(AdminError::new(502, format!("Database error: {e}"))),
    }
}

/// SurrealDB 3's refusal to read a table nobody defined.
fn is_missing_table(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    m.contains("table") && m.contains("does not exist")
}

/// The engine's clock: `std::time` panics on wasm32, chrono's does not.
fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

fn nonempty(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|s| !s.is_empty())
}

/// The value of `key` in decoded query pairs, if present and not empty.
fn pair<'a>(pairs: &'a [(String, String)], key: &str) -> Option<String> {
    pairs.iter().find(|(k, v)| k == key && !v.is_empty()).map(|(_, v)| v.clone())
}

// =============================================================
// Origin
// =============================================================

/// Which part of the system created a job. It is in the id: `schedule_core::ids`
/// mints `sch_<run key>` for a schedule fire and `wf_<run key>_<step>` for a
/// workflow step, and the backend SDK mints `dwf_<wake token>` for a run of a
/// durable workflow instance; anything else was created by the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    Schedule,
    Workflow,
    Durable,
    App,
}

impl Origin {
    pub fn as_str(self) -> &'static str {
        match self {
            Origin::Schedule => "schedule",
            Origin::Workflow => "workflow",
            Origin::Durable => "durable",
            Origin::App => "app",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "schedule" => Some(Origin::Schedule),
            "workflow" => Some(Origin::Workflow),
            "durable" => Some(Origin::Durable),
            "app" => Some(Origin::App),
            _ => None,
        }
    }

    /// The SurrealQL predicate selecting this class. `record::id(id)` is the
    /// bare key, so the prefix test does not need the table's name.
    fn predicate(self) -> &'static str {
        match self {
            Origin::Schedule => "string::starts_with(record::id(id), 'sch_')",
            Origin::Workflow => "string::starts_with(record::id(id), 'wf_')",
            Origin::Durable => "string::starts_with(record::id(id), 'dwf_')",
            Origin::App => {
                "!string::starts_with(record::id(id), 'sch_') \
                 AND !string::starts_with(record::id(id), 'wf_') \
                 AND !string::starts_with(record::id(id), 'dwf_')"
            }
        }
    }
}

/// Classify a job by the key the engine minted for it. Prefix only: the
/// back-link is the confirmation, and a job whose run was pruned must still
/// read as scheduled work.
pub fn origin_of(key: &str) -> Origin {
    if key.starts_with("sch_") {
        Origin::Schedule
    } else if key.starts_with("wf_") {
        Origin::Workflow
    } else if key.starts_with("dwf_") {
        Origin::Durable
    } else {
        Origin::App
    }
}

// =============================================================
// Job snapshot
// =============================================================

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct StatusCounts {
    pub pending: i64,
    pub processing: i64,
    pub success: i64,
    pub failed: i64,
    /// A status the schema's ASSERT does not allow. Counted rather than
    /// dropped: a row that should be impossible is exactly what you want to see.
    pub other: i64,
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

    fn merge(&mut self, other: &StatusCounts) {
        self.pending += other.pending;
        self.processing += other.processing;
        self.success += other.success;
        self.failed += other.failed;
        self.other += other.other;
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TableStat {
    pub table: String,
    pub counts: StatusCounts,
    /// `processing` rows whose lease is still live: work really in flight.
    pub in_flight: i64,
    /// `processing` rows whose lease has expired: nobody is working on them,
    /// and the next recovery sweep may reclaim them.
    pub stalled: i64,
    /// The `_00_job_policy` ceiling, or the dispatcher's default.
    pub concurrency: i64,
    /// Jobs that succeeded in the last minute.
    pub throughput_1m: i64,
    pub oldest_pending: Option<String>,
    /// This table's query failed. Per table, so one missing table does not
    /// blank the whole page.
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    /// The merged recent page, newest activity first, with origins attached.
    pub jobs: Vec<Value>,
    pub tables: Vec<TableStat>,
    pub counts: StatusCounts,
    pub in_flight: i64,
    pub stalled: i64,
    pub throughput_1m: i64,
    pub oldest_pending: Option<String>,
    pub taken_at_ms: u64,
    /// Every configured table failed to answer. Said out loud, because zeros
    /// would read as a healthy empty queue.
    pub blind: bool,
}

impl Snapshot {
    /// The totals block of a listing and of the overview tile. `samples` is
    /// the host's sparkline (empty where nothing samples in the background).
    pub fn totals(&self, samples: Value) -> Value {
        json!({
            "counts": self.counts,
            "in_flight": self.in_flight,
            "stalled": self.stalled,
            "throughput_1m": self.throughput_1m,
            "oldest_pending": self.oldest_pending,
            "tables": self.tables.len(),
            "blind": self.blind,
            "samples": samples,
            "ready": true,
        })
    }

    /// The `GET /jobs` body for this snapshot, unfiltered.
    pub fn payload(&self, limit: usize, samples: Value, live: bool) -> Value {
        let mut jobs = self.jobs.clone();
        jobs.truncate(limit);
        json!({
            "jobs": jobs,
            "returned": jobs.len(),
            "limit": limit,
            "totals": self.totals(samples),
            "tables": self.tables,
            "sampled_at_ms": self.taken_at_ms,
            "live": live,
        })
    }
}

fn int_of(v: Option<&Value>) -> i64 {
    match v {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::Object(map)) => map.get("count").and_then(Value::as_i64).unwrap_or(0),
        _ => 0,
    }
}

fn count_row(rows: &[Value]) -> i64 {
    rows.first().map(|v| int_of(v.get("n"))).unwrap_or(0)
}

fn statement_rows(results: &mut [Value], idx: usize) -> Vec<Value> {
    match results.get_mut(idx).map(Value::take) {
        Some(Value::Array(rows)) => rows,
        Some(Value::Null) | None => Vec::new(),
        Some(other) => vec![other],
    }
}

/// The outbox tables this deployment owns: `_00_retention.job_tables`, which
/// `spky deploy` writes, and the allowlist for every table interpolation here.
pub async fn job_tables(db: &dyn ScheduleDb) -> AdminResult<Vec<String>> {
    let listed = query_rows(db, "SELECT job_tables FROM _00_retention LIMIT 1;")
        .await?
        .into_iter()
        .next()
        .and_then(|row| row.get("job_tables").and_then(Value::as_array).cloned())
        .unwrap_or_default();
    Ok(listed
        .iter()
        .filter_map(Value::as_str)
        .filter(|t| is_plain_identifier(t))
        .map(str::to_string)
        .collect())
}

/// `wanted` when it is one of `all`, every table when absent, a 404 otherwise.
fn pick_tables(all: Vec<String>, wanted: Option<&str>) -> AdminResult<Vec<String>> {
    match wanted {
        Some(wanted) if all.iter().any(|t| t == wanted) => Ok(vec![wanted.to_string()]),
        Some(wanted) => Err(AdminError::new(
            404,
            format!(
                "'{wanted}' is not an outbox table on this project. Deploy writes the list to \
                 `_00_retention.job_tables`."
            ),
        )),
        None => Ok(all),
    }
}

/// Each table's execution ceiling, from the row `spky deploy` upserts.
async fn job_limits(db: &dyn ScheduleDb) -> BTreeMap<String, i64> {
    query_rows(db, "SELECT type::string(id) AS id, concurrency FROM _00_job_policy;")
        .await
        .unwrap_or_default()
        .iter()
        .filter_map(|row| {
            let key = ids::Ref::parse(row.get("id").and_then(Value::as_str)?)?.key;
            Some((key, row.get("concurrency").and_then(Value::as_i64)?))
        })
        .collect()
}

/// Everything one table contributes to a snapshot, in one round trip.
///
/// The status counts cover in-flight work plus the last hour of terminal work,
/// not all of history: an all-time `GROUP BY status` is an unbounded aggregate,
/// and once retention keeps failures longer than successes an all-time ratio
/// reports a fail rate that climbs as successes age out.
async fn sample_table(
    db: &dyn ScheduleDb,
    table: &str,
    limit: usize,
    concurrency: i64,
) -> Result<(Vec<Value>, TableStat), String> {
    let surql = format!(
        "SELECT {JOB_FIELDS} FROM {table} ORDER BY updated_at DESC LIMIT {limit}; \
         SELECT status, count() AS n FROM {table} \
         WHERE status IN ['pending', 'processing'] OR updated_at > time::now() - 1h \
         GROUP BY status; \
         SELECT count() AS n FROM {table} \
         WHERE status = 'success' AND updated_at > time::now() - 1m GROUP ALL; \
         SELECT type::string(created_at) AS created_at FROM {table} \
         WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1; \
         SELECT count() AS n FROM {table} \
         WHERE status = 'processing' AND {LEASE_LIVE} GROUP ALL;"
    );
    let mut results = db.query(&surql, &[]).await.map_err(|e| e.to_string())?;

    let mut counts = StatusCounts::default();
    for row in statement_rows(&mut results, 1) {
        let status = row.get("status").and_then(Value::as_str).unwrap_or("");
        counts.add(status, int_of(row.get("n")));
    }
    let throughput_1m = count_row(&statement_rows(&mut results, 2));
    let oldest_pending = statement_rows(&mut results, 3)
        .first()
        .and_then(|v| v.get("created_at"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let in_flight = count_row(&statement_rows(&mut results, 4));
    let jobs = statement_rows(&mut results, 0)
        .into_iter()
        .map(|mut row| {
            row["table"] = json!(table);
            row
        })
        .collect();

    Ok((
        jobs,
        TableStat {
            table: table.to_string(),
            counts,
            in_flight,
            // Every `processing` row the live-lease count did not claim.
            // Derived, so the two numbers cannot disagree.
            stalled: (counts.processing - in_flight).max(0),
            concurrency,
            throughput_1m,
            oldest_pending,
            error: None,
        },
    ))
}

/// A SurrealQL array literal of quoted strings, for an `INSIDE` test.
fn id_list(ids: &[String]) -> String {
    let inner: Vec<String> = ids.iter().map(|id| format!("'{}'", esc(id))).collect();
    format!("[{}]", inner.join(", "))
}

/// Attach each job's origin, resolving the back-link where one survives, and
/// fold its attempt history into `last_error` and `attempts`.
///
/// Two queries for a whole page, not one per row: `_00_schedule_run.job_id`
/// and `_00_step_run.job_id` are indexed, so an `INSIDE` over the page's ids
/// is an index probe.
async fn attach_origins(db: &dyn ScheduleDb, jobs: &mut [Value]) {
    let mut scheduled = Vec::new();
    let mut stepped = Vec::new();
    for job in jobs.iter() {
        let (Some(id), Some(key)) = (
            job.get("id").and_then(Value::as_str),
            job.get("key").and_then(Value::as_str),
        ) else {
            continue;
        };
        match origin_of(key) {
            Origin::Schedule => scheduled.push(id.to_string()),
            Origin::Workflow => stepped.push(id.to_string()),
            Origin::Durable | Origin::App => {}
        }
    }

    let mut by_job: BTreeMap<String, Value> = BTreeMap::new();
    if !scheduled.is_empty() {
        let found = query_rows(
            db,
            &format!(
                "SELECT job_id, schedule_name, key, type::string(id) AS schedule_run, \
                 type::string(fire_at) AS fire_at \
                 FROM _00_schedule_run WHERE job_id INSIDE {};",
                id_list(&scheduled)
            ),
        )
        .await
        .unwrap_or_default();
        for row in found {
            let Some(job_id) = row.get("job_id").and_then(Value::as_str) else { continue };
            by_job.insert(
                job_id.to_string(),
                json!({
                    "kind": Origin::Schedule.as_str(),
                    "schedule": row.get("schedule_name"),
                    "schedule_run": row.get("schedule_run"),
                    "fire_at": row.get("fire_at"),
                    "key": row.get("key"),
                }),
            );
        }
    }
    if !stepped.is_empty() {
        let found = query_rows(
            db,
            &format!(
                "SELECT job_id, step, type::string(workflow_run) AS workflow_run \
                 FROM _00_step_run WHERE job_id INSIDE {};",
                id_list(&stepped)
            ),
        )
        .await
        .unwrap_or_default();
        for row in found {
            let Some(job_id) = row.get("job_id").and_then(Value::as_str) else { continue };
            by_job.insert(
                job_id.to_string(),
                json!({
                    "kind": Origin::Workflow.as_str(),
                    "workflow_run": row.get("workflow_run"),
                    "step": row.get("step"),
                }),
            );
        }
    }

    for job in jobs.iter_mut() {
        let id = job.get("id").and_then(Value::as_str).unwrap_or("").to_string();
        let kind = origin_of(job.get("key").and_then(Value::as_str).unwrap_or(""));
        // The resolved row when the run still exists, the bare class when it
        // does not. Never absent: the page groups on `origin.kind`.
        job["origin"] = by_job.remove(&id).unwrap_or_else(|| json!({ "kind": kind.as_str() }));
        // The whole attempt history belongs on the detail page; the list wants
        // the count and the last one.
        let errors = job.as_object_mut().and_then(|o| o.remove("errors"));
        let errors = errors.as_ref().and_then(Value::as_array);
        job["last_error"] = errors.and_then(|e| e.last()).cloned().unwrap_or(Value::Null);
        job["attempts"] = json!(errors.map(|e| e.len()).unwrap_or(0));
    }
}

/// Merge per-table pages into one, newest activity first. ISO-8601 strings
/// sort chronologically, which is why `updated_at` is projected as a string.
fn merge_pages(mut jobs: Vec<Value>, limit: usize) -> Vec<Value> {
    let key = |v: &Value| v.get("updated_at").and_then(Value::as_str).unwrap_or("").to_string();
    jobs.sort_by_key(|b| std::cmp::Reverse(key(b)));
    jobs.truncate(limit);
    jobs
}

/// Sample every table and fold the results. Shared by a background sampler
/// and an on-demand listing, so the two cannot disagree on what a total means.
pub async fn build_snapshot(db: &dyn ScheduleDb, tables: &[String], per_table: usize) -> Snapshot {
    let limits = job_limits(db).await;
    let mut jobs = Vec::new();
    let mut stats = Vec::new();
    let mut failures = 0usize;

    for table in tables {
        let concurrency = limits.get(table).copied().unwrap_or(DEFAULT_CONCURRENCY);
        match sample_table(db, table, per_table, concurrency).await {
            Ok((page, stat)) => {
                jobs.extend(page);
                stats.push(stat);
            }
            Err(message) => {
                tracing::warn!(table, error = %message, "Job table sample failed");
                failures += 1;
                stats.push(TableStat {
                    table: table.clone(),
                    counts: StatusCounts::default(),
                    in_flight: 0,
                    stalled: 0,
                    concurrency,
                    throughput_1m: 0,
                    oldest_pending: None,
                    error: Some(message),
                });
            }
        }
    }

    let mut counts = StatusCounts::default();
    let (mut in_flight, mut stalled, mut throughput_1m) = (0, 0, 0);
    let mut oldest_pending: Option<String> = None;
    for stat in &stats {
        counts.merge(&stat.counts);
        in_flight += stat.in_flight;
        stalled += stat.stalled;
        throughput_1m += stat.throughput_1m;
        if let Some(candidate) = stat.oldest_pending.as_ref() {
            oldest_pending = Some(match oldest_pending {
                Some(current) if current <= *candidate => current,
                _ => candidate.clone(),
            });
        }
    }

    let mut jobs = merge_pages(jobs, per_table);
    attach_origins(db, &mut jobs).await;

    Snapshot {
        jobs,
        tables: stats,
        counts,
        in_flight,
        stalled,
        throughput_1m,
        oldest_pending,
        taken_at_ms: now_ms(),
        // Not "no tables": a project with no outbox table is a valid, fully
        // visible state. Blind means every table there refused to answer.
        blind: !tables.is_empty() && failures == tables.len(),
    }
}

/// A snapshot of every outbox table, taken now.
pub async fn snapshot(db: &dyn ScheduleDb) -> AdminResult<Snapshot> {
    let tables = job_tables(db).await?;
    Ok(build_snapshot(db, &tables, DEFAULT_LIMIT).await)
}

// =============================================================
// Job listing, detail and clear
// =============================================================

/// `GET /jobs` filters.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct JobsQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    /// `schedule`, `workflow` or `app`.
    #[serde(default)]
    pub origin: Option<String>,
    /// Substring of the job's path or its id.
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl JobsQuery {
    pub fn from_pairs(pairs: &[(String, String)]) -> Self {
        Self {
            status: pair(pairs, "status"),
            table: pair(pairs, "table"),
            origin: pair(pairs, "origin"),
            q: pair(pairs, "q"),
            limit: pair(pairs, "limit").and_then(|s| s.parse().ok()),
        }
    }

    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }

    /// Whether this is exactly the unfiltered first page a snapshot answers.
    pub fn is_snapshot_shaped(&self) -> bool {
        nonempty(&self.status).is_none()
            && nonempty(&self.table).is_none()
            && nonempty(&self.origin).is_none()
            && nonempty(&self.q).is_none()
            && self.limit.unwrap_or(DEFAULT_LIMIT) <= DEFAULT_LIMIT
    }

    /// The origin filter, or a 400 for one that is not an origin.
    pub fn origin(&self) -> AdminResult<Option<Origin>> {
        match nonempty(&self.origin) {
            Some(raw) => Origin::parse(raw).map(Some).ok_or_else(|| {
                AdminError::new(400, format!("'{raw}' is not an origin. Use schedule, workflow or app."))
            }),
            None => Ok(None),
        }
    }

    /// The `WHERE` body for one table, or empty when nothing is filtered.
    ///
    /// Pushed into SurrealQL rather than applied to the page: filtering after
    /// the LIMIT lets a busy table answer an empty page while plainly holding
    /// matching jobs.
    fn where_clause(&self, origin: Option<Origin>) -> String {
        let mut filters: Vec<String> = Vec::new();
        if let Some(status) = nonempty(&self.status) {
            filters.push(format!("status = '{}'", esc(status)));
        }
        if let Some(origin) = origin {
            filters.push(format!("({})", origin.predicate()));
        }
        if let Some(q) = nonempty(&self.q) {
            filters.push(format!(
                "(string::contains(path ?? '', '{q}') OR string::contains(record::id(id), '{q}'))",
                q = esc(q)
            ));
        }
        if filters.is_empty() {
            String::new()
        } else {
            format!("WHERE {} ", filters.join(" AND "))
        }
    }
}

/// A filtered `GET /jobs`: a bounded query per table with the filter pushed
/// into SurrealQL. `totals` is the host's (a total that moved with the filter
/// would not be a total).
pub async fn list_jobs_filtered(db: &dyn ScheduleDb, q: &JobsQuery, totals: Value, tables: Value) -> AdminResult<Value> {
    let origin = q.origin()?;
    let limit = q.limit();
    let picked = pick_tables(job_tables(db).await?, nonempty(&q.table))?;

    let where_clause = q.where_clause(origin);
    let mut jobs = Vec::new();
    let mut errors = Vec::new();
    for table in &picked {
        let surql =
            format!("SELECT {JOB_FIELDS} FROM {table} {where_clause}ORDER BY updated_at DESC LIMIT {limit};");
        match query_rows(db, &surql).await {
            Ok(page) => jobs.extend(page.into_iter().map(|mut row| {
                row["table"] = json!(table);
                row
            })),
            Err(e) => errors.push(json!({ "table": table, "error": e.message })),
        }
    }
    if !picked.is_empty() && errors.len() == picked.len() {
        return Err(AdminError::new(
            502,
            "Every outbox table refused to answer; the listing would read as an empty queue",
        ));
    }

    let mut jobs = merge_pages(jobs, limit);
    attach_origins(db, &mut jobs).await;
    Ok(json!({
        "jobs": jobs,
        "returned": jobs.len(),
        "limit": limit,
        "totals": totals,
        "tables": tables,
        "table_errors": errors,
        "filtered": true,
        "live": false,
    }))
}

/// A `GET /jobs` answered entirely now, for a host with no background
/// sampler: the unfiltered page is the snapshot itself, a filtered one takes
/// its totals from the same snapshot.
pub async fn list_jobs(db: &dyn ScheduleDb, q: &JobsQuery) -> AdminResult<Value> {
    q.origin()?;
    let snap = snapshot(db).await?;
    if q.is_snapshot_shaped() {
        return Ok(snap.payload(q.limit(), json!([]), false));
    }
    let tables = serde_json::to_value(&snap.tables).unwrap_or(Value::Null);
    list_jobs_filtered(db, q, snap.totals(json!([])), tables).await
}

/// One job row with its whole attempt history (`errors`), for the detail page.
fn job_surql(table: &str, key: &str) -> String {
    format!(
        "SELECT type::string(id) AS id, record::id(id) AS key, status, path, payload, result, \
         errors, retries, max_retries, retry_strategy, assignee, timeout, delay, \
         type::string(lease_until) AS lease_until, \
         type::string(created_at) AS created_at, \
         type::string(updated_at) AS updated_at \
         FROM type::record('{}', '{}');",
        esc(table),
        esc(key)
    )
}

/// `GET /jobs/:id`. The table is checked against the allowlist before it is
/// read: a path segment must not choose an arbitrary table.
pub async fn job_detail(db: &dyn ScheduleDb, id: &str) -> AdminResult<Value> {
    let job = ids::Ref::parse(id).ok_or_else(|| AdminError::new(400, format!("'{id}' is not a record id")))?;
    pick_tables(job_tables(db).await?, Some(&job.table))?;

    let row = query_rows(db, &job_surql(&job.table, &job.key)).await?.into_iter().next();
    let Some(mut row) = row.filter(Value::is_object) else {
        return Err(AdminError::new(
            404,
            format!(
                "No job '{id}'. Terminal jobs are pruned on the project's retention window, so a \
                 run older than that keeps its step rows and loses its jobs."
            ),
        ));
    };
    row["table"] = json!(job.table);
    // `attach_origins` strips `errors` for the listing; the detail wants them.
    let errors = row.get("errors").cloned().unwrap_or(Value::Null);
    let mut one = [row];
    attach_origins(db, &mut one).await;
    let [mut row] = one;
    row["errors"] = errors;
    Ok(json!({ "job": row }))
}

/// `POST /jobs/clear` body.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct ClearBody {
    /// One table, or every outbox table when absent.
    #[serde(default)]
    pub table: Option<String>,
    /// Also delete `pending` rows. `processing` is spared either way.
    #[serde(default)]
    pub all: bool,
}

/// `POST /jobs/clear`. Batched: deleting a backlog in one transaction is the
/// shape that stalls SurrealDB for minutes. `processing` is never deleted, at
/// either width: its lease holder is working on it.
pub async fn jobs_clear(db: &dyn ScheduleDb, body: &ClearBody) -> AdminResult<Value> {
    let tables = pick_tables(job_tables(db).await?, nonempty(&body.table))?;
    let cond = if body.all { "status != 'processing'" } else { "status IN ['success', 'failed']" };
    let mut cleared: BTreeMap<String, usize> = BTreeMap::new();
    let mut more = false;

    for table in &tables {
        // One block per batch so the answer is a single count; projecting `id`
        // keeps it small (a row carries an uncapped payload).
        let surql = format!(
            "RETURN {{ \
             LET $doomed = (SELECT id FROM {table} WHERE {cond} LIMIT {CLEAR_BATCH}); \
             DELETE $doomed.id; \
             RETURN count($doomed); \
             }};"
        );
        let mut removed = 0usize;
        for round in 0..CLEAR_MAX_BATCHES {
            let batch = db
                .query(&surql, &[])
                .await
                .map_err(|e| AdminError::new(502, format!("Database error: {e}")))?
                .first()
                .and_then(Value::as_u64)
                .unwrap_or(0) as usize;
            removed += batch;
            if batch < CLEAR_BATCH {
                break;
            }
            if round + 1 == CLEAR_MAX_BATCHES {
                more = true;
            }
        }
        cleared.insert(table.clone(), removed);
    }

    let total: usize = cleared.values().sum();
    Ok(json!({ "cleared": cleared, "total": total, "more": more }))
}

// =============================================================
// Workflow runs
// =============================================================

/// `GET /workflows/runs` filters.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct RunsQuery {
    #[serde(default)]
    pub name: Option<String>,
    /// The owning schedule, which may drive a workflow of another name.
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    /// Only the reruns of one run, for the provenance panel on its page.
    #[serde(default)]
    pub rerun_of: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl RunsQuery {
    pub fn from_pairs(pairs: &[(String, String)]) -> Self {
        Self {
            name: pair(pairs, "name"),
            schedule: pair(pairs, "schedule"),
            status: pair(pairs, "status"),
            rerun_of: pair(pairs, "rerun_of"),
            limit: pair(pairs, "limit").and_then(|s| s.parse().ok()),
        }
    }

    pub fn surql(&self) -> String {
        let mut filters = Vec::new();
        if let Some(name) = nonempty(&self.name) {
            filters.push(format!("workflow_name = '{}'", esc(name)));
        }
        if let Some(schedule) = nonempty(&self.schedule) {
            filters.push(format!("schedule_name = '{}'", esc(schedule)));
        }
        if let Some(status) = nonempty(&self.status) {
            filters.push(format!("status = '{}'", esc(status)));
        }
        if let Some(src) = nonempty(&self.rerun_of) {
            // A record link; compared as a string so the caller passes the id
            // exactly as the list handed it out.
            filters.push(format!("type::string(rerun_of) = '{}'", esc(src)));
        }
        let where_clause =
            if filters.is_empty() { String::new() } else { format!("WHERE {} ", filters.join(" AND ")) };
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
        format!("SELECT {RUN_FIELDS} FROM _00_workflow_run {where_clause}ORDER BY created_at DESC LIMIT {limit};")
    }
}

/// `GET /workflows/runs`
pub async fn list_runs(db: &dyn ScheduleDb, q: &RunsQuery) -> AdminResult<Value> {
    Ok(json!({ "runs": query_rows(db, &q.surql()).await? }))
}

/// `GET /workflows/runs/:id`: the run and its step rows.
pub async fn run_detail(db: &dyn ScheduleDb, id: &str) -> AdminResult<Value> {
    let run = query_rows(
        db,
        &format!(
            "SELECT {RUN_FIELDS}, dag, input, target_table \
             FROM _00_workflow_run WHERE type::string(id) = '{}' LIMIT 1;",
            esc(id)
        ),
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| {
        AdminError::new(
            404,
            format!(
                "No workflow run '{id}'. A successful run leaves no rows behind on a schedule \
                 with `history: failures-only`."
            ),
        )
    })?;

    // `started_at` is when the step's job was created; `created_at` is when
    // the row was made, which for a step deep in a DAG is the run's own start.
    // The timeline needs both: the truth, and the fallback for older rows.
    let steps = query_rows(
        db,
        &format!(
            "SELECT step, depends_on, status, job_id, output, error, dispatch_attempts, \
             type::string(created_at) AS created_at, \
             type::string(started_at) AS started_at, \
             type::string(finished_at) AS finished_at \
             FROM _00_step_run WHERE type::string(workflow_run) = '{}';",
            esc(id)
        ),
    )
    .await?;
    Ok(json!({ "run": run, "steps": steps }))
}

/// A workflow run id from a path segment, or a 400.
pub fn parse_run_id(id: &str) -> AdminResult<ids::Ref> {
    match ids::Ref::parse(id) {
        Some(r) if r.table == ids::WORKFLOW_RUN => Ok(r),
        Some(r) => Err(AdminError::new(400, format!("'{id}' is a {} record, not a workflow run", r.table))),
        None => Err(AdminError::new(400, format!("'{id}' is not a record id"))),
    }
}

/// The status and message for a failed workflow action.
pub fn op_error(e: WorkflowOpError) -> AdminError {
    match e {
        WorkflowOpError::NotFound(id) => AdminError::new(404, format!("No workflow run {id}")),
        WorkflowOpError::BadDag(msg) => AdminError::new(422, format!("Frozen DAG is invalid: {msg}")),
        WorkflowOpError::Db(err) => {
            let unknown_field = err
                .downcast_ref::<crate::ScheduleDbError>()
                .map(|d| d.is_unknown_field())
                .unwrap_or_else(|| err.to_string().contains("found no field"));
            if unknown_field {
                AdminError::new(409, "The schedule tables predate this action; run `spky deploy` to migrate them")
            } else {
                AdminError::new(502, format!("Database error: {err:#}"))
            }
        }
        // Everything else is a state the operator can read and act on.
        other => AdminError::new(409, other.to_string()),
    }
}

/// `POST /workflows/runs/:id/cancel` (answers 202).
pub async fn cancel_run(engine: &ScheduleEngine, id: &str) -> AdminResult<Value> {
    let out = engine.cancel_workflow(&parse_run_id(id)?).await.map_err(op_error)?;
    Ok(json!({ "run": out.run.as_string(), "status": out.status }))
}

/// `POST /workflows/runs/:id/rerun` (answers 201).
pub async fn rerun_run(engine: &ScheduleEngine, id: &str) -> AdminResult<Value> {
    let out = engine.rerun_workflow(&parse_run_id(id)?, chrono::Utc::now()).await.map_err(op_error)?;
    Ok(json!({ "run": out.run.as_string(), "rerun_of": out.rerun_of.as_string() }))
}

/// `POST /workflows/runs/:id/retry`
pub async fn retry_run(engine: &ScheduleEngine, id: &str) -> AdminResult<Value> {
    let out = engine.retry_workflow(&parse_run_id(id)?).await.map_err(op_error)?;
    Ok(json!({
        "run": out.run.as_string(),
        "retry_count": out.retry_count,
        "reset": out.reset,
        "kept": out.kept,
    }))
}

// =============================================================
// Durable workflow instances
// =============================================================
//
// Instances of a backend's durable workflow classes (`@spooky-sync/backend`,
// docs/durable-steps.md). The engine lives in the backend; the operator surface
// only reads the rows and moves the instance's state the way the SDK does
// (`terminate()`, a new wake token), so the two can never disagree about what a
// cancelled instance looks like.

const INSTANCE_FIELDS: &str = "record::id(id) AS id, binding, class, status, outbox, \
     steps_done, current_step, error, \
     type::string(wake_at) AS wake_at, type::string(lease_until) AS lease_until, \
     type::string(created_at) AS created_at, type::string(started_at) AS started_at, \
     type::string(updated_at) AS updated_at, type::string(finished_at) AS finished_at";

/// The statuses an instance can still move from.
const DURABLE_ACTIVE: &str = "['queued', 'running', 'waiting']";

/// `GET /durable/instances` filters.
#[derive(Debug, Default, Clone, Deserialize)]
pub struct InstancesQuery {
    #[serde(default)]
    pub binding: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

impl InstancesQuery {
    pub fn from_pairs(pairs: &[(String, String)]) -> Self {
        Self {
            binding: pair(pairs, "binding"),
            status: pair(pairs, "status"),
            limit: pair(pairs, "limit").and_then(|s| s.parse().ok()),
        }
    }

    pub fn surql(&self) -> String {
        let mut filters = Vec::new();
        if let Some(binding) = nonempty(&self.binding) {
            filters.push(format!("binding = '{}'", esc(binding)));
        }
        if let Some(status) = nonempty(&self.status) {
            filters.push(format!("status = '{}'", esc(status)));
        }
        let where_clause =
            if filters.is_empty() { String::new() } else { format!("WHERE {} ", filters.join(" AND ")) };
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
        format!("SELECT {INSTANCE_FIELDS} FROM _00_wf_instance {where_clause}ORDER BY created_at DESC LIMIT {limit};")
    }
}

/// An instance's key from what an operator passes: the bare id `create()`
/// returned, or the record id (`_00_wf_instance:abc`, `_00_wf_instance:⟨a-b⟩`).
pub fn instance_key(id: &str) -> String {
    let key = id.trim();
    let key = key.strip_prefix("_00_wf_instance:").unwrap_or(key);
    let key = key
        .strip_prefix('⟨')
        .and_then(|k| k.strip_suffix('⟩'))
        .or_else(|| key.strip_prefix('`').and_then(|k| k.strip_suffix('`')))
        .unwrap_or(key);
    key.to_string()
}

fn instance_record(id: &str) -> String {
    format!("type::record('_00_wf_instance', '{}')", esc(&instance_key(id)))
}

/// `GET /durable/instances`
pub async fn list_instances(db: &dyn ScheduleDb, q: &InstancesQuery) -> AdminResult<Value> {
    Ok(json!({ "instances": query_rows(db, &q.surql()).await? }))
}

/// `GET /durable/instances/:id`: the instance with its params and output, its
/// steps in the order they first ran, and the events still waiting for a step.
pub async fn instance_detail(db: &dyn ScheduleDb, id: &str) -> AdminResult<Value> {
    let rec = instance_record(id);
    let instance = query_rows(db, &format!("SELECT {INSTANCE_FIELDS}, params, output FROM {rec};"))
        .await?
        .into_iter()
        .next()
        .filter(Value::is_object)
        .ok_or_else(|| AdminError::new(404, format!("No durable workflow instance '{id}'")))?;
    let steps = query_rows(
        db,
        &format!(
            "SELECT name, n, kind, status, attempts, output, error, event_type, \
             type::string(next_at) AS next_at, type::string(started_at) AS started_at, \
             type::string(finished_at) AS finished_at, started_at AS started \
             FROM _00_wf_step WHERE instance = {rec} ORDER BY started ASC;"
        ),
    )
    .await?;
    let events = query_rows(
        db,
        &format!(
            "SELECT type, step, type::string(created_at) AS created_at FROM _00_wf_event \
             WHERE instance = {rec} ORDER BY created_at ASC LIMIT {MAX_LIMIT};"
        ),
    )
    .await?;
    Ok(json!({ "instance": instance, "steps": steps, "events": events }))
}

/// `POST /durable/instances/:id/cancel`: `terminate()`. The epoch moves, so a run
/// holding the instance stops at its next write; the token moves, so no queued
/// job can run it again.
pub async fn cancel_instance(db: &dyn ScheduleDb, id: &str) -> AdminResult<Value> {
    let rec = instance_record(id);
    let out = query_rows(
        db,
        &format!(
            "RETURN {{ \
             LET $done = (UPDATE {rec} SET status = 'terminated', lease_epoch = (lease_epoch ?? 0) + 1, \
             lease_until = NONE, wake = rand::string(32), wake_at = NONE, current_step = NONE, \
             finished_at = time::now(), updated_at = time::now() \
             WHERE status INSIDE {DURABLE_ACTIVE} RETURN VALUE status); \
             RETURN {{ done: array::len($done) > 0, status: (SELECT VALUE status FROM ONLY {rec}) }}; \
             }};"
        ),
    )
    .await?;
    answer_transition(id, out, "terminated")
}

/// `POST /durable/instances/:id/retry`: hand an active instance a new token and
/// a job due now, in one statement (a block runs in one transaction). The way out when its outbox job gave up (the backend was down
/// for longer than the job's retries): the instance is still `running` or
/// `waiting`, and nothing would ever run it again. A live run is left alone: the
/// new job answers 503 until that run's lease is free.
pub async fn retry_instance(db: &dyn ScheduleDb, id: &str) -> AdminResult<Value> {
    let rec = instance_record(id);
    let key = esc(&instance_key(id));
    let out = query_rows(
        db,
        &format!(
            "RETURN {{ \
             LET $cur = (SELECT status, outbox, path FROM ONLY {rec}); \
             LET $active = $cur != NONE AND $cur.status INSIDE {DURABLE_ACTIVE}; \
             LET $wake = rand::string(32); \
             IF $active {{ \
               UPDATE {rec} SET wake = $wake, wake_at = time::now(), updated_at = time::now(); \
               CREATE type::record($cur.outbox, string::concat('dwf_', $wake)) CONTENT {{ \
                 path: $cur.path, payload: {{ instance: '{key}', wake: $wake }}, delay: 0, \
                 max_retries: 10, retry_strategy: 'exponential' \
               }}; \
             }}; \
             RETURN {{ done: $active, status: $cur.status }}; \
             }};"
        ),
    )
    .await?;
    answer_transition(id, out, "retried")
}

/// The answer to a state change: done, already finished (409) or missing (404).
fn answer_transition(id: &str, rows: Vec<Value>, verb: &str) -> AdminResult<Value> {
    let out = rows.into_iter().last().unwrap_or(Value::Null);
    let status = out.get("status").cloned().unwrap_or(Value::Null);
    if status.is_null() {
        return Err(AdminError::new(404, format!("No durable workflow instance '{id}'")));
    }
    if out.get("done").and_then(Value::as_bool) != Some(true) {
        return Err(AdminError::new(409, format!("Instance '{id}' is already {status}")));
    }
    Ok(json!({ "instance": instance_key(id), "status": status, "action": verb }))
}

// =============================================================
// Schedules
// =============================================================

/// `GET /schedules`
pub async fn list_schedules(db: &dyn ScheduleDb) -> AdminResult<Value> {
    let schedules = query_rows(db, &format!("SELECT {SCHEDULE_FIELDS} FROM _00_schedule ORDER BY name;")).await?;
    Ok(json!({ "schedules": schedules }))
}

/// `GET /schedules/:name`: the schedule, its recent runs, an hourly outcome
/// tally and the keys it has quarantined.
pub async fn schedule_detail(db: &dyn ScheduleDb, name: &str) -> AdminResult<Value> {
    let schedule = query_rows(
        db,
        &format!("SELECT {SCHEDULE_FIELDS} FROM _00_schedule WHERE name = '{}' LIMIT 1;", esc(name)),
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| AdminError::new(404, format!("No schedule named '{name}'")))?;

    let runs = query_rows(
        db,
        &format!(
            "SELECT type::string(id) AS id, schedule_name, key, kind, status, trigger, job_id, \
             type::string(workflow_run) AS workflow_run, error, \
             type::string(fire_at) AS fire_at, \
             type::string(created_at) AS created_at, \
             type::string(finished_at) AS finished_at \
             FROM _00_schedule_run WHERE schedule_name = '{}' \
             ORDER BY created_at DESC LIMIT 25;",
            esc(name)
        ),
    )
    .await?;
    let rollup = schedule_tally(db, name, 48).await?;
    // Empty for a schedule that never set `quarantineAfter`.
    let quarantined = query_rows(
        db,
        &format!(
            "SELECT key, consecutive_failures, type::string(quarantined_at) AS quarantined_at \
             FROM _00_schedule_key WHERE schedule_name = '{}' AND quarantined_at != NONE \
             AND consecutive_failures > 0;",
            esc(name)
        ),
    )
    .await?;
    Ok(json!({ "schedule": schedule, "runs": runs, "rollup": rollup, "quarantined": quarantined }))
}

/// The hourly outcome tally for a schedule: the rollup plus the runs not yet
/// pruned. `_00_run_rollup` is folded by the prune pass alone, and retention
/// is routinely asymmetric, so the rollup alone reads `failed: 0` for weeks on
/// a visibly failing schedule. A run row is either still present or folded,
/// never both, so the sum is exact.
async fn schedule_tally(db: &dyn ScheduleDb, name: &str, limit: usize) -> AdminResult<Vec<Value>> {
    let statuses = RUN_OUTCOMES.map(|s| format!("'{s}'")).join(", ");
    let pruned = query_rows(
        db,
        &format!(
            "SELECT type::string(bucket) AS bucket, success, failed, skipped, replaced, killed \
             FROM _00_run_rollup WHERE scope = 'schedule' AND name = '{}';",
            esc(name)
        ),
    )
    .await?;
    // `time::floor` to the hour the prune pass buckets by, so a half-pruned
    // bucket adds up instead of splitting in two.
    let live = query_rows(
        db,
        &format!(
            "SELECT type::string(time::floor(fire_at, 1h)) AS bucket, status, count() AS n \
             FROM _00_schedule_run \
             WHERE schedule_name = '{}' AND status IN [{statuses}] \
             GROUP BY bucket, status;",
            esc(name)
        ),
    )
    .await?;
    Ok(merge_tally(&pruned, &live, limit))
}

/// Fold the pruned counters and the surviving rows into hourly buckets,
/// newest first.
pub fn merge_tally(pruned: &[Value], live: &[Value], limit: usize) -> Vec<Value> {
    let mut buckets: BTreeMap<String, [i64; 5]> = BTreeMap::new();
    for row in pruned {
        let Some(bucket) = row["bucket"].as_str() else { continue };
        let slot = buckets.entry(bucket.to_string()).or_default();
        for (i, outcome) in RUN_OUTCOMES.iter().enumerate() {
            slot[i] += row[*outcome].as_i64().unwrap_or(0);
        }
    }
    for row in live {
        let (Some(bucket), Some(status)) = (row["bucket"].as_str(), row["status"].as_str()) else { continue };
        let Some(i) = RUN_OUTCOMES.iter().position(|o| *o == status) else { continue };
        buckets.entry(bucket.to_string()).or_default()[i] += row["n"].as_i64().unwrap_or(0);
    }
    buckets
        .into_iter()
        .rev()
        .take(limit)
        .map(|(bucket, c)| {
            json!({
                "bucket": bucket,
                "success": c[0],
                "failed": c[1],
                "skipped": c[2],
                "replaced": c[3],
                "killed": c[4],
            })
        })
        .collect()
}

/// `_00_schedule:⟨name⟩`, the literal the CLI writes. The ⟨⟩ form quotes any
/// key: schedule names carry hyphens, and a bare one parses as a subtraction.
fn schedule_literal(name: &str) -> String {
    format!("_00_schedule:⟨{}⟩", name.replace('⟩', ""))
}

async fn load_schedule(db: &dyn ScheduleDb, name: &str) -> AdminResult<Value> {
    query_rows(
        db,
        &format!("SELECT name, paused, config_disabled FROM _00_schedule WHERE name = '{}' LIMIT 1;", esc(name)),
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| AdminError::new(404, format!("No schedule named '{name}'")))
}

/// `POST /schedules/:name/pause` and `/resume`.
pub async fn set_paused(db: &dyn ScheduleDb, name: &str, paused: bool) -> AdminResult<Value> {
    load_schedule(db, name).await?;
    query_rows(db, &format!("UPDATE {} SET paused = {paused};", schedule_literal(name))).await?;
    Ok(json!({ "name": name, "paused": paused }))
}

/// `POST /schedules/:name/release` body.
#[derive(Debug, Clone, Deserialize)]
pub struct ReleaseBody {
    pub key: String,
}

/// `POST /schedules/:name/release`: forget one quarantined key's failure
/// streak, which is the release (the gate derives quarantine from the count).
pub async fn release_key(db: &dyn ScheduleDb, name: &str, key: &str) -> AdminResult<Value> {
    load_schedule(db, name).await?;
    query_rows(db, &release_schedule_key(&ids::schedule_key(name, key))).await?;
    Ok(json!({ "name": name, "key": key, "released": true }))
}

/// `POST /schedules/:name/trigger`: queue a fire for the next sweep.
pub async fn trigger(db: &dyn ScheduleDb, name: &str) -> AdminResult<Value> {
    let schedule = load_schedule(db, name).await?;
    // Pause wins over a queued trigger in the engine, so accepting one here
    // would queue a fire that silently never happens.
    if schedule["paused"].as_bool() == Some(true) {
        return Err(AdminError::new(409, format!("Schedule '{name}' is paused; resume it before triggering")));
    }
    if schedule["config_disabled"].as_bool() == Some(true) {
        return Err(AdminError::new(
            409,
            format!("Schedule '{name}' is disabled in its config; enable it and run `spky schedules sync`"),
        ));
    }
    let updated = query_rows(
        db,
        &format!(
            "UPDATE {} SET trigger_requested_at = time::now() \
             RETURN type::string(trigger_requested_at) AS trigger_requested_at;",
            schedule_literal(name)
        ),
    )
    .await?;
    let at = updated.first().and_then(|r| r["trigger_requested_at"].as_str()).map(str::to_string);
    Ok(json!({ "name": name, "triggered_at": at }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn esc_neutralises_quotes_and_backslashes() {
        assert_eq!(esc("o'brien"), "o\\'brien");
        assert_eq!(esc(r"back\slash"), r"back\\slash");
    }

    #[test]
    fn the_job_query_carries_the_attempt_history_and_escapes() {
        let sql = job_surql("job", "sch_game_sync_1788364742716_582ceccbb9aa");
        assert!(sql.contains("errors"), "{sql}");
        assert!(sql.contains("type::record('job', 'sch_game_sync_1788364742716_582ceccbb9aa')"), "{sql}");
        assert!(job_surql("job", "o'brien").contains("'o\\'brien'"));
    }

    #[test]
    fn origin_comes_from_the_minted_prefix() {
        let run = ids::run_key("game-sync", 1_769_337_000_000, "");
        assert_eq!(origin_of(&ids::job("job", &run).key), Origin::Schedule);
        assert_eq!(origin_of(&ids::step_job("job", &run, "extract").key), Origin::Workflow);
        assert_eq!(origin_of(&ids::step_job_attempt("job", &run, "extract", 2).key), Origin::Workflow);
        assert_eq!(origin_of("send-welcome-email"), Origin::App);
    }

    #[test]
    fn query_pairs_build_the_filters() {
        let pairs = vec![
            ("status".to_string(), "failed".to_string()),
            ("origin".to_string(), "app".to_string()),
            ("table".to_string(), String::new()),
            ("limit".to_string(), "9000".to_string()),
        ];
        let q = JobsQuery::from_pairs(&pairs);
        assert_eq!(q.status.as_deref(), Some("failed"));
        assert_eq!(q.table, None, "an empty value is no filter");
        assert_eq!(q.limit(), MAX_LIMIT);
        assert!(!q.is_snapshot_shaped());
        let clause = q.where_clause(q.origin().unwrap());
        assert!(clause.starts_with("WHERE status = 'failed' AND (!string::starts_with"), "{clause}");
        assert_eq!(JobsQuery::from_pairs(&[("origin".into(), "x".into())]).origin().unwrap_err().status, 400);
    }

    #[test]
    fn run_filters_escape_and_clamp() {
        let q = RunsQuery { name: Some("a'b".into()), limit: Some(0), ..Default::default() };
        let sql = q.surql();
        assert!(sql.contains("workflow_name = 'a\\'b'"), "{sql}");
        assert!(sql.ends_with("LIMIT 1;"), "{sql}");
    }

    #[test]
    fn run_ids_must_be_workflow_runs() {
        assert_eq!(parse_run_id("_00_workflow_run:abc").unwrap().key, "abc");
        assert_eq!(parse_run_id("_00_schedule:abc").unwrap_err().status, 400);
        assert_eq!(parse_run_id("nonsense").unwrap_err().status, 400);
    }

    #[test]
    fn op_errors_map_to_statuses() {
        assert_eq!(op_error(WorkflowOpError::NotFound("x".into())).status, 404);
        assert_eq!(op_error(WorkflowOpError::BadDag("cycle".into())).status, 422);
        assert_eq!(op_error(WorkflowOpError::NotTerminal { status: "running".into() }).status, 409);
    }

    /// The three predicates must partition the table: every row is in exactly
    /// one class, or a filtered page silently loses jobs.
    #[test]
    fn the_origin_predicates_partition_the_table() {
        assert!(Origin::Schedule.predicate().contains("'sch_'"));
        assert!(Origin::Workflow.predicate().contains("'wf_'"));
        let app = Origin::App.predicate();
        assert!(app.contains("!string::starts_with(record::id(id), 'sch_')"), "{app}");
        assert!(app.contains("!string::starts_with(record::id(id), 'wf_')"), "{app}");
        for o in [Origin::Schedule, Origin::Workflow, Origin::App] {
            assert_eq!(Origin::parse(o.as_str()), Some(o));
        }
    }

    #[test]
    fn pages_merge_newest_activity_first() {
        let page = merge_pages(
            vec![
                json!({ "id": "job:a", "updated_at": "2026-09-11T10:00:00Z" }),
                json!({ "id": "job:b", "updated_at": "2026-09-11T12:00:00Z" }),
                json!({ "id": "job:c", "updated_at": "2026-09-11T11:00:00Z" }),
            ],
            2,
        );
        let ids: Vec<&str> = page.iter().filter_map(|v| v["id"].as_str()).collect();
        assert_eq!(ids, ["job:b", "job:c"]);
        assert_eq!(id_list(&["job:a".into(), "job:o'brien".into()]), "['job:a', 'job:o\\'brien']");
    }

    #[test]
    fn schedule_literals_quote_hyphens() {
        assert_eq!(schedule_literal("game-sync"), "_00_schedule:⟨game-sync⟩");
        assert_eq!(schedule_literal("a⟩b"), "_00_schedule:⟨ab⟩");
    }

    #[test]
    fn run_filters_combine_compare_links_as_strings_and_skip_empty_ones() {
        let sql = RunsQuery::default().surql();
        assert!(!sql.contains("WHERE") && sql.contains(&format!("LIMIT {DEFAULT_LIMIT}")), "{sql}");
        let sql = RunsQuery { name: Some(String::new()), status: Some(String::new()), ..Default::default() }.surql();
        assert!(!sql.contains("WHERE"), "{sql}");
        let sql = RunsQuery { status: Some("running".into()), rerun_of: Some("_00_workflow_run:x".into()), ..Default::default() }.surql();
        assert!(sql.contains("status = 'running' AND type::string(rerun_of) = '_00_workflow_run:x'"), "{sql}");
        assert!(RunsQuery { limit: Some(100_000), ..Default::default() }.surql().contains(&format!("LIMIT {MAX_LIMIT}")));
    }

    #[test]
    fn a_db_error_from_an_old_schema_asks_for_a_deploy() {
        let behind = op_error(WorkflowOpError::Db(anyhow::anyhow!("found no field `retry_count`")));
        assert_eq!(behind.status, 409);
        assert!(behind.message.contains("spky deploy"));
        assert_eq!(op_error(WorkflowOpError::Db(anyhow::anyhow!("boom"))).status, 502);
        assert_eq!(op_error(WorkflowOpError::Conflict).status, 409);
    }

    /// Asymmetric retention is the point: this hour's successes are already
    /// pruned and folded, its failures are not. Neither source alone is the
    /// truth; their sum is.
    #[test]
    fn a_half_pruned_bucket_adds_up_and_a_running_run_is_no_outcome() {
        let pruned = vec![json!({
            "bucket": "2026-09-20T06:00:00Z",
            "success": 140, "failed": 0, "skipped": 30, "replaced": 0, "killed": 0
        })];
        let live = vec![
            json!({"bucket": "2026-09-20T06:00:00Z", "status": "failed", "n": 20}),
            json!({"bucket": "2026-09-20T06:00:00Z", "status": "success", "n": 2}),
            json!({"bucket": "2026-09-20T07:00:00Z", "status": "running", "n": 9}),
        ];
        let out = merge_tally(&pruned, &live, 48);
        assert_eq!(out.len(), 1, "one bucket, and no bucket for a running run: {out:?}");
        assert_eq!(out[0]["failed"], 20);
        assert_eq!(out[0]["success"], 142);
        assert_eq!(out[0]["skipped"], 30);
    }

    #[test]
    fn the_tally_adds_pruned_and_live_counts_newest_first() {
        let pruned = vec![json!({"bucket": "2026-10-21T10:00:00Z", "success": 3, "failed": 1})];
        let live = vec![
            json!({"bucket": "2026-10-21T10:00:00Z", "status": "failed", "n": 2}),
            json!({"bucket": "2026-10-21T11:00:00Z", "status": "success", "n": 5}),
        ];
        let out = merge_tally(&pruned, &live, 48);
        assert_eq!(out[0]["bucket"], "2026-10-21T11:00:00Z");
        assert_eq!(out[1]["success"], 3);
        assert_eq!(out[1]["failed"], 3);
    }
}
