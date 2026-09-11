//! The outbox plane, as an operator sees it.
//!
//! # Why this is its own page and not a corner of Workflows
//!
//! A job is reachable from a workflow step today, and only from there. That
//! leaves two whole classes of work with no surface at all:
//!
//! - a `kind: job` schedule's fire, which writes its job id to
//!   `_00_schedule_run.job_id` and nothing else,
//! - a job the application created itself, which belongs to no schedule and no
//!   workflow and is therefore invisible to every existing view.
//!
//! # Where a job comes from is in its id
//!
//! `schedule_core::ids` mints outbox keys with a prefix: `sch_<run key>` for a
//! schedule fire, `wf_<run key>_<step>` for a workflow step. Anything else was
//! created by the application. That is a convention, not a constraint, so the
//! prefix decides the *class* and an indexed back-link (`idx_srun_job`,
//! `idx_step_job`) supplies the *link*. A job whose owning run has been pruned
//! still classifies correctly; it simply has nowhere to point.
//!
//! # One sampler
//!
//! Every aggregate here is a scan of a user table, so it is taken once by a
//! single background task and every reader serves from its snapshot — the same
//! bargain `presence.rs` strikes, for the same reason. The one loop runs slowly
//! while nobody is looking and quickly while the Jobs page holds a stream open,
//! so an idle cluster pays almost nothing and an operator watching a backlog
//! drain sees it move.

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{Extension, Json};
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Notify;
use tracing::{debug, warn};

use maintenance::db::ReconnectingDb;
use schedule_core::ids;
use schedule_core::sql::{is_plain_identifier, LEASE_LIVE};

use super::{
    api_error, db_unavailable, esc, rows, AdminConfig, AdminState, ApiError, CurrentSession,
};

/// Cap on `?limit=`, so one request cannot ask the database for everything.
const MAX_LIMIT: usize = 500;
const DEFAULT_LIMIT: usize = 50;

/// How many samples the sparkline keeps. At the idle 15s tick that is half an
/// hour; at the live 2s tick it is four minutes of close detail.
const SAMPLE_WINDOW: usize = 120;

/// Rows deleted per `POST /jobs/clear` statement. Matches `spky jobs clear`:
/// one bounded statement per round trip, looped until a short batch comes back,
/// so a backlog of a million rows is not one transaction.
const CLEAR_BATCH: usize = 500;

/// Ceiling on how many clear batches one request will run, so the HTTP call
/// cannot outlive the operator's patience. The response says whether more is
/// left, and pressing the button again picks up where it stopped.
const CLEAR_MAX_BATCHES: usize = 40;

/// The dispatcher's fallback when a table has no `_00_job_policy` row.
/// Mirrors `ssp-node`'s `DEFAULT_CONCURRENCY`; if that changes, this reads high
/// rather than wrong, and the page says where the number came from.
const DEFAULT_CONCURRENCY: i64 = 1;

/// Listing projection. Deliberately without `payload` and `result`: a payload
/// is uncapped and a result is up to 64 KiB, and fifty of each would make the
/// list the most expensive request in the dashboard. Both are on the detail.
const JOB_FIELDS: &str = "type::string(id) AS id, record::id(id) AS key, status, path, \
     retries, max_retries, retry_strategy, assignee, timeout, delay, errors, \
     type::string(lease_until) AS lease_until, \
     type::string(created_at) AS created_at, \
     type::string(updated_at) AS updated_at";

// =============================================================
// Origin
// =============================================================

/// Which part of the system created a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    /// A `kind: job` schedule fire (`sch_` keys).
    Schedule,
    /// One step of a workflow run (`wf_` keys, `_r<n>` on an operator retry).
    Workflow,
    /// The application's own code. The class with no other surface anywhere.
    App,
}

impl Origin {
    pub fn as_str(self) -> &'static str {
        match self {
            Origin::Schedule => "schedule",
            Origin::Workflow => "workflow",
            Origin::App => "app",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "schedule" => Some(Origin::Schedule),
            "workflow" => Some(Origin::Workflow),
            "app" => Some(Origin::App),
            _ => None,
        }
    }

    /// The SurrealQL predicate selecting this class.
    ///
    /// `record::id(id)` and not `type::string(id)`: the former is the bare key,
    /// so the prefix test does not have to know the table's name.
    fn predicate(self) -> &'static str {
        match self {
            Origin::Schedule => "string::starts_with(record::id(id), 'sch_')",
            Origin::Workflow => "string::starts_with(record::id(id), 'wf_')",
            Origin::App => {
                "!string::starts_with(record::id(id), 'sch_') \
                 AND !string::starts_with(record::id(id), 'wf_')"
            }
        }
    }
}

/// Classify a job by the key the engine minted for it.
///
/// Prefix only, on purpose. The back-link is the confirmation, and it is not
/// always available: terminal schedule and workflow runs are pruned on their
/// own retention window, well before the jobs that outlived them, and a job
/// that lost its run must still be readable as scheduled work rather than
/// silently becoming an application job.
pub fn origin_of(key: &str) -> Origin {
    if key.starts_with("sch_") {
        Origin::Schedule
    } else if key.starts_with("wf_") {
        Origin::Workflow
    } else {
        Origin::App
    }
}

// =============================================================
// Snapshot
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
    /// `processing` rows whose lease has expired. Nobody is working on these;
    /// the next recovery sweep may reclaim them. The state that has no name in
    /// the schema, and the one an operator most needs to see.
    pub stalled: i64,
    /// The `_00_job_policy` ceiling, or the dispatcher's default.
    pub concurrency: i64,
    /// Jobs that succeeded in the last minute.
    pub throughput_1m: i64,
    pub oldest_pending: Option<String>,
    /// This table's query failed. Recorded per table so one missing table (a
    /// fresh database, a renamed outbox) does not blank the whole page.
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct JobSample {
    pub t: u64,
    pub pending: i64,
    pub processing: i64,
    pub failed: i64,
    pub throughput_1m: i64,
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
    /// here would read as a healthy empty queue.
    pub blind: bool,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// =============================================================
// Queries
// =============================================================

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

/// The outbox tables this deployment owns.
///
/// `_00_retention.job_tables` is the list `spky deploy` writes, and it is the
/// allowlist for every interpolation below: a table name cannot be a SurrealQL
/// parameter, and the root handle this plane holds can read every table in the
/// database. `is_plain_identifier` is the second line, not the first.
async fn job_tables(db: &Arc<ReconnectingDb>) -> Result<Vec<String>, ApiError> {
    let listed = rows(db, "SELECT job_tables FROM _00_retention LIMIT 1;")
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

/// Each table's execution ceiling, from the row `spky deploy` upserts.
async fn job_limits(db: &Arc<ReconnectingDb>) -> BTreeMap<String, i64> {
    let listed = rows(
        db,
        "SELECT type::string(id) AS id, concurrency FROM _00_job_policy;",
    )
    .await
    .unwrap_or_default();
    listed
        .iter()
        .filter_map(|row| {
            let id = row.get("id").and_then(Value::as_str)?;
            let key = ids::Ref::parse(id)?.key;
            Some((key, row.get("concurrency").and_then(Value::as_i64)?))
        })
        .collect()
}

/// Everything one table contributes to a snapshot, in one round trip.
///
/// The status counts cover in-flight work plus the last hour of terminal work,
/// not all of history. An all-time `GROUP BY status` is an unbounded aggregate
/// on every tick, and once retention keeps failures longer than successes an
/// all-time ratio reports a fail rate that climbs as successes age out. A
/// bounded window is the only honest denominator, and it is the one
/// `spky jobs` already uses.
async fn sample_table(
    db: &Arc<ReconnectingDb>,
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

    let handle = db.handle();
    let mut response = match handle.query(&surql).await {
        Ok(r) => r,
        Err(e) => {
            db.note_error(&format!("{e:#}"));
            return Err(format!("{e}"));
        }
    };

    // One statement's rows, or an empty page. A single statement failing inside
    // an otherwise-good response (a table that exists but has no `lease_until`
    // yet, say) reads as a zero rather than losing the whole sample.
    macro_rules! page {
        ($idx:expr) => {{
            let taken: Vec<Value> = response.take($idx).unwrap_or_default();
            taken
        }};
    }

    let listing = page!(0);
    let mut counts = StatusCounts::default();
    for row in page!(1) {
        let status = row.get("status").and_then(Value::as_str).unwrap_or("");
        counts.add(status, int_of(row.get("n")));
    }
    let throughput_1m = count_row(&page!(2));
    let oldest_pending = page!(3)
        .first()
        .and_then(|v| v.get("created_at"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let in_flight = count_row(&page!(4));

    let jobs = listing
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
            // Every `processing` row the live-lease count did not claim. Derived
            // rather than queried, so the two numbers cannot disagree.
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

/// Attach each job's origin, resolving the back-link where one survives.
///
/// Two queries for a whole page, not one per row: `_00_schedule_run.job_id` and
/// `_00_step_run.job_id` are both indexed (`idx_srun_job`, `idx_step_job`), so
/// an `INSIDE` over the page's ids is an index probe.
async fn attach_origins(db: &Arc<ReconnectingDb>, jobs: &mut [Value]) {
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
            Origin::App => {}
        }
    }

    let mut by_job: BTreeMap<String, Value> = BTreeMap::new();

    if !scheduled.is_empty() {
        let found = rows(
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
            let Some(job_id) = row.get("job_id").and_then(Value::as_str) else {
                continue;
            };
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
        let found = rows(
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
            let Some(job_id) = row.get("job_id").and_then(Value::as_str) else {
                continue;
            };
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
        let key = job.get("key").and_then(Value::as_str).unwrap_or("");
        let kind = origin_of(key);
        // The resolved row when the run still exists, the bare class when it
        // does not. Never absent: the page groups on `origin.kind`.
        job["origin"] = by_job
            .remove(&id)
            .unwrap_or_else(|| json!({ "kind": kind.as_str() }));
        job["last_error"] = job
            .get("errors")
            .and_then(Value::as_array)
            .and_then(|e| e.last())
            .cloned()
            .unwrap_or(Value::Null);
        // The whole attempt history belongs on the detail page; the list wants
        // the count and the last one.
        job["attempts"] = json!(job
            .get("errors")
            .and_then(Value::as_array)
            .map(|e| e.len())
            .unwrap_or(0));
        job.as_object_mut().map(|o| o.remove("errors"));
    }
}

/// Merge per-table pages into one, newest activity first. ISO-8601 strings
/// sort chronologically, which is why `updated_at` is projected as a string.
fn merge_pages(mut jobs: Vec<Value>, limit: usize) -> Vec<Value> {
    jobs.sort_by(|a, b| {
        let key = |v: &Value| {
            v.get("updated_at")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        key(b).cmp(&key(a))
    });
    jobs.truncate(limit);
    jobs
}

// =============================================================
// The sampler
// =============================================================

pub struct JobSampler {
    snapshot: RwLock<Option<Snapshot>>,
    samples: Mutex<VecDeque<JobSample>>,
    tx: broadcast::Sender<Value>,
    /// How many `/jobs/stream` subscribers are connected. Non-zero is what
    /// puts the loop on its live cadence.
    watchers: AtomicUsize,
    /// Wakes the loop when the first watcher arrives, so opening the Jobs page
    /// does not wait out a whole idle tick for its first frame.
    wake: Notify,
    idle_interval: Duration,
    live_interval: Duration,
    rows_per_table: usize,
}

/// Decrements the watcher count when a stream ends, however it ends.
pub struct WatcherGuard(Arc<JobSampler>);

impl Drop for WatcherGuard {
    fn drop(&mut self) {
        self.0.watchers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl JobSampler {
    pub fn new(config: &AdminConfig) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(16);
        Arc::new(Self {
            snapshot: RwLock::new(None),
            samples: Mutex::new(VecDeque::with_capacity(SAMPLE_WINDOW)),
            tx,
            watchers: AtomicUsize::new(0),
            wake: Notify::new(),
            idle_interval: config.job_interval,
            live_interval: config.job_live_interval,
            rows_per_table: DEFAULT_LIMIT,
        })
    }

    /// Whether the background sampler runs at all. `SPKY_ADMIN_JOB_INTERVAL_SECS=0`
    /// turns it off for a deployment that would rather pay nothing until someone
    /// opens the page; the listing then queries on demand and the Overview tile
    /// says so rather than showing zeros.
    pub fn enabled(&self) -> bool {
        !self.idle_interval.is_zero()
    }

    fn current(&self) -> Option<Snapshot> {
        self.snapshot.read().ok().and_then(|g| g.clone())
    }

    fn samples(&self) -> Vec<JobSample> {
        self.samples
            .lock()
            .map(|g| g.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Start the one sampler. Ticks harmlessly while the scheduler is still
    /// cloning and has published no database handle yet, which is what every
    /// other admin reader does.
    pub fn spawn(self: &Arc<Self>, state: AdminState) {
        if !self.enabled() {
            debug!("Job sampler disabled (SPKY_ADMIN_JOB_INTERVAL_SECS=0)");
            return;
        }
        let sampler = Arc::clone(self);
        tokio::spawn(async move {
            debug!(
                idle_secs = sampler.idle_interval.as_secs(),
                live_secs = sampler.live_interval.as_secs(),
                "Job sampler started"
            );
            loop {
                sampler.sample(&state).await;
                let watching = sampler.watchers.load(Ordering::SeqCst) > 0;
                let wait = if watching {
                    sampler.live_interval
                } else {
                    sampler.idle_interval
                };
                // Racing the sleep against the notify is what makes one loop
                // serve both cadences: an arriving watcher cuts the idle wait
                // short instead of needing a second task of its own.
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = sampler.wake.notified() => {}
                }
            }
        });
    }

    async fn sample(&self, state: &AdminState) {
        let Some(db) = state.db() else { return };
        let Ok(tables) = job_tables(&db).await else {
            return;
        };
        let snapshot = build_snapshot(&db, &tables, self.rows_per_table).await;

        let sample = JobSample {
            t: snapshot.taken_at_ms,
            pending: snapshot.counts.pending,
            processing: snapshot.counts.processing,
            failed: snapshot.counts.failed,
            throughput_1m: snapshot.throughput_1m,
        };
        if let Ok(mut samples) = self.samples.lock() {
            if samples.len() == SAMPLE_WINDOW {
                samples.pop_front();
            }
            samples.push_back(sample);
        }

        let payload = self.payload_of(&snapshot, DEFAULT_LIMIT);
        if let Ok(mut slot) = self.snapshot.write() {
            *slot = Some(snapshot);
        }

        // Only publish on change, minus the timestamp, which moves every tick
        // by definition. A queue at rest is the normal state, and pushing an
        // identical frame every two seconds would turn an idle dashboard into a
        // busy one.
        if self.watchers.load(Ordering::SeqCst) > 0 {
            let _ = self.tx.send(payload);
        }
    }

    /// The listing body for a snapshot, in the shape `GET /jobs` answers with.
    fn payload_of(&self, snapshot: &Snapshot, limit: usize) -> Value {
        let mut jobs = snapshot.jobs.clone();
        jobs.truncate(limit);
        json!({
            "jobs": jobs,
            "returned": jobs.len(),
            "limit": limit,
            "totals": self.totals_of(snapshot),
            "tables": snapshot.tables,
            "sampled_at_ms": snapshot.taken_at_ms,
            "live": true,
        })
    }

    fn totals_of(&self, snapshot: &Snapshot) -> Value {
        json!({
            "counts": snapshot.counts,
            "in_flight": snapshot.in_flight,
            "stalled": snapshot.stalled,
            "throughput_1m": snapshot.throughput_1m,
            "oldest_pending": snapshot.oldest_pending,
            "tables": snapshot.tables.len(),
            "blind": snapshot.blind,
            "samples": self.samples(),
            "ready": true,
        })
    }

    /// The compact block folded into `GET /overview`.
    ///
    /// Free: it is memory the sampler already filled, so the sidebar count and
    /// the Overview tile cost the poll the dashboard already makes and add
    /// nothing to the database.
    pub fn overview_block(&self) -> Value {
        match self.current() {
            Some(snapshot) => {
                let mut out = self.totals_of(&snapshot);
                out["taken_at_ms"] = json!(snapshot.taken_at_ms);
                out["sample_interval_secs"] = json!(self.idle_interval.as_secs());
                out
            }
            // The sampler has not run yet, or is switched off. Said explicitly
            // rather than sent as zeros, which the UI would draw as an empty,
            // healthy queue.
            None => json!({
                "counts": StatusCounts::default(),
                "in_flight": 0,
                "stalled": 0,
                "throughput_1m": 0,
                "oldest_pending": Value::Null,
                "tables": 0,
                "blind": false,
                "samples": [],
                "taken_at_ms": Value::Null,
                "sample_interval_secs": self.idle_interval.as_secs(),
                "enabled": self.enabled(),
                "ready": false,
            }),
        }
    }

    fn subscribe(self: &Arc<Self>) -> (broadcast::Receiver<Value>, WatcherGuard) {
        let rx = self.tx.subscribe();
        let guard = WatcherGuard(Arc::clone(self));
        if self.watchers.fetch_add(1, Ordering::SeqCst) == 0 {
            // First watcher: cut the idle wait short so the page's second frame
            // is a live one rather than up to a full idle interval away.
            self.wake.notify_one();
        }
        (rx, guard)
    }
}

/// Sample every table and fold the results. Shared by the sampler and by an
/// on-demand listing, so a filtered page and a snapshot can never be built from
/// two different ideas of what a total means.
async fn build_snapshot(
    db: &Arc<ReconnectingDb>,
    tables: &[String],
    per_table: usize,
) -> Snapshot {
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
                warn!(table, error = %message, "Job table sample failed");
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
    let mut in_flight = 0;
    let mut stalled = 0;
    let mut throughput_1m = 0;
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
        // Not "no tables": a project with no outbox table at all is a valid,
        // fully visible state. Blind means every table there is refused to answer.
        blind: !tables.is_empty() && failures == tables.len(),
    }
}

// =============================================================
// GET /admin/api/jobs
// =============================================================

#[derive(Debug, Default, Deserialize)]
pub struct JobsQuery {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
    /// `schedule`, `workflow` or `app`. The filter this page exists for.
    #[serde(default)]
    pub origin: Option<String>,
    /// Substring of the job's path or its id.
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

fn nonempty(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|s| !s.is_empty())
}

impl JobsQuery {
    /// Whether this is exactly the query the sampler already answers.
    fn is_snapshot_shaped(&self) -> bool {
        nonempty(&self.status).is_none()
            && nonempty(&self.table).is_none()
            && nonempty(&self.origin).is_none()
            && nonempty(&self.q).is_none()
            && self.limit.unwrap_or(DEFAULT_LIMIT) <= DEFAULT_LIMIT
    }

    /// The `WHERE` body for one table, or empty when nothing is filtered.
    ///
    /// Every clause is pushed into SurrealQL rather than applied to the page in
    /// Rust. Filtering after the LIMIT is the trap `list_views` documents for
    /// its SSP filter: the limit would apply BEFORE the filter, and a busy
    /// table could answer an empty page while plainly holding matching jobs.
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

/// `GET /admin/api/jobs`
///
/// Unfiltered, this is free: it is the sampler's snapshot, already in memory.
/// Any filter runs a bounded query per table instead, because a filtered view
/// of a cached page is not the same thing as a filtered query, and the operator
/// asking for `origin=app` wants every application job, not the ones that
/// happened to be on the last page.
pub async fn list_jobs(
    State(state): State<AdminState>,
    Query(q): Query<JobsQuery>,
) -> Result<Json<Value>, ApiError> {
    // Validate before reaching for the database: a bad origin is a bad request
    // whether or not the scheduler has finished cloning.
    let origin = match nonempty(&q.origin) {
        Some(raw) => Some(Origin::parse(raw).ok_or_else(|| {
            api_error(
                StatusCode::BAD_REQUEST,
                format!("'{raw}' is not an origin. Use schedule, workflow or app."),
            )
        })?),
        None => None,
    };

    if q.is_snapshot_shaped() {
        if let Some(snapshot) = state.jobs.current() {
            let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
            return Ok(Json(state.jobs.payload_of(&snapshot, limit)));
        }
    }

    let db = state.db().ok_or_else(db_unavailable)?;
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let all_tables = job_tables(&db).await?;

    let tables: Vec<String> = match nonempty(&q.table) {
        Some(wanted) => {
            if !all_tables.iter().any(|t| t == wanted) {
                return Err(api_error(
                    StatusCode::NOT_FOUND,
                    format!(
                        "'{wanted}' is not an outbox table on this project. Deploy writes the \
                         list to `_00_retention.job_tables`."
                    ),
                ));
            }
            vec![wanted.to_string()]
        }
        None => all_tables,
    };

    let where_clause = q.where_clause(origin);
    let mut jobs = Vec::new();
    let mut errors = Vec::new();
    for table in &tables {
        let surql = format!(
            "SELECT {JOB_FIELDS} FROM {table} {where_clause}ORDER BY updated_at DESC LIMIT {limit};"
        );
        match rows(&db, &surql).await {
            Ok(page) => jobs.extend(page.into_iter().map(|mut row| {
                row["table"] = json!(table);
                row
            })),
            Err((_, Json(body))) => errors.push(json!({
                "table": table,
                "error": body.get("error").cloned().unwrap_or(Value::Null),
            })),
        }
    }

    if !tables.is_empty() && errors.len() == tables.len() {
        return Err(api_error(
            StatusCode::BAD_GATEWAY,
            "Every outbox table refused to answer; the listing would read as an empty queue",
        ));
    }

    let mut jobs = merge_pages(jobs, limit);
    attach_origins(&db, &mut jobs).await;

    // Totals always come from the sampler when it has run: recomputing them per
    // filtered request would make an operator flicking between filters the most
    // expensive reader on the plane, and a total that changes with the filter
    // is not a total.
    let totals = match state.jobs.current() {
        Some(snapshot) => state.jobs.totals_of(&snapshot),
        None => state.jobs.overview_block(),
    };

    Ok(Json(json!({
        "jobs": jobs,
        "returned": jobs.len(),
        "limit": limit,
        "totals": totals,
        "tables": state.jobs.current().map(|s| s.tables).unwrap_or_default(),
        "table_errors": errors,
        "filtered": true,
        "live": false,
    })))
}

/// `GET /admin/api/jobs/stream`
///
/// Seeded with the snapshot so a fresh client paints immediately, then fed by
/// the sampler's broadcast. Holding this open is what puts the sampler on its
/// live cadence, and dropping it is what takes it back off.
pub async fn stream_jobs(
    State(state): State<AdminState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    if !state.jobs.enabled() {
        return Err(api_error(
            StatusCode::NOT_IMPLEMENTED,
            "The job sampler is disabled on this scheduler (SPKY_ADMIN_JOB_INTERVAL_SECS=0); \
             the Jobs page polls instead",
        ));
    }

    let initial = match state.jobs.current() {
        Some(snapshot) => state.jobs.payload_of(&snapshot, DEFAULT_LIMIT),
        // The sampler has not completed a pass yet. Seed with the empty-but-not-
        // ready block rather than refusing: the next tick is seconds away and a
        // 503 here would send the page to an error screen during startup.
        None => json!({ "jobs": [], "returned": 0, "limit": DEFAULT_LIMIT,
                        "totals": state.jobs.overview_block(), "tables": [], "live": true }),
    };
    let (rx, guard) = state.jobs.subscribe();

    let first = stream::iter(vec![Ok::<_, Infallible>(
        Event::default()
            .event("jobs")
            .json_data(initial)
            .unwrap_or_else(|_| Event::default().comment("unserialisable payload")),
    )]);

    let live = stream::unfold((rx, guard), |(mut rx, guard)| async move {
        loop {
            match rx.recv().await {
                Ok(payload) => {
                    let event = Event::default()
                        .event("jobs")
                        .json_data(payload)
                        .unwrap_or_else(|_| Event::default().comment("unserialisable payload"));
                    return Some((Ok::<_, Infallible>(event), (rx, guard)));
                }
                // Only the newest frame matters for a state snapshot, so a
                // lagging client is simply served the next one.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });

    Ok(Sse::new(first.chain(live).boxed())
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

// =============================================================
// GET /admin/api/jobs/:id
// =============================================================

/// One outbox row, by table and key.
///
/// `result` is the backend's response body, capped at 64 KiB by the runner: fine for
/// one job, which is why `spky jobs get` shows it and the listing does not. `errors`
/// is the reason this endpoint exists at all.
///
/// The table is interpolated because a table name cannot be a SurrealQL parameter.
/// The caller checks it against `_00_retention.job_tables` first — the escaping here
/// is the second line, not the first.
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

/// `GET /admin/api/jobs/:id`
///
/// The outbox row behind a step, a schedule fire, or nothing at all. Everything else
/// on this plane reads the `_00_*` tables, which the engine owns; this one reaches into
/// an application's own table, so the table name is checked against
/// `_00_retention.job_tables` — the list deploy writes — before it is interpolated.
/// Without that check a path segment would choose the table, and every table in the
/// database is readable by the root handle this plane holds.
///
/// Worth its own endpoint because it is where a failure actually says what went
/// wrong: `_00_step_run.error` carries the LAST attempt, while `errors` here carries
/// every one of them, which is the difference between "the backend 500s" and "the
/// backend 500s on retry 3 only, after two timeouts".
pub async fn job_detail(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;

    let Some(job) = ids::Ref::parse(&id) else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("'{id}' is not a record id"),
        ));
    };

    let allowed = job_tables(&db).await?;
    if !allowed.iter().any(|t| *t == job.table) {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!(
                "'{}' is not an outbox table on this project. Deploy writes the list to \
                 `_00_retention.job_tables`; a job whose table is missing from it was \
                 created against a table this deployment no longer knows about.",
                job.table
            ),
        ));
    }

    let row = rows(&db, &job_surql(&job.table, &job.key))
        .await?
        .into_iter()
        .next();

    let Some(mut row) = row.filter(Value::is_object) else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!(
                "No job '{id}'. Terminal jobs are pruned on the project's retention \
                 window, so a run older than that keeps its step rows and loses its jobs."
            ),
        ));
    };
    row["table"] = json!(job.table);

    // `attach_origins` strips `errors` for the listing, and the detail page is
    // the one place that wants the whole history, so keep a copy.
    let errors = row.get("errors").cloned().unwrap_or(Value::Null);
    let mut one = [row];
    attach_origins(&db, &mut one).await;
    let [mut row] = one;
    row["errors"] = errors;

    Ok(Json(json!({ "job": row })))
}

// =============================================================
// Actions
// =============================================================

/// `POST /admin/api/jobs/:id/kill`
pub async fn job_kill(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    tracing::info!(job = %id, by = %session.subject, "Job kill from the dashboard");
    let (status, body) =
        crate::job_scheduler::kill_job(&state.metrics.ssp_pool, &state.transport, &id).await;
    relay(status, body)
}

/// `POST /admin/api/jobs/:id/retry`
pub async fn job_retry(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    tracing::info!(job = %id, by = %session.subject, "Job retry from the dashboard");
    let (status, body) =
        crate::job_scheduler::retry_job(&state.metrics.ssp_pool, &state.transport, &id).await;
    relay(status, body)
}

/// The job routes speak `{code, message}`; the admin plane speaks `{error}`.
/// Translate on failure, pass through on success.
fn relay(status: StatusCode, body: Value) -> Result<(StatusCode, Json<Value>), ApiError> {
    if status.is_success() {
        Ok((status, Json(body)))
    } else {
        let message = body
            .get("message")
            .or_else(|| body.get("error"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("Job action failed ({})", status.as_u16()));
        Err(api_error(status, message))
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ClearBody {
    /// One table, or every outbox table when absent.
    #[serde(default)]
    pub table: Option<String>,
    /// Also delete `pending` rows. `processing` is spared either way.
    #[serde(default)]
    pub all: bool,
}

/// The delete predicate. `processing` is never in it, at either width: a job
/// being worked on right now must not be yanked out from under the SSP holding
/// its lease.
fn clear_predicate(all: bool) -> &'static str {
    if all {
        "status != 'processing'"
    } else {
        "status IN ['success', 'failed']"
    }
}

/// `POST /admin/api/jobs/clear`
///
/// The dashboard's `spky jobs clear`. Batched rather than one statement: a
/// backlogged table holds hundreds of thousands of terminal rows, and deleting
/// them in one transaction is the shape that stalls SurrealDB for minutes.
pub async fn jobs_clear(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Json(body): Json<ClearBody>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    let all_tables = job_tables(&db).await?;

    let tables: Vec<String> = match body.table.as_deref().filter(|s| !s.is_empty()) {
        Some(wanted) => {
            if !all_tables.iter().any(|t| t == wanted) {
                return Err(api_error(
                    StatusCode::NOT_FOUND,
                    format!("'{wanted}' is not an outbox table on this project"),
                ));
            }
            vec![wanted.to_string()]
        }
        None => all_tables,
    };

    let cond = clear_predicate(body.all);
    let mut cleared: BTreeMap<String, usize> = BTreeMap::new();
    let mut more = false;

    for table in &tables {
        // One statement per batch, wrapped in a block so the result is a single
        // count. Projecting `id` instead of `RETURN BEFORE` keeps the response
        // small: a job row carries an uncapped `payload` and up to 64 KiB of
        // `result`, and only the number was ever wanted.
        let surql = format!(
            "RETURN {{ \
             LET $doomed = (SELECT id FROM {table} WHERE {cond} LIMIT {CLEAR_BATCH}); \
             DELETE $doomed.id; \
             RETURN count($doomed); \
             }};"
        );
        let mut removed = 0usize;
        for round in 0..CLEAR_MAX_BATCHES {
            let batch = rows(&db, &surql)
                .await?
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
    tracing::info!(
        total,
        all = body.all,
        by = %session.subject,
        "Terminal jobs cleared from the dashboard"
    );
    Ok(Json(json!({ "cleared": cleared, "total": total, "more": more })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_job_query_carries_the_attempt_history() {
        let sql = job_surql("job", "sch_game_sync_1788364742716_582ceccbb9aa");
        // The whole point of the endpoint: a step row keeps only the attempt that
        // ended the job, so without `errors` this says nothing a step cannot.
        assert!(sql.contains("errors"), "{sql}");
        assert!(sql.contains("retries, max_retries"), "{sql}");
        assert!(
            sql.contains("type::record('job', 'sch_game_sync_1788364742716_582ceccbb9aa')"),
            "{sql}"
        );
        // Never the single-argument form, which truncates a hyphenated key.
        assert!(!sql.contains("type::record('job:"), "{sql}");
    }

    #[test]
    fn the_job_query_escapes_its_interpolated_names() {
        let sql = job_surql("job", "o'brien");
        assert!(sql.contains("'o\\'brien'"), "{sql}");
    }

    /// The id prefixes `schedule_core::ids` mints, read back.
    #[test]
    fn origin_comes_from_the_minted_prefix() {
        let run = ids::run_key("game-sync", 1_769_337_000_000, "");
        assert_eq!(origin_of(&ids::job("job", &run).key), Origin::Schedule);
        assert_eq!(
            origin_of(&ids::step_job("job", &run, "extract").key),
            Origin::Workflow
        );
        // An operator retry mints `_r<n>` and must stay a workflow job.
        assert_eq!(
            origin_of(&ids::step_job_attempt("job", &run, "extract", 2).key),
            Origin::Workflow
        );
        // Anything the application made itself.
        assert_eq!(origin_of("01hq9k2n8p"), Origin::App);
        assert_eq!(origin_of("send-welcome-email"), Origin::App);
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
    }

    #[test]
    fn origins_round_trip_through_their_wire_names() {
        for origin in [Origin::Schedule, Origin::Workflow, Origin::App] {
            assert_eq!(Origin::parse(origin.as_str()), Some(origin));
        }
        assert_eq!(Origin::parse("cron"), None);
    }

    #[test]
    fn filters_are_pushed_into_the_statement_and_escaped() {
        let q = JobsQuery {
            status: Some("failed".into()),
            q: Some("o'brien".into()),
            ..Default::default()
        };
        let clause = q.where_clause(Some(Origin::App));
        assert!(clause.starts_with("WHERE "), "{clause}");
        assert!(clause.contains("status = 'failed'"), "{clause}");
        assert!(clause.contains("o\\'brien"), "{clause}");
        assert!(clause.contains("string::starts_with"), "{clause}");
        assert!(clause.contains(" AND "), "{clause}");
    }

    #[test]
    fn an_unfiltered_query_has_no_where_clause() {
        assert_eq!(JobsQuery::default().where_clause(None), "");
        // And empty strings are not filters: a cleared input box must not become
        // `status = ''`, which matches nothing.
        let blank = JobsQuery {
            status: Some(String::new()),
            q: Some(String::new()),
            table: Some(String::new()),
            ..Default::default()
        };
        assert_eq!(blank.where_clause(None), "");
        assert!(blank.is_snapshot_shaped());
    }

    #[test]
    fn only_an_unfiltered_default_page_is_served_from_the_snapshot() {
        assert!(JobsQuery::default().is_snapshot_shaped());
        assert!(!JobsQuery { origin: Some("app".into()), ..Default::default() }
            .is_snapshot_shaped());
        assert!(!JobsQuery { limit: Some(MAX_LIMIT), ..Default::default() }
            .is_snapshot_shaped());
    }

    /// A running job is never deleted, at either width. The `all` flag widens
    /// the set to `pending`, never to `processing`.
    #[test]
    fn clearing_never_touches_a_processing_job() {
        for all in [false, true] {
            let cond = clear_predicate(all);
            assert!(!cond.contains("'processing'") || cond.starts_with("status !="), "{cond}");
        }
        assert_eq!(clear_predicate(true), "status != 'processing'");
        assert_eq!(clear_predicate(false), "status IN ['success', 'failed']");
    }

    #[test]
    fn the_per_table_sample_counts_a_bounded_window() {
        let sql = format!(
            "SELECT status, count() AS n FROM job \
             WHERE status IN ['pending', 'processing'] OR updated_at > time::now() - 1h \
             GROUP BY status;"
        );
        // The shape `sample_table` builds: never an all-time GROUP BY, whose
        // fail rate climbs as successes age out of retention.
        assert!(sql.contains("time::now() - 1h"), "{sql}");
        // And the lease predicate it shares with admission control.
        assert!(LEASE_LIVE.contains("lease_until"), "{LEASE_LIVE}");
    }

    #[test]
    fn id_lists_are_quoted_and_escaped() {
        assert_eq!(id_list(&[]), "[]");
        assert_eq!(
            id_list(&["job:a".to_string(), "job:o'brien".to_string()]),
            "['job:a', 'job:o\\'brien']"
        );
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
    }
}
