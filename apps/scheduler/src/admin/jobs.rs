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
//! The queries and shapes are `schedule_core::admin`, shared with the SSP
//! node's admin API; this file adds what only the scheduler has: the sampler,
//! the stream, and pool machines.
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
use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{Extension, Json};
use futures::stream::{self, Stream, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Notify;
use tracing::debug;

use schedule_core::admin::{self as core, ClearBody, JobsQuery, Snapshot, StatusCounts, DEFAULT_LIMIT};

use super::{api_error, core_db, core_error, AdminConfig, AdminState, ApiError, CurrentSession};

/// How many samples the sparkline keeps. At the idle 15s tick that is half an
/// hour; at the live 2s tick it is four minutes of close detail.
const SAMPLE_WINDOW: usize = 120;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct JobSample {
    pub t: u64,
    pub pending: i64,
    pub processing: i64,
    pub failed: i64,
    pub throughput_1m: i64,
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

    pub(crate) fn samples(&self) -> Value {
        let samples: Vec<JobSample> = self
            .samples
            .lock()
            .map(|g| g.iter().copied().collect())
            .unwrap_or_default();
        json!(samples)
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
        let Ok(db) = core_db(state) else { return };
        let Ok(tables) = core::job_tables(&db).await else {
            return;
        };
        let snapshot = core::build_snapshot(&db, &tables, self.rows_per_table).await;

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

        // Only publish while someone listens. A queue at rest is the normal
        // state, and pushing frames nobody reads would make an idle dashboard
        // a busy one.
        if self.watchers.load(Ordering::SeqCst) > 0 {
            let _ = self.tx.send(payload);
        }
    }

    /// The unfiltered listing body of the latest sample, when there is one:
    /// what the live `jobs` topic starts from before the next broadcast.
    pub(crate) fn live_payload(&self) -> Option<Value> {
        self.current().map(|s| self.payload_of(&s, DEFAULT_LIMIT))
    }

    /// The listing body for a snapshot, in the shape `GET /jobs` answers with.
    fn payload_of(&self, snapshot: &Snapshot, limit: usize) -> Value {
        snapshot.payload(limit, self.samples(), true)
    }

    fn totals_of(&self, snapshot: &Snapshot) -> Value {
        snapshot.totals(self.samples())
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

    pub(crate) fn subscribe(self: &Arc<Self>) -> (broadcast::Receiver<Value>, WatcherGuard) {
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

// =============================================================
// GET /admin/api/jobs
// =============================================================

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
    q.origin().map_err(core_error)?;

    if q.is_snapshot_shaped() {
        if let Some(snapshot) = state.jobs.current() {
            return Ok(Json(state.jobs.payload_of(&snapshot, q.limit())));
        }
    }

    let db = core_db(&state)?;
    // Totals always come from the sampler when it has run: recomputing them per
    // filtered request would make an operator flicking between filters the most
    // expensive reader on the plane, and a total that changes with the filter
    // is not a total.
    let current = state.jobs.current();
    let totals = match current.as_ref() {
        Some(snapshot) => state.jobs.totals_of(snapshot),
        None => state.jobs.overview_block(),
    };
    let tables = json!(current.map(|s| s.tables).unwrap_or_default());
    core::list_jobs_filtered(&db, &q, totals, tables).await.map(Json).map_err(core_error)
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

/// `GET /admin/api/jobs/:id`: the outbox row behind a step, a schedule fire,
/// or nothing at all, with every attempt's error.
pub async fn job_detail(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = core_db(&state)?;
    core::job_detail(&db, &id).await.map(Json).map_err(core_error)
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
    // A pool job runs on a pool machine, not on an SSP: broadcasting `/job/kill`
    // to the SSPs would reach nobody. The pool host fences the attempt and wakes
    // the machine's poll so its agent cancels at once.
    if let Some(pools) = state.pools.as_ref() {
        let table = id.split(':').next().unwrap_or_default();
        if pools.owns_table(table).await {
            return match pools.kill_job(&id).await {
                Ok(true) => Ok((StatusCode::OK, Json(json!({ "id": id, "status": "killed" })))),
                Ok(false) => Err(api_error(
                    StatusCode::CONFLICT,
                    "Job is not pending or running, so there is nothing to kill",
                )),
                Err(e) => Err(api_error(StatusCode::BAD_GATEWAY, format!("{e:#}"))),
            };
        }
    }
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

/// `POST /admin/api/jobs/clear`: the dashboard's `spky jobs clear`.
pub async fn jobs_clear(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Json(body): Json<ClearBody>,
) -> Result<Json<Value>, ApiError> {
    let db = core_db(&state)?;
    let out = core::jobs_clear(&db, &body).await.map_err(core_error)?;
    tracing::info!(
        total = %out["total"],
        all = body.all,
        by = %session.subject,
        "Terminal jobs cleared from the dashboard"
    );
    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_errors_relay_as_admin_errors() {
        let (status, Json(body)) = relay(StatusCode::CONFLICT, json!({ "code": "x", "message": "busy" })).unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "busy");
        assert!(relay(StatusCode::OK, json!({ "id": "job:a" })).is_ok());
    }
}
