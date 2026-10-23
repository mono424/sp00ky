//! Workflow and schedule views for the dashboard.
//!
//! `_00_workflow_run`, `_00_step_run`, `_00_schedule_run` and `_00_schedule`
//! are all `PERMISSIONS NONE`, so a record-auth token cannot read a single row
//! of them. The scheduler holds root and is the cluster's only schedule ticker,
//! which makes it the right — and only — place to serve these.
//!
//! Realtime is a poll, like `spky workflows watch`, but hoisted: ONE poller in
//! the scheduler feeds every connected dashboard through a broadcast channel,
//! and it only runs while someone is watching. Ten open browser tabs cost the
//! database exactly what one does, and a closed tab costs it nothing.
//!
//! The queries and actions are `schedule_core::admin`, shared with the SSP
//! node's admin API; this file adds the poller and the engine it runs over.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{Extension, Json};
use futures::stream::{self, Stream, StreamExt};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, warn};

use maintenance::db::ReconnectingDb;
use schedule_core::admin::{self as core, InstancesQuery, ReleaseBody, RunsQuery};
use schedule_core::ScheduleEngine;

use super::{core_db, core_error, db_unavailable, rows, AdminState, ApiError, CurrentSession};

/// How often the shared poller re-reads the run tables while anyone is
/// watching. Matches `spky workflows watch`.
const POLL_INTERVAL: Duration = Duration::from_millis(1000);

pub async fn list_runs(
    State(state): State<AdminState>,
    Query(q): Query<RunsQuery>,
) -> Result<Json<Value>, ApiError> {
    core::list_runs(&core_db(&state)?, &q).await.map(Json).map_err(core_error)
}

pub async fn run_detail(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    // The path segment arrives percent-decoded by axum; the shared query
    // escapes it into a string literal.
    core::run_detail(&core_db(&state)?, &id).await.map(Json).map_err(core_error)
}

/// `GET /admin/api/durable/instances`
pub async fn list_instances(
    State(state): State<AdminState>,
    Query(q): Query<InstancesQuery>,
) -> Result<Json<Value>, ApiError> {
    core::list_instances(&core_db(&state)?, &q).await.map(Json).map_err(core_error)
}

/// `GET /admin/api/durable/instances/:id`
pub async fn instance_detail(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    core::instance_detail(&core_db(&state)?, &id).await.map(Json).map_err(core_error)
}

/// `POST /admin/api/durable/instances/:id/cancel`
pub async fn cancel_instance(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let out = core::cancel_instance(&core_db(&state)?, &id).await.map_err(core_error)?;
    tracing::info!(instance = %id, by = %session.subject, "Durable workflow instance cancelled from the dashboard");
    Ok(Json(out))
}

/// `POST /admin/api/durable/instances/:id/retry`
pub async fn retry_instance(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let out = core::retry_instance(&core_db(&state)?, &id).await.map_err(core_error)?;
    tracing::info!(instance = %id, by = %session.subject, "Durable workflow instance retried from the dashboard");
    Ok(Json(out))
}

pub async fn list_schedules(State(state): State<AdminState>) -> Result<Json<Value>, ApiError> {
    core::list_schedules(&core_db(&state)?).await.map(Json).map_err(core_error)
}

/// One schedule, plus its recent runs, an hourly success/failure tally and the
/// keys it has quarantined.
pub async fn schedule_detail(
    State(state): State<AdminState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    core::schedule_detail(&core_db(&state)?, &name).await.map(Json).map_err(core_error)
}

// =============================================================
// Shared realtime poller
// =============================================================

/// One poller, many subscribers.
///
/// `subscribers` is what starts and stops the loop: the first `subscribe()`
/// spawns it, and it exits when the count returns to zero. That is why the
/// count is incremented *before* the task is spawned and decremented by a guard
/// held for the life of each stream — an idle dashboard must not keep a 1Hz
/// query running against the tenant database forever.
pub struct WorkflowWatcher {
    tx: broadcast::Sender<Value>,
    subscribers: AtomicUsize,
}

/// Decrements the subscriber count when a stream ends, however it ends
/// (client disconnect, error, or the process shutting down).
struct SubscriberGuard(Arc<WorkflowWatcher>);

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.0.subscribers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl WorkflowWatcher {
    pub fn new() -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(32);
        Arc::new(Self {
            tx,
            subscribers: AtomicUsize::new(0),
        })
    }

    fn subscribe(self: &Arc<Self>, db: Arc<ReconnectingDb>) -> (broadcast::Receiver<Value>, SubscriberGuard) {
        let rx = self.tx.subscribe();
        let guard = SubscriberGuard(Arc::clone(self));
        let previous = self.subscribers.fetch_add(1, Ordering::SeqCst);
        if previous == 0 {
            self.spawn_poller(db);
        }
        (rx, guard)
    }

    fn spawn_poller(self: &Arc<Self>, db: Arc<ReconnectingDb>) {
        let watcher = Arc::clone(self);
        tokio::spawn(async move {
            debug!("Workflow watcher started");
            let mut ticker = tokio::time::interval(POLL_INTERVAL);
            // A slow tick must not queue up bursts once the database recovers.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last: Option<String> = None;

            loop {
                ticker.tick().await;
                if watcher.subscribers.load(Ordering::SeqCst) == 0 {
                    debug!("Workflow watcher stopped: no subscribers");
                    break;
                }

                let handle = db.handle();
                let query = RunsQuery::default().surql();
                let runs: Vec<Value> = match handle.query(&query).await.and_then(|mut r| r.take(0)) {
                    Ok(v) => v,
                    Err(e) => {
                        db.note_error(&format!("{e:#}"));
                        warn!(error = %e, "Workflow watcher poll failed");
                        continue;
                    }
                };

                let payload = json!({ "runs": runs });
                // Only publish on change. A workflow table at rest is the
                // normal state, and pushing an identical frame every second
                // would turn every idle dashboard into a busy one.
                let fingerprint = payload.to_string();
                if last.as_deref() == Some(fingerprint.as_str()) {
                    continue;
                }
                last = Some(fingerprint);
                let _ = watcher.tx.send(payload);
            }
        });
    }
}

pub async fn stream_runs(
    State(state): State<AdminState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;

    // Seed the stream with the current state, so a fresh client paints
    // immediately instead of waiting for the first change.
    let initial = rows(&db, &RunsQuery::default().surql()).await?;
    let (rx, guard) = state.workflows.subscribe(db);

    let first = stream::iter(vec![Ok::<_, Infallible>(
        Event::default()
            .event("runs")
            .json_data(json!({ "runs": initial }))
            .unwrap_or_else(|_| Event::default().comment("unserialisable payload")),
    )]);

    let live = stream::unfold((rx, guard), |(mut rx, guard)| async move {
        loop {
            match rx.recv().await {
                Ok(payload) => {
                    let event = Event::default()
                        .event("runs")
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

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

/// A per-request engine over the admin plane's shared root handle. Cheap to
/// build, and it means an operator action runs through exactly the code the
/// 5s sweep runs, rather than a hand-written imitation of it.
fn engine(state: &AdminState) -> Result<ScheduleEngine, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    Ok(crate::schedule_engine::build_engine_over(
        Arc::new(crate::schedule_engine::SharedDb(db)),
        Arc::clone(&state.metrics.ssp_pool),
        Arc::clone(&state.transport),
    ))
}

/// `POST /admin/api/workflows/runs/:id/cancel`
pub async fn cancel_run(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    // A malformed id is a 400 whether or not the database is up yet.
    core::parse_run_id(&id).map_err(core_error)?;
    let out = core::cancel_run(&engine(&state)?, &id).await.map_err(core_error)?;
    tracing::info!(run = %id, status = %out["status"], by = %session.subject, "Workflow run cancelled from the dashboard");
    Ok((StatusCode::ACCEPTED, Json(out)))
}

/// `POST /admin/api/workflows/runs/:id/rerun`
pub async fn rerun_run(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    // A malformed id is a 400 whether or not the database is up yet.
    core::parse_run_id(&id).map_err(core_error)?;
    let out = core::rerun_run(&engine(&state)?, &id).await.map_err(core_error)?;
    tracing::info!(source = %id, run = %out["run"], by = %session.subject, "Workflow rerun from the dashboard");
    Ok((StatusCode::CREATED, Json(out)))
}

/// `POST /admin/api/workflows/runs/:id/retry`
pub async fn retry_run(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    // A malformed id is a 400 whether or not the database is up yet.
    core::parse_run_id(&id).map_err(core_error)?;
    let out = core::retry_run(&engine(&state)?, &id).await.map_err(core_error)?;
    tracing::info!(run = %id, retry_count = %out["retry_count"], reset = %out["reset"], by = %session.subject, "Workflow retried from the dashboard");
    Ok(Json(out))
}

async fn set_paused(
    state: &AdminState,
    session: &CurrentSession,
    name: &str,
    paused: bool,
) -> Result<Json<Value>, ApiError> {
    let out = core::set_paused(&core_db(state)?, name, paused).await.map_err(core_error)?;
    tracing::info!(schedule = %name, paused, by = %session.0.subject, "Schedule pause state changed from the dashboard");
    Ok(Json(out))
}

/// `POST /admin/api/schedules/:name/pause`
pub async fn schedule_pause(
    State(state): State<AdminState>,
    Extension(session): Extension<CurrentSession>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    set_paused(&state, &session, &name, true).await
}

/// `POST /admin/api/schedules/:name/resume`
pub async fn schedule_resume(
    State(state): State<AdminState>,
    Extension(session): Extension<CurrentSession>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    set_paused(&state, &session, &name, false).await
}

/// `POST /admin/api/schedules/:name/release` with `{ "key": "..." }`
///
/// Release one quarantined forEach key. Forgetting the failure streak IS the
/// release: the gate derives quarantine from the count against the schedule's
/// current budget, so a key with a zero streak is simply a key that has not
/// failed, and its next fire closes the incident.
pub async fn schedule_release(
    State(state): State<AdminState>,
    Extension(session): Extension<CurrentSession>,
    Path(name): Path<String>,
    Json(body): Json<ReleaseBody>,
) -> Result<Json<Value>, ApiError> {
    let out = core::release_key(&core_db(&state)?, &name, &body.key).await.map_err(core_error)?;
    tracing::info!(
        schedule = %name, key = %body.key, by = %session.0.subject,
        "Schedule key released from the dashboard"
    );
    Ok(Json(out))
}

/// `POST /admin/api/schedules/:name/trigger`
pub async fn schedule_trigger(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let out = core::trigger(&core_db(&state)?, &name).await.map_err(core_error)?;
    tracing::info!(schedule = %name, by = %session.subject, "Schedule triggered from the dashboard");
    Ok(Json(out))
}
