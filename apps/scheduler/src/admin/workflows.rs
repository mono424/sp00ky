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

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Json;
use futures::stream::{self, Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, warn};

use maintenance::db::ReconnectingDb;

use super::{api_error, db_unavailable, esc, rows, AdminState, ApiError};

/// How often the shared poller re-reads the run tables while anyone is
/// watching. Matches `spky workflows watch`.
const POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// Cap on `?limit=`, so one request cannot ask the database for everything.
const MAX_LIMIT: usize = 500;
const DEFAULT_LIMIT: usize = 50;

const RUN_FIELDS: &str = "type::string(id) AS id, workflow_name, schedule_name, status, \
     kill_requested, error, trigger, retry_count, \
     type::string(rerun_of) AS rerun_of, \
     type::string(created_at) AS created_at, \
     type::string(updated_at) AS updated_at, \
     type::string(finished_at) AS finished_at";

#[derive(Debug, Default, Deserialize)]
pub struct RunsQuery {
    #[serde(default)]
    pub name: Option<String>,
    /// Filter by the owning schedule rather than the workflow name. A schedule
    /// can drive a workflow whose name differs from its own, so the two are not
    /// interchangeable.
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

fn runs_surql(q: &RunsQuery) -> String {
    let mut filters = Vec::new();
    if let Some(name) = q.name.as_ref().filter(|s| !s.is_empty()) {
        filters.push(format!("workflow_name = '{}'", esc(name)));
    }
    if let Some(schedule) = q.schedule.as_ref().filter(|s| !s.is_empty()) {
        filters.push(format!("schedule_name = '{}'", esc(schedule)));
    }
    if let Some(status) = q.status.as_ref().filter(|s| !s.is_empty()) {
        filters.push(format!("status = '{}'", esc(status)));
    }
    if let Some(src) = q.rerun_of.as_ref().filter(|s| !s.is_empty()) {
        // `rerun_of` is a record link; compare its string form so the caller
        // can pass the id exactly as the list handed it out.
        filters.push(format!("type::string(rerun_of) = '{}'", esc(src)));
    }
    let where_clause = if filters.is_empty() {
        String::new()
    } else {
        format!("WHERE {} ", filters.join(" AND "))
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    format!("SELECT {RUN_FIELDS} FROM _00_workflow_run {where_clause}ORDER BY created_at DESC LIMIT {limit};")
}

pub async fn list_runs(
    State(state): State<AdminState>,
    Query(q): Query<RunsQuery>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    let runs = rows(&db, &runs_surql(&q)).await?;
    Ok(Json(json!({ "runs": runs })))
}

pub async fn run_detail(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;

    // The path segment arrives percent-decoded by axum; escaping it here is
    // what keeps it a string literal rather than SurrealQL.
    let run = rows(
        &db,
        &format!(
            "SELECT {RUN_FIELDS}, dag, input, target_table \
             FROM _00_workflow_run WHERE type::string(id) = '{}' LIMIT 1;",
            esc(&id)
        ),
    )
    .await?
    .into_iter()
    .next();

    let Some(run) = run else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!(
                "No workflow run '{}'. A successful run leaves no rows behind on a schedule \
                 with `history: failures-only`.",
                id
            ),
        ));
    };

    let steps = rows(
        &db,
        &format!(
            // `started_at` is when the step's job was created; `created_at` is
            // when the ROW was made, which for a step deep in a DAG is the run's
            // own start. Plotting the latter draws every dependent step from
            // +0ms, so the timeline needs both: the truth, and the fallback for
            // rows written before `started_at` was stamped.
            "SELECT step, depends_on, status, job_id, output, error, \
             dispatch_attempts, \
             type::string(created_at) AS created_at, \
             type::string(started_at) AS started_at, \
             type::string(finished_at) AS finished_at \
             FROM _00_step_run WHERE type::string(workflow_run) = '{}';",
            esc(&id)
        ),
    )
    .await?;

    Ok(Json(json!({ "run": run, "steps": steps })))
}

const SCHEDULE_FIELDS: &str = "name, kind, cron, every_ms, timezone, paused, config_disabled, \
     concurrency, max_retries, retry_strategy, timeout, target_table, path, \
     for_each, for_each_key, history_mode, last_run_status, last_error, \
     type::string(next_fire_at) AS next_fire_at, \
     type::string(last_fire_at) AS last_fire_at, \
     type::string(last_run_at) AS last_run_at, \
     type::string(created_at) AS created_at, \
     type::string(updated_at) AS updated_at";

pub async fn list_schedules(
    State(state): State<AdminState>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    let schedules = rows(
        &db,
        &format!("SELECT {SCHEDULE_FIELDS} FROM _00_schedule ORDER BY name;"),
    )
    .await?;
    Ok(Json(json!({ "schedules": schedules })))
}

/// One schedule, plus its recent runs and a success/failure tally.
///
/// The tally comes from `_00_run_rollup`, which the engine maintains as hourly
/// buckets precisely so this kind of view does not have to count rows that
/// retention has already pruned.
pub async fn schedule_detail(
    State(state): State<AdminState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;

    let schedule = rows(
        &db,
        &format!(
            "SELECT {SCHEDULE_FIELDS} FROM _00_schedule WHERE name = '{}' LIMIT 1;",
            esc(&name)
        ),
    )
    .await?
    .into_iter()
    .next();

    let Some(schedule) = schedule else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("No schedule named '{}'", name),
        ));
    };

    let runs = rows(
        &db,
        &format!(
            "SELECT type::string(id) AS id, schedule_name, key, kind, status, trigger, job_id, \
             type::string(workflow_run) AS workflow_run, error, \
             type::string(fire_at) AS fire_at, \
             type::string(created_at) AS created_at, \
             type::string(finished_at) AS finished_at \
             FROM _00_schedule_run WHERE schedule_name = '{}' \
             ORDER BY created_at DESC LIMIT 25;",
            esc(&name)
        ),
    )
    .await?;

    let rollup = rows(
        &db,
        &format!(
            "SELECT type::string(bucket) AS bucket, success, failed, skipped, replaced, killed \
             FROM _00_run_rollup WHERE name = '{}' ORDER BY bucket DESC LIMIT 48;",
            esc(&name)
        ),
    )
    .await?;

    Ok(Json(json!({
        "schedule": schedule,
        "runs": runs,
        "rollup": rollup,
    })))
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
                let query = format!(
                    "SELECT {RUN_FIELDS} FROM _00_workflow_run ORDER BY created_at DESC LIMIT {DEFAULT_LIMIT};"
                );
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
    let initial = rows(&db, &runs_surql(&RunsQuery::default())).await?;
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

use axum::Extension;
use schedule_core::{ids, ScheduleEngine, WorkflowOpError};

use super::CurrentSession;

fn parse_run_id(id: &str) -> Result<ids::Ref, ApiError> {
    match ids::Ref::parse(id) {
        Some(r) if r.table == ids::WORKFLOW_RUN => Ok(r),
        Some(r) => Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("'{}' is a {} record, not a workflow run", id, r.table),
        )),
        None => Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("'{}' is not a record id", id),
        )),
    }
}

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

fn op_error(e: WorkflowOpError) -> ApiError {
    match e {
        WorkflowOpError::NotFound(id) => {
            api_error(StatusCode::NOT_FOUND, format!("No workflow run {id}"))
        }
        WorkflowOpError::BadDag(msg) => {
            api_error(StatusCode::UNPROCESSABLE_ENTITY, format!("Frozen DAG is invalid: {msg}"))
        }
        WorkflowOpError::Db(err) => {
            let unknown_field = err
                .downcast_ref::<schedule_core::ScheduleDbError>()
                .map(|d| d.is_unknown_field())
                .unwrap_or_else(|| err.to_string().contains("found no field"));
            if unknown_field {
                api_error(
                    StatusCode::CONFLICT,
                    "The schedule tables predate this action; run `spky deploy` to migrate them",
                )
            } else {
                api_error(StatusCode::BAD_GATEWAY, format!("Database error: {err:#}"))
            }
        }
        // Everything else is a state the operator can read and act on.
        other => api_error(StatusCode::CONFLICT, other.to_string()),
    }
}

/// `POST /admin/api/workflows/runs/:id/cancel`
pub async fn cancel_run(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let run = parse_run_id(&id)?;
    let out = engine(&state)?
        .cancel_workflow(&run)
        .await
        .map_err(op_error)?;
    tracing::info!(run = %run, status = %out.status, by = %session.subject, "Workflow run cancelled from the dashboard");
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "run": out.run.as_string(), "status": out.status })),
    ))
}

/// `POST /admin/api/workflows/runs/:id/rerun`
pub async fn rerun_run(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let run = parse_run_id(&id)?;
    let out = engine(&state)?
        .rerun_workflow(&run, chrono::Utc::now())
        .await
        .map_err(op_error)?;
    tracing::info!(source = %run, run = %out.run, by = %session.subject, "Workflow rerun from the dashboard");
    Ok((
        StatusCode::CREATED,
        Json(json!({ "run": out.run.as_string(), "rerun_of": out.rerun_of.as_string() })),
    ))
}

/// `POST /admin/api/workflows/runs/:id/retry`
pub async fn retry_run(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run = parse_run_id(&id)?;
    let out = engine(&state)?
        .retry_workflow(&run)
        .await
        .map_err(op_error)?;
    tracing::info!(run = %run, retry_count = out.retry_count, reset = ?out.reset, by = %session.subject, "Workflow retried from the dashboard");
    Ok(Json(json!({
        "run": out.run.as_string(),
        "retry_count": out.retry_count,
        "reset": out.reset,
        "kept": out.kept,
    })))
}

/// `_00_schedule:⟨name⟩`, the same literal the CLI writes. The ⟨⟩ form quotes
/// any key, which matters because schedule names routinely carry hyphens (a
/// bare `_00_schedule:game-sync` parses as a subtraction).
fn schedule_literal(name: &str) -> String {
    format!("_00_schedule:⟨{}⟩", name.replace('⟩', ""))
}

async fn load_schedule(db: &Arc<ReconnectingDb>, name: &str) -> Result<Value, ApiError> {
    rows(
        db,
        &format!(
            "SELECT name, paused, config_disabled FROM _00_schedule WHERE name = '{}' LIMIT 1;",
            esc(name)
        ),
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| api_error(StatusCode::NOT_FOUND, format!("No schedule named '{name}'")))
}

async fn set_paused(
    state: &AdminState,
    session: &CurrentSession,
    name: &str,
    paused: bool,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    load_schedule(&db, name).await?;
    rows(
        &db,
        &format!("UPDATE {} SET paused = {};", schedule_literal(name), paused),
    )
    .await?;
    tracing::info!(schedule = %name, paused, by = %session.0.subject, "Schedule pause state changed from the dashboard");
    Ok(Json(json!({ "name": name, "paused": paused })))
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

/// `POST /admin/api/schedules/:name/trigger`
pub async fn schedule_trigger(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = state.db().ok_or_else(db_unavailable)?;
    let schedule = load_schedule(&db, &name).await?;
    // Pause wins over a queued trigger in the engine (`SELECT_TRIGGERED`
    // requires `paused = false`), so accepting one here would queue a fire
    // that silently never happens.
    if schedule["paused"].as_bool() == Some(true) {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!("Schedule '{name}' is paused; resume it before triggering"),
        ));
    }
    if schedule["config_disabled"].as_bool() == Some(true) {
        return Err(api_error(
            StatusCode::CONFLICT,
            format!("Schedule '{name}' is disabled in its config; enable it and run `spky schedules sync`"),
        ));
    }
    let updated = rows(
        &db,
        &format!(
            "UPDATE {} SET trigger_requested_at = time::now() RETURN type::string(trigger_requested_at) AS trigger_requested_at;",
            schedule_literal(&name)
        ),
    )
    .await?;
    let at = updated
        .first()
        .and_then(|r| r["trigger_requested_at"].as_str())
        .map(str::to_string);
    tracing::info!(schedule = %name, by = %session.subject, "Schedule triggered from the dashboard");
    Ok(Json(json!({ "name": name, "triggered_at": at })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_ids_must_be_workflow_runs() {
        assert_eq!(parse_run_id("_00_workflow_run:abc").unwrap().key, "abc");
        assert_eq!(parse_run_id("_00_workflow_run:⟨a-b⟩").unwrap().key, "a-b");
        assert_eq!(parse_run_id("_00_schedule:abc").unwrap_err().0, StatusCode::BAD_REQUEST);
        assert_eq!(parse_run_id("nonsense").unwrap_err().0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn op_errors_map_to_statuses() {
        assert_eq!(op_error(WorkflowOpError::NotFound("x".into())).0, StatusCode::NOT_FOUND);
        assert_eq!(op_error(WorkflowOpError::BadDag("cycle".into())).0, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(op_error(WorkflowOpError::NotTerminal { status: "running".into() }).0, StatusCode::CONFLICT);
        assert_eq!(op_error(WorkflowOpError::Conflict).0, StatusCode::CONFLICT);
        let behind = op_error(WorkflowOpError::Db(anyhow::anyhow!("found no field `retry_count`")));
        assert_eq!(behind.0, StatusCode::CONFLICT);
        assert!(behind.1["error"].as_str().unwrap().contains("spky deploy"));
        assert_eq!(op_error(WorkflowOpError::Db(anyhow::anyhow!("boom"))).0, StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn schedule_literals_quote_hyphens() {
        assert_eq!(schedule_literal("game-sync"), "_00_schedule:⟨game-sync⟩");
        assert_eq!(schedule_literal("a⟩b"), "_00_schedule:⟨ab⟩");
    }

    #[test]
    fn rerun_of_filter_compares_the_string_form() {
        let sql = runs_surql(&RunsQuery { rerun_of: Some("_00_workflow_run:x".into()), ..Default::default() });
        assert!(sql.contains("type::string(rerun_of) = '_00_workflow_run:x'"), "{sql}");
    }

    #[test]
    fn runs_surql_without_filters_has_no_where_clause() {
        let q = RunsQuery::default();
        let sql = runs_surql(&q);
        assert!(!sql.contains("WHERE"), "{sql}");
        assert!(sql.contains(&format!("LIMIT {DEFAULT_LIMIT}")), "{sql}");
    }

    #[test]
    fn runs_surql_combines_filters_and_escapes_them() {
        let q = RunsQuery {
            name: Some("o'brien".into()),
            status: Some("running".into()),
            limit: Some(5),
            ..Default::default()
        };
        let sql = runs_surql(&q);
        assert!(sql.contains("workflow_name = 'o\\'brien'"), "{sql}");
        assert!(sql.contains("status = 'running'"), "{sql}");
        assert!(sql.contains("AND"), "{sql}");
        assert!(sql.contains("LIMIT 5"), "{sql}");
    }

    #[test]
    fn runs_surql_clamps_the_limit() {
        let sql = runs_surql(&RunsQuery {
            limit: Some(100_000),
            ..Default::default()
        });
        assert!(sql.contains(&format!("LIMIT {MAX_LIMIT}")), "{sql}");

        let sql = runs_surql(&RunsQuery { limit: Some(0), ..Default::default() });
        assert!(sql.contains("LIMIT 1"), "{sql}");
    }

    #[test]
    fn empty_filter_strings_are_ignored() {
        let sql = runs_surql(&RunsQuery {
            name: Some(String::new()),
            status: Some(String::new()),
            ..Default::default()
        });
        assert!(!sql.contains("WHERE"), "{sql}");
    }
}
