use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use surrealdb::engine::remote::http::Client;
use surrealdb::types::RecordId;
use surrealdb::Surreal;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::DbConfig;
use crate::router::SspPool;
use crate::transport::{HttpTransport, SspInfo};

/// Job status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Pending => write!(f, "pending"),
            JobStatus::Running => write!(f, "running"),
            JobStatus::Completed => write!(f, "completed"),
            JobStatus::Failed => write!(f, "failed"),
        }
    }
}

/// Job dispatch request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDispatch {
    pub job_id: String,
    pub table: String,
    pub payload: Value,
}

/// Job result from SSP
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: String,
    pub status: JobStatus,
    pub result: Option<Value>,
    pub error: Option<String>,
}

/// Job tracker
#[derive(Clone)]
pub struct JobTracker {
    /// Map job_id -> (ssp_id, status)
    jobs: Arc<RwLock<HashMap<String, (String, JobStatus)>>>,
}

impl JobTracker {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Assign job to SSP
    pub async fn assign(&self, job_id: String, ssp_id: String) {
        let mut jobs = self.jobs.write().await;
        jobs.insert(job_id, (ssp_id, JobStatus::Running));
    }

    /// Get SSP assigned to job
    pub async fn get_assignment(&self, job_id: &str) -> Option<(String, JobStatus)> {
        let jobs = self.jobs.read().await;
        jobs.get(job_id).cloned()
    }

    /// Update job status
    pub async fn update_status(&self, job_id: &str, status: JobStatus) {
        let mut jobs = self.jobs.write().await;
        if let Some((ssp_id, _)) = jobs.get(job_id).cloned() {
            jobs.insert(job_id.to_string(), (ssp_id, status));
        }
    }

    /// Complete job (remove from tracker)
    pub async fn complete(&self, job_id: &str) {
        let mut jobs = self.jobs.write().await;
        jobs.remove(job_id);
    }

    /// Get all jobs assigned to an SSP
    pub async fn get_ssp_jobs(&self, ssp_id: &str) -> Vec<String> {
        let jobs = self.jobs.read().await;
        jobs.iter()
            .filter(|(_, (sid, _))| sid == ssp_id)
            .map(|(jid, _)| jid.clone())
            .collect()
    }

    /// Get running jobs count
    pub async fn running_count(&self) -> usize {
        let jobs = self.jobs.read().await;
        jobs.values()
            .filter(|(_, status)| *status == JobStatus::Running)
            .count()
    }
}

/// Job service state
#[derive(Clone)]
pub struct JobState {
    pub ssp_pool: Arc<RwLock<SspPool>>,
    pub transport: Arc<HttpTransport>,
    pub job_tracker: Arc<JobTracker>,
}

/// Identifies a single job for kill/retry. Forwarded verbatim to the owning
/// SSP's `/job/kill` / `/job/retry` endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobActionRequest {
    pub id: String,
}

/// Create job router
pub fn create_job_router(state: JobState) -> Router {
    Router::new()
        .route("/job/dispatch", post(dispatch_job))
        .route("/job/result", post(handle_job_result))
        .route("/job/kill", post(cluster_job_kill))
        .route("/job/retry", post(cluster_job_retry))
        .with_state(state)
}

/// Cluster `/job/kill` — broadcast to every ready SSP.
///
/// The `JobTracker` is not populated for outbox jobs (real routing is broadcast
/// in `ingest.rs`, not via `dispatch_job`), so we cannot look up the owner. A kill
/// is idempotent and harmless on SSPs that don't host the job: only the SSP with
/// the in-flight request cancels it; the rest just set a kill flag.
async fn cluster_job_kill(State(state): State<JobState>, Json(req): Json<JobActionRequest>) -> Response {
    let (status, body) = kill_job(&state.ssp_pool, &state.transport, &req.id).await;
    (status, Json(body)).into_response()
}

/// The cluster kill, as a function: the ingest-port route above and the
/// dashboard's `/admin/api/jobs/:id/kill` both call this, so a job killed from
/// either place behaves identically.
pub async fn kill_job(
    ssp_pool: &Arc<RwLock<SspPool>>,
    transport: &HttpTransport,
    id: &str,
) -> (StatusCode, serde_json::Value) {
    let ready: Vec<SspInfo> = {
        let pool = ssp_pool.read().await;
        pool.all()
            .into_iter()
            .filter(|s| pool.is_ready(&s.id))
            .cloned()
            .collect()
    };

    if ready.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "code": "no_ssp", "message": "no ready SSP available" }),
        );
    }

    let req = JobActionRequest { id: id.to_string() };
    let results = transport.broadcast_to_ssps(&ready, "/job/kill", &req).await;
    let dispatched = results.iter().filter(|(_, r)| r.is_ok()).count();
    (
        StatusCode::OK,
        json!({ "id": id, "dispatched": dispatched, "ssps": ready.len() }),
    )
}

/// Cluster `/job/retry` — pick exactly one ready SSP and forward.
///
/// Retry resets the row and enqueues into one SSP's local runner. Broadcasting
/// would reset+enqueue on every SSP and run the job N times, so we choose a single
/// SSP fresh (the prior assignment is stale; a re-enqueued job logically gets a new
/// one, mirroring how `ingest.rs` assigns).
async fn cluster_job_retry(State(state): State<JobState>, Json(req): Json<JobActionRequest>) -> Response {
    let (status, body) = retry_job(&state.ssp_pool, &state.transport, &req.id).await;
    (status, Json(body)).into_response()
}

/// The cluster retry, as a function shared with the dashboard. See
/// [`kill_job`] for why.
pub async fn retry_job(
    ssp_pool: &Arc<RwLock<SspPool>>,
    transport: &HttpTransport,
    id: &str,
) -> (StatusCode, serde_json::Value) {
    let ssp = {
        let mut pool = ssp_pool.write().await;
        match pool.select_for_query() {
            Some(id) => pool.get(&id).map(|s| (id.clone(), s.url.clone())),
            None => None,
        }
    };

    let (ssp_id, ssp_url) = match ssp {
        Some(pair) => pair,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "code": "no_ssp", "message": "no ready SSP available" }),
            );
        }
    };

    let req = JobActionRequest { id: id.to_string() };
    match transport.post_to_ssp_status(&ssp_url, "/job/retry", &req).await {
        Ok((status, _)) if status.is_success() => (
            StatusCode::OK,
            json!({ "id": id, "status": "pending", "assigned_to": ssp_id }),
        ),
        // The SSP's verdict is the useful part (404 unknown job, 409 not
        // terminal): relay its status and body rather than flattening both
        // into a 502.
        Ok((status, body)) => {
            let parsed: Value = serde_json::from_str(&body)
                .unwrap_or_else(|_| json!({ "code": "ssp_error", "message": body }));
            (
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                parsed,
            )
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            json!({ "code": "ssp_error", "message": e.to_string() }),
        ),
    }
}

/// Dispatch job to SSP
async fn dispatch_job(
    State(state): State<JobState>,
    Json(request): Json<JobDispatch>,
) -> Result<Json<String>, (StatusCode, String)> {
    info!("Dispatching job: {}", request.job_id);

    // Select SSP for job execution
    let (ssp_id, ssp_url) = {
        let mut pool = state.ssp_pool.write().await;
        match pool.select_for_query() {
            Some(id) => {
                let ssp = pool.get(&id).ok_or_else(|| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Selected SSP not found in pool".to_string(),
                    )
                })?;
                (id, ssp.url.clone())
            }
            None => {
                error!("No ready SSP available for job {}", request.job_id);
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "No SSP available".to_string(),
                ));
            }
        }
    };

    // Track job assignment
    state.job_tracker.assign(request.job_id.clone(), ssp_id.clone()).await;

    // Send job to SSP via HTTP POST /job/dispatch
    if let Err(e) = state
        .transport
        .post_to_ssp(&ssp_url, "/job/dispatch", &request)
        .await
    {
        error!("Failed to send job to SSP: {}", e);
        // Remove from tracker on send failure
        state.job_tracker.complete(&request.job_id).await;
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to send to SSP: {}", e),
        ));
    }

    info!("Dispatched job {} to SSP {}", request.job_id, ssp_id);
    Ok(Json(ssp_id))
}

/// Handle job result from SSP
async fn handle_job_result(
    State(state): State<JobState>,
    Json(result): Json<JobResult>,
) -> Result<StatusCode, (StatusCode, String)> {
    info!("Received job result: {} - {}", result.job_id, result.status);

    // Verify job exists in tracker
    let _assignment = match state.job_tracker.get_assignment(&result.job_id).await {
        Some(a) => a,
        None => {
            warn!("Received result for unknown job: {}", result.job_id);
            return Err((
                StatusCode::NOT_FOUND,
                format!("Job {} not found", result.job_id),
            ));
        }
    };

    // Update status
    state.job_tracker.update_status(&result.job_id, result.status.clone()).await;

    // If completed or failed, remove from tracker
    if result.status == JobStatus::Completed || result.status == JobStatus::Failed {
        state.job_tracker.complete(&result.job_id).await;
        info!("Job {} finished with status: {}", result.job_id, result.status);
    }

    // TODO: Update job status in SurrealDB
    // This would require a DB connection in JobState
    // For now, just log the result

    if let Some(error) = &result.error {
        error!("Job {} failed: {}", result.job_id, error);
    }

    Ok(StatusCode::OK)
}

// --- Cluster job recovery sweep ---
//
// Job pickup is CREATE-only and the in-memory delay timer lives on the assigned
// SSP, so a job created/delayed on an SSP that then dies is never run. In
// singlenode the SSP's own sweep handles this; in cluster mode that sweep is
// disabled (a lone SSP doesn't know ownership), so the scheduler — a singleton
// that holds the SSP pool and can reach the upstream DB — owns recovery here.

/// How often the cluster recovery sweep runs.
const JOB_RECOVERY_INTERVAL_SECS: u64 = 30;
/// Only re-dispatch `pending` rows older than this, so the sweep never races the
/// live CREATE-pickup path on freshly created jobs.
const JOB_RECOVERY_PENDING_GRACE_SECS: u64 = 30;
/// How far past its due time a `pending` row must be before its claim is treated as
/// stale and re-dispatched regardless of assignee liveness (the `long_overdue` hatch
/// below). Kept well above any realistic job timeout.
///
/// It no longer governs `processing` rows: those are reclaimed when their LEASE
/// expires (`schedule_core::sql::LEASE_EXPIRED`), which is per-job rather than a flat
/// window and does not consult ownership at all.
const JOB_RECOVERY_STALE_PROCESSING_SECS: u64 = 600;

/// Outbox job-table names from `SPKY_JOB_CONFIG`. Accepts BOTH shapes the
/// platform emits — an object keyed by table name (scheduler-native), or the
/// SSP routing array `[{name, base_url, table, ...}]` — so a deploy that
/// copies the SSP value verbatim can never silently disable job recovery.
/// Empty when unset/invalid — the sweep then has nothing to do.
fn job_tables_from_env() -> Vec<String> {
    let raw = match std::env::var("SPKY_JOB_CONFIG") {
        Ok(raw) => raw,
        Err(_) => return Vec::new(),
    };
    job_tables_from_json(&raw)
}

fn job_tables_from_json(raw: &str) -> Vec<String> {
    match serde_json::from_str::<Value>(raw) {
        Ok(Value::Object(map)) => map.into_iter().map(|(k, _)| k).collect(),
        Ok(Value::Array(entries)) => entries
            .iter()
            .filter_map(|e| e.get("table").and_then(|t| t.as_str()))
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .collect(),
        Ok(_) | Err(_) => {
            warn!("SPKY_JOB_CONFIG is not a JSON object or array; cluster job recovery disabled");
            Vec::new()
        }
    }
}

/// Connect a fresh SurrealDB client to the upstream, mirroring `Scheduler::start`.
async fn connect_remote(db: &DbConfig) -> Result<Surreal<Client>> {
    maintenance::db::connect_http(db).await
}

/// The row's `assignee` has left the pool (or was never stamped).
///
/// This is a reason to reclaim a row EARLY. It is deliberately not a precondition for
/// reclaiming one at all, and treating it as one is what let `processing` rows sit
/// forever: "that process is up" and "this work is progressing" are different
/// questions, and three things routinely make the first one lie —
///
/// - an SSP that restarts under a stable `SPKY_SSP_ID` (which `spky dev` and the
///   benchmark stack both set) re-registers under the same id, so every row its
///   previous process abandoned still has a live owner;
/// - `SspPool::all()` includes `Bootstrapping`, `Replaying` and `Failed` entries, so
///   "live" means present in the map, not healthy;
/// - an SSP can be alive and heartbeating while having lost its in-memory queue.
///
/// The authority on whether work is still progressing is the row's lease. See
/// `schedule_core::sql::LEASE_EXPIRED`.
fn is_orphaned(row: &Value, live: &HashSet<String>) -> bool {
    match row.get("assignee").and_then(|v| v.as_str()) {
        Some(a) => !live.contains(a),
        None => true,
    }
}

/// Start the cluster job recovery sweep (scheduler-driven, cluster mode only).
pub async fn start_job_recovery_sweep(
    ssp_pool: Arc<RwLock<SspPool>>,
    transport: Arc<HttpTransport>,
    db_config: Arc<DbConfig>,
) {
    let job_tables = job_tables_from_env();
    if job_tables.is_empty() {
        info!("Cluster job recovery sweep idle (no job tables in SPKY_JOB_CONFIG)");
        return;
    }

    tokio::spawn(async move {
        // Connect lazily, rebuilding the handle on the next tick if a pass fails.
        let mut db: Option<Surreal<Client>> = None;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(
            JOB_RECOVERY_INTERVAL_SECS,
        ));
        loop {
            interval.tick().await;

            if db.is_none() {
                match connect_remote(&db_config).await {
                    Ok(conn) => db = Some(conn),
                    Err(e) => {
                        warn!(error = format!("{e:#}"), "Cluster job recovery: DB connect failed; retrying next tick");
                        continue;
                    }
                }
            }
            let conn = db.as_ref().unwrap();

            for table in &job_tables {
                if let Err(e) = recover_table_once(conn, &ssp_pool, &transport, table).await {
                    warn!(table = %table, error = %e, "Cluster job recovery pass failed; will reconnect");
                    db = None; // force a reconnect on the next tick
                    break;
                }
            }
        }
    });
    info!(
        interval_secs = JOB_RECOVERY_INTERVAL_SECS,
        "Cluster job recovery sweep started"
    );
}

/// One recovery pass for a single job table. Errors bubble up so the caller can
/// drop and rebuild the DB handle.
async fn recover_table_once(
    db: &Surreal<Client>,
    ssp_pool: &Arc<RwLock<SspPool>>,
    transport: &Arc<HttpTransport>,
    table: &str,
) -> Result<()> {
    // Snapshot live pool membership (ready or not): a job owned by a current
    // member is handled there; only re-dispatch jobs whose owner left the pool.
    let live: HashSet<String> = {
        let pool = ssp_pool.read().await;
        pool.all().into_iter().map(|s| s.id.clone()).collect()
    };

    // 1. Due pending rows older than the grace period. A row is DUE at
    //    `created_at + <delay>`, which keeps a delayed job inside its delay
    //    window from being recovered early; `??` falls back when `delay` is unset
    //    (NONE) and `delay=0` ⇒ ready. Must stay byte-identical to
    //    `ssp_node::jobs::PENDING_DUE_CLAUSE` — the scheduler crate can't depend
    //    on ssp-node, so this is a deliberate copy, not a divergence.
    //    `long_overdue`: the row has been DUE for longer than the stale
    //    window. A pending row claimed by a live SSP is normally in flight
    //    there (enqueued / sleeping through a delay or retry backoff), so
    //    is_orphaned skips it — but a claim can outlive the in-memory work
    //    (enqueue marker leaked on an error path, SSP dropped the queue
    //    without dying). Any healthy in-flight job acts within moments of
    //    its due time; ten minutes past due and still pending means the
    //    claim is stale, so re-dispatch regardless of assignee liveness.
    //    `/job/recover` only acts on pending rows and dedupes via
    //    mark_enqueued, so a false positive is a no-op.
    let pending_q = format!(
        "SELECT type::string(id) AS id, assignee, \
         ((created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now() - {stale}s) AS long_overdue \
         FROM {table} \
         WHERE status = 'pending' AND updated_at < time::now() - {grace}s \
         AND (created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now()",
        grace = JOB_RECOVERY_PENDING_GRACE_SECS,
        stale = JOB_RECOVERY_STALE_PROCESSING_SECS,
    );
    let mut resp = db.query(&pending_q).await?;
    let pending: Vec<Value> = resp.take(0)?;
    for row in &pending {
        let Some(id) = row.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let long_overdue = row.get("long_overdue").and_then(|v| v.as_bool()).unwrap_or(false);
        if is_orphaned(row, &live) || long_overdue {
            if long_overdue && !is_orphaned(row, &live) {
                warn!(job_id = %id, "Cluster job recovery: pending job long overdue despite live assignee — re-dispatching");
            }
            dispatch_recover(ssp_pool, transport, id).await;
        }
    }

    // 2. `processing` rows whose LEASE has expired. Reset to pending first (the SSP
    //    `/job/recover` only acts on pending rows), then dispatch.
    //
    //    The query selects on the lease and NOT on ownership. There used to be an
    //    `is_orphaned` gate here with no escape hatch — the `long_overdue` one above
    //    covers `pending` rows only — which meant a `processing` row whose assignee was
    //    still in the pool was never reset at any age, by any path in this repo. A
    //    dead assignee now only makes the reclaim happen SOONER.
    let stale_q = format!(
        "SELECT type::string(id) AS id, assignee, lease_until, updated_at FROM {table} \
         WHERE status = 'processing' AND ({expired} OR assignee != NONE)",
        expired = schedule_core::sql::LEASE_EXPIRED,
    );
    let mut resp = db.query(&stale_q).await?;
    let processing: Vec<Value> = resp.take(0)?;
    for row in &processing {
        let Some(id) = row.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let record_id = match RecordId::parse_simple(id) {
            Ok(r) => r,
            Err(e) => {
                warn!(job_id = %id, error = %e, "Cluster job recovery: bad job id; skipping");
                continue;
            }
        };
        // Two ways in, and the statement decides which applies rather than the caller:
        // the lease has run out, or the owner is gone. `RETURN AFTER` so an empty
        // result means neither held and the row is left alone — which also closes the
        // race where it terminalized between the SELECT above and this write.
        let orphaned = is_orphaned(row, &live);
        let reset = db
            .query(format!(
                "UPDATE $id SET status = 'pending', assignee = NONE, lease_until = NONE, \
                 lease_epoch = (lease_epoch ?? 0) + 1, updated_at = time::now() \
                 WHERE status = 'processing' AND ({expired} OR $orphaned) RETURN AFTER",
                expired = schedule_core::sql::LEASE_EXPIRED,
            ))
            .bind(("id", record_id))
            .bind(("orphaned", orphaned))
            .await;
        let reclaimed: Vec<Value> = match reset {
            Ok(mut r) => r.take(0).unwrap_or_default(),
            Err(e) => {
                warn!(job_id = %id, error = %e, "Cluster job recovery: could not reclaim; skipping");
                continue;
            }
        };
        if reclaimed.is_empty() {
            continue;
        }
        if !orphaned {
            warn!(job_id = %id, "Cluster job recovery: processing job past its lease despite a live assignee — reclaiming");
        }
        dispatch_recover(ssp_pool, transport, id).await;
    }

    Ok(())
}

/// Pick a ready SSP round-robin and POST `/job/recover` for one job id.
async fn dispatch_recover(
    ssp_pool: &Arc<RwLock<SspPool>>,
    transport: &Arc<HttpTransport>,
    id: &str,
) {
    let target = {
        let mut pool = ssp_pool.write().await;
        match pool.select_for_query() {
            Some(sid) => pool.get(&sid).map(|s| (sid.clone(), s.url.clone())),
            None => None,
        }
    };
    let Some((ssp_id, ssp_url)) = target else {
        warn!(job_id = %id, "Cluster job recovery: no ready SSP to recover job");
        return;
    };

    let req = JobActionRequest { id: id.to_string() };
    match transport.post_to_ssp(&ssp_url, "/job/recover", &req).await {
        Ok(_) => info!(job_id = %id, ssp_id = %ssp_id, "Cluster job recovery: re-dispatched job"),
        Err(e) => {
            warn!(job_id = %id, ssp_id = %ssp_id, error = %e, "Cluster job recovery: /job/recover failed")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::job_tables_from_json;

    #[test]
    fn object_shape_yields_keys() {
        let mut tables = job_tables_from_json(r#"{"job":{},"statistics_job":{}}"#);
        tables.sort();
        assert_eq!(tables, vec!["job", "statistics_job"]);
    }

    #[test]
    fn ssp_array_shape_yields_table_fields() {
        let mut tables = job_tables_from_json(
            r#"[{"name":"analytics","base_url":"http://analytics:3662","table":"statistics_job"},
                {"name":"gamesync","base_url":"http://gamesync:3661","table":"job"},
                {"name":"broken","base_url":"http://x","table":""}]"#,
        );
        tables.sort();
        assert_eq!(tables, vec!["job", "statistics_job"]);
    }

    #[test]
    fn empty_and_invalid_shapes_disable_sweep() {
        assert!(job_tables_from_json("{}").is_empty());
        assert!(job_tables_from_json("[]").is_empty());
        assert!(job_tables_from_json("null").is_empty());
        assert!(job_tables_from_json("not json").is_empty());
        assert!(job_tables_from_json("\"job\"").is_empty());
    }
}
