use anyhow::Result;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::Response,
    routing::{get, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::backend_health::{BackendHealthCache, BackendStatus, SharedBackendConfigs};
use crate::ingest::{pending_events_snapshot, IngestState};
use crate::job_scheduler::JobTracker;
use crate::query::QueryTracker;
use crate::replica::Replica;
use crate::router::{SspPool, SspState};

/// Local IP, resolved once at startup by `init_local_ip`. `hostname -I` is a
/// blocking fork+exec — running it per `/info` request stalled a runtime
/// worker; under memory pressure the fork itself can hang.
static LOCAL_IP: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

/// Resolve and cache the local IP (first non-loopback IPv4). Call once from
/// `main` before serving; runs the blocking command on the blocking pool.
pub async fn init_local_ip() {
    let ip = tokio::task::spawn_blocking(get_local_ip)
        .await
        .ok()
        .flatten();
    let _ = LOCAL_IP.set(ip);
}

/// Get the local IP address from network interfaces (first non-loopback IPv4)
fn get_local_ip() -> Option<String> {
    // Try reading from /proc/net/fib_trie or use a simpler approach
    // Parse IP from the hostname command or network config
    if let Ok(output) = std::process::Command::new("hostname").arg("-I").output() {
        let ips = String::from_utf8_lossy(&output.stdout);
        return ips.split_whitespace().next().map(|s| s.to_string());
    }
    None
}
use crate::SchedulerStatus;

/// Metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    pub scheduler: SchedulerMetrics,
    pub ssps: Vec<SspMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerMetrics {
    pub total_ssps: usize,
    pub ready_ssps: usize,
    pub total_queries: usize,
    pub running_jobs: usize,
    pub uptime_seconds: u64,
    pub pending_events: usize,
    pub snapshot_seq: u64,
    pub latest_seq: u64,
    pub lag: u64,
    /// E2E heartbeat: last successful probe latency (`None` = never / off).
    ///
    /// This is the LAST value, not necessarily a current one — always read it
    /// with `heartbeat_stale`. A scraper that ignores staleness re-publishes a
    /// frozen latency forever and draws a healthy flat line over a dead
    /// pipeline, which is exactly how this probe failed on 2026-08-09.
    /// Sync tables whose replica row count differed from upstream on the
    /// last drift check (`crate::drift`). Non-zero for more than one check
    /// means a re-clone is due or blocked (cooldown, disabled, stuck).
    pub replica_drift_tables: usize,
    /// Tables an automatic re-clone did not fix; they need an operator.
    pub replica_stuck_tables: usize,
    /// Automatic replica re-clones since the scheduler started.
    pub replica_auto_reclones: u64,
    pub heartbeat_last_e2e_ms: Option<u64>,
    /// Epoch-ms of the last successful probe (`None` = never / off).
    pub heartbeat_last_ok_epoch_ms: Option<u64>,
    pub heartbeat_consecutive_failures: u32,
    pub heartbeat_enabled: bool,
    /// No successful probe within the grace window — treat the latency above
    /// as unknown, not as the current value.
    pub heartbeat_stale: bool,
    /// The probe currently has nothing to measure (e.g. no ready SSPs during a
    /// bootstrap). Distinct from stale: nothing failed and nothing timed out,
    /// but the latency above describes a stack that is not serving anyone.
    pub heartbeat_blocked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SspMetrics {
    pub id: String,
    pub query_count: usize,
    pub views: usize,
    pub cpu_usage: Option<f64>,
    pub memory_usage: Option<f64>,
    pub last_heartbeat_seconds_ago: u64,
}

/// Metrics state
#[derive(Clone)]
pub struct MetricsState {
    pub ssp_pool: Arc<RwLock<SspPool>>,
    pub query_tracker: Arc<QueryTracker>,
    pub job_tracker: Arc<JobTracker>,
    pub start_time: std::time::Instant,
    pub scheduler_id: String,
    pub status: Arc<RwLock<SchedulerStatus>>,
    pub backend_health: BackendHealthCache,
    pub shared_backend_configs: SharedBackendConfigs,
    pub ingest: IngestState,
    pub replica: Arc<RwLock<Replica>>,
    pub surrealdb_version: Arc<RwLock<String>>,
    pub heartbeat: Arc<crate::heartbeat::HeartbeatStats>,
    pub heartbeat_config: crate::heartbeat::Config,
    /// Replica-vs-upstream drift state (see `crate::drift`).
    pub drift: Arc<RwLock<crate::drift::DriftState>>,
    pub drift_config: crate::drift::DriftConfig,
}

/// Create metrics router
pub fn create_metrics_router(state: MetricsState) -> Router {
    Router::new()
        .route("/metrics", get(get_metrics))
        .route("/health", get(health_check))
        // Pure liveness: touches NO shared state, NO locks, NO awaits. The
        // contract for external monitors: `/health/live` 200 while `/health`
        // times out = the process is alive but the runtime/locks are wedged —
        // restart it. Keep this handler dependency-free forever.
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(ready_check))
        .route("/health/snapshot", get(snapshot_check))
        .route("/info", get(info_handler))
        .route("/info/text", get(info_text_handler))
        .route("/backends", put(update_backends_handler))
        // Permissive CORS so browser DevTools can read /info cross-origin
        // (simple GETs, no preflight needed).
        .layer(middleware::from_fn(cors_allow_all))
        // Health/introspection must answer fast or fail fast: a probe that
        // hangs is worse than a probe that 408s (inner layer wins over the
        // global one in main.rs). /health/snapshot legitimately scans the
        // replica, hence 30s rather than a tighter budget.
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(30),
        ))
        .with_state(state)
}

/// Add `Access-Control-Allow-Origin: *` to scheduler responses so the DevTools
/// can read the version info cross-origin.
async fn cors_allow_all(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    res.headers_mut().insert(
        axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        axum::http::HeaderValue::from_static("*"),
    );
    res
}

/// Get metrics
async fn get_metrics(
    State(state): State<MetricsState>,
) -> Result<Json<Metrics>, (StatusCode, String)> {
    // Lock discipline (all handlers in this file): copy what we need out of a
    // guard inside a block and DROP it before the next await. tokio's RwLock
    // is write-preferring — a read guard held across an await plus one queued
    // writer blocks every later reader, which is how one long drain turned
    // into total HTTP silence (2026-08-08).
    let (total_ssps, ready_ssps, ssps) = {
        let pool = state.ssp_pool.read().await;
        let total_ssps = pool.count();
        let ready_ssps = pool
            .all()
            .iter()
            .filter(|ssp| pool.is_ready(&ssp.id))
            .count();
        let ssps: Vec<SspMetrics> = pool
            .all()
            .iter()
            .map(|ssp| {
                let now = std::time::Instant::now();
                let last_heartbeat_seconds_ago = now
                    .duration_since(ssp.last_heartbeat)
                    .as_secs();

                SspMetrics {
                    id: ssp.id.clone(),
                    query_count: ssp.query_count,
                    views: ssp.views,
                    cpu_usage: ssp.cpu_usage,
                    memory_usage: ssp.memory_usage,
                    last_heartbeat_seconds_ago,
                }
            })
            .collect();
        (total_ssps, ready_ssps, ssps)
    };

    let query_assignments = state.query_tracker.all().await;
    let running_jobs = state.job_tracker.running_count().await;
    let pending = pending_events_snapshot(&state.ingest).await;

    let hb = &state.heartbeat;
    let hb_last_e2e = hb.last_e2e_ms.load(std::sync::atomic::Ordering::Relaxed);
    let hb_last_ok = hb.last_ok_epoch_ms.load(std::sync::atomic::Ordering::Relaxed);
    let (drift_tables, stuck_tables, auto_reclones) = {
        let d = state.drift.read().await;
        (
            d.last_report.as_ref().map(|r| r.mismatched_tables().len()).unwrap_or(0),
            d.stuck.len(),
            d.auto_reclones,
        )
    };
    let metrics = Metrics {
        scheduler: SchedulerMetrics {
            total_ssps,
            ready_ssps,
            total_queries: query_assignments.len(),
            running_jobs,
            uptime_seconds: state.start_time.elapsed().as_secs(),
            pending_events: pending.pending_events,
            snapshot_seq: pending.snapshot_seq,
            latest_seq: pending.latest_seq,
            lag: pending.lag,
            replica_drift_tables: drift_tables,
            replica_stuck_tables: stuck_tables,
            replica_auto_reclones: auto_reclones,
            heartbeat_last_e2e_ms: (hb_last_e2e != u64::MAX).then_some(hb_last_e2e),
            heartbeat_last_ok_epoch_ms: (hb_last_ok > 0).then_some(hb_last_ok),
            heartbeat_consecutive_failures: hb
                .consecutive_failures
                .load(std::sync::atomic::Ordering::Relaxed),
            heartbeat_enabled: hb.enabled.load(std::sync::atomic::Ordering::Relaxed),
            heartbeat_stale: hb.is_stale(
                &state.heartbeat_config,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
            ),
            heartbeat_blocked: hb.blocked_reason().is_some(),
        },
        ssps,
    };

    Ok(Json(metrics))
}

/// Optional lag threshold (in seqs) above which `/health` reports `degraded`.
/// Off by default: lag is workload-dependent and only an operator knows what
/// "too far behind" means for their deployment.
fn health_max_lag() -> Option<u64> {
    std::env::var("SPKY_HEALTH_MAX_LAG")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n: &u64| *n > 0)
}

/// Health check
async fn health_check(
    State(state): State<MetricsState>,
) -> (StatusCode, Json<serde_json::Value>) {
    let (total_ssps, ready_ssps, has_active_bootstrap) = {
        let pool = state.ssp_pool.read().await;
        let total = pool.count();
        let ready = pool
            .all()
            .iter()
            .filter(|ssp| pool.is_ready(&ssp.id))
            .count();
        (total, ready, pool.has_active_bootstrap())
    };

    // Counts only — the guard must not live past this block (see get_metrics).
    let (total_backends, healthy_backends, unhealthy_backends, unreachable_backends) = {
        let backends = state.backend_health.read().await;
        (
            backends.len(),
            backends.iter().filter(|b| b.status == BackendStatus::Healthy).count(),
            backends.iter().filter(|b| b.status == BackendStatus::Unhealthy).count(),
            backends.iter().filter(|b| b.status == BackendStatus::Unreachable).count(),
        )
    };

    let ssps_ok = ready_ssps > 0;
    let all_backends_ok = total_backends == 0 || healthy_backends == total_backends;
    let all_backends_down = total_backends > 0 && (unreachable_backends + unhealthy_backends) == total_backends;

    // Snapshot-pipeline visibility: `stalled` is the exact latch predicate —
    // a frozen/updating status that no active bootstrap justifies. With the
    // updater's self-recovery this should clear within one tick; it degrades
    // (200 + "degraded") rather than 503s because an orchestrator restart
    // provably does NOT fix it (the WAL refills the same buffer on boot).
    let scheduler_status = *state.status.read().await;
    let status_label = match scheduler_status {
        SchedulerStatus::Cloning => "cloning",
        SchedulerStatus::Ready => "ready",
        SchedulerStatus::SnapshotFrozen => "frozen",
        SchedulerStatus::SnapshotUpdating => "updating",
        SchedulerStatus::Restoring => "restoring",
    };
    let stalled = !has_active_bootstrap
        && match scheduler_status {
            SchedulerStatus::SnapshotFrozen => true,
            // `SnapshotUpdating` is set by the periodic updater, under
            // `drain_lock` and only when there is a backlog, so a drain in
            // flight is normal operation and must not read as a fault. What is
            // a fault is the status outliving the work — a tick that died
            // mid-drain leaves it latched long after the lock was released.
            // `try_lock` separates the two with no extra bookkeeping, because
            // the updater takes `drain_lock` BEFORE it sets the status: lock
            // held means a real drain, lock free means a latch.
            SchedulerStatus::SnapshotUpdating => state.ingest.drain_lock.try_lock().is_ok(),
            _ => false,
        };
    let pending = pending_events_snapshot(&state.ingest).await;
    let lag_exceeded = health_max_lag().is_some_and(|max| pending.lag > max);

    // E2E heartbeat staleness degrades rather than 503s, same reasoning as
    // `stalled` above: a restart does not fix a dead upstream event pipeline.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    // Blocked degrades as well as stale: "no ready SSPs" means no client can
    // be receiving changes, which is not an operational stack however recently
    // the last probe succeeded.
    let heartbeat_stale = !state.heartbeat.is_current(&state.heartbeat_config, now_ms);
    // Scalars only — the sample window is `/info`'s job (it feeds the DevTools
    // sparkline); a health probe should stay small.
    let heartbeat = state
        .heartbeat
        .snapshot(&state.heartbeat_config, now_ms, false);

    let (status_code, status_str) = if !ssps_ok || all_backends_down {
        (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
    } else if ssps_ok && all_backends_ok && !stalled && !lag_exceeded && !heartbeat_stale {
        (StatusCode::OK, "healthy")
    } else {
        (StatusCode::OK, "degraded")
    };

    (status_code, Json(serde_json::json!({
        "status": status_str,
        "ssps": {
            "ready": ready_ssps,
            "total": total_ssps,
        },
        "backends": {
            "healthy": healthy_backends,
            "unhealthy": unhealthy_backends,
            "unreachable": unreachable_backends,
            "total": total_backends,
        },
        "scheduler": {
            "status": status_label,
            "pending_events": pending.pending_events,
            "snapshot_seq": pending.snapshot_seq,
            "latest_seq": pending.latest_seq,
            "lag": pending.lag,
            "stalled": stalled,
        },
        "heartbeat": heartbeat
    })))
}

/// Bootstrap-readiness probe used by `spky dev` (and friends) to wait until
/// the scheduler has finished cloning the upstream SurrealDB into its replica.
/// Returns 503 while in `Cloning`, 200 once `Ready` (or any post-clone state).
async fn ready_check(
    State(state): State<MetricsState>,
) -> (StatusCode, Json<serde_json::Value>) {
    let status = *state.status.read().await;
    let status_str = match status {
        SchedulerStatus::Cloning => "cloning",
        SchedulerStatus::Ready => "ready",
        SchedulerStatus::SnapshotFrozen => "frozen",
        SchedulerStatus::SnapshotUpdating => "updating",
        SchedulerStatus::Restoring => "restoring",
    };
    let code = if status == SchedulerStatus::Cloning {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    let pending = pending_events_snapshot(&state.ingest).await;
    (
        code,
        Json(serde_json::json!({
            "status": status_str,
            "pending_events": pending.pending_events,
            "lag": pending.lag,
        })),
    )
}

/// Per-table replica record counts AND content hashes. Used by `spky verify`
/// to confirm the snapshot is complete and identical-by-content vs the
/// upstream SurrealDB and each SSP's circuit store.
async fn snapshot_check(
    State(state): State<MetricsState>,
) -> (StatusCode, Json<serde_json::Value>) {
    let (counts, hashes) = {
        let replica = state.replica.read().await;
        let counts = replica.record_counts_per_table().await;
        let hashes = replica.snapshot_hashes().clone();
        (counts, hashes)
    };
    let pending = pending_events_snapshot(&state.ingest).await;
    let drift = crate::drift::state_json(&*state.drift.read().await, &state.drift_config);
    match counts {
        Ok(counts) => {
            let total: usize = counts.iter().map(|(_, c)| c).sum();
            let tables: serde_json::Map<String, serde_json::Value> = counts
                .into_iter()
                .map(|(t, c)| (t, serde_json::Value::from(c)))
                .collect();
            let hashes_value: serde_json::Map<String, serde_json::Value> = hashes
                .into_iter()
                .map(|(t, h)| (t, serde_json::Value::String(h)))
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "tables": tables,
                    "hashes": hashes_value,
                    "total_records": total,
                    "snapshot_seq": pending.snapshot_seq,
                    "latest_seq": pending.latest_seq,
                    "lag": pending.lag,
                    "drift": drift,
                })),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}

/// Patterns that indicate a sensitive environment variable (checked case-insensitively).
const SENSITIVE_PATTERNS: &[&str] = &[
    "key", "secret", "token", "password", "pass", "auth",
    "credential", "private",
];

/// Mask values in an env map where the key matches any sensitive pattern.
pub fn mask_sensitive_env(
    env: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    env.into_iter()
        .map(|(k, v)| {
            let lower = k.to_lowercase();
            let is_sensitive = SENSITIVE_PATTERNS.iter().any(|pat| lower.contains(pat));
            if is_sensitive {
                (k, serde_json::Value::String("****".to_string()))
            } else {
                (k, v)
            }
        })
        .collect()
}

/// Convert `["KEY=value", ...]` to a `serde_json::Map`.
pub fn vec_env_to_map(entries: &[String]) -> serde_json::Map<String, serde_json::Value> {
    entries
        .iter()
        .map(|entry| {
            if let Some((k, v)) = entry.split_once('=') {
                (k.to_string(), serde_json::Value::String(v.to_string()))
            } else {
                (entry.clone(), serde_json::Value::String(String::new()))
            }
        })
        .collect()
}

/// Convert `HashMap<String, String>` to a `serde_json::Map`.
fn hashmap_env_to_map(
    map: &std::collections::HashMap<String, String>,
) -> serde_json::Map<String, serde_json::Value> {
    map.iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect()
}

/// Info handler — returns entity list with identity and status
async fn info_handler(
    State(state): State<MetricsState>,
) -> Json<serde_json::Value> {
    Json(serde_json::Value::Array(build_entities(&state).await))
}

/// Assemble the `/info` entity list: one `scheduler` entity, one per SSP, one
/// per health-checked backend.
///
/// Split out of `info_handler` so the admin dashboard's `/admin/api/overview`
/// serves the *same* view of the cluster rather than a second, drifting one.
pub async fn build_entities(state: &MetricsState) -> Vec<serde_json::Value> {
    let scheduler_status = match *state.status.read().await {
        SchedulerStatus::Cloning => "cloning",
        SchedulerStatus::Ready => "ready",
        SchedulerStatus::SnapshotFrozen => "frozen",
        SchedulerStatus::SnapshotUpdating => "updating",
        SchedulerStatus::Restoring => "restoring",
    };

    // Copy everything pool-derived out and drop the guard before the awaits
    // below — this guard held across `pending_events_snapshot` was one half
    // of the reader-convoy wedge (see get_metrics).
    let (total_views, ssp_entities) = {
        let pool = state.ssp_pool.read().await;
        let now = std::time::Instant::now();
        let total_views: usize = pool.all().iter().map(|ssp| ssp.views).sum();
        let ssp_entities: Vec<serde_json::Value> = pool
            .all()
            .iter()
            .map(|ssp| {
                let ssp_status = match pool.get_state(&ssp.id) {
                    Some(SspState::Bootstrapping) => "bootstrapping",
                    Some(SspState::Replaying) => "replaying",
                    Some(SspState::Ready) => "ready",
                    Some(SspState::Lagging) => "lagging",
                    None => "unknown",
                };
                let last_heartbeat_seconds_ago = now
                    .duration_since(ssp.last_heartbeat)
                    .as_secs();
                // Extract IP from SSP's registered URL (e.g. "http://10.100.1.30:8667" -> "10.100.1.30")
                let ssp_ip = ssp.url.trim_start_matches("http://")
                    .split(':').next()
                    .map(|s| s.to_string());
                let ssp_env = ssp.env.as_ref()
                    .map(|e| serde_json::Value::Object(mask_sensitive_env(hashmap_env_to_map(e))));
                // How long this SSP has held its current phase. For a
                // bootstrapping or replaying one this is the only universally
                // available progress signal (`bootstrap` below is richer but
                // only arrives from an SSP new enough to report it).
                let state_seconds = pool
                    .state_since(&ssp.id)
                    .map(|t| now.duration_since(t).as_secs());
                serde_json::json!({
                    "entity": "ssp",
                    "id": ssp.id,
                    "ip": ssp_ip,
                    "status": ssp_status,
                    "views": ssp.views,
                    "version": ssp.version,
                    "uptime_seconds": now.duration_since(ssp.connected_at).as_secs(),
                    "last_heartbeat_seconds_ago": last_heartbeat_seconds_ago,
                    "state_seconds": state_seconds,
                    "buffered_events": pool.buffer_size(&ssp.id),
                    "publication": pool.publication(&ssp.id),
                    "bootstrap": ssp.bootstrap,
                    "env": ssp_env,
                })
            })
            .collect();
        (total_views, ssp_entities)
    };

    // Collect scheduler environment variables
    let env_vars: serde_json::Map<String, serde_json::Value> = [
        "SPKY_DB_URL", "SPKY_DB_NS",
        "SPKY_DB_NAME", "SPKY_DB_USER",
        "SPKY_SCHEDULER_ID",
    ].iter().filter_map(|&key| {
        std::env::var(key).ok().map(|val| (key.to_string(), serde_json::Value::String(val)))
    }).collect();

    // Cached at startup — never fork on the request path.
    let scheduler_ip = LOCAL_IP.get().cloned().flatten();

    let pending = pending_events_snapshot(&state.ingest).await;
    let surrealdb_version = state.surrealdb_version.read().await.clone();

    // With the sample window: this is the only surface browser DevTools can
    // reach (via `fn::spooky::info()`), and the panel has no poller of its own,
    // so the recent-cycle history has to come down with the entity.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let heartbeat = state
        .heartbeat
        .snapshot(&state.heartbeat_config, now_ms, true);

    let mut entities = vec![serde_json::json!({
        "entity": "scheduler",
        "id": state.scheduler_id,
        "ip": scheduler_ip,
        "status": scheduler_status,
        "views": total_views,
        "version": env!("CARGO_PKG_VERSION"),
        "surrealdb_version": surrealdb_version,
        "uptime_seconds": state.start_time.elapsed().as_secs(),
        "last_heartbeat_seconds_ago": null,
        "pending_events": pending.pending_events,
        "snapshot_seq": pending.snapshot_seq,
        "latest_seq": pending.latest_seq,
        "lag": pending.lag,
        "heartbeat": heartbeat,
        "env": mask_sensitive_env(env_vars),
    })];

    entities.extend(ssp_entities);

    // Read cached backend health status (guard scoped to this block).
    {
        let backend_entries = state.backend_health.read().await;
        for entry in backend_entries.iter() {
            let backend_env = entry.env.as_ref()
                .map(|e| serde_json::Value::Object(mask_sensitive_env(vec_env_to_map(e))));
            entities.push(serde_json::json!({
                "entity": "backend",
                "id": entry.name,
                "ip": entry.ip(),
                "url": entry.url,
                "port": entry.port,
                "status": entry.status.as_str(),
                "healthcheck": entry.healthcheck,
                "last_checked": entry.last_checked.map(|t| {
                    chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339()
                }),
                "last_healthy": entry.last_healthy.map(|t| {
                    chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339()
                }),
                "response_time_ms": entry.response_time_ms,
                "env": backend_env,
            }));
        }
    }

    entities
}

/// Update backend health check configs at runtime (called by orchestrator on redeploy).
async fn update_backends_handler(
    State(state): State<MetricsState>,
    Json(new_backends): Json<Vec<crate::config::BackendHealthConfig>>,
) -> StatusCode {
    info!(count = new_backends.len(), "Updating backend configs via PUT /backends");
    crate::backend_health::update_backends(
        &state.shared_backend_configs,
        &state.backend_health,
        new_backends,
    ).await;
    StatusCode::OK
}

/// Info handler that returns plain text JSON (for SurrealDB DEFINE API consumption)
async fn info_text_handler(
    State(state): State<MetricsState>,
) -> (axum::http::StatusCode, [(axum::http::header::HeaderName, &'static str); 1], String) {
    let json_resp = info_handler(State(state)).await;
    let json_string = serde_json::to_string(&json_resp.0).unwrap_or_else(|_| "[]".to_string());
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain")],
        json_string,
    )
}

/// Start query reassignment monitor
pub async fn start_query_reassignment_monitor(
    ssp_pool: Arc<RwLock<SspPool>>,
    query_tracker: Arc<QueryTracker>,
    stale_bootstrap_max_age: std::time::Duration,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

        loop {
            interval.tick().await;

            // Reap bootstraps that are genuinely hung, on their own (much
            // longer) budget. Doing it here as well as in the snapshot updater
            // keeps the latency at ~30s instead of the updater's 300s tick —
            // an SSP parked in Bootstrapping holds the snapshot freeze.
            {
                let stale_boots = {
                    let pool = ssp_pool.read().await;
                    pool.stale_active_bootstraps(stale_bootstrap_max_age)
                };
                if !stale_boots.is_empty() {
                    let mut pool = ssp_pool.write().await;
                    for id in stale_boots {
                        warn!(
                            ssp_id = %id,
                            max_age_secs = stale_bootstrap_max_age.as_secs(),
                            "Evicting SSP stuck in bootstrap/replay state"
                        );
                        pool.remove(&id);
                    }
                }
            }

            // Check for stale SSPs. An SSP only starts heartbeating once it
            // reaches Ready, so one that is still Bootstrapping/Replaying is
            // "stale" by construction — evicting it here killed live bootstraps
            // the moment they ran longer than this 30s timeout (which any
            // sizeable dataset does), and the SSP then 404'd on its first
            // heartbeat and exited. Hung bootstraps are reaped by the snapshot
            // updater's `stale_active_bootstraps` sweep instead, which is
            // budgeted against `bootstrap_timeout_secs`.
            let stale_ssps = {
                let pool = ssp_pool.read().await;
                pool.get_stale_ssps(30000) // 30s timeout
                    .into_iter()
                    .filter(|id| !pool.is_active_bootstrap(id))
                    .collect::<Vec<_>>()
            };

            if stale_ssps.is_empty() {
                continue;
            }

            info!(
                "Found {} stale SSPs, reassigning queries",
                stale_ssps.len()
            );

            // Get all query assignments
            let assignments = query_tracker.all().await;

            // For each stale SSP, unassign its queries
            for ssp_id in &stale_ssps {
                let affected_queries: Vec<_> = assignments
                    .iter()
                    .filter(|(_, sid)| *sid == ssp_id)
                    .map(|(qid, _)| qid.clone())
                    .collect();

                if !affected_queries.is_empty() {
                    info!(
                        "Unassigning {} queries from stale SSP {}",
                        affected_queries.len(),
                        ssp_id
                    );

                    for query_id in affected_queries {
                        query_tracker.unassign(&query_id).await;
                        // Client will need to re-register the query
                        info!("Query {} unassigned, client should re-register", query_id);
                    }
                }

                // Remove stale SSP
                let mut pool = ssp_pool.write().await;
                pool.remove(ssp_id);
                info!("Removed stale SSP {}", ssp_id);
            }
        }
    });
}
