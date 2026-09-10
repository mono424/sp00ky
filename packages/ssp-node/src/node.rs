//! The portable node: shared HTTP dispatch over the platform ports.
//!
//! `SspNode::route` is the single entry both shells converge on. Routes
//! migrate here from the VM shell's axum handlers one at a time (the shell
//! mounts the node as its axum FALLBACK, so a route removed from the axum
//! table falls through to the core — every intermediate commit stays green).
//!
//! Migrated so far: `/version`, `/log`, `/reset`, `/job/kill`, `/job/retry`,
//! `/job/recover`. Still in the VM shell: `/ingest`, `/view/*`, `/crdt/apply`,
//! `/debug/*`, `/health`, `/info`, `/info/text`, `/backends`, `/backup/*`.

use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use ssp::circuit::Circuit;

use crate::api::{ApiRequest, ApiResponse, RouteId};
use crate::jobs::{
    enqueue_recovered, fail_if_pending_helper, load_job_record, reset_for_retry_helper,
    set_assignee_helper, JobConfig, JobControl, JobDispatcher, JobEntry,
};
use crate::platform::Platform;
use crate::db_retry::query_retrying;
use crate::ports::{BackendHealth, Db, Telemetry};
use crate::status::SspStatus;

/// Everything the migrated handlers need, platform-independent. Constructed
/// once by the shell and shared with its framework layer.
pub struct SspNode {
    pub platform: Platform,
    pub status: Arc<RwLock<SspStatus>>,
    pub processor: Arc<RwLock<Circuit>>,
    pub job_config: Arc<JobConfig>,
    pub job_control: JobControl,
    /// Admission control for job execution. Everything that wants to run a job
    /// goes through here rather than holding the queue sender directly, so no
    /// path can push a table past its configured concurrency.
    pub job_dispatcher: Arc<JobDispatcher>,
    pub ssp_id: String,
    /// Bearer secret for authenticated routes (`NodeConfig.auth_secret`).
    pub auth_secret: String,
    pub ref_mode: ssp_protocol::RefMode,
    /// Share one operator graph across registrations computing the same thing
    /// (`NodeConfig.merge_views`, env `SPKY_SSP_MERGE_VIEWS`).
    pub merge_views: bool,
    /// The shell binary's version string (surfaced via `/version` + `/info`).
    pub version: &'static str,
    /// Upstream SurrealDB server version, queried once at startup by the shell.
    pub surrealdb_version: String,
    /// Externally reachable IP for this node (`SPKY_SSP_ADVERTISE_ADDR` host),
    /// surfaced via `/info`. `None` when unset.
    pub advertise_ip: Option<String>,
    /// Deployment env vars surfaced verbatim in `/info` (key, value). The shell
    /// collects these — the core never reads the process environment.
    pub info_env: Vec<(String, String)>,
    /// Epoch-ms when the node started, for the `/info` uptime field.
    pub start_epoch_ms: u64,
    /// Bootstrap anomalies worth an operator's eye, surfaced via `/info`. Today:
    /// tables the bootstrap source served zero rows for while upstream has
    /// rows (a drifted scheduler replica). Replaced on every bootstrap.
    pub bootstrap_warnings: Arc<RwLock<Vec<String>>>,
    /// Backend health monitor — standalone mode only (`None` in cluster mode,
    /// where the scheduler owns backend health + `PUT /backends`).
    pub backend_health: Option<Arc<dyn BackendHealth>>,
    /// Server-side CRDT merge cache (`/crdt/apply`).
    pub crdt_cache: Arc<crate::crdt::CrdtCache>,
    /// Per-view latency state, keyed by view id.
    pub view_metrics: Arc<crate::view_metrics::ViewMetrics>,
    /// Coalescing edge-update channel (view register/ingest push `ViewDelta`s
    /// here; the `run_edge_update_service` task batches them). Sender is cheap
    /// to clone and wasm-safe (`tokio::sync::mpsc`).
    pub edge_update_tx: mpsc::UnboundedSender<Vec<ssp::circuit::ViewDelta>>,
    /// When true, anonymous (empty auth) registrations route to the shared
    /// world-readable `_00_list_ref_anon` table.
    pub anonymous_live_queries: bool,
    /// `true` when no scheduler fronts this SSP (standalone mode): this node
    /// handles all jobs and owns the recovery sweep.
    pub standalone: bool,
    /// Declarative-schedule engine — standalone mode only (`None` in cluster
    /// mode, where the scheduler service is the single ticker).
    pub schedule_engine: Option<Arc<schedule_core::ScheduleEngine>>,
    /// View TTL cleanup cadence (seconds) — the `TtlCleanup` timer re-arm.
    pub ttl_cleanup_interval_secs: u64,
    /// Flush cadence for the per-view metrics on `_00_query` (see
    /// `flush_view_metrics`).
    pub view_metrics_flush_ms: u64,
    /// Rows per bootstrap page (keyset pagination) for cold-start rebuild.
    pub bootstrap_page_size: usize,
    /// Periodic circuit-checkpoint cadence; `None` disables (VM).
    pub checkpoint_interval_secs: Option<u64>,
    /// Max age of a restored snapshot before `bootstrap()` rebuilds instead.
    pub max_snapshot_age_secs: u64,
    /// Last `_00_heartbeat` probe this node ingested: `(hb_seq, epoch_ms
    /// received)`. Written at the end of `ingest_handler`, read by
    /// `GET /debug/heartbeat` — the scheduler's e2e probe polls it to measure
    /// DB event → ingest → broadcast → circuit-step latency. std Mutex on
    /// purpose: held for nanoseconds, never across an await.
    pub last_heartbeat_seen: Arc<std::sync::Mutex<Option<(u64, u64)>>>,
}

/// Standalone job-recovery sweep cadence + staleness windows (were shell consts).
/// Row ceiling for `/debug/catchup-rows/:table`.
///
/// The endpoint materializes every row it returns as a JSON tree and then
/// serializes that into a response body, so an unbounded dump costs roughly
/// twice the table — and it is reached on a persistent catch-up mismatch,
/// when the SSP is already in trouble. 50k rows is far above any real
/// divergence (the scheduler logs at most 20 differing rows) while keeping the
/// worst case bounded.
pub const CATCHUP_ROWS_DUMP_LIMIT: usize = 50_000;

pub const JOB_RECOVERY_INTERVAL_SECS: u64 = 60;
const JOB_RECOVERY_PENDING_GRACE_SECS: u64 = 30;
// A `processing` row is reclaimed when its LEASE expires, not at a fixed age — see
// `schedule_core::sql::LEASE_EXPIRED`. The old fixed 600s window survives only as
// that predicate's fallback for a table deployed before the lease fields existed.
/// Rows one sweep pass reads per table per category. A concurrency-limited
/// table can hold an arbitrarily deep pending backlog, and the sweep is a
/// safety net, not the pickup path — the drain is.
const JOB_RECOVERY_PAGE: usize = 200;

/// Drain-timer cadence while any table still has a known backlog. Only armed
/// while there is one, so a quiet deployment never wakes for this.
pub const JOB_DRAIN_INTERVAL_SECS: u64 = 2;

/// Projection for recovery row reads: `type::string(id) AS id` keeps the
/// RecordId out of the flattened JSON.
/// `created_at` / `updated_at` are selected only so the sweep can ORDER BY
/// them: SurrealDB v3 rejects an order idiom that the projection does not
/// select. `from_record` ignores both.
const RECOVERY_FIELDS: &str = "type::string(id) AS id, status, path, payload, retries, \
                               max_retries, retry_strategy, timeout, created_at, updated_at, \
                               lease_until, lease_epoch";

#[derive(Deserialize, Debug)]
struct LogRequest {
    message: String,
    #[serde(default)]
    level: String,
    #[serde(default)]
    data: Option<Value>,
}

#[derive(Deserialize, Debug)]
struct JobActionRequest {
    id: String,
}

fn ok_json(json: Value) -> ApiResponse {
    ApiResponse::json(200, json)
}

fn err_json(status: u16, code: &str, message: impl Into<String>) -> ApiResponse {
    ApiResponse::json(status, json!({ "code": code, "message": message.into() }))
}

impl SspNode {
    /// Dispatch one request. `None` = the route is not (yet) served by the
    /// core — the shell keeps handling it in its own framework layer.
    pub async fn route(&self, req: ApiRequest) -> Option<ApiResponse> {
        let route = RouteId::match_path(req.method, &req.path)?;

        // Bearer auth, identical to the shell middleware it replaces: the
        // presented token must equal the configured secret exactly.
        let requires_auth = route.requires_auth();
        if requires_auth && req.bearer.as_deref() != Some(self.auth_secret.as_str()) {
            return Some(ApiResponse::json(401, Value::Null));
        }

        let mut response = match route {
            RouteId::Version => self.version_handler(),
            RouteId::Log => self.log_handler(&req)?,
            RouteId::Reset => self.reset_handler().await,
            RouteId::Reload => self.reload_handler().await,
            RouteId::JobKill => self.job_kill_handler(&req).await?,
            RouteId::JobRetry => self.job_retry_handler(&req).await?,
            RouteId::JobRecover => self.job_recover_handler(&req).await?,
            RouteId::Health => self.health_handler().await,
            RouteId::Info => self.info_handler().await,
            RouteId::InfoText => self.info_text_handler().await,
            RouteId::BackendsUpdate => self.update_backends_handler(&req).await?,
            RouteId::DebugView { view_id } => self.debug_view_handler(&view_id).await,
            RouteId::DebugDeps => self.debug_deps_handler().await,
            RouteId::DebugHeartbeat => self.debug_heartbeat_handler(),
            RouteId::DebugCatchupRows { table } => self.debug_catchup_rows_handler(&table).await,
            RouteId::DebugMemory => self.debug_memory_handler().await,
            RouteId::CrdtApply => self.crdt_apply_handler(&req).await?,
            RouteId::ViewUnregister => self.unregister_view_handler(&req).await?,
            RouteId::ViewRegister => self.register_view_handler(&req).await?,
            RouteId::Ingest => self.ingest_handler(&req).await?,
            // Known route, not migrated yet — the shell's framework layer
            // still owns it.
            _ => return None,
        };

        if !requires_auth {
            // Public routes carry a permissive CORS header so browser
            // DevTools can read them cross-origin (simple GETs, no preflight).
            response.headers.push(("Access-Control-Allow-Origin", "*".to_string()));
        }
        Some(response)
    }

    fn version_handler(&self) -> ApiResponse {
        ok_json(json!({
            "version": self.version,
            "mode": "streaming"
        }))
    }

    fn log_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        let Ok(payload) = serde_json::from_slice::<LogRequest>(&req.body) else {
            return Some(err_json(422, "bad_body", "invalid log payload"));
        };
        let msg = if let Some(data) = &payload.data {
            format!("{} | data: {}", payload.message, data)
        } else {
            payload.message.clone()
        };

        match payload.level.to_lowercase().as_str() {
            "error" => error!(remote = true, "{}", msg),
            "warn" => warn!(remote = true, "{}", msg),
            "debug" => debug!(remote = true, "{}", msg),
            "trace" => tracing::trace!(remote = true, "{}", msg),
            _ => info!(remote = true, "{}", msg),
        }

        Some(ok_json(Value::Null))
    }

    async fn reset_handler(&self) -> ApiResponse {
        info!("Resetting circuit state");
        wipe_circuit_and_edges(
            self.platform.db.as_ref(),
            &self.processor,
            self.platform.telemetry.as_ref(),
            self.ref_mode,
        )
        .await;
        ok_json(Value::Null)
    }

    /// `POST /admin/reload` — re-scan the DB schema and reload data into a
    /// fresh circuit. Picks up tables/permissions defined AFTER the last
    /// bootstrap (view registration reads permissions captured at bootstrap, so
    /// a schema change is invisible until a reload). Gates ingest (status →
    /// Bootstrapping) for the duration, exactly like a cold start.
    async fn reload_handler(&self) -> ApiResponse {
        match self.reload().await {
            Ok(()) => ok_json(json!({ "status": "ready" })),
            Err(e) => err_json(500, "reload_failed", e.to_string()),
        }
    }

    /// Fresh rebuild-from-DB into a new circuit. Shares the cold-start REBUILD
    /// path ([`crate::bootstrap::rebuild_from_db`]) so schema discovery, view
    /// re-registration from `_00_query`, and reseed all match bootstrap.
    /// Push this node's configuration into the circuit.
    ///
    /// The circuit carries the merge policy so both the live register path and
    /// boot re-registration read ONE source, but neither `Circuit::new` nor
    /// `Circuit::restore` carries configuration — so this must run after every
    /// circuit construction, before anything registers into it.
    pub async fn apply_circuit_policy(&self) {
        let mut circuit = self.processor.write().await;
        circuit.set_merge_views(self.merge_views);
        // Server side only: an ingested body whose `_00_rv` is missing or does
        // not advance gets one that does, so subscribers refetch. The browser
        // circuit must keep versions exactly as its durable store has them.
        circuit.set_monotonic_row_versions(true);
    }

    pub async fn reload(&self) -> anyhow::Result<()> {
        *self.status.write().await = SspStatus::Bootstrapping;
        *self.processor.write().await = Circuit::new();
        self.apply_circuit_policy().await;
        match crate::bootstrap::rebuild_from_db(
            self.platform.db.as_ref(),
            &self.processor,
            self.bootstrap_page_size,
        )
        .await
        {
            Ok(()) => {
                *self.status.write().await = SspStatus::Ready;
                Ok(())
            }
            Err(e) => {
                *self.status.write().await = SspStatus::Failed;
                Err(e)
            }
        }
    }

    /// `POST /job/kill` — stop a job.
    ///
    /// - `processing` and in-flight on this SSP: fire the cancellation and let
    ///   the runner write the terminal status (single-writer invariant).
    /// - `pending`/queued (or `processing` owned elsewhere): set a kill flag
    ///   the runner honors at dequeue.
    /// - `success`/`failed`: idempotent no-op.
    async fn job_kill_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        let Ok(action) = serde_json::from_slice::<JobActionRequest>(&req.body) else {
            return Some(err_json(422, "bad_body", "invalid job action payload"));
        };
        if !valid_record_id(&action.id) {
            return Some(err_json(400, "bad_id", "id must be 'table:key'"));
        }

        let record = match load_job_record(self.platform.db.as_ref(), &action.id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                return Some(err_json(404, "not_found", format!("job '{}' not found", action.id)));
            }
            Err(e) => return Some(err_json(500, "db_error", e.to_string())),
        };

        let status = record.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let resp = match status {
            "success" | "failed" => ok_json(
                json!({ "id": action.id, "status": status, "message": "already terminal; no-op" }),
            ),
            "processing" => {
                if self.job_control.cancel_inflight(&action.id) {
                    ok_json(json!({ "id": action.id, "status": "cancelling", "message": "cancelling in-flight request" }))
                } else {
                    // Processing, but not in-flight on this SSP (cluster:
                    // another SSP owns it, or a stale row). Flag it so any
                    // later re-enqueue is dropped, and report it wasn't local.
                    self.job_control.mark_killed_pending(&action.id);
                    ok_json(json!({ "id": action.id, "status": "processing", "message": "not in-flight on this ssp; kill flag set" }))
                }
            }
            _ => {
                // pending / unknown. Two cooperating actions, in this order:
                //  1. Set the drop-flag first, so a queued copy fails at dequeue
                //     (runner is the sole status writer there — no clobber).
                //  2. Also terminalize the row directly *iff* still pending
                //     (pickup is CREATE-only; an orphaned pending row is never
                //     enqueued, so the flag alone would be a no-op forever).
                self.job_control.mark_killed_pending(&action.id);
                let error_entry = json!({ "code": "killed", "reason": "killed by operator" });
                match fail_if_pending_helper(self.platform.db.as_ref(), &action.id, error_entry).await {
                    Ok(true) => ok_json(
                        json!({ "id": action.id, "status": "failed", "message": "killed pending job" }),
                    ),
                    Ok(false) => ok_json(
                        // Not pending at write time (raced to 'processing', or
                        // already terminal). The flag still guards a queued copy.
                        json!({ "id": action.id, "status": status, "message": "kill flag set; will fail at dequeue" }),
                    ),
                    Err(e) => err_json(500, "db_error", e.to_string()),
                }
            }
        };
        Some(resp)
    }

    /// `POST /job/retry` — re-run a terminal (`failed`/`success`) job. Resets
    /// the row and re-enqueues a fresh `JobEntry` directly, because a plain
    /// `UPDATE` would not re-trigger the CREATE-gated ingest path.
    async fn job_retry_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        let Ok(action) = serde_json::from_slice::<JobActionRequest>(&req.body) else {
            return Some(err_json(422, "bad_body", "invalid job action payload"));
        };
        let Some((table, _)) = action.id.split_once(':') else {
            return Some(err_json(400, "bad_id", "id must be 'table:key'"));
        };

        let Some(backend) = self.job_config.job_tables.get(table).cloned() else {
            return Some(err_json(
                404,
                "unknown_table",
                format!("no job backend configured for table '{}'", table),
            ));
        };

        let record = match load_job_record(self.platform.db.as_ref(), &action.id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                return Some(err_json(404, "not_found", format!("job '{}' not found", action.id)));
            }
            Err(e) => return Some(err_json(500, "db_error", e.to_string())),
        };

        let status = record.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "failed" && status != "success" {
            return Some(err_json(
                409,
                "not_terminal",
                format!("cannot retry job in '{}' state; only 'failed'/'success'", status),
            ));
        }

        // Clear any stale kill flag so the re-enqueued job isn't dropped at
        // dequeue. Order matters: remove the flag BEFORE re-enqueueing.
        self.job_control.take_killed_pending(&action.id);

        if let Err(e) = reset_for_retry_helper(self.platform.db.as_ref(), &action.id).await {
            return Some(err_json(500, "db_error", e.to_string()));
        }

        // Rebuild the JobEntry. `from_record` copies `retries` from the
        // (pre-reset) snapshot, so force it to 0 to honor the retry budget.
        let timeout_override = record.get("timeout").and_then(|v| v.as_u64()).map(|v| v as u32);
        let mut job = JobEntry::from_record(
            action.id.clone(),
            backend.base_url.clone(),
            backend.auth_token.clone(),
            &record,
            backend.effective_timeout(timeout_override),
        );
        job.retries = 0;

        // Through admission, which also carries the `mark_enqueued` guard
        // against a concurrent retry or recovery sweep taking the same id.
        // Refused means the table is at its limit: the row is `pending` again
        // after the reset above, so the drain picks it up in its turn.
        let message = if self.job_dispatcher.try_admit(job).await {
            "re-enqueued"
        } else {
            self.job_dispatcher.note_backlog(table);
            "queued"
        };

        Some(ok_json(json!({ "id": action.id, "status": "pending", "message": message })))
    }

    /// `POST /job/recover` — cluster recovery entry point. This SSP takes
    /// ownership (`assignee = ssp_id`) and re-enqueues through the same
    /// `enqueue_recovered` path as the singlenode sweep, so `mark_enqueued`
    /// still guarantees a job already moving here is never double-executed.
    /// Only acts on rows that are still `pending`.
    async fn job_recover_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        let Ok(action) = serde_json::from_slice::<JobActionRequest>(&req.body) else {
            return Some(err_json(422, "bad_body", "invalid job action payload"));
        };
        let Some((table, _)) = action.id.split_once(':') else {
            return Some(err_json(400, "bad_id", "id must be 'table:key'"));
        };

        let Some(backend) = self.job_config.job_tables.get(table).cloned() else {
            return Some(err_json(
                404,
                "unknown_table",
                format!("no job backend configured for table '{}'", table),
            ));
        };

        let record = match load_job_record(self.platform.db.as_ref(), &action.id).await {
            Ok(Some(r)) => r,
            Ok(None) => {
                return Some(err_json(404, "not_found", format!("job '{}' not found", action.id)));
            }
            Err(e) => return Some(err_json(500, "db_error", e.to_string())),
        };

        // Only recover rows still pending. Anything else (terminal, or
        // processing the scheduler hasn't reset) is left alone so we never
        // re-run a finished or killed job on a race.
        let status = record.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "pending" {
            return Some(ok_json(
                json!({ "id": action.id, "status": status, "message": "not pending; recover skipped" }),
            ));
        }

        // Take ownership before enqueueing so the scheduler sweep sees this
        // SSP as the new owner on its next pass and stops re-dispatching.
        if let Err(e) =
            set_assignee_helper(self.platform.db.as_ref(), &action.id, &self.ssp_id).await
        {
            warn!(job_id = %action.id, error = %e, "Failed to persist assignee on recover");
        }

        let message = if enqueue_recovered(&self.job_dispatcher, &backend, &action.id, &record).await
        {
            "re-enqueued"
        } else {
            // Already queued or in-flight on this SSP (idempotent no-op), or the
            // table is at its limit. Either way the row keeps its place in the
            // backlog and needs no further action from the caller.
            self.job_dispatcher.note_backlog(table);
            "already queued"
        };
        Some(ok_json(json!({ "id": action.id, "status": "pending", "message": message })))
    }

    fn status_str(status: SspStatus) -> &'static str {
        match status {
            SspStatus::Bootstrapping => "bootstrapping",
            SspStatus::Ready => "ready",
            SspStatus::Failed => "failed",
        }
    }

    /// Health check. In standalone mode with a backend health monitor active,
    /// mirrors the scheduler's aggregation: healthy / degraded / unavailable
    /// derived from SSP readiness plus backend health counts.
    async fn health_handler(&self) -> ApiResponse {
        let status = *self.status.read().await;
        let ssp_ready = status == SspStatus::Ready;
        let status_str = Self::status_str(status);

        let Some(backend_health) = &self.backend_health else {
            // Cluster mode (or no monitor): historical shape, untouched.
            let http_status = if ssp_ready { 200 } else { 503 };
            return ApiResponse::json(http_status, json!({ "status": status_str }));
        };

        let c = backend_health.counts().await;
        let all_backends_ok = c.total == 0 || c.healthy == c.total;
        let all_backends_down = c.total > 0 && (c.unreachable + c.unhealthy) == c.total;

        // Same classification as the scheduler's /health, with this SSP's
        // readiness standing in for the pool's ready count.
        let (http_status, aggregate) = if ssp_ready && all_backends_ok {
            (200, "healthy")
        } else if !ssp_ready || all_backends_down {
            (503, "unavailable")
        } else {
            (200, "degraded")
        };

        ApiResponse::json(
            http_status,
            json!({
                "status": aggregate,
                "ssp": { "status": status_str },
                "backends": {
                    "healthy": c.healthy,
                    "unhealthy": c.unhealthy,
                    "unreachable": c.unreachable,
                    "total": c.total,
                }
            }),
        )
    }

    /// Update backend health check configs at runtime (standalone only — in
    /// cluster mode the scheduler owns `PUT /backends`).
    async fn update_backends_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        let Some(backend_health) = &self.backend_health else {
            return Some(err_json(
                409,
                "no_monitor",
                "backend health monitor not active (cluster mode — use the scheduler's /backends)",
            ));
        };
        let Ok(specs) = serde_json::from_slice::<Vec<crate::ports::BackendSpec>>(&req.body) else {
            return Some(err_json(422, "bad_body", "invalid backends payload"));
        };
        info!(count = specs.len(), "Updating backend configs via PUT /backends");
        backend_health.update(specs).await;
        Some(ok_json(Value::Null))
    }

    /// Build the single-entity `/info` array (shared by `/info` and
    /// `/info/text`).
    async fn info_value(&self) -> Value {
        let status_str = Self::status_str(*self.status.read().await);
        let circuit = self.processor.read().await;

        let circuit_tables: serde_json::Map<String, Value> = circuit
            .table_record_counts()
            .into_iter()
            .map(|(t, c)| (t, Value::from(c)))
            .collect();

        // Per-table content hashes — bit-identical to scheduler hashes when
        // the circuit is in sync with the frozen snapshot. Used by `spky
        // verify` and the bootstrap integrity check. The `x3:` set-hash the
        // circuit already maintains per collection: O(tables) to read, where
        // the sorted digest walked every row on each status call.
        let circuit_hashes: serde_json::Map<String, Value> = circuit
            .compute_catchup_hashes()
            .into_iter()
            .map(|(t, h)| (t, Value::String(h)))
            .collect();

        // Per-table incremental XOR set-hashes (`x3:`), maintained per ingest.
        // The scheduler reconstructs these at the catch-up cut M to verify a
        // rejoining SSP before routing live traffic to it.
        let catchup_hashes: serde_json::Map<String, Value> = circuit
            .compute_catchup_hashes()
            .into_iter()
            .map(|(t, h)| (t, Value::String(h)))
            .collect();

        let ref_mode_str = match self.ref_mode {
            ssp_protocol::RefMode::Single => "single",
            ssp_protocol::RefMode::Dedicated => "dedicated",
        };

        let env_vars: serde_json::Map<String, Value> = self
            .info_env
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();

        let uptime_seconds = crate::now_epoch_ms().saturating_sub(self.start_epoch_ms) / 1000;
        let bootstrap_warnings = self.bootstrap_warnings.read().await.clone();

        json!([
            {
                "entity": "ssp",
                "id": self.ssp_id,
                "ip": self.advertise_ip,
                "status": status_str,
                "views": circuit.view_count(),
                "version": self.version,
                "surrealdb_version": self.surrealdb_version,
                "uptime_seconds": uptime_seconds,
                "last_heartbeat_seconds_ago": null,
                "circuit_tables": circuit_tables,
                "circuit_hashes": circuit_hashes,
                "catchup_hashes": catchup_hashes,
                "ref_mode": ref_mode_str,
                "env": env_vars,
                "bootstrap_warnings": bootstrap_warnings,
            }
        ])
    }

    async fn info_handler(&self) -> ApiResponse {
        ok_json(self.info_value().await)
    }

    /// `/info/text` — the same entity list serialized to a JSON string and
    /// served as `text/plain`. SurrealDB's `/spooky` custom API proxies this
    /// via `http::get` and passes the body through verbatim, so it must
    /// already be valid JSON text.
    async fn info_text_handler(&self) -> ApiResponse {
        let body = serde_json::to_string(&self.info_value().await).unwrap_or_else(|_| "[]".into());
        ApiResponse::text(200, "text/plain", body)
    }

    /// Debug: cache state for one view.
    async fn debug_view_handler(&self, view_id: &str) -> ApiResponse {
        let circuit = self.processor.read().await;
        if let Some(view) = circuit.get_view(view_id) {
            let cache_summary: Vec<_> = view
                .cache
                .iter()
                .map(|(k, &w)| json!({ "key": k, "weight": w }))
                .collect();
            ok_json(json!({
                "view_id": view_id,
                "cache_size": view.cache.len(),
                "last_hash": view.last_hash,
                "format": format!("{:?}", view.format),
                "cache": cache_summary,
                "subquery_tables": view.subquery_tables,
                "referenced_tables": view.referenced_tables,
                "content_generation": view.content_generation,
                "subquery_cache": view.subquery_cache.iter()
                    .map(|(k, (pk, alias))| json!({"key": k, "parent_key": pk, "alias": alias}))
                    .collect::<Vec<_>>(),
            }))
        } else {
            ok_json(json!({ "error": "View not found" }))
        }
    }

    /// Debug: dump one table's circuit rows as `{ raw_id: json }`. The
    /// scheduler fetches this on a persistent catch-up hash mismatch to diff
    /// its reconstructed projection against the circuit row-by-row.
    ///
    /// Bounded by [`CATCHUP_ROWS_DUMP_LIMIT`] and sorted by id, with
    /// `truncated` and `max_id` telling the caller how far the answer is
    /// authoritative. See `Circuit::dump_table_rows` for why an unordered
    /// truncation would be actively misleading here.
    async fn debug_catchup_rows_handler(&self, table: &str) -> ApiResponse {
        let (rows, truncated) = {
            let circuit = self.processor.read().await;
            circuit.dump_table_rows(table, CATCHUP_ROWS_DUMP_LIMIT)
        };
        let max_id = rows.last().map(|(id, _)| id.clone());
        let rows: serde_json::Map<String, Value> = rows.into_iter().collect();
        ok_json(json!({
            "table": table,
            "rows": rows,
            "truncated": truncated,
            "max_id": max_id,
        }))
    }

    /// Debug: last `_00_heartbeat` probe seen (e2e heartbeat observation
    /// point). Nulls until the first probe write arrives after boot.
    fn debug_heartbeat_handler(&self) -> ApiResponse {
        let seen = *self.last_heartbeat_seen.lock().unwrap();
        match seen {
            Some((hb_seq, received_at_ms)) => ok_json(json!({
                "hb_seq": hb_seq,
                "received_at_ms": received_at_ms,
            })),
            None => ok_json(json!({
                "hb_seq": Value::Null,
                "received_at_ms": Value::Null,
            })),
        }
    }

    /// Debug: dependency map + store overview.
    async fn debug_deps_handler(&self) -> ApiResponse {
        let circuit = self.processor.read().await;
        ok_json(json!({
            "dependency_map": circuit.dependency_map_dump(),
            "tables_in_store": circuit.table_names(),
            "view_count": circuit.view_count(),
        }))
    }

    /// Debug: estimated heap footprint, attributed per table and per view.
    ///
    /// Deliberately not folded into `/info`: that route is unauthenticated,
    /// and this one walks every row in the store to produce its numbers.
    ///
    /// The estimates come from `ssp::size` and are meant to be read as deltas
    /// (which component moved, and by how much) rather than as allocator-exact
    /// totals — see that module for the reasoning.
    async fn debug_memory_handler(&self) -> ApiResponse {
        let (report, graphs, subscribers, merging) = {
            let circuit = self.processor.read().await;
            (
                circuit.size_report(),
                circuit.graph_count(),
                circuit.subscriber_count(),
                circuit.merge_views(),
            )
        };
        ok_json(json!({
            "total_bytes": report.total_bytes(),
            // The merge win, measured rather than inferred: `views` counts
            // registrations, `graphs` counts operator DAGs, and the gap is what
            // sharing saved. Equal counts with `merging` true means nothing
            // merged, which is a finding, not a formality.
            "merging": merging,
            "graphs": graphs,
            "subscribers": subscribers,
            "store_bytes": report.store_bytes,
            "query_bytes": report.query_bytes,
            "tables": report.tables.iter().map(|t| json!({
                "table": t.table,
                "rows": t.rows,
                "rows_bytes": t.rows_bytes,
                "index_bytes": t.index_bytes,
                "zset_bytes": t.zset_bytes,
                "total_bytes": t.total_bytes(),
                "bytes_per_row": t.bytes_per_row(),
            })).collect::<Vec<_>>(),
            "views": report.views.iter().map(|v| json!({
                "query_id": v.query_id,
                "auth_id": v.auth_id,
                "cached_records": v.cached_records,
                "view_bytes": v.view_bytes,
                "operator_bytes": v.operator_bytes,
                "total_bytes": v.total_bytes(),
            })).collect::<Vec<_>>(),
        }))
    }

    /// 503 with the `SspError` shape when the SSP isn't `Ready`.
    async fn ready_gate(&self) -> Option<ApiResponse> {
        let status = *self.status.read().await;
        if status != SspStatus::Ready {
            return Some(ApiResponse::json(
                503,
                json!({
                    "code": crate::status::error_codes::NOT_READY,
                    "message": format!("SSP is in {:?} state", status),
                }),
            ));
        }
        None
    }

    /// `POST /crdt/apply` — merge a Loro update into `_00_crdt[<field>]`.
    async fn crdt_apply_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        if let Some(gate) = self.ready_gate().await {
            return Some(gate);
        }
        let Ok(payload) = serde_json::from_slice::<crate::crdt::ApplyRequest>(&req.body) else {
            return Some(err_json(422, "bad_body", "invalid crdt apply payload"));
        };
        match self.crdt_cache.apply(self.platform.db.as_ref(), &payload).await {
            Ok(resp) => Some(ok_json(json!({ "rev": resp.rev }))),
            Err(e) => {
                error!(error = %e, "CRDT apply failed");
                Some(err_json(400, "crdt_apply_failed", e.to_string()))
            }
        }
    }

    /// `POST /view/unregister` — remove a view + delete its `_00_list_ref` edges.
    async fn unregister_view_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        if let Some(gate) = self.ready_gate().await {
            return Some(gate);
        }
        let Ok(payload) = serde_json::from_slice::<ssp_protocol::ViewUnregisterRequest>(&req.body)
        else {
            return Some(err_json(422, "bad_body", "invalid unregister payload"));
        };
        debug!("Unregistering view: {}", payload.id);
        // Circuit keys are the bare `<hash>` (see `ssp::canonical_query_id`);
        // callers may send either spelling. `view_metrics` is keyed the same.
        let view_key = ssp::canonical_query_id(&payload.id);

        // Look up the auth_id from the View before removing it, so the edge
        // cleanup targets the right per-user `_00_list_ref_user_<id>`.
        let auth_id = {
            let circuit = self.processor.read().await;
            circuit.get_view(&view_key).map(|v| v.auth_id.clone()).unwrap_or_default()
        };
        {
            let mut circuit = self.processor.write().await;
            // Release rather than destroy: this graph may be shared with other
            // registrations computing the same thing, and tearing it down would
            // blank their lists. `detach_subscriber` removes it only when this
            // was the last holder. With merging disabled there is never more
            // than one holder, so this is exactly `remove_query`.
            circuit.detach_subscriber(&view_key);
        }
        self.view_metrics.write().await.remove(&view_key);
        self.platform.telemetry.gauge_add("view_count", -1);

        // Delete all edges for this incantation via the Db port.
        let incantation_id = crate::edges::format_incantation_id(&payload.id);
        let list_ref = crate::tables::list_ref_table(self.ref_mode, &auth_id);
        // Bind the incantation as a record (single-arg type::record parses the
        // full `_00_query:<key>` faithfully).
        let stmt = format!("DELETE (type::record($from))->{list_ref}");
        if let Err(e) = self
            .platform
            .db
            .query(&stmt, &[("from", json!(incantation_id))])
            .await
        {
            error!("Failed to delete edges for view {}: {}", incantation_id, e);
        }

        Some(ok_json(Value::Null))
    }

    /// `POST /view/register` — create/refresh a view and seed its edges.
    /// Drop a view that was registered with no identity, so the caller's own
    /// registration can rebuild it.
    ///
    /// Returns `true` when the view was discarded and the cold path should
    /// run. `false` leaves the warm path to handle the registration as usual:
    /// the view already has an identity, or another registration shares its
    /// operator graph and tearing it down would take that one with it.
    ///
    /// Relabelling is NOT an option here. `prepare_registration_dbsp` bakes
    /// `params.auth.id` into the plan's permission injection, so a view built
    /// with `''` resolved every `$auth.id` predicate against the empty string.
    /// It does not just have its edges in the wrong table, it holds the wrong
    /// rows: `permission = true` tables come out correct while everything
    /// gated on the caller comes out empty. Handing that to a real user
    /// exchanges a blank page for a page that looks populated and is quietly
    /// missing their data, which is strictly harder to notice.
    ///
    /// The stranded edges go too. They carry `auth_id = ''`, so nobody can
    /// read them (`_00_list_ref` is gated `auth_id = $auth.id`) and nothing
    /// would ever clean them up once the row is rebuilt under a real identity.
    async fn discard_view_without_identity(&self, view_id: &str) -> bool {
        let stale_table = {
            let mut circuit = self.processor.write().await;
            match circuit.get_view(view_id) {
                Some(v) if v.auth_id.is_empty() => {}
                _ => return false,
            }
            // Merging is off by default, but when it is on another
            // registration may be reading this graph. Leave it alone rather
            // than tear it out from under them; that view is readable by
            // whoever owns it and this caller can wait for its own cold build
            // on the next distinct id.
            if !circuit.subscribers_of(view_id).is_empty() {
                return false;
            }
            let table = crate::tables::list_ref_table(self.ref_mode, "");
            circuit.remove_query(view_id);
            table
        };
        self.platform.telemetry.gauge_add("view_count", -1);

        info!(
            target: "ssp::edges",
            view_id = %view_id,
            "View was registered without an identity - rebuilding it for the caller who has one"
        );

        let cleanup = format!(
            "LET $from = type::record('_00_query', $qid); \
             DELETE {stale_table} WHERE in = $from;"
        );
        if let Err(e) = self
            .platform
            .db
            .query(&cleanup, &[("qid", json!(view_id))])
            .await
        {
            warn!(
                target: "ssp::edges",
                view_id = %view_id,
                table = %stale_table,
                error = %e,
                "Failed to clear edges stranded under the empty identity"
            );
        }
        true
    }

    /// Republish a warm-path view whose `_00_list_ref` rows have gone missing.
    ///
    /// The warm path is deliberately metadata-only, on the stated assumption
    /// that a joiner reads current membership itself out of `_00_list_ref*`.
    /// That holds only while the DB side still matches the circuit, and it
    /// stops matching when a TTL sweep releases a row and its edges while
    /// another holder keeps the graph alive. The next registration then finds
    /// the view in the circuit, takes this path, and its metadata `UPDATE`
    /// recreates `_00_query` with `rowCount` back at its schema `DEFAULT 0`
    /// and nothing behind it. A client cannot tell that from a genuinely empty
    /// result: it stops loading and renders nothing, permanently.
    ///
    /// Verify-then-heal rather than republish-always, because
    /// `edges::build_edge_batch` writes additions with a bare `RELATE`, so
    /// republishing over live edges would duplicate every row. One `SELECT`
    /// per warm registration buys the difference between a transient gap and a
    /// permanent one.
    ///
    /// `view_id` is already canonical (`<hash>`), `incantation_id` is the
    /// `_00_query:<hash>` record id.
    async fn repair_stranded_view(
        &self,
        view_id: &str,
        incantation_id: &str,
        meta: &Value,
        auth_id: &str,
    ) {
        // Read the row count and the STORED auth id: the latter is what
        // `build_edge_batch` routes on, so the probe has to look in the same
        // table a republish would write to.
        let (expected_rows, view_auth) = {
            let circuit = self.processor.read().await;
            match circuit.get_view(view_id) {
                Some(v) => (v.cache.len() as i64, v.auth_id.clone()),
                None => return,
            }
        };
        let list_ref = crate::tables::list_ref_table(self.ref_mode, &view_auth);
        let probe = format!(
            "LET $from = type::record('_00_query', $qid); \
             SELECT VALUE count() FROM {list_ref} WHERE in = $from GROUP ALL; \
             SELECT VALUE count() FROM _00_query WHERE id = $from GROUP ALL;"
        );
        let count_at = |results: &Vec<Value>, idx: usize| -> i64 {
            results
                .get(idx)
                .and_then(|v| v.as_array())
                .and_then(|rows| rows.first())
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
        };
        let (edge_count, row_present) = match self
            .platform
            .db
            .query(&probe, &[("qid", json!(view_id))])
            .await
        {
            Ok(results) => (count_at(&results, 1), count_at(&results, 2) > 0),
            Err(e) => {
                // Unknown state. Do NOT republish blind: duplicate edges are a
                // worse failure than the stall this is guarding against.
                warn!(
                    target: "ssp::edges",
                    view_id = %view_id,
                    error = %e,
                    "Stranded-view probe failed; leaving the view untouched"
                );
                return;
            }
        };
        // Healthy: the row is there, and either it has edges or the view is
        // genuinely empty (`rowCount = 0` is then the truth, not a default
        // standing in for one).
        if row_present && (expected_rows == 0 || edge_count > 0) {
            return;
        }

        warn!(
            target: "ssp::edges",
            view_id = %view_id,
            expected_rows,
            edge_count,
            row_present,
            list_ref = %list_ref,
            "Re-registered view is not backed by the DB - republishing from the live view"
        );

        // UPSERT, not UPDATE. The warm path's own metadata write is an
        // `UPDATE`, which on a record that no longer exists matches nothing and
        // silently writes NOTHING — so a sweep that took the row while the
        // circuit kept the view left the client with no `_00_query` row at all,
        // and a re-registration could not put it back. Rebuild the whole row,
        // with the same fields the cold path writes (minus `registrationTime`,
        // which belongs to a real registration).
        //
        // `rowCount` rides along in the same statement: it is how the client
        // tells "no results" from "edges still flushing", so it must be true
        // before the edges land, never after.
        let meta_str = |key: &str| meta.get(key).and_then(|v| v.as_str()).unwrap_or_default();
        let stmt = "UPSERT type::record($id) SET clientId = <string>$clientId, \
                    auth_id = <string>$authId, surql = <string>$surql, params = $params, \
                    ttl = <duration>$ttl, lastActiveAt = <datetime>$lastActiveAt, \
                    rowCount = <int>$rowCount, state = <string>$state, \
                    subscribers = array::append( \
                        array::filter(subscribers ?? [], |$s| \
                            <string>$s.id != $sid \
                            AND <datetime>$s.seenAt + <duration>$ttl > time::now()), \
                        { id: $sid, seenAt: time::now() })";
        if let Err(e) = self
            .platform
            .db
            .query(
                stmt,
                &[
                    ("id", json!(incantation_id)),
                    ("clientId", json!(meta_str("clientId"))),
                    ("sid", json!(meta_str("clientId"))),
                    ("authId", json!(if view_auth.is_empty() { auth_id } else { &view_auth })),
                    ("surql", json!(meta_str("sql"))),
                    ("params", meta.get("safe_params").cloned().unwrap_or(Value::Null)),
                    ("ttl", json!(meta_str("ttl"))),
                    ("lastActiveAt", json!(meta_str("lastActiveAt"))),
                    ("rowCount", json!(expected_rows)),
                    // The republished snapshot below is a full publish, so the
                    // flusher flips this to `ready` once the edges are in.
                    // Nothing to republish for an empty view: ready now.
                    ("state", json!(if expected_rows == 0 { "ready" } else { "materializing" })),
                ],
            )
            .await
        {
            error!(
                view_id = %view_id,
                error = %e,
                "Failed to rebuild the _00_query row for a stranded view"
            );
            return;
        }

        if expected_rows == 0 {
            return;
        }

        // Snapshot the live view rather than rebuilding it: `snapshot_delta`
        // clones the cache and leaves the graph and dependency map alone,
        // which is exactly what the early return above exists to protect.
        let delta = {
            let circuit = self.processor.read().await;
            circuit.snapshot_delta(view_id, view_auth)
        };
        let Some(delta) = delta else { return };

        if let Err(send_err) = self.edge_update_tx.send(vec![delta]) {
            let circuit = self.processor.read().await;
            let left = crate::edges::write_deltas_resilient(
                self.platform.db.as_ref(),
                send_err.0,
                &circuit,
                self.ref_mode,
                self.platform.telemetry.as_ref(),
            )
            .await;
            if !left.is_empty() {
                error!(
                    views = left.len(),
                    "stranded-view republish not written after retries (direct path)"
                );
                self.platform
                    .telemetry
                    .counter("edge_deltas_dropped", left.len() as u64);
            }
        }
        self.platform.telemetry.counter("stranded_view_repaired", 1);
    }

    async fn register_view_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        use ssp::circuit::view::OutputFormat;

        if let Some(gate) = self.ready_gate().await {
            return Some(gate);
        }
        let Ok(payload) = serde_json::from_slice::<Value>(&req.body) else {
            return Some(err_json(422, "bad_body", "invalid register payload"));
        };

        // Parse + validate under the read lock (permission injection). Failures
        // are 400 with the offending table named.
        let data = {
            let circuit = self.processor.read().await;
            match ssp::service::view::prepare_registration_dbsp(
                payload,
                circuit.permissions(),
                circuit.link_targets(),
                circuit.opaque_fields(),
            ) {
                Ok(d) => d,
                Err(e) => {
                    error!(target: "ssp::policy", error = %e, "Rejected view registration");
                    return Some(err_json(400, "rejected", e.to_string()));
                }
            }
        };
        // A live client's id arrives as `_00_query:<hash>` (SurrealDB
        // stringifies a record id with its table), while boot re-registration
        // and the TTL sweep speak the bare `<hash>`. Canonicalise once here so
        // the circuit lookup, the merge index and `view_metrics` all agree —
        // the circuit enforces this too, but only the value we carry downstream
        // makes the comparisons below correct. See `ssp::canonical_query_id`.
        let mut data = data;
        data.plan.id = ssp::canonical_query_id(&data.plan.id);

        // Auth identity for per-user routing (anon remap when enabled).
        let auth_id = {
            let raw = data.metadata.get("authId").and_then(|v| v.as_str()).unwrap_or("");
            if raw.is_empty() && self.anonymous_live_queries {
                ssp_protocol::ANON_AUTH_ID.to_string()
            } else {
                raw.to_string()
            }
        };

        // Lazy-define per-user tables (idempotent; no-op in Single mode).
        if let Err(e) =
            crate::tables::ensure_user_tables(self.platform.db.as_ref(), self.ref_mode, &auth_id)
                .await
        {
            // `?` (Debug) prints the whole anyhow chain; `%` showed only the
            // outer context and hid the database's actual reason.
            error!(error = ?e, auth_id = %auth_id, "Failed to ensure per-user tables");
            return Some(err_json(500, "db_error", "Database error"));
        }

        let raw_id = data.metadata.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");
        let incantation_id = crate::edges::format_incantation_id(raw_id);

        let view_existed = {
            let circuit = self.processor.read().await;
            circuit.get_view(&data.plan.id).is_some()
        };

        // A view registered before its session had an identity is not merely
        // mislabelled. `prepare_registration_dbsp` injects permissions using
        // `params.auth.id`, so its plan resolved every `$auth.id` predicate
        // against '' and it computes the WRONG ROWS: public tables come out
        // right, everything gated on the caller comes out empty. Relabelling
        // such a view would make it readable and silently under-populated,
        // which is harder to notice than the blank page it renders today.
        // Discard it instead and let the cold path below rebuild it with this
        // registration's own, correctly injected plan.
        let identity_upgrade = view_existed
            && !auth_id.is_empty()
            && self.discard_view_without_identity(&data.plan.id).await;

        let meta_str = |k: &str| data.metadata.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();

        if view_existed && !identity_upgrade {
            // A second (or tenth) session subscribing to a view that already
            // exists. This is the SHARED path and it is deliberately
            // metadata-only: calling `add_query_with_auth` again would
            // `views.insert` over the live view, discarding its cache and
            // subquery state, and push the query id onto the dependency map a
            // second time so every ingest stepped it twice.
            //
            // The joiner does not need a re-publish. It reads current
            // membership itself with a SELECT over `_00_list_ref*`, and the
            // `rowCount` written below on the cold path is what lets it tell
            // "no results" from "edges still flushing".
            let existing_auth = {
                let circuit = self.processor.read().await;
                circuit.get_view(&data.plan.id).map(|v| v.auth_id.clone())
            };

            // Query ids are derived from (surql, params, auth), so a caller
            // arriving at someone else's view means the id was guessed or
            // forged. Refuse rather than adopt it: the view's plan was
            // permission-injected for the OTHER user's identity, so serving it
            // here would hand them that user's rows.
            //
            // An EMPTY `auth_id` is not such a caller and must not be refused.
            // It means the registration asserted no identity at all, which
            // happens routinely: `fn::query::register` sends
            // `<string>($auth.id OR '')`, so any re-registration issued while
            // the session's auth is not (yet) established carries ''. Observed
            // in production after a SurrealDB restart — every client re-registered
            // with '' mid-reconnect, every one was refused 409, and their views
            // never came back, which rendered as "not found" on a page that had
            // been working. Treat '' as "no assertion" and let it join; the
            // stored `auth_id` is write-once and the per-user
            // `_00_list_ref_user_<uid>` table still gates what can actually be
            // read back.
            //
            // The converse — an identity-asserting caller meeting a view that
            // holds none — never reaches here: `identity_upgrade` above has
            // already discarded that view so the cold path can rebuild it
            // under the caller's own permission injection.
            if let Some(existing) = existing_auth {
                if !auth_id.is_empty() && !existing.is_empty() && existing != auth_id {
                    warn!(
                        target: "ssp::edges",
                        view_id = %incantation_id,
                        existing = %existing,
                        attempted = %auth_id,
                        "Refusing to share a view across identities"
                    );
                    return Some(err_json(
                        409,
                        "auth_mismatch",
                        "This query id belongs to a different identity",
                    ));
                }
            }

            info!(target: "ssp::edges", view_id = %incantation_id, "View already existed - joining as an additional subscriber");
            // Record this session as a watcher and refresh liveness.
            //
            // `auth_id` is deliberately NOT written here: it is write-once,
            // set by whoever created the row. The in-memory `View.auth_id`
            // has no setter, so letting a later registrant change the stored
            // value would desynchronize the two — edges would be stamped with
            // one identity while routed to the other's table.
            //
            // `ttl` is max-wins so a subscriber asking for a shorter TTL
            // cannot shorten a view another tab is depending on. The bare `ttl`
            // reads below are correct: the field is `TYPE duration`, so
            // `<datetime> + ttl` is valid arithmetic. (`$ttl` the PARAMETER is
            // a string off the register payload, hence the explicit
            // `<duration>` cast on that side only.)
            let stmt = "UPDATE type::record($id) SET clientId = <string>$clientId, \
                        lastActiveAt = <datetime>$lastActiveAt, \
                        ttl = (IF <duration>$ttl > ttl { <duration>$ttl } ELSE { ttl }), \
                        subscribers = array::append( \
                            array::filter(subscribers ?? [], |$s| \
                                <string>$s.id != $sid \
                                AND <datetime>$s.seenAt + ttl > time::now()), \
                            { id: $sid, seenAt: time::now() })";
            if let Err(e) = self
                .platform
                .db
                .query(
                    stmt,
                    &[
                        ("id", json!(incantation_id)),
                        ("clientId", json!(meta_str("clientId"))),
                        ("sid", json!(meta_str("clientId"))),
                        ("ttl", json!(meta_str("ttl"))),
                        ("lastActiveAt", json!(meta_str("lastActiveAt"))),
                    ],
                )
                .await
            {
                error!("Failed to update incantation metadata: {}", e);
            }

            // The metadata write above is all the warm path used to do. That
            // assumes the DB still holds the edges this view published when it
            // was cold — see `repair_stranded_view` for when it does not.
            self.repair_stranded_view(&data.plan.id, &incantation_id, &data.metadata, &auth_id)
                .await;

            return Some(ok_json(Value::Null));
        }

        debug!("Registering view: {}", data.plan.id);

        let register_start = web_time::Instant::now();
        // Does another registration already compute exactly this? If so, attach
        // to its graph instead of building an identical one. `merge_key` is the
        // injected plan plus the params that plan dereferences, so an attach
        // provably yields the same rows (see `ssp::merge_key`).
        //
        // Everything below this point is shared with the cold path on purpose:
        // a joiner needs the same `_00_query` row write (notably `rowCount`,
        // which is how the client tells "no results" from "edges still
        // flushing") and the same initial edge publish.
        // Read the policy off the circuit, not off `self`: boot re-registration
        // reads it there too, and one source is what keeps the two paths from
        // disagreeing about whether a graph is shareable.
        let merge_owner = {
            let circuit = self.processor.read().await;
            if circuit.merge_views() {
                circuit
                    .owner_for_merge_key(&data.merge_key)
                    .filter(|owner| *owner != data.plan.id)
                    .map(|owner| owner.to_string())
            } else {
                None
            }
        };

        let update = {
            let mut circuit = self.processor.write().await;
            match &merge_owner {
                Some(owner) => {
                    info!(
                        target: "ssp::edges",
                        view_id = %incantation_id,
                        owner = %owner,
                        "Sharing an existing operator graph for an identical computation"
                    );
                    circuit.attach_subscriber(owner, data.plan.id.clone(), auth_id.clone())
                }
                None => {
                    let delta = circuit.add_query_with_auth(
                        data.plan.clone(),
                        data.safe_params,
                        Some(OutputFormat::Streaming),
                        auth_id.clone(),
                    );
                    // Claim the key only once the graph exists, or a later
                    // registration would attach to nothing.
                    if circuit.merge_views() {
                        circuit.claim_merge_key(data.merge_key.clone(), data.plan.id.clone());
                    }
                    delta
                }
            }
        };
        let registration_time_ms = register_start.elapsed().as_secs_f64() * 1000.0;
        self.platform.telemetry.gauge_add("view_count", 1);

        // Seed an empty per-view metrics slot.
        self.view_metrics
            .write()
            .await
            .entry(data.plan.id.clone())
            .or_default();

        let params = data.metadata.get("safe_params").cloned().unwrap_or(Value::Null);
        let initial_row_count = update.as_ref().map(|d| d.records.len() as i64).unwrap_or(0);
        // Publish state for the client. A delta with anything to write goes to
        // the flusher, which flips the row to `ready` in the same transaction
        // as the edges; a delta with nothing to write is skipped by
        // `build_edge_batch`, so the row has to be born `ready` here or it
        // would say `materializing` forever.
        let initial_state = crate::edges::publish_state_for(update.as_ref());

        // createdAt is DEFAULT time::now() READONLY (set only on insert);
        // counters default to 0 if absent.
        // The creating session is also the first subscriber. `$ttl` is used
        // rather than the `ttl` field because both are being set in this same
        // statement and the field's value is not yet observable.
        let stmt = "UPSERT type::record($id) SET clientId = <string>$clientId, \
                    auth_id = <string>$authId, surql = <string>$surql, params = $params, \
                    ttl = <duration>$ttl, lastActiveAt = <datetime>$lastActiveAt, \
                    registrationTime = <float>$registrationTime, rowCount = <int>$rowCount, \
                    state = <string>$state, \
                    subscribers = array::append( \
                        array::filter(subscribers ?? [], |$s| \
                            <string>$s.id != $sid \
                            AND <datetime>$s.seenAt + <duration>$ttl > time::now()), \
                        { id: $sid, seenAt: time::now() })";
        if let Err(e) = self
            .platform
            .db
            .query(
                stmt,
                &[
                    ("id", json!(incantation_id)),
                    ("clientId", json!(meta_str("clientId"))),
                    ("sid", json!(meta_str("clientId"))),
                    ("authId", json!(auth_id)),
                    ("surql", json!(meta_str("sql"))),
                    ("params", params),
                    ("ttl", json!(meta_str("ttl"))),
                    ("lastActiveAt", json!(meta_str("lastActiveAt"))),
                    ("registrationTime", json!(registration_time_ms)),
                    ("rowCount", json!(initial_row_count)),
                    ("state", json!(initial_state)),
                ],
            )
            .await
        {
            error!("Failed to upsert incantation metadata: {}", e);
            return Some(err_json(500, "db_error", "Database error"));
        }

        // Initial edges → coalescing flusher; direct write if the flusher is gone.
        if let Some(delta) = update {
            if let Err(send_err) = self.edge_update_tx.send(vec![delta]) {
                let circuit = self.processor.read().await;
                let left = crate::edges::write_deltas_resilient(
                    self.platform.db.as_ref(),
                    send_err.0,
                    &circuit,
                    self.ref_mode,
                    self.platform.telemetry.as_ref(),
                )
                .await;
                if !left.is_empty() {
                    error!(views = left.len(), "registration edges not written after retries (direct path)");
                    self.platform.telemetry.counter("edge_deltas_dropped", left.len() as u64);
                }
            }
        }

        Some(ok_json(Value::Null))
    }

    /// `POST /ingest` — the change-push entry. SurrealDB `DEFINE EVENT`s post
    /// each mutation here: route jobs, step the circuit, fan out edge writes.
    async fn ingest_handler(&self, req: &ApiRequest) -> Option<ApiResponse> {
        use ssp::circuit::{Change, ChangeSet, Operation};

        if let Some(gate) = self.ready_gate().await {
            return Some(gate);
        }
        let Ok(payload) = serde_json::from_slice::<ssp_protocol::IngestRequest>(&req.body) else {
            error!("Invalid ingest JSON payload");
            return Some(ApiResponse::json(400, Value::Null));
        };
        let Some(op) = Operation::from_str(&payload.op) else {
            warn!(op = %payload.op, "Invalid operation type");
            return Some(ApiResponse::json(400, Value::Null));
        };

        let start = crate::now_epoch_ms();
        // Borrowed: `payload.record` is read again further down (job routing,
        // heartbeat seq, owner, job timing), so it has to survive. Cloning it
        // first meant building the tree twice, on every ingested record.
        let clean = ssp::sanitizer::normalize_record_ref(&payload.record);
        let db = self.platform.db.as_ref();

        // Pre-emptively create / drop the user's dedicated tables so the
        // client's post-auth LIVE doesn't race lazy creation.
        if payload.table == "user" && op == Operation::Create {
            if let Err(e) =
                crate::tables::ensure_user_tables(db, self.ref_mode, &payload.id).await
            {
                warn!(target: "ssp::ingest", error = ?e, auth_id = %payload.id, "Pre-emptive ensure_user_tables failed");
            }
        }
        if payload.table == "user" && op == Operation::Delete {
            if let Err(e) = crate::tables::drop_user_tables(db, self.ref_mode, &payload.id).await {
                warn!(target: "ssp::ingest", error = ?e, auth_id = %payload.id, "drop_user_tables failed");
            }
        }

        // Job routing.
        if let Some(backend_info) = self.job_config.job_tables.get(&payload.table).cloned() {
            let is_assigned =
                self.standalone || payload.job_assignee.as_deref() == Some(self.ssp_id.as_str());

            if is_assigned && op == Operation::Create {
                if payload.record.get("status").and_then(|v| v.as_str()) == Some("pending") {
                    self.route_pending_job(&payload, &backend_info).await;
                }
            } else if op == Operation::Update {
                // The runner's terminal status write fires this table's mutation
                // event, which is how the schedule engine learns a scheduled job
                // or workflow step finished. Not gated on `is_assigned`: the
                // engine only ever reads the job row, and in cluster mode the
                // node that RAN the job is not the node that ticks.
                self.observe_job_terminal(&payload.id, &payload.record).await;
            }
        }

        // Step the circuit.
        let change = match op {
            Operation::Create => Change::create(&payload.table, &payload.id, clean),
            Operation::Update => Change::update(&payload.table, &payload.id, clean),
            Operation::Merge => Change::merge(&payload.table, &payload.id, clean),
            Operation::Delete => Change::delete(&payload.table, &payload.id),
        };
        let step_start = web_time::Instant::now();
        let (deltas, rv_made_up, rv_made_up_total) = {
            let mut circuit = self.processor.write().await;
            let before = circuit.synthesized_row_versions();
            let deltas = circuit.step(ChangeSet { changes: vec![change] });
            let total = circuit.synthesized_row_versions();
            (deltas, total - before, total)
        };
        let materialization_time_ms = step_start.elapsed().as_secs_f64() * 1000.0;
        self.platform.telemetry.counter("ingest", 1);
        if rv_made_up > 0 {
            // Each of these is a row the DB-side version stamp failed on
            // (`_00_version` lookup came back NONE or did not advance). Say so
            // once, then every thousandth time, so a broken `_00_version`
            // index shows in the log without flooding it.
            self.platform.telemetry.counter("ingest_rv_synthesized", rv_made_up);
            if rv_made_up_total == rv_made_up || rv_made_up_total % 1000 < rv_made_up {
                warn!(
                    target: "ssp::ingest",
                    table = %payload.table,
                    id = %payload.id,
                    total = rv_made_up_total,
                    "ingest carried no advancing _00_rv; version synthesized (is the _00_version index intact?)"
                );
            }
        }

        // E2e heartbeat observation point: recorded AFTER the circuit step so
        // a reported seq means the full pipeline (DB event → scheduler ingest
        // → broadcast → circuit) processed the probe, not just received it.
        if payload.table == "_00_heartbeat" {
            if let Some(seq) = payload.record.get("hb_seq").and_then(|v| v.as_u64()) {
                *self.last_heartbeat_seen.lock().unwrap() =
                    Some((seq, crate::now_epoch_ms()));
            }
        }

        if !deltas.is_empty() {
            // Fan out edge writes off the request path. Waiting on `_00_version`
            // ensures the list_ref UPDATE lands AFTER the source row is readable
            // downstream (see docs/surrealdb-bugs/ws-row-cache-stale-after-update.md).
            let expected_version = payload
                .record
                .get("_00_rv")
                .and_then(|v| v.as_i64())
                .filter(|&v| v > 0);
            let row_id = payload.id.clone();
            let record_counts: Vec<usize> = deltas.iter().map(|d| d.records.len()).collect();
            let view_ids: Vec<String> = deltas.iter().map(|d| d.query_id.clone()).collect();

            let db_c = Arc::clone(&self.platform.db);
            let scheduler_c = Arc::clone(&self.platform.scheduler);
            let telemetry_c = Arc::clone(&self.platform.telemetry);
            let edge_tx = self.edge_update_tx.clone();
            let processor_c = Arc::clone(&self.processor);
            let view_metrics_c = Arc::clone(&self.view_metrics);
            let ref_mode = self.ref_mode;

            self.platform.spawner.spawn(Box::pin(async move {
                if let Some(expected) = expected_version {
                    wait_for_row_committed(
                        db_c.as_ref(),
                        scheduler_c.as_ref(),
                        &row_id,
                        expected,
                        std::time::Duration::from_secs(5),
                    )
                    .await;
                }
                if let Err(send_err) = edge_tx.send(deltas) {
                    let deltas = send_err.0;
                    let circuit = processor_c.read().await;
                    let left = crate::edges::write_deltas_resilient(db_c.as_ref(), deltas, &circuit, ref_mode, telemetry_c.as_ref()).await;
                    if !left.is_empty() {
                        error!(views = left.len(), "edge deltas not written after retries (direct path)");
                        telemetry_c.counter("edge_deltas_dropped", left.len() as u64);
                    }
                }
                // In memory only; a timer flushes the dirty views to `_00_query`
                // (see flush_view_metrics). Writing per ingest contended with the
                // edge transaction on the same rows and lost edge writes.
                note_view_metrics(&view_metrics_c, record_counts, view_ids, materialization_time_ms).await;
            }));
        }

        // Orphan-proof delete: drop every edge pointing at the deleted record,
        // independently of the circuit deltas (the circuit cache can be
        // incomplete after a missed ingest / restart).
        if op == Operation::Delete {
            if let Some(owner) = payload.record.get("owner").and_then(|v| v.as_str()) {
                if valid_record_id(&payload.id) {
                    let list_ref = crate::tables::list_ref_table(self.ref_mode, owner);
                    // out is a validated record-id literal.
                    let stmt = format!("DELETE {list_ref} WHERE out = {}", payload.id);
                    let db_c = Arc::clone(&self.platform.db);
                    let id_log = payload.id.clone();
                    self.platform.spawner.spawn(Box::pin(async move {
                        if let Err(e) = db_c.query(&stmt, &[]).await {
                            let msg = e.to_string();
                            if crate::tables::is_missing_table_error(&msg) {
                                // The owner never registered a view, so they
                                // have no per-user table and no edges to drop.
                                debug!(target: "ssp::ingest", id = %id_log, "list_ref cleanup skipped: the owner has no per-user table");
                            } else {
                                error!(target: "ssp::ingest", id = %id_log, error = %msg, "list_ref delete cleanup failed");
                            }
                        }
                    }));
                }
            }
            if self.anonymous_live_queries && valid_record_id(&payload.id) {
                let stmt = format!("DELETE _00_list_ref_anon WHERE out = {}", payload.id);
                let db_c = Arc::clone(&self.platform.db);
                self.platform.spawner.spawn(Box::pin(async move {
                    let _ = db_c.query(&stmt, &[]).await;
                }));
            }
        }

        self.platform
            .telemetry
            .histogram_ms("ingest_duration", crate::now_epoch_ms().saturating_sub(start) as f64);
        Some(ApiResponse::json(200, Value::Null))
    }

    /// Queue a pending job created via ingest (with optional delay window).
    async fn route_pending_job(
        &self,
        payload: &ssp_protocol::IngestRequest,
        backend_info: &crate::jobs::BackendInfo,
    ) {
        let timeout_override = payload.record.get("timeout").and_then(|v| v.as_u64()).map(|v| v as u32);
        let job_entry = crate::jobs::JobEntry::from_record(
            payload.id.clone(),
            backend_info.base_url.clone(),
            backend_info.auth_token.clone(),
            &payload.record,
            backend_info.effective_timeout(timeout_override),
        );
        let delay_ms = payload.record.get("delay").and_then(|v| v.as_u64()).unwrap_or(0);
        let job_id = job_entry.id.clone();
        let table = job_entry.table.clone();

        if delay_ms == 0 {
            self.admit_or_backlog(job_entry).await;
        } else {
            // Delayed: sleep on a port timer, THEN ask for a slot. Admitting up
            // front would let a job with `delay: 1h` hold the table's only
            // execution slot for an hour without running anything.
            //
            // The enqueued mark is taken at the same moment as the slot (inside
            // admission), so a duplicate CREATE during the sleep can produce a
            // second sleeper; the mark still lets exactly one of them through.
            let dispatcher = Arc::clone(&self.job_dispatcher);
            let scheduler = Arc::clone(&self.platform.scheduler);
            let delay = std::time::Duration::from_millis(delay_ms);
            let standalone = self.standalone;
            let ssp_id = self.ssp_id.clone();
            let db = Arc::clone(&self.platform.db);
            self.platform.spawner.spawn(Box::pin(async move {
                scheduler.sleep(delay).await;
                if dispatcher.try_admit(job_entry).await {
                    if !standalone {
                        if let Err(e) = crate::jobs::set_assignee_helper(db.as_ref(), &job_id, &ssp_id).await {
                            warn!(job_id = %job_id, error = %e, "Failed to persist job assignee");
                        }
                    }
                } else {
                    dispatcher.note_backlog(&table);
                    debug!(job_id = %job_id, "Delayed job deferred to the backlog");
                }
            }));
        }
    }

    /// Admit a job, or leave its row `pending` for the drain to pick up.
    ///
    /// The assignee stamp only happens on success. Stamping a job this node
    /// then refused would tell the cluster sweep "a live owner has this",
    /// so the row would wait out the full orphan window before anyone looked
    /// at it again.
    async fn admit_or_backlog(&self, entry: JobEntry) {
        let job_id = entry.id.clone();
        let table = entry.table.clone();
        if !self.job_dispatcher.try_admit(entry).await {
            self.job_dispatcher.note_backlog(&table);
            debug!(job_id = %job_id, "Job deferred to the backlog");
            return;
        }
        if !self.standalone {
            if let Err(e) =
                crate::jobs::set_assignee_helper(self.platform.db.as_ref(), &job_id, &self.ssp_id).await
            {
                warn!(job_id = %job_id, error = %e, "Failed to persist job assignee");
            }
        }
    }

    /// A job row was updated — tell the schedule engine if it just terminalized.
    ///
    /// This is the fast path for advancing scheduled runs and workflow DAGs. It
    /// is deliberately best-effort: the event is an `http::post` from a DB event,
    /// so a restart can drop it, and the engine's heal pass reaches the same
    /// conclusion within one sweep. Runs on the `Spawner` so a slow DAG
    /// advancement never holds up the ingest response.
    async fn observe_job_terminal(&self, id: &str, record: &Value) {
        let Some(engine) = self.schedule_engine.clone() else { return };
        let status = record.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if !matches!(status, "success" | "failed") {
            return;
        }
        let job_id = id.to_string();
        let status = status.to_string();
        self.platform.spawner.spawn(Box::pin(async move {
            if let Err(e) = engine.observe_job_terminal(&job_id, &status).await {
                warn!(job_id = %job_id, error = %e, "schedule engine could not observe job completion");
            }
        }));
    }

    // --- Timer-driven sweeps (invoked from Runtime::on_timer) ----------------

    /// Recover stuck `pending` + orphaned `processing` job rows across every
    /// configured job table (standalone recovery). Over the `Db` port.
    pub async fn job_recovery_sweep(&self) {
        for (table, backend) in &self.job_config.job_tables {
            if let Err(e) = self.recover_job_table(table, backend).await {
                warn!(target: "ssp::job_recovery", table = %table, error = %e, "Recovery sweep failed for table");
            }
        }
    }

    async fn recover_job_table(
        &self,
        table: &str,
        backend: &crate::jobs::BackendInfo,
    ) -> Result<(), crate::ports::DbError> {
        let db = self.platform.db.as_ref();

        // 1. Due pending rows older than the grace window.
        //
        // Bounded and ordered, unlike before: with a concurrency limit a table
        // can legitimately hold a very large pending backlog, and this used to
        // SELECT all of it into memory once a minute. Oldest first, so what the
        // sweep does pick up matches the order the drain would have used.
        let pending_q = format!(
            "SELECT {fields} FROM {table} \
             WHERE status = 'pending' AND updated_at < time::now() - {grace}s AND {due} \
             ORDER BY created_at ASC LIMIT {limit}",
            fields = RECOVERY_FIELDS,
            grace = JOB_RECOVERY_PENDING_GRACE_SECS,
            due = crate::jobs::PENDING_DUE_CLAUSE,
            limit = JOB_RECOVERY_PAGE,
        );
        let mut deferred = false;
        for row in rows_of(db.query(&pending_q, &[]).await?) {
            if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                if crate::jobs::enqueue_recovered(&self.job_dispatcher, backend, id, &row).await {
                    warn!(target: "ssp::job_recovery", job_id = %id, "Re-enqueued stuck pending job");
                } else {
                    deferred = true;
                }
            }
        }

        // 2. `processing` rows whose LEASE has expired.
        //
        // The lease, not the owner. This used to skip any row still marked in-flight
        // on this node, which meant a request hung inside the HTTP call — the exact
        // thing that needs recovering — was skipped on every pass forever. An
        // in-flight mark now says "cancel it first", not "leave it alone".
        let stale_q = format!(
            "SELECT {fields} FROM {table} WHERE status = 'processing' \
             AND {expired} \
             ORDER BY updated_at ASC LIMIT {limit}",
            fields = RECOVERY_FIELDS,
            expired = schedule_core::sql::LEASE_EXPIRED,
            limit = JOB_RECOVERY_PAGE,
        );
        for row in rows_of(db.query(&stale_q, &[]).await?) {
            let Some(id) = row.get("id").and_then(|v| v.as_str()) else { continue };
            // Still running here: the lease says it has had long enough, so stop it
            // rather than leaving a zombie holding a backend connection while its
            // replacement runs. Same mechanism `/job/kill` uses.
            if self.job_control.cancel_inflight(id) {
                warn!(target: "ssp::job_recovery", job_id = %id, "Cancelling a local request past its lease");
            }
            // Guarded on `processing` AND on the expiry, so a row that finished (or
            // renewed) between the SELECT and here is left alone — the previous
            // unguarded reset could flip a `success` row back to `pending`. It also
            // clears the assignee and bumps the fence; see `reclaim_expired_lease`.
            match crate::jobs::reclaim_expired_lease(db, id).await {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    warn!(target: "ssp::job_recovery", job_id = %id, error = %e, "Failed to reclaim a job past its lease");
                    continue;
                }
            }
            if crate::jobs::enqueue_recovered(&self.job_dispatcher, backend, id, &row).await {
                warn!(target: "ssp::job_recovery", job_id = %id, "Recovered a job past its lease");
            } else {
                deferred = true;
            }
        }

        // Anything the limit turned away is a known backlog: flag it so the
        // drain timer keeps working through it rather than waiting for the next
        // sweep a minute from now.
        if deferred {
            self.job_dispatcher.note_backlog(table);
        }
        Ok(())
    }


    /// One TTL sweep over this node's ports (see [`ttl_cleanup_sweep`]).
    /// Timer entry: flush the dirty per-view metrics (see `flush_view_metrics`).
    pub async fn flush_view_metrics(&self) -> usize {
        flush_view_metrics(self.platform.db.as_ref(), &self.view_metrics).await
    }

    pub async fn ttl_cleanup_sweep(&self) -> usize {
        ttl_cleanup_sweep(
            self.platform.db.as_ref(),
            &self.processor,
            self.platform.telemetry.as_ref(),
            self.ref_mode,
        )
        .await
    }
}

/// One TTL sweep: delete every expired query view (its `_00_query` row, its
/// per-user `_00_list_ref` edges, the in-memory circuit view) + drop any
/// per-user table that no longer backs a live query. Free fn over the `Db` +
/// `Telemetry` ports so the VM shell (and its DB integration tests) can call it
/// without an `SspNode`, while `SspNode::ttl_cleanup_sweep` delegates here —
/// single implementation, no drift.
pub async fn ttl_cleanup_sweep(
    db: &dyn Db,
    processor: &Arc<RwLock<Circuit>>,
    telemetry: &dyn Telemetry,
    ref_mode: ssp_protocol::RefMode,
) -> usize {
    // Drop subscriber entries that have gone stale, so a row that is
    // heartbeated rarely does not accumulate dead sessions between writes.
    // Purely hygiene: it never expires a row, only prunes the advisory array.
    // The WHERE matches only rows that actually carry a STALE entry, not every
    // row that has subscribers: the matched rows are the write set this
    // statement contends on, and the ingest path is UPDATEing `_00_query` rows
    // continuously. Rewriting an unchanged array bought nothing but conflicts.
    // `subscribers ?? []` in the SET, not just in the WHERE: SurrealDB 3
    // evaluates the SET expression against rows the WHERE excludes, so a single
    // row without a `subscribers` field (every row registered before the field
    // existed, and every row whose registration predates its first heartbeat)
    // failed the WHOLE statement with "Incorrect arguments for function
    // array::filter() … Expected `array` but found `NONE`". Pruning has been
    // dead in production for exactly that reason, warn-logged once per sweep.
    if let Err(e) = query_retrying(
        db,
        "UPDATE _00_query SET subscribers = array::filter(subscribers ?? [], |$s| \
             <datetime>$s.seenAt + ttl > time::now()) \
         WHERE array::len(array::filter(subscribers ?? [], |$s| \
             <datetime>$s.seenAt + ttl <= time::now())) > 0",
        &[],
    )
    .await
    {
        warn!("TTL sweep: pruning stale subscribers failed: {}", e);
    }

    // Expired queries as "<id>|<auth_id>" strings (scalar String reads cleanly).
    //
    // Deliberately keyed on `lastActiveAt`, NOT on an empty `subscribers`
    // array. A view is shared, so any live subscriber's heartbeat refreshes
    // this one timestamp — "nobody heartbeated within one TTL" is exactly
    // "nobody is watching". A client that crashes never removes its entry, so
    // gating expiry on emptiness instead would let one dead tab pin a view
    // (and its ~45MB of operator state) forever. The subscriber set only ever
    // gates the eager release path in `fn::query::unsubscribe`.
    let rows: Vec<String> = match db
        .query(
            "SELECT VALUE (<string>id + '|' + <string>(auth_id OR '')) \
             FROM _00_query WHERE lastActiveAt + ttl < time::now()",
            &[],
        )
        .await
    {
        Ok(results) => results
            .into_iter()
            .next()
            .and_then(|v| v.as_array().cloned())
            .map(|a| a.into_iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default(),
        Err(e) => {
            error!("TTL cleanup: query failed: {}", e);
            return 0;
        }
    };

    let count = rows.len();
    let mut cleaned: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in rows {
        let mut parts = row.splitn(2, '|');
        let id = parts.next().unwrap_or_default();
        let auth_id = parts.next().unwrap_or_default().to_string();
        if id.is_empty() {
            continue;
        }
        let raw = id.strip_prefix("_00_query:").unwrap_or(id).to_string();
        cleanup_expired_query(db, processor, telemetry, &raw, &auth_id, ref_mode).await;
        if !auth_id.is_empty() {
            cleaned.insert(auth_id);
        }
    }

    for auth_id in &cleaned {
        if let Err(e) = crate::tables::drop_user_table_if_unused(db, ref_mode, auth_id).await {
            warn!(auth_id = %auth_id, error = %e, "TTL cleanup: drop_user_table_if_unused failed");
        }
    }
    if let Err(e) = crate::tables::drop_orphaned_user_tables(db, ref_mode).await {
        warn!(error = %e, "TTL cleanup: drop_orphaned_user_tables failed");
    }
    if count > 0 {
        info!(count, "TTL cleanup sweep completed");
    }
    count
}

/// Delete one expired query iff its TTL is still expired (guards a heartbeat
/// racing the sweep), plus its edges + circuit view.
async fn cleanup_expired_query(
    db: &dyn Db,
    processor: &Arc<RwLock<Circuit>>,
    telemetry: &dyn Telemetry,
    query_id: &str,
    auth_id: &str,
    ref_mode: ssp_protocol::RefMode,
) {
    let list_ref = crate::tables::list_ref_table(ref_mode, auth_id);

    // Conditional delete; `RETURN array::len(...)` reads back the count.
    let deleted: i64 = match query_retrying(
        db,
        "LET $d = (DELETE type::record('_00_query', $qid) \
         WHERE lastActiveAt + ttl < time::now() RETURN BEFORE); \
         RETURN array::len($d);",
        &[("qid", json!(query_id))],
    )
    .await
    {
        Ok(results) => results.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
        Err(e) => {
            // Post-retry. A conflict that survives the budget is not fatal:
            // the row stays expired and the next sweep picks it up again.
            warn!(query_id = %query_id, error = %e, "TTL cleanup: delete failed (retries exhausted)");
            return;
        }
    };
    if deleted == 0 {
        debug!(query_id = %query_id, "TTL cleanup: query refreshed, skipping");
        return;
    }

    let edge_delete = format!("DELETE type::record('_00_query', $qid)->{list_ref}");
    if let Err(e) = query_retrying(db, &edge_delete, &[("qid", json!(query_id))]).await {
        warn!(query_id = %query_id, error = %e, "TTL cleanup: edge delete failed (retries exhausted)");
    }

    // Release, don't destroy: an expired row is one holder leaving, and the
    // graph may still be serving other registrations of the same computation.
    // Each holder has its own `_00_query` row and so its own `lastActiveAt`,
    // which is what makes per-row expiry the right granularity here.
    processor.write().await.detach_subscriber(query_id);
    telemetry.gauge_add("view_count", -1);
    telemetry.counter("ttl_cleanup", 1);
    info!(query_id = %query_id, "TTL cleanup: query expired and removed");
}

/// First statement's rows as owned `Value`s (the flattened SELECT result is an
/// array; a bare object becomes a one-element list).
fn rows_of(results: Vec<Value>) -> Vec<Value> {
    match results.into_iter().next() {
        Some(Value::Array(rows)) => rows,
        Some(other @ Value::Object(_)) => vec![other],
        _ => Vec::new(),
    }
}

/// Poll `_00_version` until the source row reaches `expected_version`, so the
/// deferred list_ref writes land after the source row is readable downstream.
/// Returns `true` if observed within `timeout`, `false` on timeout (caller
/// proceeds anyway). Times through the `Scheduler` port so it is portable.
async fn wait_for_row_committed(
    db: &dyn Db,
    scheduler: &dyn crate::ports::Scheduler,
    row_id: &str,
    expected_version: i64,
    timeout: std::time::Duration,
) -> bool {
    if !valid_record_id(row_id) {
        return false;
    }
    let start = web_time::Instant::now();
    let mut backoff_ms: u64 = 10;
    while start.elapsed() < timeout {
        if let Ok(rows) = db
            .query(
                "SELECT VALUE version FROM ONLY _00_version WHERE record_id = type::record($rid) LIMIT 1",
                &[("rid", json!(row_id))],
            )
            .await
        {
            if let Some(v) = rows.first().and_then(|v| v.as_i64()) {
                if v >= expected_version {
                    return true;
                }
            }
        }
        scheduler.sleep(std::time::Duration::from_millis(backoff_ms)).await;
        if backoff_ms < 80 {
            backoff_ms *= 2;
        }
    }
    false
}

/// Update the in-memory rolling latency window per affected view and persist
/// row count + percentiles onto each `_00_query` row.
/// Note one ingest against every view it touched. Memory only: the flush to
/// `_00_query` runs on its own timer (`flush_view_metrics`), so the ingest
/// path never writes the rows the edge transaction is writing at the same
/// moment.
pub async fn note_view_metrics(
    view_metrics: &crate::view_metrics::ViewMetrics,
    row_counts: Vec<usize>,
    view_ids: Vec<String>,
    materialization_time_ms: f64,
) {
    if view_ids.is_empty() {
        return;
    }
    let mut map = view_metrics.write().await;
    for (view_id, row_count) in view_ids.into_iter().zip(row_counts) {
        map.entry(view_id).or_default().note_ingest(row_count, materialization_time_ms);
    }
}

/// Write every dirty view's metrics to its `_00_query` row, one retried
/// UPDATE each, and clear the dirty marks. Returns how many rows were written.
/// Same statement and columns as the old per-ingest write, so the row shape is
/// unchanged; only the cadence moved (registration still writes `rowCount`
/// synchronously, so first paint does not wait on this).
pub async fn flush_view_metrics(
    db: &dyn Db,
    view_metrics: &crate::view_metrics::ViewMetrics,
) -> usize {
    let snapshots: Vec<_> = {
        let mut map = view_metrics.write().await;
        map.iter_mut()
            .filter(|(_, st)| st.dirty)
            .map(|(view_id, st)| {
                st.dirty = false;
                (view_id.clone(), st.row_count, st.update_count, st.last_ingest_ms, st.percentiles())
            })
            .collect()
    };
    let mut written = 0usize;
    for (view_id, row_count, update_count, last_ingest_ms, percentiles) in snapshots {
        let incantation_id = crate::edges::format_incantation_id(&view_id);
        let (p55, p90, p99) = match percentiles {
            Some(t) => (json!(t.0), json!(t.1), json!(t.2)),
            None => (Value::Null, Value::Null, Value::Null),
        };
        let stmt = "UPDATE type::record($id) SET \
            rowCount = <int>$rowCount, updateCount = <int>$updateCount, \
            lastIngestLatency = <float>$lastIngestLatency, \
            materializationP55 = $p55, materializationP90 = $p90, materializationP99 = $p99";
        match query_retrying(
            db,
            stmt,
            &[
                ("id", json!(incantation_id)),
                ("rowCount", json!(row_count as i64)),
                ("updateCount", json!(update_count as i64)),
                ("lastIngestLatency", json!(last_ingest_ms)),
                ("p55", p55),
                ("p90", p90),
                ("p99", p99),
            ],
        )
        .await
        {
            Ok(_) => written += 1,
            Err(e) => {
                // Left dirty for the next flush: the sample is still in memory.
                view_metrics.write().await.entry(view_id).or_default().dirty = true;
                warn!(target: "ssp::view_metrics", error = %e, view_id = %incantation_id, "Failed to persist per-view metrics");
            }
        }
    }
    written
}

fn valid_record_id(id: &str) -> bool {
    matches!(id.split_once(':'), Some((t, k)) if !t.is_empty() && !k.is_empty())
}

/// Wipe the in-memory circuit and every view edge in the database. Used by
/// the `/reset` route and by the standalone restore path (the restored dump
/// carries pre-restore `_00_list_ref*` rows that no longer match any circuit).
pub async fn wipe_circuit_and_edges(
    db: &dyn Db,
    processor: &Arc<RwLock<Circuit>>,
    telemetry: &dyn Telemetry,
    ref_mode: ssp_protocol::RefMode,
) {
    let old_view_count = {
        let mut circuit = processor.write().await;
        let count = circuit.view_count();
        *circuit = Circuit::new();
        count
    };

    telemetry.gauge_add("view_count", -(old_view_count as i64));

    // Delete all edges. In dedicated mode that's every
    // `_00_list_ref_user_*` table; single mode is just the global one.
    match ref_mode {
        ssp_protocol::RefMode::Single => {
            if let Err(e) = db.query("DELETE _00_list_ref", &[]).await {
                error!("Failed to delete all edges on reset: {}", e);
            }
        }
        ssp_protocol::RefMode::Dedicated => {
            // Walk every per-user list_ref table and wipe it. Cheap because
            // this path is only used in tests / explicit resets / restores.
            match db.query("INFO FOR DB", &[]).await {
                Ok(results) => {
                    let tables = results
                        .first()
                        .and_then(|v| v.get("tables"))
                        .and_then(|t| t.as_object())
                        .cloned()
                        .unwrap_or_default();
                    for name in tables.keys() {
                        if !name.starts_with("_00_list_ref_user_") {
                            continue;
                        }
                        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                            continue;
                        }
                        let stmt = format!("DELETE {}", name);
                        if let Err(e) = db.query(&stmt, &[]).await {
                            error!(table = %name, "Failed to delete edges on reset: {}", e);
                        }
                    }
                }
                Err(e) => error!("Failed to enumerate edge tables on reset: {}", e),
            }
        }
    }
}
