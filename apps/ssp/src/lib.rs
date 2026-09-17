use anyhow::Context;
use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use ssp::circuit::{Circuit, Record};
use ssp::circuit::view::OutputFormat;
use tokio::signal;
use tracing::{debug, error, info, warn};

// Expose modules for use in main.rs and tests
mod changefeed;
pub mod adapters;
pub mod crdt;
pub mod edge_updates;
pub mod maintenance_host;
pub mod metrics;
pub mod open_telemetry;
pub mod tables;
pub mod view_metrics;

use metrics::Metrics;
use view_metrics::ViewMetrics;

use ssp_node::jobs::{
    JobConfig, JobControl, JobDispatcher, JobEntry, JobRunner,
};
use tokio::sync::mpsc;

/// Shared database connection, shared across tasks without copying.
///
/// `ReconnectingDb` rather than a bare `Surreal<Client>` so that a SurrealDB
/// restart — which wipes the server-side HTTP session every handle is pinned to
/// — is survivable. Reach the live handle with `.handle()`; see
/// [`maintenance::db::ReconnectingDb`] for why re-signin alone cannot recover.
pub type SharedDb = Arc<maintenance::db::ReconnectingDb>;

// Status types live in the portable core; re-exported for compatibility.
pub use ssp_node::{error_codes, SspError, SspStatus};

#[derive(Clone)]
pub struct AppState {
    pub db: SharedDb,
    pub processor: Arc<RwLock<Circuit>>,
    pub status: Arc<RwLock<SspStatus>>,
    pub metrics: Arc<Metrics>,
    pub job_config: Arc<JobConfig>,
    /// Admission control for job execution — the only way onto the runner.
    pub job_dispatcher: Arc<JobDispatcher>,
    /// Shared cancellation/kill state, also held by the `JobRunner`. Lets the
    /// `/job/kill` and `/job/retry` handlers cancel in-flight requests and drop
    /// queued jobs without racing the runner (which stays the sole `status` writer).
    pub job_control: JobControl,
    pub ssp_id: String,
    pub scheduler_url: Option<String>,
    pub start_time: std::time::Instant,
    pub crdt_cache: Arc<crdt::CrdtCache>,
    /// Per-view rolling materialization-latency samples plus running
    /// update/error counters. Persisted onto `_00_query` after each
    /// affected ingest step.
    pub view_metrics: Arc<ViewMetrics>,
    /// `_00_query` / `_00_list_ref` storage layout. See
    /// `ssp_protocol::RefMode` and `crate::tables`.
    pub ref_mode: ssp_protocol::RefMode,
    /// When `true`, anonymous (empty `auth_id`) query registrations are routed
    /// to the world-readable `_00_list_ref_anon` table so logged-out clients
    /// can sync live. See `Config::anonymous_live_queries`.
    pub anonymous_live_queries: bool,
    /// Version of the upstream SurrealDB server, queried once at startup
    /// (`"unknown"` if the query failed). Surfaced via `/info` so the DevTools
    /// can report the SurrealDB backend version.
    pub surrealdb_version: String,
    /// Shared bounded publication queue. Accepted work retains capacity until
    /// publication or lifecycle invalidation, including source-commit waits
    /// and parked retries. The throttle debounces only the start of a burst.
    pub edge_update_tx: ssp_node::edges::EdgePublisher,
    /// Backend health monitoring — standalone mode only (`None` in cluster
    /// mode, where the scheduler owns backend healthchecks). Populated from
    /// `SPKY_BACKENDS` and live-updatable via `PUT /backends`.
    pub backend_health: Option<maintenance::BackendHealthCache>,
    /// Live-updatable backend configs backing the health monitor.
    pub shared_backend_configs: Option<maintenance::SharedBackendConfigs>,
    /// Platform port bundle (see `ssp-node` + `docs/platform-architecture.md`).
    /// Handlers migrate onto these ports incrementally; today it proves
    /// construction/object-safety and carries the VM adapters.
    pub platform: ssp_node::Platform,
    /// Bearer secret for the authenticated route group (`NodeConfig.auth_secret`).
    /// Empty accepts any bearer (dev only).
    pub auth_secret: String,
    /// The portable node — serves every route already migrated out of this
    /// shell (mounted as the axum fallback in `create_app`).
    pub node: Arc<ssp_node::SspNode>,
}

// --- Request/Response DTOs ---


// --- Configuration ---

/// Node configuration now lives in the portable core (`ssp_node::NodeConfig`)
/// so both shells share one definition; this shell builds it from env vars.
pub use ssp_node::NodeConfig as Config;

/// Resolve the circuit checkpoint cadence from the environment.
///
/// `None` means "never checkpoint", and it is the answer whenever a checkpoint
/// could not be consumed:
///
/// - no `SPKY_SSP_SNAPSHOT_DIR`: there is no store to write into;
/// - a cluster node (scheduler URL set): the cluster bootstrap pages the
///   database through the scheduler proxy and never calls
///   `CircuitStore::load` (`ssp_node::Runtime::bootstrap` is the standalone
///   path only), so a checkpoint here is write-only. On a large tenant that
///   write was measured at 709 MB every ~6 minutes, serialised under the
///   circuit read lock: `/ingest` blocked for a minute, SurrealDB's fsyncs
///   stalled past 25 s behind it, and the SSP heartbeat queued behind the
///   blocked ingest writer until the scheduler evicted the node;
/// - `SPKY_SSP_CHECKPOINT_INTERVAL_SECS=0`: explicit opt-out. Before this
///   guard a zero re-armed the timer immediately, i.e. checkpointed in a loop.
pub fn resolve_checkpoint_interval(
    snapshot_dir_set: bool,
    interval_env: Option<&str>,
    standalone: bool,
) -> Option<u64> {
    if !snapshot_dir_set || !standalone {
        return None;
    }
    let secs = interval_env
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(300);
    (secs > 0).then_some(secs)
}

/// The view count a heartbeat reports, without waiting on the circuit.
///
/// Liveness must not queue behind the circuit lock: a checkpoint or a long
/// registration holds it for tens of seconds, and tokio's `RwLock` is
/// write-preferring, so once an `/ingest` writer is parked behind that holder
/// a plain `read()` here parks behind the writer too. Past 30 s of silence
/// the scheduler evicts the SSP and it exits — for a view count nobody needed
/// to be exact. When the lock is busy the last observed count is reported.
pub fn heartbeat_view_count(processor: &Arc<RwLock<Circuit>>, last: &mut usize) -> usize {
    if let Ok(circuit) = processor.try_read() {
        *last = circuit.view_count();
    }
    *last
}

pub fn load_config() -> Config {
    let scheduler_url = std::env::var("SPKY_SCHEDULER_URL").ok();
    let standalone = scheduler_url.is_none();
    Config {
        listen_addr: std::env::var("SPKY_SSP_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8667".to_string()),
        // SPKY_DB_URL is canonical; SPKY_DB_WS kept as a legacy fallback
        // (any scheme is accepted — ws:// URLs are normalized to HTTP).
        db_addr: std::env::var("SPKY_DB_URL")
            .or_else(|_| std::env::var("SPKY_DB_WS"))
            .unwrap_or_else(|_| "http://127.0.0.1:8000".to_string()),
        db_user: std::env::var("SPKY_DB_USER").unwrap_or_else(|_| "root".to_string()),
        db_pass: std::env::var("SPKY_DB_PASS").unwrap_or_else(|_| "root".to_string()),
        db_ns: std::env::var("SPKY_DB_NS").unwrap_or_else(|_| "test".to_string()),
        db_db: std::env::var("SPKY_DB_NAME").unwrap_or_else(|_| "test".to_string()),
        scheduler_url,
        ssp_id: std::env::var("SPKY_SSP_ID")
            .unwrap_or_else(|_| format!("ssp-{}", uuid::Uuid::new_v4())),
        heartbeat_interval_ms: std::env::var("HEARTBEAT_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5000),
        advertise_addr: std::env::var("SPKY_SSP_ADVERTISE_ADDR").ok(),
        ttl_cleanup_interval_secs: std::env::var("TTL_CLEANUP_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60),
        view_metrics_flush_ms: std::env::var("SPKY_SSP_VIEW_METRICS_FLUSH_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2000),
        register_max_wait_secs: std::env::var("SPKY_SSP_REGISTER_MAX_WAIT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(180),
        ref_mode: std::env::var("SPKY_SSP_REF_MODE")
            .ok()
            .as_deref()
            .and_then(ssp_protocol::RefMode::parse_str)
            .unwrap_or_default(),
        query_allowlist: std::env::var("SPKY_SSP_QUERY_ALLOWLIST")
            .ok()
            .map(|v| {
                v.parse::<ssp::allowlist::Mode>().unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "SPKY_SSP_QUERY_ALLOWLIST ignored");
                    ssp::allowlist::Mode::Off
                })
            })
            .unwrap_or_default(),
        anonymous_live_queries: std::env::var("SPKY_SSP_ANON_LIVE_QUERIES")
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true"
            })
            .unwrap_or(false),
        impersonation: ssp_protocol::impersonation::env_enabled(
            std::env::var(ssp_protocol::impersonation::ENV_FLAG).ok().as_deref(),
        ),
        query_update_throttle_ms: std::env::var("SPKY_SSP_QUERY_UPDATE_THROTTLE_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100),
        merge_views: std::env::var("SPKY_SSP_MERGE_VIEWS")
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true"
            })
            .unwrap_or(false),
        auth_secret: std::env::var("SPKY_AUTH_SECRET").unwrap_or_default(),
        // 200 was safe but slow: each page is a WHERE+ORDER BY+LIMIT pass over
        // the scheduler's replica (~0.3-0.6s on a 156k-row table), so a big
        // table took minutes and blew the scheduler's bootstrap budget. 1000
        // keeps pages ~1MB while cutting round-trips 5x.
        bootstrap_page_size: std::env::var("SPKY_SSP_BOOTSTRAP_PAGE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &usize| *n > 0)
            .unwrap_or(1000),
        crdt_cache_size: std::env::var("SPKY_CRDT_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024),
        health_check_interval_secs: std::env::var("SPKY_HEALTH_CHECK_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(15),
        // Standalone hosts with a snapshot dir only; see
        // `resolve_checkpoint_interval` for why a cluster node never
        // checkpoints. `Circuit::save` still builds the whole JSON text in
        // memory (and clones every view's params on the way), so even where it
        // runs the peak is the serialised store, not a no-op.
        checkpoint_interval_secs: resolve_checkpoint_interval(
            std::env::var_os("SPKY_SSP_SNAPSHOT_DIR").is_some(),
            std::env::var("SPKY_SSP_CHECKPOINT_INTERVAL_SECS").ok().as_deref(),
            standalone,
        ),
        max_snapshot_age_secs: 3600,
    }
}

// --- Scheduler Registration Helper ---

/// Why a registration attempt failed, used by the caller's retry loop to decide
/// whether retrying can ever succeed.
enum RegisterError {
    /// The scheduler answered 503: it is up and reachable but `Cloning` /
    /// `Restoring` (a cold `--clean` re-clone, or the startup drift re-clone,
    /// which on a large tenant runs for ten minutes). Nothing to do but wait;
    /// see `register_with_retry` for why this does not burn the exit budget.
    NotReady(String),
    /// Transient: another 5xx, a transport/timeout error, or an unparseable
    /// response. Worth retrying, but bounded — a scheduler that never answers
    /// is a supervisor problem, and exiting hands it over.
    Retryable(String),
    /// Permanent for this process: the scheduler rejected the request with a
    /// 4xx (e.g. 400 bad ssp_id/url). Retrying the same payload won't help —
    /// surface it loudly instead of silently spinning.
    Fatal(String),
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegisterError::NotReady(m) => write!(f, "{}", m),
            RegisterError::Retryable(m) => write!(f, "{}", m),
            RegisterError::Fatal(m) => write!(f, "{}", m),
        }
    }
}

/// Whether a failed registration attempt restarts the exit budget instead of
/// spending it. A scheduler that answers "not ready" is alive and will admit us
/// when its clone finishes; exiting on a timer there only re-runs the same
/// wait from zero (observed as two pointless SSP restarts, `RestartCount`
/// climbing, and a ~2-minute gap in registration attempts each time).
fn registration_budget_restarts_on(err: &RegisterError) -> bool {
    matches!(err, RegisterError::NotReady(_))
}

/// Build the SSP registration payload and POST it to the scheduler.
/// Returns the registration response (which carries `snapshot_seq` and the
/// per-table content hashes the SSP must match after bootstrap) or an error
/// classified as retryable vs fatal.
async fn register_with_scheduler(
    client: &reqwest::Client,
    scheduler_url: &str,
    ssp_id: &str,
    listen_addr: &str,
    advertise_addr: Option<&str>,
) -> Result<ssp_protocol::SspRegistrationResponse, RegisterError> {
    let scheduler_base = scheduler_url.trim_end_matches('/');
    let registration_url = format!("{}/ssp/register", scheduler_base);

    let registration_host = if let Some(addr) = advertise_addr {
        addr.to_string()
    } else {
        let (host, port) = listen_addr.rsplit_once(':').unwrap_or(("0.0.0.0", "8667"));
        if host == "0.0.0.0" || host == "127.0.0.1" {
            let hostname = hostname::get()
                .map(|h| h.to_string_lossy().into_owned())
                .unwrap_or_else(|_| host.to_string());
            format!("{}:{}", hostname, port)
        } else {
            listen_addr.to_string()
        }
    };

    // Collect relevant env vars to send to scheduler
    let env_vars: std::collections::HashMap<String, String> = [
        "SPKY_DB_URL", "SPKY_DB_NS", "SPKY_DB_NAME", "SPKY_DB_USER",
        "SPKY_SCHEDULER_URL", "SPKY_SSP_LISTEN_ADDR", "SPKY_SSP_ADVERTISE_ADDR", "SPKY_SSP_ID",
        "HEARTBEAT_INTERVAL_MS", "TTL_CLEANUP_INTERVAL_SECS",
    ].iter().filter_map(|&key| {
        std::env::var(key).ok().map(|val| (key.to_string(), val))
    }).collect();

    let payload = ssp_protocol::SspRegistration {
        ssp_id: ssp_id.to_string(),
        url: format!("http://{}", registration_host),
        version: env!("CARGO_PKG_VERSION").to_string(),
        env: if env_vars.is_empty() { None } else { Some(env_vars) },
    };

    match client
        .post(&registration_url)
        .json(&payload)
        // Bound a single attempt so a hung connection can't stall the retry
        // loop. Per-request (not client-wide) so it doesn't shorten the proxy
        // bootstrap queries that reuse the same client.
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => resp
            .json::<ssp_protocol::SspRegistrationResponse>()
            .await
            // A 2xx with an unparseable body is almost always a transient proxy
            // hiccup — retry rather than give up.
            .map_err(|e| RegisterError::Retryable(format!("Failed to parse registration response: {}", e))),
        // 503: the scheduler is up but Cloning/Restoring → wait it out.
        Ok(resp) if resp.status() == StatusCode::SERVICE_UNAVAILABLE => {
            Err(RegisterError::NotReady(format!("HTTP {}", resp.status())))
        }
        // Any other 5xx: scheduler unhealthy → retry within the budget.
        Ok(resp) if resp.status().is_server_error() => {
            Err(RegisterError::Retryable(format!("HTTP {}", resp.status())))
        }
        // 4xx (e.g. 400 bad ssp_id/url): a real misconfiguration → fatal.
        Ok(resp) => Err(RegisterError::Fatal(format!("HTTP {}", resp.status()))),
        // Connection refused / DNS not ready / timeout: scheduler not up yet → retry.
        Err(e) => Err(RegisterError::Retryable(format!("{}", e))),
    }
}

/// Retry `register_with_scheduler` with exponential backoff, carrying the
/// process-exit policy — only returns on success.
///
/// The scheduler returns 503 while `Cloning`/`Restoring` (e.g. a cold
/// `--clean` re-clone), and may be unreachable for a moment while DNS/the
/// container comes up — both are transient. A single attempt that gave up
/// (the old behaviour) left the SSP a permanently-unregistered zombie: it
/// never exited, so the supervisor's restart-on-failure policy never fired,
/// and the heartbeat loop only runs once status==Ready. So we keep trying
/// within a budget, then exit so the supervisor reruns the whole handshake
/// against a (by then) Ready scheduler.
///
/// Exit codes: 6 = fatal rejection (4xx, retrying won't help); 5 = retry
/// budget exhausted. Distinct from the data-bootstrap exit(2) and heartbeat
/// exit(3)/exit(4) so the failure mode is identifiable in logs.
async fn register_with_retry(
    client: &reqwest::Client,
    scheduler_url: &str,
    ssp_id: &str,
    listen_addr: &str,
    advertise_addr: Option<&str>,
    register_max_wait_secs: u64,
    status: &Arc<RwLock<SspStatus>>,
) -> ssp_protocol::SspRegistrationResponse {
    let max_wait = std::time::Duration::from_secs(register_max_wait_secs);
    let started = std::time::Instant::now();
    // The exit budget counts time since the scheduler last answered "not
    // ready", not since we started: a reachable scheduler mid-clone is not a
    // failure to escalate, it is the thing we are waiting for.
    let mut start = started;
    let mut backoff_ms: u64 = 1000;
    loop {
        match register_with_scheduler(client, scheduler_url, ssp_id, listen_addr, advertise_addr)
            .await
        {
            Ok(r) => {
                info!(
                    snapshot_seq = r.snapshot_seq,
                    tables = r.table_hashes.len(),
                    waited_secs = started.elapsed().as_secs(),
                    "Successfully registered with scheduler"
                );
                return r;
            }
            Err(RegisterError::Fatal(e)) => {
                error!(
                    error = %e,
                    "Scheduler rejected registration (non-retryable) — exiting for visibility"
                );
                *status.write().await = SspStatus::Failed;
                std::process::exit(6);
            }
            Err(err @ RegisterError::NotReady(_)) if registration_budget_restarts_on(&err) => {
                start = std::time::Instant::now();
                info!(
                    error = %err,
                    waited_secs = started.elapsed().as_secs(),
                    "Scheduler is up but not ready (cloning or restoring); waiting for it"
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(10_000);
            }
            Err(RegisterError::Retryable(e)) | Err(RegisterError::NotReady(e)) => {
                let elapsed = start.elapsed();
                if elapsed >= max_wait {
                    error!(
                        error = %e,
                        waited_secs = elapsed.as_secs(),
                        max_wait_secs = register_max_wait_secs,
                        "Scheduler registration still failing after max wait — exiting for restart"
                    );
                    *status.write().await = SspStatus::Failed;
                    std::process::exit(5);
                }
                warn!(
                    error = %e,
                    backoff_ms,
                    waited_secs = elapsed.as_secs(),
                    "Scheduler not ready for registration, retrying"
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(10_000);
            }
        }
    }
}

/// Ask the scheduler to settle a bootstrap integrity mismatch against its
/// replica *content* (see `handle_bootstrap_verify`). Returns `None` when no
/// verdict could be obtained — standalone mode, an older scheduler without the
/// endpoint (404), or a transport error — so the caller can fall back to the
/// legacy re-register-and-retry path.
async fn verify_bootstrap_with_scheduler(
    scheduler_url: &str,
    ssp_id: &str,
    actual: &BTreeMap<String, String>,
) -> Option<ssp_protocol::SspBootstrapVerifyResponse> {
    let url = format!(
        "{}/ssp/bootstrap-verify",
        scheduler_url.trim_end_matches('/')
    );
    let payload = ssp_protocol::SspBootstrapVerifyRequest {
        ssp_id: ssp_id.to_string(),
        table_hashes: actual.clone(),
    };

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&payload)
        // The scheduler rehashes the disputed tables from content, which on a
        // large table is seconds of paged reads — generous, but bounded so a
        // hung scheduler can't park the bootstrap forever.
        .timeout(std::time::Duration::from_secs(120))
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(v) => Some(v),
            Err(e) => {
                warn!(error = %e, "Could not parse bootstrap-verify response; falling back to re-register");
                None
            }
        },
        Ok(r) => {
            warn!(status = %r.status(), "Scheduler bootstrap-verify unavailable; falling back to re-register");
            None
        }
        Err(e) => {
            warn!(error = %e, "Scheduler bootstrap-verify request failed; falling back to re-register");
            None
        }
    }
}

// --- Bootstrap Source ---

/// Abstraction for database access during bootstrap.
/// In standalone mode, bootstraps directly from SurrealDB.
/// In cluster mode, bootstraps from the scheduler's HTTP proxy.
pub enum BootstrapSource {
    /// Direct SurrealDB connection (standalone mode)
    Direct(SharedDb),
    /// HTTP proxy to scheduler's snapshot DB (cluster mode)
    Proxy {
        client: reqwest::Client,
        proxy_url: String,
    },
}

/// Publishes bootstrap progress to the scheduler so the admin dashboard can
/// render a real progress bar instead of a spinner.
///
/// Deliberately advisory: every post is fire-and-forget, failures are swallowed
/// at `debug`, and nothing about the bootstrap waits on it. It is also
/// throttled to one post per `MIN_POST_INTERVAL`, because a table of a few
/// million rows pages fast enough to otherwise turn a progress bar into a
/// denial-of-service against our own scheduler. The final post of each table is
/// forced through the throttle so the bar always lands on a truthful number.
pub struct BootstrapReporter {
    client: reqwest::Client,
    url: String,
    ssp_id: String,
    state: std::sync::Mutex<ssp_protocol::BootstrapProgress>,
    /// Last time a post actually went out. `None` = never.
    last_post: std::sync::Mutex<Option<std::time::Instant>>,
}

impl BootstrapReporter {
    const MIN_POST_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1000);

    pub fn new(client: reqwest::Client, scheduler_base: &str, ssp_id: String) -> Self {
        Self {
            client,
            url: format!("{}/ssp/bootstrap-progress", scheduler_base.trim_end_matches('/')),
            ssp_id,
            state: std::sync::Mutex::new(ssp_protocol::BootstrapProgress::default()),
            last_post: std::sync::Mutex::new(None),
        }
    }

    /// Total table count, known once `INFO FOR DB` has been read.
    pub async fn set_total(&self, total: usize) {
        {
            let mut st = self.state.lock().expect("bootstrap progress poisoned");
            st.tables_total = total;
            st.tables_done = 0;
            st.rows_loaded = 0;
            st.current_table = None;
        }
        self.post(true).await;
    }

    pub async fn start_table(&self, table: &str) {
        {
            let mut st = self.state.lock().expect("bootstrap progress poisoned");
            st.current_table = Some(table.to_string());
        }
        self.post(true).await;
    }

    /// One page landed. Throttled — this is the hot call.
    pub async fn add_rows(&self, n: usize) {
        {
            let mut st = self.state.lock().expect("bootstrap progress poisoned");
            st.rows_loaded = st.rows_loaded.saturating_add(n as u64);
        }
        self.post(false).await;
    }

    pub async fn finish_table(&self) {
        {
            let mut st = self.state.lock().expect("bootstrap progress poisoned");
            st.tables_done = st.tables_done.saturating_add(1);
            st.current_table = None;
        }
        self.post(true).await;
    }

    async fn post(&self, force: bool) {
        let payload = {
            // Both locks are held for a handful of instructions and never
            // across the await below.
            if !force {
                let mut last = self.last_post.lock().expect("bootstrap progress poisoned");
                let now = std::time::Instant::now();
                match *last {
                    Some(t) if now.duration_since(t) < Self::MIN_POST_INTERVAL => return,
                    _ => *last = Some(now),
                }
            } else {
                *self.last_post.lock().expect("bootstrap progress poisoned") =
                    Some(std::time::Instant::now());
            }
            let st = self.state.lock().expect("bootstrap progress poisoned");
            ssp_protocol::SspBootstrapProgressRequest {
                ssp_id: self.ssp_id.clone(),
                progress: st.clone(),
            }
        };

        if let Err(e) = self.client.post(&self.url).json(&payload).send().await {
            debug!(error = %e, "Bootstrap progress post failed (advisory, ignoring)");
        }
    }
}

impl BootstrapSource {
    async fn query(&self, surql: &str) -> anyhow::Result<Value> {
        match self {
            BootstrapSource::Direct(db) => {
                // JSON flattening lives with the Db adapter so the port impl
                // and this bootstrap path can never drift.
                crate::adapters::SurrealSdkDb::flatten_first(db, surql).await
            }
            BootstrapSource::Proxy { client, proxy_url } => {
                let url = format!("{}/query", proxy_url);
                let resp = client
                    .post(&url)
                    .json(&json!({"query": surql}))
                    .send()
                    .await
                    .with_context(|| format!("Proxy query failed: {}", surql))?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Proxy returned {}: {}", status, body);
                }

                resp.json().await.context("Failed to parse proxy response")
            }
        }
    }
}

// --- Database Connection ---

pub async fn connect_database(config: &Config) -> anyhow::Result<SharedDb> {
    info!(addr = %config.db_addr, "Connecting to SurrealDB");

    let db_config = maintenance::db::DbConfig {
        url: config.db_addr.clone(),
        namespace: config.db_ns.clone(),
        database: config.db_db.clone(),
        username: config.db_user.clone(),
        password: config.db_pass.clone(),
    };
    let db = maintenance::db::ReconnectingDb::connect(&db_config)
        .await
        .context("Failed to connect to SurrealDB")?;

    // HTTP-engine tokens expire and the server-side session dies with a
    // SurrealDB restart; the run_server timer dispatcher keeps the handle both
    // fresh and reconnected via TimerKind::DbResignin
    // (maintenance::db::resignin_once). That timer is the periodic floor; this
    // healer is the fast path, reconnecting on the first query that reports a
    // dead session rather than up to a full interval later.
    maintenance::db::spawn_dead_session_healer(Arc::clone(&db));

    info!("Connected to SurrealDB successfully");
    Ok(db)
}

// --- Job Config Loading ---

/// Load job config from the SPKY_JOB_CONFIG env var (JSON).
/// Format: [{"name":"api","table":"job","base_url":"http://...","auth_token":null,"timeout":10,"timeout_overridable":false}, ...]
fn load_job_config_from_env() -> Arc<JobConfig> {
    let json = match std::env::var("SPKY_JOB_CONFIG") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            info!("No SPKY_JOB_CONFIG set, job runner disabled");
            return Arc::new(JobConfig::default());
        }
    };

    // Single parser lives in ssp_node so every host (VM env var, CF Worker
    // binding) agrees on the shape + the null/[]→empty semantics.
    let cfg = JobConfig::from_json(&json);
    if cfg.job_tables.is_empty() {
        info!("SPKY_JOB_CONFIG lists no outbox backends, job runner disabled");
    } else {
        info!(job_tables = cfg.job_tables.len(), "Loaded job config from SPKY_JOB_CONFIG");
    }
    Arc::new(cfg)
}

// --- Router Setup ---

pub fn create_app(state: AppState) -> Router {
    // Routes NOT yet migrated into the portable core (ssp_node::SspNode).
    // Everything absent from this table falls through to `node_fallback`,
    // which dispatches into `node.route()` — migrated routes are simply
    // removed here, one at a time, keeping every commit green.
    //
    // Every SSP route now lives in the portable core (`SspNode::route`); the
    // shell is a pure adapter that converts axum requests and dispatches via
    // `node_fallback`. Auth + CORS are handled inside the core.
    Router::new().fallback(node_fallback).with_state(state)
}

/// Bridge axum → the portable node for every route the core already serves
/// (`/version`, `/log`, `/reset`, `/job/*`). Auth and CORS for these routes
/// live in `SspNode::route` — this shim only shuffles bytes.
async fn node_fallback(State(state): State<AppState>, req: Request) -> Response {
    let method = match *req.method() {
        axum::http::Method::GET => ssp_node::Method::Get,
        axum::http::Method::POST => ssp_node::Method::Post,
        axum::http::Method::PUT => ssp_node::Method::Put,
        axum::http::Method::DELETE => ssp_node::Method::Delete,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let path = req.uri().path().to_string();
    let bearer = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(|s| s.to_string());
    let body = match axum::body::to_bytes(req.into_body(), 32 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };

    let api_req = ssp_node::ApiRequest { method, path, bearer, body };
    match state.node.route(api_req).await {
        Some(resp) => {
            let mut builder = Response::builder().status(resp.status);
            for (name, value) in &resp.headers {
                builder = builder.header(*name, value);
            }
            let (content_type, body_bytes) = match resp.body {
                ssp_node::ApiBody::Json(v) => ("application/json", v.to_string()),
                ssp_node::ApiBody::Text { content_type, body } => (content_type, body),
            };
            builder
                .header(axum::http::header::CONTENT_TYPE, content_type)
                .body(axum::body::Body::from(body_bytes))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}


// --- Server Lifecycle ---

pub async fn run_server() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    // Initialize observability
    let log_ring =
        open_telemetry::init_tracing().context("Failed to initialize OpenTelemetry tracing")?;
    let (meter_provider, metrics) =
        metrics::init_metrics().context("Failed to initialize metrics")?;
    let metrics = Arc::new(metrics);

    info!(
        "\n ____  ____  ____\n/ ___)/ ___)(  _ \\\n\\___ \\\\___ \\ ) __/\n(____/(____/(__)    v{}\n\nSp00ky Sync Provider — streaming mode\nBuilt: {}",
        env!("CARGO_PKG_VERSION"),
        env!("SPOOKY_BUILD_TIMESTAMP"),
    );

    let config = load_config();
    let db = connect_database(&config).await?;

    // Query the upstream SurrealDB server version once; surfaced via `/info`.
    let surrealdb_version = match db.handle().version().await {
        Ok(v) => v.to_string(),
        Err(e) => {
            info!(error = %e, "Could not read SurrealDB server version");
            "unknown".to_string()
        }
    };

    // Start with an empty circuit — self-bootstrap will populate it. The merge
    // policy goes in before anything registers: boot re-registration reads it
    // from the circuit, and this shell's cluster path bootstraps the circuit
    // directly rather than through `Runtime::bootstrap`.
    let processor_arc = Arc::new(RwLock::new(Circuit::new()));
    {
        let mut circuit = processor_arc.write().await;
        circuit.set_merge_views(config.merge_views);
        // Server-side row-version floor: an ingested body whose `_00_rv` is
        // missing or does not advance gets one that does, so subscribers
        // refetch. `SspNode::apply_circuit_policy` turns this on for the
        // `Runtime::bootstrap` path; this shell configures the cluster circuit
        // itself and used to leave it off, so a partial `_00_version` index
        // (whitepawn, 2026-09-07: a callee's `accepted` UPDATE reached the DB
        // and the scheduler, and the caller's edges stayed at version 1) went
        // straight through to every peer as "nothing changed".
        circuit.set_monotonic_row_versions(true);
    }
    let status = Arc::new(RwLock::new(SspStatus::Bootstrapping));

    // VM platform adapters (ssp-node ports) — constructed early because the
    // job runner consumes them. `timer_rx` feeds the timer dispatcher spawned
    // further down (once all its dependencies exist).
    let (platform, mut timer_rx) = adapters::vm_platform(db.clone(), metrics.clone());
    // Set by the heartbeat task when the scheduler asks for a CLEAN restart.
    // The graceful-shutdown checkpoint consults it, because persisting a
    // snapshot on the way out would recreate exactly the file the clean
    // restart just deleted.
    let clean_requested = Arc::new(AtomicBool::new(false));

    // HTTP-engine tokens expire; keep the long-lived shared handle fresh.
    platform
        .scheduler
        .schedule(
            ssp_node::TimerKind::DbResignin,
            ssp_node::now_epoch_ms() + maintenance::db::RESIGNIN_INTERVAL_SECS * 1000,
        )
        .await;

    // Load job configuration from SPKY_JOB_CONFIG env var
    let job_config = load_job_config_from_env();

    // Create job queue channel. This is a handoff buffer, not the queue: the
    // real queue is the backlog of `pending` rows in the outbox, and the
    // dispatcher is what bounds how many leave it at once. Sized well above any
    // sane per-table limit so a burst of admissions is never the thing that
    // blocks.
    let (job_queue_tx, job_queue_rx) = mpsc::channel::<JobEntry>(1024);

    // Shared kill/cancel state. Constructed unconditionally (AppState always needs
    // it); the runner only gets a clone when there are job tables to run.
    let job_control = JobControl::new();

    let job_dispatcher = Arc::new(JobDispatcher::new(
        Arc::clone(&platform.db),
        Arc::clone(&platform.spawner),
        Arc::clone(&platform.scheduler),
        job_queue_tx,
        job_control.clone(),
        job_config.clone(),
        config.ssp_id.clone(),
        config.scheduler_url.is_none(),
    ));

    // Spawn job runner if there are job tables configured
    if !job_config.job_tables.is_empty() {
        // Read each table's `_00_job_policy` row once up front, so the first
        // burst after a restart is governed by the deployed limit instead of
        // running at the default until the first drain refreshes it.
        job_dispatcher.preload_policies().await;

        let job_runner = JobRunner::new(
            job_queue_rx,
            Arc::clone(&platform.db),
            Arc::clone(&platform.http),
            Arc::clone(&platform.scheduler),
            Arc::clone(&platform.spawner),
            Arc::clone(&job_dispatcher),
        );
        tokio::spawn(async move {
            job_runner.run().await;
        });
        info!("Job runner started");

        // Recovery sweep for jobs the CREATE-only pickup path can't reach (rows
        // created while this SSP was down, or 'processing' rows orphaned by a
        // crash). Singlenode only: in cluster mode the scheduler assigns jobs
        // round-robin and the assignee is not persisted on the row, so a
        // DB-driven sweep here couldn't tell ownership and might double-execute —
        // cluster recovery is the scheduler's responsibility.
        if config.scheduler_url.is_none() {
            // Timer-driven (TimerKind::JobRecoverySweep): armed immediately so
            // the first pass runs as soon as the SSP is Ready (the startup
            // sweep), then re-armed every JOB_RECOVERY_INTERVAL_SECS by the
            // dispatcher below.
            platform
                .scheduler
                .schedule(ssp_node::TimerKind::JobRecoverySweep, ssp_node::now_epoch_ms())
                .await;
            info!(
                interval_secs = JOB_RECOVERY_INTERVAL_SECS,
                "Job recovery sweep timer armed"
            );
        } else {
            info!("Job recovery sweep disabled (cluster mode — recovery is the scheduler's responsibility)");
        }
    }

    // Declarative schedules (`schedules:` / `workflows:` in sp00ky.yml). Same
    // singlenode-only reasoning as the recovery sweep: in cluster mode the
    // scheduler service is the single ticker, so an SSP that also ticked would
    // just lose the claim CAS and waste the work.
    let schedule_engine = ssp_node::schedules::build_engine(
        config.scheduler_url.is_none(),
        Arc::clone(&platform.db),
        job_control.clone(),
    );
    if schedule_engine.is_some() {
        // Armed immediately: the first pass plans any schedule whose spec changed
        // in the deploy that just happened, and can fire it in the same sweep.
        platform
            .scheduler
            .schedule(ssp_node::TimerKind::ScheduleSweep, ssp_node::now_epoch_ms())
            .await;
        info!(
            interval_secs = ssp_node::schedules::SCHEDULE_SWEEP_INTERVAL_SECS,
            "Schedule sweep timer armed"
        );
    } else {
        info!("Schedule sweep disabled (cluster mode — the scheduler service owns ticking)");
    }

    // Clone for scheduler integration
    let processor_for_scheduler = processor_arc.clone();

    let crdt_cache = Arc::new(crdt::CrdtCache::new(
        config.crdt_cache_size,
        crdt::allow_list_from_env(),
    ));

    // Bounded publisher for query edges. New bursts debounce for the configured
    // window; queued work then drains without a delay per item. Failed views
    // retain their slots and ordering while disjoint views can publish.
    // Bulk publication gets its own HTTP session. A slow edge transaction or
    // reconnect must not queue control-path work behind the same SDK handle.
    let edge_db = connect_database(&config).await?;
    let edge_connection_for_heartbeat = Arc::clone(&edge_db);
    maintenance::db::spawn_periodic_resignin(Arc::clone(&edge_db), maintenance::db::RESIGNIN_INTERVAL_SECS);
    let edge_db: Arc<dyn ssp_node::Db> = Arc::new(adapters::SurrealSdkDb::new(edge_db));
    let publication_gate = Arc::new(tokio::sync::Mutex::new(()));
    let edge_update_tx = ssp_node::edges::EdgePublisher::default();
    metrics.observe_publication(edge_update_tx.clone(), Arc::clone(&edge_connection_for_heartbeat));
    edge_update_tx.start(&platform, edge_db, processor_arc.clone(), publication_gate.clone(), config.ref_mode,
        std::time::Duration::from_millis(config.query_update_throttle_ms));

    // Standalone mode: this SSP owns the maintenance plane the scheduler
    // provides in cluster mode — backend health monitoring and the
    // backup/restore workers + routes. Gated on the same condition as the
    // job recovery sweep.
    let standalone = config.scheduler_url.is_none();
    let (backend_health, shared_backend_configs) = if standalone {
        let backends = maintenance::backend_health::backends_from_env();
        let cache = maintenance::create_health_cache(&backends);
        let configs = maintenance::create_shared_configs(&backends);
        // Timer-driven (TimerKind::BackendHealth): first sweep immediately,
        // re-armed every health_check_interval_secs by the dispatcher below.
        platform
            .scheduler
            .schedule(ssp_node::TimerKind::BackendHealth, ssp_node::now_epoch_ms())
            .await;
        (Some(cache), Some(configs))
    } else {
        (None, None)
    };


    // The portable node: serves every route already migrated out of this
    // shell (dispatched via the axum fallback in `create_app`).
    // Env vars surfaced verbatim in `/info` (the core never reads the
    // environment — the shell collects these once here).
    let info_env: Vec<(String, String)> = [
        "SPKY_DB_URL", "SPKY_DB_NS", "SPKY_DB_NAME", "SPKY_DB_USER",
        "SPKY_SCHEDULER_URL", "SPKY_SSP_LISTEN_ADDR", "SPKY_SSP_ADVERTISE_ADDR", "SPKY_SSP_ID",
        "HEARTBEAT_INTERVAL_MS", "TTL_CLEANUP_INTERVAL_SECS",
    ]
    .iter()
    .filter_map(|&key| std::env::var(key).ok().map(|val| (key.to_string(), val)))
    .collect();
    // Derive IP from SPKY_SSP_ADVERTISE_ADDR (e.g. "10.100.1.30:8667" -> "10.100.1.30").
    let advertise_ip = config
        .advertise_addr
        .as_deref()
        .and_then(|addr| addr.split(':').next().map(|s| s.to_string()));

    let node_backend_health: Option<Arc<dyn ssp_node::ports::BackendHealth>> =
        match (backend_health.clone(), shared_backend_configs.clone()) {
            (Some(cache), Some(configs)) => Some(Arc::new(
                adapters::MaintenanceBackendHealth { cache, configs },
            )),
            _ => None,
        };

    // Shared between the still-shell ingest handler (via AppState) and the
    // migrated register/unregister/crdt handlers (via SspNode) — one instance.
    let view_metrics = Arc::new(RwLock::new(std::collections::HashMap::new()));

    let node = Arc::new(ssp_node::SspNode {
        platform: platform.clone(),
        status: status.clone(),
        processor: processor_arc.clone(),
        publication_gate: publication_gate.clone(),
        job_config: job_config.clone(),
        job_control: job_control.clone(),
        job_dispatcher: Arc::clone(&job_dispatcher),
        ssp_id: config.ssp_id.clone(),
        auth_secret: config.auth_secret.clone(),
        ref_mode: config.ref_mode,
        merge_views: config.merge_views,
        version: env!("CARGO_PKG_VERSION"),
        surrealdb_version: surrealdb_version.clone(),
        advertise_ip,
        info_env,
        start_epoch_ms: ssp_node::now_epoch_ms(),
        bootstrap_warnings: Arc::new(RwLock::new(Vec::new())),
        backend_health: node_backend_health,
        crdt_cache: crdt_cache.clone(),
        view_metrics: view_metrics.clone(),
        edge_update_tx: edge_update_tx.clone(),
        anonymous_live_queries: config.anonymous_live_queries,
        impersonation: config.impersonation,
        query_allowlist: Arc::new(ssp_node::allowlist_state::QueryAllowlist::new(config.query_allowlist)),
        standalone: config.scheduler_url.is_none(),
        schedule_engine,
        ttl_cleanup_interval_secs: config.ttl_cleanup_interval_secs,
        view_metrics_flush_ms: config.view_metrics_flush_ms,
        bootstrap_page_size: config.bootstrap_page_size,
        checkpoint_interval_secs: config.checkpoint_interval_secs,
        max_snapshot_age_secs: config.max_snapshot_age_secs,
        last_heartbeat_seen: std::sync::Arc::new(std::sync::Mutex::new(None)),
    });
    let runtime = ssp_node::Runtime::new(node.clone());

    // Standalone changefeed tail: with no scheduler in front, this node reads
    // `SHOW CHANGES` itself when the schema was deployed with
    // `sync.transport: changefeed`. In cluster mode the scheduler tails.
    if standalone {
        // Its own session, for the same reason the scheduler's tail has one:
        // a `SHOW CHANGES` the server does not answer must not park the
        // node's control path behind it.
        let tail_db = connect_database(&config).await?;
        maintenance::db::spawn_periodic_resignin(Arc::clone(&tail_db), maintenance::db::RESIGNIN_INTERVAL_SECS);
        let _changefeed_stats = changefeed::spawn_if_configured(
            &config,
            tail_db,
            node.clone(),
            processor_arc.clone(),
            status.clone(),
        );
    }

    // Timer dispatcher: the VM shell's `on_timer` equivalent. Portable arms
    // (JobRecoverySweep/TtlCleanup/CircuitCheckpoint) run + re-arm in the core
    // via `runtime.on_timer` — the exact shape a DO shell drives from alarm().
    // Host-specific arms (BackendHealth/DbResignin — non-wasm `maintenance` +
    // reqwest) stay inline here.
    {
        let runtime = runtime.clone();
        let db = db.clone();
        let scheduler = Arc::clone(&platform.scheduler);
        let backend_cache = backend_health.clone();
        let backend_configs = shared_backend_configs.clone();
        let health_client = maintenance::backend_health::health_http_client();
        let health_interval_ms = config.health_check_interval_secs * 1000;
        tokio::spawn(async move {
            while let Some(kind) = timer_rx.recv().await {
                match &kind {
                    ssp_node::TimerKind::BackendHealth => {
                        if let (Some(configs), Some(cache)) = (&backend_configs, &backend_cache) {
                            maintenance::backend_health::check_backends_once(configs, cache, &health_client).await;
                            scheduler
                                .schedule(kind.clone(), ssp_node::now_epoch_ms() + health_interval_ms)
                                .await;
                        }
                    }
                    ssp_node::TimerKind::DbResignin => {
                        maintenance::db::resignin_once(&db).await;
                        scheduler
                            .schedule(
                                kind.clone(),
                                ssp_node::now_epoch_ms() + maintenance::db::RESIGNIN_INTERVAL_SECS * 1000,
                            )
                            .await;
                    }
                    _ => runtime.on_timer(kind).await,
                }
            }
        });
    }

    let state = AppState {
        db: db.clone(),
        processor: processor_arc.clone(),
        status: status.clone(),
        metrics: metrics.clone(),
        job_config,
        job_dispatcher,
        job_control,
        ssp_id: config.ssp_id.clone(),
        scheduler_url: config.scheduler_url.clone(),
        start_time: std::time::Instant::now(),
        crdt_cache,
        view_metrics,
        ref_mode: config.ref_mode,
        anonymous_live_queries: config.anonymous_live_queries,
        surrealdb_version,
        edge_update_tx,
        backend_health,
        shared_backend_configs,
        platform: platform.clone(),
        auth_secret: config.auth_secret.clone(),
        node: Arc::clone(&node),
    };

    let publication_for_heartbeat = state.edge_update_tx.clone();
    let mut app = create_app(state);

    // `/logs` is the one route that cannot live in the portable core: it is an
    // open-ended SSE stream, and `ssp_node::ApiBody` models only fully buffered
    // JSON/text responses. Giving the core a streaming body type would touch
    // every host (including the CF Worker shell) for the benefit of this single
    // endpoint, so the axum shell serves it directly — with the same bearer
    // auth the core applies to its own non-public routes.
    app = app.merge(
        Router::new()
            .route("/logs", axum::routing::get(logs_stream_handler))
            .layer(middleware::from_fn_with_state(
                config.auth_secret.clone(),
                auth_middleware,
            ))
            .with_state(Arc::clone(&log_ring)),
    );

    if standalone {
        // Backup/restore plane (same routes as the scheduler's port 9667,
        // but behind this SSP's bearer auth — spooky-cloud must send
        // `Authorization: Bearer $SPKY_AUTH_SECRET` when targeting an SSP).
        let host: Arc<dyn maintenance::MaintenanceHost> =
            Arc::new(maintenance_host::SspHost {
                publisher: publication_for_heartbeat.clone(),
                publication_gate: publication_gate.clone(),
                db: db.clone(),
                processor: processor_arc.clone(),
                status: status.clone(),
                platform: platform.clone(),
                ref_mode: config.ref_mode,
                bootstrap_page_size: config.bootstrap_page_size,
            });
        let db_config = Arc::new(maintenance::DbConfig {
            url: config.db_addr.clone(),
            namespace: config.db_ns.clone(),
            database: config.db_db.clone(),
            username: config.db_user.clone(),
            password: config.db_pass.clone(),
        });
        let backup_config = Arc::new(maintenance::BackupConfig::from_env());
        let backup_registry = Arc::new(maintenance::BackupRegistry::new());
        let restore_registry = Arc::new(maintenance::RestoreRegistry::new());
        let (backup_tx, backup_rx) = maintenance::create_backup_channel();
        let (restore_tx, restore_rx) = maintenance::create_restore_channel();
        let backup_restore_lock = Arc::new(tokio::sync::Mutex::new(()));

        tokio::spawn(maintenance::run_backup_worker(
            backup_rx,
            Arc::clone(&host),
            Arc::clone(&backup_config),
            Arc::clone(&db_config),
            Arc::clone(&backup_registry),
            Arc::clone(&backup_restore_lock),
        ));
        tokio::spawn(maintenance::run_restore_worker(
            restore_rx,
            Arc::clone(&host),
            Arc::clone(&backup_config),
            Arc::clone(&db_config),
            Arc::clone(&restore_registry),
            Arc::clone(&backup_restore_lock),
        ));

        let backup_router = maintenance::create_backup_router(maintenance::BackupState {
            host,
            config: backup_config,
            registry: backup_registry,
            tx: backup_tx,
            restore_registry,
            restore_tx,
            backup_restore_lock,
        })
        .layer(middleware::from_fn_with_state(
            config.auth_secret.clone(),
            auth_middleware,
        ));
        app = app.merge(backup_router);
        info!("Standalone maintenance plane active: /backup/*, /backends, backend health monitor");
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr)
        .await
        .context("Failed to bind port")?;

    info!(addr = %config.listen_addr, "Listening for requests");

    // Spawn self-bootstrap task (runs while server is already accepting /health requests)
    {
        let db = db.clone();
        let processor = processor_arc.clone();
        let status = status.clone();
        let metrics = metrics.clone();
        let scheduler_url = config.scheduler_url.clone();
        let ssp_id = config.ssp_id.clone();
        let listen_addr = config.listen_addr.clone();
        let advertise_addr = config.advertise_addr.clone();
        let register_max_wait_secs = config.register_max_wait_secs;
        let bootstrap_page_size = config.bootstrap_page_size;
        let bootstrap_warnings = Arc::clone(&node.bootstrap_warnings);
        let bootstrap_node = Arc::clone(&node);

        tokio::spawn(async move {
            // Choose bootstrap source based on mode. The metadata source
            // (INFO FOR DB → permissions) is always upstream SurrealDB
            // regardless of mode, because the scheduler's replica is
            // records-only and won't carry DEFINE TABLE strings.
            let metadata_source = BootstrapSource::Direct(db.clone());
            // Only cluster mode has a scheduler to report progress to; in
            // standalone there is nobody listening and this stays `None`.
            let mut reporter: Option<BootstrapReporter> = None;
            let (data_source, mut expected_hashes) = if let Some(ref scheduler_url) = scheduler_url {
                // Cluster mode: register with scheduler, then bootstrap from proxy
                let client = reqwest::Client::new();
                let scheduler_base = scheduler_url.trim_end_matches('/');
                reporter = Some(BootstrapReporter::new(
                    client.clone(),
                    scheduler_base,
                    ssp_id.clone(),
                ));

                info!("Registering SSP {} with scheduler at {}", ssp_id, scheduler_base);

                let registration = register_with_retry(
                    &client,
                    scheduler_url,
                    &ssp_id,
                    &listen_addr,
                    advertise_addr.as_deref(),
                    register_max_wait_secs,
                    &status,
                )
                .await;

                let proxy_url = format!("{}/proxy", scheduler_base);
                info!("Bootstrapping from scheduler proxy at {}", proxy_url);
                (
                    BootstrapSource::Proxy { client, proxy_url },
                    registration.table_hashes,
                )
            } else {
                // Standalone mode: bootstrap directly from DB. No expected
                // hashes — verification only applies in cluster mode.
                info!("Standalone mode: bootstrapping from SurrealDB");
                (BootstrapSource::Direct(db), BTreeMap::new())
            };

            // Retry bootstrap up to 10 times with backoff (tables may not exist yet
            // if migrations haven't run). `integrity_attempt` is counted
            // separately: a transient bootstrap error must not consume the
            // integrity gate's one retry (it used to, sending an SSP straight
            // to exit(2) on its first-ever hash mismatch).
            let mut attempt = 0;
            let mut integrity_attempt = 0;
            loop {
                attempt += 1;
                match self_bootstrap_with_metadata(
                    &metadata_source,
                    &data_source,
                    &processor,
                    bootstrap_page_size,
                    reporter.as_ref(),
                )
                .await
                {
                    Ok(bootstrap_warnings_now) => {
                        *bootstrap_warnings.write().await = bootstrap_warnings_now;
                        // Seed the catch-up XOR accumulators from the freshly
                        // bulk-loaded rows (`Circuit::load` bypasses the per-row
                        // `apply_mutation` maintenance). Must run before the SSP
                        // goes Ready, i.e. before any replay events are ingested,
                        // so the accumulator starts from the snapshot content.
                        {
                            let mut guard = processor.write().await;
                            guard.reseed_catchup_hashes();
                        }
                        // Integrity check: only when the scheduler handed us
                        // expected hashes (cluster mode). Mismatch ⇒ wipe
                        // the circuit and retry once. Second failure exits
                        // the process; supervisor restarts → fresh
                        // registration with the current frozen snapshot.
                        if !expected_hashes.is_empty() {
                            // The XOR set-hash the reseed above just settled:
                            // the same family the scheduler now maintains per
                            // event, and O(tables) to read instead of a full
                            // sorted digest over every row.
                            let actual = {
                                let guard = processor.read().await;
                                guard.compute_catchup_hashes()
                            };
                            let mut diffs = ssp_protocol::snapshot_hash::diff_table_hashes(
                                &expected_hashes,
                                &actual,
                            );
                            // Synced `_00_*` meta tables (feature flags, app
                            // releases) are low-stakes announcement data — a
                            // hash mismatch there must degrade to a warning,
                            // never crash-loop the SSP and take sync down for
                            // every real table (observed live when
                            // _00_app_release first joined the snapshot).
                            diffs.retain(|d| {
                                let meta = ssp_protocol::SYNCED_META_TABLES
                                    .contains(&d.table.as_str());
                                if meta {
                                    warn!(
                                        table = %d.table,
                                        expected = %d.a,
                                        actual = %d.b,
                                        "Ignoring integrity mismatch on synced meta table"
                                    );
                                }
                                !meta
                            });
                            if !diffs.is_empty() {
                                integrity_attempt += 1;
                                for d in &diffs {
                                    error!(
                                        table = %d.table,
                                        expected = %d.a,
                                        actual = %d.b,
                                        "Bootstrap integrity mismatch"
                                    );
                                }

                                // Ask the scheduler for a second opinion FIRST.
                                // Our rows came from its replica over /proxy,
                                // so a disagreement is usually its cached hash
                                // map having drifted from its own content, not
                                // a bad circuit. It rehashes the disputed
                                // tables from content and tells us what it
                                // really holds. (Blindly re-registering, the
                                // old first move, just refetched the same
                                // cache — a stale entry then crash-looped the
                                // SSP forever.)
                                let verdict = match scheduler_url.as_deref() {
                                    Some(sched_url) => {
                                        verify_bootstrap_with_scheduler(sched_url, &ssp_id, &actual).await
                                    }
                                    None => None,
                                };

                                if let Some(v) = verdict {
                                    if v.diverging.is_empty() {
                                        info!(
                                            tables = diffs.len(),
                                            "Scheduler rehashed from replica content and agrees — its hash cache was stale, continuing"
                                        );
                                        expected_hashes = v.table_hashes;
                                    } else if v.admit {
                                        error!(
                                            diverging = ?v.diverging,
                                            "Scheduler admitted us despite a persistent divergence — going Ready to restore sync"
                                        );
                                        expected_hashes = v.table_hashes;
                                    } else {
                                        // Genuine divergence against freshly
                                        // hashed content. Wipe and retry once;
                                        // a re-clone on the scheduler side also
                                        // invalidates what we loaded.
                                        if integrity_attempt >= 2 || v.recloned {
                                            error!(
                                                attempts = integrity_attempt,
                                                diverging = ?v.diverging,
                                                recloned = v.recloned,
                                                "Integrity mismatch persisted against replica content — exiting for restart"
                                            );
                                            *status.write().await = SspStatus::Failed;
                                            std::process::exit(2);
                                        }
                                        warn!(
                                            attempt = integrity_attempt,
                                            diverging = ?v.diverging,
                                            "Integrity mismatch confirmed against content — reloading the circuit and retrying"
                                        );
                                        {
                                            let mut guard = processor.write().await;
                                            // Carry the merge policy across the wipe; `Circuit::new`
                                            // is a fresh object and holds no configuration.
                                            let merge_views = guard.merge_views();
                                            *guard = Circuit::new();
                                            guard.set_merge_views(merge_views);
                                            guard.set_monotonic_row_versions(true);
                                        }
                                        expected_hashes = v.table_hashes;
                                        continue;
                                    }
                                } else {
                                    // No verdict (standalone, or an older
                                    // scheduler without the endpoint): fall
                                    // back to the previous behaviour — wipe,
                                    // re-register for fresh hashes, retry once.
                                    if integrity_attempt >= 2 {
                                        error!(
                                            attempts = integrity_attempt,
                                            diffs = diffs.len(),
                                            "Integrity mismatch persisted after retry — exiting for restart"
                                        );
                                        *status.write().await = SspStatus::Failed;
                                        std::process::exit(2);
                                    }
                                    warn!(
                                        attempt = integrity_attempt,
                                        diffs = diffs.len(),
                                        "Integrity mismatch — re-registering to refetch scheduler hashes before retry"
                                    );
                                    {
                                        let mut guard = processor.write().await;
                                        // Carry the merge policy across the wipe; `Circuit::new`
                                        // is a fresh object and holds no configuration.
                                        let merge_views = guard.merge_views();
                                        *guard = Circuit::new();
                                        guard.set_merge_views(merge_views);
                                        guard.set_monotonic_row_versions(true);
                                    }
                                    if let Some(sched_url) = scheduler_url.as_deref() {
                                        let client = reqwest::Client::new();
                                        let registration = register_with_retry(
                                            &client,
                                            sched_url,
                                            &ssp_id,
                                            &listen_addr,
                                            advertise_addr.as_deref(),
                                            register_max_wait_secs,
                                            &status,
                                        )
                                        .await;
                                        expected_hashes = registration.table_hashes;
                                    }
                                    continue;
                                }
                            }
                        }

                        let guard = processor.read().await;
                        metrics.view_count.add(guard.view_count() as i64, &[]);
                        info!(
                            tables = guard.table_names().len(),
                            views = guard.view_count(),
                            verified = !expected_hashes.is_empty(),
                            "Bootstrap complete"
                        );
                        drop(guard);
                        if let Err(e) = bootstrap_node.republish_restored_views().await {
                            error!(error = %e, "Bootstrap membership repair failed");
                            *status.write().await = SspStatus::Failed;
                            break;
                        }
                        *status.write().await = SspStatus::Ready;
                        break;
                    }
                    Err(e) => {
                        if attempt >= 10 {
                            error!(error = %e, attempts = attempt, "Bootstrap failed after retries");
                            *status.write().await = SspStatus::Failed;
                            break;
                        }
                        warn!(error = %e, attempt = attempt, "Bootstrap failed, retrying in 5s...");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    // Spawn heartbeat loop if scheduler configured
    if let Some(scheduler_url) = &config.scheduler_url {
        let ssp_id = config.ssp_id.clone();
        let scheduler_url_clone = scheduler_url.clone();
        let heartbeat_interval = config.heartbeat_interval_ms;
        let processor_clone = processor_for_scheduler.clone();
        // listen_addr / advertise_addr aren't read here — the heartbeat
        // path no longer self-registers; if the scheduler 404s us, we
        // exit and the supervisor reruns the full register handshake.
        let status_for_heartbeat = status.clone();
        let circuit_store_for_heartbeat = Arc::clone(&platform.circuit_store);
        let clean_requested_for_heartbeat = Arc::clone(&clean_requested);

        tokio::spawn(async move {
            // Bounded request: the scheduler evicts an SSP after 30 s of
            // silence, so one heartbeat that hangs on a slow scheduler must
            // not be allowed to eat the next few ticks as well.
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new());
            let heartbeat_url = format!("{}/ssp/heartbeat", scheduler_url_clone.trim_end_matches('/'));
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(heartbeat_interval));
            let mut last_views: usize = 0;

            loop {
                interval.tick().await;

                // Don't heartbeat until the bootstrap task has finished
                // registering and verifying. Otherwise the first tick can
                // race ahead of `POST /ssp/register`, hit the scheduler as
                // an unknown SSP, get 404, and we'd exit while our own
                // registration is still in flight.
                if *status_for_heartbeat.read().await != SspStatus::Ready {
                    continue;
                }

                let views = heartbeat_view_count(&processor_clone, &mut last_views);

                let mut publication = publication_for_heartbeat.snapshot();
                let (generation, last_reconnect_duration_ms, reconnect_failures) = edge_connection_for_heartbeat.reconnect_metrics();
                publication.connection = Some(ssp_protocol::PublicationConnectionMetrics {
                    generation, last_reconnect_duration_ms, reconnect_failures,
                });
                let payload = ssp_protocol::SspHeartbeat {
                    ssp_id: ssp_id.clone(),
                    publication: Some(publication),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    views,
                    cpu_usage: None,
                    // Reported as a 0..1 fraction of the cgroup ceiling, not raw
                    // bytes: the scheduler's `LeastLoad` strategy sums this with
                    // `cpu_usage` to rank SSPs, so the two have to be on the same
                    // scale. It stayed `None` here for a long time, which made
                    // that sum a constant 0.0 for every SSP and silently
                    // degraded `LeastLoad` into "always pick the first one".
                    memory_usage: crate::metrics::memory_load_fraction(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                };

                match client.post(&heartbeat_url).json(&payload).send().await {
                    Ok(resp) if resp.status() == StatusCode::NOT_FOUND => {
                        warn!("Scheduler doesn't recognize us, exiting for clean restart");
                        // The scheduler has dropped us. Restarting through
                        // the supervisor re-runs the bootstrap loop above,
                        // which re-registers and re-verifies hashes — much
                        // safer than trying to re-register from a heartbeat
                        // task that can't replay events into the circuit.
                        std::process::exit(3);
                    }
                    Ok(resp) if resp.status() == StatusCode::CONFLICT => {
                        // Either buffer overflow or scheduler-driven
                        // integrity-check resync. Either way the circuit
                        // can't be trusted; exit so the supervisor brings
                        // us back with a clean state.
                        let body = resp.text().await.unwrap_or_default();
                        let directive = ssp_protocol::ResyncDirective::parse(&body);
                        if directive.clean {
                            // Drop the snapshot BEFORE exiting, and tell the
                            // shutdown path not to write a fresh one: the
                            // point of a clean restart is a cold rebuild, and
                            // a checkpoint on the way out would undo it.
                            clean_requested_for_heartbeat.store(true, Ordering::SeqCst);
                            wipe_local_state(circuit_store_for_heartbeat.as_ref()).await;
                        }
                        error!(reason = %directive.reason, clean = directive.clean, "Scheduler requested re-bootstrap, exiting");
                        std::process::exit(4);
                    }
                    Ok(resp) if !resp.status().is_success() => {
                        warn!("Heartbeat failed: HTTP {}", resp.status());
                    }
                    Ok(_) => {
                        debug!("Heartbeat sent successfully");
                    }
                    Err(e) => {
                        warn!("Failed to send heartbeat: {}", e);
                    }
                }
            }
        });
    } else {
        info!("No SPKY_SCHEDULER_URL configured, running in standalone mode");
    }

    // TTL cleanup: timer-driven (TimerKind::TtlCleanup), re-armed by the
    // dispatcher every ttl_cleanup_interval_secs.
    platform
        .scheduler
        .schedule(
            ssp_node::TimerKind::TtlCleanup,
            ssp_node::now_epoch_ms() + config.ttl_cleanup_interval_secs * 1000,
        )
        .await;
    info!(interval_secs = config.ttl_cleanup_interval_secs, "TTL cleanup timer armed");

    // Per-view metrics: noted in memory on ingest, flushed to `_00_query` here
    // (TimerKind::ViewMetricsFlush), re-armed by the dispatcher.
    platform
        .scheduler
        .schedule(
            ssp_node::TimerKind::ViewMetricsFlush,
            ssp_node::now_epoch_ms() + config.view_metrics_flush_ms,
        )
        .await;
    info!(interval_ms = config.view_metrics_flush_ms, "View metrics flush timer armed");

    // Circuit checkpoint: only where a later boot can restore it, which is the
    // standalone path with a snapshot dir (see `resolve_checkpoint_interval`).
    if let Some(secs) = config.checkpoint_interval_secs {
        platform
            .scheduler
            .schedule(
                ssp_node::TimerKind::CircuitCheckpoint,
                ssp_node::now_epoch_ms() + secs * 1000,
            )
            .await;
        info!(interval_secs = secs, "Circuit checkpoint timer armed");
    } else if std::env::var_os("SPKY_SSP_SNAPSHOT_DIR").is_some() {
        info!(
            standalone = config.scheduler_url.is_none(),
            "Circuit checkpoints disabled: a cluster node bootstraps from the scheduler proxy and never restores a checkpoint, so writing one would only stall ingest"
        );
    }

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(
            meter_provider,
            runtime.clone(),
            Arc::clone(&clean_requested),
        ))
        .await
        .context("Server error")?;

    opentelemetry::global::shutdown_tracer_provider();

    Ok(())
}

/// Delete the on-disk circuit state so the next boot is a cold rebuild.
///
/// The snapshot goes through the `CircuitStore` port; the arena is a
/// sparse-file cache of row bytes that bootstrap repopulates, so it is removed
/// wholesale. Both are caches of SurrealDB, never the source of truth, which
/// is what makes deleting them a safe thing to do from a heartbeat task.
async fn wipe_local_state(store: &dyn ssp_node::CircuitStore) {
    match store.clear().await {
        Ok(()) => info!("Circuit snapshot cleared for clean restart"),
        Err(e) => warn!(error = %e, "Could not clear circuit snapshot; restart will be warm"),
    }
    if let Some(dir) = std::env::var_os("SPKY_SSP_ARENA_DIR") {
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => info!(dir = %std::path::Path::new(&dir).display(), "Arena cleared for clean restart"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => warn!(error = %e, "Could not clear arena dir"),
        }
    }
}

async fn shutdown_signal(
    meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    runtime: ssp_node::Runtime,
    clean_requested: Arc<AtomicBool>,
) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Signal received, starting graceful shutdown");

    // Persist a final snapshot so an ephemeral host restarts warm. No-op on the
    // VM (NoopCircuitStore) and skipped unless the circuit is Ready. Skipped
    // outright after a clean-restart directive, which just deleted it.
    if clean_requested.load(Ordering::SeqCst) {
        info!("Skipping shutdown checkpoint: clean restart requested");
    } else {
        runtime.checkpoint().await;
    }

    if let Err(e) = meter_provider.shutdown() {
        error!(error = %e, "Failed to shutdown meter provider");
    }
}

// --- Self-Bootstrap ---
//
// The pure bootstrap helpers (`bootstrap_page_query`, `parse_link_target`,
// `extract_select_permission_text`) and the standalone rebuild now live in the
// portable core (`ssp_node::bootstrap`); the shell keeps only the cluster
// split-source path below.

#[cfg(test)]
mod checkpoint_gating_tests {
    use super::{heartbeat_view_count, resolve_checkpoint_interval};
    use ssp::circuit::Circuit;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[test]
    fn standalone_with_snapshot_dir_checkpoints_every_300s_by_default() {
        assert_eq!(resolve_checkpoint_interval(true, None, true), Some(300));
        assert_eq!(resolve_checkpoint_interval(true, Some("45"), true), Some(45));
    }

    #[test]
    fn no_snapshot_dir_means_no_checkpoint() {
        assert_eq!(resolve_checkpoint_interval(false, None, true), None);
        assert_eq!(resolve_checkpoint_interval(false, Some("45"), true), None);
    }

    #[test]
    fn cluster_node_never_checkpoints() {
        // The cluster bootstrap goes through the scheduler proxy and never
        // restores a checkpoint, so writing one is pure cost (709 MB every
        // ~6 min on whitepawn, under the circuit lock).
        assert_eq!(resolve_checkpoint_interval(true, None, false), None);
        assert_eq!(resolve_checkpoint_interval(true, Some("45"), false), None);
    }

    #[test]
    fn zero_interval_is_an_opt_out_not_a_tight_loop() {
        assert_eq!(resolve_checkpoint_interval(true, Some("0"), true), None);
        assert_eq!(resolve_checkpoint_interval(true, Some(" 0 "), true), None);
    }

    #[test]
    fn unparseable_interval_falls_back_to_default() {
        assert_eq!(resolve_checkpoint_interval(true, Some("soon"), true), Some(300));
    }

    #[test]
    fn a_not_ready_scheduler_does_not_spend_the_registration_exit_budget() {
        use super::{registration_budget_restarts_on, RegisterError};
        // 503 = alive and cloning: keep waiting, however long it takes.
        assert!(registration_budget_restarts_on(&RegisterError::NotReady("HTTP 503".into())));
        // Unreachable or broken: bounded, then exit for the supervisor.
        assert!(!registration_budget_restarts_on(&RegisterError::Retryable("HTTP 502".into())));
        assert!(!registration_budget_restarts_on(&RegisterError::Retryable("connection refused".into())));
        assert!(!registration_budget_restarts_on(&RegisterError::Fatal("HTTP 400".into())));
    }

    #[tokio::test]
    async fn heartbeat_reports_last_count_while_circuit_is_write_locked() {
        let processor = Arc::new(RwLock::new(Circuit::new()));
        let mut last = 0usize;
        // Free lock: the real count is read and remembered.
        assert_eq!(heartbeat_view_count(&processor, &mut last), 0);

        // A checkpoint / registration holding the lock must not park the
        // heartbeat: the remembered count is reported instead.
        last = 7;
        let guard = processor.write().await;
        assert_eq!(heartbeat_view_count(&processor, &mut last), 7);
        drop(guard);

        assert_eq!(heartbeat_view_count(&processor, &mut last), 0);
    }
}

#[cfg(test)]
mod bootstrap_pagination_tests {
    use ssp_node::bootstrap::bootstrap_page_query;
    use std::collections::BTreeSet;

    #[test]
    fn page_query_uses_ordered_keyset_not_offset() {
        // Regression guard: the bootstrap scan must NOT use OFFSET/START (lossy
        // under concurrent writes) and must resume by id keyset, ordered by id.
        let none = BTreeSet::new();
        let first = bootstrap_page_query("game", 200, None, &none);
        assert_eq!(first, "SELECT * FROM game ORDER BY id LIMIT 200");

        let next = bootstrap_page_query("game", 200, Some("game:abc"), &none);
        assert_eq!(
            next,
            "SELECT * FROM game WHERE id > type::record('game', 'abc') ORDER BY id LIMIT 200"
        );

        // Neither page may fall back to offset pagination.
        assert!(!first.contains("START") && !next.contains("START"));
        assert!(first.contains("ORDER BY id") && next.contains("ORDER BY id"));
    }

    #[test]
    fn page_query_omits_opaque_fields_on_both_pages() {
        // Both pages must carry the SAME projection: a first page that keeps a
        // field the resumed page drops would hash differently per page.
        let omit: BTreeSet<String> = ["blob", "secret_token"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            bootstrap_page_query("game", 200, None, &omit),
            "SELECT * OMIT blob, secret_token FROM game ORDER BY id LIMIT 200"
        );
        assert_eq!(
            bootstrap_page_query("game", 200, Some("game:abc"), &omit),
            "SELECT * OMIT blob, secret_token FROM game WHERE id > type::record('game', 'abc') ORDER BY id LIMIT 200"
        );
    }

    // Walk a paginated scan over a table of `total` rows (ids 0..total) while a
    // concurrent delete removes the first `deleted` ids right after the first
    // page — the exact race the SSP hits paging a live table during (re)bootstrap.
    // `keyset` switches between resuming by `id > last` (the fix) and a fixed
    // `START`/offset (the bug). Returns how many of the SURVIVING rows were seen.
    fn paginate_with_concurrent_delete(
        total: usize,
        page: usize,
        deleted: usize,
        keyset: bool,
    ) -> (usize, usize) {
        // The live table, ordered by id. A delete just removes ids from this set.
        let mut table: BTreeSet<usize> = (0..total).collect();
        let mut seen: BTreeSet<usize> = BTreeSet::new();
        let mut start = 0usize;
        let mut last: Option<usize> = None;
        let mut first_page = true;
        loop {
            // What an `ORDER BY id` scan returns for this page right now.
            let ordered: Vec<usize> = table.iter().copied().collect();
            let pagerows: Vec<usize> = if keyset {
                ordered
                    .into_iter()
                    .filter(|&id| last.map_or(true, |l| id > l))
                    .take(page)
                    .collect()
            } else {
                ordered.into_iter().skip(start).take(page).collect()
            };
            let n = pagerows.len();
            if n == 0 {
                break;
            }
            for id in &pagerows {
                seen.insert(*id);
            }
            last = pagerows.last().copied();
            if first_page {
                // Concurrent delete lands between page 1 and page 2.
                for id in 0..deleted {
                    table.remove(&id);
                }
                first_page = false;
            }
            if n < page {
                break;
            }
            start += page;
        }
        let survivors = total - deleted;
        let seen_survivors = seen.iter().filter(|&&id| id >= deleted).count();
        (survivors, seen_survivors)
    }

    #[test]
    fn keyset_survives_concurrent_delete_offset_loses_rows() {
        // Offset pagination drops surviving rows when a delete shifts the scan...
        let (survivors, seen) = paginate_with_concurrent_delete(500, 100, 30, false);
        assert!(
            seen < survivors,
            "offset pagination should drop survivors under a concurrent delete (saw {seen}/{survivors})"
        );
        // ...keyset pagination sees every surviving row — this is the fix.
        let (survivors, seen) = paginate_with_concurrent_delete(500, 100, 30, true);
        assert_eq!(
            seen, survivors,
            "keyset pagination must load every surviving row ({seen}/{survivors})"
        );
    }
}

/// Bootstrap the circuit by loading all table data and view definitions.
/// Works with either a direct SurrealDB connection or the scheduler's HTTP proxy.
/// Bootstrap with separate sources for metadata (INFO FOR DB) and data
/// (record SELECTs). In cluster mode the scheduler's proxy backs onto its
/// RocksDB replica, which only stores records — DEFINE TABLE / PERMISSIONS
/// strings never land there, so `INFO FOR DB` against the proxy returns
/// at most an empty or schemaless view of each table. That breaks the
/// SSP's policy bootstrap (every table comes back without a registered
/// permission and gets default-denied at view-register time). Upstream
/// SurrealDB is the source of truth for schema, so callers in cluster
/// mode pass it explicitly here as `metadata_source`.
/// Returns the bootstrap warnings to surface via `/info` (empty on a clean
/// bootstrap). See `SspNode::bootstrap_warnings`.
async fn self_bootstrap_with_metadata(
    metadata_source: &BootstrapSource,
    data_source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    page_size: usize,
    reporter: Option<&BootstrapReporter>,
) -> anyhow::Result<Vec<String>> {
    info!("Starting self-bootstrap");

    // Standalone (metadata + data both Direct against the local DB): the
    // canonical rebuild lives in the portable core, over the `Db` port — one
    // implementation, exercised by the reference host + ssp-node tests. The
    // cluster split-source path (metadata Direct, data via the scheduler
    // Proxy) can't use a single-DB port, so it stays inline below.
    if let (BootstrapSource::Direct(_), BootstrapSource::Direct(db)) =
        (metadata_source, data_source)
    {
        return ssp_node::bootstrap::rebuild_from_db(
            &crate::adapters::SurrealSdkDb::new(db.clone()),
            processor,
            page_size,
        )
        .await
        .map(|()| Vec::new());
    }
    let mut warnings: Vec<String> = Vec::new();

    use ssp_node::bootstrap::{
        bootstrap_page_query, extract_select_permission_text, parse_link_target,
    };
    let source = data_source;

    // Step 1: Discover tables via INFO FOR DB (against upstream so we get
    // the real DEFINE TABLE strings with PERMISSIONS clauses).
    let info_json = metadata_source.query("INFO FOR DB").await
        .context("Failed to query INFO FOR DB")?;

    // INFO FOR DB returns `tables: { name: "DEFINE TABLE ... PERMISSIONS ...;" }`.
    // Keep both the table list (for data-loading) and the raw DEFINE strings
    // (for permission extraction below).
    // Tables marked `-- @nosync` carry a `COMMENT 'sp00ky:nosync'` marker in
    // their `DEFINE TABLE` string. Skip them entirely: no permission is
    // registered and no data is loaded into the circuit, so the table never
    // participates in sync. It stays in the upstream DB (still backed up).
    let table_defs: Vec<(String, String)> = match info_json.get("tables") {
        Some(Value::Object(tables_map)) => tables_map
            .iter()
            .filter(|(name, _)| !ssp_protocol::table_excluded_from_sync(name))
            .filter(|(name, def)| {
                let nosync = def
                    .as_str()
                    .map(ssp_protocol::define_str_is_nosync)
                    .unwrap_or(false);
                if nosync {
                    info!(table = %name, "Excluding @nosync table from bootstrap");
                }
                !nosync
            })
            .map(|(name, def)| {
                (name.clone(), def.as_str().unwrap_or("").to_string())
            })
            .collect(),
        _ => {
            info!("No tables found in database");
            vec![]
        }
    };
    let tables: Vec<String> = table_defs.iter().map(|(n, _)| n.clone()).collect();

    info!(count = tables.len(), "Discovered tables: {:?}", tables);
    if let Some(r) = reporter {
        r.set_total(tables.len()).await;
    }

    // Step 1b: Pull `PERMISSIONS FOR select WHERE <expr>` text out of each
    // DEFINE TABLE string and stash it on the circuit. Stored as raw text so
    // `prepare_registration_dbsp` can route it through the same converter that
    // handles user queries (see permission_inject.rs).
    {
        let mut circuit = processor.write().await;
        for (name, def) in &table_defs {
            let permission = extract_select_permission_text(def);
            info!(
                target: "ssp::policy",
                table = %name,
                permission = %permission,
                "registered table permission"
            );
            circuit.set_permission(name, permission);
        }
    }

    // Step 1c: Per-table record-link map. `INFO FOR TABLE` exposes each field's
    // `DEFINE FIELD ... TYPE record<X>`; capture `field -> X` so the
    // registration pipeline can lower a link-traversal permission
    // (`assigned_to.owner.id = $auth.id`) into a SemiJoin against `X`. The
    // target table isn't derivable from the field name (`assigned_to` links to
    // `connection`), and a flat Filter can't dereference a link across rows, so
    // without this every live query on an outbox table matches zero rows.
    // Best-effort: a failed INFO or an unparseable field just leaves that link
    // unresolved (its permission stays flat, i.e. today's behavior).
    // The same pass collects each table's opaque-field set (fields marked
    // `@nosync`/`@crdt`/`@opaque` on a DEFINE FIELD, which the CLI stamps with
    // `COMMENT 'sp00ky:opaque'`). Read from `metadata_source` — always the
    // upstream DB, which is the only side that carries the DDL — and applied as
    // an `OMIT` to the row scan below. On this path `source` is the scheduler
    // proxy, whose replica should already lack these fields; the OMIT still
    // matters because a replica cloned before this change does hold them, and
    // loading them here would put the circuit permanently out of step with the
    // ingest payload's key set.
    let mut opaque_by_table: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut columns_by_table: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    {
        let mut resolved: Vec<(String, String, String)> = Vec::new();
        for table in &tables {
            let info = match metadata_source.query(&format!("INFO FOR TABLE {}", table)).await {
                Ok(v) => v,
                Err(e) => {
                    warn!(target: "ssp::policy", table = %table, error = %e, "INFO FOR TABLE failed; skipping link map");
                    continue;
                }
            };
            let opaque = ssp_protocol::opaque_fields_from_info(&info);
            if !opaque.is_empty() {
                info!(
                    target: "ssp::policy",
                    table = %table, fields = ?opaque,
                    "omitting opaque fields from bootstrap scan"
                );
                opaque_by_table.insert(table.clone(), opaque);
            }
            columns_by_table.insert(table.clone(), ssp_protocol::columns_from_info(&info));
            let Some(fields) = info.get("fields").and_then(|f| f.as_object()) else {
                continue;
            };
            for (field_name, def) in fields {
                if let Some(target) = def.as_str().and_then(parse_link_target) {
                    resolved.push((table.clone(), field_name.clone(), target));
                }
            }
        }
        {
            let mut circuit = processor.write().await;
            for (table, field, target) in &resolved {
                info!(
                    target: "ssp::policy",
                    table = %table, field = %field, link_target = %target,
                    "registered record-link target"
                );
                circuit.set_link_target(table.clone(), field.clone(), target.clone());
            }
            // Also give the circuit the set, so a registration that tries to
            // filter/order on one of these fields is rejected rather than
            // silently matching nothing.
            for (table, fields) in &opaque_by_table {
                circuit.set_opaque_fields(table.clone(), fields.clone());
            }
            for (table, cols) in &columns_by_table {
                circuit.set_columns(table.clone(), cols.clone());
            }
        }
    }

    // Step 2: Load all table data, paged. Pulling the entire table in one
    // request blew up at multi-GB DBs because the SurrealDB engine or the
    // scheduler proxy had to materialise the full result set in one HTTP
    // body. Paging keeps each round-trip bounded; the SSP still loads
    // everything into the circuit store but does so a chunk at a time.
    // `page_size` comes from NodeConfig (env: SPKY_SSP_BOOTSTRAP_PAGE_SIZE).
    let no_omit = BTreeSet::new();
    for table in &tables {
        if let Some(r) = reporter {
            r.start_table(table).await;
        }
        let omit = opaque_by_table.get(table).unwrap_or(&no_omit);
        let mut record_count: usize = 0;
        // Keyset cursor: the highest `id` loaded so far. `None` = first page.
        let mut after_id: Option<String> = None;
        loop {
            let result = source
                .query(&bootstrap_page_query(table, page_size, after_id.as_deref(), omit))
                .await
                .with_context(|| format!("Failed to page-query table {}", table))?;

            let rows: Vec<Value> = match result {
                Value::Array(arr) => arr,
                _ => vec![],
            };
            let n = rows.len();
            if n == 0 {
                break;
            }

            // Advance the cursor to the last row's id BEFORE consuming `rows`
            // (the page is ORDER BY id, so the last row carries the max id).
            let next_after = rows
                .last()
                .and_then(|row| row.get("id"))
                .and_then(|v| v.as_str())
                .map(str::to_string);

            let records: Vec<Record> = rows
                .into_iter()
                .filter_map(|row| {
                    let id = row.get("id")?.as_str()?.to_string();
                    Some(Record::new(table, &id, row))
                })
                .collect();

            {
                let mut circuit = processor.write().await;
                circuit.load(records);
            }

            record_count += n;
            if let Some(r) = reporter {
                r.add_rows(n).await;
            }
            if n < page_size {
                break;
            }
            // No usable id to resume from → stop rather than loop forever.
            match next_after {
                Some(id) => after_id = Some(id),
                None => break,
            }
        }
        info!(table = %table, records = record_count, "Loaded table data");
        if let Some(r) = reporter {
            r.finish_table().await;
        }

        // The data source is the scheduler's replica, which is fed by the
        // per-row ingest events and can be missing everything written while
        // nothing was listening. Zero rows from it for a table upstream has
        // rows in is exactly that gap, and every view on the table computes
        // empty until the scheduler re-clones. Say so, loudly and in `/info`;
        // the count is best-effort and must never fail or slow the bootstrap.
        if record_count == 0 {
            if let Some(upstream) = upstream_row_count(metadata_source, table).await {
                if upstream > 0 {
                    let msg = format!(
                        "table `{table}` loaded 0 rows from the bootstrap source but upstream has {upstream}; \
                         views on it stay empty until the scheduler replica is re-cloned"
                    );
                    error!(table = %table, upstream_rows = upstream, "{}", msg);
                    warnings.push(msg);
                }
            }
        }
    }

    // Step 3: Re-register views from the global `_00_query` table.
    // `_00_query` is global in both modes (only `_00_list_ref` splits
    // per user); each row carries an `auth_id` field which decides
    // where the corresponding `_00_list_ref_user_<id>` writes go.
    let result = source.query("SELECT * FROM _00_query").await
        .context("Failed to query _00_query")?;
    let views: Vec<Value> = match result {
        Value::Array(arr) => arr,
        _ => vec![],
    };
    info!(count = views.len(), "Found persisted views in _00_query");

    for view_row in views {
        let view_id = match view_row.get("id") {
            Some(Value::String(s)) => s.clone(),
            Some(v) => v.to_string().trim_matches('"').to_string(),
            None => {
                warn!("Skipping view with missing id");
                continue;
            }
        };

        // Strip the table prefix (e.g. "_00_query:abc" → "abc").
        let raw_id = view_id
            .strip_prefix("_00_query:")
            .unwrap_or(&view_id)
            .to_string();

        let surql = match view_row.get("surql").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => {
                warn!(view_id = %raw_id, "Skipping view with missing surql");
                continue;
            }
        };

        let client_id = view_row
            .get("clientId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let auth_id = view_row
            .get("auth_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let ttl = view_row
            .get("ttl")
            .and_then(|v| v.as_str())
            .unwrap_or("30m")
            .to_string();
        let last_active_at = view_row
            .get("lastActiveAt")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let params = view_row
            .get("params")
            .cloned()
            .unwrap_or(json!({}));

        let payload = json!({
            "id": raw_id,
            "surql": surql,
            "clientId": client_id,
            "authId": auth_id,
            "ttl": ttl,
            "lastActiveAt": last_active_at,
            "params": params,
        });

        let prep = {
            let circuit = processor.read().await;
            ssp::service::view::prepare_registration_dbsp(
                payload,
                circuit.permissions(),
                circuit.link_targets(),
                circuit.opaque_fields(),
            )
        };
        match prep {
            Ok(data) => {
                let mut circuit = processor.write().await;
                circuit.add_query_with_auth(
                    data.plan,
                    data.safe_params,
                    Some(OutputFormat::Streaming),
                    auth_id.clone(),
                );
                info!(view_id = %raw_id, auth_id = %auth_id, "Re-registered view");
            }
            Err(e) => {
                warn!(
                    target: "ssp::policy",
                    view_id = %raw_id,
                    error = %e,
                    "Failed to re-register view; the owner client will get the same error on next register"
                );
            }
        }
    }

    Ok(warnings)
}

/// Best-effort upstream row count for the bootstrap sanity check. `None` on
/// any error or when the count takes too long: the check is advisory and the
/// bootstrap budget is not to be spent on it.
async fn upstream_row_count(source: &BootstrapSource, table: &str) -> Option<u64> {
    let query = format!("SELECT count() AS total FROM {} GROUP ALL", table);
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), source.query(&query)).await;
    match result {
        Ok(Ok(Value::Array(rows))) => rows
            .first()
            .and_then(|r| r.get("total"))
            .and_then(|v| v.as_u64()),
        Ok(Ok(_)) => None,
        Ok(Err(e)) => {
            warn!(table = %table, error = %e, "upstream row count for bootstrap check failed");
            None
        }
        Err(_) => {
            warn!(table = %table, "upstream row count for bootstrap check timed out");
            None
        }
    }
}

/// `GET /logs` — recent history then a live tail, as SSE.
///
/// Mirrors the scheduler's `/admin/api/logs` exactly, because the scheduler
/// relays this response through to the dashboard byte for byte. Behind bearer
/// auth: the scheduler attaches `SPKY_AUTH_SECRET` like it does for every other
/// SSP call.
async fn logs_stream_handler(
    State(ring): State<Arc<maintenance::log_ring::LogRing>>,
    axum::extract::Query(q): axum::extract::Query<LogsQuery>,
) -> impl IntoResponse {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures::stream::{self, StreamExt};
    use tokio::sync::broadcast::error::RecvError;

    let backfill = q.backfill.unwrap_or(500).min(10_000);
    let tail = q.tail.unwrap_or(true);

    fn line_event(line: &maintenance::log_ring::LogLine) -> axum::response::sse::Event {
        axum::response::sse::Event::default()
            .event("line")
            .json_data(line)
            .unwrap_or_else(|_| axum::response::sse::Event::default().comment("unserialisable line"))
    }

    let history = ring.snapshot(backfill);
    let rx = ring.subscribe();
    let backlog = stream::iter(
        history
            .into_iter()
            .map(|l| Ok::<_, std::convert::Infallible>(line_event(&l))),
    );

    let keep_alive = KeepAlive::new().interval(std::time::Duration::from_secs(15));

    if !tail {
        return Sse::new(backlog.boxed()).keep_alive(keep_alive);
    }

    let live = stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(line) => Some((Ok::<_, std::convert::Infallible>(line_event(&line)), rx)),
            // Report the gap rather than hiding it; the dashboard renders this
            // as an explicit "N lines dropped" marker.
            Err(RecvError::Lagged(n)) => Some((
                Ok(Event::default().event("dropped").data(n.to_string())),
                rx,
            )),
            Err(RecvError::Closed) => None,
        }
    });

    Sse::new(backlog.chain(live).boxed()).keep_alive(keep_alive)
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    #[serde(default)]
    tail: Option<bool>,
    #[serde(default)]
    backfill: Option<usize>,
}

// --- Middleware ---

/// Bearer auth against the config-carried secret (`NodeConfig.auth_secret`) —
/// no per-request env read; the CF shell provides the same secret from a
/// Worker binding.
async fn auth_middleware(
    State(secret): State<String>,
    req: Request,
    next: Next,
) -> Response {
    let auth_header = req.headers().get(AUTHORIZATION);

    match auth_header {
        Some(header) if header.to_str().unwrap_or_default() == format!("Bearer {}", secret) => {
            next.run(req).await
        }
        _ => StatusCode::UNAUTHORIZED.into_response(),
    }
}

// --- Job recovery sweep (singlenode) ---
//
// Job pickup is CREATE-only: the runner enqueues a job when it observes the
// CREATE live-query event. A row that was created while this SSP was down (or
// before its live query was established) is never enqueued, and a 'processing'
// row whose SSP crashed mid-flight is never finished — both sit stuck forever.
// This sweep periodically (and once at startup) re-enqueues them. Every enqueue
// goes through `JobControl::mark_enqueued`, so it can never double-execute a job
// the live CREATE path already queued.

/// How often the recovery sweep runs.
const JOB_RECOVERY_INTERVAL_SECS: u64 = 60;
// --- TTL Cleanup ---

/// Thin shell wrapper over `ssp_node::ttl_cleanup_sweep` (the single impl),
/// adapting the VM's concrete `SharedDb` + OTel `Metrics` to the `Db` +
/// `Telemetry` ports. Kept for the DB integration tests that call it directly.
pub async fn ttl_cleanup_sweep(
    db: &SharedDb,
    processor: &Arc<RwLock<Circuit>>,
    metrics: &Arc<Metrics>,
    mode: ssp_protocol::RefMode,
) -> usize {
    ssp_node::ttl_cleanup_sweep(
        &crate::adapters::SurrealSdkDb::new(db.clone()),
        processor,
        &crate::adapters::OtelTelemetry::new(metrics.clone()),
        mode,
    )
    .await
}

// --- Helper Functions ---




