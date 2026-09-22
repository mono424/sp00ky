use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// A backend service to health-check periodically.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackendHealthConfig {
    pub name: String,
    pub url: String,
    pub healthcheck: String,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub env: Option<Vec<String>>,
}

/// Parse backend health targets from the environment. `SPKY_BACKENDS` is the
/// canonical variable; `SPKY_SCHEDULER_BACKENDS` is accepted as a fallback for
/// existing deployments.
pub fn backends_from_env() -> Vec<BackendHealthConfig> {
    let json = std::env::var("SPKY_BACKENDS")
        .or_else(|_| std::env::var("SPKY_SCHEDULER_BACKENDS"));
    match json {
        Ok(json) => serde_json::from_str(&json).unwrap_or_default(),
        Err(_) => vec![],
    }
}

/// Shared backend configs that can be updated at runtime (e.g. via PUT /backends).
pub type SharedBackendConfigs = Arc<RwLock<Vec<BackendHealthConfig>>>;

pub fn create_shared_configs(backends: &[BackendHealthConfig]) -> SharedBackendConfigs {
    Arc::new(RwLock::new(backends.to_vec()))
}

/// Replace the backend configs and reconcile the health cache.
/// Existing backends keep their health status; new ones start as Unknown.
pub async fn update_backends(
    configs: &SharedBackendConfigs,
    cache: &BackendHealthCache,
    new_backends: Vec<BackendHealthConfig>,
) {
    // Update shared configs first (same lock order as the health monitor)
    *configs.write().await = new_backends.clone();

    // Reconcile cache: keep health status for unchanged backends
    let mut entries = cache.write().await;
    let old_entries: Vec<BackendHealthEntry> = entries.drain(..).collect();
    for cfg in &new_backends {
        if let Some(existing) = old_entries.iter().find(|e| e.name == cfg.name && e.url == cfg.url) {
            let mut entry = existing.clone();
            entry.env = cfg.env.clone();
            entry.healthcheck = cfg.healthcheck.clone();
            entry.port = cfg.port;
            entries.push(entry);
        } else {
            entries.push(BackendHealthEntry::from_config(cfg));
        }
    }
}

/// Backend health status.
///
/// `Healthy`, `Unhealthy` and `Unreachable` are what an HTTP probe can say.
/// `Idle` and `Starting` only ever describe a backend that runs on a machine
/// pool (see [`PoolBacking`]): at zero machines there is nothing to probe and
/// nothing wrong, so neither is an outage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendStatus {
    Healthy,
    Unhealthy,
    Unreachable,
    Unknown,
    /// Pool backend scaled to zero with no work waiting.
    Idle,
    /// Pool backend with machines booting, or work waiting for one.
    Starting,
}

impl BackendStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            BackendStatus::Healthy => "healthy",
            BackendStatus::Unhealthy => "unhealthy",
            BackendStatus::Unreachable => "unreachable",
            BackendStatus::Unknown => "unknown",
            BackendStatus::Idle => "idle",
            BackendStatus::Starting => "starting",
        }
    }

    /// A probe or a pool said this backend cannot do its work right now.
    pub fn is_failing(&self) -> bool {
        matches!(self, BackendStatus::Unhealthy | BackendStatus::Unreachable)
    }
}

/// What a machine pool knows about the backend it runs, sampled by the pool
/// sweep. A pool backend has no always-on container: its health IS the pool's
/// state, and the HTTP prober leaves it alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PoolBacking {
    pub pool: String,
    /// Machines ready to take a job (busy or free).
    pub ready: u32,
    /// Machines requested or booting.
    pub starting: u32,
    pub busy_slots: u32,
    /// Jobs waiting for a machine.
    pub queued: u32,
    pub paused: bool,
    /// The boot-failure breaker is open: no machines are being created.
    pub breaker_open: bool,
    /// The pool's last sweep pass failed (no provider, unreadable spec, ...).
    pub error: Option<String>,
}

impl PoolBacking {
    /// The backend status this pool state amounts to. Pure, so the mapping is
    /// asserted in one place: a pool at zero machines with nothing queued is
    /// `Idle`, not down.
    pub fn status(&self) -> BackendStatus {
        if self.error.is_some() || self.breaker_open {
            BackendStatus::Unhealthy
        } else if self.ready > 0 {
            BackendStatus::Healthy
        } else if self.starting > 0 || (self.queued > 0 && !self.paused) {
            BackendStatus::Starting
        } else {
            BackendStatus::Idle
        }
    }
}

/// How many probe results each backend keeps. At the 15s default interval this
/// is a 30-minute window — enough for the admin dashboard's detail page to show
/// a shape the moment it opens, small enough to stay a rounding error in RAM
/// (a `Vec` of 120 small structs per backend).
pub const HISTORY_LEN: usize = 120;

/// One recorded health probe. `ms` is present even for a failure, because the
/// time a failing check took to fail is itself diagnostic (a 3s timeout and an
/// instant connection-refused are very different problems).
#[derive(Debug, Clone, Copy)]
pub struct HealthSample {
    pub at: SystemTime,
    pub ms: u64,
    pub status: BackendStatus,
}

/// Cached health state for a single backend
#[derive(Debug, Clone)]
pub struct BackendHealthEntry {
    pub name: String,
    pub url: String,
    pub healthcheck: String,
    pub port: Option<u16>,
    pub env: Option<Vec<String>>,
    pub status: BackendStatus,
    pub last_checked: Option<SystemTime>,
    pub last_healthy: Option<SystemTime>,
    pub response_time_ms: Option<u64>,
    /// Rolling probe history, oldest first, capped at [`HISTORY_LEN`].
    /// Ephemeral like the rest of this struct: a scheduler restart starts the
    /// window over.
    pub history: std::collections::VecDeque<HealthSample>,
    /// Set while a machine pool runs this backend. The pool sweep writes
    /// `status` then (see [`set_pool_backing`]); the prober skips it.
    pub pool: Option<PoolBacking>,
}

impl BackendHealthEntry {
    pub fn from_config(cfg: &BackendHealthConfig) -> Self {
        Self {
            name: cfg.name.clone(),
            url: cfg.url.clone(),
            healthcheck: cfg.healthcheck.clone(),
            port: cfg.port,
            env: cfg.env.clone(),
            status: BackendStatus::Unknown,
            last_checked: None,
            last_healthy: None,
            response_time_ms: None,
            history: std::collections::VecDeque::with_capacity(HISTORY_LEN),
            pool: None,
        }
    }

    /// Extract IP from URL (e.g. "http://10.100.1.40:3000" -> "10.100.1.40")
    pub fn ip(&self) -> Option<String> {
        self.url
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .split(':')
            .next()
            .map(|s| s.to_string())
    }
}

pub type BackendHealthCache = Arc<RwLock<Vec<BackendHealthEntry>>>;

/// Create a cache pre-populated from config (all Unknown status)
pub fn create_health_cache(backends: &[BackendHealthConfig]) -> BackendHealthCache {
    let entries = backends.iter().map(BackendHealthEntry::from_config).collect();
    Arc::new(RwLock::new(entries))
}

/// Record what the machine pools know, one entry per pool, keyed by the backend
/// each runs. Authoritative for the whole cache: a backend named here takes its
/// status from its pool and stops being probed; a backend that was pool-backed
/// and is not named any more goes back to `Unknown` and the prober picks it up
/// on its next sweep. A backend the pools do not know is left alone.
pub async fn set_pool_backing(cache: &BackendHealthCache, backings: &[(String, PoolBacking)]) {
    let now = SystemTime::now();
    let mut entries = cache.write().await;
    for entry in entries.iter_mut() {
        match backings.iter().find(|(backend, _)| *backend == entry.name) {
            Some((_, backing)) => {
                let status = backing.status();
                entry.status = status;
                entry.last_checked = Some(now);
                if status == BackendStatus::Healthy {
                    entry.last_healthy = Some(now);
                }
                // No probe ran, so no probe time to report or to chart.
                entry.response_time_ms = None;
                if entry.pool.as_ref() != Some(backing) {
                    entry.pool = Some(backing.clone());
                }
            }
            None if entry.pool.is_some() => {
                entry.pool = None;
                entry.status = BackendStatus::Unknown;
                entry.response_time_ms = None;
            }
            None => {}
        }
    }
}

/// Build the short-timeout client the health checks use.
pub fn health_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap_or_default()
}

/// One health-check sweep over every configured backend, updating the cache.
/// Reads from `SharedBackendConfigs` so live updates via `update_backends()`
/// are picked up. Used by the scheduler's monitor loop and, timer-driven, by
/// the standalone SSP's `BackendHealth` wakeup.
pub async fn check_backends_once(
    configs: &SharedBackendConfigs,
    cache: &BackendHealthCache,
    http_client: &reqwest::Client,
) {
    let backends = configs.read().await.clone();
    // A pool backend's status is written by the pool sweep; there is no
    // container at its URL to ask, and asking would only log a failure.
    let pooled: Vec<String> = cache
        .read()
        .await
        .iter()
        .filter(|e| e.pool.is_some())
        .map(|e| e.name.clone())
        .collect();

    for backend in backends.iter().filter(|b| !pooled.contains(&b.name)) {
        let health_url = format!(
            "{}{}",
            backend.url.trim_end_matches('/'),
            backend.healthcheck
        );

        let start = Instant::now();
        let (status, response_time_ms) = match http_client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                (BackendStatus::Healthy, start.elapsed().as_millis() as u64)
            }
            Ok(resp) => {
                warn!(
                    backend = %backend.name,
                    status_code = resp.status().as_u16(),
                    "Backend health check returned non-success"
                );
                (BackendStatus::Unhealthy, start.elapsed().as_millis() as u64)
            }
            Err(e) => {
                warn!(
                    backend = %backend.name,
                    error = %e,
                    "Backend health check failed"
                );
                (BackendStatus::Unreachable, start.elapsed().as_millis() as u64)
            }
        };

        let now = SystemTime::now();
        let mut entries = cache.write().await;
        if let Some(entry) = entries.iter_mut().find(|e| e.name == backend.name) {
            entry.status = status;
            entry.last_checked = Some(now);
            entry.response_time_ms = Some(response_time_ms);
            if status == BackendStatus::Healthy {
                entry.last_healthy = Some(now);
            }
            if entry.history.len() == HISTORY_LEN {
                entry.history.pop_front();
            }
            entry.history.push_back(HealthSample {
                at: now,
                ms: response_time_ms,
                status,
            });
        }
    }
}

/// Spawn a background task that periodically health-checks all backends
/// (the scheduler's loop-based flavor; the standalone SSP drives
/// [`check_backends_once`] from its timer dispatcher instead).
pub fn start_backend_health_monitor(
    configs: SharedBackendConfigs,
    cache: BackendHealthCache,
    interval_secs: u64,
) {
    let http_client = health_http_client();

    info!(interval_secs, "Starting backend health monitor");

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await;

        loop {
            check_backends_once(&configs, &cache, &http_client).await;
            interval.tick().await;
        }
    });
}
