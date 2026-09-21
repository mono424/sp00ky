//! Cluster-mode host for the machine pool engine (`packages/pool-core`).
//!
//! Three things live here, all thin:
//!
//! - the **sweep**: one task, one ticker, driving `PoolEngine::tick_pass`. The
//!   scheduler is already the cluster's singleton, which is what gives job to
//!   machine assignment a single writer. (The engine's claims are still
//!   compare-and-swap, so a second ticker would be wasteful, not harmful.)
//! - the **pool listener**: the only surface a pool machine ever talks to. It is
//!   its own port so it can be published without publishing the unauthenticated
//!   ingest port next to it, and every request carries a per-machine bearer
//!   token. Machines dial in; nothing here ever dials a machine.
//! - the **providers**: `docker` when a docker socket is available, `hetzner`
//!   (VMs, created by Sp00ky Cloud on this tenant's behalf) when the scheduler
//!   is linked to a control plane.
//!
//! A poll is held open for a few seconds and woken by the sweep, so an agent
//! hears about a new assignment within a second instead of on its next poll,
//! without anybody keeping a per-agent queue: the reply is always re-derived
//! from the database (see `pool_protocol`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use hmac::{Hmac, Mac};
use pool_core::{MachineProvider, PoolEngine, PoolEngineConfig};
use pool_protocol::{
    HelloReply, HelloRequest, PollReply, PollRequest, ReadyRequest, ResultReply, ResultRequest,
    ROUTE_PREFIX,
};
use sha2::Sha256;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::admin::SharedDbSlot;
use crate::pool_docker::{DockerProvider, DockerProviderConfig};
use crate::schedule_engine::SharedDb;

/// Fast enough that a job waits a couple of seconds for a free machine, not a
/// poll interval; slow enough to be a handful of indexed reads per pool.
const POOL_SWEEP_INTERVAL_SECS: u64 = 2;

/// Longest a poll is held open. Well under any proxy's idle timeout.
const MAX_POLL_HOLD_SECS: u64 = 10;

#[derive(Clone)]
pub struct PoolHostConfig {
    pub enabled: bool,
    pub bind_host: String,
    pub port: u16,
    /// The listener's URL as a MACHINE sees it.
    pub public_url: String,
    secret: Arc<Vec<u8>>,
    pub docker: Option<DockerProviderConfig>,
    /// Image carrying `spky-agent`, for whichever provider installs it.
    pub agent_image: String,
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

impl PoolHostConfig {
    pub fn from_env() -> Self {
        let port = env("SPKY_POOL_PORT")
            .and_then(|p| p.parse().ok())
            .unwrap_or(9669);
        let public_url =
            env("SPKY_POOL_PUBLIC_URL").unwrap_or_else(|| format!("http://scheduler:{port}"));
        // A machine's token is an HMAC of its id under this secret, so the secret
        // has to survive a scheduler restart or every running machine is locked
        // out and its jobs fail over for nothing.
        let secret = env("SPKY_POOL_SECRET")
            .or_else(|| env("SPKY_AUTH_SECRET"))
            .unwrap_or_else(|| {
                warn!(
                "neither SPKY_POOL_SECRET nor SPKY_AUTH_SECRET is set: machine tokens will not \
                 survive a scheduler restart"
            );
                uuid::Uuid::new_v4().to_string()
            });
        let agent_image =
            agent_image_from(env("SPKY_POOL_AGENT_IMAGE"), env("SPKY_RELEASE_VERSION"));
        let docker = match env("SPKY_POOL_DOCKER").as_deref() {
            None | Some("0") | Some("false") | Some("off") => None,
            Some(_) => Some(DockerProviderConfig {
                agent_image: agent_image.clone(),
                network: env("SPKY_POOL_DOCKER_NETWORK"),
                pool_url: public_url.clone(),
            }),
        };
        Self {
            enabled: env("SPKY_POOL_ENABLED")
                .map(|v| v != "0" && v != "false")
                .unwrap_or(true),
            bind_host: env("SPKY_POOL_HOST").unwrap_or_else(|| "0.0.0.0".to_string()),
            port,
            public_url,
            secret: Arc::new(secret.into_bytes()),
            docker,
            agent_image,
        }
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.bind_host, self.port)
    }

    fn mac(&self, machine_id: &str) -> Hmac<Sha256> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.secret).expect("hmac accepts any key length");
        mac.update(machine_id.as_bytes());
        mac
    }

    /// The bearer token for one machine. Stateless: nothing to store, nothing to
    /// revoke beyond the machine row itself, which every handler re-checks.
    pub fn token_for(&self, machine_id: &str) -> String {
        hex::encode(self.mac(machine_id).finalize().into_bytes())
    }

    /// Constant-time check that `token` was minted for `machine_id`.
    fn verify(&self, machine_id: &str, token: &str) -> bool {
        hex::decode(token)
            .map(|raw| self.mac(machine_id).verify_slice(&raw).is_ok())
            .unwrap_or(false)
    }
}

/// Which agent image a machine installs. The agent speaks the pool protocol to
/// THIS scheduler, so by default it is the one published alongside it: the
/// publish workflow pushes `mono424/spooky-agent` under the same tag as the
/// scheduler image and stamps that tag into the image as `SPKY_RELEASE_VERSION`
/// (a manually dispatched build carries a tag that is not the crate version).
/// Outside a published image the crate version is the best guess.
fn agent_image_from(explicit: Option<String>, release: Option<String>) -> String {
    explicit.unwrap_or_else(|| {
        let tag = release.unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        format!("mono424/spooky-agent:{tag}")
    })
}

/// The process's pool host, for the one caller that cannot be handed it: the
/// schedule engine's job-kill capability is rebuilt per request in several
/// places, and a kill that silently misses pool jobs is worse than a global.
static GLOBAL: std::sync::OnceLock<PoolHost> = std::sync::OnceLock::new();

/// The pool host, once `main` has installed it (never, when pools are disabled).
pub fn global() -> Option<&'static PoolHost> {
    GLOBAL.get()
}

#[derive(Clone)]
pub struct PoolHost {
    cfg: PoolHostConfig,
    db_slot: SharedDbSlot,
    providers: Arc<BTreeMap<String, Arc<dyn MachineProvider>>>,
    /// Woken by the sweep whenever something an agent might care about changed.
    changed: Arc<Notify>,
}

impl PoolHost {
    pub fn new(cfg: PoolHostConfig, db_slot: SharedDbSlot) -> Self {
        let mut providers: BTreeMap<String, Arc<dyn MachineProvider>> = BTreeMap::new();
        if let Some(docker_cfg) = cfg.docker.clone() {
            let minter = cfg.clone();
            match DockerProvider::connect(docker_cfg, Box::new(move |id| minter.token_for(id))) {
                Ok(provider) => {
                    providers.insert("docker".to_string(), Arc::new(provider));
                }
                Err(e) => error!(error = %e, "docker pool provider unavailable"),
            }
        }
        // `hetzner`: served by Sp00ky Cloud, so it exists exactly when this
        // scheduler is linked to a control plane. `SPKY_POOL_CLOUD=0` opts out.
        let cloud_wanted = env("SPKY_POOL_CLOUD")
            .map(|v| v != "0" && v != "false")
            .unwrap_or(true);
        if let Some(link) = crate::admin::cloud::CloudLink::from_env().filter(|_| cloud_wanted) {
            let minter = cfg.clone();
            providers.insert(
                "hetzner".to_string(),
                Arc::new(crate::pool_cloud::CloudProvider::new(
                    link,
                    cfg.public_url.clone(),
                    cfg.agent_image.clone(),
                    Box::new(move |id| minter.token_for(id)),
                )),
            );
        }
        Self {
            cfg,
            db_slot,
            providers: Arc::new(providers),
            changed: Arc::new(Notify::new()),
        }
    }

    /// An engine over the scheduler's shared session. Cheap to build: it holds no
    /// state the agent entry points need.
    async fn engine(&self) -> Option<PoolEngine> {
        let db = self.db_slot.read().await.clone()?;
        Some(PoolEngine::new(
            Arc::new(SharedDb(db)),
            (*self.providers).clone(),
            PoolEngineConfig::default(),
        ))
    }

    /// Make this host reachable through [`global`]. First call wins.
    pub fn install_global(&self) {
        let _ = GLOBAL.set(self.clone());
    }

    /// Is `table` the outbox table of some pool? Decides whether a job kill goes
    /// to the pool engine or to the SSPs.
    pub async fn owns_table(&self, table: &str) -> bool {
        if !schedule_core::sql::is_plain_identifier(table) {
            return false;
        }
        let Some(db) = self.db_slot.read().await.clone() else {
            return false;
        };
        let found = schedule_core::ScheduleDb::query(
            &SharedDb(db),
            "SELECT name FROM _00_pool WHERE target_table = $table LIMIT 1",
            &[("table", serde_json::json!(table))],
        )
        .await;
        found
            .map(|r| schedule_core::db::first_row(r).is_some())
            .unwrap_or(false)
    }

    /// Operator kill for a pool job; wakes held polls so the agent hears at once.
    pub async fn kill_job(&self, job_id: &str) -> anyhow::Result<bool> {
        let Some(engine) = self.engine().await else {
            anyhow::bail!("database session not ready")
        };
        let killed = engine.kill_job(job_id).await?;
        self.changed.notify_waiters();
        Ok(killed)
    }

    /// Start the cluster pool sweep. One task, one ticker.
    pub fn start_sweep(&self) {
        let host = self.clone();
        tokio::spawn(async move {
            // The SWEEP engine is kept: its pass counter paces the orphan sweep.
            let mut engine: Option<PoolEngine> = None;
            let mut interval = tokio::time::interval(Duration::from_secs(POOL_SWEEP_INTERVAL_SECS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if engine.is_none() {
                    engine = host.engine().await;
                    if engine.is_none() {
                        debug!("Pool sweep: database session not ready yet");
                        continue;
                    }
                }
                match engine.as_ref().unwrap().tick_pass().await {
                    Ok(report) => {
                        for t in &report.transitions {
                            let summary = format!(
                                "Pool '{}' stopped creating machines after {} failed boots: {}",
                                t.pool, t.failures, t.detail
                            );
                            crate::admin::incidents::emit(
                                &format!("pool:{}", t.pool),
                                "pool_breaker_open",
                                if t.open { "open" } else { "recovered" },
                                &summary,
                                None,
                            );
                        }
                        if report.orphans_destroyed > 0 {
                            warn!(
                                count = report.orphans_destroyed,
                                "destroyed leaked pool machines"
                            );
                        }
                        let news =
                            report.assigned + report.drained + report.requeued + report.failed_jobs;
                        if news > 0 {
                            host.changed.notify_waiters();
                        }
                        if report != Default::default() {
                            debug!(?report, "pool sweep");
                        }
                    }
                    // The shared handle heals itself; the next tick runs against
                    // the refreshed session.
                    Err(e) => error!(error = format!("{e:#}"), "Pool sweep pass failed"),
                }
            }
        });
        info!(
            interval_secs = POOL_SWEEP_INTERVAL_SECS,
            providers = ?self.providers.keys().collect::<Vec<_>>(),
            "Cluster pool sweep started"
        );
    }

    pub fn router(&self) -> Router {
        let route = |path: &str| format!("{ROUTE_PREFIX}{path}");
        Router::new()
            .route(&route("/health"), get(|| async { "ok" }))
            .route(&route("/hello"), post(hello))
            .route(&route("/ready"), post(ready))
            .route(&route("/poll"), post(poll))
            .route(&route("/result"), post(result))
            .with_state(self.clone())
    }
}

type Rejection = (StatusCode, String);

fn authorize(host: &PoolHost, headers: &HeaderMap, machine: &str) -> Result<(), Rejection> {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    if host.cfg.verify(machine, token) {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            "invalid machine token".to_string(),
        ))
    }
}

async fn engine_or_503(host: &PoolHost) -> Result<PoolEngine, Rejection> {
    host.engine().await.ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "scheduler is still starting".to_string(),
    ))
}

fn internal(e: anyhow::Error) -> Rejection {
    warn!(error = format!("{e:#}"), "pool request failed");
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
}

/// `410 Gone` is how a machine learns it is no longer wanted before it has a
/// poll loop to be told `Shutdown` through.
fn gone() -> Rejection {
    (
        StatusCode::GONE,
        "this machine is not wanted any more".to_string(),
    )
}

async fn hello(
    State(host): State<PoolHost>,
    headers: HeaderMap,
    Json(req): Json<HelloRequest>,
) -> Result<Json<HelloReply>, Rejection> {
    authorize(&host, &headers, &req.machine)?;
    let engine = engine_or_503(&host).await?;
    match engine.on_hello(&req.machine).await.map_err(internal)? {
        Some(reply) => {
            info!(machine = %req.machine, agent = %req.agent_version, "pool machine said hello");
            Ok(Json(reply))
        }
        None => Err(gone()),
    }
}

async fn ready(
    State(host): State<PoolHost>,
    headers: HeaderMap,
    Json(req): Json<ReadyRequest>,
) -> Result<StatusCode, Rejection> {
    authorize(&host, &headers, &req.machine)?;
    let engine = engine_or_503(&host).await?;
    if engine.on_ready(&req.machine).await.map_err(internal)? {
        info!(machine = %req.machine, "pool machine is ready");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(gone())
    }
}

async fn poll(
    State(host): State<PoolHost>,
    headers: HeaderMap,
    Json(req): Json<PollRequest>,
) -> Result<Json<PollReply>, Rejection> {
    authorize(&host, &headers, &req.machine)?;
    let engine = engine_or_503(&host).await?;

    // Register for a wake-up BEFORE reading, so a change that lands between the
    // read and the wait is not missed.
    let changed = host.changed.notified();
    tokio::pin!(changed);
    changed.as_mut().enable();

    let reply = engine.on_poll(&req).await.map_err(internal)?;
    if !reply.commands.is_empty() {
        return Ok(Json(reply));
    }
    // Nothing to say: hold the poll until the sweep reports news, then look once
    // more. The first read already renewed the leases, so holding costs nothing.
    if tokio::time::timeout(Duration::from_secs(MAX_POLL_HOLD_SECS), changed)
        .await
        .is_err()
    {
        return Ok(Json(reply));
    }
    Ok(Json(engine.on_poll(&req).await.map_err(internal)?))
}

async fn result(
    State(host): State<PoolHost>,
    headers: HeaderMap,
    Json(req): Json<ResultRequest>,
) -> Result<Json<ResultReply>, Rejection> {
    authorize(&host, &headers, &req.machine)?;
    let engine = engine_or_503(&host).await?;
    let reply = engine.on_result(&req).await.map_err(internal)?;
    // A slot just freed: wake nothing here, the next sweep (<= 2s) places the
    // next job and wakes the polls itself.
    Ok(Json(reply))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(secret: &str) -> PoolHostConfig {
        PoolHostConfig {
            enabled: true,
            bind_host: "127.0.0.1".into(),
            port: 0,
            public_url: "http://scheduler:9669".into(),
            secret: Arc::new(secret.as_bytes().to_vec()),
            docker: None,
            agent_image: "agent:test".into(),
        }
    }

    #[test]
    fn a_token_opens_exactly_one_machine() {
        let c = cfg("s3cret");
        let token = c.token_for("_00_machine:a");
        assert!(c.verify("_00_machine:a", &token));
        assert!(
            !c.verify("_00_machine:b", &token),
            "a token is bound to its machine id"
        );
        assert!(!c.verify("_00_machine:a", "not-hex"));
        assert!(!c.verify("_00_machine:a", ""));
        assert!(
            !cfg("other").verify("_00_machine:a", &token),
            "and to the scheduler's secret"
        );
    }

    #[test]
    fn the_agent_image_follows_the_release_the_scheduler_was_published_as() {
        assert_eq!(
            agent_image_from(None, Some("pools-rc1".into())),
            "mono424/spooky-agent:pools-rc1",
            "a dispatched build's tag is not the crate version"
        );
        assert_eq!(
            agent_image_from(None, None),
            format!("mono424/spooky-agent:{}", env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            agent_image_from(
                Some("registry.local/agent:dev".into()),
                Some("pools-rc1".into())
            ),
            "registry.local/agent:dev",
            "an explicit image always wins"
        );
    }

    #[test]
    fn tokens_are_stable_across_restarts_given_the_same_secret() {
        assert_eq!(
            cfg("k").token_for("_00_machine:a"),
            cfg("k").token_for("_00_machine:a")
        );
    }
}
