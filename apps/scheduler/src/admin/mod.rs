//! The admin plane: a Solid dashboard plus the JSON API behind it, served on
//! their own listener.
//!
//! # Why a second port
//!
//! Everything on the ingest port is unauthenticated by design — `/ingest`,
//! `/proxy/query` (arbitrary SurrealQL against the replica), `/ssp/*`,
//! `PUT /backends` — because that port is only ever supposed to be reachable
//! from inside the deployment's private network. The dashboard is the first
//! surface here meant for a human browser, so it lives on its own listener
//! that can be published without publishing any of the above.
//!
//! # Why a bearer token rather than a cookie
//!
//! The same bundle has to work embedded (served from this scheduler) and
//! standalone (opened from anywhere, pointed at a scheduler by URL). A
//! cross-origin cookie needs `SameSite=None; Secure`, which no browser will
//! accept against a plain-http scheduler address — exactly the standalone case.
//! A bearer token in `localStorage` works identically in both, and takes CSRF
//! out of the picture entirely.

pub mod actions;
pub mod auth;
pub mod backends;
pub mod backups;
pub mod cloud;
pub mod config;
pub mod jobs;
pub mod logs;
pub mod mcp;
pub mod ops;
pub mod overview;
pub mod presence;
pub mod session;
pub mod tokens;
pub mod workflows;

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde_json::json;
use tokio::sync::RwLock;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tracing::{info, warn};

pub use config::AdminConfig;

use maintenance::db::{DbConfig, ReconnectingDb};
use maintenance::log_ring::LogRing;

use self::auth::RateLimiter;
use self::cloud::CloudLink;
use self::ops::Operations;
use self::session::{Scope, Session, SessionStore};
use self::workflows::WorkflowWatcher;

/// A handle to the scheduler's shared root database connection, published once
/// `Scheduler::start()` has built it.
///
/// It has to be late-bound: the HTTP servers come up before `start()` runs (so
/// health checks answer during the initial replica clone), but the database
/// handle is only created part-way through `start()`. Handlers that need it
/// answer 503 until it lands, which is the same thing every other endpoint does
/// while the scheduler is still `Cloning`.
pub type SharedDbSlot = Arc<RwLock<Option<Arc<ReconnectingDb>>>>;

pub fn new_db_slot() -> SharedDbSlot {
    Arc::new(RwLock::new(None))
}

#[derive(Clone)]
pub struct AdminState {
    pub config: Arc<AdminConfig>,
    pub metrics: crate::metrics::MetricsState,
    pub transport: Arc<crate::transport::HttpTransport>,
    pub sessions: Arc<SessionStore>,
    pub rate_limiter: Arc<RateLimiter>,
    pub logs: Arc<LogRing>,
    pub workflows: Arc<WorkflowWatcher>,
    pub db_config: Arc<DbConfig>,
    db_slot: SharedDbSlot,
    /// Cached copy of the slot, so the hot handlers don't take a lock per
    /// request once the handle exists.
    db_cache: Arc<std::sync::RwLock<Option<Arc<ReconnectingDb>>>>,
    pub bootstrap_timeout_secs: u64,
    pub health_check_interval_secs: u64,
    pub heartbeat_interval_ms: u64,
    /// Long-running actions, watched by the dashboard.
    pub ops: Arc<Operations>,
    /// Live users / sessions / registered views, sampled on a timer so every
    /// reader serves from memory.
    pub presence: Arc<presence::PresenceTracker>,
    /// The outbox tables, sampled on a timer for the same reason: every job
    /// aggregate is a scan of a user table, so it is paid for once.
    pub jobs: Arc<jobs::JobSampler>,
    /// The Sp00ky Cloud link, when this scheduler was given one.
    pub cloud: Option<CloudLink>,
    /// The backup plane's registries and queues, shared with the ingest port.
    pub backup: Arc<maintenance::BackupState>,
    /// What a reclone or rehash needs, shared with `POST /admin/resync`.
    pub resync: crate::ssp_management::ResyncArgs,
    /// Whether something relaunches this process when it exits.
    pub supervised: bool,
    /// The slug backups are stored under.
    pub project_slug: String,
    /// The finished admin router, for the MCP server's in-process dispatch.
    /// Late-bound like `db_slot`: the router is built FROM this state, so it
    /// cannot be a plain field.
    router_slot: Arc<std::sync::OnceLock<Router>>,
}

impl AdminState {
    /// A clone of the admin router, once `build` has produced it.
    pub fn router(&self) -> Option<Router> {
        self.router_slot.get().cloned()
    }

    /// The shared root handle, or `None` while the scheduler is still starting.
    pub fn db(&self) -> Option<Arc<ReconnectingDb>> {
        if let Some(db) = self
            .db_cache
            .read()
            .expect("admin db cache poisoned")
            .clone()
        {
            return Some(db);
        }
        // Not cached yet: consult the slot. `try_read` rather than a blocking
        // read because this is called from async handlers — a miss simply means
        // "not ready yet", which is already a state every caller handles.
        let found = self.db_slot.try_read().ok().and_then(|g| g.clone())?;
        *self.db_cache.write().expect("admin db cache poisoned") = Some(Arc::clone(&found));
        Some(found)
    }
}

/// The error shape every admin endpoint returns.
///
/// One shape across the whole plane, so the dashboard can always read
/// `{"error": "..."}` and show the server's own words. A handler that returned
/// a bare string here would surface in the UI as a generic "Request failed",
/// throwing away the explanation it took the trouble to write.
pub type ApiError = (StatusCode, Json<serde_json::Value>);

pub fn api_error(status: StatusCode, message: impl std::fmt::Display) -> ApiError {
    (status, Json(json!({ "error": message.to_string() })))
}

/// The answer every database-backed reader gives before `Scheduler::start()` has
/// published its handle. Shared so the whole plane says the same sentence.
pub fn db_unavailable() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "Scheduler is still starting up and has no database handle yet",
    )
}

/// Run one statement and take its rows, noting a failure on the handle so the
/// reconnect logic sees it.
pub async fn rows(
    db: &Arc<ReconnectingDb>,
    surql: &str,
) -> Result<Vec<serde_json::Value>, ApiError> {
    let handle = db.handle();
    match handle.query(surql).await.and_then(|mut r| r.take(0)) {
        Ok(v) => Ok(v),
        Err(e) => {
            db.note_error(&format!("{e:#}"));
            warn!(error = %e, surql, "Admin query failed");
            Err(api_error(
                StatusCode::BAD_GATEWAY,
                format!("Database query failed: {}", e),
            ))
        }
    }
}

/// Escape a single-quoted SurrealQL string literal.
///
/// The admin listings assemble their queries as text because the surrounding
/// CLI queries do, and their filters are user-supplied. One escaper for the
/// whole plane, matching `apps/cli/src/flag.rs::esc`, so no reader can end up
/// with a weaker one than its neighbour.
pub fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// The signed-in session, extracted by [`require_session`] and attached to the
/// request for handlers that want to know who is asking.
#[derive(Clone)]
pub struct CurrentSession(pub Session);

/// Bearer-token gate for `/admin/api/*`, minus the endpoints registered
/// outside it (`/config` and `/session`).
async fn require_session(
    State(state): State<AdminState>,
    mut req: Request,
    next: Next,
) -> Response {
    let token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);

    match token.and_then(|t| state.sessions.get(t)) {
        Some(session) => {
            // Scope is enforced here and nowhere else, so a read-only token
            // cannot write however the request is phrased. The MCP endpoint
            // is a POST that carries reads too; it checks scope per tool.
            let is_mcp = req.uri().path().trim_end_matches('/').ends_with("/mcp");
            if session.scope == Scope::Read && req.method() != axum::http::Method::GET && !is_mcp {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "This token is read-only" })),
                )
                    .into_response();
            }
            req.extensions_mut().insert(CurrentSession(session));
            next.run(req).await
        }
        None => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Not signed in" })),
        )
            .into_response(),
    }
}

/// `GET /admin/api/config` — the only unauthenticated endpoint.
///
/// It exists so the frontend can tell embedded from standalone: a successful
/// same-origin response means it is being served by a scheduler and should hide
/// its endpoint field. It reveals nothing an unauthenticated caller of `/health`
/// on the ingest port could not already see.
async fn config_handler(State(state): State<AdminState>) -> Json<serde_json::Value> {
    Json(json!({
        "scheduler_id": state.metrics.scheduler_id,
        "version": env!("CARGO_PKG_VERSION"),
        "breakglass_available": state.config.password.is_some(),
        // What the dashboard can offer: cloud-only actions need the link,
        // and a restart button on an unsupervised process deserves a warning
        // before, not after.
        "cloud_linked": state.cloud.is_some(),
        "supervised": state.supervised,
        "sessions_persistent": state.sessions.persistent(),
        "project_slug": state.project_slug,
    }))
}

/// `GET /admin/api/me` — who the current token belongs to.
async fn me_handler(
    axum::Extension(CurrentSession(session)): axum::Extension<CurrentSession>,
) -> Json<serde_json::Value> {
    Json(json!({
        "subject": session.subject,
        "label": session.label,
        "mode": session.mode,
        "scope": session.scope,
    }))
}

/// `POST /admin/api/logout` — drop the current token server-side.
async fn logout_handler(
    State(state): State<AdminState>,
    req: Request,
) -> StatusCode {
    if let Some(token) = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        state.sessions.revoke(token.trim());
    }
    StatusCode::NO_CONTENT
}

/// Placeholder served when no dashboard bundle is present.
///
/// A scheduler built with `cargo build` and run from a checkout has no
/// `dist/` — that must be a readable message, not a 404 storm or a failed
/// startup.
async fn missing_dashboard() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        concat!(
            "<!doctype html><meta charset=utf-8>",
            "<title>Sp00ky admin</title>",
            "<body style=\"font:14px system-ui;margin:3rem auto;max-width:32rem;color:#ddd;background:#141210\">",
            "<h1 style=\"font-size:1.1rem\">Dashboard not bundled</h1>",
            "<p>This scheduler has no dashboard build at its <code>SPKY_ADMIN_DIR</code>.</p>",
            "<p>Build it with <code>pnpm --filter @spooky-sync/dashboard build</code> and point ",
            "<code>SPKY_ADMIN_DIR</code> at <code>apps/dashboard/dist</code>, or use an official ",
            "scheduler image, which ships one.</p>",
            "<p>The API on this port is unaffected: <code>/admin/api/config</code> still answers.</p>",
            "</body>",
        ),
    )
}

/// Build the admin router: JSON API plus the static dashboard.
pub fn create_admin_router(state: AdminState) -> Router {
    // Origin `*` with no credentials: the token travels in a header the browser
    // only sends when the page has it, so a hostile page gains nothing here.
    // Allowing credentials with `*` is what would be unsafe, and is not done.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
        .allow_methods(Any);

    let protected = Router::new()
        .route("/me", get(me_handler))
        .route("/logout", post(logout_handler))
        .route("/overview", get(overview::overview))
        .route("/presence", get(presence::presence))
        .route("/views", get(presence::list_views))
        .route("/views/:key", get(presence::view_detail))
        .route("/backends", get(backends::list))
        .route("/backends/:name", get(backends::detail))
        .route("/logs", get(logs::stream))
        .route("/workflows/runs", get(workflows::list_runs))
        .route("/workflows/runs/:id", get(workflows::run_detail))
        .route("/workflows/stream", get(workflows::stream_runs))
        .route("/schedules", get(workflows::list_schedules))
        .route("/schedules/:name", get(workflows::schedule_detail))
        // Actions. Every one logs the session subject; the long-running
        // ones answer 202 with an operation the dashboard can watch.
        .route("/operations", get(actions::list_operations))
        .route("/operations/stream", get(actions::stream_operations))
        .route("/ssps/restart-all", post(actions::ssps_restart_all))
        .route("/ssps/:id/restart", post(actions::ssp_restart))
        .route("/scheduler/restart", post(actions::scheduler_restart))
        .route("/cloud/restart", post(actions::cloud_restart))
        .route("/cloud/deployment", get(actions::cloud_deployment))
        .route("/backups", get(backups::list).post(backups::create))
        .route("/backups/config", put(backups::configure))
        .route("/backups/:id", delete(backups::delete))
        .route(
            "/backups/:id/restore",
            post(backups::restore).get(backups::restore_status),
        )
        .route("/workflows/runs/:id/cancel", post(workflows::cancel_run))
        .route("/workflows/runs/:id/rerun", post(workflows::rerun_run))
        .route("/workflows/runs/:id/retry", post(workflows::retry_run))
        .route("/schedules/:name/pause", post(workflows::schedule_pause))
        .route("/schedules/:name/resume", post(workflows::schedule_resume))
        .route("/schedules/:name/trigger", post(workflows::schedule_trigger))
        // Jobs. `/jobs/clear` is registered before `/jobs/:id` so the literal
        // segment wins the match rather than being read as a job named "clear".
        .route("/jobs", get(jobs::list_jobs))
        .route("/jobs/stream", get(jobs::stream_jobs))
        .route("/jobs/clear", post(jobs::jobs_clear))
        .route("/jobs/:id", get(jobs::job_detail))
        .route("/jobs/:id/kill", post(jobs::job_kill))
        .route("/jobs/:id/retry", post(jobs::job_retry))
        // Agents. Tokens are minted by a person; the MCP endpoint takes any
        // bearer and dispatches tools back through this same router.
        .route("/tokens", post(tokens::mint).delete(tokens::revoke))
        .route(
            "/mcp",
            post(mcp::handle).get(mcp::not_allowed).delete(mcp::not_allowed),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ));

    let api = Router::new()
        .route("/config", get(config_handler))
        .route("/session", post(auth::login))
        .merge(protected)
        .layer(cors)
        .with_state(state.clone());

    // Serve the built dashboard, falling back to index.html so client-side
    // routes survive a reload or a deep link.
    let index = state.config.dir.join("index.html");
    let assets = if index.is_file() {
        info!(dir = %state.config.dir.display(), "Serving admin dashboard");
        get_service_router(ServeDir::new(&state.config.dir).fallback(ServeFile::new(index)))
    } else {
        warn!(
            dir = %state.config.dir.display(),
            "No dashboard bundle found; /admin serves a placeholder (the API still works)"
        );
        Router::new().fallback(missing_dashboard)
    };

    Router::new()
        .nest("/admin/api", api)
        .nest_service("/admin", assets)
        // Bare `/` is a convenience: someone who types the host and port
        // without a path means the dashboard.
        .route(
            "/",
            get(|| async { axum::response::Redirect::permanent("/admin/") }),
        )
}

fn get_service_router(svc: ServeDir<ServeFile>) -> Router {
    Router::new().fallback_service(svc)
}

/// Everything `build` needs beyond the admin config itself. A struct rather
/// than a dozen positional arguments, because the call site in `main.rs` and
/// the one in the integration tests must stay readable as they grow.
pub struct AdminDeps {
    pub metrics: crate::metrics::MetricsState,
    pub transport: Arc<crate::transport::HttpTransport>,
    pub logs: Arc<LogRing>,
    pub db_config: DbConfig,
    pub db_slot: SharedDbSlot,
    pub backup: Arc<maintenance::BackupState>,
    pub resync: crate::ssp_management::ResyncArgs,
    pub cloud: Option<CloudLink>,
    /// The cluster secret, which selects restart-surviving sessions.
    pub auth_secret: Option<String>,
    pub supervised: bool,
}

/// Assemble the admin state and its router.
pub fn build(config: AdminConfig, deps: AdminDeps) -> (AdminState, Router) {
    let project_slug = backups::project_slug(deps.cloud.as_ref());
    let scheduler_config = Arc::clone(&deps.resync.config);
    // `config` is moved into the state below, so the presence knobs are read
    // from a copy taken first.
    let config_for_presence = config.clone();
    let state = AdminState {
        sessions: SessionStore::new(config.session_ttl, deps.auth_secret.as_deref()),
        config: Arc::new(config),
        metrics: deps.metrics,
        transport: deps.transport,
        rate_limiter: RateLimiter::new(),
        logs: deps.logs,
        workflows: WorkflowWatcher::new(),
        db_config: Arc::new(deps.db_config),
        db_slot: deps.db_slot,
        db_cache: Arc::new(std::sync::RwLock::new(None)),
        bootstrap_timeout_secs: scheduler_config.bootstrap_timeout_secs,
        health_check_interval_secs: scheduler_config.health_check_interval_secs,
        heartbeat_interval_ms: scheduler_config.heartbeat_interval_ms,
        ops: Operations::new(),
        presence: presence::PresenceTracker::new(&config_for_presence),
        jobs: jobs::JobSampler::new(&config_for_presence),
        cloud: deps.cloud,
        backup: deps.backup,
        resync: deps.resync,
        supervised: deps.supervised,
        project_slug,
        router_slot: mcp::router_slot(),
    };
    let router = create_admin_router(state.clone());
    // The MCP server dispatches through the very router it is mounted on.
    let _ = state.router_slot.set(router.clone());
    // One sampler for the whole plane. Spawned here rather than from
    // `Scheduler::start` because everything it needs is already in `AdminState`:
    // it reaches the database through the same late-bound slot every other
    // admin reader uses, and simply does nothing until that slot fills.
    Arc::clone(&state.presence).spawn(state.clone());
    Arc::clone(&state.jobs).spawn(state.clone());
    (state, router)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every listing on this plane interpolates its filters, so the one escaper
    /// they share is the one thing that keeps a quote in a search box a quote.
    #[test]
    fn esc_neutralises_quotes_and_backslashes() {
        assert_eq!(esc("o'brien"), "o\\'brien");
        assert_eq!(esc(r"back\slash"), r"back\\slash");
        assert_eq!(esc("plain"), "plain");
    }
}
