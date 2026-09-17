//! Cloudflare Durable Object shell for the SSP node.
//!
//! This is `apps/ssp-portable` re-expressed for a DO: the SAME
//! `ssp_node::SspNode`/`Runtime` driven by an entirely different platform —
//! `fetch` ingress instead of hyper/axum, DO alarms instead of tokio timers, DO
//! storage instead of disk, and (critically) `HttpSqlDb` over `fetch` instead
//! of the surrealdb SDK, so the DB stays EXTERNAL (SurrealDB Cloud). Nothing in
//! the core changed to get here — every platform-specific concern is one of the
//! port adapters below.
//!
//! Build target is wasm32 only (worker-build / wrangler). It is intentionally
//! NOT a member of the main cargo workspace (see Cargo.toml). Targets the
//! `worker` 0.8 API: DurableObject methods take `&self` (interior mutability for
//! the lazily-built node), Storage ops are `&self`, and the DO is registered as
//! a SQLite-backed class by the control plane.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::RwLock;
use wasm_bindgen_futures::spawn_local;
use worker::*;

use ssp::circuit::Circuit;
use ssp_node::ports::{
    CancelWatch, CircuitStore, CircuitStoreError, HttpClient, HttpError, LocalBoxFuture,
    OutboundRequest, OutboundResponse, ResumePoint, Scheduler, Spawner, TimerKind,
};
use ssp_node::{
    now_epoch_ms, ApiBody, HttpSqlDb, Method as ApiMethod, NoopTelemetry, Platform, Runtime,
    SspNode, SspStatus, TimerMux,
};

const CIRCUIT_BLOB_KEY: &str = "ssp:circuit_blob";
const RESUME_POINT_KEY: &str = "ssp:resume_point";

// ---------------------------------------------------------------------------
// Port adapters
// ---------------------------------------------------------------------------

/// `HttpClient` over the Workers `fetch` global. Fills BOTH the job-dispatch
/// port and (indirectly) the `Db` port via `HttpSqlDb`.
struct CfHttp;

#[async_trait(?Send)]
impl HttpClient for CfHttp {
    async fn send(
        &self,
        req: OutboundRequest,
        _cancel: Option<CancelWatch>,
    ) -> std::result::Result<OutboundResponse, HttpError> {
        let headers = Headers::new();
        if let Some(bearer) = &req.bearer {
            headers
                .set("Authorization", &format!("Bearer {bearer}"))
                .map_err(|e| HttpError::Transport(e.to_string()))?;
        }
        for (k, v) in &req.headers {
            headers.set(k, v).map_err(|e| HttpError::Transport(e.to_string()))?;
        }

        let mut init = RequestInit::new();
        let mut body_js = None;
        if let Some(body) = &req.json_body {
            let s = serde_json::to_string(body).map_err(|e| HttpError::Transport(e.to_string()))?;
            // fetch() doesn't infer Content-Type from the body the way reqwest's
            // .json() does; set it explicitly or JSON endpoints 415.
            headers
                .set("Content-Type", "application/json")
                .map_err(|e| HttpError::Transport(e.to_string()))?;
            body_js = Some(wasm_bindgen::JsValue::from_str(&s));
        }
        init.with_method(map_method(req.method)).with_headers(headers);
        if body_js.is_some() {
            init.with_body(body_js);
        }

        let wreq = Request::new_with_init(&req.url, &init)
            .map_err(|e| HttpError::Transport(e.to_string()))?;
        let mut resp = Fetch::Request(wreq)
            .send()
            .await
            .map_err(|e| HttpError::Transport(e.to_string()))?;
        let status = resp.status_code();
        let body = resp.text().await.map_err(|e| HttpError::Transport(e.to_string()))?;
        Ok(OutboundResponse { status, body })
    }
}

/// `Spawner` over `wasm_bindgen_futures::spawn_local`.
struct CfSpawner;
impl Spawner for CfSpawner {
    fn spawn(&self, fut: LocalBoxFuture) {
        spawn_local(fut);
    }
}

/// `CircuitStore` over DO storage: one JSON blob + resume point. `worker` 0.8
/// Storage ops take `&self`, so a shared `Rc<Storage>` suffices (no RefCell).
struct CfCircuitStore {
    storage: Rc<Storage>,
}

#[async_trait(?Send)]
impl CircuitStore for CfCircuitStore {
    async fn save(
        &self,
        blob: &str,
        point: &ResumePoint,
    ) -> std::result::Result<(), CircuitStoreError> {
        self.storage
            .put(CIRCUIT_BLOB_KEY, blob)
            .await
            .map_err(|e| CircuitStoreError::Transport(e.to_string()))?;
        self.storage
            .put(RESUME_POINT_KEY, point)
            .await
            .map_err(|e| CircuitStoreError::Transport(e.to_string()))?;
        Ok(())
    }

    async fn load(&self) -> std::result::Result<(String, ResumePoint), CircuitStoreError> {
        // 0.8 `get` returns Ok(None) for a missing key — that's a cold start.
        let blob: String = self
            .storage
            .get(CIRCUIT_BLOB_KEY)
            .await
            .map_err(|e| CircuitStoreError::Transport(e.to_string()))?
            .ok_or(CircuitStoreError::NotFound)?;
        let point: ResumePoint = self
            .storage
            .get(RESUME_POINT_KEY)
            .await
            .map_err(|e| CircuitStoreError::Transport(e.to_string()))?
            .ok_or(CircuitStoreError::NotFound)?;
        Ok((blob, point))
    }

    async fn clear(&self) -> std::result::Result<(), CircuitStoreError> {
        let _ = self.storage.delete(CIRCUIT_BLOB_KEY).await;
        let _ = self.storage.delete(RESUME_POINT_KEY).await;
        Ok(())
    }
}

/// `Scheduler` over the DO's single `alarm()`. Pending timers live in one
/// in-memory [`TimerMux`]; the earliest deadline arms the alarm. Timers are NOT
/// persisted — eviction drops them, and bootstrap/on_timer re-arm the periodic
/// sweeps on the next wake.
struct CfScheduler {
    mux: Rc<RefCell<TimerMux>>,
    storage: Rc<Storage>,
}

impl CfScheduler {
    async fn rearm_alarm(&self) {
        let next = self.mux.borrow().next_deadline();
        if let Some(at) = next {
            let offset = (at as i64 - now_epoch_ms() as i64).max(0);
            let _ = self.storage.set_alarm(offset).await;
        }
    }
}

#[async_trait(?Send)]
impl Scheduler for CfScheduler {
    async fn schedule(&self, kind: TimerKind, at_epoch_ms: u64) {
        self.mux.borrow_mut().schedule(kind, at_epoch_ms);
        self.rearm_alarm().await;
    }
    async fn cancel(&self, kind: &TimerKind) {
        self.mux.borrow_mut().cancel(kind);
        self.rearm_alarm().await;
    }
    async fn sleep(&self, dur: Duration) {
        Delay::from(dur).await;
    }
}

fn map_method(m: ApiMethod) -> Method {
    match m {
        ApiMethod::Get => Method::Get,
        ApiMethod::Post => Method::Post,
        ApiMethod::Put => Method::Put,
        ApiMethod::Delete => Method::Delete,
    }
}

fn map_incoming_method(m: &Method) -> Option<ApiMethod> {
    match m {
        Method::Get => Some(ApiMethod::Get),
        Method::Post => Some(ApiMethod::Post),
        Method::Put => Some(ApiMethod::Put),
        Method::Delete => Some(ApiMethod::Delete),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// The Durable Object
// ---------------------------------------------------------------------------

struct NodeConfigCf {
    db_url: String,
    db_ns: String,
    db_db: String,
    db_auth: String,
    auth_secret: String,
    ssp_id: String,
    job_config: String, // SPKY_JOB_CONFIG JSON (outbox backend routing)
    ref_mode: ssp_protocol::RefMode,
    anonymous_live_queries: bool,
    impersonation: bool,
    query_allowlist: ssp::allowlist::Mode,
}

fn read_config(env: &Env) -> Result<NodeConfigCf> {
    let var = |k: &str| env.var(k).map(|v| v.to_string()).unwrap_or_default();
    let secret = |k: &str| env.secret(k).map(|v| v.to_string()).unwrap_or_default();
    Ok(NodeConfigCf {
        db_url: var("SPKY_DB_URL"),
        db_ns: var("SPKY_DB_NS"),
        db_db: var("SPKY_DB_DB"),
        db_auth: secret("SPKY_DB_AUTH"),
        auth_secret: secret("SPKY_AUTH_SECRET"),
        ssp_id: {
            let v = var("SPKY_SSP_ID");
            if v.is_empty() { "cf-ssp".to_string() } else { v }
        },
        job_config: var("SPKY_JOB_CONFIG"),
        // Must match the client's `refMode` (sp00ky.yml). Default Dedicated —
        // core's default (see ssp-node config.rs) and the common case; a
        // client/node mismatch wedges cross-session sync (views never register).
        ref_mode: match var("SPKY_REF_MODE").to_ascii_lowercase().as_str() {
            "single" => ssp_protocol::RefMode::Single,
            _ => ssp_protocol::RefMode::Dedicated,
        },
        // Must match the client's `enableAnonymousLiveQueries`, for the same
        // reason as `ref_mode` above: it decides which `_00_list_ref*` table an
        // anonymous registration writes to, while the client picks the table it
        // reads from its own flag. This was hardcoded false, so a free-plan
        // tenant that opted in had its signed-out sessions reading
        // `_00_list_ref_anon` while this node wrote the global `_00_list_ref` —
        // no rows, no realtime, nothing logged.
        anonymous_live_queries: matches!(
            var("SPKY_ANON_LIVE_QUERIES").trim().to_ascii_lowercase().as_str(),
            "1" | "true"
        ),
        impersonation: ssp_protocol::impersonation::env_enabled(Some(
            &var(ssp_protocol::impersonation::ENV_FLAG),
        )),
        query_allowlist: var("SPKY_QUERY_ALLOWLIST").parse().unwrap_or_default(),
    })
}

#[durable_object]
pub struct SspNodeDo {
    #[allow(dead_code)]
    state: State,
    env: Env,
    storage: Rc<Storage>,
    mux: Rc<RefCell<TimerMux>>,
    node: RefCell<Option<Arc<SspNode>>>,
    runtime: RefCell<Option<Runtime>>,
}

impl DurableObject for SspNodeDo {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        console_log!("[ssp-cf] DO::new");
        let storage = Rc::new(state.storage());
        Self {
            state,
            env,
            storage,
            mux: Rc::new(RefCell::new(TimerMux::new())),
            node: RefCell::new(None),
            runtime: RefCell::new(None),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        self.ensure_ready().await;
        let node = match self.node.borrow().as_ref() {
            Some(n) => n.clone(),
            None => return Response::error("node init failed", 500),
        };

        let Some(method) = map_incoming_method(&req.method()) else {
            return Response::error("method not allowed", 405);
        };
        let path = req.path();
        let bearer = req
            .headers()
            .get("Authorization")
            .ok()
            .flatten()
            .and_then(|h| h.strip_prefix("Bearer ").map(|s| s.to_string()));
        let body = req.text().await.unwrap_or_default();

        let api_req = ssp_node::ApiRequest {
            method,
            path,
            bearer,
            body: body.into_bytes().into(),
        };

        match node.route(api_req).await {
            Some(resp) => to_worker_response(resp),
            None => Response::error("not found", 404),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        self.ensure_ready().await;
        let runtime = match self.runtime.borrow().as_ref() {
            Some(r) => r.clone(),
            None => return Response::ok("not ready"),
        };
        let due = self.mux.borrow_mut().pop_due(now_epoch_ms());
        for kind in due {
            runtime.on_timer(kind).await;
        }
        Response::ok("ok")
    }
}

impl SspNodeDo {
    /// Build + bootstrap the node on first wake (or after eviction). Idempotent.
    async fn ensure_ready(&self) {
        if self.node.borrow().is_some() {
            return;
        }
        console_log!("[ssp-cf] ensure_ready: building node");
        let cfg = read_config(&self.env).unwrap_or(NodeConfigCf {
            db_url: String::new(),
            db_ns: String::new(),
            db_db: String::new(),
            db_auth: String::new(),
            auth_secret: String::new(),
            ssp_id: "cf-ssp".to_string(),
            job_config: String::new(),
            ref_mode: ssp_protocol::RefMode::Dedicated,
            anonymous_live_queries: false,
            impersonation: false,
            query_allowlist: ssp::allowlist::Mode::Off,
        });

        let http: Arc<dyn HttpClient> = Arc::new(CfHttp);
        let db = HttpSqlDb::new(
            http.clone(),
            &cfg.db_url,
            cfg.db_ns.clone(),
            cfg.db_db.clone(),
            cfg.db_auth.clone(),
        );

        let platform = Platform {
            db: Arc::new(db),
            http,
            scheduler: Arc::new(CfScheduler {
                mux: self.mux.clone(),
                storage: self.storage.clone(),
            }),
            spawner: Arc::new(CfSpawner),
            telemetry: Arc::new(NoopTelemetry),
            circuit_store: Arc::new(CfCircuitStore { storage: self.storage.clone() }),
        };

        console_log!("[ssp-cf] node built, bootstrapping (db_url_set={})", !cfg.db_url.is_empty());
        let node = Arc::new(build_node(platform, &cfg));
        let runtime = Runtime::new(node.clone());
        runtime.bootstrap().await;
        console_log!("[ssp-cf] bootstrap done");

        // Arm the periodic sweeps (each re-arms itself thereafter via on_timer).
        let sched = &node.platform.scheduler;
        sched.schedule(TimerKind::TtlCleanup, now_epoch_ms() + 60_000).await;
        sched.schedule(TimerKind::ViewMetricsFlush, now_epoch_ms() + 2000).await;
        sched.schedule(TimerKind::JobRecoverySweep, now_epoch_ms()).await;

        *self.node.borrow_mut() = Some(node);
        *self.runtime.borrow_mut() = Some(runtime);
    }
}

fn build_node(platform: Platform, cfg: &NodeConfigCf) -> SspNode {
    // `_job_rx` is dropped: there is no runner on Workers. The dispatcher
    // notices the closed channel on its first send and latches itself off, so
    // it never issues a drain query on a host that could not run the result.
    let (job_queue_tx, _job_rx) = tokio::sync::mpsc::channel(64);
    let edge_update_tx = ssp_node::edges::EdgePublisher::default();
    let job_config = Arc::new(ssp_node::jobs::JobConfig::from_json(&cfg.job_config));
    let job_control = ssp_node::jobs::JobControl::new();
    let job_dispatcher = Arc::new(ssp_node::jobs::JobDispatcher::new(
        Arc::clone(&platform.db),
        Arc::clone(&platform.spawner),
        Arc::clone(&platform.scheduler),
        job_queue_tx,
        job_control.clone(),
        Arc::clone(&job_config),
        cfg.ssp_id.clone(),
        true,
    ));
    SspNode {
        publication_gate: Arc::new(tokio::sync::Mutex::new(())),
        platform,
        status: Arc::new(RwLock::new(SspStatus::Bootstrapping)),
        // Merged views are a server-side memory optimisation; this shell
        // registers one view per client, so leave it off.
        merge_views: false,
        processor: Arc::new(RwLock::new(Circuit::new())),
        job_config,
        job_control,
        job_dispatcher,
        ssp_id: cfg.ssp_id.clone(),
        auth_secret: cfg.auth_secret.clone(),
        ref_mode: cfg.ref_mode,
        version: env!("CARGO_PKG_VERSION"),
        surrealdb_version: "external".to_string(),
        advertise_ip: None,
        info_env: vec![],
        start_epoch_ms: now_epoch_ms(),
        bootstrap_warnings: Arc::new(RwLock::new(Vec::new())),
        backend_health: None,
        crdt_cache: Arc::new(ssp_node::crdt::CrdtCache::new(
            8,
            ssp_node::crdt::CrdtAllowList::permissive(),
        )),
        view_metrics: Arc::new(RwLock::new(std::collections::HashMap::new())),
        edge_update_tx,
        anonymous_live_queries: cfg.anonymous_live_queries,
        impersonation: cfg.impersonation,
        query_allowlist: Arc::new(ssp_node::allowlist_state::QueryAllowlist::new(cfg.query_allowlist)),
        standalone: true,
        // No schedule engine on Workers: this shell drops `_job_rx`, so there is
        // no runner to execute anything a fired schedule would spawn. Scheduling
        // is VM/singlenode + cluster only until that changes.
        schedule_engine: None,
        ttl_cleanup_interval_secs: 60,
        view_metrics_flush_ms: 2000,
        bootstrap_page_size: 200,
        checkpoint_interval_secs: Some(300),
        max_snapshot_age_secs: 3600,
        last_heartbeat_seen: std::sync::Arc::new(std::sync::Mutex::new(None)),
    }
}

fn to_worker_response(resp: ssp_node::ApiResponse) -> Result<Response> {
    let (content_type, bytes) = match resp.body {
        ApiBody::Json(v) => ("application/json".to_string(), v.to_string().into_bytes()),
        ApiBody::Text { content_type, body } => (content_type.to_string(), body.into_bytes()),
    };
    let mut builder = ResponseBuilder::new().with_status(resp.status);
    for (name, value) in &resp.headers {
        builder = builder.with_header(name, value)?;
    }
    builder = builder.with_header("Content-Type", &content_type)?;
    Ok(builder.fixed(bytes))
}

// ---------------------------------------------------------------------------
// Top-level worker entry: forward every request to the project's DO instance.
// ---------------------------------------------------------------------------

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let name = req
        .headers()
        .get("X-Spooky-Project")
        .ok()
        .flatten()
        .unwrap_or_else(|| "default".to_string());
    let ns = env.durable_object("SSP_NODE")?;
    let stub = ns.id_from_name(&name)?.get_stub()?;
    // Surface DO errors to the client (they don't show in `wrangler tail` on
    // the parent script) so failures are diagnosable.
    match stub.fetch_with_request(req).await {
        Ok(resp) => Ok(resp),
        Err(e) => Response::error(format!("DO error: {e}"), 502),
    }
}
