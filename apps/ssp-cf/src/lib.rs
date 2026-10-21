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

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::RwLock;
use wasm_bindgen_futures::spawn_local;
use worker::*;

use ssp::circuit::Circuit;
use ssp_node::bucket::BucketService;
use ssp_node::ports::{
    CancelWatch, CircuitStore, CircuitStoreError, HttpClient, HttpError, LocalBoxFuture,
    ObjectMeta, ObjectStore, ObjectStoreError, OutboundRequest, OutboundResponse, ResumePoint,
    Scheduler, Spawner, StoredObject, TimerKind,
};
use ssp_node::{
    now_epoch_ms, ApiBody, HttpSqlDb, Method as ApiMethod, NoopTelemetry, Platform, Runtime,
    SspNode, SspStatus, TimerMux,
};

mod log;

/// The R2 binding holding the files of `BACKEND "r2"` buckets, bound by the
/// control plane when the project declares one.
const FILES_BINDING: &str = "SPKY_FILES";

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
        cancel: Option<CancelWatch>,
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

        // The timeout and the cancel watch both abort the fetch, so a hung DB
        // or backend cannot hold the request (or a bootstrap) open forever.
        // Cancel wins a tie, as the port contract asks.
        let controller = AbortController::default();
        let signal = controller.signal();
        let exchange = async {
            let mut resp = Fetch::Request(wreq).send_with_signal(&signal).await?;
            let status = resp.status_code();
            let body = resp.text().await?;
            Ok::<_, worker::Error>(OutboundResponse { status, body })
        };
        let cancelled = async {
            match cancel {
                Some(mut watch) => watch.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let timeout = req.timeout;
        tokio::select! {
            biased;
            _ = cancelled => {
                controller.abort();
                Err(HttpError::Cancelled)
            }
            result = exchange => result.map_err(|e| HttpError::Transport(e.to_string())),
            _ = Delay::from(timeout) => {
                controller.abort();
                Err(HttpError::Timeout(timeout))
            }
        }
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
/// [`TimerMux`], written through to the DO's SQLite on every change and read
/// back in `DO::new`, so a timer survives eviction. The platform alarm is set
/// only when the earliest deadline changes and deleted when nothing is
/// pending: an idle object then has no alarm and is evicted.
struct CfScheduler {
    mux: Rc<RefCell<TimerMux>>,
    storage: Rc<Storage>,
    /// Deadline the platform alarm is set to, `None` when none is set.
    armed_at: Cell<Option<u64>>,
}

const META_TABLE: &str = "CREATE TABLE IF NOT EXISTS ssp_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)";
const META_TIMERS: &str = "timers";
const META_ARMED_AT: &str = "armed_at";

fn meta_get(storage: &Storage, key: &str) -> Option<String> {
    let rows: Vec<serde_json::Value> = storage
        .sql()
        .exec("SELECT v FROM ssp_meta WHERE k = ?", vec![key.into()])
        .and_then(|cursor| cursor.to_array())
        .ok()?;
    rows.first()?.get("v")?.as_str().map(String::from)
}

fn meta_put(storage: &Storage, key: &str, value: Option<&str>) {
    let sql = storage.sql();
    let result = match value {
        Some(v) => sql.exec("INSERT OR REPLACE INTO ssp_meta (k, v) VALUES (?, ?)", vec![key.into(), v.into()]),
        None => sql.exec("DELETE FROM ssp_meta WHERE k = ?", vec![key.into()]),
    };
    if let Err(e) = result {
        tracing::warn!(error = %e, key, "persisting DO metadata failed");
    }
}

impl CfScheduler {
    /// Load the timers and the armed deadline a previous instance left.
    fn restore(storage: Rc<Storage>) -> Self {
        if let Err(e) = storage.sql().exec(META_TABLE, None) {
            tracing::warn!(error = %e, "creating ssp_meta failed; timers will not survive eviction");
        }
        let mux = meta_get(&storage, META_TIMERS)
            .and_then(|json| serde_json::from_str::<TimerMux>(&json).ok())
            .unwrap_or_default();
        let armed_at = meta_get(&storage, META_ARMED_AT).and_then(|v| v.parse().ok());
        Self { mux: Rc::new(RefCell::new(mux)), storage, armed_at: Cell::new(armed_at) }
    }

    fn persist_timers(&self) {
        match serde_json::to_string(&*self.mux.borrow()) {
            Ok(json) => meta_put(&self.storage, META_TIMERS, Some(&json)),
            Err(e) => tracing::warn!(error = %e, "serializing timers failed"),
        }
    }

    /// The alarm just fired, so the platform no longer holds one.
    fn alarm_consumed(&self) {
        self.armed_at.set(None);
        meta_put(&self.storage, META_ARMED_AT, None);
    }

    /// Fired timers leave the mux; `on_timer` re-adds what it re-arms.
    fn take_due(&self, now: u64) -> Vec<TimerKind> {
        let due = self.mux.borrow_mut().pop_due(now);
        if !due.is_empty() {
            self.persist_timers();
        }
        due
    }

    async fn rearm_alarm(&self) {
        let next = self.mux.borrow().next_deadline();
        match next {
            None => {
                if self.armed_at.get().is_some() {
                    let _ = self.storage.delete_alarm().await;
                    self.armed_at.set(None);
                    meta_put(&self.storage, META_ARMED_AT, None);
                    tracing::debug!("alarm deleted: no timer pending");
                }
            }
            Some(at) if self.armed_at.get() != Some(at) => {
                let offset = (at as i64 - now_epoch_ms() as i64).max(0);
                match self.storage.set_alarm(offset).await {
                    Ok(()) => {
                        self.armed_at.set(Some(at));
                        meta_put(&self.storage, META_ARMED_AT, Some(&at.to_string()));
                        tracing::debug!(in_ms = offset, "alarm set");
                    }
                    Err(e) => tracing::warn!(error = %e, "setting the alarm failed"),
                }
            }
            Some(_) => {}
        }
    }
}

#[async_trait(?Send)]
impl Scheduler for CfScheduler {
    async fn schedule(&self, kind: TimerKind, at_epoch_ms: u64) {
        self.mux.borrow_mut().schedule(kind, at_epoch_ms);
        self.persist_timers();
        self.rearm_alarm().await;
    }
    async fn cancel(&self, kind: &TimerKind) {
        let had = self.mux.borrow().len();
        self.mux.borrow_mut().cancel(kind);
        if self.mux.borrow().len() != had {
            self.persist_timers();
            self.rearm_alarm().await;
        }
    }
    async fn ensure(&self, kind: TimerKind, at_epoch_ms: u64) {
        if self.mux.borrow_mut().schedule_min(kind, at_epoch_ms) {
            self.persist_timers();
            self.rearm_alarm().await;
        }
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

/// `ObjectStore` over an R2 binding: the files of `BACKEND "r2"` buckets.
struct R2Store(Bucket);

fn r2_err(e: worker::Error) -> ObjectStoreError {
    ObjectStoreError::Transport(e.to_string())
}

fn r2_meta(object: &Object) -> ObjectMeta {
    ObjectMeta {
        size: object.size(),
        content_type: object.http_metadata().content_type,
        updated_epoch_ms: Some(object.uploaded().as_millis()),
    }
}

#[async_trait(?Send)]
impl ObjectStore for R2Store {
    async fn get(&self, key: &str) -> std::result::Result<Option<StoredObject>, ObjectStoreError> {
        let Some(object) = self.0.get(key).execute().await.map_err(r2_err)? else {
            return Ok(None);
        };
        let meta = r2_meta(&object);
        let Some(body) = object.body() else { return Ok(None) };
        let bytes = body.bytes().await.map_err(r2_err)?;
        Ok(Some(StoredObject { meta, body: bytes.into() }))
    }

    async fn head(&self, key: &str) -> std::result::Result<Option<ObjectMeta>, ObjectStoreError> {
        Ok(self.0.head(key).await.map_err(r2_err)?.as_ref().map(r2_meta))
    }

    async fn put(&self, key: &str, body: bytes::Bytes, content_type: &str) -> std::result::Result<(), ObjectStoreError> {
        self.0
            .put(key, body.to_vec())
            .http_metadata(HttpMetadata { content_type: Some(content_type.to_string()), ..Default::default() })
            .execute()
            .await
            .map_err(r2_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> std::result::Result<(), ObjectStoreError> {
        self.0.delete(key).await.map_err(r2_err)
    }
}

/// `/bucket/*` (see `ssp_node::bucket`), answered by this isolate itself: no
/// node state is involved, and going through the Durable Object would queue
/// every upload behind ingest on its single thread.
async fn bucket_route(mut req: Request, env: &Env) -> Result<Response> {
    if req.method() == Method::Options {
        return to_worker_response(BucketService::preflight());
    }
    let Ok(files) = env.bucket(FILES_BINDING) else {
        return Response::error("no R2 bucket bound to this SSP (SPKY_FILES)", 501);
    };
    let Some(method) = map_incoming_method(&req.method()) else {
        return Response::error("method not allowed", 405);
    };
    let var = |k: &str| env.var(k).map(|v| v.to_string()).unwrap_or_default();
    let service = BucketService::new(
        Arc::new(R2Store(files)),
        Arc::new(CfHttp),
        var("SPKY_DB_URL"),
        var("SPKY_DB_NS"),
        var("SPKY_DB_DB"),
    );
    let bearer = req
        .headers()
        .get("Authorization")
        .ok()
        .flatten()
        .and_then(|h| h.strip_prefix("Bearer ").map(|s| s.to_string()));
    let body = req.bytes().await.unwrap_or_default();
    let api_req = ssp_node::ApiRequest { method, path: req.path(), bearer, body: body.into() };
    to_worker_response(service.handle(api_req).await)
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
    scheduler: Arc<CfScheduler>,
    node: RefCell<Option<Arc<SspNode>>>,
    runtime: RefCell<Option<Runtime>>,
    /// Single flight for the cold build: a DB fetch opens the input gate, so a
    /// second request would otherwise build (and hold in memory) a second
    /// circuit while the first is still bootstrapping.
    boot: tokio::sync::Mutex<()>,
}

/// Longest wait between bootstrap retries.
const MAX_BOOTSTRAP_BACKOFF_MS: u64 = 60_000;

impl DurableObject for SspNodeDo {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        log::init(env.var("SPKY_LOG").ok().map(|v| v.to_string()));
        let storage = Rc::new(state.storage());
        let scheduler = Arc::new(CfScheduler::restore(storage.clone()));
        tracing::debug!(timers = scheduler.mux.borrow().len(), "DO::new");
        Self {
            state,
            env,
            storage,
            scheduler,
            node: RefCell::new(None),
            runtime: RefCell::new(None),
            boot: tokio::sync::Mutex::new(()),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        self.ensure_ready().await;
        let (node, runtime) = match (self.node.borrow().clone(), self.runtime.borrow().clone()) {
            (Some(n), Some(r)) => (n, r),
            _ => return Response::error("node init failed", 500),
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
            path: path.clone(),
            bearer,
            body: body.into_bytes().into(),
        };

        let routed = node.route(api_req).await;
        // Arm what the request made pending (dirty metrics, CRDT rows, a new
        // view's expiry, outbox rows); nothing when it changed nothing.
        runtime.after_request(&path).await;
        match routed {
            Some(resp) => to_worker_response(resp),
            None => Response::error("not found", 404),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        self.scheduler.alarm_consumed();
        self.ensure_ready().await;
        let runtime = match self.runtime.borrow().as_ref() {
            Some(r) => r.clone(),
            None => return Response::ok("not ready"),
        };
        for kind in self.scheduler.take_due(now_epoch_ms()) {
            tracing::debug!(?kind, "timer fired");
            match kind {
                TimerKind::BootstrapRetry { attempt } => {
                    // The wake itself may just have built the node Ready.
                    if *runtime.node().status.read().await != SspStatus::Ready {
                        runtime.bootstrap().await;
                        self.after_bootstrap(&runtime, attempt).await;
                    }
                }
                other => runtime.on_timer(other).await,
            }
        }
        self.scheduler.rearm_alarm().await;
        Response::ok("ok")
    }
}

impl SspNodeDo {
    /// Build + bootstrap the node on first wake (or after eviction). Idempotent
    /// and single-flight.
    async fn ensure_ready(&self) {
        if self.node.borrow().is_some() {
            return;
        }
        let _boot = self.boot.lock().await;
        if self.node.borrow().is_some() {
            return;
        }
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
            scheduler: self.scheduler.clone(),
            spawner: Arc::new(CfSpawner),
            telemetry: Arc::new(NoopTelemetry),
            circuit_store: Arc::new(CfCircuitStore { storage: self.storage.clone() }),
        };

        tracing::info!(db_url_set = !cfg.db_url.is_empty(), "building node");
        let node = Arc::new(build_node(platform, &cfg));
        let runtime = Runtime::on_demand(node.clone());
        runtime.bootstrap().await;
        *self.node.borrow_mut() = Some(node);
        *self.runtime.borrow_mut() = Some(runtime.clone());
        self.after_bootstrap(&runtime, 0).await;
    }

    /// Ready: arm what the node has pending. Failed: retry with backoff
    /// (1 s, 2 s, 4 s, ... up to a minute) instead of staying failed until
    /// eviction, which is what a failed bootstrap did before.
    async fn after_bootstrap(&self, runtime: &Runtime, attempt: u32) {
        if *runtime.node().status.read().await == SspStatus::Ready {
            tracing::info!(attempt, "bootstrap complete");
            runtime.after_boot().await;
            return;
        }
        let delay = (1000u64 << attempt.min(16)).min(MAX_BOOTSTRAP_BACKOFF_MS);
        tracing::warn!(attempt, retry_in_ms = delay, "bootstrap failed; retrying");
        self.scheduler
            .schedule(TimerKind::BootstrapRetry { attempt: attempt + 1 }, now_epoch_ms() + delay)
            .await;
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
        crdt: Arc::new(ssp_node::crdt::CrdtFold::new(
            ssp_node::crdt::FoldConfig::default(),
            cfg.ssp_id.clone(),
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
        push_engine: None,
        ttl_cleanup_interval_secs: 60,
        // Off: a 15 s alarm would keep the Durable Object from ever
        // hibernating. Registrations naming an unknown table still refresh.
        schema_poll_secs: 0,
        schema_watch: Default::default(),
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
        ApiBody::Bytes { content_type, body } => (content_type, body.to_vec()),
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
    if BucketService::handles(&req.path()) {
        return bucket_route(req, &env).await;
    }
    let name = req
        .headers()
        .get("X-Spooky-Project")
        .ok()
        .flatten()
        .unwrap_or_else(|| "default".to_string());
    let ns = env.durable_object("SSP_NODE")?;
    let id = ns.id_from_name(&name)?;
    // Place the object near its database: from the US every Hetzner round trip
    // cost ~175 ms and Ready missed its budget; near the DB it is ~22 ms
    // (spike S1). A hint only applies when the object is first created.
    let hint = env.var("SPKY_DO_LOCATION_HINT").map(|v| v.to_string()).unwrap_or_default();
    let stub = if hint.is_empty() { id.get_stub()? } else { id.get_stub_with_location_hint(&hint)? };
    // Surface DO errors to the client (they don't show in `wrangler tail` on
    // the parent script) so failures are diagnosable.
    match stub.fetch_with_request(req).await {
        Ok(resp) => Ok(resp),
        Err(e) => Response::error(format!("DO error: {e}"), 502),
    }
}
