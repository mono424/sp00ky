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
    CancelWatch, HttpClient, HttpError, LocalBoxFuture, NoopCircuitStore, ObjectMeta,
    ObjectStore, ObjectStoreError, OutboundRequest, OutboundResponse, Scheduler, Spawner,
    StoredObject, TimerKind,
};
use ssp_node::{
    now_epoch_ms, ApiBody, HttpSqlDb, Method as ApiMethod, NoopTelemetry, Platform, Runtime,
    SspNode, SspStatus, TimerMux,
};

mod log;
mod snapshot;

/// The R2 binding holding the files of `BACKEND "r2"` buckets, bound by the
/// control plane when the project declares one.
const FILES_BINDING: &str = "SPKY_FILES";

/// Folded `@crdt` documents kept in memory between folds. A cached document
/// is 36-48 KB (measured natively: a short title, and 2,000 typed characters
/// with their history), so the VM default of 1,024 could take 40 MB or more of
/// the 128 MB isolate. A miss imports the canonical snapshot the fold's claim
/// already returns, so it costs CPU, not a round trip.
const CRDT_CACHE_DOCS: usize = 64;

/// Where the JSON circuit snapshot used to live (Durable Object KV), removed
/// on boot: the row images in SQLite (`snapshot.rs`) replaced it.
const LEGACY_SNAPSHOT_KEYS: [&str; 2] = ["ssp:circuit_blob", "ssp:resume_point"];

// ---------------------------------------------------------------------------
// Port adapters
// ---------------------------------------------------------------------------

/// `HttpClient` over the Workers `fetch` global. Fills BOTH the job-dispatch
/// port and (indirectly) the `Db` port via `HttpSqlDb`.
struct CfHttp;

/// Outbound requests in flight on this object. Its requests share one I/O
/// context, and at most six fetches at a time may wait for response headers
/// (a SurrealDB answer has none until the query is done), so a few slow
/// queries hold up every other one.
static HTTP_IN_FLIGHT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// An outbound request slower than this is logged with what else was in flight.
const SLOW_HTTP_MS: u64 = 5_000;

/// The first words of the first statement after the bind `LET`s: enough to
/// tell a fold commit from a TTL delete, never a bound value.
fn statement_hint(body: Option<&serde_json::Value>) -> String {
    let Some(query) = body.and_then(|b| b.get("params")).and_then(|p| p.get(0)).and_then(|q| q.as_str()) else {
        return String::new();
    };
    query
        .split(";\n")
        .map(str::trim)
        .find(|stmt| !stmt.is_empty() && !stmt.starts_with("LET $"))
        .map(|stmt| stmt.chars().take(48).collect())
        .unwrap_or_default()
}

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

        // A job for a Worker backend (`worker://<name>/<path>`) never touches
        // the network: host.mjs loads that backend's Dynamic Worker and calls
        // its fetch. The backend sees an ordinary https URL with the job path.
        let backend = worker_backend_target(&req.url);
        let url = match &backend {
            Some((name, path)) => format!("https://{name}.worker.spky.internal{path}"),
            None => req.url.clone(),
        };
        let wreq = Request::new_with_init(&url, &init)
            .map_err(|e| HttpError::Transport(e.to_string()))?;

        // The timeout and the cancel watch both abort the fetch, so a hung DB
        // or backend cannot hold the request (or a bootstrap) open forever.
        // Cancel wins a tie, as the port contract asks.
        let controller = AbortController::default();
        let signal = controller.signal();
        let exchange = async {
            let mut resp = match &backend {
                Some((name, _)) => worker_backend_fetch(name, &wreq).await?,
                None => Fetch::Request(wreq).send_with_signal(&signal).await?,
            };
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
        let in_flight = HTTP_IN_FLIGHT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        let started = now_epoch_ms();
        let result = tokio::select! {
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
        };
        HTTP_IN_FLIGHT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        let elapsed = now_epoch_ms().saturating_sub(started);
        if elapsed >= SLOW_HTTP_MS {
            let path = req.url.split('?').next().unwrap_or_default().rsplit('/').next().unwrap_or_default();
            tracing::warn!(
                elapsed_ms = elapsed,
                in_flight_at_start = in_flight,
                in_flight_now = HTTP_IN_FLIGHT.load(std::sync::atomic::Ordering::Relaxed),
                path,
                statement = %statement_hint(req.json_body.as_ref()),
                ok = result.is_ok(),
                "slow outbound request"
            );
        }
        result
    }
}

/// The backend name and path of a `worker://<name>/<path>` job URL (the
/// `base_url` the CLI writes into `SPKY_JOB_CONFIG` for a Worker backend).
fn worker_backend_target(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("worker://")?;
    let (name, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    (!name.is_empty()).then(|| (name.to_string(), path.to_string()))
}

#[wasm_bindgen::prelude::wasm_bindgen]
extern "C" {
    /// `globalThis.__spkyBackends.fetch`, installed by host.mjs.
    #[wasm_bindgen(catch, js_namespace = __spkyBackends, js_name = fetch)]
    fn spky_backend_fetch(
        name: &str,
        request: &web_sys::Request,
    ) -> std::result::Result<js_sys::Promise, wasm_bindgen::JsValue>;
}

/// Run a job request on a Worker backend through host.mjs.
async fn worker_backend_fetch(name: &str, req: &Request) -> worker::Result<Response> {
    use wasm_bindgen::JsCast;
    let promise = spky_backend_fetch(name, req.inner()).map_err(|e| {
        Error::JsError(format!(
            "Worker backend `{name}` is unreachable: this ssp-cf build was uploaded without host.mjs ({e:?})"
        ))
    })?;
    let value = wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .map_err(|e| Error::JsError(format!("Worker backend `{name}` failed: {e:?}")))?;
    let resp: web_sys::Response = value
        .dyn_into()
        .map_err(|_| Error::JsError(format!("Worker backend `{name}` did not answer with a Response")))?;
    Ok(Response::from(resp))
}

/// `push_core::PushHttp` over `fetch`: Web Push, FCM (and its OAuth token)
/// and APNs, whose HTTP/2 the platform's fetch speaks (spike S7). The body
/// is raw bytes (an encrypted Web Push payload is not JSON).
struct CfPushHttp;

/// Longest a single push request may take.
const PUSH_TIMEOUT: Duration = Duration::from_secs(30);

#[async_trait(?Send)]
impl push_core::PushHttp for CfPushHttp {
    async fn post(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> std::result::Result<(u16, String), String> {
        let h = Headers::new();
        for (k, v) in &headers {
            h.set(k, v).map_err(|e| e.to_string())?;
        }
        let mut init = RequestInit::new();
        init.with_method(Method::Post)
            .with_headers(h)
            .with_body(Some(js_sys::Uint8Array::from(&body[..]).into()));
        let req = Request::new_with_init(url, &init).map_err(|e| e.to_string())?;
        let controller = AbortController::default();
        let signal = controller.signal();
        let exchange = async {
            let mut resp = Fetch::Request(req).send_with_signal(&signal).await?;
            let status = resp.status_code();
            let text = resp.text().await.unwrap_or_default();
            Ok::<_, worker::Error>((status, text))
        };
        tokio::select! {
            biased;
            result = exchange => result.map_err(|e| e.to_string()),
            _ = Delay::from(PUSH_TIMEOUT) => {
                controller.abort();
                Err(format!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
            }
        }
    }
}

/// The push engine, as a standalone VM SSP builds it, over fetch. On demand:
/// the runtime ticks it when there is work (`TimerKind::PushTick`).
fn build_push_engine(env: &Env, db: Arc<dyn ssp_node::ports::Db>) -> Arc<push_core::PushEngine> {
    let get = |k: &str| {
        env.secret(k)
            .map(|v| v.to_string())
            .or_else(|_| env.var(k).map(|v| v.to_string()))
            .ok()
            .filter(|v| !v.is_empty())
    };
    let opts = push_core::EngineOptions::from_env(get);
    let keys = push_core::PushEngine::keys_from_env(get);
    Arc::new(push_core::PushEngine::new(
        Arc::new(ssp_node::schedules::PortDb(db)),
        Arc::new(CfPushHttp),
        keys,
        opts,
    ))
}

/// `Spawner` over `wasm_bindgen_futures::spawn_local`.
struct CfSpawner;
impl Spawner for CfSpawner {
    fn spawn(&self, fut: LocalBoxFuture) {
        spawn_local(fut);
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
        if self.mux.borrow().is_empty() {
            if self.armed_at.get().is_some() {
                let _ = self.storage.delete_alarm().await;
                self.armed_at.set(None);
                meta_put(&self.storage, META_ARMED_AT, None);
                tracing::debug!("alarm deleted: no timer pending");
            }
            return;
        }
        let now = now_epoch_ms();
        let Some(at) = self.mux.borrow().alarm_to_arm(self.armed_at.get(), now) else { return };
        if let Some(armed) = self.armed_at.get().filter(|armed| now > armed.saturating_add(ssp_node::timers::ALARM_LOST_AFTER_MS)) {
            tracing::warn!(late_ms = now - armed, "the armed alarm never fired; arming it again");
        }
        let offset = (at as i64 - now as i64).max(0);
        match self.storage.set_alarm(offset).await {
            Ok(()) => {
                self.armed_at.set(Some(at));
                meta_put(&self.storage, META_ARMED_AT, Some(&at.to_string()));
                tracing::debug!(in_ms = offset, "alarm set");
            }
            Err(e) => tracing::warn!(error = %e, "setting the alarm failed"),
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
    // Both re-arm even when the table did not change: `alarm_to_arm` is
    // what notices an alarm lost with an invocation that died mid-run.
    async fn cancel(&self, kind: &TimerKind) {
        let had = self.mux.borrow().len();
        self.mux.borrow_mut().cancel(kind);
        if self.mux.borrow().len() != had {
            self.persist_timers();
        }
        self.rearm_alarm().await;
    }
    async fn ensure(&self, kind: TimerKind, at_epoch_ms: u64) {
        if self.mux.borrow_mut().schedule_min(kind, at_epoch_ms) {
            self.persist_timers();
        }
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
    admin: ssp_node::admin::AdminSettings,
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
        admin: ssp_node::admin::AdminSettings {
            password: Some(secret("SPKY_ADMIN_PASSWORD")).filter(|p| !p.is_empty()),
            access: Some(var("SPKY_ADMIN_ACCESS")).filter(|a| !a.is_empty()),
            project_slug: Some(var("SPKY_PROJECT_SLUG")).filter(|s| !s.is_empty()),
        },
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
    /// Change notices accepted but not yet applied (rows in `ssp_inbox`).
    inbox_len: Cell<u64>,
    /// Single flight for draining the inbox.
    drain: tokio::sync::Mutex<()>,
    /// Table images + journal for the warm start.
    snapshot: snapshot::CfSnapshot,
    /// Until when the keep-awake timer holds the object in memory, and
    /// whether that timer is running (see [`AWAKE_AFTER_REQUEST_MS`]).
    awake_until: Rc<Cell<u64>>,
    awake_timer: Rc<Cell<bool>>,
}

/// The routes a database event calls. They are acknowledged durably even
/// while the node cannot apply them, so a user's write never fails because
/// the SSP is cold, bootstrapping or busy publishing.
fn is_notice_route(path: &str) -> bool {
    matches!(path, "/ingest" | "/ingest/batch" | "/view/unregister")
}

/// Change notices in arrival order. `applied = 0` rows are the inbox (accepted,
/// not yet in the circuit); applied rows are the journal a warm start re-reads
/// after the table images, kept until every image covers them.
const INBOX_TABLE: &str = "CREATE TABLE IF NOT EXISTS ssp_inbox \
    (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, body TEXT NOT NULL, \
     applied INTEGER NOT NULL DEFAULT 0)";
/// The column phase 2 (canary.337) did not have; fails harmlessly once added.
const INBOX_APPLIED_COLUMN: &str = "ALTER TABLE ssp_inbox ADD COLUMN applied INTEGER NOT NULL DEFAULT 0";

/// Bytes of table images one checkpoint event writes (at least one table):
/// the writes hold the object's responses until they are on disk (spike S1:
/// about 0.4 s for 20 MiB in one event).
const CHECKPOINT_BUDGET_BYTES: u64 = 4 << 20;
/// A checkpoint runs this long after the first change since the last one
/// (later changes never postpone it: an evicted object's alarm wakes a fresh
/// instance, whose own arming must not push a due checkpoint out)...
const CHECKPOINT_AFTER_CHANGE_MS: u64 = 30_000;
/// ...and this soon after a bootstrap, while the circuit is in memory: a cold
/// rebuild has no images yet, a warm start has caught some tables up.
const CHECKPOINT_AFTER_BOOT_MS: u64 = 2_000;
/// Retry after a failed checkpoint.
const CHECKPOINT_RETRY_MS: u64 = 60_000;
/// Applied journal rows kept at most. Past it (checkpoints keep failing) the
/// snapshot is dropped with them, and the next cold start rebuilds.
const MAX_JOURNAL_ROWS: u64 = 200_000;
/// A snapshot older than this rebuilds (writes that bypass the events are
/// not in the journal).
const MAX_SNAPSHOT_AGE_SECS: u64 = 7 * 24 * 3600;

/// Rows read per drain pass.
const DRAIN_PAGE: usize = 200;
/// Wait before retrying a replay that failed.
const REPLAY_RETRY_MS: u64 = 5_000;

/// Longest wait between bootstrap retries.
const MAX_BOOTSTRAP_BACKOFF_MS: u64 = 60_000;

/// How long the object stays in memory after a request while it serves views.
/// With no timer pending, Cloudflare hibernates an object 10 s after its last
/// event, and the next request builds the node again: images, every view
/// re-registered and republished (95 views took 1.5 s, 341 took 9 s on
/// threads-e2e, 2026-10-22, while every request waited). A pause of ten
/// seconds between a user's actions or between test specs was enough. A
/// pending timer makes the object non-hibernateable; without requests for
/// this long it lets go, and an idle project is still evicted. Alarms do not
/// extend it, so open tabs that only heartbeat do not keep it forever.
const AWAKE_AFTER_REQUEST_MS: u64 = 5 * 60_000;

fn inbox_count(storage: &Storage) -> u64 {
    storage
        .sql()
        .exec("SELECT COUNT(*) AS n FROM ssp_inbox WHERE applied = 0", None)
        .and_then(|c| c.to_array::<serde_json::Value>())
        .ok()
        .and_then(|rows| rows.first().and_then(|r| r.get("n")).and_then(serde_json::Value::as_u64))
        .unwrap_or(0)
}

fn json_response(status: u16, body: serde_json::Value) -> Result<Response> {
    Ok(Response::from_json(&body)?.with_status(status))
}

impl DurableObject for SspNodeDo {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        log::init(env.var("SPKY_LOG").ok().map(|v| v.to_string()));
        let storage = Rc::new(state.storage());
        let scheduler = Arc::new(CfScheduler::restore(storage.clone()));
        if let Err(e) = storage.sql().exec(INBOX_TABLE, None) {
            tracing::error!(error = %e, "creating ssp_inbox failed");
        }
        let _ = storage.sql().exec(INBOX_APPLIED_COLUMN, None);
        if let Err(e) = storage.sql().exec(snapshot::TABLES, None) {
            tracing::error!(error = %e, "creating the snapshot tables failed");
        }
        let inbox_len = Cell::new(inbox_count(&storage));
        let snapshot = snapshot::CfSnapshot { storage: storage.clone() };
        tracing::debug!(timers = scheduler.mux.borrow().len(), inbox = inbox_len.get(), "DO::new");
        Self {
            state,
            env,
            storage,
            scheduler,
            node: RefCell::new(None),
            runtime: RefCell::new(None),
            boot: tokio::sync::Mutex::new(()),
            inbox_len,
            drain: tokio::sync::Mutex::new(()),
            snapshot,
            awake_until: Rc::new(Cell::new(0)),
            awake_timer: Rc::new(Cell::new(false)),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let path = req.path();
        if req.method() == Method::Post && is_notice_route(&path) {
            return self.accept_notice(req, path).await;
        }
        self.ensure_ready().await;
        // A queued unregister lands before this request: a re-registration of
        // its id must not be released by it afterwards. Queued row changes
        // are the replay timer's: a view registered before them gets their
        // deltas when they land. Waiting for them made `/view/register`
        // (which `fn::query::register` awaits with no timeout) replay the
        // whole inbox first, a database read per page, for minutes behind a
        // mass delete.
        if self.inbox_len.get() > 0 {
            if self.unregister_queued() {
                self.drain_inbox().await;
            } else {
                self.scheduler.ensure(TimerKind::IngestReplay, now_epoch_ms()).await;
            }
        }
        let (node, runtime) = match (self.node.borrow().clone(), self.runtime.borrow().clone()) {
            (Some(n), Some(r)) => (n, r),
            _ => return Response::error("node init failed", 500),
        };

        let Some(method) = map_incoming_method(&req.method()) else {
            return Response::error("method not allowed", 405);
        };
        let bearer = req
            .headers()
            .get("Authorization")
            .ok()
            .flatten()
            .and_then(|h| h.strip_prefix("Bearer ").map(|s| s.to_string()));
        let body = req.text().await.unwrap_or_default();
        // The admin API reads its filters from the query string; every other
        // route ignores it, and its path is what timers key on.
        let path = match req.url().ok().and_then(|u| u.query().map(str::to_string)) {
            Some(q) if ssp_node::admin::is_admin_path(&path) && !q.is_empty() => format!("{path}?{q}"),
            _ => path,
        };

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
        self.stay_awake();
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
                        runtime.bootstrap_with(Some(&self.snapshot)).await;
                        self.after_bootstrap(&runtime, attempt).await;
                    }
                }
                TimerKind::IngestReplay => self.drain_inbox().await,
                TimerKind::CircuitCheckpoint => self.checkpoint().await,
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
            admin: Default::default(),
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
            circuit_store: Arc::new(NoopCircuitStore),
        };

        tracing::info!(db_url_set = !cfg.db_url.is_empty(), "building node");
        let push_engine = build_push_engine(&self.env, Arc::clone(&platform.db));
        let (node, job_rx) = build_node(platform, &cfg, push_engine);
        let node = Arc::new(node);
        if let Some(job_rx) = job_rx {
            // Outbox jobs run in this object, as on a standalone VM SSP: the
            // runner is a task on this isolate, each job an HTTP call to its
            // backend. A job in flight when the object is evicted runs again
            // (at least once): the recovery sweep re-admits it.
            node.job_dispatcher.preload_policies().await;
            let runner = ssp_node::jobs::JobRunner::new(
                job_rx,
                Arc::clone(&node.platform.db),
                Arc::clone(&node.platform.http),
                Arc::clone(&node.platform.scheduler),
                Arc::clone(&node.platform.spawner),
                Arc::clone(&node.job_dispatcher),
            );
            spawn_local(runner.run());
            tracing::info!(tables = node.job_config.job_tables.len(), "job runner started");
        }
        let runtime = Runtime::on_demand(node.clone());
        for key in LEGACY_SNAPSHOT_KEYS {
            let _ = self.storage.delete(key).await;
        }
        runtime.bootstrap_with(Some(&self.snapshot)).await;
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
            // Arming the TTL, job and CRDT timers reads the DB; none of it is
            // needed to answer, and awaiting it held the first request after a
            // cold start for about 0.4 s (measured on Cloudflare).
            let armer = runtime.clone();
            spawn_local(async move { armer.after_boot().await });
            // Images for the next cold start (only the tables that moved).
            self.arm_checkpoint(CHECKPOINT_AFTER_BOOT_MS).await;
            if self.inbox_len.get() > 0 {
                self.drain_inbox().await;
            }
            return;
        }
        let delay = (1000u64 << attempt.min(16)).min(MAX_BOOTSTRAP_BACKOFF_MS);
        tracing::warn!(attempt, retry_in_ms = delay, "bootstrap failed; retrying");
        self.scheduler
            .schedule(TimerKind::BootstrapRetry { attempt: attempt + 1 }, now_epoch_ms() + delay)
            .await;
    }

    /// A database event's call. Applied right away when the node is Ready and
    /// nothing is queued ahead of it; otherwise stored durably (the output
    /// gate holds the 200 until the write is on disk) and replayed once the
    /// node can take it. Either way the user's transaction commits.
    async fn accept_notice(&self, mut req: Request, path: String) -> Result<Response> {
        let secret = self.env.secret("SPKY_AUTH_SECRET").map(|s| s.to_string()).unwrap_or_default();
        let bearer = req
            .headers()
            .get("Authorization")
            .ok()
            .flatten()
            .and_then(|h| h.strip_prefix("Bearer ").map(|s| s.to_string()));
        if secret.is_empty() || bearer.as_deref() != Some(secret.as_str()) {
            return json_response(401, serde_json::json!({ "code": "unauthorized" }));
        }
        let body = req.bytes().await.unwrap_or_default();
        let (kind, stored) = match compact_for_inbox(&path, &body) {
            Ok(v) => v,
            Err(e) => return json_response(400, serde_json::json!({ "code": "invalid_notice", "message": e })),
        };
        let queued_ahead = self.inbox_len.get() > 0;
        // Journaled first, applied or not: a warm start re-reads what was
        // noticed after its images, so every notice has to be on disk.
        let seq = match self
            .storage
            .sql()
            .exec(
                "INSERT INTO ssp_inbox (kind, body) VALUES (?, ?) RETURNING seq",
                vec![kind.into(), stored.into()],
            )
            .and_then(|c| c.to_array::<serde_json::Value>())
        {
            Ok(rows) => rows.first().and_then(|r| r.get("seq")).and_then(serde_json::Value::as_i64).unwrap_or(0),
            Err(e) => {
                // Not stored, so not acknowledged: the DB event fails and the
                // client retries the write.
                tracing::error!(error = %e, "journaling a change notice failed");
                return json_response(503, serde_json::json!({ "code": "inbox_unavailable" }));
            }
        };

        let ready = match (self.node.borrow().clone(), self.runtime.borrow().clone()) {
            (Some(node), Some(runtime)) => Some((node, runtime)),
            _ => None,
        };
        // An unregister does not wait behind queued changes: it releases the
        // view and queues its edge drop, nothing a slow database or a full
        // publication queue can hold up, and its order against row changes
        // does not matter (they only produce deltas the released view drops).
        // Queued, a registration of the same id would have to replay every
        // change ahead of it first.
        let unregister = kind == "unregister";
        if let Some((node, runtime)) = ready {
            if (!queued_ahead || unregister) && *node.status.read().await == SspStatus::Ready {
                let api_req = ssp_node::ApiRequest {
                    method: ApiMethod::Post,
                    path: path.clone(),
                    bearer,
                    body: body.clone().into(),
                };
                match node.route(api_req).await {
                    // A full publication queue or a restarting circuit: keep it
                    // queued instead of failing the write.
                    Some(resp) if resp.status >= 500 => {}
                    Some(resp) => {
                        // Applied, or refused for what it is (a retry would
                        // get the same answer): nothing left to replay.
                        self.mark_applied(seq);
                        runtime.after_request(&path).await;
                        self.arm_checkpoint(CHECKPOINT_AFTER_CHANGE_MS).await;
                        self.stay_awake();
                        return to_worker_response(resp);
                    }
                    None => {
                        self.mark_applied(seq);
                        return Response::error("not found", 404);
                    }
                }
            }
        }

        self.inbox_len.set(self.inbox_len.get() + 1);
        // Wake as soon as possible: bootstrap (if cold), then replay.
        self.scheduler.ensure(TimerKind::IngestReplay, now_epoch_ms()).await;
        self.stay_awake();
        json_response(200, serde_json::json!({ "status": "queued" }))
    }

    /// Hold the object in memory for [`AWAKE_AFTER_REQUEST_MS`] past this
    /// request while the node serves views. One timer task at a time: it
    /// sleeps to the deadline, and again while requests keep moving it. A
    /// node without views lets the object hibernate as before.
    fn stay_awake(&self) {
        let serving = self.node.borrow().as_ref().is_some_and(|node| {
            // Busy (a writer holds it): it is serving, so stay.
            node.processor.try_read().map_or(true, |circuit| circuit.view_count() > 0)
        });
        if !serving {
            return;
        }
        self.awake_until.set(now_epoch_ms() + AWAKE_AFTER_REQUEST_MS);
        if self.awake_timer.replace(true) {
            return;
        }
        let until = Rc::clone(&self.awake_until);
        let running = Rc::clone(&self.awake_timer);
        spawn_local(async move {
            loop {
                let left = until.get().saturating_sub(now_epoch_ms());
                if left == 0 {
                    break;
                }
                Delay::from(Duration::from_millis(left)).await;
            }
            running.set(false);
        });
    }

    /// Whether an unregister is still in the inbox (accepted while the node
    /// was not Ready).
    fn unregister_queued(&self) -> bool {
        self.storage
            .sql()
            .exec("SELECT 1 FROM ssp_inbox WHERE applied = 0 AND kind = 'unregister' LIMIT 1", None)
            .and_then(|c| c.to_array::<serde_json::Value>())
            .map(|rows| !rows.is_empty())
            // Unknown: drain, as before.
            .unwrap_or(true)
    }

    fn mark_applied(&self, seq: i64) {
        if let Err(e) = self
            .storage
            .sql()
            .exec("UPDATE ssp_inbox SET applied = 1 WHERE seq = ?", vec![seq.into()])
        {
            tracing::warn!(error = %e, seq, "marking a notice applied failed");
        }
    }

    /// Checkpoint within `delay_ms`, keeping an earlier deadline.
    async fn arm_checkpoint(&self, delay_ms: u64) {
        self.scheduler.ensure(TimerKind::CircuitCheckpoint, now_epoch_ms() + delay_ms).await;
    }

    /// Write the table images that moved, a byte budget per event, and come
    /// back right away while more are left.
    async fn checkpoint(&self) {
        let Some(node) = self.node.borrow().clone() else { return };
        match node.checkpoint_step(&self.snapshot, CHECKPOINT_BUDGET_BYTES).await {
            Ok(report) if report.remaining > 0 => {
                tracing::info!(?report, "checkpoint: more tables to write");
                self.scheduler.ensure(TimerKind::CircuitCheckpoint, now_epoch_ms()).await;
            }
            Ok(report) => {
                if report.skipped.is_some() || !report.written.is_empty() {
                    tracing::info!(?report, "checkpoint");
                }
            }
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), "checkpoint failed; retrying");
                self.scheduler
                    .ensure(TimerKind::CircuitCheckpoint, now_epoch_ms() + CHECKPOINT_RETRY_MS)
                    .await;
            }
        }
        self.bound_journal();
    }

    /// The journal only shrinks when checkpoints succeed. If they keep
    /// failing, drop the snapshot along with the oldest rows rather than grow
    /// without bound; the next cold start then rebuilds from the DB.
    fn bound_journal(&self) {
        let sql = self.storage.sql();
        let applied = sql
            .exec("SELECT COUNT(*) AS n FROM ssp_inbox WHERE applied = 1", None)
            .and_then(|c| c.to_array::<serde_json::Value>())
            .ok()
            .and_then(|rows| rows.first().and_then(|r| r.get("n")).and_then(serde_json::Value::as_u64))
            .unwrap_or(0);
        if applied <= MAX_JOURNAL_ROWS {
            return;
        }
        tracing::warn!(applied, "journal over its bound; dropping the snapshot");
        use ssp_node::SnapshotStore as _;
        let _ = self.snapshot.clear();
        let _ = sql.exec("DELETE FROM ssp_inbox WHERE applied = 1", None);
    }

    /// Replay the inbox in order, single flight (a second caller waits, so a
    /// registration after it sees every accepted notice). Ingest notices are
    /// applied by their records' current DB state (`SspNode::replay_notices`),
    /// view unregisters as they came. A failure keeps the rows and retries.
    async fn drain_inbox(&self) {
        let _drain = self.drain.lock().await;
        let (node, runtime) = match (self.node.borrow().clone(), self.runtime.borrow().clone()) {
            (Some(n), Some(r)) => (n, r),
            _ => return,
        };
        loop {
            // Recounted from disk each pass: notices keep arriving while a
            // replay awaits the DB.
            self.inbox_len.set(inbox_count(&self.storage));
            if self.inbox_len.get() == 0 || *node.status.read().await != SspStatus::Ready {
                return;
            }
            // A full publication queue refuses every replayed change, after
            // the database read that precedes it. Every queued notice pulls
            // this timer to now, so trying anyway read the database once per
            // notice, from the same database the publisher was draining to.
            if !node.edge_update_tx.has_room() && !self.unregister_queued() {
                self.scheduler
                    .ensure(TimerKind::IngestReplay, now_epoch_ms() + REPLAY_RETRY_MS)
                    .await;
                return;
            }
            let rows: Vec<serde_json::Value> = match self
                .storage
                .sql()
                .exec(
                    &format!("SELECT seq, kind, body FROM ssp_inbox WHERE applied = 0 ORDER BY seq LIMIT {DRAIN_PAGE}"),
                    None,
                )
                .and_then(|c| c.to_array())
            {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(error = %e, "reading ssp_inbox failed");
                    return;
                }
            };
            let mut done_through: Option<i64> = None;
            let mut notices: Vec<ssp_protocol::IngestRequest> = Vec::new();
            let mut failed = false;
            for row in &rows {
                let seq = row.get("seq").and_then(serde_json::Value::as_i64).unwrap_or(0);
                let kind = row.get("kind").and_then(serde_json::Value::as_str).unwrap_or("");
                let body = row.get("body").and_then(serde_json::Value::as_str).unwrap_or("");
                if kind == "unregister" {
                    // Keep order: whatever was noticed before it lands first.
                    if !notices.is_empty() && self.replay(&node, std::mem::take(&mut notices)).await.is_err() {
                        failed = true;
                        break;
                    }
                    let api_req = ssp_node::ApiRequest {
                        method: ApiMethod::Post,
                        path: "/view/unregister".into(),
                        bearer: Some(node.auth_secret.clone()),
                        body: body.as_bytes().to_vec().into(),
                    };
                    match node.route(api_req).await {
                        Some(resp) if resp.status < 500 => {}
                        _ => {
                            failed = true;
                            break;
                        }
                    }
                } else {
                    match serde_json::from_str::<Vec<ssp_protocol::IngestRequest>>(body) {
                        Ok(mut batch) => notices.append(&mut batch),
                        Err(e) => tracing::warn!(error = %e, seq, "dropping an unreadable inbox row"),
                    }
                }
                done_through = Some(seq);
            }
            if !failed && !notices.is_empty() && self.replay(&node, notices).await.is_err() {
                failed = true;
            }
            if failed {
                // Rows already applied are replayed again next time, which is
                // a no-op for ingest notices and for unregisters.
                self.scheduler
                    .ensure(TimerKind::IngestReplay, now_epoch_ms() + REPLAY_RETRY_MS)
                    .await;
                return;
            }
            if let Some(seq) = done_through {
                // Kept as the journal until a checkpoint covers them.
                if let Err(e) = self
                    .storage
                    .sql()
                    .exec("UPDATE ssp_inbox SET applied = 1 WHERE seq <= ? AND applied = 0", vec![seq.into()])
                {
                    tracing::warn!(error = %e, "marking replayed notices failed");
                    return;
                }
            }
            runtime.after_request("/ingest").await;
            self.arm_checkpoint(CHECKPOINT_AFTER_CHANGE_MS).await;
        }
    }

    async fn replay(&self, node: &SspNode, notices: Vec<ssp_protocol::IngestRequest>) -> std::result::Result<(), ()> {
        match node.replay_notices(notices).await {
            Ok(report) => {
                tracing::info!(?report, "replayed queued change notices");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), "replaying queued change notices failed; retrying");
                Err(())
            }
        }
    }
}

/// What the inbox keeps of a notice: `ingest` rows hold the compact notices
/// (record bodies dropped, replay reads the DB again), `unregister` rows the
/// request as it came.
fn compact_for_inbox(path: &str, body: &[u8]) -> std::result::Result<(&'static str, String), String> {
    use ssp_node::replay::compact_notice;
    let notices: Vec<ssp_protocol::IngestRequest> = match path {
        "/view/unregister" => {
            let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
            serde_json::from_str::<serde_json::Value>(text).map_err(|e| e.to_string())?;
            return Ok(("unregister", text.to_string()));
        }
        "/ingest/batch" => {
            serde_json::from_slice::<ssp_protocol::IngestBatchRequest>(body).map_err(|e| e.to_string())?.records
        }
        _ => vec![serde_json::from_slice::<ssp_protocol::IngestRequest>(body).map_err(|e| e.to_string())?],
    };
    let compact: Vec<_> = notices.into_iter().map(compact_notice).collect();
    Ok(("ingest", serde_json::to_string(&compact).map_err(|e| e.to_string())?))
}

/// The node, and the job queue's receiver when the project has job tables
/// (the caller runs a `JobRunner` on it). Without job tables the receiver is
/// dropped: the dispatcher notices the closed channel on its first send and
/// latches itself off, so it never issues a drain query nothing would run.
fn build_node(
    platform: Platform,
    cfg: &NodeConfigCf,
    push_engine: Arc<push_core::PushEngine>,
) -> (SspNode, Option<tokio::sync::mpsc::Receiver<ssp_node::jobs::JobEntry>>) {
    let (job_queue_tx, job_rx) = tokio::sync::mpsc::channel(64);
    let admin_settings = cfg.admin.clone();
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
    let has_jobs = !job_config.job_tables.is_empty();
    let schedule_engine = ssp_node::schedules::build_engine(true, Arc::clone(&platform.db), job_control.clone());
    let node = SspNode {
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
            ssp_node::crdt::FoldConfig { cache_size: CRDT_CACHE_DOCS, ..Default::default() },
            cfg.ssp_id.clone(),
        )),
        view_metrics: Arc::new(RwLock::new(std::collections::HashMap::new())),
        edge_update_tx,
        anonymous_live_queries: cfg.anonymous_live_queries,
        impersonation: cfg.impersonation,
        query_allowlist: Arc::new(ssp_node::allowlist_state::QueryAllowlist::new(cfg.query_allowlist)),
        standalone: true,
        // Declarative schedules and workflows, as on a standalone VM SSP. The
        // on-demand runtime arms each sweep for the engine's next wake, so a
        // project without schedules never sets an alarm for them.
        schedule_engine,
        push_engine: Some(push_engine),
        ttl_cleanup_interval_secs: 60,
        // Off: a 15 s alarm would keep the Durable Object from ever
        // hibernating. Registrations naming an unknown table still refresh.
        schema_poll_secs: 0,
        schema_watch: Default::default(),
        view_metrics_flush_ms: 2000,
        // As the VM SSP: SurrealDB 3.1 rescans a table from its first key for
        // every keyset page, so the page count makes a rebuild quadratic (200
        // rows took 39 s for 50k rows from Cloudflare to Hetzner). 1000 keeps
        // pages about 1 MB.
        bootstrap_page_size: 1000,
        // The JSON `CircuitStore` path is off; the shell writes table images.
        checkpoint_interval_secs: None,
        max_snapshot_age_secs: MAX_SNAPSHOT_AGE_SECS,
        last_heartbeat_seen: std::sync::Arc::new(std::sync::Mutex::new(None)),
        admin: ssp_node::admin::AdminState::new(admin_settings),
    };
    (node, has_jobs.then_some(job_rx))
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
    // A browser's preflight for the admin API needs no Durable Object.
    if req.method() == Method::Options && ssp_node::admin::is_admin_path(&req.path()) {
        return to_worker_response(ssp_node::admin::preflight());
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
