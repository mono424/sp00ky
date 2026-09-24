//! Reference non-VM host for the SSP node.
//!
//! The whole point of this crate is to prove the runtime seam is generic: it
//! drives the SAME [`ssp_node::SspNode`] / [`ssp_node::Runtime`] as the VM
//! shell, but with a completely different ingress (hyper, not axum), a
//! different circuit-persistence backend (disk, not the VM's noop), and a
//! simulated eviction cycle that forces the restore-or-rebuild path to be
//! real. Nothing here is Cloudflare-specific — a DO host is this file with
//! `hyper` swapped for `fetch` and [`DiskCircuitStore`] swapped for DO storage.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use hyper::{Request, Response};
use serde_json::Value;
use tokio::sync::{mpsc, RwLock};

use ssp::circuit::Circuit;
use ssp_node::ports::{
    CancelHandle, CancelWatch, Db, DbError, HttpClient, HttpError,
    LocalBoxFuture, OutboundRequest, OutboundResponse, Scheduler, Spawner, TimerKind,
};
use ssp_node::{ApiBody, ApiRequest, Method, NoopTelemetry, Platform, Runtime, SspNode, SspStatus};
use surrealdb::engine::local::Db as MemEngine;
use surrealdb::Surreal;

// --- Adapters ----------------------------------------------------------------

/// `Db` port over an embedded SurrealDB (`kv-mem`). Same flattening as the VM's
/// `SurrealSdkDb`: `into_json_value` unwraps SurrealDB's tagged Value enum.
pub struct PortableDb(pub Arc<Surreal<MemEngine>>);

#[async_trait::async_trait]
impl Db for PortableDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        let mut q = self.0.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| DbError::Transport(e.to_string()))?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value =
                response.take(i).map_err(|e| DbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
    async fn version(&self) -> Result<String, DbError> {
        Ok("portable-mem".to_string())
    }
}

/// `HttpClient` stub — this reference host doesn't run job backends, so
/// outbound calls just succeed. A production edge host would supply a real
/// fetch-backed client here.
pub struct StubHttp;

#[async_trait::async_trait]
impl HttpClient for StubHttp {
    async fn send(
        &self,
        _req: OutboundRequest,
        _cancel: Option<CancelWatch>,
    ) -> Result<OutboundResponse, HttpError> {
        Ok(OutboundResponse { status: 200, body: String::new() })
    }
}

/// `Scheduler` on tokio: one sleeping task per pending timer, fired kinds
/// delivered over an mpsc the host drains into `Runtime::on_timer`.
pub struct TokioScheduler {
    tx: mpsc::UnboundedSender<TimerKind>,
    pending: Mutex<HashMap<TimerKind, CancelHandle>>,
}

impl TokioScheduler {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<TimerKind>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx, pending: Mutex::new(HashMap::new()) }, rx)
    }
}

#[async_trait::async_trait]
impl Scheduler for TokioScheduler {
    async fn schedule(&self, kind: TimerKind, at_epoch_ms: u64) {
        let (handle, mut watch) = CancelHandle::new();
        if let Some(prev) = self.pending.lock().unwrap().insert(kind.clone(), handle) {
            prev.cancel();
        }
        let tx = self.tx.clone();
        let delay = at_epoch_ms.saturating_sub(ssp_node::now_epoch_ms());
        tokio::spawn(async move {
            tokio::select! {
                _ = watch.cancelled() => {}
                _ = tokio::time::sleep(Duration::from_millis(delay)) => { let _ = tx.send(kind); }
            }
        });
    }
    async fn cancel(&self, kind: &TimerKind) {
        if let Some(handle) = self.pending.lock().unwrap().remove(kind) {
            handle.cancel();
        }
    }
    async fn sleep(&self, dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

/// `Spawner` on tokio.
pub struct TokioSpawner;
impl Spawner for TokioSpawner {
    fn spawn(&self, fut: LocalBoxFuture) {
        tokio::spawn(fut);
    }
}

/// Re-exported from `ssp-node` so the server, this host and the tests all
/// exercise one implementation instead of parallel copies that can drift.
pub use ssp_node::DiskCircuitStore;

// --- Host --------------------------------------------------------------------

/// A running SSP node on the portable host. `db` + `store` outlive the circuit:
/// [`Self::evict`] drops the in-memory node (as an edge platform would reclaim
/// the instance) and reconstructs it, forcing `Runtime::bootstrap` down the
/// restore-or-rebuild path.
pub struct PortableHost {
    pub node: Arc<SspNode>,
    pub runtime: Runtime,
    db: Arc<Surreal<MemEngine>>,
    store: Arc<DiskCircuitStore>,
    secret: String,
    checkpoint_interval_secs: Option<u64>,
}

impl PortableHost {
    /// Build a host over a shared DB + disk state dir, then bootstrap it.
    pub async fn new(
        db: Arc<Surreal<MemEngine>>,
        state_dir: impl AsRef<Path>,
        secret: impl Into<String>,
        checkpoint_interval_secs: Option<u64>,
    ) -> Self {
        let store = Arc::new(DiskCircuitStore::new(state_dir));
        let secret = secret.into();
        let (node, runtime) =
            build_node(db.clone(), store.clone(), &secret, checkpoint_interval_secs);
        let host = Self { node, runtime, db, store, secret, checkpoint_interval_secs };
        host.runtime.bootstrap().await;
        host
    }

    /// Simulate eviction: drop the in-memory node/circuit and reconstruct it
    /// over the same DB + disk store, then bootstrap (restore + catch-up).
    pub async fn evict(&mut self) {
        let (node, runtime) = build_node(
            self.db.clone(),
            self.store.clone(),
            &self.secret,
            self.checkpoint_interval_secs,
        );
        self.node = node;
        self.runtime = runtime;
        self.runtime.bootstrap().await;
    }
}

fn build_node(
    db: Arc<Surreal<MemEngine>>,
    store: Arc<DiskCircuitStore>,
    secret: &str,
    checkpoint_interval_secs: Option<u64>,
) -> (Arc<SspNode>, Runtime) {
    let (scheduler, _rx) = TokioScheduler::new();
    let platform = Platform {
        db: Arc::new(PortableDb(db)),
        http: Arc::new(StubHttp),
        scheduler: Arc::new(scheduler),
        spawner: Arc::new(TokioSpawner),
        telemetry: Arc::new(NoopTelemetry),
        circuit_store: store,
    };
    // `_job_rx` is dropped: no runner in this shell. The dispatcher latches
    // itself off on the first closed send rather than querying for a backlog it
    // could never work through.
    let (job_queue_tx, _job_rx) = mpsc::channel(64);
    let edge_update_tx = ssp_node::edges::EdgePublisher::default();
    let job_config = Arc::new(ssp_node::jobs::JobConfig::default());
    let job_control = ssp_node::jobs::JobControl::new();
    let job_dispatcher = Arc::new(ssp_node::jobs::JobDispatcher::new(
        Arc::clone(&platform.db),
        Arc::clone(&platform.spawner),
        Arc::clone(&platform.scheduler),
        job_queue_tx,
        job_control.clone(),
        Arc::clone(&job_config),
        "portable-ssp".to_string(),
        true,
    ));
    let node = Arc::new(SspNode {
        publication_gate: Arc::new(tokio::sync::Mutex::new(())),
        platform,
        status: Arc::new(RwLock::new(SspStatus::Bootstrapping)),
        processor: Arc::new(RwLock::new(Circuit::new())),
        job_config,
        job_control,
        job_dispatcher,
        ssp_id: "portable-ssp".to_string(),
        auth_secret: secret.to_string(),
        ref_mode: ssp_protocol::RefMode::Single,
        // Merged views are a server-side memory optimisation; this shell
        // registers one view per client, so leave it off.
        merge_views: false,
        version: env!("CARGO_PKG_VERSION"),
        surrealdb_version: "portable-mem".to_string(),
        advertise_ip: None,
        info_env: vec![],
        start_epoch_ms: ssp_node::now_epoch_ms(),
        bootstrap_warnings: Arc::new(RwLock::new(Vec::new())),
        backend_health: None,
        crdt_cache: Arc::new(ssp_node::crdt::CrdtCache::new(
            8,
            ssp_node::crdt::CrdtAllowList::permissive(),
        )),
        view_metrics: Arc::new(RwLock::new(HashMap::new())),
        edge_update_tx,
        anonymous_live_queries: false,
        impersonation: false,
        query_allowlist: Arc::new(ssp_node::allowlist_state::QueryAllowlist::new(ssp::allowlist::Mode::Off)),
        standalone: true,
        // No schedule engine: this shell has no job runner wired (`_job_rx` is
        // dropped), so a fired schedule would spawn rows nothing executes.
        schedule_engine: None,
        ttl_cleanup_interval_secs: 60,
        // No timers are armed here; registrations still refresh on a miss.
        schema_poll_secs: 0,
        schema_watch: Default::default(),
        view_metrics_flush_ms: 2000,
        bootstrap_page_size: 200,
        checkpoint_interval_secs,
        max_snapshot_age_secs: 3600,
        last_heartbeat_seen: std::sync::Arc::new(std::sync::Mutex::new(None)),
    });
    let runtime = Runtime::new(node.clone());
    (node, runtime)
}

// --- hyper ingress -----------------------------------------------------------

/// Bridge one hyper request into `SspNode::route` and back. Generic over the
/// body type so the accept-loop (`Incoming`) and tests (`Full<Bytes>`) share
/// exactly one code path — the concrete proof that the core needs no framework.
pub async fn handle<B>(node: &SspNode, req: Request<B>) -> Response<Full<Bytes>>
where
    B: Body,
    B::Error: std::fmt::Debug,
{
    let method = match *req.method() {
        hyper::Method::GET => Method::Get,
        hyper::Method::POST => Method::Post,
        hyper::Method::PUT => Method::Put,
        hyper::Method::DELETE => Method::Delete,
        _ => return status_only(404),
    };
    let path = req.uri().path().to_string();
    let bearer = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(|s| s.to_string());
    let body = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return status_only(400),
    };

    let api_req = ApiRequest { method, path, bearer, body };
    match node.route(api_req).await {
        Some(resp) => {
            let (content_type, body_bytes) = match resp.body {
                ApiBody::Json(v) => ("application/json".to_string(), v.to_string()),
                ApiBody::Text { content_type, body } => (content_type.to_string(), body),
            };
            let mut builder = Response::builder().status(resp.status);
            for (name, value) in &resp.headers {
                builder = builder.header(*name, value);
            }
            builder
                .header(hyper::header::CONTENT_TYPE, content_type)
                .body(Full::new(Bytes::from(body_bytes)))
                .unwrap_or_else(|_| status_only(500))
        }
        None => status_only(404),
    }
}

fn status_only(code: u16) -> Response<Full<Bytes>> {
    Response::builder().status(code).body(Full::new(Bytes::new())).unwrap()
}

/// Serve `node` over hyper on `addr` until the process ends. This is the only
/// framework-specific code in the host; everything above is portable.
pub async fn serve(node: Arc<SspNode>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(%addr, "portable SSP host listening");
    loop {
        let (stream, _peer) = listener.accept().await.context("accept")?;
        let io = TokioIo::new(stream);
        let node = node.clone();
        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let node = node.clone();
                async move { Ok::<_, std::convert::Infallible>(handle(&node, req).await) }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                tracing::debug!(error = %e, "connection closed");
            }
        });
    }
}
