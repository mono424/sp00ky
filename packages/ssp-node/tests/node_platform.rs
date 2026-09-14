//! Platform-independence proof: drive `SspNode::route` over a FULLY MOCK
//! platform — no axum, no VM shell, no `tokio::time`/`spawn`, just the port
//! traits. This is the test that fails the moment a migrated handler grows a
//! hidden dependency on the VM runtime; it's the concrete check behind the
//! adapter-swap thesis in `docs/platform-architecture.md`.
//!
//! The `Db` port is backed by a real embedded SurrealDB (`kv-mem`), so job
//! control, reset, and info run their production SQL end to end — only the
//! *runtime* is mocked, not the database semantics.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, RwLock};

use ssp::circuit::Circuit;
use ssp_node::api::{ApiBody, ApiRequest, ApiResponse, Method};
use ssp_node::jobs::{JobConfig, JobControl, JobEntry};
use ssp_node::ports::{
    BackendCounts, BackendHealth, BackendSpec, CancelWatch, Db, DbError, HttpClient, HttpError,
    LocalBoxFuture, OutboundRequest, OutboundResponse, Scheduler, Spawner, Telemetry, TimerKind,
};
use ssp_node::{Platform, SspNode, SspStatus};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;

const SECRET: &str = "unit-secret";

// --- Mock ports --------------------------------------------------------------

struct MemDb(Arc<Surreal<MemEngine>>);

#[async_trait::async_trait]
impl Db for MemDb {
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
        Ok("mem-test".to_string())
    }
}

/// Records dispatched requests; returns a configurable status.
#[derive(Clone, Default)]
struct MockHttp {
    calls: Arc<Mutex<Vec<String>>>,
    status: Arc<AtomicUsize>, // 0 => use 200
}

#[async_trait::async_trait]
impl HttpClient for MockHttp {
    async fn send(
        &self,
        req: OutboundRequest,
        _cancel: Option<CancelWatch>,
    ) -> Result<OutboundResponse, HttpError> {
        self.calls.lock().unwrap().push(req.url.clone());
        let s = self.status.load(Ordering::SeqCst);
        Ok(OutboundResponse {
            status: if s == 0 { 200 } else { s as u16 },
            body: String::new(),
        })
    }
}

/// Records scheduled timers; `sleep` returns instantly so tests never wait.
#[derive(Clone, Default)]
struct MockScheduler {
    scheduled: Arc<Mutex<Vec<TimerKind>>>,
}

#[async_trait::async_trait]
impl Scheduler for MockScheduler {
    async fn schedule(&self, kind: TimerKind, _at: u64) {
        self.scheduled.lock().unwrap().push(kind);
    }
    async fn cancel(&self, _kind: &TimerKind) {}
    async fn sleep(&self, _dur: Duration) {}
}

struct MockSpawner;
impl Spawner for MockSpawner {
    fn spawn(&self, fut: LocalBoxFuture) {
        tokio::spawn(fut);
    }
}

#[derive(Clone, Default)]
struct MockTelemetry {
    gauge: Arc<Mutex<i64>>,
}
impl Telemetry for MockTelemetry {
    fn counter(&self, _name: &'static str, _value: u64) {}
    fn histogram_ms(&self, _name: &'static str, _value: f64) {}
    fn gauge_add(&self, _name: &'static str, delta: i64) {
        *self.gauge.lock().unwrap() += delta;
    }
}

/// Constant backend-health counts (standalone `/health` + `/backends`).
struct MockBackendHealth {
    counts: Arc<Mutex<BackendCounts>>,
    updates: Arc<Mutex<Vec<BackendSpec>>>,
}

#[async_trait::async_trait]
impl BackendHealth for MockBackendHealth {
    async fn counts(&self) -> BackendCounts {
        *self.counts.lock().unwrap()
    }
    async fn update(&self, backends: Vec<BackendSpec>) {
        *self.updates.lock().unwrap() = backends;
    }
}

// --- Harness -----------------------------------------------------------------

struct Harness {
    node: Arc<SspNode>,
    raw_db: Arc<Surreal<MemEngine>>,
    http_calls: Arc<Mutex<Vec<String>>>,
    scheduled: Arc<Mutex<Vec<TimerKind>>>,
    telemetry_gauge: Arc<Mutex<i64>>,
    backend_updates: Arc<Mutex<Vec<BackendSpec>>>,
    _job_rx: mpsc::Receiver<JobEntry>,
    job_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<JobEntry>>>,
}

struct HarnessOpts {
    status: SspStatus,
    job_tables: Vec<(&'static str, &'static str)>, // (table, base_url)
    backend_counts: Option<BackendCounts>,
    ref_mode: ssp_protocol::RefMode,
    /// Share one operator graph across registrations computing the same thing.
    merge_views: bool,
    circuit_store: Option<Arc<dyn ssp_node::CircuitStore>>,
    /// Wire the declarative-schedule engine (as a standalone VM node does).
    schedules: bool,
}

impl Default for HarnessOpts {
    fn default() -> Self {
        Self {
            status: SspStatus::Ready,
            job_tables: vec![],
            backend_counts: None,
            ref_mode: ssp_protocol::RefMode::Single,
            merge_views: false,
            circuit_store: None,
            schedules: false,
        }
    }
}

/// In-memory `CircuitStore` for the bootstrap restore path.
#[derive(Clone, Default)]
struct MemStore(Arc<Mutex<Option<(String, ssp_node::ResumePoint)>>>);

#[async_trait::async_trait]
impl ssp_node::CircuitStore for MemStore {
    async fn save(
        &self,
        blob: &str,
        point: &ssp_node::ResumePoint,
    ) -> Result<(), ssp_node::CircuitStoreError> {
        *self.0.lock().unwrap() = Some((blob.to_string(), point.clone()));
        Ok(())
    }
    async fn load(&self) -> Result<(String, ssp_node::ResumePoint), ssp_node::CircuitStoreError> {
        self.0
            .lock()
            .unwrap()
            .clone()
            .ok_or(ssp_node::CircuitStoreError::NotFound)
    }
    async fn clear(&self) -> Result<(), ssp_node::CircuitStoreError> {
        *self.0.lock().unwrap() = None;
        Ok(())
    }
}

async fn build(opts: HarnessOpts) -> Harness {
    let raw = Surreal::new::<Mem>(()).await.unwrap();
    raw.use_ns("test").use_db("test").await.unwrap();
    let raw = Arc::new(raw);

    let http_calls = Arc::new(Mutex::new(Vec::new()));
    let scheduled = Arc::new(Mutex::new(Vec::new()));
    let telemetry_gauge = Arc::new(Mutex::new(0));

    let platform = Platform {
        db: Arc::new(MemDb(Arc::clone(&raw))),
        http: Arc::new(MockHttp { calls: Arc::clone(&http_calls), status: Arc::new(AtomicUsize::new(0)) }),
        scheduler: Arc::new(MockScheduler { scheduled: Arc::clone(&scheduled) }),
        spawner: Arc::new(MockSpawner),
        telemetry: Arc::new(MockTelemetry { gauge: Arc::clone(&telemetry_gauge) }),
        circuit_store: opts
            .circuit_store
            .clone()
            .unwrap_or_else(|| Arc::new(ssp_node::NoopCircuitStore)),
    };

    let mut job_config = JobConfig::default();
    for (table, base_url) in &opts.job_tables {
        job_config.job_tables.insert(
            (*table).to_string(),
            ssp_node::jobs::BackendInfo {
                name: (*table).to_string(),
                base_url: (*base_url).to_string(),
                auth_token: None,
                timeout: Some(5),
                timeout_overridable: false,
            },
        );
    }

    let backend_updates = Arc::new(Mutex::new(Vec::new()));
    let backend_health: Option<Arc<dyn BackendHealth>> = opts.backend_counts.map(|c| {
        Arc::new(MockBackendHealth {
            counts: Arc::new(Mutex::new(c)),
            updates: Arc::clone(&backend_updates),
        }) as Arc<dyn BackendHealth>
    });

    let (tx, rx) = mpsc::channel::<JobEntry>(64);
    // Keep a second receiver handle so tests can drain enqueued jobs without
    // consuming the node's sender.
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    let job_control = JobControl::new();
    let job_config = Arc::new(job_config);
    let job_dispatcher = Arc::new(ssp_node::jobs::JobDispatcher::new(
        Arc::clone(&platform.db),
        Arc::clone(&platform.spawner),
        Arc::clone(&platform.scheduler),
        tx,
        job_control.clone(),
        Arc::clone(&job_config),
        "test-ssp".to_string(),
        true,
    ));
    let node = SspNode {
        publication_gate: Arc::new(tokio::sync::Mutex::new(())),
        platform,
        status: Arc::new(RwLock::new(opts.status)),
        processor: Arc::new(RwLock::new(Circuit::new())),
        job_config,
        job_control: job_control.clone(),
        job_dispatcher,
        ssp_id: "test-ssp".to_string(),
        auth_secret: SECRET.to_string(),
        ref_mode: opts.ref_mode,
        merge_views: opts.merge_views,
        version: "9.9.9-test",
        surrealdb_version: "mem-test".to_string(),
        advertise_ip: Some("10.0.0.7".to_string()),
        info_env: vec![("SPKY_SSP_ID".to_string(), "test-ssp".to_string())],
        start_epoch_ms: ssp_node::now_epoch_ms().saturating_sub(5000),
        bootstrap_warnings: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        backend_health,
        crdt_cache: Arc::new(ssp_node::crdt::CrdtCache::new(8, ssp_node::crdt::CrdtAllowList::permissive())),
        view_metrics: Arc::new(RwLock::new(std::collections::HashMap::new())),
        edge_update_tx: { let (tx, _rx) = mpsc::unbounded_channel(); tx },
        anonymous_live_queries: false,
        standalone: true,
        schedule_engine: opts.schedules.then(|| {
            ssp_node::schedules::build_engine(
                true,
                Arc::new(MemDb(Arc::clone(&raw))),
                job_control.clone(),
            )
            .expect("standalone builds an engine")
        }),
        ttl_cleanup_interval_secs: 60,
        view_metrics_flush_ms: 2000,
        bootstrap_page_size: 200,
        checkpoint_interval_secs: None,
        max_snapshot_age_secs: 3600,
        last_heartbeat_seen: std::sync::Arc::new(std::sync::Mutex::new(None)),
    };

    // A dummy second receiver is not creatable from one channel; keep the real
    // one behind the mutex and expose it.
    let (_dead_tx, dead_rx) = mpsc::channel::<JobEntry>(1);

    // What every shell does after constructing a circuit: push the node's
    // config into it, since the register path and boot re-registration both
    // read the merge policy off the circuit.
    node.apply_circuit_policy().await;

    Harness {
        node: Arc::new(node),
        raw_db: raw,
        http_calls,
        scheduled,
        telemetry_gauge,
        backend_updates,
        _job_rx: dead_rx,
        job_rx: rx,
    }
}

fn req(method: Method, path: &str, bearer: Option<&str>, body: Value) -> ApiRequest {
    ApiRequest {
        method,
        path: path.to_string(),
        bearer: bearer.map(|s| s.to_string()),
        body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
    }
}

fn authed(method: Method, path: &str, body: Value) -> ApiRequest {
    req(method, path, Some(SECRET), body)
}

fn json_of(resp: &ApiResponse) -> &Value {
    match &resp.body {
        ApiBody::Json(v) => v,
        ApiBody::Text { .. } => panic!("expected JSON body"),
    }
}

async fn insert_job(db: &Surreal<MemEngine>, id: &str, status: &str) {
    db.query(
        "CREATE type::record('job', $id) SET \
         status = $status, path = '/run', payload = {}, retries = 0, max_retries = 3, \
         retry_strategy = 'linear', recurring = false, interval = 0, errors = [], \
         created_at = time::now(), updated_at = time::now()",
    )
    .bind(("id", id.to_string()))
    .bind(("status", status.to_string()))
    .await
    .unwrap();
}

async fn job_status(db: &Surreal<MemEngine>, id: &str) -> Option<String> {
    db.query(format!("SELECT VALUE status FROM ONLY job:{id}"))
        .await
        .unwrap()
        .take(0)
        .unwrap()
}

// --- Tests -------------------------------------------------------------------

#[tokio::test]
async fn unknown_route_returns_none() {
    let h = build(HarnessOpts::default()).await;
    // Genuinely unknown paths/methods → None (shell 404s). Every real SSP
    // route is now served by the core, including /ingest.
    assert!(h.node.route(authed(Method::Get, "/nope", Value::Null)).await.is_none());
    assert!(h.node.route(authed(Method::Delete, "/ingest", Value::Null)).await.is_none());
}

#[tokio::test]
async fn auth_enforced_on_protected_routes_only() {
    let h = build(HarnessOpts::default()).await;

    // Missing/wrong bearer on an authed route → 401.
    let r = h.node.route(req(Method::Post, "/reset", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 401);
    let r = h.node.route(req(Method::Post, "/reset", Some("wrong"), Value::Null)).await.unwrap();
    assert_eq!(r.status, 401);

    // Public route needs no auth and carries the CORS header.
    let r = h.node.route(req(Method::Get, "/version", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 200);
    assert!(r.headers.iter().any(|(k, v)| *k == "Access-Control-Allow-Origin" && v == "*"));
}

#[tokio::test]
async fn version_reports_shell_version() {
    let h = build(HarnessOpts::default()).await;
    let r = h.node.route(req(Method::Get, "/version", None, Value::Null)).await.unwrap();
    assert_eq!(json_of(&r)["version"], "9.9.9-test");
    assert_eq!(json_of(&r)["mode"], "streaming");
}

#[tokio::test]
async fn health_plain_when_no_backend_monitor() {
    // Ready, no monitor → 200 + bare status. Bootstrapping → 503.
    let h = build(HarnessOpts::default()).await;
    let r = h.node.route(req(Method::Get, "/health", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["status"], "ready");
    assert!(json_of(&r).get("backends").is_none());

    let h = build(HarnessOpts { status: SspStatus::Bootstrapping, ..Default::default() }).await;
    let r = h.node.route(req(Method::Get, "/health", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 503);
}

#[tokio::test]
async fn health_aggregates_backend_counts() {
    // Ready + all healthy → healthy/200.
    let h = build(HarnessOpts {
        backend_counts: Some(BackendCounts { healthy: 2, unhealthy: 0, unreachable: 0, total: 2 }),
        ..Default::default()
    })
    .await;
    let r = h.node.route(req(Method::Get, "/health", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["status"], "healthy");
    assert_eq!(json_of(&r)["backends"]["total"], 2);

    // Ready + one unreachable → degraded/200.
    let h = build(HarnessOpts {
        backend_counts: Some(BackendCounts { healthy: 1, unhealthy: 0, unreachable: 1, total: 2 }),
        ..Default::default()
    })
    .await;
    let r = h.node.route(req(Method::Get, "/health", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["status"], "degraded");

    // Ready + ALL down → unavailable/503.
    let h = build(HarnessOpts {
        backend_counts: Some(BackendCounts { healthy: 0, unhealthy: 1, unreachable: 1, total: 2 }),
        ..Default::default()
    })
    .await;
    let r = h.node.route(req(Method::Get, "/health", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 503);
    assert_eq!(json_of(&r)["status"], "unavailable");
}

#[tokio::test]
async fn info_reports_identity_and_uptime() {
    let h = build(HarnessOpts::default()).await;
    let r = h.node.route(req(Method::Get, "/info", None, Value::Null)).await.unwrap();
    let entity = &json_of(&r)[0];
    assert_eq!(entity["id"], "test-ssp");
    assert_eq!(entity["ip"], "10.0.0.7");
    assert_eq!(entity["version"], "9.9.9-test");
    assert_eq!(entity["surrealdb_version"], "mem-test");
    assert_eq!(entity["ref_mode"], "single");
    assert!(entity["uptime_seconds"].as_u64().unwrap() >= 5, "start was 5s ago");
    assert_eq!(entity["env"]["SPKY_SSP_ID"], "test-ssp");

    // /info/text is the same array as a verbatim text/plain string.
    let r = h.node.route(req(Method::Get, "/info/text", None, Value::Null)).await.unwrap();
    match &r.body {
        ApiBody::Text { content_type, body } => {
            assert_eq!(*content_type, "text/plain");
            let parsed: Value = serde_json::from_str(body).unwrap();
            assert_eq!(parsed[0]["id"], "test-ssp");
        }
        _ => panic!("expected text body"),
    }
}

#[tokio::test]
async fn backends_update_only_when_monitor_active() {
    // No monitor → 409.
    let h = build(HarnessOpts::default()).await;
    let r = h
        .node
        .route(authed(Method::Put, "/backends", json!([])))
        .await
        .unwrap();
    assert_eq!(r.status, 409);

    // Monitor active → 200, update forwarded to the port.
    let h = build(HarnessOpts {
        backend_counts: Some(BackendCounts::default()),
        ..Default::default()
    })
    .await;
    let body = json!([{ "name": "api", "url": "http://api:3000", "healthcheck": "/health" }]);
    let r = h.node.route(authed(Method::Put, "/backends", body)).await.unwrap();
    assert_eq!(r.status, 200);
    let updates = h.backend_updates.lock().unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].name, "api");
}

#[tokio::test]
async fn reset_wipes_circuit_and_reports_gauge() {
    let h = build(HarnessOpts::default()).await;
    // Seed a view so the reset has something to clear (gauge should drop).
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }
    let r = h.node.route(authed(Method::Post, "/reset", Value::Null)).await.unwrap();
    assert_eq!(r.status, 200);
    // Reset routes through the Db port: DELETE _00_list_ref (single mode) runs
    // against mem SurrealDB without error, and the circuit is fresh.
    assert_eq!(h.node.processor.read().await.view_count(), 0);
    // Telemetry gauge was touched (0 views → delta 0, but the call path ran).
    let _ = *h.telemetry_gauge.lock().unwrap();
}

#[tokio::test]
async fn job_kill_terminalizes_pending_row() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://b")], ..Default::default() }).await;
    insert_job(&h.raw_db, "k1", "pending").await;

    let r = h.node.route(authed(Method::Post, "/job/kill", json!({ "id": "job:k1" }))).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["status"], "failed");
    assert_eq!(job_status(&h.raw_db, "k1").await.as_deref(), Some("failed"));
}

#[tokio::test]
async fn job_kill_bad_id_and_missing() {
    let h = build(HarnessOpts::default()).await;
    let r = h.node.route(authed(Method::Post, "/job/kill", json!({ "id": "nocolon" }))).await.unwrap();
    assert_eq!(r.status, 400);

    let r = h.node.route(authed(Method::Post, "/job/kill", json!({ "id": "job:ghost" }))).await.unwrap();
    assert_eq!(r.status, 404);
}

#[tokio::test]
async fn job_retry_resets_and_enqueues() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://b")], ..Default::default() }).await;
    insert_job(&h.raw_db, "r1", "failed").await;

    let r = h.node.route(authed(Method::Post, "/job/retry", json!({ "id": "job:r1" }))).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["status"], "pending");
    assert_eq!(job_status(&h.raw_db, "r1").await.as_deref(), Some("pending"));

    // The reset+re-enqueue put exactly one JobEntry on the queue.
    let mut rx = h.job_rx.lock().await;
    let job = rx.try_recv().expect("a job was enqueued");
    assert_eq!(job.id, "job:r1");
    assert_eq!(job.retries, 0, "retry budget reset");

    // Retrying a non-terminal job is a 409.
    insert_job(&h.raw_db, "p1", "processing").await;
    let r = h.node.route(authed(Method::Post, "/job/retry", json!({ "id": "job:p1" }))).await.unwrap();
    assert_eq!(r.status, 409);
}

#[tokio::test]
async fn job_recover_only_pending() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://b")], ..Default::default() }).await;
    insert_job(&h.raw_db, "rec1", "pending").await;
    let r = h.node.route(authed(Method::Post, "/job/recover", json!({ "id": "job:rec1" }))).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["message"], "re-enqueued");
    // Assignee stamped on the row (took ownership).
    let assignee: Option<String> = h
        .raw_db
        .query("SELECT VALUE assignee FROM ONLY job:rec1")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(assignee.as_deref(), Some("test-ssp"));

    // Terminal rows are skipped.
    insert_job(&h.raw_db, "rec2", "success").await;
    let r = h.node.route(authed(Method::Post, "/job/recover", json!({ "id": "job:rec2" }))).await.unwrap();
    assert_eq!(json_of(&r)["message"], "not pending; recover skipped");
}

#[tokio::test]
async fn debug_deps_reports_empty_store() {
    let h = build(HarnessOpts::default()).await;
    let r = h.node.route(authed(Method::Get, "/debug/deps", Value::Null)).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["view_count"], 0);
}

#[tokio::test]
async fn crdt_apply_gated_when_not_ready() {
    let h = build(HarnessOpts { status: SspStatus::Bootstrapping, ..Default::default() }).await;
    let body = json!({ "table": "t", "record_id": "t:1", "field": "f", "update": "", "peer": "1" });
    let r = h.node.route(authed(Method::Post, "/crdt/apply", body)).await.unwrap();
    assert_eq!(r.status, 503);
}

#[tokio::test]
async fn view_register_then_unregister_roundtrip() {
    let h = build(HarnessOpts::default()).await;
    // Default-deny permissions: seed a permissive rule so registration passes.
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }

    // Register a simple view → circuit view_count goes to 1.
    let payload = json!({
        "id": "v1",
        "surql": "SELECT * FROM thread",
        "clientId": "c1",
        "ttl": "30m",
        "lastActiveAt": "2024-01-01T00:00:00Z"
    });
    let r = h.node.route(authed(Method::Post, "/view/register", payload)).await.unwrap();
    assert_eq!(r.status, 200, "register ok: {:?}", json_of(&r));
    assert_eq!(h.node.processor.read().await.view_count(), 1);

    // A per-view metrics slot was seeded.
    assert!(h.node.view_metrics.read().await.contains_key("v1"));

    // Unregister → circuit drops back to 0.
    let r = h
        .node
        .route(authed(Method::Post, "/view/unregister", json!({ "id": "v1" })))
        .await
        .unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(h.node.processor.read().await.view_count(), 0);
    assert!(!h.node.view_metrics.read().await.contains_key("v1"));
}

/// Two sessions of the SAME user registering the same query id must SHARE one
/// view, not fight over it. This is the two-tab bug: the second registration
/// used to overwrite `auth_id` and `clientId` wholesale, after which the first
/// tab could no longer read or heartbeat its own `_00_query` row.
#[tokio::test]
async fn second_session_joins_an_existing_view_instead_of_replacing_it() {
    let h = build(HarnessOpts::default()).await;
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }

    let register = |client: &str| {
        json!({
            "id": "v1",
            "surql": "SELECT * FROM thread",
            "clientId": client,
            "ttl": "30m",
            "lastActiveAt": "2024-01-01T00:00:00Z",
            "params": { "auth": { "id": "user:alice" } }
        })
    };

    let r = h.node.route(authed(Method::Post, "/view/register", register("tab-a"))).await.unwrap();
    assert_eq!(r.status, 200, "first register: {:?}", json_of(&r));
    assert_eq!(h.node.processor.read().await.view_count(), 1);

    // Second tab, same user, same query.
    let r = h.node.route(authed(Method::Post, "/view/register", register("tab-b"))).await.unwrap();
    assert_eq!(r.status, 200, "second register: {:?}", json_of(&r));

    // Still ONE view: the second registration must not have replaced it (which
    // would silently discard the live view's cache and double the dependency
    // map) nor added a second one.
    assert_eq!(
        h.node.processor.read().await.view_count(),
        1,
        "a second subscriber must share the view, not create or replace one"
    );

    // Both sessions are recorded on the one shared row.
    let subs: Vec<serde_json::Value> = h
        .raw_db
        .query("SELECT VALUE subscribers FROM ONLY _00_query:v1")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    let ids: Vec<&str> = subs.iter().filter_map(|s| s.get("id")?.as_str()).collect();
    assert_eq!(ids.len(), 2, "each session records itself: {subs:?}");
    assert!(ids.contains(&"tab-a") && ids.contains(&"tab-b"), "got {ids:?}");

    // auth_id is write-once: the in-memory View.auth_id has no setter, so
    // letting the joiner rewrite the stored value would desynchronize the two
    // and route edges to one identity while stamping them with the other.
    let auth: Option<String> = h
        .raw_db
        .query("SELECT VALUE auth_id FROM ONLY _00_query:v1")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(auth.as_deref(), Some("user:alice"));
}

/// A query id that resolves to another identity's view must be refused, not
/// adopted. Ids are derived from (surql, params, auth), so arriving at someone
/// else's is either a guess or a forgery — and that view's plan was
/// permission-injected for THEIR identity, so serving it would leak their rows.
#[tokio::test]
async fn registering_onto_another_identitys_view_is_refused() {
    let h = build(HarnessOpts::default()).await;
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }

    let register = |auth: &str| {
        json!({
            "id": "v1",
            "surql": "SELECT * FROM thread",
            "clientId": "c1",
            "ttl": "30m",
            "lastActiveAt": "2024-01-01T00:00:00Z",
            "params": { "auth": { "id": auth } }
        })
    };

    let r = h.node.route(authed(Method::Post, "/view/register", register("user:alice"))).await.unwrap();
    assert_eq!(r.status, 200);

    let r = h.node.route(authed(Method::Post, "/view/register", register("user:mallory"))).await.unwrap();
    assert_eq!(r.status, 409, "expected auth_mismatch, got {:?}", json_of(&r));
    assert_eq!(json_of(&r)["code"], "auth_mismatch");

    // Alice's view is untouched.
    assert_eq!(h.node.processor.read().await.view_count(), 1);
}

// A registration that asserts NO identity must join, not be refused.
//
// This is a production regression, not a hypothetical: `fn::query::register`
// sends `<string>($auth.id OR '')`, so any re-registration issued while the
// session's auth is not yet established carries ''. After a SurrealDB restart
// every client re-registered mid-reconnect with '', every one was refused 409,
// and the views never came back — the page rendered "not found" for data the
// user could see a moment earlier. Refuse only a genuine identity CONFLICT.
#[tokio::test]
async fn registering_without_an_asserted_identity_joins_instead_of_being_refused() {
    let h = build(HarnessOpts::default()).await;
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }

    let register = |auth: &str, client: &str| {
        json!({
            "id": "v1",
            "surql": "SELECT * FROM thread",
            "clientId": client,
            "ttl": "30m",
            "lastActiveAt": "2024-01-01T00:00:00Z",
            "params": { "auth": { "id": auth } }
        })
    };

    let r = h.node.route(authed(Method::Post, "/view/register", register("user:alice", "tab-a"))).await.unwrap();
    assert_eq!(r.status, 200);

    // The reconnect case: same query, no identity asserted.
    let r = h.node.route(authed(Method::Post, "/view/register", register("", "tab-a"))).await.unwrap();
    assert_eq!(r.status, 200, "empty auth must join, got {:?}", json_of(&r));

    // Still one shared view, and `auth_id` was not clobbered to ''.
    assert_eq!(h.node.processor.read().await.view_count(), 1);
    let row: Option<serde_json::Value> = h
        .raw_db
        .query("SELECT auth_id FROM ONLY _00_query:v1")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(row.unwrap()["auth_id"], "user:alice");
}

#[tokio::test]
async fn view_register_rejects_when_not_ready() {
    let h = build(HarnessOpts { status: SspStatus::Bootstrapping, ..Default::default() }).await;
    let r = h
        .node
        .route(authed(Method::Post, "/view/register", json!({ "id": "v1", "surql": "SELECT * FROM t" })))
        .await
        .unwrap();
    assert_eq!(r.status, 503);
}

#[tokio::test]
async fn ingest_gated_when_not_ready() {
    let h = build(HarnessOpts { status: SspStatus::Bootstrapping, ..Default::default() }).await;
    let body = json!({ "table": "thread", "op": "CREATE", "id": "thread:1", "record": {} });
    let r = h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap();
    assert_eq!(r.status, 503);
}

#[tokio::test]
async fn heartbeat_seen_after_ingest_and_served_authed() {
    let h = build(HarnessOpts::default()).await;

    // Before any probe: nulls.
    let r = h.node.route(authed(Method::Get, "/debug/heartbeat", Value::Null)).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["hb_seq"], Value::Null);

    // Auth required.
    let r = h.node.route(req(Method::Get, "/debug/heartbeat", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 401);

    // A _00_heartbeat ingest records (hb_seq, received_at_ms) post circuit step.
    let body = json!({
        "table": "_00_heartbeat", "op": "UPDATE", "id": "_00_heartbeat:probe",
        "record": { "hb_seq": 1754690000123u64, "sent_at": "2026-08-08T00:00:00Z" }
    });
    let r = h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap();
    assert_eq!(r.status, 200);

    let r = h.node.route(authed(Method::Get, "/debug/heartbeat", Value::Null)).await.unwrap();
    assert_eq!(r.status, 200);
    assert_eq!(json_of(&r)["hb_seq"], 1754690000123u64);
    assert!(json_of(&r)["received_at_ms"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn ingest_bad_op_and_body() {
    let h = build(HarnessOpts::default()).await;
    let r = h.node.route(authed(Method::Post, "/ingest", json!("notjson-shape"))).await.unwrap();
    assert_eq!(r.status, 400);
    let bad_op = json!({ "table": "t", "op": "SIDEWAYS", "id": "t:1", "record": {} });
    let r = h.node.route(authed(Method::Post, "/ingest", bad_op)).await.unwrap();
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn ingest_routes_pending_job_to_queue() {
    // A pending job row on a configured job table → enqueued (standalone).
    let h = build(HarnessOpts { job_tables: vec![("job", "http://backend")], ..Default::default() }).await;
    let body = json!({
        "table": "job",
        "op": "CREATE",
        "id": "job:abc",
        "record": { "status": "pending", "path": "/run", "payload": {} }
    });
    let r = h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap();
    assert_eq!(r.status, 200);

    let mut rx = h.job_rx.lock().await;
    let job = rx.try_recv().expect("pending job enqueued via ingest");
    assert_eq!(job.id, "job:abc");
    assert_eq!(job.base_url, "http://backend");
}

#[tokio::test]
async fn ingest_non_pending_job_not_enqueued() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://backend")], ..Default::default() }).await;
    let body = json!({
        "table": "job", "op": "CREATE", "id": "job:done",
        "record": { "status": "success", "path": "/run", "payload": {} }
    });
    let r = h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap();
    assert_eq!(r.status, 200);
    assert!(h.job_rx.lock().await.try_recv().is_err(), "non-pending job must not enqueue");
}

/// Above the table's limit, ingest leaves the row alone. That is the whole
/// mechanism: the outbox row IS the queue entry, so there is nothing to hold in
/// memory and nothing to lose on a restart.
#[tokio::test]
async fn ingest_stops_at_the_table_concurrency_limit() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://backend")], ..Default::default() }).await;
    for id in ["job:a", "job:b", "job:c"] {
        let body = json!({
            "table": "job", "op": "CREATE", "id": id,
            "record": { "status": "pending", "path": "/run", "payload": {} }
        });
        assert_eq!(h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap().status, 200);
    }

    let mut rx = h.job_rx.lock().await;
    assert_eq!(rx.try_recv().expect("the first job runs").id, "job:a");
    assert!(
        rx.try_recv().is_err(),
        "the default limit is 1 — the rest stay pending in the outbox"
    );
}

/// A job waiting out its delay window is not running, so it must not be
/// holding the table's execution slot. Getting this wrong means one
/// `db.run(..., { delay: 3600000 })` stalls a `concurrency: 1` table for an
/// hour.
#[tokio::test]
async fn a_delayed_job_does_not_hold_a_slot() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://backend")], ..Default::default() }).await;
    let delayed = json!({
        "table": "job", "op": "CREATE", "id": "job:later",
        "record": { "status": "pending", "path": "/run", "payload": {}, "delay": 3_600_000 }
    });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", delayed)).await.unwrap().status, 200);

    let now = json!({
        "table": "job", "op": "CREATE", "id": "job:now",
        "record": { "status": "pending", "path": "/run", "payload": {} }
    });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", now)).await.unwrap().status, 200);

    let mut rx = h.job_rx.lock().await;
    let job = rx.try_recv().expect("the immediate job must not wait on the delayed one");
    assert_eq!(job.id, "job:now");
}

#[tokio::test]
async fn ingest_steps_circuit_for_registered_view() {
    // Register a view over `thread`, then ingest a matching row and confirm
    // the circuit produced a delta (view now has the record).
    let h = build(HarnessOpts::default()).await;
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }
    let reg = json!({ "id": "v1", "surql": "SELECT * FROM thread", "clientId": "c1", "ttl": "30m", "lastActiveAt": "2024-01-01T00:00:00Z" });
    assert_eq!(h.node.route(authed(Method::Post, "/view/register", reg)).await.unwrap().status, 200);

    let ingest = json!({ "table": "thread", "op": "CREATE", "id": "thread:1", "record": { "title": "hi" } });
    let r = h.node.route(authed(Method::Post, "/ingest", ingest)).await.unwrap();
    assert_eq!(r.status, 200);

    // The view's window now contains the row.
    let dbg = h.node.route(authed(Method::Get, "/debug/view/v1", Value::Null)).await.unwrap();
    assert_eq!(json_of(&dbg)["cache_size"].as_u64(), Some(1), "circuit stepped: {:?}", json_of(&dbg));
}

// One query must occupy ONE circuit slot however its id was spelled.
//
// A live client's id reaches the SSP through `fn::query::register` as
// `<string>$config.id`, and SurrealDB stringifies a record id WITH its table,
// so it arrives as `_00_query:<hash>`. Boot re-registration from `_00_query`
// (`bootstrap.rs`) and the TTL sweep both strip that prefix and speak the bare
// `<hash>`. Keying the circuit on whichever string turned up filed one query
// under two keys: after a restart without a snapshot the reconnecting client
// missed its own restored view and built a SECOND operator graph over the same
// rows, and both then sat in `dependency_map` and stepped on every ingest.
#[tokio::test]
async fn the_two_spellings_of_a_query_id_share_one_view_and_one_graph() {
    let h = build(HarnessOpts::default()).await;
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }

    let register = |id: &str| {
        json!({
            "id": id,
            "surql": "SELECT * FROM thread",
            "clientId": "tab-a",
            "ttl": "30m",
            "lastActiveAt": "2024-01-01T00:00:00Z",
            "params": { "auth": { "id": "user:alice" } }
        })
    };

    // The boot / sweep spelling…
    let r = h.node.route(authed(Method::Post, "/view/register", register("v1"))).await.unwrap();
    assert_eq!(r.status, 200);
    // …then the live-client spelling of the SAME query.
    let r = h
        .node
        .route(authed(Method::Post, "/view/register", register("_00_query:v1")))
        .await
        .unwrap();
    assert_eq!(r.status, 200, "prefixed id must join, got {:?}", json_of(&r));

    let c = h.node.processor.read().await;
    assert_eq!(c.view_count(), 1, "one query, one view");
    assert_eq!(c.graph_count(), 1, "one query, one operator graph");
}

// The TTL sweep must reclaim the in-memory view of a query a LIVE client
// registered, not just its row.
//
// The sweep reads `_00_query`, strips the `_00_query:` prefix and hands the
// bare key to the circuit. While live registrations were keyed by the prefixed
// spelling, that lookup never matched: the row and its edges went away while
// the operator graph (the ~45 MB this whole area exists to bound) stayed
// resident for the life of the process. Worse, a later re-registration found
// the orphan, took the "already exists" path, and so never republished the
// membership the sweep had just deleted.
#[tokio::test]
async fn ttl_sweep_reclaims_a_view_registered_under_the_prefixed_id() {
    use ssp_node::Runtime;
    let h = build(HarnessOpts { ref_mode: ssp_protocol::RefMode::Single, ..Default::default() }).await;
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }

    let reg = json!({
        "id": "_00_query:v1",
        "surql": "SELECT * FROM thread",
        "clientId": "tab-a",
        "ttl": "30m",
        "lastActiveAt": "2024-01-01T00:00:00Z",
        "params": { "auth": { "id": "user:alice" } }
    });
    assert_eq!(h.node.route(authed(Method::Post, "/view/register", reg)).await.unwrap().status, 200);
    assert_eq!(h.node.processor.read().await.view_count(), 1);

    // Expire it the way the sweep sees it.
    h.raw_db
        .query("UPDATE _00_query:v1 SET ttl = 1s, lastActiveAt = time::now() - 1h")
        .await
        .unwrap();

    Runtime::new(Arc::clone(&h.node)).on_timer(TimerKind::TtlCleanup).await;

    let c = h.node.processor.read().await;
    assert_eq!(c.view_count(), 0, "swept view released from the circuit");
    assert_eq!(c.graph_count(), 0, "…and its operator graph with it");
}

#[tokio::test]
async fn on_timer_ttl_cleanup_sweeps_and_rearms() {
    use ssp_node::Runtime;
    let h = build(HarnessOpts { ref_mode: ssp_protocol::RefMode::Single, ..Default::default() }).await;
    // An already-expired _00_query row.
    h.raw_db
        .query("CREATE _00_query:old SET auth_id = '', clientId = 'c', ttl = 1s, lastActiveAt = time::now() - 1h, surql = 'x', params = {};")
        .await
        .unwrap();

    let rt = Runtime::new(Arc::clone(&h.node));
    rt.on_timer(TimerKind::TtlCleanup).await;

    // Swept the expired row…
    let remaining: Option<i64> = h
        .raw_db
        .query("SELECT VALUE count() FROM _00_query GROUP ALL")
        .await
        .unwrap()
        .take(0)
        .unwrap_or(None);
    assert_eq!(remaining.unwrap_or(0), 0, "expired _00_query swept");
    // …and re-armed itself via the Scheduler port.
    assert!(
        h.scheduled.lock().unwrap().contains(&TimerKind::TtlCleanup),
        "TtlCleanup re-armed"
    );
}

#[tokio::test]
async fn on_timer_job_recovery_rearms_when_standalone() {
    use ssp_node::Runtime;
    let h = build(HarnessOpts { job_tables: vec![("job", "http://b")], ..Default::default() }).await;
    let rt = Runtime::new(Arc::clone(&h.node));
    rt.on_timer(TimerKind::JobRecoverySweep).await;
    assert!(
        h.scheduled.lock().unwrap().contains(&TimerKind::JobRecoverySweep),
        "standalone JobRecoverySweep re-arms"
    );
}

#[tokio::test]
async fn bootstrap_restore_evict_mutate_catches_up_to_full_rebuild() {
    // Headline test for the generic runtime seam: a snapshot restore + `_00_rv`
    // catch-up must converge to EXACTLY the same circuit content as a cold full
    // rebuild over the same (mutated) DB. Proves catch-up correctness and the
    // rebuild-fallback divergence backstop in one shot.
    use std::collections::BTreeMap;
    use ssp_node::{CircuitStore, Runtime};

    let store = MemStore::default();
    let h = build(HarnessOpts {
        status: SspStatus::Bootstrapping,
        circuit_store: Some(Arc::new(store.clone()) as Arc<dyn ssp_node::CircuitStore>),
        ..Default::default()
    })
    .await;

    // Schema + initial rows (each carries an `_00_rv`, as ingest would stamp).
    h.raw_db
        .query("DEFINE TABLE thread SCHEMALESS PERMISSIONS FOR select FULL;")
        .await
        .unwrap();
    h.raw_db
        .query(
            "CREATE thread:1 SET title = 'a', _00_rv = 1; \
             CREATE thread:2 SET title = 'b', _00_rv = 2;",
        )
        .await
        .unwrap();

    let rt = Runtime::new(Arc::clone(&h.node));

    // 1. Cold bootstrap: store empty → full rebuild → Ready.
    rt.bootstrap().await;
    assert_eq!(*h.node.status.read().await, SspStatus::Ready);
    let hashes_snapshot = h.node.processor.read().await.compute_table_hashes();
    assert!(hashes_snapshot.contains_key("thread"), "thread loaded");

    // 2. Persist a snapshot with a resume-point (max _00_rv folded in = 2).
    let blob = h.node.processor.read().await.save().unwrap();
    let mut max_rv = BTreeMap::new();
    max_rv.insert("thread".to_string(), 2i64);
    let point = ssp_node::ResumePoint {
        saved_at_epoch_ms: ssp_node::now_epoch_ms(),
        table_hashes: hashes_snapshot.clone(),
        max_row_version: max_rv,
    };
    store.save(&blob, &point).await.unwrap();

    // 3. Mutate the DB AFTER the snapshot: a new row + an update, both with a
    //    higher `_00_rv` (steady-state ingest bumps it).
    h.raw_db
        .query(
            "CREATE thread:3 SET title = 'c', _00_rv = 3; \
             UPDATE thread:1 SET title = 'a2', _00_rv = 4;",
        )
        .await
        .unwrap();

    // 4. Evict the in-memory circuit (host restart / DO eviction).
    *h.node.processor.write().await = Circuit::new();

    // 5. Warm bootstrap: load snapshot → restore + `_00_rv` catch-up.
    rt.bootstrap().await;
    assert_eq!(*h.node.status.read().await, SspStatus::Ready);
    let hashes_restored = h.node.processor.read().await.compute_table_hashes();

    // 6. Oracle: a cold full rebuild over the same mutated DB.
    let fresh = Arc::new(RwLock::new(Circuit::new()));
    ssp_node::bootstrap::rebuild_from_db(&MemDb(Arc::clone(&h.raw_db)), &fresh, 200)
        .await
        .unwrap();
    let hashes_fresh = fresh.read().await.compute_table_hashes();

    assert_eq!(
        hashes_restored, hashes_fresh,
        "restore+catch-up must converge to a full rebuild"
    );
    // And the catch-up actually moved past the snapshot (title update + new row).
    assert_ne!(
        hashes_restored, hashes_snapshot,
        "catch-up must reflect post-snapshot mutations"
    );
}

#[tokio::test]
async fn checkpoint_persists_snapshot_with_resume_point() {
    use ssp_node::{CircuitStore, Runtime};

    let store = MemStore::default();
    let mut h = build(HarnessOpts {
        circuit_store: Some(Arc::new(store.clone()) as Arc<dyn ssp_node::CircuitStore>),
        ..Default::default() // Ready
    })
    .await;
    // Checkpoints are opt-in (build() has no knob; mutate the node pre-share).
    Arc::get_mut(&mut h.node).unwrap().checkpoint_interval_secs = Some(300);

    // A couple of versioned rows in the circuit store.
    {
        let mut c = h.node.processor.write().await;
        c.load(vec![
            ssp::circuit::Record::new("thread", "1", json!({ "id": "thread:1", "_00_rv": 5 })),
            ssp::circuit::Record::new("thread", "2", json!({ "id": "thread:2", "_00_rv": 9 })),
        ]);
    }

    Runtime::new(Arc::clone(&h.node)).checkpoint().await;

    let (blob, point) = store.load().await.expect("snapshot persisted");
    assert!(!blob.is_empty());
    assert_eq!(point.max_row_version.get("thread"), Some(&9), "highest _00_rv folded in");
    assert!(point.table_hashes.contains_key("thread"));
    assert!(point.saved_at_epoch_ms > 0);
}

#[tokio::test]
async fn checkpoint_skips_when_not_ready() {
    use ssp_node::{CircuitStore, Runtime};
    let store = MemStore::default();
    let mut h = build(HarnessOpts {
        status: SspStatus::Bootstrapping,
        circuit_store: Some(Arc::new(store.clone()) as Arc<dyn ssp_node::CircuitStore>),
        ..Default::default()
    })
    .await;
    Arc::get_mut(&mut h.node).unwrap().checkpoint_interval_secs = Some(300);
    Runtime::new(Arc::clone(&h.node)).checkpoint().await;
    assert!(matches!(store.load().await, Err(ssp_node::CircuitStoreError::NotFound)));
}

#[tokio::test]
async fn checkpoint_writes_nothing_when_disabled() {
    // `checkpoint_interval_secs: None` means off — for the shutdown checkpoint
    // as much as for the timer. A cluster node bootstraps from the scheduler
    // proxy and never restores a snapshot, so the only thing a write here
    // could do is stall ingest under the circuit lock (709 MB per checkpoint
    // on the first tenant this was measured on).
    use ssp_node::{CircuitStore, Runtime};
    let store = MemStore::default();
    let h = build(HarnessOpts {
        circuit_store: Some(Arc::new(store.clone()) as Arc<dyn ssp_node::CircuitStore>),
        ..Default::default() // Ready, interval None
    })
    .await;
    {
        let mut c = h.node.processor.write().await;
        c.load(vec![ssp::circuit::Record::new(
            "thread",
            "1",
            json!({ "id": "thread:1", "_00_rv": 5 }),
        )]);
    }
    Runtime::new(Arc::clone(&h.node)).checkpoint().await;
    assert!(matches!(store.load().await, Err(ssp_node::CircuitStoreError::NotFound)));
}

#[tokio::test]
async fn on_timer_circuit_checkpoint_rearms_only_when_enabled() {
    use ssp_node::Runtime;

    // Disabled (interval None, the VM default) → no re-arm.
    let h = build(HarnessOpts::default()).await;
    Runtime::new(Arc::clone(&h.node)).on_timer(TimerKind::CircuitCheckpoint).await;
    assert!(
        !h.scheduled.lock().unwrap().contains(&TimerKind::CircuitCheckpoint),
        "disabled checkpoint must not re-arm"
    );

    // Enabled → re-arms via the Scheduler port.
    let store = MemStore::default();
    let mut h = build(HarnessOpts {
        circuit_store: Some(Arc::new(store) as Arc<dyn ssp_node::CircuitStore>),
        ..Default::default()
    })
    .await;
    // Flip on the interval (build() has no knob; mutate the Arc'd node pre-share).
    Arc::get_mut(&mut h.node).unwrap().checkpoint_interval_secs = Some(300);
    Runtime::new(Arc::clone(&h.node)).on_timer(TimerKind::CircuitCheckpoint).await;
    assert!(
        h.scheduled.lock().unwrap().contains(&TimerKind::CircuitCheckpoint),
        "enabled checkpoint re-arms"
    );
}

#[tokio::test]
async fn admin_reload_picks_up_schema_defined_after_bootstrap() {
    // A table defined AFTER the node is up is invisible to view registration
    // (permissions are captured at bootstrap). POST /admin/reload re-scans the
    // DB and makes it available.
    let h = build(HarnessOpts::default()).await; // Ready, empty circuit

    // A view over a not-yet-known table default-denies.
    let reg = json!({ "id": "v1", "surql": "SELECT * FROM late", "clientId": "c", "ttl": "30m", "lastActiveAt": "2024-01-01T00:00:00Z" });
    let r = h.node.route(authed(Method::Post, "/view/register", reg.clone())).await.unwrap();
    assert_ne!(r.status, 200, "unknown table should not register: {:?}", json_of(&r));

    // Define the table in the DB, then reload.
    h.raw_db
        .query("DEFINE TABLE late SCHEMALESS PERMISSIONS FOR select FULL; CREATE late:1 SET x = 1;")
        .await
        .unwrap();
    let r = h.node.route(authed(Method::Post, "/admin/reload", Value::Null)).await.unwrap();
    assert_eq!(r.status, 200, "reload ok: {:?}", json_of(&r));

    // Now the table is known — the view registers and sees the row.
    let r = h.node.route(authed(Method::Post, "/view/register", reg)).await.unwrap();
    assert_eq!(r.status, 200, "registers after reload: {:?}", json_of(&r));
    let dbg = h.node.route(authed(Method::Get, "/debug/view/v1", Value::Null)).await.unwrap();
    assert_eq!(json_of(&dbg)["cache_size"].as_u64(), Some(1), "reloaded row visible");
}

#[tokio::test]
async fn admin_reload_requires_auth() {
    let h = build(HarnessOpts::default()).await;
    let r = h.node.route(req(Method::Post, "/admin/reload", None, Value::Null)).await.unwrap();
    assert_eq!(r.status, 401);
}

#[tokio::test]
async fn log_accepts_and_swallows() {
    let h = build(HarnessOpts::default()).await;
    let r = h
        .node
        .route(authed(Method::Post, "/log", json!({ "message": "hi", "level": "warn" })))
        .await
        .unwrap();
    assert_eq!(r.status, 200);
}

// --- declarative schedules (standalone wiring) -------------------------------

/// The DDL the CLI ships for the scheduling tables, so this test exercises the
/// same schema a deploy applies.
const SCHEDULE_TABLES: &str = include_str!("../../../apps/cli/src/schedule_tables.surql");

/// The end-to-end singlenode path: the schedule sweep creates a `pending` outbox
/// row, the row's mutation event reaches `/ingest`, and the existing CREATE-only
/// pickup enqueues it for the runner. Everything past the queue is the runner's
/// job, which `jobs::db_tests` covers.
#[tokio::test]
async fn a_due_schedule_spawns_a_job_that_ingest_picks_up() {
    let h = build(HarnessOpts {
        job_tables: vec![("job", "http://backend")],
        schedules: true,
        ..Default::default()
    })
    .await;
    h.raw_db.query(SCHEDULE_TABLES).await.expect("scheduling schema");
    h.raw_db
        .query(
            "DEFINE TABLE job SCHEMALESS; \
             CREATE _00_schedule:nightly CONTENT { name: 'nightly', kind: 'job', \
             every_ms: 300000, target_table: 'job', path: '/cleanup' }",
        )
        .await
        .expect("define schedule");

    let engine = h.node.schedule_engine.clone().expect("standalone wires an engine");

    // First sweep plans; pull the clock back so the second one fires.
    engine.tick_pass().await.unwrap();
    h.raw_db
        .query("UPDATE _00_schedule:nightly SET next_fire_at = time::now() - 1s")
        .await
        .unwrap();
    let report = engine.tick_pass().await.unwrap();
    assert_eq!(report.spawned, 1, "the sweep spawned an atomic job");

    let spawned: Vec<String> = h
        .raw_db
        .query("SELECT VALUE type::string(id) FROM job")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(spawned.len(), 1);
    let job_id = spawned[0].clone();

    // The mutation event a real deploy installs would POST exactly this.
    let body = json!({
        "table": "job",
        "op": "CREATE",
        "id": job_id,
        "record": { "status": "pending", "path": "/cleanup", "payload": {} }
    });
    let r = h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap();
    assert_eq!(r.status, 200);

    let mut rx = h.job_rx.lock().await;
    let job = rx.try_recv().expect("the scheduled job reached the runner queue");
    assert_eq!(job.id, job_id);
    assert_eq!(job.path, "/cleanup");
}

/// Cluster mode must not tick: the scheduler service is the single ticker, and a
/// second one would only lose the claim CAS and waste the work.
#[tokio::test]
async fn cluster_mode_builds_no_schedule_engine() {
    let db: Arc<dyn Db> = Arc::new(MemDb(Arc::new({
        let db = Surreal::new::<Mem>(()).await.unwrap();
        db.use_ns("test").use_db("test").await.unwrap();
        db
    })));
    assert!(
        ssp_node::schedules::build_engine(false, Arc::clone(&db), JobControl::new()).is_none(),
        "cluster mode leaves ticking to the scheduler service"
    );
    assert!(ssp_node::schedules::build_engine(true, db, JobControl::new()).is_some());
}

// --- Merged views ------------------------------------------------------------
//
// Two registrations that compute the same thing must share ONE operator graph
// (the ~45MB), while keeping their own `_00_query` row and their own edges.
// The teardown cases are the reason this is gated behind a flag: releasing one
// holder must not destroy a graph another is still reading.

fn merge_register(id: &str, auth: &str) -> Value {
    json!({
        "id": id,
        "surql": "SELECT * FROM thread",
        "clientId": id,
        "ttl": "30m",
        "lastActiveAt": "2024-01-01T00:00:00Z",
        "params": { "auth": { "id": auth } }
    })
}

async fn merging_harness() -> Harness {
    let h = build(HarnessOpts { merge_views: true, ..Default::default() }).await;
    // DEFINEd in the DB as well as set in memory, so a rebuild-from-DB in the
    // same test rediscovers the table and its select permission — otherwise
    // boot re-registration rejects every row and the circuit comes back empty.
    h.raw_db
        .query("DEFINE TABLE thread SCHEMALESS PERMISSIONS FOR select FULL;")
        .await
        .unwrap();
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }
    h
}

#[tokio::test]
async fn two_registrations_of_one_computation_share_one_graph() {
    let h = merging_harness().await;

    for id in ["_00_query:a", "_00_query:b"] {
        let r = h.node.route(authed(Method::Post, "/view/register", merge_register(id, "user:alice"))).await.unwrap();
        assert_eq!(r.status, 200, "register {id}: {:?}", json_of(&r));
    }

    // The whole point: one DAG, not two.
    assert_eq!(h.node.processor.read().await.graph_count(), 1);

    // Both rows exist independently, and the joiner got a rowCount so it can
    // tell "no results" from "edges still flushing".
    for id in ["a", "b"] {
        let row: Option<Value> = h.raw_db
            .query(format!("SELECT rowCount FROM ONLY _00_query:{id}"))
            .await.unwrap().take(0).unwrap();
        assert!(row.is_some(), "row {id} should exist");
    }
}

// A restart must not un-merge the tenant.
//
// Boot re-registration reads `_00_query` and rebuilds a graph per row. Without
// merging there too, every restart undoes the sharing and rebuilds N identical
// graphs — the exact memory blowup merging exists to prevent, reintroduced on
// the one event guaranteed to happen.
#[tokio::test]
async fn boot_re_registration_rebuilds_onto_one_shared_graph() {
    let h = merging_harness().await;
    for id in ["_00_query:a", "_00_query:b"] {
        let r = h.node.route(authed(Method::Post, "/view/register", merge_register(id, "user:alice"))).await.unwrap();
        assert_eq!(r.status, 200, "register {id}: {:?}", json_of(&r));
    }
    assert_eq!(h.node.processor.read().await.graph_count(), 1);

    // Cold rebuild from the DB rows those registrations left behind.
    let fresh = Arc::new(RwLock::new(Circuit::new()));
    fresh.write().await.set_merge_views(true);
    ssp_node::bootstrap::rebuild_from_db(&MemDb(Arc::clone(&h.raw_db)), &fresh, 200)
        .await
        .unwrap();

    let c = fresh.read().await;
    assert_eq!(c.view_count(), 1, "one view for one computation");
    assert_eq!(c.graph_count(), 1, "…backed by one graph");
    assert_eq!(c.subscribers_of("a").len() + c.subscribers_of("b").len(), 1, "the other row re-registered as a subscriber");
}

// The same boot, with merging off, must keep one graph per row — the flag is
// what gates the risk, and a rebuild path that merged regardless would enable
// the behaviour behind the operator's back.
#[tokio::test]
async fn boot_re_registration_does_not_merge_when_the_flag_is_off() {
    let h = merging_harness().await;
    for id in ["_00_query:a", "_00_query:b"] {
        h.node.route(authed(Method::Post, "/view/register", merge_register(id, "user:alice"))).await.unwrap();
    }

    let fresh = Arc::new(RwLock::new(Circuit::new()));
    ssp_node::bootstrap::rebuild_from_db(&MemDb(Arc::clone(&h.raw_db)), &fresh, 200)
        .await
        .unwrap();

    assert_eq!(fresh.read().await.graph_count(), 2, "merging off: one graph per row");
}

// A snapshot restore must keep the sharing, and a registration arriving after
// it must still find the shared graph. Subscribers hold no view of their own,
// so without the persisted bookkeeping they simply vanish.
#[tokio::test]
async fn a_snapshot_restore_keeps_subscribers_attached_to_the_shared_graph() {
    let h = merging_harness().await;
    for id in ["_00_query:a", "_00_query:b"] {
        h.node.route(authed(Method::Post, "/view/register", merge_register(id, "user:alice"))).await.unwrap();
    }

    let blob = h.node.processor.read().await.save().unwrap();
    let mut restored = Circuit::restore(&blob).unwrap();
    restored.set_merge_views(true);

    assert_eq!(restored.graph_count(), 1);
    assert_eq!(restored.subscribers_of("a").len(), 1, "b still shares a's graph");

    // An ingest reaches both rows.
    let ids: Vec<String> = restored
        .step(ssp::circuit::store::ChangeSet {
            changes: vec![ssp::circuit::store::Change::create("thread", "t1", json!({ "title": "x" }))],
        })
        .into_iter()
        .map(|d| d.query_id)
        .collect();
    assert_eq!(ids.len(), 2, "both registrations get the delta, got {ids:?}");
}

#[tokio::test]
async fn a_different_identity_does_not_share_an_auth_scoped_graph() {
    // Same surql, but the permission dereferences $auth.id, so the merge key
    // differs and each identity gets its own graph. A shared graph here would
    // serve one user the other's rows.
    let h = build(HarnessOpts { merge_views: true, ..Default::default() }).await;
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "author.id = $auth.id");
    }

    for (id, auth) in [("_00_query:a", "user:alice"), ("_00_query:b", "user:bob")] {
        let r = h.node.route(authed(Method::Post, "/view/register", merge_register(id, auth))).await.unwrap();
        assert_eq!(r.status, 200, "register {id}: {:?}", json_of(&r));
    }

    assert_eq!(h.node.processor.read().await.graph_count(), 2, "identities must not share");
}

#[tokio::test]
async fn releasing_one_holder_leaves_the_shared_graph_for_the_other() {
    // THE regression this whole change exists to avoid: before refcounting,
    // the first tab to leave destroyed the graph and blanked the other's list.
    let h = merging_harness().await;
    for id in ["_00_query:a", "_00_query:b"] {
        h.node.route(authed(Method::Post, "/view/register", merge_register(id, "user:alice"))).await.unwrap();
    }
    assert_eq!(h.node.processor.read().await.graph_count(), 1);

    // The OWNER leaves first, which is the harder direction.
    let r = h.node.route(authed(Method::Post, "/view/unregister", json!({ "id": "_00_query:a" }))).await.unwrap();
    assert_eq!(r.status, 200);

    assert_eq!(
        h.node.processor.read().await.graph_count(), 1,
        "graph must survive while another registration still holds it"
    );
}

#[tokio::test]
async fn releasing_the_last_holder_removes_the_shared_graph() {
    let h = merging_harness().await;
    for id in ["_00_query:a", "_00_query:b"] {
        h.node.route(authed(Method::Post, "/view/register", merge_register(id, "user:alice"))).await.unwrap();
    }
    for id in ["_00_query:a", "_00_query:b"] {
        h.node.route(authed(Method::Post, "/view/unregister", json!({ "id": id }))).await.unwrap();
    }
    assert_eq!(
        h.node.processor.read().await.graph_count(), 0,
        "nothing holds it any more; the memory must actually come back"
    );
}

#[tokio::test]
async fn merging_disabled_keeps_one_graph_per_registration() {
    // Default path. Proves the flag genuinely gates the behaviour change.
    let h = build(HarnessOpts::default()).await;
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }
    for id in ["_00_query:a", "_00_query:b"] {
        h.node.route(authed(Method::Post, "/view/register", merge_register(id, "user:alice"))).await.unwrap();
    }
    assert_eq!(h.node.processor.read().await.graph_count(), 2);
}

// --- the recovery sweep reclaims on the lease, not on ownership --------------
//
// The sweep is only reachable from this harness (it is a method on `SspNode`, and the
// in-crate job tests never build one), so the reclaim half of the lease lives here.

/// Insert a `processing` row that is being held by a named, LIVE owner whose lease has
/// already run out. This is the exact shape that was unreclaimable at any age: the
/// cluster sweep skipped it because the assignee was in the pool, and the singlenode
/// sweep skipped it because the request was still marked in-flight.
async fn insert_held_job(db: &Surreal<MemEngine>, id: &str, lease: &str, epoch: i64) {
    db.query(format!(
        "CREATE job:{id} SET status = 'processing', path = '/run', payload = {{}}, \
         retries = 0, max_retries = 3, retry_strategy = 'linear', errors = [], \
         assignee = 'test-ssp', lease_epoch = {epoch}, \
         lease_until = time::now() {lease}, \
         created_at = time::now(), updated_at = time::now()"
    ))
    .await
    .unwrap();
}

/// A string field, raw. Deliberately NOT via `type::string(...)`: that renders a NONE
/// as the literal text "NONE", so "the field was cleared" and "the field holds the word
/// NONE" would read identically — and clearing it is exactly what these assert.
async fn job_field(db: &Surreal<MemEngine>, id: &str, field: &str) -> Option<String> {
    db.query(format!("SELECT VALUE {field} FROM ONLY job:{id}"))
        .await
        .unwrap()
        .take(0)
        .unwrap()
}

#[tokio::test]
async fn the_sweep_reclaims_an_expired_lease_from_a_live_owner() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://b")], ..Default::default() }).await;
    // `test-ssp` is this node's own id, so the owner could not look more alive.
    insert_held_job(&h.raw_db, "held", "- 1s", 4).await;

    h.node.job_recovery_sweep().await;

    assert_eq!(
        job_status(&h.raw_db, "held").await.as_deref(),
        Some("pending"),
        "an expired lease is reclaimed however alive its owner looks"
    );
    assert_eq!(
        job_field(&h.raw_db, "held", "assignee").await,
        None,
        "and ownership is released, or no other node could ever drain the row"
    );
    let epoch: Option<i64> =
        h.raw_db.query("SELECT VALUE lease_epoch FROM ONLY job:held").await.unwrap().take(0).unwrap();
    assert_eq!(epoch, Some(5), "the fence moves on, so the old attempt cannot write");
}

#[tokio::test]
async fn the_sweep_leaves_a_live_lease_alone() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://b")], ..Default::default() }).await;
    insert_held_job(&h.raw_db, "working", "+ 1h", 1).await;

    h.node.job_recovery_sweep().await;

    assert_eq!(
        job_status(&h.raw_db, "working").await.as_deref(),
        Some("processing"),
        "work inside its lease must not be taken away from it"
    );
    assert_eq!(job_field(&h.raw_db, "working", "assignee").await.as_deref(), Some("test-ssp"));
}

/// A row on a table deployed before the lease fields existed has no `lease_until`, so
/// the sweep falls back to the `updated_at` staleness rule it always used. Neither
/// deployment gets worse, and none needs a coordinated upgrade.
#[tokio::test]
async fn an_unleased_row_still_follows_the_old_staleness_rule() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://b")], ..Default::default() }).await;
    // Fresh: inside the legacy window, so untouched.
    insert_job(&h.raw_db, "fresh", "processing").await;
    // Old: past it, so reclaimed — exactly today's behaviour.
    h.raw_db
        .query(
            "CREATE job:ancient SET status = 'processing', path = '/run', payload = {}, \
             retries = 0, max_retries = 3, retry_strategy = 'linear', errors = [], \
             created_at = time::now() - 700s, updated_at = time::now() - 700s",
        )
        .await
        .unwrap();

    h.node.job_recovery_sweep().await;

    assert_eq!(
        job_status(&h.raw_db, "fresh").await.as_deref(),
        Some("processing"),
        "a row with no lease is not reclaimed on sight"
    );
    assert_eq!(
        job_status(&h.raw_db, "ancient").await.as_deref(),
        Some("pending"),
        "but the legacy window still applies to it"
    );
}

/// A request hung inside the HTTP call is the case the old guard got exactly backwards:
/// being in-flight on this node meant "never touch it", so the one shape that most
/// needed recovering was the one shape that could never be recovered. It now means
/// "cancel it first".
#[tokio::test]
async fn the_sweep_cancels_a_local_request_past_its_lease() {
    let h = build(HarnessOpts { job_tables: vec![("job", "http://b")], ..Default::default() }).await;
    insert_held_job(&h.raw_db, "hung", "- 1s", 1).await;
    // Stand in for the runner having a request out for this job.
    let watch = h.node.job_control.register_inflight("job:hung");
    assert!(!watch.is_cancelled(), "not cancelled before the sweep");

    h.node.job_recovery_sweep().await;

    assert!(
        watch.is_cancelled(),
        "the hung request is stopped, not left running while its replacement goes"
    );
    assert_eq!(job_status(&h.raw_db, "hung").await.as_deref(), Some("pending"));
}

// --- Stranded views ----------------------------------------------------------
//
// The warm ("view already exists") register path is metadata-only, on the
// assumption that a joiner can read current membership straight out of
// `_00_list_ref*`. That assumption fails whenever the circuit view outlives its
// DB rows: the metadata UPDATE recreates `_00_query` with `rowCount` back at its
// schema DEFAULT 0 and nothing behind it, which a client cannot tell from a
// genuinely empty result. It stops loading and renders nothing, for good.
//
// `Node::repair_stranded_view` is what closes that: it probes, and republishes
// from the live view when the DB side has gone missing.

async fn thread_harness() -> Harness {
    let h = build(HarnessOpts { ref_mode: ssp_protocol::RefMode::Single, ..Default::default() }).await;
    h.raw_db
        .query("DEFINE TABLE thread SCHEMALESS PERMISSIONS FOR select FULL;")
        .await
        .unwrap();
    {
        let mut c = h.node.processor.write().await;
        c.set_permission("thread", "true");
    }
    h
}

fn thread_register(id: &str) -> Value {
    json!({
        "id": id,
        "surql": "SELECT * FROM thread",
        "clientId": "tab-a",
        "ttl": "30m",
        "lastActiveAt": "2024-01-01T00:00:00Z",
        "params": { "auth": { "id": "user:alice" } }
    })
}

async fn row_count_of(h: &Harness, key: &str) -> i64 {
    let v: Option<i64> = h
        .raw_db
        .query(format!("SELECT VALUE rowCount FROM ONLY _00_query:{key}"))
        .await
        .unwrap()
        .take(0)
        .unwrap_or(None);
    v.unwrap_or(-1)
}

async fn state_of(h: &Harness, key: &str) -> String {
    let v: Option<String> = h
        .raw_db
        .query(format!("SELECT VALUE state FROM ONLY _00_query:{key}"))
        .await
        .unwrap()
        .take(0)
        .unwrap_or(None);
    v.unwrap_or_else(|| "<none>".to_string())
}

async fn edge_count_of(h: &Harness, key: &str) -> i64 {
    let n: Option<i64> = h
        .raw_db
        .query(format!("SELECT VALUE count() FROM _00_list_ref WHERE in = _00_query:{key} GROUP ALL"))
        .await
        .unwrap()
        .take(0)
        .unwrap_or(None);
    n.unwrap_or(0)
}

// `_00_query.state` is how a client tells "the SSP is still publishing this
// view" from "the SSP says this view is empty". The test node has no flusher
// task (its receiver is dropped), so registration edges take the direct write
// path and both the edges and the `ready` flip are in the DB by the time
// `/view/register` answers.

#[tokio::test]
async fn cold_register_of_a_populated_view_ends_ready_with_one_edge_per_row() {
    let h = thread_harness().await;
    let ingest = json!({ "table": "thread", "op": "CREATE", "id": "thread:1", "record": { "title": "t" } });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", ingest)).await.unwrap().status, 200);

    assert_eq!(
        h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap().status,
        200
    );
    assert_eq!(row_count_of(&h, "v1").await, 1);
    assert_eq!(edge_count_of(&h, "v1").await, 1);
    assert_eq!(
        state_of(&h, "v1").await,
        "ready",
        "the initial publish flips the row to ready in the same transaction as its edges"
    );
}

#[tokio::test]
async fn cold_register_of_an_empty_view_is_ready_at_once() {
    let h = thread_harness().await;
    assert_eq!(
        h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap().status,
        200
    );
    assert_eq!(row_count_of(&h, "v1").await, 0);
    assert_eq!(edge_count_of(&h, "v1").await, 0);
    // An empty initial delta never reaches the edge writer, so nothing would
    // ever flip this row; it has to be born ready or the client could never
    // trust "0 edges, rowCount 0" as a real empty result.
    assert_eq!(state_of(&h, "v1").await, "ready");
}

#[tokio::test]
async fn incremental_ingest_leaves_state_ready() {
    let h = thread_harness().await;
    assert_eq!(
        h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap().status,
        200
    );
    let ingest = json!({ "table": "thread", "op": "CREATE", "id": "thread:1", "record": { "title": "t" } });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", ingest)).await.unwrap().status, 200);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(edge_count_of(&h, "v1").await, 1);
    assert_eq!(state_of(&h, "v1").await, "ready", "an increment must not rewrite state");
}

#[tokio::test]
async fn re_registration_republishes_a_view_whose_rows_went_missing() {
    let h = thread_harness().await;

    assert_eq!(
        h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap().status,
        200
    );
    // Give the view something to hold, so the repair has membership to restore.
    let ingest = json!({ "table": "thread", "op": "CREATE", "id": "thread:1", "record": { "title": "t" } });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", ingest)).await.unwrap().status, 200);
    assert_eq!(h.node.processor.read().await.get_view("v1").map(|v| v.cache.len()), Some(1));
    // The ingest delta's edge write is spawned; let it land before wiping, or
    // it lands after and skews the edge counts below.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Wipe the DB side the way a TTL sweep would, but leave the circuit view
    // in place — that mismatch is the whole bug.
    h.raw_db.query("DELETE _00_query:v1->_00_list_ref; DELETE _00_query:v1;").await.unwrap();
    // ...and leave two orphaned edges behind, the way a scheduler restart does
    // (its `_00_query` wipe cannot reach the edges). A republish must replace
    // them, not pile a third copy on top.
    h.raw_db
        .query(
            "RELATE _00_query:v1->_00_list_ref->thread:1 SET version = 1, clientId = 'x', auth_id = ''; \
             RELATE _00_query:v1->_00_list_ref->thread:1 SET version = 1, clientId = 'x', auth_id = '';",
        )
        .await
        .unwrap();
    assert_eq!(edge_count_of(&h, "v1").await, 2, "precondition: orphan duplicates seeded");

    // The re-registration now takes the warm path against an empty DB.
    let r = h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap();
    assert_eq!(r.status, 200, "warm re-register: {:?}", json_of(&r));

    assert_eq!(
        row_count_of(&h, "v1").await,
        1,
        "rowCount must be restored from the live view, not left at its DEFAULT 0 — \
         a client reads 0-with-no-edges as 'confirmed empty' and stops loading forever"
    );
    assert_eq!(state_of(&h, "v1").await, "ready", "the republish must end in ready");
    assert_eq!(
        edge_count_of(&h, "v1").await,
        1,
        "a full publish replaces the row's edges: no duplicates over the orphans"
    );

    // The regression the early return exists to prevent: a repair must publish
    // what the view already holds, never rebuild it.
    let c = h.node.processor.read().await;
    assert_eq!(c.view_count(), 1, "repair must not insert a second view");
    assert_eq!(c.graph_count(), 1, "repair must not build a second operator graph");
}

#[tokio::test]
async fn re_registration_leaves_a_healthy_view_alone() {
    let h = thread_harness().await;

    assert_eq!(
        h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap().status,
        200
    );
    let ingest = json!({ "table": "thread", "op": "CREATE", "id": "thread:1", "record": { "title": "t" } });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", ingest)).await.unwrap().status, 200);

    // The ingest delta's edge write is spawned, so let it land before reading.
    let edges = || async {
        let n: Option<i64> = h
            .raw_db
            .query("SELECT VALUE count() FROM _00_list_ref WHERE in = _00_query:v1 GROUP ALL")
            .await
            .unwrap()
            .take(0)
            .unwrap_or(None);
        n.unwrap_or(0)
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Edges present: the probe must find them and return without republishing,
    // because `build_edge_batch` writes additions with a bare RELATE and a
    // blind republish would duplicate every row.
    let before = edges().await;
    assert!(before > 0, "precondition: the ingest delta produced edges");

    assert_eq!(
        h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap().status,
        200
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(before, edges().await, "a healthy view must not have its edges republished");
}

// A view registered before its session had an identity is broken twice over:
// its edges go to the global `_00_list_ref` stamped `auth_id = ''`, which that
// table's own rule (`auth_id = $auth.id`) makes unreadable to the very user who
// registered it, AND its plan resolved every `$auth.id` predicate against ''
// so it holds the wrong rows. Write-once would keep it that way for life, so an
// identity-asserting re-registration must REBUILD it, not relabel it.
#[tokio::test]
async fn an_authenticated_re_registration_rebuilds_a_view_with_no_identity() {
    let h = thread_harness().await;

    let anonymous = json!({
        "id": "_00_query:v1",
        "surql": "SELECT * FROM thread",
        "clientId": "tab-a",
        "ttl": "30m",
        "lastActiveAt": "2024-01-01T00:00:00Z",
        "params": { "auth": { "id": "" } }
    });
    assert_eq!(h.node.route(authed(Method::Post, "/view/register", anonymous)).await.unwrap().status, 200);
    assert_eq!(h.node.processor.read().await.get_view("v1").map(|v| v.auth_id.clone()), Some(String::new()));

    // Same query id, this time asserting who is asking.
    let r = h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap();
    assert_eq!(r.status, 200, "an identity-asserting re-register must join, not 409: {:?}", json_of(&r));

    assert_eq!(
        h.node.processor.read().await.get_view("v1").map(|v| v.auth_id.clone()),
        Some("user:alice".to_string()),
        "the view must carry the caller's identity so its edges become reachable"
    );
    let stored: Option<String> = h
        .raw_db
        .query("SELECT VALUE auth_id FROM ONLY _00_query:v1")
        .await
        .unwrap()
        .take(0)
        .unwrap_or(None);
    assert_eq!(stored.as_deref(), Some("user:alice"), "and persist it to _00_query");

    // Rebuilt, not duplicated: the discard has to leave the circuit clean or
    // the cold path stacks a second graph and every ingest steps it twice.
    let c = h.node.processor.read().await;
    assert_eq!(c.view_count(), 1);
    assert_eq!(c.graph_count(), 1);
}

// The guard the adoption above must not weaken: one concrete identity may never
// be replaced by another, or edges get stamped for one user and routed to
// another's table.
#[tokio::test]
async fn adoption_never_overwrites_a_concrete_identity() {
    let h = thread_harness().await;
    assert_eq!(
        h.node.route(authed(Method::Post, "/view/register", thread_register("_00_query:v1"))).await.unwrap().status,
        200
    );

    let mallory = json!({
        "id": "_00_query:v1",
        "surql": "SELECT * FROM thread",
        "clientId": "tab-b",
        "ttl": "30m",
        "lastActiveAt": "2024-01-01T00:00:00Z",
        "params": { "auth": { "id": "user:mallory" } }
    });
    let r = h.node.route(authed(Method::Post, "/view/register", mallory)).await.unwrap();
    assert_eq!(r.status, 409, "cross-identity join must still be refused");
    assert_eq!(
        h.node.processor.read().await.get_view("v1").map(|v| v.auth_id.clone()),
        Some("user:alice".to_string())
    );
}

/// A body whose `_00_rv` is missing or does not advance still has to reach
/// subscribers: the circuit floors it above the stored version (server-side
/// policy, applied by `apply_circuit_policy`). This is the measured failure
/// mode of a partially built `_00_version.record_id` index, where the
/// mutation event stamps NONE and peers never refetch.
#[tokio::test]
async fn ingest_floors_missing_or_stale_row_versions() {
    let h = build(HarnessOpts::default()).await;
    h.node.apply_circuit_policy().await;
    let version = || async { h.node.processor.read().await.store.get_record_version_by_key("thread:1") };

    let body = json!({ "table": "thread", "op": "CREATE", "id": "thread:1", "record": { "title": "a", "_00_rv": 5 } });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap().status, 200);
    assert_eq!(version().await, Some(5));

    // The event's `_00_version` lookup came back NONE: no version in the body.
    let body = json!({ "table": "thread", "op": "UPDATE", "id": "thread:1", "record": { "title": "b" } });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap().status, 200);
    assert_eq!(version().await, Some(6), "a missing version advances past the stored one");

    // A version that did not move.
    let body = json!({ "table": "thread", "op": "UPDATE", "id": "thread:1", "record": { "title": "c", "_00_rv": 6 } });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap().status, 200);
    assert_eq!(version().await, Some(7));

    // A real advance is kept as stamped.
    let body = json!({ "table": "thread", "op": "UPDATE", "id": "thread:1", "record": { "title": "d", "_00_rv": 12 } });
    assert_eq!(h.node.route(authed(Method::Post, "/ingest", body)).await.unwrap().status, 200);
    assert_eq!(version().await, Some(12));
    assert_eq!(h.node.processor.read().await.synthesized_row_versions(), 2);
}

#[tokio::test]
async fn reload_repairs_versions_deleted_memberships_and_shared_empty_views() {
    let h = build(HarnessOpts { merge_views: true, ..Default::default() }).await;
    h.raw_db.query("DEFINE TABLE thread SCHEMALESS PERMISSIONS FULL; \
        CREATE thread:1 SET title = 'current', public_key = 'new'; \
        CREATE _00_version:v1 SET record_id = thread:1, version = 7;")
        .await.unwrap().check().unwrap();
    for (id, surql) in [("v1", "SELECT * FROM thread"), ("v2", "SELECT * FROM thread"),
                       ("empty", "SELECT * FROM thread WHERE title = 'deleted'")] {
        h.raw_db.query("CREATE type::record('_00_query', $id) SET surql = $sql, \
            clientId = 'c', auth_id = '', params = {}, ttl = 30m, lastActiveAt = time::now(), rowCount = 2;")
            .bind(("id", id)).bind(("sql", surql)).await.unwrap().check().unwrap();
        h.raw_db.query(format!("RELATE _00_query:{id}->_00_list_ref->thread:1 SET version = 1; \
            RELATE _00_query:{id}->_00_list_ref->thread:deleted SET version = 1;"))
            .await.unwrap().check().unwrap();
    }
    for _ in 0..2 {
        h.node.reload().await.unwrap();
        for id in ["v1", "v2"] {
            let mut result = h.raw_db.query(format!("SELECT version, out FROM _00_list_ref WHERE in = _00_query:{id}"))
                .await.unwrap();
            let rows: surrealdb::types::Value = result.take(0).unwrap();
            assert_eq!(rows.into_json_value(), json!([{ "out": "thread:1", "version": 7 }]));
            assert_eq!(row_count_of(&h, id).await, 1);
            assert_eq!(state_of(&h, id).await, "ready");
        }
        assert_eq!(edge_count_of(&h, "empty").await, 0);
        assert_eq!(row_count_of(&h, "empty").await, 0);
        assert_eq!(state_of(&h, "empty").await, "ready");
        assert_eq!(h.node.processor.read().await.graph_count(), 2);
    }
}

#[tokio::test]
async fn ledger_snapshot_recovery_sees_low_version_updates_and_deletions() {
    let store = MemStore::default();
    let h = build(HarnessOpts { circuit_store: Some(Arc::new(store)), ..Default::default() }).await;
    h.raw_db.query("DEFINE TABLE thread SCHEMALESS PERMISSIONS FULL; \
        CREATE thread:low SET title = 'old'; CREATE thread:high SET title = 'high'; \
        CREATE _00_version:low SET record_id = thread:low, version = 1; \
        CREATE _00_version:high SET record_id = thread:high, version = 1000;")
        .await.unwrap().check().unwrap();
    let runtime = ssp_node::Runtime::new(Arc::clone(&h.node));
    runtime.bootstrap().await;
    runtime.checkpoint().await;
    h.raw_db.query("UPDATE thread:low SET title = 'new'; UPDATE _00_version:low SET version = 2; \
        DELETE thread:high; DELETE _00_version:high;").await.unwrap().check().unwrap();
    *h.node.processor.write().await = Circuit::new();
    runtime.bootstrap().await;
    assert_eq!(*h.node.status.read().await, SspStatus::Ready);
    let fresh = Arc::new(RwLock::new(Circuit::new()));
    ssp_node::bootstrap::rebuild_from_db(&MemDb(Arc::clone(&h.raw_db)), &fresh, 200).await.unwrap();
    assert_eq!(h.node.processor.read().await.compute_table_hashes(),
        fresh.read().await.compute_table_hashes());
}
