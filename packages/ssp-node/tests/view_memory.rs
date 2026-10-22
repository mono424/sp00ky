//! What one registered view costs in SSP memory, counted by a global
//! allocator rather than estimated.
//!
//! A Cloudflare SSP Durable Object has a 128 MB isolate and wasm linear memory
//! never shrinks, so the per-view cost decides how many views a project can
//! hold. The threads e2e suite leaves ~400 views registered over ~130 rows.
//! This drives the real `/view/register` route with the threads example schema
//! and the query shapes the threads app registers, then charges each view what
//! removing it from the circuit frees.
//!
//! Measured 2026-10-21 (400 views, 140 rows): 55.4 KB per view natively, most
//! of it the empty 1,024-entry `indexset` node (64 KB) every `LIMIT` reserved;
//! 16.0 KB with the node sized to the window. Under local workerd the same 400
//! views took 21.5 MB of linear memory before and 7.3 MB after.
//!
//! The counting allocator is why this test has a binary of its own: it must
//! not share the process with tests allocating in parallel.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::RwLock;

use ssp::circuit::Circuit;
use ssp_node::api::{ApiRequest, Method};
use ssp_node::ports::{
    CancelWatch, Db, DbError, HttpClient, HttpError, LocalBoxFuture, OutboundRequest,
    OutboundResponse, Scheduler, Spawner, TimerKind,
};
use ssp_node::{Platform, SspNode, SspStatus};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(new_size as isize - layout.size() as isize, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

const SECRET: &str = "mem-secret";

struct MemDb(Arc<Surreal<MemEngine>>);

#[async_trait::async_trait]
impl Db for MemDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        let mut q = self.0.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| DbError::Transport(e.to_string()))?;
        let mut out = Vec::new();
        for i in 0..response.num_statements() {
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

struct NoHttp;
#[async_trait::async_trait]
impl HttpClient for NoHttp {
    async fn send(&self, _req: OutboundRequest, _cancel: Option<CancelWatch>) -> Result<OutboundResponse, HttpError> {
        Ok(OutboundResponse { status: 200, body: String::new() })
    }
}

struct NoTimers;
#[async_trait::async_trait]
impl Scheduler for NoTimers {
    async fn schedule(&self, _kind: TimerKind, _at: u64) {}
    async fn cancel(&self, _kind: &TimerKind) {}
    async fn sleep(&self, _dur: Duration) {}
}

struct TokioSpawner;
impl Spawner for TokioSpawner {
    fn spawn(&self, fut: LocalBoxFuture) {
        tokio::spawn(fut);
    }
}

/// The threads example schema (`example/schema/src/schema.surql`), cut to the
/// synced tables and their select permissions, plus the `@crdt` edit-log and
/// presence tables the CLI generates for `thread.title` / `thread.content`.
const SCHEMA: &str = r#"
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD username ON TABLE user TYPE string;
DEFINE FIELD profile_picture ON TABLE user TYPE option<string>;

DEFINE TABLE thread SCHEMAFULL
  PERMISSIONS FOR select WHERE published = true OR ($access = "account" AND author.id = $auth.id);
DEFINE FIELD title ON TABLE thread TYPE string;
DEFINE FIELD content ON TABLE thread TYPE string;
DEFINE FIELD author ON TABLE thread TYPE record<user>;
DEFINE FIELD created_at ON TABLE thread TYPE datetime VALUE time::now();
DEFINE FIELD active ON TABLE thread TYPE bool VALUE $value OR false;
DEFINE FIELD published ON TABLE thread TYPE bool VALUE $value OR false;

DEFINE TABLE comment SCHEMAFULL
  PERMISSIONS FOR select WHERE thread.published = true OR ($access = "account" AND thread.author.id = $auth.id);
DEFINE FIELD thread ON TABLE comment TYPE record<thread>;
DEFINE FIELD content ON TABLE comment TYPE string;
DEFINE FIELD author ON TABLE comment TYPE record<user>;
DEFINE FIELD created_at ON TABLE comment TYPE datetime VALUE time::now();

DEFINE TABLE thread__crdt_log SCHEMAFULL
  PERMISSIONS FOR select WHERE doc.published = true OR ($access = "account" AND doc.author.id = $auth.id);
DEFINE FIELD doc ON TABLE thread__crdt_log TYPE record<thread>;
DEFINE FIELD field ON TABLE thread__crdt_log TYPE string;
DEFINE FIELD peer ON TABLE thread__crdt_log TYPE string;
DEFINE FIELD size ON TABLE thread__crdt_log TYPE int;
DEFINE INDEX thread__crdt_log_doc ON TABLE thread__crdt_log FIELDS doc, field;

DEFINE TABLE thread__crdt_presence SCHEMAFULL
  PERMISSIONS FOR select WHERE doc.published = true OR ($access = "account" AND doc.author.id = $auth.id);
DEFINE FIELD doc ON TABLE thread__crdt_presence TYPE record<thread>;
DEFINE FIELD field ON TABLE thread__crdt_presence TYPE string;
DEFINE FIELD session ON TABLE thread__crdt_presence TYPE string;
DEFINE INDEX thread__crdt_presence_doc ON TABLE thread__crdt_presence FIELDS doc, field;
"#;

const AUTHOR: &str = "(SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author";

/// What one signed-in threads session registers: the auth user, the home
/// list and its preload, the thread detail page, the sidebar list, and the
/// title / content edit logs plus content presence of the open thread.
fn session_views(user: &str, thread: &str) -> Vec<(String, Value)> {
    let comments = format!(
        "(SELECT *, {AUTHOR} FROM comment WHERE thread=$parent.id ORDER BY created_at desc LIMIT 10) AS comments"
    );
    let auth = json!({ "id": user });
    vec![
        ("SELECT * FROM user WHERE id = $id LIMIT 1".to_string(), json!({ "id": user, "auth": auth })),
        (format!("SELECT *, {AUTHOR} FROM thread ORDER BY title asc LIMIT 10"), json!({ "auth": auth })),
        (format!("SELECT *, {AUTHOR}, {comments} FROM thread ORDER BY title asc LIMIT 10"), json!({ "auth": auth })),
        (format!("SELECT *, {AUTHOR}, {comments} FROM thread WHERE id = $id LIMIT 1"), json!({ "id": thread, "auth": auth })),
        ("SELECT * FROM thread ORDER BY title asc LIMIT 20".to_string(), json!({ "auth": auth })),
        ("SELECT * FROM thread__crdt_log WHERE doc = $doc AND field = $field".to_string(), json!({ "doc": thread, "field": "title", "auth": auth })),
        ("SELECT * FROM thread__crdt_log WHERE doc = $doc AND field = $field".to_string(), json!({ "doc": thread, "field": "content", "auth": auth })),
        ("SELECT * FROM thread__crdt_presence WHERE doc = $doc AND field = $field".to_string(), json!({ "doc": thread, "field": "content", "auth": auth })),
    ]
}

fn authed(method: Method, path: &str, body: Value) -> ApiRequest {
    ApiRequest {
        method,
        path: path.to_string(),
        bearer: Some(SECRET.to_string()),
        body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
    }
}

async fn node() -> (Arc<SspNode>, Arc<Surreal<MemEngine>>) {
    let raw = Surreal::new::<Mem>(()).await.unwrap();
    raw.use_ns("test").use_db("test").await.unwrap();
    raw.query(SCHEMA).await.unwrap().check().unwrap();
    let raw = Arc::new(raw);
    let platform = Platform {
        db: Arc::new(MemDb(Arc::clone(&raw))),
        http: Arc::new(NoHttp),
        scheduler: Arc::new(NoTimers),
        spawner: Arc::new(TokioSpawner),
        telemetry: Arc::new(ssp_node::NoopTelemetry),
        circuit_store: Arc::new(ssp_node::NoopCircuitStore),
    };
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let job_control = ssp_node::jobs::JobControl::new();
    let job_config = Arc::new(ssp_node::jobs::JobConfig::default());
    let job_dispatcher = Arc::new(ssp_node::jobs::JobDispatcher::new(
        Arc::clone(&platform.db),
        Arc::clone(&platform.spawner),
        Arc::clone(&platform.scheduler),
        tx,
        job_control.clone(),
        Arc::clone(&job_config),
        "mem-ssp".to_string(),
        true,
    ));
    let node = SspNode {
        publication_gate: Arc::new(tokio::sync::Mutex::new(())),
        platform,
        status: Arc::new(RwLock::new(SspStatus::Ready)),
        processor: Arc::new(RwLock::new(Circuit::new())),
        job_config,
        job_control,
        job_dispatcher,
        ssp_id: "mem-ssp".to_string(),
        auth_secret: SECRET.to_string(),
        ref_mode: ssp_protocol::RefMode::Dedicated,
        merge_views: false,
        version: "9.9.9-test",
        surrealdb_version: "mem-test".to_string(),
        advertise_ip: None,
        info_env: vec![],
        start_epoch_ms: ssp_node::now_epoch_ms(),
        bootstrap_warnings: Arc::new(RwLock::new(Vec::new())),
        backend_health: None,
        crdt: Arc::new(ssp_node::crdt::CrdtFold::new(ssp_node::crdt::FoldConfig::default(), "mem-ssp")),
        view_metrics: Arc::new(RwLock::new(std::collections::HashMap::new())),
        edge_update_tx: ssp_node::edges::EdgePublisher::default(),
        anonymous_live_queries: false,
        impersonation: false,
        query_allowlist: Arc::new(ssp_node::allowlist_state::QueryAllowlist::new(ssp::allowlist::Mode::Off)),
        standalone: true,
        schedule_engine: None,
        push_engine: None,
        ttl_cleanup_interval_secs: 60,
        schema_poll_secs: 0,
        schema_watch: Default::default(),
        view_metrics_flush_ms: 2000,
        bootstrap_page_size: 200,
        checkpoint_interval_secs: None,
        max_snapshot_age_secs: 3600,
        last_heartbeat_seen: Arc::new(std::sync::Mutex::new(None)),
    };
    node.apply_circuit_policy().await;
    let node = Arc::new(node);
    node.refresh_schema(false).await.unwrap();
    (node, raw)
}

/// About the circuit the threads e2e suite leaves behind: ~130 rows.
async fn seed(node: &SspNode, users: usize, threads: usize, comments_per_thread: usize) {
    let ingest = |table: &str, id: String, record: Value| {
        authed(Method::Post, "/ingest", json!({ "table": table, "op": "CREATE", "id": id, "record": record }))
    };
    let mut reqs = Vec::new();
    for u in 0..users {
        reqs.push(ingest("user", format!("user:u{u}"), json!({ "username": format!("user-{u}"), "_00_rv": 1 })));
    }
    for t in 0..threads {
        let author = format!("user:u{}", t % users);
        reqs.push(ingest("thread", format!("thread:t{t}"), json!({
            "title": format!("Thread number {t}"), "content": "Some body text for the thread, a sentence or two long.",
            "author": author, "published": t % 2 == 0, "active": false, "created_at": "2026-10-21T00:00:00Z", "_00_rv": 1,
        })));
        for c in 0..comments_per_thread {
            reqs.push(ingest("comment", format!("comment:t{t}c{c}"), json!({
                "thread": format!("thread:t{t}"), "content": "A comment.", "author": format!("user:u{}", c % users),
                "created_at": "2026-10-21T00:00:00Z", "_00_rv": 1,
            })));
        }
        for field in ["title", "content"] {
            reqs.push(ingest("thread__crdt_log", format!("thread__crdt_log:t{t}{field}"), json!({
                "doc": format!("thread:t{t}"), "field": field, "peer": "p1", "size": 10, "_00_rv": 1,
            })));
        }
    }
    for r in reqs {
        let resp = node.route(r).await.unwrap();
        assert_eq!(resp.status, 200);
    }
}

/// Register `n` views, a session's worth at a time, each session its own
/// user and thread (round robin over the seeded ones).
async fn register(node: &SspNode, n: usize) {
    let mut registered = 0;
    let mut s = 0;
    while registered < n {
        let (user, thread) = (format!("user:u{}", s % 20), format!("thread:t{}", s % 20));
        for (i, (surql, params)) in session_views(&user, &thread).into_iter().enumerate().take(n - registered) {
            let id = format!("s{s}v{i}");
            let body = json!({
                "id": id, "surql": surql, "params": params, "clientId": format!("client-{s}"),
                "ttl": "30m", "lastActiveAt": "2026-10-21T00:00:00Z",
            });
            let resp = node.route(authed(Method::Post, "/view/register", body)).await.unwrap();
            assert_eq!(resp.status, 200, "register {id}");
            registered += 1;
        }
        s += 1;
    }
}

/// Bytes a value owned: what dropping it frees.
fn freed_by<T>(value: T) -> isize {
    let before = live();
    drop(value);
    before - live()
}

const VIEWS: usize = 400;

/// Average circuit bytes per view over the shapes a threads session
/// registers. 16.0 KB today; 55.4 KB before the TopK node was sized to its
/// window. Ratchet down, never up.
const MAX_BYTES_PER_VIEW: isize = 20 * 1024;

/// No single shape may cost more than this. The heaviest, the home list with
/// its comment preload, is 23.4 KB today and was 86.9 KB.
const MAX_BYTES_PER_SHAPE: isize = 32 * 1024;

#[tokio::test(flavor = "current_thread")]
async fn a_threads_view_costs_kilobytes_not_a_preallocated_node() {
    let (node, _db) = node().await;
    // 20 users, 20 threads, 2 comments and 2 edit-log rows per thread.
    seed(&node, 20, 20, 2).await;

    let start = live();
    register(&node, VIEWS).await;
    // Let the edge publisher drain what the registrations queued.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let grown = live() - start;

    let mut circuit = std::mem::take(&mut *node.processor.write().await);
    assert_eq!(circuit.graph_count(), VIEWS);
    let ids = circuit.view_ids();
    let (mut total, mut heaviest) = (0, 0);
    for shape in 0..session_views("user:u0", "thread:t0").len() {
        let suffix = format!("v{shape}");
        let of_shape: Vec<&String> = ids.iter().filter(|id| id.ends_with(&suffix)).collect();
        let (mut removed, mut view, mut plan) = (0, 0, 0);
        for id in &of_shape {
            let v = circuit.get_view(id).unwrap();
            view += freed_by(v.clone());
            plan += freed_by(v.plan.clone());
            let before = live();
            circuit.remove_query(id);
            removed += before - live();
        }
        let n = of_shape.len() as isize;
        println!(
            "shape {suffix}: {} B per view (its View {} B, of which the plan {} B)",
            removed / n,
            view / n,
            plan / n
        );
        heaviest = heaviest.max(removed / n);
        total += removed;
    }
    let metrics = freed_by(std::mem::take(&mut *node.view_metrics.write().await));
    let per_view = total / VIEWS as isize;
    println!("circuit: {per_view} B per view; view_metrics: {} B per view", metrics / VIEWS as isize);
    println!("everything, embedded DB included: {} B per view", grown / VIEWS as isize);
    assert!(per_view <= MAX_BYTES_PER_VIEW, "a view holds {per_view} B of circuit state");
    assert!(heaviest <= MAX_BYTES_PER_SHAPE, "a shape holds {heaviest} B per view");
}
