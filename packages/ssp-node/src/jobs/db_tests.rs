//! Close-to-e2e tests for the job engine THROUGH THE PORTS: a real embedded
//! SurrealDB behind a `Db` adapter, a real mock HTTP backend behind an
//! `HttpClient` adapter, driving the actual runner SQL. Migrated from the former
//! `packages/job-runner` — same coverage, but every DB write now travels the
//! same `type::record($id)` SQL the production port uses.

use super::dispatcher::JobDispatcher;
use super::runner::*;
use super::types::{BackendInfo, JobConfig, JobControl, JobEntry};
use crate::api::Method;
use crate::ports::{
    CancelWatch, Db, DbError, HttpClient, HttpError, OutboundRequest, OutboundResponse,
    Scheduler, Spawner, TimerKind,
};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;
use tokio::sync::mpsc;
use wiremock::matchers::{method, path as match_path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// --- Port adapters over test infrastructure ---------------------------------

struct MemDb(Arc<Surreal<MemEngine>>);

#[async_trait::async_trait]
impl Db for MemDb {
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, Value)],
    ) -> Result<Vec<Value>, DbError> {
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
        Ok("mem".to_string())
    }
}

struct TestHttp(reqwest::Client);

#[async_trait::async_trait]
impl HttpClient for TestHttp {
    async fn send(
        &self,
        req: OutboundRequest,
        _cancel: Option<CancelWatch>,
    ) -> Result<OutboundResponse, HttpError> {
        let mut builder = match req.method {
            Method::Post => self.0.post(&req.url),
            _ => unimplemented!("job dispatch is POST-only"),
        }
        .timeout(req.timeout);
        if let Some(b) = &req.bearer {
            builder = builder.bearer_auth(b);
        }
        if let Some(j) = &req.json_body {
            builder = builder.json(j);
        }
        match builder.send().await {
            Ok(r) => Ok(OutboundResponse {
                status: r.status().as_u16(),
                body: r.text().await.unwrap_or_default(),
            }),
            Err(e) if e.is_timeout() => Err(HttpError::Timeout(req.timeout)),
            Err(e) => Err(HttpError::Transport(e.to_string())),
        }
    }
}

struct TestScheduler;

#[async_trait::async_trait]
impl Scheduler for TestScheduler {
    async fn schedule(&self, _kind: TimerKind, _at: u64) {}
    async fn cancel(&self, _kind: &TimerKind) {}
    async fn sleep(&self, dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

struct TestSpawner;
impl Spawner for TestSpawner {
    fn spawn(&self, fut: crate::ports::LocalBoxFuture) {
        tokio::spawn(fut);
    }
}

// --- Harness -----------------------------------------------------------------

/// The outbox table as `spky add api` actually generates it
/// (`apps/cli/src/add_api.rs::outbox_template`): SCHEMAFULL, same field set, same
/// nullability, same DEFAULT-vs-VALUE choices.
///
/// These tests used to run against a SCHEMALESS table, which meant no ASSERT was
/// enforced, `errors[*] FLEXIBLE` was untested, and — most importantly — the
/// `created_at` / `updated_at` semantics the recovery sweeps and the delay window
/// depend on were invisible. Keep this in step with the template.
///
/// Permissions are omitted: these queries run as root, which bypasses them.
const OUTBOX_DDL: &str = "\
DEFINE TABLE OVERWRITE job SCHEMAFULL;
DEFINE FIELD OVERWRITE assigned_to ON job TYPE option<record>;
DEFINE FIELD OVERWRITE path ON job TYPE string;
DEFINE FIELD OVERWRITE payload ON job TYPE any;
DEFINE FIELD OVERWRITE retries ON job TYPE int DEFAULT ALWAYS 0;
DEFINE FIELD OVERWRITE max_retries ON job TYPE int DEFAULT ALWAYS 3;
DEFINE FIELD OVERWRITE retry_strategy ON job TYPE string DEFAULT ALWAYS 'linear'
    ASSERT $value IN ['linear', 'exponential'];
DEFINE FIELD OVERWRITE status ON job TYPE string DEFAULT ALWAYS 'pending'
    ASSERT $value IN ['pending', 'processing', 'success', 'failed'];
DEFINE FIELD OVERWRITE errors ON job TYPE array<object> DEFAULT ALWAYS [];
DEFINE FIELD OVERWRITE errors[*] ON job TYPE object FLEXIBLE;
DEFINE FIELD OVERWRITE updated_at ON job TYPE datetime DEFAULT ALWAYS time::now();
DEFINE FIELD OVERWRITE created_at ON job TYPE datetime DEFAULT time::now();
DEFINE FIELD OVERWRITE assignee ON job TYPE option<string>;
DEFINE FIELD OVERWRITE lease_until ON job TYPE option<datetime>;
DEFINE FIELD OVERWRITE lease_epoch ON job TYPE option<int>;
DEFINE FIELD OVERWRITE result ON job TYPE any;
DEFINE FIELD OVERWRITE timeout ON job TYPE option<int>;
DEFINE FIELD OVERWRITE delay ON job TYPE option<int>;";

async fn mem_db() -> (Arc<dyn Db>, Arc<Surreal<MemEngine>>) {
    let db = Surreal::new::<Mem>(()).await.expect("start mem db");
    db.use_ns("test").use_db("test").await.expect("use ns/db");
    let raw = Arc::new(db);
    raw.query(OUTBOX_DDL).await.expect("apply outbox schema");
    (Arc::new(MemDb(Arc::clone(&raw))), raw)
}

/// Insert a job row. `id_part` is the id after `job:`.
async fn insert_job(db: &Surreal<MemEngine>, id_part: &str, status: &str) {
    db.query(
        "CREATE type::record('job', $id) SET \
         status = $status, retries = 3, max_retries = 3, path = '/run', payload = {}, \
         retry_strategy = 'linear', errors = [], \
         created_at = time::now(), updated_at = time::now()",
    )
    .bind(("id", id_part.to_string()))
    .bind(("status", status.to_string()))
    .await
    .expect("insert job");
}

async fn select_string(db: &Surreal<MemEngine>, sql: &str) -> Option<String> {
    db.query(sql).await.expect("query").take(0).expect("take")
}
async fn select_bool(db: &Surreal<MemEngine>, sql: &str) -> Option<bool> {
    db.query(sql).await.expect("query").take(0).expect("take")
}
async fn select_i64(db: &Surreal<MemEngine>, sql: &str) -> Option<i64> {
    db.query(sql).await.expect("query").take(0).expect("take")
}
/// `SELECT VALUE count() ... GROUP ALL` comes back as a bare int on some plans
/// and as `{ count: n }` on others — defining an index is enough to flip it.
async fn count_rows(db: &Surreal<MemEngine>, sql: &str) -> i64 {
    let v: surrealdb::types::Value = db.query(sql).await.expect("query").take(0).expect("take");
    match v.into_json_value() {
        Value::Number(n) => n.as_i64().unwrap_or(0),
        Value::Array(rows) => rows
            .first()
            .and_then(|r| r.get("count").or(Some(r)))
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        Value::Object(map) => map.get("count").and_then(|v| v.as_i64()).unwrap_or(0),
        _ => 0,
    }
}

/// A dispatcher over the same ports, with `job` configured to reach `base_url`.
/// `concurrency` is what the `_00_job_policy` row would say.
fn make_dispatcher(db: Arc<dyn Db>, base_url: String) -> (Arc<JobDispatcher>, mpsc::Receiver<JobEntry>) {
    let (tx, rx) = mpsc::channel::<JobEntry>(64);
    let mut job_tables = std::collections::HashMap::new();
    job_tables.insert(
        "job".to_string(),
        BackendInfo {
            name: "api".to_string(),
            base_url,
            auth_token: None,
            timeout: Some(5),
            timeout_overridable: false,
        },
    );
    let dispatcher = Arc::new(JobDispatcher::new(
        db,
        Arc::new(TestSpawner),
        Arc::new(TestScheduler),
        tx,
        JobControl::new(),
        Arc::new(JobConfig { job_tables }),
        "ssp-test".to_string(),
        true,
    ));
    (dispatcher, rx)
}

fn make_runner(db: Arc<dyn Db>) -> JobRunner {
    let (dispatcher, rx) = make_dispatcher(Arc::clone(&db), "http://unused".to_string());
    JobRunner::new(
        rx,
        db,
        Arc::new(TestHttp(reqwest::Client::new())),
        Arc::new(TestScheduler),
        Arc::new(TestSpawner),
        dispatcher,
    )
}

fn job_entry(id: &str, base_url: String, max_retries: u32) -> JobEntry {
    JobEntry {
        table: crate::jobs::table_of(id).unwrap_or_default().to_string(),
        id: id.to_string(),
        base_url,
        path: "/run".to_string(),
        payload: Value::Null,
        retries: 0,
        max_retries,
        retry_strategy: "linear".to_string(),
        auth_token: None,
        timeout: Duration::from_secs(5),
        // Pre-claim value. `execute_job` takes the claim CAS and overwrites it with
        // the epoch it mints, so a test never has to set this itself.
        lease_epoch: None,
        // Driving `execute_job` directly, so no slot was taken.
        permit: None,
    }
}

// --- helper: fail_if_pending through the port ---------------------------

#[tokio::test]
async fn fail_if_pending_updates_only_pending_rows() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "k1", "pending").await;
    insert_job(&raw, "k2", "processing").await;

    let killed = fail_if_pending_helper(port.as_ref(), "job:k1", json!({"code": "killed"}))
        .await
        .unwrap();
    assert!(killed, "pending row was terminalized");
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:k1").await.as_deref(),
        Some("failed")
    );

    let killed = fail_if_pending_helper(port.as_ref(), "job:k2", json!({"code": "killed"}))
        .await
        .unwrap();
    assert!(!killed, "processing row must not be clobbered");
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:k2").await.as_deref(),
        Some("processing")
    );
}

// --- result capture -----------------------------------------------------

#[tokio::test]
async fn success_captures_the_backend_response_as_json() {
    let backend = MockServer::start().await;
    Mock::given(method("POST"))
        .and(match_path("/run"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"fileId": "f_8123"})))
        .mount(&backend)
        .await;

    let (port, raw) = mem_db().await;
    insert_job(&raw, "res1", "pending").await;
    let runner = make_runner(Arc::clone(&port));

    runner
        .execute_job(job_entry("job:res1", backend.uri(), 3))
        .await
        .unwrap();

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:res1").await.as_deref(),
        Some("success")
    );
    assert_eq!(
        select_string(&raw, "SELECT VALUE result.fileId FROM ONLY job:res1").await.as_deref(),
        Some("f_8123"),
        "a JSON body is stored parsed, so a dependent workflow step can address its fields"
    );
}

#[tokio::test]
async fn success_stores_a_non_json_body_as_a_string() {
    let backend = MockServer::start().await;
    Mock::given(method("POST"))
        .and(match_path("/run"))
        .respond_with(ResponseTemplate::new(200).set_body_string("done"))
        .mount(&backend)
        .await;

    let (port, raw) = mem_db().await;
    insert_job(&raw, "res2", "pending").await;
    let runner = make_runner(Arc::clone(&port));

    runner
        .execute_job(job_entry("job:res2", backend.uri(), 3))
        .await
        .unwrap();

    assert_eq!(
        select_string(&raw, "SELECT VALUE result FROM ONLY job:res2").await.as_deref(),
        Some("done")
    );
}

#[tokio::test]
async fn oversized_results_are_replaced_by_a_marker() {
    let backend = MockServer::start().await;
    Mock::given(method("POST"))
        .and(match_path("/run"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("x".repeat(JOB_RESULT_MAX_BYTES + 1)),
        )
        .mount(&backend)
        .await;

    let (port, raw) = mem_db().await;
    insert_job(&raw, "res3", "pending").await;
    let runner = make_runner(Arc::clone(&port));

    runner
        .execute_job(job_entry("job:res3", backend.uri(), 3))
        .await
        .unwrap();

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:res3").await.as_deref(),
        Some("success"),
        "an oversized body must not stop the job from completing"
    );
    assert_eq!(
        select_bool(&raw, "SELECT VALUE result.truncated FROM ONLY job:res3").await,
        Some(true),
        "the operator can see WHY the output is missing"
    );
}

#[tokio::test]
async fn success_still_terminalizes_when_the_table_has_no_result_field() {
    // A project that upgraded the stack without re-applying its schema has a
    // SCHEMAFULL outbox table with no `result` field. SurrealDB rejects the
    // whole UPDATE — completing the job matters far more than storing output.
    let backend = MockServer::start().await;
    Mock::given(method("POST"))
        .and(match_path("/run"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&backend)
        .await;

    let (port, raw) = mem_db().await;
    raw.query(
        "REMOVE TABLE job; \
         DEFINE TABLE job SCHEMAFULL; \
         DEFINE FIELD status ON job TYPE string; \
         DEFINE FIELD path ON job TYPE option<string>; \
         DEFINE FIELD payload ON job TYPE option<object> FLEXIBLE; \
         DEFINE FIELD retries ON job TYPE option<int>; \
         DEFINE FIELD max_retries ON job TYPE option<int>; \
         DEFINE FIELD retry_strategy ON job TYPE option<string>; \
         DEFINE FIELD errors ON job TYPE option<array>; \
         DEFINE FIELD created_at ON job TYPE option<datetime>; \
         DEFINE FIELD updated_at ON job TYPE option<datetime>;",
    )
    .await
    .expect("define schemafull job table without a result field");
    insert_job(&raw, "res4", "pending").await;
    let runner = make_runner(Arc::clone(&port));

    runner
        .execute_job(job_entry("job:res4", backend.uri(), 3))
        .await
        .expect("job completion must not fail on a missing result field");

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:res4").await.as_deref(),
        Some("success"),
        "fell back to a status-only write"
    );
}

// --- the shared due-clause selects due rows and skips future ones -------

#[tokio::test]
async fn pending_due_clause_selects_due_and_skips_delayed() {
    let (_, raw) = mem_db().await;
    // Ready: no delay.
    raw.query("CREATE job:ready SET status='pending', path='/run', created_at = time::now() - 1s, delay = 0")
        .await
        .unwrap();
    // Ready: its delay window has elapsed.
    raw.query("CREATE job:elapsed SET status='pending', path='/run', created_at = time::now() - 10s, delay = 1000")
        .await
        .unwrap();
    // Still inside its delay window.
    raw.query("CREATE job:delayed SET status='pending', path='/run', created_at = time::now(), delay = 3600000")
        .await
        .unwrap();
    // No delay field at all — the `?? 0` fallback makes it ready.
    raw.query("CREATE job:nodelay SET status='pending', path='/run', created_at = time::now() - 1s")
        .await
        .unwrap();

    let sql = format!(
        "SELECT VALUE type::string(id) FROM job WHERE status = 'pending' AND {} ORDER BY id",
        PENDING_DUE_CLAUSE
    );
    let all: Vec<String> =
        raw.query("SELECT VALUE type::string(id) FROM job").await.unwrap().take(0).unwrap();
    assert_eq!(all.len(), 4, "fixture rows must actually insert; got {all:?}");

    let ids: Vec<String> = raw.query(&sql).await.unwrap().take(0).unwrap();

    assert!(ids.contains(&"job:ready".to_string()));
    assert!(ids.contains(&"job:elapsed".to_string()));
    assert!(ids.contains(&"job:nodelay".to_string()), "an unset delay means ready now");
    assert!(!ids.contains(&"job:delayed".to_string()), "a job inside its delay window waits");
}

// --- timestamp semantics the sweeps and the delay window depend on ----------

/// `created_at` must not move when the row is updated.
///
/// The shipped template defined it `VALUE time::now()`, which SurrealDB recomputes
/// on EVERY update — so `created_at` silently meant "last modified". Two things
/// break: the due clause below (`created_at + delay`), whose window slides forward
/// on any write to a pending row, and `spky jobs`' age column.
///
/// The write used here is exactly the assignee stamp, which is the one write the
/// platform performs on a job that has not run yet.
#[tokio::test]
async fn stamping_an_assignee_does_not_slide_a_delayed_jobs_due_time() {
    let (port, raw) = mem_db().await;
    // Due in an hour.
    raw.query("CREATE job:d SET status='pending', path='/run', delay = 3600000")
        .await
        .expect("insert delayed job");
    let created_before = select_string(&raw, "SELECT VALUE <string>created_at FROM ONLY job:d")
        .await
        .expect("created_at set");

    // The SSP claims the job (`UPDATE ... SET assignee = ...`).
    port.query(
        "UPDATE type::record($id) SET assignee = $assignee RETURN NONE",
        &[("id", json!("job:d")), ("assignee", json!("ssp-0"))],
    )
    .await
    .expect("stamp assignee");

    let created_after = select_string(&raw, "SELECT VALUE <string>created_at FROM ONLY job:d")
        .await
        .expect("created_at still set");
    assert_eq!(
        created_before, created_after,
        "claiming a job must not restart its delay window"
    );
}

/// The assignee stamp must NOT bump `updated_at` either — but for the opposite
/// reason. `updated_at` is the recovery sweeps' staleness clock, so resetting it on
/// every claim would stop a wedged job from ever looking stale enough to recover.
/// This is why the field is `DEFAULT ALWAYS` (write-once) rather than `VALUE`.
#[tokio::test]
async fn stamping_an_assignee_does_not_reset_the_recovery_staleness_clock() {
    let (port, raw) = mem_db().await;
    raw.query("CREATE job:s SET status='pending', path='/run', updated_at = time::now() - 5m")
        .await
        .expect("insert stale job");

    port.query(
        "UPDATE type::record($id) SET assignee = $assignee RETURN NONE",
        &[("id", json!("job:s")), ("assignee", json!("ssp-0"))],
    )
    .await
    .expect("stamp assignee");

    let still_stale = select_bool(
        &raw,
        "SELECT VALUE updated_at < time::now() - 30s FROM ONLY job:s",
    )
    .await;
    assert_eq!(
        still_stale,
        Some(true),
        "the staleness clock must survive a claim, or recovery never sees a wedged job"
    );
}

/// The runner appends `{ code, reason }` objects to `errors`. On a SCHEMAFULL
/// table that is only legal because `errors[*]` is FLEXIBLE — without it the append
/// is rejected, and since the append is best-effort the job still completes with a
/// silently empty error history.
#[tokio::test]
async fn error_entries_append_on_a_schemafull_table() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "e1", "pending").await;

    port.query(
        "UPDATE type::record($id) SET errors = array::append(errors, $error), \
         updated_at = time::now()",
        &[
            ("id", json!("job:e1")),
            ("error", json!({ "code": 500, "reason": "boom" })),
        ],
    )
    .await
    .expect("append error");

    assert_eq!(select_i64(&raw, "SELECT VALUE array::len(errors) FROM ONLY job:e1").await, Some(1));
    assert_eq!(
        select_string(&raw, "SELECT VALUE errors[0].reason FROM ONLY job:e1").await.as_deref(),
        Some("boom"),
        "a rejected append would leave the history empty without failing the job"
    );
}

/// Every status the runner writes has to satisfy the table's ASSERT. A rejected
/// status write leaves the job in its previous state with no error surfaced to the
/// caller, which for `processing` means the row looks pending forever.
#[tokio::test]
async fn every_status_the_runner_writes_is_accepted_by_the_table() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "st", "pending").await;

    for status in ["processing", "success", "failed", "pending"] {
        port.query(
            "UPDATE type::record($id) SET status = $status, updated_at = time::now()",
            &[("id", json!("job:st")), ("status", json!(status))],
        )
        .await
        .unwrap_or_else(|e| panic!("status '{status}' rejected by the outbox table: {e}"));
        assert_eq!(
            select_string(&raw, "SELECT VALUE status FROM ONLY job:st").await.as_deref(),
            Some(status),
            "status '{status}' did not stick"
        );
    }
}

/// A job whose retry budget is exhausted lands `failed` and keeps its whole error
/// history — the row is terminal, so `spky jobs retry` is the only way back.
#[tokio::test]
async fn a_job_that_exhausts_its_retries_lands_failed_with_its_error_history() {
    let backend = MockServer::start().await;
    Mock::given(method("POST"))
        .and(match_path("/run"))
        .respond_with(ResponseTemplate::new(500).set_body_string("nope"))
        .mount(&backend)
        .await;

    let (port, raw) = mem_db().await;
    // Already used its budget, so this attempt is the last one.
    raw.query(
        "CREATE job:r SET status='pending', path='/run', payload={}, retries=3, \
         max_retries=3, retry_strategy='linear', errors=[]",
    )
    .await
    .expect("insert job");
    let runner = make_runner(Arc::clone(&port));
    // The retry budget lives on the in-memory entry (built at enqueue), not on the
    // row — so the entry has to be at its limit for this to be the last attempt.
    let mut entry = job_entry("job:r", backend.uri(), 3);
    entry.retries = 3;

    runner.execute_job(entry).await.expect("execute");

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:r").await.as_deref(),
        Some("failed")
    );
    assert!(
        select_i64(&raw, "SELECT VALUE array::len(errors) FROM ONLY job:r").await.unwrap_or(0) >= 1,
        "the failure reason must be recorded on the row"
    );
}

// --- admission control + drain ------------------------------------------------

/// `_00_job_policy` as `apps/cli/src/schedule_tables.surql` defines it, plus the
/// dispatch index `build_outbox_platform_fields` injects. The index matters:
/// adding one changes the plan for `SELECT VALUE count() ... GROUP ALL`, and
/// with it the shape the result comes back in.
const DISPATCH_DDL: &str = "\
DEFINE TABLE OVERWRITE _00_job_policy SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE concurrency ON TABLE _00_job_policy TYPE int DEFAULT 1
    ASSERT $value > 0;
DEFINE FIELD OVERWRITE updated_at ON TABLE _00_job_policy TYPE datetime VALUE time::now();
DEFINE INDEX IF NOT EXISTS idx_job_dispatch ON job COLUMNS status, created_at;";

struct DispatchHarness {
    dispatcher: Arc<JobDispatcher>,
    rx: mpsc::Receiver<JobEntry>,
    raw: Arc<Surreal<MemEngine>>,
}

impl DispatchHarness {
    async fn new(standalone: bool, base_url: String) -> Self {
        let (port, raw) = mem_db().await;
        raw.query(DISPATCH_DDL).await.expect("apply dispatch schema");
        let (tx, rx) = mpsc::channel::<JobEntry>(64);
        let mut job_tables = std::collections::HashMap::new();
        job_tables.insert(
            "job".to_string(),
            BackendInfo {
                name: "api".to_string(),
                base_url,
                auth_token: None,
                timeout: Some(5),
                timeout_overridable: false,
            },
        );
        let dispatcher = Arc::new(JobDispatcher::new(
            port,
            Arc::new(TestSpawner),
            Arc::new(TestScheduler),
            tx,
            JobControl::new(),
            Arc::new(JobConfig { job_tables }),
            "ssp-test".to_string(),
            standalone,
        ));
        Self { dispatcher, rx, raw }
    }

    /// A pending row created `age_ms` ago, so a test can fix the drain order
    /// without sleeping.
    async fn pending(&self, id_part: &str, age_ms: i64, path: &str) {
        self.raw
            .query(
                "CREATE type::record('job', $id) SET \
                 status = 'pending', retries = 0, max_retries = 1, path = $path, payload = {}, \
                 retry_strategy = 'linear', errors = [], \
                 created_at = time::now() - <duration>(string::concat(<string>$age, 'ms')), \
                 updated_at = time::now()",
            )
            .bind(("id", id_part.to_string()))
            .bind(("age", age_ms))
            .bind(("path", path.to_string()))
            .await
            .expect("insert pending job");
    }

    /// Ids currently sitting on the queue, in the order they were admitted.
    fn admitted(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(entry) = self.rx.try_recv() {
            out.push(entry.id.clone());
            // Hold the entry (and so its permit) for the length of the test:
            // dropping it here would release the slot and defeat the point.
            std::mem::forget(entry);
        }
        out
    }
}

/// The cap is the point: above it, rows stay `pending` rather than piling up in
/// memory. And the order is `created_at`, so the oldest waiting row goes first.
#[tokio::test]
async fn the_drain_admits_the_oldest_rows_up_to_the_limit() {
    let mut h = DispatchHarness::new(true, "http://unused".to_string()).await;
    h.dispatcher.set_limit("job", 2);
    // Deliberately inserted newest-first, so passing cannot be an accident of
    // insertion order.
    for (id, age) in [("k1", 100), ("k2", 200), ("k3", 300), ("k4", 400), ("k5", 500)] {
        h.pending(id, age, "/run").await;
    }

    h.dispatcher.note_backlog("job");
    h.dispatcher.drain("job").await;

    assert_eq!(
        h.admitted(),
        vec!["job:k5".to_string(), "job:k4".to_string()],
        "only the two oldest rows may be admitted at concurrency 2"
    );
    assert_eq!(
        count_rows(&h.raw, "SELECT VALUE count() FROM job WHERE status = 'pending' GROUP ALL").await,
        5,
        "the other three stay pending — the outbox is the queue"
    );
}

/// A limit that cannot be read must never become a stop: no policy row means
/// the serial behavior that predates this feature, not zero.
#[tokio::test]
async fn a_missing_policy_row_means_one_not_zero() {
    let mut h = DispatchHarness::new(true, "http://unused".to_string()).await;
    h.pending("k1", 100, "/run").await;
    h.pending("k2", 200, "/run").await;

    h.dispatcher.note_backlog("job");
    h.dispatcher.drain("job").await;

    assert_eq!(h.admitted().len(), 1, "the default is one, and it is enforced");
}

/// The deployed policy is what governs, not the default.
#[tokio::test]
async fn the_limit_comes_from_the_policy_row() {
    let mut h = DispatchHarness::new(true, "http://unused".to_string()).await;
    h.raw
        .query("UPSERT _00_job_policy:job MERGE { concurrency: 3 }")
        .await
        .expect("write policy");
    for (id, age) in [("k1", 100), ("k2", 200), ("k3", 300), ("k4", 400)] {
        h.pending(id, age, "/run").await;
    }

    h.dispatcher.note_backlog("job");
    h.dispatcher.drain("job").await;

    assert_eq!(h.admitted().len(), 3);
}

/// A row left `processing` by a crashed SSP would otherwise hold the cluster
/// budget until the recovery sweep resets it — ten minutes of a table that
/// admits nothing.
///
/// This row carries NO `lease_until`, which is what a table deployed before the lease
/// fields existed looks like. So it also pins the un-migrated fallback: the count still
/// ages such a row out on `updated_at` at exactly the window it always used.
#[tokio::test]
async fn the_cluster_count_ignores_stale_processing_rows() {
    let mut h = DispatchHarness::new(false, "http://unused".to_string()).await;
    h.raw
        .query(
            "CREATE job:orphan SET status = 'processing', retries = 0, max_retries = 1, \
             path = '/run', payload = {}, retry_strategy = 'linear', errors = [], \
             created_at = time::now() - 700s, updated_at = time::now() - 700s",
        )
        .await
        .expect("insert orphan");
    h.pending("k1", 100, "/run").await;

    h.dispatcher.note_backlog("job");
    h.dispatcher.drain("job").await;

    assert_eq!(
        h.admitted(),
        vec!["job:k1".to_string()],
        "an abandoned 'processing' row must not spend the budget"
    );
}

/// The same count, when the row is fresh, is exactly what bounds the cluster.
#[tokio::test]
async fn a_live_processing_row_elsewhere_spends_the_cluster_budget() {
    let mut h = DispatchHarness::new(false, "http://unused".to_string()).await;
    h.raw
        .query(
            "CREATE job:elsewhere SET status = 'processing', retries = 0, max_retries = 1, \
             path = '/run', payload = {}, retry_strategy = 'linear', errors = [], \
             created_at = time::now(), updated_at = time::now()",
        )
        .await
        .expect("insert in-flight row");
    h.pending("k1", 100, "/run").await;

    h.dispatcher.note_backlog("job");
    h.dispatcher.drain("job").await;

    assert!(
        h.admitted().is_empty(),
        "another node is already using the table's single slot"
    );
}

/// Cloudflare and the portable shell construct a queue and drop the receiver —
/// they have no runner. Without the latch, every ingest on those hosts would
/// kick a drain query for work that could never run.
#[tokio::test]
async fn a_closed_queue_latches_dispatch_off() {
    let mut h = DispatchHarness::new(true, "http://unused".to_string()).await;
    h.dispatcher.set_limit("job", 4);
    h.pending("k1", 100, "/run").await;
    drop(std::mem::replace(&mut h.rx, mpsc::channel::<JobEntry>(1).1));

    h.dispatcher.note_backlog("job");
    h.dispatcher.drain("job").await;

    assert!(
        h.dispatcher.backlogged_tables().is_empty(),
        "a host with no runner must stop looking for work"
    );
    // And the marker stays off for every later attempt.
    h.dispatcher.note_backlog("job");
    assert!(h.dispatcher.backlogged_tables().is_empty());
}

/// The permit is released when execution ends, NOT when the `enqueued` mark is
/// cleared. Those differ across retry backoff — the mark is deliberately held,
/// and holding the slot with it would idle a `concurrency: 1` table through
/// every backoff, which the serial runner this replaces never did.
#[tokio::test]
async fn a_retry_backoff_does_not_hold_the_slot() {
    let backend = MockServer::start().await;
    Mock::given(method("POST"))
        .and(match_path("/fail"))
        .respond_with(ResponseTemplate::new(500).set_body_string("nope"))
        .mount(&backend)
        .await;
    Mock::given(method("POST"))
        .and(match_path("/ok"))
        .respond_with(ResponseTemplate::new(200).set_body_string("done"))
        .mount(&backend)
        .await;

    let (port, raw) = mem_db().await;
    raw.query(DISPATCH_DDL).await.expect("apply dispatch schema");
    let (tx, rx) = mpsc::channel::<JobEntry>(64);
    let mut job_tables = std::collections::HashMap::new();
    job_tables.insert(
        "job".to_string(),
        BackendInfo {
            name: "api".to_string(),
            base_url: backend.uri(),
            auth_token: None,
            timeout: Some(5),
            timeout_overridable: false,
        },
    );
    let dispatcher = Arc::new(JobDispatcher::new(
        Arc::clone(&port),
        Arc::new(TestSpawner),
        Arc::new(TestScheduler),
        tx,
        JobControl::new(),
        Arc::new(JobConfig { job_tables }),
        "ssp-test".to_string(),
        true,
    ));
    // One slot, so the second job can only run if the first has given it up.
    dispatcher.set_limit("job", 1);

    let runner = JobRunner::new(
        rx,
        port,
        Arc::new(TestHttp(reqwest::Client::new())),
        Arc::new(TestScheduler),
        Arc::new(TestSpawner),
        Arc::clone(&dispatcher),
    );
    tokio::spawn(runner.run());

    // `retriable` fails immediately, then sleeps 400ms (linear backoff, attempt
    // 1) before its second attempt. `follower` is younger, so FIFO puts it
    // second — it can only get in during that sleep.
    raw.query(
        "CREATE job:retriable SET status = 'pending', retries = 0, max_retries = 5, \
         path = '/fail', payload = {}, retry_strategy = 'linear', errors = [], \
         created_at = time::now() - 500ms, updated_at = time::now();
         CREATE job:follower SET status = 'pending', retries = 0, max_retries = 1, \
         path = '/ok', payload = {}, retry_strategy = 'linear', errors = [], \
         created_at = time::now() - 100ms, updated_at = time::now()",
    )
    .await
    .expect("insert jobs");

    dispatcher.note_backlog("job");
    dispatcher.drain("job").await;

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:follower").await.as_deref(),
        Some("success"),
        "the follower must run while the first job sits in its 400ms backoff"
    );
    // Still mid-backoff, so the follower really did overlap it rather than
    // running after it finished. The row reads `processing` for the whole
    // backoff — it is only reset to `pending` immediately before the re-admit.
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:retriable").await.as_deref(),
        Some("processing"),
        "the first job must still be in its backoff, not finished"
    );
}

// --- the lease: claim, fence, expiry -----------------------------------------
//
// A `processing` row used to be reclaimable only if its OWNER looked dead, which is a
// different question from whether the work is progressing — and one that answers "yes,
// it's fine" for an SSP that restarted under the same id, for a pool entry that is
// merely present rather than healthy, and for a request hung inside the HTTP call on
// this very node. These pin the replacement: the lease decides, ownership does not.

/// The budget follows the LEASE, not the row's age. A row that is young but whose
/// lease has already run out (a short job timeout) must stop spending the budget, or
/// admission control and recovery disagree about which rows are alive — the state
/// where a row is invisible to both.
#[tokio::test]
async fn the_cluster_count_follows_the_lease_not_the_row_age() {
    let mut h = DispatchHarness::new(false, "http://unused".to_string()).await;
    h.raw
        .query(
            "CREATE job:shortlease SET status = 'processing', retries = 0, max_retries = 1, \
             path = '/run', payload = {}, retry_strategy = 'linear', errors = [], \
             created_at = time::now(), updated_at = time::now(), \
             lease_until = time::now() - 1s, lease_epoch = 1",
        )
        .await
        .expect("insert expired-lease row");
    h.pending("k1", 100, "/run").await;

    h.dispatcher.note_backlog("job");
    h.dispatcher.drain("job").await;

    assert_eq!(
        h.admitted(),
        vec!["job:k1".to_string()],
        "an expired lease frees the slot even though the row was touched a moment ago"
    );
}

/// And the converse: a row far older than the legacy 600s window still holds the
/// budget while its lease is live. A long job is not an abandoned one.
#[tokio::test]
async fn a_long_lease_keeps_the_budget_even_on_an_old_row() {
    let mut h = DispatchHarness::new(false, "http://unused".to_string()).await;
    h.raw
        .query(
            "CREATE job:longlease SET status = 'processing', retries = 0, max_retries = 1, \
             path = '/run', payload = {}, retry_strategy = 'linear', errors = [], \
             created_at = time::now() - 2h, updated_at = time::now() - 2h, \
             lease_until = time::now() + 1h, lease_epoch = 1",
        )
        .await
        .expect("insert long-lease row");
    h.pending("k1", 100, "/run").await;

    h.dispatcher.note_backlog("job");
    h.dispatcher.drain("job").await;

    assert!(
        h.admitted().is_empty(),
        "a live lease means the work is still running, whatever the row's age says"
    );
}

/// Claiming is a compare-and-swap on `pending`. Before this, the transition to
/// `processing` was an unguarded `SET`, so two nodes could both "start" one row.
#[tokio::test]
async fn only_one_claim_of_a_pending_row_can_win() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "c1", "pending").await;

    let first = claim_processing(port.as_ref(), "job:c1", "ssp-a", 60).await.expect("claim");
    let second = claim_processing(port.as_ref(), "job:c1", "ssp-b", 60).await.expect("claim");

    assert!(matches!(first, Claim::Fenced(_)), "the first claim takes the row: {first:?}");
    assert_eq!(second, Claim::Lost, "the second must not also start the job");
    assert_eq!(
        select_string(&raw, "SELECT VALUE assignee FROM ONLY job:c1").await.as_deref(),
        Some("ssp-a"),
        "and the winner owns it",
    );
}

/// A claim mints a lease that outlives the request it covers, and a fresh token.
#[tokio::test]
async fn a_claim_mints_a_lease_and_bumps_the_epoch() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "c2", "pending").await;

    let claim = claim_processing(port.as_ref(), "job:c2", "ssp-a", 90).await.expect("claim");
    assert_eq!(claim, Claim::Fenced(1), "the first claim opens the count at 1");
    assert_eq!(
        select_i64(&raw, "SELECT VALUE lease_epoch FROM ONLY job:c2").await,
        Some(1)
    );
    assert_eq!(
        select_bool(&raw, "SELECT VALUE lease_until > time::now() + 80s FROM ONLY job:c2").await,
        Some(true),
        "the lease covers the request plus its grace"
    );

    // Back to pending (a retry) and claimed again: a new attempt, a new token. The
    // lease covers ONE attempt, which is why no renewal is needed for a job that
    // burns several.
    h_reset_pending(&raw, "job:c2").await;
    let again = claim_processing(port.as_ref(), "job:c2", "ssp-a", 90).await.expect("claim");
    assert_eq!(again, Claim::Fenced(2), "each attempt gets its own token");
}

/// The lease is derived from the job's own timeout, and clamped. An unbounded
/// `timeout` on a row must not mint a lease so long the row becomes unreclaimable —
/// that is the original bug arriving through a different door.
#[test]
fn a_lease_always_outlives_its_request_and_is_never_unbounded() {
    assert!(
        lease_secs(Duration::from_secs(10)) > 10,
        "a lease shorter than its request would reclaim healthy work"
    );
    assert!(lease_secs(Duration::from_secs(300)) > 300);
    let absurd = lease_secs(Duration::from_secs(u32::MAX as u64));
    assert!(absurd <= 24 * 60 * 60, "a lease must stay finite, got {absurd}");
}

/// The whole point of the fencing token: a reclaimed job's ORIGINAL attempt must not
/// be able to report an outcome over the top of the attempt that replaced it.
#[tokio::test]
async fn a_reclaimed_jobs_original_attempt_cannot_write_its_outcome() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "f1", "pending").await;

    // Attempt one claims the row and is now "running".
    let Claim::Fenced(first_epoch) =
        claim_processing(port.as_ref(), "job:f1", "ssp-a", 60).await.expect("claim")
    else {
        panic!("expected a fenced claim")
    };

    // Its lease runs out and the sweep hands the row back to the queue.
    raw.query("UPDATE job:f1 SET lease_until = time::now() - 1s")
        .await
        .expect("expire the lease");
    assert!(
        reclaim_expired_lease(port.as_ref(), "job:f1").await.expect("reclaim"),
        "an expired lease is reclaimable"
    );

    // Attempt two picks it up.
    let Claim::Fenced(second_epoch) =
        claim_processing(port.as_ref(), "job:f1", "ssp-b", 60).await.expect("claim")
    else {
        panic!("expected a fenced claim")
    };
    assert!(second_epoch > first_epoch, "the token moved on");

    // Now attempt one finally answers. It must not land.
    complete_success_helper(port.as_ref(), "job:f1", "{\"ok\":true}", Some(first_epoch))
        .await
        .expect("a fenced write is not an error");

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:f1").await.as_deref(),
        Some("processing"),
        "the zombie's success must not terminalize the row attempt two is running"
    );

    // Attempt two's write, on the current token, does land.
    complete_success_helper(port.as_ref(), "job:f1", "{\"ok\":true}", Some(second_epoch))
        .await
        .expect("the live attempt writes");
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:f1").await.as_deref(),
        Some("success"),
    );
}

/// Reclaim keys off the lease and NOTHING else. This row has a live, named assignee —
/// the exact shape that was unreclaimable at any age.
#[tokio::test]
async fn an_expired_lease_is_reclaimed_even_with_a_live_assignee() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "r1", "pending").await;
    claim_processing(port.as_ref(), "job:r1", "ssp-1", 60).await.expect("claim");
    raw.query("UPDATE job:r1 SET lease_until = time::now() - 1s")
        .await
        .expect("expire the lease");

    assert!(reclaim_expired_lease(port.as_ref(), "job:r1").await.expect("reclaim"));

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:r1").await.as_deref(),
        Some("pending"),
    );
    // Clearing the owner is load-bearing, not tidying: `JobDispatcher::claim` only
    // takes rows with `assignee = NONE OR assignee = $me`, so a reclaimed row that
    // kept a dead node's id would be un-drainable by every other node.
    assert_eq!(
        select_string(&raw, "SELECT VALUE assignee FROM ONLY job:r1").await,
        None,
        "the reclaim must release ownership or nobody else can pick the row up"
    );
}

/// A live lease is left alone, whoever owns it and however long the row has existed.
#[tokio::test]
async fn a_live_lease_is_never_reclaimed() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "r2", "pending").await;
    claim_processing(port.as_ref(), "job:r2", "ssp-1", 3600).await.expect("claim");
    raw.query("UPDATE job:r2 SET created_at = time::now() - 3d")
        .await
        .expect("age the row");

    assert!(
        !reclaim_expired_lease(port.as_ref(), "job:r2").await.expect("reclaim"),
        "work still inside its lease must not be taken away from it"
    );
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:r2").await.as_deref(),
        Some("processing"),
    );
}

/// The reclaim re-checks the expiry in its own `WHERE`, so a row that finished between
/// a sweep's SELECT and its write is left alone. The singlenode reset used to be
/// unguarded and could flip a `success` row back to `pending`.
#[tokio::test]
async fn a_job_that_finished_under_the_sweep_is_not_dragged_back_to_pending() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "r3", "pending").await;
    let claim = claim_processing(port.as_ref(), "job:r3", "ssp-1", 60).await.expect("claim");
    raw.query("UPDATE job:r3 SET lease_until = time::now() - 1s")
        .await
        .expect("expire the lease");
    // It completes just before the sweep gets to it.
    complete_success_helper(port.as_ref(), "job:r3", "{}", claim.epoch()).await.expect("success");

    assert!(
        !reclaim_expired_lease(port.as_ref(), "job:r3").await.expect("reclaim"),
        "only a `processing` row is reclaimable"
    );
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:r3").await.as_deref(),
        Some("success"),
    );
}

/// Back to `pending` the way a retry does, so a test can claim the same row twice.
async fn h_reset_pending(db: &Surreal<MemEngine>, id: &str) {
    db.query(format!("UPDATE {id} SET status = 'pending'")).await.expect("reset");
}

/// End to end, against a real SurrealDB and a real socket: a backend that accepts the
/// connection and then never answers.
///
/// This is the failure the lease exists for, and every layer of it is genuine here —
/// the runner claims the row, the request really hangs, the sweep's own reclaim
/// statement hands the row back, a second attempt really runs it, and the first
/// attempt's eventual write is really fenced out. The only simulated part is the
/// passage of time: there is no controllable clock in this codebase, so the lease is
/// expired by writing `lease_until`, the same way the schedule tests age a run.
#[tokio::test]
async fn a_hung_request_is_reclaimed_re_run_and_cannot_report_its_own_outcome() {
    // A listener that accepts and then holds the connection open, saying nothing.
    // wiremock cannot express this: it always answers eventually.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let hung_addr = listener.local_addr().expect("addr");
    let accepted = Arc::new(tokio::sync::Notify::new());
    let accepted_tx = Arc::clone(&accepted);
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = listener.accept().await {
            accepted_tx.notify_waiters();
            held.push(sock); // never written to, never dropped
        }
    });

    let (port, raw) = mem_db().await;
    insert_job(&raw, "hung", "pending").await;

    // Attempt one: a short request timeout so the test does not sit for minutes, but
    // long enough that the request is genuinely still in flight while we reclaim.
    let runner = make_runner(Arc::clone(&port));
    let ctx = Arc::clone(runner.ctx());
    let mut entry = job_entry("job:hung", format!("http://{hung_addr}"), 1);
    entry.timeout = Duration::from_secs(4);
    let first = tokio::spawn(async move { ctx.execute_job(entry).await });

    // Wait until the request is actually out on the wire.
    tokio::time::timeout(Duration::from_secs(5), accepted.notified())
        .await
        .expect("the backend received the request");

    // The row is claimed, leased, and fenced.
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:hung").await.as_deref(),
        Some("processing"),
    );
    let first_epoch = select_i64(&raw, "SELECT VALUE lease_epoch FROM ONLY job:hung")
        .await
        .expect("a lease epoch");
    assert_eq!(
        select_bool(&raw, "SELECT VALUE lease_until > time::now() FROM ONLY job:hung").await,
        Some(true),
        "the lease is live while the request is in flight, so nothing reclaims it yet"
    );
    assert!(
        !reclaim_expired_lease(port.as_ref(), "job:hung").await.expect("reclaim"),
        "a live lease is left alone even though the backend is hung"
    );

    // The lease runs out. This is the sweep's own write.
    raw.query("UPDATE job:hung SET lease_until = time::now() - 1s").await.expect("expire");
    assert!(
        reclaim_expired_lease(port.as_ref(), "job:hung").await.expect("reclaim"),
        "past its lease the row comes back, with the request still hanging"
    );
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:hung").await.as_deref(),
        Some("pending"),
    );

    // Attempt two, against a backend that answers.
    let good = MockServer::start().await;
    Mock::given(method("POST"))
        .and(match_path("/run"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"attempt": 2})))
        .mount(&good)
        .await;
    let runner2 = make_runner(Arc::clone(&port));
    runner2
        .execute_job(job_entry("job:hung", good.uri(), 1))
        .await
        .expect("the second attempt runs");

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:hung").await.as_deref(),
        Some("success"),
        "the re-run completed the work the hung attempt never could"
    );
    let second_epoch = select_i64(&raw, "SELECT VALUE lease_epoch FROM ONLY job:hung").await;
    assert_eq!(second_epoch, Some(first_epoch + 2), "reclaim bumped it, then the re-claim did");

    // Now attempt one finally gives up (its request times out) and tries to record a
    // failure. It must not touch the row the re-run already finished.
    first.await.expect("the first attempt's task").expect("it returns Ok, having been fenced");

    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:hung").await.as_deref(),
        Some("success"),
        "the zombie must not overwrite the outcome of the attempt that replaced it"
    );
    assert_eq!(
        select_i64(&raw, "SELECT VALUE array::len(errors) FROM ONLY job:hung").await,
        Some(0),
        "nor append its error to the successful run's history"
    );
}

// --- live pickup vs. the creating transaction --------------------------------
//
// The outbox mutation event runs INSIDE the transaction that creates the job row,
// so the `/ingest` that drives live pickup reaches the SSP before that row is
// committed. These tests hold a real transaction open across the claim to
// reproduce it: the embedded engine behaves like the server here, answering a
// concurrent UPDATE with "no such row" instead of blocking on the writer.

/// A `Scheduler` that counts its sleeps, so a test can tell "gave up at once" from
/// "waited".
#[derive(Default)]
struct CountingScheduler(std::sync::atomic::AtomicUsize);

impl CountingScheduler {
    fn sleeps(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Scheduler for CountingScheduler {
    async fn schedule(&self, _kind: TimerKind, _at: u64) {}
    async fn cancel(&self, _kind: &TimerKind) {}
    async fn sleep(&self, dur: Duration) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(dur).await;
    }
}

/// Create `job:<id_part>` as `pending` in a transaction held open for `hold`, then
/// commit it (or cancel it, when `commit` is false).
fn create_job_in_open_transaction(
    raw: &Arc<Surreal<MemEngine>>,
    id_part: &str,
    hold: Duration,
    commit: bool,
) -> tokio::task::JoinHandle<()> {
    let sql = format!(
        "BEGIN TRANSACTION; \
         CREATE type::record('job', $id) SET status = 'pending', path = '/run', payload = {{}}, \
         retries = 0, max_retries = 1, retry_strategy = 'linear', errors = []; \
         SLEEP {}ms; \
         {} TRANSACTION;",
        hold.as_millis(),
        if commit { "COMMIT" } else { "CANCEL" },
    );
    let raw = Arc::clone(raw);
    let id = id_part.to_string();
    tokio::spawn(async move {
        let response = raw.query(sql).bind(("id", id)).await.expect("creating transaction");
        if commit {
            response.check().expect("creating transaction committed");
        }
    })
}

/// The bug behind "outbox jobs only run when the recovery sweep finds them": the
/// claim reached a row whose creating transaction had not committed yet, read
/// "matched nothing" as "claimed elsewhere", and dropped the job.
#[tokio::test]
async fn a_claim_waits_for_the_creating_transaction_to_commit() {
    let (port, raw) = mem_db().await;
    let creator = create_job_in_open_transaction(&raw, "t1", Duration::from_millis(400), true);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // What the runner used to do: one CAS, while the row is still invisible.
    let plain = claim_processing(port.as_ref(), "job:t1", "ssp-a", 60).await.expect("claim");
    assert_eq!(plain, Claim::Lost, "an uncommitted row cannot be claimed directly");

    let scheduler = CountingScheduler::default();
    let claim = claim_processing_when_visible(
        port.as_ref(),
        &scheduler,
        "job:t1",
        "ssp-a",
        60,
        Duration::from_secs(5),
    )
    .await
    .expect("claim");
    creator.await.expect("creator task");

    assert_eq!(claim, Claim::Fenced(1), "the claim lands once the row commits");
    assert!(scheduler.sleeps() > 0, "and it got there by waiting, not by luck");
    assert_eq!(
        select_string(&raw, "SELECT VALUE status FROM ONLY job:t1").await.as_deref(),
        Some("processing")
    );
    assert_eq!(
        select_string(&raw, "SELECT VALUE assignee FROM ONLY job:t1").await.as_deref(),
        Some("ssp-a")
    );
}

/// Waiting is only for an absent row. One that exists and is not `pending` belongs
/// to another attempt (or is finished) and must be given up on immediately.
#[tokio::test]
async fn a_claim_never_waits_on_a_row_that_is_not_pending() {
    let (port, raw) = mem_db().await;
    insert_job(&raw, "t2", "processing").await;
    insert_job(&raw, "t3", "success").await;

    let scheduler = CountingScheduler::default();
    for id in ["job:t2", "job:t3"] {
        let claim = claim_processing_when_visible(
            port.as_ref(),
            &scheduler,
            id,
            "ssp-a",
            60,
            Duration::from_secs(5),
        )
        .await
        .expect("claim");
        assert_eq!(claim, Claim::Lost, "{id} is not ours to run");
    }
    assert_eq!(scheduler.sleeps(), 0, "a real loss must not cost a backoff");
}

/// A rolled-back creation never produces a row. The claim gives up after its
/// budget instead of holding the table's slot forever.
#[tokio::test]
async fn a_claim_gives_up_on_a_row_that_never_commits() {
    let (port, raw) = mem_db().await;
    let creator = create_job_in_open_transaction(&raw, "t4", Duration::from_millis(100), false);

    let scheduler = CountingScheduler::default();
    let started = std::time::Instant::now();
    let claim = claim_processing_when_visible(
        port.as_ref(),
        &scheduler,
        "job:t4",
        "ssp-a",
        60,
        Duration::from_millis(300),
    )
    .await
    .expect("claim");
    creator.await.expect("creator task");

    assert_eq!(claim, Claim::Lost);
    assert!(scheduler.sleeps() > 0, "it did wait for the row");
    assert!(started.elapsed() < Duration::from_secs(3), "but only for its budget");
    assert_eq!(
        count_rows(&raw, "SELECT VALUE count() FROM job WHERE id = job:t4 GROUP ALL").await,
        0,
        "and waiting never creates the row"
    );
}

/// End to end, the way live pickup reaches the runner in cluster mode: the ingest
/// admits the job while its row is still uncommitted. It must run exactly once,
/// right after the commit, not be dropped for the recovery sweep.
#[tokio::test]
async fn a_job_admitted_before_its_row_commits_still_runs_once() {
    let backend = MockServer::start().await;
    Mock::given(method("POST"))
        .and(match_path("/run"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"ok\":true}"))
        .expect(1)
        .mount(&backend)
        .await;

    let DispatchHarness { dispatcher, rx, raw } = DispatchHarness::new(false, backend.uri()).await;
    let port: Arc<dyn Db> = Arc::new(MemDb(Arc::clone(&raw)));
    let runner = JobRunner::new(
        rx,
        port,
        Arc::new(TestHttp(reqwest::Client::new())),
        Arc::new(TestScheduler),
        Arc::new(TestSpawner),
        Arc::clone(&dispatcher),
    );
    tokio::spawn(runner.run());

    let creator = create_job_in_open_transaction(&raw, "t5", Duration::from_millis(300), true);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // `SspNode::admit_or_backlog`, reached from the job's own CREATE ingest.
    assert!(dispatcher.try_admit(job_entry("job:t5", backend.uri(), 1)).await);
    creator.await.expect("creator task");

    let mut status = None;
    for _ in 0..100 {
        status = select_string(&raw, "SELECT VALUE status FROM ONLY job:t5").await;
        if status.as_deref() == Some("success") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(status.as_deref(), Some("success"), "the live path ran the job");
    assert_eq!(
        select_string(&raw, "SELECT VALUE assignee FROM ONLY job:t5").await.as_deref(),
        Some("ssp-test"),
        "owned by the node that ran it, so the cluster sweep leaves it alone"
    );
    // `expect(1)` on the mock is verified when `backend` drops.
}
