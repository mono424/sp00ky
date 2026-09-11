//! Close-to-e2e engine tests: a real embedded SurrealDB, the REAL table DDL
//! (`include_str!`'d from `apps/cli/src/schedule_tables.surql`, so a DDL change
//! that breaks the engine's SQL fails here rather than on a deploy), and a stub
//! outbox table standing in for the app's job table.
//!
//! There is no runner in these tests. Jobs are "executed" by writing the terminal
//! status a runner would have written, which is exactly the boundary the engine
//! observes across — so these tests cover the engine's half of the contract
//! without simulating HTTP.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{Duration, Utc};
use serde_json::{json, Value};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;

use crate::db::{ScheduleDb, ScheduleDbError};
use crate::engine::{stamp, EngineConfig, ScheduleEngine};
use crate::ids;
use crate::kill::{JobKill, NoopJobKill};

// --- adapters ---------------------------------------------------------------

struct MemDb(Arc<Surreal<MemEngine>>);

#[async_trait::async_trait]
impl ScheduleDb for MemDb {
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, Value)],
    ) -> Result<Vec<Value>, ScheduleDbError> {
        let mut q = self.0.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| ScheduleDbError::Transport(e.to_string()))?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value =
                response.take(i).map_err(|e| ScheduleDbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
}

/// Records which jobs were asked to die, so `replace` and kill paths are
/// observable without a runner.
#[derive(Default)]
struct RecordingKill {
    killed: Mutex<Vec<String>>,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl JobKill for RecordingKill {
    async fn kill(&self, job_id: &str) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.killed.lock().unwrap().push(job_id.to_string());
        Ok(())
    }
}

// --- harness ----------------------------------------------------------------

/// The real shipped DDL, so a schema change that breaks the engine's SQL fails
/// in this file rather than on someone's deploy.
const SCHEDULE_TABLES: &str = include_str!("../../../apps/cli/src/schedule_tables.surql");

/// Stand-in for an app's outbox table, kept FAITHFUL to what `spky add api`
/// generates (`apps/cli/src/add_api.rs::outbox_template`) — SCHEMAFULL, the same
/// field set, the same nullability, the same DEFAULT-vs-VALUE choices.
///
/// Fidelity here is the whole point. This stub used to be a loose approximation
/// (no `assigned_to`, no ASSERTs, `created_at DEFAULT` instead of the shipped
/// `VALUE`), and every difference was a bug the engine tests could not see:
/// a required `assigned_to` rejected every scheduled spawn, and a schedule with
/// `timeout:` was rejected for naming a field the real table did not define.
/// If you change the template, change this too.
///
/// Permissions are omitted deliberately: these queries run as root, which
/// bypasses them, so carrying them would only add noise.
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

struct Harness {
    engine: ScheduleEngine,
    raw: Arc<Surreal<MemEngine>>,
    kill: Arc<RecordingKill>,
}

async fn harness() -> Harness {
    let db = Surreal::new::<Mem>(()).await.expect("start mem db");
    db.use_ns("test").use_db("test").await.expect("use ns/db");
    let raw = Arc::new(db);

    raw.query(SCHEDULE_TABLES).await.expect("apply scheduling schema");
    raw.query(OUTBOX_DDL).await.expect("apply outbox schema");

    let kill = Arc::new(RecordingKill::default());
    let engine = ScheduleEngine::new(
        Arc::new(MemDb(Arc::clone(&raw))),
        Arc::clone(&kill) as Arc<dyn JobKill>,
        EngineConfig::default(),
    );
    Harness { engine, raw, kill }
}

impl Harness {
    /// Write a schedule row the way `spky deploy` would (spec fields only).
    async fn define_schedule(&self, name: &str, spec: Value) {
        let mut content = spec.as_object().cloned().expect("spec object");
        content.insert("name".into(), json!(name));
        let id = ids::schedule(name);
        self.raw
            .query("CREATE type::record($tb, $key) CONTENT $content")
            .bind(("tb", id.table))
            .bind(("key", id.key))
            .bind(("content", Value::Object(content)))
            .await
            .expect("define schedule");
    }

    /// Runs a statement AND surfaces statement-level errors. `query().await` only
    /// reports transport failures — a rejected write hides in the response until
    /// something takes it, which is exactly how a silently-dropped `errors`
    /// append went unnoticed.
    async fn set(&self, sql: &str) {
        let mut response = self.raw.query(sql).await.expect("statement");
        if let Some(errors) = Some(response.take_errors()).filter(|e| !e.is_empty()) {
            panic!("statement failed: {errors:?}");
        }
    }

    async fn strings(&self, sql: &str) -> Vec<String> {
        self.raw.query(sql).await.expect("query").take(0).expect("take")
    }

    async fn one_string(&self, sql: &str) -> Option<String> {
        self.raw.query(sql).await.expect("query").take(0).expect("take")
    }

    /// Datetime columns come back as datetimes, not strings — compare them via
    /// `type::string(...)` or a boolean predicate rather than `one_string`.
    async fn one_bool(&self, sql: &str) -> Option<bool> {
        self.raw.query(sql).await.expect("query").take(0).expect("take")
    }

    /// Counts go through the same tolerant reader the engine uses: a
    /// `count() ... GROUP ALL` is a bare int on some query plans and a
    /// `{ count: n }` object on others, and which one you get depends on the
    /// indexes that happen to exist. Taking it straight into `i64` made these
    /// tests fail for a reason unrelated to what they assert.
    async fn count(&self, sql: &str) -> i64 {
        let mut response = self.raw.query(sql).await.expect("query");
        let value: surrealdb::types::Value = response.take(0).expect("take");
        crate::db::count_value(vec![value.into_json_value()])
    }

    /// Stand in for the JobRunner finishing a job.
    async fn finish_job(&self, job_id: &str, status: &str, result: Value) {
        let mut response = self
            .raw
            .query("UPDATE type::record($id) SET status = $status, result = $result")
            .bind(("id", job_id.to_string()))
            .bind(("status", status.to_string()))
            .bind(("result", result))
            .await
            .expect("finish job");
        let errors = response.take_errors();
        assert!(errors.is_empty(), "finish_job rejected: {errors:?}");
    }

    async fn fail_job(&self, job_id: &str, reason: &str) {
        let mut response = self
            .raw
            .query(
                "UPDATE type::record($id) SET status = 'failed', \
                 errors = array::append(errors, { code: 500, reason: $reason })",
            )
            .bind(("id", job_id.to_string()))
            .bind(("reason", reason.to_string()))
            .await
            .expect("fail job");
        let errors = response.take_errors();
        assert!(errors.is_empty(), "fail_job rejected: {errors:?}");
    }

    async fn job_ids(&self) -> Vec<String> {
        self.strings("SELECT VALUE type::string(id) FROM job").await
    }

    async fn step_status(&self, step: &str) -> Option<String> {
        self.raw
            .query("SELECT VALUE status FROM ONLY _00_step_run WHERE step = $step LIMIT 1")
            .bind(("step", step.to_string()))
            .await
            .expect("query")
            .take(0)
            .expect("take")
    }

    async fn step_job(&self, step: &str) -> Option<String> {
        self.raw
            .query("SELECT VALUE job_id FROM ONLY _00_step_run WHERE step = $step LIMIT 1")
            .bind(("step", step.to_string()))
            .await
            .expect("query")
            .take(0)
            .expect("take")
    }

    async fn workflow_status(&self) -> Option<String> {
        self.one_string("SELECT VALUE status FROM ONLY _00_workflow_run WHERE true LIMIT 1").await
    }

    /// Status of ONE run by id. `workflow_status` reads whichever row comes
    /// first, which is ambiguous as soon as a rerun puts a second run in the
    /// table.
    async fn run_status(&self, run: &ids::Ref) -> Option<String> {
        self.raw
            .query("SELECT VALUE status FROM ONLY type::record($tb, $key)")
            .bind(("tb", run.table.clone()))
            .bind(("key", run.key.clone()))
            .await
            .expect("query")
            .take(0)
            .expect("take")
    }

    async fn run_field(&self, run: &ids::Ref, field: &str) -> Option<String> {
        self.raw
            .query(format!("SELECT VALUE type::string({field}) FROM ONLY type::record($tb, $key)"))
            .bind(("tb", run.table.clone()))
            .bind(("key", run.key.clone()))
            .await
            .expect("query")
            .take(0)
            .expect("take")
    }

    /// Status of one step of one run, by name.
    async fn step_status_of(&self, run: &ids::Ref, step: &str) -> Option<String> {
        self.raw
            .query(
                "SELECT VALUE status FROM ONLY _00_step_run \
                 WHERE workflow_run = type::record($tb, $key) AND step = $step LIMIT 1",
            )
            .bind(("tb", run.table.clone()))
            .bind(("key", run.key.clone()))
            .bind(("step", step.to_string()))
            .await
            .expect("query")
            .take(0)
            .expect("take")
    }

    async fn step_job_of(&self, run: &ids::Ref, step: &str) -> Option<String> {
        self.raw
            .query(
                "SELECT VALUE job_id FROM ONLY _00_step_run \
                 WHERE workflow_run = type::record($tb, $key) AND step = $step LIMIT 1",
            )
            .bind(("tb", run.table.clone()))
            .bind(("key", run.key.clone()))
            .bind(("step", step.to_string()))
            .await
            .expect("query")
            .take(0)
            .expect("take")
    }

    /// Age every workflow run by `interval`, to stand in for the passage of time.
    ///
    /// `_00_workflow_run.created_at` is `READONLY` in the shipped DDL, which is
    /// exactly the property that makes it the right anchor for a deadline — it
    /// cannot be moved by anything the engine or an operator does. So the field is
    /// lifted for one statement and put back, rather than the production DDL being
    /// loosened to suit a test.
    async fn age_workflow_runs(&self, interval: &str) {
        self.set("DEFINE FIELD OVERWRITE created_at ON TABLE _00_workflow_run TYPE datetime").await;
        self.set(&format!("UPDATE _00_workflow_run SET created_at = time::now() - {interval}"))
            .await;
        self.set(
            "DEFINE FIELD OVERWRITE created_at ON TABLE _00_workflow_run \
             TYPE datetime DEFAULT time::now() READONLY",
        )
        .await;
    }

    /// The `error` object on a run row, as JSON. Read as an object rather than a
    /// string because the tests assert on its fields — the whole point of the
    /// error being structured is that `code` is machine-readable.
    async fn run_error(&self, run: &ids::Ref) -> Option<Value> {
        let mut response = self
            .raw
            .query("SELECT VALUE error FROM ONLY type::record($tb, $key)")
            .bind(("tb", run.table.clone()))
            .bind(("key", run.key.clone()))
            .await
            .expect("query");
        let value: surrealdb::types::Value = response.take(0).expect("take");
        Some(value.into_json_value()).filter(|v| v.is_object())
    }

    /// The `error` object on one step of one run.
    async fn step_error(&self, run: &ids::Ref, step: &str) -> Option<Value> {
        let mut response = self
            .raw
            .query(
                "SELECT VALUE error FROM ONLY _00_step_run \
                 WHERE workflow_run = type::record($tb, $key) AND step = $step LIMIT 1",
            )
            .bind(("tb", run.table.clone()))
            .bind(("key", run.key.clone()))
            .bind(("step", step.to_string()))
            .await
            .expect("query");
        let value: surrealdb::types::Value = response.take(0).expect("take");
        Some(value.into_json_value()).filter(|v| v.is_object())
    }

    /// A second engine over the same database — a replicated scheduler, or a
    /// heal pass racing an ingest event.
    fn rival(&self) -> ScheduleEngine {
        ScheduleEngine::new(
            Arc::new(MemDb(Arc::clone(&self.raw))),
            Arc::new(NoopJobKill),
            EngineConfig::default(),
        )
    }
}

fn every_5m_job() -> Value {
    json!({
        "kind": "job",
        "every_ms": 300_000,
        "target_table": "job",
        "path": "/sync",
    })
}

// --- planning + firing ------------------------------------------------------

#[tokio::test]
async fn plans_then_fires_once_due() {
    let h = harness().await;
    h.define_schedule("nightly", every_5m_job()).await;

    // A freshly deployed schedule has next_fire_at = NONE. Planning happens
    // before firing, but an `every` schedule's first fire is one interval out —
    // so the first sweep plans and does not fire.
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.planned, 1);
    assert_eq!(report.fired, 0);
    assert!(h.job_ids().await.is_empty());

    h.set("UPDATE _00_schedule:nightly SET next_fire_at = time::now() - 1s").await;
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.fired, 1);
    assert_eq!(report.spawned, 1);

    assert_eq!(h.job_ids().await.len(), 1, "one atomic job spawned");
    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY job WHERE true LIMIT 1").await.as_deref(),
        Some("pending"),
        "the engine creates a pending row and lets the runner take it from there"
    );
    assert_eq!(
        h.one_bool("SELECT VALUE last_fire_at != NONE FROM ONLY _00_schedule:nightly").await,
        Some(true),
        "the fire is stamped on the schedule"
    );
}

#[tokio::test]
async fn a_bad_cron_is_recorded_and_stays_inert() {
    let h = harness().await;
    h.define_schedule(
        "broken",
        json!({ "kind": "job", "cron": "not a cron", "target_table": "job", "path": "/x" }),
    )
    .await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.errored, 1);
    assert_eq!(report.planned, 0);
    let error = h.one_string("SELECT VALUE last_error FROM ONLY _00_schedule:broken").await;
    assert!(error.unwrap().contains("invalid cron"), "the operator can see why");
    assert_eq!(
        h.one_bool("SELECT VALUE next_fire_at = NONE FROM ONLY _00_schedule:broken").await,
        Some(true),
        "an unplannable schedule never fires"
    );

    // One bad schedule must not stop a good one in the same sweep.
    h.define_schedule("fine", every_5m_job()).await;
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.planned, 1);
    assert_eq!(report.errored, 1);
}

#[tokio::test]
async fn paused_and_disabled_schedules_never_fire() {
    let h = harness().await;
    h.define_schedule("paused-one", every_5m_job()).await;
    h.define_schedule("disabled-one", every_5m_job()).await;
    h.engine.tick_pass().await.unwrap();
    h.set(
        "UPDATE _00_schedule:`paused-one` SET paused = true, next_fire_at = time::now() - 1s; \
         UPDATE _00_schedule:`disabled-one` SET config_disabled = true, \
         next_fire_at = time::now() - 1s",
    )
    .await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.fired, 0);
    assert!(h.job_ids().await.is_empty());

    // Pausing also wins over a queued trigger: the big red button stays red.
    h.set("UPDATE _00_schedule:`paused-one` SET trigger_requested_at = time::now()").await;
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.fired, 0);
    assert!(h.job_ids().await.is_empty());
}

#[tokio::test]
async fn trigger_fires_now_without_moving_the_cron_clock() {
    let h = harness().await;
    h.define_schedule(
        "nightly",
        json!({ "kind": "job", "cron": "0 3 * * *", "target_table": "job", "path": "/cleanup" }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    let planned = h
        .one_string("SELECT VALUE type::string(next_fire_at) FROM ONLY _00_schedule:nightly")
        .await;

    h.set("UPDATE _00_schedule:nightly SET trigger_requested_at = time::now()").await;
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.fired, 1);
    assert_eq!(report.spawned, 1);
    assert_eq!(
        h.one_string("SELECT VALUE type::string(next_fire_at) FROM ONLY _00_schedule:nightly")
            .await,
        planned,
        "a manual run must not shift the schedule"
    );
    assert_eq!(
        h.one_string("SELECT VALUE trigger FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("manual"),
        "history distinguishes a manual run from a cron fire"
    );
    assert_eq!(
        h.one_bool("SELECT VALUE trigger_requested_at = NONE FROM ONLY _00_schedule:nightly")
            .await,
        Some(true),
        "the trigger is consumed, not left to fire again"
    );
}

#[tokio::test]
async fn a_second_ticker_cannot_double_fire() {
    let h = harness().await;
    h.define_schedule("nightly", every_5m_job()).await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:nightly SET next_fire_at = time::now() - 1s").await;

    let rival = h.rival();
    let (a, b) = tokio::join!(h.engine.tick_pass(), rival.tick_pass());
    let fired = a.unwrap().fired + b.unwrap().fired;

    assert_eq!(fired, 1, "the claim CAS lets exactly one ticker win the fire");
    assert_eq!(h.job_ids().await.len(), 1, "and only one job exists");
}

#[tokio::test]
async fn downtime_coalesces_into_a_single_catch_up_fire() {
    let h = harness().await;
    h.define_schedule(
        "hourly",
        json!({ "kind": "job", "cron": "0 * * * *", "target_table": "job", "path": "/tick" }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    // Pretend the process slept for a week.
    h.set("UPDATE _00_schedule:hourly SET next_fire_at = time::now() - 7d").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.fired, 1, "a week of missed slots is one catch-up fire, not 168");
    assert_eq!(h.job_ids().await.len(), 1);
    assert_eq!(
        h.count(
            "SELECT VALUE count() FROM _00_schedule WHERE next_fire_at > time::now() GROUP ALL"
        )
        .await,
        1,
        "the clock is rearmed ahead of now, so the next sweep is quiet"
    );
}

// --- fan-out + concurrency --------------------------------------------------

async fn fan_out_harness(policy: &str) -> Harness {
    let h = harness().await;
    h.set(
        "CREATE connection:alice SET active = true; \
         CREATE connection:bob SET active = true; \
         CREATE connection:carol SET active = false",
    )
    .await;
    h.define_schedule(
        "game-sync",
        json!({
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/syncGames",
            "for_each": "SELECT id FROM connection WHERE active = true",
            "for_each_key": "id",
            "concurrency": policy,
        }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:`game-sync` SET next_fire_at = time::now() - 1s").await;
    h
}

#[tokio::test]
async fn fan_out_spawns_one_job_per_row_with_the_row_in_the_payload() {
    let h = fan_out_harness("skip").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.fired, 1);
    assert_eq!(report.spawned, 2, "only the two active connections");

    assert_eq!(h.job_ids().await.len(), 2);
    let payload_ids = h.strings("SELECT VALUE payload.row.id FROM job").await;
    assert_eq!(payload_ids.len(), 2);
    assert!(payload_ids.contains(&"connection:alice".to_string()));
    assert!(payload_ids.contains(&"connection:bob".to_string()));

    let keys = h.strings("SELECT VALUE key FROM _00_schedule_run").await;
    assert!(keys.contains(&"connection:alice".to_string()), "runs are keyed per item");
}

#[tokio::test]
async fn skip_suppresses_only_the_key_that_is_still_running() {
    let h = fan_out_harness("skip").await;
    h.engine.tick_pass().await.unwrap();

    // Finish alice's job; leave bob's running.
    let alice_job = h
        .one_string(
            "SELECT VALUE job_id FROM ONLY _00_schedule_run \
             WHERE key = 'connection:alice' LIMIT 1",
        )
        .await
        .unwrap();
    h.finish_job(&alice_job, "success", json!({"synced": 3})).await;
    h.engine.tick_pass().await.unwrap();

    h.set("UPDATE _00_schedule:`game-sync` SET next_fire_at = time::now() - 1s").await;
    let report = h.engine.tick_pass().await.unwrap();

    assert_eq!(report.spawned, 1, "alice is free and runs again");
    assert_eq!(report.skipped, 1, "bob's tick is suppressed, not queued");
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run WHERE status = 'skipped' GROUP ALL")
            .await,
        1,
        "the suppressed tick is recorded so an operator can see it"
    );

    // And recorded with its cause. A `skipped` row on its own cannot be told apart
    // from a healthy suppression and a run wedged in `running` for hours, which is
    // the whole reason someone opens this table.
    let code = h
        .one_string(
            "SELECT VALUE error.code FROM ONLY _00_schedule_run \
             WHERE status = 'skipped' LIMIT 1",
        )
        .await;
    assert_eq!(code.as_deref(), Some("concurrency_skip"));
    let blocked_by = h
        .one_string(
            "SELECT VALUE error.blocked_by FROM ONLY _00_schedule_run \
             WHERE status = 'skipped' LIMIT 1",
        )
        .await;
    let bobs_run = h
        .one_string(
            "SELECT VALUE type::string(id) FROM ONLY _00_schedule_run \
             WHERE key = 'connection:bob' AND status = 'running' LIMIT 1",
        )
        .await;
    assert!(bobs_run.is_some(), "bob's run is what holds the key");
    assert_eq!(
        blocked_by, bobs_run,
        "blocked_by names the run actually holding bob's key, not just any run"
    );
}

#[tokio::test]
async fn allow_lets_runs_overlap() {
    let h = fan_out_harness("allow").await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:`game-sync` SET next_fire_at = time::now() - 1s").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.spawned, 2);
    assert_eq!(report.skipped, 0);
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run WHERE status = 'running' GROUP ALL")
            .await,
        4,
        "two overlapping runs per key"
    );
}

#[tokio::test]
async fn replace_kills_the_previous_run_for_that_key() {
    let h = fan_out_harness("replace").await;
    h.engine.tick_pass().await.unwrap();
    let first_jobs = h.job_ids().await;
    h.set("UPDATE _00_schedule:`game-sync` SET next_fire_at = time::now() - 1s").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.replaced, 2);
    assert_eq!(report.spawned, 2);

    let killed = h.kill.killed.lock().unwrap().clone();
    assert_eq!(killed.len(), 2, "both in-flight jobs were killed");
    for job in &first_jobs {
        assert!(killed.contains(job), "{job} should have been killed");
    }
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run WHERE status = 'replaced' GROUP ALL")
            .await,
        2
    );
}

#[tokio::test]
async fn a_failing_for_each_query_is_recorded_not_fatal() {
    let h = harness().await;
    h.define_schedule(
        "broken-fanout",
        json!({
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/x",
            "for_each": "SELECT id FROM",
        }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:`broken-fanout` SET next_fire_at = time::now() - 1s").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.errored, 1);
    assert!(h.job_ids().await.is_empty());
    assert!(h
        .one_string("SELECT VALUE last_error FROM ONLY _00_schedule:`broken-fanout`")
        .await
        .unwrap()
        .contains("forEach"));
}

// --- healing ----------------------------------------------------------------

/// Fire a plain `every` job schedule once and return its job id.
async fn fire_once(h: &Harness) -> String {
    h.define_schedule("nightly", every_5m_job()).await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:nightly SET next_fire_at = time::now() - 1s").await;
    h.engine.tick_pass().await.unwrap();
    h.one_string("SELECT VALUE job_id FROM ONLY _00_schedule_run WHERE true LIMIT 1")
        .await
        .expect("a job was spawned")
}

#[tokio::test]
async fn heal_finalizes_a_run_whose_job_finished_without_an_event() {
    let h = harness().await;
    let job = fire_once(&h).await;
    // The runner finished the job, but the ingest event never reached the engine.
    h.finish_job(&job, "success", json!({"ok": true})).await;

    let report = h.engine.tick_pass().await.unwrap();
    assert!(report.healed >= 1);
    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("success"),
        "polling caps the delay of a lost event at one sweep"
    );
}

#[tokio::test]
async fn heal_fails_a_run_whose_job_row_vanished() {
    let h = harness().await;
    fire_once(&h).await;
    h.set("DELETE job").await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("failed"),
        "a run whose job can never complete must not stay `running` forever"
    );
}

#[tokio::test]
async fn a_failed_job_carries_its_error_onto_the_run() {
    let h = harness().await;
    let job = fire_once(&h).await;
    h.fail_job(&job, "backend exploded").await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("failed")
    );
    assert_eq!(
        h.one_string("SELECT VALUE error.reason FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("backend exploded")
    );
}

// --- retention --------------------------------------------------------------

#[tokio::test]
async fn prune_drops_old_history_and_spares_live_runs() {
    let h = harness().await;
    let old = stamp(Utc::now() - Duration::days(60));
    h.raw
        .query(
            "CREATE _00_schedule_run:old CONTENT { schedule_name: 'x', key: '', \
             fire_at: <datetime>$old, kind: 'job', status: 'success', \
             finished_at: <datetime>$old }; \
             CREATE _00_schedule_run:recent CONTENT { schedule_name: 'x', key: '', \
             fire_at: time::now(), kind: 'job', status: 'success', finished_at: time::now() }; \
             CREATE _00_schedule_run:live CONTENT { schedule_name: 'x', key: '', \
             fire_at: <datetime>$old, kind: 'job', status: 'running' }",
        )
        .bind(("old", old))
        .await
        .expect("seed history");

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.pruned, 1);

    let ids = h.strings("SELECT VALUE type::string(id) FROM _00_schedule_run").await;
    assert!(!ids.contains(&"_00_schedule_run:old".to_string()), "aged-out history is gone");
    assert!(ids.contains(&"_00_schedule_run:recent".to_string()));
    assert!(
        ids.contains(&"_00_schedule_run:live".to_string()),
        "an in-flight run is never pruned, however old"
    );
}

/// Seed `n` aged, terminal schedule runs.
async fn seed_aged_runs(h: &Harness, n: usize, prefix: &str) {
    let old = stamp(Utc::now() - Duration::days(60));
    for i in 0..n {
        h.raw
            .query(
                "CREATE type::record('_00_schedule_run', $key) CONTENT { \
                 schedule_name: 'x', key: '', fire_at: <datetime>$old, kind: 'job', \
                 status: 'success', finished_at: <datetime>$old }",
            )
            .bind(("key", format!("{prefix}{i}")))
            .bind(("old", old.clone()))
            .await
            .expect("seed aged run");
    }
}

/// Retention runs on its own slow cadence, not once per 5-second sweep. Every
/// prune statement is a scan over a status partition, and at high fan-out that
/// made the prune the most expensive thing in a tick while almost always
/// deleting nothing.
#[tokio::test]
async fn prune_runs_on_its_own_cadence_not_every_sweep() {
    let h = harness().await;
    // The first sweep claims the prune slot.
    h.engine.tick_pass().await.unwrap();

    // Aged history appears afterwards, so the next sweep is inside the interval.
    seed_aged_runs(&h, 1, "gated").await;
    let report = h.engine.tick_pass().await.unwrap();

    assert_eq!(report.pruned, 0, "a second sweep in the same interval does not prune");
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await,
        1,
        "the row survives until the prune interval elapses"
    );
}

/// A backlog bigger than one batch keeps draining within the same pass. Deleting a
/// row costs a round-trip on a synced table, so each statement is capped — but the
/// loop must continue while batches come back full, or a backlog would drain at one
/// batch per minute and never catch up.
#[tokio::test]
async fn prune_drains_a_backlog_larger_than_one_batch() {
    let h = harness().await;
    seed_aged_runs(&h, 601, "bulk").await;

    let report = h.engine.tick_pass().await.unwrap();

    assert_eq!(report.pruned, 601, "a full batch is followed by another");
    assert_eq!(h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await, 0);
}

/// A workflow run is pruned together with its steps, and a step whose run is
/// still live is never touched. The step delete rides `workflow_run INSIDE $ids`
/// rather than dereferencing `workflow_run.status`, which was a point read per
/// candidate row.
#[tokio::test]
async fn pruning_a_workflow_run_takes_its_steps_and_spares_live_ones() {
    let h = harness().await;
    let old = stamp(Utc::now() - Duration::days(60));
    h.raw
        .query(
            "CREATE _00_workflow_run:done CONTENT { workflow_name: 'w', dag: {}, \
             status: 'success', finished_at: <datetime>$old }; \
             CREATE _00_workflow_run:live CONTENT { workflow_name: 'w', dag: {}, \
             status: 'running' }; \
             CREATE _00_step_run:s1 CONTENT { workflow_run: _00_workflow_run:done, \
             step: 'a', status: 'success', finished_at: <datetime>$old }; \
             CREATE _00_step_run:s2 CONTENT { workflow_run: _00_workflow_run:live, \
             step: 'a', status: 'success', finished_at: <datetime>$old }",
        )
        .bind(("old", old))
        .await
        .expect("seed workflow history");

    h.engine.tick_pass().await.unwrap();

    let wf = h.strings("SELECT VALUE type::string(id) FROM _00_workflow_run").await;
    assert!(!wf.contains(&"_00_workflow_run:done".to_string()), "aged run pruned");
    assert!(wf.contains(&"_00_workflow_run:live".to_string()), "in-flight run spared");

    let steps = h.strings("SELECT VALUE type::string(id) FROM _00_step_run").await;
    assert!(
        !steps.contains(&"_00_step_run:s1".to_string()),
        "a pruned run takes its steps with it"
    );
    assert!(
        steps.contains(&"_00_step_run:s2".to_string()),
        "a step whose run is still running is never pruned"
    );
}

/// The `job_missing` safety property.
///
/// `heal_pass` runs before `prune_pass` in the same tick, so a job that reached a
/// terminal status without its ingest event arriving is reconciled BEFORE anything
/// is pruned. If that order ever inverted, a pruned job row would make `heal_pass`
/// finalize a perfectly successful run as `failed` with `code: "job_missing"`.
#[tokio::test]
async fn a_tick_that_prunes_still_heals_first() {
    let h = harness().await;
    let job = fire_once(&h).await;
    h.finish_job(&job, "success", json!({"ok": true})).await;
    // Aged history so this sweep both heals and prunes. `fire_once` already swept,
    // which claimed the prune slot.
    seed_aged_runs(&h, 1, "aged").await;
    h.engine.force_prune_next_pass();

    let report = h.engine.tick_pass().await.unwrap();

    assert!(report.healed >= 1, "the lost event was reconciled");
    assert_eq!(report.pruned, 1, "and the same tick pruned aged history");
    assert_eq!(
        h.one_string(
            "SELECT VALUE status FROM ONLY _00_schedule_run WHERE schedule_name != 'x' LIMIT 1"
        )
        .await
        .as_deref(),
        Some("success"),
        "healing precedes pruning, so the run is never mislabelled job_missing"
    );
}

// --- workflows --------------------------------------------------------------

/// extract-orders ┐
///                ├→ transform → {notify, archive}
/// extract-users  ┘
fn diamond_workflow() -> Value {
    json!({
        "kind": "workflow",
        "every_ms": 3_600_000,
        "target_table": "job",
        "workflow": {
            "target_table": "job",
            "steps": [
                {"name": "extract-orders", "path": "/exportOrders"},
                {"name": "extract-users", "path": "/exportUsers"},
                {"name": "transform", "path": "/buildReport",
                 "depends_on": ["extract-orders", "extract-users"]},
                {"name": "notify", "path": "/postSlack", "depends_on": ["transform"]},
                {"name": "archive", "path": "/archiveReport", "depends_on": ["transform"]},
            ],
            "on_failure": "halt",
        },
    })
}

async fn workflow_harness(def: Value) -> Harness {
    let h = harness().await;
    h.define_schedule("report", def).await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:report SET next_fire_at = time::now() - 1s").await;
    h.engine.tick_pass().await.unwrap();
    h
}

async fn workflow_run_id(h: &Harness) -> ids::Ref {
    let raw = h
        .one_string("SELECT VALUE type::string(id) FROM ONLY _00_workflow_run WHERE true LIMIT 1")
        .await
        .expect("a workflow run exists");
    ids::Ref::parse(&raw).expect("a well-formed record id")
}

#[tokio::test]
async fn a_workflow_dispatches_its_roots_in_parallel_and_blocks_the_rest() {
    let h = workflow_harness(diamond_workflow()).await;

    assert_eq!(h.step_status("extract-orders").await.as_deref(), Some("dispatched"));
    assert_eq!(h.step_status("extract-users").await.as_deref(), Some("dispatched"));
    assert_eq!(h.step_status("transform").await.as_deref(), Some("blocked"));
    assert_eq!(h.job_ids().await.len(), 2, "only the roots have jobs");
    assert_eq!(h.workflow_status().await.as_deref(), Some("running"));
}

#[tokio::test]
async fn a_join_waits_for_every_dependency_then_passes_their_outputs_on() {
    let h = workflow_harness(diamond_workflow()).await;

    // First branch done: the join must still wait.
    h.finish_job(
        &h.step_job("extract-orders").await.unwrap(),
        "success",
        json!({"fileId": "orders-1"}),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.step_status("extract-orders").await.as_deref(), Some("success"));
    assert_eq!(h.step_status("transform").await.as_deref(), Some("blocked"), "join not satisfied");

    // Second branch done: the join dispatches, carrying both outputs.
    h.finish_job(
        &h.step_job("extract-users").await.unwrap(),
        "success",
        json!({"fileId": "users-1"}),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.step_status("transform").await.as_deref(), Some("dispatched"));

    let transform_job = h.step_job("transform").await.unwrap();
    let orders: Option<String> = h
        .raw
        .query("SELECT VALUE payload.steps.`extract-orders`.fileId FROM ONLY type::record($id)")
        .bind(("id", transform_job.clone()))
        .await
        .expect("query")
        .take(0)
        .expect("take");
    assert_eq!(orders.as_deref(), Some("orders-1"), "a step reads its dependency's output");

    // The join's own completion fans out to both dependents.
    h.finish_job(&transform_job, "success", json!({"report": "r-1"})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.step_status("notify").await.as_deref(), Some("dispatched"));
    assert_eq!(h.step_status("archive").await.as_deref(), Some("dispatched"));
}

#[tokio::test]
async fn a_workflow_succeeds_only_once_every_step_has() {
    let h = workflow_harness(diamond_workflow()).await;
    for step in ["extract-orders", "extract-users"] {
        h.finish_job(&h.step_job(step).await.unwrap(), "success", json!({"fileId": step})).await;
    }
    h.engine.tick_pass().await.unwrap();
    h.finish_job(&h.step_job("transform").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.workflow_status().await.as_deref(), Some("running"), "leaves still in flight");

    for step in ["notify", "archive"] {
        h.finish_job(&h.step_job(step).await.unwrap(), "success", json!({})).await;
    }
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.workflow_status().await.as_deref(), Some("success"));
    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("success"),
        "the outcome mirrors onto the schedule run that spawned it"
    );
}

#[tokio::test]
async fn halt_skips_everything_downstream_of_a_failure() {
    let h = workflow_harness(diamond_workflow()).await;

    h.fail_job(&h.step_job("extract-orders").await.unwrap(), "boom").await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.step_status("extract-orders").await.as_deref(), Some("failed"));
    assert_eq!(h.step_status("transform").await.as_deref(), Some("skipped"));
    assert_eq!(h.step_status("notify").await.as_deref(), Some("skipped"));
    assert_eq!(
        h.step_status("extract-users").await.as_deref(),
        Some("dispatched"),
        "a sibling already in flight is left to finish rather than abandoned"
    );

    // Once the in-flight sibling lands, the run terminalizes as failed.
    h.finish_job(&h.step_job("extract-users").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.workflow_status().await.as_deref(), Some("failed"));
    assert_eq!(
        h.one_string("SELECT VALUE error.step FROM ONLY _00_workflow_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("extract-orders"),
        "the failing step is named"
    );
}

#[tokio::test]
async fn continue_independent_only_skips_the_affected_branch() {
    // extract-orders → transform        (this branch dies)
    // independent → after-independent   (this one must still run)
    let h = workflow_harness(json!({
        "kind": "workflow",
        "every_ms": 3_600_000,
        "target_table": "job",
        "workflow": {
            "target_table": "job",
            "steps": [
                {"name": "extract-orders", "path": "/a"},
                {"name": "transform", "path": "/b", "depends_on": ["extract-orders"]},
                {"name": "independent", "path": "/c"},
                {"name": "after-independent", "path": "/d", "depends_on": ["independent"]},
            ],
            "on_failure": "continue-independent",
        },
    }))
    .await;

    h.fail_job(&h.step_job("extract-orders").await.unwrap(), "boom").await;
    h.finish_job(&h.step_job("independent").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.step_status("transform").await.as_deref(), Some("skipped"), "doomed branch");
    // A skipped step is the most common row on a failed run and the least
    // self-explanatory: `skipped` alone cannot say whether this step was doomed by
    // its own branch or stopped because an unrelated one broke.
    let run = workflow_run_id(&h).await;
    let reason = h.step_error(&run, "transform").await.expect("a skipped step says why");
    assert_eq!(reason["code"], json!("upstream_failed"));
    assert_eq!(
        reason["blocked_by"],
        json!("extract-orders"),
        "the step names the dependency that doomed it, which is one hop away"
    );
    assert_eq!(reason["reason"], json!("depends on extract-orders, which did not succeed"));
    assert_eq!(
        h.step_status("after-independent").await.as_deref(),
        Some("dispatched"),
        "an unrelated branch keeps going"
    );

    h.finish_job(&h.step_job("after-independent").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(
        h.workflow_status().await.as_deref(),
        Some("failed"),
        "the run still failed overall"
    );
}

#[tokio::test]
async fn a_retrying_step_is_not_treated_as_failed() {
    let h = workflow_harness(diamond_workflow()).await;
    let job = h.step_job("extract-orders").await.unwrap();

    // The runner's retry path puts the row back to `pending` between attempts.
    h.raw
        .query("UPDATE type::record($id) SET status = 'pending', retries = 1")
        .bind(("id", job.clone()))
        .await
        .unwrap();
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.step_status("extract-orders").await.as_deref(),
        Some("dispatched"),
        "per-step retry is the job's own retry budget; the step waits it out"
    );
    assert_eq!(h.workflow_status().await.as_deref(), Some("running"));
}

#[tokio::test]
async fn killing_a_run_skips_what_hasnt_started_and_kills_what_has() {
    let h = workflow_harness(diamond_workflow()).await;
    let run_id = workflow_run_id(&h).await;
    let root_jobs = [
        h.step_job("extract-orders").await.unwrap(),
        h.step_job("extract-users").await.unwrap(),
    ];

    // The operator writes only the flag; the engine does the rest.
    h.set(&format!("UPDATE {run_id} SET kill_requested = true")).await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.workflow_status().await.as_deref(), Some("killed"));
    assert_eq!(h.step_status("transform").await.as_deref(), Some("skipped"));
    // `skipped` here means "never started", not "ran and was discarded", and the two
    // are indistinguishable from the status. A kill must not leave a step looking as
    // though its own branch was at fault.
    let reason = h.step_error(&run_id, "transform").await.expect("a skipped step says why");
    assert_eq!(reason["code"], json!("not_started"));
    assert_eq!(reason["reason"], json!("the run was killed before this step started"));
    assert_eq!(reason["run_error"]["code"], json!("killed"));
    let killed = h.kill.killed.lock().unwrap().clone();
    for job in root_jobs {
        assert!(killed.contains(&job), "in-flight step job {job} should have been killed");
    }
    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("killed")
    );
}

#[tokio::test]
async fn advancing_twice_concurrently_dispatches_each_step_once() {
    let h = workflow_harness(diamond_workflow()).await;
    for step in ["extract-orders", "extract-users"] {
        h.finish_job(&h.step_job(step).await.unwrap(), "success", json!({})).await;
    }
    let run_id = workflow_run_id(&h).await;

    // An ingest event and a heal pass can genuinely land at the same moment.
    let rival = h.rival();
    let (a, b) = tokio::join!(h.engine.advance_workflow(&run_id), rival.advance_workflow(&run_id));
    a.unwrap();
    b.unwrap();

    assert_eq!(
        h.count("SELECT VALUE count() FROM job WHERE path = '/buildReport' GROUP ALL").await,
        1,
        "the blocked→ready CAS means only one pass dispatches the join"
    );
}

#[tokio::test]
async fn a_dag_with_a_cycle_fails_its_run_instead_of_wedging_the_sweep() {
    let h = harness().await;
    h.define_schedule(
        "cyclic",
        json!({
            "kind": "workflow",
            "every_ms": 3_600_000,
            "target_table": "job",
            "workflow": {
                "target_table": "job",
                "steps": [
                    {"name": "a", "path": "/a", "depends_on": ["b"]},
                    {"name": "b", "path": "/b", "depends_on": ["a"]},
                ],
            },
        }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:cyclic SET next_fire_at = time::now() - 1s").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.errored, 1, "the bad definition is reported, not panicked on");
    assert!(h.job_ids().await.is_empty());

    // And a healthy schedule still fires in the same sweep.
    h.define_schedule("nightly", every_5m_job()).await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:nightly SET next_fire_at = time::now() - 1s").await;
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.spawned, 1);
}


// --- outbox-schema fidelity regressions -------------------------------------

/// Plan a freshly defined schedule, then make it due and sweep. `every_ms`
/// schedules are planned into the future by the first pass, so a test that wants
/// an immediate fire has to move the clock back the way the fan-out harness does.
async fn plan_then_fire(h: &Harness, name: &str) -> crate::engine::TickReport {
    h.engine.tick_pass().await.unwrap();
    h.raw
        .query("UPDATE type::record('_00_schedule', $n) SET next_fire_at = time::now() - 1s")
        .bind(("n", name.to_string()))
        .await
        .expect("make due");
    h.engine.tick_pass().await.unwrap()
}

/// A schedule that declares `timeout:` must still spawn.
///
/// `build_job_content` puts `timeout` in the job content, so the outbox table has
/// to define the field. It did not: the shipped `spky add api` template had no
/// `timeout` field, and on a SCHEMAFULL table that rejects the ENTIRE create with
/// "Found field 'timeout', but no such field exists for table 'job'". Only the
/// loose test stub (which did define it) hid this.
#[tokio::test]
async fn a_schedule_that_declares_a_timeout_still_spawns_its_job() {
    let h = harness().await;
    let mut spec = every_5m_job();
    // SECONDS. The field's own DDL comment used to say milliseconds and this fixture
    // used 30_000, which every consumer reads through `Duration::from_secs` as eight
    // hours. Harmless while the value only bounded an HTTP call; it now also sizes the
    // reclaim lease, so the unit decides whether a stuck row is recoverable this
    // afternoon or next spring.
    spec.as_object_mut().unwrap().insert("timeout".into(), json!(30));
    h.define_schedule("with-timeout", spec).await;

    let report = plan_then_fire(&h, "with-timeout").await;

    assert_eq!(report.spawned, 1, "a timeout on the schedule must not block the spawn");
    assert_eq!(report.errored, 0);
    assert_eq!(
        h.count("SELECT VALUE count() FROM job WHERE timeout = 30 GROUP ALL").await,
        1,
        "the per-job timeout override reaches the job row"
    );
}

/// A scheduled job belongs to no domain record, so it is created with no
/// `assigned_to`.
///
/// The shipped template declared `assigned_to TYPE record` (required), which made
/// EVERY scheduled fire fail with "Expected `record` but found `NONE`" — the run
/// was finalized `failed` with `spawn_failed` and no job ever ran. Invisible to
/// tests because the stub omitted the field entirely.
#[tokio::test]
async fn a_scheduled_job_spawns_without_an_assigned_to() {
    let h = harness().await;
    h.define_schedule("nightly", every_5m_job()).await;

    let report = plan_then_fire(&h, "nightly").await;

    assert_eq!(report.spawned, 1);
    assert_eq!(report.errored, 0);
    assert_eq!(
        h.count("SELECT VALUE count() FROM job WHERE assigned_to = NONE GROUP ALL").await,
        1,
        "system-initiated jobs carry no owning record"
    );
    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("running"),
        "the run is live, not failed with spawn_failed"
    );
}

// --- fan-out keying ---------------------------------------------------------

/// Two forEach rows that produce the SAME key collapse into one job.
///
/// The run id is `hash(schedule, fire_at, key)`, and a duplicate id is read as
/// "this fire already happened". So a `key:` pointing at a non-unique field (or at
/// a field that does not exist, which makes every key `''`) silently drops all but
/// one row of the fan-out. With `allow` nothing is recorded for the dropped rows at
/// all — `spawned` is 1 and there is no run row to show the operator.
#[tokio::test]
async fn duplicate_fan_out_keys_silently_collapse_into_one_run() {
    let h = harness().await;
    h.set("CREATE tenant:a SET region = 'eu'; CREATE tenant:b SET region = 'eu'").await;
    h.define_schedule(
        "per-region",
        json!({
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/sync",
            "for_each": "SELECT region FROM tenant",
            "for_each_key": "region",
            "concurrency": "allow",
        }),
    )
    .await;

    let report = plan_then_fire(&h, "per-region").await;

    assert_eq!(report.spawned, 1, "both rows share key 'eu', so the second is a no-op");
    assert_eq!(h.count("SELECT VALUE count() FROM job GROUP ALL").await, 1);
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await,
        1,
        "the dropped row leaves no run row behind"
    );
    let err = h.one_string("SELECT VALUE last_error FROM ONLY _00_schedule:`per-region`").await;
    assert!(
        err.as_deref().is_some_and(|e| e.contains("duplicate key")),
        "a collapsed fan-out must be reported, not silently dropped: {err:?}"
    );
}

/// A `key:` naming a field the forEach rows do not have degrades to a single
/// serialized run rather than N overlapping ones (`fan_out_key` returns `''`).
/// Documented behaviour, pinned so it stays deliberate.
#[tokio::test]
async fn a_missing_key_field_serializes_the_whole_fan_out() {
    let h = harness().await;
    h.set("CREATE tenant:a SET region = 'eu'; CREATE tenant:b SET region = 'us'").await;
    h.define_schedule(
        "bad-key",
        json!({
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/sync",
            "for_each": "SELECT region FROM tenant",
            "for_each_key": "nope",
            "concurrency": "skip",
        }),
    )
    .await;

    let report = plan_then_fire(&h, "bad-key").await;

    assert_eq!(report.spawned, 1);
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run WHERE key = '' GROUP ALL").await,
        1,
        "every row collapsed onto the empty key"
    );
}

/// A forEach wider than `fanout_max` is truncated LOUDLY: the excess is dropped,
/// the schedule records why, and the fire is not silently partial.
#[tokio::test]
async fn a_fan_out_wider_than_the_cap_is_truncated_and_recorded() {
    let db = Surreal::new::<Mem>(()).await.expect("start mem db");
    db.use_ns("test").use_db("test").await.expect("use ns/db");
    let raw = Arc::new(db);
    raw.query(SCHEDULE_TABLES).await.expect("schema");
    raw.query(OUTBOX_DDL).await.expect("outbox");
    let kill = Arc::new(RecordingKill::default());
    let h = Harness {
        engine: ScheduleEngine::new(
            Arc::new(MemDb(Arc::clone(&raw))),
            Arc::clone(&kill) as Arc<dyn JobKill>,
            EngineConfig { fanout_max: 2, history_max_age: Duration::days(30) },
        ),
        raw,
        kill,
    };
    h.set("CREATE tenant:a SET n = 1; CREATE tenant:b SET n = 2; CREATE tenant:c SET n = 3").await;
    h.define_schedule(
        "wide",
        json!({
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/sync",
            "for_each": "SELECT n FROM tenant",
            "for_each_key": "n",
            "concurrency": "allow",
        }),
    )
    .await;

    let report = plan_then_fire(&h, "wide").await;

    assert_eq!(report.spawned, 2, "capped at fanout_max");
    let err = h.one_string("SELECT VALUE last_error FROM ONLY _00_schedule:wide").await;
    assert!(
        err.as_deref().is_some_and(|e| e.contains("fan-out") || e.contains("cap")),
        "the truncation must be recorded on the schedule, got {err:?}"
    );
}

/// A step that names no outbox table cannot dispatch, and must FAIL rather than
/// sit `ready` forever.
///
/// `FINALIZE_STEP` is guarded on `dispatched`, so it would silently match nothing
/// for a step stuck at `ready` — which is exactly why `FAIL_UNDISPATCHABLE_STEP`
/// exists and is guarded on `ready` instead. Without it the whole run wedges.
#[tokio::test]
async fn a_step_with_no_outbox_table_fails_instead_of_wedging_the_run() {
    let h = harness().await;
    h.define_schedule(
        "no-table",
        json!({
            "kind": "workflow",
            "every_ms": 3_600_000,
            "workflow": {
                // No target_table anywhere: not on the workflow, not on the step.
                "steps": [{"name": "only", "path": "/run"}],
                "on_failure": "halt",
            },
        }),
    )
    .await;

    plan_then_fire(&h, "no-table").await;

    assert_eq!(
        h.step_status("only").await.as_deref(),
        Some("failed"),
        "an undispatchable step must land terminal, not stay ready"
    );
    assert_eq!(
        h.workflow_status().await.as_deref(),
        Some("failed"),
        "and it must take the run down with it"
    );
    assert_eq!(h.count("SELECT VALUE count() FROM job GROUP ALL").await, 0);
}

/// A step's captured output reaches its dependants through `_00_step_run.output`,
/// NOT by reading the job row. That is what makes pruning finished job rows safe:
/// the output is copied at finalize time, so a dependant can still start after the
/// upstream job row is gone.
#[tokio::test]
async fn a_dependant_still_gets_its_input_after_the_upstream_job_row_is_deleted() {
    let h = workflow_harness(json!({
        "kind": "workflow",
        "every_ms": 3_600_000,
        "target_table": "job",
        "workflow": {
            "target_table": "job",
            "steps": [
                {"name": "first", "path": "/first"},
                {"name": "second", "path": "/second", "depends_on": ["first"]},
            ],
            "on_failure": "halt",
        },
    }))
    .await;

    let first_job = h
        .one_string("SELECT VALUE job_id FROM ONLY _00_step_run WHERE step = 'first' LIMIT 1")
        .await
        .expect("first step dispatched a job");
    h.finish_job(&first_job, "success", json!({"rows": 42})).await;
    h.engine.tick_pass().await.unwrap();

    // The upstream job row is gone (retention, or `spky jobs clear`).
    h.set(&format!("DELETE {first_job}")).await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.step_status("second").await.as_deref(), Some("dispatched"));
    let payload = h
        .one_string(
            "SELECT VALUE <string>payload.steps.first.rows FROM ONLY job \
             WHERE path = '/second' LIMIT 1",
        )
        .await;
    assert_eq!(
        payload.as_deref(),
        Some("42"),
        "the dependant reads output off the step row, so a pruned job row is harmless"
    );
}

/// Only DIRECT dependencies are injected. A long chain must not accumulate every
/// ancestor's output in every payload.
#[tokio::test]
async fn a_chain_injects_only_direct_dependency_outputs() {
    let h = workflow_harness(json!({
        "kind": "workflow",
        "every_ms": 3_600_000,
        "target_table": "job",
        "workflow": {
            "target_table": "job",
            "steps": [
                {"name": "a", "path": "/a"},
                {"name": "b", "path": "/b", "depends_on": ["a"]},
                {"name": "c", "path": "/c", "depends_on": ["b"]},
            ],
            "on_failure": "halt",
        },
    }))
    .await;

    for (step, path) in [("a", "/a"), ("b", "/b")] {
        let job = h
            .one_string(&format!(
                "SELECT VALUE job_id FROM ONLY _00_step_run WHERE step = '{step}' LIMIT 1"
            ))
            .await
            .unwrap_or_else(|| panic!("{step} dispatched"));
        h.finish_job(&job, "success", json!({"from": path})).await;
        h.engine.tick_pass().await.unwrap();
    }

    let keys = h
        .one_string(
            "SELECT VALUE <string>array::sort(object::keys(payload.steps)) FROM ONLY job \
             WHERE path = '/c' LIMIT 1",
        )
        .await;
    assert_eq!(
        keys.as_deref(),
        Some("['b']"),
        "c depends only on b, so a's output must not be carried along"
    );
}

/// Two engines advancing the same run concurrently must not double-dispatch, and
/// must not leave a step claimed-but-never-dispatched either.
#[tokio::test]
async fn a_rival_engine_cannot_double_dispatch_a_ready_step() {
    let h = workflow_harness(diamond_workflow()).await;
    let wf = workflow_run_id(&h).await;
    let rival = h.rival();

    let (a, b) = tokio::join!(h.engine.advance_workflow(&wf), rival.advance_workflow(&wf));
    a.unwrap();
    b.unwrap();

    assert_eq!(
        h.count("SELECT VALUE count() FROM job GROUP ALL").await,
        2,
        "the two roots dispatch exactly once each, however many passes race"
    );
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_step_run WHERE status = 'dispatched' GROUP ALL")
            .await,
        2
    );
}



// --- outcome-asymmetric retention -------------------------------------------

/// Write the deploy-owned policy row.
async fn set_retention(h: &Harness, patch: &str) {
    h.set(&format!("UPSERT _00_retention:default MERGE {{ {patch} }}")).await;
}

/// Seed one terminal run in a given status, aged `secs` seconds.
async fn seed_run(h: &Harness, id: &str, schedule: &str, status: &str, secs: i64) {
    h.raw
        .query(
            "CREATE type::record('_00_schedule_run', $id) CONTENT { \
             schedule_name: $sched, key: '', fire_at: <datetime>$old, kind: 'job', \
             status: $status, finished_at: <datetime>$old }",
        )
        .bind(("id", id.to_string()))
        .bind(("sched", schedule.to_string()))
        .bind(("status", status.to_string()))
        .bind(("old", stamp(Utc::now() - Duration::seconds(secs))))
        .await
        .expect("seed run");
}

async fn surviving_runs(h: &Harness) -> Vec<String> {
    let mut ids = h.strings("SELECT VALUE type::string(id) FROM _00_schedule_run").await;
    ids.sort();
    ids
}

/// Successes die on the short window while failures keep the long one. This is the
/// whole point of the feature: a successful run is read once, if ever, but a failure
/// is the thing you came looking for.
#[tokio::test]
async fn successes_are_pruned_long_before_failures() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 60, run_failed_secs: 86400").await;
    seed_run(&h, "ok", "x", "success", 600).await;
    seed_run(&h, "bad", "x", "failed", 600).await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        surviving_runs(&h).await,
        vec!["_00_schedule_run:bad"],
        "the 10-minute-old success is past its 60s window; the failure is not past its day"
    );
}

/// `skipped` and `replaced` follow the SUCCESS window, not the failure one. With the
/// default `concurrency: skip` a slow wide fan-out writes one `skipped` row per item
/// per fire — the highest-volume, lowest-information rows in the system.
#[tokio::test]
async fn suppressed_and_replaced_runs_follow_the_success_window() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 60, run_failed_secs: 86400").await;
    seed_run(&h, "skip1", "x", "skipped", 600).await;
    seed_run(&h, "repl1", "x", "replaced", 600).await;
    seed_run(&h, "kill1", "x", "killed", 600).await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        surviving_runs(&h).await,
        vec!["_00_schedule_run:kill1"],
        "killed is a failure outcome and keeps the long window"
    );
}

/// A per-schedule `history:` override wins over the project default, and the global
/// pass must not prune that schedule's rows on the default window behind its back.
#[tokio::test]
async fn a_per_schedule_history_override_wins_over_the_project_default() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 60, run_failed_secs: 86400").await;
    // `noisy` keeps successes for a day; the project default is 60s.
    h.define_schedule(
        "noisy",
        json!({
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/sync",
            "history_success_secs": 86400,
        }),
    )
    .await;
    seed_run(&h, "noisy1", "noisy", "success", 600).await;
    seed_run(&h, "other1", "other", "success", 600).await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        surviving_runs(&h).await,
        vec!["_00_schedule_run:noisy1"],
        "the override keeps noisy's history; the default still prunes everyone else"
    );
}

/// A per-schedule override can also be SHORTER than the default, which is the case
/// the feature exists for: one minutely fan-out that should not accumulate.
#[tokio::test]
async fn a_shorter_per_schedule_override_prunes_sooner_than_the_default() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 86400, run_failed_secs: 86400").await;
    h.define_schedule(
        "chatty",
        json!({
            "kind": "job",
            "every_ms": 60_000,
            "target_table": "job",
            "path": "/sync",
            "history_success_secs": 60,
        }),
    )
    .await;
    seed_run(&h, "chatty1", "chatty", "success", 600).await;
    seed_run(&h, "calm1", "calm", "success", 600).await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(surviving_runs(&h).await, vec!["_00_schedule_run:calm1"]);
}

// --- outbox job-table retention ---------------------------------------------

/// Terminal job rows are pruned on the same asymmetric windows, and in-flight rows
/// are never touched at any age.
#[tokio::test]
async fn job_rows_are_pruned_by_outcome_and_never_while_in_flight() {
    let h = harness().await;
    set_retention(&h, "success_secs: 60, failed_secs: 86400, job_tables: ['job']").await;
    let old = stamp(Utc::now() - Duration::seconds(600));
    for (id, status) in
        [("ok", "success"), ("bad", "failed"), ("queued", "pending"), ("busy", "processing")]
    {
        h.raw
            .query(
                "CREATE type::record('job', $id) CONTENT { path: '/run', payload: {}, \
                 status: $status, updated_at: <datetime>$old }",
            )
            .bind(("id", id.to_string()))
            .bind(("status", status.to_string()))
            .bind(("old", old.clone()))
            .await
            .expect("seed job");
    }

    h.engine.tick_pass().await.unwrap();

    let mut ids = h.job_ids().await;
    ids.sort();
    assert_eq!(
        ids,
        vec!["job:bad", "job:busy", "job:queued"],
        "only the aged success is gone: pending and processing are never pruned, and \
         the failure keeps the long window"
    );
}

/// A table that deploy did not list in `_00_retention.job_tables` is never swept,
/// so retention can never reach a table the platform does not own.
#[tokio::test]
async fn an_unlisted_job_table_is_never_swept() {
    let h = harness().await;
    set_retention(&h, "success_secs: 60, failed_secs: 60, job_tables: []").await;
    h.raw
        .query(
            "CREATE job:ok CONTENT { path: '/run', payload: {}, status: 'success', \
             updated_at: <datetime>$old }",
        )
        .bind(("old", stamp(Utc::now() - Duration::seconds(600))))
        .await
        .expect("seed job");

    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.job_ids().await, vec!["job:ok"]);
}

/// With no policy row at all — a fresh database, or a stack running ahead of its
/// CLI — retention falls back to the engine defaults instead of pruning nothing or
/// everything.
#[tokio::test]
async fn a_missing_policy_row_falls_back_to_the_engine_defaults() {
    let h = harness().await;
    seed_run(&h, "ancient", "x", "success", 60 * 60 * 24 * 60).await;
    seed_run(&h, "recent", "x", "success", 60).await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        surviving_runs(&h).await,
        vec!["_00_schedule_run:recent"],
        "the built-in 30-day window still applies with no _00_retention row"
    );
}

// --- last-run denormalization -----------------------------------------------

/// A finished run records its outcome on the schedule row, so `spky schedules list`
/// needs neither a scan of the run table nor the run row to still exist.
#[tokio::test]
async fn a_finished_run_records_its_outcome_on_the_schedule() {
    let h = harness().await;
    let job = fire_once(&h).await;
    h.finish_job(&job, "success", json!({"ok": true})).await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.one_string("SELECT VALUE last_run_status FROM ONLY _00_schedule:nightly").await.as_deref(),
        Some("success")
    );
    assert_eq!(
        h.one_bool("SELECT VALUE last_run_at != NONE FROM ONLY _00_schedule:nightly").await,
        Some(true)
    );
}

/// Within one fire of a fan-out, a failure must win over a success — a single status
/// cannot summarise N items, and the failure is the one worth surfacing.
#[tokio::test]
async fn a_failure_in_a_fan_out_wins_over_a_sibling_success() {
    let h = fan_out_harness("allow").await;
    h.engine.tick_pass().await.unwrap();

    let jobs = h.job_ids().await;
    assert_eq!(jobs.len(), 2, "alice and bob both spawned");
    h.finish_job(&jobs[0], "success", json!({})).await;
    h.fail_job(&jobs[1], "boom").await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.one_string("SELECT VALUE last_run_status FROM ONLY _00_schedule:`game-sync`")
            .await
            .as_deref(),
        Some("failed"),
        "a fan-out where any item failed must not report success"
    );
}

/// Deleting a schedule while its history lives on must not resurrect the schedule
/// row: `spky schedules list` would then show a nameless, unparseable entry.
#[tokio::test]
async fn recording_an_outcome_never_resurrects_a_deleted_schedule() {
    let h = harness().await;
    let job = fire_once(&h).await;
    h.set("DELETE _00_schedule:nightly").await;
    h.finish_job(&job, "success", json!({"ok": true})).await;

    h.engine.tick_pass().await.unwrap();

    assert!(
        h.strings("SELECT VALUE type::string(id) FROM _00_schedule").await.is_empty(),
        "the schedule stays deleted"
    );
}

// --- rollup counters ---------------------------------------------------------

async fn rollup_rows(h: &Harness) -> Vec<String> {
    let mut rows = h
        .strings(
            "SELECT VALUE string::concat(scope, '/', name, ' s=', <string>success, \
             ' f=', <string>failed, ' sk=', <string>skipped, ' r=', <string>replaced, \
             ' k=', <string>killed) FROM _00_run_rollup",
        )
        .await;
    rows.sort();
    rows
}

/// Pruning folds what it deleted into an hourly counter first. This is what makes a
/// short retention window acceptable: the rows go, the totals stay.
#[tokio::test]
async fn pruning_folds_the_deleted_rows_into_hourly_counters() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 60, run_failed_secs: 60").await;
    for (id, status) in [("a", "success"), ("b", "success"), ("c", "failed")] {
        seed_run(&h, id, "nightly", status, 600).await;
    }

    h.engine.tick_pass().await.unwrap();

    assert!(surviving_runs(&h).await.is_empty(), "all three were past their window");
    assert_eq!(
        rollup_rows(&h).await,
        vec!["schedule/nightly s=2 f=1 sk=0 r=0 k=0"],
        "one bucket per (scope, name, hour), with each outcome counted separately"
    );
}

/// A second prune into the same hour accumulates rather than replacing. The fold is a
/// blind `UPSERT ... +=`, so this also covers two pruners racing one bucket.
#[tokio::test]
async fn folding_the_same_bucket_twice_accumulates() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 60, run_failed_secs: 60").await;
    seed_run(&h, "a", "nightly", "success", 600).await;
    h.engine.tick_pass().await.unwrap();

    seed_run(&h, "b", "nightly", "success", 600).await;
    h.engine.force_prune_next_pass();
    h.engine.tick_pass().await.unwrap();

    assert_eq!(rollup_rows(&h).await, vec!["schedule/nightly s=2 f=0 sk=0 r=0 k=0"]);
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_run_rollup GROUP ALL").await,
        1,
        "same hour, same bucket row"
    );
}

/// Rows from different schedules and different hours land in different buckets, so a
/// rollup stays attributable rather than becoming one global number.
#[tokio::test]
async fn buckets_are_keyed_by_scope_name_and_hour() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 60, run_failed_secs: 60").await;
    seed_run(&h, "a", "nightly", "success", 600).await;
    seed_run(&h, "b", "hourly", "success", 600).await;
    // Two hours older: a different bucket for the same schedule.
    seed_run(&h, "c", "nightly", "success", 2 * 60 * 60 + 600).await;

    h.engine.tick_pass().await.unwrap();

    let rows = rollup_rows(&h).await;
    assert_eq!(rows.len(), 3, "two schedules x distinct hours: {rows:?}");
    assert!(rows.iter().any(|r| r.starts_with("schedule/hourly")));
    assert_eq!(rows.iter().filter(|r| r.starts_with("schedule/nightly")).count(), 2);
}

/// Pruned JOB rows are counted too, under the table name.
#[tokio::test]
async fn pruned_job_rows_are_counted_under_their_table() {
    let h = harness().await;
    set_retention(&h, "success_secs: 60, failed_secs: 60, job_tables: ['job']").await;
    let old = stamp(Utc::now() - Duration::seconds(600));
    for (id, status) in [("ok1", "success"), ("ok2", "success"), ("bad", "failed")] {
        h.raw
            .query(
                "CREATE type::record('job', $id) CONTENT { path: '/run', payload: {}, \
                 status: $status, updated_at: <datetime>$old }",
            )
            .bind(("id", id.to_string()))
            .bind(("status", status.to_string()))
            .bind(("old", old.clone()))
            .await
            .expect("seed job");
    }

    h.engine.tick_pass().await.unwrap();

    assert!(h.job_ids().await.is_empty());
    assert_eq!(rollup_rows(&h).await, vec!["job/job s=2 f=1 sk=0 r=0 k=0"]);
}

/// A rollup row must never be created for a prune that deleted nothing — otherwise
/// every idle minute would write a zero-count bucket forever.
#[tokio::test]
async fn a_prune_that_deletes_nothing_writes_no_counters() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 86400, run_failed_secs: 86400").await;
    seed_run(&h, "recent", "nightly", "success", 60).await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(surviving_runs(&h).await.len(), 1);
    assert!(rollup_rows(&h).await.is_empty(), "nothing was pruned, so nothing was counted");
}

// --- row cap ----------------------------------------------------------------

/// The cap trims what is left over the limit even when every row is INSIDE its age
/// window — that is the whole point of it.
#[tokio::test]
async fn the_row_cap_trims_inside_the_age_window() {
    let h = harness().await;
    // A day-long window, so age alone would keep all of these.
    set_retention(&h, "run_success_secs: 86400, run_failed_secs: 86400, max_rows: 2").await;
    for i in 0..5 {
        seed_run(&h, &format!("s{i}"), "noisy", "success", 60 + i as i64).await;
    }

    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await,
        2,
        "trimmed down to the cap"
    );
    // And the trimmed rows are still counted.
    assert_eq!(rollup_rows(&h).await, vec!["schedule/noisy s=3 f=0 sk=0 r=0 k=0"]);
}

/// The cap trims the OLDEST rows, so the newest history — the part anyone would
/// actually look at — is what survives.
#[tokio::test]
async fn the_row_cap_trims_the_oldest_rows_first() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 86400, run_failed_secs: 86400, max_rows: 1").await;
    seed_run(&h, "older", "noisy", "success", 3600).await;
    seed_run(&h, "newer", "noisy", "success", 60).await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(surviving_runs(&h).await, vec!["_00_schedule_run:newer"]);
}

/// A volume cap must never be the reason a failure disappears, so a table can sit
/// above the cap when failures alone exceed it.
#[tokio::test]
async fn the_row_cap_never_trims_failures() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 86400, run_failed_secs: 86400, max_rows: 1").await;
    for i in 0..4 {
        seed_run(&h, &format!("f{i}"), "noisy", "failed", 60 + i as i64).await;
    }

    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await,
        4,
        "failures are kept even over the cap"
    );
    assert!(rollup_rows(&h).await.is_empty());
}

/// In-flight rows are not eligible either, so a cap can never yank a running job's
/// bookkeeping out from under it.
#[tokio::test]
async fn the_row_cap_never_trims_in_flight_rows() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 86400, run_failed_secs: 86400, max_rows: 1").await;
    h.set(
        "CREATE _00_schedule_run:live1 CONTENT { schedule_name: 'noisy', key: 'a', \
         fire_at: time::now(), kind: 'job', status: 'running' }; \
         CREATE _00_schedule_run:live2 CONTENT { schedule_name: 'noisy', key: 'b', \
         fire_at: time::now(), kind: 'job', status: 'running' }",
    )
    .await;

    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await, 2);
}

/// `max_rows` unset (the DDL default of 0) means disabled, NOT "keep nothing" — the
/// difference between a no-op and deleting a project's entire history.
#[tokio::test]
async fn a_zero_row_cap_means_disabled_not_keep_nothing() {
    let h = harness().await;
    set_retention(&h, "run_success_secs: 86400, run_failed_secs: 86400, max_rows: 0").await;
    for i in 0..3 {
        seed_run(&h, &format!("s{i}"), "noisy", "success", 60).await;
    }

    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await, 3);
}



// --- batched healing ---------------------------------------------------------

/// Healing many in-flight runs must resolve them in one pass, and must classify each
/// one independently: a finished job, a failed job, and a vanished job all appear in
/// the same batch.
///
/// The lookup reads the job rows in bulk. The previous form issued one query per
/// running run, sequentially, so a 500-wide fan-out cost 500 round-trips every five
/// seconds — and sweep latency is exactly what bounds how long a lost ingest event
/// delays a completion.
#[tokio::test]
async fn one_heal_pass_resolves_a_whole_fan_out_of_in_flight_runs() {
    let h = harness().await;
    // Three runs, each waiting on its own job: one succeeded, one failed, one gone.
    for (n, status) in [("ok", Some("success")), ("bad", Some("failed")), ("gone", None)] {
        h.raw
            .query(
                "CREATE type::record('_00_schedule_run', $rid) CONTENT { \
                 schedule_name: 'wide', key: $rid, fire_at: time::now(), kind: 'job', \
                 status: 'running', job_id: $jid }",
            )
            .bind(("rid", n.to_string()))
            .bind(("jid", format!("job:sch_{n}")))
            .await
            .expect("seed run");
        if let Some(status) = status {
            h.raw
                .query(
                    "CREATE type::record('job', $id) CONTENT { path: '/x', payload: {}, \
                     status: $status, errors: $errors }",
                )
                .bind(("id", format!("sch_{n}")))
                .bind(("status", status.to_string()))
                .bind((
                    "errors",
                    if status == "failed" {
                        json!([{ "code": 500, "reason": "boom" }])
                    } else {
                        json!([])
                    },
                ))
                .await
                .expect("seed job");
        }
    }

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.healed, 3, "all three resolved in one pass");

    let mut statuses = Vec::new();
    for id in ["ok", "bad", "gone"] {
        let sql = format!("SELECT VALUE status FROM ONLY _00_schedule_run:{id} LIMIT 1");
        statuses.push(h.one_string(&sql).await);
    }
    assert_eq!(statuses[0].as_deref(), Some("success"));
    assert_eq!(statuses[1].as_deref(), Some("failed"));
    assert_eq!(
        statuses[2].as_deref(),
        Some("failed"),
        "a run whose job row vanished cannot complete, so it stops being `running`"
    );
    assert_eq!(
        h.one_string("SELECT VALUE error.code FROM ONLY _00_schedule_run:gone LIMIT 1")
            .await
            .as_deref(),
        Some("job_missing")
    );
    assert_eq!(
        h.one_string("SELECT VALUE error.reason FROM ONLY _00_schedule_run:bad LIMIT 1")
            .await
            .as_deref(),
        Some("boom"),
        "the failing job's error is carried onto its run"
    );
}

/// A job id that isn't a plain identifier is refused rather than spliced into the
/// lookup statement. The ids the engine writes are always safe; a hand-edited row is
/// the case this guards.
#[tokio::test]
async fn an_unsafe_job_id_on_a_run_is_refused_not_interpolated() {
    let h = harness().await;
    h.set(
        "CREATE _00_schedule_run:evil CONTENT { schedule_name: 'x', key: '', \
         fire_at: time::now(), kind: 'job', status: 'running', \
         job_id: 'job:a; REMOVE TABLE _00_schedule_run' }",
    )
    .await;

    // The sweep must complete normally, and the table must still be there.
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run:evil LIMIT 1")
            .await
            .as_deref(),
        Some("running"),
        "the run is skipped, not healed off a statement we refused to build"
    );
}

// --- history: failures-only ---------------------------------------------------

/// Define a fan-out schedule in `failures-only` mode and make it due.
async fn failures_only_harness() -> Harness {
    let h = harness().await;
    h.set("CREATE connection:alice SET active = true").await;
    h.define_schedule(
        "lean",
        json!({
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/sync",
            "for_each": "SELECT id FROM connection WHERE active = true",
            "for_each_key": "id",
            "concurrency": "skip",
            "history_mode": "failures-only",
        }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:lean SET next_fire_at = time::now() - 1s").await;
    h.engine.tick_pass().await.unwrap();
    h
}

/// A run that finishes cleanly leaves no row at all — but its count and its outcome
/// both survive, so nothing an operator reads gets worse.
#[tokio::test]
async fn a_successful_run_under_failures_only_leaves_no_row() {
    let h = failures_only_harness().await;
    let job = h
        .one_string("SELECT VALUE job_id FROM ONLY _00_schedule_run WHERE true LIMIT 1")
        .await
        .expect("a job was spawned");

    h.finish_job(&job, "success", json!({"ok": true})).await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await,
        0,
        "the successful run is discarded as it finalizes, not left for retention"
    );
    assert_eq!(
        rollup_rows(&h).await,
        vec!["schedule/lean s=1 f=0 sk=0 r=0 k=0"],
        "its count still lands in the rollup"
    );
    assert_eq!(
        h.one_string("SELECT VALUE last_run_status FROM ONLY _00_schedule:lean").await.as_deref(),
        Some("success"),
        "and the schedule still reports its last outcome"
    );
}

/// A FAILED run is kept, which is the entire point of the mode.
#[tokio::test]
async fn a_failed_run_under_failures_only_is_kept() {
    let h = failures_only_harness().await;
    let job = h
        .one_string("SELECT VALUE job_id FROM ONLY _00_schedule_run WHERE true LIMIT 1")
        .await
        .expect("a job was spawned");

    h.fail_job(&job, "boom").await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("failed")
    );
    assert_eq!(
        h.one_string("SELECT VALUE error.reason FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("boom"),
        "with its error intact"
    );
}

/// A suppressed tick writes NO row in the first place. This is the case that matters
/// at width: with `concurrency: skip`, a slow wide fan-out is mostly suppressed ticks,
/// and creating a row only to prune it later is the cost the mode exists to avoid.
#[tokio::test]
async fn a_suppressed_tick_under_failures_only_writes_nothing() {
    let h = failures_only_harness().await;
    // alice's run is still in flight; fire again so this tick is suppressed.
    h.set("UPDATE _00_schedule:lean SET next_fire_at = time::now() - 1s").await;
    let report = h.engine.tick_pass().await.unwrap();

    assert_eq!(report.skipped, 1, "the tick was suppressed");
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run WHERE status = 'skipped' GROUP ALL")
            .await,
        0,
        "and left no row behind"
    );
    assert_eq!(
        rollup_rows(&h).await,
        vec!["schedule/lean s=0 f=0 sk=1 r=0 k=0"],
        "the suppression is still counted, so it is observable in aggregate"
    );
}

/// The RUNNING row must still exist while the job is in flight, or `concurrency: skip`
/// would stop suppressing: it counts running rows, so discarding them early would let
/// a slow fan-out stack up on itself — the exact failure the policy prevents.
#[tokio::test]
async fn failures_only_still_keeps_the_row_while_the_run_is_in_flight() {
    let h = failures_only_harness().await;

    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run WHERE status = 'running' GROUP ALL")
            .await,
        1,
        "an in-flight run is never discarded, whatever the history mode"
    );

    // And the suppression it drives still happens.
    h.set("UPDATE _00_schedule:lean SET next_fire_at = time::now() - 1s").await;
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.skipped, 1);
    assert_eq!(report.spawned, 0, "skip still sees the in-flight run");
}

/// The mode is frozen onto the run at spawn, like a workflow's `dag`. Switching the
/// schedule to `all` mid-flight must not resurrect a run already destined for discard,
/// and vice versa — otherwise a deploy would silently change the fate of work in
/// progress.
#[tokio::test]
async fn the_history_mode_is_frozen_at_spawn() {
    let h = failures_only_harness().await;
    let job = h
        .one_string("SELECT VALUE job_id FROM ONLY _00_schedule_run WHERE true LIMIT 1")
        .await
        .expect("a job was spawned");
    assert_eq!(
        h.one_string("SELECT VALUE history_mode FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("failures-only"),
        "the run carries its own copy"
    );

    // A redeploy flips the schedule back to keeping everything.
    h.set("UPDATE _00_schedule:lean SET history_mode = 'all'").await;
    h.finish_job(&job, "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await,
        0,
        "the in-flight run keeps the mode it was spawned under"
    );
}

/// A schedule left in the default mode is unaffected: successful runs persist until
/// their retention window elapses.
#[tokio::test]
async fn the_default_mode_still_keeps_successful_runs() {
    let h = harness().await;
    let job = fire_once(&h).await;
    h.finish_job(&job, "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1")
            .await
            .as_deref(),
        Some("success"),
        "without `failures-only` a successful run is ordinary history"
    );
}

// --- failures-only cascades through a workflow --------------------------------

/// A two-step workflow schedule in `failures-only` mode, fired once.
async fn failures_only_workflow() -> Harness {
    let h = harness().await;
    h.define_schedule(
        "lean-wf",
        json!({
            "kind": "workflow",
            "every_ms": 3_600_000,
            "target_table": "job",
            "history_mode": "failures-only",
            "workflow": {
                "target_table": "job",
                "steps": [
                    {"name": "first", "path": "/first"},
                    {"name": "second", "path": "/second", "depends_on": ["first"]},
                ],
                "on_failure": "halt",
            },
        }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:`lean-wf` SET next_fire_at = time::now() - 1s").await;
    h.engine.tick_pass().await.unwrap();
    h
}

/// Drive a step to success by finishing its job and sweeping.
async fn finish_step(h: &Harness, step: &str) {
    let job = h
        .one_string(&format!(
            "SELECT VALUE job_id FROM ONLY _00_step_run WHERE step = '{step}' LIMIT 1"
        ))
        .await
        .unwrap_or_else(|| panic!("{step} dispatched a job"));
    h.finish_job(&job, "success", json!({ "from": step })).await;
    h.engine.tick_pass().await.unwrap();
}

/// A workflow that succeeds end to end leaves NOTHING: no workflow run, no steps, no
/// outer schedule run, and none of the job rows its steps spawned. Only the counters
/// and the schedule's last outcome survive.
#[tokio::test]
async fn a_successful_workflow_under_failures_only_leaves_nothing_behind() {
    let h = failures_only_workflow().await;
    finish_step(&h, "first").await;
    finish_step(&h, "second").await;

    for (table, label) in [
        ("_00_workflow_run", "the workflow run"),
        ("_00_step_run", "its step rows"),
        ("_00_schedule_run", "the outer schedule run"),
        ("job", "the job rows its steps spawned"),
    ] {
        assert_eq!(
            h.count(&format!("SELECT VALUE count() FROM {table} GROUP ALL")).await,
            0,
            "{label} should be gone"
        );
    }

    assert_eq!(
        rollup_rows(&h).await,
        vec!["schedule/lean-wf s=2 f=0 sk=0 r=0 k=0"],
        "both the schedule run and the workflow run are counted"
    );
    assert_eq!(
        h.one_string("SELECT VALUE last_run_status FROM ONLY _00_schedule:`lean-wf`")
            .await
            .as_deref(),
        Some("success")
    );
}

/// The owning schedule run must be finalized BEFORE anything is deleted (hazard 1).
///
/// If the workflow run were deleted first, the schedule run would never be finalized and
/// would stay `running` forever: the workflow heal only selects `running` WORKFLOW runs,
/// and the job heal filters `kind = 'job'`, so nothing would ever reach it. It would then
/// be seen by `SELECT_BLOCKING_RUN` forever and permanently wedge `concurrency: skip`.
/// Asserting the active count is 0 is what catches that.
#[tokio::test]
async fn discarding_a_workflow_never_strands_its_schedule_run_as_running() {
    let h = failures_only_workflow().await;
    finish_step(&h, "first").await;
    finish_step(&h, "second").await;

    assert_eq!(
        h.count(
            "SELECT VALUE count() FROM _00_schedule_run WHERE status = 'running' GROUP ALL"
        )
        .await,
        0,
        "a run left `running` here would wedge this schedule's concurrency forever"
    );
    // And a further sweep must not resurrect or mislabel anything.
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.healed, 0);
    assert_eq!(h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await, 0);
}

/// A workflow that FAILS keeps everything — including the job rows of the steps that
/// succeeded, which are exactly what you read to work out why the run died.
#[tokio::test]
async fn a_failed_workflow_under_failures_only_keeps_all_of_it() {
    let h = failures_only_workflow().await;
    finish_step(&h, "first").await;

    let job = h
        .one_string("SELECT VALUE job_id FROM ONLY _00_step_run WHERE step = 'second' LIMIT 1")
        .await
        .expect("second dispatched");
    h.fail_job(&job, "boom").await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.workflow_status().await.as_deref(), Some("failed"));
    assert_eq!(h.step_status("first").await.as_deref(), Some("success"));
    assert_eq!(h.step_status("second").await.as_deref(), Some("failed"));
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await,
        1,
        "the outer run is kept too"
    );
    assert_eq!(
        h.count("SELECT VALUE count() FROM job GROUP ALL").await,
        2,
        "both step jobs survive, including the one that succeeded"
    );
}

/// A run whose steps vanished while it was still `running` must NOT be flipped to
/// `success` (hazard 2): `all_terminal` is computed with `Iterator::all`, which is `true`
/// for an empty list, and `find(Failed)` is then `None`. Seed exactly that state.
#[tokio::test]
async fn a_running_run_with_no_steps_is_not_flipped_to_success() {
    let h = failures_only_workflow().await;
    let before = h.workflow_status().await;
    assert_eq!(before.as_deref(), Some("running"), "fixture is a live run");

    h.set("DELETE _00_step_run").await;
    h.engine.tick_pass().await.unwrap();

    // It must NOT be reported as a completed workflow. `Iterator::all` is `true` for an
    // empty list, so without the `!steps.is_empty()` guard this run is flipped to
    // `success` and then — under failures-only — discarded, announcing work that
    // provably never finished. The row surviving is the evidence the guard held.
    assert_eq!(
        h.workflow_status().await.as_deref(),
        Some("running"),
        "a run whose steps vanished is left alone, not declared successful"
    );
    assert_eq!(
        h.one_string("SELECT VALUE last_run_status FROM ONLY _00_schedule:`lean-wf`")
            .await
            .as_deref(),
        None,
        "and it certainly must not report success onto its schedule"
    );
}

/// Steps are never left behind without their run (hazard 3). They are only reachable
/// through the run table, so an orphan is unreachable forever.
#[tokio::test]
async fn steps_never_outlive_their_workflow_run() {
    let h = failures_only_workflow().await;
    finish_step(&h, "first").await;
    finish_step(&h, "second").await;

    let orphans = h
        .count(
            "SELECT VALUE count() FROM _00_step_run \
             WHERE workflow_run NOT IN (SELECT VALUE id FROM _00_workflow_run) GROUP ALL",
        )
        .await;
    assert_eq!(orphans, 0, "a step with no run can never be found again");
}

/// A dependant still receives its input even though the upstream step's JOB row is
/// destined for deletion — the output is copied onto the step row when the step
/// finalizes, one pass before anything is discarded (hazard 5).
#[tokio::test]
async fn a_dependant_still_gets_its_input_under_failures_only() {
    let h = failures_only_workflow().await;
    finish_step(&h, "first").await;

    assert_eq!(h.step_status("second").await.as_deref(), Some("dispatched"));
    let payload = h
        .one_string(
            "SELECT VALUE <string>payload.steps.first.from FROM ONLY job \
             WHERE path = '/second' LIMIT 1",
        )
        .await;
    assert_eq!(
        payload.as_deref(),
        Some("first"),
        "the second step was dispatched with the first step's captured output"
    );
}

/// A `kind: job` schedule discards its spawned job row along with the run, and a heal
/// sweep afterwards must not mislabel anything as `job_missing` (hazard 6).
#[tokio::test]
async fn a_discarded_job_row_is_not_mistaken_for_a_missing_one() {
    let h = failures_only_harness().await;
    let job = h
        .one_string("SELECT VALUE job_id FROM ONLY _00_schedule_run WHERE true LIMIT 1")
        .await
        .expect("a job was spawned");
    h.finish_job(&job, "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.job_ids().await, Vec::<String>::new(), "the job row went with the run");

    // A further sweep has nothing running to heal, so nothing can be failed.
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.healed, 0);
    assert_eq!(
        h.one_string("SELECT VALUE last_run_status FROM ONLY _00_schedule:lean").await.as_deref(),
        Some("success"),
        "the outcome recorded before the delete still stands"
    );
}


// --- operator actions: rerun, retry, cancel ---------------------------------

use crate::workflow::WorkflowOpError;

/// Drive the diamond to a halted failure: `extract-orders` fails, `extract-users`
/// succeeds, the rest is skipped, the run is `failed`.
async fn failed_diamond() -> (Harness, ids::Ref) {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;
    h.fail_job(&h.step_job("extract-orders").await.unwrap(), "boom").await;
    h.finish_job(&h.step_job("extract-users").await.unwrap(), "success", json!({"fileId": "u"}))
        .await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("failed"));
    (h, run)
}

async fn schedule_run_status(h: &Harness) -> Option<String> {
    h.one_string("SELECT VALUE status FROM ONLY _00_schedule_run WHERE true LIMIT 1").await
}

#[tokio::test]
async fn a_rerun_is_a_fresh_ad_hoc_run_of_the_same_workflow() {
    let (h, source) = failed_diamond().await;
    let source_error = h.run_field(&source, "error.step").await;

    let out = h.engine.rerun_workflow(&source, Utc::now()).await.unwrap();

    assert_ne!(out.run, source);
    assert_eq!(out.rerun_of, source);
    assert_eq!(h.run_status(&out.run).await.as_deref(), Some("running"));
    assert_eq!(h.run_field(&out.run, "rerun_of").await, Some(source.as_string()));
    assert_eq!(h.run_field(&out.run, "trigger").await.as_deref(), Some("manual"));
    assert_eq!(
        h.run_field(&out.run, "schedule_name").await.as_deref(),
        Some("NONE"),
        "a rerun has no owning schedule"
    );
    assert_eq!(
        h.run_field(&out.run, "dag").await,
        h.run_field(&source, "dag").await,
        "the frozen dag is copied verbatim"
    );
    // Roots dispatched under the NEW key, so the old failed job row is untouched.
    for step in ["extract-orders", "extract-users"] {
        assert_eq!(h.step_status_of(&out.run, step).await.as_deref(), Some("dispatched"));
        let job = h.step_job_of(&out.run, step).await.unwrap();
        assert!(job.contains(&out.run.key), "job {job} belongs to the rerun");
    }
    assert_eq!(h.step_status_of(&out.run, "transform").await.as_deref(), Some("blocked"));
    // The source is exactly as it was.
    assert_eq!(h.run_status(&source).await.as_deref(), Some("failed"));
    assert_eq!(h.run_field(&source, "error.step").await, source_error);
}

#[tokio::test]
async fn a_rerun_never_touches_the_schedule_run_and_rolls_up_as_ad_hoc() {
    let mut def = diamond_workflow();
    def["history_mode"] = json!("failures-only");
    let h = workflow_harness(def).await;
    let source = workflow_run_id(&h).await;
    h.fail_job(&h.step_job("extract-orders").await.unwrap(), "boom").await;
    h.finish_job(&h.step_job("extract-users").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(schedule_run_status(&h).await.as_deref(), Some("failed"));

    let out = h.engine.rerun_workflow(&source, Utc::now()).await.unwrap();
    for step in ["extract-orders", "extract-users"] {
        h.finish_job(&h.step_job_of(&out.run, step).await.unwrap(), "success", json!({})).await;
    }
    h.engine.tick_pass().await.unwrap();
    h.finish_job(&h.step_job_of(&out.run, "transform").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    for step in ["notify", "archive"] {
        h.finish_job(&h.step_job_of(&out.run, step).await.unwrap(), "success", json!({})).await;
    }
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        schedule_run_status(&h).await.as_deref(),
        Some("failed"),
        "the original fire's outcome is not rewritten by a rerun"
    );
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await,
        1,
        "no second schedule run was minted"
    );
    assert!(
        h.run_status(&out.run).await.is_none(),
        "under failures-only a successful rerun is discarded like any other success"
    );
    assert!(
        rollup_rows(&h).await.iter().any(|r| r.starts_with("schedule/(ad-hoc) s=1")),
        "and counted under the ad-hoc bucket: {:?}",
        rollup_rows(&h).await
    );
}

#[tokio::test]
async fn a_rerun_of_a_run_whose_frozen_dag_is_broken_is_refused() {
    let (h, source) = failed_diamond().await;
    h.set(&format!(
        "UPDATE {source} SET dag = {{ steps: [ \
           {{ name: 'a', path: '/a', depends_on: ['b'] }}, \
           {{ name: 'b', path: '/b', depends_on: ['a'] }} ] }}"
    ))
    .await;

    let err = h.engine.rerun_workflow(&source, Utc::now()).await.unwrap_err();
    assert!(matches!(err, WorkflowOpError::BadDag(_)), "got {err:?}");
    assert_eq!(h.count("SELECT VALUE count() FROM _00_workflow_run GROUP ALL").await, 1);
}

#[tokio::test]
async fn two_reruns_in_the_same_millisecond_collide_instead_of_double_spawning() {
    let (h, source) = failed_diamond().await;
    let at = Utc::now();
    h.engine.rerun_workflow(&source, at).await.unwrap();
    let err = h.engine.rerun_workflow(&source, at).await.unwrap_err();
    assert!(matches!(err, WorkflowOpError::AlreadyExists), "got {err:?}");
    assert_eq!(h.count("SELECT VALUE count() FROM _00_workflow_run GROUP ALL").await, 2);
}

#[tokio::test]
async fn a_retry_resumes_from_the_failure_and_keeps_what_succeeded() {
    let (h, run) = failed_diamond().await;
    let users_job = h.step_job_of(&run, "extract-users").await.unwrap();
    let old_orders_job = h.step_job_of(&run, "extract-orders").await.unwrap();

    let out = h.engine.retry_workflow(&run).await.unwrap();

    assert_eq!(out.retry_count, 1);
    let mut reset = out.reset.clone();
    reset.sort();
    assert_eq!(reset, vec!["archive", "extract-orders", "notify", "transform"]);
    assert_eq!(out.kept, vec!["extract-users"]);

    assert_eq!(h.run_status(&run).await.as_deref(), Some("running"));
    assert_eq!(h.run_field(&run, "error").await.as_deref(), Some("NONE"));
    assert_eq!(h.run_field(&run, "retry_count").await.as_deref(), Some("1"));
    assert_eq!(schedule_run_status(&h).await.as_deref(), Some("running"), "the owning fire reopens");

    // The success is untouched, output and all.
    assert_eq!(h.step_status_of(&run, "extract-users").await.as_deref(), Some("success"));
    assert_eq!(h.step_job_of(&run, "extract-users").await.as_deref(), Some(users_job.as_str()));
    assert_eq!(
        h.one_string("SELECT VALUE output.fileId FROM ONLY _00_step_run WHERE step = 'extract-users' LIMIT 1").await.as_deref(),
        Some("u")
    );
    // The failure re-dispatched under a fresh attempt id; the old row is history.
    assert_eq!(h.step_status_of(&run, "extract-orders").await.as_deref(), Some("dispatched"));
    let new_orders_job = h.step_job_of(&run, "extract-orders").await.unwrap();
    assert_ne!(new_orders_job, old_orders_job);
    assert!(new_orders_job.ends_with("_r1"), "got {new_orders_job}");
    assert!(h.job_ids().await.contains(&old_orders_job), "the failed row is kept as history");
    // The skipped join is back to waiting, not re-skipped.
    assert_eq!(h.step_status_of(&run, "transform").await.as_deref(), Some("blocked"));
}

#[tokio::test]
async fn a_retried_run_finishes_and_mirrors_onto_its_schedule_run() {
    let (h, run) = failed_diamond().await;
    h.engine.retry_workflow(&run).await.unwrap();

    h.finish_job(&h.step_job_of(&run, "extract-orders").await.unwrap(), "success", json!({"fileId": "o"})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.step_status_of(&run, "transform").await.as_deref(), Some("dispatched"));
    let transform_job = h.step_job_of(&run, "transform").await.unwrap();
    let users: Option<String> = h
        .raw
        .query("SELECT VALUE payload.steps.`extract-users`.fileId FROM ONLY type::record($id)")
        .bind(("id", transform_job.clone()))
        .await
        .expect("query")
        .take(0)
        .expect("take");
    assert_eq!(users.as_deref(), Some("u"), "the kept step's output still feeds the join");

    h.finish_job(&transform_job, "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    for step in ["notify", "archive"] {
        h.finish_job(&h.step_job_of(&run, step).await.unwrap(), "success", json!({})).await;
    }
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.run_status(&run).await.as_deref(), Some("success"));
    assert_eq!(schedule_run_status(&h).await.as_deref(), Some("success"));
}

#[tokio::test]
async fn a_killed_run_cannot_be_retried_until_its_jobs_have_settled() {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;
    h.set(&format!("UPDATE {run} SET kill_requested = true")).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("killed"));

    // RecordingKill never terminalizes the job rows, so both roots are still pending.
    let err = h.engine.retry_workflow(&run).await.unwrap_err();
    assert!(matches!(err, WorkflowOpError::Settling { .. }), "got {err:?}");
    assert_eq!(h.run_status(&run).await.as_deref(), Some("killed"), "nothing was written");

    // Once the SSP has processed the kill, the retry goes through.
    for step in ["extract-orders", "extract-users"] {
        h.fail_job(&h.step_job_of(&run, step).await.unwrap(), "killed by operator").await;
    }
    let out = h.engine.retry_workflow(&run).await.unwrap();
    assert!(out.reset.contains(&"extract-orders".to_string()));
    assert_eq!(h.run_status(&run).await.as_deref(), Some("running"));
    assert_eq!(h.run_field(&run, "kill_requested").await.as_deref(), Some("false"));

    // A further sweep must not re-kill it.
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("running"));
    assert_eq!(h.step_status_of(&run, "extract-orders").await.as_deref(), Some("dispatched"));
}

#[tokio::test]
async fn only_a_failed_or_killed_run_can_be_retried() {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;
    let err = h.engine.retry_workflow(&run).await.unwrap_err();
    assert!(matches!(err, WorkflowOpError::NotTerminal { .. }), "got {err:?}");

    for step in ["extract-orders", "extract-users"] {
        h.finish_job(&h.step_job(step).await.unwrap(), "success", json!({})).await;
    }
    h.engine.tick_pass().await.unwrap();
    h.finish_job(&h.step_job("transform").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    for step in ["notify", "archive"] {
        h.finish_job(&h.step_job(step).await.unwrap(), "success", json!({})).await;
    }
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("success"));
    let err = h.engine.retry_workflow(&run).await.unwrap_err();
    assert!(matches!(err, WorkflowOpError::NotTerminal { .. }), "got {err:?}");

    let missing = ids::workflow_run("no_such_run");
    assert!(matches!(
        h.engine.retry_workflow(&missing).await.unwrap_err(),
        WorkflowOpError::NotFound(_)
    ));
}

#[tokio::test]
async fn a_retry_racing_a_sweep_still_dispatches_each_step_once() {
    let (h, run) = failed_diamond().await;
    let rival = h.rival();

    let (a, b) = tokio::join!(h.engine.retry_workflow(&run), rival.tick_pass());
    a.unwrap();
    b.unwrap();
    h.engine.tick_pass().await.unwrap();

    assert_eq!(
        h.count("SELECT VALUE count() FROM job WHERE path = '/exportOrders' GROUP ALL").await,
        2,
        "the original attempt plus exactly one retry attempt"
    );
    assert_eq!(h.run_status(&run).await.as_deref(), Some("running"));
}

#[tokio::test]
async fn a_second_retry_mints_the_next_attempt_id() {
    let (h, run) = failed_diamond().await;
    h.engine.retry_workflow(&run).await.unwrap();
    h.fail_job(&h.step_job_of(&run, "extract-orders").await.unwrap(), "boom again").await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("failed"));

    let out = h.engine.retry_workflow(&run).await.unwrap();
    assert_eq!(out.retry_count, 2);
    let job = h.step_job_of(&run, "extract-orders").await.unwrap();
    assert!(job.ends_with("_r2"), "got {job}");
    assert_eq!(h.count("SELECT VALUE count() FROM job WHERE path = '/exportOrders' GROUP ALL").await, 3);
}

#[tokio::test]
async fn a_continue_independent_retry_resets_only_the_doomed_branch() {
    let h = workflow_harness(json!({
        "kind": "workflow",
        "every_ms": 3_600_000,
        "target_table": "job",
        "workflow": {
            "target_table": "job",
            "steps": [
                {"name": "extract-orders", "path": "/a"},
                {"name": "transform", "path": "/b", "depends_on": ["extract-orders"]},
                {"name": "independent", "path": "/c"},
                {"name": "after-independent", "path": "/d", "depends_on": ["independent"]},
            ],
            "on_failure": "continue-independent",
        },
    }))
    .await;
    let run = workflow_run_id(&h).await;
    h.fail_job(&h.step_job("extract-orders").await.unwrap(), "boom").await;
    h.finish_job(&h.step_job("independent").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    h.finish_job(&h.step_job("after-independent").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("failed"));

    let out = h.engine.retry_workflow(&run).await.unwrap();
    let mut reset = out.reset.clone();
    reset.sort();
    assert_eq!(reset, vec!["extract-orders", "transform"]);
    let mut kept = out.kept.clone();
    kept.sort();
    assert_eq!(kept, vec!["after-independent", "independent"]);
    assert_eq!(h.step_status_of(&run, "after-independent").await.as_deref(), Some("success"));
    assert_eq!(h.step_status_of(&run, "extract-orders").await.as_deref(), Some("dispatched"));
}

#[tokio::test]
async fn a_retry_that_succeeds_under_failures_only_leaves_no_rows() {
    let mut def = diamond_workflow();
    def["history_mode"] = json!("failures-only");
    let h = workflow_harness(def).await;
    let run = workflow_run_id(&h).await;
    h.fail_job(&h.step_job("extract-orders").await.unwrap(), "boom").await;
    h.finish_job(&h.step_job("extract-users").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("failed"), "a failure is kept");

    h.engine.retry_workflow(&run).await.unwrap();
    h.finish_job(&h.step_job_of(&run, "extract-orders").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    h.finish_job(&h.step_job_of(&run, "transform").await.unwrap(), "success", json!({})).await;
    h.engine.tick_pass().await.unwrap();
    for step in ["notify", "archive"] {
        h.finish_job(&h.step_job_of(&run, step).await.unwrap(), "success", json!({})).await;
    }
    h.engine.tick_pass().await.unwrap();

    assert_eq!(h.count("SELECT VALUE count() FROM _00_workflow_run GROUP ALL").await, 0);
    assert_eq!(h.count("SELECT VALUE count() FROM _00_step_run GROUP ALL").await, 0);
    assert_eq!(h.count("SELECT VALUE count() FROM _00_schedule_run GROUP ALL").await, 0);
}

#[tokio::test]
async fn cancel_kills_the_in_flight_jobs_without_waiting_for_a_sweep() {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;
    let roots = [
        h.step_job("extract-orders").await.unwrap(),
        h.step_job("extract-users").await.unwrap(),
    ];

    let out = h.engine.cancel_workflow(&run).await.unwrap();

    assert_eq!(out.status, "killed");
    assert_eq!(h.run_status(&run).await.as_deref(), Some("killed"));
    let killed = h.kill.killed.lock().unwrap().clone();
    for job in roots {
        assert!(killed.contains(&job), "{job} should have been killed immediately");
    }
    assert_eq!(h.step_status_of(&run, "transform").await.as_deref(), Some("skipped"));
}

#[tokio::test]
async fn only_a_running_run_can_be_cancelled() {
    let (h, run) = failed_diamond().await;
    let err = h.engine.cancel_workflow(&run).await.unwrap_err();
    assert!(matches!(err, WorkflowOpError::NotRunning { .. }), "got {err:?}");
    assert!(matches!(
        h.engine.cancel_workflow(&ids::workflow_run("no_such_run")).await.unwrap_err(),
        WorkflowOpError::NotFound(_)
    ));
}

/// A run written before `retry_count` existed carries NONE in it. SurrealDB
/// re-validates the whole row on every UPDATE, so with a bare `int` field that
/// row could never be cancelled, killed, or finalized again ("Expected `int`
/// but found `NONE`"). The field is `option<int>` precisely so those rows stay
/// writable; readers treat NONE as 0.
#[tokio::test]
async fn a_run_from_before_retry_count_existed_can_still_be_cancelled_and_finalized() {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;
    // Make the row look like it predates the field.
    h.set(&format!(
        "UPDATE {} UNSET retry_count;",
        ids::Ref::new(run.table.clone(), run.key.clone())
    ))
    .await;
    assert_eq!(h.run_field(&run, "retry_count").await.as_deref(), Some("NONE"));

    let out = h.engine.cancel_workflow(&run).await.unwrap();
    assert_eq!(out.status, "killed");
    assert_eq!(h.run_status(&run).await.as_deref(), Some("killed"));

    // The sweep still writes the row without tripping validation.
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("killed"));
}

#[tokio::test]
async fn retrying_a_run_from_before_retry_count_existed_counts_from_zero() {
    let (h, run) = failed_diamond().await;
    h.set(&format!(
        "UPDATE {} UNSET retry_count;",
        ids::Ref::new(run.table.clone(), run.key.clone())
    ))
    .await;
    assert_eq!(h.run_field(&run, "retry_count").await.as_deref(), Some("NONE"));

    let out = h.engine.retry_workflow(&run).await.unwrap();
    assert_eq!(out.retry_count, 1, "NONE reads as 0, so the first retry is attempt 1");
    assert_eq!(h.run_field(&run, "retry_count").await.as_deref(), Some("1"));
    assert_eq!(h.run_status(&run).await.as_deref(), Some("running"));
}

// --- nothing stays stuck forever -------------------------------------------
//
// Every test below is a regression for a shape that, before this, held a run
// `running` indefinitely — and with the default `concurrency: skip` that means it
// held its fan-out key indefinitely too, so the schedule silently stopped doing
// any work while every healthcheck stayed green.

/// A step that won its `blocked -> ready` promotion and then failed to dispatch is
/// unreachable by every other path in the engine: `ready_steps` yields only
/// `blocked` steps and `CLAIM_STEP_READY` guards on `blocked`. This is the shape
/// that sat untouched for four weeks in production.
#[tokio::test]
async fn a_step_stranded_at_ready_is_re_dispatched() {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;

    // Rewind one root to the state a pass that died between the promotion and the
    // job stamp leaves behind, and drop the job it had created.
    let job = h.step_job_of(&run, "extract-orders").await.expect("dispatched");
    h.set(&format!("DELETE {job}")).await;
    h.set(
        "UPDATE _00_step_run SET status = 'ready', job_id = NONE \
         WHERE step = 'extract-orders'",
    )
    .await;
    assert_eq!(h.step_status_of(&run, "extract-orders").await.as_deref(), Some("ready"));
    assert_eq!(h.step_job_of(&run, "extract-orders").await, None);

    h.engine.advance_workflow(&run).await.unwrap();

    assert_eq!(
        h.step_status_of(&run, "extract-orders").await.as_deref(),
        Some("dispatched"),
        "the stranded step is recovered rather than left behind"
    );
    let recovered = h.step_job_of(&run, "extract-orders").await.expect("a job again");
    assert_eq!(recovered, job, "the deterministic id means recovery reuses it");
    assert!(h.job_ids().await.contains(&recovered), "and the job row is really there");
}

/// The same, one state later: `dispatched` with no `job_id`. Step 1 of the
/// advancement needs a job id to land anything, so this used to be skipped on
/// every pass forever and `all_terminal` was never reached.
#[tokio::test]
async fn a_step_stranded_at_dispatched_without_a_job_is_re_dispatched() {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;
    let job = h.step_job_of(&run, "extract-users").await.expect("dispatched");
    h.set(&format!("DELETE {job}")).await;
    h.set("UPDATE _00_step_run SET job_id = NONE WHERE step = 'extract-users'").await;

    h.engine.advance_workflow(&run).await.unwrap();

    assert_eq!(h.step_job_of(&run, "extract-users").await.as_deref(), Some(job.as_str()));
    assert_eq!(h.step_status_of(&run, "extract-users").await.as_deref(), Some("dispatched"));
}

/// Recovery is bounded. A step whose dispatch fails the same way every pass must
/// fail the run rather than be retried every five seconds until the deployment
/// ends. The dispatch here is impossible to satisfy: the step names an outbox
/// table that does not exist.
#[tokio::test]
async fn a_step_that_cannot_be_dispatched_gives_up_and_fails_the_run() {
    let h = harness().await;
    // A CREATE against an UNKNOWN table succeeds on a non-strict database — it just
    // defines a schemaless one. So the step is pointed at a table that exists and
    // rejects the job instead: SCHEMAFULL with a required field nothing writes.
    h.set(
        "DEFINE TABLE OVERWRITE hostile SCHEMAFULL; \
         DEFINE FIELD OVERWRITE required_by_nobody ON TABLE hostile TYPE string",
    )
    .await;
    h.define_schedule(
        "report",
        json!({
            "kind": "workflow",
            "every_ms": 3_600_000,
            "workflow": {
                "steps": [{"name": "only", "path": "/x", "table": "hostile"}],
                "on_failure": "halt",
            },
        }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:report SET next_fire_at = time::now() - 1s").await;
    h.engine.tick_pass().await.unwrap();
    let run = workflow_run_id(&h).await;

    // The job CREATE is rejected every time, so the step stays `ready` with no job
    // — exactly the stranded shape, but a permanent one.
    for _ in 0..6 {
        // The dispatch error propagates, which is fine: the sweep logs it and the
        // attempt has already been charged.
        let _ = h.engine.advance_workflow(&run).await;
    }

    assert_eq!(
        h.step_status_of(&run, "only").await.as_deref(),
        Some("failed"),
        "the engine gives up on the step instead of retrying it forever"
    );
    let error = h.step_error(&run, "only").await.expect("a reason");
    assert_eq!(error.get("code").and_then(Value::as_str), Some("lost_dispatch"));
    assert_eq!(
        h.run_status(&run).await.as_deref(),
        Some("failed"),
        "and the run terminalizes rather than staying `running`"
    );
}

/// A fire whose workflow-run skeleton cannot be written has already created the
/// schedule run that gates it. Leaving that row `running` holds the key forever,
/// so the spawn failure has to terminalize BOTH rows.
#[tokio::test]
async fn a_fire_that_cannot_write_its_steps_fails_instead_of_holding_the_key() {
    let h = harness().await;
    h.define_schedule(
        "report",
        json!({
            "kind": "workflow",
            "every_ms": 3_600_000,
            "target_table": "job",
            "workflow": { "steps": [{"name": "only", "path": "/x"}], "on_failure": "halt" },
        }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    // Make the step CREATE impossible. `_00_step_run` is SCHEMAFULL, so a required
    // field the engine does not write rejects every CREATE against it.
    h.set("DEFINE FIELD required_by_nobody ON TABLE _00_step_run TYPE string").await;
    h.set("UPDATE _00_schedule:report SET next_fire_at = time::now() - 1s").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.errored, 1, "the fire is reported as an error, not as a success");

    let run = workflow_run_id(&h).await;
    assert_eq!(
        h.run_status(&run).await.as_deref(),
        Some("failed"),
        "the workflow run does not stay `running` with no steps"
    );
    assert_eq!(
        h.count("SELECT VALUE count() FROM _00_schedule_run WHERE status = 'running' GROUP ALL")
            .await,
        0,
        "and neither does the schedule run that holds the concurrency key"
    );
    let sched_run = ids::schedule_run(&run.key);
    assert_eq!(
        h.run_error(&sched_run).await.and_then(|e| e.get("code").and_then(Value::as_str).map(str::to_string)),
        Some("spawn_failed".to_string()),
        "and it says why"
    );
}

/// The backstop. Every other mechanism needs something to observe; a clock does
/// not. Here the step's job row is left `processing` forever — no terminal status
/// to heal against, nothing missing to notice.
#[tokio::test]
async fn a_workflow_run_past_its_deadline_is_reaped() {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;
    h.set("UPDATE job SET status = 'processing'").await;

    // Nothing to reap yet: the default ceiling is an hour.
    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.reaped, 0);
    assert_eq!(h.run_status(&run).await.as_deref(), Some("running"));

    h.age_workflow_runs("2h").await;
    let report = h.engine.tick_pass().await.unwrap();

    assert_eq!(report.reaped, 1);
    assert_eq!(h.run_status(&run).await.as_deref(), Some("failed"));
    let error = h.run_error(&run).await.expect("a reason");
    assert_eq!(error.get("code").and_then(Value::as_str), Some("deadline_exceeded"));
    assert!(
        !h.kill.killed.lock().unwrap().is_empty(),
        "a reaped run's in-flight step jobs are killed, or reaping it achieved nothing"
    );
    // `failed`, not `killed`: nobody killed this, and it must not read as if
    // someone had.
    assert_ne!(h.run_status(&run).await.as_deref(), Some("killed"));
    assert_eq!(
        h.run_status(&ids::schedule_run(&run.key)).await.as_deref(),
        Some("failed"),
        "the owning schedule run releases the key too"
    );
}

/// A per-schedule `deadline:` overrides the project default in both directions,
/// and it is frozen onto the run so editing the schedule cannot move the ceiling
/// of a run already in flight.
#[tokio::test]
async fn a_schedule_deadline_overrides_the_project_default() {
    let h = harness().await;
    h.define_schedule(
        "sync",
        json!({
            "kind": "job",
            "every_ms": 300_000,
            "target_table": "job",
            "path": "/sync",
            "deadline_secs": 60,
        }),
    )
    .await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:sync SET next_fire_at = time::now() - 1s").await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE job SET status = 'processing'").await;

    // Ten minutes old: inside the 1h project default, past this schedule's 60s.
    h.set("UPDATE _00_schedule_run SET fire_at = time::now() - 10m").await;
    // Raising the schedule's deadline now must not save the in-flight run: the
    // ceiling travelled with it at spawn.
    h.set("UPDATE _00_schedule:sync SET deadline_secs = 86400").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.reaped, 1, "the frozen 60s ceiling applies, not the schedule's new one");
    let run = h
        .one_string("SELECT VALUE type::string(id) FROM ONLY _00_schedule_run WHERE true LIMIT 1")
        .await
        .expect("a run");
    let run = ids::Ref::parse(&run).expect("well-formed");
    assert_eq!(h.run_status(&run).await.as_deref(), Some("failed"));
    assert_eq!(
        h.run_error(&run).await.and_then(|e| e.get("deadline_secs").and_then(Value::as_i64)),
        Some(60),
    );
}

/// `run_deadline_secs = 0` is an explicit opt-out and must be honoured — it is the
/// behaviour every deployment had before the reaper existed.
#[tokio::test]
async fn the_reaper_can_be_turned_off() {
    let h = workflow_harness(diamond_workflow()).await;
    let run = workflow_run_id(&h).await;
    h.set("UPSERT _00_retention:default MERGE { run_deadline_secs: 0 }").await;
    h.age_workflow_runs("100d").await;

    let report = h.engine.tick_pass().await.unwrap();
    assert_eq!(report.reaped, 0);
    assert_eq!(h.run_status(&run).await.as_deref(), Some("running"));
}

/// Healing runs before reaping, so a run whose job finished a moment before the
/// deadline is reported by its real outcome and not by the clock.
#[tokio::test]
async fn healing_beats_the_reaper_to_a_run_that_actually_finished() {
    let h = workflow_harness(single_step_workflow()).await;
    let run = workflow_run_id(&h).await;
    let job = h.step_job_of(&run, "only").await.expect("dispatched");
    h.finish_job(&job, "success", json!({"ok": true})).await;
    h.age_workflow_runs("2h").await;

    let report = h.engine.tick_pass().await.unwrap();

    assert_eq!(report.reaped, 0, "there was nothing left to reap");
    assert_eq!(
        h.run_status(&run).await.as_deref(),
        Some("success"),
        "a run that finished must never be relabelled by the reaper"
    );
}

/// A job that fails without recording an error used to leave the step `failed`
/// with `error = NONE` — a failure with no reason, which is exactly what an
/// operator comes looking for. The runner's append is best-effort by necessity, so
/// the engine synthesizes the shape instead.
#[tokio::test]
async fn a_step_whose_job_lost_its_error_still_says_why_it_failed() {
    let h = workflow_harness(single_step_workflow()).await;
    let run = workflow_run_id(&h).await;
    let job = h.step_job_of(&run, "only").await.expect("dispatched");

    // Terminally failed, `errors` empty — the append the SSP could not make.
    h.set(&format!("UPDATE {job} SET status = 'failed', errors = [], retries = 3")).await;
    h.engine.advance_workflow(&run).await.unwrap();

    let error = h.step_error(&run, "only").await.expect("a step failure always has a reason");
    assert_eq!(error.get("code").and_then(Value::as_str), Some("job_failed"));
    let reason = error.get("reason").and_then(Value::as_str).unwrap_or_default();
    assert!(reason.contains(&job), "the reason names the job: {reason}");
    assert!(reason.contains("3 of 3 attempts"), "and its attempt count: {reason}");
    assert_eq!(error.get("retries").and_then(Value::as_i64), Some(3));
}

/// The real error still wins when there is one.
#[tokio::test]
async fn a_recorded_job_error_is_preferred_over_the_synthesized_one() {
    let h = workflow_harness(single_step_workflow()).await;
    let run = workflow_run_id(&h).await;
    let job = h.step_job_of(&run, "only").await.expect("dispatched");
    h.fail_job(&job, "backend said no").await;
    h.engine.advance_workflow(&run).await.unwrap();

    let error = h.step_error(&run, "only").await.expect("a reason");
    assert_eq!(error.get("reason").and_then(Value::as_str), Some("backend said no"));
    assert_eq!(error.get("code").and_then(Value::as_i64), Some(500));
}

/// A run that throws on every advancement must not take the sweep with it.
///
/// This is the failure that made every other guarantee moot: `heal_workflow_runs`
/// propagated one run's error, which aborted `tick_pass` — so `reap_pass` and
/// `prune_pass` never ran, and the one run nobody could advance blocked the very
/// machinery meant to clear it, on every sweep, forever.
#[tokio::test]
async fn a_run_that_cannot_advance_does_not_abort_the_sweep() {
    let h = harness().await;
    h.set(
        "DEFINE TABLE OVERWRITE hostile SCHEMAFULL; \
         DEFINE FIELD OVERWRITE required_by_nobody ON TABLE hostile TYPE string",
    )
    .await;
    h.define_schedule(
        "doomed",
        json!({
            "kind": "workflow",
            "every_ms": 3_600_000,
            "workflow": { "steps": [{"name": "only", "path": "/x", "table": "hostile"}],
                          "on_failure": "halt" },
        }),
    )
    .await;
    h.define_schedule("healthy", every_5m_job()).await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:doomed SET next_fire_at = time::now() - 1s").await;
    h.engine.tick_pass().await.unwrap();

    // The doomed run is now `running` with a step nothing can dispatch.
    h.set("UPDATE _00_schedule:healthy SET next_fire_at = time::now() - 1s").await;
    let report = h.engine.tick_pass().await.expect("the sweep survives an unadvanceable run");

    assert!(report.errored > 0, "the failure is counted, not hidden");
    assert_eq!(report.fired, 1, "and the healthy schedule still fires");
    assert_eq!(
        h.count("SELECT VALUE count() FROM job WHERE true GROUP ALL").await,
        1,
        "the healthy schedule's job is really spawned"
    );
}

fn single_step_workflow() -> Value {
    json!({
        "kind": "workflow",
        "every_ms": 3_600_000,
        "target_table": "job",
        "workflow": { "steps": [{"name": "only", "path": "/x"}], "on_failure": "halt" },
    })
}

/// A workflow must still dispatch when the deployment's schema is OLDER than the
/// engine running it.
///
/// This is not hypothetical: it is what whitepawn ran into on 2026-09-04. A
/// `spky deploy --upgrade` replaces the SurrealDB container, the internal-schema batch
/// raced that restart and failed, and the images rolled anyway — so the engine was
/// writing `dispatch_attempts` to a SCHEMAFULL `_00_step_run` that had no such field.
/// Every promotion was rejected and no workflow could dispatch a single step, silently.
///
/// The fields are dropped here to stand in for that state. A step must still reach
/// `dispatched` and land its job; only the two newer bookkeeping fields may be lost.
#[tokio::test]
async fn a_workflow_dispatches_against_a_schema_that_predates_the_engine() {
    let h = harness().await;
    // Roll `_00_step_run` back to before the lease/attempt work, keeping it SCHEMAFULL
    // so a statement naming a missing field is rejected exactly as it would be live.
    h.set("REMOVE FIELD dispatch_attempts ON TABLE _00_step_run").await;
    h.set("REMOVE FIELD started_at ON TABLE _00_step_run").await;

    h.define_schedule("report", single_step_workflow()).await;
    h.engine.tick_pass().await.unwrap();
    h.set("UPDATE _00_schedule:report SET next_fire_at = time::now() - 1s").await;
    h.engine.tick_pass().await.unwrap();

    let run = workflow_run_id(&h).await;
    assert_eq!(
        h.step_status_of(&run, "only").await.as_deref(),
        Some("dispatched"),
        "the step must dispatch even though the schema is missing newer fields"
    );
    let job = h.step_job_of(&run, "only").await.expect("the step holds its job id");
    assert!(h.job_ids().await.contains(&job), "and the job row really exists");

    // And the run still completes end to end.
    h.finish_job(&job, "success", json!({"ok": true})).await;
    h.engine.tick_pass().await.unwrap();
    assert_eq!(h.run_status(&run).await.as_deref(), Some("success"));
}
