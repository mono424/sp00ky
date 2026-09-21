//! Close-to-e2e engine tests: a real embedded SurrealDB, the REAL pool DDL
//! (`include_str!`'d from `apps/cli/src/pool_tables.surql`), an outbox table kept
//! faithful to what `spky add api` generates, a fake provider with failure
//! injection, and agents simulated by calling the engine's agent entry points the
//! way the scheduler's pool listener does.
//!
//! Time is never slept through. A timeout is "reached" by moving the row's own
//! timestamps into the past, which exercises exactly the predicates the engine
//! evaluates against the database clock.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pool_protocol::{Command, JobRef, Outcome, PollRequest, ResultRequest};
use schedule_core::{ScheduleDb, ScheduleDbError};
use serde_json::{json, Value};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;

use crate::engine::{PoolEngine, PoolEngineConfig};
use crate::provider::{CreateMachine, MachineProvider, ProviderError, ProviderMachine};

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
        let mut response = q
            .await
            .map_err(|e| ScheduleDbError::Transport(e.to_string()))?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value = response
                .take(i)
                .map_err(|e| ScheduleDbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
}

/// An in-memory cloud. `create` is idempotent on the machine id, as the port
/// demands; failures are switched on per verb.
#[derive(Default)]
struct FakeProvider {
    machines: Mutex<BTreeMap<String, String>>, // machine_id -> provider_id
    next: AtomicUsize,
    create_calls: AtomicUsize,
    fail_create: AtomicBool,
    fail_destroy: AtomicBool,
}

impl FakeProvider {
    fn ids(&self) -> BTreeSet<String> {
        self.machines.lock().unwrap().keys().cloned().collect()
    }
    fn count(&self) -> usize {
        self.machines.lock().unwrap().len()
    }
    /// A machine the engine knows nothing about (a leak from another life).
    fn plant(&self, machine_id: &str) {
        self.machines
            .lock()
            .unwrap()
            .insert(machine_id.into(), "leaked".into());
    }
}

#[async_trait::async_trait]
impl MachineProvider for FakeProvider {
    async fn create(&self, req: &CreateMachine) -> Result<ProviderMachine, ProviderError> {
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_create.load(Ordering::SeqCst) {
            return Err(ProviderError::Refused("out of capacity".into()));
        }
        let mut machines = self.machines.lock().unwrap();
        let provider_id = machines
            .entry(req.machine_id.clone())
            .or_insert_with(|| format!("vm-{}", self.next.fetch_add(1, Ordering::SeqCst)))
            .clone();
        Ok(ProviderMachine {
            provider_id,
            machine_id: req.machine_id.clone(),
        })
    }

    async fn destroy(
        &self,
        machine_id: &str,
        _provider_id: Option<&str>,
    ) -> Result<(), ProviderError> {
        if self.fail_destroy.load(Ordering::SeqCst) {
            return Err(ProviderError::Transient("api 503".into()));
        }
        self.machines.lock().unwrap().remove(machine_id);
        Ok(())
    }

    async fn list(&self, _pool: &str) -> Result<Vec<ProviderMachine>, ProviderError> {
        Ok(self
            .machines
            .lock()
            .unwrap()
            .iter()
            .map(|(m, p)| ProviderMachine {
                provider_id: p.clone(),
                machine_id: m.clone(),
            })
            .collect())
    }
}

// --- harness ----------------------------------------------------------------

const POOL_TABLES: &str = include_str!("../../../apps/cli/src/pool_tables.surql");

/// Faithful to `apps/cli/src/add_api.rs::outbox_template` plus the platform
/// fields `schema_builder` injects. If you change the template, change this.
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
    engine: PoolEngine,
    raw: Arc<Surreal<MemEngine>>,
    cloud: Arc<FakeProvider>,
}

async fn harness() -> Harness {
    harness_with(PoolEngineConfig {
        orphan_sweep_every: 1,
        ..Default::default()
    })
    .await
}

async fn harness_with(cfg: PoolEngineConfig) -> Harness {
    let db = Surreal::new::<Mem>(()).await.expect("start mem db");
    db.use_ns("test").use_db("test").await.expect("use ns/db");
    let raw = Arc::new(db);
    raw.query(POOL_TABLES)
        .await
        .expect("apply pool schema")
        .check()
        .expect("pool schema is valid");
    raw.query(OUTBOX_DDL)
        .await
        .expect("apply outbox schema")
        .check()
        .expect("outbox schema is valid");

    let cloud = Arc::new(FakeProvider::default());
    let mut providers: BTreeMap<String, Arc<dyn MachineProvider>> = BTreeMap::new();
    providers.insert("docker".into(), cloud.clone());
    let engine = PoolEngine::new(Arc::new(MemDb(raw.clone())), providers, cfg);
    Harness { engine, raw, cloud }
}

impl Harness {
    async fn sql(&self, surql: &str) -> Vec<Value> {
        let mut response = self.raw.query(surql).await.expect("query");
        let n = response.num_statements();
        (0..n)
            .map(|i| {
                let v: surrealdb::types::Value = response.take(i).expect("statement");
                v.into_json_value()
            })
            .collect()
    }

    async fn rows(&self, surql: &str) -> Vec<Value> {
        match self.sql(surql).await.into_iter().next() {
            Some(Value::Array(rows)) => rows,
            Some(Value::Null) | None => vec![],
            Some(other) => vec![other],
        }
    }

    /// A pool named `render` serving table `job`. `extra` overrides spec fields.
    async fn pool(&self, extra: Value) {
        let mut spec = json!({
            "name": "render", "provider": "docker", "slots": 1, "min": 0, "autoscale": true,
            "max": 4, "buffer": 0, "idle_timeout_secs": 600, "lease_secs": 90,
            "boot_timeout_secs": 300, "backend": "renderer", "target_table": "job",
            "container": { "image": "renderer:1", "port": 8080, "healthcheck": "/health",
                           "env": { "MODE": "test" } },
            "spec_hash": "h1",
        });
        for (k, v) in extra.as_object().cloned().unwrap_or_default() {
            // `null` here means "leave it out": an absent key is NONE, whereas a
            // bound JSON null is NULL, which an option<int> field rejects.
            if v.is_null() {
                spec.as_object_mut().unwrap().remove(&k);
            } else {
                spec[k] = v;
            }
        }
        self.raw
            // MERGE, like deploy: spec fields only, so a re-deploy never wipes the
            // operator's `paused` or the engine's breaker fields.
            .query("UPSERT _00_pool:render MERGE $spec")
            .bind(("spec", spec))
            .await
            .expect("upsert pool")
            .check()
            .expect("pool row is valid");
    }

    async fn job(&self, name: &str) -> String {
        self.raw
            .query(format!(
                "CREATE job:{name} SET path = '/render', payload = '{{\"n\":\"{name}\"}}'"
            ))
            .await
            .expect("create job")
            .check()
            .expect("job row is valid");
        format!("job:{name}")
    }

    async fn tick(&self) -> crate::TickReport {
        self.engine.tick_pass().await.expect("tick")
    }

    async fn machines(&self, state: &str) -> Vec<String> {
        self.rows(&format!(
            "SELECT id, created_at FROM _00_machine WHERE state = '{state}' ORDER BY created_at ASC"
        ))
        .await
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect()
    }

    async fn state_of(&self, machine: &str) -> String {
        self.rows(&format!("SELECT state FROM {machine}")).await[0]["state"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn job_row(&self, job: &str) -> Value {
        self.rows(&format!("SELECT * FROM {job}")).await.remove(0)
    }

    /// Boot every machine that is waiting for its agent, like agents would.
    async fn boot_all(&self) -> Vec<String> {
        let mut up = self.machines("requested").await;
        up.extend(self.machines("booting").await);
        for m in &up {
            assert!(
                self.engine.on_hello(m).await.unwrap().is_some(),
                "hello refused for {m}"
            );
            assert!(
                self.engine.on_ready(m).await.unwrap(),
                "ready refused for {m}"
            );
        }
        up
    }

    async fn poll(&self, machine: &str, running: &[JobRef]) -> Vec<Command> {
        self.engine
            .on_poll(&PollRequest {
                machine: machine.into(),
                running: running.to_vec(),
                stats: None,
            })
            .await
            .expect("poll")
            .commands
    }

    async fn result(&self, machine: &str, job: &JobRef, outcome: Outcome) -> bool {
        self.engine
            .on_result(&ResultRequest {
                machine: machine.into(),
                job: job.job.clone(),
                epoch: job.epoch,
                outcome,
            })
            .await
            .expect("result")
            .accepted
    }

    /// Move a machine's clocks into the past.
    async fn age_machine(&self, machine: &str, secs: i64) {
        self.sql(&format!(
            "UPDATE {machine} SET created_at = created_at - {secs}s, \
             last_seen = IF last_seen != NONE {{ last_seen - {secs}s }} ELSE {{ NONE }}, \
             idle_since = IF idle_since != NONE {{ idle_since - {secs}s }} ELSE {{ NONE }}"
        ))
        .await;
    }
}

fn assigns(commands: &[Command]) -> Vec<JobRef> {
    commands
        .iter()
        .filter_map(|c| match c {
            Command::Assign { job, epoch, .. } => Some(JobRef {
                job: job.clone(),
                epoch: *epoch,
            }),
            _ => None,
        })
        .collect()
}

fn ok() -> Outcome {
    Outcome::Success {
        body: "{\"done\":true}".into(),
    }
}

// --- the DDL and the statements agree ------------------------------------------

#[tokio::test]
async fn the_shipped_ddl_applies_and_a_pool_row_round_trips() {
    let h = harness().await;
    h.pool(json!({})).await;
    let report = h.tick().await;
    assert_eq!(
        report.errored, 0,
        "a well-formed pool must sweep cleanly: {report:?}"
    );
}

// --- the three modes ------------------------------------------------------------

#[tokio::test]
async fn fixed_size_pool_keeps_min_machines_and_queues_the_rest() {
    let h = harness().await;
    h.pool(json!({ "autoscale": false, "min": 2, "max": null }))
        .await;
    for n in ["a", "b", "c"] {
        h.job(n).await;
    }

    assert_eq!(
        h.tick().await.created,
        2,
        "exactly the fixed size, whatever is queued"
    );
    let up = h.boot_all().await;
    assert_eq!(up.len(), 2);
    for m in &up {
        h.poll(m, &[]).await; // freshly polled machines may take jobs
    }

    let report = h.tick().await;
    assert_eq!(
        (report.assigned, report.created),
        (2, 0),
        "two run, the third waits, nothing grows"
    );
    assert_eq!(h.job_row("job:c").await["status"], "pending");

    // One finishes: the waiting job takes the freed machine on the next pass.
    let first = assigns(&h.poll(&up[0], &[]).await);
    assert_eq!(first.len(), 1);
    assert!(h.result(&up[0], &first[0], ok()).await);
    assert_eq!(h.tick().await.assigned, 1);
    assert_eq!(h.job_row("job:c").await["status"], "processing");
    assert_eq!(h.cloud.count(), 2, "still exactly two machines");
}

#[tokio::test]
async fn baseline_with_no_buffer_spawns_for_a_waiting_job_and_shrinks_back() {
    let h = harness().await;
    h.pool(json!({ "min": 0, "buffer": 0, "max": 3 })).await;
    assert_eq!(
        h.tick().await.created,
        0,
        "scale to zero: nothing to do, nothing running"
    );

    let job = h.job("a").await;
    assert_eq!(h.tick().await.created, 1, "the waiting job is the demand");
    assert_eq!(
        h.tick().await.created,
        0,
        "a booting machine already counts as supply"
    );

    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    assert_eq!(h.tick().await.assigned, 1);

    let attempt = assigns(&h.poll(&m, &[]).await).remove(0);
    assert_eq!(attempt.job, job);
    assert!(h.result(&m, &attempt, ok()).await);
    let row = h.job_row(&job).await;
    assert_eq!(row["status"], "success");
    assert_eq!(
        row["result"],
        json!({ "done": true }),
        "the backend's body is captured, parsed"
    );

    // Idle, but not for long enough: it stays as free warm capacity.
    assert_eq!(h.tick().await.drained, 0);
    h.age_machine(&m, 3600).await;
    h.poll(&m, &[]).await; // still alive, just idle
    let report = h.tick().await;
    assert_eq!(
        (report.drained, report.destroyed),
        (1, 1),
        "idle past the timeout: drained and destroyed"
    );
    assert_eq!(h.cloud.count(), 0);
    assert_eq!(h.state_of(&m).await, "gone");
}

#[tokio::test]
async fn buffer_keeps_ready_machines_above_usage_and_refills_behind_a_job() {
    let h = harness().await;
    h.pool(json!({ "min": 0, "buffer": 2, "max": 5 })).await;
    assert_eq!(
        h.tick().await.created,
        2,
        "two warm machines before any job exists"
    );
    for m in h.boot_all().await {
        h.poll(&m, &[]).await;
    }

    h.job("a").await;
    let report = h.tick().await;
    assert_eq!(report.assigned, 1, "a warm machine takes the job at once");
    assert_eq!(
        report.created, 1,
        "and the buffer is refilled behind it: 1 busy + 2 warm"
    );
    assert_eq!(h.cloud.count(), 3);
}

#[tokio::test]
async fn max_is_never_exceeded_however_much_is_queued() {
    let h = harness().await;
    h.pool(json!({ "min": 0, "buffer": 1, "max": 3 })).await;
    for n in 0..20 {
        h.job(&format!("j{n}")).await;
    }
    for _ in 0..5 {
        h.tick().await;
        for m in h.boot_all().await {
            h.poll(&m, &[]).await;
        }
    }
    assert_eq!(h.cloud.count(), 3, "max: 3");
    assert_eq!(
        h.rows("SELECT id FROM job WHERE status = 'processing'")
            .await
            .len(),
        3
    );
}

#[tokio::test]
async fn creation_ramps_instead_of_stampeding_the_provider() {
    let h = harness_with(PoolEngineConfig {
        create_cap_per_tick: 2,
        ..Default::default()
    })
    .await;
    h.pool(json!({ "min": 0, "buffer": 0, "max": 10 })).await;
    for n in 0..8 {
        h.job(&format!("j{n}")).await;
    }
    assert_eq!(h.tick().await.created, 2);
    assert_eq!(h.tick().await.created, 2);
}

#[tokio::test]
async fn slots_pack_several_jobs_onto_one_machine() {
    let h = harness().await;
    h.pool(json!({ "slots": 3, "min": 1, "buffer": 0, "max": 4 }))
        .await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    for n in ["a", "b", "c", "d"] {
        h.job(n).await;
    }
    let report = h.tick().await;
    assert_eq!(report.assigned, 3, "three slots, three jobs");
    assert_eq!(report.created, 1, "the fourth needs a second machine");
    assert_eq!(assigns(&h.poll(&m, &[]).await).len(), 3);
    assert_eq!(
        h.rows(&format!("SELECT busy_slots FROM {m}")).await[0]["busy_slots"],
        3
    );
}

// --- the agent conversation -------------------------------------------------------

#[tokio::test]
async fn hello_carries_everything_the_agent_needs_to_run_the_backend() {
    let h = harness().await;
    h.pool(json!({ "min": 1, "lease_secs": 90 })).await;
    h.tick().await;
    let m = h.machines("booting").await.remove(0);
    let hello = h
        .engine
        .on_hello(&m)
        .await
        .unwrap()
        .expect("a live machine is welcome");
    assert_eq!(hello.port, 8080);
    assert_eq!(hello.healthcheck.as_deref(), Some("/health"));
    assert_eq!(hello.env.get("MODE").map(String::as_str), Some("test"));
    assert!(hello.recycle_per_job);
    assert!(hello.lease_secs - hello.stop_margin_secs < hello.lease_secs);
    assert!(
        h.engine
            .on_hello("_00_machine:nope")
            .await
            .unwrap()
            .is_none(),
        "unknown machine"
    );
}

#[tokio::test]
async fn poll_replies_are_derived_from_rows_so_a_lost_reply_is_simply_repeated() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    h.job("a").await;
    h.tick().await;

    let first = h.poll(&m, &[]).await;
    let again = h.poll(&m, &[]).await;
    assert_eq!(
        first, again,
        "nothing was queued in memory: the same state yields the same command"
    );
    match &first[0] {
        Command::Assign {
            path,
            payload,
            deadline_secs,
            ..
        } => {
            assert_eq!(path, "/render");
            assert_eq!(
                payload,
                &json!({ "n": "a" }),
                "string payloads are decoded for the backend"
            );
            assert_eq!(*deadline_secs, 28_800);
        }
        other => panic!("expected an assignment, got {other:?}"),
    }

    // Once the agent reports it running there is nothing more to say, and the
    // poll has renewed the lease.
    let attempt = assigns(&first).remove(0);
    h.sql("UPDATE job:a SET lease_until = time::now() + 1s")
        .await;
    assert!(h.poll(&m, &[attempt.clone()]).await.is_empty());
    let renewed = h
        .rows("SELECT lease_until > time::now() + 30s AS renewed FROM job:a")
        .await;
    assert_eq!(
        renewed[0]["renewed"], true,
        "polling is what keeps a long job's lease alive"
    );
}

#[tokio::test]
async fn a_job_is_only_handed_to_a_machine_that_is_actually_polling() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.job("a").await;

    h.age_machine(&m, 60).await; // ready, but has not polled for a minute
    assert_eq!(
        h.tick().await.assigned,
        0,
        "a silent machine gets nothing new"
    );
    h.poll(&m, &[]).await;
    assert_eq!(h.tick().await.assigned, 1);
}

// --- fencing and failure ---------------------------------------------------------

#[tokio::test]
async fn a_result_from_an_attempt_that_lost_its_lease_is_refused() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    let job = h.job("a").await;
    h.tick().await;
    let stale = assigns(&h.poll(&m, &[]).await).remove(0);

    // The lease runs out (a partition, say); the engine takes the job back.
    h.sql("UPDATE job:a SET lease_until = time::now() - 1s")
        .await;
    assert_eq!(h.tick().await.requeued, 1);
    let row = h.job_row(&job).await;
    assert_eq!(
        (row["retries"].clone(), row["errors"][0]["code"].clone()),
        (json!(1), json!("lease_expired"))
    );

    assert!(
        !h.result(&m, &stale, ok()).await,
        "the old attempt may not write the outcome"
    );
    assert_ne!(h.job_row(&job).await["status"], "success");

    // It is re-assigned under a new epoch; the agent is told to drop the old one.
    let commands = h.poll(&m, &[stale.clone()]).await;
    assert!(commands.contains(&Command::Cancel {
        job: stale.job.clone(),
        epoch: stale.epoch
    }));
    let fresh = assigns(&commands);
    assert_eq!(fresh.len(), 1);
    assert_eq!(
        fresh[0].epoch,
        stale.epoch + 2,
        "reclaim and re-claim each bump the epoch"
    );
    assert!(h.result(&m, &fresh[0], ok()).await);
    assert_eq!(h.job_row(&job).await["status"], "success");
}

#[tokio::test]
async fn a_result_from_the_wrong_machine_is_refused() {
    let h = harness().await;
    h.pool(json!({ "min": 2 })).await;
    h.tick().await;
    let up = h.boot_all().await;
    for m in &up {
        h.poll(m, &[]).await;
    }
    h.job("a").await;
    h.tick().await;
    let (owner, other) = if assigns(&h.poll(&up[0], &[]).await).is_empty() {
        (&up[1], &up[0])
    } else {
        (&up[0], &up[1])
    };
    let attempt = assigns(&h.poll(owner, &[]).await).remove(0);
    assert!(!h.result(other, &attempt, ok()).await);
    assert!(h.result(owner, &attempt, ok()).await);
}

#[tokio::test]
async fn a_failed_attempt_is_retried_until_the_budget_runs_out() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    let job = h.job("a").await;
    let boom = || Outcome::Failed {
        code: json!(500),
        reason: "boom".into(),
    };

    for attempt_no in 1..=3 {
        h.poll(&m, &[]).await;
        h.tick().await;
        let attempt = assigns(&h.poll(&m, &[]).await).remove(0);
        assert!(h.result(&m, &attempt, boom()).await);
        let row = h.job_row(&job).await;
        assert_eq!(row["retries"], attempt_no);
        let expected = if attempt_no < 3 { "pending" } else { "failed" };
        assert_eq!(row["status"], expected, "after attempt {attempt_no}");
    }
    assert_eq!(h.job_row(&job).await["errors"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn cancelled_and_overdue_attempts_are_terminal_not_retried() {
    let h = harness().await;
    h.pool(json!({ "min": 1, "slots": 2 })).await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    h.job("a").await;
    h.job("b").await;
    h.tick().await;
    let attempts = assigns(&h.poll(&m, &[]).await);
    assert!(h.result(&m, &attempts[0], Outcome::Cancelled).await);
    assert!(h.result(&m, &attempts[1], Outcome::DeadlineExceeded).await);
    for a in &attempts {
        let row = h.job_row(&a.job).await;
        assert_eq!(
            (row["status"].clone(), row["retries"].clone()),
            (json!("failed"), json!(0))
        );
    }
}

#[tokio::test]
async fn an_operator_kill_fences_the_attempt_and_the_agent_is_told_to_cancel() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    let job = h.job("a").await;
    h.tick().await;
    let attempt = assigns(&h.poll(&m, &[]).await).remove(0);

    assert!(h.engine.kill_job(&job).await.unwrap());
    assert_eq!(
        h.poll(&m, &[attempt.clone()]).await,
        vec![Command::Cancel {
            job: attempt.job.clone(),
            epoch: attempt.epoch
        }]
    );
    assert!(
        !h.result(&m, &attempt, ok()).await,
        "a killed job stays killed"
    );
    assert_eq!(h.job_row(&job).await["errors"][0]["code"], "killed");
}

#[tokio::test]
async fn the_engine_fails_an_attempt_the_agent_let_run_past_the_hard_limit() {
    let h = harness().await;
    h.pool(json!({ "min": 1, "max_job_duration_secs": 60 }))
        .await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    let job = h.job("a").await;
    h.tick().await;
    let attempt = assigns(&h.poll(&m, &[]).await).remove(0);

    // Still polling, still renewing - but it started over an hour ago.
    h.sql("UPDATE job:a SET updated_at = time::now() - 1h")
        .await;
    h.poll(&m, &[attempt.clone()]).await;
    assert_eq!(h.tick().await.failed_jobs, 1);
    assert_eq!(h.job_row(&job).await["errors"][0]["code"], "deadline");
}

// --- machines that go wrong -------------------------------------------------------

#[tokio::test]
async fn a_machine_that_stops_polling_is_destroyed_and_its_job_moves_on() {
    let h = harness().await;
    h.pool(json!({ "min": 1, "lease_secs": 90 })).await;
    h.tick().await;
    let dead = h.boot_all().await.remove(0);
    h.poll(&dead, &[]).await;
    let job = h.job("a").await;
    h.tick().await;
    assert_eq!(h.job_row(&job).await["assignee"], json!(dead));

    // The machine dies: no polls, so no lease renewals either.
    h.age_machine(&dead, 600).await;
    h.sql("UPDATE job:a SET lease_until = time::now() - 1s")
        .await;
    let report = h.tick().await;
    assert_eq!((report.lost, report.requeued, report.destroyed), (1, 1, 1));
    assert_eq!(
        h.state_of(&dead).await,
        "failed",
        "a lost machine is filed as a failure"
    );
    assert_eq!(
        report.created, 1,
        "and the pool replaces it in the same pass"
    );
    assert!(!h.cloud.ids().contains(&dead));

    let replacement = h.boot_all().await.remove(0);
    h.poll(&replacement, &[]).await;
    assert_eq!(h.tick().await.assigned, 1);
    assert_eq!(h.job_row(&job).await["assignee"], json!(replacement));
    assert_eq!(
        h.poll(&dead, &[]).await,
        vec![Command::Shutdown],
        "a zombie that wakes up is sent home"
    );
}

#[tokio::test]
async fn a_job_that_keeps_killing_machines_runs_out_of_budget() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    let job = h.job("poison").await;
    for _ in 0..3 {
        h.tick().await;
        for m in h.boot_all().await {
            h.poll(&m, &[]).await;
        }
        h.tick().await;
        h.sql("UPDATE job:poison SET lease_until = time::now() - 1s")
            .await;
        for m in h.machines("ready").await {
            h.age_machine(&m, 600).await;
        }
    }
    h.tick().await;
    let row = h.job_row(&job).await;
    assert_eq!(
        (row["status"].clone(), row["retries"].clone()),
        (json!("failed"), json!(3))
    );
}

#[tokio::test]
async fn machines_that_never_boot_open_the_breaker_and_a_good_boot_closes_it() {
    let h = harness().await;
    h.pool(json!({ "min": 1, "boot_timeout_secs": 300 })).await;

    let mut opened = Vec::new();
    for round in 1..=3 {
        assert_eq!(h.tick().await.created, 1, "round {round}");
        let m = h.machines("booting").await.remove(0);
        h.age_machine(&m, 301).await;
        // The reaping pass also wants to replace the machine; whether it may is
        // exactly what the breaker decides.
        let report = h.tick().await;
        assert_eq!(report.boot_timeouts, 1);
        opened.extend(report.transitions);
        for stuck in h.machines("booting").await {
            h.sql(&format!("UPDATE {stuck} SET state = 'gone'")).await; // keep rounds independent
        }
    }
    assert_eq!(opened.len(), 1, "reported once, when it opens: {opened:?}");
    assert!(opened[0].open && opened[0].failures == 3);

    assert_eq!(
        h.tick().await.created,
        0,
        "open breaker: a broken image must not burn money in a loop"
    );

    // The pause elapses, one machine makes it: the streak is forgotten.
    h.sql("UPDATE _00_pool:render SET breaker_until = time::now() - 1s")
        .await;
    assert_eq!(h.tick().await.created, 1);
    h.boot_all().await;
    let pool = h
        .rows("SELECT boot_failures, breaker_until FROM _00_pool:render")
        .await
        .remove(0);
    assert_eq!(pool["boot_failures"], 0);
    assert!(pool["breaker_until"].is_null());
}

#[tokio::test]
async fn a_provider_that_refuses_creates_is_recorded_and_backs_off() {
    let h = harness().await;
    h.pool(json!({ "min": 0, "max": 5 })).await;
    for n in 0..5 {
        h.job(&format!("j{n}")).await;
    }
    h.cloud.fail_create.store(true, Ordering::SeqCst);

    let report = h.tick().await;
    assert_eq!(report.created, 0);
    assert_eq!(
        report.create_failed, 3,
        "stops asking the moment the breaker opens"
    );
    assert_eq!(report.transitions.len(), 1);
    assert_eq!(
        h.machines("requested").await.len(),
        0,
        "failed creates do not linger as phantom supply"
    );
    assert_eq!(
        h.tick().await.create_failed,
        0,
        "breaker open: the provider is left alone"
    );
    assert_eq!(
        h.rows("SELECT id FROM job WHERE status = 'pending'")
            .await
            .len(),
        5,
        "and no job is lost"
    );
}

#[tokio::test]
async fn a_crash_between_the_row_and_the_provider_call_makes_one_machine_not_two() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    // What a crash leaves behind: the row exists, the provider was never asked.
    h.sql("CREATE _00_machine:crashed SET pool = 'render', provider = 'docker', slots = 1, spec_hash = 'h1'").await;

    assert_eq!(
        h.tick().await.created,
        0,
        "the requested row already counts as supply"
    );
    assert_eq!(
        h.cloud.count(),
        0,
        "too fresh to tell a crash from a create in flight"
    );
    h.age_machine("_00_machine:crashed", 30).await;
    h.tick().await;
    assert_eq!(
        h.cloud.ids(),
        BTreeSet::from(["_00_machine:crashed".to_string()])
    );
    assert_eq!(h.state_of("_00_machine:crashed").await, "booting");
    h.tick().await;
    assert_eq!(
        h.cloud.count(),
        1,
        "same id, same machine: create is idempotent"
    );
}

#[tokio::test]
async fn a_failing_destroy_is_retried_and_cannot_turn_into_a_runaway_bill() {
    let h = harness().await;
    h.pool(json!({ "min": 0, "max": 2, "buffer": 0 })).await;
    h.cloud.fail_destroy.store(true, Ordering::SeqCst);

    // Machines keep dying on boot and cannot be destroyed: they pile up.
    for _ in 0..12 {
        h.job("x").await.clear();
        h.sql("UPDATE _00_pool:render SET breaker_until = NONE, boot_failures = 0")
            .await;
        h.tick().await;
        for m in h.machines("booting").await {
            h.age_machine(&m, 301).await;
        }
        h.sql("DELETE job").await;
    }
    assert!(
        h.cloud.count() <= 5,
        "hard stop at 2x ceiling + 1, got {}",
        h.cloud.count()
    );
    assert!(!h.machines("terminating").await.is_empty());

    h.cloud.fail_destroy.store(false, Ordering::SeqCst);
    h.tick().await;
    assert_eq!(
        h.cloud.count(),
        0,
        "every owed destroy is retried until it sticks"
    );
    assert!(h.machines("terminating").await.is_empty());
}

#[tokio::test]
async fn leaked_provider_machines_are_destroyed_but_a_live_one_never_is() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    h.tick().await;
    let live = h.boot_all().await.remove(0);

    h.cloud.plant("_00_machine:from_another_life"); // no row at all
    h.sql("CREATE _00_machine:old SET pool = 'render', provider = 'docker', slots = 1, spec_hash = 'h1', state = 'gone'").await;
    h.cloud.plant("_00_machine:old"); // row exists but is terminal

    h.poll(&live, &[]).await;
    assert_eq!(h.tick().await.orphans_destroyed, 2);
    assert_eq!(h.cloud.ids(), BTreeSet::from([live]));
}

// --- deploys and operators --------------------------------------------------------

#[tokio::test]
async fn a_deploy_rolls_idle_machines_now_and_busy_ones_when_they_finish() {
    let h = harness().await;
    h.pool(json!({ "autoscale": false, "min": 2, "max": null }))
        .await;
    h.tick().await;
    let old = h.boot_all().await;
    for m in &old {
        h.poll(m, &[]).await;
    }
    h.job("long").await;
    h.tick().await;
    let (busy, idle) = if assigns(&h.poll(&old[0], &[]).await).is_empty() {
        (old[1].clone(), old[0].clone())
    } else {
        (old[0].clone(), old[1].clone())
    };
    let attempt = assigns(&h.poll(&busy, &[]).await).remove(0);

    h.pool(json!({ "autoscale": false, "min": 2, "max": null, "spec_hash": "h2" }))
        .await;
    let report = h.tick().await;
    assert_eq!(report.drained, 2, "both leave the supply");
    assert_eq!(report.destroyed, 1, "the idle one goes at once");
    assert_eq!(
        report.created, 2,
        "and the fixed size is restored on the new deploy"
    );
    assert_eq!(h.state_of(&idle).await, "gone");
    assert_eq!(
        h.state_of(&busy).await,
        "draining",
        "a running job is never cut short by a deploy"
    );

    // New work only lands on the new deploy.
    let new = h.boot_all().await;
    for m in &new {
        h.poll(m, &[]).await;
    }
    let job = h.job("after").await;
    h.tick().await;
    assert!(new.contains(
        &h.job_row(&job).await["assignee"]
            .as_str()
            .unwrap()
            .to_string()
    ));

    assert!(h.result(&busy, &attempt, ok()).await);
    assert_eq!(h.poll(&busy, &[]).await, vec![Command::Shutdown]);
    assert_eq!(h.tick().await.destroyed, 1);
}

#[tokio::test]
async fn a_paused_pool_assigns_and_spawns_nothing_but_lets_running_jobs_finish() {
    let h = harness().await;
    h.pool(json!({ "min": 1, "max": 4 })).await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    h.job("running").await;
    h.tick().await;
    let attempt = assigns(&h.poll(&m, &[]).await).remove(0);

    h.sql("UPDATE _00_pool:render SET paused = true").await;
    h.job("waiting").await;
    let report = h.tick().await;
    assert_eq!((report.assigned, report.created), (0, 0));
    assert!(
        h.result(&m, &attempt, ok()).await,
        "pausing never breaks a job in flight"
    );

    h.sql("UPDATE _00_pool:render SET paused = false").await;
    h.poll(&m, &[]).await;
    assert_eq!(h.tick().await.assigned, 1);
}

#[tokio::test]
async fn a_machine_past_its_lifetime_is_replaced_without_dropping_its_job() {
    let h = harness().await;
    h.pool(json!({ "min": 1, "max_lifetime_secs": 3600 })).await;
    h.tick().await;
    let m = h.boot_all().await.remove(0);
    h.poll(&m, &[]).await;
    h.job("a").await;
    h.tick().await;
    let attempt = assigns(&h.poll(&m, &[]).await).remove(0);

    h.sql(&format!("UPDATE {m} SET created_at = created_at - 2h"))
        .await;
    h.poll(&m, &[attempt.clone()]).await;
    let report = h.tick().await;
    assert_eq!(
        (report.drained, report.created, report.destroyed),
        (1, 1, 0)
    );
    assert_eq!(h.job_row(&attempt.job).await["status"], "processing");
}

#[tokio::test]
async fn one_broken_pool_does_not_stop_the_others() {
    let h = harness().await;
    h.pool(json!({ "min": 1 })).await;
    h.sql(
        "UPSERT _00_pool:broken CONTENT { name: 'broken', provider: 'hetzner', min: 1, \
         backend: 'b', target_table: 'job', spec_hash: 'x' }",
    )
    .await;
    let report = h.tick().await;
    assert_eq!(
        report.errored, 1,
        "no `hetzner` provider is configured in this harness"
    );
    assert_eq!(report.created, 1, "the healthy pool still got its machine");
    let err = h
        .rows("SELECT last_error FROM _00_pool:broken")
        .await
        .remove(0);
    assert!(err["last_error"].as_str().unwrap().contains("hetzner"));
}

/// Pool names routinely carry hyphens, which SurrealDB quotes in the record key.
/// Every engine write to a pool must still land.
#[tokio::test]
async fn a_hyphenated_pool_name_works_end_to_end() {
    let h = harness().await;
    h.sql(
        "UPSERT _00_pool:⟨gpu-render⟩ CONTENT { name: 'gpu-render', provider: 'docker', min: 1, \
         boot_timeout_secs: 300, backend: 'b', target_table: 'job', spec_hash: 'x' }",
    )
    .await;
    assert_eq!(h.tick().await.created, 1);
    let m = h.machines("booting").await.remove(0);
    h.age_machine(&m, 301).await;
    assert_eq!(h.tick().await.boot_timeouts, 1);
    let pool = h
        .rows("SELECT boot_failures, last_error FROM _00_pool WHERE name = 'gpu-render'")
        .await
        .remove(0);
    assert_eq!(
        pool["boot_failures"], 1,
        "the failure was recorded on the hyphenated pool"
    );

    let replacement = h.machines("booting").await.remove(0);
    assert!(h.engine.on_hello(&replacement).await.unwrap().is_some());
    assert!(h.engine.on_ready(&replacement).await.unwrap());
    let pool = h
        .rows("SELECT boot_failures FROM _00_pool WHERE name = 'gpu-render'")
        .await
        .remove(0);
    assert_eq!(pool["boot_failures"], 0, "and reset by the first good boot");
}

// --- everything at once ------------------------------------------------------------

/// xorshift: deterministic, dependency-free randomness for the simulation.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// Random jobs, boots, polls, results, machine deaths, provider outages and
/// lease expiries, with the invariants checked after every single step. Then the
/// chaos stops and the pool must settle: every job terminal, exactly the
/// baseline running, nothing leaked.
#[tokio::test]
async fn simulation_holds_every_invariant_under_chaos_and_settles() {
    for seed in [1_u64, 7, 42, 1337] {
        let h = harness().await;
        h.pool(json!({ "min": 1, "buffer": 1, "max": 4, "slots": 2, "idle_timeout_secs": 60 }))
            .await;
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut running: BTreeMap<String, Vec<JobRef>> = BTreeMap::new();
        let mut jobs = 0;

        for step in 0..120 {
            if rng.chance(45) && jobs < 40 {
                h.job(&format!("s{seed}_{jobs}")).await;
                jobs += 1;
            }
            h.cloud.fail_create.store(rng.chance(15), Ordering::SeqCst);
            h.cloud.fail_destroy.store(rng.chance(15), Ordering::SeqCst);
            if rng.chance(10) {
                h.sql("UPDATE _00_pool:render SET breaker_until = NONE, boot_failures = 0")
                    .await;
            }

            if rng.chance(70) {
                h.boot_all().await;
            }
            for m in h
                .machines("ready")
                .await
                .into_iter()
                .chain(h.machines("draining").await)
            {
                if rng.chance(8) {
                    // The machine dies: every attempt bound to it dies with it (also
                    // ones it was assigned but never got to hear about), and with
                    // nobody renewing them their leases run out.
                    h.age_machine(&m, 900).await;
                    running.remove(&m);
                    h.sql(&format!(
                        "UPDATE job SET lease_until = time::now() - 1s \
                         WHERE status = 'processing' AND assignee = '{m}'"
                    ))
                    .await;
                    continue;
                }
                let mine = running.entry(m.clone()).or_default();
                for command in h.poll(&m, mine).await {
                    match command {
                        Command::Assign { job, epoch, .. } => mine.push(JobRef { job, epoch }),
                        Command::Cancel { job, epoch } => {
                            mine.retain(|a| !(a.job == job && a.epoch == epoch))
                        }
                        Command::Shutdown => mine.clear(),
                    }
                }
                if !mine.is_empty() && rng.chance(50) {
                    let attempt = mine.remove(rng.below(mine.len() as u64) as usize);
                    let outcome = if rng.chance(75) {
                        ok()
                    } else {
                        Outcome::Failed {
                            code: json!(500),
                            reason: "sim".into(),
                        }
                    };
                    h.result(&m, &attempt, outcome).await;
                }
            }

            let report = h.tick().await;
            assert_eq!(report.errored, 0, "seed {seed} step {step}: {report:?}");
            check_invariants(&h, 4, 2, &format!("seed {seed} step {step}")).await;
        }

        // Calm: healthy provider, every agent boots, polls and finishes its work.
        h.cloud.fail_create.store(false, Ordering::SeqCst);
        h.cloud.fail_destroy.store(false, Ordering::SeqCst);
        for round in 0..40 {
            h.sql("UPDATE _00_pool:render SET breaker_until = NONE, boot_failures = 0")
                .await;
            h.boot_all().await;
            for m in h
                .machines("ready")
                .await
                .into_iter()
                .chain(h.machines("draining").await)
            {
                let mine = running.entry(m.clone()).or_default();
                for command in h.poll(&m, mine).await {
                    match command {
                        Command::Assign { job, epoch, .. } => mine.push(JobRef { job, epoch }),
                        Command::Cancel { job, epoch } => {
                            mine.retain(|a| !(a.job == job && a.epoch == epoch))
                        }
                        Command::Shutdown => mine.clear(),
                    }
                }
                for attempt in std::mem::take(mine) {
                    h.result(&m, &attempt, ok()).await;
                }
            }
            h.tick().await;
            check_invariants(&h, 4, 2, &format!("seed {seed} calm {round}")).await;
        }

        let open = h
            .rows("SELECT id, status FROM job WHERE status INSIDE ['pending', 'processing']")
            .await;
        assert!(
            open.is_empty(),
            "seed {seed}: every job must end up terminal, left: {open:?}"
        );

        // Idle surplus goes away once it has been idle long enough; min + buffer stay.
        for m in h.machines("ready").await {
            h.sql(&format!("UPDATE {m} SET idle_since = time::now() - 1h"))
                .await;
            h.poll(&m, &[]).await;
        }
        h.tick().await;
        h.tick().await;
        let live = h
            .rows("SELECT id FROM _00_machine WHERE state NOT IN ['gone', 'failed']")
            .await
            .len();
        assert_eq!(
            live, 1,
            "seed {seed}: buffer of one above zero usage, and min is one"
        );
        assert_eq!(
            h.cloud.count(),
            1,
            "seed {seed}: nothing leaked at the provider"
        );
    }
}

async fn check_invariants(h: &Harness, max: usize, slots: i64, at: &str) {
    // 1. Supply never exceeds max.
    let supply = h
        .rows("SELECT id FROM _00_machine WHERE state INSIDE ['requested', 'booting', 'ready']")
        .await
        .len();
    assert!(supply <= max, "{at}: supply {supply} > max {max}");

    // 2. Live machines never exceed the hard stop, even while destroys fail.
    let live = h
        .rows("SELECT id FROM _00_machine WHERE state NOT IN ['gone', 'failed']")
        .await
        .len();
    assert!(live <= 2 * max + 1, "{at}: {live} live machines");

    // 3. Every running attempt is bound to exactly one machine, which exists,
    //    and no machine holds more attempts than it has slots.
    let bound = h
        .rows(
            "SELECT assignee, count() AS n FROM job WHERE status = 'processing' GROUP BY assignee",
        )
        .await;
    for row in &bound {
        let machine = row["assignee"]
            .as_str()
            .unwrap_or_else(|| panic!("{at}: unbound processing job"));
        assert!(
            row["n"].as_i64().unwrap() <= slots,
            "{at}: {machine} is over its slots: {row}"
        );
        assert_eq!(
            h.rows(&format!("SELECT id FROM {machine}")).await.len(),
            1,
            "{at}: {machine} has no row"
        );
    }

    // 4. Nothing at the provider without a row. (Rows whose machine is already
    //    gone at the provider are fine: that is what `terminating` retries fix.)
    for id in h.cloud.ids() {
        assert_eq!(
            h.rows(&format!("SELECT id FROM {id}")).await.len(),
            1,
            "{at}: {id} leaked"
        );
    }

    // 5. A pending job is never still bound.
    let stuck = h
        .rows("SELECT id FROM job WHERE status = 'pending' AND assignee != NONE")
        .await;
    assert!(stuck.is_empty(), "{at}: pending but bound: {stuck:?}");
}
