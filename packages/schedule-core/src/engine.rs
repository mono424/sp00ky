//! The engine: one `tick_pass` per sweep, plus the healing that makes event
//! delivery best-effort instead of load-bearing.
//!
//! ```text
//! tick_pass
//!   ├─ plan_pass      compute next_fire_at for unplanned schedules
//!   ├─ fire_pass      due + operator-triggered schedules → claim → fan out → spawn
//!   ├─ heal_pass      reconcile runs whose job already finished (lost event)
//!   ├─ reap_pass      fail runs that blew their deadline, so none can wedge forever
//!   └─ prune_pass     drop terminal history past the retention window
//! ```
//!
//! Spawning creates a `pending` row in the app's outbox table and stops there:
//! the existing ingest → pickup → `JobRunner` path executes it, so every
//! guarantee that machinery already has (claim/lease, retry backoff, kill,
//! recovery sweeps) applies to scheduled work for free. The engine's own writes
//! are confined to the `_00_schedule*` tables.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Duration, SecondsFormat, Timelike, Utc};
use serde_json::{json, Map, Value};

use crate::db::{count_value, first_row, rows, ScheduleDb, ScheduleDbError};
use crate::kill::JobKill;
use crate::spec::{Concurrency, ScheduleKind, ScheduleSpec, HISTORY_FAILURES_ONLY};
use crate::{ids, sql};

/// Tunables. Defaults are deliberate rather than configurable-by-accident: a
/// host may override them, but nothing reads the environment here (the portable
/// core never does).
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Upper bound on rows a single `forEach` fire may fan out over. A runaway
    /// query should degrade to a logged error, not spawn a hundred thousand jobs.
    pub fanout_max: usize,
    /// How long finished history is kept.
    pub history_max_age: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { fanout_max: 10_000, history_max_age: Duration::days(30) }
    }
}

/// Resolved retention policy for one prune pass.
///
/// Read from `_00_retention:default` (written by deploy) so singlenode, cluster,
/// and Cloudflare hosts all obey the same numbers without another environment
/// variable threaded through three shells. Falls back to [`EngineConfig`] when the
/// row is absent — a fresh database, or a stack running ahead of its CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Retention {
    /// Outbox rows in `success`.
    job_success: Duration,
    /// Outbox rows in `failed`.
    job_failed: Duration,
    /// Run history in `success` / `skipped` / `replaced`.
    run_success: Duration,
    /// Run history in `failed` / `killed`.
    run_failed: Duration,
    /// Outbox tables this pass may sweep. Empty = sweep none.
    job_tables: Vec<String>,
    /// Hard cap on rows in a trimmable status, or `None` when disabled.
    max_rows: Option<usize>,
    /// Default ceiling on one run, for schedules that set no `deadline:` of their
    /// own. `None` when the reaper is disabled (`run_deadline_secs = 0`).
    run_deadline: Option<Duration>,
}

impl Retention {
    /// The window for one run status. Success-shaped statuses share the short
    /// window: with the default `concurrency: skip`, a slow wide fan-out writes one
    /// `skipped` row per item per fire, which is the highest-volume and
    /// lowest-information row the engine produces.
    fn for_run_status(&self, status: &str) -> Duration {
        match status {
            "failed" | "killed" => self.run_failed,
            _ => self.run_success,
        }
    }

    fn for_job_status(&self, status: &str) -> Duration {
        match status {
            "failed" => self.job_failed,
            _ => self.job_success,
        }
    }
}

/// Add the hour bucket [`ScheduleEngine::fold_rollup`] groups by.
///
/// The batched prunes get this from SurrealDB's `time::floor`; a single row folded in
/// Rust (a `failures-only` discard) has to floor it here, and the two must agree or
/// one run's counts land in a bucket of their own.
fn bucketed(row: &Value, at: DateTime<Utc>) -> Value {
    let hour = at.with_minute(0).and_then(|t| t.with_second(0)).and_then(|t| t.with_nanosecond(0));
    let mut out = row.as_object().cloned().unwrap_or_default();
    out.insert("bucket".into(), json!(stamp(hour.unwrap_or(at))));
    Value::Object(out)
}

/// Where a rollup row's `name` comes from.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RollupName<'a> {
    /// Read it off each pruned row (the schedule name).
    Field(&'a str),
    /// Use a constant (the outbox table name).
    Fixed(&'a str),
}

/// Which rollup counter a batch of deleted rows belongs to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Fold<'a> {
    scope: &'a str,
    status: &'a str,
    name: RollupName<'a>,
}

/// How often the prune pass actually runs, independent of the sweep interval.
/// Retention does not need 5-second responsiveness and each prune statement is a
/// scan; firing and healing do need it.
const PRUNE_INTERVAL_SECS: i64 = 60;

/// Rows deleted per prune statement. Small on purpose: on a synced table each
/// deleted row fires the generated `_00_<t>_delete` event, which POSTs the record
/// to `/ingest` — the cost per row is a network round-trip, not a b-tree write.
const PRUNE_BATCH: usize = 500;

/// Statements per (table, status) per pass. With the batch size above this drains
/// 10k rows per target per minute, so any realistic backlog clears in hours
/// without ever spiking the ingest path.
const PRUNE_MAX_BATCHES: usize = 20;

/// Job rows looked up per statement when healing. The ids are interpolated, so this
/// bounds statement length as well as result size.
const HEAL_LOOKUP_BATCH: usize = 500;

/// Fallback run ceiling when `_00_retention` has nothing to say — a fresh database,
/// or a stack deployed ahead of the CLI that writes the row. Matches the DDL default.
const DEFAULT_RUN_DEADLINE_SECS: i64 = 3600;

/// Runs terminalized per reap statement per pass. Each one kills jobs and writes
/// two rows, so it is bounded for the same reason the prune is; a backlog of stuck
/// runs drains over a few sweeps rather than in one spike.
const REAP_BATCH: usize = 200;

/// Run the row cap once every N prune passes. The cap has to COUNT, which is an
/// index walk, and it is a safety valve rather than the main mechanism — the age
/// windows already bound volume at rate x window.
const CAP_EVERY_N_PRUNES: u64 = 10;

/// Statuses the cap may trim. Success-shaped only: a volume cap must never be the
/// reason a failure disappears.
const CAP_TRIMMABLE_RUN_STATUSES: [&str; 3] = ["success", "skipped", "replaced"];
const CAP_TRIMMABLE_JOB_STATUSES: [&str; 1] = ["success"];

/// `_00_run_rollup.scope` values.
pub(crate) const SCOPE_SCHEDULE: &str = "schedule";

/// Bucket name for a run with no owning schedule (`spky workflows trigger`). Counted
/// under a stable placeholder rather than dropped.
pub(crate) const ROLLUP_NAME_AD_HOC: &str = "(ad-hoc)";
pub(crate) const SCOPE_JOB: &str = "job";

/// What one `tick_pass` did. Returned for logging and asserted on in tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    pub planned: usize,
    pub fired: usize,
    pub spawned: usize,
    pub skipped: usize,
    pub replaced: usize,
    pub healed: usize,
    /// Runs terminalized for blowing their deadline.
    pub reaped: usize,
    pub pruned: usize,
    /// Schedules whose plan or fan-out failed. Recorded on the row's
    /// `last_error`; one bad schedule never aborts the sweep.
    pub errored: usize,
}

pub struct ScheduleEngine {
    pub(crate) db: Arc<dyn ScheduleDb>,
    pub(crate) kill: Arc<dyn JobKill>,
    cfg: EngineConfig,
    /// Epoch millis of the last prune, or 0 for "never". Gates the prune pass to
    /// [`PRUNE_INTERVAL_SECS`] without needing a second timer or a mutex.
    last_prune_at: AtomicI64,
    /// Prune passes so far, so the row cap can run on a slow multiple of them.
    prune_passes: AtomicU64,
}

/// How a fire was initiated — recorded on the run row so `spky schedules runs`
/// can distinguish a cron fire from someone hitting `trigger`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Trigger {
    Cron,
    Manual,
}

impl Trigger {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Trigger::Cron => "cron",
            Trigger::Manual => "manual",
        }
    }
}

impl ScheduleEngine {
    pub fn new(db: Arc<dyn ScheduleDb>, kill: Arc<dyn JobKill>, cfg: EngineConfig) -> Self {
        Self { db, kill, cfg, last_prune_at: AtomicI64::new(0), prune_passes: AtomicU64::new(0) }
    }

    pub fn db(&self) -> &Arc<dyn ScheduleDb> {
        &self.db
    }

    /// One sweep. Never returns `Err` for a single bad schedule — those land on
    /// the schedule's `last_error` and are counted in the report; only a
    /// transport-level failure reading the schedule table propagates.
    pub async fn tick_pass(&self) -> anyhow::Result<TickReport> {
        let now = Utc::now();
        let mut report = TickReport::default();

        self.plan_pass(now, &mut report).await?;
        self.fire_pass(now, &mut report).await?;
        self.heal_pass(&mut report).await?;
        self.reap_pass(&mut report).await?;
        self.prune_pass(now, &mut report).await?;

        Ok(report)
    }

    // -- planning -----------------------------------------------------------

    /// Give every unplanned schedule a `next_fire_at`. Runs before firing so a
    /// freshly deployed schedule can fire in the same sweep it was planned in.
    async fn plan_pass(&self, now: DateTime<Utc>, report: &mut TickReport) -> anyhow::Result<()> {
        let unplanned = rows(self.db.query(sql::SELECT_UNPLANNED, &[]).await?);
        for row in unplanned {
            let Some(id) = row_ref(&row) else { continue };
            let spec = match ScheduleSpec::from_row(&row) {
                Ok(spec) => spec,
                Err(e) => {
                    self.record_error(&id, &format!("unreadable schedule row: {e}"), report).await;
                    continue;
                }
            };
            let next = match spec.fire_spec().and_then(|f| f.next_fire_after(now)) {
                Ok(next) => next,
                Err(e) => {
                    self.record_error(&id, &e.to_string(), report).await;
                    continue;
                }
            };
            let mut binds = bind_ref("", &id).to_vec();
            binds.push(("next", json!(stamp(next))));
            match self.db.query(sql::PLAN_NEXT_FIRE, &binds).await {
                Ok(_) => report.planned += 1,
                Err(e) => {
                    tracing::warn!(schedule = %id, error = %e, "could not store next fire time");
                }
            }
        }
        Ok(())
    }

    // -- firing -------------------------------------------------------------

    async fn fire_pass(&self, now: DateTime<Utc>, report: &mut TickReport) -> anyhow::Result<()> {
        // Cron fires: CAS on the fire time we observed, and advance the clock to
        // the next occurrence after NOW — not after the missed slot. Downtime
        // therefore coalesces into a single catch-up fire instead of replaying
        // every slot it slept through.
        for row in rows(self.db.query(sql::SELECT_DUE, &[]).await?) {
            let Some((id, spec)) = self.read_spec(&row, report).await else { continue };
            let Some(observed) = row.get("next_fire_at").and_then(Value::as_str) else { continue };

            let next = match spec.fire_spec().and_then(|f| f.next_fire_after(now)) {
                Ok(next) => next,
                Err(e) => {
                    self.record_error(&id, &e.to_string(), report).await;
                    continue;
                }
            };

            let mut binds = bind_ref("", &id).to_vec();
            binds.extend([
                ("fire", json!(stamp(now))),
                ("next", json!(stamp(next))),
                ("observed", json!(observed)),
            ]);
            let claimed = self.db.query(sql::CLAIM_FIRE, &binds).await?;
            if first_row(claimed).is_none() {
                tracing::debug!(schedule = %id, "fire already claimed elsewhere");
                continue;
            }
            report.fired += 1;
            self.fan_out(&id, &spec, now, Trigger::Cron, report).await;
        }

        // Operator triggers: same spawn path, but the cron clock is untouched so
        // a manual run doesn't shift the schedule.
        for row in rows(self.db.query(sql::SELECT_TRIGGERED, &[]).await?) {
            let Some((id, spec)) = self.read_spec(&row, report).await else { continue };
            let Some(observed) = row.get("trigger_requested_at").and_then(Value::as_str) else {
                continue;
            };

            let mut binds = bind_ref("", &id).to_vec();
            binds.extend([("fire", json!(stamp(now))), ("observed", json!(observed))]);
            let claimed = self.db.query(sql::CLAIM_TRIGGER, &binds).await?;
            if first_row(claimed).is_none() {
                continue;
            }
            report.fired += 1;
            self.fan_out(&id, &spec, now, Trigger::Manual, report).await;
        }

        Ok(())
    }

    /// Resolve the fan-out rows for a fire and spawn one run per row.
    async fn fan_out(
        &self,
        id: &ids::Ref,
        spec: &ScheduleSpec,
        fire_at: DateTime<Utc>,
        trigger: Trigger,
        report: &mut TickReport,
    ) {
        let (items, truncated_from) = match self.fan_out_rows(spec).await {
            Ok(out) => out,
            Err(e) => {
                self.record_error(id, &format!("forEach query failed: {e}"), report).await;
                return;
            }
        };
        // A clipped fan-out is a partial fire, so it belongs on the row an operator
        // reads — not only in the server log.
        if let Some(rows) = truncated_from {
            self.record_error(
                id,
                &format!(
                    "forEach returned {rows} rows, over the fan-out limit of {} — \
                     dropped {} row(s) for this fire",
                    self.cfg.fanout_max,
                    rows - self.cfg.fanout_max
                ),
                report,
            )
            .await;
        }

        // A run's id is derived from (schedule, fire time, key), so two rows that
        // produce the SAME key collide on that id and the second is read as "this
        // fire already happened" — the row is silently dropped. That is a `key:`
        // misconfiguration (a non-unique field, or one the rows don't have, which
        // makes every key ''), and it costs work nobody asked to lose, so it is
        // reported rather than swallowed.
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut duplicates = 0usize;

        for item in items {
            let key = item.as_ref().map(|row| fan_out_key(row, spec.key_field())).unwrap_or_default();
            if !seen.insert(key.clone()) {
                duplicates += 1;
            }
            if let Err(e) = self.spawn_one(id, spec, fire_at, trigger, &key, item, report).await {
                // A single item must never take down the rest of the fan-out.
                tracing::warn!(schedule = %id, key = %key, error = %e, "could not spawn run");
                report.errored += 1;
            }
        }

        if duplicates > 0 {
            let field = spec.key_field();
            self.record_error(
                id,
                &format!(
                    "forEach produced {duplicates} duplicate key(s) for `{field}` — those rows \
                     share a run id and only the first spawned; give `key` a field that is \
                     unique per row"
                ),
                report,
            )
            .await;
        }
    }

    /// `None` items = no fan-out (one run per fire). The second field is the
    /// pre-truncation row count when `fanout_max` clipped the query.
    #[allow(clippy::type_complexity)]
    async fn fan_out_rows(
        &self,
        spec: &ScheduleSpec,
    ) -> Result<(Vec<Option<Value>>, Option<usize>), ScheduleDbError> {
        let Some(query) = spec.for_each.as_deref() else {
            return Ok((vec![None], None));
        };
        let mut items = rows(self.db.query(query, &[]).await?);
        let mut truncated_from = None;
        if items.len() > self.cfg.fanout_max {
            // Truncation is surfaced twice on purpose: silently covering "most" of
            // the fan-out reads as success when it isn't, and a log line alone is
            // invisible to `spky schedules`. The caller records it on the row.
            tracing::error!(
                schedule = %spec.name,
                rows = items.len(),
                limit = self.cfg.fanout_max,
                "forEach returned more rows than the fan-out limit — dropping the excess"
            );
            truncated_from = Some(items.len());
            items.truncate(self.cfg.fanout_max);
        }
        Ok((items.into_iter().map(Some).collect(), truncated_from))
    }

    /// Apply the concurrency policy, then spawn (or record a skip).
    async fn spawn_one(
        &self,
        schedule_id: &ids::Ref,
        spec: &ScheduleSpec,
        fire_at: DateTime<Utc>,
        trigger: Trigger,
        key: &str,
        item: Option<Value>,
        report: &mut TickReport,
    ) -> anyhow::Result<()> {
        let run_key = ids::run_key(&spec.name, fire_at.timestamp_millis(), key);
        let run_ref = ids::schedule_run(&run_key);

        match spec.concurrency {
            Concurrency::Allow => {}
            Concurrency::Skip => {
                if let Some(blocker) = self.blocking_run(&spec.name, key).await? {
                    // Recorded rather than dropped, and recorded WITH ITS CAUSE: an
                    // operator needs to see that ticks are being suppressed, and the
                    // next question is always which run is holding the key. A run
                    // wedged in `running` suppresses every later fire of its key
                    // forever, so the id below is the whole diagnosis.
                    self.create_schedule_run(
                        schedule_id,
                        spec,
                        &run_key,
                        key,
                        fire_at,
                        trigger,
                        "skipped",
                        Some(skip_reason(&blocker)),
                        None,
                        None,
                    )
                    .await?;
                    report.skipped += 1;
                    return Ok(());
                }
            }
            Concurrency::Replace => {
                let replaced = self.replace_active_runs(&spec.name, key).await?;
                report.replaced += replaced;
            }
        }

        match spec.kind {
            ScheduleKind::Job => {
                let Some(table) = spec.target_table.as_deref() else {
                    anyhow::bail!("schedule has no target_table");
                };
                let Some(path) = spec.path.as_deref() else {
                    anyhow::bail!("schedule has no path");
                };
                let job = ids::job(table, &run_key);

                // Run row first: if the job CREATE lands and the process dies
                // before the run row exists, nothing would ever observe the job.
                let created = self
                    .create_schedule_run(
                        schedule_id,
                        spec,
                        &run_key,
                        key,
                        fire_at,
                        trigger,
                        "running",
                        None,
                        Some(&job.as_string()),
                        None,
                    )
                    .await?;
                if !created {
                    tracing::debug!(schedule = %schedule_id, key, "fire already spawned");
                    return Ok(());
                }

                let payload = build_payload(spec.payload.as_ref(), item.as_ref(), None);
                let content = build_job_content(
                    path,
                    payload,
                    spec.max_retries,
                    spec.retry_strategy.as_deref(),
                    spec.timeout,
                );
                let mut binds = bind_ref("", &job).to_vec();
                binds.push(("content", content));
                match self.db.query(sql::CREATE_JOB, &binds).await {
                    Ok(_) => report.spawned += 1,
                    Err(e) if e.is_already_exists() => {
                        tracing::debug!(job = %job, "job row already exists");
                    }
                    Err(e) => {
                        // The run row exists but its job doesn't and never will;
                        // fail it now rather than leaving it `running` forever.
                        self.finalize_schedule_run(
                            &run_ref,
                            "failed",
                            Some(json!({ "code": "spawn_failed", "reason": e.to_string() })),
                        )
                        .await?;
                        return Err(e.into());
                    }
                }
            }
            ScheduleKind::Workflow => {
                let spawned = self
                    .spawn_workflow(schedule_id, spec, &run_key, key, fire_at, trigger, item)
                    .await?;
                if spawned {
                    report.spawned += 1;
                }
            }
        }
        Ok(())
    }

    /// The run already in flight for this (schedule, key), if any. `None` = free.
    async fn blocking_run(&self, name: &str, key: &str) -> Result<Option<Value>, ScheduleDbError> {
        let results = self
            .db
            .query(sql::SELECT_BLOCKING_RUN, &[("name", json!(name)), ("key", json!(key))])
            .await?;
        Ok(first_row(results))
    }

    /// Kill whatever is in flight for this key and mark those runs `replaced`.
    /// Best-effort by nature (the request may already be mid-flight), which is
    /// why the finalize is guarded — a late completion can't relabel the run.
    async fn replace_active_runs(&self, name: &str, key: &str) -> anyhow::Result<usize> {
        let active = rows(
            self.db
                .query(sql::SELECT_ACTIVE_RUNS, &[("name", json!(name)), ("key", json!(key))])
                .await?,
        );
        let mut count = 0;
        for run in active {
            let Some(run_ref) = row_ref(&run) else { continue };
            if let Some(job_id) = run.get("job_id").and_then(Value::as_str) {
                if let Err(e) = self.kill.kill(job_id).await {
                    tracing::warn!(job_id, error = %e, "could not kill replaced job");
                }
            }
            if let Some(wf_run) = run.get("workflow_run").and_then(Value::as_str) {
                if let Some(wf_ref) = ids::Ref::parse(wf_run) {
                    self.request_workflow_kill(&wf_ref).await?;
                }
            }
            self.finalize_schedule_run(&run_ref, "replaced", None).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Create the history row. `false` = this fire already produced one.
    #[allow(clippy::too_many_arguments)]
    /// Create the run row for one fire of one fan-out item.
    ///
    /// Deliberately does NOT store the fan-out item. The row is already on the
    /// spawned job's `payload.row` (job kind) or the workflow run's `input`
    /// (workflow kind), which is where every reader looks; a second copy here
    /// was write-only and, at high fan-out, most of this table's bytes.
    pub(crate) async fn create_schedule_run(
        &self,
        schedule_id: &ids::Ref,
        spec: &ScheduleSpec,
        run_key: &str,
        key: &str,
        fire_at: DateTime<Utc>,
        trigger: Trigger,
        status: &str,
        // Why, for a run born terminal. Mirrors `finalize_schedule_run`: the status
        // and the reason for it are one decision, so they travel together.
        error: Option<Value>,
        job_id: Option<&str>,
        workflow_run: Option<&str>,
    ) -> anyhow::Result<bool> {
        let mut content = Map::new();
        content.insert("schedule_name".into(), json!(spec.name));
        content.insert("key".into(), json!(key));
        content.insert("kind".into(), json!(match spec.kind {
            ScheduleKind::Job => "job",
            ScheduleKind::Workflow => "workflow",
        }));
        content.insert("status".into(), json!(status));
        content.insert("trigger".into(), json!(trigger.as_str()));
        if let Some(error) = error {
            content.insert("error".into(), error);
        }
        // Absent optionals are OMITTED, never nulled: `option<T>` rejects NULL.
        if let Some(job_id) = job_id {
            content.insert("job_id".into(), json!(job_id));
        }
        if let Some(wf) = workflow_run {
            content.insert("workflow_run".into(), json!(wf));
        }

        // `failures-only`: a run that is BORN terminal and went fine is not history
        // anyone asked for, so it is never written at all — no row to create and then
        // prune. Its counts still reach the rollup, and it still reports its outcome
        // onto the schedule, so `spky schedules` is unaffected. This is the case that
        // matters at width: with `concurrency: skip` a slow wide fan-out is mostly
        // suppressed ticks.
        let failures_only = spec.history_mode.as_deref() == Some(HISTORY_FAILURES_ONLY);
        if failures_only && status != "running" && sql::is_success_shaped(status) {
            let row = json!({ "schedule_name": spec.name, "fire_at": stamp(fire_at) });
            self.fold_rollup(
                &[bucketed(&row, fire_at)],
                Fold { scope: SCOPE_SCHEDULE, status, name: RollupName::Fixed(&spec.name) },
            )
            .await;
            self.record_last_run(&row, status).await;
            return Ok(true);
        }
        if failures_only {
            content.insert("history_mode".into(), json!(HISTORY_FAILURES_ONLY));
        }
        // Frozen for the same reason as `history_mode`: the reaper must be able to
        // bound a run without a point read of the schedule row per in-flight run,
        // and editing or deleting the schedule must not move the ceiling of a run
        // already in flight. Omitted when unset, so the reaper applies the project
        // default — `option<int>` rejects NULL.
        if let Some(secs) = spec.deadline_secs.filter(|n| *n > 0) {
            content.insert("deadline_secs".into(), json!(secs));
        }

        let mut binds = bind_ref("", &ids::schedule_run(run_key)).to_vec();
        binds.extend(bind_ref("schedule", schedule_id));
        binds.extend([("content", Value::Object(content)), ("fire", json!(stamp(fire_at)))]);
        // A run that is born terminal (a suppressed `skip`) needs its
        // `finished_at` too, and that has to be cast in the statement.
        let stmt = if status == "running" {
            sql::CREATE_SCHEDULE_RUN
        } else {
            sql::CREATE_TERMINAL_SCHEDULE_RUN
        };
        let result = self.db.query(stmt, &binds).await;

        match result {
            Ok(_) => {
                // A run born terminal (a suppressed `skip`) never reaches
                // `finalize_schedule_run`, so it reports its own outcome. The
                // `fire_at` guard collapses a whole fan-out's worth of these into a
                // single write.
                if status != "running" {
                    let row = json!({ "schedule_name": spec.name, "fire_at": stamp(fire_at) });
                    self.record_last_run(&row, status).await;
                }
                Ok(true)
            }
            // The deterministic id turns a duplicate into the idempotency check:
            // two tickers racing the same fire, or a replayed sweep, collide here
            // instead of double-spawning.
            Err(e) if e.is_already_exists() => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn finalize_schedule_run(
        &self,
        run: &ids::Ref,
        status: &str,
        error: Option<Value>,
    ) -> anyhow::Result<()> {
        let mut patch = Map::new();
        patch.insert("status".into(), json!(status));
        if let Some(error) = error {
            patch.insert("error".into(), error);
        }
        let mut binds = bind_ref("", run).to_vec();
        binds.push(("patch", Value::Object(patch)));
        let finalized = self.db.query(sql::FINALIZE_SCHEDULE_RUN, &binds).await?;

        // An empty result means the `running` guard didn't match (already
        // finalized, or killed), and a run that didn't transition must not report
        // an outcome onto its schedule.
        if let Some(row) = first_row(finalized) {
            self.record_last_run(&row, status).await;
            self.discard_if_failures_only(run, &row, status).await;
        }
        Ok(())
    }

    /// Under `history: failures-only`, a run that finished cleanly is deleted as it
    /// finalizes rather than waiting out a retention window.
    ///
    /// Order matters: the outcome is recorded on the schedule and folded into the
    /// rollup FIRST, because after the delete there is nothing left to count. The mode
    /// is read off the run row itself (frozen there at spawn), so this costs no extra
    /// read and a config change never alters the fate of a run already in flight.
    async fn discard_if_failures_only(&self, run: &ids::Ref, row: &Value, status: &str) {
        if row.get("history_mode").and_then(Value::as_str) != Some(HISTORY_FAILURES_ONLY) {
            return;
        }
        if !sql::is_success_shaped(status) {
            return;
        }
        let Some(name) = row.get("schedule_name").and_then(Value::as_str) else { return };
        let bucket_at = row
            .get("fire_at")
            .and_then(Value::as_str)
            .and_then(crate::cron::parse_datetime)
            .unwrap_or_else(Utc::now);
        self.fold_rollup(
            &[bucketed(row, bucket_at)],
            Fold { scope: SCOPE_SCHEDULE, status, name: RollupName::Fixed(name) },
        )
        .await;

        // The job this run spawned goes too. Safe only in this order: the run is already
        // terminal, and the heal pass only ever selects `running` runs — so no pass can
        // now see the job as missing and mislabel a successful run `job_missing`. A rival
        // engine mid-heal may still write that, but its finalize is guarded on `running`
        // and matches nothing, so it is a no-op (it does over-count `report.healed` by
        // one, which is cosmetic).
        //
        // Workflow runs are excluded: their step jobs are discarded with the workflow
        // run, so that a run which FAILED keeps the successful steps' jobs — those are
        // what you read to work out why it died.
        if row.get("kind").and_then(Value::as_str) == Some("job") {
            if let Some(job) = row.get("job_id").and_then(Value::as_str).and_then(ids::Ref::parse)
            {
                if sql::is_plain_identifier(&job.table) && sql::is_plain_identifier(&job.key) {
                    let stmt = sql::delete_records(&[job.as_string()]);
                    if let Err(e) = self.db.query(&stmt, &[]).await {
                        tracing::warn!(error = %e, "could not discard a successful job row");
                    }
                }
            }
        }

        if let Err(e) = self.db.query(sql::DELETE_SCHEDULE_RUN, &bind_ref("", run)).await {
            // Leaving the row behind is harmless — retention collects it later — so
            // this must not fail the run.
            tracing::warn!(run = %run, error = %e, "could not discard a successful run");
        }
    }

    /// Denormalize a run's outcome onto its schedule row.
    ///
    /// Best-effort: this is an operator convenience (`spky schedules list`), so a
    /// failure here must not turn a completed run into a failed sweep.
    async fn record_last_run(&self, run_row: &Value, status: &str) {
        let Some(name) = run_row.get("schedule_name").and_then(Value::as_str) else {
            return;
        };
        let Some(fire_at) = run_row.get("fire_at").and_then(Value::as_str) else {
            return;
        };
        let mut binds = bind_ref("", &ids::schedule(name)).to_vec();
        binds.extend([("status", json!(status)), ("at", json!(fire_at))]);
        if let Err(e) = self.db.query(sql::SET_LAST_RUN, &binds).await {
            tracing::warn!(schedule = name, error = %e, "could not record last run outcome");
        }
    }

    // -- observing ----------------------------------------------------------

    /// React to a job that just reached a terminal status.
    ///
    /// Called from the ingest path: the runner's terminal `UPDATE` fires the
    /// table's `_00_<table>_mutation` event, which POSTs `/ingest`, and the host
    /// hands the job id here. That is the fast path — delivery is best-effort, so
    /// [`Self::heal_pass`] reaches the same conclusion within one sweep if the
    /// event is lost. Both are idempotent and guarded, so a duplicate is a no-op.
    ///
    /// Returns `true` when the job belonged to a schedule or a workflow step.
    pub async fn observe_job_terminal(&self, job_id: &str, status: &str) -> anyhow::Result<bool> {
        if !matches!(status, "success" | "failed") {
            return Ok(false);
        }

        // A one-shot scheduled job: mirror the outcome onto its run.
        let runs = rows(self.db.query(sql::FIND_RUN_BY_JOB, &[("job_id", json!(job_id))]).await?);
        let mut matched = false;
        for run in runs {
            let Some(run_ref) = row_ref(&run) else { continue };
            let error = match status {
                "failed" => self.load_job(job_id).await?.as_ref().and_then(last_error),
                _ => None,
            };
            self.finalize_schedule_run(&run_ref, status, error).await?;
            matched = true;
        }

        // A workflow step: let the DAG advance, which finalizes the step, may
        // dispatch its dependents, and may finish the run.
        let steps = rows(self.db.query(sql::FIND_STEP_BY_JOB, &[("job_id", json!(job_id))]).await?);
        for step in steps {
            let Some(wf_run) = step
                .get("workflow_run")
                .and_then(Value::as_str)
                .and_then(ids::Ref::parse)
            else {
                continue;
            };
            self.advance_workflow(&wf_run).await?;
            matched = true;
        }

        Ok(matched)
    }

    // -- healing ------------------------------------------------------------

    /// Reconcile state that an event should have delivered but didn't.
    ///
    /// The engine learns about finished jobs from the ingest event the runner's
    /// terminal UPDATE fires, and that delivery is best-effort — an SSP restart
    /// or a dropped `http::post` loses it. So every sweep also asks the database
    /// directly: any `running` job-run whose job row is already terminal gets
    /// finalized here, capping the worst-case delay at one sweep interval.
    async fn heal_pass(&self, report: &mut TickReport) -> anyhow::Result<()> {
        // Every in-flight run, paired with the job it is waiting on. Collected up
        // front so the job rows can be read in bulk: this used to be one query per
        // running run, issued sequentially, which at fan-out width is hundreds of
        // round-trips every five seconds — and the sweep's latency is what caps how
        // long a lost event delays a completion.
        let mut waiting: Vec<(ids::Ref, String)> = Vec::new();
        for run in rows(self.db.query(sql::SELECT_RUNNING_JOB_RUNS, &[]).await?) {
            let Some(run_ref) = row_ref(&run) else { continue };
            let Some(job_id) = run.get("job_id").and_then(Value::as_str) else { continue };
            let Some(job) = ids::Ref::parse(job_id) else {
                tracing::warn!(job_id, "unparseable job id on a running run");
                continue;
            };
            // The id is spliced into the statement, so both halves must look like
            // identifiers. `ids::job` guarantees it; a hand-edited row need not.
            if !sql::is_plain_identifier(&job.table) || !sql::is_plain_identifier(&job.key) {
                tracing::warn!(job_id, "refusing to look up an unsafe job id");
                continue;
            }
            waiting.push((run_ref, job.as_string()));
        }

        for chunk in waiting.chunks(HEAL_LOOKUP_BATCH) {
            let ids: Vec<String> = chunk.iter().map(|(_, job)| job.clone()).collect();
            let found = rows(self.db.query(&sql::select_jobs_terminal(&ids), &[]).await?);
            let by_id: BTreeMap<&str, &Value> = found
                .iter()
                .filter_map(|row| row.get("id").and_then(Value::as_str).map(|id| (id, row)))
                .collect();

            for (run_ref, job_id) in chunk {
                let Some(job) = by_id.get(job_id.as_str()) else {
                    // The job row is gone (pruned, or an operator deleted it): the
                    // run can never complete, so stop calling it `running`.
                    self.finalize_schedule_run(
                        run_ref,
                        "failed",
                        Some(json!({ "code": "job_missing", "reason": "job row no longer exists" })),
                    )
                    .await?;
                    report.healed += 1;
                    continue;
                };
                match job.get("status").and_then(Value::as_str).unwrap_or("") {
                    "success" => {
                        self.finalize_schedule_run(run_ref, "success", None).await?;
                        report.healed += 1;
                    }
                    "failed" => {
                        self.finalize_schedule_run(
                            run_ref,
                            "failed",
                            Some(job_failure_error(job, job_id)),
                        )
                        .await?;
                        report.healed += 1;
                    }
                    _ => {}
                }
            }
        }

        report.healed += self.heal_workflow_runs(report).await?;
        Ok(())
    }

    pub(crate) async fn load_job(&self, job_id: &str) -> anyhow::Result<Option<Value>> {
        let Some(job) = ids::Ref::parse(job_id) else { return Ok(None) };
        let results = self.db.query(sql::SELECT_JOB, &bind_ref("", &job)).await?;
        Ok(match results.into_iter().next() {
            Some(v @ Value::Object(_)) => Some(v),
            _ => None,
        })
    }

    // -- reaping ------------------------------------------------------------

    /// Terminalize runs that have blown their deadline.
    ///
    /// This is the backstop that makes "stuck forever" unreachable. Every other
    /// mechanism in the engine is a reconciliation: it needs SOMETHING to observe —
    /// a job row reaching a terminal status, a step holding a job id, a kill flag.
    /// The failures that actually stranded runs in production were the ones where
    /// there was nothing left to observe at all, and no amount of re-advancing a run
    /// moves it. Only a clock does.
    ///
    /// Placed after `heal_pass` and before `prune_pass`, and the order is
    /// load-bearing in both directions. Heal must get first refusal, or a run whose
    /// job succeeded a second before the deadline is reported failed when it
    /// actually finished. Prune must come after, for the reason spelled out on
    /// `prune_pass`: a prune that runs first manufactures the `job_missing` failure
    /// this pass would otherwise resolve honestly.
    ///
    /// Never returns `Err` for a single unreapable run — a run whose kill request
    /// cannot be delivered is logged and retried next sweep, exactly like a
    /// schedule whose fan-out failed.
    async fn reap_pass(&self, report: &mut TickReport) -> anyhow::Result<()> {
        let Some(default) = self.load_retention().await.run_deadline else {
            // `run_deadline_secs = 0`: the operator has explicitly turned the
            // reaper off. Their deployment, their call.
            return Ok(());
        };
        let binds =
            [("default", json!(default.num_seconds())), ("batch", json!(REAP_BATCH as i64))];

        // Workflow runs first. Failing one kills its dispatched steps' jobs and
        // mirrors the outcome onto its owning schedule run, so doing these before
        // the job-kind scan means a workflow's schedule run is already terminal by
        // the time the second query looks — it never sees it twice.
        for row in rows(self.db.query(sql::SELECT_EXPIRED_WORKFLOW_RUNS, &binds).await?) {
            let Some(wf_run) = row_ref(&row) else { continue };
            let secs = row.get("deadline_secs").and_then(Value::as_i64).unwrap_or(default.num_seconds());
            let error = json!({
                "code": "deadline_exceeded",
                "reason": format!("run exceeded its {secs}s deadline without finishing"),
                "deadline_secs": secs,
            });
            // Re-read rather than trust the scan: `heal_pass` ran moments ago and
            // may have landed a real completion. `stop_workflow_run` is guarded on
            // `running` too, but skipping the steps of an already-finished run would
            // be a visible lie in the step rows.
            let loaded = match self.load_workflow_run(&wf_run).await {
                Ok(Some(loaded)) => loaded,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(run = %wf_run, error = %e, "could not re-read a run to reap");
                    report.errored += 1;
                    continue;
                }
            };
            if loaded.get("status").and_then(Value::as_str) != Some("running") {
                continue;
            }
            // Reuse the stop path rather than writing `failed` directly: it is the
            // one place that skips what never started, kills the jobs of what did,
            // and terminalizes both rows. Leaving a reaped run's step jobs running
            // would defeat the point of reaping it. `failed`, not `killed` — nobody
            // killed this, and it must not read as though someone did.
            if let Err(e) = self.stop_workflow_run(&wf_run, &loaded, "failed", error).await {
                tracing::warn!(run = %wf_run, error = %e, "could not stop a run past its deadline");
                report.errored += 1;
                continue;
            }
            tracing::warn!(run = %wf_run, deadline_secs = secs, "reaped a workflow run past its deadline");
            report.reaped += 1;
        }

        // Job-kind runs. There is no DAG here: one run, one outbox job.
        for row in rows(self.db.query(sql::SELECT_EXPIRED_JOB_RUNS, &binds).await?) {
            let Some(run_ref) = row_ref(&row) else { continue };
            let secs = row.get("deadline_secs").and_then(Value::as_i64).unwrap_or(default.num_seconds());
            if let Some(job_id) = row.get("job_id").and_then(Value::as_str) {
                // Best-effort: the job may already be terminal, or its SSP may be
                // unreachable. Either way the run must stop being `running`, or the
                // key it holds is exactly the wedge this pass exists to clear.
                if let Err(e) = self.kill.kill(job_id).await {
                    tracing::warn!(job_id, error = %e, "could not kill the job of a run past its deadline");
                }
            }
            let error = json!({
                "code": "deadline_exceeded",
                "reason": format!("run exceeded its {secs}s deadline without finishing"),
                "deadline_secs": secs,
            });
            if let Err(e) = self.finalize_schedule_run(&run_ref, "failed", Some(error)).await {
                tracing::warn!(run = %run_ref, error = %e, "could not fail a run past its deadline");
                report.errored += 1;
                continue;
            }
            tracing::warn!(run = %run_ref, deadline_secs = secs, "reaped a job run past its deadline");
            report.reaped += 1;
        }

        Ok(())
    }

    // -- retention ----------------------------------------------------------

    /// Drop terminal history past the retention window.
    ///
    /// Runs on its own slow cadence ([`PRUNE_INTERVAL_SECS`]) rather than every
    /// sweep: firing and healing need 5-second responsiveness, retention does not,
    /// and each prune statement is a scan over a status partition. At high fan-out
    /// this pass is the most expensive thing in the tick while deleting nothing
    /// almost every time it runs.
    ///
    /// **Ordering is load-bearing.** [`Self::tick_pass`] runs `heal_pass` before
    /// this and propagates its error, so by the time a prune happens every
    /// `running` job-run has already been reconciled against its job row. That is
    /// what stops a prune from manufacturing the `job_missing` failure in
    /// [`Self::heal_pass`]: the grace needed between a job going terminal and its
    /// row becoming prunable is one sweep interval, not one retention window. Do
    /// not reorder these passes, and do not let a heal failure fall through to a
    /// prune.
    async fn prune_pass(&self, now: DateTime<Utc>, report: &mut TickReport) -> anyhow::Result<()> {
        if !self.claim_prune(now) {
            return Ok(());
        }
        let policy = self.load_retention().await;
        let overrides = self.load_history_overrides().await;
        let except = json!(overrides.iter().map(|(n, _, _)| n.clone()).collect::<Vec<_>>());

        // Schedules with their own `history:` window are excluded from the global
        // pass and pruned per-schedule below, so neither window can clobber the
        // other.
        for status in sql::SCHEDULE_RUN_PRUNABLE {
            let before = json!(stamp(now - policy.for_run_status(status)));
            self.prune_batches(
                sql::PRUNE_SCHEDULE_RUNS,
                &[("status", json!(status)), ("except", except.clone())],
                &before,
                Fold { scope: SCOPE_SCHEDULE, status, name: RollupName::Field("schedule_name") },
                report,
            )
            .await;
        }
        for (name, success, failed) in &overrides {
            for status in sql::SCHEDULE_RUN_PRUNABLE {
                let window = match status {
                    "failed" | "killed" => failed.unwrap_or(policy.run_failed),
                    _ => success.unwrap_or(policy.run_success),
                };
                let before = json!(stamp(now - window));
                self.prune_batches(
                    sql::PRUNE_SCHEDULE_RUNS_FOR,
                    &[("status", json!(status)), ("name", json!(name))],
                    &before,
                    Fold { scope: SCOPE_SCHEDULE, status, name: RollupName::Fixed(name) },
                    report,
                )
                .await;
            }
        }
        for status in sql::WORKFLOW_RUN_PRUNABLE {
            let before = json!(stamp(now - policy.for_run_status(status)));
            self.prune_batches(
                sql::PRUNE_WORKFLOW_RUNS,
                &[("status", json!(status))],
                &before,
                Fold { scope: SCOPE_SCHEDULE, status, name: RollupName::Field("schedule_name") },
                report,
            )
            .await;
        }

        // Outbox job rows. These are ordinary synced user tables, so every deleted
        // row fires the table's generated delete event and POSTs the record to
        // `/ingest` — which is why the batch budget matters far more here than on
        // the `_00_*` tables.
        for table in &policy.job_tables {
            if !sql::is_plain_identifier(table) {
                tracing::warn!(table = %table, "skipping retention for an unsafe table name");
                continue;
            }
            let stmt = sql::prune_job_table(table);
            for status in sql::JOB_PRUNABLE {
                let before = json!(stamp(now - policy.for_job_status(status)));
                self.prune_batches(
                    &stmt,
                    &[("status", json!(status))],
                    &before,
                    Fold { scope: SCOPE_JOB, status, name: RollupName::Fixed(table) },
                    report,
                )
                .await;
            }
        }

        self.enforce_row_cap(now, &policy, report).await;
        Ok(())
    }

    /// Trim whatever is left over a hard row cap, regardless of age.
    ///
    /// The age windows already bound volume at rate x window, so this is a valve for
    /// the pathological case: a fan-out so wide that even a short window holds more
    /// rows than the deployment can carry. `max_rows: 0` (the default) disables it.
    ///
    /// Runs on a slow multiple of the prune cadence because it has to COUNT, which is
    /// an index walk rather than a point read. Only success-shaped statuses are ever
    /// trimmed: a cap must never be the reason a failure disappears, so a table can
    /// legitimately sit above the cap if failures alone exceed it.
    async fn enforce_row_cap(
        &self,
        now: DateTime<Utc>,
        policy: &Retention,
        report: &mut TickReport,
    ) {
        let Some(max_rows) = policy.max_rows else { return };
        if !self.prune_passes.fetch_add(1, Ordering::Relaxed).is_multiple_of(CAP_EVERY_N_PRUNES) {
            return;
        }
        // No age bound: the cap is about volume, so every terminal row is a
        // candidate and the batch size is what limits the damage.
        let unbounded = json!(stamp(now));

        for status in CAP_TRIMMABLE_RUN_STATUSES {
            let n = self.count_status(sql::COUNT_RUNS_IN_STATUS, status).await;
            let Some(over) = n.checked_sub(max_rows).filter(|o| *o > 0) else { continue };
            tracing::info!(status, over, max_rows, "trimming schedule-run history over the cap");
            self.prune_batches_capped(
                sql::PRUNE_SCHEDULE_RUNS,
                &[("status", json!(status)), ("except", json!(Vec::<String>::new()))],
                &unbounded,
                Fold { scope: SCOPE_SCHEDULE, status, name: RollupName::Field("schedule_name") },
                Some(over),
                report,
            )
            .await;
        }

        for table in &policy.job_tables {
            if !sql::is_plain_identifier(table) {
                continue;
            }
            let count_stmt = sql::count_jobs_in_status(table);
            let prune_stmt = sql::prune_job_table(table);
            for status in CAP_TRIMMABLE_JOB_STATUSES {
                let n = self.count_status(&count_stmt, status).await;
                let Some(over) = n.checked_sub(max_rows).filter(|o| *o > 0) else { continue };
                tracing::info!(table = %table, status, over, max_rows, "trimming jobs over the cap");
                self.prune_batches_capped(
                    &prune_stmt,
                    &[("status", json!(status))],
                    &unbounded,
                    Fold { scope: SCOPE_JOB, status, name: RollupName::Fixed(table) },
                    Some(over),
                    report,
                )
                .await;
            }
        }
    }

    async fn count_status(&self, stmt: &str, status: &str) -> usize {
        match self.db.query(stmt, &[("status", json!(status))]).await {
            Ok(results) => count_value(results).max(0) as usize,
            Err(e) => {
                tracing::warn!(error = %e, status, "could not count rows for the cap");
                0
            }
        }
    }

    /// Read the deploy-written policy, falling back to the engine defaults.
    async fn load_retention(&self) -> Retention {
        let fallback = Retention {
            job_success: self.cfg.history_max_age,
            job_failed: self.cfg.history_max_age,
            run_success: self.cfg.history_max_age,
            run_failed: self.cfg.history_max_age,
            job_tables: Vec::new(),
            max_rows: None,
            // A stack running ahead of its CLI has no `_00_retention` row at all.
            // Defaulting the reaper ON there matters more than the numbers being
            // exact: the failure this guards against is a run that never ends.
            run_deadline: Some(Duration::seconds(DEFAULT_RUN_DEADLINE_SECS)),
        };
        let row = match self.db.query(sql::SELECT_RETENTION, &[]).await {
            Ok(results) => first_row(results),
            Err(e) => {
                tracing::warn!(error = %e, "could not read retention policy; using defaults");
                return fallback;
            }
        };
        let Some(row) = row.filter(|v| v.is_object()) else {
            return fallback;
        };
        let secs = |key: &str, default: Duration| {
            row.get(key)
                .and_then(Value::as_i64)
                .filter(|n| *n > 0)
                .map(Duration::seconds)
                .unwrap_or(default)
        };
        Retention {
            job_success: secs("success_secs", fallback.job_success),
            job_failed: secs("failed_secs", fallback.job_failed),
            run_success: secs("run_success_secs", fallback.run_success),
            run_failed: secs("run_failed_secs", fallback.run_failed),
            job_tables: row
                .get("job_tables")
                .and_then(Value::as_array)
                .map(|xs| xs.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default(),
            // 0 (the DDL default) means disabled, not "keep nothing".
            max_rows: row
                .get("max_rows")
                .and_then(Value::as_i64)
                .filter(|n| *n > 0)
                .map(|n| n as usize),
            // Absent (a row written by an older CLI) falls back to the default;
            // present-and-0 is an explicit "no reaper" and is honoured.
            run_deadline: match row.get("run_deadline_secs").and_then(Value::as_i64) {
                Some(n) if n > 0 => Some(Duration::seconds(n)),
                Some(_) => None,
                None => fallback.run_deadline,
            },
        }
    }

    /// `(schedule name, success window, failed window)` for schedules that set
    /// `history:` in config.
    #[allow(clippy::type_complexity)]
    async fn load_history_overrides(&self) -> Vec<(String, Option<Duration>, Option<Duration>)> {
        let rows = match self.db.query(sql::SELECT_HISTORY_OVERRIDES, &[]).await {
            Ok(results) => rows(results),
            Err(e) => {
                tracing::warn!(error = %e, "could not read per-schedule retention overrides");
                return Vec::new();
            }
        };
        rows.into_iter()
            .filter_map(|row| {
                let name = row.get("name").and_then(Value::as_str)?.to_string();
                let win = |key: &str| {
                    row.get(key).and_then(Value::as_i64).filter(|n| *n > 0).map(Duration::seconds)
                };
                Some((name, win("history_success_secs"), win("history_failed_secs")))
            })
            .collect()
    }

    /// Make the next sweep prune regardless of when the last one did.
    ///
    /// Test-only: the cadence gate is deliberately not reachable in production — a
    /// host that wants a different interval should change the constant rather than
    /// race the gate. Tests need it because most of them already ran a sweep (and
    /// so claimed the prune slot) while arranging their fixture.
    #[cfg(test)]
    pub(crate) fn force_prune_next_pass(&self) {
        self.last_prune_at.store(0, Ordering::Relaxed);
    }

    /// Has `PRUNE_INTERVAL_SECS` elapsed since the last prune? Claims the slot if
    /// so. The first call after start always claims, so a restart loop cannot
    /// starve retention.
    fn claim_prune(&self, now: DateTime<Utc>) -> bool {
        let now_ms = now.timestamp_millis();
        let last = self.last_prune_at.load(Ordering::Relaxed);
        if last != 0 && now_ms - last < PRUNE_INTERVAL_SECS * 1000 {
            return false;
        }
        self.last_prune_at.store(now_ms, Ordering::Relaxed);
        true
    }

    /// Run one prune statement repeatedly until it stops returning a full batch.
    ///
    /// Bounded twice over: `PRUNE_BATCH` rows per statement and
    /// `PRUNE_MAX_BATCHES` statements per call. Deleting a row on a synced table
    /// costs an HTTP round-trip out of the database (the generated
    /// `_00_<t>_delete` event ingests the record), so a backlog has to drain at a
    /// deliberate rate rather than in one transaction. A partial batch means this
    /// status is drained, so stop.
    ///
    /// Never returns `Err`: retention failing is a warning, not a reason to abort a
    /// sweep that has already planned and fired.
    async fn prune_batches(
        &self,
        stmt: &str,
        extra: &[(&str, Value)],
        before: &Value,
        fold: Fold<'_>,
        report: &mut TickReport,
    ) {
        self.prune_batches_capped(stmt, extra, before, fold, None, report).await
    }

    /// As [`Self::prune_batches`], but stopping after `total` rows.
    ///
    /// `total` is what the row cap needs and what the age-based prune must NOT have:
    /// the age prune drains everything past its window (bounded only by the per-pass
    /// batch budget), while the cap wants exactly the overage. Without the bound the
    /// cap's loop keeps going as long as batches come back full — and since it runs
    /// with no age filter, "full" is always true until the table is empty. That would
    /// delete a project's entire success history the first time a cap was set.
    async fn prune_batches_capped(
        &self,
        stmt: &str,
        extra: &[(&str, Value)],
        before: &Value,
        fold: Fold<'_>,
        total: Option<usize>,
        report: &mut TickReport,
    ) {
        let mut remaining = total.unwrap_or(usize::MAX);
        for _ in 0..PRUNE_MAX_BATCHES {
            let batch = PRUNE_BATCH.min(remaining);
            if batch == 0 {
                return;
            }
            let mut binds = vec![("before", before.clone()), ("batch", json!(batch))];
            binds.extend(extra.iter().cloned());
            match self.db.query(stmt, &binds).await {
                Ok(results) => {
                    let doomed = rows(results);
                    let n = doomed.len();
                    report.pruned += n;
                    // Fold BEFORE deciding to stop: the rows are already deleted, so a
                    // fold that never happens is a count lost for good.
                    self.fold_rollup(&doomed, fold).await;
                    remaining = remaining.saturating_sub(n);
                    if n < batch || remaining == 0 {
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, status = %fold.status, "history prune failed");
                    return;
                }
            }
        }
        tracing::info!(
            status = %fold.status,
            batches = PRUNE_MAX_BATCHES,
            "history prune hit its per-pass batch budget; continuing next pass"
        );
    }

    /// Fold a SINGLE row into the rollup, hour-bucketing it here.
    ///
    /// The batched prunes get their bucket from SurrealDB's `time::floor`; a row
    /// discarded one at a time has to floor it in Rust, and the two must agree or the
    /// same hour ends up split across two buckets.
    pub(crate) async fn fold_rollup_one(
        &self,
        row: &Value,
        at: DateTime<Utc>,
        scope: &str,
        status: &str,
        name: &str,
    ) {
        self.fold_rollup(&[bucketed(row, at)], Fold { scope, status, name: RollupName::Fixed(name) })
            .await;
    }

    /// Accumulate a deleted batch into the hourly rollup.
    ///
    /// The counts come out of the projection the prune already selected, so this
    /// costs one UPSERT per (name, hour) touched — typically one — and no extra read.
    /// Best-effort: losing a counter must never turn a completed prune into an error.
    async fn fold_rollup(&self, doomed: &[Value], fold: Fold<'_>) {
        if doomed.is_empty() {
            return;
        }
        let mut buckets: BTreeMap<(String, String), i64> = BTreeMap::new();
        for row in doomed {
            let Some(bucket) = row.get("bucket").and_then(Value::as_str) else { continue };
            let name = match fold.name {
                RollupName::Fixed(name) => name.to_string(),
                RollupName::Field(field) => match row.get(field).and_then(Value::as_str) {
                    Some(name) => name.to_string(),
                    // A workflow run spawned ad hoc has no owning schedule; count it
                    // under a stable placeholder rather than dropping it.
                    None => ROLLUP_NAME_AD_HOC.to_string(),
                },
            };
            *buckets.entry((name, bucket.to_string())).or_default() += 1;
        }

        for ((name, bucket), n) in buckets {
            let id = ids::rollup(fold.scope, &name, &bucket);
            let mut binds = bind_ref("", &id).to_vec();
            binds.extend([
                ("scope", json!(fold.scope)),
                ("name", json!(name)),
                ("bucket", json!(bucket)),
            ]);
            // Exactly one counter moves per statement; the rest must still be bound.
            for status in ["success", "failed", "skipped", "replaced", "killed"] {
                binds.push((status, json!(if status == fold.status { n } else { 0 })));
            }
            if let Err(e) = self.db.query(sql::UPSERT_ROLLUP, &binds).await {
                tracing::warn!(error = %e, scope = fold.scope, "could not fold rollup counters");
            }
        }
    }

    // -- shared helpers -----------------------------------------------------

    /// Parse a schedule row, recording (rather than propagating) a bad one.
    async fn read_spec(
        &self,
        row: &Value,
        report: &mut TickReport,
    ) -> Option<(ids::Ref, ScheduleSpec)> {
        let id = row_ref(row)?;
        match ScheduleSpec::from_row(row) {
            Ok(spec) => Some((id, spec)),
            Err(e) => {
                self.record_error(&id, &format!("unreadable schedule row: {e}"), report).await;
                None
            }
        }
    }

    async fn record_error(&self, id: &ids::Ref, error: &str, report: &mut TickReport) {
        report.errored += 1;
        tracing::warn!(schedule = %id, error, "schedule error");
        let mut binds = bind_ref("", id).to_vec();
        binds.push(("error", json!(error)));
        if let Err(e) = self.db.query(sql::RECORD_ERROR, &binds).await {
            tracing::warn!(schedule = %id, error = %e, "could not record schedule error");
        }
    }
}

/// The `error` a suppressed fire carries, built from the run that suppressed it.
///
/// Shaped like every other recorded cause in this crate — a machine-readable `code`
/// plus a sentence — with the blocker's ids alongside so a dashboard can link
/// straight to whatever is holding the key. `since` is the blocking run's
/// `created_at`: a fire skipped behind something that started nine hours ago is a
/// wedge, and one skipped behind something that started nine seconds ago is a
/// schedule firing faster than its own work.
fn skip_reason(blocker: &Value) -> Value {
    let field = |name: &str| blocker.get(name).and_then(Value::as_str).map(str::to_string);
    let mut reason = Map::new();
    reason.insert("code".into(), json!("concurrency_skip"));
    reason.insert(
        "reason".into(),
        json!(match field("created_at") {
            Some(since) => format!("a run for this key has been in flight since {since}"),
            None => "a run for this key is still in flight".to_string(),
        }),
    );
    for (name, value) in [
        ("blocked_by", field("id")),
        ("blocked_by_workflow_run", field("workflow_run")),
        ("blocked_by_job", field("job_id")),
        ("since", field("created_at")),
    ] {
        // `type::string(NONE)` is the literal "NONE", which would read as a real id.
        if let Some(value) = value.filter(|v| v != "NONE" && !v.is_empty()) {
            reason.insert(name.into(), json!(value));
        }
    }
    Value::Object(reason)
}

/// Bind a record id the only way that is safe: as a (table, key) pair. `prefix`
/// names the pair — `""` for the statement's primary `$tb`/`$key`, or e.g.
/// `"schedule"` for a second reference in the same statement.
pub(crate) fn bind_ref(prefix: &str, r: &ids::Ref) -> [(&'static str, Value); 2] {
    match prefix {
        "schedule" => [("schedule_tb", json!(r.table)), ("schedule_key", json!(r.key))],
        "wf" => [("wf_tb", json!(r.table)), ("wf_key", json!(r.key))],
        _ => [("tb", json!(r.table)), ("key", json!(r.key))],
    }
}

/// RFC 3339 with millisecond precision — the form SurrealDB's flattened values
/// hand back, so a stamp we write can be compared against one we read.
pub(crate) fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// `id` out of a flattened row, split into the (table, key) pair every statement
/// needs. Flattened ids arrive backtick-quoted when the key needs escaping, which
/// [`ids::Ref::parse`] strips.
pub(crate) fn row_ref(row: &Value) -> Option<ids::Ref> {
    row.get("id").and_then(Value::as_str).and_then(ids::Ref::parse)
}

/// Last entry of a job row's `errors` array, for the run's `error` field.
pub(crate) fn last_error(job: &Value) -> Option<Value> {
    job.get("errors").and_then(Value::as_array).and_then(|errors| errors.last().cloned())
}

/// The `error` object to record for a job that failed terminally. NEVER `None`.
///
/// `last_error` is the preferred answer, but the runner's append to the outbox's
/// `errors` array is best-effort by necessity (a `?` there wedges the job on the
/// SSP), so a genuinely failed job can arrive here with an empty `errors`. That
/// used to mean the step or run recorded `status = 'failed'` with `error = NONE`
/// — a failure with no reason, which is the single least useful row this system
/// can produce and exactly what an operator comes looking for.
///
/// So the shape is synthesized instead. `_00_step_run.error` and
/// `_00_schedule_run.error` are `option<object> FLEXIBLE` on tables this crate
/// owns, so unlike the outbox append this write cannot be rejected.
pub(crate) fn job_failure_error(job: &Value, job_id: &str) -> Value {
    if let Some(error) = last_error(job) {
        return error;
    }
    let mut reason = format!(
        "job {job_id} reported `failed` but recorded no error;          its `errors` array is empty"
    );
    let retries = job.get("retries").and_then(Value::as_i64);
    let max_retries = job.get("max_retries").and_then(Value::as_i64);
    if let (Some(retries), Some(max)) = (retries, max_retries) {
        reason.push_str(&format!(" (after {retries} of {max} attempts)"));
    }
    // Name the likeliest cause: the append the SSP could not make. Its own log
    // carries the rejection; this points the reader at it.
    reason.push_str(". Check the SSP log for a failed `errors` append.");
    let mut error = Map::new();
    error.insert("code".into(), json!("job_failed"));
    error.insert("reason".into(), json!(reason));
    if let Some(retries) = retries {
        error.insert("retries".into(), json!(retries));
    }
    if let Some(max) = max_retries {
        error.insert("max_retries".into(), json!(max));
    }
    Value::Object(error)
}

/// Concurrency key for a fan-out row: the value at `key_field`, stringified.
fn fan_out_key(row: &Value, key_field: &str) -> String {
    match row.get(key_field) {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        // No such field: every row shares one key, so `skip` serializes the
        // whole fan-out. Surprising, but safer than silently spawning overlap.
        None => String::new(),
    }
}

/// Payload handed to a spawned job.
///
/// There is no `{{…}}` template language: the engine injects what a job could
/// need under reserved keys and the backend reads them. `row` is the fan-out row,
/// `steps` maps each direct dependency's name to its captured output.
pub(crate) fn build_payload(
    base: Option<&Value>,
    row: Option<&Value>,
    step_outputs: Option<&BTreeMap<String, Value>>,
) -> Value {
    let mut payload = match base {
        Some(Value::Object(map)) => map.clone(),
        _ => Map::new(),
    };
    if let Some(row) = row {
        payload.insert("row".into(), row.clone());
    }
    if let Some(outputs) = step_outputs {
        if !outputs.is_empty() {
            let steps: Map<String, Value> =
                outputs.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            payload.insert("steps".into(), Value::Object(steps));
        }
    }
    Value::Object(payload)
}

/// The outbox row the engine creates. `status` is deliberately absent — the
/// schema defaults it to `pending`, and the runner owns it from there.
pub(crate) fn build_job_content(
    path: &str,
    payload: Value,
    max_retries: Option<i64>,
    retry_strategy: Option<&str>,
    timeout: Option<i64>,
) -> Value {
    let mut content = Map::new();
    content.insert("path".into(), json!(path));
    content.insert("payload".into(), payload);
    // Omit rather than null: the outbox schema's own DEFAULTs then apply.
    if let Some(max_retries) = max_retries {
        content.insert("max_retries".into(), json!(max_retries));
    }
    if let Some(strategy) = retry_strategy {
        content.insert("retry_strategy".into(), json!(strategy));
    }
    if let Some(timeout) = timeout {
        content.insert("timeout".into(), json!(timeout));
    }
    Value::Object(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cron::parse_datetime;

    #[test]
    fn job_content_never_carries_status() {
        let content = build_job_content("/run", json!({}), Some(3), Some("linear"), None);
        let obj = content.as_object().unwrap();
        assert!(!obj.contains_key("status"), "the runner is the single status writer");
        assert!(!obj.contains_key("timeout"), "absent optionals are omitted, not nulled");
        assert_eq!(obj["max_retries"], json!(3));
    }

    #[test]
    fn payload_injects_row_and_step_outputs() {
        let mut outputs = BTreeMap::new();
        outputs.insert("extract".to_string(), json!({"fileId": "f_1"}));
        let payload = build_payload(
            Some(&json!({"olderThanDays": 30})),
            Some(&json!({"id": "connection:alice"})),
            Some(&outputs),
        );
        assert_eq!(payload["olderThanDays"], json!(30));
        assert_eq!(payload["row"]["id"], json!("connection:alice"));
        assert_eq!(payload["steps"]["extract"]["fileId"], json!("f_1"));
    }

    #[test]
    fn payload_omits_absent_sections() {
        let payload = build_payload(None, None, None);
        assert_eq!(payload, json!({}));
    }

    #[test]
    fn fan_out_key_stringifies_non_strings() {
        assert_eq!(fan_out_key(&json!({"id": "connection:a"}), "id"), "connection:a");
        assert_eq!(fan_out_key(&json!({"n": 7}), "n"), "7");
        assert_eq!(fan_out_key(&json!({"other": 1}), "id"), "");
    }

    #[test]
    fn stamps_round_trip_through_millis() {
        let now = Utc::now();
        let parsed = parse_datetime(&stamp(now)).unwrap();
        assert_eq!(parsed.timestamp_millis(), now.timestamp_millis());
    }
}
