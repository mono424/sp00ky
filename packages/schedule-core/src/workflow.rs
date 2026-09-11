//! Workflow DAG execution.
//!
//! A workflow run is a set of step rows plus the frozen DAG that produced them.
//! There is no in-memory execution state and no long-lived coordinator task:
//! every advancement reads the step rows, decides what changed, and writes the
//! transitions back under guards. That makes advancement idempotent, so it is
//! safe to trigger from a lost-event heal pass, from a job-completion event, and
//! from two of them at once.
//!
//! ```text
//! spawn ─▶ step rows (all blocked)
//!            │
//!            ├─ advance: deps satisfied? ─▶ CAS blocked→ready ─▶ CREATE job ─▶ dispatched
//!            │                                (winner of the CAS dispatches)
//!            ├─ job terminal ─▶ finalize step (success + output │ failed)
//!            ├─ a step failed ─▶ fail run, skip what can never run
//!            └─ nothing left non-terminal ─▶ finalize run, finalize schedule run
//! ```

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use crate::dag::{StepStatus, WorkflowDag};
use crate::db::{first_row, rows};
use crate::engine::{
    bind_ref, build_job_content, build_payload, job_failure_error, row_ref, ScheduleEngine, Trigger,
    ROLLUP_NAME_AD_HOC, SCOPE_SCHEDULE,
};
use crate::spec::{OnFailure, ScheduleSpec, StepDef, HISTORY_FAILURES_ONLY};
use crate::{ids, sql};

/// Why an operator action on a workflow run was refused.
///
/// Each variant maps to a distinct HTTP outcome at the admin API, which is why
/// they are enumerated rather than folded into one `anyhow` string.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowOpError {
    #[error("no workflow run {0}")]
    NotFound(String),
    #[error("run is {status}; only failed or killed runs can be retried")]
    NotTerminal { status: String },
    #[error("run is {status}, not running")]
    NotRunning { status: String },
    /// A kill leaves in-flight steps `dispatched` until the SSP processes it.
    /// Retrying before that would run the step twice.
    #[error("step {step}'s job is still {job_status}; wait for the kill to settle")]
    Settling { step: String, job_status: String },
    #[error("nothing to retry")]
    NothingToRetry,
    #[error("frozen dag is invalid: {0}")]
    BadDag(String),
    #[error("a rerun of this run was already created this millisecond")]
    AlreadyExists,
    #[error("run changed underneath the operation")]
    Conflict,
    #[error(transparent)]
    Db(#[from] anyhow::Error),
}

impl From<crate::db::ScheduleDbError> for WorkflowOpError {
    fn from(e: crate::db::ScheduleDbError) -> Self {
        WorkflowOpError::Db(e.into())
    }
}

/// The run a rerun produced, and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerunOutcome {
    pub run: ids::Ref,
    pub rerun_of: ids::Ref,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryOutcome {
    pub run: ids::Ref,
    /// Operator retries applied to this run so far, including this one.
    pub retry_count: i64,
    /// Steps sent back to `blocked` and about to be re-dispatched.
    pub reset: Vec<String>,
    /// Steps left as they were, with their outputs.
    pub kept: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOutcome {
    pub run: ids::Ref,
    /// `killed` when the run terminalized in this call, `kill_requested` when
    /// only the durable flag landed and the next sweep will finish it.
    pub status: String,
}

/// A step row as the engine reads it back.
struct StepRow {
    id: ids::Ref,
    step: String,
    status: StepStatus,
    job_id: Option<String>,
    output: Option<Value>,
    /// Dispatch attempts charged so far. NONE on rows older than the field, which
    /// reads as 0 — those get the full budget, which is the safe direction.
    dispatch_attempts: i64,
}

/// Recovery re-dispatches a single step gets before the engine gives up and fails it.
///
/// Only RECOVERY attempts are counted. The original dispatch is not, because the
/// promotion that precedes it deliberately writes no new field (see
/// `CLAIM_STEP_READY`), so there is nowhere to record it that would not put the
/// dispatch path at the mercy of this deployment's schema. Counting only recoveries
/// costs one extra retry of a hopeless step and buys a dispatch path that cannot be
/// broken by a field.
///
/// Three, not unbounded, because a step whose dispatch fails deterministically — a
/// job CREATE the outbox rejects — would otherwise be retried every five seconds for
/// as long as the deployment lives. And not one, because the first recovery runs on
/// the very next sweep and may still be racing the tail of whatever killed the
/// original attempt.
const MAX_DISPATCH_ATTEMPTS: i64 = 3;

impl ScheduleEngine {
    /// Spawn a workflow run for one fire (one fan-out item). `false` = this fire
    /// already produced a run.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn spawn_workflow(
        &self,
        schedule_id: &ids::Ref,
        spec: &ScheduleSpec,
        run_key: &str,
        key: &str,
        fire_at: DateTime<Utc>,
        trigger: Trigger,
        item: Option<Value>,
    ) -> anyhow::Result<bool> {
        let Some(def) = spec.workflow.as_ref() else {
            anyhow::bail!("workflow schedule has no `workflow` definition");
        };
        // Validate again here even though the CLI already did: a hand-edited row
        // should fail its own run, not wedge the sweep.
        let dag = WorkflowDag::validate(def)?;

        let wf_run = ids::workflow_run(run_key);

        // The schedule run is the idempotency gate for the whole fire, so it goes
        // first — before the workflow run and its steps exist.
        let created = self
            .create_schedule_run(
                schedule_id,
                spec,
                run_key,
                key,
                fire_at,
                trigger,
                "running",
                None,
                None,
                None,
            )
            .await?;
        if !created {
            return Ok(false);
        }

        let mut content = Map::new();
        content.insert("schedule_name".into(), json!(spec.name));
        content.insert("workflow_name".into(), json!(spec.name));
        // Frozen: redeploying the workflow can't change a run already in flight.
        content.insert("dag".into(), serde_json::to_value(def)?);
        // Same reasoning for the history mode: a successful run is discarded as it
        // finalizes, and at that point its owning schedule run may already be gone, so
        // the mode has to travel with the run rather than be read back.
        if let Some(mode) = spec.history_mode.as_deref() {
            content.insert("history_mode".into(), json!(mode));
        }
        // Frozen for the same reason, and read by the reaper.
        if let Some(secs) = spec.deadline_secs.filter(|n| *n > 0) {
            content.insert("deadline_secs".into(), json!(secs));
        }
        content.insert("status".into(), json!("running"));
        if let Some(table) = spec.target_table.as_deref() {
            content.insert("target_table".into(), json!(table));
        }
        if let Some(row) = item {
            content.insert("input".into(), row);
        }
        let mut binds = bind_ref("", &wf_run).to_vec();
        binds.extend(bind_ref("schedule", schedule_id));
        binds.push(("content", Value::Object(content)));

        // The run row and its step rows are the run's SKELETON, and the schedule
        // run that gates this fire is already `running` by now. So a half-written
        // skeleton is not a fire that did not happen — it is a fire that can never
        // finish: `advance_workflow` computes `all_terminal` over the step rows it
        // finds, and a run missing some (or all) of them never reaches it. The
        // schedule run then counts as in flight for `concurrency: skip` forever.
        //
        // Both writes are therefore terminalized on failure rather than merely
        // reported, which is the workflow-kind twin of the `spawn_failed` path
        // `spawn_one` already takes when a job CREATE fails.
        let skeleton = match self.create_if_absent(sql::CREATE_WORKFLOW_RUN, &binds).await {
            Ok(_) => self.create_step_rows(&dag, run_key, &wf_run).await,
            Err(e) => Err(e),
        };
        if let Err(e) = skeleton {
            self.fail_unspawnable_run(&wf_run, run_key, &e).await;
            return Err(e);
        }

        let mut binds = bind_ref("", &ids::schedule_run(run_key)).to_vec();
        binds.extend(bind_ref("wf", &wf_run));
        self.db.query(sql::LINK_WORKFLOW_RUN, &binds).await?;

        // Roots become ready through the same readiness pass as everything else.
        self.advance_workflow(&wf_run).await?;
        Ok(true)
    }

    /// One `blocked` step row per DAG node. Shared by a schedule fire and an
    /// operator rerun, so both start from the identical shape.
    async fn create_step_rows(
        &self,
        dag: &WorkflowDag,
        run_key: &str,
        wf_run: &ids::Ref,
    ) -> anyhow::Result<()> {
        for step in dag.steps() {
            let mut content = Map::new();
            content.insert("step".into(), json!(step.name));
            content.insert("depends_on".into(), json!(step.depends_on));
            content.insert("status".into(), json!("blocked"));
            let mut binds = bind_ref("", &ids::step_run(run_key, &step.name)).to_vec();
            binds.extend(bind_ref("wf", wf_run));
            binds.push(("content", Value::Object(content)));
            self.create_if_absent(sql::CREATE_STEP_RUN, &binds).await?;
        }
        Ok(())
    }

    // -- operator actions ---------------------------------------------------

    /// Start a fresh run of the same workflow: same frozen dag, same input, same
    /// target table, a new key, and no owning schedule.
    ///
    /// A rerun is deliberately ad-hoc. It never touches `_00_schedule_run`, never
    /// counts toward `concurrency: skip`, and rolls up under `(ad-hoc)`; tying it
    /// to the schedule would make it overlap with `spky schedules trigger`, which
    /// already exists for "fire this schedule now".
    pub async fn rerun_workflow(
        &self,
        source: &ids::Ref,
        now: DateTime<Utc>,
    ) -> Result<RerunOutcome, WorkflowOpError> {
        let run = self
            .load_workflow_run(source)
            .await?
            .ok_or_else(|| WorkflowOpError::NotFound(source.as_string()))?;
        // Refuse up front rather than spawn a run that fails `bad_dag` on its
        // first advancement.
        let dag = self.frozen_dag(&run).map_err(|e| WorkflowOpError::BadDag(e.to_string()))?;
        let workflow_name = run
            .get("workflow_name")
            .and_then(Value::as_str)
            .ok_or_else(|| WorkflowOpError::BadDag("run has no workflow_name".into()))?
            .to_string();

        let run_key = ids::rerun_key(&workflow_name, now.timestamp_millis(), &source.key);
        let wf_run = ids::workflow_run(&run_key);

        let mut content = Map::new();
        content.insert("workflow_name".into(), json!(workflow_name));
        content.insert("status".into(), json!("running"));
        content.insert("trigger".into(), json!(Trigger::Manual.as_str()));
        // Absent optionals are omitted, never nulled: `option<T>` rejects NULL.
        for field in ["dag", "target_table", "input", "history_mode", "deadline_secs"] {
            if let Some(v) = run.get(field).filter(|v| !v.is_null()) {
                content.insert(field.into(), v.clone());
            }
        }
        let mut binds = bind_ref("", &wf_run).to_vec();
        binds.push(("src_tb", json!(source.table)));
        binds.push(("src_key", json!(source.key)));
        binds.push(("content", Value::Object(content)));
        if !self.create_if_absent(sql::CREATE_WORKFLOW_RUN_RERUN, &binds).await? {
            return Err(WorkflowOpError::AlreadyExists);
        }

        self.create_step_rows(&dag, &run_key, &wf_run).await?;
        // Roots dispatch now rather than on the next sweep.
        self.advance_workflow(&wf_run).await?;
        Ok(RerunOutcome { run: wf_run, rerun_of: source.clone() })
    }

    /// Resume a failed or killed run from its failure point: successful steps keep
    /// their outputs, everything that failed or was skipped goes back to `blocked`
    /// and is dispatched again under a new attempt id.
    ///
    /// All the step work happens while the run is still terminal, which the sweep
    /// ignores entirely, and a single guarded write then reopens it. So a rival
    /// pass can only ever see the old terminal run or the fully reset one.
    pub async fn retry_workflow(&self, wf_run: &ids::Ref) -> Result<RetryOutcome, WorkflowOpError> {
        let run = self
            .load_workflow_run(wf_run)
            .await?
            .ok_or_else(|| WorkflowOpError::NotFound(wf_run.as_string()))?;
        let status = run.get("status").and_then(Value::as_str).unwrap_or("").to_string();
        if !matches!(status.as_str(), "failed" | "killed") {
            return Err(WorkflowOpError::NotTerminal { status });
        }
        // A run that failed on its dag would only fail the same way again.
        self.frozen_dag(&run).map_err(|e| WorkflowOpError::BadDag(e.to_string()))?;

        let steps = self.load_step_rows(wf_run).await?;

        // Pass A: nothing is written until every in-flight step has settled. A kill
        // leaves dispatched steps dispatched; their jobs go terminal only once the
        // SSP processes the kill, and resetting before that would double-run them.
        let mut dispatched_jobs: BTreeMap<String, Option<String>> = BTreeMap::new();
        for step in steps.iter().filter(|s| s.status == StepStatus::Dispatched) {
            let Some(job_id) = step.job_id.as_deref() else {
                dispatched_jobs.insert(step.step.clone(), None);
                continue;
            };
            let job_status = self
                .load_job(job_id)
                .await?
                .and_then(|j| j.get("status").and_then(Value::as_str).map(str::to_string));
            match job_status.as_deref() {
                Some("pending") | Some("processing") => {
                    return Err(WorkflowOpError::Settling {
                        step: step.step.clone(),
                        job_status: job_status.unwrap_or_default(),
                    });
                }
                _ => {
                    dispatched_jobs.insert(step.step.clone(), job_status);
                }
            }
        }

        // Pass B: reset what did not work. A dispatched step whose job succeeded is
        // left alone; the reopened run's first advancement lands it with its output.
        let mut reset = Vec::new();
        let mut kept = Vec::new();
        for step in &steps {
            match step.status {
                StepStatus::Failed | StepStatus::Skipped => {
                    let done = self.db.query(sql::RESET_STEP_FOR_RETRY, &bind_ref("", &step.id)).await?;
                    if first_row(done).is_some() {
                        reset.push(step.step.clone());
                    }
                }
                StepStatus::Dispatched => {
                    let job_status = dispatched_jobs.get(&step.step).cloned().flatten();
                    if job_status.as_deref() == Some("success") {
                        kept.push(step.step.clone());
                        continue;
                    }
                    let mut binds = bind_ref("", &step.id).to_vec();
                    binds.push(("job_id", json!(step.job_id.clone().unwrap_or_default())));
                    let done = self.db.query(sql::RESET_DISPATCHED_STEP_FOR_RETRY, &binds).await?;
                    if first_row(done).is_some() {
                        reset.push(step.step.clone());
                    }
                }
                StepStatus::Success => kept.push(step.step.clone()),
                // `blocked`/`ready` on a terminal run only happens on a `bad_dag`
                // failure, which was refused above; they re-dispatch as-is.
                StepStatus::Blocked | StepStatus::Ready => {}
            }
        }
        let landable = steps.iter().any(|s| {
            s.status == StepStatus::Dispatched
                && dispatched_jobs.get(&s.step).cloned().flatten().as_deref() == Some("success")
        });
        if reset.is_empty() && !landable {
            return Err(WorkflowOpError::NothingToRetry);
        }

        let reopened = self.db.query(sql::REOPEN_WORKFLOW_RUN, &bind_ref("", wf_run)).await?;
        let Some(reopened) = first_row(reopened) else {
            // Pruned, or reopened by someone else, between our read and this write.
            return Err(WorkflowOpError::Conflict);
        };
        let retry_count = reopened.get("retry_count").and_then(Value::as_i64).unwrap_or(1);

        // The owning schedule run comes back too, so the retry's final outcome is
        // mirrored there (finalize is guarded on `running`) and the key counts as
        // in flight for `concurrency: skip`, which it now is. Best effort: a missing
        // schedule run is an ad-hoc run or pruned history, not a failed retry.
        if run.get("schedule_name").and_then(Value::as_str).is_some() {
            let sched_run = ids::schedule_run(&wf_run.key);
            if let Err(e) = self.db.query(sql::REOPEN_SCHEDULE_RUN, &bind_ref("", &sched_run)).await {
                tracing::warn!(run = %sched_run, error = %e, "could not reopen the owning schedule run");
            }
        }

        self.advance_workflow(wf_run).await?;
        Ok(RetryOutcome { run: wf_run.clone(), retry_count, reset, kept })
    }

    /// Stop a running run now: skip what has not started, kill what has, and
    /// terminalize. The flag is written first as the durable fallback, so a crash
    /// mid-kill is finished by the next sweep.
    pub async fn cancel_workflow(&self, wf_run: &ids::Ref) -> Result<CancelOutcome, WorkflowOpError> {
        let run = self
            .load_workflow_run(wf_run)
            .await?
            .ok_or_else(|| WorkflowOpError::NotFound(wf_run.as_string()))?;
        let status = run.get("status").and_then(Value::as_str).unwrap_or("").to_string();
        if status != "running" {
            return Err(WorkflowOpError::NotRunning { status });
        }
        self.request_workflow_kill(wf_run).await?;
        let after = self
            .load_workflow_run(wf_run)
            .await?
            .and_then(|r| r.get("status").and_then(Value::as_str).map(str::to_string));
        let status = match after.as_deref() {
            Some("killed") => "killed",
            _ => "kill_requested",
        };
        Ok(CancelOutcome { run: wf_run.clone(), status: status.to_string() })
    }

    /// Drive one workflow run as far as it can currently go.
    ///
    /// Safe to call concurrently with itself: each transition is a guarded
    /// compare-and-swap, and dispatch only happens for steps whose
    /// `blocked → ready` promotion this call won.
    pub async fn advance_workflow(&self, wf_run: &ids::Ref) -> anyhow::Result<()> {
        let Some(run) = self.load_workflow_run(wf_run).await? else {
            return Ok(());
        };
        // Only `running` runs advance — this is what makes the pass a no-op for
        // runs that were killed, replaced, or already finished.
        if run.get("status").and_then(Value::as_str) != Some("running") {
            return Ok(());
        }
        if run.get("kill_requested").and_then(Value::as_bool) == Some(true) {
            return self.kill_workflow_run(wf_run, &run).await;
        }

        let dag = match self.frozen_dag(&run) {
            Ok(dag) => dag,
            Err(e) => {
                return self
                    .fail_workflow_run(wf_run, json!({ "code": "bad_dag", "reason": e.to_string() }))
                    .await;
            }
        };

        let mut steps = self.load_step_rows(wf_run).await?;

        // 1. Land the terminal state of any step whose job has finished. The
        //    engine reads job rows but never writes their status.
        for step in steps.iter_mut() {
            if step.status != StepStatus::Dispatched {
                continue;
            }
            // No job to read: the step won its promotion and then lost the
            // dispatch. Nothing lands here — step 3 recovers it, or gives up on
            // it once its attempt budget is spent.
            let Some(job_id) = step.job_id.as_deref() else { continue };
            let Some(job) = self.load_job(job_id).await? else {
                self.finalize_step(
                    &step.id,
                    "failed",
                    None,
                    Some(json!({ "code": "job_missing", "reason": "job row no longer exists" })),
                )
                .await?;
                step.status = StepStatus::Failed;
                continue;
            };
            match job.get("status").and_then(Value::as_str) {
                Some("success") => {
                    let output = job.get("result").cloned();
                    self.finalize_step(&step.id, "success", output.clone(), None).await?;
                    step.status = StepStatus::Success;
                    step.output = output;
                }
                Some("failed") => {
                    // Never `None`: a step that failed must say why, even when the
                    // job row lost its own error. See `job_failure_error`.
                    let error = job_failure_error(&job, job_id);
                    self.finalize_step(&step.id, "failed", None, Some(error)).await?;
                    step.status = StepStatus::Failed;
                }
                // pending / processing: the job is retrying or still running. A
                // step is only `failed` once its job row is terminally failed, so
                // per-step retry is just the job's own retry budget.
                _ => {}
            }
        }

        let statuses: BTreeMap<String, StepStatus> =
            steps.iter().map(|s| (s.step.clone(), s.status)).collect();
        let any_failed = steps.iter().any(|s| s.status == StepStatus::Failed);

        // 2. Propagate failure. `halt` stops everything that hasn't started;
        //    `continue-independent` skips only the branch below the failure and
        //    lets unrelated branches finish.
        if any_failed {
            let on_failure = self.on_failure(&run);
            // Paired with the cause to record on each one. The two policies doom a
            // step for different reasons and an operator reading the row later
            // cannot tell them apart from the status alone: `halt` stops work that
            // was perfectly healthy and merely unlucky in its timing, while
            // `continue-independent` stops only what genuinely depended on the
            // failure.
            let failed: Vec<&str> =
                steps.iter().filter(|s| s.status == StepStatus::Failed).map(|s| s.step.as_str()).collect();
            let to_skip: Vec<(ids::Ref, Value)> = match on_failure {
                OnFailure::Halt => {
                    let reason = json!({
                        "code": "run_halted",
                        "reason": format!(
                            "the run halted after {} failed, and this step had not started",
                            list(&failed),
                        ),
                        "failed_steps": failed,
                    });
                    steps
                        .iter()
                        .filter(|s| matches!(s.status, StepStatus::Blocked | StepStatus::Ready))
                        .map(|s| (s.id.clone(), reason.clone()))
                        .collect()
                }
                OnFailure::ContinueIndependent => {
                    let doomed = dag.doomed_steps(&statuses);
                    steps
                        .iter()
                        .filter_map(|s| {
                            let (_, blocker) =
                                doomed.iter().find(|(d, _)| d.name == s.step)?;
                            Some((
                                s.id.clone(),
                                json!({
                                    "code": "upstream_failed",
                                    "reason": format!(
                                        "depends on {blocker}, which did not succeed",
                                    ),
                                    "blocked_by": blocker,
                                    "failed_steps": failed,
                                }),
                            ))
                        })
                        .collect()
                }
            };
            for (step_ref, reason) in &to_skip {
                let mut binds = bind_ref("", step_ref).to_vec();
                binds.push(("error", reason.clone()));
                self.db.query(sql::SKIP_STEP, &binds).await?;
            }
            for step in steps.iter_mut() {
                if to_skip.iter().any(|(id, _)| id == &step.id) {
                    step.status = StepStatus::Skipped;
                }
            }

            // `SKIP_STEP` matches only `blocked`/`ready`, and `doomed_steps`
            // considers only those two, so a step stranded at `dispatched` with no
            // job survives both — and then survives everything else, because it is
            // not terminal and has no job that could ever make it terminal. It
            // provably never ran (`job_id = NONE` is the statement's own guard), so
            // under a failing run it is skipped like any other unstarted step.
            for step in steps.iter_mut() {
                if step.status != StepStatus::Dispatched || step.job_id.is_some() {
                    continue;
                }
                let mut binds = bind_ref("", &step.id).to_vec();
                binds.push(("error", never_dispatched()));
                let skipped = self.db.query(sql::SKIP_UNDISPATCHED_STEP, &binds).await?;
                if first_row(skipped).is_some() {
                    step.status = StepStatus::Skipped;
                }
            }
        }

        // 3. Dispatch whatever is ready now. Recompute statuses: step 2 may have
        //    skipped some of them.
        if !matches!(self.on_failure(&run), OnFailure::Halt) || !any_failed {
            let statuses: BTreeMap<String, StepStatus> =
                steps.iter().map(|s| (s.step.clone(), s.status)).collect();
            let outputs: BTreeMap<String, Value> = steps
                .iter()
                .filter(|s| s.status == StepStatus::Success)
                .filter_map(|s| s.output.clone().map(|o| (s.step.clone(), o)))
                .collect();

            // 3a. Recover steps that are past `blocked` but hold no job.
            //
            // Two shapes reach here, both from a pass that won a promotion and then
            // died (or hit a rejected write) before the job was stamped on the row:
            // `ready` with no job, and `dispatched` with no job. Neither is
            // reachable by anything else in this engine — `ready_steps` yields only
            // `blocked` steps, `CLAIM_STEP_READY` guards on `blocked`, and step 1
            // above needs a `job_id` to land anything — so a stranded step used to
            // hold its run `running`, and its key held, indefinitely.
            //
            // Re-dispatching is the recovery rather than a second execution:
            // `dispatch_step` mints a DETERMINISTIC job id, creates it through
            // `create_if_absent`, and `MARK_STEP_DISPATCHED` tolerates `dispatched`.
            // Worst case the job row already exists and the step is stamped with the
            // id it would have used anyway.
            for step in steps.iter_mut() {
                let stranded = match step.status {
                    StepStatus::Ready => true,
                    StepStatus::Dispatched => step.job_id.is_none(),
                    _ => false,
                };
                if !stranded {
                    continue;
                }
                let Some(step_def) = dag.step(&step.step) else { continue };
                // The same predicate `ready_steps` applies. However a step got
                // here, it must not run before its dependencies have succeeded.
                if !step_def
                    .depends_on
                    .iter()
                    .all(|dep| statuses.get(dep).copied() == Some(StepStatus::Success))
                {
                    continue;
                }

                if step.dispatch_attempts >= MAX_DISPATCH_ATTEMPTS {
                    // Give up, so the run can terminalize instead of looping. Which
                    // statement applies depends on which status the step stranded
                    // in; both are guarded so neither can touch a step that has
                    // since moved on.
                    let error = json!({
                        "code": "lost_dispatch",
                        "reason": format!(
                            "step could not be dispatched in {MAX_DISPATCH_ATTEMPTS} attempts",
                        ),
                        "attempts": step.dispatch_attempts,
                    });
                    let mut binds = bind_ref("", &step.id).to_vec();
                    binds.push(("error", error));
                    let stmt = match step.status {
                        StepStatus::Ready => sql::FAIL_UNDISPATCHABLE_STEP,
                        _ => sql::FAIL_UNDISPATCHED_STEP,
                    };
                    self.db.query(stmt, &binds).await?;
                    step.status = StepStatus::Failed;
                    tracing::warn!(
                        run = %wf_run,
                        step = %step.step,
                        attempts = step.dispatch_attempts,
                        "gave up dispatching a workflow step",
                    );
                    continue;
                }

                // Charge the attempt BEFORE trying it. An attempt that dies partway
                // still has to count, or a deterministically failing dispatch is
                // retried on every sweep forever.
                let charged =
                    self.db.query(sql::BUMP_STEP_DISPATCH_ATTEMPT, &bind_ref("", &step.id)).await?;
                let Some(charged) = first_row(charged) else { continue };
                step.dispatch_attempts =
                    charged.get("dispatch_attempts").and_then(Value::as_i64).unwrap_or(step.dispatch_attempts + 1);
                tracing::warn!(
                    run = %wf_run,
                    step = %step.step,
                    status = step.status.as_str(),
                    attempt = step.dispatch_attempts,
                    "re-dispatching a workflow step that lost its job",
                );
                self.dispatch_step(wf_run, &run, step_def, &step.id, &outputs).await?;
                step.status = StepStatus::Dispatched;
            }

            let ready: Vec<&StepDef> = dag.ready_steps(&statuses).collect();
            for step_def in ready {
                let Some(row) = steps.iter().find(|s| s.step == step_def.name) else { continue };
                // Winning this CAS is the right to dispatch. A concurrent pass
                // that loses it skips the step and never creates a second job.
                let claimed = self.db.query(sql::CLAIM_STEP_READY, &bind_ref("", &row.id)).await?;
                if first_row(claimed).is_none() {
                    continue;
                }
                self.dispatch_step(wf_run, &run, step_def, &row.id, &outputs).await?;
            }
        }

        // 4. Finalize when nothing can move any more.
        //
        // `!steps.is_empty()` is load-bearing: `Iterator::all` is `true` for an EMPTY
        // list and `find(Failed)` is then `None`, so a run whose step rows had vanished
        // would be reported a clean `success` — announcing work that provably never
        // finished. A run always has at least one step (an empty DAG is rejected at
        // validation), so no rows means something deleted them, not that there was
        // nothing to do.
        let steps = self.load_step_rows(wf_run).await?;
        let all_terminal = !steps.is_empty() && steps.iter().all(|s| s.status.is_terminal());
        if all_terminal {
            match steps.iter().find(|s| s.status == StepStatus::Failed) {
                Some(step) => {
                    self.fail_workflow_run(
                        wf_run,
                        json!({ "code": "step_failed", "step": step.step }),
                    )
                    .await?
                }
                None => self.complete_workflow_run(wf_run).await?,
            }
        }
        Ok(())
    }

    /// Create the step's job row and mark the step dispatched.
    async fn dispatch_step(
        &self,
        wf_run: &ids::Ref,
        run: &Value,
        step: &StepDef,
        step_row: &ids::Ref,
        outputs: &BTreeMap<String, Value>,
    ) -> anyhow::Result<()> {
        let run_key = &wf_run.key;
        let table = step
            .table
            .as_deref()
            .or_else(|| run.get("target_table").and_then(Value::as_str))
            .map(str::to_string);
        let Some(table) = table else {
            // The step is still `ready` here, so this needs the ready-guarded
            // statement rather than the dispatched-guarded finalize.
            let mut binds = bind_ref("", step_row).to_vec();
            binds.push((
                "error",
                json!({
                    "code": "no_target_table",
                    "reason": "step names no outbox table and the schedule has no target_table",
                }),
            ));
            self.db.query(sql::FAIL_UNDISPATCHABLE_STEP, &binds).await?;
            return Ok(());
        };

        // The attempt number comes off the run row this pass already loaded, so a
        // retried step gets a fresh job id rather than colliding with its old row.
        let attempt = run.get("retry_count").and_then(Value::as_i64).unwrap_or(0);
        let job = ids::step_job_attempt(&table, run_key, &step.name, attempt);

        // Only the direct dependencies' outputs are injected, so a long chain
        // can't grow an ever-larger payload.
        let deps: BTreeMap<String, Value> = step
            .depends_on
            .iter()
            .filter_map(|dep| outputs.get(dep).map(|out| (dep.clone(), out.clone())))
            .collect();
        let mut payload = build_payload(step.payload.as_ref(), None, Some(&deps));
        if let Some(input) = run.get("input") {
            if let Value::Object(map) = &mut payload {
                map.insert("input".into(), input.clone());
            }
        }

        let content = build_job_content(
            &step.path,
            payload,
            step.max_retries,
            step.retry_strategy.as_deref(),
            step.timeout,
        );

        // Deterministic job id: if the process died between this CREATE and the
        // MARK below, the retry collides here, and the step is stamped with the
        // id it would have used anyway.
        let mut binds = bind_ref("", &job).to_vec();
        binds.push(("content", content));
        self.create_if_absent(sql::CREATE_JOB, &binds).await?;

        let mut binds = bind_ref("", step_row).to_vec();
        binds.push(("job_id", json!(job.as_string())));
        self.db.query(sql::MARK_STEP_DISPATCHED, &binds).await?;

        // Best-effort, and separate on purpose: `started_at` is newer than the table
        // it lives on, so a deployment whose schema has not caught up rejects it. That
        // must cost a timestamp, never the step — by this point the job row exists, so
        // a failure here that took the whole statement with it would strand a real job
        // with no step pointing at it.
        if let Err(e) = self.db.query(sql::STAMP_STEP_STARTED, &bind_ref("", step_row)).await {
            tracing::debug!(step = %step_row, error = %e, "could not stamp a step's start time");
        }
        Ok(())
    }

    /// Kill a run an operator asked to stop: skip what hasn't started, kill the
    /// jobs of everything in flight, then terminalize.
    async fn kill_workflow_run(&self, wf_run: &ids::Ref, run: &Value) -> anyhow::Result<()> {
        self.stop_workflow_run(
            wf_run,
            run,
            "killed",
            json!({ "code": "killed", "reason": "killed by operator" }),
        )
        .await
    }

    /// Stop a running run: skip every step that has not started, kill the job of
    /// every step that has, and terminalize the run and its owning schedule run
    /// with `status` and `error`.
    ///
    /// Parameterized because there are two reasons to stop a run and they must not
    /// be reported as the same thing. An operator kill is `killed by operator`; the
    /// reaper's is a blown deadline, and labelling that one "killed by operator"
    /// tells whoever reads the row later that a human did this. The mechanics are
    /// identical, so they stay one implementation — a reaped run whose step jobs
    /// kept running would defeat the point of reaping it.
    pub(crate) async fn stop_workflow_run(
        &self,
        wf_run: &ids::Ref,
        run: &Value,
        status: &str,
        error: Value,
    ) -> anyhow::Result<()> {
        let steps = self.load_step_rows(wf_run).await?;
        // The run's own reason says what happened to the RUN; a step that never
        // started needs to say that about itself, or a `skipped` step on a reaped
        // run reads as though it ran and was thrown away.
        let not_started = json!({
            "code": "not_started",
            "reason": format!("the run was {status} before this step started"),
            "run_error": error.clone(),
        });
        for step in &steps {
            match step.status {
                StepStatus::Blocked | StepStatus::Ready => {
                    let mut binds = bind_ref("", &step.id).to_vec();
                    binds.push(("error", not_started.clone()));
                    self.db.query(sql::SKIP_STEP, &binds).await?;
                }
                StepStatus::Dispatched => {
                    match step.job_id.as_deref() {
                        Some(job_id) => {
                            if let Err(e) = self.kill.kill(job_id).await {
                                tracing::warn!(job_id, error = %e, "could not kill workflow step job");
                            }
                        }
                        // Dispatched with no job: it never started, so there is
                        // nothing to kill and it is safe to skip. Without this it
                        // would survive the stop and hold the run non-terminal.
                        None => {
                            let mut binds = bind_ref("", &step.id).to_vec();
                            binds.push(("error", never_dispatched()));
                            self.db.query(sql::SKIP_UNDISPATCHED_STEP, &binds).await?;
                        }
                    }
                }
                _ => {}
            }
        }
        let mut patch = Map::new();
        patch.insert("status".into(), json!(status));
        patch.insert("error".into(), error.clone());
        let mut binds = bind_ref("", wf_run).to_vec();
        binds.push(("patch", Value::Object(patch)));
        self.db.query(sql::FINALIZE_WORKFLOW_RUN, &binds).await?;
        // The schedule run carries the same reason, not a bare status: it is the row
        // `spky schedules runs` and the dashboard read, and a terminal run with no
        // error there is the exact gap that made a wedge look like a quiet skip.
        self.finalize_owning_schedule_run(wf_run, run, status, Some(error)).await
    }

    /// Flag a run for the engine to kill on its next pass. Used by
    /// `concurrency: replace` and by `spky workflows kill`.
    pub async fn request_workflow_kill(&self, wf_run: &ids::Ref) -> anyhow::Result<()> {
        self.db.query(sql::REQUEST_WORKFLOW_KILL, &bind_ref("", wf_run)).await?;
        // Act immediately when we're already the engine; the flag is the durable
        // fallback if this pass dies mid-way.
        if let Some(run) = self.load_workflow_run(wf_run).await? {
            if run.get("status").and_then(Value::as_str) == Some("running") {
                self.kill_workflow_run(wf_run, &run).await?;
            }
        }
        Ok(())
    }

    /// Re-examine every `running` workflow run, and honour any pending kill.
    /// This is the polling fallback for a lost job-completion event.
    ///
    /// One run's failure never aborts the pass. That is the same rule the rest of
    /// the sweep already follows — "one bad schedule never aborts the sweep" — and
    /// getting it wrong here was worse than anywhere else: a run whose advancement
    /// throws every time (a step whose job CREATE is rejected, say) would take down
    /// `reap_pass` and `prune_pass` with it on every single sweep, including the
    /// reaper whose whole job is to clear that run. The one thing that must not be
    /// blocked by a stuck run is the machinery for unsticking it.
    pub(crate) async fn heal_workflow_runs(
        &self,
        report: &mut crate::engine::TickReport,
    ) -> anyhow::Result<usize> {
        let mut healed = 0;

        for id in rows(self.db.query(sql::SELECT_KILL_REQUESTED, &[]).await?) {
            let Some(wf_run) = id.as_str().and_then(ids::Ref::parse) else { continue };
            let run = match self.load_workflow_run(&wf_run).await {
                Ok(Some(run)) => run,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(run = %wf_run, error = %e, "could not read a run to kill");
                    report.errored += 1;
                    continue;
                }
            };
            match self.kill_workflow_run(&wf_run, &run).await {
                Ok(()) => healed += 1,
                Err(e) => {
                    tracing::warn!(run = %wf_run, error = %e, "could not honour a kill request");
                    report.errored += 1;
                }
            }
        }

        for id in rows(self.db.query(sql::SELECT_RUNNING_WORKFLOW_RUNS, &[]).await?) {
            let Some(wf_run) = id.as_str().and_then(ids::Ref::parse) else { continue };
            match self.advance_workflow(&wf_run).await {
                Ok(()) => healed += 1,
                Err(e) => {
                    tracing::warn!(run = %wf_run, error = %e, "could not advance a run");
                    report.errored += 1;
                }
            }
        }
        Ok(healed)
    }

    // -- small helpers ------------------------------------------------------

    pub(crate) async fn load_workflow_run(&self, wf_run: &ids::Ref) -> anyhow::Result<Option<Value>> {
        let results = self.db.query(sql::SELECT_WORKFLOW_RUN, &bind_ref("", wf_run)).await?;
        Ok(match results.into_iter().next() {
            Some(v @ Value::Object(_)) => Some(v),
            _ => None,
        })
    }

    async fn load_step_rows(&self, wf_run: &ids::Ref) -> anyhow::Result<Vec<StepRow>> {
        let raw = rows(self.db.query(sql::SELECT_STEP_RUNS, &bind_ref("wf", wf_run)).await?);
        Ok(raw
            .into_iter()
            .filter_map(|row| {
                Some(StepRow {
                    id: row_ref(&row)?,
                    step: row.get("step").and_then(Value::as_str)?.to_string(),
                    status: row
                        .get("status")
                        .and_then(Value::as_str)
                        .and_then(StepStatus::parse)?,
                    job_id: row.get("job_id").and_then(Value::as_str).map(str::to_string),
                    output: row.get("output").cloned().filter(|v| !v.is_null()),
                    dispatch_attempts: row
                        .get("dispatch_attempts")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                })
            })
            .collect())
    }

    fn frozen_dag(&self, run: &Value) -> anyhow::Result<WorkflowDag> {
        let dag = run.get("dag").ok_or_else(|| anyhow::anyhow!("workflow run has no dag"))?;
        let def = serde_json::from_value(dag.clone())?;
        Ok(WorkflowDag::validate(&def)?)
    }

    fn on_failure(&self, run: &Value) -> OnFailure {
        run.get("dag")
            .and_then(|dag| dag.get("on_failure"))
            .and_then(Value::as_str)
            .and_then(|s| match s {
                "continue-independent" => Some(OnFailure::ContinueIndependent),
                "halt" => Some(OnFailure::Halt),
                _ => None,
            })
            .unwrap_or_default()
    }

    async fn finalize_step(
        &self,
        step_row: &ids::Ref,
        status: &str,
        output: Option<Value>,
        error: Option<Value>,
    ) -> anyhow::Result<()> {
        let mut patch = Map::new();
        patch.insert("status".into(), json!(status));
        if let Some(output) = output.filter(|v| !v.is_null()) {
            patch.insert("output".into(), output);
        }
        if let Some(error) = error {
            patch.insert("error".into(), error);
        }
        let mut binds = bind_ref("", step_row).to_vec();
        binds.push(("patch", Value::Object(patch)));
        self.db.query(sql::FINALIZE_STEP, &binds).await?;
        Ok(())
    }

    async fn complete_workflow_run(&self, wf_run: &ids::Ref) -> anyhow::Result<()> {
        let mut patch = Map::new();
        patch.insert("status".into(), json!("success"));
        let mut binds = bind_ref("", wf_run).to_vec();
        binds.push(("patch", Value::Object(patch)));
        // `RETURN AFTER` rather than a re-read: by the time this returns the row may
        // be on its way out, and reading it back to find `schedule_name` is what
        // would strand the owning schedule run as `running` forever.
        let finalized = self.db.query(sql::FINALIZE_WORKFLOW_RUN, &binds).await?;
        let Some(run) = first_row(finalized) else { return Ok(()) };
        self.finalize_owning_schedule_run(wf_run, &run, "success", None).await?;
        // Strictly last: everything above has already taken what it needs off the row.
        self.discard_workflow_if_failures_only(wf_run, &run).await;
        Ok(())
    }

    async fn fail_workflow_run(&self, wf_run: &ids::Ref, error: Value) -> anyhow::Result<()> {
        let mut patch = Map::new();
        patch.insert("status".into(), json!("failed"));
        patch.insert("error".into(), error.clone());
        let mut binds = bind_ref("", wf_run).to_vec();
        binds.push(("patch", Value::Object(patch)));
        // Same shape as the success path. A failed run is never discarded — its rows are
        // the whole reason `failures-only` exists.
        let finalized = self.db.query(sql::FINALIZE_WORKFLOW_RUN, &binds).await?;
        if let Some(run) = first_row(finalized) {
            self.finalize_owning_schedule_run(wf_run, &run, "failed", Some(error)).await?;
        }
        Ok(())
    }

    /// Terminalize a fire whose workflow-run skeleton could not be written.
    ///
    /// Ordering matters. `fail_workflow_run` already mirrors its outcome onto the
    /// owning schedule run, but it can only do so when the workflow run row
    /// exists — and that row is one of the two things that may have failed. So the
    /// schedule run is finalized directly as well. Both statements are guarded on
    /// `running`, so the second is a no-op whenever the first got there first, and
    /// neither can relabel a run something else already finished.
    ///
    /// Best-effort on purpose: the caller returns the original error either way,
    /// and a failure to record the failure must not mask it.
    async fn fail_unspawnable_run(
        &self,
        wf_run: &ids::Ref,
        run_key: &str,
        cause: &anyhow::Error,
    ) {
        // `{cause:#}` walks the context chain — the outermost context alone is
        // rarely the part that says which write was rejected.
        let error = json!({ "code": "spawn_failed", "reason": format!("{cause:#}") });
        if let Err(e) = self.fail_workflow_run(wf_run, error.clone()).await {
            tracing::warn!(run = %wf_run, error = %e, "could not fail an unspawnable workflow run");
        }
        let sched_run = ids::schedule_run(run_key);
        if let Err(e) = self.finalize_schedule_run(&sched_run, "failed", Some(error)).await {
            tracing::warn!(run = %sched_run, error = %e, "could not fail an unspawnable schedule run");
        }
    }

    /// Under `history: failures-only`, a workflow run that succeeded leaves nothing
    /// behind: its steps, its step jobs and the run row itself all go.
    ///
    /// Called ONLY from the success path. `kill_workflow_run` and the `bad_dag` failure
    /// finalize a run while steps are still `dispatched`/`blocked`, so "the run
    /// finalized" does not imply "its steps are terminal" — only `success` does, because
    /// it is gated on `all_terminal`.
    ///
    /// Everything that reads the run has already run by this point: the outcome is
    /// mirrored onto the owning schedule run, and the rollup is folded here BEFORE the
    /// delete, because afterwards there is nothing left to count.
    async fn discard_workflow_if_failures_only(&self, wf_run: &ids::Ref, run: &Value) {
        if run.get("history_mode").and_then(Value::as_str) != Some(HISTORY_FAILURES_ONLY) {
            return;
        }

        // `_00_workflow_run` has no `fire_at`, so the hourly bucket comes off
        // `created_at`. An ad-hoc `spky workflows trigger` run has no owning schedule;
        // count it under the same placeholder the prune's fold uses rather than dropping
        // it on the floor.
        let name =
            run.get("schedule_name").and_then(Value::as_str).unwrap_or(ROLLUP_NAME_AD_HOC);
        let at = run
            .get("created_at")
            .and_then(Value::as_str)
            .and_then(crate::cron::parse_datetime)
            .unwrap_or_else(Utc::now);
        self.fold_rollup_one(run, at, SCOPE_SCHEDULE, "success", name).await;

        // The step rows carry the job ids, so they have to be read before they go. Their
        // `output` was copied onto them when each step finalized, so nothing downstream
        // needs the job rows any more.
        let steps = rows(
            self.db
                .query(sql::SELECT_STEP_RUNS, &bind_ref("wf", wf_run))
                .await
                .unwrap_or_default(),
        );
        let jobs: Vec<String> = steps
            .iter()
            .filter_map(|s| s.get("job_id").and_then(Value::as_str))
            .filter_map(ids::Ref::parse)
            .filter(|j| {
                sql::is_plain_identifier(&j.table) && sql::is_plain_identifier(&j.key)
            })
            .map(|j| j.as_string())
            .collect();
        if !jobs.is_empty() {
            if let Err(e) = self.db.query(&sql::delete_records(&jobs), &[]).await {
                tracing::warn!(error = %e, "could not discard a workflow's job rows");
            }
        }

        // Steps and run together, steps first — see DELETE_WORKFLOW_RUN_CASCADE.
        if let Err(e) =
            self.db.query(sql::DELETE_WORKFLOW_RUN_CASCADE, &bind_ref("", wf_run)).await
        {
            // Leaving the rows behind is harmless: retention collects them later.
            tracing::warn!(run = %wf_run, error = %e, "could not discard a successful workflow run");
        }
    }

    /// Mirror a workflow run's outcome onto the schedule run that spawned it.
    /// The link is derived from the shared run key rather than read back, so a
    /// crash before `LINK_WORKFLOW_RUN` can't orphan the history row.
    async fn finalize_owning_schedule_run(
        &self,
        wf_run: &ids::Ref,
        run: &Value,
        status: &str,
        error: Option<Value>,
    ) -> anyhow::Result<()> {
        // Ad-hoc runs (`spky workflows trigger`) have no owning schedule.
        if run.get("schedule_name").and_then(Value::as_str).is_none() {
            return Ok(());
        }
        // The run key is shared, so the sibling history row is derivable — no
        // need to read the link back, which a crash could have left unwritten.
        self.finalize_schedule_run(&ids::schedule_run(&wf_run.key), status, error).await
    }

    /// `CREATE` that treats a duplicate id as success — the deterministic-id
    /// idempotency check, in the one place every creator can share.
    pub(crate) async fn create_if_absent(
        &self,
        stmt: &str,
        binds: &[(&str, Value)],
    ) -> anyhow::Result<bool> {
        match self.db.query(stmt, binds).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_already_exists() => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// The cause recorded on a step that reached `dispatched` without ever getting a
/// job row. It is the one skip that is not about the DAG at all — the step's own
/// dispatch was lost — so saying "an ancestor failed" would point the reader at
/// the wrong half of the system.
fn never_dispatched() -> Value {
    json!({
        "code": "never_dispatched",
        "reason": "the step was promoted but its job row was never created, \
                   and the run stopped before recovery could re-dispatch it",
    })
}

/// Step names as a reader would say them: `a`, `a and b`, `a, b and c`.
///
/// A recorded reason is read by a person, and `["a", "b"]` in the middle of a
/// sentence is not a sentence.
fn list(names: &[&str]) -> String {
    match names {
        [] => "a step".to_string(),
        [one] => (*one).to_string(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

#[cfg(test)]
mod reason_tests {
    use super::list;

    #[test]
    fn step_lists_read_as_prose() {
        assert_eq!(list(&[]), "a step");
        assert_eq!(list(&["extract"]), "extract");
        assert_eq!(list(&["extract", "load"]), "extract and load");
        assert_eq!(list(&["a", "b", "c"]), "a, b and c");
    }
}
