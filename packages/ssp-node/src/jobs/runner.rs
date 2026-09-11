use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::dispatcher::JobDispatcher;
use super::types::{JobControl, JobEntry};
use crate::api::Method;
use crate::ports::{Db, HttpClient, HttpError, OutboundRequest, Scheduler, Spawner};

/// SQL boolean expression (for recovery-sweep `WHERE` clauses): is a pending job
/// DUE now? A job is due at `created_at + <delay>`; `??` falls back when `delay`
/// is unset (NONE), and `delay = 0` ⇒ ready immediately. Shared by the SSP
/// singlenode sweep and the cluster scheduler sweep so the two never drift, and
/// exercised directly by tests.
///
/// Recurring schedules used to add a `next_run_at ??` arm here. They are now
/// declarative and server-side (`schedules:` in sp00ky.yml): the schedule engine
/// keeps its own clock in `_00_schedule.next_fire_at` and spawns a NEW job row
/// per cycle, so an outbox row is once again always a single execution.
pub const PENDING_DUE_CLAUSE: &str =
    "(created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now()";

/// Slack added to a job's own timeout to get its lease length.
///
/// The lease must always outlive the request it covers, or a perfectly healthy
/// long-running job gets reclaimed and run twice. This covers connection setup, the
/// runner's own write latency either side of the request, and clock skew between
/// SSPs — all of which sit outside the HTTP deadline itself.
const LEASE_GRACE_SECS: u64 = 30;

/// Ceiling on a lease, whatever the job asked for.
///
/// `BackendInfo::effective_timeout` applies no upper bound, so a row carrying
/// `timeout: 4294967295` would mint a lease lasting longer than the deployment. That
/// is the bug this whole mechanism removes, arriving by a different door: an
/// unreclaimable row. A day is far longer than any real job and still finite.
const MAX_LEASE_SECS: u64 = 24 * 60 * 60;

/// How long this attempt may hold the row before it becomes reclaimable.
pub fn lease_secs(timeout: Duration) -> u64 {
    timeout.as_secs().saturating_add(LEASE_GRACE_SECS).min(MAX_LEASE_SECS)
}

/// How long a claim keeps waiting for a job row that does not exist yet.
///
/// Live pickup is driven by the outbox table's mutation event, and SurrealDB runs
/// that event INSIDE the transaction that creates the row: the `/ingest` post goes
/// out before the CREATE commits, and in cluster mode the scheduler acknowledges it
/// before fanning it out. So a job reached through its own CREATE routinely gets to
/// the claim while the row is still invisible to this node's connection. The claim
/// CAS then matches nothing, which is indistinguishable from "claimed elsewhere",
/// and the job used to be dropped until the recovery sweep found it 30-60 s later.
///
/// The creating transaction normally commits within milliseconds of its event, and
/// the DB-side `/ingest` call itself is bounded at 10 s. A row still absent after
/// this was most likely rolled back; one that commits even later is left to the
/// recovery sweep, exactly as before. The slot is held while waiting, so this is
/// also the most a rolled-back job can cost its table.
pub const CLAIM_VISIBILITY_BUDGET: Duration = Duration::from_secs(10);
const CLAIM_VISIBILITY_FIRST_BACKOFF: Duration = Duration::from_millis(10);
const CLAIM_VISIBILITY_MAX_BACKOFF: Duration = Duration::from_millis(250);

/// Guard the `type::record($id)` conversion: SurrealDB record ids are
/// `table:key`. The former implementation used `RecordId::parse_simple`;
/// this keeps the same reject-garbage-early behavior without the SDK type.
fn validate_job_id(job_id: &str) -> Result<()> {
    let (table, key) = job_id
        .split_once(':')
        .with_context(|| format!("Invalid job ID: {}", job_id))?;
    if table.is_empty() || key.is_empty() {
        anyhow::bail!("Invalid job ID: {}", job_id);
    }
    Ok(())
}

/// Everything executing a job needs, minus the receiver. Split out from
/// [`JobRunner`] so the loop can hand an `Arc` of it to each spawned execution.
pub struct JobRunnerCtx {
    db: Arc<dyn Db>,
    http: Arc<dyn HttpClient>,
    scheduler: Arc<dyn Scheduler>,
    spawner: Arc<dyn Spawner>,
    dispatcher: Arc<JobDispatcher>,
    job_control: JobControl,
}

pub struct JobRunner {
    queue_rx: mpsc::Receiver<JobEntry>,
    ctx: Arc<JobRunnerCtx>,
}

impl JobRunner {
    pub fn new(
        queue_rx: mpsc::Receiver<JobEntry>,
        db: Arc<dyn Db>,
        http: Arc<dyn HttpClient>,
        scheduler: Arc<dyn Scheduler>,
        spawner: Arc<dyn Spawner>,
        dispatcher: Arc<JobDispatcher>,
    ) -> Self {
        let job_control = dispatcher.job_control().clone();
        Self {
            queue_rx,
            ctx: Arc::new(JobRunnerCtx {
                db,
                http,
                scheduler,
                spawner,
                dispatcher,
                job_control,
            }),
        }
    }

    /// The execution context, for callers that need to run a job directly.
    pub fn ctx(&self) -> &Arc<JobRunnerCtx> {
        &self.ctx
    }

    /// Run the job runner loop.
    ///
    /// The loop dispatches and moves on rather than awaiting each job: how many
    /// run at once is the [`JobDispatcher`]'s decision, and nothing reaches this
    /// receiver without a permit. Awaiting here would pin every table in the
    /// deployment to one concurrent job no matter what the policy says.
    pub async fn run(mut self) {
        info!("Job runner started");

        while let Some(job) = self.queue_rx.recv().await {
            debug!(job_id = %job.id, path = %job.path, "Processing job");

            let ctx = Arc::clone(&self.ctx);
            let table = job.table.clone();
            self.ctx.spawner.spawn(Box::pin(async move {
                if let Err(e) = ctx.execute_job(job).await {
                    error!(error = %e, "Error executing job");
                }
                // The permit went with the entry, so the slot is free by now.
                ctx.dispatcher.kick_drain(&table);
            }));
        }

        info!("Job runner stopped");
    }

    /// Execute one job on this runner's context, bypassing the queue.
    ///
    /// Test-only: production always arrives through `run()`, which is what
    /// guarantees a job carries the permit that bounds concurrency.
    #[cfg(test)]
    pub(crate) async fn execute_job(&self, job: JobEntry) -> Result<()> {
        self.ctx.execute_job(job).await
    }
}

impl JobRunnerCtx {
    /// Execute a single job
    pub(crate) async fn execute_job(&self, mut job: JobEntry) -> Result<()> {
        // Killed-while-pending: an operator called /job/kill before this job was
        // dequeued. Fail it terminally without ever firing the request.
        //
        // Guarded on `pending` rather than fenced on a lease epoch, because this runs
        // BEFORE the claim — this attempt owns no epoch yet, and the row's own epoch
        // belongs to whatever claimed it last. `pending` is the right question anyway:
        // it is precisely "nothing has claimed this", and it fixes what the previous
        // unguarded write only worried about in a comment, that a row which raced to
        // `processing` elsewhere could be clobbered from here.
        if self.job_control.take_killed_pending(&job.id) {
            info!(job_id = %job.id, "Job killed while pending — failing without execution");
            let error_entry = json!({ "code": "killed", "reason": "killed by operator" });
            // One statement for the error and the status, so a kill cannot land half
            // applied. Release the in-flight mark even if it fails — leaking it wedges
            // the id against every future re-enqueue.
            let killed =
                fail_if_pending_helper(self.db.as_ref(), &job.id, error_entry).await;
            self.job_control.clear_enqueued(&job.id);
            if !killed? {
                // Already claimed or already terminal. Whoever holds it owns the
                // outcome; the kill flag has done all it can from here.
                info!(job_id = %job.id, "Kill arrived after the job left `pending` — leaving it alone");
            }
            return Ok(());
        }

        // Claim the row: `pending` -> `processing`, stamping the lease this attempt
        // holds it under and the fencing token every later write of ours is guarded on.
        //
        // This used to be an unguarded `SET status = 'processing'`, so two nodes could
        // both "start" the same row. The CAS closes that, and losing it is not an
        // error — someone else owns the attempt, so this one must not run.
        //
        // On a DB failure, release the in-flight mark before bailing (same leak hazard
        // as above): the row is still pending, so the sweep can re-dispatch it cleanly.
        //
        // The claim waits for a row that is not visible yet: a job arriving through
        // its own CREATE event gets here before the creating transaction commits.
        // See `CLAIM_VISIBILITY_BUDGET`.
        let owner = self.dispatcher.ssp_id().to_string();
        match claim_processing_when_visible(
            self.db.as_ref(),
            self.scheduler.as_ref(),
            &job.id,
            &owner,
            lease_secs(job.timeout),
            CLAIM_VISIBILITY_BUDGET,
        )
        .await
        {
            Ok(claim @ (Claim::Fenced(_) | Claim::Unfenced)) => job.lease_epoch = claim.epoch(),
            Ok(Claim::Lost) => {
                // Not ours. Nothing to undo — we never wrote to the row, and whoever
                // does hold it owns the outcome.
                info!(job_id = %job.id, "Job is not pending (claimed elsewhere, or already terminal) — not executing");
                self.job_control.clear_enqueued(&job.id);
                return Ok(());
            }
            Err(e) => {
                self.job_control.clear_enqueued(&job.id);
                return Err(e);
            }
        }

        // Build URL
        let url = format!("{}{}", job.base_url, job.path);

        debug!(job_id = %job.id, url = %url, "Sending HTTP request");

        // Parse payload if it's a string containing JSON
        let payload = match &job.payload {
            serde_json::Value::String(s) => {
                serde_json::from_str(s).unwrap_or_else(|_| job.payload.clone())
            }
            _ => job.payload.clone(),
        };

        // Register cancellation for the duration of the in-flight request so
        // `/job/kill` on a 'processing' job can abort it. The HttpClient port
        // guarantees cancel WINS ties against a response that completed in the
        // same poll (the old `select! { biased; … }` semantics).
        let cancel = self.job_control.register_inflight(&job.id);

        let outcome = self
            .http
            .send(
                OutboundRequest {
                    method: Method::Post,
                    url,
                    bearer: job.auth_token.clone(),
                    headers: vec![],
                    json_body: Some(payload),
                    timeout: job.timeout,
                },
                Some(cancel),
            )
            .await;

        // Always release the registration; see `JobControl::release_inflight`.
        self.job_control.release_inflight(&job.id);

        match outcome {
            Err(HttpError::Cancelled) => {
                info!(job_id = %job.id, "Job cancelled by operator — marking failed");
                let error_entry = json!({ "code": "cancelled", "reason": "killed by operator" });
                self.append_error_reported(&job.table, &job.id, error_entry, job.lease_epoch).await;
                // An operator kill is terminal: do NOT run handle_failure, so the
                // backoff-retry path never re-queues a job the operator killed.
                self.terminalize(&job.id, "failed", job.lease_epoch).await?;
                self.job_control.clear_enqueued(&job.id);
            }
            Ok(response) if (200..300).contains(&response.status) => {
                info!(job_id = %job.id, status = response.status, "Job completed successfully");
                self.complete_success(&job.id, &response.body, job.lease_epoch).await?;
                self.job_control.clear_enqueued(&job.id);
            }
            Ok(response) => {
                warn!(
                    job_id = %job.id,
                    status = response.status,
                    error_body = %response.body,
                    "Job request failed with non-success status"
                );

                // Create error entry with code and reason
                let error_entry = json!({
                    "code": response.status,
                    "reason": response.body
                });

                self.handle_failure(job, Some(error_entry)).await?;
            }
            Err(e) => {
                warn!(job_id = %job.id, error = %e, "Job request failed");

                // "The backend never answered" is the failure this whole mechanism
                // exists for, so it gets its own code rather than sharing `0` with
                // every transport error and being distinguishable only by prose.
                let code = match &e {
                    HttpError::Timeout(_) => json!("timeout"),
                    _ => json!(0),
                };
                let error_entry = json!({ "code": code, "reason": e.to_string() });

                self.handle_failure(job, Some(error_entry)).await?;
            }
        }

        Ok(())
    }

    /// Handle job failure - retry or mark as failed
    ///
    /// The DB bookkeeping in here (error append, retry counter) is
    /// best-effort: a `?` on those writes used to abort the whole handler,
    /// which skipped BOTH the retry scheduling and `clear_enqueued` — the job
    /// then sat marked in-flight on this SSP forever, and every later
    /// `/job/recover` bounced off `mark_enqueued` as "already queued" (a
    /// permanently wedged job the sweep could never rescue). The retry budget
    /// is enforced from the in-memory `job.retries` either way.
    async fn handle_failure(&self, mut job: JobEntry, error_entry: Option<serde_json::Value>) -> Result<()> {
        job.retries += 1;

        // Append error to database if provided (best-effort, see above)
        if let Some(error) = error_entry {
            self.append_error_reported(&job.table, &job.id, error, job.lease_epoch).await;
        }

        // Persist the incremented attempt count regardless of outcome, so a job
        // that exhausts its budget ends at retries == max_retries. Best-effort:
        // the enforced budget is the in-memory job.retries.
        if let Err(e) = self.increment_retries(&job.id, job.lease_epoch).await {
            // `{e:#}` not `{e}`: these are `anyhow` errors carrying a `.context`,
            // and the plain Display shows ONLY the outermost context — which is
            // how the real cause stayed invisible here for so long.
            warn!(job_id = %job.id, error = format!("{e:#}"), "Failed to persist retry count (continuing)");
        }

        if job.retries < job.max_retries {
            // Calculate delay based on retry strategy
            let delay = calculate_delay(job.retries, &job.retry_strategy);

            info!(
                job_id = %job.id,
                retries = job.retries,
                max_retries = job.max_retries,
                delay_ms = delay.as_millis(),
                "Job will be retried"
            );

            // Release the execution slot for the duration of the backoff. A job
            // asleep on a timer is not running, and holding its permit would
            // mean a table with `concurrency: 1` sits idle through every
            // backoff — a regression against the serial runner this replaces.
            job.permit = None;

            // Requeue with delay: a fire-and-forget task that sleeps through the
            // backoff. Lost on restart/eviction by design — the recovery sweep
            // re-picks the pending row (deadlines live in SurrealQL, not here).
            let dispatcher = Arc::clone(&self.dispatcher);
            let db = Arc::clone(&self.db);
            let scheduler = Arc::clone(&self.scheduler);
            let job_control = self.job_control.clone();
            let job_id = job.id.clone();
            let table = job.table.clone();
            let epoch = job.lease_epoch;
            self.spawner.spawn(Box::pin(async move {
                scheduler.sleep(delay).await;

                // Back to pending before re-queueing — fenced, because this write
                // happens after a sleep and is therefore the most likely of all of
                // them to land late. If the lease expired during the backoff and the
                // row was reclaimed and re-run, an unfenced write here would drag the
                // NEW attempt back to `pending` and hand this dead attempt's job to
                // the queue a second time.
                match update_status_fenced(db.as_ref(), &job_id, "pending", epoch).await {
                    Ok(true) => {}
                    Ok(false) => {
                        fenced(&job_id, epoch, "retry requeue");
                        job_control.clear_enqueued(&job_id);
                        return;
                    }
                    Err(e) => {
                        error!(job_id = %job_id, error = %e, "Failed to update status for retry");
                        return;
                    }
                }

                // Re-queue the job — through admission, so a retry cannot push
                // the table over its limit. Refused means the slots are full:
                // release the in-flight mark and let the row take its turn in
                // the backlog like any other pending row. The attempt count
                // survives on the row (`increment_retries` above), which is how
                // the recovery path has always rebuilt it.
                if !dispatcher.try_admit(job).await {
                    job_control.clear_enqueued(&job_id);
                    dispatcher.note_backlog(&table);
                    debug!(job_id = %job_id, "Retry deferred to the backlog");
                }
            }));
        } else {
            warn!(
                job_id = %job.id,
                retries = job.retries,
                "Job exceeded max retries - marking as failed"
            );
            // Terminal: the retry branch above deliberately keeps the enqueued
            // mark (the job stays in flight across the backoff), but here the
            // job is done, so release it for any future re-enqueue — even when
            // the status write fails (see doc comment).
            let status = self.terminalize(&job.id, "failed", job.lease_epoch).await;
            self.job_control.clear_enqueued(&job.id);
            status?;
        }

        Ok(())
    }

    /// A terminal status write by the attempt that is running the job, fenced on its
    /// own lease epoch. A fenced-out write is reported and swallowed: the row belongs
    /// to a later attempt, and this one has nothing left to say about it.
    async fn terminalize(&self, job_id: &str, status: &str, epoch: Option<i64>) -> Result<()> {
        if !update_status_fenced(self.db.as_ref(), job_id, status, epoch).await? {
            fenced(job_id, epoch, status);
        }
        Ok(())
    }

    async fn append_error(&self, job_id: &str, error: serde_json::Value, epoch: Option<i64>) -> Result<()> {
        append_error_helper(self.db.as_ref(), job_id, error, epoch).await
    }

    /// `append_error`, with the failure reported instead of dropped.
    ///
    /// The append itself stays best-effort — see `handle_failure` for why a `?`
    /// here is not survivable — but a lost append is not a cosmetic loss: the
    /// engine reads `errors.last()` and it is the ONLY source for a step's or a
    /// schedule run's `error`, so a swallowed append is why a failed job could
    /// report no reason at all.
    ///
    /// The usual cause is a SCHEMAFULL outbox missing its `errors[*]` element
    /// declaration, which SurrealDB rejects at statement level with
    /// `Found field 'errors[0].code', but no such field exists`. That is one
    /// condition per table, so it is reported at `error!` with the remediation
    /// the first time and at `warn!` afterwards.
    async fn append_error_reported(
        &self,
        table: &str,
        job_id: &str,
        error: serde_json::Value,
        epoch: Option<i64>,
    ) {
        let Err(e) = self.append_error(job_id, error, epoch).await else { return };
        // `{e:#}` walks the whole `anyhow` context chain. The plain `%e` this
        // replaced printed only "Failed to append error" and never once named
        // the statement-level rejection underneath it.
        let error = format!("{e:#}");
        if self.job_control.note_append_failure(table) {
            error!(
                job_id = %job_id,
                table = %table,
                error = %error,
                "Failed to append job error — failures on this table will record no reason. \
                 If this says a field does not exist, the outbox is missing `errors[*] FLEXIBLE`: \
                 run `spky deploy` (or `spky migrate`) to add it."
            );
        } else {
            warn!(job_id = %job_id, table = %table, error = %error, "Failed to append job error (continuing)");
        }
    }

    async fn complete_success(&self, job_id: &str, body: &str, epoch: Option<i64>) -> Result<()> {
        complete_success_helper(self.db.as_ref(), job_id, body, epoch).await
    }

    async fn increment_retries(&self, job_id: &str, epoch: Option<i64>) -> Result<()> {
        validate_job_id(job_id)?;
        let (sql, binds) =
            fenced_write(job_id, epoch, "SET retries = retries + 1, updated_at = time::now()");
        let results =
            self.db.query(&sql, &binds).await.context("Failed to increment retries")?;
        if !updated_any(&results) {
            fenced(job_id, epoch, "retry count");
        }
        Ok(())
    }
}

/// Cap on the stored size of a job's captured `result`. A backend that streams
/// back a megabyte of rows should not bloat every SELECT of the outbox table (or
/// the payload of a workflow step that depends on it). Bodies past the cap are
/// stored as a marker object instead — the job still succeeds, and the operator
/// sees why the output is missing. A const rather than an env var because the
/// portable core reads the process environment zero times (see `NodeConfig`).
pub const JOB_RESULT_MAX_BYTES: usize = 64 * 1024;

/// Terminalize a successful job AND capture the backend's response body in
/// `result`.
///
/// The body is stored as parsed JSON when it parses (so `result.fileId` works in
/// a dependent workflow step) and as a plain string otherwise.
///
/// Outbox tables are user-owned and usually SCHEMAFULL, and `result` only
/// arrived with the platform-field injection in `build_outbox_platform_fields`.
/// A project that upgraded the stack without re-applying its schema therefore
/// has no such field, and SurrealDB rejects the WHOLE update — which would wedge
/// job completion, the one thing that must never happen. So a failed write falls
/// back to a status-only update: such a job completes normally, it just has no
/// captured output.
pub async fn complete_success_helper(
    db: &dyn Db,
    job_id: &str,
    body: &str,
    epoch: Option<i64>,
) -> Result<()> {
    validate_job_id(job_id)?;
    let result = encode_job_result(body);
    // Two statements rather than one with a nullable guard. Binding JSON `null` for
    // "no fence" does NOT work: it arrives as SurrealDB's NULL, `NULL = NONE` is
    // false, and the guard then matches nothing — the write is silently dropped
    // instead of being unguarded. Same trap the sql.rs module header documents.
    let (sql, binds) = fenced_write(
        job_id,
        epoch,
        "SET status = 'success', result = $result, updated_at = time::now()",
    );
    let mut binds = binds;
    binds.push(("result", result));
    let attempt = db.query(&sql, &binds).await;

    match attempt {
        Ok(results) => {
            if !updated_any(&results) {
                fenced(job_id, epoch, "success");
            }
            Ok(())
        }
        Err(crate::ports::DbError::Query(e)) => {
            warn!(
                job_id = %job_id,
                error = %e,
                "Could not store job result (is the outbox schema up to date?) — \
                 completing the job without it"
            );
            if !update_status_fenced(db, job_id, "success", epoch).await? {
                fenced(job_id, epoch, "success");
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// One place to report a write this attempt was not allowed to make.
///
/// It means the row was reclaimed while this attempt was running: its lease expired,
/// something re-ran the job, and the epoch moved. Dropping the write is correct — the
/// re-run owns the outcome now — but it must not be SILENT, because it is also what a
/// lease that is too short looks like from the outside. If this appears routinely, the
/// lease is being cut before the work finishes.
fn fenced(job_id: &str, epoch: Option<i64>, what: &str) {
    warn!(
        job_id = %job_id,
        epoch = ?epoch,
        write = %what,
        "Discarding a job write: this attempt's lease was reclaimed and the job re-run \
         elsewhere. If this is not rare, the lease is shorter than the work."
    );
}

/// Response body → the value stored in `result`: parsed JSON when it parses, a
/// plain string otherwise, or a marker when it exceeds [`JOB_RESULT_MAX_BYTES`].
fn encode_job_result(body: &str) -> Value {
    if body.len() > JOB_RESULT_MAX_BYTES {
        return json!({
            "truncated": true,
            "bytes": body.len(),
            "limit": JOB_RESULT_MAX_BYTES,
        });
    }
    if body.trim().is_empty() {
        return Value::Null;
    }
    serde_json::from_str(body).unwrap_or_else(|_| json!(body))
}

/// The outcome of trying to take a `pending` row for execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claim {
    /// Claimed, and every write this attempt makes must be fenced on this epoch.
    Fenced(i64),
    /// Claimed, but the outbox table has no lease fields yet — a project that
    /// upgraded the stack without re-applying its schema. Such a row gets exactly
    /// today's unfenced semantics until the next `spky deploy` adds the fields.
    Unfenced,
    /// The row was not `pending`: already claimed elsewhere, or already terminal.
    /// A normal outcome, not an error — the caller must simply not execute.
    Lost,
}

impl Claim {
    /// The fencing token to guard this attempt's writes with, or `None` when the
    /// table predates leases.
    pub fn epoch(self) -> Option<i64> {
        match self {
            Claim::Fenced(epoch) => Some(epoch),
            _ => None,
        }
    }
}

/// Claim a `pending` row for execution: `processing`, plus the lease this attempt
/// holds it under and the fencing token it must present to write its outcome.
///
/// The lease is minted per ATTEMPT, not per row. A job that fails and backs off
/// returns to `pending` and comes through here again, so a three-attempt job never
/// needs a lease covering all three; each attempt gets a fresh window and a fresh
/// token. That is what makes a static lease sufficient and heartbeat renewal
/// unnecessary.
///
/// Outbox tables are user-owned and usually SCHEMAFULL, and the lease fields only
/// arrived with the platform-field injection in `build_outbox_platform_fields`. On a
/// table that has not been re-deployed, SurrealDB rejects this whole statement — so a
/// rejection falls back to a claim that writes no lease, exactly as
/// `complete_success_helper` falls back when `result` is missing. Wedging every job on
/// an un-migrated table would be a far worse failure than the one leases fix.
///
/// `<duration>(string::concat(...))` is the only way to build a duration from a bound
/// number in SurrealQL — a duration cannot be multiplied by an int parameter.
pub async fn claim_processing(
    db: &dyn Db,
    job_id: &str,
    assignee: &str,
    lease_secs: u64,
) -> Result<Claim> {
    validate_job_id(job_id)?;
    let attempt = db
        .query(
            "UPDATE type::record($id) SET status = 'processing', assignee = $me, \
             lease_epoch = (lease_epoch ?? 0) + 1, \
             lease_until = time::now() + <duration>(string::concat(<string>$lease, 's')), \
             updated_at = time::now() \
             WHERE status = 'pending' RETURN AFTER",
            &[
                ("id", json!(job_id)),
                ("me", json!(assignee)),
                ("lease", json!(lease_secs)),
            ],
        )
        .await;

    match attempt {
        Ok(results) => Ok(match claimed_epoch(&results) {
            Some(epoch) => Claim::Fenced(epoch),
            // Matched nothing: not pending.
            None if !updated_any(&results) => Claim::Lost,
            // Matched, but the epoch is unreadable. Claim it unfenced rather than
            // fencing every later write against a token we do not have, which would
            // silently discard the job's outcome.
            None => Claim::Unfenced,
        }),
        Err(crate::ports::DbError::Query(e)) => {
            warn!(
                job_id = %job_id,
                error = %e,
                "Could not lease this job (is the outbox schema up to date? run `spky deploy`) — \
                 claiming it without a lease; it falls back to the old staleness rule"
            );
            let results = db
                .query(
                    "UPDATE type::record($id) SET status = 'processing', \
                     updated_at = time::now() \
                     WHERE status = 'pending' RETURN AFTER",
                    &[("id", json!(job_id))],
                )
                .await
                .context("Failed to claim job")?;
            Ok(if updated_any(&results) { Claim::Unfenced } else { Claim::Lost })
        }
        Err(e) => Err(anyhow::Error::from(e).context("Failed to claim job")),
    }
}

/// [`claim_processing`], tolerant of a row whose creating transaction has not
/// committed yet.
///
/// A `Lost` from the CAS conflates two answers: "the row is not pending" and "there
/// is no row". Only the first means another attempt owns the job or it already
/// finished. The second is the normal state of a job reached through its own CREATE
/// event (see [`CLAIM_VISIBILITY_BUDGET`]), so it is retried with a short backoff
/// until the row appears or `budget` runs out.
///
/// Nothing here can double-execute: every retry is the same CAS on `pending`, so a
/// row that becomes visible already claimed by someone else still comes back `Lost`.
pub async fn claim_processing_when_visible(
    db: &dyn Db,
    scheduler: &dyn Scheduler,
    job_id: &str,
    assignee: &str,
    lease_secs: u64,
    budget: Duration,
) -> Result<Claim> {
    let mut waited = Duration::ZERO;
    let mut backoff = CLAIM_VISIBILITY_FIRST_BACKOFF;
    loop {
        let claim = claim_processing(db, job_id, assignee, lease_secs).await?;
        if claim != Claim::Lost {
            if !waited.is_zero() {
                debug!(
                    job_id = %job_id,
                    waited_ms = waited.as_millis() as u64,
                    "Claimed a job once its creating transaction committed"
                );
            }
            return Ok(claim);
        }

        // The CAS matched nothing. A row that exists and is not `pending` really is
        // someone else's, or finished. A row that turned up `pending` between the two
        // reads is simply claimed again; only an absent one is waited for.
        if let Some(row) = load_job_record(db, job_id).await? {
            if row.get("status").and_then(Value::as_str) != Some("pending") {
                return Ok(Claim::Lost);
            }
        }

        if waited >= budget {
            warn!(
                job_id = %job_id,
                waited_ms = waited.as_millis() as u64,
                "Job row never became claimable (creating transaction rolled back, or \
                 slower than the claim budget). Leaving it to the recovery sweep."
            );
            return Ok(Claim::Lost);
        }
        let step = backoff.min(budget.saturating_sub(waited)).max(Duration::from_millis(1));
        scheduler.sleep(step).await;
        waited += step;
        backoff = (backoff * 2).min(CLAIM_VISIBILITY_MAX_BACKOFF);
    }
}

/// `lease_epoch` out of a claim's `RETURN AFTER`, or `None` when nothing matched.
///
/// The result of a single-row `UPDATE` arrives either as a one-element array or as a
/// bare object depending on the plan, and as `null`/empty when the guard did not
/// match — the same tolerance `fail_if_pending_helper` needs.
fn claimed_epoch(results: &[Value]) -> Option<i64> {
    let first = results.first()?;
    let row = match first {
        Value::Array(rows) => rows.first()?,
        Value::Object(_) => first,
        _ => return None,
    };
    // A claim that matched but whose epoch is unreadable would fence every later
    // write of this attempt against a token it does not have, silently dropping the
    // job's outcome. Treating it as "not claimed" is the safe reading: the row stays
    // `processing` and the reclaim picks it up once the lease runs out.
    row.get("lease_epoch").and_then(Value::as_i64)
}

/// Hand a `processing` row whose lease has EXPIRED back to the queue.
///
/// Three things in one statement, and each one is load-bearing:
///
/// - it re-checks the expiry, so a row that renewed (or terminalized) between the
///   sweep's SELECT and this write is left alone. The singlenode reset used to be
///   unguarded entirely and could flip a finished row back to `pending`;
/// - it clears `assignee`, because `JobDispatcher::claim` only takes rows with
///   `assignee = NONE OR assignee = $me`. A reclaimed row that kept a dead node's id
///   would be un-drainable by every other node — reclaiming it would produce a
///   `pending` row nobody can pick up;
/// - it bumps `lease_epoch`, which fences the original attempt. If that attempt is
///   still alive and later tries to report an outcome, its write now matches no row
///   instead of overwriting the re-run.
///
/// Returns `true` when a row was actually reclaimed.
pub async fn reclaim_expired_lease(db: &dyn Db, job_id: &str) -> Result<bool> {
    validate_job_id(job_id)?;
    let results = db
        .query(
            &format!(
                "UPDATE type::record($id) SET status = 'pending', assignee = NONE, \
                 lease_until = NONE, lease_epoch = (lease_epoch ?? 0) + 1, \
                 updated_at = time::now() \
                 WHERE status = 'processing' AND {expired} RETURN AFTER",
                expired = schedule_core::sql::LEASE_EXPIRED,
            ),
            &[("id", json!(job_id))],
        )
        .await
        .context("Failed to reclaim an expired job lease")?;
    Ok(updated_any(&results))
}

/// Build one of this attempt's writes: the `SET` clause, fenced on `epoch` when there
/// is one to fence on.
///
/// The fence has to be present or absent in the STATEMENT, not expressed as a
/// nullable parameter. A bound JSON `null` becomes SurrealDB's NULL, `NULL = NONE` is
/// false, and `(lease_epoch ?? 0) = NULL` is false too — so a "no fence" sentinel bound
/// that way matches nothing and drops the write entirely. That is the failure mode this
/// helper exists to make unrepresentable.
fn fenced_write(
    job_id: &str,
    epoch: Option<i64>,
    set_clause: &str,
) -> (String, Vec<(&'static str, Value)>) {
    let mut binds: Vec<(&'static str, Value)> = vec![("id", json!(job_id))];
    match epoch {
        Some(epoch) => {
            binds.push(("epoch", json!(epoch)));
            (
                format!(
                    "UPDATE type::record($id) {set_clause} \
                     WHERE (lease_epoch ?? 0) = $epoch RETURN AFTER"
                ),
                binds,
            )
        }
        // Pre-lease table: nothing to fence against, so this is today's statement.
        None => (format!("UPDATE type::record($id) {set_clause} RETURN AFTER"), binds),
    }
}

/// Did a guarded single-row `UPDATE` match anything? Tolerates the array / bare-object
/// / null shapes SurrealDB returns depending on the plan.
fn updated_any(results: &[Value]) -> bool {
    match results.first() {
        Some(Value::Array(rows)) => !rows.is_empty(),
        Some(Value::Null) | None => false,
        Some(_) => true,
    }
}

/// Helper function to update status (used by both JobRunner and spawned tasks)
///
/// UNFENCED, and only for callers that own the row outright: the recovery sweeps and
/// the `/job/*` handlers. An attempt reporting its own outcome must use
/// [`update_status_fenced`] instead, or a reclaimed job's original attempt can
/// overwrite the re-run that replaced it.
pub async fn update_status_helper(db: &dyn Db, job_id: &str, status: &str) -> Result<()> {
    validate_job_id(job_id)?;
    db.query(
        "UPDATE type::record($id) SET status = $status, updated_at = time::now()",
        &[("id", json!(job_id)), ("status", json!(status))],
    )
    .await
    .context("Failed to update status")?;
    Ok(())
}

/// [`update_status_helper`], guarded on the caller's fencing token.
///
/// Returns `false` when the row has moved on to a later epoch, i.e. this attempt was
/// reclaimed while it was running and something else now owns the job. The caller must
/// treat that as "my work no longer counts" and stop — not retry, and not fight for
/// the row.
pub async fn update_status_fenced(
    db: &dyn Db,
    job_id: &str,
    status: &str,
    epoch: Option<i64>,
) -> Result<bool> {
    validate_job_id(job_id)?;
    let (sql, binds) = fenced_write(job_id, epoch, "SET status = $status, updated_at = time::now()");
    let mut binds = binds;
    binds.push(("status", json!(status)));
    let results = db.query(&sql, &binds).await.context("Failed to update status")?;
    Ok(updated_any(&results))
}

/// Persist the owning SSP node id (`ssp_id`) onto a job row so cluster recovery
/// can tell which SSP accepted a job (and re-dispatch only if that SSP dies).
/// Deliberately does **not** touch `updated_at`: the recovery staleness clock
/// must keep measuring from the row's last real status change, not from this
/// ownership stamp (which happens right after create). Written server-side
/// (root) only.
pub async fn set_assignee_helper(db: &dyn Db, job_id: &str, assignee: &str) -> Result<()> {
    validate_job_id(job_id)?;
    db.query(
        "UPDATE type::record($id) SET assignee = $assignee RETURN NONE",
        &[("id", json!(job_id)), ("assignee", json!(assignee))],
    )
    .await
    .context("Failed to set job assignee")?;
    Ok(())
}

/// Append an error object to a job's `errors` array. Shared by the runner and the
/// SSP's `/job/kill`/`/job/retry` handlers so every writer uses identical SQL.
pub async fn append_error_helper(
    db: &dyn Db,
    job_id: &str,
    error: Value,
    epoch: Option<i64>,
) -> Result<()> {
    validate_job_id(job_id)?;
    let (sql, binds) = fenced_write(
        job_id,
        epoch,
        "SET errors = array::append(errors, $error), updated_at = time::now()",
    );
    let mut binds = binds;
    binds.push(("error", error));
    let results = db.query(&sql, &binds).await.context("Failed to append error")?;
    if !updated_any(&results) {
        fenced(job_id, epoch, "error append");
    }
    Ok(())
}

/// Reset a terminal job for re-execution: `status='pending'`, `retries=0`, errors
/// cleared. Used by the SSP `/job/retry` handler before re-enqueueing the job.
pub async fn reset_for_retry_helper(db: &dyn Db, job_id: &str) -> Result<()> {
    validate_job_id(job_id)?;
    db.query(
        "UPDATE type::record($id) SET status = 'pending', retries = 0, errors = [], updated_at = time::now()",
        &[("id", json!(job_id))],
    )
    .await
    .context("Failed to reset job for retry")?;
    Ok(())
}

/// Atomically fail a job **only if it is still `pending`**, appending a `killed`
/// error. Used by `/job/kill` so an orphaned pending job (one that was never
/// enqueued — pickup is CREATE-only) is actually stopped, instead of merely
/// setting an in-memory flag that the runner can only honor at dequeue. The
/// `WHERE status = 'pending'` guard keeps the single-writer invariant intact:
/// it never clobbers a row the runner has already advanced to `processing`.
/// Returns `true` when a row was updated (it was pending), `false` otherwise.
pub async fn fail_if_pending_helper(db: &dyn Db, job_id: &str, error: Value) -> Result<bool> {
    validate_job_id(job_id)?;
    let results = db
        .query(
            "UPDATE type::record($id) SET status = 'failed', \
             errors = array::append(errors, $error), updated_at = time::now() \
             WHERE status = 'pending' RETURN AFTER",
            &[("id", json!(job_id)), ("error", error)],
        )
        .await
        .context("Failed to fail pending job")?;

    // First statement's flattened result: array of updated rows (possibly a
    // bare object for a single row, or null for none).
    let updated = match results.first() {
        Some(Value::Array(rows)) => !rows.is_empty(),
        Some(Value::Null) | None => false,
        Some(_) => true,
    };
    Ok(updated)
}

/// Load only the scalar fields of a job row we need. Returns `None` when the
/// job does not exist (or the id is malformed — mirrors the historical
/// RecordId-parse-to-None behavior); statement-level errors also map to
/// `None` (the row shape is wrong ⇒ treat as absent), transport/auth errors
/// surface as `Err`.
pub async fn load_job_record(db: &dyn Db, job_id: &str) -> Result<Option<Value>> {
    if validate_job_id(job_id).is_err() {
        return Ok(None);
    }
    let results = db
        .query(
            "SELECT status, path, payload, retries, max_retries, retry_strategy, timeout \
             FROM ONLY type::record($id)",
            &[("id", json!(job_id))],
        )
        .await;
    match results {
        Ok(values) => Ok(match values.into_iter().next() {
            Some(v @ Value::Object(_)) => Some(v),
            _ => None,
        }),
        Err(crate::ports::DbError::Query(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Mark + enqueue a recovered/re-dispatched job. Returns `false` when the job
/// is already queued or in-flight (the `mark_enqueued` guard), when the queue is
/// closed, or when the table is at its concurrency limit. Shared by the
/// singlenode recovery sweep and `/job/recover`.
///
/// A `false` from the limit is not a failure: the row stays `pending`, which is
/// where the backlog lives, and the drain will admit it in `created_at` order.
pub async fn enqueue_recovered(
    dispatcher: &Arc<JobDispatcher>,
    backend_info: &super::types::BackendInfo,
    id: &str,
    row: &Value,
) -> bool {
    let timeout_override = row.get("timeout").and_then(|v| v.as_u64()).map(|v| v as u32);
    let job_entry = JobEntry::from_record(
        id.to_string(),
        backend_info.base_url.clone(),
        backend_info.auth_token.clone(),
        row,
        backend_info.effective_timeout(timeout_override),
    );
    dispatcher.try_admit(job_entry).await
}

/// Calculate retry delay based on strategy
fn calculate_delay(retries: u32, strategy: &str) -> Duration {
    match strategy {
        "exponential" => {
            // Exponential backoff: 200ms * 2^retries (200ms, 400ms, 800ms, 1.6s...)
            let base_ms = 200u64;
            let multiplier = 2u64.saturating_pow(retries);
            Duration::from_millis(base_ms.saturating_mul(multiplier))
        }
        _ => {
            // Linear backoff: 200ms * (retries + 1) (200ms, 400ms, 600ms...)
            Duration::from_millis(200 * (retries as u64 + 1))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_job_id_accepts_table_colon_key() {
        assert!(validate_job_id("job:abc").is_ok());
        assert!(validate_job_id("job:⟨weird key⟩").is_ok());
        assert!(validate_job_id("nojcolon").is_err());
        assert!(validate_job_id(":nokey").is_err());
        assert!(validate_job_id("notable:").is_err());
    }

    #[test]
    fn delay_strategies() {
        assert_eq!(calculate_delay(1, "linear"), Duration::from_millis(400));
        assert_eq!(calculate_delay(2, "linear"), Duration::from_millis(600));
        assert_eq!(calculate_delay(1, "exponential"), Duration::from_millis(400));
        assert_eq!(calculate_delay(3, "exponential"), Duration::from_millis(1600));
    }

    #[test]
    fn from_record_reads_the_dispatch_fields_and_defaults_the_rest() {
        let e = JobEntry::from_record(
            "job:a".into(),
            "http://b".into(),
            None,
            &json!({
                "path": "/run",
                "payload": { "x": 1 },
                "retries": 2,
                "max_retries": 5,
                "retry_strategy": "exponential",
            }),
            Duration::from_secs(7),
        );
        assert_eq!(e.path, "/run");
        assert_eq!(e.retries, 2);
        assert_eq!(e.max_retries, 5);
        assert_eq!(e.retry_strategy, "exponential");
        assert_eq!(e.timeout, Duration::from_secs(7));

        // A sparse row falls back to the documented defaults.
        let e = JobEntry::from_record(
            "job:b".into(),
            "http://b".into(),
            None,
            &json!({ "path": "/run" }),
            Duration::from_secs(5),
        );
        assert_eq!(e.retries, 0);
        assert_eq!(e.max_retries, 3);
        assert_eq!(e.retry_strategy, "linear");
    }

    #[test]
    fn encodes_results_by_shape_and_size() {
        assert_eq!(encode_job_result("{\"a\":1}"), json!({"a": 1}), "JSON stays addressable");
        assert_eq!(encode_job_result("done"), json!("done"), "non-JSON becomes a string");
        assert_eq!(encode_job_result("   "), Value::Null, "an empty body stores nothing");
        let huge = "x".repeat(JOB_RESULT_MAX_BYTES + 1);
        assert_eq!(encode_job_result(&huge)["truncated"], json!(true));
    }
}
