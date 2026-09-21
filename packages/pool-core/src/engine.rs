//! The pool engine: one `tick_pass` per sweep, plus the four agent entry points.
//!
//! ```text
//! tick_pass, per pool
//!   ├─ reclaim     attempts whose lease ran out: requeue, or fail when out of budget
//!   ├─ deadline    attempts running past the pool's hard limit: fail
//!   ├─ liveness    machines that never booted, or stopped polling: take away
//!   ├─ occupancy   jobs bound to each machine (read from the outbox, cached on the row)
//!   ├─ roll        machines from an older deploy, or past their lifetime: drain
//!   ├─ assign      oldest due pending jobs -> free slots on fresh, current machines
//!   ├─ scale       create up to the desired size; drain idle surplus
//!   ├─ terminate   drained-and-empty machines; retry every pending destroy
//!   └─ orphans     provider machines with no live row (every Nth pass)
//! ```
//!
//! Every step reads what is and moves it toward what should be. None of them
//! depends on a previous pass having finished, so the process may die between any
//! two statements and the next pass simply carries on.
//!
//! **Ordering within a pass is deliberate.** Reclaim and liveness run before
//! assignment so freed jobs are re-assigned in the same pass; assignment runs
//! before scaling so the pool is sized for what is still queued, not for what
//! this pass just placed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pool_protocol::{
    Command, HelloReply, JobRef, Outcome, PollReply, PollRequest, ResultReply, ResultRequest,
};
use schedule_core::db::{count_value, first_row, rows};
use schedule_core::{ScheduleDb, ScheduleDbError};
use serde_json::{json, Value};

use crate::provider::{CreateMachine, MachineProvider, ProviderError};
use crate::sizing::{desired_machines, Demand};
use crate::spec::{MachineRow, MachineState, PoolSpec};
use crate::sql;

/// Tunables. Deliberate defaults, nothing read from the environment here.
#[derive(Debug, Clone)]
pub struct PoolEngineConfig {
    /// Most machines created for one pool in one pass. A runaway queue should
    /// ramp, not stampede the provider.
    pub create_cap_per_tick: u32,
    /// Consecutive failed boots that open the breaker.
    pub breaker_threshold: i64,
    /// First breaker pause; doubles per further failure up to the max.
    pub breaker_base_secs: i64,
    pub breaker_max_secs: i64,
    /// Slack past `max_job_duration_secs` before the engine, rather than the
    /// agent, fails an attempt. The agent enforces the deadline itself; this
    /// only catches an agent that did not.
    pub deadline_grace_secs: i64,
    /// Look for leaked provider machines every N passes.
    pub orphan_sweep_every: u64,
    /// How long terminal machine rows are kept for inspection.
    pub machine_retention_secs: i64,
    /// A job result larger than this is replaced by a marker (same cap as the
    /// job runner).
    pub result_max_bytes: usize,
}

impl Default for PoolEngineConfig {
    fn default() -> Self {
        Self {
            create_cap_per_tick: 5,
            breaker_threshold: 3,
            breaker_base_secs: 60,
            breaker_max_secs: 900,
            deadline_grace_secs: 120,
            orphan_sweep_every: 15,
            machine_retention_secs: 86_400,
            result_max_bytes: 64 * 1024,
        }
    }
}

/// A pool crossing the boot-failure breaker, either way. Reported, not logged:
/// the host owns incidents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolTransition {
    pub pool: String,
    pub open: bool,
    pub failures: i64,
    pub detail: String,
}

/// What one `tick_pass` did. Returned for logging and asserted on in tests.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    pub created: usize,
    pub create_failed: usize,
    pub assigned: usize,
    /// Attempts requeued after their lease expired.
    pub requeued: usize,
    /// Attempts failed for good: lease expired with no budget left, or overdue.
    pub failed_jobs: usize,
    /// Machines taken away because they never became ready.
    pub boot_timeouts: usize,
    /// Machines taken away because they stopped polling.
    pub lost: usize,
    pub drained: usize,
    pub destroyed: usize,
    pub orphans_destroyed: usize,
    /// Pools whose pass failed. Recorded on the pool's `last_error`; one bad
    /// pool never aborts the sweep.
    pub errored: usize,
    pub transitions: Vec<PoolTransition>,
}

/// Seconds between agent polls for a pool with this lease. Six polls per lease,
/// so one or two lost polls never cost a job.
pub fn poll_secs(lease_secs: i64) -> i64 {
    (lease_secs / 6).clamp(1, 10)
}

/// How recently a machine must have polled to be trusted with a new job.
pub fn fresh_secs(lease_secs: i64) -> i64 {
    poll_secs(lease_secs) * 3
}

/// How long before its lease expires an agent gives a job up on its own. The
/// agent therefore always stops before the engine may hand the job elsewhere.
pub fn stop_margin_secs(lease_secs: i64) -> i64 {
    (lease_secs / 3).clamp(1, 30)
}

pub struct PoolEngine {
    db: Arc<dyn ScheduleDb>,
    providers: BTreeMap<String, Arc<dyn MachineProvider>>,
    cfg: PoolEngineConfig,
    passes: AtomicU64,
}

type Binds<'a> = Vec<(&'a str, Value)>;

impl PoolEngine {
    pub fn new(
        db: Arc<dyn ScheduleDb>,
        providers: BTreeMap<String, Arc<dyn MachineProvider>>,
        cfg: PoolEngineConfig,
    ) -> Self {
        Self {
            db,
            providers,
            cfg,
            passes: AtomicU64::new(0),
        }
    }

    pub fn db(&self) -> &Arc<dyn ScheduleDb> {
        &self.db
    }

    // -- the sweep ------------------------------------------------------------

    /// One sweep over every pool. Only a failure to read the pool table itself
    /// propagates; anything that goes wrong inside one pool lands on that pool.
    pub async fn tick_pass(&self) -> anyhow::Result<TickReport> {
        let pass = self.passes.fetch_add(1, Ordering::Relaxed);
        let mut report = TickReport::default();

        for row in rows(self.db.query(sql::SELECT_POOLS, &[]).await?) {
            let spec = match PoolSpec::from_row(&row) {
                Ok(spec) => spec,
                Err(e) => {
                    report.errored += 1;
                    tracing::warn!(error = %e, "unreadable pool row");
                    continue;
                }
            };
            if let Err(e) = self.tick_pool(&spec, pass, &mut report).await {
                report.errored += 1;
                tracing::warn!(pool = %spec.name, error = format!("{e:#}"), "pool pass failed");
                let _ = self
                    .db
                    .query(
                        sql::RECORD_POOL_ERROR,
                        &[
                            ("name", json!(spec.name)),
                            ("error", json!(format!("{e:#}"))),
                        ],
                    )
                    .await;
            }
        }

        if pass.checked_rem(20) == Some(0) {
            let keep = [("keep", json!(self.cfg.machine_retention_secs))];
            if let Err(e) = self.db.query(sql::PRUNE_MACHINES, &keep).await {
                tracing::debug!(error = %e, "machine prune failed");
            }
        }
        Ok(report)
    }

    async fn tick_pool(
        &self,
        spec: &PoolSpec,
        pass: u64,
        report: &mut TickReport,
    ) -> anyhow::Result<()> {
        let table = spec.target_table.as_str();

        // -- reclaim + deadline: jobs first, so what they free is re-placed below.
        let expired = json!({ "code": "lease_expired",
            "reason": "the machine running this attempt stopped renewing its lease" });
        let exhausted = rows(
            self.db
                .query(
                    &sql::fail_expired_exhausted(table),
                    &[("error", expired.clone())],
                )
                .await?,
        );
        let requeued = rows(
            self.db
                .query(&sql::requeue_expired(table), &[("error", expired)])
                .await?,
        );
        let overdue = rows(
            self.db
                .query(
                    &sql::fail_overdue(table),
                    &[
                        (
                            "error",
                            json!({ "code": "deadline",
                            "reason": "ran past the pool's max job duration" }),
                        ),
                        (
                            "limit",
                            json!(spec.max_job_duration_secs + self.cfg.deadline_grace_secs),
                        ),
                    ],
                )
                .await?,
        );
        report.requeued += requeued.len();
        report.failed_jobs += exhausted.len() + overdue.len();

        // -- machines, with every clock evaluated by the database.
        let mut machines = self.live_machines(spec).await?;

        // -- liveness.
        let provider = self.providers.get(&spec.provider).cloned();
        let mut boot_failures = spec.boot_failures;
        let mut breaker_open = spec.breaker_open;
        for m in machines.iter_mut() {
            let (from, reason): (&[&str], &str) = match m.state {
                MachineState::Requested | MachineState::Booting if m.boot_overdue => (
                    &["requested", "booting"],
                    "never became ready within the boot timeout",
                ),
                MachineState::Ready | MachineState::Draining if m.heartbeat_lost => {
                    (&["ready", "draining"], "stopped polling for a whole lease")
                }
                _ => continue,
            };
            if !self.begin_terminate(&m.id, from, reason, true).await? {
                continue; // raced with the agent (it just became ready / polled)
            }
            if matches!(m.state, MachineState::Requested | MachineState::Booting) {
                report.boot_timeouts += 1;
                boot_failures += 1;
                breaker_open |= self
                    .record_boot_failure(spec, boot_failures, reason, breaker_open, report)
                    .await;
            } else {
                report.lost += 1;
            }
            m.state = MachineState::Terminating;
        }

        // -- occupancy, straight from the outbox.
        let ready_ids: Vec<&str> = machines
            .iter()
            .filter(|m| matches!(m.state, MachineState::Ready | MachineState::Draining))
            .map(|m| m.id.as_str())
            .collect();
        let mut occupancy: BTreeMap<String, u32> = BTreeMap::new();
        if !ready_ids.is_empty() {
            let found = rows(
                self.db
                    .query(
                        &sql::select_occupancy(table),
                        &[("machines", json!(ready_ids))],
                    )
                    .await?,
            );
            for row in found {
                if let (Some(id), Some(n)) = (
                    row.get("assignee").and_then(Value::as_str),
                    row.get("n").and_then(Value::as_i64),
                ) {
                    occupancy.insert(id.to_string(), n.max(0) as u32);
                }
            }
        }
        for m in machines.iter().filter(|m| m.state == MachineState::Ready) {
            let busy = occupancy.get(&m.id).copied().unwrap_or(0);
            if busy == 0 && (m.busy_slots != 0 || !m.has_idle_since) {
                self.db
                    .query(sql::SET_OCCUPANCY_IDLE, &[("id", json!(m.id))])
                    .await?;
            } else if busy > 0 && (m.busy_slots != busy || m.has_idle_since) {
                self.db
                    .query(
                        sql::SET_OCCUPANCY_BUSY,
                        &[("id", json!(m.id)), ("busy", json!(busy))],
                    )
                    .await?;
            }
        }

        // -- roll: an older deploy, or too old. Finish what you have, take nothing new.
        for m in machines
            .iter_mut()
            .filter(|m| m.state == MachineState::Ready)
        {
            if (m.spec_hash != spec.spec_hash || m.lifetime_over)
                && self.move_machine(&m.id, &["ready"], "draining").await?
            {
                m.state = MachineState::Draining;
                report.drained += 1;
            }
        }

        // -- assign.
        let mut queued =
            count_value(self.db.query(&sql::count_queued(table), &[]).await?).max(0) as u32;
        if !spec.paused && queued > 0 {
            let assigned = self.assign(spec, &machines, &mut occupancy).await?;
            report.assigned += assigned;
            queued = queued.saturating_sub(assigned as u32);
        }

        // -- scale.
        let supply = machines.iter().filter(|m| m.state.is_supply()).count() as u32;
        let busy_slots: u32 = machines
            .iter()
            .filter(|m| m.state == MachineState::Ready)
            .map(|m| occupancy.get(&m.id).copied().unwrap_or(0))
            .sum();
        let desired = desired_machines(
            spec,
            Demand {
                busy_slots,
                queued_jobs: queued,
            },
        );

        if desired > supply && !spec.paused && !breaker_open {
            // Draining and terminating machines are on their way out and do not
            // count against the ceiling (that is the surge a roll needs). But if
            // destroys are failing they pile up, and then creating more is how a
            // stuck provider turns into a bill: stop at twice the ceiling.
            let live = machines.len() as u32;
            let hard_stop = spec.ceiling().saturating_mul(2).saturating_add(1);
            let room = hard_stop.saturating_sub(live);
            let want = (desired - supply)
                .min(self.cfg.create_cap_per_tick)
                .min(room);
            match provider.as_ref() {
                None if want > 0 => {
                    anyhow::bail!("no `{}` machine provider is configured", spec.provider)
                }
                None => {}
                Some(provider) => {
                    for _ in 0..want {
                        match self.create_machine(spec, provider.as_ref()).await {
                            Ok(()) => report.created += 1,
                            Err(e) => {
                                report.create_failed += 1;
                                boot_failures += 1;
                                let detail = format!("create failed: {e}");
                                breaker_open |= self
                                    .record_boot_failure(
                                        spec,
                                        boot_failures,
                                        &detail,
                                        breaker_open,
                                        report,
                                    )
                                    .await;
                                if breaker_open {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        } else if supply > desired {
            // Longest idle first. Never a machine holding a job, never below desired.
            let mut surplus = supply - desired;
            for m in machines.iter_mut() {
                if surplus == 0 {
                    break;
                }
                let idle = m.state == MachineState::Ready
                    && occupancy.get(&m.id).copied().unwrap_or(0) == 0
                    && m.idle_expired;
                if idle && self.move_machine(&m.id, &["ready"], "draining").await? {
                    m.state = MachineState::Draining;
                    report.drained += 1;
                    surplus -= 1;
                }
            }
        }

        // -- requested rows nobody finished creating (a crash between the row and
        //    the provider call). Same id, so the provider hands back the same machine.
        if let Some(provider) = provider.as_ref() {
            for m in machines
                .iter()
                .filter(|m| m.state == MachineState::Requested && m.settled)
            {
                let _ = self.ensure_created(spec, &m.id, provider.as_ref()).await;
            }
        }

        // -- terminate: drained and empty, then every destroy still owed.
        for m in machines
            .iter_mut()
            .filter(|m| m.state == MachineState::Draining)
        {
            if occupancy.get(&m.id).copied().unwrap_or(0) == 0
                && self
                    .begin_terminate(&m.id, &["draining"], "drained", false)
                    .await?
            {
                m.state = MachineState::Terminating;
            }
        }
        if let Some(provider) = provider.as_ref() {
            for m in machines
                .iter()
                .filter(|m| m.state == MachineState::Terminating)
            {
                match provider.destroy(&m.id, m.provider_id.as_deref()).await {
                    Ok(()) => {
                        self.db
                            .query(sql::FINISH_TERMINATE, &[("id", json!(m.id))])
                            .await?;
                        report.destroyed += 1;
                    }
                    // Stays `terminating`; the next pass asks again.
                    Err(e) => {
                        tracing::warn!(machine = %m.id, error = %e, "destroy failed, will retry")
                    }
                }
            }

            if pass.checked_rem(self.cfg.orphan_sweep_every.max(1)) == Some(0) {
                report.orphans_destroyed += self.destroy_orphans(spec, provider.as_ref()).await;
            }
        }

        Ok(())
    }

    async fn live_machines(&self, spec: &PoolSpec) -> anyhow::Result<Vec<MachineRow>> {
        let binds = [
            ("pool", json!(spec.name)),
            ("boot", json!(spec.boot_timeout_secs)),
            ("lease", json!(spec.lease_secs)),
            ("fresh", json!(fresh_secs(spec.lease_secs))),
            ("idle", json!(spec.idle_timeout_secs)),
            ("life", json!(spec.max_lifetime_secs)),
        ];
        Ok(
            rows(self.db.query(sql::SELECT_LIVE_MACHINES, &binds).await?)
                .iter()
                .filter_map(MachineRow::from_row)
                .collect(),
        )
    }

    /// Place the oldest due jobs on free slots. Only machines that are ready, on
    /// the current deploy and polling: a job handed to a machine that then never
    /// polls costs a whole lease before it is reclaimed.
    async fn assign(
        &self,
        spec: &PoolSpec,
        machines: &[MachineRow],
        occupancy: &mut BTreeMap<String, u32>,
    ) -> anyhow::Result<usize> {
        let mut free: Vec<(&MachineRow, u32)> = machines
            .iter()
            .filter(|m| m.state == MachineState::Ready && m.fresh && m.spec_hash == spec.spec_hash)
            .filter_map(|m| {
                let busy = occupancy.get(&m.id).copied().unwrap_or(0);
                (m.slots > busy).then(|| (m, m.slots - busy))
            })
            .collect();
        // Fullest first: packing leaves whole machines idle, and only a wholly
        // idle machine can be scaled down.
        free.sort_by_key(|(m, left)| (*left, m.id.clone()));
        let total: u32 = free.iter().map(|(_, left)| *left).sum();
        if total == 0 {
            return Ok(0);
        }

        let queued = rows(
            self.db
                .query(
                    &sql::select_queued(&spec.target_table),
                    &[("n", json!(total))],
                )
                .await?,
        );
        let mut jobs = queued
            .iter()
            .filter_map(|r| r.get("id").and_then(Value::as_str));
        let mut assigned = 0;
        'machines: for (machine, left) in free {
            for _ in 0..left {
                // A lost claim means someone else took that row (an operator kill,
                // a second ticker): try the next job for this same slot.
                loop {
                    let Some(job) = jobs.next() else {
                        break 'machines;
                    };
                    let claimed = self
                        .db
                        .query(
                            sql::CLAIM_JOB,
                            &[
                                ("id", json!(job)),
                                ("machine", json!(machine.id)),
                                ("lease", json!(spec.lease_secs)),
                            ],
                        )
                        .await?;
                    if first_row(claimed).is_some() {
                        *occupancy.entry(machine.id.clone()).or_insert(0) += 1;
                        assigned += 1;
                        break;
                    }
                }
            }
        }
        if assigned > 0 {
            for (id, busy) in occupancy.iter() {
                let _ = self
                    .db
                    .query(
                        sql::SET_OCCUPANCY_BUSY,
                        &[("id", json!(id)), ("busy", json!(busy))],
                    )
                    .await;
            }
        }
        Ok(assigned)
    }

    async fn create_machine(
        &self,
        spec: &PoolSpec,
        provider: &dyn MachineProvider,
    ) -> Result<(), ProviderError> {
        let created = self
            .db
            .query(
                sql::CREATE_MACHINE,
                &[
                    ("pool", json!(spec.name)),
                    ("provider", json!(spec.provider)),
                    ("slots", json!(spec.slots)),
                    ("hash", json!(spec.spec_hash)),
                ],
            )
            .await
            .map_err(|e| ProviderError::Other(format!("could not record the machine: {e}")))?;
        let id = first_row(created)
            .and_then(|r| r.get("id").and_then(Value::as_str).map(str::to_string))
            .ok_or_else(|| ProviderError::Other("machine row was created without an id".into()))?;

        match self.ensure_created(spec, &id, provider).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Nothing exists at the provider (or it will be found as an orphan);
                // file the row straight away rather than waiting out a boot timeout.
                let reason = format!("create failed: {e}");
                let _ = self
                    .begin_terminate(&id, &["requested"], &reason, true)
                    .await;
                Err(e)
            }
        }
    }

    /// Ask the provider for the machine behind an existing row. Idempotent on the
    /// row id, which is what makes re-running it after a crash safe.
    async fn ensure_created(
        &self,
        spec: &PoolSpec,
        machine_id: &str,
        provider: &dyn MachineProvider,
    ) -> Result<(), ProviderError> {
        let made = provider
            .create(&CreateMachine {
                machine_id: machine_id.to_string(),
                pool: spec.name.clone(),
                machine_type: spec.machine_type.clone(),
                locations: spec.locations.clone(),
                slots: spec.slots,
                container: spec.container.clone(),
            })
            .await?;
        let db_err =
            |e: ScheduleDbError| ProviderError::Other(format!("could not record the machine: {e}"));
        self.db
            .query(
                sql::SET_PROVIDER_ID,
                &[
                    ("id", json!(machine_id)),
                    ("provider_id", json!(made.provider_id)),
                ],
            )
            .await
            .map_err(db_err)?;
        // The agent may already have reported ready; then this CAS simply misses.
        self.db
            .query(
                sql::MOVE_MACHINE,
                &[
                    ("id", json!(machine_id)),
                    ("to", json!("booting")),
                    ("from", json!(["requested"])),
                ],
            )
            .await
            .map_err(db_err)?;
        Ok(())
    }

    /// Destroy provider machines that have no live row. Each candidate's row is
    /// read individually first: a list or a scan that came back short must never
    /// be what gets a healthy machine destroyed.
    async fn destroy_orphans(&self, spec: &PoolSpec, provider: &dyn MachineProvider) -> usize {
        let listed = match provider.list(&spec.name).await {
            Ok(listed) => listed,
            Err(e) => {
                tracing::debug!(pool = %spec.name, error = %e, "could not list provider machines");
                return 0;
            }
        };
        let mut destroyed = 0;
        for pm in listed {
            let row = match self
                .db
                .query(sql::SELECT_MACHINE, &[("id", json!(pm.machine_id))])
                .await
            {
                Ok(found) => first_row(found),
                Err(_) => continue, // cannot tell: leave it alone
            };
            let live = row
                .as_ref()
                .and_then(|r| r.get("state").and_then(Value::as_str))
                .and_then(MachineState::parse)
                .map(MachineState::is_live)
                .unwrap_or(false);
            if live {
                continue;
            }
            match provider
                .destroy(&pm.machine_id, Some(&pm.provider_id))
                .await
            {
                Ok(()) => {
                    destroyed += 1;
                    tracing::warn!(pool = %spec.name, machine = %pm.machine_id, "destroyed a leaked machine");
                }
                Err(e) => {
                    tracing::warn!(machine = %pm.machine_id, error = %e, "could not destroy a leaked machine")
                }
            }
        }
        destroyed
    }

    async fn move_machine(&self, id: &str, from: &[&str], to: &str) -> anyhow::Result<bool> {
        let moved = self
            .db
            .query(
                sql::MOVE_MACHINE,
                &[("id", json!(id)), ("to", json!(to)), ("from", json!(from))],
            )
            .await?;
        Ok(first_row(moved).is_some())
    }

    async fn begin_terminate(
        &self,
        id: &str,
        from: &[&str],
        reason: &str,
        failure: bool,
    ) -> anyhow::Result<bool> {
        let moved = self
            .db
            .query(
                sql::BEGIN_TERMINATE,
                &[
                    ("id", json!(id)),
                    ("from", json!(from)),
                    ("reason", json!(reason)),
                    ("failure", json!(failure)),
                ],
            )
            .await?;
        Ok(first_row(moved).is_some())
    }

    /// Count a failed boot and, past the threshold, open the breaker with an
    /// exponentially growing pause. Returns whether the breaker is open now.
    async fn record_boot_failure(
        &self,
        spec: &PoolSpec,
        failures: i64,
        detail: &str,
        already_open: bool,
        report: &mut TickReport,
    ) -> bool {
        let over = failures - self.cfg.breaker_threshold;
        let until_secs = if over >= 0 {
            let doubled = self
                .cfg
                .breaker_base_secs
                .saturating_mul(1_i64 << over.min(16));
            doubled.min(self.cfg.breaker_max_secs).max(1)
        } else {
            0
        };
        let binds: Binds = vec![
            ("name", json!(spec.name)),
            ("failures", json!(failures)),
            ("error", json!(detail)),
            ("until_secs", json!(until_secs)),
        ];
        if let Err(e) = self.db.query(sql::RECORD_BOOT_FAILURE, &binds).await {
            tracing::warn!(pool = %spec.name, error = %e, "could not record a boot failure");
        }
        let open = until_secs > 0;
        if open && !already_open {
            report.transitions.push(PoolTransition {
                pool: spec.name.clone(),
                open: true,
                failures,
                detail: detail.to_string(),
            });
        }
        open
    }

    // -- agent entry points -----------------------------------------------------

    async fn pool_of_machine(
        &self,
        machine_id: &str,
    ) -> anyhow::Result<Option<(MachineState, PoolSpec)>> {
        let Some(row) = first_row(
            self.db
                .query(sql::SELECT_MACHINE, &[("id", json!(machine_id))])
                .await?,
        ) else {
            return Ok(None);
        };
        let Some(state) = row
            .get("state")
            .and_then(Value::as_str)
            .and_then(MachineState::parse)
        else {
            return Ok(None);
        };
        let Some(pool) = row.get("pool").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(pool_row) = first_row(
            self.db
                .query(sql::SELECT_POOL_BY_NAME, &[("name", json!(pool))])
                .await?,
        ) else {
            return Ok(None);
        };
        Ok(Some((state, PoolSpec::from_row(&pool_row)?)))
    }

    /// An agent starting up. `None` means "you have no business here": the row is
    /// gone or terminal, and the agent should shut its machine down.
    pub async fn on_hello(&self, machine_id: &str) -> anyhow::Result<Option<HelloReply>> {
        let Some((state, spec)) = self.pool_of_machine(machine_id).await? else {
            return Ok(None);
        };
        if !state.is_live() || state == MachineState::Terminating {
            return Ok(None);
        }
        let container = &spec.container;
        let env = container
            .get("env")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Some(HelloReply {
            port: container
                .get("port")
                .and_then(Value::as_u64)
                .unwrap_or(8080) as u16,
            healthcheck: container
                .get("healthcheck")
                .and_then(Value::as_str)
                .map(str::to_string),
            env,
            poll_secs: poll_secs(spec.lease_secs) as u64,
            lease_secs: spec.lease_secs as u64,
            stop_margin_secs: stop_margin_secs(spec.lease_secs) as u64,
            recycle_per_job: spec.recycle_per_job,
            orphan_secs: (spec.lease_secs * 10).clamp(300, 1800) as u64,
        }))
    }

    /// The backend on the machine is healthy. Returns false when the machine is no
    /// longer wanted (the agent should shut down).
    pub async fn on_ready(&self, machine_id: &str) -> anyhow::Result<bool> {
        let moved = first_row(
            self.db
                .query(sql::MARK_READY, &[("id", json!(machine_id))])
                .await?,
        );
        if let Some(row) = moved {
            // A machine made it: whatever was failing boots is over.
            if let Some(pool) = row.get("pool").and_then(Value::as_str) {
                let _ = self
                    .db
                    .query(sql::RESET_BOOT_FAILURES, &[("name", json!(pool))])
                    .await;
            }
            return Ok(true);
        }
        // Not requested/booting: fine if it is already serving (an agent restart).
        Ok(matches!(
            self.pool_of_machine(machine_id).await?.map(|(s, _)| s),
            Some(MachineState::Ready | MachineState::Draining)
        ))
    }

    /// Heartbeat in, commands out - derived from the rows, never from memory.
    pub async fn on_poll(&self, req: &PollRequest) -> anyhow::Result<PollReply> {
        // Only an object is storable in `agent`; anything else is just a heartbeat.
        let touched = first_row(match req.stats.as_ref().filter(|s| s.is_object()) {
            Some(stats) => {
                let binds = [("id", json!(req.machine)), ("stats", stats.clone())];
                self.db.query(sql::TOUCH_MACHINE_WITH_STATS, &binds).await?
            }
            None => {
                self.db
                    .query(sql::TOUCH_MACHINE, &[("id", json!(req.machine))])
                    .await?
            }
        });
        let shutdown = || PollReply {
            commands: vec![Command::Shutdown],
        };
        let Some(touched) = touched else {
            return Ok(shutdown());
        };
        let state = touched
            .get("state")
            .and_then(Value::as_str)
            .and_then(MachineState::parse);
        let Some(pool) = touched.get("pool").and_then(Value::as_str) else {
            return Ok(shutdown());
        };
        let Some(pool_row) = first_row(
            self.db
                .query(sql::SELECT_POOL_BY_NAME, &[("name", json!(pool))])
                .await?,
        ) else {
            // The pool was removed from sp00ky.yml: nothing left for this machine.
            return Ok(shutdown());
        };
        let spec = PoolSpec::from_row(&pool_row)?;

        let bound = rows(
            self.db
                .query(
                    &sql::select_bound(&spec.target_table),
                    &[("machine", json!(req.machine))],
                )
                .await?,
        );
        let bound: BTreeMap<JobRef, &Value> = bound
            .iter()
            .filter_map(|row| {
                let job = row.get("id")?.as_str()?.to_string();
                let epoch = row.get("lease_epoch").and_then(Value::as_i64).unwrap_or(0);
                Some((JobRef { job, epoch }, row))
            })
            .collect();
        let running: BTreeSet<&JobRef> = req.running.iter().collect();

        let mut commands = Vec::new();
        for attempt in &req.running {
            if bound.contains_key(attempt) {
                self.db
                    .query(
                        sql::RENEW_LEASE,
                        &[
                            ("id", json!(attempt.job)),
                            ("machine", json!(req.machine)),
                            ("epoch", json!(attempt.epoch)),
                            ("lease", json!(spec.lease_secs)),
                        ],
                    )
                    .await?;
            } else {
                // Reclaimed, killed, finished elsewhere: it no longer counts.
                commands.push(Command::Cancel {
                    job: attempt.job.clone(),
                    epoch: attempt.epoch,
                });
            }
        }
        for (attempt, row) in &bound {
            if running.contains(attempt) {
                continue;
            }
            let own = row
                .get("timeout")
                .and_then(Value::as_i64)
                .filter(|t| *t > 0);
            let deadline = own
                .unwrap_or(spec.max_job_duration_secs)
                .min(spec.max_job_duration_secs);
            commands.push(Command::Assign {
                job: attempt.job.clone(),
                epoch: attempt.epoch,
                path: row
                    .get("path")
                    .and_then(Value::as_str)
                    .unwrap_or("/")
                    .to_string(),
                payload: decode_payload(row.get("payload")),
                deadline_secs: deadline.max(1) as u64,
            });
        }

        let leaving = matches!(state, Some(MachineState::Terminating))
            || (matches!(state, Some(MachineState::Draining)) && bound.is_empty());
        if leaving {
            commands.push(Command::Shutdown);
        }
        Ok(PollReply { commands })
    }

    /// An attempt ended on a machine. The write is fenced on the machine AND the
    /// epoch, so a result from an attempt that lost its lease is simply refused.
    pub async fn on_result(&self, req: &ResultRequest) -> anyhow::Result<ResultReply> {
        let fence: Binds = vec![
            ("id", json!(req.job)),
            ("machine", json!(req.machine)),
            ("epoch", json!(req.epoch)),
        ];
        let with = |extra: (&'static str, Value)| {
            let mut binds = fence.clone();
            binds.push(extra);
            binds
        };
        let written = match &req.outcome {
            Outcome::Success { body } => {
                let binds = with(("result", self.encode_result(body)));
                match self.db.query(&sql::complete_success(), &binds).await {
                    Ok(written) => written,
                    Err(e) if e.is_unknown_field() => {
                        tracing::warn!(job = %req.job, error = %e,
                            "could not store the job result (is the outbox schema up to date?), completing without it");
                        self.db
                            .query(&sql::complete_success_without_result(), &fence)
                            .await?
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Outcome::Failed { code, reason } => {
                let error = json!({ "code": code, "reason": reason });
                self.db
                    .query(&sql::complete_failure(), &with(("error", error)))
                    .await?
            }
            Outcome::Cancelled => {
                let error = json!({ "code": "cancelled", "reason": "cancelled on the machine" });
                self.db
                    .query(&sql::complete_terminal_failure(), &with(("error", error)))
                    .await?
            }
            Outcome::DeadlineExceeded => {
                let error = json!({ "code": "deadline", "reason": "ran past its deadline" });
                self.db
                    .query(&sql::complete_terminal_failure(), &with(("error", error)))
                    .await?
            }
        };
        let accepted = first_row(written).is_some();
        if !accepted {
            tracing::info!(job = %req.job, epoch = req.epoch, machine = %req.machine,
                "dropped a result from an attempt that no longer owns its job");
        }
        Ok(ResultReply { accepted })
    }

    /// Operator kill for a pool job (pending or running). The running attempt is
    /// fenced at once; its agent is told to cancel on the next poll.
    /// Operator retry: true when the job was finished and is pending again.
    pub async fn retry_job(&self, job_id: &str) -> anyhow::Result<bool> {
        Ok(first_row(
            self.db
                .query(sql::RETRY_JOB, &[("id", json!(job_id))])
                .await?,
        )
        .is_some())
    }

    pub async fn kill_job(&self, job_id: &str) -> anyhow::Result<bool> {
        Ok(first_row(
            self.db
                .query(sql::KILL_JOB, &[("id", json!(job_id))])
                .await?,
        )
        .is_some())
    }

    /// Parsed JSON when the body parses (so a dependent workflow step can read
    /// `result.field`), the raw string otherwise, a marker when it is too large.
    fn encode_result(&self, body: &str) -> Value {
        if body.len() > self.cfg.result_max_bytes {
            return json!({ "truncated": true, "bytes": body.len() });
        }
        serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_string()))
    }
}

/// Outbox payloads are usually stored as a JSON *string* (that is what the
/// client mutation writes); hand the backend the decoded value, as the job
/// runner does.
fn decode_payload(payload: Option<&Value>) -> Value {
    match payload {
        Some(Value::String(s)) => {
            serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone()))
        }
        Some(other) => other.clone(),
        None => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_agent_always_gives_up_before_its_lease_can_be_reclaimed() {
        for lease in [1, 5, 30, 90, 300, 3600] {
            assert!(stop_margin_secs(lease) >= 1);
            assert!(stop_margin_secs(lease) <= lease);
            // Several polls fit in a lease, so a lost poll or two costs nothing.
            assert!(poll_secs(lease) * 3 <= lease.max(3));
            assert!(fresh_secs(lease) >= poll_secs(lease));
        }
    }

    #[test]
    fn payload_strings_are_decoded_like_the_runner_does() {
        assert_eq!(decode_payload(Some(&json!("{\"a\":1}"))), json!({ "a": 1 }));
        assert_eq!(decode_payload(Some(&json!("plain"))), json!("plain"));
        assert_eq!(decode_payload(Some(&json!({ "a": 1 }))), json!({ "a": 1 }));
        assert_eq!(decode_payload(None), Value::Null);
    }
}
