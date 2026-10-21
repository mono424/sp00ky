//! The platform-independent lifecycle owner.
//!
//! A host shell = implement the [`crate::Platform`] ports + drive a `Runtime`.
//! The shell keeps its own HTTP ingress and pushes `ApiRequest`s straight into
//! [`crate::SspNode::route`]; the `Runtime` owns only the two lifecycle
//! entrypoints that are otherwise easy to get subtly wrong per platform:
//!
//! - [`Runtime::bootstrap`] — cold-start / re-init (restore-or-rebuild).
//! - [`Runtime::on_timer`] — every scheduled wakeup; periodic kinds re-arm
//!   themselves through the [`crate::ports::Scheduler`] port.
//!
//! No HTTP, no framework, no accept-loop — a DO shell drives `on_timer` from
//! its single `alarm()` via [`crate::TimerMux`]; the VM drives it from a tokio
//! mpsc drain loop. Both call the same code here.

use std::sync::Arc;

use ssp::circuit::Circuit;

use crate::node::JOB_RECOVERY_INTERVAL_SECS;
use crate::schedules::SCHEDULE_SWEEP_INTERVAL_SECS;
use crate::ports::CircuitStoreError;
use crate::status::SspStatus;
use crate::{now_epoch_ms, SspNode, TimerKind};

/// How the portable sweeps re-arm themselves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerPolicy {
    /// Every sweep re-arms itself at its fixed cadence (the VM: a process that
    /// runs anyway, where a wake costs nothing).
    Periodic,
    /// A sweep is armed only while it has work, at the moment that work is
    /// due, so an idle node sets no timer at all and a Durable Object can be
    /// evicted (measured: 0 alarms over 6 h idle, against about 2,600 an hour
    /// periodic). Requests and boots arm what they made pending
    /// ([`Runtime::after_request`], [`Runtime::after_boot`]).
    OnDemand,
}

#[derive(Clone)]
pub struct Runtime {
    node: Arc<SspNode>,
    policy: TimerPolicy,
}

impl Runtime {
    pub fn new(node: Arc<SspNode>) -> Self {
        Self { node, policy: TimerPolicy::Periodic }
    }

    /// A runtime whose sweeps are armed on demand (see [`TimerPolicy::OnDemand`]).
    pub fn on_demand(node: Arc<SspNode>) -> Self {
        Self { node, policy: TimerPolicy::OnDemand }
    }

    pub fn node(&self) -> &Arc<SspNode> {
        &self.node
    }

    pub fn policy(&self) -> TimerPolicy {
        self.policy
    }

    /// Arm what a freshly bootstrapped node has pending. On demand only (the
    /// periodic shells arm their own fixed cadences). The DB probes run in the
    /// background so they never add a round trip to the cold start.
    pub async fn after_boot(&self) {
        if self.policy != TimerPolicy::OnDemand {
            return;
        }
        let runtime = self.clone();
        self.node.platform.spawner.spawn(Box::pin(async move {
            let node = &runtime.node;
            runtime.arm_ttl().await;
            runtime.arm_job_recovery_if_pending().await;
            // What the periodic CrdtSweep was for: rebuild the fold tracker from
            // the log tables once per wake, then fold only if that found work.
            if !node.is_standby() {
                if let Err(e) = node.crdt.sweep(node.platform.db.as_ref()).await {
                    tracing::warn!(error = %format!("{e:#}"), "CRDT sweep at boot failed");
                }
            }
            runtime.arm_crdt_fold().await;
        }));
    }

    /// Arm what a request just made pending: dirty view metrics, newly logged
    /// `@crdt` rows, a registration's view (it may expire first), outbox rows.
    /// On demand only; cheap checks inline, DB probes in the background.
    pub async fn after_request(&self, path: &str) {
        if self.policy != TimerPolicy::OnDemand {
            return;
        }
        let node = &self.node;
        let sched = &node.platform.scheduler;
        if any_view_metric_dirty(node).await {
            sched
                .ensure(TimerKind::ViewMetricsFlush, now_epoch_ms() + node.view_metrics_flush_ms)
                .await;
        }
        let crdt_work = node.crdt.next_due_ms(now_epoch_ms()).is_some();
        if crdt_work {
            self.arm_crdt_fold().await;
            sched
                .ensure(TimerKind::CrdtSweep, now_epoch_ms() + node.crdt.config().sweep_secs * 1000)
                .await;
        }
        let registered = path.starts_with("/view/register");
        let ingested = path.starts_with("/ingest") && !node.job_config.job_tables.is_empty();
        if registered || ingested {
            let runtime = self.clone();
            node.platform.spawner.spawn(Box::pin(async move {
                if registered {
                    runtime.arm_ttl().await;
                }
                if ingested {
                    runtime.arm_job_recovery_if_pending().await;
                }
            }));
        }
    }

    /// TTL cleanup at the earliest `_00_query` expiry (plus a second so the
    /// sweep finds the row expired), never later than now; cancelled when no
    /// view exists. A heartbeat moves the expiry later without reaching the
    /// SSP, so an early sweep finds nothing and re-arms at the new earliest.
    async fn arm_ttl(&self) {
        let sched = &self.node.platform.scheduler;
        match earliest_view_expiry_ms(self.node.platform.db.as_ref()).await {
            Ok(Some(at)) => sched.ensure(TimerKind::TtlCleanup, (at + 1000).max(now_epoch_ms())).await,
            Ok(None) => sched.cancel(&TimerKind::TtlCleanup).await,
            // Unknown: keep the periodic fallback rather than lose expiry.
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), "TTL: reading the earliest expiry failed");
                sched
                    .ensure(
                        TimerKind::TtlCleanup,
                        now_epoch_ms() + self.node.ttl_cleanup_interval_secs * 1000,
                    )
                    .await;
            }
        }
    }

    async fn arm_job_recovery_if_pending(&self) {
        let node = &self.node;
        if !node.standalone {
            return;
        }
        if jobs_pending(node).await {
            node.platform
                .scheduler
                .ensure(
                    TimerKind::JobRecoverySweep,
                    now_epoch_ms() + JOB_RECOVERY_INTERVAL_SECS * 1000,
                )
                .await;
        }
    }

    async fn arm_crdt_fold(&self) {
        let node = &self.node;
        if let Some(at) = node.crdt.next_due_ms(now_epoch_ms()) {
            node.platform
                .scheduler
                .ensure(TimerKind::CrdtFoldTick, at.max(now_epoch_ms()))
                .await;
        }
    }

    /// Dispatch one fired timer. Handles the PORTABLE periodic kinds
    /// (`JobRecoverySweep`, `JobDrain`, `ScheduleSweep`, `TtlCleanup`,
    /// `CircuitCheckpoint`) — each runs its
    /// work (gated on `Ready`) then re-arms itself via the `Scheduler` port.
    /// Host-specific kinds (`DbResignin`, `BackendHealth` — they need the
    /// non-wasm `maintenance` crate) are NOT handled here; the shell's drain
    /// loop keeps those inline. One-shot kinds (`DelayedJob`, `JobRetry`,
    /// `BootstrapRetry`, `IngestReplay`) are driven directly by their
    /// originators, not here.
    pub async fn on_timer(&self, kind: TimerKind) {
        let node = &self.node;
        let sched = &node.platform.scheduler;
        match kind {
            TimerKind::JobRecoverySweep => {
                // Standalone owns recovery; cluster mode leaves it to the scheduler.
                if node.standalone && *node.status.read().await == SspStatus::Ready {
                    node.job_recovery_sweep().await;
                }
                if node.standalone {
                    match self.policy {
                        TimerPolicy::Periodic => {
                            sched
                                .schedule(
                                    TimerKind::JobRecoverySweep,
                                    now_epoch_ms() + JOB_RECOVERY_INTERVAL_SECS * 1000,
                                )
                                .await;
                        }
                        TimerPolicy::OnDemand => self.arm_job_recovery_if_pending().await,
                    }
                }
            }
            TimerKind::JobDrain { table } => {
                // Armed in BOTH modes, unlike the recovery sweep: the cluster
                // case is precisely where a node can have free local slots, be
                // blocked by the global count, and so never complete a job to
                // kick its own drain. The drain re-arms this itself while the
                // table still has a backlog.
                if *node.status.read().await == SspStatus::Ready {
                    node.job_dispatcher.drain(&table).await;
                }
            }
            TimerKind::ScheduleSweep => {
                // Standalone owns the schedule engine; in cluster mode the
                // scheduler service ticks and `schedule_engine` is None.
                if let Some(engine) = node.schedule_engine.as_ref() {
                    if *node.status.read().await == SspStatus::Ready {
                        match engine.tick_pass().await {
                            Ok(report) => {
                                if report != Default::default() {
                                    tracing::debug!(?report, "schedule sweep");
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "schedule sweep failed");
                            }
                        }
                    }
                    sched
                        .schedule(
                            TimerKind::ScheduleSweep,
                            now_epoch_ms() + SCHEDULE_SWEEP_INTERVAL_SECS * 1000,
                        )
                        .await;
                }
            }
            TimerKind::TtlCleanup => {
                if *node.status.read().await == SspStatus::Ready {
                    node.ttl_cleanup_sweep().await;
                }
                match self.policy {
                    TimerPolicy::Periodic => {
                        sched
                            .schedule(
                                TimerKind::TtlCleanup,
                                now_epoch_ms() + node.ttl_cleanup_interval_secs * 1000,
                            )
                            .await;
                    }
                    TimerPolicy::OnDemand => self.arm_ttl().await,
                }
            }
            TimerKind::ViewMetricsFlush => {
                if *node.status.read().await == SspStatus::Ready {
                    node.flush_view_metrics().await;
                }
                if self.policy == TimerPolicy::Periodic || any_view_metric_dirty(node).await {
                    sched
                        .schedule(
                            TimerKind::ViewMetricsFlush,
                            now_epoch_ms() + node.view_metrics_flush_ms,
                        )
                        .await;
                }
            }
            TimerKind::SchemaPoll => {
                if node.schema_poll_secs == 0 {
                    return;
                }
                if let Err(e) = node.refresh_schema(true).await {
                    tracing::warn!(error = %e, "Schema poll failed; retrying at the next one");
                }
                sched
                    .schedule(TimerKind::SchemaPoll, now_epoch_ms() + node.schema_poll_secs * 1000)
                    .await;
            }
            TimerKind::CrdtFoldTick => {
                // A standby only follows; its predecessor folds.
                if !node.is_standby() && *node.status.read().await == SspStatus::Ready {
                    node.crdt.tick(
                        std::sync::Arc::clone(&node.platform.db),
                        node.platform.spawner.as_ref(),
                        now_epoch_ms(),
                    );
                }
                match self.policy {
                    TimerPolicy::Periodic => {
                        sched
                            .schedule(
                                TimerKind::CrdtFoldTick,
                                now_epoch_ms() + node.crdt.config().tick_ms,
                            )
                            .await;
                    }
                    // Folds started above are still running, so this arms a
                    // look one tick later, then the next due fold, then nothing.
                    TimerPolicy::OnDemand => self.arm_crdt_fold().await,
                }
            }
            TimerKind::CrdtSweep => {
                if !node.is_standby() && *node.status.read().await == SspStatus::Ready {
                    if let Err(e) = node.crdt.sweep(node.platform.db.as_ref()).await {
                        tracing::warn!(error = %e, "CRDT sweep failed; retrying at the next one");
                    }
                }
                // On demand the sweep runs once per wake (`after_boot`) and
                // after `@crdt` activity (`after_request`), never on a cadence.
                if self.policy == TimerPolicy::Periodic {
                    sched
                        .schedule(TimerKind::CrdtSweep, now_epoch_ms() + node.crdt.config().sweep_secs * 1000)
                        .await;
                }
            }
            TimerKind::CircuitCheckpoint => {
                // Filled in step 4 (checkpoint triggers). Re-arm only if enabled.
                if let Some(secs) = node.checkpoint_interval_secs {
                    self.checkpoint().await;
                    sched
                        .schedule(TimerKind::CircuitCheckpoint, now_epoch_ms() + secs * 1000)
                        .await;
                }
            }
            other => {
                tracing::debug!(?other, "Runtime::on_timer: kind not handled by the core");
            }
        }
    }

    /// Cold-start / re-init. Restore-or-rebuild the DBSP circuit, then flip
    /// status to `Ready`. Standalone Direct path only — the cluster/Proxy
    /// bootstrap keeps its bespoke wrapper in the shell (needs reqwest) and
    /// calls [`crate::bootstrap::rebuild_from_db`] for the data load.
    ///
    /// - `CircuitStore::load` NotFound/Corrupt, or a snapshot older than
    ///   `max_snapshot_age_secs` → full REBUILD (INFO FOR DB + paged SELECT).
    /// - Fresh snapshot → `Circuit::restore` + incremental `_00_rv` catch-up
    ///   (a caught-up-table's content still converges to the DB because the
    ///   `/ingest` 503 gate holds new writes until we reach `Ready`).
    ///
    /// Errors flip status to `Failed`; the shell's `BootstrapRetry` timer (or a
    /// DO's next `fetch`) drives the retry.
    pub async fn bootstrap(&self) {
        self.bootstrap_with(None).await
    }

    /// [`Self::bootstrap`], first trying a warm start from `snapshot`'s table
    /// images ([`crate::snapshot`]). An unusable snapshot falls back to the
    /// usual path; a database failure part-way through resets the circuit and
    /// fails the bootstrap, so the retry starts clean.
    pub async fn bootstrap_with(&self, snapshot: Option<&dyn crate::ports::SnapshotStore>) {
        let node = &self.node;
        *node.status.write().await = SspStatus::Bootstrapping;
        match async {
            let warm = match snapshot {
                Some(store) => {
                    node.apply_circuit_policy().await;
                    match node.warm_bootstrap(store, node.max_snapshot_age_secs).await {
                        Ok(report) => report.is_some(),
                        Err(e) => {
                            *node.processor.write().await = Circuit::new();
                            return Err(e.context("warm start"));
                        }
                    }
                }
                None => false,
            };
            if !warm {
                self.bootstrap_inner().await?;
            }
            node.republish_restored_views().await
        }.await {
            Ok(()) => {
                *node.status.write().await = SspStatus::Ready;
                tracing::info!("Runtime::bootstrap complete — Ready");
            }
            Err(e) => {
                *node.status.write().await = SspStatus::Failed;
                // `{:#}`: the whole chain, not just the outer context
                // ("INFO FOR DB" alone hid "connection refused").
                tracing::error!(error = %format!("{e:#}"), "Runtime::bootstrap failed");
            }
        }
    }

    async fn bootstrap_inner(&self) -> anyhow::Result<()> {
        let node = &self.node;
        let db = node.platform.db.as_ref();
        let page_size = node.bootstrap_page_size;
        // Boot re-registration merges too, so the policy has to be in the
        // circuit before `rebuild_from_db` starts registering into it.
        node.apply_circuit_policy().await;

        match node.platform.circuit_store.load().await {
            Ok((blob, point)) => {
                let now = now_epoch_ms();
                let age_ms = now.saturating_sub(point.saved_at_epoch_ms);
                if age_ms > node.max_snapshot_age_secs.saturating_mul(1000) {
                    tracing::info!(age_ms, "Snapshot too old — full rebuild");
                    crate::bootstrap::rebuild_from_db(db, &node.processor, page_size).await?;
                    return Ok(());
                }
                tracing::info!(age_ms, "Restoring circuit snapshot");
                // A snapshot that won't deserialize is indistinguishable from a
                // corrupt one: both mean "this blob is unusable", and the only
                // safe answer is the same full rebuild that `Corrupt` takes
                // below. Erroring out here instead would wedge the node in
                // `SspStatus::Failed` until the `max_snapshot_age_secs` gate
                // above finally rejects the blob on age (an hour by default) —
                // and a snapshot *format* change makes every restore fail, so
                // the whole fleet would sit failed for that hour rather than
                // rebuilding once.
                match Circuit::restore(&blob) {
                    Ok(restored) => {
                        // Ledger versions are per record, not a table-wide clock.
                        // An old row can change from 1 to 2 while another is at
                        // 1000, so max-version catch-up would miss that update.
                        let info = db.query("INFO FOR DB", &[]).await
                            .map_err(|e| anyhow::anyhow!("snapshot schema check failed: {e}"))?;
                        if info.first().and_then(|v| v.get("tables"))
                            .and_then(|v| v.get("_00_version")).is_some() {
                            *node.processor.write().await = Circuit::new();
                            node.apply_circuit_policy().await;
                            crate::bootstrap::rebuild_from_db(db, &node.processor, page_size).await?;
                            return Ok(());
                        }
                        *node.processor.write().await = restored;
                        // The restored circuit is a fresh object; re-apply.
                        node.apply_circuit_policy().await;
                        crate::bootstrap::catch_up_from_db(db, &node.processor, &point).await?;
                        Ok(())
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Unreadable snapshot — full rebuild");
                        crate::bootstrap::rebuild_from_db(db, &node.processor, page_size).await?;
                        Ok(())
                    }
                }
            }
            Err(CircuitStoreError::NotFound) => {
                tracing::info!("No snapshot — full rebuild");
                crate::bootstrap::rebuild_from_db(db, &node.processor, page_size).await?;
                Ok(())
            }
            Err(CircuitStoreError::Corrupt(msg)) => {
                tracing::warn!(%msg, "Corrupt snapshot — full rebuild");
                crate::bootstrap::rebuild_from_db(db, &node.processor, page_size).await?;
                Ok(())
            }
            Err(CircuitStoreError::Transport(msg)) => {
                Err(anyhow::anyhow!("CircuitStore transport error: {msg}"))
            }
        }
    }

    /// Persist a circuit snapshot + resume-point via the `CircuitStore` port.
    /// Called on the `CircuitCheckpoint` timer (config-gated) and on graceful
    /// shutdown. NEVER per-ingest: `Circuit::save` clones the whole store.
    ///
    /// The resume-point pins `saved_at_epoch_ms` (staleness gate) and, per
    /// table, the highest `_00_rv` folded in + a content hash, so a warm
    /// restart catches up only the delta (see [`Runtime::bootstrap`]). The VM's
    /// `NoopCircuitStore` makes `save` a cheap no-op.
    pub async fn checkpoint(&self) {
        let node = &self.node;
        // Checkpoints off (no snapshot dir, cluster node, or an explicit 0)
        // means off for the shutdown checkpoint too: the shell used to
        // serialise the whole store on every SIGTERM of a cluster node that
        // could never read it back, which turned each restart into an extra
        // minute of lock-held serialisation before the real bootstrap began.
        if node.checkpoint_interval_secs.is_none() {
            return;
        }
        // Skip while not Ready — a half-built circuit is not a valid snapshot.
        if *node.status.read().await != SspStatus::Ready {
            return;
        }
        let (blob, point) = {
            let circuit = node.processor.read().await;
            let blob = match circuit.save() {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(error = %e, "checkpoint: Circuit::save failed");
                    return;
                }
            };
            let point = crate::ports::ResumePoint {
                saved_at_epoch_ms: now_epoch_ms(),
                table_hashes: circuit.compute_table_hashes(),
                max_row_version: circuit.max_row_versions(),
            };
            (blob, point)
        };
        match node.platform.circuit_store.save(&blob, &point).await {
            Ok(()) => tracing::debug!(bytes = blob.len(), "circuit checkpoint saved"),
            Err(e) => tracing::error!(error = %e, "checkpoint: CircuitStore::save failed"),
        }
    }
}

async fn any_view_metric_dirty(node: &SspNode) -> bool {
    node.view_metrics.read().await.values().any(|s| s.dirty)
}

/// Earliest `lastActiveAt + ttl` over `_00_query`, in epoch ms; `None` when
/// no view exists. The same expression the TTL sweep compares to `time::now()`.
async fn earliest_view_expiry_ms(db: &dyn crate::ports::Db) -> anyhow::Result<Option<u64>> {
    let rows = match db
        .query(
            "RETURN math::min((SELECT VALUE time::unix(lastActiveAt + ttl) FROM _00_query))",
            &[],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) if missing_table(&e.to_string()) => return Ok(None),
        Err(e) => return Err(anyhow::anyhow!("{e}")),
    };
    Ok(rows
        .first()
        .and_then(serde_json::Value::as_f64)
        .map(|secs| (secs.max(0.0) * 1000.0) as u64))
}

/// Whether any outbox table holds a row the recovery sweep could act on
/// (`pending` or `processing`). One multi-statement request; a failed probe
/// answers `true` so recovery is kept rather than lost.
async fn jobs_pending(node: &SspNode) -> bool {
    let tables: Vec<&String> = node.job_config.job_tables.keys().collect();
    if tables.is_empty() {
        return false;
    }
    let probe = |t: &str| {
        format!(
            "RETURN array::len((SELECT VALUE id FROM {t} \
             WHERE status IN ['pending', 'processing'] LIMIT 1));"
        )
    };
    let db = node.platform.db.as_ref();
    let batch: String = tables.iter().map(|t| probe(t)).collect();
    match db.query(&batch, &[]).await {
        Ok(rows) => rows.iter().any(|v| v.as_u64().unwrap_or(0) > 0),
        // SurrealDB 3 errors on a table nobody has written to yet, rather than
        // returning no rows, and one such table fails the whole batch: ask
        // each table on its own, counting a missing one as empty.
        Err(e) if missing_table(&e.to_string()) => {
            for table in &tables {
                match db.query(&probe(table), &[]).await {
                    Ok(rows) if rows.iter().any(|v| v.as_u64().unwrap_or(0) > 0) => return true,
                    Ok(_) => {}
                    Err(e) if missing_table(&e.to_string()) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "job recovery: pending-rows probe failed");
                        return true;
                    }
                }
            }
            false
        }
        Err(e) => {
            tracing::warn!(error = %e, "job recovery: pending-rows probe failed");
            true
        }
    }
}

/// SurrealDB 3's answer to a SELECT on a table that was never defined or
/// written ("The table 'x' does not exist"), which earlier versions answered
/// with no rows.
fn missing_table(error: &str) -> bool {
    error.contains("does not exist") && error.contains("table")
}
