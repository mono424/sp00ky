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

#[derive(Clone)]
pub struct Runtime {
    node: Arc<SspNode>,
}

impl Runtime {
    pub fn new(node: Arc<SspNode>) -> Self {
        Self { node }
    }

    pub fn node(&self) -> &Arc<SspNode> {
        &self.node
    }

    /// Dispatch one fired timer. Handles the PORTABLE periodic kinds
    /// (`JobRecoverySweep`, `JobDrain`, `ScheduleSweep`, `TtlCleanup`,
    /// `CircuitCheckpoint`) — each runs its
    /// work (gated on `Ready`) then re-arms itself via the `Scheduler` port.
    /// Host-specific kinds (`DbResignin`, `BackendHealth` — they need the
    /// non-wasm `maintenance` crate) are NOT handled here; the shell's drain
    /// loop keeps those inline. One-shot kinds (`DelayedJob`, `JobRetry`,
    /// `BootstrapRetry`) are driven directly by their originators, not here.
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
                    sched
                        .schedule(
                            TimerKind::JobRecoverySweep,
                            now_epoch_ms() + JOB_RECOVERY_INTERVAL_SECS * 1000,
                        )
                        .await;
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
                sched
                    .schedule(
                        TimerKind::TtlCleanup,
                        now_epoch_ms() + node.ttl_cleanup_interval_secs * 1000,
                    )
                    .await;
            }
            TimerKind::ViewMetricsFlush => {
                if *node.status.read().await == SspStatus::Ready {
                    node.flush_view_metrics().await;
                }
                sched
                    .schedule(
                        TimerKind::ViewMetricsFlush,
                        now_epoch_ms() + node.view_metrics_flush_ms,
                    )
                    .await;
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
        let node = &self.node;
        *node.status.write().await = SspStatus::Bootstrapping;
        match async {
            self.bootstrap_inner().await?;
            node.republish_restored_views().await
        }.await {
            Ok(()) => {
                *node.status.write().await = SspStatus::Ready;
                tracing::info!("Runtime::bootstrap complete — Ready");
            }
            Err(e) => {
                *node.status.write().await = SspStatus::Failed;
                tracing::error!(error = %e, "Runtime::bootstrap failed");
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
