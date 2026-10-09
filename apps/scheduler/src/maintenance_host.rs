use anyhow::{Context, Result};
use maintenance::{MaintenanceHost, RestoreOutcome, RestoreProgress};
use std::sync::atomic::Ordering;
use tracing::{info, warn};

use crate::ingest::{pending_events_snapshot, IngestState};
use crate::SchedulerStatus;

const BOOTSTRAP_DRAIN_TIMEOUT_SECS: u64 = 10;

/// Scheduler-side hooks for the shared backup/restore workers: keeps the
/// replica, WAL, event buffer, seq counter and SSP pool consistent with the
/// restored main database.
pub struct SchedulerHost {
    pub ingest: IngestState,
}

#[async_trait::async_trait]
impl MaintenanceHost for SchedulerHost {
    /// Drain in-memory events into the replica. This keeps the replica's
    /// snapshot_seq current (useful for SSP bootstrap) even though the
    /// backup itself exports the main DB.
    ///
    /// Skipped while the snapshot is frozen or an SSP bootstrap is in flight:
    /// mutating the replica then would silently invalidate the hashes the
    /// bootstrapping SSP was handed at registration, minting exactly the
    /// integrity mismatch that sends SSPs into an exit(2) loop. The backup
    /// exports the main DB either way; only `snapshot_seq` freshness is lost.
    async fn pre_backup(&self) -> Result<Option<u64>> {
        let _drain_guard = crate::acquire_drain_lock(&self.ingest.drain_lock, "pre-backup").await;

        let status = *self.ingest.status.read().await;
        let active_bootstrap = self.ingest.ssp_pool.read().await.has_active_bootstrap();
        if status != SchedulerStatus::Ready || active_bootstrap {
            warn!(
                ?status,
                active_bootstrap,
                "Skipping pre-backup drain: snapshot frozen or SSP bootstrap in flight"
            );
            return Ok(Some(self.ingest.replica.read().await.snapshot_seq()));
        }

        let applied = crate::drain_and_apply(
            &self.ingest.event_buffer,
            &self.ingest.replica,
            &self.ingest.wal,
        )
        .await
        .context("Failed to drain pending events into replica before backup")?;
        if applied > 0 {
            info!(applied, "Drained pending events into replica");
        }
        Ok(Some(self.ingest.replica.read().await.snapshot_seq()))
    }

    /// Transition Ready → Restoring (refusing otherwise), then wait (bounded)
    /// for in-flight SSP bootstraps to drain — they read from the replica we
    /// are about to reset.
    async fn begin_restore(&self) -> Result<()> {
        {
            let mut status = self.ingest.status.write().await;
            if *status != SchedulerStatus::Ready {
                anyhow::bail!(
                    "Cannot restore: scheduler status is {:?}, expected Ready",
                    *status
                );
            }
            *status = SchedulerStatus::Restoring;
            info!("Scheduler status → Restoring");
        }

        let deadline = std::time::Instant::now()
            + std::time::Duration::from_secs(BOOTSTRAP_DRAIN_TIMEOUT_SECS);
        loop {
            let active = self.ingest.ssp_pool.read().await.has_active_bootstrap();
            if !active {
                break;
            }
            if std::time::Instant::now() >= deadline {
                warn!("Proceeding with restore despite active SSP bootstraps (timed out)");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        Ok(())
    }

    /// The worker already wiped and re-imported the main DB. Bring every piece
    /// of scheduler state in line with it: replica, seq counter, WAL, event
    /// buffer, and the SSP pool.
    async fn post_restore(&self, dump_path: &std::path::Path) -> Result<RestoreOutcome> {
        // Restore the snapshot replica: drop the on-disk DB, reopen empty, import.
        let restored_seq = {
            let mut rep = self.ingest.replica.write().await;
            rep.reset().await.context("Failed to reset replica")?;
            rep.import_from_file(dump_path)
                .await
                .context("Failed to import dump into replica")?;
            rep.reload_snapshot_seq()
                .await
                .context("Failed to reload snapshot_seq from restored replica")?
        };
        info!(restored_seq, "Replica restored from dump");

        // Clear pending items.
        let buffer_cleared = {
            let mut buffer = self.ingest.event_buffer.write().await;
            let n = buffer.len();
            buffer.clear();
            n
        };
        {
            let mut wal = self.ingest.wal.write().await;
            wal.truncate(u64::MAX)
                .context("Failed to truncate WAL during restore")?;
        }
        self.ingest.seq_counter.store(restored_seq, Ordering::SeqCst);
        {
            // Persist the restored seq explicitly so the metadata row matches
            // the authoritative counter even if the dump's seq differs subtly.
            //
            // Rehash EVERYTHING while we're here. The dump is an export of the
            // main DB, so it carries no `_00_metadata:snapshot` row: the reload
            // above left `known_tables`/`snapshot_hashes` empty over fully
            // populated content. Persisting just the seq (`set_snapshot_seq`
            // rehashes nothing) shipped that lie to every SSP that registered
            // afterwards.
            let mut rep = self.ingest.replica.write().await;
            let discovered = rep
                .rediscover_known_tables()
                .await
                .context("Failed to rediscover replica tables after restore")?;
            rep.set_snapshot_state(restored_seq, None)
                .await
                .context("Failed to persist restored snapshot state")?;
            info!(
                restored_seq,
                tables = discovered,
                "Replica snapshot state rehashed from restored content"
            );
        }

        // Evict SSPs. They will re-register on next heartbeat.
        let evicted = {
            let mut pool = self.ingest.ssp_pool.write().await;
            pool.clear_all()
        };
        info!(evicted, "SSPs evicted; will re-register against restored state");

        Ok(RestoreOutcome {
            snapshot_seq: Some(restored_seq),
            pending_cleared: buffer_cleared,
            main_db_restored: true,
            host_state_restored: true,
            ssps_evicted: Some(evicted),
        })
    }

    async fn finish_restore(&self, result: &Result<RestoreOutcome>, progress: RestoreProgress) {
        match result {
            Ok(_) => {
                *self.ingest.status.write().await = SchedulerStatus::Ready;
                info!("Scheduler status → Ready");
            }
            Err(e) => {
                if progress.host_state_restored {
                    // Replica import succeeded but a later step failed.
                    // Replica + main are consistent; safe to return to Ready.
                    *self.ingest.status.write().await = SchedulerStatus::Ready;
                    warn!(error = %e, "Restore post-import step failed; status → Ready anyway");
                } else if progress.main_db_restored {
                    // Main DB wiped/imported but replica is still the
                    // pre-restore state — serving reads would return wrong
                    // data. Leave Restoring to block traffic.
                    warn!(
                        error = %e,
                        "Restore partial failure after main DB changed; status stays Restoring"
                    );
                } else {
                    // Nothing mutated; safe to recover.
                    *self.ingest.status.write().await = SchedulerStatus::Ready;
                    warn!(error = %e, "Restore failed before any DB mutation; status → Ready");
                }
            }
        }
    }

    async fn status_extras(&self) -> serde_json::Value {
        let pending = pending_events_snapshot(&self.ingest).await;
        serde_json::json!({
            "pending_events": pending.pending_events,
            "snapshot_seq": pending.snapshot_seq,
            "latest_seq": pending.latest_seq,
            "lag": pending.lag,
        })
    }
}
