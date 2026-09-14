use anyhow::{Context, Result};
use maintenance::{MaintenanceHost, RestoreOutcome, RestoreProgress};
use ssp::circuit::Circuit;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{BootstrapSource, SharedDb, SspStatus};
use ssp_node::wipe_circuit_and_edges;

/// Standalone-SSP hooks for the shared backup/restore workers. The SSP has no
/// replica or WAL — its restorable state is the DBSP circuit, which is
/// re-bootstrapped from the restored main database.
pub struct SspHost {
    pub db: SharedDb,
    pub processor: Arc<RwLock<Circuit>>,
    pub publication_gate: Arc<tokio::sync::Mutex<()>>,
    pub status: Arc<RwLock<SspStatus>>,
    /// Platform ports (Db for the edge wipe, Telemetry for view-count gauges).
    pub platform: ssp_node::Platform,
    pub ref_mode: ssp_protocol::RefMode,
    /// Rows per bootstrap page for the post-restore re-bootstrap
    /// (NodeConfig.bootstrap_page_size).
    pub bootstrap_page_size: usize,
}

#[async_trait::async_trait]
impl MaintenanceHost for SspHost {
    /// Nothing to prepare — the main-DB export is authoritative and the SSP
    /// keeps no durable state of its own.
    async fn pre_backup(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    /// CAS Ready → Bootstrapping. While Bootstrapping, `/ingest` returns 503
    /// (and the TTL/recovery sweeps skip), which is the standalone equivalent
    /// of the scheduler's Restoring-blocks-ingest gate. Events pushed by
    /// SurrealDB `DEFINE EVENT` triggers during this window are rejected and
    /// NOT replayed afterwards — the post-restore state is exactly the
    /// restored dump, matching the scheduler's pending-buffer clear.
    async fn begin_restore(&self) -> Result<()> {
        let mut status = self.status.write().await;
        if *status != SspStatus::Ready {
            anyhow::bail!(
                "Cannot restore: SSP status is {:?}, expected Ready",
                *status
            );
        }
        *status = SspStatus::Bootstrapping;
        info!("SSP status → Bootstrapping (restore in progress)");
        Ok(())
    }

    /// The worker already wiped and re-imported the main DB. Clear stale sync
    /// state that came in with the dump, wipe the circuit, and re-bootstrap
    /// from the restored database.
    async fn post_restore(&self, _dump_path: &std::path::Path) -> Result<RestoreOutcome> {
        let _publication = self.publication_gate.lock().await;
        // 1. Registered views are tied to live clients; rows restored from the
        //    dump point at sessions that no longer exist (mirrors the
        //    scheduler's startup `DELETE _00_query`).
        if let Err(e) = self.db.handle().query("DELETE _00_query").await {
            warn!(error = %e, "Failed to clear _00_query after restore (table may not exist)");
        }

        // 2. Wipe circuit + all `_00_list_ref*` edges from the dump.
        wipe_circuit_and_edges(
            self.platform.db.as_ref(),
            &self.processor,
            self.platform.telemetry.as_ref(),
            self.ref_mode,
        )
        .await;

        // 3. Re-bootstrap directly from the restored DB. No expected hashes —
        //    hash verification only applies in cluster mode.
        let source = BootstrapSource::Direct(self.db.clone());
        // No progress reporting: this is a post-restore rebuild against the
        // local DB, not a cluster bootstrap, so there is no scheduler waiting
        // to render a bar for it.
        crate::self_bootstrap_with_metadata(
            &source,
            &source,
            &self.processor,
            self.bootstrap_page_size,
            None,
        )
        .await
            .context("Failed to re-bootstrap circuit from restored database")?;
        {
            let mut guard = self.processor.write().await;
            guard.reseed_catchup_hashes();
        }
        {
            let guard = self.processor.read().await;
            self.platform
                .telemetry
                .gauge_add("view_count", guard.view_count() as i64);
            info!(
                tables = guard.table_names().len(),
                views = guard.view_count(),
                "Circuit re-bootstrapped from restored database"
            );
        }

        // Note: job entries queued before the restore reference pre-restore
        // records; their status writes fail harmlessly and the recovery sweep
        // re-picks pending rows from the restored DB on its next tick.
        Ok(RestoreOutcome {
            snapshot_seq: None,
            pending_cleared: 0,
            main_db_restored: true,
            host_state_restored: true,
            ssps_evicted: None,
        })
    }

    async fn finish_restore(&self, result: &Result<RestoreOutcome>, progress: RestoreProgress) {
        match result {
            Ok(_) => {
                *self.status.write().await = SspStatus::Ready;
                info!("SSP status → Ready (restore complete)");
            }
            Err(e) => {
                if progress.main_db_restored && !progress.host_state_restored {
                    // Main DB was replaced but the circuit re-bootstrap failed:
                    // the circuit disagrees with the database, so keep blocking
                    // traffic. A supervisor restart reruns the normal startup
                    // bootstrap against the restored DB and self-heals.
                    warn!(
                        error = %e,
                        "Restore imported main DB but circuit re-bootstrap failed; \
                         status stays Bootstrapping — restart to self-heal"
                    );
                } else {
                    // Nothing mutated (download/connect/import failed early):
                    // safe to serve the pre-restore state again.
                    *self.status.write().await = SspStatus::Ready;
                    warn!(error = %e, "Restore failed before any DB mutation; status → Ready");
                }
            }
        }
    }
}
