pub mod admin;
pub mod config;
pub mod maintenance_host;
pub mod replica;
pub mod router;

// Backup/restore/backend-health moved to the shared `maintenance` crate so a
// standalone SSP can expose the same plane. These shims keep the historical
// `scheduler::backup::*` / `scheduler::restore::*` paths working.
pub use maintenance::backend_health;
pub mod backup {
    pub use maintenance::backup::*;
    pub use maintenance::routes::{create_backup_router, BackupState};
    pub use maintenance::s3::{ensure_bucket, BackupConfig};
}
pub mod restore {
    pub use maintenance::db::connect_http as connect_remote;
    pub use maintenance::restore::*;
}
pub mod job_scheduler;
pub mod transport;
pub mod messages;
pub mod ingest;
pub mod query;
pub mod metrics;
pub mod ssp_management;
pub mod wal;
pub mod proxy;
pub mod feature_flags;
pub mod heartbeat;
pub mod schedule_engine;
pub mod drift;

use anyhow::{Context, Result};

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};

use crate::config::SchedulerConfig;
use crate::messages::BufferedEvent;
use crate::replica::Replica;
use crate::router::SspPool;
use crate::transport::HttpTransport;
use crate::wal::EventWal;

/// Tables a drained batch wrote, i.e. the set whose cached hashes must be
/// recomputed alongside the content.
///
/// The filter is `table_excluded_from_sync`, NOT a raw `_00_` prefix test:
/// `Replica::apply` DOES write the synced meta tables (`_00_user_feature`,
/// `_00_app_release`) into the replica and into `known_tables`. Leaving them
/// out advanced the content while their cached hash stayed behind — exactly
/// the stale-cache drift that makes an SSP's bootstrap integrity check fail,
/// forever and unrecoverably.
fn touched_tables(events: &[BufferedEvent]) -> BTreeSet<String> {
    events
        .iter()
        .filter(|e| !ssp_protocol::table_excluded_from_sync(&e.update.table))
        .map(|e| e.update.table.clone())
        .collect()
}

/// Boot-time WAL recovery. Returns the backlog (events past the persisted
/// `snapshot_seq`, in order) and the seq the counter resumes from.
///
/// A backlog is the normal case after a restart: events land in the buffer
/// and the WAL and are only applied at the next drain, so a stop between two
/// drains leaves everything since the last one unapplied. Those fold at the
/// first drain like any other batch. The exception is a process that died
/// inside a drain, after writing some of the batch's rows and before
/// committing (`Replica::interrupted_apply`): re-folding "before out, after
/// in" against rows that already carry the change would leave those tables'
/// hashes wrong for good, so their tables are flagged dirty and the first
/// drain rehashes exactly those from content, and nothing else. This is what
/// replaced the boot-time full rehash of every table.
pub fn recover_wal_backlog(
    replica: &mut Replica,
    wal: &EventWal,
) -> Result<(VecDeque<BufferedEvent>, u64)> {
    let snapshot_seq = replica.snapshot_seq();
    let recovered_events = wal.recover()?;
    let recovered_count = recovered_events.len();

    let max_wal_seq = recovered_events.last().map(|e| e.seq).unwrap_or(0);
    let initial_seq = max_wal_seq.max(snapshot_seq);

    let mut event_buffer: VecDeque<BufferedEvent> = recovered_events
        .into_iter()
        .filter(|e| e.seq > snapshot_seq)
        .collect();

    if recovered_count > 0 {
        info!(
            recovered_count,
            buffer_size = event_buffer.len(),
            snapshot_seq,
            initial_seq,
            "Recovered events from WAL"
        );
    }
    if !event_buffer.is_empty() {
        let tables = touched_tables(event_buffer.make_contiguous());
        if replica.interrupted_apply() {
            warn!(
                tables = ?tables,
                "The previous process died inside a drain; the WAL backlog's tables will be rehashed from content at the first drain"
            );
            replica.mark_tables_dirty(tables);
        } else {
            info!(
                tables = ?tables,
                "WAL backlog was never applied; it folds into the table hashes at the first drain"
            );
        }
    }
    Ok((event_buffer, initial_seq))
}

/// Drain the in-memory event buffer and apply all events to the replica.
/// Also advances `snapshot_seq` and truncates the WAL up to that seq.
/// Returns the number of events applied (may be 0). Does NOT touch `SchedulerStatus`.
pub async fn drain_and_apply(
    event_buffer: &Arc<RwLock<VecDeque<BufferedEvent>>>,
    replica: &Arc<RwLock<Replica>>,
    wal: &Arc<RwLock<EventWal>>,
) -> Result<usize> {
    let events: Vec<BufferedEvent> = {
        let mut buffer = event_buffer.write().await;
        buffer.drain(..).collect()
    };

    // Track which tables this batch touched so the snapshot-state writer
    // only rehashes the affected tables, not the whole replica.
    let touched = touched_tables(&events);

    // Nothing to apply, and every hash current: done. A dirty table with
    // nothing to apply still needs its from-content rehash below — the
    // persisted hash is wrong until then and a registration would hand it
    // out (the WAL-backlog case at boot, a failed fold, a failed hash).
    let (event_count, max_seq) = if events.is_empty() {
        let rep = replica.read().await;
        if rep.dirty_tables().is_empty() {
            return Ok(0);
        }
        info!(dirty = rep.dirty_tables().len(), "Nothing to apply; rehashing dirty tables from content");
        (0, rep.snapshot_seq())
    } else {
        // Persist the marker before the first row write; if that fails,
        // the touched tables are rehashed from content at this drain's end,
        // which is the same protection by a costlier route.
        let mut rep = replica.write().await;
        if let Err(e) = rep.begin_apply().await {
            warn!(error = %e, "Could not persist the applying marker; rehashing this batch's tables from content");
            rep.mark_tables_dirty(touched.iter().cloned());
        }
        (events.len(), events.last().map(|e| e.seq).unwrap_or(0))
    };

    // Apply in bounded chunks, releasing the write guard between chunks so
    // queued readers (/proxy pages, registration, integrity checks) interleave
    // instead of starving for the whole batch. Safe because `drain_lock`
    // serializes drains and the freeze protocol keeps bootstraps and drains
    // mutually exclusive — nobody hands out hashes mid-batch.
    const DRAIN_APPLY_CHUNK: usize = 256;
    for chunk in events.chunks(DRAIN_APPLY_CHUNK) {
        let mut rep = replica.write().await;
        for event in chunk {
            // Sync-excluded tables were never cloned into the replica, so
            // applying them is guaranteed to fail — `_00_heartbeat`'s probe
            // write logged "table does not exist" every 30s. The SSPs still
            // receive these events via the broadcast; the replica just has no
            // business storing them.
            if ssp_protocol::table_excluded_from_sync(&event.update.table) {
                continue;
            }
            let op = match event.update.operation {
                crate::messages::RecordOp::Create => crate::replica::RecordOp::Create,
                crate::messages::RecordOp::Update => crate::replica::RecordOp::Update,
                crate::messages::RecordOp::Delete => crate::replica::RecordOp::Delete,
            };
            if let Err(e) = rep
                .apply(
                    &event.update.table,
                    op,
                    &event.update.record_id,
                    event.update.data.clone(),
                )
                .await
            {
                error!(seq = event.seq, error = ?e, "Failed to apply event to snapshot");
            }
        }
    }

    // The per-table hashes were folded event by event inside `apply` above,
    // so this rehashes only tables the fold could not keep current (dirty).
    // Under a READ guard: a from-content hash pages a whole table out of
    // RocksDB, and holding the write lock for that starved /proxy for minutes
    // on large tables. The content can't move underneath us: drains are
    // serialized by `drain_lock` and ingest only appends to the buffer.
    let (hashed, failed) = {
        let rep = replica.read().await;
        // An empty explicit set: nothing beyond the dirty tables. `touched`
        // is only kept for the trace below.
        rep.compute_hashes_for(Some(&BTreeSet::new())).await
    };
    debug!(touched = touched.len(), rehashed = hashed.len(), "Drain hashes folded; rehashed only dirty tables");
    {
        let mut rep = replica.write().await;
        if let Err(e) = rep.commit_snapshot_state(max_seq, hashed, failed).await {
            error!(error = %e, "Failed to persist snapshot state");
        }
    }

    // Truncation rewrites the whole WAL file synchronously — blocking pool,
    // not a runtime worker (see handle_ingest's append for the same pattern).
    {
        let wal_guard = Arc::clone(wal).write_owned().await;
        let truncate_result = tokio::task::spawn_blocking(move || {
            let mut wal = wal_guard;
            wal.truncate(max_seq)
        })
        .await;
        match truncate_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "Failed to truncate WAL"),
            Err(e) => error!(error = %e, "WAL truncate task panicked"),
        }
    }

    Ok(event_count)
}

/// Scheduler lifecycle status
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SchedulerStatus {
    /// Initial DB clone in progress
    Cloning,
    /// Normal operation, snapshot unfrozen
    Ready,
    /// Snapshot frozen — SSP bootstrapping or catching up
    SnapshotFrozen,
    /// Batch-applying buffered events to snapshot
    SnapshotUpdating,
    /// Restore in progress — ingest, register, proxy are all rejected
    Restoring,
}

/// Main Scheduler service that orchestrates SurrealDB and SSP sidecars
pub struct Scheduler {
    config: SchedulerConfig,
    /// Filled in by `start()` once the shared root handle exists. The admin
    /// plane reads it through `admin::SharedDbSlot`; nothing else needs it,
    /// because every other consumer is handed the `Arc` directly at the point
    /// `start()` creates it.
    pub db_slot: crate::admin::SharedDbSlot,
    transport: Arc<HttpTransport>,
    pub replica: Arc<RwLock<Replica>>,
    pub ssp_pool: Arc<RwLock<SspPool>>,
    pub status: Arc<RwLock<SchedulerStatus>>,
    pub event_buffer: Arc<RwLock<VecDeque<BufferedEvent>>>,
    pub seq_counter: Arc<AtomicU64>,
    pub wal: Arc<RwLock<EventWal>>,
    /// Serializes every `drain_and_apply` caller (periodic updater, SSP
    /// registration, pre-backup) so none of them ever observes — or hands
    /// out hashes for — a half-applied batch.
    pub drain_lock: Arc<tokio::sync::Mutex<()>>,
    start_time: std::time::Instant,
    /// Upstream SurrealDB server version, queried once on connect and surfaced
    /// via `/info` (`"unknown"` until the bootstrap connect populates it).
    surrealdb_version: Arc<RwLock<String>>,
    /// Shared cap for concurrent schedule-observer tasks (see
    /// `IngestState::observer_permits`).
    observer_permits: Arc<tokio::sync::Semaphore>,
    /// Lock-free mirror of the replica's `snapshot_seq` (see
    /// `IngestState::snapshot_seq`).
    snapshot_seq_cell: Arc<AtomicU64>,
    /// E2E heartbeat probe results, surfaced via `/metrics` and `/health`.
    pub heartbeat: Arc<crate::heartbeat::HeartbeatStats>,
    /// The probe's timing config (also drives `/health` staleness math).
    pub heartbeat_config: crate::heartbeat::Config,
    /// Replica-vs-upstream drift bookkeeping (see `drift`), surfaced via
    /// `/health/snapshot` and `/metrics`.
    pub drift: Arc<RwLock<crate::drift::DriftState>>,
    pub drift_config: crate::drift::DriftConfig,
    /// Serializes replica re-clones (admin, breakers, drift) so two of them
    /// can never reset + re-ingest the shared replica concurrently.
    pub reclone_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Scheduler {
    /// Create a new Scheduler instance
    pub async fn new(config: SchedulerConfig, transport: Arc<HttpTransport>) -> Result<Self> {
        let strategy = config.load_balance.clone();

        // Initialize persistent replica with embedded SurrealDB/RocksDB
        let mut replica = Replica::new(
            config.replica_db_path.clone(),
        ).await?;

        // Initialize WAL
        let wal = EventWal::new(config.wal_path.clone())?;

        let (event_buffer, initial_seq) = recover_wal_backlog(&mut replica, &wal)?;

        let max_buffer_per_ssp = config.max_buffer_per_ssp;
        let snapshot_seq_cell = replica.snapshot_seq_cell();
        Ok(Self {
            config,
            db_slot: crate::admin::new_db_slot(),
            transport,
            replica: Arc::new(RwLock::new(replica)),
            ssp_pool: Arc::new(RwLock::new(SspPool::new(strategy, max_buffer_per_ssp))),
            status: Arc::new(RwLock::new(SchedulerStatus::Cloning)),
            event_buffer: Arc::new(RwLock::new(event_buffer)),
            seq_counter: Arc::new(AtomicU64::new(initial_seq)),
            wal: Arc::new(RwLock::new(wal)),
            drain_lock: Arc::new(tokio::sync::Mutex::new(())),
            start_time: std::time::Instant::now(),
            surrealdb_version: Arc::new(RwLock::new("unknown".to_string())),
            observer_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            snapshot_seq_cell,
            heartbeat: crate::heartbeat::HeartbeatStats::new(),
            heartbeat_config: crate::heartbeat::Config::from_env(),

            drift: Arc::new(RwLock::new(crate::drift::DriftState::default())),
            drift_config: crate::drift::DriftConfig::from_env(),
            reclone_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Get ingest state for HTTP handlers
    pub fn ingest_state(&self) -> crate::ingest::IngestState {
        crate::ingest::IngestState {
            replica: Arc::clone(&self.replica),
            transport: Arc::clone(&self.transport),
            ssp_pool: Arc::clone(&self.ssp_pool),
            status: Arc::clone(&self.status),
            event_buffer: Arc::clone(&self.event_buffer),
            seq_counter: Arc::clone(&self.seq_counter),
            wal: Arc::clone(&self.wal),
            drain_lock: Arc::clone(&self.drain_lock),
            db_config: Arc::new(self.config().db.clone()),
            job_tables: Arc::new(crate::schedule_engine::job_tables_from_env()),
            observer_permits: Arc::clone(&self.observer_permits),
            snapshot_seq: Arc::clone(&self.snapshot_seq_cell),
        }
    }

    /// Get query state for HTTP handlers
    pub fn query_state(&self) -> crate::query::QueryState {
        crate::query::QueryState {
            ssp_pool: Arc::clone(&self.ssp_pool),
            transport: Arc::clone(&self.transport),
            query_tracker: Arc::new(crate::query::QueryTracker::new()),
        }
    }

    /// Get job state for HTTP handlers
    pub fn job_state(&self) -> crate::job_scheduler::JobState {
        crate::job_scheduler::JobState {
            ssp_pool: Arc::clone(&self.ssp_pool),
            transport: Arc::clone(&self.transport),
            job_tracker: Arc::new(crate::job_scheduler::JobTracker::new()),
        }
    }

    /// Get metrics state for HTTP handlers
    pub fn metrics_state(
        &self,
        query_tracker: Arc<crate::query::QueryTracker>,
        job_tracker: Arc<crate::job_scheduler::JobTracker>,
        backend_health: crate::backend_health::BackendHealthCache,
        shared_backend_configs: crate::backend_health::SharedBackendConfigs,
    ) -> crate::metrics::MetricsState {
        crate::metrics::MetricsState {
            ssp_pool: Arc::clone(&self.ssp_pool),
            query_tracker,
            job_tracker,
            start_time: self.start_time,
            scheduler_id: self.config.scheduler_id.clone(),
            status: Arc::clone(&self.status),
            backend_health,
            shared_backend_configs,
            ingest: self.ingest_state(),
            replica: Arc::clone(&self.replica),
            surrealdb_version: Arc::clone(&self.surrealdb_version),
            heartbeat: Arc::clone(&self.heartbeat),
            heartbeat_config: self.heartbeat_config.clone(),
            drift: Arc::clone(&self.drift),
            drift_config: self.drift_config.clone(),
        }
    }

    /// The drift hook the snapshot updater and the startup pass run: upstream
    /// counts through `db`, remediation through the same re-clone the admin
    /// endpoint and the breakers use.
    fn drift_hook(&self, db: Arc<maintenance::db::ReconnectingDb>) -> Arc<crate::drift::DriftHook> {
        Arc::new(crate::drift::DriftHook {
            cfg: self.drift_config.clone(),
            upstream: Arc::new(crate::drift::SurrealUpstream { db }),
            state: Arc::clone(&self.drift),
            reclone: Arc::new(SchedulerRecloner {
                config: self.config.clone(),
                replica: Arc::clone(&self.replica),
                seq_counter: Arc::clone(&self.seq_counter),
                reclone_lock: Arc::clone(&self.reclone_lock),
                ssp_pool: Arc::clone(&self.ssp_pool),
            }),
        })
    }

    /// Get proxy state for HTTP handlers
    pub fn proxy_state(&self) -> crate::proxy::ProxyState {
        crate::proxy::ProxyState {
            replica: Arc::clone(&self.replica),
            status: Arc::clone(&self.status),
        }
    }

    /// Host hooks for the shared backup/restore workers.
    pub fn maintenance_host(&self) -> Arc<crate::maintenance_host::SchedulerHost> {
        Arc::new(crate::maintenance_host::SchedulerHost {
            ingest: self.ingest_state(),
        })
    }

    /// Get config
    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<()> {
        info!("Scheduler shutting down gracefully...");

        // Log replica state
        {
            let replica = self.replica.read().await;
            let count = replica.record_count().await.unwrap_or(0);
            info!("Replica has {} records, snapshot_seq={}", count, replica.snapshot_seq());
        }

        info!("Scheduler shutdown complete");
        Ok(())
    }

    /// Start the scheduler service
    pub async fn start(&self) -> Result<()> {
        info!("Starting Scheduler service...");

        // Step 1: Connect to remote SurrealDB
        info!(
            url = %self.config.db.url,
            namespace = %self.config.db.namespace,
            database = %self.config.db.database,
            user = %self.config.db.username,
            "Bootstrap target: ns={} db={} url={}",
            self.config.db.namespace,
            self.config.db.database,
            self.config.db.url,
        );
        // HTTP engine (raw: signin only) — the NS/DB self-heal below must run
        // before selecting them on the connection.
        let db = maintenance::db::connect_http_raw(&self.config.db).await?;

        // Self-heal NS/DB so the scheduler can bootstrap against a brand-new
        // SurrealDB instance — Phase 4a (CLI) usually defines these, but the
        // scheduler may be started against an upstream that hasn't run it yet.
        // Idempotent on populated DBs. Backtick-quote identifiers since the
        // configured names can contain hyphens.
        let ns = &self.config.db.namespace;
        let db_name = &self.config.db.database;
        db.query(format!("DEFINE NAMESPACE IF NOT EXISTS `{}`", ns))
            .await
            .with_context(|| format!("DEFINE NAMESPACE `{}` send failed", ns))?
            .check()
            .with_context(|| format!("DEFINE NAMESPACE `{}` returned an error", ns))?;

        db.use_ns(ns).await?;

        db.query(format!("DEFINE DATABASE IF NOT EXISTS `{}`", db_name))
            .await
            .with_context(|| format!("DEFINE DATABASE `{}` send failed", db_name))?
            .check()
            .with_context(|| format!("DEFINE DATABASE `{}` returned an error", db_name))?;

        db.use_db(db_name).await?;

        info!("Connected to SurrealDB");

        // Query the upstream SurrealDB server version once; surfaced via `/info`
        // (mirrors apps/ssp/src/lib.rs).
        match db.version().await {
            Ok(v) => *self.surrealdb_version.write().await = v.to_string(),
            Err(e) => info!(error = %e, "Could not read SurrealDB server version"),
        }

        // Step 2: Clear stale registered views from the remote DB (default).
        // Views are tied to live SSPs/clients, so leftover `_00_query` rows
        // from a prior scheduler run point at SSPs that no longer exist. Wiping
        // them before cloning starts the replica with a clean view registry;
        // clients notice their row is gone and re-register against the fresh
        // scheduler. Note the cost: the `_00_dbsp_cleanup` event on each row
        // reaches `/view/unregister` on THIS scheduler, whose tracker is empty
        // after a restart, so the edges are orphaned rather than deleted (the
        // SSP's next full publish of the same id replaces them).
        //
        // `clear_views_on_start = false` keeps the rows: the replica clones
        // them, the SSPs re-register them at bootstrap, and clients keep their
        // views across the restart with no re-registration at all. Rows nobody
        // heartbeats any more are retired by the SSP's TTL sweep once it is
        // ready, edges included. Single-SSP tenants only for now, see the
        // config field doc.
        if self.config.clear_views_on_start {
            info!("Clearing registered view data from remote SurrealDB...");
            trace!(ns = %self.config.db.namespace, db = %self.config.db.database, "remote query: DELETE _00_query");
            match db.query("DELETE _00_query").await {
                Ok(_) => {}
                Err(e) if crate::replica::is_missing_error(&e) => {
                    debug!("_00_query missing on remote — nothing to clear");
                }
                Err(e) => return Err(anyhow::Error::from(e)
                    .context("Failed to clear _00_query on remote")),
            }
        } else {
            info!("Keeping registered views across the restart (SPKY_CLEAR_VIEWS_ON_START=false); the SSP TTL sweep retires expired ones");
        }

        // Step 3: Clone remote DB into local snapshot replica — only when
        // there's nothing already persisted. A non-zero `snapshot_seq` means
        // `Replica::new` restored a real snapshot (with hashes + known tables)
        // from `_00_metadata:snapshot`, and re-cloning would wipe that state
        // and reset `snapshot_seq` to whatever the in-memory counter says.
        // `spky dev --clean` deletes `.sp00ky/scheduler_data`, which is what
        // forces `snapshot_seq == 0` and triggers a fresh clone here.
        let needs_bootstrap = {
            let replica = self.replica.read().await;
            replica.snapshot_seq() == 0
        };

        if needs_bootstrap {
            info!(
                timeout_secs = self.config.clone_timeout_secs,
                "No persisted snapshot found — cloning remote database...",
            );
            let mut replica = self.replica.write().await;

            // The replica may still hold orphan records from a prior startup
            // that ingested some tables and then crashed before persisting
            // `_00_metadata:snapshot`. Without a wipe, `ingest_all` re-issues
            // `CREATE` on those rows and fails with "record already exists",
            // so reset before re-cloning. Safe because `needs_bootstrap` is
            // gated on `snapshot_seq == 0` — no committed snapshot to lose.
            replica.reset().await.context("Failed to reset replica before bootstrap")?;

            trace!(
                ns = %self.config.db.namespace,
                db = %self.config.db.database,
                "starting replica.ingest_all from remote"
            );
            // Bounded, because a clone that hangs is not a slow clone: it is a
            // scheduler that answers 503 to every SSP registration for as long
            // as the process lives, with nothing to restart it and no error to
            // report. That is exactly how whitepawn lost 57h of sync on
            // 2026-08-22 — the embedded RocksDB stalled its writes mid-clone
            // and every thread parked at 0% CPU, forever.
            //
            // A timeout here fails `start()`, which exits the process, which
            // the container restart policy turns into another attempt. A
            // crash-loop is a bad state; it is a strictly better bad state
            // than a silent one.
            match tokio::time::timeout(
                Duration::from_secs(self.config.clone_timeout_secs),
                async {
                    replica.ingest_all(&db).await?;
                    // Pass `None` for touched_tables so set_snapshot_state
                    // hashes every table we just ingested — that hash is the
                    // integrity baseline an SSP gets handed at /ssp/register.
                    let current_seq = self.seq_counter.load(Ordering::SeqCst);
                    replica.set_snapshot_state(current_seq, None).await?;
                    anyhow::Ok(())
                },
            )
            .await
            {
                Ok(res) => res?,
                Err(_) => {
                    error!(
                        timeout_secs = self.config.clone_timeout_secs,
                        "Snapshot clone did not finish in time — exiting so the \
                         container restarts instead of serving 503 forever. If this \
                         repeats, the replica's RocksDB is stalling: raise the \
                         scheduler's memory cap (it sizes the write-buffer budget) \
                         or raise SPKY_CLONE_TIMEOUT_SECS for a genuinely slow clone.",
                    );
                    anyhow::bail!(
                        "snapshot clone exceeded {}s",
                        self.config.clone_timeout_secs
                    );
                }
            }

            let hashes = replica.snapshot_hashes();
            info!(
                tables = hashes.len(),
                "Snapshot integrity hashes computed: {:?}",
                hashes
                    .iter()
                    .map(|(t, h)| (t.as_str(), &h[..h.len().min(11)]))
                    .collect::<Vec<_>>(),
            );
            info!("Snapshot clone complete");
        } else {
            // A table the upstream stopped syncing since this snapshot was
            // written (`-- @nosync` added, or dropped) must not reach the
            // SSPs: they leave it out of their own load and dispute its hash
            // on every bootstrap, so the cluster never comes up until the
            // volume is wiped. Trim to what a fresh clone would load. A failed
            // discovery keeps the persisted set: the drift check below needs
            // the upstream anyway and reports it properly.
            match Replica::discover_sync_tables(&db).await {
                Ok(tables) => {
                    let current: BTreeSet<String> = tables.into_iter().collect();
                    let mut replica = self.replica.write().await;
                    match replica.forget_tables_not_in(&current).await {
                        Ok(gone) if !gone.is_empty() => warn!(
                            tables = ?gone,
                            "Dropped persisted tables the upstream no longer syncs (@nosync or removed)"
                        ),
                        Ok(_) => {}
                        Err(e) => warn!(error = %e, "Could not trim persisted tables; SSP bootstraps may dispute them"),
                    }
                }
                Err(e) => warn!(error = %e, "Could not discover upstream tables at boot; keeping the persisted set"),
            }
            let replica = self.replica.read().await;
            info!(
                snapshot_seq = replica.snapshot_seq(),
                tables = replica.snapshot_hashes().len(),
                known_tables = replica.known_tables().len(),
                "Reusing persisted snapshot — skipping bootstrap clone"
            );
        }

        // No boot-time rehash from content. It used to page every table out
        // of RocksDB on every start (two to four minutes on whitepawn, during
        // which no SSP could register and every upstream write was refused).
        // The two ways persisted hashes go stale are covered without it:
        // - a crash between applying and committing: the WAL backlog's tables
        //   were marked dirty in `new()` and are rehashed at the first drain;
        // - content changed underneath (bad backup, manual edit): the first
        //   SSP to bootstrap disputes the table and `handle_bootstrap_verify`
        //   rehashes exactly the disputed tables from content.
        // `startup_integrity_check` stays available for an explicit full check.
        {
            let replica = self.replica.read().await;
            let dirty = replica.dirty_tables().len();
            info!(
                tables = replica.snapshot_hashes().len(),
                dirty,
                "Persisted table hashes reused; stale ones are repaired at the first drain or on dispute"
            );
        }

        // Bootstrap is done with the raw handle; hand it to the long-lived
        // consumers wrapped so a SurrealDB restart (which drops the server-side
        // session this handle is pinned to) is recoverable without restarting
        // the scheduler.
        //
        // One `ReconnectingDb` shared by every consumer, deliberately: each
        // `Surreal::clone()` attaches its OWN server-side session, so the
        // previous `db.clone()` per consumer meant several independent sessions
        // to lose and several to re-establish.
        let shared_db = maintenance::db::ReconnectingDb::new(db, self.config.db.clone());
        // Publish it for the admin plane, which came up with the HTTP servers
        // (before this point) and answers 503 until this lands.
        *self.db_slot.write().await = Some(Arc::clone(&shared_db));
        let drift_hook = self.drift_hook(Arc::clone(&shared_db));

        // The check the integrity check above cannot do: compare the replica
        // against UPSTREAM. A persisted snapshot that missed writes made while
        // nothing was listening (a bulk migration with the stack down) is
        // internally consistent and serves every SSP an empty table. Nothing
        // is buffered yet, so counts are comparable, and no SSP can register
        // until `Ready`, so a re-clone here costs nobody a bootstrap.
        let drift_started = std::time::Instant::now();
        match crate::drift::run_check(&drift_hook, &self.replica).await {
            crate::drift::Action::Clean => info!(
                elapsed_ms = drift_started.elapsed().as_millis() as u64,
                "Startup drift check passed"
            ),
            other => warn!(
                action = ?other,
                elapsed_ms = drift_started.elapsed().as_millis() as u64,
                "Startup drift check acted"
            ),
        }

        // Transition to Ready
        *self.status.write().await = SchedulerStatus::Ready;
        info!(
            boot_ms = self.start_time.elapsed().as_millis() as u64,
            "Scheduler is ready and running"
        );

        // Step 3: Spawn periodic snapshot update task (which also runs the
        // periodic drift check after each drain).
        self.spawn_snapshot_updater(Some(drift_hook));

        // Keep the handle's HTTP auth token fresh, and replace the handle
        // outright if its session dies.
        maintenance::db::spawn_periodic_resignin(
            Arc::clone(&shared_db),
            maintenance::db::RESIGNIN_INTERVAL_SECS,
        );

        // Spawn the feature-flag materialization sweep. The `spky flag` CLI
        // materializes existing users inline on every write; this periodic
        // pass fills in users who signed up since (and self-heals after an
        // interrupted CLI run).
        crate::feature_flags::spawn(
            Arc::clone(&shared_db),
            self.config.feature_flag_sweep_interval_secs,
        );

        // E2E heartbeat probe: writes _00_heartbeat:probe upstream, watches
        // it round-trip through /ingest → broadcast → every ready SSP.
        crate::heartbeat::spawn(
            shared_db,
            Arc::clone(&self.ssp_pool),
            Arc::clone(&self.transport),
            Arc::clone(&self.heartbeat),
            self.heartbeat_config.clone(),
        );

        // Keep running until shutdown signal
        tokio::signal::ctrl_c().await?;

        Ok(())
    }

    /// Recompute every table's hash from the current replica state and
    /// compare against the persisted `snapshot_hashes`. On a mismatch the
    /// replica *content* wins — it is what `/proxy` serves to bootstrapping
    /// SSPs — so the persisted hashes are recomputed from it. Without this
    /// repair, every SSP registers against stale hashes, fails its bootstrap
    /// integrity check, and exit(2)s in a loop that survives plain restarts.
    ///
    /// This cannot detect replica-vs-*upstream* divergence; that remains
    /// `POST /admin/resync` (mode `reclone`) / `spky verify --fix` territory.
    /// Public for the integration tests.
    pub async fn startup_integrity_check(&self) -> Result<()> {
        let diffs = {
            let replica = self.replica.read().await;
            let persisted = replica.snapshot_hashes().clone();
            let fresh = replica.compute_table_hashes().await?;

            let diffs = ssp_protocol::snapshot_hash::diff_table_hashes(&persisted, &fresh);
            if diffs.is_empty() {
                info!(tables = persisted.len(), "Startup integrity check passed");
                return Ok(());
            }
            diffs
        };

        for d in &diffs {
            error!(
                table = %d.table,
                persisted = %d.a,
                actual = %d.b,
                "Startup integrity mismatch"
            );
        }

        // Re-acquire as write and rehash everything from content. Runs before
        // the status flips to Ready and before any SSP can register, so there
        // is no concurrent reader to invalidate.
        {
            let mut replica = self.replica.write().await;
            let seq = replica.snapshot_seq();
            replica
                .set_snapshot_state(seq, None)
                .await
                .context("startup integrity repair: rehash failed")?;
        }
        warn!(
            repaired = diffs.len(),
            "Persisted snapshot hashes disagreed with replica content — rehashed from content"
        );
        Ok(())
    }

    /// Spawn a background task that periodically applies buffered events to the snapshot
    fn spawn_snapshot_updater(&self, drift: Option<Arc<crate::drift::DriftHook>>) {
        let interval_secs = self.config.snapshot_update_interval_secs;
        // Strictly larger than the bootstrap poll task's own timeout, so a
        // parked SSP is only evicted once its poll task is guaranteed dead.
        let stale_bootstrap_max_age =
            std::time::Duration::from_secs(self.config.bootstrap_timeout_secs + 60);
        let status = Arc::clone(&self.status);
        let event_buffer = Arc::clone(&self.event_buffer);
        let replica = Arc::clone(&self.replica);
        let ssp_pool = Arc::clone(&self.ssp_pool);
        let wal = Arc::clone(&self.wal);
        let drain_lock = Arc::clone(&self.drain_lock);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_secs(interval_secs)
            );
            // Skip the first immediate tick
            interval.tick().await;

            loop {
                interval.tick().await;
                // The loop is serial: a tick that never returns takes every
                // later drain with it, and the only symptom is silence — no
                // "Snapshot update complete", `/health` still ready, and
                // `pending_events` climbing for hours (whitepawn, 2026-09-09,
                // a tick parked in the drift check's upstream `count()`).
                // Nothing here can safely abort a tick mid-drain, so say so
                // instead, loudly and repeatedly, naming the elapsed time.
                let started = std::time::Instant::now();
                let tick = snapshot_updater_tick(
                    &status,
                    &event_buffer,
                    &replica,
                    &ssp_pool,
                    &wal,
                    &drain_lock,
                    stale_bootstrap_max_age,
                    drift.as_deref(),
                );
                tokio::pin!(tick);
                loop {
                    tokio::select! {
                        _ = &mut tick => break,
                        _ = tokio::time::sleep(
                            std::time::Duration::from_secs(TICK_OVERDUE_WARN_SECS),
                        ) => warn!(
                            elapsed_secs = started.elapsed().as_secs(),
                            "Snapshot updater tick has not returned; the replica is not draining"
                        ),
                    }
                }
            }
        });
    }
}

/// The drift module's remediation, bound to this scheduler's replica: the same
/// re-clone the admin endpoint and the bootstrap/catch-up breakers run, plus
/// flagging every SSP so it re-bootstraps on its next heartbeat.
struct SchedulerRecloner {
    config: SchedulerConfig,
    replica: Arc<RwLock<Replica>>,
    seq_counter: Arc<AtomicU64>,
    reclone_lock: Arc<tokio::sync::Mutex<()>>,
    ssp_pool: Arc<RwLock<SspPool>>,
}

#[async_trait::async_trait]
impl crate::drift::Recloner for SchedulerRecloner {
    async fn reclone_and_resync(&self) -> Result<bool> {
        let done = crate::ssp_management::reclone_replica_from_upstream(
            &self.config,
            &self.replica,
            &self.seq_counter,
            &self.reclone_lock,
        )
        .await?;
        if done {
            let marked = self.ssp_pool.write().await.mark_all_for_resync();
            info!(marked, "Drift re-clone: SSPs flagged for re-bootstrap");
        }
        Ok(done)
    }
}

/// One iteration of the periodic snapshot updater. Extracted from the spawn
/// loop so the eviction/self-recovery/drain sequence is unit-testable.
///
/// Self-recovery: `SnapshotFrozen` is only ever cleared by a *successful*
/// bootstrap poll (or its error handler). If neither ran — the scheduler
/// restarted mid-bootstrap, or a prior updater iteration panicked and left
/// `SnapshotUpdating` pinned — nothing else would ever set `Ready` again and
/// the drain (and with it WAL truncation and `pending_events`) would stall
/// forever. Since step 1 has already established there is no active
/// bootstrap, latched `SnapshotFrozen`/`SnapshotUpdating` here is provably
/// orphaned and safe to recover from.
/// A drain that takes longer than this is worth a warning line of its own.
const DRAIN_SLOW_WARN_SECS: u64 = 60;

/// How often a caller still waiting for `drain_lock` says so.
const DRAIN_LOCK_WAIT_WARN_SECS: u64 = 30;

/// How often a snapshot updater tick that has not returned says so.
const TICK_OVERDUE_WARN_SECS: u64 = 60;

/// Take `drain_lock`, and say so in the log while the wait drags on.
///
/// Every holder of this lock is bounded in principle (a drain, a registration
/// critical section, a rehash), but a holder that hangs — a stuck replica
/// write, an SSP that never answers its bootstrap poll — turns every other
/// caller into a silent wait, and the only visible symptom is a replica that
/// stops advancing. Logging the wait, with who is waiting, is what turns that
/// into a diagnosable incident.
pub async fn acquire_drain_lock<'a>(
    drain_lock: &'a Arc<tokio::sync::Mutex<()>>,
    who: &str,
) -> tokio::sync::MutexGuard<'a, ()> {
    let started = std::time::Instant::now();
    loop {
        match tokio::time::timeout(
            std::time::Duration::from_secs(DRAIN_LOCK_WAIT_WARN_SECS),
            drain_lock.lock(),
        )
        .await
        {
            Ok(guard) => {
                let waited = started.elapsed().as_secs();
                if waited >= DRAIN_LOCK_WAIT_WARN_SECS {
                    warn!(who, waited_secs = waited, "drain_lock acquired after a long wait");
                }
                return guard;
            }
            Err(_) => {
                warn!(
                    who,
                    waited_secs = started.elapsed().as_secs(),
                    "still waiting for drain_lock; another holder has not released it"
                );
            }
        }
    }
}

pub async fn snapshot_updater_tick(
    status: &Arc<RwLock<SchedulerStatus>>,
    event_buffer: &Arc<RwLock<VecDeque<BufferedEvent>>>,
    replica: &Arc<RwLock<Replica>>,
    ssp_pool: &Arc<RwLock<SspPool>>,
    wal: &Arc<RwLock<EventWal>>,
    drain_lock: &Arc<tokio::sync::Mutex<()>>,
    stale_bootstrap_max_age: std::time::Duration,
    drift: Option<&crate::drift::DriftHook>,
) {
    // Step 1: evict SSPs parked in Bootstrapping/Replaying past the bound —
    // their poll task is dead, and they would otherwise hold
    // `has_active_bootstrap()` (and with it the snapshot freeze) forever.
    {
        let mut pool = ssp_pool.write().await;
        for id in pool.stale_active_bootstraps(stale_bootstrap_max_age) {
            warn!(
                ssp_id = %id,
                max_age_secs = stale_bootstrap_max_age.as_secs(),
                "Evicting SSP stuck in bootstrap/replay state"
            );
            pool.remove(&id);
        }
    }

    // Everything below runs under `drain_lock`. Registration freezes the
    // status AND inserts the SSP into the pool inside the same lock, so a
    // lock-holder here sees a consistent world: either the registration
    // completed (active bootstrap visible → skip) or it hasn't started its
    // critical section (safe to drain; it will capture post-drain hashes).
    let _guard = acquire_drain_lock(drain_lock, "snapshot updater").await;

    // Step 2: never drain while an SSP holds bootstrap hashes.
    if ssp_pool.read().await.has_active_bootstrap() {
        info!("Skipping snapshot update: SSPs are bootstrapping");
        return;
    }

    // Step 3: status gate with self-recovery (see doc comment). Safe under
    // the lock: no registration can be mid-critical-section, so a latched
    // SnapshotFrozen/SnapshotUpdating with no active bootstrap is orphaned.
    //
    // Advertise `SnapshotUpdating` only when there is actually something to
    // apply. This used to be unconditional, so every tick flipped the status
    // on an idle cluster — and `/health` reads exactly that status as
    // `stalled` (see `metrics::health_check`), which reported the whole stack
    // "degraded" once per `snapshot_update_interval_secs`, forever, for a tick
    // that had no work to do. The drain below still runs either way: it is a
    // no-op on an empty buffer, and leaving it unconditional means an event
    // that lands between this peek and the drain is applied on this tick
    // rather than waiting out a whole interval.
    let has_backlog = !event_buffer.read().await.is_empty();
    {
        let mut st = status.write().await;
        match *st {
            SchedulerStatus::Ready => {}
            SchedulerStatus::SnapshotFrozen | SchedulerStatus::SnapshotUpdating => {
                warn!(
                    status = ?*st,
                    "Snapshot status latched with no active bootstrap — self-recovering to Ready"
                );
            }
            SchedulerStatus::Cloning | SchedulerStatus::Restoring => {
                info!("Skipping snapshot update: scheduler status is {:?}", *st);
                return;
            }
        }
        *st = if has_backlog {
            SchedulerStatus::SnapshotUpdating
        } else {
            // Also the landing point for the self-recovery branch above.
            SchedulerStatus::Ready
        };
    }

    // Step 4: drain, in a child task so a panic can't kill the updater loop
    // (or leave the status pinned at SnapshotUpdating).
    let drain_started = std::time::Instant::now();
    let res = {
        let (buffer, rep, wal) = (
            Arc::clone(event_buffer),
            Arc::clone(replica),
            Arc::clone(wal),
        );
        tokio::spawn(async move { drain_and_apply(&buffer, &rep, &wal).await }).await
    };
    // The drain runs while `drain_lock` is held, so a drain that never
    // returns silently stalls every later tick (they queue on the lock
    // without logging) and the replica falls behind while `/health` still
    // says ready. Observed 2026-09-06: no "Snapshot update" line for 30 min,
    // pending events climbing past 3900, nothing in the log to say why. The
    // wait for the lock is reported by `acquire_drain_lock`; the drain's own
    // duration is reported here so the two can be told apart.
    let drain_secs = drain_started.elapsed().as_secs();
    if drain_secs >= DRAIN_SLOW_WARN_SECS {
        warn!(drain_secs, "Snapshot drain took unusually long");
    }
    match res {
        Ok(Ok(0)) => {}
        Ok(Ok(event_count)) => info!(event_count, drain_secs, "Snapshot update complete"),
        Ok(Err(e)) => error!(error = %e, "Snapshot update failed"),
        Err(join_err) => {
            error!(error = %join_err, "Snapshot update task panicked — status restored to Ready")
        }
    }

    // Step 5: back to Ready. Still under the lock, so no registration has
    // frozen the status since step 3 — the write can't clobber a freeze.
    {
        let mut st = status.write().await;
        if *st == SchedulerStatus::SnapshotUpdating {
            *st = SchedulerStatus::Ready;
        }
    }

    // Step 6: replica-vs-upstream drift check, only on a tick whose drain
    // left nothing buffered (otherwise the replica legitimately trails
    // upstream by exactly what is still buffered and the counts say nothing).
    // The lock is released first: a re-clone takes the replica write lock for
    // minutes and must not hold up registrations behind `drain_lock` with it.
    let Some(hook) = drift else { return };
    if !hook.cfg.enabled {
        return;
    }
    let drained = event_buffer.read().await.is_empty();
    drop(_guard);
    if !drained {
        debug!("Skipping drift check: events still buffered after the drain");
        return;
    }
    crate::drift::run_check(hook, replica).await;
}

#[cfg(test)]
mod drain_tests {
    use super::*;
    use crate::messages::{RecordOp, RecordUpdate};

    fn ev(seq: u64, table: &str) -> BufferedEvent {
        BufferedEvent {
            seq,
            update: RecordUpdate {
                table: table.to_string(),
                operation: RecordOp::Update,
                record_id: format!("{}:r1", table),
                data: Some(serde_json::json!({"a": 1})),
                version: 0,
            },
            received_at: 0,
        }
    }

    #[test]
    fn touched_includes_synced_meta_tables() {
        // `Replica::apply` writes these into the replica, so the drain must
        // rehash them too — a raw `_00_` prefix filter left their cached hash
        // stale and broke every later bootstrap integrity check.
        let events = vec![
            ev(1, "game"),
            ev(2, "_00_user_feature"),
            ev(3, "_00_app_release"),
        ];
        let touched = touched_tables(&events);
        assert!(touched.contains("game"));
        assert!(touched.contains("_00_user_feature"));
        assert!(touched.contains("_00_app_release"));
    }

    #[test]
    fn touched_excludes_runtime_internal_meta_tables() {
        let events = vec![ev(1, "_00_query"), ev(2, "_00_list_ref_user_abc"), ev(3, "user")];
        let touched = touched_tables(&events);
        assert_eq!(touched.len(), 1);
        assert!(touched.contains("user"));
    }
}
