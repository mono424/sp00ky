use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{error, info};

use crate::db::{connect_http, DbConfig};
use crate::host::MaintenanceHost;
use crate::s3::BackupConfig;

const RECENT_JOB_LIMIT: usize = 50;
const RESTORE_QUEUE_CAPACITY: usize = 8;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RestoreStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

/// Serialized field names are API surface (spooky-cloud reads `snapshot_seq`);
/// `replica_restored` historically meant "the scheduler's replica was
/// restored" and now means "host-specific state was restored" (replica for the
/// scheduler, circuit re-bootstrap for a standalone SSP). `ssps_evicted` is
/// scheduler-specific and null for other hosts.
#[derive(Debug, Clone, Serialize)]
pub struct RestoreJobState {
    pub restore_id: String,
    pub backup_id: String,
    pub project_slug: String,
    pub storage_path: String,
    pub status: RestoreStatus,
    pub enqueued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub snapshot_seq: Option<u64>,
    pub pending_cleared: Option<usize>,
    pub main_db_restored: bool,
    pub replica_restored: bool,
    pub ssps_evicted: Option<usize>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RestoreJob {
    pub restore_id: String,
    pub backup_id: String,
    pub project_slug: String,
    pub storage_path: String,
}

pub struct RestoreRegistry {
    jobs: RwLock<HashMap<String, RestoreJobState>>,
    order: RwLock<VecDeque<String>>,
}

impl RestoreRegistry {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            order: RwLock::new(VecDeque::new()),
        }
    }

    pub async fn contains(&self, id: &str) -> bool {
        self.jobs.read().await.contains_key(id)
    }

    pub async fn enqueue(
        &self,
        restore_id: String,
        backup_id: String,
        project_slug: String,
        storage_path: String,
    ) -> RestoreJobState {
        let state = RestoreJobState {
            restore_id: restore_id.clone(),
            backup_id,
            project_slug,
            storage_path,
            status: RestoreStatus::Queued,
            enqueued_at: Utc::now(),
            started_at: None,
            finished_at: None,
            snapshot_seq: None,
            pending_cleared: None,
            main_db_restored: false,
            replica_restored: false,
            ssps_evicted: None,
            error: None,
        };
        self.jobs.write().await.insert(restore_id.clone(), state.clone());
        self.order.write().await.push_back(restore_id);
        state
    }

    async fn update<F: FnOnce(&mut RestoreJobState)>(&self, id: &str, f: F) {
        if let Some(state) = self.jobs.write().await.get_mut(id) {
            f(state);
        }
    }

    pub async fn mark_running(&self, id: &str) {
        self.update(id, |s| {
            s.status = RestoreStatus::Running;
            s.started_at = Some(Utc::now());
        })
        .await;
    }

    pub async fn mark_completed(&self, id: &str, outcome: RestoreOutcome) {
        self.update(id, |s| {
            s.status = RestoreStatus::Completed;
            s.finished_at = Some(Utc::now());
            s.snapshot_seq = outcome.snapshot_seq;
            s.pending_cleared = Some(outcome.pending_cleared);
            s.main_db_restored = outcome.main_db_restored;
            s.replica_restored = outcome.host_state_restored;
            s.ssps_evicted = outcome.ssps_evicted;
        })
        .await;
        self.trim().await;
    }

    pub async fn mark_failed(&self, id: &str, err: String, progress: RestoreProgress) {
        self.update(id, |s| {
            s.status = RestoreStatus::Failed;
            s.finished_at = Some(Utc::now());
            s.error = Some(err);
            s.main_db_restored = progress.main_db_restored;
            s.replica_restored = progress.host_state_restored;
        })
        .await;
        self.trim().await;
    }

    async fn trim(&self) {
        let mut jobs = self.jobs.write().await;
        let mut order = self.order.write().await;
        let finished: Vec<String> = order
            .iter()
            .filter(|id| {
                jobs.get(id.as_str())
                    .map(|s| matches!(s.status, RestoreStatus::Completed | RestoreStatus::Failed))
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if finished.len() <= RECENT_JOB_LIMIT {
            return;
        }
        let to_drop = finished.len() - RECENT_JOB_LIMIT;
        for id in finished.into_iter().take(to_drop) {
            jobs.remove(&id);
            if let Some(pos) = order.iter().position(|x| x == &id) {
                order.remove(pos);
            }
        }
    }

    pub async fn get(&self, id: &str) -> Option<RestoreJobState> {
        self.jobs.read().await.get(id).cloned()
    }

    pub async fn current_running(&self) -> Option<RestoreJobState> {
        self.jobs
            .read()
            .await
            .values()
            .find(|s| matches!(s.status, RestoreStatus::Running))
            .cloned()
    }

    pub async fn queue_len(&self) -> usize {
        self.jobs
            .read()
            .await
            .values()
            .filter(|s| matches!(s.status, RestoreStatus::Queued))
            .count()
    }

    pub async fn recent(&self) -> Vec<RestoreJobState> {
        let jobs = self.jobs.read().await;
        let order = self.order.read().await;
        order
            .iter()
            .filter_map(|id| jobs.get(id).cloned())
            .collect()
    }
}

impl Default for RestoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub fn create_restore_channel() -> (mpsc::Sender<RestoreJob>, mpsc::Receiver<RestoreJob>) {
    mpsc::channel(RESTORE_QUEUE_CAPACITY)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RestoreProgress {
    /// True once `begin_restore` succeeded — only then is the host's
    /// finish_restore transition invoked (a failure before the gate leaves
    /// host status untouched, matching pre-refactor behavior).
    pub gate_entered: bool,
    pub main_db_restored: bool,
    pub host_state_restored: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct RestoreOutcome {
    pub snapshot_seq: Option<u64>,
    pub pending_cleared: usize,
    pub main_db_restored: bool,
    pub host_state_restored: bool,
    pub ssps_evicted: Option<usize>,
}

pub async fn run_restore_worker(
    mut rx: mpsc::Receiver<RestoreJob>,
    host: Arc<dyn MaintenanceHost>,
    s3_config: Arc<BackupConfig>,
    db_config: Arc<DbConfig>,
    registry: Arc<RestoreRegistry>,
    lock: Arc<Mutex<()>>,
) {
    info!("Restore worker started");
    while let Some(job) = rx.recv().await {
        registry.mark_running(&job.restore_id).await;
        info!(
            restore_id = %job.restore_id,
            backup_id = %job.backup_id,
            project = %job.project_slug,
            "Restore worker running job"
        );

        let mut progress = RestoreProgress::default();
        let result = execute_restore(
            &job,
            host.as_ref(),
            &s3_config,
            &db_config,
            &lock,
            &mut progress,
        )
        .await;

        // Host decides whether it is safe to serve traffic again — but only
        // if the gate was ever entered; earlier failures never touched status.
        if progress.gate_entered {
            host.finish_restore(&result, progress).await;
        }

        match result {
            Ok(outcome) => {
                registry.mark_completed(&job.restore_id, outcome).await;
                info!(
                    restore_id = %job.restore_id,
                    snapshot_seq = ?outcome.snapshot_seq,
                    pending_cleared = outcome.pending_cleared,
                    ssps_evicted = ?outcome.ssps_evicted,
                    "Restore completed"
                );
            }
            Err(e) => {
                let err_str = format!("{:#}", e);
                registry
                    .mark_failed(&job.restore_id, err_str.clone(), progress)
                    .await;
                error!(
                    restore_id = %job.restore_id,
                    error = %err_str,
                    main_db_restored = progress.main_db_restored,
                    host_state_restored = progress.host_state_restored,
                    "Restore failed"
                );
            }
        }
    }
    info!("Restore worker exiting (channel closed)");
}

async fn execute_restore(
    job: &RestoreJob,
    host: &dyn MaintenanceHost,
    s3_config: &BackupConfig,
    db_config: &DbConfig,
    lock: &Arc<Mutex<()>>,
    progress: &mut RestoreProgress,
) -> Result<RestoreOutcome> {
    // 1. Download the gzipped dump from S3.
    let bucket = s3_config
        .get_bucket()
        .context("Failed to build S3 bucket client")?;
    //    Streamed to disk: a large project's dump does not fit in the host's
    //    memory limit, packed or unpacked.
    let gz = tempfile::NamedTempFile::new().context("Failed to create tempfile for download")?;
    {
        let mut file = tokio::fs::File::create(gz.path())
            .await
            .context("Failed to open download tempfile")?;
        let status = bucket
            .get_object_to_writer(job.storage_path.as_str(), &mut file)
            .await
            .with_context(|| format!("Failed to download {} from S3", job.storage_path))?;
        if status != 200 {
            anyhow::bail!(
                "S3 download returned status {} for {}",
                status,
                job.storage_path
            );
        }
        file.flush().await.context("Failed to write download tempfile")?;
    }

    // 2. Gunzip file to file.
    let tmp = tempfile::NamedTempFile::new().context("Failed to create tempfile for dump")?;
    let (src, dst) = (gz.path().to_path_buf(), tmp.path().to_path_buf());
    tokio::task::spawn_blocking(move || -> Result<()> {
        let input = File::open(&src).context("Failed to open downloaded dump")?;
        let mut decoder = GzDecoder::new(BufReader::new(input));
        let mut output =
            BufWriter::new(File::create(&dst).context("Failed to create dump tempfile")?);
        std::io::copy(&mut decoder, &mut output).context("Failed to decompress S3 artifact")?;
        output.flush().context("Failed to write dump tempfile")?;
        Ok(())
    })
    .await
    .context("Gunzip task panicked")??;
    drop(gz);
    let dump_path = tmp.path().to_path_buf();

    // 3. Serialize with the backup worker — only one DB-mutating op at a time.
    let _guard = lock.lock().await;

    // 4. Host gate: refuse unless restorable, block ingest while restoring.
    host.begin_restore()
        .await
        .context("Host refused restore")?;
    progress.gate_entered = true;

    // 5. Restore the main remote SurrealDB.
    let remote = connect_http(db_config)
        .await
        .context("Failed to connect to main SurrealDB for restore")?;

    // Wipe: REMOVE DATABASE drops every table/record inside, DEFINE recreates.
    let wipe_sql = format!(
        "REMOVE DATABASE IF EXISTS {db}; DEFINE DATABASE {db}; USE DB {db};",
        db = db_config.database
    );
    remote
        .query(&wipe_sql)
        .await
        .context("Failed to wipe remote database before import")?;
    // Re-select the database on the connection after the REMOVE/DEFINE cycle.
    remote
        .use_db(&db_config.database)
        .await
        .context("Failed to re-select remote database after wipe")?;

    remote
        .import(&dump_path)
        .await
        .context("Failed to import dump into main SurrealDB")?;
    progress.main_db_restored = true;
    info!(restore_id = %job.restore_id, "Main SurrealDB restored");

    // 6. Host resynchronizes its own state from the restored database.
    let outcome = host
        .post_restore(&dump_path)
        .await
        .context("Host post-restore step failed")?;
    progress.host_state_restored = outcome.host_state_restored;

    Ok(outcome)
}
