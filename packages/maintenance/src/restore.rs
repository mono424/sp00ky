use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tracing::{error, info, warn};

use crate::db::{connect_http, DbConfig};
use crate::host::MaintenanceHost;
use crate::s3::BackupConfig;

const RECENT_JOB_LIMIT: usize = 50;
const RESTORE_QUEUE_CAPACITY: usize = 8;

/// How long a held restore waits for its point-in-time replay. The main
/// database is restored and the ingest gate is closed the whole time, so this
/// is also the longest the project can sit in `Restoring` on the cloud's
/// account.
pub const REPLAY_HOLD_TIMEOUT: Duration = Duration::from_secs(3 * 60 * 60);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RestoreStatus {
    Queued,
    Running,
    /// A held restore imported the main database and waits for Sp00ky Cloud
    /// to replay the change feed up to the chosen moment
    /// (`POST /backup/restore/:id/continue`).
    #[serde(rename = "awaiting_replay")]
    AwaitingReplay,
    Completed,
    Failed,
}

/// What the cloud reports once it has replayed the change feed onto the
/// main database of a held restore.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReplayOutcome {
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
}

/// Why a continue signal did not reach a held restore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinueResult {
    Delivered,
    /// The restore exists but is not waiting for a replay (queued, running,
    /// finished, or its hold just timed out).
    NotAwaiting,
    Unknown,
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
    /// Stop after the main database import and wait for a point-in-time
    /// replay (`POST /backup/restore/held`). The replica is then rebuilt from
    /// an export of the main database instead of from the dump.
    pub hold_for_replay: bool,
}

pub struct RestoreRegistry {
    jobs: RwLock<HashMap<String, RestoreJobState>>,
    order: RwLock<VecDeque<String>>,
    /// The continue channel of each restore that is awaiting its replay.
    holds: Mutex<HashMap<String, oneshot::Sender<ReplayOutcome>>>,
}

impl RestoreRegistry {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            order: RwLock::new(VecDeque::new()),
            holds: Mutex::new(HashMap::new()),
        }
    }

    /// Marks a restore `awaiting_replay` and waits for [`continue_replay`]
    /// or `timeout`, whichever comes first, then marks it `running` again.
    /// A timeout reads as a failed replay.
    ///
    /// [`continue_replay`]: Self::continue_replay
    pub async fn await_replay(&self, id: &str, timeout: Duration) -> ReplayOutcome {
        let (tx, rx) = oneshot::channel();
        {
            let mut holds = self.holds.lock().await;
            holds.insert(id.to_string(), tx);
            self.update(id, |s| s.status = RestoreStatus::AwaitingReplay).await;
        }
        let outcome = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => ReplayOutcome {
                ok: false,
                error: Some("the point-in-time replay was abandoned".to_string()),
            },
            Err(_) => ReplayOutcome {
                ok: false,
                error: Some(format!(
                    "no point-in-time replay arrived within {}h",
                    timeout.as_secs() / 3600
                )),
            },
        };
        // Removing the hold and leaving `awaiting_replay` happen under the
        // same lock a continue takes, so a continue racing the timeout gets
        // NotAwaiting instead of a lost delivery.
        {
            let mut holds = self.holds.lock().await;
            holds.remove(id);
            self.update(id, |s| s.status = RestoreStatus::Running).await;
        }
        outcome
    }

    /// Hands a replay outcome to the restore waiting for it.
    pub async fn continue_replay(&self, id: &str, outcome: ReplayOutcome) -> ContinueResult {
        if !self.contains(id).await {
            return ContinueResult::Unknown;
        }
        let mut holds = self.holds.lock().await;
        match holds.remove(id) {
            Some(tx) => match tx.send(outcome) {
                Ok(()) => ContinueResult::Delivered,
                Err(_) => ContinueResult::NotAwaiting,
            },
            None => ContinueResult::NotAwaiting,
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
            .find(|s| matches!(s.status, RestoreStatus::Running | RestoreStatus::AwaitingReplay))
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
            &registry,
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
    registry: &RestoreRegistry,
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
    let dropped = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
        let input = File::open(&src).context("Failed to open downloaded dump")?;
        let decoder = BufReader::new(GzDecoder::new(BufReader::new(input)));
        let mut output =
            BufWriter::new(File::create(&dst).context("Failed to create dump tempfile")?);
        let dropped =
            copy_sanitized(decoder, &mut output).context("Failed to decompress S3 artifact")?;
        output.flush().context("Failed to write dump tempfile")?;
        Ok(dropped)
    })
    .await
    .context("Gunzip task panicked")??;
    for name in &dropped {
        info!(restore_id = %job.restore_id, access = %name, "DEFINE ACCESS carried a redacted key and was left out of the restore; define it again afterwards");
    }
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

    if !job.hold_for_replay {
        // 6. Host resynchronizes its own state from the restored database.
        let outcome = host
            .post_restore(&dump_path)
            .await
            .context("Host post-restore step failed")?;
        progress.host_state_restored = outcome.host_state_restored;
        return Ok(outcome);
    }

    // 6. A held restore waits, gate closed, while Sp00ky Cloud replays the
    //    change feed onto the main database up to the chosen moment.
    drop(remote);
    info!(restore_id = %job.restore_id, "Holding for the point-in-time replay");
    let replay = registry
        .await_replay(&job.restore_id, REPLAY_HOLD_TIMEOUT)
        .await;
    if replay.ok {
        info!(restore_id = %job.restore_id, "Point-in-time replay applied; rebuilding host state from the main database");
    } else {
        warn!(
            restore_id = %job.restore_id,
            error = ?replay.error,
            "Point-in-time replay failed; rebuilding host state from the main database as it is"
        );
    }

    // 7. The host's state must match the main database, replayed changes
    //    included, so it is rebuilt from a fresh export rather than the dump.
    //    Reconnect: the hold can outlast the session the import signed in with.
    let remote = connect_http(db_config)
        .await
        .context("Failed to connect to main SurrealDB after the replay")?;
    let export = tempfile::NamedTempFile::new().context("Failed to create tempfile for export")?;
    remote
        .export(export.path())
        .await
        .context("Failed to export main SurrealDB after the replay")?;
    drop(remote);
    // The export prints access keys as '[REDACTED]' again; the host must not
    // import that any more than the main database did.
    let sanitized = tempfile::NamedTempFile::new().context("Failed to create tempfile for the sanitized export")?;
    let (src, dst) = (export.path().to_path_buf(), sanitized.path().to_path_buf());
    let dropped = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
        let input = BufReader::new(File::open(&src).context("Failed to open the export")?);
        let mut output =
            BufWriter::new(File::create(&dst).context("Failed to create the sanitized export")?);
        let dropped = copy_sanitized(input, &mut output).context("Failed to sanitize the export")?;
        output.flush().context("Failed to write the sanitized export")?;
        Ok(dropped)
    })
    .await
    .context("Sanitize task panicked")??;
    drop(export);
    for name in &dropped {
        info!(restore_id = %job.restore_id, access = %name, "DEFINE ACCESS carried a redacted key and was left out of the host's copy");
    }

    let outcome = host
        .post_restore(sanitized.path())
        .await
        .context("Host post-restore step failed")?;
    progress.host_state_restored = outcome.host_state_restored;

    if !replay.ok {
        anyhow::bail!(
            "point-in-time replay failed: {}; the database holds the backup plus whatever was replayed before the failure, and the replica matches it",
            replay.error.as_deref().unwrap_or("no reason given")
        );
    }
    Ok(outcome)
}

/// SurrealDB's export prints the signing keys of `DEFINE ACCESS` as
/// `'[REDACTED]'`, and importing that sets the key to the literal string
/// "[REDACTED]": anyone could then sign a token for any user (reproduced on
/// 3.1.5). A restore drops the redacted key clauses of record accesses, so
/// SurrealDB generates fresh keys and signed-in users sign in again. An
/// access whose key cannot be dropped that way (a JWT access verifying an
/// outside issuer's tokens) is left out, to be defined again by hand. The
/// export writes one statement per line.
const REDACTED_KEY: &str = "'[REDACTED]'";

fn sanitize_access(line: &str) -> Option<String> {
    if !line.starts_with("DEFINE ACCESS ") || !line.contains(REDACTED_KEY) {
        return Some(line.to_string());
    }
    let mut out = line.to_string();
    if out.contains(" TYPE RECORD ") {
        out = drop_clause(&out, " WITH JWT ALGORITHM ", true);
        out = drop_clause(&out, " WITH ISSUER ", false);
    }
    (!out.contains(REDACTED_KEY)).then_some(out)
}

/// Removes `<prefix>[ALGORITHM X ]KEY '[REDACTED]'` (with `algorithm_first`,
/// `<prefix>X KEY '[REDACTED]'`) wherever it appears.
fn drop_clause(line: &str, prefix: &str, algorithm_first: bool) -> String {
    let mut out = line.to_string();
    let mut from = 0;
    while let Some(rel) = out[from..].find(prefix) {
        let start = from + rel;
        let mut rest = &out[start + prefix.len()..];
        if algorithm_first {
            match rest.split_once(' ') {
                Some((_, after)) => rest = after,
                None => break,
            }
        } else if let Some(after) = rest.strip_prefix("ALGORITHM ") {
            match after.split_once(' ') {
                Some((_, after)) => rest = after,
                None => break,
            }
        }
        let key = format!("KEY {REDACTED_KEY}");
        if let Some(after) = rest.strip_prefix(key.as_str()) {
            let end = out.len() - after.len();
            out.replace_range(start..end, "");
            from = start;
        } else {
            from = start + prefix.len();
        }
    }
    out
}

/// Copies a dump line by line through [`sanitize_access`]; returns the names
/// of the accesses left out.
fn copy_sanitized(
    mut input: impl BufRead,
    output: &mut impl Write,
) -> std::io::Result<Vec<String>> {
    let mut dropped = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        if input.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if !line.starts_with(b"DEFINE ACCESS ") {
            output.write_all(&line)?;
            continue;
        }
        let text = String::from_utf8_lossy(&line);
        match sanitize_access(&text) {
            Some(clean) => output.write_all(clean.as_bytes())?,
            None => dropped.push(
                text.trim_start_matches("DEFINE ACCESS ")
                    .trim_start_matches("OVERWRITE ")
                    .trim_start_matches("IF NOT EXISTS ")
                    .split_whitespace()
                    .next()
                    .unwrap_or("?")
                    .to_string(),
            ),
        }
    }
    Ok(dropped)
}

#[cfg(test)]
mod hold_tests {
    use super::*;

    async fn registry_with(id: &str) -> Arc<RestoreRegistry> {
        let reg = Arc::new(RestoreRegistry::new());
        reg.enqueue(id.into(), "b1".into(), "proj".into(), "proj/b1.surql.gz".into())
            .await;
        reg.mark_running(id).await;
        reg
    }

    async fn status(reg: &RestoreRegistry, id: &str) -> String {
        serde_json::to_value(&reg.get(id).await.unwrap().status)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn a_continue_reaches_the_waiting_restore() {
        let reg = registry_with("r1").await;
        let waiting = tokio::spawn({
            let reg = reg.clone();
            async move { reg.await_replay("r1", Duration::from_secs(30)).await }
        });
        // Wait until the hold is in place.
        for _ in 0..100 {
            if status(&reg, "r1").await == "awaiting_replay" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(status(&reg, "r1").await, "awaiting_replay");
        assert!(reg.current_running().await.is_some(), "a held restore still counts as running");

        let sent = ReplayOutcome { ok: true, error: None };
        assert_eq!(reg.continue_replay("r1", sent.clone()).await, ContinueResult::Delivered);
        assert_eq!(waiting.await.unwrap(), sent);
        assert_eq!(status(&reg, "r1").await, "running");

        // The hold is gone: a second continue has nobody to reach.
        assert_eq!(
            reg.continue_replay("r1", ReplayOutcome::default()).await,
            ContinueResult::NotAwaiting
        );
    }

    #[tokio::test]
    async fn a_continue_needs_a_held_restore() {
        let reg = registry_with("r1").await;
        assert_eq!(
            reg.continue_replay("r1", ReplayOutcome { ok: true, error: None }).await,
            ContinueResult::NotAwaiting
        );
        assert_eq!(
            reg.continue_replay("nope", ReplayOutcome { ok: true, error: None }).await,
            ContinueResult::Unknown
        );
    }

    #[tokio::test]
    async fn a_hold_that_times_out_reads_as_a_failed_replay() {
        let reg = registry_with("r1").await;
        let outcome = reg.await_replay("r1", Duration::from_millis(20)).await;
        assert!(!outcome.ok);
        assert!(outcome.error.unwrap().contains("no point-in-time replay arrived"));
        assert_eq!(status(&reg, "r1").await, "running");
        assert_eq!(
            reg.continue_replay("r1", ReplayOutcome { ok: true, error: None }).await,
            ContinueResult::NotAwaiting
        );
    }
}

#[cfg(test)]
mod sanitize_tests {
    use super::*;

    #[test]
    fn restore_drops_redacted_access_keys() {
        let dump = concat!(
            "OPTION IMPORT;\n",
            "DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNUP (CREATE user SET name = $name) SIGNIN (SELECT * FROM user WHERE name = $name) WITH JWT ALGORITHM HS512 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION 1d;\n",
            "DEFINE ACCESS partner ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' DURATION FOR SESSION 1h;\n",
            "DEFINE ACCESS jwks ON DATABASE TYPE JWT URL 'https://example.com/jwks.json';\n",
            "INSERT [ { id: note:1, text: 'KEY \\'[REDACTED]\\'' } ];\n",
        );
        let mut out = Vec::new();
        let dropped = copy_sanitized(dump.as_bytes(), &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("SIGNIN (SELECT * FROM user WHERE name = $name) DURATION FOR TOKEN 1h, FOR SESSION 1d;\n"), "{out}");
        assert!(!out.contains("ACCESS partner"), "{out}");
        assert_eq!(dropped, vec!["partner".to_string()]);
        for keep in ["OPTION IMPORT;", "ACCESS jwks", "INSERT [ { id: note:1"] {
            assert!(out.contains(keep), "lost {keep}: {out}");
        }
    }
}
