//! Warm restart for a cluster SSP.
//!
//! A cluster SSP used to rebuild from nothing on every restart: it paged every
//! synced table through the scheduler proxy while the scheduler held the
//! tenant's sync frozen, four minutes on whitepawn and growing with the data.
//! It keeps its rows instead:
//!
//! - **Checkpoint.** Each table's rows go to
//!   `$SPKY_SSP_SNAPSHOT_DIR/rows/<table>.rows` in the binary format of
//!   [`ssp::circuit::checkpoint`]: on SIGTERM, after every bootstrap, and on a
//!   slow timer. Only tables whose content hash moved since they were last
//!   written are written again.
//! - **Load.** At boot the files go back into the circuit before the SSP
//!   registers. A file that fails its checks is deleted and its table paged.
//! - **Verify, then repair.** Registration hands back the scheduler's hash of
//!   every table at the cut it freezes. The bootstrap keeps a table whose rows
//!   hash the same, repairs one that differs from an `(id, _00_rv)` listing of
//!   the replica ([`repair_table`]), and pages the rest in full.
//!
//! The same bootstrap serves an in-process re-bootstrap (the scheduler forgot
//! this SSP, or asked for a resync): the rows are already in memory and only
//! the views are rebuilt. Nothing is trusted beyond the scheduler's hash, so a
//! stale, partial or corrupt checkpoint costs time, never correctness.

use crate::BootstrapSource;
use serde_json::Value;
use ssp::circuit::checkpoint::{read_collection, write_collection};
use ssp::circuit::store::Collection;
use ssp::circuit::{Circuit, Operation, Record};
use ssp::types::Sp00kyValue;
use ssp_node::SspStatus;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Rows per `(id, _00_rv)` listing page. Two small fields per row, so a page
/// can be much larger than a bootstrap page of whole bodies.
const LIST_PAGE: usize = 10_000;

/// Rows per fetch of changed bodies.
const FETCH_BATCH: usize = 500;

/// Default for `SPKY_SSP_ROW_CHECKPOINT_SECS`.
const DEFAULT_INTERVAL_SECS: u64 = 1800;

/// How long after a bootstrap the first checkpoint waits, so its table locks
/// stay out of the scheduler's replay and catch-up verification.
pub const POST_BOOTSTRAP_WRITE_DELAY: Duration = Duration::from_secs(120);

/// The row checkpoint directory and what is in it.
pub struct RowCheckpoints {
    dir: PathBuf,
    /// Each table's catch-up hash as it is on disk, so a write skips the
    /// tables that have not changed since.
    on_disk: std::sync::Mutex<HashMap<String, [u8; 32]>>,
    /// One writer at a time: the timer, the post-bootstrap write and the
    /// shutdown write can otherwise overlap.
    writing: tokio::sync::Mutex<()>,
}

impl RowCheckpoints {
    /// `$SPKY_SSP_SNAPSHOT_DIR/rows`, when that is set and writable. Without
    /// it every restart is a cold bootstrap, as before.
    pub fn from_env() -> Option<Arc<Self>> {
        let base = std::env::var_os("SPKY_SSP_SNAPSHOT_DIR")?;
        let dir = PathBuf::from(base).join("rows");
        let probe = dir.join(".probe");
        let writable = std::fs::create_dir_all(&dir).is_ok() && std::fs::write(&probe, b"").is_ok();
        let _ = std::fs::remove_file(&probe);
        if !writable {
            warn!(dir = %dir.display(), "Row checkpoint dir is not writable; every restart will bootstrap cold");
            return None;
        }
        info!(dir = %dir.display(), "Row checkpoints enabled");
        Some(Arc::new(Self {
            dir,
            on_disk: Default::default(),
            writing: Default::default(),
        }))
    }

    /// `SPKY_SSP_ROW_CHECKPOINT_SECS`: how often the timer writes changed
    /// tables. `0` turns the timer off; shutdown and post-bootstrap writes
    /// still happen.
    pub fn interval_from_env() -> Option<Duration> {
        let secs = std::env::var("SPKY_SSP_ROW_CHECKPOINT_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_INTERVAL_SECS);
        (secs > 0).then(|| Duration::from_secs(secs))
    }

    fn path_for(&self, table: &str) -> PathBuf {
        self.dir.join(format!("{}.rows", file_stem(table)))
    }

    /// Read every checkpointed table into the circuit, replacing whatever it
    /// held for those tables. Returns the number of tables loaded.
    pub async fn load_into(&self, processor: &Arc<RwLock<Circuit>>) -> usize {
        let started = Instant::now();
        let dir = self.dir.clone();
        let loaded = tokio::task::spawn_blocking(move || read_dir_tables(&dir))
            .await
            .unwrap_or_default();
        let tables = loaded.len();
        let rows: usize = loaded.iter().map(|c| c.rows.len()).sum();
        {
            let mut circuit = processor.write().await;
            let mut on_disk = self.on_disk.lock().unwrap();
            for coll in loaded {
                on_disk.insert(coll.name.clone(), coll.catchup_xor);
                circuit.store.collections.insert(coll.name.clone(), coll);
            }
        }
        if tables > 0 {
            info!(tables, rows, ms = started.elapsed().as_millis() as u64, "Loaded row checkpoint");
        }
        tables
    }

    /// Write every table whose content changed since it was last written, and
    /// drop the files of tables the circuit no longer holds.
    ///
    /// Each table is written under its own short read lock, so ingest waits
    /// for one table at a time rather than for the whole store. Tables can
    /// therefore come from slightly different moments; that is fine, because
    /// the next boot verifies every table on its own.
    pub async fn write(&self, processor: &Arc<RwLock<Circuit>>, reason: &'static str) {
        let _one_writer = self.writing.lock().await;
        let started = Instant::now();
        let held: Vec<(String, [u8; 32])> = {
            let circuit = processor.read().await;
            circuit
                .store
                .collections
                .iter()
                // Runtime-internal tables (`_00_heartbeat`) are not synced and a
                // bootstrap drops them anyway; writing them is churn.
                .filter(|(name, _)| !ssp_protocol::table_excluded_from_sync(name))
                .map(|(name, coll)| (name.clone(), coll.catchup_xor))
                .collect()
        };
        let changed: Vec<String> = {
            let on_disk = self.on_disk.lock().unwrap();
            held.iter()
                .filter(|(name, hash)| on_disk.get(name) != Some(hash))
                .map(|(name, _)| name.clone())
                .collect()
        };

        let (mut written, mut rows, mut failed) = (0usize, 0u64, 0usize);
        for table in changed {
            let guard = Arc::clone(processor).read_owned().await;
            let path = self.path_for(&table);
            let name = table.clone();
            let result = tokio::task::spawn_blocking(move || {
                let Some(coll) = guard.store.collections.get(&name) else {
                    return Ok(None);
                };
                let hash = coll.catchup_xor;
                let tmp = path.with_extension("rows.tmp");
                let file = std::fs::File::create(&tmp)?;
                let mut out = std::io::BufWriter::with_capacity(1 << 20, file);
                let n = write_collection(coll, &mut out)?;
                // Release the circuit before the fsync: a flush to disk can
                // take seconds and ingest has no reason to wait for it.
                drop(guard);
                let file = out.into_inner().map_err(|e| e.into_error())?;
                file.sync_all()?;
                std::fs::rename(&tmp, &path)?;
                Ok::<_, std::io::Error>(Some((hash, n)))
            })
            .await;
            match result {
                Ok(Ok(Some((hash, n)))) => {
                    self.on_disk.lock().unwrap().insert(table, hash);
                    written += 1;
                    rows += n;
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => {
                    failed += 1;
                    warn!(table = %table, error = %e, "Could not write row checkpoint");
                }
                Err(e) => {
                    failed += 1;
                    warn!(table = %table, error = %e, "Row checkpoint task failed");
                }
            }
        }

        let live: HashSet<&str> = held.iter().map(|(name, _)| name.as_str()).collect();
        let gone: Vec<String> = {
            let mut on_disk = self.on_disk.lock().unwrap();
            let gone: Vec<String> = on_disk.keys().filter(|t| !live.contains(t.as_str())).cloned().collect();
            for t in &gone {
                on_disk.remove(t);
            }
            gone
        };
        for table in &gone {
            let _ = std::fs::remove_file(self.path_for(table));
        }

        if written > 0 || failed > 0 || !gone.is_empty() {
            info!(
                reason,
                written,
                unchanged = held.len() - written - failed,
                removed = gone.len(),
                failed,
                rows,
                ms = started.elapsed().as_millis() as u64,
                "Row checkpoint written"
            );
        }
    }

    /// Delete every checkpoint, for a clean restart.
    pub fn clear(&self) {
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        self.on_disk.lock().unwrap().clear();
        info!(dir = %self.dir.display(), "Row checkpoints cleared for clean restart");
    }

    /// Write changed tables every `every` while the SSP is Ready.
    pub fn spawn_timer(
        self: &Arc<Self>,
        processor: Arc<RwLock<Circuit>>,
        status: Arc<RwLock<SspStatus>>,
        every: Duration,
    ) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if *status.read().await == SspStatus::Ready {
                    this.write(&processor, "timer").await;
                }
            }
        });
        info!(interval_secs = every.as_secs(), "Row checkpoint timer armed");
    }
}

/// A file name for a table: identifier characters kept, anything else hex
/// escaped. The table name inside the file is what counts on load; this only
/// has to be unique and safe.
fn file_stem(table: &str) -> String {
    let mut out = String::with_capacity(table.len());
    for b in table.bytes() {
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02x}"));
        }
    }
    out
}

/// Read every `*.rows` file. A file that fails to read is deleted: its table
/// is paged instead, and the next write replaces it.
fn read_dir_tables(dir: &Path) -> Vec<Collection> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("rows") {
            // A `.rows.tmp` left by a write that never finished.
            if path.to_string_lossy().ends_with(".tmp") {
                let _ = std::fs::remove_file(&path);
            }
            continue;
        }
        let read = std::fs::File::open(&path)
            .map_err(ssp::circuit::checkpoint::CheckpointError::from)
            .and_then(|f| read_collection(std::io::BufReader::with_capacity(1 << 20, f)));
        match read {
            Ok(coll) => out.push(coll),
            Err(e) => {
                warn!(file = %path.display(), error = %e, "Discarding row checkpoint");
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    out
}

/// What [`repair_table`] did.
pub struct Repair {
    pub listed: usize,
    pub fetched: usize,
    pub deleted: usize,
    /// The table now hashes as the scheduler expects.
    pub matched: bool,
}

/// Bring one table's rows in line with the scheduler's replica, fetching only
/// the rows that differ.
///
/// Lists `(id, _00_rv)` for the whole table (two small fields per row, keyset
/// paged), deletes what the replica no longer has, fetches what it holds at
/// another version or that is missing here, then compares the table hash.
/// `matched: false` tells the caller to page the table in full, which is also
/// the answer when most of the table differs anyway.
///
/// Runs inside the registration window, where the scheduler holds its replica
/// frozen at the cut it handed out, so the listing and the fetches see one
/// consistent table.
pub async fn repair_table(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    expected: &str,
) -> anyhow::Result<Repair> {
    let mut listing: Vec<(String, Option<i64>)> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let rows = match source.query(&listing_query(table, after.as_deref())).await? {
            Value::Array(rows) => rows,
            _ => Vec::new(),
        };
        let n = rows.len();
        for row in rows {
            if let Some(id) = row.get("id").and_then(Value::as_str) {
                listing.push((id.to_string(), row.get("_00_rv").and_then(Value::as_i64)));
            }
        }
        if n < LIST_PAGE {
            break;
        }
        match listing.last() {
            Some((id, _)) => after = Some(id.clone()),
            None => break,
        }
    }

    let (fetch, stale) = {
        let circuit = processor.read().await;
        match circuit.store.get_collection(table) {
            Some(coll) => coll.version_diff(&listing),
            None => return Ok(Repair { listed: listing.len(), fetched: 0, deleted: 0, matched: false }),
        }
    };
    if fetch.len() * 2 > listing.len().max(1) {
        return Ok(Repair { listed: listing.len(), fetched: 0, deleted: 0, matched: false });
    }

    {
        let mut circuit = processor.write().await;
        let coll = circuit.store.ensure_collection(table);
        for id in &stale {
            coll.apply(Operation::Delete, id, Sp00kyValue::Null);
        }
    }

    let mut fetched = 0;
    for batch in fetch.chunks(FETCH_BATCH) {
        let rows = match source.query(&fetch_query(table, batch, omit)).await? {
            Value::Array(rows) => rows,
            _ => Vec::new(),
        };
        let records: Vec<Record> = rows
            .into_iter()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_string();
                Some(Record::new(table, &id, row))
            })
            .collect();
        fetched += records.len();
        let mut circuit = processor.write().await;
        let coll = circuit.store.ensure_collection(table);
        for record in records {
            coll.apply(Operation::Update, &record.id, record.data);
        }
    }

    let matched = {
        let circuit = processor.read().await;
        circuit
            .store
            .get_collection(table)
            .map(|coll| ssp_protocol::snapshot_hash::xor_acc_to_hex(&coll.catchup_xor) == expected)
            .unwrap_or(false)
    };
    Ok(Repair { listed: listing.len(), fetched, deleted: stale.len(), matched })
}

/// A SurrealQL string literal body: backslashes and single quotes escaped.
fn quote(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\'', "\\'")
}

fn record_expr(table: &str, id: &str) -> String {
    let raw = id.strip_prefix(&format!("{table}:")).unwrap_or(id);
    format!("type::record('{table}', '{}')", quote(raw))
}

/// One keyset page of `(id, _00_rv)`, in id order like the bootstrap pager.
fn listing_query(table: &str, after: Option<&str>) -> String {
    match after {
        None => format!("SELECT id, _00_rv FROM {table} ORDER BY id LIMIT {LIST_PAGE}"),
        Some(id) => format!(
            "SELECT id, _00_rv FROM {table} WHERE id > {} ORDER BY id LIMIT {LIST_PAGE}",
            record_expr(table, id)
        ),
    }
}

/// Whole bodies of the listed ids, by record id rather than by scan.
fn fetch_query(table: &str, ids: &[String], omit: &BTreeSet<String>) -> String {
    let omit = ssp_protocol::omit_clause(omit);
    let ids: Vec<String> = ids.iter().map(|id| record_expr(table, id)).collect();
    format!("SELECT *{omit} FROM [{}]", ids.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_pages_by_id() {
        assert_eq!(
            listing_query("game", None),
            "SELECT id, _00_rv FROM game ORDER BY id LIMIT 10000"
        );
        assert_eq!(
            listing_query("game", Some("game:abc")),
            "SELECT id, _00_rv FROM game WHERE id > type::record('game', 'abc') ORDER BY id LIMIT 10000"
        );
    }

    #[test]
    fn fetch_selects_by_record_id_and_omits_opaque_fields() {
        let omit: BTreeSet<String> = ["blob".to_string()].into();
        assert_eq!(
            fetch_query("game", &["a".to_string(), "game:b".to_string()], &omit),
            "SELECT * OMIT blob FROM [type::record('game', 'a'), type::record('game', 'b')]"
        );
    }

    #[test]
    fn ids_are_quoted() {
        assert_eq!(record_expr("t", "it's\\x"), "type::record('t', 'it\\'s\\\\x')");
    }

    #[test]
    fn file_stems_are_unique_and_safe() {
        assert_eq!(file_stem("game_insight"), "game_insight");
        assert_eq!(file_stem("a/b"), "a%2fb");
        assert_ne!(file_stem("a.b"), file_stem("a_b"));
    }

    #[tokio::test]
    async fn write_then_load_restores_rows_and_skips_unchanged_tables() {
        let dir = std::env::temp_dir().join(format!("ssp-rows-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let checkpoints = RowCheckpoints {
            dir: dir.clone(),
            on_disk: Default::default(),
            writing: Default::default(),
        };

        let processor = Arc::new(RwLock::new(Circuit::new()));
        {
            let mut c = processor.write().await;
            for (table, id) in [("game", "a"), ("game", "b"), ("user", "u")] {
                c.store.ensure_collection(table).apply(
                    Operation::Create,
                    id,
                    Sp00kyValue::from(serde_json::json!({ "id": format!("{table}:{id}"), "n": 1 })),
                );
            }
        }
        checkpoints.write(&processor, "test").await;
        let game_file = checkpoints.path_for("game");
        let first_write = std::fs::metadata(&game_file).unwrap().modified().unwrap();

        // Unchanged: nothing is rewritten.
        checkpoints.write(&processor, "test").await;
        assert_eq!(std::fs::metadata(&game_file).unwrap().modified().unwrap(), first_write);

        // A table that disappears loses its file.
        processor.write().await.store.collections.remove("user");
        checkpoints.write(&processor, "test").await;
        assert!(!checkpoints.path_for("user").exists());

        let fresh = Arc::new(RwLock::new(Circuit::new()));
        let reader = RowCheckpoints { dir: dir.clone(), on_disk: Default::default(), writing: Default::default() };
        assert_eq!(reader.load_into(&fresh).await, 1);
        let c = fresh.read().await;
        let game = c.store.get_collection("game").unwrap();
        assert_eq!(game.rows.len(), 2);
        assert_eq!(game.catchup_xor, processor.read().await.store.get_collection("game").unwrap().catchup_xor);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
