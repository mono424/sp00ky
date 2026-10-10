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
//!   hash the same, repairs one that differs ([`repair_table`]), and pages the
//!   rest in full. A repair compares the scheduler's id-range hashes with its
//!   own and lists only the ranges that differ; without ranges it lists the
//!   whole table's `(id, _00_rv)`.
//!
//! The same bootstrap serves an in-process re-bootstrap (the scheduler forgot
//! this SSP, or asked for a resync): the rows are already in memory and only
//! the views are rebuilt. Nothing is trusted beyond the scheduler's hash, so a
//! stale, partial or corrupt checkpoint costs time, never correctness.
//!
//! **Shared directory (blue/green).** An SSP and the standby replacing it
//! share one volume and one `SPKY_SSP_SNAPSHOT_DIR`: the standby loads the
//! files while its predecessor may still write them. Only one of them writes
//! at a time, by [`RowCheckpoints::set_gate`]: a standby writes nothing until
//! promoted, and a retired SSP nothing after its retire, its shutdown write
//! included. Beyond that:
//!
//! - every write goes to a temp file unique to its process (`<table>.rows.tmp.
//!   <pid>.<random>`; pids repeat across containers) and is renamed into
//!   place, so a reader sees an old file or a new one, never a torn one, and
//!   the loader only removes temp files old enough to be abandoned;
//! - a write removes only the files of tables this process itself loaded or
//!   wrote and no longer holds, and only while the gate lets it write;
//! - a file the loader cannot read is deleted, which the other instance may
//!   have written in a format this build does not read. That table is paged
//!   instead and the owner writes it again; time, not correctness;
//! - [`RowCheckpoints::clear`] (a clean restart directive) empties the
//!   directory for both; the VM shell never calls it from a standby.

use crate::BootstrapSource;
use serde_json::Value;
use ssp::circuit::checkpoint::{load_file, write_collection, LoadStats};
use ssp::circuit::store::Collection;
use ssp::circuit::{Circuit, Operation, Record};
use ssp::types::Sp00kyValue;
use ssp_node::SspStatus;
use ssp_protocol::range_hash::RangeHashes;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

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

/// A temp file older than this is a write that will never finish (its process
/// died); younger ones may belong to the other instance sharing the directory.
const STALE_TMP_AGE: Duration = Duration::from_secs(600);

/// Whether this process may write checkpoints right now. See
/// [`RowCheckpoints::set_gate`].
pub type WriteGate = Box<dyn Fn() -> bool + Send + Sync>;

/// The row checkpoint directory and what is in it.
pub struct RowCheckpoints {
    dir: PathBuf,
    /// Each table's catch-up hash as it is on disk, so a write skips the
    /// tables that have not changed since.
    on_disk: std::sync::Mutex<HashMap<String, [u8; 32]>>,
    /// One writer at a time: the timer, the post-bootstrap write and the
    /// shutdown write can otherwise overlap.
    writing: tokio::sync::Mutex<()>,
    /// Consulted before a write and before each table of it. Unset: always.
    gate: std::sync::OnceLock<WriteGate>,
}

impl RowCheckpoints {
    /// `$SPKY_SSP_SNAPSHOT_DIR/rows`, when that is set and writable. Without
    /// it every restart is a cold bootstrap, as before.
    pub fn from_env() -> Option<Arc<Self>> {
        let base = std::env::var_os("SPKY_SSP_SNAPSHOT_DIR")?;
        let dir = PathBuf::from(base).join("rows");
        // Unique: another instance may share the directory (blue/green).
        let probe = dir.join(format!(".probe.{}", unique_suffix()));
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
            gate: Default::default(),
        }))
    }

    /// Install the write gate (once). The VM shell lets a process write only
    /// while it owns the directory: never as a blue/green standby, never once
    /// retired. Checked before every table, so a retire mid-write stops it.
    pub fn set_gate(&self, gate: WriteGate) {
        let _ = self.gate.set(gate);
    }

    fn may_write(&self) -> bool {
        self.gate.get().is_none_or(|gate| gate())
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
    ///
    /// The files are mapped (or adopted) rather than copied, so this costs
    /// one verification pass and one index walk per table; the log line
    /// carries both so the split stays visible.
    pub async fn load_into(&self, processor: &Arc<RwLock<Circuit>>) -> usize {
        let started = Instant::now();
        let dir = self.dir.clone();
        let loaded = tokio::task::spawn_blocking(move || read_dir_tables(&dir))
            .await
            .unwrap_or_default();
        let tables = loaded.tables.len();
        let rows: usize = loaded.tables.iter().map(|(c, _)| c.rows.len()).sum();
        let bytes: u64 = loaded.tables.iter().map(|(_, s)| s.bytes).sum();
        let mapped = loaded.tables.iter().filter(|(_, s)| s.mapped).count();
        let heads_ms: u64 = loaded.tables.iter().map(|(_, s)| s.heads_ms).sum();
        let verify_ms: u64 = loaded.tables.iter().map(|(_, s)| s.verify_ms).sum();
        let install_started = Instant::now();
        {
            let mut circuit = processor.write().await;
            let mut on_disk = self.on_disk.lock().unwrap();
            for (coll, _) in loaded.tables {
                on_disk.insert(coll.name.clone(), coll.catchup_xor);
                circuit.store.collections.insert(coll.name.clone(), coll);
            }
        }
        let install_ms = install_started.elapsed().as_millis() as u64;
        if tables > 0 || loaded.discarded > 0 {
            info!(
                tables,
                rows,
                bytes,
                mapped,
                copied = tables - mapped,
                discarded = loaded.discarded,
                heads_ms,
                verify_ms,
                install_ms,
                ms = started.elapsed().as_millis() as u64,
                "Loaded row checkpoint"
            );
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
        if !self.may_write() {
            info!(reason, "Skipping row checkpoint: this process does not own the checkpoint dir");
            return;
        }
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

        let (mut written, mut rows, mut bytes, mut failed) = (0usize, 0u64, 0u64, 0usize);
        let (mut encode_ms, mut fsync_ms) = (0u64, 0u64);
        for table in changed {
            if !self.may_write() {
                info!(reason, written, "Row checkpoint stopped: this process no longer owns the checkpoint dir");
                return;
            }
            let guard = Arc::clone(processor).read_owned().await;
            let path = self.path_for(&table);
            let name = table.clone();
            let result = tokio::task::spawn_blocking(move || {
                let Some(coll) = guard.store.collections.get(&name) else {
                    return Ok(None);
                };
                let hash = coll.catchup_xor;
                let tmp = tmp_path_for(&path);
                let encode_started = Instant::now();
                let file = std::fs::File::create(&tmp)?;
                let mut out = std::io::BufWriter::with_capacity(1 << 20, file);
                let w = write_collection(coll, ssp::circuit::checkpoint::fresh_image_id(), &mut out)?;
                // Release the circuit before the fsync: a flush to disk can
                // take seconds and ingest has no reason to wait for it.
                drop(guard);
                let encode_ms = encode_started.elapsed().as_millis() as u64;
                let sync_started = Instant::now();
                let file = out.into_inner().map_err(|e| e.into_error())?;
                file.sync_all()?;
                std::fs::rename(&tmp, &path)?;
                let fsync_ms = sync_started.elapsed().as_millis() as u64;
                Ok::<_, std::io::Error>(Some((hash, w, encode_ms, fsync_ms)))
            })
            .await;
            match result {
                Ok(Ok(Some((hash, w, table_encode_ms, table_fsync_ms)))) => {
                    debug!(
                        table = %table,
                        rows = w.rows,
                        bytes = w.bytes,
                        encode_ms = table_encode_ms,
                        fsync_ms = table_fsync_ms,
                        "Row checkpoint table written"
                    );
                    self.on_disk.lock().unwrap().insert(table, hash);
                    written += 1;
                    rows += w.rows;
                    bytes += w.bytes;
                    encode_ms += table_encode_ms;
                    fsync_ms += table_fsync_ms;
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
        if !gone.is_empty() && !self.may_write() {
            return;
        }
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
                bytes,
                encode_ms,
                fsync_ms,
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

    /// Persist what a bootstrap (or a blue/green promotion) just verified,
    /// so a restart from here on is warm. Not right away: the scheduler
    /// replays the events it buffered and verifies the catch-up as soon as
    /// the SSP reports ready, and every one of those ingests waits for a table
    /// the write is holding. On whitepawn a 16 s write held 3 replayed events
    /// for 13 s, and the SSP out of rotation with them. A restart inside the
    /// delay still loads the previous checkpoint and repairs the difference.
    pub fn spawn_post_bootstrap_write(
        self: &Arc<Self>,
        processor: Arc<RwLock<Circuit>>,
        status: Arc<RwLock<SspStatus>>,
        reason: &'static str,
    ) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(POST_BOOTSTRAP_WRITE_DELAY).await;
            if *status.read().await == SspStatus::Ready {
                this.write(&processor, reason).await;
            }
        });
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

/// `<pid>.<random>`: pids alone repeat across containers (often pid 1).
fn unique_suffix() -> String {
    format!("{}.{}", std::process::id(), uuid::Uuid::new_v4().simple())
}

/// `<table>.rows.tmp.<pid>.<random>` next to `<table>.rows`.
fn tmp_path_for(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(format!(".tmp.{}", unique_suffix()));
    path.with_file_name(name)
}

/// A temp file of an abandoned write: `.rows.tmp` in its name (this format or
/// the older bare one) and untouched for [`STALE_TMP_AGE`].
fn is_stale_tmp(path: &Path) -> bool {
    let named = path.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.contains(".rows.tmp"));
    named
        && std::fs::metadata(path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age >= STALE_TMP_AGE)
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

/// What [`read_dir_tables`] found.
#[derive(Default)]
struct Loaded {
    tables: Vec<(Collection, LoadStats)>,
    /// Files that failed their checks and were deleted.
    discarded: usize,
}

/// Read every `*.rows` file. A file that fails to read is deleted: its table
/// is paged instead, and the next write replaces it.
fn read_dir_tables(dir: &Path) -> Loaded {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Loaded::default();
    };
    let mut out = Loaded::default();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("rows") {
            // A temp file left by a write that never finished. Only an old
            // one: a fresh one may be another instance's write in progress.
            if is_stale_tmp(&path) {
                let _ = std::fs::remove_file(&path);
            }
            continue;
        }
        let started = Instant::now();
        match load_file(&path, ssp::circuit::checkpoint::BodyVerify::AtLoad) {
            Ok(load) => {
                let (coll, stats) = (load.collection, load.stats);
                debug!(
                    table = %coll.name,
                    rows = coll.rows.len(),
                    bytes = stats.bytes,
                    mapped = stats.mapped,
                    heads_ms = stats.heads_ms,
                    verify_ms = stats.verify_ms,
                    ms = started.elapsed().as_millis() as u64,
                    "Loaded row checkpoint table"
                );
                out.tables.push((coll, stats));
            }
            Err(e) => {
                warn!(file = %path.display(), error = %e, "Discarding row checkpoint");
                let _ = std::fs::remove_file(&path);
                out.discarded += 1;
            }
        }
    }
    out
}

/// What [`repair_table`] did.
#[derive(Debug, Default)]
pub struct Repair {
    pub listed: usize,
    pub fetched: usize,
    pub deleted: usize,
    /// The scheduler's id ranges for the table (0: it served none, and the
    /// whole table was listed) and how many of them differed.
    pub ranges: usize,
    pub differing: usize,
    /// The table now hashes as the scheduler expects.
    pub matched: bool,
}

/// Bring one table's rows in line with the scheduler's replica, fetching only
/// the rows that differ.
///
/// With the scheduler's id-range hashes (`/proxy/ranges`), only the ranges
/// whose rows hash differently here are listed: one key-range scan each,
/// against the whole table's listing without them (whitepawn 2026-10-09:
/// 105k `game_insight` ids in 24 s to find 3 changed rows). Either way the
/// `(id, _00_rv)` listing names what to delete and what to fetch, and a range
/// that still differs after that (content changed at an equal version) is
/// re-read whole. `matched: false` tells the caller to page the table in full,
/// which is also the answer when most of the table differs anyway.
///
/// Runs inside the registration window, where the scheduler holds its replica
/// frozen at the cut it handed out, so the ranges, the listing and the fetches
/// see one consistent table.
pub async fn repair_table(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    expected: &str,
) -> anyhow::Result<Repair> {
    match source.table_ranges(table).await {
        Some(wire) if wire.hash == expected => match RangeHashes::from_wire(&wire) {
            Some(ranges) => return repair_by_ranges(source, processor, table, omit, expected, &ranges).await,
            None => warn!(table, "Scheduler served range hashes that do not add up; listing the table in full"),
        },
        Some(wire) => debug!(table, ranges = %wire.hash, expected, "Range hashes are of another cut; listing the table in full"),
        None => {}
    }
    repair_by_listing(source, processor, table, omit, expected).await
}

/// [`repair_table`] over the scheduler's id ranges.
async fn repair_by_ranges(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    expected: &str,
    ranges: &RangeHashes,
) -> anyhow::Result<Repair> {
    let mut repair = Repair { ranges: ranges.len(), ..Repair::default() };
    let Some(differing) = differing_ranges(processor, table, ranges, None).await else {
        return Ok(repair);
    };
    repair.differing = differing.len();

    // Listing a range ships two fields a row; fetching ships bodies. Only a
    // repair that would fetch most of the table is better off paging it.
    let mut listing: Vec<(String, Option<i64>)> = Vec::new();
    for &i in &differing {
        let rows = rows_of(source.query(&range_listing_query(table, ranges, i)).await?);
        listing.extend(rows.iter().filter_map(listing_entry));
    }
    repair.listed = listing.len();
    let scope: HashSet<usize> = differing.iter().copied().collect();
    let (fetch, stale) = {
        let circuit = processor.read().await;
        let Some(coll) = circuit.store.get_collection(table) else { return Ok(repair) };
        coll.version_diff_in(&listing, |id| ranges.range_of(id).is_some_and(|i| scope.contains(&i)))
    };
    if fetch.len() as u64 * 2 > ranges.rows().max(1) {
        return Ok(repair);
    }
    repair.deleted = delete_rows(processor, table, &stale).await;
    repair.fetched = fetch_rows(source, processor, table, omit, &fetch).await?;

    // Equal versions were taken as equal content. A range that still differs
    // holds a row changed without a new version: read it whole.
    let Some(still) = differing_ranges(processor, table, ranges, Some(&differing)).await else {
        return Ok(repair);
    };
    let still_rows: u64 = still.iter().map(|&i| ranges.count(i)).sum();
    if still_rows * 2 > ranges.rows().max(1) {
        return Ok(repair);
    }
    for &i in &still {
        let (fetched, deleted) = reread_range(source, processor, table, omit, ranges, i).await?;
        repair.fetched += fetched;
        repair.deleted += deleted;
    }

    repair.matched = table_matches(processor, table, expected).await;
    Ok(repair)
}

/// The ranges (of `among`, or all) whose rows hash differently here. `None`
/// when the table is not held or a held id cannot be ranged.
async fn differing_ranges(
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    ranges: &RangeHashes,
    among: Option<&[usize]>,
) -> Option<Vec<usize>> {
    let circuit = processor.read().await;
    let accs = circuit.store.get_collection(table)?.range_accs(ranges)?;
    let differing = ranges.differing(&accs);
    Some(match among {
        Some(among) => differing.into_iter().filter(|i| among.contains(i)).collect(),
        None => differing,
    })
}

/// Replace range `i` with the replica's rows: every row read is applied (an
/// unchanged one is a no-op in the store) and every row held here that the
/// replica no longer has is deleted. Returns `(fetched, deleted)`.
async fn reread_range(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    ranges: &RangeHashes,
    i: usize,
) -> anyhow::Result<(usize, usize)> {
    let omit_sql = ssp_protocol::omit_clause(omit);
    let rows = rows_of(source.query(&format!("SELECT *{omit_sql} FROM {}", ranges.target(table, i))).await?);
    let records = records_of(table, rows);
    let read: HashSet<&str> = records.iter().map(|r| ssp::types::raw_id(&r.id)).collect();
    let gone: Vec<String> = {
        let circuit = processor.read().await;
        circuit
            .store
            .get_collection(table)
            .map(|coll| {
                coll.rows
                    .keys()
                    .filter(|id| ranges.range_of(id) == Some(i) && !read.contains(id))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };
    let deleted = delete_rows(processor, table, &gone).await;
    let fetched = records.len();
    apply_records(processor, table, records).await;
    Ok((fetched, deleted))
}

/// [`repair_table`] without ranges: list `(id, _00_rv)` for the whole table
/// (keyset paged), delete what the replica no longer has, fetch what it holds
/// at another version or that is missing here, then compare the table hash.
async fn repair_by_listing(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    expected: &str,
) -> anyhow::Result<Repair> {
    let mut listing: Vec<(String, Option<i64>)> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let rows = rows_of(source.query(&listing_query(table, after.as_deref())).await?);
        let n = rows.len();
        listing.extend(rows.iter().filter_map(listing_entry));
        if n < LIST_PAGE {
            break;
        }
        match listing.last() {
            Some((id, _)) => after = Some(id.clone()),
            None => break,
        }
    }

    let mut repair = Repair { listed: listing.len(), ..Repair::default() };
    let (fetch, stale) = {
        let circuit = processor.read().await;
        match circuit.store.get_collection(table) {
            Some(coll) => coll.version_diff(&listing),
            None => return Ok(repair),
        }
    };
    if fetch.len() * 2 > listing.len().max(1) {
        return Ok(repair);
    }
    repair.deleted = delete_rows(processor, table, &stale).await;
    repair.fetched = fetch_rows(source, processor, table, omit, &fetch).await?;
    repair.matched = table_matches(processor, table, expected).await;
    Ok(repair)
}

fn rows_of(result: Value) -> Vec<Value> {
    match result {
        Value::Array(rows) => rows,
        _ => Vec::new(),
    }
}

fn listing_entry(row: &Value) -> Option<(String, Option<i64>)> {
    let id = row.get("id")?.as_str()?;
    Some((id.to_string(), row.get("_00_rv").and_then(Value::as_i64)))
}

fn records_of(table: &str, rows: Vec<Value>) -> Vec<Record> {
    rows.into_iter()
        .filter_map(|row| {
            let id = row.get("id")?.as_str()?.to_string();
            Some(Record::new(table, &id, row))
        })
        .collect()
}

async fn delete_rows(processor: &Arc<RwLock<Circuit>>, table: &str, ids: &[String]) -> usize {
    if ids.is_empty() {
        return 0;
    }
    let mut circuit = processor.write().await;
    let coll = circuit.store.ensure_collection(table);
    for id in ids {
        coll.apply(Operation::Delete, id, Sp00kyValue::Null);
    }
    ids.len()
}

async fn apply_records(processor: &Arc<RwLock<Circuit>>, table: &str, records: Vec<Record>) {
    let mut circuit = processor.write().await;
    let coll = circuit.store.ensure_collection(table);
    for record in records {
        coll.apply(Operation::Update, &record.id, record.data);
    }
}

/// Fetch the listed ids' bodies by record id, `FETCH_BATCH` at a time, and
/// apply them. Returns how many came back.
async fn fetch_rows(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    ids: &[String],
) -> anyhow::Result<usize> {
    let mut fetched = 0;
    for batch in ids.chunks(FETCH_BATCH) {
        let records = records_of(table, rows_of(source.query(&fetch_query(table, batch, omit)).await?));
        fetched += records.len();
        apply_records(processor, table, records).await;
    }
    Ok(fetched)
}

async fn table_matches(processor: &Arc<RwLock<Circuit>>, table: &str, expected: &str) -> bool {
    let circuit = processor.read().await;
    circuit
        .store
        .get_collection(table)
        .map(|coll| ssp_protocol::snapshot_hash::xor_acc_to_hex(&coll.catchup_xor) == expected)
        .unwrap_or(false)
}

/// `FROM` targets that page `table` by its id ranges: consecutive ranges
/// grouped up to about `page_size` rows by the scheduler's counts, one range
/// at least, so every page is a single key-range scan.
pub fn page_targets(table: &str, ranges: &RangeHashes, page_size: usize) -> Vec<String> {
    let span = |first: usize, last: usize| {
        ssp_protocol::range_hash::range_target(table, ranges.bounds(first).0, ranges.bounds(last).1)
    };
    let mut targets = Vec::new();
    let (mut first, mut rows) = (0usize, 0u64);
    for i in 0..ranges.len() {
        let n = ranges.count(i);
        if i > first && rows + n > page_size as u64 {
            targets.push(span(first, i - 1));
            (first, rows) = (i, 0);
        }
        rows += n;
    }
    targets.push(span(first, ranges.len() - 1));
    targets
}

/// One keyset page of `(id, _00_rv)`, in id order like the bootstrap pager.
fn listing_query(table: &str, after: Option<&str>) -> String {
    match after {
        None => format!("SELECT id, _00_rv FROM {table} ORDER BY id LIMIT {LIST_PAGE}"),
        Some(id) => format!(
            "SELECT id, _00_rv FROM {table} WHERE id > {} ORDER BY id LIMIT {LIST_PAGE}",
            ssp_protocol::record_id_literal(table, id)
        ),
    }
}

/// `(id, _00_rv)` of range `i`: a key-range scan, so it reads only that
/// range. Not paged: `LIMIT` is not pushed into a range scan, and a range is
/// about `RANGE_ROWS` rows.
fn range_listing_query(table: &str, ranges: &RangeHashes, i: usize) -> String {
    format!("SELECT id, _00_rv FROM {}", ranges.target(table, i))
}

/// Whole bodies of the listed ids, by record id rather than by scan.
fn fetch_query(table: &str, ids: &[String], omit: &BTreeSet<String>) -> String {
    let omit = ssp_protocol::omit_clause(omit);
    let ids: Vec<String> = ids.iter().map(|id| ssp_protocol::record_id_literal(table, id)).collect();
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
            "SELECT id, _00_rv FROM game WHERE id > game:abc ORDER BY id LIMIT 10000"
        );
        assert_eq!(
            listing_query("game", Some("game:7")),
            "SELECT id, _00_rv FROM game WHERE id > game:7 ORDER BY id LIMIT 10000"
        );
    }

    #[test]
    fn fetch_selects_by_record_id_and_omits_opaque_fields() {
        let omit: BTreeSet<String> = ["blob".to_string()].into();
        assert_eq!(
            fetch_query("game", &["a".to_string(), "game:b".to_string(), "game:`a-b`".to_string(), "game:42".to_string()], &omit),
            "SELECT * OMIT blob FROM [game:a, game:b, game:`a-b`, game:42]"
        );
    }

    #[test]
    fn range_listings_are_key_range_scans() {
        let ranges = RangeHashes::with_starts(vec!["".into(), "g".into(), "p".into()]).unwrap();
        assert_eq!(range_listing_query("game", &ranges, 0), "SELECT id, _00_rv FROM game:..⟨g⟩");
        assert_eq!(range_listing_query("game", &ranges, 1), "SELECT id, _00_rv FROM game:⟨g⟩..⟨p⟩");
        assert_eq!(range_listing_query("game", &ranges, 2), "SELECT id, _00_rv FROM game:⟨p⟩..");
    }

    #[test]
    fn pages_group_ranges_up_to_the_page_size() {
        let mut ranges =
            RangeHashes::with_starts(vec!["".into(), "d".into(), "h".into(), "p".into()]).unwrap();
        for (i, n) in [(0, 400), (1, 500), (2, 300), (3, 900)] {
            for k in 0..n {
                ranges.add_at(i, &ssp_protocol::snapshot_hash::record_digest(&format!("{i}{k}"), &serde_json::json!({})));
            }
        }
        assert_eq!(page_targets("t", &ranges, 1000), vec!["t:..⟨h⟩", "t:⟨h⟩..⟨p⟩", "t:⟨p⟩.."]);
        // A range bigger than a page is still one page.
        assert_eq!(page_targets("t", &ranges, 100), vec!["t:..⟨d⟩", "t:⟨d⟩..⟨h⟩", "t:⟨h⟩..⟨p⟩", "t:⟨p⟩.."]);
        assert_eq!(page_targets("t", &ranges, 10_000), vec!["t"]);
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
            gate: Default::default(),
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
        let reader = RowCheckpoints { dir: dir.clone(), on_disk: Default::default(), writing: Default::default(), gate: Default::default() };
        assert_eq!(reader.load_into(&fresh).await, 1);
        let c = fresh.read().await;
        let game = c.store.get_collection("game").unwrap();
        assert_eq!(game.rows.len(), 2);
        assert_eq!(game.catchup_xor, processor.read().await.store.get_collection("game").unwrap().catchup_xor);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A stand-in for the scheduler's `/proxy`: one table held as JSON rows,
    /// answering exactly the query shapes the repair sends, and recording them.
    #[derive(Clone)]
    struct FakeProxy {
        rows: Arc<std::collections::BTreeMap<String, Value>>,
        starts: Vec<String>,
        serve_ranges: bool,
        queries: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl FakeProxy {
        fn ranges(&self) -> RangeHashes {
            let mut r = RangeHashes::with_starts(self.starts.clone()).unwrap();
            for (raw, row) in self.rows.iter() {
                let i = r.range_of(raw).unwrap();
                r.add_at(i, &ssp_protocol::snapshot_hash::record_digest(raw, row));
            }
            r
        }

        /// Rows whose key is in the target `game:⟨lo⟩..⟨hi⟩` (either side open).
        fn in_target(&self, target: &str) -> Vec<Value> {
            let spec = target.strip_prefix("game").unwrap().strip_prefix(':').unwrap_or("..");
            let (lo, hi) = spec.split_once("..").unwrap();
            let unwrap = |b: &str| b.trim_start_matches('⟨').trim_end_matches('⟩').to_string();
            let (lo, hi) = (unwrap(lo), unwrap(hi));
            self.rows
                .iter()
                .filter(|(k, _)| k.as_str() >= lo.as_str() && (hi.is_empty() || k.as_str() < hi.as_str()))
                .map(|(_, v)| v.clone())
                .collect()
        }

        fn answer(&self, q: &str) -> Value {
            self.queries.lock().unwrap().push(q.to_string());
            let listing = |rows: Vec<Value>| {
                Value::Array(rows.iter().map(|r| serde_json::json!({ "id": r["id"], "_00_rv": r["_00_rv"] })).collect())
            };
            if let Some(target) = q.strip_prefix("SELECT id, _00_rv FROM ") {
                if let Some(rest) = target.strip_prefix("game ") {
                    // The keyset listing.
                    let after = rest
                        .strip_prefix("WHERE id > game:")
                        .and_then(|r| r.split_once(' ').map(|(a, _)| a.to_string()));
                    let rows: Vec<Value> = self
                        .rows
                        .iter()
                        .filter(|(k, _)| after.as_ref().map_or(true, |a| k.as_str() > a.as_str()))
                        .take(LIST_PAGE)
                        .map(|(_, v)| v.clone())
                        .collect();
                    return listing(rows);
                }
                return listing(self.in_target(target));
            }
            if let Some(ids) = q.strip_prefix("SELECT * FROM [") {
                let rows = ids
                    .trim_end_matches(']')
                    .split(", ")
                    .filter_map(|id| id.strip_prefix("game:"))
                    .filter_map(|id| self.rows.get(id).cloned())
                    .collect();
                return Value::Array(rows);
            }
            if let Some(target) = q.strip_prefix("SELECT * FROM ") {
                return Value::Array(self.in_target(target));
            }
            panic!("unexpected proxy query: {q}");
        }

        async fn serve(self) -> String {
            use axum::{routing::post, Json, Router};
            let query = self.clone();
            let ranges = self.clone();
            let app = Router::new()
                .route(
                    "/proxy/query",
                    post(move |Json(body): Json<Value>| {
                        let fake = query.clone();
                        async move { Json(fake.answer(body["query"].as_str().unwrap())) }
                    }),
                )
                .route(
                    "/proxy/ranges",
                    post(move || {
                        let fake = ranges.clone();
                        async move {
                            if !fake.serve_ranges {
                                return Err(axum::http::StatusCode::NOT_FOUND);
                            }
                            fake.queries.lock().unwrap().push("ranges".to_string());
                            Ok(Json(fake.ranges().to_wire("game")))
                        }
                    }),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            format!("http://{addr}/proxy")
        }
    }

    fn game_row(i: usize, n: i64, rv: i64) -> (String, Value) {
        let raw = format!("g{i:05}");
        let row = serde_json::json!({ "id": format!("game:{raw}"), "n": n, "_00_rv": rv });
        (raw, row)
    }

    /// The replica holds 3000 rows in three ranges. The SSP holds the same
    /// except: one row changed without a new version (range 0), one row
    /// missing and one extra (range 2). Range 1 must never be read.
    async fn warm_case(serve_ranges: bool) -> (Repair, Vec<String>, bool) {
        let rows: std::collections::BTreeMap<String, Value> =
            (0..3000).map(|i| game_row(i, i as i64, 1)).collect();
        let fake = FakeProxy {
            rows: Arc::new(rows.clone()),
            starts: vec!["".into(), "g01000".into(), "g02000".into()],
            serve_ranges,
            queries: Default::default(),
        };
        let expected = ssp_protocol::snapshot_hash::xor_acc_to_hex(&fake.ranges().total());
        let proxy_url = fake.clone().serve().await;
        let source = BootstrapSource::Proxy { client: reqwest::Client::new(), proxy_url };

        let processor = Arc::new(RwLock::new(Circuit::new()));
        {
            let mut c = processor.write().await;
            let coll = c.store.ensure_collection("game");
            for (raw, row) in rows.iter() {
                let row = match raw.as_str() {
                    "g00500" => game_row(500, -1, 1).1,
                    "g02500" => continue,
                    _ => row.clone(),
                };
                coll.apply(Operation::Create, raw, Sp00kyValue::from(row));
            }
            let (raw, row) = game_row(2600, 0, 1);
            coll.apply(Operation::Create, &format!("{raw}x"), Sp00kyValue::from(row));
        }

        let repair = repair_table(&source, &processor, "game", &BTreeSet::new(), &expected).await.unwrap();
        let queries = fake.queries.lock().unwrap().clone();
        let matched = table_matches(&processor, "game", &expected).await;
        (repair, queries, matched)
    }

    #[tokio::test]
    async fn repair_reads_only_the_ranges_that_differ() {
        let (repair, queries, matched) = warm_case(true).await;
        assert!(repair.matched && matched, "{repair:?}");
        assert_eq!((repair.ranges, repair.differing), (3, 2));
        assert_eq!(repair.listed, 2000, "ranges 0 and 2 listed, not range 1");
        assert_eq!(repair.deleted, 1, "the extra row");
        assert!(queries.iter().all(|q| !q.contains("g01000⟩..⟨g02000")), "range 1 was read: {queries:?}");
        assert_eq!(
            queries.iter().filter(|q| q.starts_with("SELECT id, _00_rv FROM game:")).count(),
            2
        );
        // Range 0's change kept its version, so range 0 was re-read whole.
        assert!(queries.contains(&"SELECT * FROM game:..⟨g01000⟩".to_string()), "{queries:?}");
        assert!(!queries.iter().any(|q| q.starts_with("SELECT * FROM game:⟨g02000⟩")));
    }

    #[tokio::test]
    async fn repair_lists_the_whole_table_without_ranges() {
        let (repair, queries, _) = warm_case(false).await;
        assert_eq!(repair.ranges, 0);
        assert_eq!(repair.listed, 3000);
        assert!(queries.iter().any(|q| q.starts_with("SELECT id, _00_rv FROM game ORDER BY id")));
        // Without ranges an equal-version change goes unseen; the caller pages.
        assert!(!repair.matched);
    }

    fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("ssp-rows-{name}-{}", unique_suffix()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn one_table_circuit() -> Arc<RwLock<Circuit>> {
        let mut c = Circuit::new();
        c.store.ensure_collection("game").apply(
            Operation::Create,
            "a",
            Sp00kyValue::from(serde_json::json!({ "id": "game:a", "n": 1 })),
        );
        Arc::new(RwLock::new(c))
    }

    #[tokio::test]
    async fn a_closed_gate_writes_and_removes_nothing() {
        let dir = scratch_dir("gate");
        let checkpoints = RowCheckpoints { dir: dir.clone(), on_disk: Default::default(), writing: Default::default(), gate: Default::default() };
        let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&open);
        checkpoints.set_gate(Box::new(move || flag.load(std::sync::atomic::Ordering::SeqCst)));

        let processor = one_table_circuit();
        checkpoints.write(&processor, "test").await;
        assert!(!checkpoints.path_for("game").exists(), "a standby or retired SSP writes nothing");

        open.store(true, std::sync::atomic::Ordering::SeqCst);
        checkpoints.write(&processor, "test").await;
        assert!(checkpoints.path_for("game").exists());

        // Closed again: a table this process dropped keeps its file, because
        // the directory belongs to the other instance now.
        open.store(false, std::sync::atomic::Ordering::SeqCst);
        processor.write().await.store.collections.remove("game");
        checkpoints.write(&processor, "test").await;
        assert!(checkpoints.path_for("game").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn temp_files_are_unique_per_write_and_only_stale_ones_are_swept() {
        let dir = scratch_dir("tmp");
        let target = dir.join("game.rows");
        let (a, b) = (tmp_path_for(&target), tmp_path_for(&target));
        assert_ne!(a, b);
        let name = a.file_name().unwrap().to_str().unwrap().to_string();
        assert!(name.starts_with("game.rows.tmp."), "{name}");
        assert_eq!(a.parent(), Some(dir.as_path()));

        // A fresh one may be the other instance's write in progress.
        std::fs::write(&a, b"partial").unwrap();
        assert!(read_dir_tables(&dir).tables.is_empty());
        assert!(a.exists(), "a fresh temp file is left alone");

        // An old one is an abandoned write. The bare legacy name counts too.
        let legacy = dir.join("user.rows.tmp");
        for path in [&a, &legacy] {
            let file = std::fs::File::options().create(true).write(true).open(path).unwrap();
            file.set_modified(std::time::SystemTime::now() - STALE_TMP_AGE - Duration::from_secs(1)).unwrap();
        }
        read_dir_tables(&dir);
        assert!(!a.exists() && !legacy.exists(), "stale temp files are swept");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
