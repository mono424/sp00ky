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

use crate::BootstrapSource;
use serde_json::Value;
use ssp::circuit::checkpoint::{read_collection, write_collection};
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

/// `(id, _00_rv)` of range `i`: a key-range scan, so it reads only that
/// range. Not paged: `LIMIT` is not pushed into a range scan, and a range is
/// about `RANGE_ROWS` rows.
fn range_listing_query(table: &str, ranges: &RangeHashes, i: usize) -> String {
    format!("SELECT id, _00_rv FROM {}", ranges.target(table, i))
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
                        .strip_prefix("WHERE id > type::record('game', '")
                        .and_then(|r| r.split_once('\'').map(|(a, _)| a.to_string()));
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
                    .split("type::record('game', '")
                    .skip(1)
                    .filter_map(|part| part.split_once('\'').map(|(id, _)| id))
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
}
