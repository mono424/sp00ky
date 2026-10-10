//! Warm restart for a cluster SSP.
//!
//! A cluster SSP used to rebuild from nothing on every restart: it paged every
//! synced table through the scheduler proxy while the scheduler held the
//! tenant's sync frozen, four minutes on whitepawn and growing with the data.
//! It keeps its rows instead:
//!
//! - **Checkpoint.** Each table's rows go to `$SPKY_SSP_SNAPSHOT_DIR/rows/` in
//!   the binary format of [`ssp::circuit::checkpoint`]: on SIGTERM, after
//!   every bootstrap, and on a timer. A table is written whole once
//!   (`<table>.rows`, a base image); after that, only the rows changed since
//!   the last write go out, as a delta (`<table>.NNNNNN.delta`) chained on
//!   the file before it, so a write costs the churn rather than the table.
//!   The table is written whole again when its deltas pile up
//!   ([`MAX_DELTAS`], or more than half the base's bytes), when a legacy
//!   file was converted at load, or when the base on disk is not the one
//!   this process extends. Only tables whose content hash moved since they
//!   were last written are written at all.
//! - **Load.** At boot the files go back into the circuit before the SSP
//!   registers. A file that fails its checks is deleted and its table paged;
//!   a delta that does not chain is deleted with the ones after it, and the
//!   table keeps the files before it. The load reads only the heads of each
//!   file (`checkpoint`); the bodies are verified by a pass that starts right
//!   away and runs while the SSP registers and repairs, streaming each file
//!   through sequentially, which is also what warms the page cache for the
//!   view priming that follows. A table whose bodies fail is dropped and
//!   paged again by an in-process re-bootstrap once this one has finished
//!   (`SPKY_SSP_CHECKPOINT_VERIFY=load` verifies before the table is served
//!   instead, at the cost of reading every file whole at boot).
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
//! - every write goes to a temp file unique to its process (`<file>.tmp.
//!   <pid>.<random>`; pids repeat across containers) and is renamed into
//!   place, so a reader sees an old file or a new one, never a torn one, and
//!   the loader only removes temp files old enough to be abandoned;
//! - a standby loads whatever listing it sees; every file is whole, a delta
//!   renamed in after the listing is simply not seen, and a chain the
//!   predecessor moved on from is caught by the chain checks;
//! - once promoted, before extending a chain it checks that the base on
//!   disk is still the one it loaded (the predecessor may have written the
//!   table whole meanwhile) and writes the table whole otherwise, and after
//!   a delta it removes the predecessor's later deltas of the old chain;
//! - a write removes only the files of tables this process itself loaded or
//!   wrote and no longer holds, and only while the gate lets it write;
//! - a file the loader cannot read is deleted, which the other instance may
//!   have written in a format this build does not read. That table is paged
//!   instead and the owner writes it again; time, not correctness;
//! - [`RowCheckpoints::clear`] (a clean restart directive) empties the
//!   directory for both; the VM shell never calls it from a standby.

use crate::BootstrapSource;
use serde_json::Value;
use ssp::circuit::arena::{configured_backing, ArenaBacking};
use ssp::circuit::checkpoint::{
    fresh_image_id, load_table, peek_identity, touch_bodies, verify_bodies, write_collection, write_delta,
    BodyVerify, ImageId, PendingVerify, TableLoad, Throttle, FORMAT,
};
use ssp::circuit::{Circuit, Operation, Record};
use ssp::types::Sp00kyValue;
use ssp_node::SspStatus;
use ssp_protocol::range_hash::RangeHashes;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Rows per `(id, _00_rv)` listing page. Two small fields per row, so a page
/// can be much larger than a bootstrap page of whole bodies.
const LIST_PAGE: usize = 10_000;

/// Rows per fetch of changed bodies.
const FETCH_BATCH: usize = 500;

/// Default for `SPKY_SSP_REPAIR_CONCURRENCY`: how many tables, and within a
/// table how many range listings, fetches or re-reads, a repair runs at a
/// time. The scheduler's default worker count: every proxy query is one
/// synchronous embedded-SurrealDB read on one of its workers, so more only
/// queues there. On whitepawn 11 tables repaired one after another took
/// 5.1 s, `game` alone 1.4 s over 203 range listings.
const REPAIR_CONCURRENCY_DEFAULT: usize = 4;

/// `SPKY_SSP_REPAIR_CONCURRENCY`, or the default; never zero.
pub fn resolve_repair_concurrency(value: Option<&str>) -> usize {
    value
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(REPAIR_CONCURRENCY_DEFAULT)
}

/// The repair concurrency for this process, resolved once.
pub fn repair_concurrency() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| resolve_repair_concurrency(std::env::var("SPKY_SSP_REPAIR_CONCURRENCY").ok().as_deref()))
}

/// Default for `SPKY_SSP_ROW_CHECKPOINT_SECS`. A delta costs the churn, so
/// a short cadence is cheap, and it keeps the repair after a restart small.
const DEFAULT_INTERVAL_SECS: u64 = 300;

/// A table with this many deltas is written whole next time.
pub const MAX_DELTAS: usize = if cfg!(test) { 4 } else { 64 };

/// Deltas that outweigh half the base's bytes end the chain too.
fn deltas_outweigh(base_bytes: u64, delta_bytes: u64) -> bool {
    delta_bytes * 2 > base_bytes
}

/// How long after a bootstrap the first checkpoint waits, so its table locks
/// stay out of the scheduler's replay and catch-up verification.
pub const POST_BOOTSTRAP_WRITE_DELAY: Duration = Duration::from_secs(120);

/// A temp file older than this is a write that will never finish (its process
/// died); younger ones may belong to the other instance sharing the directory.
const STALE_TMP_AGE: Duration = Duration::from_secs(600);

/// The secondary indexes the circuit held at the last checkpoint, by table,
/// next to the row files. Not a `.rows`/`.delta` name, so the table listing
/// skips it.
const INDEXES_FILE: &str = "indexes.json";

/// Whether this process may write checkpoints right now. See
/// [`RowCheckpoints::set_gate`].
pub type WriteGate = Box<dyn Fn() -> bool + Send + Sync>;

/// One table as it is on disk, the way this process last saw it.
#[derive(Debug, Clone)]
struct OnDisk {
    /// The table's catch-up hash as the last file written or loaded holds it.
    xor: [u8; 32],
    image_id: ImageId,
    base_bytes: u64,
    /// `(seq, bytes)` of each delta, in seq order.
    deltas: Vec<(u32, u64)>,
}

impl OnDisk {
    fn delta_bytes(&self) -> u64 {
        self.deltas.iter().map(|(_, bytes)| bytes).sum()
    }

    fn next_seq(&self) -> u32 {
        self.deltas.last().map_or(1, |(seq, _)| seq + 1)
    }

    fn wants_full(&self) -> bool {
        self.deltas.len() >= MAX_DELTAS || deltas_outweigh(self.base_bytes, self.delta_bytes())
    }
}

/// What one write of a table produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    Full,
    Delta {
        seq: u32,
        base: ImageId,
        prev_xor: [u8; 32],
    },
}

/// The row checkpoint directory and what is in it.
pub struct RowCheckpoints {
    dir: PathBuf,
    /// Each table as it is on disk, so a write skips the tables that have
    /// not changed since and extends the chain of those that have.
    on_disk: std::sync::Mutex<HashMap<String, OnDisk>>,
    /// One writer at a time: the timer, the post-bootstrap write and the
    /// shutdown write can otherwise overlap.
    writing: tokio::sync::Mutex<()>,
    /// Consulted before a write and before each table of it. Unset: always.
    gate: std::sync::OnceLock<WriteGate>,
    /// When a load checks the bodies of a mapped file.
    verify: BodyVerify,
    /// How the loaded rows are backed: mapped files or the heap.
    backing: ArenaBacking,
    /// Mapped files whose bodies the load left to [`Self::verify_pending`].
    pending: std::sync::Mutex<Vec<PendingVerify>>,
}

/// `SPKY_SSP_CHECKPOINT_VERIFY`: `background` (the default) verifies the
/// bodies of a mapped checkpoint while the SSP bootstraps; `load` verifies
/// them before the table is served, reading every file whole at boot.
pub fn verify_mode(value: Option<&str>) -> Result<BodyVerify, String> {
    match value.map(str::trim) {
        None | Some("") | Some("background") => Ok(BodyVerify::Deferred),
        Some("load") => Ok(BodyVerify::AtLoad),
        Some(other) => Err(format!("SPKY_SSP_CHECKPOINT_VERIFY={other:?}: expected `background` or `load`")),
    }
}

fn verify_mode_from_env() -> BodyVerify {
    let value = std::env::var("SPKY_SSP_CHECKPOINT_VERIFY").ok();
    match verify_mode(value.as_deref()) {
        Ok(mode) => mode,
        Err(why) => {
            warn!("{why}; verifying in the background");
            BodyVerify::Deferred
        }
    }
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
        let verify = verify_mode_from_env();
        info!(dir = %dir.display(), ?verify, "Row checkpoints enabled");
        Some(Arc::new(Self::at(dir, verify, configured_backing().clone())))
    }

    /// Checkpoints in `dir`, loaded with `backing` and verified per `verify`.
    pub fn at(dir: PathBuf, verify: BodyVerify, backing: ArenaBacking) -> Self {
        Self {
            dir,
            on_disk: Default::default(),
            writing: Default::default(),
            gate: Default::default(),
            verify,
            backing,
            pending: Default::default(),
        }
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

    /// `<table>.rows`: the base image.
    fn path_for(&self, table: &str) -> PathBuf {
        self.dir.join(format!("{}.rows", file_stem(table)))
    }

    /// `<table>.NNNNNN.delta`: delta `seq` of the base.
    fn delta_path_for(&self, table: &str, seq: u32) -> PathBuf {
        self.dir.join(delta_file_name(&file_stem(table), seq))
    }

    /// Read every checkpointed table into the circuit, replacing whatever it
    /// held for those tables. Returns the number of tables loaded.
    ///
    /// The files are mapped (or adopted) rather than copied, and a mapped
    /// load reads the heads alone: the bodies wait for
    /// [`Self::verify_pending`], unless the verify mode says otherwise. The
    /// log line carries the split so it stays visible.
    pub async fn load_into(&self, processor: &Arc<RwLock<Circuit>>) -> usize {
        let started = Instant::now();
        let (dir, verify, backing) = (self.dir.clone(), self.verify, self.backing.clone());
        let loaded = tokio::task::spawn_blocking(move || read_dir_tables(&dir, verify, &backing))
            .await
            .unwrap_or_default();
        let tables = loaded.tables.len();
        let rows: usize = loaded.tables.iter().map(|t| t.collection.rows.len()).sum();
        let bytes: u64 = loaded.tables.iter().map(|t| t.stats.bytes).sum();
        let bodies_bytes: u64 = loaded.tables.iter().map(|t| t.stats.bodies_bytes).sum();
        let mapped = loaded.tables.iter().filter(|t| t.stats.mapped).count();
        let converted = loaded.tables.iter().filter(|t| t.stats.converted).count();
        let heads_ms: u64 = loaded.tables.iter().map(|t| t.stats.heads_ms).sum();
        let verify_ms: u64 = loaded.tables.iter().map(|t| t.stats.verify_ms).sum();
        let pending = loaded.pending.len();
        let install_started = Instant::now();
        {
            let mut circuit = processor.write().await;
            let mut on_disk = self.on_disk.lock().unwrap();
            for load in loaded.tables {
                let name = load.collection.name.clone();
                // A converted file is not what is on disk: leaving it out of
                // `on_disk` makes the next write rewrite it in this format.
                if !load.stats.converted {
                    on_disk.insert(
                        name.clone(),
                        OnDisk {
                            xor: load.collection.catchup_xor,
                            image_id: load.image_id,
                            base_bytes: load.base_bytes,
                            deltas: load.deltas,
                        },
                    );
                }
                circuit.store.collections.insert(name, load.collection);
            }
        }
        self.pending.lock().unwrap().extend(loaded.pending);
        let install_ms = install_started.elapsed().as_millis() as u64;
        if tables > 0 || loaded.discarded > 0 || loaded.deltas_discarded > 0 {
            info!(
                tables,
                rows,
                bytes,
                bodies_bytes,
                mapped,
                copied = tables - mapped,
                converted,
                deltas = loaded.deltas,
                discarded = loaded.discarded,
                deltas_discarded = loaded.deltas_discarded,
                pending,
                heads_ms,
                verify_ms,
                install_ms,
                ms = started.elapsed().as_millis() as u64,
                "Loaded row checkpoint"
            );
        }
        tables
    }

    /// The mapped files whose bodies the load did not verify, handed over
    /// once: whoever takes them runs [`Self::verify_pending`].
    pub fn take_pending(&self) -> Vec<PendingVerify> {
        std::mem::take(&mut *self.pending.lock().unwrap())
    }

    /// Hash the bodies of each pending file against its trailer, one file
    /// after another on the blocking pool with a throttle, and return the
    /// ones that failed. Reading them through is also what warms the page
    /// cache behind the mappings.
    pub async fn verify_pending(&self, pending: Vec<PendingVerify>) -> Vec<PendingVerify> {
        let started = Instant::now();
        let (files, mut bytes, mut failed) = (pending.len(), 0u64, Vec::new());
        for p in pending {
            bytes += p.image.bodies.len() as u64;
            let image = p.image.clone();
            let outcome = tokio::task::spawn_blocking(move || verify_bodies(&image, Throttle::background())).await;
            match outcome {
                Ok(Ok(true)) => debug!(table = %p.table, file = %p.path.display(), "Row checkpoint bodies verified"),
                Ok(Ok(false)) => {
                    error!(table = %p.table, file = %p.path.display(), "Row checkpoint bodies do not match their hash");
                    failed.push(p);
                }
                Ok(Err(e)) => {
                    error!(table = %p.table, file = %p.path.display(), error = %e, "Row checkpoint bodies could not be read");
                    failed.push(p);
                }
                Err(e) => {
                    error!(table = %p.table, error = %e, "Row checkpoint verification task failed");
                    failed.push(p);
                }
            }
        }
        if files > 0 {
            info!(
                files,
                bytes,
                failed = failed.len(),
                ms = started.elapsed().as_millis() as u64,
                "Verified row checkpoint bodies"
            );
        }
        failed
    }

    /// Drop the tables whose bodies failed verification: each is removed
    /// from the store if it still holds that very image (a table paged or
    /// replaced since is left alone), its files are deleted when this
    /// process may write the directory, and it is forgotten on disk either
    /// way. Returns the tables removed, for the caller to page again.
    pub async fn discard_failed(&self, processor: &Arc<RwLock<Circuit>>, failed: Vec<PendingVerify>) -> Vec<String> {
        let mut dropped = Vec::new();
        let mut circuit = processor.write().await;
        for p in failed {
            let holds_image = circuit
                .store
                .get_collection(&p.table)
                .is_some_and(|coll| coll.rows.holds_image(&p.image));
            if holds_image {
                circuit.store.collections.remove(&p.table);
                if !dropped.contains(&p.table) {
                    dropped.push(p.table.clone());
                }
                warn!(table = %p.table, "Dropped the table loaded from a checkpoint whose bodies failed verification");
            }
            self.on_disk.lock().unwrap().remove(&p.table);
            if self.may_write() {
                self.remove_table_files(&p.table);
            }
        }
        dropped
    }

    /// Delete a table's base and every delta of it in the directory.
    fn remove_table_files(&self, table: &str) {
        let stem = file_stem(table);
        let _ = std::fs::remove_file(self.path_for(table));
        if let Some(state) = list_dir(&self.dir).get(&stem) {
            for seq in &state.deltas {
                let _ = std::fs::remove_file(self.delta_path_for(table, *seq));
            }
        }
    }

    /// Write every table whose content changed since it was last written,
    /// as a delta where a chain can be extended and whole otherwise, and
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
        // (name, hash, whether nothing of the table's arena is on disk)
        let held: Vec<(String, [u8; 32], bool)> = {
            let circuit = processor.read().await;
            self.write_built_indexes(&circuit.built_indexes());
            circuit
                .store
                .collections
                .iter()
                // Runtime-internal tables (`_00_heartbeat`) are not synced and a
                // bootstrap drops them anyway; writing them is churn.
                .filter(|(name, _)| !ssp_protocol::table_excluded_from_sync(name))
                .map(|(name, coll)| (name.clone(), coll.catchup_xor, coll.rows.persisted().is_none()))
                .collect()
        };
        let known: HashMap<String, OnDisk> = self.on_disk.lock().unwrap().clone();
        let listing = list_dir(&self.dir);
        let plans: Vec<(String, Plan)> = held
            .iter()
            .filter_map(|(name, hash, unpersisted)| {
                let entry = known.get(name);
                if entry.is_some_and(|d| d.xor == *hash) {
                    return None;
                }
                let plan = match entry {
                    Some(d) if !unpersisted && !d.wants_full() && self.base_on_disk_is(name, d.image_id, &listing) => {
                        Plan::Delta {
                            seq: d.next_seq(),
                            base: d.image_id,
                            prev_xor: d.xor,
                        }
                    }
                    _ => Plan::Full,
                };
                Some((name.clone(), plan))
            })
            .collect();

        let (mut written, mut full, mut deltas, mut tombstones, mut rows, mut bytes, mut failed) =
            (0usize, 0usize, 0usize, 0u64, 0u64, 0u64, 0usize);
        let (mut encode_ms, mut fsync_ms) = (0u64, 0u64);
        for (table, plan) in plans {
            if !self.may_write() {
                info!(reason, written, "Row checkpoint stopped: this process no longer owns the checkpoint dir");
                return;
            }
            if plan == Plan::Full {
                // A whole write copies every body; read the mapped ones
                // through first, outside the lock, so the copy under it
                // never waits on the disk.
                let images = processor
                    .read()
                    .await
                    .store
                    .get_collection(&table)
                    .map(|coll| coll.rows.images())
                    .unwrap_or_default();
                for image in images {
                    let _ = tokio::task::spawn_blocking(move || touch_bodies(&image, Throttle::none())).await;
                }
            }
            let guard = Arc::clone(processor).read_owned().await;
            let stem = file_stem(&table);
            let (name, dir, task_stem) = (table.clone(), self.dir.clone(), stem.clone());
            let result = tokio::task::spawn_blocking(move || {
                let Some(coll) = guard.store.collections.get(&name) else {
                    return Ok(None);
                };
                let hash = coll.catchup_xor;
                // The chain can only be extended from a watermark; one that
                // went between the plan and now makes this a whole write.
                let plan = match plan {
                    Plan::Delta { .. } if coll.rows.persisted().is_none() => Plan::Full,
                    plan => plan,
                };
                let path = match plan {
                    Plan::Full => dir.join(format!("{task_stem}.rows")),
                    Plan::Delta { seq, .. } => dir.join(delta_file_name(&task_stem, seq)),
                };
                let tmp = tmp_path_for(&path);
                let encode_started = Instant::now();
                let file = std::fs::File::create(&tmp)?;
                let mut out = std::io::BufWriter::with_capacity(1 << 20, file);
                let w = match plan {
                    Plan::Full => write_collection(coll, fresh_image_id(), &mut out)?,
                    Plan::Delta { seq, base, prev_xor } => write_delta(coll, base, seq, prev_xor, &mut out)?,
                };
                // Release the circuit before the fsync: a flush to disk can
                // take seconds and ingest has no reason to wait for it.
                drop(guard);
                let encode_ms = encode_started.elapsed().as_millis() as u64;
                let sync_started = Instant::now();
                let file = out.into_inner().map_err(|e| e.into_error())?;
                file.sync_all()?;
                std::fs::rename(&tmp, &path)?;
                let fsync_ms = sync_started.elapsed().as_millis() as u64;
                Ok::<_, std::io::Error>(Some((hash, w, plan, encode_ms, fsync_ms)))
            })
            .await;
            match result {
                Ok(Ok(Some((hash, w, plan, table_encode_ms, table_fsync_ms)))) => {
                    debug!(
                        table = %table,
                        kind = match plan { Plan::Full => "full", Plan::Delta { .. } => "delta" },
                        seq = w.seq,
                        rows = w.rows,
                        tombstones = w.tombstones,
                        bytes = w.bytes,
                        encode_ms = table_encode_ms,
                        fsync_ms = table_fsync_ms,
                        "Row checkpoint table written"
                    );
                    // The table knows what is on disk now. A mark of another
                    // epoch means the table was replaced meanwhile; its
                    // watermark is gone and the next write is whole.
                    {
                        let mut circuit = processor.write().await;
                        if let Some(coll) = circuit.store.collections.get_mut(&table) {
                            coll.rows.mark_persisted(w.mark);
                        }
                    }
                    let stale: Vec<u32> = {
                        let mut on_disk = self.on_disk.lock().unwrap();
                        let in_dir = listing.get(&stem).map(|s| s.deltas.clone()).unwrap_or_default();
                        match plan {
                            Plan::Full => {
                                let old = on_disk.insert(
                                    table.clone(),
                                    OnDisk { xor: hash, image_id: w.image_id, base_bytes: w.bytes, deltas: Vec::new() },
                                );
                                // Every delta is of a chain that ended; a
                                // crash before this leaves orphans the
                                // loader discards.
                                old.map(|d| d.deltas.into_iter().map(|(seq, _)| seq).collect::<Vec<_>>())
                                    .unwrap_or_default()
                                    .into_iter()
                                    .chain(in_dir)
                                    .collect()
                            }
                            Plan::Delta { seq, .. } => {
                                if let Some(d) = on_disk.get_mut(&table) {
                                    d.deltas.push((seq, w.bytes));
                                    d.xor = hash;
                                }
                                // The predecessor's tail of the chain this
                                // process just moved past.
                                in_dir.into_iter().filter(|s| *s > seq).collect()
                            }
                        }
                    };
                    for seq in stale {
                        let _ = std::fs::remove_file(self.delta_path_for(&table, seq));
                    }
                    written += 1;
                    match plan {
                        Plan::Full => full += 1,
                        Plan::Delta { .. } => deltas += 1,
                    }
                    tombstones += w.tombstones;
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

        let live: HashSet<&str> = held.iter().map(|(name, _, _)| name.as_str()).collect();
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
            self.remove_table_files(table);
        }

        if written > 0 || failed > 0 || !gone.is_empty() {
            info!(
                reason,
                written,
                full,
                deltas,
                tombstones,
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

    /// Whether the base on disk for `table` is the image this process
    /// extends. The other instance of a shared directory may have written
    /// the table whole since; a delta of the old chain would be an orphan.
    fn base_on_disk_is(&self, table: &str, image_id: ImageId, listing: &BTreeMap<String, DirState>) -> bool {
        listing.get(&file_stem(table)).is_some_and(|s| s.base)
            && peek_identity(&self.path_for(table))
                .is_ok_and(|identity| identity.format == FORMAT && identity.image_id == image_id)
    }

    /// Delete every checkpoint, for a clean restart.
    /// What [`Self::write`] last recorded of the circuit's built indexes; a
    /// bootstrap rebuilds these before it serves (`Circuit::prebuild_indexes`).
    pub fn read_built_indexes(&self) -> BTreeMap<String, Vec<String>> {
        std::fs::read(self.dir.join(INDEXES_FILE))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    fn write_built_indexes(&self, built: &BTreeMap<String, Vec<String>>) {
        let path = self.dir.join(INDEXES_FILE);
        if built.is_empty() && !path.exists() {
            return;
        }
        let tmp = tmp_path_for(&path);
        let written = serde_json::to_vec(built)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(&tmp, bytes))
            .and_then(|()| std::fs::rename(&tmp, &path));
        if let Err(e) = written {
            warn!(error = %e, "Could not record the built indexes for the next restart");
        }
    }

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

/// `<file>.tmp.<pid>.<random>` next to `<file>`.
fn tmp_path_for(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(format!(".tmp.{}", unique_suffix()));
    path.with_file_name(name)
}

/// A temp file of an abandoned write: `.tmp.` in its name (or the older
/// bare `.tmp` ending) and untouched for [`STALE_TMP_AGE`].
fn is_stale_tmp(path: &Path) -> bool {
    let named = path
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.contains(".tmp.") || n.ends_with(".tmp"));
    named
        && std::fs::metadata(path)
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age >= STALE_TMP_AGE)
}

/// A file name for a table: identifier characters kept, anything else hex
/// escaped, so a stem never holds a `.` and the file kinds below parse
/// without ambiguity. The table name inside the file is what counts on
/// load; this only has to be unique and safe.
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

fn delta_file_name(stem: &str, seq: u32) -> String {
    format!("{stem}.{seq:06}.delta")
}

/// What a file in the directory is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Base,
    Delta(u32),
}

/// `<stem>.rows` or `<stem>.NNNNNN.delta`; anything else (temp files,
/// probes) is `None`.
fn parse_file_name(name: &str) -> Option<(&str, FileKind)> {
    let stem_ok = |stem: &str| !stem.is_empty() && !stem.contains('.');
    if let Some(stem) = name.strip_suffix(".rows") {
        return stem_ok(stem).then_some((stem, FileKind::Base));
    }
    let rest = name.strip_suffix(".delta")?;
    let (stem, seq) = rest.rsplit_once('.')?;
    if !stem_ok(stem) || seq.len() != 6 || !seq.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let seq: u32 = seq.parse().ok()?;
    (seq > 0).then_some((stem, FileKind::Delta(seq)))
}

/// The files of one table, by stem.
#[derive(Debug, Default, Clone)]
struct DirState {
    base: bool,
    deltas: BTreeSet<u32>,
}

/// One readdir: which files each stem has right now. Temp files are not
/// listed.
fn list_dir(dir: &Path) -> BTreeMap<String, DirState> {
    let mut out: BTreeMap<String, DirState> = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some((stem, kind)) = name.to_str().and_then(parse_file_name) else {
            continue;
        };
        let state = out.entry(stem.to_string()).or_default();
        match kind {
            FileKind::Base => state.base = true,
            FileKind::Delta(seq) => {
                state.deltas.insert(seq);
            }
        }
    }
    out
}

/// What [`read_dir_tables`] found.
#[derive(Default)]
struct Loaded {
    tables: Vec<TableLoad>,
    /// Tables whose base failed its checks; their files were deleted.
    discarded: usize,
    /// Deltas applied.
    deltas: usize,
    /// Deltas that did not chain, were orphaned or left a gap; deleted.
    deltas_discarded: usize,
    /// Mapped files whose bodies are still to be verified.
    pending: Vec<PendingVerify>,
}

/// Read every table in the directory: its base and the longest unbroken
/// chain of deltas after it. A base that fails to read is deleted with its
/// deltas and its table paged instead; a delta that fails is deleted with
/// every later one and the table keeps the files before it. The next write
/// replaces what went.
fn read_dir_tables(dir: &Path, verify: BodyVerify, backing: &ArenaBacking) -> Loaded {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Loaded::default();
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let known = path.file_name().and_then(|n| n.to_str()).and_then(parse_file_name).is_some();
        // A temp file left by a write that never finished. Only an old one:
        // a fresh one may be another instance's write in progress.
        if !known && is_stale_tmp(&path) {
            let _ = std::fs::remove_file(&path);
        }
    }
    let mut out = Loaded::default();
    for (stem, state) in list_dir(dir) {
        let base = dir.join(format!("{stem}.rows"));
        let delta_path = |seq: u32| dir.join(delta_file_name(&stem, seq));
        if !state.base {
            warn!(stem, deltas = state.deltas.len(), "Discarding row checkpoint deltas without a base");
            for seq in &state.deltas {
                let _ = std::fs::remove_file(delta_path(*seq));
            }
            out.deltas_discarded += state.deltas.len();
            continue;
        }
        // The chain is the longest run 1, 2, ... present; a gap ends it.
        let mut chain: Vec<PathBuf> = Vec::new();
        for seq in &state.deltas {
            if *seq == chain.len() as u32 + 1 {
                chain.push(delta_path(*seq));
            } else {
                let _ = std::fs::remove_file(delta_path(*seq));
                out.deltas_discarded += 1;
            }
        }
        let started = Instant::now();
        match load_table(&base, &chain, verify, backing) {
            Ok(load) => {
                if let Some((k, e)) = &load.refused {
                    warn!(
                        table = %load.collection.name,
                        file = %chain[*k].display(),
                        error = %e,
                        later = chain.len() - k - 1,
                        "Discarding a row checkpoint delta and the ones after it"
                    );
                    for path in &chain[*k..] {
                        let _ = std::fs::remove_file(path);
                    }
                    out.deltas_discarded += chain.len() - k;
                }
                debug!(
                    table = %load.collection.name,
                    rows = load.collection.rows.len(),
                    bytes = load.stats.bytes,
                    bodies_bytes = load.stats.bodies_bytes,
                    deltas = load.deltas.len(),
                    mapped = load.stats.mapped,
                    converted = load.stats.converted,
                    deferred = load.pending.len(),
                    heads_ms = load.stats.heads_ms,
                    verify_ms = load.stats.verify_ms,
                    ms = started.elapsed().as_millis() as u64,
                    "Loaded row checkpoint table"
                );
                out.deltas += load.deltas.len();
                out.pending.extend(load.pending.iter().cloned());
                out.tables.push(load);
            }
            Err(e) => {
                warn!(file = %base.display(), error = %e, "Discarding row checkpoint");
                let _ = std::fs::remove_file(&base);
                for path in &chain {
                    let _ = std::fs::remove_file(path);
                }
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

/// One table to repair: what the scheduler expects it to hash to, and the
/// opaque fields to leave out of the fetches.
#[derive(Debug, Clone)]
pub struct RepairJob {
    pub table: String,
    pub omit: BTreeSet<String>,
    pub want: String,
}

/// Repair `jobs`, `concurrency` tables at a time, each with the same
/// concurrency inside ([`repair_table`]). Returns every job's outcome and
/// how long it took; the order is completion order.
pub async fn repair_tables(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    jobs: Vec<RepairJob>,
    concurrency: usize,
) -> Vec<(RepairJob, anyhow::Result<Repair>, u64)> {
    use futures::stream::{self, StreamExt};
    let concurrency = concurrency.max(1);
    // The futures are built up front rather than by `StreamExt::map`: a
    // closure producing futures over borrowed arguments is what rustc cannot
    // prove `Send` for every lifetime, and this whole bootstrap is spawned.
    let repairs: Vec<_> = jobs
        .into_iter()
        .map(|job| async move {
            let started = Instant::now();
            let outcome = repair_table(source, processor, &job.table, &job.omit, &job.want, concurrency).await;
            (job, outcome, started.elapsed().as_millis() as u64)
        })
        .collect();
    stream::iter(repairs).buffer_unordered(concurrency).collect().await
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
/// see one consistent table. `concurrency` range listings, fetches or
/// re-reads run at a time; the ranges are disjoint, so their writes never
/// touch the same rows.
pub async fn repair_table(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    expected: &str,
    concurrency: usize,
) -> anyhow::Result<Repair> {
    match source.table_ranges(table).await {
        Some(wire) if wire.hash == expected => match RangeHashes::from_wire(&wire) {
            Some(ranges) => {
                return repair_by_ranges(source, processor, table, omit, expected, &ranges, concurrency).await
            }
            None => warn!(table, "Scheduler served range hashes that do not add up; listing the table in full"),
        },
        Some(wire) => debug!(table, ranges = %wire.hash, expected, "Range hashes are of another cut; listing the table in full"),
        None => {}
    }
    repair_by_listing(source, processor, table, omit, expected, concurrency).await
}

/// [`repair_table`] over the scheduler's id ranges.
async fn repair_by_ranges(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    expected: &str,
    ranges: &RangeHashes,
    concurrency: usize,
) -> anyhow::Result<Repair> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    let concurrency = concurrency.max(1);
    let mut repair = Repair { ranges: ranges.len(), ..Repair::default() };
    let Some(differing) = differing_ranges(processor, table, ranges, None).await else {
        return Ok(repair);
    };
    repair.differing = differing.len();

    // Listing a range ships two fields a row; fetching ships bodies. Only a
    // repair that would fetch most of the table is better off paging it.
    // (Futures built up front, see `repair_tables`.)
    let listing_reads: Vec<_> = differing
        .iter()
        .map(|&i| async move { source.query(&range_listing_query(table, ranges, i)).await.map(rows_of) })
        .collect();
    let listings: Vec<Vec<Value>> = stream::iter(listing_reads)
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;
    let listing: Vec<(String, Option<i64>)> =
        listings.iter().flatten().filter_map(listing_entry).collect();
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
    repair.fetched = fetch_rows(source, processor, table, omit, &fetch, concurrency).await?;

    // Equal versions were taken as equal content. A range that still differs
    // holds a row changed without a new version: read it whole.
    let Some(still) = differing_ranges(processor, table, ranges, Some(&differing)).await else {
        return Ok(repair);
    };
    let still_rows: u64 = still.iter().map(|&i| ranges.count(i)).sum();
    if still_rows * 2 > ranges.rows().max(1) {
        return Ok(repair);
    }
    let rereads: Vec<_> = still
        .iter()
        .map(|&i| reread_range(source, processor, table, omit, ranges, i))
        .collect();
    let reread: Vec<(usize, usize)> = stream::iter(rereads)
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;
    for (fetched, deleted) in reread {
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
    concurrency: usize,
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
    repair.fetched = fetch_rows(source, processor, table, omit, &fetch, concurrency).await?;
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

/// Fetch the listed ids' bodies by record id, `FETCH_BATCH` at a time and
/// `concurrency` batches at once, and apply them. Returns how many came back.
async fn fetch_rows(
    source: &BootstrapSource,
    processor: &Arc<RwLock<Circuit>>,
    table: &str,
    omit: &BTreeSet<String>,
    ids: &[String],
    concurrency: usize,
) -> anyhow::Result<usize> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    let fetches: Vec<_> = ids
        .chunks(FETCH_BATCH)
        .map(|batch| async move {
            let records = records_of(table, rows_of(source.query(&fetch_query(table, batch, omit)).await?));
            let fetched = records.len();
            apply_records(processor, table, records).await;
            Ok::<usize, anyhow::Error>(fetched)
        })
        .collect();
    let counts: Vec<usize> = stream::iter(fetches)
        .buffer_unordered(concurrency.max(1))
        .try_collect()
        .await?;
    Ok(counts.into_iter().sum())
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
        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);

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
            for i in 0..40 {
                c.store.ensure_collection("game").apply(
                    Operation::Create,
                    &format!("r{i}"),
                    Sp00kyValue::from(serde_json::json!({ "id": format!("game:r{i}"), "n": i, "s": "filler row" })),
                );
            }
        }
        checkpoints.write(&processor, "test").await;
        let game_file = checkpoints.path_for("game");
        let first_write = std::fs::metadata(&game_file).unwrap().modified().unwrap();

        // Unchanged: nothing is rewritten.
        checkpoints.write(&processor, "test").await;
        assert_eq!(std::fs::metadata(&game_file).unwrap().modified().unwrap(), first_write);

        // Changed: a delta, the base untouched.
        processor.write().await.store.ensure_collection("game").apply(
            Operation::Update,
            "a",
            Sp00kyValue::from(serde_json::json!({ "id": "game:a", "n": 2 })),
        );
        checkpoints.write(&processor, "test").await;
        assert_eq!(std::fs::metadata(&game_file).unwrap().modified().unwrap(), first_write);
        assert!(checkpoints.delta_path_for("game", 1).exists());
        checkpoints.write(&processor, "test").await;
        assert!(!checkpoints.delta_path_for("game", 2).exists(), "unchanged again: nothing");

        // A table that disappears loses its file.
        processor.write().await.store.collections.remove("user");
        checkpoints.write(&processor, "test").await;
        assert!(!checkpoints.path_for("user").exists());

        let fresh = Arc::new(RwLock::new(Circuit::new()));
        let reader = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        assert_eq!(reader.load_into(&fresh).await, 1);
        assert!(reader.take_pending().is_empty(), "a heap load verifies at once");
        {
            let c = fresh.read().await;
            let game = c.store.get_collection("game").unwrap();
            assert_eq!(game.rows.len(), 42);
            assert_eq!(game.get_row("a").get("n").as_i64(), Some(2), "the delta applied");
            assert_eq!(game.catchup_xor, processor.read().await.store.get_collection("game").unwrap().catchup_xor);
        }
        // Seeded from the load: nothing to write, and the next change
        // extends the chain the load saw.
        reader.write(&fresh, "test").await;
        assert!(!reader.delta_path_for("game", 2).exists());
        fresh.write().await.store.ensure_collection("game").apply(Operation::Delete, "b", Sp00kyValue::Null);
        reader.write(&fresh, "test").await;
        assert!(reader.delta_path_for("game", 2).exists(), "the chain continues after a load");
        let again = Arc::new(RwLock::new(Circuit::new()));
        assert_eq!(RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap).load_into(&again).await, 1);
        let c = again.read().await;
        let game = c.store.get_collection("game").unwrap();
        assert!(!game.has_row("b"), "the tombstone applied");
        assert_eq!(game.catchup_xor, fresh.read().await.store.get_collection("game").unwrap().catchup_xor);

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
        /// Queries in flight right now, and the most there ever were.
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
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
                        async move {
                            use std::sync::atomic::Ordering;
                            let now = fake.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                            fake.max_in_flight.fetch_max(now, Ordering::SeqCst);
                            // Long enough for concurrent queries to overlap.
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            let answer = fake.answer(body["query"].as_str().unwrap());
                            fake.in_flight.fetch_sub(1, Ordering::SeqCst);
                            Json(answer)
                        }
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

    /// What a repair of the warm case produced: its outcome, the queries
    /// the proxy saw, whether the table matches, and the most queries the
    /// proxy had in flight at once.
    struct WarmOutcome {
        repair: Repair,
        queries: Vec<String>,
        matched: bool,
        max_in_flight: usize,
        xor: [u8; 32],
    }

    /// The replica holds 3000 rows in three ranges. The SSP holds the same
    /// except: one row changed without a new version (range 0), one row
    /// missing and one extra (range 2). Range 1 must never be read.
    async fn warm_case(serve_ranges: bool, concurrency: usize) -> WarmOutcome {
        let rows: std::collections::BTreeMap<String, Value> =
            (0..3000).map(|i| game_row(i, i as i64, 1)).collect();
        let fake = FakeProxy {
            rows: Arc::new(rows.clone()),
            starts: vec!["".into(), "g01000".into(), "g02000".into()],
            serve_ranges,
            queries: Default::default(),
            in_flight: Default::default(),
            max_in_flight: Default::default(),
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

        let repair = repair_table(&source, &processor, "game", &BTreeSet::new(), &expected, concurrency).await.unwrap();
        let queries = fake.queries.lock().unwrap().clone();
        let matched = table_matches(&processor, "game", &expected).await;
        let xor = processor.read().await.store.get_collection("game").unwrap().catchup_xor;
        WarmOutcome {
            repair,
            queries,
            matched,
            max_in_flight: fake.max_in_flight.load(std::sync::atomic::Ordering::SeqCst),
            xor,
        }
    }

    #[tokio::test]
    async fn repair_reads_only_the_ranges_that_differ() {
        let WarmOutcome { repair, queries, matched, .. } = warm_case(true, 1).await;
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

    /// Four listings, fetches and re-reads at a time land on the same rows
    /// and the same hash as one at a time, and the proxy does see them
    /// overlap.
    #[tokio::test]
    async fn concurrent_repair_matches_the_sequential_result() {
        let one = warm_case(true, 1).await;
        let four = warm_case(true, 4).await;
        assert!(one.repair.matched && four.repair.matched);
        assert_eq!(
            (four.repair.ranges, four.repair.differing, four.repair.listed, four.repair.fetched, four.repair.deleted),
            (one.repair.ranges, one.repair.differing, one.repair.listed, one.repair.fetched, one.repair.deleted)
        );
        assert_eq!(four.xor, one.xor);
        assert_eq!(one.max_in_flight, 1, "sequential");
        assert!((2..=4).contains(&four.max_in_flight), "concurrent: {}", four.max_in_flight);
        assert_eq!(one.queries.len(), four.queries.len());
    }

    #[tokio::test]
    async fn repair_tables_hands_back_every_job_with_its_outcome() {
        let rows: std::collections::BTreeMap<String, Value> = (0..300).map(|i| game_row(i, i as i64, 1)).collect();
        let fake = FakeProxy {
            rows: Arc::new(rows.clone()),
            starts: vec!["".into(), "g00100".into(), "g00200".into()],
            serve_ranges: true,
            queries: Default::default(),
            in_flight: Default::default(),
            max_in_flight: Default::default(),
        };
        let expected = ssp_protocol::snapshot_hash::xor_acc_to_hex(&fake.ranges().total());
        let proxy_url = fake.clone().serve().await;
        let source = BootstrapSource::Proxy { client: reqwest::Client::new(), proxy_url };
        let processor = Arc::new(RwLock::new(Circuit::new()));
        {
            let mut c = processor.write().await;
            let coll = c.store.ensure_collection("game");
            for (raw, row) in rows.iter().filter(|(raw, _)| raw.as_str() != "g00250") {
                coll.apply(Operation::Create, raw, Sp00kyValue::from(row.clone()));
            }
        }
        let jobs = vec![
            RepairJob { table: "game".into(), omit: BTreeSet::new(), want: expected.clone() },
            RepairJob { table: "absent".into(), omit: BTreeSet::new(), want: "x3:00".into() },
        ];
        let outcomes = repair_tables(&source, &processor, jobs, 4).await;
        assert_eq!(outcomes.len(), 2);
        for (job, outcome, _ms) in outcomes {
            match job.table.as_str() {
                "game" => {
                    let repair = outcome.unwrap();
                    assert!(repair.matched, "{repair:?}");
                    assert_eq!(repair.fetched, 1);
                }
                "absent" => assert!(!outcome.map(|r| r.matched).unwrap_or(false), "a table not held cannot be repaired"),
                other => panic!("{other}"),
            }
        }
    }

    #[test]
    fn repair_concurrency_defaults_to_four_and_rejects_zero() {
        assert_eq!(resolve_repair_concurrency(None), 4);
        assert_eq!(resolve_repair_concurrency(Some("0")), 4);
        assert_eq!(resolve_repair_concurrency(Some("nope")), 4);
        assert_eq!(resolve_repair_concurrency(Some(" 8 ")), 8);
    }

    #[tokio::test]
    async fn repair_lists_the_whole_table_without_ranges() {
        let WarmOutcome { repair, queries, .. } = warm_case(false, 1).await;
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
        table_circuit(0)
    }

    /// A `game` table with row `a` and `filler` more rows: enough of a base
    /// that a one-row delta does not outweigh it.
    fn table_circuit(filler: usize) -> Arc<RwLock<Circuit>> {
        let mut c = Circuit::new();
        let coll = c.store.ensure_collection("game");
        coll.apply(
            Operation::Create,
            "a",
            Sp00kyValue::from(serde_json::json!({ "id": "game:a", "n": 1 })),
        );
        for i in 0..filler {
            coll.apply(
                Operation::Create,
                &format!("r{i}"),
                Sp00kyValue::from(serde_json::json!({ "id": format!("game:r{i}"), "n": i, "s": "filler row" })),
            );
        }
        Arc::new(RwLock::new(c))
    }

    #[tokio::test]
    async fn a_closed_gate_writes_and_removes_nothing() {
        let dir = scratch_dir("gate");
        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
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
        let d = tmp_path_for(&dir.join("game.000003.delta"));
        std::fs::write(&d, b"partial").unwrap();
        assert!(read_dir_tables(&dir, BodyVerify::AtLoad, &ArenaBacking::Heap).tables.is_empty());
        assert!(a.exists() && d.exists(), "a fresh temp file is left alone");

        // An old one is an abandoned write. The bare legacy name counts too.
        let legacy = dir.join("user.rows.tmp");
        for path in [&a, &d, &legacy] {
            let file = std::fs::File::options().create(true).write(true).open(path).unwrap();
            file.set_modified(std::time::SystemTime::now() - STALE_TMP_AGE - Duration::from_secs(1)).unwrap();
        }
        read_dir_tables(&dir, BodyVerify::AtLoad, &ArenaBacking::Heap);
        assert!(!a.exists() && !d.exists() && !legacy.exists(), "stale temp files are swept");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_names_parse_back_to_stem_and_kind() {
        assert_eq!(parse_file_name("game.rows"), Some(("game", FileKind::Base)));
        assert_eq!(parse_file_name("a%2eb.rows"), Some(("a%2eb", FileKind::Base)));
        assert_eq!(parse_file_name("game.000001.delta"), Some(("game", FileKind::Delta(1))));
        assert_eq!(parse_file_name("game.123456.delta"), Some(("game", FileKind::Delta(123456))));
        assert_eq!(parse_file_name(&delta_file_name(&file_stem("a.b"), 7)), Some(("a%2eb", FileKind::Delta(7))));
        for name in [
            "game.rows.tmp.1.abc",
            "game.000001.delta.tmp.1.abc",
            ".probe.1.abc",
            "game.000000.delta",
            "game.1.delta",
            "game.delta",
            ".rows",
            "game.x.rows",
        ] {
            assert_eq!(parse_file_name(name), None, "{name}");
        }
    }

    /// Change the table, write, and hand back the new file's path.
    async fn change_and_write(checkpoints: &RowCheckpoints, processor: &Arc<RwLock<Circuit>>, n: i64) {
        processor.write().await.store.ensure_collection("game").apply(
            Operation::Update,
            "a",
            Sp00kyValue::from(serde_json::json!({ "id": "game:a", "n": n })),
        );
        checkpoints.write(processor, "test").await;
    }

    fn files_of(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .filter_map(|e| e.file_name().to_str().map(str::to_string))
            .collect();
        names.sort();
        names
    }

    #[tokio::test]
    async fn the_delta_cap_and_the_bytes_cap_force_a_full_base() {
        let dir = scratch_dir("cap");
        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        let processor = table_circuit(40);
        checkpoints.write(&processor, "test").await;
        let base = checkpoints.path_for("game");
        let first = ssp::circuit::checkpoint::peek_identity(&base).unwrap().image_id;
        // `n = 1` is what the base holds: a change has to change something.
        for n in 2..=MAX_DELTAS as i64 + 1 {
            change_and_write(&checkpoints, &processor, n).await;
        }
        assert_eq!(files_of(&dir).len(), 1 + MAX_DELTAS, "{:?}", files_of(&dir));
        // One more change: the chain is full, the table is written whole and
        // the deltas go.
        change_and_write(&checkpoints, &processor, 100).await;
        assert_eq!(files_of(&dir), vec!["game.rows".to_string()]);
        assert_ne!(ssp::circuit::checkpoint::peek_identity(&base).unwrap().image_id, first, "a fresh image id");

        // The bytes cap: a one-row base, a delta that outweighs half of it.
        let big = Arc::new(RwLock::new(Circuit::new()));
        big.write().await.store.ensure_collection("game").apply(
            Operation::Create,
            "a",
            Sp00kyValue::from(serde_json::json!({ "id": "game:a", "n": 1 })),
        );
        let dir2 = scratch_dir("bytes-cap");
        let checkpoints = RowCheckpoints::at(dir2.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        checkpoints.write(&big, "test").await;
        big.write().await.store.ensure_collection("game").apply(
            Operation::Create,
            "b",
            Sp00kyValue::from(serde_json::json!({ "id": "game:b", "blob": "x".repeat(4000) })),
        );
        checkpoints.write(&big, "test").await;
        assert!(checkpoints.delta_path_for("game", 1).exists(), "the first change is still a delta");
        big.write().await.store.ensure_collection("game").apply(
            Operation::Update,
            "a",
            Sp00kyValue::from(serde_json::json!({ "id": "game:a", "n": 2 })),
        );
        checkpoints.write(&big, "test").await;
        assert_eq!(files_of(&dir2), vec!["game.rows".to_string()], "the deltas outweighed the base");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[tokio::test]
    async fn a_delta_with_a_seq_gap_is_discarded_with_everything_after_it() {
        let dir = scratch_dir("gap");
        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        let processor = table_circuit(40);
        checkpoints.write(&processor, "test").await;
        for n in 2..=4 {
            change_and_write(&checkpoints, &processor, n).await;
        }
        std::fs::remove_file(checkpoints.delta_path_for("game", 2)).unwrap();
        let fresh = Arc::new(RwLock::new(Circuit::new()));
        let reader = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        assert_eq!(reader.load_into(&fresh).await, 1);
        assert_eq!(fresh.read().await.store.get_collection("game").unwrap().get_row("a").get("n").as_i64(), Some(2), "the state after delta 1");
        assert_eq!(files_of(&dir), vec!["game.000001.delta".to_string(), "game.rows".to_string()], "delta 3 went");
        // The next write continues from delta 1.
        change_and_write(&reader, &fresh, 9).await;
        assert!(reader.delta_path_for("game", 2).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_delta_of_another_base_is_an_orphan() {
        let dir = scratch_dir("orphan");
        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        let processor = table_circuit(40);
        checkpoints.write(&processor, "test").await;
        change_and_write(&checkpoints, &processor, 2).await;
        let delta = checkpoints.delta_path_for("game", 1);
        let kept = std::fs::read(&delta).unwrap();
        // A second process writes the table whole: a new chain.
        let other = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        other.write(&processor, "test").await;
        assert!(!delta.exists(), "a whole write ends the old chain");
        std::fs::write(&delta, kept).unwrap();
        let fresh = Arc::new(RwLock::new(Circuit::new()));
        let reader = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        assert_eq!(reader.load_into(&fresh).await, 1);
        assert!(!delta.exists(), "the orphan is removed");
        assert_eq!(fresh.read().await.store.get_collection("game").unwrap().get_row("a").get("n").as_i64(), Some(2));
        // Deltas without any base go too.
        std::fs::remove_file(checkpoints.path_for("game")).unwrap();
        std::fs::write(&delta, b"whatever").unwrap();
        let none = Arc::new(RwLock::new(Circuit::new()));
        assert_eq!(RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap).load_into(&none).await, 0);
        assert!(files_of(&dir).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Blue/green: a promoted standby loaded a chain its predecessor has
    /// since replaced. Its next write is whole, not an orphan delta, and
    /// after a delta of its own it removes the predecessor's later ones.
    #[tokio::test]
    async fn a_base_replaced_underneath_makes_the_next_write_full() {
        let dir = scratch_dir("underneath");
        let processor = table_circuit(40);
        let predecessor = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        predecessor.write(&processor, "test").await;
        change_and_write(&predecessor, &processor, 2).await;

        let standby_circuit = Arc::new(RwLock::new(Circuit::new()));
        let standby = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        assert_eq!(standby.load_into(&standby_circuit).await, 1);

        // The predecessor compacts: a new base, then a delta of it.
        for n in 3..=MAX_DELTAS as i64 + 2 {
            change_and_write(&predecessor, &processor, n).await;
        }
        assert_eq!(files_of(&dir), vec!["game.rows".to_string()]);
        change_and_write(&predecessor, &processor, 50).await;
        assert!(predecessor.delta_path_for("game", 1).exists());
        let new_base = ssp::circuit::checkpoint::peek_identity(&standby.path_for("game")).unwrap().image_id;

        // The standby, promoted, writes: whole, since its chain is gone.
        change_and_write(&standby, &standby_circuit, 7).await;
        assert_eq!(files_of(&dir), vec!["game.rows".to_string()], "{:?}", files_of(&dir));
        assert_ne!(ssp::circuit::checkpoint::peek_identity(&standby.path_for("game")).unwrap().image_id, new_base);
        // And its chain continues from there: the loader agrees.
        change_and_write(&standby, &standby_circuit, 8).await;
        let fresh = Arc::new(RwLock::new(Circuit::new()));
        assert_eq!(RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap).load_into(&fresh).await, 1);
        assert_eq!(fresh.read().await.store.get_collection("game").unwrap().get_row("a").get("n").as_i64(), Some(8));

        // A foreign delta beyond this process's chain goes with its delta.
        std::fs::write(standby.delta_path_for("game", 5), b"the predecessor's tail").unwrap();
        change_and_write(&standby, &standby_circuit, 9).await;
        assert_eq!(files_of(&dir), vec!["game.000001.delta".to_string(), "game.000002.delta".to_string(), "game.rows".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A checkpoint records the built indexes, and the next process's
    /// bootstrap reads them back to rebuild before it serves.
    #[tokio::test]
    async fn a_checkpoint_records_the_built_indexes() {
        use ssp::circuit::index::IndexDef;
        use ssp::circuit::TableMeta;
        let dir = scratch_dir("indexes");
        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        let processor = table_circuit(10);
        let def = IndexDef { name: "game_n".into(), fields: vec!["n".into()] };
        {
            let mut c = processor.write().await;
            c.set_table_meta("game", TableMeta { permission: "true".into(), indexes: vec![def.clone()], ..Default::default() });
        }
        checkpoints.write(&processor, "test").await;
        assert!(checkpoints.read_built_indexes().is_empty(), "nothing built, nothing recorded");
        let wanted = BTreeMap::from([("game".to_string(), vec!["game_n".to_string()])]);
        assert_eq!(processor.read().await.prebuild_indexes(&wanted).indexes_built, 1);
        change_and_write(&checkpoints, &processor, 2).await;
        assert_eq!(checkpoints.read_built_indexes(), wanted);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_removed_table_loses_its_base_and_its_deltas() {
        let dir = scratch_dir("removed");
        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        let processor = table_circuit(40);
        checkpoints.write(&processor, "test").await;
        change_and_write(&checkpoints, &processor, 2).await;
        assert_eq!(files_of(&dir).len(), 2);
        processor.write().await.store.collections.remove("game");
        checkpoints.write(&processor, "test").await;
        assert!(files_of(&dir).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A table replaced wholesale between two writes (a re-bootstrap paged
    /// it) has no watermark: its next write is whole and starts a chain.
    #[tokio::test]
    async fn a_replaced_table_starts_a_new_chain() {
        let dir = scratch_dir("replaced-chain");
        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap);
        let processor = table_circuit(40);
        checkpoints.write(&processor, "test").await;
        change_and_write(&checkpoints, &processor, 2).await;
        assert!(checkpoints.delta_path_for("game", 1).exists());
        {
            let mut c = processor.write().await;
            c.store.collections.remove("game");
            c.store.ensure_collection("game").apply(
                Operation::Create,
                "z",
                Sp00kyValue::from(serde_json::json!({ "id": "game:z", "n": 1 })),
            );
        }
        checkpoints.write(&processor, "test").await;
        assert_eq!(files_of(&dir), vec!["game.rows".to_string()]);
        let fresh = Arc::new(RwLock::new(Circuit::new()));
        assert_eq!(RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Heap).load_into(&fresh).await, 1);
        assert!(fresh.read().await.store.get_collection("game").unwrap().has_row("z"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn verify_mode_defaults_to_background() {
        assert_eq!(verify_mode(None), Ok(BodyVerify::Deferred));
        assert_eq!(verify_mode(Some("")), Ok(BodyVerify::Deferred));
        assert_eq!(verify_mode(Some("background")), Ok(BodyVerify::Deferred));
        assert_eq!(verify_mode(Some(" load ")), Ok(BodyVerify::AtLoad));
        assert!(verify_mode(Some("eager")).is_err());
    }

    /// A mapped checkpoint, loaded with its bodies deferred, then written
    /// over with a flipped body byte: the table serves until the pass finds
    /// it, then it is dropped with its file.
    async fn mapped_with_a_flipped_body(name: &str) -> (RowCheckpoints, Arc<RwLock<Circuit>>, PathBuf) {
        let dir = scratch_dir(name);
        let backing = ArenaBacking::Files { dir: dir.join("arena"), segment_bytes: 64 * 1024 };
        let writer = RowCheckpoints::at(dir.clone(), BodyVerify::Deferred, backing.clone());
        let processor = table_circuit(40);
        writer.write(&processor, "test").await;
        change_and_write(&writer, &processor, 2).await;
        assert!(writer.delta_path_for("game", 1).exists());
        let file = writer.path_for("game");
        let mut bytes = std::fs::read(&file).unwrap();
        let body_at = ssp::circuit::checkpoint::parse_header(&bytes).unwrap().bodies.start + 2;
        bytes[body_at] ^= 0x40;
        std::fs::write(&file, &bytes).unwrap();

        let reader = RowCheckpoints::at(dir.clone(), BodyVerify::Deferred, backing);
        let fresh = Arc::new(RwLock::new(Circuit::new()));
        assert_eq!(reader.load_into(&fresh).await, 1, "the heads check out, the table is served");
        assert_eq!(fresh.read().await.store.get_collection("game").unwrap().get_row("a").get("n").as_i64(), Some(2));
        (reader, fresh, file)
    }

    #[tokio::test]
    async fn a_failed_body_check_drops_the_table_and_its_file() {
        let (reader, fresh, file) = mapped_with_a_flipped_body("flipped").await;
        let pending = reader.take_pending();
        assert_eq!(pending.len(), 2, "the base and its delta");
        assert!(reader.take_pending().is_empty(), "handed over once");
        let failed = reader.verify_pending(pending).await;
        assert_eq!(failed.len(), 1, "only the base was damaged");
        let dropped = reader.discard_failed(&fresh, failed).await;
        assert_eq!(dropped, vec!["game".to_string()]);
        assert!(fresh.read().await.store.get_collection("game").is_none());
        assert!(!file.exists(), "the file is gone with the table");
        assert!(!reader.delta_path_for("game", 1).exists(), "and so are its deltas");
        assert!(reader.on_disk.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    #[tokio::test]
    async fn discard_leaves_a_table_that_no_longer_holds_the_image() {
        let (reader, fresh, file) = mapped_with_a_flipped_body("replaced").await;
        let pending = reader.take_pending();
        assert_eq!(pending.len(), 2, "the base and its delta");
        let failed = reader.verify_pending(pending).await;
        assert_eq!(failed.len(), 1, "only the base was damaged");
        // Paged again meanwhile: the table is not the one the file backed.
        {
            let mut c = fresh.write().await;
            c.store.collections.remove("game");
            c.store.ensure_collection("game").apply(
                Operation::Create,
                "z",
                Sp00kyValue::from(serde_json::json!({ "id": "game:z", "n": 9 })),
            );
        }
        let dropped = reader.discard_failed(&fresh, failed).await;
        assert!(dropped.is_empty());
        assert!(fresh.read().await.store.get_collection("game").unwrap().has_row("z"));
        assert!(!file.exists(), "the failed file still goes");
        let _ = std::fs::remove_dir_all(file.parent().unwrap());
    }

    /// A FORMAT 1 file is converted at load and, although its rows did not
    /// change, written again in this format by the next write.
    #[tokio::test]
    async fn a_converted_table_is_rewritten_as_format_two() {
        use ssp::circuit::checkpoint::{legacy, peek_identity};
        let dir = scratch_dir("legacy");
        let processor = one_table_circuit();
        let file = dir.join("game.rows");
        {
            let c = processor.read().await;
            let mut out = std::fs::File::create(&file).unwrap();
            legacy::write_v1(c.store.get_collection("game").unwrap(), &mut out).unwrap();
        }
        assert_eq!(peek_identity(&file).unwrap().format, legacy::FORMAT_V1);

        let checkpoints = RowCheckpoints::at(dir.clone(), BodyVerify::Deferred, ArenaBacking::Heap);
        let fresh = Arc::new(RwLock::new(Circuit::new()));
        assert_eq!(checkpoints.load_into(&fresh).await, 1);
        assert!(checkpoints.take_pending().is_empty());
        assert!(fresh.read().await.store.get_collection("game").unwrap().has_row("a"));
        assert!(checkpoints.on_disk.lock().unwrap().is_empty(), "a converted file is not what is on disk");

        checkpoints.write(&fresh, "test").await;
        assert_eq!(peek_identity(&file).unwrap().format, ssp::circuit::checkpoint::FORMAT);
        let rewritten = std::fs::metadata(&file).unwrap().modified().unwrap();
        checkpoints.write(&fresh, "test").await;
        assert_eq!(std::fs::metadata(&file).unwrap().modified().unwrap(), rewritten, "written once");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_verified_file_keeps_its_table() {
        let dir = scratch_dir("verified");
        let backing = ArenaBacking::Files { dir: dir.join("arena"), segment_bytes: 64 * 1024 };
        let writer = RowCheckpoints::at(dir.clone(), BodyVerify::Deferred, backing.clone());
        writer.write(&table_circuit(40), "test").await;
        let reader = RowCheckpoints::at(dir.clone(), BodyVerify::Deferred, backing);
        let fresh = Arc::new(RwLock::new(Circuit::new()));
        assert_eq!(reader.load_into(&fresh).await, 1);
        let pending = reader.take_pending();
        assert_eq!(pending.len(), 1);
        assert!(reader.verify_pending(pending).await.is_empty());
        assert!(fresh.read().await.store.get_collection("game").unwrap().has_row("a"));
        // A mapped table extends its chain: the delta's rows are appended
        // past the images, and a whole write copies the images through.
        change_and_write(&reader, &fresh, 2).await;
        assert!(reader.delta_path_for("game", 1).exists());
        for n in 3..=MAX_DELTAS as i64 + 2 {
            change_and_write(&reader, &fresh, n).await;
        }
        assert_eq!(files_of(&dir).iter().filter(|f| f.ends_with(".rows")).count(), 1);
        assert!(files_of(&dir).iter().all(|f| !f.ends_with(".delta")), "{:?}", files_of(&dir));
        let again = Arc::new(RwLock::new(Circuit::new()));
        let reader = RowCheckpoints::at(dir.clone(), BodyVerify::AtLoad, ArenaBacking::Files { dir: dir.join("arena"), segment_bytes: 64 * 1024 });
        assert_eq!(reader.load_into(&again).await, 1);
        assert_eq!(again.read().await.store.get_collection("game").unwrap().get_row("a").get("n").as_i64(), Some(MAX_DELTAS as i64 + 2));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
