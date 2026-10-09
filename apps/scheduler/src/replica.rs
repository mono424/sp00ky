use anyhow::{bail, Context, Result};
use serde_json::Value;
use ssp_protocol::range_hash::{self, RangeHashes, TableRanges};
use ssp_protocol::snapshot_hash;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use surrealdb::engine::local::RocksDb;
use surrealdb::opt::capabilities::{Capabilities, ExperimentalFeature};
use surrealdb::opt::Config;
use surrealdb::Surreal;
use tracing::{debug, info, trace, warn};

/// Config for the embedded replica DB: enable `Files` + `Surrealism`
/// experimental capabilities so dumps that reference `DEFINE BUCKET ...` (a
/// Files feature) or surrealism modules import cleanly. The main SurrealDB
/// runs with these enabled (via `SURREAL_CAPS_ALLOW_EXPERIMENTAL=surrealism,files`),
/// so if the replica isn't configured to match, every post-v3 restore that
/// touches buckets dies with "expected the experimental files feature to be
/// enabled" when the replica tries to import the dump.
fn replica_config() -> Config {
    Config::new().capabilities(
        Capabilities::default().with_experimental_features_allowed(&[
            ExperimentalFeature::Files,
            ExperimentalFeature::Surrealism,
        ]),
    )
}

/// The container's memory ceiling, if we are running under one. Read from
/// cgroup v2 first, then v1; `None` when unlimited or unreadable (a bare
/// process, a non-Linux dev box).
///
/// Only used for logging — but the number matters: SurrealDB derives its
/// RocksDB write-buffer budget from this limit, so it is what decides how much
/// a bootstrap clone can write before the engine stalls.
fn cgroup_memory_limit() -> Option<u64> {
    ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"]
        .iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .find_map(|raw| parse_cgroup_limit(&raw))
}

/// Parse one cgroup memory-limit file. `None` for "unlimited", which the two
/// cgroup versions spell differently: v2 writes the literal `max`, v1 writes a
/// huge sentinel (`i64::MAX` rounded down to a page multiple, so the exact
/// number varies by kernel — hence a plausibility bound, not an equality test).
fn parse_cgroup_limit(raw: &str) -> Option<u64> {
    /// No container is capped in petabytes; past this a value is a sentinel.
    const IMPLAUSIBLE: u64 = 1 << 50;
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    match raw.parse::<u64>() {
        Ok(v) if v < IMPLAUSIBLE => Some(v),
        _ => None,
    }
}

// Re-export RecordOp from messages to avoid duplication
pub use crate::messages::RecordOp;

/// True if an error from SurrealDB indicates a missing namespace, database,
/// or table. Used to translate "upstream isn't initialized yet" into an empty
/// result so bootstrap can run against a brand-new SurrealDB.
pub(crate) fn is_missing_error<E: std::fmt::Display>(e: &E) -> bool {
    let msg = e.to_string();
    msg.contains("does not exist")
        || msg.contains("Table not found")
        || msg.contains("not found in this database")
        || msg.contains("The namespace")
        || msg.contains("The database")
}

/// One-line description of a JSON value's variant for error messages.
fn json_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Build one page of a bootstrap table scan using KEYSET pagination on the
/// record `id` (`WHERE id > <last>`), never `OFFSET`/`START`.
///
/// The remote DB is live while we page it, so offset pagination is unsafe: a
/// concurrent delete behind the offset shifts every later row up one, so the
/// next `START n` skips a row. A skipped record never lands in the replica (nor
/// in any SSP that bootstraps from it), so a later delete of that record emits
/// no removal delta and clients' live queries go stale until reload. Keyset
/// resumes from the last id seen, immune to shifts behind the cursor. Mirrors
/// `bootstrap_page_query` in apps/ssp/src/lib.rs.
/// `omit` drops opaque columns from the projection — see
/// [`Replica::opaque_fields`]. Every producer of a content hash must emit the
/// identical `OMIT` list or their digests diverge, so the clause is rendered by
/// the shared `ssp_protocol::omit_clause`.
fn keyset_page_query(
    table: &str,
    page_size: usize,
    after_id: Option<&str>,
    omit: &BTreeSet<String>,
) -> String {
    let omit = ssp_protocol::omit_clause(omit);
    match after_id {
        None => format!("SELECT *{omit} FROM {table} ORDER BY id LIMIT {page_size}"),
        Some(id) => {
            let raw = id.strip_prefix(&format!("{table}:")).unwrap_or(id);
            format!("SELECT *{omit} FROM {table} WHERE id > type::record('{table}', '{raw}') ORDER BY id LIMIT {page_size}")
        }
    }
}

/// Build a full SurrealDB thing ID, handling both `"table:id"` and bare `"id"` formats.
/// SurrealDB event triggers send IDs that already include the table prefix (e.g. `"user:abc"`),
/// so we must avoid doubling it into `"user:user:abc"`.
fn build_thing_id(table: &str, id: &str) -> String {
    let prefix = format!("{}:", table);
    if id.starts_with(&prefix) {
        id.to_string()
    } else {
        format!("{}:{}", table, id)
    }
}

/// In-memory representation of `_00_metadata:snapshot` — the persisted
/// integrity-check state restored at startup.
#[derive(Default, Debug, Clone)]
struct SnapshotState {
    seq: u64,
    hashes: BTreeMap<String, String>,
    tables: BTreeSet<String>,
    /// A drain wrote rows past `seq` and never committed: set by
    /// `begin_apply` before the first event of a batch is applied, cleared
    /// by `commit_snapshot_state`. A fresh process that finds it set cannot
    /// fold the recovered WAL backlog (some of it is already in the rows) and
    /// rehashes those tables from content instead.
    applying: bool,
    /// Changefeed cursor the rows are known to include (see
    /// [`Replica::changefeed_vs`]).
    changefeed_vs: u64,
}

/// A table's hash as computed from its content, with the id ranges cut along
/// the way. `ranges` is `None` for a table whose keys cannot be ranged (see
/// `range_hash::string_key`).
#[derive(Debug, Clone)]
pub struct ContentHash {
    pub hash: String,
    pub ranges: Option<RangeHashes>,
}

/// A table's ranges being built a window at a time while the replica keeps
/// draining (see [`Replica::range_build_step`]). Windows below `scanned` are
/// read and receive every later fold; the rest are read when their turn comes,
/// with whatever they hold by then.
struct RangeBuild {
    table: String,
    ranges: RangeHashes,
    scanned: usize,
}

/// What one background build step did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeBuildStep {
    /// A window was read; more remain.
    More,
    /// Every window is read; [`Replica::range_build_finish`] installs it.
    Done,
    /// No build is running (nothing started, or it was abandoned).
    Idle,
    /// The table's keys cannot be ranged; the build was dropped.
    Unsupported,
}

/// The `_00_metadata` record id a table's ranges persist under.
fn ranges_record_key(table: &str) -> String {
    format!("ranges:{table}")
}

/// Fold one applied change into `ranges`, but only into ranges below `limit`
/// (a build's unread windows pick the change up when they are read). `false`
/// when an id is not a plain string key, so the table cannot be ranged.
fn fold_into_ranges(
    ranges: &mut RangeHashes,
    before: Option<(&str, &[u8; 32])>,
    after: Option<(&str, &[u8; 32])>,
    limit: usize,
) -> bool {
    for (side, add) in [(before, false), (after, true)] {
        let Some((id, digest)) = side else { continue };
        let Some(i) = ranges.range_of(id) else { return false };
        if i < limit {
            if add {
                ranges.add_at(i, digest);
            } else {
                ranges.remove_at(i, digest);
            }
        }
    }
    true
}

/// Chunk of replica data for bootstrap
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplicaChunk {
    pub chunk_index: usize,
    pub table: String,
    pub records: Vec<(String, Value)>,
}

/// One table paged out of the remote into a JSONL spool file
/// (see [`Replica::fetch_all_to_spool`]).
pub struct SpooledTable {
    pub table: String,
    pub path: std::path::PathBuf,
    pub records: usize,
}

/// A completed phase-1 spool: every sync table on disk plus the fetched view
/// definitions. Dropping it deletes the spool directory.
pub struct SpoolManifest {
    /// Owns the temp dir so the spool is cleaned up on drop.
    _dir: tempfile::TempDir,
    pub tables: Vec<SpooledTable>,
    pub views: Vec<Value>,
    /// Opaque-field exclusions read from upstream during phase 1, carried across
    /// to phase 2 so [`Replica::load_from_spool`] can adopt them. Phase 1 is a
    /// static fn with no `&self`, so it cannot update the replica's map itself.
    pub opaque_fields: BTreeMap<String, BTreeSet<String>>,
}

/// Persistent replica backed by embedded SurrealDB with RocksDB
pub struct Replica {
    db: Surreal<surrealdb::engine::local::Db>,
    db_path: PathBuf,
    /// Sequence number of the last event applied to this snapshot
    snapshot_seq: u64,
    /// Per-table content hashes at `snapshot_seq`. Persisted in
    /// `_00_metadata:snapshot.hashes`. Populated by `compute_table_hashes`
    /// after a full clone and updated incrementally in `set_snapshot_state`.
    snapshot_hashes: BTreeMap<String, String>,
    /// Tables we have ever written to (via `ingest_all` or `apply`).
    /// SurrealDB's `INFO FOR DB` only lists explicitly `DEFINE`d tables, so
    /// we cannot rediscover schemaless tables from the engine — we track
    /// them ourselves and persist alongside the hashes so a fresh process
    /// can find them.
    known_tables: BTreeSet<String>,
    /// Per-table fields that must never be held in the replica or counted in a
    /// content hash: `DEFINE FIELD`s the CLI stamped `COMMENT 'sp00ky:opaque'`
    /// for `-- @nosync` / `-- @crdt` / `-- @opaque` (see
    /// [`ssp_protocol::OPAQUE_FIELD_COMMENT`]). Refreshed from upstream
    /// `INFO FOR TABLE` on every clone and rediscover.
    ///
    /// Applied in two places, for two different reasons. On the clone
    /// (`page_whole_table`) it stops the value entering the replica at all. On
    /// the hash reads (`snapshot_rows`, `hash_one_table`) it makes the digest
    /// ignore the column even when a row still carries it — which is what lets a
    /// replica cloned *before* this change agree with a freshly bootstrapped SSP
    /// without a forced re-clone.
    ///
    /// Not persisted: it is re-derived from upstream DDL, which is the only
    /// authority. A fresh process with an empty map hashes as it always did
    /// until the first discover repopulates it.
    opaque_fields: BTreeMap<String, BTreeSet<String>>,
    /// Whether `opaque_fields` was ever read from upstream in this process. A
    /// persisted snapshot boots with an empty map that is not knowledge, so the
    /// first read after it is not a change of anyone's opaque set.
    opaque_known: bool,
    /// Tables whose last hash attempt failed. Re-tried on the next
    /// `set_snapshot_state` so a transient error can't strand a table with a
    /// stale (or absent) hash indefinitely. Not persisted: a fresh process
    /// re-derives the truth via `startup_integrity_check`.
    dirty_hashes: BTreeSet<String>,
    /// The persisted `applying` marker as read at open (see
    /// [`SnapshotState::applying`]): the previous process died inside a
    /// drain, after writing rows and before committing. Cleared by the next
    /// commit.
    interrupted_apply: bool,
    /// Highest SurrealDB changefeed versionstamp whose changes are folded into
    /// the rows, persisted with the snapshot state. Together with the WAL's
    /// own stamps it is where the tail resumes after a restart; a re-clone
    /// sets it to just before the clone started.
    changefeed_vs: u64,
    /// Lock-free mirror of `snapshot_seq` for health/metrics readers. A drain
    /// or reclone holds the replica write lock for a long time; probes must
    /// never queue behind it just to read one number.
    seq_cell: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Per-id-range hashes per table (see `ssp_protocol::range_hash`): what a
    /// warm SSP whose table hash differs compares against to find the rows
    /// that differ, instead of listing the whole table. Folded with the table
    /// hash on every applied event, replaced by every from-content hash, and
    /// persisted per table as `_00_metadata:⟨ranges:<table>⟩`. A table without
    /// them (not built yet, or keys that are not plain strings) is listed in
    /// full by the SSP, as before.
    ranges: BTreeMap<String, RangeHashes>,
    /// Tables whose ranges changed (or were dropped) since last persisted.
    ranges_unpersisted: BTreeSet<String>,
    /// Tables whose keys cannot be ranged. Not persisted: a fresh process
    /// finds out again on its first attempt.
    ranges_unsupported: BTreeSet<String>,
    /// The background range build, if one is running. Behind a mutex so a
    /// step runs under a READ guard: an event is applied under the write
    /// guard, so it lands either before a window is read or after it is, never
    /// in between.
    range_build: std::sync::Mutex<Option<RangeBuild>>,
}

impl Replica {
    /// Create a new replica with persistent SurrealDB/RocksDB storage
    pub async fn new(db_path: PathBuf) -> Result<Self> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        // `?sync=never` — the ONE knob the embedded SDK actually forwards to
        // the storage engine (query params become `datastore_*` ConfigMap keys;
        // `rocksdb_*` keys are reachable only from the server binary, which
        // reads `SURREAL_*` env we do not run through).
        //
        // Durability is not the property this store needs. The replica is a
        // rebuildable cache of upstream: a crash mid-clone leaves
        // `snapshot_seq == 0`, and the next start clones again from the source
        // of truth. What it *does* need is to survive a bulk clone, and the
        // default (`sync=every`) makes every commit wait on a grouped WAL
        // fsync — which is what let memtables outrun flushes until RocksDB's
        // WriteBufferManager stalled writes and never recovered: all threads
        // parked, 0% CPU, bootstrap frozen forever with the scheduler stuck in
        // `cloning` and every SSP registration answering 503 (whitepawn,
        // 2026-08-22, 57h of dead sync).
        let path = db_path.to_str().unwrap_or("./data/replica");
        let db = Surreal::new::<RocksDb>((format!("{path}?sync=never"), replica_config()))
            .await
            .with_context(|| format!("Failed to open RocksDB at {:?}", db_path))?;

        db.use_ns("sp00ky").use_db("snapshot").await
            .context("Failed to select namespace/database on replica")?;

        // The engine sizes its RocksDB memory budget from the CGROUP limit, so
        // the container's cap silently sets the stall threshold. Log what we
        // are running under: it is the first number worth knowing when a clone
        // wedges, and it is invisible from the outside.
        info!(
            memory_limit = %cgroup_memory_limit()
                .map(|b| format!("{}MB", b / (1024 * 1024)))
                .unwrap_or_else(|| "unknown".to_string()),
            "Opened replica SurrealDB at {:?} (sync=never)", db_path,
        );

        let SnapshotState {
            seq: snapshot_seq,
            hashes: snapshot_hashes,
            tables: known_tables,
            applying: interrupted_apply,
            changefeed_vs,
        } = Self::read_snapshot_state_from_db(&db).await.unwrap_or_default();
        if snapshot_seq > 0 {
            info!(
                snapshot_seq,
                hash_tables = snapshot_hashes.len(),
                interrupted_apply,
                "Restored snapshot state from metadata"
            );
        }
        let dirty_hashes = Self::stale_format_hashes(&snapshot_hashes);

        let mut replica = Self {
            db,
            db_path,
            snapshot_seq,
            snapshot_hashes,
            known_tables,
            dirty_hashes,
            interrupted_apply,
            changefeed_vs,
            // Re-derived from upstream DDL on the first clone/rediscover; a
            // fresh process starts with no exclusions.
            opaque_fields: BTreeMap::new(),
            opaque_known: false,
            seq_cell: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(snapshot_seq)),
            ranges: BTreeMap::new(),
            ranges_unpersisted: BTreeSet::new(),
            ranges_unsupported: BTreeSet::new(),
            range_build: std::sync::Mutex::new(None),
        };
        replica.load_ranges().await;
        Ok(replica)
    }

    /// Read the persisted ranges back, keeping a table's only when they add up
    /// to its persisted hash and that hash is not about to be recomputed.
    /// Persisted ranges that do not match are left for the next build: they
    /// are from an older commit (a write lost in a crash), and the check is
    /// what makes losing one harmless.
    async fn load_ranges(&mut self) {
        self.ranges.clear();
        self.ranges_unpersisted.clear();
        *self.range_build.get_mut().unwrap() = None;
        let rows = match self
            .db
            .query("SELECT table, hash, starts, hashes, counts FROM _00_metadata WHERE starts != NONE")
            .await
        {
            Ok(mut r) => r
                .take::<surrealdb::types::Value>(0)
                .map(|v| v.into_json_value())
                .unwrap_or(Value::Null),
            Err(e) => {
                debug!(error = %e, "No persisted range hashes");
                return;
            }
        };
        let (mut loaded, mut stale) = (0usize, 0usize);
        for row in rows.as_array().into_iter().flatten() {
            let Ok(wire) = serde_json::from_value::<TableRanges>(row.clone()) else { continue };
            let current = self.snapshot_hashes.get(&wire.table);
            match RangeHashes::from_wire(&wire) {
                Some(r) if current == Some(&wire.hash) && !self.dirty_hashes.contains(&wire.table) => {
                    self.ranges.insert(wire.table, r);
                    loaded += 1;
                }
                _ => stale += 1,
            }
        }
        if loaded > 0 || stale > 0 {
            info!(tables = loaded, stale, "Restored range hashes from metadata");
        }
    }

    /// Write the ranges that changed since they were last persisted, and
    /// delete those of tables that lost theirs. Ranges are an optimisation:
    /// a failure is logged and retried with the next commit.
    async fn persist_ranges(&mut self) {
        if self.ranges_unpersisted.is_empty() {
            return;
        }
        let tables: Vec<String> = self.ranges_unpersisted.iter().cloned().collect();
        let mut surql = String::new();
        let mut binds: Vec<(String, Value)> = Vec::new();
        for (i, table) in tables.iter().enumerate() {
            binds.push((format!("k{i}"), Value::String(ranges_record_key(table))));
            match self.ranges.get(table) {
                Some(r) => {
                    surql.push_str(&format!("UPSERT type::record('_00_metadata', $k{i}) CONTENT $v{i};\n"));
                    binds.push((
                        format!("v{i}"),
                        serde_json::to_value(r.to_wire(table)).unwrap_or(Value::Null),
                    ));
                }
                None => surql.push_str(&format!("DELETE type::record('_00_metadata', $k{i});\n")),
            }
        }
        let mut query = self.db.query(surql);
        for bind in binds {
            query = query.bind(bind);
        }
        match query.await.and_then(|r| r.check()) {
            Ok(_) => {
                for t in &tables {
                    self.ranges_unpersisted.remove(t);
                }
            }
            Err(e) => warn!(error = %e, tables = tables.len(), "Could not persist range hashes; retrying with the next commit"),
        }
    }

    /// `table`'s ranges as `/proxy/ranges` serves them: only while the table's
    /// hash is current and the ranges add up to it, so an SSP never compares
    /// against ranges of another cut than the hash it was handed.
    pub fn table_ranges(&self, table: &str) -> Option<TableRanges> {
        if self.dirty_hashes.contains(table) {
            return None;
        }
        let hash = self.snapshot_hashes.get(table)?;
        let wire = self.ranges.get(table)?.to_wire(table);
        if wire.hash != *hash {
            warn!(table, ranges = %wire.hash, table_hash = %hash, "Range hashes do not add up to the table hash; not serving them");
            return None;
        }
        Some(wire)
    }

    /// Install a table's ranges from a from-content hash (`None`: its keys
    /// cannot be ranged). Any build of the table is dropped: it would only
    /// arrive at the same thing later.
    fn install_ranges(&mut self, table: &str, ranges: Option<RangeHashes>) {
        match ranges {
            Some(r) => {
                self.ranges.insert(table.to_string(), r);
                self.ranges_unsupported.remove(table);
            }
            None => {
                self.ranges.remove(table);
                self.ranges_unsupported.insert(table.to_string());
            }
        }
        self.ranges_unpersisted.insert(table.to_string());
        self.drop_range_build(table);
    }

    /// Abandon a build of `table`: an event reached it that could not be
    /// folded, so its windows no longer describe the rows.
    fn drop_range_build(&mut self, table: &str) {
        let build = self.range_build.get_mut().unwrap();
        if build.as_ref().is_some_and(|b| b.table == table) {
            *build = None;
        }
    }

    /// The table the background builder should range next: one with a
    /// current hash and no ranges, or ranges with one far past its share
    /// (keys that only grow all land in the last range).
    pub fn next_range_build(&self, skip: &BTreeSet<String>) -> Option<String> {
        self.snapshot_hashes
            .keys()
            .filter(|t| !ssp_protocol::table_excluded_from_sync(t))
            .filter(|t| !self.dirty_hashes.contains(*t) && !self.ranges_unsupported.contains(*t))
            .filter(|t| !skip.contains(*t))
            .find(|t| self.ranges.get(*t).map_or(true, RangeHashes::overgrown))
            .cloned()
    }

    /// Start building `table`'s ranges: cut the boundaries (one linear scan
    /// that returns only every `RANGE_ROWS`th key) with no window read yet,
    /// so nothing folds into it until [`Self::range_build_step`] reads one.
    pub async fn range_build_begin(&self, table: &str) -> Result<RangeBuildStep> {
        let boundaries = Self::range_boundaries_on(&self.db, table).await?;
        Ok(self.range_build_start(table, boundaries))
    }

    /// The replica's database handle, for a read that needs no consistency
    /// with the replica lock (the range boundaries: any increasing keys cut a
    /// table correctly, so they can be read without holding up a drain).
    pub fn db_handle(&self) -> Surreal<surrealdb::engine::local::Db> {
        self.db.clone()
    }

    /// Install a build over boundaries [`Self::range_boundaries_on`] cut
    /// (`None`: the keys cannot be ranged). Skipped when the table's hash is
    /// no longer current: the from-content rehash it waits for cuts ranges.
    pub fn range_build_start(&self, table: &str, boundaries: Option<RangeHashes>) -> RangeBuildStep {
        let Some(ranges) = boundaries else {
            return RangeBuildStep::Unsupported;
        };
        if !self.snapshot_hashes.contains_key(table) || self.dirty_hashes.contains(table) {
            return RangeBuildStep::Idle;
        }
        *self.range_build.lock().unwrap() = Some(RangeBuild {
            table: table.to_string(),
            ranges,
            scanned: 0,
        });
        RangeBuildStep::More
    }

    /// Read the build's next window. Under a READ guard, which is what makes
    /// it atomic against `apply` (see `range_build`).
    pub async fn range_build_step(&self) -> Result<RangeBuildStep> {
        let (table, mut ranges, i) = {
            let build = self.range_build.lock().unwrap();
            let Some(b) = build.as_ref() else { return Ok(RangeBuildStep::Idle) };
            if b.scanned == b.ranges.len() {
                return Ok(RangeBuildStep::Done);
            }
            (b.table.clone(), b.ranges.clone(), b.scanned)
        };
        let in_window = self.scan_window(&table, &mut ranges, i).await?;
        let mut build = self.range_build.lock().unwrap();
        let Some(b) = build.as_mut().filter(|b| b.table == table && b.scanned == i) else {
            return Ok(RangeBuildStep::Idle);
        };
        if !in_window {
            *build = None;
            return Ok(RangeBuildStep::Unsupported);
        }
        b.ranges = ranges;
        b.scanned += 1;
        Ok(if b.scanned == b.ranges.len() { RangeBuildStep::Done } else { RangeBuildStep::More })
    }

    /// Drop the running build, after a step failed.
    pub fn range_build_abandon(&self) {
        *self.range_build.lock().unwrap() = None;
    }

    /// Install a finished build when it adds up to the table's current hash,
    /// and persist it. A build that does not add up is dropped and `false`
    /// returned: either the cached table hash disagrees with the rows (the
    /// next SSP dispute or rehash settles that) or something reached the rows
    /// without a fold.
    pub async fn range_build_finish(&mut self) -> bool {
        let Some(b) = self.range_build.get_mut().unwrap().take() else { return false };
        if b.scanned < b.ranges.len() {
            return false;
        }
        let total = snapshot_hash::xor_acc_to_hex(&b.ranges.total());
        let current = self.snapshot_hashes.get(&b.table);
        if self.dirty_hashes.contains(&b.table) || current != Some(&total) {
            warn!(
                table = %b.table,
                built = %total,
                table_hash = %current.map(String::as_str).unwrap_or("<none>"),
                "Built range hashes do not add up to the table hash; dropping them"
            );
            return false;
        }
        info!(table = %b.table, ranges = b.ranges.len(), rows = b.ranges.rows(), "Range hashes built");
        self.ranges.insert(b.table.clone(), b.ranges);
        self.ranges_unpersisted.insert(b.table);
        self.persist_ranges().await;
        true
    }

    /// Mark a table's keys as unrangeable (a build found a key that is not a
    /// plain string), so the builder stops trying.
    pub fn mark_ranges_unsupported(&mut self, table: &str) {
        self.ranges_unsupported.insert(table.to_string());
        if self.ranges.remove(table).is_some() {
            self.ranges_unpersisted.insert(table.to_string());
        }
        self.drop_range_build(table);
    }

    /// The ranges `table` is cut into, with empty accumulators: the key every
    /// `RANGE_ROWS` rows, from one scan that ships only those keys. `None`
    /// when a key is not a plain string. A table that does not exist is one
    /// empty range.
    pub async fn range_boundaries_on(
        db: &Surreal<surrealdb::engine::local::Db>,
        table: &str,
    ) -> Result<Option<RangeHashes>> {
        let ids = match db.query(range_hash::boundary_query(table)).await {
            Ok(mut response) => match response.take::<surrealdb::types::Value>(0) {
                Ok(v) => v.into_json_value(),
                Err(e) if is_missing_error(&e) => Value::Array(Vec::new()),
                Err(e) => {
                    return Err(anyhow::Error::from(e)
                        .context(format!("range boundaries: take(0) failed for '{table}'")))
                }
            },
            Err(e) if is_missing_error(&e) => Value::Array(Vec::new()),
            Err(e) => {
                return Err(anyhow::Error::from(e)
                    .context(format!("range boundaries: query failed for '{table}'")))
            }
        };
        let prefix = format!("{table}:");
        let mut keys = Vec::new();
        for id in ids.as_array().into_iter().flatten() {
            let Some(id) = id.as_str() else { return Ok(None) };
            match range_hash::string_key(id.strip_prefix(&prefix).unwrap_or(id)) {
                Some(key) => keys.push(key.to_string()),
                None => return Ok(None),
            }
        }
        Ok(RangeHashes::from_boundaries(keys))
    }

    /// Read range `i` of `table` (a key-range scan) and digest its rows into
    /// `ranges`. `false` when a row read there does not belong there by key
    /// order, or is not a plain string key: SurrealDB orders those keys
    /// differently, so the table cannot be ranged.
    async fn scan_window(&self, table: &str, ranges: &mut RangeHashes, i: usize) -> Result<bool> {
        let omit = ssp_protocol::omit_clause(self.omit_for(table));
        let query = format!("SELECT *{omit} FROM {}", ranges.target(table, i));
        let rows = match self.db.query(&query).await {
            Ok(mut response) => match response.take::<surrealdb::types::Value>(0) {
                Ok(v) => v.into_json_value(),
                Err(e) if is_missing_error(&e) => Value::Array(Vec::new()),
                Err(e) => return Err(anyhow::Error::from(e).context(format!("scan_window: take(0) failed for [{query}]"))),
            },
            Err(e) if is_missing_error(&e) => Value::Array(Vec::new()),
            Err(e) => return Err(anyhow::Error::from(e).context(format!("scan_window: [{query}] failed"))),
        };
        let prefix = format!("{table}:");
        for row in rows.as_array().into_iter().flatten() {
            let Some(id) = row.get("id").and_then(Value::as_str) else { continue };
            let raw_id = id.strip_prefix(&prefix).unwrap_or(id);
            if ranges.range_of(raw_id) != Some(i) {
                return Ok(false);
            }
            ranges.add_at(i, &snapshot_hash::record_digest(raw_id, row));
        }
        Ok(true)
    }

    /// Get current snapshot sequence number
    pub fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
    }

    /// The persisted changefeed cursor (see the field doc).
    pub fn changefeed_vs(&self) -> u64 {
        self.changefeed_vs
    }

    /// Record the changefeed position the rows reflect; persisted by the next
    /// snapshot-state commit. Never moves backwards except through `reset`.
    pub fn set_changefeed_vs(&mut self, versionstamp: u64) {
        if versionstamp > self.changefeed_vs {
            self.changefeed_vs = versionstamp;
        }
    }

    /// A row as the replica holds it (opaque fields omitted), or `None`. The
    /// changefeed carries only the id of a deleted row, and the SSP's delete
    /// path wants the row's `owner` to drop its per-user edges, so the tail
    /// reads the before-image here before applying the delete.
    pub async fn row(&self, table: &str, id: &str) -> Result<Option<Value>> {
        let thing_id = build_thing_id(table, id);
        Ok(self.read_row_for_hash(table, &thing_id).await?.map(|(_, row)| row))
    }

    /// Shared lock-free view of `snapshot_seq` (see `seq_cell`). Clone once at
    /// wiring time; reading it never touches the replica lock.
    pub fn snapshot_seq_cell(&self) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
        std::sync::Arc::clone(&self.seq_cell)
    }

    /// Keep the atomic mirror in step with `snapshot_seq`. Call from every
    /// site that assigns the field.
    fn publish_seq(&self) {
        self.seq_cell
            .store(self.snapshot_seq, std::sync::atomic::Ordering::Relaxed);
    }

    /// Per-table content hashes at the current `snapshot_seq`.
    pub fn snapshot_hashes(&self) -> &BTreeMap<String, String> {
        &self.snapshot_hashes
    }

    /// Tables whose hash must be recomputed from content on the next drain:
    /// a failed hash, an event that could not be folded, or a table that has
    /// no accumulator yet.
    pub fn dirty_tables(&self) -> &BTreeSet<String> {
        &self.dirty_hashes
    }

    /// Flag tables for a from-content rehash at the next drain. Used at boot
    /// for the tables a recovered WAL backlog touches, whose events may have
    /// been applied before the crash and cannot be folded a second time.
    pub fn mark_tables_dirty(&mut self, tables: impl IntoIterator<Item = String>) {
        for table in tables {
            if !ssp_protocol::table_excluded_from_sync(&table) {
                self.dirty_hashes.insert(table);
            }
        }
    }

    /// Persisted hashes that predate the incremental `x3:` format (or are
    /// otherwise unparsable) cannot be folded into, so they are rehashed from
    /// content once. Happens exactly once per replica on the upgrade to the
    /// incremental scheme, at the first drain, and never again.
    fn stale_format_hashes(hashes: &BTreeMap<String, String>) -> BTreeSet<String> {
        let stale: BTreeSet<String> = hashes
            .iter()
            .filter(|(_, h)| snapshot_hash::xor_acc_from_hex(h).is_none())
            .map(|(t, _)| t.clone())
            .collect();
        if !stale.is_empty() {
            info!(
                tables = stale.len(),
                "Persisted table hashes predate the incremental format; rehashing them from content on the next drain"
            );
        }
        stale
    }

    /// All tables this replica has ever written to.
    pub fn known_tables(&self) -> &BTreeSet<String> {
        &self.known_tables
    }

    /// Tables this replica holds state for: every table it has written to or
    /// holds a hash for. What a schema reconcile compares upstream against.
    pub fn held_tables(&self) -> BTreeSet<String> {
        self.known_tables
            .iter()
            .chain(self.snapshot_hashes.keys())
            .cloned()
            .collect()
    }

    /// Record that the replica holds all of `table`, also when that is no rows
    /// at all. A table added upstream while empty is never written to, so it
    /// had no hash and no `known_tables` entry, and every boot reported it as
    /// newly added again (whitepawn's `stream_video`). Persisted with the next
    /// snapshot advance.
    pub fn hold_table(&mut self, table: &str) {
        self.known_tables.insert(table.to_string());
    }

    /// Opaque fields per table, as the replica currently omits them.
    pub fn opaque_fields(&self) -> &BTreeMap<String, BTreeSet<String>> {
        &self.opaque_fields
    }

    /// Bring the replica in line with upstream's schema (see `crate::schema`).
    ///
    /// `opaque`, when given, replaces the opaque-field sets. A table whose set
    /// moved is rehashed from content at the next drain: its persisted `x3`
    /// accumulator was folded with the old set, and every SSP that bootstraps
    /// omits by the new one.
    ///
    /// `removed` are tables upstream no longer syncs (`-- @nosync` added, or
    /// dropped outright), as confirmed by the schema tracker. Each is dropped
    /// here: its rows (so `/proxy` stops serving them and a later re-add starts
    /// clean), its known-table entry and its hash, and the trimmed lists are
    /// persisted. Their hashes used to be handed to every bootstrapping SSP,
    /// which leaves such a table out of its own load and so disputed it on
    /// every attempt: whitepawn after `analysis` went @nosync logged "Bootstrap
    /// integrity mismatch table=analysis expected=x3:f731… actual=x3:000…" per
    /// bootstrap until the breaker re-cloned the whole replica. Returns the
    /// tables dropped, sorted.
    pub async fn reconcile_schema(
        &mut self,
        removed: &[String],
        opaque: Option<BTreeMap<String, BTreeSet<String>>>,
    ) -> Result<Vec<String>> {
        if let Some(opaque) = opaque {
            if self.opaque_known {
                let moved: BTreeSet<String> = self
                    .opaque_fields
                    .keys()
                    .chain(opaque.keys())
                    .filter(|t| self.opaque_fields.get(*t) != opaque.get(*t))
                    .filter(|t| self.snapshot_hashes.contains_key(*t))
                    .cloned()
                    .collect();
                if !moved.is_empty() {
                    info!(tables = ?moved, "Opaque fields changed upstream; rehashing those tables from content at the next drain");
                    for t in &moved {
                        self.drop_range_build(t);
                    }
                    self.dirty_hashes.extend(moved);
                }
            }
            self.opaque_fields = opaque;
            self.opaque_known = true;
        }

        let held = self.held_tables();
        let gone: Vec<String> = removed
            .iter()
            .filter(|t| held.contains(*t))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if gone.is_empty() {
            return Ok(gone);
        }
        for t in &gone {
            self.db
                .query(format!("REMOVE TABLE IF EXISTS {t}"))
                .await
                .and_then(|r| r.check())
                .with_context(|| format!("Drop table {t} from the replica"))?;
            self.known_tables.remove(t);
            self.snapshot_hashes.remove(t);
            self.dirty_hashes.remove(t);
            self.ranges.remove(t);
            self.ranges_unsupported.remove(t);
            self.ranges_unpersisted.insert(t.clone());
            self.drop_range_build(t);
        }
        let seq = self.snapshot_seq;
        self.commit_snapshot_state(seq, BTreeMap::new(), BTreeSet::new())
            .await
            .context("Persist snapshot state after dropping unsynced tables")?;
        Ok(gone)
    }

    /// Set snapshot sequence number AND advance the per-table hashes for the
    /// supplied tables. Pass `None` for `touched_tables` after a full clone
    /// to recompute every known table; pass `Some(set)` from the drain loop
    /// to only rehash the tables a batch touched.
    ///
    /// Convenience wrapper over [`Self::compute_hashes_for`] +
    /// [`Self::commit_snapshot_state`] — hashing happens under whatever lock
    /// the caller already holds. Callers that can afford it (the drain loop)
    /// should hash under a READ guard and only take the write guard for the
    /// commit: hashing pages whole tables out of RocksDB, and holding the
    /// write lock for that starves /proxy and every bootstrap.
    pub async fn set_snapshot_state(
        &mut self,
        seq: u64,
        touched_tables: Option<&BTreeSet<String>>,
    ) -> Result<()> {
        let (hashed, failed) = self.compute_hashes_for(touched_tables).await;
        self.commit_snapshot_state(seq, hashed, failed).await
    }

    /// Hash the supplied tables (`None` = every known table) plus anything a
    /// previous attempt failed to hash. Pure read — safe under a read guard.
    /// Returns the successfully computed hashes and the set that failed.
    ///
    /// The dirty-hash retry matters: a single transient error would otherwise
    /// drop that table out of the hash map until the next full recompute — and
    /// a missing entry reads as the *empty-table* hash in `diff_table_hashes`,
    /// so every bootstrap of a populated table mismatched from then on.
    pub async fn compute_hashes_for(
        &self,
        touched_tables: Option<&BTreeSet<String>>,
    ) -> (BTreeMap<String, ContentHash>, BTreeSet<String>) {
        // `Some(tables)`: hash exactly those from content (the admin rehash,
        // the bootstrap-verify dispute) plus whatever is dirty. The drain
        // passes an EMPTY set: its tables were folded event by event in
        // `apply`, so only the dirty ones need content. `None` (after a
        // clone) recomputes every known table.
        let mut to_hash: BTreeSet<String> = match touched_tables {
            Some(t) => t.clone(),
            None => self.known_tables.clone(),
        };
        to_hash.extend(self.dirty_hashes.iter().cloned());

        let mut hashed = BTreeMap::new();
        let mut failed = BTreeSet::new();
        for table in &to_hash {
            match self.hash_one_table(table).await {
                Ok(hash) => {
                    hashed.insert(table.clone(), hash);
                }
                Err(e) => {
                    // Don't fail the snapshot advance just because one table
                    // can't be hashed (e.g. schema race). Keep the previous
                    // value (a stale hash is recoverable; a missing one is
                    // indistinguishable from "empty table") and mark it for
                    // retry on the next advance.
                    warn!(table = %table, error = %e, "Failed to hash table — keeping previous hash, will retry");
                    failed.insert(table.clone());
                }
            }
        }
        (hashed, failed)
    }

    /// Commit a snapshot advance: fold in hashes from `compute_hashes_for`,
    /// mark failures for retry, and persist. Milliseconds under a write guard.
    pub async fn commit_snapshot_state(
        &mut self,
        seq: u64,
        hashed: BTreeMap<String, ContentHash>,
        failed: BTreeSet<String>,
    ) -> Result<()> {
        self.snapshot_seq = seq;
        self.publish_seq();

        for (table, content) in hashed {
            self.dirty_hashes.remove(&table);
            self.install_ranges(&table, content.ranges);
            self.snapshot_hashes.insert(table, content.hash);
        }
        for table in failed {
            self.dirty_hashes.insert(table);
        }

        let hashes_value = serde_json::to_value(&self.snapshot_hashes)
            .context("Serialize snapshot_hashes failed")?;
        let tables_value = serde_json::to_value(
            self.known_tables.iter().cloned().collect::<Vec<_>>(),
        )
        .context("Serialize known_tables failed")?;

        // `applying = false`: the batch whose rows were written under the
        // marker is now described by `seq` + `hashes`.
        self.db
            .query("UPSERT _00_metadata:snapshot SET seq = $seq, hashes = $hashes, tables = $tables, applying = false, changefeed_vs = $changefeed_vs")
            .bind(("seq", seq))
            .bind(("hashes", hashes_value))
            .bind(("tables", tables_value))
            .bind(("changefeed_vs", self.changefeed_vs))
            .await
            .context("Failed to persist snapshot state")?;
        self.interrupted_apply = false;
        // After the hashes, so ranges that made it to disk never describe a
        // newer cut than the hash they are checked against at load.
        self.persist_ranges().await;
        Ok(())
    }

    /// Persist the `applying` marker before the first event of a drain batch
    /// touches the rows. Until `commit_snapshot_state` clears it, the rows
    /// may be ahead of the persisted `seq`/`hashes`; a process that opens the
    /// replica in that state (see [`Replica::interrupted_apply`]) must rehash
    /// the WAL backlog's tables from content instead of folding them.
    ///
    /// Ordering holds under RocksDB's `sync=never`: the marker is a write
    /// sequenced before the row writes, so a lost suffix of the RocksDB WAL
    /// either drops the marker together with every row written after it, or
    /// keeps the marker (a harmless extra rehash).
    pub async fn begin_apply(&mut self) -> Result<()> {
        self.db
            .query("UPSERT _00_metadata:snapshot SET applying = true")
            .await
            .context("Failed to persist the applying marker")?;
        Ok(())
    }

    /// Whether the persisted state was opened with the `applying` marker set:
    /// the previous process died between writing a batch's rows and
    /// committing its `seq` + `hashes`. Stays true until the next commit.
    pub fn interrupted_apply(&self) -> bool {
        self.interrupted_apply
    }

    /// Backward-compatible single-field setter used by `drain_and_apply`
    /// when called without a touched-tables hint. Updates the seq only and
    /// leaves cached hashes alone — callers that want the hashes refreshed
    /// must use `set_snapshot_state`.
    pub async fn set_snapshot_seq(&mut self, seq: u64) -> Result<()> {
        self.set_snapshot_state(seq, Some(&BTreeSet::new())).await
    }

    /// Compute hashes for every known table. Returns the new map without
    /// mutating `self.snapshot_hashes` — caller decides when to commit.
    pub async fn compute_table_hashes(&self) -> Result<BTreeMap<String, String>> {
        let mut out = BTreeMap::new();
        for table in &self.known_tables {
            match self.hash_one_table(table).await {
                Ok(h) => {
                    out.insert(table.clone(), h.hash);
                }
                Err(e) => {
                    warn!(table = %table, error = %e, "Failed to hash table during recompute");
                }
            }
        }
        Ok(out)
    }

    /// Opaque fields to `OMIT` when scanning `table`. Empty for a table with no
    /// annotated fields, or before the first discovery has run.
    pub(crate) fn omit_for(&self, table: &str) -> &BTreeSet<String> {
        static EMPTY: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
        self.opaque_fields
            .get(table)
            .unwrap_or_else(|| EMPTY.get_or_init(BTreeSet::new))
    }

    /// Read all rows of `table` from the replica as `(raw_id, value)` pairs, the
    /// id stripped of its `table:` prefix to match the SSP circuit's raw keys.
    /// Read-only; used both by `hash_one_table` and to seed the scheduler's
    /// catch-up projection when verifying a rejoining SSP.
    pub async fn snapshot_rows(&self, table: &str) -> Result<Vec<(String, Value)>> {
        let omit = ssp_protocol::omit_clause(self.omit_for(table));
        let mut response = self
            .db
            .query(format!("SELECT *{} FROM {}", omit, table))
            .await
            .with_context(|| format!("snapshot_rows: SELECT * FROM {} failed", table))?;
        let sdk_val: surrealdb::types::Value = response
            .take(0)
            .with_context(|| format!("snapshot_rows: take(0) failed for '{}'", table))?;
        let rows: Vec<Value> = match sdk_val.into_json_value() {
            Value::Array(arr) => arr,
            _ => Vec::new(),
        };

        let pairs: Vec<(String, Value)> = rows
            .into_iter()
            .filter_map(|mut row| {
                let id = row.as_object_mut()
                    .and_then(|obj| obj.get("id").and_then(|v| v.as_str()).map(String::from))?;
                let raw_id = id.strip_prefix(&format!("{}:", table)).unwrap_or(&id).to_string();
                Some((raw_id, row))
            })
            .collect();

        Ok(pairs)
    }

    /// Hash one table from its content, cutting its id ranges on the way.
    ///
    /// Two passes, both linear: the range boundaries (one scan that returns
    /// only every `RANGE_ROWS`th key), then one key-range scan per range. The
    /// keyset pager this replaced (`WHERE id > $last ORDER BY id LIMIT n`) is
    /// a table scan from the first key on SurrealDB 3.1, so every page cost
    /// more than the one before it and a table cost the square of its pages.
    /// A table whose keys cannot be ranged still goes through that pager.
    async fn hash_one_table(&self, table: &str) -> Result<ContentHash> {
        if let Some(mut ranges) = Self::range_boundaries_on(&self.db, table).await? {
            let mut in_order = true;
            for i in 0..ranges.len() {
                if !self.scan_window(table, &mut ranges, i).await? {
                    in_order = false;
                    break;
                }
            }
            if in_order {
                return Ok(ContentHash {
                    hash: snapshot_hash::xor_acc_to_hex(&ranges.total()),
                    ranges: Some(ranges),
                });
            }
        }
        Ok(ContentHash {
            hash: self.hash_one_table_paged(table).await?,
            ranges: None,
        })
    }

    /// Hash one table by paging it out of the replica instead of a single
    /// `SELECT * FROM table`. The single-shot form materialized the WHOLE
    /// table three times over (SDK `Value` → JSON `Value` → pairs vec) on
    /// every hash recompute; on active tenants that transient became the
    /// scheduler's dominant anon-heap high-water mark (allocators don't
    /// return freed pages), pinning the container at its cgroup cap. Paging
    /// via keyset + `TableHasher` keeps only one page of parsed rows plus
    /// compact canonical bytes per record in memory, with a bit-identical
    /// digest (see `TableHasher` in ssp-protocol).
    ///
    /// A table that was empty at clone time never gets created in the
    /// schemaless replica, and SurrealDB v3 errors on SELECT from an
    /// undefined table — treat that as an empty table instead of failing so
    /// its hash still lands in the snapshot map (the old path warned
    /// "Failed to hash table" forever and never hashed such tables).
    async fn hash_one_table_paged(&self, table: &str) -> Result<String> {
        const HASH_PAGE_SIZE: usize = 500;
        // The `x3:` set-hash: what `apply` folds into per event. Computing it
        // from content is the exception (clone, upgrade, a fold that failed),
        // not the every-drain rule it used to be.
        let mut acc = snapshot_hash::xor_empty();
        let mut after_id: Option<String> = None;
        loop {
            let query =
                keyset_page_query(table, HASH_PAGE_SIZE, after_id.as_deref(), self.omit_for(table));
            let mut response = match self.db.query(&query).await {
                Ok(r) => r,
                Err(e) if is_missing_error(&e) => break,
                Err(e) => {
                    return Err(anyhow::Error::from(e)
                        .context(format!("hash_one_table: page query failed for '{}'", table)))
                }
            };
            let sdk_val: surrealdb::types::Value = match response.take(0) {
                Ok(v) => v,
                Err(e) if is_missing_error(&e) => break,
                Err(e) => {
                    return Err(anyhow::Error::from(e).context(format!(
                        "hash_one_table: take(0) failed for '{}' (after_id={:?})",
                        table, after_id,
                    )))
                }
            };
            let rows: Vec<Value> = match sdk_val.into_json_value() {
                Value::Array(arr) => arr,
                _ => Vec::new(),
            };
            let n = rows.len();
            // Advance the cursor to this page's last id (page is ORDER BY id)
            // BEFORE consuming `rows` — mirrors the bootstrap pager above.
            let next_after = rows
                .last()
                .and_then(|row| row.get("id"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            for row in rows {
                let Some(id) = row
                    .as_object()
                    .and_then(|obj| obj.get("id"))
                    .and_then(|v| v.as_str())
                    .map(String::from)
                else {
                    continue;
                };
                let raw_id = id.strip_prefix(&format!("{}:", table)).unwrap_or(&id).to_string();
                snapshot_hash::xor_in(&mut acc, &raw_id, &row);
            }
            if n < HASH_PAGE_SIZE {
                break;
            }
            // No usable id to resume from → stop rather than loop forever.
            match next_after {
                Some(id) => after_id = Some(id),
                None => break,
            }
        }
        Ok(snapshot_hash::xor_acc_to_hex(&acc))
    }

    /// Project one replica row into the `(raw_id, value)` pair the table hash
    /// is computed over: opaque fields dropped (what `SELECT * OMIT` does on
    /// the paged path) and the id stripped of its table prefix.
    fn hash_pair_for(&self, table: &str, mut row: Value) -> Option<(String, Value)> {
        let obj = row.as_object_mut()?;
        for field in self.omit_for(table).iter() {
            obj.remove(field);
        }
        let id = obj.get("id")?.as_str()?.to_string();
        let raw_id = id.strip_prefix(&format!("{}:", table)).unwrap_or(&id).to_string();
        Some((raw_id, row))
    }

    /// The first row of a statement's result as JSON (`RETURN AFTER` /
    /// `RETURN BEFORE` / a single-record SELECT), or `None` when it produced
    /// nothing.
    fn first_row_json(v: surrealdb::types::Value) -> Option<Value> {
        match v.into_json_value() {
            Value::Array(arr) => arr.into_iter().next(),
            Value::Null => None,
            other => Some(other),
        }
    }

    /// The current replica row for `thing_id`, projected for hashing. `None`
    /// when the record (or its table) does not exist.
    async fn read_row_for_hash(&self, table: &str, thing_id: &str) -> Result<Option<(String, Value)>> {
        let omit = ssp_protocol::omit_clause(self.omit_for(table));
        let mut response = match self.db.query(format!("SELECT *{} FROM {}", omit, thing_id)).await {
            Ok(r) => r,
            Err(e) if is_missing_error(&e) => return Ok(None),
            Err(e) => {
                return Err(anyhow::Error::from(e)
                    .context(format!("read_row_for_hash: SELECT {} failed", thing_id)))
            }
        };
        let v: surrealdb::types::Value = match response.take(0) {
            Ok(v) => v,
            Err(e) if is_missing_error(&e) => return Ok(None),
            Err(e) => {
                return Err(anyhow::Error::from(e)
                    .context(format!("read_row_for_hash: take(0) failed for {}", thing_id)))
            }
        };
        Ok(Self::first_row_json(v).and_then(|row| self.hash_pair_for(table, row)))
    }

    /// The `_00_rv` the replica holds for a record, `None` when the row is
    /// absent or unversioned.
    async fn stored_rv(&self, thing_id: &str) -> Result<Option<i64>> {
        let mut response = match self
            .db
            .query(format!("SELECT VALUE _00_rv FROM ONLY {}", thing_id))
            .await
        {
            Ok(r) => r,
            Err(e) if is_missing_error(&e) => return Ok(None),
            Err(e) => return Err(anyhow::Error::from(e).context("stored_rv: SELECT failed")),
        };
        let v: surrealdb::types::Value = match response.take(0) {
            Ok(v) => v,
            Err(e) if is_missing_error(&e) => return Ok(None),
            Err(e) => return Err(anyhow::Error::from(e).context("stored_rv: take(0) failed")),
        };
        Ok(v.into_json_value().as_i64())
    }

    /// Fold one applied event into `table`'s accumulator: the before-image
    /// out, the after-image in. A table whose accumulator cannot be parsed
    /// is marked dirty instead, so the next drain rehashes it from content.
    fn fold_hash_delta(
        &mut self,
        table: &str,
        before: Option<&(String, Value)>,
        after: Option<&(String, Value)>,
    ) {
        let Some(current) = self.snapshot_hashes.get(table) else { return };
        let Some(mut acc) = snapshot_hash::xor_acc_from_hex(current) else {
            self.dirty_hashes.insert(table.to_string());
            self.drop_range_build(table);
            return;
        };
        let before = before.map(|(id, value)| (id.as_str(), snapshot_hash::record_digest(id, value)));
        let after = after.map(|(id, value)| (id.as_str(), snapshot_hash::record_digest(id, value)));
        for (_, digest) in before.iter().chain(after.iter()) {
            snapshot_hash::xor_digest(&mut acc, digest);
        }
        self.snapshot_hashes
            .insert(table.to_string(), snapshot_hash::xor_acc_to_hex(&acc));

        // The ranges, and a build's windows already read, take the same
        // change. A key that cannot be ranged ends both for this table.
        let before = before.as_ref().map(|(id, d)| (*id, d));
        let after = after.as_ref().map(|(id, d)| (*id, d));
        if let Some(ranges) = self.ranges.get_mut(table) {
            if !fold_into_ranges(ranges, before, after, usize::MAX) {
                self.mark_ranges_unsupported(table);
                return;
            }
            self.ranges_unpersisted.insert(table.to_string());
        }
        let build = self.range_build.get_mut().unwrap();
        if let Some(b) = build.as_mut().filter(|b| b.table == table) {
            if !fold_into_ranges(&mut b.ranges, before, after, b.scanned) {
                self.mark_ranges_unsupported(table);
            }
        }
    }

    /// Read combined snapshot state (seq + hashes + tables) from metadata.
    async fn read_snapshot_state_from_db(
        db: &Surreal<surrealdb::engine::local::Db>,
    ) -> Result<SnapshotState> {
        let mut response = db
            .query("SELECT seq, hashes, tables, applying, changefeed_vs FROM _00_metadata:snapshot")
            .await
            .context("Failed to query snapshot metadata")?;

        let rows: Vec<Value> = response.take(0).unwrap_or_default();
        let row = match rows.first() {
            Some(r) => r,
            None => return Ok(SnapshotState::default()),
        };

        let seq = row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
        let applying = row.get("applying").and_then(|v| v.as_bool()).unwrap_or(false);
        let changefeed_vs = row.get("changefeed_vs").and_then(|v| v.as_u64()).unwrap_or(0);
        let hashes: BTreeMap<String, String> = row
            .get("hashes")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let tables: Vec<String> = row
            .get("tables")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let mut known: BTreeSet<String> = tables.into_iter().collect();
        for k in hashes.keys() {
            known.insert(k.clone());
        }
        Ok(SnapshotState {
            seq,
            hashes,
            tables: known,
            applying,
            changefeed_vs,
        })
    }

    /// Full initial load from a remote SurrealDB instance.
    /// Repopulates `known_tables` from the upstream INFO FOR DB.
    pub async fn ingest_all<C>(&mut self, remote_db: &surrealdb::Surreal<C>) -> Result<()>
    where
        C: surrealdb::Connection,
    {
        let total_start = std::time::Instant::now();

        let tables = Self::discover_sync_tables(remote_db).await?;
        // Refresh field-level exclusions before the clone reads a single row —
        // the whole point is that these values never enter the replica.
        self.opaque_fields = Self::discover_opaque_fields(remote_db, &tables).await;
        self.opaque_known = true;

        // Track which tables we are about to populate so the integrity-check
        // path can rediscover them after a restart (INFO FOR DB on the
        // schemaless replica won't list them).
        for t in &tables {
            self.known_tables.insert(t.clone());
        }

        info!(
            table_count = tables.len(),
            "Snapshot clone starting: {} tables to ingest [{}]",
            tables.len(),
            tables.join(", "),
        );

        let mut total_records: usize = 0;
        for (idx, table_name) in tables.iter().enumerate() {
            let table_start = std::time::Instant::now();
            info!(
                table = %table_name,
                progress = format!("{}/{}", idx + 1, tables.len()),
                "[{}/{}] Ingesting table '{}' from remote...",
                idx + 1,
                tables.len(),
                table_name,
            );

            // Page in, insert, drop — never hold a whole table. `omit` is
            // cloned so the sink can borrow `self` for the insert without the
            // pager still holding a borrow of `self.opaque_fields`.
            let omit = self.omit_for(table_name).clone();
            let insert_ms = std::sync::atomic::AtomicU64::new(0);
            // Shared reborrow: `bulk_insert` only needs `&Replica`, and the
            // borrow ends with the paging call, so the `&mut self` methods
            // after this loop are unaffected.
            let this: &Self = self;
            let count = Self::page_table(remote_db, table_name, &omit, |page| {
                let insert_ms = &insert_ms;
                async move {
                    let started = std::time::Instant::now();
                    this.bulk_insert(table_name, page).await?;
                    insert_ms.fetch_add(
                        started.elapsed().as_millis() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    Ok(())
                }
            })
            .await?;

            let elapsed_ms = table_start.elapsed().as_millis() as u64;
            let insert_ms = insert_ms.load(std::sync::atomic::Ordering::Relaxed);
            total_records += count;
            info!(
                table = %table_name,
                records = count,
                fetch_ms = elapsed_ms.saturating_sub(insert_ms),
                insert_ms = insert_ms,
                "[{}/{}] Done '{}' — {} records (fetch {}ms, insert {}ms)",
                idx + 1,
                tables.len(),
                table_name,
                count,
                elapsed_ms.saturating_sub(insert_ms),
                insert_ms,
            );
        }

        let views_start = std::time::Instant::now();
        let views = Self::fetch_remote_views(remote_db).await?;
        let view_count = self.insert_views(views).await?;
        info!(
            views = view_count,
            elapsed_ms = views_start.elapsed().as_millis() as u64,
            "Copied {} view definitions",
            view_count,
        );

        info!(
            tables = tables.len(),
            records = total_records,
            views = view_count,
            elapsed_ms = total_start.elapsed().as_millis() as u64,
            "Snapshot clone summary: {} tables, {} records, {} views in {}ms",
            tables.len(),
            total_records,
            view_count,
            total_start.elapsed().as_millis(),
        );

        Ok(())
    }

    /// Discover the sync-relevant tables on the remote. Tolerates "database
    /// doesn't exist yet" by treating it as an empty table list — the
    /// scheduler may legitimately be pointed at a fresh SurrealDB where
    /// neither the user schema nor Phase 4a has run.
    ///
    /// If the upstream has no `tables` block, ingest nothing. (Previously a
    /// hardcoded `[thread, job, user]` fallback lived here — actively wrong
    /// for any project not happening to use those exact names.)
    ///
    /// Tables marked `-- @nosync` carry a `COMMENT 'sp00ky:nosync'` marker in
    /// their `DEFINE TABLE` string (baked in by the CLI). They are excluded
    /// from the snapshot entirely — never cloned, never added to
    /// `known_tables`, never hashed. They remain in the main DB (still
    /// backed up); they just don't participate in sync.
    ///
    /// Field-level exclusions are discovered separately, by
    /// [`Self::discover_opaque_fields`] — they need `INFO FOR TABLE`, which
    /// `INFO FOR DB` does not include.
    pub async fn discover_sync_tables<C>(remote_db: &surrealdb::Surreal<C>) -> Result<Vec<String>>
    where
        C: surrealdb::Connection,
    {
        let info = Self::info_for_db(remote_db).await?;
        let Some((synced, nosync)) = ssp_protocol::schema::sync_table_defs(&info) else {
            return Ok(Vec::new());
        };
        for name in &nosync {
            info!(table = %name, "Excluding @nosync table from snapshot");
        }
        Ok(synced.into_keys().collect())
    }

    /// `INFO FOR DB` on the remote, "database doesn't exist yet" read as
    /// `Null` (no table list at all).
    async fn info_for_db<C>(remote_db: &surrealdb::Surreal<C>) -> Result<Value>
    where
        C: surrealdb::Connection,
    {
        trace!("remote query: INFO FOR DB");
        match remote_db.query(ssp_protocol::schema::INFO_FOR_DB).await {
            Ok(mut response) => {
                let v: Vec<Value> = response.take(0).unwrap_or_default();
                Ok(v.into_iter().next().unwrap_or_default())
            }
            Err(e) if is_missing_error(&e) => {
                debug!("INFO FOR DB on missing database — treating as empty");
                Ok(Value::Null)
            }
            Err(e) => Err(anyhow::Error::from(e).context("Failed to query INFO FOR DB on remote")),
        }
    }

    /// Read upstream's schema probe (see `ssp_protocol::schema`). `None` when
    /// upstream has no table list at all (a database no migration touched).
    pub async fn probe_schema<C>(remote_db: &surrealdb::Surreal<C>) -> Result<Option<ssp_protocol::schema::SchemaProbe>>
    where
        C: surrealdb::Connection,
    {
        let info = Self::info_for_db(remote_db).await?;
        // Absent until the CLI first applies its schema: that is "no rows".
        let state: Value = match remote_db.query(ssp_protocol::schema::SCHEMA_STATE_QUERY).await {
            Ok(mut response) => response
                .take::<surrealdb::types::Value>(0)
                .map(|v| v.into_json_value())
                .unwrap_or(Value::Null),
            Err(_) => Value::Null,
        };
        Ok(ssp_protocol::schema::SchemaProbe::parse(&info, &state))
    }

    /// Per-table opaque-field sets, read from upstream `INFO FOR TABLE`.
    ///
    /// Best-effort per table: a failed `INFO FOR TABLE` yields no exclusions for
    /// that table rather than aborting the clone. The cost of missing one is a
    /// hash mismatch that the existing verify/re-clone machinery already
    /// handles; the cost of aborting is no replica at all.
    pub(crate) async fn discover_opaque_fields<C>(
        remote_db: &surrealdb::Surreal<C>,
        tables: &[String],
    ) -> BTreeMap<String, BTreeSet<String>>
    where
        C: surrealdb::Connection,
    {
        let mut out = BTreeMap::new();
        for table in tables {
            let info: Value = match remote_db.query(format!("INFO FOR TABLE {}", table)).await {
                Ok(mut response) => match response.take::<surrealdb::types::Value>(0) {
                    Ok(v) => v.into_json_value(),
                    Err(e) => {
                        warn!(table = %table, error = %e, "INFO FOR TABLE decode failed; no field exclusions");
                        continue;
                    }
                },
                Err(e) => {
                    warn!(table = %table, error = %e, "INFO FOR TABLE failed; no field exclusions");
                    continue;
                }
            };
            let opaque = ssp_protocol::opaque_fields_from_info(&info);
            if !opaque.is_empty() {
                info!(table = %table, fields = ?opaque, "Excluding opaque fields from snapshot");
                out.insert(table.clone(), opaque);
            }
        }
        out
    }

    /// Page a whole table out of the remote with a keyset cursor.
    ///
    /// Paging instead of one SELECT: the SurrealDB Rust SDK's WebSocket
    /// engine inherits tungstenite's default `max_message_size` of 64 MiB. A
    /// SELECT response that exceeds that fails the entire query, which
    /// historically caused bootstrap to stall on tables past ~60 MiB.
    ///
    /// Page size is chosen adaptively: a one-row probe measures actual
    /// serialised row size, then we pick a page count that targets ~32 MiB
    /// per response (half the WS frame ceiling, comfortable headroom).
    /// `SPKY_BOOTSTRAP_PAGE_SIZE` lets the operator override the result.
    ///
    /// Keyset cursor rationale: offset (`START n`) pagination silently drops
    /// rows when a concurrent write shifts the table between page requests —
    /// and the remote DB is LIVE during bootstrap — leaving the replica (and
    /// every SSP that bootstraps from it) with an incomplete table. Resume by
    /// `id > $last` instead: a delete behind the cursor can't shift rows
    /// ahead of it out of view. Mirrors `bootstrap_page_query` in apps/ssp.
    ///
    /// Takes the SDK's own `Value` then calls `into_json_value()` so
    /// RecordId/Datetime are flattened into normal JSON strings instead of
    /// `{"RecordId":{...}}` shapes. Tolerates the table disappearing between
    /// INFO FOR DB and SELECT (race) or simply not existing yet — treated as
    /// zero records.
    /// Page a whole table and collect it in memory. Used by the spool path,
    /// which writes one file per table and needs the rows together.
    ///
    /// The clone path deliberately does NOT use this: see [`Self::page_table`].
    async fn page_whole_table<C>(
        remote_db: &surrealdb::Surreal<C>,
        table_name: &str,
        omit: &BTreeSet<String>,
    ) -> Result<Vec<Value>>
    where
        C: surrealdb::Connection,
    {
        let mut out: Vec<Value> = Vec::new();
        Self::page_table(remote_db, table_name, omit, |page| {
            out.extend(page);
            std::future::ready(Ok(()))
        })
        .await?;
        Ok(out)
    }

    /// Page a table with keyset pagination, handing each page to `sink` as it
    /// arrives, and return the row count.
    ///
    /// The sink exists so a caller can consume a table it could not hold: the
    /// clone inserts each page and drops it, which keeps peak memory at one
    /// page instead of one table. Buffering the whole table first put ~220MB
    /// of `analysis` in a single `Vec` on a scheduler capped at 1GB.
    pub(crate) async fn page_table<C, F, Fut>(
        remote_db: &surrealdb::Surreal<C>,
        table_name: &str,
        omit: &BTreeSet<String>,
        mut sink: F,
    ) -> Result<usize>
    where
        C: surrealdb::Connection,
        F: FnMut(Vec<Value>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut info = remote_db.query("INFO FOR DB").await?;
        let info: surrealdb::types::Value = info.take(0)?;
        let has_versions = info.into_json_value().get("tables")
            .and_then(|v| v.get("_00_version")).is_some();
        let omit_clause = ssp_protocol::omit_clause(omit);
        let target_page_bytes: usize = 32 * 1024 * 1024;
        // Probe with the same projection the real pages use, or the auto-tuned
        // page size is computed from a row size that includes columns the clone
        // never fetches.
        let probe_row_bytes: Option<usize> = match remote_db
            .query(format!(
                "SELECT *{} FROM {} LIMIT 1",
                omit_clause, table_name
            ))
            .await
        {
            Ok(mut r) => match r.take::<surrealdb::types::Value>(0) {
                Ok(sdk_val) => match sdk_val.into_json_value() {
                    Value::Array(arr) => arr
                        .first()
                        .map(|v| serde_json::to_vec(v).map(|b| b.len()).unwrap_or(1024)),
                    _ => None,
                },
                Err(_) => None,
            },
            Err(_) => None,
        };
        let auto_page_size = probe_row_bytes
            .map(|b| (target_page_bytes / b.max(1)).max(1))
            .unwrap_or(200);
        let page_size: usize = std::env::var("SPKY_BOOTSTRAP_PAGE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &usize| *n > 0)
            .unwrap_or(auto_page_size)
            // Hard ceiling to avoid pathological large pages even if
            // the probe misses (e.g. variable row sizes).
            .min(2000);
        if let Some(b) = probe_row_bytes {
            trace!(
                table = %table_name,
                probe_bytes_per_row = b,
                page_size,
                "bootstrap page-size auto-tuned",
            );
        }
        let mut total: usize = 0;
        // Keyset cursor: the highest `id` paged so far (`None` = first page).
        let mut after_id: Option<String> = None;
        loop {
            let query = keyset_page_query(table_name, page_size, after_id.as_deref(), omit);
            let query = if has_versions {
                ssp_protocol::with_durable_row_versions(&query)
            } else { query };
            trace!(table = %table_name, after_id = ?after_id, page_size, "remote page query: {}", query);
            let resp = remote_db.query(query).await;
            let page: Vec<Value> = match resp {
                Ok(mut response) => match response.take::<surrealdb::types::Value>(0) {
                    Ok(sdk_val) => match sdk_val.into_json_value() {
                        Value::Array(arr) => arr,
                        other => bail!(
                            "Expected array from paged SELECT on {}, got {}",
                            table_name,
                            json_kind(&other),
                        ),
                    },
                    Err(e) if is_missing_error(&e) => {
                        debug!(table = %table_name, "remote table missing during page take — stopping");
                        break;
                    }
                    Err(e) => return Err(anyhow::anyhow!(
                        "take(0) failed for table '{}' page (after_id={:?}): {}",
                        table_name, after_id, e,
                    )),
                },
                Err(e) if is_missing_error(&e) => {
                    debug!(table = %table_name, "remote table missing during page query — stopping");
                    break;
                }
                Err(e) => return Err(anyhow::Error::from(e)
                    .context(format!(
                        "SELECT page from {} (after_id={:?}, limit={}) failed",
                        table_name, after_id, page_size,
                    ))),
            };
            let n = page.len();
            // Advance the cursor to this page's last id (page is ORDER BY id,
            // so the last row carries the max id) BEFORE consuming `page`.
            let next_after = page
                .last()
                .and_then(|row| row.get("id"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            total += n;
            // Hand the page over and drop it here: from this point the caller
            // owns those rows, and this loop holds nothing but the cursor.
            sink(page).await?;
            if n < page_size {
                break;
            }
            // No usable id to resume from → stop rather than loop forever.
            match next_after {
                Some(id) => after_id = Some(id),
                None => break,
            }
        }
        Ok(total)
    }

    /// The current rows of `table` upstream for the given record ids (as the
    /// replica spells them, `table:key`), keyed by that same spelling, with the
    /// same projection and `_00_rv` a clone reads. Ids that no longer exist
    /// are simply absent.
    pub(crate) async fn fetch_rows_by_id<C>(
        remote_db: &surrealdb::Surreal<C>,
        table: &str,
        ids: &[String],
        omit: &BTreeSet<String>,
    ) -> Result<std::collections::HashMap<String, Value>>
    where
        C: surrealdb::Connection,
    {
        let mut info = remote_db.query("INFO FOR DB").await?;
        let info: surrealdb::types::Value = info.take(0)?;
        let has_versions = info.into_json_value().get("tables")
            .and_then(|v| v.get("_00_version")).is_some();
        let query = format!(
            "SELECT *{} FROM $ids.map(|$i| <record> $i)",
            ssp_protocol::omit_clause(omit)
        );
        let query = if has_versions {
            ssp_protocol::with_durable_row_versions(&query)
        } else {
            query
        };
        let mut out = std::collections::HashMap::new();
        for chunk in ids.chunks(500) {
            let mut response = remote_db
                .query(&query)
                .bind(("ids", chunk.to_vec()))
                .await
                .with_context(|| format!("SELECT {} rows by id from {}", chunk.len(), table))?;
            let rows = match response.take::<surrealdb::types::Value>(0) {
                Ok(v) => v.into_json_value(),
                Err(e) if is_missing_error(&e) => break,
                Err(e) => return Err(anyhow::anyhow!("take(0) failed for rows of {}: {}", table, e)),
            };
            for row in rows.as_array().into_iter().flatten() {
                if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                    out.insert(id.to_string(), row.clone());
                }
            }
        }
        Ok(out)
    }

    /// Every row id of `table` with its `_00_rv` (`None` when the row carries
    /// none), as the replica holds them right now.
    pub async fn row_versions(&self, table: &str) -> Result<std::collections::HashMap<String, Option<i64>>> {
        let rows = self.query(&format!("SELECT id, _00_rv FROM {}", table)).await?;
        Ok(rows
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_string();
                Some((id, row.get("_00_rv").and_then(|v| v.as_i64())))
            })
            .collect())
    }

    /// [`Self::row_versions`] for just these record ids (`table:id`); ids the
    /// replica does not hold are absent from the result.
    pub async fn row_versions_for(
        &self,
        table: &str,
        ids: &[String],
    ) -> Result<std::collections::HashMap<String, Option<i64>>> {
        let mut out = std::collections::HashMap::new();
        for chunk in ids.chunks(500) {
            let mut response = self
                .db
                .query("SELECT id, _00_rv FROM $ids.map(|$i| <record> $i)")
                .bind(("ids", chunk.to_vec()))
                .await
                .with_context(|| format!("SELECT {} row versions from replica {}", chunk.len(), table))?;
            let rows: surrealdb::types::Value = response
                .take(0)
                .with_context(|| format!("take(0) for row versions of {}", table))?;
            for row in rows.into_json_value().as_array().into_iter().flatten() {
                if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                    out.insert(id.to_string(), row.get("_00_rv").and_then(|v| v.as_i64()));
                }
            }
        }
        Ok(out)
    }

    /// Bulk-insert records into a replica table in bounded batches. The
    /// per-record `CREATE … CONTENT` loop this replaced was O(N) round-trips;
    /// for tables with large records the per-call cost in SurrealDB 3.0 was
    /// non-linear and stalled bootstrap entirely past ~60 MiB total.
    /// `INSERT INTO <table> $records` collapses each batch to one round-trip.
    ///
    /// We still validate per-record that `id` exists; we let SurrealDB parse
    /// the string id (e.g. "comment:42") into the record-id field on insert.
    /// `.check()` once per batch is enough because a single bad record fails
    /// the whole batch (any insert failure aborts the whole clone).
    async fn bulk_insert(&self, table_name: &str, records: Vec<Value>) -> Result<()> {
        const INSERT_BATCH_SIZE: usize = 500;
        let chunks: Vec<Vec<Value>> = records
            .chunks(INSERT_BATCH_SIZE)
            .map(<[Value]>::to_vec)
            .collect();
        for (chunk_idx, mut chunk) in chunks.into_iter().enumerate() {
            // Validate ids upfront so we can give a specific error on the
            // offending record. Also strip the leading `<table>:` from each
            // id — without it, SurrealDB INSERT INTO treats the colon as
            // part of an escaped composite id and stores the row as
            // `<table>:`<table>:<raw>``, breaking every SELECT-by-id query
            // against the replica.
            let table_prefix = format!("{}:", table_name);
            for (within, rec) in chunk.iter_mut().enumerate() {
                let obj = match rec.as_object_mut() {
                    Some(o) => o,
                    None => anyhow::bail!(
                        "Record {}/{} in '{}' (batch {}) is not a JSON object",
                        within,
                        chunk_idx,
                        table_name,
                        chunk_idx,
                    ),
                };
                let id = obj
                    .get("id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!(
                        "Record {}/{} in '{}' (batch {}) missing string `id` after JSON flatten",
                        within,
                        chunk_idx,
                        table_name,
                        chunk_idx,
                    ))?
                    .to_string();
                let raw = id.strip_prefix(&table_prefix).unwrap_or(&id).to_string();
                obj.insert("id".to_string(), Value::String(raw));
            }
            let chunk_len = chunk.len();
            trace!(
                table = %table_name,
                chunk = chunk_idx,
                records = chunk_len,
                "bootstrap INSERT batch"
            );
            self.db
                .query(format!("INSERT INTO {} $records RETURN NONE", table_name))
                .bind(("records", Value::Array(chunk)))
                .await
                .with_context(|| format!(
                    "INSERT into {} batch {} send failed", table_name, chunk_idx,
                ))?
                .check()
                .with_context(|| format!(
                    "INSERT into {} batch {} returned an error", table_name, chunk_idx,
                ))?;
        }
        Ok(())
    }

    /// Copy view definitions from the remote. Tolerates `_00_query` not
    /// existing yet — happens when the scheduler is pointed at a fresh
    /// SurrealDB before Phase 4a has applied the internal Sp00ky schema.
    async fn fetch_remote_views<C>(remote_db: &surrealdb::Surreal<C>) -> Result<Vec<Value>>
    where
        C: surrealdb::Connection,
    {
        trace!("remote query: SELECT * FROM _00_query");
        Ok(match remote_db.query("SELECT * FROM _00_query").await {
            Ok(mut response) => match response.take::<surrealdb::types::Value>(0) {
                Ok(sdk_val) => match sdk_val.into_json_value() {
                    Value::Array(arr) => arr,
                    other => bail!(
                        "Expected array from SELECT * FROM _00_query, got {}",
                        json_kind(&other),
                    ),
                },
                Err(e) if is_missing_error(&e) => {
                    debug!("_00_query missing on remote — no views to copy");
                    Vec::new()
                }
                Err(e) => return Err(anyhow::anyhow!("take(0) failed for _00_query: {}", e)),
            },
            Err(e) if is_missing_error(&e) => {
                debug!("_00_query missing on remote — no views to copy");
                Vec::new()
            }
            Err(e) => return Err(anyhow::Error::from(e)
                .context("Failed to query _00_query on remote")),
        })
    }

    /// Write fetched view definitions into the replica. Returns the count.
    async fn insert_views(&self, views: Vec<Value>) -> Result<usize> {
        let view_count = views.len();
        for mut record in views {
            let id_str = match record.as_object_mut() {
                Some(obj) => obj.remove("id").and_then(|v| v.as_str().map(String::from)),
                None => None,
            };
            let id_str = id_str.context("_00_query record missing string `id` field")?;
            let key = if id_str.starts_with("_00_query:") {
                id_str
            } else {
                format!("_00_query:{}", id_str)
            };
            self.db
                .query(format!("CREATE {} CONTENT $data", key))
                .bind(("data", record))
                .await
                .with_context(|| format!("CREATE view {} send failed", key))?
                .check()
                .with_context(|| format!("CREATE view {} returned an error", key))?;
        }
        Ok(view_count)
    }

    /// Phase 1 of a two-phase re-clone: page every sync table (and the view
    /// definitions) out of the remote into a JSONL spool on disk, WITHOUT
    /// touching the replica or taking any lock. The old single-phase reclone
    /// held the replica write lock across the whole network clone, starving
    /// /proxy and every probe for minutes.
    ///
    /// The spool lives in a fresh temp dir under `spool_parent` (same disk as
    /// the replica) and is deleted when the returned manifest drops.
    pub async fn fetch_all_to_spool<C>(
        remote_db: &surrealdb::Surreal<C>,
        spool_parent: &std::path::Path,
    ) -> Result<SpoolManifest>
    where
        C: surrealdb::Connection,
    {
        use std::io::Write as _;

        std::fs::create_dir_all(spool_parent)
            .with_context(|| format!("Failed to create spool parent {:?}", spool_parent))?;
        let dir = tempfile::tempdir_in(spool_parent)
            .with_context(|| format!("Failed to create spool dir under {:?}", spool_parent))?;

        let tables = Self::discover_sync_tables(remote_db).await?;
        let opaque_fields = Self::discover_opaque_fields(remote_db, &tables).await;
        let empty = BTreeSet::new();
        let mut spooled = Vec::with_capacity(tables.len());
        for (idx, table_name) in tables.iter().enumerate() {
            let omit = opaque_fields.get(table_name).unwrap_or(&empty);
            let records = Self::page_whole_table(remote_db, table_name, omit).await?;
            let count = records.len();
            let path = dir.path().join(format!("{}.jsonl", table_name));
            // Blocking pool: the serialize+write of a large table is real
            // file IO, and phase 1 runs on the live runtime.
            let write_path = path.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let file = std::fs::File::create(&write_path)
                    .with_context(|| format!("Failed to create spool file {:?}", write_path))?;
                let mut w = std::io::BufWriter::new(file);
                for record in &records {
                    serde_json::to_writer(&mut w, record)?;
                    w.write_all(b"\n")?;
                }
                w.flush()?;
                Ok(())
            })
            .await
            .context("spool write task panicked")??;
            info!(
                table = %table_name,
                records = count,
                progress = format!("{}/{}", idx + 1, tables.len()),
                "Spooled table from remote",
            );
            spooled.push(SpooledTable {
                table: table_name.clone(),
                path,
                records: count,
            });
        }

        let views = Self::fetch_remote_views(remote_db).await?;
        Ok(SpoolManifest {
            _dir: dir,
            tables: spooled,
            views,
            opaque_fields,
        })
    }

    /// Phase 2 of a two-phase re-clone: load a spool produced by
    /// [`Self::fetch_all_to_spool`] into the (freshly reset) replica. Local
    /// disk → local RocksDB only; hold the write lock for this, not for the
    /// network clone.
    pub async fn load_from_spool(&mut self, manifest: &SpoolManifest) -> Result<()> {
        // Adopt phase 1's exclusions before any hash is computed off this data,
        // so `hash_one_table` and the SSP's circuit agree on the key set.
        self.opaque_fields = manifest.opaque_fields.clone();
        self.opaque_known = true;
        for spooled in &manifest.tables {
            let path = spooled.path.clone();
            let records = tokio::task::spawn_blocking(move || -> Result<Vec<Value>> {
                use std::io::BufRead as _;
                let file = std::fs::File::open(&path)
                    .with_context(|| format!("Failed to open spool file {:?}", path))?;
                let reader = std::io::BufReader::new(file);
                let mut out = Vec::new();
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    out.push(serde_json::from_str(&line)?);
                }
                Ok(out)
            })
            .await
            .context("spool read task panicked")??;

            self.known_tables.insert(spooled.table.clone());
            self.bulk_insert(&spooled.table, records).await?;
        }
        let view_count = self.insert_views(manifest.views.clone()).await?;
        info!(
            tables = manifest.tables.len(),
            records = manifest.tables.iter().map(|t| t.records).sum::<usize>(),
            views = view_count,
            "Loaded replica from spool",
        );
        Ok(())
    }

    /// Apply a single record event to the snapshot
    pub async fn apply(&mut self, table: &str, op: RecordOp, id: &str, record: Option<Value>) -> Result<()> {
        let synced = !ssp_protocol::table_excluded_from_sync(table);
        if synced {
            self.known_tables.insert(table.to_string());
        }
        let thing_id = build_thing_id(table, id);

        // Incremental hash maintenance. The table hash is an XOR set-hash of
        // per-row digests, so one event is folded as "old row out, new row
        // in" — two small reads instead of the drain paging the whole table
        // out of RocksDB again (408k rows every five minutes on whitepawn).
        // A table with no accumulator yet (first seen through this event) or
        // one already flagged dirty is left to the drain's from-content hash;
        // any failure to read a before/after image flags the table the same
        // way, never guesses. The write itself is applied regardless.
        let tracking = synced
            && self.snapshot_hashes.contains_key(table)
            && !self.dirty_hashes.contains(table);
        if synced && !self.snapshot_hashes.contains_key(table) {
            self.dirty_hashes.insert(table.to_string());
        }
        let mut before: Option<(String, Value)> = None;
        let mut after: Option<(String, Value)> = None;
        let mut fold_failed = false;

        // Idempotency for replayed changes. The changefeed tail replays the
        // window between a clone's cut and the tail's start, a WAL recovered
        // after a crash re-delivers what the last drain already folded, and
        // the http->changefeed switch delivers a few rows through both paths.
        // A row whose stored `_00_rv` is not older than the incoming one has
        // nothing to learn from it, so the write (and its hash fold) is
        // skipped rather than applied twice.
        if matches!(op, RecordOp::Create | RecordOp::Update) {
            let incoming = record
                .as_ref()
                .and_then(|r| r.get("_00_rv"))
                .and_then(|v| v.as_i64());
            if let Some(incoming) = incoming {
                match self.stored_rv(&thing_id).await {
                    Ok(Some(stored)) if stored >= incoming => {
                        debug!(table, id, stored, incoming, "Skipping a change the replica already holds");
                        return Ok(());
                    }
                    Ok(_) => {}
                    Err(e) => debug!(table, id, error = %e, "Could not read the stored _00_rv; applying anyway"),
                }
            }
        }

        if tracking && matches!(op, RecordOp::Update) {
            match self.read_row_for_hash(table, &thing_id).await {
                Ok(row) => before = row,
                Err(e) => {
                    warn!(table = %table, error = %e, "Could not read the before-image for the table hash; rehashing from content on the next drain");
                    fold_failed = true;
                }
            }
        }

        match op {
            RecordOp::Create => {
                if let Some(mut data) = record {
                    // Strip `id` from the payload before CREATE: SurrealDB
                    // 3.0 takes the record id from the `thing` literal.
                    // Keeping `id` in CONTENT as the string `"user:abc"`
                    // makes SurrealDB treat the colon as part of an
                    // escaped composite id and store it as
                    // `user:`user:abc``. Subsequent SELECTs by the clean
                    // `user:abc` form return nothing — and the SSP's
                    // bootstrap loads the corrupted shape, so live
                    // queries from the client never resolve.
                    if let Some(obj) = data.as_object_mut() {
                        obj.remove("id");
                    }
                    let mut response = self
                        .db
                        .query(format!("CREATE {} CONTENT $data RETURN AFTER", thing_id))
                        .bind(("data", data))
                        .await
                        .with_context(|| format!("CREATE {} send failed", thing_id))?
                        .check()
                        .with_context(|| format!("CREATE {} returned a statement error", thing_id))?;
                    if tracking {
                        match response.take::<surrealdb::types::Value>(0) {
                            Ok(v) => after = Self::first_row_json(v).and_then(|r| self.hash_pair_for(table, r)),
                            Err(e) => {
                                warn!(table = %table, error = %e, "Could not read the after-image for the table hash");
                                fold_failed = true;
                            }
                        }
                    }
                }
            }
            RecordOp::Update => {
                if let Some(mut data) = record {
                    if let Some(obj) = data.as_object_mut() {
                        obj.remove("id");
                    }
                    // UPSERT, not UPDATE: since SurrealDB 2 an UPDATE of a
                    // record that does not exist is a no-op, so an update
                    // whose create the replica never saw (a change feed
                    // entry read past a clone cut, a restart) would leave the
                    // row missing until the next re-clone.
                    let mut response = self
                        .db
                        .query(format!("UPSERT {} MERGE $data RETURN AFTER", thing_id))
                        .bind(("data", data))
                        .await
                        .with_context(|| format!("UPSERT {} send failed", thing_id))?
                        .check()
                        .with_context(|| format!("UPSERT {} returned a statement error", thing_id))?;
                    if tracking {
                        match response.take::<surrealdb::types::Value>(0) {
                            Ok(v) => after = Self::first_row_json(v).and_then(|r| self.hash_pair_for(table, r)),
                            Err(e) => {
                                warn!(table = %table, error = %e, "Could not read the after-image for the table hash");
                                fold_failed = true;
                            }
                        }
                    }
                }
            }
            RecordOp::Delete => {
                let mut response = self
                    .db
                    .query(format!("DELETE {} RETURN BEFORE", thing_id))
                    .await
                    .with_context(|| format!("DELETE {} send failed", thing_id))?
                    .check()
                    .with_context(|| format!("DELETE {} returned a statement error", thing_id))?;
                if tracking {
                    match response.take::<surrealdb::types::Value>(0) {
                        Ok(v) => before = Self::first_row_json(v).and_then(|r| self.hash_pair_for(table, r)),
                        Err(e) => {
                            warn!(table = %table, error = %e, "Could not read the before-image for the table hash");
                            fold_failed = true;
                        }
                    }
                }
            }
        }

        if tracking {
            if fold_failed {
                self.dirty_hashes.insert(table.to_string());
                self.drop_range_build(table);
            } else {
                self.fold_hash_delta(table, before.as_ref(), after.as_ref());
            }
        } else if synced {
            // Not folded, so a build of the table no longer describes it. Its
            // ranges are replaced with the from-content hash it is waiting for.
            self.drop_range_build(table);
        }

        debug!("Applied {:?} for {}", op, thing_id);
        Ok(())
    }

    /// Export the replica to a file using SurrealDB's native export.
    /// Produces a standard SurrealQL dump importable via `surreal import`.
    pub async fn export_to_file(&self, path: &std::path::Path) -> Result<()> {
        self.db
            .export(path)
            .await
            .with_context(|| format!("Failed to export replica to {:?}", path))?;
        Ok(())
    }

    /// Import a SurrealQL dump file into the replica. Caller must ensure the
    /// underlying DB is empty (call `reset` first) — `import` executes the
    /// statements from the file and will error on duplicate records.
    pub async fn import_from_file(&self, path: &std::path::Path) -> Result<()> {
        self.db
            .import(path)
            .await
            .with_context(|| format!("Failed to import replica from {:?}", path))?;
        Ok(())
    }

    /// Wipe the replica's logical contents in place via SurrealQL. Resets
    /// `snapshot_seq` to 0. The caller must hold the write lock on the replica.
    ///
    /// We deliberately do NOT drop + reopen the RocksDB handle here: RocksDB's
    /// `LOCK` file is released lazily after all handles drop, and the old
    /// `Surreal<Db>` is an Arc'd handle that SurrealDB keeps alive beyond our
    /// assignment — so reopening at the same path immediately races with the
    /// prior lock and fails with "No locks available". REMOVE DATABASE +
    /// DEFINE DATABASE achieves the same logical empty state without touching
    /// the filesystem and mirrors how the main remote DB is wiped in
    /// `restore::execute_restore_inner`.
    pub async fn reset(&mut self) -> Result<()> {
        self.db
            .query("REMOVE DATABASE IF EXISTS snapshot; DEFINE DATABASE snapshot;")
            .await
            .context("Failed to wipe replica database")?;
        self.db
            .use_db("snapshot")
            .await
            .context("Failed to re-select replica database after wipe")?;
        self.snapshot_seq = 0;
        self.publish_seq();
        self.snapshot_hashes.clear();
        self.known_tables.clear();
        self.dirty_hashes.clear();
        self.interrupted_apply = false;
        self.changefeed_vs = 0;
        self.opaque_fields.clear();
        self.opaque_known = false;
        self.ranges.clear();
        self.ranges_unpersisted.clear();
        self.ranges_unsupported.clear();
        *self.range_build.get_mut().unwrap() = None;
        info!(path = ?self.db_path, "Replica reset (REMOVE DATABASE)");
        Ok(())
    }

    /// Re-derive `known_tables` from the replica's OWN schema. Used after a
    /// restore: the imported dump is an export of the *main* database, which
    /// carries `DEFINE TABLE` statements but no `_00_metadata:snapshot` row —
    /// so `reload_snapshot_seq` finds nothing and leaves the replica claiming
    /// zero known tables (and zero hashes) over fully populated content.
    /// Returns the number of tables discovered.
    pub async fn rediscover_known_tables(&mut self) -> Result<usize> {
        let info: Value = match self.db.query("INFO FOR DB").await {
            Ok(mut response) => {
                let v: Vec<Value> = response.take(0).unwrap_or_default();
                v.into_iter().next().unwrap_or_default()
            }
            Err(e) if is_missing_error(&e) => Value::Null,
            Err(e) => {
                return Err(anyhow::Error::from(e)
                    .context("Failed to query INFO FOR DB on the replica"))
            }
        };

        let tables: Vec<String> = ssp_protocol::schema::sync_table_defs(&info)
            .map(|(synced, _)| synced.into_keys().collect())
            .unwrap_or_default();

        for t in &tables {
            self.known_tables.insert(t.clone());
        }

        // A restored dump is an export of the MAIN database, so it carries the
        // user's `DEFINE FIELD` statements — including the opaque markers. That
        // makes the replica itself a usable source here, unlike on the clone
        // path where the replica is schemaless.
        let discovered = Self::discover_opaque_fields(&self.db, &tables).await;
        self.opaque_fields = discovered;
        self.opaque_known = true;

        Ok(tables.len())
    }

    /// Re-read snapshot state from the embedded metadata table. Useful after
    /// importing a dump — the imported `_00_metadata:snapshot` row carries the
    /// seq, hashes, and table list from the time of backup.
    pub async fn reload_snapshot_seq(&mut self) -> Result<u64> {
        let state = Self::read_snapshot_state_from_db(&self.db)
            .await
            .unwrap_or_default();
        self.snapshot_seq = state.seq;
        self.publish_seq();
        self.dirty_hashes = Self::stale_format_hashes(&state.hashes);
        self.snapshot_hashes = state.hashes;
        self.known_tables = state.tables;
        self.interrupted_apply = state.applying;
        self.ranges_unsupported.clear();
        self.load_ranges().await;
        Ok(self.snapshot_seq)
    }

    /// Run an arbitrary SurrealQL query against the snapshot DB
    /// Returns the raw JSON response (used by the HTTP proxy).
    ///
    /// SurrealDB 3.0 errors on `SELECT * FROM <undefined>` instead of returning
    /// an empty array. The replica is schemaless and tables only "exist" once
    /// they receive a `CREATE`, so callers (notably SSP bootstrap querying
    /// `_00_query`) need missing tables to behave like empty result sets. We
    /// detect that case via the SDK's `NotFound` error and translate to `[]`.
    pub async fn query(&self, surql: &str) -> Result<Value> {
        trace!(query = %surql, "local replica query");
        let mut response = self.db
            .query(surql)
            .await
            .with_context(|| format!("Failed to execute query: {}", surql))?;

        match response.take::<surrealdb::types::Value>(0) {
            Ok(v) => Ok(v.into_json_value()),
            Err(e) => {
                if is_missing_error(&e) {
                    debug!(query = %surql, "query targets a missing table — returning []");
                    Ok(Value::Array(Vec::new()))
                } else {
                    Err(anyhow::anyhow!(
                        "take(0) failed for query [{}]: {}", surql, e
                    ))
                }
            }
        }
    }

    /// Serialize all records for SSP bootstrap (chunked)
    pub async fn iter_chunks(&self, chunk_size: usize) -> Result<Vec<ReplicaChunk>> {
        // Discover tables
        let mut response = self.db
            .query("INFO FOR DB")
            .await
            .context("Failed to query INFO FOR DB on replica")?;

        let info: Vec<Value> = response.take(0).unwrap_or_default();
        let info = info.into_iter().next().unwrap_or_default();

        let tables: Vec<String> = match info.get("tables") {
            Some(Value::Object(tables_map)) => tables_map
                .keys()
                .filter(|name| !ssp_protocol::table_excluded_from_sync(name))
                .cloned()
                .collect(),
            _ => vec!["thread".to_string(), "job".to_string(), "user".to_string()],
        };

        let mut chunks = Vec::new();
        let mut chunk_index = 0;

        for table_name in tables {
            let mut response = self.db
                .query(format!("SELECT * FROM {}", table_name))
                .await
                .with_context(|| format!("Failed to select from replica table '{}'", table_name))?;

            let records: Vec<Value> = response.take(0).unwrap_or_default();
            let mut current_chunk = Vec::new();

            for record in records {
                let id = record.get("id")
                    .map(|v| v.to_string().trim_matches('"').to_string())
                    .unwrap_or_default();
                current_chunk.push((id, record));

                if current_chunk.len() >= chunk_size {
                    chunks.push(ReplicaChunk {
                        chunk_index,
                        table: table_name.clone(),
                        records: std::mem::take(&mut current_chunk),
                    });
                    chunk_index += 1;
                }
            }

            if !current_chunk.is_empty() {
                chunks.push(ReplicaChunk {
                    chunk_index,
                    table: table_name,
                    records: current_chunk,
                });
                chunk_index += 1;
            }
        }

        Ok(chunks)
    }

    /// Get total record count across all tables
    pub async fn record_count(&self) -> Result<usize> {
        Ok(self.record_counts_per_table().await?.into_iter().map(|(_, c)| c).sum())
    }

    /// Get per-table record counts for every non-`_00_` table in the replica.
    /// Used by the `/health/snapshot` endpoint and `spky verify` to compare
    /// replica state against the upstream SurrealDB.
    ///
    /// Discovers tables from `known_tables` (populated by `ingest_all` and
    /// `apply`, persisted in `_00_metadata:snapshot.tables`). SurrealDB
    /// `INFO FOR DB` doesn't list schemaless tables we created via CREATE,
    /// so we cannot rely on the engine for discovery.
    pub async fn record_counts_per_table(&self) -> Result<Vec<(String, usize)>> {
        let mut counts = Vec::with_capacity(self.known_tables.len());
        for table_name in &self.known_tables {
            let count = self.count_table(table_name).await?;
            counts.push((table_name.clone(), count));
        }
        Ok(counts)
    }

    pub async fn count_table(&self, table_name: &str) -> Result<usize> {
        let mut response = self.db
            .query(format!("SELECT count() AS total FROM {} GROUP ALL", table_name))
            .await
            .with_context(|| format!("count() query failed for table '{}'", table_name))?;
        // A table with zero rows upstream is never created in the replica (the
        // clone only inserts), so the statement fails here instead of
        // returning an empty result. That is an empty table, the same way
        // `hash_one_table` hashes a missing table as empty; erroring took
        // `/health/snapshot` (and with it `spky verify`) down for every
        // deployment whose meta tables were empty.
        let sdk_val: surrealdb::types::Value = match response.take(0) {
            Ok(v) => v,
            Err(e) => {
                debug!(table = table_name, error = %e, "count() on a table absent from the replica; treating as empty");
                return Ok(0);
            }
        };
        let json = sdk_val.into_json_value();
        let count = json.as_array()
            .and_then(|arr| arr.first())
            .and_then(|row| row.get("total"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        Ok(count)
    }

    /// Number of non-`_00_` tables present in the replica.
    pub async fn table_count(&self) -> Result<usize> {
        Ok(self.record_counts_per_table().await?.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cgroup_limit_parses_both_versions_and_unlimited() {
        // v2 container with a cap.
        assert_eq!(parse_cgroup_limit("3221225472\n"), Some(3 * 1024 * 1024 * 1024));
        // v2 without one.
        assert_eq!(parse_cgroup_limit("max\n"), None);
        // v1's "unlimited" sentinel (i64::MAX page-aligned) must not read as
        // a ~9EB cap.
        assert_eq!(parse_cgroup_limit("9223372036854771712"), None);
        // v1 with a cap.
        assert_eq!(parse_cgroup_limit("1073741824"), Some(1024 * 1024 * 1024));
        // Anything else is "we don't know", never a guess.
        assert_eq!(parse_cgroup_limit(""), None);
        assert_eq!(parse_cgroup_limit("not-a-number"), None);
    }

    #[test]
    fn keyset_page_query_uses_ordered_cursor_not_offset() {
        // Regression guard: replica bootstrap must page by id keyset, never by
        // OFFSET/START (lossy under the concurrent writes a live DB sees while
        // it's being paged). Mirrors the SSP bootstrap fix.
        let none = BTreeSet::new();
        let first = keyset_page_query("game", 200, None, &none);
        assert_eq!(first, "SELECT * FROM game ORDER BY id LIMIT 200");

        let next = keyset_page_query("game", 200, Some("game:abc"), &none);
        assert_eq!(
            next,
            "SELECT * FROM game WHERE id > type::record('game', 'abc') ORDER BY id LIMIT 200"
        );

        assert!(!first.contains("START") && !next.contains("START"));
        assert!(first.contains("ORDER BY id") && next.contains("ORDER BY id"));
    }

    /// The clone pager must render a projection byte-identical to the SSP's
    /// `bootstrap_page_query` — the two feed opposite sides of the same content
    /// hash comparison. Both delegate the clause to `ssp_protocol::omit_clause`
    /// (tested there); these literals pin the surrounding query shape, which is
    /// duplicated across the two crates and can only be kept in step by hand.
    #[test]
    fn clone_pager_omit_projection_matches_the_ssp_bootstrap_shape() {
        let omit: BTreeSet<String> = ["blob", "secret_token"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            keyset_page_query("user", 200, None, &omit),
            "SELECT * OMIT blob, secret_token FROM user ORDER BY id LIMIT 200",
        );
        assert_eq!(
            keyset_page_query("user", 200, Some("user:abc"), &omit),
            "SELECT * OMIT blob, secret_token FROM user WHERE id > type::record('user', 'abc') ORDER BY id LIMIT 200",
        );
    }

    async fn insert_thread(db: &Surreal<surrealdb::engine::local::Db>, title: &str) -> Result<()> {
        db.query(format!("CREATE thread SET title = '{}'", title))
            .await?;
        Ok(())
    }

    async fn count_threads(db: &Surreal<surrealdb::engine::local::Db>) -> Result<usize> {
        let mut resp = db.query("SELECT count() FROM thread GROUP ALL").await?;
        let rows: Vec<Value> = resp.take(0).unwrap_or_default();
        Ok(rows
            .first()
            .and_then(|r| r.get("count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize)
    }

    /// Reset must wipe data in place without tripping RocksDB's file lock, and
    /// the handle must stay usable. This would have caught the original bug
    /// (dropping + reopening at the same path failed with "No locks available").
    /// The changefeed tail replays a clone's overlap window and a recovered
    /// WAL re-delivers what the last drain already folded: both must be
    /// no-ops for a row already at that version, and an UPDATE for a row the
    /// replica never saw must create it rather than vanish.
    #[tokio::test]
    async fn apply_is_idempotent_on_rv_and_upserts_unknown_updates() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut replica = Replica::new(tmp.path().join("replica")).await?;
        let row = |x: i64, rv: i64| Some(serde_json::json!({ "x": x, "_00_rv": rv }));

        replica.apply("game", RecordOp::Create, "game:a", row(1, 3)).await?;
        // Older and equal versions are skipped.
        replica.apply("game", RecordOp::Update, "game:a", row(99, 2)).await?;
        replica.apply("game", RecordOp::Update, "game:a", row(98, 3)).await?;
        assert_eq!(replica.row("game", "game:a").await?.unwrap()["x"], 1);
        // A newer one lands.
        replica.apply("game", RecordOp::Update, "game:a", row(2, 4)).await?;
        assert_eq!(replica.row("game", "game:a").await?.unwrap()["x"], 2);
        // A replayed CREATE for a row already ahead is skipped, not an error.
        replica.apply("game", RecordOp::Create, "game:a", row(0, 1)).await?;
        assert_eq!(replica.row("game", "game:a").await?.unwrap()["x"], 2);
        // An UPDATE for a row the replica never saw creates it.
        replica.apply("game", RecordOp::Update, "game:b", row(7, 1)).await?;
        assert_eq!(replica.row("game", "game:b").await?.unwrap()["x"], 7);
        assert!(replica.row("game", "game:zzz").await?.is_none());
        // Unversioned rows keep the old behaviour: every write applies.
        replica.apply("game", RecordOp::Update, "game:b", Some(serde_json::json!({ "x": 8 }))).await?;
        assert_eq!(replica.row("game", "game:b").await?.unwrap()["x"], 8);
        Ok(())
    }

    /// The cursor rides `_00_metadata:snapshot` with the seq, so a restart
    /// resumes the tail where the rows are. (Read back through the metadata
    /// reader rather than by reopening the path: the embedded RocksDB keeps
    /// its lock until the runtime tears the engine down.)
    #[tokio::test]
    async fn changefeed_cursor_persists_with_the_snapshot_state() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut replica = Replica::new(tmp.path().join("replica")).await?;
        replica.set_changefeed_vs(500);
        replica.set_changefeed_vs(400); // never backwards
        assert_eq!(replica.changefeed_vs(), 500);
        replica.set_snapshot_seq(7).await?;

        let persisted = Replica::read_snapshot_state_from_db(&replica.db).await?;
        assert_eq!(persisted.changefeed_vs, 500);
        assert_eq!(persisted.seq, 7);

        replica.reset().await?;
        assert_eq!(replica.changefeed_vs(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn reset_wipes_data_and_stays_usable() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut replica = Replica::new(tmp.path().join("replica")).await?;

        insert_thread(&replica.db, "hello").await?;
        assert_eq!(count_threads(&replica.db).await?, 1);
        replica.set_snapshot_seq(42).await?;

        replica.reset().await?;

        assert_eq!(replica.snapshot_seq(), 0);
        assert_eq!(replica.reload_snapshot_seq().await?, 0);
        assert_eq!(count_threads(&replica.db).await?, 0);

        insert_thread(&replica.db, "world").await?;
        assert_eq!(count_threads(&replica.db).await?, 1);

        replica.reset().await?;
        assert_eq!(count_threads(&replica.db).await?, 0);

        Ok(())
    }

    /// A table that turned `@nosync` (or was dropped) must lose its rows, its
    /// hash and its known-table entry: the SSP leaves it out of its bootstrap
    /// and would dispute the hash forever.
    #[tokio::test]
    async fn reconcile_drops_removed_tables_and_persists() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut replica = Replica::new(tmp.path().join("replica")).await?;
        replica
            .apply("user", RecordOp::Create, "user:u1", Some(serde_json::json!({"name": "a", "_00_rv": 1})))
            .await?;
        replica
            .apply("analysis", RecordOp::Create, "analysis:a1", Some(serde_json::json!({"fen": "x", "_00_rv": 1})))
            .await?;
        replica.set_snapshot_state(7, None).await?;
        assert!(replica.snapshot_hashes().contains_key("analysis"));
        assert!(replica.known_tables().contains("analysis"));

        let removed = vec!["analysis".to_string(), "never_held".to_string()];
        let gone = replica.reconcile_schema(&removed, None).await?;
        assert_eq!(gone, vec!["analysis".to_string()]);
        assert_eq!(replica.count_table("analysis").await?, 0, "rows dropped");
        assert!(!replica.snapshot_hashes().contains_key("analysis"));
        assert!(!replica.known_tables().contains("analysis"));
        assert!(replica.snapshot_hashes().contains_key("user"), "synced tables keep their hash");
        assert_eq!(replica.snapshot_seq(), 7, "the sequence is untouched");

        // Persisted, so the next boot does not resurrect it from metadata.
        let mut resp = replica
            .db
            .query("SELECT seq, hashes, tables, applying FROM _00_metadata:snapshot")
            .await?;
        let rows: Vec<Value> = resp.take(0)?;
        let tables: Vec<String> = serde_json::from_value(rows[0]["tables"].clone())?;
        assert_eq!(tables, vec!["user".to_string()]);
        let hashes: BTreeMap<String, String> = serde_json::from_value(rows[0]["hashes"].clone())?;
        assert!(!hashes.contains_key("analysis"));

        // Idempotent.
        assert!(replica.reconcile_schema(&removed, None).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn hold_table_is_persisted() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut replica = Replica::new(tmp.path().join("replica")).await?;
        replica.hold_table("stream_video");
        replica.set_snapshot_state(1, Some(&BTreeSet::new())).await?;
        let mut resp = replica.db.query("SELECT tables FROM _00_metadata:snapshot").await?;
        let rows: Vec<Value> = resp.take(0)?;
        let tables: Vec<String> = serde_json::from_value(rows[0]["tables"].clone())?;
        assert_eq!(tables, vec!["stream_video".to_string()], "a boot restores it as held");
        assert!(!replica.snapshot_hashes().contains_key("stream_video"));
        Ok(())
    }

    /// A table whose opaque set moved at runtime is rehashed from content; the
    /// first read after a restart is not a move.
    #[tokio::test]
    async fn a_moved_opaque_set_marks_the_table_dirty() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let mut replica = Replica::new(tmp.path().join("replica")).await?;
        replica
            .apply("game", RecordOp::Create, "game:g1", Some(serde_json::json!({"pgn": "x", "_00_rv": 1})))
            .await?;
        replica.set_snapshot_state(3, None).await?;
        let set = |fields: &[&str]| -> BTreeMap<String, BTreeSet<String>> {
            [("game".to_string(), fields.iter().map(|f| f.to_string()).collect())].into_iter().collect()
        };

        replica.reconcile_schema(&[], Some(set(&["crdt"]))).await?;
        assert!(replica.dirty_tables().is_empty(), "first read after boot");
        replica.reconcile_schema(&[], Some(set(&["crdt"]))).await?;
        assert!(replica.dirty_tables().is_empty(), "unchanged");
        replica.reconcile_schema(&[], Some(set(&["crdt", "notes"]))).await?;
        assert!(replica.dirty_tables().contains("game"));
        assert_eq!(replica.omit_for("game").len(), 2);
        Ok(())
    }

    /// Full backup-restore shape: export → reset → import on a different path.
    #[tokio::test]
    async fn reset_then_import_round_trips_data() -> Result<()> {
        let src_tmp = tempfile::tempdir()?;
        let src = Replica::new(src_tmp.path().join("src")).await?;
        insert_thread(&src.db, "hello").await?;

        let dump = src_tmp.path().join("dump.surql");
        src.export_to_file(&dump).await?;

        let dst_tmp = tempfile::tempdir()?;
        let mut dst = Replica::new(dst_tmp.path().join("dst")).await?;
        insert_thread(&dst.db, "stale").await?;
        assert_eq!(count_threads(&dst.db).await?, 1);

        dst.reset().await?;
        assert_eq!(count_threads(&dst.db).await?, 0);

        dst.import_from_file(&dump).await?;

        let mut resp = dst.db.query("SELECT title FROM thread").await?;
        let rows: Vec<Value> = resp.take(0).unwrap_or_default();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("title").and_then(|v| v.as_str()), Some("hello"));

        Ok(())
    }

    /// A replica holding `game` rows `g00000..` (n of them), hashed from content.
    async fn ranged_replica(n: usize) -> Result<(tempfile::TempDir, Replica)> {
        let tmp = tempfile::tempdir()?;
        let mut replica = Replica::new(tmp.path().join("replica")).await?;
        let mut i = 0;
        while i < n {
            let mut q = String::from("BEGIN;");
            for j in i..(i + 500).min(n) {
                q.push_str(&format!(" CREATE game:g{j:05} SET n = {j}, _00_rv = 1;"));
            }
            q.push_str(" COMMIT;");
            replica.db.query(q).await?.check()?;
            i += 500;
        }
        replica.known_tables.insert("game".to_string());
        replica.set_snapshot_state(1, None).await?;
        Ok((tmp, replica))
    }

    fn row(n: i64, rv: i64) -> Option<Value> {
        Some(serde_json::json!({ "n": n, "_00_rv": rv }))
    }

    #[tokio::test]
    async fn content_hash_cuts_ranges_and_matches_the_keyset_pager() -> Result<()> {
        let (_tmp, replica) = ranged_replica(2500).await?;
        let paged = replica.hash_one_table_paged("game").await?;
        assert_eq!(replica.snapshot_hashes()["game"], paged);

        let wire = replica.table_ranges("game").expect("ranges cut by the content hash");
        assert_eq!(wire.hash, paged);
        assert_eq!(wire.starts, vec!["", "g01000", "g02000"]);
        assert_eq!(wire.counts, vec![1000, 1000, 500]);

        // A table with a numeric key cannot be ranged, and still hashes right.
        replica.db.query("CREATE mixed:abc SET n = 1; CREATE mixed:7 SET n = 2;").await?.check()?;
        let content = replica.hash_one_table("mixed").await?;
        assert!(content.ranges.is_none());
        assert_eq!(content.hash, replica.hash_one_table_paged("mixed").await?);

        // A table that does not exist is one empty range.
        let empty = replica.hash_one_table("nothing").await?;
        assert_eq!(empty.hash, snapshot_hash::xor_empty_table_hash());
        assert_eq!(empty.ranges.map(|r| r.len()), Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn applied_events_keep_the_ranges_equal_to_a_rebuild() -> Result<()> {
        let (_tmp, mut replica) = ranged_replica(2500).await?;
        replica.apply("game", RecordOp::Update, "game:g00010", row(-1, 2)).await?;
        replica.apply("game", RecordOp::Delete, "game:g01500", None).await?;
        replica.apply("game", RecordOp::Create, "game:g01500x", row(7, 1)).await?;
        replica.apply("game", RecordOp::Update, "game:zz", row(8, 1)).await?;
        replica.apply("game", RecordOp::Create, "game:`a-b`", row(9, 1)).await?;

        let folded = replica.table_ranges("game").unwrap();
        assert_eq!(folded.hash, replica.hash_one_table("game").await?.hash);
        // Same boundaries as before the events, and every range holds what a
        // fresh read of it holds now.
        assert_eq!(folded.starts, vec!["", "g01000", "g02000"]);
        let mut fresh = RangeHashes::with_starts(folded.starts.clone()).unwrap();
        for i in 0..fresh.len() {
            assert!(replica.scan_window("game", &mut fresh, i).await?);
        }
        assert_eq!(folded, fresh.to_wire("game"));
        assert_eq!(folded.counts, vec![1001, 1000, 501]);

        // A numeric key ends ranging for the table, not the hash.
        replica.apply("game", RecordOp::Create, "game:42", row(1, 1)).await?;
        assert!(replica.table_ranges("game").is_none());
        assert_eq!(replica.snapshot_hashes()["game"], replica.hash_one_table_paged("game").await?);
        Ok(())
    }

    #[tokio::test]
    async fn ranges_persist_and_are_dropped_when_they_do_not_add_up() -> Result<()> {
        let (_tmp, mut replica) = ranged_replica(1500).await?;
        replica.apply("game", RecordOp::Update, "game:g00001", row(5, 2)).await?;
        replica.commit_snapshot_state(2, BTreeMap::new(), BTreeSet::new()).await?;
        let before = replica.table_ranges("game").unwrap();

        replica.reload_snapshot_seq().await?;
        assert_eq!(replica.table_ranges("game"), Some(before.clone()));

        // A persisted hash newer than the persisted ranges (a crash between
        // the two writes): the ranges are not trusted.
        replica
            .db
            .query("UPSERT _00_metadata:snapshot SET hashes.game = $h")
            .bind(("h", snapshot_hash::xor_empty_table_hash()))
            .await?
            .check()?;
        replica.reload_snapshot_seq().await?;
        assert!(replica.table_ranges("game").is_none());

        // A dirty table serves none either.
        let (_tmp2, mut other) = ranged_replica(10).await?;
        assert!(other.table_ranges("game").is_some());
        other.mark_tables_dirty(["game".to_string()]);
        assert!(other.table_ranges("game").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_background_build_takes_the_events_applied_while_it_runs() -> Result<()> {
        let (_tmp, mut replica) = ranged_replica(3500).await?;
        // A replica from before ranges: hashes, no ranges.
        replica.ranges.clear();
        assert_eq!(replica.next_range_build(&BTreeSet::new()).as_deref(), Some("game"));
        assert_eq!(replica.range_build_begin("game").await?, RangeBuildStep::More);
        assert_eq!(replica.range_build_step().await?, RangeBuildStep::More);
        assert_eq!(replica.range_build_step().await?, RangeBuildStep::More);

        // Windows 0 and 1 are read, 2 and 3 are not.
        replica.apply("game", RecordOp::Update, "game:g00002", row(-2, 2)).await?;
        replica.apply("game", RecordOp::Delete, "game:g01999", None).await?;
        replica.apply("game", RecordOp::Update, "game:g02500", row(-3, 2)).await?;
        replica.apply("game", RecordOp::Create, "game:g03999x", row(-4, 1)).await?;

        assert_eq!(replica.range_build_step().await?, RangeBuildStep::More);
        replica.apply("game", RecordOp::Update, "game:g02001", row(-5, 2)).await?;
        assert_eq!(replica.range_build_step().await?, RangeBuildStep::Done);
        assert!(replica.range_build_finish().await);

        let built = replica.table_ranges("game").unwrap();
        assert_eq!(built.hash, replica.hash_one_table("game").await?.hash);
        // Every range holds what a fresh read of it holds now.
        let mut fresh = RangeHashes::with_starts(built.starts.clone()).unwrap();
        for i in 0..fresh.len() {
            assert!(replica.scan_window("game", &mut fresh, i).await?);
        }
        assert_eq!(built, fresh.to_wire("game"));
        assert_eq!(built.counts, vec![1000, 999, 1000, 501]);
        assert_eq!(replica.next_range_build(&BTreeSet::new()), None);
        Ok(())
    }

    /// The background driver ranges every table that lacks ranges, a window
    /// per read guard, and leaves a table whose keys cannot be ranged alone.
    #[tokio::test]
    async fn the_builder_ranges_every_table_that_has_none() -> Result<()> {
        let (_tmp, mut replica) = ranged_replica(2200).await?;
        replica.db.query("CREATE mixed:abc SET n = 1; CREATE mixed:7 SET n = 2;").await?.check()?;
        replica.known_tables.insert("mixed".to_string());
        replica.set_snapshot_state(2, None).await?;
        assert!(replica.ranges_unsupported.contains("mixed"));
        replica.ranges.clear();
        replica.ranges_unsupported.clear();
        let replica = std::sync::Arc::new(tokio::sync::RwLock::new(replica));

        let mut skip = BTreeSet::new();
        let mut built = Vec::new();
        while let Some((table, installed)) = crate::build_ranges_once(&replica, &skip).await {
            built.push((table.clone(), installed));
            skip.insert(table);
        }
        assert_eq!(built, vec![("game".to_string(), true), ("mixed".to_string(), false)]);
        let rep = replica.read().await;
        assert_eq!(rep.table_ranges("game").unwrap().counts, vec![1000, 1000, 200]);
        assert!(rep.table_ranges("mixed").is_none());
        assert_eq!(rep.next_range_build(&BTreeSet::new()), None, "mixed is not tried again");
        Ok(())
    }

    #[tokio::test]
    async fn a_build_is_dropped_when_its_table_goes_dirty() -> Result<()> {
        let (_tmp, mut replica) = ranged_replica(1200).await?;
        replica.ranges.clear();
        replica.range_build_begin("game").await?;
        replica.range_build_step().await?;
        replica.mark_tables_dirty(["game".to_string()]);
        replica.apply("game", RecordOp::Update, "game:g00003", row(1, 2)).await?;
        assert_eq!(replica.range_build_step().await?, RangeBuildStep::Idle);
        assert!(!replica.range_build_finish().await);
        Ok(())
    }
}
