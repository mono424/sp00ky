use anyhow::{bail, Context, Result};
use serde_json::Value;
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
    /// Lock-free mirror of `snapshot_seq` for health/metrics readers. A drain
    /// or reclone holds the replica write lock for a long time; probes must
    /// never queue behind it just to read one number.
    seq_cell: std::sync::Arc<std::sync::atomic::AtomicU64>,
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

        Ok(Self {
            db,
            db_path,
            snapshot_seq,
            snapshot_hashes,
            known_tables,
            dirty_hashes,
            interrupted_apply,
            // Re-derived from upstream DDL on the first clone/rediscover; a
            // fresh process starts with no exclusions.
            opaque_fields: BTreeMap::new(),
            seq_cell: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(snapshot_seq)),
        })
    }

    /// Get current snapshot sequence number
    pub fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
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

    /// Forget tables the upstream no longer syncs: marked `-- @nosync` since
    /// this snapshot was persisted, or dropped outright. `current` is what a
    /// fresh clone would load ([`Self::discover_sync_tables`]).
    ///
    /// Their hashes used to be handed to every bootstrapping SSP, which leaves
    /// such a table out of its own load and so disputed it on every attempt:
    /// whitepawn after `analysis` went @nosync logged "Bootstrap integrity
    /// mismatch table=analysis expected=x3:f731… actual=x3:000…" per bootstrap
    /// until a clean restart threw the snapshot away. The rows stay in the
    /// store as dead weight; a clone never pages them again. Persists the
    /// trimmed lists. Returns the tables dropped, sorted.
    pub async fn forget_tables_not_in(&mut self, current: &BTreeSet<String>) -> Result<Vec<String>> {
        let gone: Vec<String> = self
            .known_tables
            .iter()
            .chain(self.snapshot_hashes.keys())
            .filter(|t| !current.contains(*t))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if gone.is_empty() {
            return Ok(gone);
        }
        for t in &gone {
            self.known_tables.remove(t);
            self.snapshot_hashes.remove(t);
            self.dirty_hashes.remove(t);
        }
        let seq = self.snapshot_seq;
        self.commit_snapshot_state(seq, BTreeMap::new(), BTreeSet::new())
            .await
            .context("Persist snapshot state after forgetting unsynced tables")?;
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
    ) -> (BTreeMap<String, String>, BTreeSet<String>) {
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
        hashed: BTreeMap<String, String>,
        failed: BTreeSet<String>,
    ) -> Result<()> {
        self.snapshot_seq = seq;
        self.publish_seq();

        for (table, hash) in hashed {
            self.dirty_hashes.remove(&table);
            self.snapshot_hashes.insert(table, hash);
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
            .query("UPSERT _00_metadata:snapshot SET seq = $seq, hashes = $hashes, tables = $tables, applying = false")
            .bind(("seq", seq))
            .bind(("hashes", hashes_value))
            .bind(("tables", tables_value))
            .await
            .context("Failed to persist snapshot state")?;
        self.interrupted_apply = false;
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
                    out.insert(table.clone(), h);
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
    fn omit_for(&self, table: &str) -> &BTreeSet<String> {
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
    async fn hash_one_table(&self, table: &str) -> Result<String> {
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
            return;
        };
        if let Some((id, value)) = before {
            snapshot_hash::xor_out(&mut acc, id, value);
        }
        if let Some((id, value)) = after {
            snapshot_hash::xor_in(&mut acc, id, value);
        }
        self.snapshot_hashes
            .insert(table.to_string(), snapshot_hash::xor_acc_to_hex(&acc));
    }

    /// Read combined snapshot state (seq + hashes + tables) from metadata.
    async fn read_snapshot_state_from_db(
        db: &Surreal<surrealdb::engine::local::Db>,
    ) -> Result<SnapshotState> {
        let mut response = db
            .query("SELECT seq, hashes, tables, applying FROM _00_metadata:snapshot")
            .await
            .context("Failed to query snapshot metadata")?;

        let rows: Vec<Value> = response.take(0).unwrap_or_default();
        let row = match rows.first() {
            Some(r) => r,
            None => return Ok(SnapshotState::default()),
        };

        let seq = row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
        let applying = row.get("applying").and_then(|v| v.as_bool()).unwrap_or(false);
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
        trace!("remote query: INFO FOR DB");
        let info: Value = match remote_db.query("INFO FOR DB").await {
            Ok(mut response) => {
                let v: Vec<Value> = response.take(0).unwrap_or_default();
                v.into_iter().next().unwrap_or_default()
            }
            Err(e) if is_missing_error(&e) => {
                debug!("INFO FOR DB on missing database — treating as empty");
                Value::Null
            }
            Err(e) => return Err(anyhow::Error::from(e).context("Failed to query INFO FOR DB on remote")),
        };

        Ok(match info.get("tables") {
            Some(Value::Object(tables_map)) => tables_map
                .iter()
                .filter(|(name, _)| !ssp_protocol::table_excluded_from_sync(name))
                .filter(|(name, def)| {
                    let nosync = def
                        .as_str()
                        .map(ssp_protocol::define_str_is_nosync)
                        .unwrap_or(false);
                    if nosync {
                        info!(table = %name, "Excluding @nosync table from snapshot");
                    }
                    !nosync
                })
                .map(|(name, _)| name.clone())
                .collect(),
            _ => Vec::new(),
        })
    }

    /// Per-table opaque-field sets, read from upstream `INFO FOR TABLE`.
    ///
    /// Best-effort per table: a failed `INFO FOR TABLE` yields no exclusions for
    /// that table rather than aborting the clone. The cost of missing one is a
    /// hash mismatch that the existing verify/re-clone machinery already
    /// handles; the cost of aborting is no replica at all.
    async fn discover_opaque_fields<C>(
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
    async fn page_table<C, F, Fut>(
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
                    let mut response = self
                        .db
                        .query(format!("UPDATE {} MERGE $data RETURN AFTER", thing_id))
                        .bind(("data", data))
                        .await
                        .with_context(|| format!("UPDATE {} send failed", thing_id))?
                        .check()
                        .with_context(|| format!("UPDATE {} returned a statement error", thing_id))?;
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
            } else {
                self.fold_hash_delta(table, before.as_ref(), after.as_ref());
            }
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
        self.opaque_fields.clear();
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

        let tables: Vec<String> = match info.get("tables") {
            Some(Value::Object(tables_map)) => tables_map
                .iter()
                .filter(|(name, _)| !ssp_protocol::table_excluded_from_sync(name))
                .filter(|(_, def)| {
                    !def.as_str()
                        .map(ssp_protocol::define_str_is_nosync)
                        .unwrap_or(false)
                })
                .map(|(name, _)| name.clone())
                .collect(),
            _ => Vec::new(),
        };

        for t in &tables {
            self.known_tables.insert(t.clone());
        }

        // A restored dump is an export of the MAIN database, so it carries the
        // user's `DEFINE FIELD` statements — including the opaque markers. That
        // makes the replica itself a usable source here, unlike on the clone
        // path where the replica is schemaless.
        let discovered = Self::discover_opaque_fields(&self.db, &tables).await;
        self.opaque_fields = discovered;

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

    /// A table that turned `@nosync` (or was dropped) after the snapshot was
    /// persisted must lose its hash and its known-table entry: the SSP leaves
    /// it out of its bootstrap and would dispute the hash forever.
    #[tokio::test]
    async fn forget_tables_not_in_drops_hashes_and_persists() -> Result<()> {
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

        let current: BTreeSet<String> = ["user".to_string()].into_iter().collect();
        let gone = replica.forget_tables_not_in(&current).await?;
        assert_eq!(gone, vec!["analysis".to_string()]);
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
        assert!(replica.forget_tables_not_in(&current).await?.is_empty());
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
}
