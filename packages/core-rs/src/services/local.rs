//! The sqlite-backed local store (Dart `LocalDatabaseService`), the holder that
//! swaps it per bucket (`LocalStoreHolder`) and the [`LocalPort`] the runtime
//! drives (`LocalStoreAdapter`).
//!
//! Domain records are stored document-style as JSON in a single `records`
//! table partitioned by logical table name, so arbitrary records round-trip.
//! Dedicated tables hold query state, the circuit snapshot, the pending
//! mutation outbox and the schema hash. The tables and SQL are the Dart ones,
//! byte for byte, so a store written by one client opens in the other. The
//! materialization read path resolves result sets from the circuit's
//! `localArray` via [`LocalDatabaseService::get_by_id`] rather than executing
//! SurrealQL (sqlite cannot run SurrealQL).

use std::cell::{Cell, Ref, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, OptionalExtension};

use crate::error::{Error, Result};
use crate::kernel::ports::{LocalOp, LocalPort, WriteMode};
use crate::services::column_reset::{changed_tables, same_column_sets, COLUMN_SETS_KEY};
use crate::types::StoreType;
use crate::value::{deep_merge, table_of, Row, Value};

/// `(record id, _00_rv)` pairs per table, as [`LocalDatabaseService::scan_versions`]
/// reads them.
pub type TableVersions = BTreeMap<String, Vec<(String, i64)>>;

/// The file a bucket's store lives in: `spooky.db` -> `spooky.<bucket>.db`, a
/// name with no extension gets `.<bucket>` appended (Dart `bucketPath`).
///
/// Only the file name is split, so a dot in a parent directory never decides
/// where the bucket goes.
pub fn bucket_path(base: &Path, bucket: &str) -> PathBuf {
    let name = base
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let file = match name.rfind('.') {
        Some(dot) if dot > 0 => format!("{}.{bucket}{}", &name[..dot], &name[dot..]),
        _ => format!("{name}.{bucket}"),
    };
    base.with_file_name(file)
}

/// A JSON document as stored: [`Value::to_json`] (records as `table:id`,
/// datetimes ISO-8601, durations their text), so a stored doc round-trips to
/// the same string values the typed models expect.
fn encode_doc(doc: &Row) -> String {
    serde_json::Value::Object(
        doc.iter()
            .filter(|(_, v)| !v.is_none())
            .map(|(k, v)| (k.clone(), v.to_json()))
            .collect(),
    )
    .to_string()
}

fn decode_doc(raw: &str) -> Result<Row> {
    let json: serde_json::Value = serde_json::from_str(raw)?;
    match Value::from(json) {
        Value::Object(o) => Ok(o),
        other => Err(Error::Local(
            format!("stored document is not an object: {}", other.to_json()).into(),
        )),
    }
}

/// A number field as an integer, the way Dart's `(x as num?)?.toInt()` reads it.
fn int_field(doc: &Row, key: &str) -> Option<i64> {
    doc.get(key).and_then(Value::as_f64).map(|f| f as i64)
}

fn with_id(mut doc: Row, id: &str) -> Row {
    if doc.get("id").is_none_or(Value::is_nullish) {
        doc.insert("id".into(), Value::String(id.to_string()));
    }
    doc
}

/// Commits on [`TxGuard::commit`], rolls back when dropped without one (an
/// error or a panic in the body).
struct TxGuard<'a> {
    db: &'a LocalDatabaseService,
    done: bool,
}

impl TxGuard<'_> {
    fn commit(mut self) -> Result<()> {
        // A failed COMMIT leaves `done` unset, so the drop rolls back.
        self.db.conn()?.execute_batch("COMMIT")?;
        self.done = true;
        Ok(())
    }
}

impl Drop for TxGuard<'_> {
    fn drop(&mut self) {
        if !self.done {
            if let Ok(conn) = self.db.conn() {
                let _ = conn.execute_batch("ROLLBACK");
            }
        }
    }
}

/// The sqlite-backed local store, replacing the JS `@surrealdb/wasm` local
/// engine.
pub struct LocalDatabaseService {
    /// `None` once closed: a handle captured before a bucket switch fails
    /// instead of writing into the store that replaced it.
    conn: RefCell<Option<Connection>>,
}

const SNAPSHOT_ID: &str = "circuit";

impl LocalDatabaseService {
    pub fn new(conn: Connection) -> Self {
        LocalDatabaseService {
            conn: RefCell::new(Some(conn)),
        }
    }

    /// Open a database: in memory for [`StoreType::Memory`] (the JS `memory`
    /// store), else the file at `path`, falling back to the store's own path.
    pub fn open(store: &StoreType, path: Option<&Path>) -> Result<Self> {
        match store {
            StoreType::Memory => Self::open_memory(),
            StoreType::File(base) => Self::open_file(path.unwrap_or(base)),
        }
    }

    pub fn open_memory() -> Result<Self> {
        Ok(Self::new(Connection::open_in_memory()?))
    }

    pub fn open_file(path: &Path) -> Result<Self> {
        Ok(Self::new(Connection::open(path)?))
    }

    fn conn(&self) -> Result<Ref<'_, Connection>> {
        Ref::filter_map(self.conn.borrow(), Option::as_ref)
            .map_err(|_| Error::Local("the local store is closed".into()))
    }

    pub fn is_closed(&self) -> bool {
        self.conn.borrow().is_none()
    }

    /// Close the database. Later calls fail. Idempotent.
    pub fn close(&self) {
        if let Ok(mut slot) = self.conn.try_borrow_mut() {
            slot.take();
        }
    }

    /// Create the system tables. Idempotent.
    pub fn provision(&self) -> Result<()> {
        self.conn()?.execute_batch(
            "CREATE TABLE IF NOT EXISTS records (
        tbl TEXT NOT NULL,
        id  TEXT NOT NULL,
        doc TEXT NOT NULL,
        rv  INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (tbl, id)
      );
      CREATE INDEX IF NOT EXISTS idx_records_tbl ON records(tbl);
      CREATE TABLE IF NOT EXISTS _00_query (
        id  TEXT PRIMARY KEY,
        doc TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _00_pending_mutations (
        id  TEXT PRIMARY KEY,
        doc TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _00_schema (
        hash       TEXT PRIMARY KEY,
        created_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _00_snapshot (
        id    TEXT PRIMARY KEY,
        bytes BLOB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _00_kv (
        id    TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );",
        )?;
        tracing::debug!("Provisioned local sqlite store");
        Ok(())
    }

    fn select_docs(&self, sql: &str, params: impl rusqlite::Params) -> Result<Vec<Row>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare_cached(sql)?;
        let raws: Vec<String> = stmt
            .query_map(params, |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        raws.iter().map(|raw| decode_doc(raw)).collect()
    }

    fn select_strings(&self, sql: &str) -> Result<Vec<String>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare_cached(sql)?;
        let rows: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    fn select_doc(&self, sql: &str, params: impl rusqlite::Params) -> Result<Option<Row>> {
        let conn = self.conn()?;
        let raw: Option<String> = conn
            .prepare_cached(sql)?
            .query_row(params, |r| r.get(0))
            .optional()?;
        raw.as_deref().map(decode_doc).transpose()
    }

    fn execute(&self, sql: &str, params: impl rusqlite::Params) -> Result<()> {
        self.conn()?.prepare_cached(sql)?.execute(params)?;
        Ok(())
    }

    // ---- record CRUD (logical-table aware) -----------------------------------

    /// `SELECT * FROM ONLY $id`: the full record, or `None`.
    pub fn get_by_id(&self, id: &str) -> Result<Option<Row>> {
        self.select_doc(
            "SELECT doc FROM records WHERE tbl = ? AND id = ?",
            params![table_of(id), id],
        )
    }

    /// `SELECT * FROM <table>`: all records of the logical table.
    pub fn get_all(&self, table: &str) -> Result<Vec<Row>> {
        self.select_docs("SELECT doc FROM records WHERE tbl = ?", params![table])
    }

    /// `CREATE ONLY $id CONTENT $doc`.
    pub fn create(&self, id: &str, mut doc: Row) -> Result<()> {
        doc.insert("id".into(), Value::String(id.to_string()));
        self.write_doc(id, &doc)
    }

    /// `UPSERT ONLY $id REPLACE $doc` (rollback restore).
    pub fn replace(&self, id: &str, doc: Row) -> Result<()> {
        self.create(id, doc)
    }

    /// `UPSERT ONLY $id MERGE $patch`: deep-merge into the existing doc
    /// (creating it if absent), preserving keys the patch omits (e.g.
    /// `_00_crdt`). The top-level keys in `clear` are then removed: fields
    /// cleared upstream, which SurrealDB's MERGE removes when the patch holds
    /// NONE for them.
    pub fn upsert_merge(&self, id: &str, patch: &Row, clear: &[String]) -> Result<()> {
        let mut merged = match self.get_by_id(id)? {
            Some(existing) => existing,
            None => Row::from([("id".to_string(), Value::String(id.to_string()))]),
        };
        deep_merge(&mut merged, patch);
        for key in clear {
            if key != "id" {
                merged.remove(key);
            }
        }
        merged.insert("id".into(), Value::String(id.to_string()));
        self.write_doc(id, &merged)
    }

    /// `UPDATE ONLY $id MERGE $patch`: like [`Self::upsert_merge`] but a no-op
    /// when the row is absent.
    pub fn update_merge(&self, id: &str, patch: &Row) -> Result<()> {
        let Some(mut merged) = self.get_by_id(id)? else {
            return Ok(());
        };
        deep_merge(&mut merged, patch);
        merged.insert("id".into(), Value::String(id.to_string()));
        self.write_doc(id, &merged)
    }

    /// `UPDATE $id SET <fields>`: shallow field assignment.
    pub fn update_set(&self, id: &str, fields: &Row) -> Result<()> {
        let Some(mut updated) = self.get_by_id(id)? else {
            return Ok(());
        };
        updated.extend(fields.iter().map(|(k, v)| (k.clone(), v.clone())));
        updated.insert("id".into(), Value::String(id.to_string()));
        self.write_doc(id, &updated)
    }

    /// `_00_rv += 1`.
    pub fn increment_rv(&self, id: &str) -> Result<()> {
        let Some(mut existing) = self.get_by_id(id)? else {
            return Ok(());
        };
        let rv = int_field(&existing, "_00_rv").unwrap_or(0) + 1;
        existing.insert("_00_rv".into(), Value::Int(rv));
        self.write_doc(id, &existing)
    }

    /// `DELETE $id`.
    pub fn delete(&self, id: &str) -> Result<()> {
        self.execute(
            "DELETE FROM records WHERE tbl = ? AND id = ?",
            params![table_of(id), id],
        )
    }

    fn write_doc(&self, id: &str, doc: &Row) -> Result<()> {
        let rv = int_field(doc, "_00_rv").unwrap_or(1);
        self.execute(
            "INSERT INTO records (tbl, id, doc, rv) VALUES (?, ?, ?, ?) \
             ON CONFLICT(tbl, id) DO UPDATE SET doc = excluded.doc, rv = excluded.rv",
            params![table_of(id), id, encode_doc(doc), rv],
        )
    }

    // ---- generic document access ----------------------------------------------
    //
    // The saga core addresses the store as a document store by `(table, id)`.
    // Domain rows and the internal tables that have no dedicated sqlite table
    // (`_00_view`, `_00_window`, `_00_failed_mutations`) live in `records`;
    // `_00_query` and `_00_pending_mutations` keep the dedicated tables they
    // already had, so an existing local database keeps working.

    fn is_query_registry(table: &str) -> bool {
        table == "_00_query"
    }

    fn is_outbox(table: &str) -> bool {
        table == "_00_pending_mutations"
    }

    pub fn get_doc(&self, table: &str, id: &str) -> Result<Option<Row>> {
        if Self::is_query_registry(table) {
            return Ok(self.get_query_config(id)?.map(|d| with_id(d, id)));
        }
        if Self::is_outbox(table) {
            let doc = self.select_doc(
                "SELECT doc FROM _00_pending_mutations WHERE id = ?",
                params![id],
            )?;
            return Ok(doc.map(|d| with_id(d, id)));
        }
        self.get_by_id(id)
    }

    /// Many rows by id, in the order asked. Ids the store does not hold are
    /// dropped: this is the materialization read.
    pub fn get_docs(&self, table: &str, ids: &[String]) -> Result<Vec<Row>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(doc) = self.get_doc(table, id)? {
                out.push(doc);
            }
        }
        Ok(out)
    }

    pub fn get_all_docs(&self, table: &str) -> Result<Vec<Row>> {
        if Self::is_query_registry(table) {
            return self.get_all_query_configs();
        }
        if Self::is_outbox(table) {
            let conn = self.conn()?;
            let mut stmt = conn.prepare_cached("SELECT id, doc FROM _00_pending_mutations")?;
            let rows: Vec<(String, String)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?;
            return rows
                .iter()
                .map(|(id, raw)| decode_doc(raw).map(|d| with_id(d, id)))
                .collect();
        }
        self.get_all(table)
    }

    pub fn put_doc(
        &self,
        table: &str,
        id: &str,
        data: Row,
        merge: bool,
        clear: &[String],
    ) -> Result<()> {
        if Self::is_query_registry(table) {
            return if merge {
                self.patch_query_config(id, &data)
            } else {
                self.put_query_config(id, &data)
            };
        }
        if Self::is_outbox(table) {
            return self.put_mutation(id, &data);
        }
        if merge {
            self.upsert_merge(id, &data, clear)
        } else {
            self.replace(id, data)
        }
    }

    pub fn delete_doc(&self, table: &str, id: &str) -> Result<()> {
        if Self::is_query_registry(table) {
            return self.delete_query_config(id);
        }
        if Self::is_outbox(table) {
            return self.delete_mutation(id);
        }
        self.delete(id)
    }

    pub fn bump_rv(&self, _table: &str, id: &str) -> Result<()> {
        self.increment_rv(id)
    }

    /// Every stored row's `(id, _00_rv)` per table, for the boot-time circuit
    /// prime. Rows are read by their index, not decoded, so this stays cheap on
    /// a warm store.
    pub fn scan_versions(&self, tables: &[String]) -> Result<TableVersions> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare_cached("SELECT id, rv FROM records WHERE tbl = ?")?;
        let mut out = TableVersions::new();
        for table in tables {
            let rows: Vec<(String, i64)> = stmt
                .query_map(params![table], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?;
            out.insert(table.clone(), rows);
        }
        Ok(out)
    }

    /// Reset the cached `_00_rv` of every table whose column set changed since
    /// it was last recorded, except the rows in `keep`, and record
    /// `fingerprints`. Returns the tables reset. See
    /// [`crate::services::column_reset`].
    pub fn reset_changed_tables(
        &self,
        fingerprints: &BTreeMap<String, String>,
        keep: &HashSet<String>,
    ) -> Result<Vec<String>> {
        let stored: Option<BTreeMap<String, String>> = self
            .kv_get(COLUMN_SETS_KEY)?
            .and_then(|raw| serde_json::from_str(&raw).ok());
        if same_column_sets(stored.as_ref(), fingerprints) {
            return Ok(Vec::new());
        }

        let with_rows: HashSet<String> = self
            .select_strings("SELECT DISTINCT tbl FROM records")?
            .into_iter()
            .collect();
        let changed = changed_tables(stored.as_ref(), fingerprints, &with_rows);
        self.tx(|| {
            for table in &changed {
                let kept: Vec<&str> = keep
                    .iter()
                    .filter(|id| table_of(id) == table)
                    .map(String::as_str)
                    .collect();
                self.execute(
                    "UPDATE records SET rv = 0, doc = json_set(doc, '$._00_rv', 0) \
                     WHERE tbl = ? AND rv != 0 \
                     AND id NOT IN (SELECT value FROM json_each(?))",
                    params![table, serde_json::to_string(&kept)?],
                )?;
            }
            self.kv_set(COLUMN_SETS_KEY, &serde_json::to_string(fingerprints)?)
        })?;
        Ok(changed)
    }

    // ---- circuit snapshot ------------------------------------------------------

    /// The stored circuit snapshot, or `None` when there is none.
    pub fn get_snapshot(&self) -> Result<Option<Vec<u8>>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare_cached("SELECT bytes FROM _00_snapshot WHERE id = ?")?;
        let bytes = stmt
            .query_row(params![SNAPSHOT_ID], |r| {
                Ok(match r.get_ref(0)? {
                    ValueRef::Blob(b) => Some(b.to_vec()),
                    _ => None,
                })
            })
            .optional()?;
        Ok(bytes.flatten())
    }

    pub fn put_snapshot(&self, bytes: &[u8]) -> Result<()> {
        self.execute(
            "INSERT INTO _00_snapshot (id, bytes) VALUES (?, ?) \
             ON CONFLICT(id) DO UPDATE SET bytes = excluded.bytes",
            params![SNAPSHOT_ID, bytes],
        )
    }

    pub fn clear_snapshot(&self) -> Result<()> {
        self.execute(
            "DELETE FROM _00_snapshot WHERE id = ?",
            params![SNAPSHOT_ID],
        )
    }

    // ---- _00_query registry --------------------------------------------------

    pub fn get_query_config(&self, id: &str) -> Result<Option<Row>> {
        self.select_doc("SELECT doc FROM _00_query WHERE id = ?", params![id])
    }

    pub fn get_all_query_configs(&self) -> Result<Vec<Row>> {
        self.select_docs("SELECT doc FROM _00_query", [])
    }

    pub fn put_query_config(&self, id: &str, doc: &Row) -> Result<()> {
        self.execute(
            "INSERT INTO _00_query (id, doc) VALUES (?, ?) \
             ON CONFLICT(id) DO UPDATE SET doc = excluded.doc",
            params![id, encode_doc(doc)],
        )
    }

    /// Shallow-merge `fields` into a stored registration; a no-op when absent.
    pub fn patch_query_config(&self, id: &str, fields: &Row) -> Result<()> {
        let Some(mut existing) = self.get_query_config(id)? else {
            return Ok(());
        };
        existing.extend(fields.iter().map(|(k, v)| (k.clone(), v.clone())));
        self.put_query_config(id, &existing)
    }

    pub fn delete_query_config(&self, id: &str) -> Result<()> {
        self.execute("DELETE FROM _00_query WHERE id = ?", params![id])
    }

    // ---- pending mutations ----------------------------------------------------

    pub fn put_mutation(&self, id: &str, doc: &Row) -> Result<()> {
        self.execute(
            "INSERT INTO _00_pending_mutations (id, doc) VALUES (?, ?) \
             ON CONFLICT(id) DO UPDATE SET doc = excluded.doc",
            params![id, encode_doc(doc)],
        )
    }

    pub fn delete_mutation(&self, id: &str) -> Result<()> {
        self.execute(
            "DELETE FROM _00_pending_mutations WHERE id = ?",
            params![id],
        )
    }

    /// All pending mutations ordered by their stored `created_at` (oldest
    /// first).
    pub fn get_all_mutations(&self) -> Result<Vec<Row>> {
        let mut docs = self.select_docs("SELECT doc FROM _00_pending_mutations", [])?;
        let created = |d: &Row| {
            d.get("created_at")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        docs.sort_by_key(created);
        Ok(docs)
    }

    // ---- legacy stream processor state ---------------------------------------

    /// Remove the full-circuit JSON older clients persisted (a dedicated table
    /// and a `_00_kv` row). Only the store snapshot is used now.
    pub fn drop_legacy_stream_state(&self) -> Result<()> {
        self.conn()?
            .execute_batch("DROP TABLE IF EXISTS _00_stream_processor_state")?;
        self.kv_remove("_00_stream_processor_state")
    }

    // ---- generic key-value (backs the sqlite persistence) ---------------------

    pub fn kv_get(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn()?;
        let value = conn
            .prepare_cached("SELECT value FROM _00_kv WHERE id = ?")?
            .query_row(params![key], |r| r.get(0))
            .optional()?;
        Ok(value)
    }

    pub fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        self.execute(
            "INSERT INTO _00_kv (id, value) VALUES (?, ?) \
             ON CONFLICT(id) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
    }

    pub fn kv_remove(&self, key: &str) -> Result<()> {
        self.execute("DELETE FROM _00_kv WHERE id = ?", params![key])
    }

    // ---- schema hash ----------------------------------------------------------

    pub fn latest_schema_hash(&self) -> Result<Option<String>> {
        let conn = self.conn()?;
        let hash = conn
            .prepare_cached("SELECT hash FROM _00_schema ORDER BY created_at DESC LIMIT 1")?
            .query_row([], |r| r.get(0))
            .optional()?;
        Ok(hash)
    }

    pub fn record_schema_hash(&self, hash: &str, created_at_iso: &str) -> Result<()> {
        self.execute(
            "INSERT INTO _00_schema (hash, created_at) VALUES (?, ?) \
             ON CONFLICT(hash) DO UPDATE SET created_at = excluded.created_at",
            params![hash, created_at_iso],
        )
    }

    // ---- migration ------------------------------------------------------------

    /// Wipe cached domain data, query registrations, the outbox and the circuit
    /// snapshot. Preserves the schema-hash table and the `_00_kv` rows (the
    /// auth token among them).
    pub fn reset_local_data(&self) -> Result<()> {
        self.conn()?.execute_batch(
            "DELETE FROM records; DELETE FROM _00_query; \
             DELETE FROM _00_pending_mutations; DELETE FROM _00_snapshot;",
        )?;
        self.drop_legacy_stream_state()
    }

    // ---- transactions ---------------------------------------------------------

    /// Run `body` in one sqlite transaction: committed when it returns `Ok`,
    /// rolled back on an error or a panic. Not reentrant.
    pub fn tx<T>(&self, body: impl FnOnce() -> Result<T>) -> Result<T> {
        self.conn()?.execute_batch("BEGIN")?;
        let guard = TxGuard {
            db: self,
            done: false,
        };
        let result = body()?;
        guard.commit()?;
        Ok(result)
    }
}

// ---- the holder ----------------------------------------------------------------------

/// Holds the local store so a bucket switch can replace it under the adapters
/// without rebuilding them.
pub struct LocalStoreHolder {
    store: StoreType,
    db: RefCell<Option<Rc<LocalDatabaseService>>>,
    bucket_id: RefCell<String>,
    epoch: Cell<u64>,
}

impl LocalStoreHolder {
    pub fn new(store: StoreType) -> Self {
        LocalStoreHolder {
            store,
            db: RefCell::new(None),
            bucket_id: RefCell::new(String::new()),
            epoch: Cell::new(0),
        }
    }

    pub fn store(&self) -> &StoreType {
        &self.store
    }

    /// The open store. Do not keep it across a bucket switch: the switch closes
    /// it, and a kept handle then fails rather than writing into the wrong
    /// bucket.
    pub fn db(&self) -> Result<Rc<LocalDatabaseService>> {
        self.db
            .borrow()
            .clone()
            .ok_or_else(|| Error::Local("The local store is not open yet".into()))
    }

    pub fn is_open(&self) -> bool {
        self.db.borrow().is_some()
    }

    pub fn bucket_id(&self) -> String {
        self.bucket_id.borrow().clone()
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.get()
    }

    /// The file a bucket's store lives in.
    ///
    /// Every bucket gets its own file, including the anonymous one: a single
    /// shared file is how one user's cached rows end up readable by the next.
    /// An in-memory store needs no separation: reopening it IS a fresh
    /// database.
    pub fn path_for(&self, bucket_id: &str) -> Option<PathBuf> {
        match &self.store {
            StoreType::Memory => None,
            StoreType::File(base) => Some(bucket_path(base, bucket_id)),
        }
    }

    /// Close the open store (if any) and open `bucket_id`'s, provisioned. The
    /// epoch is bumped so writes fenced on the old one are dropped.
    pub fn connect(&self, bucket_id: &str) -> Result<()> {
        if let Some(old) = self.db.borrow_mut().take() {
            old.close();
        }
        *self.bucket_id.borrow_mut() = bucket_id.to_string();
        let db = LocalDatabaseService::open(&self.store, self.path_for(bucket_id).as_deref())?;
        db.provision()?;
        *self.db.borrow_mut() = Some(Rc::new(db));
        self.epoch.set(self.epoch.get() + 1);
        tracing::debug!("Local store open for bucket {bucket_id}");
        Ok(())
    }

    /// Close the open store, if any.
    pub fn close(&self) {
        if let Some(old) = self.db.borrow_mut().take() {
            old.close();
        }
    }
}

// ---- the port ------------------------------------------------------------------------

/// The local document store, as the runtime sees it.
pub struct LocalStoreAdapter {
    holder: Rc<LocalStoreHolder>,
}

impl LocalStoreAdapter {
    pub fn new(holder: Rc<LocalStoreHolder>) -> Self {
        LocalStoreAdapter { holder }
    }

    pub fn holder(&self) -> &Rc<LocalStoreHolder> {
        &self.holder
    }
}

impl LocalPort for LocalStoreAdapter {
    fn get(&self, table: &str, id: &str) -> Result<Option<Row>> {
        self.holder.db()?.get_doc(table, id)
    }

    fn get_many(&self, table: &str, ids: &[String]) -> Result<Vec<Row>> {
        self.holder.db()?.get_docs(table, ids)
    }

    fn get_all(&self, table: &str) -> Result<Vec<Row>> {
        self.holder.db()?.get_all_docs(table)
    }

    fn put(&self, table: &str, id: &str, data: Row, mode: WriteMode) -> Result<()> {
        self.holder
            .db()?
            .put_doc(table, id, data, mode == WriteMode::Merge, &[])
    }

    fn delete(&self, table: &str, id: &str) -> Result<()> {
        self.holder.db()?.delete_doc(table, id)
    }

    fn tx(&self, ops: Vec<LocalOp>) -> Result<()> {
        let db = self.holder.db()?;
        db.tx(|| {
            for op in ops {
                match op {
                    LocalOp::Put {
                        table,
                        id,
                        data,
                        mode,
                        clear,
                    } => db.put_doc(&table, &id, data, mode == WriteMode::Merge, &clear)?,
                    LocalOp::Delete { table, id } => db.delete_doc(&table, &id)?,
                    LocalOp::BumpRv { table, id } => db.bump_rv(&table, &id)?,
                }
            }
            Ok(())
        })
    }

    fn epoch(&self) -> u64 {
        self.holder.epoch()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::row;
    use serde_json::json;

    fn db() -> LocalDatabaseService {
        let db = LocalDatabaseService::open_memory().unwrap();
        db.provision().unwrap();
        db
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn bucket_paths() {
        assert_eq!(
            bucket_path(Path::new("spooky.db"), "anon"),
            PathBuf::from("spooky.anon.db")
        );
        assert_eq!(
            bucket_path(Path::new("/a/b/cache.db"), "u1"),
            PathBuf::from("/a/b/cache.u1.db")
        );
        assert_eq!(
            bucket_path(Path::new("/a.b/store"), "u1"),
            PathBuf::from("/a.b/store.u1")
        );
        assert_eq!(
            bucket_path(Path::new(".hidden"), "x"),
            PathBuf::from(".hidden.x")
        );
    }

    // ---- services_test.dart: LocalDatabaseService (sqlite) ----

    #[test]
    fn create_get_by_id_get_all() {
        let db = db();
        db.create("thread:a", row(json!({"title": "hello", "_00_rv": 1})))
            .unwrap();
        let got = db.get_by_id("thread:a").unwrap().unwrap();
        assert_eq!(got["title"], Value::from("hello"));
        assert_eq!(got["id"], Value::from("thread:a"));
        assert_eq!(db.get_all("thread").unwrap().len(), 1);
    }

    #[test]
    fn upsert_merge_preserves_omitted_local_only_fields() {
        let db = db();
        db.create(
            "thread:a",
            row(json!({"title": "hello", "_00_crdt": "blob", "_00_rv": 1})),
        )
        .unwrap();
        db.upsert_merge(
            "thread:a",
            &row(json!({"title": "updated", "_00_rv": 2})),
            &[],
        )
        .unwrap();
        let got = db.get_by_id("thread:a").unwrap().unwrap();
        assert_eq!(got["title"], Value::from("updated"));
        assert_eq!(got["_00_crdt"], Value::from("blob"));
    }

    #[test]
    fn upsert_merge_removes_cleared_keys_and_keeps_local_only_fields() {
        let db = db();
        db.create(
            "thread:a",
            row(json!({"title": "hello", "club": "club:1", "_00_crdt": "blob", "_00_rv": 1})),
        )
        .unwrap();
        db.upsert_merge(
            "thread:a",
            &row(json!({"title": "updated", "_00_rv": 2})),
            &s(&["club", "id"]),
        )
        .unwrap();
        let got = db.get_by_id("thread:a").unwrap().unwrap();
        assert!(!got.contains_key("club"), "cleared upstream");
        assert_eq!(got["title"], Value::from("updated"));
        assert_eq!(got["_00_crdt"], Value::from("blob"));
        assert_eq!(got["id"], Value::from("thread:a"), "id is never cleared");
    }

    #[test]
    fn put_doc_merge_forwards_clear_replace_ignores_it() {
        let db = db();
        db.create(
            "thread:a",
            row(json!({"title": "x", "club": "club:1", "_00_rv": 1})),
        )
        .unwrap();
        db.put_doc(
            "thread",
            "thread:a",
            row(json!({"title": "y"})),
            true,
            &s(&["club"]),
        )
        .unwrap();
        assert!(!db
            .get_by_id("thread:a")
            .unwrap()
            .unwrap()
            .contains_key("club"));
        db.put_doc(
            "thread",
            "thread:a",
            row(json!({"title": "z", "club": "club:2"})),
            false,
            &s(&["club"]),
        )
        .unwrap();
        assert_eq!(
            db.get_by_id("thread:a").unwrap().unwrap()["club"],
            Value::from("club:2")
        );
    }

    #[test]
    fn increment_rv_and_delete() {
        let db = db();
        db.create("thread:a", row(json!({"title": "x", "_00_rv": 1})))
            .unwrap();
        db.increment_rv("thread:a").unwrap();
        assert_eq!(
            db.get_by_id("thread:a").unwrap().unwrap()["_00_rv"],
            Value::Int(2)
        );
        assert_eq!(
            db.scan_versions(&s(&["thread"])).unwrap()["thread"],
            vec![("thread:a".to_string(), 2)]
        );
        db.delete("thread:a").unwrap();
        assert!(db.get_by_id("thread:a").unwrap().is_none());
    }

    #[test]
    fn update_merge_and_set_are_no_ops_when_absent() {
        let db = db();
        db.update_merge("thread:x", &row(json!({"title": "y"})))
            .unwrap();
        db.update_set("thread:x", &row(json!({"title": "y"})))
            .unwrap();
        assert!(db.get_by_id("thread:x").unwrap().is_none());

        db.create(
            "thread:a",
            row(json!({"meta": {"a": 1, "b": 2}, "_00_rv": 1})),
        )
        .unwrap();
        db.update_merge("thread:a", &row(json!({"meta": {"b": 3}})))
            .unwrap();
        assert_eq!(
            db.get_by_id("thread:a").unwrap().unwrap()["meta"],
            Value::from(json!({"a": 1, "b": 3}))
        );
        db.update_set("thread:a", &row(json!({"meta": {"c": 4}})))
            .unwrap();
        assert_eq!(
            db.get_by_id("thread:a").unwrap().unwrap()["meta"],
            Value::from(json!({"c": 4}))
        );
    }

    #[test]
    fn query_config_round_trip() {
        let db = db();
        db.put_query_config(
            "_00_query:h",
            &row(json!({"surql": "SELECT * FROM thread"})),
        )
        .unwrap();
        assert_eq!(
            db.get_query_config("_00_query:h").unwrap().unwrap()["surql"],
            Value::from("SELECT * FROM thread")
        );
        db.patch_query_config("_00_query:h", &row(json!({"ttl": "10m"})))
            .unwrap();
        let got = db.get_doc("_00_query", "_00_query:h").unwrap().unwrap();
        assert_eq!(got["ttl"], Value::from("10m"));
        assert_eq!(
            got["id"],
            Value::from("_00_query:h"),
            "the registry doc gains its id"
        );
        db.patch_query_config("_00_query:none", &row(json!({"ttl": "1m"})))
            .unwrap();
        assert!(db.get_query_config("_00_query:none").unwrap().is_none());
    }

    #[test]
    fn legacy_full_circuit_state_is_dropped() {
        let db = db();
        db.kv_set("_00_stream_processor_state", &"x".repeat(1024))
            .unwrap();
        db.drop_legacy_stream_state().unwrap();
        assert!(db.kv_get("_00_stream_processor_state").unwrap().is_none());
        db.drop_legacy_stream_state().unwrap();
    }

    #[test]
    fn tx_rolls_back_on_error() {
        let db = db();
        let res: Result<()> = db.tx(|| {
            db.create("thread:a", row(json!({"title": "x", "_00_rv": 1})))?;
            Err(Error::other("boom"))
        });
        assert!(res.is_err());
        assert!(db.get_by_id("thread:a").unwrap().is_none());
        // The connection is usable again (no transaction left open).
        db.tx(|| db.create("thread:b", Row::new())).unwrap();
        assert!(db.get_by_id("thread:b").unwrap().is_some());
    }

    #[test]
    fn tx_rolls_back_on_panic() {
        let db = db();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = db.tx(|| -> Result<()> {
                db.create("thread:a", Row::new())?;
                panic!("boom");
            });
        }));
        assert!(caught.is_err());
        assert!(db.get_by_id("thread:a").unwrap().is_none());
        db.tx(|| db.create("thread:b", Row::new())).unwrap();
    }

    #[test]
    fn mutations_are_ordered_by_created_at() {
        let db = db();
        db.put_mutation(
            "_00_pending_mutations:2",
            &row(json!({"created_at": "2026-01-02", "n": 2})),
        )
        .unwrap();
        db.put_mutation(
            "_00_pending_mutations:1",
            &row(json!({"created_at": "2026-01-01", "n": 1})),
        )
        .unwrap();
        let all = db.get_all_mutations().unwrap();
        assert_eq!(
            all.iter()
                .map(|m| m["n"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let docs = db.get_all_docs("_00_pending_mutations").unwrap();
        assert!(docs.iter().all(|d| d.contains_key("id")));
    }

    #[test]
    fn snapshot_round_trip() {
        let db = db();
        assert!(db.get_snapshot().unwrap().is_none());
        db.put_snapshot(&[1, 2, 3]).unwrap();
        assert_eq!(db.get_snapshot().unwrap(), Some(vec![1, 2, 3]));
        db.put_snapshot(&[4]).unwrap();
        assert_eq!(db.get_snapshot().unwrap(), Some(vec![4]));
        db.clear_snapshot().unwrap();
        assert!(db.get_snapshot().unwrap().is_none());
    }

    #[test]
    fn typed_values_are_stored_as_their_json_spelling() {
        let db = db();
        let mut doc = Row::new();
        doc.insert(
            "author".into(),
            Value::RecordId(crate::value::RecordId::new("user", "k")),
        );
        doc.insert("gone".into(), Value::None);
        doc.insert("null".into(), Value::Null);
        doc.insert("bytes".into(), Value::Bytes(vec![1, 2]));
        db.create("thread:a", doc).unwrap();
        let got = db.get_by_id("thread:a").unwrap().unwrap();
        assert_eq!(got["author"], Value::from("user:k"));
        assert!(!got.contains_key("gone"));
        assert_eq!(got["null"], Value::Null);
        assert_eq!(got["bytes"], Value::from(json!([1, 2])));
        // A doc with no `_00_rv` is stored at version 1.
        assert_eq!(db.scan_versions(&s(&["thread"])).unwrap()["thread"][0].1, 1);
    }

    #[test]
    fn reset_local_data_keeps_kv_and_schema() {
        let db = db();
        db.create("thread:a", Row::new()).unwrap();
        db.put_query_config("_00_query:h", &Row::new()).unwrap();
        db.put_mutation("m", &Row::new()).unwrap();
        db.put_snapshot(&[1]).unwrap();
        db.kv_set("sp00ky_auth_token", "tok").unwrap();
        db.record_schema_hash("h", "2026-01-01T00:00:00.000Z")
            .unwrap();
        db.reset_local_data().unwrap();
        assert!(db.get_by_id("thread:a").unwrap().is_none());
        assert!(db.get_all_query_configs().unwrap().is_empty());
        assert!(db.get_all_mutations().unwrap().is_empty());
        assert!(db.get_snapshot().unwrap().is_none());
        assert_eq!(
            db.kv_get("sp00ky_auth_token").unwrap().as_deref(),
            Some("tok")
        );
        assert_eq!(db.latest_schema_hash().unwrap().as_deref(), Some("h"));
    }

    #[test]
    fn closed_store_fails_instead_of_writing() {
        let db = db();
        db.close();
        assert!(db.is_closed());
        assert!(matches!(
            db.create("thread:a", Row::new()),
            Err(Error::Local(_))
        ));
        db.close();
    }

    // ---- LocalStoreHolder / LocalStoreAdapter ----

    #[test]
    fn holder_opens_one_file_per_bucket_and_bumps_the_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let holder = Rc::new(LocalStoreHolder::new(StoreType::File(
            dir.path().join("spooky.db"),
        )));
        assert!(holder.db().is_err());
        assert_eq!(holder.epoch(), 0);

        holder.connect("anon").unwrap();
        assert_eq!(holder.epoch(), 1);
        assert_eq!(holder.bucket_id(), "anon");
        assert!(dir.path().join("spooky.anon.db").exists());
        let anon = holder.db().unwrap();
        anon.create("thread:a", row(json!({"title": "anon"})))
            .unwrap();

        holder.connect("alice").unwrap();
        assert_eq!(holder.epoch(), 2);
        assert!(dir.path().join("spooky.alice.db").exists());
        assert!(anon.is_closed(), "the old store is closed on switch");
        assert!(holder
            .db()
            .unwrap()
            .get_by_id("thread:a")
            .unwrap()
            .is_none());

        holder.connect("anon").unwrap();
        assert_eq!(
            holder.db().unwrap().get_by_id("thread:a").unwrap().unwrap()["title"],
            Value::from("anon")
        );
        assert_eq!(holder.path_for("x"), Some(dir.path().join("spooky.x.db")));
    }

    #[test]
    fn memory_holder_reconnect_is_a_fresh_database() {
        let holder = LocalStoreHolder::new(StoreType::Memory);
        assert_eq!(holder.path_for("anon"), None);
        holder.connect("anon").unwrap();
        holder.db().unwrap().create("thread:a", Row::new()).unwrap();
        holder.connect("anon").unwrap();
        assert!(holder
            .db()
            .unwrap()
            .get_by_id("thread:a")
            .unwrap()
            .is_none());
    }

    fn adapter() -> LocalStoreAdapter {
        let holder = Rc::new(LocalStoreHolder::new(StoreType::Memory));
        holder.connect("anon").unwrap();
        LocalStoreAdapter::new(holder)
    }

    #[test]
    fn adapter_tx_is_atomic() {
        let local = adapter();
        local
            .put(
                "thread",
                "thread:a",
                row(json!({"title": "a", "_00_rv": 1})),
                WriteMode::Replace,
            )
            .unwrap();
        local
            .tx(vec![
                LocalOp::delete("thread", "thread:a"),
                LocalOp::put("thread", "thread:b", row(json!({"title": "b"}))),
            ])
            .unwrap();
        assert!(local.get("thread", "thread:a").unwrap().is_none());
        assert!(local.get("thread", "thread:b").unwrap().is_some());

        // Make the last op of the next transaction fail.
        local
            .holder()
            .db()
            .unwrap()
            .conn()
            .unwrap()
            .execute_batch("DROP TABLE _00_query")
            .unwrap();
        let res = local.tx(vec![
            LocalOp::delete("thread", "thread:b"),
            LocalOp::put("thread", "thread:c", row(json!({"title": "c"}))),
            LocalOp::put("_00_query", "_00_query:y", Row::new()),
        ]);
        assert!(res.is_err());
        assert!(
            local.get("thread", "thread:b").unwrap().is_some(),
            "the delete was rolled back"
        );
        assert!(
            local.get("thread", "thread:c").unwrap().is_none(),
            "the put was rolled back"
        );
        // The store takes the next transaction normally.
        local
            .tx(vec![LocalOp::put("thread", "thread:d", Row::new())])
            .unwrap();
        assert!(local.get("thread", "thread:d").unwrap().is_some());
    }

    #[test]
    fn adapter_merge_and_clear() {
        let local = adapter();
        local
            .put(
                "thread",
                "thread:a",
                row(json!({"title": "a", "club": "club:1", "_00_crdt": "blob", "_00_rv": 1})),
                WriteMode::Replace,
            )
            .unwrap();
        local
            .tx(vec![LocalOp::merge(
                "thread",
                "thread:a",
                row(json!({"title": "b", "_00_rv": 2})),
                s(&["club"]),
            )])
            .unwrap();
        let got = local.get("thread", "thread:a").unwrap().unwrap();
        assert_eq!(got["title"], Value::from("b"));
        assert_eq!(got["_00_crdt"], Value::from("blob"));
        assert!(!got.contains_key("club"));

        // A plain merge put keeps every field the patch omits.
        local
            .put(
                "thread",
                "thread:a",
                row(json!({"title": "c"})),
                WriteMode::Merge,
            )
            .unwrap();
        let got = local.get("thread", "thread:a").unwrap().unwrap();
        assert_eq!(got["title"], Value::from("c"));
        assert_eq!(got["_00_rv"], Value::Int(2));

        local
            .tx(vec![LocalOp::BumpRv {
                table: "thread".into(),
                id: "thread:a".into(),
            }])
            .unwrap();
        assert_eq!(
            local.get("thread", "thread:a").unwrap().unwrap()["_00_rv"],
            Value::Int(3)
        );
    }

    #[test]
    fn adapter_routes_registry_and_outbox_tables() {
        let local = adapter();
        local
            .put(
                "_00_query",
                "_00_query:h",
                row(json!({"surql": "x"})),
                WriteMode::Replace,
            )
            .unwrap();
        local
            .put(
                "_00_query",
                "_00_query:h",
                row(json!({"ttl": "1m"})),
                WriteMode::Merge,
            )
            .unwrap();
        let q = local.get("_00_query", "_00_query:h").unwrap().unwrap();
        assert_eq!(q["surql"], Value::from("x"));
        assert_eq!(q["ttl"], Value::from("1m"));
        local
            .put(
                "_00_pending_mutations",
                "_00_pending_mutations:1",
                row(json!({"mutationType": "create", "recordId": "thread:d"})),
                WriteMode::Replace,
            )
            .unwrap();
        let db = local.holder().db().unwrap();
        let mutations = db.get_all_mutations().unwrap();
        assert_eq!(
            mutations.last().unwrap()["mutationType"],
            Value::from("create")
        );
        assert_eq!(
            mutations.last().unwrap()["recordId"],
            Value::from("thread:d")
        );
        assert!(
            db.get_all("_00_query").unwrap().is_empty(),
            "registry rows never land in records"
        );
        local.delete("_00_query", "_00_query:h").unwrap();
        assert!(local.get_all("_00_query").unwrap().is_empty());
        assert_eq!(
            local
                .get_many(
                    "_00_pending_mutations",
                    &s(&["_00_pending_mutations:1", "nope"])
                )
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn adapter_epoch_follows_the_holder() {
        let local = adapter();
        let first = local.epoch();
        local.holder().connect("alice").unwrap();
        assert_eq!(local.epoch(), first + 1);
    }

    #[test]
    fn adapter_get_many_keeps_order_and_drops_missing() {
        let local = adapter();
        for id in ["thread:a", "thread:b", "thread:c"] {
            local
                .put("thread", id, Row::new(), WriteMode::Replace)
                .unwrap();
        }
        let got = local
            .get_many("thread", &s(&["thread:c", "thread:x", "thread:a"]))
            .unwrap();
        let ids: Vec<String> = got.iter().map(|r| r["id"].to_id_string()).collect();
        assert_eq!(ids, s(&["thread:c", "thread:a"]));
    }
}
