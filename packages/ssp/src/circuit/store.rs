use crate::algebra::{RowKey, Weight, ZSet};
use crate::circuit::row_table::RowTable;
use crate::eval::value_ref::ValueRef;
use crate::types::{make_key, raw_id, Sp00kyValue};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

/// Fields every projected row keeps regardless of what the registered plans
/// reference: the identity and the version the sync layer keys on.
pub const ALWAYS_RETAINED: [&str; 2] = ["id", "_00_rv"];

/// What one applied mutation did to its collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    /// Full `table:id` z-set key.
    pub key: RowKey,
    /// Membership delta: `+1` a row appeared, `-1` it disappeared, `0` it
    /// stayed (content may or may not have changed, see below).
    pub weight: Weight,
    /// The stored bytes changed. `false` for a Create/Update whose canonical
    /// digest matches what is already stored (nothing was written, nothing
    /// needs re-evaluating) and for a Delete of an absent row.
    pub content_changed: bool,
}

/// A base collection (table) in the store.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Collection {
    pub name: String,
    /// Z-set tracking record membership and weights.
    pub zset: ZSet,
    /// Actual record data, keyed by raw record ID (without table prefix).
    ///
    /// Flat-encoded behind an index rather than a map of parsed values — see
    /// [`RowTable`]. Serializes to the same shape as the `HashMap` it replaced,
    /// so existing snapshots still load.
    pub rows: RowTable,
    /// Incremental XOR set-hash of `rows`, maintained in lockstep with every
    /// mutation (see `apply_mutation`) so it can never drift from the actual
    /// rows. The scheduler reconstructs the same hash at the catch-up cut to
    /// verify a rejoining SSP. Not serialized — re-seeded after a bulk load via
    /// [`reseed_catchup_xor`].
    #[serde(skip)]
    pub catchup_xor: [u8; 32],
    /// Reusable canonicalization buffer for digest computation. Kept on the
    /// collection so a bulk re-seed (or a steady stream of mutations) reuses
    /// one allocation instead of one per row. Never serialized; carries no
    /// state between calls beyond its capacity.
    #[serde(skip)]
    scratch: Vec<u8>,
    /// Projection: the root fields this collection keeps per row, or `None`
    /// to keep whole bodies (the server default). A client circuit only ever
    /// reads the fields its registered plans evaluate (`resolve_field` on
    /// predicates, join keys, sort keys), and renders bodies from its own
    /// durable store, so everything else is dead weight in a wasm heap that
    /// never shrinks. Configuration, not state: never serialized, re-applied
    /// from the registered plans after a restore.
    #[serde(skip)]
    pub retained: Option<BTreeSet<String>>,
}

impl Collection {
    pub fn new(name: String) -> Self {
        let rows = RowTable::with_arena(crate::circuit::arena::new_arena(&name));
        Self {
            name,
            zset: HashMap::new(),
            rows,
            catchup_xor: ssp_protocol::snapshot_hash::xor_empty(),
            scratch: Vec::new(),
            retained: None,
        }
    }

    /// Reduce `value` to this collection's retained fields (plus
    /// [`ALWAYS_RETAINED`]). Identity when the collection keeps whole bodies
    /// or the value is not an object.
    pub fn project(&self, value: Sp00kyValue) -> Sp00kyValue {
        match (&self.retained, value) {
            (Some(keep), Sp00kyValue::Object(mut map)) => {
                map.retain(|k, _| keep.contains(k) || ALWAYS_RETAINED.contains(&k.as_str()));
                Sp00kyValue::Object(map)
            }
            (_, value) => value,
        }
    }

    /// Rebuild the row bytes without the space orphaned by updates and
    /// deletes, re-projecting each row through [`Self::project`] so a
    /// narrowed retained set takes effect on already-stored rows too.
    /// Preserves the arena kind (a file-backed table stays file-backed).
    ///
    /// Transiently holds every row decoded, so callers should run this off
    /// the hot path (a checkpoint, an idle timer) and only when
    /// `rows.dead_bytes()` is worth it.
    pub fn compact(&mut self) {
        let decoded: Vec<(String, Sp00kyValue)> = self
            .rows
            .iter()
            .map(|(id, v)| (id.to_string(), v.to_owned_value()))
            .collect();
        self.rows.clear();
        let mut acc = ssp_protocol::snapshot_hash::xor_empty();
        for (id, value) in decoded {
            let value = self.project(value);
            let digest = self.incoming_digest(&id, &value);
            ssp_protocol::snapshot_hash::xor_digest(&mut acc, &digest);
            self.rows.insert(&id, &value, &digest);
        }
        self.catchup_xor = acc;
    }

    /// Recompute the incremental XOR accumulator from the current rows. Call
    /// after a bulk load (`Circuit::load`) or a deserialize, where the per-row
    /// `apply_mutation` maintenance didn't run.
    ///
    /// Reads each row's stored digest rather than re-canonicalizing it, so a
    /// re-seed is now a walk of 32-byte header reads.
    pub fn reseed_catchup_xor(&mut self) {
        let mut acc = ssp_protocol::snapshot_hash::xor_empty();
        let ids: Vec<&str> = self.rows.keys().collect();
        for id in ids {
            if let Some(digest) = self.rows.digest_of(id) {
                ssp_protocol::snapshot_hash::xor_digest(&mut acc, &digest);
            }
        }
        self.catchup_xor = acc;
    }

    /// Digest of a value about to be stored under `id`, via the canonical
    /// writer. Exposed so a bulk load can canonicalize once and reuse the
    /// result for both the accumulator and the record header.
    pub fn digest_for(&mut self, id: &str, value: &Sp00kyValue) -> [u8; 32] {
        self.incoming_digest(id, value)
    }

    /// Digest of a value that is about to be stored under `id`.
    fn incoming_digest(&mut self, id: &str, value: &Sp00kyValue) -> [u8; 32] {
        let mut scratch = std::mem::take(&mut self.scratch);
        let digest = value.record_digest_into(id, &mut scratch);
        self.scratch = scratch;
        digest
    }

    /// Apply a mutation to this collection. Returns (zset_key, weight).
    ///
    /// Thin wrapper over [`Self::apply`] for callers that only care about the
    /// membership delta.
    pub fn apply_mutation(
        &mut self,
        op: Operation,
        id: &str,
        data: Sp00kyValue,
    ) -> (RowKey, Weight) {
        let applied = self.apply(op, id, data);
        (applied.key, applied.weight)
    }

    /// Apply a mutation to this collection.
    ///
    /// Presence-driven, so every op is idempotent against the store rather
    /// than trusting the caller's verb:
    ///
    /// - Create/Update/Merge of an ABSENT row: `+1`, the row appears.
    /// - Create/Update/Merge of a PRESENT row: `0`, content replaced.
    /// - Delete of a present row: `-1`; of an absent row: `0`, nothing written.
    ///
    /// Trusting the verb produced z-set weights of 2 (a second Create) that a
    /// later Delete could never bring back to 0, `-1` entries for rows that
    /// never existed, and Update-on-absent rows that sat in `rows` with no
    /// z-set entry and were invisible to every Scan. A client replaying its
    /// own cache into a restored store hits all three on the first batch.
    ///
    /// A Create/Update whose canonical digest equals the stored one writes
    /// nothing: no arena append (which is what made a re-sync of unchanged
    /// rows grow the heap by the whole table), no XOR churn, and
    /// `content_changed == false` so the circuit skips re-evaluation.
    ///
    /// `Merge` overlays the incoming fields on the stored row before any of
    /// the above, for widening a projected row with fields it did not keep.
    pub fn apply(&mut self, op: Operation, id: &str, data: Sp00kyValue) -> Applied {
        let normalized = raw_id(id);
        let key = make_key(&self.name, id);
        let present = self.rows.contains_key(normalized);

        if op == Operation::Delete {
            if !present {
                return Applied { key, weight: 0, content_changed: false };
            }
            // Maintain the incremental XOR set-hash atomically with the row
            // change, here at the single row-mutation chokepoint so the
            // accumulator cannot miss an op.
            if let Some(d) = self.rows.digest_of(normalized) {
                ssp_protocol::snapshot_hash::xor_digest(&mut self.catchup_xor, &d);
            }
            self.rows.remove(normalized);
            self.bump_zset(&key, -1);
            return Applied { key, weight: -1, content_changed: true };
        }

        let data = if op == Operation::Merge && present {
            match (self.rows.get(normalized).to_owned_value(), data) {
                (Sp00kyValue::Object(mut base), Sp00kyValue::Object(incoming)) => {
                    base.extend(incoming);
                    Sp00kyValue::Object(base)
                }
                (_, incoming) => incoming,
            }
        } else {
            data
        };
        let data = self.project(data);

        // The incoming digest is computed once by the canonical writer and
        // handed to the row store, which keeps it in the record header. The
        // outgoing one is read back from that header: 32 bytes at a fixed
        // offset, no decode.
        let incoming = self.incoming_digest(normalized, &data);
        if present && self.rows.digest_of(normalized) == Some(incoming) {
            return Applied { key, weight: 0, content_changed: false };
        }
        if let Some(d) = self.rows.digest_of(normalized) {
            ssp_protocol::snapshot_hash::xor_digest(&mut self.catchup_xor, &d);
        }
        ssp_protocol::snapshot_hash::xor_digest(&mut self.catchup_xor, &incoming);
        self.rows.insert(normalized, &data, &incoming);

        let weight = if present { 0 } else { 1 };
        if weight != 0 {
            self.bump_zset(&key, weight);
        }
        Applied { key, weight, content_changed: true }
    }

    fn bump_zset(&mut self, key: &RowKey, weight: Weight) {
        let entry = self.zset.entry(key.clone()).or_insert(0);
        *entry += weight;
        if *entry == 0 {
            self.zset.remove(key);
        }
    }

    /// Look up a row by its raw ID.
    pub fn get_row(&self, id: &str) -> ValueRef<'_> {
        self.rows.get(raw_id(id))
    }

    /// Whether a row exists under this raw ID.
    pub fn has_row(&self, id: &str) -> bool {
        self.rows.contains_key(raw_id(id))
    }

    /// Approximate heap bytes held by `rows`: the id index, the encoded row
    /// bytes, and the field-name dictionary.
    pub fn rows_bytes(&self) -> usize {
        self.rows.heap_bytes()
    }

    /// Heap held by the row index alone. Broken out because it is the part
    /// that stays O(rows) and resident no matter how the bodies are stored.
    pub fn index_bytes(&self) -> usize {
        self.rows.index_bytes()
    }

    /// Approximate heap bytes held by `zset`.
    ///
    /// Only its bucket array: the keys are shared `Arc<str>` clones, so their
    /// bytes are charged once rather than once per structure holding them.
    pub fn zset_bytes(&self) -> usize {
        crate::size::zset_bytes(&self.zset)
    }

    /// Get the version of a record from its `_00_rv` field.
    ///
    /// Read from the record header, where the encoder lifts it, rather than by
    /// resolving a field out of the body.
    pub fn get_record_version(&self, id: &str) -> Option<i64> {
        self.rows.rv_of(raw_id(id))
    }

    /// Compare the rows with an authoritative `(id, _00_rv)` listing of the
    /// same table. Returns the ids to `fetch` (listed, but absent here or held
    /// at another version) and the `stale` ids (held here, not listed). Ids
    /// may be raw or `table:id`; both come back raw.
    ///
    /// Any version difference counts, not just "lower here": a server row can
    /// carry a version the row-version floor synthesized, which says nothing
    /// about whether its content matches. Equal versions are taken as equal
    /// content, so a caller must still check the table hash afterwards.
    pub fn version_diff(&self, listing: &[(String, Option<i64>)]) -> (Vec<String>, Vec<String>) {
        self.version_diff_in(listing, |_| true)
    }

    /// [`Self::version_diff`] against a listing of only part of the table:
    /// `stale` is limited to the held ids `listed_part` says the listing
    /// covers, so rows outside it are not taken for deleted.
    pub fn version_diff_in(
        &self,
        listing: &[(String, Option<i64>)],
        listed_part: impl Fn(&str) -> bool,
    ) -> (Vec<String>, Vec<String>) {
        let listed: std::collections::HashSet<&str> =
            listing.iter().map(|(id, _)| raw_id(id)).collect();
        let fetch = listing
            .iter()
            .map(|(id, rv)| (raw_id(id), *rv))
            .filter(|(id, rv)| !self.rows.contains_key(id) || self.rows.rv_of(id) != *rv)
            .map(|(id, _)| id.to_string())
            .collect();
        let stale = self
            .rows
            .keys()
            .filter(|id| listed_part(id) && !listed.contains(id))
            .map(str::to_string)
            .collect();
        (fetch, stale)
    }

    /// The rows folded into `ranges`' accumulators, from the digest stored
    /// with each row: what the scheduler's id-range hashes are compared
    /// against. `None` when a held id is not a plain string key.
    pub fn range_accs(&self, ranges: &ssp_protocol::range_hash::RangeHashes) -> Option<Vec<[u8; 32]>> {
        ranges.accumulate(
            self.rows
                .keys()
                .filter_map(|id| self.rows.digest_of(id).map(|d| (id, d))),
        )
    }
}

/// The store holds all base collections (tables).
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Store {
    pub collections: HashMap<String, Collection>,
    /// Transient, per-step overlay of rows being deleted in the CURRENT step,
    /// keyed by full `table:id`. A `Delete` removes the row from `collections`
    /// immediately (the subquery/join path relies on it being gone), but the
    /// Filter/Scan predicate evaluation still needs the row's content to decide
    /// whether the retraction (`-1`) belongs to the view. Predicate evaluation
    /// consults this overlay as a fallback; nothing else does. Populated before
    /// `apply_change` and cleared after the step. Never serialized.
    #[serde(skip)]
    pub pending_deleted_rows: HashMap<RowKey, Sp00kyValue>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ensure_collection(&mut self, name: &str) -> &mut Collection {
        self.collections
            .entry(name.to_string())
            .or_insert_with(|| Collection::new(name.to_string()))
    }

    pub fn get_collection(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }

    /// Apply a mutation whose row body is already owned. Returns
    /// (zset_key, weight).
    ///
    /// The hot ingest path uses this so the incoming body moves into the
    /// collection; [`apply_change`] clones because it only has a borrow.
    pub fn apply_owned(
        &mut self,
        table: &str,
        op: Operation,
        id: &str,
        data: Sp00kyValue,
    ) -> Applied {
        self.ensure_collection(table).apply(op, id, data)
    }

    /// Set (or clear, with `None`) the projection for `table`. Takes effect on
    /// the next write of each row; call [`Collection::compact`] to re-project
    /// rows already stored. Never widens what a stored row holds: widening is
    /// the caller's job via [`Operation::Merge`] with the missing fields.
    pub fn set_retained_fields(&mut self, table: &str, fields: Option<BTreeSet<String>>) {
        self.ensure_collection(table).retained = fields;
    }

    /// Apply a borrowed Change to the store. Returns (zset_key, weight).
    ///
    /// Convenience for callers holding a `&Change` (tests, mostly); it has to
    /// clone the body. Prefer [`apply_owned`] where the body is owned.
    pub fn apply_change(&mut self, change: &Change) -> (RowKey, Weight) {
        let applied = self.apply_owned(&change.table, change.op, &change.id, change.data.clone());
        (applied.key, applied.weight)
    }

    /// Get row data by zset key (format "table:id").
    ///
    /// Returns [`ValueRef::Missing`] for an unparseable key, an unknown table,
    /// or an absent row — the caller does not distinguish those cases.
    pub fn get_row_by_key(&self, key: &str) -> ValueRef<'_> {
        let Some((table, id)) = crate::types::parse_key(key) else {
            return ValueRef::Missing;
        };
        let Some(coll) = self.collections.get(table) else {
            return ValueRef::Missing;
        };
        // Try raw ID first, then with table prefix
        let by_raw = coll.rows.get(id);
        if by_raw.is_missing() {
            coll.rows.get(key)
        } else {
            by_raw
        }
    }

    /// Like [`get_row_by_key`], but falls back to a row staged for deletion in
    /// the current step (see [`pending_deleted_rows`]). Used ONLY by predicate
    /// evaluation so a delete's `-1` retraction can be tested against the WHERE
    /// clause even though the row is already gone from `collections`.
    pub fn get_row_by_key_or_deleted(&self, key: &str) -> ValueRef<'_> {
        let live = self.get_row_by_key(key);
        if live.is_missing() {
            ValueRef::from_opt(self.pending_deleted_rows.get(key))
        } else {
            live
        }
    }

    /// Stage a row's content before a `Delete` removes it, so predicate
    /// evaluation in this step can still read it. No-op if the row is absent.
    ///
    /// This is the one place that materializes a stored row: the overlay
    /// outlives the row's bytes within the step, so it has to own its copy.
    /// It holds at most the rows deleted in a single step.
    pub fn stage_deleted_row(&mut self, table: &str, id: &str) {
        let key = make_key(table, id);
        let row = self.get_row_by_key(&key);
        if !row.is_missing() {
            let owned = row.to_owned_value();
            self.pending_deleted_rows.insert(key, owned);
        }
    }

    /// Clear the per-step deleted-row overlay (call after stepping).
    pub fn clear_pending_deleted_rows(&mut self) {
        self.pending_deleted_rows.clear();
    }

    /// Get the version of a record by its zset key (format "table:id").
    pub fn get_record_version_by_key(&self, key: &str) -> Option<i64> {
        let (table, id) = crate::types::parse_key(key)?;
        self.collections.get(table)?.get_record_version(id)
    }
}

/// A single record mutation.
#[derive(Debug, Clone)]
pub struct Change {
    pub table: String,
    pub op: Operation,
    pub id: String,
    pub data: Sp00kyValue,
}

impl Change {
    pub fn create(table: &str, id: &str, data: impl Into<Sp00kyValue>) -> Self {
        Self {
            table: table.to_string(),
            op: Operation::Create,
            id: id.to_string(),
            data: data.into(),
        }
    }

    pub fn update(table: &str, id: &str, data: impl Into<Sp00kyValue>) -> Self {
        Self {
            table: table.to_string(),
            op: Operation::Update,
            id: id.to_string(),
            data: data.into(),
        }
    }

    pub fn merge(table: &str, id: &str, data: impl Into<Sp00kyValue>) -> Self {
        Self {
            table: table.to_string(),
            op: Operation::Merge,
            id: id.to_string(),
            data: data.into(),
        }
    }

    pub fn delete(table: &str, id: &str) -> Self {
        Self {
            table: table.to_string(),
            op: Operation::Delete,
            id: id.to_string(),
            data: Sp00kyValue::Null,
        }
    }
}

/// A batch of changes to apply in a single step.
#[derive(Debug, Clone, Default)]
pub struct ChangeSet {
    pub changes: Vec<Change>,
}

/// A record for initial bulk loading.
#[derive(Debug, Clone)]
pub struct Record {
    pub table: String,
    pub id: String,
    pub data: Sp00kyValue,
}

impl Record {
    pub fn new(table: &str, id: &str, data: impl Into<Sp00kyValue>) -> Self {
        Self {
            table: table.to_string(),
            id: id.to_string(),
            data: data.into(),
        }
    }
}

/// Mutation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Create,
    Update,
    /// Overlay the given fields on the stored row (Update semantics
    /// otherwise). The way a projected row is widened with fields it did not
    /// keep, without the caller having to resend the whole body.
    Merge,
    Delete,
}

impl Operation {
    /// The membership weight this verb CLAIMS. The store no longer trusts it
    /// (see [`Collection::apply`]); kept for callers that classify ops.
    pub fn weight(&self) -> Weight {
        match self {
            Operation::Create => 1,
            Operation::Update | Operation::Merge => 0,
            Operation::Delete => -1,
        }
    }

    pub fn changes_content(&self) -> bool {
        matches!(self, Operation::Create | Operation::Update | Operation::Merge)
    }

    /// Parse an operation from a string (case-insensitive).
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "CREATE" => Some(Operation::Create),
            "UPDATE" => Some(Operation::Update),
            "MERGE" => Some(Operation::Merge),
            "DELETE" => Some(Operation::Delete),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn coll() -> Collection {
        Collection::new("thread".to_string())
    }

    fn sv(j: serde_json::Value) -> Sp00kyValue {
        Sp00kyValue::from(j)
    }

    #[test]
    fn create_twice_then_delete_leaves_no_row() {
        let mut c = coll();
        let first = c.apply(Operation::Create, "a", sv(json!({ "title": "x" })));
        assert_eq!(first.weight, 1);
        assert!(first.content_changed);

        // A second Create of a present row is an Update: no membership change.
        let second = c.apply(Operation::Create, "a", sv(json!({ "title": "y" })));
        assert_eq!(second.weight, 0);
        assert!(second.content_changed);
        assert_eq!(c.zset.get(&*second.key), Some(&1), "weight must not climb to 2");

        let gone = c.apply(Operation::Delete, "a", Sp00kyValue::Null);
        assert_eq!(gone.weight, -1);
        assert!(c.zset.is_empty(), "the row must leave the z-set");
        assert!(c.rows.is_empty());
    }

    #[test]
    fn delete_of_an_absent_row_writes_nothing() {
        let mut c = coll();
        let applied = c.apply(Operation::Delete, "ghost", Sp00kyValue::Null);
        assert_eq!(applied.weight, 0);
        assert!(!applied.content_changed);
        assert!(c.zset.is_empty(), "no -1 entry for a row that never existed");
    }

    #[test]
    fn update_of_an_absent_row_is_an_insert() {
        let mut c = coll();
        let applied = c.apply(Operation::Update, "a", sv(json!({ "title": "x" })));
        assert_eq!(applied.weight, 1, "the row appeared, whatever the verb said");
        assert_eq!(c.zset.get(&*applied.key), Some(&1));
        assert!(c.rows.contains_key("a"));
    }

    #[test]
    fn an_unchanged_write_is_a_no_op() {
        let mut c = coll();
        c.apply(Operation::Create, "a", sv(json!({ "title": "x", "n": 1 })));
        let dead_before = c.rows.dead_bytes();
        let xor_before = c.catchup_xor;

        let again = c.apply(Operation::Update, "a", sv(json!({ "n": 1, "title": "x" })));
        assert_eq!(again.weight, 0);
        assert!(!again.content_changed, "same digest, nothing to do");
        assert_eq!(c.rows.dead_bytes(), dead_before, "no arena append for an unchanged row");
        assert_eq!(c.catchup_xor, xor_before, "accumulator untouched");

        let changed = c.apply(Operation::Update, "a", sv(json!({ "title": "x", "n": 2 })));
        assert!(changed.content_changed);
        assert!(c.rows.dead_bytes() > dead_before, "a real change orphans the old bytes");
    }

    #[test]
    fn merge_overlays_the_stored_fields() {
        let mut c = coll();
        c.apply(Operation::Create, "a", sv(json!({ "a": 1, "b": 1 })));
        let merged = c.apply(Operation::Merge, "a", sv(json!({ "b": 2, "c": 3 })));
        assert_eq!(merged.weight, 0);
        let row = c.rows.get("a").to_owned_value();
        assert_eq!(row, sv(json!({ "a": 1, "b": 2, "c": 3 })));

        // Merge into an absent row is just an insert of what was given.
        let fresh = c.apply(Operation::Merge, "z", sv(json!({ "q": 1 })));
        assert_eq!(fresh.weight, 1);
        assert_eq!(c.rows.get("z").to_owned_value(), sv(json!({ "q": 1 })));
    }

    #[test]
    fn projection_keeps_retained_fields_plus_identity() {
        let mut c = coll();
        c.retained = Some(["score".to_string()].into_iter().collect());
        c.apply(
            Operation::Create,
            "a",
            sv(json!({ "id": "thread:a", "_00_rv": 3, "score": 7, "title": "big", "pgn": "1. e4" })),
        );
        let row = c.rows.get("a").to_owned_value();
        assert_eq!(row, sv(json!({ "id": "thread:a", "_00_rv": 3, "score": 7 })));
        assert_eq!(c.rows.rv_of("a"), Some(3), "the lifted rv survives projection");

        // The digest is over the projection, so a body that differs only in
        // dropped fields is the same row.
        let same = c.apply(
            Operation::Update,
            "a",
            sv(json!({ "id": "thread:a", "_00_rv": 3, "score": 7, "title": "other", "pgn": "1. d4" })),
        );
        assert!(!same.content_changed);
    }

    #[test]
    fn compact_reclaims_dead_bytes_and_reprojects() {
        let mut c = coll();
        for i in 0..20 {
            c.apply(Operation::Create, &format!("r{i}"), sv(json!({ "n": i, "blob": "x".repeat(200) })));
        }
        for i in 0..20 {
            c.apply(Operation::Update, &format!("r{i}"), sv(json!({ "n": i + 1, "blob": "y".repeat(200) })));
        }
        assert!(c.rows.dead_bytes() > 0);
        let live_before = c.rows.live_bytes();

        // Narrow the projection, then compact: rows shed `blob`.
        c.retained = Some(["n".to_string()].into_iter().collect());
        c.compact();
        assert_eq!(c.rows.dead_bytes(), 0);
        assert!(c.rows.live_bytes() < live_before / 4, "the 200-byte blobs are gone");
        assert_eq!(c.rows.len(), 20);
        assert_eq!(c.rows.get("r3").to_owned_value(), sv(json!({ "n": 4 })));

        // The accumulator is rebuilt from what is now stored, and agrees with
        // a from-scratch re-seed.
        let after = c.catchup_xor;
        c.reseed_catchup_xor();
        assert_eq!(c.catchup_xor, after);
        assert!(c.zset.len() == 20, "membership is untouched by compaction");
    }

    #[test]
    fn version_diff_names_what_to_fetch_and_what_is_gone() {
        let mut c = coll();
        c.apply(Operation::Create, "same", sv(json!({ "_00_rv": 3 })));
        c.apply(Operation::Create, "older", sv(json!({ "_00_rv": 1 })));
        c.apply(Operation::Create, "newer", sv(json!({ "_00_rv": 9 })));
        c.apply(Operation::Create, "unversioned", sv(json!({ "n": 1 })));
        c.apply(Operation::Create, "gone", sv(json!({ "_00_rv": 2 })));

        let listing = vec![
            ("thread:same".to_string(), Some(3)),
            ("older".to_string(), Some(2)),
            // Higher here than upstream: a floor-synthesized version, content unknown.
            ("newer".to_string(), Some(5)),
            ("unversioned".to_string(), None),
            ("thread:added".to_string(), Some(1)),
        ];
        let (mut fetch, stale) = c.version_diff(&listing);
        fetch.sort();
        assert_eq!(fetch, vec!["added", "newer", "older"]);
        assert_eq!(stale, vec!["gone"]);

        // A listing of part of the table only judges the ids in that part.
        let (fetch, stale) = c.version_diff_in(&[("same".to_string(), Some(3))], |id| id < "o");
        assert!(fetch.is_empty());
        assert_eq!(stale, vec!["gone", "newer"]);
    }

    #[test]
    fn range_accs_match_the_ranges_the_same_rows_fold_to() {
        use ssp_protocol::range_hash::RangeHashes;
        let mut c = coll();
        let mut ranges = RangeHashes::with_starts(vec!["".into(), "m".into()]).unwrap();
        for (id, n) in [("a", 1), ("k", 2), ("q", 3)] {
            let body = json!({ "id": format!("thread:{id}"), "n": n });
            c.apply(Operation::Create, id, sv(body.clone()));
            let i = ranges.range_of(id).unwrap();
            ranges.add_at(i, &ssp_protocol::snapshot_hash::record_digest(id, &body));
        }
        let accs = c.range_accs(&ranges).unwrap();
        assert!(ranges.differing(&accs).is_empty());
        assert_eq!(ranges.total(), c.catchup_xor);

        c.apply(Operation::Update, "q", sv(json!({ "id": "thread:q", "n": 4 })));
        assert_eq!(ranges.differing(&c.range_accs(&ranges).unwrap()), vec![1]);

        c.apply(Operation::Create, "42", sv(json!({ "id": "thread:42" })));
        assert!(c.range_accs(&ranges).is_none(), "a numeric key cannot be ranged");
    }
}
