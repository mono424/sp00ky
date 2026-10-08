//! A table's rows, stored as flat bytes behind an index.
//!
//! Replaces the `HashMap<String, Sp00kyValue>` the store used to keep. That
//! map cost roughly six times the rows' own JSON, mostly in per-row per-field
//! `String` allocations for field names that a table only has a few dozen of.
//! Here the names live once in a [`FieldDict`], the bodies live in an
//! [`Arena`] as encoded bytes, and what stays on the heap per row is an index
//! entry: an id and a [`RowSlot`].
//!
//! # The index
//!
//! Still O(rows), and still anonymous memory — this is the floor the row
//! encoding cannot move. It is kept as small as it can be: a
//! [`hashbrown::HashTable`] holding nothing but a 12-byte [`RowSlot`] per row,
//! keyed by the hash of the id. The id itself lives in the arena record, so
//! the index owns no string at all.
//!
//! That is a `HashTable` rather than a `HashMap` specifically because a
//! `HashMap` requires the key to live in the table. Here the key lives in the
//! arena and the table stores only where to find it, which is what takes the
//! per-row cost from roughly 60 bytes to around 15.
//!
//! Reported separately by [`RowTable::index_bytes`] so the floor stays visible
//! rather than hiding inside a total.

use crate::circuit::arena::{Arena, HeapArena, Span};
use crate::circuit::row_codec::{self as codec, FieldDict};
use crate::eval::value_ref::{FlatRef, ValueRef};
use crate::types::Sp00kyValue;
use hashbrown::HashTable;

/// Where one row's encoded bytes live.
pub type RowSlot = Span;

/// Rows of one table.
#[derive(Debug)]
pub struct RowTable {
    dict: FieldDict,
    /// Row slots keyed by the hash of the record id. Holds no key of its own —
    /// see the module docs.
    index: HashTable<RowSlot>,
    arena: Box<dyn Arena>,
    /// Reused encode buffer, so a steady stream of writes does not allocate
    /// one per row.
    scratch: Vec<u8>,
}

/// Hash of a record id, used as the index key.
///
/// FxHash: the ids are internal record keys, not attacker-chosen, and every
/// lookup verifies the real id anyway — so a collision costs one extra
/// comparison, never a wrong answer.
fn id_hash(id: &str) -> u64 {
    use std::hash::{BuildHasher, BuildHasherDefault};
    BuildHasherDefault::<rustc_hash::FxHasher>::default().hash_one(id)
}

impl Default for RowTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RowTable {
    pub fn new() -> Self {
        Self::with_arena(Box::new(HeapArena::new()))
    }

    /// Build a table over a caller-supplied arena.
    ///
    /// The seam that lets the row bytes live somewhere other than the heap —
    /// a file mapping, on platforms that have one — without any operator or
    /// codec change.
    pub fn with_arena(arena: Box<dyn Arena>) -> Self {
        Self {
            dict: FieldDict::new(),
            index: HashTable::new(),
            arena,
            scratch: Vec::new(),
        }
    }

    /// The id stored in the record a slot points at.
    fn slot_id(arena: &dyn Arena, slot: RowSlot) -> Option<&str> {
        codec::record_id(arena.get(slot))
    }

    /// Find the slot for `id`.
    ///
    /// The hash only narrows the search: the match is confirmed against the
    /// id stored in the record. Two ids that hash alike are rare but not
    /// impossible, and trusting the hash would return the wrong row —
    /// which surfaces not as an error but as wrong query results, then a
    /// table-hash mismatch, then `exit(2)`.
    fn find_slot(&self, id: &str) -> Option<RowSlot> {
        let arena = &*self.arena;
        self.index
            .find(id_hash(id), |slot| Self::slot_id(arena, *slot) == Some(id))
            .copied()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn contains_key(&self, id: &str) -> bool {
        self.find_slot(id).is_some()
    }

    /// Ids of every row, read out of the arena records.
    pub fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        let arena = &*self.arena;
        self.index
            .iter()
            .filter_map(move |slot| Self::slot_id(arena, *slot))
    }

    pub fn dict(&self) -> &FieldDict {
        &self.dict
    }

    /// Borrow a row. [`ValueRef::Missing`] when absent.
    pub fn get(&self, id: &str) -> ValueRef<'_> {
        match self.record(id) {
            Some(rec) => FlatRef {
                bytes: codec::record_value(rec),
                dict: &self.dict,
            }
            .value(),
            None => ValueRef::Missing,
        }
    }

    /// The raw record (header + value) for a row.
    fn record(&self, id: &str) -> Option<&[u8]> {
        let bytes = self.arena.get(self.find_slot(id)?);
        (!bytes.is_empty()).then_some(bytes)
    }

    /// The digest stored in a row's header.
    ///
    /// This is why writes no longer canonicalize anything: the digest was
    /// computed once, at insert, and is read back as 32 bytes.
    pub fn digest_of(&self, id: &str) -> Option<[u8; codec::DIGEST_LEN]> {
        codec::record_digest(self.record(id)?).copied()
    }

    /// A row's lifted `_00_rv`. Two loads at a fixed offset — no decode.
    pub fn rv_of(&self, id: &str) -> Option<i64> {
        codec::record_rv(self.record(id)?)
    }

    /// Highest `_00_rv` across the table, or `None` if no row carries one.
    pub fn max_rv(&self) -> Option<i64> {
        self.index
            .iter()
            .filter_map(|slot| codec::record_rv(self.arena.get(*slot)))
            .max()
    }

    /// Insert or replace a row, returning the digest that was stored.
    ///
    /// `digest` comes from the caller rather than being computed here so it
    /// always originates from the canonical writer that is pinned against the
    /// `serde_json` hash pipeline.
    pub fn insert(
        &mut self,
        id: &str,
        value: &Sp00kyValue,
        digest: &[u8; codec::DIGEST_LEN],
    ) -> RowSlot {
        let mut scratch = std::mem::take(&mut self.scratch);
        codec::encode_record(id, value, digest, &mut self.dict, &mut scratch);
        let slot = self.arena.append(&scratch);
        self.scratch = scratch;
        self.place(id, slot);
        slot
    }

    /// Point `id` at `slot`, freeing whatever slot it pointed at before.
    fn place(&mut self, id: &str, slot: RowSlot) {
        let hash = id_hash(id);
        let arena = &*self.arena;
        match self
            .index
            .find_mut(hash, |s| Self::slot_id(arena, *s) == Some(id))
        {
            Some(existing) => {
                let old = std::mem::replace(existing, slot);
                self.arena.free(old);
            }
            None => {
                // The rehash closure re-reads each entry's id from the arena,
                // which is why the arena borrow is taken up front and the two
                // fields are borrowed separately.
                let arena = &*self.arena;
                self.index.insert_unique(hash, slot, |s| {
                    Self::slot_id(arena, *s).map_or(0, id_hash)
                });
            }
        }
    }

    /// Every live row's raw record (header + value), in unspecified order.
    ///
    /// The value's field ids refer to [`Self::dict`], so the bytes only mean
    /// something next to that dictionary. A row checkpoint writes both and
    /// reads them back with [`Self::restore_dict`] + [`Self::insert_encoded`],
    /// which is what makes a checkpoint a copy of bytes rather than a decode
    /// and re-encode of every row.
    pub fn records(&self) -> impl Iterator<Item = &[u8]> + '_ {
        let arena = &*self.arena;
        self.index
            .iter()
            .map(move |slot| arena.get(*slot))
            .filter(|record| !record.is_empty())
    }

    /// Give an EMPTY table the dictionary another table was written with,
    /// `names` in id order (see [`FieldDict::names`]). Refused (`false`) when
    /// the table already holds rows or a name repeats: in both cases the ids
    /// would not line up with the records about to be inserted.
    pub fn restore_dict<'a>(&mut self, names: impl IntoIterator<Item = &'a str>) -> bool {
        if !self.is_empty() || !self.dict.is_empty() {
            return false;
        }
        for (expected, name) in names.into_iter().enumerate() {
            if self.dict.intern(name) as usize != expected {
                self.dict = FieldDict::new();
                return false;
            }
        }
        true
    }

    /// Store a record that was encoded against this table's dictionary, as
    /// is. `false`, and nothing stored, when it carries no readable id.
    pub fn insert_encoded(&mut self, record: &[u8]) -> bool {
        let Some(id) = codec::record_id(record) else {
            return false;
        };
        let slot = self.arena.append(record);
        self.place(id, slot);
        true
    }

    /// Insert under a caller-chosen hash, so a test can force every row into
    /// one bucket and prove that collisions are resolved by comparing ids
    /// rather than assumed away.
    #[cfg(test)]
    fn insert_with_hash(
        &mut self,
        id: &str,
        value: &Sp00kyValue,
        digest: &[u8; codec::DIGEST_LEN],
        hash: u64,
    ) {
        let mut scratch = std::mem::take(&mut self.scratch);
        codec::encode_record(id, value, digest, &mut self.dict, &mut scratch);
        let slot = self.arena.append(&scratch);
        self.scratch = scratch;
        // The rehash closure must agree with the hash used to insert, or a
        // resize relocates entries where a lookup will not look for them.
        self.index.insert_unique(hash, slot, |_| hash);
    }

    /// Look up under a caller-chosen hash. Test counterpart to
    /// [`Self::insert_with_hash`].
    #[cfg(test)]
    fn get_with_hash(&self, id: &str, hash: u64) -> ValueRef<'_> {
        let arena = &*self.arena;
        match self
            .index
            .find(hash, |slot| Self::slot_id(arena, *slot) == Some(id))
        {
            Some(slot) => FlatRef {
                bytes: codec::record_value(self.arena.get(*slot)),
                dict: &self.dict,
            }
            .value(),
            None => ValueRef::Missing,
        }
    }

    /// Remove a row. Returns whether it was present.
    pub fn remove(&mut self, id: &str) -> bool {
        let arena = &*self.arena;
        let Ok(entry) = self
            .index
            .find_entry(id_hash(id), |s| Self::slot_id(arena, *s) == Some(id))
        else {
            return false;
        };
        let (slot, _) = entry.remove();
        self.arena.free(slot);
        true
    }

    pub fn clear(&mut self) {
        self.index.clear();
        self.arena.clear();
        self.dict = FieldDict::new();
    }

    /// Iterate `(raw_id, row)` pairs. Order is unspecified.
    pub fn iter(&self) -> impl Iterator<Item = (&str, ValueRef<'_>)> + '_ {
        let arena = &*self.arena;
        let dict = &self.dict;
        self.index.iter().filter_map(move |slot| {
            let record = arena.get(*slot);
            let id = codec::record_id(record)?;
            let value = FlatRef {
                bytes: codec::record_value(record),
                dict,
            }
            .value();
            Some((id, value))
        })
    }

    /// Heap held by the index: the id strings and the bucket array. Reported
    /// apart from the arena because this is the part that does not shrink.
    /// The index holds only slots — the ids live in the arena — so this is the
    /// bucket array and nothing else.
    pub fn index_bytes(&self) -> usize {
        crate::size::map_table_bytes_for::<RowSlot>(self.index.capacity())
    }

    /// Bytes held by the encoded rows themselves, including space orphaned by
    /// updates and deletes and not yet reclaimed.
    pub fn arena_bytes(&self) -> usize {
        self.arena.capacity_bytes() as usize
    }

    pub fn dict_bytes(&self) -> usize {
        self.dict.heap_bytes()
    }

    /// Total approximate heap for this table's rows.
    pub fn heap_bytes(&self) -> usize {
        self.index_bytes() + self.arena_bytes() + self.dict_bytes()
    }

    /// Bytes orphaned by updates and deletes — what a compaction pass would
    /// reclaim.
    pub fn dead_bytes(&self) -> u64 {
        self.arena.dead_bytes()
    }

    /// Bytes currently referenced by a live row.
    pub fn live_bytes(&self) -> u64 {
        self.arena.live_bytes()
    }
}

impl Clone for RowTable {
    /// Rebuilds through the encoder rather than copying the arena, so a clone
    /// is compact even if the original had accumulated dead bytes.
    fn clone(&self) -> Self {
        let mut out = RowTable::new();
        for slot in self.index.iter() {
            let record = self.arena.get(*slot);
            let (Some(id), Some(digest)) = (
                codec::record_id(record),
                codec::record_digest(record).copied(),
            ) else {
                continue;
            };
            if let Some(value) = codec::decode_value(codec::record_value(record), &self.dict) {
                out.insert(id, &value, &digest);
            }
        }
        out
    }
}

// --- serialization ---
//
// Serialized as `{ raw_id: <row as JSON> }`, i.e. exactly the shape the old
// `HashMap<String, Sp00kyValue>` produced. Keeping that shape means a snapshot
// written before this change still deserializes, and the flat encoding stays
// an in-memory detail rather than an on-disk format that would have to be
// versioned and migrated.
//
// Digests are NOT serialized: they are recomputed on load from the canonical
// writer, which is the single source of truth for them.

impl serde::Serialize for RowTable {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.index.len()))?;
        for (id, value) in self.iter() {
            map.serialize_entry(id, &value.to_owned_value())?;
        }
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for RowTable {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use std::collections::HashMap;
        let rows: HashMap<String, Sp00kyValue> = HashMap::deserialize(deserializer)?;
        let mut table = RowTable::new();
        let mut scratch = Vec::new();
        for (id, value) in rows {
            let digest = value.record_digest_into(&id, &mut scratch);
            table.insert(&id, &value, &digest);
        }
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sv(j: serde_json::Value) -> Sp00kyValue {
        Sp00kyValue::from(j)
    }

    fn digest_of(id: &str, v: &Sp00kyValue) -> [u8; codec::DIGEST_LEN] {
        let mut scratch = Vec::new();
        v.record_digest_into(id, &mut scratch)
    }

    fn put(t: &mut RowTable, id: &str, j: serde_json::Value) -> Sp00kyValue {
        let v = sv(j);
        let d = digest_of(id, &v);
        t.insert(id, &v, &d);
        v
    }

    #[test]
    fn insert_then_read_back() {
        let mut t = RowTable::new();
        let v = put(&mut t, "a", json!({ "title": "x", "n": 1, "nested": { "k": [1, 2] } }));
        assert_eq!(t.len(), 1);
        assert!(t.contains_key("a"));
        assert_eq!(t.get("a").to_owned_value(), v);
        assert!(t.get("nope").is_missing());
    }

    #[test]
    fn field_lookup_matches_the_owned_value() {
        let mut t = RowTable::new();
        let v = put(&mut t, "a", json!({ "s": "str", "i": 5, "f": 1.5, "b": true, "n": null }));
        let row = t.get("a");
        for key in ["s", "i", "f", "b", "n"] {
            assert_eq!(
                row.get(key).to_owned_value(),
                *v.get(key).unwrap(),
                "field {key} mismatched"
            );
        }
        assert!(row.get("absent").is_missing());
    }

    #[test]
    fn update_replaces_and_frees_the_old_bytes() {
        let mut t = RowTable::new();
        put(&mut t, "a", json!({ "v": 1 }));
        assert_eq!(t.dead_bytes(), 0);
        let second = put(&mut t, "a", json!({ "v": 2 }));
        assert_eq!(t.len(), 1, "update must not add a row");
        assert_eq!(t.get("a").to_owned_value(), second);
        assert!(t.dead_bytes() > 0, "the superseded record must be accounted dead");
    }

    #[test]
    fn remove_drops_the_row() {
        let mut t = RowTable::new();
        put(&mut t, "a", json!({ "v": 1 }));
        assert!(t.remove("a"));
        assert!(!t.remove("a"), "removing twice reports absent");
        assert_eq!(t.len(), 0);
        assert!(t.get("a").is_missing());
    }

    #[test]
    fn digest_is_stored_and_matches_the_canonical_writer() {
        let mut t = RowTable::new();
        let v = put(&mut t, "a", json!({ "x": 1, "_00_rv": 9, "gone": null }));
        assert_eq!(t.digest_of("a"), Some(digest_of("a", &v)));
        assert_eq!(t.digest_of("missing"), None);
    }

    #[test]
    fn rv_is_readable_without_decoding() {
        let mut t = RowTable::new();
        put(&mut t, "a", json!({ "_00_rv": 7 }));
        put(&mut t, "b", json!({ "_00_rv": 12 }));
        put(&mut t, "c", json!({ "no": "rv" }));
        assert_eq!(t.rv_of("a"), Some(7));
        assert_eq!(t.rv_of("c"), None);
        assert_eq!(t.max_rv(), Some(12));

        let empty = RowTable::new();
        assert_eq!(empty.max_rv(), None);
    }

    #[test]
    fn iter_yields_every_row() {
        let mut t = RowTable::new();
        for i in 0..16 {
            put(&mut t, &format!("r{i}"), json!({ "n": i }));
        }
        let mut seen: Vec<(String, i64)> = t
            .iter()
            .map(|(id, v)| (id.to_string(), v.get("n").as_i64().unwrap()))
            .collect();
        seen.sort();
        assert_eq!(seen.len(), 16);
        assert_eq!(seen[0], ("r0".to_string(), 0));
    }

    #[test]
    fn serde_round_trip_preserves_content_and_digests() {
        let mut t = RowTable::new();
        for i in 0..8 {
            put(
                &mut t,
                &format!("r{i}"),
                json!({ "n": i, "s": format!("v{i}"), "_00_rv": i, "nil": null }),
            );
        }
        let json = serde_json::to_string(&t).unwrap();
        let back: RowTable = serde_json::from_str(&json).unwrap();

        assert_eq!(back.len(), t.len());
        for i in 0..8 {
            let id = format!("r{i}");
            assert_eq!(
                back.get(&id).to_owned_value(),
                t.get(&id).to_owned_value(),
                "row {id} changed across serde"
            );
            assert_eq!(back.digest_of(&id), t.digest_of(&id), "digest for {id}");
            assert_eq!(back.rv_of(&id), t.rv_of(&id));
        }
    }

    /// The on-disk shape must be byte-for-byte what the old
    /// `HashMap<String, Sp00kyValue>` produced, or every existing snapshot
    /// stops loading and every warm restart becomes a cold rebuild.
    ///
    /// Note what that shape actually is: `Sp00kyValue`'s derived `Serialize`
    /// is externally tagged, so a row is `{"Object":{"n":{"Int":1}}}`, not
    /// plain JSON. The flat encoding is an in-memory detail and deliberately
    /// does not reach the snapshot.
    #[test]
    fn serialized_shape_matches_the_previous_hashmap_encoding() {
        let mut t = RowTable::new();
        put(&mut t, "abc", json!({ "n": 1 }));

        // What the old `HashMap<String, Sp00kyValue>` field would have emitted.
        let legacy: std::collections::HashMap<String, Sp00kyValue> =
            [("abc".to_string(), sv(json!({ "n": 1 })))].into_iter().collect();
        let expected: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&legacy).unwrap()).unwrap();
        let actual: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&t).unwrap()).unwrap();
        assert_eq!(actual, expected);
    }

    /// A snapshot in the pre-flat-encoding format must still load.
    #[test]
    fn deserializes_a_legacy_snapshot_payload() {
        let legacy = r#"{"abc":{"Object":{"n":{"Int":1}}},"def":{"Object":{"s":{"Str":"x"}}}}"#;
        let t: RowTable = serde_json::from_str(legacy).unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(t.get("abc").get("n").as_i64(), Some(1));
        assert_eq!(t.get("def").get("s").as_str(), Some("x"));
        // Digests are recomputed on load rather than carried in the snapshot.
        assert_eq!(
            t.digest_of("abc"),
            Some(digest_of("abc", &sv(json!({ "n": 1 }))))
        );
    }

    #[test]
    fn clone_is_independent_and_compact() {
        let mut t = RowTable::new();
        put(&mut t, "a", json!({ "v": 1 }));
        // Churn so the original carries dead bytes.
        for i in 0..10 {
            put(&mut t, "a", json!({ "v": i }));
        }
        assert!(t.dead_bytes() > 0);

        let c = t.clone();
        assert_eq!(c.dead_bytes(), 0, "clone rebuilds compactly");
        assert_eq!(c.get("a").to_owned_value(), t.get("a").to_owned_value());
        assert_eq!(c.digest_of("a"), t.digest_of("a"));

        // Mutating the clone must not touch the original.
        let mut c = c;
        c.remove("a");
        assert!(t.contains_key("a"));
    }

    #[test]
    fn clear_empties_everything() {
        let mut t = RowTable::new();
        put(&mut t, "a", json!({ "v": 1 }));
        t.clear();
        assert_eq!(t.len(), 0);
        assert!(t.get("a").is_missing());
        assert_eq!(t.dead_bytes(), 0);
    }

    /// The index is keyed by a hash and stores no key of its own, so a hash
    /// match is only ever a *candidate*. Every one of these rows is forced
    /// into the same bucket; each must still resolve to its own row, and an
    /// absent id sharing that hash must miss.
    ///
    /// Getting this wrong does not raise an error — it returns a different
    /// row, which becomes wrong query results, then a table-hash mismatch
    /// against the scheduler, then exit(2).
    #[test]
    fn colliding_hashes_resolve_by_comparing_the_stored_id() {
        const COLLIDE: u64 = 0xdead_beef;
        let mut t = RowTable::new();
        for i in 0..64 {
            let id = format!("row{i}");
            let v = sv(json!({ "n": i }));
            let d = digest_of(&id, &v);
            t.insert_with_hash(&id, &v, &d, COLLIDE);
        }
        assert_eq!(t.len(), 64);

        for i in 0..64 {
            let id = format!("row{i}");
            assert_eq!(
                t.get_with_hash(&id, COLLIDE).get("n").as_i64(),
                Some(i),
                "{id} resolved to the wrong row"
            );
        }
        // An id that is not present but hashes into the same bucket must miss
        // rather than return whichever row happened to be there.
        assert!(t.get_with_hash("row999", COLLIDE).is_missing());
        assert!(t.get_with_hash("", COLLIDE).is_missing());
    }

    /// At scale: every present id returns its own row, and a large number of
    /// absent ids all miss. This exercises the eq path heavily, since any two
    /// ids landing in the same bucket invoke it.
    #[test]
    fn many_rows_never_cross_talk() {
        let mut t = RowTable::new();
        const N: usize = 2000;
        for i in 0..N {
            put(&mut t, &format!("{i:026}"), json!({ "n": i }));
        }
        assert_eq!(t.len(), N);
        for i in 0..N {
            let id = format!("{i:026}");
            assert_eq!(t.get(&id).get("n").as_i64(), Some(i as i64), "{id}");
        }
        for i in N..(N + 500) {
            let id = format!("{i:026}");
            assert!(t.get(&id).is_missing(), "{id} must be absent");
        }
    }

    /// The id round-trips byte-exactly, including shapes that are easy to
    /// mangle through a length-prefixed encoding.
    #[test]
    fn ids_round_trip_exactly() {
        let mut t = RowTable::new();
        let ids = ["", "a", "⟨escaped⟩", "with:colon", "e\u{0301}", "🎃", &"x".repeat(500)];
        for id in ids {
            put(&mut t, id, json!({ "marker": id }));
        }
        for id in ids {
            assert!(t.contains_key(id), "{id:?} not found");
            assert_eq!(t.get(id).get("marker").as_str(), Some(id));
        }
        let mut keys: Vec<&str> = t.keys().collect();
        keys.sort_unstable();
        let mut want: Vec<&str> = ids.to_vec();
        want.sort_unstable();
        assert_eq!(keys, want, "keys() must yield the ids that went in");
    }

    #[test]
    fn field_names_are_stored_once_per_table_not_once_per_row() {
        // The entire point of the dictionary. 200 rows sharing 4 field names
        // must intern 4 entries, not 800.
        let mut t = RowTable::new();
        for i in 0..200 {
            put(
                &mut t,
                &format!("r{i}"),
                json!({ "alpha": i, "beta": "x", "gamma": true, "delta": null }),
            );
        }
        assert_eq!(t.dict().len(), 4);
    }
}
