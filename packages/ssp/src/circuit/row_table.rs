//! A table's rows, stored as flat bytes behind an index.
//!
//! Replaces the `HashMap<String, Sp00kyValue>` the store used to keep. That
//! map cost roughly six times the rows' own JSON, mostly in per-row per-field
//! `String` allocations for field names that a table only has a few dozen of.
//! Here the names live once in a [`FieldDict`], the rows live in an
//! [`Arena`] as encoded bytes, and what stays on the heap per row is an index
//! entry: a [`RowSlot`].
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
//!
//! # Heads and bodies
//!
//! A slot points at a record's *head*: digest, version, body pointer, id (see
//! `row_codec`). Everything the table does short of reading a value touches
//! the head alone: a lookup compares the id, a version read or a hash fold
//! reads a fixed field. The body, the encoded value, is reached through the
//! head's pointer only when a value is wanted. A checkpoint image keeps all
//! heads together, so loading one is a walk over the heads that leaves the
//! bodies on disk until a row is read.

use crate::circuit::arena::{Arena, HeapArena, Span};
#[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
use crate::circuit::arena::ImageRef;
use crate::circuit::row_codec::{self as codec, FieldDict};
use crate::eval::value_ref::{FlatRef, ValueRef};
use crate::types::Sp00kyValue;
use hashbrown::hash_table::Entry;
use hashbrown::HashTable;
use std::ops::Range;

/// Where one row's head lives. The body is found through the head.
pub type RowSlot = Span;

/// An index built over the heads of a checkpoint image by
/// [`RowTable::index_heads`]. The image itself becomes the arena.
pub(crate) struct IndexedHeads {
    pub index: HashTable<RowSlot>,
    /// XOR of every head's stored digest: the catch-up hash the image
    /// should add up to.
    pub xor: [u8; codec::DIGEST_LEN],
    /// Bytes referenced by the rows: every head and every body.
    pub live: u64,
}

/// Why an image could not be indexed. Every variant is a malformed image, not
/// an I/O condition; the caller treats it as "not a checkpoint".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlockError {
    /// A head runs past the heads region.
    Truncated,
    /// Record `n` has no readable id or digest.
    BadRecord(u64),
    /// Record `n` repeats an id an earlier record carried.
    DuplicateId(u64),
    /// The declared row count does not account for every byte of a region.
    TrailingBytes,
    /// The declared row count cannot fit in the heads region.
    TooManyRows,
    /// Offsets would not fit a [`Span`].
    TooLarge,
    /// Record `n`'s body does not lie where the bodies region says it must.
    BadBody(u64),
    /// Record `n` has no body: a tombstone, which a base image never holds.
    Tombstone(u64),
}

impl std::fmt::Display for BlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockError::Truncated => write!(f, "a head runs past the end of the heads"),
            BlockError::BadRecord(n) => write!(f, "record {n} has no readable id"),
            BlockError::DuplicateId(n) => write!(f, "record {n} carries an id stored twice"),
            BlockError::TrailingBytes => write!(f, "bytes after the declared rows"),
            BlockError::TooManyRows => write!(f, "declares more rows than the file holds"),
            BlockError::TooLarge => write!(f, "image too large to map (4 GiB per table)"),
            BlockError::BadBody(n) => write!(f, "record {n}'s body is not where the bodies lie"),
            BlockError::Tombstone(n) => write!(f, "record {n} is a tombstone"),
        }
    }
}

/// The id stored in the head a slot points at, within one block.
fn id_in<'a>(block: &'a [u8], slot: &RowSlot) -> Option<&'a str> {
    let start = slot.off as usize;
    codec::head_id(block.get(start..start + slot.len as usize)?)
}

/// Where a head's body lies: in the same segment, `body_rel` bytes on from
/// the head's own start. `None` for a tombstone or a malformed head.
fn body_of(head: &[u8], slot: RowSlot) -> Option<Span> {
    let rel = codec::head_body_rel(head)?;
    let len = codec::head_body_len(head)?;
    if len == 0 {
        return None;
    }
    Some(Span {
        seg: slot.seg,
        off: slot.off.checked_add(rel)?,
        len,
    })
}

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

    /// A table over an index and an arena that were built together by
    /// [`Self::index_heads`]: the slots in `index` point into `arena`'s first
    /// segment, which holds the image the index was walked over.
    pub(crate) fn from_parts(dict: FieldDict, index: HashTable<RowSlot>, arena: Box<dyn Arena>) -> Self {
        Self {
            dict,
            index,
            arena,
            scratch: Vec::new(),
        }
    }

    /// Index the `rows` heads that fill `block[heads]`, whose bodies tile
    /// `block[bodies]` in the same order, as they lie: no byte is copied, the
    /// slots are the heads' offsets in `block`, tagged with segment `seg`.
    ///
    /// Per head this reads the id (to hash it, with the same `id_hash` a
    /// lookup uses, which is why this lives here and the hash is never
    /// persisted), the stored digest (to fold the catch-up hash) and the body
    /// pointer, which has to land exactly where the previous body ended: the
    /// bodies are located and bounds-checked, never touched, and the check is
    /// what makes "the bodies hash covers exactly the bytes a row can read"
    /// true. The table is pre-sized, so there is no rehash on the way, and a
    /// repeated id is an error rather than a silent overwrite.
    pub(crate) fn index_heads(
        block: &[u8],
        heads: Range<usize>,
        bodies: Range<usize>,
        rows: u64,
        seg: u16,
    ) -> Result<IndexedHeads, BlockError> {
        if heads.start > heads.end
            || heads.end > bodies.start
            || bodies.start > bodies.end
            || bodies.end > block.len()
        {
            return Err(BlockError::Truncated);
        }
        if bodies.end > u32::MAX as usize {
            return Err(BlockError::TooLarge);
        }
        if rows > (heads.end - heads.start) as u64 / codec::HEAD_MIN_LEN as u64 {
            return Err(BlockError::TooManyRows);
        }

        let mut index: HashTable<RowSlot> = HashTable::with_capacity(rows as usize);
        let mut xor = ssp_protocol::snapshot_hash::xor_empty();
        let mut live = 0u64;
        let (mut at, mut next_body) = (heads.start, bodies.start);
        for n in 0..rows {
            let head_len = codec::head_len(&block[at..heads.end]).ok_or(BlockError::Truncated)?;
            let head = &block[at..at + head_len];
            let id = codec::head_id(head).ok_or(BlockError::BadRecord(n))?;
            let digest = codec::head_digest(head).ok_or(BlockError::BadRecord(n))?;
            let rel = codec::head_body_rel(head).ok_or(BlockError::BadRecord(n))? as usize;
            let len = codec::head_body_len(head).ok_or(BlockError::BadRecord(n))? as usize;
            if len == 0 {
                return Err(BlockError::Tombstone(n));
            }
            if rel < head_len || at + rel != next_body || next_body + len > bodies.end {
                return Err(BlockError::BadBody(n));
            }
            let slot = Span {
                seg,
                off: at as u32,
                len: head_len as u32,
            };
            match index.entry(
                id_hash(id),
                |s| id_in(block, s) == Some(id),
                |s| id_in(block, s).map_or(0, id_hash),
            ) {
                Entry::Occupied(_) => return Err(BlockError::DuplicateId(n)),
                Entry::Vacant(v) => {
                    v.insert(slot);
                }
            }
            ssp_protocol::snapshot_hash::xor_digest(&mut xor, digest);
            live += (head_len + len) as u64;
            at += head_len;
            next_body += len;
        }
        if at != heads.end || next_body != bodies.end {
            return Err(BlockError::TrailingBytes);
        }
        Ok(IndexedHeads { index, xor, live })
    }

    /// The id stored in the head a slot points at.
    fn slot_id(arena: &dyn Arena, slot: RowSlot) -> Option<&str> {
        codec::head_id(arena.get(slot))
    }

    /// Find the slot for `id`.
    ///
    /// The hash only narrows the search: the match is confirmed against the
    /// id stored in the head. Two ids that hash alike are rare but not
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

    /// Ids of every row, read out of the heads.
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
        let Some(slot) = self.find_slot(id) else {
            return ValueRef::Missing;
        };
        let body = self.body(slot);
        if body.is_empty() {
            return ValueRef::Missing;
        }
        FlatRef {
            bytes: body,
            dict: &self.dict,
        }
        .value()
    }

    /// The head a slot points at; empty when the slot is stale.
    fn head_at(&self, slot: RowSlot) -> &[u8] {
        self.arena.get(slot)
    }

    /// A row's body, reached through its head. Empty for a tombstone or a
    /// stale slot.
    fn body(&self, slot: RowSlot) -> &[u8] {
        match body_of(self.head_at(slot), slot) {
            Some(span) => self.arena.get(span),
            None => &[],
        }
    }

    /// The head of a row.
    fn head(&self, id: &str) -> Option<&[u8]> {
        let head = self.head_at(self.find_slot(id)?);
        (!head.is_empty()).then_some(head)
    }

    /// The digest stored in a row's head.
    ///
    /// This is why writes no longer canonicalize anything: the digest was
    /// computed once, at insert, and is read back as 32 bytes.
    pub fn digest_of(&self, id: &str) -> Option<[u8; codec::DIGEST_LEN]> {
        codec::head_digest(self.head(id)?).copied()
    }

    /// A row's lifted `_00_rv`. Two loads at a fixed offset — no decode.
    pub fn rv_of(&self, id: &str) -> Option<i64> {
        codec::head_rv(self.head(id)?)
    }

    /// Highest `_00_rv` across the table, or `None` if no row carries one.
    pub fn max_rv(&self) -> Option<i64> {
        self.index
            .iter()
            .filter_map(|slot| codec::head_rv(self.arena.get(*slot)))
            .max()
    }

    /// Insert or replace a row, returning the slot its head was stored at.
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
        let head_len = codec::encode_record(id, value, digest, &mut self.dict, &mut scratch);
        let slot = self.append_record(&scratch, head_len);
        self.scratch = scratch;
        self.place(id, slot);
        slot
    }

    /// Store a row whose body is already encoded against this table's
    /// dictionary, with the digest and the raw rv (`RV_ABSENT` included) it
    /// was stored with: what a converter does with a record of another
    /// layout. Returns the head's slot and whether a row of that id was
    /// replaced.
    pub fn insert_raw(
        &mut self,
        id: &str,
        digest: &[u8; codec::DIGEST_LEN],
        rv: i64,
        body: &[u8],
    ) -> (RowSlot, bool) {
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        let head_len = codec::encode_head(
            id,
            digest,
            rv,
            codec::head_len_for(id) as u32,
            body.len() as u32,
            &mut scratch,
        );
        scratch.extend_from_slice(body);
        let slot = self.append_record(&scratch, head_len);
        self.scratch = scratch;
        let replaced = self.place(id, slot);
        (slot, replaced)
    }

    /// Append an encoded record (head then body) and return the head's slot.
    fn append_record(&mut self, record: &[u8], head_len: usize) -> RowSlot {
        let span = self.arena.append(record);
        Span {
            len: head_len as u32,
            ..span
        }
    }

    /// Make room for `additional` more rows without a rehash per growth step.
    pub fn reserve(&mut self, additional: usize) {
        let arena = &*self.arena;
        self.index
            .reserve(additional, |s| Self::slot_id(arena, *s).map_or(0, id_hash));
    }

    /// Point `id` at `slot`, freeing whatever slot it pointed at before.
    /// Returns whether there was one.
    fn place(&mut self, id: &str, slot: RowSlot) -> bool {
        let hash = id_hash(id);
        let arena = &*self.arena;
        match self
            .index
            .find_mut(hash, |s| Self::slot_id(arena, *s) == Some(id))
        {
            Some(existing) => {
                let old = std::mem::replace(existing, slot);
                self.free_slot(old);
                true
            }
            None => {
                // The rehash closure re-reads each entry's id from the arena,
                // which is why the arena borrow is taken up front and the two
                // fields are borrowed separately.
                let arena = &*self.arena;
                self.index.insert_unique(hash, slot, |s| {
                    Self::slot_id(arena, *s).map_or(0, id_hash)
                });
                false
            }
        }
    }

    /// Account a head and its body as no longer referenced.
    fn free_slot(&mut self, slot: RowSlot) {
        let body = body_of(self.arena.get(slot), slot);
        self.arena.free(slot);
        if let Some(body) = body {
            self.arena.free(body);
        }
    }

    /// Every live row's head, in unspecified order. What a catch-up re-seed
    /// walks: 32 bytes a row, no body touched.
    pub fn heads(&self) -> impl Iterator<Item = &[u8]> + '_ {
        let arena = &*self.arena;
        self.index
            .iter()
            .map(move |slot| arena.get(*slot))
            .filter(|head| !head.is_empty())
    }

    /// Every live row as `(head, body)`, in unspecified order.
    ///
    /// The body's field ids refer to [`Self::dict`], so the bytes only mean
    /// something next to that dictionary. A row checkpoint writes both and
    /// reads them back as they are, which is what makes a checkpoint a copy of
    /// bytes rather than a decode and re-encode of every row.
    pub fn records(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        let arena = &*self.arena;
        self.index.iter().filter_map(move |slot| {
            let head = arena.get(*slot);
            if head.is_empty() {
                return None;
            }
            let body = arena.get(body_of(head, *slot)?);
            Some((head, body))
        })
    }

    /// Give an EMPTY table the dictionary another table was written with,
    /// `names` in id order (see [`FieldDict::names`]). Refused (`false`) when
    /// the table already holds rows or a name repeats: in both cases the ids
    /// would not line up with the records about to be inserted.
    pub fn restore_dict<'a>(&mut self, names: impl IntoIterator<Item = &'a str>) -> bool {
        if !self.is_empty() || !self.dict.is_empty() {
            return false;
        }
        match FieldDict::from_names(names) {
            Some(dict) => {
                self.dict = dict;
                true
            }
            None => false,
        }
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
        let head_len = codec::encode_record(id, value, digest, &mut self.dict, &mut scratch);
        let slot = self.append_record(&scratch, head_len);
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
                bytes: self.body(*slot),
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
        self.free_slot(slot);
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
            let head = arena.get(*slot);
            let id = codec::head_id(head)?;
            let body = arena.get(body_of(head, *slot)?);
            let value = FlatRef { bytes: body, dict }.value();
            Some((id, value))
        })
    }

    /// The checkpoint image behind arena segment `seg`, if that is what it
    /// is: segment 0 after a mapped load, until the table is cleared.
    #[cfg(all(feature = "mmap-store", not(target_arch = "wasm32")))]
    pub fn image(&self, seg: u16) -> Option<&ImageRef> {
        self.arena.image(seg)
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
        for (head, body) in self.records() {
            let (Some(id), Some(digest)) = (codec::head_id(head), codec::head_digest(head).copied()) else {
                continue;
            };
            if let Some(value) = codec::decode_value(body, &self.dict) {
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

    /// An update retires the old head AND the old body: the accounting is
    /// exact, which is what a compaction decision reads.
    #[test]
    fn update_replaces_and_frees_the_old_bytes() {
        let mut t = RowTable::new();
        put(&mut t, "a", json!({ "v": 1 }));
        assert_eq!(t.dead_bytes(), 0);
        let (old_head, old_body) = t.records().map(|(h, b)| (h.len() as u64, b.len() as u64)).next().unwrap();
        let second = put(&mut t, "a", json!({ "v": 2 }));
        assert_eq!(t.len(), 1, "update must not add a row");
        assert_eq!(t.get("a").to_owned_value(), second);
        assert_eq!(t.dead_bytes(), old_head + old_body, "the superseded head and body are dead");
        let (head, body) = t.records().map(|(h, b)| (h.len() as u64, b.len() as u64)).next().unwrap();
        assert_eq!(t.live_bytes(), head + body);
    }

    #[test]
    fn remove_drops_the_row() {
        let mut t = RowTable::new();
        put(&mut t, "a", json!({ "v": 1 }));
        let (head, body) = t.records().map(|(h, b)| (h.len() as u64, b.len() as u64)).next().unwrap();
        assert!(t.remove("a"));
        assert!(!t.remove("a"), "removing twice reports absent");
        assert_eq!(t.len(), 0);
        assert!(t.get("a").is_missing());
        assert_eq!(t.dead_bytes(), head + body);
        assert_eq!(t.live_bytes(), 0);
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

    /// `heads()` and `records()` describe the same rows, and an appended
    /// record's body sits right behind its head.
    #[test]
    fn heads_and_records_agree() {
        let mut t = RowTable::new();
        for i in 0..8 {
            put(&mut t, &format!("r{i}"), json!({ "n": i, "s": "x".repeat(i) }));
        }
        put(&mut t, "r3", json!({ "n": 30 }));
        t.remove("r5");
        assert_eq!(t.heads().count(), 7);
        let records: Vec<(&[u8], &[u8])> = t.records().collect();
        assert_eq!(records.len(), 7);
        let mut heads: Vec<&[u8]> = t.heads().collect();
        heads.sort();
        let mut from_records: Vec<&[u8]> = records.iter().map(|(h, _)| *h).collect();
        from_records.sort();
        assert_eq!(heads, from_records);
        for (head, body) in &records {
            let id = codec::head_id(head).unwrap();
            assert!(t.contains_key(id));
            assert_eq!(codec::head_body_rel(head), Some(head.len() as u32));
            assert_eq!(codec::head_body_len(head), Some(body.len() as u32));
            assert_eq!(codec::decode_value(body, t.dict()).unwrap(), t.get(id).to_owned_value());
        }
    }

    /// A converter hands over a body encoded against the table's dictionary
    /// plus the head fields; the row reads back as if inserted.
    #[test]
    fn insert_raw_round_trips_and_reports_replacement() {
        let mut src = RowTable::new();
        put(&mut src, "a", json!({ "n": 1, "s": "one", "_00_rv": 4 }));
        put(&mut src, "b", json!({ "n": 2, "nested": { "k": [true, null] } }));
        let mut dst = RowTable::new();
        assert!(dst.restore_dict(src.dict().names()));
        for (head, body) in src.records() {
            let id = codec::head_id(head).unwrap();
            let (_, replaced) = dst.insert_raw(id, codec::head_digest(head).unwrap(), codec::head_rv_raw(head).unwrap(), body);
            assert!(!replaced);
        }
        assert_eq!(dst.len(), 2);
        for id in ["a", "b"] {
            assert_eq!(dst.get(id).to_owned_value(), src.get(id).to_owned_value());
            assert_eq!(dst.digest_of(id), src.digest_of(id));
            assert_eq!(dst.rv_of(id), src.rv_of(id));
        }
        assert_eq!(dst.dead_bytes(), 0);
        let (head, body) = src.records().find(|(h, _)| codec::head_id(h) == Some("a")).unwrap();
        let (_, replaced) = dst.insert_raw("a", codec::head_digest(head).unwrap(), codec::head_rv_raw(head).unwrap(), body);
        assert!(replaced);
        assert_eq!(dst.len(), 2);
        assert!(dst.dead_bytes() > 0);
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

    /// Lay `records` out the way a checkpoint image does: every head, then
    /// every body, with each head pointing at its body across the distance.
    /// Returns the block and the two regions.
    pub(super) fn split_image(records: &[(&[u8], &[u8])]) -> (Vec<u8>, Range<usize>, Range<usize>) {
        let mut block = b"header-bytes-before-the-heads".to_vec();
        let heads_start = block.len();
        let heads_len: usize = records.iter().map(|(h, _)| h.len()).sum();
        let bodies_start = heads_start + heads_len;
        let (mut head_at, mut body_at) = (heads_start, bodies_start);
        for (head, body) in records {
            codec::relink_head(head, (body_at - head_at) as u32, &mut block);
            head_at += head.len();
            body_at += body.len();
        }
        for (_, body) in records {
            block.extend_from_slice(body);
        }
        let bodies_end = block.len();
        block.extend_from_slice(b"trailer");
        (block, heads_start..bodies_start, bodies_start..bodies_end)
    }

    #[test]
    fn index_heads_finds_every_row_without_copying() {
        let mut t = RowTable::new();
        const N: usize = 3000;
        for i in 0..N {
            put(&mut t, &format!("{i:026}"), json!({ "n": i, "s": format!("v{i}") }));
        }
        let records: Vec<(&[u8], &[u8])> = t.records().collect();
        let (block, heads, bodies) = split_image(&records);

        let indexed = RowTable::index_heads(&block, heads, bodies, N as u64, 0).unwrap();
        assert_eq!(indexed.index.len(), N);
        assert_eq!(indexed.live, records.iter().map(|(h, b)| (h.len() + b.len()) as u64).sum::<u64>());
        let mut expected_xor = ssp_protocol::snapshot_hash::xor_empty();
        for (h, _) in &records {
            ssp_protocol::snapshot_hash::xor_digest(&mut expected_xor, codec::head_digest(h).unwrap());
        }
        assert_eq!(indexed.xor, expected_xor);

        let dict = FieldDict::from_names(t.dict().names()).unwrap();
        let back = RowTable::from_parts(dict, indexed.index, Box::new(HeapArena::from_buf(block, indexed.live)));
        for i in 0..N {
            let id = format!("{i:026}");
            assert_eq!(back.get(&id).get("n").as_i64(), Some(i as i64), "{id}");
            assert_eq!(back.digest_of(&id), t.digest_of(&id));
            assert_eq!(back.rv_of(&id), t.rv_of(&id));
        }
        for i in N..N + 200 {
            assert!(back.get(&format!("{i:026}")).is_missing());
        }
        // Still a table: a write after the load retires the image's copy.
        let mut back = back;
        let (seven, eight) = (format!("{:026}", 7), format!("{:026}", 8));
        let dead_before = back.dead_bytes();
        put(&mut back, &seven, json!({ "n": 70 }));
        assert_eq!(back.get(&seven).get("n").as_i64(), Some(70));
        assert!(back.dead_bytes() > dead_before, "the image's copy of the row is dead");
        assert!(back.remove(&eight));
        assert_eq!(back.len(), N - 1);
    }

    #[test]
    fn index_heads_rejects_malformed_regions() {
        let mut t = RowTable::new();
        put(&mut t, "a", json!({ "n": 1 }));
        put(&mut t, "b", json!({ "n": 2 }));
        let records: Vec<(&[u8], &[u8])> = t.records().collect();
        let (block, heads, bodies) = split_image(&records);
        let ix = |block: &[u8], heads: Range<usize>, bodies: Range<usize>, rows: u64| {
            RowTable::index_heads(block, heads, bodies, rows, 0).map(|i| i.index.len()).map_err(|e| e)
        };

        assert_eq!(ix(&block, heads.clone(), bodies.clone(), 2), Ok(2));
        assert_eq!(ix(&block, heads.clone(), bodies.clone(), 1), Err(BlockError::TrailingBytes));
        assert!(matches!(ix(&block, heads.clone(), bodies.clone(), 3), Err(BlockError::Truncated | BlockError::TooManyRows)));
        assert_eq!(ix(&block, heads.start..bodies.start + 1, bodies.clone(), 2), Err(BlockError::Truncated));
        assert_eq!(ix(&block, heads.clone(), bodies.start..block.len() + 1, 2), Err(BlockError::Truncated));
        assert_eq!(ix(&block, heads.clone(), bodies.clone(), u64::MAX), Err(BlockError::TooManyRows));
        // A body region cut short leaves the last body hanging past it.
        assert_eq!(ix(&block, heads.clone(), bodies.start..bodies.end - 1, 2), Err(BlockError::BadBody(1)));
        // A longer body region leaves bytes no body accounts for.
        assert_eq!(ix(&block, heads.clone(), bodies.start..bodies.end + 1, 2), Err(BlockError::TrailingBytes));

        let (twice, h2, b2) = split_image(&[records[0], records[0]]);
        assert_eq!(ix(&twice, h2, b2, 2), Err(BlockError::DuplicateId(1)));

        // A region too small for one head is refused before the walk.
        let stub = vec![0u8; 10];
        assert_eq!(ix(&stub, 0..10, 10..10, 1), Err(BlockError::TooManyRows));

        // A head whose id length runs past the heads has no readable length.
        let mut bad = vec![0u8; codec::HEAD_MIN_LEN];
        bad[codec::HEAD_ID_OFFSET] = 0xC8;
        assert_eq!(ix(&bad, 0..bad.len(), bad.len()..bad.len(), 1), Err(BlockError::Truncated));

        // The first head pointing inside itself, or at the wrong body.
        let head_len = records[0].0.len();
        let mut inward = block.clone();
        let at = heads.start + codec::HEAD_BODY_REL_OFFSET;
        inward[at..at + 4].copy_from_slice(&((head_len - 1) as u32).to_le_bytes());
        assert_eq!(ix(&inward, heads.clone(), bodies.clone(), 2), Err(BlockError::BadBody(0)));
        let mut skewed = block.clone();
        let rel = u32::from_le_bytes(block[at..at + 4].try_into().unwrap());
        skewed[at..at + 4].copy_from_slice(&(rel + 1).to_le_bytes());
        assert_eq!(ix(&skewed, heads.clone(), bodies.clone(), 2), Err(BlockError::BadBody(0)));

        // A tombstone never belongs in a base image.
        let mut tomb = block.clone();
        let at = heads.start + codec::HEAD_BODY_LEN_OFFSET;
        tomb[at..at + 4].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(ix(&tomb, heads.clone(), bodies.clone(), 2), Err(BlockError::Tombstone(0)));
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
        for name in ["alpha", "beta", "gamma", "delta"] {
            assert!(t.dict().id_of(name).is_some(), "{name} interned");
        }
    }

    #[test]
    fn restore_dict_only_on_an_empty_table() {
        let mut t = RowTable::new();
        assert!(t.restore_dict(["a", "b"]));
        assert_eq!(t.dict().id_of("a"), Some(0));
        assert_eq!(t.dict().id_of("b"), Some(1));
        assert!(!t.restore_dict(["c"]), "a second dictionary must be refused");
        assert!(!RowTable::new().restore_dict(["dup", "dup"]), "a repeated name must be refused");
    }
}
