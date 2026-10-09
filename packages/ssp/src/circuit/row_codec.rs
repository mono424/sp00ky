//! Flat byte encoding for stored rows.
//!
//! A parsed `Sp00kyValue` row costs roughly six times its own JSON: a nested
//! `HashMap` per object, a separately heap-allocated `String` for every field
//! name on every row, and ~56 bytes of enum per node. This module encodes the
//! same value into a contiguous byte string that is read in place.
//!
//! # Layout
//!
//! A record is a header followed by one encoded value:
//!
//! ```text
//! [u8; 32] digest    blake3 of the canonical form, computed once at write time
//! [i64]    rv        `_00_rv` lifted out of the body; RV_ABSENT when there is none
//! [varint] id_len
//! [bytes]  id        the raw record id, i.e. the row's own key
//! [value]            tag-prefixed, see below
//! ```
//!
//! The id lives in the record so the index does not have to own a copy of it.
//! That moves one string allocation per row out of anonymous memory and into
//! the arena — which, when the arena is file-backed, is reclaimable page cache
//! instead. It is also what lets a lookup verify a hash match against the real
//! key rather than trusting the hash.
//!
//! Values are tag-prefixed and little-endian:
//!
//! | tag    | payload                                                     |
//! |--------|-------------------------------------------------------------|
//! | `0x00` | Null                                                        |
//! | `0x01` | Bool false                                                  |
//! | `0x02` | Bool true                                                   |
//! | `0x03` | i64, 8 bytes                                                |
//! | `0x04` | f64, 8 bytes                                                |
//! | `0x05` | varint byte length + UTF-8                                  |
//! | `0x06` | varint count + that many values, in order                   |
//! | `0x07` | varint count + sorted `(u32 name_id, u32 offset)` + values   |
//!
//! # Why this shape
//!
//! **No alignment requirement.** Every multi-byte read goes through
//! `from_le_bytes` on a sub-slice, which compiles to one unaligned load on
//! x86-64 and aarch64. Records therefore pack back to back at any offset, with
//! no padding, and a file-backed arena needs no alignment discipline. Every
//! read is also slice-bounds-checked, so corrupt bytes produce a wrong-but-safe
//! value or `None`, never undefined behaviour — which matters once these bytes
//! come from a file rather than from this process.
//!
//! **Object fields are an offset table sorted by field id, not by name.**
//! Field *names* live once per table in a [`FieldDict`], so a row stores 4-byte
//! ids instead of repeating every key. Lookup is one hash of the segment
//! against the dictionary, then a binary search over `u32`s inside one or two
//! cache lines. That is cheaper than the `HashMap<String, _>` probe it
//! replaces, which pays SipHash plus a pointer chase to compare the key bytes.
//!
//! **The digest is stored, not derived.** It is computed at write time from the
//! `Sp00kyValue` via the canonical writer that is already pinned against the
//! `serde_json` pipeline by a differential test. Nothing ever recomputes a
//! digest from decoded bytes, so the encoding cannot drift away from the hash
//! the scheduler verifies against.

use crate::types::Sp00kyValue;
use rustc_hash::FxHashMap;
use smol_str::SmolStr;

pub const TAG_NULL: u8 = 0x00;
pub const TAG_FALSE: u8 = 0x01;
pub const TAG_TRUE: u8 = 0x02;
pub const TAG_INT: u8 = 0x03;
pub const TAG_FLOAT: u8 = 0x04;
pub const TAG_STR: u8 = 0x05;
pub const TAG_ARR: u8 = 0x06;
pub const TAG_OBJ: u8 = 0x07;

/// Sentinel for "this row carries no `_00_rv`". `i64::MIN` rather than -1
/// because -1 is the documented "no versioned row" value at the table level
/// and a row could in principle carry it.
pub const RV_ABSENT: i64 = i64::MIN;

/// Version of the record layout above. A row checkpoint stores records
/// verbatim, so it is only readable by a build that lays records out the same
/// way: bump this on ANY change to the header, the tags or the object table.
/// Readable-but-misread is the failure it guards against, and it is silent:
/// the digest is carried in the header rather than recomputed, so a misread
/// body would still hash as if it were right.
pub const RECORD_FORMAT: u32 = 1;

pub const DIGEST_LEN: usize = 32;
pub const RV_OFFSET: usize = DIGEST_LEN;
/// Offset of the id's length prefix. The value region follows the id, so its
/// position is record-dependent — use [`record_value`].
pub const ID_OFFSET: usize = DIGEST_LEN + 8;

/// Per-table field-name dictionary.
///
/// This is where the savings come from: a table has tens of distinct field
/// names including nested ones, and they are stored once here instead of once
/// per row per field.
#[derive(Debug, Default, Clone)]
pub struct FieldDict {
    names: Vec<SmolStr>,
    ids: FxHashMap<SmolStr, u32>,
}

impl FieldDict {
    pub fn new() -> Self {
        Self::default()
    }

    /// Id for `name`, interning it if new.
    pub fn intern(&mut self, name: &str) -> u32 {
        if let Some(id) = self.ids.get(name) {
            return *id;
        }
        let id = self.names.len() as u32;
        let name: SmolStr = name.into();
        self.names.push(name.clone());
        self.ids.insert(name, id);
        id
    }

    /// Id for `name` if it has been seen. A miss means no row can hold that
    /// field, so lookups short-circuit.
    pub fn id_of(&self, name: &str) -> Option<u32> {
        self.ids.get(name).copied()
    }

    pub fn name(&self, id: u32) -> Option<&str> {
        self.names.get(id as usize).map(|s| s.as_str())
    }

    /// Every interned name, in id order. Interning these into an empty
    /// dictionary in this order reproduces the same ids, which is what lets
    /// encoded records be carried across a checkpoint without re-encoding.
    pub fn names(&self) -> impl Iterator<Item = &str> + '_ {
        self.names.iter().map(|s| s.as_str())
    }

    /// The dictionary that assigns `names` their position as ids, i.e. the
    /// one [`Self::names`] was read from. `None` when a name repeats: the ids
    /// would then not line up with records encoded against the original.
    pub fn from_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Option<Self> {
        let mut dict = Self::new();
        for (expected, name) in names.into_iter().enumerate() {
            if dict.intern(name) as usize != expected {
                return None;
            }
        }
        Some(dict)
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Approximate heap bytes. Small and bounded by the schema, but reported
    /// so the per-table accounting adds up.
    pub fn heap_bytes(&self) -> usize {
        crate::size::vec_bytes::<SmolStr>(self.names.capacity())
            + crate::size::map_table_bytes::<SmolStr, u32>(self.ids.capacity())
            // SmolStr stores up to 22 bytes inline; only longer names spill.
            + self
                .names
                .iter()
                .map(|n| if n.len() > 22 { n.len() * 2 } else { 0 })
                .sum::<usize>()
    }
}

// --- varint ---

fn write_varint(v: u64, out: &mut Vec<u8>) {
    let mut v = v;
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Read a varint, returning the value and the number of bytes consumed.
/// `None` on a truncated or over-long encoding.
fn read_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, byte) in bytes.iter().enumerate() {
        if shift >= 64 {
            return None;
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None
}

// --- encoding ---

/// Encode a row into `out`, returning nothing — the caller owns the buffer.
///
/// `digest` is supplied rather than computed here so it always comes from the
/// canonical writer, which is the thing pinned against the `serde_json`
/// pipeline.
pub fn encode_record(
    id: &str,
    value: &Sp00kyValue,
    digest: &[u8; DIGEST_LEN],
    dict: &mut FieldDict,
    out: &mut Vec<u8>,
) {
    out.clear();
    out.extend_from_slice(digest);
    let rv = match value.get("_00_rv") {
        Some(Sp00kyValue::Int(rv)) => *rv,
        _ => RV_ABSENT,
    };
    out.extend_from_slice(&rv.to_le_bytes());
    write_varint(id.len() as u64, out);
    out.extend_from_slice(id.as_bytes());
    encode_value(value, dict, out);
}

fn encode_value(value: &Sp00kyValue, dict: &mut FieldDict, out: &mut Vec<u8>) {
    match value {
        Sp00kyValue::Null => out.push(TAG_NULL),
        Sp00kyValue::Bool(false) => out.push(TAG_FALSE),
        Sp00kyValue::Bool(true) => out.push(TAG_TRUE),
        Sp00kyValue::Int(i) => {
            out.push(TAG_INT);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Sp00kyValue::Float(f) => {
            out.push(TAG_FLOAT);
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        Sp00kyValue::Str(s) => {
            out.push(TAG_STR);
            write_varint(s.len() as u64, out);
            out.extend_from_slice(s.as_bytes());
        }
        Sp00kyValue::Array(items) => {
            out.push(TAG_ARR);
            write_varint(items.len() as u64, out);
            for item in items {
                encode_value(item, dict, out);
            }
        }
        Sp00kyValue::Object(map) => {
            out.push(TAG_OBJ);
            write_varint(map.len() as u64, out);
            // Intern first so the entry table can be sorted by id, which is
            // what makes lookup a binary search rather than a scan.
            let mut entries: Vec<(u32, &Sp00kyValue)> = map
                .iter()
                .map(|(name, v)| (dict.intern(name), v))
                .collect();
            entries.sort_unstable_by_key(|(id, _)| *id);

            // Reserve the entry table, then backfill each offset once the
            // value it points at has been written.
            let table_start = out.len();
            for (id, _) in &entries {
                out.extend_from_slice(&id.to_le_bytes());
                out.extend_from_slice(&0u32.to_le_bytes());
            }
            let values_base = out.len();
            for (i, (_, v)) in entries.iter().enumerate() {
                let offset = (out.len() - values_base) as u32;
                let slot = table_start + i * 8 + 4;
                out[slot..slot + 4].copy_from_slice(&offset.to_le_bytes());
                encode_value(v, dict, out);
            }
        }
    }
}

// --- header reads ---

/// The stored digest. `None` if the record is too short to hold one.
pub fn record_digest(record: &[u8]) -> Option<&[u8; DIGEST_LEN]> {
    record.get(..DIGEST_LEN)?.try_into().ok()
}

/// The lifted `_00_rv`, or `None` when the row carries none.
///
/// Reading this is two loads at a fixed offset, which is why
/// `max_row_versions` no longer decodes anything.
pub fn record_rv(record: &[u8]) -> Option<i64> {
    let raw = i64::from_le_bytes(record.get(RV_OFFSET..RV_OFFSET + 8)?.try_into().ok()?);
    (raw != RV_ABSENT).then_some(raw)
}

/// The raw record id stored in a record.
///
/// This is the authority a lookup compares against: the index is keyed by a
/// hash, and a hash match is only a candidate until the id itself agrees.
pub fn record_id(record: &[u8]) -> Option<&str> {
    let (len, n) = read_varint(record.get(ID_OFFSET..)?)?;
    let start = ID_OFFSET + n;
    std::str::from_utf8(record.get(start..start + len as usize)?).ok()
}

/// The encoded value region of a record, which follows the id.
pub fn record_value(record: &[u8]) -> &[u8] {
    let Some((len, n)) = record.get(ID_OFFSET..).and_then(read_varint) else {
        return &[];
    };
    record.get(ID_OFFSET + n + len as usize..).unwrap_or(&[])
}

// --- structural reads ---

/// Byte length of the encoded value starting at `bytes[0]`, for skipping.
pub fn value_len(bytes: &[u8]) -> Option<usize> {
    let tag = *bytes.first()?;
    Some(match tag {
        TAG_NULL | TAG_FALSE | TAG_TRUE => 1,
        TAG_INT | TAG_FLOAT => 9,
        TAG_STR => {
            let (len, n) = read_varint(bytes.get(1..)?)?;
            1 + n + len as usize
        }
        TAG_ARR => {
            let (count, n) = read_varint(bytes.get(1..)?)?;
            let mut at = 1 + n;
            for _ in 0..count {
                at += value_len(bytes.get(at..)?)?;
            }
            at
        }
        TAG_OBJ => {
            let (count, n) = read_varint(bytes.get(1..)?)?;
            let count = count as usize;
            let values_base = 1 + n + count * 8;
            let mut at = values_base;
            for _ in 0..count {
                at += value_len(bytes.get(at..)?)?;
            }
            at
        }
        _ => return None,
    })
}

/// Parsed view of an encoded object: its entry table and its value region.
pub struct ObjParts<'a> {
    pub table: &'a [u8],
    pub values: &'a [u8],
    pub count: usize,
}

/// Split an encoded object into its entry table and value region.
pub fn obj_parts(bytes: &[u8]) -> Option<ObjParts<'_>> {
    if *bytes.first()? != TAG_OBJ {
        return None;
    }
    let (count, n) = read_varint(bytes.get(1..)?)?;
    let count = count as usize;
    let table_start = 1 + n;
    let values_base = table_start + count * 8;
    Some(ObjParts {
        table: bytes.get(table_start..values_base)?,
        values: bytes.get(values_base..)?,
        count,
    })
}

/// Find a field by dictionary id via binary search over the sorted entry
/// table, returning its encoded value.
pub fn obj_lookup<'a>(bytes: &'a [u8], name_id: u32) -> Option<&'a [u8]> {
    let parts = obj_parts(bytes)?;
    let (mut lo, mut hi) = (0usize, parts.count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let at = mid * 8;
        let id = u32::from_le_bytes(parts.table.get(at..at + 4)?.try_into().ok()?);
        match id.cmp(&name_id) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                let off =
                    u32::from_le_bytes(parts.table.get(at + 4..at + 8)?.try_into().ok()?) as usize;
                return parts.values.get(off..);
            }
        }
    }
    None
}

/// The `i`-th `(name_id, encoded value)` pair of an object, in id order.
pub fn obj_entry_at<'a>(bytes: &'a [u8], i: usize) -> Option<(u32, &'a [u8])> {
    let parts = obj_parts(bytes)?;
    if i >= parts.count {
        return None;
    }
    let at = i * 8;
    let id = u32::from_le_bytes(parts.table.get(at..at + 4)?.try_into().ok()?);
    let off = u32::from_le_bytes(parts.table.get(at + 4..at + 8)?.try_into().ok()?) as usize;
    Some((id, parts.values.get(off..)?))
}

/// Borrow the UTF-8 payload of an encoded string, without copying.
pub fn str_bytes(bytes: &[u8]) -> Option<&str> {
    if *bytes.first()? != TAG_STR {
        return None;
    }
    let (len, n) = read_varint(bytes.get(1..)?)?;
    std::str::from_utf8(bytes.get(1 + n..1 + n + len as usize)?).ok()
}

/// Number of elements in an encoded array, and its first element.
pub fn arr_parts(bytes: &[u8]) -> Option<(usize, &[u8])> {
    if *bytes.first()? != TAG_ARR {
        return None;
    }
    let (count, n) = read_varint(bytes.get(1..)?)?;
    Some((count as usize, bytes.get(1 + n..)?))
}

// --- decoding ---

/// Decode an encoded value back into an owned `Sp00kyValue`.
///
/// The inverse of [`encode_value`]. Only for callers that genuinely need
/// ownership; ordinary reads go through `ValueRef` and never decode.
pub fn decode_value(bytes: &[u8], dict: &FieldDict) -> Option<Sp00kyValue> {
    let tag = *bytes.first()?;
    match tag {
        TAG_NULL => Some(Sp00kyValue::Null),
        TAG_FALSE => Some(Sp00kyValue::Bool(false)),
        TAG_TRUE => Some(Sp00kyValue::Bool(true)),
        TAG_INT => Some(Sp00kyValue::Int(i64::from_le_bytes(
            bytes.get(1..9)?.try_into().ok()?,
        ))),
        TAG_FLOAT => Some(Sp00kyValue::Float(f64::from_bits(u64::from_le_bytes(
            bytes.get(1..9)?.try_into().ok()?,
        )))),
        TAG_STR => {
            let (len, n) = read_varint(bytes.get(1..)?)?;
            let raw = bytes.get(1 + n..1 + n + len as usize)?;
            Some(Sp00kyValue::Str(std::str::from_utf8(raw).ok()?.to_string()))
        }
        TAG_ARR => {
            let (count, rest) = arr_parts(bytes)?;
            let mut items = Vec::with_capacity(count);
            let mut at = 0usize;
            for _ in 0..count {
                let slice = rest.get(at..)?;
                items.push(decode_value(slice, dict)?);
                at += value_len(slice)?;
            }
            Some(Sp00kyValue::Array(items))
        }
        TAG_OBJ => {
            let parts = obj_parts(bytes)?;
            let mut map = std::collections::HashMap::with_capacity(parts.count);
            for i in 0..parts.count {
                let (id, value_bytes) = obj_entry_at(bytes, i)?;
                let name = dict.name(id)?;
                map.insert(name.to_string(), decode_value(value_bytes, dict)?);
            }
            Some(Sp00kyValue::Object(map))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(j: serde_json::Value) {
        let sv = Sp00kyValue::from(j.clone());
        let mut dict = FieldDict::new();
        let mut buf = Vec::new();
        encode_record("r1", &sv, &[7u8; DIGEST_LEN], &mut dict, &mut buf);
        let decoded = decode_value(record_value(&buf), &dict).expect("decodes");
        assert_eq!(decoded, sv, "round trip changed {j}");
        assert_eq!(record_digest(&buf), Some(&[7u8; DIGEST_LEN]));
    }

    #[test]
    fn scalars_round_trip() {
        for j in [
            json!(null),
            json!(true),
            json!(false),
            json!(0),
            json!(-1),
            json!(i64::MAX),
            json!(i64::MIN),
            json!(1.5),
            json!(-0.0),
            json!(""),
            json!("hello"),
        ] {
            round_trip(j);
        }
    }

    #[test]
    fn non_finite_floats_survive_the_bit_pattern() {
        // Encoded via to_bits, so NaN and the infinities come back exactly —
        // the canonicalization that maps them to null happens at digest time,
        // not at storage time, and the two must not be confused.
        for f in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let sv = Sp00kyValue::Float(f);
            let mut dict = FieldDict::new();
            let mut buf = Vec::new();
            encode_record("r1", &sv, &[0u8; DIGEST_LEN], &mut dict, &mut buf);
            let back = decode_value(record_value(&buf), &dict).unwrap();
            match back {
                Sp00kyValue::Float(g) => {
                    assert_eq!(g.is_nan(), f.is_nan());
                    if !f.is_nan() {
                        assert_eq!(g, f);
                    }
                }
                other => panic!("expected float, got {other:?}"),
            }
        }
    }

    #[test]
    fn containers_round_trip() {
        round_trip(json!({}));
        round_trip(json!([]));
        round_trip(json!({ "a": 1, "z": "two", "m": null }));
        round_trip(json!([1, "two", true, null, 3.5, [], {}]));
        round_trip(json!({ "deep": [[[{ "a": [{ "b": 1 }] }]]] }));
        round_trip(json!({ "unicode key é 🎃": "value 世界", "ctrl\n": "a\tb" }));
    }

    #[test]
    fn int_and_float_stay_distinct() {
        // The hash invariant depends on `5` and `5.0` being different values.
        let i = Sp00kyValue::from(json!({ "n": 5 }));
        let f = Sp00kyValue::from(json!({ "n": 5.0 }));
        assert_ne!(i, f);
        let mut dict = FieldDict::new();
        let (mut a, mut b) = (Vec::new(), Vec::new());
        encode_record("a", &i, &[0; DIGEST_LEN], &mut dict, &mut a);
        encode_record("b", &f, &[0; DIGEST_LEN], &mut dict, &mut b);
        assert_eq!(decode_value(record_value(&a), &dict).unwrap(), i);
        assert_eq!(decode_value(record_value(&b), &dict).unwrap(), f);
    }

    #[test]
    fn rv_is_lifted_into_the_header() {
        let mut dict = FieldDict::new();
        let mut buf = Vec::new();
        encode_record(
            "r1",
            &Sp00kyValue::from(json!({ "_00_rv": 42, "x": 1 })),
            &[0; DIGEST_LEN],
            &mut dict,
            &mut buf,
        );
        assert_eq!(record_rv(&buf), Some(42));

        encode_record(
            "r1",
            &Sp00kyValue::from(json!({ "x": 1 })),
            &[0; DIGEST_LEN],
            &mut dict,
            &mut buf,
        );
        assert_eq!(record_rv(&buf), None, "absent rv must not read as a value");

        // A non-integer `_00_rv` is not a version — matching max_row_versions,
        // which counts only ints so the resume point cannot advance past rows
        // catch-up still needs.
        encode_record(
            "r1",
            &Sp00kyValue::from(json!({ "_00_rv": 5.0 })),
            &[0; DIGEST_LEN],
            &mut dict,
            &mut buf,
        );
        assert_eq!(record_rv(&buf), None);
    }

    #[test]
    fn object_lookup_finds_every_field() {
        let sv = Sp00kyValue::from(json!({
            "alpha": 1, "beta": "two", "gamma": null, "delta": [1, 2], "epsilon": { "n": 9 }
        }));
        let mut dict = FieldDict::new();
        let mut buf = Vec::new();
        encode_record("r1", &sv, &[0; DIGEST_LEN], &mut dict, &mut buf);
        let value = record_value(&buf);

        for name in ["alpha", "beta", "gamma", "delta", "epsilon"] {
            let id = dict.id_of(name).expect("interned");
            let found = obj_lookup(value, id).expect("field present");
            let decoded = decode_value(found, &dict).unwrap();
            assert_eq!(&decoded, sv.get(name).unwrap(), "field {name} mismatched");
        }
        // A dictionary id that this row does not carry must miss cleanly.
        let absent = dict.intern("not_on_this_row");
        assert!(obj_lookup(value, absent).is_none());
    }

    #[test]
    fn entry_table_is_sorted_by_id() {
        // Binary search is only correct if encode sorted the table.
        let sv = Sp00kyValue::from(json!({ "z": 1, "a": 2, "m": 3, "b": 4 }));
        let mut dict = FieldDict::new();
        // Intern in an order that does not match the object's key order.
        dict.intern("a");
        dict.intern("z");
        dict.intern("b");
        dict.intern("m");
        let mut buf = Vec::new();
        encode_record("r1", &sv, &[0; DIGEST_LEN], &mut dict, &mut buf);
        let value = record_value(&buf);
        let mut prev = None;
        for i in 0..4 {
            let (id, _) = obj_entry_at(value, i).unwrap();
            if let Some(p) = prev {
                assert!(id > p, "entry table not sorted: {p} then {id}");
            }
            prev = Some(id);
        }
    }

    #[test]
    fn value_len_skips_exactly() {
        // Two values encoded back to back: skipping the first by value_len
        // must land exactly on the second.
        let mut dict = FieldDict::new();
        let mut buf = Vec::new();
        encode_value(
            &Sp00kyValue::from(json!({ "a": [1, { "b": "xx" }], "c": null })),
            &mut dict,
            &mut buf,
        );
        let first_len = buf.len();
        encode_value(&Sp00kyValue::Int(99), &mut dict, &mut buf);
        assert_eq!(value_len(&buf), Some(first_len));
        assert_eq!(
            decode_value(&buf[first_len..], &dict).unwrap(),
            Sp00kyValue::Int(99)
        );
    }

    #[test]
    fn truncated_input_returns_none_rather_than_panicking() {
        let sv = Sp00kyValue::from(json!({ "a": [1, 2, 3], "b": "text" }));
        let mut dict = FieldDict::new();
        let mut buf = Vec::new();
        encode_record("r1", &sv, &[0; DIGEST_LEN], &mut dict, &mut buf);
        // Every prefix must be handled without panicking. Once the arena is
        // file-backed these bytes are untrusted input.
        for cut in 0..buf.len() {
            let _ = decode_value(record_value(&buf[..cut]), &dict);
            let _ = value_len(record_value(&buf[..cut]));
            let _ = record_rv(&buf[..cut]);
            let _ = record_digest(&buf[..cut]);
            let _ = obj_lookup(record_value(&buf[..cut]), 0);
        }
    }

    #[test]
    fn unknown_tag_is_rejected() {
        let dict = FieldDict::new();
        assert!(decode_value(&[0xff], &dict).is_none());
        assert!(value_len(&[0xff]).is_none());
        assert!(decode_value(&[], &dict).is_none());
    }

    #[test]
    fn dictionary_interning_is_stable() {
        let mut dict = FieldDict::new();
        let a = dict.intern("field");
        let b = dict.intern("field");
        assert_eq!(a, b, "re-interning must return the same id");
        assert_eq!(dict.name(a), Some("field"));
        assert_eq!(dict.id_of("field"), Some(a));
        assert_eq!(dict.id_of("never seen"), None);
        assert_eq!(dict.len(), 1);
    }

    #[test]
    fn varint_round_trips_across_widths() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(v, &mut buf);
            assert_eq!(read_varint(&buf), Some((v, buf.len())), "varint {v}");
        }
        assert_eq!(read_varint(&[]), None);
        // A truncated continuation must not loop or panic.
        assert_eq!(read_varint(&[0x80]), None);
    }
}
