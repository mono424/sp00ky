//! Per-table content hashes used to detect drift between the upstream
//! SurrealDB, the scheduler's replica, and an SSP's circuit store.
//!
//! Both producers (scheduler replica, SSP circuit) feed `(raw_id, value)`
//! pairs through the same code path so digests are bit-identical when the
//! contents agree. JSON objects are recursively key-sorted before
//! serialization, so HashMap iteration order and SurrealDB column ordering
//! can never change the output.
//!
//! Two families, told apart by prefix:
//!
//! - `x3:` — the XOR set-hash of per-record digests (see the section at the
//!   bottom). Order-independent and self-inverse, so both sides maintain it
//!   INCREMENTALLY: the scheduler folds every applied event into it
//!   (`Replica::apply`) and the SSP circuit keeps it per collection
//!   (`catchup_xor`). This is the hash the cluster exchanges — bootstrap
//!   integrity, catch-up verification, `spky verify`. It replaced the sorted
//!   digest there because recomputing that one meant paging every row of a
//!   table out of RocksDB on every snapshot drain; on a 400k-row table that
//!   pinned the scheduler's CPU quota for minutes, every five minutes.
//! - `b3:` — the sorted blake3 digest over all records (`hash_table`,
//!   `TableHasher`). Kept for the standalone circuit checkpoint's resume
//!   point and for tests that pin canonicalization.
//!
//! See `Replica::hash_one_table` (apps/scheduler/src/replica.rs) and
//! `Circuit::compute_catchup_hashes` (packages/ssp/src/circuit/circuit.rs).

use serde_json::Value;
use std::collections::BTreeMap;

const HASH_PREFIX: &str = "b3:";

/// Hash a table's contents. Iterator order does not matter; the function
/// sorts by `raw_id` internally.
pub fn hash_table<I>(records: I) -> String
where
    I: IntoIterator<Item = (String, Value)>,
{
    // Drop reserved `_00_*` metadata keys (notably `_00_rv`) before hashing so
    // the two producers stay bit-identical. The SSP circuit stores rows
    // verbatim from the ingest event payload, which the `_00_<table>_mutation`
    // DB events stamp with `_00_rv` (the record version, read back in
    // `Collection::get_record_version`). The scheduler replica instead hashes a
    // SCHEMAFULL `SELECT *`, which never returns that undefined field. Without
    // this, any table touched by event replay (e.g. `comment` after a write)
    // hashes differently on the two sides, fails the post-replay integrity
    // check, and force-re-bootstraps the SSP in an endless loop. See
    // `strip_reserved_keys`.
    let mut pairs: Vec<(String, Value)> = records
        .into_iter()
        .map(|(id, value)| (normalize_record_id(&id), strip_reserved_keys(value)))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = blake3::Hasher::new();
    for (id, value) in &pairs {
        hasher.update(id.as_bytes());
        hasher.update(b"\0");
        let canonical = canonical_json(value);
        hasher.update(&canonical);
        hasher.update(b"\0");
    }

    format!("{}{}", HASH_PREFIX, hasher.finalize().to_hex())
}

/// Incremental, page-fed builder producing output bit-identical to
/// [`hash_table`]. Callers can stream a large table out of the database in
/// pages (any order) instead of materializing every row as a parsed JSON
/// `Value` at once: each record is normalized and reduced to its canonical
/// byte serialization on `add`, which is typically several times smaller
/// than the `Value` tree it came from. `finish` sorts by normalized id and
/// hashes with the exact framing `hash_table` uses, so the two are
/// interchangeable. See `Replica::hash_one_table` (apps/scheduler).
#[derive(Default)]
pub struct TableHasher {
    pairs: Vec<(String, Vec<u8>)>,
}

impl TableHasher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one record — same normalization pipeline as [`hash_table`]
    /// (id normalization + reserved-key stripping + canonical JSON).
    pub fn add(&mut self, raw_id: &str, value: Value) {
        self.pairs.push((
            normalize_record_id(raw_id),
            canonical_json(&strip_reserved_keys(value)),
        ));
    }

    /// Number of records added so far.
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Finalize into the `b3:`-prefixed table hash.
    pub fn finish(mut self) -> String {
        self.pairs.sort_by(|a, b| a.0.cmp(&b.0));
        let mut hasher = blake3::Hasher::new();
        for (id, canonical) in &self.pairs {
            hasher.update(id.as_bytes());
            hasher.update(b"\0");
            hasher.update(canonical);
            hasher.update(b"\0");
        }
        format!("{}{}", HASH_PREFIX, hasher.finalize().to_hex())
    }
}

/// Strip SurrealDB's identifier escaping (`⟨...⟩` and backticks) from a record
/// id so both hash producers agree on one spelling. Table names that are not
/// plain identifiers (e.g. the `_00_*` synced meta tables, whose leading
/// underscore makes some serializers emit `⟨_00_app_release⟩:web` while others
/// emit `_00_app_release:web`) otherwise hash differently on the scheduler and
/// SSP sides and crash-loop the SSP on a false integrity mismatch.
pub fn normalize_record_id(id: &str) -> String {
    if id.contains('\u{27e8}') || id.contains('`') {
        id.chars().filter(|c| *c != '\u{27e8}' && *c != '\u{27e9}' && *c != '`').collect()
    } else {
        id.to_string()
    }
}

/// Hash for an empty table — useful when comparing tables that exist on one
/// side but not the other.
pub fn empty_table_hash() -> String {
    hash_table(std::iter::empty())
}

/// Normalize a record's top-level object so the scheduler replica and the SSP
/// circuit hash equal content identically. Two top-level keys are dropped:
///
/// 1. Reserved `_00_*` metadata keys — a reserved sp00ky prefix, never user
///    content. The SSP circuit keeps them (the ingest event payload stamps
///    `_00_rv`); the replica's SCHEMAFULL `SELECT *` never returns them.
/// 2. Keys whose value is `null` — treated as absent. The replica's SCHEMAFULL
///    `SELECT *` materializes an unset `option<...>` field as JSON `null`,
///    whereas the SSP circuit stores rows verbatim from the ingest event
///    payload, which OMITS unset optionals. Without this, any row with an unset
///    optional (e.g. `broadcast.target_renderer`, `renderer_device.owner`)
///    hashes differently on the two sides, fails the catch-up integrity check,
///    and force-re-bootstraps the SSP forever.
///
/// Only **top-level** keys are stripped — matching the replica's `SELECT *`,
/// which drops only undefined top-level fields while preserving nested object
/// content inside a defined field. Non-object values pass through unchanged.
fn strip_reserved_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(k, v)| !k.starts_with("_00_") && !v.is_null())
                .map(|(k, v)| {
                    // The row's own `id` field carries the same
                    // escaping ambiguity as the pair key (see
                    // `normalize_record_id`) — normalize it too.
                    if k == "id" {
                        if let Value::String(s) = &v {
                            return (k, Value::String(normalize_record_id(s)));
                        }
                    }
                    (k, v)
                })
                .collect(),
        ),
        other => other,
    }
}

/// Recursively-keyed canonical JSON serialization. Objects are emitted with
/// keys in lexicographic order; arrays preserve order; primitives are
/// formatted by `serde_json` as usual.
pub fn canonical_json(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_canonical(value, &mut buf);
    buf
}

fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                // serde_json escapes the key the same way it would inside a
                // serialized object — reuse it via a temporary Value to keep
                // the encoding identical to non-canonical output for keys.
                let escaped = serde_json::to_vec(&Value::String((*k).clone()))
                    .expect("string serialization is infallible");
                out.extend_from_slice(&escaped);
                out.push(b':');
                write_canonical(&map[*k], out);
            }
            out.push(b'}');
        }
        Value::Array(arr) => {
            out.push(b'[');
            for (i, v) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(v, out);
            }
            out.push(b']');
        }
        other => {
            // For primitives, serde_json's default output is already
            // canonical (numbers don't get reformatted, strings get the
            // same escaping rules).
            let bytes = serde_json::to_vec(other)
                .expect("primitive serialization is infallible");
            out.extend_from_slice(&bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Canonical-JSON writer primitives.
//
// These let a caller emit canonical bytes for a value tree that is NOT a
// `serde_json::Value` — notably the SSP circuit's `Sp00kyValue` — without
// first converting it into one. That conversion was deep-copying every row
// body on every mutation purely to feed the hash.
//
// Each primitive delegates its actual formatting to `serde_json` rather than
// reimplementing it, so escaping and float formatting stay identical to
// `canonical_json` by construction rather than by inspection. Anything that
// diverges here fails the scheduler's integrity check and crash-loops the SSP,
// so "identical by construction" is the only acceptable standard.
// ---------------------------------------------------------------------------

/// Write a JSON string literal (quotes + escaping) exactly as `canonical_json`
/// would for a `Value::String`, including for object keys.
pub fn write_json_string(s: &str, out: &mut Vec<u8>) {
    serde_json::to_writer(&mut *out, s).expect("string serialization is infallible");
}

/// Write a JSON integer.
pub fn write_json_i64(i: i64, out: &mut Vec<u8>) {
    serde_json::to_writer(&mut *out, &i).expect("integer serialization is infallible");
}

/// Write a JSON float, reproducing what a `serde_json::Value` round trip does
/// with one: **non-finite floats become `null`**, because `Number::from_f64`
/// rejects them and `Value::from(f64)` falls back to `Value::Null`. A writer
/// that emitted `NaN` here would diverge from every previously computed hash.
pub fn write_json_f64(f: f64, out: &mut Vec<u8>) {
    match serde_json::Number::from_f64(f) {
        Some(n) => serde_json::to_writer(&mut *out, &n).expect("number serialization is infallible"),
        None => out.extend_from_slice(b"null"),
    }
}

/// Digest a record from canonical bytes the caller already produced. Same
/// framing as [`record_digest`], which is defined in terms of this.
pub fn digest_from_canonical(raw_id: &str, canonical: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(raw_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(canonical);
    *hasher.finalize().as_bytes()
}

/// XOR a precomputed digest into an accumulator. Self-inverse, so this both
/// adds and removes — see [`xor_in`] / [`xor_out`].
///
/// Lets a caller that has already stored a record's digest update the
/// accumulator without re-canonicalizing (or even decoding) the row.
pub fn xor_digest(acc: &mut [u8; 32], digest: &[u8; 32]) {
    for (a, b) in acc.iter_mut().zip(digest.iter()) {
        *a ^= *b;
    }
}

/// Whether a top-level key is dropped before hashing — the reserved `_00_*`
/// prefix. Exposed so a foreign value tree can apply the same rule without
/// duplicating the literal.
pub fn is_reserved_key(key: &str) -> bool {
    key.starts_with("_00_")
}

/// The empty-table hash in the same family as `like`: a table that exists on
/// one side only is compared against "no rows" spelled the way that side
/// spells it, so an `x3:` map and a `b3:` map each get their own zero.
pub fn empty_hash_like(like: &str) -> String {
    if like.starts_with(XOR_PREFIX) {
        xor_empty_table_hash()
    } else {
        empty_table_hash()
    }
}

/// Compare two per-table hash maps and return the tables that disagree.
/// A table missing on one side counts as a mismatch (paired with the
/// empty-table hash of the present side's family, see [`empty_hash_like`]).
pub fn diff_table_hashes(
    a: &BTreeMap<String, String>,
    b: &BTreeMap<String, String>,
) -> Vec<TableHashMismatch> {
    let all: std::collections::BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    all.into_iter()
        .filter_map(|table| {
            let (av, bv) = match (a.get(table), b.get(table)) {
                (Some(av), Some(bv)) => (av.clone(), bv.clone()),
                (Some(av), None) => (av.clone(), empty_hash_like(av)),
                (None, Some(bv)) => (empty_hash_like(bv), bv.clone()),
                (None, None) => return None,
            };
            if av == bv {
                None
            } else {
                Some(TableHashMismatch {
                    table: table.clone(),
                    a: av,
                    b: bv,
                })
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableHashMismatch {
    pub table: String,
    pub a: String,
    pub b: String,
}

// ---------------------------------------------------------------------------
// XOR set-hash (catch-up verification)
//
// `hash_table` above sorts all records and digests them together — correct, but
// it must rescan the whole table to recompute. The catch-up path instead needs
// a hash that can be maintained INCREMENTALLY as records are ingested one event
// at a time, on both the SSP circuit and the scheduler's projection, so the two
// can be compared at the exact sequence cut the SSP caught up to.
//
// The XOR of per-record digests is exactly that: commutative+associative (so
// order-independent, no sort) and self-inverse (so a record can be removed by
// XOR-ing its digest again). On an update, `xor_out(old); xor_in(new)`. Records
// feed through the SAME `strip_reserved_keys` + `canonical_json` path as
// `hash_table`, so a record hashes identically here and there — `_00_*`
// (including `_00_rv`) never affects the digest. Output is prefixed `x3:` to
// keep it visibly distinct from the `b3:` sorted hash in diagnostics.
// ---------------------------------------------------------------------------

/// Reserved-prefix-stripped per-record digest (raw blake3-256 bytes) — the unit
/// of the XOR set-hash. Same canonicalization as [`hash_table`], so equal record
/// content yields an identical digest on the SSP circuit and the scheduler.
pub fn record_digest(raw_id: &str, value: &Value) -> [u8; 32] {
    let stripped = strip_reserved_keys(value.clone());
    digest_from_canonical(raw_id, &canonical_json(&stripped))
}

/// XOR a record's digest into an accumulator (add the record to the set).
pub fn xor_in(acc: &mut [u8; 32], raw_id: &str, value: &Value) {
    xor_digest(acc, &record_digest(raw_id, value));
}

/// Remove a record from the accumulator. XOR is self-inverse, so this is the
/// same operation as [`xor_in`]; named separately for call-site clarity.
pub fn xor_out(acc: &mut [u8; 32], raw_id: &str, value: &Value) {
    xor_in(acc, raw_id, value);
}

/// The accumulator for an empty table (all zeros).
pub fn xor_empty() -> [u8; 32] {
    [0u8; 32]
}

const XOR_PREFIX: &str = "x3:";

/// Format an XOR accumulator as `x3:<hex>`.
pub fn xor_acc_to_hex(acc: &[u8; 32]) -> String {
    let hex: String = acc.iter().map(|b| format!("{:02x}", b)).collect();
    format!("{}{}", XOR_PREFIX, hex)
}

/// Parse an `x3:<hex>` table hash back into its accumulator, so a persisted
/// hash can keep being folded incrementally. `None` for anything that is not
/// a well-formed `x3:` value (including the older `b3:` family).
pub fn xor_acc_from_hex(s: &str) -> Option<[u8; 32]> {
    let hex = s.strip_prefix(XOR_PREFIX)?;
    if hex.len() != 64 {
        return None;
    }
    let mut acc = [0u8; 32];
    for (i, byte) in acc.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(acc)
}

/// The `x3:` hash of an empty table.
pub fn xor_empty_table_hash() -> String {
    xor_acc_to_hex(&xor_empty())
}

/// On-demand XOR set-hash of a table's records — equivalent to folding
/// [`xor_in`] over every record. Used to seed an accumulator from existing rows.
pub fn xor_table_hash<I>(records: I) -> String
where
    I: IntoIterator<Item = (String, Value)>,
{
    let mut acc = xor_empty();
    for (id, value) in records {
        xor_in(&mut acc, &id, &value);
    }
    xor_acc_to_hex(&acc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A record's digest must survive the JSON text the SSP receives it as:
    /// the scheduler hashes its in-process value, the SSP the one it parsed
    /// back. whitepawn's `inventory_item.card_y = 0.09791666666666667` parsed
    /// one bit off without serde_json's `float_roundtrip`, and every SSP
    /// bootstrap of that table failed its integrity check (2026-10-27).
    #[test]
    fn digest_survives_a_json_round_trip_of_its_floats() {
        for f in [0.09791666666666667_f64, 0.8433333333333334, 0.1 + 0.2, 1e-300, 123456.789e10] {
            let row = json!({ "id": "t:1", "x": f });
            let text = serde_json::to_string(&row).unwrap();
            let parsed: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(parsed["x"].as_f64().unwrap().to_bits(), f.to_bits(), "{f} must parse back exactly");
            assert_eq!(record_digest("1", &row), record_digest("1", &parsed), "{f}");
        }
    }

    #[test]
    fn order_independent() {
        let a = vec![
            ("u1".to_string(), json!({"name": "alice", "age": 30})),
            ("u2".to_string(), json!({"name": "bob", "age": 25})),
            ("u3".to_string(), json!({"name": "carol", "age": 40})),
        ];
        let b = vec![
            ("u3".to_string(), json!({"age": 40, "name": "carol"})),
            ("u1".to_string(), json!({"age": 30, "name": "alice"})),
            ("u2".to_string(), json!({"age": 25, "name": "bob"})),
        ];
        assert_eq!(hash_table(a), hash_table(b));
    }

    #[test]
    fn detects_value_change() {
        let a = vec![("u1".to_string(), json!({"v": 1}))];
        let b = vec![("u1".to_string(), json!({"v": 2}))];
        assert_ne!(hash_table(a), hash_table(b));
    }

    #[test]
    fn detects_missing_record() {
        let a = vec![
            ("u1".to_string(), json!({"v": 1})),
            ("u2".to_string(), json!({"v": 2})),
        ];
        let b = vec![("u1".to_string(), json!({"v": 1}))];
        assert_ne!(hash_table(a), hash_table(b));
    }

    #[test]
    fn detects_id_collision_split() {
        // "u1" record with all fields vs two records — id boundary must be
        // honored, otherwise concatenating ids+values could collide.
        let a = vec![("u1".to_string(), json!({"x": 1, "y": 2}))];
        let b = vec![
            ("u1".to_string(), json!({"x": 1})),
            ("u1y".to_string(), json!({"y": 2})),
        ];
        assert_ne!(hash_table(a), hash_table(b));
    }

    #[test]
    fn empty_is_stable() {
        assert_eq!(empty_table_hash(), empty_table_hash());
        assert_eq!(empty_table_hash(), hash_table(std::iter::empty()));
    }

    #[test]
    fn nested_object_canonicalized() {
        let a = vec![("r1".to_string(), json!({"a": {"x": 1, "y": 2}, "b": [1, 2]}))];
        let b = vec![("r1".to_string(), json!({"b": [1, 2], "a": {"y": 2, "x": 1}}))];
        assert_eq!(hash_table(a), hash_table(b));
    }

    #[test]
    fn ignores_reserved_metadata_keys() {
        // The SSP circuit keeps `_00_rv` (injected by the ingest event payload);
        // the SCHEMAFULL replica row never has it. The two MUST hash identically
        // — otherwise every replayed table (e.g. `comment`) trips the integrity
        // check and the SSP re-bootstraps forever.
        let circuit = vec![(
            "CM_x".to_string(),
            json!({"id": "comment:CM_x", "content": "hi", "game": "game:g1", "_00_rv": 7}),
        )];
        let replica = vec![(
            "CM_x".to_string(),
            json!({"id": "comment:CM_x", "content": "hi", "game": "game:g1"}),
        )];
        assert_eq!(hash_table(circuit), hash_table(replica));
    }

    #[test]
    fn reserved_strip_is_top_level_only() {
        // SCHEMAFULL drops only undefined *top-level* fields; nested content
        // inside a defined object field is preserved on both sides, so a change
        // there must still change the hash (don't over-strip).
        let a = vec![("r1".to_string(), json!({"meta": {"_00_rv": 1}}))];
        let b = vec![("r1".to_string(), json!({"meta": {"_00_rv": 2}}))];
        assert_ne!(hash_table(a), hash_table(b));
    }

    #[test]
    fn ignores_null_optional_fields() {
        // The SCHEMAFULL replica `SELECT *` materializes an unset `option<...>`
        // field as JSON null; the SSP circuit stores the event payload, which
        // omits it. The two MUST hash identically — otherwise a row with an
        // unset optional (e.g. `broadcast.target_renderer`) trips the catch-up
        // check and re-bootstraps the SSP forever.
        let replica = vec![(
            "B_x".to_string(),
            json!({"id": "broadcast:B_x", "title": "G", "target_renderer": null, "camera_key": null}),
        )];
        let circuit = vec![(
            "B_x".to_string(),
            json!({"id": "broadcast:B_x", "title": "G"}),
        )];
        assert_eq!(hash_table(replica), hash_table(circuit));
    }

    #[test]
    fn null_strip_is_top_level_only() {
        // A null nested *inside* a defined field is real content on both sides;
        // changing it must still change the hash (don't over-strip).
        let a = vec![("r1".to_string(), json!({"meta": {"x": null}}))];
        let b = vec![("r1".to_string(), json!({"meta": {"x": 1}}))];
        assert_ne!(hash_table(a), hash_table(b));
    }

    #[test]
    fn diff_finds_differing_tables() {
        let mut a = BTreeMap::new();
        a.insert("t1".to_string(), "b3:aaaa".to_string());
        a.insert("t2".to_string(), "b3:bbbb".to_string());

        let mut b = BTreeMap::new();
        b.insert("t1".to_string(), "b3:aaaa".to_string());
        b.insert("t2".to_string(), "b3:cccc".to_string());
        b.insert("t3".to_string(), "b3:dddd".to_string());

        let diffs = diff_table_hashes(&a, &b);
        let names: Vec<&str> = diffs.iter().map(|d| d.table.as_str()).collect();
        assert_eq!(names, vec!["t2", "t3"]);
    }

    // --- XOR set-hash ---

    #[test]
    fn xor_hex_round_trips_and_rejects_the_sorted_family() {
        let mut acc = xor_empty();
        xor_in(&mut acc, "u1", &json!({"v": 1}));
        xor_in(&mut acc, "u2", &json!({"v": 2}));
        let hex = xor_acc_to_hex(&acc);
        assert_eq!(xor_acc_from_hex(&hex), Some(acc));
        assert_eq!(xor_acc_from_hex(&xor_empty_table_hash()), Some(xor_empty()));
        assert_eq!(xor_acc_from_hex(&empty_table_hash()), None, "b3: is not an accumulator");
        assert_eq!(xor_acc_from_hex("x3:zz"), None);
    }

    #[test]
    fn diff_pairs_a_missing_table_with_the_empty_hash_of_the_same_family() {
        // A table with no rows on the SSP has no collection at all, so it is
        // absent from its map; the scheduler holds `x3:00…` for it. Those must
        // compare equal — and the same holds for the `b3:` family.
        let mut xor_side = BTreeMap::new();
        xor_side.insert("t1".to_string(), xor_empty_table_hash());
        assert!(diff_table_hashes(&xor_side, &BTreeMap::new()).is_empty());
        assert!(diff_table_hashes(&BTreeMap::new(), &xor_side).is_empty());

        let mut b3_side = BTreeMap::new();
        b3_side.insert("t1".to_string(), empty_table_hash());
        assert!(diff_table_hashes(&b3_side, &BTreeMap::new()).is_empty());

        // A populated table missing on the other side is still a mismatch.
        let mut populated = BTreeMap::new();
        populated.insert("t1".to_string(), xor_table_hash(vec![("a".to_string(), json!({"v": 1}))]));
        assert_eq!(diff_table_hashes(&populated, &BTreeMap::new()).len(), 1);
    }

    #[test]
    fn xor_is_order_independent() {
        let a = vec![
            ("u1".to_string(), json!({"v": 1})),
            ("u2".to_string(), json!({"v": 2})),
            ("u3".to_string(), json!({"v": 3})),
        ];
        let b = vec![
            ("u3".to_string(), json!({"v": 3})),
            ("u1".to_string(), json!({"v": 1})),
            ("u2".to_string(), json!({"v": 2})),
        ];
        assert_eq!(xor_table_hash(a), xor_table_hash(b));
    }

    #[test]
    fn xor_create_update_delete_round_trips_to_empty() {
        // Lifecycle of one record applied incrementally must return to the
        // empty-table accumulator: create v1, update v1->v2, delete v2.
        let mut acc = xor_empty();
        xor_in(&mut acc, "r1", &json!({"v": 1})); // create
        xor_out(&mut acc, "r1", &json!({"v": 1})); // update: remove old
        xor_in(&mut acc, "r1", &json!({"v": 2})); //         add new
        xor_out(&mut acc, "r1", &json!({"v": 2})); // delete
        assert_eq!(acc, xor_empty());
    }

    #[test]
    fn xor_incremental_matches_on_demand() {
        // Building the accumulator event-by-event (with an intermediate update)
        // must equal hashing the final row set in one pass.
        let mut acc = xor_empty();
        xor_in(&mut acc, "a", &json!({"v": 1}));
        xor_in(&mut acc, "b", &json!({"v": 2}));
        xor_out(&mut acc, "a", &json!({"v": 1})); // a updated 1 -> 9
        xor_in(&mut acc, "a", &json!({"v": 9}));
        let incremental = xor_acc_to_hex(&acc);

        let final_set = vec![
            ("a".to_string(), json!({"v": 9})),
            ("b".to_string(), json!({"v": 2})),
        ];
        assert_eq!(incremental, xor_table_hash(final_set));
    }

    #[test]
    fn xor_ignores_reserved_metadata_keys() {
        // Same parity guarantee as the sorted hash: the SSP circuit keeps
        // `_00_rv`; the SCHEMAFULL replica row never has it. Digests must match.
        let circuit = vec![(
            "CM_x".to_string(),
            json!({"id": "comment:CM_x", "content": "hi", "_00_rv": 7}),
        )];
        let replica = vec![(
            "CM_x".to_string(),
            json!({"id": "comment:CM_x", "content": "hi"}),
        )];
        assert_eq!(xor_table_hash(circuit), xor_table_hash(replica));
    }

    #[test]
    fn xor_ignores_null_optional_fields() {
        // Same null==absent parity as the sorted hash, on the incremental path.
        let replica = vec![(
            "B_x".to_string(),
            json!({"id": "broadcast:B_x", "title": "G", "target_renderer": null}),
        )];
        let circuit = vec![(
            "B_x".to_string(),
            json!({"id": "broadcast:B_x", "title": "G"}),
        )];
        assert_eq!(xor_table_hash(replica), xor_table_hash(circuit));
    }

    #[test]
    fn xor_empty_is_stable_and_detects_change() {
        assert_eq!(xor_table_hash(std::iter::empty()), xor_acc_to_hex(&xor_empty()));
        let a = vec![("u1".to_string(), json!({"v": 1}))];
        let b = vec![("u1".to_string(), json!({"v": 2}))];
        assert_ne!(xor_table_hash(a), xor_table_hash(b));
    }
}

#[cfg(test)]
mod table_hasher_tests {
    use super::*;
    use serde_json::json;

    fn sample_records() -> Vec<(String, Value)> {
        vec![
            (
                "u2".to_string(),
                json!({"id": "user:u2", "name": "b", "opt": null, "_00_rv": 7}),
            ),
            ("u1".to_string(), json!({"id": "user:u1", "name": "a"})),
            (
                "\u{27e8}_00_meta\u{27e9}:x".to_string(),
                json!({"id": "\u{27e8}_00_meta\u{27e9}:x", "v": [1, {"z": 2, "a": 3}]}),
            ),
        ]
    }

    #[test]
    fn table_hasher_matches_hash_table_bit_identically() {
        let full = hash_table(sample_records());
        // Feed in a different order to prove finish() sorting matches.
        let mut incremental = TableHasher::new();
        let mut recs = sample_records();
        recs.reverse();
        for (id, v) in recs {
            incremental.add(&id, v);
        }
        assert_eq!(incremental.finish(), full);
    }

    #[test]
    fn table_hasher_empty_matches_empty_table_hash() {
        assert_eq!(TableHasher::new().finish(), empty_table_hash());
    }
}
