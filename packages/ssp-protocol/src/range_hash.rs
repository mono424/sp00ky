//! Per-id-range table hashes: a table's `x3:` set-hash split by record id, so
//! two sides that disagree about a table can tell WHERE without listing every
//! row of it.
//!
//! The scheduler cuts a table into ranges of about [`RANGE_ROWS`] rows in id
//! order and keeps one XOR accumulator and one row count per range, folded on
//! every applied event exactly like the table hash (the ranges XOR together to
//! it). An SSP whose table hash differs computes the same accumulators over its
//! own rows from the digests it already stores, and only the ranges that
//! disagree are listed and fetched.
//!
//! Two SurrealDB 3.1 facts shape this:
//!
//! - A range is read with the record-id range form, `t:⟨lo⟩..⟨hi⟩`, which is a
//!   key-range scan. `WHERE id >= lo AND id < hi` (and the keyset pager's
//!   `WHERE id > x ORDER BY id LIMIT n`) is a table scan from the first key
//!   with a filter, so its cost grows with the position, not the page.
//! - `LIMIT` is not pushed into a range scan, so a range is always bounded on
//!   both sides except the first and the last one.
//!
//! Membership is by byte order of the record's string key, which is how
//! SurrealDB orders string keys. Only tables whose every key is a plain string
//! ([`string_key`]) get ranges; numeric, uuid, array and object keys sort
//! outside the strings and are left to the full listing.

use crate::snapshot_hash::{xor_acc_from_hex, xor_acc_to_hex, xor_digest, xor_empty};
use serde::{Deserialize, Serialize};

/// Rows per range when ranges are cut from a scan.
pub const RANGE_ROWS: usize = 1000;

/// Most ranges one table is cut into: past `RANGE_ROWS * MAX_RANGES` rows the
/// ranges grow instead, which keeps what is persisted and served per table
/// bounded.
pub const MAX_RANGES: usize = 1024;

/// A range this many times its share of the table is cut again by the next
/// rebuild: keys that only grow (time-ordered ids) all land in the last range.
const OVERGROWN_FACTOR: u64 = 4;

/// The string a raw record id (`abc`, `` `a-b` ``, `⟨a-b⟩`) keys on, when it is
/// a plain string key: letters, digits, `_` and `-`. `None` for a numeric key
/// (a bare all-digit id), and for uuid, array, object or escaped keys.
pub fn string_key(raw: &str) -> Option<&str> {
    let (key, quoted) = match raw
        .strip_prefix('`')
        .and_then(|r| r.strip_suffix('`'))
        .or_else(|| raw.strip_prefix('⟨').and_then(|r| r.strip_suffix('⟩')))
    {
        Some(inner) => (inner, true),
        None => (raw, false),
    };
    let plain = !key.is_empty()
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    let numeric = !quoted && key.bytes().all(|b| b.is_ascii_digit());
    (plain && !numeric).then_some(key)
}

/// The `FROM` target that reads the keys in `[lo, hi)`: `t:⟨lo⟩..⟨hi⟩`, open
/// on a side that is `None`, the whole table when both are. Bounds are string
/// keys as [`string_key`] returns them, so they need no escaping.
pub fn range_target(table: &str, lo: Option<&str>, hi: Option<&str>) -> String {
    match (lo, hi) {
        (None, None) => table.to_string(),
        (Some(lo), None) => format!("{table}:⟨{lo}⟩.."),
        (None, Some(hi)) => format!("{table}:..⟨{hi}⟩"),
        (Some(lo), Some(hi)) => format!("{table}:⟨{lo}⟩..⟨{hi}⟩"),
    }
}

/// The SurrealQL that returns the key every `RANGE_ROWS` rows of `table`, in
/// id order: the range boundaries, from one linear scan that ships only them.
pub fn boundary_query(table: &str) -> String {
    format!("RETURN array::clump((SELECT VALUE id FROM {table}), {RANGE_ROWS}).map(|$c| $c[0])")
}

/// One table's ranges. Range `i` holds the string keys in
/// `[starts[i], starts[i + 1])`; `starts[0]` is always `""`, so every key
/// belongs to exactly one range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeHashes {
    starts: Vec<String>,
    accs: Vec<[u8; 32]>,
    counts: Vec<u64>,
}

impl RangeHashes {
    /// Empty ranges over the given lower bounds. The first is replaced by
    /// `""`, so the first range is open below. `None` when the bounds are not
    /// strictly increasing string keys.
    pub fn with_starts(mut starts: Vec<String>) -> Option<Self> {
        if starts.is_empty() {
            starts.push(String::new());
        }
        starts[0] = String::new();
        let ordered = starts.windows(2).all(|w| w[0] < w[1]);
        let keys = starts[1..].iter().all(|s| string_key(s) == Some(s.as_str()));
        if !ordered || !keys {
            return None;
        }
        let n = starts.len();
        Some(Self {
            starts,
            accs: vec![xor_empty(); n],
            counts: vec![0; n],
        })
    }

    /// Lower bounds from the keys [`boundary_query`] returned (one per
    /// `RANGE_ROWS` rows), merged so there are at most [`MAX_RANGES`].
    pub fn from_boundaries(keys: Vec<String>) -> Option<Self> {
        let step = keys.len().div_ceil(MAX_RANGES).max(1);
        let starts: Vec<String> = keys.into_iter().step_by(step).collect();
        Self::with_starts(starts)
    }

    pub fn len(&self) -> usize {
        self.starts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.starts.is_empty()
    }

    pub fn starts(&self) -> &[String] {
        &self.starts
    }

    pub fn count(&self, i: usize) -> u64 {
        self.counts[i]
    }

    pub fn acc(&self, i: usize) -> &[u8; 32] {
        &self.accs[i]
    }

    /// Rows over every range.
    pub fn rows(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// The range a string key belongs to.
    pub fn range_of_key(&self, key: &str) -> usize {
        self.starts.partition_point(|s| s.as_str() <= key) - 1
    }

    /// The range a raw record id belongs to, `None` for an id that is not a
    /// plain string key.
    pub fn range_of(&self, raw_id: &str) -> Option<usize> {
        string_key(raw_id).map(|k| self.range_of_key(k))
    }

    /// `[lo, hi)` of range `i` as string keys, `None` on an open side.
    pub fn bounds(&self, i: usize) -> (Option<&str>, Option<&str>) {
        let lo = (i > 0).then(|| self.starts[i].as_str());
        let hi = self.starts.get(i + 1).map(String::as_str);
        (lo, hi)
    }

    /// The `FROM` target that reads range `i`.
    pub fn target(&self, table: &str, i: usize) -> String {
        let (lo, hi) = self.bounds(i);
        range_target(table, lo, hi)
    }

    /// Add a row (by its record digest) to range `i`.
    pub fn add_at(&mut self, i: usize, digest: &[u8; 32]) {
        xor_digest(&mut self.accs[i], digest);
        self.counts[i] += 1;
    }

    /// Remove a row (by its record digest) from range `i`.
    pub fn remove_at(&mut self, i: usize, digest: &[u8; 32]) {
        xor_digest(&mut self.accs[i], digest);
        self.counts[i] = self.counts[i].saturating_sub(1);
    }

    /// The table hash: every range folded together.
    pub fn total(&self) -> [u8; 32] {
        let mut acc = xor_empty();
        for a in &self.accs {
            xor_digest(&mut acc, a);
        }
        acc
    }

    /// The same ranges' accumulators over another side's rows, given as
    /// `(raw id, record digest)`. `None` when a row's id is not a plain
    /// string key: that side cannot be compared range by range.
    pub fn accumulate<'a, I>(&self, rows: I) -> Option<Vec<[u8; 32]>>
    where
        I: IntoIterator<Item = (&'a str, [u8; 32])>,
    {
        let mut accs = vec![xor_empty(); self.len()];
        for (id, digest) in rows {
            let i = self.range_of(id)?;
            xor_digest(&mut accs[i], &digest);
        }
        Some(accs)
    }

    /// Ranges whose accumulator differs from `other`'s (as [`Self::accumulate`]
    /// returns them).
    pub fn differing(&self, other: &[[u8; 32]]) -> Vec<usize> {
        (0..self.len())
            .filter(|&i| other.get(i) != Some(&self.accs[i]))
            .collect()
    }

    /// One range holds far more than its share of the table, so a rebuild
    /// would cut it again.
    pub fn overgrown(&self) -> bool {
        let share = (self.rows() / MAX_RANGES as u64).max(RANGE_ROWS as u64);
        self.counts.iter().any(|&c| c > OVERGROWN_FACTOR * share)
    }

    /// The wire form for `table`, carrying the table hash the ranges add up to.
    pub fn to_wire(&self, table: &str) -> TableRanges {
        TableRanges {
            table: table.to_string(),
            hash: xor_acc_to_hex(&self.total()),
            starts: self.starts.clone(),
            hashes: self.accs.iter().map(xor_acc_to_hex).collect(),
            counts: self.counts.clone(),
        }
    }

    /// Parse and check the wire form: lengths agree, bounds are ordered string
    /// keys, every hash parses and together they make `hash`. `None` otherwise.
    pub fn from_wire(wire: &TableRanges) -> Option<Self> {
        let n = wire.starts.len();
        if n == 0 || wire.hashes.len() != n || wire.counts.len() != n || !wire.starts[0].is_empty() {
            return None;
        }
        let mut ranges = Self::with_starts(wire.starts.clone())?;
        for (i, h) in wire.hashes.iter().enumerate() {
            ranges.accs[i] = xor_acc_from_hex(h)?;
        }
        ranges.counts = wire.counts.clone();
        (xor_acc_to_hex(&ranges.total()) == wire.hash).then_some(ranges)
    }
}

/// `POST /proxy/ranges` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeHashesRequest {
    pub table: String,
}

/// One table's ranges on the wire (and as the scheduler persists them).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRanges {
    pub table: String,
    /// The table's `x3:` hash, which the ranges fold to.
    pub hash: String,
    pub starts: Vec<String>,
    pub hashes: Vec<String>,
    pub counts: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot_hash::record_digest;
    use serde_json::json;

    fn d(id: &str) -> [u8; 32] {
        record_digest(id, &json!({ "id": id }))
    }

    #[test]
    fn string_keys_unwrap_quotes_and_refuse_other_key_types() {
        assert_eq!(string_key("abc"), Some("abc"));
        assert_eq!(string_key("0abc"), Some("0abc"));
        assert_eq!(string_key("`123`"), Some("123"));
        assert_eq!(string_key("`a-b`"), Some("a-b"));
        assert_eq!(string_key("⟨a-b⟩"), Some("a-b"));
        // A bare all-digit id is a numeric key: SurrealDB orders it as a number.
        assert_eq!(string_key("123"), None);
        assert_eq!(string_key("u'0190d5d6-0000-7000-8000-000000000000'"), None);
        assert_eq!(string_key("[1, 2]"), None);
        assert_eq!(string_key("`a b`"), None);
        assert_eq!(string_key(""), None);
    }

    #[test]
    fn range_targets_are_key_range_scans() {
        assert_eq!(range_target("game", None, None), "game");
        assert_eq!(range_target("game", Some("a"), None), "game:⟨a⟩..");
        assert_eq!(range_target("game", None, Some("m")), "game:..⟨m⟩");
        assert_eq!(range_target("game", Some("a"), Some("m")), "game:⟨a⟩..⟨m⟩");
        assert_eq!(
            boundary_query("game"),
            "RETURN array::clump((SELECT VALUE id FROM game), 1000).map(|$c| $c[0])"
        );
    }

    #[test]
    fn every_key_lands_in_exactly_one_range() {
        let r = RangeHashes::with_starts(vec!["ignored".into(), "g".into(), "p".into()]).unwrap();
        assert_eq!(r.starts(), &["".to_string(), "g".into(), "p".into()]);
        assert_eq!(r.range_of("0a"), Some(0));
        assert_eq!(r.range_of("f"), Some(0));
        assert_eq!(r.range_of("g"), Some(1));
        assert_eq!(r.range_of("`o-z`"), Some(1));
        assert_eq!(r.range_of("p"), Some(2));
        assert_eq!(r.range_of("zzz"), Some(2));
        assert_eq!(r.range_of("123"), None);
        assert_eq!(r.bounds(0), (None, Some("g")));
        assert_eq!(r.bounds(1), (Some("g"), Some("p")));
        assert_eq!(r.bounds(2), (Some("p"), None));
        assert_eq!(r.target("t", 1), "t:⟨g⟩..⟨p⟩");
    }

    #[test]
    fn unordered_or_non_string_bounds_are_refused() {
        assert!(RangeHashes::with_starts(vec!["".into(), "b".into(), "a".into()]).is_none());
        assert!(RangeHashes::with_starts(vec!["".into(), "a".into(), "a".into()]).is_none());
        assert!(RangeHashes::with_starts(vec!["".into(), "12".into()]).is_none());
        assert!(RangeHashes::with_starts(vec!["".into(), "`a`".into()]).is_none());
        assert_eq!(RangeHashes::with_starts(Vec::new()).unwrap().len(), 1);
    }

    #[test]
    fn boundaries_are_merged_down_to_the_cap() {
        let keys: Vec<String> = (0..3000).map(|i| format!("k{i:05}")).collect();
        let r = RangeHashes::from_boundaries(keys).unwrap();
        assert!(r.len() <= MAX_RANGES, "{} ranges", r.len());
        assert_eq!(r.starts()[1], "k00003");
        let small = RangeHashes::from_boundaries(vec!["a".into(), "f".into()]).unwrap();
        assert_eq!(small.starts(), &["".to_string(), "f".into()]);
    }

    #[test]
    fn folded_ranges_add_up_to_the_table_hash_and_point_at_the_change() {
        let mut ours = RangeHashes::with_starts(vec!["".into(), "g".into(), "p".into()]).unwrap();
        let ids = ["a", "c", "h", "k", "q", "z"];
        let mut table = xor_empty();
        for id in ids {
            let i = ours.range_of(id).unwrap();
            ours.add_at(i, &d(id));
            xor_digest(&mut table, &d(id));
        }
        assert_eq!(ours.total(), table);
        assert_eq!(ours.rows(), 6);
        assert_eq!((ours.count(0), ours.count(1), ours.count(2)), (2, 2, 2));

        // The other side lost `k` and holds a different `q`.
        let theirs: Vec<(&str, [u8; 32])> = vec![
            ("a", d("a")),
            ("c", d("c")),
            ("h", d("h")),
            ("q", record_digest("q", &json!({ "id": "q", "n": 2 }))),
            ("z", d("z")),
        ];
        let accs = ours.accumulate(theirs.iter().map(|(id, dg)| (*id, *dg))).unwrap();
        assert_eq!(ours.differing(&accs), vec![1, 2]);

        // Removing a row restores the accumulator it had without it.
        let i = ours.range_of("k").unwrap();
        ours.remove_at(i, &d("k"));
        assert_eq!(ours.count(1), 1);
        let without_k = ours
            .accumulate(ids.iter().filter(|id| **id != "k").map(|id| (*id, d(id))))
            .unwrap();
        assert!(ours.differing(&without_k).is_empty());

        assert!(ours.accumulate([("42", d("42"))]).is_none());
    }

    #[test]
    fn wire_round_trips_and_rejects_ranges_that_do_not_add_up() {
        let mut r = RangeHashes::with_starts(vec!["".into(), "m".into()]).unwrap();
        r.add_at(0, &d("a"));
        r.add_at(1, &d("x"));
        let wire = r.to_wire("t");
        assert_eq!(wire.hash, xor_acc_to_hex(&r.total()));
        assert_eq!(RangeHashes::from_wire(&wire), Some(r.clone()));

        let mut bad = wire.clone();
        bad.hash = crate::snapshot_hash::xor_empty_table_hash();
        assert!(RangeHashes::from_wire(&bad).is_none());
        let mut short = wire.clone();
        short.counts.pop();
        assert!(RangeHashes::from_wire(&short).is_none());
        let mut open = wire;
        open.starts[0] = "a".into();
        assert!(RangeHashes::from_wire(&open).is_none());
    }

    #[test]
    fn a_range_far_past_its_share_is_overgrown() {
        let mut r = RangeHashes::with_starts(vec!["".into(), "m".into()]).unwrap();
        for i in 0..(RANGE_ROWS * 4) {
            r.add_at(1, &d(&format!("m{i}")));
        }
        assert!(!r.overgrown());
        r.add_at(1, &d("mz"));
        assert!(r.overgrown());
    }
}
