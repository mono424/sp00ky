use crate::algebra::{RowKey, ZSet};
use crate::circuit::store::Store;
use crate::eval::value_ops::resolve_field;
use crate::eval::value_ref::ValueRef;
use crate::operator::plan::OrderSpec;
use crate::types::Sp00kyValue;
use indexset::BTreeSet;
use smallvec::SmallVec;
use smol_str::SmolStr;
use std::collections::HashMap;

/// A row's full sort key: one [`SortableValue`] per `ORDER BY` field.
///
/// Inline for a single field and spilling beyond that. Single-field ordering
/// is the overwhelmingly common case, and it now costs no heap allocation at
/// all — where a `Vec` cost one per row, twice over (once in the sorted
/// buffer, once in the reverse index).
///
/// Inline capacity 1 rather than 2 deliberately: `SortableValue` is 40 bytes,
/// so reserving two slots costs 88 bytes inline on *every* key to save an
/// allocation only multi-field sorts would make — measurably worse overall
/// than spilling those.
type SortKey = SmallVec<[SortableValue; 1]>;

/// Least slack a bounded buffer keeps past its window; see [`TopK`].
const MIN_SLACK: usize = 16;

/// TopK operator with sorted buffer state (Z⁻¹).
///
/// Keeps the smallest input records in a sorted buffer and emits the window
/// `[offset, offset + limit)` of it (SurrealQL `LIMIT limit START offset`).
/// On each delta:
///   1. Insert/remove records from the buffer
///   2. Compute which records enter/leave the window
///   3. Emit +1 for new entrants, -1 for displaced records
///
/// The buffer is bounded at `capacity` (the window plus `limit` of slack).
/// It used to hold every input record: on whitepawn a 50-row window over a
/// 422,000-game database kept 422,000 sort keys twice over, about 60 MB, and
/// built them under the circuit lock in up to 37 s, once per window, so a
/// user scrolling registered a new one every 50 rows until the SSP stalled
/// and neared its memory limit. Records past the bound are dropped and the
/// smallest of them becomes `boundary`; the buffer then holds exactly the
/// input records below it, which is all the window needs while at least
/// `offset + limit` of them remain. Retractions that leave fewer make the
/// operator ask for a rebuild from the store ([`Operator::needs_rebuild`]).
#[derive(Debug)]
pub struct TopK {
    pub limit: usize,
    /// Number of leading sorted rows to skip (SurrealQL `START`). 0 = top-N.
    pub offset: usize,
    pub order_by: Option<Vec<OrderSpec>>,
    /// The smallest input records, sorted. Each entry is (sort_key_parts, row_key).
    /// Using BTreeSet for automatic sorted order.
    buffer: BTreeSet<(SortKey, RowKey)>,
    /// Reverse index: row_key → sort key parts (for removal)
    key_index: HashMap<RowKey, SortKey>,
    /// Most records the buffer keeps.
    capacity: usize,
    /// The smallest record ever dropped for want of room, if any. Every input
    /// record below it is buffered; nothing at or above it is.
    boundary: Option<(SortKey, RowKey)>,
}

/// One field's sort key: an orderable scalar plus the field's direction.
///
/// `Ord` folds the direction in, so a `Vec<SortableValue>` compares
/// lexicographically across fields with mixed directions — which is what a
/// multi-key `ORDER BY a ASC, b DESC` needs. Crucially this works for *every*
/// scalar type, including strings: descending order is the reverse of the
/// scalar comparison, not a negated value. (The previous version negated
/// `Int`/`Bool` but left strings untouched with a "rely on reverse iteration"
/// note — but `current_window`/`snapshot` never reverse, so `DESC` on a string
/// or `datetime` field silently sorted ascending.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortableValue {
    scalar: Scalar,
    descending: bool,
}

/// One orderable scalar.
///
/// Numbers keep their own type rather than being flattened into a fixed-point
/// integer. The previous shape stored `(value * 1_000_000.0) as i64`, which
/// was wrong at both ends of the range: the cast saturates, so any two values
/// above ~9.2e12 compared *equal* and sorted arbitrarily, and anything below
/// 1e-6 truncated to zero and tied with every other tiny value. Timestamps in
/// microseconds and large counters both land in the broken range.
///
/// `Int` and `Float` compare numerically across the pair, matching
/// `compare_values`, so `5` and `5.0` still order together.
#[derive(Debug, Clone, PartialEq)]
enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// `SmolStr` stores up to 22 bytes inline, which covers most sort keys
    /// (an RFC3339 timestamp is 20) and avoids a heap allocation per row.
    Str(SmolStr),
}

impl Eq for Scalar {}

impl Scalar {
    /// Rank across variants, for ordering values of different types.
    fn rank(&self) -> u8 {
        match self {
            Scalar::Null => 0,
            Scalar::Bool(_) => 1,
            Scalar::Int(_) | Scalar::Float(_) => 2,
            Scalar::Str(_) => 3,
        }
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Scalar {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match self.rank().cmp(&other.rank()) {
            Ordering::Equal => {}
            non_equal => return non_equal,
        }
        match (self, other) {
            (Scalar::Null, Scalar::Null) => Ordering::Equal,
            (Scalar::Bool(a), Scalar::Bool(b)) => a.cmp(b),
            (Scalar::Int(a), Scalar::Int(b)) => a.cmp(b),
            // `total_cmp` rather than `partial_cmp`: a NaN sort key must still
            // produce a total order or the BTreeSet's invariants break.
            (Scalar::Float(a), Scalar::Float(b)) => a.total_cmp(b),
            (Scalar::Int(a), Scalar::Float(b)) => (*a as f64).total_cmp(b),
            (Scalar::Float(a), Scalar::Int(b)) => a.total_cmp(&(*b as f64)),
            (Scalar::Str(a), Scalar::Str(b)) => a.cmp(b),
            // Unreachable: equal ranks are covered above.
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for SortableValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let base = self.scalar.cmp(&other.scalar);
        // Within a Vec the same position always carries the same direction (it
        // comes from the same OrderSpec), so reversing on `self.descending` is
        // well-defined.
        if self.descending {
            base.reverse()
        } else {
            base
        }
    }
}

impl SortableValue {
    fn from_value(val: ValueRef<'_>, descending: bool) -> Self {
        let scalar = match val {
            ValueRef::Missing | ValueRef::Null => Scalar::Null,
            ValueRef::Bool(b) => Scalar::Bool(b),
            ValueRef::Int(i) => Scalar::Int(i),
            ValueRef::Float(f) => Scalar::Float(f),
            ValueRef::Str(s) => Scalar::Str(s.into()),
            // Containers have no meaningful order; they sort with nulls, as
            // they always have.
            _ => Scalar::Null,
        };
        SortableValue { scalar, descending }
    }
}

/// Heap bytes held by one row's sort key.
///
/// `SmallVec` keeps up to two keys inline, and `SmolStr` keeps strings up to
/// 22 bytes inline, so a typical single-field sort key now costs no heap at
/// all.
fn sortable_bytes(key: &SortKey) -> usize {
    let spilled = if key.spilled() {
        crate::size::vec_bytes::<SortableValue>(key.len())
    } else {
        0
    };
    spilled
        + key
            .iter()
            .map(|sv| match &sv.scalar {
                Scalar::Str(s) if s.len() > 22 => s.len(),
                _ => 0,
            })
            .sum::<usize>()
}

impl TopK {
    pub fn new(limit: usize, offset: usize, order_by: Option<Vec<OrderSpec>>) -> Self {
        // Slack so ordinary retractions inside the window refill from the
        // buffer instead of costing a rebuild.
        let slack = limit.max(MIN_SLACK);
        Self {
            limit,
            offset,
            order_by,
            buffer: BTreeSet::new(),
            key_index: HashMap::new(),
            capacity: offset.saturating_add(limit).saturating_add(slack),
            boundary: None,
        }
    }

    /// Remember `entry` as dropped: the boundary is the smallest such record.
    fn lower_boundary(&mut self, entry: (SortKey, RowKey)) {
        if self.boundary.as_ref().is_none_or(|b| entry < *b) {
            self.boundary = Some(entry);
        }
    }

    fn compute_sort_key(&self, key: &str, store: &Store) -> SortKey {
        let row = store.get_row_by_key(key);
        match &self.order_by {
            Some(orders) => orders
                .iter()
                .map(|ord| {
                    let val = resolve_field(row, &ord.field);
                    let desc = ord.direction.eq_ignore_ascii_case("DESC");
                    SortableValue::from_value(val, desc)
                })
                .collect(),
            None => SmallVec::from_elem(
                SortableValue {
                    scalar: Scalar::Str(key.into()),
                    descending: false,
                },
                1,
            ),
        }
    }

    /// The keys currently in the output window `[offset, offset + limit)`.
    ///
    /// Reads the window by rank via `get_index` (O(log n) per lookup on the
    /// Fenwick-indexed B-tree) rather than `iter().skip(offset)` (O(offset)), so
    /// the cost is O(limit · log n) — independent of how deep the window is.
    fn current_window(&self) -> Vec<RowKey> {
        let n = self.buffer.len();
        if self.offset >= n {
            return Vec::new();
        }
        let end = (self.offset + self.limit).min(n);
        (self.offset..end)
            .filter_map(|i| self.buffer.get_index(i).map(|(_, key)| key.clone()))
            .collect()
    }
}

impl super::Operator for TopK {
    fn snapshot(&self, inputs: &[&ZSet], store: &Store, _ctx: Option<&Sp00kyValue>) -> ZSet {
        let upstream = inputs[0];
        let mut items: Vec<(SortKey, &RowKey)> = upstream
            .iter()
            .filter(|(_, &w)| w > 0)
            .map(|(key, _)| (self.compute_sort_key(key, store), key))
            .collect();

        items.sort();

        let mut out = HashMap::new();
        for (_, key) in items.into_iter().skip(self.offset).take(self.limit) {
            out.insert(key.clone(), 1);
        }
        out
    }

    fn step(
        &mut self,
        input_deltas: &[&ZSet],
        store: &Store,
        _ctx: Option<&Sp00kyValue>,
    ) -> ZSet {
        let upstream_delta = input_deltas[0];
        let old_top_k = self.current_window();

        for (key, &weight) in upstream_delta {
            if weight > 0 {
                if let Some(stale) = self.key_index.remove(key) {
                    self.buffer.remove(&(stale, key.clone()));
                }
                let entry = (self.compute_sort_key(key, store), key.clone());
                // At or past the boundary, or past a full buffer: not needed.
                let full_and_larger =
                    self.buffer.len() >= self.capacity && self.buffer.last().is_some_and(|last| entry > *last);
                if full_and_larger || self.boundary.as_ref().is_some_and(|b| entry >= *b) {
                    self.lower_boundary(entry);
                    continue;
                }
                self.key_index.insert(key.clone(), entry.0.clone());
                self.buffer.insert(entry);
                if self.buffer.len() > self.capacity {
                    if let Some(evicted) = self.buffer.pop_last() {
                        self.key_index.remove(&evicted.1);
                        self.lower_boundary(evicted);
                    }
                }
            } else if weight < 0 {
                if let Some(sort_key) = self.key_index.remove(key) {
                    self.buffer.remove(&(sort_key, key.clone()));
                }
            }
        }

        let new_top_k = self.current_window();

        // Compute displacement delta
        let mut output_delta = HashMap::new();
        let old_set: std::collections::HashSet<&RowKey> = old_top_k.iter().collect();
        let new_set: std::collections::HashSet<&RowKey> = new_top_k.iter().collect();

        for key in &new_top_k {
            if !old_set.contains(key) {
                *output_delta.entry(key.clone()).or_insert(0) += 1;
            }
        }
        for key in &old_top_k {
            if !new_set.contains(key) {
                *output_delta.entry(key.clone()).or_insert(0) -= 1;
            }
        }

        output_delta.retain(|_, w| *w != 0);
        output_delta
    }

    fn arity(&self) -> usize {
        1
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.key_index.clear();
        self.boundary = None;
    }

    fn needs_rebuild(&self) -> bool {
        // Records were dropped and the window now reaches past what is left:
        // the rows that belong there are among the dropped ones.
        self.boundary.is_some() && self.buffer.len() < self.offset.saturating_add(self.limit)
    }

    fn state_bytes(&self) -> usize {
        // Both structures hold up to `capacity` rows (the window plus slack)
        // — `buffer` to keep them sorted, `key_index` so a retraction can
        // find its sort key again.
        let buffer: usize = self
            .buffer
            .iter()
            .map(|(sort_key, _)| {
                std::mem::size_of::<(SortKey, RowKey)>() + sortable_bytes(sort_key)
            })
            .sum();
        let index: usize =
            crate::size::map_table_bytes::<RowKey, SortKey>(self.key_index.capacity())
                + self
                    .key_index
                    .values()
                    .map(sortable_bytes)
                    .sum::<usize>();
        // Row keys are shared `Arc<str>` clones of the ones the store already
        // holds, so they are deliberately not counted again here.
        buffer + index
    }

    fn evaluate_key(
        &self,
        key: &str,
        input_evals: &[bool],
        _store: &Store,
        _ctx: Option<&Sp00kyValue>,
    ) -> bool {
        // Membership in the CURRENT window, not a pass-through of upstream:
        // the pass-through admitted every updated row into `LIMIT n` views.
        // Content updates that move a row across the window edge go through
        // `reorder_key`, which the circuit prefers when it is available.
        input_evals.first().copied().unwrap_or(false) && self.current_window().iter().any(|k| &**k == key)
    }

    fn reorder_key(
        &mut self,
        key: &str,
        upstream_now: bool,
        store: &Store,
        ctx: Option<&Sp00kyValue>,
    ) -> Option<ZSet> {
        let mut out: ZSet = HashMap::new();
        let mut fold = |delta: ZSet| {
            for (k, w) in delta {
                *out.entry(k).or_insert(0) += w;
            }
        };
        let row_key: RowKey = key.into();
        if self.key_index.contains_key(&row_key) {
            let retract: ZSet = HashMap::from([(row_key.clone(), -1)]);
            fold(self.step(&[&retract], store, ctx));
        }
        if upstream_now {
            let insert: ZSet = HashMap::from([(row_key, 1)]);
            fold(self.step(&[&insert], store, ctx));
        }
        out.retain(|_, w| *w != 0);
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Operator;
    use crate::algebra::ZSetOps;
    use crate::circuit::store::Change;
    use crate::types::Path;
    use serde_json::json;

    fn zset(items: &[(&str, i64)]) -> ZSet {
        items.iter().map(|(k, w)| ((*k).into(), *w)).collect()
    }


    fn by_rank() -> Option<Vec<OrderSpec>> {
        Some(vec![OrderSpec { field: Path::new("rank"), direction: "ASC".into() }])
    }

    fn window_of(op: &TopK) -> Vec<String> {
        op.current_window().iter().map(|k| k.to_string()).collect()
    }

    fn sorted_window(store: &Store, live: &std::collections::BTreeSet<String>, limit: usize, offset: usize) -> Vec<String> {
        let full: ZSet = live.iter().map(|k| (k.as_str().into(), 1)).collect();
        let op = TopK::new(limit, offset, by_rank());
        let mut items: Vec<(SortKey, String)> =
            full.keys().map(|k| (op.compute_sort_key(k, store), k.to_string())).collect();
        items.sort();
        items.into_iter().skip(offset).take(limit).map(|(_, k)| k).collect()
    }

    /// A deep window over a big input keeps only the window plus slack, not
    /// every input row, and still answers exactly what a full sort would.
    #[test]
    fn a_window_over_many_rows_keeps_only_the_window_and_slack() {
        let mut store = Store::new();
        store.ensure_collection("g");
        let mut input = ZSet::new();
        for i in 0..5_000 {
            store.apply_change(&Change::create("g", &format!("r{i}"), json!({ "rank": (i * 7919) % 5_000 })));
            input.insert(format!("g:r{i}").as_str().into(), 1);
        }
        let mut op = TopK::new(50, 100, by_rank());
        op.step(&[&input], &store, None);
        assert!(op.buffer.len() <= op.capacity && op.key_index.len() <= op.capacity, "buffer {} > capacity {}", op.buffer.len(), op.capacity);
        assert_eq!(op.capacity, 200);
        let live: std::collections::BTreeSet<String> = input.keys().map(|k| k.to_string()).collect();
        assert_eq!(window_of(&op), sorted_window(&store, &live, 50, 100));
        assert!(!op.needs_rebuild());
    }

    /// Random inserts and retractions against a full sort: while the operator
    /// does not ask for a rebuild its window is exact, and when it does ask, a
    /// fresh operator primed from the input is exact again.
    #[test]
    fn a_bounded_window_matches_a_full_sort_through_random_churn() {
        let mut store = Store::new();
        store.ensure_collection("g");
        let mut live: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let (limit, offset) = (5usize, 3usize);
        let mut op = TopK::new(limit, offset, by_rank());
        // A deterministic LCG; the crate has no rand dependency.
        let mut seed: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = |n: u64| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        let mut rebuilds = 0;
        for round in 0..2_000 {
            // Alternate mixed churn with phases that only retract, so the
            // buffer drains on every run whatever order a delta's keys come
            // out of the map in.
            let draining = (round / 200) % 2 == 1;
            let mut delta = ZSet::new();
            for _ in 0..(1 + next(6)) {
                let id = format!("r{}", next(40));
                let key = format!("g:{id}");
                if live.contains(&key) && (draining || next(2) == 0) {
                    delta.insert(key.as_str().into(), -1);
                    live.remove(&key);
                } else if !draining && !live.contains(&key) && !delta.contains_key(key.as_str()) {
                    store.apply_change(&Change::create("g", &id, json!({ "rank": next(1_000) })));
                    delta.insert(key.as_str().into(), 1);
                    live.insert(key);
                }
            }
            op.step(&[&delta], &store, None);
            if op.needs_rebuild() {
                rebuilds += 1;
                let full: ZSet = live.iter().map(|k| (k.as_str().into(), 1)).collect();
                op = TopK::new(limit, offset, by_rank());
                op.step(&[&full], &store, None);
            }
            assert!(op.buffer.len() <= op.capacity, "round {round}: buffer over capacity");
            assert_eq!(window_of(&op), sorted_window(&store, &live, limit, offset), "round {round}");
        }
        assert!(rebuilds > 0, "the churn should have drained the buffer at least once");
    }

    /// Sort keys used to be stored as `(value * 1_000_000.0) as i64`. That
    /// cast saturates at `i64::MAX`, so every value above ~9.2e12 collapsed to
    /// the same key and sorted arbitrarily. Epoch milliseconds are already
    /// ~1.7e12 and epoch microseconds are ~1.7e15, so "order by a timestamp"
    /// was squarely in the broken range.
    #[test]
    fn large_numbers_do_not_collapse_to_one_sort_key() {
        let mut store = Store::new();
        store.ensure_collection("events");
        // Three distinct microsecond timestamps, all past the old saturation
        // point.
        let stamps = [1_700_000_000_000_000i64, 1_700_000_000_000_001, 1_700_000_000_000_002];
        for (i, ts) in stamps.iter().enumerate() {
            store.apply_change(&Change::create("events", &format!("e{i}"), json!({ "at": ts })));
        }
        let input = zset(&[("events:e0", 1), ("events:e1", 1), ("events:e2", 1)]);

        let newest = TopK::new(1, 0, Some(vec![OrderSpec {
            field: Path::new("at"),
            direction: "DESC".into(),
        }]))
        .snapshot(&[&input], &store, None);
        assert!(newest.is_present("events:e2"), "newest must win, got {newest:?}");

        let oldest = TopK::new(1, 0, Some(vec![OrderSpec {
            field: Path::new("at"),
            direction: "ASC".into(),
        }]))
        .snapshot(&[&input], &store, None);
        assert!(oldest.is_present("events:e0"), "oldest must win, got {oldest:?}");
    }

    /// The other end of the same bug: multiplying by 1e6 and truncating sent
    /// everything below 1e-6 to zero, so small distinct values tied.
    #[test]
    fn small_numbers_do_not_truncate_to_one_sort_key() {
        let mut store = Store::new();
        store.ensure_collection("m");
        for (i, v) in [1e-9f64, 2e-9, 3e-9].iter().enumerate() {
            store.apply_change(&Change::create("m", &format!("r{i}"), json!({ "v": v })));
        }
        let input = zset(&[("m:r0", 1), ("m:r1", 1), ("m:r2", 1)]);
        let top = TopK::new(1, 0, Some(vec![OrderSpec {
            field: Path::new("v"),
            direction: "DESC".into(),
        }]))
        .snapshot(&[&input], &store, None);
        assert!(top.is_present("m:r2"), "largest small value must win, got {top:?}");
    }

    /// Integers and whole floats have to interleave correctly, matching how
    /// `compare_values` treats them.
    #[test]
    fn ints_and_floats_order_together() {
        let mut store = Store::new();
        store.ensure_collection("n");
        store.apply_change(&Change::create("n", "a", json!({ "v": 1 })));
        store.apply_change(&Change::create("n", "b", json!({ "v": 2.5 })));
        store.apply_change(&Change::create("n", "c", json!({ "v": 3 })));
        let input = zset(&[("n:a", 1), ("n:b", 1), ("n:c", 1)]);
        let top2 = TopK::new(2, 0, Some(vec![OrderSpec {
            field: Path::new("v"),
            direction: "DESC".into(),
        }]))
        .snapshot(&[&input], &store, None);
        assert!(top2.is_present("n:c"));
        assert!(top2.is_present("n:b"));
        assert!(!top2.is_present("n:a"), "1 must not outrank 2.5");
    }

    /// A NaN sort key must not break the `BTreeSet`'s ordering invariants.
    #[test]
    fn nan_sort_keys_stay_totally_ordered() {
        let mut store = Store::new();
        store.ensure_collection("n");
        store.apply_change(&Change::create("n", "a", json!({ "v": 1.0 })));
        store.apply_change(&Change::create("n", "b", json!({ "v": 2.0 })));
        let mut top = TopK::new(2, 0, Some(vec![OrderSpec {
            field: Path::new("v"),
            direction: "ASC".into(),
        }]));
        // Insert, then a NaN row, then read the window — must not panic and
        // must still return a full window.
        let d1 = zset(&[("n:a", 1), ("n:b", 1)]);
        let _ = top.step(&[&d1], &store, None);
        store.apply_change(&Change::create("n", "c", json!({ "v": f64::NAN })));
        let _ = top.step(&[&zset(&[("n:c", 1)])], &store, None);
        assert_eq!(top.buffer.len(), 3);
    }

    #[test]
    fn snapshot_returns_top_k_entries() {
        let mut store = Store::new();
        store.ensure_collection("posts");
        store.apply_change(&Change::create("posts", "post:1", json!({"score": 10})));
        store.apply_change(&Change::create("posts", "post:2", json!({"score": 30})));
        store.apply_change(&Change::create("posts", "post:3", json!({"score": 20})));

        let top_k = TopK::new(
            2,
            0,
            Some(vec![OrderSpec {
                field: Path::new("score"),
                direction: "DESC".into(),
            }]),
        );
        let input = zset(&[("posts:1", 1), ("posts:2", 1), ("posts:3", 1)]);
        let result = top_k.snapshot(&[&input], &store, None);

        assert_eq!(result.len(), 2);
        assert!(result.is_present("posts:2")); // score 30
        assert!(result.is_present("posts:3")); // score 20
    }

    #[test]
    fn snapshot_offset_skips_leading_rows() {
        let mut store = Store::new();
        store.ensure_collection("posts");
        store.apply_change(&Change::create("posts", "post:1", json!({"score": 10})));
        store.apply_change(&Change::create("posts", "post:2", json!({"score": 40})));
        store.apply_change(&Change::create("posts", "post:3", json!({"score": 30})));
        store.apply_change(&Change::create("posts", "post:4", json!({"score": 20})));

        // ORDER BY score DESC → [40, 30, 20, 10]. LIMIT 2 START 1 → [30, 20].
        let top_k = TopK::new(
            2,
            1,
            Some(vec![OrderSpec {
                field: Path::new("score"),
                direction: "DESC".into(),
            }]),
        );
        let input = zset(&[("posts:1", 1), ("posts:2", 1), ("posts:3", 1), ("posts:4", 1)]);
        let result = top_k.snapshot(&[&input], &store, None);

        assert_eq!(result.len(), 2);
        assert!(result.is_present("posts:3")); // score 30 (skipped post:2 @ 40)
        assert!(result.is_present("posts:4")); // score 20
        assert!(!result.is_present("posts:2")); // skipped by START 1
        assert!(!result.is_present("posts:1")); // below the window
    }

    #[test]
    fn step_offset_emits_window_shift_on_insert() {
        let mut store = Store::new();
        store.ensure_collection("posts");
        store.apply_change(&Change::create("posts", "post:1", json!({"score": 10})));
        store.apply_change(&Change::create("posts", "post:2", json!({"score": 40})));
        store.apply_change(&Change::create("posts", "post:3", json!({"score": 20})));

        // LIMIT 1 START 1 → window is the 2nd row. Order [40, 20, 10] → [20].
        let mut top_k = TopK::new(
            1,
            1,
            Some(vec![OrderSpec {
                field: Path::new("score"),
                direction: "DESC".into(),
            }]),
        );
        let d1 = zset(&[("posts:1", 1), ("posts:2", 1), ("posts:3", 1)]);
        let _ = top_k.step(&[&d1], &store, None);

        // Insert score 30 → order [40, 30, 20, 10], window (idx 1) is now 30.
        store.apply_change(&Change::create("posts", "post:4", json!({"score": 30})));
        let d2 = zset(&[("posts:4", 1)]);
        let result = top_k.step(&[&d2], &store, None);

        assert_eq!(result.get("posts:4"), Some(&1)); // enters the window
        assert_eq!(result.get("posts:3"), Some(&-1)); // pushed out (now idx 2)
    }

    #[test]
    fn step_emits_displacement_on_insert() {
        let mut store = Store::new();
        store.ensure_collection("posts");
        store.apply_change(&Change::create("posts", "post:1", json!({"score": 10})));
        store.apply_change(&Change::create("posts", "post:2", json!({"score": 30})));

        let mut top_k = TopK::new(
            2,
            0,
            Some(vec![OrderSpec {
                field: Path::new("score"),
                direction: "DESC".into(),
            }]),
        );

        // Initial: top 2 = [post:2(30), post:1(10)]
        let d1 = zset(&[("posts:1", 1), ("posts:2", 1)]);
        let _ = top_k.step(&[&d1], &store, None);

        // Insert post:3 with score 20 → displaces post:1
        store.apply_change(&Change::create("posts", "post:3", json!({"score": 20})));
        let d2 = zset(&[("posts:3", 1)]);
        let result = top_k.step(&[&d2], &store, None);

        assert_eq!(result.get("posts:3"), Some(&1)); // enters top-K
        assert_eq!(result.get("posts:1"), Some(&-1)); // displaced
    }

    // Reproduces the solid-app game-list query window:
    //   ORDER BY sort_index ASC, date DESC LIMIT n START m
    // All rows share sort_index = 0 (the pre-migration default / a tie), so the
    // secondary `date DESC` is the deciding key — newest date must come first.
    // `date` is a SurrealDB `datetime`, which reaches the SSP as a `Str` (ISO
    // 8601). The window pages must be complete, non-overlapping, and ordered
    // newest→oldest.
    fn game(id: &str, date: &str) -> Change {
        Change::create("game", id, json!({ "sort_index": 0, "date": date }))
    }

    fn game_order() -> Option<Vec<OrderSpec>> {
        Some(vec![
            OrderSpec { field: Path::new("sort_index"), direction: "ASC".into() },
            OrderSpec { field: Path::new("date"), direction: "DESC".into() },
        ])
    }

    #[test]
    fn paged_window_orders_datetime_string_descending() {
        let mut store = Store::new();
        store.ensure_collection("game");
        store.apply_change(&game("game:a", "2020-01-01T00:00:00Z"));
        store.apply_change(&game("game:b", "2021-01-01T00:00:00Z"));
        store.apply_change(&game("game:c", "2022-01-01T00:00:00Z"));
        store.apply_change(&game("game:d", "2023-01-01T00:00:00Z"));
        store.apply_change(&game("game:e", "2024-01-01T00:00:00Z"));
        let input = zset(&[
            ("game:a", 1), ("game:b", 1), ("game:c", 1), ("game:d", 1), ("game:e", 1),
        ]);

        // Expected global order (sort_index ASC tie → date DESC): e, d, c, b, a.
        // Page 0 = LIMIT 2 START 0 → [e, d] (newest two).
        let p0 = TopK::new(2, 0, game_order()).snapshot(&[&input], &store, None);
        assert_eq!(p0.len(), 2);
        assert!(p0.is_present("game:e"), "page 0 must hold the newest game");
        assert!(p0.is_present("game:d"), "page 0 must hold the 2nd-newest game");

        // Page 1 = LIMIT 2 START 2 → [c, b].
        let p1 = TopK::new(2, 2, game_order()).snapshot(&[&input], &store, None);
        assert_eq!(p1.len(), 2, "page 1 (START 2) must be a full window");
        assert!(p1.is_present("game:c"));
        assert!(p1.is_present("game:b"));

        // Page 2 = LIMIT 2 START 4 → [a] (short tail = real end).
        let p2 = TopK::new(2, 4, game_order()).snapshot(&[&input], &store, None);
        assert_eq!(p2.len(), 1);
        assert!(p2.is_present("game:a"));

        // Windows must not overlap.
        assert!(!p0.is_present("game:c") && !p0.is_present("game:a"));
        assert!(!p1.is_present("game:e") && !p1.is_present("game:a"));
    }

    #[test]
    fn step_no_displacement_when_below_cutoff() {
        let mut store = Store::new();
        store.ensure_collection("posts");
        store.apply_change(&Change::create("posts", "post:1", json!({"score": 10})));
        store.apply_change(&Change::create("posts", "post:2", json!({"score": 30})));

        let mut top_k = TopK::new(
            2,
            0,
            Some(vec![OrderSpec {
                field: Path::new("score"),
                direction: "DESC".into(),
            }]),
        );
        let d1 = zset(&[("posts:1", 1), ("posts:2", 1)]);
        let _ = top_k.step(&[&d1], &store, None);

        // Insert post:3 with score 5 → below cutoff
        store.apply_change(&Change::create("posts", "post:3", json!({"score": 5})));
        let d2 = zset(&[("posts:3", 1)]);
        let result = top_k.step(&[&d2], &store, None);

        assert!(result.is_empty());
    }

    // Regression guard for the deep-window slowdown: `current_window()` must read
    // its window by rank (O(limit·log n)), NOT by `iter().skip(offset)` (O(offset)).
    // With the rank-based read the cost is ~flat across offsets; the old skip made
    // offset 10000 hundreds of times slower than offset 0. We assert a generous
    // ratio so it only trips on a true O(offset) regression, never on CI noise.
    #[test]
    fn current_window_cost_is_offset_independent() {
        use std::time::Instant;

        let mut store = Store::new();
        store.ensure_collection("posts");
        let n: usize = 20_000;
        let mut items: Vec<(String, i64)> = Vec::with_capacity(n);
        for i in 0..n {
            let id = format!("post:{}", i);
            store.apply_change(&Change::create("posts", &id, json!({ "score": i as i64 })));
            items.push((format!("posts:{}", i), 1));
        }

        // Seed all rows in one delta (mirrors Circuit::run_initial_snapshot).
        // One operator per depth: the buffer is bounded at the window plus
        // slack, so it only reaches as deep as the offset it was built for.
        let delta: ZSet = items.into_iter().map(|(k, w)| (k.into(), w)).collect();
        let seeded = |offset: usize| {
            let mut top_k = TopK::new(
                30,
                offset,
                Some(vec![OrderSpec {
                    field: Path::new("score"),
                    direction: "ASC".into(),
                }]),
            );
            let _ = top_k.step(&[&delta], &store, None);
            assert_eq!(top_k.buffer.len(), offset + 60);
            top_k
        };

        let measure = |tk: &TopK, iters: usize| -> u128 {
            let mut acc = 0usize;
            let t0 = Instant::now();
            for _ in 0..iters {
                acc = acc.wrapping_add(tk.current_window().len());
            }
            // Keep `acc` observable so the loop isn't optimized away.
            assert!(acc > 0);
            t0.elapsed().as_nanos()
        };

        let iters = 2000;
        let (shallow, mid, deep) = (seeded(0), seeded(1_000), seeded(10_000));
        let _ = measure(&shallow, 200); // warm up
        let t_shallow = measure(&shallow, iters);
        let t_mid = measure(&mid, iters);
        let t_deep = measure(&deep, iters);

        eprintln!(
            "current_window {} iters: offset0={}ns offset1k={}ns offset10k={}ns",
            iters, t_shallow, t_mid, t_deep
        );

        // O(offset) at depth 10000 would be ~300x the work at offset 0; O(log n)
        // is ~1-2x. 10x cleanly separates them with margin for a noisy machine. A
        // 1µs floor keeps a near-zero shallow time from exploding the ratio.
        let floor: u128 = 1_000;
        assert!(
            t_deep <= 10 * t_shallow.max(floor),
            "current_window at offset 10000 ({}ns) scales with offset vs offset 0 ({}ns) — O(offset) regression?",
            t_deep,
            t_shallow
        );
    }
}
