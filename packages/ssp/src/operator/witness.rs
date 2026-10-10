//! Indexed presence joins shared by permission semi- and anti-joins.
//!
//! Retain the old join value with each input row: the store already holds the
//! new value (or no row) when a retraction or content update reaches us.
//! Hash buckets deliberately still use `hash_value` followed by
//! `compare_values`, exactly like the snapshot implementations. In particular,
//! numeric comparison is not an equivalence relation for all Int/Float values,
//! so grouping witnesses with a new `Eq` implementation would change results.

use crate::algebra::{RowKey, ZSet};
use crate::circuit::store::Store;
use crate::eval::value_ops::{compare_values, hash_value, resolve_field};
use crate::eval::value_ref::ValueRef;
use crate::operator::plan::JoinCondition;
use crate::types::{Path, Sp00kyValue};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Exact representations are safe to count even when join equality is not
/// transitive (large Int/Float pairs, NaN). Matching still uses the original
/// hash and comparison, rather than this representation's equality.
#[derive(Clone, Debug)]
struct WitnessValue(Sp00kyValue);

impl WitnessValue {
    fn as_ref(&self) -> ValueRef<'_> {
        ValueRef::from_value(&self.0)
    }

    fn kind(&self) -> usize {
        match self.0 {
            Sp00kyValue::Null => 0,
            Sp00kyValue::Bool(_) => 1,
            Sp00kyValue::Int(_) => 2,
            Sp00kyValue::Float(_) => 3,
            Sp00kyValue::Str(_) => 4,
            Sp00kyValue::Array(_) => 5,
            Sp00kyValue::Object(_) => 6,
        }
    }

    fn is_nan(&self) -> bool {
        matches!(self.0, Sp00kyValue::Float(v) if v.is_nan())
    }
}

impl PartialEq for WitnessValue {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Sp00kyValue::Null, Sp00kyValue::Null)
            | (Sp00kyValue::Array(_), Sp00kyValue::Array(_))
            | (Sp00kyValue::Object(_), Sp00kyValue::Object(_)) => true,
            (Sp00kyValue::Bool(a), Sp00kyValue::Bool(b)) => a == b,
            (Sp00kyValue::Int(a), Sp00kyValue::Int(b)) => a == b,
            (Sp00kyValue::Float(a), Sp00kyValue::Float(b)) => a.to_bits() == b.to_bits(),
            (Sp00kyValue::Str(a), Sp00kyValue::Str(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for WitnessValue {}

impl Hash for WitnessValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            Sp00kyValue::Bool(v) => v.hash(state),
            Sp00kyValue::Int(v) => v.hash(state),
            Sp00kyValue::Float(v) => v.to_bits().hash(state),
            Sp00kyValue::Str(v) => v.hash(state),
            _ => {}
        }
    }
}

#[derive(Debug)]
struct InputRow {
    weight: i64,
    exists: bool,
    value: Option<WitnessValue>,
}

impl InputRow {
    fn read(key: &str, weight: i64, field: &Path, store: &Store) -> Self {
        let row = store.get_row_by_key(key);
        let value = resolve_field(row, field);
        // Containers compare by presence/kind, never contents. Keep that
        // representation without copying a potentially large nested body.
        let value = if weight <= 0 || value.is_missing() {
            None
        } else {
            Some(WitnessValue(match value {
                ValueRef::Arr(_) => Sp00kyValue::Array(Vec::new()),
                ValueRef::Obj(_) => Sp00kyValue::Object(HashMap::new()),
                _ => value.to_owned_value(),
            }))
        };
        Self {
            weight,
            exists: !row.is_missing(),
            value,
        }
    }

    fn hash(&self) -> Option<u64> {
        self.value.as_ref().map(|v| hash_value(v.as_ref()))
    }
}

#[derive(Debug, Default)]
pub(super) struct WitnessState {
    left: HashMap<RowKey, InputRow>,
    right: HashMap<RowKey, InputRow>,
    left_by_hash: HashMap<u64, HashSet<RowKey>>,
    right_by_hash: HashMap<u64, HashMap<WitnessValue, usize>>,
    right_kind_counts: [usize; 7],
    right_nan_count: usize,
    output: HashSet<RowKey>,
    content_updates: HashSet<RowKey>,
    #[cfg(test)]
    pub(super) refreshed_rows: usize,
    #[cfg(test)]
    pub(super) evaluated_left_rows: usize,
}

fn remove_bucket(buckets: &mut HashMap<u64, HashSet<RowKey>>, hash: u64, key: &RowKey) {
    if let Some(bucket) = buckets.get_mut(&hash) {
        bucket.remove(key);
        if bucket.is_empty() {
            buckets.remove(&hash);
        }
    }
}

impl WitnessState {
    pub(super) fn note_content_updates(&mut self, keys: &[RowKey]) {
        self.content_updates.extend(keys.iter().cloned());
    }

    pub(super) fn has_witness(&self, value: ValueRef<'_>) -> bool {
        if value.is_missing() {
            return false;
        }
        self.right_by_hash
            .get(&hash_value(value))
            .is_some_and(|bucket| {
                bucket
                    .keys()
                    .any(|right| compare_values(value, right.as_ref()) == Ordering::Equal)
            })
    }

    /// Point reevaluation historically used compare_values without a hash
    /// guard. Preserve that behavior with constant-size type summaries plus
    /// indexed numeric/string lookups, including NaN and signed zero.
    pub(super) fn has_point_witness(&self, value: ValueRef<'_>) -> bool {
        let scalar_count: usize = self.right_kind_counts[1..].iter().sum();
        let numeric_count = self.right_kind_counts[2] + self.right_kind_counts[3];
        match value {
            ValueRef::Missing => false,
            ValueRef::Null => self.right_kind_counts[0] > 0,
            ValueRef::Bool(_) => {
                scalar_count > self.right_kind_counts[1] || self.has_witness(value)
            }
            ValueRef::Str(_) => scalar_count > self.right_kind_counts[4] || self.has_witness(value),
            ValueRef::Arr(_) | ValueRef::Obj(_) => scalar_count > 0,
            ValueRef::Int(_) | ValueRef::Float(_) => {
                let is_nan = matches!(value, ValueRef::Float(v) if v.is_nan());
                let is_zero = matches!(value, ValueRef::Int(0))
                    || matches!(value, ValueRef::Float(v) if v == 0.0);
                scalar_count > numeric_count
                    || self.right_nan_count > 0
                    || is_nan && numeric_count > 0
                    || self.has_witness(value)
                    || is_zero
                        && (self.has_witness(ValueRef::Float(0.0))
                            || self.has_witness(ValueRef::Float(-0.0)))
            }
        }
    }

    pub(super) fn step(
        &mut self,
        deltas: &[&ZSet],
        store: &Store,
        condition: &JoinCondition,
        anti: bool,
    ) -> ZSet {
        #[cfg(test)]
        {
            self.refreshed_rows = 0;
            self.evaluated_left_rows = 0;
        }
        let mut left_keys: HashSet<RowKey> = deltas[0].keys().cloned().collect();
        let mut right_keys: HashSet<RowKey> = deltas[1].keys().cloned().collect();
        for key in self.content_updates.drain() {
            if self.left.contains_key(&key) {
                left_keys.insert(key.clone());
            }
            if self.right.contains_key(&key) {
                right_keys.insert(key);
            }
        }
        let mut affected = left_keys.clone();
        let mut changed_hashes = HashSet::new();
        let mut witness_deltas: HashMap<WitnessValue, i64> = HashMap::new();
        for key in left_keys {
            #[cfg(test)]
            {
                self.refreshed_rows += 1;
            }
            let old = self.left.remove(&key);
            let weight = old.as_ref().map_or(0, |row| row.weight)
                + deltas[0].get(&key).copied().unwrap_or(0);
            if let Some(hash) = old.as_ref().and_then(InputRow::hash) {
                remove_bucket(&mut self.left_by_hash, hash, &key);
            }
            if weight != 0 {
                let row = InputRow::read(&key, weight, &condition.left_field, store);
                if let Some(hash) = row.hash() {
                    self.left_by_hash
                        .entry(hash)
                        .or_default()
                        .insert(key.clone());
                }
                self.left.insert(key, row);
            }
        }
        for key in right_keys {
            #[cfg(test)]
            {
                self.refreshed_rows += 1;
            }
            let old = self.right.remove(&key);
            let weight = old.as_ref().map_or(0, |row| row.weight)
                + deltas[1].get(&key).copied().unwrap_or(0);
            if let Some(value) = old.and_then(|row| row.value) {
                *witness_deltas.entry(value).or_default() -= 1;
            }
            if weight != 0 {
                let row = InputRow::read(&key, weight, &condition.right_field, store);
                if let Some(value) = &row.value {
                    *witness_deltas.entry(value.clone()).or_default() += 1;
                }
                self.right.insert(key, row);
            }
        }
        // Apply net witness changes only after integrating the entire batch.
        // Duplicate witnesses and unrelated body updates therefore do not
        // invalidate even their own left bucket.
        for (value, delta) in witness_deltas {
            if delta == 0 {
                continue;
            }
            self.right_kind_counts[value.kind()] =
                (self.right_kind_counts[value.kind()] as i64 + delta) as usize;
            if value.is_nan() {
                self.right_nan_count = (self.right_nan_count as i64 + delta) as usize;
            }
            let hash = hash_value(value.as_ref());
            let bucket = self.right_by_hash.entry(hash).or_default();
            let old = bucket.get(&value).copied().unwrap_or(0);
            let new = (old as i64 + delta) as usize;
            if (old > 0) != (new > 0) {
                changed_hashes.insert(hash);
            }
            if new == 0 {
                bucket.remove(&value);
                if bucket.is_empty() {
                    self.right_by_hash.remove(&hash);
                }
            } else {
                bucket.insert(value, new);
            }
        }
        for hash in changed_hashes {
            if let Some(keys) = self.left_by_hash.get(&hash) {
                affected.extend(keys.iter().cloned());
            }
        }
        let mut output = ZSet::new();
        for key in affected {
            #[cfg(test)]
            {
                self.evaluated_left_rows += 1;
            }
            let present = self.left.get(&key).is_some_and(|row| {
                row.weight > 0
                    && row.exists
                    && (anti
                        != row
                            .value
                            .as_ref()
                            .is_some_and(|v| self.has_witness(v.as_ref())))
            });
            let was_present = self.output.contains(&key);
            if present != was_present {
                if present {
                    self.output.insert(key.clone());
                    output.insert(key, 1);
                } else {
                    self.output.remove(&key);
                    output.insert(key, -1);
                }
            }
        }
        output
    }

    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }

    pub(super) fn state_bytes(&self) -> usize {
        fn rows_bytes(rows: &HashMap<RowKey, InputRow>) -> usize {
            crate::size::map_table_bytes::<RowKey, InputRow>(rows.capacity())
                + rows
                    .values()
                    .filter_map(|row| row.value.as_ref())
                    .map(|v| v.0.heap_bytes())
                    .sum::<usize>()
        }
        fn buckets_bytes(buckets: &HashMap<u64, HashSet<RowKey>>) -> usize {
            crate::size::map_table_bytes::<u64, HashSet<RowKey>>(buckets.capacity())
                + buckets
                    .values()
                    .map(|bucket| crate::size::map_table_bytes::<RowKey, ()>(bucket.capacity()))
                    .sum::<usize>()
        }
        rows_bytes(&self.left)
            + rows_bytes(&self.right)
            + buckets_bytes(&self.left_by_hash)
            + crate::size::map_table_bytes::<u64, HashMap<WitnessValue, usize>>(
                self.right_by_hash.capacity(),
            )
            + self
                .right_by_hash
                .values()
                .map(|bucket| {
                    crate::size::map_table_bytes::<WitnessValue, usize>(bucket.capacity())
                        + bucket.keys().map(|v| v.0.heap_bytes()).sum::<usize>()
                })
                .sum::<usize>()
            + crate::size::map_table_bytes::<RowKey, ()>(self.output.capacity())
            + crate::size::map_table_bytes::<RowKey, ()>(self.content_updates.capacity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::ZSetOps;
    use crate::circuit::store::Change;
    use crate::operator::{AntiJoin, Operator, SemiJoin};
    use serde_json::json;

    struct Harness {
        store: Store,
        condition: JoinCondition,
        inputs: [ZSet; 2],
        joins: [WitnessState; 2],
        outputs: [ZSet; 2],
    }

    impl Harness {
        fn new() -> Self {
            Self {
                store: Store::new(),
                condition: JoinCondition {
                    left_field: Path::new("join"),
                    right_field: Path::new("join"),
                },
                inputs: [ZSet::new(), ZSet::new()],
                joins: [WitnessState::default(), WitnessState::default()],
                outputs: [ZSet::new(), ZSet::new()],
            }
        }

        fn step(&mut self, deltas: [ZSet; 2], changed: &[RowKey]) {
            self.inputs[0].add(&deltas[0]);
            self.inputs[1].add(&deltas[1]);
            let refs = [&self.inputs[0], &self.inputs[1]];
            let expected = [
                SemiJoin::new(self.condition.clone()).snapshot(&refs, &self.store, None),
                AntiJoin::new(self.condition.clone()).snapshot(&refs, &self.store, None),
            ];
            for (i, next) in expected.into_iter().enumerate() {
                self.joins[i].note_content_updates(changed);
                let actual = self.joins[i].step(
                    &[&deltas[0], &deltas[1]],
                    &self.store,
                    &self.condition,
                    i == 1,
                );
                assert_eq!(
                    actual,
                    self.outputs[i].diff(&next),
                    "{} join delta",
                    if i == 1 { "anti" } else { "semi" }
                );
                self.outputs[i].add(&actual);
                assert_eq!(self.outputs[i], next);
            }
        }

        fn mutate(&mut self, changes: Vec<Change>) {
            let mut deltas = [ZSet::new(), ZSet::new()];
            let mut changed = Vec::new();
            for change in changes {
                let side = usize::from(change.table == "right");
                let (key, weight) = self.store.apply_change(&change);
                *deltas[side].entry(key.clone()).or_insert(0) += weight;
                changed.push(key);
            }
            for delta in &mut deltas {
                delta.retain(|_, weight| *weight != 0);
            }
            self.step(deltas, &changed);
        }
    }

    #[test]
    fn content_updates_duplicate_witnesses_and_recreation_match_snapshots() {
        let mut h = Harness::new();
        h.mutate(vec![
            Change::create("left", "a", json!({"join": 1})),
            Change::create("left", "b", json!({"join": 2})),
            Change::create("left", "missing", json!({})),
            Change::create("right", "a", json!({"join": 1})),
            Change::create("right", "b", json!({"join": 1.0})),
        ]);
        // A duplicate witness goes away without retracting the left row.
        h.mutate(vec![Change::delete("right", "a")]);
        h.mutate(vec![Change::merge("right", "b", json!({"join": 2}))]);
        h.mutate(vec![Change::update("left", "a", json!({"join": 2}))]);
        h.mutate(vec![Change::update("right", "b", json!({}))]);
        h.mutate(vec![Change::update("right", "b", json!({"join": null}))]);
        h.mutate(vec![Change::merge(
            "left",
            "missing",
            json!({"join": null}),
        )]);
        // The net membership delta is empty, but the old witness must move.
        h.mutate(vec![
            Change::delete("right", "b"),
            Change::create("right", "b", json!({"join": 2})),
        ]);
        h.mutate(vec![
            Change::delete("left", "a"),
            Change::create("left", "a", json!({"join": 7})),
        ]);
        h.mutate(vec![]);
        for join in &mut h.joins {
            join.reset();
        }
        h.outputs = [ZSet::new(), ZSet::new()];
        let inputs = std::mem::replace(&mut h.inputs, [ZSet::new(), ZSet::new()]);
        h.step(inputs, &[]);
    }

    fn value(n: u64) -> Sp00kyValue {
        match n % 14 {
            0 => Sp00kyValue::Null,
            1 => Sp00kyValue::Bool(false),
            2 => Sp00kyValue::Bool(true),
            3 => Sp00kyValue::Int(1),
            4 => Sp00kyValue::Float(1.0),
            5 => Sp00kyValue::Int(9_007_199_254_740_992),
            6 => Sp00kyValue::Int(9_007_199_254_740_993),
            7 => Sp00kyValue::Float(9_007_199_254_740_992.0),
            8 => Sp00kyValue::Float(f64::NAN),
            9 => Sp00kyValue::Float(-0.0),
            10 => Sp00kyValue::Str("one".into()),
            11 => Sp00kyValue::Array(vec![Sp00kyValue::Int(n as i64)]),
            12 => Sp00kyValue::Object(HashMap::from([("n".into(), Sp00kyValue::Int(n as i64))])),
            _ => Sp00kyValue::Float(f64::INFINITY),
        }
    }

    #[test]
    fn deterministic_churn_matches_snapshot_oracles_for_mixed_values_and_batches() {
        for seed in 1..=4 {
            let mut rng = seed * 0x9E37_79B9;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let mut h = Harness::new();
            for _ in 0..500 {
                let mut changes = Vec::new();
                for _ in 0..1 + next() % 4 {
                    let table = if next() % 2 == 0 { "left" } else { "right" };
                    let id = format!("r{}", next() % 30);
                    let mut fields =
                        HashMap::from([("other".into(), Sp00kyValue::Int(next() as i64))]);
                    if next() % 8 != 0 {
                        fields.insert("join".into(), value(next()));
                    }
                    let data = Sp00kyValue::Object(fields);
                    changes.push(match next() % 5 {
                        0 => Change::delete(table, &id),
                        1 => Change::merge(table, &id, data),
                        2 => Change::update(table, &id, data),
                        _ => Change::create(table, &id, data),
                    });
                }
                h.mutate(changes);
            }
        }
    }

    #[test]
    fn arbitrary_weights_and_absent_rows_keep_snapshot_semantics() {
        let mut h = Harness::new();
        h.mutate(vec![
            Change::create("left", "a", json!({"join": 1})),
            Change::create("right", "a", json!({"join": 1})),
        ]);
        for (l, r) in [(2, 4), (-4, -2), (2, -4), (0, 2), (1, 0), (-2, 1), (1, -2)] {
            h.step(
                [
                    ZSet::from([("left:a".into(), l)]),
                    ZSet::from([("right:a".into(), r)]),
                ],
                &[],
            );
        }
        h.step(
            [
                ZSet::from([("left:absent".into(), 1)]),
                ZSet::from([("right:absent".into(), 1)]),
            ],
            &[],
        );
    }

    #[test]
    fn point_checks_match_legacy_compare_only_semantics() {
        let mut h = Harness::new();
        let mut values: Vec<_> = (0..14).map(value).collect();
        values.extend([
            Sp00kyValue::Int(0),
            Sp00kyValue::Float(0.0),
            Sp00kyValue::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        ]);
        for witness in &values {
            h.mutate(vec![Change::create(
                "right",
                "a",
                Sp00kyValue::Object(HashMap::from([("join".into(), witness.clone())])),
            )]);
            for query in &values {
                let query = ValueRef::from_value(query);
                let expected = h.inputs[1].iter().any(|(key, &weight)| {
                    let right =
                        resolve_field(h.store.get_row_by_key(key), &h.condition.right_field);
                    weight > 0
                        && !right.is_missing()
                        && compare_values(query, right) == Ordering::Equal
                });
                for join in &h.joins {
                    assert_eq!(
                        join.has_point_witness(query),
                        expected,
                        "query {query:?}, witness {witness:?}"
                    );
                    assert!(!join.has_point_witness(ValueRef::Missing));
                }
            }
        }
        h.mutate(vec![Change::delete("right", "a")]);
        for join in &h.joins {
            for query in &values {
                assert!(!join.has_point_witness(ValueRef::from_value(query)));
            }
        }
    }

    #[test]
    fn unrelated_and_empty_deltas_do_not_walk_large_join_state() {
        let mut h = Harness::new();
        h.mutate(
            (0..10_000)
                .flat_map(|i| {
                    [
                        Change::create("left", &format!("r{i}"), json!({"join": i})),
                        Change::create("right", &format!("r{i}"), json!({"join": i})),
                    ]
                })
                .collect(),
        );
        h.mutate(vec![]);
        for join in &h.joins {
            assert_eq!(join.refreshed_rows, 0);
            assert_eq!(join.evaluated_left_rows, 0);
        }
        h.mutate(vec![Change::create(
            "right",
            "unrelated",
            json!({"join": 20_000}),
        )]);
        for join in &h.joins {
            assert_eq!(join.refreshed_rows, 1);
            assert_eq!(join.evaluated_left_rows, 0);
        }
        h.mutate(vec![Change::merge("right", "r5000", json!({"other": 1}))]);
        for join in &h.joins {
            assert_eq!(join.refreshed_rows, 1);
            assert_eq!(join.evaluated_left_rows, 0);
        }
        h.mutate(vec![Change::create(
            "right",
            "duplicate",
            json!({"join": 5000}),
        )]);
        for join in &h.joins {
            assert_eq!(join.refreshed_rows, 1);
            assert_eq!(join.evaluated_left_rows, 0);
        }
        h.mutate(vec![Change::delete("right", "duplicate")]);
        for join in &h.joins {
            assert_eq!(join.refreshed_rows, 1);
            assert_eq!(join.evaluated_left_rows, 0);
        }
        h.mutate(vec![Change::delete("right", "r5000")]);
        for join in &h.joins {
            assert_eq!(join.refreshed_rows, 1);
            assert_eq!(join.evaluated_left_rows, 1);
        }
    }
}
