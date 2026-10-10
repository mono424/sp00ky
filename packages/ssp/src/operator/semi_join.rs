use super::witness::WitnessState;
use crate::algebra::ZSet;
use crate::circuit::store::Store;
use crate::eval::value_ops::{compare_values, hash_value, resolve_field};
use crate::eval::value_ref::ValueRef;
use crate::operator::plan::JoinCondition;
use crate::types::{Path, Sp00kyValue};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Semi-join operator: emits left rows whose join key has at least one match
/// on the right side. Output is keyed only by left-side keys with weights in
/// `{0, 1}` (semi-join semantics — the right side is a witness check, not a
/// cartesian product source).
///
/// Used by the policy rewriter when lowering `IN (SELECT … WHERE …)` permission
/// subqueries: the outer scan becomes the left, the inner subquery's plan
/// becomes the right, and the correlation predicate becomes the join condition.
///
/// Integrated input rows are indexed by join value. A step refreshes only
/// changed rows and rechecks left rows in the affected witness buckets; an
/// empty or unrelated delta never recomputes the whole joined snapshot.
#[derive(Debug)]
pub struct SemiJoin {
    pub condition: JoinCondition,
    state: WitnessState,
}

impl SemiJoin {
    pub fn new(condition: JoinCondition) -> Self {
        Self {
            condition,
            state: WitnessState::default(),
        }
    }

    /// Compute the semi-join snapshot: emit each left key with weight 1 iff at
    /// least one right row has the join key value present (the right side is a
    /// witness — multiplicities don't escape into the output).
    fn semi_join(left: &ZSet, right: &ZSet, condition: &JoinCondition, store: &Store) -> ZSet {
        if left.is_empty() {
            return HashMap::new();
        }

        // Build a set of distinct right-side join-field values that are
        // currently "live" (positive weight). Multiplicities on the right side
        // don't affect semi-join output, so this is a presence check.
        // Borrowed join-field values rather than owned clones: this used to
        // deep-copy the field of every live right-side row.
        let mut right_present: HashMap<u64, Vec<ValueRef<'_>>> = HashMap::new();
        for (r_key, &r_weight) in right {
            if r_weight <= 0 {
                continue;
            }
            let r_field = resolve_field(store.get_row_by_key(r_key), &condition.right_field);
            if !r_field.is_missing() {
                right_present
                    .entry(hash_value(r_field))
                    .or_default()
                    .push(r_field);
            }
        }

        if right_present.is_empty() {
            return HashMap::new();
        }

        let mut out = HashMap::new();
        for (l_key, &l_weight) in left {
            if l_weight <= 0 {
                continue;
            }
            let l_field = resolve_field(store.get_row_by_key(l_key), &condition.left_field);
            if l_field.is_missing() {
                continue;
            }
            if let Some(matches) = right_present.get(&hash_value(l_field)) {
                if matches
                    .iter()
                    .any(|m| compare_values(l_field, *m) == Ordering::Equal)
                {
                    out.insert(l_key.clone(), 1i64);
                }
            }
        }
        out
    }
}

impl super::Operator for SemiJoin {
    fn snapshot(&self, inputs: &[&ZSet], store: &Store, _ctx: Option<&Sp00kyValue>) -> ZSet {
        Self::semi_join(inputs[0], inputs[1], &self.condition, store)
    }

    fn step(&mut self, input_deltas: &[&ZSet], store: &Store, _ctx: Option<&Sp00kyValue>) -> ZSet {
        self.state.step(input_deltas, store, &self.condition, false)
    }

    fn note_content_updates(&mut self, keys: &[crate::algebra::RowKey]) {
        self.state.note_content_updates(keys);
    }

    fn arity(&self) -> usize {
        2
    }

    fn reset(&mut self) {
        self.state.reset();
    }

    fn state_bytes(&self) -> usize {
        self.state.state_bytes()
    }

    fn evaluate_key(
        &self,
        key: &str,
        input_evals: &[bool],
        store: &Store,
        _ctx: Option<&Sp00kyValue>,
    ) -> bool {
        if !input_evals.first().copied().unwrap_or(false) {
            return false;
        }
        // `id = id` is the permission intersection wrapper
        // (`SemiJoin(view, perm)`): the right side shares the left key
        // space, so the freshly recomputed input eval is authoritative.
        // Integrated membership can be stale for weight-0 content updates.
        if self.condition.left_field == Path::new("id")
            && self.condition.right_field == Path::new("id")
        {
            return input_evals.get(1).copied().unwrap_or(false);
        }
        // Lowered IN-subquery: the right side is a different key space,
        // so its input eval is meaningless for `key`. Witness-check this
        // row's join field against the integrated right-side state
        // (up to date: step() ran for every node before the membership
        // re-evaluation pass calls evaluate_key).
        let l_field = resolve_field(store.get_row_by_key(key), &self.condition.left_field);
        if l_field.is_missing() {
            return false;
        }
        self.state.has_point_witness(l_field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::ZSetOps;
    use crate::circuit::store::Change;
    use crate::operator::Operator;
    use crate::types::Path;
    use serde_json::json;

    fn zset(items: &[(&str, i64)]) -> ZSet {
        items.iter().map(|(k, w)| ((*k).into(), *w)).collect()
    }

    fn setup_store() -> Store {
        // Mirrors permission lowering for `thread.id IN (SELECT VALUE out FROM
        // collaborates_on)`: threads are the left, collaborates_on edges are
        // the right, condition is `thread.id = collaborates_on.out`.
        let mut store = Store::new();
        store.ensure_collection("threads");
        store.ensure_collection("collab");
        store.apply_change(&Change::create(
            "threads",
            "thread:1",
            json!({"id": "thread:1", "title": "t1"}),
        ));
        store.apply_change(&Change::create(
            "threads",
            "thread:2",
            json!({"id": "thread:2", "title": "t2"}),
        ));
        store.apply_change(&Change::create(
            "collab",
            "collab:1",
            json!({"id": "collab:1", "in": "user:a", "out": "thread:1"}),
        ));
        store
    }

    fn cond() -> JoinCondition {
        JoinCondition {
            left_field: Path::new("id"),
            right_field: Path::new("out"),
        }
    }

    #[test]
    fn snapshot_emits_left_keys_with_witnesses() {
        let store = setup_store();
        let sj = SemiJoin::new(cond());

        let left = zset(&[("threads:1", 1), ("threads:2", 1)]);
        let right = zset(&[("collab:1", 1)]);
        let result = sj.snapshot(&[&left, &right], &store, None);

        assert_eq!(result.get("threads:1"), Some(&1));
        assert!(!result.contains_key("threads:2"));
    }

    #[test]
    fn snapshot_clamps_to_one_even_when_multiple_right_matches() {
        // If the right side has 5 rows pointing at the same left key, the
        // semi-join still emits weight 1 (presence, not count).
        let mut store = setup_store();
        for i in 2..=5 {
            store.apply_change(&Change::create(
                "collab",
                &format!("collab:{}", i),
                json!({"id": format!("collab:{}", i), "in": format!("user:{}", i), "out": "thread:1"}),
            ));
        }
        let sj = SemiJoin::new(cond());

        let left = zset(&[("threads:1", 1)]);
        let right = zset(&[
            ("collab:1", 1),
            ("collab:2", 1),
            ("collab:3", 1),
            ("collab:4", 1),
            ("collab:5", 1),
        ]);
        let result = sj.snapshot(&[&left, &right], &store, None);

        assert_eq!(result.get("threads:1"), Some(&1));
    }

    #[test]
    fn step_emits_thread_when_collaboration_added() {
        let store = setup_store();
        let mut sj = SemiJoin::new(cond());

        // First step: threads in, no collaborations yet.
        let dl_initial = zset(&[("threads:1", 1), ("threads:2", 1)]);
        let dr_initial: ZSet = HashMap::new();
        let r0 = sj.step(&[&dl_initial, &dr_initial], &store, None);
        assert!(r0.is_empty());

        // Second step: collaboration linking threads:1 arrives.
        let dl_empty: ZSet = HashMap::new();
        let dr1 = zset(&[("collab:1", 1)]);
        let r1 = sj.step(&[&dl_empty, &dr1], &store, None);

        // threads:1 should now appear with weight +1.
        assert_eq!(r1.get("threads:1"), Some(&1));
        assert!(!r1.contains_key("threads:2"));
    }

    #[test]
    fn step_retracts_thread_when_collaboration_removed() {
        let store = setup_store();
        let mut sj = SemiJoin::new(cond());

        // Prime with a thread + a collaboration → thread visible.
        let dl1 = zset(&[("threads:1", 1)]);
        let dr1 = zset(&[("collab:1", 1)]);
        let _ = sj.step(&[&dl1, &dr1], &store, None);

        // Now retract the collaboration.
        let dl_empty: ZSet = HashMap::new();
        let dr2 = zset(&[("collab:1", -1)]);
        let r2 = sj.step(&[&dl_empty, &dr2], &store, None);
        assert_eq!(r2.get("threads:1"), Some(&-1));
    }

    #[test]
    fn step_does_not_double_emit_on_extra_collaboration() {
        let mut store = setup_store();
        store.apply_change(&Change::create(
            "collab",
            "collab:2",
            json!({"id": "collab:2", "in": "user:b", "out": "thread:1"}),
        ));
        let mut sj = SemiJoin::new(cond());

        let dl = zset(&[("threads:1", 1)]);
        let dr1 = zset(&[("collab:1", 1)]);
        let _ = sj.step(&[&dl, &dr1], &store, None);

        // Adding a second collaborator on the same thread must not re-emit
        // threads:1 (it's already visible).
        let dl_empty: ZSet = HashMap::new();
        let dr2 = zset(&[("collab:2", 1)]);
        let r2 = sj.step(&[&dl_empty, &dr2], &store, None);
        assert!(r2.is_empty());
    }

    #[test]
    fn evaluate_key_requires_left_and_witness() {
        let store = setup_store();
        let mut sj = SemiJoin::new(cond());

        // Prime state: threads:1 has a collab witness, threads:2 doesn't.
        let dl = zset(&[("threads:1", 1), ("threads:2", 1)]);
        let dr = zset(&[("collab:1", 1)]);
        let _ = sj.step(&[&dl, &dr], &store, None);

        // Left admitted + witness present → true. input_evals[1] is the
        // right branch's (meaningless, cross-key-space) eval and must be
        // ignored for a non-id=id condition.
        assert!(sj.evaluate_key("threads:1", &[true, false], &store, None));
        // Left admitted, no witness → false.
        assert!(!sj.evaluate_key("threads:2", &[true, false], &store, None));
        // Left not admitted → false even with a witness.
        assert!(!sj.evaluate_key("threads:1", &[false, false], &store, None));
    }

    #[test]
    fn evaluate_key_ignores_retracted_witness() {
        let store = setup_store();
        let mut sj = SemiJoin::new(cond());

        let dl = zset(&[("threads:1", 1)]);
        let dr = zset(&[("collab:1", 1)]);
        let _ = sj.step(&[&dl, &dr], &store, None);

        // Retract the witness; the integrated right state drops to 0.
        let dl_empty: ZSet = HashMap::new();
        let dr_retract = zset(&[("collab:1", -1)]);
        let _ = sj.step(&[&dl_empty, &dr_retract], &store, None);

        assert!(!sj.evaluate_key("threads:1", &[true, false], &store, None));
    }

    #[test]
    fn evaluate_key_id_id_wrapper_delegates_to_fresh_input_eval() {
        // The permission intersection wrapper joins on id = id: both sides
        // share the key space, so evaluate_key must use the freshly
        // recomputed right input eval instead of the (stale for weight-0
        // updates) integrated right state.
        let store = setup_store();
        let sj = SemiJoin::new(JoinCondition {
            left_field: Path::new("id"),
            right_field: Path::new("id"),
        });

        assert!(sj.evaluate_key("threads:1", &[true, true], &store, None));
        assert!(!sj.evaluate_key("threads:1", &[true, false], &store, None));
        assert!(!sj.evaluate_key("threads:1", &[false, true], &store, None));
    }

    #[test]
    fn step_matches_snapshot_diff() {
        // DBSP correctness: step(dA, dB) == snapshot(A+dA, B+dB) - snapshot(A, B)
        let mut store = setup_store();
        let condition = cond();

        let state_a = zset(&[("threads:1", 1), ("threads:2", 1)]);
        let state_b = zset(&[("collab:1", 1)]);
        let snap_before =
            SemiJoin::new(condition.clone()).snapshot(&[&state_a, &state_b], &store, None);

        // Add a thread and a collaboration linking the new thread.
        store.apply_change(&Change::create(
            "threads",
            "thread:3",
            json!({"id": "thread:3", "title": "t3"}),
        ));
        store.apply_change(&Change::create(
            "collab",
            "collab:2",
            json!({"id": "collab:2", "in": "user:a", "out": "thread:3"}),
        ));

        let new_a = zset(&[("threads:1", 1), ("threads:2", 1), ("threads:3", 1)]);
        let new_b = zset(&[("collab:1", 1), ("collab:2", 1)]);
        let snap_after = SemiJoin::new(condition.clone()).snapshot(&[&new_a, &new_b], &store, None);

        let expected_delta = snap_before.diff(&snap_after);

        // Now compute incrementally — replay the same change.
        let mut sj = SemiJoin::new(condition);
        sj.step(&[&state_a, &state_b], &store, None);

        let dl = zset(&[("threads:3", 1)]);
        let dr = zset(&[("collab:2", 1)]);
        let actual_delta = sj.step(&[&dl, &dr], &store, None);

        assert_eq!(actual_delta, expected_delta);
    }
}
