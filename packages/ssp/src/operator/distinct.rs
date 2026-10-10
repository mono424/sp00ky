use crate::algebra::ZSet;
use crate::circuit::store::Store;
use crate::types::Sp00kyValue;
use std::collections::HashMap;

/// Distinct operator: ensures output weights are 0 or 1.
///
/// DBSP rule: `distinct = D(threshold(I(input)))`
/// where `threshold` clamps weights to {0, 1}.
///
/// On each step:
///   1. integrated += delta_in       (I: integration)
///   2. Emit only the changed keys whose integrated weight crosses zero.
///
/// Unchanged keys are never walked and no duplicate output cache is needed.
#[derive(Debug)]
pub struct Distinct {
    /// Z⁻¹: accumulated input state.
    integrated: ZSet,
    #[cfg(test)]
    processed_keys: usize,
}

impl Distinct {
    pub fn new() -> Self {
        Self {
            integrated: HashMap::new(),
            #[cfg(test)]
            processed_keys: 0,
        }
    }

    fn threshold(zset: &ZSet) -> ZSet {
        zset.iter()
            .filter(|(_, &w)| w > 0)
            .map(|(k, _)| (k.clone(), 1i64))
            .collect()
    }
}

impl super::Operator for Distinct {
    fn snapshot(&self, inputs: &[&ZSet], _store: &Store, _ctx: Option<&Sp00kyValue>) -> ZSet {
        Self::threshold(inputs[0])
    }

    fn step(&mut self, input_deltas: &[&ZSet], _store: &Store, _ctx: Option<&Sp00kyValue>) -> ZSet {
        let mut output = ZSet::new();
        #[cfg(test)]
        {
            self.processed_keys = 0;
        }
        for (key, &delta) in input_deltas[0] {
            #[cfg(test)]
            {
                self.processed_keys += 1;
            }
            let old = self.integrated.get(key).copied().unwrap_or(0);
            let new = old + delta;
            if (old > 0) != (new > 0) {
                output.insert(key.clone(), if new > 0 { 1 } else { -1 });
            }
            if new == 0 {
                self.integrated.remove(key);
            } else {
                self.integrated.insert(key.clone(), new);
            }
        }
        output
    }

    fn arity(&self) -> usize {
        1
    }

    fn reset(&mut self) {
        self.integrated.clear();
        #[cfg(test)]
        {
            self.processed_keys = 0;
        }
    }

    fn state_bytes(&self) -> usize {
        crate::size::zset_bytes(&self.integrated)
    }

    fn evaluate_key(
        &self,
        _key: &str,
        input_evals: &[bool],
        _store: &Store,
        _ctx: Option<&Sp00kyValue>,
    ) -> bool {
        // Distinct doesn't change membership semantics; if upstream
        // admits the key with positive weight, distinct emits it once.
        input_evals.first().copied().unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::ZSetOps;
    use crate::operator::Operator;

    fn zset(items: &[(&str, i64)]) -> ZSet {
        items.iter().map(|(k, w)| ((*k).into(), *w)).collect()
    }

    #[test]
    fn step_clamps_to_binary_presence() {
        let store = Store::new();
        let mut distinct = Distinct::new();

        // Insert with weight 3 → should appear with weight 1
        let d1 = zset(&[("a", 3)]);
        let result = distinct.step(&[&d1], &store, None);
        assert_eq!(result.get("a"), Some(&1));
    }

    #[test]
    fn step_emits_removal_when_weight_drops_to_zero() {
        let store = Store::new();
        let mut distinct = Distinct::new();

        let d1 = zset(&[("a", 2)]);
        let _ = distinct.step(&[&d1], &store, None);

        // Remove 2 → weight goes to 0 → should emit -1
        let d2 = zset(&[("a", -2)]);
        let result = distinct.step(&[&d2], &store, None);
        assert_eq!(result.get("a"), Some(&-1));
    }

    #[test]
    fn step_no_output_for_multiplicity_change_within_positive() {
        let store = Store::new();
        let mut distinct = Distinct::new();

        let d1 = zset(&[("a", 1)]);
        let _ = distinct.step(&[&d1], &store, None); // a enters

        let d2 = zset(&[("a", 2)]);
        let result = distinct.step(&[&d2], &store, None); // weight 1→3, threshold unchanged
        assert!(result.is_empty());
    }

    #[test]
    fn incremental_threshold_matches_snapshot_through_signed_weight_churn_and_reset() {
        let store = Store::new();
        let mut distinct = Distinct::new();
        let mut integrated = ZSet::new();
        let mut output = ZSet::new();
        let mut rng = 0xDEAD_BEEFu64;
        for _ in 0..500 {
            let mut delta = ZSet::new();
            for _ in 0..4 {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                *delta.entry(format!("r{}", rng % 30).into()).or_insert(0) += (rng % 7) as i64 - 3;
            }
            integrated.add(&delta);
            let expected = distinct.snapshot(&[&integrated], &store, None);
            let actual = distinct.step(&[&delta], &store, None);
            assert_eq!(actual, output.diff(&expected));
            output.add(&actual);
            assert_eq!(output, expected);
        }
        distinct.reset();
        assert_eq!(distinct.step(&[&integrated], &store, None), output);
    }

    #[test]
    fn large_state_only_processes_keys_in_the_delta() {
        let store = Store::new();
        let mut distinct = Distinct::new();
        let initial = (0..10_000).map(|i| (format!("r{i}").into(), 1)).collect();
        distinct.step(&[&initial], &store, None);
        assert!(distinct.step(&[&ZSet::new()], &store, None).is_empty());
        assert_eq!(distinct.processed_keys, 0);
        assert_eq!(
            distinct.step(&[&zset(&[("r5000", -1)])], &store, None),
            zset(&[("r5000", -1)])
        );
        assert_eq!(distinct.processed_keys, 1);
    }
}
