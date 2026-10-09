use crate::algebra::ZSet;
use crate::circuit::store::Store;
use crate::types::Sp00kyValue;

/// Scan operator: reads a base collection's Z-set.
///
/// This is a leaf node (arity 0) in the circuit graph.
/// Stateless — the collection itself is the state, managed by Store.
///
/// - `snapshot`: returns the full Z-set of the collection
/// - `step`: passes through the table delta injected by the scheduler
#[derive(Debug)]
pub struct Scan {
    pub table: String,
}

impl Scan {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
        }
    }
}

impl super::Operator for Scan {
    fn snapshot(&self, _inputs: &[&ZSet], store: &Store, _ctx: Option<&Sp00kyValue>) -> ZSet {
        store
            .get_collection(&self.table)
            .map(|c| c.membership().clone())
            .unwrap_or_default()
    }

    fn step(
        &mut self,
        input_deltas: &[&ZSet],
        _store: &Store,
        _ctx: Option<&Sp00kyValue>,
    ) -> ZSet {
        // The scheduler injects the table delta as input_deltas[0]
        input_deltas
            .first()
            .map(|d| (*d).clone())
            .unwrap_or_default()
    }

    fn arity(&self) -> usize {
        0
    }

    fn reset(&mut self) {}

    fn collections(&self) -> Vec<String> {
        vec![self.table.clone()]
    }

    fn evaluate_key(
        &self,
        key: &str,
        _input_evals: &[bool],
        store: &Store,
        _ctx: Option<&Sp00kyValue>,
    ) -> bool {
        store
            .get_collection(&self.table)
            .map(|c| c.has_key(key))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Operator;
    use crate::circuit::store::Change;
    use serde_json::json;

    fn zset(items: &[(&str, i64)]) -> ZSet {
        items.iter().map(|(k, w)| ((*k).into(), *w)).collect()
    }

    #[test]
    fn snapshot_returns_collection_zset() {
        let mut store = Store::new();
        store.ensure_collection("users");
        store.apply_change(&Change::create("users", "user:1", json!({"name": "alice"})));

        let scan = Scan::new("users");
        let result = scan.snapshot(&[], &store, None);

        assert_eq!(result.get("users:1"), Some(&1));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn snapshot_returns_empty_for_missing_collection() {
        let store = Store::new();
        let scan = Scan::new("users");
        let result = scan.snapshot(&[], &store, None);
        assert!(result.is_empty());
    }

    #[test]
    fn step_passes_through_delta() {
        let store = Store::new();
        let mut scan = Scan::new("users");
        let delta = zset(&[("users:1", 1), ("users:2", -1)]);

        let result = scan.step(&[&delta], &store, None);
        assert_eq!(result, delta);
    }

    #[test]
    fn arity_is_zero() {
        assert_eq!(Scan::new("t").arity(), 0);
    }
}
