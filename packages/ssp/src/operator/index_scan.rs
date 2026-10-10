//! Source operators that read a mirrored secondary index instead of the whole
//! collection (see [`crate::circuit::index`]).
//!
//! - [`IndexedScan`] stands in for a `Scan` under a `Filter` whose equalities
//!   cover an index's leading fields. It starts a registration from the rows
//!   in that index range rather than from every row of the table, and passes
//!   deltas through exactly like `Scan`; the `Filter` above it still checks
//!   every predicate, so it narrows work and never changes results.
//! - [`IndexWindow`] stands in for `Limit` over `Filter` over `Scan` when the
//!   filter is equalities on an index's leading fields and the `ORDER BY` is
//!   the rest of that index. Its window is read by position in the index: a
//!   registration reads `limit` rows, and a write re-reads the window instead
//!   of re-sorting the filtered rows.
//! - [`KeyScan`] stands in for the `Scan` under a filter holding `id` equal to
//!   one record: the record key is the index SurrealDB always has.
//!
//! The circuit decides where these apply (`Circuit::build_graph`); all keep
//! the semantics of the operators they replace, `TopK` order included: each
//! `ORDER BY` field in its own direction, ties by row key ascending.

use serde_json::Value;

use crate::algebra::{RowKey, ZSet};
use crate::circuit::index::IndexDef;
use crate::circuit::store::Store;
use crate::eval::value_ref::ValueRef;
use crate::operator::filter::{check_predicate_recursive, resolve_predicate_value};
use crate::operator::predicate::Predicate;
use crate::operator::top_k::{Scalar, SortableValue};
use crate::types::Sp00kyValue;

/// Which index a source reads and the equality operands of its leading
/// fields, in index field order. Operands are the plan's own values: a
/// literal, or `{"$param": …}` resolved per view like `Filter` does.
#[derive(Debug, Clone)]
pub struct IndexBinding {
    pub table: String,
    pub def: IndexDef,
    pub prefix: Vec<Value>,
    /// Predicates that read no row (`$access = …`, `true`): the whole range
    /// or nothing, decided per view. Only a window carries them, since it
    /// replaces the `Filter`s that would otherwise check them.
    pub gates: Vec<Predicate>,
}

impl IndexBinding {
    /// The prefix as index values, or `None` when an operand does not
    /// resolve to an orderable scalar (the view then matches nothing, as the
    /// `Filter` it replaces would fail closed).
    fn prefix_values(&self, store: &Store, ctx: Option<&Sp00kyValue>) -> Option<Vec<Scalar>> {
        if !self.gates.iter().all(|gate| check_predicate_recursive(gate, "", store, ctx)) {
            return None;
        }
        self.prefix
            .iter()
            .map(|operand| {
                let value = resolve_predicate_value(operand, ctx)?;
                let value_ref = ValueRef::from_value(&value);
                SortableValue::is_orderable(value_ref).then(|| SortableValue::from_value(value_ref, false).scalar)
            })
            .collect()
    }

    fn owns(&self, key: &str) -> bool {
        key.len() > self.table.len() && key.starts_with(self.table.as_str()) && key.as_bytes()[self.table.len()] == b':'
    }

    /// Every row in the prefix range.
    fn range_keys(&self, store: &Store, ctx: Option<&Sp00kyValue>) -> ZSet {
        let (Some(prefix), Some(coll)) = (self.prefix_values(store, ctx), store.get_collection(&self.table)) else {
            return ZSet::new();
        };
        coll.with_index(&self.def, |index| {
            let (lo, hi) = index.prefix_range(&prefix);
            (lo..hi).filter_map(|p| index.row_at(p).map(|row| (row.clone(), 1))).collect()
        })
    }
}

/// A `Scan` that starts from an index range; see the module docs.
#[derive(Debug)]
pub struct IndexedScan {
    pub binding: IndexBinding,
}

impl IndexedScan {
    pub fn new(binding: IndexBinding) -> Self {
        Self { binding }
    }
}

impl super::Operator for IndexedScan {
    fn snapshot(&self, _inputs: &[&ZSet], store: &Store, ctx: Option<&Sp00kyValue>) -> ZSet {
        self.binding.range_keys(store, ctx)
    }

    fn step(&mut self, input_deltas: &[&ZSet], _store: &Store, _ctx: Option<&Sp00kyValue>) -> ZSet {
        input_deltas.first().map(|d| (*d).clone()).unwrap_or_default()
    }

    fn arity(&self) -> usize {
        0
    }

    fn reset(&mut self) {}

    fn collections(&self) -> Vec<String> {
        vec![self.binding.table.clone()]
    }

    fn initial_input(&self, store: &Store, ctx: Option<&Sp00kyValue>) -> Option<ZSet> {
        Some(self.binding.range_keys(store, ctx))
    }

    fn index_use(&self) -> Option<(&str, &str)> {
        Some((&self.binding.table, &self.binding.def.name))
    }

    fn evaluate_key(&self, key: &str, _input_evals: &[bool], store: &Store, _ctx: Option<&Sp00kyValue>) -> bool {
        store.get_collection(&self.binding.table).is_some_and(|c| c.has_key(key))
    }
}

/// A `LIMIT … START … ORDER BY` window read off an index; see the module
/// docs.
#[derive(Debug)]
pub struct IndexWindow {
    pub binding: IndexBinding,
    pub limit: usize,
    pub offset: usize,
    /// Per `ORDER BY` field (the index fields after the equality prefix),
    /// whether it sorts descending.
    pub descending: Vec<bool>,
    /// The keys currently in the window, in order.
    window: Vec<RowKey>,
    primed: bool,
}

impl IndexWindow {
    pub fn new(binding: IndexBinding, limit: usize, offset: usize, descending: Vec<bool>) -> Self {
        Self { binding, limit, offset, descending, window: Vec::new(), primed: false }
    }

    /// The window as the index holds it now.
    fn read(&self, store: &Store, ctx: Option<&Sp00kyValue>) -> Vec<RowKey> {
        let (Some(prefix), Some(coll)) = (self.binding.prefix_values(store, ctx), store.get_collection(&self.binding.table)) else {
            return Vec::new();
        };
        let (limit, offset) = (self.limit, self.offset);
        coll.with_index(&self.binding.def, |index| {
            let (lo, hi) = index.prefix_range(&prefix);
            let len = hi - lo;
            if offset >= len {
                return Vec::new();
            }
            let take = limit.min(len - offset);
            if !self.descending.contains(&true) {
                // All ascending: the order is the index's own.
                return (lo + offset..lo + offset + take).filter_map(|p| index.row_at(p).cloned()).collect();
            }
            (offset..offset + take)
                .filter_map(|k| self.nth(index, prefix.len(), lo, hi, k))
                .filter_map(|p| index.row_at(p).cloned())
                .collect()
        })
    }

    /// The index position of the `k`-th row of `[lo, hi)` in this window's
    /// order. Field by field: the rows sharing their values up to a field sit
    /// together in the index, and those groups come in index order for an
    /// ascending field and reversed for a descending one. So the `k`-th row
    /// falls in the group holding index position `lo + k` (or `hi - 1 - k`),
    /// and the search narrows to that group with `k` less the rows of the
    /// groups before it. Ties left after the last field are by row key
    /// ascending, which is index order: TopK's order exactly, at two rank
    /// lookups per field.
    fn nth(&self, index: &crate::circuit::index::OrderedIndex, prefix_len: usize, lo: usize, hi: usize, k: usize) -> Option<usize> {
        let (mut a, mut b, mut k) = (lo, hi, k);
        for (field, &desc) in self.descending.iter().enumerate() {
            if k >= b - a {
                return None;
            }
            let p = if desc { b - 1 - k } else { a + k };
            let (g_lo, g_hi) = index.group_of(p, prefix_len + field + 1)?;
            k -= if desc { b - g_hi } else { g_lo - a };
            (a, b) = (g_lo, g_hi);
        }
        (k < b - a).then_some(a + k)
    }

    /// Replace the window with `next` and return the membership change.
    fn swap_in(&mut self, next: Vec<RowKey>) -> ZSet {
        let mut delta = ZSet::new();
        for key in &next {
            *delta.entry(key.clone()).or_insert(0) += 1;
        }
        for key in &self.window {
            *delta.entry(key.clone()).or_insert(0) -= 1;
        }
        delta.retain(|_, w| *w != 0);
        self.window = next;
        delta
    }
}

impl super::Operator for IndexWindow {
    fn snapshot(&self, _inputs: &[&ZSet], store: &Store, ctx: Option<&Sp00kyValue>) -> ZSet {
        self.read(store, ctx).into_iter().map(|key| (key, 1)).collect()
    }

    fn step(&mut self, input_deltas: &[&ZSet], store: &Store, ctx: Option<&Sp00kyValue>) -> ZSet {
        // The store applied this step's writes, and kept the index in step,
        // before any operator runs: re-reading the window is the delta.
        let changed = input_deltas.first().is_some_and(|d| !d.is_empty());
        if self.primed && !changed {
            return ZSet::new();
        }
        self.primed = true;
        let next = self.read(store, ctx);
        self.swap_in(next)
    }

    fn arity(&self) -> usize {
        0
    }

    fn reset(&mut self) {
        self.window.clear();
        self.primed = false;
    }

    fn collections(&self) -> Vec<String> {
        vec![self.binding.table.clone()]
    }

    fn initial_input(&self, _store: &Store, _ctx: Option<&Sp00kyValue>) -> Option<ZSet> {
        // The window reads the index itself; it needs no rows handed in.
        Some(ZSet::new())
    }

    fn index_use(&self) -> Option<(&str, &str)> {
        Some((&self.binding.table, &self.binding.def.name))
    }

    fn evaluate_key(&self, key: &str, _input_evals: &[bool], _store: &Store, _ctx: Option<&Sp00kyValue>) -> bool {
        self.window.iter().any(|k| &**k == key)
    }

    fn reorder_key(&mut self, key: &str, _upstream_now: bool, store: &Store, ctx: Option<&Sp00kyValue>) -> Option<ZSet> {
        if !self.binding.owns(key) {
            return None;
        }
        let next = self.read(store, ctx);
        Some(self.swap_in(next))
    }

    fn state_bytes(&self) -> usize {
        self.window.len() * std::mem::size_of::<RowKey>()
    }
}

/// The `Scan` under a filter holding `id` equal to one record: starts from
/// that row alone and passes deltas through like `Scan`; the `Filter` above
/// still checks every predicate.
#[derive(Debug)]
pub struct KeyScan {
    pub table: String,
    /// The `id` operand: a literal or `{"$param": …}`.
    pub key: Value,
}

impl KeyScan {
    /// The row the operand names, when this table holds it. The `Filter` on
    /// `id` compares the whole key, so only `<table>:<id>` spelled out can
    /// match.
    fn start(&self, store: &Store, ctx: Option<&Sp00kyValue>) -> ZSet {
        let Some(Sp00kyValue::Str(key)) = resolve_predicate_value(&self.key, ctx) else {
            return ZSet::new();
        };
        let held = key.strip_prefix(self.table.as_str()).is_some_and(|rest| rest.starts_with(':'))
            && store.get_collection(&self.table).is_some_and(|c| c.has_key(&key));
        if held {
            std::iter::once((RowKey::from(key.as_str()), 1)).collect()
        } else {
            ZSet::new()
        }
    }
}

impl super::Operator for KeyScan {
    fn snapshot(&self, _inputs: &[&ZSet], store: &Store, ctx: Option<&Sp00kyValue>) -> ZSet {
        self.start(store, ctx)
    }

    fn step(&mut self, input_deltas: &[&ZSet], _store: &Store, _ctx: Option<&Sp00kyValue>) -> ZSet {
        input_deltas.first().map(|d| (*d).clone()).unwrap_or_default()
    }

    fn arity(&self) -> usize {
        0
    }

    fn reset(&mut self) {}

    fn collections(&self) -> Vec<String> {
        vec![self.table.clone()]
    }

    fn initial_input(&self, store: &Store, ctx: Option<&Sp00kyValue>) -> Option<ZSet> {
        Some(self.start(store, ctx))
    }

    fn evaluate_key(&self, key: &str, _input_evals: &[bool], store: &Store, _ctx: Option<&Sp00kyValue>) -> bool {
        store.get_collection(&self.table).is_some_and(|c| c.has_key(key))
    }
}
