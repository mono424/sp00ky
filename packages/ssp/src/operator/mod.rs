pub mod predicate;
pub mod plan;
pub mod scan;
pub mod filter;
pub mod join;
pub mod semi_join;
pub mod anti_join;
pub mod union;
pub mod map;
pub mod top_k;
pub mod index_scan;
pub mod aggregate;
pub mod distinct;

use crate::algebra::ZSet;
use crate::circuit::store::Store;
use crate::types::Sp00kyValue;
use std::fmt::Debug;

/// A node in the DBSP circuit.
///
/// Each operator implements two evaluation modes derived from the
/// DBSP incrementalization theorem `Q_inc = D . lift(Q) . I`:
///
///   - `snapshot`: corresponds to `lift(Q)` — full evaluation from complete input Z-sets
///   - `step`: corresponds to the differentiated delta rule `D(Q)` — incremental
///     evaluation from input deltas, producing output deltas
///
/// Stateful operators (Join, TopK, Aggregate, Distinct) hold Z⁻¹
/// integration state internally and update it on each `step()` call.
/// Stateless operators (Scan, Filter, Map) have identical `snapshot` and `step`.
pub trait Operator: Debug + Send + Sync {
    /// Full evaluation: input Z-sets → output Z-set.
    ///
    /// Used for initial load and correctness verification.
    /// Does NOT modify internal state.
    fn snapshot(&self, inputs: &[&ZSet], store: &Store, ctx: Option<&Sp00kyValue>) -> ZSet;

    /// Incremental evaluation: input deltas → output delta.
    ///
    /// Stateful operators update their Z⁻¹ integration state here.
    /// Always produces a delta (may be empty). Never returns None.
    fn step(
        &mut self,
        input_deltas: &[&ZSet],
        store: &Store,
        ctx: Option<&Sp00kyValue>,
    ) -> ZSet;

    /// Number of input ports. Scan=0, unary operators=1, Join=2.
    fn arity(&self) -> usize;

    /// Reset all internal state (for re-initialization).
    fn reset(&mut self);

    /// The operator's state can no longer answer from deltas alone and the
    /// view must be primed again from the store. Only a bounded TopK that
    /// dropped rows and then lost enough of its window to need them says yes.
    fn needs_rebuild(&self) -> bool {
        false
    }

    /// Base collections this operator directly reads from (Scan only).
    fn collections(&self) -> Vec<String> {
        vec![]
    }

    /// What a source operator starts from when its view registers, instead
    /// of the whole collection. `None` (the default) is the whole collection;
    /// an index-backed source answers with just the rows its index range
    /// holds, which is what spares a registration the full-table pass.
    fn initial_input(&self, _store: &Store, _ctx: Option<&Sp00kyValue>) -> Option<ZSet> {
        None
    }

    /// The `(table, index name)` this operator reads, so the circuit can drop
    /// an index no registered view plans over any more.
    fn index_use(&self) -> Option<(&str, &str)> {
        None
    }

    /// Membership test for a single key against this operator's CURRENT
    /// snapshot output, without recomputing the full snapshot.
    ///
    /// Used by `Circuit::step_query` to detect membership transitions
    /// caused by `Operation::Update` (weight 0): the Scan emits an
    /// empty delta for Updates, so Filter never re-evaluates the
    /// predicate against the new row content. Walking the DAG with
    /// `evaluate_key` lets us ask "is this key in the current
    /// snapshot?" and synthesize +1/-1 weights when membership
    /// transitioned.
    ///
    /// `input_evals[i]` is the result of `evaluate_key` on the i-th
    /// input node, computed in topological order.
    ///
    /// Default: returns `false`. Conservative — operators that don't
    /// implement this won't surface membership transitions through
    /// Updates. Override on operators whose membership semantics are
    /// well-defined per-row (Scan, Filter, Map, TopK, Union, Distinct).
    fn evaluate_key(
        &self,
        _key: &str,
        _input_evals: &[bool],
        _store: &Store,
        _ctx: Option<&Sp00kyValue>,
    ) -> bool {
        false
    }

    /// Re-place one key whose CONTENT changed, for operators whose output
    /// depends on row content beyond membership (ordering, grouping).
    ///
    /// `evaluate_key` answers "is this key in the output now?", which is the
    /// right question for Filter and Scan but not for TopK: a row can be
    /// admitted upstream and still fall outside the window, and a row inside
    /// the window can move out of it when its sort field changes. Answering
    /// `true` from TopK over-emits a `+1` that nothing evicts, so an UPDATE of
    /// any row outside a `LIMIT n` window used to grow the window by one.
    ///
    /// Returns the output delta of retracting the key's old placement (if
    /// held) and re-inserting it with its current content (if `upstream_now`
    /// admits it), or `None` for operators that have no such state, in which
    /// case the circuit falls back to `evaluate_key`.
    fn reorder_key(
        &mut self,
        _key: &str,
        _upstream_now: bool,
        _store: &Store,
        _ctx: Option<&Sp00kyValue>,
    ) -> Option<ZSet> {
        None
    }

    /// Approximate heap bytes held in this operator's Z⁻¹ state.
    ///
    /// This is the term that scales with *both* table size and query count:
    /// a stateful operator keeps integrated copies of its inputs, so N
    /// registered queries over the same large table each pay for their own.
    /// It is invisible in the row-store numbers and can exceed them outright,
    /// so it gets its own line in [`crate::circuit::Circuit::size_report`].
    ///
    /// Default: 0, correct for the stateless operators (Scan, Filter, Map,
    /// Union). Stateful ones override.
    fn state_bytes(&self) -> usize {
        0
    }
}

pub use aggregate::{Aggregate, AggregateFunc};
pub use distinct::Distinct;
pub use filter::Filter;
pub use index_scan::{IndexBinding, IndexWindow, IndexedScan, KeyScan};
pub use join::Join;
pub use map::Map;
pub use plan::{JoinCondition, OperatorPlan, OrderSpec, Projection, QueryPlan};
pub use predicate::Predicate;
pub use anti_join::AntiJoin;
pub use scan::Scan;
pub use semi_join::SemiJoin;
pub use top_k::TopK;
pub use union::Union;
