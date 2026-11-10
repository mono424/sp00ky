//! The fluent query builder (TS `QueryBuilder`) and the `.related()` plan it
//! hands the engine.
//!
//! PORT: `packages/spooky_core/lib/src/modules/query_builder.dart`.

/// One `.related()` join, as materialization resolves it from the local cache.
#[derive(Clone, Debug, PartialEq)]
pub struct RelationPlan {
    /// Result key the joined value is attached under (the `.related()` field).
    pub alias: String,
    /// Table the joined rows come from.
    pub table: String,
    /// `one` attaches a single row (or null); `many` attaches a list.
    pub cardinality: crate::types::Cardinality,
    /// Correlation field: for `many`, the child field pointing at the parent
    /// id; for `one`, the parent field holding the child id.
    pub foreign_key_field: String,
    pub select: Vec<String>,
    pub where_: crate::value::Object,
    /// `(field, direction)` terms.
    pub order_by: Vec<(String, String)>,
    pub limit: Option<usize>,
    pub relations: Vec<RelationPlan>,
}

impl RelationPlan {
    pub fn is_one(&self) -> bool {
        self.cardinality == crate::types::Cardinality::One
    }
}
