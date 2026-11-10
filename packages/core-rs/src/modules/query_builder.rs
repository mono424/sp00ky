//! The fluent query builder (TS `QueryBuilder`) and the `.related()` plan it
//! hands the engine.
//!
//! PORT: `packages/spooky_core/lib/src/modules/query_builder.dart`. Produces the
//! same SurrealQL as the TS `buildQueryFromOptions` for select / where /
//! orderBy / limit / offset / one, including `.related()` subquery projections,
//! and registers through a [`QueryHost`].

use std::rc::Rc;
use std::sync::{Arc, LazyLock};

use regex::Regex;

use crate::error::{Error, Result};
use crate::modules::query_host::QueryHost;
use crate::modules::relationships::find_relationship;
use crate::runtime::{RecordsCallback, Subscription};
use crate::types::{Cardinality, QueryHash, Schema, DEFAULT_TTL};
use crate::utils::{base_field_of_param, parse_query_params};
use crate::value::{Object, RecordId, Row, Value};

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

/// A comparison operator condition, mirroring the JS `{ _op, _val, _swap }`
/// shape: `QueryOp::new(">", 18)` on `age` emits `age > $age`.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryOp {
    pub op: String,
    /// The operand. A string starting with `$` references an existing param
    /// verbatim instead of binding a new one.
    pub value: Value,
    /// Emit `$field <op> field` instead of `field <op> $field`.
    pub swap: bool,
}

impl QueryOp {
    pub fn new(op: impl Into<String>, value: impl Into<Value>) -> Self {
        QueryOp {
            op: op.into(),
            value: value.into(),
            swap: false,
        }
    }

    /// The operand on the left: `$field <op> field` (e.g. `CONTAINS`).
    pub fn swapped(op: impl Into<String>, value: impl Into<Value>) -> Self {
        QueryOp {
            swap: true,
            ..QueryOp::new(op, value)
        }
    }
}

/// The right-hand side of one `where` condition.
#[derive(Clone, Debug, PartialEq)]
pub enum WhereValue {
    /// `field = $field`.
    Eq(Value),
    /// `field <op> $field` (or swapped).
    Op(QueryOp),
}

impl From<QueryOp> for WhereValue {
    fn from(op: QueryOp) -> Self {
        WhereValue::Op(op)
    }
}

impl From<Value> for WhereValue {
    fn from(v: Value) -> Self {
        WhereValue::Eq(v)
    }
}

/// One AND-joined conjunct of the query's `WHERE`.
#[derive(Clone, Debug, PartialEq)]
pub enum WhereClause {
    /// One condition, bound as `$<field>`.
    Cond { field: String, value: WhereValue },
    /// A parenthesised `(c1 OR c2 ...)` group (TS `_or`). Each branch binds a
    /// position-indexed `$<field>__or<n>` so it never collides with a
    /// top-level condition on the same field.
    Or(Vec<(String, WhereValue)>),
}

/// Fluent SurrealQL query builder. Methods consume and return the builder so
/// they chain; a misuse (an `-- @ephemeral` field in `where`, an operator in a
/// `.related()` sub-where) is held and reported by [`QueryBuilder::build`].
#[derive(Clone)]
pub struct QueryBuilder {
    table: String,
    host: Option<Rc<dyn QueryHost>>,
    schema: Arc<Schema>,
    select: Vec<String>,
    where_: Vec<WhereClause>,
    order_by: Vec<(String, String)>,
    relations: Vec<RelationPlan>,
    limit: Option<usize>,
    offset: Option<usize>,
    is_one: bool,
    ignore_unused_args: bool,
    error: Option<Error>,
}

impl std::fmt::Debug for QueryBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryBuilder")
            .field("table", &self.table)
            .field("select", &self.select)
            .field("where", &self.where_)
            .field("order_by", &self.order_by)
            .field("relations", &self.relations)
            .field("limit", &self.limit)
            .field("offset", &self.offset)
            .field("is_one", &self.is_one)
            .finish_non_exhaustive()
    }
}

/// A `table:id` string in a sub-where is a record id and must NOT be quoted, or
/// the comparison never matches.
static RECORD_LITERAL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*:\S+$").unwrap());

impl QueryBuilder {
    /// A build-only builder with no schema: `.related()` warn-skips and params
    /// are not re-typed. Attach a host and schema with [`QueryBuilder::bound`].
    pub fn new(table: impl Into<String>) -> Self {
        QueryBuilder {
            table: table.into(),
            host: None,
            schema: Arc::new(Schema::default()),
            select: vec!["*".to_string()],
            where_: Vec::new(),
            order_by: Vec::new(),
            relations: Vec::new(),
            limit: None,
            offset: None,
            is_one: false,
            ignore_unused_args: false,
            error: None,
        }
    }

    /// A builder that registers through `host`, with `schema` for
    /// relationships, `-- @ephemeral` checks and param typing.
    pub fn bound(table: impl Into<String>, host: Rc<dyn QueryHost>, schema: Arc<Schema>) -> Self {
        QueryBuilder::new(table).with_host(host).with_schema(schema)
    }

    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = schema;
        self
    }

    pub fn with_host(mut self, host: Rc<dyn QueryHost>) -> Self {
        self.host = Some(host);
        self
    }

    // ---- accessors ---------------------------------------------------------------

    /// The table the query reads.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The `where` conjuncts as given, AND-joined, in insertion order.
    pub fn conditions(&self) -> &[WhereClause] {
        &self.where_
    }

    pub fn select_fields(&self) -> &[String] {
        &self.select
    }

    pub fn order_fields(&self) -> &[(String, String)] {
        &self.order_by
    }

    pub fn limit_value(&self) -> Option<usize> {
        self.limit
    }

    pub fn offset_value(&self) -> Option<usize> {
        self.offset
    }

    pub fn is_one(&self) -> bool {
        self.is_one
    }

    /// Whether [`QueryBuilder::ignore_unused_args`] was called.
    pub fn ignores_unused_args(&self) -> bool {
        self.ignore_unused_args
    }

    /// The accumulated `.related()` plan, for the client to resolve joined rows
    /// from the local cache.
    pub fn relations(&self) -> &[RelationPlan] {
        &self.relations
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    // ---- clauses -------------------------------------------------------------------

    /// `SELECT <fields>`; an empty list means `*`.
    pub fn select<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let fields: Vec<String> = fields.into_iter().map(Into::into).collect();
        self.select = if fields.is_empty() {
            vec!["*".to_string()]
        } else {
            fields
        };
        self
    }

    /// Add one condition (AND-joined). A repeated field replaces the earlier
    /// condition in place, as a Dart map entry would.
    pub fn where_cond(mut self, field: impl Into<String>, value: impl Into<WhereValue>) -> Self {
        let field = field.into();
        self.assert_not_ephemeral(&field, "where");
        let value = value.into();
        let existing = self
            .where_
            .iter_mut()
            .find(|c| matches!(c, WhereClause::Cond { field: f, .. } if *f == field));
        match existing {
            Some(WhereClause::Cond { value: slot, .. }) => *slot = value,
            _ => self.where_.push(WhereClause::Cond { field, value }),
        }
        self
    }

    /// `field = $field`.
    pub fn where_eq(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.where_cond(field, WhereValue::Eq(value.into()))
    }

    /// `field <op> $field` (or `$field <op> field` when swapped).
    pub fn where_op(self, field: impl Into<String>, op: QueryOp) -> Self {
        self.where_cond(field, WhereValue::Op(op))
    }

    /// Equality on every entry of `conditions` (a `json!({...})` literal works
    /// through [`crate::value::object_from_json`]).
    pub fn where_map(mut self, conditions: Object) -> Self {
        for (field, value) in conditions {
            self = self.where_eq(field, value);
        }
        self
    }

    /// One parenthesised `(c1 OR c2 ...)` conjunct (TS `_or`).
    pub fn where_or<I, S>(mut self, branches: I) -> Self
    where
        I: IntoIterator<Item = (S, WhereValue)>,
        S: Into<String>,
    {
        let branches: Vec<(String, WhereValue)> =
            branches.into_iter().map(|(f, v)| (f.into(), v)).collect();
        for (field, _) in &branches {
            self.assert_not_ephemeral(field, "where");
        }
        if !branches.is_empty() {
            self.where_.push(WhereClause::Or(branches));
        }
        self
    }

    /// `ORDER BY field direction` (`ASC` / `DESC`).
    pub fn order_by(mut self, field: impl Into<String>, direction: impl Into<String>) -> Self {
        let field = field.into();
        self.assert_not_ephemeral(&field, "orderBy");
        self.order_by.push((field, direction.into()));
        self
    }

    pub fn limit(mut self, count: usize) -> Self {
        self.limit = Some(count);
        self
    }

    pub fn offset(mut self, count: usize) -> Self {
        self.offset = Some(count);
        self
    }

    /// At most one row (`LIMIT 1`).
    pub fn one(mut self) -> Self {
        self.is_one = true;
        self
    }

    /// Say that parts of this query are ignored where it is used, on purpose. A
    /// channel (`client.channel(query, peers)`) only reads the `where`, so
    /// `orderBy`, `limit`, `offset`, `select` and `.one()` do nothing there and
    /// draw a warning; this silences it while showing the reader the query
    /// carries arguments that do not apply (handy when one query object serves
    /// both a list and a channel). Does not change the query's hash.
    pub fn ignore_unused_args(mut self) -> Self {
        self.ignore_unused_args = true;
        self
    }

    /// An `-- @ephemeral` field only travels between channel peers; the
    /// database never holds it, so a query cannot filter or order on it.
    fn assert_not_ephemeral(&mut self, field: &str, clause: &str) {
        let Some(columns) = self.schema.tables.get(&self.table).map(|t| &t.columns) else {
            return;
        };
        let head = field.split('.').next().unwrap_or(field);
        let column = columns.get(field).or_else(|| columns.get(head));
        if column.is_some_and(|c| c.ephemeral) && self.error.is_none() {
            self.error = Some(Error::InvalidArgument(format!(
                "Cannot use '{field}' in {clause}(): it is marked '-- @ephemeral' on {}, so it only travels \
                 between peers and the database never holds it. Read the field from query results instead.",
                self.table
            )));
        }
    }

    /// Include a related table as a subquery projection (TS `related`).
    ///
    /// `field` is a relationship declared in the schema: a forward `record<x>`
    /// field (attached as a single row under the same name) or the derived
    /// reverse name (attached as a list). `modifier` shapes the joined rows: its
    /// `select` / `where` / `order_by` / `limit` apply per parent; a nested
    /// `.related()` inside it joins another level.
    ///
    /// An unknown relationship is SKIPPED with a warning rather than failing: a
    /// table owned by a devOnly backend is absent from the generated schema on
    /// some deployments, and failing hard would take the whole query (and its
    /// other `.related()` siblings) down. Mirrors the server's "unpermitted
    /// subquery -> empty" degradation.
    pub fn related(self, field: &str) -> Self {
        self.related_with(field, |q| q)
    }

    /// [`QueryBuilder::related`] with a modifier for the joined rows.
    pub fn related_with(
        mut self,
        field: &str,
        modifier: impl FnOnce(QueryBuilder) -> QueryBuilder,
    ) -> Self {
        if self.relations.iter().any(|r| r.alias == field) {
            return self;
        }
        let Some(rel) = find_relationship(&self.schema.relationships, &self.table, field).cloned()
        else {
            tracing::warn!(
                target: "spooky",
                ".related('{field}') skipped: no such relationship on '{}' in the client schema",
                self.table
            );
            return self;
        };

        let sub = modifier(QueryBuilder::new(rel.to.clone()).with_schema(self.schema.clone()));
        if let Some(e) = sub.error.clone() {
            self.error.get_or_insert(e);
        }
        // A subquery runs per parent row, so it cannot carry params of its own:
        // its `where` is inlined as literals, which only plain values can be.
        let mut where_ = Object::new();
        for clause in &sub.where_ {
            match clause {
                WhereClause::Cond {
                    field,
                    value: WhereValue::Eq(v),
                } => {
                    where_.insert(field.clone(), v.clone());
                }
                _ => {
                    self.error.get_or_insert(Error::InvalidArgument(format!(
                        ".related('{field}'): a sub-where takes plain equality values only"
                    )));
                }
            }
        }
        let limit = sub.limit.or(match rel.cardinality {
            // A one-to-one join takes at most one row (TS adds an implicit LIMIT 1).
            Cardinality::One => Some(1),
            Cardinality::Many => None,
        });
        self.relations.push(RelationPlan {
            alias: field.to_string(),
            table: rel.to.clone(),
            cardinality: rel.cardinality,
            foreign_key_field: rel.foreign_key_field().to_string(),
            select: sub.select,
            where_,
            order_by: sub.order_by,
            limit,
            relations: sub.relations,
        });
        self
    }

    // ---- output ---------------------------------------------------------------------

    /// The SurrealQL and its bind vars. Vars of a table the schema knows are
    /// re-typed from its columns (records, datetimes), so a `record<user>`
    /// condition matches on the server.
    pub fn build(&self) -> Result<(String, Object)> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        let effective_limit = if self.is_one { Some(1) } else { self.limit };
        let fetch = if self.relations.is_empty() {
            String::new()
        } else {
            let subs: Vec<String> = self.relations.iter().map(build_subquery).collect();
            format!(", {}", subs.join(", "))
        };
        let mut query = format!(
            "SELECT {}{fetch} FROM {}",
            self.select.join(", "),
            self.table
        );

        let mut vars = Object::new();
        if !self.where_.is_empty() {
            let mut conditions = Vec::new();
            let mut or_index = 0usize;
            for clause in &self.where_ {
                match clause {
                    WhereClause::Cond { field, value } => {
                        conditions.push(condition(field, value, field, &mut vars))
                    }
                    WhereClause::Or(branches) => {
                        let parts: Vec<String> = branches
                            .iter()
                            .map(|(field, value)| {
                                let name = or_param_name(field, or_index);
                                or_index += 1;
                                condition(field, value, &name, &mut vars)
                            })
                            .collect();
                        conditions.push(format!("({})", parts.join(" OR ")));
                    }
                }
            }
            query.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
        }
        if !self.order_by.is_empty() {
            let clauses: Vec<String> = self
                .order_by
                .iter()
                .map(|(f, d)| format!("{f} {d}"))
                .collect();
            query.push_str(&format!(" ORDER BY {}", clauses.join(", ")));
        }
        if let Some(limit) = effective_limit {
            query.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = self.offset {
            query.push_str(&format!(" START {offset}"));
        }
        query.push(';');
        let vars = type_params(&self.schema, &self.table, vars)?;
        Ok((query, vars))
    }

    /// Register the query and return its hash (TS `.run()`).
    pub async fn run(&self, ttl: &str) -> Result<QueryHash> {
        let host = self.host.clone().ok_or_else(|| {
            Error::InvalidArgument("QueryBuilder has no host (build-only)".into())
        })?;
        let (sql, vars) = self.build()?;
        host.register_query(&self.table, &sql, vars, ttl, self.relations.clone())
            .await
    }

    /// [`QueryBuilder::run`] with the default ttl.
    pub async fn run_default(&self) -> Result<QueryHash> {
        self.run(DEFAULT_TTL).await
    }

    /// Register the query and watch its rows (Dart `stream()`): `callback`
    /// fires with the current rows at once, then on every change, until the
    /// guard drops.
    pub async fn subscribe(
        &self,
        ttl: &str,
        callback: impl Fn(&Arc<Vec<Row>>) + 'static,
    ) -> Result<Subscription> {
        let hash = self.run(ttl).await?;
        let host = self
            .host
            .clone()
            .expect("run succeeded, so a host is attached");
        let callback: RecordsCallback = Rc::new(callback);
        Ok(host.subscribe(&hash, callback, true))
    }
}

/// `field__or<n>`: the FIELD is part of the name on purpose, so a consumer can
/// type the param by looking its name up in the table's columns
/// ([`base_field_of_param`]).
pub fn or_param_name(field: &str, index: usize) -> String {
    let safe: String = field
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{safe}__or{index}")
}

/// Bind one condition under `var_name`, returning its SurrealQL.
fn condition(field: &str, value: &WhereValue, var_name: &str, vars: &mut Object) -> String {
    match value {
        WhereValue::Op(op) => {
            let right = match &op.value {
                Value::String(s) if s.starts_with('$') => s.clone(),
                v => {
                    vars.insert(var_name.to_string(), v.clone());
                    format!("${var_name}")
                }
            };
            if op.swap {
                format!("{right} {} {field}", op.op)
            } else {
                format!("{field} {} {right}", op.op)
            }
        }
        WhereValue::Eq(v) => {
            vars.insert(var_name.to_string(), v.clone());
            format!("{field} = ${var_name}")
        }
    }
}

/// Re-type `vars` from the table's columns. `id` is always a record: a
/// `table:key` string becomes that record, a bare key a record of this table
/// (TS `parseObjectIdsToRecordId`). Null values stay null.
fn type_params(schema: &Schema, table: &str, vars: Object) -> Result<Object> {
    let Some(columns) = schema.tables.get(table).map(|t| &t.columns) else {
        return Ok(vars);
    };
    let mut typed = parse_query_params(columns, &vars)?;
    for (k, v) in &vars {
        if v.is_nullish() {
            typed.insert(k.clone(), v.clone());
        } else if base_field_of_param(k) == "id" && !columns.contains_key("id") {
            if let Value::String(s) = v {
                let id = if s.contains(':') {
                    RecordId::parse(s)
                } else {
                    RecordId::new(table, s.as_str())
                };
                typed.insert(k.clone(), Value::RecordId(id));
            }
        }
    }
    Ok(typed)
}

/// Emit one `.related()` branch as a correlated subquery projection (TS
/// `buildSubquery`).
///
/// A `one` relation correlates on the parent's foreign key
/// (`WHERE id=$parent.<fk>`) and takes the first row (`[0]`); a `many`
/// relation correlates on the child's back-reference (`WHERE <fk>=$parent.id`).
/// Sub-`where` values are INLINED as literals, not bound: a subquery runs per
/// parent row, so it cannot carry its own params.
fn build_subquery(rel: &RelationPlan) -> String {
    let mut select = if rel.select.is_empty() {
        "*".to_string()
    } else {
        rel.select.join(", ")
    };
    if !rel.relations.is_empty() {
        let nested: Vec<String> = rel.relations.iter().map(build_subquery).collect();
        select.push_str(&format!(", {}", nested.join(", ")));
    }
    let where_ = if rel.where_.is_empty() {
        String::new()
    } else {
        let parts: Vec<String> = rel
            .where_
            .iter()
            .map(|(k, v)| format!("{k} = {}", literal(v)))
            .collect();
        format!(" AND {}", parts.join(" AND "))
    };
    let order_by = if rel.order_by.is_empty() {
        String::new()
    } else {
        let parts: Vec<String> = rel
            .order_by
            .iter()
            .map(|(f, d)| format!("{f} {d}"))
            .collect();
        format!(" ORDER BY {}", parts.join(", "))
    };
    let limit = rel.limit.map(|l| format!(" LIMIT {l}")).unwrap_or_default();
    let correlation = if rel.is_one() {
        format!("WHERE id=$parent.{}", rel.foreign_key_field)
    } else {
        format!("WHERE {}=$parent.id", rel.foreign_key_field)
    };
    let subquery = format!(
        "(SELECT {select} FROM {} {correlation}{where_}{order_by}{limit})",
        rel.table
    );
    format!(
        "{subquery}{} AS {}",
        if rel.is_one() { "[0]" } else { "" },
        rel.alias
    )
}

/// A sub-`where` value as a SurrealQL literal: a record id unquoted, anything
/// else as JSON.
fn literal(value: &Value) -> String {
    match value {
        Value::RecordId(r) => r.to_string(),
        Value::String(s) if RECORD_LITERAL.is_match(s) => s.clone(),
        v => v.to_json().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::testing::run_local;
    use crate::types::ColumnSchema;
    use crate::types::SchemaRelationship;
    use crate::value::object_from_json;

    fn sql(b: QueryBuilder) -> String {
        b.build().unwrap().0
    }

    // ---- SURQL generation (Dart `query_builder_test.dart`) -------------------------

    #[test]
    fn default_select_all() {
        let (sql, vars) = QueryBuilder::new("thread").build().unwrap();
        assert_eq!(sql, "SELECT * FROM thread;");
        assert!(vars.is_empty());
    }

    #[test]
    fn explicit_select_fields() {
        assert_eq!(
            sql(QueryBuilder::new("thread").select(["title", "author"])),
            "SELECT title, author FROM thread;"
        );
        assert_eq!(
            sql(QueryBuilder::new("thread").select(Vec::<String>::new())),
            "SELECT * FROM thread;"
        );
    }

    #[test]
    fn equality_where() {
        let (sql, vars) = QueryBuilder::new("thread")
            .where_eq("published", true)
            .build()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM thread WHERE published = $published;");
        assert_eq!(vars, object_from_json(json!({"published": true})));
    }

    #[test]
    fn operator_where() {
        let (sql, vars) = QueryBuilder::new("thread")
            .where_op("age", QueryOp::new(">", 18))
            .build()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM thread WHERE age > $age;");
        assert_eq!(vars, object_from_json(json!({"age": 18})));
    }

    #[test]
    fn swapped_operator_where() {
        assert_eq!(
            sql(QueryBuilder::new("thread").where_op("tags", QueryOp::swapped("CONTAINS", "x"))),
            "SELECT * FROM thread WHERE $tags CONTAINS tags;"
        );
    }

    #[test]
    fn param_reference_operand_binds_nothing() {
        let (sql, vars) = QueryBuilder::new("thread")
            .where_op("id", QueryOp::swapped("∋", "$parentIds"))
            .build()
            .unwrap();
        assert_eq!(sql, "SELECT * FROM thread WHERE $parentIds ∋ id;");
        assert!(vars.is_empty());
    }

    #[test]
    fn multiple_conditions_are_and_joined_in_insertion_order() {
        let (sql, vars) = QueryBuilder::new("thread")
            .where_eq("published", true)
            .where_op("score", QueryOp::new(">=", 10))
            .build()
            .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM thread WHERE published = $published AND score >= $score;"
        );
        assert_eq!(
            vars,
            object_from_json(json!({"published": true, "score": 10}))
        );
        // Insertion order, not alphabetical.
        assert_eq!(
            sql_of(QueryBuilder::new("t").where_eq("z", 1).where_eq("a", 2)),
            "SELECT * FROM t WHERE z = $z AND a = $a;"
        );
        // A repeated field replaces its condition in place.
        assert_eq!(
            sql_of(
                QueryBuilder::new("t")
                    .where_eq("z", 1)
                    .where_eq("a", 2)
                    .where_op("z", QueryOp::new(">", 3))
            ),
            "SELECT * FROM t WHERE z > $z AND a = $a;"
        );
    }

    fn sql_of(b: QueryBuilder) -> String {
        sql(b)
    }

    #[test]
    fn order_by_limit_offset() {
        assert_eq!(
            sql(QueryBuilder::new("thread")
                .order_by("createdAt", "DESC")
                .limit(10)
                .offset(5)),
            "SELECT * FROM thread ORDER BY createdAt DESC LIMIT 10 START 5;"
        );
    }

    #[test]
    fn one_forces_limit_1() {
        assert_eq!(
            sql(QueryBuilder::new("thread").one()),
            "SELECT * FROM thread LIMIT 1;"
        );
        assert_eq!(
            sql(QueryBuilder::new("thread").limit(9).one()),
            "SELECT * FROM thread LIMIT 1;"
        );
    }

    #[test]
    fn full_clause_ordering() {
        assert_eq!(
            sql(QueryBuilder::new("thread")
                .where_eq("published", true)
                .order_by("createdAt", "ASC")
                .limit(3)),
            "SELECT * FROM thread WHERE published = $published ORDER BY createdAt ASC LIMIT 3;"
        );
    }

    #[test]
    fn or_groups_bind_field_indexed_params() {
        let (sql, vars) = QueryBuilder::new("game")
            .where_eq("white", "user:a")
            .where_or([
                ("white", WhereValue::Eq("user:b".into())),
                ("black", WhereValue::Op(QueryOp::new("!=", "user:c"))),
            ])
            .build()
            .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM game WHERE white = $white AND (white = $white__or0 OR black != $black__or1);"
        );
        assert_eq!(vars.len(), 3);
        assert_eq!(or_param_name("a.b", 2), "a_b__or2");
    }

    // ---- schema typing and ephemeral fields ------------------------------------------

    fn typed_schema() -> Schema {
        let mut s = Schema::default()
            .with_table(
                "thread",
                [
                    ("title", ColumnSchema::of_type("string")),
                    (
                        "author",
                        ColumnSchema {
                            record_id: true,
                            kind: Some("record".into()),
                            ..Default::default()
                        },
                    ),
                    (
                        "created_at",
                        ColumnSchema {
                            date_time: true,
                            kind: Some("datetime".into()),
                            ..Default::default()
                        },
                    ),
                ],
            )
            .with_table(
                "call",
                [(
                    "view_pos",
                    ColumnSchema {
                        ephemeral: true,
                        optional: true,
                        kind: Some("string".into()),
                        ..Default::default()
                    },
                )],
            );
        s.tables.get_mut("call").unwrap().channel = true;
        s
    }

    #[test]
    fn params_are_typed_from_the_schema() {
        let (_, vars) = QueryBuilder::new("thread")
            .with_schema(Arc::new(typed_schema()))
            .where_eq("author", "user:u1")
            .where_op("created_at", QueryOp::new(">", "2026-01-01T00:00:00Z"))
            .where_eq("id", "t1")
            .where_eq("title", Value::Null)
            .where_or([("author", WhereValue::Eq("user:u2".into()))])
            .build()
            .unwrap();
        assert_eq!(vars["author"], Value::RecordId(RecordId::new("user", "u1")));
        assert!(matches!(vars["created_at"], Value::Datetime(_)));
        assert_eq!(vars["id"], Value::RecordId(RecordId::new("thread", "t1")));
        assert_eq!(vars["title"], Value::Null);
        assert!(matches!(vars["author__or0"], Value::RecordId(_)));
    }

    #[test]
    fn ephemeral_fields_cannot_be_filtered_or_ordered() {
        let schema = Arc::new(typed_schema());
        let err = QueryBuilder::new("call")
            .with_schema(schema.clone())
            .where_eq("view_pos", "x")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("@ephemeral"), "{err}");
        assert!(QueryBuilder::new("call")
            .with_schema(schema)
            .order_by("view_pos.x", "ASC")
            .build()
            .is_err());
    }

    // ---- .related() (Dart `relations_test.dart`, builder side) ----------------------

    fn rel(from: &str, field: &str, to: &str, cardinality: Cardinality) -> SchemaRelationship {
        SchemaRelationship {
            from: from.into(),
            field: field.into(),
            to: to.into(),
            cardinality,
        }
    }

    fn rel_schema() -> Arc<Schema> {
        let record = || ColumnSchema {
            record_id: true,
            kind: Some("record".into()),
            ..Default::default()
        };
        let mut s = Schema::default()
            .with_table(
                "thread",
                [
                    ("title", ColumnSchema::of_type("string")),
                    ("author", record()),
                ],
            )
            .with_table(
                "comment",
                [
                    ("body", ColumnSchema::of_type("string")),
                    ("score", ColumnSchema::of_type("int")),
                    ("thread", record()),
                    ("author", record()),
                ],
            )
            .with_table("user", [("name", ColumnSchema::of_type("string"))]);
        s.relationships = vec![
            rel("thread", "author", "user", Cardinality::One),
            rel("comment", "thread", "thread", Cardinality::One),
            rel("comment", "author", "user", Cardinality::One),
            rel("user", "threads", "thread", Cardinality::Many),
            rel("thread", "comments", "comment", Cardinality::Many),
            rel("user", "comments", "comment", Cardinality::Many),
        ];
        Arc::new(s)
    }

    fn builder(table: &str) -> QueryBuilder {
        QueryBuilder::new(table).with_schema(rel_schema())
    }

    #[test]
    fn one_relation_correlates_on_the_parent_key() {
        assert_eq!(
            sql(builder("thread").related("author")),
            "SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM thread;"
        );
    }

    #[test]
    fn many_relation_correlates_on_the_child_back_reference() {
        assert_eq!(
            sql(builder("thread").related("comments")),
            "SELECT *, (SELECT * FROM comment WHERE thread=$parent.id) AS comments FROM thread;"
        );
    }

    #[test]
    fn modifier_shapes_select_where_order_limit() {
        assert_eq!(
            sql(builder("thread").related_with("comments", |c| c
                .select(["id", "body"])
                .where_eq("score", 5)
                .order_by("score", "DESC")
                .limit(3))),
            "SELECT *, (SELECT id, body FROM comment WHERE thread=$parent.id AND score = 5 ORDER BY score DESC LIMIT 3) AS comments FROM thread;"
        );
    }

    #[test]
    fn record_literal_in_sub_where_is_not_quoted() {
        let s =
            sql(builder("thread").related_with("comments", |c| c.where_eq("author", "user:u1")));
        assert!(s.contains("AND author = user:u1"), "{s}");
        assert!(!s.contains("\"user:u1\""));
        let s = sql(builder("thread").related_with("comments", |c| c.where_eq("body", "hi there")));
        assert!(s.contains("AND body = \"hi there\""), "{s}");
    }

    #[test]
    fn operator_in_sub_where_is_an_error() {
        let err = builder("thread")
            .related_with("comments", |c| c.where_op("score", QueryOp::new(">", 1)))
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn nested_relations_nest_their_subqueries() {
        assert_eq!(
            sql(builder("thread").related_with("comments", |c| c.related("author"))),
            "SELECT *, (SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author \
             FROM comment WHERE thread=$parent.id) AS comments FROM thread;"
        );
    }

    #[test]
    fn relations_compose_with_parent_where_order_window() {
        let (sql, vars) = builder("thread")
            .related("author")
            .where_eq("title", "x")
            .order_by("title", "ASC")
            .limit(2)
            .offset(2)
            .build()
            .unwrap();
        assert!(sql.starts_with("SELECT *, (SELECT * FROM user"));
        assert!(sql.contains("FROM thread WHERE title = $title"));
        assert!(sql.ends_with("ORDER BY title ASC LIMIT 2 START 2;"));
        assert_eq!(vars, object_from_json(json!({"title": "x"})));
    }

    #[test]
    fn unknown_relation_is_skipped_not_fatal() {
        let s = sql(builder("thread").related("nope").related("author"));
        assert!(!s.contains("nope"));
        assert!(
            s.contains("AS author"),
            "a sibling relation must survive an unknown one"
        );
    }

    #[test]
    fn repeated_relation_is_added_once() {
        let b = builder("thread").related("author").related("author");
        assert_eq!(b.relations().len(), 1);
        let plan = &b.relations()[0];
        assert_eq!(plan.table, "user");
        assert_eq!(plan.foreign_key_field, "author");
        assert_eq!(plan.limit, Some(1));
        assert_eq!(plan.select, ["*"]);
    }

    #[test]
    fn schema_without_relationships_skips_every_relation() {
        assert_eq!(
            sql(QueryBuilder::new("thread").related("author")),
            "SELECT * FROM thread;"
        );
    }

    // ---- registration through the host ------------------------------------------------

    /// `(table, surql, params, ttl, relation count)`.
    type Registered = (String, String, Object, String, usize);

    #[derive(Default)]
    struct Host {
        registered: RefCell<Vec<Registered>>,
        subscribed: RefCell<Vec<(String, bool)>>,
    }

    #[async_trait(?Send)]
    impl QueryHost for Host {
        async fn register_query(
            &self,
            table: &str,
            surql: &str,
            params: Object,
            ttl: &str,
            relations: Vec<RelationPlan>,
        ) -> Result<QueryHash> {
            self.registered.borrow_mut().push((
                table.into(),
                surql.into(),
                params,
                ttl.into(),
                relations.len(),
            ));
            Ok("hash-1".into())
        }

        fn subscribe(
            &self,
            hash: &str,
            callback: RecordsCallback,
            immediate: bool,
        ) -> Subscription {
            self.subscribed.borrow_mut().push((hash.into(), immediate));
            callback(&Arc::new(vec![object_from_json(json!({"id": "thread:a"}))]));
            Subscription::noop()
        }
    }

    #[tokio::test]
    async fn run_registers_through_the_host() {
        run_local(async {
            let host = Rc::new(Host::default());
            let b = QueryBuilder::bound("thread", host.clone(), rel_schema()).related("author");
            assert_eq!(b.run("5m").await.unwrap(), "hash-1");
            let (table, surql, _, ttl, relations) = host.registered.borrow()[0].clone();
            assert_eq!(table, "thread");
            assert!(surql.starts_with("SELECT *, (SELECT"));
            assert_eq!(ttl, "5m");
            assert_eq!(relations, 1);

            let seen = Rc::new(RefCell::new(0));
            let s = seen.clone();
            let _sub = b
                .subscribe(DEFAULT_TTL, move |rows| *s.borrow_mut() += rows.len())
                .await
                .unwrap();
            assert_eq!(*seen.borrow(), 1);
            assert_eq!(host.subscribed.borrow()[0], ("hash-1".to_string(), true));

            let err = QueryBuilder::new("thread").run_default().await.unwrap_err();
            assert!(err.to_string().contains("build-only"));
        })
        .await;
    }
}
