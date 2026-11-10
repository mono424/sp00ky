//! Every statement the query side sends, as pure builders.
//!
//! sqlite cannot run SurrealQL, so the `_00_view` row is addressed through the
//! local store by `(table, id)` instead of by a SurrealQL string.

use crate::kernel::ports::StatementResult;
use crate::modules::ref_tables::ANON_LIST_REF_TABLE;
use crate::types::{QueryTimeToLive, RecordVersion, RecordVersionArray};
use crate::value::{Object, RecordId, Value};

pub const VIEW_TABLE: &str = "_00_view";
pub const LEGACY_VIEW_TABLE: &str = "_00_window";

/// Record id of the durable local membership row for `view_key`.
pub fn view_record_id(view_key: &str) -> String {
    format!("{VIEW_TABLE}:{view_key}")
}

fn pairs_value(pairs: &[RecordVersion]) -> Value {
    Value::Array(
        pairs
            .iter()
            .map(|(id, v)| Value::Array(vec![Value::String(id.clone()), Value::Int(*v)]))
            .collect(),
    )
}

/// The durable local membership row's document. `children` are the
/// `.related()` subquery rows the view vouches for.
pub fn view_row(ids: &[RecordVersion], children: &[RecordVersion], confirmed: bool, now: i64) -> Object {
    let mut o = Object::new();
    o.insert("ids".into(), pairs_value(ids));
    o.insert("children".into(), pairs_value(children));
    o.insert("confirmed".into(), Value::Bool(confirmed));
    o.insert("updatedAt".into(), Value::Int(now));
    o
}

/// Read a `[[id, version], ...]` list back into pairs.
pub fn decode_view_ids(raw: Option<&Value>) -> RecordVersionArray {
    let Some(Value::Array(items)) = raw else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|entry| match entry {
            Value::Array(pair) if pair.len() >= 2 => Some((pair[0].to_id_string(), pair[1].as_i64().unwrap_or(0))),
            _ => None,
        })
        .collect()
}

// ---- remote reads ------------------------------------------------------------------

/// A query's primary window edges, read through the graph (`$in->table`): the
/// `_00_list_ref*` tables have no index on `in`.
pub fn list_ref_select(table: &str) -> String {
    format!("SELECT out, version FROM $in->{table} WHERE parent IS NONE")
}

/// A `.related()` query's SUBQUERY child edges.
pub fn subquery_list_ref_select(table: &str) -> String {
    format!("SELECT out, version FROM $in->{table} WHERE parent IS NOT NONE")
}

pub fn query_row_count_select() -> &'static str {
    "SELECT VALUE { rowCount: rowCount, state: state } FROM ONLY $in"
}

/// Every edge of many queries in one flat list; `in` names the query.
pub fn list_ref_batch_select(table: &str) -> String {
    format!("SELECT in, out, version, parent FROM $ins->{table}")
}

pub fn query_row_count_batch_select() -> &'static str {
    "SELECT VALUE { id: id, rowCount: rowCount, state: state } FROM $ins"
}

/// The single-query read: edges, meta and subquery children in one request.
pub fn single_snapshot_select(table: &str) -> String {
    format!(
        "{};\n{};\n{}",
        list_ref_select(table),
        query_row_count_select(),
        subquery_list_ref_select(table)
    )
}

/// The many-query read: all edges plus all metas in one request.
pub fn batch_snapshot_select(table: &str) -> String {
    format!("{};\n{}", list_ref_batch_select(table), query_row_count_batch_select())
}

/// The session's one LIVE subscription on its `_00_list_ref` table.
///
/// `inline_bodies` adds `FETCH out` (the body arrives with the doorbell). On
/// the anon table it is filtered to this client's own edges: the current
/// socket by `session::id()`, earlier sockets of this client by `session_ids`.
/// The `<string>` cast is load-bearing (`session::id()` is a uuid).
pub fn list_ref_live(table: &str, inline_bodies: bool, session_ids: &[String]) -> (String, Option<Object>) {
    let join = if inline_bodies { " FETCH out" } else { "" };
    if table != ANON_LIST_REF_TABLE {
        return (format!("LIVE SELECT * FROM {table}{join}"), None);
    }
    let mut vars = Object::new();
    vars.insert(
        "sids".into(),
        Value::Array(session_ids.iter().cloned().map(Value::String).collect()),
    );
    (
        format!("LIVE SELECT * FROM {table} WHERE clientId = <string>session::id() OR clientId IN $sids{join}"),
        Some(vars),
    )
}

/// What `fn::query::register($config)` is given.
#[derive(Clone, Debug)]
pub struct RegisterPayload {
    pub id: RecordId,
    pub surql: String,
    pub params: Object,
    pub ttl: QueryTimeToLive,
    /// The registering client's secret: the SSP lets a registration onto an
    /// existing anonymous view only with the hash it was registered with. Kept
    /// out of `params`, which name the computation the SSP shares.
    pub owner: Option<String>,
}

impl RegisterPayload {
    pub fn to_object(&self) -> Object {
        let mut o = Object::new();
        o.insert("id".into(), Value::RecordId(self.id.clone()));
        o.insert("surql".into(), Value::String(self.surql.clone()));
        o.insert("params".into(), Value::Object(self.params.clone()));
        o.insert("ttl".into(), Value::String(self.ttl.clone()));
        if let Some(owner) = &self.owner {
            o.insert("owner".into(), Value::String(owner.clone()));
        }
        o
    }
}

/// Register plus read back edges/meta/children in ONE request.
pub fn register_select(table: &str) -> String {
    format!("fn::query::register($config);\n{}", single_snapshot_select(table))
}

pub fn register_vars(payload: &RegisterPayload) -> Object {
    let mut o = Object::new();
    o.insert("config".into(), Value::Object(payload.to_object()));
    o.insert("in".into(), Value::RecordId(payload.id.clone()));
    o
}

/// One heartbeat statement per view id; result index `i` answers id `i`.
pub fn heartbeat_batch(ids: &[RecordId]) -> (String, Object) {
    let mut vars = Object::new();
    let mut stmts = Vec::with_capacity(ids.len());
    for (i, id) in ids.iter().enumerate() {
        vars.insert(format!("id{i}"), Value::RecordId(id.clone()));
        stmts.push(format!("fn::query::heartbeat($id{i})"));
    }
    (stmts.join(";\n"), vars)
}

/// A heartbeat answer whose statement matched no row: the view was reclaimed.
pub fn heartbeat_row_gone(result: Option<&Value>) -> bool {
    matches!(result, Some(Value::Array(a)) if a.is_empty())
}

pub fn body_select() -> &'static str {
    "SELECT * FROM $ids"
}

/// Release a view the client no longer holds.
pub fn unsubscribe_select() -> &'static str {
    "fn::query::unsubscribe($id)"
}

// ---- result shaping -------------------------------------------------------------------

/// `[out, version]` pairs from a `_00_list_ref` select result.
pub fn parse_list_ref_rows(result: Option<&Value>) -> RecordVersionArray {
    let Some(Value::Array(items)) = result else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let o = item.as_object()?;
            Some((
                o.get("out").map(Value::to_id_string).unwrap_or_default(),
                o.get("version").and_then(Value::as_i64).unwrap_or(0),
            ))
        })
        .collect()
}

/// The statement at `index`, or `None` when the response was shorter.
pub fn stmt(results: &[StatementResult], index: usize) -> Option<&StatementResult> {
    results.get(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_statements() {
        let (sql, vars) = list_ref_live("_00_list_ref_user_a", true, &[]);
        assert_eq!(sql, "LIVE SELECT * FROM _00_list_ref_user_a FETCH out");
        assert!(vars.is_none());
        let (sql, vars) = list_ref_live(ANON_LIST_REF_TABLE, false, &["s1".into()]);
        assert!(sql.contains("clientId IN $sids"));
        assert_eq!(vars.unwrap()["sids"], Value::Array(vec![Value::String("s1".into())]));
    }

    #[test]
    fn view_row_round_trip() {
        let row = view_row(&[("t:1".into(), 3)], &[], true, 5);
        assert_eq!(decode_view_ids(row.get("ids")), vec![("t:1".to_string(), 3)]);
    }

    #[test]
    fn heartbeat() {
        let (sql, vars) = heartbeat_batch(&[RecordId::new("_00_query", "a"), RecordId::new("_00_query", "b")]);
        assert_eq!(sql, "fn::query::heartbeat($id0);\nfn::query::heartbeat($id1)");
        assert_eq!(vars.len(), 2);
        assert!(heartbeat_row_gone(Some(&Value::Array(vec![]))));
        assert!(!heartbeat_row_gone(None));
    }
}
