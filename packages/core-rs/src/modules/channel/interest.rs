//! Interest: what a device's open queries can show.
//!
//! PORT: the `interest` section of `packages/core/src/modules/channel/index.ts`,
//! and the same message on the wire, so a Rust device and a browser tab of one
//! user read each other's.
//!
//! The devices of one user share their optimistic writes (`channels.share`).
//! Each tells the others which rows its subscriptions can show, and a sender
//! skips a device that shows nothing of the row: the server brings the write
//! there anyway, it only would not have been on screen any sooner. Whatever
//! the row alone cannot decide counts as the whole table, so being unsure
//! costs a few messages too many and never a missing one.

use std::collections::BTreeMap;

use serde_json::{json, Map as JsonMap, Value as Json};

use super::{decode_value, encode_value, op_fn, ChannelCondition};
use crate::modules::query_builder::{QueryBuilder, RelationPlan, WhereClause, WhereValue};
use crate::types::{ColumnSchema, Schema};
use crate::utils::parse_datetime;
use crate::value::{RecordId, Row, Value};

/// One conjunct of a filter: a comparison, or a group of which one has to hold
/// (the query builder's `where_or`).
#[derive(Clone, Debug, PartialEq)]
pub enum WhereNode {
    Cond(ChannelCondition),
    Or(Vec<ChannelCondition>),
}

/// One query's claim on a table: the rows its filter passes, or (`None`) all
/// of them.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryInterest {
    pub table: String,
    pub where_: Option<Vec<WhereNode>>,
}

impl QueryInterest {
    /// Every row of `table`.
    pub fn whole(table: impl Into<String>) -> Self {
        QueryInterest {
            table: table.into(),
            where_: None,
        }
    }
}

/// What a device shows of one table.
#[derive(Clone, Debug, PartialEq)]
pub enum TableInterest {
    /// Every row.
    All,
    /// The filters of the queries on it: a row one of them passes is shown.
    Filters(Vec<Vec<WhereNode>>),
}

/// What a device's open queries can show.
#[derive(Clone, Debug, PartialEq)]
pub enum Interest {
    /// Everything (it could not say more).
    All,
    /// Per table. A table left out shows nothing.
    Tables(BTreeMap<String, TableInterest>),
}

/// Past this many filters on one table a device just asks for all of it: the
/// message stays small.
pub const MAX_INTEREST_FILTERS: usize = 24;

/// Whether `row` passes a filter (a conjunction of comparisons and OR groups).
pub fn matches_nodes(where_: &[WhereNode], row: &Row) -> bool {
    where_.iter().all(|node| match node {
        WhereNode::Cond(c) => super::matches_where(std::slice::from_ref(c), row),
        WhereNode::Or(group) => group
            .iter()
            .any(|c| super::matches_where(std::slice::from_ref(c), row)),
    })
}

/// A filter operand typed by its column before it goes out, so a browser
/// compares a date with a date (this client's rows and query values hold
/// records and dates as strings).
fn typed_operand(col: Option<&ColumnSchema>, v: &Value) -> Value {
    match (col, v) {
        (Some(c), Value::String(s)) if c.record_id && s.contains(':') => {
            Value::RecordId(RecordId::parse(s))
        }
        (Some(c), Value::String(s)) if c.date_time => parse_datetime(s)
            .map(Value::Datetime)
            .unwrap_or_else(|| v.clone()),
        (Some(_), Value::Array(items)) => {
            Value::Array(items.iter().map(|x| typed_operand(col, x)).collect())
        }
        _ => v.clone(),
    }
}

/// One condition of a query as a test the row alone decides, or `None`.
fn row_condition(
    columns: Option<&BTreeMap<String, ColumnSchema>>,
    table: &str,
    field: &str,
    value: &WhereValue,
) -> Option<ChannelCondition> {
    let (op, operand, swap) = match value {
        WhereValue::Eq(v) => ("=", v, false),
        WhereValue::Op(op) => {
            // `$name` names a parameter the builder does not hold the value of.
            if matches!(&op.value, Value::String(s) if s.starts_with('$')) {
                return None;
            }
            (op.op.as_str(), &op.value, op.swap)
        }
    };
    op_fn(op)?;
    let root = field.split('.').next().unwrap_or(field);
    let root_column = columns.and_then(|c| c.get(root));
    if field.contains('.') && root_column.is_some_and(|c| c.record_id) {
        return None;
    }
    let operand = match operand {
        // A bare key on `id` is a row of this table, as the builder binds it.
        Value::String(key) if field == "id" && !key.contains(':') => {
            Value::from(format!("{table}:{key}"))
        }
        other => typed_operand(columns.and_then(|c| c.get(field)), other),
    };
    Some(ChannelCondition {
        field: field.to_string(),
        op: op.to_string(),
        value: operand,
        swap,
    })
}

/// A query's `where` as a test the row alone decides. `None` when it cannot be
/// one (an operator this module does not evaluate, a path through a record
/// link, a parameter without a value): then the query counts as showing the
/// whole table, which only costs a few messages too many.
fn row_filter(
    columns: Option<&BTreeMap<String, ColumnSchema>>,
    table: &str,
    where_: &[WhereClause],
) -> Option<Vec<WhereNode>> {
    where_
        .iter()
        .map(|clause| match clause {
            WhereClause::Cond { field, value } => {
                row_condition(columns, table, field, value).map(WhereNode::Cond)
            }
            WhereClause::Or(branches) => branches
                .iter()
                .map(|(field, value)| row_condition(columns, table, field, value))
                .collect::<Option<Vec<_>>>()
                .map(WhereNode::Or),
        })
        .collect()
}

/// Every table a `.related()` plan reads, however deep: which children a query
/// shows depends on the parents, not on the child row alone.
pub fn related_interest(relations: &[RelationPlan], out: &mut Vec<QueryInterest>) {
    for r in relations {
        out.push(QueryInterest::whole(r.table.clone()));
        related_interest(&r.relations, out);
    }
}

/// What a registered query can show: the rows of its own table that pass its
/// filter, and every row of the tables it reads through `.related()`. Order,
/// limit and offset are ignored: a row inside the filter and outside the
/// window is sent and then not shown.
pub fn query_interest(schema: &Schema, q: &QueryBuilder) -> Vec<QueryInterest> {
    let table = q.table();
    let mut out = vec![QueryInterest {
        table: table.to_string(),
        where_: row_filter(schema.columns(table), table, q.conditions()),
    }];
    related_interest(q.relations(), &mut out);
    out
}

fn encode_condition(c: &ChannelCondition) -> Json {
    let mut out = JsonMap::new();
    out.insert("field".into(), Json::from(c.field.clone()));
    out.insert("op".into(), Json::from(c.op.clone()));
    out.insert("value".into(), encode_value(&c.value));
    if c.swap {
        out.insert("swap".into(), Json::Bool(true));
    }
    Json::Object(out)
}

fn encode_where(where_: &[WhereNode]) -> Json {
    Json::Array(
        where_
            .iter()
            .map(|node| match node {
                WhereNode::Cond(c) => encode_condition(c),
                WhereNode::Or(group) => {
                    json!({"or": group.iter().map(encode_condition).collect::<Vec<_>>()})
                }
            })
            .collect(),
    )
}

/// The interests of every open subscription as one, for the tables `shared`
/// names. [`Interest::All`] once one of them is unknown (`None`). Each filter
/// once and in a fixed order, so the same set of queries always reads the
/// same.
pub fn merge_interest<'a>(
    list: impl IntoIterator<Item = Option<&'a [QueryInterest]>>,
    shared: impl Fn(&str) -> bool,
) -> Interest {
    // Per table: `None` for all of it, else its filters by their wire text.
    let mut tables: BTreeMap<String, Option<BTreeMap<String, Vec<WhereNode>>>> = BTreeMap::new();
    for queries in list {
        let Some(queries) = queries else {
            return Interest::All;
        };
        for q in queries {
            if !shared(&q.table) {
                continue;
            }
            let entry = tables
                .entry(q.table.clone())
                .or_insert_with(|| Some(BTreeMap::new()));
            let Some(filters) = entry else { continue };
            match q.where_.as_deref() {
                None | Some([]) => *entry = None,
                Some(where_) => {
                    filters.insert(encode_where(where_).to_string(), where_.to_vec());
                    if filters.len() > MAX_INTEREST_FILTERS {
                        *entry = None;
                    }
                }
            }
        }
    }
    Interest::Tables(
        tables
            .into_iter()
            .map(|(table, filters)| {
                let claim = match filters {
                    None => TableInterest::All,
                    Some(filters) => TableInterest::Filters(filters.into_values().collect()),
                };
                (table, claim)
            })
            .collect(),
    )
}

/// The message a device sends its user's other devices:
/// `{"t":"interest","all":true}`, or `{"t":"interest","v":{table: true | [filter, ...]}}`
/// with every operand wire-encoded (see [`encode_value`]).
pub fn encode_interest(interest: &Interest) -> Json {
    match interest {
        Interest::All => json!({"t": "interest", "all": true}),
        Interest::Tables(tables) => {
            let v: JsonMap<String, Json> = tables
                .iter()
                .map(|(table, claim)| {
                    let wire = match claim {
                        TableInterest::All => Json::Bool(true),
                        TableInterest::Filters(filters) => {
                            Json::Array(filters.iter().map(|w| encode_where(w)).collect())
                        }
                    };
                    (table.clone(), wire)
                })
                .collect();
            json!({"t": "interest", "v": v})
        }
    }
}

/// A wire operand as a filter compares it: a record id and a date keep their
/// type (they are compared, never stored), so a date matches whatever
/// precision the row spells it with.
fn decode_operand(v: &Json) -> Value {
    match v {
        Json::Array(items) => Value::Array(items.iter().map(decode_operand).collect()),
        Json::Object(o) if o.len() == 1 => {
            if let Some(Json::String(rid)) = o.get("$rid") {
                return Value::RecordId(RecordId::parse(rid));
            }
            if let Some(date) = o
                .get("$date")
                .and_then(Json::as_str)
                .and_then(parse_datetime)
            {
                return Value::Datetime(date);
            }
            decode_value(v)
        }
        other => decode_value(other),
    }
}

fn decode_condition(raw: &Json) -> Option<ChannelCondition> {
    let c = raw.as_object()?;
    let field = c.get("field")?.as_str()?;
    let op = c.get("op")?.as_str()?;
    op_fn(op)?;
    Some(ChannelCondition {
        field: field.to_string(),
        op: op.to_string(),
        value: decode_operand(c.get("value").unwrap_or(&Json::Null)),
        swap: c.get("swap") == Some(&Json::Bool(true)),
    })
}

/// One filter of a peer's message, or `None` when this side cannot read it.
fn decode_where(raw: &Json) -> Option<Vec<WhereNode>> {
    raw.as_array()?
        .iter()
        .map(|node| {
            let group = node.get("or").and_then(Json::as_array);
            match group {
                Some(group) if group.is_empty() => None,
                Some(group) => group
                    .iter()
                    .map(decode_condition)
                    .collect::<Option<Vec<_>>>()
                    .map(WhereNode::Or),
                None => decode_condition(node).map(WhereNode::Cond),
            }
        })
        .collect()
}

/// A peer's interest message, or `None` when it is not one. A filter this side
/// cannot read turns its table into "all of it": never into less than the peer
/// asked for.
pub fn decode_interest(raw: &Json) -> Option<Interest> {
    let m = raw.as_object()?;
    if m.get("t").and_then(Json::as_str) != Some("interest") {
        return None;
    }
    if m.get("all") == Some(&Json::Bool(true)) {
        return Some(Interest::All);
    }
    let v = m.get("v")?.as_object()?;
    let mut out = BTreeMap::new();
    for (table, filters) in v {
        let claim = match filters {
            Json::Bool(true) => TableInterest::All,
            Json::Array(filters) => {
                let readable = (filters.len() <= MAX_INTEREST_FILTERS)
                    .then(|| filters.iter().map(decode_where).collect::<Option<Vec<_>>>())
                    .flatten();
                readable
                    .map(TableInterest::Filters)
                    .unwrap_or(TableInterest::All)
            }
            _ => continue,
        };
        out.insert(table.clone(), claim);
    }
    Some(Interest::Tables(out))
}

/// Whether a device with this interest can show one of `rows` (a row before
/// and after a write) of `table`.
pub fn shows_row(interest: &Interest, table: &str, rows: &[Row]) -> bool {
    let Interest::Tables(tables) = interest else {
        return true;
    };
    match tables.get(table) {
        None => false,
        Some(TableInterest::All) => true,
        Some(TableInterest::Filters(filters)) => filters
            .iter()
            .any(|where_| rows.iter().any(|r| matches_nodes(where_, r))),
    }
}
