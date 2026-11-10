//! Small pure helpers shared by the sagas and services.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::LazyLock;

use chrono::{DateTime, TimeZone, Utc};
use regex::Regex;

use crate::error::{Error, Result};
use crate::types::ColumnSchema;
use crate::value::{Object, RecordId, Row, Value};

// ---- parser (TS `utils/parser`) ------------------------------------------------

/// Strip server-only fields: keep `id`, any `_00_*` metadata, and declared
/// schema columns (TS `cleanRecord`).
pub fn clean_record(columns: &BTreeMap<String, ColumnSchema>, record: &Row) -> Row {
    record
        .iter()
        .filter(|(k, _)| k.as_str() == "id" || k.starts_with("_00_") || columns.contains_key(k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// The OPTIONAL schema columns a full server row does not carry, for clearing
/// when that row is landed with MERGE (TS `absentOptionalColumns`). A key that
/// is present but null counts as absent. Required columns are left alone.
pub fn absent_optional_columns(columns: Option<&BTreeMap<String, ColumnSchema>>, record: &Row) -> Vec<String> {
    let Some(columns) = columns else {
        return Vec::new();
    };
    columns
        .iter()
        .filter(|(k, c)| {
            k.as_str() != "id"
                && !k.starts_with("_00_")
                && c.optional
                && record.get(k.as_str()).is_none_or(Value::is_nullish)
        })
        .map(|(k, _)| k.clone())
        .collect()
}

/// Coerce query params to their schema column types, keeping only declared
/// columns (TS `parseParams`).
pub fn parse_params(columns: &BTreeMap<String, ColumnSchema>, params: &Object) -> Result<Object> {
    let mut out = Object::new();
    for (k, v) in params {
        if let Some(c) = columns.get(k) {
            if !v.is_nullish() {
                out.insert(k.clone(), parse_value(k, c, v)?);
            }
        }
    }
    Ok(out)
}

static OR_SUFFIX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(.+)__or\d+$").unwrap());

/// The schema field an `OR`-group parameter was minted from (`field__or<n>`).
pub fn base_field_of_param(name: &str) -> &str {
    OR_SUFFIX
        .captures(name)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str())
        .unwrap_or(name)
}

/// Re-type values from the schema, KEEPING keys the schema does not declare
/// (TS `parseQueryParams`). Null/None values are omitted: an unset optional
/// must stay NONE, and SurrealDB rejects NULL for `option<T>`.
pub fn parse_query_params(columns: &BTreeMap<String, ColumnSchema>, params: &Object) -> Result<Object> {
    let mut out = Object::new();
    for (k, v) in params {
        if v.is_nullish() {
            continue;
        }
        let column = columns.get(k).or_else(|| columns.get(base_field_of_param(k)));
        out.insert(
            k.clone(),
            match column {
                Some(c) => parse_value(k, c, v)?,
                None => v.clone(),
            },
        );
    }
    Ok(out)
}

/// Parse an ISO-8601 / RFC 3339 datetime (also a date with no offset, read as
/// UTC).
pub fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(d) = DateTime::parse_from_rfc3339(s) {
        return Some(d.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"] {
        if let Ok(n) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(Utc.from_utc_datetime(&n));
        }
    }
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|n| Utc.from_utc_datetime(&n))
}

fn parse_value(name: &str, column: &ColumnSchema, value: &Value) -> Result<Value> {
    if column.record_id {
        return match value {
            Value::RecordId(_) => Ok(value.clone()),
            Value::String(s) => Ok(Value::RecordId(RecordId::parse(s))),
            // Arrays of links (`array<record<x>>`) re-type element-wise.
            Value::Array(items) => items
                .iter()
                .map(|v| parse_value(name, column, v))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array),
            _ => Err(Error::InvalidArgument(format!("Invalid value for {name}: {value}"))),
        };
    }
    if column.date_time {
        return match value {
            Value::Datetime(_) => Ok(value.clone()),
            Value::Int(ms) => Utc
                .timestamp_millis_opt(*ms)
                .single()
                .map(Value::Datetime)
                .ok_or_else(|| Error::InvalidArgument(format!("Invalid value for {name}: {value}"))),
            Value::String(s) => parse_datetime(s)
                .map(Value::Datetime)
                .ok_or_else(|| Error::InvalidArgument(format!("Invalid value for {name}: {s}"))),
            _ => Err(Error::InvalidArgument(format!("Invalid value for {name}: {value}"))),
        };
    }
    Ok(value.clone())
}

// ---- error classification (TS `classifySyncError`) ---------------------------------

const NETWORK_ERROR_PATTERNS: &[&str] = &[
    "connection",
    // Session fencing cancels transport work, never rejects the user's write.
    "account changed",
    // SurrealDB's ConnectionUnavailableError reads "You must be connected...".
    "must be connected",
    "connectionunavailable",
    "timeout",
    "timed out",
    "websocket",
    "fetch failed",
    "disconnected",
    "socket",
    "network",
    "econnrefused",
    "econnreset",
    "enotfound",
    "epipe",
    "abort",
    // Transient server-side states that clear on their own: retry, never roll
    // the write back (whitepawn 2026-09-08 lost 752 games to these).
    "transaction conflict",
    "resource busy",
    "can be retried",
    "specify a namespace",
    "specify a database",
];

/// A write refused because sp00ky's OWN ingest post failed inside the write's
/// transaction (scheduler/SSP down, slow or 5xx): an outage, not the write's
/// fault. Only `_00_` events count.
static SP00KY_EVENT_POST_FAILED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"error while processing event _00_[^:\s]*: (?:there was an error processing a remote http request: (?:error sending request|5\d\d\b|408\b|429\b)|failed to lookup address information)",
    )
    .unwrap()
});

/// Whether a sync failure is recoverable (`Network`) or terminal
/// (`Application`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Network,
    Application,
}

impl ErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::Network => "network",
            ErrorKind::Application => "application",
        }
    }
}

/// Classify a sync error message (TS `classifySyncError`).
pub fn classify_sync_error(message: &str) -> ErrorKind {
    let m = message.to_lowercase();
    if SP00KY_EVENT_POST_FAILED.is_match(&m) || NETWORK_ERROR_PATTERNS.iter().any(|p| m.contains(p)) {
        ErrorKind::Network
    } else {
        ErrorKind::Application
    }
}

/// [`classify_sync_error`] for an [`Error`]: transport failures are network
/// whatever their text says.
pub fn classify_error(error: &Error) -> ErrorKind {
    if error.is_network() || matches!(error, Error::AccountChanged | Error::Closed) {
        ErrorKind::Network
    } else {
        classify_sync_error(&error.to_string())
    }
}

// ---- row ordering (TS `sortRows`) --------------------------------------------------

/// One `ORDER BY` term: field and direction (`asc` / `desc`, any case).
pub type OrderByTerm = (String, String);

/// Stable string key for a value, for ordering and relation matching. A record
/// id and its `table:id` string collapse to the same key.
pub fn stable_key(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::None) | Some(Value::Null) => " null".to_string(),
        Some(v) => v.to_id_string(),
    }
}

/// Compare two field values: nulls last (ascending), numbers numerically,
/// everything else by [`stable_key`].
pub fn compare_values(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    let a = a.filter(|v| !v.is_nullish());
    let b = b.filter(|v| !v.is_nullish());
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(x), Some(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(p), Some(q)) if matches!(x, Value::Int(_) | Value::Float(_)) && matches!(y, Value::Int(_) | Value::Float(_)) => {
                p.partial_cmp(&q).unwrap_or(Ordering::Equal)
            }
            _ => stable_key(Some(x)).cmp(&stable_key(Some(y))),
        },
    }
}

/// Stable multi-key sort matching SurrealQL `ORDER BY` closely enough for
/// cache display order. Equal rows keep input order.
pub fn sort_rows(rows: &mut [Row], order_by: &[OrderByTerm]) {
    if order_by.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for (field, direction) in order_by {
            let cmp = compare_values(a.get(field), b.get(field));
            if cmp != Ordering::Equal {
                return if direction.eq_ignore_ascii_case("desc") { cmp.reverse() } else { cmp };
            }
        }
        Ordering::Equal
    });
}

// ---- semver (TS `utils/semver`) -----------------------------------------------------

fn parse_semver(v: &str) -> Option<[u64; 3]> {
    let parts: Vec<&str> = v.trim().split('.').collect();
    if parts.is_empty() || parts.len() > 3 || parts[0].is_empty() {
        return None;
    }
    let mut out = [0u64; 3];
    for (i, slot) in out.iter_mut().enumerate() {
        let raw = parts.get(i).copied().unwrap_or("0");
        if raw.is_empty() || !raw.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        *slot = raw.parse().ok()?;
    }
    Some(out)
}

/// True when `a` is a valid `X.Y.Z` version strictly greater than valid `b`.
/// Malformed input never compares greater.
pub fn semver_gt(a: &str, b: &str) -> bool {
    match (parse_semver(a), parse_semver(b)) {
        (Some(pa), Some(pb)) => pa > pb,
        _ => false,
    }
}

// ---- surql ----------------------------------------------------------------------

static FROM_TABLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bFROM\s+(?:ONLY\s+)?([A-Za-z_][A-Za-z0-9_]*)").unwrap());
static ORDER_BY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bORDER\s+BY\b").unwrap());

/// The target table of a `SELECT ... FROM <table>` query.
pub fn parse_table_from_surql(sql: &str) -> Result<String> {
    FROM_TABLE
        .captures(sql)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .ok_or_else(|| Error::InvalidArgument(format!("Could not parse table from query: {sql}")))
}

/// Whether the query orders itself.
pub fn has_order_by(sql: &str) -> bool {
    ORDER_BY.is_match(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::object_from_json;
    use serde_json::json;

    #[test]
    fn classify() {
        assert_eq!(classify_sync_error("Transaction conflict: Resource busy"), ErrorKind::Network);
        assert_eq!(
            classify_sync_error("Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 503 Service Unavailable"),
            ErrorKind::Network
        );
        assert_eq!(classify_sync_error("Found NONE for field `title`"), ErrorKind::Application);
    }

    #[test]
    fn sorts_nulls_last_and_stable() {
        let mut rows = vec![
            object_from_json(json!({"id": "a", "n": 2})),
            object_from_json(json!({"id": "b"})),
            object_from_json(json!({"id": "c", "n": 1})),
            object_from_json(json!({"id": "d", "n": 2})),
        ];
        sort_rows(&mut rows, &[("n".into(), "asc".into())]);
        let ids: Vec<_> = rows.iter().map(|r| r["id"].to_id_string()).collect();
        assert_eq!(ids, ["c", "a", "d", "b"]);
        sort_rows(&mut rows, &[("n".into(), "DESC".into())]);
        let ids: Vec<_> = rows.iter().map(|r| r["id"].to_id_string()).collect();
        assert_eq!(ids, ["b", "a", "d", "c"]);
    }

    #[test]
    fn query_params_retype() {
        let mut cols = BTreeMap::new();
        cols.insert("author".to_string(), ColumnSchema { record_id: true, ..Default::default() });
        cols.insert("at".to_string(), ColumnSchema { date_time: true, ..Default::default() });
        let p = parse_query_params(&cols, &object_from_json(json!({"author__or1": "user:x", "at": "2026-01-01T00:00:00Z", "skip": null, "k": 1}))).unwrap();
        assert!(matches!(p["author__or1"], Value::RecordId(_)));
        assert!(matches!(p["at"], Value::Datetime(_)));
        assert!(!p.contains_key("skip"));
        assert_eq!(p["k"], Value::Int(1));
    }

    #[test]
    fn semver() {
        assert!(semver_gt("1.2.1", "1.2"));
        assert!(!semver_gt("1.2", "1.2.0"));
        assert!(!semver_gt("x", "1.0.0"));
    }

    #[test]
    fn table_parse() {
        assert_eq!(parse_table_from_surql("SELECT * FROM ONLY thread WHERE x").unwrap(), "thread");
        assert!(has_order_by("select * from t order  by x"));
    }
}
