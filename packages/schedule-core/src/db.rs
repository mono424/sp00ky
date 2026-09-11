//! Database port.
//!
//! Mirrors `ssp_node::ports::Db` so the singlenode adapter is a one-line
//! forward, while `apps/scheduler` (which does not depend on ssp-node) can
//! implement it over its own `Surreal<Client>`. Results follow the same
//! flattened-JSON convention: RecordIds and Datetimes arrive as plain strings.

use crate::MaybeSendSync;

#[derive(Debug, thiserror::Error)]
pub enum ScheduleDbError {
    #[error("auth: {0}")]
    Auth(String),
    #[error("transport: {0}")]
    Transport(String),
    #[error("query: {0}")]
    Query(String),
}

impl ScheduleDbError {
    /// A duplicate-record-id `CREATE` — the engine's idempotency signal, not a
    /// failure. SurrealDB phrases it as "Database record `x:y` already exists".
    pub fn is_already_exists(&self) -> bool {
        match self {
            ScheduleDbError::Query(msg) => {
                let msg = msg.to_ascii_lowercase();
                msg.contains("already exists") || msg.contains("already contains")
            }
            _ => false,
        }
    }

    /// An UPDATE that failed because the table is SCHEMAFULL and the field
    /// isn't defined. Lets callers degrade instead of wedging (see the job
    /// `result` capture fallback).
    pub fn is_unknown_field(&self) -> bool {
        match self {
            ScheduleDbError::Query(msg) => {
                let msg = msg.to_ascii_lowercase();
                msg.contains("found no field") || msg.contains("does not exist")
            }
            _ => false,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait ScheduleDb: MaybeSendSync {
    /// Execute one SurrealQL statement (or a `;`-joined block) with bindings.
    /// Returns one flattened-JSON value per statement.
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, serde_json::Value)],
    ) -> Result<Vec<serde_json::Value>, ScheduleDbError>;
}

/// First statement's result as an array of rows (the shape every engine SELECT
/// expects). A single-object result is wrapped, `NONE`/null becomes empty.
pub fn rows(results: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    match results.into_iter().next() {
        Some(serde_json::Value::Array(rows)) => rows,
        Some(serde_json::Value::Null) | None => Vec::new(),
        Some(other) => vec![other],
    }
}

/// First row of the first statement, if any.
pub fn first_row(results: Vec<serde_json::Value>) -> Option<serde_json::Value> {
    rows(results).into_iter().next()
}

/// First statement's result read as a count.
///
/// `SELECT VALUE count() ... GROUP ALL` does NOT have a stable result shape: on
/// some query plans it yields a bare int, on others a `{ "count": n }` group
/// object — and merely defining an index that covers the predicate is enough to
/// flip it. Reading only the int shape is therefore a latent bug: adding
/// `idx_srun_retention` silently turned the active-run lookup into `0` rows, which
/// disables `concurrency: skip` and `replace` without any error anywhere.
///
/// So every count goes through here, and accepts either shape.
pub fn count_value(results: Vec<serde_json::Value>) -> i64 {
    match first_row(results) {
        Some(serde_json::Value::Object(map)) => {
            map.get("count").and_then(serde_json::Value::as_i64).unwrap_or(0)
        }
        Some(v) => v.as_i64().unwrap_or(0),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rows_normalizes_result_shapes() {
        assert_eq!(rows(vec![json!([{"a": 1}])]).len(), 1);
        assert_eq!(rows(vec![json!({"a": 1})]).len(), 1);
        assert!(rows(vec![json!(null)]).is_empty());
        assert!(rows(vec![]).is_empty());
    }

    /// Both shapes `SELECT VALUE count() ... GROUP ALL` can return, because which
    /// one you get depends on the query plan and therefore on which indexes exist.
    #[test]
    fn count_value_accepts_either_aggregate_shape() {
        assert_eq!(count_value(vec![json!([3])]), 3);
        assert_eq!(count_value(vec![json!([{ "count": 3 }])]), 3);
        assert_eq!(count_value(vec![json!(3)]), 3);
        assert_eq!(count_value(vec![json!({ "count": 3 })]), 3);
        assert_eq!(count_value(vec![json!(null)]), 0);
        assert_eq!(count_value(vec![]), 0);
    }

    #[test]
    fn detects_duplicate_create() {
        let err = ScheduleDbError::Query(
            "Database record `_00_schedule_run:x` already exists".into(),
        );
        assert!(err.is_already_exists());
        assert!(!ScheduleDbError::Transport("down".into()).is_already_exists());
    }

    #[test]
    fn detects_unknown_field() {
        let err = ScheduleDbError::Query(
            "Found no field `result` on table `job`, but expected one".into(),
        );
        assert!(err.is_unknown_field());
    }
}
