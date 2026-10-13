//! Regression: the TTL sweep vs SurrealDB's optimistic-concurrency write
//! conflicts.
//!
//! Production symptom (one line per contended row, dozens per sweep):
//!
//! ```text
//! [ssp] ERROR TTL cleanup: delete failed query_id=097f94… \
//!   error=query: Transaction conflict: Transaction write conflict. \
//!   This transaction can be retried
//! ```
//!
//! The sweep writes exactly the rows the ingest path writes (every
//! materialization UPDATEs `_00_query` for the views it touched, and clients
//! heartbeat the same rows), so under load its per-row DELETE loses the race
//! routinely. It used to log and give up: the row stayed expired and its
//! circuit view stayed resident until some later sweep happened to win.
//!
//! The conflict is injected here rather than raced for: the error string is
//! verbatim from production, and a real MVCC race is not schedulable from
//! outside the engine.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;
use ssp::circuit::Circuit;
use ssp_node::ports::{Db, DbError, Telemetry};
use tokio::sync::RwLock;

use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;

/// Verbatim from the SSP logs.
const CONFLICT: &str = "Transaction conflict: Transaction write conflict. This transaction can be retried";

struct MemDb(Arc<Surreal<MemEngine>>);

#[async_trait::async_trait]
impl Db for MemDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        let mut q = self.0.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| DbError::Transport(e.to_string()))?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value =
                response.take(i).map_err(|e| DbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
    async fn version(&self) -> Result<String, DbError> {
        Ok("mem".into())
    }
}

/// Fails the first `fail_first` calls whose statement contains `needle` with a
/// real SurrealDB write-conflict error, then passes everything through.
struct ConflictDb {
    inner: MemDb,
    needle: &'static str,
    fail_first: usize,
    attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Db for ConflictDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        if surql.contains(self.needle) {
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first {
                return Err(DbError::Query(CONFLICT.to_string()));
            }
        }
        self.inner.query(surql, binds).await
    }
    async fn version(&self) -> Result<String, DbError> {
        self.inner.version().await
    }
}

#[derive(Default)]
struct CountingTelemetry {
    gauge: std::sync::Mutex<i64>,
}
impl Telemetry for CountingTelemetry {
    fn counter(&self, _name: &'static str, _value: u64) {}
    fn histogram_ms(&self, _name: &'static str, _value: f64) {}
    fn gauge_add(&self, _name: &'static str, delta: i64) {
        *self.gauge.lock().unwrap() += delta;
    }
}

/// One expired `_00_query` row + the `ConflictDb` in front of it.
async fn setup(
    needle: &'static str,
    fail_first: usize,
) -> (ConflictDb, Arc<Surreal<MemEngine>>, Arc<AtomicUsize>) {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("t").use_db("t").await.unwrap();
    let raw = Arc::new(db);
    raw.query(
        "CREATE _00_query:expired SET auth_id = '', clientId = 'c', ttl = 1s, \
         lastActiveAt = time::now() - 1h, surql = 'SELECT * FROM game', params = {};",
    )
    .await
    .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let cdb = ConflictDb {
        inner: MemDb(Arc::clone(&raw)),
        needle,
        fail_first,
        attempts: Arc::clone(&attempts),
    };
    (cdb, raw, attempts)
}

async fn remaining_queries(raw: &Surreal<MemEngine>) -> usize {
    let rows: Value = raw
        .query("SELECT VALUE id FROM _00_query")
        .await
        .unwrap()
        .take::<surrealdb::types::Value>(0)
        .unwrap()
        .into_json_value();
    rows.as_array().map(|a| a.len()).unwrap_or(0)
}

async fn subscriber_count(raw: &Surreal<MemEngine>, id: &str) -> Option<usize> {
    let v: Value = raw
        .query(format!("SELECT VALUE subscribers FROM {id}"))
        .await
        .unwrap()
        .take::<surrealdb::types::Value>(0)
        .unwrap()
        .into_json_value();
    v.as_array()
        .and_then(|a| a.first())
        .map(|subs| subs.as_array().map(|a| a.len()).unwrap_or(0))
}

async fn sweep(db: &dyn Db) -> usize {
    let processor = Arc::new(RwLock::new(Circuit::new()));
    let telemetry = CountingTelemetry::default();
    ssp_node::ttl_cleanup_sweep(db, &processor, &telemetry, ssp_protocol::RefMode::Single).await
}

#[tokio::test]
async fn delete_retries_through_a_write_conflict() {
    // Two conflicts then success — exactly what a contended row looks like.
    let (db, raw, attempts) = setup("RETURN array::len($d)", 2).await;

    let count = sweep(&db).await;

    assert_eq!(count, 1, "one expired view seen");
    assert_eq!(
        remaining_queries(&raw).await,
        0,
        "expired row must be deleted: a write conflict is retryable, not terminal"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3, "2 conflicts + 1 successful retry");
}

#[tokio::test]
async fn delete_retry_budget_is_bounded() {
    // A conflict that never clears must not spin: bail after the budget and
    // leave the row for the next sweep.
    let (db, raw, attempts) = setup("RETURN array::len($d)", usize::MAX).await;

    let count = sweep(&db).await;

    assert_eq!(count, 1);
    assert_eq!(remaining_queries(&raw).await, 1, "row survives, retried next sweep");
    assert_eq!(attempts.load(Ordering::SeqCst), 6, "1 try + 5 retries, then give up");
}

#[tokio::test]
async fn edge_delete_retries_through_a_write_conflict() {
    let (db, raw, attempts) = setup("$qid)->_00_list_ref", 2).await;

    let count = sweep(&db).await;

    assert_eq!(count, 1);
    assert_eq!(remaining_queries(&raw).await, 0, "the row itself still goes");
    assert!(
        attempts.load(Ordering::SeqCst) >= 3,
        "edge delete retried past its conflicts, got {}",
        attempts.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn subscriber_prune_retries_and_only_touches_stale_rows() {
    let (db, raw, attempts) = setup("SET subscribers", 2).await;
    // One row with a stale subscriber, one with a fresh one.
    raw.query(
        "CREATE _00_query:stale SET auth_id = '', clientId = 'c', ttl = 10m, \
             lastActiveAt = time::now(), surql = 'x', params = {}, \
             subscribers = [{ sid: 'dead', seenAt: time::now() - 1h }]; \
         CREATE _00_query:fresh SET auth_id = '', clientId = 'c', ttl = 10m, \
             lastActiveAt = time::now(), surql = 'x', params = {}, \
             subscribers = [{ sid: 'live', seenAt: time::now() }];",
    )
    .await
    .unwrap();

    sweep(&db).await;

    assert_eq!(attempts.load(Ordering::SeqCst), 3, "2 conflicts + 1 successful retry");
    assert_eq!(
        subscriber_count(&raw, "_00_query:stale").await,
        Some(0),
        "stale subscriber pruned"
    );
    assert_eq!(
        subscriber_count(&raw, "_00_query:fresh").await,
        Some(1),
        "live subscriber kept"
    );
}

/// Second bug found while reproducing the first: the prune statement itself
/// died on any `_00_query` row that carries no `subscribers` field, because
/// SurrealDB 3 evaluates the SET expression on rows the WHERE excludes:
///
/// ```text
/// TTL sweep: pruning stale subscribers failed: query: Incorrect arguments for
/// function array::filter(). Argument 1 was the wrong type. Expected `array`
/// but found `NONE`
/// ```
///
/// One such row (the expired one seeded here has none) disabled pruning for the
/// entire table, on every sweep.
#[tokio::test]
async fn subscriber_prune_survives_rows_without_a_subscribers_field() {
    // No injected conflicts: this one is pure SQL.
    let (db, raw, _attempts) = setup("never-matches", 0).await;
    raw.query(
        "CREATE _00_query:stale SET auth_id = '', clientId = 'c', ttl = 10m, \
             lastActiveAt = time::now(), surql = 'x', params = {}, \
             subscribers = [{ sid: 'dead', seenAt: time::now() - 1h }];",
    )
    .await
    .unwrap();

    sweep(&db).await;

    assert_eq!(
        subscriber_count(&raw, "_00_query:stale").await,
        Some(0),
        "a field-less row must not abort the prune for every other row"
    );
}

/// Stands in for the row's `_00_dbsp_cleanup` event: deleting a `_00_query`
/// row calls the node's `/view/unregister`, which takes the publication gate.
/// While someone else holds the gate the call times out and the DELETE fails,
/// with the error SurrealDB reports for the event.
struct EventCallbackDb {
    inner: MemDb,
    gate: Arc<tokio::sync::Mutex<()>>,
}

#[async_trait::async_trait]
impl Db for EventCallbackDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        if surql.contains("DELETE type::record('_00_query'") && self.gate.try_lock().is_err() {
            return Err(DbError::Query(
                "Error while processing event _00_dbsp_cleanup: There was an error processing a remote HTTP request"
                    .to_string(),
            ));
        }
        self.inner.query(surql, binds).await
    }
    async fn version(&self) -> Result<String, DbError> {
        self.inner.version().await
    }
}

/// The sweep must not hold the publication gate across the DELETE: the
/// DELETE's own event needs it. Holding it made every expired row fail, and
/// starved edge publication while the sweep ran (new views stuck
/// `materializing`).
#[tokio::test]
async fn sweep_does_not_hold_the_gate_across_the_delete() {
    let (_cdb, raw, _) = setup("never-matches", 0).await;
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let db = EventCallbackDb { inner: MemDb(Arc::clone(&raw)), gate: Arc::clone(&gate) };
    let processor = Arc::new(RwLock::new(Circuit::new()));
    let telemetry = CountingTelemetry::default();
    let swept =
        ssp_node::ttl_cleanup_sweep_gated(&db, &processor, &telemetry, ssp_protocol::RefMode::Single, Some(&gate)).await;
    assert_eq!(swept, 1);
    assert_eq!(remaining_queries(&raw).await, 0, "the expired row was deleted");
}
