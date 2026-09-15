//! Regression: an edge transaction that fails must not lose its deltas.
//!
//! Production (SSP log, one line per lost batch):
//!
//! ```text
//! ERROR ssp::edges: Edge update transaction failed - data may be out of sync \
//!   error=query: The query was not executed due to a failed transaction operations=7
//! ```
//!
//! 26 ms after `Received ingest: CREATE message:…`. The clients subscribed to
//! those views never received the row until a reload rebuilt the view. The
//! conflict is injected (the error strings are verbatim) rather than raced for.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;
use ssp::circuit::{Circuit, ViewDelta};
use ssp_node::db_retry::CONFLICT_RETRIES;
use ssp_node::edges::write_deltas_resilient;
use ssp_node::ports::{Db, DbError, NoopTelemetry};
use ssp_protocol::RefMode;
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;

const CONFLICT: &str = "Transaction conflict: Transaction write conflict. This transaction can be retried";
const FAILED_TX: &str = "The query was not executed due to a failed transaction";

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

/// Fails a query when `fail_if(surql)` says so, with `message`, for the first
/// `fail_first` matching calls (`usize::MAX` = forever).
struct FailingDb {
    inner: MemDb,
    fail_if: Box<dyn Fn(&str) -> bool + Send + Sync>,
    message: &'static str,
    fail_first: usize,
    attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Db for FailingDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        if (self.fail_if)(surql) {
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first {
                return Err(DbError::Query(self.message.to_string()));
            }
        }
        self.inner.query(surql, binds).await
    }
    async fn version(&self) -> Result<String, DbError> {
        self.inner.version().await
    }
}

async fn mem() -> Arc<Surreal<MemEngine>> {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("t").use_db("t").await.unwrap();
    Arc::new(db)
}

async fn seed(raw: &Surreal<MemEngine>, key: &str, target: &str) {
    raw.query("CREATE type::record('_00_query', $k) SET clientId = 'c1', auth_id = 'user:a';")
        .bind(("k", key.to_string()))
        .await
        .unwrap();
    raw.query(format!("CREATE {target} SET n = 1;")).await.unwrap();
}

async fn edges_to(raw: &Surreal<MemEngine>, target: &str) -> usize {
    match raw
        .query(format!("SELECT VALUE type::string(id) FROM _00_list_ref WHERE out = {target}"))
        .await
    {
        Ok(mut resp) => resp.take::<Vec<String>>(0).map(|v| v.len()).unwrap_or(0),
        Err(_) => 0,
    }
}

fn delta(query_id: &str, add: &str) -> ViewDelta {
    ViewDelta {
        query_id: query_id.to_string(),
        additions: vec![add.to_string()],
        removals: vec![],
        updates: vec![],
        row_count: 0,
        result_hash: String::new(),
        subquery_items: vec![],
        auth_id: "user:a".to_string(),
        initial: false,
    }
}

fn failing(
    raw: &Arc<Surreal<MemEngine>>,
    needle: &'static str,
    message: &'static str,
    fail_first: usize,
) -> (FailingDb, Arc<AtomicUsize>) {
    let attempts = Arc::new(AtomicUsize::new(0));
    (
        FailingDb {
            inner: MemDb(Arc::clone(raw)),
            fail_if: Box::new(move |q| q.contains(needle)),
            message,
            fail_first,
            attempts: Arc::clone(&attempts),
        },
        attempts,
    )
}

#[tokio::test]
async fn edge_tx_retries_through_write_conflict() {
    let raw = mem().await;
    seed(&raw, "abc", "user:x").await;
    let (db, attempts) = failing(&raw, "RELATE", CONFLICT, 2);
    let circuit = Circuit::new();

    let left = write_deltas_resilient(&db, vec![delta("view:abc", "user:x")], &circuit, RefMode::Single, &NoopTelemetry).await;

    assert!(left.is_empty(), "nothing left over");
    assert_eq!(edges_to(&raw, "user:x").await, 1, "the edge landed after the conflicts cleared");
    assert_eq!(attempts.load(Ordering::SeqCst), 3, "2 conflicts + 1 success");
}

#[tokio::test]
async fn failed_transaction_message_is_retryable() {
    let raw = mem().await;
    seed(&raw, "abc", "user:x").await;
    let (db, attempts) = failing(&raw, "RELATE", FAILED_TX, 1);
    let circuit = Circuit::new();

    let left = write_deltas_resilient(&db, vec![delta("view:abc", "user:x")], &circuit, RefMode::Single, &NoopTelemetry).await;

    assert!(left.is_empty());
    assert_eq!(edges_to(&raw, "user:x").await, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn poisoned_delta_is_isolated_by_splitting() {
    let raw = mem().await;
    seed(&raw, "a", "user:x").await;
    seed(&raw, "b", "user:poison").await;
    // Any transaction mentioning the poisoned target fails forever; once the
    // batch is split, the healthy half no longer mentions it.
    let (db, _attempts) = failing(&raw, "user:poison", CONFLICT, usize::MAX);
    let circuit = Circuit::new();

    let left = write_deltas_resilient(
        &db,
        vec![delta("view:a", "user:x"), delta("view:b", "user:poison")],
        &circuit,
        RefMode::Single,
        &NoopTelemetry,
    )
    .await;

    assert_eq!(edges_to(&raw, "user:x").await, 1, "the healthy delta was written on its own");
    assert_eq!(edges_to(&raw, "user:poison").await, 0);
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].query_id, "view:b", "the poisoned delta is handed back, not dropped");
}

#[tokio::test]
async fn single_bad_delta_returns_itself_without_spinning() {
    let raw = mem().await;
    seed(&raw, "abc", "user:x").await;
    let (db, attempts) = failing(&raw, "RELATE", CONFLICT, usize::MAX);
    let circuit = Circuit::new();

    let left = write_deltas_resilient(&db, vec![delta("view:abc", "user:x")], &circuit, RefMode::Single, &NoopTelemetry).await;

    assert_eq!(left.len(), 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 1 + CONFLICT_RETRIES, "bounded: one try plus the retry budget");
}
