//! Per-view metrics are noted in memory on ingest and flushed to `_00_query`
//! on a timer, so the ingest path never writes the rows the edge transaction
//! is writing at the same moment.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::Value;
use ssp_node::ports::{Db, DbError};
use ssp_node::view_metrics::ViewMetrics;
use ssp_node::node::{flush_view_metrics, note_view_metrics};
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;
use tokio::sync::RwLock;

struct MemDb(Arc<Surreal<MemEngine>>, Arc<AtomicUsize>);

#[async_trait::async_trait]
impl Db for MemDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        if surql.contains("rowCount") {
            self.1.fetch_add(1, Ordering::SeqCst);
        }
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

async fn setup() -> (MemDb, Arc<Surreal<MemEngine>>, Arc<AtomicUsize>) {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("t").use_db("t").await.unwrap();
    let raw = Arc::new(db);
    raw.query("CREATE _00_query:v1 SET clientId = 'c', auth_id = 'user:a', rowCount = 0, updateCount = 0;")
        .await
        .unwrap();
    let writes = Arc::new(AtomicUsize::new(0));
    (MemDb(Arc::clone(&raw), Arc::clone(&writes)), raw, writes)
}

async fn row(raw: &Surreal<MemEngine>) -> Value {
    raw.query("SELECT rowCount, updateCount, materializationP90 FROM ONLY _00_query:v1")
        .await
        .unwrap()
        .take::<surrealdb::types::Value>(0)
        .unwrap()
        .into_json_value()
}

#[tokio::test]
async fn many_ingests_flush_as_one_write_and_a_quiet_flush_writes_nothing() {
    let (db, raw, writes) = setup().await;
    let metrics: ViewMetrics = RwLock::new(Default::default());
    for i in 0..100 {
        note_view_metrics(&metrics, vec![i + 1], vec!["_00_query:v1".to_string()], 3.0).await;
    }
    assert_eq!(writes.load(Ordering::SeqCst), 0, "noting never touches the DB");

    let flushed = flush_view_metrics(&db, &metrics).await;
    assert_eq!(flushed, 1);
    assert_eq!(writes.load(Ordering::SeqCst), 1, "100 ingests, one UPDATE");
    let r = row(&raw).await;
    assert_eq!(r["rowCount"], 100);
    assert_eq!(r["updateCount"], 100);
    assert_eq!(r["materializationP90"], 3.0);

    let again = flush_view_metrics(&db, &metrics).await;
    assert_eq!(again, 0, "nothing dirty, nothing written");
    assert_eq!(writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restart_increments_the_durable_counter_and_preserves_legacy_latency() {
    let (db, raw, _) = setup().await;
    raw.query("UPDATE _00_query:v1 SET updateCount = 41, materializationP99 = 700.0, lastIngestLatency = 700.0;")
        .await.unwrap().check().unwrap();
    let fresh: ViewMetrics = RwLock::new(Default::default());
    {
        let mut state = ssp_node::view_metrics::ViewMetricsState::default();
        state.row_count = 12;
        state.registration = Some(ssp_node::view_metrics::RegistrationStages { request_ms: 15.0, ..Default::default() });
        state.dirty = true;
        fresh.write().await.insert("v1".into(), state);
    }
    assert_eq!(flush_view_metrics(&db, &fresh).await, 1);
    let read = || raw.query("SELECT * FROM ONLY _00_query:v1");
    let r = read().await.unwrap().take::<surrealdb::types::Value>(0).unwrap().into_json_value();
    assert_eq!(r["updateCount"], 41);
    assert_eq!(r["materializationP99"], 700.0, "a new registration sample must not erase old ingest evidence");
    assert_eq!(r["registrationStages"]["request_ms"], 15.0);
    note_view_metrics(&fresh, vec![13], vec!["v1".into()], 2.0).await;
    assert_eq!(flush_view_metrics(&db, &fresh).await, 1);
    let r = read().await.unwrap().take::<surrealdb::types::Value>(0).unwrap().into_json_value();
    assert_eq!(r["updateCount"], 42, "fresh SSP adds to the durable lifetime counter");
    assert_eq!(r["materializationP99"], 2.0);
}

#[tokio::test]
async fn slow_evidence_is_masked_bounded_and_outlives_view_reclamation() {
    use ssp_node::view_metrics::{persist_slow_registration, slow_registration_evidence, RegistrationStages};
    let (db, raw, _) = setup().await;
    let plan: ssp::operator::OperatorPlan = serde_json::from_value(
        ssp::converter::convert_surql_to_dbsp("SELECT * FROM game WHERE owner = 'super-secret-user' AND title = $secret LIMIT 50").unwrap()
    ).unwrap();
    let shape = ssp::allowlist::Shape::of(&plan);
    for i in 0..105 {
        let evidence = slow_registration_evidence(&shape, "test-version", &[("game".into(), "owner".into())], i,
            &RegistrationStages { request_ms: 500.0 + i as f64, ..Default::default() });
        let text = evidence.to_string();
        assert!(!text.contains("super-secret-user"));
        assert!(!text.contains("$secret"));
        persist_slow_registration(&db, evidence).await.unwrap();
    }
    raw.query("DELETE _00_query;").await.unwrap().check().unwrap();
    // A new DB adapter models the next process; no evidence lives in memory.
    let restarted = MemDb(raw.clone(), Arc::new(AtomicUsize::new(0)));
    let r = restarted.query("SELECT samples FROM ONLY _00_sync_evidence:slow_operations", &[]).await.unwrap();
    let samples = r[0]["samples"].as_array().unwrap();
    assert_eq!(samples.len(), 100);
    assert_eq!(samples[0]["row_count"], 5);
    assert_eq!(samples[99]["row_count"], 104);
}

#[tokio::test]
async fn concurrent_evidence_appends_preserve_both_writers() {
    use ssp_node::view_metrics::persist_slow_registration;
    let (db, raw, _) = setup().await;
    let a = persist_slow_registration(&db, serde_json::json!({ "kind": "ingest", "at_ms": 1 }));
    let b = persist_slow_registration(&db, serde_json::json!({ "kind": "registration", "at_ms": 2 }));
    let (a, b) = tokio::join!(a, b);
    a.unwrap(); b.unwrap();
    let r = raw.query("SELECT samples FROM ONLY _00_sync_evidence:slow_operations").await.unwrap()
        .take::<surrealdb::types::Value>(0).unwrap().into_json_value();
    let samples = r["samples"].as_array().unwrap();
    assert_eq!(samples.len(), 2);
    assert!(samples.iter().any(|s| s["kind"] == "ingest"));
    assert!(samples.iter().any(|s| s["kind"] == "registration"));
}
