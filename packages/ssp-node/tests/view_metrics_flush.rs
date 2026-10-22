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

/// Replays a request that "lost" a write conflict after it had landed, the
/// way SurrealDB can fail one statement of many while the rest commit.
struct ConflictOnceDb(MemDb, std::sync::atomic::AtomicBool);

#[async_trait::async_trait]
impl Db for ConflictOnceDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        let out = self.0.query(surql, binds).await?;
        if surql.contains("metricsFlush") && !self.1.swap(true, Ordering::SeqCst) {
            return Err(DbError::Query(
                "Transaction conflict: Resource busy: . This transaction can be retried".into(),
            ));
        }
        Ok(out)
    }
    async fn version(&self) -> Result<String, DbError> {
        Ok("mem".into())
    }
}

async fn views(raw: &Surreal<MemEngine>, n: usize) -> ViewMetrics {
    let metrics: ViewMetrics = RwLock::new(Default::default());
    for i in 0..n {
        raw.query(format!("CREATE _00_query:w{i} SET rowCount = 0, updateCount = 0;")).await.unwrap().check().unwrap();
        for _ in 0..3 {
            note_view_metrics(&metrics, vec![i], vec![format!("w{i}")], 4.0).await;
        }
    }
    metrics
}

async fn update_counts(raw: &Surreal<MemEngine>) -> Vec<i64> {
    let v = raw
        .query("SELECT VALUE updateCount FROM _00_query WHERE id != _00_query:v1 ORDER BY id")
        .await
        .unwrap()
        .take::<surrealdb::types::Value>(0)
        .unwrap()
        .into_json_value();
    v.as_array().unwrap().iter().map(|c| c.as_i64().unwrap()).collect()
}

// Cloudflare 2026-10-22: one UPDATE per dirty view made a flush of ~300 views
// 300 sequential round trips, 2.5 to 7 s per alarm.
#[tokio::test]
async fn a_flush_of_many_views_writes_them_in_batches() {
    let (db, raw, writes) = setup().await;
    let n = 2 * ssp_node::node::VIEW_METRICS_FLUSH_BATCH + 7;
    let metrics = views(&raw, n).await;
    assert_eq!(flush_view_metrics(&db, &metrics).await, n);
    assert_eq!(writes.load(Ordering::SeqCst), 3, "{n} views, three requests");
    let counts = update_counts(&raw).await;
    assert_eq!(counts.len(), n);
    assert!(counts.iter().all(|c| *c == 3), "every view counts its three ingests: {counts:?}");
    let rows = raw.query("SELECT VALUE rowCount FROM ONLY _00_query:w5").await.unwrap()
        .take::<surrealdb::types::Value>(0).unwrap().into_json_value();
    assert_eq!(rows, 5);
}

// A batch sent again after a write conflict re-runs the statements that had
// already landed; their ingests must still count once.
#[tokio::test]
async fn a_retried_batch_counts_each_ingest_once() {
    let (db, raw, _) = setup().await;
    let metrics = views(&raw, 10).await;
    let flaky = ConflictOnceDb(db, std::sync::atomic::AtomicBool::new(false));
    assert_eq!(flush_view_metrics(&flaky, &metrics).await, 10);
    assert_eq!(update_counts(&raw).await, vec![3; 10]);

    // The next flush is a new one: its increments land on top.
    for i in 0..10 {
        note_view_metrics(&metrics, vec![i], vec![format!("w{i}")], 4.0).await;
    }
    assert_eq!(flush_view_metrics(&flaky, &metrics).await, 10);
    assert_eq!(update_counts(&raw).await, vec![4; 10]);
}

/// Refuses every request that writes `bad`'s row, as a row whose statement
/// keeps failing would.
struct OneBadRowDb(MemDb, &'static str);

#[async_trait::async_trait]
impl Db for OneBadRowDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        if binds.iter().any(|(_, v)| v.as_str() == Some(self.1)) {
            return Err(DbError::Query("Found NONE for field `rowCount`".into()));
        }
        self.0.query(surql, binds).await
    }
    async fn version(&self) -> Result<String, DbError> {
        Ok("mem".into())
    }
}

#[tokio::test]
async fn a_failing_row_stays_dirty_without_holding_back_its_batch() {
    let (db, raw, _) = setup().await;
    let metrics = views(&raw, 6).await;
    let db = OneBadRowDb(db, "_00_query:w2");
    assert_eq!(flush_view_metrics(&db, &metrics).await, 5);
    assert_eq!(update_counts(&raw).await, vec![3, 3, 0, 3, 3, 3]);
    let map = metrics.read().await;
    assert!(map["w2"].dirty, "kept for the next flush");
    assert_eq!(map["w2"].update_count, 3, "with its ingests");
    assert!(!map["w1"].dirty);
}

/// Never answers: every request times out, as the shared staging DB did for
/// two minutes on 2026-10-22.
struct SilentDb(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl Db for SilentDb {
    async fn query(&self, _surql: &str, _binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(DbError::Transport("timeout after 30s".into()))
    }
    async fn version(&self) -> Result<String, DbError> {
        Ok("silent".into())
    }
}

// Retrying each view of a batch after a 30 s timeout held the Cloudflare
// alarm, and every timer behind it, for minutes while the DB stalled.
#[tokio::test]
async fn a_database_that_does_not_answer_gets_one_request_and_every_view_stays_dirty() {
    let (_, raw, _) = setup().await;
    let n = 2 * ssp_node::node::VIEW_METRICS_FLUSH_BATCH + 7;
    let metrics = views(&raw, n).await;
    let calls = Arc::new(AtomicUsize::new(0));
    assert_eq!(flush_view_metrics(&SilentDb(Arc::clone(&calls)), &metrics).await, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no per-view retries against a DB that does not answer");
    let map = metrics.read().await;
    assert!(map.values().all(|st| st.dirty && st.update_count == 3), "all kept, with their counts");
}

/// Refuses one row's statement, then stops answering.
struct BadRowThenSilentDb(MemDb, &'static str, std::sync::atomic::AtomicBool);

#[async_trait::async_trait]
impl Db for BadRowThenSilentDb {
    async fn query(&self, surql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        if self.2.load(Ordering::SeqCst) {
            return Err(DbError::Transport("timeout after 30s".into()));
        }
        if binds.iter().any(|(_, v)| v.as_str() == Some(self.1)) {
            if binds.len() < 20 {
                // The bad row on its own: from now on the DB is gone.
                self.2.store(true, Ordering::SeqCst);
            }
            return Err(DbError::Query("Found NONE for field `rowCount`".into()));
        }
        self.0.query(surql, binds).await
    }
    async fn version(&self) -> Result<String, DbError> {
        Ok("mem".into())
    }
}

#[tokio::test]
async fn a_database_that_stops_answering_during_the_per_view_retry_ends_the_flush() {
    let (db, raw, _) = setup().await;
    let n = ssp_node::node::VIEW_METRICS_FLUSH_BATCH + 5;
    let metrics = views(&raw, n).await;
    let db = BadRowThenSilentDb(db, "_00_query:w2", std::sync::atomic::AtomicBool::new(false));
    let written = flush_view_metrics(&db, &metrics).await;
    let map = metrics.read().await;
    let dirty = map.values().filter(|st| st.dirty).count();
    assert_eq!(written + dirty, n, "every view either written or kept: {written} + {dirty}");
    assert!(map["w2"].dirty && map["w2"].update_count == 3);
}
