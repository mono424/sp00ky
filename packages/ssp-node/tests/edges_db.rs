//! Validate the ported edge-write path against a REAL embedded SurrealDB via
//! the `Db` port. The key risk in the port was swapping the `$fromN` RecordId
//! bind for `type::thing('_00_query', $fromN)` string binds — this test proves
//! `RELATE`/`UPDATE`/`DELETE` with that form actually create/read/remove
//! `_00_list_ref` rows, i.e. the migration preserved edge-write semantics.

use std::sync::Arc;

use serde_json::Value;
use ssp::circuit::{Circuit, SubqueryDeltaItem, SubqueryOp, ViewDelta};
use ssp_node::edges::{run_edge_writes, EdgeSink, SurrealEdgeSink};
use ssp_node::ports::{Db, DbError, NoopTelemetry};
use ssp_protocol::RefMode;
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;
use tokio::sync::RwLock;

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

async fn mem() -> Arc<Surreal<MemEngine>> {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("t").use_db("t").await.unwrap();
    Arc::new(db)
}

/// Seed the incantation `_00_query:<key>` row + the target record so RELATE has
/// real endpoints, and define `_00_list_ref` as a normal edge table.
async fn seed(raw: &Surreal<MemEngine>, incantation_key: &str, target: &str) {
    // Create the incantation via type::record + bind so arbitrary (non-ident)
    // keys are created correctly — matching how the edge writer references it.
    raw.query("CREATE type::record('_00_query', $k) SET clientId = 'c1', auth_id = 'user:a';")
        .bind(("k", incantation_key.to_string()))
        .await
        .unwrap();
    raw.query(format!("CREATE {target} SET n = 1;")).await.unwrap();
}

async fn edge_count(raw: &Surreal<MemEngine>) -> usize {
    // Count rows in Rust. Tolerate the table not existing yet (SurrealDB v3
    // errors on SELECT from an undefined table) — that just means zero edges.
    match raw.query("SELECT VALUE type::string(id) FROM _00_list_ref").await {
        Ok(mut resp) => resp.take::<Vec<String>>(0).map(|v| v.len()).unwrap_or(0),
        Err(_) => 0,
    }
}

fn delta(query_id: &str, additions: Vec<&str>, removals: Vec<&str>) -> ViewDelta {
    ViewDelta {
        query_id: query_id.to_string(),
        additions: additions.into_iter().map(String::from).collect(),
        removals: removals.into_iter().map(String::from).collect(),
        updates: vec![],
        records: vec![],
        result_hash: String::new(),
        subquery_items: vec![],
        auth_id: "user:a".to_string(),
        initial: false,
    }
}

#[tokio::test]
async fn relate_then_delete_roundtrip_through_type_thing_bind() {
    let raw = mem().await;
    seed(&raw, "abc", "user:x").await;
    let db = MemDb(Arc::clone(&raw));
    let circuit = Circuit::new();

    // ADD: RELATE type::thing('_00_query',$from0)->_00_list_ref->user:x
    let d = delta("view:abc", vec!["user:x"], vec![]);
    run_edge_writes(&db, &[&d], &circuit, RefMode::Single, &NoopTelemetry).await;
    assert_eq!(edge_count(&raw).await, 1, "RELATE created one _00_list_ref edge");

    // Verify the edge actually links the incantation → target (the bind resolved).
    let from: Option<String> = raw
        .query("SELECT VALUE type::string(in) FROM _00_list_ref LIMIT 1")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(from.as_deref(), Some("_00_query:abc"), "edge.in is the incantation");

    // REMOVE: DELETE type::thing('_00_query',$from0)->_00_list_ref WHERE out = user:x
    let d = delta("view:abc", vec![], vec!["user:x"]);
    run_edge_writes(&db, &[&d], &circuit, RefMode::Single, &NoopTelemetry).await;
    assert_eq!(edge_count(&raw).await, 0, "DELETE removed the edge");
}

#[tokio::test]
async fn special_char_incantation_key_survives_bind() {
    // A key that would be unsafe to interpolate raw — proves the type::thing
    // string bind (not literal interpolation) is doing its job.
    let raw = mem().await;
    let key = "weird-key.with:stuff"; // note: query_id tail after last ':'
    // format_incantation_id takes the tail after the LAST ':', so craft a
    // query_id whose tail is a hyphen/dot key.
    let query_id = "view:weird-key.with_stuff";
    seed(&raw, "weird-key.with_stuff", "user:y").await;
    let _ = key;

    let db = MemDb(Arc::clone(&raw));
    let circuit = Circuit::new();
    let d = delta(query_id, vec!["user:y"], vec![]);
    run_edge_writes(&db, &[&d], &circuit, RefMode::Single, &NoopTelemetry).await;
    assert_eq!(edge_count(&raw).await, 1, "edge created despite non-ident key");
}

/// Reproduce the "related data (author/comments) never loads" bug: a `.related()`
/// subquery child edge must be written with a NON-NONE `parent`, because the
/// client pulls subquery children with `WHERE parent IS NOT NONE`
/// (buildSubqueryListRefSelect). Main record `thread:t` and its child `user:u`
/// (alias `author`) go in ONE delta → ONE transaction, so the parent lookup can
/// see the main edge written just before it.
#[tokio::test]
async fn subquery_child_edge_gets_non_none_parent() {
    let raw = mem().await;
    seed(&raw, "q1", "thread:t").await;
    raw.query("CREATE user:u SET n = 1;").await.unwrap();
    let db = MemDb(Arc::clone(&raw));
    let circuit = Circuit::new();

    let d = ViewDelta {
        query_id: "view:q1".to_string(),
        additions: vec!["thread:t".to_string()],
        removals: vec![],
        updates: vec![],
        records: vec![],
        result_hash: String::new(),
        subquery_items: vec![SubqueryDeltaItem {
            id: "user:u".to_string(),
            parent_key: "thread:t".to_string(),
            alias: "author".to_string(),
            op: SubqueryOp::Add,
        }],
        auth_id: "user:a".to_string(),
        initial: false,
    };
    run_edge_writes(&db, &[&d], &circuit, RefMode::Single, &NoopTelemetry).await;

    // Two edges: the main thread:t edge + the author user:u edge.
    assert_eq!(edge_count(&raw).await, 2, "main + subquery edge both written");

    // The subquery child edge must have parent IS NOT NONE, or the client's
    // `WHERE parent IS NOT NONE` select drops it and the author never syncs.
    let non_none: Option<i64> = raw
        .query("SELECT VALUE count() FROM _00_list_ref WHERE parent IS NOT NONE GROUP ALL")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(
        non_none,
        Some(1),
        "the author subquery edge must carry a non-NONE parent (client filters on it)"
    );
}

#[tokio::test]
async fn edge_sink_flush_writes_through_port() {
    let raw = mem().await;
    seed(&raw, "sink", "user:z").await;
    let mut circuit = Circuit::new();
    circuit.add_query(ssp::operator::plan::QueryPlan { id: "sink".into(), root: ssp::operator::plan::OperatorPlan::Scan { table: "user".into() } }, None, None);
    let sink = SurrealEdgeSink {
        publication_gate: Arc::new(tokio::sync::Mutex::new(())),
        db: Arc::new(MemDb(Arc::clone(&raw))),
        processor: Arc::new(RwLock::new(circuit)),
        telemetry: Arc::new(NoopTelemetry),
        mode: RefMode::Single,
    };
    sink.flush(vec![delta("view:sink", vec!["user:z"], vec![])]).await;
    assert_eq!(edge_count(&raw).await, 1, "SurrealEdgeSink wrote through the Db port");
}

async fn edge_rows(raw: &Surreal<MemEngine>) -> Vec<Value> {
    let mut response = raw.query(
        "SELECT type::string(id) AS edge_id, type::string(in) AS owner, type::string(out) AS target, version, type::string(parent) AS parent, parent_rel FROM _00_list_ref ORDER BY owner, target",
    ).await.unwrap();
    let rows: surrealdb::types::Value = response.take(0).unwrap();
    rows.into_json_value().as_array().unwrap().clone()
}

#[tokio::test]
async fn graph_indexed_updates_preserve_child_links_and_other_view_versions() {
    use serde_json::json;
    use ssp::circuit::store::{Change, ChangeSet, Record};

    let raw = mem().await;
    seed(&raw, "changed", "thread:main").await;
    raw.query("CREATE _00_query:other SET clientId = 'c2', auth_id = 'user:a'; CREATE user:author; CREATE thread:untouched;")
        .await.unwrap().check().unwrap();
    let db = MemDb(raw.clone());
    let mut circuit = Circuit::new();
    circuit.load(vec![
        Record::new("thread", "thread:main", json!({"_00_rv": 11})),
        Record::new("thread", "thread:untouched", json!({"_00_rv": 13})),
        Record::new("user", "user:author", json!({"_00_rv": 17})),
    ]);
    let mut initial = delta("view:changed", vec!["thread:main", "thread:untouched"], vec![]);
    initial.subquery_items.push(SubqueryDeltaItem {
        id: "user:author".into(), parent_key: "thread:main".into(),
        alias: "author".into(), op: SubqueryOp::Add,
    });
    let mut other = initial.clone();
    other.query_id = "view:other".into();
    run_edge_writes(&db, &[&initial, &other], &circuit, RefMode::Single, &NoopTelemetry).await;
    let before = edge_rows(&raw).await;
    assert_eq!(before.len(), 6);

    circuit.step(ChangeSet { changes: vec![
        Change::update("thread", "thread:main", json!({"_00_rv": 101, "changed": true})),
        Change::update("user", "user:author", json!({"_00_rv": 107, "changed": true})),
    ] });
    let mut update = delta("view:changed", vec![], vec![]);
    update.updates.push("thread:main".into());
    update.subquery_items.push(SubqueryDeltaItem {
        id: "user:author".into(), parent_key: "thread:main".into(),
        alias: "author".into(), op: SubqueryOp::Update,
    });
    run_edge_writes(&db, &[&update], &circuit, RefMode::Single, &NoopTelemetry).await;
    let after = edge_rows(&raw).await;
    assert_eq!(after.len(), before.len());
    for (old, new) in before.iter().zip(&after) {
        assert_eq!(new["edge_id"], old["edge_id"], "updates retain edge identity");
        assert_eq!(new["parent"], old["parent"], "child ownership survives updates");
        assert_eq!(new["parent_rel"], old["parent_rel"]);
        let expected = match (new["owner"].as_str().unwrap(), new["target"].as_str().unwrap()) {
            ("_00_query:changed", "thread:main") => json!(101),
            ("_00_query:changed", "user:author") => json!(107),
            _ => old["version"].clone(),
        };
        assert_eq!(new["version"], expected, "only the selected view and target receive captured versions");
    }
    let child = after.iter().find(|row| row["owner"] == "_00_query:changed" && row["target"] == "user:author").unwrap();
    assert!(child["parent"].as_str().unwrap().starts_with("_00_list_ref:"));
    run_edge_writes(&db, &[&update], &circuit, RefMode::Single, &NoopTelemetry).await;
    assert_eq!(edge_rows(&raw).await, after, "replayed updates create no duplicate edges");
}

struct RecordingMemDb {
    inner: MemDb,
    queries: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl Db for RecordingMemDb {
    async fn query(&self, sql: &str, binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
        self.queries.lock().unwrap().push(sql.to_string());
        self.inner.query(sql, binds).await
    }
    async fn version(&self) -> Result<String, DbError> { self.inner.version().await }
}

#[tokio::test]
async fn chunked_version_only_publish_and_replay_update_every_edge_once() {
    use serde_json::json;
    use ssp::circuit::store::{Change, ChangeSet, Record};
    use ssp_node::edges::MAX_TX_STATEMENTS;

    let raw = mem().await;
    seed(&raw, "large", "user:author").await;
    let targets: Vec<String> = (0..501).map(|i| format!("thread:r{i}")).collect();
    let creates = targets.iter().map(|id| format!("CREATE {id};")).collect::<String>();
    raw.query(creates).await.unwrap().check().unwrap();
    let db = RecordingMemDb { inner: MemDb(raw.clone()), queries: Default::default() };
    let mut circuit = Circuit::new();
    circuit.load(targets.iter().map(|id| Record::new("thread", id, json!({"_00_rv": 1})))
        .chain(std::iter::once(Record::new("user", "user:author", json!({"_00_rv": 2})))));
    let mut initial = delta("view:large", targets.iter().map(String::as_str).collect(), vec![]);
    initial.initial = true;
    initial.subquery_items.push(SubqueryDeltaItem {
        id: "user:author".into(), parent_key: targets[0].clone(), alias: "author".into(), op: SubqueryOp::Add,
    });
    run_edge_writes(&db, &[&initial], &circuit, RefMode::Single, &NoopTelemetry).await;
    let before = edge_rows(&raw).await;
    assert_eq!(before.len(), 502);

    circuit.step(ChangeSet { changes: targets.iter().map(|id| Change::update("thread", id, json!({"_00_rv": 9001, "changed": true})))
        .chain(std::iter::once(Change::update("user", "user:author", json!({"_00_rv": 9002, "changed": true})))).collect() });
    let mut update = delta("view:large", vec![], vec![]);
    update.updates = targets;
    update.subquery_items.push(SubqueryDeltaItem {
        id: "user:author".into(), parent_key: "thread:r0".into(), alias: "author".into(), op: SubqueryOp::Update,
    });
    db.queries.lock().unwrap().clear();
    run_edge_writes(&db, &[&update], &circuit, RefMode::Single, &NoopTelemetry).await;
    {
        let queries = db.queries.lock().unwrap();
        assert_eq!(queries.len(), 2, "version-only delta crosses the transaction cap");
        assert_eq!(queries.iter().map(|sql| sql.matches("UPDATE (").count()).sum::<usize>(), 502);
        for sql in queries.iter() {
            let body_statements = sql.split(';').map(str::trim)
                .filter(|s| !s.is_empty() && !s.starts_with("BEGIN") && !s.starts_with("COMMIT")).count();
            assert!(body_statements <= MAX_TX_STATEMENTS, "transaction has {body_statements} statements");
        }
    }
    let after = edge_rows(&raw).await;
    assert_eq!(after.len(), 502);
    for (old, new) in before.iter().zip(&after) {
        assert_eq!(new["edge_id"], old["edge_id"]);
        assert_eq!(new["parent"], old["parent"]);
        assert_eq!(new["version"], if new["target"] == "user:author" { json!(9002) } else { json!(9001) });
    }
    run_edge_writes(&db, &[&update], &circuit, RefMode::Single, &NoopTelemetry).await;
    assert_eq!(edge_rows(&raw).await, after, "replaying every committed chunk is idempotent");
}
