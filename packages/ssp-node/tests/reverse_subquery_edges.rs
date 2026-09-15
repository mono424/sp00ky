//! Reproduce the production "comments vanish" bug at the NODE edge-write level.
//!
//! Drives the real register pipeline — convert surql → `add_query_with_auth`
//! (initial snapshot) → `run_edge_writes` — against an embedded SurrealDB via
//! the `Db` port (same harness as `edges_db.rs`), then asserts that the reverse
//! one-to-many `comments` subquery produces a `_00_list_ref` edge whose `parent`
//! is NON-NONE. The client pulls subquery children with `WHERE parent IS NOT
//! NONE`, so a missing edge — or one with `parent = NONE` — makes comments
//! disappear even though authors (forward links) work.

use std::sync::Arc;

use serde_json::{json, Value};
use ssp::circuit::{Change, ChangeSet, Circuit, OutputFormat, Record, ViewDelta};
use ssp::converter;
use ssp::operator::plan::{OperatorPlan, QueryPlan};
use ssp_node::edges::run_edge_writes;
use ssp_node::ports::{Db, DbError, NoopTelemetry};
use ssp_protocol::RefMode;
use surrealdb::engine::local::{Db as MemEngine, Mem};
use surrealdb::Surreal;

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

/// The real thread-detail query: forward `author`, windowed reverse `comments`
/// with a nested `author`, and a `jobs` subquery.
const DETAIL_SQL: &str = "SELECT *, \
    (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author, \
    (SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author \
     FROM comment WHERE thread=$parent.id ORDER BY created_at desc LIMIT 10) AS comments, \
    (SELECT * FROM job WHERE assigned_to=$parent.id ORDER BY created_at desc LIMIT 1) AS jobs \
    FROM thread";

/// The EXACT surql the app's query builder renders for ThreadDetail (captured
/// via packages/query-builder repro). Unlike DETAIL_SQL it filters the parent
/// thread with `WHERE id = $id` (a bound param) — the real prod path. If the
/// parameterised parent filter changes how the reverse `comments` subquery is
/// planned/emitted, the comments edge goes missing exactly as in production.
const DETAIL_SQL_CLIENT: &str = "SELECT *, \
    (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author, \
    (SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author \
     FROM comment WHERE thread=$parent.id ORDER BY created_at desc LIMIT 10) AS comments, \
    (SELECT * FROM job WHERE assigned_to=$parent.id AND path = \"/spookify\" ORDER BY created_at desc LIMIT 1) AS jobs \
    FROM thread WHERE id = $id";

// Faithful prod reproduction: the client's real ThreadDetail surql, with the
// parent thread pinned by `WHERE id = $id` and $id bound. Assert the reverse
// `comments` subquery still writes a non-NONE-parent edge. If this fails while
// register_writes_comments_edge_with_parent (no parent filter) passes, the bug
// is the parameterised parent filter dropping the reverse subquery.
#[tokio::test]
async fn client_detail_query_with_parent_filter_writes_comments_edge() {
    let raw = mem().await;
    raw.query("CREATE type::record('_00_query', 'detail') SET clientId = 'c1', auth_id = 'user:a';")
        .await
        .unwrap();
    let db = MemDb(Arc::clone(&raw));

    let mut circuit = Circuit::new();
    circuit.load(vec![
        Record::new("user", "user:u", json!({ "username": "alice" })),
        Record::new("thread", "thread:t", json!({ "title": "Hello", "author": "user:u" })),
        Record::new(
            "comment",
            "comment:c",
            json!({ "text": "hi", "thread": "thread:t", "author": "user:u", "created_at": "2026-01-01T00:00:00Z" }),
        ),
    ]);

    let root: OperatorPlan =
        serde_json::from_value(converter::convert_surql_to_dbsp(DETAIL_SQL_CLIENT).unwrap()).unwrap();
    let plan = QueryPlan { id: "view:detail".to_string(), root };
    let delta = circuit
        .add_query_with_auth(
            plan,
            Some(json!({ "id": "thread:t" })),
            Some(OutputFormat::Streaming),
            "user:a".to_string(),
        )
        .expect("registration must yield an initial delta");

    let aliases: Vec<&str> = delta.subquery_items.iter().map(|i| i.alias.as_str()).collect();
    assert!(
        aliases.contains(&"comments"),
        "the parameterised-parent client query emitted NO comments subquery item — reverse \
         subquery dropped when parent is filtered by `WHERE id = $id`. aliases: {aliases:?}"
    );

    run_edge_writes(&db, &[&delta], &circuit, RefMode::Dedicated, &NoopTelemetry).await;

    let table = ssp_protocol::list_ref_table_for(RefMode::Dedicated, "user:a");
    let mut resp = raw
        .query(format!("SELECT parent_rel, (parent IS NOT NONE) AS has_parent FROM {table};"))
        .await
        .unwrap();
    let rows: Vec<Value> = resp.take(0).unwrap();
    let comment_edge = rows.iter().find(|r| r["parent_rel"] == json!("comments"));
    assert!(
        comment_edge.is_some(),
        "client ThreadDetail query wrote NO comments edge in {table} — matches the live bug. edges: {rows:?}"
    );
    assert_eq!(
        comment_edge.unwrap()["has_parent"],
        json!(true),
        "comments edge has parent = NONE → client filters it out. edges: {rows:?}"
    );
}

#[tokio::test]
async fn register_writes_comments_edge_with_parent() {
    let raw = mem().await;
    // The incantation row the edge writer reads clientId/auth_id from.
    raw.query("CREATE type::record('_00_query', 'detail') SET clientId = 'c1', auth_id = 'user:a';")
        .await
        .unwrap();
    let db = MemDb(Arc::clone(&raw));

    // Circuit store populated as if bootstrapped/ingested from the DB.
    let mut circuit = Circuit::new();
    circuit.load(vec![
        Record::new("user", "user:u", json!({ "username": "alice" })),
        Record::new("thread", "thread:t", json!({ "title": "Hello", "author": "user:u" })),
        Record::new(
            "comment",
            "comment:c",
            json!({ "text": "hi", "thread": "thread:t", "author": "user:u", "created_at": "2026-01-01T00:00:00Z" }),
        ),
    ]);

    // Register the query (initial snapshot delta).
    let root: OperatorPlan =
        serde_json::from_value(converter::convert_surql_to_dbsp(DETAIL_SQL).unwrap()).unwrap();
    let plan = QueryPlan { id: "view:detail".to_string(), root };
    let delta = circuit
        .add_query_with_auth(plan, None, Some(OutputFormat::Streaming), "user:a".to_string())
        .expect("registration must yield an initial delta");

    // Sanity: the circuit emitted both an author AND a comments subquery item.
    let aliases: Vec<&str> = delta.subquery_items.iter().map(|i| i.alias.as_str()).collect();
    assert!(aliases.contains(&"author"), "expected author subquery item; got {aliases:?}");
    assert!(aliases.contains(&"comments"), "expected comments subquery item; got {aliases:?}");

    // Write edges through the real node path.
    run_edge_writes(&db, &[&delta], &circuit, RefMode::Single, &NoopTelemetry).await;

    // Inspect the written edges.
    let mut resp = raw
        .query("SELECT parent_rel, (parent IS NOT NONE) AS has_parent, type::string(out) AS out FROM _00_list_ref;")
        .await
        .unwrap();
    let rows: Vec<Value> = resp.take(0).unwrap();

    let comment_edge = rows.iter().find(|r| r["parent_rel"] == json!("comments"));
    assert!(
        comment_edge.is_some(),
        "no `comments` _00_list_ref edge was written — comments never reach the client. edges: {rows:?}"
    );
    assert_eq!(
        comment_edge.unwrap()["has_parent"],
        json!(true),
        "the `comments` edge has parent = NONE; the client filters `parent IS NOT NONE`, so \
         comments vanish. edges: {rows:?}"
    );
}

// The production scenario: the view is registered FIRST (thread exists, no
// comments yet), then a comment is created and ingested (`step`). The step
// delta must carry a `comments` subquery item so a `_00_list_ref` edge is
// written. If `view.subquery_tables` doesn't include `comment` (e.g. the
// windowed/nested reverse subquery table isn't tracked), the ingest is a no-op
// and the comment never syncs — exactly the live symptom.
#[tokio::test]
async fn ingest_new_comment_writes_comments_edge() {
    let raw = mem().await;
    raw.query("CREATE type::record('_00_query', 'detail') SET clientId = 'c1', auth_id = 'user:a';")
        .await
        .unwrap();
    let db = MemDb(Arc::clone(&raw));

    let mut circuit = Circuit::new();
    // Thread + author present; NO comment yet.
    circuit.load(vec![
        Record::new("user", "user:u", json!({ "username": "alice" })),
        Record::new("thread", "thread:t", json!({ "title": "Hello", "author": "user:u" })),
    ]);

    let root: OperatorPlan =
        serde_json::from_value(converter::convert_surql_to_dbsp(DETAIL_SQL).unwrap()).unwrap();
    let plan = QueryPlan { id: "view:detail".to_string(), root };
    let reg = circuit
        .add_query_with_auth(plan, None, Some(OutputFormat::Streaming), "user:a".to_string())
        .expect("registration delta");
    run_edge_writes(&db, &[&reg], &circuit, RefMode::Single, &NoopTelemetry).await;

    // Now ingest a brand-new comment on the registered thread.
    let deltas = circuit.step(ChangeSet {
        changes: vec![Change::create(
            "comment",
            "comment:c",
            json!({ "text": "hi", "thread": "thread:t", "author": "user:u", "created_at": "2026-01-01T00:00:00Z" }),
        )],
    });
    let refs: Vec<&ViewDelta> = deltas.iter().collect();
    run_edge_writes(&db, &refs, &circuit, RefMode::Single, &NoopTelemetry).await;

    let mut resp = raw
        .query("SELECT parent_rel, (parent IS NOT NONE) AS has_parent FROM _00_list_ref;")
        .await
        .unwrap();
    let rows: Vec<Value> = resp.take(0).unwrap();
    let comment_edge = rows.iter().find(|r| r["parent_rel"] == json!("comments"));
    assert!(
        comment_edge.is_some(),
        "ingesting a new comment on a registered view wrote NO comments edge — the ingest path \
         doesn't emit reverse subquery children (production bug). edges: {rows:?}"
    );
    assert_eq!(
        comment_edge.unwrap()["has_parent"],
        json!(true),
        "the ingested comment's edge has parent = NONE → client filters it out. edges: {rows:?}"
    );
}

// Production runs ref_mode = Dedicated (per-user `_00_list_ref_user_<id>`
// tables), NOT Single. Re-run the register path in Dedicated mode and assert
// the comments edge lands in the per-user table with a non-NONE parent. If the
// Dedicated path drops/mis-parents reverse subquery edges, that's the live bug.
#[tokio::test]
async fn register_writes_comments_edge_dedicated_mode() {
    let auth = "user:a";
    let table = ssp_protocol::list_ref_table_for(RefMode::Dedicated, auth);

    let raw = mem().await;
    raw.query("CREATE type::record('_00_query', 'detail') SET clientId = 'c1', auth_id = 'user:a';")
        .await
        .unwrap();
    let db = MemDb(Arc::clone(&raw));

    let mut circuit = Circuit::new();
    circuit.load(vec![
        Record::new("user", "user:u", json!({ "username": "alice" })),
        Record::new("thread", "thread:t", json!({ "title": "Hello", "author": "user:u" })),
        Record::new(
            "comment",
            "comment:c",
            json!({ "text": "hi", "thread": "thread:t", "author": "user:u", "created_at": "2026-01-01T00:00:00Z" }),
        ),
    ]);

    let root: OperatorPlan =
        serde_json::from_value(converter::convert_surql_to_dbsp(DETAIL_SQL).unwrap()).unwrap();
    let plan = QueryPlan { id: "view:detail".to_string(), root };
    let delta = circuit
        .add_query_with_auth(plan, None, Some(OutputFormat::Streaming), auth.to_string())
        .expect("registration delta");

    run_edge_writes(&db, &[&delta], &circuit, RefMode::Dedicated, &NoopTelemetry).await;

    let mut resp = raw
        .query(format!("SELECT parent_rel, (parent IS NOT NONE) AS has_parent FROM {table};"))
        .await
        .unwrap();
    let rows: Vec<Value> = resp.take(0).unwrap();
    let comment_edge = rows.iter().find(|r| r["parent_rel"] == json!("comments"));
    assert!(
        comment_edge.is_some(),
        "DEDICATED mode ({table}): no `comments` edge written — comments never sync. edges: {rows:?}"
    );
    assert_eq!(
        comment_edge.unwrap()["has_parent"],
        json!(true),
        "DEDICATED mode ({table}): `comments` edge has parent = NONE → client filters it out. edges: {rows:?}"
    );
}

// Regression for the exact production bug: a ViewDelta that carries ONLY
// subquery items (no primary additions/updates/removals) must still write its
// `_00_list_ref` edges. `build_edge_batch` skipped such deltas ("a delta
// carrying only subquery items is not emitted here"), so a comment added to an
// already-in-view thread — whose membership doesn't change — never produced an
// edge and never synced. Forward-link edges survived only because their delta
// also carried a parent addition/content-update.
#[tokio::test]
async fn subquery_only_delta_still_writes_edges() {
    use ssp::circuit::{SubqueryDeltaItem, SubqueryOp, ViewDelta};

    let raw = mem().await;
    raw.query("CREATE type::record('_00_query', 'detail') SET clientId = 'c1', auth_id = 'user:a';")
        .await
        .unwrap();
    raw.query("CREATE thread:t SET n = 1; CREATE comment:c SET n = 1;").await.unwrap();
    // Parent (thread) edge already exists, as if from an earlier registration.
    raw.query("RELATE _00_query:detail->_00_list_ref->thread:t SET version = 1, clientId = 'c1', auth_id = 'user:a';")
        .await
        .unwrap();

    let db = MemDb(Arc::clone(&raw));
    let circuit = Circuit::new();

    let d = ViewDelta {
        query_id: "view:detail".to_string(),
        additions: vec![],
        removals: vec![],
        updates: vec![],
        row_count: 0,
        result_hash: String::new(),
        subquery_items: vec![SubqueryDeltaItem {
            id: "comment:c".to_string(),
            parent_key: "thread:t".to_string(),
            alias: "comments".to_string(),
            op: SubqueryOp::Add,
        }],
        auth_id: "user:a".to_string(),
        initial: false,
    };
    run_edge_writes(&db, &[&d], &circuit, RefMode::Single, &NoopTelemetry).await;

    let mut resp = raw
        .query("SELECT parent_rel FROM _00_list_ref WHERE parent_rel = 'comments';")
        .await
        .unwrap();
    let rows: Vec<Value> = resp.take(0).unwrap();
    assert!(
        !rows.is_empty(),
        "a delta carrying ONLY subquery_items must still write its subquery edge — else a \
         comment on an already-in-view thread never syncs (production bug)"
    );
}
