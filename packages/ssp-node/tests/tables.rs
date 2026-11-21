//! `ssp_node::tables` over a real embedded SurrealDB via the `Db` port.
//! Proves the per-user `_00_list_ref` DDL + cleanup SQL run correctly through
//! the flattened-JSON port interface (was validated only inside the VM shell).

use std::sync::Arc;

use serde_json::Value;
use ssp_node::ports::{Db, DbError};
use ssp_node::tables;
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

async fn mem() -> (Arc<dyn Db>, Arc<Surreal<MemEngine>>) {
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("t").use_db("t").await.unwrap();
    let raw = Arc::new(db);
    (Arc::new(MemDb(Arc::clone(&raw))), raw)
}

async fn table_exists(raw: &Surreal<MemEngine>, name: &str) -> bool {
    let info = raw
        .query("INFO FOR DB")
        .await
        .unwrap()
        .take::<surrealdb::types::Value>(0)
        .unwrap()
        .into_json_value();
    info.get("tables")
        .and_then(|t| t.as_object())
        .map(|m| m.contains_key(name))
        .unwrap_or(false)
}

#[test]
fn list_ref_table_names() {
    assert_eq!(tables::list_ref_table(RefMode::Single, "user:alice"), "_00_list_ref");
    let dedicated = tables::list_ref_table(RefMode::Dedicated, "user:alice");
    assert!(dedicated.starts_with("_00_list_ref"), "dedicated: {dedicated}");
}

#[tokio::test]
async fn single_mode_defines_no_per_user_tables() {
    let (db, raw) = mem().await;
    tables::ensure_user_tables(db.as_ref(), RefMode::Single, "user:alice")
        .await
        .unwrap();
    // No _00_list_ref_user_* table created in single mode.
    let info = raw
        .query("INFO FOR DB")
        .await
        .unwrap()
        .take::<surrealdb::types::Value>(0)
        .unwrap()
        .into_json_value();
    let any_user = info
        .get("tables")
        .and_then(|t| t.as_object())
        .map(|m| m.keys().any(|k| k.starts_with("_00_list_ref_user_")))
        .unwrap_or(false);
    assert!(!any_user);
}

#[tokio::test]
async fn dedicated_mode_defines_and_drops_per_user_table() {
    let (db, raw) = mem().await;
    let tbl = tables::list_ref_table(RefMode::Dedicated, "user:alice");

    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:alice")
        .await
        .unwrap();
    assert!(table_exists(&raw, &tbl).await, "{tbl} should exist after ensure");

    // Idempotent (OVERWRITE) — second call is fine.
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:alice")
        .await
        .unwrap();

    tables::drop_user_tables(db.as_ref(), RefMode::Dedicated, "user:alice")
        .await
        .unwrap();
    assert!(!table_exists(&raw, &tbl).await, "{tbl} should be gone after drop");
}

#[tokio::test]
async fn drop_if_unused_keeps_table_with_live_query() {
    let (db, raw) = mem().await;
    let tbl = tables::list_ref_table(RefMode::Dedicated, "user:bob");
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:bob")
        .await
        .unwrap();

    // A live query for bob → table must survive.
    raw.query("CREATE _00_query SET auth_id = 'user:bob'").await.unwrap();
    tables::drop_user_table_if_unused(db.as_ref(), RefMode::Dedicated, "user:bob")
        .await
        .unwrap();
    assert!(table_exists(&raw, &tbl).await, "table kept while a query is live");

    // Remove the query → now it should be dropped.
    raw.query("DELETE _00_query").await.unwrap();
    tables::drop_user_table_if_unused(db.as_ref(), RefMode::Dedicated, "user:bob")
        .await
        .unwrap();
    assert!(!table_exists(&raw, &tbl).await, "table dropped once no query remains");
}

#[tokio::test]
async fn drop_orphaned_removes_only_dead_owners() {
    let (db, raw) = mem().await;
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:live")
        .await
        .unwrap();
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:dead")
        .await
        .unwrap();
    // Only `live` has a registered query.
    raw.query("CREATE _00_query SET auth_id = 'user:live'").await.unwrap();

    tables::drop_orphaned_user_tables(db.as_ref(), RefMode::Dedicated)
        .await
        .unwrap();

    assert!(table_exists(&raw, &tables::list_ref_table(RefMode::Dedicated, "user:live")).await);
    assert!(!table_exists(&raw, &tables::list_ref_table(RefMode::Dedicated, "user:dead")).await);
}

async fn returns(raw: &Surreal<MemEngine>, surql: &str) -> Value {
    raw.query(surql)
        .await
        .unwrap()
        .take::<surrealdb::types::Value>(0)
        .unwrap()
        .into_json_value()
}

/// The per-user edge table asks the app's session guard, and an SSP that runs
/// ahead of `spky migrate` defines its default so the rule never names a
/// function that does not exist (that would deny every signed-in edge).
#[tokio::test]
async fn per_user_table_asks_the_session_guard_and_brings_its_default() {
    let (db, raw) = mem().await;
    let tbl = tables::list_ref_table(RefMode::Dedicated, "user:guarded");
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:guarded")
        .await
        .unwrap();

    let info = returns(&raw, "INFO FOR DB").await;
    let rule = info["tables"][&tbl].as_str().unwrap_or_default().to_string();
    // SurrealDB renders the literal id back as `(user:guarded)`.
    assert!(
        rule.contains("FOR select WHERE $auth.id = (user:guarded) AND fn::sp00ky::session_live(),"),
        "per-user rule must ask the guard: {rule}"
    );
    assert_eq!(returns(&raw, "RETURN fn::sp00ky::session_live()").await, Value::Bool(true));
}

/// The default never replaces a guard the internal schema already defined.
#[tokio::test]
async fn per_user_table_keeps_the_apps_session_guard() {
    let (db, raw) = mem().await;
    raw.query("DEFINE FUNCTION fn::sp00ky::session_live() -> bool { RETURN false; } PERMISSIONS FULL;")
        .await
        .unwrap()
        .check()
        .unwrap();
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:guarded_app")
        .await
        .unwrap();
    assert_eq!(returns(&raw, "RETURN fn::sp00ky::session_live()").await, Value::Bool(false));
}

/// What the TTL sweep needs to find a user's table unused: one expired view
/// of theirs (the sweep deletes it, then finds no `_00_query` row left).
async fn seed_expired_view(raw: &Surreal<MemEngine>, key: &str, auth_id: &str) {
    raw.query(format!(
        "CREATE _00_query:{key} SET auth_id = '{auth_id}', clientId = 'c', ttl = 1s, \
         lastActiveAt = time::now() - 1h, surql = 'SELECT * FROM thread', params = {{}};"
    ))
    .await
    .unwrap()
    .check()
    .unwrap();
}

fn circuit_serving(auth_id: &str) -> Arc<tokio::sync::RwLock<ssp::circuit::Circuit>> {
    let mut circuit = ssp::circuit::Circuit::new();
    circuit.add_query_with_auth(
        ssp::operator::QueryPlan {
            id: "fresh".into(),
            root: ssp::operator::OperatorPlan::Scan { table: "thread".into() },
        },
        None,
        None,
        auth_id.to_string(),
    );
    Arc::new(tokio::sync::RwLock::new(circuit))
}

/// A registration defines its user's table before its `_00_query` row
/// exists, so the sweep cannot tell from `_00_query` alone that the table is
/// about to be written. Removing it under that registration's first edge
/// writes is what left rows SurrealDB 4.0.1 could no longer read or delete
/// once the table was defined again (whitepawn 2026-11-21). The circuit
/// knows: while it serves the user, the table stays.
#[tokio::test]
async fn the_sweep_keeps_a_users_table_while_the_circuit_serves_them() {
    let (db, raw) = mem().await;
    let tbl = tables::list_ref_table(RefMode::Dedicated, "user:busy_sweep");
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:busy_sweep").await.unwrap();
    seed_expired_view(&raw, "old", "user:busy_sweep").await;
    let processor = circuit_serving("user:busy_sweep");
    let telemetry = ssp_node::ports::NoopTelemetry;

    let cleaned = ssp_node::ttl_cleanup_sweep(db.as_ref(), &processor, &telemetry, RefMode::Dedicated).await;
    assert_eq!(cleaned, 1, "the expired row itself is still swept");
    assert!(table_exists(&raw, &tbl).await, "kept while the circuit serves the user");

    // Once the circuit lets go, the table goes as before.
    processor.write().await.remove_query("fresh");
    ssp_node::ttl_cleanup_sweep(db.as_ref(), &processor, &telemetry, RefMode::Dedicated).await;
    assert!(!table_exists(&raw, &tbl).await, "dropped once nothing serves the user");
}

/// The sweep removes a table only under the publication gate, so never in
/// the middle of a round writing that table's edges.
#[tokio::test]
async fn the_sweep_removes_a_table_only_under_the_publication_gate() {
    let (db, raw) = mem().await;
    let tbl = tables::list_ref_table(RefMode::Dedicated, "user:gated_sweep");
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:gated_sweep").await.unwrap();
    seed_expired_view(&raw, "gone", "user:gated_sweep").await;
    let processor = Arc::new(tokio::sync::RwLock::new(ssp::circuit::Circuit::new()));
    let telemetry = ssp_node::ports::NoopTelemetry;
    let gate = tokio::sync::Mutex::new(());

    let round = gate.lock().await;
    let sweep = ssp_node::ttl_cleanup_sweep_gated(db.as_ref(), &processor, &telemetry, RefMode::Dedicated, Some(&gate));
    tokio::pin!(sweep);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut sweep).await.is_err(),
        "the sweep waits for the round"
    );
    assert!(table_exists(&raw, &tbl).await, "nothing removed while the round holds the gate");
    drop(round);
    sweep.await;
    assert!(!table_exists(&raw, &tbl).await);
}

/// The DDL, now one transaction, still defines the table, its rule and its
/// fields on a real SurrealDB.
#[tokio::test]
async fn the_per_user_ddl_transaction_defines_every_field() {
    let (db, raw) = mem().await;
    let tbl = tables::list_ref_table(RefMode::Dedicated, "user:one_tx");
    tables::ensure_user_tables(db.as_ref(), RefMode::Dedicated, "user:one_tx").await.unwrap();
    let info = returns(&raw, &format!("INFO FOR TABLE {tbl}")).await;
    let mut fields: Vec<String> = info["fields"].as_object().unwrap().keys().cloned().collect();
    fields.sort();
    assert_eq!(fields, ["auth_id", "clientId", "in", "out", "parent", "parent_rel", "updatedAt", "version"]);
}
