//! The export a backup takes, against a real SurrealDB. Env-gated: set
//! `SPKY_EXPORT_TEST_URL` (e.g. `http://127.0.0.1:8000`, root/root) to run.

use maintenance::db::{connect_http, DbConfig};
use maintenance::export::{export_database, export_without_rows_of};

fn config(url: &str, database: &str) -> DbConfig {
    DbConfig {
        url: url.to_string(),
        namespace: "export_test".into(),
        database: database.into(),
        username: "root".into(),
        password: "root".into(),
    }
}

const EDGES: &str = "_00_list_ref_user_exporttest";

async fn seed(url: &str, database: &str) {
    let raw = maintenance::db::connect_http_raw(&config(url, database))
        .await
        .unwrap();
    raw.query(format!(
        "DEFINE NAMESPACE IF NOT EXISTS export_test; USE NS export_test; \
         REMOVE DATABASE IF EXISTS {database}; DEFINE DATABASE {database}; USE DB {database};
         CREATE game:a SET n = 1; CREATE game:b SET n = 2; CREATE _00_query:q SET surql = 'x';
         DEFINE TABLE {EDGES} SCHEMALESS PERMISSIONS FOR select WHERE $auth.id = user:exporttest, \
           FOR create, update, delete WHERE false;
         DEFINE FIELD version ON {EDGES} TYPE int;
         RELATE _00_query:q->{EDGES}->game:a SET version = 1;
         RELATE _00_query:q->{EDGES}->game:b SET version = 1;"
    ))
    .await
    .unwrap()
    .check()
    .unwrap();
}

#[tokio::test]
async fn an_export_without_an_edge_tables_rows_keeps_its_definition() {
    let Ok(url) = std::env::var("SPKY_EXPORT_TEST_URL") else {
        eprintln!("SPKY_EXPORT_TEST_URL unset; skipping");
        return;
    };
    seed(&url, "src").await;
    let db = connect_http(&config(&url, "src")).await.unwrap();

    // A healthy database exports whole, nothing left out.
    let whole = tempfile::NamedTempFile::new().unwrap();
    assert!(export_database(&db, whole.path()).await.unwrap().is_empty());
    let text = std::fs::read_to_string(whole.path()).unwrap();
    assert!(text.contains(&format!("TABLE DATA: {EDGES}")), "{text}");

    // Without the edge table's rows: everything else whole, its definition kept.
    let partial = tempfile::NamedTempFile::new().unwrap();
    export_without_rows_of(&db, partial.path(), &[EDGES.to_string()])
        .await
        .unwrap();

    let raw = maintenance::db::connect_http_raw(&config(&url, "dst"))
        .await
        .unwrap();
    raw.query("USE NS export_test; REMOVE DATABASE IF EXISTS dst; DEFINE DATABASE dst;")
        .await
        .unwrap()
        .check()
        .unwrap();
    let dst = connect_http(&config(&url, "dst")).await.unwrap();
    dst.import(partial.path()).await.unwrap();

    let mut r = dst
        .query(format!(
            "RETURN count(SELECT * FROM game); RETURN count(SELECT * FROM _00_query); \
             RETURN count(SELECT * FROM {EDGES}); RETURN (INFO FOR TABLE {EDGES}).fields; \
             RETURN (INFO FOR DB).tables.{EDGES};"
        ))
        .await
        .unwrap();
    let game: Option<i64> = r.take(0).unwrap();
    let queries: Option<i64> = r.take(1).unwrap();
    let edges: Option<i64> = r.take(2).unwrap();
    let fields: surrealdb::types::Value = r.take(3).unwrap();
    let table: surrealdb::types::Value = r.take(4).unwrap();
    assert_eq!(game, Some(2));
    assert_eq!(queries, Some(1));
    assert_eq!(edges, Some(0), "the left-out table's rows stay out");
    let fields = fields.into_json_value().to_string();
    assert!(fields.contains("version"), "its fields come back: {fields}");
    let table = table.into_json_value().to_string();
    assert!(
        table.contains("user:exporttest"),
        "its permissions come back: {table}"
    );
}
