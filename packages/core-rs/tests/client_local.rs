//! The thread-safe client against a local-only engine (no endpoint): writes
//! land optimistically and live queries see them.

use std::time::Duration;

use serde::Deserialize;
use serde_json::json;
use spooky_core::value::object_from_json;
use spooky_core::{ColumnSchema, DatabaseConfig, QueryOptions, Schema, Sp00kyClient, Sp00kyConfig, StoreType};

const SURQL: &str = "DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;\n\
DEFINE FIELD title ON thread TYPE string;\n\
DEFINE FIELD score ON thread TYPE int;\n";

fn config(store: StoreType) -> Sp00kyConfig {
    let schema = Schema::default().with_table(
        "thread",
        [("title", ColumnSchema::of_type("string")), ("score", ColumnSchema::of_type("int"))],
    );
    Sp00kyConfig::new(DatabaseConfig::new("test", "test").store(store), schema, SURQL)
}

#[derive(Debug, Deserialize, PartialEq)]
struct Thread {
    id: String,
    title: String,
    score: i64,
}

async fn wait_for<F: Fn(&[spooky_core::Row]) -> bool>(live: &mut spooky_core::LiveQuery, pred: F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if pred(&live.rows()) {
            return;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out; rows = {:?}", live.rows());
        let _ = tokio::time::timeout(remaining, live.changed()).await;
    }
}

#[tokio::test]
async fn local_write_shows_in_live_query() {
    let client = Sp00kyClient::open(config(StoreType::Memory)).await.unwrap();
    let mut live = client
        .live_raw("SELECT * FROM thread ORDER BY score DESC", Default::default(), QueryOptions::default())
        .await
        .unwrap();
    assert!(live.rows().is_empty());

    client
        .create("thread:a", object_from_json(json!({"title": "first", "score": 1})))
        .await
        .unwrap();
    client
        .create("thread:b", object_from_json(json!({"title": "second", "score": 2})))
        .await
        .unwrap();
    wait_for(&mut live, |rows| rows.len() == 2).await;
    let threads: Vec<Thread> = live.rows_as().unwrap();
    assert_eq!(threads[0].id, "thread:b");
    assert_eq!(threads[1].title, "first");

    client
        .update("thread:a", object_from_json(json!({"score": 9})))
        .await
        .unwrap();
    wait_for(&mut live, |rows| rows.first().and_then(|r| r.get("id")).map(|v| v.to_id_string()) == Some("thread:a".into())).await;

    client.delete("thread:b").await.unwrap();
    wait_for(&mut live, |rows| rows.len() == 1).await;
    client.close().await.unwrap();
}

#[tokio::test]
async fn file_store_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spooky.db");
    {
        let client = Sp00kyClient::open(config(StoreType::File(path.clone()))).await.unwrap();
        client
            .create("thread:keep", object_from_json(json!({"title": "kept", "score": 3})))
            .await
            .unwrap();
        client.close().await.unwrap();
    }
    let client = Sp00kyClient::open(config(StoreType::File(path))).await.unwrap();
    let row = client.read_row("thread:keep").await.unwrap();
    assert_eq!(row.and_then(|r| r.get("title").map(|v| v.to_id_string())), Some("kept".into()));
    let mut live = client
        .live_raw("SELECT * FROM thread", Default::default(), QueryOptions::default())
        .await
        .unwrap();
    wait_for(&mut live, |rows| rows.len() == 1).await;
    client.close().await.unwrap();
}
