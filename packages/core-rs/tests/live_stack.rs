//! End to end against a running sp00ky dev stack (SurrealDB + SSP +
//! scheduler). Skipped unless `SPOOKY_IT_ENDPOINT` is set, e.g.
//!
//! ```text
//! SPOOKY_IT_ENDPOINT=ws://127.0.0.1:8666/rpc SPOOKY_IT_NS=main SPOOKY_IT_DB=rustcore \
//!   cargo test -p spooky-core --test live_stack -- --nocapture
//! ```
//!
//! The stack's schema must define `ACCESS account` (username + password) and a
//! `note` table (`owner: record<user>`, `title: string`, `body:
//! option<string>`, `rank: int`) readable by its owner; see `SCHEMA` below.

use std::time::Duration;

use serde_json::json;
use spooky_core::value::object_from_json;
use spooky_core::{
    ColumnSchema, DatabaseConfig, LiveQuery, PersistenceConfig, Row, Schema, Sp00kyClient, Sp00kyConfig, StoreType,
};

const SCHEMA: &str = r#"DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP ( CREATE user SET username = $username, password = crypto::argon2::generate($password) )
  SIGNIN ( SELECT * FROM user WHERE username = $username AND crypto::argon2::compare(password, $password) )
  DURATION FOR TOKEN 1h, FOR SESSION 1d;
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select, update, delete WHERE id = $auth.id FOR create WHERE true;
DEFINE FIELD username ON TABLE user TYPE string ASSERT string::len($value) > 3;
DEFINE FIELD password ON TABLE user TYPE string PERMISSIONS FOR select WHERE false;
DEFINE TABLE note SCHEMAFULL PERMISSIONS FOR select, create, update, delete WHERE owner = $auth.id;
DEFINE FIELD owner ON TABLE note TYPE record<user>;
DEFINE FIELD title ON TABLE note TYPE string;
DEFINE FIELD body ON TABLE note TYPE option<string>;
DEFINE FIELD rank ON TABLE note TYPE int;
"#;

fn endpoint() -> Option<String> {
    std::env::var("SPOOKY_IT_ENDPOINT").ok().filter(|s| !s.is_empty())
}

fn schema() -> Schema {
    let mut schema = Schema::default()
        .with_table(
            "user",
            [("username", ColumnSchema::of_type("string"))],
        )
        .with_table(
            "note",
            [
                (
                    "owner",
                    ColumnSchema {
                        record_id: true,
                        kind: Some("record<user>".into()),
                        ..Default::default()
                    },
                ),
                ("title", ColumnSchema::of_type("string")),
                (
                    "body",
                    ColumnSchema {
                        kind: Some("option<string>".into()),
                        optional: true,
                        ..Default::default()
                    },
                ),
                ("rank", ColumnSchema::of_type("int")),
            ],
        );
    let mut account = spooky_core::AccessDef::default();
    for m in [&mut account.signin, &mut account.signup] {
        m.params.insert("username".into(), ColumnSchema::of_type("string"));
        m.params.insert("password".into(), ColumnSchema::of_type("string"));
    }
    schema.access.insert("account".into(), account);
    schema
}

async fn client(endpoint: &str) -> Sp00kyClient {
    let ns = std::env::var("SPOOKY_IT_NS").unwrap_or_else(|_| "main".into());
    let db = std::env::var("SPOOKY_IT_DB").unwrap_or_else(|_| "rustcore".into());
    let mut config = Sp00kyConfig::new(
        DatabaseConfig::new(ns, db).endpoint(endpoint).store(StoreType::Memory),
        schema(),
        SCHEMA,
    );
    config.persistence = PersistenceConfig::Memory;
    Sp00kyClient::open(config).await.expect("open client")
}

async fn wait_rows(live: &mut LiveQuery, what: &str, pred: impl Fn(&[Row]) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if pred(&live.rows()) {
            return;
        }
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!left.is_zero(), "timed out waiting for {what}; rows = {:?}", live.rows());
        let _ = tokio::time::timeout(left.min(Duration::from_millis(250)), live.changed()).await;
    }
}

async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn title_of(rows: &[Row], id: &str) -> Option<String> {
    rows.iter()
        .find(|r| r.get("id").map(|v| v.to_id_string()).as_deref() == Some(id))
        .and_then(|r| r.get("title"))
        .map(|v| v.to_id_string())
}

#[tokio::test]
async fn two_clients_of_one_user_sync_live() {
    let Some(endpoint) = endpoint() else {
        eprintln!("SPOOKY_IT_ENDPOINT not set; skipping");
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let username = format!("rs{}", &spooky_core::value::generate_id()[..10]);
    let creds = || object_from_json(json!({"username": username, "password": "hunter2-pass"}));

    let a = client(&endpoint).await;
    a.sign_up("account", creds()).await.expect("sign up");
    let user = a.user_id().expect("signed in");
    assert!(user.starts_with("user:"), "{user}");

    let query = |c: &Sp00kyClient| c.query("note").where_eq("owner", user.as_str()).order_by("rank", "desc");
    let mut live_a = a.live(query(&a)).await.expect("live a");

    // An optimistic create paints at once, then the server acknowledges it.
    let note = format!("note:{}", spooky_core::value::generate_id());
    a.create(
        &note,
        object_from_json(json!({"owner": user, "title": "from a", "rank": 1})),
    )
    .await
    .expect("create");
    wait_rows(&mut live_a, "optimistic create", |rows| title_of(rows, &note).is_some()).await;
    wait_until("outbox drained", || a.pending_mutation_count() == 0).await;
    assert_eq!(a.failed_mutation_count(), 0, "create was rejected");
    wait_until("authoritative", || live_a.is_authoritative()).await;

    // A second client of the same user receives it through the server.
    let b = client(&endpoint).await;
    b.sign_in("account", creds()).await.expect("sign in");
    let mut live_b = b.live(query(&b)).await.expect("live b");
    wait_rows(&mut live_b, "row on b", |rows| title_of(rows, &note).as_deref() == Some("from a")).await;

    // B edits; A sees it live.
    b.update(&note, object_from_json(json!({"title": "edited on b"})))
        .await
        .expect("update");
    wait_until("b's update acked", || b.pending_mutation_count() == 0).await;
    assert_eq!(b.failed_mutation_count(), 0, "update was rejected: {:?}", b.list_failed_mutations().await);
    if tokio::time::timeout(
        Duration::from_secs(12),
        wait_rows(&mut live_a, "b's edit on a", |rows| title_of(rows, &note).as_deref() == Some("edited on b")),
    )
    .await
    .is_err()
    {
        let st = a.inspect_state().await.unwrap();
        for (h, e) in &st.queries {
            eprintln!("A query {h}: {:?} remote={:?} local={:?} polled={:?}", e.lifecycle, e.remote_array, e.local_array, e.last_polled_at);
        }
        eprintln!("A versions {:?}\nA sync {:?}", st.versions, st.sync);
        let st = b.inspect_state().await.unwrap();
        for (h, e) in &st.queries {
            eprintln!("B query {h}: {:?} remote={:?} local={:?}", e.lifecycle, e.remote_array, e.local_array);
        }
        eprintln!("B versions {:?}\nB sync {:?}", st.versions, st.sync);
        panic!("b's edit never reached a");
    }

    // A creates a second, higher-ranked note; B sees both, in order.
    let note2 = format!("note:{}", spooky_core::value::generate_id());
    a.create(
        &note2,
        object_from_json(json!({"owner": user, "title": "second", "rank": 5})),
    )
    .await
    .expect("create 2");
    wait_rows(&mut live_b, "ordered pair on b", |rows| {
        rows.len() == 2 && rows[0].get("id").map(|v| v.to_id_string()) == Some(note2.clone())
    })
    .await;

    // A deletes; B sees it go.
    a.delete(&note).await.expect("delete");
    wait_rows(&mut live_b, "delete on b", |rows| title_of(rows, &note).is_none() && rows.len() == 1).await;

    assert!(!a.sync_health().is_degraded(), "{:?}", a.sync_health());
    a.sign_out().await.expect("sign out");
    assert_eq!(a.user_id(), None);
    a.close().await.unwrap();
    b.close().await.unwrap();
}
