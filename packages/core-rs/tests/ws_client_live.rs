//! [`WebSocketSurrealClient`] against a real SurrealDB.
//!
//! Skipped unless `SURREAL_IT_ENDPOINT` is set. Boot one first, e.g.:
//!
//! ```sh
//! docker run -d --name spooky-surreal-it -p 18011:8000 \
//!   surrealdb/surrealdb:v2.1.4 start --user root --pass root --allow-all memory
//! SURREAL_IT_ENDPOINT=ws://127.0.0.1:18011 cargo test -p spooky-core --test ws_client_live
//! ```
//!
//! Each test works in a database of its own, so they can run in parallel.
#![cfg(feature = "remote")]

use std::time::Duration;

use spooky_core::surreal::rpc::{LiveMessage, TransportEvent, WebSocketSurrealClient};
use spooky_core::{Error, Object, RecordId, SurrealDuration, Value};

fn endpoint() -> Option<String> {
    std::env::var("SURREAL_IT_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())
}

fn vars(entries: Vec<(&str, Value)>) -> Option<Object> {
    Some(
        entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

/// A root-signed-in client using database `it_<name>`, or `None` (the test
/// is skipped) when no endpoint is configured.
async fn client(name: &str) -> Option<WebSocketSurrealClient> {
    let Some(endpoint) = endpoint() else {
        eprintln!("SURREAL_IT_ENDPOINT not set; skipping");
        return None;
    };
    let client = WebSocketSurrealClient::new();
    client.set_connect_timeout(Duration::from_secs(3));
    client.connect(&endpoint).await.expect("connect");
    let root = serde_json::json!({"user": "root", "pass": "root"});
    client
        .signin(spooky_core::value::object_from_json(root))
        .await
        .expect("root signin");
    // Tests defining databases at once can conflict in the catalog (v3:
    // "Transaction conflict ... can be retried").
    let database = format!("it_{name}");
    let mut attempt = 0;
    while let Err(e) = client.use_db("it", &database).await {
        attempt += 1;
        assert!(attempt < 10, "use: {e:?}");
        tokio::time::sleep(Duration::from_millis(50 * attempt)).await;
    }
    let _ = client.query("REMOVE TABLE IF EXISTS thing", None).await;
    Some(client)
}

#[tokio::test]
async fn query_create_then_select_round_trips() {
    let Some(client) = client("query").await else {
        return;
    };
    client
        .query(
            "CREATE thing:a SET name = $name",
            vars(vec![("name", "alice".into())]),
        )
        .await
        .unwrap();
    let results = client.query("SELECT * FROM thing", None).await.unwrap();
    let rows = results[0].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::from("alice")));
    // Records come back normalized to `table:id` strings.
    assert_eq!(rows[0].get("id"), Some(&Value::from("thing:a")));
    client.close().await;
}

#[tokio::test]
async fn bind_vars_record_id_encodes_and_matches() {
    let Some(client) = client("bind").await else {
        return;
    };
    let id = || vars(vec![("id", RecordId::new("thing", "x").into())]);
    client.query("CREATE $id SET v = 1", id()).await.unwrap();
    let results = client.query("SELECT * FROM $id", id()).await.unwrap();
    let rows = results[0].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("v"), Some(&Value::Int(1)));
    client.close().await;
}

#[tokio::test]
async fn a_per_statement_err_fails_the_query() {
    let Some(client) = client("err").await else {
        return;
    };
    assert!(matches!(
        client.query("THROW 'boom'", None).await,
        Err(Error::Statement(_))
    ));
    let stmts = client
        .query_statements("RETURN 1; THROW 'boom'", None)
        .await
        .unwrap();
    assert!(stmts[0].is_ok());
    assert!(!stmts[1].is_ok());
    client.close().await;
}

#[tokio::test]
async fn live_receives_a_notification_and_stops_after_kill() {
    let Some(client) = client("live").await else {
        return;
    };
    // v3 rejects LIVE on a non-existent table; seed a row so it exists.
    client
        .query("CREATE thing:seed SET name = 'seed'", None)
        .await
        .unwrap();
    let (live_id, mut stream) = client.live("LIVE SELECT * FROM thing", None).await.unwrap();
    assert!(!live_id.is_empty());

    client
        .query(
            "CREATE thing:live SET name = $n",
            vars(vec![("n", "bob".into())]),
        )
        .await
        .unwrap();
    let got: LiveMessage = tokio::time::timeout(Duration::from_secs(3), stream.recv())
        .await
        .expect("no notification")
        .expect("stream ended");
    assert!(
        got.action == "CREATE" || got.action == "UPDATE",
        "{}",
        got.action
    );
    assert_eq!(got.value.get("name"), Some(&Value::from("bob")));

    client.kill(&live_id).await.unwrap();
    client
        .query(
            "CREATE thing:live2 SET name = $n",
            vars(vec![("n", "carol".into())]),
        )
        .await
        .unwrap();
    // Killed: the stream ends without delivering the second create. v3 sends a
    // `KILLED` notification ahead of the kill's answer, which may still be
    // buffered.
    loop {
        let next = tokio::time::timeout(Duration::from_millis(500), stream.recv())
            .await
            .expect("the stream must end after KILL");
        match next {
            None => break,
            Some(m) => assert_eq!(m.action, "KILLED", "no notifications after KILL: {m:?}"),
        }
    }
    client.close().await;
}

#[tokio::test]
async fn connected_lifecycle_event_fires() {
    let Some(endpoint) = endpoint() else { return };
    let client = WebSocketSurrealClient::new();
    let mut events = client.events();
    client.connect(&endpoint).await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap();
    assert_eq!(event.unwrap(), TransportEvent::Connected);
    client.close().await;
}

#[tokio::test]
async fn typed_values_come_back_normalized() {
    let Some(client) = client("shapes").await else {
        return;
    };
    let sql = "RETURN [1h30m, 250ms, d'2026-01-02T03:04:05.006Z', <decimal> 1.5, \
               <uuid> '0193f1d2-a4b6-7c8d-9e0f-112233445566', <bytes> 'ab', NONE, NULL, thing:x, $d, $t]";
    let when = chrono::DateTime::parse_from_rfc3339("2026-01-02T03:04:05.000007Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let out = client
        .query(
            sql,
            vars(vec![
                ("d", SurrealDuration::new("1h30m").into()),
                ("t", Value::Datetime(when)),
            ]),
        )
        .await
        .unwrap();
    let got = out[0].as_array().unwrap();
    let expected = vec![
        Value::from("1h30m"),
        Value::from("250ms"),
        Value::from("2026-01-02T03:04:05.006Z"),
        Value::from("1.5"),
        Value::from("0193f1d2-a4b6-7c8d-9e0f-112233445566"),
        Value::Bytes(b"ab".to_vec()),
        Value::None,
        Value::Null,
        Value::from("thing:x"),
        // Sent as `[seconds, nanoseconds]` (tag 14), which SurrealDB 4 requires.
        Value::from("1h30m"),
        Value::from("2026-01-02T03:04:05.000007Z"),
    ];
    assert_eq!(*got, expected);
    client.close().await;
}

#[tokio::test]
async fn the_supervisor_revives_a_dropped_socket() {
    use std::rc::Rc;

    use spooky_core::services::remote::RemoteDatabaseService;
    use spooky_core::services::supervisor::ConnectionSupervisor;
    use spooky_core::{ConnectionState, DatabaseConfig, ReconnectConfig};

    let Some(endpoint) = endpoint() else { return };
    spooky_core::testing::run_local(async move {
        // A root token: the reconnect must re-apply it, or the probe runs
        // anonymous and is refused.
        let token = {
            let c = WebSocketSurrealClient::new();
            c.connect(&endpoint).await.unwrap();
            let root = serde_json::json!({"user": "root", "pass": "root"});
            let token = c
                .signin(spooky_core::value::object_from_json(root))
                .await
                .unwrap();
            c.close().await;
            token.as_str().unwrap().to_string()
        };
        let ws = WebSocketSurrealClient::new();
        let config = DatabaseConfig::new("it", "it_supervisor").endpoint(endpoint);
        let remote = RemoteDatabaseService::new(config, Rc::new(ws.clone()));
        remote.set_auth_token(Some(token));
        remote.arm_connect_gate();
        remote.connect().await.unwrap();
        let supervisor = ConnectionSupervisor::for_remote(&remote, ReconnectConfig::default());
        supervisor.start();
        assert_eq!(supervisor.connection(), ConnectionState::Connected);

        // The new socket reads its own session id in the background.
        while remote.session_ids().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // What a half-open socket's heartbeat teardown does.
        ws.force_close().await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut saw_drop = false;
        while tokio::time::Instant::now() < deadline {
            match supervisor.connection() {
                ConnectionState::Connected if saw_drop => break,
                ConnectionState::Connected => {}
                _ => saw_drop = true,
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(saw_drop, "the drop was never reported");
        assert_eq!(supervisor.connection(), ConnectionState::Connected);
        remote.probe().await.unwrap();
        while remote.session_ids().len() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            remote.session_ids().len(),
            2,
            "each socket is its own session"
        );
        supervisor.dispose();
        remote.close().await;
    })
    .await;
}
