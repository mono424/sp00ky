//! The Web Push SQL the CLI writes, run against an embedded SurrealDB v3 with
//! the real shipped DDL (`push_tables.surql`, in both transport forms).
//!
//! The string-level tests next to each builder pin the shape; these pin the
//! meaning on the engine and parser the server actually runs (the CLI's own
//! `surrealdb-core` is the 2.x parser): the config round-trips byte for byte,
//! the direct-push ingest event posts what the changefeed would carry, and the
//! status / devices / send statements work against the real tables.

use serde_json::{json, Value};
use surrealdb::engine::local::{Db, Mem};
use surrealdb::Surreal;

use crate::backend::{SyncConfig, SyncTransport};
use crate::push_cmd::{self, Message, MessageArgs};
use crate::push_sync;

async fn db() -> Surreal<Db> {
    let db = Surreal::new::<Mem>(()).await.expect("mem db");
    db.use_ns("t").use_db("t").await.expect("ns/db");
    db
}

/// Every statement's result as JSON; panics with the SQL on any error.
async fn run(db: &Surreal<Db>, sql: &str) -> Vec<Value> {
    let mut response = db
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n-> {e}"));
    let n = response.num_statements();
    (0..n)
        .map(|i| {
            let v: surrealdb::types::Value = response
                .take(i)
                .unwrap_or_else(|e| panic!("statement {i} of\n{sql}\n-> {e}"));
            v.into_json_value()
        })
        .collect()
}

fn sync_config(transport: SyncTransport) -> SyncConfig {
    SyncConfig {
        transport: Some(transport),
        ..Default::default()
    }
}

fn message(to: &[&str], title: Option<&str>, at: Option<&str>) -> Message {
    Message::build(MessageArgs {
        to: to.iter().map(|s| s.to_string()).collect(),
        title: title.map(str::to_owned),
        body: title.map(|_| "body".to_string()),
        link: None,
        icon: None,
        tag: None,
        topic: Some("t1".into()),
        at: at.map(str::to_owned),
        ttl: Some("1h".into()),
        urgency: Some("high".into()),
        data: Some(r#"{"k":1}"#.into()),
    })
    .expect("valid message")
}

/// Create `msg` the way `spky push send` does; the new id.
async fn create(db: &Surreal<Db>, msg: &Message) -> String {
    let out = run(db, &msg.create_sql()).await;
    let created = out.last().expect("RETURN");
    let id = created["id"].as_str().expect("string id").to_string();
    assert!(id.starts_with("_00_push_message:"), "{created}");
    id
}

#[tokio::test]
async fn the_config_upsert_round_trips_byte_for_byte() {
    let db = db().await;
    run(&db, &push_sync_ddl(SyncTransport::Http)).await;

    let cfg: push_core::PushConfig = serde_yaml::from_str(
        r#"
subject: "mailto:o'brien@example.com"
rules:
  new-message:
    table: message
    to: recipient
    topic: "dm:{{conversation | key}}"
    notification:
      title: "{{sender}} says \"hi\" \\ o'clock"
      body: "line one\nline two\ttabbed, café ☃ \U0001F600 {{ text }}"
"#,
    )
    .unwrap();
    let spec = push_sync::spec_of(&cfg).unwrap();
    run(&db, &push_sync::upsert_sql(&spec)).await;
    run(&db, &push_sync::upsert_sql(&spec)).await;

    let out = run(&db, "SELECT spec_json, hash FROM _00_push_config;").await;
    let rows = out[0].as_array().unwrap();
    assert_eq!(rows.len(), 1, "one row, however often it is written");
    assert_eq!(
        rows[0]["spec_json"].as_str().unwrap(),
        spec.json,
        "stored byte for byte"
    );
    assert_eq!(rows[0]["hash"].as_str().unwrap(), spec.hash);
    let back: push_core::PushConfig =
        serde_json::from_str(rows[0]["spec_json"].as_str().unwrap()).unwrap();
    assert_eq!(back, cfg);

    // The shape `read_stored_hash` parses.
    let out = run(
        &db,
        &format!("SELECT hash FROM {};", push_sync::CONFIG_RECORD),
    )
    .await;
    assert_eq!(out[0], json!([{ "hash": spec.hash }]));
}

fn push_sync_ddl(transport: SyncTransport) -> String {
    crate::schema_builder::push_tables_sql(&sync_config(transport))
}

/// Under the changefeed transport the feed carries a direct push as a CREATE
/// (`{"update": row}`), the whole stored row, record id and datetimes as
/// strings, unset optional fields absent. This is what the scheduler's
/// changefeed sink hands `ingest_event` (op `CREATE`, `id` = the record id).
#[tokio::test]
async fn the_feed_carries_a_direct_push_as_a_create() {
    let db = db().await;
    let ddl = push_sync_ddl(SyncTransport::Changefeed);
    run(&db, &ddl).await;
    run(&db, &crate::sp00ky::push_message_events(false)).await;

    let id = create(&db, &message(&["user:u1"], Some("Hi"), None)).await;
    let out = run(&db, "SHOW CHANGES FOR DATABASE SINCE 0 LIMIT 100;").await;
    let entries = out[0].as_array().expect("changes");
    let row = entries
        .iter()
        .flat_map(|e| e["changes"].as_array().cloned().unwrap_or_default())
        .find_map(|c| {
            let row = c.get("update")?;
            (row.get("id")?.as_str()? == id).then(|| row.clone())
        })
        .unwrap_or_else(|| panic!("no CREATE for {id} in the feed: {out:?}"));
    assert_eq!(row["to"], json!(["user:u1"]));
    assert_eq!(row["status"], json!("pending"));
    assert_eq!(row["notification"]["title"], json!("Hi"));
    assert_eq!(row["data"], json!({ "k": 1 }));
    assert_eq!(row["ttl"], json!(3600));
    assert_eq!(row["urgency"], json!("high"));
    assert!(row["created_at"].is_string(), "{row}");
    assert!(
        row.get("send_at").is_none_or(Value::is_null),
        "unset stays unset: {row}"
    );
}

/// Under the http transport the event posts the stored row. The post itself is
/// swapped for a capture table here (no network in the test), everything else
/// is the shipped event.
#[tokio::test]
async fn the_http_event_posts_the_stored_row() {
    // The shipped event, post and TIMEOUT included, is valid v3 DDL.
    let plain = db().await;
    run(&plain, &push_sync_ddl(SyncTransport::Http)).await;
    run(&plain, &crate::sp00ky::push_message_events(true)).await;

    let db = db().await;
    run(&db, &push_sync_ddl(SyncTransport::Http)).await;
    let event = crate::sp00ky::push_message_events(true);
    let post = event
        .lines()
        .find(|l| l.contains("http::post($sp00ky_endpoint + '/ingest'"))
        .expect("the event posts to /ingest");
    let captured = event.replace(post, "    CREATE _capture CONTENT { payload: $payload };");
    assert_ne!(captured, event);
    run(&db, &captured).await;

    let now_id = create(&db, &message(&["user:u1", "user:u2"], Some("Hi"), None)).await;
    let later_id = create(
        &db,
        &message(&["user:u1"], None, Some("2099-01-01T00:00:00Z")),
    )
    .await;
    // Engine-side writes never post.
    run(
        &db,
        &format!("UPDATE {now_id} SET status = 'sent', delivered = 2, sent_at = time::now();"),
    )
    .await;

    let out = run(&db, "SELECT VALUE payload FROM _capture;").await;
    let payloads = out[0].as_array().unwrap();
    assert_eq!(
        payloads.len(),
        2,
        "one post per CREATE, none for the UPDATE: {payloads:?}"
    );
    let by_id = |id: &str| {
        payloads
            .iter()
            .find(|p| p["id"] == json!(id))
            .cloned()
            .unwrap()
    };

    let now = by_id(&now_id);
    assert_eq!(now["table"], json!("_00_push_message"));
    assert_eq!(now["op"], json!("CREATE"));
    let r = &now["record"];
    assert_eq!(r["id"], json!(now_id));
    assert_eq!(r["to"], json!(["user:u1", "user:u2"]));
    assert_eq!(r["notification"]["title"], json!("Hi"));
    assert_eq!(r["status"], json!("pending"));
    assert!(r["created_at"].is_string(), "{r}");
    assert!(
        r.get("send_at").is_none_or(Value::is_null),
        "unset send_at is not a string: {r}"
    );
    assert!(r.get("notification").is_some());

    let later = by_id(&later_id);
    let r = &later["record"];
    assert!(
        r.get("notification").is_none_or(Value::is_null),
        "a nudge: {r}"
    );
    assert!(
        r["send_at"]
            .as_str()
            .is_some_and(|s| s.starts_with("2099-01-01T00:00:00")),
        "send_at as a string: {r}"
    );
}

#[tokio::test]
async fn status_devices_and_send_read_the_real_tables() {
    let db = db().await;
    run(&db, &push_sync_ddl(SyncTransport::Http)).await;
    run(
        &db,
        "DEFINE PARAM OVERWRITE $sp00ky_vapid_public_key VALUE 'BPk' PERMISSIONS FULL;
         DEFINE PARAM OVERWRITE $sp00ky_vapid_kid VALUE 'k1' PERMISSIONS FULL;
         CREATE _00_push_subscription:a SET auth_id = 'user:u1', endpoint = 'https://fcm.googleapis.com/fcm/send/a', p256dh = 'P', auth = 'A', kid = 'k1', label = 'Laptop';
         CREATE _00_push_subscription:b SET auth_id = 'user:u1', endpoint = 'https://updates.push.services.mozilla.com/b', p256dh = 'P', auth = 'A', kid = 'old', last_ok_at = time::now();
         CREATE _00_push_subscription:c SET auth_id = 'user:u2', endpoint = 'https://web.push.apple.com/c', p256dh = 'P', auth = 'A', kid = 'k1', disabled_at = time::now(), disabled_reason = 'http_403: gone';
         CREATE _00_push_message SET to = ['user:u1'], status = 'failed', error = 'boom';",
    )
    .await;
    let pending = create(&db, &message(&["user:u1"], Some("Hi"), None)).await;
    create(
        &db,
        &message(&["user:u1"], None, Some("2099-01-01T00:00:00Z")),
    )
    .await;

    let s = push_cmd::parse_status(&run(&db, push_cmd::STATUS_SQL).await);
    assert!(
        s.config.is_none() && s.config_error.is_none(),
        "no config row yet"
    );
    assert_eq!(s.public_key.as_deref(), Some("BPk"));
    assert_eq!(s.kid.as_deref(), Some("k1"));
    assert_eq!(
        (s.subscriptions, s.disabled, s.stale, s.users),
        (3, 1, 1, 1)
    );
    assert_eq!(s.messages.get("pending"), Some(&2));
    assert_eq!(s.messages.get("failed"), Some(&1));
    assert_eq!(s.scheduled, 1);
    assert_eq!(s.recent_failures[0]["error"], json!("boom"));
    assert!(s.recent_failures[0]["id"]
        .as_str()
        .unwrap()
        .starts_with("_00_push_message:"));

    // With a stored config, status reads it back.
    let spec =
        push_sync::spec_of(&serde_yaml::from_str("rules: { r: { table: m, to: u } }").unwrap())
            .unwrap();
    run(&db, &push_sync::upsert_sql(&spec)).await;
    let s = push_cmd::parse_status(&run(&db, push_cmd::STATUS_SQL).await);
    assert_eq!(s.config.unwrap().rules.len(), 1);
    assert!(s.config_updated_at.unwrap().starts_with("20"));

    let rows = run(&db, &push_cmd::devices_sql("user:u1")).await[0]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert!(
            row.get("p256dh").is_none() && row.get("auth").is_none(),
            "no keys: {row}"
        );
        assert!(
            row.get("disabled_at").is_none_or(Value::is_null),
            "NONE, not \"NONE\": {row}"
        );
    }
    let a = rows
        .iter()
        .find(|r| r["id"] == json!("_00_push_subscription:a"))
        .unwrap();
    let b = rows
        .iter()
        .find(|r| r["id"] == json!("_00_push_subscription:b"))
        .unwrap();
    assert_eq!(a["current"], json!(true));
    assert_eq!(b["current"], json!(false));
    assert!(a.get("last_ok_at").is_none_or(Value::is_null), "{a}");
    assert!(b["last_ok_at"].is_string(), "{b}");
    assert_eq!(a["label"], json!("Laptop"));

    let state = run(&db, &push_cmd::message_state_sql(&pending).unwrap()).await;
    assert_eq!(state[0][0]["status"], json!("pending"));
}

/// What `sync_native` writes, run for real: the credential survives the
/// quoting byte for byte, `fn::push::info()` reads the param, and `spky push
/// status` / `devices` see the native rows.
#[tokio::test]
async fn native_sync_output_runs_and_reads_back() {
    let db = db().await;
    run(&db, &push_sync_ddl(SyncTransport::Http)).await;
    let cfg: push_core::PushConfig = serde_yaml::from_str(
        r#"
apns: { teamId: ABCDE12345, keyId: XYZ987WVUT, key: { vault: K }, bundleIds: [im.app] }
fcm:
  serviceAccount: { vault: S }
  android: { projectId: sp00ky-test, appId: "1:42:android:ab", apiKey: "AIza'x", senderId: "42" }
"#,
    )
    .unwrap();
    run(&db, &push_sync::native_param_sql(&cfg, true, false)).await;
    let secret = "{\"key\":\"-----BEGIN PRIVATE KEY-----\\nab'c\\\\d\\n-----END PRIVATE KEY-----\"}";
    run(
        &db,
        &format!(
            "UPSERT _00_push_credential:apns SET secret = {}, hash = 'h';",
            push_sync::surql_string(secret)
        ),
    )
    .await;
    let stored = run(&db, "SELECT VALUE secret FROM ONLY _00_push_credential:apns").await;
    assert_eq!(stored[0], json!(secret));

    let info = run(&db, "RETURN fn::push::info()").await;
    assert_eq!(info[0]["providers"], json!(["apns"]));
    assert_eq!(info[0]["android"], Value::Null, "fcm is not usable, so no client config");
    run(&db, &push_sync::native_param_sql(&cfg, true, true)).await;
    let info = run(&db, "RETURN fn::push::info()").await;
    assert_eq!(info[0]["providers"], json!(["apns", "fcm"]));
    assert_eq!(info[0]["android"]["apiKey"], json!("AIza'x"));

    run(
        &db,
        "CREATE _00_push_subscription:n SET auth_id = 'user:u1', kind = 'apns', endpoint = 'apns:ab', token = 'ab', app_id = 'im.app', environment = 'sandbox'; \
         CREATE _00_push_subscription:w SET auth_id = 'user:u1', endpoint = 'https://p.example/x', p256dh = 'p', auth = 'a', kid = 'old';",
    )
    .await;
    let s = push_cmd::parse_status(&run(&db, push_cmd::STATUS_SQL).await);
    assert_eq!(s.credentials, vec!["apns".to_string()]);
    assert_eq!(s.by_kind, [1, 1, 0]);
    assert_eq!(s.stale, 1, "only the web row can be on an old key");
    assert_eq!(s.native["bundleIds"], json!(["im.app"]));

    let rows = run(&db, &push_cmd::devices_sql("user:u1")).await[0].clone();
    let native = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == json!("apns"))
        .unwrap()
        .clone();
    assert_eq!(native["current"], json!(true));
    assert_eq!(native["environment"], json!("sandbox"));
    let web = rows.as_array().unwrap().iter().find(|r| r["kind"] == json!("web")).unwrap().clone();
    assert_eq!(web["current"], json!(false));
}
