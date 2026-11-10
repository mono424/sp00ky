//! PORT: `packages/spooky_core/test/client/channel_client_test.dart`. Two
//! local-only engines on one thread stand in for two devices, joined by the
//! in-memory transport; a local-only engine has no auth, so each test tells
//! its channels who it is.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;

use super::*;
use crate::client::engine::QueryOptions;
use crate::modules::channel::tests::MemoryNet;
use crate::modules::channel::{
    id_key, ChannelMessage, ChannelStatus, ChannelTransport, ChannelsConfig, PeerLinkStatus,
    SignalKind, TransportHooks,
};
use crate::testing::{row, run_local};
use crate::types::{ColumnSchema, DatabaseConfig, PersistenceConfig, Schema, Sp00kyConfig};
use crate::value::RecordId;

const SURQL: &str = "DEFINE TABLE msg SCHEMAFULL PERMISSIONS FULL;\n\
DEFINE FIELD convo ON msg TYPE string;\n\
DEFINE FIELD text ON msg TYPE string;\n\
DEFINE FIELD mood ON msg TYPE option<string>;\n";

fn schema() -> Schema {
    let mut s = Schema::default().with_table(
        "msg",
        [
            ("convo", ColumnSchema::of_type("string")),
            ("text", ColumnSchema::of_type("string")),
            (
                "mood",
                ColumnSchema {
                    optional: true,
                    ephemeral: true,
                    ..ColumnSchema::of_type("string")
                },
            ),
        ],
    );
    s.tables.get_mut("msg").unwrap().channel = true;
    s
}

fn config(channels: ChannelsConfig) -> Sp00kyConfig {
    let mut c = Sp00kyConfig::new(DatabaseConfig::new("test", "test"), schema(), SURQL);
    c.persistence = PersistenceConfig::Memory;
    c.channels = Some(channels);
    c
}

/// Polls `read` until `ok` holds; the runtime materializes on its own timers.
async fn eventually<T: std::fmt::Debug>(read: impl Fn() -> T, ok: impl Fn(&T) -> bool) -> T {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let v = read();
        if ok(&v) {
            return v;
        }
        assert!(
            Instant::now() < until,
            "condition not met; last value: {v:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn find(rows: &[Row], id: &str) -> Option<Row> {
    rows.iter()
        .find(|r| r.get("id").and_then(id_key).as_deref() == Some(id))
        .cloned()
}

struct Device {
    engine: Rc<Engine>,
    rows: Rc<RefCell<Arc<Vec<Row>>>>,
    _sub: Subscription,
}

impl Device {
    async fn open(user: &str, net: &MemoryNet) -> Device {
        let channels = ChannelsConfig {
            transport: Some(net.config_factory()),
            ..Default::default()
        }
        .accept("msg", |op| {
            op.patch.get("text") != Some(&Value::from("spam"))
        });
        let engine = Engine::open(config(channels)).await.unwrap();
        engine.channel_manager().unwrap().set_user(Some(user));
        let rows: Rc<RefCell<Arc<Vec<Row>>>> = Rc::default();
        let sub = subscribe_all(&engine, &rows).await;
        Device {
            engine,
            rows,
            _sub: sub,
        }
    }

    fn row(&self, id: &str) -> Option<Row> {
        find(&self.rows.borrow(), id)
    }

    async fn close(&self) {
        self.engine.close().await;
    }
}

async fn subscribe_all(engine: &Rc<Engine>, into: &Rc<RefCell<Arc<Vec<Row>>>>) -> Subscription {
    let hash = engine
        .query_raw("SELECT * FROM msg", Object::new(), QueryOptions::default())
        .await
        .unwrap();
    let r = into.clone();
    engine.subscribe(&hash, move |rows| *r.borrow_mut() = rows.clone(), true)
}

fn data(v: serde_json::Value) -> Object {
    row(v)
}

async fn connect(a: &Device, b: &Device) -> (Sp00kyChannel, Sp00kyChannel) {
    let ca = a
        .engine
        .channel(&[a.engine.query("msg").where_eq("convo", "c1")], ["user:b"])
        .unwrap();
    let cb = b
        .engine
        .channel(
            &[b.engine.query("msg").where_eq("convo", "c1")],
            [Value::RecordId(RecordId::new("user", "a"))],
        )
        .unwrap();
    eventually(|| ca.state().status, |s| *s == ChannelStatus::Connected).await;
    eventually(|| cb.state().status, |s| *s == ChannelStatus::Connected).await;
    assert_eq!(ca.state().peers, vec!["user:b".to_string()]);
    (ca, cb)
}

async fn two() -> (MemoryNet, Device, Device) {
    let net = MemoryNet::new();
    let a = Device::open("user:a", &net).await;
    let b = Device::open("user:b", &net).await;
    (net, a, b)
}

#[tokio::test]
async fn peer_rows_and_signals_reach_the_engine() {
    run_local(async {
        let engine = Engine::open(config(ChannelsConfig::default().accept("msg", |_| true)))
            .await
            .unwrap();
        let rows: Rc<RefCell<Arc<Vec<Row>>>> = Rc::default();
        let _sub = subscribe_all(&engine, &rows).await;

        assert!(engine
            .peer_create("msg:1", data(json!({"convo": "c1", "text": "hi"})), 60_000)
            .await
            .unwrap());
        // One unconfirmed peer write per row.
        assert!(!engine
            .peer_create("msg:1", data(json!({"convo": "c1", "text": "x"})), 60_000)
            .await
            .unwrap());
        let stored = engine.read_row("msg:1").await.unwrap().unwrap();
        assert_eq!(stored["text"], Value::from("hi"));
        eventually(|| find(&rows.borrow(), "msg:1"), Option::is_some).await;
        assert_eq!(engine.pending_mutation_count(), 0);

        engine
            .create("msg:2", data(json!({"convo": "c1", "text": "own"})))
            .await
            .unwrap();
        assert!(engine.peer_delete("msg:2", 60_000).await.unwrap());
        eventually(|| find(&rows.borrow(), "msg:2"), Option::is_none).await;

        // No server to signal through.
        let sent = engine
            .channel_signal()
            .send(OutgoingSignal {
                to: "user:b".into(),
                to_peer: None,
                from_peer: "p".into(),
                kind: SignalKind::Hello,
                payload: None,
            })
            .await;
        assert!(sent.is_err());
        engine
            .channel_signal()
            .listen(&PeerRef::new("user:a", "p"), Rc::new(|_| {}))();
        engine.close().await;
    })
    .await;
}

#[tokio::test]
async fn a_create_reaches_the_peer_store_and_circuit_ephemeral_fields_only_its_overlay() {
    run_local(async {
        let (_net, a, b) = two().await;
        connect(&a, &b).await;
        let out = a
            .engine
            .create(
                "msg:1",
                data(json!({"convo": "c1", "text": "hi", "mood": "happy"})),
            )
            .await
            .unwrap();
        // The caller sees what it wrote.
        assert_eq!(out["mood"], Value::from("happy"));
        assert_eq!(a.engine.pending_mutation_count(), 1);

        let seen = eventually(
            || b.row("msg:1"),
            |r| r.as_ref().is_some_and(|r| r.contains_key("mood")),
        )
        .await;
        let seen = seen.unwrap();
        assert_eq!(seen["text"], Value::from("hi"));
        assert_eq!(seen["mood"], Value::from("happy"));
        // A peer's create never enters our outbox.
        assert_eq!(b.engine.pending_mutation_count(), 0);
        let stored = b.engine.read_row("msg:1").await.unwrap().unwrap();
        assert_eq!(stored["text"], Value::from("hi"));
        // Ephemeral values never reach a store.
        assert!(
            stored.get("mood").is_none_or(Value::is_nullish),
            "{stored:?}"
        );
        assert!(b.engine.state().peer_writes.contains_key("msg:1"));
        a.close().await;
        b.close().await;
    })
    .await;
}

#[tokio::test]
async fn an_ephemeral_update_is_peers_only_a_persistent_one_shows_before_the_server_has_it() {
    run_local(async {
        let (_net, a, b) = two().await;
        connect(&a, &b).await;
        a.engine
            .create("msg:1", data(json!({"convo": "c1", "text": "hi"})))
            .await
            .unwrap();
        eventually(|| b.row("msg:1"), Option::is_some).await;
        eventually(|| a.row("msg:1"), Option::is_some).await;
        let pending = a.engine.pending_mutation_count();

        let out = a
            .engine
            .update(
                "msg:1",
                data(json!({"mood": "typing"})),
                UpdateOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(out["mood"], Value::from("typing"));
        // An ephemeral-only update writes nothing.
        assert_eq!(a.engine.pending_mutation_count(), pending);
        eventually(
            || b.row("msg:1").and_then(|r| r.get("mood").cloned()),
            |m| *m == Some(Value::from("typing")),
        )
        .await;
        // Own ephemeral values show locally too.
        assert_eq!(a.row("msg:1").unwrap()["mood"], Value::from("typing"));

        a.engine
            .update(
                "msg:1",
                data(json!({"text": "edited"})),
                UpdateOptions::default(),
            )
            .await
            .unwrap();
        eventually(
            || b.row("msg:1").and_then(|r| r.get("text").cloned()),
            |t| *t == Some(Value::from("edited")),
        )
        .await;
        a.close().await;
        b.close().await;
    })
    .await;
}

#[tokio::test]
async fn a_delete_hides_the_row_on_the_peer() {
    run_local(async {
        let (_net, a, b) = two().await;
        connect(&a, &b).await;
        a.engine
            .create("msg:1", data(json!({"convo": "c1", "text": "hi"})))
            .await
            .unwrap();
        eventually(|| b.row("msg:1"), Option::is_some).await;
        a.engine.delete("msg:1").await.unwrap();
        eventually(|| b.row("msg:1"), Option::is_none).await;
        a.close().await;
        b.close().await;
    })
    .await;
}

#[tokio::test]
async fn rows_outside_the_filter_and_refused_rows_stay_home() {
    run_local(async {
        let (_net, a, b) = two().await;
        let (ca, _cb) = connect(&a, &b).await;
        a.engine
            .create("msg:2", data(json!({"convo": "c2", "text": "elsewhere"})))
            .await
            .unwrap();
        a.engine
            .create("msg:3", data(json!({"convo": "c1", "text": "spam"})))
            .await
            .unwrap();
        a.engine
            .create("msg:4", data(json!({"convo": "c1", "text": "ok"})))
            .await
            .unwrap();
        eventually(|| b.row("msg:4"), Option::is_some).await;
        assert!(b.row("msg:2").is_none());
        // The accept rule refused it.
        assert!(b.row("msg:3").is_none());

        let err = ca
            .update("note:1", Object::new(), UpdateOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
        ca.create("msg:5", data(json!({"convo": "c1", "text": "scoped"})))
            .await
            .unwrap();
        eventually(|| b.row("msg:5"), Option::is_some).await;
        ca.close();
        assert!(ca.is_closed());
        assert!(ca.create("msg:6", Object::new()).await.is_err());
        a.close().await;
        b.close().await;
    })
    .await;
}

#[tokio::test]
async fn a_channel_needs_a_channel_table() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::open("user:a", &net).await;
        let err = a
            .engine
            .channel(&[a.engine.query("nope")], ["user:b"])
            .err()
            .unwrap();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
        assert!(a.engine.channel(&[], ["user:b"]).is_err());
        a.close().await;
    })
    .await;
}

// ---- the thread-safe client -----------------------------------------------------------------

/// A transport that connects every wanted user at once and records what it
/// sends. `Send + Sync` in its factory only, like any app transport.
struct Recorder {
    hooks: Rc<dyn TransportHooks>,
    sent: Arc<Mutex<Vec<String>>>,
    up: RefCell<BTreeSet<String>>,
}

impl ChannelTransport for Recorder {
    fn supported(&self) -> bool {
        true
    }

    fn want(&self, users: &BTreeSet<String>) {
        let new: Vec<String> = users.difference(&self.up.borrow()).cloned().collect();
        *self.up.borrow_mut() = users.clone();
        for user in new {
            self.hooks.on_peer(&PeerRef::new(user.clone(), "x"), true);
            self.hooks.on_status(&user, PeerLinkStatus::Connected);
        }
    }

    fn send(&self, _to: &PeerRef, message: &ChannelMessage) {
        self.sent
            .lock()
            .unwrap()
            .push(message.to_json().to_string());
    }

    fn close(&self) {}
}

#[tokio::test]
async fn the_client_opens_a_channel_and_writes_to_its_peers() {
    let sent: Arc<Mutex<Vec<String>>> = Arc::default();
    let s = sent.clone();
    let channels = ChannelsConfig {
        transport: Some(Arc::new(move |hooks| {
            Rc::new(Recorder {
                hooks,
                sent: s.clone(),
                up: RefCell::default(),
            }) as Rc<dyn ChannelTransport>
        })),
        ..Default::default()
    };
    let client = Sp00kyClient::open(config(channels)).await.unwrap();
    client
        .call(|w| async move {
            w.engine.channel_manager().unwrap().set_user(Some("user:a"));
            Ok(())
        })
        .await
        .unwrap();

    let err = client
        .channel(&[client.query("nope")], ["user:b"])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");

    let mut ch = client
        .channel(&[client.query("msg").where_eq("convo", "c1")], ["user:b"])
        .await
        .unwrap();
    assert_eq!(ch.tables(), ["msg".to_string()]);
    assert_eq!(ch.state().status, ChannelStatus::Connected);
    assert_eq!(ch.state().peers, vec!["user:b".to_string()]);

    let out = ch
        .create(
            "msg:1",
            data(json!({"convo": "c1", "text": "hi", "mood": "happy"})),
        )
        .await
        .unwrap();
    assert_eq!(out["mood"], Value::from("happy"));
    {
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains(r#""kind":"create""#), "{}", sent[0]);
        assert!(sent[0].contains(r#""mood":"happy""#), "{}", sent[0]);
    }
    // The ephemeral field never reached the store.
    let stored = client.read_row("msg:1").await.unwrap().unwrap();
    assert!(
        stored.get("mood").is_none_or(Value::is_nullish),
        "{stored:?}"
    );
    assert!(ch.update("note:1", Object::new()).await.is_err());

    ch.set_peers(["user:b", "user:c"]);
    let state = tokio::time::timeout(Duration::from_secs(5), ch.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        state.peers,
        vec!["user:b".to_string(), "user:c".to_string()]
    );

    ch.set_queries(&[client.query("msg").where_eq("convo", "c2")])
        .await
        .unwrap();
    ch.create("msg:2", data(json!({"convo": "c1", "text": "elsewhere"})))
        .await
        .unwrap();
    // user:c got the own ephemeral `mood` of msg:1 when it connected; msg:2
    // went nowhere, c1 is no longer this channel's filter.
    {
        let sent = sent.lock().unwrap();
        assert!(sent
            .iter()
            .any(|m| m.contains(r#""id":"msg:1""#) && m.contains(r#""mood":"happy""#)));
        assert!(!sent.iter().any(|m| m.contains("msg:2")), "{sent:?}");
    }

    ch.close();
    assert!(ch.is_closed());
    assert!(ch
        .create("msg:3", data(json!({"convo": "c2", "text": "x"})))
        .await
        .is_err());

    // Peer writes cross the thread boundary too.
    assert!(client
        .peer_create(
            "msg:9",
            data(json!({"convo": "c1", "text": "peer"})),
            60_000
        )
        .await
        .unwrap());
    assert!(client.peer_delete("msg:9", 60_000).await.unwrap());
    client.close().await.unwrap();
}
