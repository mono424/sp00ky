//! Sharing writes between the devices of one user, through the REAL room
//! relay (`sp00ky-relay`, `apps/relay`) on a local port.
//!
//! Every client here is a whole local-only engine (its own store and
//! circuit), told who it is and which token to show the relay; the relay
//! checks tokens with a stand-in for the database. One member of the room is
//! a bare WebSocket that behaves like a browser holding two tabs, saying and
//! expecting exactly the frames `packages/core` puts on the wire, which is
//! what keeps a Rust client and a browser of one user in one conversation.
#![cfg(feature = "remote")]

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value as Json};
use sp00ky_relay::{Config, Relay, Verifier, VerifyError};
use spooky_core::modules::channel::{
    ChannelRelayConfig, ChannelState, ChannelStatus, ChannelsConfig,
};
use spooky_core::modules::query_builder::QueryBuilder;
use spooky_core::runtime::Subscription;
use spooky_core::value::object_from_json;
use spooky_core::{
    ColumnSchema, DatabaseConfig, Engine, PersistenceConfig, Row, Schema, Sp00kyConfig,
    UpdateOptions, Value,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// How long a test waits for something that should happen at once.
const WAIT: Duration = Duration::from_secs(10);

// ---- the relay -----------------------------------------------------------------------------

/// Token `t-<name>` is user `user:<name>`, anything else is nobody.
struct Tokens;

#[async_trait::async_trait]
impl Verifier for Tokens {
    async fn verify(&self, token: &str) -> Result<String, VerifyError> {
        match token.strip_prefix("t-") {
            Some(name) if !name.is_empty() => Ok(format!("user:{name}")),
            _ => Err(VerifyError::Unauthorized),
        }
    }
}

/// Timers short enough that a client that did not answer the relay's pings
/// would be thrown out within a test.
fn quick() -> Config {
    Config {
        ping_interval: Duration::from_millis(200),
        idle_timeout: Duration::from_secs(1),
        ..Config::default()
    }
}

/// A relay on a free local port: its address, and a handle to look into it.
/// `Relay::new(..).router()` is what `sp00ky_relay::app` serves.
async fn start(config: Config) -> (String, Relay) {
    let relay = Relay::new(config, Arc::new(Tokens));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let router = relay.router();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("ws://{addr}/room"), relay)
}

// ---- a client ------------------------------------------------------------------------------

const SURQL: &str = "DEFINE TABLE note SCHEMAFULL PERMISSIONS FULL;\n\
DEFINE FIELD folder ON note TYPE string;\n\
DEFINE FIELD title ON note TYPE string;\n";

fn schema() -> Schema {
    Schema::default().with_table(
        "note",
        [
            ("folder", ColumnSchema::of_type("string")),
            ("title", ColumnSchema::of_type("string")),
        ],
    )
}

/// Polls `read` until `ok` holds.
async fn eventually<T: std::fmt::Debug>(read: impl Fn() -> T, ok: impl Fn(&T) -> bool) -> T {
    let until = tokio::time::Instant::now() + WAIT;
    loop {
        let v = read();
        if ok(&v) {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "condition not met; last value: {v:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn data(v: Json) -> Row {
    object_from_json(v)
}

/// One live query of a device, as a screen holds it.
struct Screen {
    rows: Rc<RefCell<Arc<Vec<Row>>>>,
    _sub: Subscription,
}

impl Screen {
    fn row(&self, id: &str) -> Option<Row> {
        let found = |r: &&Row| r.get("id").map(Value::to_id_string).as_deref() == Some(id);
        self.rows.borrow().iter().find(found).cloned()
    }

    fn title(&self, id: &str) -> Option<String> {
        let row = self.row(id)?;
        row.get("title").and_then(Value::as_str).map(str::to_string)
    }
}

/// One device of `user:<name>`: an engine that shares `note` through the
/// relay at `url`.
struct Device {
    engine: Rc<Engine>,
}

impl Device {
    async fn open(url: &str, name: &str) -> Device {
        let token = format!("t-{name}");
        let channels = ChannelsConfig {
            // No server here to confirm a peer's write, so nothing may expire
            // under a test.
            confirm_timeout_ms: 600_000,
            ..Default::default()
        }
        .share("note")
        .relay(ChannelRelayConfig::new(url).token(move || Some(token.clone())));
        let mut config = Sp00kyConfig::new(DatabaseConfig::new("test", "test"), schema(), SURQL);
        config.persistence = PersistenceConfig::Memory;
        config.channels = Some(channels);
        let engine = Engine::open(config).await.unwrap();
        // A local-only engine has no auth: tell its channels who it is.
        let user = format!("user:{name}");
        engine.channel_manager().unwrap().set_user(Some(&user));
        Device { engine }
    }

    /// This device's id in the room, and the one tab it speaks for.
    fn peer(&self) -> String {
        self.engine.channel_manager().unwrap().peer_id().to_string()
    }

    fn notes(&self) -> QueryBuilder {
        self.engine.query("note")
    }

    async fn show(&self, query: QueryBuilder) -> Screen {
        let rows: Rc<RefCell<Arc<Vec<Row>>>> = Rc::default();
        let into = rows.clone();
        let sub = query
            .subscribe("10m", move |next| *into.borrow_mut() = next.clone())
            .await
            .unwrap();
        Screen { rows, _sub: sub }
    }

    fn share(&self) -> ChannelState {
        self.engine.share_state()
    }

    async fn connected(&self) {
        eventually(|| self.share(), |s| s.status == ChannelStatus::Connected).await;
    }

    /// How many of the user's other devices said what they show.
    fn told(&self) -> usize {
        let manager = self.engine.channel_manager().unwrap();
        manager.peer_interests().len()
    }

    async fn create(&self, id: &str, folder: &str, title: &str) {
        let note = data(json!({"folder": folder, "title": title}));
        self.engine.create(id, note).await.unwrap();
    }

    async fn retitle(&self, id: &str, title: &str) {
        let patch = data(json!({"title": title}));
        self.engine
            .update(id, patch, UpdateOptions::default())
            .await
            .unwrap();
    }

    async fn stored(&self, id: &str) -> Option<Row> {
        self.engine.read_row(id).await.unwrap()
    }
}

async fn local<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::LocalSet::new().run_until(f).await
}

// ---- a browser, as the relay sees one ---------------------------------------------------------

/// A member of the room that is not this crate: a socket that says hello and
/// then whatever the test tells it to. Reading goes on in the background, as
/// in a browser, so the relay's pings are answered.
struct Hub {
    out: mpsc::UnboundedSender<Json>,
    frames: mpsc::UnboundedReceiver<Json>,
    welcome: Json,
}

impl Hub {
    async fn join(url: &str, name: &str, peer: &str) -> Hub {
        let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let (out, mut outbound) = mpsc::unbounded_channel::<Json>();
        let (inbound, frames) = mpsc::unbounded_channel::<Json>();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    next = outbound.recv() => match next {
                        Some(frame) => socket.send(Message::text(frame.to_string())).await.unwrap(),
                        None => break,
                    },
                    frame = socket.next() => match frame {
                        Some(Ok(Message::Text(text))) => {
                            let _ = inbound.send(serde_json::from_str(text.as_str()).unwrap());
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        Some(Ok(_)) => {}
                    },
                }
            }
        });
        let hello = json!({"t": "hello", "v": 1, "token": format!("t-{name}"), "peer": peer});
        out.send(hello).unwrap();
        let mut hub = Hub {
            out,
            frames,
            welcome: Json::Null,
        };
        hub.welcome = hub.next().await;
        assert_eq!(hub.welcome["t"], "welcome", "{}", hub.welcome);
        hub
    }

    /// The next frame the relay hands this member.
    async fn next(&mut self) -> Json {
        tokio::time::timeout(WAIT, self.frames.recv())
            .await
            .expect("timed out waiting for a frame")
            .expect("the relay hung up")
    }

    /// The next thing another member said: `(its peer id, what it said)`.
    async fn hear(&mut self) -> (String, Json) {
        loop {
            let frame = self.next().await;
            if frame["t"] == "msg" {
                let from = frame["from"].as_str().unwrap().to_string();
                return (from, frame["d"]["m"].clone());
            }
        }
    }

    /// Say something to the whole room, as one of the hub envelopes.
    fn say(&self, envelope: Json) {
        self.out
            .send(json!({"t": "msg", "d": {"m": envelope}}))
            .unwrap();
    }

    /// A message of one of this browser's tabs.
    fn tab_says(&self, tab: &str, to: Json, message: Json) {
        self.say(json!({"h": "m", "from": tab, "to": to, "m": message}));
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn folder(name: &str) -> Json {
    json!([{"field": "folder", "op": "=", "value": name}])
}

// ---- the tests -----------------------------------------------------------------------------

#[tokio::test]
async fn two_clients_of_one_user_share_a_write_and_another_user_gets_none_of_it() {
    local(async {
        let (url, relay) = start(quick()).await;
        let phone = Device::open(&url, "ann").await;
        let laptop = Device::open(&url, "ann").await;
        let bob = Device::open(&url, "bob").await;
        let phones = phone.show(phone.notes()).await;
        let screen = laptop.show(laptop.notes()).await;
        let bobs = bob.show(bob.notes()).await;
        phone.connected().await;
        laptop.connected().await;
        assert_eq!(phone.share().peers, vec!["user:ann".to_string()]);
        eventually(|| relay.peer_count(), |n| *n == 3).await;
        assert_eq!(relay.room_count(), 2);

        // A create lands in the other device's store, like an own optimistic
        // one, without ever entering its outbox.
        phone.create("note:1", "a", "one").await;
        eventually(|| screen.title("note:1"), |t| t.as_deref() == Some("one")).await;
        let stored = laptop.stored("note:1").await.unwrap();
        assert_eq!(stored["folder"], Value::from("a"));
        assert_eq!(laptop.engine.pending_mutation_count(), 0);
        assert_eq!(phone.engine.pending_mutation_count(), 1);

        // An update shows at once and stays out of the store: the synced row
        // confirms it later.
        phone.retitle("note:1", "two").await;
        eventually(|| screen.title("note:1"), |t| t.as_deref() == Some("two")).await;
        let stored = laptop.stored("note:1").await.unwrap();
        assert_eq!(stored["title"], Value::from("one"));

        // And the other way round.
        laptop.create("note:2", "a", "from the laptop").await;
        eventually(|| phones.title("note:2"), |t| t.is_some()).await;
        laptop.retitle("note:2", "still the laptop").await;
        eventually(
            || phones.title("note:2"),
            |t| t.as_deref() == Some("still the laptop"),
        )
        .await;

        // A delete hides the row.
        phone.engine.delete("note:1").await.unwrap();
        eventually(|| screen.row("note:1"), Option::is_none).await;

        // The relay pinged all of them five times over by now, and would have
        // dropped whoever did not answer.
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert_eq!(relay.peer_count(), 3);
        assert_eq!(laptop.share().status, ChannelStatus::Connected);

        // Another user is in another room: none of it reached him.
        assert!(bobs.rows.borrow().is_empty());
        assert!(bob.stored("note:1").await.is_none());
        assert_eq!(bob.share(), ChannelState::new(ChannelStatus::Connecting));

        // A device that goes is gone for the others.
        laptop.engine.close().await;
        eventually(|| phone.share().status, |s| *s == ChannelStatus::Connecting).await;
        phone.engine.close().await;
        bob.engine.close().await;
        eventually(|| relay.peer_count(), |n| *n == 0).await;
    })
    .await;
}

#[tokio::test]
async fn a_write_is_kept_from_a_client_that_shows_nothing_of_the_row() {
    local(async {
        let (url, _relay) = start(quick()).await;
        let phone = Device::open(&url, "ann").await;
        let laptop = Device::open(&url, "ann").await;
        // The laptop has one folder open, and says so.
        let inbox = laptop
            .show(laptop.notes().where_eq("folder", "inbox"))
            .await;
        phone.connected().await;
        eventually(|| phone.told(), |n| *n == 1).await;

        // Frames of one sender arrive in order: had the note for the archive
        // been sent, it would be there before the one for the inbox.
        phone.create("note:archived", "archive", "old").await;
        phone.create("note:new", "inbox", "new").await;
        eventually(|| inbox.title("note:new"), |t| t.is_some()).await;
        assert!(laptop.stored("note:archived").await.is_none());

        // The same for updates.
        phone.retitle("note:archived", "older").await;
        phone.retitle("note:new", "newer").await;
        eventually(
            || inbox.title("note:new"),
            |t| t.as_deref() == Some("newer"),
        )
        .await;
        let overlay = laptop.engine.channel_manager().unwrap().overlay();
        assert_eq!(overlay.keys().collect::<Vec<_>>(), vec!["note:new"]);

        // It opens the archive: the claim grows, and what is written there
        // from now on arrives.
        let archive = laptop
            .show(laptop.notes().where_eq("folder", "archive"))
            .await;
        let interest = || {
            let manager = phone.engine.channel_manager().unwrap();
            manager.peer_interests().into_values().next()
        };
        let both = spooky_core::modules::channel::decode_interest(&json!({
            "t": "interest",
            "v": {"note": [folder("archive"), folder("inbox")]},
        }));
        eventually(interest, |known| *known == both).await;
        phone.create("note:filed", "archive", "filed").await;
        eventually(|| archive.title("note:filed"), |t| t.is_some()).await;

        // Both screens close: nothing is shown, so nothing is sent.
        drop(inbox);
        drop(archive);
        eventually(interest, |known| {
            *known
                == spooky_core::modules::channel::decode_interest(
                    &json!({"t": "interest", "v": {}}),
                )
        })
        .await;
        phone.create("note:unseen", "inbox", "unseen").await;
        let all = laptop.show(laptop.notes()).await;
        eventually(interest, |known| {
            *known
                == spooky_core::modules::channel::decode_interest(
                    &json!({"t": "interest", "v": {"note": true}}),
                )
        })
        .await;
        phone.create("note:seen", "inbox", "seen").await;
        eventually(|| all.title("note:seen"), |t| t.is_some()).await;
        assert!(laptop.stored("note:unseen").await.is_none());

        phone.engine.close().await;
        laptop.engine.close().await;
    })
    .await;
}

#[tokio::test]
async fn talks_to_a_browser_that_holds_two_tabs() {
    local(async {
        let (url, relay) = start(quick()).await;
        let app = Device::open(&url, "ann").await;
        let me = app.peer();
        app.create("note:1", "a", "one").await;
        let screen = app.show(app.notes().where_eq("folder", "a")).await;
        eventually(|| relay.peer_count(), |n| *n == 1).await;

        let mut hub = Hub::join(&url, "ann", "browser1").await;
        assert_eq!(hub.welcome["peers"], json!([me]));
        // The app says which tabs it speaks for: itself.
        assert_eq!(
            hub.next().await,
            json!({"t": "msg", "from": me, "d": {"m": {"h": "tabs", "tabs": [me]}}})
        );
        assert_eq!(app.share().status, ChannelStatus::Connecting);

        // The browser's two tabs are two peers, and each is told what the app
        // shows, in one frame for both.
        hub.say(json!({"h": "tabs", "tabs": ["t1", "t2"]}));
        assert_eq!(
            hub.hear().await,
            (
                me.clone(),
                json!({
                    "h": "m",
                    "from": me,
                    "to": ["t1", "t2"],
                    "m": {"t": "interest", "v": {"note": [folder("a")]}},
                })
            )
        );
        app.connected().await;
        assert_eq!(app.share().peers, vec!["user:ann".to_string()]);

        // A tab's update and create show in the app.
        let patch = json!({"title": "from the browser"});
        hub.tab_says(
            "t1",
            Json::Null,
            json!({"t": "op", "table": "note", "id": "note:1", "patch": patch, "hlc": [now_ms(), 0, "t1"]}),
        );
        eventually(
            || screen.title("note:1"),
            |t| t.as_deref() == Some("from the browser"),
        )
        .await;
        let made = json!({"folder": "a", "title": "made in a tab"});
        hub.tab_says(
            "t2",
            json!([me]),
            json!({"t": "op", "kind": "create", "table": "note", "id": "note:9", "patch": made, "hlc": [now_ms(), 0, "t2"]}),
        );
        eventually(|| screen.title("note:9"), |t| t.is_some()).await;
        assert!(app.stored("note:9").await.is_some());

        // What does not come from a tab the browser speaks for is not taken,
        // nor is what is addressed to another tab; the signaling of the
        // browser's direct link is left unanswered.
        let forged = json!({"t": "op", "table": "note", "id": "note:1", "patch": {"title": "forged"}, "hlc": [now_ms() + 60_000, 0, "zz"]});
        hub.tab_says("zz", Json::Null, forged.clone());
        hub.tab_says("t1", json!(["elsewhere"]), forged);
        hub.out
            .send(json!({"t": "msg", "d": {"s": {"kind": "hello", "payload": null, "toPeer": null}}}))
            .unwrap();
        // One tab says what it shows; the other says nothing and so gets
        // everything.
        hub.tab_says(
            "t1",
            Json::Null,
            json!({"t": "interest", "v": {"note": [folder("b")]}}),
        );
        eventually(|| app.told(), |n| *n == 1).await;
        assert_eq!(screen.title("note:1").as_deref(), Some("from the browser"));

        // The app's writes, as the browser reads them. An update names no
        // kind and only goes to the tab that may show it.
        app.retitle("note:1", "from the app").await;
        let (from, envelope) = hub.hear().await;
        let hlc = envelope["m"]["hlc"].clone();
        assert_eq!(from, me);
        assert_eq!(
            envelope,
            json!({
                "h": "m",
                "from": me,
                "to": ["t2"],
                "m": {"t": "op", "table": "note", "id": "note:1", "patch": {"title": "from the app"}, "hlc": hlc},
            })
        );
        assert!(hlc[0].as_i64().unwrap() > 1_700_000_000_000);
        assert!(hlc[1].is_i64());
        assert_eq!(hlc[2], json!(me));

        // A create carries the row and reaches the tab that shows its folder
        // too; a delete carries nothing.
        app.create("note:2", "b", "two").await;
        let (_, envelope) = hub.hear().await;
        assert_eq!(envelope["to"], json!(["t1", "t2"]));
        assert_eq!(
            envelope["m"],
            json!({
                "t": "op",
                "kind": "create",
                "table": "note",
                "id": "note:2",
                "patch": {"folder": "b", "title": "two"},
                "hlc": envelope["m"]["hlc"],
            })
        );
        app.engine.delete("note:2").await.unwrap();
        let (_, envelope) = hub.hear().await;
        assert_eq!(envelope["to"], json!(["t1", "t2"]));
        assert_eq!(
            envelope["m"],
            json!({"t": "op", "kind": "delete", "table": "note", "id": "note:2", "patch": {}, "hlc": envelope["m"]["hlc"]})
        );

        // A tab closes: it is no peer anymore, and nothing is addressed to it.
        hub.say(json!({"h": "tabs", "tabs": ["t2"]}));
        eventually(|| app.told(), |n| *n == 0).await;
        app.create("note:3", "b", "three").await;
        let (_, envelope) = hub.hear().await;
        assert_eq!(envelope["to"], json!(["t2"]));
        assert_eq!(envelope["m"]["id"], "note:3");

        // The browser goes, and with it the last of the user's other devices.
        drop(hub);
        eventually(|| app.share().status, |s| *s == ChannelStatus::Connecting).await;
        app.engine.close().await;
    })
    .await;
}

#[tokio::test]
async fn comes_back_with_its_token_when_the_relay_ends_the_session() {
    local(async {
        // A session of one second: the relay says `reauth` and hangs up, over
        // and over.
        let (url, relay) = start(Config {
            session_max: Duration::from_secs(1),
            ..quick()
        })
        .await;
        let phone = Device::open(&url, "ann").await;
        let laptop = Device::open(&url, "ann").await;
        let seen: Rc<RefCell<Vec<ChannelStatus>>> = Rc::default();
        let into = seen.clone();
        let _sub = phone
            .engine
            .subscribe_share_state(move |s| into.borrow_mut().push(s.status));
        // Found each other, lost each other to a `reauth`, found each other
        // again, and never gave up in between.
        let again = [
            ChannelStatus::Connected,
            ChannelStatus::Connecting,
            ChannelStatus::Connected,
        ];
        eventually(
            || seen.borrow().clone(),
            |all| all.windows(3).any(|w| w == again),
        )
        .await;
        assert!(!seen.borrow().contains(&ChannelStatus::Failed));
        assert!(relay.room_count() <= 1);
        phone.engine.close().await;
        laptop.engine.close().await;
    })
    .await;
}
