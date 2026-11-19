//! PORT: `packages/core/src/modules/channel/room.test.ts` (the link) and the
//! parts of `hub.test.ts` a client that speaks for one tab has, against a
//! relay in memory that speaks protocol v1 as `apps/relay` documents it (the
//! `FakeRelay` of `rooms.fixture.ts`). Time is tokio's paused clock.
//! `tests/relay_room.rs` runs the same code against the real relay.

use std::cell::{Cell, RefCell};

use super::*;
use crate::modules::channel::{ChannelMessage, ChannelOpKind, Hlc};
use crate::testing::run_local;

// ---- the relay -----------------------------------------------------------------------------

struct FakeSocket {
    /// Every frame the client sent, parsed.
    sent: RefCell<Vec<Json>>,
    /// The relay's end; `None` once the connection is gone.
    to_client: RefCell<Option<mpsc::Sender<String>>>,
    /// `(user, peer)` once it said hello.
    member: RefCell<Option<(String, String)>>,
    reader: RefCell<Option<JoinHandle<()>>>,
}

impl FakeSocket {
    fn is_open(&self) -> bool {
        self.to_client.borrow().is_some()
    }

    /// The relay's side: a frame for the client.
    fn push(&self, frame: Json) {
        if let Some(tx) = &*self.to_client.borrow() {
            let _ = tx.try_send(frame.to_string());
        }
    }

    fn sent_of(&self, t: &str) -> Vec<Json> {
        let of_kind = |f: &&Json| f["t"] == t;
        self.sent.borrow().iter().filter(of_kind).cloned().collect()
    }
}

#[derive(Default)]
struct RelayState {
    sockets: RefCell<Vec<Rc<FakeSocket>>>,
    rooms: RefCell<BTreeMap<String, BTreeMap<String, Rc<FakeSocket>>>>,
    /// Refuse every connection, as an unreachable relay does.
    down: Cell<bool>,
    /// Answer the next hello with this error code instead of a welcome.
    refuse_with: RefCell<Option<String>>,
    /// Do not answer pings (a dead connection the client has to notice).
    mute: Cell<bool>,
    /// Do not answer a hello either (something that takes sockets and says
    /// nothing).
    silent: Cell<bool>,
    /// Spell the user in the welcome this way, as the database would print it.
    welcome_as: RefCell<Option<String>>,
    /// Frames forwarded between members.
    forwarded: Cell<usize>,
}

#[derive(Clone, Default)]
struct FakeRelay(Rc<RelayState>);

/// `t-<name>` is the session token of `user:<name>`; anything else is refused.
fn user_of(token: Option<&str>) -> Option<String> {
    token?.strip_prefix("t-").map(|name| format!("user:{name}"))
}

impl FakeRelay {
    /// What a room takes as its socket.
    fn connector(&self) -> RoomConnector {
        let relay = self.clone();
        Rc::new(move |_url| {
            let relay = relay.clone();
            async move {
                if relay.0.down.get() {
                    return Err(Error::Network("connection refused".into()));
                }
                let (out, mut from_client) = mpsc::channel::<String>(64);
                let (to_client, frames) = mpsc::channel::<String>(64);
                let socket = Rc::new(FakeSocket {
                    sent: RefCell::default(),
                    to_client: RefCell::new(Some(to_client)),
                    member: RefCell::default(),
                    reader: RefCell::default(),
                });
                relay.0.sockets.borrow_mut().push(socket.clone());
                let (r, s) = (relay.clone(), socket.clone());
                let reader = tokio::task::spawn_local(async move {
                    while let Some(text) = from_client.recv().await {
                        let frame: Json = serde_json::from_str(&text).unwrap();
                        s.sent.borrow_mut().push(frame.clone());
                        r.receive(&s, &frame);
                    }
                    // The client let go of it.
                    r.gone(&s);
                });
                *socket.reader.borrow_mut() = Some(reader);
                Ok(RoomSocket { out, frames })
            }
            .boxed_local()
        })
    }

    fn socket(&self, index: usize) -> Rc<FakeSocket> {
        self.0.sockets.borrow()[index].clone()
    }

    fn socket_count(&self) -> usize {
        self.0.sockets.borrow().len()
    }

    /// The socket `peer` is in a room with.
    fn socket_of(&self, peer: &str) -> Rc<FakeSocket> {
        let sockets = self.0.sockets.borrow();
        let joined = |s: &&Rc<FakeSocket>| s.member.borrow().as_ref().is_some_and(|m| m.1 == peer);
        sockets
            .iter()
            .rev()
            .find(joined)
            .expect("in a room")
            .clone()
    }

    fn members(&self, user: &str) -> Vec<String> {
        let rooms = self.0.rooms.borrow();
        rooms
            .get(user)
            .map(|room| room.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn receive(&self, socket: &Rc<FakeSocket>, frame: &Json) {
        if !socket.is_open() {
            return;
        }
        let member = socket.member.borrow().clone();
        let Some((user, peer)) = member else {
            if self.0.silent.get() {
                return;
            }
            let user = user_of(frame["token"].as_str());
            let peer = frame["peer"].as_str();
            let refusal = self.0.refuse_with.borrow_mut().take().or_else(|| {
                if frame["t"] != "hello" || peer.is_none() {
                    Some("bad_request".into())
                } else if user.is_none() {
                    Some("unauthorized".into())
                } else {
                    None
                }
            });
            let (Some(user), Some(peer), None) = (user, peer, &refusal) else {
                socket.push(json!({"t": "error", "code": refusal}));
                return self.drop_socket(socket);
            };
            let mut rooms = self.0.rooms.borrow_mut();
            let room = rooms.entry(user.clone()).or_default();
            let others: Vec<&String> = room.keys().filter(|p| *p != peer).collect();
            let spelled = self.0.welcome_as.borrow().clone().unwrap_or(user.clone());
            socket.push(json!({"t": "welcome", "user": spelled, "peer": peer, "peers": others}));
            for (other, s) in room.iter() {
                if other != peer {
                    s.push(json!({"t": "join", "peer": peer}));
                }
            }
            room.insert(peer.to_string(), socket.clone());
            *socket.member.borrow_mut() = Some((user, peer.to_string()));
            return;
        };
        match frame["t"].as_str() {
            Some("ping") if !self.0.mute.get() => socket.push(json!({"t": "pong"})),
            Some("msg") => {
                let to = frame["to"].as_array();
                let rooms = self.0.rooms.borrow();
                for (other, s) in rooms.get(&user).into_iter().flatten() {
                    if *other == peer || to.is_some_and(|to| !to.iter().any(|p| p == other)) {
                        continue;
                    }
                    self.0.forwarded.set(self.0.forwarded.get() + 1);
                    s.push(json!({"t": "msg", "from": peer, "d": frame["d"]}));
                }
            }
            _ => {}
        }
    }

    /// The connection is over: the others hear it.
    fn gone(&self, socket: &Rc<FakeSocket>) {
        socket.to_client.borrow_mut().take();
        let Some((user, peer)) = socket.member.borrow_mut().take() else {
            return;
        };
        let mut rooms = self.0.rooms.borrow_mut();
        let Some(room) = rooms.get_mut(&user) else {
            return;
        };
        if !room.get(&peer).is_some_and(|s| Rc::ptr_eq(s, socket)) {
            return;
        }
        room.remove(&peer);
        for other in room.values() {
            other.push(json!({"t": "leave", "peer": peer}));
        }
        if room.is_empty() {
            rooms.remove(&user);
        }
    }

    /// The connection ends from the relay's side.
    fn drop_socket(&self, socket: &Rc<FakeSocket>) {
        if let Some(reader) = socket.reader.borrow_mut().take() {
            reader.abort();
        }
        self.gone(socket);
    }

    /// The relay tells a member something and hangs up (`reauth`, ...).
    fn kick(&self, peer: &str, code: &str) {
        let socket = self.socket_of(peer);
        socket.push(json!({"t": "error", "code": code}));
        self.drop_socket(&socket);
    }
}

// ---- helpers -------------------------------------------------------------------------------

use PeerLinkStatus::{Connected, Connecting, Failed};

/// Let everything in flight land; virtual time moves by `ms`.
async fn advance(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

async fn settle() {
    advance(1).await;
}

type Warnings = Rc<RefCell<Vec<String>>>;

fn options(
    relay: &FakeRelay,
    token: &Rc<RefCell<Option<String>>>,
    warned: &Warnings,
) -> RoomOptions {
    let (token, warned) = (token.clone(), warned.clone());
    RoomOptions {
        connect: relay.connector(),
        logger: Some(Rc::new(move |level, message| {
            if level == LogLevel::Warn {
                warned.borrow_mut().push(message.to_string());
            }
        })),
        // No jitter: every wait is exactly the backoff.
        random: Rc::new(|| 0.5),
        ..RoomOptions::new("ws://relay.test/room", move || token.borrow().clone())
    }
}

/// What a link told its owner.
#[derive(Default)]
struct Heard {
    me: RefCell<Option<PeerRef>>,
    events: RefCell<Vec<String>>,
    messages: RefCell<Vec<(String, Json)>>,
    status: RefCell<Vec<PeerLinkStatus>>,
}

impl Heard {
    fn note_peer(&self, peer: &str, up: bool) {
        let event = format!("{} {peer}", if up { "up" } else { "down" });
        self.events.borrow_mut().push(event);
    }
}

impl LinkHooks for Heard {
    fn self_ref(&self) -> Option<PeerRef> {
        self.me.borrow().clone()
    }
    fn on_peer(&self, member: &str, up: bool) {
        self.note_peer(member, up);
    }
    fn on_message(&self, from: &str, data: &Json) {
        self.messages
            .borrow_mut()
            .push((from.to_string(), data.clone()));
    }
    fn on_status(&self, status: PeerLinkStatus) {
        self.status.borrow_mut().push(status);
    }
}

impl TransportHooks for Heard {
    fn self_ref(&self) -> Option<PeerRef> {
        self.me.borrow().clone()
    }
    fn on_message(&self, from: &PeerRef, message: &Json) {
        self.messages
            .borrow_mut()
            .push((from.peer.clone(), message.clone()));
    }
    fn on_peer(&self, peer: &PeerRef, up: bool) {
        self.note_peer(&peer.peer, up);
    }
    fn on_status(&self, _user: &str, status: PeerLinkStatus) {
        self.status.borrow_mut().push(status);
    }
}

/// One member of `user:<name>`'s room, as a bare link: what a browser's hub
/// holds, and what a test scripts one with.
struct Member {
    link: Link,
    heard: Rc<Heard>,
    token: Rc<RefCell<Option<String>>>,
    warned: Warnings,
}

impl Member {
    fn join(relay: &FakeRelay, name: &str, peer: &str) -> Member {
        Member::with_token(relay, name, peer, Some(format!("t-{name}")))
    }

    fn with_token(relay: &FakeRelay, name: &str, peer: &str, token: Option<String>) -> Member {
        let heard = Rc::new(Heard::default());
        *heard.me.borrow_mut() = Some(PeerRef::new(format!("user:{name}"), peer));
        let (token, warned) = (Rc::new(RefCell::new(token)), Warnings::default());
        let link = Link::open(heard.clone(), Rc::new(options(relay, &token, &warned)));
        Member {
            link,
            heard,
            token,
            warned,
        }
    }

    fn events(&self) -> Vec<String> {
        self.heard.events.borrow().clone()
    }

    fn messages(&self) -> Vec<(String, Json)> {
        self.heard.messages.borrow().clone()
    }

    fn status(&self) -> Vec<PeerLinkStatus> {
        self.heard.status.borrow().clone()
    }

    fn set_token(&self, token: Option<&str>) {
        *self.token.borrow_mut() = token.map(str::to_string);
    }
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

fn from(peer: &str, data: Json) -> (String, Json) {
    (peer.to_string(), data)
}

// ---- the link ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn joins_the_room_of_its_user_and_learns_who_is_in_it() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::join(&relay, "ann", "a");
        settle().await;
        assert_eq!(a.status(), vec![Connecting, Connected]);
        assert_eq!(
            relay.socket(0).sent.borrow()[0],
            json!({"t": "hello", "v": 1, "token": "t-ann", "peer": "a"})
        );
        let b = Member::join(&relay, "ann", "b");
        let c = Member::join(&relay, "ann", "c");
        settle().await;
        assert_eq!(a.events(), vec!["up b", "up c"]);
        assert_eq!(b.events(), vec!["up a", "up c"]);
        assert_eq!(c.events(), vec!["up a", "up b"]);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn sends_to_everybody_or_to_the_members_named_never_to_another_user() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::join(&relay, "ann", "a");
        let b = Member::join(&relay, "ann", "b");
        let c = Member::join(&relay, "ann", "c");
        let other = Member::join(&relay, "bob", "x");
        settle().await;
        a.link.send(None, &json!({"n": 1}));
        a.link.send(Some(&names(&["c"])), &json!({"n": 2}));
        a.link
            .send(Some(&names(&["nobody", "b"])), &json!({"n": 3}));
        settle().await;
        assert_eq!(
            b.messages(),
            vec![from("a", json!({"n": 1})), from("a", json!({"n": 3}))]
        );
        assert_eq!(
            c.messages(),
            vec![from("a", json!({"n": 1})), from("a", json!({"n": 2}))]
        );
        assert_eq!(a.messages(), vec![]);
        assert_eq!(other.messages(), vec![]);
        assert_eq!(other.events(), Vec::<String>::new());
        // To all of them is one frame without a list; the payload is wrapped
        // as the browser wraps it.
        assert_eq!(
            relay.socket_of("a").sent_of("msg"),
            vec![
                json!({"t": "msg", "d": {"m": {"n": 1}}}),
                json!({"t": "msg", "to": ["c"], "d": {"m": {"n": 2}}}),
                json!({"t": "msg", "to": ["b"], "d": {"m": {"n": 3}}}),
            ]
        );
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn reports_a_member_that_left_and_ignores_what_it_sends_after() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::join(&relay, "ann", "a");
        let b = Member::join(&relay, "ann", "b");
        settle().await;
        b.link.close();
        b.link.close();
        settle().await;
        assert_eq!(a.events(), vec!["up b", "down b"]);
        b.link.send(None, &json!({"late": true}));
        a.link.send(Some(&names(&["b"])), &json!({"n": 1}));
        settle().await;
        assert_eq!(a.messages(), vec![]);
        assert_eq!(relay.members("user:ann"), vec!["a"]);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn comes_back_after_the_socket_drops_with_everybody_down_in_between() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::join(&relay, "ann", "a");
        let b = Member::join(&relay, "ann", "b");
        settle().await;
        assert_eq!(a.events(), vec!["up b"]);
        relay.0.down.set(true);
        relay.drop_socket(&relay.socket_of("a"));
        settle().await;
        assert_eq!(a.events(), vec!["up b", "down b"]);
        assert_eq!(a.status().last(), Some(&Connecting));
        a.link.send(None, &json!({"lost": true}));
        // Refused while the relay is down, a little later each time.
        advance(5_000).await;
        assert_eq!(a.status().last(), Some(&Failed));
        relay.0.down.set(false);
        advance(40_000).await;
        assert_eq!(a.status().last(), Some(&Connected));
        assert_eq!(a.events(), vec!["up b", "down b", "up b"]);
        assert_eq!(b.messages(), vec![]);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn waits_half_a_second_then_twice_as_long_each_time_up_to_thirty() {
    run_local(async {
        let relay = FakeRelay::default();
        relay.0.down.set(true);
        // The relay is down, so every attempt fails at once: note when each
        // one is made.
        let tries = Rc::new(Cell::new(0u32));
        let (counted, connect) = (tries.clone(), relay.connector());
        let heard = Rc::new(Heard::default());
        *heard.me.borrow_mut() = Some(PeerRef::new("user:ann", "a"));
        let (token, warned) = (
            Rc::new(RefCell::new(Some("t-ann".to_string()))),
            Warnings::default(),
        );
        let opts = RoomOptions {
            connect: Rc::new(move |url| {
                counted.set(counted.get() + 1);
                connect(url)
            }),
            ..options(&relay, &token, &warned)
        };
        let _link = Link::open(heard.clone(), Rc::new(opts));
        let start = Instant::now();
        let mut at = Vec::new();
        while at.len() < 9 {
            let before = tries.get();
            advance(1).await;
            if tries.get() != before {
                at.push(start.elapsed().as_millis() as u64 / 100 * 100);
            }
        }
        // 0.5 s, doubling, capped at 30 s; `failed` from the third miss on.
        assert_eq!(
            at,
            vec![0, 500, 1_500, 3_500, 7_500, 15_500, 31_500, 61_500, 91_500]
        );
        assert_eq!(
            heard.status.borrow()[..4],
            [Connecting, Connecting, Connecting, Failed]
        );
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn is_refused_without_a_valid_token_and_asks_again_later() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::with_token(&relay, "ann", "a", Some("nonsense".into()));
        advance(1_000).await;
        assert_eq!(a.status().last(), Some(&Failed));
        assert_eq!(relay.members("user:ann"), Vec::<String>::new());
        let tries = relay.socket_count();
        advance(20_000).await;
        assert_eq!(relay.socket_count(), tries);
        a.set_token(Some("t-ann"));
        advance(15_000).await;
        assert_eq!(a.status().last(), Some(&Connected));
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn waits_for_a_token_before_it_opens_a_socket() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::with_token(&relay, "ann", "a", None);
        advance(6_000).await;
        assert_eq!(relay.socket_count(), 0);
        assert_eq!(a.status(), vec![Connecting]);
        a.set_token(Some("t-ann"));
        advance(1_000).await;
        assert_eq!(a.status().last(), Some(&Connected));
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn signs_in_again_at_once_when_the_relay_ends_a_long_session() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::join(&relay, "ann", "a");
        settle().await;
        a.set_token(Some("t-ann"));
        relay.kick("a", "reauth");
        advance(1_000).await;
        // Never `failed` in between: this is the relay's routine.
        assert_eq!(
            a.status(),
            vec![Connecting, Connected, Connecting, Connected]
        );
        assert_eq!(relay.socket_count(), 2);
        // Any other refusal waits.
        relay.kick("a", "room_full");
        advance(20_000).await;
        assert_eq!(a.status().last(), Some(&Failed));
        assert_eq!(relay.socket_count(), 2);
        advance(11_000).await;
        assert_eq!(a.status().last(), Some(&Connected));
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn notices_a_socket_that_stopped_answering() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::join(&relay, "ann", "a");
        settle().await;
        advance(20_000).await;
        let socket = relay.socket(0);
        assert_eq!(socket.sent.borrow().last(), Some(&json!({"t": "ping"})));
        assert!(socket.is_open());
        // Answered: the next beat is no reason to hang up.
        advance(20_000).await;
        assert!(socket.is_open());
        relay.0.mute.set(true);
        advance(40_000).await;
        assert!(!socket.is_open());
        relay.0.mute.set(false);
        advance(5_000).await;
        assert_eq!(a.status().last(), Some(&Connected));
        assert_eq!(relay.socket_count(), 2);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn gives_up_on_a_socket_that_never_answers_the_hello() {
    run_local(async {
        let relay = FakeRelay::default();
        relay.0.silent.set(true);
        let a = Member::join(&relay, "ann", "a");
        advance(29_000).await;
        assert!(relay.socket(0).is_open());
        assert_eq!(a.status(), vec![Connecting]);
        advance(2_000).await;
        // Hung up on, and the next one is tried like after any drop.
        assert!(!relay.socket(0).is_open());
        assert_eq!(relay.socket_count(), 2);
        relay.0.silent.set(false);
        advance(31_000).await;
        assert_eq!(a.status().last(), Some(&Connected));
        assert!(!a.status().contains(&Failed));
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn does_not_join_when_the_relay_answers_for_another_user() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::with_token(&relay, "ann", "a", Some("t-bob".into()));
        let _bob = Member::join(&relay, "bob", "x");
        settle().await;
        assert_eq!(a.warned.borrow().len(), 1, "{:?}", a.warned);
        assert_eq!(a.status(), vec![Connecting, Failed]);
        assert_eq!(a.events(), Vec::<String>::new());
        a.link.send(None, &json!({"n": 1}));
        settle().await;
        assert_eq!(relay.socket(0).sent_of("msg"), Vec::<Json>::new());
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn joins_whatever_escaping_the_database_added_to_the_user() {
    run_local(async {
        let relay = FakeRelay::default();
        *relay.0.welcome_as.borrow_mut() = Some("user:⟨ann⟩".into());
        let a = Member::join(&relay, "ann", "a");
        settle().await;
        assert_eq!(a.status(), vec![Connecting, Connected]);
        assert!(a.warned.borrow().is_empty());
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn does_nothing_while_nobody_is_signed_in() {
    run_local(async {
        let relay = FakeRelay::default();
        let heard = Rc::new(Heard::default());
        let (token, warned) = (
            Rc::new(RefCell::new(Some("t-ann".to_string()))),
            Warnings::default(),
        );
        let link = Link::open(heard.clone(), Rc::new(options(&relay, &token, &warned)));
        settle().await;
        assert_eq!(relay.socket_count(), 0);
        link.close();
        link.close();
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn takes_only_data_from_members_and_leaves_the_browsers_signaling_alone() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Member::join(&relay, "ann", "a");
        let _b = Member::join(&relay, "ann", "b");
        settle().await;
        let socket = relay.socket_of("a");
        // The offer of a browser's direct WebRTC link: nobody here answers.
        socket.push(json!({"t": "msg", "from": "b", "d": {"s": {"kind": "hello", "payload": null, "toPeer": null}}}));
        socket.push(json!({"t": "msg", "from": "b", "d": {"s": null, "m": {"n": 0}}}));
        socket.push(json!({"t": "msg", "from": "stranger", "d": {"m": {"n": 0}}}));
        socket.push(json!({"t": "msg", "from": "b", "d": "text"}));
        socket.push(json!({"t": "msg", "from": "b", "d": {}}));
        socket.push(json!({"t": "what"}));
        socket.push(json!(["not", "a", "frame"]));
        if let Some(tx) = &*socket.to_client.borrow() {
            tx.try_send("not json".into()).unwrap();
        }
        socket.push(json!({"t": "welcome", "user": "user:bob", "peer": "a", "peers": ["z"]}));
        socket.push(json!({"t": "join", "peer": "a"}));
        socket.push(json!({"t": "leave", "peer": "nobody"}));
        socket.push(json!({"t": "msg", "from": "b", "d": {"m": {"n": 1}}}));
        settle().await;
        assert_eq!(a.messages(), vec![from("b", json!({"n": 1}))]);
        assert_eq!(a.events(), vec!["up b"]);
        assert_eq!(a.status(), vec![Connecting, Connected]);
    })
    .await;
}

#[test]
fn compares_record_ids_whatever_escaping_the_database_added() {
    assert!(same_user("user:⟨a-b⟩", "user:a-b"));
    assert!(same_user("user:`a-b`", "user:⟨a-b⟩"));
    assert!(same_user(
        "user:u'0190f2a4-7c1b-7d3e-9f00-795045881219'",
        "user:0190f2a4-7c1b-7d3e-9f00-795045881219"
    ));
    assert!(!same_user("user:a", "user:b"));
    assert!(!same_user("user:ua", "user:a"));
    assert!(!same_user("admin:a", "user:a"));
}

#[test]
fn knows_the_peer_ids_the_relay_takes() {
    assert!(valid_peer_id("a1B2_c-3"));
    assert!(valid_peer_id(&"f".repeat(64)));
    assert!(!valid_peer_id(""));
    assert!(!valid_peer_id(&"f".repeat(65)));
    assert!(!valid_peer_id("user:a"));
    assert!(!valid_peer_id("ä"));
}

// ---- the transport: a member that speaks for one tab ---------------------------------------

const USER: &str = "user:ann";

/// One client of `USER`: the transport and what it told the manager.
struct Client {
    t: Rc<dyn ChannelTransport>,
    heard: Rc<Heard>,
    warned: Warnings,
}

impl Client {
    fn new(relay: &FakeRelay, peer: &str) -> Client {
        let heard = Rc::new(Heard::default());
        *heard.me.borrow_mut() = Some(PeerRef::new(USER, peer));
        let (token, warned) = (
            Rc::new(RefCell::new(Some("t-ann".to_string()))),
            Warnings::default(),
        );
        let t = room_transport(heard.clone(), options(relay, &token, &warned));
        Client { t, heard, warned }
    }

    fn join(&self) {
        self.t.want(&BTreeSet::from([USER.to_string()]));
    }

    fn send(&self, tab: &str, message: &ChannelWire) {
        self.t.send(&PeerRef::new(USER, tab), message);
    }

    fn events(&self) -> Vec<String> {
        self.heard.events.borrow().clone()
    }

    fn messages(&self) -> Vec<(String, Json)> {
        self.heard.messages.borrow().clone()
    }

    fn status(&self) -> Vec<PeerLinkStatus> {
        self.heard.status.borrow().clone()
    }
}

fn op(n: i64) -> ChannelWire {
    let mut patch = JsonMap::new();
    patch.insert("n".into(), json!(n));
    ChannelWire::Op(ChannelMessage {
        kind: ChannelOpKind::Update,
        table: "note".into(),
        id: format!("note:{n}"),
        patch,
        hlc: Hlc::new(n, 0, "x"),
    })
}

#[tokio::test(start_paused = true)]
async fn two_clients_of_one_user_are_one_tab_each_to_each_other() {
    run_local(async {
        let relay = FakeRelay::default();
        let (a, b) = (Client::new(&relay, "pa"), Client::new(&relay, "pb"));
        a.join();
        b.join();
        settle().await;
        assert_eq!(a.events(), vec!["up pb"]);
        assert_eq!(b.events(), vec!["up pa"]);
        assert_eq!(a.status(), vec![Connecting, Connected]);
        assert_eq!(relay.members(USER), vec!["pa", "pb"]);
        a.send("pb", &op(1));
        b.send("pa", &op(2));
        settle().await;
        assert_eq!(b.messages(), vec![from("pa", op(1).to_json())]);
        assert_eq!(a.messages(), vec![from("pb", op(2).to_json())]);
        // Another user, or a tab nobody speaks for: nowhere to go.
        let before = relay.0.forwarded.get();
        a.t.send(&PeerRef::new("user:bob", "pb"), &op(3));
        a.send("ghost", &op(4));
        settle().await;
        assert_eq!(relay.0.forwarded.get(), before);
        b.t.close();
        settle().await;
        assert_eq!(a.events(), vec!["up pb", "down pb"]);
        assert_eq!(b.events(), vec!["up pa", "down pa"]);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn speaks_the_envelope_of_a_browser_that_holds_two_tabs() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Client::new(&relay, "pa");
        a.join();
        settle().await;
        // A browser: one member of the room, speaking for its tabs.
        let hub = Member::join(&relay, "ann", "h1");
        settle().await;
        // It is told which tabs this client speaks for: itself.
        assert_eq!(
            hub.messages(),
            vec![from("pa", json!({"h": "tabs", "tabs": ["pa"]}))]
        );
        // Nobody is a peer before the browser said which tabs it has.
        assert_eq!(a.events(), Vec::<String>::new());
        hub.link
            .send(None, &json!({"h": "tabs", "tabs": ["t1", "t2"]}));
        settle().await;
        assert_eq!(a.events(), vec!["up t1", "up t2"]);

        // One message for both tabs is one frame naming both.
        a.send("t1", &op(1));
        a.send("t2", &op(1));
        a.send("t2", &op(2));
        settle().await;
        assert_eq!(
            hub.messages()[1..],
            [
                from(
                    "pa",
                    json!({"h": "m", "from": "pa", "to": ["t1", "t2"], "m": op(1).to_json()})
                ),
                from(
                    "pa",
                    json!({"h": "m", "from": "pa", "to": ["t2"], "m": op(2).to_json()})
                ),
            ]
        );

        // A message of one of its tabs, for everybody or for this client.
        let say = |envelope: Json| hub.link.send(None, &envelope);
        say(json!({"h": "m", "from": "t1", "to": null, "m": op(3).to_json()}));
        say(json!({"h": "m", "from": "t2", "to": ["pa", "elsewhere"], "m": op(4).to_json()}));
        settle().await;
        assert_eq!(
            a.messages(),
            vec![from("t1", op(3).to_json()), from("t2", op(4).to_json())]
        );
        // Not for this client, not from a tab the browser speaks for, or not
        // an envelope at all.
        say(json!({"h": "m", "from": "t1", "to": ["elsewhere"], "m": op(5).to_json()}));
        say(json!({"h": "m", "from": "zz", "to": null, "m": op(5).to_json()}));
        say(json!({"h": "m", "from": "t1", "to": "pa", "m": op(5).to_json()}));
        say(json!({"h": "m", "from": "t1", "m": op(5).to_json()}));
        say(json!({"h": "tabs", "tabs": "nope"}));
        say(json!({"h": "what"}));
        say(json!(null));
        settle().await;
        assert_eq!(a.messages().len(), 2);
        assert_eq!(a.events().len(), 2);

        // A tab closes: it is down, and nothing goes its way anymore.
        say(json!({"h": "tabs", "tabs": ["t1"]}));
        settle().await;
        assert_eq!(a.events().last().unwrap(), "down t2");
        let frames = hub.messages().len();
        a.send("t2", &op(6));
        settle().await;
        assert_eq!(hub.messages().len(), frames);
        // The browser goes: so do the tabs it spoke for.
        hub.link.close();
        settle().await;
        assert_eq!(a.events().last().unwrap(), "down t1");
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn addresses_a_frame_to_the_members_that_speak_for_its_tabs() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Client::new(&relay, "pa");
        a.join();
        let (left, right) = (
            Member::join(&relay, "ann", "h1"),
            Member::join(&relay, "ann", "h2"),
        );
        settle().await;
        left.link.send(None, &json!({"h": "tabs", "tabs": ["x1", "x2"]}));
        right.link.send(None, &json!({"h": "tabs", "tabs": ["y1"]}));
        settle().await;
        let mut up = a.events();
        up.sort();
        assert_eq!(up, vec!["up x1", "up x2", "up y1"]);
        a.send("x1", &op(1));
        a.send("x2", &op(1));
        settle().await;
        a.send("x2", &op(2));
        a.send("y1", &op(2));
        settle().await;
        let sent = relay.socket_of("pa").sent_of("msg");
        // The two announcements of its own tab, then the messages.
        assert_eq!(
            sent[sent.len() - 2..],
            [
                json!({"t": "msg", "to": ["h1"], "d": {"m": {"h": "m", "from": "pa", "to": ["x1", "x2"], "m": op(1).to_json()}}}),
                json!({"t": "msg", "d": {"m": {"h": "m", "from": "pa", "to": ["x2", "y1"], "m": op(2).to_json()}}}),
            ]
        );
        // The relay only handed the first one to the browser it was for.
        let messages = |m: &Member| -> Vec<Json> {
            let heard = m.messages().into_iter();
            heard
                .filter(|(_, data)| data["h"] == "m")
                .map(|(_, data)| data["m"].clone())
                .collect()
        };
        assert_eq!(messages(&left), vec![op(1).to_json(), op(2).to_json()]);
        assert_eq!(messages(&right), vec![op(2).to_json()]);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn leaves_when_the_user_is_no_longer_wanted_and_returns_when_wanted_again() {
    run_local(async {
        let relay = FakeRelay::default();
        let (a, b) = (Client::new(&relay, "pa"), Client::new(&relay, "pb"));
        a.join();
        b.join();
        settle().await;
        a.t.want(&BTreeSet::new());
        settle().await;
        assert_eq!(a.events(), vec!["up pb", "down pb"]);
        assert_eq!(relay.members(USER), vec!["pb"]);
        assert_eq!(b.events(), vec!["up pa", "down pa"]);
        a.join();
        settle().await;
        assert_eq!(a.events(), vec!["up pb", "down pb", "up pb"]);
        assert_eq!(b.events().last().unwrap(), "up pa");
        // Wanting the same again changes nothing.
        let sockets = relay.socket_count();
        a.join();
        settle().await;
        assert_eq!(relay.socket_count(), sockets);
        // Signed out: nothing to join as.
        *b.heard.me.borrow_mut() = None;
        b.join();
        settle().await;
        assert_eq!(relay.members(USER), vec!["pa"]);
        b.send("pa", &op(7));
        settle().await;
        assert_eq!(a.messages(), vec![]);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn tells_the_manager_how_the_link_is_doing() {
    run_local(async {
        let relay = FakeRelay::default();
        relay.0.down.set(true);
        let a = Client::new(&relay, "pa");
        a.join();
        advance(5_000).await;
        // Each change once, however many attempts were behind it.
        assert_eq!(a.status(), vec![Connecting, Failed]);
        relay.0.down.set(false);
        advance(40_000).await;
        assert_eq!(a.status(), vec![Connecting, Failed, Connected]);
        assert!(a.t.supported());
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn does_not_knock_with_a_peer_id_the_relay_would_refuse() {
    run_local(async {
        let relay = FakeRelay::default();
        let a = Client::new(&relay, "not a peer id");
        a.join();
        advance(60_000).await;
        assert_eq!(relay.socket_count(), 0);
        assert_eq!(a.status(), vec![Failed]);
        assert!(a.warned.borrow()[0].contains("not a peer id"));
        a.t.close();
    })
    .await;
}
