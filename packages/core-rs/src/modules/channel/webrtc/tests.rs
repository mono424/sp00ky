//! PORT: `packages/spooky_core/test/modules/channel_webrtc_test.dart`.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::FutureExt;
use serde_json::{json, Value as Json};
use tokio::sync::mpsc;

use super::*;
use crate::modules::channel::{
    ChannelMessage, ChannelOpKind, ChannelTransport, Hlc, PeerLinkStatus, PeerRef,
};
use crate::surreal::rpc::LiveMessage;
use crate::testing::run_local;
use crate::value::{Object, RecordId, Value};

// ---- a fake WebRTC stack ---------------------------------------------------------------

type Cb<T> = RefCell<Option<Rc<T>>>;

#[derive(Default)]
struct FakeChannel {
    open: Cell<bool>,
    closed: Cell<bool>,
    on_open: Cb<dyn Fn()>,
    on_close: Cb<dyn Fn()>,
    on_message: Cb<dyn Fn(String)>,
    other: RefCell<Weak<FakeChannel>>,
}

impl FakeChannel {
    fn open_now(&self) {
        self.open.set(true);
        let cb = self.on_open.borrow().clone();
        if let Some(cb) = cb {
            cb();
        }
    }
}

#[async_trait(?Send)]
impl RtcDataChannel for FakeChannel {
    fn set_on_open(&self, cb: Option<Rc<dyn Fn()>>) {
        *self.on_open.borrow_mut() = cb;
    }
    fn set_on_close(&self, cb: Option<Rc<dyn Fn()>>) {
        *self.on_close.borrow_mut() = cb;
    }
    fn set_on_message(&self, cb: Option<Rc<dyn Fn(String)>>) {
        *self.on_message.borrow_mut() = cb;
    }
    fn is_open(&self) -> bool {
        self.open.get()
    }
    fn send(&self, text: &str) -> crate::Result<()> {
        let (other, text) = (self.other.borrow().clone(), text.to_string());
        tokio::task::spawn_local(async move {
            let cb = other.upgrade().and_then(|o| o.on_message.borrow().clone());
            if let Some(cb) = cb {
                cb(text);
            }
        });
        Ok(())
    }
    async fn close(&self) -> crate::Result<()> {
        if self.closed.replace(true) {
            return Ok(());
        }
        self.open.set(false);
        let cb = self.on_close.borrow().clone();
        if let Some(cb) = cb {
            cb();
        }
        let other = self.other.borrow().upgrade();
        if let Some(other) = other {
            other.close().await?;
        }
        Ok(())
    }
}

/// Offers by SDP, so the answering side can find who it is talking to.
#[derive(Default)]
struct Registry {
    offers: HashMap<String, Rc<FakePc>>,
    all: Vec<Rc<FakePc>>,
    seq: u32,
    /// The data channels, kept alive for the whole test.
    channels: Vec<Rc<FakeChannel>>,
}

thread_local! {
    static REGISTRY: RefCell<Registry> = RefCell::new(Registry::default());
}

fn next_seq() -> u32 {
    REGISTRY.with(|r| {
        let mut r = r.borrow_mut();
        r.seq += 1;
        r.seq
    })
}

fn new_channel() -> Rc<FakeChannel> {
    let c = Rc::new(FakeChannel::default());
    REGISTRY.with(|r| r.borrow_mut().channels.push(c.clone()));
    c
}

#[derive(Default)]
struct FakePc {
    me: RefCell<Weak<FakePc>>,
    on_ice: Cb<dyn Fn(Json)>,
    on_data_channel: Cb<dyn Fn(Rc<dyn RtcDataChannel>)>,
    dc: RefCell<Option<Rc<FakeChannel>>>,
    remote: RefCell<Weak<FakePc>>,
    local: RefCell<Option<RtcDescription>>,
    ice: RefCell<Vec<Json>>,
    closed: Cell<bool>,
}

impl FakePc {
    fn create() -> Rc<FakePc> {
        let pc = Rc::new(FakePc::default());
        *pc.me.borrow_mut() = Rc::downgrade(&pc);
        REGISTRY.with(|r| r.borrow_mut().all.push(pc.clone()));
        pc
    }
}

#[async_trait(?Send)]
impl RtcPeerConnection for FakePc {
    fn set_on_ice_candidate(&self, cb: Option<IceCandidateCallback>) {
        *self.on_ice.borrow_mut() = cb;
    }
    fn set_on_connection_state(&self, _cb: Option<ConnectionStateCallback>) {}
    fn set_on_data_channel(&self, cb: Option<DataChannelCallback>) {
        *self.on_data_channel.borrow_mut() = cb;
    }
    async fn create_data_channel(
        &self,
        label: &str,
        ordered: bool,
    ) -> crate::Result<Rc<dyn RtcDataChannel>> {
        assert_eq!(label, "sp00ky");
        assert!(ordered);
        let dc = new_channel();
        *self.dc.borrow_mut() = Some(dc.clone());
        Ok(dc)
    }
    async fn create_offer(&self) -> crate::Result<RtcDescription> {
        let sdp = format!("offer-{}", next_seq());
        let me = self.me.borrow().upgrade().unwrap();
        REGISTRY.with(|r| r.borrow_mut().offers.insert(sdp.clone(), me));
        Ok(RtcDescription::new("offer", sdp))
    }
    async fn create_answer(&self) -> crate::Result<RtcDescription> {
        Ok(RtcDescription::new(
            "answer",
            format!("answer-{}", next_seq()),
        ))
    }
    async fn set_local_description(&self, d: RtcDescription) -> crate::Result<()> {
        *self.local.borrow_mut() = Some(d);
        // Two candidates in one burst: they go out as one batched signal.
        let cb = self.on_ice.borrow().clone();
        tokio::task::spawn_local(async move {
            if let Some(cb) = cb {
                cb(json!({"candidate": "c1", "sdpMid": "0", "sdpMLineIndex": 0}));
                cb(json!({"candidate": "c2", "sdpMid": "0", "sdpMLineIndex": 0}));
            }
        });
        Ok(())
    }
    async fn set_remote_description(&self, d: RtcDescription) -> crate::Result<()> {
        if d.kind == "offer" {
            let offerer = REGISTRY.with(|r| r.borrow().offers.get(&d.sdp).map(Rc::downgrade));
            *self.remote.borrow_mut() = offerer.unwrap_or_default();
        }
        if d.kind == "answer" {
            // The offerer learns the answer: wire both data channels and open
            // them.
            let me = self.me.borrow().clone();
            let answerer = REGISTRY.with(|r| {
                r.borrow()
                    .all
                    .iter()
                    .rev()
                    .find(|p| Weak::ptr_eq(&p.remote.borrow(), &me))
                    .cloned()
            });
            let mine = self.dc.borrow().clone();
            let (Some(answerer), Some(mine)) = (answerer, mine) else {
                return Ok(());
            };
            let theirs = new_channel();
            *mine.other.borrow_mut() = Rc::downgrade(&theirs);
            *theirs.other.borrow_mut() = Rc::downgrade(&mine);
            let cb = answerer.on_data_channel.borrow().clone();
            if let Some(cb) = cb {
                cb(theirs.clone());
            }
            tokio::task::spawn_local(async move {
                mine.open_now();
                theirs.open_now();
            });
        }
        Ok(())
    }
    async fn local_description(&self) -> crate::Result<Option<RtcDescription>> {
        Ok(self.local.borrow().clone())
    }
    async fn add_ice_candidate(&self, candidate: Json) -> crate::Result<()> {
        self.ice.borrow_mut().push(candidate);
        Ok(())
    }
    async fn close(&self) -> crate::Result<()> {
        self.closed.set(true);
        let dc = self.dc.borrow().clone();
        if let Some(dc) = dc {
            dc.close().await?;
        }
        Ok(())
    }
}

fn fake_rtc() -> RtcPeerConnectionFactory {
    Arc::new(|_config| async { Ok(FakePc::create() as Rc<dyn RtcPeerConnection>) }.boxed_local())
}

// ---- signals and hooks -----------------------------------------------------------------------

/// Signals between users, delivered like the LIVE would: to every device of
/// the recipient.
type SignalListeners = HashMap<String, Vec<(u64, Rc<dyn Fn(Signal)>)>>;

#[derive(Default)]
struct MemorySignals {
    listeners: RefCell<SignalListeners>,
    next: Cell<u64>,
    sent: RefCell<Vec<(String, SignalKind, Option<String>)>>,
}

struct Port {
    net: Rc<MemorySignals>,
    user: String,
}

#[async_trait(?Send)]
impl SignalPort for Port {
    async fn send(&self, s: OutgoingSignal) -> crate::Result<()> {
        self.net
            .sent
            .borrow_mut()
            .push((self.user.clone(), s.kind, s.payload.clone()));
        let signal = Signal {
            from: self.user.clone(),
            from_peer: s.from_peer,
            to_peer: s.to_peer,
            kind: s.kind,
            payload: s.payload,
        };
        let net = self.net.clone();
        tokio::task::spawn_local(async move {
            let listeners: Vec<Rc<dyn Fn(Signal)>> = net
                .listeners
                .borrow()
                .get(&s.to)
                .map(|l| l.iter().map(|(_, cb)| cb.clone()).collect())
                .unwrap_or_default();
            for cb in listeners {
                cb(signal.clone());
            }
        });
        Ok(())
    }

    fn listen(&self, me: &PeerRef, cb: Rc<dyn Fn(Signal)>) -> Box<dyn FnOnce()> {
        let id = self.net.next.get() + 1;
        self.net.next.set(id);
        self.net
            .listeners
            .borrow_mut()
            .entry(me.user.clone())
            .or_default()
            .push((id, cb));
        let (net, user) = (self.net.clone(), me.user.clone());
        Box::new(move || {
            if let Some(l) = net.listeners.borrow_mut().get_mut(&user) {
                l.retain(|(i, _)| *i != id);
            }
        })
    }
}

struct Hooks {
    me: PeerRef,
    events: RefCell<Vec<String>>,
    messages: RefCell<Vec<Json>>,
    statuses: RefCell<Vec<String>>,
}

impl TransportHooks for Hooks {
    fn self_ref(&self) -> Option<PeerRef> {
        Some(self.me.clone())
    }
    fn on_message(&self, _from: &PeerRef, message: &Json) {
        self.messages.borrow_mut().push(message.clone());
    }
    fn on_peer(&self, peer: &PeerRef, up: bool) {
        self.events
            .borrow_mut()
            .push(format!("{} {}", if up { "up" } else { "down" }, peer.user));
    }
    fn on_status(&self, user: &str, status: PeerLinkStatus) {
        self.statuses
            .borrow_mut()
            .push(format!("{user} {status:?}"));
    }
}

struct TestDevice {
    t: Rc<dyn ChannelTransport>,
    hooks: Rc<Hooks>,
}

fn device(user: &str, peer: &str, signals: &Rc<MemorySignals>, rtc: bool) -> TestDevice {
    let hooks = Rc::new(Hooks {
        me: PeerRef::new(user, peer),
        events: RefCell::default(),
        messages: RefCell::default(),
        statuses: RefCell::default(),
    });
    let mut opts = WebRtcOptions::new(Rc::new(Port {
        net: signals.clone(),
        user: user.to_string(),
    }));
    opts.rtc = rtc.then(fake_rtc);
    let t = webrtc_transport(hooks.clone(), opts);
    TestDevice { t, hooks }
}

fn users(list: &[&str]) -> BTreeSet<String> {
    list.iter().map(|s| s.to_string()).collect()
}

async fn settle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    // ICE batches flush after 20 ms.
    tokio::time::sleep(Duration::from_millis(30)).await;
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

fn events(d: &TestDevice) -> Vec<String> {
    d.hooks.events.borrow().clone()
}

fn sent_of(signals: &MemorySignals, kind: SignalKind) -> Vec<(String, Option<String>)> {
    signals
        .sent
        .borrow()
        .iter()
        .filter(|(_, k, _)| *k == kind)
        .map(|(from, _, payload)| (from.clone(), payload.clone()))
        .collect()
}

// ---- the transport -----------------------------------------------------------------------

#[tokio::test]
async fn connects_two_devices_that_want_each_other_whichever_starts_first() {
    run_local(async {
        let signals = Rc::new(MemorySignals::default());
        let a = device("user:a", "pa", &signals, true);
        let b = device("user:b", "pb", &signals, true);
        a.t.want(&users(&["user:b"]));
        settle().await;
        assert!(events(&a).is_empty());
        b.t.want(&users(&["user:a"]));
        settle().await;
        assert_eq!(events(&a), vec!["up user:b"]);
        assert_eq!(events(&b), vec!["up user:a"]);
        let mut patch = serde_json::Map::new();
        patch.insert("view_pos".into(), json!("1"));
        a.t.send(
            &PeerRef::new("user:b", "pb"),
            &ChannelMessage {
                kind: ChannelOpKind::Update,
                table: "call".into(),
                id: "call:1".into(),
                patch,
                hlc: Hlc::new(1, 0, "pa"),
            },
        );
        settle().await;
        assert_eq!(
            *b.hooks.messages.borrow(),
            vec![json!({
                "t": "op",
                "table": "call",
                "id": "call:1",
                "patch": {"view_pos": "1"},
                "hlc": [1, 0, "pa"],
            })]
        );
        // The lower `user#peer` key offers; candidates trickle as one batch per
        // burst, tagged with the negotiation they belong to.
        let offers = sent_of(&signals, SignalKind::Offer);
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].0, "user:a");
        let ice = sent_of(&signals, SignalKind::Ice);
        assert_eq!(ice.len(), 2, "one batch per side");
        let batch: Json = serde_json::from_str(ice[0].1.as_deref().unwrap()).unwrap();
        assert_eq!(batch["candidates"].as_array().unwrap().len(), 2);
        assert!(batch["sid"].is_string());
        a.t.close();
        b.t.close();
    })
    .await;
}

#[tokio::test]
async fn the_higher_key_answers_a_broadcast_hello_with_a_directed_one() {
    run_local(async {
        let signals = Rc::new(MemorySignals::default());
        let b = device("user:b", "pb", &signals, true);
        let a = device("user:a", "pa", &signals, true);
        b.t.want(&users(&["user:a"]));
        a.t.want(&users(&["user:b"]));
        settle().await;
        assert_eq!(events(&a), vec!["up user:b"]);
        assert_eq!(events(&b), vec!["up user:a"]);
        let offers = sent_of(&signals, SignalKind::Offer);
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].0, "user:a");
        a.t.close();
        b.t.close();
    })
    .await;
}

#[tokio::test]
async fn never_connects_to_a_user_who_does_not_want_it_back() {
    run_local(async {
        let signals = Rc::new(MemorySignals::default());
        let a = device("user:a", "pa", &signals, true);
        let b = device("user:b", "pb", &signals, true);
        a.t.want(&users(&["user:b"]));
        b.t.want(&users(&["user:c"]));
        settle().await;
        assert!(events(&a).is_empty());
        assert!(events(&b).is_empty());
        assert!(sent_of(&signals, SignalKind::Offer).is_empty());
        a.t.close();
        b.t.close();
    })
    .await;
}

#[tokio::test]
async fn closes_the_link_when_the_user_is_no_longer_wanted() {
    run_local(async {
        let signals = Rc::new(MemorySignals::default());
        let a = device("user:a", "pa", &signals, true);
        let b = device("user:b", "pb", &signals, true);
        a.t.want(&users(&["user:b"]));
        b.t.want(&users(&["user:a"]));
        settle().await;
        a.t.want(&BTreeSet::new());
        settle().await;
        assert_eq!(events(&a), vec!["up user:b", "down user:b"]);
        assert_eq!(events(&b), vec!["up user:a", "down user:a"]);
        assert!(b
            .hooks
            .statuses
            .borrow()
            .contains(&"user:a Failed".to_string()));
        a.t.close();
        b.t.close();
    })
    .await;
}

#[tokio::test]
async fn is_unsupported_without_a_peer_connection_factory_and_sends_nothing() {
    run_local(async {
        let signals = Rc::new(MemorySignals::default());
        let a = device("user:a", "pa", &signals, false);
        assert!(!a.t.supported());
        a.t.want(&users(&["user:b"]));
        settle().await;
        assert!(signals.sent.borrow().is_empty());
    })
    .await;
}

// ---- signaling over SurrealDB ------------------------------------------------------------

#[derive(Default)]
struct FakeRemote {
    queries: RefCell<Vec<(String, Object)>>,
    lives: RefCell<Vec<mpsc::UnboundedSender<LiveMessage>>>,
    live_calls: RefCell<Vec<(String, Object)>>,
    killed: RefCell<Vec<String>>,
    connection: Cb<dyn Fn(ConnectionState)>,
}

struct RemoteRef(Rc<FakeRemote>);

#[async_trait(?Send)]
impl SignalRemote for RemoteRef {
    async fn query(&self, sql: &str, vars: Object) -> crate::Result<Vec<Value>> {
        self.0.queries.borrow_mut().push((sql.to_string(), vars));
        Ok(vec![])
    }
    async fn live(&self, sql: &str, vars: Object) -> crate::Result<(String, LiveStream)> {
        self.0.live_calls.borrow_mut().push((sql.to_string(), vars));
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.lives.borrow_mut().push(tx);
        Ok((format!("live-{}", self.0.lives.borrow().len()), rx))
    }
    async fn kill(&self, live_id: &str) -> crate::Result<()> {
        self.0.killed.borrow_mut().push(live_id.to_string());
        Ok(())
    }
    fn subscribe_connection(&self, cb: Box<dyn Fn(ConnectionState)>) -> Box<dyn FnOnce()> {
        let cb: Rc<dyn Fn(ConnectionState)> = Rc::from(cb);
        cb(ConnectionState::Connected);
        *self.0.connection.borrow_mut() = Some(cb);
        let remote = self.0.clone();
        Box::new(move || {
            remote.connection.borrow_mut().take();
        })
    }
}

fn live(action: &str, value: Object) -> LiveMessage {
    LiveMessage {
        action: action.into(),
        value,
    }
}

fn obj(pairs: &[(&str, Value)]) -> Object {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

async fn yields() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn writes_a_row_per_signal_and_reads_its_live_re_armed_on_reconnect() {
    run_local(async {
        let remote = Rc::new(FakeRemote::default());
        let port = surreal_signal(Rc::new(RemoteRef(remote.clone())), None);

        port.send(OutgoingSignal {
            to: "user:b".into(),
            to_peer: None,
            from_peer: "pa".into(),
            kind: SignalKind::Hello,
            payload: None,
        })
        .await
        .unwrap();
        {
            let queries = remote.queries.borrow();
            assert_eq!(queries.len(), 1);
            assert_eq!(
                queries[0].0,
                "CREATE _00_channel_signal CONTENT $c RETURN NONE"
            );
            assert_eq!(
                queries[0].1["c"],
                Value::Object(obj(&[
                    ("recipient", Value::RecordId(RecordId::new("user", "b"))),
                    ("sender_peer", Value::from("pa")),
                    ("kind", Value::from("hello")),
                ]))
            );
        }

        let got: Rc<RefCell<Vec<Signal>>> = Rc::default();
        let g = got.clone();
        let stop = port.listen(
            &PeerRef::new("user:a", "pa"),
            Rc::new(move |s| g.borrow_mut().push(s)),
        );
        yields().await;
        {
            let calls = remote.live_calls.borrow();
            assert!(calls[0]
                .0
                .contains("FROM _00_channel_signal WHERE recipient = $me"));
            assert_eq!(
                calls[0].1["me"],
                Value::RecordId(RecordId::new("user", "a"))
            );
        }
        let tx = remote.lives.borrow()[0].clone();
        tx.send(live(
            "CREATE",
            obj(&[
                ("sender", Value::RecordId(RecordId::new("user", "b"))),
                ("sender_peer", Value::from("pb")),
                ("recipient_peer", Value::from("pa")),
                ("kind", Value::from("offer")),
                ("payload", Value::from("{}")),
            ]),
        ))
        .unwrap();
        tx.send(live("DELETE", obj(&[("sender", Value::from("user:b"))])))
            .unwrap();
        tx.send(live("CREATE", obj(&[("sender", Value::from("user:b"))])))
            .unwrap();
        yields().await;
        {
            let got = got.borrow();
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].from, "user:b");
            assert_eq!(got[0].kind, SignalKind::Offer);
            assert_eq!(got[0].to_peer.as_deref(), Some("pa"));
        }

        let connection = remote.connection.borrow().clone().unwrap();
        connection(ConnectionState::Reconnecting);
        connection(ConnectionState::Connected);
        yields().await;
        assert_eq!(
            remote.lives.borrow().len(),
            2,
            "a reconnect re-arms the LIVE"
        );
        assert_eq!(*remote.killed.borrow(), vec!["live-1".to_string()]);
        stop();
        yields().await;
        assert_eq!(
            *remote.killed.borrow(),
            vec!["live-1".to_string(), "live-2".to_string()]
        );
        assert!(remote.connection.borrow().is_none());
    })
    .await;
}

#[tokio::test]
async fn a_failed_listen_retries() {
    struct Failing(Rc<Cell<u32>>);

    #[async_trait(?Send)]
    impl SignalRemote for Failing {
        async fn query(&self, _sql: &str, _vars: Object) -> crate::Result<Vec<Value>> {
            Ok(vec![])
        }
        async fn live(&self, _sql: &str, _vars: Object) -> crate::Result<(String, LiveStream)> {
            self.0.set(self.0.get() + 1);
            Err(crate::Error::Network("down".into()))
        }
        async fn kill(&self, _live_id: &str) -> crate::Result<()> {
            Ok(())
        }
        fn subscribe_connection(&self, _cb: Box<dyn Fn(ConnectionState)>) -> Box<dyn FnOnce()> {
            Box::new(|| {})
        }
    }

    run_local(async {
        tokio::time::pause();
        let attempts = Rc::new(Cell::new(0));
        let port = surreal_signal(Rc::new(Failing(attempts.clone())), None);
        let stop = port.listen(&PeerRef::new("user:a", "pa"), Rc::new(|_| {}));
        yields().await;
        assert_eq!(attempts.get(), 1);
        tokio::time::advance(Duration::from_millis(3100)).await;
        yields().await;
        assert_eq!(attempts.get(), 2);
        stop();
        tokio::time::advance(Duration::from_secs(10)).await;
        yields().await;
        assert_eq!(attempts.get(), 2, "a stopped listener never retries");
    })
    .await;
}
