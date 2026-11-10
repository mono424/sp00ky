//! The default channel transport: one WebRTC data channel per pair of devices,
//! set up through signaling rows in the database.
//!
//! PORT: `packages/spooky_core/lib/src/modules/channel/webrtc.dart` (itself a
//! port of `packages/core/src/modules/channel/webrtc.ts`), and the same
//! protocol, so a Rust device, a Dart device and a browser tab connect to each
//! other.
//!
//! The core has no WebRTC stack: the app injects one through
//! [`RtcPeerConnectionFactory`] (`ChannelsConfig::rtc`), adapting whatever
//! implementation it uses to [`RtcPeerConnection`] and [`RtcDataChannel`].
//!
//! Signaling (all rows of `_00_channel_signal`, read through one raw LIVE):
//!
//! 1. A device that wants user U writes `hello` to U. Every device of U hears
//!    it.
//! 2. Of each pair of devices, the one with the lower `user#peer` key offers.
//!    A device that hears a hello from a higher key offers at once; one that
//!    hears it from a lower key answers with a `hello` addressed back to that
//!    device, which then offers. So both orders of arrival connect, and nobody
//!    loops.
//! 3. `offer` / `answer` carry the SDPs, `ice` batches of candidates
//!    (trickled, so nothing waits for gathering to finish).
//!
//! A device only ever reacts to signals from users it wants itself, so a
//! connection needs both sides to list each other. The database stamps every
//! row's `sender`, and the offer carries the DTLS fingerprint of the
//! connection, so the user on the other end of a data channel is proven.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::{FutureExt, LocalBoxFuture, Shared};
use serde_json::{json, Value as Json};
use tokio::task::JoinHandle;

use super::{
    id_key, ChannelLogger, ChannelMessage, ChannelTransport, PeerLinkStatus, PeerRef,
    TransportHooks,
};
use crate::error::{Error, Result};
use crate::kernel::events::LogLevel;
use crate::surreal::rpc::LiveStream;
use crate::types::ConnectionState;
use crate::value::{Object, RecordId, Value};

// ---- the injected WebRTC stack ---------------------------------------------------------

/// A session description: `kind` is `offer` or `answer` (`RTCSessionDescription.type`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtcDescription {
    pub kind: String,
    pub sdp: String,
}

impl RtcDescription {
    pub fn new(kind: impl Into<String>, sdp: impl Into<String>) -> Self {
        RtcDescription {
            kind: kind.into(),
            sdp: sdp.into(),
        }
    }
}

/// Hears a local ICE candidate, as `RTCIceCandidate.toJSON()` shapes it
/// (`candidate`, `sdpMid`, `sdpMLineIndex`).
pub type IceCandidateCallback = Rc<dyn Fn(Json)>;
/// Hears `new`, `connecting`, `connected`, `disconnected`, `failed` or
/// `closed`.
pub type ConnectionStateCallback = Rc<dyn Fn(&str)>;
/// Hears a data channel the other side opened (the answering side).
pub type DataChannelCallback = Rc<dyn Fn(Rc<dyn RtcDataChannel>)>;

/// The bits of an `RTCPeerConnection` the default transport uses.
#[async_trait(?Send)]
pub trait RtcPeerConnection {
    fn set_on_ice_candidate(&self, cb: Option<IceCandidateCallback>);
    fn set_on_connection_state(&self, cb: Option<ConnectionStateCallback>);
    fn set_on_data_channel(&self, cb: Option<DataChannelCallback>);
    async fn create_data_channel(
        &self,
        label: &str,
        ordered: bool,
    ) -> Result<Rc<dyn RtcDataChannel>>;
    async fn create_offer(&self) -> Result<RtcDescription>;
    async fn create_answer(&self) -> Result<RtcDescription>;
    async fn set_local_description(&self, description: RtcDescription) -> Result<()>;
    async fn set_remote_description(&self, description: RtcDescription) -> Result<()>;
    async fn local_description(&self) -> Result<Option<RtcDescription>>;
    async fn add_ice_candidate(&self, candidate: Json) -> Result<()>;
    async fn close(&self) -> Result<()>;
}

#[async_trait(?Send)]
pub trait RtcDataChannel {
    fn set_on_open(&self, cb: Option<Rc<dyn Fn()>>);
    fn set_on_close(&self, cb: Option<Rc<dyn Fn()>>);
    /// Text frames only; binary frames are not part of the protocol.
    fn set_on_message(&self, cb: Option<Rc<dyn Fn(String)>>);
    fn is_open(&self) -> bool;
    fn send(&self, text: &str) -> Result<()>;
    async fn close(&self) -> Result<()>;
}

/// Builds a peer connection from an `RTCConfiguration` map
/// (`{"iceServers": [...]}`). Called on the engine thread; the connection it
/// returns stays there.
pub type RtcPeerConnectionFactory =
    Arc<dyn Fn(Json) -> LocalBoxFuture<'static, Result<Rc<dyn RtcPeerConnection>>> + Send + Sync>;

// ---- signals -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SignalKind {
    Hello,
    Offer,
    Answer,
    Ice,
}

impl SignalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SignalKind::Hello => "hello",
            SignalKind::Offer => "offer",
            SignalKind::Answer => "answer",
            SignalKind::Ice => "ice",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "hello" => SignalKind::Hello,
            "offer" => SignalKind::Offer,
            "answer" => SignalKind::Answer,
            "ice" => SignalKind::Ice,
            _ => return None,
        })
    }
}

/// A signal as it arrives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signal {
    pub from: String,
    pub from_peer: String,
    pub to_peer: Option<String>,
    pub kind: SignalKind,
    pub payload: Option<String>,
}

/// A signal as it goes out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutgoingSignal {
    pub to: String,
    pub to_peer: Option<String>,
    pub from_peer: String,
    pub kind: SignalKind,
    pub payload: Option<String>,
}

/// Where signals go and come from. The database by default.
#[async_trait(?Send)]
pub trait SignalPort {
    async fn send(&self, signal: OutgoingSignal) -> Result<()>;
    /// Hear the signals addressed to `me`'s user until the returned stop runs.
    fn listen(&self, me: &PeerRef, cb: Rc<dyn Fn(Signal)>) -> Box<dyn FnOnce()>;
}

pub struct WebRtcOptions {
    pub signal: Rc<dyn SignalPort>,
    /// Builds peer connections; without it the transport is unsupported.
    pub rtc: Option<RtcPeerConnectionFactory>,
    pub ice_servers: Option<Vec<Json>>,
    pub logger: Option<ChannelLogger>,
    /// Give up on one attempt after this long.
    pub connect_timeout: Duration,
}

impl WebRtcOptions {
    pub fn new(signal: Rc<dyn SignalPort>) -> Self {
        WebRtcOptions {
            signal,
            rtc: None,
            ice_servers: None,
            logger: None,
            connect_timeout: Duration::from_secs(15),
        }
    }
}

const DEFAULT_ICE: &str = "stun:stun.cloudflare.com:3478";
const ICE_FLUSH: Duration = Duration::from_millis(20);
const RETRY_BASE_MS: u64 = 2000;
const RETRY_MAX_MS: u64 = 30_000;

type PcFuture = Shared<LocalBoxFuture<'static, Result<Rc<dyn RtcPeerConnection>>>>;

struct Link {
    peer: PeerRef,
    offerer: bool,
    /// One negotiation: answers and candidates of another are ignored.
    sid: String,
    pc: PcFuture,
    dc: RefCell<Option<Rc<dyn RtcDataChannel>>>,
    open: Cell<bool>,
    /// An answer was taken (offerer) or the offer applied (answerer).
    described: Cell<bool>,
    remote_set: Cell<bool>,
    pending_ice: RefCell<Vec<Json>>,
    out_ice: RefCell<Vec<Json>>,
    ice_timer: RefCell<Option<JoinHandle<()>>>,
    connect_timer: RefCell<Option<JoinHandle<()>>>,
    /// Signals of this link go out in order (offer before its candidates).
    queue: RefCell<Shared<LocalBoxFuture<'static, ()>>>,
    closed: Cell<bool>,
}

impl Link {
    fn key(&self) -> String {
        self.peer.key()
    }
}

#[derive(Default)]
struct Retry {
    attempts: u32,
    timer: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct WState {
    links: HashMap<String, Rc<Link>>,
    retries: HashMap<String, Retry>,
    wanted: BTreeSet<String>,
    stop_listen: Option<Box<dyn FnOnce()>>,
    listening_as: Option<String>,
    closed: bool,
}

struct WebRtc {
    hooks: Rc<dyn TransportHooks>,
    opts: WebRtcOptions,
    st: RefCell<WState>,
    me: Weak<WebRtc>,
}

struct WebRtcTransport(Rc<WebRtc>);

/// The WebRTC transport over `opts.signal`, reporting to `hooks`.
pub fn webrtc_transport(
    hooks: Rc<dyn TransportHooks>,
    opts: WebRtcOptions,
) -> Rc<dyn ChannelTransport> {
    Rc::new(WebRtcTransport(Rc::new_cyclic(|me| WebRtc {
        hooks,
        opts,
        st: RefCell::new(WState::default()),
        me: me.clone(),
    })))
}

fn random_sid() -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    (0..8)
        .map(|_| ALPHABET[rand::random_range(0..ALPHABET.len())] as char)
        .collect()
}

fn ready() -> Shared<LocalBoxFuture<'static, ()>> {
    async {}.boxed_local().shared()
}

impl ChannelTransport for WebRtcTransport {
    fn supported(&self) -> bool {
        self.0.opts.rtc.is_some()
    }

    fn want(&self, users: &BTreeSet<String>) {
        let t = &self.0;
        let before = {
            let mut st = t.st.borrow_mut();
            if st.closed || t.opts.rtc.is_none() {
                st.wanted = users.clone();
                return;
            }
            std::mem::replace(&mut st.wanted, users.clone())
        };
        if !users.is_empty() {
            t.ensure_listening();
        }
        let stale: Vec<Rc<Link>> =
            t.st.borrow()
                .links
                .values()
                .filter(|l| !users.contains(&l.peer.user))
                .cloned()
                .collect();
        for link in stale {
            t.drop_link(&link, false);
        }
        for (key, r) in t.st.borrow_mut().retries.iter_mut() {
            let user = key.rsplit_once('#').map(|(u, _)| u).unwrap_or(key);
            if !users.contains(user) {
                if let Some(timer) = r.timer.take() {
                    timer.abort();
                }
            }
        }
        for user in users.difference(&before) {
            t.signal(None, user, None, SignalKind::Hello, None);
            t.hooks.on_status(user, PeerLinkStatus::Connecting);
        }
    }

    fn send(&self, to: &PeerRef, message: &ChannelMessage) {
        let link = self.0.st.borrow().links.get(&to.key()).cloned();
        let Some(link) = link else { return };
        let dc = link.dc.borrow().clone();
        let Some(dc) = dc else { return };
        if !link.open.get() || !dc.is_open() {
            return;
        }
        if let Err(e) = dc.send(&message.to_json().to_string()) {
            self.0.log(&format!("channel send failed: {e}"));
        }
    }

    fn close(&self) {
        let t = &self.0;
        let (links, stop) = {
            let mut st = t.st.borrow_mut();
            st.closed = true;
            for (_, r) in st.retries.drain() {
                if let Some(timer) = r.timer {
                    timer.abort();
                }
            }
            (
                st.links.values().cloned().collect::<Vec<_>>(),
                st.stop_listen.take(),
            )
        };
        for link in links {
            t.drop_link(&link, false);
        }
        if let Some(stop) = stop {
            stop();
        }
    }
}

impl WebRtc {
    fn log(&self, message: &str) {
        match &self.opts.logger {
            Some(log) => log(LogLevel::Debug, message),
            None => tracing::debug!(target: "spooky.channel", "{message}"),
        }
    }

    fn self_key(&self) -> Option<String> {
        self.hooks.self_ref().map(|p| p.key())
    }

    fn signal(
        &self,
        link: Option<&Rc<Link>>,
        to: &str,
        to_peer: Option<&str>,
        kind: SignalKind,
        payload: Option<String>,
    ) {
        let Some(me) = self.hooks.self_ref() else {
            return;
        };
        let out = OutgoingSignal {
            to: to.to_string(),
            to_peer: to_peer.map(str::to_string),
            from_peer: me.peer,
            kind,
            payload,
        };
        let port = self.opts.signal.clone();
        let weak = self.me.clone();
        let run = async move {
            if let Err(e) = port.send(out).await {
                if let Some(t) = weak.upgrade() {
                    t.log(&format!("channel signal ({}) failed: {e}", kind.as_str()));
                }
            }
        };
        match link {
            Some(link) => {
                let prev = link.queue.borrow().clone();
                let next = async move {
                    prev.await;
                    run.await;
                }
                .boxed_local()
                .shared();
                *link.queue.borrow_mut() = next.clone();
                tokio::task::spawn_local(next);
            }
            None => {
                tokio::task::spawn_local(run);
            }
        }
    }

    fn drop_link(&self, link: &Rc<Link>, failed: bool) {
        if link.closed.replace(true) {
            return;
        }
        if let Some(t) = link.ice_timer.borrow_mut().take() {
            t.abort();
        }
        if let Some(t) = link.connect_timer.borrow_mut().take() {
            t.abort();
        }
        if let Some(dc) = link.dc.borrow().clone() {
            tokio::task::spawn_local(async move {
                let _ = dc.close().await;
            });
        }
        let pc = link.pc.clone();
        tokio::task::spawn_local(async move {
            if let Ok(pc) = pc.await {
                // Already gone, most likely.
                let _ = pc.close().await;
            }
        });
        let key = link.key();
        {
            let mut st = self.st.borrow_mut();
            if st.links.get(&key).is_some_and(|l| Rc::ptr_eq(l, link)) {
                st.links.remove(&key);
            }
        }
        if link.open.replace(false) {
            self.hooks.on_peer(&link.peer, false);
        }
        let (wanted, closed, any_open) = {
            let st = self.st.borrow();
            (
                st.wanted.contains(&link.peer.user),
                st.closed,
                st.links
                    .values()
                    .any(|l| l.peer.user == link.peer.user && l.open.get()),
            )
        };
        if failed && wanted && !closed {
            if !any_open {
                self.hooks
                    .on_status(&link.peer.user, PeerLinkStatus::Failed);
            }
            self.schedule_retry(&link.peer);
        }
    }

    fn schedule_retry(&self, peer: &PeerRef) {
        let key = peer.key();
        let mut st = self.st.borrow_mut();
        let r = st.retries.entry(key).or_default();
        if r.timer.is_some() {
            return;
        }
        let delay = RETRY_BASE_MS
            .saturating_mul(1u64 << r.attempts.min(16))
            .min(RETRY_MAX_MS);
        r.attempts += 1;
        let (weak, peer) = (self.me.clone(), peer.clone());
        r.timer = Some(tokio::task::spawn_local(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if let Some(t) = weak.upgrade() {
                t.retry(&peer);
            }
        }));
    }

    fn retry(&self, peer: &PeerRef) {
        let key = peer.key();
        {
            let mut st = self.st.borrow_mut();
            if let Some(r) = st.retries.get_mut(&key) {
                r.timer = None;
            }
            if st.closed || !st.wanted.contains(&peer.user) || st.links.contains_key(&key) {
                return;
            }
        }
        let Some(mine) = self.self_key() else { return };
        if mine < key {
            self.start_offer(peer);
        } else {
            self.signal(None, &peer.user, Some(&peer.peer), SignalKind::Hello, None);
        }
    }

    fn attach_channel(&self, link: &Rc<Link>, dc: Rc<dyn RtcDataChannel>) {
        *link.dc.borrow_mut() = Some(dc.clone());
        let (t, l) = (self.me.clone(), Rc::downgrade(link));
        dc.set_on_open(Some(Rc::new(move || {
            let (Some(t), Some(link)) = (t.upgrade(), l.upgrade()) else {
                return;
            };
            if link.closed.get() {
                return;
            }
            link.open.set(true);
            if let Some(timer) = link.connect_timer.borrow_mut().take() {
                timer.abort();
            }
            t.st.borrow_mut().retries.remove(&link.key());
            t.hooks.on_peer(&link.peer, true);
            t.hooks
                .on_status(&link.peer.user, PeerLinkStatus::Connected);
        })));
        let (t, l) = (self.me.clone(), Rc::downgrade(link));
        dc.set_on_close(Some(Rc::new(move || {
            if let (Some(t), Some(link)) = (t.upgrade(), l.upgrade()) {
                t.drop_link(&link, true);
            }
        })));
        let (t, l) = (self.me.clone(), Rc::downgrade(link));
        dc.set_on_message(Some(Rc::new(move |text: String| {
            let (Some(t), Some(link)) = (t.upgrade(), l.upgrade()) else {
                return;
            };
            let Ok(message) = serde_json::from_str::<Json>(&text) else {
                return;
            };
            t.hooks.on_message(&link.peer, &message);
        })));
    }

    fn new_link(&self, peer: &PeerRef, offerer: bool, sid: String) -> Option<Rc<Link>> {
        let rtc = self.opts.rtc.clone()?;
        let prev = self.st.borrow().links.get(&peer.key()).cloned();
        if let Some(prev) = prev {
            self.drop_link(&prev, false);
        }
        let ice: Json = self
            .opts
            .ice_servers
            .clone()
            .map(Json::Array)
            .unwrap_or_else(|| json!([{"urls": DEFAULT_ICE}]));
        let created = rtc(json!({"iceServers": ice}));
        let t = self.me.clone();
        let link = Rc::new_cyclic(|weak: &Weak<Link>| {
            let w = weak.clone();
            // The handlers go in before anything awaiting the connection can
            // apply a description.
            let pc = async move {
                let pc = match created.await {
                    Ok(pc) => pc,
                    Err(e) => {
                        if let (Some(t), Some(link)) = (t.upgrade(), w.upgrade()) {
                            t.log(&format!("channel peer connection failed: {e}"));
                            t.drop_link(&link, true);
                        }
                        return Err(e);
                    }
                };
                let Some(link) = w.upgrade() else {
                    return Ok(pc);
                };
                if link.closed.get() {
                    let closing = pc.clone();
                    tokio::task::spawn_local(async move {
                        let _ = closing.close().await;
                    });
                    return Ok(pc);
                }
                let (t1, l1) = (t.clone(), w.clone());
                pc.set_on_ice_candidate(Some(Rc::new(move |candidate: Json| {
                    let (Some(t), Some(link)) = (t1.upgrade(), l1.upgrade()) else {
                        return;
                    };
                    t.queue_ice(&link, candidate);
                })));
                let (t2, l2) = (t.clone(), w.clone());
                pc.set_on_connection_state(Some(Rc::new(move |state: &str| {
                    if state == "failed" || state == "closed" {
                        if let (Some(t), Some(link)) = (t2.upgrade(), l2.upgrade()) {
                            t.drop_link(&link, true);
                        }
                    }
                })));
                if !link.offerer {
                    let (t3, l3) = (t.clone(), w.clone());
                    pc.set_on_data_channel(Some(Rc::new(move |dc: Rc<dyn RtcDataChannel>| {
                        if let (Some(t), Some(link)) = (t3.upgrade(), l3.upgrade()) {
                            t.attach_channel(&link, dc);
                        }
                    })));
                }
                Ok(pc)
            }
            .boxed_local()
            .shared();
            Link {
                peer: peer.clone(),
                offerer,
                sid,
                pc,
                dc: RefCell::new(None),
                open: Cell::new(false),
                described: Cell::new(false),
                remote_set: Cell::new(false),
                pending_ice: RefCell::new(Vec::new()),
                out_ice: RefCell::new(Vec::new()),
                ice_timer: RefCell::new(None),
                connect_timer: RefCell::new(None),
                queue: RefCell::new(ready()),
                closed: Cell::new(false),
            }
        });
        self.st.borrow_mut().links.insert(link.key(), link.clone());
        // Drive the connection even before anyone awaits it.
        tokio::task::spawn_local(link.pc.clone().map(|_| ()));
        let (t, l, timeout) = (
            self.me.clone(),
            Rc::downgrade(&link),
            self.opts.connect_timeout,
        );
        *link.connect_timer.borrow_mut() = Some(tokio::task::spawn_local(async move {
            tokio::time::sleep(timeout).await;
            if let (Some(t), Some(link)) = (t.upgrade(), l.upgrade()) {
                link.connect_timer.borrow_mut().take();
                if !link.open.get() {
                    t.drop_link(&link, true);
                }
            }
        }));
        self.hooks.on_status(&peer.user, PeerLinkStatus::Connecting);
        Some(link)
    }

    /// Candidates of one burst go out as one signal.
    fn queue_ice(&self, link: &Rc<Link>, candidate: Json) {
        if link.closed.get() {
            return;
        }
        link.out_ice.borrow_mut().push(candidate);
        if link.ice_timer.borrow().is_some() {
            return;
        }
        let (t, l) = (self.me.clone(), Rc::downgrade(link));
        *link.ice_timer.borrow_mut() = Some(tokio::task::spawn_local(async move {
            tokio::time::sleep(ICE_FLUSH).await;
            let (Some(t), Some(link)) = (t.upgrade(), l.upgrade()) else {
                return;
            };
            link.ice_timer.borrow_mut().take();
            let candidates = std::mem::take(&mut *link.out_ice.borrow_mut());
            if candidates.is_empty() || link.closed.get() {
                return;
            }
            let payload = json!({"sid": link.sid, "candidates": candidates}).to_string();
            t.signal(
                Some(&link),
                &link.peer.user,
                Some(&link.peer.peer),
                SignalKind::Ice,
                Some(payload),
            );
        }));
    }

    fn start_offer(&self, peer: &PeerRef) {
        let Some(link) = self.new_link(peer, true, random_sid()) else {
            return;
        };
        let t = self.me.clone();
        tokio::task::spawn_local(async move {
            let Some(this) = t.upgrade() else { return };
            let run = async {
                let pc = link.pc.clone().await?;
                if link.closed.get() {
                    return Ok(());
                }
                let dc = pc.create_data_channel("sp00ky", true).await?;
                this.attach_channel(&link, dc);
                let offer = pc.create_offer().await?;
                pc.set_local_description(offer.clone()).await?;
                let local = pc.local_description().await?;
                if link.closed.get() {
                    return Ok(());
                }
                let sdp = local.map(|d| d.sdp).unwrap_or(offer.sdp);
                let payload = json!({"sid": link.sid, "sdp": sdp}).to_string();
                this.signal(
                    Some(&link),
                    &link.peer.user,
                    Some(&link.peer.peer),
                    SignalKind::Offer,
                    Some(payload),
                );
                Ok::<(), Error>(())
            };
            if let Err(e) = run.await {
                this.log(&format!("channel offer failed: {e}"));
                this.drop_link(&link, true);
            }
        });
    }

    async fn add_ice(link: &Link, candidates: Vec<Json>) {
        let Ok(pc) = link.pc.clone().await else {
            return;
        };
        for c in candidates {
            // A candidate of a torn-down negotiation, or one the platform
            // rejects.
            let _ = pc.add_ice_candidate(c).await;
        }
    }

    fn on_signal(&self, s: Signal) {
        if self.st.borrow().closed {
            return;
        }
        let Some(me) = self.hooks.self_ref() else {
            return;
        };
        let Some(from) = id_key(&Value::String(s.from.clone())) else {
            return;
        };
        if !self.st.borrow().wanted.contains(&from) {
            return;
        }
        if s.to_peer.as_ref().is_some_and(|p| *p != me.peer) {
            return;
        }
        let peer = PeerRef::new(from.clone(), s.from_peer.clone());
        let key = peer.key();
        let mine = me.key();
        if key == mine {
            return;
        }
        let payload = parse_payload(s.payload.as_deref());
        let field = |name: &str| payload.as_ref().and_then(|p| p.get(name)).cloned();
        let link = self.st.borrow().links.get(&key).cloned();

        match s.kind {
            SignalKind::Hello => {
                if link.is_some_and(|l| !l.closed.get()) {
                    return;
                }
                if mine < key {
                    self.start_offer(&peer);
                } else if s.to_peer.is_none() {
                    self.signal(None, &from, Some(&peer.peer), SignalKind::Hello, None);
                }
            }
            SignalKind::Offer => {
                let (Some(Json::String(sdp)), Some(Json::String(sid))) =
                    (field("sdp"), field("sid"))
                else {
                    return;
                };
                if mine < key {
                    return;
                }
                let Some(answerer) = self.new_link(&peer, false, sid) else {
                    return;
                };
                answerer.described.set(true);
                let t = self.me.clone();
                tokio::task::spawn_local(async move {
                    let Some(this) = t.upgrade() else { return };
                    let run = async {
                        let pc = answerer.pc.clone().await?;
                        pc.set_remote_description(RtcDescription::new("offer", sdp))
                            .await?;
                        answerer.remote_set.set(true);
                        let pending = std::mem::take(&mut *answerer.pending_ice.borrow_mut());
                        Self::add_ice(&answerer, pending).await;
                        let answer = pc.create_answer().await?;
                        pc.set_local_description(answer.clone()).await?;
                        let local = pc.local_description().await?;
                        if answerer.closed.get() {
                            return Ok(());
                        }
                        let sdp = local.map(|d| d.sdp).unwrap_or(answer.sdp);
                        let payload = json!({"sid": answerer.sid, "sdp": sdp}).to_string();
                        this.signal(
                            Some(&answerer),
                            &answerer.peer.user,
                            Some(&answerer.peer.peer),
                            SignalKind::Answer,
                            Some(payload),
                        );
                        Ok::<(), Error>(())
                    };
                    if let Err(e) = run.await {
                        this.log(&format!("channel answer failed: {e}"));
                        this.drop_link(&answerer, true);
                    }
                });
            }
            SignalKind::Answer => {
                let Some(link) = link else { return };
                let Some(Json::String(sdp)) = field("sdp") else {
                    return;
                };
                if !link.offerer
                    || link.described.get()
                    || field("sid") != Some(Json::String(link.sid.clone()))
                {
                    return;
                }
                link.described.set(true);
                let t = self.me.clone();
                tokio::task::spawn_local(async move {
                    let Some(this) = t.upgrade() else { return };
                    let run = async {
                        let pc = link.pc.clone().await?;
                        pc.set_remote_description(RtcDescription::new("answer", sdp))
                            .await?;
                        link.remote_set.set(true);
                        let pending = std::mem::take(&mut *link.pending_ice.borrow_mut());
                        Self::add_ice(&link, pending).await;
                        Ok::<(), Error>(())
                    };
                    if run.await.is_err() {
                        this.drop_link(&link, true);
                    }
                });
            }
            SignalKind::Ice => {
                let Some(link) = link else { return };
                let Some(Json::Array(candidates)) = field("candidates") else {
                    return;
                };
                if field("sid") != Some(Json::String(link.sid.clone())) {
                    return;
                }
                let list: Vec<Json> = candidates.into_iter().filter(Json::is_object).collect();
                if link.remote_set.get() {
                    tokio::task::spawn_local(async move { Self::add_ice(&link, list).await });
                } else {
                    link.pending_ice.borrow_mut().extend(list);
                }
            }
        }
    }

    fn ensure_listening(&self) {
        let me = self.hooks.self_ref();
        let key = me.as_ref().map(PeerRef::key);
        let stop = {
            let mut st = self.st.borrow_mut();
            if st.listening_as == key {
                return;
            }
            st.listening_as = key;
            st.stop_listen.take()
        };
        if let Some(stop) = stop {
            stop();
        }
        if let Some(me) = me {
            let weak = self.me.clone();
            let stop = self.opts.signal.listen(
                &me,
                Rc::new(move |s: Signal| {
                    if let Some(t) = weak.upgrade() {
                        t.on_signal(s);
                    }
                }),
            );
            self.st.borrow_mut().stop_listen = Some(stop);
        }
    }
}

fn parse_payload(payload: Option<&str>) -> Option<serde_json::Map<String, Json>> {
    match serde_json::from_str::<Json>(payload.filter(|p| !p.is_empty())?) {
        Ok(Json::Object(o)) => Some(o),
        _ => None,
    }
}

// ---- signaling over SurrealDB --------------------------------------------------------------

pub const SIGNAL_TABLE: &str = "_00_channel_signal";
const LISTEN_RETRY: Duration = Duration::from_secs(3);

/// The bits of the remote service signaling needs.
#[async_trait(?Send)]
pub trait SignalRemote {
    async fn query(&self, sql: &str, vars: Object) -> Result<Vec<Value>>;
    async fn live(&self, sql: &str, vars: Object) -> Result<(String, LiveStream)>;
    async fn kill(&self, live_id: &str) -> Result<()>;
    /// Called at once with the current state, then on every change, until the
    /// returned unsubscribe runs.
    fn subscribe_connection(&self, cb: Box<dyn Fn(ConnectionState)>) -> Box<dyn FnOnce()>;
}

/// Signals as rows of `_00_channel_signal` (generated for schemas with a
/// `@channel` table): a CREATE to send, one raw `LIVE SELECT` on rows
/// addressed to this user to receive. Not the outbox and not a synced query:
/// each signal costs one round trip, which is what keeps connection setup
/// short. The LIVE is re-armed whenever the socket reconnects.
pub fn surreal_signal(
    remote: Rc<dyn SignalRemote>,
    logger: Option<ChannelLogger>,
) -> Rc<dyn SignalPort> {
    Rc::new(SurrealSignal { remote, logger })
}

struct SurrealSignal {
    remote: Rc<dyn SignalRemote>,
    logger: Option<ChannelLogger>,
}

#[derive(Default)]
struct Listening {
    stopped: bool,
    live_id: Option<String>,
    reader: Option<JoinHandle<()>>,
    retry: Option<JoinHandle<()>>,
    state: Option<ConnectionState>,
}

struct Listener {
    remote: Rc<dyn SignalRemote>,
    logger: Option<ChannelLogger>,
    user: String,
    cb: Rc<dyn Fn(Signal)>,
    st: RefCell<Listening>,
}

impl Listener {
    fn kill_live(&self, id: String) {
        let remote = self.remote.clone();
        tokio::task::spawn_local(async move {
            let _ = remote.kill(&id).await;
        });
    }

    fn arm(self: Rc<Self>) -> LocalBoxFuture<'static, ()> {
        async move {
            let old = {
                let mut st = self.st.borrow_mut();
                if st.stopped {
                    return;
                }
                if let Some(r) = st.retry.take() {
                    r.abort();
                }
                if let Some(r) = st.reader.take() {
                    r.abort();
                }
                st.live_id.take()
            };
            if let Some(old) = old {
                self.kill_live(old);
            }
            let sql = format!("LIVE SELECT * FROM {SIGNAL_TABLE} WHERE recipient = $me");
            let vars = Object::from([(
                "me".to_string(),
                Value::RecordId(RecordId::parse(&self.user)),
            )]);
            match self.remote.live(&sql, vars).await {
                Ok((id, mut stream)) => {
                    if self.st.borrow().stopped {
                        return self.kill_live(id);
                    }
                    let me = Rc::downgrade(&self);
                    let reader = tokio::task::spawn_local(async move {
                        while let Some(message) = stream.recv().await {
                            let Some(me) = me.upgrade() else { return };
                            if me.st.borrow().stopped || message.action != "CREATE" {
                                continue;
                            }
                            if let Some(signal) = signal_of(&message.value) {
                                (me.cb)(signal);
                            }
                        }
                    });
                    let mut st = self.st.borrow_mut();
                    st.live_id = Some(id);
                    st.reader = Some(reader);
                }
                Err(e) => {
                    let message = format!("channel signal listen failed; retrying: {e}");
                    match &self.logger {
                        Some(log) => log(LogLevel::Debug, &message),
                        None => tracing::debug!(target: "spooky.channel", "{message}"),
                    }
                    if self.st.borrow().stopped {
                        return;
                    }
                    let me = self.clone();
                    let retry = tokio::task::spawn_local(async move {
                        tokio::time::sleep(LISTEN_RETRY).await;
                        me.st.borrow_mut().retry.take();
                        me.arm().await;
                    });
                    self.st.borrow_mut().retry = Some(retry);
                }
            }
        }
        .boxed_local()
    }
}

fn signal_of(v: &Object) -> Option<Signal> {
    let from = id_key(v.get("sender")?)?;
    let from_peer = v.get("sender_peer")?.as_str()?.to_string();
    let kind = SignalKind::parse(v.get("kind")?.as_str()?)?;
    Some(Signal {
        from,
        from_peer,
        to_peer: v
            .get("recipient_peer")
            .and_then(Value::as_str)
            .map(str::to_string),
        kind,
        payload: v.get("payload").and_then(Value::as_str).map(str::to_string),
    })
}

#[async_trait(?Send)]
impl SignalPort for SurrealSignal {
    async fn send(&self, s: OutgoingSignal) -> Result<()> {
        let mut c = Object::new();
        c.insert("recipient".into(), Value::RecordId(RecordId::parse(&s.to)));
        c.insert("sender_peer".into(), Value::from(s.from_peer));
        c.insert("kind".into(), Value::from(s.kind.as_str()));
        if let Some(p) = s.to_peer {
            c.insert("recipient_peer".into(), Value::from(p));
        }
        if let Some(p) = s.payload {
            c.insert("payload".into(), Value::from(p));
        }
        let sql = format!("CREATE {SIGNAL_TABLE} CONTENT $c RETURN NONE");
        self.remote
            .query(&sql, Object::from([("c".to_string(), Value::Object(c))]))
            .await
            .map(|_| ())
    }

    fn listen(&self, me: &PeerRef, cb: Rc<dyn Fn(Signal)>) -> Box<dyn FnOnce()> {
        let listener = Rc::new(Listener {
            remote: self.remote.clone(),
            logger: self.logger.clone(),
            user: me.user.clone(),
            cb,
            st: RefCell::new(Listening::default()),
        });
        let weak = Rc::downgrade(&listener);
        let unsubscribe = self.remote.subscribe_connection(Box::new(move |next| {
            let Some(l) = weak.upgrade() else { return };
            let was = l.st.borrow_mut().state.replace(next);
            if next == ConnectionState::Connected
                && was.is_some_and(|w| w != ConnectionState::Connected)
            {
                tokio::task::spawn_local(l.arm());
            }
        }));
        tokio::task::spawn_local(listener.clone().arm());
        Box::new(move || {
            let id = {
                let mut st = listener.st.borrow_mut();
                st.stopped = true;
                if let Some(r) = st.retry.take() {
                    r.abort();
                }
                if let Some(r) = st.reader.take() {
                    r.abort();
                }
                st.live_id.take()
            };
            unsubscribe();
            if let Some(id) = id {
                listener.kill_live(id);
            }
        })
    }
}

#[cfg(test)]
mod tests;
