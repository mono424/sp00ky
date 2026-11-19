//! The room: how the devices of one user reach each other.
//!
//! PORT: `packages/core/src/modules/channel/room.ts` (the link to the relay)
//! and `hub.ts` (what the members of a room say to each other), as a client
//! that is one process needs them. The frames are the browser's, so the
//! phone, the laptop's browser and a Rust app of one user sit in one room.
//!
//! Every client keeps one WebSocket to the room relay (`sp00ky-relay`, see
//! `apps/relay`). The relay proves the user from the session token, puts the
//! socket into that user's room and forwards what the members send each other
//! without reading it. So a message is there after one hop to the relay and
//! one back, whatever network each device sits in, and nothing waits for a
//! connection to be negotiated.
//!
//! A browser shares its socket between its tabs, so a member of the room
//! speaks for a list of tabs, and every message names the tab it comes from
//! and the tabs it is for. To the [`super::ChannelManager`] a peer is one tab.
//! This client is a member that speaks for exactly one: itself.
//!
//! Two browsers alone in a room also open a WebRTC link to each other, set up
//! through the relay. This client has no WebRTC of its own here and does not
//! answer; the browser then simply keeps talking through the relay.
//!
//! Nothing here is kept or retried: a message lost on the way is one the
//! normal sync delivers a moment later.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::{Rc, Weak};
use std::time::Duration;

use futures::future::{FutureExt, LocalBoxFuture};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Map as JsonMap, Value as Json};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::Connector;

use super::manager::log_to;
use super::{
    ChannelLogger, ChannelTransport, ChannelWire, PeerLinkStatus, PeerRef, TransportHooks,
};
use crate::error::{Error, Result};
use crate::kernel::events::LogLevel;

/// The relay protocol this client speaks (`apps/relay/README.md`).
const PROTOCOL: u64 = 1;
const RETRY_BASE_MS: u64 = 500;
const RETRY_MAX_MS: u64 = 30_000;
/// After a refusal that a retry will not fix soon (not signed in at the relay,
/// room full).
const RETRY_REFUSED: Duration = Duration::from_secs(30);
const BEAT: Duration = Duration::from_secs(20);
/// How long the relay has to answer the hello. It asks the database who the
/// token belongs to and gives that 10 s; a socket that stays silent longer
/// leads nowhere, and without this nothing would ever notice.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a close may take before the socket is simply dropped.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
/// Frames that may wait on either side of a socket. A frame past that is
/// dropped: nothing here is worth holding memory for.
const QUEUE_FRAMES: usize = 256;
const MAX_TABS: usize = 64;

/// Two spellings of one record id. The database prints a key the way it would
/// have to be written (`user:⟨a-b⟩`, backticks, or `user:u'0190…'` for a
/// uuid), the client keeps it bare (`user:a-b`): the wrapping is not part of
/// who it is. Verified against SurrealDB 4.0.1.
fn bare_id(id: &str) -> String {
    let (table, key) = match id.find(':') {
        Some(at) => id.split_at(at + 1),
        None => ("", id),
    };
    let key = match key.strip_prefix('u') {
        Some(rest) if rest.starts_with(['\'', '"']) => rest,
        _ => key,
    };
    let key = key
        .chars()
        .filter(|c| !matches!(c, '⟨' | '⟩' | '`' | '\'' | '"'));
    table.chars().chain(key).collect()
}

/// Whether two record ids name one user, whatever escaping the database
/// added.
pub fn same_user(a: &str, b: &str) -> bool {
    bare_id(a) == bare_id(b)
}

/// What the relay takes as a peer id: 1 to 64 of `[A-Za-z0-9_-]`.
fn valid_peer_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn is_tab_id(id: &str) -> bool {
    (1..=64).contains(&id.encode_utf16().count())
}

/// A list of tab ids as a member sends it, or `None` when it is not one.
fn tab_list(v: Option<&Json>) -> Option<Vec<String>> {
    let list = v?.as_array()?;
    if list.len() > MAX_TABS {
        return None;
    }
    list.iter()
        .map(|t| t.as_str().filter(|t| is_tab_id(t)).map(str::to_string))
        .collect()
}

// ---- the socket ----------------------------------------------------------------------------

/// One WebSocket to the relay, as the link uses it: text frames out, text
/// frames in. Dropping `out` closes it; `frames` ends when it is gone,
/// whoever closed it.
pub struct RoomSocket {
    pub out: mpsc::Sender<String>,
    pub frames: mpsc::Receiver<String>,
}

/// Opens the socket to the relay at a `ws://` or `wss://` address.
/// [`ws_connector`] by default; tests hand in a relay that lives in memory.
pub type RoomConnector = Rc<dyn Fn(String) -> LocalBoxFuture<'static, Result<RoomSocket>>>;

/// The session token the relay proves the user with; `None` while there is
/// none.
pub type RoomToken = Rc<dyn Fn() -> Option<String>>;

/// The real socket, over the WebSocket stack the database connection uses.
pub fn ws_connector() -> RoomConnector {
    Rc::new(|url: String| open_socket(url).boxed_local())
}

async fn open_socket(url: String) -> Result<RoomSocket> {
    let request = url
        .as_str()
        .into_client_request()
        .map_err(|e| Error::Network(e.to_string()))?;
    let connector = if url.starts_with("wss://") {
        Connector::Rustls(crate::surreal::rpc::tls_config()?)
    } else {
        Connector::Plain
    };
    let (mut socket, _) =
        tokio_tungstenite::connect_async_tls_with_config(request, None, true, Some(connector))
            .await
            .map_err(|e| Error::Network(e.to_string()))?;
    let (out, mut outbound) = mpsc::channel::<String>(QUEUE_FRAMES);
    let (inbound, frames) = mpsc::channel::<String>(QUEUE_FRAMES);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                next = outbound.recv() => match next {
                    Some(text) => {
                        if socket.send(Message::text(text)).await.is_err() {
                            break;
                        }
                    }
                    // The link let go of the socket.
                    None => break,
                },
                // Reading is also what answers the relay's pings.
                frame = socket.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
                        if inbound.send(text.as_str().to_owned()).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                },
            }
        }
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, socket.close(None)).await;
    });
    Ok(RoomSocket { out, frames })
}

/// What a room is built with.
pub struct RoomOptions {
    /// The relay's WebSocket address, e.g. `wss://relay.example.com/room`.
    pub url: String,
    pub token: RoomToken,
    pub connect: RoomConnector,
    pub logger: Option<ChannelLogger>,
    /// Give up on opening the socket after this long: a refused port fails
    /// fast, a black-holed one never answers.
    pub connect_timeout: Duration,
    /// A number in `0..1` that spreads the reconnects of many clients (tests
    /// pin it).
    pub random: Rc<dyn Fn() -> f64>,
}

impl RoomOptions {
    pub fn new(url: impl Into<String>, token: impl Fn() -> Option<String> + 'static) -> Self {
        RoomOptions {
            url: url.into(),
            token: Rc::new(token),
            connect: ws_connector(),
            logger: None,
            connect_timeout: Duration::from_secs(15),
            random: Rc::new(rand::random::<f64>),
        }
    }

    fn log(&self, level: LogLevel, message: &str) {
        log_to(&self.logger, level, message);
    }
}

// ---- the link: one socket, the members of the room --------------------------------------

/// How the link reports: who is in the room (by the peer id each joined
/// with), what they send, and how the socket is doing.
trait LinkHooks {
    /// Who this client is, or `None` while signed out.
    fn self_ref(&self) -> Option<PeerRef>;
    fn on_peer(&self, member: &str, up: bool);
    fn on_message(&self, from: &str, data: &Json);
    fn on_status(&self, status: PeerLinkStatus);
}

#[derive(Default)]
struct LinkState {
    peers: BTreeSet<String>,
    out: Option<mpsc::Sender<String>>,
    welcomed: bool,
    closed: bool,
}

/// How one socket ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    /// It went away, or never opened: try again, a little later each time.
    Dropped,
    /// The relay said no in a way that asking again at once will not fix.
    Refused,
    /// The relay ended a long session: come back with the token of now.
    Reauth,
}

struct Link {
    st: Rc<RefCell<LinkState>>,
    opts: Rc<RoomOptions>,
    task: JoinHandle<()>,
}

struct LinkTask {
    hooks: Rc<dyn LinkHooks>,
    opts: Rc<RoomOptions>,
    st: Rc<RefCell<LinkState>>,
}

async fn next_beat(beat: &mut Option<Interval>) {
    match beat {
        Some(beat) => {
            beat.tick().await;
        }
        None => std::future::pending().await,
    }
}

impl Link {
    fn open(hooks: Rc<dyn LinkHooks>, opts: Rc<RoomOptions>) -> Link {
        let st: Rc<RefCell<LinkState>> = Rc::default();
        hooks.on_status(PeerLinkStatus::Connecting);
        let run = LinkTask {
            hooks,
            opts: opts.clone(),
            st: st.clone(),
        };
        Link {
            st,
            opts,
            task: tokio::task::spawn_local(run.run()),
        }
    }

    /// Send to these members of the room, or (`None`) to all of them. Fire
    /// and forget.
    fn send(&self, to: Option<&[String]>, data: &Json) {
        let st = self.st.borrow();
        let Some(out) = st.out.as_ref().filter(|_| st.welcomed && !st.closed) else {
            return;
        };
        let targets: Vec<&String> = match to {
            None => st.peers.iter().collect(),
            Some(list) => list.iter().filter(|p| st.peers.contains(*p)).collect(),
        };
        if targets.is_empty() {
            return;
        }
        // To all of them is one frame without a list.
        let frame = if targets.len() == st.peers.len() {
            json!({"t": "msg", "d": {"m": data}})
        } else {
            json!({"t": "msg", "to": targets, "d": {"m": data}})
        };
        if out.try_send(frame.to_string()).is_err() {
            self.opts.log(LogLevel::Debug, "room send failed");
        }
    }

    fn close(&self) {
        self.task.abort();
        let mut st = self.st.borrow_mut();
        st.closed = true;
        st.welcomed = false;
        st.out = None;
        st.peers.clear();
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl LinkTask {
    /// Hold a socket to the relay for as long as somebody is signed in, and
    /// open the next one when it goes.
    async fn run(self) {
        let mut attempts: u32 = 0;
        loop {
            let Some(me) = self.hooks.self_ref() else {
                return;
            };
            let Some(token) = (self.opts.token)() else {
                // Signed in, and the session has no token yet (it is being
                // restored).
                tokio::time::sleep(Duration::from_millis(RETRY_BASE_MS)).await;
                continue;
            };
            let end = self.session(&me, &token, &mut attempts).await;
            // The socket is gone: every member with it.
            let was = {
                let mut st = self.st.borrow_mut();
                st.out = None;
                st.welcomed = false;
                std::mem::take(&mut st.peers)
            };
            for member in &was {
                self.hooks.on_peer(member, false);
            }
            if self.st.borrow().closed {
                return;
            }
            // `reauth` is no failure: the first step of the backoff, about
            // half a second, is all it waits.
            if end == End::Reauth {
                attempts = 0;
            }
            attempts += 1;
            let refused = end == End::Refused;
            self.hooks.on_status(if refused || attempts > 2 {
                PeerLinkStatus::Failed
            } else {
                PeerLinkStatus::Connecting
            });
            let delay = if refused {
                RETRY_REFUSED
            } else {
                let backoff = RETRY_BASE_MS
                    .saturating_mul(1 << (attempts - 1).min(16))
                    .min(RETRY_MAX_MS);
                Duration::from_millis(backoff).mul_f64(0.75 + (self.opts.random)() * 0.5)
            };
            tokio::time::sleep(delay).await;
        }
    }

    fn joined(&self, me: &PeerRef, member: &str) {
        {
            // Closed from inside an earlier report of this very frame: the
            // rest of it is nobody's business anymore.
            let mut st = self.st.borrow_mut();
            if st.closed || member == me.peer || !st.peers.insert(member.to_string()) {
                return;
            }
        }
        self.hooks.on_peer(member, true);
    }

    /// One socket, from the hello to its end.
    async fn session(&self, me: &PeerRef, token: &str, attempts: &mut u32) -> End {
        let opening = (self.opts.connect)(self.opts.url.clone());
        let socket = match tokio::time::timeout(self.opts.connect_timeout, opening).await {
            Ok(Ok(socket)) => socket,
            Ok(Err(e)) => {
                let message = format!("the room relay cannot be reached: {e}");
                self.opts.log(LogLevel::Debug, &message);
                return End::Dropped;
            }
            Err(_) => {
                self.opts
                    .log(LogLevel::Debug, "the room relay did not answer in time");
                return End::Dropped;
            }
        };
        let RoomSocket { out, mut frames } = socket;
        let hello = json!({"t": "hello", "v": PROTOCOL, "token": token, "peer": me.peer});
        if out.try_send(hello.to_string()).is_err() {
            return End::Dropped;
        }
        self.st.borrow_mut().out = Some(out.clone());
        let mut welcomed = false;
        let mut awaiting_pong = false;
        let mut beat: Option<Interval> = None;
        let unanswered = tokio::time::sleep(WELCOME_TIMEOUT);
        tokio::pin!(unanswered);
        loop {
            let text = tokio::select! {
                frame = frames.recv() => match frame {
                    Some(text) => text,
                    None => return End::Dropped,
                },
                _ = &mut unanswered, if !welcomed => {
                    self.opts
                        .log(LogLevel::Debug, "the room relay did not answer the hello");
                    return End::Dropped;
                }
                _ = next_beat(&mut beat) => {
                    // Two beats without an answer in between: the socket is
                    // dead, however long the process was held up in between.
                    if awaiting_pong {
                        return End::Dropped;
                    }
                    awaiting_pong = true;
                    let _ = out.try_send(r#"{"t":"ping"}"#.to_string());
                    continue;
                }
            };
            // Closed from inside a report of the frame before: the task ends
            // at its next pause, and until then nothing more is said.
            if self.st.borrow().closed {
                return End::Dropped;
            }
            let Ok(Json::Object(f)) = serde_json::from_str::<Json>(&text) else {
                continue;
            };
            let field = |name: &str| f.get(name).and_then(Json::as_str);
            match field("t") {
                Some("welcome") if !welcomed => {
                    if !field("user").is_some_and(|user| same_user(user, &me.user)) {
                        // The token belongs to somebody else than the client
                        // thinks is signed in: not a room to speak in under
                        // this name.
                        self.opts.log(
                            LogLevel::Warn,
                            "the room relay answered for another user; not joining",
                        );
                        return End::Refused;
                    }
                    welcomed = true;
                    *attempts = 0;
                    awaiting_pong = false;
                    let mut every = tokio::time::interval_at(Instant::now() + BEAT, BEAT);
                    every.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    beat = Some(every);
                    self.st.borrow_mut().welcomed = true;
                    self.hooks.on_status(PeerLinkStatus::Connected);
                    for member in f
                        .get("peers")
                        .and_then(Json::as_array)
                        .into_iter()
                        .flatten()
                    {
                        if let Some(member) = member.as_str() {
                            self.joined(me, member);
                        }
                    }
                }
                Some("join") if welcomed => {
                    if let Some(member) = field("peer") {
                        self.joined(me, member);
                    }
                }
                Some("leave") => {
                    let Some(member) = field("peer") else {
                        continue;
                    };
                    if self.st.borrow_mut().peers.remove(member) {
                        self.hooks.on_peer(member, false);
                    }
                }
                Some("msg") => {
                    let Some(from) = field("from") else { continue };
                    let Some(d) = f.get("d").and_then(Json::as_object) else {
                        continue;
                    };
                    if !self.st.borrow().peers.contains(from) {
                        continue;
                    }
                    // `s` is the signaling of the browsers' direct WebRTC
                    // link. Nobody answers it here, and the browser stays on
                    // the relay.
                    if d.contains_key("s") {
                        continue;
                    }
                    if let Some(m) = d.get("m") {
                        self.hooks.on_message(from, m);
                    }
                }
                Some("pong") => awaiting_pong = false,
                Some("error") => {
                    let code = field("code").unwrap_or("unknown");
                    self.opts
                        .log(LogLevel::Debug, &format!("room relay refused: {code}"));
                    return if code == "reauth" {
                        End::Reauth
                    } else {
                        End::Refused
                    };
                }
                _ => {}
            }
        }
    }
}

// ---- the transport: a member of the room that speaks for one tab ---------------------------

struct RoomState {
    /// This client while it takes part, else `None`.
    me: Option<PeerRef>,
    link: Option<Link>,
    /// The other members of the room and the tabs each speaks for.
    members: BTreeMap<String, BTreeSet<String>>,
    /// The tabs reported to the manager as up.
    up: BTreeSet<String>,
    status: PeerLinkStatus,
    reported: Option<PeerLinkStatus>,
    /// Messages of this turn, each with the tabs it goes to: one frame per
    /// message, not one per tab.
    pending: Vec<(Json, Vec<String>)>,
    flusher: Option<JoinHandle<()>>,
}

struct Room {
    hooks: Rc<dyn TransportHooks>,
    opts: Rc<RoomOptions>,
    st: RefCell<RoomState>,
    /// Wakes the flusher.
    wake: Rc<Notify>,
    me: Weak<Room>,
}

struct RoomTransport(Rc<Room>);

struct RoomLinkHooks(Weak<Room>);

/// The transport to the user's own devices: one socket to the relay, joined
/// while the manager wants the signed-in user, speaking for this client alone.
/// Other users are not its business (see [`super::split_transport`]).
pub fn room_transport(
    hooks: Rc<dyn TransportHooks>,
    opts: RoomOptions,
) -> Rc<dyn ChannelTransport> {
    Rc::new(RoomTransport(Rc::new_cyclic(|me| Room {
        hooks,
        opts: Rc::new(opts),
        st: RefCell::new(RoomState {
            me: None,
            link: None,
            members: BTreeMap::new(),
            up: BTreeSet::new(),
            status: PeerLinkStatus::Connecting,
            reported: None,
            pending: Vec::new(),
            flusher: None,
        }),
        wake: Rc::new(Notify::new()),
        me: me.clone(),
    })))
}

impl LinkHooks for RoomLinkHooks {
    fn self_ref(&self) -> Option<PeerRef> {
        self.0.upgrade()?.st.borrow().me.clone()
    }

    fn on_peer(&self, member: &str, up: bool) {
        let Some(room) = self.0.upgrade() else { return };
        {
            let mut st = room.st.borrow_mut();
            if st.me.is_none() {
                return;
            }
            if up {
                st.members.insert(member.to_string(), BTreeSet::new());
            } else if st.members.remove(member).is_none() {
                return;
            }
        }
        if up {
            // It learns which tabs this client speaks for: itself.
            room.announce_tabs(member);
        }
        room.apply();
    }

    fn on_message(&self, from: &str, data: &Json) {
        if let (Some(room), Some(data)) = (self.0.upgrade(), data.as_object()) {
            room.heard(from, data);
        }
    }

    fn on_status(&self, status: PeerLinkStatus) {
        let Some(room) = self.0.upgrade() else { return };
        room.st.borrow_mut().status = status;
        room.apply();
    }
}

impl Room {
    /// Tell the manager which tabs of the other members came and went, and
    /// how the link is doing.
    fn apply(&self) {
        let (me, down, up, status) = {
            let mut st = self.st.borrow_mut();
            let Some(me) = st.me.clone() else { return };
            let now: BTreeSet<String> = st
                .members
                .values()
                .flatten()
                .filter(|tab| **tab != me.peer)
                .cloned()
                .collect();
            let down: Vec<String> = st.up.difference(&now).cloned().collect();
            let up: Vec<String> = now.difference(&st.up).cloned().collect();
            st.up = now;
            let status = (st.reported != Some(st.status)).then_some(st.status);
            st.reported = Some(st.status);
            (me, down, up, status)
        };
        for tab in down {
            self.hooks
                .on_peer(&PeerRef::new(me.user.clone(), tab), false);
        }
        for tab in up {
            self.hooks
                .on_peer(&PeerRef::new(me.user.clone(), tab), true);
        }
        if let Some(status) = status {
            self.hooks.on_status(&me.user, status);
        }
    }

    fn announce_tabs(&self, member: &str) {
        let st = self.st.borrow();
        if let (Some(me), Some(link)) = (&st.me, &st.link) {
            link.send(
                Some(&[member.to_string()]),
                &json!({"h": "tabs", "tabs": [me.peer]}),
            );
        }
    }

    /// What a member of the room said: the tabs it speaks for, or a message
    /// of one of them.
    fn heard(&self, member: &str, data: &JsonMap<String, Json>) {
        let (me, tabs) = {
            let st = self.st.borrow();
            match (&st.me, st.members.get(member)) {
                (Some(me), Some(tabs)) => (me.clone(), tabs.clone()),
                _ => return,
            }
        };
        match data.get("h").and_then(Json::as_str) {
            Some("tabs") => {
                let Some(list) = tab_list(data.get("tabs")) else {
                    return;
                };
                self.st
                    .borrow_mut()
                    .members
                    .insert(member.to_string(), list.into_iter().collect());
                self.apply();
            }
            Some("m") => {
                let to = match data.get("to") {
                    Some(Json::Null) => None,
                    other => match tab_list(other) {
                        Some(list) => Some(list),
                        None => return,
                    },
                };
                // A member speaks for its own tabs only.
                let from = data.get("from").and_then(Json::as_str);
                let Some(from) = from.filter(|from| tabs.contains(*from)) else {
                    return;
                };
                if to.is_none_or(|to| to.contains(&me.peer)) {
                    self.hooks.on_message(
                        &PeerRef::new(me.user, from),
                        data.get("m").unwrap_or(&Json::Null),
                    );
                }
            }
            _ => {}
        }
    }

    /// Send what this turn queued: each message once, to the members that
    /// speak for at least one of the tabs it is for.
    fn flush(&self) {
        let (me, batch) = {
            let mut st = self.st.borrow_mut();
            let batch = std::mem::take(&mut st.pending);
            match st.me.clone() {
                Some(me) => (me, batch),
                None => return,
            }
        };
        let st = self.st.borrow();
        let Some(link) = &st.link else { return };
        for (m, to) in batch {
            let dest: Vec<String> = st
                .members
                .iter()
                .filter(|(_, tabs)| to.iter().any(|tab| tabs.contains(tab)))
                .map(|(member, _)| member.clone())
                .collect();
            if dest.is_empty() {
                continue;
            }
            let everyone = dest.len() == st.members.len();
            link.send(
                (!everyone).then_some(dest.as_slice()),
                &json!({"h": "m", "from": me.peer, "to": to, "m": m}),
            );
        }
    }

    fn start(&self, me: PeerRef) {
        let valid = valid_peer_id(&me.peer);
        let (weak, wake) = (self.me.clone(), self.wake.clone());
        let flusher = tokio::task::spawn_local(async move {
            loop {
                wake.notified().await;
                let Some(room) = weak.upgrade() else { return };
                room.flush();
            }
        });
        {
            let mut st = self.st.borrow_mut();
            st.me = Some(me.clone());
            st.members.clear();
            st.reported = None;
            st.status = if valid {
                PeerLinkStatus::Connecting
            } else {
                PeerLinkStatus::Failed
            };
            st.flusher = Some(flusher);
        }
        if valid {
            let link = Link::open(Rc::new(RoomLinkHooks(self.me.clone())), self.opts.clone());
            self.st.borrow_mut().link = Some(link);
        } else {
            let message = format!(
                "peer id '{}' cannot join a room: the relay takes 1 to 64 of [A-Za-z0-9_-]",
                me.peer
            );
            self.opts.log(LogLevel::Warn, &message);
        }
        self.apply();
    }

    fn stop(&self) {
        let (me, link, flusher, up) = {
            let mut st = self.st.borrow_mut();
            let Some(me) = st.me.take() else { return };
            st.members.clear();
            st.pending.clear();
            (
                me,
                st.link.take(),
                st.flusher.take(),
                std::mem::take(&mut st.up),
            )
        };
        if let Some(link) = link {
            link.close();
        }
        if let Some(flusher) = flusher {
            flusher.abort();
        }
        for tab in up {
            self.hooks
                .on_peer(&PeerRef::new(me.user.clone(), tab), false);
        }
    }
}

impl Drop for Room {
    fn drop(&mut self) {
        // The flusher waits on a wake that would never come again.
        if let Some(flusher) = self.st.get_mut().flusher.take() {
            flusher.abort();
        }
    }
}

impl ChannelTransport for RoomTransport {
    fn supported(&self) -> bool {
        true
    }

    fn want(&self, users: &BTreeSet<String>) {
        let room = &self.0;
        let me = room.hooks.self_ref();
        let wanted = me.as_ref().is_some_and(|me| users.contains(&me.user));
        let current = room.st.borrow().me.clone();
        if current.is_some() && (!wanted || current != me) {
            room.stop();
        }
        if let Some(me) = me.filter(|_| wanted && room.st.borrow().me.is_none()) {
            room.start(me);
        }
    }

    fn send(&self, to: &PeerRef, message: &ChannelWire) {
        let room = &self.0;
        {
            let mut st = room.st.borrow_mut();
            if st.me.as_ref().is_none_or(|me| me.user != to.user) {
                return;
            }
            // The manager sends one message to each peer in turn.
            let message = message.to_json();
            match st.pending.last_mut() {
                Some((last, tabs)) if *last == message => tabs.push(to.peer.clone()),
                _ => st.pending.push((message, vec![to.peer.clone()])),
            }
        }
        room.wake.notify_one();
    }

    fn close(&self) {
        self.0.stop();
    }
}

#[cfg(test)]
mod tests;
