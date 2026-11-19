//! sp00ky relay: a memory-only WebSocket relay for the devices of one user.
//!
//! Every connection says who it is with a session token, the [`Verifier`]
//! turns that token into a user id, and the user id IS the room. Members of a
//! room send each other small JSON messages (optimistic updates) that the
//! relay forwards without reading them. Nothing is stored and nothing is
//! retried: a lost message is harmless, the normal sync path still delivers
//! the change.
//!
//! The wire protocol is documented in the crate README, which is the contract
//! the clients are written against.
//!
//! How a connection is built:
//!
//! - one task reads the socket and runs the protocol ([`session`]);
//! - one task owns the write half and drains a bounded queue ([`write_loop`]).
//!
//! Everything a connection is sent goes through that queue with `try_send`, so
//! forwarding never waits for a receiver. A member whose queue is full is told
//! to go through a one-shot [`Kick`], and it then cleans up after itself, so
//! each room entry has exactly one owner that removes it ([`Membership`]).

pub mod auth;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// The protocol version a hello must carry.
pub const PROTOCOL_VERSION: u64 = 1;

/// How long a closing connection gets to take its last frame and answer the
/// close before the socket is dropped anyway.
const CLOSE_GRACE: Duration = Duration::from_secs(2);

/// Limits and timers. The defaults are the production values; tests shorten
/// the timers instead of sleeping through them.
#[derive(Clone, Debug)]
pub struct Config {
    /// Largest frame (and message) a client may send. A larger one closes the
    /// connection.
    pub max_frame_bytes: usize,
    /// Members per room. A hello into a full room is refused, a hello that
    /// replaces an existing peer id is not an extra member.
    pub max_room_peers: usize,
    /// Token bucket per connection: frames it may send in one burst.
    pub rate_burst: u32,
    /// Token bucket per connection: frames per second it refills.
    pub rate_per_sec: u32,
    /// Frames dropped in a row by the rate limit before the connection is
    /// closed with `rate_limited`.
    pub rate_drop_limit: u32,
    /// Frames waiting to be written to one connection. A member whose queue
    /// is full is disconnected.
    pub queue_frames: usize,
    /// How long a client has to send its hello after the upgrade.
    pub hello_timeout: Duration,
    /// How often the server sends a WebSocket Ping.
    pub ping_interval: Duration,
    /// A connection the server has heard nothing from for this long is closed.
    pub idle_timeout: Duration,
    /// Longest session. After it the client is told to `reauth` and reconnects
    /// with a fresh token, so a revoked session cannot stay in a room forever.
    pub session_max: Duration,
    /// Origins allowed to upgrade. Empty means any.
    pub allowed_origins: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_frame_bytes: 65_536,
            max_room_peers: 16,
            rate_burst: 400,
            rate_per_sec: 200,
            rate_drop_limit: 1000,
            queue_frames: 256,
            hello_timeout: Duration::from_secs(10),
            ping_interval: Duration::from_secs(25),
            idle_timeout: Duration::from_secs(60),
            session_max: Duration::from_secs(3600),
            allowed_origins: Vec::new(),
        }
    }
}

/// Why a token could not be turned into a user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerifyError {
    /// The token is invalid, expired, or belongs to no user.
    Unauthorized,
    /// The answer could not be had (database down, timeout). The client may
    /// retry with the same token. The text is for the relay's log only.
    Unavailable(String),
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized => f.write_str("the token belongs to no user"),
            Self::Unavailable(why) => write!(f, "the token could not be checked: {why}"),
        }
    }
}

impl std::error::Error for VerifyError {}

/// Turns a session token into the user it belongs to. A trait so the tests
/// run against a fake and the relay itself never links a database client.
#[async_trait::async_trait]
pub trait Verifier: Send + Sync + 'static {
    /// The user a session token belongs to, e.g. `"user:abc"`.
    ///
    /// An implementation bounds its own time: the relay waits for the answer.
    async fn verify(&self, token: &str) -> Result<String, VerifyError>;
}

/// The relay: its rooms and the router that serves them.
#[derive(Clone)]
pub struct Relay {
    shared: Arc<Shared>,
}

impl Relay {
    pub fn new(config: Config, verifier: Arc<dyn Verifier>) -> Self {
        Self {
            shared: Arc::new(Shared {
                config,
                verifier,
                rooms: Mutex::new(HashMap::new()),
                next_conn: AtomicU64::new(1),
            }),
        }
    }

    /// `GET /room` (the WebSocket) and `GET /health`.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/room", get(upgrade))
            .route("/health", get(health))
            .with_state(Arc::clone(&self.shared))
    }

    /// Rooms held in memory right now. A room exists only while it has a
    /// member.
    pub fn room_count(&self) -> usize {
        lock(&self.shared.rooms).len()
    }

    /// Members over all rooms.
    pub fn peer_count(&self) -> usize {
        lock(&self.shared.rooms)
            .values()
            .map(|room| lock(&room.members).len())
            .sum()
    }
}

/// The router of a fresh relay.
pub fn app(config: Config, verifier: Arc<dyn Verifier>) -> Router {
    Relay::new(config, verifier).router()
}

struct Shared {
    config: Config,
    verifier: Arc<dyn Verifier>,
    /// User id to room. Lock order is always this map first, then a room's
    /// members, and neither lock is ever held across an await.
    rooms: Mutex<HashMap<String, Arc<Room>>>,
    /// Tells two connections with the same peer id apart.
    next_conn: AtomicU64,
}

#[derive(Default)]
struct Room {
    /// Peer id to member. Forwarding only takes this lock, so the devices of
    /// one user never wait on another user's traffic.
    members: Mutex<HashMap<String, Member>>,
}

struct Member {
    conn: u64,
    tx: mpsc::Sender<Out>,
    /// Taken on the first kick, so a connection is told to go only once.
    kick: Option<oneshot::Sender<Kick>>,
}

impl Member {
    /// Hand this member a frame without ever waiting for it.
    fn deliver(&mut self, out: Out) {
        // A full queue is a stuck or too slow device, a closed one a writer
        // that already died. Either way the connection is told to go and
        // cleans up itself: growing memory or blocking the others is worse
        // than one device reconnecting.
        if self.tx.try_send(out).is_err() {
            self.kick(Kick::Overflow);
        }
    }

    fn kick(&mut self, why: Kick) {
        if let Some(kick) = self.kick.take() {
            let _ = kick.send(why);
        }
    }
}

impl Room {
    /// Forward one `msg` frame from `from` to `to`, or to every other member
    /// when `to` is `None`.
    fn forward(&self, conn: u64, from: &str, to: Option<Vec<String>>, frame: Arc<String>) {
        // Sorted and deduplicated before the lock, so a peer named twice gets
        // the frame once and a long list costs nothing while the lock is held.
        let to = to.map(|mut list| {
            list.sort_unstable();
            list.dedup();
            list
        });
        let mut members = lock(&self.members);
        // A replaced connection still holds the room for a moment, but its
        // peer id now belongs to its successor: it may not send any more.
        if members.get(from).is_none_or(|member| member.conn != conn) {
            return;
        }
        match to {
            None => {
                for (peer, member) in members.iter_mut() {
                    if peer != from {
                        member.deliver(Out::Text(Arc::clone(&frame)));
                    }
                }
            }
            Some(list) => {
                for peer in list.iter().filter(|peer| *peer != from) {
                    if let Some(member) = members.get_mut(peer) {
                        member.deliver(Out::Text(Arc::clone(&frame)));
                    }
                }
            }
        }
    }
}

/// One connection's seat in a room. Dropping it frees the seat, so the entry
/// goes away however the connection ends, a cancelled task included.
struct Membership {
    shared: Arc<Shared>,
    room: Arc<Room>,
    user: String,
    peer: String,
    conn: u64,
}

impl Drop for Membership {
    fn drop(&mut self) {
        let mut rooms = lock(&self.shared.rooms);
        let mut members = lock(&self.room.members);
        // A replaced connection no longer owns its peer id: the entry is its
        // successor's, and nobody is told that anything happened.
        if members
            .get(&self.peer)
            .is_some_and(|member| member.conn == self.conn)
        {
            members.remove(&self.peer);
            let leave = text(&ServerFrame::Leave { peer: &self.peer });
            for member in members.values_mut() {
                member.deliver(Out::Text(Arc::clone(&leave)));
            }
        }
        // The map may already hold a newer room for this user (this one
        // emptied and was removed while a replaced connection still held it),
        // and that one is not ours to remove.
        if members.is_empty()
            && rooms
                .get(&self.user)
                .is_some_and(|room| Arc::ptr_eq(room, &self.room))
        {
            rooms.remove(&self.user);
        }
    }
}

/// Take a seat in the user's room. `None` when the room is full.
fn join(
    shared: &Arc<Shared>,
    user: &str,
    peer: &str,
    tx: &mpsc::Sender<Out>,
) -> Option<(Membership, oneshot::Receiver<Kick>)> {
    let conn = shared.next_conn.fetch_add(1, Ordering::Relaxed);
    let (kick, kicked) = oneshot::channel();

    let mut rooms = lock(&shared.rooms);
    let room = Arc::clone(rooms.entry(user.to_owned()).or_default());
    let mut members = lock(&room.members);

    if !members.contains_key(peer) && members.len() >= shared.config.max_room_peers {
        // Only a limit of zero gets here with a room this call just created.
        if members.is_empty() {
            rooms.remove(user);
        }
        return None;
    }

    let mut peers: Vec<&str> = members
        .keys()
        .map(String::as_str)
        .filter(|other| *other != peer)
        .collect();
    peers.sort_unstable();
    // The welcome goes into the still empty queue before the member is
    // visible to anyone, so it is always the first frame the client gets.
    let _ = tx.try_send(Out::Text(text(&ServerFrame::Welcome { user, peer, peers })));

    let member = Member {
        conn,
        tx: tx.clone(),
        kick: Some(kick),
    };
    match members.insert(peer.to_owned(), member) {
        // Same peer id again (a reload, a reconnect that raced its old
        // socket): the new connection takes the seat and the others see
        // neither a leave nor a join.
        Some(mut old) => old.kick(Kick::Replaced),
        None => {
            let joined = text(&ServerFrame::Join { peer });
            for (other, member) in members.iter_mut() {
                if other != peer {
                    member.deliver(Out::Text(Arc::clone(&joined)));
                }
            }
        }
    }
    drop(members);
    drop(rooms);

    let membership = Membership {
        shared: Arc::clone(shared),
        room,
        user: user.to_owned(),
        peer: peer.to_owned(),
        conn,
    };
    Some((membership, kicked))
}

/// What the writer task is asked to put on the socket.
enum Out {
    /// A JSON text frame. Shared, because one forwarded frame goes to several
    /// queues.
    Text(Arc<String>),
    /// The answer to a client `{"t":"ping"}`.
    Pong,
    /// A WebSocket Ping.
    Ping,
}

/// Why another task told a connection to go.
enum Kick {
    /// A newer connection said hello with the same peer id.
    Replaced,
    /// The outbound queue filled up.
    Overflow,
}

/// How a session ended, which decides what the client is still told.
enum End {
    /// The client closed, or the socket failed.
    Gone,
    /// Nothing was heard for the idle timeout.
    Idle,
    /// The outbound queue filled up. The socket is dropped at once: the
    /// writer is most likely stuck on it.
    Overflow,
    /// An error frame, then close.
    Error(Code, &'static str),
}

/// The `code` of an error frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Code {
    BadRequest,
    Unauthorized,
    Unavailable,
    RoomFull,
    Replaced,
    RateLimited,
    Reauth,
}

/// The frames the relay writes itself. A forwarded `msg` is not here: it is
/// put together by hand around the untouched payload.
#[derive(Serialize)]
#[serde(tag = "t", rename_all = "lowercase")]
enum ServerFrame<'a> {
    Welcome {
        user: &'a str,
        peer: &'a str,
        peers: Vec<&'a str>,
    },
    Join {
        peer: &'a str,
    },
    Leave {
        peer: &'a str,
    },
    Error {
        code: Code,
        message: &'a str,
    },
}

const PONG: &str = r#"{"t":"pong"}"#;

fn text(frame: &ServerFrame<'_>) -> Arc<String> {
    // These shapes always serialize. Should that ever stop being true, the
    // client gets a frame it will treat as a protocol error and reconnect,
    // which beats a panic in the relay.
    Arc::new(serde_json::to_string(frame).unwrap_or_else(|_| "{}".to_owned()))
}

/// A frame from a client. One flat struct for all types, because a raw value
/// cannot be borrowed through serde's tagged enums.
#[derive(Deserialize)]
struct ClientFrame<'a> {
    #[serde(borrow)]
    t: Cow<'a, str>,
    #[serde(default)]
    v: Option<u64>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    peer: Option<String>,
    #[serde(default)]
    to: Option<Vec<String>>,
    #[serde(borrow, default, deserialize_with = "present")]
    d: Option<&'a RawValue>,
}

/// Keeps an explicit `"d":null` apart from a missing `d`: a plain `Option`
/// would read both as `None`, and a null payload is still a payload.
fn present<'de: 'a, 'a, D>(deserializer: D) -> Result<Option<&'a RawValue>, D::Error>
where
    D: Deserializer<'de>,
{
    <&RawValue>::deserialize(deserializer).map(Some)
}

/// 1 to 64 characters of `[A-Za-z0-9_-]`. The narrow alphabet is what lets a
/// peer id go into a frame without escaping.
fn valid_peer(peer: &str) -> bool {
    (1..=64).contains(&peer.len())
        && peer
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// The token and peer id of a hello, or what is wrong with it.
fn parse_hello(frame: &str) -> Result<(String, String), &'static str> {
    let Ok(frame) = serde_json::from_str::<ClientFrame<'_>>(frame) else {
        return Err("the first frame must be a hello");
    };
    if frame.t != "hello" {
        return Err("the first frame must be a hello");
    }
    if frame.v != Some(PROTOCOL_VERSION) {
        return Err("unsupported protocol version, this relay speaks v 1");
    }
    let Some(token) = frame.token else {
        return Err("hello has no token");
    };
    match frame.peer {
        Some(peer) if valid_peer(&peer) => Ok((token, peer)),
        _ => Err("peer must be 1 to 64 characters of [A-Za-z0-9_-]"),
    }
}

/// Token bucket: `burst` frames at once, refilled at `per_sec`.
struct Bucket {
    tokens: f64,
    burst: f64,
    per_sec: f64,
    at: Instant,
}

impl Bucket {
    fn new(burst: u32, per_sec: u32, now: Instant) -> Self {
        let burst = f64::from(burst);
        Self {
            tokens: burst,
            burst,
            per_sec: f64::from(per_sec),
            at: now,
        }
    }

    /// Whether a frame arriving at `now` is within the limit.
    fn take(&mut self, now: Instant) -> bool {
        let waited = now.saturating_duration_since(self.at).as_secs_f64();
        self.at = now;
        self.tokens = (self.tokens + waited * self.per_sec).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// `from + wait`. A wait too long to add (a "never" from a huge configured
/// duration) becomes thirty years, which is what tokio itself does.
fn after(from: Instant, wait: Duration) -> Instant {
    const NEVER: Duration = Duration::from_secs(86_400 * 365 * 30);
    from.checked_add(wait.min(NEVER)).unwrap_or(from)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // The maps stay consistent between statements, so a panic elsewhere while
    // a lock was held must not take every later connection down with it.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn health() -> &'static str {
    "ok"
}

fn origin_allowed(allowed: &[String], origin: Option<&HeaderValue>) -> bool {
    if allowed.is_empty() {
        return true;
    }
    // A browser puts an Origin on every WebSocket upgrade. A request without
    // one comes from a native client, which could forge the header anyway, so
    // refusing it would lock out the native apps and stop nobody else.
    let Some(origin) = origin else {
        return true;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    let origin = origin.trim_end_matches('/');
    allowed.iter().any(|entry| {
        entry
            .trim()
            .trim_end_matches('/')
            .eq_ignore_ascii_case(origin)
    })
}

async fn upgrade(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !origin_allowed(&shared.config.allowed_origins, headers.get(header::ORIGIN)) {
        return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
    }
    // Both limits: a message may be split over frames, and without the second
    // one a client could still build a huge message out of small frames.
    let limit = shared.config.max_frame_bytes;
    ws.max_frame_size(limit)
        .max_message_size(limit)
        .on_upgrade(move |socket| serve(shared, socket))
}

/// One connection, from the upgrade to the dropped socket.
async fn serve(shared: Arc<Shared>, socket: WebSocket) {
    let (sink, mut stream) = socket.split();
    let (tx, rx) = mpsc::channel(shared.config.queue_frames.max(1));
    let (last, last_words) = oneshot::channel();
    let mut writer = tokio::spawn(write_loop(sink, rx, last_words));

    let words = match session(&shared, &mut stream, &tx).await {
        End::Overflow => {
            // The writer is most likely blocked on a socket nobody reads.
            // Asking it to say goodbye would only keep the memory around.
            writer.abort();
            return;
        }
        End::Gone => {
            // The client closed first: answer its close and be done.
            let _ = last.send(None);
            let _ = tokio::time::timeout(CLOSE_GRACE, &mut writer).await;
            writer.abort();
            return;
        }
        End::Idle => None,
        End::Error(code, message) => Some(text(&ServerFrame::Error { code, message })),
    };

    // The relay closes. Keep reading until the client answers the close:
    // dropping a socket that still has unread input resets the connection,
    // and a reset can throw away the error frame before the client reads it.
    let _ = last.send(words);
    let drain = async {
        while let Some(Ok(message)) = stream.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    };
    let _ = tokio::time::timeout(CLOSE_GRACE, async { tokio::join!(&mut writer, drain) }).await;
    writer.abort();
}

/// The only place that writes to a socket. It may block on a slow client for
/// as long as it likes: nothing else ever waits for it.
async fn write_loop(
    mut sink: SplitSink<WebSocket, Message>,
    mut queue: mpsc::Receiver<Out>,
    mut last_words: oneshot::Receiver<Option<Arc<String>>>,
) {
    loop {
        tokio::select! {
            // Checked first, so a closing connection does not work through a
            // full queue before it hears why it is being closed.
            biased;
            words = &mut last_words => {
                if let Ok(Some(words)) = words {
                    let _ = sink.send(Message::Text(owned(words))).await;
                }
                // Sends the Close frame, or flushes the answer to the
                // client's own.
                let _ = sink.close().await;
                return;
            }
            out = queue.recv() => {
                let message = match out {
                    Some(Out::Text(frame)) => Message::Text(owned(frame)),
                    Some(Out::Pong) => Message::Text(PONG.to_owned()),
                    Some(Out::Ping) => Message::Ping(Vec::new()),
                    None => return,
                };
                if sink.send(message).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// The last queue to write a shared frame takes the string itself, so a room
/// of two devices never copies a payload here.
fn owned(frame: Arc<String>) -> String {
    Arc::try_unwrap(frame).unwrap_or_else(|shared| String::clone(&shared))
}

/// The first text frame, which has to be the hello.
async fn first_text(stream: &mut SplitStream<WebSocket>) -> Result<String, End> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(frame))) => return Ok(frame),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(Message::Binary(_))) => {
                return Err(End::Error(
                    Code::BadRequest,
                    "binary frames are not part of the protocol",
                ));
            }
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return Err(End::Gone),
        }
    }
}

/// The protocol for one connection: hello, room, frames, until it ends.
async fn session(
    shared: &Arc<Shared>,
    stream: &mut SplitStream<WebSocket>,
    tx: &mpsc::Sender<Out>,
) -> End {
    let config = &shared.config;

    let hello = match tokio::time::timeout(config.hello_timeout, first_text(stream)).await {
        Ok(Ok(frame)) => frame,
        Ok(Err(end)) => return end,
        Err(_) => return End::Error(Code::BadRequest, "no hello in time"),
    };
    let (token, peer) = match parse_hello(&hello) {
        Ok(hello) => hello,
        Err(message) => return End::Error(Code::BadRequest, message),
    };
    drop(hello);

    let user = if token.is_empty() {
        // Not worth a round trip to the database.
        Err(VerifyError::Unauthorized)
    } else {
        shared.verifier.verify(&token).await
    };
    drop(token);
    let user = match user {
        Ok(user) if !user.is_empty() => user,
        Ok(_) | Err(VerifyError::Unauthorized) => {
            return End::Error(Code::Unauthorized, "the token belongs to no user");
        }
        Err(VerifyError::Unavailable(why)) => {
            tracing::warn!(%why, "could not verify a token");
            return End::Error(
                Code::Unavailable,
                "the session could not be checked, try again",
            );
        }
    };

    let Some((membership, mut kicked)) = join(shared, &user, &peer, tx) else {
        return End::Error(Code::RoomFull, "the room is full");
    };
    tracing::debug!(%user, %peer, "joined");

    let start = Instant::now();
    let session_over = tokio::time::sleep_until(after(start, config.session_max));
    let idle = tokio::time::sleep_until(after(start, config.idle_timeout));
    let ping = tokio::time::sleep_until(after(start, config.ping_interval));
    tokio::pin!(session_over, idle, ping);
    let mut bucket = Bucket::new(config.rate_burst, config.rate_per_sec, start);
    let mut heard = start;
    let mut dropped: u32 = 0;

    let end = loop {
        tokio::select! {
            kick = &mut kicked => break match kick {
                Ok(Kick::Replaced) => {
                    End::Error(Code::Replaced, "another connection took this peer id")
                }
                Ok(Kick::Overflow) => End::Overflow,
                Err(_) => End::Gone,
            },
            _ = &mut session_over => {
                break End::Error(Code::Reauth, "session too old, reconnect with a fresh token");
            }
            _ = &mut idle => {
                // The timer is not moved on every frame: when it fires it
                // looks at when the client was last heard and sleeps on.
                let due = after(heard, config.idle_timeout);
                if Instant::now() >= due {
                    break End::Idle;
                }
                idle.as_mut().reset(due);
            }
            _ = &mut ping => {
                if tx.try_send(Out::Ping).is_err() {
                    break End::Overflow;
                }
                // Never a busy loop, whatever the configured interval.
                let wait = config.ping_interval.max(Duration::from_millis(1));
                ping.as_mut().reset(after(Instant::now(), wait));
            }
            // The writer ended on its own: the socket failed under it.
            _ = tx.closed() => break End::Gone,
            frame = stream.next() => {
                let Some(Ok(message)) = frame else {
                    break End::Gone;
                };
                let now = Instant::now();
                heard = now;
                if matches!(message, Message::Close(_)) {
                    break End::Gone;
                }
                if !bucket.take(now) {
                    dropped = dropped.saturating_add(1);
                    if dropped >= config.rate_drop_limit {
                        break End::Error(Code::RateLimited, "too many frames");
                    }
                    continue;
                }
                dropped = 0;
                match message {
                    Message::Text(frame) => {
                        if let Err(end) = on_frame(&membership, tx, &frame) {
                            break end;
                        }
                    }
                    Message::Binary(_) => {
                        break End::Error(
                            Code::BadRequest,
                            "binary frames are not part of the protocol",
                        );
                    }
                    // Pings are answered by the WebSocket layer, and a pong
                    // only had to count as heard.
                    _ => {}
                }
            }
        }
    };

    // Frees the seat and tells the others before the goodbye, so a slow close
    // never keeps a dead member in the room.
    drop(membership);
    tracing::debug!(%user, %peer, "left");
    end
}

/// One text frame from a member.
fn on_frame(member: &Membership, tx: &mpsc::Sender<Out>, frame: &str) -> Result<(), End> {
    let Ok(frame) = serde_json::from_str::<ClientFrame<'_>>(frame) else {
        return Err(End::Error(Code::BadRequest, "not a protocol frame"));
    };
    match frame.t.as_ref() {
        "msg" => {
            let Some(payload) = frame.d else {
                return Err(End::Error(Code::BadRequest, "a msg needs a d"));
            };
            // Put together by hand around the payload exactly as it came in.
            // The peer id needs no escaping, see `valid_peer`.
            let payload = payload.get();
            let mut out = String::with_capacity(payload.len() + member.peer.len() + 26);
            out.push_str(r#"{"t":"msg","from":""#);
            out.push_str(&member.peer);
            out.push_str(r#"","d":"#);
            out.push_str(payload);
            out.push('}');
            member
                .room
                .forward(member.conn, &member.peer, frame.to, Arc::new(out));
            Ok(())
        }
        "ping" => tx.try_send(Out::Pong).map_err(|_| End::Overflow),
        "hello" => Err(End::Error(
            Code::BadRequest,
            "hello is only valid as the first frame",
        )),
        _ => Err(End::Error(Code::BadRequest, "unknown frame type")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_ids() {
        assert!(valid_peer("a"));
        assert!(valid_peer("Tab_1-x"));
        assert!(valid_peer(&"a".repeat(64)));
        assert!(!valid_peer(""));
        assert!(!valid_peer(&"a".repeat(65)));
        assert!(!valid_peer("a b"));
        assert!(!valid_peer("a\"b"));
        assert!(!valid_peer("pé"));
    }

    #[test]
    fn hello_is_checked() {
        let ok = r#"{"t":"hello","v":1,"token":"tok","peer":"p1"}"#;
        assert_eq!(parse_hello(ok), Ok(("tok".to_owned(), "p1".to_owned())));
        // Fields this version does not know are ignored, so a later client
        // can say more.
        let extra = r#"{"t":"hello","v":1,"token":"tok","peer":"p1","client":"web"}"#;
        assert!(parse_hello(extra).is_ok());

        for bad in [
            "",
            "nope",
            "[]",
            r#"{"v":1,"token":"tok","peer":"p1"}"#,
            r#"{"t":"msg","d":1}"#,
            r#"{"t":"hello","token":"tok","peer":"p1"}"#,
            r#"{"t":"hello","v":2,"token":"tok","peer":"p1"}"#,
            r#"{"t":"hello","v":"1","token":"tok","peer":"p1"}"#,
            r#"{"t":"hello","v":1,"peer":"p1"}"#,
            r#"{"t":"hello","v":1,"token":7,"peer":"p1"}"#,
            r#"{"t":"hello","v":1,"token":"tok"}"#,
            r#"{"t":"hello","v":1,"token":"tok","peer":""}"#,
            r#"{"t":"hello","v":1,"token":"tok","peer":"a/b"}"#,
        ] {
            assert!(parse_hello(bad).is_err(), "accepted: {bad}");
        }
    }

    #[test]
    fn payload_is_kept_as_written() {
        let frame: ClientFrame<'_> =
            serde_json::from_str(r#"{"t":"msg","d": {"a" : [1, 2.50],"s":"é"} ,"to":["x"]}"#)
                .unwrap();
        assert_eq!(
            frame.d.map(RawValue::get),
            Some(r#"{"a" : [1, 2.50],"s":"é"}"#)
        );
        assert_eq!(frame.to, Some(vec!["x".to_owned()]));

        let null: ClientFrame<'_> =
            serde_json::from_str(r#"{"t":"msg","d":null,"to":null}"#).unwrap();
        assert_eq!(null.d.map(RawValue::get), Some("null"));
        assert_eq!(null.to, None);

        let missing: ClientFrame<'_> = serde_json::from_str(r#"{"t":"msg"}"#).unwrap();
        assert!(missing.d.is_none());
    }

    #[test]
    fn server_frames() {
        let welcome = text(&ServerFrame::Welcome {
            user: "user:a\"b",
            peer: "p1",
            peers: vec!["p2"],
        });
        assert_eq!(
            *welcome,
            r#"{"t":"welcome","user":"user:a\"b","peer":"p1","peers":["p2"]}"#
        );
        assert_eq!(
            *text(&ServerFrame::Join { peer: "p1" }),
            r#"{"t":"join","peer":"p1"}"#
        );
        assert_eq!(
            *text(&ServerFrame::Leave { peer: "p1" }),
            r#"{"t":"leave","peer":"p1"}"#
        );
        assert_eq!(
            *text(&ServerFrame::Error {
                code: Code::RateLimited,
                message: "m"
            }),
            r#"{"t":"error","code":"rate_limited","message":"m"}"#
        );
        assert_eq!(
            *text(&ServerFrame::Error {
                code: Code::RoomFull,
                message: "m"
            }),
            r#"{"t":"error","code":"room_full","message":"m"}"#
        );
    }

    #[test]
    fn bucket_bursts_then_refills() {
        let start = Instant::now();
        let mut bucket = Bucket::new(3, 10, start);
        assert!(bucket.take(start));
        assert!(bucket.take(start));
        assert!(bucket.take(start));
        assert!(!bucket.take(start));
        // 10 a second: one token every 100 ms.
        assert!(!bucket.take(start + Duration::from_millis(50)));
        assert!(bucket.take(start + Duration::from_millis(160)));
        assert!(!bucket.take(start + Duration::from_millis(170)));
        // A long pause fills it up to the burst and no further.
        let later = start + Duration::from_secs(60);
        assert!(bucket.take(later));
        assert!(bucket.take(later));
        assert!(bucket.take(later));
        assert!(!bucket.take(later));
    }

    #[test]
    fn bucket_without_refill_never_recovers() {
        let start = Instant::now();
        let mut bucket = Bucket::new(1, 0, start);
        assert!(bucket.take(start));
        assert!(!bucket.take(start + Duration::from_secs(3600)));
    }

    #[test]
    fn origins() {
        let value = |origin: &'static str| HeaderValue::from_static(origin);
        let allowed = vec![
            "https://app.example.com".to_owned(),
            " http://localhost:5173/ ".to_owned(),
        ];

        assert!(origin_allowed(&[], Some(&value("https://evil.example"))));
        assert!(origin_allowed(&[], None));

        assert!(origin_allowed(
            &allowed,
            Some(&value("https://app.example.com"))
        ));
        assert!(origin_allowed(
            &allowed,
            Some(&value("HTTPS://APP.example.com"))
        ));
        assert!(origin_allowed(
            &allowed,
            Some(&value("http://localhost:5173"))
        ));
        assert!(!origin_allowed(
            &allowed,
            Some(&value("https://evil.example"))
        ));
        assert!(!origin_allowed(
            &allowed,
            Some(&value("https://app.example.com.evil.example"))
        ));
        assert!(!origin_allowed(&allowed, Some(&value("null"))));
        // No Origin at all is a native client, not a browser.
        assert!(origin_allowed(&allowed, None));
    }

    #[test]
    fn a_huge_wait_does_not_overflow() {
        let now = Instant::now();
        assert!(after(now, Duration::MAX) > now + Duration::from_secs(86_400 * 365));
        assert_eq!(
            after(now, Duration::from_secs(5)),
            now + Duration::from_secs(5)
        );
    }
}
