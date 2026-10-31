//! `GET /admin/api/v2/live`: the dashboard's one connection to this scheduler.
//!
//! The protocol itself (messages, the three shapes, diff and apply) lives in
//! [`ssp_protocol::admin_live`]; this is the host half. It owns the topic
//! catalog and keeps each topic's latest state in a [`Topic`], which one task
//! refreshes from the samplers the plane already runs and every subscriber
//! shares.
//!
//! # Why the cost does not grow with viewers
//!
//! The polling dashboard paid for every open tab: `/overview` rebuilt the
//! whole fleet three times a second per tab, hidden tabs included. Here a
//! topic is computed once per tick whoever watches, the patch is encoded once,
//! and each subscriber only copies bytes. A tick that changes nothing sends
//! nothing.
//!
//! # Resume
//!
//! A topic outlives its last subscriber by [`GRACE`] and keeps its last
//! [`RING`] patches, so a dashboard that reconnects (a network blip, a reload
//! with persisted collections) sends the seq it holds and receives only what
//! it missed. A gap the ring no longer covers costs one snapshot, never a
//! wrong state: every patch names the seq it applies on top of.
//!
//! # Backpressure
//!
//! Topics publish into a bounded broadcast channel and never wait for a
//! reader. A subscriber whose socket is slow falls behind, the channel tells
//! it so, and it sends one fresh snapshot instead of the patches it skipped.
//!
//! # Auth
//!
//! The upgrade request carries no credentials: the session token arrives as
//! the first message, so it never lands in a proxy's access log the way a
//! query string would. The same session store gates it as every other admin
//! request, the session is re-checked every [`RECHECK_EVERY`] (a logout closes
//! the socket), and the connection closes when the session expires unless
//! the client renews it in-band after [`ServerMsg::Expiring`].

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ssp_protocol::admin_live::{self as proto, ClientMsg, Delta, ServerMsg, SessionInfo, Shape};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::session::Session;
use super::{api_error, ApiError, AdminState};

/// Every topic this host serves.
pub const TOPICS: &[&str] = &[
    "overview",
    "presence",
    "metrics",
    "jobs",
    "runs",
    "incidents",
    "operations",
    "logs",
];

/// Patches a topic keeps for reconnecting readers.
const RING: usize = 256;
/// How long a topic keeps running after its last subscriber leaves, so a
/// reload resumes from patches rather than a snapshot.
const GRACE: Duration = Duration::from_secs(60);
const MAX_SUBS: usize = 32;
const MAX_CONNECTIONS: usize = 256;
const MAX_MESSAGE: usize = 64 * 1024;
const AUTH_DEADLINE: Duration = Duration::from_secs(5);
/// Frames a connection may have queued for its socket before its topics
/// start skipping ahead to snapshots.
const OUT_QUEUE: usize = 256;
/// Keeps idle connections alive through proxies that drop quiet sockets.
const PING_EVERY: Duration = Duration::from_secs(25);
const RECHECK_EVERY: Duration = Duration::from_secs(30);
const EXPIRY_WARNING: Duration = Duration::from_secs(60);
/// How long `/v2/snapshot` waits for a topic that has never produced a state.
const FIRST_STATE_WAIT: Duration = Duration::from_secs(3);
/// Items the `logs` topic holds.
const LOG_KEEP: usize = 500;
/// Incidents the `incidents` topic holds.
const INCIDENT_ROWS: usize = 200;

/* ------------------------------------------------------------------ */
/* The hub                                                              */
/* ------------------------------------------------------------------ */

pub struct LiveHub {
    /// Identifies this process: a `since` from another boot is never honoured.
    boot: String,
    /// One counter for every topic, so a seq never means two states.
    seq: AtomicU64,
    topics: Mutex<HashMap<String, Arc<Topic>>>,
    connections: AtomicUsize,
}

impl LiveHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            boot: uuid::Uuid::new_v4().simple().to_string()[..12].to_string(),
            seq: AtomicU64::new(0),
            topics: Mutex::new(HashMap::new()),
            connections: AtomicUsize::new(0),
        })
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Join the topic for `name` + `args`, starting it if it is not running.
    ///
    /// The subscriber count is taken under the registry lock, the same lock a
    /// topic retires under, so a topic can never retire between being handed
    /// out and being counted.
    fn subscribe(
        self: &Arc<Self>,
        state: &AdminState,
        name: &str,
        args: &Value,
    ) -> Result<(Arc<Topic>, Subscriber), String> {
        if !TOPICS.contains(&name) {
            return Err(format!("Unknown topic `{name}`"));
        }
        let key = proto::topic_key(name, args);
        let mut topics = self.topics.lock().expect("live topics poisoned");
        let topic = match topics.get(&key) {
            Some(topic) => Arc::clone(topic),
            None => {
                let opened = open(name, args, state)?;
                let topic = Arc::new(Topic::new(key.clone(), opened.shape, opened.key));
                topics.insert(key, Arc::clone(&topic));
                tokio::spawn(run(Arc::clone(self), Arc::clone(&topic), opened.producer, state.clone()));
                topic
            }
        };
        topic.subscribers.fetch_add(1, Ordering::SeqCst);
        Ok((Arc::clone(&topic), Subscriber(topic)))
    }

    /// Live topic instances, for tests and the debug surface.
    pub fn running(&self) -> usize {
        self.topics.lock().map(|t| t.len()).unwrap_or(0)
    }
}

/// Holds a topic open; dropping it starts the topic's grace period.
struct Subscriber(Arc<Topic>);

impl Drop for Subscriber {
    fn drop(&mut self) {
        if self.0.subscribers.fetch_sub(1, Ordering::SeqCst) == 1 {
            *self.0.idle_since.lock().expect("idle clock poisoned") = Instant::now();
        }
    }
}

/// One topic instance: a topic name with one set of arguments.
struct Topic {
    key: String,
    shape: Shape,
    key_field: &'static str,
    current: RwLock<Option<Current>>,
    ring: Mutex<VecDeque<Arc<Step>>>,
    tx: broadcast::Sender<Published>,
    subscribers: AtomicUsize,
    idle_since: Mutex<Instant>,
}

struct Current {
    seq: u64,
    data: Value,
    /// The encoded snapshot, made on first demand and shared after that.
    snap: Mutex<Option<Arc<str>>>,
}

/// One published patch, encoded once for every subscriber.
struct Step {
    from: u64,
    seq: u64,
    text: Arc<str>,
}

#[derive(Clone)]
enum Published {
    Step(Arc<Step>),
    /// The state changed in a way no patch describes (or it is the first
    /// one): read the current snapshot.
    Reset,
}

#[derive(Serialize)]
struct SnapRef<'a> {
    op: &'static str,
    key: &'a str,
    seq: u64,
    data: &'a Value,
}

impl Topic {
    fn new(key: String, shape: Shape, key_field: &'static str) -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            key,
            shape,
            key_field,
            current: RwLock::new(None),
            ring: Mutex::new(VecDeque::with_capacity(RING)),
            tx,
            subscribers: AtomicUsize::new(0),
            idle_since: Mutex::new(Instant::now()),
        }
    }

    /// Make `next` (canonical) the topic's state and tell subscribers how.
    fn publish(&self, hub: &LiveHub, next: Value) {
        let mut cur = self.current.write().expect("live topic poisoned");
        let published = match cur.as_ref() {
            None => {
                *cur = Some(Current { seq: hub.next_seq(), data: next, snap: Mutex::new(None) });
                Published::Reset
            }
            Some(prev) => match proto::diff(self.shape, self.key_field, &prev.data, &next) {
                Delta::Unchanged => return,
                Delta::Patch(patch) => {
                    let seq = hub.next_seq();
                    let text = encode(&ServerMsg::Diff { key: self.key.clone(), from: prev.seq, seq, patch });
                    let step = Arc::new(Step { from: prev.seq, seq, text });
                    let mut ring = self.ring.lock().expect("live ring poisoned");
                    if ring.len() == RING {
                        ring.pop_front();
                    }
                    ring.push_back(Arc::clone(&step));
                    drop(ring);
                    *cur = Some(Current { seq, data: next, snap: Mutex::new(None) });
                    Published::Step(step)
                }
                Delta::Reset => {
                    self.ring.lock().expect("live ring poisoned").clear();
                    *cur = Some(Current { seq: hub.next_seq(), data: next, snap: Mutex::new(None) });
                    Published::Reset
                }
            },
        };
        drop(cur);
        // No subscriber is normal during the grace period.
        let _ = self.tx.send(published);
    }

    /// The current state as an encoded `snap`, when there is one.
    fn snapshot(&self) -> Option<(u64, Arc<str>)> {
        let cur = self.current.read().expect("live topic poisoned");
        let c = cur.as_ref()?;
        let mut cached = c.snap.lock().expect("snap cache poisoned");
        let text = cached
            .get_or_insert_with(|| {
                let msg = SnapRef { op: "snap", key: &self.key, seq: c.seq, data: &c.data };
                serde_json::to_string(&msg).unwrap_or_default().into()
            })
            .clone();
        Some((c.seq, text))
    }

    /// The current seq and state, for the HTTP snapshot.
    fn state(&self) -> Option<(u64, Value)> {
        let cur = self.current.read().expect("live topic poisoned");
        cur.as_ref().map(|c| (c.seq, c.data.clone()))
    }

    /// The patches that bring a reader at `since` up to date, or `None` when
    /// the ring no longer reaches back that far.
    fn replay_from(&self, since: u64) -> Option<Vec<Arc<Step>>> {
        let cur = self.current.read().expect("live topic poisoned");
        let c = cur.as_ref()?;
        if c.seq == since {
            return Some(Vec::new());
        }
        let ring = self.ring.lock().expect("live ring poisoned");
        let start = ring.iter().position(|s| s.from == since)?;
        Some(ring.iter().skip(start).cloned().collect())
    }
}

fn encode(msg: &ServerMsg) -> Arc<str> {
    serde_json::to_string(msg).unwrap_or_default().into()
}

/// A topic's refresh loop: produce, publish, and retire once nobody has
/// watched for [`GRACE`].
async fn run(hub: Arc<LiveHub>, topic: Arc<Topic>, mut producer: Box<dyn Producer>, state: AdminState) {
    debug!(key = %topic.key, "Live topic started");
    let interval = producer.interval();
    loop {
        if let Some(next) = producer.next(&state).await {
            topic.publish(&hub, proto::canonical(next));
        }
        if topic.subscribers.load(Ordering::SeqCst) == 0 {
            let idle_for = topic.idle_since.lock().map(|t| t.elapsed()).unwrap_or(GRACE);
            if idle_for >= GRACE {
                let mut topics = hub.topics.lock().expect("live topics poisoned");
                // Re-checked under the lock `subscribe` counts under.
                if topic.subscribers.load(Ordering::SeqCst) == 0 {
                    topics.remove(&topic.key);
                    break;
                }
            }
        }
        tokio::time::sleep(interval).await;
    }
    debug!(key = %topic.key, "Live topic retired");
}

/* ------------------------------------------------------------------ */
/* Producers                                                            */
/* ------------------------------------------------------------------ */

/// Computes a topic's next full state from what the plane already holds.
#[async_trait]
trait Producer: Send {
    /// The next state, or `None` while there is none yet.
    async fn next(&mut self, state: &AdminState) -> Option<Value>;

    fn interval(&self) -> Duration {
        Duration::from_secs(1)
    }
}

struct Opened {
    shape: Shape,
    /// The window row / series item key; empty for a doc.
    key: &'static str,
    producer: Box<dyn Producer>,
}

fn no_args(args: &Value) -> bool {
    args.is_null() || args.as_object().is_some_and(|m| m.is_empty())
}

/// The topic catalog.
fn open(name: &str, args: &Value, state: &AdminState) -> Result<Opened, String> {
    let plain = |shape, key, producer: Box<dyn Producer>| {
        if no_args(args) {
            Ok(Opened { shape, key, producer })
        } else {
            Err(format!("Topic `{name}` takes no arguments"))
        }
    };
    match name {
        "overview" => plain(Shape::Doc, "", Box::new(OverviewTopic)),
        "presence" => plain(Shape::Doc, "", Box::new(PresenceTopic)),
        "metrics" => {
            let metric = args.get("name").and_then(Value::as_str).unwrap_or_default();
            let (key, metric) = match metric {
                "sync" => ("ts", Metric::Sync),
                "presence" => ("ts", Metric::Presence),
                "jobs" => ("t", Metric::Jobs),
                _ => return Err("Topic `metrics` takes {\"name\": \"sync\" | \"presence\" | \"jobs\"}".into()),
            };
            if args.as_object().map_or(0, |m| m.len()) != 1 {
                return Err("Topic `metrics` takes only `name`".into());
            }
            Ok(Opened { shape: Shape::Series, key, producer: Box::new(MetricsTopic(metric)) })
        }
        "jobs" => {
            if !state.jobs.enabled() {
                return Err("The job sampler is switched off (SPKY_ADMIN_JOB_INTERVAL_SECS=0)".into());
            }
            plain(Shape::Window, "id", Box::new(JobsTopic::new(state)))
        }
        "runs" => plain(Shape::Window, "id", Box::new(RunsTopic { sub: None, latest: None })),
        "incidents" => plain(Shape::Window, "id", Box::new(IncidentsTopic)),
        "operations" => plain(Shape::Window, "id", Box::new(OperationsTopic)),
        "logs" => plain(Shape::Series, "n", Box::new(LogsTopic::new(state))),
        _ => Err(format!("Unknown topic `{name}`")),
    }
}

/// Re-key an array of objects as an object keyed by `key`, so a doc patch
/// touches one entry instead of replacing the array.
fn keyed(items: Vec<Value>, key: &str) -> Value {
    Value::Object(
        items
            .into_iter()
            .filter_map(|item| {
                let k = item.get(key).and_then(Value::as_str)?.to_string();
                Some((k, item))
            })
            .collect(),
    )
}

fn remove_at(v: &mut Value, pointer: &str, member: &str) {
    if let Some(obj) = v.pointer_mut(pointer).and_then(Value::as_object_mut) {
        obj.remove(member);
    }
}

/// `/overview`, reshaped for patching: entity lists keyed by id, the sample
/// rings moved to `metrics`, and the clock readings (which change on every
/// tick and describe nothing) dropped.
struct OverviewTopic;

#[async_trait]
impl Producer for OverviewTopic {
    async fn next(&mut self, state: &AdminState) -> Option<Value> {
        let mut v = super::overview::overview_value(state).await;
        let obj = v.as_object_mut()?;
        obj.remove("server_time_ms");
        for list in ["ssps", "backends", "operations"] {
            if let Some(Value::Array(items)) = obj.remove(list) {
                obj.insert(list.to_string(), keyed(items, "id"));
            }
        }
        remove_at(&mut v, "/presence", "samples");
        remove_at(&mut v, "/jobs", "samples");
        remove_at(&mut v, "/scheduler/heartbeat", "samples");
        remove_at(&mut v, "/incidents", "server_time_ms");
        Some(v)
    }
}

/// `/presence` minus its sample ring (that is `metrics?name=presence`).
struct PresenceTopic;

#[async_trait]
impl Producer for PresenceTopic {
    async fn next(&mut self, state: &AdminState) -> Option<Value> {
        let mut v = state.presence.presence_json();
        if let Some(obj) = v.as_object_mut() {
            obj.remove("samples");
        }
        Some(v)
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(2)
    }
}

enum Metric {
    /// The end-to-end sync heartbeat (`{ts, ms, ok}`).
    Sync,
    /// Users, sessions and views (`{ts, users, sessions, views}`).
    Presence,
    /// Queue depth and throughput (`{t, pending, processing, failed, throughput_1m}`).
    Jobs,
}

struct MetricsTopic(Metric);

#[async_trait]
impl Producer for MetricsTopic {
    async fn next(&mut self, state: &AdminState) -> Option<Value> {
        let items: Vec<Value> = match self.0 {
            Metric::Sync => serde_json::to_value(state.metrics.heartbeat.samples()).ok()?,
            Metric::Presence => serde_json::to_value(state.presence.samples()).ok()?,
            Metric::Jobs => state.jobs.samples(),
        }
        .as_array()
        .cloned()
        .unwrap_or_default();
        // The rings' own capacity is the window: a reader keeps as many.
        let keep = items.len().max(120);
        Some(proto::series(items, keep, json!({})))
    }
}

/// The unfiltered job listing. Holding the sampler's watcher guard is what
/// puts it on its live cadence while anyone subscribes.
struct JobsTopic {
    rx: broadcast::Receiver<Value>,
    _guard: super::jobs::WatcherGuard,
    latest: Option<Value>,
}

impl JobsTopic {
    fn new(state: &AdminState) -> Self {
        let (rx, guard) = state.jobs.subscribe();
        Self { rx, _guard: guard, latest: state.jobs.live_payload() }
    }
}

/// Drain a broadcast receiver down to its newest value.
fn newest(rx: &mut broadcast::Receiver<Value>, latest: &mut Option<Value>) {
    loop {
        match rx.try_recv() {
            Ok(v) => *latest = Some(v),
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
}

#[async_trait]
impl Producer for JobsTopic {
    async fn next(&mut self, _state: &AdminState) -> Option<Value> {
        newest(&mut self.rx, &mut self.latest);
        let mut payload = self.latest.clone()?;
        let rows = payload.get_mut("jobs").map(Value::take)?.as_array().cloned().unwrap_or_default();
        let mut meta = payload;
        if let Some(obj) = meta.as_object_mut() {
            obj.remove("jobs");
        }
        // The sparkline is `metrics?name=jobs`.
        remove_at(&mut meta, "/totals", "samples");
        Some(proto::window(rows, meta))
    }
}

/// The newest workflow runs, from the one shared watcher.
struct RunsTopic {
    sub: Option<(broadcast::Receiver<Value>, super::workflows::SubscriberGuard)>,
    latest: Option<Value>,
}

#[async_trait]
impl Producer for RunsTopic {
    async fn next(&mut self, state: &AdminState) -> Option<Value> {
        if self.sub.is_none() {
            // The watcher needs the database handle, which lands after boot.
            let db = state.db()?;
            self.sub = Some(state.workflows.subscribe(db));
        }
        let (rx, _) = self.sub.as_mut()?;
        newest(rx, &mut self.latest);
        let runs = self.latest.as_ref()?.get("runs")?.as_array()?.clone();
        Some(proto::window(runs, json!({})))
    }
}

struct IncidentsTopic;

#[async_trait]
impl Producer for IncidentsTopic {
    async fn next(&mut self, state: &AdminState) -> Option<Value> {
        let rows = state.incidents.recent(INCIDENT_ROWS);
        let mut meta = state.incidents.summary();
        if let Some(obj) = meta.as_object_mut() {
            // The rows carry these, and the clock changes every tick.
            obj.remove("recent");
            obj.remove("latest");
            obj.remove("server_time_ms");
        }
        Some(proto::window(rows, meta))
    }
}

struct OperationsTopic;

#[async_trait]
impl Producer for OperationsTopic {
    async fn next(&mut self, state: &AdminState) -> Option<Value> {
        let rows: Vec<Value> = state
            .ops
            .recent()
            .into_iter()
            .filter_map(|op| serde_json::to_value(op).ok())
            .collect();
        Some(proto::window(rows, json!({})))
    }

    fn interval(&self) -> Duration {
        Duration::from_millis(500)
    }
}

/// The scheduler's own log tail. Lines get a topic-local counter `n`, the
/// series key, because two lines can share a millisecond.
struct LogsTopic {
    rx: broadcast::Receiver<maintenance::log_ring::LogLine>,
    items: VecDeque<Value>,
    n: u64,
    dropped: u64,
    /// Lines of the seed that share its last timestamp: the receiver, opened
    /// first, may deliver them again.
    seed_tail: Vec<(u64, String, String)>,
}

impl LogsTopic {
    fn new(state: &AdminState) -> Self {
        // Subscribe before reading the history, so nothing falls in between.
        let rx = state.logs.subscribe();
        let mut topic = Self { rx, items: VecDeque::new(), n: 0, dropped: 0, seed_tail: Vec::new() };
        let seed = state.logs.snapshot(LOG_KEEP);
        let last_ts = seed.last().map(|l| l.ts);
        for line in seed {
            if Some(line.ts) == last_ts {
                topic.seed_tail.push((line.ts, line.target.clone(), line.message.clone()));
            }
            topic.push(line);
        }
        topic
    }

    fn push(&mut self, line: maintenance::log_ring::LogLine) {
        self.n += 1;
        let mut item = serde_json::to_value(line).unwrap_or_else(|_| json!({}));
        item["n"] = json!(self.n);
        if self.items.len() == LOG_KEEP {
            self.items.pop_front();
        }
        self.items.push_back(item);
    }
}

#[async_trait]
impl Producer for LogsTopic {
    async fn next(&mut self, _state: &AdminState) -> Option<Value> {
        loop {
            match self.rx.try_recv() {
                Ok(line) => {
                    let id = (line.ts, line.target.clone(), line.message.clone());
                    if let Some(pos) = self.seed_tail.iter().position(|s| *s == id) {
                        self.seed_tail.remove(pos);
                        continue;
                    }
                    if self.seed_tail.first().is_some_and(|s| line.ts > s.0) {
                        self.seed_tail.clear();
                    }
                    self.push(line);
                }
                Err(broadcast::error::TryRecvError::Lagged(n)) => self.dropped += n,
                Err(_) => break,
            }
        }
        Some(proto::series(self.items.iter().cloned().collect(), LOG_KEEP, json!({ "dropped": self.dropped })))
    }

    fn interval(&self) -> Duration {
        Duration::from_millis(250)
    }
}

/* ------------------------------------------------------------------ */
/* The socket                                                           */
/* ------------------------------------------------------------------ */

/// Whether a browser on `Origin` may open this socket.
///
/// The session token is what really gates the connection, and it never
/// travels by cookie, so a hostile page cannot ride a victim's session here.
/// The origin check is defence in depth: with `SPKY_ADMIN_ALLOWED_ORIGINS`
/// set, only those origins (and this host itself) may connect at all.
pub fn origin_allowed(allowed: &[String], headers: &HeaderMap) -> bool {
    if allowed.is_empty() || allowed.iter().any(|o| o == "*") {
        return true;
    }
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        // Not a browser: the token alone decides.
        return true;
    };
    let origin = origin.trim_end_matches('/');
    if allowed.iter().any(|a| a.trim_end_matches('/').eq_ignore_ascii_case(origin)) {
        return true;
    }
    // Same host as the request: the scheduler-hosted dashboard itself.
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    let origin_host = origin.split_once("://").map(|(_, h)| h);
    matches!((host, origin_host), (Some(h), Some(o)) if h.eq_ignore_ascii_case(o))
}

/// Counts a connection while it lives.
struct Connection(Arc<LiveHub>);

impl Drop for Connection {
    fn drop(&mut self) {
        self.0.connections.fetch_sub(1, Ordering::SeqCst);
    }
}

/// `GET /admin/api/v2/live`
pub async fn live(
    ws: WebSocketUpgrade,
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Response {
    if !origin_allowed(&state.config.allowed_origins, &headers) {
        return api_error(StatusCode::FORBIDDEN, "Origin not allowed").into_response();
    }
    ws.max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .on_upgrade(move |socket| connection(socket, state))
}

fn epoch_secs_of(at: Instant) -> u64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    now + at.saturating_duration_since(Instant::now()).as_secs()
}

fn session_info(session: &Session) -> SessionInfo {
    SessionInfo {
        subject: session.subject.clone(),
        label: session.label.clone(),
        mode: session.mode.as_str().to_string(),
        scope: session.scope.as_str().to_string(),
        expires_at: epoch_secs_of(session.expires_at),
    }
}

fn text(msg: &ServerMsg) -> Message {
    Message::Text(encode(msg).to_string())
}

fn close(code: u16, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame { code, reason: reason.into() }))
}

/// The next message from the client: `None` when the socket is gone.
async fn next_client_msg(
    stream: &mut futures::stream::SplitStream<WebSocket>,
) -> Option<Result<ClientMsg, &'static str>> {
    loop {
        match stream.next().await? {
            Ok(Message::Text(t)) => {
                return Some(serde_json::from_str(&t).map_err(|_| "Unreadable message"));
            }
            Ok(Message::Binary(_)) => return Some(Err("Binary messages are not part of the protocol")),
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
            Ok(Message::Close(_)) | Err(_) => return None,
        }
    }
}

async fn connection(socket: WebSocket, state: AdminState) {
    let hub = Arc::clone(&state.live);
    let count = hub.connections.fetch_add(1, Ordering::SeqCst) + 1;
    let _connection = Connection(Arc::clone(&hub));

    let (mut sink, mut stream) = socket.split();
    let (out, mut out_rx) = mpsc::channel::<Message>(OUT_QUEUE);

    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(PING_EVERY);
        ping.tick().await;
        loop {
            tokio::select! {
                msg = out_rx.recv() => {
                    let Some(msg) = msg else { break };
                    let closing = matches!(msg, Message::Close(_));
                    if sink.send(msg).await.is_err() || closing {
                        break;
                    }
                }
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Vec::new())).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = sink.close().await;
    });

    let finish = |out: mpsc::Sender<Message>, writer: JoinHandle<()>| async move {
        drop(out);
        let _ = writer.await;
    };

    if count > MAX_CONNECTIONS {
        let _ = out.send(close(proto::CLOSE_BUSY, "Too many connections")).await;
        return finish(out, writer).await;
    }

    // The first message must be the session token.
    let (mut token, mut session) = match tokio::time::timeout(AUTH_DEADLINE, next_client_msg(&mut stream)).await {
        Ok(Some(Ok(ClientMsg::Auth { token }))) => match state.sessions.get(token.trim()) {
            Some(session) => (token, session),
            None => {
                let _ = out.send(close(proto::CLOSE_UNAUTHORIZED, "Not signed in")).await;
                return finish(out, writer).await;
            }
        },
        Ok(None) => return finish(out, writer).await,
        Ok(Some(_)) => {
            let _ = out.send(close(proto::CLOSE_PROTOCOL, "The first message must be auth")).await;
            return finish(out, writer).await;
        }
        Err(_) => {
            let _ = out.send(close(proto::CLOSE_AUTH_TIMEOUT, "No auth message")).await;
            return finish(out, writer).await;
        }
    };
    debug!(subject = %session.subject, "Live connection opened");

    let ready = |session: &Session| ServerMsg::Ready {
        boot: hub.boot.clone(),
        session: session_info(session),
        topics: TOPICS.iter().map(|t| t.to_string()).collect(),
    };
    if out.send(text(&ready(&session))).await.is_err() {
        return finish(out, writer).await;
    }

    let mut subs: HashMap<String, JoinHandle<()>> = HashMap::new();
    let mut recheck = tokio::time::interval(RECHECK_EVERY);
    recheck.tick().await;
    let mut warned = false;

    loop {
        let expires = tokio::time::Instant::from_std(session.expires_at);
        let warn_at = expires.checked_sub(EXPIRY_WARNING).unwrap_or(expires);
        tokio::select! {
            msg = next_client_msg(&mut stream) => {
                let msg = match msg {
                    None => break,
                    Some(Err(reason)) => {
                        let _ = out.send(close(proto::CLOSE_PROTOCOL, reason)).await;
                        break;
                    }
                    Some(Ok(msg)) => msg,
                };
                match msg {
                    ClientMsg::Auth { token: renewed } => match state.sessions.get(renewed.trim()) {
                        Some(next) => {
                            token = renewed;
                            session = next;
                            warned = false;
                            let _ = out.send(text(&ready(&session))).await;
                        }
                        None => {
                            let _ = out.send(close(proto::CLOSE_UNAUTHORIZED, "Not signed in")).await;
                            break;
                        }
                    },
                    ClientMsg::Sub { reference, topic, args, since } => {
                        let key = proto::topic_key(&topic, &args);
                        if !subs.contains_key(&key) && subs.len() >= MAX_SUBS {
                            let _ = out
                                .send(text(&ServerMsg::Error {
                                    reference: Some(reference),
                                    code: "too_many".into(),
                                    message: format!("At most {MAX_SUBS} subscriptions per connection"),
                                }))
                                .await;
                            continue;
                        }
                        match hub.subscribe(&state, &topic, &args) {
                            Ok((t, subscriber)) => {
                                // A repeated sub restarts delivery from its own
                                // `since`: it is how a reader recovers a gap.
                                if let Some(previous) = subs.remove(&key) {
                                    previous.abort();
                                }
                                let _ = out
                                    .send(text(&ServerMsg::Subscribed { reference, key: key.clone(), shape: t.shape }))
                                    .await;
                                subs.insert(key, tokio::spawn(forward(t, subscriber, since, out.clone())));
                            }
                            Err(message) => {
                                let _ = out
                                    .send(text(&ServerMsg::Error { reference: Some(reference), code: "bad_topic".into(), message }))
                                    .await;
                            }
                        }
                    }
                    ClientMsg::Unsub { key } => {
                        if let Some(handle) = subs.remove(&key) {
                            handle.abort();
                        }
                    }
                    ClientMsg::Ping => {
                        let _ = out.send(text(&ServerMsg::Pong)).await;
                    }
                }
            }
            _ = tokio::time::sleep_until(warn_at), if !warned => {
                warned = true;
                let _ = out.send(text(&ServerMsg::Expiring { at: epoch_secs_of(session.expires_at) })).await;
            }
            _ = tokio::time::sleep_until(expires) => {
                let _ = out.send(close(proto::CLOSE_UNAUTHORIZED, "Session expired")).await;
                break;
            }
            _ = recheck.tick() => {
                // A logout (or a revoked token) ends the socket too.
                if state.sessions.get(token.trim()).is_none() {
                    let _ = out.send(close(proto::CLOSE_UNAUTHORIZED, "Not signed in")).await;
                    break;
                }
            }
        }
    }

    for (_, handle) in subs {
        handle.abort();
    }
    debug!(subject = %session.subject, "Live connection closed");
    finish(out, writer).await;
}

/// Deliver one topic to one connection until the connection drops it.
async fn forward(topic: Arc<Topic>, _subscriber: Subscriber, since: Option<u64>, out: mpsc::Sender<Message>) {
    // Subscribe before reading any state, so no publish falls in between.
    let mut rx = topic.tx.subscribe();
    let mut held: Option<u64> = None;

    if let Some(since) = since {
        if let Some(steps) = topic.replay_from(since) {
            held = Some(since);
            for step in steps {
                if out.send(Message::Text(step.text.to_string())).await.is_err() {
                    return;
                }
                held = Some(step.seq);
            }
        }
    }
    if held.is_none() {
        match send_snap(&topic, held, &out).await {
            Ok(h) => held = h,
            Err(()) => return,
        }
    }

    loop {
        let next = match rx.recv().await {
            Ok(Published::Step(step)) => match held {
                Some(h) if step.seq <= h => continue,
                Some(h) if step.from == h => {
                    if out.send(Message::Text(step.text.to_string())).await.is_err() {
                        return;
                    }
                    Ok(Some(step.seq))
                }
                _ => send_snap(&topic, held, &out).await,
            },
            Ok(Published::Reset) => send_snap(&topic, held, &out).await,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                debug!(key = %topic.key, skipped, "Live subscriber fell behind; resending a snapshot");
                send_snap(&topic, held, &out).await
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        match next {
            Ok(h) => held = h,
            Err(()) => return,
        }
    }
}

/// Send the current snapshot unless the reader already holds it. The new
/// held seq, or `Err` when the connection is gone.
async fn send_snap(topic: &Topic, held: Option<u64>, out: &mpsc::Sender<Message>) -> Result<Option<u64>, ()> {
    match topic.snapshot() {
        Some((seq, _)) if held == Some(seq) => Ok(held),
        Some((seq, text)) => {
            out.send(Message::Text(text.to_string())).await.map_err(|_| ())?;
            Ok(Some(seq))
        }
        None => Ok(held),
    }
}

/* ------------------------------------------------------------------ */
/* GET /admin/api/v2/snapshot                                           */
/* ------------------------------------------------------------------ */

#[derive(Deserialize)]
pub struct SnapshotQuery {
    /// JSON: `[{"topic": "overview"}, {"topic": "metrics", "args": {"name": "sync"}}]`.
    subs: String,
}

#[derive(Deserialize)]
struct SnapshotSub {
    topic: String,
    #[serde(default)]
    args: Value,
}

/// `GET /admin/api/v2/snapshot?subs=[...]`: the same states the socket
/// serves, for a reader that polls (the MCP server, a host without sockets,
/// a network that blocks them). A poll keeps the topics warm for [`GRACE`],
/// so steady polling costs the topic's own cadence, not a rebuild per call.
pub async fn snapshot(
    State(state): State<AdminState>,
    Query(q): Query<SnapshotQuery>,
) -> Result<Json<Value>, ApiError> {
    let subs: Vec<SnapshotSub> = serde_json::from_str(&q.subs)
        .map_err(|e| api_error(StatusCode::BAD_REQUEST, format!("`subs` is not a list of topics: {e}")))?;
    if subs.len() > MAX_SUBS {
        return Err(api_error(StatusCode::BAD_REQUEST, format!("At most {MAX_SUBS} topics per request")));
    }
    let hub = Arc::clone(&state.live);
    let mut snaps = Vec::with_capacity(subs.len());
    for sub in subs {
        let (topic, _subscriber) = hub
            .subscribe(&state, &sub.topic, &sub.args)
            .map_err(|m| api_error(StatusCode::BAD_REQUEST, m))?;
        let mut rx = topic.tx.subscribe();
        if topic.state().is_none() {
            let _ = tokio::time::timeout(FIRST_STATE_WAIT, rx.recv()).await;
        }
        let entry = match topic.state() {
            Some((seq, data)) => json!({ "key": topic.key, "shape": topic.shape, "seq": seq, "data": data }),
            None => json!({ "key": topic.key, "shape": topic.shape, "seq": Value::Null, "data": Value::Null }),
        };
        snaps.push(entry);
    }
    if snaps.iter().any(|s| s["seq"].is_null()) {
        warn!("Live snapshot served a topic that has produced no state yet");
    }
    Ok(Json(json!({ "boot": hub.boot, "snaps": snaps })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(origin: Option<&str>, host: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(o) = origin {
            h.insert(header::ORIGIN, HeaderValue::from_str(o).unwrap());
        }
        h.insert(header::HOST, HeaderValue::from_str(host).unwrap());
        h
    }

    #[test]
    fn origins_are_open_until_configured() {
        assert!(origin_allowed(&[], &headers(Some("https://evil.example"), "a")));
        assert!(origin_allowed(&["*".into()], &headers(Some("https://evil.example"), "a")));
    }

    #[test]
    fn configured_origins_are_exact() {
        let allowed = vec!["https://console.sp00ky.cloud".to_string()];
        assert!(origin_allowed(&allowed, &headers(Some("https://console.sp00ky.cloud"), "x-admin.spky.cloud")));
        assert!(origin_allowed(&allowed, &headers(Some("https://console.sp00ky.cloud/"), "x-admin.spky.cloud")));
        assert!(!origin_allowed(&allowed, &headers(Some("https://console.sp00ky.cloud.evil.example"), "x")));
        assert!(!origin_allowed(&allowed, &headers(Some("https://evil.example"), "x-admin.spky.cloud")));
        // The scheduler-hosted dashboard is same-host and always allowed.
        assert!(origin_allowed(&allowed, &headers(Some("https://x-admin.spky.cloud"), "x-admin.spky.cloud")));
        // Not a browser.
        assert!(origin_allowed(&allowed, &headers(None, "x-admin.spky.cloud")));
    }

    #[test]
    fn keyed_drops_items_without_ids() {
        let v = keyed(vec![json!({"id": "a", "n": 1}), json!({"n": 2})], "id");
        assert_eq!(v, json!({"a": {"id": "a", "n": 1}}));
    }

    #[test]
    fn publish_chains_seqs_and_replays() {
        let hub = LiveHub::new();
        let topic = Topic::new("t".into(), Shape::Doc, "");
        topic.publish(&hub, json!({"a": 1}));
        let (s1, _) = topic.snapshot().unwrap();
        topic.publish(&hub, json!({"a": 1})); // unchanged: no new seq
        assert_eq!(topic.snapshot().unwrap().0, s1);
        topic.publish(&hub, json!({"a": 2}));
        topic.publish(&hub, json!({"a": 3}));
        let (s3, snap) = topic.snapshot().unwrap();
        assert!(snap.contains("\"op\":\"snap\""));
        let steps = topic.replay_from(s1).unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].from, s1);
        assert_eq!(steps[1].seq, s3);
        assert!(topic.replay_from(s3).unwrap().is_empty());
        assert!(topic.replay_from(9999).is_none());
    }

    #[test]
    fn reset_clears_the_ring() {
        let hub = LiveHub::new();
        let topic = Topic::new("s".into(), Shape::Series, "t");
        topic.publish(&hub, proto::series(vec![json!({"t": 5})], 10, json!({})));
        let (s1, _) = topic.snapshot().unwrap();
        topic.publish(&hub, proto::series(vec![json!({"t": 1})], 10, json!({})));
        assert!(topic.replay_from(s1).is_none(), "a reset cannot be replayed across");
    }
}
