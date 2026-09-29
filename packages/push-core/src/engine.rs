//! The push engine: observes ingested rows, delivers pushes, keeps the books.
//!
//! Hosts own the plumbing (a database port, an HTTP port, a task spawner, a
//! 1 s timer); this owns every decision. Nothing here blocks or sleeps: time
//! is read from a clock and all waiting is expressed as "due at" timestamps
//! that [`PushEngine::tick`] checks, so the same code runs under tokio and on
//! a wasm32 isolate.
//!
//! Locking: one `std::sync::Mutex` for the mutable state and one `RwLock` for
//! the loaded config. Neither is ever held across an `.await`, which keeps the
//! futures `Send` for hosts that spawn them.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use futures::stream::{self, StreamExt};
use lru::LruCache;
use schedule_core::db::{first_row, rows};
use schedule_core::{MaybeSendSync, ScheduleDb, ScheduleDbError};
use serde::Serialize;
use serde_json::{json, Map, Value};
use tracing::{debug, info, warn};

use crate::config::{valid_app_id, Op, Platform, PushConfig, Rule, Target, Urgency};
use crate::ece;
use crate::native::{self, Answer, DeviceKind, Providers};
use crate::rules::{self, RowRef};
use crate::util::{canonical_record_id, epoch_millis, now_ms, record_binding, split_record_id};
use crate::vapid::VapidKeys;

pub const MESSAGE_TABLE: &str = "_00_push_message";
pub const SUBSCRIPTION_TABLE: &str = "_00_push_subscription";
pub const CONFIG_TABLE: &str = "_00_push_config";

/// Outbound HTTP, supplied by the host (reqwest natively, `fetch` on
/// Workers).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait PushHttp: MaybeSendSync {
    /// POST `body` to `url` with `headers`. Ok((status, body_prefix)) for any
    /// HTTP answer, Err for transport failures (retried).
    async fn post(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(u16, String), String>;
}

/// Where an observed row came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    /// A change as it happens (changefeed sink, `/ingest`).
    Live,
    /// A change seen again (changefeed look-back, WAL replay). Pushes, but
    /// the dedupe and `maxAge` usually stop it.
    Replay,
    /// Drift repair re-emitting old rows. Never pushes.
    Repair,
}

#[derive(Debug, Clone)]
pub struct ObservedChange {
    pub table: String,
    pub op: Op,
    /// `table:key`, as the ingest path carries it.
    pub id: String,
    /// The row after the change; for a delete, the row before it.
    pub record: Value,
    pub origin: Origin,
    /// Ingest order, from [`PushEngine::next_seq`] taken synchronously on the
    /// ingest path before the observe is spawned. Spawned observes race each
    /// other; this is how a throttled topic still ends on its newest row.
    /// `0` = unknown (stamped on arrival).
    pub seq: u64,
}

/// Epoch milliseconds. Injectable so tests can walk through throttles and
/// rate limits without sleeping.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Wait this many milliseconds. Supplied by the host (the crate has no
/// runtime); without one the engine never waits and leaves the case it would
/// have waited for to the sweep.
pub type Sleep =
    Arc<dyn Fn(u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Clone)]
pub struct EngineOptions {
    /// How often `_00_push_config` is re-read.
    pub config_reload_ms: u64,
    /// How often due / missed `_00_push_message` rows are swept.
    pub sweep_ms: u64,
    /// How often old messages and long-disabled subscriptions are deleted.
    pub prune_ms: u64,
    /// How often the published VAPID params are re-checked once published.
    pub vapid_recheck_ms: u64,
    /// Per-user subscription cache for rule pushes.
    pub subscription_cache_ms: u64,
    /// Rule dedupe keys remembered.
    pub dedupe_capacity: usize,
    /// Delay before each retry of a failed send; its length is the number of
    /// retries.
    pub retry_backoff_ms: Vec<u64>,
    pub retry_queue_cap: usize,
    /// Concurrent HTTP sends per observe / tick.
    pub send_concurrency: usize,
    /// Push-service TTL when neither the rule nor the defaults set one.
    pub default_ttl_secs: u64,
    /// `last_ok_at` is written at most this often per subscription.
    pub last_ok_interval_ms: u64,
    /// VAPID JWTs are valid 12 h and re-signed after this.
    pub jwt_refresh_ms: u64,
    /// A pending message this old was missed by ingest; the sweep sends it.
    pub stale_pending_ms: u64,
    /// A message `sending` this long lost its host; the sweep resets it.
    pub stuck_sending_ms: u64,
    pub sweep_batch: usize,
    pub message_retention_ms: u64,
    pub disabled_retention_ms: u64,
    /// Accept `http://` and private / local endpoint hosts. Off in production:
    /// see [`endpoint_allowed`].
    pub allow_private_endpoints: bool,
    pub clock: Option<Clock>,
    pub sleep: Option<Sleep>,
    /// How long a direct message's claim keeps retrying while its row is not
    /// visible yet (see `observe_message`).
    pub claim_visible_budget_ms: u64,
    /// A 404/410 for a subscription (re)made this recently is retried, not
    /// taken as "gone" (see `send_one`).
    pub fresh_subscription_grace_ms: u64,
    /// `SPKY_PUSH=off`: nothing is sent, web or native.
    pub off: bool,
}

const MINUTE: u64 = 60_000;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            config_reload_ms: 30_000,
            sweep_ms: 5_000,
            prune_ms: HOUR,
            vapid_recheck_ms: 5 * MINUTE,
            subscription_cache_ms: 5_000,
            dedupe_capacity: 100_000,
            retry_backoff_ms: vec![2_000, 10_000, 60_000],
            retry_queue_cap: 10_000,
            send_concurrency: 8,
            default_ttl_secs: 86_400,
            last_ok_interval_ms: 10 * MINUTE,
            jwt_refresh_ms: 11 * HOUR,
            stale_pending_ms: 30_000,
            stuck_sending_ms: 5 * MINUTE,
            sweep_batch: 100,
            message_retention_ms: 7 * DAY,
            disabled_retention_ms: 30 * DAY,
            allow_private_endpoints: false,
            clock: None,
            sleep: None,
            claim_visible_budget_ms: 5_000,
            fresh_subscription_grace_ms: 2 * MINUTE,
            off: false,
        }
    }
}

impl std::fmt::Debug for EngineOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineOptions")
            .field("config_reload_ms", &self.config_reload_ms)
            .field("sweep_ms", &self.sweep_ms)
            .field("dedupe_capacity", &self.dedupe_capacity)
            .field("send_concurrency", &self.send_concurrency)
            .field("retry_backoff_ms", &self.retry_backoff_ms)
            .field("clock", &self.clock.as_ref().map(|_| "custom"))
            .finish_non_exhaustive()
    }
}

impl EngineOptions {
    /// Defaults with the few operator knobs read from the environment:
    /// `SPKY_PUSH_DEDUPE_CAPACITY`, `SPKY_PUSH_SEND_CONCURRENCY`,
    /// `SPKY_PUSH_CONFIG_RELOAD_SECS`, `SPKY_PUSH_DEFAULT_TTL_SECS`.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> EngineOptions {
        let num = |k: &str| {
            get(k)
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|n| *n > 0)
        };
        let mut o = EngineOptions::default();
        if let Some(n) = num("SPKY_PUSH_DEDUPE_CAPACITY") {
            o.dedupe_capacity = n as usize;
        }
        if let Some(n) = num("SPKY_PUSH_SEND_CONCURRENCY") {
            o.send_concurrency = n as usize;
        }
        if let Some(n) = num("SPKY_PUSH_CONFIG_RELOAD_SECS") {
            o.config_reload_ms = n * 1000;
        }
        if let Some(n) = num("SPKY_PUSH_DEFAULT_TTL_SECS") {
            o.default_ttl_secs = n;
        }
        o.off = get("SPKY_PUSH").is_some_and(|v| truthy_off(&v));
        // Local development against a mock push service only.
        o.allow_private_endpoints = get("SPKY_PUSH_ALLOW_PRIVATE_ENDPOINTS")
            .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        o
    }
}

// ── Status ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PushCounters {
    /// Rule matches (after `when`, `maxAge` and the dedupe).
    pub matched: u64,
    /// `_00_push_message` rows handled.
    pub messages: u64,
    /// HTTP sends the push service accepted.
    pub sent: u64,
    /// Sends that failed for good.
    pub failed: u64,
    /// Pushes dropped by a limit (per user, per project, queue caps).
    pub dropped: u64,
    /// Pushes folded into a trailing throttled push.
    pub throttled: u64,
    /// Sends put on the retry queue.
    pub retried: u64,
    /// Rows already pushed (re-ingested, or `once`).
    pub deduped: u64,
    /// Rows from drift repair.
    pub skipped_repair: u64,
    /// Native devices skipped because their provider (push.apns / push.fcm)
    /// has no usable credential.
    pub no_provider: u64,
}

impl PushCounters {
    fn add(&mut self, o: &PushCounters) {
        self.matched += o.matched;
        self.messages += o.messages;
        self.sent += o.sent;
        self.failed += o.failed;
        self.dropped += o.dropped;
        self.throttled += o.throttled;
        self.retried += o.retried;
        self.deduped += o.deduped;
        self.skipped_repair += o.skipped_repair;
        self.no_provider += o.no_provider;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PushQueues {
    pub retry: usize,
    /// Throttle slots holding a trailing push.
    pub trailing: usize,
    pub throttle_slots: usize,
    /// Messages waiting on retries before their final status is written.
    pub messages_in_flight: usize,
    pub dedupe_entries: usize,
    pub cached_users: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PushStatus {
    pub enabled: bool,
    /// Why `enabled` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub kid: Option<String>,
    pub public_key: Option<String>,
    pub subject: String,
    pub rules: usize,
    pub rule_names: Vec<String>,
    pub config_hash: Option<String>,
    pub config_loaded: bool,
    pub vapid_published: bool,
    pub totals: PushCounters,
    pub last_minute: PushCounters,
    pub queues: PushQueues,
    pub last_error: Option<String>,
    pub providers: ProviderStatus,
}

/// What each delivery path can do right now.
#[derive(Debug, Clone, Serialize)]
pub struct ProviderStatus {
    /// A VAPID key is loaded.
    pub web: bool,
    pub apns: ProviderState,
    pub fcm: ProviderState,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProviderState {
    /// A credential row exists.
    pub configured: bool,
    /// It parsed; devices of this kind get pushes.
    pub ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ProviderState {
    fn of<T>(p: &Option<Result<T, String>>) -> ProviderState {
        match p {
            None => ProviderState::default(),
            Some(Ok(_)) => ProviderState { configured: true, ready: true, error: None },
            Some(Err(e)) => ProviderState { configured: true, ready: false, error: Some(e.clone()) },
        }
    }
}

// ── Internals ────────────────────────────────────────────────────────────

struct Loaded {
    config: Arc<PushConfig>,
    /// `None` until the first successful read; `Some("")` for "no row".
    hash: Option<String>,
    /// table -> bitmask of watched ops. Empty while disabled.
    watch: HashMap<String, u8>,
    /// Native providers from `_00_push_credential`, rebuilt only when
    /// `credentials` (their fingerprint) changes.
    providers: Arc<Providers>,
    credentials: String,
}

fn op_bit(op: Op) -> u8 {
    match op {
        Op::Create => 1,
        Op::Update => 2,
        Op::Delete => 4,
    }
}

impl Loaded {
    fn new(
        config: PushConfig,
        hash: Option<String>,
        providers: Arc<Providers>,
        credentials: String,
    ) -> Loaded {
        let mut watch: HashMap<String, u8> = HashMap::new();
        if config.enabled {
            for rule in config.rules.values().filter(|r| r.enabled) {
                let bits = rule.ops().into_iter().fold(0u8, |acc, op| acc | op_bit(op));
                *watch.entry(rule.table.clone()).or_default() |= bits;
            }
        }
        Loaded {
            config: Arc::new(config),
            hash,
            watch,
            providers,
            credentials,
        }
    }
}

#[derive(Debug, Clone)]
struct Subscription {
    /// As the database printed it; the map key for per-subscription state.
    id: String,
    /// Bound for `type::record('_00_push_subscription', $k)`.
    key: Value,
    user: String,
    kind: DeviceKind,
    platform: Platform,
    /// Web: the push service URL. Native: `<kind>:<token>`, never contacted.
    endpoint: String,
    p256dh: String,
    auth: String,
    /// Native device token.
    token: Option<String>,
    /// Bundle id / package name (APNs topic).
    app_id: Option<String>,
    /// APNs `sandbox` or `production`.
    environment: Option<String>,
    kid: String,
    rules: Option<Vec<String>>,
    failures: i64,
    /// Last (re)subscribe, epoch ms.
    updated_ms: Option<i64>,
}

fn parse_subscription(v: &Value) -> Option<Subscription> {
    let id = v.get("id")?.as_str()?.to_string();
    let (_, key) = record_binding(&id)?;
    let text = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
    let kind = DeviceKind::parse(v.get("kind").and_then(Value::as_str))?;
    Some(Subscription {
        key,
        user: canonical_record_id(v.get("auth_id")?.as_str()?)?,
        kind,
        platform: kind.platform(v.get("platform").and_then(Value::as_str)),
        endpoint: v.get("endpoint")?.as_str()?.to_string(),
        p256dh: text("p256dh").unwrap_or_default(),
        auth: text("auth").unwrap_or_default(),
        token: text("token"),
        app_id: text("app_id"),
        environment: text("environment"),
        kid: v
            .get("kid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        rules: v.get("rules").and_then(Value::as_array).map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        }),
        failures: v.get("failures").and_then(Value::as_i64).unwrap_or(0),
        updated_ms: v.get("updated_at").and_then(crate::util::epoch_millis),
        id,
    })
}

/// One push rendered for every kind of device.
#[derive(Debug)]
struct Rendered {
    /// The Web Push plaintext; `None` when it could not be encoded (web
    /// devices then get nothing, native ones still do).
    web: Option<Vec<u8>>,
    native: native::NativePush,
}

/// One push for one user, ready to encrypt per subscription.
#[derive(Debug, Clone)]
struct Delivery {
    user: String,
    /// `ObservedChange::seq` of the row it renders.
    seq: u64,
    push: Arc<Rendered>,
    ttl: u64,
    urgency: Urgency,
    /// The readable topic; each transport derives its collapse header.
    topic: Option<String>,
    /// Rule name, for the per-device rule filter. `None` for messages.
    rule: Option<String>,
    /// The rule's `platforms`; `None` = every kind of device.
    platforms: Option<Arc<Vec<Platform>>>,
    /// Canonical `_00_push_message` id this delivery reports to.
    message: Option<String>,
}

#[derive(Debug, Clone)]
struct SendJob {
    sub: Subscription,
    delivery: Delivery,
    attempt: usize,
}

#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Ok,
    /// Delivered after switching the APNs environment; store the new one.
    OkMoved(String),
    /// The provider refused OUR credentials (APNs provider token, FCM
    /// access token): retry with a fresh token, the device is fine.
    Provider(String),
    /// 404/410: the subscription no longer exists.
    Gone(String),
    /// The push service refused the subscription (400/401/403) or its keys
    /// are unusable: disable it.
    Rejected(String),
    /// Give up on this push, keep the subscription (413, unexpected codes).
    GiveUp(String),
    /// 429/5xx/transport: retry later.
    Retry(String),
    /// A limit dropped it before sending.
    Dropped(String),
}

struct RetryItem {
    job: SendJob,
    due_ms: u64,
}

struct ThrottleSlot {
    gap_ms: u64,
    last_sent_ms: Option<u64>,
    /// Newest row already sent under this slot.
    last_sent_seq: u64,
    pending: Option<Delivery>,
}

/// Token bucket refilled continuously at `capacity` per minute.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    updated_ms: u64,
}

impl Bucket {
    fn full(capacity: u32, now: u64) -> Bucket {
        Bucket {
            tokens: capacity as f64,
            updated_ms: now,
        }
    }

    fn refill(&mut self, capacity: u32, now: u64) {
        let elapsed = now.saturating_sub(self.updated_ms) as f64;
        self.tokens =
            (self.tokens + elapsed * capacity as f64 / MINUTE as f64).min(capacity as f64);
        self.updated_ms = now;
    }

    fn take(&mut self, capacity: u32, now: u64) -> bool {
        self.refill(capacity, now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
struct MsgProgress {
    key: Value,
    delivered: u32,
    outstanding: u32,
    error: Option<String>,
}

/// What one batch of sends did for one message.
#[derive(Debug, Clone, Default)]
struct MsgTally {
    jobs: u32,
    delivered: u32,
    retrying: u32,
    error: Option<String>,
    /// Why devices were left out (no provider); reported only when nothing
    /// was delivered.
    skipped: Option<String>,
}

/// Subscription writes collected from a batch of sends, written in one
/// round-trip.
#[derive(Default)]
struct Book {
    ok: Vec<Value>,
    gone: Vec<Value>,
    disable: Vec<Value>,
    fail: Vec<Value>,
    /// `{ k, environment }`: APNs rows registered under the wrong one.
    moved: Vec<Value>,
}

impl Book {
    fn is_empty(&self) -> bool {
        self.ok.is_empty()
            && self.gone.is_empty()
            && self.disable.is_empty()
            && self.fail.is_empty()
            && self.moved.is_empty()
    }
}

/// 60 one-second buckets for the "last minute" counters.
struct Window {
    buckets: Vec<(u64, PushCounters)>,
}

impl Window {
    fn new() -> Window {
        Window {
            buckets: vec![(u64::MAX, PushCounters::default()); 60],
        }
    }
    fn bucket(&mut self, now: u64) -> &mut PushCounters {
        let sec = now / 1000;
        let slot = &mut self.buckets[(sec % 60) as usize];
        if slot.0 != sec {
            *slot = (sec, PushCounters::default());
        }
        &mut slot.1
    }
    fn sum(&self, now: u64) -> PushCounters {
        let sec = now / 1000;
        let mut out = PushCounters::default();
        for (s, c) in &self.buckets {
            if *s != u64::MAX && *s + 60 > sec && *s <= sec {
                out.add(c);
            }
        }
        out
    }
}

struct State {
    dedupe: LruCache<String, ()>,
    throttles: HashMap<String, ThrottleSlot>,
    user_buckets: HashMap<String, Bucket>,
    global_bucket: Option<Bucket>,
    retries: Vec<RetryItem>,
    sub_cache: HashMap<String, (u64, Arc<Vec<Subscription>>)>,
    last_ok_written: HashMap<String, u64>,
    jwt_cache: HashMap<String, (String, u64)>,
    messages: HashMap<String, MsgProgress>,
    totals: PushCounters,
    window: Window,
    last_error: Option<String>,
    last_limit_warn_ms: Option<u64>,
    vapid_published: bool,
    last_vapid_check_ms: Option<u64>,
    last_config_ms: Option<u64>,
    last_sweep_ms: Option<u64>,
    last_prune_ms: Option<u64>,
    bad_config_hash: Option<String>,
}

impl State {
    fn count(&mut self, now: u64, f: impl Fn(&mut PushCounters)) {
        f(&mut self.totals);
        f(self.window.bucket(now));
    }

    /// Limits are expected to bite under load; one line a minute is enough to
    /// notice without drowning the log.
    fn limit_warn(&mut self, now: u64, what: &str) {
        if self.last_limit_warn_ms.is_none_or(|t| now >= t + MINUTE) {
            self.last_limit_warn_ms = Some(now);
            warn!(target: "push", "push limit reached ({what}); dropping pushes (logged once a minute)");
        }
    }
}

const ROW_PRELUDE: &str =
    "LET $row = (SELECT * FROM ONLY type::record($__tb, $__key)) ?? $__row; LET $rule = $__rule;\n";
const ROW_PRELUDE_JSON: &str = "LET $row = $__row; LET $rule = $__rule;\n";

const SELECT_SUBSCRIPTIONS: &str = "\
LET $subs = SELECT * FROM _00_push_subscription WHERE auth_id IN $ids AND disabled_at = NONE; \
RETURN $subs; \
SELECT id, auth_id, endpoint, updated_at FROM _00_push_subscription \
WHERE endpoint IN $subs.endpoint AND disabled_at = NONE;";

const CLAIM_MESSAGE: &str = "\
UPDATE type::record('_00_push_message', $k) SET status = 'sending', claimed_at = time::now() \
WHERE status = 'pending' AND (send_at = NONE OR send_at <= time::now()) RETURN AFTER;";

/// What a failed claim found: the row's status and whether it is due, or
/// NONE when the row does not exist (yet).
const MESSAGE_STATE: &str = "RETURN (SELECT status, (send_at = NONE OR send_at <= time::now()) AS due \
FROM ONLY type::record('_00_push_message', $k));";

/// What one claim attempt found.
enum Claim {
    /// Claimed and handled, or not claimable (taken, cancelled, not due).
    Skipped,
    /// No such row yet (the creating transaction has not committed), or a
    /// row that became claimable after the claim ran: retry.
    Missing,
}

const FINISH_MESSAGE: &str = "\
UPDATE type::record('_00_push_message', $k) SET status = $status, sent_at = time::now(), \
delivered = $delivered, error = $error ?? NONE RETURN NONE;";

const SELECT_CONFIG: &str = "SELECT spec_json, hash FROM ONLY _00_push_config:default;";
/// Its own query: a database migrated before native push has no such table,
/// which must not break loading the config.
const SELECT_CREDENTIALS: &str = "SELECT id, secret, hash FROM _00_push_credential;";

// ── The engine ───────────────────────────────────────────────────────────

pub struct PushEngine {
    db: Arc<dyn ScheduleDb>,
    http: Arc<dyn PushHttp>,
    keys: Option<VapidKeys>,
    opts: EngineOptions,
    loaded: RwLock<Arc<Loaded>>,
    state: Mutex<State>,
    ticking: AtomicBool,
    seq: std::sync::atomic::AtomicU64,
}

/// Clears the tick flag even when the host drops a tick future midway.
struct TickGuard<'a>(&'a AtomicBool);

impl Drop for TickGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

fn truthy_off(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "off" | "0" | "false" | "no" | "disabled"
    )
}

impl PushEngine {
    pub fn new(
        db: Arc<dyn ScheduleDb>,
        http: Arc<dyn PushHttp>,
        keys: Option<VapidKeys>,
        opts: EngineOptions,
    ) -> Self {
        let capacity = NonZeroUsize::new(opts.dedupe_capacity.max(1)).unwrap_or(NonZeroUsize::MIN);
        PushEngine {
            db,
            http,
            keys,
            loaded: RwLock::new(Arc::new(Loaded::new(
                PushConfig::default(),
                None,
                Arc::new(Providers::default()),
                String::new(),
            ))),
            state: Mutex::new(State {
                dedupe: LruCache::new(capacity),
                throttles: HashMap::new(),
                user_buckets: HashMap::new(),
                global_bucket: None,
                retries: Vec::new(),
                sub_cache: HashMap::new(),
                last_ok_written: HashMap::new(),
                jwt_cache: HashMap::new(),
                messages: HashMap::new(),
                totals: PushCounters::default(),
                window: Window::new(),
                last_error: None,
                last_limit_warn_ms: None,
                vapid_published: false,
                last_vapid_check_ms: None,
                last_config_ms: None,
                last_sweep_ms: None,
                last_prune_ms: None,
                bad_config_hash: None,
            }),
            ticking: AtomicBool::new(false),
            seq: std::sync::atomic::AtomicU64::new(1),
            opts,
        }
    }

    /// Keys from env: `SPKY_VAPID_PRIVATE_KEY`, else derived from
    /// `SPKY_AUTH_SECRET`. `SPKY_PUSH=off` (or 0/false/no/disabled) returns
    /// `None`. Hosts pass `|k| std::env::var(k).ok()`.
    pub fn keys_from_env(get: impl Fn(&str) -> Option<String>) -> Option<VapidKeys> {
        if get("SPKY_PUSH").is_some_and(|v| truthy_off(&v)) {
            info!(target: "push", "web push is off (SPKY_PUSH)");
            return None;
        }
        if let Some(private) = get("SPKY_VAPID_PRIVATE_KEY").filter(|v| !v.trim().is_empty()) {
            return match VapidKeys::from_private_b64url(private.trim()) {
                Ok(keys) => Some(keys),
                Err(e) => {
                    // Not falling back to the derived key: it would silently
                    // orphan every subscription made under the configured one.
                    warn!(target: "push", "SPKY_VAPID_PRIVATE_KEY is unusable ({e}); web push is off");
                    None
                }
            };
        }
        match get("SPKY_AUTH_SECRET").filter(|v| !v.trim().is_empty()) {
            Some(secret) => VapidKeys::from_secret(&secret).ok(),
            None => {
                warn!(target: "push", "web push is off: neither SPKY_VAPID_PRIVATE_KEY nor SPKY_AUTH_SECRET is set");
                None
            }
        }
    }

    /// The next ingest sequence number (see [`ObservedChange::seq`]). Hosts
    /// take it on the ingest path itself, before spawning the observe.
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    pub fn keys(&self) -> Option<&VapidKeys> {
        self.keys.as_ref()
    }

    fn now(&self) -> u64 {
        match &self.opts.clock {
            Some(clock) => clock(),
            None => now_ms(),
        }
    }

    fn loaded(&self) -> Arc<Loaded> {
        match self.loaded.read() {
            Ok(guard) => Arc::clone(&guard),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // A panic while holding the lock (none is expected) must not take
        // push down for the rest of the process.
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Something can be delivered: not switched off, and a VAPID key or a
    /// native provider is loaded. (`push.enabled` is checked separately.)
    fn can_send(&self) -> bool {
        !self.opts.off && (self.keys.is_some() || self.loaded().providers.any())
    }

    fn set_error(&self, msg: impl Into<String>) {
        self.state().last_error = Some(msg.into());
    }

    fn count(&self, f: impl Fn(&mut PushCounters)) {
        let now = self.now();
        self.state().count(now, f);
    }

    /// Sync, cheap: does this change need [`PushEngine::observe`]? True for
    /// every `_00_push_message` change (hosts intercept that table anyway)
    /// and for (table, op) pairs an enabled rule watches.
    pub fn wants(&self, table: &str, op: Op) -> bool {
        if table == MESSAGE_TABLE {
            return true;
        }
        if !self.can_send() {
            return false;
        }
        let loaded = self.loaded();
        loaded
            .watch
            .get(table)
            .is_some_and(|bits| bits & op_bit(op) != 0)
    }

    /// Everything for one row. Hosts spawn it (bounded concurrency).
    pub async fn observe(&self, mut change: ObservedChange) {
        if change.seq == 0 {
            change.seq = self.next_seq();
        }
        if change.table == MESSAGE_TABLE {
            self.observe_message(change).await;
        } else {
            self.observe_rule_row(change).await;
        }
    }

    // ── Rules ────────────────────────────────────────────────────────────

    async fn observe_rule_row(&self, change: ObservedChange) {
        if !self.can_send() {
            return;
        }
        let loaded = self.loaded();
        let cfg = Arc::clone(&loaded.config);
        if !cfg.enabled {
            return;
        }
        if change.origin == Origin::Repair {
            self.count(|c| c.skipped_repair += 1);
            return;
        }
        let now = self.now();
        let id = canonical_record_id(&change.id).unwrap_or_else(|| change.id.clone());
        for (name, rule) in &cfg.rules {
            if !rule.enabled || rule.table != change.table || !rule.ops().contains(&change.op) {
                continue;
            }
            if !rules::when_matches(&rule.when, &change.record) {
                continue;
            }
            if let Some(max_age) = &rule.max_age {
                if !rules::max_age_ok(max_age, &change.record, now) {
                    debug!(target: "push", rule = %name, id = %id, "row older than maxAge");
                    continue;
                }
            }
            let key = rules::dedupe_key(name, rule, &id, change.op, &change.record);
            let fresh = {
                let mut st = self.state();
                if st.dedupe.get(&key).is_some() {
                    st.count(now, |c| c.deduped += 1);
                    false
                } else {
                    st.dedupe.put(key, ());
                    st.count(now, |c| c.matched += 1);
                    true
                }
            };
            if !fresh {
                debug!(target: "push", rule = %name, id = %id, "already pushed");
                continue;
            }
            self.fire_rule(name, rule, &cfg, &change, &id, now).await;
        }
    }

    /// Run a manifest query (`with`, `to.query`) as root with `$row` and
    /// `$rule` bound. `$row` is re-read from the table so record links are
    /// real records (`SELECT .. FROM ONLY $row.sender` works); a deleted row
    /// falls back to the observed before-image, whose links are strings.
    /// The result is the query's last statement.
    async fn user_query(
        &self,
        surql: &str,
        change: &ObservedChange,
        rule_name: &str,
    ) -> Result<Value, ScheduleDbError> {
        let mut binds: Vec<(&str, Value)> = vec![
            ("__row", change.record.clone()),
            ("__rule", Value::String(rule_name.to_string())),
        ];
        let prelude = match record_binding(&change.id) {
            Some((tb, key)) => {
                binds.push(("__tb", Value::String(tb)));
                binds.push(("__key", key));
                ROW_PRELUDE
            }
            None => ROW_PRELUDE_JSON,
        };
        let sql = format!("{prelude}{surql}");
        let results = self.db.query(&sql, &binds).await?;
        Ok(results.into_iter().skip(2).last().unwrap_or(Value::Null))
    }

    async fn fire_rule(
        &self,
        name: &str,
        rule: &Rule,
        cfg: &PushConfig,
        change: &ObservedChange,
        id: &str,
        now: u64,
    ) {
        let ids = match (&rule.to, rules::target_field_ids(rule, &change.record)) {
            (_, Some(ids)) => ids,
            (Target::Query { query }, None) => match self.user_query(query, change, name).await {
                Ok(v) => {
                    let mut out = Vec::new();
                    rules::collect_ids(&v, &mut out);
                    out
                }
                Err(e) => {
                    warn!(target: "push", rule = %name, id = %id, error = %e, "to.query failed; row skipped");
                    self.set_error(format!("rule {name}: to.query failed: {e}"));
                    return;
                }
            },
            (Target::Fields(_), None) => Vec::new(),
        };
        let except = rules::field_ids(&rule.except, &change.record);
        let cap = cfg.limits.max_recipients() as usize;
        let (recipients, capped) = rules::finalize_recipients(ids, &except, cap);
        if capped {
            warn!(target: "push", rule = %name, id = %id, cap, "recipients capped at limits.maxRecipients");
        }
        if recipients.is_empty() {
            debug!(target: "push", rule = %name, id = %id, "no recipients");
            return;
        }

        // After the recipients: a row nobody receives costs no extra queries.
        let mut with = Map::new();
        for (wname, surql) in &rule.with {
            let value = match self.user_query(surql, change, name).await {
                Ok(v) => v,
                Err(e) => {
                    debug!(target: "push", rule = %name, with = %wname, error = %e, "with query failed; rendering null");
                    Value::Null
                }
            };
            with.insert(wname.clone(), value);
        }

        let row = RowRef {
            table: &change.table,
            id,
            op: change.op,
            record: &change.record,
        };
        let ctx = rules::context(row, name, &with, now);
        let per_recipient = rules::rule_references_recipient(rule, &cfg.defaults);
        let ttl = rule
            .ttl
            .as_ref()
            .or(cfg.defaults.ttl.as_ref())
            .map(|d| d.as_secs())
            .unwrap_or(self.opts.default_ttl_secs);
        let urgency = rule
            .urgency
            .or(cfg.defaults.urgency)
            .unwrap_or(Urgency::Normal);
        let gap_ms = rule
            .throttle
            .as_ref()
            .or(cfg.defaults.throttle.as_ref())
            .map(|d| d.as_millis())
            .filter(|ms| *ms > 0);

        let build = |ctx: &Value| -> (String, Arc<Rendered>) {
            let topic = rules::render_topic(rule, ctx, id);
            let payload = rules::rule_payload(name, rule, &cfg.defaults, row, ctx, &topic, now);
            let web = match rules::encode_payload(&payload) {
                Ok(enc) => {
                    if !enc.degraded.is_empty() {
                        info!(target: "push", rule = %name, id = %id, dropped = ?enc.degraded, "payload over 3993 bytes; trimmed");
                    }
                    Some(enc.bytes)
                }
                Err(e) => {
                    warn!(target: "push", rule = %name, id = %id, error = %e, "payload cannot be encoded for web push");
                    None
                }
            };
            let native = native::rule_native(rule, &cfg.defaults, ctx, &topic, &payload);
            (topic, Arc::new(Rendered { web, native }))
        };
        let shared = (!per_recipient).then(|| build(&ctx));
        let platforms = rule.platforms.clone().map(Arc::new);

        let mut items = Vec::with_capacity(recipients.len());
        for user in recipients {
            let (topic, push) = match &shared {
                Some(s) => s.clone(),
                None => build(&rules::with_recipient(&ctx, &user)),
            };
            let throttle_key = gap_ms.map(|gap| (format!("{name}\u{1f}{user}\u{1f}{topic}"), gap));
            items.push((
                throttle_key,
                Delivery {
                    user,
                    seq: change.seq,
                    push,
                    ttl,
                    urgency,
                    topic: Some(topic),
                    rule: Some(name.to_string()),
                    platforms: platforms.clone(),
                    message: None,
                },
            ));
        }
        let admitted = self.admit(items, cfg, now);
        self.deliver(admitted, false).await;
    }

    /// Throttle and the per-user limit, under one lock. Throttled pushes
    /// replace the slot's trailing push (latest row wins).
    fn admit(
        &self,
        items: Vec<(Option<(String, u64)>, Delivery)>,
        cfg: &PushConfig,
        now: u64,
    ) -> Vec<Delivery> {
        let per_user = cfg.limits.per_user_per_minute();
        let mut out = Vec::with_capacity(items.len());
        let mut st = self.state();
        for (throttle, delivery) in items {
            if let Some((key, gap_ms)) = throttle {
                let slot = st.throttles.entry(key).or_insert(ThrottleSlot {
                    gap_ms,
                    last_sent_ms: None,
                    last_sent_seq: 0,
                    pending: None,
                });
                slot.gap_ms = gap_ms;
                if slot.last_sent_ms.is_some_and(|t| now < t + gap_ms) {
                    // Only a row newer than what is on screen (and than what
                    // already waits) may become the trailing push: observes
                    // run concurrently, so an older row can arrive last.
                    let newest_pending = slot.pending.as_ref().map_or(0, |p| p.seq);
                    if delivery.seq > slot.last_sent_seq && delivery.seq >= newest_pending {
                        slot.pending = Some(delivery);
                    }
                    st.count(now, |c| c.throttled += 1);
                    continue;
                }
                slot.last_sent_ms = Some(now);
                slot.last_sent_seq = slot.last_sent_seq.max(delivery.seq);
            }
            if !Self::take_user(&mut st, &delivery.user, per_user, now) {
                st.count(now, |c| c.dropped += 1);
                st.limit_warn(now, "limits.perUserPerMinute");
                continue;
            }
            out.push(delivery);
        }
        out
    }

    fn take_user(st: &mut State, user: &str, capacity: u32, now: u64) -> bool {
        st.user_buckets
            .entry(user.to_string())
            .or_insert_with(|| Bucket::full(capacity, now))
            .take(capacity, now)
    }

    // ── Delivery ─────────────────────────────────────────────────────────

    /// Enabled subscriptions per user (canonical id), shared endpoints
    /// resolved. Rule pushes use a short per-user cache; direct messages read
    /// fresh.
    async fn subscriptions(
        &self,
        users: &[String],
        fresh: bool,
    ) -> Result<HashMap<String, Arc<Vec<Subscription>>>, String> {
        let now = self.now();
        let mut out = HashMap::new();
        let mut misses = Vec::new();
        {
            let st = self.state();
            for u in users {
                match st.sub_cache.get(u) {
                    Some((at, subs)) if !fresh && now < at + self.opts.subscription_cache_ms => {
                        out.insert(u.clone(), Arc::clone(subs));
                    }
                    _ => misses.push(u.clone()),
                }
            }
        }
        if misses.is_empty() {
            return Ok(out);
        }
        // auth_id is `<string> $auth.id`, which quotes keys that need it, so
        // ask for every spelling of each id.
        let mut ids = Vec::with_capacity(misses.len() * 3);
        for u in &misses {
            ids.push(Value::String(u.clone()));
            if let Some((t, k)) = split_record_id(u) {
                ids.push(Value::String(format!("{t}:⟨{k}⟩")));
                ids.push(Value::String(format!("{t}:`{k}`")));
            }
        }
        let results = self
            .db
            .query(SELECT_SUBSCRIPTIONS, &[("ids", Value::Array(ids))])
            .await
            .map_err(|e| format!("reading subscriptions: {e}"))?;
        let mut results = results.into_iter();
        let _ = results.next();
        let subs: Vec<Subscription> = rows(results.next().into_iter().collect())
            .iter()
            .filter_map(parse_subscription)
            .collect();
        let owners = rows(results.next().into_iter().collect());

        // Newest owner of each endpoint wins; ties broken by id so every host
        // agrees.
        let mut winners: HashMap<String, (i64, String)> = HashMap::new();
        for o in &owners {
            let (Some(endpoint), Some(id)) = (
                o.get("endpoint").and_then(Value::as_str),
                o.get("id").and_then(Value::as_str),
            ) else {
                continue;
            };
            let at = o.get("updated_at").and_then(epoch_millis).unwrap_or(0);
            let entry = winners
                .entry(endpoint.to_string())
                .or_insert((at, id.to_string()));
            if (at, id) > (entry.0, entry.1.as_str()) {
                *entry = (at, id.to_string());
            }
        }
        let mut losers: Vec<(Value, Option<String>)> = Vec::new();
        for o in &owners {
            let (Some(endpoint), Some(id)) = (
                o.get("endpoint").and_then(Value::as_str),
                o.get("id").and_then(Value::as_str),
            ) else {
                continue;
            };
            if winners.get(endpoint).is_some_and(|(_, w)| w != id) {
                if let Some((_, key)) = record_binding(id) {
                    let user = o
                        .get("auth_id")
                        .and_then(Value::as_str)
                        .and_then(canonical_record_id);
                    losers.push((key, user));
                }
            }
        }

        let mut by_user: HashMap<String, Vec<Subscription>> =
            misses.iter().map(|u| (u.clone(), Vec::new())).collect();
        for s in subs {
            // A row absent from `owners` (it cannot be, but a racing write
            // could make it so) counts as its endpoint's only owner.
            let is_winner = winners.get(&s.endpoint).is_none_or(|(_, w)| *w == s.id);
            if !is_winner {
                continue;
            }
            if let Some(list) = by_user.get_mut(&s.user) {
                list.push(s);
            }
        }

        if !losers.is_empty() {
            let keys: Vec<Value> = losers.iter().map(|(k, _)| k.clone()).collect();
            debug!(target: "push", count = keys.len(), "deleting subscriptions of an endpoint's previous owners");
            if let Err(e) = self
                .db
                .query("FOR $k IN $keys { DELETE type::record('_00_push_subscription', $k) RETURN NONE };", &[("keys", Value::Array(keys))])
                .await
            {
                warn!(target: "push", error = %e, "could not delete stale shared-endpoint subscriptions");
            }
        }

        let mut st = self.state();
        for (_, user) in &losers {
            if let Some(user) = user {
                st.sub_cache.remove(user);
            }
        }
        for (user, list) in by_user {
            let list = Arc::new(list);
            st.sub_cache.insert(user.clone(), (now, Arc::clone(&list)));
            out.insert(user, list);
        }
        Ok(out)
    }

    /// Send deliveries to their users' subscriptions. Returns what happened
    /// per `_00_push_message`.
    async fn deliver(&self, deliveries: Vec<Delivery>, fresh: bool) -> HashMap<String, MsgTally> {
        let mut tallies: HashMap<String, MsgTally> = HashMap::new();
        if deliveries.is_empty() {
            return tallies;
        }
        let mut users: Vec<String> = deliveries.iter().map(|d| d.user.clone()).collect();
        users.sort();
        users.dedup();
        let subs = match self.subscriptions(&users, fresh).await {
            Ok(s) => s,
            Err(e) => {
                warn!(target: "push", error = %e, "push not delivered");
                self.set_error(e.clone());
                let now = self.now();
                let mut st = self.state();
                for d in &deliveries {
                    st.count(now, |c| c.failed += 1);
                    if let Some(m) = &d.message {
                        tallies.entry(m.clone()).or_default().error = Some(e.clone());
                    }
                }
                return tallies;
            }
        };
        let kid = self.keys.as_ref().map(|k| k.kid());
        let providers = Arc::clone(&self.loaded().providers);
        let mut jobs = Vec::new();
        let mut no_provider = 0u64;
        for d in deliveries {
            let Some(list) = subs.get(&d.user) else {
                continue;
            };
            for s in list.iter() {
                if d.platforms.as_ref().is_some_and(|p| !p.contains(&s.platform)) {
                    continue;
                }
                let usable = match s.kind {
                    // A row made under another VAPID key is rejected by the
                    // push service; the browser re-subscribes on its next boot.
                    DeviceKind::Web => kid == Some(s.kid.as_str()) && d.push.web.is_some(),
                    DeviceKind::Apns => providers.apns().is_some(),
                    DeviceKind::Fcm => providers.fcm().is_some(),
                };
                if !usable {
                    if s.kind != DeviceKind::Web {
                        no_provider += 1;
                        if let Some(m) = &d.message {
                            let why = format!("push.{} is not configured", if s.kind == DeviceKind::Apns { "apns" } else { "fcm" });
                            tallies.entry(m.clone()).or_default().skipped.get_or_insert(why);
                        }
                    }
                    continue;
                }
                if let (Some(rule), Some(allowed)) = (&d.rule, &s.rules) {
                    if !allowed.iter().any(|r| r == rule) {
                        continue;
                    }
                }
                jobs.push(SendJob {
                    sub: s.clone(),
                    delivery: d.clone(),
                    attempt: 0,
                });
            }
        }
        if no_provider > 0 {
            self.count(|c| c.no_provider += no_provider);
        }
        self.run_jobs(jobs, &mut tallies).await;
        tallies
    }

    async fn run_jobs(&self, jobs: Vec<SendJob>, tallies: &mut HashMap<String, MsgTally>) {
        if jobs.is_empty() {
            return;
        }
        let concurrency = self.opts.send_concurrency.max(1);
        let results: Vec<(SendJob, Outcome)> = stream::iter(jobs)
            .map(|job| async move {
                let outcome = self.send(&job).await;
                (job, outcome)
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;
        let book = self.settle(results, tallies);
        self.write_book(book).await;
    }

    async fn send(&self, job: &SendJob) -> Outcome {
        match job.sub.kind {
            DeviceKind::Web => self.send_web(job).await,
            DeviceKind::Apns => self.send_apns(job).await,
            DeviceKind::Fcm => self.send_fcm(job).await,
        }
    }

    /// One token of the project-wide `limits.perMinute` bucket.
    fn take_global(&self) -> bool {
        let now = self.now();
        let cap = self.loaded().config.limits.per_minute();
        let mut st = self.state();
        let bucket = st
            .global_bucket
            .get_or_insert_with(|| Bucket::full(cap, now));
        if bucket.take(cap, now) {
            true
        } else {
            st.limit_warn(now, "limits.perMinute");
            false
        }
    }

    async fn send_web(&self, job: &SendJob) -> Outcome {
        let Some(keys) = &self.keys else {
            return Outcome::Dropped("push is off".into());
        };
        let Some(plaintext) = job.delivery.push.web.as_deref() else {
            return Outcome::GiveUp("payload cannot be encoded for web push".into());
        };
        if let Err(why) = endpoint_allowed(&job.sub.endpoint, self.opts.allow_private_endpoints) {
            return Outcome::Rejected(format!("bad_endpoint: {why}"));
        }
        if !self.take_global() {
            return Outcome::Dropped("rate_limited".into());
        }
        let now = self.now();
        let cfg = self.loaded().config.clone();
        let body = match ece::encrypt(plaintext, &job.sub.p256dh, &job.sub.auth) {
            Ok(b) => b,
            Err(e) => return Outcome::Rejected(format!("bad_keys: {e}")),
        };
        let Some(authorization) = self.authorization(keys, &job.sub.endpoint, cfg.subject(), now)
        else {
            return Outcome::Rejected("bad_endpoint: not an http(s) URL".into());
        };
        let d = &job.delivery;
        let mut headers = vec![
            ("TTL".to_string(), d.ttl.to_string()),
            ("Urgency".to_string(), d.urgency.as_header().to_string()),
            ("Content-Encoding".to_string(), "aes128gcm".to_string()),
            (
                "Content-Type".to_string(),
                "application/octet-stream".to_string(),
            ),
            ("Authorization".to_string(), authorization),
        ];
        if let Some(topic) = &d.topic {
            headers.push(("Topic".to_string(), rules::topic_header(topic)));
        }
        match self.http.post(&job.sub.endpoint, headers, body).await {
            Ok((status, text)) => match classify(status, &text) {
                // FCM answers 410 "unsubscribed or expired" for a subscription
                // made seconds ago (seen with Chrome, the same endpoint takes
                // pushes shortly after). Deleting it would break exactly the
                // "enable, then send a test push" flow, so a fresh one is
                // retried through the backoff and only a later push deletes it.
                Outcome::Gone(reason)
                    if job.sub.updated_ms.is_some_and(|t| {
                        self.now() as i64 - t < self.opts.fresh_subscription_grace_ms as i64
                    }) =>
                {
                    Outcome::Retry(reason)
                }
                other => other,
            },
            Err(e) => Outcome::Retry(format!("transport: {}", prefix(&e))),
        }
    }

    fn native_send<'a>(&self, d: &'a Delivery) -> native::Send<'a> {
        native::Send {
            push: &d.push.native,
            ttl_secs: d.ttl,
            urgency: d.urgency,
            topic: d.topic.as_deref(),
            now_ms: self.now(),
        }
    }

    async fn post_native(&self, req: native::Request) -> Result<(u16, String), Outcome> {
        self.http
            .post(&req.url, req.headers, req.body)
            .await
            .map_err(|e| Outcome::Retry(format!("transport: {}", prefix(&e))))
    }

    async fn send_apns(&self, job: &SendJob) -> Outcome {
        let loaded = self.loaded();
        let Some(apns) = loaded.providers.apns().cloned() else {
            return Outcome::Dropped("push.apns is not configured".into());
        };
        let sub = &job.sub;
        let Some(token) = sub.token.as_deref().filter(|t| native::valid_apns_token(t)) else {
            return Outcome::Rejected("bad_token: not an APNs device token".into());
        };
        let Some(topic) = sub.app_id.as_deref().filter(|a| valid_app_id(a)) else {
            return Outcome::Rejected("bad_app_id: an APNs device needs its bundle id".into());
        };
        // `fn::push::register` checks this too, but a record user can write
        // their row directly.
        if let Some(cfg) = &loaded.config.apns {
            if !cfg.bundle_ids.is_empty() && !cfg.bundle_ids.iter().any(|b| b == topic) {
                return Outcome::Rejected(format!("bad_app_id: `{topic}` is not in push.apns.bundleIds"));
            }
        }
        if !self.take_global() {
            return Outcome::Dropped("rate_limited".into());
        }
        let send = self.native_send(&job.delivery);
        let bearer = apns.bearer(send.now_ms);
        let sandbox = sub.environment.as_deref() == Some("sandbox");
        let request = |sandbox: bool| native::apns_request(&send, token, topic, sandbox, &bearer);
        let req = match request(sandbox) {
            Ok(r) => r,
            Err(e) => return Outcome::GiveUp(format!("payload: {e}")),
        };
        let answer = match self.post_native(req).await {
            Ok((status, body)) => native::classify_apns(status, &body),
            Err(o) => return o,
        };
        match answer {
            // Almost always a development build's token sent to production or
            // the reverse. Try the other host once and remember the answer.
            Answer::BadDeviceToken(reason) => {
                let Ok(req) = request(!sandbox) else {
                    return Outcome::Gone(reason);
                };
                match self.post_native(req).await {
                    Ok((status, body)) => match native::classify_apns(status, &body) {
                        Answer::Ok => Outcome::OkMoved(if sandbox { "production" } else { "sandbox" }.into()),
                        Answer::BadDeviceToken(_) => Outcome::Gone(reason),
                        other => self.native_outcome(other, || apns.forget_token()),
                    },
                    Err(o) => o,
                }
            }
            other => self.native_outcome(other, || apns.forget_token()),
        }
    }

    async fn send_fcm(&self, job: &SendJob) -> Outcome {
        let Some(fcm) = self.loaded().providers.fcm().cloned() else {
            return Outcome::Dropped("push.fcm is not configured".into());
        };
        let sub = &job.sub;
        let Some(token) = sub.token.as_deref().filter(|t| native::valid_fcm_token(t)) else {
            return Outcome::Rejected("bad_token: not an FCM registration token".into());
        };
        if !self.take_global() {
            return Outcome::Dropped("rate_limited".into());
        }
        let access = match self.fcm_access_token(&fcm).await {
            Ok(t) => t,
            Err(o) => return o,
        };
        let send = self.native_send(&job.delivery);
        let req = match native::fcm_request(&send, fcm.project_id(), token, sub.platform, &access) {
            Ok(r) => r,
            Err(e) => return Outcome::GiveUp(format!("payload: {e}")),
        };
        match self.post_native(req).await {
            Ok((status, body)) => self.native_outcome(native::classify_fcm(status, &body), || fcm.forget_token()),
            Err(o) => o,
        }
    }

    /// The cached FCM access token, or a fresh one. One exchange at a time;
    /// concurrent sends wait for it instead of each asking Google.
    async fn fcm_access_token(&self, fcm: &native::Fcm) -> Result<String, Outcome> {
        if let Some(t) = fcm.cached_token(self.now()) {
            return Ok(t);
        }
        let _one = fcm.refresh.lock().await;
        let now = self.now();
        if let Some(t) = fcm.cached_token(now) {
            return Ok(t);
        }
        let req = fcm.token_request(now / 1000);
        let (status, body) = self.post_native(req).await?;
        let fail = |why: String| {
            let msg = format!("fcm: access token: {why}");
            self.set_error(msg.clone());
            Outcome::Provider(msg)
        };
        if !(200..300).contains(&status) {
            return Err(fail(format!("http_{status}: {}", prefix(&body))));
        }
        fcm.accept_token(&body, now).map_err(fail)
    }

    fn native_outcome(&self, answer: Answer, forget_token: impl FnOnce()) -> Outcome {
        match answer {
            Answer::Ok => Outcome::Ok,
            Answer::Gone(r) | Answer::BadDeviceToken(r) => Outcome::Gone(r),
            Answer::Rejected(r) => Outcome::Rejected(r),
            Answer::GiveUp(r) => Outcome::GiveUp(r),
            Answer::Retry(r) => Outcome::Retry(r),
            Answer::Provider(r) => {
                forget_token();
                self.set_error(r.clone());
                Outcome::Provider(r)
            }
        }
    }

    /// Cached per (audience, subject), re-signed well before the 12 h expiry.
    fn authorization(
        &self,
        keys: &VapidKeys,
        endpoint: &str,
        subject: &str,
        now: u64,
    ) -> Option<String> {
        let aud = VapidKeys::audience_of(endpoint)?;
        let cache_key = format!("{aud}\n{subject}");
        let mut st = self.state();
        if let Some((header, at)) = st.jwt_cache.get(&cache_key) {
            if now < at + self.opts.jwt_refresh_ms {
                return Some(header.clone());
            }
        }
        let header = keys.authorization_for_audience(&aud, subject, now / 1000);
        st.jwt_cache.insert(cache_key, (header.clone(), now));
        Some(header)
    }

    /// Fold send outcomes into counters, the retry queue, message tallies and
    /// subscription writes.
    fn settle(
        &self,
        results: Vec<(SendJob, Outcome)>,
        tallies: &mut HashMap<String, MsgTally>,
    ) -> Book {
        let now = self.now();
        let mut book = Book::default();
        let mut st = self.state();
        for (job, outcome) in results {
            let mut tally = job
                .delivery
                .message
                .as_ref()
                .map(|m| tallies.entry(m.clone()).or_default());
            if let Some(t) = tally.as_deref_mut() {
                t.jobs += 1;
            }
            let sub = &job.sub;
            let outcome = match outcome {
                Outcome::OkMoved(environment) => {
                    info!(target: "push", sub = %sub.id, environment = %environment, "APNs token belongs to the other environment; row updated");
                    book.moved.push(json!({ "k": sub.key, "environment": environment }));
                    st.sub_cache.remove(&sub.user);
                    Outcome::Ok
                }
                other => other,
            };
            // A refused provider credential is retried like a transport error,
            // but it is not the device's fault: its row is left alone.
            let (outcome, device_fault) = match outcome {
                Outcome::Provider(reason) => (Outcome::Retry(reason), false),
                other => (other, true),
            };
            match outcome {
                Outcome::Ok => {
                    st.count(now, |c| c.sent += 1);
                    if let Some(t) = tally {
                        t.delivered += 1;
                    }
                    let stale = st
                        .last_ok_written
                        .get(&sub.id)
                        .is_none_or(|t| now >= t + self.opts.last_ok_interval_ms);
                    if sub.failures > 0 || stale {
                        book.ok.push(sub.key.clone());
                        st.last_ok_written.insert(sub.id.clone(), now);
                    }
                }
                Outcome::Gone(reason) => {
                    st.count(now, |c| c.failed += 1);
                    info!(target: "push", sub = %sub.id, reason = %reason, "push service says the subscription is gone; deleting it");
                    book.gone.push(sub.key.clone());
                    st.sub_cache.remove(&sub.user);
                    if let Some(t) = tally {
                        t.error = Some("subscription gone".into());
                    }
                }
                Outcome::Rejected(reason) => {
                    st.count(now, |c| c.failed += 1);
                    info!(target: "push", sub = %sub.id, reason = %reason, "push service rejected the subscription; disabling it");
                    book.disable.push(json!({ "k": sub.key, "reason": reason }));
                    st.sub_cache.remove(&sub.user);
                    if let Some(t) = tally {
                        t.error = Some(reason);
                    }
                }
                Outcome::GiveUp(reason) => {
                    st.count(now, |c| c.failed += 1);
                    warn!(target: "push", sub = %sub.id, reason = %reason, "push not accepted; giving up on it");
                    if let Some(t) = tally {
                        t.error = Some(reason);
                    }
                }
                Outcome::Dropped(reason) => {
                    st.count(now, |c| c.dropped += 1);
                    if let Some(t) = tally {
                        t.error = Some(reason);
                    }
                }
                Outcome::OkMoved(_) | Outcome::Provider(_) => unreachable!("mapped above"),
                Outcome::Retry(reason) => {
                    if device_fault {
                        book.fail.push(json!({ "k": sub.key, "error": reason }));
                    }
                    let backoff = self.opts.retry_backoff_ms.get(job.attempt).copied();
                    match backoff {
                        Some(delay) if st.retries.len() < self.opts.retry_queue_cap => {
                            debug!(target: "push", sub = %sub.id, attempt = job.attempt + 1, reason = %reason, "send failed; retrying");
                            st.count(now, |c| c.retried += 1);
                            if let Some(t) = tally {
                                t.retrying += 1;
                            }
                            let mut job = job;
                            job.attempt += 1;
                            st.retries.push(RetryItem {
                                job,
                                due_ms: now + delay,
                            });
                        }
                        Some(_) => {
                            st.count(now, |c| c.dropped += 1);
                            st.limit_warn(now, "retry queue full");
                            if let Some(t) = tally {
                                t.error = Some(reason);
                            }
                        }
                        None => {
                            st.count(now, |c| c.failed += 1);
                            info!(target: "push", sub = %sub.id, reason = %reason, "send failed after every retry");
                            if let Some(t) = tally {
                                t.error = Some(reason);
                            }
                        }
                    }
                }
            }
        }
        book
    }

    /// Subscription bookkeeping for one batch, one round-trip. Root writes.
    async fn write_book(&self, book: Book) {
        if book.is_empty() {
            return;
        }
        let mut sql = String::new();
        let mut binds: Vec<(&str, Value)> = Vec::new();
        if !book.ok.is_empty() {
            sql.push_str("FOR $k IN $ok { UPDATE type::record('_00_push_subscription', $k) SET last_ok_at = time::now(), failures = 0, last_error = NONE RETURN NONE };\n");
            binds.push(("ok", Value::Array(book.ok)));
        }
        if !book.gone.is_empty() {
            sql.push_str("FOR $k IN $gone { DELETE type::record('_00_push_subscription', $k) RETURN NONE };\n");
            binds.push(("gone", Value::Array(book.gone)));
        }
        if !book.disable.is_empty() {
            sql.push_str("FOR $d IN $disable { UPDATE type::record('_00_push_subscription', $d.k) SET disabled_at = time::now(), disabled_reason = $d.reason RETURN NONE };\n");
            binds.push(("disable", Value::Array(book.disable)));
        }
        if !book.fail.is_empty() {
            sql.push_str("FOR $f IN $fail { UPDATE type::record('_00_push_subscription', $f.k) SET failures += 1, last_error = $f.error RETURN NONE };\n");
            binds.push(("fail", Value::Array(book.fail)));
        }
        if !book.moved.is_empty() {
            sql.push_str("FOR $m IN $moved { UPDATE type::record('_00_push_subscription', $m.k) SET environment = $m.environment RETURN NONE };\n");
            binds.push(("moved", Value::Array(book.moved)));
        }
        if let Err(e) = self.db.query(&sql, &binds).await {
            warn!(target: "push", error = %e, "subscription bookkeeping failed");
            self.set_error(format!("subscription bookkeeping: {e}"));
        }
    }

    // ── Direct messages ──────────────────────────────────────────────────

    async fn observe_message(&self, change: ObservedChange) {
        if change.op != Op::Create || change.origin == Origin::Repair {
            return;
        }
        if !self.can_send() || !self.loaded().config.enabled {
            return;
        }
        let status = change.record.get("status").and_then(Value::as_str);
        if status.is_some_and(|s| s != "pending") {
            return;
        }
        if let Some(at) = change.record.get("send_at").and_then(epoch_millis) {
            if at > self.now() as i64 {
                debug!(target: "push", id = %change.id, "scheduled message; left for the sweep");
                return;
            }
        }
        // Under the http transport the ingest event fires INSIDE the creating
        // transaction, so the row is not visible to the claim yet. Retry while
        // it is absent (not while it is merely taken), 10 ms doubling to
        // 250 ms, instead of leaving it to the 30 s stale-pending sweep.
        let mut delay = 10u64;
        let mut waited = 0u64;
        loop {
            match self.process_message(&change.id).await {
                Claim::Missing if waited < self.opts.claim_visible_budget_ms => {
                    let Some(sleep) = self.opts.sleep.clone() else { break };
                    sleep(delay).await;
                    waited += delay;
                    delay = (delay * 2).min(250);
                }
                _ => break,
            }
        }
    }

    /// Claim one message (the CAS against other hosts and the sweep), send
    /// it, record the outcome.
    async fn process_message(&self, raw_id: &str) -> Claim {
        let Some((table, key)) = record_binding(raw_id) else {
            return Claim::Skipped;
        };
        if table != MESSAGE_TABLE {
            return Claim::Skipped;
        }
        let claimed = match self.db.query(CLAIM_MESSAGE, &[("k", key.clone())]).await {
            Ok(r) => first_row(r),
            Err(e) => {
                warn!(target: "push", id = %raw_id, error = %e, "could not claim message");
                self.set_error(format!("claiming {raw_id}: {e}"));
                return Claim::Skipped;
            }
        };
        let Some(row) = claimed else {
            // Absent, or pending and due (its transaction committed between
            // the claim and this read): both mean "try again". Anything else
            // was taken, cancelled or is not due.
            let state = match self.db.query(MESSAGE_STATE, &[("k", key.clone())]).await {
                Ok(r) => first_row(r).filter(|v| !v.is_null()),
                Err(_) => return Claim::Skipped,
            };
            let retry = match &state {
                None => true,
                Some(row) => {
                    row.get("status").and_then(Value::as_str) == Some("pending")
                        && row.get("due").and_then(Value::as_bool).unwrap_or(false)
                }
            };
            if retry {
                return Claim::Missing;
            }
            debug!(target: "push", id = %raw_id, "message not claimable (taken, cancelled or not due)");
            return Claim::Skipped;
        };
        let id = canonical_record_id(raw_id).unwrap_or_else(|| raw_id.to_string());
        let cfg = self.loaded().config.clone();
        let now = self.now();
        self.count(|c| c.messages += 1);

        let payload = rules::message_payload(&id, &row, &cfg.defaults, now);
        let (web, web_error) = match rules::encode_payload(&payload) {
            Ok(enc) => {
                if !enc.degraded.is_empty() {
                    info!(target: "push", id = %id, dropped = ?enc.degraded, "message payload over 3993 bytes; trimmed");
                }
                (Some(enc.bytes), None)
            }
            Err(e) => (None, Some(format!("payload: {e}"))),
        };
        let native = native::message_native(&row, &cfg.defaults, &payload, now);
        let push = Arc::new(Rendered { web, native });
        let ttl = row
            .get("ttl")
            .and_then(Value::as_u64)
            .or_else(|| cfg.defaults.ttl.as_ref().map(|d| d.as_secs()))
            .unwrap_or(self.opts.default_ttl_secs);
        let urgency = row
            .get("urgency")
            .and_then(Value::as_str)
            .and_then(Urgency::parse)
            .or(cfg.defaults.urgency)
            .unwrap_or(Urgency::Normal);
        let topic = payload.topic.clone();
        let mut ids = Vec::new();
        if let Some(to) = row.get("to") {
            rules::collect_ids(to, &mut ids);
        }
        let (recipients, _) =
            rules::finalize_recipients(ids, &[], cfg.limits.max_recipients() as usize);
        if recipients.is_empty() {
            self.finish_message(&key, 0, Some("no recipients".into()))
                .await;
            return Claim::Skipped;
        }

        let per_user = cfg.limits.per_user_per_minute();
        let mut admitted = Vec::new();
        let mut limited = 0;
        {
            let mut st = self.state();
            for user in recipients {
                if Self::take_user(&mut st, &user, per_user, now) {
                    admitted.push(Delivery {
                        user,
                        seq: 0,
                        push: Arc::clone(&push),
                        ttl,
                        urgency,
                        topic: topic.clone(),
                        rule: None,
                        platforms: None,
                        message: Some(id.clone()),
                    });
                } else {
                    limited += 1;
                    st.count(now, |c| c.dropped += 1);
                    st.limit_warn(now, "limits.perUserPerMinute");
                }
            }
        }
        let tallies = self.deliver(admitted, true).await;
        let t = tallies.get(&id).cloned().unwrap_or_default();
        let error = t.error.clone().or_else(|| {
            if t.delivered > 0 {
                None
            } else if limited > 0 {
                Some("rate_limited".into())
            } else if t.jobs == 0 {
                Some(t.skipped.clone().or(web_error).unwrap_or_else(|| "no subscriptions".into()))
            } else {
                None
            }
        });
        if t.retrying == 0 {
            self.finish_message(&key, t.delivered, error).await;
        } else {
            self.state().messages.insert(
                id,
                MsgProgress {
                    key,
                    delivered: t.delivered,
                    outstanding: t.retrying,
                    error,
                },
            );
        }
        Claim::Skipped
    }

    async fn finish_message(&self, key: &Value, delivered: u32, error: Option<String>) {
        let status = if delivered > 0 { "sent" } else { "failed" };
        let binds = [
            ("k", key.clone()),
            ("status", Value::String(status.into())),
            ("delivered", Value::from(delivered)),
            ("error", error.map(Value::String).unwrap_or(Value::Null)),
        ];
        if let Err(e) = self.db.query(FINISH_MESSAGE, &binds).await {
            warn!(target: "push", key = %key, error = %e, "could not record the message outcome");
            self.set_error(format!("recording message outcome: {e}"));
        }
    }

    // ── Periodic work ────────────────────────────────────────────────────

    /// Periodic work, call every ~1 s: publish VAPID params until done, reload
    /// config every 30 s (hash short-circuit), flush due trailing throttles,
    /// retry failed sends, send due and missed messages every 5 s, prune every
    /// hour. Overlapping calls return immediately.
    pub async fn tick(&self) {
        if self.ticking.swap(true, Ordering::SeqCst) {
            return;
        }
        let _guard = TickGuard(&self.ticking);
        let now = self.now();
        let due = |last: Option<u64>, every: u64| last.is_none_or(|t| now >= t + every);

        let (config_due, vapid_due, sweep_due, prune_due) = {
            let mut st = self.state();
            let config_due = due(st.last_config_ms, self.opts.config_reload_ms);
            if config_due {
                st.last_config_ms = Some(now);
            }
            let vapid_due = self.keys.is_some()
                && (!st.vapid_published || due(st.last_vapid_check_ms, self.opts.vapid_recheck_ms));
            if vapid_due {
                st.last_vapid_check_ms = Some(now);
            }
            let sweep_due = due(st.last_sweep_ms, self.opts.sweep_ms);
            if sweep_due {
                st.last_sweep_ms = Some(now);
            }
            let prune_due = due(st.last_prune_ms, self.opts.prune_ms);
            if prune_due {
                st.last_prune_ms = Some(now);
            }
            (config_due, vapid_due, sweep_due, prune_due)
        };

        if config_due && self.reload_config().await.is_err() && self.loaded().hash.is_none() {
            // Never loaded yet (DB still coming up at boot): try again next
            // tick instead of running on the default config for 30 s.
            self.state().last_config_ms = None;
        }
        if vapid_due {
            self.publish_vapid().await;
        }
        let active = self.can_send() && self.loaded().config.enabled;
        if active {
            self.flush_trailing(now).await;
            self.run_retries(now).await;
            if sweep_due {
                self.sweep_messages().await;
            }
        } else {
            self.drop_queued(now);
        }
        if prune_due {
            self.prune().await;
        }
        self.gc(now);
    }

    /// Re-read `_00_push_config:default` and `_00_push_credential`. `Ok(true)`
    /// when a different config or credential set is now active. A config row
    /// that does not parse keeps the previous config; a credential that does
    /// not parse turns only its provider off.
    pub async fn reload_config(&self) -> Result<bool, String> {
        let row = match self.db.query(SELECT_CONFIG, &[]).await {
            Ok(r) => first_row(r),
            Err(e) => {
                let msg = format!("reading _00_push_config: {e}");
                debug!(target: "push", "{msg}");
                self.set_error(msg.clone());
                return Err(msg);
            }
        };
        let credential_rows = match self.db.query(SELECT_CREDENTIALS, &[]).await {
            Ok(r) => rows(r),
            Err(e) => {
                // Older schema without the table: no native providers.
                debug!(target: "push", error = %e, "reading _00_push_credential failed; no native providers");
                Vec::new()
            }
        };
        let credentials = Providers::fingerprint(&credential_rows);
        let (spec, hash) = match &row {
            Some(r) => (
                r.get("spec_json")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                r.get("hash")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            ),
            None => (None, String::new()),
        };
        let current = self.loaded();
        let same_config = current.hash.as_deref() == Some(hash.as_str());
        let same_credentials = current.hash.is_some() && current.credentials == credentials;
        if same_config && same_credentials {
            return Ok(false);
        }
        let config = if same_config {
            (*current.config).clone()
        } else {
            match spec {
                None => PushConfig::default(),
                Some(spec) => match serde_json::from_str::<PushConfig>(&spec) {
                    Ok(c) => c,
                    Err(e) => {
                        let msg = format!("_00_push_config (hash {hash}) does not parse: {e}; keeping the previous config");
                        let mut st = self.state();
                        if st.bad_config_hash.as_deref() != Some(hash.as_str()) {
                            warn!(target: "push", "{msg}");
                            st.bad_config_hash = Some(hash);
                        }
                        st.last_error = Some(msg.clone());
                        return Err(msg);
                    }
                },
            }
        };
        let providers = if same_credentials {
            Arc::clone(&current.providers)
        } else {
            let p = Providers::from_rows(&credential_rows);
            for (name, state) in [("apns", ProviderState::of(&p.apns)), ("fcm", ProviderState::of(&p.fcm))] {
                match (state.ready, &state.error) {
                    (true, _) => info!(target: "push", provider = name, "native push credential loaded"),
                    (false, Some(e)) => {
                        warn!(target: "push", provider = name, error = %e, "native push credential unusable");
                        self.set_error(format!("push.{name}: {e}"));
                    }
                    _ => {}
                }
            }
            Arc::new(p)
        };
        let loaded = Arc::new(Loaded::new(config, Some(hash.clone()), providers, credentials));
        let (rules_count, enabled) = (loaded.config.rules.len(), loaded.config.enabled);
        match self.loaded.write() {
            Ok(mut guard) => *guard = loaded,
            Err(poisoned) => *poisoned.into_inner() = loaded,
        }
        info!(target: "push", rules = rules_count, enabled, hash = %hash, "push config loaded");
        Ok(true)
    }

    /// `$sp00ky_vapid_public_key` / `$sp00ky_vapid_kid` for `fn::push::info()`.
    /// Only written when they differ, so several hosts do not fight.
    async fn publish_vapid(&self) {
        let Some(keys) = &self.keys else { return };
        let (public, kid) = (keys.public_key_b64url(), keys.kid());
        let current = self
            .db
            .query("RETURN [$sp00ky_vapid_public_key, $sp00ky_vapid_kid];", &[])
            .await;
        if let Ok(r) = &current {
            if r.first() == Some(&json!([public, kid])) {
                self.state().vapid_published = true;
                return;
            }
        }
        // Inlined, not bound: DEFINE PARAM takes a literal. Both values are
        // base64url / hex, so there is nothing to escape.
        let safe = |s: &str| {
            s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        };
        if !safe(public) || !safe(kid) {
            self.set_error("VAPID key has unexpected characters");
            return;
        }
        let sql = format!(
            "DEFINE PARAM OVERWRITE $sp00ky_vapid_public_key VALUE '{public}' PERMISSIONS FULL; \
             DEFINE PARAM OVERWRITE $sp00ky_vapid_kid VALUE '{kid}' PERMISSIONS FULL;"
        );
        match self.db.query(&sql, &[]).await {
            Ok(_) => {
                info!(target: "push", kid = %kid, "published the VAPID public key");
                self.state().vapid_published = true;
            }
            Err(e) => {
                debug!(target: "push", error = %e, "publishing the VAPID key failed; retrying next tick");
                let mut st = self.state();
                st.vapid_published = false;
                st.last_error = Some(format!("publishing the VAPID key: {e}"));
            }
        }
    }

    async fn flush_trailing(&self, now: u64) {
        let cfg = self.loaded().config.clone();
        let per_user = cfg.limits.per_user_per_minute();
        let mut due = Vec::new();
        {
            let mut st = self.state();
            let mut ready = Vec::new();
            for (key, slot) in st.throttles.iter_mut() {
                let open = slot.last_sent_ms.is_none_or(|t| now >= t + slot.gap_ms);
                if open {
                    if let Some(d) = slot.pending.take() {
                        slot.last_sent_ms = Some(now);
                        slot.last_sent_seq = slot.last_sent_seq.max(d.seq);
                        ready.push((key.clone(), d));
                    }
                }
            }
            for (_, d) in ready {
                if Self::take_user(&mut st, &d.user, per_user, now) {
                    due.push(d);
                } else {
                    st.count(now, |c| c.dropped += 1);
                    st.limit_warn(now, "limits.perUserPerMinute");
                }
            }
            st.throttles.retain(|_, s| {
                s.pending.is_some() || s.last_sent_ms.is_some_and(|t| now < t + s.gap_ms)
            });
        }
        if !due.is_empty() {
            debug!(target: "push", count = due.len(), "sending trailing throttled pushes");
            self.deliver(due, false).await;
        }
    }

    async fn run_retries(&self, now: u64) {
        let due: Vec<SendJob> = {
            let mut st = self.state();
            let (due, keep): (Vec<RetryItem>, Vec<RetryItem>) = std::mem::take(&mut st.retries)
                .into_iter()
                .partition(|r| r.due_ms <= now);
            st.retries = keep;
            due.into_iter().map(|r| r.job).collect()
        };
        if due.is_empty() {
            return;
        }
        let mut tallies = HashMap::new();
        self.run_jobs(due, &mut tallies).await;
        self.resolve_messages(tallies).await;
    }

    /// Fold retry results into waiting messages; write the ones that are done.
    async fn resolve_messages(&self, tallies: HashMap<String, MsgTally>) {
        let mut done = Vec::new();
        {
            let mut st = self.state();
            for (id, t) in tallies {
                let Some(p) = st.messages.get_mut(&id) else {
                    continue;
                };
                p.delivered += t.delivered;
                p.outstanding = (p.outstanding + t.retrying).saturating_sub(t.jobs);
                if t.error.is_some() {
                    p.error = t.error;
                }
                if p.outstanding == 0 {
                    if let Some(p) = st.messages.remove(&id) {
                        done.push(p);
                    }
                }
            }
        }
        for p in done {
            self.finish_message(&p.key, p.delivered, p.error).await;
        }
    }

    /// Messages the ingest path did not deliver: scheduled ones now due,
    /// pending ones whose ingest was missed (host restarting, engine not
    /// built yet, a failed http event), and ones stuck in `sending` because
    /// their host died.
    async fn sweep_messages(&self) {
        let reset = format!(
            "UPDATE _00_push_message SET status = 'pending', claimed_at = NONE \
             WHERE status = 'sending' AND (claimed_at ?? created_at) < time::now() - {}ms RETURN NONE;",
            self.opts.stuck_sending_ms
        );
        if let Err(e) = self.db.query(&reset, &[]).await {
            debug!(target: "push", error = %e, "resetting stuck messages failed");
        }
        let select = format!(
            "SELECT VALUE id FROM _00_push_message WHERE status = 'pending' AND (\
             (send_at != NONE AND send_at <= time::now()) OR \
             (send_at = NONE AND created_at < time::now() - {}ms)) LIMIT {};",
            self.opts.stale_pending_ms,
            self.opts.sweep_batch.max(1)
        );
        let ids: Vec<String> = match self.db.query(&select, &[]).await {
            Ok(r) => rows(r)
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            Err(e) => {
                warn!(target: "push", error = %e, "message sweep failed");
                self.set_error(format!("message sweep: {e}"));
                return;
            }
        };
        if ids.is_empty() {
            return;
        }
        debug!(target: "push", count = ids.len(), "sweeping due messages");
        let concurrency = self.opts.send_concurrency.max(1);
        stream::iter(ids)
            .for_each_concurrent(
                concurrency,
                |id| async move {
                    self.process_message(&id).await;
                },
            )
            .await;
    }

    async fn prune(&self) {
        let sql = format!(
            "DELETE _00_push_message WHERE status IN ['sent', 'failed', 'cancelled'] \
             AND (sent_at ?? created_at) < time::now() - {}ms RETURN NONE; \
             DELETE _00_push_subscription WHERE disabled_at != NONE \
             AND disabled_at < time::now() - {}ms RETURN NONE;",
            self.opts.message_retention_ms, self.opts.disabled_retention_ms
        );
        if let Err(e) = self.db.query(&sql, &[]).await {
            debug!(target: "push", error = %e, "prune failed");
        }
    }

    /// Push is off (config or keys): queued work must not go out later when
    /// it comes back on.
    fn drop_queued(&self, now: u64) {
        let mut st = self.state();
        let dropped = st.retries.len()
            + st.throttles
                .values()
                .filter(|s| s.pending.is_some())
                .count();
        if dropped > 0 {
            st.retries.clear();
            st.throttles.clear();
            st.count(now, |c| c.dropped += dropped as u64);
        }
    }

    fn gc(&self, now: u64) {
        let mut st = self.state();
        let cache_ms = self.opts.subscription_cache_ms;
        st.sub_cache.retain(|_, (at, _)| now < *at + cache_ms);
        // A bucket untouched for a minute is full again; forget it.
        st.user_buckets.retain(|_, b| now < b.updated_ms + MINUTE);
        let ok_ms = self.opts.last_ok_interval_ms;
        st.last_ok_written.retain(|_, t| now < *t + ok_ms);
        let jwt_ms = self.opts.jwt_refresh_ms;
        st.jwt_cache.retain(|_, (_, at)| now < *at + jwt_ms);
    }

    /// For /health, /metrics, admin overview.
    pub fn status(&self) -> PushStatus {
        let loaded = self.loaded();
        let cfg = &loaded.config;
        let now = self.now();
        let st = self.state();
        let providers = &loaded.providers;
        let reason = if self.opts.off {
            Some("switched off (SPKY_PUSH=off)".to_string())
        } else if self.keys.is_none() && !providers.any() {
            Some("no VAPID key (set SPKY_AUTH_SECRET or SPKY_VAPID_PRIVATE_KEY) and no usable push.apns / push.fcm credential".to_string())
        } else if !cfg.enabled {
            Some("disabled by push.enabled: false".to_string())
        } else {
            None
        };
        PushStatus {
            enabled: reason.is_none(),
            reason,
            kid: self.keys.as_ref().map(|k| k.kid().to_string()),
            public_key: self
                .keys
                .as_ref()
                .map(|k| k.public_key_b64url().to_string()),
            subject: cfg.subject().to_string(),
            rules: cfg.rules.len(),
            rule_names: cfg.rules.keys().cloned().collect(),
            config_hash: loaded.hash.clone().filter(|h| !h.is_empty()),
            config_loaded: loaded.hash.is_some(),
            vapid_published: st.vapid_published,
            totals: st.totals,
            last_minute: st.window.sum(now),
            queues: PushQueues {
                retry: st.retries.len(),
                trailing: st
                    .throttles
                    .values()
                    .filter(|s| s.pending.is_some())
                    .count(),
                throttle_slots: st.throttles.len(),
                messages_in_flight: st.messages.len(),
                dedupe_entries: st.dedupe.len(),
                cached_users: st.sub_cache.len(),
            },
            last_error: st.last_error.clone(),
            providers: ProviderStatus {
                web: self.keys.is_some(),
                apns: ProviderState::of(&providers.apns),
                fcm: ProviderState::of(&providers.fcm),
            },
        }
    }
}

/// Whether the engine may POST to a subscription endpoint.
///
/// Endpoints come from record users, and the engine runs inside the
/// platform's network: without a check, anyone could make it POST to an
/// internal address and read the answer back through `last_error` /
/// `disabled_reason` in `fn::push::list()`. Browser push services are always
/// public `https` hosts, so everything else is refused. Names that resolve to
/// private addresses are the host HTTP client's to refuse.
pub fn endpoint_allowed(endpoint: &str, allow_private: bool) -> Result<(), String> {
    let Some(origin) = VapidKeys::audience_of(endpoint) else {
        return Err("not an http(s) URL".into());
    };
    if allow_private {
        return Ok(());
    }
    let Some(host) = origin.strip_prefix("https://") else {
        return Err("push endpoints must be https".into());
    };
    // `[v6]` literals are refused whole; otherwise drop a `:port`.
    let is_v6 = host.starts_with('[');
    let host = host.split(':').next().unwrap_or(host).trim_end_matches('.');
    let is_ip = is_v6 || host.parse::<std::net::Ipv4Addr>().is_ok();
    let local = host == "localhost"
        || !host.contains('.')
        || [".localhost", ".local", ".internal", ".lan", ".home.arpa"]
            .iter()
            .any(|suffix| host.ends_with(suffix));
    if is_ip || local {
        return Err(format!("`{host}` is not a public push service host"));
    }
    Ok(())
}

fn prefix(s: &str) -> String {
    let t: String = s.chars().take(200).collect();
    t.trim().to_string()
}

/// RFC 8030 / push-service answers to what the engine does next.
fn classify(status: u16, body: &str) -> Outcome {
    let reason = || {
        let p = prefix(body);
        if p.is_empty() {
            format!("http_{status}")
        } else {
            format!("http_{status}: {p}")
        }
    };
    match status {
        200..=299 => Outcome::Ok,
        404 | 410 => Outcome::Gone(reason()),
        400 | 401 | 403 => Outcome::Rejected(reason()),
        429 | 500..=599 => Outcome::Retry(reason()),
        _ => Outcome::GiveUp(reason()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_follows_the_contract() {
        assert_eq!(classify(201, ""), Outcome::Ok);
        assert_eq!(classify(404, ""), Outcome::Gone("http_404".into()));
        assert_eq!(classify(410, "expired"), Outcome::Gone("http_410: expired".into()));
        assert_eq!(
            classify(403, "bad jwt"),
            Outcome::Rejected("http_403: bad jwt".into())
        );
        assert_eq!(classify(400, ""), Outcome::Rejected("http_400".into()));
        assert_eq!(classify(413, ""), Outcome::GiveUp("http_413".into()));
        assert_eq!(classify(429, ""), Outcome::Retry("http_429".into()));
        assert_eq!(classify(503, ""), Outcome::Retry("http_503".into()));
        assert!(matches!(classify(302, ""), Outcome::GiveUp(_)));
    }

    #[test]
    fn only_public_https_endpoints_are_allowed() {
        for ok in [
            "https://fcm.googleapis.com/fcm/send/abc",
            "https://updates.push.services.mozilla.com/wpush/v2/x",
            "https://web.push.apple.com:443/Q",
            "https://wns2-par02p.notify.windows.com/w/?token=x",
        ] {
            assert_eq!(endpoint_allowed(ok, false), Ok(()), "{ok}");
        }
        for bad in [
            "http://fcm.googleapis.com/x",
            "https://localhost/x",
            "https://127.0.0.1/x",
            "https://10.0.0.8:8443/x",
            "https://[::1]/x",
            "https://metadata/computeMetadata",
            "https://redis.internal/x",
            "https://printer.local./x",
            "ftp://x.example.com",
            "not a url",
        ] {
            assert!(endpoint_allowed(bad, false).is_err(), "{bad}");
        }
        assert_eq!(endpoint_allowed("http://localhost:8080/x", true), Ok(()));
        assert!(endpoint_allowed("not a url", true).is_err());
    }

    #[test]
    fn buckets_refill_per_minute() {
        let mut b = Bucket::full(60, 0);
        for _ in 0..60 {
            assert!(b.take(60, 0));
        }
        assert!(!b.take(60, 0));
        assert!(!b.take(60, 500));
        assert!(b.take(60, 1_000));
        assert!(!b.take(60, 1_000));
        assert!(b.take(60, 120_000));
    }

    #[test]
    fn window_counts_the_last_minute_only() {
        let mut w = Window::new();
        w.bucket(1_000).sent += 1;
        w.bucket(30_000).sent += 2;
        assert_eq!(w.sum(30_000).sent, 3);
        assert_eq!(w.sum(61_500).sent, 2);
        w.bucket(61_000).sent += 5;
        assert_eq!(w.sum(61_500).sent, 7);
        assert_eq!(w.sum(200_000).sent, 0);
    }

    #[test]
    fn keys_from_env() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert!(PushEngine::keys_from_env(env(&[])).is_none());
        let derived = PushEngine::keys_from_env(env(&[("SPKY_AUTH_SECRET", "s3cret")])).unwrap();
        assert_eq!(
            derived.kid(),
            VapidKeys::from_secret("s3cret").unwrap().kid()
        );
        assert!(PushEngine::keys_from_env(env(&[
            ("SPKY_AUTH_SECRET", "s3cret"),
            ("SPKY_PUSH", "off")
        ]))
        .is_none());
        let other = VapidKeys::from_secret("other")
            .unwrap()
            .private_key_b64url();
        let leaked: &'static str = Box::leak(other.into_boxed_str());
        let pairs: &'static [(&'static str, &'static str)] = Box::leak(
            vec![
                ("SPKY_AUTH_SECRET", "s3cret"),
                ("SPKY_VAPID_PRIVATE_KEY", leaked),
            ]
            .into_boxed_slice(),
        );
        let explicit = PushEngine::keys_from_env(env(pairs)).unwrap();
        assert_eq!(
            explicit.kid(),
            VapidKeys::from_secret("other").unwrap().kid()
        );
        assert!(PushEngine::keys_from_env(env(&[
            ("SPKY_AUTH_SECRET", "s3cret"),
            ("SPKY_VAPID_PRIVATE_KEY", "junk")
        ]))
        .is_none());
    }

    #[test]
    fn options_from_env() {
        let o = EngineOptions::from_env(|k| match k {
            "SPKY_PUSH_DEDUPE_CAPACITY" => Some("50".into()),
            "SPKY_PUSH_CONFIG_RELOAD_SECS" => Some("10".into()),
            "SPKY_PUSH_SEND_CONCURRENCY" => Some("zero".into()),
            _ => None,
        });
        assert_eq!(o.dedupe_capacity, 50);
        assert_eq!(o.config_reload_ms, 10_000);
        assert_eq!(
            o.send_concurrency,
            EngineOptions::default().send_concurrency
        );
    }
}
