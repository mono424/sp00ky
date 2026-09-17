use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Connection settings for the main SurrealDB.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DbConfig {
    pub url: String,
    pub namespace: String,
    pub database: String,
    pub username: String,
    pub password: String,
}

/// Split a DB URL into (address, secure). Accepts ws://, wss://, http://,
/// https:// and bare host:port so legacy `SPKY_DB_WS` values keep working.
pub fn normalize_url(url: &str) -> (&str, bool) {
    if let Some(rest) = url.strip_prefix("wss://") {
        (rest, true)
    } else if let Some(rest) = url.strip_prefix("ws://") {
        (rest, false)
    } else if let Some(rest) = url.strip_prefix("https://") {
        (rest, true)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (rest, false)
    } else {
        (url, false)
    }
}

/// Open a fresh HTTP connection to the main SurrealDB and sign in as root,
/// WITHOUT selecting a namespace/database. Callers that need to DEFINE the
/// namespace/database before selecting it (self-heal on a brand-new SurrealDB)
/// use this and select afterwards.
///
/// We use the HTTP engine (not WS) because `Surreal::import()` / `Surreal::export()`
/// are only implemented for HTTP and local storage engines. Calling `.import()` on
/// a WebSocket client returns `BackupsNotSupported`.
pub async fn connect_http_raw(
    db_config: &DbConfig,
) -> Result<surrealdb::Surreal<surrealdb::engine::remote::http::Client>> {
    Ok(open_http(db_config).await?.0)
}

/// [`connect_http_raw`], plus how long the root token it signed in with lives
/// (`None` when the token could not be read).
async fn open_http(db_config: &DbConfig) -> Result<(HttpDb, Option<std::time::Duration>)> {
    let (addr, secure) = normalize_url(&db_config.url);

    let db = if secure {
        surrealdb::Surreal::new::<surrealdb::engine::remote::http::Https>(addr)
            .await
            .with_context(|| format!("Failed to open HTTPS to {}", db_config.url))?
    } else {
        surrealdb::Surreal::new::<surrealdb::engine::remote::http::Http>(addr)
            .await
            .with_context(|| format!("Failed to open HTTP to {}", db_config.url))?
    };

    let token = db
        .signin(surrealdb::opt::auth::Root {
            username: db_config.username.clone(),
            password: db_config.password.clone(),
        })
        .await
        .context("Remote SurrealDB signin failed")?;

    Ok((db, token_lifetime(token.access.as_insecure_token())))
}

/// Open a fresh HTTP connection to the main SurrealDB: root signin plus
/// namespace/database selection.
pub async fn connect_http(
    db_config: &DbConfig,
) -> Result<surrealdb::Surreal<surrealdb::engine::remote::http::Client>> {
    Ok(open_http_selected(db_config).await?.0)
}

async fn open_http_selected(db_config: &DbConfig) -> Result<(HttpDb, Option<std::time::Duration>)> {
    let (db, token_life) = open_http(db_config).await?;

    db.use_ns(&db_config.namespace)
        .use_db(&db_config.database)
        .await
        .context("Failed to select remote namespace/database")?;

    Ok((db, token_life))
}

/// How long a JWT is valid (`exp - iat`), read from its payload WITHOUT
/// verifying it: the server just issued it to us, and the only use is deciding
/// when to sign in again.
fn token_lifetime(jwt: &str) -> Option<std::time::Duration> {
    use base64::Engine as _;
    let payload = jwt.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let exp = claims.get("exp")?.as_u64()?;
    let iat = claims.get("iat")?.as_u64()?;
    exp.checked_sub(iat)
        .filter(|secs| *secs > 0)
        .map(std::time::Duration::from_secs)
}

/// How long a handle may serve before [`ReconnectingDb::refresh`] replaces it:
/// half its token's life, so a rotation that fails still leaves the other half
/// to retry in before requests start coming back unauthenticated.
fn rotation_delay(token_life: Option<std::time::Duration>) -> std::time::Duration {
    token_life
        .map(|life| life / 2)
        .unwrap_or(std::time::Duration::from_secs(FALLBACK_ROTATION_SECS))
        .max(std::time::Duration::from_secs(1))
}

/// The concrete HTTP-engine handle every long-lived caller talks to.
pub type HttpDb = surrealdb::Surreal<surrealdb::engine::remote::http::Client>;

/// True for errors that mean "this handle's session is gone; a new signin on it
/// cannot help".
///
/// The HTTP engine tags each request with the UUID of a session that lives in
/// SurrealDB's *memory*. Once the server forgets that UUID the only fix is a new
/// handle, so these are the errors [`ReconnectingDb`] reconnects on rather than
/// retries:
///
/// * `Session not found: <uuid>` — server restarted (its session map is empty)
///   or the session was detached.
/// * `The session has expired` — the session outlived its auth duration.
/// * `401 Unauthorized` — what the RPC endpoint returns for a request whose
///   session can no longer be authenticated.
pub fn is_dead_session_error(msg: &str) -> bool {
    msg.contains("Session not found")
        || msg.contains("session has expired")
        || msg.contains("401 Unauthorized")
}

/// A long-lived SurrealDB handle that survives a SurrealDB restart.
///
/// # Why this exists
///
/// The HTTP engine is only *nominally* stateless. On connect it sends an RPC
/// `attach`, which registers a session in a `HashMap<Uuid, Session>` held in the
/// SurrealDB **process memory**, and every later request carries that UUID. When
/// SurrealDB restarts, that map is empty again, so every request from a
/// previously-connected handle fails with `Session not found: <uuid>` — forever.
///
/// Re-running `signin` on the same handle cannot recover it: the signin is
/// itself routed through the dead session UUID and fails the same way. That is
/// exactly what a plain `spawn_periodic_resignin` loop used to do, so a single
/// SurrealDB restart left the SSP and scheduler permanently unable to reach the
/// database — no job drain, no view registration, no realtime — until their own
/// containers were restarted.
///
/// `ReconnectingDb` closes that hole: the periodic tick probes the current
/// handle and, when the probe says the session is dead, builds a *brand-new*
/// handle (which attaches a fresh session) and swaps it in atomically. Callers
/// hold the `ReconnectingDb`, not the raw handle, so they pick up the
/// replacement on their next call without being restarted.
///
/// # Never write to a session other tasks are using
///
/// SurrealDB 3.1.x deadlocks a session when a session write (`signin`,
/// `authenticate`, `use`, `let`) lands between the two session reads inside
/// its `query()` handler (`rpc/protocol.rs`: the first read guard is still
/// held when `run_query` takes the second, and tokio's fair `RwLock` parks
/// the second read behind the queued writer). Every later request on that
/// session then queues behind the writer forever, and a client timeout does
/// not cancel them. This type used to refresh its token with a `signin` on the
/// shared handle every minute, which wedged the scheduler's heartbeat, drift
/// check and snapshot tick roughly hourly on whitepawn (2026-09-16).
///
/// So the handle in use is never written to: the liveness probe is a
/// read-only `version()`, and the token is renewed by building a new handle
/// (signed in before anyone else can see it) and swapping it in once the old
/// token is half spent. Requests still running on the old handle finish
/// normally, and dropping its last `Arc` detaches its session.
pub struct ReconnectingDb {
    /// Only ever held long enough to clone the `Arc`; never across an await.
    current: std::sync::RwLock<std::sync::Arc<HttpDb>>,
    config: DbConfig,
    /// Poked by [`ReconnectingDb::note_error`] so a dead session is replaced on
    /// the next data-path failure instead of waiting out the probe interval.
    wake: tokio::sync::Notify,
    /// Consecutive probe failures that were NOT a dead session (transport
    /// errors). See [`TRANSPORT_FAILURES_BEFORE_RECONNECT`].
    transport_failures: std::sync::atomic::AtomicU32,
    generation: std::sync::atomic::AtomicU64,
    last_reconnect_ms: std::sync::atomic::AtomicU64,
    reconnect_failures: std::sync::atomic::AtomicU64,
    /// When the current handle's token is half spent and the handle should be
    /// replaced by a freshly signed-in one.
    rotate_at: std::sync::Mutex<tokio::time::Instant>,
    /// Planned token rotations. Kept apart from `generation`, which counts
    /// reconnects after a failure and is what dashboards read as trouble.
    rotations: std::sync::atomic::AtomicU64,
}

impl ReconnectingDb {
    /// Wrap an already-connected handle. Its token lifetime is unknown here, so
    /// the first rotation uses the fallback delay; later ones read the token.
    pub fn new(db: HttpDb, config: DbConfig) -> std::sync::Arc<Self> {
        Self::with_token_life(db, config, None)
    }

    fn with_token_life(
        db: HttpDb,
        config: DbConfig,
        token_life: Option<std::time::Duration>,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            current: std::sync::RwLock::new(std::sync::Arc::new(db)),
            config,
            wake: tokio::sync::Notify::new(),
            transport_failures: std::sync::atomic::AtomicU32::new(0),
            generation: std::sync::atomic::AtomicU64::new(1),
            last_reconnect_ms: std::sync::atomic::AtomicU64::new(0),
            reconnect_failures: std::sync::atomic::AtomicU64::new(0),
            rotate_at: std::sync::Mutex::new(tokio::time::Instant::now() + rotation_delay(token_life)),
            rotations: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Connect (signin + ns/db select) and wrap the result.
    pub async fn connect(config: &DbConfig) -> Result<std::sync::Arc<Self>> {
        let (db, token_life) = open_http_selected(config).await?;
        Ok(Self::with_token_life(db, config.clone(), token_life))
    }

    /// Planned token rotations so far (see [`ReconnectingDb::refresh`]).
    pub fn rotations(&self) -> u64 {
        self.rotations.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Successful session generation, most recent reconnect attempt duration,
    /// and failed reconnect attempts. Read-only, with no database calls.
    pub fn reconnect_metrics(&self) -> (u64, Option<u64>, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let duration = self.last_reconnect_ms.load(Relaxed);
        (self.generation.load(Relaxed), duration.checked_sub(1), self.reconnect_failures.load(Relaxed))
    }

    /// The handle to use right now.
    ///
    /// Cloning the returned `Arc` is free. Cloning the `Surreal` inside it is
    /// NOT — `Surreal::clone` mints a new session id and attaches it server-side
    /// — so callers must go through the `Arc` and never clone the inner handle.
    pub fn handle(&self) -> std::sync::Arc<HttpDb> {
        self.current
            .read()
            .expect("ReconnectingDb lock poisoned")
            .clone()
    }

    /// Report an error seen on the data path. A dead-session error wakes the
    /// refresh task immediately; anything else is ignored (transient transport
    /// failures recover on their own and must not churn the connection).
    pub fn note_error(&self, msg: &str) {
        if is_dead_session_error(msg) {
            self.wake.notify_one();
        }
    }

    /// Ask for a reconnect on the next tick, whatever the error text said.
    ///
    /// [`note_error`](Self::note_error) can only recognise a dead session from
    /// its message, but a session the server has forgotten does not always
    /// produce one — the HTTP engine has no request timeout, so the request can
    /// simply never return. A caller that time-boxes its own query has nothing
    /// but that timeout to go on, and it is strong evidence: a healthy handle
    /// answers a write in milliseconds. Observed on 2026-08-09, where the
    /// scheduler's writes hung for eight minutes straight while a direct query
    /// to the same database returned in 10ms.
    pub fn force_reconnect(&self) {
        self.wake.notify_one();
    }

    /// One maintenance pass. Returns `true` if the handle is usable afterwards.
    ///
    /// A healthy handle with a young token costs one read-only `version()`
    /// request. A half-spent token costs a new connection, and a dead session a
    /// reconnect. Nothing here writes to the session of the handle in use (see
    /// the type docs for the deadlock that rule prevents).
    pub async fn refresh(&self) -> bool {
        let db = self.handle();
        // `version()` still travels through the session with the bearer token,
        // so a forgotten session or a rejected token surfaces here just as the
        // old signin probe did. Time-boxed, because a session gone in the
        // hanging way never answers, and a probe that parks forever would take
        // the whole recovery loop with it.
        let err = match tokio::time::timeout(
            std::time::Duration::from_secs(REFRESH_PROBE_TIMEOUT_SECS),
            db.version(),
        )
        .await
        {
            Ok(Ok(_)) => {
                self.transport_failures
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                let due = tokio::time::Instant::now()
                    >= *self.rotate_at.lock().expect("ReconnectingDb lock poisoned");
                if !due {
                    return true;
                }
                // A failed rotation keeps the old handle: its token still has
                // half its life left, and the next tick tries again.
                self.replace(Replacement::Rotation).await;
                return true;
            }
            Ok(Err(e)) => e.to_string(),
            Err(_) => {
                tracing::warn!(
                    timeout_secs = REFRESH_PROBE_TIMEOUT_SECS,
                    "SurrealDB liveness probe timed out; treating the session as gone"
                );
                // Fall through to the reconnect below rather than the
                // "transient blip" branch: a probe that never returns is the
                // dead-session signature, not a slow server.
                String::new()
            }
        };

        if !err.is_empty() && !is_dead_session_error(&err) {
            // SurrealDB is unreachable or erroring for some other reason. For a
            // blip the existing session may still be perfectly valid once it
            // passes, so the first few failures leave it alone. But not
            // forever: on 2026-09-06 the control plane recreated the database
            // container at a new address, and this handle, pinned to the old
            // one, failed every probe with a transport error for 35 minutes
            // while a fresh connection to the same name worked instantly. A
            // sustained transport failure is exactly the case where a new
            // connection (and a fresh name resolution) is the only thing that
            // can help, and if the server really is down the reconnect fails
            // within its own deadline and we keep the old handle anyway.
            let streak = self
                .transport_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            if !transport_streak_exhausted(streak) {
                tracing::warn!(
                    error = %err,
                    streak,
                    limit = TRANSPORT_FAILURES_BEFORE_RECONNECT,
                    "SurrealDB liveness probe failed; retrying next tick"
                );
                return false;
            }
            tracing::warn!(
                error = %err,
                streak,
                "SurrealDB unreachable through this handle for consecutive probes (server moved?); reconnecting"
            );
        } else {
            tracing::warn!(
                error = %err,
                "SurrealDB session is gone (server restarted?); reconnecting"
            );
        }

        self.replace(Replacement::Reconnect).await
    }

    /// Build a new handle and swap it in. Returns whether the swap happened.
    async fn replace(&self, kind: Replacement) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        // The connect gets a deadline for the same reason the probe does:
        // `open_http_selected` signs in and can hang just as easily, and a
        // recovery path that can hang is not a recovery path.
        //
        // Always select the namespace/database, even for handles originally
        // opened with `connect_http_raw`: by now they exist (the raw handle's
        // caller defined them), and the replacement has to come back with
        // them selected.
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(RECONNECT_TIMEOUT_SECS),
            open_http_selected(&self.config),
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("connect timed out")));
        let elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128 - 1) as u64;
        if kind == Replacement::Reconnect {
            self.last_reconnect_ms.store(elapsed_ms + 1, Relaxed);
        }
        match result {
            Ok((fresh, token_life)) => {
                *self.current.write().expect("ReconnectingDb lock poisoned") =
                    std::sync::Arc::new(fresh);
                *self.rotate_at.lock().expect("ReconnectingDb lock poisoned") =
                    tokio::time::Instant::now() + rotation_delay(token_life);
                self.transport_failures.store(0, Relaxed);
                match kind {
                    Replacement::Reconnect => {
                        let generation = self.generation.fetch_add(1, Relaxed) + 1;
                        tracing::info!(generation, elapsed_ms, "Reconnected to SurrealDB with a fresh session");
                    }
                    Replacement::Rotation => {
                        let rotations = self.rotations.fetch_add(1, Relaxed) + 1;
                        tracing::debug!(
                            rotations,
                            elapsed_ms,
                            token_life_secs = token_life.map(|life| life.as_secs()),
                            "Rotated to a freshly signed-in SurrealDB session"
                        );
                    }
                }
                true
            }
            Err(e) => {
                match kind {
                    Replacement::Reconnect => {
                        self.reconnect_failures.fetch_add(1, Relaxed);
                        tracing::warn!(error = %e, elapsed_ms, "SurrealDB reconnect failed; retrying next tick");
                    }
                    Replacement::Rotation => {
                        tracing::warn!(
                            error = %e,
                            elapsed_ms,
                            "SurrealDB token rotation failed; keeping the current session and retrying next tick"
                        );
                    }
                }
                false
            }
        }
    }
}

/// Why [`ReconnectingDb::replace`] builds a new handle.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Replacement {
    /// The current session is dead or unreachable.
    Reconnect,
    /// The current session is fine but its token is half spent.
    Rotation,
}

/// Keep a long-lived handle usable: probe it, rotate it before its token
/// expires, and replace it outright if its server-side session dies.
///
/// Ticks on `interval_secs`, and early whenever [`ReconnectingDb::note_error`]
/// sees a dead-session error on the data path — so the common case (SurrealDB
/// restarted under us) heals on the next failed query rather than at the next
/// tick.
pub fn spawn_periodic_resignin(db: std::sync::Arc<ReconnectingDb>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
        interval.tick().await; // skip the immediate first tick — caller just signed in
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = db.wake.notified() => {}
            }
            db.refresh().await;
        }
    });
}

/// React to dead-session reports from the data path, with no timer of its own.
///
/// For hosts that already drive the periodic refresh through their own
/// scheduler (the SSP arms `TimerKind::DbResignin`, so that a Durable-Object
/// shell can fire it from `alarm()`) and therefore do not run
/// [`spawn_periodic_resignin`]. Without this, [`ReconnectingDb::note_error`]
/// would have no listener and a SurrealDB restart would wait out the full
/// refresh interval instead of healing on the first failed query.
pub fn spawn_dead_session_healer(db: std::sync::Arc<ReconnectingDb>) {
    tokio::spawn(async move {
        loop {
            db.wake.notified().await;
            db.refresh().await;
        }
    });
}

/// One refresh attempt (the timer-driven flavor of
/// [`spawn_periodic_resignin`] — the standalone SSP fires this from its
/// `DbResignin` wakeup). Failures are logged; the next wakeup retries.
pub async fn resignin_once(db: &ReconnectingDb) {
    db.refresh().await;
}

/// Default cadence for [`spawn_periodic_resignin`].
///
/// This is the worst-case detection window for a SurrealDB restart that no
/// data-path error has reported yet, and the granularity of token rotation
/// (a rotation happens on the first tick after the token is half spent).
pub const RESIGNIN_INTERVAL_SECS: u64 = 60;

/// Rotation delay when the token's lifetime cannot be read: half of the one
/// hour SurrealDB gives root tokens.
const FALLBACK_ROTATION_SECS: u64 = 30 * 60;

/// Deadline for the liveness probe in [`ReconnectingDb::refresh`].
/// Generous for a healthy server (which answers in milliseconds) and short
/// enough that a dead session is replaced within one tick rather than parking
/// the recovery loop forever.
const REFRESH_PROBE_TIMEOUT_SECS: u64 = 10;

/// Consecutive transport-level probe failures after which [`ReconnectingDb::refresh`]
/// replaces the handle instead of waiting for a dead-session error. Three
/// ticks: long enough that a restarting server (a few seconds) never churns
/// the connection, short enough that a database that moved to a new address
/// is picked up within minutes rather than at the next process restart.
const TRANSPORT_FAILURES_BEFORE_RECONNECT: u32 = 3;

/// Whether `streak` consecutive transport failures justify a reconnect.
fn transport_streak_exhausted(streak: u32) -> bool {
    streak >= TRANSPORT_FAILURES_BEFORE_RECONNECT
}

/// Deadline for building a replacement handle. The connect signs in, so it can
/// hang exactly like a probe on a wedged session.
const RECONNECT_TIMEOUT_SECS: u64 = 15;

#[cfg(test)]
mod tests {
    use super::normalize_url;

    use super::is_dead_session_error;
    use super::{rotation_delay, token_lifetime, FALLBACK_ROTATION_SECS};
    use super::{transport_streak_exhausted, TRANSPORT_FAILURES_BEFORE_RECONNECT};
    use std::time::Duration;

    fn jwt(claims: &str) -> String {
        use base64::Engine as _;
        let part = |raw: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
        format!("{}.{}.sig", part(r#"{"alg":"HS512","typ":"JWT"}"#), part(claims))
    }

    /// Root tokens live one hour, so the handle is replaced after half of it.
    #[test]
    fn rotation_happens_at_half_the_token_life() {
        let token = jwt(r#"{"iat":1789607000,"nbf":1789607000,"exp":1789610600,"ac":null,"id":"root"}"#);
        assert_eq!(token_lifetime(&token), Some(Duration::from_secs(3600)));
        assert_eq!(rotation_delay(token_lifetime(&token)), Duration::from_secs(1800));
    }

    #[test]
    fn unreadable_tokens_fall_back_to_the_root_default() {
        let fallback = Duration::from_secs(FALLBACK_ROTATION_SECS);
        for token in ["", "not-a-jwt", "a.%%%.c", &jwt(r#"{"exp":10}"#), &jwt(r#"{"iat":10,"exp":10}"#)] {
            assert_eq!(rotation_delay(token_lifetime(token)), fallback, "token {token:?}");
        }
        assert_eq!(rotation_delay(Some(Duration::from_millis(10))), Duration::from_secs(1));
    }

    #[test]
    fn transport_failures_reconnect_only_after_the_streak() {
        assert!(!transport_streak_exhausted(1), "a single blip keeps the session");
        assert!(!transport_streak_exhausted(TRANSPORT_FAILURES_BEFORE_RECONNECT - 1));
        assert!(transport_streak_exhausted(TRANSPORT_FAILURES_BEFORE_RECONNECT));
        assert!(transport_streak_exhausted(TRANSPORT_FAILURES_BEFORE_RECONNECT + 5));
    }

    /// The three strings below are copied verbatim out of a production incident
    /// where SurrealDB restarted at 19:58 and the SSP + scheduler stayed broken
    /// until their containers were restarted 84 minutes later. Every one of them
    /// has to route to "reconnect", not "retry the same handle".
    #[test]
    fn dead_session_errors_are_recognized() {
        assert!(is_dead_session_error(
            "transport: Session not found: bf4e163e-c09f-42c5-a589-9bb8421d917b"
        ));
        assert!(is_dead_session_error(
            "transport: HTTP status client error (401 Unauthorized) for url (http://surrealdb:8000/rpc)"
        ));
        assert!(is_dead_session_error("The session has expired"));
    }

    /// A SurrealDB that is merely unreachable must NOT trigger a reconnect: the
    /// existing session is still valid on the other side of the blip, and a new
    /// connection would fail identically anyway.
    #[test]
    fn transient_transport_errors_are_not_dead_sessions() {
        assert!(!is_dead_session_error(
            "transport: error sending request for url (http://surrealdb:8000/rpc)"
        ));
        assert!(!is_dead_session_error(
            "Failed to open HTTP to http://surrealdb:8000"
        ));
        assert!(!is_dead_session_error("Failed to query _00_feature_flag"));
    }

    #[test]
    fn scheme_normalization() {
        assert_eq!(normalize_url("ws://host:8000"), ("host:8000", false));
        assert_eq!(normalize_url("wss://host:8000"), ("host:8000", true));
        assert_eq!(normalize_url("http://host:8000"), ("host:8000", false));
        assert_eq!(normalize_url("https://host:8000"), ("host:8000", true));
        assert_eq!(normalize_url("host:8000"), ("host:8000", false));
    }
}
