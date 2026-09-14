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

    db.signin(surrealdb::opt::auth::Root {
        username: db_config.username.clone(),
        password: db_config.password.clone(),
    })
    .await
    .context("Remote SurrealDB signin failed")?;

    Ok(db)
}

/// Open a fresh HTTP connection to the main SurrealDB: root signin plus
/// namespace/database selection.
pub async fn connect_http(
    db_config: &DbConfig,
) -> Result<surrealdb::Surreal<surrealdb::engine::remote::http::Client>> {
    let db = connect_http_raw(db_config).await?;

    db.use_ns(&db_config.namespace)
        .use_db(&db_config.database)
        .await
        .context("Failed to select remote namespace/database")?;

    Ok(db)
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
}

impl ReconnectingDb {
    /// Wrap an already-connected handle.
    pub fn new(db: HttpDb, config: DbConfig) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            current: std::sync::RwLock::new(std::sync::Arc::new(db)),
            config,
            wake: tokio::sync::Notify::new(),
            transport_failures: std::sync::atomic::AtomicU32::new(0),
            generation: std::sync::atomic::AtomicU64::new(1),
            last_reconnect_ms: std::sync::atomic::AtomicU64::new(0),
            reconnect_failures: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Connect (signin + ns/db select) and wrap the result.
    pub async fn connect(config: &DbConfig) -> Result<std::sync::Arc<Self>> {
        Ok(Self::new(connect_http(config).await?, config.clone()))
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
    /// Ordering matters: `signin` doubles as the liveness probe *and* the token
    /// refresh, so a healthy handle costs exactly one request per tick. Only
    /// when it fails do we pay for a reconnect.
    pub async fn refresh(&self) -> bool {
        let db = self.handle();
        let probe = db.signin(surrealdb::opt::auth::Root {
            username: self.config.username.clone(),
            password: self.config.password.clone(),
        });
        // Time-boxed, because this probe is routed through the very session it
        // is testing: when that session is gone in the hanging way, an
        // un-bounded signin parks forever and takes the whole recovery loop
        // with it — the handle is then never replaced and every consumer stays
        // wedged until the container restarts. That is the failure this type
        // exists to prevent, so it must not be reachable from inside it.
        let err = match tokio::time::timeout(
            std::time::Duration::from_secs(REFRESH_PROBE_TIMEOUT_SECS),
            probe,
        )
        .await
        {
            Ok(Ok(_)) => {
                self.transport_failures
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
            Ok(Err(e)) => e.to_string(),
            Err(_) => {
                tracing::warn!(
                    timeout_secs = REFRESH_PROBE_TIMEOUT_SECS,
                    "SurrealDB re-signin timed out; treating the session as gone"
                );
                // Fall through to the reconnect below rather than the
                // "transient blip" branch: a signin that never returns is the
                // dead-session signature, not a slow server.
                String::new()
            }
        };

        if !err.is_empty() && !is_dead_session_error(&err) {
            // SurrealDB is unreachable or erroring for some other reason. For a
            // blip the existing session may still be perfectly valid once it
            // passes, so the first few failures leave it alone. But not
            // forever: on 2026-09-06 the control plane recreated the database
            // container at a new address, and this handle — pinned to the old
            // one — failed every probe with a transport error for 35 minutes
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
                    "SurrealDB re-signin failed; retrying next tick"
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

        // The reconnect gets a deadline for the same reason the probe does —
        // `connect_http` performs its own signin and can hang just as easily,
        // and a recovery path that can hang is not a recovery path.

        // Always reconnect via `connect_http`, even for handles originally
        // opened with `connect_http_raw`: by the time a reconnect is needed the
        // namespace/database exist (the raw handle's caller defined them), and
        // the replacement has to come back with them selected.
        let reconnect = connect_http(&self.config);
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(RECONNECT_TIMEOUT_SECS),
            reconnect,
        )
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("connect timed out")));
        let elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128 - 1) as u64;
        self.last_reconnect_ms.store(elapsed_ms + 1, std::sync::atomic::Ordering::Relaxed);
        match result {
            Ok(fresh) => {
                *self
                    .current
                    .write()
                    .expect("ReconnectingDb lock poisoned") = std::sync::Arc::new(fresh);
                self.transport_failures
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                let generation = self.generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                tracing::info!(generation, elapsed_ms, "Reconnected to SurrealDB with a fresh session");
                true
            }
            Err(e) => {
                self.reconnect_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(error = %e, elapsed_ms, "SurrealDB reconnect failed; retrying next tick");
                false
            }
        }
    }
}

/// Keep a long-lived handle usable: refresh its auth token and replace it
/// outright if its server-side session dies.
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
/// This is both the token-refresh cadence (well below the shortest common token
/// duration of 1h) and the worst-case detection window for a SurrealDB restart
/// that no data-path error has reported yet — hence a minute rather than the
/// token lifetime.
pub const RESIGNIN_INTERVAL_SECS: u64 = 60;

/// Deadline for the liveness/token-refresh signin in [`ReconnectingDb::refresh`].
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

/// Deadline for building the replacement handle. `connect_http` signs in too,
/// so it can hang exactly like the probe it is replacing.
const RECONNECT_TIMEOUT_SECS: u64 = 15;

#[cfg(test)]
mod tests {
    use super::normalize_url;

    use super::is_dead_session_error;
    use super::{transport_streak_exhausted, TRANSPORT_FAILURES_BEFORE_RECONNECT};

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
