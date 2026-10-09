//! SurrealDB WebSocket-accept watchdog.
//!
//! Every cycle the scheduler opens a FRESH WebSocket to the upstream
//! SurrealDB (`GET /rpc` with `Upgrade: websocket`) and waits for the
//! `101 Switching Protocols`. That is the one path nothing else here
//! exercises: the changefeed doorbell, the heartbeat and every SSP reuse
//! sockets they opened long ago, so they all stay green while no new client
//! can connect at all.
//!
//! Why it exists (whitepawn staging, three times on 2026-10-08, each after a
//! large PGN import): SurrealDB 3.x `dispatch_live_notification` holds the
//! `web_sockets` read guard (taken inside an `if let` chain, so it lives for
//! the whole then-block) across `send().await` into the client's bounded
//! response channel. A client that stops draining its LIVE notifications
//! keeps that guard. The next socket that disconnects queues
//! `web_sockets.write()`, and tokio's fair RwLock then parks every later
//! `read()`, including the one `get_handler` takes BEFORE it answers an
//! upgrade. New WebSockets hang with zero bytes, while HTTP and every open
//! socket keep working. Nothing recovers it except restarting SurrealDB.
//!
//! The signature is narrow on purpose: the TCP connect succeeds, `/health`
//! answers, and the upgrade gets no response at all. A database that is down
//! (refused, reset, `/health` failing) is the control plane's job, not a
//! reason to restart it again. After `fail_threshold` wedged cycles in a row
//! the watchdog asks Sp00ky Cloud to recreate the `surrealdb` role (the same
//! request as `spky restart db`), then waits `cooldown_secs` before it may ask
//! again. Without the cloud link it only reports.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use crate::admin::cloud::CloudLink;

#[derive(Clone, Debug)]
pub struct Config {
    /// 0 disables the watchdog entirely.
    pub interval_secs: u64,
    /// How long one upgrade may take before the cycle counts as wedged.
    pub timeout_secs: u64,
    /// Wedged cycles in a row before a restart is requested.
    pub fail_threshold: u32,
    /// Minimum gap between two restart requests.
    pub cooldown_secs: u64,
    /// False = report only, never restart.
    pub restart: bool,
}

impl Config {
    pub fn from_env() -> Self {
        fn num<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(default)
        }
        let interval_secs = num("SPKY_SURREAL_WS_WATCHDOG_INTERVAL_SECS", 30u64);
        Self {
            interval_secs,
            // The cycle must finish before the next tick.
            timeout_secs: num("SPKY_SURREAL_WS_WATCHDOG_TIMEOUT_SECS", 10u64)
                .max(1)
                .min(interval_secs.saturating_sub(1).max(1)),
            fail_threshold: num("SPKY_SURREAL_WS_WATCHDOG_FAIL_THRESHOLD", 3u32).max(1),
            cooldown_secs: num("SPKY_SURREAL_WS_WATCHDOG_COOLDOWN_SECS", 900u64),
            restart: std::env::var("SPKY_SURREAL_WS_WATCHDOG_RESTART")
                .map(|v| !matches!(v.trim(), "0" | "false" | "off" | "no"))
                .unwrap_or(true),
        }
    }
}

/// Lock-free state for `/health` and the admin overview. A single process-wide
/// instance: there is one upstream SurrealDB and one watchdog per scheduler.
struct Stats {
    enabled: AtomicBool,
    restart_enabled: AtomicBool,
    consecutive_wedged: AtomicU32,
    /// Epoch-ms of the last 101. 0 = never.
    last_ok_epoch_ms: AtomicU64,
    /// Latency of the last 101. `u64::MAX` = never.
    last_upgrade_ms: AtomicU64,
    /// Epoch-ms of the last restart request. 0 = never.
    last_restart_epoch_ms: AtomicU64,
    restarts_requested: AtomicU64,
}

static STATS: Stats = Stats {
    enabled: AtomicBool::new(false),
    restart_enabled: AtomicBool::new(false),
    consecutive_wedged: AtomicU32::new(0),
    last_ok_epoch_ms: AtomicU64::new(0),
    last_upgrade_ms: AtomicU64::new(u64::MAX),
    last_restart_epoch_ms: AtomicU64::new(0),
    restarts_requested: AtomicU64::new(0),
};

/// Informational only: the watchdog never decides the scheduler's health.
pub fn status_json() -> serde_json::Value {
    let ok = STATS.last_ok_epoch_ms.load(Ordering::Relaxed);
    let ms = STATS.last_upgrade_ms.load(Ordering::Relaxed);
    let restart = STATS.last_restart_epoch_ms.load(Ordering::Relaxed);
    serde_json::json!({
        "enabled": STATS.enabled.load(Ordering::Relaxed),
        "restart_enabled": STATS.restart_enabled.load(Ordering::Relaxed),
        "consecutive_wedged": STATS.consecutive_wedged.load(Ordering::Relaxed),
        "last_ok_epoch_ms": (ok > 0).then_some(ok),
        "last_upgrade_ms": (ms != u64::MAX).then_some(ms),
        "last_restart_epoch_ms": (restart > 0).then_some(restart),
        "restarts_requested": STATS.restarts_requested.load(Ordering::Relaxed),
    })
}

/// What one upgrade attempt found.
#[derive(Debug, PartialEq, Eq)]
pub enum Probe {
    /// `101 Switching Protocols`.
    Upgraded { ms: u64 },
    /// TCP connected, request sent, no response before the deadline.
    NoResponse,
    /// Anything else: refused, reset, a non-101 status. Not the wedge.
    Failed(String),
}

/// What the watchdog does about one cycle.
#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Healthy,
    /// The wedge signature: no upgrade response while `/health` answers.
    Wedged,
    /// Down or misbehaving in some other way; left to the control plane.
    Unavailable,
}

pub fn classify(probe: &Probe, health_ok: bool) -> Verdict {
    match probe {
        Probe::Upgraded { .. } => Verdict::Healthy,
        Probe::NoResponse if health_ok => Verdict::Wedged,
        _ => Verdict::Unavailable,
    }
}

/// Whether a restart may be requested now.
pub fn restart_due(
    consecutive_wedged: u32,
    cfg: &Config,
    since_last_restart: Option<Duration>,
) -> bool {
    cfg.restart
        && consecutive_wedged >= cfg.fail_threshold
        && since_last_restart.is_none_or(|d| d >= Duration::from_secs(cfg.cooldown_secs))
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `host:port` of a plain-HTTP SurrealDB URL, `None` for TLS (the probe speaks
/// raw HTTP/1.1; every cloud deployment reaches SurrealDB as `http://surrealdb:8000`).
pub fn probe_addr(db_url: &str) -> Option<String> {
    let (rest, secure) = maintenance::db::normalize_url(db_url.trim());
    if secure {
        return None;
    }
    let host = rest.split('/').next().unwrap_or(rest);
    if host.is_empty() {
        return None;
    }
    Some(if host.contains(':') { host.to_string() } else { format!("{host}:80") })
}

/// Open a new WebSocket to `addr` and wait for the upgrade response.
pub async fn probe_upgrade(addr: &str, timeout: Duration) -> Probe {
    let started = Instant::now();
    let mut stream = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Probe::Failed(format!("connect: {e}")),
        Err(_) => return Probe::Failed("connect timed out".into()),
    };
    let request = format!(
        "GET /rpc HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: c3AwMGt5LXdhdGNoZG9nLQ==\r\n\
         Sec-WebSocket-Protocol: cbor\r\nUser-Agent: sp00ky-scheduler-ws-watchdog\r\n\r\n"
    );
    if let Err(e) = stream.write_all(request.as_bytes()).await {
        return Probe::Failed(format!("write: {e}"));
    }
    let remaining = timeout.saturating_sub(started.elapsed());
    let mut buf = [0u8; 64];
    let mut got = Vec::with_capacity(64);
    let read = tokio::time::timeout(remaining, async {
        // The status line is all we need.
        while !got.contains(&b'\n') && got.len() < 64 {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    })
    .await;
    match read {
        Err(_) if got.is_empty() => Probe::NoResponse,
        Err(_) => Probe::Failed("partial response before the deadline".into()),
        Ok(Err(e)) => Probe::Failed(format!("read: {e}")),
        Ok(Ok(())) => {
            let line = String::from_utf8_lossy(&got);
            let line = line.lines().next().unwrap_or("").trim().to_string();
            if line.split_whitespace().nth(1) == Some("101") {
                // Dropping the stream closes the socket; SurrealDB cleans up.
                Probe::Upgraded { ms: started.elapsed().as_millis() as u64 }
            } else if line.is_empty() {
                Probe::Failed("connection closed without a response".into())
            } else {
                Probe::Failed(format!("unexpected response: {line}"))
            }
        }
    }
}

async fn health_ok(client: &reqwest::Client, addr: &str) -> bool {
    match client.get(format!("http://{addr}/health")).send().await {
        Ok(res) => res.status().is_success(),
        Err(_) => false,
    }
}

/// Spawn the watchdog. No-op when disabled or when the DB URL is not plain HTTP.
pub fn spawn(db_url: &str, cfg: Config, cloud: Option<CloudLink>) {
    if cfg.interval_secs == 0 {
        info!("SurrealDB WebSocket watchdog disabled (SPKY_SURREAL_WS_WATCHDOG_INTERVAL_SECS=0)");
        return;
    }
    let Some(addr) = probe_addr(db_url) else {
        info!("SurrealDB WebSocket watchdog off: it needs a plain http:// or ws:// SPKY_DB_URL");
        return;
    };
    let restart = cfg.restart && cloud.is_some();
    STATS.enabled.store(true, Ordering::Relaxed);
    STATS.restart_enabled.store(restart, Ordering::Relaxed);
    info!(
        %addr,
        interval_secs = cfg.interval_secs,
        timeout_secs = cfg.timeout_secs,
        fail_threshold = cfg.fail_threshold,
        cooldown_secs = cfg.cooldown_secs,
        restart,
        "SurrealDB WebSocket watchdog started"
    );

    tokio::spawn(async move {
        let health = match reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "SurrealDB WebSocket watchdog could not build its HTTP client; not started");
                STATS.enabled.store(false, Ordering::Relaxed);
                return;
            }
        };
        let mut interval = tokio::time::interval(Duration::from_secs(cfg.interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let timeout = Duration::from_secs(cfg.timeout_secs);
        let mut wedged: u32 = 0;
        // Separate from `wedged`, which a restart request resets: the
        // incident stays open until a new WebSocket actually connects again.
        let mut incident_open = false;
        let mut last_restart: Option<Instant> = None;
        loop {
            interval.tick().await;
            let probe = probe_upgrade(&addr, timeout).await;
            let verdict = match &probe {
                // Only ask /health when it decides something.
                Probe::NoResponse => classify(&probe, health_ok(&health, &addr).await),
                _ => classify(&probe, false),
            };
            match verdict {
                Verdict::Healthy => {
                    if let Probe::Upgraded { ms } = probe {
                        STATS.last_upgrade_ms.store(ms, Ordering::Relaxed);
                    }
                    STATS.last_ok_epoch_ms.store(now_epoch_ms(), Ordering::Relaxed);
                    if incident_open {
                        info!("SurrealDB accepts WebSockets again");
                        crate::admin::incidents::emit(
                            "surrealdb",
                            "ws_accept_wedge",
                            "recovered",
                            "SurrealDB accepts new WebSocket connections again",
                            None,
                        );
                        incident_open = false;
                    }
                    wedged = 0;
                    debug!(?probe, "SurrealDB WebSocket probe ok");
                }
                Verdict::Unavailable => {
                    // Not the wedge. Reset, so a restart the control plane is
                    // already doing never counts toward requesting another.
                    wedged = 0;
                    debug!(?probe, "SurrealDB WebSocket probe failed (database unavailable, not wedged)");
                }
                Verdict::Wedged => {
                    wedged += 1;
                    warn!(
                        cycles = wedged,
                        threshold = cfg.fail_threshold,
                        timeout_secs = cfg.timeout_secs,
                        "SurrealDB answers HTTP but a new WebSocket got no upgrade response"
                    );
                    if !incident_open {
                        incident_open = true;
                        crate::admin::incidents::emit(
                            "surrealdb",
                            "ws_accept_wedge",
                            "open",
                            "SurrealDB answers HTTP but no new WebSocket can connect",
                            None,
                        );
                    }
                }
            }
            STATS.consecutive_wedged.store(wedged, Ordering::Relaxed);

            if !restart_due(wedged, &cfg, last_restart.map(|t| t.elapsed())) {
                continue;
            }
            let Some(link) = cloud.as_ref() else { continue };
            warn!(cycles = wedged, "Asking Sp00ky Cloud to restart SurrealDB (WebSocket accept wedged)");
            let body = serde_json::json!({
                "roles": ["surrealdb"],
                "upgrade": false,
                "clean": false,
                "surreal": false,
            });
            // The attempt starts the cooldown whatever its outcome, so a
            // control plane that keeps refusing is not asked every 30s.
            last_restart = Some(Instant::now());
            STATS.last_restart_epoch_ms.store(now_epoch_ms(), Ordering::Relaxed);
            match link.post("/restart", body).await {
                Ok(_) => {
                    STATS.restarts_requested.fetch_add(1, Ordering::Relaxed);
                    crate::admin::incidents::emit(
                        "surrealdb",
                        "ws_accept_wedge",
                        "recorded",
                        "Restart of SurrealDB requested from Sp00ky Cloud by the WebSocket watchdog",
                        None,
                    );
                    wedged = 0;
                    STATS.consecutive_wedged.store(0, Ordering::Relaxed);
                }
                Err((status, body)) => {
                    error!(%status, error = %body.0, "SurrealDB restart request refused");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn cfg() -> Config {
        Config { interval_secs: 30, timeout_secs: 2, fail_threshold: 3, cooldown_secs: 900, restart: true }
    }

    #[test]
    fn only_no_response_with_healthy_http_is_the_wedge() {
        assert_eq!(classify(&Probe::Upgraded { ms: 3 }, false), Verdict::Healthy);
        assert_eq!(classify(&Probe::NoResponse, true), Verdict::Wedged);
        assert_eq!(classify(&Probe::NoResponse, false), Verdict::Unavailable);
        assert_eq!(classify(&Probe::Failed("connect: refused".into()), true), Verdict::Unavailable);
    }

    #[test]
    fn restart_needs_threshold_cooldown_and_permission() {
        let c = cfg();
        assert!(!restart_due(2, &c, None));
        assert!(restart_due(3, &c, None));
        assert!(!restart_due(3, &c, Some(Duration::from_secs(60))));
        assert!(restart_due(3, &c, Some(Duration::from_secs(900))));
        assert!(!restart_due(9, &Config { restart: false, ..c }, None));
    }

    #[test]
    fn probe_addr_takes_plain_urls_only() {
        assert_eq!(probe_addr("http://surrealdb:8000").as_deref(), Some("surrealdb:8000"));
        assert_eq!(probe_addr("ws://surrealdb:8000/rpc").as_deref(), Some("surrealdb:8000"));
        assert_eq!(probe_addr("localhost:8000").as_deref(), Some("localhost:8000"));
        assert_eq!(probe_addr("http://db.internal").as_deref(), Some("db.internal:80"));
        assert_eq!(probe_addr("https://db.example.com"), None);
        assert_eq!(probe_addr("wss://db.example.com/rpc"), None);
    }

    async fn serve_once(reply: Option<&'static [u8]>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            match reply {
                Some(bytes) => {
                    let _ = sock.write_all(bytes).await;
                }
                // The wedge: accept, read the request, never answer.
                None => tokio::time::sleep(Duration::from_secs(30)).await,
            }
        });
        addr
    }

    #[tokio::test]
    async fn probe_sees_upgrade_silence_and_other_statuses() {
        let up = serve_once(Some(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n")).await;
        assert!(matches!(probe_upgrade(&up, Duration::from_secs(2)).await, Probe::Upgraded { .. }));

        let silent = serve_once(None).await;
        assert_eq!(probe_upgrade(&silent, Duration::from_millis(300)).await, Probe::NoResponse);

        let refused = serve_once(Some(b"HTTP/1.1 400 Bad Request\r\n\r\n")).await;
        assert!(matches!(probe_upgrade(&refused, Duration::from_secs(2)).await, Probe::Failed(_)));
    }

    #[tokio::test]
    async fn probe_reports_a_closed_port_as_failed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        assert!(matches!(probe_upgrade(&addr, Duration::from_secs(2)).await, Probe::Failed(_)));
    }
}
