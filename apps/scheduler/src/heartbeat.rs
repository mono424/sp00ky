//! End-to-end sync-pipeline heartbeat.
//!
//! Every cycle the probe UPSERTs `_00_heartbeat:probe` on the UPSTREAM
//! SurrealDB with a fresh `hb_seq`. The row's hand-written mutation event
//! (see `apps/cli/src/sp00ky.rs`) posts to this scheduler's `/ingest`, which
//! WALs and broadcasts it to every ready SSP, whose ingest handler records
//! the seq after stepping the circuit. The probe then polls each SSP's
//! `GET /debug/heartbeat` until ALL of them report the new seq (queries are
//! pinned per-SSP, so one deaf SSP is a real outage for its clients) and
//! records the wall-clock as the end-to-end latency.
//!
//! This exercises exactly the path that died in the 2026-08-08 outage:
//! DB event → scheduler HTTP → WAL → broadcast → SSP circuit. A wedged
//! scheduler stops the loop entirely — which is why the success-side ping
//! (dead-man's-switch, see `maintenance::alert`) is the primary alert
//! channel, and the failure webhook only the fast path.
//!
//! Results land in four places:
//! - `HeartbeatStats` (atomics) → `/metrics` + `/health` (staleness folds
//!   into "degraded"),
//! - the recent-cycle window → `/info`, which is what browser DevTools can
//!   reach through `fn::spooky::info()` (it has no poller of its own, so the
//!   history has to arrive with the entity),
//! - `_00_heartbeat_rollup` hourly rows upstream (bounded time series the
//!   control plane can scrape),
//! - the alerter's ping/webhook URLs.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::router::SspPool;
use crate::transport::HttpTransport;
use maintenance::alert::Alerter;
use maintenance::db::ReconnectingDb;

/// How many recent probe cycles `/info` carries. 60 at the 30s default is a
/// 30-minute window — enough for the DevTools sparkline to show something the
/// moment the panel opens (it has no poller of its own), small enough that the
/// added `/info` payload stays around 2KB.
const SAMPLE_WINDOW: usize = 60;

/// One probe cycle, as served to UIs. `ms` is `None` for a failed cycle.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Sample {
    /// Epoch-ms the cycle was recorded.
    pub ts: u64,
    pub ms: Option<u64>,
    pub ok: bool,
}

/// Lock-free probe results, shared with the metrics/health handlers.
pub struct HeartbeatStats {
    /// Last successful cycle's end-to-end latency. `u64::MAX` = never.
    pub last_e2e_ms: AtomicU64,
    /// Epoch-ms of the last successful cycle. 0 = never.
    pub last_ok_epoch_ms: AtomicU64,
    /// Epoch-ms of the last attempt (any outcome). 0 = never.
    pub last_attempt_epoch_ms: AtomicU64,
    pub consecutive_failures: AtomicU32,
    /// False until `spawn` runs (and stays false when disabled via env), so
    /// `/health` doesn't report a stale heartbeat on deployments without one.
    pub enabled: AtomicBool,
    /// Rolling window of recent cycles, newest last. std Mutex on purpose:
    /// held for nanoseconds, never across an await.
    samples: std::sync::Mutex<std::collections::VecDeque<Sample>>,
    /// Why the probe currently cannot measure anything, if it cannot.
    ///
    /// "No ready SSPs" is not a slow cycle or a failed one — there is nothing
    /// to measure, because the probe's whole test is whether a write reaches
    /// every ready provider. Recording nothing left `last_e2e_ms` sitting at
    /// its last good value with `stale` still false, so for a full grace
    /// window (~2 min) a bootstrap looked exactly like a healthy 9ms — during
    /// the one period when clients demonstrably cannot sync.
    blocked_reason: std::sync::Mutex<Option<&'static str>>,
}

impl HeartbeatStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            last_e2e_ms: AtomicU64::new(u64::MAX),
            last_ok_epoch_ms: AtomicU64::new(0),
            last_attempt_epoch_ms: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
            enabled: AtomicBool::new(false),
            samples: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(
                SAMPLE_WINDOW,
            )),
            blocked_reason: std::sync::Mutex::new(None),
        })
    }

    /// Append one cycle, evicting the oldest past `SAMPLE_WINDOW`.
    fn push_sample(&self, sample: Sample) {
        let mut samples = self.samples.lock().unwrap();
        if samples.len() == SAMPLE_WINDOW {
            samples.pop_front();
        }
        samples.push_back(sample);
    }

    /// The recent-cycle window, oldest first.
    pub fn samples(&self) -> Vec<Sample> {
        self.samples.lock().unwrap().iter().copied().collect()
    }

    /// Why the probe cannot currently measure, if it cannot.
    pub fn blocked_reason(&self) -> Option<&'static str> {
        *self.blocked_reason.lock().unwrap()
    }

    /// Whether the reported latency describes the pipeline right now. False
    /// when the probe is stale OR blocked — the two ways a number on screen
    /// can be historical rather than current.
    pub fn is_current(&self, cfg: &Config, now_epoch_ms: u64) -> bool {
        !self.is_stale(cfg, now_epoch_ms) && self.blocked_reason().is_none()
    }

    /// The single heartbeat shape every consumer reads. `/info` serves it with
    /// `samples` (DevTools draws the sparkline from them); `/health` and
    /// `/metrics` serve the scalars only.
    ///
    /// Keep the scalar keys stable — `spky status`, the control-plane scrape
    /// and external monitors all read them by name.
    pub fn snapshot(&self, cfg: &Config, now_epoch_ms: u64, with_samples: bool) -> serde_json::Value {
        let last_e2e = self.last_e2e_ms.load(Ordering::Relaxed);
        let last_ok = self.last_ok_epoch_ms.load(Ordering::Relaxed);
        let blocked = self.blocked_reason();
        let mut out = serde_json::json!({
            "enabled": self.enabled.load(Ordering::Relaxed),
            "stale": self.is_stale(cfg, now_epoch_ms),
            "blocked": blocked.is_some(),
            "blocked_reason": blocked,
            "last_e2e_ms": (last_e2e != u64::MAX).then_some(last_e2e),
            "last_ok_epoch_ms": (last_ok > 0).then_some(last_ok),
            "consecutive_failures": self.consecutive_failures.load(Ordering::Relaxed),
            "interval_secs": cfg.interval_secs,
        });
        if with_samples {
            out["samples"] = serde_json::json!(self.samples());
        }
        out
    }

    /// Staleness predicate for `/health`: enabled AND the last success is
    /// older than the grace window (3 full cycles + one timeout, or the
    /// `SPKY_HEALTH_MAX_HEARTBEAT_AGE_SECS` override if larger).
    pub fn is_stale(&self, cfg: &Config, now_epoch_ms: u64) -> bool {
        if !self.enabled.load(Ordering::Relaxed) {
            return false;
        }
        let last_ok = self.last_ok_epoch_ms.load(Ordering::Relaxed);
        let grace_secs = std::env::var("SPKY_HEALTH_MAX_HEARTBEAT_AGE_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
            .max(3 * cfg.interval_secs + cfg.timeout_secs);
        // Never-succeeded counts as stale once the grace window has passed
        // since boot would have allowed several cycles; use last_attempt as
        // the anchor when there has been no success yet.
        let anchor = if last_ok > 0 {
            last_ok
        } else {
            self.last_attempt_epoch_ms.load(Ordering::Relaxed)
        };
        anchor > 0 && now_epoch_ms.saturating_sub(anchor) > grace_secs * 1000
    }
}

#[derive(Clone)]
pub struct Config {
    /// 0 disables the probe entirely.
    pub interval_secs: u64,
    pub timeout_secs: u64,
    pub fail_threshold: u32,
    pub ping_url: Option<String>,
    pub webhook_url: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        let interval_secs: u64 = std::env::var("SPKY_HEARTBEAT_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        let timeout_secs = std::env::var("SPKY_HEARTBEAT_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &u64| *n > 0)
            .unwrap_or(25)
            // The cycle must finish before the next tick.
            .min(interval_secs.saturating_sub(1).max(1));
        Self {
            interval_secs,
            timeout_secs,
            fail_threshold: std::env::var("SPKY_HEARTBEAT_FAIL_THRESHOLD")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|n: &u32| *n > 0)
                .unwrap_or(3),
            ping_url: std::env::var("SPKY_HEARTBEAT_PING_URL").ok(),
            webhook_url: std::env::var("SPKY_ALERT_WEBHOOK_URL").ok(),
        }
    }
}

enum CycleOutcome {
    Ok { e2e_ms: u64 },
    /// Nothing to probe (no ready SSPs). Counted, not alerted — `/health`
    /// already 503s on zero ready SSPs.
    Skipped(&'static str),
    Failed { stage: &'static str, detail: String },
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Spawn the probe loop. No-op when `cfg.interval_secs == 0`.
pub fn spawn(
    db: Arc<ReconnectingDb>,
    ssp_pool: Arc<RwLock<SspPool>>,
    transport: Arc<HttpTransport>,
    stats: Arc<HeartbeatStats>,
    cfg: Config,
    poke: Option<Arc<tokio::sync::Notify>>,
) {
    if cfg.interval_secs == 0 {
        info!("E2E heartbeat disabled (SPKY_HEARTBEAT_INTERVAL_SECS=0)");
        return;
    }
    stats.enabled.store(true, Ordering::Relaxed);
    info!(
        interval_secs = cfg.interval_secs,
        timeout_secs = cfg.timeout_secs,
        ping = cfg.ping_url.is_some(),
        webhook = cfg.webhook_url.is_some(),
        "E2E heartbeat probe started"
    );

    tokio::spawn(async move {
        let alerter = Alerter::new(
            cfg.ping_url.clone(),
            cfg.webhook_url.clone(),
            cfg.fail_threshold,
        );
        let mut interval = tokio::time::interval(Duration::from_secs(cfg.interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_prune_epoch_ms: u64 = 0;
        // Hard ceiling on one cycle. The probe's own timeout bounds the SSP
        // poll, but the upstream write does not have one of its own: the
        // SurrealDB HTTP engine ships no request timeout, so a dead connection
        // parks the await forever. That is not hypothetical — it stopped the
        // probe dead after four cycles on 2026-08-09 while `/metrics` happily
        // kept serving the last good latency, which the control plane scraped
        // every 15s into a flat, healthy-looking chart. A monitor that hangs
        // must report failure, not silence.
        let cycle_budget = Duration::from_secs(cfg.timeout_secs + 30);
        loop {
            interval.tick().await;

            // Run the cycle as its own task so a panic inside it surfaces as a
            // JoinError instead of unwinding this loop and ending monitoring
            // for the life of the process.
            let cycle = tokio::spawn({
                let db = Arc::clone(&db);
                let ssp_pool = Arc::clone(&ssp_pool);
                let transport = Arc::clone(&transport);
                let timeout_secs = cfg.timeout_secs;
                let poke = poke.clone();
                async move { run_one_cycle(&db, &ssp_pool, &transport, timeout_secs, poke.as_deref()).await }
            });
            let outcome = match tokio::time::timeout(cycle_budget, cycle).await {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(join_err)) => CycleOutcome::Failed {
                    stage: "panic",
                    detail: join_err.to_string(),
                },
                Err(_) => CycleOutcome::Failed {
                    stage: "cycle_timeout",
                    detail: format!("cycle exceeded {}s", cycle_budget.as_secs()),
                },
            };

            record(&stats, &alerter, &outcome);
            // Same reasoning: a hung rollup write must not take the loop with
            // it. Best-effort already, so a timeout just skips the sample.
            if tokio::time::timeout(Duration::from_secs(15), write_rollup(&db, &outcome))
                .await
                .is_err()
            {
                warn!("heartbeat rollup write timed out");
            }

            // Bounded retention: prune rollup rows older than 90 days, once
            // a day, from the probe loop itself (no reaper exists for this).
            let now = now_epoch_ms();
            if now.saturating_sub(last_prune_epoch_ms) > 24 * 3600 * 1000 {
                last_prune_epoch_ms = now;
                let handle = db.handle();
                let prune = handle.query("DELETE _00_heartbeat_rollup WHERE bucket < time::now() - 90d");
                match tokio::time::timeout(Duration::from_secs(30), prune).await {
                    Ok(Err(e)) => debug!(error = %e, "heartbeat rollup prune failed"),
                    Err(_) => debug!("heartbeat rollup prune timed out"),
                    Ok(Ok(_)) => {}
                }
            }
        }
    });
}

/// One probe cycle: write upstream, then poll every ready SSP until all have
/// seen the new seq or the deadline passes.
async fn run_one_cycle(
    db: &Arc<ReconnectingDb>,
    ssp_pool: &Arc<RwLock<SspPool>>,
    transport: &Arc<HttpTransport>,
    timeout_secs: u64,
    poke: Option<&tokio::sync::Notify>,
) -> CycleOutcome {
    // Snapshot (id, url) of ready SSPs — take-and-drop, never held across
    // an await (the whole point of this probe is catching lock convoys, not
    // causing them).
    let targets: Vec<(String, String)> = {
        let pool = ssp_pool.read().await;
        pool.all()
            .iter()
            .filter(|ssp| pool.is_ready(&ssp.id))
            .map(|ssp| (ssp.id.clone(), ssp.url.clone()))
            .collect()
    };
    if targets.is_empty() {
        return CycleOutcome::Skipped("no ready SSPs");
    }

    let hb_seq = now_epoch_ms();
    let started = tokio::time::Instant::now();
    let handle = db.handle();
    // Explicitly time-boxed: the SurrealDB HTTP engine has no request timeout,
    // so a connection that died under us parks this await forever — the probe
    // then reports neither success nor failure, which is worse than either.
    let write = handle
        .query("UPSERT _00_heartbeat:probe SET hb_seq = $s")
        .bind(("s", hb_seq as i64));
    match tokio::time::timeout(Duration::from_secs(timeout_secs), write).await {
        Ok(Ok(_)) => {
            // With the changefeed transport the probe row does not write
            // `_00_version`, so the doorbell stays silent for it; the
            // scheduler rings its own tail instead of waiting out the
            // fallback interval.
            if let Some(poke) = poke {
                poke.notify_one();
            }
        }
        Ok(Err(e)) => {
            let msg = e.to_string();
            db.note_error(&msg);
            return CycleOutcome::Failed {
                stage: "db_write",
                detail: msg,
            };
        }
        Err(_) => {
            // A write that never returns is the signature of a session the
            // server has already forgotten. `note_error` cannot see that — it
            // matches on error text and a hang produces none — so ask for the
            // reconnect explicitly.
            db.force_reconnect();
            return CycleOutcome::Failed {
                stage: "db_write_timeout",
                detail: format!("probe write exceeded {timeout_secs}s"),
            };
        }
    }

    // Poll until every target reports hb_seq >= the seq we just wrote.
    let deadline = started + Duration::from_secs(timeout_secs);
    let mut pending: Vec<(String, String)> = targets;
    // Short first waits, doubling: the probe measures the pipeline, not the
    // poll cadence. With the changefeed transport the change lands a few ms
    // after the write returns, so a fixed 500 ms sleep after the first miss
    // reported half a second for a ~10 ms pipeline.
    let mut backoff = Duration::from_millis(5);
    loop {
        let mut still_pending = Vec::new();
        for (id, url) in pending {
            let seen = match transport.get_from_ssp(&url, "/debug/heartbeat").await {
                Ok(resp) => resp
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|v| v.get("hb_seq").and_then(|s| s.as_u64())),
                Err(_) => None,
            };
            if seen.map(|s| s >= hb_seq).unwrap_or(false) {
                continue;
            }
            still_pending.push((id, url));
        }
        pending = still_pending;
        if pending.is_empty() {
            return CycleOutcome::Ok {
                e2e_ms: started.elapsed().as_millis() as u64,
            };
        }
        if tokio::time::Instant::now() >= deadline {
            let missing: Vec<&str> = pending.iter().map(|(id, _)| id.as_str()).collect();
            return CycleOutcome::Failed {
                stage: "ssp_visibility",
                detail: format!("SSPs never saw hb_seq {}: {}", hb_seq, missing.join(", ")),
            };
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(250));
    }
}

fn record(stats: &Arc<HeartbeatStats>, alerter: &Alerter, outcome: &CycleOutcome) {
    let now = now_epoch_ms();
    stats.last_attempt_epoch_ms.store(now, Ordering::Relaxed);
    match outcome {
        CycleOutcome::Ok { e2e_ms } => {
            stats.last_e2e_ms.store(*e2e_ms, Ordering::Relaxed);
            stats.last_ok_epoch_ms.store(now, Ordering::Relaxed);
            if stats.consecutive_failures.swap(0, Ordering::Relaxed) > 0 {
                crate::admin::incidents::emit("scheduler", "heartbeat", "recovered", "End-to-end heartbeat recovered", None);
            }
            stats.push_sample(Sample { ts: now, ms: Some(*e2e_ms), ok: true });
            *stats.blocked_reason.lock().unwrap() = None;
            debug!(e2e_ms, "heartbeat ok");
            alerter.observe(true, 0, || {
                serde_json::json!({ "component": "scheduler_heartbeat", "e2e_ms": e2e_ms })
            });
        }
        CycleOutcome::Skipped(reason) => {
            // Not a failure of the pipeline itself; don't page, and leave no
            // sample — a gap in the window reads truthfully as "not probed".
            // But DO mark the reading not-current immediately: waiting for the
            // staleness window to expire would publish the last good latency
            // as though it still described a stack that cannot serve anyone.
            *stats.blocked_reason.lock().unwrap() = Some(reason);
            debug!(reason, "heartbeat skipped");
        }
        CycleOutcome::Failed { stage, detail } => {
            let failures = stats.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
            stats.push_sample(Sample { ts: now, ms: None, ok: false });
            // A failure is a measurement: we asked and it did not work.
            *stats.blocked_reason.lock().unwrap() = None;
            warn!(stage, %detail, failures, "heartbeat failed");
            crate::admin::incidents::emit("scheduler", "heartbeat", "open", &format!("End-to-end heartbeat failed at {stage}"), None);
            alerter.observe(false, failures, || {
                serde_json::json!({
                    "component": "scheduler_heartbeat",
                    "stage": stage,
                    "detail": detail,
                    "consecutive_failures": failures,
                })
            });
        }
    }
}

/// Best-effort hourly rollup UPSERT upstream (`_00_heartbeat_rollup`).
/// The row id is the hour bucket, so concurrent schedulers can't race:
/// `count += 1` is atomic within SurrealDB.
async fn write_rollup(db: &Arc<ReconnectingDb>, outcome: &CycleOutcome) {
    let (ok, e2e_ms) = match outcome {
        CycleOutcome::Ok { e2e_ms } => (true, *e2e_ms),
        CycleOutcome::Failed { .. } => (false, 0),
        CycleOutcome::Skipped(_) => return,
    };
    let epoch_hour = now_epoch_ms() / 1000 / 3600 * 3600;
    let key = format!("h{}", epoch_hour);
    // v3 SurrealQL: `type::thing` is gone (`type::record`), and on the
    // create branch of an UPSERT the SET expressions see fields as NONE
    // (DEFAULTs apply after), hence the `?? 0`.
    let query = if ok {
        "UPSERT type::record('_00_heartbeat_rollup', $key) SET \
             bucket = <datetime>$bucket, count += 1, ok += 1, sum_ms += $ms, \
             max_ms = math::max([max_ms ?? 0, $ms]), last_e2e_ms = $ms, last_ok_at = time::now()"
    } else {
        "UPSERT type::record('_00_heartbeat_rollup', $key) SET \
             bucket = <datetime>$bucket, count += 1"
    };
    let bucket_iso = chrono::DateTime::from_timestamp(epoch_hour as i64, 0)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default();
    let handle = db.handle();
    if let Err(e) = handle
        .query(query)
        .bind(("key", key))
        .bind(("bucket", bucket_iso))
        .bind(("ms", e2e_ms as i64))
        .await
    {
        let msg = e.to_string();
        db.note_error(&msg);
        debug!(error = %msg, "heartbeat rollup write failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(interval: u64, timeout: u64) -> Config {
        Config {
            interval_secs: interval,
            timeout_secs: timeout,
            fail_threshold: 3,
            ping_url: None,
            webhook_url: None,
        }
    }

    #[test]
    fn staleness_needs_enabled_and_age_past_grace() {
        let c = cfg(30, 25);
        let stats = HeartbeatStats::new();
        let now: u64 = 10_000_000;

        // Disabled → never stale.
        stats.last_ok_epoch_ms.store(1, Ordering::Relaxed);
        assert!(!stats.is_stale(&c, now));

        stats.enabled.store(true, Ordering::Relaxed);
        // Grace = 3*30 + 25 = 115s. Fresh success → not stale.
        stats.last_ok_epoch_ms.store(now - 60_000, Ordering::Relaxed);
        assert!(!stats.is_stale(&c, now));
        // Older than grace → stale.
        stats.last_ok_epoch_ms.store(now - 200_000, Ordering::Relaxed);
        assert!(stats.is_stale(&c, now));

        // Never-succeeded anchors on last_attempt.
        let never = HeartbeatStats::new();
        never.enabled.store(true, Ordering::Relaxed);
        assert!(!never.is_stale(&c, now), "no attempts yet → not stale");
        never.last_attempt_epoch_ms.store(now - 200_000, Ordering::Relaxed);
        assert!(never.is_stale(&c, now));
    }

    #[test]
    fn sample_window_is_bounded_and_ordered() {
        let stats = HeartbeatStats::new();
        let alerter = maintenance::alert::Alerter::new(None, None, 3);

        for i in 0..(SAMPLE_WINDOW + 10) {
            record(&stats, &alerter, &CycleOutcome::Ok { e2e_ms: i as u64 });
        }
        let samples = stats.samples();
        assert_eq!(samples.len(), SAMPLE_WINDOW, "window must be capped");
        assert_eq!(
            samples.first().unwrap().ms,
            Some(10),
            "oldest samples are evicted first"
        );
        assert_eq!(samples.last().unwrap().ms, Some((SAMPLE_WINDOW + 9) as u64));

        // A failed cycle is recorded with no latency; a skipped one leaves no
        // sample at all (a gap reads as "not probed", not as "0ms").
        record(&stats, &alerter, &CycleOutcome::Failed { stage: "db_write", detail: "x".into() });
        assert_eq!(stats.samples().last().unwrap().ms, None);
        assert!(!stats.samples().last().unwrap().ok);
        let len = stats.samples().len();
        record(&stats, &alerter, &CycleOutcome::Skipped("no ready SSPs"));
        assert_eq!(stats.samples().len(), len);
    }

    #[test]
    fn snapshot_shape_is_stable() {
        let c = cfg(30, 25);
        let stats = HeartbeatStats::new();

        // Never probed: scalars null rather than sentinel values.
        let v = stats.snapshot(&c, 1_000, false);
        assert_eq!(v["enabled"], false);
        assert_eq!(v["last_e2e_ms"], serde_json::Value::Null);
        assert_eq!(v["last_ok_epoch_ms"], serde_json::Value::Null);
        assert_eq!(v["consecutive_failures"], 0);
        assert_eq!(v["interval_secs"], 30);
        assert!(v.get("samples").is_none(), "samples are opt-in");

        let alerter = maintenance::alert::Alerter::new(None, None, 3);
        stats.enabled.store(true, Ordering::Relaxed);
        record(&stats, &alerter, &CycleOutcome::Ok { e2e_ms: 42 });

        let v = stats.snapshot(&c, now_epoch_ms(), true);
        assert_eq!(v["enabled"], true);
        assert_eq!(v["stale"], false);
        assert_eq!(v["last_e2e_ms"], 42);
        assert!(v["last_ok_epoch_ms"].as_u64().unwrap() > 0);
        let samples = v["samples"].as_array().expect("samples array");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0]["ms"], 42);
        assert_eq!(samples[0]["ok"], true);
        assert!(samples[0]["ts"].as_u64().unwrap() > 0);
    }

    #[test]
    fn a_blocked_probe_is_not_a_healthy_one() {
        // The bug this guards: during an SSP bootstrap the probe has nothing
        // to measure, so it recorded nothing at all — leaving the last good
        // latency on display with `stale` still false for a full grace window,
        // i.e. a healthy-looking 9ms during the one period when no client can
        // sync.
        let c = cfg(30, 25);
        let stats = HeartbeatStats::new();
        let alerter = maintenance::alert::Alerter::new(None, None, 3);
        stats.enabled.store(true, Ordering::Relaxed);

        record(&stats, &alerter, &CycleOutcome::Ok { e2e_ms: 9 });
        let now = now_epoch_ms();
        assert!(stats.is_current(&c, now), "a fresh success is current");
        assert!(stats.blocked_reason().is_none());

        record(&stats, &alerter, &CycleOutcome::Skipped("no ready SSPs"));
        assert_eq!(stats.blocked_reason(), Some("no ready SSPs"));
        assert!(
            !stats.is_current(&c, now),
            "blocked must stop the reading counting as current immediately, \
             without waiting for the staleness window"
        );
        // The number itself is still reported — consumers decide what to do
        // with it — but never as a current one.
        let v = stats.snapshot(&c, now, false);
        assert_eq!(v["blocked"], true);
        assert_eq!(v["blocked_reason"], "no ready SSPs");
        assert_eq!(v["last_e2e_ms"], 9);
        assert_eq!(v["stale"], false, "blocked is not the same as stale");

        // Recovery clears it.
        record(&stats, &alerter, &CycleOutcome::Ok { e2e_ms: 12 });
        assert!(stats.blocked_reason().is_none());
        assert!(stats.is_current(&c, now_epoch_ms()));

        // A failure is a measurement, not a block.
        record(&stats, &alerter, &CycleOutcome::Failed { stage: "db_write", detail: "x".into() });
        assert!(stats.blocked_reason().is_none());
    }

    #[test]
    fn outcome_recording_tracks_edges() {
        let stats = HeartbeatStats::new();
        let alerter = maintenance::alert::Alerter::new(None, None, 3);

        record(&stats, &alerter, &CycleOutcome::Failed {
            stage: "db_write",
            detail: "boom".into(),
        });
        record(&stats, &alerter, &CycleOutcome::Failed {
            stage: "db_write",
            detail: "boom".into(),
        });
        assert_eq!(stats.consecutive_failures.load(Ordering::Relaxed), 2);
        assert_eq!(stats.last_ok_epoch_ms.load(Ordering::Relaxed), 0);

        record(&stats, &alerter, &CycleOutcome::Ok { e2e_ms: 42 });
        assert_eq!(stats.consecutive_failures.load(Ordering::Relaxed), 0);
        assert_eq!(stats.last_e2e_ms.load(Ordering::Relaxed), 42);
        assert!(stats.last_ok_epoch_ms.load(Ordering::Relaxed) > 0);

        // Skipped cycles touch only last_attempt.
        let before = stats.last_e2e_ms.load(Ordering::Relaxed);
        record(&stats, &alerter, &CycleOutcome::Skipped("no ready SSPs"));
        assert_eq!(stats.last_e2e_ms.load(Ordering::Relaxed), before);
    }
}
