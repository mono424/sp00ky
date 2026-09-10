//! Replica-vs-upstream drift detection and auto-remediation.
//!
//! The scheduler's replica is cloned from upstream SurrealDB exactly once and
//! then kept current only by the per-row `_00_<table>_mutation` events that
//! POST to `/ingest`. Anything written upstream while nothing was listening
//! (a bulk migration with the stack down, an event whose HTTP call failed) is
//! missing from the replica forever, and every SSP that bootstraps from
//! `/proxy` inherits the gap. The existing integrity checks cannot see it:
//! `startup_integrity_check` hashes the replica against its own persisted
//! hashes, and `/ssp/bootstrap-verify` compares SSP against replica. A table
//! that is empty on both sides hashes identically on both sides.
//!
//! This module compares row COUNTS between upstream and the replica, per sync
//! table, and decides whether to re-clone. Counts, not hashes: they cost one
//! `count()` per table per check and they are exactly what `spky verify`
//! compares.
//!
//! When it runs matters as much as what it compares. Events reach the replica
//! only through `drain_and_apply`, which the snapshot updater runs every
//! `snapshot_update_interval_secs` under `drain_lock` and never while an SSP
//! is bootstrapping. Between drains a busy table's replica count legitimately
//! trails upstream by everything still buffered. So the periodic check is a
//! step of that same tick, run right after a drain that left the buffer
//! empty, and the only mismatch acted on at first sight is the one that
//! cannot be drain lag: a table with ZERO replica rows that upstream has rows
//! for. Every other mismatch has to repeat across consecutive checks.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::replica::Replica;

/// Tunables, read from the environment by [`DriftConfig::from_env`].
#[derive(Debug, Clone)]
pub struct DriftConfig {
    /// `SPKY_DRIFT_CHECK` (default true). Off disables the check entirely,
    /// including the startup pass.
    pub enabled: bool,
    /// `SPKY_DRIFT_AUTO_RECLONE` (default true). Off keeps detection and
    /// reporting but never acts.
    pub auto_reclone: bool,
    /// `SPKY_DRIFT_CONFIRM_TICKS` (default 2): consecutive checks a non-zero
    /// count mismatch must persist before it is acted on.
    pub confirm_ticks: u32,
    /// `SPKY_DRIFT_RECLONE_COOLDOWN_SECS` (default 3600): minimum spacing
    /// between two automatic re-clones.
    pub reclone_cooldown: Duration,
    /// `SPKY_DRIFT_CHECK_TIMEOUT_SECS` (default 120): deadline for one whole
    /// check. The check is the last step of the snapshot updater's tick, and
    /// that tick is a serial loop — a check that never returns stops the
    /// replica draining forever. See [`run_check`].
    pub check_timeout: Duration,
}

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_reclone: true,
            confirm_ticks: 2,
            reclone_cooldown: Duration::from_secs(3600),
            check_timeout: Duration::from_secs(120),
        }
    }
}

impl DriftConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(v) = env_bool("SPKY_DRIFT_CHECK") {
            cfg.enabled = v;
        }
        if let Some(v) = env_bool("SPKY_DRIFT_AUTO_RECLONE") {
            cfg.auto_reclone = v;
        }
        if let Some(n) = env_u64("SPKY_DRIFT_CONFIRM_TICKS") {
            cfg.confirm_ticks = n.max(1) as u32;
        }
        if let Some(n) = env_u64("SPKY_DRIFT_RECLONE_COOLDOWN_SECS") {
            cfg.reclone_cooldown = Duration::from_secs(n);
        }
        if let Some(n) = env_u64("SPKY_DRIFT_CHECK_TIMEOUT_SECS") {
            cfg.check_timeout = Duration::from_secs(n.max(1));
        }
        cfg
    }
}

fn env_bool(name: &str) -> Option<bool> {
    let v = std::env::var(name).ok()?;
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse().ok()
}

/// Where the upstream side of the comparison comes from. Production wires the
/// scheduler's upstream SurrealDB handle; tests substitute a fixed map.
#[async_trait]
pub trait UpstreamCounts: Send + Sync {
    /// Sync tables upstream (already filtered by `table_excluded_from_sync` and
    /// `@nosync`) with their row counts. A table whose count could not be read
    /// is `None`; a table that no longer exists upstream is absent.
    async fn upstream_counts(&self) -> Result<BTreeMap<String, Option<u64>>>;

    /// Called when a check was abandoned on its deadline. The handle behind
    /// it is then presumed wedged, and the next check must not inherit it.
    fn note_stalled(&self) {}
}

/// Upstream counts read through the scheduler's shared SurrealDB handle.
pub struct SurrealUpstream {
    pub db: Arc<maintenance::db::ReconnectingDb>,
}

#[async_trait]
impl UpstreamCounts for SurrealUpstream {
    async fn upstream_counts(&self) -> Result<BTreeMap<String, Option<u64>>> {
        let handle = self.db.handle();
        let tables = Replica::discover_sync_tables(&*handle)
            .await
            .context("drift: discover sync tables upstream")?;
        let mut out = BTreeMap::new();
        for table in tables {
            let count = match count_upstream(&*handle, &table).await {
                Ok(n) => Some(n),
                Err(e) => {
                    self.db.note_error(&e.to_string());
                    warn!(table = %table, error = %e, "drift: upstream count failed; table skipped this check");
                    None
                }
            };
            out.insert(table, count);
        }
        Ok(out)
    }

    fn note_stalled(&self) {
        // The HTTP engine has no request timeout, so a session the server has
        // forgotten answers nothing at all rather than erroring. Our own
        // deadline is the only evidence there is, and it is strong: a healthy
        // handle counts these tables in seconds.
        self.db.force_reconnect();
    }
}

async fn count_upstream<C: surrealdb::Connection>(
    db: &surrealdb::Surreal<C>,
    table: &str,
) -> Result<u64> {
    let mut response = db
        .query(format!("SELECT count() AS total FROM {} GROUP ALL", table))
        .await
        .with_context(|| format!("count() failed for upstream table '{}'", table))?;
    let sdk_val: surrealdb::types::Value = response
        .take(0)
        .with_context(|| format!("take(0) failed for upstream count of '{}'", table))?;
    let json = sdk_val.into_json_value();
    Ok(json
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|row| row.get("total"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0))
}

/// One table's two counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TableCounts {
    /// `None` when the upstream count could not be read this check.
    pub upstream: Option<u64>,
    pub replica: u64,
}

impl TableCounts {
    pub fn mismatched(&self) -> bool {
        matches!(self.upstream, Some(u) if u != self.replica)
    }

    /// The one shape that cannot be drain lag: nothing in the replica for a
    /// table upstream has rows in.
    pub fn replica_empty_upstream_not(&self) -> bool {
        self.replica == 0 && matches!(self.upstream, Some(u) if u > 0)
    }
}

/// The outcome of one check.
#[derive(Debug, Clone, Serialize)]
pub struct DriftReport {
    pub checked_at_epoch_ms: u64,
    pub tables: BTreeMap<String, TableCounts>,
}

impl DriftReport {
    pub fn mismatched_tables(&self) -> Vec<String> {
        self.tables
            .iter()
            .filter(|(_, c)| c.mismatched())
            .map(|(t, _)| t.clone())
            .collect()
    }
}

/// Compare upstream against the replica. The table set is upstream's: a table
/// dropped upstream but lingering in the replica is not drift the SSP can
/// serve wrong rows for, so it is skipped rather than counted.
pub async fn check_once(
    upstream: &dyn UpstreamCounts,
    replica: &Arc<RwLock<Replica>>,
) -> Result<DriftReport> {
    let upstream_counts = upstream.upstream_counts().await?;
    let mut tables = BTreeMap::new();
    {
        let rep = replica.read().await;
        for (table, upstream_count) in upstream_counts {
            let replica_count = rep.count_table(&table).await.unwrap_or(0) as u64;
            tables.insert(
                table,
                TableCounts {
                    upstream: upstream_count,
                    replica: replica_count,
                },
            );
        }
    }
    Ok(DriftReport {
        checked_at_epoch_ms: now_epoch_ms(),
        tables,
    })
}

/// What a check concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Counts agree (or nothing is actionable yet).
    Clean,
    /// Mismatches exist but are not (yet, or not allowed to be) acted on.
    Report { tables: Vec<String> },
    /// Re-clone the replica from upstream and re-bootstrap every SSP.
    Reclone { tables: Vec<String> },
}

/// Cross-check bookkeeping. Persists only for the process lifetime; a restart
/// re-runs the startup pass anyway.
#[derive(Debug, Default, Clone, Serialize)]
pub struct DriftState {
    pub last_report: Option<DriftReport>,
    /// Consecutive checks each table has been mismatched (non-zero shape).
    pub streaks: BTreeMap<String, u32>,
    /// Tables that stayed mismatched right after an automatic re-clone. Only
    /// reported from then on, until their counts change: a table the clone
    /// cannot load (a row the replica schema rejects) would otherwise re-clone
    /// the whole replica every cooldown and bounce every SSP with it.
    pub stuck: BTreeMap<String, TableCounts>,
    #[serde(skip)]
    pub last_auto_reclone: Option<Instant>,
    pub last_auto_reclone_epoch_ms: Option<u64>,
    pub auto_reclones: u64,
    /// Mismatch set of the last report, so `Report` logs only on change.
    pub last_reported: BTreeSet<String>,
    pub last_error: Option<String>,
    /// Set right after an auto re-clone; the next check compares against it
    /// to find tables the re-clone did not fix.
    #[serde(skip)]
    pub verify_after_reclone: bool,
}

/// Fold a report into the state and decide. Pure, so the escalation rules are
/// unit-testable without a replica or an upstream.
pub fn decide(report: &DriftReport, state: &mut DriftState, cfg: &DriftConfig, now: Instant) -> Action {
    let mut zero_shape: Vec<String> = Vec::new();
    let mut confirmed: Vec<String> = Vec::new();
    let mut mismatched: Vec<String> = Vec::new();

    for (table, counts) in &report.tables {
        if let Some(stuck) = state.stuck.get(table) {
            if *stuck == *counts {
                // Unchanged since the re-clone that did not fix it.
                mismatched.push(table.clone());
                continue;
            }
            state.stuck.remove(table);
        }
        if !counts.mismatched() {
            state.streaks.remove(table);
            continue;
        }
        mismatched.push(table.clone());
        if state.verify_after_reclone {
            // The re-clone just ran and this table is still off: latch it.
            state.stuck.insert(table.clone(), *counts);
            continue;
        }
        if counts.replica_empty_upstream_not() {
            zero_shape.push(table.clone());
            continue;
        }
        let streak = state.streaks.entry(table.clone()).or_insert(0);
        *streak += 1;
        if *streak >= cfg.confirm_ticks {
            confirmed.push(table.clone());
        }
    }
    state.verify_after_reclone = false;
    state.streaks.retain(|t, _| report.tables.get(t).map_or(false, |c| c.mismatched()));
    state.last_report = Some(report.clone());

    if mismatched.is_empty() {
        state.last_reported.clear();
        return Action::Clean;
    }

    let mut actionable: Vec<String> = zero_shape;
    actionable.extend(confirmed);
    actionable.sort();
    actionable.dedup();

    let in_cooldown = state
        .last_auto_reclone
        .map(|t| now.duration_since(t) < cfg.reclone_cooldown)
        .unwrap_or(false);

    if !actionable.is_empty() && cfg.auto_reclone && !in_cooldown {
        state.last_auto_reclone = Some(now);
        state.last_auto_reclone_epoch_ms = Some(now_epoch_ms());
        state.auto_reclones += 1;
        state.streaks.clear();
        state.verify_after_reclone = true;
        return Action::Reclone { tables: actionable };
    }
    Action::Report { tables: mismatched }
}

/// Log a `Report` outcome, once per change of the mismatch set.
pub fn log_report(report: &DriftReport, state: &mut DriftState, tables: &[String], cfg: &DriftConfig) {
    let set: BTreeSet<String> = tables.iter().cloned().collect();
    if set == state.last_reported {
        return;
    }
    state.last_reported = set;
    let detail: Vec<String> = tables
        .iter()
        .filter_map(|t| report.tables.get(t).map(|c| format!("{t}: upstream={:?} replica={}", c.upstream, c.replica)))
        .collect();
    let stuck: Vec<&String> = state.stuck.keys().collect();
    warn!(
        tables = ?detail,
        stuck = ?stuck,
        auto_reclone = cfg.auto_reclone,
        "Replica drift: row counts differ from upstream (not acting yet)"
    );
}

/// Everything the snapshot updater needs to run a check and act on it.
pub struct DriftHook {
    pub cfg: DriftConfig,
    pub upstream: Arc<dyn UpstreamCounts>,
    pub state: Arc<RwLock<DriftState>>,
    pub reclone: Arc<dyn Recloner>,
}

/// The remediation, abstracted so tests can observe it without an upstream.
#[async_trait]
pub trait Recloner: Send + Sync {
    /// Re-clone the replica and flag every SSP for re-bootstrap. `Ok(false)`
    /// when a re-clone was already in progress.
    async fn reclone_and_resync(&self) -> Result<bool>;
}

/// Run one check + decision + remediation. Returns the action taken.
///
/// Called by the snapshot updater AFTER a drain that emptied the buffer, with
/// `drain_lock` released (the re-clone takes the replica write lock itself and
/// can run for minutes). Also called once at startup.
pub async fn run_check(hook: &DriftHook, replica: &Arc<RwLock<Replica>>) -> Action {
    if !hook.cfg.enabled {
        return Action::Clean;
    }
    // Time-boxed. This check is the last step of the snapshot updater's tick,
    // and that tick is a serial loop: a check that never returns takes the
    // replica drain with it, forever, with nothing in the log to say so — the
    // whole pipeline goes quiet while `/health` still reports ready (observed
    // on whitepawn 2026-09-09: no drain for 3h, 11.6k events buffered, the
    // updater parked in an upstream `count()` that never answered).
    let report = match tokio::time::timeout(
        hook.cfg.check_timeout,
        check_once(&*hook.upstream, replica),
    )
    .await
    {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            warn!(error = %e, "Replica drift check failed");
            hook.state.write().await.last_error = Some(e.to_string());
            return Action::Clean;
        }
        Err(_) => {
            let secs = hook.cfg.check_timeout.as_secs();
            warn!(
                timeout_secs = secs,
                "Replica drift check timed out; abandoning it and reconnecting upstream"
            );
            hook.upstream.note_stalled();
            hook.state.write().await.last_error =
                Some(format!("drift check timed out after {secs}s"));
            return Action::Clean;
        }
    };
    let action = {
        let mut st = hook.state.write().await;
        st.last_error = None;
        let action = decide(&report, &mut st, &hook.cfg, Instant::now());
        if let Action::Report { tables } = &action {
            log_report(&report, &mut st, tables, &hook.cfg);
        }
        action
    };
    if let Action::Reclone { tables } = &action {
        let detail: Vec<String> = tables
            .iter()
            .filter_map(|t| report.tables.get(t).map(|c| format!("{t}: upstream={:?} replica={}", c.upstream, c.replica)))
            .collect();
        error!(
            tables = ?detail,
            "Replica drift: the replica is missing rows upstream has; re-cloning from upstream and re-bootstrapping every SSP"
        );
        match hook.reclone.reclone_and_resync().await {
            Ok(true) => info!(tables = ?tables, "Replica drift: re-clone complete"),
            Ok(false) => warn!("Replica drift: a re-clone was already running; will re-check next tick"),
            Err(e) => {
                error!(error = %e, "Replica drift: automatic re-clone failed");
                hook.state.write().await.last_error = Some(format!("reclone: {e}"));
            }
        }
    }
    action
}

/// JSON for `/health/snapshot` and friends.
pub fn state_json(state: &DriftState, cfg: &DriftConfig) -> serde_json::Value {
    let (checked_at, tables, mismatched) = match &state.last_report {
        Some(r) => (
            serde_json::Value::from(r.checked_at_epoch_ms),
            serde_json::to_value(&r.tables).unwrap_or(serde_json::Value::Null),
            serde_json::to_value(r.mismatched_tables()).unwrap_or(serde_json::Value::Null),
        ),
        None => (serde_json::Value::Null, serde_json::json!({}), serde_json::json!([])),
    };
    serde_json::json!({
        "enabled": cfg.enabled,
        "auto_reclone_enabled": cfg.auto_reclone,
        "checked_at": checked_at,
        "tables": tables,
        "mismatched": mismatched,
        "stuck": state.stuck.keys().collect::<Vec<_>>(),
        "last_auto_reclone": state.last_auto_reclone_epoch_ms,
        "auto_reclones": state.auto_reclones,
        "last_error": state.last_error,
    })
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(rows: &[(&str, Option<u64>, u64)]) -> DriftReport {
        DriftReport {
            checked_at_epoch_ms: 1,
            tables: rows
                .iter()
                .map(|(t, u, r)| (t.to_string(), TableCounts { upstream: *u, replica: *r }))
                .collect(),
        }
    }

    fn cfg() -> DriftConfig {
        DriftConfig::default()
    }

    /// The whole reason the check is time-boxed: an upstream query that never
    /// answers (a SurrealDB session the server has forgotten — the HTTP engine
    /// has no request timeout) used to park the snapshot updater's tick, and
    /// with it every later drain, silently.
    #[tokio::test]
    async fn a_check_that_never_answers_is_abandoned_on_its_deadline() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Hangs(Arc<AtomicBool>);
        #[async_trait]
        impl UpstreamCounts for Hangs {
            async fn upstream_counts(&self) -> Result<BTreeMap<String, Option<u64>>> {
                std::future::pending().await
            }
            fn note_stalled(&self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        struct NeverReclones;
        #[async_trait]
        impl Recloner for NeverReclones {
            async fn reclone_and_resync(&self) -> Result<bool> {
                unreachable!("a timed-out check decides nothing")
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let replica = Arc::new(RwLock::new(
            Replica::new(tmp.path().join("replica")).await.unwrap(),
        ));
        let stalled = Arc::new(AtomicBool::new(false));
        let hook = DriftHook {
            cfg: DriftConfig {
                check_timeout: Duration::from_millis(20),
                ..DriftConfig::default()
            },
            upstream: Arc::new(Hangs(Arc::clone(&stalled))),
            state: Arc::new(RwLock::new(DriftState::default())),
            reclone: Arc::new(NeverReclones),
        };

        assert_eq!(run_check(&hook, &replica).await, Action::Clean);
        assert!(
            stalled.load(Ordering::SeqCst),
            "the handle behind an abandoned check is dropped for the next one"
        );
        let err = hook.state.read().await.last_error.clone().unwrap();
        assert!(err.contains("timed out"), "{err}");
    }

    #[test]
    fn matching_counts_are_clean() {
        let mut st = DriftState::default();
        let a = decide(&report(&[("game", Some(10), 10), ("user", Some(0), 0)]), &mut st, &cfg(), Instant::now());
        assert_eq!(a, Action::Clean);
        assert!(st.streaks.is_empty());
    }

    #[test]
    fn an_empty_replica_table_reclones_on_first_sight() {
        // The observed case: contact had 5386 rows upstream, 0 in the replica.
        let mut st = DriftState::default();
        let a = decide(&report(&[("contact", Some(5386), 0), ("game", Some(7), 7)]), &mut st, &cfg(), Instant::now());
        assert_eq!(a, Action::Reclone { tables: vec!["contact".into()] });
        assert_eq!(st.auto_reclones, 1);
        assert!(st.verify_after_reclone);
    }

    #[test]
    fn a_partial_mismatch_needs_consecutive_checks() {
        let mut st = DriftState::default();
        let r = report(&[("game", Some(101), 100)]);
        assert_eq!(decide(&r, &mut st, &cfg(), Instant::now()), Action::Report { tables: vec!["game".into()] });
        assert_eq!(st.streaks["game"], 1);
        // A clean read in between resets the streak: it was drain lag.
        assert_eq!(decide(&report(&[("game", Some(101), 101)]), &mut st, &cfg(), Instant::now()), Action::Clean);
        assert!(st.streaks.is_empty());
        assert_eq!(decide(&r, &mut st, &cfg(), Instant::now()), Action::Report { tables: vec!["game".into()] });
        assert_eq!(decide(&r, &mut st, &cfg(), Instant::now()), Action::Reclone { tables: vec!["game".into()] });
    }

    #[test]
    fn unreadable_upstream_counts_are_not_drift() {
        let mut st = DriftState::default();
        let a = decide(&report(&[("game", None, 0)]), &mut st, &cfg(), Instant::now());
        assert_eq!(a, Action::Clean);
    }

    #[test]
    fn cooldown_and_disabled_downgrade_to_report() {
        let mut st = DriftState::default();
        let r = report(&[("contact", Some(5), 0)]);
        let t0 = Instant::now();
        assert!(matches!(decide(&r, &mut st, &cfg(), t0), Action::Reclone { .. }));
        // Still zero right after the re-clone: latched as stuck, reported only.
        assert_eq!(decide(&r, &mut st, &cfg(), t0 + Duration::from_secs(1)), Action::Report { tables: vec!["contact".into()] });
        assert!(st.stuck.contains_key("contact"));
        // Its counts change: the latch lifts, but the cooldown still holds.
        let r2 = report(&[("contact", Some(6), 0)]);
        assert_eq!(decide(&r2, &mut st, &cfg(), t0 + Duration::from_secs(2)), Action::Report { tables: vec!["contact".into()] });
        assert!(!st.stuck.contains_key("contact"));
        // Past the cooldown it may act again.
        assert!(matches!(decide(&r2, &mut st, &cfg(), t0 + Duration::from_secs(3601)), Action::Reclone { .. }));

        let mut off = DriftState::default();
        let c = DriftConfig { auto_reclone: false, ..DriftConfig::default() };
        assert_eq!(decide(&r, &mut off, &c, Instant::now()), Action::Report { tables: vec!["contact".into()] });
        assert_eq!(off.auto_reclones, 0);
    }

    #[test]
    fn a_fixed_table_clears_its_stuck_latch() {
        let mut st = DriftState::default();
        let t0 = Instant::now();
        decide(&report(&[("contact", Some(5), 0)]), &mut st, &cfg(), t0);
        decide(&report(&[("contact", Some(5), 0)]), &mut st, &cfg(), t0);
        assert!(st.stuck.contains_key("contact"));
        assert_eq!(decide(&report(&[("contact", Some(5), 5)]), &mut st, &cfg(), t0), Action::Clean);
        assert!(st.stuck.is_empty());
    }
}
