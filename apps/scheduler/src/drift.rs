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
//! step of that same tick, run right after a drain, over the tables that have
//! NOTHING still buffered — the check samples the busy set itself, after the
//! upstream counts and again after the replica counts, and those tables sit
//! this pass out, keeping whatever streak they had. Skipping the whole check
//! instead (the original rule) meant a tenant whose scheduler writes job rows
//! several times a second never checked at all after startup. The only
//! mismatch acted on at first sight is the one that cannot be drain lag: a
//! table with ZERO replica rows that upstream has rows for. Every other
//! mismatch has to repeat across consecutive checks.
//!
//! What it does about a confirmed mismatch is a per-table repair
//! ([`repair_table`]): diff the table's ids and `_00_rv`s against upstream and
//! push the difference through the ingest pipeline as ordinary events, so the
//! replica and every SSP converge without anyone re-bootstrapping. Only a
//! repair that cannot explain the mismatch, or would be too large, falls back
//! to re-cloning the whole replica and re-bootstrapping every SSP, which is
//! what every mismatch used to cost (whitepawn 2026-09-17: one missing
//! `puzzle` row re-cloned 196k rows and restarted the SSP). A table new to the
//! replica (see [`DriftState::new_tables`]) is never re-cloned for: its repair
//! is an uncapped backfill in a task of its own.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::replica::Replica;
use maintenance::changefeed::TailerStats;

/// Tunables, read from the environment by [`DriftConfig::from_env`].
#[derive(Debug, Clone)]
pub struct DriftConfig {
    /// `SPKY_DRIFT_CHECK` (default true). Off disables the check entirely,
    /// including the startup pass.
    pub enabled: bool,
    /// `SPKY_DRIFT_AUTO_RECLONE` (default true). Off keeps detection and
    /// reporting but never acts, neither by repair nor by re-clone.
    pub auto_reclone: bool,
    /// `SPKY_DRIFT_CONFIRM_TICKS` (default 2): consecutive checks a non-zero
    /// count mismatch must persist before it is acted on.
    pub confirm_ticks: u32,
    /// `SPKY_DRIFT_RECLONE_COOLDOWN_SECS` (default 3600): minimum spacing
    /// between two automatic re-clones. Repairs have none: each needs a
    /// fresh confirmed mismatch, and one that does not fix its table latches.
    pub reclone_cooldown: Duration,
    /// `SPKY_DRIFT_REPAIR_MAX_ROWS` (default 2000): the most rows one repair
    /// of a table the replica already held may push through the ingest
    /// pipeline. A bigger difference is cheaper as a re-clone and a bootstrap
    /// than as that many events. A table new to the replica has no cap: a
    /// re-clone would cost at least its rows plus every other table's.
    pub repair_max_rows: usize,
    /// `SPKY_DRIFT_REPAIR_TIMEOUT_SECS` (default 300): deadline for one capped
    /// table repair, for the same reason as `check_timeout`, and the longest
    /// any repair (a backfill included) may go without finishing a page.
    pub repair_timeout: Duration,
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
            repair_max_rows: 2000,
            repair_timeout: Duration::from_secs(300),
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
        if let Some(n) = env_u64("SPKY_DRIFT_REPAIR_MAX_ROWS") {
            cfg.repair_max_rows = n as usize;
        }
        if let Some(n) = env_u64("SPKY_DRIFT_REPAIR_TIMEOUT_SECS") {
            cfg.repair_timeout = Duration::from_secs(n.max(1));
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

/// Tables that received events since the last drain. Asked twice per check,
/// after the upstream counts and after the replica counts, so the set covers
/// the whole comparison window and not just its start.
#[async_trait]
pub trait BusyTables: Send + Sync {
    async fn busy_tables(&self) -> BTreeSet<String>;
}

/// A fixed set, for tests.
#[async_trait]
impl BusyTables for BTreeSet<String> {
    async fn busy_tables(&self) -> BTreeSet<String> {
        self.clone()
    }
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
    busy: &dyn BusyTables,
) -> Result<DriftReport> {
    let upstream_counts = upstream.upstream_counts().await?;
    // Sampled AFTER the upstream counts, not before them: those are one serial
    // `count()` per table, seconds on a large database, and a row written
    // upstream while they run sits in the event buffer, not the replica.
    // Sampling first read every job-table churn as drift on whitepawn
    // (2026-09-15: seven automatic re-clones in a day, each restarting every
    // SSP) — the replica a few rows AHEAD of upstream on tables whose rows had
    // just been deleted upstream.
    let mut skip = busy.busy_tables().await;
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
    // And once more: the replica counts take time as well.
    skip.extend(busy.busy_tables().await);
    tables.retain(|table, _| !skip.contains(table));
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
    /// Repair these tables in place ([`repair_table`]).
    Repair { tables: Vec<String> },
    /// Re-clone the replica from upstream and re-bootstrap every SSP: what a
    /// repair escalates to when it cannot fix a table.
    Reclone { tables: Vec<String> },
}

/// Cross-check bookkeeping. Persists only for the process lifetime; a restart
/// re-runs the startup pass anyway.
#[derive(Debug, Default, Clone, Serialize)]
pub struct DriftState {
    pub last_report: Option<DriftReport>,
    /// Consecutive checks each table has been mismatched (non-zero shape).
    pub streaks: BTreeMap<String, u32>,
    /// Tables that stayed mismatched right after an automatic repair or
    /// re-clone. Only reported from then on, until their counts change: a
    /// table neither can fix (a row the replica rejects) would otherwise be
    /// acted on forever.
    pub stuck: BTreeMap<String, TableCounts>,
    #[serde(skip)]
    pub last_auto_reclone: Option<Instant>,
    pub last_auto_reclone_epoch_ms: Option<u64>,
    pub auto_reclones: u64,
    pub last_auto_repair_epoch_ms: Option<u64>,
    /// Table repairs that pushed at least one row since start.
    pub auto_repairs: u64,
    /// Mismatch set of the last report, so `Report` logs only on change.
    pub last_reported: BTreeSet<String>,
    pub last_error: Option<String>,
    /// Tables just repaired or re-cloned; the next check that compares one
    /// latches it as stuck if it is still off.
    #[serde(skip)]
    pub verify_tables: BTreeSet<String>,
    /// Tables new to the replica since its clone: added upstream while the
    /// scheduler ran (the schema reconcile's `added`), or not in the persisted
    /// hashes at boot. Rows such a table had before its ingest hook existed (a
    /// migration seeding the table it creates, `@nosync` taken off a populated
    /// table) never reached the replica. Its repair is a backfill: uncapped,
    /// because the re-clone a cap escalates to would cost every row of every
    /// table plus a bootstrap of every SSP, and in a task of its own, so the
    /// tick keeps draining what it emits. Forgotten once the table compares
    /// clean or its backfill is done.
    pub new_tables: BTreeSet<String>,
    /// Backfills running now; their tables sit every check out.
    pub backfilling: BTreeSet<String>,
}

/// Fold a report into the state and decide. Pure, so the escalation rules are
/// unit-testable without a replica or an upstream.
pub fn decide(report: &DriftReport, state: &mut DriftState, cfg: &DriftConfig) -> Action {
    let mut actionable: Vec<String> = Vec::new();
    let mut mismatched: Vec<String> = Vec::new();

    for (table, counts) in &report.tables {
        if state.backfilling.contains(table) {
            // Its counts move with every page the backfill sends.
            continue;
        }
        let verifying = state.verify_tables.remove(table);
        if let Some(stuck) = state.stuck.get(table) {
            if *stuck == *counts {
                // Unchanged since the action that did not fix it.
                mismatched.push(table.clone());
                continue;
            }
            state.stuck.remove(table);
        }
        if !counts.mismatched() {
            state.streaks.remove(table);
            state.new_tables.remove(table);
            continue;
        }
        mismatched.push(table.clone());
        if verifying {
            // Just repaired or re-cloned and still off: latch it.
            state.streaks.remove(table);
            state.stuck.insert(table.clone(), *counts);
            continue;
        }
        if counts.replica_empty_upstream_not() {
            actionable.push(table.clone());
            continue;
        }
        let streak = state.streaks.entry(table.clone()).or_insert(0);
        *streak += 1;
        if *streak >= cfg.confirm_ticks {
            actionable.push(table.clone());
        }
    }
    // A table missing from the report was not compared this pass (its events
    // are still buffered), so its streak stands: only a table that WAS
    // compared and came back clean loses it.
    state
        .streaks
        .retain(|t, _| report.tables.get(t).map_or(true, |c| c.mismatched()));
    state.last_report = Some(report.clone());

    if mismatched.is_empty() {
        state.last_reported.clear();
        return Action::Clean;
    }
    if !actionable.is_empty() && cfg.auto_reclone {
        return Action::Repair { tables: actionable };
    }
    Action::Report { tables: mismatched }
}

/// Take the re-clone slot if the cooldown allows one now.
fn claim_reclone(state: &mut DriftState, cfg: &DriftConfig, now: Instant) -> bool {
    let in_cooldown = state
        .last_auto_reclone
        .map(|t| now.duration_since(t) < cfg.reclone_cooldown)
        .unwrap_or(false);
    if !cfg.auto_reclone || in_cooldown {
        return false;
    }
    state.last_auto_reclone = Some(now);
    state.last_auto_reclone_epoch_ms = Some(now_epoch_ms());
    state.auto_reclones += 1;
    true
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
    pub repair: Arc<dyn TableRepairer>,
    pub reclone: Arc<dyn Recloner>,
    /// Upstream's table set, followed on the same tick (see `crate::schema`).
    /// `None` in tests that exercise the drift rules alone.
    pub schema: Option<Arc<crate::schema::SchemaWatch>>,
}

/// The remediation, abstracted so tests can observe it without an upstream.
#[async_trait]
pub trait Recloner: Send + Sync {
    /// Re-clone the replica and flag every SSP for re-bootstrap. `Ok(false)`
    /// when a re-clone was already in progress.
    async fn reclone_and_resync(&self) -> Result<bool>;

    /// Whether a re-clone is running right now. It holds the replica's write
    /// lock for its whole reset and load, a minute or more.
    fn in_progress(&self) -> bool {
        false
    }
}

/// One table's in-place repair. Production is [`repair_table`] over the
/// scheduler's upstream handle; tests substitute an outcome.
#[async_trait]
pub trait TableRepairer: Send + Sync {
    /// `max_rows: None` is uncapped: the backfill of a table new to the
    /// replica.
    async fn repair(&self, table: &str, max_rows: Option<usize>) -> Result<RepairOutcome>;

    /// Called when a repair was abandoned on its deadline, like
    /// [`UpstreamCounts::note_stalled`].
    fn note_stalled(&self) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairOp {
    Create,
    Update,
    Delete,
}

impl RepairOp {
    fn as_str(&self) -> &'static str {
        match self {
            RepairOp::Create => "CREATE",
            RepairOp::Update => "UPDATE",
            RepairOp::Delete => "DELETE",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RepairStats {
    pub created: usize,
    pub updated: usize,
    pub deleted: usize,
    /// Rows re-sent because upstream changed them while the repair ran.
    pub corrected: usize,
}

impl RepairStats {
    fn add(&mut self, other: &RepairStats) {
        self.created += other.created;
        self.updated += other.updated;
        self.deleted += other.deleted;
        self.corrected += other.corrected;
    }

    fn is_empty(&self) -> bool {
        *self == RepairStats::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairOutcome {
    /// These rows were pushed through the ingest pipeline.
    Applied(RepairStats),
    /// Ids and versions agree: the repair cannot explain the count mismatch.
    NothingToDo,
    /// More than `max_rows` rows differ.
    TooLarge,
    /// The ingest side had not caught up with the upstream read in time; try
    /// again on the next check.
    Deferred,
}

/// A change the ingest pipeline holds but has not applied to the replica yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingChange {
    pub id: String,
    pub deleted: bool,
    pub rv: Option<i64>,
}

/// What a repair needs from the ingest side.
#[async_trait]
pub trait RepairFeed: Send + Sync {
    /// Wait until every change committed upstream before `unix_ms` has been
    /// ingested. `false` when that did not happen in time.
    async fn wait_ingested(&self, unix_ms: u64) -> bool;
    /// `table`'s changes that are ingested but not yet applied, in order.
    async fn pending(&self, table: &str) -> Vec<PendingChange>;
    /// Push one change through the pipeline, exactly as if the feed (or the
    /// DB event) had delivered it.
    async fn emit(&self, table: &str, op: RepairOp, id: &str, record: serde_json::Value) -> Result<()>;
}

/// The scheduler's ingest pipeline as a [`RepairFeed`].
pub struct IngestRepairFeed {
    pub ingest: crate::ingest::IngestState,
    pub changefeed: Arc<TailerStats>,
    pub changefeed_notify: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl RepairFeed for IngestRepairFeed {
    async fn wait_ingested(&self, unix_ms: u64) -> bool {
        // Over HTTP the DB event posts inside the writing transaction, so a
        // change is ingested before it is even committed. With no tail
        // running yet (the startup pass runs before it starts, and before any
        // SSP can be ready), what the tail later replays lands on rows the
        // repair already wrote and is skipped by the replica's rv check.
        if !self.changefeed.enabled.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.changefeed.caught_up_ms() < unix_ms {
            if Instant::now() >= deadline {
                return false;
            }
            self.changefeed_notify.notify_one();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        true
    }

    async fn pending(&self, table: &str) -> Vec<PendingChange> {
        self.ingest
            .event_buffer
            .read()
            .await
            .iter()
            .filter(|e| e.update.table == table)
            .map(|e| PendingChange {
                id: e.update.record_id.clone(),
                deleted: matches!(e.update.operation, crate::messages::RecordOp::Delete),
                rv: e
                    .update
                    .data
                    .as_ref()
                    .and_then(|d| d.get("_00_rv"))
                    .and_then(|v| v.as_i64()),
            })
            .collect()
    }

    async fn emit(&self, table: &str, op: RepairOp, id: &str, record: serde_json::Value) -> Result<()> {
        let request = ssp_protocol::IngestRequest {
            table: table.to_string(),
            op: op.as_str().to_string(),
            id: id.to_string(),
            record,
            job_assignee: None,
        };
        crate::ingest::ingest_event(&self.ingest, request, 0)
            .await
            .map(|_| ())
            .map_err(|(status, reason)| anyhow::anyhow!("ingest refused the repair ({status}): {reason}"))
    }
}

/// `true` when upstream holds a strictly newer version than the replica. A
/// side without a version says nothing either way.
fn upstream_newer(upstream: Option<i64>, local: Option<i64>) -> bool {
    matches!((upstream, local), (Some(u), Some(l)) if u > l)
}

/// The replica's rows with the not-yet-applied changes laid over them: what
/// the replica will hold after the next drain.
pub fn overlay_pending(
    mut rows: HashMap<String, Option<i64>>,
    pending: &[PendingChange],
) -> HashMap<String, Option<i64>> {
    for change in pending {
        if change.deleted {
            rows.remove(&change.id);
        } else {
            rows.insert(change.id.clone(), change.rv);
        }
    }
    rows
}

/// Decide a table repair. Pure.
///
/// - `before`: the replica's rows, read BEFORE upstream was.
/// - `upstream`: upstream's rows and versions.
/// - `after`: the replica's rows read AFTER upstream was and after every change
///   committed before that read was ingested, pending changes laid over.
/// - `touched`: ids with a change still pending at that point.
///
/// A row upstream but in neither local read, and with nothing pending, was
/// lost: CREATE. A row upstream at a newer version than `after`: UPDATE. A row
/// in both local reads but not upstream, and nothing pending (a pending change
/// could be its re-creation after the upstream read): DELETE. Rows that
/// appeared locally between the reads are changes the upstream read may simply
/// predate, so they are never judged.
pub fn plan_repair(
    before: &HashMap<String, Option<i64>>,
    upstream: &HashMap<String, Option<i64>>,
    after: &HashMap<String, Option<i64>>,
    touched: &HashSet<String>,
) -> Vec<(RepairOp, String)> {
    let mut plan: Vec<(RepairOp, String)> = Vec::new();
    for (id, up_rv) in upstream {
        match after.get(id) {
            None if !before.contains_key(id) && !touched.contains(id) => {
                plan.push((RepairOp::Create, id.clone()));
            }
            Some(local) if upstream_newer(*up_rv, *local) => {
                plan.push((RepairOp::Update, id.clone()));
            }
            _ => {}
        }
    }
    for id in before.keys() {
        if !upstream.contains_key(id) && after.contains_key(id) && !touched.contains(id) {
            plan.push((RepairOp::Delete, id.clone()));
        }
    }
    plan.sort_by(|a, b| a.1.cmp(&b.1));
    plan
}

/// Re-read after the repair's events are in the pipeline: anything upstream
/// changed since its first read gets the row as it is now. Pure.
///
/// `emitted` carries the version each CREATE/UPDATE was sent at; `current` is
/// upstream now, keyed by id. Only UPDATE and DELETE come out: a row that
/// exists locally by now must not be re-created.
pub fn plan_corrections(
    emitted: &[(RepairOp, String, Option<i64>)],
    current: &HashMap<String, serde_json::Value>,
) -> Vec<(RepairOp, String)> {
    let mut out = Vec::new();
    for (op, id, sent_rv) in emitted {
        let now = current.get(id);
        match (op, now) {
            (RepairOp::Delete, Some(_)) => out.push((RepairOp::Update, id.clone())),
            (RepairOp::Delete, None) => {}
            (_, None) => out.push((RepairOp::Delete, id.clone())),
            (_, Some(row)) => {
                let now_rv = row.get("_00_rv").and_then(|v| v.as_i64());
                if now_rv != *sent_rv {
                    out.push((RepairOp::Update, id.clone()));
                }
            }
        }
    }
    out
}

/// Repair one table in place: push the rows the replica is missing, holds
/// stale, or holds but upstream deleted, through the ingest pipeline, so the
/// replica AND every SSP receive them as ordinary events.
///
/// Page by page, so a table of any size costs one page of memory: `max_rows`
/// caps a repair of a table the replica already held (a big difference there
/// is cheaper as a re-clone), `None` is the backfill of a table new to the
/// replica, which a re-clone could only ever cost more (see
/// [`DriftState::new_tables`]). Every step is ordered against the live
/// pipeline:
/// 1. read the replica's ids, once;
/// 2. per upstream page: wait until everything committed before that page was
///    read has been ingested, so a row still in flight is not mistaken for a
///    lost one; read the page's pending changes, then the replica's versions
///    for the page's ids (in that order, so a drain in between shows up in
///    the second read); plan creates and updates; emit; then re-read the
///    planned rows upstream and emit corrections;
/// 3. after the last page, the same for deletes: rows the replica held that
///    no page had.
///
/// The corrections are what make it safe to emit at all: a change committed
/// before a re-read is caught by it; one committed after is ingested after
/// the repair's events and overrides them. That matters because an SSP
/// applies a repeated or stale row as new, and a CREATE that raced a DELETE
/// would resurrect the row.
pub async fn repair_table<C: surrealdb::Connection>(
    upstream: &surrealdb::Surreal<C>,
    replica: &Arc<RwLock<Replica>>,
    feed: &dyn RepairFeed,
    table: &str,
    max_rows: Option<usize>,
    stall: Duration,
) -> Result<RepairOutcome> {
    let (omit, before) = {
        let rep = replica.read().await;
        (rep.omit_for(table).clone(), rep.row_versions(table).await?)
    };

    /// What the pages have done so far.
    #[derive(Default)]
    struct Pass {
        up: HashMap<String, Option<i64>>,
        planned: usize,
        stats: RepairStats,
        stop: Option<RepairOutcome>,
    }
    let pass = tokio::sync::Mutex::new(Pass::default());
    // No deadline for the whole repair (a backfill takes as long as its table
    // does), but one for progress: a page that never comes back is a session
    // the server forgot, which answers nothing rather than erroring.
    let progress = std::sync::atomic::AtomicU64::new(now_epoch_ms());
    let paging = Replica::page_table(upstream, table, &omit, |page| {
        let (pass, before, omit, progress) = (&pass, &before, &omit, &progress);
        async move {
            let mut pass = pass.lock().await;
            let mut page_up: HashMap<String, Option<i64>> = HashMap::new();
            let mut rows: HashMap<String, serde_json::Value> = HashMap::new();
            for row in page {
                let Some(id) = row.get("id").and_then(|v| v.as_str()).map(str::to_owned) else {
                    continue;
                };
                let rv = row.get("_00_rv").and_then(|v| v.as_i64());
                pass.up.insert(id.clone(), rv);
                page_up.insert(id.clone(), rv);
                if before.get(&id).map_or(true, |local| upstream_newer(rv, *local)) {
                    rows.insert(id, row);
                }
            }
            if rows.is_empty() {
                return Ok(());
            }
            pass.planned += rows.len();
            if max_rows.is_some_and(|max| pass.planned > max) {
                pass.stop = Some(RepairOutcome::TooLarge);
                anyhow::bail!("repair too large");
            }
            let read_at = now_epoch_ms();
            if !feed.wait_ingested(read_at).await {
                pass.stop = Some(RepairOutcome::Deferred);
                anyhow::bail!("repair deferred");
            }
            let ids: Vec<String> = page_up.keys().cloned().collect();
            let pending: Vec<PendingChange> = feed
                .pending(table)
                .await
                .into_iter()
                .filter(|c| page_up.contains_key(&c.id))
                .collect();
            let local = replica.read().await.row_versions_for(table, &ids).await?;
            let after = overlay_pending(local, &pending);
            let touched: HashSet<String> = pending.iter().map(|c| c.id.clone()).collect();
            let page_before: HashMap<String, Option<i64>> = before
                .iter()
                .filter(|(id, _)| page_up.contains_key(*id))
                .map(|(id, rv)| (id.clone(), *rv))
                .collect();
            let plan = plan_repair(&page_before, &page_up, &after, &touched);
            let stats = emit_plan(upstream, replica, feed, table, omit, &plan, &mut rows).await?;
            pass.stats.add(&stats);
            progress.store(now_epoch_ms(), std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    });
    let watchdog = async {
        loop {
            tokio::time::sleep(Duration::from_secs(1).min(stall)).await;
            let idle = now_epoch_ms().saturating_sub(progress.load(std::sync::atomic::Ordering::Relaxed));
            if idle >= stall.as_millis() as u64 {
                return;
            }
        }
    };
    let paged = tokio::select! {
        paged = paging => paged,
        () = watchdog => Err(anyhow::anyhow!("repair of {table} made no progress for {}s", stall.as_secs())),
    };
    let mut pass = pass.into_inner();
    if let Some(outcome) = pass.stop.take() {
        return Ok(outcome);
    }
    paged.with_context(|| format!("repair: page upstream {table}"))?;

    // Deletes: rows the replica held before the repair that no page had.
    if !feed.wait_ingested(now_epoch_ms()).await {
        return Ok(if pass.stats.is_empty() { RepairOutcome::Deferred } else { RepairOutcome::Applied(pass.stats) });
    }
    let pending = feed.pending(table).await;
    let after = overlay_pending(replica.read().await.row_versions(table).await?, &pending);
    let touched: HashSet<String> = pending.iter().map(|c| c.id.clone()).collect();
    let deletes: Vec<(RepairOp, String)> = plan_repair(&before, &pass.up, &after, &touched)
        .into_iter()
        .filter(|(op, _)| *op == RepairOp::Delete)
        .collect();
    if max_rows.is_some_and(|max| pass.planned + deletes.len() > max) {
        return Ok(RepairOutcome::TooLarge);
    }
    let stats = tokio::time::timeout(stall, emit_plan(upstream, replica, feed, table, &omit, &deletes, &mut HashMap::new()))
        .await
        .map_err(|_| anyhow::anyhow!("repair of {table}: the delete pass made no progress for {}s", stall.as_secs()))??;
    pass.stats.add(&stats);

    if pass.stats.is_empty() {
        Ok(RepairOutcome::NothingToDo)
    } else {
        Ok(RepairOutcome::Applied(pass.stats))
    }
}

/// Emit one repair plan and then its corrections (see [`repair_table`]).
/// `rows` holds the upstream bodies of the planned CREATEs and UPDATEs.
async fn emit_plan<C: surrealdb::Connection>(
    upstream: &surrealdb::Surreal<C>,
    replica: &Arc<RwLock<Replica>>,
    feed: &dyn RepairFeed,
    table: &str,
    omit: &BTreeSet<String>,
    plan: &[(RepairOp, String)],
    rows: &mut HashMap<String, serde_json::Value>,
) -> Result<RepairStats> {
    let mut stats = RepairStats::default();
    let mut emitted: Vec<(RepairOp, String, Option<i64>)> = Vec::with_capacity(plan.len());
    for (op, id) in plan {
        let record = match op {
            RepairOp::Delete => {
                // The SSPs' delete path reads the row's owner from it.
                let row = replica.read().await.row(table, id).await.ok().flatten();
                row.unwrap_or_else(|| serde_json::json!({}))
            }
            _ => match rows.get(id) {
                Some(row) => row.clone(),
                None => {
                    warn!(table, id, "repair: planned row was not kept from the upstream read; skipped");
                    continue;
                }
            },
        };
        let rv = record.get("_00_rv").and_then(|v| v.as_i64());
        if *op == RepairOp::Delete {
            // Kept as the before-image, should a correction delete it again.
            rows.insert(id.clone(), record.clone());
        }
        feed.emit(table, *op, id, record).await?;
        match op {
            RepairOp::Create => stats.created += 1,
            RepairOp::Update => stats.updated += 1,
            RepairOp::Delete => stats.deleted += 1,
        }
        emitted.push((*op, id.clone(), if *op == RepairOp::Delete { None } else { rv }));
    }
    if emitted.is_empty() {
        return Ok(stats);
    }

    let ids: Vec<String> = emitted.iter().map(|(_, id, _)| id.clone()).collect();
    let current = Replica::fetch_rows_by_id(upstream, table, &ids, omit)
        .await
        .with_context(|| format!("repair: re-read {table}"))?;
    for (op, id) in plan_corrections(&emitted, &current) {
        let record = match op {
            RepairOp::Delete => rows.get(&id).cloned().unwrap_or_else(|| serde_json::json!({})),
            _ => match current.get(&id) {
                Some(row) => row.clone(),
                None => continue,
            },
        };
        feed.emit(table, op, &id, record).await?;
        stats.corrected += 1;
    }
    Ok(stats)
}

/// Run one check + decision + remediation. Returns the action taken.
///
/// Called by the snapshot updater after each drain, with the event buffer as
/// the busy source and `drain_lock` released (a re-clone takes the replica
/// write lock itself and can run for minutes). Also called once at startup.
pub async fn run_check(
    hook: &DriftHook,
    replica: &Arc<RwLock<Replica>>,
    busy: &dyn BusyTables,
) -> Action {
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
        check_once(&*hook.upstream, replica, busy),
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
        let action = decide(&report, &mut st, &hook.cfg);
        if let Action::Report { tables } = &action {
            log_report(&report, &mut st, tables, &hook.cfg);
        }
        action
    };
    let Action::Repair { tables } = &action else {
        return action;
    };
    let detail = |t: &str| {
        report
            .tables
            .get(t)
            .map(|c| format!("{t}: upstream={:?} replica={}", c.upstream, c.replica))
            .unwrap_or_else(|| t.to_string())
    };

    let mut escalate: Vec<String> = Vec::new();
    for table in tables {
        {
            let mut st = hook.state.write().await;
            if st.new_tables.contains(table) {
                if st.backfilling.insert(table.clone()) {
                    warn!(table = %detail(table), "Replica drift: table new to the replica; backfilling it in place");
                    spawn_backfill(hook, table.clone());
                }
                continue;
            }
        }
        warn!(table = %detail(table), "Replica drift: repairing the table in place");
        let outcome = tokio::time::timeout(
            hook.cfg.repair_timeout,
            hook.repair.repair(table, Some(hook.cfg.repair_max_rows)),
        )
        .await;
        let mut st = hook.state.write().await;
        match outcome {
            Ok(Ok(RepairOutcome::Applied(stats))) => {
                info!(table = %table, ?stats, "Replica drift: table repaired");
                crate::admin::incidents::emit(
                    "scheduler",
                    "drift_repair",
                    "recorded",
                    &format!(
                        "Replica drift on {}: repaired in place ({} created, {} updated, {} deleted, {} corrected)",
                        detail(table), stats.created, stats.updated, stats.deleted, stats.corrected
                    ),
                    None,
                );
                st.streaks.remove(table);
                st.verify_tables.insert(table.clone());
                st.auto_repairs += 1;
                st.last_auto_repair_epoch_ms = Some(now_epoch_ms());
            }
            Ok(Ok(RepairOutcome::Deferred)) => {
                // The streak stands, so the next check tries again.
                info!(table = %table, "Replica drift: ingest had not caught up with the repair's read; retrying next check");
            }
            Err(_) => {
                warn!(table = %table, timeout_secs = hook.cfg.repair_timeout.as_secs(), "Replica drift: repair timed out; reconnecting upstream and retrying next check");
                hook.repair.note_stalled();
                st.last_error = Some(format!("repair of {table} timed out"));
            }
            Ok(Ok(RepairOutcome::NothingToDo)) => {
                warn!(table = %table, "Replica drift: ids and versions agree with upstream, the repair cannot explain the counts");
                escalate.push(table.clone());
            }
            Ok(Ok(RepairOutcome::TooLarge)) => {
                warn!(table = %table, max_rows = hook.cfg.repair_max_rows, "Replica drift: too many rows differ to repair in place");
                escalate.push(table.clone());
            }
            Ok(Err(e)) => {
                error!(table = %table, error = %e, "Replica drift: repair failed");
                st.last_error = Some(format!("repair of {table}: {e}"));
                escalate.push(table.clone());
            }
        }
    }
    if escalate.is_empty() {
        return action;
    }

    if !claim_reclone(&mut *hook.state.write().await, &hook.cfg, Instant::now()) {
        warn!(tables = ?escalate, "Replica drift: a re-clone is due but the cooldown holds; retrying next check");
        return action;
    }
    let details: Vec<String> = escalate.iter().map(|t| detail(t)).collect();
    error!(
        tables = ?details,
        "Replica drift: the replica is missing rows upstream has; re-cloning from upstream and re-bootstrapping every SSP"
    );
    match hook.reclone.reclone_and_resync().await {
        Ok(true) => {
            info!(tables = ?escalate, "Replica drift: re-clone complete");
            let mut st = hook.state.write().await;
            for t in &escalate {
                st.streaks.remove(t);
                st.verify_tables.insert(t.clone());
            }
        }
        Ok(false) => warn!("Replica drift: a re-clone was already running; will re-check next tick"),
        Err(e) => {
            error!(error = %e, "Replica drift: automatic re-clone failed");
            hook.state.write().await.last_error = Some(format!("reclone: {e}"));
        }
    }
    Action::Reclone { tables: escalate }
}

/// Backfill a table new to the replica ([`DriftState::new_tables`]) in a task
/// of its own: uncapped, paced by its own progress deadline rather than by
/// the check's, and never escalated to a re-clone. A failed or deferred
/// backfill is retried by the next check that sees the table still off.
fn spawn_backfill(hook: &DriftHook, table: String) {
    let repair = Arc::clone(&hook.repair);
    let state = Arc::clone(&hook.state);
    tokio::spawn(async move {
        let outcome = repair.repair(&table, None).await;
        let mut st = state.write().await;
        st.backfilling.remove(&table);
        match outcome {
            Ok(RepairOutcome::Applied(stats)) => {
                info!(table = %table, ?stats, "Replica drift: new table backfilled");
                crate::admin::incidents::emit(
                    "scheduler",
                    "drift_repair",
                    "recorded",
                    &format!(
                        "Table `{table}` was new to the replica: backfilled in place ({} created, {} updated, {} deleted, {} corrected)",
                        stats.created, stats.updated, stats.deleted, stats.corrected
                    ),
                    None,
                );
                st.new_tables.remove(&table);
                st.streaks.remove(&table);
                st.verify_tables.insert(table);
                st.auto_repairs += 1;
                st.last_auto_repair_epoch_ms = Some(now_epoch_ms());
            }
            Ok(RepairOutcome::NothingToDo) => {
                st.new_tables.remove(&table);
            }
            Ok(RepairOutcome::Deferred) | Ok(RepairOutcome::TooLarge) => {
                info!(table = %table, "Replica drift: backfill deferred; retrying next check");
            }
            Err(e) => {
                repair.note_stalled();
                error!(table = %table, error = %e, "Replica drift: backfill failed; retrying next check");
                st.last_error = Some(format!("backfill of {table}: {e}"));
            }
        }
    });
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
        "last_auto_repair": state.last_auto_repair_epoch_ms,
        "auto_repairs": state.auto_repairs,
        "repair_max_rows": cfg.repair_max_rows,
        "backfilling": state.backfilling.iter().collect::<Vec<_>>(),
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
        #[async_trait]
        impl TableRepairer for NeverReclones {
            async fn repair(&self, _table: &str, _max_rows: Option<usize>) -> Result<RepairOutcome> {
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
            repair: Arc::new(NeverReclones),
            reclone: Arc::new(NeverReclones),
            schema: None,
        };

        assert_eq!(run_check(&hook, &replica, &BTreeSet::new()).await, Action::Clean);
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
        let a = decide(&report(&[("game", Some(10), 10), ("user", Some(0), 0)]), &mut st, &cfg());
        assert_eq!(a, Action::Clean);
        assert!(st.streaks.is_empty());
    }

    #[test]
    fn an_empty_replica_table_is_acted_on_at_first_sight() {
        // The observed case: contact had 5386 rows upstream, 0 in the replica.
        let mut st = DriftState::default();
        let a = decide(&report(&[("contact", Some(5386), 0), ("game", Some(7), 7)]), &mut st, &cfg());
        assert_eq!(a, Action::Repair { tables: vec!["contact".into()] });
    }

    #[test]
    fn a_partial_mismatch_needs_consecutive_checks() {
        let mut st = DriftState::default();
        let r = report(&[("game", Some(101), 100)]);
        assert_eq!(decide(&r, &mut st, &cfg()), Action::Report { tables: vec!["game".into()] });
        assert_eq!(st.streaks["game"], 1);
        // A clean read in between resets the streak: it was drain lag.
        assert_eq!(decide(&report(&[("game", Some(101), 101)]), &mut st, &cfg()), Action::Clean);
        assert!(st.streaks.is_empty());
        assert_eq!(decide(&r, &mut st, &cfg()), Action::Report { tables: vec!["game".into()] });
        assert_eq!(decide(&r, &mut st, &cfg()), Action::Repair { tables: vec!["game".into()] });
    }

    #[tokio::test]
    async fn a_table_with_events_still_buffered_sits_the_pass_out() {
        // The whole point of the per-table gate: on a tenant whose scheduler
        // writes job rows several times a second, `job` is always busy and
        // would otherwise take every other table's check down with it.
        struct Fixed;
        #[async_trait]
        impl UpstreamCounts for Fixed {
            async fn upstream_counts(&self) -> Result<BTreeMap<String, Option<u64>>> {
                Ok([("game".to_string(), Some(0u64)), ("job".to_string(), Some(9u64))]
                    .into_iter()
                    .collect())
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let replica = Arc::new(RwLock::new(
            Replica::new(tmp.path().join("replica")).await.unwrap(),
        ));
        let busy: BTreeSet<String> = ["job".to_string()].into_iter().collect();

        let report = check_once(&Fixed, &replica, &busy).await.unwrap();
        assert!(report.tables.contains_key("game"), "an idle table is still compared");
        assert!(
            !report.tables.contains_key("job"),
            "a table with buffered events is not judged on counts that cannot agree yet"
        );
    }

    #[tokio::test]
    async fn a_table_that_turns_busy_while_the_check_runs_is_not_judged() {
        // The whitepawn 2026-09-15 shape: a job row is deleted upstream while
        // the upstream counts are still running, so upstream reads one row
        // fewer than the replica drained a moment earlier. The busy set taken
        // before the counts did not contain the table; the one taken after
        // does, and that is the one that must win.
        struct Fixed;
        #[async_trait]
        impl UpstreamCounts for Fixed {
            async fn upstream_counts(&self) -> Result<BTreeMap<String, Option<u64>>> {
                Ok([("game".to_string(), Some(0u64)), ("job".to_string(), Some(9u64))]
                    .into_iter()
                    .collect())
            }
        }
        /// Empty the first time it is asked, `job` from then on.
        struct LandsLate(std::sync::atomic::AtomicUsize);
        #[async_trait]
        impl BusyTables for LandsLate {
            async fn busy_tables(&self) -> BTreeSet<String> {
                if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    return BTreeSet::new();
                }
                ["job".to_string()].into_iter().collect()
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let replica = Arc::new(RwLock::new(
            Replica::new(tmp.path().join("replica")).await.unwrap(),
        ));
        let busy = LandsLate(std::sync::atomic::AtomicUsize::new(0));

        let report = check_once(&Fixed, &replica, &busy).await.unwrap();
        assert!(report.tables.contains_key("game"), "an idle table is still compared");
        assert!(
            !report.tables.contains_key("job"),
            "a table that received an event during the check is not judged this pass"
        );
        assert_eq!(busy.0.load(std::sync::atomic::Ordering::SeqCst), 2, "sampled around the replica counts");
    }

    #[test]
    fn a_skipped_table_keeps_the_streak_it_had() {
        // Absent from the report means "not compared", not "clean": dropping
        // the streak there would let a table that is busy every other pass
        // never reach `confirm_ticks`.
        let mut st = DriftState::default();
        let r = report(&[("game", Some(101), 100)]);
        assert_eq!(decide(&r, &mut st, &cfg()), Action::Report { tables: vec!["game".into()] });
        assert_eq!(st.streaks["game"], 1);

        // `game` is busy this pass, so it is not in the report at all.
        let a = decide(&report(&[("user", Some(3), 3)]), &mut st, &cfg());
        assert_eq!(a, Action::Clean);
        assert_eq!(st.streaks["game"], 1, "the streak survives a pass that skipped the table");

        // Back in the report and still off: this is the second sighting.
        assert_eq!(decide(&r, &mut st, &cfg()), Action::Repair { tables: vec!["game".into()] });
    }

    #[test]
    fn unreadable_upstream_counts_are_not_drift() {
        let mut st = DriftState::default();
        let a = decide(&report(&[("game", None, 0)]), &mut st, &cfg());
        assert_eq!(a, Action::Clean);
    }

    #[test]
    fn a_table_still_off_after_its_repair_latches_and_disabled_only_reports() {
        let mut st = DriftState::default();
        let r = report(&[("contact", Some(5), 0)]);
        assert!(matches!(decide(&r, &mut st, &cfg()), Action::Repair { .. }));
        // run_check marks what it repaired.
        st.verify_tables.insert("contact".into());
        // Still zero right after: latched as stuck, reported only.
        assert_eq!(decide(&r, &mut st, &cfg()), Action::Report { tables: vec!["contact".into()] });
        assert!(st.stuck.contains_key("contact"));
        assert_eq!(decide(&r, &mut st, &cfg()), Action::Report { tables: vec!["contact".into()] });
        // Its counts change: the latch lifts and it may be acted on again.
        let r2 = report(&[("contact", Some(6), 0)]);
        assert!(matches!(decide(&r2, &mut st, &cfg()), Action::Repair { .. }));
        assert!(!st.stuck.contains_key("contact"));

        let mut off = DriftState::default();
        let c = DriftConfig { auto_reclone: false, ..DriftConfig::default() };
        assert_eq!(decide(&r, &mut off, &c), Action::Report { tables: vec!["contact".into()] });
    }

    #[test]
    fn a_verify_mark_waits_for_a_pass_that_compares_the_table() {
        let mut st = DriftState::default();
        st.verify_tables.insert("game".into());
        // `game` busy: not in the report, the mark stays.
        decide(&report(&[("user", Some(1), 1)]), &mut st, &cfg());
        assert!(st.verify_tables.contains("game"));
        decide(&report(&[("game", Some(2), 2)]), &mut st, &cfg());
        assert!(st.verify_tables.is_empty());
    }

    #[test]
    fn a_fixed_table_clears_its_stuck_latch() {
        let mut st = DriftState::default();
        st.verify_tables.insert("contact".into());
        decide(&report(&[("contact", Some(5), 0)]), &mut st, &cfg());
        assert!(st.stuck.contains_key("contact"));
        assert_eq!(decide(&report(&[("contact", Some(5), 5)]), &mut st, &cfg()), Action::Clean);
        assert!(st.stuck.is_empty());
    }

    fn versions(rows: &[(&str, Option<i64>)]) -> HashMap<String, Option<i64>> {
        rows.iter().map(|(id, rv)| (id.to_string(), *rv)).collect()
    }

    fn ids(rows: &[&str]) -> HashSet<String> {
        rows.iter().map(|id| id.to_string()).collect()
    }

    #[test]
    fn repair_plan_finds_lost_stale_and_deleted_rows() {
        let before = versions(&[("p:kept", Some(1)), ("p:stale", Some(1)), ("p:gone", Some(1))]);
        let upstream = versions(&[("p:kept", Some(1)), ("p:stale", Some(3)), ("p:lost", Some(2))]);
        let after = before.clone();
        let plan = plan_repair(&before, &upstream, &after, &HashSet::new());
        assert_eq!(
            plan,
            vec![
                (RepairOp::Delete, "p:gone".to_string()),
                (RepairOp::Create, "p:lost".to_string()),
                (RepairOp::Update, "p:stale".to_string()),
            ]
        );
    }

    #[test]
    fn repair_plan_leaves_in_flight_changes_to_the_pipeline() {
        let before = versions(&[("p:old", Some(1)), ("p:recreated", Some(1))]);
        // Upstream read: `new` just committed, `old` and `recreated` deleted.
        let upstream = versions(&[("p:new", Some(1)), ("p:moved_on", Some(5))]);
        // After catching up: `new` arrived (pending), `old` delete is pending,
        // `recreated` came back after the upstream read, `moved_on` was
        // created and updated past the read.
        let pending = vec![
            PendingChange { id: "p:new".into(), deleted: false, rv: Some(1) },
            PendingChange { id: "p:old".into(), deleted: true, rv: None },
            PendingChange { id: "p:recreated".into(), deleted: false, rv: Some(2) },
            PendingChange { id: "p:moved_on".into(), deleted: false, rv: Some(6) },
        ];
        let after = overlay_pending(before.clone(), &pending);
        let touched: HashSet<String> = pending.iter().map(|c| c.id.clone()).collect();
        assert_eq!(after.get("p:moved_on"), Some(&Some(6)));
        assert!(!after.contains_key("p:old"));
        assert!(plan_repair(&before, &upstream, &after, &touched).is_empty());

        // A row the replica had at the first read but lost by the second is a
        // local delete the upstream read predates: never re-created.
        let before = versions(&[("p:x", Some(1))]);
        let upstream = versions(&[("p:x", Some(1))]);
        assert!(plan_repair(&before, &upstream, &HashMap::new(), &ids(&[])).is_empty());
        // Rows without versions are judged on presence only.
        let before = versions(&[("p:y", None)]);
        let upstream = versions(&[("p:y", Some(4))]);
        assert!(plan_repair(&before, &upstream, &before, &ids(&[])).is_empty());
    }

    #[test]
    fn corrections_follow_what_upstream_did_during_the_repair() {
        let emitted = vec![
            (RepairOp::Create, "p:same".to_string(), Some(2)),
            (RepairOp::Create, "p:deleted_since".to_string(), Some(2)),
            (RepairOp::Update, "p:updated_since".to_string(), Some(3)),
            (RepairOp::Delete, "p:recreated_since".to_string(), None),
            (RepairOp::Delete, "p:still_gone".to_string(), None),
        ];
        let current: HashMap<String, serde_json::Value> = [
            ("p:same", serde_json::json!({"id": "p:same", "_00_rv": 2})),
            ("p:updated_since", serde_json::json!({"id": "p:updated_since", "_00_rv": 4})),
            ("p:recreated_since", serde_json::json!({"id": "p:recreated_since", "_00_rv": 1})),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(
            plan_corrections(&emitted, &current),
            vec![
                (RepairOp::Delete, "p:deleted_since".to_string()),
                (RepairOp::Update, "p:updated_since".to_string()),
                (RepairOp::Update, "p:recreated_since".to_string()),
            ]
        );
    }

    struct FixedCounts(BTreeMap<String, Option<u64>>);
    #[async_trait]
    impl UpstreamCounts for FixedCounts {
        async fn upstream_counts(&self) -> Result<BTreeMap<String, Option<u64>>> {
            Ok(self.0.clone())
        }
    }

    struct Scripted {
        outcome: std::sync::Mutex<Option<Result<RepairOutcome>>>,
        repairs: std::sync::atomic::AtomicUsize,
        reclones: std::sync::atomic::AtomicUsize,
        /// The cap each repair was called with.
        caps: std::sync::Mutex<Vec<Option<usize>>>,
    }

    impl Scripted {
        fn new(outcome: Result<RepairOutcome>) -> Arc<Self> {
            Arc::new(Self {
                outcome: std::sync::Mutex::new(Some(outcome)),
                repairs: Default::default(),
                reclones: Default::default(),
                caps: Default::default(),
            })
        }
    }

    #[async_trait]
    impl TableRepairer for Scripted {
        async fn repair(&self, _table: &str, max_rows: Option<usize>) -> Result<RepairOutcome> {
            self.caps.lock().unwrap().push(max_rows);
            self.repairs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut slot = self.outcome.lock().unwrap();
            match slot.as_ref() {
                Some(Ok(o)) => Ok(o.clone()),
                Some(Err(_)) => Err(slot.take().unwrap().unwrap_err()),
                None => Err(anyhow::anyhow!("already failed")),
            }
        }
    }

    #[async_trait]
    impl Recloner for Scripted {
        async fn reclone_and_resync(&self) -> Result<bool> {
            self.reclones.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(true)
        }
    }

    async fn drifted_hook(script: Arc<Scripted>) -> (DriftHook, Arc<RwLock<Replica>>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let replica = Arc::new(RwLock::new(
            Replica::new(tmp.path().join("replica")).await.unwrap(),
        ));
        let hook = DriftHook {
            cfg: DriftConfig::default(),
            // Nothing in the replica: acted on at first sight.
            upstream: Arc::new(FixedCounts([("puzzle".to_string(), Some(48u64))].into_iter().collect())),
            state: Arc::new(RwLock::new(DriftState::default())),
            repair: script.clone(),
            reclone: script,
            schema: None,
        };
        (hook, replica, tmp)
    }

    #[tokio::test]
    async fn a_repaired_table_costs_no_reclone() {
        let script = Scripted::new(Ok(RepairOutcome::Applied(RepairStats { created: 1, ..Default::default() })));
        let (hook, replica, _tmp) = drifted_hook(script.clone()).await;
        let action = run_check(&hook, &replica, &BTreeSet::new()).await;
        assert_eq!(action, Action::Repair { tables: vec!["puzzle".into()] });
        assert_eq!(script.reclones.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(*script.caps.lock().unwrap(), vec![Some(2000)], "a table the replica held stays capped");
        let st = hook.state.read().await;
        assert_eq!(st.auto_repairs, 1);
        assert_eq!(st.auto_reclones, 0);
        assert!(st.verify_tables.contains("puzzle"));
    }

    #[tokio::test]
    async fn a_new_table_is_backfilled_uncapped_and_never_recloned() {
        for outcome in [Ok(RepairOutcome::TooLarge), Err(anyhow::anyhow!("boom")), Ok(RepairOutcome::Applied(RepairStats { created: 48, ..Default::default() }))] {
            let applied = matches!(outcome, Ok(RepairOutcome::Applied(_)));
            let script = Scripted::new(outcome);
            let (hook, replica, _tmp) = drifted_hook(script.clone()).await;
            hook.state.write().await.new_tables.insert("puzzle".into());

            let action = run_check(&hook, &replica, &BTreeSet::new()).await;
            assert_eq!(action, Action::Repair { tables: vec!["puzzle".into()] });
            for _ in 0..200 {
                if hook.state.read().await.backfilling.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(*script.caps.lock().unwrap(), vec![None], "uncapped");
            assert_eq!(script.reclones.load(std::sync::atomic::Ordering::SeqCst), 0, "never a re-clone");
            let st = hook.state.read().await;
            assert!(st.backfilling.is_empty());
            assert_eq!(st.new_tables.contains("puzzle"), !applied, "kept for a retry until it lands");
        }
    }

    #[test]
    fn a_table_being_backfilled_sits_the_check_out() {
        let mut st = DriftState::default();
        st.backfilling.insert("puzzle".into());
        assert_eq!(decide(&report(&[("puzzle", Some(48), 3)]), &mut st, &cfg()), Action::Clean);
        st.backfilling.clear();
        st.new_tables.insert("puzzle".into());
        decide(&report(&[("puzzle", Some(48), 48)]), &mut st, &cfg());
        assert!(st.new_tables.is_empty(), "a new table that compares clean is no longer new");
    }

    #[tokio::test]
    async fn a_repair_that_cannot_help_escalates_to_one_reclone_per_cooldown() {
        for outcome in [Ok(RepairOutcome::NothingToDo), Ok(RepairOutcome::TooLarge), Err(anyhow::anyhow!("boom"))] {
            let script = Scripted::new(outcome);
            let (hook, replica, _tmp) = drifted_hook(script.clone()).await;
            let action = run_check(&hook, &replica, &BTreeSet::new()).await;
            assert_eq!(action, Action::Reclone { tables: vec!["puzzle".into()] });
            assert_eq!(script.reclones.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(hook.state.read().await.auto_reclones, 1);

            // The re-clone did not fix it: latched, nothing more happens.
            run_check(&hook, &replica, &BTreeSet::new()).await;
            assert!(hook.state.read().await.stuck.contains_key("puzzle"));
            assert_eq!(script.repairs.load(std::sync::atomic::Ordering::SeqCst), 1);
        }

        // Inside the cooldown the repair is retried, the re-clone is not.
        let script = Scripted::new(Ok(RepairOutcome::TooLarge));
        let (hook, replica, _tmp) = drifted_hook(script.clone()).await;
        hook.state.write().await.last_auto_reclone = Some(Instant::now());
        let action = run_check(&hook, &replica, &BTreeSet::new()).await;
        assert_eq!(action, Action::Repair { tables: vec!["puzzle".into()] });
        assert_eq!(script.reclones.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_deferred_repair_is_retried_on_the_next_check() {
        let script = Scripted::new(Ok(RepairOutcome::Deferred));
        let (hook, replica, _tmp) = drifted_hook(script.clone()).await;
        // One row locally, so the mismatch needs confirming first.
        replica
            .write()
            .await
            .apply("puzzle", crate::replica::RecordOp::Create, "puzzle:a", Some(serde_json::json!({"t": 1})))
            .await
            .unwrap();
        assert!(matches!(run_check(&hook, &replica, &BTreeSet::new()).await, Action::Report { .. }));
        assert!(matches!(run_check(&hook, &replica, &BTreeSet::new()).await, Action::Repair { .. }));
        // Deferred keeps the streak: acted on again right away.
        assert!(matches!(run_check(&hook, &replica, &BTreeSet::new()).await, Action::Repair { .. }));
        assert_eq!(script.repairs.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(script.reclones.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(hook.state.read().await.stuck.is_empty());
    }
}
