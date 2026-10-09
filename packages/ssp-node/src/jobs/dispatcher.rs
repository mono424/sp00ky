//! Admission control for job execution: how many of an outbox table's jobs may
//! be running at once, and what happens to the rest.
//!
//! # Why this exists
//!
//! Pickup is CREATE-event-driven: a `CREATE` on an outbox table fires a DB event
//! that reaches [`crate::node::SspNode`], which used to push the job straight
//! onto a bounded in-memory channel. A wide fan-out therefore turned N rows into
//! N channel sends, and once the channel filled, the *ingest handler itself*
//! blocked — the DB-side `http::post` timed out and rows were only rescued a
//! minute later by the recovery sweep.
//!
//! So the queue moved to where the durable data already is. Above the limit,
//! nothing is queued in memory at all: the row simply stays `pending` in the
//! outbox and is admitted later, oldest first. The outbox *is* the queue. That
//! costs nothing extra to persist, survives a restart, and needs no new status
//! (`pending` already means "not yet picked up").
//!
//! # What a permit means
//!
//! A permit means **"this job is, or is about to be, `status = 'processing'`"** —
//! exactly the population the cluster count measures. It is acquired immediately
//! before the channel send and released when execution finishes.
//!
//! It is deliberately NOT tied to [`JobControl`]'s `enqueued` mark, even though
//! the two look interchangeable. That mark is a dedupe token with a much longer
//! life: it is held across the delay window in `route_pending_job` and across
//! retry backoff. Tying a permit to it would let one job with `delay: 1h` hold
//! the table's only slot for an hour, and would stop the runner from taking the
//! next job during a 200 ms backoff — something it does today.
//!
//! And it is an RAII guard rather than a `release()` call, because the runner
//! now executes jobs on spawned tasks: a panic inside one is isolated and would
//! skip any explicit release, permanently wedging a table whose limit is 1.
//!
//! # Scope of the limit
//!
//! Exact per process. In cluster mode the bound is best-effort across processes:
//! each drain counts non-stale `processing` rows and takes the tighter of the
//! local and global views. Two nodes draining in the same instant can overshoot
//! by up to one batch; they converge on the next pass. A single-node deployment
//! never runs the count and is exact.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::types::{BackendInfo, JobConfig, JobControl, JobEntry};
use crate::ports::{Db, Scheduler, Spawner, TimerKind};

/// Fallback when no `_00_job_policy` row exists for a table. One, because that
/// is precisely what the runner did before this module existed — adopting the
/// feature must not change the behavior of a project that never opts in.
pub const DEFAULT_CONCURRENCY: u32 = 1;

/// How long a cached limit is trusted before the next drain re-reads it. Short
/// enough that `UPDATE _00_job_policy:job SET concurrency = 20` takes hold
/// while an operator is still watching, long enough that a busy table is not
/// re-reading policy on every completion.
const POLICY_TTL_MS: u64 = 30_000;

/// Hard ceiling on one drain's page, independent of how many slots are free.
/// The due-window predicate is a computed expression over `created_at + delay`,
/// so it cannot be served by an index and is applied after the scan — a table
/// with a million pending rows should not be asked for all of them at once.
const DRAIN_PAGE_MAX: u32 = 200;

/// How long after an unfinished drain the table is looked at again.
///
/// The primary trigger is a job completing, which frees a slot and kicks a
/// drain directly. This timer covers the case that has no local completion to
/// wait for: in cluster mode the binding constraint can be the *global* count,
/// so a node sits with free local slots, runs nothing, and would otherwise
/// never wake itself up.
const DRAIN_RETRY_SECS: u64 = 2;

// A `processing` row stops counting against the cluster budget exactly when its
// LEASE expires — see `schedule_core::sql::LEASE_LIVE`. That pairing is the point:
// the row that stops spending the budget is the same row that has become
// reclaimable, so admission control and recovery can no longer disagree. Before it,
// the two used different rules and a row could fall outside the budget while
// remaining unreachable by recovery: invisible to both.
//
// Note this count still includes a job sitting in retry backoff, whose row stays
// `processing` until the moment it is re-admitted. The local permit is released for
// that window but the cluster budget is not, which is the conservative direction for
// a ceiling and is bounded by the backoff length.

/// Per-table admission state.
#[derive(Debug)]
struct TableState {
    /// Cached `_00_job_policy` value.
    limit: u32,
    /// Epoch-ms of the last policy read; 0 = never read.
    limit_read_at_ms: u64,
    /// Outstanding permits.
    inflight: u32,
    /// A pending row is known to be waiting: something was refused, or a drain
    /// saw more rows than it could take. Gates every drain, so an unsaturated
    /// table never issues a single extra query.
    backlog: bool,
    /// A drain is running for this table.
    draining: bool,
    /// A drain was requested while one was running; the running one loops again.
    rerun: bool,
}

impl Default for TableState {
    fn default() -> Self {
        Self {
            limit: DEFAULT_CONCURRENCY,
            limit_read_at_ms: 0,
            inflight: 0,
            backlog: false,
            draining: false,
            rerun: false,
        }
    }
}

#[derive(Debug, Default)]
struct Shared {
    tables: Mutex<HashMap<String, TableState>>,
    /// Latched once the queue receiver is gone. The Cloudflare and portable
    /// shells construct a channel and immediately drop the receiver — they have
    /// no runner at all — so without this latch every ingest on those hosts
    /// would kick a drain query that can never lead anywhere.
    closed: Mutex<bool>,
    /// Blue/green: claims stopped (a standby that must not write, or a
    /// retired SSP whose successor now owns new work). Jobs already admitted
    /// run to completion; nothing new is admitted or drained, and refused
    /// rows stay `pending`, flagged as backlog for when claims resume.
    paused: std::sync::atomic::AtomicBool,
}

impl Shared {
    fn with<T>(&self, table: &str, f: impl FnOnce(&mut TableState) -> T) -> T {
        let mut guard = self.tables.lock().unwrap();
        f(guard.entry(table.to_string()).or_default())
    }
}

/// One slot on a table, released on drop.
///
/// Carried inside the [`JobEntry`] it was taken for, so the slot's lifetime is
/// the entry's lifetime whatever happens to it: a clean return, a panic in a
/// spawned task, a dropped channel, or a `try_send` that handed the entry back.
pub struct Permit {
    shared: Arc<Shared>,
    table: String,
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Permit({})", self.table)
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.shared.with(&self.table, |st| {
            st.inflight = st.inflight.saturating_sub(1);
        });
    }
}

/// Decides which jobs may run now and pulls the rest off the outbox as slots free.
pub struct JobDispatcher {
    shared: Arc<Shared>,
    db: Arc<dyn Db>,
    spawner: Arc<dyn Spawner>,
    scheduler: Arc<dyn Scheduler>,
    queue_tx: mpsc::Sender<JobEntry>,
    job_control: JobControl,
    job_config: Arc<JobConfig>,
    /// This node's id, stamped as the assignee when it claims a drained row.
    ssp_id: String,
    /// No scheduler in front of us: skip the cluster count and the claim CAS.
    standalone: bool,
}

impl JobDispatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Arc<dyn Db>,
        spawner: Arc<dyn Spawner>,
        scheduler: Arc<dyn Scheduler>,
        queue_tx: mpsc::Sender<JobEntry>,
        job_control: JobControl,
        job_config: Arc<JobConfig>,
        ssp_id: String,
        standalone: bool,
    ) -> Self {
        Self {
            shared: Arc::new(Shared::default()),
            db,
            spawner,
            scheduler,
            queue_tx,
            job_control,
            job_config,
            ssp_id,
            standalone,
        }
    }

    /// This node's SSP id, stamped onto a row as its lease owner. The runner needs it
    /// at claim time and the dispatcher is the one that was handed it.
    pub fn ssp_id(&self) -> &str {
        &self.ssp_id
    }

    pub fn job_control(&self) -> &JobControl {
        &self.job_control
    }

    fn is_closed(&self) -> bool {
        *self.shared.closed.lock().unwrap()
    }

    /// Stop (`true`) or resume (`false`) new job claims; see `Shared::paused`.
    /// Resuming kicks a drain for every table with a known backlog, so the
    /// rows refused meanwhile are picked up without waiting for a trigger.
    pub fn set_paused(self: &Arc<Self>, paused: bool) {
        let was = self.shared.paused.swap(paused, std::sync::atomic::Ordering::SeqCst);
        if was && !paused {
            for table in self.backlogged_tables() {
                self.kick_drain(&table);
            }
        }
    }

    pub fn is_paused(&self) -> bool {
        self.shared.paused.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Record that a pending row is waiting, so the next drain (and the drain
    /// timer) know there is something to do.
    pub fn note_backlog(&self, table: &str) {
        self.shared.with(table, |st| st.backlog = true);
    }

    /// Tables with a known backlog — what [`crate::ports::TimerKind::JobDrain`]
    /// re-arms against.
    pub fn backlogged_tables(&self) -> Vec<String> {
        if self.is_closed() {
            return Vec::new();
        }
        self.shared
            .tables
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, st)| st.backlog)
            .map(|(t, _)| t.clone())
            .collect()
    }

    /// Try to run `entry` now.
    ///
    /// Returns `false` when the job was not admitted, in which case the caller
    /// must leave the row `pending` and do nothing else — the row is the queue,
    /// and a later drain will pick it up in `created_at` order.
    ///
    /// This is the entry point for every path that is NOT the drain itself:
    /// ingest, `/job/retry`, `/job/recover`, the recovery sweep, and the
    /// post-backoff re-queue.
    pub async fn try_admit(self: &Arc<Self>, entry: JobEntry) -> bool {
        self.admit(entry, true).await
    }

    /// `respect_backlog`: refuse while older rows are known to be waiting.
    ///
    /// Without that fence the ingest fast path jumps the queue — a freshly
    /// created row takes the slot a drain was about to give to the oldest
    /// pending row, and at a steady arrival rate near the cap the head of the
    /// backlog is never served. The drain itself passes `false`, since it is
    /// the thing serving that order.
    async fn admit(self: &Arc<Self>, mut entry: JobEntry, respect_backlog: bool) -> bool {
        if self.is_closed() {
            return false;
        }
        let table = entry.table.clone();
        if self.is_paused() {
            // No drain kick: it would refuse too. `set_paused(false)` kicks.
            self.note_backlog(&table);
            return false;
        }

        enum Decision {
            Admit,
            /// `kick` is false when a drain is already running: it has been
            /// asked to loop again, and spawning a second one per refused job
            /// would turn a 10 000-row burst into 10 000 no-op tasks.
            Deferred { kick: bool },
        }
        let decision = self.shared.with(&table, |st| {
            let refuse = (respect_backlog && st.backlog) || st.inflight >= st.limit;
            if refuse {
                st.backlog = true;
                if st.draining {
                    st.rerun = true;
                    return Decision::Deferred { kick: false };
                }
                return Decision::Deferred { kick: true };
            }
            st.inflight += 1;
            Decision::Admit
        });

        if let Decision::Deferred { kick } = decision {
            if kick {
                self.kick_drain(&table);
            }
            return false;
        }

        let permit = Permit {
            shared: Arc::clone(&self.shared),
            table: table.clone(),
        };

        // Dedupe against a life of this id already moving through the runner.
        // Taken after the permit so the guard releases the slot on the way out.
        if !self.job_control.mark_enqueued(&entry.id) {
            debug!(job_id = %entry.id, "Skipping admit — already queued");
            return false;
        }

        entry.permit = Some(permit);
        match self.queue_tx.try_send(entry) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(entry)) => {
                // Admission is the real bound now, so this means the channel is
                // sized below the configured limit. The row stays pending.
                self.job_control.clear_enqueued(&entry.id);
                self.shared.with(&table, |st| st.backlog = true);
                warn!(job_id = %entry.id, "Job queue full — leaving the row pending");
                false
            }
            Err(mpsc::error::TrySendError::Closed(entry)) => {
                self.job_control.clear_enqueued(&entry.id);
                *self.shared.closed.lock().unwrap() = true;
                debug!(job_id = %entry.id, "No job runner on this host — dispatch disabled");
                false
            }
        }
    }

    /// Schedule a drain on the spawner. Cheap and idempotent: a drain with
    /// nothing to do returns before issuing a query.
    pub fn kick_drain(self: &Arc<Self>, table: &str) {
        if self.is_closed() {
            return;
        }
        let this = Arc::clone(self);
        let table = table.to_string();
        self.spawner.spawn(Box::pin(async move {
            this.drain(&table).await;
        }));
    }

    /// Admit as many waiting rows as the table's remaining budget allows.
    ///
    /// Coalesced: a second caller while one is running just asks the running
    /// one to loop again, so N simultaneous completions cannot each read the
    /// same free count and each admit that many.
    pub async fn drain(self: &Arc<Self>, table: &str) {
        if self.is_closed() || self.is_paused() {
            return;
        }
        let start = self.shared.with(table, |st| {
            if !st.backlog {
                return false;
            }
            if st.draining {
                st.rerun = true;
                return false;
            }
            st.draining = true;
            true
        });
        if !start {
            return;
        }

        loop {
            self.drain_once(table).await;
            let again = self.shared.with(table, |st| {
                if st.rerun && st.backlog {
                    st.rerun = false;
                    true
                } else {
                    st.draining = false;
                    st.rerun = false;
                    false
                }
            });
            if !again {
                break;
            }
        }

        // Left something behind — make sure someone comes back for it even if
        // no job on this node completes in the meantime.
        if self.shared.with(table, |st| st.backlog) {
            self.scheduler
                .schedule(
                    TimerKind::JobDrain { table: table.to_string() },
                    crate::now_epoch_ms() + DRAIN_RETRY_SECS * 1000,
                )
                .await;
        }
    }

    async fn drain_once(self: &Arc<Self>, table: &str) {
        let Some(backend) = self.job_config.job_tables.get(table).cloned() else {
            // Not a table we dispatch for: stop flagging it forever.
            self.shared.with(table, |st| st.backlog = false);
            return;
        };
        if !is_plain_identifier(table) {
            self.shared.with(table, |st| st.backlog = false);
            warn!(table = %table, "Refusing to drain a table whose name is not a plain identifier");
            return;
        }

        self.refresh_policy(table).await;

        // Local budget first — it is exact and free.
        let (limit, local_free) = self.shared.with(table, |st| {
            (st.limit, st.limit.saturating_sub(st.inflight))
        });

        // Cluster budget: the tighter of the two views. Only worth a query when
        // another process could be spending the same budget.
        let mut free = local_free;
        if !self.standalone {
            if let Some(cluster) = self.count_processing(table).await {
                free = free.min(limit.saturating_sub(cluster));
            }
        }

        // Fetch at least one row even at zero budget: it is the only way to
        // learn the backlog has cleared, and without that the drain timer would
        // keep waking for a table that has nothing left to run.
        let page = free.clamp(1, DRAIN_PAGE_MAX);
        let rows = match self.select_pending(table, page).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(table = %table, error = %e, "Drain query failed");
                return;
            }
        };

        if rows.is_empty() {
            self.shared.with(table, |st| st.backlog = false);
            return;
        }

        let mut admitted = 0u32;
        for row in &rows {
            // Paused mid-drain (a retire): stop before stamping another row
            // with a claim this node will not run.
            if admitted >= free || self.is_paused() {
                break;
            }
            let Some(id) = row.get("id").and_then(|v| v.as_str()) else { continue };
            if !self.claim(id).await {
                continue;
            }
            if self.admit(self.entry_for(id, row, &backend), false).await {
                admitted += 1;
            }
        }

        // Still-known backlog if we could not take everything this page held,
        // or if the page came back full (there is very likely more behind it).
        let more = admitted < rows.len() as u32 || rows.len() as u32 >= page;
        self.shared.with(table, |st| st.backlog = more);
        if admitted > 0 {
            debug!(table = %table, admitted, "Drained pending jobs");
        }
    }

    fn entry_for(&self, id: &str, row: &Value, backend: &BackendInfo) -> JobEntry {
        let timeout_override = row.get("timeout").and_then(|v| v.as_u64()).map(|v| v as u32);
        JobEntry::from_record(
            id.to_string(),
            backend.base_url.clone(),
            backend.auth_token.clone(),
            row,
            backend.effective_timeout(timeout_override),
        )
    }

    /// Take ownership of a drained row before running it.
    ///
    /// Standalone has exactly one candidate runner, so there is nothing to
    /// claim. In cluster mode the cap makes a backlogged row sit pending for as
    /// long as the backlog lasts, which is exactly the shape the scheduler's
    /// re-dispatch sweep treats as "stuck" — so ownership has to be settled in
    /// the database, not in one process's memory.
    ///
    /// Like `set_assignee_helper`, it leaves `updated_at` alone: the recovery
    /// staleness clock must keep measuring from the row's last real status
    /// change, not from an ownership stamp.
    async fn claim(&self, job_id: &str) -> bool {
        if self.standalone {
            return true;
        }
        let result = self
            .db
            .query(
                "UPDATE type::record($id) SET assignee = $me \
                 WHERE status = 'pending' AND (assignee = NONE OR assignee = $me) \
                 RETURN AFTER",
                &[("id", json!(job_id)), ("me", json!(self.ssp_id))],
            )
            .await;
        match result {
            Ok(values) => values.iter().any(|v| match v {
                Value::Array(rows) => !rows.is_empty(),
                Value::Null => false,
                _ => true,
            }),
            Err(e) => {
                warn!(job_id = %job_id, error = %e, "Could not claim job for dispatch");
                false
            }
        }
    }

    /// Oldest waiting rows first. `created_at`, never `updated_at`: a retry
    /// touches `updated_at`, which would send a job that has already waited its
    /// turn back to the end of the line.
    async fn select_pending(&self, table: &str, limit: u32) -> Result<Vec<Value>, crate::ports::DbError> {
        let sql = format!(
            "SELECT {fields} FROM {table} \
             WHERE status = 'pending' AND {due} \
             ORDER BY created_at ASC LIMIT {limit}",
            fields = DISPATCH_FIELDS,
            due = super::PENDING_DUE_CLAUSE,
        );
        let values = self.db.query(&sql, &[]).await?;
        Ok(values
            .into_iter()
            .flat_map(|v| match v {
                Value::Array(rows) => rows,
                Value::Null => Vec::new(),
                other => vec![other],
            })
            .collect())
    }

    /// `processing` rows whose lease is still live, across the whole deployment.
    async fn count_processing(&self, table: &str) -> Option<u32> {
        let sql = format!(
            "SELECT VALUE count() FROM {table} \
             WHERE status = 'processing' AND {live} GROUP ALL",
            live = schedule_core::sql::LEASE_LIVE,
        );
        match self.db.query(&sql, &[]).await {
            Ok(values) => values.iter().find_map(count_value).map(|n| n as u32),
            Err(e) => {
                warn!(table = %table, error = %e, "Could not count in-flight jobs; using the local view");
                None
            }
        }
    }

    /// Re-read `_00_job_policy` when the cached value has aged out.
    async fn refresh_policy(&self, table: &str) {
        let now = crate::now_epoch_ms();
        let stale = self.shared.with(table, |st| {
            st.limit_read_at_ms == 0 || now.saturating_sub(st.limit_read_at_ms) >= POLICY_TTL_MS
        });
        if !stale {
            return;
        }
        let limit = self.read_policy(table).await;
        self.shared.with(table, |st| {
            st.limit = limit;
            st.limit_read_at_ms = now;
        });
    }

    /// Set a table's limit directly, bypassing `_00_job_policy`.
    ///
    /// For tests and harnesses that have no policy row to read (and would
    /// otherwise silently run at the default of 1). The next TTL refresh
    /// overwrites it from the database.
    #[doc(hidden)]
    pub fn set_limit(&self, table: &str, limit: u32) {
        self.shared.with(table, |st| {
            st.limit = limit.max(1);
            st.limit_read_at_ms = crate::now_epoch_ms();
        });
    }

    /// Load every configured table's policy once, at bootstrap, so the first
    /// burst is governed by the deployed limit rather than by the default.
    pub async fn preload_policies(&self) {
        let now = crate::now_epoch_ms();
        let tables: Vec<String> = self.job_config.job_tables.keys().cloned().collect();
        for table in tables {
            let limit = self.read_policy(&table).await;
            self.shared.with(&table, |st| {
                st.limit = limit;
                st.limit_read_at_ms = now;
            });
        }
    }

    /// Every failure mode here — no table, no row, a malformed value, a dead
    /// connection — resolves to the default rather than to zero. A throttle
    /// that cannot be read must not become a stop.
    async fn read_policy(&self, table: &str) -> u32 {
        let sql = "SELECT VALUE concurrency FROM ONLY type::record('_00_job_policy', $table)";
        match self.db.query(sql, &[("table", json!(table))]).await {
            Ok(values) => values
                .iter()
                .find_map(|v| v.as_u64())
                .map(|n| (n as u32).max(1))
                .unwrap_or(DEFAULT_CONCURRENCY),
            Err(e) => {
                debug!(table = %table, error = %e, "No job policy readable; using the default");
                DEFAULT_CONCURRENCY
            }
        }
    }
}

/// Projection for a drained row: everything [`JobEntry::from_record`] reads.
/// `type::string(id) AS id` keeps the RecordId out of the flattened JSON.
///
/// `created_at` is in the list because SurrealDB v3 rejects `ORDER BY` on a
/// field the projection does not select ("Missing order idiom `created_at` in
/// statement selection"), and the drain's whole contract is that ordering.
const DISPATCH_FIELDS: &str = "type::string(id) AS id, status, path, payload, retries, \
                               max_retries, retry_strategy, timeout, created_at";

/// `SELECT VALUE count() ... GROUP ALL` comes back as a bare number on some
/// query plans and as `{ count: n }` on others — and adding an index is exactly
/// the kind of change that flips it. Accept both.
fn count_value(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64(),
        Value::Array(rows) => rows.iter().find_map(count_value),
        Value::Object(map) => map.get("count").and_then(|v| v.as_i64()),
        _ => None,
    }
}

/// The table half of a job id (`job:abc` -> `job`).
///
/// Bracket-quoted keys (`job:⟨weird key⟩`) are common; a bracket-quoted TABLE is
/// not, because outbox names come from `method.table` and must be plain
/// identifiers. Strip the brackets anyway rather than silently returning a name
/// that matches nothing in the backend map.
pub fn table_of(job_id: &str) -> Option<&str> {
    let (table, key) = job_id.split_once(':')?;
    if key.is_empty() {
        return None;
    }
    let table = table.trim_start_matches('⟨').trim_end_matches('⟩');
    if table.is_empty() {
        None
    } else {
        Some(table)
    }
}

/// Table names are interpolated into the drain and count statements (a bound
/// parameter cannot stand in for a table). They come from the deployment's own
/// config, but interpolation is interpolation.
fn is_plain_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_of_handles_the_shapes_ids_actually_take() {
        assert_eq!(table_of("job:abc"), Some("job"));
        assert_eq!(table_of("statistics_job:k1"), Some("statistics_job"));
        assert_eq!(table_of("⟨job⟩:⟨weird key⟩"), Some("job"));
        assert_eq!(table_of("nocolon"), None);
        assert_eq!(table_of(":nokey"), None);
        assert_eq!(table_of("job:"), None);
    }

    #[test]
    fn count_value_reads_both_group_all_shapes() {
        assert_eq!(count_value(&json!(7)), Some(7));
        assert_eq!(count_value(&json!([7])), Some(7));
        assert_eq!(count_value(&json!({ "count": 7 })), Some(7));
        assert_eq!(count_value(&json!([{ "count": 7 }])), Some(7));
        assert_eq!(count_value(&json!(null)), None);
    }

    #[test]
    fn identifiers_that_would_break_interpolation_are_rejected() {
        assert!(is_plain_identifier("job"));
        assert!(is_plain_identifier("statistics_job"));
        assert!(!is_plain_identifier(""));
        assert!(!is_plain_identifier("job; DELETE user"));
        assert!(!is_plain_identifier("my-table"));
        assert!(!is_plain_identifier("1job"));
    }

    /// The guard, not a `release()` call, is what makes a panicking job safe.
    #[test]
    fn a_dropped_permit_releases_the_slot() {
        let shared = Arc::new(Shared::default());
        shared.with("job", |st| st.inflight = 1);
        {
            let _permit = Permit { shared: Arc::clone(&shared), table: "job".into() };
            assert_eq!(shared.with("job", |st| st.inflight), 1);
        }
        assert_eq!(shared.with("job", |st| st.inflight), 0);
    }

    /// Releasing more than was taken must saturate, never wrap to u32::MAX and
    /// wedge the table forever.
    #[test]
    fn releasing_an_empty_table_saturates() {
        let shared = Arc::new(Shared::default());
        drop(Permit { shared: Arc::clone(&shared), table: "job".into() });
        assert_eq!(shared.with("job", |st| st.inflight), 0);
    }
}
