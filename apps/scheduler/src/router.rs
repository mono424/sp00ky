use crate::config::LoadBalanceStrategy;
use crate::messages::{RecordOp, RecordUpdate};
use crate::transport::SspInfo;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};
use tracing::warn;

/// SSP initialization state
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SspState {
    /// SSP is bootstrapping from the snapshot proxy
    Bootstrapping,
    /// SSP reported ready, scheduler is replaying missed events
    Replaying,
    /// SSP is fully caught up and receiving live updates
    Ready,
    /// A Ready SSP that missed at least one live delivery (the `/ingest` POST
    /// failed or timed out). Its events queue in the per-SSP buffer, in
    /// order, until `ingest::redeliver_to_lagging_ssp` has caught it up. Not
    /// a bootstrap state: it neither freezes the snapshot nor counts as an
    /// active bootstrap, and the heartbeat-stale sweep still applies.
    Lagging,
    /// Blue/green: an SSP whose standby is being promoted in its place. Off
    /// the live path, its events queue exactly as for `Lagging` (without a
    /// redelivery task), so a promotion that fails can hand it back every
    /// event it missed. Gets no views and no jobs.
    Retiring,
    /// Blue/green: replaced by its standby and on its way out. Gets nothing
    /// at all, but stays in the pool while it heartbeats, so jobs it is still
    /// running are not taken for orphans and re-run before it stops.
    Retired,
}

/// One SSP as a scheduler handover carries it to the successor scheduler.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SspSnapshot {
    pub info: SspInfo,
    pub state: SspState,
    #[serde(default)]
    pub buffer: Vec<RecordUpdate>,
    #[serde(default)]
    pub bootstrap_seq: Option<u64>,
    #[serde(default)]
    pub registration_gen: u64,
    #[serde(default)]
    pub buffer_overflowed: bool,
    #[serde(default)]
    pub forced_resync: Option<ResyncKind>,
    /// Set on a standby: the SSP it is to replace.
    #[serde(default)]
    pub standby_of: Option<String>,
    /// Set on a successor: the SSP it replaced (see `SspPool::replaced_by`).
    #[serde(default)]
    pub replaced: Option<String>,
}

/// What a forced re-bootstrap should do on the SSP's side before it exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResyncKind {
    /// Exit and relaunch; the snapshot (if any) is restored and caught up.
    Resync,
    /// Drop the on-disk snapshot first, so the relaunch is a cold rebuild.
    Clean,
}

/// Pool of connected SSPs with load balancing
pub struct SspPool {
    ssps: HashMap<String, SspInfo>,
    ssp_states: HashMap<String, SspState>,
    message_buffers: HashMap<String, VecDeque<RecordUpdate>>,
    /// Per-SSP snapshot_seq recorded at registration time
    ssp_snapshot_seqs: HashMap<String, u64>,
    /// SSPs that the operator (or an integrity check) has flagged as needing
    /// to re-bootstrap, and how. The next heartbeat from these SSPs returns
    /// 409 so they tear down and re-register against the current frozen
    /// snapshot; a `Clean` entry also tells them to drop their snapshot first.
    forced_resync: HashMap<String, ResyncKind>,
    /// Consecutive catch-up verification failures per SSP, reset on any pass.
    /// A plain re-bootstrap can't fix a *deterministic* scheduler-vs-circuit
    /// hash gap (the SSP refetches the same diverging state every cycle), so
    /// this counter lets the catch-up path escalate — re-clone the replica,
    /// then admit anyway — instead of looping forever. See `poll_and_replay_ssp`.
    catchup_failures: HashMap<String, u32>,
    /// Consecutive *bootstrap* integrity failures per SSP, reset on any pass.
    /// Same escalation rationale as `catchup_failures`, for the earlier gate:
    /// the SSP's post-bootstrap hash check. See `handle_bootstrap_verify`.
    bootstrap_failures: HashMap<String, u32>,
    /// SSPs whose per-SSP buffer actually overflowed and was dropped. An
    /// explicit flag, because "buffer entry exists but is empty" is ALSO the
    /// normal state right after `drain_buffer` — inferring overflow from it
    /// made healthy SSPs 409 (→ exit(4)) in the window between the last drain
    /// and `mark_ready`.
    buffer_overflowed: HashSet<String>,
    /// When each SSP last changed state. Lets the snapshot updater evict SSPs
    /// parked in `Bootstrapping`/`Replaying` (which would otherwise hold
    /// `has_active_bootstrap()` — and thus the snapshot freeze — forever).
    state_since: HashMap<String, Instant>,
    /// Monotonic registration generation per SSP id, bumped on every
    /// `/ssp/register`. A `poll_and_replay_ssp` task checks its captured gen
    /// at phase boundaries and bails when superseded by a re-registration,
    /// so a stale poll task never removes or admits the newer registration.
    registration_gen: HashMap<String, u64>,
    publication: HashMap<String, ssp_protocol::PublicationMetrics>,
    /// Blue/green standbys: standby id -> the SSP it is to replace. A standby
    /// follows ingest like any Ready SSP but is never chosen for a view or a
    /// job until it is promoted.
    standby_of: HashMap<String, String>,
    /// Blue/green: replaced SSP id -> the SSP that replaced it. While the
    /// successor is in the pool, the replaced id may not register again: a
    /// predecessor that comes back (a scheduler restart made it re-register,
    /// or it crashed after retiring) would serve every view beside its
    /// successor and publish each change twice.
    replaced_by: HashMap<String, String>,
    strategy: LoadBalanceStrategy,
    round_robin_index: usize,
    max_buffer_size: usize,
}

impl SspPool {
    /// Create a new SSP pool with configurable buffer size
    pub fn new(strategy: LoadBalanceStrategy, max_buffer_size: usize) -> Self {
        Self {
            ssps: HashMap::new(),
            ssp_states: HashMap::new(),
            message_buffers: HashMap::new(),
            ssp_snapshot_seqs: HashMap::new(),
            forced_resync: HashMap::new(),
            catchup_failures: HashMap::new(),
            bootstrap_failures: HashMap::new(),
            buffer_overflowed: HashSet::new(),
            state_since: HashMap::new(),
            registration_gen: HashMap::new(),
            publication: HashMap::new(),
            standby_of: HashMap::new(),
            replaced_by: HashMap::new(),
            strategy,
            round_robin_index: 0,
            max_buffer_size,
        }
    }

    /// Record one more consecutive catch-up verification failure for this SSP
    /// and return the new running count. Cleared by `reset_catchup_failures`
    /// on any successful verification (or admit).
    pub fn record_catchup_failure(&mut self, ssp_id: &str) -> u32 {
        let entry = self.catchup_failures.entry(ssp_id.to_string()).or_insert(0);
        *entry += 1;
        crate::admin::incidents::emit(ssp_id, "integrity_failure", "open", "Bootstrap or catch-up integrity verification failed", None);
        *entry
    }

    /// Reset the consecutive catch-up failure count for this SSP (on a pass,
    /// or once we admit it to broadcast to break the loop).
    pub fn reset_catchup_failures(&mut self, ssp_id: &str) {
        self.catchup_failures.remove(ssp_id);
    }

    /// Record one more consecutive bootstrap integrity failure for this SSP
    /// and return the new running count.
    pub fn record_bootstrap_failure(&mut self, ssp_id: &str) -> u32 {
        let entry = self
            .bootstrap_failures
            .entry(ssp_id.to_string())
            .or_insert(0);
        *entry += 1;
        crate::admin::incidents::emit(ssp_id, "integrity_failure", "open", "Bootstrap or catch-up integrity verification failed", None);
        *entry
    }

    /// Reset the consecutive bootstrap failure count (on a pass, or once the
    /// breaker admits the SSP anyway).
    pub fn reset_bootstrap_failures(&mut self, ssp_id: &str) {
        self.bootstrap_failures.remove(ssp_id);
    }

    /// Flag an SSP for forced re-bootstrap on its next heartbeat. Used by
    /// the integrity-check path when the SSP's circuit hashes disagree with
    /// the scheduler's frozen snapshot — the SSP is told (via 409) to wipe
    /// and re-register rather than continue serving stale state.
    pub fn mark_for_resync(&mut self, ssp_id: &str) {
        self.mark_for_resync_with(ssp_id, ResyncKind::Resync);
    }

    /// Flag an SSP for forced re-bootstrap of a particular kind. `Clean` is
    /// sticky: an integrity check that later flags the same SSP with a plain
    /// `Resync` must not quietly downgrade what the operator asked for.
    pub fn mark_for_resync_with(&mut self, ssp_id: &str, kind: ResyncKind) {
        if !self.forced_resync.contains_key(ssp_id) {
            crate::admin::incidents::emit(ssp_id, "resync_requested", "open", "SSP instructed to restart and resynchronize on its next heartbeat", None);
        }
        let entry = self
            .forced_resync
            .entry(ssp_id.to_string())
            .or_insert(kind);
        if kind == ResyncKind::Clean {
            *entry = ResyncKind::Clean;
        }
    }

    /// Flag every connected SSP for forced re-bootstrap.
    pub fn mark_all_for_resync(&mut self) -> usize {
        self.mark_all_for_resync_with(ResyncKind::Resync)
    }

    /// Flag every connected SSP for forced re-bootstrap of a particular kind.
    pub fn mark_all_for_resync_with(&mut self, kind: ResyncKind) -> usize {
        let ids: Vec<String> = self.ssps.keys().cloned().collect();
        for id in &ids {
            self.mark_for_resync_with(id, kind);
        }
        ids.len()
    }

    /// Take-and-clear: returns true if this SSP was flagged for forced
    /// resync, removing the flag in the same step.
    pub fn take_resync_flag(&mut self, ssp_id: &str) -> bool {
        self.take_resync(ssp_id).is_some()
    }

    /// Take-and-clear, returning the kind of resync that was requested.
    pub fn take_resync(&mut self, ssp_id: &str) -> Option<ResyncKind> {
        self.forced_resync.remove(ssp_id)
    }

    /// Whether an SSP is currently flagged, without clearing it.
    pub fn pending_resync(&self, ssp_id: &str) -> Option<ResyncKind> {
        self.forced_resync.get(ssp_id).copied()
    }

    /// Add or update an SSP
    pub fn upsert(&mut self, ssp: SspInfo) {
        self.ssps.insert(ssp.id.clone(), ssp);
    }

    /// Update SSP from heartbeat
    pub fn update_ssp(
        &mut self,
        ssp_id: &str,
        views: usize,
        cpu_usage: Option<f64>,
        memory_usage: Option<f64>,
        version: String,
    ) {
        if let Some(ssp) = self.ssps.get_mut(ssp_id) {
            ssp.last_heartbeat = Instant::now();
            ssp.views = views;
            ssp.cpu_usage = cpu_usage;
            ssp.memory_usage = memory_usage;
            ssp.version = version;
        } else {
            // Add new SSP
            let info = SspInfo {
                id: ssp_id.to_string(),
                url: String::new(), // URL must be set via registration, not heartbeat
                version,
                connected_at: Instant::now(),
                last_heartbeat: Instant::now(),
                query_count: 0,
                views,
                cpu_usage,
                memory_usage,
                env: None,
                bootstrap: None,
            };
            self.ssps.insert(ssp_id.to_string(), info);
        }
    }

    /// When this SSP last changed lifecycle state. Drives the "how long has it
    /// been bootstrapping" reading on the admin dashboard.
    pub fn state_since(&self, ssp_id: &str) -> Option<Instant> {
        self.state_since.get(ssp_id).copied()
    }

    /// Record an advisory bootstrap-progress report. Ignored for an SSP we do
    /// not know (a report can outlive a removal) and for one already `Ready`,
    /// so a late in-flight post can never resurrect a finished progress bar.
    pub fn set_bootstrap_progress(
        &mut self,
        ssp_id: &str,
        progress: ssp_protocol::BootstrapProgress,
    ) {
        if self.ssp_states.get(ssp_id) == Some(&SspState::Ready) {
            return;
        }
        if let Some(ssp) = self.ssps.get_mut(ssp_id) {
            ssp.bootstrap = Some(progress);
        }
    }

    /// Buffer a message for an SSP that's not ready yet
    /// Returns true if buffered successfully, false if buffer overflow requires re-bootstrap
    pub fn buffer_message(&mut self, ssp_id: &str, message: RecordUpdate) -> bool {
        // Buffer for SSPs that are bootstrapping, replaying, or lagging behind
        // a failed live delivery.
        match self.ssp_states.get(ssp_id) {
            Some(SspState::Bootstrapping)
            | Some(SspState::Replaying)
            | Some(SspState::Lagging)
            | Some(SspState::Retiring) => {
                let buffer = self
                    .message_buffers
                    .entry(ssp_id.to_string())
                    .or_insert_with(VecDeque::new);

                // Check if buffer would overflow
                if buffer.len() >= self.max_buffer_size {
                    warn!(
                        "Buffer overflow for SSP '{}' ({} messages). SSP needs to re-bootstrap.",
                        ssp_id,
                        buffer.len()
                    );
                    buffer.clear();
                    self.buffer_overflowed.insert(ssp_id.to_string());
                    return false;
                }

                buffer.push_back(message);
                true
            }
            _ => {
                // SSP is ready or doesn't exist, no buffering needed
                true
            }
        }
    }

    /// Check if SSP has buffer overflow (needs re-bootstrap).
    ///
    /// Reads the explicit flag set by `buffer_message` when it dropped a full
    /// buffer. The old form inferred overflow from an empty-but-present buffer
    /// entry, which `drain_buffer` also produces on the happy path — so a
    /// heartbeat landing between the final drain and `mark_ready` got a
    /// spurious 409 and the SSP exited(4) mid-handshake.
    pub fn has_buffer_overflow(&self, ssp_id: &str) -> bool {
        self.buffer_overflowed.contains(ssp_id)
    }

    /// Mark SSP as ready and return any remaining buffered messages
    pub fn mark_ready(&mut self, ssp_id: &str) -> Vec<RecordUpdate> {
        if self.message_buffers.get(ssp_id).map_or(true, |b| b.is_empty()) {
            let reason = if self.is_lagging(ssp_id) { "Missed events replayed; SSP ready without a restart" } else { "Bootstrap and replay completed; SSP ready" };
            crate::admin::incidents::emit(ssp_id, "ready", "recovered", reason, None);
        }
        self.ssp_states.insert(ssp_id.to_string(), SspState::Ready);
        self.state_since.insert(ssp_id.to_string(), Instant::now());
        self.buffer_overflowed.remove(ssp_id);
        self.bootstrap_failures.remove(ssp_id);
        // The bar is done; drop it so `/info` never shows a stale one.
        if let Some(ssp) = self.ssps.get_mut(ssp_id) {
            ssp.bootstrap = None;
        }

        // Return and clear buffered messages
        self.message_buffers
            .remove(ssp_id)
            .map(|buf| buf.into_iter().collect())
            .unwrap_or_default()
    }

    /// Mark SSP as bootstrapping
    pub fn mark_bootstrapping(&mut self, ssp_id: &str) {
        self.ssp_states
            .insert(ssp_id.to_string(), SspState::Bootstrapping);
        self.state_since.insert(ssp_id.to_string(), Instant::now());
    }

    /// Mark SSP as replaying (SSP is ready, scheduler replaying missed events)
    pub fn mark_replaying(&mut self, ssp_id: &str) {
        self.ssp_states
            .insert(ssp_id.to_string(), SspState::Replaying);
        self.state_since.insert(ssp_id.to_string(), Instant::now());
    }

    /// A live delivery to a `Ready` SSP failed: park it in `Lagging` so
    /// subsequent events queue behind the missed one instead of overtaking
    /// it. Returns `true` only on the `Ready → Lagging` transition, which is
    /// the caller's cue to start exactly one redelivery task; a concurrent
    /// failure for an SSP already lagging just buffers. Any other state
    /// (bootstrapping, replaying, evicted) is left alone.
    pub fn mark_lagging(&mut self, ssp_id: &str) -> bool {
        if self.ssp_states.get(ssp_id) != Some(&SspState::Ready) {
            return false;
        }
        self.ssp_states
            .insert(ssp_id.to_string(), SspState::Lagging);
        crate::admin::incidents::emit(ssp_id, "lagging", "open", "Live ingest delivery failed or timed out; subsequent events are buffered", None);
        self.state_since.insert(ssp_id.to_string(), Instant::now());
        true
    }

    pub fn is_lagging(&self, ssp_id: &str) -> bool {
        matches!(self.ssp_states.get(ssp_id), Some(SspState::Lagging))
    }

    /// Whether jobs may be handed to this SSP: `Ready`, or `Lagging` with its
    /// buffer intact. A lagging SSP is alive and gets every event it missed,
    /// in order and each with its `job_assignee`, so a job given to it runs
    /// once redelivery reaches the CREATE. An overflowed buffer has dropped
    /// those events and the SSP is about to re-bootstrap.
    pub fn takes_jobs(&self, ssp_id: &str) -> bool {
        if self.standby_of.contains_key(ssp_id) {
            return false;
        }
        match self.ssp_states.get(ssp_id) {
            Some(SspState::Ready) => true,
            Some(SspState::Lagging) => !self.buffer_overflowed.contains(ssp_id),
            _ => false,
        }
    }

    /// The SSP to run a job: a `Ready` one by the load-balancing strategy,
    /// else a `Lagging` one that [`takes_jobs`](Self::takes_jobs). Refusing a
    /// lagging SSP left every job created during a lag unassigned, so nobody
    /// ran it until cluster recovery found a Ready SSP again (whitepawn
    /// 2026-10-08: 22 pending, throughput 0, for each lag of a PGN import).
    pub fn select_job_runner(&mut self) -> Option<String> {
        if let Some(ready) = self.select_for_query() {
            return Some(ready);
        }
        let lagging: Vec<String> = self
            .ssps
            .keys()
            .filter(|id| self.is_lagging(id) && self.takes_jobs(id))
            .cloned()
            .collect();
        self.select_among(&lagging)
    }

    /// Every SSP a job may be running on, `Ready` or `Lagging`: where a kill
    /// has to reach.
    pub fn job_runners(&self) -> Vec<SspInfo> {
        self.ssps
            .values()
            .filter(|s| !self.standby_of.contains_key(&s.id))
            .filter(|s| {
                matches!(
                    self.ssp_states.get(&s.id),
                    Some(SspState::Ready | SspState::Lagging | SspState::Retiring | SspState::Retired)
                )
            })
            .cloned()
            .collect()
    }

    /// Record ids of job CREATEs still queued for the SSP they were assigned
    /// to. Such a job has an owner although its row carries no `assignee` yet
    /// (the SSP stamps it on admission), so cluster recovery must not treat it
    /// as orphaned and run it ahead of its turn.
    pub fn queued_jobs(&self) -> HashSet<String> {
        let mut queued = HashSet::new();
        for (ssp_id, buffer) in &self.message_buffers {
            for message in buffer {
                if message.operation == RecordOp::Create
                    && message.job_assignee.as_deref() == Some(ssp_id.as_str())
                {
                    queued.insert(message.record_id.clone());
                }
            }
        }
        queued
    }

    /// Put undelivered events back at the FRONT of an SSP's buffer, keeping
    /// their order, so a redelivery that failed part-way resumes from the
    /// first event the SSP never acknowledged. Does not count toward the
    /// overflow bound: these events were already admitted once.
    pub fn requeue_front(&mut self, ssp_id: &str, messages: Vec<RecordUpdate>) {
        if messages.is_empty() {
            return;
        }
        let buffer = self
            .message_buffers
            .entry(ssp_id.to_string())
            .or_insert_with(VecDeque::new);
        for message in messages.into_iter().rev() {
            buffer.push_front(message);
        }
    }

    /// Bump and return the registration generation for this SSP id. Called by
    /// `handle_register`; the returned gen is captured by the spawned poll
    /// task and re-checked via `registration_gen` at phase boundaries.
    pub fn bump_registration_gen(&mut self, ssp_id: &str) -> u64 {
        self.publication.remove(ssp_id);
        let gen = self.registration_gen.entry(ssp_id.to_string()).or_insert(0);
        *gen += 1;
        if *gen > 1 { crate::admin::incidents::emit(ssp_id, "registered_again", "open", "SSP registered again; restart or rebootstrap observed", None); }
        *gen
    }

    /// Current registration generation for this SSP id (0 = never registered).
    pub fn registration_gen(&self, ssp_id: &str) -> u64 {
        self.registration_gen.get(ssp_id).copied().unwrap_or(0)
    }

    pub fn publication(&self, ssp_id: &str) -> Option<&ssp_protocol::PublicationMetrics> {
        self.publication.get(ssp_id)
    }

    pub fn update_publication(&mut self, ssp_id: &str, metrics: Option<ssp_protocol::PublicationMetrics>) {
        if let Some(mut metrics) = metrics {
            metrics.worst_views.truncate(8);
            for view in &mut metrics.worst_views {
                view.query_id = view.query_id.chars().take(256).collect();
            }
            self.publication.insert(ssp_id.to_owned(), metrics);
        } else {
            self.publication.remove(ssp_id);
        }
    }

    /// SSPs stuck in `Bootstrapping`/`Replaying` longer than `max_age` as of
    /// `now`. Pure helper for testability; see `stale_active_bootstraps`.
    pub fn stale_active_bootstraps_at(&self, now: Instant, max_age: Duration) -> Vec<String> {
        self.ssp_states
            .iter()
            .filter(|(_, s)| matches!(s, SspState::Bootstrapping | SspState::Replaying))
            .filter(|(id, _)| {
                self.state_since
                    .get(*id)
                    .is_some_and(|since| now.duration_since(*since) > max_age)
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// SSPs parked in an active-bootstrap state past `max_age`. These hold
    /// `has_active_bootstrap()` true (and with it the snapshot freeze), so the
    /// snapshot updater evicts them before deciding whether to drain.
    pub fn stale_active_bootstraps(&self, max_age: Duration) -> Vec<String> {
        self.stale_active_bootstraps_at(Instant::now(), max_age)
    }

    /// Drain buffered messages for an SSP without changing its state
    pub fn drain_buffer(&mut self, ssp_id: &str) -> Vec<RecordUpdate> {
        self.message_buffers
            .get_mut(ssp_id)
            .map(|buf| buf.drain(..).collect())
            .unwrap_or_default()
    }

    /// Record the snapshot_seq at which this SSP was registered
    pub fn set_bootstrap_seq(&mut self, ssp_id: &str, seq: u64) {
        self.ssp_snapshot_seqs.insert(ssp_id.to_string(), seq);
    }

    /// Get the snapshot_seq recorded when this SSP registered
    pub fn get_bootstrap_seq(&self, ssp_id: &str) -> Option<u64> {
        self.ssp_snapshot_seqs.get(ssp_id).copied()
    }

    /// Check if SSP is ready to receive updates
    pub fn is_ready(&self, ssp_id: &str) -> bool {
        matches!(self.ssp_states.get(ssp_id), Some(SspState::Ready))
    }

    /// Get the current state of an SSP
    pub fn get_state(&self, ssp_id: &str) -> Option<&SspState> {
        self.ssp_states.get(ssp_id)
    }

    /// Get buffer size for an SSP
    pub fn buffer_size(&self, ssp_id: &str) -> usize {
        self.message_buffers
            .get(ssp_id)
            .map(|buf| buf.len())
            .unwrap_or(0)
    }

    /// Remove an SSP
    pub fn remove(&mut self, ssp_id: &str) -> Option<SspInfo> {
        self.publication.remove(ssp_id);
        if self.ssps.contains_key(ssp_id) {
            crate::admin::incidents::emit(ssp_id, "removed", "open", "SSP removed from routing; heartbeat, bootstrap deadline or administrative removal", None);
        }
        self.ssp_states.remove(ssp_id);
        self.message_buffers.remove(ssp_id);
        self.ssp_snapshot_seqs.remove(ssp_id);
        self.forced_resync.remove(ssp_id);
        self.catchup_failures.remove(ssp_id);
        self.bootstrap_failures.remove(ssp_id);
        self.buffer_overflowed.remove(ssp_id);
        self.state_since.remove(ssp_id);
        self.standby_of.remove(ssp_id);
        // registration_gen is intentionally kept: it must stay monotonic
        // across remove/re-register so stale poll tasks always lose.
        self.ssps.remove(ssp_id)
    }

    /// Drop every SSP from the pool and clear all associated buffers/state.
    /// Used when the replica has been restored and SSPs must re-register
    /// against the new state. Returns the count of SSPs removed.
    pub fn clear_all(&mut self) -> usize {
        self.publication.clear();
        let count = self.ssps.len();
        self.ssps.clear();
        self.ssp_states.clear();
        self.message_buffers.clear();
        self.ssp_snapshot_seqs.clear();
        self.forced_resync.clear();
        self.catchup_failures.clear();
        self.bootstrap_failures.clear();
        self.buffer_overflowed.clear();
        self.state_since.clear();
        self.standby_of.clear();
        // registration_gen kept monotonic; see `remove`.
        self.round_robin_index = 0;
        count
    }

    /// Get an SSP by ID
    pub fn get(&self, ssp_id: &str) -> Option<&SspInfo> {
        self.ssps.get(ssp_id)
    }

    /// Get all connected SSPs
    pub fn all(&self) -> Vec<&SspInfo> {
        self.ssps.values().collect()
    }

    /// Select the best SSP for a new query based on load balancing strategy.
    /// Only considers SSPs that are in the `Ready` state.
    pub fn select_for_query(&mut self) -> Option<String> {
        let ready_ids: Vec<String> = self
            .ssps
            .keys()
            .filter(|id| matches!(self.ssp_states.get(*id), Some(SspState::Ready)))
            .filter(|id| !self.standby_of.contains_key(*id))
            .cloned()
            .collect();
        self.select_among(&ready_ids)
    }

    /// One of `ids` by the load-balancing strategy.
    fn select_among(&mut self, ids: &[String]) -> Option<String> {
        if ids.is_empty() {
            return None;
        }

        match self.strategy {
            LoadBalanceStrategy::RoundRobin => self.select_round_robin(ids),
            LoadBalanceStrategy::LeastQueries => self.select_least_queries(ids),
            LoadBalanceStrategy::LeastLoad => self.select_least_load(ids),
        }
    }

    /// Select SSP using round-robin
    fn select_round_robin(&mut self, ready_ids: &[String]) -> Option<String> {
        if ready_ids.is_empty() {
            return None;
        }

        let selected = ready_ids[self.round_robin_index % ready_ids.len()].clone();
        self.round_robin_index += 1;
        Some(selected)
    }

    /// Select SSP with fewest queries
    fn select_least_queries(&self, ready_ids: &[String]) -> Option<String> {
        ready_ids
            .iter()
            .filter_map(|id| self.ssps.get(id).map(|info| (id, info)))
            .min_by_key(|(_, info)| info.query_count)
            .map(|(id, _)| id.clone())
    }

    /// Select SSP with least load (CPU + memory)
    fn select_least_load(&self, ready_ids: &[String]) -> Option<String> {
        ready_ids
            .iter()
            .filter_map(|id| self.ssps.get(id).map(|info| (id, info)))
            .min_by(|(_, a), (_, b)| {
                let load_a = a.cpu_usage.unwrap_or(0.0) + a.memory_usage.unwrap_or(0.0);
                let load_b = b.cpu_usage.unwrap_or(0.0) + b.memory_usage.unwrap_or(0.0);
                load_a
                    .partial_cmp(&load_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(id, _)| id.clone())
    }

    /// Increment query count for an SSP
    pub fn increment_query_count(&mut self, ssp_id: &str) {
        if let Some(ssp) = self.ssps.get_mut(ssp_id) {
            ssp.query_count += 1;
        }
    }

    /// Move `from`'s query count onto `to` (a promoted standby takes over
    /// every view its predecessor held).
    pub fn transfer_query_count(&mut self, from: &str, to: &str) {
        let moved = self.ssps.get_mut(from).map(|s| std::mem::take(&mut s.query_count)).unwrap_or(0);
        if let Some(ssp) = self.ssps.get_mut(to) {
            ssp.query_count += moved;
        }
    }

    /// Decrement query count for an SSP
    pub fn decrement_query_count(&mut self, ssp_id: &str) {
        if let Some(ssp) = self.ssps.get_mut(ssp_id) {
            ssp.query_count = ssp.query_count.saturating_sub(1);
        }
    }

    /// Get SSPs that haven't sent a heartbeat within the timeout
    pub fn get_stale_ssps(&self, timeout_ms: u64) -> Vec<String> {
        let now = Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);

        self.ssps
            .iter()
            .filter(|(_, info)| now.duration_since(info.last_heartbeat) > timeout)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Record that `successor` replaces `replaced` (see `replaced_by`).
    pub fn mark_replaced(&mut self, replaced: &str, successor: &str) {
        self.replaced_by.insert(replaced.to_string(), successor.to_string());
    }

    /// The live SSP that replaced `ssp_id`, if any. A successor that left the
    /// pool (a swap that was abandoned) no longer blocks its predecessor.
    pub fn replaced_by(&self, ssp_id: &str) -> Option<&str> {
        self.replaced_by
            .get(ssp_id)
            .map(String::as_str)
            .filter(|successor| self.ssps.contains_key(*successor))
    }

    /// Whether `predecessor` is still on its way to serving: registered and
    /// bootstrapping or replaying. A standby for it waits instead of taking
    /// over, or both would end up serving.
    pub fn is_coming_up(&self, ssp_id: &str) -> bool {
        matches!(self.ssp_states.get(ssp_id), Some(SspState::Bootstrapping | SspState::Replaying))
    }

    /// Take `ssp_id` in as the standby for `predecessor` (blue/green).
    pub fn mark_standby(&mut self, ssp_id: &str, predecessor: &str) {
        self.standby_of.insert(ssp_id.to_string(), predecessor.to_string());
    }

    /// The SSP `ssp_id` is standing by to replace, if it is a standby.
    pub fn standby_predecessor(&self, ssp_id: &str) -> Option<&str> {
        self.standby_of.get(ssp_id).map(String::as_str)
    }

    pub fn is_standby(&self, ssp_id: &str) -> bool {
        self.standby_of.contains_key(ssp_id)
    }

    /// Promotion done: `ssp_id` is an ordinary SSP from now on.
    pub fn clear_standby(&mut self, ssp_id: &str) {
        self.standby_of.remove(ssp_id);
    }

    /// Ready standbys waiting to replace `predecessor`: where a view
    /// registration or teardown sent to it is shadowed.
    pub fn ready_standbys_of(&self, predecessor: &str) -> Vec<SspInfo> {
        self.standby_of
            .iter()
            .filter(|(id, pred)| pred.as_str() == predecessor && self.is_ready(id))
            .filter_map(|(id, _)| self.ssps.get(id).cloned())
            .collect()
    }

    /// An SSP serving views and jobs right now: `Ready` or `Lagging`, not a
    /// standby, not retiring. What a standby may be promoted in place of.
    pub fn is_serving(&self, ssp_id: &str) -> bool {
        !self.standby_of.contains_key(ssp_id)
            && matches!(self.ssp_states.get(ssp_id), Some(SspState::Ready | SspState::Lagging))
    }

    /// Start retiring `ssp_id`: off the live path, events queued for it.
    pub fn mark_retiring(&mut self, ssp_id: &str) {
        if self.ssps.contains_key(ssp_id) {
            self.ssp_states.insert(ssp_id.to_string(), SspState::Retiring);
            self.state_since.insert(ssp_id.to_string(), Instant::now());
        }
    }

    /// Finish retiring `ssp_id`: nothing more goes to it, and the events
    /// queued while it was retiring are dropped (its standby has them).
    pub fn mark_retired(&mut self, ssp_id: &str) {
        if self.ssps.contains_key(ssp_id) {
            self.ssp_states.insert(ssp_id.to_string(), SspState::Retired);
            self.state_since.insert(ssp_id.to_string(), Instant::now());
            self.message_buffers.remove(ssp_id);
            self.buffer_overflowed.remove(ssp_id);
            self.forced_resync.remove(ssp_id);
        }
    }

    /// Take back a retire: the SSP lags behind by exactly the events queued
    /// while it was retiring, and the caller starts their redelivery.
    pub fn unretire(&mut self, ssp_id: &str) -> bool {
        if self.ssp_states.get(ssp_id) == Some(&SspState::Retiring) {
            self.ssp_states.insert(ssp_id.to_string(), SspState::Lagging);
            self.state_since.insert(ssp_id.to_string(), Instant::now());
            return true;
        }
        false
    }

    pub fn is_retired(&self, ssp_id: &str) -> bool {
        matches!(self.ssp_states.get(ssp_id), Some(SspState::Retiring | SspState::Retired))
    }

    /// Everything a successor scheduler needs to carry this pool on.
    pub fn export(&self) -> Vec<SspSnapshot> {
        self.ssps
            .values()
            .filter_map(|info| {
                let state = *self.ssp_states.get(&info.id)?;
                Some(SspSnapshot {
                    info: info.clone(),
                    state,
                    buffer: self
                        .message_buffers
                        .get(&info.id)
                        .map(|b| b.iter().cloned().collect())
                        .unwrap_or_default(),
                    bootstrap_seq: self.ssp_snapshot_seqs.get(&info.id).copied(),
                    registration_gen: self.registration_gen(&info.id),
                    buffer_overflowed: self.buffer_overflowed.contains(&info.id),
                    forced_resync: self.forced_resync.get(&info.id).copied(),
                    standby_of: self.standby_of.get(&info.id).cloned(),
                    replaced: self
                        .replaced_by
                        .iter()
                        .find(|(_, successor)| **successor == info.id)
                        .map(|(replaced, _)| replaced.clone()),
                })
            })
            .collect()
    }

    /// Take over a predecessor's pool. Heartbeat clocks start now: the
    /// predecessor stopped answering heartbeats a moment ago, and an SSP must
    /// not be counted stale for that.
    pub fn import(&mut self, snapshots: Vec<SspSnapshot>) {
        let now = Instant::now();
        for snap in snapshots {
            let id = snap.info.id.clone();
            let mut info = snap.info;
            info.last_heartbeat = now;
            self.ssps.insert(id.clone(), info);
            self.ssp_states.insert(id.clone(), snap.state);
            self.state_since.insert(id.clone(), now);
            if !snap.buffer.is_empty() {
                self.message_buffers.insert(id.clone(), snap.buffer.into_iter().collect());
            }
            if let Some(seq) = snap.bootstrap_seq {
                self.ssp_snapshot_seqs.insert(id.clone(), seq);
            }
            let gen = self.registration_gen.entry(id.clone()).or_insert(0);
            *gen = (*gen).max(snap.registration_gen);
            if snap.buffer_overflowed {
                self.buffer_overflowed.insert(id.clone());
            }
            if let Some(kind) = snap.forced_resync {
                self.forced_resync.insert(id.clone(), kind);
            }
            if let Some(replaced) = snap.replaced {
                self.replaced_by.insert(replaced, id.clone());
            }
            if let Some(pred) = snap.standby_of {
                self.standby_of.insert(id, pred);
            }
        }
    }

    /// Count of connected SSPs
    pub fn count(&self) -> usize {
        self.ssps.len()
    }

    /// Check if any SSP is currently bootstrapping or replaying
    pub fn has_active_bootstrap(&self) -> bool {
        self.ssp_states
            .values()
            .any(|s| matches!(s, SspState::Bootstrapping | SspState::Replaying))
    }

    /// Like [`has_active_bootstrap`], but ignoring one SSP id.
    ///
    /// Registration uses this with the registering SSP's own id. An SSP that
    /// re-registers (after an integrity mismatch, or after a restart) is still
    /// parked in `Bootstrapping` from its previous attempt, and counting itself
    /// as a "sibling" made the scheduler skip the pre-registration drain and
    /// hand back the very same (possibly stale) hashes it just failed against —
    /// so its one retry could never succeed.
    pub fn has_active_bootstrap_excluding(&self, ssp_id: &str) -> bool {
        self.ssp_states
            .iter()
            .filter(|(id, _)| id.as_str() != ssp_id)
            .any(|(_, s)| matches!(s, SspState::Bootstrapping | SspState::Replaying))
    }

    /// True while this SSP is still bootstrapping or replaying — i.e. before it
    /// has any reason to have sent a heartbeat. The heartbeat-staleness sweep
    /// must skip these: the SSP only starts heartbeating once it goes Ready, so
    /// any bootstrap slower than the sweep's timeout would otherwise be evicted
    /// mid-flight (and then 404 on its first heartbeat → exit(3) → restart →
    /// same again, with the cluster stuck at zero ready SSPs). Bootstraps that
    /// genuinely hang are reaped by `stale_active_bootstraps` instead, which is
    /// budgeted against `bootstrap_timeout_secs`.
    pub fn is_active_bootstrap(&self, ssp_id: &str) -> bool {
        matches!(
            self.ssp_states.get(ssp_id),
            Some(SspState::Bootstrapping) | Some(SspState::Replaying)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> SspPool {
        SspPool::new(LoadBalanceStrategy::RoundRobin, 100)
    }

    #[test]
    fn catchup_failures_count_up_then_reset() {
        let mut p = pool();
        assert_eq!(p.record_catchup_failure("ssp-0"), 1);
        assert_eq!(p.record_catchup_failure("ssp-0"), 2);
        assert_eq!(p.record_catchup_failure("ssp-0"), 3);
        // Independent per SSP.
        assert_eq!(p.record_catchup_failure("ssp-1"), 1);
        // Reset clears only the named SSP and restarts its streak.
        p.reset_catchup_failures("ssp-0");
        assert_eq!(p.record_catchup_failure("ssp-0"), 1);
        assert_eq!(p.record_catchup_failure("ssp-1"), 2);
    }

    #[test]
    fn bootstrap_failures_count_up_and_clear_on_ready() {
        let mut p = pool();
        assert_eq!(p.record_bootstrap_failure("ssp-1"), 1);
        assert_eq!(p.record_bootstrap_failure("ssp-1"), 2);
        // Going Ready ends the streak — the next failure starts over.
        let _ = p.mark_ready("ssp-1");
        assert_eq!(p.record_bootstrap_failure("ssp-1"), 1);
        p.reset_bootstrap_failures("ssp-1");
        assert_eq!(p.record_bootstrap_failure("ssp-1"), 1);
    }

    fn update(id: &str) -> RecordUpdate {
        RecordUpdate {
            table: "game".to_string(),
            operation: crate::messages::RecordOp::Create,
            record_id: id.to_string(),
            data: None,
            version: 0,
            job_assignee: None,
        }
    }

    fn with_ssp(p: &mut SspPool, id: &str) {
        p.update_ssp(id, 0, None, None, "test".to_string());
        p.mark_bootstrapping(id);
    }

    #[test]
    fn a_standby_follows_ingest_but_gets_no_views_and_no_jobs() {
        let mut p = pool();
        with_ssp(&mut p, "ssp-0");
        let _ = p.mark_ready("ssp-0");
        with_ssp(&mut p, "ssp-0-g1");
        p.mark_standby("ssp-0-g1", "ssp-0");
        let _ = p.mark_ready("ssp-0-g1");

        assert!(p.is_ready("ssp-0-g1"), "a ready standby is broadcast to");
        for _ in 0..4 {
            assert_eq!(p.select_for_query().as_deref(), Some("ssp-0"));
            assert_eq!(p.select_job_runner().as_deref(), Some("ssp-0"));
        }
        assert!(!p.takes_jobs("ssp-0-g1"));
        assert!(p.job_runners().iter().all(|s| s.id != "ssp-0-g1"));
        assert_eq!(
            p.ready_standbys_of("ssp-0").iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["ssp-0-g1"]
        );
        assert!(p.is_serving("ssp-0"));
        assert!(!p.is_serving("ssp-0-g1"));
    }

    #[test]
    fn retiring_queues_events_and_unretire_hands_them_back() {
        let mut p = pool();
        with_ssp(&mut p, "ssp-0");
        let _ = p.mark_ready("ssp-0");
        p.mark_retiring("ssp-0");
        assert!(!p.is_ready("ssp-0"), "off the live path");
        assert!(p.is_retired("ssp-0"));
        assert!(p.buffer_message("ssp-0", update("game:1")));
        assert_eq!(p.buffer_size("ssp-0"), 1);
        assert_eq!(p.select_for_query(), None);
        assert!(p.job_runners().iter().any(|s| s.id == "ssp-0"), "kills still reach it");

        assert!(p.unretire("ssp-0"));
        assert!(p.is_lagging("ssp-0"));
        assert_eq!(p.buffer_size("ssp-0"), 1, "the missed event waits for redelivery");
    }

    #[test]
    fn promotion_retires_the_predecessor_and_moves_its_query_count() {
        let mut p = pool();
        with_ssp(&mut p, "ssp-0");
        let _ = p.mark_ready("ssp-0");
        p.increment_query_count("ssp-0");
        p.increment_query_count("ssp-0");
        with_ssp(&mut p, "ssp-0-g1");
        p.mark_standby("ssp-0-g1", "ssp-0");
        let _ = p.mark_ready("ssp-0-g1");
        p.mark_retiring("ssp-0");
        assert!(p.buffer_message("ssp-0", update("game:1")));

        p.clear_standby("ssp-0-g1");
        p.mark_retired("ssp-0");
        p.transfer_query_count("ssp-0", "ssp-0-g1");

        assert_eq!(p.get_state("ssp-0"), Some(&SspState::Retired));
        assert_eq!(p.buffer_size("ssp-0"), 0, "its standby has those events");
        assert!(p.buffer_message("ssp-0", update("game:2")), "a retired SSP takes nothing");
        assert_eq!(p.buffer_size("ssp-0"), 0);
        assert_eq!(p.get("ssp-0-g1").map(|s| s.query_count), Some(2));
        assert_eq!(p.select_for_query().as_deref(), Some("ssp-0-g1"));
        assert!(!p.unretire("ssp-0"), "only a retiring SSP can be taken back");
    }

    #[test]
    fn a_replaced_ssp_is_refused_only_while_its_successor_lives() {
        let mut p = pool();
        with_ssp(&mut p, "ssp-0-g1");
        p.mark_replaced("ssp-0", "ssp-0-g1");
        assert_eq!(p.replaced_by("ssp-0"), Some("ssp-0-g1"));
        assert_eq!(p.replaced_by("ssp-0-g1"), None);
        // The successor left (a swap that was abandoned): the old one may
        // register again.
        p.remove("ssp-0-g1");
        assert_eq!(p.replaced_by("ssp-0"), None);
    }

    #[test]
    fn a_predecessor_re_registering_is_coming_up_not_serving() {
        let mut p = pool();
        with_ssp(&mut p, "ssp-0");
        assert!(p.is_coming_up("ssp-0"));
        assert!(!p.is_serving("ssp-0"));
        let _ = p.mark_ready("ssp-0");
        assert!(!p.is_coming_up("ssp-0"));
        assert!(p.is_serving("ssp-0"));
        assert!(!p.is_coming_up("nobody"));
    }

    #[test]
    fn export_import_carries_the_pool_to_a_successor() {
        let mut p = pool();
        with_ssp(&mut p, "ssp-0");
        let _ = p.mark_ready("ssp-0");
        assert!(p.mark_lagging("ssp-0"));
        assert!(p.buffer_message("ssp-0", update("game:1")));
        p.set_bootstrap_seq("ssp-0", 41);
        let _ = p.bump_registration_gen("ssp-0");
        with_ssp(&mut p, "ssp-0-g1");
        p.mark_standby("ssp-0-g1", "ssp-0");
        with_ssp(&mut p, "ssp-1-g2");
        p.mark_replaced("ssp-1", "ssp-1-g2");

        let wire = serde_json::to_string(&p.export()).unwrap();
        let mut q = pool();
        q.import(serde_json::from_str(&wire).unwrap());

        assert!(q.is_lagging("ssp-0"));
        assert_eq!(q.buffer_size("ssp-0"), 1);
        assert_eq!(q.get_bootstrap_seq("ssp-0"), Some(41));
        assert_eq!(q.registration_gen("ssp-0"), 1);
        assert_eq!(q.standby_predecessor("ssp-0-g1"), Some("ssp-0"));
        assert_eq!(q.replaced_by("ssp-1"), Some("ssp-1-g2"));
        assert_eq!(q.get_state("ssp-0-g1"), Some(&SspState::Bootstrapping));
        assert!(q.get_stale_ssps(1_000).is_empty(), "heartbeat clocks restart on import");
    }

    #[test]
    fn jobs_go_to_a_ready_ssp_first_then_a_lagging_one() {
        let mut p = SspPool::new(LoadBalanceStrategy::RoundRobin, 1);
        with_ssp(&mut p, "ssp-boot");
        with_ssp(&mut p, "ssp-lag");
        with_ssp(&mut p, "ssp-ready");
        assert_eq!(p.select_job_runner(), None, "a bootstrapping SSP takes no jobs");

        let _ = p.mark_ready("ssp-lag");
        assert!(p.mark_lagging("ssp-lag"));
        let _ = p.mark_ready("ssp-ready");
        for _ in 0..3 {
            assert_eq!(p.select_job_runner().as_deref(), Some("ssp-ready"));
        }
        let mut runners: Vec<String> = p.job_runners().into_iter().map(|s| s.id).collect();
        runners.sort();
        assert_eq!(runners, ["ssp-lag", "ssp-ready"], "a kill reaches both");

        // No SSP Ready: the lagging one, while its queue is intact.
        p.remove("ssp-ready");
        assert_eq!(p.select_job_runner().as_deref(), Some("ssp-lag"));
        assert!(p.buffer_message("ssp-lag", update("game:1")));
        assert!(!p.buffer_message("ssp-lag", update("game:2")), "overflow");
        assert!(!p.takes_jobs("ssp-lag"));
        assert_eq!(p.select_job_runner(), None, "its queue was dropped; it re-bootstraps");
        assert_eq!(p.job_runners().len(), 1, "but a job it already runs can still be killed");
    }

    #[test]
    fn queued_jobs_are_only_creates_queued_for_their_assignee() {
        let mut p = pool();
        with_ssp(&mut p, "ssp-0");
        with_ssp(&mut p, "ssp-1");
        let assigned = |id: &str, op: crate::messages::RecordOp, to: Option<&str>| RecordUpdate {
            operation: op,
            job_assignee: to.map(str::to_string),
            ..update(id)
        };
        use crate::messages::RecordOp::{Create, Update};
        assert!(p.buffer_message("ssp-0", assigned("job:mine", Create, Some("ssp-0"))));
        assert!(p.buffer_message("ssp-0", assigned("job:theirs", Create, Some("ssp-1"))));
        assert!(p.buffer_message("ssp-0", assigned("job:nobody", Create, None)));
        assert!(p.buffer_message("ssp-0", assigned("job:done", Update, Some("ssp-0"))));
        assert!(p.buffer_message("ssp-1", assigned("job:other", Create, Some("ssp-1"))));
        let mut queued: Vec<String> = p.queued_jobs().into_iter().collect();
        queued.sort();
        assert_eq!(queued, ["job:mine", "job:other"]);
    }

    #[test]
    fn lagging_is_entered_only_from_ready_and_buffers_in_order() {
        let mut p = pool();
        // Not registered / bootstrapping: a failed delivery is not "lagging".
        assert!(!p.mark_lagging("ssp-0"));
        p.mark_bootstrapping("ssp-0");
        assert!(!p.mark_lagging("ssp-0"));

        let _ = p.mark_ready("ssp-0");
        assert!(p.mark_lagging("ssp-0"), "Ready → Lagging");
        assert!(!p.mark_lagging("ssp-0"), "second failure: already lagging, no new task");
        assert!(p.is_lagging("ssp-0"));
        assert!(!p.is_ready("ssp-0"), "a lagging SSP is not a live broadcast target");
        assert!(!p.is_active_bootstrap("ssp-0"), "lagging must not freeze the snapshot");
        assert!(!p.has_active_bootstrap());

        // Events queue behind the missed one, in order, and a partial
        // redelivery resumes from the first unacknowledged event.
        assert!(p.buffer_message("ssp-0", update("game:1")));
        assert!(p.buffer_message("ssp-0", update("game:2")));
        assert!(p.buffer_message("ssp-0", update("game:3")));
        let batch = p.drain_buffer("ssp-0");
        assert_eq!(
            batch.iter().map(|u| u.record_id.as_str()).collect::<Vec<_>>(),
            ["game:1", "game:2", "game:3"]
        );
        p.requeue_front("ssp-0", batch[1..].to_vec());
        assert!(p.buffer_message("ssp-0", update("game:4")));
        assert_eq!(
            p.drain_buffer("ssp-0").iter().map(|u| u.record_id.as_str()).collect::<Vec<_>>(),
            ["game:2", "game:3", "game:4"]
        );

        // Caught up: back to Ready, buffer handed over atomically.
        assert!(p.buffer_message("ssp-0", update("game:5")));
        let remaining = p.mark_ready("ssp-0");
        assert_eq!(remaining.len(), 1);
        assert!(p.is_ready("ssp-0"));
        assert!(!p.is_lagging("ssp-0"));
    }

    #[test]
    fn active_bootstrap_check_can_exclude_the_caller() {
        let mut p = pool();
        p.mark_bootstrapping("ssp-1");

        // Its own entry makes the plain check true...
        assert!(p.has_active_bootstrap());
        // ...but a re-registering ssp-1 must not count itself as a sibling,
        // or registration skips the drain and hands back the same hashes.
        assert!(!p.has_active_bootstrap_excluding("ssp-1"));

        p.mark_replaying("ssp-0");
        assert!(p.has_active_bootstrap_excluding("ssp-1"));
    }

    #[test]
    fn is_active_bootstrap_tracks_state() {
        let mut p = pool();
        p.mark_bootstrapping("ssp-1");
        assert!(p.is_active_bootstrap("ssp-1"));
        p.mark_replaying("ssp-1");
        assert!(p.is_active_bootstrap("ssp-1"));
        // Once Ready it heartbeats for itself and the stale sweep may judge it.
        let _ = p.mark_ready("ssp-1");
        assert!(!p.is_active_bootstrap("ssp-1"));
        assert!(!p.is_active_bootstrap("never-seen"));
    }

    #[test]
    fn overflow_flag_is_explicit_not_inferred_from_an_empty_buffer() {
        let mut p = SspPool::new(LoadBalanceStrategy::RoundRobin, 2);
        p.mark_bootstrapping("ssp-1");

        let msg = || RecordUpdate {
            table: "game".to_string(),
            operation: crate::messages::RecordOp::Update,
            record_id: "game:r1".to_string(),
            data: None,
            version: 0,
            job_assignee: None,
        };

        // Normal buffering then a normal drain leaves the entry present but
        // empty — that is NOT an overflow (the old inference said it was, so
        // any heartbeat in the drain→mark_ready window got a 409 → exit(4)).
        assert!(p.buffer_message("ssp-1", msg()));
        assert_eq!(p.drain_buffer("ssp-1").len(), 1);
        assert!(!p.has_buffer_overflow("ssp-1"));

        // A real overflow drops the buffer and latches the flag.
        assert!(p.buffer_message("ssp-1", msg()));
        assert!(p.buffer_message("ssp-1", msg()));
        assert!(!p.buffer_message("ssp-1", msg()));
        assert!(p.has_buffer_overflow("ssp-1"));

        // Cleared once the SSP is admitted.
        let _ = p.mark_ready("ssp-1");
        assert!(!p.has_buffer_overflow("ssp-1"));
    }

    #[test]
    fn remove_clears_catchup_failures() {
        let mut p = pool();
        p.record_catchup_failure("ssp-0");
        p.record_catchup_failure("ssp-0");
        p.remove("ssp-0");
        // A re-registered SSP of the same id starts a fresh streak.
        assert_eq!(p.record_catchup_failure("ssp-0"), 1);
    }

    #[test]
    fn stale_active_bootstraps_respects_age_and_state() {
        let mut p = pool();
        p.mark_bootstrapping("ssp-boot");
        p.mark_replaying("ssp-replay");
        p.mark_bootstrapping("ssp-done");
        let _ = p.mark_ready("ssp-done");

        let now = Instant::now();
        let bound = Duration::from_secs(180);

        // Fresh: nothing is stale yet.
        assert!(p.stale_active_bootstraps_at(now, bound).is_empty());

        // Past the bound: both parked states are stale; Ready never is.
        let later = now + bound + Duration::from_secs(1);
        let mut stale = p.stale_active_bootstraps_at(later, bound);
        stale.sort();
        assert_eq!(stale, vec!["ssp-boot".to_string(), "ssp-replay".to_string()]);

        // A state refresh resets the clock.
        p.mark_replaying("ssp-boot");
        // (ssp-boot's state_since is now ~Instant::now(); using `later` from
        // before that stamp would underflow duration_since, so re-anchor.)
        let re_anchor = Instant::now() + bound + Duration::from_secs(1);
        let stale = p.stale_active_bootstraps_at(re_anchor, bound);
        assert!(stale.contains(&"ssp-boot".to_string()));
        assert!(p
            .stale_active_bootstraps_at(Instant::now(), bound)
            .iter()
            .all(|id| id != "ssp-boot"));
    }

    #[test]
    fn eviction_clears_active_bootstrap_latch() {
        let mut p = pool();
        p.mark_replaying("ssp-parked");
        assert!(p.has_active_bootstrap());
        for id in p.stale_active_bootstraps_at(
            Instant::now() + Duration::from_secs(999),
            Duration::from_secs(1),
        ) {
            p.remove(&id);
        }
        assert!(!p.has_active_bootstrap());
    }

    #[test]
    fn registration_gen_is_monotonic_across_remove() {
        let mut p = pool();
        assert_eq!(p.registration_gen("ssp-0"), 0, "never registered");
        assert_eq!(p.bump_registration_gen("ssp-0"), 1);
        assert_eq!(p.bump_registration_gen("ssp-0"), 2);
        assert_eq!(p.registration_gen("ssp-0"), 2);
        // Gen survives removal so a stale poll task always loses the compare.
        p.remove("ssp-0");
        assert_eq!(p.registration_gen("ssp-0"), 2);
        assert_eq!(p.bump_registration_gen("ssp-0"), 3);
        // Independent per SSP id.
        assert_eq!(p.bump_registration_gen("ssp-1"), 1);
    }
}
