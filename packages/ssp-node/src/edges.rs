//! Query edge-update service (ported from `apps/ssp/src/edge_updates.rs`).
//!
//! The circuit emits a [`ViewDelta`] whenever a registered query's window
//! changes. The production [`EdgePublisher`] retains bounded admission through
//! ordered publication and retries. Legacy coalescing helpers remain available
//! for callers using the lower-level sink interface.
//!
//! Portability note: the previous shell version bound the `_00_query`
//! incantation record id as a `surrealdb::RecordId` param (`$fromN`). The core
//! can't depend on the SDK, so the incantation now crosses the [`Db`] port as
//! a plain **string** key and the SQL wraps it in `type::record('_00_query',
//! $fromN)` — preserving the original bind safety (arbitrary keys) without a
//! RecordId. The `out`/`parent`/subquery record ids keep the existing literal
//! interpolation (they arrive already-validated from the circuit).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use crate::db_retry::query_retrying;

use ssp::circuit::{Circuit, SubqueryOp, ViewDelta};
use ssp_protocol::RefMode;

use crate::ports::{Db, Scheduler, Telemetry};
use crate::tables;

/// Cap on buffered deltas between flushes. A sustained flood flushes early once
/// it crosses this, so the buffer (and resulting transaction) stay bounded.
pub const MAX_EDGE_BATCH: usize = 4096;

/// Looks up the stored version for a record key. Abstracts `Circuit.store` so
/// [`build_edge_batch`] is pure and unit-testable without a circuit.
pub trait RecordVersions {
    fn version_of(&self, key: &str) -> i64;
}

/// `RecordVersions` over a live circuit store.
pub struct CircuitVersions<'a>(pub &'a Circuit);
impl RecordVersions for CircuitVersions<'_> {
    fn version_of(&self, key: &str) -> i64 {
        self.0.store.get_record_version_by_key(key).unwrap_or(1)
    }
}

/// An immutable version snapshot. Never retain the circuit guard during I/O.
struct CapturedVersions(HashMap<String, i64>);
impl RecordVersions for CapturedVersions {
    fn version_of(&self, key: &str) -> i64 { self.0.get(key).copied().unwrap_or(1) }
}

/// The production publication boundary: capture only the required versions,
/// then release the circuit before database calls, retries and orphan probes.
pub async fn write_deltas_unlocked(
    db: &dyn Db,
    mut deltas: Vec<ViewDelta>,
    processor: &RwLock<Circuit>,
    publication_gate: &tokio::sync::Mutex<()>,
    mode: RefMode,
    telemetry: &dyn Telemetry,
) -> Vec<ViewDelta> {
    let _publication = publication_gate.lock().await;
    let wait = web_time::Instant::now();
    let versions = {
        let circuit = processor.read().await;
        telemetry.histogram_ms("edge_lock_wait", wait.elapsed().as_secs_f64() * 1000.0);
        let hold = web_time::Instant::now();
        deltas.retain(|d| circuit.is_registered(&d.query_id));
        let source = CircuitVersions(&circuit);
        let mut versions = HashMap::new();
        for d in &deltas {
            for key in d.additions.iter().chain(&d.updates).chain(d.subquery_items.iter().map(|i| &i.id)) {
                versions.entry(key.clone()).or_insert_with(|| source.version_of(key));
            }
        }
        telemetry.histogram_ms("edge_lock_hold", hold.elapsed().as_secs_f64() * 1000.0);
        CapturedVersions(versions)
    };
    let start = web_time::Instant::now();
    let left = write_deltas_with_versions(db, deltas, &versions, mode, telemetry).await;
    telemetry.histogram_ms("edge_publish", start.elapsed().as_secs_f64() * 1000.0);
    left
}


// Cooperative yield without depending on Tokio's native runtime feature.
async fn publication_yield() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded { std::task::Poll::Ready(()) } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }).await
}

/// Admission thresholds for the production publication queue. Slots are a hard
/// limit, including requests preparing their deltas. Bytes and operations are
/// measured after circuit evaluation: already admitted slots may overshoot
/// these thresholds, and new admission then stops until publication catches up.
#[derive(Clone, Copy)]
pub struct PublicationLimits {
    pub slots: usize,
    pub bytes: u64,
    pub operations: u64,
}
impl Default for PublicationLimits {
    fn default() -> Self { Self { slots: 256, bytes: 64 * 1024 * 1024, operations: 100_000 } }
}

#[derive(Clone)]
pub struct EdgePublisher(Arc<PublicationQueue>);
struct PublicationQueue {
    state: std::sync::Mutex<PublicationState>,
    wake: tokio::sync::Notify,
    started: std::sync::atomic::AtomicBool,
    telemetry: std::sync::OnceLock<Arc<dyn Telemetry>>,
    limits: PublicationLimits,
}
#[derive(Default)]
struct PublicationState {
    generation: u64,
    next: u64,
    epochs: HashMap<String, u64>,
    leases: HashMap<u64, PublicationLeaseStats>,
    queue: std::collections::VecDeque<PublicationWork>,
    last_success: Option<u64>,
    overloaded: u64,
}
struct PublicationLeaseStats {
    at: u64,
    bytes: u64,
    operations: u64,
    parked: bool,
    views: Vec<(String, u64, u64)>,
}
/// A reservation must be obtained before any ingest side effects. Dropping an
/// unqueued reservation returns capacity, including validation/error paths.
pub struct PublicationPermit {
    queue: std::sync::Weak<PublicationQueue>,
    id: u64,
    generation: u64,
}
impl Drop for PublicationPermit {
    fn drop(&mut self) {
        if let Some(queue) = self.queue.upgrade() {
            queue.state.lock().unwrap().leases.remove(&self.id);
            queue.wake.notify_one();
        }
    }
}
/// Table lifecycle and orphan deletes share the same ordered publication lane.
pub enum PublicationCleanup {
    Statement(String),
    EnsureUser(String),
    DropUser(String),
}
impl PublicationCleanup {
    fn bytes(&self) -> u64 {
        match self { Self::Statement(s) | Self::EnsureUser(s) | Self::DropUser(s) => s.capacity() as u64 }
    }
}
struct PublicationWork {
    permit: Arc<PublicationPermit>,
    deltas: Vec<ViewDelta>,
    versions: CapturedVersions,
    epochs: HashMap<String, u64>,
    source: Option<(String, i64)>,
    source_ready: Arc<std::sync::atomic::AtomicBool>,
    source_cancel: Arc<tokio::sync::Notify>,
    cleanup: Vec<PublicationCleanup>,
    ready: Arc<std::sync::atomic::AtomicU8>,
    retry_at: Option<web_time::Instant>,
}
impl Drop for PublicationWork {
    fn drop(&mut self) { self.source_cancel.notify_one(); }
}
/// Registration metadata must exist before its initial publication can run.
/// A dropped handler cancels its slot and invalidates dependent queued work.
pub struct PublicationReady {
    publisher: EdgePublisher,
    ready: Arc<std::sync::atomic::AtomicU8>,
    epochs: HashMap<String, u64>,
    generation: u64,
    complete: bool,
}
impl PublicationReady {
    pub fn is_current(&self) -> bool {
        let state = self.publisher.0.state.lock().unwrap();
        state.generation == self.generation && self.epochs.iter().all(|(id, epoch)|
            state.epochs.get(id).copied().unwrap_or(0) == *epoch)
    }
    pub fn complete(mut self) {
        self.complete = true;
        self.ready.store(1, std::sync::atomic::Ordering::Release);
        self.publisher.0.wake.notify_one();
    }
}
impl Drop for PublicationReady {
    fn drop(&mut self) {
        if !self.complete {
            self.ready.store(2, std::sync::atomic::Ordering::Release);
            let mut state = self.publisher.0.state.lock().unwrap();
            if state.generation == self.generation {
                for (id, epoch) in &self.epochs {
                    if state.epochs.get(id).copied().unwrap_or(0) == *epoch {
                        state.epochs.remove(id);
                    }
                }
            }
            drop(state);
            self.publisher.0.wake.notify_one();
        }
    }
}

impl EdgePublisher {
    pub fn new(limits: PublicationLimits) -> Self {
        Self(Arc::new(PublicationQueue { state: Default::default(), wake: Default::default(),
            started: Default::default(), telemetry: Default::default(), limits }))
    }
    /// The estimate covers request-owned input before evaluation. Oversize input
    /// is refused immediately. Delta sizes replace this estimate when enqueued.
    pub fn try_reserve(&self, input_bytes: usize) -> Option<PublicationPermit> {
        let mut state = self.0.state.lock().unwrap();
        let bytes = state.leases.values().map(|s| s.bytes).sum::<u64>();
        let operations = state.leases.values().map(|s| s.operations).sum::<u64>();
        if state.leases.len() >= self.0.limits.slots
            || bytes.saturating_add(input_bytes as u64) > self.0.limits.bytes
            || operations >= self.0.limits.operations {
            state.overloaded += 1;
            return None;
        }
        let id = state.next;
        state.next += 1;
        let generation = state.generation;
        state.leases.insert(id, PublicationLeaseStats { at: crate::now_epoch_ms(), bytes: input_bytes as u64,
            operations: 0, parked: false, views: vec![] });
        Some(PublicationPermit { queue: Arc::downgrade(&self.0), id, generation })
    }
    pub fn is_current(&self, permit: &PublicationPermit) -> bool {
        self.0.state.lock().unwrap().generation == permit.generation
    }
    /// Call under the publication gate and circuit lifecycle lock.
    pub fn invalidate_all(&self) {
        let mut state = self.0.state.lock().unwrap();
        state.generation += 1;
        state.epochs.clear();
        drop(state);
        self.0.wake.notify_one();
    }
    /// Call under the circuit lock on every cold registration, and under the
    /// publication gate before unregistering. Reused IDs receive a new epoch.
    pub fn invalidate_view(&self, query_id: &str) {
        let mut state = self.0.state.lock().unwrap();
        state.epochs.remove(&ssp::canonical_query_id(query_id));
        drop(state);
        self.0.wake.notify_one();
    }
    pub fn prune_epochs(&self, circuit: &Circuit) {
        self.0.state.lock().unwrap().epochs.retain(|id, _| circuit.is_registered(id));
    }
    /// Enqueue at the circuit mutation/snapshot point, while its lock is held.
    /// A registration passes `metadata_pending`; complete the returned guard
    /// only after its metadata write succeeds.
    pub fn enqueue(&self, permit: PublicationPermit, deltas: Vec<ViewDelta>, circuit: &Circuit,
        source: Option<(String, i64)>, metadata_pending: bool, cleanup: Vec<PublicationCleanup>) -> Option<PublicationReady> {
        if deltas.is_empty() && cleanup.is_empty() { return None; }
        let capture_started = web_time::Instant::now();
        let ready = Arc::new(std::sync::atomic::AtomicU8::new(if metadata_pending { 0 } else { 1 }));
        let mut versions = HashMap::new();
        let mut views = Vec::new();
        for d in &deltas {
            let operations = (d.additions.len() + d.removals.len() + d.updates.len() + d.subquery_items.len()) as u64;
            let bytes = std::mem::size_of::<ViewDelta>() as u64 + d.query_id.len() as u64 + d.auth_id.len() as u64
                + d.result_hash.len() as u64
                + d.additions.iter().chain(&d.removals).chain(&d.updates)
                    .map(|s| (s.capacity() + std::mem::size_of::<String>()) as u64).sum::<u64>()
                + d.subquery_items.iter().map(|s| (std::mem::size_of::<ssp::circuit::SubqueryDeltaItem>()
                    + s.id.capacity() + s.parent_key.capacity() + s.alias.capacity()) as u64).sum::<u64>();
            views.push((d.query_id.clone(), operations, bytes));
            for key in d.additions.iter().chain(&d.updates).chain(d.subquery_items.iter().map(|i| &i.id)) {
                versions.entry(key.clone()).or_insert_with(|| CircuitVersions(circuit).version_of(key));
            }
        }
        let mut state = self.0.state.lock().unwrap();
        let epochs = deltas.iter().map(|d| {
            let id = ssp::canonical_query_id(&d.query_id);
            let epoch = if let Some(epoch) = state.epochs.get(&id) { *epoch } else {
                state.next += 1;
                let epoch = state.next;
                state.epochs.insert(id.clone(), epoch);
                epoch
            };
            (id, epoch)
        }).collect::<HashMap<_, _>>();
        if let Some(stats) = state.leases.get_mut(&permit.id) {
            stats.operations = views.iter().map(|v| v.1).sum::<u64>() + cleanup.len() as u64;
            stats.bytes = views.iter().map(|v| v.2).sum::<u64>() + cleanup.iter().map(PublicationCleanup::bytes).sum::<u64>();
            stats.views = views;
        }
        let generation = permit.generation;
        state.queue.push_back(PublicationWork { permit: Arc::new(permit), deltas, versions: CapturedVersions(versions), epochs: epochs.clone(),
            source_ready: Arc::new(std::sync::atomic::AtomicBool::new(source.is_none())),
            source_cancel: Arc::new(tokio::sync::Notify::new()), source, cleanup, ready: ready.clone(), retry_at: None });
        drop(state);
        self.0.wake.notify_one();
        if let Some(telemetry) = self.0.telemetry.get() {
            telemetry.histogram_ms("edge_lock_hold", capture_started.elapsed().as_secs_f64() * 1000.0);
        }
        metadata_pending.then(|| PublicationReady { publisher: self.clone(), ready, epochs, generation, complete: false })
    }
    pub fn snapshot(&self) -> ssp_protocol::PublicationMetrics {
        let state = self.0.state.lock().unwrap();
        let now = crate::now_epoch_ms();
        let mut views: HashMap<String, ssp_protocol::PublicationViewMetrics> = HashMap::new();
        for lease in state.leases.values() {
            for (id, operations, bytes) in &lease.views {
                let v = views.entry(id.clone()).or_insert_with(|| ssp_protocol::PublicationViewMetrics {
                    query_id: id.clone(), ..Default::default()
                });
                v.pending_operations += operations;
                v.pending_bytes += bytes;
                v.oldest_age_ms = v.oldest_age_ms.max(now.saturating_sub(lease.at));
            }
        }
        let mut worst_views: Vec<_> = views.into_values().collect();
        worst_views.sort_by_key(|v| std::cmp::Reverse(v.pending_bytes));
        worst_views.truncate(8);
        ssp_protocol::PublicationMetrics {
            pending_batches: state.leases.len() as u64,
            pending_operations: state.leases.values().map(|s| s.operations).sum(),
            pending_bytes: state.leases.values().map(|s| s.bytes).sum(),
            oldest_age_ms: state.leases.values().map(|s| now.saturating_sub(s.at)).max().unwrap_or(0),
            parked_batches: state.leases.values().filter(|s| s.parked).count() as u64,
            last_success_at_ms: state.last_success,
            overload_total: state.overloaded,
            worst_views,
            ..Default::default()
        }
    }
    pub fn start(&self, platform: &crate::platform::Platform, db: Arc<dyn Db>, processor: Arc<RwLock<Circuit>>,
        gate: Arc<tokio::sync::Mutex<()>>, mode: RefMode, window: Duration) {
        if self.0.started.swap(true, std::sync::atomic::Ordering::AcqRel) { return; }
        let _ = self.0.telemetry.set(platform.telemetry.clone());
        let publisher = self.clone();
        let scheduler = platform.scheduler.clone();
        let telemetry = platform.telemetry.clone();
        let source_db = platform.db.clone();
        let spawner = platform.spawner.clone();
        platform.spawner.spawn(Box::pin(async move {
            publisher.run(db, processor, gate, mode, scheduler, telemetry, window, source_db, spawner).await;
        }));
    }
    async fn run(&self, db: Arc<dyn Db>, processor: Arc<RwLock<Circuit>>, gate: Arc<tokio::sync::Mutex<()>>,
        mode: RefMode, scheduler: Arc<dyn Scheduler>, telemetry: Arc<dyn Telemetry>, window: Duration,
        source_db: Arc<dyn Db>, spawner: Arc<dyn crate::ports::Spawner>) {
        // Limit active probes, not entire visibility waits: all admitted source
        // deadlines run concurrently, without serial five-second waves.
        let probes = Arc::new(tokio::sync::Semaphore::new(16));
        loop {
            let sources = {
                let mut state = self.0.state.lock().unwrap();
                state.queue.iter_mut().filter_map(|work| work.source.take().map(|source|
                    (source, work.permit.clone(), work.source_ready.clone(), work.source_cancel.clone())))
                    .collect::<Vec<_>>()
            };
            for ((row, version), permit, ready, cancel) in sources {
                let db = source_db.clone();
                let scheduler = scheduler.clone();
                let probes = probes.clone();
                let queue = self.0.clone();
                spawner.spawn(Box::pin(async move {
                    // The shared lease outlives canceled queued work until this
                    // task and its DB future have actually been dropped.
                    let _permit = permit;
                    tokio::select! {
                        _ = cancel.notified() => {},
                        _ = scheduler.sleep(Duration::from_secs(5)) => {},
                        _ = crate::node::wait_for_row_committed(db.as_ref(), scheduler.as_ref(), &row, version,
                            Duration::from_secs(5), probes.as_ref()) => {},
                    }
                    ready.store(true, std::sync::atomic::Ordering::Release);
                    queue.wake.notify_one();
                }));
            }
            let notified = self.0.wake.notified();
            let work = {
                let mut state = self.0.state.lock().unwrap();
                let mut blocked = HashSet::new();
                let mut chosen = None;
                for (index, work) in state.queue.iter().enumerate() {
                    let stale = state.generation != work.permit.generation ||
                        (work.cleanup.is_empty() && work.epochs.iter().all(|(id, epoch)|
                            state.epochs.get(id).copied().unwrap_or(0) != *epoch));
                    let ready = work.ready.load(std::sync::atomic::Ordering::Acquire);
                    if stale || ready == 2 { chosen = Some(index); break; }
                    // Cross-view orphan cleanup is a global barrier. Ordinary
                    // publications only wait for earlier work for their views.
                    let global = !work.cleanup.is_empty();
                    if ready == 1 && work.source_ready.load(std::sync::atomic::Ordering::Acquire) && work.retry_at.map_or(true, |at| at <= web_time::Instant::now())
                        && !work.epochs.keys().any(|id| blocked.contains(id))
                        && (!global || index == 0) {
                        chosen = Some(index); break;
                    }
                    blocked.extend(work.epochs.keys().cloned());
                    if global { break; }
                }
                chosen.and_then(|index| state.queue.remove(index))
            };
            let Some(mut work) = work else {
                let idle = self.0.state.lock().unwrap().queue.is_empty();
                if idle {
                    notified.await;
                    // Debounce a new burst once. Never impose a fixed delay
                    // per queued item, which would cap drain throughput.
                    if !window.is_zero() { scheduler.sleep(window).await; }
                } else {
                    tokio::select! { _ = notified => {}, _ = scheduler.sleep(Duration::from_millis(50)) => {} }
                }
                publication_yield().await;
                continue;
            };
            publication_yield().await;
            let obsolete = {
                let state = self.0.state.lock().unwrap();
                state.generation != work.permit.generation || work.ready.load(std::sync::atomic::Ordering::Acquire) == 2
                    || (work.cleanup.is_empty() && work.epochs.iter().all(|(id, epoch)| state.epochs.get(id) != Some(epoch)))
            };
            if obsolete { continue; }
            let publication = gate.lock().await;
            let wait = web_time::Instant::now();
            {
                let circuit = processor.read().await;
                telemetry.histogram_ms("edge_lock_wait", wait.elapsed().as_secs_f64() * 1000.0);
                let hold = web_time::Instant::now();
                let state = self.0.state.lock().unwrap();
                work.deltas.retain(|d| circuit.is_registered(&d.query_id)
                    && state.generation == work.permit.generation
                    && state.epochs.get(&ssp::canonical_query_id(&d.query_id)).copied().unwrap_or(0)
                        == work.epochs[&ssp::canonical_query_id(&d.query_id)]);
                telemetry.histogram_ms("edge_lock_hold", hold.elapsed().as_secs_f64() * 1000.0);
            }
            if self.0.state.lock().unwrap().generation != work.permit.generation { continue; }
            if work.deltas.is_empty() && work.cleanup.is_empty() { continue; }
            let publish = web_time::Instant::now();
            while let Some(cleanup) = work.cleanup.first() {
                let result = match cleanup {
                    PublicationCleanup::Statement(sql) => query_retrying(db.as_ref(), sql, &[]).await.map(|_| ()).map_err(anyhow::Error::from),
                    PublicationCleanup::EnsureUser(user) => crate::tables::ensure_user_tables(db.as_ref(), mode, user).await,
                    PublicationCleanup::DropUser(user) => crate::tables::drop_user_tables(db.as_ref(), mode, user).await,
                };
                match result {
                    Ok(_) => { work.cleanup.remove(0); }
                    Err(e) if crate::tables::is_missing_table_error(&e.to_string()) => { work.cleanup.remove(0); }
                    Err(_) => break,
                }
            }
            if work.cleanup.is_empty() {
                let mut tables_ready = true;
                for delta in work.deltas.iter().filter(|d| d.initial) {
                    if crate::tables::ensure_user_tables(db.as_ref(), mode, &delta.auth_id).await.is_err() {
                        tables_ready = false; break;
                    }
                }
                if tables_ready {
                    work.deltas = write_deltas_with_versions(db.as_ref(), std::mem::take(&mut work.deltas), &work.versions, mode, telemetry.as_ref()).await;
                }
            }
            telemetry.histogram_ms("edge_publish", publish.elapsed().as_secs_f64() * 1000.0);
            drop(publication);
            if work.deltas.is_empty() && work.cleanup.is_empty() {
                self.0.state.lock().unwrap().last_success = Some(crate::now_epoch_ms());
            } else {
                let failed: HashSet<_> = work.deltas.iter().map(|d| ssp::canonical_query_id(&d.query_id)).collect();
                work.epochs.retain(|id, _| failed.contains(id));
                work.retry_at = Some(web_time::Instant::now() + if cfg!(test) { Duration::from_millis(10) } else { Duration::from_secs(5) });
                let mut state = self.0.state.lock().unwrap();
                if let Some(stats) = state.leases.get_mut(&work.permit.id) { stats.parked = true; }
                // Moving ahead of disjoint earlier work is safe; overlapping
                // work could not have been selected in the first place.
                state.queue.push_front(work);
            }
        }
    }

}
impl Default for EdgePublisher {
    fn default() -> Self { Self::new(PublicationLimits::default()) }
}

/// One delta's edge-write statements.
///
/// `preamble` holds the `LET`s the body references (`$fromN`, and the view's
/// `clientId`/`auth_id` resolved once). A `LET` is scoped to its transaction,
/// so every transaction carrying any of `body` repeats the whole preamble —
/// which is also why the preamble is kept apart from the body instead of
/// living at the front of one flat statement list.
#[derive(Debug, Default, PartialEq)]
pub struct DeltaStatements {
    /// Index of the delta in the batch's input slice.
    pub delta: usize,
    pub preamble: Vec<String>,
    /// `fromN` → the `_00_query` record KEY (bound as a string; the SQL wraps
    /// it in `type::record('_00_query', $fromN)`).
    pub bindings: Vec<(String, String)>,
    pub body: Vec<String>,
    /// A full publish opens with `DELETE $from->list_ref`, so re-running it
    /// converges however much of it already landed. That is what makes it
    /// safe to spread across several transactions; an incremental delta has
    /// no such guard (the `RELATE`s are bare, there is no unique index on
    /// `in, out`) and must commit whole or not at all.
    pub idempotent: bool,
    pub created: u64,
    pub updated: u64,
    pub deleted: u64,
}

/// The aggregated edge-write statements for a batch of deltas, one group per
/// delta that had anything to write.
#[derive(Debug, Default, PartialEq)]
pub struct EdgeBatch {
    pub deltas: Vec<DeltaStatements>,
}

impl EdgeBatch {
    pub fn is_empty(&self) -> bool {
        self.deltas.iter().all(|d| d.body.is_empty())
    }

    /// Every statement in execution order, preambles included — the shape a
    /// single-transaction publish has.
    pub fn statements(&self) -> Vec<String> {
        self.deltas
            .iter()
            .flat_map(|d| d.preamble.iter().chain(d.body.iter()).cloned())
            .collect()
    }

    pub fn bindings(&self) -> Vec<(String, String)> {
        self.deltas
            .iter()
            .flat_map(|d| d.bindings.iter().cloned())
            .collect()
    }

    pub fn created(&self) -> u64 {
        self.deltas.iter().map(|d| d.created).sum()
    }

    pub fn updated(&self) -> u64 {
        self.deltas.iter().map(|d| d.updated).sum()
    }

    pub fn deleted(&self) -> u64 {
        self.deltas.iter().map(|d| d.deleted).sum()
    }
}

/// Structural record-id check (`table:key`, both non-empty). Replaces the
/// former `RecordId::parse_simple` guard — the SDK isn't available in the core.
fn is_valid_record_id(id: &str) -> bool {
    matches!(id.split_once(':'), Some((t, k)) if !t.is_empty() && !k.is_empty())
}

/// The `_00_query` incantation record id (`_00_query:<key>`) for a view id.
pub fn format_incantation_id(id: &str) -> String {
    let raw = id.rsplit(':').next().unwrap_or(id);
    format!("_00_query:{}", raw)
}

/// The incantation KEY (`<key>` of `_00_query:<key>`) — what gets bound.
fn incantation_key(id: &str) -> String {
    id.rsplit(':').next().unwrap_or(id).to_string()
}

/// Build the `_00_list_ref` edge-write statements for a batch of deltas. PURE.
/// Each delta binds its `_00_query` key as `$from{idx}` (unique across the
/// batch); the SQL references `type::record('_00_query', $from{idx})`.
/// Whether a delta carries anything the edge writer would put in a
/// transaction. Mirrors the skip at the top of [`build_edge_batch`].
pub fn delta_has_edges(delta: &ViewDelta) -> bool {
    !(delta.additions.is_empty()
        && delta.updates.is_empty()
        && delta.removals.is_empty()
        && delta.subquery_items.is_empty())
}

/// The `_00_query.state` a registration should write for its initial delta.
///
/// `materializing` when the delta will reach the flusher (which flips the row
/// to `ready` in the same transaction as the edges); `ready` when there is
/// nothing to publish, because a skipped delta would otherwise leave the row
/// saying `materializing` forever and the client would never trust its empty
/// result.
pub fn publish_state_for(delta: Option<&ViewDelta>) -> &'static str {
    match delta {
        Some(d) if delta_has_edges(d) => "materializing",
        _ => "ready",
    }
}

pub fn build_edge_batch(
    deltas: &[&ViewDelta],
    mode: RefMode,
    versions: &impl RecordVersions,
) -> EdgeBatch {
    let mut batch = EdgeBatch::default();

    for (idx, delta) in deltas.iter().enumerate() {
        let mut group = DeltaStatements {
            delta: idx,
            idempotent: delta.initial || (delta.additions.is_empty()
                && delta.removals.is_empty()
                && delta.subquery_items.iter().all(|i| i.op == SubqueryOp::Update)),
            ..Default::default()
        };
        // Empty full snapshots still delete stale memberships. Skip only empty
        // incremental deltas. A delta that changes ONLY subquery
        // children (a comment added to a thread already in the view — the
        // parent's membership is unchanged) still carries `subquery_items` that
        // must become `_00_list_ref` edges, so it must NOT be skipped. Skipping
        // it (the old behavior) is why reverse-link children like comments never
        // synced while forward links, whose delta also carried a parent
        // addition/content-update, worked.
        if !delta.initial && !delta_has_edges(delta) {
            continue;
        }

        let incantation_id = format_incantation_id(&delta.query_id);
        let list_ref = tables::list_ref_table(mode, &delta.auth_id);

        if !is_valid_record_id(&incantation_id) {
            error!(incantation_id = %incantation_id, "Invalid incantation ID format - skipping view");
            continue;
        }

        // RELATE/DELETE only accept a record id or a PARAM as the graph
        // endpoint (not a `type::record(...)` expression). So bind the key as a
        // string and `LET $fromN` to the record inside the transaction; every
        // statement then references `$fromN` exactly as the original
        // RecordId-bound code did — but nothing SDK-specific crosses the port.
        let bn = format!("from{}", idx);
        let from = format!("${bn}");
        group
            .preamble
            .push(format!("LET ${bn} = type::record('_00_query', ${bn}key)"));
        group
            .bindings
            .push((format!("{bn}key"), incantation_key(&delta.query_id)));

        // The view's `clientId`/`auth_id`, resolved ONCE per delta. Every
        // `RELATE` used to carry two `(SELECT VALUE … FROM $from LIMIT 1)[0]`
        // subselects of its own, so a cold publish of a 3,861-row view sent
        // 7,722 subselects in a single transaction and SurrealDB stalled for
        // minutes under it — long enough for other registrations to 500 on
        // "Transaction conflict: Resource busy" and for view heartbeats to be
        // lost (whitepawn, 2026-09-08).
        let meta = format!("LET $cid{idx} = (SELECT VALUE clientId FROM {from} LIMIT 1)[0]");
        let auth = format!("LET $aid{idx} = (SELECT VALUE auth_id FROM {from} LIMIT 1)[0]");
        let (cid, aid) = (format!("$cid{idx}"), format!("$aid{idx}"));

        // A full publish REPLACES the row's edges. Registration, repair and
        // subscriber-attach all snapshot the whole membership, and the
        // `RELATE`s below are bare (no unique index on `in, out`), so
        // publishing over edges that already exist - orphans left by a
        // scheduler restart, a view the sweep half-reclaimed, a stranded
        // repair - used to duplicate every row. The unfiltered graph delete is
        // the form the unregister and TTL paths already rely on.
        if delta.initial {
            group
                .body
                .push(format!("DELETE {from}->{list_ref}", from = from, list_ref = list_ref));
        }

        // Additions (Created)
        for id in &delta.additions {
            if !is_valid_record_id(id) {
                error!(target: "ssp::edges", record_id = %id, view_id = %delta.query_id, "Invalid record ID - skipping edge create");
                continue;
            }
            let version = versions.version_of(id);
            group.created += 1;
            group.body.push(format!(
                "RELATE {from}->{list_ref}->{out} SET version = {version}, clientId = {cid}, auth_id = {aid}",
                from = from, list_ref = list_ref, out = id, version = version, cid = cid, aid = aid,
            ));
        }

        // Updates (Updated)
        for id in &delta.updates {
            if !is_valid_record_id(id) {
                error!(target: "ssp::edges", record_id = %id, view_id = %delta.query_id, "Invalid record ID - skipping edge update");
                continue;
            }
            let version = versions.version_of(id);
            group.updated += 1;
            group.body.push(format!(
                "UPDATE (SELECT VALUE id FROM {from}->{list_ref} WHERE out = {out}) SET version = {version} RETURN NONE",
                list_ref = list_ref,
                version = version,
                from = from,
                out = id,
            ));
        }

        // Removals (Deleted)
        for id in &delta.removals {
            if !is_valid_record_id(id) {
                error!(target: "ssp::edges", record_id = %id, view_id = %delta.query_id, "Invalid record ID - skipping edge delete");
                continue;
            }
            group.deleted += 1;
            // Resolve the edge through the graph index, then delete by id.
            // `DELETE $from->edge WHERE out = x` (a filtered graph-path
            // delete) fails on SurrealDB 3.0.x with "Cannot execute DELETE
            // statement using value: NONE": every eviction from a full
            // window (`ORDER BY … LIMIT 30` with a 31st row) failed its whole
            // edge transaction, so the client never saw its own new message in
            // that view and dropped it after the settled-write grace. The
            // unfiltered `DELETE $from->edge` still works and is used as-is
            // by the unregister and TTL paths.
            group.body.push(format!(
                "DELETE (SELECT VALUE id FROM {from}->{list_ref} WHERE out = {out})",
                from = from,
                list_ref = list_ref,
                out = id,
            ));
        }

        // Subquery child edges. Processed AFTER main records so parent
        // list_ref entries exist in the same transaction.
        for item in &delta.subquery_items {
            if !is_valid_record_id(&item.id) {
                error!(target: "ssp::edges", record_id = %item.id, view_id = %delta.query_id, "Invalid subquery record ID - skipping");
                continue;
            }
            match item.op {
                SubqueryOp::Add => {
                    let version = versions.version_of(&item.id);
                    group.created += 1;
                    group.body.push(format!(
                        "RELATE {from}->{list_ref}->{id} SET \
                         version = {version}, \
                         clientId = {cid}, \
                         auth_id = {aid}, \
                         parent = (SELECT VALUE id FROM {list_ref} WHERE in = {from} AND out = {parent} LIMIT 1)[0], \
                         parent_rel = '{alias}'",
                        from = from, list_ref = list_ref, id = item.id, cid = cid, aid = aid,
                        version = version, parent = item.parent_key, alias = item.alias,
                    ));
                }
                SubqueryOp::Update => {
                    let version = versions.version_of(&item.id);
                    group.updated += 1;
                    group.body.push(format!(
                        "UPDATE (SELECT VALUE id FROM {from}->{list_ref} WHERE out = {id}) SET version = {version} RETURN NONE",
                        list_ref = list_ref, from = from, id = item.id, version = version,
                    ));
                }
                SubqueryOp::Remove => {
                    group.deleted += 1;
                    // Same subquery form as the primary removal above (see
                    // the note there).
                    group.body.push(format!(
                        "DELETE (SELECT VALUE id FROM {from}->{list_ref} WHERE out = {id})",
                        from = from,
                        list_ref = list_ref,
                        id = item.id,
                    ));
                }
            }
        }

        let mut seen_updates = HashSet::new();
        group.body.retain(|statement| !statement.starts_with("UPDATE ") || seen_updates.insert(statement.clone()));
        group.updated = seen_updates.len() as u64;

        // The edges of a full publish are now in this transaction; say so on
        // the row in the LAST one, so a client can never read `ready` with
        // edges still in flight (or the edges with the row still
        // `materializing`). Incremental deltas leave `state` alone.
        if delta.initial {
            group
                .body
                .push(format!("UPDATE {from} SET state = 'ready'", from = from));
        }

        // Only a body with a `RELATE` in it needs the view's metadata.
        if group.body.iter().any(|s| s.starts_with("RELATE ")) {
            group.preamble.push(meta);
            group.preamble.push(auth);
        }

        batch.deltas.push(group);
    }

    batch
}

/// Statements per transaction when a batch is published.
///
/// One `_00_query` row's full membership can be thousands of `RELATE`s; as a
/// single transaction that is minutes of SurrealDB holding write intents over
/// `_00_list_ref_*`, which is what took whitepawn's database down on
/// 2026-09-08. Chunked, the same publish lands progressively — the client
/// applies any non-empty edge set — and every other writer gets the lock back
/// between chunks.
pub const MAX_TX_STATEMENTS: usize = 500;

/// One transaction of a planned publish.
#[derive(Debug, PartialEq)]
pub struct PlannedTx {
    pub sql: String,
    pub bindings: Vec<(String, String)>,
    /// Indices (into the batch's input deltas) this transaction carries. A
    /// delta split across several transactions appears in each of them.
    pub deltas: Vec<usize>,
}

/// Split a batch into transactions of at most `max_statements` statements.
///
/// Whole deltas are packed together while they fit. A delta too big for one
/// transaction is split only when replay is safe: a full publish (which opens
/// with `DELETE $from->list_ref`) or version-only updates. Incremental
/// membership changes stay atomic because replaying half could duplicate edges.
pub fn plan_transactions(batch: &EdgeBatch, max_statements: usize) -> Vec<PlannedTx> {
    let cap = max_statements.max(1);
    let mut planned = Vec::new();
    let mut open = OpenTx::default();

    for group in &batch.deltas {
        let mut rest = group.body.as_slice();
        while !rest.is_empty() {
            let room = cap.saturating_sub(open.statements.len() + group.preamble.len());
            let take = if group.idempotent { rest.len().min(room) } else { rest.len() };
            if take == 0 || take > room {
                // Does not fit: flush what is open and try again on an empty
                // transaction. If nothing is open it cannot fit anywhere, so
                // it rides oversized rather than looping forever.
                if let Some(tx) = open.close() {
                    planned.push(tx);
                    continue;
                }
                open.push(group, rest);
                rest = &[];
                continue;
            }
            open.push(group, &rest[..take]);
            rest = &rest[take..];
        }
    }
    planned.extend(open.close());
    planned
}

/// A transaction under construction in [`plan_transactions`].
#[derive(Default)]
struct OpenTx {
    statements: Vec<String>,
    bindings: Vec<(String, String)>,
    deltas: Vec<usize>,
}

impl OpenTx {
    fn push(&mut self, group: &DeltaStatements, body: &[String]) {
        if self.deltas.last() != Some(&group.delta) {
            self.statements.extend(group.preamble.iter().cloned());
            self.bindings.extend(group.bindings.iter().cloned());
            self.deltas.push(group.delta);
        }
        self.statements.extend(body.iter().cloned());
    }

    fn close(&mut self) -> Option<PlannedTx> {
        let sql = wrap_in_transaction(&std::mem::take(&mut self.statements))?;
        Some(PlannedTx {
            sql,
            bindings: std::mem::take(&mut self.bindings),
            deltas: std::mem::take(&mut self.deltas),
        })
    }
}

/// Wrap edge statements in a single SurrealDB transaction. `None` when there is
/// nothing to write (so the caller skips the round-trip entirely).
pub fn wrap_in_transaction(statements: &[String]) -> Option<String> {
    if statements.is_empty() {
        return None;
    }
    Some(format!(
        "BEGIN TRANSACTION;\n{};\nCOMMIT TRANSACTION;",
        statements.join(";\n")
    ))
}

/// Rounds a batch's leftovers ride at the front of the very next window
/// before they are PARKED and retried on a slower cadence (see
/// [`run_edge_update_service`]). Nothing is ever dropped.
pub const MAX_EDGE_CARRY: u32 = 5;

/// How long a parked set waits between retries. Long enough that a schema
/// apply or a stuck transaction on `_00_list_ref` has moved on, short enough
/// that the clients waiting on those views notice nothing worse than a slow
/// round trip. Expressed in flush windows at runtime (see
/// [`parked_retry_every`]) so the loop stays free of wall-clock reads, which
/// the Durable Object build does not have.
pub const PARKED_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// [`PARKED_RETRY_INTERVAL`] in flush windows of `window`. A zero window
/// (flush on every push) counts flushes instead.
pub fn parked_retry_every(window: Duration) -> u32 {
    if window.is_zero() {
        return 50;
    }
    let windows = PARKED_RETRY_INTERVAL.as_millis() / window.as_millis().max(1);
    (windows as u32).max(1)
}

/// Build + execute the aggregated edge transaction for a batch of deltas
/// through the [`Db`] port, and NEVER silently drop them.
///
/// Returns the deltas that could not be written. The transaction is retried
/// through [`query_retrying`] (SurrealDB's optimistic conflicts are the
/// ordinary case on these rows: the TTL sweep, the view metrics and client
/// heartbeats all write `_00_query` / `_00_list_ref_*`); once the budget is
/// out the batch is split in halves and each half retried on its own, so one
/// poisoned statement cannot take the other 4095 down with it. A single delta
/// that still fails is returned to the caller, logged with its view.
///
/// Before this, a failed transaction was one `error!` and the deltas were
/// gone: the clients subscribed to those views never received the row (a
/// message, a call's `accepted`) until something re-materialized the view,
/// which in practice was a reload.
pub async fn write_deltas_resilient(
    db: &dyn Db,
    deltas: Vec<ViewDelta>,
    circuit: &Circuit,
    mode: RefMode,
    telemetry: &dyn Telemetry,
) -> Vec<ViewDelta> {
    write_deltas_with_versions(db, deltas, &CircuitVersions(circuit), mode, telemetry).await
}

async fn write_deltas_with_versions(
    db: &dyn Db,
    deltas: Vec<ViewDelta>,
    versions: &(impl RecordVersions + Sync),
    mode: RefMode,
    telemetry: &dyn Telemetry,
) -> Vec<ViewDelta> {
    if deltas.is_empty() {
        return Vec::new();
    }
    let refs: Vec<&ViewDelta> = deltas.iter().collect();
    let batch = build_edge_batch(&refs, mode, versions);
    if batch.is_empty() {
        return Vec::new();
    }
    let op_count: usize = batch.deltas.iter().map(|d| d.body.len()).sum();

    debug!(
        created = batch.created(),
        updated = batch.updated(),
        deleted = batch.deleted(),
        views = deltas.len(),
        "Processing edge operations"
    );

    // Run the planned transactions in order. A delta whose transaction failed
    // is not carried into the ones after it: for a split publish those hold
    // the rest of the same membership, and replaying them over a retry that
    // has already re-published it would duplicate every edge.
    let mut failed: HashSet<usize> = HashSet::new();
    let mut error: Option<String> = None;
    for plan in plan_transactions(&batch, MAX_TX_STATEMENTS) {
        if plan.deltas.iter().any(|d| failed.contains(d)) {
            failed.extend(plan.deltas);
            continue;
        }
        let binds: Vec<(&str, Value)> = plan
            .bindings
            .iter()
            .map(|(name, key)| (name.as_str(), json!(key)))
            .collect();
        let started = web_time::Instant::now();
        let outcome = query_retrying(db, &plan.sql, &binds).await;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        telemetry.histogram_ms("edge_transaction", elapsed_ms);
        if let Err(e) = outcome {
            telemetry.counter("edge_publish_failures", 1);
            warn!(target: "ssp::edges", view_id = %deltas[plan.deltas[0]].query_id,
                elapsed_ms, statement_bytes = plan.sql.len(), operations = op_count,
                "Edge transaction failed; publication remains pending");
            error = Some(e.to_string());
            failed.extend(plan.deltas);
        }
    }

    let landed: u64 = batch
        .deltas
        .iter()
        .filter(|d| !failed.contains(&d.delta))
        .map(|d| d.created + d.updated + d.deleted)
        .sum();
    telemetry.counter("edge_operations", landed);

    let Some(e) = error else {
        debug!(target: "ssp::edges", operations = op_count, "Edge update transaction completed");
        return Vec::new();
    };

    // Only the deltas whose transaction failed are retried; the rest are
    // committed.
    let mut slots: Vec<Option<ViewDelta>> = deltas.into_iter().map(Some).collect();
    let deltas: Vec<ViewDelta> = {
        let mut idx: Vec<usize> = failed.into_iter().collect();
        idx.sort_unstable();
        idx.into_iter().filter_map(|i| slots[i].take()).collect()
    };

    if deltas.len() > 1 {
        // Split on ANY error, not just a conflict: a statement that is wrong
        // (not merely contended) must be isolated, not retried as part of
        // everything else forever.
        telemetry.counter("edge_batch_split", 1);
        warn!(target: "ssp::edges", error = %e, views = deltas.len(), operations = op_count, "Edge update transaction failed after retries; splitting the batch");
        let mut left = deltas;
        let right = left.split_off(left.len() / 2);
        let mut leftovers =
            Box::pin(write_deltas_with_versions(db, left, versions, mode, telemetry)).await;
        leftovers
            .extend(Box::pin(write_deltas_with_versions(db, right, versions, mode, telemetry)).await);
        return leftovers;
    }

    // A view the client has since released (TTL sweep, unsubscribe, a
    // boot-time re-registration of a row the sweep then removed) has no
    // `_00_query` record to relate from, so its deltas can never land and
    // nobody is waiting for them. Those are dropped here, deliberately,
    // instead of being carried forever.
    if view_is_gone(db, &deltas[0].query_id).await {
        telemetry.counter("edge_deltas_orphaned", 1);
        info!(target: "ssp::edges", view_id = %deltas[0].query_id, operations = op_count, "Edge delta dropped: its view is no longer registered");
        return Vec::new();
    }
    error!(target: "ssp::edges", error = %e, view_id = %deltas[0].query_id, operations = op_count, "Edge delta not written after retries");
    deltas
}

/// Whether the `_00_query` record behind a view id is gone. Only a definite
/// "not there" answers true: a query error is no evidence either way, and the
/// caller then keeps carrying the delta.
async fn view_is_gone(db: &dyn Db, query_id: &str) -> bool {
    // The same key derivation the publish itself binds (`incantation_key`).
    // A prefix strip is not it: a view id spelled any other way resolved to a
    // record that cannot exist, so a delta whose transaction merely failed was
    // declared orphaned and dropped.
    let key = incantation_key(query_id);
    match db
        .query(
            "SELECT VALUE id FROM ONLY type::record('_00_query', $key)",
            &[("key", json!(key))],
        )
        .await
    {
        Ok(rows) => rows.first().map(|v| v.is_null()).unwrap_or(true),
        Err(e) => {
            debug!(target: "ssp::edges", error = %e, view_id = %query_id, "Could not check whether the view still exists; carrying its delta");
            false
        }
    }
}

/// Fire-and-forget form of [`write_deltas_resilient`] over borrowed deltas;
/// leftovers are counted and logged. Kept for the direct-write callers and
/// the existing tests.
pub async fn run_edge_writes(
    db: &dyn Db,
    deltas: &[&ViewDelta],
    circuit: &Circuit,
    mode: RefMode,
    telemetry: &dyn Telemetry,
) {
    let owned: Vec<ViewDelta> = deltas.iter().map(|d| (*d).clone()).collect();
    let left = write_deltas_resilient(db, owned, circuit, mode, telemetry).await;
    if !left.is_empty() {
        telemetry.counter("edge_deltas_dropped", left.len() as u64);
        error!(target: "ssp::edges", views = left.len(), "edge deltas dropped after retries");
    }
}

/// Pure buffer/size-cap state machine for the throttler.
#[derive(Default)]
pub struct Batcher {
    buf: Vec<ViewDelta>,
    max_batch: usize,
}

impl Batcher {
    pub fn new(max_batch: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_batch,
        }
    }

    /// Buffer deltas. Returns `Some(batch)` to flush NOW if the buffer reached
    /// `max_batch` (`0` = no size cap).
    pub fn push(&mut self, deltas: Vec<ViewDelta>) -> Option<Vec<ViewDelta>> {
        self.buf.extend(deltas);
        if self.max_batch != 0 && self.buf.len() >= self.max_batch {
            Some(std::mem::take(&mut self.buf))
        } else {
            None
        }
    }

    /// Re-queue deltas a flush could not write, AHEAD of anything buffered
    /// since: a carried RELATE must land before a newer DELETE for the same
    /// view, or the edge would resurrect.
    pub fn push_front(&mut self, deltas: Vec<ViewDelta>) {
        if deltas.is_empty() {
            return;
        }
        let mut merged = deltas;
        merged.append(&mut self.buf);
        self.buf = merged;
    }

    /// Drain for a window-tick / shutdown flush. `None` when empty.
    pub fn take(&mut self) -> Option<Vec<ViewDelta>> {
        if self.buf.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.buf))
        }
    }

    pub fn pending(&self) -> usize {
        self.buf.len()
    }
}

/// The flush boundary. The real impl ([`SurrealEdgeSink`]) writes to SurrealDB;
/// tests use a recording mock. `Send` bounds are cfg-gated: native tokio needs
/// `Send` futures, workers-rs DO futures are `!Send`.
/// `flush` returns the deltas it could NOT write; the service carries them
/// into the next window (see [`MAX_EDGE_CARRY`]).
#[cfg(not(target_arch = "wasm32"))]
pub trait EdgeSink: Send + Sync {
    fn flush(
        &self,
        deltas: Vec<ViewDelta>,
    ) -> impl std::future::Future<Output = Vec<ViewDelta>> + Send;
}
#[cfg(target_arch = "wasm32")]
pub trait EdgeSink {
    fn flush(&self, deltas: Vec<ViewDelta>) -> impl std::future::Future<Output = Vec<ViewDelta>>;
}

/// Drain `rx`, coalescing pushed delta batches and flushing them through
/// `sink` every `window` (or immediately when `window` is zero, or early once
/// `max_batch` is hit). The window is timed through the [`Scheduler`] port —
/// the loop is otherwise channel receives (`tokio::sync`), so it is portable.
pub async fn run_edge_update_service<S>(
    rx: mpsc::UnboundedReceiver<Vec<ViewDelta>>,
    sink: S,
    scheduler: Arc<dyn Scheduler>,
    window: Duration,
    max_batch: usize,
) where
    S: EdgeSink + 'static,
{
    let every = parked_retry_every(window);
    run_edge_update_service_with(rx, sink, scheduler, window, max_batch, every).await
}

/// [`run_edge_update_service`] with an explicit parked-retry cadence (in
/// flush windows). Public for tests, which want a short cadence at a short
/// window without waiting out [`PARKED_RETRY_INTERVAL`].
pub async fn run_edge_update_service_with<S>(
    mut rx: mpsc::UnboundedReceiver<Vec<ViewDelta>>,
    sink: S,
    scheduler: Arc<dyn Scheduler>,
    window: Duration,
    max_batch: usize,
    parked_retry_every: u32,
) where
    S: EdgeSink + 'static,
{
    let mut carry = CarryState::new(parked_retry_every);

    if window.is_zero() {
        let mut batcher = Batcher::new(0);
        while let Some(deltas) = rx.recv().await {
            carry.route(&mut batcher, deltas);
            carry.before_flush(&mut batcher);
            if let Some(ready) = batcher.take() {
                let left = sink.flush(ready).await;
                carry.absorb(&mut batcher, left);
            }
        }
        carry.drain_parked(&mut batcher);
        if let Some(remainder) = batcher.take() {
            sink.flush(remainder).await;
        }
        return;
    }

    let mut batcher = Batcher::new(max_batch);

    'windows: loop {
        // Pinned OUTSIDE the receive loop so a steady stream can't keep
        // restarting the window and starve the flush.
        let sleep_fut = scheduler.sleep(window);
        tokio::pin!(sleep_fut);

        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(deltas) => {
                        if let Some(ready) = carry.route(&mut batcher, deltas) {
                            let left = sink.flush(ready).await;
                            carry.absorb(&mut batcher, left);
                        }
                    }
                    None => {
                        // Shutdown: one last attempt at everything, parked
                        // included. Whatever still fails here is lost with
                        // the process, and the clients' TTL re-register is
                        // what heals that.
                        carry.drain_parked(&mut batcher);
                        if let Some(remainder) = batcher.take() {
                            sink.flush(remainder).await;
                        }
                        break 'windows;
                    }
                },
                _ = &mut sleep_fut => {
                    carry.before_flush(&mut batcher);
                    if let Some(batch) = batcher.take() {
                        let left = sink.flush(batch).await;
                        carry.absorb(&mut batcher, left);
                    }
                    continue 'windows;
                }
            }
        }
    }

    debug!("edge-update service stopped");
}

/// What the flush loop does with deltas a flush could not write.
///
/// Leftovers ride at the front of the next batch for [`MAX_EDGE_CARRY`]
/// consecutive rounds. A delta still failing after that is PARKED, not
/// dropped: it waits [`PARKED_RETRY_INTERVAL`] and is then pushed back to the
/// front of the batch, and so on until it lands. Before this, the sixth
/// failure dropped the delta for good, and every client subscribed to that
/// view waited on a membership edge that no longer existed anywhere: the
/// whitepawn 1.0.26 deploy's schema apply held `_00_list_ref` in a failed
/// transaction for a few windows, five views lost their edges, and their
/// clients sat on a loading screen until the 10-minute TTL re-register.
///
/// The one legitimate drop is a view that no longer exists; the sink decides
/// that (see `view_is_gone`), so it never reaches here as a leftover.
///
/// Ordering while parked: a view's edges are a stream (a RELATE must land
/// before the DELETE that follows it), so once a view has parked deltas, every
/// NEWER delta for that view is parked behind them too, and the whole run is
/// retried together, in order. Views that are not parked flow as normal, so
/// one stuck view never delays the others.
struct CarryState {
    /// Flush windows between retries of the parked set.
    retry_every: u32,
    /// Consecutive rounds the current leftovers have failed.
    carry_rounds: u32,
    /// Parked deltas, oldest first.
    parked: Vec<ViewDelta>,
    /// The views that own a parked delta (incoming deltas for these are
    /// appended to `parked`, not batched).
    parked_views: HashSet<String>,
    /// Windows elapsed since the parked set was last retried.
    windows_since_retry: u32,
    /// How many times the parked set has been retried (log/telemetry only).
    retries: u32,
}

impl CarryState {
    fn new(retry_every: u32) -> Self {
        Self {
            retry_every: retry_every.max(1),
            carry_rounds: 0,
            parked: Vec::new(),
            parked_views: HashSet::new(),
            windows_since_retry: 0,
            retries: 0,
        }
    }

    /// Route incoming deltas: views with parked deltas queue behind them,
    /// the rest go to the batcher. Returns a ready batch when the batcher's
    /// size cap trips.
    fn route(&mut self, batcher: &mut Batcher, deltas: Vec<ViewDelta>) -> Option<Vec<ViewDelta>> {
        if self.parked_views.is_empty() {
            return batcher.push(deltas);
        }
        let mut fresh = Vec::with_capacity(deltas.len());
        for d in deltas {
            if self.parked_views.contains(&d.query_id) {
                self.parked.push(d);
            } else {
                fresh.push(d);
            }
        }
        if fresh.is_empty() {
            None
        } else {
            batcher.push(fresh)
        }
    }

    /// Called once per window before the flush: when the parked set has waited
    /// long enough, it goes back to the FRONT of this window's batch.
    fn before_flush(&mut self, batcher: &mut Batcher) {
        if self.parked.is_empty() {
            return;
        }
        self.windows_since_retry += 1;
        if self.windows_since_retry < self.retry_every {
            return;
        }
        self.retries += 1;
        // Loud once a minute at the default cadence, quiet in between: a set
        // stuck for a long time should be visible, not fill the log.
        if self.retries % 12 == 1 {
            warn!(target: "ssp::edges", views = self.parked_views.len(), deltas = self.parked.len(), retry = self.retries, "retrying parked edge deltas");
        } else {
            debug!(target: "ssp::edges", views = self.parked_views.len(), deltas = self.parked.len(), retry = self.retries, "retrying parked edge deltas");
        }
        self.drain_parked(batcher);
    }

    /// Move every parked delta back to the front of the batch, unconditionally.
    fn drain_parked(&mut self, batcher: &mut Batcher) {
        if self.parked.is_empty() {
            return;
        }
        self.windows_since_retry = 0;
        self.parked_views.clear();
        let parked = std::mem::take(&mut self.parked);
        batcher.push_front(parked);
    }

    /// Take a flush's leftovers: carry them into the next round, or park them
    /// once they have failed [`MAX_EDGE_CARRY`] rounds in a row.
    fn absorb(&mut self, batcher: &mut Batcher, left: Vec<ViewDelta>) {
        if left.is_empty() {
            if self.carry_rounds > 0 && self.retries > 0 {
                debug!(target: "ssp::edges", retries = self.retries, "previously parked edge deltas landed");
            }
            self.carry_rounds = 0;
            return;
        }
        self.carry_rounds += 1;
        if self.carry_rounds <= MAX_EDGE_CARRY {
            debug!(target: "ssp::edges", views = left.len(), rounds = self.carry_rounds, "carrying unwritten edge deltas into the next window");
            batcher.push_front(left);
            return;
        }
        // Park: the next flushes proceed without these, and they come back at
        // the front of a batch every PARKED_RETRY_WINDOWS windows.
        self.carry_rounds = 0;
        self.windows_since_retry = 0;
        for d in &left {
            self.parked_views.insert(d.query_id.clone());
        }
        if self.retries == 0 {
            error!(
                target: "ssp::edges",
                views = self.parked_views.len(),
                deltas = left.len() + self.parked.len(),
                every_windows = self.retry_every,
                "edge deltas parked after carry rounds; they will be retried until they land"
            );
        } else {
            debug!(
                target: "ssp::edges",
                views = self.parked_views.len(),
                deltas = left.len() + self.parked.len(),
                retries = self.retries,
                "edge deltas parked again after a failed retry"
            );
        }
        // Oldest first: anything already parked stays ahead of this round's.
        self.parked.extend(left);
    }
}

/// Real [`EdgeSink`]: builds + writes the aggregated transaction via the
/// [`Db`] port, reading versions from the circuit.
pub struct SurrealEdgeSink {
    pub db: Arc<dyn Db>,
    pub processor: Arc<RwLock<Circuit>>,
    pub publication_gate: Arc<tokio::sync::Mutex<()>>,
    pub telemetry: Arc<dyn Telemetry>,
    pub mode: RefMode,
}

impl EdgeSink for SurrealEdgeSink {
    async fn flush(&self, deltas: Vec<ViewDelta>) -> Vec<ViewDelta> {
        write_deltas_unlocked(
            self.db.as_ref(),
            deltas,
            &self.processor,
            &self.publication_gate,
            self.mode,
            self.telemetry.as_ref(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ssp::circuit::{SubqueryDeltaItem, SubqueryOp, ViewDelta};

    struct ConstV(i64);
    impl RecordVersions for ConstV {
        fn version_of(&self, _key: &str) -> i64 {
            self.0
        }
    }

    fn delta(query_id: &str, auth_id: &str) -> ViewDelta {
        ViewDelta {
            query_id: query_id.to_string(),
            additions: vec![],
            removals: vec![],
            updates: vec![],
            row_count: 0,
            result_hash: String::new(),
            subquery_items: vec![],
            auth_id: auth_id.to_string(),
            initial: false,
        }
    }

    struct SlowDb { entered: tokio::sync::Notify, release: tokio::sync::Notify }
    #[async_trait::async_trait]
    impl Db for SlowDb {
        async fn query(&self, _: &str, _: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(vec![Value::Null])
        }
        async fn version(&self) -> Result<String, DbError> { Ok("test".into()) }
    }

    #[tokio::test]
    async fn stalled_publication_does_not_hold_the_ingest_circuit_lock() {
        let db = Arc::new(SlowDb { entered: tokio::sync::Notify::new(), release: tokio::sync::Notify::new() });
        let processor = Arc::new(RwLock::new(Circuit::new()));
        processor.write().await.add_query(ssp::operator::plan::QueryPlan { id: "abc".into(), root: ssp::operator::plan::OperatorPlan::Scan { table: "game".into() } }, None, None);
        let sink = SurrealEdgeSink { db: db.clone(), processor: processor.clone(), publication_gate: Arc::new(tokio::sync::Mutex::new(())), telemetry: Arc::new(crate::ports::NoopTelemetry), mode: RefMode::Single };
        let mut d = delta("abc", "user:a"); d.updates.push("game:1".into());
        let writer = tokio::spawn(async move { sink.flush(vec![d]).await });
        db.entered.notified().await;
        // The network call is still blocked. Before the fix this timed out.
        let mut circuit = tokio::time::timeout(Duration::from_millis(250), processor.write()).await.expect("publication blocked ingest");
        circuit.step(ssp::circuit::ChangeSet { changes: vec![ssp::circuit::Change::create("game", "game:1", json!({"id": "game:1"}))] });
        drop(circuit);
        db.release.notify_one();
        assert!(writer.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cleanup_waits_for_publication_and_queued_detached_work_is_discarded() {
        let db = Arc::new(SlowDb { entered: tokio::sync::Notify::new(), release: tokio::sync::Notify::new() });
        let processor = Arc::new(RwLock::new(Circuit::new()));
        processor.write().await.add_query(ssp::operator::plan::QueryPlan { id: "abc".into(), root: ssp::operator::plan::OperatorPlan::Scan { table: "game".into() } }, None, None);
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let sink = Arc::new(SurrealEdgeSink { db: db.clone(), processor: processor.clone(), publication_gate: gate.clone(), telemetry: Arc::new(crate::ports::NoopTelemetry), mode: RefMode::Single });
        let mut d = delta("abc", "user:a"); d.initial = true; d.additions.push("game:1".into());
        let writer = { let sink = sink.clone(); let d = d.clone(); tokio::spawn(async move { sink.flush(vec![d]).await }) };
        db.entered.notified().await;
        assert!(tokio::time::timeout(Duration::from_millis(25), gate.lock()).await.is_err(), "cleanup must wait for in-flight publication");
        db.release.notify_one();
        assert!(writer.await.unwrap().is_empty());
        { let _cleanup = gate.lock().await; processor.write().await.detach_subscriber("abc"); }
        // SlowDb would block if queued work recreated the detached view's edges.
        let left = tokio::time::timeout(Duration::from_millis(250), sink.flush(vec![d])).await.expect("detached publication reached database");
        assert!(left.is_empty());
    }

    #[test]
    fn version_only_deltas_split_safely_and_deduplicate_updates() {
        let mut d = delta("abc", "user:a");
        d.updates = (0..600).map(|n| format!("game:{n}")).collect();
        d.updates.push("game:1".into());
        let batch = build_edge_batch(&[&d], RefMode::Single, &ConstV(2));
        assert_eq!(batch.deltas[0].body.len(), 600);
        let txs = plan_transactions(&batch, 100);
        assert!(txs.len() > 1);
        assert!(txs.iter().all(|t| t.sql.matches(";\n").count() <= 101));
        assert_eq!(txs.iter().map(|t| t.sql.matches("UPDATE ").count()).sum::<usize>(), 600);
    }

    #[test]
    fn empty_delta_produces_nothing() {
        let d = delta("q:1", "user:a");
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(1));
        assert!(b.is_empty());
        assert!(b.bindings().is_empty());
    }

    #[test]
    fn initial_publish_replaces_existing_edges_and_marks_the_row_ready() {
        // A full publish (registration snapshot, repair, subscriber attach)
        // must be idempotent over whatever edges the row already has, and the
        // `ready` flip must ride in the same transaction as the edges.
        let mut d = delta("view:abc", "user:a");
        d.initial = true;
        d.additions = vec!["thread:1".to_string(), "thread:2".to_string()];
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(3));

        assert_eq!(b.created(), 2);
        assert_eq!(
            b.deltas[0].preamble[0],
            "LET $from0 = type::record('_00_query', $from0key)"
        );
        let body = &b.deltas[0].body;
        assert_eq!(body[0], "DELETE $from0->_00_list_ref", "delete-all precedes the RELATEs");
        assert!(body[1].starts_with("RELATE $from0->_00_list_ref->thread:1"), "{}", body[1]);
        assert!(body[2].starts_with("RELATE $from0->_00_list_ref->thread:2"), "{}", body[2]);
        assert_eq!(
            body.last().unwrap(),
            "UPDATE $from0 SET state = 'ready'",
            "the row flips to ready after its edges, inside the same batch"
        );
        assert_eq!(body.len(), 4);
    }

    #[test]
    fn initial_publish_uses_the_per_user_table_in_dedicated_mode() {
        let mut d = delta("view:abc", "user:a");
        d.initial = true;
        d.additions = vec!["thread:1".to_string()];
        let b = build_edge_batch(&[&d], RefMode::Dedicated, &ConstV(1));
        assert_eq!(b.deltas[0].body[0], "DELETE $from0->_00_list_ref_user_a");
        assert_eq!(b.statements().last().unwrap(), "UPDATE $from0 SET state = 'ready'");
    }

    #[test]
    fn incremental_delta_neither_wipes_edges_nor_touches_state() {
        let mut d = delta("view:abc", "user:a");
        d.additions = vec!["thread:3".to_string()];
        d.removals = vec!["thread:1".to_string()];
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(1));
        assert!(
            b.statements().iter().all(|s| !s.starts_with("DELETE $from0->_00_list_ref")),
            "an increment must not delete the row's other edges: {:?}",
            b.statements()
        );
        assert!(
            b.statements().iter().all(|s| !s.contains("state")),
            "an increment must not rewrite state: {:?}",
            b.statements()
        );
    }

    #[test]
    fn publish_state_reflects_whether_the_flusher_will_see_the_delta() {
        // Nothing to publish: the row must be born `ready`, because the
        // flusher skips such a delta and would never flip it.
        assert_eq!(publish_state_for(None), "ready");
        let mut empty = delta("view:abc", "user:a");
        empty.initial = true;
        assert_eq!(publish_state_for(Some(&empty)), "ready");
        // Something to publish: `materializing` until the batch commits.
        let mut full = delta("view:abc", "user:a");
        full.initial = true;
        full.additions = vec!["thread:1".to_string()];
        assert_eq!(publish_state_for(Some(&full)), "materializing");
        // Subquery-only deltas reach the flusher too.
        let mut sub = delta("view:abc", "user:a");
        sub.subquery_items = vec![SubqueryDeltaItem {
            id: "comment:1".to_string(),
            parent_key: "thread:1".to_string(),
            alias: "comments".to_string(),
            op: SubqueryOp::Add,
        }];
        assert_eq!(publish_state_for(Some(&sub)), "materializing");
    }

    #[test]
    fn addition_binds_incantation_key_and_wraps_type_thing() {
        let mut d = delta("view:abc", "user:a");
        d.additions = vec!["user:x".to_string()];
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(7));

        assert_eq!(b.created(), 1);
        assert_eq!(
            b.bindings(),
            vec![("from0key".to_string(), "abc".to_string())]
        );
        // The preamble binds the incantation record and resolves the view's
        // metadata once; the RELATEs then reference those params.
        assert_eq!(
            b.deltas[0].preamble,
            vec![
                "LET $from0 = type::record('_00_query', $from0key)".to_string(),
                "LET $cid0 = (SELECT VALUE clientId FROM $from0 LIMIT 1)[0]".to_string(),
                "LET $aid0 = (SELECT VALUE auth_id FROM $from0 LIMIT 1)[0]".to_string(),
            ]
        );
        let stmt = &b.deltas[0].body[0];
        assert!(
            stmt.contains("RELATE $from0->_00_list_ref->user:x"),
            "{stmt}"
        );
        assert!(stmt.contains("version = 7"), "{stmt}");
        assert!(
            stmt.contains("clientId = $cid0, auth_id = $aid0"),
            "one resolved param per field, not a subselect per row: {stmt}"
        );
    }

    #[test]
    fn multiple_deltas_get_unique_bindings() {
        let mut d0 = delta("view:a", "user:1");
        d0.additions = vec!["t:1".to_string()];
        let mut d1 = delta("view:b", "user:2");
        d1.removals = vec!["t:2".to_string()];
        let b = build_edge_batch(&[&d0, &d1], RefMode::Single, &ConstV(1));
        assert_eq!(b.bindings().len(), 2);
        assert_eq!(b.bindings()[0], ("from0key".to_string(), "a".to_string()));
        assert_eq!(b.bindings()[1], ("from1key".to_string(), "b".to_string()));
        assert_eq!(b.created(), 1);
        assert_eq!(b.deleted(), 1);
    }

    #[test]
    fn removal_deletes_by_resolved_edge_id_not_by_filtered_graph_path() {
        // `DELETE $from->edge WHERE out = x` is rejected by SurrealDB 3.0.x
        // ("Cannot execute DELETE statement using value: NONE"); the removal
        // must resolve the edge id through the graph first.
        let mut d = delta("view:w", "user:a");
        d.removals = vec!["message:old".to_string()];
        d.subquery_items = vec![SubqueryDeltaItem {
            id: "child:gone".to_string(),
            parent_key: "parent:1".to_string(),
            alias: "kids".to_string(),
            op: SubqueryOp::Remove,
        }];
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(1));
        assert_eq!(b.deleted(), 2);
        let deletes: Vec<String> = b
            .statements()
            .into_iter()
            .filter(|s| s.starts_with("DELETE"))
            .collect();
        assert_eq!(deletes.len(), 2);
        for stmt in deletes {
            assert!(
                stmt.starts_with("DELETE (SELECT VALUE id FROM $from0->_00_list_ref WHERE out = "),
                "{stmt}"
            );
            assert!(
                !stmt.contains("DELETE $from0->"),
                "filtered graph-path delete must not be emitted: {stmt}"
            );
        }
    }

    #[test]
    fn subquery_add_references_parent_and_binds() {
        let mut d = delta("view:z", "user:9");
        d.additions = vec!["parent:1".to_string()];
        d.subquery_items = vec![SubqueryDeltaItem {
            id: "child:1".to_string(),
            parent_key: "parent:1".to_string(),
            alias: "kids".to_string(),
            op: SubqueryOp::Add,
        }];
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(1));
        // one primary + one subquery add
        assert_eq!(b.created(), 2);
        let stmts = b.statements();
        let sub = stmts.iter().find(|s| s.contains("child:1")).unwrap();
        assert!(sub.contains("parent_rel = 'kids'"), "{sub}");
        assert!(sub.contains("$from0"), "{sub}");
    }

    #[test]
    fn invalid_record_ids_skipped() {
        let mut d = delta("view:q", "user:a");
        d.additions = vec!["nocolon".to_string(), "ok:1".to_string()];
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(1));
        assert_eq!(b.created(), 1, "only the valid id produced an edge");
    }

    #[test]
    fn wrap_transaction_roundtrip() {
        assert_eq!(wrap_in_transaction(&[]), None);
        let q = wrap_in_transaction(&["A".into(), "B".into()]).unwrap();
        assert!(q.starts_with("BEGIN TRANSACTION;"));
        assert!(q.contains("A;\nB"));
        assert!(q.trim_end().ends_with("COMMIT TRANSACTION;"));
    }

    // ---- transaction planning ------------------------------------------

    #[test]
    fn a_batch_that_fits_stays_one_transaction() {
        let mut d = delta("view:abc", "user:a");
        d.initial = true;
        d.additions = vec!["thread:1".to_string(), "thread:2".to_string()];
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(1));
        let planned = plan_transactions(&b, MAX_TX_STATEMENTS);
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].deltas, vec![0]);
        assert_eq!(
            planned[0].sql,
            wrap_in_transaction(&b.statements()).unwrap(),
            "a batch under the cap is byte-for-byte the single-transaction form"
        );
    }

    #[test]
    fn a_big_publish_is_split_and_every_chunk_repeats_the_preamble() {
        let mut d = delta("view:abc", "user:a");
        d.initial = true;
        d.additions = (0..20).map(|i| format!("thread:{i}")).collect();
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(1));
        // 3 preamble + 20 RELATEs + DELETE + state flip.
        let planned = plan_transactions(&b, 8);
        assert!(planned.len() > 1, "a 22-statement body did not split at 8");
        for tx in &planned {
            assert!(
                tx.sql.contains("LET $from0 = type::record('_00_query', $from0key)"),
                "a LET is transaction-scoped, so every chunk repeats it: {}",
                tx.sql
            );
            assert_eq!(tx.bindings, vec![("from0key".to_string(), "abc".to_string())]);
            assert_eq!(tx.deltas, vec![0]);
        }
        assert!(
            planned[0].sql.contains("DELETE $from0->_00_list_ref"),
            "the wipe opens the publish"
        );
        assert!(
            planned.last().unwrap().sql.contains("state = 'ready'"),
            "the row flips ready only in the last chunk"
        );
        let relates = planned
            .iter()
            .map(|tx| tx.sql.matches("RELATE ").count())
            .sum::<usize>();
        assert_eq!(relates, 20, "every edge is written exactly once");
    }

    #[test]
    fn an_incremental_delta_is_never_split() {
        // No leading DELETE means replaying half of it would duplicate edges,
        // so it rides one transaction however far over the cap it is.
        let mut d = delta("view:abc", "user:a");
        d.additions = (0..20).map(|i| format!("thread:{i}")).collect();
        let b = build_edge_batch(&[&d], RefMode::Single, &ConstV(1));
        let planned = plan_transactions(&b, 5);
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].sql.matches("RELATE ").count(), 20);
    }

    #[test]
    fn deltas_are_packed_together_and_never_straddle_a_chunk() {
        let mut d0 = delta("view:a", "user:1");
        d0.additions = vec!["t:1".to_string()];
        let mut d1 = delta("view:b", "user:1");
        d1.additions = vec!["t:2".to_string()];
        let mut d2 = delta("view:c", "user:1");
        d2.additions = vec!["t:3".to_string()];
        let b = build_edge_batch(&[&d0, &d1, &d2], RefMode::Single, &ConstV(1));
        // Each delta is 3 preamble + 1 RELATE, so two fit in 8 statements.
        let planned = plan_transactions(&b, 8);
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].deltas, vec![0, 1]);
        assert_eq!(planned[1].deltas, vec![2]);
        assert_eq!(planned[0].bindings.len(), 2);
    }

    // ---- carry / park policy -------------------------------------------

    use crate::ports::{DbError, TimerKind};
    use std::sync::Mutex;

    /// Parked-retry cadence for the loop tests: short, so a 1ms window retries
    /// within milliseconds instead of waiting out PARKED_RETRY_INTERVAL.
    const TEST_RETRY_WINDOWS: u32 = 20;

    #[test]
    fn parked_retry_cadence_follows_the_window() {
        assert_eq!(parked_retry_every(Duration::from_millis(100)), 50);
        assert_eq!(parked_retry_every(Duration::from_millis(1000)), 5);
        // Never zero, even for a window longer than the interval.
        assert_eq!(parked_retry_every(Duration::from_secs(30)), 1);
        assert_eq!(parked_retry_every(Duration::ZERO), 50);
    }

    /// A `Db` that answers the view-existence probe with a fixed value.
    struct ViewProbeDb {
        exists: bool,
        fail: bool,
        calls: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl Db for ViewProbeDb {
        async fn query(
            &self,
            surql: &str,
            _binds: &[(&str, Value)],
        ) -> Result<Vec<Value>, DbError> {
            self.calls.lock().unwrap().push(surql.to_string());
            if self.fail {
                return Err(DbError::Transport("down".into()));
            }
            Ok(vec![if self.exists {
                json!("_00_query:abc")
            } else {
                Value::Null
            }])
        }
        async fn version(&self) -> Result<String, DbError> {
            Ok("test".into())
        }
    }

    #[tokio::test]
    async fn view_is_gone_only_on_a_definite_miss() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let gone = ViewProbeDb {
            exists: false,
            fail: false,
            calls: Arc::clone(&calls),
        };
        assert!(view_is_gone(&gone, "_00_query:abc").await);
        assert!(
            view_is_gone(&gone, "abc").await,
            "a bare key is accepted too"
        );
        let present = ViewProbeDb {
            exists: true,
            fail: false,
            calls: Arc::clone(&calls),
        };
        assert!(!view_is_gone(&present, "abc").await);
        let down = ViewProbeDb {
            exists: false,
            fail: true,
            calls: Arc::clone(&calls),
        };
        assert!(
            !view_is_gone(&down, "abc").await,
            "a failed probe is not evidence the view is gone"
        );
        assert!(calls
            .lock()
            .unwrap()
            .iter()
            .all(|q| q.contains("type::record('_00_query', $key)")));
    }

    struct NoopScheduler;
    #[async_trait::async_trait]
    impl Scheduler for NoopScheduler {
        async fn schedule(&self, _kind: TimerKind, _at_epoch_ms: u64) {}
        async fn cancel(&self, _kind: &TimerKind) {}
        async fn sleep(&self, dur: Duration) {
            tokio::time::sleep(dur).await
        }
    }

    /// A sink that fails its first `fail_flushes` flushes wholesale and then
    /// writes everything, recording `(query_id, first addition)` per delta in
    /// the order written.
    struct FlakySink {
        fail_flushes: u32,
        calls: Arc<Mutex<u32>>,
        written: Arc<Mutex<Vec<(String, String)>>>,
    }
    impl EdgeSink for FlakySink {
        async fn flush(&self, deltas: Vec<ViewDelta>) -> Vec<ViewDelta> {
            let call = {
                let mut c = self.calls.lock().unwrap();
                *c += 1;
                *c
            };
            if call <= self.fail_flushes {
                return deltas;
            }
            let mut w = self.written.lock().unwrap();
            for d in &deltas {
                w.push((
                    d.query_id.clone(),
                    d.additions.first().cloned().unwrap_or_default(),
                ));
            }
            Vec::new()
        }
    }

    fn add_delta(query_id: &str, record: &str) -> ViewDelta {
        let mut d = delta(query_id, "user:a");
        d.additions = vec![record.to_string()];
        d
    }

    #[tokio::test]
    async fn a_delta_that_outlives_the_carry_budget_is_parked_and_still_lands() {
        let calls = Arc::new(Mutex::new(0));
        let written = Arc::new(Mutex::new(Vec::new()));
        let sink = FlakySink {
            // Carry budget is MAX_EDGE_CARRY rounds after the first failure;
            // fail well past it so the delta is parked before the sink heals.
            fail_flushes: MAX_EDGE_CARRY + 3,
            calls: Arc::clone(&calls),
            written: Arc::clone(&written),
        };
        let (tx, rx) = mpsc::unbounded_channel::<Vec<ViewDelta>>();
        let svc = tokio::spawn(run_edge_update_service_with(
            rx,
            sink,
            Arc::new(NoopScheduler),
            Duration::from_millis(1),
            0,
            TEST_RETRY_WINDOWS,
        ));

        tx.send(vec![add_delta("view:stuck", "row:1")]).unwrap();
        // A parked set retries after PARKED_RETRY_WINDOWS windows of 1ms; give
        // it a few of those, then keep the loop alive with unrelated traffic
        // so the windows actually tick.
        for i in 0..(TEST_RETRY_WINDOWS * 3) {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if i % 7 == 0 {
                tx.send(vec![add_delta("view:other", &format!("row:{i}"))])
                    .unwrap();
            }
        }
        drop(tx);
        svc.await.unwrap();

        let w = written.lock().unwrap();
        let stuck: Vec<_> = w.iter().filter(|(q, _)| q == "view:stuck").collect();
        assert_eq!(
            stuck.len(),
            1,
            "the parked delta must be written exactly once: {w:?}"
        );
        assert!(
            *calls.lock().unwrap() > MAX_EDGE_CARRY + 3,
            "the sink must have been retried past its failing flushes"
        );
    }

    #[tokio::test]
    async fn newer_deltas_for_a_parked_view_land_after_the_parked_ones_in_order() {
        let calls = Arc::new(Mutex::new(0));
        let written = Arc::new(Mutex::new(Vec::new()));
        let sink = FlakySink {
            fail_flushes: MAX_EDGE_CARRY + 1,
            calls: Arc::clone(&calls),
            written: Arc::clone(&written),
        };
        let (tx, rx) = mpsc::unbounded_channel::<Vec<ViewDelta>>();
        let svc = tokio::spawn(run_edge_update_service_with(
            rx,
            sink,
            Arc::new(NoopScheduler),
            Duration::from_millis(1),
            0,
            TEST_RETRY_WINDOWS,
        ));

        tx.send(vec![add_delta("view:v", "row:first")]).unwrap();
        // Let it fail through the carry budget and get parked.
        tokio::time::sleep(Duration::from_millis((MAX_EDGE_CARRY as u64 + 4) * 3)).await;
        // While parked, a newer delta for the same view and one for another
        // view arrive. The other view must not wait; the same view must queue
        // behind the parked delta.
        tx.send(vec![
            add_delta("view:v", "row:second"),
            add_delta("view:free", "row:x"),
        ])
        .unwrap();
        tokio::time::sleep(Duration::from_millis((TEST_RETRY_WINDOWS as u64 + 5) * 3)).await;
        drop(tx);
        svc.await.unwrap();

        let w = written.lock().unwrap();
        let v: Vec<&str> = w
            .iter()
            .filter(|(q, _)| q == "view:v")
            .map(|(_, r)| r.as_str())
            .collect();
        assert_eq!(
            v,
            vec!["row:first", "row:second"],
            "parked view must land in order: {w:?}"
        );
        let free_pos = w
            .iter()
            .position(|(q, _)| q == "view:free")
            .expect("free view written");
        let first_pos = w
            .iter()
            .position(|(q, r)| q == "view:v" && r == "row:first")
            .unwrap();
        assert!(
            free_pos < first_pos,
            "an unparked view must not wait for the parked one: {w:?}"
        );
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    use std::sync::{Mutex, atomic::{AtomicBool, Ordering}};
    use crate::ports::{DbError, NoopTelemetry, TimerKind};

    #[derive(Default)]
    struct TestDb {
        fail: AtomicBool,
        source_started: tokio::sync::Notify,
        source_release: tokio::sync::Notify,
        block_source: AtomicBool,
        hold_all_sources: AtomicBool,
        source_delay_ms: std::sync::atomic::AtomicU64,
        active_probes: std::sync::atomic::AtomicUsize,
        max_probes: std::sync::atomic::AtomicUsize,
        sql: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl Db for TestDb {
        async fn query(&self, sql: &str, _binds: &[(&str, Value)]) -> Result<Vec<Value>, DbError> {
            if sql.contains("SELECT VALUE version") {
                struct Active<'a>(&'a std::sync::atomic::AtomicUsize);
                impl Drop for Active<'_> { fn drop(&mut self) { self.0.fetch_sub(1, Ordering::AcqRel); } }
                let active = self.active_probes.fetch_add(1, Ordering::AcqRel) + 1;
                let _active = Active(&self.active_probes);
                self.max_probes.fetch_max(active, Ordering::AcqRel);
                if self.hold_all_sources.load(Ordering::Acquire) { std::future::pending::<()>().await; }
                let delay = self.source_delay_ms.load(Ordering::Acquire);
                if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                if self.block_source.swap(false, Ordering::AcqRel) {
                    self.source_started.notify_one();
                    self.source_release.notified().await;
                }
                return Ok(vec![json!(99)]);
            }
            if sql.starts_with("SELECT VALUE id FROM ONLY") { return Ok(vec![json!("_00_query:q")]); }
            if self.fail.load(Ordering::Acquire) && sql.contains("thread:a") { return Err(DbError::Transport("offline".into())); }
            self.sql.lock().unwrap().push(sql.into());
            Ok(vec![])
        }
        async fn version(&self) -> Result<String, DbError> { Ok("test".into()) }
    }
    struct FastScheduler;
    #[async_trait::async_trait]
    impl Scheduler for FastScheduler {
        async fn schedule(&self, _: TimerKind, _: u64) {}
        async fn cancel(&self, _: &TimerKind) {}
        async fn sleep(&self, duration: Duration) { tokio::time::sleep(duration).await; }
    }
    fn circuit() -> Arc<RwLock<Circuit>> {
        let mut c = Circuit::new();
        c.add_query(ssp::operator::QueryPlan { id: "q".into(), root: ssp::operator::OperatorPlan::Scan { table: "thread".into() } }, None, None);
        Arc::new(RwLock::new(c))
    }
    fn delta(id: &str, add: bool) -> ViewDelta {
        ViewDelta { query_id: "q".into(), additions: if add { vec![id.into()] } else { vec![] },
            removals: if add { vec![] } else { vec![id.into()] }, updates: vec![], row_count: 0,
            result_hash: String::new(), subquery_items: vec![], auth_id: String::new(), initial: false }
    }
    struct TestSpawner;
    impl crate::ports::Spawner for TestSpawner {
        fn spawn(&self, fut: crate::ports::LocalBoxFuture) { tokio::spawn(fut); }
    }
    fn start(p: EdgePublisher, db: Arc<TestDb>, c: Arc<RwLock<Circuit>>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { p.run(db.clone(), c, Arc::new(tokio::sync::Mutex::new(())), RefMode::Single,
            Arc::new(FastScheduler), Arc::new(NoopTelemetry), Duration::ZERO, db, Arc::new(TestSpawner)).await })
    }
    async fn drained(p: &EdgePublisher) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while p.snapshot().pending_batches > 0 { tokio::task::yield_now().await; }
        }).await.expect("publication queue drained");
    }
    #[test]
    fn publication_admission_bounds_slots_and_input_and_releases_unused_permits() {
        let p = EdgePublisher::new(PublicationLimits { slots: 1, bytes: 128, operations: 10 });
        assert!(p.try_reserve(129).is_none());
        let permit = p.try_reserve(128).unwrap();
        assert!(p.try_reserve(0).is_none());
        assert_eq!(p.snapshot().pending_bytes, 128);
        assert_eq!(p.snapshot().overload_total, 2);
        drop(permit);
        assert!(p.try_reserve(0).is_some());
    }
    #[tokio::test]
    async fn publication_measured_operations_stop_further_admission() {
        let p = EdgePublisher::new(PublicationLimits { slots: 2, bytes: 4096, operations: 1 });
        let c = circuit();
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", true)], &*c.read().await, None, false, vec![]);
        assert_eq!(p.snapshot().pending_operations, 1);
        assert!(p.try_reserve(0).is_none());
        assert_eq!(p.snapshot().worst_views[0].query_id, "q");
    }
    #[tokio::test]
    async fn publication_registration_barrier_and_commit_wait_preserve_add_delete_order() {
        let p = EdgePublisher::default();
        let c = circuit();
        let db = Arc::new(TestDb::default());
        db.block_source.store(true, Ordering::Release);
        let ready = p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", true)], &*c.read().await,
            Some(("thread:a".into(), 9)), true, vec![]).unwrap();
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", false)], &*c.read().await, None, false, vec![]);
        let task = start(p.clone(), db.clone(), c.clone());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(db.sql.lock().unwrap().is_empty(), "metadata must precede initial publication");
        ready.complete();
        db.source_started.notified().await;
        assert!(c.try_write().is_ok(), "source commit wait must not hold the circuit");
        assert!(db.sql.lock().unwrap().is_empty(), "delete cannot overtake the blocked add");
        assert_eq!(p.snapshot().pending_batches, 2);
        db.source_release.notify_one();
        drained(&p).await;
        let sql = db.sql.lock().unwrap();
        assert_eq!(sql.len(), 2);
        assert!(sql[0].contains("RELATE "));
        assert!(sql[1].contains("DELETE (SELECT"));
        task.abort();
    }
    #[tokio::test]
    async fn publication_source_wait_allows_unrelated_view_and_preserves_same_view_order() {
        let p = EdgePublisher::default();
        let c = circuit();
        c.write().await.add_query(ssp::operator::QueryPlan { id: "other".into(), root: ssp::operator::OperatorPlan::Scan { table: "thread".into() } }, None, None);
        let db = Arc::new(TestDb::default());
        db.block_source.store(true, Ordering::Release);
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", true)], &*c.read().await,
            Some(("thread:a".into(), 9)), false, vec![]);
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", false)], &*c.read().await, None, false, vec![]);
        let task = start(p.clone(), db.clone(), c.clone());
        db.source_started.notified().await;
        let mut other = delta("thread:b", true);
        other.query_id = "other".into();
        other.initial = true;
        p.enqueue(p.try_reserve(0).unwrap(), vec![other], &*c.read().await, None, false, vec![]);
        tokio::time::timeout(Duration::from_millis(500), async {
            while db.sql.lock().unwrap().is_empty() { tokio::task::yield_now().await; }
        }).await.expect("unrelated registration publishes while source DB remains blocked");
        // Use real elapsed time: the five-second visibility fallback must not
        // fire early, and the same-view delete must still wait for the add.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(db.sql.lock().unwrap().len(), 1);
        assert!(db.sql.lock().unwrap()[0].contains("thread:b"));
        assert_eq!(p.snapshot().pending_batches, 2);
        db.source_release.notify_one();
        drained(&p).await;
        let sql = db.sql.lock().unwrap();
        assert!(sql[1].contains("RELATE "));
        assert!(sql[2].contains("DELETE (SELECT"));
        task.abort();
    }
    #[tokio::test]
    async fn publication_source_burst_probes_concurrently_with_hard_probe_bound() {
        let p = EdgePublisher::default();
        let c = circuit();
        let db = Arc::new(TestDb::default());
        db.source_delay_ms.store(100, Ordering::Release);
        for i in 0..32 {
            let id = format!("thread:r{i}");
            p.enqueue(p.try_reserve(0).unwrap(), vec![delta(&id, true)], &*c.read().await,
                Some((id, 9)), false, vec![]);
        }
        let begin = web_time::Instant::now();
        let task = start(p.clone(), db.clone(), c);
        tokio::time::timeout(Duration::from_secs(1), drained(&p)).await
            .expect("32 real100ms source probes must overlap, not take3.2 seconds");
        assert!(begin.elapsed() >= Duration::from_millis(200));
        assert_eq!(db.max_probes.load(Ordering::Acquire), 16);
        assert_eq!(db.active_probes.load(Ordering::Acquire), 0);
        assert_eq!(db.sql.lock().unwrap().len(), 32);
        task.abort();
    }
    #[tokio::test]
    async fn publication_source_deadline_includes_probe_capacity_and_network_wait() {
        let p = EdgePublisher::default();
        let c = circuit();
        let db = Arc::new(TestDb::default());
        db.hold_all_sources.store(true, Ordering::Release);
        for i in 0..32 {
            let id = format!("thread:r{i}");
            p.enqueue(p.try_reserve(0).unwrap(), vec![delta(&id, true)], &*c.read().await,
                Some((id, 9)), false, vec![]);
        }
        let begin = web_time::Instant::now();
        let task = start(p.clone(), db.clone(), c);
        tokio::time::timeout(Duration::from_secs(6), async {
            while p.snapshot().pending_batches > 0 { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.expect("all source deadlines expire together, including probes waiting for capacity");
        assert!(begin.elapsed() >= Duration::from_secs(5));
        assert_eq!(db.max_probes.load(Ordering::Acquire), 16);
        assert_eq!(db.active_probes.load(Ordering::Acquire), 0);
        assert_eq!(db.sql.lock().unwrap().len(), 32);
        task.abort();
    }
    #[tokio::test]
    async fn publication_invalidated_source_wait_cancels_and_releases_shared_lease() {
        let p = EdgePublisher::new(PublicationLimits { slots: 1, ..Default::default() });
        let c = circuit();
        let db = Arc::new(TestDb::default());
        db.block_source.store(true, Ordering::Release);
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", true)], &*c.read().await,
            Some(("thread:a".into(), 9)), false, vec![]);
        let task = start(p.clone(), db.clone(), c);
        db.source_started.notified().await;
        assert!(p.try_reserve(0).is_none());
        p.invalidate_all();
        drained(&p).await;
        assert!(p.try_reserve(0).is_some());
        assert_eq!(db.active_probes.load(Ordering::Acquire), 0);
        assert!(db.sql.lock().unwrap().is_empty());
        task.abort();
    }
    #[tokio::test]
    async fn publication_parked_work_keeps_permit_until_recovery() {
        let p = EdgePublisher::new(PublicationLimits { slots: 1, ..Default::default() });
        let c = circuit();
        let db = Arc::new(TestDb::default());
        db.fail.store(true, Ordering::Release);
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", true)], &*c.read().await, None, false, vec![]);
        let task = start(p.clone(), db.clone(), c);
        tokio::time::timeout(Duration::from_secs(2), async {
            while p.snapshot().parked_batches == 0 { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert!(p.try_reserve(0).is_none());
        assert_eq!(p.snapshot().pending_operations, 1);
        db.fail.store(false, Ordering::Release);
        drained(&p).await;
        assert!(p.snapshot().last_success_at_ms.is_some());
        assert!(p.try_reserve(0).is_some());
        assert_eq!(db.sql.lock().unwrap().len(), 1);
        task.abort();
    }
    #[tokio::test]
    async fn publication_poisoned_view_does_not_block_unrelated_registration() {
        let p = EdgePublisher::default();
        let c = circuit();
        c.write().await.add_query(ssp::operator::QueryPlan { id: "other".into(), root: ssp::operator::OperatorPlan::Scan { table: "thread".into() } }, None, None);
        let db = Arc::new(TestDb::default());
        db.fail.store(true, Ordering::Release);
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", true)], &*c.read().await, None, false, vec![]);
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", false)], &*c.read().await, None, false, vec![]);
        let mut other = delta("thread:b", true);
        other.query_id = "other".into();
        other.initial = true;
        p.enqueue(p.try_reserve(0).unwrap(), vec![other], &*c.read().await, None, false, vec![]);
        let task = start(p.clone(), db.clone(), c);
        tokio::time::timeout(Duration::from_secs(2), async {
            while db.sql.lock().unwrap().is_empty() { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert!(db.sql.lock().unwrap()[0].contains("thread:b"));
        assert_eq!(p.snapshot().parked_batches, 1);
        db.fail.store(false, Ordering::Release);
        drained(&p).await;
        let sql = db.sql.lock().unwrap();
        assert!(sql[1].contains("RELATE "));
        assert!(sql[2].contains("DELETE (SELECT"));
        task.abort();
    }
    #[tokio::test]
    async fn publication_empty_registration_still_blocks_dependent_ingest() {
        let p = EdgePublisher::default();
        let c = circuit();
        let db = Arc::new(TestDb::default());
        let empty = c.read().await.snapshot_delta("q", String::new()).unwrap();
        assert!(empty.additions.is_empty());
        let ready = p.enqueue(p.try_reserve(0).unwrap(), vec![empty], &*c.read().await, None, true, vec![]).unwrap();
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", true)], &*c.read().await, None, false, vec![]);
        let task = start(p.clone(), db.clone(), c);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(db.sql.lock().unwrap().is_empty());
        ready.complete();
        drained(&p).await;
        assert!(db.sql.lock().unwrap().last().unwrap().contains("thread:a"));
        task.abort();
    }
    #[tokio::test]
    async fn publication_lifecycle_discards_old_registration_and_reset_work() {
        let p = EdgePublisher::default();
        let c = circuit();
        let db = Arc::new(TestDb::default());
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:old", true)], &*c.read().await, None, false, vec![]);
        p.invalidate_view("_00_query:q");
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:middle", true)], &*c.read().await, None, false, vec![]);
        let obsolete = p.try_reserve(0).unwrap();
        p.invalidate_all();
        assert!(!p.is_current(&obsolete));
        drop(obsolete);
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:new", true)], &*c.read().await, None, false, vec![]);
        let task = start(p.clone(), db.clone(), c);
        drained(&p).await;
        let sql = db.sql.lock().unwrap();
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("thread:new"));
        task.abort();
    }
    #[tokio::test]
    async fn publication_canceled_registration_discards_dependent_work() {
        let p = EdgePublisher::default();
        let c = circuit();
        let db = Arc::new(TestDb::default());
        let ready = p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", true)], &*c.read().await, None, true, vec![]).unwrap();
        p.enqueue(p.try_reserve(0).unwrap(), vec![delta("thread:a", false)], &*c.read().await, None, false, vec![]);
        drop(ready);
        let task = start(p.clone(), db.clone(), c);
        drained(&p).await;
        assert!(db.sql.lock().unwrap().is_empty());
        task.abort();
    }
}
