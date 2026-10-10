use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Json, Router,
};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::messages::{BufferedEvent, RecordUpdate, RecordOp};
use crate::replica::Replica;
use crate::router::SspPool;
use crate::transport::HttpTransport;
use crate::wal::EventWal;
use crate::SchedulerStatus;
use ssp_protocol::IngestRequest;

/// Shared state for ingest handlers
#[derive(Clone)]
pub struct IngestState {
    pub replica: Arc<RwLock<Replica>>,
    pub transport: Arc<HttpTransport>,
    pub ssp_pool: Arc<RwLock<SspPool>>,
    pub status: Arc<RwLock<SchedulerStatus>>,
    pub event_buffer: Arc<RwLock<VecDeque<BufferedEvent>>>,
    pub seq_counter: Arc<AtomicU64>,
    pub wal: Arc<RwLock<EventWal>>,
    /// Serializes every `drain_and_apply` caller (periodic updater, SSP
    /// registration, pre-backup); see `Scheduler::drain_lock`.
    pub drain_lock: Arc<tokio::sync::Mutex<()>>,
    /// Shared upstream connection, published after scheduler initialization.
    pub db_slot: crate::admin::SharedDbSlot,
    /// Outbox tables from `SPKY_JOB_CONFIG`: only an UPDATE on one of these can
    /// be a job finishing, so everything else skips the hook entirely.
    pub job_tables: Arc<Vec<String>>,
    /// Caps concurrent `observe_job_terminal` tasks on the shared connection.
    /// Saturation is safe to drop: the schedule sweep heals within one tick.
    pub observer_permits: Arc<tokio::sync::Semaphore>,
    /// Lock-free mirror of the replica's `snapshot_seq`
    /// (`Replica::snapshot_seq_cell`). Health/metrics probes read this so
    /// they never queue behind a drain holding the replica write lock.
    pub snapshot_seq: Arc<AtomicU64>,
    /// Serialised SSP fan-out. `/ingest` hands the event over here once it is
    /// durable instead of delivering it inline; see [`Fanout`].
    pub fanout: Arc<Fanout>,
    /// What upstream syncs, lock-free (see `crate::schema`).
    pub schema: crate::schema::SchemaCell,
    /// The Web Push engine once `start()` has built it (see `crate::push`).
    pub push: crate::push::PushSlot,
    pub push_permits: Arc<tokio::sync::Semaphore>,
}

/// The SSP fan-out, moved off the `/ingest` request path.
///
/// # Why
///
/// The `_00_<table>_*` DB events `http::post` to `/ingest` **inside the user's
/// transaction**. While the fan-out ran inline, that transaction stayed open
/// until every ready SSP had acknowledged the event — so an SSP that was slow
/// (typically because it was waiting on the very same SurrealDB) made every
/// user write slow, which made SurrealDB slower still. A write → scheduler →
/// SSP → SurrealDB → write cycle, with the scheduler's 30s POST timeout as the
/// only bound. That is what produced `heartbeat failed stage="db_write_timeout"
/// detail=probe write exceeded 25s` and SurrealDB's "transaction was dropped
/// without being committed or cancelled".
///
/// The event is already durable before the fan-out (WAL append with flush,
/// then the in-memory buffer), and a scheduler crash replays from the WAL, so
/// answering the DB event at that point loses nothing.
///
/// # Why a queue and not a bare `tokio::spawn`
///
/// Delivery order and the `Lagging` bookkeeping are one interleaved sequence:
/// a failed delivery must mark the SSP lagging, the event must then be
/// buffered, and only then may redelivery start — otherwise redelivery finds
/// an empty queue, flips the SSP back to `Ready`, and drops exactly the event
/// that failed. Spawning per event would let two events race through that
/// sequence. One consumer keeps it strictly ordered, which is in fact stronger
/// than the old inline path, where concurrent requests could already interleave
/// across the broadcast await.
///
/// A stall does not grow the queue without bound in practice: the first failed
/// delivery parks the SSP in `Lagging`, and every later event then skips the
/// network entirely and goes straight to the buffer.
pub struct Fanout {
    tx: tokio::sync::mpsc::UnboundedSender<FanoutJob>,
    /// Events handed to the queue. Paired with `completed` so callers can wait
    /// for the queue to catch up; see [`Fanout::idle`].
    submitted: AtomicU64,
    completed: tokio::sync::watch::Receiver<u64>,
}

struct FanoutJob {
    /// Carrying the state per job (rather than handing it to the worker once)
    /// keeps `Fanout` constructible before the state that references it. The
    /// resulting `Arc` cycle is deliberate: the queue is meant to live exactly
    /// as long as the process.
    state: IngestState,
    request: IngestRequest,
    operation: RecordOp,
    seq: u64,
}

impl Fanout {
    /// Start the consumer. One per scheduler.
    pub fn start() -> Arc<Self> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<FanoutJob>();
        let (done_tx, completed) = tokio::sync::watch::channel(0u64);
        tokio::spawn(async move {
            let mut n = 0u64;
            let mut pending = None;
            loop {
                let job = match pending.take() { Some(job) => job, None => match rx.recv().await {
                    Some(job) => job, None => break,
                }};
                let mut bytes = serde_json::to_vec(&job.request).map_or(usize::MAX, |v| v.len()).saturating_add(14);
                let can_batch = batchable(&job) && bytes <= ssp_protocol::MAX_INGEST_BATCH_BYTES;
                let mut jobs = vec![job];
                if can_batch {
                    // Drain only what is already queued: no timer and no added latency.
                    while jobs.len() < ssp_protocol::MAX_INGEST_BATCH_RECORDS {
                        let Ok(next) = rx.try_recv() else { break };
                        let size = serde_json::to_vec(&next.request).map_or(usize::MAX, |v| v.len()).saturating_add(1);
                        if !batchable(&next) || !Arc::ptr_eq(&jobs[0].state.ssp_pool, &next.state.ssp_pool)
                            || bytes.saturating_add(size) > ssp_protocol::MAX_INGEST_BATCH_BYTES {
                            pending = Some(next);
                            break;
                        }
                        bytes += size;
                        jobs.push(next);
                    }
                }
                n += jobs.len() as u64;
                if jobs.len() == 1 {
                    let job = jobs.pop().unwrap();
                    fan_out_event(job.state, job.request, job.operation, job.seq).await;
                } else { fan_out_batch(jobs).await; }
                let _ = done_tx.send(n);
            }
        });
        Arc::new(Self {
            tx,
            submitted: AtomicU64::new(0),
            completed,
        })
    }

    fn submit(&self, job: FanoutJob) {
        self.submitted.fetch_add(1, Ordering::SeqCst);
        // The only way this fails is a dropped consumer, i.e. the runtime is
        // going away. The event is in the WAL either way.
        if self.tx.send(job).is_err() {
            error!("Ingest fan-out queue is closed; event stays for WAL replay");
        }
    }

    /// Wait until every event submitted so far has been fanned out.
    ///
    /// Delivery is no longer finished when `/ingest` answers, so anything that
    /// needs to observe its effect — tests asserting on SSP state after a
    /// post, a drain that wants the queue quiet — has to wait for it here
    /// rather than assume it already happened.
    pub async fn idle(&self) {
        let target = self.submitted.load(Ordering::SeqCst);
        let mut completed = self.completed.clone();
        while *completed.borrow_and_update() < target {
            if completed.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Snapshot of how far behind the replica is vs. the ingest stream.
/// `pending_events` are durable in the WAL but not yet applied to the replica.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PendingEventsStat {
    pub pending_events: usize,
    pub snapshot_seq: u64,
    pub latest_seq: u64,
    pub lag: u64,
}

/// Cheap, non-blocking read of the in-memory buffer + counters. Deliberately
/// does NOT touch the replica lock: a drain/reclone holds it for a long time,
/// and this runs on every /health, /health/ready and /metrics request.
pub async fn pending_events_snapshot(state: &IngestState) -> PendingEventsStat {
    let pending_events = state.event_buffer.read().await.len();
    let snapshot_seq = state.snapshot_seq.load(Ordering::Relaxed);
    let latest_seq = state.seq_counter.load(Ordering::SeqCst);
    let lag = latest_seq.saturating_sub(snapshot_seq);
    PendingEventsStat {
        pending_events,
        snapshot_seq,
        latest_seq,
        lag,
    }
}

/// Create ingest router
pub fn create_ingest_router(state: IngestState) -> Router {
    Router::new()
        .route("/ingest", post(handle_ingest))
        .with_state(state)
}

/// Handle ingest requests from database events
async fn handle_ingest(
    State(state): State<IngestState>,
    Json(request): Json<IngestRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    ingest_event(&state, request, 0).await.map(|_| StatusCode::OK)
}

/// Take one row change into the pipeline: WAL, buffer, job-terminal observer,
/// SSP fan-out. Shared by the HTTP `/ingest` route (the generated DB events)
/// and the changefeed tail, which passes the commit `versionstamp` it read the
/// change at so the WAL can say where the tail resumes after a restart.
/// Returns the seq the event was assigned.
pub async fn ingest_event(
    state: &IngestState,
    request: IngestRequest,
    versionstamp: u64,
) -> Result<u64, (StatusCode, String)> {
    ingest_event_from(state, request, versionstamp, push_core::Origin::Live).await
}

/// [`ingest_event`] for a caller that is not a live change: drift repair
/// re-emits rows that changed long ago, and those must never push.
pub async fn ingest_event_from(
    state: &IngestState,
    request: IngestRequest,
    versionstamp: u64,
    origin: push_core::Origin,
) -> Result<u64, (StatusCode, String)> {
    // A direct push. Never a synced row: it goes to the push engine and
    // nowhere else (no WAL, no replica, no SSP), so it is taken before the
    // clone gate below, which would otherwise refuse (under the http
    // transport: abort) the write that created it. Left `pending` when the
    // engine is not up yet; the engine's sweep sends missed messages.
    if request.table == push_core::engine::MESSAGE_TABLE {
        crate::push::observe(
            &state.push,
            &state.push_permits,
            &request.table,
            &request.op,
            &request.id,
            &request.record,
            origin,
        );
        return Ok(0);
    }

    // Gate. A 503 here is not a soft failure upstream: the `_00_<table>_*`
    // DB events `http::post` to this endpoint inside the user's transaction,
    // so a refused ingest ABORTS the user's write. That is acceptable only
    // while there is genuinely nowhere to put the event — the initial clone
    // (no replica yet, and the event may or may not be inside the clone's
    // cut) and a restore. A scheduler booting on a persisted snapshot has a
    // replica and a WAL: the event is appended and applied at the first
    // drain, exactly as during normal operation. Before this, every
    // scheduler restart was a multi-minute window of failed writes.
    let scheduler_status = *state.status.read().await;
    match scheduler_status {
        SchedulerStatus::Cloning if state.snapshot_seq.load(Ordering::Relaxed) == 0 => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "SSP_NOT_READY: Scheduler is cloning database".to_string(),
            ));
        }
        SchedulerStatus::Restoring => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "SSP_NOT_READY: Scheduler is restoring from backup".to_string(),
            ));
        }
        _ => {}
    }

    info!(
        "Received ingest: {} {} on {}",
        request.op, request.id, request.table
    );

    // A table upstream marks `-- @nosync` can still fire an event it was
    // generated before the marker, or keep a changefeed it was defined with.
    // Applying one would put the table straight back into the replica (and
    // its hash back into every bootstrap) right after the schema reconcile
    // dropped it. Only positive knowledge: a table the last probe did not list
    // at all is one a deploy just added, and its events are the point.
    if crate::schema::SchemaWatch::is_nosync(&state.schema, &request.table) {
        tracing::debug!(table = %request.table, id = %request.id, "Ingest for a @nosync table dropped");
        return Ok(0);
    }

    // Parse operation
    let operation = match request.op.to_uppercase().as_str() {
        "CREATE" => RecordOp::Create,
        "UPDATE" => RecordOp::Update,
        "DELETE" => RecordOp::Delete,
        _ => {
            error!("Invalid operation: {}", request.op);
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Invalid operation: {}", request.op),
            ));
        }
    };

    // An event whose record id belongs to ANOTHER table is not a row of
    // `request.table` and must not enter the WAL, the replica or an SSP.
    // Observed on SurrealDB 3.0.5 during `spky release`: deleting
    // `_00_app_release:web` cascade-deleted the `_00_list_ref_*` edges that
    // pointed at it, and the vertex table's DELETE event fired once per edge
    // with the EDGE as `$before`, so the scheduler got
    // `table=_00_app_release id=_00_list_ref_anon:<edge>`. Applying that
    // produced `DELETE _00_app_release:_00_list_ref_anon:…` (a parse error)
    // on every drain. Answer 200 so the upstream event does not retry.
    if let Some(foreign) = foreign_table_prefix(&request.table, &request.id) {
        warn!(
            table = %request.table,
            record_id = %request.id,
            foreign_table = %foreign,
            "Ignoring ingest event whose record id belongs to another table (cascaded edge delete?)"
        );
        return Ok(0);
    }

    // From the seq to the fan-out hand-off, one unit: a scheduler handing
    // over takes this gate for writing, so no event is ever half taken (in
    // the WAL but never fanned out) when its successor starts.
    let _ingest = crate::handover::ingest_gate().read().await;

    // Assign monotonic sequence number
    let seq = state.seq_counter.fetch_add(1, Ordering::SeqCst) + 1;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Create the buffered event
    let record_update = RecordUpdate {
        table: request.table.clone(),
        operation,
        record_id: request.id.clone(),
        data: Some(request.record.clone()),
        version: seq,
        job_assignee: None,
    };

    let buffered_event = BufferedEvent {
        seq,
        update: record_update,
        received_at: now,
        versionstamp,
    };

    // Write-ahead: append to WAL before processing. The append is synchronous
    // file IO (write + flush) — run it on the blocking pool, never on a
    // runtime worker. The owned guard moves into the closure so the WAL lock
    // still serializes appends.
    {
        let wal_guard = Arc::clone(&state.wal).write_owned().await;
        let event = buffered_event.clone();
        let append_result = tokio::task::spawn_blocking(move || {
            let mut wal = wal_guard;
            wal.append(&event)
        })
        .await;
        match append_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(error = %e, "Failed to write to WAL");
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("WAL write failed: {}", e),
                ));
            }
            Err(e) => {
                error!(error = %e, "WAL append task panicked");
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("WAL write failed: {}", e),
                ));
            }
        }
    }

    // Append to in-memory event buffer
    {
        let mut buffer = state.event_buffer.write().await;
        buffer.push_back(buffered_event.clone());
    }

    // A job-table UPDATE may be the runner terminalizing a scheduled job or a
    // workflow step. The scheduler is both the ingest entrypoint and the
    // cluster's single ticker, so this is where the engine learns about it.
    // Best-effort: the sweep's heal pass covers a missed event within one tick.
    if request.op.eq_ignore_ascii_case("UPDATE")
        && state.job_tables.iter().any(|t| t == &request.table)
    {
        if let Some(status) = request.record.get("status").and_then(|v| v.as_str()) {
            crate::schedule_engine::observe_job_terminal(
                Arc::clone(&state.ssp_pool),
                Arc::clone(&state.transport),
                Arc::clone(&state.db_slot),
                Arc::clone(&state.observer_permits),
                request.id.clone(),
                status.to_string(),
            );
        }
    }

    // Push rules watch synced rows. Spawned and bounded: the tail and the
    // user's transaction never wait on a push service.
    crate::push::observe(
        &state.push,
        &state.push_permits,
        &request.table,
        &request.op,
        &request.id,
        &request.record,
        origin,
    );

    // Durable now (WAL flushed, buffered). Hand the SSP fan-out to the queue
    // and answer, so the user's transaction is not held open for it.
    state.fanout.submit(FanoutJob {
        state: state.clone(),
        request,
        operation,
        seq,
    });

    info!(seq, "Ingest accepted");
    Ok(seq)
}

/// Deliver one ingested event to the SSPs. Runs on the [`Fanout`] consumer,
/// never on the `/ingest` request path.
async fn fan_out_event(
    state: IngestState,
    request: IngestRequest,
    operation: RecordOp,
    seq: u64,
) {
    let mut request = request;
    let mut update = RecordUpdate {
        table: request.table.clone(),
        operation,
        record_id: request.id.clone(),
        data: Some(request.record.clone()),
        version: seq,
        job_assignee: None,
    };

    // One pool lock settles who runs a job this event creates, who gets the
    // event live, and who gets it queued. Every copy names the same assignee:
    // the queued ones are replayed with it, so it holds even for a lagging
    // SSP that is the assignee itself. Split over two locks, an SSP that went
    // from lagging or replaying to Ready in between was neither broadcast to
    // nor queued for, and missed the event.
    let ready_ssps = {
        let mut pool = state.ssp_pool.write().await;
        request.job_assignee = pool.select_job_runner();
        update.job_assignee = request.job_assignee.clone();

        let mut ready_ssps = Vec::new();
        let mut off_live_path = Vec::new();
        for ssp in pool.all() {
            if pool.is_ready(&ssp.id) {
                ready_ssps.push(ssp.clone());
            } else {
                off_live_path.push(ssp.id.clone());
            }
        }
        // Bootstrapping, replaying, or lagging behind a failed delivery.
        for ssp_id in off_live_path {
            if !pool.buffer_message(&ssp_id, update.clone()) {
                warn!("Buffer overflow for SSP '{}', needs re-bootstrap", ssp_id);
            }
        }
        ready_ssps
    };

    info!(
        table = %request.table,
        op = %request.op,
        record_id = %request.id,
        job_assignee = ?request.job_assignee,
        "Ingest: job assignee selected for event"
    );

    // SSPs that missed THIS event and need a redelivery task (see below).
    let mut newly_lagging: Vec<String> = Vec::new();

    if !ready_ssps.is_empty() {
        info!("Broadcasting to {} ready SSPs", ready_ssps.len());
        let results = state
            .transport
            .broadcast_to_ssps(&ready_ssps, "/ingest", &request)
            .await;

        for (ssp_id, result) in results {
            if let Err(e) = result {
                error!("Failed to send to SSP '{}': {}", ssp_id, e);
                // A Ready SSP that did not acknowledge this event has not
                // applied it (a POST that times out is dropped with the
                // connection), and nothing downstream would ever resend it:
                // the row would be missing from every view on that SSP until
                // a cold re-registration. Park the SSP in `Lagging` and queue
                // this event, so every later one queues behind it. The
                // redelivery task is started only after that, or it could
                // find an empty queue, flip the SSP back to Ready, and lose
                // exactly this event.
                let mut pool = state.ssp_pool.write().await;
                if pool.mark_lagging(&ssp_id) {
                    newly_lagging.push(ssp_id.clone());
                }
                if !pool.buffer_message(&ssp_id, update.clone()) {
                    warn!("Buffer overflow for SSP '{}', needs re-bootstrap", ssp_id);
                }
            }
        }
    }

    // Only now, with the missed event safely queued, start catching up the
    // SSPs that missed it. One task per `Ready → Lagging` transition.
    for ssp_id in newly_lagging {
        crate::handover::spawn_singleton("lagging-redelivery", redeliver_to_lagging_ssp(state.clone(), ssp_id));
    }

    info!(seq, "Ingest fanned out to SSPs");
}

/// Jobs and lifecycle records retain their existing single-event side effects.
fn batchable(job: &FanoutJob) -> bool {
    let table = &job.request.table;
    table != "user" && !table.starts_with("_00_") && !job.state.job_tables.contains(table)
}

/// Deliver one bounded, ordered group. Legacy SSPs still receive single rows.
async fn fan_out_batch(jobs: Vec<FanoutJob>) {
    let state = jobs[0].state.clone();
    let (requests, updates, ready) = {
        let mut pool = state.ssp_pool.write().await;
        let mut requests = Vec::with_capacity(jobs.len());
        let mut updates = Vec::with_capacity(jobs.len());
        for job in jobs {
            let mut request = job.request;
            request.job_assignee = pool.select_job_runner();
            updates.push(RecordUpdate { table: request.table.clone(), operation: job.operation,
                record_id: request.id.clone(), data: Some(request.record.clone()), version: job.seq,
                job_assignee: request.job_assignee.clone() });
            requests.push(request);
        }
        let mut ready = Vec::new();
        let mut buffered = Vec::new();
        for ssp in pool.all() {
            if pool.is_ready(&ssp.id) { ready.push(ssp.clone()); }
            else { buffered.push(ssp.id.clone()); }
        }
        for id in buffered { for update in &updates {
            if !pool.buffer_message(&id, update.clone()) { warn!(ssp_id = %id, "Buffer overflow; SSP needs re-bootstrap"); }
        }}
        (requests, updates, ready)
    };
    let mut newly_lagging = Vec::new();
    let outcomes = futures::future::join_all(ready.into_iter().map(|ssp| {
        let requests = &requests;
        let transport = &state.transport;
        async move {
            let limit = ssp.ingest_batch_limit.min(ssp_protocol::MAX_INGEST_BATCH_RECORDS);
            let mut delivered = 0;
            while delivered < requests.len() {
                let mut end = if limit > 1 { (delivered + limit).min(requests.len()) } else { delivered + 1 };
                // Assignment is part of the wire body. Check the exact final envelope,
                // so even long SSP identifiers cannot turn an admitted group into 413.
                while end - delivered > 1 && serde_json::to_vec(&ssp_protocol::IngestBatchRequest {
                    records: requests[delivered..end].to_vec(),
                }).map_or(true, |body| body.len() > ssp_protocol::MAX_INGEST_BATCH_BYTES) { end -= 1; }
                let result = if end - delivered > 1 {
                    transport.post_to_ssp(&ssp.url, "/ingest/batch", &ssp_protocol::IngestBatchRequest {
                        records: requests[delivered..end].to_vec(),
                    }).await
                } else { transport.post_to_ssp(&ssp.url, "/ingest", &requests[delivered]).await };
                if let Err(error) = result { return (ssp.id, delivered, Some(error)); }
                delivered = end;
            }
            (ssp.id, delivered, None)
        }
    })).await;
    for (id, delivered, error) in outcomes {
        if let Some(error) = error {
            error!(ssp_id = %id, %error, undelivered = requests.len() - delivered, "Failed batch delivery");
            let mut pool = state.ssp_pool.write().await;
            if pool.mark_lagging(&id) { newly_lagging.push(id.clone()); }
            // A timeout may have applied the batch: replay remains ordered and idempotent,
            // just as with /ingest. Never let later rows overtake the failed group.
            for update in &updates[delivered..] {
                if !pool.buffer_message(&id, update.clone()) { warn!(ssp_id = %id, "Buffer overflow; SSP needs re-bootstrap"); }
            }
        }
    }
    for id in newly_lagging {
        crate::handover::spawn_singleton("lagging-redelivery", redeliver_to_lagging_ssp(state.clone(), id));
    }
}

/// The table an event's record id names when it is NOT `table`.
///
/// Ids arrive either bare (`abc`) or qualified (`game:abc`). A qualified id
/// whose prefix is a different table name means the event is not about a row
/// of `table` at all. Ids with escaped or composite forms (`⟨…⟩`, `{…}`,
/// backticks, or a first segment that is not a plain identifier) are left
/// alone: only a clean `identifier:` prefix is compared.
pub fn foreign_table_prefix(table: &str, id: &str) -> Option<String> {
    let (prefix, _) = id.split_once(':')?;
    let is_ident = !prefix.is_empty()
        && prefix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if is_ident && prefix != table {
        Some(prefix.to_string())
    } else {
        None
    }
}

/// The `/ingest` body for a buffered event, here and in the bootstrap replay
/// (`ssp_management::poll_and_replay_ssp`): the request the live broadcast
/// sent, job assignee included. Without it the SSP that was given a job while
/// lagging received the CREATE as nobody's job and never ran it.
pub(crate) fn replay_payload(message: &RecordUpdate) -> IngestRequest {
    IngestRequest {
        table: message.table.clone(),
        op: message.operation.to_string(),
        id: message.record_id.clone(),
        record: message.data.clone().unwrap_or(serde_json::json!({})),
        job_assignee: message.job_assignee.clone(),
    }
}

/// Bring a `Lagging` SSP back to `Ready` by delivering its buffered events in
/// order, retrying with backoff while it stays unresponsive.
///
/// Spawned by `handle_ingest` on the `Ready → Lagging` transition, exactly
/// once per episode. Runs until the SSP is caught up (buffer empty, flipped
/// back to `Ready` atomically with the final drain), or until the episode is
/// over for another reason: the SSP was evicted by the heartbeat-stale sweep,
/// re-registered (which re-bootstraps it, replaying from the frozen
/// snapshot), or overflowed its buffer (its next heartbeat gets 409 and it
/// re-bootstraps). In every one of those the events reach it another way.
pub async fn redeliver_to_lagging_ssp(state: IngestState, ssp_id: String) {
    let mut backoff = REDELIVERY_INITIAL_BACKOFF;

    loop {
        let (url, batch) = {
            let mut pool = state.ssp_pool.write().await;
            let Some(url) = pool.get(&ssp_id).map(|info| info.url.clone()) else {
                info!(ssp_id, "Redelivery stopped: SSP no longer in the pool");
                return;
            };
            if !pool.is_lagging(&ssp_id) {
                info!(ssp_id, "Redelivery stopped: SSP re-registered, its bootstrap replays instead");
                return;
            }
            if pool.has_buffer_overflow(&ssp_id) {
                warn!(ssp_id, "Redelivery stopped: buffer overflowed, SSP will re-bootstrap");
                return;
            }
            let batch = pool.drain_buffer(&ssp_id);
            if batch.is_empty() {
                // Nothing left: go Ready atomically with a final drain. An
                // event that landed between the drain above and `mark_ready`
                // comes back here and keeps the SSP lagging until it is out.
                let remaining = pool.mark_ready(&ssp_id);
                if remaining.is_empty() {
                    info!(ssp_id, "SSP caught up on missed live events, back to ready");
                    return;
                }
                pool.mark_lagging(&ssp_id);
                pool.requeue_front(&ssp_id, remaining);
                continue;
            }
            (url, batch)
        };

        let mut failed_at = None;
        for (i, message) in batch.iter().enumerate() {
            if let Err(e) = state
                .transport
                .post_to_ssp(&url, "/ingest", &replay_payload(message))
                .await
            {
                let busy = crate::transport::is_publication_backlog(&e);
                if busy && i > 0 {
                    // Flow control, not a fault: it took events until its
                    // publication queue filled up again.
                    debug!(ssp_id, delivered = i, undelivered = batch.len() - i,
                        "Lagging SSP's publication queue is full; resuming shortly");
                } else {
                    warn!(
                        ssp_id,
                        error = %e,
                        undelivered = batch.len() - i,
                        "Redelivery to lagging SSP failed; retrying after backoff"
                    );
                }
                failed_at = Some((i, busy));
                break;
            }
        }

        match failed_at {
            None => {
                info!(ssp_id, delivered = batch.len(), "Redelivered missed events to lagging SSP");
                backoff = REDELIVERY_INITIAL_BACKOFF;
            }
            Some((i, busy)) => {
                state
                    .ssp_pool
                    .write()
                    .await
                    .requeue_front(&ssp_id, batch[i..].to_vec());
                let (wait, next) = redelivery_wait(backoff, i, busy);
                tokio::time::sleep(wait).await;
                backoff = next;
            }
        }
    }
}

/// First redelivery wait, and the doubling ceiling while the SSP does not
/// answer at all (connection refused, timeout, an error status).
pub(crate) const REDELIVERY_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(500);
const REDELIVERY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(10);
/// The ceiling while the SSP answers `503 publication_backlog`: it is up and
/// emptying its queue, which takes about a second. Riding the 10 s ceiling
/// there kept it idle nine seconds in ten (whitepawn 2026-10-08: ~400 events
/// per 10.5 s against a PGN import writing ~125 a second, a 4-minute lag
/// during which every user's registrations were refused).
const REDELIVERY_BUSY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

/// How long to wait after a redelivery attempt that stopped at a refused
/// event, and the backoff to carry into the next one. `delivered` is how many
/// events the attempt got through first: any progress means the SSP is taking
/// events, so doubling starts over. A `busy` SSP is never waited on for more
/// than [`REDELIVERY_BUSY_MAX_BACKOFF`].
pub(crate) fn redelivery_wait(
    backoff: std::time::Duration,
    delivered: usize,
    busy: bool,
) -> (std::time::Duration, std::time::Duration) {
    let base = if delivered > 0 { REDELIVERY_INITIAL_BACKOFF } else { backoff };
    let wait = if busy { base.min(REDELIVERY_BUSY_MAX_BACKOFF) } else { base };
    (wait, (wait * 2).min(REDELIVERY_MAX_BACKOFF))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn waits(mut backoff: Duration, attempts: &[(usize, bool)]) -> Vec<Duration> {
        attempts.iter().map(|&(delivered, busy)| {
            let (wait, next) = redelivery_wait(backoff, delivered, busy);
            backoff = next;
            wait
        }).collect()
    }

    #[test]
    fn an_unreachable_ssp_backs_off_to_ten_seconds() {
        let ms = |v: u64| Duration::from_millis(v);
        assert_eq!(
            waits(REDELIVERY_INITIAL_BACKOFF, &[(0, false); 7]),
            [ms(500), ms(1000), ms(2000), ms(4000), ms(8000), ms(10_000), ms(10_000)]
        );
    }

    #[test]
    fn a_busy_ssp_is_retried_within_a_second() {
        let ms = |v: u64| Duration::from_millis(v);
        assert_eq!(
            waits(REDELIVERY_INITIAL_BACKOFF, &[(0, true); 5]),
            [ms(500), ms(1000), ms(1000), ms(1000), ms(1000)]
        );
        // Even after an unreachable stretch reached the ten-second ceiling.
        assert_eq!(redelivery_wait(REDELIVERY_MAX_BACKOFF, 0, true).0, ms(1000));
    }

    #[test]
    fn progress_starts_the_backoff_over() {
        let ms = |v: u64| Duration::from_millis(v);
        assert_eq!(redelivery_wait(REDELIVERY_MAX_BACKOFF, 400, true), (ms(500), ms(1000)));
        assert_eq!(redelivery_wait(REDELIVERY_MAX_BACKOFF, 1, false), (ms(500), ms(1000)));
    }
}
