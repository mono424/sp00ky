use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Json, Router,
};
use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::config::SchedulerConfig;
use crate::messages::{BufferedEvent, RecordOp, RecordUpdate, SspHeartbeat};
use crate::replica::Replica;
use crate::router::SspPool;
use crate::transport::{HttpTransport, SspInfo};
use crate::wal::EventWal;
use crate::SchedulerStatus;
use ssp_protocol::{SspRegistration, SspRegistrationResponse};

/// Shared state for SSP management handlers
#[derive(Clone)]
pub struct SspManagementState {
    pub ssp_pool: Arc<RwLock<SspPool>>,
    pub replica: Arc<RwLock<Replica>>,
    pub transport: Arc<HttpTransport>,
    pub config: Arc<SchedulerConfig>,
    pub status: Arc<RwLock<SchedulerStatus>>,
    pub event_buffer: Arc<RwLock<VecDeque<BufferedEvent>>>,
    /// Global sequence counter — the seq the replica reflects after a re-clone.
    pub seq_counter: Arc<AtomicU64>,
    /// Serializes catch-up replica re-clones so two looping SSPs can't reset +
    /// re-ingest the shared replica concurrently. A `try_lock` failure means a
    /// re-clone is already running; the losing task just re-bootstraps instead.
    pub reclone_lock: Arc<Mutex<()>>,
    /// Event WAL, needed by the pre-registration drain (`drain_and_apply`
    /// truncates it as part of advancing the snapshot).
    pub wal: Arc<RwLock<EventWal>>,
    /// Shared with the periodic snapshot updater and `pre_backup`: serializes
    /// every `drain_and_apply` caller so registration never captures hashes
    /// mid-drain. Lock order is always `drain_lock` → replica; never combine
    /// with `reclone_lock` in one path.
    pub drain_lock: Arc<Mutex<()>>,
}

/// Create SSP management router
pub fn create_ssp_router(state: SspManagementState) -> Router {
    Router::new()
        .route("/ssp/register", post(handle_register))
        .route("/ssp/bootstrap-verify", post(handle_bootstrap_verify))
        .route("/ssp/heartbeat", post(handle_heartbeat))
        .route("/ssp/bootstrap-progress", post(handle_bootstrap_progress))
        .route("/admin/ssp/resync-all", post(handle_resync_all))
        .route("/admin/resync", post(handle_resync))
        .with_state(state)
}

/// Record an SSP's bootstrap progress. Purely advisory: the value is surfaced
/// on `/info` and the admin dashboard and read by nothing else, so this always
/// answers 200 — a bootstrapping SSP must never be made to care whether its
/// progress bar landed.
async fn handle_bootstrap_progress(
    State(state): State<SspManagementState>,
    Json(req): Json<ssp_protocol::SspBootstrapProgressRequest>,
) -> StatusCode {
    let mut pool = state.ssp_pool.write().await;
    pool.set_bootstrap_progress(&req.ssp_id, req.progress);
    StatusCode::OK
}

/// Force every connected SSP to re-bootstrap on its next heartbeat.
/// Used by `spky verify --fix` and the integrity-check pipeline when an SSP's
/// circuit hashes have drifted from the scheduler's frozen snapshot.
async fn handle_resync_all(
    State(state): State<SspManagementState>,
) -> Json<serde_json::Value> {
    let count = {
        let mut pool = state.ssp_pool.write().await;
        pool.mark_all_for_resync()
    };
    info!(count, "Flagged SSPs for forced re-bootstrap");
    Json(serde_json::json!({ "marked_for_resync": count }))
}

/// Body for `POST /admin/resync`. Both fields optional; an empty (or absent)
/// body means a full re-clone.
#[derive(Debug, Default, serde::Deserialize)]
struct ResyncRequest {
    /// `"reclone"` (default): reset the replica and re-ingest everything from
    /// upstream SurrealDB. `"rehash"`: keep the replica content and recompute
    /// the persisted snapshot hashes from it (repairs hash-metadata drift).
    mode: Option<String>,
    /// For `rehash` only: limit the recompute to these tables.
    tables: Option<Vec<String>>,
}

/// Repair the scheduler's snapshot state without a volume wipe. This is the
/// operator remedy the startup integrity check points at — the alternative to
/// `spky cloud restart --clean`, which throws away the whole scheduler volume
/// for what is usually just stale snapshot metadata.
async fn handle_resync(
    State(state): State<SspManagementState>,
    body: Option<Json<ResyncRequest>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    let mode = req.mode.as_deref().unwrap_or("reclone");
    let args = ResyncArgs {
        ssp_pool: Arc::clone(&state.ssp_pool),
        replica: Arc::clone(&state.replica),
        config: Arc::clone(&state.config),
        status: Arc::clone(&state.status),
        seq_counter: Arc::clone(&state.seq_counter),
        reclone_lock: Arc::clone(&state.reclone_lock),
    };
    run_resync(&args, mode, req.tables).await.map(Json)
}

fn directive_body(d: &ssp_protocol::ResyncDirective) -> String {
    serde_json::to_string(d).unwrap_or_else(|_| d.reason.clone())
}

/// Everything a resync needs, so the admin plane can run one without holding
/// the whole `SspManagementState` (whose WAL and drain lock it has no business
/// touching).
#[derive(Clone)]
pub struct ResyncArgs {
    pub ssp_pool: Arc<RwLock<SspPool>>,
    pub replica: Arc<RwLock<Replica>>,
    pub config: Arc<SchedulerConfig>,
    pub status: Arc<RwLock<SchedulerStatus>>,
    pub seq_counter: Arc<AtomicU64>,
    pub reclone_lock: Arc<Mutex<()>>,
}

/// Run a `reclone` or `rehash` and flag every SSP to re-verify afterwards.
/// Shared by `POST /admin/resync` on the ingest port and the dashboard's
/// scheduler actions, so the two can never disagree about what a mode means.
pub async fn run_resync(
    state: &ResyncArgs,
    mode: &str,
    tables: Option<Vec<String>>,
) -> Result<serde_json::Value, (StatusCode, String)> {
    match *state.status.read().await {
        SchedulerStatus::Cloning => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "Scheduler is still cloning database".to_string(),
            ));
        }
        SchedulerStatus::Restoring => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "Scheduler is restoring from backup".to_string(),
            ));
        }
        _ => {}
    }

    match mode {
        "rehash" => {
            let tables: Option<BTreeSet<String>> =
                tables.map(|t| t.into_iter().collect());
            let rehashed = {
                let mut rep = state.replica.write().await;
                let seq = rep.snapshot_seq();
                rep.set_snapshot_state(seq, tables.as_ref())
                    .await
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("rehash failed: {}", e),
                        )
                    })?;
                tables
                    .as_ref()
                    .map(|t| t.len())
                    .unwrap_or_else(|| rep.snapshot_hashes().len())
            };
            // The corrected hashes may disagree with what running SSPs
            // bootstrapped against — make them re-verify.
            let marked = state.ssp_pool.write().await.mark_all_for_resync();
            info!(mode, rehashed, marked, "Admin resync complete");
            Ok(serde_json::json!({
                "mode": "rehash",
                "recloned": false,
                "rehashed_tables": rehashed,
                "marked_for_resync": marked,
            }))
        }
        "reclone" => {
            match reclone_replica_from_upstream(
                &state.config,
                &state.replica,
                &state.seq_counter,
                &state.reclone_lock,
            )
            .await
            {
                Ok(true) => {
                    let marked = state.ssp_pool.write().await.mark_all_for_resync();
                    info!(mode, marked, "Admin resync complete");
                    Ok(serde_json::json!({
                        "mode": "reclone",
                        "recloned": true,
                        "rehashed_tables": 0,
                        "marked_for_resync": marked,
                    }))
                }
                Ok(false) => Err((
                    StatusCode::CONFLICT,
                    "A replica re-clone is already in progress".to_string(),
                )),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("reclone failed: {}", e),
                )),
            }
        }
        other => Err((
            StatusCode::BAD_REQUEST,
            format!("Unknown resync mode '{}' (expected \"reclone\" or \"rehash\")", other),
        )),
    }
}

/// Recompute the persisted hashes for `tables` **from replica content** and
/// return the fresh values. The replica is the authority; `snapshot_hashes` is
/// only a cache of it, advanced incrementally as batches drain. Any path that
/// mutates content without rehashing (or a one-off hash error) leaves that
/// cache lying, and every subsequent bootstrap fails against the lie.
///
/// Content-only read plus a metadata write — no rows change, so this is safe
/// to run while an SSP is mid-bootstrap. Takes `drain_lock` so it can't
/// interleave with a drain's apply/rehash pair.
async fn rehash_tables_from_content(
    state: &SspManagementState,
    tables: &BTreeSet<String>,
) -> Result<std::collections::BTreeMap<String, String>> {
    let _drain_guard = state.drain_lock.lock().await;
    let mut rep = state.replica.write().await;
    let seq = rep.snapshot_seq();
    rep.set_snapshot_state(seq, Some(tables)).await?;
    Ok(rep.snapshot_hashes().clone())
}

/// Second opinion for an SSP whose post-bootstrap integrity check failed.
///
/// The SSP bootstraps its rows from this scheduler's replica (via `/proxy`), so
/// "SSP hash != scheduler hash" does NOT imply the SSP is wrong — far more
/// often the scheduler's *cached* hash has drifted from its own replica
/// content. Rehash the disputed tables from content and compare again:
///
///   * agreement  → the cache was stale, now repaired; SSP proceeds to Ready.
///   * divergence → real. Escalate by consecutive-failure count, mirroring the
///     catch-up breaker: re-clone the replica once, then admit anyway rather
///     than leave the cluster with no ready SSP.
async fn handle_bootstrap_verify(
    State(state): State<SspManagementState>,
    Json(request): Json<ssp_protocol::SspBootstrapVerifyRequest>,
) -> Result<Json<ssp_protocol::SspBootstrapVerifyResponse>, (StatusCode, String)> {
    let ssp_id = request.ssp_id.clone();

    // Which tables do we actually disagree about? Compare the SSP's hashes
    // against the cached map; `diff_table_hashes` treats a table missing on
    // either side as the empty-table hash, so appearing/disappearing tables
    // are disputed too.
    let cached = { state.replica.read().await.snapshot_hashes().clone() };
    let disputed: BTreeSet<String> =
        ssp_protocol::snapshot_hash::diff_table_hashes(&cached, &request.table_hashes)
            .into_iter()
            .map(|d| d.table)
            .filter(|t| !ssp_protocol::SYNCED_META_TABLES.contains(&t.as_str()))
            .collect();

    if disputed.is_empty() {
        // Nothing to settle (the SSP retried against hashes that already
        // agree) — let it through.
        state.ssp_pool.write().await.reset_bootstrap_failures(&ssp_id);
        return Ok(Json(ssp_protocol::SspBootstrapVerifyResponse {
            table_hashes: cached,
            diverging: Vec::new(),
            admit: false,
            recloned: false,
        }));
    }

    let refreshed = rehash_tables_from_content(&state, &disputed)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("content rehash failed: {}", e),
            )
        })?;

    // Compare with the same semantics as `diff_table_hashes`: a table absent on
    // either side counts as the empty-table hash. The SSP creates no collection
    // for a zero-row table, so "absent on the SSP" must match "empty here".
    let empty = ssp_protocol::snapshot_hash::xor_empty_table_hash();
    let still: Vec<String> = disputed
        .iter()
        .filter(|t| {
            let ours = refreshed.get(*t).unwrap_or(&empty);
            let theirs = request.table_hashes.get(*t).unwrap_or(&empty);
            ours != theirs
        })
        .cloned()
        .collect();

    let repaired: Vec<&String> = disputed.iter().filter(|t| !still.contains(t)).collect();
    if !repaired.is_empty() {
        warn!(
            ssp_id = %ssp_id,
            tables = ?repaired,
            "Bootstrap hash cache was stale — repaired from replica content"
        );
    }

    if still.is_empty() {
        state.ssp_pool.write().await.reset_bootstrap_failures(&ssp_id);
        info!(
            ssp_id = %ssp_id,
            repaired = repaired.len(),
            "Bootstrap integrity settled against replica content — admitting SSP"
        );
        return Ok(Json(ssp_protocol::SspBootstrapVerifyResponse {
            table_hashes: refreshed,
            diverging: Vec::new(),
            admit: false,
            recloned: false,
        }));
    }

    // Real divergence: the SSP's circuit disagrees with content we just hashed.
    for table in &still {
        error!(
            ssp_id = %ssp_id,
            table = %table,
            scheduler = %refreshed.get(table).map(String::as_str).unwrap_or("<none>"),
            ssp = %request.table_hashes.get(table).map(String::as_str).unwrap_or("<none>"),
            "Bootstrap integrity mismatch persists against freshly hashed replica content"
        );
    }

    let fails = state.ssp_pool.write().await.record_bootstrap_failure(&ssp_id);
    let (reclone_after, admit_after) = bootstrap_breaker_thresholds();

    if fails >= admit_after {
        error!(
            ssp_id = %ssp_id,
            fails,
            tables = ?still,
            "ALERT: bootstrap integrity never converged after {} attempts — admitting SSP anyway to restore sync. Its circuit may be marginally divergent from the replica.",
            fails,
        );
        state.ssp_pool.write().await.reset_bootstrap_failures(&ssp_id);
        return Ok(Json(ssp_protocol::SspBootstrapVerifyResponse {
            table_hashes: refreshed,
            diverging: still,
            admit: true,
            recloned: false,
        }));
    }

    let mut recloned = false;
    if fails == reclone_after {
        warn!(
            ssp_id = %ssp_id,
            fails,
            "Bootstrap integrity failed {} times — re-cloning replica from upstream before the next attempt",
            fails,
        );
        match reclone_replica_from_upstream(
            &state.config,
            &state.replica,
            &state.seq_counter,
            &state.reclone_lock,
        )
        .await
        {
            Ok(true) => {
                recloned = true;
                state.ssp_pool.write().await.mark_all_for_resync();
                info!("Replica re-cloned from upstream after repeated bootstrap mismatches");
            }
            Ok(false) => {
                info!(ssp_id = %ssp_id, "Re-clone already in progress on another task")
            }
            Err(e) => {
                error!(ssp_id = %ssp_id, error = %e, "Replica re-clone failed; SSP will retry a plain bootstrap")
            }
        }
    }

    Ok(Json(ssp_protocol::SspBootstrapVerifyResponse {
        table_hashes: refreshed,
        diverging: still,
        admit: false,
        recloned,
    }))
}

/// Handle SSP registration — freezes snapshot, returns snapshot_seq, spawns poll task
async fn handle_register(
    State(state): State<SspManagementState>,
    Json(request): Json<SspRegistration>,
) -> Result<(StatusCode, Json<SspRegistrationResponse>), (StatusCode, String)> {
    info!("SSP registration: {} at {}", request.ssp_id, request.url);

    // Validate ssp_id (non-empty)
    if request.ssp_id.trim().is_empty() {
        error!("Invalid SSP ID: empty");
        return Err((StatusCode::BAD_REQUEST, "SSP ID cannot be empty".to_string()));
    }

    // Validate URL (basic check for http/https)
    if !request.url.starts_with("http://") && !request.url.starts_with("https://") {
        error!("Invalid SSP URL: {}", request.url);
        return Err((
            StatusCode::BAD_REQUEST,
            "SSP URL must start with http:// or https://".to_string(),
        ));
    }

    // Check scheduler is not Cloning or Restoring
    {
        let scheduler_status = *state.status.read().await;
        match scheduler_status {
            SchedulerStatus::Cloning => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Scheduler is still cloning database".to_string(),
                ));
            }
            SchedulerStatus::Restoring => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Scheduler is restoring from backup".to_string(),
                ));
            }
            _ => {}
        }
    }

    // Create SspInfo
    let ssp_info = SspInfo {
        id: request.ssp_id.clone(),
        url: request.url.clone(),
        version: request.version.clone(),
        connected_at: std::time::Instant::now(),
        last_heartbeat: std::time::Instant::now(),
        query_count: 0,
        views: 0,
        cpu_usage: None,
        memory_usage: None,
        env: request.env.clone(),
        bootstrap: None,
    };

    // Under `drain_lock`: wait out any in-flight drain, freeze the snapshot,
    // opportunistically apply the pending backlog so the SSP gets CURRENT
    // hashes (a stale persisted snapshot is exactly what sends SSPs into the
    // integrity-mismatch exit(2) loop), then capture seq + hashes and insert
    // into the pool. Freezing AND inserting inside the lock is what makes
    // every drainer's under-lock `has_active_bootstrap` check sound; it also
    // serializes two simultaneous registrations — the second sees the first
    // in the pool and skips its drain.
    //
    // Latency: registration runs under the SSP's 10s client timeout with
    // retry/backoff. A backlog, or a from-content rehash of a dirty table,
    // may push the first attempt past that timeout. The critical section
    // therefore runs in its own task: when the client times out, hyper drops
    // the handler future, and an inline drain that died mid-way had already
    // taken its events out of the buffer (lost until the next boot's WAL
    // recovery) and left every retry restarting the rehash from scratch —
    // the SSP never registered until its budget ran out. The task holds
    // `drain_lock` for the whole section, so a retry queues behind it and
    // registers against the drained state.
    let critical = {
        let state = state.clone();
        let ssp_info = ssp_info.clone();
        let ssp_id = request.ssp_id.clone();
        tokio::spawn(async move {
            let _drain_guard =
                crate::acquire_drain_lock(&state.drain_lock, "ssp registration").await;
            // Dropped with the guard: reports a registration that held the
            // lock long enough to have delayed the snapshot updater.
            struct HoldReport(std::time::Instant);
            impl Drop for HoldReport {
                fn drop(&mut self) {
                    let held_secs = self.0.elapsed().as_secs();
                    if held_secs >= 60 {
                        tracing::warn!(held_secs, "ssp registration held drain_lock for a long time");
                    }
                }
            }
            let _hold_report = HoldReport(std::time::Instant::now());

            *state.status.write().await = SchedulerStatus::SnapshotFrozen;

            // Exclude ourselves: a re-registering SSP is still parked in
            // `Bootstrapping` from its previous attempt, and treating that as
            // a sibling skipped the drain and handed back the same hashes it
            // just failed against — making the SSP's single integrity retry
            // pointless.
            let sibling_active = state
                .ssp_pool
                .read()
                .await
                .has_active_bootstrap_excluding(&ssp_id);
            if sibling_active {
                // A sibling bootstrap holds hashes captured earlier; mutating
                // the replica now would invalidate them mid-flight. This SSP
                // still gets a self-consistent (if slightly stale) snapshot —
                // nothing else can drain while the status stays frozen.
                info!("Skipping pre-registration drain: sibling SSP bootstrap in flight");
            } else {
                match crate::drain_and_apply(&state.event_buffer, &state.replica, &state.wal)
                    .await
                {
                    Ok(0) => {}
                    Ok(applied) => {
                        info!(applied, "Drained pending events before handing bootstrap hashes")
                    }
                    Err(e) => {
                        warn!(error = %e, "Pre-registration drain failed; handing out persisted hashes")
                    }
                }
            }

            let (snapshot_seq, table_hashes) = {
                let replica = state.replica.read().await;
                (replica.snapshot_seq(), replica.snapshot_hashes().clone())
            };

            // Add to pool, mark as bootstrapping, record snapshot_seq, and
            // bump the registration generation so any poll task from a
            // previous registration of this ssp_id knows it has been
            // superseded.
            let mut pool = state.ssp_pool.write().await;
            pool.upsert(ssp_info);
            pool.mark_bootstrapping(&ssp_id);
            pool.set_bootstrap_seq(&ssp_id, snapshot_seq);
            let generation = pool.bump_registration_gen(&ssp_id);

            (snapshot_seq, table_hashes, generation)
        })
        .await
    };
    let (snapshot_seq, table_hashes, generation) = match critical {
        Ok(v) => v,
        Err(e) => {
            error!(error = %e, "Registration critical section panicked");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "registration failed; retry".to_string(),
            ));
        }
    };
    info!(snapshot_seq, "Snapshot frozen for SSP bootstrap");

    // Spawn polling + replay task
    let ssp_id = request.ssp_id.clone();
    let ssp_url = request.url.clone();
    let ssp_pool = state.ssp_pool.clone();
    let transport = state.transport.clone();
    let event_buffer = state.event_buffer.clone();
    let scheduler_status = state.status.clone();
    let status_for_err = state.status.clone();
    let drain_lock_for_err = state.drain_lock.clone();
    let config = state.config.clone();
    let seq_counter = state.seq_counter.clone();
    let reclone_lock = state.reclone_lock.clone();

    let replica = state.replica.clone();
    tokio::spawn(async move {
        if let Err(e) = poll_and_replay_ssp(
            ssp_id.clone(),
            ssp_url,
            snapshot_seq,
            generation,
            ssp_pool.clone(),
            transport,
            event_buffer,
            scheduler_status,
            config,
            replica,
            seq_counter,
            reclone_lock,
        )
        .await
        {
            error!("Bootstrap/replay failed for SSP '{}': {}", ssp_id, e);

            // Under `drain_lock` so the cleanup can't interleave with a
            // registration's freeze+insert critical section (which could
            // otherwise get its fresh freeze clobbered by the unfreeze below).
            let _drain_guard = drain_lock_for_err.lock().await;
            let mut pool = ssp_pool.write().await;

            // Only clean up if this registration is still the current one —
            // a superseding re-register owns the pool entry (and the freeze)
            // now, and removing it would tear down a live bootstrap.
            if pool.registration_gen(&ssp_id) != generation {
                info!(
                    ssp_id = %ssp_id,
                    "Skipping cleanup: SSP re-registered since this bootstrap started"
                );
                return;
            }
            pool.remove(&ssp_id);

            // Unfreeze the snapshot if no other bootstrap is active. Without
            // this, SnapshotFrozen latches forever: the periodic updater
            // refuses to run on a non-Ready status, the drain never happens,
            // and pending_events pins until a volume wipe.
            if !pool.has_active_bootstrap() {
                drop(pool);
                let mut status = status_for_err.write().await;
                if *status == SchedulerStatus::SnapshotFrozen {
                    *status = SchedulerStatus::Ready;
                    info!(ssp_id = %ssp_id, "Snapshot unfrozen after bootstrap failure");
                }
            }
        }
    });

    info!(
        tables = table_hashes.len(),
        "SSP registration accepted, polling for bootstrap completion"
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(SspRegistrationResponse {
            snapshot_seq,
            table_hashes,
        }),
    ))
}

/// Handle SSP heartbeat
async fn handle_heartbeat(
    State(state): State<SspManagementState>,
    Json(heartbeat): Json<SspHeartbeat>,
) -> Result<StatusCode, (StatusCode, String)> {
    // Reject heartbeats during restore so SSPs back off instead of spamming
    if *state.status.read().await == SchedulerStatus::Restoring {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Scheduler is restoring from backup".to_string(),
        ));
    }

    // Check if SSP exists in pool
    let ssp_exists = {
        let pool = state.ssp_pool.read().await;
        pool.get(&heartbeat.ssp_id).is_some()
    };

    if !ssp_exists {
        warn!("Heartbeat from unregistered SSP: {}", heartbeat.ssp_id);
        return Err((
            StatusCode::NOT_FOUND,
            "SSP not registered. Please re-register.".to_string(),
        ));
    }

    // Update SSP with heartbeat data, then check both forced-resync and
    // overflow flags under a single write lock so we can clear the resync
    // flag atomically with returning 409.
    let resync_requested;
    let has_overflow;
    {
        let mut pool = state.ssp_pool.write().await;
        pool.update_ssp(
            &heartbeat.ssp_id,
            heartbeat.views,
            heartbeat.cpu_usage,
            heartbeat.memory_usage,
            heartbeat.version.clone(),
        );
        resync_requested = pool.take_resync(&heartbeat.ssp_id);
        has_overflow = pool.has_buffer_overflow(&heartbeat.ssp_id);
    }

    // The 409 body is a `ResyncDirective` rather than prose: it is the one
    // message that reaches an SSP on its way out, and "drop your snapshot
    // first" has to ride on it. Old SSPs log it as text and restart anyway.
    if let Some(kind) = resync_requested {
        warn!(ssp_id = %heartbeat.ssp_id, ?kind, "Forced resync requested");
        let directive = ssp_protocol::ResyncDirective {
            reason: match kind {
                crate::router::ResyncKind::Resync => {
                    "Resync requested. SSP must re-bootstrap.".to_string()
                }
                crate::router::ResyncKind::Clean => {
                    "Clean restart requested. SSP must drop its snapshot and re-bootstrap."
                        .to_string()
                }
            },
            clean: kind == crate::router::ResyncKind::Clean,
        };
        return Err((StatusCode::CONFLICT, directive_body(&directive)));
    }

    if has_overflow {
        error!("Buffer overflow detected for SSP: {}", heartbeat.ssp_id);
        let directive = ssp_protocol::ResyncDirective {
            reason: "Buffer overflow detected. SSP needs to re-bootstrap.".to_string(),
            clean: false,
        };
        return Err((StatusCode::CONFLICT, directive_body(&directive)));
    }

    Ok(StatusCode::OK)
}

/// Poll SSP health until ready, then replay missed events.
///
/// `generation` is the registration generation captured when this task was
/// spawned; it is re-checked at phase boundaries so a task orphaned by a
/// re-registration of the same `ssp_id` bails out instead of mutating (or,
/// via its error handler, removing) the newer registration's pool state.
#[allow(clippy::too_many_arguments)]
async fn poll_and_replay_ssp(
    ssp_id: String,
    ssp_url: String,
    snapshot_seq: u64,
    generation: u64,
    ssp_pool: Arc<RwLock<SspPool>>,
    transport: Arc<HttpTransport>,
    event_buffer: Arc<RwLock<VecDeque<BufferedEvent>>>,
    scheduler_status: Arc<RwLock<SchedulerStatus>>,
    config: Arc<SchedulerConfig>,
    replica: Arc<RwLock<Replica>>,
    seq_counter: Arc<AtomicU64>,
    reclone_lock: Arc<Mutex<()>>,
) -> Result<()> {
    let poll_interval = std::time::Duration::from_millis(config.ssp_poll_interval_ms);
    let timeout = std::time::Duration::from_secs(config.bootstrap_timeout_secs);
    let start = std::time::Instant::now();

    info!(
        ssp_id = %ssp_id,
        snapshot_seq,
        "Polling SSP health, waiting for bootstrap completion"
    );

    // Phase 1: Poll SSP health until it reports Ready
    loop {
        if start.elapsed() > timeout {
            anyhow::bail!(
                "Bootstrap timeout ({:?}) exceeded for SSP '{}'",
                timeout,
                ssp_id
            );
        }

        // If the heartbeat-driven stale-SSP sweep has already evicted us
        // from the pool (e.g. the SSP container died mid-bootstrap), stop
        // polling — otherwise we burn the full `bootstrap_timeout_secs`
        // hammering an unreachable URL.
        {
            let pool = ssp_pool.read().await;
            if pool.get(&ssp_id).is_none() {
                anyhow::bail!(
                    "SSP '{}' removed from pool during bootstrap — aborting poll",
                    ssp_id
                );
            }
            if pool.registration_gen(&ssp_id) != generation {
                anyhow::bail!(
                    "SSP '{}' bootstrap superseded by re-registration — aborting poll",
                    ssp_id
                );
            }
        }

        tokio::time::sleep(poll_interval).await;

        match transport.check_ssp_health_status(&ssp_url).await {
            Some(status) if status == "ready" => {
                info!("SSP '{}' reports ready, starting event replay", ssp_id);
                break;
            }
            Some(status) if status == "failed" => {
                anyhow::bail!("SSP '{}' reported bootstrap failure", ssp_id);
            }
            Some(status) => {
                debug!("SSP '{}' status: {}", ssp_id, status);
            }
            None => {
                warn!("Cannot reach SSP '{}', retrying...", ssp_id);
            }
        }
    }

    // Phase 2: Mark SSP as Replaying (ingest will buffer per-SSP during replay)
    {
        let mut pool = ssp_pool.write().await;
        if pool.registration_gen(&ssp_id) != generation {
            anyhow::bail!(
                "SSP '{}' bootstrap superseded by re-registration — aborting before replay",
                ssp_id
            );
        }
        pool.mark_replaying(&ssp_id);
    }

    // Phase 3: Collect and replay events from global buffer
    let events_to_replay: Vec<BufferedEvent> = {
        let buffer = event_buffer.read().await;
        buffer
            .iter()
            .filter(|e| e.seq > snapshot_seq)
            .cloned()
            .collect()
    };

    // Accumulate every event we replay to this SSP, in seq order, so the
    // catch-up check (Phase 4b) can reconstruct the SSP's content at cut M.
    let mut replayed: Vec<RecordUpdate> =
        events_to_replay.iter().map(|e| e.update.clone()).collect();

    if !events_to_replay.is_empty() {
        info!(
            "Replaying {} events to SSP '{}' (seq > {})",
            events_to_replay.len(),
            ssp_id,
            snapshot_seq
        );

        for event in &events_to_replay {
            let ingest_payload = serde_json::json!({
                "table": event.update.table,
                "op": event.update.operation.to_string(),
                "id": event.update.record_id,
                "record": event.update.data.clone().unwrap_or(serde_json::json!({}))
            });

            if let Err(e) = transport.post_to_ssp(&ssp_url, "/ingest", &ingest_payload).await {
                warn!(
                    "Failed to replay event seq={} to SSP '{}': {}",
                    event.seq, ssp_id, e
                );
            }
        }

        info!(
            "Replayed {} global buffer events to SSP '{}'",
            events_to_replay.len(),
            ssp_id
        );
    }

    // Phase 4: Drain and replay per-SSP buffered events (accumulated during replay)
    loop {
        let buffered = {
            let mut pool = ssp_pool.write().await;
            pool.drain_buffer(&ssp_id)
        };

        if buffered.is_empty() {
            break;
        }

        replayed.extend(buffered.iter().cloned());

        info!(
            "Replaying {} per-SSP buffered events to SSP '{}'",
            buffered.len(),
            ssp_id
        );

        for message in &buffered {
            let ingest_payload = serde_json::json!({
                "table": message.table,
                "op": message.operation.to_string(),
                "id": message.record_id,
                "record": message.data.clone().unwrap_or(serde_json::json!({}))
            });

            if let Err(e) = transport.post_to_ssp(&ssp_url, "/ingest", &ingest_payload).await {
                warn!(
                    "Failed to replay buffered event to SSP '{}': {}",
                    ssp_id, e
                );
            }
        }
    }

    // Phase 4b: Verify the SSP's caught-up state at the cut M BEFORE routing any
    // live traffic to it. The SSP is still `Replaying`, so ingest keeps buffering
    // for it (it can't be selected as a broadcast target) — it stays pinned at M
    // while we reconstruct and compare. This replaces the old post-replay check,
    // which compared the SSP's live (seq M) hashes against the frozen snapshot
    // (seq N) and so falsely flagged any table written during catch-up.
    let verified = match verify_catchup_at_m(&ssp_id, &ssp_url, &transport, &replica, &replayed).await {
        Ok(true) => {
            // Passed — clear the consecutive-failure streak.
            ssp_pool.write().await.reset_catchup_failures(&ssp_id);
            true
        }
        Ok(false) => {
            // Real divergence. A plain re-bootstrap can't fix a *deterministic*
            // scheduler-vs-circuit gap — the SSP refetches the same diverging
            // state every cycle — so escalate by consecutive-failure count
            // instead of looping forever (which leaves the whole cluster with
            // NO ready SSP and all live queries failing).
            let fails = ssp_pool.write().await.record_catchup_failure(&ssp_id);
            let (reclone_after, admit_after) = catchup_breaker_thresholds();

            if fails >= admit_after {
                // Give up on the gate: admit the SSP so sync is restored. The
                // circuit passed its own @N bootstrap self-check and every
                // shared row is (almost certainly) semantically correct; a
                // never-converging hash is not worth an indefinite outage. The
                // row-level diagnostic above shows exactly what disagreed.
                error!(
                    ssp_id = %ssp_id,
                    fails,
                    "ALERT: catch-up never converged after {} attempts — admitting SSP to broadcast anyway to restore sync. Circuit may be marginally divergent from the scheduler projection (see the row-level diff above).",
                    fails,
                );
                ssp_pool.write().await.reset_catchup_failures(&ssp_id);
                true
            } else {
                if fails == reclone_after {
                    // The replica itself may hold the odd representation. Do ONE
                    // full re-clone from upstream before the next attempt so the
                    // SSP re-bootstraps from a freshly-cloned snapshot.
                    warn!(
                        ssp_id = %ssp_id,
                        fails,
                        "Catch-up failed {} times — re-cloning replica from upstream before the next attempt",
                        fails,
                    );
                    match reclone_replica_from_upstream(&config, &replica, &seq_counter, &reclone_lock).await {
                        Ok(true) => {
                            info!("Replica re-cloned from upstream; flagging all SSPs to re-bootstrap from the fresh snapshot");
                            ssp_pool.write().await.mark_all_for_resync();
                        }
                        Ok(false) => {
                            info!(ssp_id = %ssp_id, "Re-clone already in progress on another task; just re-bootstrapping this SSP");
                            ssp_pool.write().await.mark_for_resync(&ssp_id);
                        }
                        Err(e) => {
                            error!(ssp_id = %ssp_id, error = %e, "Replica re-clone failed; falling back to a plain re-bootstrap");
                            ssp_pool.write().await.mark_for_resync(&ssp_id);
                        }
                    }
                } else {
                    ssp_pool.write().await.mark_for_resync(&ssp_id);
                }
                warn!(ssp_id = %ssp_id, fails, "Catch-up verification failed — withholding from broadcast; SSP will re-bootstrap");
                false
            }
        }
        Err(e) => {
            // Transient failure (e.g. /info unreachable). Favor availability —
            // the SSP already passed its own @N bootstrap self-check.
            warn!(ssp_id = %ssp_id, error = %e, "Catch-up verification skipped (transient); proceeding to ready");
            true
        }
    };

    // Phase 5: Mark SSP as Ready (atomic with final buffer drain) — only once
    // verification has passed, so live traffic is never routed to an unverified
    // or diverged SSP.
    if verified {
        let mut pool = ssp_pool.write().await;
        if pool.registration_gen(&ssp_id) != generation {
            anyhow::bail!(
                "SSP '{}' bootstrap superseded by re-registration — aborting before ready",
                ssp_id
            );
        }
        let remaining = pool.mark_ready(&ssp_id);

        // Replay any events that snuck in between last drain and mark_ready
        if !remaining.is_empty() {
            info!(
                "Replaying {} final buffered events to SSP '{}'",
                remaining.len(),
                ssp_id
            );
            for message in &remaining {
                let ingest_payload = serde_json::json!({
                    "table": message.table,
                    "op": message.operation.to_string(),
                    "id": message.record_id,
                    "record": message.data.clone().unwrap_or(serde_json::json!({}))
                });

                // Drop pool lock before making HTTP call
                drop(pool);

                let outcome = transport
                    .post_to_ssp(&ssp_url, "/ingest", &ingest_payload)
                    .await;
                pool = ssp_pool.write().await;
                anyhow::ensure!(
                    pool.registration_gen(&ssp_id) == generation,
                    "SSP '{}' bootstrap superseded by re-registration during final replay",
                    ssp_id
                );
                if let Err(e) = outcome {
                    warn!(
                        "Failed to replay final event to SSP '{}': {}",
                        ssp_id, e
                    );
                    pool.mark_for_resync(&ssp_id);
                    anyhow::bail!("Final replay failed for SSP '{}': {}", ssp_id, e);
                }
            }
        }
        // Keep recovery correlated with this registration while holding the pool lock.
        crate::admin::incidents::emit(&ssp_id, "ready", "recovered", "Bootstrap verification and final replay completed", None);
    }

    // Phase 6: Unfreeze snapshot if no other SSPs are bootstrapping/replaying
    {
        let has_active = {
            let pool = ssp_pool.read().await;
            pool.has_active_bootstrap()
        };

        if !has_active {
            let mut status = scheduler_status.write().await;
            if *status == SchedulerStatus::SnapshotFrozen {
                *status = SchedulerStatus::Ready;
                info!("Snapshot unfrozen: all SSPs caught up");
            }
        }
    }

    if verified {
        info!("SSP '{}' is now fully caught up and ready", ssp_id);
    } else {
        info!(ssp_id = %ssp_id, "SSP withheld from broadcast pending re-bootstrap");
    }
    Ok(())
}

/// Verify a caught-up SSP at the catch-up cut M, comparing hashes taken at the
/// SAME sequence point on both sides (the bug this replaces compared the SSP's
/// live state@M against the scheduler's frozen snapshot@N).
///
/// Only tables *touched* by the replayed events `(N, M]` need checking —
/// untouched tables were already verified at N by the SSP's bootstrap
/// self-check and did not change. For each touched base table the scheduler
/// reconstructs content@M in memory — rows@N from the frozen replica, then the
/// replayed events applied by REPLACE/remove (matching the SSP circuit's
/// `rows.insert`, never the replica's own MERGE) — and XOR-hashes it the same
/// way the SSP maintains its `catchup_hashes`. No replica mutation, so
/// concurrent bootstraps are unaffected.
///
/// Returns `Ok(true)` on agreement (or nothing to check), `Ok(false)` on a real
/// divergence (caller flags re-bootstrap), `Err` on a transient failure (caller
/// proceeds — the SSP already passed its own @N self-check).
async fn verify_catchup_at_m(
    ssp_id: &str,
    ssp_url: &str,
    transport: &Arc<HttpTransport>,
    replica: &Arc<RwLock<Replica>>,
    replayed: &[RecordUpdate],
) -> Result<bool> {
    use serde_json::Value;
    use std::collections::BTreeMap;

    // Group replayed events per touched base table, preserving seq order. Skip
    // `_00_*` system tables (scheduler/view bookkeeping, not replicated content).
    let mut per_table: BTreeMap<String, Vec<&RecordUpdate>> = BTreeMap::new();
    for ev in replayed {
        // Same carve-out as the drain's touched set: the synced meta tables are
        // real replicated content and must be verified like any other table.
        if ssp_protocol::table_excluded_from_sync(&ev.table) {
            continue;
        }
        per_table.entry(ev.table.clone()).or_default().push(ev);
    }
    if per_table.is_empty() {
        // Nothing changed in (N, M]; the @N self-check already covered it.
        return Ok(true);
    }

    // Reconstruct reference@M per touched table (rows@N + replayed events). Keep
    // the projected rows (not just the hash) so a persistent mismatch can dump
    // the diverging row's canonical JSON for diagnosis.
    let mut reference_rows: BTreeMap<String, std::collections::HashMap<String, Value>> =
        BTreeMap::new();
    for (table, events) in &per_table {
        let seed = {
            let rep = replica.read().await;
            rep.snapshot_rows(table)
                .await
                .with_context(|| format!("snapshot_rows for '{}'", table))?
        };
        reference_rows.insert(table.clone(), project_table_rows(table, seed, events));
    }
    let reference: BTreeMap<String, String> = reference_rows
        .iter()
        .map(|(table, rows)| {
            (
                table.clone(),
                ssp_protocol::snapshot_hash::xor_table_hash(rows.clone().into_iter()),
            )
        })
        .collect();

    let empty = ssp_protocol::snapshot_hash::xor_acc_to_hex(&ssp_protocol::snapshot_hash::xor_empty());

    // Compare against the SSP's catch-up hashes with a bounded retry. The SSP is
    // pinned at M, but its `/ingest` handler may apply the last replayed events
    // slightly behind the HTTP ack, so a first read can briefly lag. A genuine
    // divergence stays mismatched across every attempt; a settle lag clears. We
    // never relax the check itself — only give the SSP a moment to converge
    // before forcing a (costly) full re-bootstrap.
    let mut mismatches: Vec<(String, String, String)> = Vec::new();
    for attempt in 1..=CATCHUP_VERIFY_ATTEMPTS {
        // Fetch the SSP's catch-up hashes (it's still `Replaying`, pinned at M).
        let info_resp = transport
            .get_from_ssp(ssp_url, "/info")
            .await
            .map_err(|e| anyhow::anyhow!("GET /info failed: {}", e))?;
        if !info_resp.status().is_success() {
            anyhow::bail!("SSP /info returned HTTP {}", info_resp.status());
        }
        let info_json: Value = info_resp
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Parse /info JSON failed: {}", e))?;
        let ssp_hashes: BTreeMap<String, String> = info_json
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|entry| entry.get("catchup_hashes"))
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        if ssp_hashes.is_empty() {
            // Older SSP build without `catchup_hashes` (deploy SSP before scheduler).
            warn!(ssp_id = %ssp_id, "SSP /info has no catchup_hashes (old build?), skipping catch-up check");
            return Ok(true);
        }

        mismatches = reference
            .iter()
            .filter_map(|(table, ref_hash)| {
                let ssp_hash = ssp_hashes.get(table).cloned().unwrap_or_else(|| empty.clone());
                if *ref_hash != ssp_hash {
                    Some((table.clone(), ref_hash.clone(), ssp_hash))
                } else {
                    None
                }
            })
            .collect();

        if mismatches.is_empty() {
            info!(ssp_id = %ssp_id, tables = reference.len(), attempt, "Catch-up verification passed");
            return Ok(true);
        }

        if attempt < CATCHUP_VERIFY_ATTEMPTS {
            warn!(
                ssp_id = %ssp_id,
                attempt,
                mismatches = mismatches.len(),
                "Catch-up mismatch; letting the SSP settle and re-checking"
            );
            tokio::time::sleep(std::time::Duration::from_millis(CATCHUP_VERIFY_RETRY_DELAY_MS)).await;
        }
    }

    // Persistent mismatch after every attempt → a real divergence. Pull the
    // SSP's actual circuit rows and diff them against the scheduler's projection
    // row-by-row, so the log names the specific missing / extra / differing rows
    // instead of the old one-sided dump (which showed the scheduler's first N
    // rows in map order — not necessarily the diverging ones).
    for (table, sched, ssp_h) in &mismatches {
        error!(
            ssp_id = %ssp_id,
            table = %table,
            scheduler = %sched,
            ssp = %ssp_h,
            "Catch-up hash mismatch"
        );
        let ref_rows = match reference_rows.get(table) {
            Some(r) => r,
            None => continue,
        };
        match fetch_ssp_catchup_rows(transport, ssp_url, table).await {
            Ok((ssp_rows, horizon)) => {
                if let Some(h) = &horizon {
                    warn!(
                        ssp_id = %ssp_id, table = %table, max_id = %h,
                        "SSP row dump was truncated — diffing only up to this id; rows above it are unknown, not missing"
                    );
                }
                let all_ids: std::collections::BTreeSet<&String> =
                    ref_rows.keys().chain(ssp_rows.keys()).collect();
                let mut logged = 0usize;
                for id in all_ids {
                    if logged >= CATCHUP_DIAG_MAX_ROWS {
                        warn!(ssp_id = %ssp_id, table = %table, "More diverging rows omitted (diagnostic cap reached)");
                        break;
                    }
                    // Past the SSP's truncation point we simply have no data.
                    // Falling through would report every such row as "missing
                    // on SSP" and send an operator hunting a divergence that
                    // does not exist.
                    if horizon.as_ref().is_some_and(|h| id.as_str() > h.as_str()) {
                        continue;
                    }
                    match (ref_rows.get(id), ssp_rows.get(id)) {
                        (Some(r), None) => {
                            error!(ssp_id = %ssp_id, table = %table, row_id = %id, scheduler_row = %canon_json(r), "Catch-up diff: row on scheduler, MISSING on SSP");
                            logged += 1;
                        }
                        (None, Some(s)) => {
                            error!(ssp_id = %ssp_id, table = %table, row_id = %id, ssp_row = %canon_json(s), "Catch-up diff: EXTRA row on SSP, absent on scheduler");
                            logged += 1;
                        }
                        (Some(r), Some(s)) => {
                            if ssp_protocol::snapshot_hash::record_digest(id.as_str(), r)
                                != ssp_protocol::snapshot_hash::record_digest(id.as_str(), s)
                            {
                                error!(ssp_id = %ssp_id, table = %table, row_id = %id, scheduler_row = %canon_json(r), ssp_row = %canon_json(s), "Catch-up diff: row content differs");
                                logged += 1;
                            }
                        }
                        (None, None) => {}
                    }
                }
                if logged == 0 {
                    error!(ssp_id = %ssp_id, table = %table, "Catch-up hashes differ but no row-level diff found — possible canonicalization gap between scheduler and circuit");
                }
            }
            Err(e) => {
                // Couldn't reach the SSP's row dump (old build / transient) —
                // fall back to the one-sided scheduler-projection dump.
                warn!(ssp_id = %ssp_id, table = %table, error = %e, "Could not fetch SSP rows for row-level diff; dumping scheduler projection only");
                for (row_id, val) in ref_rows.iter().take(CATCHUP_DIAG_MAX_ROWS) {
                    error!(
                        ssp_id = %ssp_id,
                        table = %table,
                        row_id = %row_id,
                        reference_row = %canon_json(val),
                        "Catch-up mismatch diagnostic (scheduler reconstructed row@M)"
                    );
                }
            }
        }
    }
    error!(
        ssp_id = %ssp_id,
        mismatches = mismatches.len(),
        "SSP catch-up state disagrees with scheduler projection — flagging for re-bootstrap"
    );
    Ok(false)
}

/// Catch-up verification re-checks the SSP this many times before treating a
/// mismatch as a real divergence (the SSP's apply pipeline can briefly lag the
/// `/ingest` HTTP ack while pinned at M).
const CATCHUP_VERIFY_ATTEMPTS: usize = 3;
/// Delay between catch-up re-checks.
const CATCHUP_VERIFY_RETRY_DELAY_MS: u64 = 150;
/// Cap on per-table rows dumped in the mismatch diagnostic, to keep the log
/// bounded. Higher than the old one-sided dump because the row-level diff only
/// logs rows that actually diverge (missing / extra / differing), so the cap is
/// a real ceiling on divergences shown, not just the first N rows.
const CATCHUP_DIAG_MAX_ROWS: usize = 20;

/// Default consecutive catch-up failures before the breaker re-clones the
/// replica from upstream. Override with `SPKY_CATCHUP_RECLONE_AFTER`.
const CATCHUP_RECLONE_AFTER_DEFAULT: u32 = 3;
/// Default consecutive catch-up failures before the breaker gives up on the
/// gate and admits the SSP to broadcast anyway (restoring sync). Override with
/// `SPKY_CATCHUP_ADMIT_AFTER`. Set very high to effectively disable admit; set
/// re-clone/admit equal to skip the re-clone step.
const CATCHUP_ADMIT_AFTER_DEFAULT: u32 = 5;

/// Read the catch-up breaker thresholds `(reclone_after, admit_after)` from env,
/// falling back to the compiled defaults. `admit_after` is floored to at least
/// `reclone_after` so admit never precedes the re-clone attempt.
fn catchup_breaker_thresholds() -> (u32, u32) {
    resolve_breaker_thresholds(
        breaker_env("SPKY_CATCHUP_RECLONE_AFTER"),
        breaker_env("SPKY_CATCHUP_ADMIT_AFTER"),
    )
}

/// Same escalation for the *bootstrap* integrity gate. Separate env knobs so a
/// deployment can tune the two gates independently; same defaults.
fn bootstrap_breaker_thresholds() -> (u32, u32) {
    resolve_breaker_thresholds(
        breaker_env("SPKY_BOOTSTRAP_RECLONE_AFTER"),
        breaker_env("SPKY_BOOTSTRAP_ADMIT_AFTER"),
    )
}

/// Read a positive breaker threshold from the environment; `None` when unset,
/// unparseable, or zero (so a typo can't silently disable a gate).
fn breaker_env(key: &str) -> Option<u32> {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n: &u32| *n > 0)
}

/// Pure core of [`catchup_breaker_thresholds`]: apply defaults and floor
/// `admit` to at least `reclone` so admit never precedes the re-clone attempt.
fn resolve_breaker_thresholds(reclone: Option<u32>, admit: Option<u32>) -> (u32, u32) {
    let reclone = reclone.unwrap_or(CATCHUP_RECLONE_AFTER_DEFAULT);
    let admit = admit.unwrap_or(CATCHUP_ADMIT_AFTER_DEFAULT).max(reclone);
    (reclone, admit)
}

/// Canonical-JSON string of a value, for the mismatch diagnostic. Same encoding
/// the hash uses, so the logged bytes are exactly what feeds the digest.
fn canon_json(v: &serde_json::Value) -> String {
    String::from_utf8_lossy(&ssp_protocol::snapshot_hash::canonical_json(v)).into_owned()
}

/// Fetch an SSP's circuit rows for one table (`/debug/catchup-rows/:table`),
/// returned as `raw_id -> value` alongside the id ceiling the answer is
/// authoritative up to.
///
/// The SSP caps the dump (an unbounded one costs roughly twice its store, at
/// the worst possible moment) and returns a sorted prefix. `Some(max_id)`
/// means "ids above this were not examined"; `None` means the whole table came
/// back. Used by the catch-up diagnostic to diff the circuit against the
/// scheduler's reconstructed projection.
async fn fetch_ssp_catchup_rows(
    transport: &Arc<HttpTransport>,
    ssp_url: &str,
    table: &str,
) -> Result<(
    std::collections::HashMap<String, serde_json::Value>,
    Option<String>,
)> {
    let resp = transport
        .get_from_ssp(ssp_url, &format!("/debug/catchup-rows/{}", table))
        .await
        .map_err(|e| anyhow::anyhow!("GET /debug/catchup-rows failed: {}", e))?;
    if !resp.status().is_success() {
        anyhow::bail!("SSP /debug/catchup-rows returned HTTP {}", resp.status());
    }
    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow::anyhow!("Parse /debug/catchup-rows JSON failed: {}", e))?;
    let rows = json
        .get("rows")
        .and_then(|v| v.as_object())
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    // Only a truncated dump has a horizon; a complete one is authoritative
    // everywhere. An older SSP omits both fields, which reads as complete —
    // matching its actual behaviour.
    let horizon = json
        .get("truncated")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        .then(|| {
            json.get("max_id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        })
        .flatten();
    Ok((rows, horizon))
}

/// Catch-up breaker step: re-clone the replica from upstream SurrealDB. Serialized
/// by `reclone_lock` so two looping SSPs can't reset + re-ingest the shared
/// replica at once. Returns `Ok(true)` if this call performed the re-clone,
/// `Ok(false)` if another task already holds the lock (caller falls back to a
/// plain re-bootstrap), `Err` on a clone failure.
///
/// Two-phase, mirroring the bootstrap clone in `Scheduler::start` but without
/// its worst property: phase 1 pages every table from upstream into a disk
/// spool while holding NO replica lock (the network clone is the slow part —
/// the old version held the write lock across it, starving /proxy and every
/// probe for minutes); phase 2 takes the write lock only for reset + local
/// bulk load + rehash, so in-flight bootstrap reads still see a consistent
/// snapshot (they block, then read the fresh clone).
pub(crate) async fn reclone_replica_from_upstream(
    config: &SchedulerConfig,
    replica: &Arc<RwLock<Replica>>,
    seq_counter: &Arc<AtomicU64>,
    reclone_lock: &Arc<Mutex<()>>,
) -> Result<bool> {
    let _guard = match reclone_lock.try_lock() {
        Ok(g) => g,
        Err(_) => return Ok(false),
    };

    let db = crate::restore::connect_remote(&config.db)
        .await
        .context("reclone: connect to upstream SurrealDB failed")?;

    // Snapshot the seq the clone reflects BEFORE the (slow) fetch, matching
    // the bootstrap path — new events past this point re-arrive via replay.
    let seq = seq_counter.load(Ordering::SeqCst);

    // Phase 1: network → disk, no lock. Spool next to the replica so the
    // local load stays on the same filesystem.
    let spool_parent = config
        .replica_db_path
        .parent()
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    let manifest = Replica::fetch_all_to_spool(&db, &spool_parent)
        .await
        .context("reclone: spool fetch from upstream failed")?;

    // Phase 2: disk → replica, short(er) write lock. Reset+load must be
    // atomic vs readers; a failure here leaves a reset replica, which is the
    // same failure mode the single-phase version had.
    {
        let mut rep = replica.write().await;
        rep.reset().await.context("reclone: replica reset failed")?;
        rep.load_from_spool(&manifest)
            .await
            .context("reclone: spool load failed")?;
        rep.set_snapshot_state(seq, None)
            .await
            .context("reclone: set_snapshot_state failed")?;
    }

    info!(seq, "Catch-up breaker: replica re-clone from upstream complete");
    Ok(true)
}

/// Reconstruct a touched table's content at the catch-up cut from its rows@N
/// (`seed`) plus the replayed `events`, then XOR-hash it. Events are applied by
/// REPLACE on Create/Update (matching the SSP circuit's `rows.insert`) and
/// remove on Delete — never merged, even though the replica's own `apply`
/// merges. This REPLACE-not-MERGE semantics is the one detail that must match
/// the SSP exactly; it's a pure function so it can be pinned by tests.
#[cfg(test)]
fn project_table_hash(
    table: &str,
    seed: Vec<(String, serde_json::Value)>,
    events: &[&RecordUpdate],
) -> String {
    ssp_protocol::snapshot_hash::xor_table_hash(project_table_rows(table, seed, events).into_iter())
}

/// Reconstruct a touched table's `raw_id -> row` map at the catch-up cut from
/// its rows@N (`seed`) plus the replayed `events`, by REPLACE on Create/Update
/// and remove on Delete — matching the SSP circuit's `rows.insert`. Split out of
/// [`project_table_hash`] so the verifier can also surface the rows themselves
/// (for the mismatch diagnostic), not just their hash.
fn project_table_rows(
    table: &str,
    seed: Vec<(String, serde_json::Value)>,
    events: &[&RecordUpdate],
) -> std::collections::HashMap<String, serde_json::Value> {
    let mut rows: std::collections::HashMap<String, serde_json::Value> = seed.into_iter().collect();
    for ev in events {
        let raw = ev
            .record_id
            .strip_prefix(&format!("{}:", table))
            .unwrap_or(&ev.record_id)
            .to_string();
        match ev.operation {
            RecordOp::Create | RecordOp::Update => {
                if let Some(data) = &ev.data {
                    rows.insert(raw, data.clone());
                }
            }
            RecordOp::Delete => {
                rows.remove(&raw);
            }
        }
    }
    rows
}

#[cfg(test)]
mod catchup_tests {
    use super::*;
    use serde_json::{json, Value};
    use ssp_protocol::snapshot_hash::{xor_acc_to_hex, xor_empty, xor_table_hash};

    fn upd(table: &str, op: RecordOp, id: &str, data: Option<Value>) -> RecordUpdate {
        RecordUpdate {
            table: table.to_string(),
            operation: op,
            record_id: id.to_string(),
            data,
            version: 0,
        }
    }

    #[test]
    fn breaker_thresholds_default_and_floor() {
        // Defaults when unset.
        assert_eq!(
            resolve_breaker_thresholds(None, None),
            (CATCHUP_RECLONE_AFTER_DEFAULT, CATCHUP_ADMIT_AFTER_DEFAULT)
        );
        // Explicit values pass through when admit >= reclone.
        assert_eq!(resolve_breaker_thresholds(Some(2), Some(6)), (2, 6));
        // admit is floored to reclone so admit never precedes the re-clone.
        assert_eq!(resolve_breaker_thresholds(Some(4), Some(1)), (4, 4));
        // Setting them equal disables the re-clone step (admit on the same count).
        assert_eq!(resolve_breaker_thresholds(Some(3), Some(3)), (3, 3));
    }

    #[test]
    fn bootstrap_breaker_shares_defaults_with_catchup() {
        // Separate env knobs, same escalation shape: re-clone first, admit
        // later, admit never before re-clone.
        assert_eq!(
            resolve_breaker_thresholds(breaker_env("SPKY_BOOTSTRAP_RECLONE_AFTER"), breaker_env("SPKY_BOOTSTRAP_ADMIT_AFTER")),
            (CATCHUP_RECLONE_AFTER_DEFAULT, CATCHUP_ADMIT_AFTER_DEFAULT),
            "unset env must fall back to the compiled defaults"
        );
    }

    #[test]
    fn catchup_projection_covers_synced_meta_tables() {
        // `_00_user_feature` / `_00_app_release` are replicated content, so the
        // catch-up verifier must project them like any other table; only the
        // runtime-internal `_00_*` tables are skipped.
        assert!(!ssp_protocol::table_excluded_from_sync("_00_user_feature"));
        assert!(!ssp_protocol::table_excluded_from_sync("_00_app_release"));
        assert!(ssp_protocol::table_excluded_from_sync("_00_query"));
        assert!(ssp_protocol::table_excluded_from_sync("_00_list_ref_user_abc"));
    }

    #[test]
    fn projection_replaces_does_not_merge() {
        // Seed has {a, b}; a partial-looking update payload {a:9} must REPLACE
        // the whole row (matching the SSP), leaving {a:9} — NOT merge to {a:9,b:2}.
        let seed = vec![("r1".to_string(), json!({"a": 1, "b": 2}))];
        let ev = upd("presence", RecordOp::Update, "presence:r1", Some(json!({"a": 9})));
        let got = project_table_hash("presence", seed, &[&ev]);

        assert_eq!(got, xor_table_hash(vec![("r1".to_string(), json!({"a": 9}))]));
        assert_ne!(
            got,
            xor_table_hash(vec![("r1".to_string(), json!({"a": 9, "b": 2}))]),
            "must not behave like a MERGE"
        );
    }

    #[test]
    fn projection_multi_update_last_write_wins() {
        let evs = vec![
            upd("t", RecordOp::Create, "t:r1", Some(json!({"v": 1}))),
            upd("t", RecordOp::Update, "t:r1", Some(json!({"v": 2}))),
        ];
        let refs: Vec<&RecordUpdate> = evs.iter().collect();
        let got = project_table_hash("t", vec![], &refs);
        assert_eq!(got, xor_table_hash(vec![("r1".to_string(), json!({"v": 2}))]));
    }

    #[test]
    fn projection_delete_removes_row() {
        let seed = vec![("r1".to_string(), json!({"v": 1}))];
        let ev = upd("t", RecordOp::Delete, "t:r1", None);
        let got = project_table_hash("t", seed, &[&ev]);
        assert_eq!(got, xor_acc_to_hex(&xor_empty()));
    }

    #[test]
    fn projection_matches_ssp_incremental_for_hot_table() {
        // The whole point: a row written during catch-up. SSP seeds @N then
        // applies the same events incrementally; the scheduler projection must
        // land on the identical hash so the table is NOT falsely flagged.
        let seed = vec![("dev1".to_string(), json!({"fen": "start", "_00_rv": 3}))];
        let ev = upd(
            "presence",
            RecordOp::Update,
            "presence:dev1",
            Some(json!({"fen": "e4", "_00_rv": 4})),
        );
        let got = project_table_hash("presence", seed, &[&ev]);
        // `_00_rv` is stripped, so only the user content matters.
        assert_eq!(got, xor_table_hash(vec![("dev1".to_string(), json!({"fen": "e4"}))]));
    }
}
