use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::info;

use crate::backup::{BackupJob, BackupJobState, BackupRegistry};
use crate::host::MaintenanceHost;
use crate::restore::{
    ContinueResult, ReplayOutcome, RestoreJob, RestoreJobState, RestoreProgress, RestoreRegistry,
};
use crate::s3::BackupConfig;

#[derive(Clone)]
pub struct BackupState {
    pub host: Arc<dyn MaintenanceHost>,
    pub config: Arc<BackupConfig>,
    pub registry: Arc<BackupRegistry>,
    pub tx: mpsc::Sender<BackupJob>,
    pub restore_registry: Arc<RestoreRegistry>,
    pub restore_tx: mpsc::Sender<RestoreJob>,
    pub backup_restore_lock: Arc<Mutex<()>>,
}

pub fn create_backup_router(state: BackupState) -> Router {
    Router::new()
        .route("/backup/create", post(create_backup))
        .route("/backup/restore", post(restore_backup))
        .route("/backup/restore/held", post(restore_backup_held))
        .route("/backup/restore/status/:restore_id", get(restore_status_by_id))
        .route("/backup/restore/:restore_id/continue", post(continue_restore))
        .route("/backup/status", get(backup_status))
        .route("/backup/status/:backup_id", get(backup_status_by_id))
        .with_state(state)
}

#[derive(Deserialize)]
struct CreateBackupRequest {
    backup_id: String,
    project_slug: String,
}

#[derive(Serialize)]
struct CreateBackupResponse {
    backup_id: String,
    status: String,
    queue_position: usize,
}

async fn create_backup(
    State(state): State<BackupState>,
    Json(req): Json<CreateBackupRequest>,
) -> Result<(StatusCode, Json<CreateBackupResponse>), (StatusCode, String)> {
    if state.registry.contains(&req.backup_id).await {
        return Err((
            StatusCode::CONFLICT,
            format!("backup_id {} already exists", req.backup_id),
        ));
    }

    state
        .registry
        .enqueue(req.backup_id.clone(), req.project_slug.clone())
        .await;

    let job = BackupJob {
        backup_id: req.backup_id.clone(),
        project_slug: req.project_slug.clone(),
    };

    if let Err(e) = state.tx.send(job).await {
        state
            .registry
            .mark_failed(&req.backup_id, format!("queue send failed: {}", e))
            .await;
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Backup queue is closed".to_string(),
        ));
    }

    let queue_position = state.registry.queue_len().await;
    info!(backup_id = %req.backup_id, queue_position, "Backup enqueued");

    Ok((
        StatusCode::ACCEPTED,
        Json(CreateBackupResponse {
            backup_id: req.backup_id,
            status: "queued".to_string(),
            queue_position,
        }),
    ))
}

#[derive(Deserialize)]
struct RestoreRequest {
    /// Optional — if omitted the host uses `backup_id` as the restore id.
    #[serde(default)]
    restore_id: Option<String>,
    backup_id: String,
    project_slug: String,
    storage_path: String,
}

#[derive(Serialize)]
struct RestoreResponse {
    restore_id: String,
    status: String,
    queue_position: usize,
}

async fn restore_backup(
    State(state): State<BackupState>,
    Json(req): Json<RestoreRequest>,
) -> Result<(StatusCode, Json<RestoreResponse>), (StatusCode, String)> {
    enqueue_restore(state, req, false).await
}

/// `POST /backup/restore/held`: a restore that stops after the main database
/// import until Sp00ky Cloud has replayed the change feed onto it, then
/// rebuilds the host's state from the main database. A scheduler without
/// this route answers 404, which is how the cloud tells it cannot do a
/// point-in-time restore before it touches any data.
async fn restore_backup_held(
    State(state): State<BackupState>,
    Json(req): Json<RestoreRequest>,
) -> Result<(StatusCode, Json<RestoreResponse>), (StatusCode, String)> {
    enqueue_restore(state, req, true).await
}

async fn enqueue_restore(
    state: BackupState,
    req: RestoreRequest,
    hold_for_replay: bool,
) -> Result<(StatusCode, Json<RestoreResponse>), (StatusCode, String)> {
    let restore_id = req.restore_id.unwrap_or_else(|| req.backup_id.clone());

    if state.restore_registry.contains(&restore_id).await {
        return Err((
            StatusCode::CONFLICT,
            format!("restore_id {} already exists", restore_id),
        ));
    }

    state
        .restore_registry
        .enqueue(
            restore_id.clone(),
            req.backup_id.clone(),
            req.project_slug.clone(),
            req.storage_path.clone(),
        )
        .await;

    let job = RestoreJob {
        restore_id: restore_id.clone(),
        backup_id: req.backup_id.clone(),
        project_slug: req.project_slug.clone(),
        storage_path: req.storage_path.clone(),
        hold_for_replay,
    };

    if let Err(e) = state.restore_tx.send(job).await {
        state
            .restore_registry
            .mark_failed(
                &restore_id,
                format!("queue send failed: {}", e),
                RestoreProgress::default(),
            )
            .await;
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Restore queue is closed".to_string(),
        ));
    }

    let queue_position = state.restore_registry.queue_len().await;
    info!(
        restore_id = %restore_id,
        backup_id = %req.backup_id,
        queue_position,
        hold_for_replay,
        "Restore enqueued"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(RestoreResponse {
            restore_id,
            status: "queued".to_string(),
            queue_position,
        }),
    ))
}

async fn restore_status_by_id(
    State(state): State<BackupState>,
    Path(restore_id): Path<String>,
) -> Result<Json<RestoreJobState>, (StatusCode, String)> {
    match state.restore_registry.get(&restore_id).await {
        Some(s) => Ok(Json(s)),
        None => Err((
            StatusCode::NOT_FOUND,
            format!("restore_id {} not found", restore_id),
        )),
    }
}

/// `POST /backup/restore/:restore_id/continue` with `{"ok": bool, "error"?:
/// string}`: the point-in-time replay of a held restore is done (or failed),
/// so it may rebuild the host's state and finish.
async fn continue_restore(
    State(state): State<BackupState>,
    Path(restore_id): Path<String>,
    Json(outcome): Json<ReplayOutcome>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match state.restore_registry.continue_replay(&restore_id, outcome.clone()).await {
        ContinueResult::Delivered => {
            info!(restore_id = %restore_id, ok = outcome.ok, "Point-in-time replay reported");
            Ok(Json(serde_json::json!({ "restore_id": restore_id, "status": "running" })))
        }
        ContinueResult::NotAwaiting => Err((
            StatusCode::CONFLICT,
            format!("restore_id {} is not awaiting a replay", restore_id),
        )),
        ContinueResult::Unknown => Err((
            StatusCode::NOT_FOUND,
            format!("restore_id {} not found", restore_id),
        )),
    }
}

async fn backup_status(State(state): State<BackupState>) -> Json<serde_json::Value> {
    let current = state.registry.current_running().await;
    let queue_len = state.registry.queue_len().await;
    let recent = state.registry.recent().await;
    let s3_reachable = state.config.get_bucket().is_ok();

    let mut body = serde_json::json!({
        "current_running": current,
        "queue_len": queue_len,
        "recent": recent,
        "s3_endpoint": state.config.s3_endpoint,
        "s3_bucket": state.config.s3_bucket,
        "s3_reachable": s3_reachable,
    });

    // Host-specific counters (scheduler: pending events / seq / lag).
    if let (Some(obj), serde_json::Value::Object(extras)) =
        (body.as_object_mut(), state.host.status_extras().await)
    {
        for (k, v) in extras {
            obj.insert(k, v);
        }
    }

    Json(body)
}

async fn backup_status_by_id(
    State(state): State<BackupState>,
    Path(backup_id): Path<String>,
) -> Result<Json<BackupJobState>, (StatusCode, String)> {
    match state.registry.get(&backup_id).await {
        Some(s) => Ok(Json(s)),
        None => Err((StatusCode::NOT_FOUND, format!("backup_id {} not found", backup_id))),
    }
}
