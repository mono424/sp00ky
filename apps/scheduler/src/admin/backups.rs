//! The Backups tab's API.
//!
//! Two sources of truth, deliberately merged rather than picked between:
//!
//! * **This scheduler** executes backups and restores. Its registries (the
//!   same ones the ingest-port `/backup/*` routes and the control-plane
//!   worker drive) know the live state of anything in flight, including the
//!   restore stages (`main_db_restored`, `replica_restored`, `ssps_evicted`)
//!   that nothing else can see. They are in memory and forget on restart.
//! * **Sp00ky Cloud**, when linked, owns the catalog (rows in Postgres),
//!   the schedule, retention, and deletion. It knows nothing about stages.
//!
//! A linked scheduler shows the cloud catalog with each row joined to its
//! local registry entry by id (the worker uses the Postgres id as the
//! scheduler's backup id, so the join is exact). An unlinked scheduler with
//! S3 configured lists the bucket prefix instead, which is enough to create
//! and restore but offers no schedule, no retention and no delete; the tab
//! says so rather than hiding the panels.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

use maintenance::backup::{BackupJob, BackupStatus};
use maintenance::restore::{RestoreJob, RestoreStatus};
use maintenance::s3::BackupConfig;

use super::cloud::not_linked;
use super::ops::OpKind;
use super::{api_error, AdminState, ApiError, CurrentSession};

/// How long an in-flight backup or restore is watched before the operation
/// is called failed. Matches the control-plane worker's own 30 minute cap.
const WATCH_BUDGET: Duration = Duration::from_secs(30 * 60);

fn s3_unconfigured() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "No backup storage configured: set S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY and S3_BUCKET on the scheduler",
    )
}

fn storage_path(slug: &str, id: &str) -> String {
    format!("{slug}/{id}.surql.gz")
}

/// The id a stored object encodes, if it is one of ours.
fn id_from_key(slug: &str, key: &str) -> Option<String> {
    let rest = key.strip_prefix(slug)?.strip_prefix('/')?;
    let id = rest.strip_suffix(".surql.gz")?;
    (!id.is_empty() && !id.contains('/')).then(|| id.to_string())
}

fn stage_from_local(local: &Value) -> &'static str {
    match local.get("status").and_then(Value::as_str) {
        Some("queued") => "queued",
        Some("completed") => "done",
        Some("failed") => "failed",
        Some("running") => {
            if local.get("replica_restored").and_then(Value::as_bool) == Some(true) {
                "replica"
            } else if local.get("main_db_restored").and_then(Value::as_bool) == Some(true) {
                "main_db"
            } else {
                "running"
            }
        }
        _ => "running",
    }
}

fn stage_from_cloud(cloud: &Value) -> &'static str {
    match cloud.get("status").and_then(Value::as_str) {
        Some("pending") => "queued",
        Some("completed") => "done",
        Some("failed") => "failed",
        _ => "running",
    }
}

/// Unwrap the control plane's list shape without depending on whether it is
/// a bare array or `{backups: [...]}`.
fn cloud_list(v: Value) -> Vec<Value> {
    match v {
        Value::Array(a) => a,
        Value::Object(mut o) => o
            .remove("backups")
            .and_then(|b| b.as_array().cloned())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

async fn s3_catalog(config: &BackupConfig, slug: &str) -> Result<Vec<Value>, String> {
    // The bucket is created lazily by the first backup that writes to it;
    // listing one that does not exist yet is an XML error from the store, not
    // an empty catalog. Ensuring it first is idempotent and turns "no backups
    // yet" into the empty list it is.
    maintenance::s3::ensure_bucket(config).await;
    let bucket = config.get_bucket().map_err(|e| e.to_string())?;
    let pages = bucket
        .list(format!("{slug}/"), None)
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for page in pages {
        for obj in page.contents {
            let Some(id) = id_from_key(slug, &obj.key) else {
                continue;
            };
            out.push(json!({
                "id": id,
                "name": Value::Null,
                "status": "completed",
                "size_bytes": obj.size,
                "storage_path": obj.key,
                "snapshot_seq": Value::Null,
                "created_at": obj.last_modified,
                "completed_at": obj.last_modified,
                "error": Value::Null,
                "source": "s3",
            }));
        }
    }
    // Newest first, the order an operator reads a catalog in.
    out.sort_by(|a, b| {
        b["created_at"]
            .as_str()
            .unwrap_or("")
            .cmp(a["created_at"].as_str().unwrap_or(""))
    });
    Ok(out)
}

/// `GET /admin/api/backups`
pub async fn list(State(state): State<AdminState>) -> Result<Json<Value>, ApiError> {
    let backup = &state.backup;
    let configured = BackupConfig::env_configured();
    let slug = state.project_slug.clone();

    let recent = backup.registry.recent().await;
    let local_by_id: std::collections::HashMap<String, Value> = recent
        .iter()
        .map(|j| (j.backup_id.clone(), json!(j)))
        .collect();

    let (mut catalog, config, catalog_error): (Vec<Value>, Value, Option<String>) =
        match state.cloud.as_ref() {
            Some(link) => {
                let catalog = match link.get("/backups").await {
                    Ok(v) => Ok(cloud_list(v)),
                    Err((_, Json(body))) => Err(body["error"].as_str().unwrap_or("").to_string()),
                };
                let config = link.get("/backups/config").await.unwrap_or(Value::Null);
                match catalog {
                    Ok(mut rows) => {
                        for row in &mut rows {
                            row["source"] = json!("cloud");
                        }
                        (rows, config, None)
                    }
                    Err(e) => (Vec::new(), config, Some(e)),
                }
            }
            None if configured => match s3_catalog(&backup.config, &slug).await {
                Ok(rows) => (rows, Value::Null, None),
                Err(e) => (
                    Vec::new(),
                    Value::Null,
                    Some(format!("Could not list backup storage: {e}")),
                ),
            },
            None => (Vec::new(), Value::Null, None),
        };

    // Join each catalog row to what this scheduler knows about it, and
    // surface local jobs the catalog has not caught up with yet (a backup
    // still being written, or one made while unlinked).
    let mut seen = std::collections::HashSet::new();
    for row in &mut catalog {
        if let Some(id) = row["id"].as_str().map(str::to_string) {
            seen.insert(id.clone());
            row["local"] = local_by_id.get(&id).cloned().unwrap_or(Value::Null);
        }
    }
    for job in &recent {
        if seen.contains(&job.backup_id) {
            continue;
        }
        let status = match job.status {
            BackupStatus::Queued => "pending",
            BackupStatus::Running => "in_progress",
            BackupStatus::Completed => "completed",
            BackupStatus::Failed => "failed",
        };
        catalog.push(json!({
            "id": job.backup_id,
            "name": Value::Null,
            "status": status,
            "size_bytes": job.size_bytes.unwrap_or(0),
            "storage_path": job.storage_path,
            "snapshot_seq": job.snapshot_seq,
            "created_at": job.enqueued_at,
            "completed_at": job.finished_at,
            "error": job.error,
            "source": "local",
            "local": json!(job),
        }));
    }

    let mut out = json!({
        "linked": state.cloud.is_some(),
        "s3": {
            "configured": configured,
            "endpoint": configured.then(|| backup.config.s3_endpoint.clone()),
            "bucket": configured.then(|| backup.config.s3_bucket.clone()),
        },
        "project_slug": slug,
        "scheduler_status": format!("{:?}", *state.resync.status.read().await).to_lowercase(),
        "local": {
            "current_running": backup.registry.current_running().await,
            "queue_len": backup.registry.queue_len().await,
            "recent": recent,
        },
        "catalog": catalog,
        "restores": backup.restore_registry.recent().await,
        "config": config,
    });
    if let Some(e) = catalog_error {
        out["catalog_error"] = json!(e);
    }
    Ok(Json(out))
}

#[derive(Debug, Default, Deserialize)]
pub struct CreateRequest {
    #[serde(default)]
    pub name: Option<String>,
}

/// Watch the local backup registry for `id` to finish, updating `op_id`.
fn watch_backup(state: AdminState, op_id: String, id: String) {
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + WATCH_BUDGET;
        loop {
            if let Some(job) = state.backup.registry.get(&id).await {
                state
                    .ops
                    .progress(&op_id, json!({ "status": job.status, "size_bytes": job.size_bytes, "snapshot_seq": job.snapshot_seq }));
                match job.status {
                    BackupStatus::Completed => {
                        let size = job.size_bytes.unwrap_or(0);
                        state.ops.finish(
                            &op_id,
                            Some(format!("Backup written ({} bytes compressed)", size)),
                        );
                        return;
                    }
                    BackupStatus::Failed => {
                        state.ops.fail(
                            &op_id,
                            job.error.unwrap_or_else(|| "Backup failed".to_string()),
                        );
                        return;
                    }
                    _ => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                state
                    .ops
                    .fail(&op_id, "Backup did not finish within 30 minutes");
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// `POST /admin/api/backups`
pub async fn create(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    body: Option<Json<CreateRequest>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let req = body.map(|Json(b)| b).unwrap_or_default();
    if !BackupConfig::env_configured() {
        return Err(s3_unconfigured());
    }

    let backup_id = match state.cloud.as_ref() {
        Some(link) => {
            let (_, created) = link.post("/backups", json!({ "name": req.name })).await?;
            let id = created["id"]
                .as_str()
                .ok_or_else(|| {
                    api_error(
                        StatusCode::BAD_GATEWAY,
                        "Sp00ky Cloud returned no backup id",
                    )
                })?
                .to_string();
            info!(backup_id = %id, by = %session.subject, "Backup requested through Sp00ky Cloud");
            id
        }
        None => {
            let id = uuid::Uuid::new_v4().to_string();
            let slug = state.project_slug.clone();
            let backup = &state.backup;
            backup.registry.enqueue(id.clone(), slug.clone()).await;
            if let Err(e) = backup
                .tx
                .send(BackupJob {
                    backup_id: id.clone(),
                    project_slug: slug,
                })
                .await
            {
                backup
                    .registry
                    .mark_failed(&id, format!("queue send failed: {e}"))
                    .await;
                return Err(api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Backup queue is closed",
                ));
            }
            info!(backup_id = %id, by = %session.subject, "Backup enqueued locally");
            id
        }
    };

    let op = state.ops.start(
        OpKind::BackupCreate,
        Some(backup_id.clone()),
        session.subject.clone(),
        json!({ "backup_id": backup_id, "name": req.name, "status": "queued" }),
    );
    watch_backup(state.clone(), op.id.clone(), backup_id.clone());
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "operation": op, "backup_id": backup_id })),
    ))
}

fn watch_restore(state: AdminState, op_id: String, restore_id: String) {
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + WATCH_BUDGET;
        loop {
            if let Some(job) = state.backup.restore_registry.get(&restore_id).await {
                let local = json!(job);
                state.ops.progress(
                    &op_id,
                    json!({ "restore_id": restore_id, "stage": stage_from_local(&local), "local": local }),
                );
                match job.status {
                    RestoreStatus::Completed => {
                        state.ops.finish(
                            &op_id,
                            Some("Restore complete; SSPs are re-bootstrapping".to_string()),
                        );
                        return;
                    }
                    RestoreStatus::Failed => {
                        state.ops.fail(
                            &op_id,
                            job.error.unwrap_or_else(|| "Restore failed".to_string()),
                        );
                        return;
                    }
                    _ => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                state
                    .ops
                    .fail(&op_id, "Restore did not finish within 30 minutes");
                return;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// `POST /admin/api/backups/:id/restore`
pub async fn restore(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if !BackupConfig::env_configured() {
        return Err(s3_unconfigured());
    }
    // Mirror `begin_restore`'s own precondition so the refusal is immediate
    // and in the operator's words, not a failed operation a moment later.
    let status = *state.resync.status.read().await;
    if status != crate::SchedulerStatus::Ready {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Cannot restore while the scheduler is {status:?}; it must be Ready"),
        ));
    }

    let restore_id = match state.cloud.as_ref() {
        Some(link) => {
            let (_, res) = link
                .post(&format!("/backups/{id}/restore"), json!({}))
                .await?;
            let rid = res["id"]
                .as_str()
                .ok_or_else(|| {
                    api_error(
                        StatusCode::BAD_GATEWAY,
                        "Sp00ky Cloud returned no restore id",
                    )
                })?
                .to_string();
            warn!(backup_id = %id, restore_id = %rid, by = %session.subject, "Restore requested through Sp00ky Cloud");
            rid
        }
        None => {
            let backup = &state.backup;
            let slug = state.project_slug.clone();
            let path = storage_path(&slug, &id);
            // The restore id doubles as the backup id, as the ingest-port
            // route does when none is given; a second restore of the same
            // backup is refused until the first has left the registry.
            if backup.restore_registry.contains(&id).await {
                if let Some(existing) = backup.restore_registry.get(&id).await {
                    if matches!(
                        existing.status,
                        RestoreStatus::Queued | RestoreStatus::Running
                    ) {
                        return Err(api_error(
                            StatusCode::CONFLICT,
                            "A restore of this backup is already in progress",
                        ));
                    }
                }
            }
            backup
                .restore_registry
                .enqueue(id.clone(), id.clone(), slug.clone(), path.clone())
                .await;
            if let Err(e) = backup
                .restore_tx
                .send(RestoreJob {
                    restore_id: id.clone(),
                    backup_id: id.clone(),
                    project_slug: slug,
                    storage_path: path,
                })
                .await
            {
                backup
                    .restore_registry
                    .mark_failed(&id, format!("queue send failed: {e}"), Default::default())
                    .await;
                return Err(api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Restore queue is closed",
                ));
            }
            warn!(backup_id = %id, by = %session.subject, "Restore enqueued locally");
            id.clone()
        }
    };

    let op = state.ops.start(
        OpKind::BackupRestore,
        Some(id.clone()),
        session.subject.clone(),
        json!({ "backup_id": id, "restore_id": restore_id, "stage": "queued" }),
    );
    watch_restore(state.clone(), op.id.clone(), restore_id.clone());
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "operation": op, "restore_id": restore_id })),
    ))
}

/// `GET /admin/api/backups/:id/restore`, where `:id` is the backup id.
pub async fn restore_status(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let cloud = match state.cloud.as_ref() {
        Some(link) => match link.get(&format!("/backups/{id}/restore")).await {
            Ok(v) => Some(v),
            // No restore row yet is an ordinary answer, not a failure.
            Err((StatusCode::NOT_FOUND, _)) => None,
            Err(e) => return Err(e),
        },
        None => None,
    };

    // The local registry is keyed by restore id, which the cloud row names;
    // unlinked restores use the backup id for both.
    let restore_id = cloud
        .as_ref()
        .and_then(|c| c["id"].as_str().map(str::to_string))
        .unwrap_or_else(|| id.clone());
    let registry = &state.backup.restore_registry;
    let local = match registry.get(&restore_id).await {
        Some(j) => Some(j),
        None => registry
            .recent()
            .await
            .into_iter()
            .filter(|j| j.backup_id == id)
            .max_by_key(|j| j.enqueued_at),
    }
    .map(|j| json!(j));

    let stage = match (&local, &cloud) {
        (Some(l), _) => stage_from_local(l),
        (None, Some(c)) => stage_from_cloud(c),
        (None, None) => {
            return Err(api_error(
                StatusCode::NOT_FOUND,
                format!("No restore of backup '{id}' is known"),
            ))
        }
    };

    Ok(Json(
        json!({ "cloud": cloud, "local": local, "stage": stage }),
    ))
}

/// `DELETE /admin/api/backups/:id`
pub async fn delete(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let link = state.cloud.as_ref().ok_or_else(not_linked)?;
    warn!(backup_id = %id, by = %session.subject, "Backup deletion requested through Sp00ky Cloud");
    Ok(Json(link.delete(&format!("/backups/{id}")).await?))
}

#[derive(Debug, Default, Deserialize)]
pub struct ConfigRequest {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub schedule: Option<String>,
    /// Keep the newest N backups whatever their age.
    #[serde(default)]
    pub retention: Option<u32>,
    /// Retention tiers; Sp00ky Cloud merges them into the stored ones.
    #[serde(default)]
    pub keep: Option<KeepTiers>,
}

/// How many periods keep their newest backup, per tier. A tier left out keeps
/// its stored value. Unknown tiers are refused here rather than dropped, so a
/// typo is an error instead of a policy that silently keeps less.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KeepTiers {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hourly: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daily: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weekly: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monthly: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarterly: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yearly: Option<u32>,
}

/// `PUT /admin/api/backups/config`
pub async fn configure(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Json(req): Json<ConfigRequest>,
) -> Result<Json<Value>, ApiError> {
    let link = state.cloud.as_ref().ok_or_else(not_linked)?;
    let mut body = serde_json::Map::new();
    if let Some(v) = req.enabled {
        body.insert("enabled".into(), json!(v));
    }
    if let Some(v) = req.schedule {
        body.insert("schedule".into(), json!(v));
    }
    if let Some(v) = req.retention {
        body.insert("retention".into(), json!(v));
    }
    if let Some(v) = req.keep {
        body.insert("keep".into(), json!(v));
    }
    info!(by = %session.subject, config = ?body, "Backup schedule updated through Sp00ky Cloud");
    let (_, v) = link.post("/backups/configure", Value::Object(body)).await?;
    Ok(Json(v))
}

/// Used by `AdminState` construction so the slug is decided in one place.
pub fn project_slug(cloud: Option<&super::cloud::CloudLink>) -> String {
    cloud
        .map(|c| c.project.clone())
        .or_else(|| std::env::var("SPKY_PROJECT_SLUG").ok())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "default".to_string())
}

pub fn shared(state: &AdminState) -> Arc<maintenance::BackupState> {
    Arc::clone(&state.backup)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_through_ids() {
        assert_eq!(storage_path("wp", "abc"), "wp/abc.surql.gz");
        assert_eq!(id_from_key("wp", "wp/abc.surql.gz").as_deref(), Some("abc"));
        assert_eq!(id_from_key("wp", "other/abc.surql.gz"), None);
        assert_eq!(id_from_key("wp", "wp/abc.txt"), None);
        assert_eq!(id_from_key("wp", "wp/sub/abc.surql.gz"), None);
        assert_eq!(id_from_key("wp", "wp/.surql.gz"), None);
    }

    #[test]
    fn stages_follow_the_registry_flags() {
        assert_eq!(stage_from_local(&json!({ "status": "queued" })), "queued");
        assert_eq!(stage_from_local(&json!({ "status": "running" })), "running");
        assert_eq!(
            stage_from_local(&json!({ "status": "running", "main_db_restored": true })),
            "main_db"
        );
        assert_eq!(
            stage_from_local(
                &json!({ "status": "running", "main_db_restored": true, "replica_restored": true })
            ),
            "replica"
        );
        assert_eq!(stage_from_local(&json!({ "status": "completed" })), "done");
        assert_eq!(
            stage_from_local(&json!({ "status": "failed", "main_db_restored": true })),
            "failed"
        );
        assert_eq!(stage_from_cloud(&json!({ "status": "pending" })), "queued");
        assert_eq!(
            stage_from_cloud(&json!({ "status": "in_progress" })),
            "running"
        );
    }

    #[test]
    fn cloud_lists_come_bare_or_wrapped() {
        assert_eq!(cloud_list(json!([{ "id": 1 }])).len(), 1);
        assert_eq!(
            cloud_list(json!({ "backups": [{ "id": 1 }, { "id": 2 }] })).len(),
            2
        );
        assert!(cloud_list(json!("nope")).is_empty());
    }
}
