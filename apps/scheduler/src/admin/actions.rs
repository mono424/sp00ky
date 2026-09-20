//! Restart-shaped actions: SSPs, the scheduler itself, and the cloud bridge.
//!
//! # How an SSP is restarted from here
//!
//! The scheduler never reaches into a container. It flags the SSP in the pool,
//! the SSP's next heartbeat gets a 409 carrying a [`ssp_protocol::ResyncDirective`],
//! the SSP exits, and its supervisor relaunches it into the ordinary register
//! + bootstrap + verify path. That path is the one every SSP already takes on
//! boot, which is why it is trusted over any in-place trick: a restart from
//! the dashboard is indistinguishable from a restart by Docker.
//!
//! The consequence is latency and asynchrony. Nothing happens until the next
//! heartbeat (`heartbeat_interval_ms`, 5s by default), and "done" means the
//! SSP re-registered and reached `ready`, which a watcher task decides by
//! looking at the pool. That watcher is what an [`super::ops::Operation`]
//! reports on.
//!
//! # Restarting the scheduler
//!
//! `std::process::exit(0)` after the response has flushed. The control plane
//! runs the scheduler under `unless-stopped`, which relaunches on any exit
//! code. A scheduler run from a checkout is not supervised and simply stops;
//! `/admin/api/config` says which it is (`supervised`) so the dashboard can
//! warn before, not after.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::router::ResyncKind;

use super::cloud::not_linked;
use super::ops::{OpKind, Operations};
use super::{api_error, AdminState, ApiError, CurrentSession};

fn accepted(op: super::ops::Operation) -> (StatusCode, Json<Value>) {
    (StatusCode::ACCEPTED, Json(json!({ "operation": op })))
}

// ---------------------------------------------------------------------------
// SSPs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SspRestartRequest {
    #[serde(default = "default_mode")]
    pub mode: String,
}

fn default_mode() -> String {
    "restart".to_string()
}

#[derive(Debug, Deserialize)]
pub struct RestartAllRequest {
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "default_true")]
    pub rolling: bool,
}

fn default_true() -> bool {
    true
}

fn resync_kind(mode: &str) -> Result<ResyncKind, ApiError> {
    match mode {
        "restart" => Ok(ResyncKind::Resync),
        "clean" => Ok(ResyncKind::Clean),
        other => Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("Unknown mode '{other}' (expected \"restart\", \"clean\" or \"reload\")"),
        )),
    }
}

async fn ssp_snapshot(state: &AdminState, id: &str) -> Option<(String, String, Instant)> {
    let pool = state.metrics.ssp_pool.read().await;
    pool.get(id)
        .map(|s| (s.url.clone(), s.version.clone(), s.connected_at))
}

/// How long to wait for a restarted SSP before calling the operation failed:
/// one heartbeat for the flag to be consumed, then the bootstrap budget the
/// scheduler itself reaps hung bootstraps on, plus slack for the relaunch.
fn comeback_budget(state: &AdminState) -> Duration {
    Duration::from_millis(state.heartbeat_interval_ms)
        + Duration::from_secs(state.bootstrap_timeout_secs)
        + Duration::from_secs(30)
}

/// Wait for `id` to re-register after `since` and become ready.
///
/// Returns `Ok(())` on ready, `Err(reason)` on timeout. Polls the pool rather
/// than subscribing to anything: a one-second poll for the duration of one
/// restart is nothing, and it needs no new plumbing in the registration path.
async fn await_comeback(
    state: &AdminState,
    id: &str,
    since: Instant,
    budget: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + budget;
    let mut seen_gone = false;
    loop {
        {
            let pool = state.metrics.ssp_pool.read().await;
            match pool.get(id) {
                Some(s) if s.connected_at > since => {
                    if pool.is_ready(id) {
                        return Ok(());
                    }
                }
                Some(_) => {
                    // Still the old registration: the flag has not been
                    // consumed yet, or the SSP is between exit and relaunch
                    // with its stale entry not yet reaped.
                }
                None => seen_gone = true,
            }
        }
        if Instant::now() >= deadline {
            return Err(if seen_gone {
                format!(
                    "{id} left the pool but did not re-register within {}s",
                    budget.as_secs()
                )
            } else {
                format!(
                    "{id} did not restart within {}s (was the flag consumed?)",
                    budget.as_secs()
                )
            });
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// `POST /admin/api/ssps/:id/restart`
pub async fn ssp_restart(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    Path(id): Path<String>,
    body: Option<Json<SspRestartRequest>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mode = body.map(|Json(b)| b.mode).unwrap_or_else(default_mode);
    let Some((url, version, _)) = ssp_snapshot(&state, &id).await else {
        return Err(api_error(
            StatusCode::NOT_FOUND,
            format!("No SSP with id '{id}'"),
        ));
    };

    if mode == "reload" {
        let op = state.ops.start(
            OpKind::SspReload,
            Some(id.clone()),
            session.subject.clone(),
            json!({ "ssp_version": version }),
        );
        info!(ssp_id = %id, by = %session.subject, "SSP reload requested");
        let ops = Arc::clone(&state.ops);
        let transport = Arc::clone(&state.transport);
        let op_id = op.id.clone();
        tokio::spawn(async move {
            // `/admin/reload` blocks until the rebuild is done, so the call's
            // outcome IS the operation's outcome.
            match transport
                .post_to_ssp(&url, "/admin/reload", &json!({}))
                .await
            {
                Ok(_) => ops.finish(
                    &op_id,
                    Some("Circuit rebuilt from the database".to_string()),
                ),
                Err(e) => ops.fail(&op_id, format!("Reload failed: {e}")),
            }
        });
        return Ok(accepted(op));
    }

    let kind = resync_kind(&mode)?;
    let op_kind = match kind {
        ResyncKind::Resync => OpKind::SspRestart,
        ResyncKind::Clean => OpKind::SspClean,
    };
    let requested_at = Instant::now();
    state
        .metrics
        .ssp_pool
        .write()
        .await
        .mark_for_resync_with(&id, kind);
    info!(ssp_id = %id, ?kind, by = %session.subject, "SSP restart requested");

    let op = state.ops.start(
        op_kind,
        Some(id.clone()),
        session.subject.clone(),
        json!({ "ssp_version": version, "phase": "awaiting heartbeat" }),
    );
    let budget = comeback_budget(&state);
    let watcher_state = state.clone();
    let op_id = op.id.clone();
    tokio::spawn(async move {
        match await_comeback(&watcher_state, &id, requested_at, budget).await {
            Ok(()) => watcher_state
                .ops
                .finish(&op_id, Some("Back and ready".to_string())),
            Err(reason) => watcher_state.ops.fail(&op_id, reason),
        }
    });
    Ok(accepted(op))
}

/// `POST /admin/api/ssps/restart-all`
pub async fn ssps_restart_all(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    body: Option<Json<RestartAllRequest>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let req = body.map(|Json(b)| b).unwrap_or(RestartAllRequest {
        mode: default_mode(),
        rolling: true,
    });
    let kind = resync_kind(&req.mode)?;

    let ids: Vec<String> = {
        let pool = state.metrics.ssp_pool.read().await;
        let mut ids: Vec<String> = pool.all().into_iter().map(|s| s.id.clone()).collect();
        ids.sort();
        ids
    };
    if ids.is_empty() {
        return Err(api_error(StatusCode::CONFLICT, "No SSPs are registered"));
    }

    if !req.rolling {
        let requested_at = Instant::now();
        let marked = state
            .metrics
            .ssp_pool
            .write()
            .await
            .mark_all_for_resync_with(kind);
        info!(marked, ?kind, by = %session.subject, "All SSPs flagged for restart");
        let op = state.ops.start(
            OpKind::RollingRestart,
            None,
            session.subject.clone(),
            json!({ "rolling": false, "kind": kind, "total": marked, "done": 0 }),
        );
        let budget = comeback_budget(&state);
        let watcher = state.clone();
        let op_id = op.id.clone();
        tokio::spawn(async move {
            let mut failed = Vec::new();
            let mut done = 0usize;
            for id in ids {
                match await_comeback(&watcher, &id, requested_at, budget).await {
                    Ok(()) => {
                        done += 1;
                        watcher.ops.progress(&op_id, json!({ "done": done }));
                    }
                    Err(reason) => failed.push(reason),
                }
            }
            if failed.is_empty() {
                watcher
                    .ops
                    .finish(&op_id, Some(format!("{done} SSPs back and ready")));
            } else {
                watcher.ops.fail(&op_id, failed.join("; "));
            }
        });
        return Ok(accepted(op));
    }

    if state.ops.is_running(OpKind::RollingRestart, None) {
        return Err(api_error(
            StatusCode::CONFLICT,
            "A rolling restart is already in progress",
        ));
    }
    let op = state.ops.start(
        OpKind::RollingRestart,
        None,
        session.subject.clone(),
        json!({ "rolling": true, "kind": kind, "total": ids.len(), "done": 0, "current": ids[0] }),
    );
    info!(count = ids.len(), ?kind, by = %session.subject, "Rolling restart started");
    let budget = comeback_budget(&state);
    let watcher = state.clone();
    let op_id = op.id.clone();
    tokio::spawn(async move {
        let total = ids.len();
        for (i, id) in ids.into_iter().enumerate() {
            watcher
                .ops
                .progress(&op_id, json!({ "done": i, "current": id }));
            let requested_at = Instant::now();
            {
                let mut pool = watcher.metrics.ssp_pool.write().await;
                if pool.get(&id).is_none() {
                    // Left on its own between listing and its turn; nothing
                    // to restart, and not a failure of the roll.
                    continue;
                }
                pool.mark_for_resync_with(&id, kind);
            }
            if let Err(reason) = await_comeback(&watcher, &id, requested_at, budget).await {
                // Stop the roll: continuing would take down the next SSP
                // while this one is still missing, which is the exact
                // outage a rolling restart exists to avoid.
                watcher
                    .ops
                    .fail(&op_id, format!("Stopped after {i} of {total}: {reason}"));
                return;
            }
        }
        watcher
            .ops
            .progress(&op_id, json!({ "done": total, "current": Value::Null }));
        watcher.ops.finish(
            &op_id,
            Some(format!("{total} SSPs restarted one at a time")),
        );
    });
    Ok(accepted(op))
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SchedulerRestartRequest {
    #[serde(default = "default_mode")]
    pub mode: String,
}

/// `POST /admin/api/scheduler/restart`
pub async fn scheduler_restart(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    body: Option<Json<SchedulerRestartRequest>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mode = body.map(|Json(b)| b.mode).unwrap_or_else(default_mode);
    match mode.as_str() {
        "restart" => {
            let op = state.ops.start(
                OpKind::SchedulerRestart,
                None,
                session.subject.clone(),
                json!({ "supervised": state.supervised }),
            );
            state.incidents.flush_operation(&op).await;
            warn!(by = %session.subject, supervised = state.supervised, "Scheduler restart requested from the dashboard; exiting");
            tokio::spawn(async move {
                // Let the 202 leave the socket first. The operation itself is
                // never marked done: the process that would do it is gone,
                // and the dashboard's reconnect loop is what observes the
                // outcome.
                tokio::time::sleep(Duration::from_millis(300)).await;
                std::process::exit(0);
            });
            Ok(accepted(op))
        }
        "reclone" | "rehash" => {
            let kind = if mode == "reclone" {
                OpKind::Reclone
            } else {
                OpKind::Rehash
            };
            if state.ops.is_running(kind, None) {
                return Err(api_error(
                    StatusCode::CONFLICT,
                    format!("A {mode} is already in progress"),
                ));
            }
            // Preflight the status guards synchronously so the operator gets
            // the 503/409 in their face rather than in an operation that
            // fails a second later.
            match *state.resync.status.read().await {
                crate::SchedulerStatus::Cloning => {
                    return Err(api_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Scheduler is still cloning the database",
                    ))
                }
                crate::SchedulerStatus::Restoring => {
                    return Err(api_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Scheduler is restoring from a backup",
                    ))
                }
                _ => {}
            }
            let op = state
                .ops
                .start(kind, None, session.subject.clone(), json!({}));
            info!(mode, by = %session.subject, "Scheduler resync requested from the dashboard");
            let ops = Arc::clone(&state.ops);
            let args = state.resync.clone();
            let op_id = op.id.clone();
            tokio::spawn(async move {
                match crate::ssp_management::run_resync(&args, &mode, None).await {
                    Ok(result) => {
                        ops.progress(&op_id, result);
                        ops.finish(
                            &op_id,
                            Some(format!("{mode} complete; SSPs flagged to re-verify")),
                        );
                    }
                    Err((_, msg)) => ops.fail(&op_id, msg),
                }
            });
            Ok(accepted(op))
        }
        other => Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("Unknown mode '{other}' (expected \"restart\", \"reclone\" or \"rehash\")"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Cloud
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
pub struct CloudRestartRequest {
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub upgrade: bool,
    #[serde(default)]
    pub clean: bool,
    #[serde(default)]
    pub surreal: bool,
}

/// `POST /admin/api/cloud/restart`
pub async fn cloud_restart(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    body: Option<Json<CloudRestartRequest>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let link = state.cloud.as_ref().ok_or_else(not_linked)?;
    let req = body.map(|Json(b)| b).unwrap_or_default();

    let mut payload = json!({
        "upgrade": req.upgrade,
        "clean": req.clean,
        "surreal": req.surreal,
    });
    if !req.roles.is_empty() {
        payload["roles"] = json!(req.roles);
    }
    warn!(
        by = %session.subject, roles = ?req.roles, upgrade = req.upgrade, clean = req.clean, surreal = req.surreal,
        "Cloud restart requested from the dashboard"
    );

    // Will this process survive? Empty roles means scheduler + SSPs; clean
    // always forces the scheduler in. If not, there is nobody left to mark
    // the operation done, and the dashboard's reconnect loop takes over.
    let restarts_scheduler = req.clean
        || req.roles.is_empty()
        || req
            .roles
            .iter()
            .any(|r| r == "scheduler" || r == "surrealdb");
    let op = state.ops.start(
        OpKind::CloudRestart,
        None,
        session.subject.clone(),
        json!({ "roles": req.roles, "upgrade": req.upgrade, "clean": req.clean, "surreal": req.surreal, "restarts_scheduler": restarts_scheduler }),
    );

    state.incidents.flush_operation(&op).await;
    let (_, cloud) = match link.post("/restart", payload).await {
        Ok(response) => response,
        Err(error) => { state.ops.fail(&op.id, "Cloud restart request failed"); return Err(error); }
    };
    state.ops.progress(&op.id, json!({ "cloud": cloud }));

    if restarts_scheduler {
        state.ops.progress(
            &op.id,
            json!({ "phase": "queued; this scheduler will be recreated" }),
        );
    } else {
        // SSP-only: watch them all come back. The worker destroys and
        // recreates them, so every current registration will be replaced.
        let ids: Vec<String> = {
            let pool = state.metrics.ssp_pool.read().await;
            pool.all().into_iter().map(|s| s.id.clone()).collect()
        };
        let requested_at = Instant::now();
        let watcher = state.clone();
        let op_id = op.id.clone();
        tokio::spawn(async move {
            // The control-plane worker polls every 5s and image pulls take a
            // while; give this far more room than a native restart.
            let budget = comeback_budget(&watcher) + Duration::from_secs(600);
            let mut failed = Vec::new();
            for id in ids {
                if let Err(reason) = await_comeback(&watcher, &id, requested_at, budget).await {
                    failed.push(reason);
                }
            }
            if failed.is_empty() {
                watcher
                    .ops
                    .finish(&op_id, Some("SSPs recreated and ready".to_string()));
            } else {
                watcher.ops.fail(&op_id, failed.join("; "));
            }
        });
    }
    Ok(accepted(state.ops.get(&op.id).unwrap_or(op)))
}

/// `GET /admin/api/cloud/deployment`
pub async fn cloud_deployment(State(state): State<AdminState>) -> Result<Json<Value>, ApiError> {
    let link = state.cloud.as_ref().ok_or_else(not_linked)?;
    // Masked: the control plane's answer carries the database root password and
    // every backend's environment, and this endpoint is also an MCP tool.
    Ok(Json(crate::metrics::mask_cloud_secrets(link.get("/deployment").await?)))
}

/// `GET /admin/api/operations`
pub async fn list_operations(State(state): State<AdminState>) -> Json<Value> {
    Json(state.ops.snapshot())
}

/// `GET /admin/api/operations/stream`: the full recent list on every change.
pub async fn stream_operations(
    State(state): State<AdminState>,
) -> axum::response::sse::Sse<
    impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures::StreamExt;
    use tokio::sync::broadcast::error::RecvError;

    let ops: Arc<Operations> = Arc::clone(&state.ops);
    let rx = ops.subscribe();
    let first = futures::stream::iter(vec![Ok::<_, std::convert::Infallible>(
        Event::default()
            .event("operations")
            .json_data(ops.snapshot())
            .unwrap_or_else(|_| Event::default().comment("unserialisable payload")),
    )]);
    let live = futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(payload) => {
                    let event = Event::default()
                        .event("operations")
                        .json_data(payload)
                        .unwrap_or_else(|_| Event::default().comment("unserialisable payload"));
                    return Some((Ok::<_, std::convert::Infallible>(event), rx));
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    Sse::new(first.chain(live).boxed())
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_map_to_resync_kinds() {
        assert_eq!(resync_kind("restart").unwrap(), ResyncKind::Resync);
        assert_eq!(resync_kind("clean").unwrap(), ResyncKind::Clean);
        assert!(resync_kind("reload").is_err(), "reload is not a resync");
        assert!(resync_kind("nope").is_err());
    }
}
