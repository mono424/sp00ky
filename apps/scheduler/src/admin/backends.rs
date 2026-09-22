//! `GET /admin/api/backends` and `/admin/api/backends/:name`.
//!
//! Backends are the user's own deployed services, health-checked from
//! `SPKY_BACKENDS`. The scheduler has no container handle for them, so what it
//! can honestly report is reachability: status, how long the probe took, when
//! it last succeeded, and the recent history of both. A backend that runs on a
//! machine pool is the exception: it is not probed at all (there may be no
//! machine to probe), and its status is what the pool sweep last saw.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::json;

use maintenance::backend_health::{BackendHealthEntry, HealthSample};

use super::{api_error, AdminState, ApiError};

fn sample_json(s: &HealthSample) -> serde_json::Value {
    json!({
        "at": s.at.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0),
        "ms": s.ms,
        "status": s.status.as_str(),
        "ok": s.status == maintenance::backend_health::BackendStatus::Healthy,
    })
}

fn entry_json(e: &BackendHealthEntry, with_history: bool) -> serde_json::Value {
    let mut v = json!({
        "name": e.name,
        "url": e.url,
        "ip": e.ip(),
        "port": e.port,
        "healthcheck": e.healthcheck,
        // The URL the poller actually requests, so the detail page does not
        // make the reader reassemble it from two fields.
        "healthcheck_url": format!("{}{}", e.url.trim_end_matches('/'), e.healthcheck),
        "status": e.status.as_str(),
        "response_time_ms": e.response_time_ms,
        "last_checked": e.last_checked.and_then(rfc3339),
        "last_healthy": e.last_healthy.and_then(rfc3339),
        // A pool backend has no container at `url` between jobs: its status
        // is the pool's (machines ready, booting, or none and nothing queued).
        "pool": e.pool,
    });

    if with_history {
        v["history"] = serde_json::Value::Array(e.history.iter().map(sample_json).collect());
        v["env"] = e
            .env
            .as_ref()
            .map(|env| {
                serde_json::Value::Object(crate::metrics::mask_sensitive_env(
                    crate::metrics::vec_env_to_map(env),
                ))
            })
            .unwrap_or(serde_json::Value::Null);
    }
    v
}

fn rfc3339(t: std::time::SystemTime) -> Option<String> {
    Some(chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339())
}

pub async fn list(State(state): State<AdminState>) -> Json<serde_json::Value> {
    let entries = state.metrics.backend_health.read().await;
    Json(json!({
        "backends": entries.iter().map(|e| entry_json(e, false)).collect::<Vec<_>>(),
        // Told to the client rather than assumed by it, so the detail page can
        // label its history axis with real time rather than "120 probes".
        "check_interval_secs": state.health_check_interval_secs,
    }))
}

pub async fn detail(
    State(state): State<AdminState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let entries = state.metrics.backend_health.read().await;
    match entries.iter().find(|e| e.name == name) {
        Some(e) => {
            let mut v = entry_json(e, true);
            v["check_interval_secs"] = json!(state.health_check_interval_secs);
            // Said explicitly rather than left for the UI to infer: the
            // scheduler health-checks backends over HTTP and has no pipe to
            // their stdout, so there is no log stream to offer here.
            v["logs_available"] = json!(false);
            Ok(Json(v))
        }
        None => Err(api_error(
            StatusCode::NOT_FOUND,
            format!("No backend named '{}'", name),
        )),
    }
}
