//! `GET /admin/api/overview` — the dashboard's single polling endpoint.
//!
//! Built on top of [`crate::metrics::build_entities`], which is the same
//! assembly `/info` serves. That reuse is the point: the dashboard and `/info`
//! must never be able to disagree about whether an SSP is ready.

use axum::extract::State;
use axum::Json;
use serde_json::json;

use super::AdminState;

pub async fn overview(State(state): State<AdminState>) -> Json<serde_json::Value> {
    let entities = crate::metrics::build_entities(&state.metrics).await;

    let mut scheduler = serde_json::Value::Null;
    let mut ssps = Vec::new();
    let mut backends = Vec::new();
    for e in entities {
        match e.get("entity").and_then(|v| v.as_str()) {
            Some("scheduler") => scheduler = e,
            Some("ssp") => ssps.push(e),
            Some("backend") => backends.push(e),
            _ => {}
        }
    }

    let ready_ssps = ssps
        .iter()
        .filter(|s| s.get("status").and_then(|v| v.as_str()) == Some("ready"))
        .count();
    let healthy_backends = backends
        .iter()
        .filter(|b| b.get("status").and_then(|v| v.as_str()) == Some("healthy"))
        .count();

    Json(json!({
        "scheduler": scheduler,
        "ssps": ssps,
        "backends": backends,
        "totals": {
            "ssps": ssps.len(),
            "ssps_ready": ready_ssps,
            "backends": backends.len(),
            "backends_healthy": healthy_backends,
        },
        // The dashboard renders a deadline bar for a bootstrapping SSP against
        // this; it is the same budget the scheduler reaps hung bootstraps on.
        "bootstrap_timeout_secs": state.bootstrap_timeout_secs,
        // Running operations ride on the poll the dashboard already makes,
        // so an activity strip costs no extra request.
        "operations": state.ops.running(),
        // Same bargain for presence: the sampler already holds these in memory,
        // so the live-user tile and the sidebar count come free with the poll
        // the app makes anyway, and add nothing to the database.
        "presence": state.presence.overview_block(),
        // And again for jobs: the sampler holds the outbox counts, so the Jobs
        // tile and the sidebar count ride the poll the dashboard already makes
        // rather than scanning a user table per open tab.
        "jobs": state.jobs.overview_block(),
        "cloud_linked": state.cloud.is_some(),
        "server_time_ms": now_ms(),
    }))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
