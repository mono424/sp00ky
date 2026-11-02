//! `/admin/api/data/*`: tables, users and bucket files, for the dashboard's
//! Database, Users and Storage pages. The routes and everything they do are
//! `schedule_core::admin_data`, shared with the SSP node; this is the scheduler's
//! half: the checks only a host can make, and the frame around the answer.

use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{Method, StatusCode};
use axum::{Extension, Json};
use schedule_core::admin_data::{self as core, Request};
use serde_json::Value;

use super::session::SessionMode;
use super::{api_error, core_db, core_error, AdminState, ApiError, CurrentSession};

/// The database stops each browse statement sooner (see `READ_TIMEOUT` there);
/// this frees the request if the database itself has stopped answering.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Any `/admin/api/data/*` request. The bearer gate has already refused a
/// read-only session everything but GET.
pub async fn handle(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    method: Method,
    Path(rest): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    if !state.config.data {
        return Err(api_error(StatusCode::FORBIDDEN, "Data access is turned off on this project (SPKY_ADMIN_DATA)"));
    }
    // An agent token was minted to operate the cluster, and existed before this
    // surface did; it does not grow into reading the app's data.
    if session.mode == SessionMode::Mcp {
        return Err(api_error(StatusCode::FORBIDDEN, "MCP tokens cannot reach project data; sign in to the dashboard"));
    }
    let db = core_db(&state)?;
    let path: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();
    let request = Request { method: method.as_str(), path: &path, query: &query, body: &body, actor: &session.subject };
    match tokio::time::timeout(REQUEST_TIMEOUT, core::dispatch(&db, request)).await {
        Ok(answer) => answer.map(Json).map_err(core_error),
        Err(_) => Err(api_error(StatusCode::GATEWAY_TIMEOUT, "The database did not answer within 60s")),
    }
}
