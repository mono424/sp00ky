//! An MCP server for the admin plane: every action as a tool.
//!
//! The tool table and the JSON-RPC protocol are `ssp_protocol::admin_mcp`,
//! shared with the SSP node's admin API. This file is the scheduler's half: a
//! `tools/call` is sent through a clone of the admin router **in-process**
//! (`tower::ServiceExt::oneshot`), carrying the caller's own bearer, so the
//! audit log lines keep the real subject and the bearer middleware's scope
//! rule applies to agents exactly as it does to browsers.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json, Router};
use serde_json::{json, Value};
use ssp_protocol::admin_mcp::{self as protocol, Rpc, ServerInfo, ToolDef};
use tower::ServiceExt;
use tracing::{info, warn};

use super::session::Scope;
use super::{AdminState, CurrentSession};

/// A tool call is one admin request; a minute is more than any of them takes.
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(60);
/// Cap on a collected response body. The largest honest answer (a log
/// backfill of 10 000 lines) is well under this.
const BODY_LIMIT: usize = 4 * 1024 * 1024;

/// `POST /admin/api/mcp`
pub async fn handle(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    request: Request,
) -> Response {
    let auth = request.headers().get(header::AUTHORIZATION).cloned();
    let body = match axum::body::to_bytes(request.into_body(), BODY_LIMIT).await {
        Ok(b) => b,
        Err(_) => return rpc_response(protocol::rpc_error(Value::Null, -32700, "Request body unreadable")),
    };
    let message: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return rpc_response(protocol::rpc_error(Value::Null, -32700, format!("Parse error: {e}"))),
    };
    if message.get("method").and_then(Value::as_str) == Some("initialize") {
        let client = message.pointer("/params/clientInfo/name").and_then(|n| n.as_str());
        info!(subject = %session.subject, client = ?client, "MCP session initialized");
    }
    let server = ServerInfo { version: env!("CARGO_PKG_VERSION"), instructions: instructions(&state, session.scope) };
    // The scheduler serves every tool in the table.
    match protocol::handle(&message, session.scope, &|_| true, server) {
        Rpc::Accepted => StatusCode::ACCEPTED.into_response(),
        Rpc::Reply(value) => rpc_response(value),
        Rpc::Call { id, tool, method, uri, body } => {
            info!(tool = tool.name, by = %session.subject, "MCP tool call");
            rpc_response(call_tool(&state, tool, id, method, &uri, body, auth).await)
        }
    }
}

fn instructions(state: &AdminState, scope: Scope) -> String {
    let mut s = format!(
        "Operator tools for the Sp00ky scheduler of project '{}'. Start with `overview`. \
         Restart, cancel, retry and backup actions are asynchronous: they answer with an \
         operation you can watch through `operations_list`.",
        state.project_slug
    );
    if scope == Scope::Read {
        s.push_str(" This token is read-only, so only inspection tools are available.");
    }
    if state.cloud.is_none() {
        s.push_str(" This scheduler is not linked to Sp00ky Cloud, so cloud_* tools and backup delete or configure will be refused.");
    }
    s
}

fn rpc_response(value: Value) -> Response {
    (StatusCode::OK, Json(value)).into_response()
}

/// `GET` and `DELETE /admin/api/mcp`: this server keeps no session-scoped
/// stream and nothing to terminate.
pub async fn not_allowed() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({ "error": "This MCP server is stateless: POST JSON-RPC messages here" })),
    )
        .into_response()
}

async fn call_tool(
    state: &AdminState,
    tool: &ToolDef,
    id: Value,
    method: &str,
    uri: &str,
    body: Option<Value>,
    auth: Option<HeaderValue>,
) -> Value {
    let failed = |msg: String| protocol::rpc_result(id.clone(), protocol::tool_text(json!({ "error": msg }), true));
    let Some(router) = state.router() else {
        return failed("Admin router not ready".into());
    };
    let method = match Method::from_bytes(method.as_bytes()) {
        Ok(m) => m,
        Err(e) => return failed(e.to_string()),
    };
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    let request = match body {
        Some(b) => builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(b.to_string())),
        None => builder.body(Body::empty()),
    };
    let request = match request {
        Ok(r) => r,
        Err(e) => return failed(e.to_string()),
    };

    let response = match tokio::time::timeout(DISPATCH_TIMEOUT, router.oneshot(request)).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return failed(format!("{e:?}")),
        Err(_) => {
            warn!(tool = tool.name, "MCP tool call timed out");
            return failed("The call did not finish within 60s".into());
        }
    };
    let status = response.status();
    let event_stream = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("text/event-stream"));
    let bytes: Bytes = match tokio::time::timeout(
        DISPATCH_TIMEOUT,
        axum::body::to_bytes(response.into_body(), BODY_LIMIT),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => return failed(format!("Response too large or unreadable: {e}")),
        Err(_) => return failed("The response did not finish within 60s".into()),
    };
    protocol::tool_reply(id, status.as_u16(), &String::from_utf8_lossy(&bytes), event_stream)
}

/// So `build()` can hand the finished router to the state it was built from.
pub fn router_slot() -> Arc<std::sync::OnceLock<Router>> {
    Arc::new(std::sync::OnceLock::new())
}
