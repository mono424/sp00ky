//! An MCP server for the admin plane: every action as a tool.
//!
//! Streamable HTTP, stateless, JSON responses only. The protocol surface an
//! agent needs is five methods over JSON-RPC (`initialize`, `ping`,
//! `tools/list`, `tools/call`, and the initialized notification), which is
//! why this is written over `serde_json::Value` rather than pulling in an SDK.
//!
//! # One table, one dispatch
//!
//! Tools are rows in [`TOOLS`]: a name, the HTTP method and path of the admin
//! endpoint they stand for, and which arguments go into the path, the query
//! string, or the JSON body. A `tools/call` builds that request and sends it
//! through a clone of the admin router **in-process** (`tower::ServiceExt::
//! oneshot`), carrying the caller's own bearer. Nothing is re-implemented, so
//! a tool can never drift from the endpoint the dashboard uses, the audit log
//! lines keep the real subject, and the bearer middleware's scope rule applies
//! to agents exactly as it does to browsers.
//!
//! # Scope
//!
//! A read-only token sees only read-only tools in `tools/list` and is refused
//! by the middleware if it tries a write anyway. Destructive tools carry the
//! `destructiveHint` annotation so a client can ask before calling.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json, Router};
use serde_json::{json, Map, Value};
use tower::ServiceExt;
use tracing::{info, warn};

use super::session::Scope;
use super::{AdminState, CurrentSession};

/// Protocol revisions this server speaks. The newest is offered when a client
/// asks for one we do not know.
const PROTOCOL_VERSIONS: &[&str] = &["2024-11-05", "2025-03-26", "2025-06-18"];
const LATEST_PROTOCOL: &str = "2025-06-18";

/// A tool call is one admin request; a minute is more than any of them takes.
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(60);
/// Cap on a collected response body. The largest honest answer (a log
/// backfill of 10 000 lines) is well under this.
const BODY_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct Param {
    pub name: &'static str,
    pub ty: &'static str,
    pub desc: &'static str,
    pub required: bool,
    pub choices: &'static [&'static str],
}

const fn p(name: &'static str, ty: &'static str, desc: &'static str) -> Param {
    Param {
        name,
        ty,
        desc,
        required: false,
        choices: &[],
    }
}

const fn req(name: &'static str, ty: &'static str, desc: &'static str) -> Param {
    Param {
        name,
        ty,
        desc,
        required: true,
        choices: &[],
    }
}

const fn choice(mut p: Param, choices: &'static [&'static str]) -> Param {
    p.choices = choices;
    p
}

#[derive(Debug, Clone, Copy)]
pub struct ToolDef {
    pub name: &'static str,
    pub description: &'static str,
    pub method: &'static str,
    /// Path under `/admin/api`, with `{name}` placeholders.
    pub path: &'static str,
    pub path_params: &'static [Param],
    pub query: &'static [Param],
    /// Query parameters pinned by the server, never settable by the caller.
    pub fixed: &'static [(&'static str, &'static str)],
    pub body: &'static [Param],
    pub read_only: bool,
    pub destructive: bool,
}

const RESTART_MODES: &[&str] = &["restart", "clean", "reload"];
const ALL_MODES: &[&str] = &["restart", "clean"];
const SCHED_MODES: &[&str] = &["restart", "reclone", "rehash"];
const RUN_STATUSES: &[&str] = &["running", "success", "failed", "killed"];
const VIEW_SORTS: &[&str] = &["slowest", "newest", "rows", "updates", "errors", "active"];
/// The four the outbox table's own ASSERT allows.
const JOB_STATUSES: &[&str] = &["pending", "processing", "success", "failed"];
const JOB_ORIGINS: &[&str] = &["schedule", "workflow", "app"];

const fn t(
    name: &'static str,
    description: &'static str,
    method: &'static str,
    path: &'static str,
) -> ToolDef {
    ToolDef {
        name,
        description,
        method,
        path,
        path_params: &[],
        query: &[],
        fixed: &[],
        body: &[],
        read_only: false,
        destructive: false,
    }
}

macro_rules! tool {
    ($name:literal, $desc:literal, $method:literal, $path:literal $(, $field:ident = $value:expr)* $(,)?) => {
        ToolDef { $($field: $value,)* ..t($name, $desc, $method, $path) }
    };
}

/// The whole tool surface, in the order `tools/list` presents it.
pub const TOOLS: &[ToolDef] = &[
    // ---- Cluster ----
    tool!("admin_config", "Scheduler identity and version, whether it is linked to Sp00ky Cloud and supervised.", "GET", "/config", read_only = true),
    tool!("overview", "The whole cluster at a glance: scheduler status and end-to-end latency, every SSP with its phase and bootstrap progress, backends, running operations.", "GET", "/overview", read_only = true),
    tool!("backends_list", "Health-checked application backends with status and response time.", "GET", "/backends", read_only = true),
    tool!("backend_get", "One backend with its probe history and masked environment.", "GET", "/backends/{name}",
        path_params = &[req("name", "string", "Backend name as listed by backends_list")], read_only = true),
    // ---- Views (who is connected, and what they are watching) ----
    tool!("presence", "Live users, sessions and registered views right now, with a recent sample history and the heaviest users. A client refreshes its liveness on a 0.9 x ttl timer (about 9 minutes by default), so a closed tab decays out of these numbers rather than vanishing from them.", "GET", "/presence", read_only = true),
    tool!("views_list", "Registered live queries. Sort by slowest to find what is costing materialization time, or filter to one user or SSP.", "GET", "/views",
        query = &[
            p("limit", "integer", "Rows to return (default 100, max 500)"),
            p("user", "string", "auth_id to filter by, e.g. 'user:abc'"),
            p("ssp", "string", "SSP id. Applied after the database query, so it narrows the page rather than the total"),
            choice(p("sort", "string", "Order: slowest (default), newest, rows, updates, errors or active"), VIEW_SORTS),
            p("slow_ms", "number", "Only views whose materialization p99 reaches this"),
            p("q", "string", "Substring of the registered SurrealQL"),
            p("shared", "boolean", "Only views more than one session subscribes to"),
            p("large", "boolean", "Only views holding at least the large-view row threshold (SPKY_ADMIN_LARGE_VIEW_ROWS, default 1000); these republish every row on each cold registration"),
            p("include_expired", "boolean", "Include rows past lastActiveAt + ttl that the sweep has not reclaimed yet"),
        ], read_only = true),
    tool!("view_get", "One registered view in full: its SurrealQL and params, every subscribing session with its age, materialization percentiles, the SSP serving it with that view's memory footprint, and the other live sessions running the identical query.", "GET", "/views/{key}",
        path_params = &[req("key", "string", "The _00_query key as listed by views_list; the _00_query:<key> spelling is accepted too")], read_only = true),
    tool!("logs_recent", "Recent log lines from the scheduler or one SSP (bounded, non-streaming).", "GET", "/logs",
        query = &[
            p("source", "string", "'scheduler' (default) or 'ssp:<id>'"),
            p("backfill", "number", "How many recent lines to return (default 500, max 10000)"),
        ],
        fixed = &[("tail", "false")], read_only = true),
    tool!("operations_list", "Recent and running operations (restarts, reclones, backups, restores) with their status and progress.", "GET", "/operations", read_only = true),
    // ---- Workflows ----
    tool!("workflow_runs_list", "Workflow runs, newest first, with optional filters.", "GET", "/workflows/runs",
        query = &[
            p("name", "string", "Only runs of this workflow"),
            p("schedule", "string", "Only runs owned by this schedule"),
            choice(p("status", "string", "Only runs in this status"), RUN_STATUSES),
            p("rerun_of", "string", "Only reruns of this run id"),
            p("limit", "number", "Max rows (default 50, max 500)"),
        ], read_only = true),
    tool!("workflow_run_get", "One workflow run with its steps, DAG and payloads.", "GET", "/workflows/runs/{id}",
        path_params = &[req("id", "string", "Run id, e.g. _00_workflow_run:abc")], read_only = true),
    tool!("workflow_run_cancel", "Cancel a running workflow run: kills its in-flight jobs now and marks it killed.", "POST", "/workflows/runs/{id}/cancel",
        path_params = &[req("id", "string", "Run id")]),
    tool!("workflow_run_rerun", "Start a new ad-hoc run with the same DAG and input as this one (recorded as rerun_of).", "POST", "/workflows/runs/{id}/rerun",
        path_params = &[req("id", "string", "Run id")]),
    tool!("workflow_run_retry", "Retry a failed or killed run from its failed steps; successful steps keep their output.", "POST", "/workflows/runs/{id}/retry",
        path_params = &[req("id", "string", "Run id")]),
    tool!("schedules_list", "Every schedule with cadence, pause state, next and last fire.", "GET", "/schedules", read_only = true),
    tool!("schedule_get", "One schedule with its recent fires and hourly outcome tally.", "GET", "/schedules/{name}",
        path_params = &[req("name", "string", "Schedule name")], read_only = true),
    tool!("schedule_pause", "Pause a schedule: no fires and no queued triggers until resumed.", "POST", "/schedules/{name}/pause",
        path_params = &[req("name", "string", "Schedule name")]),
    tool!("schedule_resume", "Resume a paused schedule.", "POST", "/schedules/{name}/resume",
        path_params = &[req("name", "string", "Schedule name")]),
    tool!("schedule_trigger", "Fire a schedule once now (refused while paused or config-disabled).", "POST", "/schedules/{name}/trigger",
        path_params = &[req("name", "string", "Schedule name")]),
    // ---- Jobs ----
    tool!("jobs_list", "Outbox jobs across every table, newest activity first, with the queue totals. Filter by origin to separate a schedule's fires from a workflow's steps and from jobs the application created itself, which have no other surface. Unfiltered it is served from the scheduler's sampler and costs the database nothing.", "GET", "/jobs",
        query = &[
            choice(p("status", "string", "Only jobs in this status"), JOB_STATUSES),
            p("table", "string", "Outbox table name; defaults to every table in _00_retention.job_tables"),
            choice(p("origin", "string", "Where the job came from: schedule (a kind: job fire), workflow (a step) or app (the application's own code)"), JOB_ORIGINS),
            p("q", "string", "Substring of the job's path or its id"),
            p("limit", "number", "Max rows (default 50, max 500)"),
        ], read_only = true),
    tool!("job_get", "One outbox job in full: status, path, payload, result, every attempt in `errors`, retry budget, assignee, lease, and the schedule fire or workflow step it came from.", "GET", "/jobs/{id}",
        path_params = &[req("id", "string", "Job record id, e.g. job:abc")], read_only = true),
    tool!("job_kill", "Kill one outbox job: cancels it in flight or fails it before it starts.", "POST", "/jobs/{id}/kill",
        path_params = &[req("id", "string", "Job record id, e.g. job:abc")]),
    tool!("job_retry", "Retry a terminal (failed or succeeded) outbox job on one SSP.", "POST", "/jobs/{id}/retry",
        path_params = &[req("id", "string", "Job record id")]),
    tool!("jobs_clear", "Delete terminal outbox rows in batches. `processing` jobs are never touched at either width, so work in flight is safe. Returns how many went per table and whether more is left.", "POST", "/jobs/clear",
        body = &[
            p("table", "string", "One outbox table; defaults to every table"),
            p("all", "boolean", "Also delete pending jobs, not just success and failed"),
        ], destructive = true),
    // ---- Restart ----
    tool!("ssp_restart", "Restart one SSP. 'restart' exits and relaunches from its snapshot; 'clean' drops the snapshot first for a cold rebuild; 'reload' rebuilds in place without exiting.", "POST", "/ssps/{id}/restart",
        path_params = &[req("id", "string", "SSP id as listed by overview")],
        body = &[choice(p("mode", "string", "restart (default), clean or reload"), RESTART_MODES)]),
    tool!("ssps_restart_all", "Restart every SSP, one at a time by default so clients never lose them all at once.", "POST", "/ssps/restart-all",
        body = &[
            choice(p("mode", "string", "restart (default) or clean"), ALL_MODES),
            p("rolling", "boolean", "One SSP at a time (default true); false takes them all down together"),
        ]),
    tool!("scheduler_restart", "Restart the scheduler process ('restart', the supervisor relaunches it), or repair its replica in place ('reclone' refetches everything from upstream, 'rehash' recomputes snapshot hashes).", "POST", "/scheduler/restart",
        body = &[choice(p("mode", "string", "restart (default), reclone or rehash"), SCHED_MODES)], destructive = true),
    tool!("cloud_deployment", "The deployment as Sp00ky Cloud sees it (needs the cloud link).", "GET", "/cloud/deployment", read_only = true),
    tool!("cloud_restart", "Ask Sp00ky Cloud to recreate containers: optionally pull newer images (upgrade), wipe the scheduler volume (clean) or bounce SurrealDB (surreal). Needs the cloud link.", "POST", "/cloud/restart",
        body = &[
            p("roles", "array", "Roles to restart: scheduler, ssp, surrealdb, backend, frontend (default scheduler + ssp)"),
            p("upgrade", "boolean", "Pull the latest scheduler and SSP images"),
            p("clean", "boolean", "Wipe the scheduler's volume (replica and WAL) before recreating it"),
            p("surreal", "boolean", "Also restart the SurrealDB container"),
        ], destructive = true),
    // ---- Backups ----
    tool!("backups_list", "The backup catalog, schedule and retention, plus what this scheduler is doing right now.", "GET", "/backups", read_only = true),
    tool!("backup_create", "Take a backup now: a gzipped SurrealDB export uploaded to the bucket.", "POST", "/backups",
        body = &[p("name", "string", "Optional label")]),
    tool!("backup_restore", "Restore a backup. Wipes the database, evicts every SSP, and requires migrations to be re-run afterwards.", "POST", "/backups/{id}/restore",
        path_params = &[req("id", "string", "Backup id from backups_list")], destructive = true),
    tool!("backup_restore_status", "Progress of the restore of a backup, with the scheduler's own stages.", "GET", "/backups/{id}/restore",
        path_params = &[req("id", "string", "Backup id")], read_only = true),
    tool!("backup_delete", "Delete a backup from the catalog (needs the cloud link).", "DELETE", "/backups/{id}",
        path_params = &[req("id", "string", "Backup id")], destructive = true),
    tool!("backups_configure", "Set the scheduled-backup policy (needs the cloud link).", "PUT", "/backups/config",
        body = &[
            p("enabled", "boolean", "Whether scheduled backups run"),
            p("schedule", "string", "Standard 5-field cron, e.g. '0 3 * * *'"),
            p("retention", "number", "How many completed backups to keep"),
        ]),
];

pub fn find_tool(name: &str) -> Option<&'static ToolDef> {
    TOOLS.iter().find(|t| t.name == name)
}

fn json_type(ty: &str) -> Value {
    match ty {
        "array" => json!({ "type": "array", "items": { "type": "string" } }),
        other => json!({ "type": other }),
    }
}

/// The flat argument object MCP clients fill in: path, query and body
/// parameters share one namespace, as in the cloud MCP server.
pub fn input_schema(tool: &ToolDef) -> Value {
    let mut props = Map::new();
    let mut required = Vec::new();
    for param in tool.path_params.iter().chain(tool.query).chain(tool.body) {
        let mut schema = json_type(param.ty);
        schema["description"] = json!(param.desc);
        if !param.choices.is_empty() {
            schema["enum"] = json!(param.choices);
        }
        props.insert(param.name.to_string(), schema);
        if param.required {
            required.push(param.name);
        }
    }
    json!({
        "type": "object",
        "properties": props,
        "required": required,
        "additionalProperties": false,
    })
}

fn tool_json(tool: &ToolDef) -> Value {
    json!({
        "name": tool.name,
        "description": tool.description,
        "inputSchema": input_schema(tool),
        "annotations": {
            "title": tool.name.replace('_', " "),
            "readOnlyHint": tool.read_only,
            "destructiveHint": tool.destructive,
            "idempotentHint": tool.read_only,
            "openWorldHint": false,
        },
    })
}

/// Tools this scope may call. The middleware would refuse the write anyway;
/// hiding it keeps an agent from planning around a tool it cannot use.
pub fn tools_for(scope: Scope) -> Vec<&'static ToolDef> {
    TOOLS
        .iter()
        .filter(|t| scope == Scope::Full || t.read_only)
        .collect()
}

/// Turn a call's arguments into the admin request it stands for.
///
/// Returns `(method, uri, body)`; the error is the sentence for the agent.
pub fn build_request(
    tool: &ToolDef,
    args: &Map<String, Value>,
) -> Result<(Method, String, Option<Value>), String> {
    let mut path = tool.path.to_string();
    for param in tool.path_params {
        let value = args.get(param.name).and_then(scalar_string);
        match value {
            Some(v) => {
                path = path.replace(&format!("{{{}}}", param.name), &percent_encode(&v));
            }
            None if param.required => {
                return Err(format!("Missing required argument '{}'", param.name));
            }
            None => {}
        }
    }
    let mut query: Vec<(String, String)> = tool
        .fixed
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    for param in tool.query {
        if let Some(v) = args.get(param.name).and_then(scalar_string) {
            query.push((param.name.to_string(), v));
        } else if param.required {
            return Err(format!("Missing required argument '{}'", param.name));
        }
    }
    let uri = if query.is_empty() {
        format!("/admin/api{path}")
    } else {
        let qs = query
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        format!("/admin/api{path}?{qs}")
    };
    let body = if tool.body.is_empty() {
        None
    } else {
        let mut b = Map::new();
        for param in tool.body {
            if let Some(v) = args.get(param.name) {
                if !v.is_null() {
                    b.insert(param.name.to_string(), v.clone());
                }
            } else if param.required {
                return Err(format!("Missing required argument '{}'", param.name));
            }
        }
        // Handlers that take a required `Json<T>` need an object, and the
        // optional ones accept one, so an empty body is `{}` rather than
        // nothing at all.
        Some(Value::Object(b))
    };
    let method = Method::from_bytes(tool.method.as_bytes()).map_err(|e| e.to_string())?;
    Ok((method, uri, body))
}

fn scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Percent-encode a path segment or query component. Unreserved characters
/// pass; everything else, including `/` and `:`, is escaped, because a record
/// id like `_00_workflow_run:abc` is one segment.
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Fold an SSE body into one JSON value: `data:` frames become an array, and
/// a `dropped` event becomes a note rather than an error.
pub fn collapse_sse(text: &str) -> Value {
    let mut items = Vec::new();
    let mut truncated: Option<Value> = None;
    for frame in text.split("\n\n") {
        let mut event = "message";
        let mut data = Vec::new();
        for line in frame.lines() {
            if line.starts_with(':') {
                continue;
            }
            if let Some(e) = line.strip_prefix("event:") {
                event = e.trim();
            } else if let Some(d) = line.strip_prefix("data:") {
                data.push(d.trim_start());
            }
        }
        if data.is_empty() {
            continue;
        }
        let joined = data.join("\n");
        let value = serde_json::from_str(&joined).unwrap_or(Value::String(joined));
        if event == "dropped" {
            truncated = Some(value);
        } else {
            items.push(value);
        }
    }
    match truncated {
        Some(t) => json!({ "items": items, "truncated": t }),
        None => Value::Array(items),
    }
}

// ---------------------------------------------------------------------------
// JSON-RPC
// ---------------------------------------------------------------------------

fn rpc_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message.into() } })
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn tool_text(value: Value, is_error: bool) -> Value {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
    json!({ "content": [{ "type": "text", "text": text }], "isError": is_error })
}

fn negotiate(requested: Option<&str>) -> &'static str {
    match requested {
        Some(v) => PROTOCOL_VERSIONS
            .iter()
            .copied()
            .find(|k| *k == v)
            .unwrap_or(LATEST_PROTOCOL),
        None => LATEST_PROTOCOL,
    }
}

/// `POST /admin/api/mcp`
pub async fn handle(
    State(state): State<AdminState>,
    Extension(CurrentSession(session)): Extension<CurrentSession>,
    request: Request,
) -> Response {
    let auth = request.headers().get(header::AUTHORIZATION).cloned();
    let body = match axum::body::to_bytes(request.into_body(), BODY_LIMIT).await {
        Ok(b) => b,
        Err(_) => return rpc_response(rpc_error(Value::Null, -32700, "Request body unreadable")),
    };
    let message: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return rpc_response(rpc_error(Value::Null, -32700, format!("Parse error: {e}"))),
    };
    if message.is_array() {
        return rpc_response(rpc_error(
            Value::Null,
            -32600,
            "Batched requests are not supported; send one message per request",
        ));
    }
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
    let params = message
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    // A message without an id is a notification (or a stray response); the
    // transport rule is 202 with nothing to say.
    if id.is_null() || method.starts_with("notifications/") {
        return StatusCode::ACCEPTED.into_response();
    }

    let result = match method {
        "initialize" => {
            let requested = params.get("protocolVersion").and_then(Value::as_str);
            info!(
                subject = %session.subject,
                client = ?params.get("clientInfo").and_then(|c| c.get("name")).and_then(|n| n.as_str()),
                "MCP session initialized"
            );
            rpc_result(
                id,
                json!({
                    "protocolVersion": negotiate(requested),
                    "capabilities": { "tools": { "listChanged": false } },
                    "serverInfo": { "name": "spky-admin", "version": env!("CARGO_PKG_VERSION") },
                    "instructions": instructions(&state, session.scope),
                }),
            )
        }
        "ping" => rpc_result(id, json!({})),
        "tools/list" => rpc_result(
            id,
            json!({ "tools": tools_for(session.scope).into_iter().map(tool_json).collect::<Vec<_>>() }),
        ),
        "tools/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params
                .get("arguments")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            match find_tool(name) {
                None => rpc_error(id, -32602, format!("Unknown tool '{name}'")),
                Some(tool) if session.scope == Scope::Read && !tool.read_only => rpc_result(
                    id,
                    tool_text(
                        json!({ "error": format!("'{}' is a write action and this token is read-only", tool.name) }),
                        true,
                    ),
                ),
                Some(tool) => {
                    info!(tool = tool.name, by = %session.subject, "MCP tool call");
                    rpc_result(id, call_tool(&state, tool, &args, auth).await)
                }
            }
        }
        "resources/list" => rpc_result(id, json!({ "resources": [] })),
        "resources/templates/list" => rpc_result(id, json!({ "resourceTemplates": [] })),
        "prompts/list" => rpc_result(id, json!({ "prompts": [] })),
        other => rpc_error(id, -32601, format!("Method not found: {other}")),
    };
    rpc_response(result)
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
    args: &Map<String, Value>,
    auth: Option<HeaderValue>,
) -> Value {
    let (method, uri, body) = match build_request(tool, args) {
        Ok(parts) => parts,
        Err(msg) => return tool_text(json!({ "error": msg }), true),
    };
    let Some(router) = state.router() else {
        return tool_text(json!({ "error": "Admin router not ready" }), true);
    };
    let mut builder = Request::builder().method(method).uri(&uri);
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
        Err(e) => return tool_text(json!({ "error": e.to_string() }), true),
    };

    let response = match tokio::time::timeout(DISPATCH_TIMEOUT, router.oneshot(request)).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return tool_text(json!({ "error": format!("{e:?}") }), true),
        Err(_) => {
            warn!(tool = tool.name, "MCP tool call timed out");
            return tool_text(
                json!({ "error": "The call did not finish within 60s" }),
                true,
            );
        }
    };
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes: Bytes = match tokio::time::timeout(
        DISPATCH_TIMEOUT,
        axum::body::to_bytes(response.into_body(), BODY_LIMIT),
    )
    .await
    {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => {
            return tool_text(
                json!({ "error": format!("Response too large or unreadable: {e}") }),
                true,
            )
        }
        Err(_) => {
            return tool_text(
                json!({ "error": "The response did not finish within 60s" }),
                true,
            )
        }
    };
    let text = String::from_utf8_lossy(&bytes).to_string();

    let value = if content_type.contains("text/event-stream") {
        collapse_sse(&text)
    } else if text.trim().is_empty() {
        json!({ "status": status.as_u16() })
    } else {
        serde_json::from_str(&text).unwrap_or(Value::String(text))
    };

    if status.is_success() {
        tool_text(value, false)
    } else {
        let message = value
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        tool_text(
            json!({ "error": format!("API error (HTTP {}): {}", status.as_u16(), message), "status": status.as_u16() }),
            true,
        )
    }
}

/// So `build()` can hand the finished router to the state it was built from.
pub fn router_slot() -> Arc<std::sync::OnceLock<Router>> {
    Arc::new(std::sync::OnceLock::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tool_name_is_unique_and_snake_case() {
        let mut seen = std::collections::HashSet::new();
        for t in TOOLS {
            assert!(seen.insert(t.name), "duplicate tool {}", t.name);
            assert!(
                t.name.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{} is not snake_case",
                t.name
            );
            assert!(
                !t.description.contains('\u{2014}'),
                "{} has an em dash",
                t.name
            );
        }
    }

    #[test]
    fn schemas_flatten_path_query_and_body() {
        let t = find_tool("ssp_restart").unwrap();
        let s = input_schema(t);
        assert_eq!(s["required"], json!(["id"]));
        assert_eq!(s["properties"]["mode"]["enum"], json!(RESTART_MODES));
        let t = find_tool("logs_recent").unwrap();
        let s = input_schema(t);
        assert!(
            s["properties"].get("tail").is_none(),
            "fixed params are not arguments"
        );
        assert!(s["properties"].get("backfill").is_some());
    }

    #[test]
    fn read_only_scope_hides_writes() {
        let names: Vec<&str> = tools_for(Scope::Read).iter().map(|t| t.name).collect();
        assert!(names.contains(&"overview"));
        assert!(!names.contains(&"scheduler_restart"));
        assert_eq!(tools_for(Scope::Full).len(), TOOLS.len());
    }

    #[test]
    fn requests_are_built_with_escaped_ids_and_fixed_query() {
        let t = find_tool("workflow_run_cancel").unwrap();
        let mut args = Map::new();
        args.insert("id".into(), json!("_00_workflow_run:a-b"));
        let (m, uri, body) = build_request(t, &args).unwrap();
        assert_eq!(m, Method::POST);
        assert_eq!(
            uri,
            "/admin/api/workflows/runs/_00_workflow_run%3Aa-b/cancel"
        );
        assert!(body.is_none());

        let t = find_tool("logs_recent").unwrap();
        let mut args = Map::new();
        args.insert("source".into(), json!("ssp:ssp-1"));
        args.insert("backfill".into(), json!(20));
        let (_, uri, _) = build_request(t, &args).unwrap();
        assert!(uri.starts_with("/admin/api/logs?tail=false&"), "{uri}");
        assert!(
            uri.contains("source=ssp%3Assp-1") && uri.contains("backfill=20"),
            "{uri}"
        );

        let t = find_tool("ssps_restart_all").unwrap();
        let (_, _, body) = build_request(t, &Map::new()).unwrap();
        assert_eq!(body, Some(json!({})), "an empty body is still an object");

        let t = find_tool("backend_get").unwrap();
        assert!(build_request(t, &Map::new()).unwrap_err().contains("name"));
    }

    #[test]
    fn sse_collapses_to_an_array_and_flags_drops() {
        let text = "event: line\ndata: {\"a\":1}\n\nevent: dropped\ndata: 3\n\n: keep-alive\n\nevent: line\ndata: {\"a\":2}\n\n";
        let v = collapse_sse(text);
        assert_eq!(v["items"], json!([{ "a": 1 }, { "a": 2 }]));
        assert_eq!(v["truncated"], json!(3));
        assert_eq!(collapse_sse("data: [1]\n\n"), json!([[1]]));
    }

    #[test]
    fn protocol_version_is_negotiated_not_invented() {
        assert_eq!(negotiate(Some("2024-11-05")), "2024-11-05");
        assert_eq!(negotiate(Some("2030-01-01")), LATEST_PROTOCOL);
        assert_eq!(negotiate(None), LATEST_PROTOCOL);
    }
}
