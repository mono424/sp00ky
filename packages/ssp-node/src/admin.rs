//! The admin API (`/admin/api`) on the SSP node, for a project with no
//! scheduler (a Cloudflare project): the dashboard's and the MCP clients'
//! surface.
//!
//! Same wire contract as the scheduler's: the session tokens are
//! `ssp_protocol::admin_session` (a token minted by either host verifies on
//! the other with the same project secret), the job, schedule and workflow
//! answers are `schedule_core::admin`, and the MCP tool table is
//! `ssp_protocol::admin_mcp`. What only a cluster has (other SSPs, a replica,
//! log rings, pools, backups, live streams) is answered 501 with the reason,
//! and the MCP server lists only the tools this host serves.
//!
//! Nothing here runs in the background: a Durable Object that sampled jobs
//! on a timer could never sleep. Every answer is read when it is asked for,
//! and the overview's job totals are cached for a few seconds so a dashboard
//! polling it does not rescan the outbox on every tick.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde_json::{json, Value};
use ssp_protocol::admin_mcp::{self as mcp, Rpc, ServerInfo, ToolDef};
use ssp_protocol::admin_session::{self as signed, Claims, Scope, SessionMode};

use schedule_core::admin as core;

use crate::api::{ApiRequest, ApiResponse, Method};
use crate::node::SspNode;
use crate::schedules::PortDb;

/// Every admin route starts with this. The shell passes the query string on
/// these paths (and only these).
pub const PREFIX: &str = "/admin/api";

/// A signed-in session lasts this long, as on the scheduler.
const SESSION_TTL_SECS: u64 = 8 * 60 * 60;
/// MCP tokens: 90 days unless asked otherwise, never more than a year.
const DEFAULT_TOKEN_DAYS: u64 = 90;
const MAX_TOKEN_DAYS: u64 = 365;

/// Failed logins allowed inside [`LOGIN_WINDOW_MS`], across every caller (the
/// node does not see client addresses). Generous enough that a person who
/// mistyped is never locked out, tight enough to make guessing pointless.
const MAX_LOGIN_FAILURES: usize = 20;
const LOGIN_WINDOW_MS: u64 = 5 * 60 * 1000;

/// How long the overview's job totals are served from memory.
const JOB_TOTALS_TTL_MS: u64 = 10_000;

/// Host settings, read by the shell from its environment.
#[derive(Debug, Clone, Default)]
pub struct AdminSettings {
    /// `SPKY_ADMIN_PASSWORD`: offers a break-glass login when set.
    pub password: Option<String>,
    /// `SPKY_ADMIN_ACCESS`: the record access a roster login signs in
    /// through. Discovered from `INFO FOR DB` when absent.
    pub access: Option<String>,
    /// The project's slug, for the MCP server's instructions.
    pub project_slug: Option<String>,
}

/// The admin API's state on one node: its settings and what it remembers
/// between requests. Lost on restart, which costs a revoked token its
/// revocation for the rest of its expiry (the scheduler makes the same trade).
#[derive(Default)]
pub struct AdminState {
    pub settings: AdminSettings,
    revoked: Mutex<HashSet<String>>,
    failures: Mutex<VecDeque<u64>>,
    job_totals: Mutex<Option<(u64, Value)>>,
}

impl AdminState {
    pub fn new(settings: AdminSettings) -> Self {
        Self { settings, ..Default::default() }
    }
}

/// Who is asking.
struct Session {
    subject: String,
    label: String,
    mode: SessionMode,
    scope: Scope,
}

fn now_secs() -> u64 {
    crate::now_epoch_ms() / 1000
}

fn json_response(status: u16, body: Value) -> ApiResponse {
    ApiResponse::json(status, body)
}

fn error(status: u16, message: impl Into<String>) -> ApiResponse {
    json_response(status, json!({ "error": message.into() }))
}

fn from_core(result: core::AdminResult<Value>, ok: u16) -> ApiResponse {
    match result {
        Ok(body) => json_response(ok, body),
        Err(e) => error(e.status, e.message),
    }
}

/// The CORS answer to a preflight on an admin path. The token travels in a
/// header the browser only sends when the page holds it, so `*` without
/// credentials gives a hostile page nothing.
pub fn preflight() -> ApiResponse {
    let mut resp = ApiResponse::text(204, "text/plain", String::new());
    resp.headers.extend([
        ("Access-Control-Allow-Origin", "*".to_string()),
        ("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS".to_string()),
        ("Access-Control-Allow-Headers", "Authorization, Content-Type".to_string()),
        ("Access-Control-Max-Age", "86400".to_string()),
    ]);
    resp
}

/// Whether a request path (query string included) is an admin route.
pub fn is_admin_path(path: &str) -> bool {
    path == PREFIX || path.starts_with("/admin/api/") || path.starts_with("/admin/api?")
}

/// Whether a request may have changed what the schedule sweep must do next,
/// so the runtime re-arms it.
pub fn touches_schedules(path: &str) -> bool {
    path.starts_with("/admin/api/schedules/") || path.starts_with("/admin/api/workflows/runs/")
}

// =============================================================
// Request parsing
// =============================================================

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
                match hex {
                    Some(b) => {
                        out.push(b);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `(segments under /admin/api, decoded query pairs)`.
fn split_path(path: &str) -> (Vec<String>, Vec<(String, String)>) {
    let rest = path.strip_prefix(PREFIX).unwrap_or(path);
    let (route, query) = rest.split_once('?').unwrap_or((rest, ""));
    let segments = route.split('/').filter(|s| !s.is_empty()).map(percent_decode).collect();
    let pairs = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect();
    (segments, pairs)
}

fn body_json(req: &ApiRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

// =============================================================
// Sessions
// =============================================================

impl SspNode {
    /// The session a bearer names: the node's own secret (the control
    /// plane), or a signed admin token that has not expired or been revoked.
    fn admin_session(&self, bearer: Option<&str>) -> Option<Session> {
        let bearer = bearer?.trim();
        // No secret, no sessions: a key derived from "" would be public.
        if self.auth_secret.is_empty() || bearer.is_empty() {
            return None;
        }
        if ssp_protocol::impersonation::constant_time_eq(bearer.as_bytes(), self.auth_secret.as_bytes()) {
            return Some(Session {
                subject: "node".into(),
                label: "Control plane".into(),
                mode: SessionMode::Node,
                scope: Scope::Full,
            });
        }
        if self.admin.revoked.lock().ok()?.contains(bearer) {
            return None;
        }
        let claims = signed::verify(&signed::session_key(&self.auth_secret), bearer, now_secs())?;
        Some(Session { subject: claims.subject, label: claims.label, mode: claims.mode, scope: claims.scope })
    }

    fn mint_admin_token(&self, subject: String, label: String, mode: SessionMode, scope: Scope, ttl_secs: u64) -> (String, u64) {
        static NONCE: AtomicU64 = AtomicU64::new(0);
        let expires_at_secs = now_secs() + ttl_secs;
        let nonce = format!("{:x}.{:x}", crate::now_epoch_ms(), NONCE.fetch_add(1, Ordering::Relaxed));
        let claims = Claims { subject, label, mode, scope, expires_at_secs };
        (signed::mint(&signed::session_key(&self.auth_secret), &claims, &nonce), expires_at_secs)
    }

    /// Count a failed login; `false` once the window is full.
    fn admin_login_allowed(&self) -> bool {
        let now = crate::now_epoch_ms();
        let Ok(mut failures) = self.admin.failures.lock() else { return false };
        while failures.front().is_some_and(|t| now.saturating_sub(*t) > LOGIN_WINDOW_MS) {
            failures.pop_front();
        }
        failures.len() < MAX_LOGIN_FAILURES
    }

    fn admin_login_failed(&self) {
        if let Ok(mut failures) = self.admin.failures.lock() {
            failures.push_back(crate::now_epoch_ms());
        }
    }
}

/// The `ID` claim of a SurrealDB token: the record the session signed in as.
fn record_id_from_jwt(token: &str) -> Option<String> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    claims.get("ID").and_then(Value::as_str).map(str::to_string)
}

// =============================================================
// Dispatch
// =============================================================

/// What this host serves, by method and route template (`{x}` for a
/// segment). The MCP server lists exactly these tools.
fn served(method: &str, template: &str) -> bool {
    matches!(
        (method, template),
        ("GET", "/config")
            | ("GET", "/overview")
            | ("GET", "/backends")
            | ("GET", "/backends/{name}")
            | ("GET", "/jobs")
            | ("GET", "/jobs/{id}")
            | ("POST", "/jobs/clear")
            | ("POST", "/jobs/{id}/kill")
            | ("POST", "/jobs/{id}/retry")
            | ("GET", "/schedules")
            | ("GET", "/schedules/{name}")
            | ("POST", "/schedules/{name}/pause")
            | ("POST", "/schedules/{name}/resume")
            | ("POST", "/schedules/{name}/trigger")
            | ("POST", "/schedules/{name}/release")
            | ("GET", "/workflows/runs")
            | ("GET", "/workflows/runs/{id}")
            | ("POST", "/workflows/runs/{id}/cancel")
            | ("POST", "/workflows/runs/{id}/rerun")
            | ("POST", "/workflows/runs/{id}/retry")
    )
}

fn serves_tool(tool: &ToolDef) -> bool {
    served(tool.method, tool.path)
}

/// Routes only a cluster has. Answered 501 with the reason rather than 404,
/// so the dashboard can say why a page is empty.
const CLUSTER_ONLY: &[&str] = &[
    "presence", "views", "incidents", "logs", "operations", "ssps", "scheduler", "cloud", "backups", "pools",
    "machines",
];

impl SspNode {
    /// Answer one `/admin/api` request.
    pub async fn admin_route(&self, req: &ApiRequest) -> ApiResponse {
        let mut resp = self.admin_dispatch(req).await;
        resp.headers.push(("Access-Control-Allow-Origin", "*".to_string()));
        resp.headers.push(("Cache-Control", "no-store".to_string()));
        resp
    }

    async fn admin_dispatch(&self, req: &ApiRequest) -> ApiResponse {
        let (segments, pairs) = split_path(&req.path);
        let seg: Vec<&str> = segments.iter().map(String::as_str).collect();

        // The two routes that need no session.
        match (req.method, seg.as_slice()) {
            (Method::Get, ["config"]) => return self.admin_config(),
            (Method::Post, ["session"]) => return self.admin_login(req).await,
            _ => {}
        }

        let Some(session) = self.admin_session(req.bearer.as_deref()) else {
            return error(401, "Not signed in");
        };
        let is_mcp = seg.as_slice() == ["mcp"];
        if session.scope == Scope::Read && req.method != Method::Get && !is_mcp {
            return error(403, "This token is read-only");
        }

        let db = PortDb(self.platform.db.clone());
        match (req.method, seg.as_slice()) {
            (Method::Get, ["me"]) => json_response(
                200,
                json!({ "subject": session.subject, "label": session.label, "mode": session.mode, "scope": session.scope }),
            ),
            (Method::Post, ["logout"]) => {
                if let (Some(token), Ok(mut revoked)) = (req.bearer.as_deref(), self.admin.revoked.lock()) {
                    if session.mode != SessionMode::Node {
                        revoked.insert(token.trim().to_string());
                    }
                }
                ApiResponse::text(204, "text/plain", String::new())
            }
            (Method::Post, ["tokens"]) => self.admin_mint_token(&session, &body_json(req)),
            (Method::Delete, ["tokens"]) => self.admin_revoke_token(&session, &body_json(req)),
            (Method::Post, ["mcp"]) => self.admin_mcp(&session, req).await,
            (Method::Get, ["overview"]) => self.admin_overview(&db).await,
            (Method::Get, ["backends"]) => json_response(200, json!({ "backends": self.admin_backends() })),
            (Method::Get, ["backends", name]) => match self.admin_backends().into_iter().find(|b| b["name"] == *name) {
                Some(mut b) => {
                    b["history"] = json!([]);
                    b["env"] = Value::Null;
                    json_response(200, b)
                }
                None => error(404, format!("No backend named '{name}'")),
            },

            // ---- Jobs ----
            (Method::Get, ["jobs"]) => from_core(core::list_jobs(&db, &core::JobsQuery::from_pairs(&pairs)).await, 200),
            (Method::Get, ["jobs", "stream"]) | (Method::Get, ["workflows", "stream"]) => error(
                501,
                "Live streams need a scheduler; this project has none, so the page polls instead",
            ),
            (Method::Post, ["jobs", "clear"]) => {
                let body: core::ClearBody = serde_json::from_slice(&req.body).unwrap_or_default();
                let out = core::jobs_clear(&db, &body).await;
                if let Ok(v) = &out {
                    tracing::info!(total = %v["total"], all = body.all, by = %session.subject, "Terminal jobs cleared from the admin API");
                }
                from_core(out, 200)
            }
            (Method::Get, ["jobs", id]) => from_core(core::job_detail(&db, id).await, 200),
            (Method::Post, ["jobs", id, "kill"]) => {
                tracing::info!(job = %id, by = %session.subject, "Job kill from the admin API");
                self.admin_job_action(id, true).await
            }
            (Method::Post, ["jobs", id, "retry"]) => {
                tracing::info!(job = %id, by = %session.subject, "Job retry from the admin API");
                self.admin_job_action(id, false).await
            }

            // ---- Schedules ----
            (Method::Get, ["schedules"]) => from_core(core::list_schedules(&db).await, 200),
            (Method::Get, ["schedules", name]) => from_core(core::schedule_detail(&db, name).await, 200),
            (Method::Post, ["schedules", name, action]) => {
                let out = match *action {
                    "pause" => core::set_paused(&db, name, true).await,
                    "resume" => core::set_paused(&db, name, false).await,
                    "trigger" => core::trigger(&db, name).await,
                    "release" => match serde_json::from_slice::<core::ReleaseBody>(&req.body) {
                        Ok(body) => core::release_key(&db, name, &body.key).await,
                        Err(_) => return error(400, "A body of { \"key\": \"...\" } is required"),
                    },
                    other => return error(404, format!("No schedule action '{other}'")),
                };
                if out.is_ok() {
                    tracing::info!(schedule = %name, action, by = %session.subject, "Schedule action from the admin API");
                }
                from_core(out, 200)
            }

            // ---- Workflow runs ----
            (Method::Get, ["workflows", "runs"]) => {
                from_core(core::list_runs(&db, &core::RunsQuery::from_pairs(&pairs)).await, 200)
            }
            (Method::Get, ["workflows", "runs", id]) => from_core(core::run_detail(&db, id).await, 200),
            (Method::Post, ["workflows", "runs", id, action]) => {
                let Some(engine) = self.schedule_engine.as_deref() else {
                    return error(501, "Schedules and workflows are not enabled on this node");
                };
                let (out, ok) = match *action {
                    "cancel" => (core::cancel_run(engine, id).await, 202),
                    "rerun" => (core::rerun_run(engine, id).await, 201),
                    "retry" => (core::retry_run(engine, id).await, 200),
                    other => return error(404, format!("No workflow action '{other}'")),
                };
                if out.is_ok() {
                    tracing::info!(run = %id, action, by = %session.subject, "Workflow action from the admin API");
                }
                from_core(out, ok)
            }

            (_, [first, ..]) if CLUSTER_ONLY.contains(first) => error(
                501,
                format!("'{first}' needs a scheduler; this project runs without one (a Cloudflare project)"),
            ),
            _ => error(404, "No such admin route"),
        }
    }

    fn admin_config(&self) -> ApiResponse {
        json_response(
            200,
            json!({
                // The dashboard names its host by this; a node has no scheduler.
                "scheduler_id": self.ssp_id,
                "host": "ssp",
                "version": self.version,
                "breakglass_available": self.admin.settings.password.is_some(),
                "cloud_linked": false,
                "supervised": true,
                "sessions_persistent": !self.auth_secret.is_empty(),
                "project_slug": self.admin.settings.project_slug,
                // Streams need a background sampler; a node answers by poll.
                "streams": false,
            }),
        )
    }

    async fn admin_login(&self, req: &ApiRequest) -> ApiResponse {
        if self.auth_secret.is_empty() {
            return error(503, "This node has no SPKY_AUTH_SECRET to sign sessions with");
        }
        if !self.admin_login_allowed() {
            return error(429, "Too many attempts. Try again later.");
        }
        let body = body_json(req);
        let password = body.get("password").and_then(Value::as_str).unwrap_or("");
        let username = body.get("username").and_then(Value::as_str).filter(|u| !u.is_empty());

        let Some(username) = username else {
            // Break-glass: password only, and only when one is configured.
            let accepted = self.admin.settings.password.as_deref().is_some_and(|expected| {
                ssp_protocol::impersonation::constant_time_eq(expected.as_bytes(), password.as_bytes())
            });
            if !accepted {
                self.admin_login_failed();
                tracing::warn!("Break-glass admin login failed");
                return error(401, "Invalid credentials");
            }
            tracing::info!("Break-glass admin login");
            return self.admin_login_answer("breakglass".into(), "Break-glass".into(), SessionMode::Breakglass);
        };

        let db = self.platform.db.as_ref();
        let accesses = match &self.admin.settings.access {
            Some(ac) => vec![ac.clone()],
            None => record_accesses(db).await,
        };
        let mut jwt = None;
        'access: for access in &accesses {
            // Apps name the identifier themselves: `$username` in the
            // templates, `$email` in plenty of real schemas.
            for field in ["username", "email"] {
                match db.signin_record(access, json!({ field: username, "password": password })).await {
                    Ok(Some(token)) => {
                        jwt = Some(token);
                        break 'access;
                    }
                    Ok(None) => {}
                    // A database we could not reach is not a credential verdict.
                    Err(e) => {
                        tracing::warn!(error = %e, "Admin login could not reach the database");
                        return error(503, "The project's database could not be reached");
                    }
                }
            }
        }
        let Some(user_id) = jwt.as_deref().and_then(record_id_from_jwt) else {
            self.admin_login_failed();
            tracing::warn!(username, "Admin login failed: no access method accepted the credentials");
            return error(401, "Invalid credentials");
        };
        // Root, deliberately: one answer however the app's own permissions
        // on `_00_admin` are shaped.
        let on_roster = db
            .query("SELECT VALUE id FROM _00_admin WHERE <string>user = $uid LIMIT 1;", &[("uid", json!(user_id))])
            .await
            .map(|r| r.first().and_then(Value::as_array).is_some_and(|rows| !rows.is_empty()));
        match on_roster {
            Ok(true) => {
                tracing::info!(username, user_id, "Admin login");
                self.admin_login_answer(user_id, username.to_string(), SessionMode::Roster)
            }
            Ok(false) => {
                tracing::warn!(username, user_id, "Authenticated but not on the _00_admin roster");
                error(403, "This account is not an administrator. Grant access with `spky admin add`.")
            }
            Err(e) => {
                tracing::warn!(error = %e, "Roster lookup against _00_admin failed");
                error(503, "The project's database could not be reached")
            }
        }
    }

    fn admin_login_answer(&self, subject: String, label: String, mode: SessionMode) -> ApiResponse {
        let (token, _) = self.mint_admin_token(subject.clone(), label.clone(), mode, Scope::Full, SESSION_TTL_SECS);
        json_response(
            200,
            json!({ "token": token, "mode": mode, "subject": subject, "label": label, "expires_in_secs": SESSION_TTL_SECS }),
        )
    }

    fn admin_mint_token(&self, session: &Session, body: &Value) -> ApiResponse {
        // A token that can mint tokens is a token that never expires.
        if session.mode == SessionMode::Mcp {
            return error(403, "An MCP token cannot mint further tokens; sign in to the dashboard to create one");
        }
        let label = body.get("label").and_then(Value::as_str).unwrap_or("").trim();
        if label.is_empty() || label.len() > 64 {
            return error(400, "A label of 1 to 64 characters is required");
        }
        let Some(scope) = Scope::parse(body.get("scope").and_then(Value::as_str).unwrap_or("read")) else {
            return error(400, "scope must be \"read\" or \"full\"");
        };
        let days = body.get("ttl_days").and_then(Value::as_u64).unwrap_or(DEFAULT_TOKEN_DAYS);
        if days == 0 || days > MAX_TOKEN_DAYS {
            return error(400, format!("ttl_days must be between 1 and {MAX_TOKEN_DAYS}"));
        }
        let (token, expires_at) =
            self.mint_admin_token(format!("mcp:{label}"), label.to_string(), SessionMode::Mcp, scope, days * 86_400);
        tracing::info!(label, ?scope, days, by = %session.subject, "MCP token minted");
        let expires_at = chrono_like_rfc3339(expires_at);
        json_response(
            201,
            json!({
                "token": token,
                "label": label,
                "scope": scope,
                "expires_at": expires_at,
                "endpoint": "/admin/api/mcp",
                "persistent": true,
            }),
        )
    }

    fn admin_revoke_token(&self, session: &Session, body: &Value) -> ApiResponse {
        let Some(token) = body.get("token").and_then(Value::as_str).map(str::trim).filter(|t| !t.is_empty()) else {
            return error(400, "A body of { \"token\": \"...\" } is required");
        };
        if let Ok(mut revoked) = self.admin.revoked.lock() {
            revoked.insert(token.to_string());
        }
        tracing::info!(by = %session.subject, "Admin token revoked");
        ApiResponse::text(204, "text/plain", String::new())
    }

    async fn admin_mcp(&self, session: &Session, req: &ApiRequest) -> ApiResponse {
        let message: Value = match serde_json::from_slice(&req.body) {
            Ok(v) => v,
            Err(e) => return json_response(200, mcp::rpc_error(Value::Null, -32700, format!("Parse error: {e}"))),
        };
        let mut instructions = format!(
            "Operator tools for the Sp00ky project '{}', served by its SSP (the project runs without a \
             scheduler). Start with `overview`.",
            self.admin.settings.project_slug.as_deref().unwrap_or(&self.ssp_id)
        );
        if session.scope == Scope::Read {
            instructions.push_str(" This token is read-only, so only inspection tools are available.");
        }
        let server = ServerInfo { version: self.version, instructions };
        match mcp::handle(&message, session.scope, &serves_tool, server) {
            Rpc::Accepted => ApiResponse::text(202, "text/plain", String::new()),
            Rpc::Reply(value) => json_response(200, value),
            Rpc::Call { id, tool, method, uri, body } => {
                tracing::info!(tool = tool.name, by = %session.subject, "MCP tool call");
                let method = match method {
                    "GET" => Method::Get,
                    "POST" => Method::Post,
                    "PUT" => Method::Put,
                    _ => Method::Delete,
                };
                let inner = ApiRequest {
                    method,
                    path: uri,
                    bearer: req.bearer.clone(),
                    body: body.map(|b| b.to_string().into_bytes().into()).unwrap_or_default(),
                };
                let resp = Box::pin(self.admin_dispatch(&inner)).await;
                let text = match resp.body {
                    crate::api::ApiBody::Json(v) => v.to_string(),
                    crate::api::ApiBody::Text { body, .. } => body,
                    crate::api::ApiBody::Bytes { body, .. } => String::from_utf8_lossy(&body).into_owned(),
                };
                json_response(200, mcp::tool_reply(id, resp.status, &text, false))
            }
        }
    }

    /// The overview: this node as the one SSP, its job backends, and the
    /// outbox totals. Shaped like the scheduler's so the dashboard renders it.
    async fn admin_overview(&self, db: &PortDb) -> ApiResponse {
        let ssps: Vec<Value> = match self.info_value().await {
            Value::Array(entities) => entities.into_iter().filter(|e| e["entity"] == "ssp").collect(),
            other => vec![other],
        };
        let ready = ssps.iter().filter(|s| s["status"] == "ready").count();
        let backends = self.admin_backends();
        json_response(
            200,
            json!({
                "host": "ssp",
                "scheduler": Value::Null,
                "ssps": ssps,
                "backends": backends,
                "totals": {
                    "ssps": ssps.len(),
                    "ssps_ready": ready,
                    "backends": backends.len(),
                    // Nothing probes a backend here; none is counted unhealthy.
                    "backends_healthy": backends.len(),
                },
                "bootstrap_timeout_secs": 0,
                "jobs": self.admin_job_totals(db).await,
                "cloud_linked": false,
                "server_time_ms": crate::now_epoch_ms(),
            }),
        )
    }

    /// Outbox totals, from memory for [`JOB_TOTALS_TTL_MS`].
    async fn admin_job_totals(&self, db: &PortDb) -> Value {
        let now = crate::now_epoch_ms();
        if let Ok(slot) = self.admin.job_totals.lock() {
            if let Some((at, totals)) = slot.as_ref().filter(|(at, _)| now.saturating_sub(*at) < JOB_TOTALS_TTL_MS) {
                let mut totals = totals.clone();
                totals["taken_at_ms"] = json!(at);
                return totals;
            }
        }
        let totals = match core::snapshot(db).await {
            Ok(snapshot) => snapshot.totals(json!([])),
            Err(e) => json!({ "ready": false, "error": e.message }),
        };
        if let Ok(mut slot) = self.admin.job_totals.lock() {
            *slot = Some((now, totals.clone()));
        }
        let mut totals = totals;
        totals["taken_at_ms"] = json!(now);
        totals
    }

    /// The backends jobs are dispatched to, one entry per backend with the
    /// outbox tables it serves. Nothing health-checks them on this host.
    fn admin_backends(&self) -> Vec<Value> {
        let mut by_name: std::collections::BTreeMap<String, (String, Vec<String>)> = Default::default();
        for (table, backend) in &self.job_config.job_tables {
            let entry = by_name.entry(backend.name.clone()).or_insert_with(|| (backend.base_url.clone(), Vec::new()));
            entry.1.push(table.clone());
        }
        by_name
            .into_iter()
            .map(|(name, (url, mut tables))| {
                tables.sort();
                json!({
                    "name": name,
                    "id": name,
                    "url": url,
                    "ip": Value::Null,
                    "port": Value::Null,
                    "healthcheck": "",
                    "healthcheck_url": "",
                    "status": "unknown",
                    "response_time_ms": Value::Null,
                    "last_checked": Value::Null,
                    "last_healthy": Value::Null,
                    "tables": tables,
                })
            })
            .collect()
    }

    /// Kill or retry through the node's own `/job/kill` and `/job/retry`,
    /// translating their `{code, message}` errors to the admin `{error}`.
    async fn admin_job_action(&self, id: &str, kill: bool) -> ApiResponse {
        let inner = ApiRequest {
            method: Method::Post,
            path: if kill { "/job/kill" } else { "/job/retry" }.into(),
            bearer: None,
            body: json!({ "id": id }).to_string().into_bytes().into(),
        };
        let resp = if kill { self.job_kill_handler(&inner).await } else { self.job_retry_handler(&inner).await };
        let Some(resp) = resp else {
            return error(501, "Jobs are not enabled on this node");
        };
        match resp.body {
            crate::api::ApiBody::Json(body) if resp.status >= 400 => {
                let message = body
                    .get("message")
                    .or_else(|| body.get("error"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("Job action failed ({})", resp.status));
                error(resp.status, message)
            }
            body => ApiResponse { status: resp.status, headers: Vec::new(), body },
        }
    }
}

/// Record-access methods to try for a roster login: the app's own
/// `DEFINE ACCESS ... TYPE RECORD`, found in `INFO FOR DB`.
async fn record_accesses(db: &dyn crate::ports::Db) -> Vec<String> {
    let info = match db.query("INFO FOR DB;", &[]).await {
        Ok(results) => results.into_iter().next().unwrap_or(Value::Null),
        Err(e) => {
            tracing::warn!(error = %e, "INFO FOR DB failed while discovering record accesses");
            return Vec::new();
        }
    };
    let info = match info {
        Value::Array(mut rows) if !rows.is_empty() => rows.swap_remove(0),
        other => other,
    };
    info.get("accesses")
        .and_then(Value::as_object)
        .map(|accesses| {
            accesses
                .iter()
                .filter(|(_, def)| def.as_str().is_some_and(|d| d.contains("TYPE RECORD")))
                .map(|(name, _)| name.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// `YYYY-MM-DDTHH:MM:SSZ` for epoch seconds, without a date crate.
fn chrono_like_rfc3339(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // Civil-from-days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_split_into_decoded_segments_and_query_pairs() {
        let (seg, pairs) = split_path("/admin/api/jobs/job%3Aa-b/kill?status=failed&q=a+b&empty=");
        assert_eq!(seg, ["jobs", "job:a-b", "kill"]);
        assert_eq!(pairs, [("status".into(), "failed".into()), ("q".into(), "a b".into()), ("empty".into(), String::new())]);
        assert_eq!(split_path("/admin/api").0, Vec::<String>::new());
        assert_eq!(percent_decode("100%"), "100%", "a stray percent stays");
        assert_eq!(percent_decode("%e2%9f%a9"), "⟩");
    }

    #[test]
    fn admin_paths_and_schedule_touches_are_recognised() {
        assert!(is_admin_path("/admin/api/config"));
        assert!(is_admin_path("/admin/api?x=1"));
        assert!(!is_admin_path("/admin/reload"));
        assert!(!is_admin_path("/admin/apis"));
        assert!(touches_schedules("/admin/api/schedules/nightly/trigger"));
        assert!(!touches_schedules("/admin/api/jobs"));
    }

    #[test]
    fn the_mcp_server_lists_only_what_a_node_serves() {
        let names: Vec<&str> = ssp_protocol::admin_mcp::TOOLS.iter().filter(|t| serves_tool(t)).map(|t| t.name).collect();
        for expected in ["overview", "jobs_list", "job_kill", "schedule_trigger", "workflow_run_retry", "backends_list"] {
            assert!(names.contains(&expected), "{expected} missing from {names:?}");
        }
        for absent in ["ssp_restart", "logs_recent", "backup_create", "pools_list", "views_list"] {
            assert!(!names.contains(&absent), "{absent} is cluster-only");
        }
    }

    #[test]
    fn a_surreal_token_names_its_record() {
        use base64::Engine as _;
        let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"ID":"user:alice","AC":"account"}"#);
        assert_eq!(record_id_from_jwt(&format!("h.{claims}.s")).as_deref(), Some("user:alice"));
        assert_eq!(record_id_from_jwt("garbage"), None);
    }

    #[test]
    fn expiry_dates_render_as_rfc3339() {
        assert_eq!(chrono_like_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(chrono_like_rfc3339(1_792_616_928), "2026-10-21T21:08:48Z");
        assert_eq!(chrono_like_rfc3339(951_782_400), "2000-02-29T00:00:00Z");
    }
}
