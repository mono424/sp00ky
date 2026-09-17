//! Framework-agnostic HTTP surface shared by every shell.
//!
//! Both shells expose the SAME routes; this module is the single source of
//! truth for that table. Today only the types + route matching live here;
//! handler dispatch (`SspNode::route`) arrives as handler logic migrates out
//! of `apps/ssp` (see docs/platform-architecture.md §8 — the VM shell mounts
//! the core as an axum catch-all fallback, so routes migrate one at a time
//! with every commit green).

use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

/// A request as the core sees it: no axum, no workers-rs.
pub struct ApiRequest {
    pub method: Method,
    pub path: String,
    /// Bearer token from the `Authorization` header, if any.
    pub bearer: Option<String>,
    pub body: Bytes,
}

/// A response as the core produces it. Shells convert to their framework's
/// response type; `headers` carries the odd extra (CORS on public routes).
pub struct ApiResponse {
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: ApiBody,
}

/// Response body + how the shell should frame it. Most routes are JSON; a few
/// (e.g. `/info/text`) must emit a verbatim string under a specific
/// content-type (SurrealDB's `/spooky` proxy passes the body through).
pub enum ApiBody {
    Json(serde_json::Value),
    Text { content_type: &'static str, body: String },
}

impl ApiResponse {
    pub fn json(status: u16, json: serde_json::Value) -> Self {
        Self { status, headers: Vec::new(), body: ApiBody::Json(json) }
    }

    pub fn text(status: u16, content_type: &'static str, body: String) -> Self {
        Self { status, headers: Vec::new(), body: ApiBody::Text { content_type, body } }
    }
}

/// Every route the SSP data plane serves, on either platform.
///
/// `requires_auth()` mirrors today's split: the authenticated group sits
/// behind the bearer middleware, the public group gets a permissive CORS
/// header. The maintenance (`/backup/*`, `/backends`) routes only exist in
/// standalone mode and are always authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteId {
    // -- authenticated --
    Ingest,
    Log,
    DebugView { view_id: String },
    DebugDeps,
    /// Last `_00_heartbeat` seq this node ingested — the observation point
    /// for the scheduler's e2e heartbeat probe.
    DebugHeartbeat,
    DebugCatchupRows { table: String },
    /// Estimated heap footprint of the circuit, attributed per table and per
    /// registered query. The SSP mirrors every syncable table in RAM, so an
    /// OOM kill is the dominant failure mode — and it arrives as a SIGKILL
    /// with no log line, leaving the control plane's process-wide `mem_bytes`
    /// as the only signal and no way to tell which table is responsible.
    DebugMemory,
    ViewRegister,
    ViewUnregister,
    CrdtApply,
    Reset,
    /// Re-scan the DB schema + reload data into a fresh circuit (picks up
    /// tables/permissions defined after the last bootstrap). Authed.
    Reload,
    JobKill,
    JobRetry,
    JobRecover,
    /// Sign an admin impersonation token (`ssp_protocol::impersonation`).
    /// Called by `fn::_00_impersonate::*` with the shared bearer.
    ImpersonateMint,
    /// Root-backed user search for the impersonation picker.
    ImpersonateUsers,
    // -- standalone maintenance plane (authenticated) --
    BackendsUpdate,
    BackupCreate,
    BackupStatus,
    BackupStatusById { backup_id: String },
    BackupRestore,
    BackupRestoreStatusById { restore_id: String },
    // -- public --
    Health,
    Info,
    InfoText,
    Version,
}

impl RouteId {
    /// Match a (method, path) pair against the route table.
    /// Path params are captured; query strings are not part of the surface.
    pub fn match_path(method: Method, path: &str) -> Option<RouteId> {
        use Method::*;
        let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
        let route = match (method, segments.as_slice()) {
            (Post, ["ingest"]) => RouteId::Ingest,
            (Post, ["log"]) => RouteId::Log,
            (Get, ["debug", "view", view_id]) => RouteId::DebugView { view_id: (*view_id).to_string() },
            (Get, ["debug", "deps"]) => RouteId::DebugDeps,
            (Get, ["debug", "heartbeat"]) => RouteId::DebugHeartbeat,
            (Get, ["debug", "catchup-rows", table]) => RouteId::DebugCatchupRows { table: (*table).to_string() },
            (Get, ["debug", "memory"]) => RouteId::DebugMemory,
            (Post, ["view", "register"]) => RouteId::ViewRegister,
            (Post, ["view", "unregister"]) => RouteId::ViewUnregister,
            (Post, ["crdt", "apply"]) => RouteId::CrdtApply,
            (Post, ["reset"]) => RouteId::Reset,
            (Post, ["admin", "reload"]) => RouteId::Reload,
            (Post, ["job", "kill"]) => RouteId::JobKill,
            (Post, ["job", "retry"]) => RouteId::JobRetry,
            (Post, ["job", "recover"]) => RouteId::JobRecover,
            (Post, ["impersonate", "mint"]) => RouteId::ImpersonateMint,
            (Post, ["impersonate", "users"]) => RouteId::ImpersonateUsers,
            (Put, ["backends"]) => RouteId::BackendsUpdate,
            (Post, ["backup", "create"]) => RouteId::BackupCreate,
            (Get, ["backup", "status"]) => RouteId::BackupStatus,
            (Get, ["backup", "status", id]) => RouteId::BackupStatusById { backup_id: (*id).to_string() },
            (Post, ["backup", "restore"]) => RouteId::BackupRestore,
            (Get, ["backup", "restore", "status", id]) => {
                RouteId::BackupRestoreStatusById { restore_id: (*id).to_string() }
            }
            (Get, ["health"]) => RouteId::Health,
            (Get, ["info"]) => RouteId::Info,
            (Get, ["info", "text"]) => RouteId::InfoText,
            (Get, ["version"]) => RouteId::Version,
            _ => return None,
        };
        Some(route)
    }

    pub fn requires_auth(&self) -> bool {
        !matches!(
            self,
            RouteId::Health | RouteId::Info | RouteId::InfoText | RouteId::Version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_full_route_table() {
        let cases = [
            (Method::Post, "/ingest", RouteId::Ingest),
            (Method::Post, "/view/register", RouteId::ViewRegister),
            (Method::Get, "/debug/view/v1", RouteId::DebugView { view_id: "v1".into() }),
            (Method::Get, "/debug/catchup-rows/thread", RouteId::DebugCatchupRows { table: "thread".into() }),
            (Method::Get, "/debug/heartbeat", RouteId::DebugHeartbeat),
            (Method::Get, "/debug/memory", RouteId::DebugMemory),
            (Method::Put, "/backends", RouteId::BackendsUpdate),
            (Method::Post, "/impersonate/mint", RouteId::ImpersonateMint),
            (Method::Get, "/backup/status/b1", RouteId::BackupStatusById { backup_id: "b1".into() }),
            (Method::Get, "/backup/restore/status/r1", RouteId::BackupRestoreStatusById { restore_id: "r1".into() }),
            (Method::Get, "/health", RouteId::Health),
            (Method::Get, "/info/text", RouteId::InfoText),
        ];
        for (method, path, expected) in cases {
            assert_eq!(RouteId::match_path(method, path), Some(expected), "{path}");
        }
        assert_eq!(RouteId::match_path(Method::Get, "/ingest"), None); // wrong method
        assert_eq!(RouteId::match_path(Method::Post, "/nope"), None);
    }

    #[test]
    fn auth_split_matches_current_middleware_groups() {
        assert!(RouteId::Ingest.requires_auth());
        assert!(RouteId::BackupCreate.requires_auth());
        assert!(RouteId::DebugHeartbeat.requires_auth());
        assert!(RouteId::ImpersonateMint.requires_auth());
        assert!(!RouteId::Health.requires_auth());
        assert!(!RouteId::Version.requires_auth());
    }
}
