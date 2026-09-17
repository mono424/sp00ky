//! `POST /impersonate/mint` for cluster mode, where `{{ENDPOINT}}` in the
//! SurrealDB functions is the scheduler. Singlenode serves the same route from
//! the SSP (`ssp_node::SspNode::route`); both call the shared, stateless signer
//! in `ssp_protocol::impersonation`, which documents the security model.
//!
//! The rest of the ingest port is unauthenticated (it is assumed private), but
//! a signed identity is worth more than anything else on it, so this route
//! checks the shared bearer itself.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde_json::json;
use ssp_protocol::impersonation::{
    constant_time_eq, env_enabled, mint, users_query, MintError, MintRequest, UsersRequest, ENV_FLAG,
};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

#[derive(Clone)]
pub struct ImpersonationState {
    pub enabled: bool,
    pub auth_secret: Arc<str>,
    /// Root handle for the user search. `None` until the scheduler connects.
    pub db_slot: crate::admin::SharedDbSlot,
}

impl ImpersonationState {
    pub fn from_env(db_slot: crate::admin::SharedDbSlot) -> Self {
        Self {
            enabled: env_enabled(std::env::var(ENV_FLAG).ok().as_deref()),
            auth_secret: std::env::var("SPKY_AUTH_SECRET").unwrap_or_default().into(),
            db_slot,
        }
    }

    fn active(&self) -> bool {
        self.enabled && !self.auth_secret.is_empty()
    }
}

pub fn create_impersonation_router(state: ImpersonationState) -> Router {
    if state.enabled {
        if !state.active() {
            warn!("{ENV_FLAG} is on but SPKY_AUTH_SECRET is empty; impersonation stays disabled");
        } else {
            info!("Admin impersonation enabled");
        }
    }
    Router::new()
        .route("/impersonate/mint", post(handle_mint))
        .route("/impersonate/users", post(handle_users))
        .with_state(state)
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({ "code": code, "message": message }))).into_response()
}

/// The gate every route shares: feature on, and the shared bearer presented.
fn guard(state: &ImpersonationState, headers: &HeaderMap) -> Result<(), Response> {
    if !state.active() {
        return Err(error(StatusCode::NOT_FOUND, "not_found", "impersonation is disabled"));
    }
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !constant_time_eq(presented.as_bytes(), state.auth_secret.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED.into_response());
    }
    Ok(())
}

async fn handle_mint(
    State(state): State<ImpersonationState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Err(r) = guard(&state, &headers) {
        return r;
    }
    let Ok(req) = serde_json::from_slice::<MintRequest>(&body) else {
        return error(StatusCode::UNPROCESSABLE_ENTITY, "bad_body", "invalid mint payload");
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    match mint(&state.auth_secret, &req, now) {
        Ok(out) => {
            info!(
                session = %req.session,
                admin = %req.admin,
                target = %req.target,
                exp = out.exp,
                "impersonation token minted"
            );
            Json(out).into_response()
        }
        Err(MintError::Disabled) => error(StatusCode::NOT_FOUND, "not_found", "impersonation is disabled"),
        Err(MintError::Invalid(why)) => error(StatusCode::BAD_REQUEST, "invalid", why),
    }
}

/// Root-backed user search for the DevTools picker; see
/// `ssp_protocol::impersonation::users_query`.
async fn handle_users(
    State(state): State<ImpersonationState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Err(r) = guard(&state, &headers) {
        return r;
    }
    let Ok(req) = serde_json::from_slice::<UsersRequest>(&body) else {
        return error(StatusCode::UNPROCESSABLE_ENTITY, "bad_body", "invalid users payload");
    };
    let (surql, q) = match users_query(&req) {
        Ok(v) => v,
        Err(MintError::Invalid(why)) => return error(StatusCode::BAD_REQUEST, "invalid", why),
        Err(MintError::Disabled) => {
            return error(StatusCode::NOT_FOUND, "not_found", "impersonation is disabled")
        }
    };
    let Some(db) = state.db_slot.read().await.clone() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "starting", "database not connected yet");
    };
    let rows: Result<Vec<serde_json::Value>, _> = db
        .handle()
        .query(surql)
        .bind(("q", q))
        .await
        .and_then(|mut r| r.take(0));
    match rows {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => {
            db.note_error(&format!("{e:#}"));
            error(StatusCode::INTERNAL_SERVER_ERROR, "query_failed", &e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn app(enabled: bool, secret: &str) -> Router {
        create_impersonation_router(ImpersonationState {
            enabled,
            auth_secret: secret.into(),
            db_slot: Default::default(),
        })
    }

    fn request(bearer: Option<&str>, body: serde_json::Value) -> Request<Body> {
        let mut b = Request::post("/impersonate/mint").header("content-type", "application/json");
        if let Some(t) = bearer {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        b.body(Body::from(body.to_string())).unwrap()
    }

    fn body() -> serde_json::Value {
        json!({
            "session": "_00_impersonation:s1", "target": "user:bob", "admin": "user:alice",
            "access": "account", "ns": "n", "db": "d", "ttl_secs": 900, "session_remaining_secs": 600,
        })
    }

    #[tokio::test]
    async fn disabled_is_404() {
        let r = app(false, "s").oneshot(request(Some("s"), body())).await.unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn empty_secret_is_404_even_when_enabled() {
        let r = app(true, "").oneshot(request(Some(""), body())).await.unwrap();
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn wrong_or_missing_bearer_is_401() {
        let r = app(true, "s").oneshot(request(Some("x"), body())).await.unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        let r = app(true, "s").oneshot(request(None, body())).await.unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn invalid_request_is_400() {
        let mut b = body();
        b["session"] = json!("user:x");
        let r = app(true, "s").oneshot(request(Some("s"), b)).await.unwrap();
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn mints_a_token() {
        let r = app(true, "s").oneshot(request(Some("s"), body())).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(r.into_body(), usize::MAX).await.unwrap();
        let out: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(out["token"].as_str().unwrap().split('.').count(), 3);
    }
}
