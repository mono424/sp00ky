use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Json, Router,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, trace};

use crate::replica::Replica;
use crate::SchedulerStatus;

/// Request to execute a SurrealQL query against the snapshot DB
#[derive(Debug, Deserialize)]
pub struct ProxyQueryRequest {
    pub query: String,
}

/// Shared state for proxy handlers
#[derive(Clone)]
pub struct ProxyState {
    pub replica: Arc<RwLock<Replica>>,
    pub status: Arc<RwLock<SchedulerStatus>>,
}

/// Create proxy router for SSP bootstrap
pub fn create_proxy_router(state: ProxyState) -> Router {
    Router::new()
        .route("/proxy/query", post(handle_proxy_query))
        .route("/proxy/ranges", post(handle_proxy_ranges))
        .route("/proxy/signin", post(handle_proxy_signin))
        .route("/proxy/use", post(handle_proxy_use))
        .with_state(state)
}

async fn reject_if_restoring(
    status: &Arc<RwLock<SchedulerStatus>>,
) -> Result<(), (StatusCode, String)> {
    if *status.read().await == SchedulerStatus::Restoring {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Scheduler is restoring from backup".to_string(),
        ));
    }
    Ok(())
}

/// Handle a SurrealQL query forwarded to the snapshot DB
async fn handle_proxy_query(
    State(state): State<ProxyState>,
    Json(request): Json<ProxyQueryRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    reject_if_restoring(&state.status).await?;

    trace!(query = %request.query, "proxy query (forwarded to local replica)");

    let replica = state.replica.read().await;
    match replica.query(&request.query).await {
        Ok(result) => Ok(Json(result)),
        Err(e) => {
            error!(error = %e, query = %request.query, "Proxy query failed");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Query failed: {}", e),
            ))
        }
    }
}

/// A table's id-range hashes (see `ssp_protocol::range_hash`), for a warm SSP
/// whose table hash differs to find the ranges that differ instead of listing
/// the whole table. 404 when the replica has none to vouch for (not built yet,
/// keys that cannot be ranged, or a hash waiting to be recomputed): the SSP
/// then lists the table in full, which is also what an older scheduler, which
/// has no such route, makes it do.
async fn handle_proxy_ranges(
    State(state): State<ProxyState>,
    Json(request): Json<ssp_protocol::range_hash::RangeHashesRequest>,
) -> Result<Json<ssp_protocol::range_hash::TableRanges>, (StatusCode, String)> {
    reject_if_restoring(&state.status).await?;
    let replica = state.replica.read().await;
    replica.table_ranges(&request.table).map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("no range hashes for table {}", request.table),
        )
    })
}

/// No-op signin — snapshot DB doesn't need auth
async fn handle_proxy_signin(
    State(state): State<ProxyState>,
) -> Result<StatusCode, (StatusCode, String)> {
    reject_if_restoring(&state.status).await?;
    Ok(StatusCode::OK)
}

/// No-op namespace/db selection — already configured
async fn handle_proxy_use(
    State(state): State<ProxyState>,
) -> Result<StatusCode, (StatusCode, String)> {
    reject_if_restoring(&state.status).await?;
    Ok(StatusCode::OK)
}
