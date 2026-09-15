use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::router::SspPool;
use crate::transport::HttpTransport;
use ssp_protocol::{ViewRegisterRequest, ViewUnregisterRequest};

/// Query assignment response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryAssignment {
    pub query_id: String,
    pub ssp_id: String,
    pub assigned_at: u64,
}

/// Query tracker state
#[derive(Clone)]
pub struct QueryTracker {
    /// Map query_id -> (ssp_id, unix ms of the latest registration).
    ///
    /// The timestamp is what makes an asynchronous unregister safe: a view
    /// teardown that arrives after the client already re-registered the same
    /// id (a tab closed and reopened, a strict-mode double mount) must not
    /// tear down the fresh registration. See [`unregister_local`].
    assignments: Arc<RwLock<HashMap<String, (String, u64)>>>,
}

impl QueryTracker {
    pub fn new() -> Self {
        Self {
            assignments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Assign a query to an SSP, stamping the registration time. Called on
    /// every registration, sticky ones included, so the stamp always says
    /// when the client last (re)registered.
    pub async fn assign(&self, query_id: String, ssp_id: String) {
        let mut assignments = self.assignments.write().await;
        assignments.insert(query_id, (ssp_id, now_ms()));
    }

    /// Get SSP assigned to a query
    pub async fn get_assignment(&self, query_id: &str) -> Option<String> {
        let assignments = self.assignments.read().await;
        assignments.get(query_id).map(|(ssp, _)| ssp.clone())
    }

    /// Unix ms of the query's latest registration.
    pub async fn assigned_at_ms(&self, query_id: &str) -> Option<u64> {
        let assignments = self.assignments.read().await;
        assignments.get(query_id).map(|(_, at)| *at)
    }

    /// Unassign a query (when client disconnects)
    pub async fn unassign(&self, query_id: &str) {
        let mut assignments = self.assignments.write().await;
        assignments.remove(query_id);
    }

    /// Unassign all queries from an SSP (when SSP disconnects)
    pub async fn unassign_ssp(&self, ssp_id: &str) -> Vec<String> {
        let mut assignments = self.assignments.write().await;
        let removed: Vec<String> = assignments
            .iter()
            .filter(|(_, (sid, _))| sid == ssp_id)
            .map(|(qid, _)| qid.clone())
            .collect();
        
        for qid in &removed {
            assignments.remove(qid);
        }
        
        removed
    }

    /// Get all assignments
    pub async fn all(&self) -> HashMap<String, String> {
        let assignments = self.assignments.read().await;
        assignments
            .iter()
            .map(|(q, (s, _))| (q.clone(), s.clone()))
            .collect()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Shared state for query handlers
#[derive(Clone)]
pub struct QueryState {
    pub ssp_pool: Arc<RwLock<SspPool>>,
    pub transport: Arc<HttpTransport>,
    pub query_tracker: Arc<QueryTracker>,
}

/// Create query router
pub fn create_query_router(state: QueryState) -> Router {
    Router::new()
        .route("/view/register", post(register_query))
        .route("/view/unregister", post(unregister_query))
        .with_state(state)
}

/// Handle query registration
async fn register_query(
    State(state): State<QueryState>,
    Json(request): Json<ViewRegisterRequest>,
) -> Result<Json<QueryAssignment>, (StatusCode, String)> {
    let query_id = request.id.clone();

    // Clients re-issue `fn::query::register` for live views on reconnect and
    // keepalive, so re-registration of an already-assigned query is the
    // COMMON case. Keep it sticky: forward to the SSP that already owns the
    // view (refreshing its metadata/TTL there) instead of re-running
    // selection. Re-selecting round-robined the same view onto a different
    // SSP on every call (ssp-0 ↔ ssp-1 ping-pong) and incremented the new
    // SSP's query_count without decrementing the old one, skewing
    // least-queries balancing forever.
    let previous = state.query_tracker.get_assignment(&query_id).await;

    // `sticky` is true when we reuse the existing assignment — query_count
    // already accounts for this query there, so no increment.
    let (sticky, ssp_id, ssp_url) = {
        let mut pool = state.ssp_pool.write().await;
        let sticky_target = previous.as_ref().and_then(|prev| {
            if pool.is_ready(prev) {
                pool.get(prev).map(|s| (prev.clone(), s.url.clone()))
            } else {
                None
            }
        });
        match sticky_target {
            Some((id, url)) => (true, id, url),
            None => match pool.select_for_query() {
                Some(id) => {
                    // Get SSP URL before incrementing count
                    let url = pool.get(&id)
                        .ok_or_else(|| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "Selected SSP not found in pool".to_string(),
                            )
                        })?
                        .url
                        .clone();
                    // Genuine (re)assignment: move the count with the query.
                    if let Some(prev) = &previous {
                        pool.decrement_query_count(prev);
                    }
                    pool.increment_query_count(&id);
                    (false, id, url)
                }
                None => {
                    error!("No ready SSP available for query {}", query_id);
                    return Err((
                        StatusCode::SERVICE_UNAVAILABLE,
                        "No SSP available".to_string(),
                    ));
                }
            },
        }
    };
    if sticky {
        // debug: clients re-register every keepalive — info would spam.
        tracing::debug!("Query {} already assigned to ready SSP {} — sticky re-register", query_id, ssp_id);
    }

    // Assign query to SSP in tracker
    state.query_tracker.assign(query_id.clone(), ssp_id.clone()).await;

    // Send registration to SSP via HTTP POST /view/register. The SSP's own
    // verdict is relayed with its status: a 4xx (400 `rejected`, 403
    // `not_allowlisted`, 409 `auth_mismatch`) is the client's problem and
    // must reach it as such, not folded into a 500 that reads as an outage.
    let outcome = state
        .transport
        .post_to_ssp_status(&ssp_url, "/view/register", &request)
        .await;
    let failure = match outcome {
        Ok((status, _)) if status.is_success() => None,
        Ok((status, body)) if status.is_client_error() => {
            warn!(query = %query_id, %status, body = %body, "SSP refused query registration");
            Some((status, body))
        }
        Ok((status, body)) => {
            error!("SSP returned {} for query registration: {}", status, body);
            Some((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to send to SSP: SSP returned {status}: {body}"),
            ))
        }
        Err(e) => {
            error!("Failed to send query registration to SSP: {}", e);
            Some((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to send to SSP: {}", e),
            ))
        }
    };
    if let Some(err) = failure {
        // Remove from tracker on failure
        state.query_tracker.unassign(&query_id).await;
        // Decrement query count on failure
        {
            let mut pool = state.ssp_pool.write().await;
            pool.decrement_query_count(&ssp_id);
        }
        return Err(err);
    }

    let assignment = QueryAssignment {
        query_id: query_id.clone(),
        ssp_id,
        assigned_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };

    if !sticky {
        info!("Assigned query {} to SSP {}", assignment.query_id, assignment.ssp_id);
    }
    Ok(Json(assignment))
}

/// Handle query unregistration.
///
/// Acknowledges as soon as the tracker is updated and forwards to the SSP in
/// the background. With the `http` transport this request is made by the
/// `_00_dbsp_cleanup` DB event INSIDE the transaction deleting the
/// `_00_query` row, and the SSP's own TTL sweep is one such deleter: a
/// synchronous forward made the sweep wait on the scheduler, which waited on
/// the SSP, which waited on the sweep's lock (2026-09-14, ten seconds per
/// expired row until the event's TIMEOUT cut it). Nothing about the forward
/// needs the caller to wait for it.
async fn unregister_query(
    State(state): State<QueryState>,
    Json(request): Json<ViewUnregisterRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    unregister_local(&state, &request.id, Some(now_ms())).await;
    Ok(StatusCode::OK)
}

/// Tear down a view registration: clear the tracker now, tell the SSP in the
/// background. Shared by the HTTP route and the changefeed tail (a
/// `_00_query` DELETE in the feed).
///
/// `deleted_at_ms` is when the `_00_query` row was deleted. A registration
/// stamped AFTER that is a client that re-registered the same id since, and
/// its view stays: the delete the caller saw is older than the view the SSP
/// now serves. Returns whether a teardown was forwarded.
pub async fn unregister_local(state: &QueryState, query_id: &str, deleted_at_ms: Option<u64>) -> bool {
    info!("Unregistering query: {}", query_id);

    // Unregister is idempotent: if the query isn't tracked (e.g. fired by
    // `_00_dbsp_cleanup` on a stale row after a scheduler restart), there's
    // nothing to forward.
    let Some(ssp_id) = state.query_tracker.get_assignment(query_id).await else {
        info!("Unregister for unknown query {} — treating as already unregistered", query_id);
        return false;
    };
    if let (Some(deleted), Some(assigned)) =
        (deleted_at_ms, state.query_tracker.assigned_at_ms(query_id).await)
    {
        if assigned > deleted {
            info!(
                query_id,
                assigned_ms = assigned,
                deleted_ms = deleted,
                "Unregister is older than the query's latest registration; keeping the view"
            );
            return false;
        }
    }

    // Tracker first, so a re-registration racing this call selects afresh
    // instead of sticking to an assignment that is being torn down.
    state.query_tracker.unassign(query_id).await;

    let ssp_url = {
        let pool = state.ssp_pool.read().await;
        match pool.get(&ssp_id) {
            Some(ssp) => ssp.url.clone(),
            None => {
                info!("Unregister for query {} whose SSP {} is gone — cleared tracker", query_id, ssp_id);
                return false;
            }
        }
    };

    let transport = Arc::clone(&state.transport);
    let ssp_pool = Arc::clone(&state.ssp_pool);
    let request = ViewUnregisterRequest { id: query_id.to_string() };
    let query_id = query_id.to_string();
    tokio::spawn(async move {
        if let Err(e) = transport
            .post_to_ssp(&ssp_url, "/view/unregister", &request)
            .await
        {
            error!("Failed to send query unregistration to SSP: {}", e);
        }
        ssp_pool.write().await.decrement_query_count(&ssp_id);
        info!("Unregistered query {}", query_id);
    });
    true
}

#[cfg(test)]
mod unregister_tests {
    use super::*;
    use crate::config::LoadBalanceStrategy;

    fn state() -> QueryState {
        QueryState {
            ssp_pool: Arc::new(RwLock::new(SspPool::new(LoadBalanceStrategy::LeastQueries, 16))),
            transport: Arc::new(HttpTransport::new()),
            query_tracker: Arc::new(QueryTracker::new()),
        }
    }

    /// A teardown read from the changefeed (or a DB event that arrived late)
    /// must not remove a view the client registered again since the delete.
    #[tokio::test]
    async fn unregister_keeps_a_registration_newer_than_the_delete() {
        let state = state();
        state.query_tracker.assign("q1".into(), "ssp-0".into()).await;
        let assigned = state.query_tracker.assigned_at_ms("q1").await.unwrap();

        assert!(!unregister_local(&state, "q1", Some(assigned - 10_000)).await);
        assert_eq!(state.query_tracker.get_assignment("q1").await.as_deref(), Some("ssp-0"), "older delete keeps the view");

        // A delete after the registration clears the tracker even when the
        // SSP is gone (nothing to forward to).
        assert!(!unregister_local(&state, "q1", Some(assigned + 10_000)).await);
        assert!(state.query_tracker.get_assignment("q1").await.is_none());

        // Unknown queries are a no-op.
        assert!(!unregister_local(&state, "nope", None).await);
    }

    #[tokio::test]
    async fn tracker_all_and_unassign_ssp_keep_working_with_timestamps() {
        let tracker = QueryTracker::new();
        tracker.assign("a".into(), "ssp-0".into()).await;
        tracker.assign("b".into(), "ssp-1".into()).await;
        let all = tracker.all().await;
        assert_eq!(all.get("a").map(String::as_str), Some("ssp-0"));
        let removed = tracker.unassign_ssp("ssp-0").await;
        assert_eq!(removed, vec!["a".to_string()]);
        assert!(tracker.assigned_at_ms("b").await.is_some());
    }
}
