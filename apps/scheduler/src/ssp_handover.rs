//! Blue/green SSP replacement, scheduler side.
//!
//! The control plane starts the new SSP (green) next to the one it replaces
//! (blue), with `SPKY_SSP_REPLACES` naming blue. Green registers with
//! `replaces` set; while blue is serving, [`crate::ssp_management`] takes green
//! in as blue's STANDBY: it bootstraps like any SSP (warm, from the shared row
//! checkpoints), follows every ingest, receives a shadow copy of every view
//! registration and teardown sent to blue, and writes nothing to the
//! database. It is never chosen for a view or a job.
//!
//! Once green is caught up, [`promote`] swaps them:
//!
//! 1. Hold view registrations (`view_gate`), so none lands mid-swap.
//! 2. Take blue off the live path (`Retiring`: its events queue, so a failed
//!    swap can hand them back) and wait for the fan-out to settle.
//! 3. `POST /handover/retire` to blue: it stops taking work, drains its
//!    publication queue and answers with the membership digest of every view
//!    it published. The edges in the database now say exactly that.
//! 4. `POST /handover/promote` to green with those digests: green republishes
//!    only the views whose membership differs from blue's, then starts
//!    publishing. Ingest kept flowing to green throughout; green makes the
//!    comparison and the switch atomically with respect to it.
//! 5. Blue's view assignments move to green, green stops being a standby, blue
//!    becomes `Retired` (it keeps heartbeating, so jobs it still runs are not
//!    re-run, until the control plane stops it). Registrations resume.
//!
//! If anything fails before step 5, blue is resumed (`/handover/resume`) and
//! gets back every event it missed; green stays a standby and the control
//! plane, which waits for the swap with a deadline, removes it.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::ingest::{Fanout, IngestState};
use crate::query::QueryTracker;
use crate::router::SspPool;
use crate::transport::HttpTransport;

/// Deadline for blue's retire, which drains its publication queue (the SSP
/// caps that at 60 s by default).
const RETIRE_TIMEOUT: Duration = Duration::from_secs(90);
/// Deadline for green's promote, which may republish views.
const PROMOTE_TIMEOUT: Duration = Duration::from_secs(300);
/// Promotions kept for `/handover/ssps`.
const REPORTS_KEPT: usize = 10;

/// Shared by the query router (registrations hold `view_gate`), the SSP
/// management routes (registration decides standby, a caught-up standby
/// triggers its promotion) and the scheduler handover (refused while a
/// promotion runs). One per process, see [`shared`].
pub struct SspHandover {
    /// Read-held by every view registration; a promotion takes it for writing.
    pub view_gate: RwLock<()>,
    promoting: tokio::sync::Mutex<()>,
    reports: std::sync::Mutex<VecDeque<PromotionReport>>,
}

impl Default for SspHandover {
    fn default() -> Self {
        Self {
            view_gate: RwLock::new(()),
            promoting: tokio::sync::Mutex::new(()),
            reports: std::sync::Mutex::new(VecDeque::new()),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct PromotionReport {
    pub standby: String,
    pub predecessor: String,
    /// `holding` | `retiring` | `promoting` | `done` | `failed`
    pub phase: String,
    pub error: Option<String>,
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    pub republished: Option<usize>,
}

static SHARED: std::sync::OnceLock<SspHandover> = std::sync::OnceLock::new();
static DEPS: std::sync::OnceLock<PromoteDeps> = std::sync::OnceLock::new();

/// The process's handover state.
pub fn shared() -> &'static SspHandover {
    SHARED.get_or_init(SspHandover::default)
}

/// Hand the promotion its dependencies; `main` does this once at boot. Until
/// then (and in tests that never call it) caught-up standbys are not promoted.
pub fn install(deps: PromoteDeps) {
    let _ = DEPS.set(deps);
}

pub fn deps() -> Option<PromoteDeps> {
    DEPS.get().cloned()
}

impl SspHandover {
    pub fn in_progress(&self) -> bool {
        self.promoting.try_lock().is_err()
    }

    pub fn reports(&self) -> Vec<PromotionReport> {
        self.reports.lock().unwrap_or_else(|e| e.into_inner()).iter().cloned().collect()
    }

    fn report(&self, standby: &str, predecessor: &str, phase: &str) {
        let mut reports = self.reports.lock().unwrap_or_else(|e| e.into_inner());
        match reports.iter_mut().rev().find(|r| r.standby == standby && r.finished_ms.is_none()) {
            Some(r) => {
                r.phase = phase.to_string();
                r.predecessor = predecessor.to_string();
            }
            None => {
                reports.push_back(PromotionReport {
                    standby: standby.to_string(),
                    predecessor: predecessor.to_string(),
                    phase: phase.to_string(),
                    error: None,
                    started_ms: now_ms(),
                    finished_ms: None,
                    republished: None,
                });
                while reports.len() > REPORTS_KEPT {
                    reports.pop_front();
                }
            }
        }
    }

    fn finish(&self, standby: &str, error: Option<String>, republished: Option<usize>) {
        let mut reports = self.reports.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(r) = reports.iter_mut().rev().find(|r| r.standby == standby && r.finished_ms.is_none()) {
            r.phase = if error.is_some() { "failed" } else { "done" }.to_string();
            r.error = error;
            r.finished_ms = Some(now_ms());
            r.republished = republished;
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Everything [`promote`] touches.
#[derive(Clone)]
pub struct PromoteDeps {
    pub ssp_pool: Arc<RwLock<SspPool>>,
    pub transport: Arc<HttpTransport>,
    pub query_tracker: Arc<QueryTracker>,
    pub fanout: Arc<Fanout>,
    /// For resuming a predecessor's redelivery when a swap is abandoned.
    pub ingest: IngestState,
}

/// Start promoting `standby` in the background, if it is a ready standby.
pub fn spawn_promotion(deps: PromoteDeps, standby: String) {
    tokio::spawn(async move {
        if let Err(e) = promote(&deps, &standby).await {
            error!(standby = %standby, error = %e, "SSP promotion failed; the predecessor keeps serving");
        }
    });
}

/// Promote every ready standby: after a scheduler handover, a standby that was
/// caught up before it would otherwise wait forever.
pub async fn promote_ready_standbys(deps: PromoteDeps) {
    let ready: Vec<String> = {
        let pool = deps.ssp_pool.read().await;
        pool.all()
            .iter()
            .filter(|s| pool.is_standby(&s.id) && pool.is_ready(&s.id))
            .map(|s| s.id.clone())
            .collect()
    };
    for standby in ready {
        spawn_promotion(deps.clone(), standby);
    }
}

/// Swap `standby` in for the SSP it stands by for. See the module docs.
pub async fn promote(deps: &PromoteDeps, standby: &str) -> anyhow::Result<()> {
    let _one_at_a_time = shared().promoting.lock().await;

    let (predecessor, predecessor_url, standby_url, others) = {
        let pool = deps.ssp_pool.read().await;
        let Some(predecessor) = pool.standby_predecessor(standby).map(str::to_string) else {
            return Ok(()); // promoted already, or no longer a standby
        };
        anyhow::ensure!(pool.is_ready(standby), "standby '{standby}' is not ready");
        let standby_url = pool
            .get(standby)
            .map(|s| s.url.clone())
            .ok_or_else(|| anyhow::anyhow!("standby '{standby}' left the pool"))?;
        let predecessor_url = pool
            .is_serving(&predecessor)
            .then(|| pool.get(&predecessor).map(|s| s.url.clone()))
            .flatten();
        let others = pool
            .all()
            .iter()
            .any(|s| s.id != predecessor && s.id != standby && pool.is_serving(&s.id));
        (predecessor, predecessor_url, standby_url, others)
    };

    shared().report(standby, &predecessor, "holding");
    info!(standby, predecessor = %predecessor, "Promoting SSP standby");
    let _views = shared().view_gate.write().await;

    // The predecessor died while its standby bootstrapped: nothing to retire,
    // and nothing published can be trusted to match. Republish everything.
    let Some(predecessor_url) = predecessor_url else {
        warn!(standby, predecessor = %predecessor, "Predecessor no longer serving; promoting the standby with a full republish");
        let promoted = promote_call(deps, &standby_url, None, None).await;
        return commit_or_fail(deps, standby, &predecessor, promoted, None).await;
    };

    // Off the live path first, then let anything already in flight to it land.
    deps.ssp_pool.write().await.mark_retiring(&predecessor);
    deps.fanout.idle().await;

    shared().report(standby, &predecessor, "retiring");
    let retire = deps
        .transport
        .post_to_ssp_with_timeout(
            &predecessor_url,
            "/handover/retire",
            &ssp_protocol::SspRetireRequest { successor: standby.to_string() },
            RETIRE_TIMEOUT,
        )
        .await;
    let digests = match retire {
        Ok((status, body)) if status.is_success() => {
            match serde_json::from_str::<ssp_protocol::SspRetireResponse>(&body) {
                Ok(r) if r.drained => {
                    info!(predecessor = %predecessor, views = r.digests.len(), "Predecessor retired");
                    Some(r.digests)
                }
                Ok(r) => {
                    warn!(predecessor = %predecessor, views = r.digests.len(), "Predecessor retired without draining its publications; the standby republishes every view");
                    None
                }
                Err(e) => {
                    warn!(predecessor = %predecessor, error = %e, "Predecessor's retire answer did not parse; the standby republishes every view");
                    None
                }
            }
        }
        Ok((status, _)) if status == reqwest::StatusCode::NOT_FOUND => {
            // An SSP from before handovers. It gets no more events or views
            // from here on and the control plane stops it next; the standby
            // cannot know what it published, so it republishes everything,
            // as a warm restart does.
            warn!(predecessor = %predecessor, "Predecessor predates handovers; the standby republishes every view");
            None
        }
        Ok((status, body)) => {
            let why = format!("retire answered {status}: {body}");
            return abandon(deps, standby, &predecessor, &predecessor_url, why).await;
        }
        Err(e) => {
            let why = format!("retire failed: {e:#}");
            return abandon(deps, standby, &predecessor, &predecessor_url, why).await;
        }
    };

    // With other SSPs serving, the standby must give up the views they own:
    // every SSP loads every `_00_query` row at boot.
    let keep = if others { Some(deps.query_tracker.queries_of(&predecessor).await) } else { None };

    shared().report(standby, &predecessor, "promoting");
    let promoted = promote_call(deps, &standby_url, digests, keep).await;
    if let Err(e) = &promoted {
        let why = format!("{e:#}");
        return abandon(deps, standby, &predecessor, &predecessor_url, why).await;
    }
    commit_or_fail(deps, standby, &predecessor, promoted, Some(&predecessor_url)).await
}

async fn promote_call(
    deps: &PromoteDeps,
    standby_url: &str,
    digests: Option<BTreeMap<String, String>>,
    keep: Option<Vec<String>>,
) -> anyhow::Result<ssp_protocol::SspPromoteResponse> {
    let (status, body) = deps
        .transport
        .post_to_ssp_with_timeout(
            standby_url,
            "/handover/promote",
            &ssp_protocol::SspPromoteRequest { digests, keep },
            PROMOTE_TIMEOUT,
        )
        .await?;
    anyhow::ensure!(status.is_success(), "promote answered {status}: {body}");
    Ok(serde_json::from_str(&body).unwrap_or_default())
}

async fn commit_or_fail(
    deps: &PromoteDeps,
    standby: &str,
    predecessor: &str,
    promoted: anyhow::Result<ssp_protocol::SspPromoteResponse>,
    predecessor_url: Option<&str>,
) -> anyhow::Result<()> {
    let response = match promoted {
        Ok(r) => r,
        Err(e) => {
            let why = format!("{e:#}");
            return match predecessor_url {
                Some(url) => abandon(deps, standby, predecessor, url, why).await,
                None => {
                    shared().finish(standby, Some(why.clone()), None);
                    Err(anyhow::anyhow!(why))
                }
            };
        }
    };
    let moved = deps.query_tracker.reassign(predecessor, standby).await;
    {
        let mut pool = deps.ssp_pool.write().await;
        pool.clear_standby(standby);
        pool.mark_retired(predecessor);
        pool.transfer_query_count(predecessor, standby);
    }
    crate::admin::incidents::emit(
        standby,
        "promoted",
        "recovered",
        "Standby SSP promoted in place of its predecessor (blue/green upgrade)",
        None,
    );
    info!(
        standby,
        predecessor,
        views = response.views,
        republished = response.republished,
        dropped = response.dropped,
        assignments_moved = moved,
        "SSP standby promoted"
    );
    shared().finish(standby, None, Some(response.republished));
    Ok(())
}

/// Give the swap up: the predecessor serves on and gets back every event it
/// missed while retiring.
async fn abandon(
    deps: &PromoteDeps,
    standby: &str,
    predecessor: &str,
    predecessor_url: &str,
    why: String,
) -> anyhow::Result<()> {
    warn!(standby, predecessor, reason = %why, "Abandoning SSP promotion; resuming the predecessor");
    if let Err(e) = deps
        .transport
        .post_to_ssp_with_timeout(predecessor_url, "/handover/resume", &ssp_protocol::SspResumeRequest {}, Duration::from_secs(30))
        .await
    {
        warn!(predecessor, error = %e, "Resume call failed; the predecessor re-registers if it cannot serve");
    }
    let lagging = deps.ssp_pool.write().await.unretire(predecessor);
    if lagging {
        tokio::spawn(crate::ingest::redeliver_to_lagging_ssp(deps.ingest.clone(), predecessor.to_string()));
    }
    shared().finish(standby, Some(why.clone()), None);
    Err(anyhow::anyhow!(why))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_track_one_promotion_through_its_phases() {
        let h = SspHandover::default();
        h.report("ssp-0-g1", "ssp-0", "holding");
        h.report("ssp-0-g1", "ssp-0", "retiring");
        assert_eq!(h.reports().len(), 1);
        assert_eq!(h.reports()[0].phase, "retiring");
        h.finish("ssp-0-g1", None, Some(3));
        let r = &h.reports()[0];
        assert_eq!(r.phase, "done");
        assert_eq!(r.republished, Some(3));
        assert!(r.finished_ms.is_some());
        // A later promotion of the same id starts a new report.
        h.report("ssp-0-g1", "ssp-0", "holding");
        assert_eq!(h.reports().len(), 2);
    }
}
