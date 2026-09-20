//! Cluster-mode host for the shared scheduling engine.
//!
//! Same `schedule-core` engine the singlenode SSP runs, with the two
//! capabilities supplied in their cluster form:
//!
//! - [`RemoteDb`] talks to the upstream SurrealDB over the maintenance HTTP
//!   client, reconnecting on the next tick if a pass fails (the pattern
//!   `start_job_recovery_sweep` already uses).
//! - [`ClusterJobKill`] broadcasts `/job/kill` to every ready SSP, because the
//!   scheduler cannot know which one claimed the row — the same reasoning as
//!   `cluster_job_kill`.
//!
//! **The scheduler is the only ticker in a cluster.** SSPs leave
//! `schedule_engine` as `None`, so there is exactly one fire per schedule by
//! construction; the engine's claim compare-and-swap is the backstop that keeps
//! even an accidental second ticker harmless rather than double-firing.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use schedule_core::{EngineConfig, JobKill, ScheduleDb, ScheduleDbError, ScheduleEngine};
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::job_scheduler::JobActionRequest;
use crate::router::SspPool;
use crate::transport::{HttpTransport, SspInfo};

/// Sweep cadence, matching the singlenode `SCHEDULE_SWEEP_INTERVAL_SECS`: fast
/// enough that `spky schedules trigger` feels immediate and a lost
/// job-completion event heals quickly.
const SCHEDULE_SWEEP_INTERVAL_SECS: u64 = 5;

/// `ScheduleDb` over the scheduler's shared re-signing SurrealDB session.
///
/// Every query takes the current handle, so a token refresh or a reconnect
/// behind `ReconnectingDb` is picked up on the next statement. A private
/// connection here lost one sweep pass per hour to `401 Unauthorized` (root
/// signins live one hour; whitepawn 2026-09-15 at 23:00:07 and 00:00:17).
struct RemoteDb(Arc<maintenance::db::ReconnectingDb>);

#[async_trait::async_trait]
impl ScheduleDb for RemoteDb {
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, Value)],
    ) -> Result<Vec<Value>, ScheduleDbError> {
        let handle = self.0.handle();
        let mut q = handle.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = q.await.map_err(|e| {
            let msg = e.to_string();
            self.0.note_error(&msg);
            ScheduleDbError::Transport(msg)
        })?;
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value =
                response.take(i).map_err(|e| ScheduleDbError::Query(e.to_string()))?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
}

/// Kill a job by broadcasting to every ready SSP.
///
/// Best-effort by nature: only the SSP holding the in-flight request actually
/// cancels it, the rest just set a kill flag, and a job that already finished
/// ignores it entirely. Every engine write that follows a kill is status-guarded,
/// so a late completion cannot overwrite the terminal state the engine chose.
struct ClusterJobKill {
    ssp_pool: Arc<RwLock<SspPool>>,
    transport: Arc<HttpTransport>,
}

#[async_trait::async_trait]
impl JobKill for ClusterJobKill {
    async fn kill(&self, job_id: &str) -> Result<()> {
        let ready: Vec<SspInfo> = {
            let pool = self.ssp_pool.read().await;
            pool.all().into_iter().filter(|s| pool.is_ready(&s.id)).cloned().collect()
        };
        if ready.is_empty() {
            warn!(job_id, "schedule engine wanted to kill a job but no SSP is ready");
            return Ok(());
        }
        let req = JobActionRequest { id: job_id.to_string() };
        let results = self.transport.broadcast_to_ssps(&ready, "/job/kill", &req).await;
        let dispatched = results.iter().filter(|(_, r)| r.is_ok()).count();
        debug!(job_id, dispatched, ssps = ready.len(), "schedule engine dispatched a job kill");
        Ok(())
    }
}

/// `ScheduleDb` over the scheduler's shared reconnecting root handle.
///
/// The admin plane runs operator actions (rerun, retry, cancel) through this
/// rather than opening a session per request like the sweep does: the handle
/// already exists, and a dead session is reported back so the reconnect task
/// replaces it instead of every subsequent action failing the same way.
pub struct SharedDb(pub Arc<maintenance::db::ReconnectingDb>);

#[async_trait::async_trait]
impl ScheduleDb for SharedDb {
    async fn query(
        &self,
        surql: &str,
        binds: &[(&str, Value)],
    ) -> Result<Vec<Value>, ScheduleDbError> {
        // Never clone the inner `Surreal`: that mints a new server-side session.
        let handle = self.0.handle();
        let mut q = handle.query(surql);
        for (name, value) in binds {
            q = q.bind(((*name).to_string(), value.clone()));
        }
        let mut response = match q.await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                self.0.note_error(&msg);
                return Err(ScheduleDbError::Transport(msg));
            }
        };
        let n = response.num_statements();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let val: surrealdb::types::Value = response.take(i).map_err(|e| {
                let msg = e.to_string();
                self.0.note_error(&msg);
                ScheduleDbError::Query(msg)
            })?;
            out.push(val.into_json_value());
        }
        Ok(out)
    }
}

/// Assemble an engine over any database port, with the cluster kill capability.
/// Cheap to build per request; it holds no state beyond the prune clock.
pub fn build_engine_over(
    db: Arc<dyn ScheduleDb>,
    ssp_pool: Arc<RwLock<SspPool>>,
    transport: Arc<HttpTransport>,
) -> ScheduleEngine {
    ScheduleEngine::new(
        db,
        Arc::new(ClusterJobKill { ssp_pool, transport }),
        EngineConfig::default(),
    )
}

fn build_engine(
    db: Arc<maintenance::db::ReconnectingDb>,
    ssp_pool: Arc<RwLock<SspPool>>,
    transport: Arc<HttpTransport>,
) -> ScheduleEngine {
    build_engine_over(Arc::new(RemoteDb(db)), ssp_pool, transport)
}

/// Start the cluster schedule sweep. One task, one ticker, for the whole cluster.
///
/// Reads and writes through the admin session slot (a `ReconnectingDb`); the
/// slot is empty until `Scheduler::start` has connected, and a tick that finds
/// it empty waits for the next one.
pub fn start_schedule_sweep(
    ssp_pool: Arc<RwLock<SspPool>>,
    transport: Arc<HttpTransport>,
    db_slot: crate::admin::SharedDbSlot,
) {
    tokio::spawn(async move {
        let mut engine: Option<ScheduleEngine> = None;
        let mut interval = tokio::time::interval(Duration::from_secs(SCHEDULE_SWEEP_INTERVAL_SECS));
        loop {
            interval.tick().await;

            if engine.is_none() {
                let Some(db) = db_slot.read().await.clone() else {
                    debug!("Schedule sweep: database session not ready yet");
                    continue;
                };
                engine = Some(build_engine(db, Arc::clone(&ssp_pool), Arc::clone(&transport)));
            }

            match engine.as_ref().unwrap().tick_pass().await {
                Ok(report) => {
                    // The engine is a library beneath this crate and cannot
                    // reach the incident recorder; it reports key transitions
                    // and the host turns them into incidents, which is how
                    // every other operator-facing surface here works.
                    for t in &report.quarantined {
                        let (state, summary) = if t.suppressed {
                            (
                                "open",
                                format!(
                                    "Schedule '{}' stopped firing key {} after {} consecutive failures",
                                    t.schedule, t.key, t.failures
                                ),
                            )
                        } else {
                            (
                                "recovered",
                                format!(
                                    "Schedule '{}' is firing key {} again",
                                    t.schedule, t.key
                                ),
                            )
                        };
                        // Component is the key, not "scheduler": the
                        // recorder correlates an open episode by component, so
                        // two quarantined keys must not collapse into one
                        // incident that either recovery then closes.
                        crate::admin::incidents::emit(
                            &format!("schedule:{}/{}", t.schedule, t.key),
                            "schedule_quarantined",
                            state,
                            &summary,
                            None,
                        );
                    }
                    if report != Default::default() {
                        debug!(?report, "schedule sweep");
                    }
                }
                // The shared handle heals itself (`note_error` in `RemoteDb`);
                // the next tick simply runs against the refreshed session.
                Err(e) => error!(error = %e, "Schedule sweep pass failed"),
            }
        }
    });
    info!(
        interval_secs = SCHEDULE_SWEEP_INTERVAL_SECS,
        "Cluster schedule sweep started"
    );
}

/// Observe a job that may have just reached a terminal status.
///
/// Called from the ingest path, which is the scheduler's own HTTP surface and
/// therefore sees every job-status write in the cluster. Spawned rather than
/// awaited so advancing a DAG never delays the ingest response, and best-effort
/// because the engine's heal pass reaches the same conclusion within one sweep.
pub fn observe_job_terminal(
    ssp_pool: Arc<RwLock<SspPool>>,
    transport: Arc<HttpTransport>,
    db_slot: crate::admin::SharedDbSlot,
    permits: Arc<tokio::sync::Semaphore>,
    job_id: String,
    status: String,
) {
    if !matches!(status.as_str(), "success" | "failed") {
        return;
    }
    // Reuse the scheduler connection: opening a session for every completed
    // job can exhaust the upstream HTTP session limit. Keep work bounded and
    // time-boxed; the sweep heals events dropped on saturation or timeout.
    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
        debug!(job_id = %job_id, "schedule observers saturated; leaving it to the sweep");
        return;
    };
    tokio::spawn(async move {
        let _permit = permit;
        let work = async {
            let Some(db) = db_slot.read().await.clone() else {
                debug!("schedule observer waiting for scheduler initialization; leaving it to the sweep");
                return;
            };
            let engine = build_engine_over(Arc::new(SharedDb(db)), ssp_pool, transport);
            match engine.observe_job_terminal(&job_id, &status).await {
                Ok(true) => debug!(job_id = %job_id, %status, "schedule engine advanced on job completion"),
                Ok(false) => {}
                Err(e) => warn!(job_id = %job_id, error = %e, "schedule observer failed"),
            }
        };
        if tokio::time::timeout(Duration::from_secs(10), work).await.is_err() {
            warn!(job_id = %job_id, "schedule observer timed out; leaving it to the sweep");
        }
    });
}

/// Job tables the engine's spawned rows live in, from `SPKY_JOB_CONFIG`. Used by
/// the ingest hook to skip observing tables that can't hold jobs.
pub fn job_tables_from_env() -> Vec<String> {
    let raw = std::env::var("SPKY_JOB_CONFIG").unwrap_or_default();
    let Ok(parsed) = serde_json::from_str::<Value>(&raw) else {
        return Vec::new();
    };
    match parsed {
        // The canonical array shape: [{ name, table, base_url, ... }].
        Value::Array(entries) => entries
            .iter()
            .filter_map(|e| e.get("table").and_then(Value::as_str).map(str::to_string))
            .collect(),
        // Tolerate the object-keyed-by-table shape the recovery sweep also accepts.
        Value::Object(map) => map.keys().cloned().collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn reads_both_job_config_shapes() {
        // Not run in parallel with anything that mutates the env in this module.
        std::env::set_var(
            "SPKY_JOB_CONFIG",
            json!([{ "name": "api", "table": "job", "base_url": "http://b" }]).to_string(),
        );
        assert_eq!(job_tables_from_env(), vec!["job".to_string()]);

        std::env::set_var("SPKY_JOB_CONFIG", json!({ "statistics_job": {} }).to_string());
        assert_eq!(job_tables_from_env(), vec!["statistics_job".to_string()]);

        std::env::set_var("SPKY_JOB_CONFIG", "null");
        assert!(job_tables_from_env().is_empty(), "a disabled runner observes nothing");

        std::env::remove_var("SPKY_JOB_CONFIG");
        assert!(job_tables_from_env().is_empty());
    }
}
