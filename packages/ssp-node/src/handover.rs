//! Blue/green SSP handover: standby, retire, promote, resume.
//!
//! A replacement SSP registers with `replaces: <live ssp>` and, when the
//! scheduler answers `standby: true`, boots like any SSP but writes nothing to
//! the database: it follows ingest and the registration set (the scheduler
//! forwards it a shadow of every register/unregister) so its circuit tracks
//! its predecessor's. Once it has caught up the scheduler retires the
//! predecessor, which drains its publication queue and reports the membership
//! digest of every registration, and then promotes the standby, which
//! republishes only the registrations whose digest it does not reproduce.
//!
//! State, and why it lives where it does (no new `SspNode` field, so every
//! shell constructing a node keeps compiling unchanged):
//!
//! - **standby** is the [`crate::edges::EdgePublisher`]'s flag: the publisher
//!   is the boundary every edge write crosses, and it drops all work while the
//!   flag is set. [`SspNode::is_standby`] reads it there, and every other
//!   writer (the `_00_query` writes on registration, unregister edge deletes,
//!   job pickup, the TTL sweep, the metrics flush, row checkpoints in the VM
//!   shell) consults that one flag.
//! - **retired** is [`SspStatus::Retired`]: everything already gated on
//!   `Ready` (ingest, registrations, timers, schema poll, row checkpoints)
//!   stops by itself.
//! - job claims stop through [`crate::jobs::JobDispatcher::set_paused`] in
//!   both states.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde_json::{json, Value};
use tracing::{info, warn};

use ssp_protocol::{SspPromoteRequest, SspPromoteResponse, SspRetireRequest, SspRetireResponse};

use crate::api::{ApiRequest, ApiResponse};
use crate::node::SspNode;
use crate::status::{error_codes, SspStatus};

/// How long `POST /handover/retire` waits for the publication queue to drain
/// unless the shell says otherwise (VM: `SPKY_SSP_RETIRE_DRAIN_SECS`).
pub const DEFAULT_RETIRE_DRAIN: Duration = Duration::from_secs(60);

/// Values a shell reads from its environment for [`SspNode::route_with`].
#[derive(Debug, Clone)]
pub struct RouteOptions {
    /// Publication drain budget for `POST /handover/retire`.
    pub retire_drain: Duration,
}

impl Default for RouteOptions {
    fn default() -> Self {
        Self { retire_drain: DEFAULT_RETIRE_DRAIN }
    }
}

fn err_json(status: u16, code: &str, message: impl Into<String>) -> ApiResponse {
    ApiResponse::json(status, json!({ "code": code, "message": message.into() }))
}

impl SspNode {
    /// Whether this node is a blue/green standby that must not write to the
    /// database. See the module docs for where the flag lives.
    pub fn is_standby(&self) -> bool {
        self.edge_update_tx.is_standby()
    }

    /// Enter standby (the registration answered `standby: true`) or leave it
    /// without a promotion (a later registration answered as a normal SSP,
    /// whose boot then republishes everything). Call while not serving
    /// (`Bootstrapping`); a serving standby leaves only through
    /// [`Self::handover_promote`].
    pub fn set_standby(&self, standby: bool) {
        self.edge_update_tx.set_standby(standby);
        self.job_dispatcher.set_paused(standby);
    }

    /// 503 for a route that writes to the database when this node may not:
    /// `retired` once handed over, `standby` before promotion.
    pub(crate) async fn handover_gate(&self) -> Option<ApiResponse> {
        if *self.status.read().await == SspStatus::Retired {
            return Some(err_json(503, error_codes::RETIRED, "SSP is retired; its successor serves this tenant"));
        }
        if self.is_standby() {
            return Some(err_json(503, error_codes::STANDBY, "SSP is a standby and writes nothing until promoted"));
        }
        None
    }

    pub(crate) async fn handover_retire_handler(&self, req: &ApiRequest, drain: Duration) -> ApiResponse {
        let Ok(body) = serde_json::from_slice::<SspRetireRequest>(&req.body) else {
            return err_json(422, "bad_body", "invalid retire payload");
        };
        match self.handover_retire(body, drain).await {
            Ok(resp) => ApiResponse::json(200, json!(resp)),
            Err(refused) => refused,
        }
    }

    pub(crate) async fn handover_promote_handler(&self, req: &ApiRequest) -> ApiResponse {
        let Ok(body) = serde_json::from_slice::<SspPromoteRequest>(&req.body) else {
            return err_json(422, "bad_body", "invalid promote payload");
        };
        match self.handover_promote(body).await {
            Ok(resp) => ApiResponse::json(200, json!(resp)),
            Err(refused) => refused,
        }
    }

    /// `POST /handover/retire`: stop serving in favour of `req.successor`.
    ///
    /// Ingest and registrations answer 503 `retired` from here on, and the
    /// node's own writers stop: TTL sweep, metrics flush and schema poll (all
    /// gated on `Ready`), new job claims (running jobs finish), row
    /// checkpoints (the successor owns the snapshot dir now). Then the
    /// publication queue drains, at most `drain`, and the answer carries the
    /// digest of every registration as published. Idempotent.
    pub async fn handover_retire(&self, req: SspRetireRequest, drain: Duration) -> Result<SspRetireResponse, ApiResponse> {
        {
            let mut status = self.status.write().await;
            match *status {
                SspStatus::Retired => {}
                SspStatus::Ready if self.is_standby() => {
                    return Err(err_json(409, error_codes::STANDBY, "a standby has nothing to hand over"));
                }
                SspStatus::Ready => *status = SspStatus::Retired,
                other => {
                    return Err(err_json(503, error_codes::NOT_READY, format!("SSP is in {other:?} state")));
                }
            }
        }
        self.job_dispatcher.set_paused(true);
        info!(successor = %req.successor, drain_secs = drain.as_secs(), "Retiring: waiting for the publication queue to drain");

        let started = web_time::Instant::now();
        let drained = self.edge_update_tx.wait_drained(self.platform.scheduler.as_ref(), drain).await;
        // Under the publication gate: a TTL sweep or unregister that started
        // before the status flip finishes (and its circuit change lands)
        // before the digests are taken.
        let digests = {
            let _publication = self.publication_gate.lock().await;
            self.processor.read().await.membership_digests()
        };
        if drained {
            info!(successor = %req.successor, views = digests.len(), ms = started.elapsed().as_millis() as u64, "Retired: publication drained");
        } else {
            let pending = self.edge_update_tx.snapshot();
            warn!(
                successor = %req.successor,
                views = digests.len(),
                pending_batches = pending.pending_batches,
                parked_batches = pending.parked_batches,
                "Retired with publications still pending: the digests describe intent, not the database"
            );
        }
        Ok(SspRetireResponse { digests, drained })
    }

    /// `POST /handover/resume`: take back a retire whose promotion failed.
    /// Always 200; a node that is not retired is left as it is.
    pub async fn handover_resume(&self) -> ApiResponse {
        let resumed = {
            let mut status = self.status.write().await;
            let retired = *status == SspStatus::Retired;
            if retired {
                *status = SspStatus::Ready;
            }
            retired
        };
        if resumed {
            self.job_dispatcher.set_paused(false);
            info!("Handover rolled back: serving again");
        }
        let status = *self.status.read().await;
        ApiResponse::json(200, json!({ "status": status, "resumed": resumed }))
    }

    /// `POST /handover/promote`: this standby takes over from its retired
    /// predecessor.
    ///
    /// Ingest keeps arriving throughout (the scheduler broadcasts to the
    /// standby the whole time), so the comparison and the flip out of standby
    /// happen in one critical section under the circuit write lock, which
    /// every ingest step holds while it enqueues: a delta computed before the
    /// flip was dropped and is covered by the comparison, one computed after
    /// it is published. The republished snapshots are enqueued inside the
    /// same section, so every later delta of the same view queues behind its
    /// snapshot. The publication gate is held from before that section until
    /// the `_00_query` metadata is written, so no edge lands before its row
    /// says `materializing`.
    ///
    /// Registration changes are held by the scheduler meanwhile. Not standby:
    /// 200 with zeros, nothing done.
    pub async fn handover_promote(&self, req: SspPromoteRequest) -> Result<SspPromoteResponse, ApiResponse> {
        if !self.is_standby() {
            return Ok(SspPromoteResponse::default());
        }
        let status = *self.status.read().await;
        if status != SspStatus::Ready {
            return Err(err_json(503, error_codes::NOT_READY, format!("standby is in {status:?} state")));
        }
        let digests: Option<HashMap<String, String>> = req
            .digests
            .map(|d| d.into_iter().map(|(id, digest)| (ssp::canonical_query_id(&id), digest)).collect());
        let keep: Option<HashSet<String>> =
            req.keep.map(|k| k.iter().map(|id| ssp::canonical_query_id(id)).collect());
        let kept = |id: &str| keep.as_ref().is_none_or(|k| k.contains(id));

        // 1. Upstream reads, before any lock: the `_00_query` rows behind
        //    every registration the digests do not vouch for (do they still
        //    exist?) and behind every digest this standby does not hold (to
        //    register it). Only when there is such a registration.
        let registered: HashSet<String> = self.processor.read().await.registration_ids().into_iter().collect();
        let unconfirmed = registered
            .iter()
            .any(|id| kept(id) && digests.as_ref().is_none_or(|d| !d.contains_key(id)));
        let unknown = digests
            .as_ref()
            .is_some_and(|d| d.keys().any(|id| kept(id) && !registered.contains(id)));
        let rows: Option<HashMap<String, Value>> = if unconfirmed || unknown {
            Some(self.read_persisted_views().await.map_err(|e| {
                warn!(error = %e, "Promotion aborted: could not read _00_query");
                err_json(503, "db_error", format!("could not read _00_query: {e}"))
            })?)
        } else {
            None
        };

        // 2. The critical section.
        let publication = self.publication_gate.lock().await;
        // Capacity first, so running out of it refuses the promotion before
        // anything changed rather than after.
        let Some(permit) = self.publication_admission(0) else {
            return Err(err_json(503, "publication_backlog", "Publication backlog is full; retry the promotion"));
        };
        let mut dropped_ids = Vec::new();
        let mut vanished_ids = Vec::new();
        let mut registered_ids = Vec::new();
        let (metadata, views) = {
            let mut circuit = self.processor.write().await;
            if !self.is_standby() {
                // A concurrent promotion got here first.
                return Ok(SspPromoteResponse::default());
            }

            // 2a. Registrations another SSP owns: out of the circuit, their
            //     edges untouched.
            if keep.is_some() {
                for id in circuit.registration_ids() {
                    if !kept(&id) {
                        self.edge_update_tx.invalidate_view(&id);
                        circuit.detach_subscriber(&id);
                        dropped_ids.push(id);
                    }
                }
            }

            // 2b. Registrations the predecessor held and this standby does
            //     not (a shadow registration that never arrived): register
            //     them from their rows, as a boot would. A row that is gone
            //     is skipped.
            if let (Some(digests), Some(rows)) = (&digests, &rows) {
                for id in digests.keys() {
                    if !kept(id) || circuit.registration(id).is_some() {
                        continue;
                    }
                    if let Some(row) = rows.get(id) {
                        if let Some(id) = crate::bootstrap::register_persisted_view(&mut circuit, row) {
                            registered_ids.push(id);
                        }
                    }
                }
            }

            // 2c. Compare. Republish what the digests do not vouch for; drop
            //     an unvouched registration whose row is gone (swept by the
            //     predecessor's TTL sweep, which a standby never hears of).
            let mine = circuit.membership_digests();
            let mut republish = Vec::new();
            for (id, digest) in &mine {
                match digests.as_ref().and_then(|d| d.get(id)) {
                    Some(theirs) if theirs == digest => {}
                    Some(_) => republish.push(id.clone()),
                    // No evidence the row is gone (nothing was read): publish.
                    None if rows.as_ref().is_none_or(|rows| rows.contains_key(id)) => republish.push(id.clone()),
                    None => vanished_ids.push(id.clone()),
                }
            }
            for id in &vanished_ids {
                self.edge_update_tx.invalidate_view(id);
                circuit.detach_subscriber(id);
            }

            // 2d. Snapshot what to republish, at this exact point.
            let mut deltas = Vec::with_capacity(republish.len());
            for id in &republish {
                let Some((graph, auth)) = circuit.registration(id) else { continue };
                if let Some(delta) = circuit.snapshot_delta_from(&graph, id.clone(), auth) {
                    deltas.push(delta);
                }
            }
            let metadata: Vec<(String, usize)> = deltas.iter().map(|d| (d.query_id.clone(), d.row_count)).collect();

            // 2e. Out of standby, then the snapshots, still under the lock.
            self.edge_update_tx.set_standby(false);
            if deltas.is_empty() {
                drop(permit);
            } else {
                self.edge_update_tx.enqueue(permit, deltas, &circuit, None, false, vec![]);
            }
            (metadata, circuit.registration_ids().len())
        };

        // 3. Each republished row says `materializing` before its edges land:
        //    the publisher is waiting on the gate held here. The snapshot's
        //    full publish flips it to `ready` in the edge transaction.
        for (id, count) in &metadata {
            if let Err(e) = crate::db_retry::query_retrying(
                self.platform.db.as_ref(),
                "UPDATE type::record('_00_query', $qid) SET rowCount = $count, state = 'materializing'",
                &[("qid", json!(id)), ("count", json!(count))],
            )
            .await
            {
                warn!(view_id = %id, error = %e, "Promotion: could not mark the view materializing; publishing its edges anyway");
            }
        }
        drop(publication);

        {
            let mut metrics = self.view_metrics.write().await;
            for id in dropped_ids.iter().chain(&vanished_ids) {
                metrics.remove(id);
            }
            for id in &registered_ids {
                metrics.entry(id.clone()).or_default();
            }
        }
        let net = registered_ids.len() as i64 - (dropped_ids.len() + vanished_ids.len()) as i64;
        if net != 0 {
            self.platform.telemetry.gauge_add("view_count", net);
        }
        self.job_dispatcher.set_paused(false);

        info!(
            views,
            republished = metadata.len(),
            dropped = dropped_ids.len(),
            vanished = vanished_ids.len(),
            registered = registered_ids.len(),
            digests = digests.as_ref().map(|d| d.len() as i64).unwrap_or(-1),
            "Promoted out of standby: publishing"
        );
        Ok(SspPromoteResponse { views, republished: metadata.len(), dropped: dropped_ids.len() })
    }

    /// Every `_00_query` row, by registration key, with the columns a boot
    /// re-registers from.
    async fn read_persisted_views(&self) -> Result<HashMap<String, Value>, crate::ports::DbError> {
        let sql = format!("SELECT {} FROM _00_query", crate::bootstrap::PERSISTED_VIEW_FIELDS);
        let rows = match self.platform.db.query(&sql, &[]).await?.into_iter().next() {
            Some(Value::Array(rows)) => rows,
            Some(row @ Value::Object(_)) => vec![row],
            _ => Vec::new(),
        };
        Ok(rows
            .into_iter()
            .filter_map(|row| Some((crate::bootstrap::persisted_view_key(&row)?, row)))
            .collect())
    }
}
