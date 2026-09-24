//! Following upstream's table set while the node runs.
//!
//! A circuit's per-table schema (select permission, link targets, opaque
//! fields, columns) used to be read only by a full bootstrap, so a table a
//! deploy added was default-denied at registration, a subquery on it was
//! degraded to empty, and a removed table lingered, all until a restart or an
//! `/admin/reload` rebuilt everything. [`SspNode::refresh_schema`] instead
//! applies what changed in place: the metadata of added or changed tables, and
//! a dropped table's rows stepped out of every view.
//!
//! It runs on the `SchemaPoll` timer and, rate-limited, when a registration
//! names a table the circuit has no schema for: the window between a deploy
//! and the next poll is exactly when clients on the new app version register
//! views on the new table. The decisions (what was added, what is confirmed
//! gone, whether the metadata must be re-read) are
//! [`ssp_protocol::schema::SchemaTracker`]'s, shared with the scheduler.

use std::collections::BTreeSet;

use ssp::circuit::{Change, ChangeSet};
use ssp_protocol::schema::SchemaTracker;
use tracing::{info, warn};

use crate::node::SspNode;
use crate::status::SspStatus;

/// Rows one `drop_table` step retracts under a single publication permit.
const DROP_CHUNK: usize = 500;

/// A registration-triggered refresh runs at most this often, so a client
/// registering views on a table that really does not exist cannot turn into a
/// schema read per request.
const ON_MISS_MIN_INTERVAL_MS: u64 = 1000;

/// The node's schema-following state. `Default` is "never probed": the first
/// refresh re-reads every table's metadata once, which is what the bootstrap
/// just loaded, and logs nothing as added.
#[derive(Default)]
pub struct SchemaWatch {
    /// Held across a whole refresh: refreshes are single-flight.
    state: tokio::sync::Mutex<WatchState>,
    last_on_miss_ms: std::sync::Mutex<u64>,
}

#[derive(Default)]
struct WatchState {
    tracker: SchemaTracker,
    primed: bool,
}

impl SspNode {
    /// Probe upstream's schema and apply what changed. Returns whether the
    /// circuit's schema changed. `drop` also retracts tables upstream no longer
    /// syncs; the registration path passes `false`, because a drop publishes
    /// through the same admission it is itself holding a place in.
    ///
    /// Only while `Ready`: during bootstrap and replay the table set is being
    /// verified against the scheduler's, and changing it underneath would turn
    /// a clean bootstrap into a dispute.
    pub async fn refresh_schema(&self, drop: bool) -> anyhow::Result<bool> {
        if *self.status.read().await != SspStatus::Ready {
            return Ok(false);
        }
        let mut st = self.schema_watch.state.lock().await;
        let db = self.platform.db.as_ref();
        let Some(probe) = crate::bootstrap::probe_schema(db).await? else {
            return Ok(false);
        };
        // What the circuit syncs is what it holds a permission for. Not the
        // collection names: ingest creates collections for runtime `_00_*`
        // tables (`_00_heartbeat`) that no schema will ever list.
        let held: BTreeSet<String> = self.processor.read().await.permissions().keys().cloned().collect();
        let step = st.tracker.observe(&probe, &held);
        let primed = std::mem::replace(&mut st.primed, true);
        let mut changed = false;

        if step.reload {
            let schema = crate::bootstrap::load_schema(db).await?;
            {
                let mut circuit = self.processor.write().await;
                for (table, meta) in &schema.tables {
                    let before = circuit.table_meta(table);
                    if before.as_ref() == Some(meta) {
                        continue;
                    }
                    if primed {
                        match before {
                            None => info!(target: "ssp::policy", table = %table, permission = %meta.permission, "Schema: table added upstream; now synced"),
                            Some(_) => info!(target: "ssp::policy", table = %table, permission = %meta.permission, "Schema: table definition changed upstream; metadata reloaded"),
                        }
                    }
                    circuit.set_table_meta(table, meta.clone());
                    changed = true;
                }
            }
            if let Some(loaded) = &schema.probe {
                st.tracker.loaded(&loaded.fingerprint);
            }
            if changed {
                // Allowlist entries are compiled against the link map.
                self.refresh_query_allowlist().await;
            }
        }

        if drop {
            for table in step.removed {
                if table == "user" {
                    // Dropping `user` releases every view its rows own, one
                    // row at a time; that is a rebuild, not a step.
                    warn!(table = %table, "Schema: table no longer synced upstream; not dropped live, POST /admin/reload to drop it");
                    continue;
                }
                match self.drop_table(&table).await {
                    Ok(rows) => {
                        info!(table = %table, rows, "Schema: table no longer synced upstream; dropped");
                        changed = true;
                    }
                    Err(e) => warn!(table = %table, error = %e, "Schema: could not drop table yet; retrying at the next poll"),
                }
            }
        }
        Ok(changed)
    }

    /// [`Self::refresh_schema`] for a registration that named a table the
    /// circuit has no schema for. Never drops, and rate-limited.
    pub async fn refresh_schema_on_miss(&self) -> bool {
        {
            let now = crate::now_epoch_ms();
            let mut last = self.schema_watch.last_on_miss_ms.lock().unwrap();
            if now.saturating_sub(*last) < ON_MISS_MIN_INTERVAL_MS {
                return false;
            }
            *last = now;
        }
        match self.refresh_schema(false).await {
            Ok(changed) => changed,
            Err(e) => {
                warn!(error = %e, "Schema refresh for an unknown table failed");
                false
            }
        }
    }

    /// Tables a prepared plan scans that the circuit holds no schema for. A
    /// root scan on one fails registration outright; a subquery on one is
    /// degraded to empty but keeps its inner scan, so it shows up here too.
    pub async fn unknown_tables(&self, plan: &ssp::operator::plan::OperatorPlan) -> Vec<String> {
        let circuit = self.processor.read().await;
        plan.referenced_tables()
            .into_iter()
            .filter(|t| !t.is_empty() && !t.starts_with("_00_") && !circuit.permissions().contains_key(t))
            .collect()
    }

    /// Retract every row of `table` from the circuit's views, publishing the
    /// deltas and the per-row edge cleanup exactly as ingested deletes would
    /// (`REMOVE TABLE` fires no per-row events, so nothing else will), then
    /// forget the table. Returns the rows dropped. Idempotent: an interrupted
    /// drop leaves the rest of the rows for the next attempt.
    pub(crate) async fn drop_table(&self, table: &str) -> anyhow::Result<usize> {
        let mut dropped = 0;
        loop {
            let chunk: Vec<(String, serde_json::Value)> = {
                let circuit = self.processor.read().await;
                match circuit.store.get_collection(table) {
                    Some(coll) => coll
                        .rows
                        .keys()
                        .take(DROP_CHUNK)
                        .map(|id| {
                            let row = circuit.record(table, id).unwrap_or_default();
                            (id.to_string(), row)
                        })
                        .collect(),
                    None => Vec::new(),
                }
            };
            if chunk.is_empty() {
                self.processor.write().await.forget_table(table);
                return Ok(dropped);
            }
            let Some(permit) = self.publication_admission(chunk.len() * 256) else {
                anyhow::bail!("publication backlog is full");
            };
            let mut circuit = self.processor.write().await;
            if !self.edge_update_tx.is_current(&permit) || *self.status.read().await != SspStatus::Ready {
                anyhow::bail!("circuit restarted");
            }
            let mut cleanup = Vec::new();
            let changes = chunk
                .iter()
                .map(|(id, row)| {
                    cleanup.extend(crate::node::delete_cleanup(
                        self.ref_mode,
                        self.anonymous_live_queries,
                        &format!("{table}:{id}"),
                        row,
                    ));
                    Change::delete(table, id)
                })
                .collect();
            let deltas = circuit.step(ChangeSet { changes });
            self.edge_update_tx.enqueue(permit, deltas, &circuit, None, false, cleanup);
            if circuit.contains(table, &chunk[0].0) {
                // A delete that removes nothing would loop here forever.
                anyhow::bail!("a stepped delete left `{table}:{}` in the store", chunk[0].0);
            }
            dropped += chunk.len();
        }
    }
}
