//! The scheduler's end of the changefeed tail (see `maintenance::changefeed`):
//! reads `SHOW CHANGES FOR DATABASE` through the shared upstream handle and
//! feeds every committed row change into the same pipeline the HTTP `/ingest`
//! route uses (WAL, buffer, job observer, SSP fan-out).
//!
//! With `SPKY_INGEST_TRANSPORT=changefeed` the generated DB events no longer
//! `http::post` anything: a user write commits without waiting on this
//! process, and this process learns about it a few milliseconds later from
//! the feed. A `_00_query` DELETE in the feed is a view teardown and goes to
//! `query::unregister_local` instead of the WAL.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use serde_json::Value;
use tracing::{error, info, warn};

use maintenance::changefeed::{
    ChangeOp, ChangeRecord, ChangeSink, ChangeSource, ReconnectingSource, SinkError, TailerConfig,
    TailerStats,
};
use maintenance::db::ReconnectingDb;
use ssp_protocol::IngestRequest;

use crate::config::ChangefeedSettings;
use crate::ingest::IngestState;
use crate::query::QueryState;
use crate::SchedulerStatus;

/// The ingest pipeline as a sink.
pub struct IngestSink {
    pub ingest: IngestState,
    pub query: QueryState,
    pub recloner: Arc<dyn crate::drift::Recloner>,
    pub stats: Arc<TailerStats>,
}

#[async_trait]
impl ChangeSink for IngestSink {
    fn ready(&self) -> bool {
        // Same gate as the HTTP route: nowhere to put an event during the
        // initial clone or a restore. Unlike the route this is not an error,
        // the loop simply waits and the feed keeps the changes.
        let status = self
            .ingest
            .status
            .try_read()
            .map(|s| *s)
            .unwrap_or(SchedulerStatus::Ready);
        match status {
            SchedulerStatus::Cloning => self.ingest.snapshot_seq.load(Ordering::Relaxed) > 0,
            SchedulerStatus::Restoring => false,
            _ => true,
        }
    }

    async fn before_image(&self, table: &str, id: &str) -> Option<Value> {
        if ssp_protocol::table_excluded_from_sync(table) {
            return None;
        }
        let replica = self.ingest.replica.read().await;
        match replica.row(table, id).await {
            Ok(row) => row,
            Err(e) => {
                warn!(table, id, error = %e, "Could not read the before-image of a deleted row");
                None
            }
        }
    }

    async fn deliver(&self, record: ChangeRecord) -> Result<(), SinkError> {
        if record.table == "_00_query" {
            if record.op == ChangeOp::Delete {
                let deleted_at = maintenance::changefeed::stamp_ms(record.versionstamp);
                crate::query::unregister_local(&self.query, &record.id, Some(deleted_at)).await;
            }
            return Ok(());
        }
        let request = IngestRequest {
            table: record.table,
            op: record.op.as_str().to_string(),
            id: record.id,
            record: record
                .record
                .unwrap_or_else(|| Value::Object(Default::default())),
            job_assignee: None,
        };
        match crate::ingest::ingest_event(&self.ingest, request, record.versionstamp).await {
            Ok(_) => Ok(()),
            Err((StatusCode::BAD_REQUEST, reason)) => {
                // Malformed for good; retrying would loop forever on it.
                error!(
                    reason,
                    "Changefeed record rejected by the ingest path; skipped"
                );
                Ok(())
            }
            Err((status, reason)) => Err(SinkError::Retry(format!("{status}: {reason}"))),
        }
    }

    async fn on_gap(&self) -> anyhow::Result<()> {
        crate::admin::incidents::emit(
            "scheduler",
            "changefeed_gap",
            "open",
            "Changefeed cursor fell behind the retained window; re-cloning the replica and re-bootstrapping every SSP",
            None,
        );
        match self.recloner.reclone_and_resync().await {
            Ok(true) => {
                crate::admin::incidents::emit(
                    "scheduler",
                    "changefeed_gap",
                    "recovered",
                    "Replica re-cloned; changefeed tail resumed from a fresh cursor",
                    None,
                );
                Ok(())
            }
            Ok(false) => anyhow::bail!("a re-clone is already running"),
            Err(e) => Err(e),
        }
    }
}

/// Start the tail (and its doorbell) against the shared upstream handle.
///
/// `stats.cursor` must already hold the resume point, or 0 to start from
/// just before now (see `Scheduler::spawn_changefeed_tail`).
pub fn spawn(
    db: Arc<ReconnectingDb>,
    db_config: maintenance::db::DbConfig,
    ingest: IngestState,
    query: QueryState,
    recloner: Arc<dyn crate::drift::Recloner>,
    settings: &ChangefeedSettings,
    stats: Arc<TailerStats>,
    notify: Arc<tokio::sync::Notify>,
) {
    let cfg: TailerConfig = settings.tailer_config();
    if settings.doorbell {
        maintenance::doorbell::spawn(
            db_config,
            maintenance::changefeed::DOORBELL_TABLE.to_string(),
            Arc::clone(&notify),
            Arc::clone(&stats),
        );
    } else {
        info!(
            "Changefeed doorbell disabled (SPKY_CHANGEFEED_DOORBELL=false); polling every {}ms",
            settings.fallback_ms
        );
    }
    let source: Arc<dyn ChangeSource> = Arc::new(ReconnectingSource { db });
    let sink: Arc<dyn ChangeSink> = Arc::new(IngestSink {
        ingest,
        query,
        recloner,
        stats: Arc::clone(&stats),
    });
    tokio::spawn(maintenance::changefeed::run_tailer(
        source, sink, cfg, stats, notify,
    ));
}
