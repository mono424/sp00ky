//! The standalone SSP's changefeed tail. In cluster mode the scheduler tails
//! the feed and fans out over `/ingest`; a standalone SSP has no scheduler,
//! so it reads `SHOW CHANGES` itself and feeds its own `/ingest` route
//! in-process. Same core, same env keys as the scheduler
//! (`maintenance::changefeed`).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use maintenance::changefeed::{
    ChangeOp, ChangeRecord, ChangeSink, ChangeSource, ChangefeedSettings, IngestTransport,
    ReconnectingSource, SinkError, TailerStats,
};
use ssp::circuit::Circuit;
use ssp_node::SspStatus;

/// This node's ingest and unregister routes as a sink.
struct NodeSink {
    node: Arc<ssp_node::SspNode>,
    processor: Arc<RwLock<Circuit>>,
    status: Arc<RwLock<SspStatus>>,
    auth_secret: String,
}

impl NodeSink {
    async fn post(&self, path: &str, body: Value) -> Option<ssp_node::ApiResponse> {
        self.node
            .route(ssp_node::ApiRequest {
                method: ssp_node::Method::Post,
                path: path.to_string(),
                bearer: Some(self.auth_secret.clone()),
                body: axum::body::Bytes::from(body.to_string()),
            })
            .await
    }
}

#[async_trait]
impl ChangeSink for NodeSink {
    fn ready(&self) -> bool {
        self.status
            .try_read()
            .map(|s| *s == SspStatus::Ready)
            .unwrap_or(false)
    }

    async fn before_image(&self, table: &str, id: &str) -> Option<Value> {
        self.processor.read().await.record(table, id)
    }

    async fn deliver(&self, record: ChangeRecord) -> Result<(), SinkError> {
        if record.table == "_00_query" {
            if record.op == ChangeOp::Delete {
                // Standalone mode has no scheduler tracker; the node's own
                // unregister route drops the view and its edges.
                match self
                    .post("/view/unregister", serde_json::json!({ "id": record.id }))
                    .await
                {
                    Some(resp) if resp.status < 400 => {}
                    Some(resp) => warn!(
                        status = resp.status,
                        "Changefeed view teardown refused by the node"
                    ),
                    None => warn!("Changefeed view teardown: route not served"),
                }
            }
            return Ok(());
        }
        // The feed cannot tell a CREATE from an UPDATE the circuit already
        // holds after a replay; the circuit can.
        let op = match record.op {
            ChangeOp::Create
                if self
                    .processor
                    .read()
                    .await
                    .contains(&record.table, &record.id) =>
            {
                ChangeOp::Update
            }
            other => other,
        };
        let body = serde_json::json!({
            "table": record.table,
            "op": op.as_str(),
            "id": record.id,
            "record": record.record.unwrap_or_else(|| Value::Object(Default::default())),
        });
        match self.post("/ingest", body).await {
            Some(resp) if resp.status < 300 => Ok(()),
            Some(resp) if resp.status == 400 || resp.status == 422 => {
                error!(
                    status = resp.status,
                    "Changefeed record rejected by /ingest; skipped"
                );
                Ok(())
            }
            Some(resp) => Err(SinkError::Retry(format!(
                "/ingest answered {}",
                resp.status
            ))),
            None => Err(SinkError::Retry("/ingest route not served".into())),
        }
    }

    async fn on_gap(&self) -> anyhow::Result<()> {
        // Rebuild the circuit from the database; the loop already reset the
        // cursor to just before now, so the rebuild and the tail overlap.
        self.node.reload().await
    }
}

/// Start the tail when `SPKY_INGEST_TRANSPORT=changefeed`. Returns the stats
/// handle (for `/health`) whether or not a tail runs.
pub fn spawn_if_configured(
    config: &ssp_node::NodeConfig,
    db: Arc<maintenance::db::ReconnectingDb>,
    node: Arc<ssp_node::SspNode>,
    processor: Arc<RwLock<Circuit>>,
    status: Arc<RwLock<SspStatus>>,
) -> Arc<TailerStats> {
    let stats = TailerStats::new();
    if IngestTransport::from_env() != IngestTransport::Changefeed {
        return stats;
    }
    let settings = ChangefeedSettings::from_env();
    let notify = Arc::new(tokio::sync::Notify::new());
    let db_config = maintenance::db::DbConfig {
        url: config.db_addr.clone(),
        namespace: config.db_ns.clone(),
        database: config.db_db.clone(),
        username: config.db_user.clone(),
        password: config.db_pass.clone(),
    };
    if settings.doorbell {
        maintenance::doorbell::spawn(
            db_config,
            maintenance::changefeed::DOORBELL_TABLE.to_string(),
            Arc::clone(&notify),
            Arc::clone(&stats),
        );
    }
    info!(
        retention = %settings.retention,
        doorbell = settings.doorbell,
        "Ingest transport: changefeed (standalone tail)"
    );
    let source: Arc<dyn ChangeSource> = Arc::new(ReconnectingSource { db });
    let sink: Arc<dyn ChangeSink> = Arc::new(NodeSink {
        node,
        processor,
        status,
        auth_secret: config.auth_secret.clone(),
    });
    tokio::spawn(maintenance::changefeed::run_tailer(
        source,
        sink,
        settings.tailer_config(),
        Arc::clone(&stats),
        notify,
    ));
    stats
}
