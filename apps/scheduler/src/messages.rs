use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use ssp_protocol::SspHeartbeat;

/// Record operation type
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOp {
    Create,
    Update,
    Delete,
}

impl std::fmt::Display for RecordOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordOp::Create => write!(f, "CREATE"),
            RecordOp::Update => write!(f, "UPDATE"),
            RecordOp::Delete => write!(f, "DELETE"),
        }
    }
}

/// Record update message broadcast to SSPs
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RecordUpdate {
    pub table: String,
    pub operation: RecordOp,
    pub record_id: String,
    pub data: Option<Value>,
    pub version: u64,
}

/// Bootstrap request from SSP
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapRequest {
    pub ssp_id: String,
    pub tables: Vec<String>, // Which tables to bootstrap
}

/// Bootstrap chunk response
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapChunk {
    pub chunk_index: usize,
    pub total_chunks: usize,
    pub table: String,
    pub records: Vec<(String, Value)>,
}

/// Complete bootstrap response
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapResponse {
    pub ssp_id: String,
    pub chunks: Vec<BootstrapChunk>,
}

/// A buffered ingest event with ordering metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferedEvent {
    /// Monotonically increasing sequence number assigned by the scheduler
    pub seq: u64,
    /// The original record update
    pub update: RecordUpdate,
    /// Unix timestamp when this event was received
    pub received_at: u64,
    /// SurrealDB changefeed versionstamp the event was read at, `0` for an
    /// event that arrived over HTTP `/ingest`. The tail resumes from the
    /// highest one in the WAL after a restart (`serde(default)` keeps WALs
    /// written before this field readable).
    #[serde(default)]
    pub versionstamp: u64,
}
