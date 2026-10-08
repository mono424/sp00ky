use serde::Serialize;

/// SSP lifecycle status. Lives in the core so both shells and the core's own
/// handlers gate on the same state machine.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SspStatus {
    Bootstrapping,
    Ready,
    Failed,
    /// Shutting down: no more ingest, the rows are being checkpointed.
    Stopping,
}

#[derive(Serialize)]
pub struct SspError {
    pub code: &'static str,
    pub message: String,
}

pub mod error_codes {
    pub const NOT_READY: &str = "SSP_NOT_READY";
}
