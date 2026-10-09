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
    /// Blue/green: handed over to a successor (`POST /handover/retire`). No
    /// ingest, no registrations, no database writers of its own; alive and
    /// heartbeating until the process is stopped, or back to `Ready` on
    /// `POST /handover/resume`.
    Retired,
}

#[derive(Serialize)]
pub struct SspError {
    pub code: &'static str,
    pub message: String,
}

pub mod error_codes {
    pub const NOT_READY: &str = "SSP_NOT_READY";
    /// Refused because this SSP has been retired in favour of a successor.
    pub const RETIRED: &str = "retired";
    /// Refused because this SSP is a standby that must not write yet.
    pub const STANDBY: &str = "standby";
}
