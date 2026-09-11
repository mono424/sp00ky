//! Job execution engine (absorbed from the former `packages/job-runner`).
//!
//! Fully portable: dispatch goes through the [`crate::ports::HttpClient`]
//! port, status writes through [`crate::ports::Db`], retry backoff through
//! [`crate::ports::Scheduler::sleep`] on a [`crate::ports::Spawner`] task.
//! The queue itself is `tokio::sync::mpsc` (wasm-safe); cancellation uses
//! the portable [`crate::ports::CancelHandle`]/[`CancelWatch`] pair instead
//! of `tokio_util::CancellationToken`.

mod dispatcher;
mod runner;
mod types;

#[cfg(test)]
mod db_tests;

pub use dispatcher::{table_of, JobDispatcher, Permit, DEFAULT_CONCURRENCY};
pub use runner::{
    append_error_helper, claim_processing, claim_processing_when_visible,
    complete_success_helper, enqueue_recovered,
    fail_if_pending_helper, lease_secs, load_job_record, reclaim_expired_lease,
    reset_for_retry_helper, set_assignee_helper, update_status_fenced, update_status_helper,
    JobRunner, JOB_RESULT_MAX_BYTES, PENDING_DUE_CLAUSE,
};
pub use types::{BackendInfo, JobConfig, JobControl, JobEntry};
