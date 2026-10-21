//! Portable scheduling/workflow engine core.
//!
//! Two hosts embed this crate and share every line of engine logic:
//! - the singlenode SSP (`packages/ssp-node`, driven by `TimerKind::ScheduleSweep`)
//! - the cluster scheduler (`apps/scheduler`, driven by a tokio interval loop)
//!
//! The host provides two capabilities ([`ScheduleDb`], [`JobKill`]); everything
//! else — next-fire computation, tick claiming, fan-out, DAG advancement,
//! healing — lives here. The engine is the sole writer of the `_00_schedule*`
//! tables' engine-owned fields and NEVER writes outbox job `status` (the
//! `JobRunner` stays the single status writer).
//!
//! wasm wrinkle: same `MaybeSendSync` / `async_trait(?Send)` split as
//! `ssp-node::ports` so a future wasm shell can link the crate.

pub mod admin;
pub mod cron;
pub mod dag;
pub mod db;
pub mod engine;
pub mod ids;
pub mod kill;
pub mod spec;
pub mod sql;
mod workflow;

#[cfg(test)]
mod db_tests;

pub use cron::{FireSpec, FireSpecError};
pub use dag::{DagError, StepStatus, WorkflowDag};
pub use db::{ScheduleDb, ScheduleDbError};
pub use engine::{EngineConfig, ScheduleEngine, TickReport, RUN_POLL_SECS};
pub use kill::{JobKill, NoopJobKill};
pub use spec::{Concurrency, OnFailure, ScheduleKind, ScheduleSpec, StepDef, WorkflowDef};
pub use workflow::{CancelOutcome, RerunOutcome, RetryOutcome, WorkflowOpError};

/// `Send + Sync` on native targets, no bounds on wasm32. Blanket-implemented.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> MaybeSendSync for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSendSync {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSendSync for T {}
