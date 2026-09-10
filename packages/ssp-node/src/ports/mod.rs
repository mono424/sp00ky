//! Platform ports: the ONLY seams between the portable core and a runtime.
//!
//! A shell (VM: tokio/axum in `apps/ssp`; Cloudflare: workers-rs Durable
//! Object in the future `apps/ssp-cf`) implements these traits and hands the
//! core a [`crate::Platform`]. Swapping platforms = swapping adapters —
//! nothing in the core changes.
//!
//! wasm wrinkle: workers-rs futures are `!Send` (they wrap `JsFuture`), so on
//! `wasm32` every trait drops its `Send` bounds via the cfg-gated
//! `async_trait(?Send)` + [`MaybeSendSync`] pattern. Native keeps full
//! `Send + Sync` so tokio can move futures across threads.

mod artifacts;
mod backend_health;
mod circuit_store;
mod db;
mod http;
mod scheduler;
mod spawn;
mod telemetry;

pub use artifacts::{ArtifactError, ArtifactMeta, ArtifactStore};
pub use backend_health::{BackendCounts, BackendHealth, BackendSpec};
pub use circuit_store::{CircuitStore, CircuitStoreError, NoopCircuitStore, ResumePoint};
#[cfg(not(target_arch = "wasm32"))]
pub use circuit_store::DiskCircuitStore;
pub use db::{Db, DbConnection, DbError};
pub use http::{CancelHandle, CancelWatch, HttpClient, HttpError, OutboundRequest, OutboundResponse};
pub use scheduler::{Scheduler, TimerKind};
pub use spawn::{LocalBoxFuture, Spawner};
pub use telemetry::{NoopTelemetry, Telemetry};

/// `Send + Sync` on native targets, no bounds on wasm32 (single-threaded,
/// `!Send` futures). Blanket-implemented — never implement manually.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> MaybeSendSync for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSendSync {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSendSync for T {}
