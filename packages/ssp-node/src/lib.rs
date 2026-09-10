//! Portable SSP node core.
//!
//! Everything platform-specific in the SSP data plane crosses one of the port
//! traits in [`ports`]; the core itself is event-driven with exactly three
//! entrypoints (implemented incrementally as handler logic migrates here from
//! `apps/ssp`):
//!
//! - `route(ApiRequest) -> ApiResponse` — every HTTP entry
//! - `on_timer(TimerKind)` — every scheduled wakeup (re-arms itself)
//! - `bootstrap()` — init / re-init after restart or DO eviction
//!
//! The crate must stay buildable for `wasm32-unknown-unknown`:
//! `cargo check -p ssp-node --target wasm32-unknown-unknown` is the
//! portability gate (see `scripts/check-portability.sh`). Allowed runtime
//! deps: `tokio::sync` (wasm-safe), `web_time`. Forbidden: `tokio::{spawn,
//! time,net}`, reqwest, surrealdb, axum, opentelemetry, `std::env`,
//! `std::fs`, `std::process`.
//!
//! See `docs/platform-architecture.md` for the full design.

pub mod api;
pub mod bootstrap;
pub mod config;
pub mod db_retry;
pub mod crdt;
pub mod edges;
pub mod http_sql_db;
pub mod jobs;
pub mod node;
pub mod platform;
pub mod ports;
pub mod status;
pub mod runtime;
pub mod schedules;
pub mod tables;
pub mod view_metrics;
pub mod timers;

pub use api::{ApiBody, ApiRequest, ApiResponse, Method, RouteId};
pub use config::NodeConfig;
pub use http_sql_db::HttpSqlDb;
pub use node::{ttl_cleanup_sweep, wipe_circuit_and_edges, SspNode};
pub use platform::Platform;
pub use runtime::Runtime;
pub use status::{error_codes, SspError, SspStatus};
pub use ports::{
    ArtifactError, ArtifactMeta, ArtifactStore, CancelHandle, CancelWatch, CircuitStore,
    CircuitStoreError, Db, DbConnection, DbError, HttpClient, HttpError, MaybeSendSync, NoopCircuitStore,
    NoopTelemetry, OutboundRequest, OutboundResponse, ResumePoint, Scheduler, Spawner, Telemetry,
    TimerKind,
};
#[cfg(not(target_arch = "wasm32"))]
pub use ports::DiskCircuitStore;
pub use timers::TimerMux;

/// Milliseconds since the Unix epoch, portable across native and wasm
/// (`web_time::SystemTime` falls back to `std` off-wasm).
pub fn now_epoch_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
