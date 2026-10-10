//! VM (tokio/axum) implementations of the `ssp-node` platform ports.
//!
//! This is the entire tokio/reqwest/OTel-facing adapter layer — the future
//! Cloudflare shell (`apps/ssp-cf`) provides its own equivalents
//! (AlarmScheduler / FetchHttp / spawn_local / …) against the same traits.
//! See `docs/platform-architecture.md`.

mod backend_health;
mod db;
mod http;
mod scheduler;
mod spawn;
mod telemetry;

pub use backend_health::MaintenanceBackendHealth;
pub use db::SurrealSdkDb;
pub use http::ReqwestHttp;
pub use scheduler::TokioScheduler;
pub use spawn::TokioSpawner;
pub use telemetry::OtelTelemetry;

use std::sync::Arc;

/// Assemble the VM platform bundle. `timer_rx` is the delivery side of the
/// scheduler: the shell drains it and dispatches each fired `TimerKind` to
/// the node (today: a placeholder drain loop — wired to `on_timer` as core
/// migration proceeds).
pub fn vm_platform(
    db: crate::SharedDb,
    metrics: Arc<crate::metrics::Metrics>,
) -> (ssp_node::Platform, tokio::sync::mpsc::UnboundedReceiver<ssp_node::TimerKind>) {
    let (scheduler, timer_rx) = TokioScheduler::new();
    let platform = ssp_node::Platform {
        db: Arc::new(SurrealSdkDb::new(db)),
        http: Arc::new(ReqwestHttp::new()),
        scheduler: Arc::new(scheduler),
        spawner: Arc::new(TokioSpawner),
        telemetry: Arc::new(OtelTelemetry::new(metrics)),
        // The VM never restores a circuit snapshot: a cluster SSP keeps row
        // checkpoints (`crate::warm`) and a standalone one rebuilds from its
        // own database, which holds no content hash to verify a snapshot
        // against. `DiskCircuitStore` stays in ssp-node for the portable
        // host, which does restore one.
        circuit_store: Arc::new(ssp_node::NoopCircuitStore),
    };
    (platform, timer_rx)
}
