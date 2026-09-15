//! Shared maintenance plane for the scheduler and standalone SSP.
//!
//! Backup/restore of the main SurrealDB (export → gzip → S3, and the inverse)
//! and backend health monitoring are host-independent: the scheduler and a
//! standalone SSP expose the exact same HTTP surface. Host-specific concerns
//! (the scheduler's replica/WAL drain, the SSP's circuit re-bootstrap) are
//! injected through the [`host::MaintenanceHost`] trait.

pub mod alert;
pub mod backend_health;
pub mod backup;
pub mod changefeed;
pub mod db;
pub mod doorbell;
pub mod host;
pub mod log_ring;
pub mod restore;
pub mod routes;
pub mod s3;

pub use backend_health::{
    create_health_cache, create_shared_configs, start_backend_health_monitor, update_backends,
    BackendHealthCache, BackendHealthConfig, BackendHealthEntry, BackendStatus,
    SharedBackendConfigs,
};
pub use backup::{
    create_backup_channel, run_backup_worker, BackupJob, BackupJobState, BackupRegistry,
    BackupStatus,
};
pub use db::{connect_http, connect_http_raw, DbConfig};
pub use host::MaintenanceHost;
pub use restore::{
    create_restore_channel, run_restore_worker, RestoreJob, RestoreJobState, RestoreOutcome,
    RestoreProgress, RestoreRegistry, RestoreStatus,
};
pub use routes::{create_backup_router, BackupState};
pub use s3::{ensure_bucket, BackupConfig};
