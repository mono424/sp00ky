//! The engine's single state value, and the pure functions over it.
//!
//! [`ClientState`] is owned by the runtime and changed only through reducers
//! (`reducers.rs`), plain functions of `&mut ClientState` that a saga applies
//! with `ctx.update`. Reducers that change a query's render inputs add it to
//! `dirty` themselves, which is what drives materialization. Reads go through
//! selectors (`selectors.rs`). Nothing here performs I/O.

pub mod lifecycle;
pub mod reducers;
pub mod selectors;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::modules::query_builder::RelationPlan;
use crate::types::{
    ConnectionState, MutationEventType, QueryHash, QueryTimeToLive, RecordVersionArray,
    RegistrationTimings, SyncHealth, SyncHealthStatus, TimingPhase,
};
use crate::value::{Object, RecordId, Row};

pub use lifecycle::{QueryLifecycle, QueryPhase, RemotePhase};

/// Everything a query is, minus anything that changes after registration.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryDefinition {
    /// Session-salted `_00_query:<hash>` record id: the remote view id.
    pub id: RecordId,
    pub hash: QueryHash,
    /// Unsalted `sha256({surql, params})`: key of the durable `_00_view` row.
    pub view_key: String,
    pub surql: String,
    pub params: Object,
    pub ttl: QueryTimeToLive,
    pub ttl_ms: u64,
    pub table_name: String,
    pub created_at: i64,
    /// The `.related()` plan, resolved from the local cache on every
    /// materialization. Not persisted: the surql already encodes the shape.
    pub relations: Vec<RelationPlan>,
    /// The query orders itself, so the render set must not be re-sorted by id.
    pub has_explicit_order: bool,
}

/// What the server's `_00_query` row last said about this view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerViewState {
    Materializing,
    Ready,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryTelemetry {
    pub update_count: u64,
    pub error_count: u64,
    pub last_updated_at: Option<i64>,
    pub materialization_samples: Vec<f64>,
    pub last_ingest_latency_ms: Option<f64>,
    pub phase_samples: BTreeMap<TimingPhase, Vec<f64>>,
    pub phase_last: BTreeMap<TimingPhase, f64>,
    pub registration_timings: RegistrationTimings,
}

/// One registered query.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryEntry {
    pub def: QueryDefinition,
    pub lifecycle: QueryLifecycle,
    /// Server membership (ids + versions) or the durable seed.
    pub remote_array: RecordVersionArray,
    /// The in-process view's id-set: local truth for "does this row match".
    pub local_array: RecordVersionArray,
    pub subquery_remote_array: RecordVersionArray,
    /// What subscribers last received.
    pub records: Arc<Vec<Row>>,
    pub server_state: Option<ServerViewState>,
    pub subscribers: u32,
    pub last_subscriber_left_at: Option<i64>,
    pub last_heartbeat_at: Option<i64>,
    pub last_polled_at: Option<i64>,
    pub register_attempts: u32,
    /// Re-registrations issued because the `_00_query` row read as gone, since
    /// the row was last seen. Paces the next one.
    pub view_lost_count: u32,
    /// When the paced re-registration is due.
    pub view_lost_retry_at: Option<i64>,
    pub telemetry: QueryTelemetry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxStatus {
    Pending,
    Acked,
}

/// In-memory mirror of one `_00_pending_mutations` row plus its push progress.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxItem {
    pub id: String,
    pub kind: MutationEventType,
    pub record_id: String,
    pub table: String,
    pub status: OutboxStatus,
    pub acked_at: Option<i64>,
    pub attempts: u32,
    /// A request carrying this row may have committed without the answer
    /// reaching this client (a transport error, or loaded from the store after
    /// a run that died mid-push). Its CREATE then accepts the row already on
    /// the server when that row holds what it would write.
    pub in_doubt: bool,
}

/// Kept for shape parity with the TypeScript core. A native app is one
/// process, so the runtime never leaves [`TabRole::Solo`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TabRole {
    #[default]
    Solo,
    Leader,
    Follower,
}

/// A debounced update accumulating locally until its flush writes the outbox
/// row.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingWrite {
    pub key: String,
    pub table: String,
    pub record_id: String,
    pub data: Object,
    pub before: Option<Object>,
    pub first_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerWriteKind {
    Create,
    Delete,
}

/// A create or delete a channel peer sent, applied here before the server has
/// it. Shown like an own optimistic write through the overlay; when
/// `expires_at` passes it is confirmed (the server's body landed) or undone.
#[derive(Clone, Debug, PartialEq)]
pub struct PeerWrite {
    pub kind: PeerWriteKind,
    pub table: String,
    pub expires_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncSlice {
    pub health: SyncHealth,
    pub consecutive_failures: u32,
    pub has_synced_once: bool,
    pub self_heal_attempts: u32,
    pub poll_idle_streak: u32,
    pub last_reconnect_refetch_at: Option<i64>,
    pub needs_resubscribe: bool,
    pub fetch_attempts: u32,
    pub live_uuid: Option<String>,
    pub live_table: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClientState {
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    /// The principal the current session salt was minted for.
    pub salt_user_id: Option<String>,
    /// This client's secret, sent with every registration (`$config.owner`).
    pub owner_secret: Option<String>,
    /// Latest bucket-switch target; an older switch that wakes to a newer
    /// target skips.
    pub pending_bucket: Option<String>,
    pub tab_id: String,
    pub tab_role: TabRole,
    pub bucket_id: Option<String>,
    pub local_ready: bool,
    pub primed: bool,
    pub queries: BTreeMap<QueryHash, QueryEntry>,
    /// Hashes whose local registration is in flight.
    pub registering: BTreeSet<QueryHash>,
    /// Re-read attempts per hash while the server reports `materializing`.
    pub membership_reread: BTreeMap<QueryHash, u32>,
    /// Local body versions (`_00_rv`) by record id.
    pub versions: BTreeMap<String, i64>,
    pub outbox: Vec<OutboxItem>,
    pub pending_writes: BTreeMap<String, PendingWrite>,
    /// Channel peers' unconfirmed creates and deletes, by record id.
    pub peer_writes: BTreeMap<String, PeerWrite>,
    pub failed_count: usize,
    /// Queries whose render inputs changed since their last materialization.
    pub dirty: BTreeSet<QueryHash>,
    /// Queries whose server membership must be re-read.
    pub membership_dirty: BTreeSet<QueryHash>,
    pub sync: SyncSlice,
}

pub fn initial_health(connection: ConnectionState) -> SyncHealth {
    SyncHealth {
        status: SyncHealthStatus::Healthy,
        consecutive_failures: 0,
        kind: None,
        error: None,
        ever_connected: false,
        connection,
    }
}

impl ClientState {
    pub fn empty(tab_id: impl Into<String>) -> Self {
        ClientState {
            session_id: None,
            user_id: None,
            salt_user_id: None,
            owner_secret: None,
            pending_bucket: None,
            tab_id: tab_id.into(),
            tab_role: TabRole::Solo,
            bucket_id: None,
            local_ready: false,
            primed: false,
            queries: BTreeMap::new(),
            registering: BTreeSet::new(),
            membership_reread: BTreeMap::new(),
            versions: BTreeMap::new(),
            outbox: Vec::new(),
            pending_writes: BTreeMap::new(),
            peer_writes: BTreeMap::new(),
            failed_count: 0,
            dirty: BTreeSet::new(),
            membership_dirty: BTreeSet::new(),
            sync: SyncSlice {
                health: initial_health(ConnectionState::Disconnected),
                consecutive_failures: 0,
                has_synced_once: false,
                self_heal_attempts: 0,
                poll_idle_streak: 0,
                last_reconnect_refetch_at: None,
                needs_resubscribe: false,
                fetch_attempts: 0,
                live_uuid: None,
                live_table: None,
            },
        }
    }
}
