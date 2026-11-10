//! Inbound runtime events (what starts a saga) and outbound events (what the
//! runtime hands to subscribers, the logger and the host app).

use std::collections::BTreeSet;

use crate::kernel::ports::StreamUpdate;
use crate::types::{
    ConnectionState, InlineRow, MutationEventType, QueryHash, QueryStatus, SyncHealth,
};
use crate::value::{Object, Value};

/// Everything that can start a saga: public API calls, adapter callbacks,
/// timers, and `dispatch` from other sagas. The router maps each to its saga
/// and lane.
#[derive(Clone, Debug)]
pub enum RuntimeEvent {
    EnsureRegistered { require_auth: bool, attempt: u32 },
    RegisterRemote { hash: QueryHash, retry: bool },
    SyncOutcome { ok: bool, error: Option<String> },
    AckPrune,
    RecoverLostView { hash: QueryHash },
    ReadDirtyMembership,
    ReadMembership { hashes: Vec<QueryHash> },
    FetchRows,
    Materialize { hash: QueryHash },
    MaterializeDirty,
    LifecycleTick,
    GcTick,
    Drain,
    FlushWrite { key: String },
    /// A channel peer's create or delete stops showing as unconfirmed.
    PeerWriteExpired { id: String },
    PollTick,
    SelfHealTick,
    HeartbeatNow,
    StartRemote,
    PrimeCircuit,
    VersionsPrimed { entries: Vec<(String, i64)> },
    AuthFlip { user_id: Option<String> },
    BucketSwitch { target: String },
    AppDetached,
    ConnectionChanged { state: ConnectionState },
    StreamUpdate { update: Box<StreamUpdate> },
    LiveStart,
    LiveChange { hashes: Vec<QueryHash>, rows: Option<Vec<InlineRow>> },
}

impl RuntimeEvent {
    pub fn ensure_registered() -> Self {
        RuntimeEvent::EnsureRegistered {
            require_auth: false,
            attempt: 0,
        }
    }

    /// Stable discriminator, used by the router's logs and test assertions.
    pub fn kind(&self) -> &'static str {
        use RuntimeEvent::*;
        match self {
            EnsureRegistered { .. } => "EnsureRegistered",
            RegisterRemote { .. } => "RegisterRemote",
            SyncOutcome { .. } => "SyncOutcome",
            AckPrune => "AckPrune",
            RecoverLostView { .. } => "RecoverLostView",
            ReadDirtyMembership => "ReadDirtyMembership",
            ReadMembership { .. } => "ReadMembership",
            FetchRows => "FetchRows",
            Materialize { .. } => "Materialize",
            MaterializeDirty => "MaterializeDirty",
            LifecycleTick => "LifecycleTick",
            GcTick => "GcTick",
            Drain => "Drain",
            FlushWrite { .. } => "FlushWrite",
            PeerWriteExpired { .. } => "PeerWriteExpired",
            PollTick => "PollTick",
            SelfHealTick => "SelfHealTick",
            HeartbeatNow => "HeartbeatNow",
            StartRemote => "StartRemote",
            PrimeCircuit => "PrimeCircuit",
            VersionsPrimed { .. } => "VersionsPrimed",
            AuthFlip { .. } => "AuthFlip",
            BucketSwitch { .. } => "BucketSwitch",
            AppDetached => "AppDetached",
            ConnectionChanged { .. } => "ConnectionChanged",
            StreamUpdate { .. } => "StreamUpdate",
            LiveStart => "LiveStart",
            LiveChange { .. } => "LiveChange",
        }
    }

    /// Events whose saga may run across an account change (they cause it).
    pub fn allows_account_change(&self) -> bool {
        matches!(
            self,
            RuntimeEvent::AuthFlip { .. } | RuntimeEvent::BucketSwitch { .. } | RuntimeEvent::StartRemote
        )
    }
}

/// Log severity of a [`OutEvent::Log`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// One optimistic write, as the host app observes it.
#[derive(Clone, Debug, PartialEq)]
pub struct MutationEvent {
    pub kind: MutationEventType,
    pub mutation_id: String,
    pub record_id: String,
    pub data: Option<Object>,
    pub record: Option<Object>,
    pub created_at: i64,
}

/// What the runtime hands out.
#[derive(Clone, Debug)]
pub enum OutEvent {
    QueryRecords { hash: QueryHash, records: std::sync::Arc<Vec<Object>> },
    QueryStatus { hash: QueryHash, status: QueryStatus },
    QueryAuthority { hash: QueryHash, known: bool },
    QueryViewLost { hash: QueryHash },
    QueryEvicted { hash: QueryHash },
    MutationEmitted(MutationEvent),
    MutationSettled { mutation_id: String, record_id: String, kind: MutationEventType },
    MutationRolledBack { mutation_id: String, record_id: String, kind: MutationEventType, error: String },
    TrayChanged { count: usize },
    HealthChanged { health: SyncHealth },
    ActivityChanged { fetching: usize, pending: usize },
    /// The set of record ids with unacknowledged local writes changed.
    UnsyncedChanged { record_ids: BTreeSet<String> },
    Log { level: LogLevel, message: String, data: Option<Value> },
}

impl OutEvent {
    pub fn kind(&self) -> &'static str {
        use OutEvent::*;
        match self {
            QueryRecords { .. } => "query:records",
            QueryStatus { .. } => "query:status",
            QueryAuthority { .. } => "query:authority",
            QueryViewLost { .. } => "query:view-lost",
            QueryEvicted { .. } => "query:evicted",
            MutationEmitted(_) => "mutation:event",
            MutationSettled { .. } => "mutation:settled",
            MutationRolledBack { .. } => "mutation:rolled-back",
            TrayChanged { .. } => "tray:changed",
            HealthChanged { .. } => "health:changed",
            ActivityChanged { .. } => "activity:changed",
            UnsyncedChanged { .. } => "unsynced:changed",
            Log { .. } => "log",
        }
    }
}
