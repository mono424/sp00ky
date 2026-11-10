//! The adapters the runtime drives: the one seam between a saga and the world.
//!
//! Sagas never hold an adapter. They call [`crate::kernel::Ctx`], which fences
//! every call against an account change and forwards it here. Tests swap any of
//! these for a fake (`crate::testing`).

use std::rc::Rc;

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{InlineRow, QueryHash, QueryTimeToLive, RecordVersionArray, RegistrationTimings};
use crate::value::{Object, Row, Value};

// ---- local store ---------------------------------------------------------------

/// How a document write lands on an existing row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WriteMode {
    /// Replace the stored document wholesale.
    #[default]
    Replace,
    /// Deep-merge into the stored document, preserving keys the patch omits
    /// (`_00_crdt`, `_00_cursor`).
    Merge,
}

/// One step of a local transaction. A transaction is data, so a saga can
/// describe a multi-write commit without reaching for the store.
#[derive(Clone, Debug, PartialEq)]
pub enum LocalOp {
    Put {
        table: String,
        id: String,
        data: Row,
        mode: WriteMode,
        /// Keys a merge removes after merging `data`: fields the server
        /// cleared, which a merge alone would keep. Ignored by `Replace`.
        clear: Vec<String>,
    },
    Delete {
        table: String,
        id: String,
    },
    /// `_00_rv += 1` on a stored row.
    BumpRv {
        table: String,
        id: String,
    },
}

impl LocalOp {
    pub fn put(table: impl Into<String>, id: impl Into<String>, data: Row) -> Self {
        LocalOp::Put {
            table: table.into(),
            id: id.into(),
            data,
            mode: WriteMode::Replace,
            clear: Vec::new(),
        }
    }

    pub fn merge(table: impl Into<String>, id: impl Into<String>, data: Row, clear: Vec<String>) -> Self {
        LocalOp::Put {
            table: table.into(),
            id: id.into(),
            data,
            mode: WriteMode::Merge,
            clear,
        }
    }

    pub fn delete(table: impl Into<String>, id: impl Into<String>) -> Self {
        LocalOp::Delete {
            table: table.into(),
            id: id.into(),
        }
    }
}

/// The local document store, addressed by `(table, id)` (sqlite cannot run
/// SurrealQL). Ids are full `table:key` strings.
pub trait LocalPort {
    fn get(&self, table: &str, id: &str) -> Result<Option<Row>>;
    /// Many rows by id, in the order asked; ids the store lacks are dropped.
    fn get_many(&self, table: &str, ids: &[String]) -> Result<Vec<Row>>;
    fn get_all(&self, table: &str) -> Result<Vec<Row>>;
    fn put(&self, table: &str, id: &str, data: Row, mode: WriteMode) -> Result<()>;
    fn delete(&self, table: &str, id: &str) -> Result<()>;
    /// Apply `ops` atomically.
    fn tx(&self, ops: Vec<LocalOp>) -> Result<()>;
    /// Bumped whenever the open store is replaced (bucket switch). A write
    /// fenced with a stale epoch is dropped rather than landing in the wrong
    /// bucket.
    fn epoch(&self) -> u64;
}

// ---- remote --------------------------------------------------------------------

/// One statement's outcome of a multi-statement request.
#[derive(Clone, Debug, PartialEq)]
pub struct StatementResult(pub std::result::Result<Value, String>);

impl StatementResult {
    pub fn ok(value: impl Into<Value>) -> Self {
        StatementResult(Ok(value.into()))
    }

    pub fn err(message: impl Into<String>) -> Self {
        StatementResult(Err(message.into()))
    }

    pub fn is_ok(&self) -> bool {
        self.0.is_ok()
    }

    /// The result value of an `OK` statement.
    pub fn value(&self) -> Option<&Value> {
        self.0.as_ref().ok()
    }

    pub fn into_value(self) -> Option<Value> {
        self.0.ok()
    }

    /// The error message of an `ERR` statement.
    pub fn error(&self) -> Option<&str> {
        self.0.as_ref().err().map(String::as_str)
    }
}

/// Called with the changed query hashes of one LIVE notification, plus the
/// joined rows when the subscription carries bodies (`FETCH out`).
pub type LiveCallback = Rc<dyn Fn(Vec<QueryHash>, Option<Vec<InlineRow>>)>;

/// The remote socket.
#[async_trait(?Send)]
pub trait RemotePort {
    /// Per-statement outcomes, in order. Fails only when the request as a
    /// whole failed (transport, RPC error), never for one statement. Reads
    /// share a pool and overlap.
    async fn query(&self, sql: &str, vars: Option<Object>) -> Result<Vec<StatementResult>>;
    /// [`RemotePort::query`] for a row body fetch: it does not wait behind
    /// other reads (a registration can spend seconds on the server).
    async fn bodies(&self, sql: &str, vars: Option<Object>) -> Result<Vec<StatementResult>> {
        self.query(sql, vars).await
    }
    /// [`RemotePort::query`] for a write: starts once every earlier write has
    /// settled, so writes reach the server one at a time, in issue order.
    async fn write(&self, sql: &str, vars: Option<Object>) -> Result<Vec<StatementResult>>;
    /// Subscribe to the session's `_00_list_ref` table; resolves to the live
    /// query uuid.
    async fn live(&self, table: &str, on_change: LiveCallback) -> Result<String>;
    async fn kill(&self, uuid: &str) -> Result<()>;
}

// ---- stream processor ------------------------------------------------------------

/// What the in-process circuit needs to build a local view.
#[derive(Clone, Debug, PartialEq)]
pub struct RegisterPlan {
    pub query_hash: QueryHash,
    pub surql: String,
    pub params: Object,
    pub ttl: QueryTimeToLive,
    pub table_name: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RegisterResult {
    pub local_array: RecordVersionArray,
    pub timings: RegistrationTimings,
}

/// Circuit ingest operation. `Merge` overlays the given fields on the stored
/// row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestOp {
    Create,
    Update,
    Delete,
    Merge,
}

impl IngestOp {
    /// The spelling the circuit expects.
    pub fn as_str(self) -> &'static str {
        match self {
            IngestOp::Create => "CREATE",
            IngestOp::Update => "UPDATE",
            IngestOp::Delete => "DELETE",
            IngestOp::Merge => "MERGE",
        }
    }
}

/// One record handed to the circuit.
#[derive(Clone, Debug, PartialEq)]
pub struct IngestRecord {
    pub table: String,
    pub op: IngestOp,
    pub id: String,
    pub record: Row,
}

impl IngestRecord {
    pub fn new(table: impl Into<String>, op: IngestOp, id: impl Into<String>, record: Row) -> Self {
        IngestRecord {
            table: table.into(),
            op,
            id: id.into(),
            record,
        }
    }

    pub fn delete(id: &str) -> Self {
        IngestRecord::new(crate::value::table_of(id), IngestOp::Delete, id, Row::new())
    }
}

/// What changed in a view (`WasmDelta`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ViewDelta {
    pub additions: RecordVersionArray,
    pub removals: Vec<String>,
    pub updates: RecordVersionArray,
}

/// A materialized-view update emitted by the circuit.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StreamUpdate {
    /// The query this update is for.
    pub query_hash: QueryHash,
    /// The full result set as id/version pairs.
    pub local_array: RecordVersionArray,
    pub result_hash: String,
    pub delta: ViewDelta,
    /// Operation that produced it; `None` for a registration snapshot.
    pub op: Option<String>,
    /// End-to-end ingest wall time of the call that produced it.
    pub materialization_time_ms: Option<f64>,
    pub store_apply_ms: Option<f64>,
    pub circuit_step_ms: Option<f64>,
    pub transform_ms: Option<f64>,
    pub parse_ms: Option<f64>,
    pub plan_ms: Option<f64>,
    pub snapshot_ms: Option<f64>,
}

/// The in-process circuit.
pub trait SspPort {
    fn register(&self, plan: RegisterPlan) -> Result<RegisterResult>;
    fn unregister(&self, hash: &str) -> Result<()>;
    fn ingest_many(&self, records: Vec<IngestRecord>) -> Result<()>;
}

// ---- timers ----------------------------------------------------------------------

/// Keyed one-shot timers. Setting a key replaces its pending timer.
pub trait TimerPort {
    fn set(&self, key: &str, ms: u64, fire: Box<dyn FnOnce()>);
    fn clear(&self, key: &str);
    fn clear_all(&self);
}

// ---- services ----------------------------------------------------------------------

/// Calls into the services that are adapters, not state: auth, the migrator,
/// persistence, the circuit lifecycle, the socket, the supervisor. A saga names
/// the call; the runtime binds it to the real service.
#[async_trait(?Send)]
pub trait Services {
    /// The bucket this process last used, so boot can open the right store
    /// before the network answers.
    async fn hint_read(&self) -> Result<Option<String>>;
    async fn hint_write(&self, bucket_id: &str) -> Result<()>;

    async fn local_connect(&self, bucket_id: &str) -> Result<()>;
    /// Snapshot the circuit into the outgoing store, then open `bucket_id`.
    async fn local_switch_store(&self, bucket_id: &str) -> Result<()>;
    fn local_current_bucket_id(&self) -> String;

    async fn migrator_provision(&self) -> Result<()>;

    async fn ssp_init(&self) -> Result<()>;
    fn ssp_set_permissions(&self) -> Result<()>;
    fn ssp_set_session_auth(&self, auth_id: Option<&str>, access: Option<&str>);
    /// Prime the circuit from the local rows (and its snapshot); `pending_ids`
    /// have unsettled local writes.
    async fn ssp_prime(&self, pending_ids: Vec<String>) -> Result<()>;
    async fn ssp_reset(&self) -> Result<()>;
    fn ssp_set_persistence(&self, enabled: bool);

    async fn auth_restore_session(&self) -> Result<Option<String>>;
    async fn auth_init(&self) -> Result<()>;
    fn auth_session_auth_id(&self) -> Option<String>;
    fn auth_access(&self) -> Option<String>;
    fn auth_token(&self) -> Option<String>;
    fn auth_current_user(&self) -> Option<Object>;

    async fn remote_connect(&self) -> Result<()>;
    /// The host reports no network at all (`Sp00kyClient::set_offline`).
    fn net_offline(&self) -> bool;
    /// Hand views back to the server (`fn::query::unsubscribe`), best effort.
    fn remote_release_views(&self, ids: Vec<String>);
    fn supervisor_start(&self);

    fn crdt_set_session_id(&self, session_id: &str);
    fn crdt_close_all(&self, flush: bool);

    async fn persistence_set(&self, key: &str, value: Option<Value>) -> Result<()>;
    /// Start the modules whose live query must begin once boot has finished.
    fn features_init(&self);
    fn releases_init(&self);
}

// ---- the bundle ----------------------------------------------------------------------

/// Which id an [`Adapters::ids`] call mints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdScope {
    /// An outbox row id.
    Mutation,
    /// A query-key salt.
    Salt,
    /// The client's anonymous-view owner secret.
    Owner,
}

/// The adapters plus the pure ports (clock, ids, hash).
#[derive(Clone)]
pub struct Adapters {
    pub local: Rc<dyn LocalPort>,
    pub remote: Rc<dyn RemotePort>,
    pub ssp: Rc<dyn SspPort>,
    pub timers: Rc<dyn TimerPort>,
    pub services: Rc<dyn Services>,
    /// Milliseconds since the epoch.
    pub now: Rc<dyn Fn() -> i64>,
    pub ids: Rc<dyn Fn(IdScope) -> String>,
    /// Lowercase hex sha256.
    pub hash: Rc<dyn Fn(&str) -> String>,
}
