//! Configuration and the plain data types the public API speaks.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::value::Object;

// ---- schema ------------------------------------------------------------------

/// One declared column, as much as the client needs to know about it (TS
/// `ColumnSchema`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ColumnSchema {
    /// A `record<...>` link: re-typed to a [`crate::RecordId`] on the way out.
    pub record_id: bool,
    /// A `datetime`: re-typed to a datetime on the way out.
    pub date_time: bool,
    /// The SurrealQL type as written (`string`, `option<int>`, ...).
    #[serde(rename = "type")]
    pub kind: Option<String>,
    /// `option<...>`: the server leaves it out when unset.
    pub optional: bool,
    /// `-- @ephemeral` on a `-- @channel` table: travels between peers only and
    /// is never written to the database.
    pub ephemeral: bool,
}

impl ColumnSchema {
    pub fn of_type(kind: impl Into<String>) -> Self {
        ColumnSchema {
            kind: Some(kind.into()),
            ..Default::default()
        }
    }
}

/// One table of the schema.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TableSchema {
    pub columns: BTreeMap<String, ColumnSchema>,
    /// `-- @channel`: clients may open peer to peer channels on its rows.
    pub channel: bool,
}

/// A relationship `.related()` can follow (TS
/// `SchemaStructure.relationships[n]`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRelationship {
    /// The table declaring it.
    pub from: String,
    /// The name used in `.related()` and the alias of the subquery.
    pub field: String,
    /// The joined table.
    pub to: String,
    /// `one` for a forward `record<x>` field, `many` for the reverse direction.
    pub cardinality: Cardinality,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Cardinality {
    One,
    Many,
}

impl SchemaRelationship {
    pub fn is_many(&self) -> bool {
        self.cardinality == Cardinality::Many
    }

    /// The correlation field: a `many` relation matches the child's field named
    /// after the PARENT table, a `one` relation reads the parent's own field.
    pub fn foreign_key_field(&self) -> &str {
        if self.is_many() {
            &self.from
        } else {
            &self.field
        }
    }
}

/// One argument of a backend route.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RouteArg {
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub optional: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BackendRoute {
    pub args: BTreeMap<String, RouteArg>,
}

/// A backend jobs are enqueued for (`client.run(backend, path, payload)`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct BackendDef {
    pub outbox_table: String,
    pub routes: BTreeMap<String, BackendRoute>,
}

/// The declared parameters of one access method (`signIn` / `signup`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AccessMethod {
    pub params: BTreeMap<String, ColumnSchema>,
}

/// A `DEFINE ACCESS ... TYPE RECORD`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AccessDef {
    #[serde(rename = "signIn")]
    pub signin: AccessMethod,
    pub signup: AccessMethod,
}

/// The runtime schema (the Dart `Map<String, dynamic>` schema, typed).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Schema {
    pub tables: BTreeMap<String, TableSchema>,
    pub relationships: Vec<SchemaRelationship>,
    pub backends: BTreeMap<String, BackendDef>,
    pub access: BTreeMap<String, AccessDef>,
}

impl Schema {
    /// The declared columns of `table`, or `None` when the schema does not know
    /// it. Internal `_00_*` tables are known-and-empty: their rows are stored
    /// whole (TS `columnsFor`).
    pub fn columns(&self, table: &str) -> Option<&BTreeMap<String, ColumnSchema>> {
        static EMPTY: BTreeMap<String, ColumnSchema> = BTreeMap::new();
        match self.tables.get(table) {
            Some(t) => Some(&t.columns),
            None if table.starts_with("_00_") => Some(&EMPTY),
            None => None,
        }
    }

    /// Every table the client syncs (all schema tables).
    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    /// Builder: add a table with its columns.
    pub fn with_table<I, K>(mut self, name: impl Into<String>, columns: I) -> Self
    where
        I: IntoIterator<Item = (K, ColumnSchema)>,
        K: Into<String>,
    {
        self.tables.insert(
            name.into(),
            TableSchema {
                columns: columns.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                channel: false,
            },
        );
        self
    }
}

// ---- config ------------------------------------------------------------------

/// Where the local store lives.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum StoreType {
    /// An in-memory sqlite database per bucket: nothing survives a restart.
    #[default]
    Memory,
    /// sqlite files, one per bucket, derived from this path
    /// (`spooky.db` -> `spooky.<bucket>.db`).
    File(PathBuf),
}

/// The remote database (TS `Sp00kyConfig.database`).
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// `ws://` / `wss://` RPC endpoint, e.g. `wss://host/rpc`. `None` makes the
    /// client local-only.
    pub endpoint: Option<String>,
    pub namespace: String,
    pub database: String,
    pub store: StoreType,
    /// A token to authenticate with on connect.
    pub token: Option<String>,
    /// Remote reads in flight at once (registrations, membership reads, body
    /// fetches, probes, `query_remote`). Writes run one at a time in a lane of
    /// their own.
    pub max_concurrent_queries: usize,
}

impl DatabaseConfig {
    pub fn new(namespace: impl Into<String>, database: impl Into<String>) -> Self {
        DatabaseConfig {
            endpoint: None,
            namespace: namespace.into(),
            database: database.into(),
            store: StoreType::Memory,
            token: None,
            max_concurrent_queries: 6,
        }
    }

    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn store(mut self, store: StoreType) -> Self {
        self.store = store;
        self
    }
}

/// When a run of failures is reported as `degraded` (TS `SyncHealthConfig`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncHealthConfig {
    /// Consecutive failed sync rounds before the status flips to degraded. `0`
    /// never reports degraded.
    pub degrade_after_consecutive_failures: u32,
}

impl Default for SyncHealthConfig {
    fn default() -> Self {
        SyncHealthConfig {
            degrade_after_consecutive_failures: 3,
        }
    }
}

impl SyncHealthConfig {
    pub const DISABLED: SyncHealthConfig = SyncHealthConfig {
        degrade_after_consecutive_failures: 0,
    };
}

/// Transport supervision knobs (TS `ReconnectConfig`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReconnectConfig {
    /// Cap on the exponential backoff between reconnect attempts.
    pub retry_delay_max_ms: u64,
    /// Cadence of the liveness probe (`RETURN true`). `0` disables it.
    pub heartbeat_interval_ms: u64,
    /// Deadline for a probe answer; past it the socket is rebuilt.
    pub heartbeat_timeout_ms: u64,
    /// Deadline for opening a socket (a black-holed endpoint never answers).
    pub connect_timeout_ms: u64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        ReconnectConfig {
            retry_delay_max_ms: 15_000,
            heartbeat_interval_ms: 20_000,
            heartbeat_timeout_ms: 10_000,
            connect_timeout_ms: 10_000,
        }
    }
}

/// The local cache for bucket files (TS `Sp00kyConfig.blobCache`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobCacheConfig {
    /// Root of the on-disk cache; `None` keeps it in memory.
    pub directory: Option<PathBuf>,
    /// Budget for one user's files; least recently used entries go first.
    pub max_bytes: u64,
    /// Wipe the signed-out user's files on sign-out.
    pub clear_on_sign_out: bool,
}

impl Default for BlobCacheConfig {
    fn default() -> Self {
        BlobCacheConfig {
            directory: None,
            max_bytes: 256 * 1024 * 1024,
            clear_on_sign_out: false,
        }
    }
}

/// Where the client keeps per-device key/value state (the boot hint, the auth
/// token).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum PersistenceConfig {
    /// A `_00_kv` table in a session database next to the local store (the
    /// default; in memory for a memory store).
    #[default]
    Session,
    /// Process memory only.
    Memory,
}

/// Configuration for the client (TS `Sp00kyConfig`).
#[derive(Clone, Debug)]
pub struct Sp00kyConfig {
    pub database: DatabaseConfig,
    pub schema: Schema,
    /// The compiled SurrealQL schema text (provisioning and permissions).
    pub schema_surql: String,
    pub persistence: PersistenceConfig,
    /// Minimum gap (ms) between two re-materializations of one query.
    pub stream_debounce_ms: u64,
    pub crdt_debounce_ms: u64,
    /// `_00_list_ref` poll base cadence (ms).
    pub ref_sync_interval_ms: u64,
    /// Realtime sync while signed out (`_00_list_ref_anon`). Must match the
    /// server's `anonymousLiveQueries`.
    pub enable_anonymous_live_queries: bool,
    pub sync_health: SyncHealthConfig,
    /// Ask the LIVE subscription to join the changed row (`FETCH out`).
    pub live_inline_bodies: bool,
    pub reconnect: ReconnectConfig,
    pub blob_cache: BlobCacheConfig,
    /// How often a dirty circuit is snapshotted to the local store (ms).
    pub circuit_checkpoint_ms: u64,
    /// Peer to peer channels for `-- @channel` tables: accept rules, the
    /// WebRTC stack, a custom transport. `None` uses the defaults (no WebRTC
    /// stack, so channels report `unsupported` unless a transport is given).
    pub channels: Option<crate::modules::channel::ChannelsConfig>,
}

impl Sp00kyConfig {
    pub fn new(database: DatabaseConfig, schema: Schema, schema_surql: impl Into<String>) -> Self {
        Sp00kyConfig {
            database,
            schema,
            schema_surql: schema_surql.into(),
            persistence: PersistenceConfig::Session,
            stream_debounce_ms: 50,
            crdt_debounce_ms: 500,
            ref_sync_interval_ms: 500,
            enable_anonymous_live_queries: false,
            sync_health: SyncHealthConfig::default(),
            live_inline_bodies: false,
            reconnect: ReconnectConfig::default(),
            blob_cache: BlobCacheConfig::default(),
            circuit_checkpoint_ms: 30_000,
            channels: None,
        }
    }
}

// ---- sync health -------------------------------------------------------------

/// Transport state of the socket, independent of sync health.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Reconnecting,
    #[default]
    Disconnected,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SyncHealthStatus {
    #[default]
    Healthy,
    Degraded,
}

/// What a "can't reach the server" banner reads (TS `SyncHealth`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncHealth {
    pub status: SyncHealthStatus,
    /// Consecutive failed sync rounds at the moment of this report.
    pub consecutive_failures: u32,
    /// `network` / `application`; only while degraded.
    pub kind: Option<String>,
    /// The last failure's message; only while degraded.
    pub error: Option<String>,
    /// At least one round succeeded this session. Never resets.
    pub ever_connected: bool,
    pub connection: ConnectionState,
}

impl SyncHealth {
    pub fn is_degraded(&self) -> bool {
        self.status == SyncHealthStatus::Degraded
    }
}

// ---- queries -----------------------------------------------------------------

/// The salted `_00_query` hash that names a registered query.
pub type QueryHash = String;

/// A `(record id, version)` pair.
pub type RecordVersion = (String, i64);

/// A result set as id/version pairs.
pub type RecordVersionArray = Vec<RecordVersion>;

/// A query's fetch status: `fetching` while bodies are being pulled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum QueryStatus {
    #[default]
    Idle,
    Fetching,
}

/// Query time-to-live (`10m`, `1h`, `30s`).
pub type QueryTimeToLive = String;

/// The client-wide default ttl.
pub const DEFAULT_TTL: &str = "10m";

/// `<n>[smh]` -> milliseconds; anything else is 10 minutes (TS `parseDuration`).
pub fn parse_ttl_ms(ttl: &str) -> u64 {
    let digits = ttl.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits == 0 || digits + 1 != ttl.len() {
        return 600_000;
    }
    let Ok(n) = ttl[..digits].parse::<u64>() else {
        return 600_000;
    };
    match &ttl[digits..] {
        "s" => n * 1000,
        "h" => n * 3_600_000,
        "m" => n * 60_000,
        _ => 600_000,
    }
}

/// What the server's `_00_query` row says about a view (TS `ServerViewMeta`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServerViewMeta {
    /// False: the row is gone (TTL sweep, SSP reset). An empty edge set read at
    /// the same time means "no view", not "no rows".
    pub present: bool,
    pub row_count: Option<i64>,
    /// `materializing` or `ready`.
    pub state: Option<String>,
}

impl ServerViewMeta {
    pub const ABSENT: ServerViewMeta = ServerViewMeta {
        present: false,
        row_count: None,
        state: None,
    };
}

/// A row that rode in on a LIVE notification (`FETCH out`).
#[derive(Clone, Debug, PartialEq)]
pub struct InlineRow {
    pub id: String,
    /// The edge's version, the number a membership read would report.
    pub version: i64,
    pub record: Object,
}

/// One-shot registration timings (ms).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RegistrationTimings {
    pub parse_ms: Option<f64>,
    pub plan_ms: Option<f64>,
    pub snapshot_ms: Option<f64>,
    pub wall_ms: Option<f64>,
}

/// Timed phases surfaced per query (TS `TimingPhase`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimingPhase {
    SspStoreApply,
    SspCircuitStep,
    SspTransform,
    LocalFetch,
    RemoteFetch,
    Frontend,
}

/// Percentile summary of one timed phase.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PhaseStat {
    pub last_ms: Option<f64>,
    pub p50: Option<f64>,
    pub p90: Option<f64>,
    pub p99: Option<f64>,
    pub count: usize,
}

/// Per-query processing-time breakdown.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryTimings {
    pub ssp: PhaseStat,
    pub ssp_store_apply: PhaseStat,
    pub ssp_circuit_step: PhaseStat,
    pub ssp_transform: PhaseStat,
    pub local_fetch: PhaseStat,
    pub remote_fetch: PhaseStat,
    pub frontend: PhaseStat,
    pub registration: RegistrationTimings,
    pub update_count: u64,
    pub error_count: u64,
}

// ---- mutations ---------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MutationEventType {
    Create,
    Update,
    Delete,
}

impl MutationEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            MutationEventType::Create => "create",
            MutationEventType::Update => "update",
            MutationEventType::Delete => "delete",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "create" => Some(MutationEventType::Create),
            "update" => Some(MutationEventType::Update),
            "delete" => Some(MutationEventType::Delete),
            _ => None,
        }
    }
}

/// How a debounced update is keyed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DebounceKey {
    /// One pending write per record.
    RecordId,
    /// One pending write per record and set of fields (the default, as in the
    /// Dart and TypeScript cores).
    #[default]
    RecordIdXFields,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DebounceOptions {
    pub key: DebounceKey,
    /// Milliseconds; the default is 300.
    pub delay_ms: Option<u64>,
}

/// Options for `update` (TS `UpdateOptions`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UpdateOptions {
    /// Fold rapid updates of one record into one outbox write.
    pub debounced: Option<DebounceOptions>,
}

impl UpdateOptions {
    pub fn debounced() -> Self {
        UpdateOptions {
            debounced: Some(DebounceOptions::default()),
        }
    }
}

/// Options for `run` (TS `RunOptions`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunOptions {
    pub assigned_to: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_strategy: Option<String>,
    pub timeout: Option<u64>,
    /// Minimum delay (ms) before the job may run.
    pub delay: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_parsing() {
        assert_eq!(parse_ttl_ms("10m"), 600_000);
        assert_eq!(parse_ttl_ms("30s"), 30_000);
        assert_eq!(parse_ttl_ms("2h"), 7_200_000);
        assert_eq!(parse_ttl_ms("1d"), 600_000);
        assert_eq!(parse_ttl_ms("bogus"), 600_000);
    }

    #[test]
    fn schema_json_shape() {
        let s: Schema = serde_json::from_value(serde_json::json!({
            "tables": {"thread": {"columns": {"author": {"recordId": true, "type": "record<user>"}}}},
            "relationships": [{"from": "thread", "field": "author", "to": "user", "cardinality": "one"}]
        }))
        .unwrap();
        assert!(s.columns("thread").unwrap()["author"].record_id);
        assert!(s.columns("_00_view").unwrap().is_empty());
        assert!(s.columns("nope").is_none());
        assert_eq!(s.relationships[0].foreign_key_field(), "author");
    }
}
