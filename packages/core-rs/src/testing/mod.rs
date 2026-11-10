//! Fakes for every port plus builders, so a saga runs against a real
//! [`Runtime`] with canned adapters. Every adapter call is recorded in a shared
//! [`CallLog`], so a test can assert the effect sequence the way the Dart and
//! TypeScript tests assert their effect logs.
//!
//! ```ignore
//! let rig = TestRig::new();
//! rig.run(|ctx| async move { my_saga(&ctx).await }).await?;
//! assert!(rig.log.contains("remote.query"));
//! ```

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::kernel::ports::{
    Adapters, IdScope, IngestRecord, LiveCallback, LocalOp, LocalPort, RegisterPlan, RegisterResult, RemotePort,
    Services, SspPort, StatementResult, TimerPort, WriteMode,
};
use crate::kernel::Ctx;
use crate::modules::query_builder::RelationPlan;
use crate::query::env::SagaEnv;
use crate::runtime::Runtime;
use crate::state::lifecycle::{seed_lifecycle, QueryLifecycle};
use crate::state::{ClientState, OutboxItem, OutboxStatus, QueryDefinition, QueryEntry, QueryTelemetry};
use crate::types::{MutationEventType, RecordVersionArray, Schema};
use crate::value::{deep_merge, Object, RecordId, Row, Value};

/// One recorded adapter call: its name and a debug rendering of its args.
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    pub name: String,
    pub args: String,
}

/// Shared, ordered record of adapter calls.
#[derive(Clone, Default)]
pub struct CallLog(Rc<RefCell<Vec<Call>>>);

impl CallLog {
    pub fn push(&self, name: &str, args: impl std::fmt::Debug) {
        self.0.borrow_mut().push(Call {
            name: name.to_string(),
            args: format!("{args:?}"),
        });
    }

    pub fn calls(&self) -> Vec<Call> {
        self.0.borrow().clone()
    }

    /// The call names, in order.
    pub fn names(&self) -> Vec<String> {
        self.0.borrow().iter().map(|c| c.name.clone()).collect()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.0.borrow().iter().any(|c| c.name == name)
    }

    pub fn count(&self, name: &str) -> usize {
        self.0.borrow().iter().filter(|c| c.name == name).count()
    }

    /// The calls named `name`.
    pub fn named(&self, name: &str) -> Vec<Call> {
        self.0.borrow().iter().filter(|c| c.name == name).cloned().collect()
    }

    pub fn clear(&self) {
        self.0.borrow_mut().clear();
    }
}

// ---- timers ---------------------------------------------------------------------

/// Timers that only fire when the test says so.
type PendingTimers = BTreeMap<String, (u64, Option<Box<dyn FnOnce()>>)>;

#[derive(Clone, Default)]
pub struct FakeTimers {
    pending: Rc<RefCell<PendingTimers>>,
}

impl FakeTimers {
    /// Pending keys with their delays.
    pub fn pending(&self) -> BTreeMap<String, u64> {
        self.pending.borrow().iter().map(|(k, (ms, _))| (k.clone(), *ms)).collect()
    }

    pub fn has(&self, key: &str) -> bool {
        self.pending.borrow().contains_key(key)
    }

    pub fn delay(&self, key: &str) -> Option<u64> {
        self.pending.borrow().get(key).map(|(ms, _)| *ms)
    }

    /// Fire one timer; false when none was pending.
    pub fn fire(&self, key: &str) -> bool {
        let entry = self.pending.borrow_mut().remove(key);
        match entry {
            Some((_, Some(f))) => {
                f();
                true
            }
            _ => false,
        }
    }

    /// Fire every timer whose key starts with `prefix`.
    pub fn fire_prefix(&self, prefix: &str) -> usize {
        let keys: Vec<String> = self.pending.borrow().keys().filter(|k| k.starts_with(prefix)).cloned().collect();
        keys.iter().filter(|k| self.fire(k)).count()
    }
}

impl TimerPort for FakeTimers {
    fn set(&self, key: &str, ms: u64, fire: Box<dyn FnOnce()>) {
        self.pending.borrow_mut().insert(key.to_string(), (ms, Some(fire)));
    }

    fn clear(&self, key: &str) {
        self.pending.borrow_mut().remove(key);
    }

    fn clear_all(&self) {
        self.pending.borrow_mut().clear();
    }
}

// ---- local store -------------------------------------------------------------------

/// An in-memory document store: `table -> id -> row`.
#[derive(Clone)]
pub struct FakeLocal {
    log: CallLog,
    pub tables: Rc<RefCell<BTreeMap<String, BTreeMap<String, Row>>>>,
    epoch: Rc<Cell<u64>>,
    /// When set, every write fails with this message.
    pub fail_writes: Rc<RefCell<Option<String>>>,
}

impl FakeLocal {
    pub fn new(log: CallLog) -> Self {
        FakeLocal {
            log,
            tables: Rc::default(),
            epoch: Rc::new(Cell::new(1)),
            fail_writes: Rc::default(),
        }
    }

    pub fn bump_epoch(&self) {
        self.epoch.set(self.epoch.get() + 1);
    }

    /// Seed a row (not logged).
    pub fn seed(&self, table: &str, id: &str, row: Row) {
        self.tables.borrow_mut().entry(table.to_string()).or_default().insert(id.to_string(), row);
    }

    pub fn row(&self, table: &str, id: &str) -> Option<Row> {
        self.tables.borrow().get(table).and_then(|t| t.get(id)).cloned()
    }

    fn check_write(&self) -> Result<()> {
        match &*self.fail_writes.borrow() {
            Some(msg) => Err(Error::Local(msg.as_str().into())),
            None => Ok(()),
        }
    }

    fn apply_put(&self, table: &str, id: &str, data: Row, mode: WriteMode, clear: &[String]) {
        let mut tables = self.tables.borrow_mut();
        let t = tables.entry(table.to_string()).or_default();
        match mode {
            WriteMode::Replace => {
                t.insert(id.to_string(), data);
            }
            WriteMode::Merge => {
                let row = t.entry(id.to_string()).or_default();
                deep_merge(row, &data);
                for k in clear {
                    if k != "id" {
                        row.remove(k);
                    }
                }
            }
        }
    }
}

impl LocalPort for FakeLocal {
    fn get(&self, table: &str, id: &str) -> Result<Option<Row>> {
        self.log.push("local.get", (table, id));
        Ok(self.row(table, id))
    }

    fn get_many(&self, table: &str, ids: &[String]) -> Result<Vec<Row>> {
        self.log.push("local.getMany", (table, ids));
        let tables = self.tables.borrow();
        let t = tables.get(table);
        Ok(ids.iter().filter_map(|id| t.and_then(|t| t.get(id)).cloned()).collect())
    }

    fn get_all(&self, table: &str) -> Result<Vec<Row>> {
        self.log.push("local.getAll", table);
        Ok(self.tables.borrow().get(table).map(|t| t.values().cloned().collect()).unwrap_or_default())
    }

    fn put(&self, table: &str, id: &str, data: Row, mode: WriteMode) -> Result<()> {
        self.log.push("local.put", (table, id, &data, mode));
        self.check_write()?;
        self.apply_put(table, id, data, mode, &[]);
        Ok(())
    }

    fn delete(&self, table: &str, id: &str) -> Result<()> {
        self.log.push("local.delete", (table, id));
        self.check_write()?;
        if let Some(t) = self.tables.borrow_mut().get_mut(table) {
            t.remove(id);
        }
        Ok(())
    }

    fn tx(&self, ops: Vec<LocalOp>) -> Result<()> {
        self.log.push("local.tx", &ops);
        self.check_write()?;
        for op in ops {
            match op {
                LocalOp::Put {
                    table,
                    id,
                    data,
                    mode,
                    clear,
                } => self.apply_put(&table, &id, data, mode, &clear),
                LocalOp::Delete { table, id } => {
                    if let Some(t) = self.tables.borrow_mut().get_mut(&table) {
                        t.remove(&id);
                    }
                }
                LocalOp::BumpRv { table, id } => {
                    if let Some(row) = self.tables.borrow_mut().get_mut(&table).and_then(|t| t.get_mut(&id)) {
                        let rv = row.get("_00_rv").and_then(Value::as_i64).unwrap_or(0) + 1;
                        row.insert("_00_rv".into(), Value::Int(rv));
                    }
                }
            }
        }
        Ok(())
    }

    fn epoch(&self) -> u64 {
        self.epoch.get()
    }
}

// ---- remote -----------------------------------------------------------------------

/// Answers a remote request: `(sql, vars) -> outcome`.
pub type RemoteAnswer = Rc<dyn Fn(&str, Option<&Object>) -> Result<Vec<StatementResult>>>;

/// A scripted remote. Without an answer every request returns no statements.
#[derive(Clone)]
pub struct FakeRemote {
    log: CallLog,
    pub answer: Rc<RefCell<Option<RemoteAnswer>>>,
    /// The callback of the last `live` subscription, to push notifications.
    pub on_live_change: Rc<RefCell<Option<LiveCallback>>>,
    pub killed: Rc<RefCell<Vec<String>>>,
    /// When set, `live` fails with this message.
    pub fail_live: Rc<RefCell<Option<String>>>,
}

impl FakeRemote {
    pub fn new(log: CallLog) -> Self {
        FakeRemote {
            log,
            answer: Rc::default(),
            on_live_change: Rc::default(),
            killed: Rc::default(),
            fail_live: Rc::default(),
        }
    }

    /// Script every answer.
    pub fn answer_with(&self, f: impl Fn(&str, Option<&Object>) -> Result<Vec<StatementResult>> + 'static) {
        *self.answer.borrow_mut() = Some(Rc::new(f));
    }

    /// Push a LIVE notification through the last subscription.
    pub fn push_live(&self, hashes: Vec<String>, rows: Option<Vec<crate::types::InlineRow>>) {
        let cb = self.on_live_change.borrow().clone();
        if let Some(cb) = cb {
            cb(hashes, rows);
        }
    }

    fn respond(&self, sql: &str, vars: Option<&Object>) -> Result<Vec<StatementResult>> {
        let answer = self.answer.borrow().clone();
        match answer {
            Some(f) => f(sql, vars),
            None => Ok(Vec::new()),
        }
    }
}

#[async_trait(?Send)]
impl RemotePort for FakeRemote {
    async fn query(&self, sql: &str, vars: Option<Object>) -> Result<Vec<StatementResult>> {
        self.log.push("remote.query", (sql, &vars));
        self.respond(sql, vars.as_ref())
    }

    async fn write(&self, sql: &str, vars: Option<Object>) -> Result<Vec<StatementResult>> {
        self.log.push("remote.write", (sql, &vars));
        self.respond(sql, vars.as_ref())
    }

    async fn live(&self, table: &str, on_change: LiveCallback) -> Result<String> {
        self.log.push("remote.live", table);
        if let Some(msg) = self.fail_live.borrow().clone() {
            return Err(Error::Network(msg));
        }
        *self.on_live_change.borrow_mut() = Some(on_change);
        Ok("live-uuid".to_string())
    }

    async fn kill(&self, uuid: &str) -> Result<()> {
        self.log.push("remote.kill", uuid);
        self.killed.borrow_mut().push(uuid.to_string());
        Ok(())
    }
}

// ---- circuit ----------------------------------------------------------------------

type LocalArrayFn = Rc<dyn Fn(&str) -> RecordVersionArray>;

/// A circuit that answers registrations from a script and records ingests.
#[derive(Clone)]
pub struct FakeSsp {
    log: CallLog,
    pub local_array_for: Rc<RefCell<Option<LocalArrayFn>>>,
    pub ingested: Rc<RefCell<Vec<IngestRecord>>>,
    pub unregistered: Rc<RefCell<Vec<String>>>,
    pub registered: Rc<RefCell<Vec<RegisterPlan>>>,
}

impl FakeSsp {
    pub fn new(log: CallLog) -> Self {
        FakeSsp {
            log,
            local_array_for: Rc::default(),
            ingested: Rc::default(),
            unregistered: Rc::default(),
            registered: Rc::default(),
        }
    }

    pub fn local_arrays(&self, f: impl Fn(&str) -> RecordVersionArray + 'static) {
        *self.local_array_for.borrow_mut() = Some(Rc::new(f));
    }
}

impl SspPort for FakeSsp {
    fn register(&self, plan: RegisterPlan) -> Result<RegisterResult> {
        self.log.push("ssp.register", &plan.query_hash);
        let local_array = self
            .local_array_for
            .borrow()
            .as_ref()
            .map(|f| f(&plan.query_hash))
            .unwrap_or_default();
        self.registered.borrow_mut().push(plan);
        Ok(RegisterResult {
            local_array,
            timings: Default::default(),
        })
    }

    fn unregister(&self, hash: &str) -> Result<()> {
        self.log.push("ssp.unregister", hash);
        self.unregistered.borrow_mut().push(hash.to_string());
        Ok(())
    }

    fn ingest_many(&self, records: Vec<IngestRecord>) -> Result<()> {
        self.log.push("ssp.ingest", records.len());
        self.ingested.borrow_mut().extend(records);
        Ok(())
    }
}

// ---- services ---------------------------------------------------------------------

/// Canned service answers; every call is logged as `service.<name>`.
#[derive(Clone)]
pub struct FakeServices {
    log: CallLog,
    pub hint: Rc<RefCell<Option<String>>>,
    pub bucket: Rc<RefCell<String>>,
    pub auth_user: Rc<RefCell<Option<Object>>>,
    pub auth_access: Rc<RefCell<Option<String>>>,
    pub auth_token: Rc<RefCell<Option<String>>>,
    pub restored_user: Rc<RefCell<Option<String>>>,
    pub offline: Rc<Cell<bool>>,
    pub persisted: Rc<RefCell<BTreeMap<String, Option<Value>>>>,
}

impl FakeServices {
    pub fn new(log: CallLog) -> Self {
        FakeServices {
            log,
            hint: Rc::default(),
            bucket: Rc::new(RefCell::new("anon".into())),
            auth_user: Rc::default(),
            auth_access: Rc::default(),
            auth_token: Rc::default(),
            restored_user: Rc::default(),
            offline: Rc::default(),
            persisted: Rc::default(),
        }
    }
}

#[async_trait(?Send)]
impl Services for FakeServices {
    async fn hint_read(&self) -> Result<Option<String>> {
        self.log.push("service.hintRead", ());
        Ok(self.hint.borrow().clone())
    }
    async fn hint_write(&self, bucket_id: &str) -> Result<()> {
        self.log.push("service.hintWrite", bucket_id);
        *self.hint.borrow_mut() = Some(bucket_id.to_string());
        Ok(())
    }
    async fn local_connect(&self, bucket_id: &str) -> Result<()> {
        self.log.push("service.localConnect", bucket_id);
        *self.bucket.borrow_mut() = bucket_id.to_string();
        Ok(())
    }
    async fn local_switch_store(&self, bucket_id: &str) -> Result<()> {
        self.log.push("service.localSwitchStore", bucket_id);
        *self.bucket.borrow_mut() = bucket_id.to_string();
        Ok(())
    }
    fn local_current_bucket_id(&self) -> String {
        self.log.push("service.localCurrentBucketId", ());
        self.bucket.borrow().clone()
    }
    async fn migrator_provision(&self) -> Result<()> {
        self.log.push("service.migratorProvision", ());
        Ok(())
    }
    async fn ssp_init(&self) -> Result<()> {
        self.log.push("service.sspInit", ());
        Ok(())
    }
    fn ssp_set_permissions(&self) -> Result<()> {
        self.log.push("service.sspSetPermissions", ());
        Ok(())
    }
    fn ssp_set_session_auth(&self, auth_id: Option<&str>, access: Option<&str>) {
        self.log.push("service.sspSetSessionAuth", (auth_id, access));
    }
    async fn ssp_prime(&self, pending_ids: Vec<String>) -> Result<()> {
        self.log.push("service.sspPrime", pending_ids);
        Ok(())
    }
    async fn ssp_reset(&self) -> Result<()> {
        self.log.push("service.sspReset", ());
        Ok(())
    }
    fn ssp_set_persistence(&self, enabled: bool) {
        self.log.push("service.sspSetPersistence", enabled);
    }
    async fn auth_restore_session(&self) -> Result<Option<String>> {
        self.log.push("service.authRestoreSession", ());
        Ok(self.restored_user.borrow().clone())
    }
    async fn auth_init(&self) -> Result<()> {
        self.log.push("service.authInit", ());
        Ok(())
    }
    fn auth_session_auth_id(&self) -> Option<String> {
        self.log.push("service.authSessionAuthId", ());
        self.auth_user.borrow().as_ref().and_then(|u| u.get("id")).map(Value::to_id_string)
    }
    fn auth_access(&self) -> Option<String> {
        self.log.push("service.authAccess", ());
        self.auth_access.borrow().clone()
    }
    fn auth_token(&self) -> Option<String> {
        self.log.push("service.authToken", ());
        self.auth_token.borrow().clone()
    }
    fn auth_current_user(&self) -> Option<Object> {
        self.log.push("service.authCurrentUser", ());
        self.auth_user.borrow().clone()
    }
    async fn remote_connect(&self) -> Result<()> {
        self.log.push("service.remoteConnect", ());
        Ok(())
    }
    fn net_offline(&self) -> bool {
        self.log.push("service.netOffline", ());
        self.offline.get()
    }
    fn remote_release_views(&self, ids: Vec<String>) {
        self.log.push("service.remoteReleaseViews", ids);
    }
    fn supervisor_start(&self) {
        self.log.push("service.supervisorStart", ());
    }
    fn crdt_set_session_id(&self, session_id: &str) {
        self.log.push("service.crdtSetSessionId", session_id);
    }
    fn crdt_close_all(&self, flush: bool) {
        self.log.push("service.crdtCloseAll", flush);
    }
    async fn persistence_set(&self, key: &str, value: Option<Value>) -> Result<()> {
        self.log.push("service.persistenceSet", (key, &value));
        self.persisted.borrow_mut().insert(key.to_string(), value);
        Ok(())
    }
    fn features_init(&self) {
        self.log.push("service.featuresInit", ());
    }
    fn releases_init(&self) {
        self.log.push("service.releasesInit", ());
    }
}

// ---- the rig -----------------------------------------------------------------------

/// A runtime over fakes, plus handles on every fake.
///
/// Must be used inside a `tokio::task::LocalSet` (see [`run_local`]).
pub struct TestRig {
    pub log: CallLog,
    pub local: FakeLocal,
    pub remote: FakeRemote,
    pub ssp: FakeSsp,
    pub services: FakeServices,
    pub timers: FakeTimers,
    pub now: Rc<Cell<i64>>,
    pub runtime: Runtime,
}

impl TestRig {
    /// A rig with an empty schema, a remote, and `now = 1_700_000_000_000`.
    pub fn new() -> Self {
        Self::with(SagaEnv::new(Arc::new(Schema::default())), ClientState::empty("tab"))
    }

    pub fn with_env(env: SagaEnv) -> Self {
        Self::with(env, ClientState::empty("tab"))
    }

    pub fn with(env: SagaEnv, state: ClientState) -> Self {
        let log = CallLog::default();
        let local = FakeLocal::new(log.clone());
        let remote = FakeRemote::new(log.clone());
        let ssp = FakeSsp::new(log.clone());
        let services = FakeServices::new(log.clone());
        let timers = FakeTimers::default();
        let now = Rc::new(Cell::new(1_700_000_000_000i64));
        let counter = Rc::new(Cell::new(0u64));
        let adapters = Adapters {
            local: Rc::new(local.clone()),
            remote: Rc::new(remote.clone()),
            ssp: Rc::new(ssp.clone()),
            timers: Rc::new(timers.clone()),
            services: Rc::new(services.clone()),
            now: {
                let now = now.clone();
                Rc::new(move || now.get())
            },
            ids: Rc::new(move |scope| {
                counter.set(counter.get() + 1);
                let prefix = match scope {
                    IdScope::Mutation => "m",
                    IdScope::Salt => "salt",
                    IdScope::Owner => "owner",
                };
                format!("{prefix}{}", counter.get())
            }),
            hash: Rc::new(crate::value::sha256_hex),
        };
        let runtime = Runtime::new(env, adapters, state);
        TestRig {
            log,
            local,
            remote,
            ssp,
            services,
            timers,
            now,
            runtime,
        }
    }

    pub fn ctx(&self) -> Ctx {
        self.runtime.ctx()
    }

    pub fn env(&self) -> SagaEnv {
        self.runtime.env().clone()
    }

    pub fn state(&self) -> ClientState {
        self.runtime.snapshot()
    }

    pub fn set_now(&self, ms: i64) {
        self.now.set(ms);
    }

    pub fn advance(&self, ms: i64) {
        self.now.set(self.now.get() + ms);
    }

    /// Run a saga to completion with no lane and no fence.
    pub async fn run<R, F, Fut>(&self, saga: F) -> Result<R>
    where
        F: FnOnce(Ctx) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        self.runtime.run_unfenced(None, saga).await
    }

    /// Let spawned dispatches run (yields to the local executor a few times).
    pub async fn settle(&self) {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }
}

impl Default for TestRig {
    fn default() -> Self {
        Self::new()
    }
}

/// Run a test body inside a `LocalSet` on the current-thread runtime.
pub async fn run_local<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::LocalSet::new().run_until(f).await
}

// ---- builders ------------------------------------------------------------------------

pub fn build_definition(hash: &str, table: &str) -> QueryDefinition {
    QueryDefinition {
        id: RecordId::new("_00_query", hash),
        hash: hash.to_string(),
        view_key: format!("view-{hash}"),
        surql: format!("SELECT * FROM {table}"),
        params: Object::new(),
        ttl: "10m".into(),
        ttl_ms: 600_000,
        table_name: table.to_string(),
        created_at: 1_700_000_000_000,
        relations: Vec::<RelationPlan>::new(),
        has_explicit_order: false,
    }
}

pub fn build_entry(def: QueryDefinition) -> QueryEntry {
    QueryEntry {
        def,
        lifecycle: seed_lifecycle(false),
        remote_array: Vec::new(),
        local_array: Vec::new(),
        subquery_remote_array: Vec::new(),
        records: Arc::new(Vec::new()),
        server_state: None,
        subscribers: 0,
        last_subscriber_left_at: None,
        last_heartbeat_at: None,
        last_polled_at: None,
        register_attempts: 0,
        view_lost_count: 0,
        view_lost_retry_at: None,
        telemetry: QueryTelemetry::default(),
    }
}

/// An entry for `hash` on `table` with the given lifecycle.
pub fn entry_with(hash: &str, table: &str, lifecycle: QueryLifecycle) -> QueryEntry {
    QueryEntry {
        lifecycle,
        ..build_entry(build_definition(hash, table))
    }
}

pub fn build_outbox_item(id: &str, kind: MutationEventType, record_id: &str) -> OutboxItem {
    OutboxItem {
        id: id.to_string(),
        kind,
        record_id: record_id.to_string(),
        table: crate::value::table_of(record_id).to_string(),
        status: OutboxStatus::Pending,
        acked_at: None,
        attempts: 0,
        in_doubt: false,
    }
}

/// A row from JSON.
pub fn row(v: serde_json::Value) -> Row {
    crate::value::object_from_json(v)
}

/// Statement results: each `Ok(json)` / `Err(msg)`.
pub fn results(items: Vec<std::result::Result<serde_json::Value, &str>>) -> Vec<StatementResult> {
    items
        .into_iter()
        .map(|r| match r {
            Ok(v) => StatementResult::ok(Value::from(v)),
            Err(e) => StatementResult::err(e),
        })
        .collect()
}
