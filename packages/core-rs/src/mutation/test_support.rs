//! Test helpers for the mutation and boot sagas: a [`TestRig`] whose adapters
//! can be told to fail (the Dart tests script throwing handlers), an outbound
//! event recorder, and a per-bucket local store for account switches.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::kernel::events::{LogLevel, OutEvent};
use crate::kernel::ports::{
    Adapters, IdScope, IngestRecord, LocalOp, LocalPort, RegisterPlan, RegisterResult, Services,
    SspPort, WriteMode,
};
use crate::query::env::SagaEnv;
use crate::runtime::{Runtime, Subscription};
use crate::state::{ClientState, QueryEntry};
use crate::testing::{CallLog, FakeLocal, FakeRemote, FakeServices, FakeSsp, FakeTimers, TestRig};
use crate::types::{ColumnSchema, Schema};
use crate::value::{Object, Row, Value};

type Tables = BTreeMap<String, BTreeMap<String, Row>>;

/// What the adapters of a [`new_rig`] should fail on.
#[derive(Clone, Default)]
pub struct Faults {
    services: Rc<RefCell<BTreeSet<&'static str>>>,
    ssp_register: Rc<Cell<bool>>,
    ssp_ingest: Rc<Cell<bool>>,
    local_get_all: Rc<Cell<bool>>,
    /// `local_switch_store` swaps the fake store's tables per bucket.
    bucket_stores: Rc<Cell<bool>>,
}

impl Faults {
    /// Fail the service call named like its `service.<name>` log entry.
    pub fn fail_service(&self, name: &'static str) {
        self.services.borrow_mut().insert(name);
    }

    pub fn fail_ssp_register(&self) {
        self.ssp_register.set(true);
    }

    pub fn fail_ssp_ingest(&self) {
        self.ssp_ingest.set(true);
    }

    pub fn fail_local_get_all(&self) {
        self.local_get_all.set(true);
    }

    /// Give every bucket its own local store, as the real service does.
    pub fn bucket_stores(&self) {
        self.bucket_stores.set(true);
    }

    fn service(&self, name: &str) -> Result<()> {
        if self.services.borrow().contains(name) {
            Err(Error::other(format!("{name}: offline")))
        } else {
            Ok(())
        }
    }
}

struct FlakyLocal {
    inner: FakeLocal,
    faults: Faults,
}

impl LocalPort for FlakyLocal {
    fn get(&self, table: &str, id: &str) -> Result<Option<Row>> {
        self.inner.get(table, id)
    }
    fn get_many(&self, table: &str, ids: &[String]) -> Result<Vec<Row>> {
        self.inner.get_many(table, ids)
    }
    fn get_all(&self, table: &str) -> Result<Vec<Row>> {
        let rows = self.inner.get_all(table)?;
        if self.faults.local_get_all.get() {
            return Err(Error::Local("no such table".into()));
        }
        Ok(rows)
    }
    fn put(&self, table: &str, id: &str, data: Row, mode: WriteMode) -> Result<()> {
        self.inner.put(table, id, data, mode)
    }
    fn delete(&self, table: &str, id: &str) -> Result<()> {
        self.inner.delete(table, id)
    }
    fn tx(&self, ops: Vec<LocalOp>) -> Result<()> {
        self.inner.tx(ops)
    }
    fn epoch(&self) -> u64 {
        self.inner.epoch()
    }
}

struct FlakySsp {
    inner: FakeSsp,
    faults: Faults,
}

impl SspPort for FlakySsp {
    fn register(&self, plan: RegisterPlan) -> Result<RegisterResult> {
        let out = self.inner.register(plan)?;
        if self.faults.ssp_register.get() {
            return Err(Error::Ssp("circuit denied".into()));
        }
        Ok(out)
    }
    fn unregister(&self, hash: &str) -> Result<()> {
        self.inner.unregister(hash)
    }
    fn ingest_many(&self, records: Vec<IngestRecord>) -> Result<()> {
        self.inner.ingest_many(records)?;
        if self.faults.ssp_ingest.get() {
            return Err(Error::Ssp("circuit busy".into()));
        }
        Ok(())
    }
}

struct FlakyServices {
    inner: FakeServices,
    local: FakeLocal,
    stores: RefCell<BTreeMap<String, Tables>>,
    faults: Faults,
}

#[async_trait(?Send)]
impl Services for FlakyServices {
    async fn hint_read(&self) -> Result<Option<String>> {
        let out = self.inner.hint_read().await?;
        self.faults.service("hintRead").map(|_| out)
    }
    async fn hint_write(&self, bucket_id: &str) -> Result<()> {
        self.inner.hint_write(bucket_id).await?;
        self.faults.service("hintWrite")
    }
    async fn local_connect(&self, bucket_id: &str) -> Result<()> {
        self.inner.local_connect(bucket_id).await?;
        self.faults.service("localConnect")
    }
    async fn local_switch_store(&self, bucket_id: &str) -> Result<()> {
        let current = self.inner.bucket.borrow().clone();
        self.inner.local_switch_store(bucket_id).await?;
        self.faults.service("localSwitchStore")?;
        if self.faults.bucket_stores.get() {
            let mut stores = self.stores.borrow_mut();
            let outgoing = std::mem::take(&mut *self.local.tables.borrow_mut());
            stores.insert(current, outgoing);
            *self.local.tables.borrow_mut() = stores.remove(bucket_id).unwrap_or_default();
            self.local.bump_epoch();
        }
        Ok(())
    }
    fn local_current_bucket_id(&self) -> String {
        self.inner.local_current_bucket_id()
    }
    async fn migrator_provision(&self) -> Result<()> {
        self.inner.migrator_provision().await?;
        self.faults.service("migratorProvision")
    }
    async fn ssp_init(&self) -> Result<()> {
        self.inner.ssp_init().await?;
        self.faults.service("sspInit")
    }
    fn ssp_set_permissions(&self) -> Result<()> {
        self.inner.ssp_set_permissions()?;
        self.faults.service("sspSetPermissions")
    }
    fn ssp_set_session_auth(&self, auth_id: Option<&str>, access: Option<&str>) {
        self.inner.ssp_set_session_auth(auth_id, access)
    }
    async fn ssp_prime(&self, pending_ids: Vec<String>) -> Result<()> {
        self.inner.ssp_prime(pending_ids).await?;
        self.faults.service("sspPrime")
    }
    async fn ssp_reset(&self) -> Result<()> {
        self.inner.ssp_reset().await?;
        self.faults.service("sspReset")
    }
    fn ssp_set_persistence(&self, enabled: bool) {
        self.inner.ssp_set_persistence(enabled)
    }
    async fn auth_restore_session(&self) -> Result<Option<String>> {
        let out = self.inner.auth_restore_session().await?;
        self.faults.service("authRestoreSession").map(|_| out)
    }
    async fn auth_init(&self) -> Result<()> {
        self.inner.auth_init().await?;
        self.faults.service("authInit")
    }
    fn auth_session_auth_id(&self) -> Option<String> {
        self.inner.auth_session_auth_id()
    }
    fn auth_access(&self) -> Option<String> {
        self.inner.auth_access()
    }
    fn auth_token(&self) -> Option<String> {
        self.inner.auth_token()
    }
    fn auth_current_user(&self) -> Option<Object> {
        self.inner.auth_current_user()
    }
    async fn remote_connect(&self) -> Result<()> {
        self.inner.remote_connect().await?;
        self.faults.service("remoteConnect")
    }
    fn net_offline(&self) -> bool {
        self.inner.net_offline()
    }
    fn remote_release_views(&self, ids: Vec<String>) {
        self.inner.remote_release_views(ids)
    }
    fn supervisor_start(&self) {
        self.inner.supervisor_start()
    }
    fn crdt_set_session_id(&self, session_id: &str) {
        self.inner.crdt_set_session_id(session_id)
    }
    fn crdt_close_all(&self, flush: bool) {
        self.inner.crdt_close_all(flush)
    }
    async fn persistence_set(&self, key: &str, value: Option<Value>) -> Result<()> {
        self.inner.persistence_set(key, value).await?;
        self.faults.service("persistenceSet")
    }
    fn features_init(&self) {
        self.inner.features_init()
    }
    fn releases_init(&self) {
        self.inner.releases_init()
    }
}

/// A [`TestRig`] over fakes that a test can make fail (see [`Faults`]).
pub fn new_rig(env: SagaEnv, state: ClientState) -> (TestRig, Faults) {
    let log = CallLog::default();
    let local = FakeLocal::new(log.clone());
    let remote = FakeRemote::new(log.clone());
    let ssp = FakeSsp::new(log.clone());
    let services = FakeServices::new(log.clone());
    let timers = FakeTimers::default();
    let now = Rc::new(Cell::new(1_700_000_000_000i64));
    let counter = Rc::new(Cell::new(0u64));
    let faults = Faults::default();
    let adapters = Adapters {
        local: Rc::new(FlakyLocal {
            inner: local.clone(),
            faults: faults.clone(),
        }),
        remote: Rc::new(remote.clone()),
        ssp: Rc::new(FlakySsp {
            inner: ssp.clone(),
            faults: faults.clone(),
        }),
        timers: Rc::new(timers.clone()),
        services: Rc::new(FlakyServices {
            inner: services.clone(),
            local: local.clone(),
            stores: RefCell::default(),
            faults: faults.clone(),
        }),
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
    let rig = TestRig {
        log,
        local,
        remote,
        ssp,
        services,
        timers,
        now,
        runtime,
    };
    (rig, faults)
}

/// The schema the Dart saga tests share: one domain table plus a bare one.
pub fn test_schema() -> Schema {
    Schema::default()
        .with_table(
            "thing",
            [
                ("title", ColumnSchema::of_type("string")),
                (
                    "owner",
                    ColumnSchema {
                        record_id: true,
                        ..ColumnSchema::of_type("record<user>")
                    },
                ),
            ],
        )
        .with_table("child", Vec::<(String, ColumnSchema)>::new())
}

pub fn test_env() -> SagaEnv {
    SagaEnv::new(Arc::new(test_schema()))
}

/// Dart `buildState(entries)`: the entries, with `dirty` cleared (as after
/// materialization).
pub fn state_with(entries: Vec<QueryEntry>) -> ClientState {
    let mut s = ClientState::empty("tab-a");
    for e in entries {
        crate::state::reducers::put_query(&mut s, e);
    }
    s.dirty.clear();
    s
}

/// Every outbound event, in order.
pub struct Events {
    list: Rc<RefCell<Vec<OutEvent>>>,
    _sub: Subscription,
}

impl Events {
    pub fn all(&self) -> Vec<OutEvent> {
        self.list.borrow().clone()
    }

    pub fn logs(&self) -> Vec<(LogLevel, String)> {
        self.list
            .borrow()
            .iter()
            .filter_map(|e| match e {
                OutEvent::Log { level, message, .. } => Some((*level, message.clone())),
                _ => None,
            })
            .collect()
    }

    pub fn count(&self, kind: &str) -> usize {
        self.list
            .borrow()
            .iter()
            .filter(|e| e.kind() == kind)
            .count()
    }
}

pub fn record_events(rig: &TestRig) -> Events {
    let list: Rc<RefCell<Vec<OutEvent>>> = Rc::default();
    let sink = list.clone();
    let sub = rig
        .runtime
        .on("*", move |e| sink.borrow_mut().push(e.clone()));
    Events { list, _sub: sub }
}

/// The service calls made, by name, in order.
pub fn service_calls(rig: &TestRig) -> Vec<String> {
    rig.log
        .names()
        .into_iter()
        .filter_map(|n| n.strip_prefix("service.").map(str::to_string))
        .collect()
}

/// `needles` appear in `haystack` in this order (Dart `containsAllInOrder`).
pub fn in_order(haystack: &[String], needles: &[&str]) -> bool {
    let mut it = haystack.iter();
    needles.iter().all(|n| it.any(|h| h == n))
}
