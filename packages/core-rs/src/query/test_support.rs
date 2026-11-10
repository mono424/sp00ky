//! Shared helpers for the query and sync saga tests: a [`TestRig`] whose
//! store and circuit can be made to fail or to run a hook mid-call, plus
//! builders for the server answers the sagas read.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use serde_json::json;

use crate::error::{Error, Result};
use crate::kernel::events::OutEvent;
use crate::kernel::ports::{
    Adapters, IdScope, IngestRecord, LocalOp, LocalPort, RegisterPlan, RegisterResult, SspPort,
    StatementResult, WriteMode,
};
use crate::query::env::SagaEnv;
use crate::runtime::{Runtime, Subscription, WeakRuntime};
use crate::state::lifecycle::{QueryLifecycle, QueryPhase, RemotePhase};
use crate::state::{reducers as r, ClientState, QueryEntry};
use crate::testing::{
    entry_with, results, CallLog, FakeLocal, FakeRemote, FakeServices, FakeSsp, FakeTimers, TestRig,
};
use crate::types::{ColumnSchema, RecordVersionArray, RegistrationTimings, Schema};
use crate::value::{Object, Value};

type Flag = Rc<RefCell<Option<String>>>;
type Hook = Rc<RefCell<Option<Box<dyn Fn(&Runtime, &str)>>>>;

fn fail(flag: &Flag) -> Result<()> {
    match flag.borrow().as_ref() {
        Some(msg) => Err(Error::Local(msg.as_str().into())),
        None => Ok(()),
    }
}

/// Switches that make the rig's store or circuit misbehave.
#[derive(Clone, Default)]
pub struct Hooks {
    pub fail_get: Flag,
    pub fail_get_many: Flag,
    pub fail_get_all: Flag,
    pub fail_register: Flag,
    pub fail_unregister: Flag,
    pub fail_ingest: Flag,
    /// Runs inside `get_all(table)`, before it answers.
    pub on_get_all: Hook,
    runtime: Rc<RefCell<Option<WeakRuntime>>>,
    timings: Rc<Cell<Option<f64>>>,
}

impl Hooks {
    pub fn set(flag: &Flag, msg: &str) {
        *flag.borrow_mut() = Some(msg.to_string());
    }

    pub fn on_get_all(&self, f: impl Fn(&Runtime, &str) + 'static) {
        *self.on_get_all.borrow_mut() = Some(Box::new(f));
    }
}

struct HookedLocal {
    inner: FakeLocal,
    hooks: Hooks,
}

impl LocalPort for HookedLocal {
    fn get(&self, table: &str, id: &str) -> Result<Option<crate::value::Row>> {
        fail(&self.hooks.fail_get)?;
        self.inner.get(table, id)
    }
    fn get_many(&self, table: &str, ids: &[String]) -> Result<Vec<crate::value::Row>> {
        fail(&self.hooks.fail_get_many)?;
        self.inner.get_many(table, ids)
    }
    fn get_all(&self, table: &str) -> Result<Vec<crate::value::Row>> {
        fail(&self.hooks.fail_get_all)?;
        let rt = self
            .hooks
            .runtime
            .borrow()
            .as_ref()
            .and_then(WeakRuntime::upgrade);
        if let (Some(rt), Some(hook)) = (rt, self.hooks.on_get_all.borrow().as_ref()) {
            hook(&rt, table);
        }
        self.inner.get_all(table)
    }
    fn put(&self, table: &str, id: &str, data: crate::value::Row, mode: WriteMode) -> Result<()> {
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

struct HookedSsp {
    inner: FakeSsp,
    hooks: Hooks,
}

impl SspPort for HookedSsp {
    fn register(&self, plan: RegisterPlan) -> Result<RegisterResult> {
        fail(&self.hooks.fail_register)?;
        let mut out = self.inner.register(plan)?;
        out.timings = RegistrationTimings {
            parse_ms: self.hooks.timings.get(),
            ..Default::default()
        };
        Ok(out)
    }
    fn unregister(&self, hash: &str) -> Result<()> {
        fail(&self.hooks.fail_unregister)?;
        self.inner.unregister(hash)
    }
    fn ingest_many(&self, records: Vec<IngestRecord>) -> Result<()> {
        fail(&self.hooks.fail_ingest)?;
        self.inner.ingest_many(records)
    }
}

/// The schema most saga tests need: one domain table plus a bare child table.
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
                        kind: Some("record<user>".into()),
                        ..Default::default()
                    },
                ),
            ],
        )
        .with_table("child", Vec::<(String, ColumnSchema)>::new())
}

pub fn env() -> SagaEnv {
    SagaEnv::new(Arc::new(test_schema()))
}

/// A [`TestRig`] over hookable store and circuit adapters.
pub fn rig_with(env: SagaEnv, state: ClientState) -> (TestRig, Hooks) {
    let log = CallLog::default();
    let local = FakeLocal::new(log.clone());
    let remote = FakeRemote::new(log.clone());
    let ssp = FakeSsp::new(log.clone());
    let services = FakeServices::new(log.clone());
    let timers = FakeTimers::default();
    let hooks = Hooks::default();
    hooks.timings.set(Some(1.0));
    let now = Rc::new(Cell::new(1_700_000_000_000i64));
    let counter = Rc::new(Cell::new(0u64));
    let adapters = Adapters {
        local: Rc::new(HookedLocal {
            inner: local.clone(),
            hooks: hooks.clone(),
        }),
        remote: Rc::new(remote.clone()),
        ssp: Rc::new(HookedSsp {
            inner: ssp.clone(),
            hooks: hooks.clone(),
        }),
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
    *hooks.runtime.borrow_mut() = Some(runtime.downgrade());
    (
        TestRig {
            log,
            local,
            remote,
            ssp,
            services,
            timers,
            now,
            runtime,
        },
        hooks,
    )
}

pub fn new_rig() -> (TestRig, Hooks) {
    rig_with(env(), ClientState::empty("tab"))
}

/// A rig holding `entries`, with `extra` applied after them.
pub fn rig_of(entries: Vec<QueryEntry>, extra: impl FnOnce(&mut ClientState)) -> (TestRig, Hooks) {
    let mut state = ClientState::empty("tab");
    for e in entries {
        state.queries.insert(e.def.hash.clone(), e);
    }
    extra(&mut state);
    rig_with(env(), state)
}

pub fn life(phase: QueryPhase, remote: RemotePhase) -> QueryLifecycle {
    QueryLifecycle {
        phase,
        remote,
        fetch_depth: 0,
        background_depth: 0,
        notified: false,
    }
}

/// An entry for `hash` on `thing` in `phase` / `remote`, holding `remote_array`.
pub fn entry(
    hash: &str,
    phase: QueryPhase,
    remote: RemotePhase,
    remote_array: &[(&str, i64)],
) -> QueryEntry {
    let mut e = entry_with(hash, "thing", life(phase, remote));
    e.remote_array = pairs(remote_array);
    e
}

pub fn pairs(p: &[(&str, i64)]) -> RecordVersionArray {
    p.iter().map(|(id, v)| (id.to_string(), *v)).collect()
}

/// `[{out, version}, ...]` edges.
pub fn edges(p: &[(&str, i64)]) -> serde_json::Value {
    serde_json::Value::Array(
        p.iter()
            .map(|(id, v)| json!({"out": id, "version": v}))
            .collect(),
    )
}

pub fn ready_meta(row_count: i64) -> serde_json::Value {
    json!({"rowCount": row_count, "state": "ready"})
}

/// The three statements `single_snapshot_select` answers with.
pub fn snapshot(
    primary: &[(&str, i64)],
    meta: serde_json::Value,
    children: &[(&str, i64)],
) -> Vec<StatementResult> {
    results(vec![Ok(edges(primary)), Ok(meta), Ok(edges(children))])
}

/// The four statements `register_select` answers with.
pub fn register_answer(
    primary: &[(&str, i64)],
    meta: serde_json::Value,
    children: &[(&str, i64)],
) -> Vec<StatementResult> {
    let mut out = results(vec![Ok(serde_json::Value::Null)]);
    out.extend(snapshot(primary, meta, children));
    out
}

/// One remote request as the fake saw it.
#[derive(Clone, Debug)]
pub struct Sent {
    pub sql: String,
    pub vars: Option<Object>,
}

pub type SentLog = Rc<RefCell<Vec<Sent>>>;

/// Script the remote and record every request.
pub fn answer(
    rig: &TestRig,
    f: impl Fn(&str, Option<&Object>) -> Result<Vec<StatementResult>> + 'static,
) -> SentLog {
    let sent: SentLog = Rc::default();
    let s2 = sent.clone();
    rig.remote.answer_with(move |sql, vars| {
        s2.borrow_mut().push(Sent {
            sql: sql.to_string(),
            vars: vars.cloned(),
        });
        f(sql, vars)
    });
    sent
}

/// Record every request and answer nothing.
pub fn record(rig: &TestRig) -> SentLog {
    answer(rig, |_, _| Ok(Vec::new()))
}

/// Every outbound event, from now on.
pub fn events(rig: &TestRig) -> (Subscription, Rc<RefCell<Vec<OutEvent>>>) {
    let seen: Rc<RefCell<Vec<OutEvent>>> = Rc::default();
    let s2 = seen.clone();
    let sub = rig
        .runtime
        .on("*", move |e| s2.borrow_mut().push(e.clone()));
    (sub, seen)
}

pub fn count_kind(events: &Rc<RefCell<Vec<OutEvent>>>, kind: &str) -> usize {
    events.borrow().iter().filter(|e| e.kind() == kind).count()
}

pub fn logs(
    events: &Rc<RefCell<Vec<OutEvent>>>,
) -> Vec<(crate::kernel::events::LogLevel, String, Option<Value>)> {
    events
        .borrow()
        .iter()
        .filter_map(|e| match e {
            OutEvent::Log {
                level,
                message,
                data,
            } => Some((*level, message.clone(), data.clone())),
            _ => None,
        })
        .collect()
}

/// Mark the query set dirty-free and the store primed, as a settled client is.
pub fn primed(state: &mut ClientState) {
    r::set_identity(
        state,
        r::Identity {
            primed: Some(true),
            ..Default::default()
        },
    );
}

/// Versions as a sorted map, for comparisons.
pub fn versions(rig: &TestRig) -> BTreeMap<String, i64> {
    rig.state().versions
}
