//! The one effectful object of the engine: holds the state, runs sagas on
//! lanes, fires timers, fans events out to subscribers and schedules
//! materialization for dirty queries. No decision logic lives here.
//!
//! Single-threaded by design (like the Dart isolate the Dart core runs in):
//! everything here is `Rc`/`RefCell` and must run inside a tokio `LocalSet`.
//! The thread-safe public client (`crate::Sp00kyClient`) owns one runtime on a
//! dedicated thread and talks to it over channels.

pub mod router;
pub mod services;
pub mod timers;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use futures::future::{FutureExt, LocalBoxFuture, Shared};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::kernel::events::{LogLevel, OutEvent, RuntimeEvent};
use crate::kernel::lanes::Lane;
use crate::kernel::ports::Adapters;
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;
use crate::state::lifecycle::{derive_status, is_authoritative};
use crate::state::selectors::{fetching_query_count, pending_mutation_count, unsynced_record_ids};
use crate::state::{reducers as r, ClientState};
use crate::types::{QueryHash, QueryStatus, SyncHealth};
use crate::value::Row;

pub use timers::TokioTimers;

type DedupeFuture = Shared<LocalBoxFuture<'static, Result<()>>>;

/// A dedupe lane's running saga and the account (store epoch) it runs for.
struct DedupeRun {
    fence: Option<u64>,
    future: DedupeFuture,
}

/// Called with a query's rows on every materialization that changed them.
pub type RecordsCallback = Rc<dyn Fn(&Arc<Vec<Row>>)>;
pub type StatusCallback = Rc<dyn Fn(QueryStatus)>;
pub type AuthorityCallback = Rc<dyn Fn(bool)>;
pub type EventCallback = Rc<dyn Fn(&OutEvent)>;

struct Waiter {
    until: Box<dyn Fn(&ClientState) -> bool>,
    done: oneshot::Sender<Result<()>>,
}

#[derive(Default)]
struct Subscribers {
    next_id: u64,
    records: HashMap<QueryHash, Vec<(u64, RecordsCallback)>>,
    status: HashMap<QueryHash, Vec<(u64, StatusCallback)>>,
    authority: HashMap<QueryHash, Vec<(u64, AuthorityCallback)>>,
    /// By event kind; `*` for all.
    listeners: HashMap<&'static str, Vec<(u64, EventCallback)>>,
}

impl Subscribers {
    fn next(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

struct LastNotified {
    status: HashMap<QueryHash, QueryStatus>,
    activity: (usize, usize),
    unsynced: BTreeSet<String>,
    health: SyncHealth,
}

struct Inner {
    env: SagaEnv,
    adapters: Adapters,
    state: RefCell<ClientState>,
    serial: RefCell<HashMap<String, Rc<tokio::sync::Mutex<()>>>>,
    dedupe: RefCell<HashMap<String, DedupeRun>>,
    waiters: RefCell<Vec<Waiter>>,
    subs: RefCell<Subscribers>,
    scheduled_materialize: RefCell<BTreeSet<QueryHash>>,
    materialized_at: RefCell<HashMap<QueryHash, i64>>,
    last: RefCell<LastNotified>,
    disposed: Cell<bool>,
}

/// Unsubscribes when dropped (or on [`Subscription::cancel`]).
#[must_use = "dropping a Subscription unsubscribes"]
pub struct Subscription(Option<Box<dyn FnOnce()>>);

impl Subscription {
    /// A subscription that runs `f` once, when cancelled or dropped. Public so
    /// a seam implementor (a `QueryHost`, an auth source) can hand out the
    /// same guard the runtime does.
    pub fn new(f: impl FnOnce() + 'static) -> Self {
        Subscription(Some(Box::new(f)))
    }

    /// A subscription that does nothing.
    pub fn noop() -> Self {
        Subscription(None)
    }

    pub fn cancel(mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }

    /// Keep the subscription for the runtime's lifetime.
    pub fn forget(mut self) {
        self.0 = None;
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

/// The saga runtime. Cheap to clone; all clones share one state.
#[derive(Clone)]
pub struct Runtime(Rc<Inner>);

/// A weak handle, for timers and callbacks that must not keep the runtime
/// alive.
#[derive(Clone)]
pub struct WeakRuntime(Weak<Inner>);

impl WeakRuntime {
    pub fn upgrade(&self) -> Option<Runtime> {
        self.0.upgrade().map(Runtime)
    }
}

fn remove_sub<T>(map: &mut HashMap<QueryHash, Vec<(u64, T)>>, hash: &str, id: u64) -> (bool, bool) {
    let Some(list) = map.get_mut(hash) else {
        return (false, false);
    };
    let before = list.len();
    list.retain(|(i, _)| *i != id);
    let removed = list.len() != before;
    let empty = list.is_empty();
    if empty {
        map.remove(hash);
    }
    (removed, empty)
}

impl Runtime {
    pub fn new(env: SagaEnv, adapters: Adapters, initial: ClientState) -> Self {
        let health = initial.sync.health.clone();
        Runtime(Rc::new(Inner {
            env,
            adapters,
            state: RefCell::new(initial),
            serial: RefCell::default(),
            dedupe: RefCell::default(),
            waiters: RefCell::default(),
            subs: RefCell::default(),
            scheduled_materialize: RefCell::default(),
            materialized_at: RefCell::default(),
            last: RefCell::new(LastNotified {
                status: HashMap::new(),
                activity: (0, 0),
                unsynced: BTreeSet::new(),
                health,
            }),
            disposed: Cell::new(false),
        }))
    }

    pub fn downgrade(&self) -> WeakRuntime {
        WeakRuntime(Rc::downgrade(&self.0))
    }

    pub fn env(&self) -> &SagaEnv {
        &self.0.env
    }

    pub fn adapters(&self) -> &Adapters {
        &self.0.adapters
    }

    pub fn is_disposed(&self) -> bool {
        self.0.disposed.get()
    }

    /// A context for code outside a lane (facade conveniences, tests).
    pub fn ctx(&self) -> Ctx {
        Ctx::new(self.clone(), None)
    }

    // ---- state ---------------------------------------------------------------

    pub fn read<R>(&self, select: impl FnOnce(&ClientState) -> R) -> R {
        select(&self.0.state.borrow())
    }

    /// A copy of the whole state (tests and diagnostics).
    pub fn snapshot(&self) -> ClientState {
        self.0.state.borrow().clone()
    }

    /// Apply reducers, then notify waiters, subscribers and the materializer.
    pub fn update<R>(&self, f: impl FnOnce(&mut ClientState) -> R) -> R {
        let out = f(&mut self.0.state.borrow_mut());
        self.after_update();
        out
    }

    /// Resolves once `until` holds (checked now and after every update).
    pub fn wait_for(&self, until: Box<dyn Fn(&ClientState) -> bool>) -> impl Future<Output = Result<()>> + 'static {
        let ready = until(&self.0.state.borrow());
        let rx = if ready || self.is_disposed() {
            None
        } else {
            let (tx, rx) = oneshot::channel();
            self.0.waiters.borrow_mut().push(Waiter { until, done: tx });
            Some(rx)
        };
        let disposed = self.is_disposed() && !ready;
        async move {
            if disposed {
                return Err(Error::Closed);
            }
            match rx {
                None => Ok(()),
                Some(rx) => rx.await.unwrap_or(Err(Error::Closed)),
            }
        }
    }

    fn after_update(&self) {
        let inner = &self.0;
        // Waiters.
        let ready: Vec<Waiter> = {
            let s = inner.state.borrow();
            let mut waiters = inner.waiters.borrow_mut();
            let mut ready = Vec::new();
            let mut i = 0;
            while i < waiters.len() {
                if (waiters[i].until)(&s) {
                    ready.push(waiters.swap_remove(i));
                } else {
                    i += 1;
                }
            }
            ready
        };
        for w in ready {
            let _ = w.done.send(Ok(()));
        }

        // Materialization: throttle with a leading edge. The first change after
        // a quiet spell materializes on the next tick (a zero delay still folds
        // every update of the current turn into it); later ones inside the
        // window fold into one run at its end.
        let to_schedule: Vec<QueryHash> = {
            let s = inner.state.borrow();
            let scheduled = inner.scheduled_materialize.borrow();
            s.dirty.iter().filter(|h| !scheduled.contains(*h)).cloned().collect()
        };
        if !to_schedule.is_empty() {
            let now = (inner.adapters.now)();
            for hash in to_schedule {
                inner.scheduled_materialize.borrow_mut().insert(hash.clone());
                let last = inner.materialized_at.borrow().get(&hash).copied();
                let wait = last
                    .map(|l| l + inner.env.materialize_debounce_ms as i64 - now)
                    .unwrap_or(0)
                    .max(0) as u64;
                let weak = self.downgrade();
                let h = hash.clone();
                inner.adapters.timers.set(
                    &format!("mat:{hash}"),
                    wait,
                    Box::new(move || {
                        let Some(rt) = weak.upgrade() else { return };
                        rt.0.scheduled_materialize.borrow_mut().remove(&h);
                        let at = (rt.0.adapters.now)();
                        rt.0.materialized_at.borrow_mut().insert(h.clone(), at);
                        rt.dispatch(RuntimeEvent::Materialize { hash: h });
                    }),
                );
            }
        }

        // Derived notifications.
        let mut calls: Vec<(StatusCallback, QueryStatus)> = Vec::new();
        let mut events: Vec<OutEvent> = Vec::new();
        {
            let s = inner.state.borrow();
            let subs = inner.subs.borrow();
            let mut last = inner.last.borrow_mut();
            for (hash, list) in &subs.status {
                let Some(q) = s.queries.get(hash) else { continue };
                let status = derive_status(&q.lifecycle);
                if last.status.get(hash) == Some(&status) {
                    continue;
                }
                last.status.insert(hash.clone(), status);
                calls.extend(list.iter().map(|(_, cb)| (cb.clone(), status)));
                events.push(OutEvent::QueryStatus {
                    hash: hash.clone(),
                    status,
                });
            }
            let activity = (fetching_query_count(&s), pending_mutation_count(&s));
            if activity != last.activity {
                last.activity = activity;
                events.push(OutEvent::ActivityChanged {
                    fetching: activity.0,
                    pending: activity.1,
                });
            }
            let unsynced = unsynced_record_ids(&s);
            if unsynced != last.unsynced {
                last.unsynced = unsynced.clone();
                events.push(OutEvent::UnsyncedChanged { record_ids: unsynced });
            }
            // Health is notified here, not from the saga that folds a sync
            // round: `connection` moves on transport events with no round.
            if s.sync.health != last.health {
                last.health = s.sync.health.clone();
                events.push(OutEvent::HealthChanged {
                    health: s.sync.health.clone(),
                });
            }
        }
        for (cb, status) in calls {
            cb(status);
        }
        for e in events {
            self.notify(&e);
        }
    }

    // ---- events ----------------------------------------------------------------

    /// Deliver an outbound event to its subscribers and listeners.
    pub fn emit(&self, event: OutEvent) {
        match &event {
            OutEvent::QueryRecords { hash, records } => {
                let cbs: Vec<RecordsCallback> = self
                    .0
                    .subs
                    .borrow()
                    .records
                    .get(hash)
                    .map(|l| l.iter().map(|(_, cb)| cb.clone()).collect())
                    .unwrap_or_default();
                for cb in cbs {
                    cb(records);
                }
            }
            OutEvent::QueryAuthority { hash, known } => {
                let cbs: Vec<AuthorityCallback> = self
                    .0
                    .subs
                    .borrow()
                    .authority
                    .get(hash)
                    .map(|l| l.iter().map(|(_, cb)| cb.clone()).collect())
                    .unwrap_or_default();
                for cb in cbs {
                    cb(*known);
                }
            }
            OutEvent::Log { level, message, data } => {
                let data = data.as_ref().map(|d| d.to_json_string()).unwrap_or_default();
                match level {
                    LogLevel::Debug => tracing::debug!(target: "spooky", "{message} {data}"),
                    LogLevel::Info => tracing::info!(target: "spooky", "{message} {data}"),
                    LogLevel::Warn => tracing::warn!(target: "spooky", "{message} {data}"),
                    LogLevel::Error => tracing::error!(target: "spooky", "{message} {data}"),
                }
            }
            _ => {}
        }
        self.notify(&event);
    }

    fn notify(&self, event: &OutEvent) {
        let cbs: Vec<EventCallback> = {
            let subs = self.0.subs.borrow();
            subs.listeners
                .get(event.kind())
                .into_iter()
                .chain(subs.listeners.get("*"))
                .flat_map(|l| l.iter().map(|(_, cb)| cb.clone()))
                .collect()
        };
        for cb in cbs {
            cb(event);
        }
    }

    /// Observe outbound events by kind (`"*"` for all; see [`OutEvent::kind`]).
    pub fn on(&self, kind: &'static str, cb: impl Fn(&OutEvent) + 'static) -> Subscription {
        let id = {
            let mut subs = self.0.subs.borrow_mut();
            let id = subs.next();
            subs.listeners.entry(kind).or_default().push((id, Rc::new(cb)));
            id
        };
        let weak = self.downgrade();
        Subscription::new(move || {
            if let Some(rt) = weak.upgrade() {
                let mut subs = rt.0.subs.borrow_mut();
                if let Some(l) = subs.listeners.get_mut(kind) {
                    l.retain(|(i, _)| *i != id);
                }
            }
        })
    }

    /// Subscribe to a query's rows. Counts as a subscriber (keeps the query
    /// from being evicted) until dropped. `immediate` replays the current rows.
    pub fn subscribe(&self, hash: &str, cb: impl Fn(&Arc<Vec<Row>>) + 'static, immediate: bool) -> Subscription {
        let cb: RecordsCallback = Rc::new(cb);
        let id = {
            let mut subs = self.0.subs.borrow_mut();
            let id = subs.next();
            subs.records.entry(hash.to_string()).or_default().push((id, cb.clone()));
            id
        };
        self.update(|s| r::subscribe_query(s, hash));
        if immediate {
            let records = self.read(|s| s.queries.get(hash).map(|e| e.records.clone()));
            if let Some(records) = records {
                cb(&records);
            }
        }
        let weak = self.downgrade();
        let hash = hash.to_string();
        Subscription::new(move || {
            let Some(rt) = weak.upgrade() else { return };
            let (removed, _) = remove_sub(&mut rt.0.subs.borrow_mut().records, &hash, id);
            if removed && !rt.is_disposed() {
                let now = (rt.0.adapters.now)();
                rt.update(|s| r::unsubscribe_query(s, &hash, now));
            }
        })
    }

    /// A query's fetch status, on every change.
    pub fn subscribe_status(&self, hash: &str, cb: impl Fn(QueryStatus) + 'static, immediate: bool) -> Subscription {
        let cb: StatusCallback = Rc::new(cb);
        let id = {
            let mut subs = self.0.subs.borrow_mut();
            let id = subs.next();
            subs.status.entry(hash.to_string()).or_default().push((id, cb.clone()));
            id
        };
        let status = self.read(|s| s.queries.get(hash).map(|e| derive_status(&e.lifecycle)));
        if let Some(status) = status {
            self.0.last.borrow_mut().status.insert(hash.to_string(), status);
            if immediate {
                cb(status);
            }
        }
        let weak = self.downgrade();
        let hash = hash.to_string();
        Subscription::new(move || {
            let Some(rt) = weak.upgrade() else { return };
            let (_, empty) = remove_sub(&mut rt.0.subs.borrow_mut().status, &hash, id);
            if empty {
                rt.0.last.borrow_mut().status.remove(&hash);
            }
        })
    }

    /// A query's authority flips (true once server membership is known).
    pub fn subscribe_authority(&self, hash: &str, cb: impl Fn(bool) + 'static, immediate: bool) -> Subscription {
        let cb: AuthorityCallback = Rc::new(cb);
        let id = {
            let mut subs = self.0.subs.borrow_mut();
            let id = subs.next();
            subs.authority.entry(hash.to_string()).or_default().push((id, cb.clone()));
            id
        };
        if immediate {
            if let Some(known) = self.read(|s| s.queries.get(hash).map(|e| is_authoritative(&e.lifecycle))) {
                cb(known);
            }
        }
        let weak = self.downgrade();
        let hash = hash.to_string();
        Subscription::new(move || {
            if let Some(rt) = weak.upgrade() {
                remove_sub(&mut rt.0.subs.borrow_mut().authority, &hash, id);
            }
        })
    }

    // ---- sagas -----------------------------------------------------------------

    fn fence_for(&self, allows_account_change: bool) -> Option<u64> {
        (!allows_account_change).then(|| self.0.adapters.local.epoch())
    }

    /// Run a saga with no lane. Errors propagate to the caller.
    pub async fn run<R, F, Fut>(&self, saga: F) -> Result<R>
    where
        F: FnOnce(Ctx) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        self.run_on(None, false, saga).await
    }

    /// Run a saga on a serial lane: one at a time per key, in arrival order.
    pub async fn run_serial<R, F, Fut>(&self, key: &str, saga: F) -> Result<R>
    where
        F: FnOnce(Ctx) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        self.run_on(Some(key), false, saga).await
    }

    /// Run a saga that may cross an account change (boot, sign-in flips).
    pub async fn run_unfenced<R, F, Fut>(&self, serial: Option<&str>, saga: F) -> Result<R>
    where
        F: FnOnce(Ctx) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        self.run_on(serial, true, saga).await
    }

    async fn run_on<R, F, Fut>(&self, serial: Option<&str>, allows_account_change: bool, saga: F) -> Result<R>
    where
        F: FnOnce(Ctx) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        // The fence is the epoch at the call, before waiting on the lane: a
        // write queued behind a bucket switch must not land in the new bucket.
        let ctx = Ctx::new(self.clone(), self.fence_for(allows_account_change));
        let Some(key) = serial else {
            ctx.check()?;
            return saga(ctx).await;
        };
        let lock = self
            .0
            .serial
            .borrow_mut()
            .entry(key.to_string())
            .or_insert_with(|| Rc::new(tokio::sync::Mutex::new(())))
            .clone();
        let out = {
            let _guard = lock.lock().await;
            match ctx.check() {
                Ok(()) => saga(ctx).await,
                Err(e) => Err(e),
            }
        };
        // Drop the lane once nobody holds or waits on it.
        if Rc::strong_count(&lock) == 2 {
            let mut lanes = self.0.serial.borrow_mut();
            if lanes.get(key).is_some_and(|l| Rc::ptr_eq(l, &lock)) {
                lanes.remove(key);
            }
        }
        out
    }

    async fn run_dedupe(&self, key: String, allows_account_change: bool, saga: router::SagaFn) -> Result<()> {
        let fence = self.fence_for(allows_account_change);
        // Join only a run for the same account. A run started before a bucket
        // switch is about to fail with `AccountChanged`; joining it would
        // swallow the switch's own dispatch (the post-sign-in `PollTick`), and
        // a lane that only re-arms itself from its tick would stay dead for
        // the session.
        let running = self
            .0
            .dedupe
            .borrow()
            .get(&key)
            .filter(|r| r.fence == fence)
            .map(|r| r.future.clone());
        if let Some(running) = running {
            return running.await;
        }
        let ctx = Ctx::new(self.clone(), fence);
        let fut: LocalBoxFuture<'static, Result<()>> = async move {
            ctx.check()?;
            saga(ctx).await
        }
        .boxed_local();
        let shared = fut.shared();
        self.0.dedupe.borrow_mut().insert(
            key.clone(),
            DedupeRun {
                fence,
                future: shared.clone(),
            },
        );
        let out = shared.clone().await;
        let mut lanes = self.0.dedupe.borrow_mut();
        if lanes.get(&key).is_some_and(|r| r.future.ptr_eq(&shared)) {
            lanes.remove(&key);
        }
        out
    }

    /// Route an event to its saga in the background. Never fails: errors are
    /// logged.
    pub fn dispatch(&self, event: RuntimeEvent) {
        if self.is_disposed() {
            return;
        }
        let rt = self.clone();
        tokio::task::spawn_local(async move {
            rt.dispatch_async(event).await;
        });
    }

    /// Route an event to its saga and wait for it.
    pub async fn dispatch_async(&self, event: RuntimeEvent) {
        if self.is_disposed() {
            return;
        }
        let kind = event.kind();
        let allows = event.allows_account_change();
        tracing::trace!(target: "spooky::saga", "start {kind}");
        let route = router::route(&self.0.env, event);
        let out = match route.lane {
            None => self.run_on(None, allows, route.saga).await,
            Some(Lane::Serial(key)) => self.run_on(Some(&key), allows, route.saga).await,
            Some(Lane::Dedupe(key)) => self.run_dedupe(key, allows, route.saga).await,
        };
        tracing::trace!(target: "spooky::saga", "end {kind}: {:?}", out.as_ref().err());
        match out {
            Ok(()) => {}
            Err(Error::AccountChanged | Error::Closed) => {
                tracing::debug!(target: "spooky", "saga for {kind} fenced off");
            }
            Err(e) => tracing::warn!(target: "spooky", "saga failed for {kind}: {e}"),
        }
    }

    /// Stop everything: timers cleared, waiters failed, dispatch disabled.
    pub fn dispose(&self) {
        if self.0.disposed.replace(true) {
            return;
        }
        self.0.adapters.timers.clear_all();
        self.0.scheduled_materialize.borrow_mut().clear();
        for w in self.0.waiters.borrow_mut().drain(..) {
            let _ = w.done.send(Err(Error::Closed));
        }
    }
}

#[cfg(test)]
mod tests;
