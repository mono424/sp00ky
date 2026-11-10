//! Reactive feature flags (TS `FeatureFlagModule`).
//!
//! PORT: `packages/spooky_core/lib/src/modules/feature_flag/feature_flag.dart`.
//! Runs one shared live query over the signed-in user's `_00_user_feature`
//! rows and fans per-key snapshots out to [`FeatureFlagHandle`]s. Auth-aware:
//! a user change tears the query down, clears the snapshots and re-observes.
//!
//! Registration is spawned with `tokio::task::spawn_local`, so the module must
//! live inside the engine's `LocalSet`.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::error::Result;
use crate::modules::auth::AuthSource;
use crate::modules::query_host::QueryHost;
use crate::runtime::Subscription;
use crate::types::DEFAULT_TTL;
use crate::value::{from_value, Object, Row, Value};

/// One shared LIVE query over ALL of the signed-in user's assignments: the
/// `_00_user_feature` select permission scopes it to `user = $auth.id`, so no
/// per-key `WHERE key = $key` param is needed. A single registration means
/// every flag the user is (or becomes) assigned is observed at once: new
/// assignments stream in live, and a handle for an unassigned key simply
/// resolves to its fallback.
///
/// `_00_user_feature` is a server-provisioned meta table, absent from the app
/// schema; the circuit seeds its permission as a built-in (without it the view
/// is default-deny and empty).
pub const FEATURE_QUERY: &str = "SELECT key, variant, payload FROM _00_user_feature";

const FEATURE_TABLE: &str = "_00_user_feature";

/// Immutable view of a flag's current assignment.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FeatureFlagSnapshot {
    pub variant: Option<String>,
    pub payload: Value,
}

type SnapshotListener = Rc<dyn Fn(&FeatureFlagSnapshot)>;

struct HandleInner {
    key: String,
    fallback: Option<String>,
    latest: RefCell<FeatureFlagSnapshot>,
    listeners: RefCell<Vec<(u64, SnapshotListener)>>,
    next_id: Cell<u64>,
    closed: Cell<bool>,
    on_close: RefCell<Option<Box<dyn FnOnce()>>>,
}

/// A per-key handle (TS `FeatureFlagHandle`): the latest snapshot for one
/// flag, falling back to `fallback` while the key has no assignment. Cheap to
/// clone; clones share one handle.
#[derive(Clone)]
pub struct FeatureFlagHandle(Rc<HandleInner>);

impl FeatureFlagHandle {
    pub fn new(key: impl Into<String>, fallback: Option<String>) -> Self {
        FeatureFlagHandle(Rc::new(HandleInner {
            key: key.into(),
            fallback,
            latest: RefCell::default(),
            listeners: RefCell::default(),
            next_id: Cell::new(0),
            closed: Cell::new(false),
            on_close: RefCell::new(None),
        }))
    }

    pub fn key(&self) -> &str {
        &self.0.key
    }

    pub fn fallback(&self) -> Option<&str> {
        self.0.fallback.as_deref()
    }

    /// Push a fresh snapshot to this handle and notify its listeners.
    pub fn set(&self, snapshot: FeatureFlagSnapshot) {
        if self.0.closed.get() {
            return;
        }
        *self.0.latest.borrow_mut() = snapshot.clone();
        let listeners: Vec<SnapshotListener> = self
            .0
            .listeners
            .borrow()
            .iter()
            .map(|(_, l)| l.clone())
            .collect();
        for l in listeners {
            l(&snapshot);
        }
    }

    /// Current variant, or the fallback when unassigned.
    pub fn variant(&self) -> Option<String> {
        self.0
            .latest
            .borrow()
            .variant
            .clone()
            .or_else(|| self.0.fallback.clone())
    }

    /// Current payload, `Value::None` when unassigned.
    pub fn payload(&self) -> Value {
        self.0.latest.borrow().payload.clone()
    }

    /// The payload as your own type, `None` when unassigned.
    pub fn payload_as<T: DeserializeOwned>(&self) -> Result<Option<T>> {
        let payload = self.payload();
        if payload.is_nullish() {
            return Ok(None);
        }
        from_value(payload).map(Some)
    }

    /// True when a real variant exists and is not the `off` sentinel.
    pub fn enabled(&self) -> bool {
        self.variant().is_some_and(|v| v != "off")
    }

    /// Subscribe to snapshot changes. Fires immediately with the current value
    /// (its variant already falling back), then on every change.
    pub fn subscribe(&self, listener: impl Fn(&FeatureFlagSnapshot) + 'static) -> Subscription {
        let listener: SnapshotListener = Rc::new(listener);
        let id = self.0.next_id.get() + 1;
        self.0.next_id.set(id);
        self.0.listeners.borrow_mut().push((id, listener.clone()));
        listener(&FeatureFlagSnapshot {
            variant: self.variant(),
            payload: self.payload(),
        });
        let weak = Rc::downgrade(&self.0);
        Subscription::new(move || {
            if let Some(inner) = weak.upgrade() {
                inner.listeners.borrow_mut().retain(|(i, _)| *i != id);
            }
        })
    }

    /// Tear down this handle: drops its listeners and detaches it from the
    /// module.
    pub fn close(&self) {
        if self.0.closed.replace(true) {
            return;
        }
        self.0.listeners.borrow_mut().clear();
        let on_close = self.0.on_close.borrow_mut().take();
        if let Some(f) = on_close {
            f();
        }
    }
}

#[derive(Default)]
struct ModuleState {
    handles: Vec<FeatureFlagHandle>,
    auth_sub: Option<Subscription>,
    last_user: Option<String>,
    auth_initialized: bool,
    /// The single shared live query over the user's assignments.
    query_sub: Option<Subscription>,
    starting: bool,
    /// Bumped by every refresh and by close: a registration that resolves
    /// after either belongs to a stale session and is dropped.
    epoch: u64,
    closed: bool,
    /// The ttl the latest caller asked for (the query is shared across flags,
    /// so it is read when the registration starts).
    ttl: Option<String>,
    /// Only ASSIGNED keys; an absent key reads its fallback.
    snapshots: HashMap<String, FeatureFlagSnapshot>,
    /// The query resolved at least once, so a new handle knows to seed from
    /// it rather than wait.
    loaded: bool,
}

struct ModuleInner {
    host: Rc<dyn QueryHost>,
    auth: Rc<dyn AuthSource>,
    state: RefCell<ModuleState>,
}

/// The feature-flag module. Cheap to clone; clones share one module.
#[derive(Clone)]
pub struct FeatureFlagModule(Rc<ModuleInner>);

impl FeatureFlagModule {
    pub fn new(host: Rc<dyn QueryHost>, auth: Rc<dyn AuthSource>) -> Self {
        FeatureFlagModule(Rc::new(ModuleInner {
            host,
            auth,
            state: RefCell::default(),
        }))
    }

    /// Follow auth so the shared query tracks sign-in and sign-out.
    pub fn init(&self) {
        if std::mem::replace(&mut self.0.state.borrow_mut().auth_initialized, true) {
            return;
        }
        let weak = Rc::downgrade(&self.0);
        let sub = self.0.auth.subscribe_user(Rc::new(move |user| {
            let Some(inner) = weak.upgrade() else { return };
            let changed = {
                let mut s = inner.state.borrow_mut();
                if s.last_user.as_deref() == user {
                    false
                } else {
                    s.last_user = user.map(str::to_string);
                    true
                }
            };
            if changed {
                FeatureFlagModule(inner).refresh();
            }
        }));
        self.0.state.borrow_mut().auth_sub = Some(sub);
    }

    /// A reactive handle for `key`. A late caller is seeded at once from the
    /// already-resolved shared query, so an assigned key does not flash its
    /// fallback.
    pub fn feature(
        &self,
        key: &str,
        fallback: Option<&str>,
        ttl: Option<&str>,
    ) -> FeatureFlagHandle {
        let handle = FeatureFlagHandle::new(key, fallback.map(str::to_string));
        let weak = Rc::downgrade(&self.0);
        let h = Rc::downgrade(&handle.0);
        *handle.0.on_close.borrow_mut() = Some(Box::new(move || {
            if let Some(inner) = weak.upgrade() {
                inner
                    .state
                    .borrow_mut()
                    .handles
                    .retain(|x| !std::ptr::eq(Rc::as_ptr(&x.0), h.as_ptr()));
            }
        }));
        let seed = {
            let mut s = self.0.state.borrow_mut();
            s.handles.push(handle.clone());
            if let Some(ttl) = ttl {
                s.ttl = Some(ttl.to_string());
            }
            s.loaded
                .then(|| s.snapshots.get(key).cloned().unwrap_or_default())
        };
        if let Some(snapshot) = seed {
            handle.set(snapshot);
        }
        self.ensure_started();
        handle
    }

    /// Tear everything down (the client's close).
    pub fn close_all(&self) {
        let (auth_sub, query_sub, handles) = {
            let mut s = self.0.state.borrow_mut();
            s.closed = true;
            s.epoch += 1;
            (s.auth_sub.take(), s.query_sub.take(), s.handles.clone())
        };
        drop(auth_sub);
        drop(query_sub);
        for h in handles {
            h.close();
        }
    }

    /// Auth changed: drop the old user's query and snapshots and re-observe.
    fn refresh(&self) {
        let (query_sub, handles) = {
            let mut s = self.0.state.borrow_mut();
            s.epoch += 1;
            s.loaded = false;
            s.snapshots.clear();
            (s.query_sub.take(), s.handles.clone())
        };
        drop(query_sub);
        // Clear handles at once so a sign-out hides flag-gated UI without lag.
        for h in handles {
            h.set(FeatureFlagSnapshot::default());
        }
        self.ensure_started();
    }

    /// Start the single shared live query (idempotent; no-op with no handles).
    /// Unawaited: a handle reads its fallback until the first result lands.
    fn ensure_started(&self) {
        let (epoch, ttl) = {
            let mut s = self.0.state.borrow_mut();
            if s.query_sub.is_some() || s.starting || s.handles.is_empty() || s.closed {
                return;
            }
            s.starting = true;
            (
                s.epoch,
                s.ttl.clone().unwrap_or_else(|| DEFAULT_TTL.to_string()),
            )
        };
        let host = self.0.host.clone();
        let weak = Rc::downgrade(&self.0);
        tokio::task::spawn_local(async move {
            let result = host
                .register_query(
                    FEATURE_TABLE,
                    FEATURE_QUERY,
                    Object::new(),
                    &ttl,
                    Vec::new(),
                )
                .await;
            let Some(inner) = weak.upgrade() else { return };
            let stale = {
                let mut s = inner.state.borrow_mut();
                s.starting = false;
                s.epoch != epoch
            };
            let module = FeatureFlagModule(inner);
            match result {
                Err(e) => {
                    tracing::warn!(target: "spooky", "Failed to register feature flag query: {e}")
                }
                // Registered under a session that has since changed: start
                // again under the current one.
                Ok(_) if stale => module.ensure_started(),
                Ok(hash) => {
                    let w: Weak<ModuleInner> = Rc::downgrade(&module.0);
                    let sub = host.subscribe(
                        &hash,
                        Rc::new(move |rows: &Arc<Vec<Row>>| {
                            if let Some(inner) = w.upgrade() {
                                FeatureFlagModule(inner).apply_records(rows);
                            }
                        }),
                        true,
                    );
                    module.0.state.borrow_mut().query_sub = Some(sub);
                }
            }
        });
    }

    /// Live query result -> per-key snapshots -> every active handle.
    fn apply_records(&self, records: &[Row]) {
        let (handles, snapshots) = {
            let mut s = self.0.state.borrow_mut();
            s.snapshots.clear();
            for row in records {
                if let Some(Value::String(key)) = row.get("key") {
                    s.snapshots.insert(
                        key.clone(),
                        FeatureFlagSnapshot {
                            variant: row
                                .get("variant")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                            payload: row.get("payload").cloned().unwrap_or_default(),
                        },
                    );
                }
            }
            s.loaded = true;
            (s.handles.clone(), s.snapshots.clone())
        };
        for h in handles {
            h.set(snapshots.get(h.key()).cloned().unwrap_or_default());
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::modules::query_host::fakes::{tick, FakeAuth, FakeQueryHost};
    use crate::testing::run_local;
    use crate::value::object_from_json;

    fn flag(key: &str, variant: &str, payload: serde_json::Value) -> Row {
        object_from_json(
            json!({"id": format!("_00_user_feature:{key}"), "key": key, "variant": variant, "payload": payload}),
        )
    }

    struct Rig {
        host: Rc<FakeQueryHost>,
        auth: Rc<FakeAuth>,
        flags: FeatureFlagModule,
    }

    fn rig() -> Rig {
        let host = Rc::new(FakeQueryHost::default());
        let auth = Rc::new(FakeAuth::default());
        let flags = FeatureFlagModule::new(host.clone(), auth.clone());
        Rig { host, auth, flags }
    }

    impl Rig {
        async fn push(&self, rows: Vec<Row>) {
            tick().await;
            self.host.emit(rows);
        }
    }

    #[tokio::test]
    async fn registers_one_shared_unfiltered_query_for_many_flags() {
        run_local(async {
            let r = rig();
            r.flags.feature("a", None, Some("1h"));
            r.flags.feature("b", None, None);
            tick().await;
            let regs = r.host.registrations.borrow().clone();
            assert_eq!(regs.len(), 1);
            assert_eq!(regs[0].0, "_00_user_feature");
            assert!(!regs[0].1.contains("WHERE"));
            assert_eq!(regs[0].2, "1h");
        })
        .await;
    }

    #[tokio::test]
    async fn fans_the_result_out_by_key_with_fallbacks() {
        run_local(async {
            let r = rig();
            let beta = r.flags.feature("beta", Some("off"), None);
            let missing = r.flags.feature("missing", Some("control"), None);
            assert_eq!(beta.variant().as_deref(), Some("off"));
            assert!(!beta.enabled(), "the off sentinel is disabled");

            r.push(vec![flag("beta", "on", json!({"limit": 3}))]).await;
            assert_eq!(beta.variant().as_deref(), Some("on"));
            assert!(beta.enabled());
            #[derive(serde::Deserialize, PartialEq, Debug)]
            struct P {
                limit: i64,
            }
            assert_eq!(beta.payload_as::<P>().unwrap(), Some(P { limit: 3 }));
            assert_eq!(missing.variant().as_deref(), Some("control"));
            assert_eq!(missing.payload_as::<P>().unwrap(), None);
            assert!(FeatureFlagHandle::new("x", None).variant().is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn seeds_a_late_handle_and_streams_changes() {
        run_local(async {
            let r = rig();
            let first = r.flags.feature("beta", None, None);
            r.push(vec![flag("beta", "on", json!(null))]).await;
            let late = r.flags.feature("beta", None, None);
            assert_eq!(
                late.variant().as_deref(),
                Some("on"),
                "a late handle must not flash its fallback"
            );

            let seen: Rc<RefCell<Vec<Option<String>>>> = Rc::default();
            let s = seen.clone();
            let _sub = first.subscribe(move |snap| s.borrow_mut().push(snap.variant.clone()));
            r.push(vec![flag("beta", "v2", json!(null))]).await;
            assert_eq!(
                *seen.borrow(),
                vec![Some("on".to_string()), Some("v2".to_string())]
            );
            assert_eq!(r.host.registrations.borrow().len(), 1);

            first.close();
            r.push(vec![flag("beta", "v3", json!(null))]).await;
            assert_eq!(seen.borrow().len(), 2, "a closed handle hears nothing");
            assert_eq!(late.variant().as_deref(), Some("v3"));
        })
        .await;
    }

    #[tokio::test]
    async fn a_user_change_clears_handles_and_re_registers() {
        run_local(async {
            let r = rig();
            r.flags.init();
            let beta = r.flags.feature("beta", Some("off"), None);
            r.push(vec![flag("beta", "on", json!(null))]).await;
            assert!(beta.enabled());

            r.auth.emit(Some("user:other"));
            assert_eq!(
                beta.variant().as_deref(),
                Some("off"),
                "cleared at once on a user change"
            );
            tick().await;
            assert_eq!(r.host.registrations.borrow().len(), 2);
            assert_eq!(r.host.live_subscriptions(), 1);

            r.auth.emit(Some("user:other"));
            tick().await;
            assert_eq!(
                r.host.registrations.borrow().len(),
                2,
                "the same user is no change"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn a_refresh_during_registration_restarts_under_the_new_session() {
        run_local(async {
            let r = rig();
            r.flags.init();
            r.flags.feature("beta", None, None);
            // Before the first registration lands.
            r.auth.emit(Some("user:b"));
            tick().await;
            assert_eq!(r.host.registrations.borrow().len(), 2);
            assert_eq!(r.host.live_subscriptions(), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn close_all_releases_everything() {
        run_local(async {
            let r = rig();
            r.flags.init();
            let beta = r.flags.feature("beta", None, None);
            tick().await;
            assert_eq!(r.host.live_subscriptions(), 1);
            r.flags.close_all();
            assert_eq!(r.host.live_subscriptions(), 0);
            r.host.emit(vec![flag("beta", "on", json!(null))]);
            assert!(beta.variant().is_none());
        })
        .await;
    }
}
