//! Reactive app releases (TS `AppReleaseModule`).
//!
//! PORT: `packages/spooky_core/lib/src/modules/app_release/app_release.dart`.
//! Runs one shared live query over `_00_app_release` and fans per-app
//! snapshots out to [`AppReleaseHandle`]s, so an app can prompt (or force) an
//! update when the deployed version moves past the running build. Mirrors
//! [`crate::modules::feature_flag`].
//!
//! Registration is spawned with `tokio::task::spawn_local`, so the module must
//! live inside the engine's `LocalSet`.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use crate::modules::auth::AuthSource;
use crate::modules::query_host::QueryHost;
use crate::runtime::Subscription;
use crate::types::DEFAULT_TTL;
use crate::utils::semver_gt;
use crate::value::{Object, Row, Value};

/// One shared LIVE query over every app's release row (TS `RELEASE_QUERY`).
///
/// `_00_app_release` is world-readable (root-only writes), one row per app
/// keyed by name, written by `spky deploy` / `spky release` / the git-linked
/// builder. A single registration observes every app at once; a handle for an
/// app with no row simply reports no update.
///
/// Like `_00_user_feature`, this is a server-provisioned meta table absent from
/// the app schema, so the circuit seeds its permission as a built-in (without
/// it the view is default-deny and stays empty).
pub const RELEASE_QUERY: &str = "SELECT * FROM _00_app_release";

const RELEASE_TABLE: &str = "_00_app_release";

/// Immutable view of an app's latest announced release.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AppReleaseSnapshot {
    /// Latest announced version, `None` when no row exists for the app.
    pub version: Option<String>,
    /// Clients should clear caches when reloading onto this version.
    pub cache_bust: bool,
    /// Clients should update immediately instead of asking.
    pub mandatory: bool,
    pub released_at: Option<String>,
}

type SnapshotListener = Rc<dyn Fn(&AppReleaseSnapshot)>;

struct HandleInner {
    app: String,
    latest: RefCell<AppReleaseSnapshot>,
    listeners: RefCell<Vec<(u64, SnapshotListener)>>,
    next_id: Cell<u64>,
    closed: Cell<bool>,
    on_close: RefCell<Option<Box<dyn FnOnce()>>>,
}

/// A per-app handle (TS `AppReleaseHandle`). Cheap to clone; clones share one
/// handle.
#[derive(Clone)]
pub struct AppReleaseHandle(Rc<HandleInner>);

impl AppReleaseHandle {
    pub fn new(app: impl Into<String>) -> Self {
        AppReleaseHandle(Rc::new(HandleInner {
            app: app.into(),
            latest: RefCell::default(),
            listeners: RefCell::default(),
            next_id: Cell::new(0),
            closed: Cell::new(false),
            on_close: RefCell::new(None),
        }))
    }

    pub fn app(&self) -> &str {
        &self.0.app
    }

    /// Push a fresh snapshot to this handle and notify its listeners.
    pub fn set(&self, snapshot: AppReleaseSnapshot) {
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

    pub fn snapshot(&self) -> AppReleaseSnapshot {
        self.0.latest.borrow().clone()
    }

    /// Latest announced version, `None` when none has been announced.
    pub fn version(&self) -> Option<String> {
        self.0.latest.borrow().version.clone()
    }

    pub fn cache_bust(&self) -> bool {
        self.0.latest.borrow().cache_bust
    }

    pub fn mandatory(&self) -> bool {
        self.0.latest.borrow().mandatory
    }

    /// True when the announced version is semver-newer than
    /// `current_version`. A malformed version on either side is never newer,
    /// so a bad release row cannot nag every client.
    pub fn update_available(&self, current_version: &str) -> bool {
        self.version()
            .is_some_and(|v| semver_gt(&v, current_version))
    }

    /// Subscribe to snapshot changes. Fires immediately with the current value,
    /// then on every change.
    pub fn subscribe(&self, listener: impl Fn(&AppReleaseSnapshot) + 'static) -> Subscription {
        let listener: SnapshotListener = Rc::new(listener);
        let id = self.0.next_id.get() + 1;
        self.0.next_id.set(id);
        self.0.listeners.borrow_mut().push((id, listener.clone()));
        listener(&self.snapshot());
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
    handles: Vec<AppReleaseHandle>,
    auth_sub: Option<Subscription>,
    last_user: Option<String>,
    auth_initialized: bool,
    query_sub: Option<Subscription>,
    starting: bool,
    /// Bumped by every refresh and by close: a registration that resolves
    /// after either is dropped.
    epoch: u64,
    closed: bool,
    ttl: Option<String>,
    snapshots: HashMap<String, AppReleaseSnapshot>,
    loaded: bool,
}

struct ModuleInner {
    host: Rc<dyn QueryHost>,
    auth: Rc<dyn AuthSource>,
    state: RefCell<ModuleState>,
}

/// The app-release module. Cheap to clone; clones share one module.
#[derive(Clone)]
pub struct AppReleaseModule(Rc<ModuleInner>);

impl AppReleaseModule {
    pub fn new(host: Rc<dyn QueryHost>, auth: Rc<dyn AuthSource>) -> Self {
        AppReleaseModule(Rc::new(ModuleInner {
            host,
            auth,
            state: RefCell::default(),
        }))
    }

    /// Follow auth so the shared query tracks sign-in and sign-out. The table
    /// is world-readable, so the data is the same for every user: this is
    /// plumbing hygiene, a new session invalidates the old circuit plan.
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
                AppReleaseModule(inner).refresh();
            }
        }));
        self.0.state.borrow_mut().auth_sub = Some(sub);
    }

    /// A reactive handle for `app`. A late caller is seeded at once from the
    /// already-resolved shared query, so a known release does not flash empty.
    pub fn release(&self, app: &str, ttl: Option<&str>) -> AppReleaseHandle {
        let handle = AppReleaseHandle::new(app);
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
                .then(|| s.snapshots.get(app).cloned().unwrap_or_default())
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

    /// Auth changed: drop the old query and snapshots and re-observe.
    fn refresh(&self) {
        let query_sub = {
            let mut s = self.0.state.borrow_mut();
            s.epoch += 1;
            s.loaded = false;
            s.snapshots.clear();
            s.query_sub.take()
        };
        drop(query_sub);
        self.ensure_started();
    }

    /// Start the single shared live query (idempotent; no-op with no handles).
    /// Unawaited: a handle reports no update until the first result lands.
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
                    RELEASE_TABLE,
                    RELEASE_QUERY,
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
            let module = AppReleaseModule(inner);
            match result {
                Err(e) => {
                    tracing::warn!(target: "spooky", "Failed to register app release query: {e}")
                }
                Ok(_) if stale => module.ensure_started(),
                Ok(hash) => {
                    let w: Weak<ModuleInner> = Rc::downgrade(&module.0);
                    let sub = host.subscribe(
                        &hash,
                        Rc::new(move |rows: &Arc<Vec<Row>>| {
                            if let Some(inner) = w.upgrade() {
                                AppReleaseModule(inner).apply_records(rows);
                            }
                        }),
                        true,
                    );
                    module.0.state.borrow_mut().query_sub = Some(sub);
                }
            }
        });
    }

    /// Live query result -> per-app snapshots -> every active handle.
    fn apply_records(&self, records: &[Row]) {
        let (handles, snapshots) = {
            let mut s = self.0.state.borrow_mut();
            s.snapshots.clear();
            for row in records {
                if let (Some(Value::String(app)), Some(Value::String(version))) =
                    (row.get("app"), row.get("version"))
                {
                    s.snapshots.insert(
                        app.clone(),
                        AppReleaseSnapshot {
                            version: Some(version.clone()),
                            cache_bust: row.get("cache_bust") == Some(&Value::Bool(true)),
                            mandatory: row.get("mandatory") == Some(&Value::Bool(true)),
                            released_at: row
                                .get("released_at")
                                .filter(|v| !v.is_nullish())
                                .map(Value::to_id_string),
                        },
                    );
                }
            }
            s.loaded = true;
            (s.handles.clone(), s.snapshots.clone())
        };
        for h in handles {
            h.set(snapshots.get(h.app()).cloned().unwrap_or_default());
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

    fn release_row(
        app: &str,
        version: &str,
        cache_bust: Option<bool>,
        mandatory: Option<bool>,
    ) -> Row {
        object_from_json(json!({
            "id": format!("_00_app_release:{app}"),
            "app": app,
            "version": version,
            "cache_bust": cache_bust,
            "mandatory": mandatory,
            "released_at": null,
        }))
    }

    struct Rig {
        host: Rc<FakeQueryHost>,
        auth: Rc<FakeAuth>,
        releases: AppReleaseModule,
    }

    fn rig() -> Rig {
        let host = Rc::new(FakeQueryHost::default());
        let auth = Rc::new(FakeAuth::default());
        let releases = AppReleaseModule::new(host.clone(), auth.clone());
        Rig {
            host,
            auth,
            releases,
        }
    }

    impl Rig {
        /// Deliver release rows the way the shared query would.
        async fn push(&self, rows: Vec<Row>) {
            tick().await;
            self.host.emit(rows);
        }
    }

    #[tokio::test]
    async fn registers_one_shared_unfiltered_query_for_many_apps() {
        run_local(async {
            let r = rig();
            r.releases.release("web", None);
            r.releases.release("admin", None);
            tick().await;
            let regs = r.host.registrations.borrow().clone();
            assert_eq!(regs.len(), 1);
            assert!(regs[0].1.contains("FROM _00_app_release"));
            assert!(!regs[0].1.contains("WHERE"));
        })
        .await;
    }

    #[tokio::test]
    async fn fans_the_shared_result_out_to_each_handle_by_app() {
        run_local(async {
            let r = rig();
            let web = r.releases.release("web", None);
            let missing = r.releases.release("missing", None);
            r.push(vec![release_row("web", "1.2.0", Some(true), None)])
                .await;
            assert_eq!(web.version().as_deref(), Some("1.2.0"));
            assert!(web.cache_bust());
            assert!(!web.mandatory(), "a null flag reads as false");
            assert!(missing.version().is_none());
            assert!(!missing.update_available("1.0.0"));
        })
        .await;
    }

    #[tokio::test]
    async fn update_available_compares_semver_against_the_running_build() {
        run_local(async {
            let r = rig();
            let web = r.releases.release("web", None);
            r.push(vec![release_row("web", "1.2.0", None, None)]).await;
            assert!(web.update_available("1.1.9"));
            assert!(!web.update_available("1.2.0"));
            assert!(!web.update_available("1.3.0"));
            assert!(!web.update_available("garbage"));
        })
        .await;
    }

    #[tokio::test]
    async fn observes_row_updates_live_without_re_registering() {
        run_local(async {
            let r = rig();
            let web = r.releases.release("web", None);
            r.push(vec![release_row("web", "1.0.0", None, None)]).await;
            assert!(!web.update_available("1.0.0"));
            r.push(vec![release_row("web", "1.0.1", None, Some(true))])
                .await;
            assert!(web.update_available("1.0.0"));
            assert!(web.mandatory());
            assert_eq!(r.host.registrations.borrow().len(), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn seeds_a_late_handle_from_the_already_loaded_snapshot() {
        run_local(async {
            let r = rig();
            r.releases.release("web", None);
            r.push(vec![release_row("web", "2.0.0", None, None)]).await;
            let late = r.releases.release("web", None);
            assert_eq!(
                late.version().as_deref(),
                Some("2.0.0"),
                "a late handle must not flash empty"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn subscribe_fires_immediately_and_on_each_change() {
        run_local(async {
            let r = rig();
            let web = r.releases.release("web", None);
            let seen: Rc<RefCell<Vec<Option<String>>>> = Rc::default();
            let s = seen.clone();
            let _sub = web.subscribe(move |snap| s.borrow_mut().push(snap.version.clone()));
            assert_eq!(
                *seen.borrow(),
                vec![None],
                "immediate fire with the empty snapshot"
            );
            r.push(vec![release_row("web", "1.0.0", None, None)]).await;
            assert_eq!(seen.borrow().last().unwrap().as_deref(), Some("1.0.0"));
        })
        .await;
    }

    #[tokio::test]
    async fn a_closed_handle_stops_receiving_snapshots() {
        run_local(async {
            let r = rig();
            let web = r.releases.release("web", None);
            let seen: Rc<RefCell<Vec<Option<String>>>> = Rc::default();
            let s = seen.clone();
            let _sub = web.subscribe(move |snap| s.borrow_mut().push(snap.version.clone()));
            web.close();
            r.push(vec![release_row("web", "9.9.9", None, None)]).await;
            assert_eq!(*seen.borrow(), vec![None]);
        })
        .await;
    }

    #[tokio::test]
    async fn re_registers_on_a_user_change() {
        run_local(async {
            let r = rig();
            r.releases.init();
            let web = r.releases.release("web", None);
            r.push(vec![release_row("web", "1.0.0", None, None)]).await;
            assert_eq!(web.version().as_deref(), Some("1.0.0"));

            r.auth.emit(Some("user:other"));
            tick().await;
            // Re-observed under the new session: one registration per
            // observation, and the previous subscription released.
            assert_eq!(r.host.registrations.borrow().len(), 2);
            assert_eq!(r.host.live_subscriptions(), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn rows_without_an_app_or_version_are_ignored() {
        run_local(async {
            let r = rig();
            let web = r.releases.release("web", None);
            r.push(vec![object_from_json(
                json!({"id": "_00_app_release:x", "app": "web"}),
            )])
            .await;
            assert!(web.version().is_none());
        })
        .await;
    }
}
