//! [`Engine`]: the in-process client (Dart `InProcessSp00kyClient`).
//!
//! A thin facade over the saga [`Runtime`]: every method runs one saga, reads a
//! selector, or attaches a subscriber. Single-threaded: it must live on a tokio
//! `LocalSet`. The thread-safe [`crate::Sp00kyClient`] runs one on a dedicated
//! thread; use this type directly only when you own the scheduling (tests,
//! embedding into an existing `LocalSet`).

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::FutureExt;

use crate::boot::{auth_flip, boot as boot_saga, preload as preload_saga};
use crate::client::wiring::EngineHost;
#[cfg(feature = "remote")]
use crate::client::wiring::PersistenceBridge;
use crate::modules::app_release::{AppReleaseHandle, AppReleaseModule};
use crate::modules::auth::{AuthService, AuthSource};
use crate::modules::blobs::{BlobCache, BlobStore, FileBlobStore, MemoryBlobStore};
use crate::modules::bucket::{BucketHandle, RemoteQuery};
use crate::modules::feature_flag::{FeatureFlagHandle, FeatureFlagModule};
use crate::modules::query_builder::QueryBuilder;
use crate::modules::query_host::QueryHost;
use crate::modules::ref_tables::bucket_id_for_user;
use crate::error::{Error, Result};
use crate::kernel::events::{OutEvent, RuntimeEvent};
use crate::kernel::ports::{Adapters, IdScope, LiveCallback, RemotePort, StatementResult};
use crate::modules::query_builder::RelationPlan;
use crate::modules::ref_tables::{RefMode, ANON_USER_ID};
use crate::mutation::rows::FailedMutationRow;
use crate::mutation::{jobs, mutation_id, tray, write as write_saga};
use crate::query::env::SagaEnv;
use crate::query::lifecycle::evict_query;
use crate::query::register::{register_local, RegisterInput};
use crate::runtime::services::{AuthSession, ClientServices};
use crate::runtime::{Runtime, Subscription, TokioTimers};
use crate::services::column_reset::column_fingerprints;
use crate::services::local::{LocalStoreAdapter, LocalStoreHolder};
use crate::services::persistence::{MemoryPersistence, PersistenceClient, SessionPersistence, StoreResolver};
use crate::services::ssp::{SspAdapter, StreamProcessorService, CHECKPOINT_FLOOR};
use crate::state::lifecycle::is_authoritative;
use crate::state::selectors as sel;
use crate::state::{reducers as r, ClientState};
use crate::types::{
    MutationEventType, PersistenceConfig, QueryHash, QueryStatus, QueryTimings, RunOptions, Schema, Sp00kyConfig,
    SyncHealth, TimingPhase, UpdateOptions, DEFAULT_TTL,
};
use crate::utils::{has_order_by, parse_table_from_surql};
use crate::value::{generate_id, generate_owner_secret, sha256_hex, Object, Row, Value};

#[cfg(feature = "remote")]
use crate::services::remote::{RemoteAdapter, RemoteDatabaseService};
#[cfg(feature = "remote")]
use crate::services::supervisor::{ConnectionSupervisor, SupervisorHooks};

/// The first circuit checkpoint comes this soon after boot.
const FIRST_CHECKPOINT_MS: u64 = 5000;

/// Options for registering a query.
#[derive(Clone, Debug, Default)]
pub struct QueryOptions {
    pub ttl: Option<String>,
    pub relations: Vec<RelationPlan>,
}

/// The remote port of a local-only client: every call fails the way an
/// unreachable server would, so the sagas take their offline paths.
struct OfflineRemote;

#[async_trait(?Send)]
impl RemotePort for OfflineRemote {
    async fn query(&self, _sql: &str, _vars: Option<Object>) -> Result<Vec<StatementResult>> {
        Err(Error::NoRemote("No remote endpoint is configured".into()))
    }
    async fn write(&self, _sql: &str, _vars: Option<Object>) -> Result<Vec<StatementResult>> {
        Err(Error::NoRemote("No remote endpoint is configured".into()))
    }
    async fn live(&self, _table: &str, _on_change: LiveCallback) -> Result<String> {
        Err(Error::NoRemote("No remote endpoint is configured".into()))
    }
    async fn kill(&self, _uuid: &str) -> Result<()> {
        Ok(())
    }
}

/// The in-process client.
pub struct Engine {
    config: Sp00kyConfig,
    schema: Arc<Schema>,
    env: SagaEnv,
    runtime: Runtime,
    holder: Rc<LocalStoreHolder>,
    ssp: Rc<StreamProcessorService>,
    persistence: Rc<dyn PersistenceClient>,
    session_persistence: Option<Rc<SessionPersistence>>,
    services: Rc<ClientServices>,
    #[cfg(feature = "remote")]
    remote: Option<RemoteDatabaseService>,
    #[cfg(feature = "remote")]
    remote_adapter: Option<Rc<RemoteAdapter>>,
    #[cfg(feature = "remote")]
    supervisor: Option<ConnectionSupervisor>,
    auth: Option<AuthService>,
    host: RefCell<Option<Rc<dyn QueryHost>>>,
    features: RefCell<Option<FeatureFlagModule>>,
    releases: RefCell<Option<AppReleaseModule>>,
    blobs: RefCell<Option<BlobCache>>,
    offline: Rc<Cell<bool>>,
    closed: Cell<bool>,
    checkpoint_task: RefCell<Option<tokio::task::JoinHandle<()>>>,
    /// Unsubscribes kept for the engine's lifetime.
    keep: RefCell<Vec<Box<dyn FnOnce()>>>,
    /// Peer to peer channels (`client/channels.rs`).
    pub(super) channels: crate::client::channels::ChannelSlot,
}

/// The tables the circuit primes from: every schema table.
fn synced_tables(schema: &Schema) -> Vec<String> {
    schema.table_names().map(str::to_string).collect()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl Engine {
    /// Build the engine and run the local-first half of boot: the store opens,
    /// the schema provisions, the circuit primes from local rows and the
    /// session is restored from the cached token. The network half runs in the
    /// background, so a query registered right after this paints from cache.
    pub async fn open(config: Sp00kyConfig) -> Result<Rc<Engine>> {
        let engine = Rc::new(Self::build(config)?);
        engine.wire();
        engine.boot().await?;
        Ok(engine)
    }

    fn build(config: Sp00kyConfig) -> Result<Engine> {
        let schema = Arc::new(config.schema.clone());
        let client_id: String = generate_id().chars().take(8).collect();

        let holder = Rc::new(LocalStoreHolder::new(config.database.store.clone()));
        // The store has to exist before persistence can read the boot hint out
        // of it; the bucket the hint names replaces it a moment later.
        holder.connect(ANON_USER_ID)?;

        let (persistence, session_persistence): (Rc<dyn PersistenceClient>, Option<Rc<SessionPersistence>>) =
            match config.persistence {
                PersistenceConfig::Memory => (Rc::new(MemoryPersistence::new()), None),
                PersistenceConfig::Session => {
                    let h = holder.clone();
                    let resolver: StoreResolver = Rc::new(move || h.db());
                    let sp = Rc::new(SessionPersistence::open(&config.database.store, resolver)?);
                    (sp.clone(), Some(sp))
                }
            };

        let ssp = Rc::new(StreamProcessorService::new());
        let has_remote = config.database.endpoint.is_some() && cfg!(feature = "remote");

        let tables = synced_tables(&schema);
        let column_sets =
            column_fingerprints(&schema, tables.iter().map(String::as_str).filter(|t| !t.starts_with("_00_")));
        let services = Rc::new(ClientServices::new(
            holder.clone(),
            ssp.clone(),
            persistence.clone(),
            config.schema_surql.clone(),
            tables,
            column_sets,
        ));
        let offline = services.offline.clone();

        let mut env = SagaEnv::new(schema.clone());
        env.ref_mode = RefMode::Dedicated;
        env.anon_live = config.enable_anonymous_live_queries;
        env.materialize_debounce_ms = config.stream_debounce_ms;
        env.poll_base_ms = config.ref_sync_interval_ms;
        env.degrade_after = if has_remote {
            config.sync_health.degrade_after_consecutive_failures
        } else {
            0
        };
        env.has_remote = has_remote;

        #[cfg(not(feature = "remote"))]
        let auth: Option<AuthService> = None;
        #[cfg(feature = "remote")]
        let (remote, remote_adapter, supervisor, auth) = if has_remote {
            let client = crate::surreal::rpc::WebSocketSurrealClient::new();
            client.set_connect_timeout(Duration::from_millis(config.reconnect.connect_timeout_ms));
            let remote = RemoteDatabaseService::new(config.database.clone(), Rc::new(client));
            // Armed before boot: boot is local-first and returns before the
            // network half has run, so an app that signs up the instant `open`
            // resolves would otherwise reach the server before `use(ns, db)`.
            remote.arm_connect_gate();
            let adapter = Rc::new(RemoteAdapter::new(remote.clone(), config.live_inline_bodies));
            let auth = AuthService::new(
                &schema,
                Rc::new(remote.clone()),
                Rc::new(PersistenceBridge(persistence.clone())),
            );
            // The liveness probe also retries a deferred auth verification.
            let mut hooks = SupervisorHooks::for_remote(&remote);
            let (probe_remote, probe_auth) = (remote.clone(), auth.clone());
            hooks.probe = Rc::new(move || {
                let (remote, auth) = (probe_remote.clone(), probe_auth.clone());
                async move {
                    remote.probe().await?;
                    if auth.needs_verification() {
                        if let Err(e) = auth.check(None).await {
                            tracing::warn!(target: "spooky", "Auth retry deferred: {e}");
                        }
                    }
                    Ok(())
                }
                .boxed_local()
            });
            let supervisor = ConnectionSupervisor::new(hooks, remote.events(), config.reconnect);
            let handshake_auth = auth.clone();
            remote.set_on_handshake(Some(Rc::new(move || {
                let auth = handshake_auth.clone();
                tokio::task::spawn_local(async move {
                    if let Err(e) = auth.check(None).await {
                        tracing::warn!(target: "spooky", "Auth verification deferred: {e}");
                    }
                });
            })));
            *services.remote.borrow_mut() = Some(remote.clone());
            *services.supervisor.borrow_mut() = Some(supervisor.clone());
            *services.auth.borrow_mut() = Some(Rc::new(auth.clone()));
            (Some(remote), Some(adapter), Some(supervisor), Some(auth))
        } else {
            (None, None, None, None)
        };

        let remote_port: Rc<dyn RemotePort> = {
            #[cfg(feature = "remote")]
            {
                match &remote_adapter {
                    Some(a) => a.clone(),
                    None => Rc::new(OfflineRemote),
                }
            }
            #[cfg(not(feature = "remote"))]
            {
                Rc::new(OfflineRemote)
            }
        };

        let id_client = client_id.clone();
        let adapters = Adapters {
            local: Rc::new(LocalStoreAdapter::new(holder.clone())),
            remote: remote_port,
            ssp: Rc::new(SspAdapter::new(ssp.clone())),
            timers: Rc::new(TokioTimers::new()),
            services: services.clone(),
            now: Rc::new(now_ms),
            ids: Rc::new(move |scope| match scope {
                IdScope::Mutation => mutation_id::mint_mutation_id(&id_client),
                IdScope::Salt => generate_id(),
                IdScope::Owner => generate_owner_secret(),
            }),
            hash: Rc::new(sha256_hex),
        };
        let runtime = Runtime::new(env.clone(), adapters, ClientState::empty(client_id));

        // The circuit's updates are what dirty a query, so they have to reach
        // the runtime before anything registers.
        let weak = runtime.downgrade();
        ssp.add_receiver(Rc::new(move |update| {
            if let Some(rt) = weak.upgrade() {
                rt.dispatch(RuntimeEvent::StreamUpdate { update: Box::new(update) });
            }
        }));
        let weak = runtime.downgrade();
        *services.on_versions.borrow_mut() = Some(Rc::new(move |entries| {
            if let Some(rt) = weak.upgrade() {
                rt.dispatch(RuntimeEvent::VersionsPrimed { entries });
            }
        }));

        let engine = Engine {
            config,
            schema,
            env,
            runtime,
            holder,
            ssp,
            persistence,
            session_persistence,
            services,
            #[cfg(feature = "remote")]
            remote,
            #[cfg(feature = "remote")]
            remote_adapter,
            #[cfg(feature = "remote")]
            supervisor,
            auth,
            host: RefCell::new(None),
            features: RefCell::new(None),
            releases: RefCell::new(None),
            blobs: RefCell::new(None),
            offline,
            closed: Cell::new(false),
            checkpoint_task: RefCell::new(None),
            keep: RefCell::new(Vec::new()),
            channels: Default::default(),
        };
        Ok(engine)
    }

    /// Hooks that need the engine itself: the query host, the auth session
    /// flips, the sign-out drain, the flag and release modules.
    fn wire(self: &Rc<Self>) {
        let host: Rc<dyn QueryHost> = Rc::new(EngineHost(Rc::downgrade(self)));
        *self.host.borrow_mut() = Some(host.clone());
        self.wire_channels();
        let Some(auth) = self.auth.clone() else { return };

        // Push what is already queued while the session is still valid.
        // Bounded: an unreachable server must not hold sign-out open; anything
        // left drains on that user's next sign-in.
        let rt = self.runtime.downgrade();
        let bound = Duration::from_millis(self.config.reconnect.connect_timeout_ms);
        auth.on_before_sign_out(Rc::new(move |_| {
            let rt = rt.clone();
            async move {
                if let Some(rt) = rt.upgrade() {
                    let _ = tokio::time::timeout(bound, rt.dispatch_async(RuntimeEvent::Drain)).await;
                }
                Ok(())
            }
            .boxed_local()
        }))
        .forget();

        // The signed-in principal drives routing, the local `$auth`, the
        // bucket and the salt: all of that is one saga.
        let weak = Rc::downgrade(self);
        auth.set_on_session_changed(Some(Rc::new(move |user_id: Option<String>| {
            let weak = weak.clone();
            async move {
                let Some(engine) = weak.upgrade() else { return Ok(()) };
                let current = engine.auth.as_ref().and_then(|a| a.user_id());
                if current != user_id {
                    return Ok(());
                }
                engine.flip_session(user_id).await
            }
            .boxed_local()
        })));

        let source: Rc<dyn AuthSource> = Rc::new(auth.clone());
        let features = FeatureFlagModule::new(host.clone(), source.clone());
        let releases = AppReleaseModule::new(host, source);
        let (f, r) = (features.clone(), releases.clone());
        self.add_late_module(Rc::new(move || {
            f.init();
            r.init();
        }));
        *self.features.borrow_mut() = Some(features);
        *self.releases.borrow_mut() = Some(releases);
    }

    async fn boot(self: &Rc<Self>) -> Result<()> {
        #[cfg(feature = "remote")]
        if let Some(sup) = &self.supervisor {
            let weak = self.runtime.downgrade();
            let off = sup.subscribe(move |state| {
                if let Some(rt) = weak.upgrade() {
                    rt.dispatch(RuntimeEvent::ConnectionChanged { state });
                }
            });
            self.keep.borrow_mut().push(off);
        }
        let env = self.env.clone();
        self.runtime
            .run_unfenced(None, |ctx| async move { boot_saga::boot(&ctx, &env).await })
            .await?;
        // Soon after boot, so a store rebuilt from rows is snapshotted before
        // the app can be killed; late enough not to queue behind the first
        // screen's queries.
        self.arm_checkpoints(self.config.circuit_checkpoint_ms.min(FIRST_CHECKPOINT_MS));
        if let Some(auth) = &self.auth {
            if let Err(e) = auth.publish_session().await {
                tracing::warn!(target: "spooky", "publishing the restored session failed: {e}");
            }
        }
        if self.env.has_remote {
            self.runtime.dispatch(RuntimeEvent::StartRemote);
        }
        tracing::info!(target: "spooky", "Sp00kyClient initialized");
        Ok(())
    }

    /// Install the auth module: the services read the session through it, and
    /// every change of the signed-in principal runs the auth flip.
    pub fn attach_auth(&self, auth: Rc<dyn AuthSession>) {
        *self.services.auth.borrow_mut() = Some(auth);
    }

    /// Run the account flip for `user_id` (sign-in, sign-out, restore).
    pub async fn flip_session(&self, user_id: Option<String>) -> Result<()> {
        let env = self.env.clone();
        self.runtime
            .run_unfenced(Some("bucket"), |ctx| async move { auth_flip::auth_flip(&ctx, &env, user_id).await })
            .await
    }

    /// Register modules whose live query starts once boot has finished.
    pub fn add_late_module(&self, start: Rc<dyn Fn()>) {
        self.services.late_modules.borrow_mut().push(start);
    }

    // ---- accessors -------------------------------------------------------------

    pub fn config(&self) -> &Sp00kyConfig {
        &self.config
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn env(&self) -> &SagaEnv {
        &self.env
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn stream_processor(&self) -> &Rc<StreamProcessorService> {
        &self.ssp
    }

    pub fn local_store(&self) -> &Rc<LocalStoreHolder> {
        &self.holder
    }

    #[cfg(feature = "remote")]
    pub fn remote(&self) -> Option<&RemoteDatabaseService> {
        self.remote.as_ref()
    }

    #[cfg(feature = "remote")]
    pub fn supervisor(&self) -> Option<&ConnectionSupervisor> {
        self.supervisor.as_ref()
    }

    pub fn has_remote(&self) -> bool {
        self.env.has_remote
    }

    /// The engine state, read-only (tests and diagnostics).
    pub fn state(&self) -> ClientState {
        self.runtime.snapshot()
    }

    pub fn store_epoch(&self) -> u64 {
        self.holder.epoch()
    }

    /// True once the local half of boot has finished.
    pub fn is_local_ready(&self) -> bool {
        self.runtime.read(|s| s.local_ready)
    }

    /// Feed an event into the engine and wait for its saga.
    pub async fn dispatch(&self, event: RuntimeEvent) {
        self.runtime.dispatch_async(event).await;
    }

    // ---- lifecycle -------------------------------------------------------------

    /// The host came back to the foreground, or the network returned: reset
    /// the reconnect backoff, probe a possibly half-open socket, beat the TTL
    /// heartbeat.
    pub fn wake(&self) {
        #[cfg(feature = "remote")]
        if let Some(s) = &self.supervisor {
            s.wake("resumed");
        }
        if let Some(auth) = self.services.auth.borrow().clone() {
            tokio::task::spawn_local(async move {
                if let Err(e) = auth.init().await {
                    tracing::warn!(target: "spooky", "Auth retry deferred: {e}");
                }
            });
        }
        self.runtime.dispatch(RuntimeEvent::HeartbeatNow);
    }

    /// The host's network went away (`true`) or came back (`false`). Coming
    /// back does not reconnect by itself; call [`Engine::wake`].
    pub fn set_offline(&self, offline: bool) {
        self.offline.set(offline);
    }

    /// The host is going away for good: hand this client's views back rather
    /// than leaving them to expire by TTL.
    pub async fn detach(&self) {
        self.runtime.dispatch_async(RuntimeEvent::AppDetached).await;
    }

    fn arm_checkpoints(self: &Rc<Self>, first_ms: u64) {
        let weak = Rc::downgrade(self);
        let every = self.config.circuit_checkpoint_ms.max(1);
        let task = tokio::task::spawn_local(async move {
            let mut wait = first_ms;
            loop {
                tokio::time::sleep(Duration::from_millis(wait)).await;
                let Some(engine) = weak.upgrade() else { return };
                if engine.closed.get() {
                    return;
                }
                if engine.ssp.dirty_rows() >= CHECKPOINT_FLOOR {
                    let _ = engine.checkpoint().await;
                }
                wait = every;
            }
        });
        if let Some(old) = self.checkpoint_task.borrow_mut().replace(task) {
            old.abort();
        }
    }

    /// Snapshot the circuit's rows so the next boot restores them instead of
    /// re-reading every row. A no-op when nothing changed since the last one.
    pub async fn checkpoint(&self) -> Result<()> {
        if self.closed.get() {
            return Ok(());
        }
        let ssp = self.ssp.clone();
        let holder = self.holder.clone();
        self.runtime
            .run_serial("bucket", |_ctx| async move { Self::checkpoint_now(&ssp, &holder) })
            .await
    }

    fn checkpoint_now(ssp: &StreamProcessorService, holder: &LocalStoreHolder) -> Result<()> {
        if !ssp.snapshot_dirty() {
            return Ok(());
        }
        match holder.db().and_then(|db| ssp.checkpoint_into(&db)) {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!(target: "spooky", "Circuit checkpoint failed: {e}");
                Ok(())
            }
        }
    }

    /// Close the client: stop the runtime, snapshot the circuit, close the
    /// socket and the stores. Idempotent.
    pub async fn close(&self) {
        if self.closed.replace(true) {
            return;
        }
        if let Some(task) = self.checkpoint_task.borrow_mut().take() {
            task.abort();
        }
        for off in self.keep.borrow_mut().drain(..) {
            off();
        }
        self.close_channels();
        #[cfg(feature = "remote")]
        if let Some(remote) = &self.remote {
            remote.set_on_handshake(None);
        }
        if let Some(auth) = &self.auth {
            auth.dispose();
        }
        if let Some(f) = self.features.borrow_mut().take() {
            f.close_all();
        }
        if let Some(r) = self.releases.borrow_mut().take() {
            r.close_all();
        }
        self.host.borrow_mut().take();
        self.runtime.dispose();
        let _ = Self::checkpoint_now(&self.ssp, &self.holder);
        #[cfg(feature = "remote")]
        {
            if let Some(s) = &self.supervisor {
                s.dispose();
            }
            if let Some(a) = &self.remote_adapter {
                a.dispose();
            }
            if let Some(r) = &self.remote {
                r.close().await;
            }
        }
        self.ssp.close();
        self.holder.close();
        if let Some(sp) = &self.session_persistence {
            sp.close();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.get()
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed.get() {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    // ---- key/value ---------------------------------------------------------------

    /// One key of the client's persistence, for modules that keep per-device
    /// state.
    pub async fn kv_read(&self, key: &str) -> Result<Option<Value>> {
        self.persistence.get(key).await
    }

    /// Write (or with `None`, remove) one key of the client's persistence.
    pub async fn kv_write(&self, key: &str, value: Option<Value>) -> Result<()> {
        match value {
            Some(v) => self.persistence.set(key, v).await,
            None => self.persistence.remove(key).await,
        }
    }

    pub fn persistence(&self) -> &Rc<dyn PersistenceClient> {
        &self.persistence
    }

    // ---- queries -------------------------------------------------------------------

    /// One-shot remote query, bypassing the sync layer. Results are NOT synced
    /// into the local cache.
    pub async fn query_remote(&self, sql: &str, vars: Option<Object>) -> Result<Vec<Value>> {
        #[cfg(feature = "remote")]
        if let Some(remote) = &self.remote {
            return remote.query(sql, vars).await;
        }
        let _ = (sql, vars);
        Err(Error::NoRemote("query_remote requires a remote endpoint".into()))
    }

    fn register_input(&self, sql: &str, params: Object, options: QueryOptions) -> Result<RegisterInput> {
        Ok(RegisterInput {
            table_name: parse_table_from_surql(sql)?,
            surql: sql.to_string(),
            params,
            ttl: options.ttl.unwrap_or_else(|| DEFAULT_TTL.to_string()),
            has_explicit_order: has_order_by(sql),
            relations: options.relations,
        })
    }

    /// Register a raw SurrealQL query and return its hash. Returns as soon as
    /// the LOCAL registration completes, so the first paint comes from the
    /// local store; the remote registration is dispatched, never awaited.
    pub async fn query_raw(&self, sql: &str, params: Object, options: QueryOptions) -> Result<QueryHash> {
        self.ensure_open()?;
        let input = self.register_input(sql, params, options)?;
        let env = self.env.clone();
        self.runtime
            .run(|ctx| async move { register_local(&ctx, &env, input).await })
            .await
    }

    /// Prewarm a query without subscribing. Resolved before on this device:
    /// returns at once. Never resolved: resolves once the server's membership
    /// and every body are local.
    pub async fn preload(&self, sql: &str, params: Object, options: QueryOptions) -> Result<QueryHash> {
        self.ensure_open()?;
        let input = self.register_input(sql, params, options)?;
        let env = self.env.clone();
        self.runtime
            .run(|ctx| async move { preload_saga::preload(&ctx, &env, input).await })
            .await
            .map(|(hash, _)| hash)
    }

    /// A registered query's rows as the engine renders them, before any
    /// channel overlay ([`Engine::subscribe`] folds peers' values in).
    pub fn subscribe_records(&self, hash: &str, cb: impl Fn(&Arc<Vec<Row>>) + 'static, immediate: bool) -> Subscription {
        self.runtime.subscribe(hash, cb, immediate)
    }

    /// A query's fetch status (idle/fetching).
    pub fn subscribe_status(&self, hash: &str, cb: impl Fn(QueryStatus) + 'static, immediate: bool) -> Subscription {
        self.runtime.subscribe_status(hash, cb, immediate)
    }

    /// A query's authority: true once server membership is known for it,
    /// false when a bucket switch resets it. This is what tells "no rows yet"
    /// apart from "no rows".
    pub fn subscribe_authority(&self, hash: &str, cb: impl Fn(bool) + 'static, immediate: bool) -> Subscription {
        self.runtime.subscribe_authority(hash, cb, immediate)
    }

    /// The current rows of a registered query.
    pub fn records(&self, hash: &str) -> Option<Arc<Vec<Row>>> {
        self.runtime.read(|s| s.queries.get(hash).map(|e| e.records.clone()))
    }

    pub fn query_status(&self, hash: &str) -> Option<QueryStatus> {
        self.runtime.read(|s| sel::query_status(s, hash))
    }

    pub fn is_query_authoritative(&self, hash: &str) -> bool {
        self.runtime
            .read(|s| s.queries.get(hash).is_some_and(|e| is_authoritative(&e.lifecycle)))
    }

    /// Authoritative and complete: server membership accepted, every body
    /// local, nothing left to re-render.
    pub fn is_query_settled(&self, hash: &str) -> bool {
        self.runtime.read(|s| sel::settled(s, hash))
    }

    pub fn query_timings(&self, hash: &str) -> Option<QueryTimings> {
        self.runtime.read(|s| s.queries.get(hash).map(sel::phase_timings))
    }

    /// Report the UI reconcile time (ms) for a query.
    pub fn report_frontend_timing(&self, hash: &str, ms: f64) {
        if ms.is_finite() {
            self.runtime.update(|s| r::record_phase(s, hash, TimingPhase::Frontend, ms));
        }
    }

    /// Opt-in eager teardown of a query nobody subscribes to. Most queries
    /// should not call this: the keep-alive avoids re-registration churn.
    pub fn deregister_query(&self, hash: &str) {
        let idle = self.runtime.read(|s| s.queries.get(hash).is_some_and(|e| e.subscribers == 0));
        if !idle {
            return;
        }
        let rt = self.runtime.clone();
        let hash = hash.to_string();
        tokio::task::spawn_local(async move {
            let key = format!("mat:{hash}");
            let _ = rt.run_serial(&key, |ctx| async move { evict_query(&ctx, &hash).await }).await;
        });
    }

    // ---- mutations -------------------------------------------------------------------

    async fn write(&self, input: write_saga::WriteInput) -> Result<write_saga::WriteResult> {
        self.ensure_open()?;
        let env = self.env.clone();
        self.runtime
            .run_serial("write", |ctx| async move { write_saga::write(&ctx, &env, input).await })
            .await
    }

    /// The optimistic create itself, without channel peers (see
    /// [`Engine::create`]).
    pub(super) async fn create_record(&self, id: &str, data: Object) -> Result<Row> {
        let out = self
            .write(write_saga::WriteInput {
                kind: MutationEventType::Create,
                record_id: id.to_string(),
                data: Some(data.clone()),
                options: None,
            })
            .await?;
        Ok(out.record.unwrap_or_else(|| with_id(data, id)))
    }

    /// The optimistic update itself, without channel peers.
    pub(super) async fn update_record(&self, id: &str, data: Object, options: UpdateOptions) -> Result<Row> {
        let out = self
            .write(write_saga::WriteInput {
                kind: MutationEventType::Update,
                record_id: id.to_string(),
                data: Some(data.clone()),
                options: Some(options),
            })
            .await?;
        Ok(out.record.unwrap_or_else(|| with_id(data, id)))
    }

    /// The optimistic delete itself, without channel peers.
    pub(super) async fn delete_record(&self, id: &str) -> Result<()> {
        self.write(write_saga::WriteInput {
            kind: MutationEventType::Delete,
            record_id: id.to_string(),
            data: None,
            options: None,
        })
        .await
        .map(|_| ())
    }

    /// The row as the local store holds it.
    pub async fn read_row(&self, id: &str) -> Result<Option<Row>> {
        let id = id.to_string();
        self.runtime.run(|ctx| async move { write_saga::read_row(&ctx, &id).await }).await
    }

    /// Enqueue a backend job (`backend` route `path`).
    pub async fn run_job(&self, backend: &str, path: &str, payload: &Object, options: &RunOptions) -> Result<()> {
        let (table, record) = jobs::build_job_record(&self.schema, backend, path, payload, options)?;
        self.create(&format!("{table}:{}", generate_id()), record).await.map(|_| ())
    }

    // ---- failed writes -----------------------------------------------------------------

    /// Writes the server rejected: undone locally and parked here.
    pub fn failed_mutation_count(&self) -> usize {
        self.runtime.read(|s| s.failed_count)
    }

    pub async fn list_failed_mutations(&self) -> Result<Vec<FailedMutationRow>> {
        self.runtime.run(|ctx| async move { tray::list_failed(&ctx).await }).await
    }

    /// Re-apply a rejected mutation as a new optimistic write.
    pub async fn retry_failed_mutation(&self, mutation_id: &str) -> Result<bool> {
        let env = self.env.clone();
        let id = mutation_id.to_string();
        self.runtime
            .run_serial("tray", |ctx| async move { tray::retry_failed(&ctx, &env, &id).await })
            .await
    }

    pub async fn discard_failed_mutation(&self, mutation_id: &str) -> Result<bool> {
        let id = mutation_id.to_string();
        self.runtime
            .run_serial("tray", |ctx| async move { tray::discard_failed(&ctx, &id).await })
            .await
    }

    // ---- observability -------------------------------------------------------------------

    pub fn pending_mutation_count(&self) -> usize {
        self.runtime.read(sel::pending_mutation_count)
    }

    /// Record ids with a local write the server has not acknowledged yet.
    pub fn unsynced_record_ids(&self) -> BTreeSet<String> {
        self.runtime.read(sel::unsynced_record_ids)
    }

    /// How many queries are pulling rows right now.
    pub fn fetching_query_count(&self) -> usize {
        self.runtime.read(sel::fetching_query_count)
    }

    pub fn sync_health(&self) -> SyncHealth {
        self.runtime.read(|s| s.sync.health.clone())
    }

    /// Observe outbound events by kind (`"*"` for all; see
    /// [`OutEvent::kind`]).
    pub fn on(&self, kind: &'static str, cb: impl Fn(&OutEvent) + 'static) -> Subscription {
        self.runtime.on(kind, cb)
    }

    // ---- raw auth ------------------------------------------------------------------------

    /// Authenticate the socket with a raw token from outside the client.
    pub async fn authenticate(&self, token: &str) -> Result<Value> {
        #[cfg(feature = "remote")]
        if let Some(remote) = &self.remote {
            remote.set_auth_token(Some(token.to_string()));
            return remote.authenticate(token).await;
        }
        let _ = token;
        Err(Error::NoRemote("authenticate() requires a remote endpoint".into()))
    }

    pub async fn deauthenticate(&self) -> Result<()> {
        #[cfg(feature = "remote")]
        if let Some(remote) = &self.remote {
            remote.set_auth_token(None);
            return remote.invalidate().await;
        }
        Err(Error::NoRemote("deauthenticate() requires a remote endpoint".into()))
    }

    /// A fresh owner secret (diagnostics).
    #[doc(hidden)]
    pub fn mint_owner_secret() -> String {
        generate_owner_secret()
    }
}

impl Engine {
    // ---- auth ------------------------------------------------------------------

    /// The auth module. `None` without a remote endpoint.
    pub fn auth(&self) -> Option<&AuthService> {
        self.auth.as_ref()
    }

    fn require_auth(&self) -> Result<&AuthService> {
        self.auth
            .as_ref()
            .ok_or_else(|| Error::NoRemote("auth requires a remote endpoint".into()))
    }

    /// Sign in through `DEFINE ACCESS <access>` with its declared params.
    pub async fn sign_in(&self, access: &str, params: Object) -> Result<()> {
        self.require_auth()?.sign_in(access, params).await
    }

    /// Sign up through `DEFINE ACCESS <access>` with its declared params.
    pub async fn sign_up(&self, access: &str, params: Object) -> Result<()> {
        self.require_auth()?.sign_up(access, params).await
    }

    /// Sign out: drain the outbox while the session is valid, then drop it.
    pub async fn sign_out(&self) -> Result<()> {
        self.require_auth()?.sign_out().await
    }

    /// The signed-in user's id.
    pub fn user_id(&self) -> Option<String> {
        self.auth.as_ref().and_then(|a| a.user_id())
    }

    // ---- builder and modules ------------------------------------------------------

    /// The query host the builder and the modules register through.
    pub fn query_host(&self) -> Option<Rc<dyn QueryHost>> {
        self.host.borrow().clone()
    }

    /// A fluent query on `table`, bound to this engine and its schema.
    pub fn query(&self, table: &str) -> QueryBuilder {
        let b = QueryBuilder::new(table).with_schema(self.schema.clone());
        match self.query_host() {
            Some(host) => b.with_host(host),
            None => b,
        }
    }

    /// A reactive handle for feature flag `key`. Requires a remote endpoint.
    pub fn feature(&self, key: &str, fallback: Option<&str>, ttl: Option<&str>) -> Result<FeatureFlagHandle> {
        let f = self.features.borrow();
        let f = f
            .as_ref()
            .ok_or_else(|| Error::NoRemote("feature() requires a remote endpoint".into()))?;
        Ok(f.feature(key, fallback, ttl))
    }

    /// A reactive handle for app `app`'s latest announced release.
    pub fn app_release(&self, app: &str, ttl: Option<&str>) -> Result<AppReleaseHandle> {
        let r = self.releases.borrow();
        let r = r
            .as_ref()
            .ok_or_else(|| Error::NoRemote("app_release() requires a remote endpoint".into()))?;
        Ok(r.release(app, ttl))
    }

    // ---- buckets ---------------------------------------------------------------------

    #[cfg(feature = "remote")]
    fn remote_query(&self) -> Result<RemoteQuery> {
        let remote = self
            .remote
            .clone()
            .ok_or_else(|| Error::NoRemote("bucket() requires a remote endpoint".into()))?;
        Ok(Rc::new(move |sql: String, vars: Option<Object>| {
            let fut = remote.query(&sql, vars);
            fut.boxed_local()
        }))
    }

    #[cfg(not(feature = "remote"))]
    fn remote_query(&self) -> Result<RemoteQuery> {
        Err(Error::NoRemote("bucket() requires a remote endpoint".into()))
    }

    /// The cache namespace for the signed-in user: the per-user bucket id
    /// the local store is split by.
    pub fn blob_namespace(&self) -> String {
        bucket_id_for_user(self.user_id().as_deref())
    }

    /// The one blob cache for this client, built on first use.
    pub fn blob_cache(self: &Rc<Self>) -> Result<BlobCache> {
        if let Some(c) = self.blobs.borrow().as_ref() {
            return Ok(c.clone());
        }
        let cfg = &self.config.blob_cache;
        let store: Rc<dyn BlobStore> = match &cfg.directory {
            Some(dir) => Rc::new(FileBlobStore::new(dir.clone())),
            None => Rc::new(MemoryBlobStore::default()),
        };
        let cache = BlobCache::new(store, BucketHandle::fetcher(self.remote_query()?), cfg.max_bytes)
            .with_namespace(self.blob_namespace());
        if cfg.clear_on_sign_out {
            if let Some(auth) = &self.auth {
                let last = Rc::new(RefCell::new(auth.user_id()));
                let c = cache.clone();
                auth.subscribe(move |user| {
                    let previous = last.replace(user.map(str::to_string));
                    if user.is_none() {
                        if let Some(prev) = previous {
                            let c = c.clone();
                            tokio::task::spawn_local(async move {
                                c.clear(Some(&bucket_id_for_user(Some(&prev)))).await;
                            });
                        }
                    }
                })
                .forget();
            }
        }
        *self.blobs.borrow_mut() = Some(cache.clone());
        Ok(cache)
    }

    /// A handle to storage bucket `name`, reading through the blob cache.
    pub fn bucket(self: &Rc<Self>, name: &str) -> Result<BucketHandle> {
        let cache = self.blob_cache()?;
        let weak = Rc::downgrade(self);
        Ok(BucketHandle::new(name, self.remote_query()?)
            .with_blobs(cache)
            .with_namespace(Rc::new(move || {
                let ns = weak
                    .upgrade()
                    .map(|e| e.blob_namespace())
                    .unwrap_or_else(|| crate::modules::ref_tables::ANON_USER_ID.to_string());
                async move { ns }.boxed_local()
            })))
    }
}

fn with_id(mut data: Object, id: &str) -> Row {
    data.insert("id".into(), Value::String(id.to_string()));
    data
}
