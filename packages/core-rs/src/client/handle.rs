//! [`Sp00kyClient`]: the thread-safe client handle.
//!
//! The engine is single-threaded (like the Dart isolate it mirrors). This
//! handle runs it on a dedicated thread with its own current-thread tokio
//! runtime and talks to it over a channel, so it is `Send + Sync + Clone` and
//! its futures and streams work on any executor (GPUI, smol, tokio). Dropping
//! the last handle closes the client.
//!
//! Live results are `tokio::sync::watch` channels: a UI always reads the
//! latest rows, and a slow consumer never builds a backlog.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::{FutureExt, LocalBoxFuture};
use futures::Stream;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot, watch};

use crate::client::engine::{Engine, QueryOptions};
use crate::error::{Error, Result};
use crate::kernel::events::OutEvent;
use crate::mutation::rows::FailedMutationRow;
use crate::runtime::Subscription;
use crate::types::{QueryHash, QueryStatus, QueryTimings, RunOptions, Schema, Sp00kyConfig, SyncHealth, UpdateOptions};
use crate::value::{from_row, Object, Row, Value};

/// What runs on the engine thread.
pub(crate) struct Worker {
    pub(crate) engine: Rc<Engine>,
    subs: std::cell::RefCell<HashMap<u64, Vec<Subscription>>>,
}

type Job = Box<dyn FnOnce(Rc<Worker>) -> LocalBoxFuture<'static, ()> + Send>;

enum Msg {
    Job(Job),
    /// The last handle is gone: close the engine and end the thread.
    Shutdown,
}

struct Observed {
    health: watch::Receiver<SyncHealth>,
    pending: watch::Receiver<usize>,
    fetching: watch::Receiver<usize>,
    failed: watch::Receiver<usize>,
    unsynced: watch::Receiver<BTreeSet<String>>,
}

struct Shared {
    tx: mpsc::UnboundedSender<Msg>,
    schema: Arc<Schema>,
    observed: Observed,
    next_sub: AtomicU64,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        // Close in the background. Dropping never blocks the caller; await
        // `Sp00kyClient::close` when the snapshot must be written before exit.
        let _ = self.tx.send(Msg::Shutdown);
        drop(self.thread.lock().ok().and_then(|mut t| t.take()));
    }
}

/// The sp00ky client. Cheap to clone; all clones share one engine.
#[derive(Clone)]
pub struct Sp00kyClient {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Sp00kyClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sp00kyClient").finish_non_exhaustive()
    }
}

impl Sp00kyClient {
    /// Start the engine thread and run the local-first half of boot. Resolves
    /// once a query registered next can paint from the local cache; the
    /// network half (connect, auth check, registrations) runs in the
    /// background.
    pub async fn open(config: Sp00kyConfig) -> Result<Sp00kyClient> {
        let schema = Arc::new(config.schema.clone());
        let (tx, mut rx) = mpsc::unbounded_channel::<Msg>();
        let (ready_tx, ready_rx) = oneshot::channel::<Result<Observed>>();
        let thread = std::thread::Builder::new()
            .name("sp00ky".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = ready_tx.send(Err(Error::other(format!("tokio runtime: {e}"))));
                        return;
                    }
                };
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    let engine = match Engine::open(config).await {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = ready_tx.send(Err(e));
                            return;
                        }
                    };
                    let (observed, keep) = observe(&engine);
                    let worker = Rc::new(Worker {
                        engine: engine.clone(),
                        subs: Default::default(),
                    });
                    if ready_tx.send(Ok(observed)).is_err() {
                        engine.close().await;
                        return;
                    }
                    while let Some(Msg::Job(job)) = rx.recv().await {
                        tokio::task::spawn_local(job(worker.clone()));
                    }
                    drop(keep);
                    worker.subs.borrow_mut().clear();
                    engine.close().await;
                });
            })
            .map_err(|e| Error::other(format!("spawn engine thread: {e}")))?;
        let observed = ready_rx.await.map_err(|_| Error::Closed)??;
        Ok(Sp00kyClient {
            shared: Arc::new(Shared {
                tx,
                schema,
                observed,
                next_sub: AtomicU64::new(1),
                thread: Mutex::new(Some(thread)),
            }),
        })
    }

    /// Run `f` on the engine thread and wait for its result.
    pub(crate) async fn call<R, F, Fut>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(Rc<Worker>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R>> + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job: Job = Box::new(move |w| {
            async move {
                let _ = tx.send(f(w).await);
            }
            .boxed_local()
        });
        self.shared.tx.send(Msg::Job(job)).map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Run `f` on the engine thread without waiting.
    fn fire(&self, f: impl FnOnce(Rc<Worker>) + Send + 'static) {
        let job: Job = Box::new(move |w| {
            f(w);
            async {}.boxed_local()
        });
        let _ = self.shared.tx.send(Msg::Job(job));
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.shared.schema
    }

    // ---- lifecycle -------------------------------------------------------------

    /// Close the client now (also happens when the last handle is dropped).
    pub async fn close(&self) -> Result<()> {
        self.call(|w| async move {
            w.subs.borrow_mut().clear();
            w.engine.close().await;
            Ok(())
        })
        .await
    }

    /// Snapshot the circuit now (call on app background / quit).
    pub async fn checkpoint(&self) -> Result<()> {
        self.call(|w| async move { w.engine.checkpoint().await }).await
    }

    /// The host came back to the foreground or the network returned.
    pub fn wake(&self) {
        self.fire(|w| w.engine.wake());
    }

    /// Report that the host has no network (`true`) or has it again (`false`).
    pub fn set_offline(&self, offline: bool) {
        self.fire(move |w| w.engine.set_offline(offline));
    }

    /// Hand this client's views back to the server before going away.
    pub async fn detach(&self) -> Result<()> {
        self.call(|w| async move {
            w.engine.detach().await;
            Ok(())
        })
        .await
    }

    pub async fn is_local_ready(&self) -> Result<bool> {
        self.call(|w| async move { Ok(w.engine.is_local_ready()) }).await
    }

    // ---- queries -----------------------------------------------------------------

    /// Register a raw SurrealQL query and return its hash.
    pub async fn query_raw(&self, sql: &str, params: Object, options: QueryOptions) -> Result<QueryHash> {
        let sql = sql.to_string();
        self.call(move |w| async move { w.engine.query_raw(&sql, params, options).await }).await
    }

    /// Register a raw SurrealQL query and subscribe to it in one call.
    pub async fn live_raw(&self, sql: &str, params: Object, options: QueryOptions) -> Result<LiveQuery> {
        let hash = self.query_raw(sql, params, options).await?;
        self.subscribe(&hash).await
    }

    /// Prewarm a query without subscribing (see [`Engine::preload`]).
    pub async fn preload(&self, sql: &str, params: Object, options: QueryOptions) -> Result<QueryHash> {
        let sql = sql.to_string();
        self.call(move |w| async move { w.engine.preload(&sql, params, options).await }).await
    }

    /// Subscribe to a registered query. The query stays alive while the
    /// returned [`LiveQuery`] does.
    pub async fn subscribe(&self, hash: &str) -> Result<LiveQuery> {
        let id = self.shared.next_sub.fetch_add(1, Ordering::Relaxed);
        let hash_owned = hash.to_string();
        let (rows, status, authority) = self
            .call(move |w| async move {
                let engine = &w.engine;
                let (rows_tx, rows_rx) = watch::channel(engine.records(&hash_owned).unwrap_or_default());
                let (status_tx, status_rx) =
                    watch::channel(engine.query_status(&hash_owned).unwrap_or(QueryStatus::Idle));
                let (auth_tx, auth_rx) = watch::channel(engine.is_query_authoritative(&hash_owned));
                let subs = vec![
                    engine.subscribe(&hash_owned, move |rows| { rows_tx.send_replace(rows.clone()); }, true),
                    engine.subscribe_status(&hash_owned, move |s| { status_tx.send_replace(s); }, true),
                    engine.subscribe_authority(&hash_owned, move |k| { auth_tx.send_replace(k); }, true),
                ];
                w.subs.borrow_mut().insert(id, subs);
                Ok((rows_rx, status_rx, auth_rx))
            })
            .await?;
        Ok(LiveQuery {
            hash: hash.to_string(),
            rows,
            status,
            authority,
            _guard: SubGuard {
                id,
                client: Some(self.clone()),
            },
        })
    }

    pub async fn is_query_settled(&self, hash: &str) -> Result<bool> {
        let hash = hash.to_string();
        self.call(move |w| async move { Ok(w.engine.is_query_settled(&hash)) }).await
    }

    pub async fn query_timings(&self, hash: &str) -> Result<Option<QueryTimings>> {
        let hash = hash.to_string();
        self.call(move |w| async move { Ok(w.engine.query_timings(&hash)) }).await
    }

    pub fn report_frontend_timing(&self, hash: &str, ms: f64) {
        let hash = hash.to_string();
        self.fire(move |w| w.engine.report_frontend_timing(&hash, ms));
    }

    /// Eagerly free a query nobody subscribes to.
    pub fn deregister_query(&self, hash: &str) {
        let hash = hash.to_string();
        self.fire(move |w| w.engine.deregister_query(&hash));
    }

    /// One-shot remote query, bypassing the sync layer.
    pub async fn query_remote(&self, sql: &str, vars: Option<Object>) -> Result<Vec<Value>> {
        let sql = sql.to_string();
        self.call(move |w| async move { w.engine.query_remote(&sql, vars).await }).await
    }

    // ---- mutations -----------------------------------------------------------------

    /// Create `id` (`table:key`) optimistically; returns the row.
    pub async fn create(&self, id: &str, data: impl Into<Object>) -> Result<Row> {
        let (id, data) = (id.to_string(), data.into());
        self.call(move |w| async move { w.engine.create(&id, data).await }).await
    }

    /// Merge `data` into `id` optimistically; returns the row.
    pub async fn update(&self, id: &str, data: impl Into<Object>) -> Result<Row> {
        self.update_with(id, data, UpdateOptions::default()).await
    }

    /// [`Sp00kyClient::update`] with options (debouncing).
    pub async fn update_with(&self, id: &str, data: impl Into<Object>, options: UpdateOptions) -> Result<Row> {
        let (id, data) = (id.to_string(), data.into());
        self.call(move |w| async move { w.engine.update(&id, data, options).await }).await
    }

    /// Delete `id` optimistically.
    pub async fn delete(&self, id: &str) -> Result<()> {
        let id = id.to_string();
        self.call(move |w| async move { w.engine.delete(&id).await }).await
    }

    /// The row as the local store holds it.
    pub async fn read_row(&self, id: &str) -> Result<Option<Row>> {
        let id = id.to_string();
        self.call(move |w| async move { w.engine.read_row(&id).await }).await
    }

    /// Enqueue a backend job.
    pub async fn run(&self, backend: &str, path: &str, payload: Object, options: RunOptions) -> Result<()> {
        let (backend, path) = (backend.to_string(), path.to_string());
        self.call(move |w| async move { w.engine.run_job(&backend, &path, &payload, &options).await })
            .await
    }

    // ---- failed writes -----------------------------------------------------------------

    pub async fn list_failed_mutations(&self) -> Result<Vec<FailedMutationRow>> {
        self.call(|w| async move { w.engine.list_failed_mutations().await }).await
    }

    pub async fn retry_failed_mutation(&self, mutation_id: &str) -> Result<bool> {
        let id = mutation_id.to_string();
        self.call(move |w| async move { w.engine.retry_failed_mutation(&id).await }).await
    }

    pub async fn discard_failed_mutation(&self, mutation_id: &str) -> Result<bool> {
        let id = mutation_id.to_string();
        self.call(move |w| async move { w.engine.discard_failed_mutation(&id).await }).await
    }

    // ---- observability -------------------------------------------------------------------

    pub fn sync_health(&self) -> SyncHealth {
        self.shared.observed.health.borrow().clone()
    }

    /// Sync health: current value now, every transition after.
    pub fn sync_health_watch(&self) -> watch::Receiver<SyncHealth> {
        self.shared.observed.health.clone()
    }

    pub fn pending_mutation_count(&self) -> usize {
        *self.shared.observed.pending.borrow()
    }

    pub fn pending_mutations_watch(&self) -> watch::Receiver<usize> {
        self.shared.observed.pending.clone()
    }

    pub fn fetching_query_count(&self) -> usize {
        *self.shared.observed.fetching.borrow()
    }

    pub fn fetch_activity_watch(&self) -> watch::Receiver<usize> {
        self.shared.observed.fetching.clone()
    }

    pub fn failed_mutation_count(&self) -> usize {
        *self.shared.observed.failed.borrow()
    }

    pub fn failed_mutations_watch(&self) -> watch::Receiver<usize> {
        self.shared.observed.failed.clone()
    }

    /// Record ids with local writes the server has not acknowledged.
    pub fn unsynced_record_ids(&self) -> BTreeSet<String> {
        self.shared.observed.unsynced.borrow().clone()
    }

    pub fn unsynced_records_watch(&self) -> watch::Receiver<BTreeSet<String>> {
        self.shared.observed.unsynced.clone()
    }

    // ---- key/value and raw auth ------------------------------------------------------------

    pub async fn kv_read(&self, key: &str) -> Result<Option<Value>> {
        let key = key.to_string();
        self.call(move |w| async move { w.engine.kv_read(&key).await }).await
    }

    pub async fn kv_write(&self, key: &str, value: Option<Value>) -> Result<()> {
        let key = key.to_string();
        self.call(move |w| async move { w.engine.kv_write(&key, value).await }).await
    }

    /// Authenticate the socket with a raw token from outside the client.
    pub async fn authenticate(&self, token: &str) -> Result<Value> {
        let token = token.to_string();
        self.call(move |w| async move { w.engine.authenticate(&token).await }).await
    }

    pub async fn deauthenticate(&self) -> Result<()> {
        self.call(|w| async move { w.engine.deauthenticate().await }).await
    }
}

/// Wire the engine's outbound events into watch channels the handle reads
/// from any thread.
fn observe(engine: &Rc<Engine>) -> (Observed, Vec<Subscription>) {
    let (health_tx, health) = watch::channel(engine.sync_health());
    let (pending_tx, pending) = watch::channel(engine.pending_mutation_count());
    let (fetching_tx, fetching) = watch::channel(engine.fetching_query_count());
    let (failed_tx, failed) = watch::channel(engine.failed_mutation_count());
    let (unsynced_tx, unsynced) = watch::channel(engine.unsynced_record_ids());
    let sub = engine.on("*", move |e| match e {
        OutEvent::HealthChanged { health } => {
            health_tx.send_replace(health.clone());
        }
        OutEvent::ActivityChanged { fetching, pending } => {
            fetching_tx.send_replace(*fetching);
            pending_tx.send_replace(*pending);
        }
        OutEvent::TrayChanged { count } => {
            failed_tx.send_replace(*count);
        }
        OutEvent::UnsyncedChanged { record_ids } => {
            unsynced_tx.send_replace(record_ids.clone());
        }
        _ => {}
    });
    (
        Observed {
            health,
            pending,
            fetching,
            failed,
            unsynced,
        },
        vec![sub],
    )
}

/// Unsubscribes on the engine thread when dropped.
struct SubGuard {
    id: u64,
    client: Option<Sp00kyClient>,
}

impl Drop for SubGuard {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let id = self.id;
            client.fire(move |w| {
                w.subs.borrow_mut().remove(&id);
            });
        }
    }
}

/// A subscribed query: its latest rows, status and authority. The query
/// stays registered (and its rows live) while this is alive.
pub struct LiveQuery {
    hash: QueryHash,
    rows: watch::Receiver<Arc<Vec<Row>>>,
    status: watch::Receiver<QueryStatus>,
    authority: watch::Receiver<bool>,
    _guard: SubGuard,
}

impl LiveQuery {
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// The latest rows.
    pub fn rows(&self) -> Arc<Vec<Row>> {
        self.rows.borrow().clone()
    }

    /// The latest rows as your own type.
    pub fn rows_as<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.rows.borrow().iter().map(from_row).collect()
    }

    /// Wait until the rows change; `Err(Closed)` once the client is gone.
    pub async fn changed(&mut self) -> Result<Arc<Vec<Row>>> {
        self.rows.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.rows.borrow_and_update().clone())
    }

    /// Fetching or idle.
    pub fn status(&self) -> QueryStatus {
        *self.status.borrow()
    }

    pub fn status_watch(&self) -> watch::Receiver<QueryStatus> {
        self.status.clone()
    }

    /// True once server membership is known: an empty result only means
    /// "empty" once this holds.
    pub fn is_authoritative(&self) -> bool {
        *self.authority.borrow()
    }

    pub fn authority_watch(&self) -> watch::Receiver<bool> {
        self.authority.clone()
    }

    pub fn rows_watch(&self) -> watch::Receiver<Arc<Vec<Row>>> {
        self.rows.clone()
    }

    /// The current rows, then every change, as a `Stream`. The query stays
    /// subscribed while the stream lives.
    pub fn into_stream(self) -> impl Stream<Item = Arc<Vec<Row>>> + Send + Unpin {
        let first = Some(self.rows());
        Box::pin(futures::stream::unfold((self, first), |(mut live, first)| async move {
            if let Some(rows) = first {
                return Some((rows, (live, None)));
            }
            let rows = live.changed().await.ok()?;
            Some((rows, (live, None)))
        }))
    }
}

impl std::fmt::Debug for LiveQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveQuery")
            .field("hash", &self.hash)
            .field("rows", &self.rows.borrow().len())
            .finish()
    }
}
