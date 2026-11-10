//! Peer to peer channels on the client.
//!
//! PORT: the channel half of `packages/spooky_core/lib/src/sp00ky_client.dart`:
//! `channel()`, `Sp00kyChannel`, the write wrappers that split `-- @ephemeral`
//! fields off and tell peers first, and the subscriber tap that folds peers'
//! values into what subscribers see.
//!
//! [`Sp00kyChannel`] is the engine-side handle (single-threaded, like the
//! [`Engine`]); [`Channel`] is the thread-safe one the [`Sp00kyClient`] hands
//! out, its state mirrored into a `watch` channel like [`crate::LiveQuery`].

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::client::engine::Engine;
use crate::client::handle::Sp00kyClient;
use crate::error::{Error, Result};
use crate::modules::channel::PeerRef;
use crate::modules::channel::{
    prepare_scope, webrtc_transport, ChannelHandle, ChannelManager, ChannelRows, ChannelState,
    LocalTransportFactory, ManagerOptions, OutgoingSignal, PreparedScope, Signal, SignalPort,
    WebRtcOptions,
};
use crate::modules::query_builder::QueryBuilder;
use crate::mutation::write::{self as write_saga, PeerWriteInput};
use crate::runtime::Subscription;
use crate::types::UpdateOptions;
use crate::value::{table_of, Object, Row, Value};

/// The engine's channel state: the manager (built at wiring), the signaling
/// port, and the channels the thread-safe client opened.
#[derive(Default)]
pub(crate) struct ChannelSlot {
    manager: RefCell<Option<ChannelManager>>,
    signal: RefCell<Option<Rc<dyn SignalPort>>>,
    shared: RefCell<HashMap<u64, SharedChannel>>,
    keep: RefCell<Vec<Subscription>>,
}

struct SharedChannel {
    channel: Sp00kyChannel,
    _state: Subscription,
}

/// The engine's peer sagas, as the manager reaches them.
struct EngineRows(Weak<Engine>);

impl EngineRows {
    fn engine(&self) -> Result<Rc<Engine>> {
        self.0.upgrade().ok_or(Error::Closed)
    }
}

#[async_trait(?Send)]
impl ChannelRows for EngineRows {
    async fn create(&self, _table: &str, id: &str, row: Object, ttl_ms: u64) -> Result<bool> {
        self.engine()?.peer_create(id, row, ttl_ms).await
    }

    async fn delete(&self, _table: &str, id: &str, ttl_ms: u64) -> Result<bool> {
        self.engine()?.peer_delete(id, ttl_ms).await
    }

    async fn read(&self, id: &str) -> Result<Option<Row>> {
        self.engine()?.read_row(id).await
    }
}

/// The signaling port of a client with no server: sending fails the way an
/// unreachable server would, listening hears nothing.
struct OfflineSignal;

#[async_trait(?Send)]
impl SignalPort for OfflineSignal {
    async fn send(&self, _signal: OutgoingSignal) -> Result<()> {
        Err(Error::NoRemote(
            "channel signaling requires a remote endpoint".into(),
        ))
    }

    fn listen(&self, _me: &PeerRef, _cb: Rc<dyn Fn(Signal)>) -> Box<dyn FnOnce()> {
        Box::new(|| {})
    }
}

#[cfg(feature = "remote")]
mod remote_signal {
    use super::*;
    use crate::modules::channel::SignalRemote;
    use crate::services::remote::RemoteDatabaseService;
    use crate::services::supervisor::ConnectionSupervisor;
    use crate::surreal::rpc::LiveStream;
    use crate::types::ConnectionState;

    /// Signaling over the engine's socket and its connection state.
    pub(super) struct EngineSignalRemote {
        pub remote: RemoteDatabaseService,
        pub supervisor: ConnectionSupervisor,
    }

    #[async_trait(?Send)]
    impl SignalRemote for EngineSignalRemote {
        async fn query(&self, sql: &str, vars: Object) -> Result<Vec<Value>> {
            self.remote.query(sql, Some(vars)).await
        }

        async fn live(&self, sql: &str, vars: Object) -> Result<(String, LiveStream)> {
            self.remote.live(sql, Some(vars)).await
        }

        async fn kill(&self, live_id: &str) -> Result<()> {
            self.remote.kill(live_id).await
        }

        fn subscribe_connection(&self, cb: Box<dyn Fn(ConnectionState)>) -> Box<dyn FnOnce()> {
            self.supervisor.subscribe(cb)
        }
    }
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

impl Engine {
    /// Build the channel manager. Its transport only starts once a channel
    /// lists someone and a user is signed in.
    pub(super) fn wire_channels(self: &Rc<Self>) {
        let cfg = self.config().channels.clone();
        let signal = self.build_signal();
        *self.channels.signal.borrow_mut() = Some(signal.clone());
        let transport: LocalTransportFactory = match cfg.as_ref().and_then(|c| c.transport.clone())
        {
            Some(factory) => Rc::new(move |hooks| factory(hooks)),
            None => {
                let rtc = cfg.as_ref().and_then(|c| c.rtc.clone());
                let ice_servers = cfg.as_ref().and_then(|c| c.ice_servers.clone());
                Rc::new(move |hooks| {
                    webrtc_transport(
                        hooks,
                        WebRtcOptions {
                            signal: signal.clone(),
                            rtc: rtc.clone(),
                            ice_servers: ice_servers.clone(),
                            logger: None,
                            connect_timeout: CONNECT_TIMEOUT,
                        },
                    )
                })
            }
        };
        let rows: Rc<dyn ChannelRows> = Rc::new(EngineRows(Rc::downgrade(self)));
        let manager = ChannelManager::new(
            self.schema().clone(),
            ManagerOptions {
                config: cfg,
                rows: Some(rows),
                transport: Some(transport),
                ..Default::default()
            },
        );
        // Who this device is to its peers: the signed-in user.
        if manager.enabled() {
            if let Some(auth) = self.auth() {
                let m = manager.clone();
                let sub = auth.subscribe(move |user| m.set_user(user));
                self.channels.keep.borrow_mut().push(sub);
            }
        }
        *self.channels.manager.borrow_mut() = Some(manager);
    }

    fn build_signal(&self) -> Rc<dyn SignalPort> {
        #[cfg(feature = "remote")]
        if let (Some(remote), Some(supervisor)) = (self.remote(), self.supervisor()) {
            return crate::modules::channel::surreal_signal(
                Rc::new(remote_signal::EngineSignalRemote {
                    remote: remote.clone(),
                    supervisor: supervisor.clone(),
                }),
                None,
            );
        }
        Rc::new(OfflineSignal)
    }

    pub(super) fn close_channels(&self) {
        self.channels.shared.borrow_mut().clear();
        self.channels.keep.borrow_mut().clear();
        let manager = self.channels.manager.borrow_mut().take();
        if let Some(m) = manager {
            m.close();
        }
    }

    /// The channel manager, for any schema (`None` once closed). A client with
    /// no auth (no endpoint) tells it who this device is with
    /// [`ChannelManager::set_user`].
    pub fn channel_manager(&self) -> Option<ChannelManager> {
        self.channels.manager.borrow().clone()
    }

    /// The channel manager when the schema has `@channel` tables, else `None`.
    fn active_channels(&self) -> Option<ChannelManager> {
        self.channel_manager().filter(ChannelManager::enabled)
    }

    /// Where WebRTC signals go: rows of `_00_channel_signal`.
    pub fn channel_signal(&self) -> Rc<dyn SignalPort> {
        self.channels
            .signal
            .borrow()
            .clone()
            .unwrap_or_else(|| Rc::new(OfflineSignal))
    }

    /// Open a peer to peer channel to `peers` (`table:id` strings or record
    /// ids; your own id is skipped, and a connection forms only when both sides
    /// list each other) for the rows `queries` match: queries on `-- @channel`
    /// tables. Only the `where` counts, evaluated against each row; the query
    /// is never registered. Writes to matching rows (plain or the channel's
    /// own) reach the peers, and theirs show in this engine's query results
    /// before the database has them. Close it when done.
    pub fn channel<P: Into<Value>>(
        self: &Rc<Self>,
        queries: &[QueryBuilder],
        peers: impl IntoIterator<Item = P>,
    ) -> Result<Sp00kyChannel> {
        let manager = self.channel_manager().ok_or(Error::Closed)?;
        let scopes = queries
            .iter()
            .map(|q| manager.scope(q))
            .collect::<Result<Vec<_>>>()?;
        let handle = manager.open(scopes, peers.into_iter().map(Into::into).collect())?;
        Ok(Sp00kyChannel {
            engine: Rc::downgrade(self),
            manager,
            handle,
        })
    }

    /// A channel the thread-safe client opened, scopes compiled on its thread.
    fn open_shared_channel(
        self: &Rc<Self>,
        id: u64,
        prepared: Vec<PreparedScope>,
        peers: Vec<Value>,
    ) -> Result<(Vec<String>, watch::Receiver<ChannelState>)> {
        let manager = self.channel_manager().ok_or(Error::Closed)?;
        let scopes = prepared.into_iter().map(|p| manager.adopt(p)).collect();
        let handle = manager.open(scopes, peers)?;
        let channel = Sp00kyChannel {
            engine: Rc::downgrade(self),
            manager,
            handle,
        };
        let (tx, rx) = watch::channel(channel.state());
        // The first call repeats the current state: not a change.
        let sub = channel.subscribe(move |s| {
            tx.send_if_modified(|current| {
                let changed = current != s;
                if changed {
                    *current = s.clone();
                }
                changed
            });
        });
        let tables = channel.tables();
        self.channels.shared.borrow_mut().insert(
            id,
            SharedChannel {
                channel,
                _state: sub,
            },
        );
        Ok((tables, rx))
    }

    fn shared_channel(&self, id: u64) -> Result<Sp00kyChannel> {
        self.channels
            .shared
            .borrow()
            .get(&id)
            .map(|c| c.channel.clone())
            .ok_or_else(|| Error::other("This channel is closed."))
    }

    fn close_shared_channel(&self, id: u64) {
        let removed = self.channels.shared.borrow_mut().remove(&id);
        if let Some(c) = removed {
            c.channel.close();
        }
    }

    // ---- channel peers' writes ---------------------------------------------------

    /// A channel peer created a row: into the store and the circuit,
    /// unconfirmed for `ttl_ms` (the `peer_create` saga). False when refused.
    pub async fn peer_create(&self, id: &str, row: Object, ttl_ms: u64) -> Result<bool> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let env = self.env().clone();
        let input = PeerWriteInput {
            record_id: id.to_string(),
            data: Some(row),
            ttl_ms,
        };
        self.runtime()
            .run_serial("write", |ctx| async move {
                write_saga::peer_create(&ctx, &env, input).await
            })
            .await
    }

    /// A channel peer deleted a row: hidden until confirmed (the `peer_delete`
    /// saga). False when there was nothing to do.
    pub async fn peer_delete(&self, id: &str, ttl_ms: u64) -> Result<bool> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let env = self.env().clone();
        let input = PeerWriteInput {
            record_id: id.to_string(),
            data: None,
            ttl_ms,
        };
        self.runtime()
            .run_serial("write", |ctx| async move {
                write_saga::peer_delete(&ctx, &env, input).await
            })
            .await
    }

    // ---- writes and subscriptions, through the channels --------------------------------
    //
    // Peers go first in all three: the local write costs a transaction they
    // need not wait for. `@ephemeral` fields never reach the database.

    /// Create `id` (`table:key`) with `data`, optimistically. Returns the row.
    /// On a `@channel` table the channels' peers get it at once.
    pub async fn create(&self, id: &str, data: Object) -> Result<Row> {
        self.write_create(id, data, None).await
    }

    /// Merge `data` into `id`, optimistically. Returns the row. On a `@channel`
    /// table `@ephemeral` fields only reach the peers.
    pub async fn update(&self, id: &str, data: Object, options: UpdateOptions) -> Result<Row> {
        self.write_update(id, data, options, None).await
    }

    /// Delete `id`, optimistically.
    pub async fn delete(&self, id: &str) -> Result<()> {
        self.write_delete(id, None).await
    }

    /// A registered query's rows, on every change. `immediate` replays the
    /// current rows. With `@channel` tables in the schema, channel peers'
    /// values are folded in (output only, never the store).
    pub fn subscribe(
        &self,
        hash: &str,
        cb: impl Fn(&Arc<Vec<Row>>) + 'static,
        immediate: bool,
    ) -> Subscription {
        let Some(channels) = self.active_channels() else {
            return self.subscribe_records(hash, cb, immediate);
        };
        let tap = Rc::new(channels.tap(cb));
        let feed = tap.clone();
        let sub = self.subscribe_records(hash, move |rows| feed.feed(rows), immediate);
        Subscription::new(move || {
            drop(sub);
            tap.stop();
        })
    }

    async fn write_create(
        &self,
        id: &str,
        data: Object,
        scope: Option<&ChannelHandle>,
    ) -> Result<Row> {
        let Some(channels) = self.active_channels() else {
            return self.create_record(id, data).await;
        };
        let table = table_of(id);
        let (persistent, ephemeral) = channels.split(table, &data);
        channels.local_create(table, id, &data, scope);
        let mut out = self.create_record(id, persistent).await?;
        out.extend(ephemeral.unwrap_or_default());
        Ok(out)
    }

    async fn write_update(
        &self,
        id: &str,
        data: Object,
        options: UpdateOptions,
        scope: Option<&ChannelHandle>,
    ) -> Result<Row> {
        let Some(channels) = self.active_channels() else {
            return self.update_record(id, data, options).await;
        };
        let table = table_of(id);
        let (persistent, ephemeral) = channels.split(table, &data);
        channels.local(table, id, &persistent, ephemeral.as_ref(), scope);
        if ephemeral.is_some() && persistent.is_empty() {
            let mut out = data;
            out.insert("id".into(), Value::from(id));
            return Ok(out);
        }
        let mut out = self.update_record(id, persistent, options).await?;
        out.extend(ephemeral.unwrap_or_default());
        Ok(out)
    }

    async fn write_delete(&self, id: &str, scope: Option<&ChannelHandle>) -> Result<()> {
        // Awaited: the filter is checked against the row, which the delete
        // removes.
        if let Some(channels) = self.active_channels() {
            channels.local_delete(table_of(id), id, scope).await;
        }
        self.delete_record(id).await
    }
}

/// An open peer to peer channel ([`Engine::channel`]): its state, plus writes
/// that go to ITS peers only (when the row matches its filter). Plain
/// `create` / `update` / `delete` reach the peers of every open channel whose
/// filter the row matches.
#[derive(Clone)]
pub struct Sp00kyChannel {
    engine: Weak<Engine>,
    manager: ChannelManager,
    handle: ChannelHandle,
}

impl Sp00kyChannel {
    fn engine(&self) -> Result<Rc<Engine>> {
        self.engine.upgrade().ok_or(Error::Closed)
    }

    /// The manager-level handle.
    pub fn handle(&self) -> &ChannelHandle {
        &self.handle
    }

    /// The tables its queries read.
    pub fn tables(&self) -> Vec<String> {
        self.handle.tables()
    }

    pub fn state(&self) -> ChannelState {
        self.handle.state()
    }

    /// Called at once with the current state, then on every change.
    pub fn subscribe(&self, cb: impl Fn(&ChannelState) + 'static) -> Subscription {
        self.handle.subscribe(cb)
    }

    /// Replace who it connects to (your own id is skipped).
    pub fn set_peers<P: Into<Value>>(&self, peers: impl IntoIterator<Item = P>) {
        self.handle
            .set_peers(peers.into_iter().map(Into::into).collect());
    }

    /// Replace the queries (their filters changed).
    pub fn set_queries(&self, queries: &[QueryBuilder]) -> Result<()> {
        let scopes = queries
            .iter()
            .map(|q| self.manager.scope(q))
            .collect::<Result<Vec<_>>>()?;
        self.handle.set_scopes(scopes)
    }

    pub fn is_closed(&self) -> bool {
        self.handle.is_closed()
    }

    pub fn close(&self) {
        self.handle.close();
    }

    pub async fn create(&self, id: &str, data: Object) -> Result<Row> {
        self.manager.assert_carries(&self.handle, table_of(id))?;
        self.engine()?
            .write_create(id, data, Some(&self.handle))
            .await
    }

    pub async fn update(&self, id: &str, data: Object, options: UpdateOptions) -> Result<Row> {
        self.manager.assert_carries(&self.handle, table_of(id))?;
        self.engine()?
            .write_update(id, data, options, Some(&self.handle))
            .await
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        self.manager.assert_carries(&self.handle, table_of(id))?;
        self.engine()?.write_delete(id, Some(&self.handle)).await
    }
}

// ---- the thread-safe client ------------------------------------------------------------

static NEXT_CHANNEL: AtomicU64 = AtomicU64::new(1);

impl Sp00kyClient {
    /// Open a peer to peer channel (see [`Engine::channel`]). The queries are
    /// compiled here, so a query a channel cannot carry fails before anything
    /// reaches the engine.
    pub fn channel<P: Into<Value>>(
        &self,
        queries: &[QueryBuilder],
        peers: impl IntoIterator<Item = P>,
    ) -> impl Future<Output = Result<Channel>> + Send + 'static {
        let prepared: Result<Vec<PreparedScope>> = queries
            .iter()
            .map(|q| prepare_scope(self.schema(), q))
            .collect();
        let peers: Vec<Value> = peers.into_iter().map(Into::into).collect();
        let client = self.clone();
        async move {
            let prepared = prepared?;
            let id = NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed);
            let (tables, state) = client
                .call(move |w| async move { w.engine.open_shared_channel(id, prepared, peers) })
                .await?;
            Ok(Channel {
                id,
                tables,
                state,
                closed: AtomicBool::new(false),
                client,
            })
        }
    }

    /// A channel peer created a row (diagnostics and tests; see
    /// [`Engine::peer_create`]).
    pub async fn peer_create(&self, id: &str, row: impl Into<Object>, ttl_ms: u64) -> Result<bool> {
        let (id, row) = (id.to_string(), row.into());
        self.call(move |w| async move { w.engine.peer_create(&id, row, ttl_ms).await })
            .await
    }

    /// A channel peer deleted a row (see [`Engine::peer_delete`]).
    pub async fn peer_delete(&self, id: &str, ttl_ms: u64) -> Result<bool> {
        let id = id.to_string();
        self.call(move |w| async move { w.engine.peer_delete(&id, ttl_ms).await })
            .await
    }
}

/// An open peer to peer channel of a [`Sp00kyClient`]: its state, plus writes
/// that go to its peers only. Closes when dropped.
pub struct Channel {
    id: u64,
    tables: Vec<String>,
    state: watch::Receiver<ChannelState>,
    closed: AtomicBool,
    client: Sp00kyClient,
}

impl std::fmt::Debug for Channel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Channel")
            .field("tables", &self.tables)
            .field("state", &*self.state.borrow())
            .finish()
    }
}

impl Channel {
    /// The tables its queries read.
    pub fn tables(&self) -> &[String] {
        &self.tables
    }

    pub fn state(&self) -> ChannelState {
        self.state.borrow().clone()
    }

    pub fn state_watch(&self) -> watch::Receiver<ChannelState> {
        self.state.clone()
    }

    /// Wait for the next state change; `Err(Closed)` once the client is gone.
    pub async fn changed(&mut self) -> Result<ChannelState> {
        self.state.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.state.borrow_and_update().clone())
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn close(&self) {
        if self.closed.swap(true, Ordering::Relaxed) {
            return;
        }
        let id = self.id;
        self.client.fire(move |w| w.engine.close_shared_channel(id));
    }

    /// Replace who it connects to (your own id is skipped).
    pub fn set_peers<P: Into<Value>>(&self, peers: impl IntoIterator<Item = P>) {
        let peers: Vec<Value> = peers.into_iter().map(Into::into).collect();
        let id = self.id;
        self.client.fire(move |w| {
            if let Ok(c) = w.engine.shared_channel(id) {
                c.handle.set_peers(peers);
            }
        });
    }

    /// Replace the queries (their filters changed).
    pub fn set_queries(
        &self,
        queries: &[QueryBuilder],
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let prepared: Result<Vec<PreparedScope>> = queries
            .iter()
            .map(|q| prepare_scope(self.client.schema(), q))
            .collect();
        let (client, id) = (self.client.clone(), self.id);
        async move {
            let prepared = prepared?;
            client
                .call(move |w| async move {
                    let c = w.engine.shared_channel(id)?;
                    let scopes = prepared.into_iter().map(|p| c.manager.adopt(p)).collect();
                    c.handle.set_scopes(scopes)
                })
                .await
        }
    }

    /// Create `id` and send it to this channel's peers only.
    pub async fn create(&self, id: &str, data: impl Into<Object>) -> Result<Row> {
        let (cid, id, data) = (self.id, id.to_string(), data.into());
        self.client
            .call(move |w| async move { w.engine.shared_channel(cid)?.create(&id, data).await })
            .await
    }

    /// Update `id` and send it to this channel's peers only.
    pub async fn update(&self, id: &str, data: impl Into<Object>) -> Result<Row> {
        self.update_with(id, data, UpdateOptions::default()).await
    }

    pub async fn update_with(
        &self,
        id: &str,
        data: impl Into<Object>,
        options: UpdateOptions,
    ) -> Result<Row> {
        let (cid, id, data) = (self.id, id.to_string(), data.into());
        self.client
            .call(move |w| async move {
                w.engine
                    .shared_channel(cid)?
                    .update(&id, data, options)
                    .await
            })
            .await
    }

    /// Delete `id` and tell this channel's peers only.
    pub async fn delete(&self, id: &str) -> Result<()> {
        let (cid, id) = (self.id, id.to_string());
        self.client
            .call(move |w| async move { w.engine.shared_channel(cid)?.delete(&id).await })
            .await
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;
