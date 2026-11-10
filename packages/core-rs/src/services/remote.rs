//! Remote SurrealDB access: [`RemoteDatabaseService`] (lanes, gates, session
//! tracking) and [`RemoteAdapter`], the [`RemotePort`] the runtime drives.
//!
//! Single-threaded like the engine: every call spawns onto the current tokio
//! `LocalSet`, so it must run inside one.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::{FutureExt, LocalBoxFuture, Shared};
use tokio::sync::{broadcast, oneshot, Semaphore};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::kernel::ports::{LiveCallback, RemotePort, StatementResult};
use crate::query::sql::list_ref_live;
use crate::surreal::rpc::{LiveStream, RemoteSurrealClient, TransportEvent};
use crate::types::{DatabaseConfig, InlineRow};
use crate::value::{key_of, Object, Value};

/// A one-shot latch: closed until opened, then open for good.
struct Gate {
    tx: RefCell<Option<oneshot::Sender<()>>>,
    rx: Shared<oneshot::Receiver<()>>,
}

impl Gate {
    fn new() -> Rc<Gate> {
        let (tx, rx) = oneshot::channel();
        Rc::new(Gate {
            tx: RefCell::new(Some(tx)),
            rx: rx.shared(),
        })
    }

    fn is_open(&self) -> bool {
        self.tx.borrow().is_none()
    }

    fn open(&self) {
        if let Some(tx) = self.tx.borrow_mut().take() {
            let _ = tx.send(());
        }
    }

    /// Resolves once open (a dropped sender counts as open).
    async fn wait(&self) {
        let _ = self.rx.clone().await;
    }
}

#[derive(Clone, Copy)]
enum Lane {
    Read,
    Write,
    Bodies,
    Interactive,
}

/// Body fetches in flight at once. Matches
/// [`crate::kernel::constants::FETCH_ROUND_CHUNKS`].
pub const BODY_LANE_CAPACITY: usize = 4;

/// One-shot reads a screen is waiting on, in flight at once.
pub const INTERACTIVE_LANE_CAPACITY: usize = 3;

/// The heartbeat statement, sent through both lanes.
const PROBE_SQL: &str = "RETURN true";

/// How many `session::id()`s are remembered (see
/// [`RemoteDatabaseService::session_ids`]).
const MAX_SESSION_IDS: usize = 16;

struct Inner {
    config: DatabaseConfig,
    client: Rc<dyn RemoteSurrealClient>,
    reads: Semaphore,
    writes: Semaphore,
    bodies: Semaphore,
    interactive: Semaphore,

    /// The token the session last authenticated with, re-applied by `connect`
    /// so a socket rebuilt from scratch comes back authenticated.
    ///
    /// Without this a reconnect left the client reporting a signed-in user
    /// while its session was anonymous, and every view registered afterwards
    /// was stamped with an empty identity.
    auth_token: RefCell<Option<String>>,
    auth_epoch: Cell<u64>,
    transition: Cell<u64>,
    identity_gate: RefCell<Option<Rc<Gate>>>,

    /// Opens when the current connect attempt has finished, successfully or
    /// not. Every RPC waits on it.
    ///
    /// The engine's boot is local-first: it returns before the network half
    /// has run, so `use(ns, db)` and `authenticate(token)` have NOT happened
    /// yet when the app makes its first call. A signup issued in that window
    /// reaches the server with no namespace and fails as "There was a problem
    /// with signing up". Re-armed on every connect, so an RPC issued during a
    /// reconnect waits for the new socket's handshake rather than riding the
    /// dead one.
    connect_gate: RefCell<Option<Rc<Gate>>>,
    connect_gate_timeout: Cell<Duration>,

    /// How long `use` and `authenticate` get on a socket that just opened.
    /// Neither has a deadline of its own, and a socket that opened and then
    /// went quiet would otherwise hold the boot's connect, and every RPC
    /// behind it, forever.
    handshake_timeout: Cell<Duration>,

    session_ids: RefCell<Vec<String>>,
    on_handshake: RefCell<Option<Rc<dyn Fn()>>>,
}

impl Inner {
    fn check_epoch(&self, epoch: u64) -> Result<()> {
        if epoch == self.auth_epoch.get() {
            Ok(())
        } else {
            Err(Error::AccountChanged)
        }
    }

    async fn await_identity(&self) {
        let gate = self.identity_gate.borrow().clone();
        if let Some(gate) = gate {
            gate.wait().await;
        }
    }

    async fn await_connect(&self) {
        let gate = self.connect_gate.borrow().clone();
        let Some(gate) = gate.filter(|g| !g.is_open()) else {
            return;
        };
        let timeout = self.connect_gate_timeout.get();
        if tokio::time::timeout(timeout, gate.wait()).await.is_err() {
            tracing::warn!(
                target: "spooky",
                "Proceeding without a connected socket: no handshake within {}s",
                timeout.as_secs()
            );
        }
    }

    fn open_connect_gate(&self) {
        if let Some(gate) = self.connect_gate.borrow().as_ref() {
            gate.open();
        }
    }

    fn lane(&self, lane: Lane) -> &Semaphore {
        match lane {
            Lane::Read => &self.reads,
            Lane::Write => &self.writes,
            Lane::Bodies => &self.bodies,
            Lane::Interactive => &self.interactive,
        }
    }
}

/// Opens the connect gate when a connect attempt ends, including one whose
/// future was dropped half way: a gate left closed would hold every RPC for
/// the full gate timeout.
struct AttemptGuard(Rc<Inner>);

impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.0.open_connect_gate();
    }
}

/// A task's output, a panic re-raised in the caller.
async fn joined<T>(handle: JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(out) => out,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(_) => Err(Error::Closed),
    }
}

type ClientCall<T> =
    Box<dyn FnOnce(Rc<dyn RemoteSurrealClient>) -> LocalBoxFuture<'static, Result<T>>>;

/// Remote SurrealDB access over a [`RemoteSurrealClient`]. Cheap to clone; all
/// clones share one socket, lanes and gates.
///
/// Requests take slots, like the TS `AbstractDatabaseService` slot limit, but
/// in two lanes where TS has one pool. Reads share a FIFO pool of
/// [`DatabaseConfig::max_concurrent_queries`]: a SurrealDB socket multiplexes
/// requests by id, and running them one at a time capped the client at one
/// request per round trip, so at mobile RTT a startup's registrations arrived
/// ~250 ms apart and a screen opened afterwards waited behind all of them.
/// Writes ([`RemoteDatabaseService::write_statements`], the outbox push) run
/// one at a time in issue order, so one push never overlaps another.
///
/// Row bodies ([`RemoteDatabaseService::body_statements`]) and reads a screen
/// is waiting on ([`RemoteDatabaseService::query_interactive`]) have small
/// pools of their own. A registration can spend seconds on the server building
/// an index, and a launch sends dozens: in one shared queue six slow ones held
/// back the row the user had just been notified about, and the search box.
///
/// A request starts when it is made, not when its future is first polled, and
/// holds its slot until the RPC settles even when its caller has stopped
/// waiting: a slot is only free when the server is done with it.
#[derive(Clone)]
pub struct RemoteDatabaseService(Rc<Inner>);

impl RemoteDatabaseService {
    pub fn new(config: DatabaseConfig, client: Rc<dyn RemoteSurrealClient>) -> Self {
        let pool = config.max_concurrent_queries.max(1);
        RemoteDatabaseService(Rc::new(Inner {
            config,
            client,
            reads: Semaphore::new(pool),
            writes: Semaphore::new(1),
            bodies: Semaphore::new(BODY_LANE_CAPACITY),
            interactive: Semaphore::new(INTERACTIVE_LANE_CAPACITY),
            auth_token: RefCell::new(None),
            auth_epoch: Cell::new(0),
            transition: Cell::new(0),
            identity_gate: RefCell::new(None),
            connect_gate: RefCell::new(None),
            connect_gate_timeout: Cell::new(Duration::from_secs(20)),
            handshake_timeout: Cell::new(Duration::from_secs(10)),
            session_ids: RefCell::new(Vec::new()),
            on_handshake: RefCell::new(None),
        }))
    }

    pub fn config(&self) -> &DatabaseConfig {
        &self.0.config
    }

    pub fn client(&self) -> Rc<dyn RemoteSurrealClient> {
        self.0.client.clone()
    }

    /// The socket's lifecycle events, for the supervisor.
    pub fn events(&self) -> broadcast::Receiver<TransportEvent> {
        self.0.client.events()
    }

    // ---- session transitions -----------------------------------------------------

    /// Hold every lane until [`RemoteDatabaseService::end_session_transition`]
    /// with the returned id, and discard what is in flight for the outgoing
    /// identity.
    pub fn begin_session_transition(&self) -> u64 {
        let inner = &self.0;
        inner.auth_epoch.set(inner.auth_epoch.get() + 1);
        let mut gate = inner.identity_gate.borrow_mut();
        if gate.as_ref().is_none_or(|g| g.is_open()) {
            *gate = Some(Gate::new());
        }
        inner.transition.set(inner.transition.get() + 1);
        inner.transition.get()
    }

    /// Release the lanes, if `transition` is still the latest one.
    pub fn end_session_transition(&self, transition: u64) {
        let inner = &self.0;
        if transition != inner.transition.get() {
            return;
        }
        if let Some(gate) = inner.identity_gate.borrow().as_ref() {
            gate.open();
        }
    }

    /// The token a reconnect authenticates with. A change discards what is in
    /// flight for the previous identity.
    pub fn set_auth_token(&self, token: Option<String>) {
        let inner = &self.0;
        if *inner.auth_token.borrow() != token {
            inner.auth_epoch.set(inner.auth_epoch.get() + 1);
        }
        *inner.auth_token.borrow_mut() = token;
    }

    /// Called after every connect attempt, success or not (the client wires
    /// auth verification here).
    pub fn set_on_handshake(&self, callback: Option<Rc<dyn Fn()>>) {
        *self.0.on_handshake.borrow_mut() = callback;
    }

    /// Every `session::id()` this client's sockets have had, oldest first.
    ///
    /// Each supervisor reconnect is a new session. A view registered before it
    /// stays registered, so its edges keep naming the socket that registered
    /// them, and the anonymous LIVE filters on these ids (see `list_ref_live`).
    /// Capped: the SSP restamps an edge whenever its row changes, so an id that
    /// falls off costs at most the LIVE removal of a row untouched since, which
    /// the membership poll still catches.
    pub fn session_ids(&self) -> Vec<String> {
        self.0.session_ids.borrow().clone()
    }

    // ---- connection --------------------------------------------------------------

    /// How long an RPC waits for a connect before going ahead anyway. Going
    /// ahead fails it as a network error, which the outbox and the
    /// registrations retry; waiting forever would wedge them instead.
    pub fn set_connect_gate_timeout(&self, timeout: Duration) {
        self.0.connect_gate_timeout.set(timeout);
    }

    /// How long `use` and `authenticate` get on a freshly opened socket before
    /// the connect fails and the socket is dropped. Default 10 s.
    pub fn set_handshake_timeout(&self, timeout: Duration) {
        self.0.handshake_timeout.set(timeout);
    }

    /// Hold RPCs until the first connect attempt finishes. Called before the
    /// engine boots, so a call made before [`RemoteDatabaseService::connect`]
    /// still waits for it.
    pub fn arm_connect_gate(&self) {
        let mut gate = self.0.connect_gate.borrow_mut();
        if gate.is_none() {
            *gate = Some(Gate::new());
        }
    }

    /// True while a socket is open, for the supervisor.
    pub fn is_connected(&self) -> bool {
        self.0.client.is_connected()
    }

    /// Drop the socket without disposing the client, so the supervisor can
    /// force the close a half-open connection never delivers.
    pub async fn force_close(&self) {
        self.0.client.force_close().await;
    }

    /// Open the socket, select the namespace and database, and re-apply the
    /// session's token. Releases every held RPC when it ends, either way.
    pub async fn connect(&self) -> Result<()> {
        let inner = &self.0;
        let Some(endpoint) = inner.config.endpoint.clone() else {
            tracing::warn!(target: "spooky", "No endpoint configured for remote database");
            inner.open_connect_gate();
            return Ok(());
        };
        {
            let mut gate = inner.connect_gate.borrow_mut();
            if gate.as_ref().is_none_or(|g| g.is_open()) {
                *gate = Some(Gate::new());
            }
        }
        let guard = AttemptGuard(inner.clone());
        let out = self.handshake(&endpoint).await;
        // The attempt is over: held RPCs stop waiting.
        drop(guard);
        let weak = Rc::downgrade(inner);
        tokio::task::spawn_local(async move {
            let callback = weak.upgrade().and_then(|i| i.on_handshake.borrow().clone());
            if let Some(callback) = callback {
                callback();
            }
        });
        out
    }

    async fn handshake(&self, endpoint: &str) -> Result<()> {
        let inner = &self.0;
        inner.client.connect(endpoint).await?;
        let timeout = inner.handshake_timeout.get();
        match tokio::time::timeout(timeout, self.use_and_authenticate()).await {
            Ok(out) => out?,
            Err(_) => {
                self.force_close().await;
                return Err(Error::Timeout(timeout.as_millis() as u64));
            }
        }
        tokio::task::spawn_local(record_session_id(inner.clone()));
        tracing::info!(target: "spooky", "Connected to remote database");
        Ok(())
    }

    async fn use_and_authenticate(&self) -> Result<()> {
        let inner = &self.0;
        let client = inner.client.clone();
        client
            .use_db(&inner.config.namespace, &inner.config.database)
            .await?;
        let epoch = inner.auth_epoch.get();
        let token = inner
            .auth_token
            .borrow()
            .clone()
            .or_else(|| inner.config.token.clone());
        if let Some(token) = token {
            client.authenticate(&token).await?;
        }
        if epoch != inner.auth_epoch.get() {
            self.force_close().await;
            inner.check_epoch(epoch)?;
        }
        Ok(())
    }

    /// Release every waiter and dispose the client.
    pub async fn close(&self) {
        self.end_session_transition(self.0.transition.get());
        // Never leave an RPC waiting on a client that is gone.
        self.0.open_connect_gate();
        self.0.client.close().await;
    }

    // ---- lanes -------------------------------------------------------------------

    /// Run `rpc` in `lane` behind the identity and connect gates.
    ///
    /// The epoch is taken when the call is made, not when its slot comes up: a
    /// request issued under one account must not run, or answer, under the
    /// next.
    fn run<T: 'static>(
        &self,
        lane: Lane,
        rpc: ClientCall<T>,
    ) -> impl Future<Output = Result<T>> + 'static {
        let inner = self.0.clone();
        let epoch = inner.auth_epoch.get();
        let task = tokio::task::spawn_local(async move {
            let _slot = inner
                .lane(lane)
                .acquire()
                .await
                .map_err(|_| Error::Closed)?;
            inner.await_identity().await;
            inner.await_connect().await;
            inner.check_epoch(epoch)?;
            let out = rpc(inner.client.clone()).await?;
            inner.check_epoch(epoch)?;
            Ok(out)
        });
        joined(task)
    }

    /// A read, in the shared pool. Fails on a failed statement.
    pub fn query(
        &self,
        sql: &str,
        vars: Option<Object>,
    ) -> impl Future<Output = Result<Vec<Value>>> + 'static {
        let sql = sql.to_string();
        self.run(
            Lane::Read,
            Box::new(move |c| async move { c.query(&sql, vars).await }.boxed_local()),
        )
    }

    /// Per-statement outcomes of a read, in the shared pool.
    pub fn query_statements(
        &self,
        sql: &str,
        vars: Option<Object>,
    ) -> impl Future<Output = Result<Vec<StatementResult>>> + 'static {
        self.statements(Lane::Read, sql, vars)
    }

    /// A read a screen is waiting on, in its own small pool: it never queues
    /// behind the sync's registrations and body fetches. Fails on a failed
    /// statement.
    pub fn query_interactive(
        &self,
        sql: &str,
        vars: Option<Object>,
    ) -> impl Future<Output = Result<Vec<Value>>> + 'static {
        let sql = sql.to_string();
        self.run(
            Lane::Interactive,
            Box::new(move |c| async move { c.query(&sql, vars).await }.boxed_local()),
        )
    }

    /// Per-statement outcomes of a row body fetch, in its own pool.
    pub fn body_statements(
        &self,
        sql: &str,
        vars: Option<Object>,
    ) -> impl Future<Output = Result<Vec<StatementResult>>> + 'static {
        self.statements(Lane::Bodies, sql, vars)
    }

    /// Per-statement outcomes of a write, in the write lane: it starts only
    /// once every write issued before it has settled.
    pub fn write_statements(
        &self,
        sql: &str,
        vars: Option<Object>,
    ) -> impl Future<Output = Result<Vec<StatementResult>>> + 'static {
        self.statements(Lane::Write, sql, vars)
    }

    fn statements(
        &self,
        lane: Lane,
        sql: &str,
        vars: Option<Object>,
    ) -> impl Future<Output = Result<Vec<StatementResult>>> + 'static {
        let sql = sql.to_string();
        self.run(
            lane,
            Box::new(move |c| async move { c.query_statements(&sql, vars).await }.boxed_local()),
        )
    }

    /// The supervisor's heartbeat: one `RETURN true` through each lane.
    ///
    /// A lane held by an RPC that never settles fails it, so the supervisor
    /// tears the socket down and the close fails what was stuck. A write lane
    /// wedged that way would otherwise stall the outbox while every read still
    /// answered. One hung read does not fail it: the rest of the pool still
    /// moves, and a pool with every slot hung does.
    pub fn probe(&self) -> impl Future<Output = Result<()>> + 'static {
        let read = self.query(PROBE_SQL, None);
        let write = self.run(
            Lane::Write,
            Box::new(|c| async move { c.query(PROBE_SQL, None).await }.boxed_local()),
        );
        async move {
            let (read, write) = futures::future::join(read, write).await;
            read?;
            write?;
            Ok(())
        }
    }

    /// One `RETURN true` on the socket itself, outside every lane: it asks
    /// whether the socket works, and a busy lane cannot fail it.
    pub fn probe_socket(&self) -> impl Future<Output = Result<()>> + 'static {
        let inner = self.0.clone();
        async move {
            inner.await_connect().await;
            inner.client.query(PROBE_SQL, None).await.map(|_| ())
        }
    }

    // ---- live --------------------------------------------------------------------

    /// Gated LIVE subscribe. A LIVE issued before the handshake is rejected
    /// with "Specify a namespace to use" and then never retried, so the
    /// down-path would run on the poll alone until the next reconnect.
    pub async fn live(&self, sql: &str, vars: Option<Object>) -> Result<(String, LiveStream)> {
        let inner = &self.0;
        let epoch = inner.auth_epoch.get();
        inner.await_identity().await;
        inner.await_connect().await;
        inner.check_epoch(epoch)?;
        let out = inner.client.live(sql, vars).await?;
        inner.check_epoch(epoch)?;
        Ok(out)
    }

    pub async fn kill(&self, live_id: &str) -> Result<()> {
        self.0.await_connect().await;
        self.0.client.kill(live_id).await
    }

    // ---- auth --------------------------------------------------------------------

    /// An auth RPC: waits for the handshake (`signin`/`signup` carry the
    /// namespace and database the `use` call established, and reach the server
    /// without them if they run first), not for a lane or the identity gate.
    fn auth_rpc<T: 'static>(
        &self,
        rpc: ClientCall<T>,
    ) -> impl Future<Output = Result<T>> + 'static {
        let service = self.clone();
        let epoch = self.0.auth_epoch.get();
        let task = tokio::task::spawn_local(async move {
            let inner = &service.0;
            inner.await_connect().await;
            inner.check_epoch(epoch)?;
            let out = rpc(inner.client.clone()).await;
            // A late auth RPC can change the server socket's identity even when
            // its result is discarded locally. Reconnect with the current token.
            if epoch != inner.auth_epoch.get() {
                service.force_close().await;
            }
            let out = out?;
            inner.check_epoch(epoch)?;
            Ok(out)
        });
        joined(task)
    }

    pub fn signin(&self, params: Object) -> impl Future<Output = Result<Value>> + 'static {
        self.auth_rpc(Box::new(move |c| {
            async move { c.signin(params).await }.boxed_local()
        }))
    }

    pub fn signup(&self, params: Object) -> impl Future<Output = Result<Value>> + 'static {
        self.auth_rpc(Box::new(move |c| {
            async move { c.signup(params).await }.boxed_local()
        }))
    }

    pub fn authenticate(&self, token: &str) -> impl Future<Output = Result<Value>> + 'static {
        let token = token.to_string();
        self.auth_rpc(Box::new(move |c| {
            async move { c.authenticate(&token).await }.boxed_local()
        }))
    }

    pub fn invalidate(&self) -> impl Future<Output = Result<()>> + 'static {
        self.auth_rpc(Box::new(|c| {
            async move { c.invalidate().await }.boxed_local()
        }))
    }

    /// The signed-in user's row. Bypasses the lanes: every slot may be waiting
    /// for this very transition to finish. It cannot write account cache data.
    pub async fn query_auth_user(&self) -> Result<Vec<Value>> {
        self.0.await_connect().await;
        self.0
            .client
            .query("SELECT * FROM ONLY $auth.id", None)
            .await
    }
}

/// Ask the socket that just connected for its session id. Straight to the
/// client: it needs no identity, so it need not wait for a slot.
async fn record_session_id(inner: Rc<Inner>) {
    // On failure the socket now open is still matched by `session::id()` itself.
    let Ok(result) = inner
        .client
        .query("RETURN <string>session::id()", None)
        .await
    else {
        return;
    };
    let Some(Value::String(sid)) = result.into_iter().next() else {
        return;
    };
    let mut ids = inner.session_ids.borrow_mut();
    if ids.contains(&sid) {
        return;
    }
    ids.push(sid);
    if ids.len() > MAX_SESSION_IDS {
        ids.remove(0);
    }
}

// ---- the port ----------------------------------------------------------------------

/// The remote socket, as the runtime sees it.
pub struct RemoteAdapter {
    remote: RemoteDatabaseService,
    inline_bodies: bool,
    live_task: RefCell<Option<JoinHandle<()>>>,
}

impl RemoteAdapter {
    pub fn new(remote: RemoteDatabaseService, inline_bodies: bool) -> Self {
        RemoteAdapter {
            remote,
            inline_bodies,
            live_task: RefCell::new(None),
        }
    }

    pub fn remote(&self) -> &RemoteDatabaseService {
        &self.remote
    }

    /// Stop forwarding LIVE notifications.
    pub fn dispose(&self) {
        if let Some(task) = self.live_task.borrow_mut().take() {
            task.abort();
        }
    }
}

impl Drop for RemoteAdapter {
    fn drop(&mut self) {
        self.dispose();
    }
}

#[async_trait(?Send)]
impl RemotePort for RemoteAdapter {
    async fn query(&self, sql: &str, vars: Option<Object>) -> Result<Vec<StatementResult>> {
        self.remote.query_statements(sql, vars).await
    }

    async fn bodies(&self, sql: &str, vars: Option<Object>) -> Result<Vec<StatementResult>> {
        self.remote.body_statements(sql, vars).await
    }

    async fn write(&self, sql: &str, vars: Option<Object>) -> Result<Vec<StatementResult>> {
        self.remote.write_statements(sql, vars).await
    }

    async fn live(&self, table: &str, on_change: LiveCallback) -> Result<String> {
        // `FETCH out` resolves the edge's target into the notification, so the
        // body arrives with the doorbell instead of costing a second round
        // trip. The clause is only ever added, never relied on: a server that
        // ignores it, or a row the session cannot read, simply yields no inline
        // row and the fetch path takes over.
        let (sql, vars) = list_ref_live(table, self.inline_bodies, &self.remote.session_ids());
        let (uuid, mut stream) = self.remote.live(&sql, vars).await?;
        let task = tokio::task::spawn_local(async move {
            while let Some(message) = stream.recv().await {
                if message.action == "KILLED" {
                    continue;
                }
                let Some(hash) = hash_of_edge(&message.value) else {
                    continue;
                };
                // Not on DELETE: that edge is a row leaving the view, and for a
                // row that was actually deleted the join yields nothing anyway.
                // Landing a body there would only ever write back what is going
                // away.
                let row = if message.action == "DELETE" {
                    None
                } else {
                    row_of_edge(&message.value)
                };
                on_change(vec![hash], row.map(|r| vec![r]));
            }
        });
        if let Some(old) = self.live_task.borrow_mut().replace(task) {
            old.abort();
        }
        Ok(uuid)
    }

    async fn kill(&self, uuid: &str) -> Result<()> {
        self.dispose();
        self.remote.kill(uuid).await
    }
}

/// The hash a `_00_list_ref` edge belongs to: the id part of its `in`
/// (`_00_query:<hash>`).
pub fn hash_of_edge(value: &Object) -> Option<String> {
    let in_id = value.get("in").filter(|v| !v.is_nullish())?.to_id_string();
    let hash = key_of(&in_id);
    (!hash.is_empty()).then(|| hash.to_string())
}

/// The row an edge notification carries, when the subscription joined it on.
///
/// Without `FETCH out` the edge's `out` is a record-id string and there is
/// nothing to land, so this returns `None` and the caller falls back to
/// fetching the body. It also returns `None` for a row the session may not
/// read (the join yields `out: null`) and for an edge whose target has been
/// deleted.
pub fn row_of_edge(value: &Object) -> Option<InlineRow> {
    let Some(Value::Object(out)) = value.get("out") else {
        return None;
    };
    // The landing path keys off `row.id`; without one it would record a version
    // for a body it never wrote.
    let id = out.get("id").filter(|v| !v.is_nullish())?;
    let version = match value.get("version")? {
        Value::Int(i) => *i,
        Value::Float(f) if f.is_finite() => f.trunc() as i64,
        _ => return None,
    };
    Some(InlineRow {
        id: id.to_id_string(),
        version,
        record: out.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tokio::sync::mpsc;

    use super::*;
    use crate::modules::ref_tables::ANON_LIST_REF_TABLE;
    use crate::surreal::rpc::LiveMessage;
    use crate::testing::run_local;
    use crate::value::object_from_json;

    /// Let every ready local task run to its next await.
    async fn pump() {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    fn config(pool: usize) -> DatabaseConfig {
        let mut c = DatabaseConfig::new("main", "main").endpoint("ws://example.invalid");
        c.max_concurrent_queries = pool;
        c
    }

    fn access(name: &str) -> Object {
        object_from_json(serde_json::json!({"access": name, "variables": {}}))
    }

    // ---- connect gate --------------------------------------------------------------

    /// Records the order of RPCs and lets the test hold `connect` open, so the
    /// window between "boot returned" and "the handshake finished" is explicit.
    struct SlowRemote {
        calls: RefCell<Vec<String>>,
        release: Shared<oneshot::Receiver<()>>,
        release_tx: RefCell<Option<oneshot::Sender<()>>>,
        connect_fails: bool,
        /// `use` never answers: a socket that opened and then went quiet.
        use_hangs: Cell<bool>,
    }

    impl SlowRemote {
        fn new(connect_fails: bool) -> Rc<Self> {
            let (tx, rx) = oneshot::channel();
            Rc::new(SlowRemote {
                calls: RefCell::default(),
                release: rx.shared(),
                release_tx: RefCell::new(Some(tx)),
                connect_fails,
                use_hangs: Cell::new(false),
            })
        }

        fn release_connect(&self) {
            if let Some(tx) = self.release_tx.borrow_mut().take() {
                let _ = tx.send(());
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.borrow().clone()
        }

        fn log(&self, call: &str) {
            self.calls.borrow_mut().push(call.to_string());
        }
    }

    #[async_trait(?Send)]
    impl RemoteSurrealClient for SlowRemote {
        async fn connect(&self, _endpoint: &str) -> Result<()> {
            self.log("connect");
            if self.connect_fails {
                return Err(Error::Network("boom".into()));
            }
            let _ = self.release.clone().await;
            Ok(())
        }
        async fn use_db(&self, _: &str, _: &str) -> Result<()> {
            self.log("use");
            if self.use_hangs.get() {
                futures::future::pending::<()>().await;
            }
            Ok(())
        }
        async fn authenticate(&self, _: &str) -> Result<Value> {
            self.log("authenticate");
            Ok(Value::None)
        }
        async fn signin(&self, _: Object) -> Result<Value> {
            self.log("signin");
            Ok("token".into())
        }
        async fn signup(&self, _: Object) -> Result<Value> {
            if self.connect_fails {
                return Err(Error::NoRemote("not connected".into()));
            }
            self.log("signup");
            Ok("token".into())
        }
        async fn invalidate(&self) -> Result<()> {
            self.log("invalidate");
            Ok(())
        }
        async fn query(&self, _: &str, _: Option<Object>) -> Result<Vec<Value>> {
            self.log("query");
            Ok(vec![])
        }
        async fn query_statements(
            &self,
            _: &str,
            _: Option<Object>,
        ) -> Result<Vec<StatementResult>> {
            self.log("queryStatements");
            Ok(vec![])
        }
        async fn live(&self, _: &str, _: Option<Object>) -> Result<(String, LiveStream)> {
            Ok(("live".into(), mpsc::unbounded_channel().1))
        }
        async fn kill(&self, _: &str) -> Result<()> {
            Ok(())
        }
        async fn close(&self) {
            self.log("close");
        }
    }

    fn gated(remote: &Rc<SlowRemote>) -> RemoteDatabaseService {
        let service = RemoteDatabaseService::new(config(6), remote.clone());
        service.arm_connect_gate();
        service
    }

    #[tokio::test]
    async fn an_rpc_issued_before_the_handshake_waits_for_it() {
        run_local(async {
            // Boot is local-first: it returns before the network half has run,
            // so the app can call sign-up while `connect` is still in flight.
            // Without the gate the RPC reaches the server with no namespace
            // selected, which SurrealDB reports as "There was a problem with
            // signing up".
            let remote = SlowRemote::new(false);
            let service = gated(&remote);
            let s = service.clone();
            tokio::task::spawn_local(async move { s.connect().await });
            pump().await;
            assert_eq!(remote.calls(), ["connect"], "connect is still in flight");

            let signup = tokio::task::spawn_local(service.signup(access("account")));
            pump().await;
            assert!(!signup.is_finished());
            assert_eq!(
                remote.calls(),
                ["connect"],
                "signup must not ride a socket that has no namespace yet"
            );

            remote.release_connect();
            pump().await;
            assert!(signup.is_finished());
            assert_eq!(signup.await.unwrap().unwrap(), Value::from("token"));
            // `query` is the new socket reading its own `session::id()` (the
            // anonymous LIVE filters on it); it needs no namespace either way.
            assert_eq!(remote.calls(), ["connect", "use", "query", "signup"]);
        })
        .await;
    }

    #[tokio::test]
    async fn a_failed_connect_releases_the_waiters() {
        run_local(async {
            let remote = SlowRemote::new(true);
            let service = gated(&remote);
            assert!(service.connect().await.is_err());
            // The attempt is over: a held RPC goes ahead and fails as a network
            // error, which the outbox and the registrations retry. Waiting
            // forever would wedge them instead.
            assert!(service.signup(access("account")).await.is_err());
        })
        .await;
    }

    #[tokio::test]
    async fn close_releases_the_waiters() {
        run_local(async {
            let remote = SlowRemote::new(false);
            let service = gated(&remote);
            let s = service.clone();
            tokio::task::spawn_local(async move { s.connect().await });
            pump().await;
            let pending = service.signin(access("account"));
            service.close().await;
            assert_eq!(pending.await.unwrap(), Value::from("token"));
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_connect_that_never_finishes_holds_rpcs_only_for_the_gate_timeout() {
        run_local(async {
            let remote = SlowRemote::new(false);
            let service = gated(&remote);
            let s = service.clone();
            tokio::task::spawn_local(async move { s.connect().await });
            let signin = tokio::task::spawn_local(service.signin(access("account")));
            tokio::time::sleep(Duration::from_secs(19)).await;
            pump().await;
            assert!(!signin.is_finished());
            tokio::time::sleep(Duration::from_secs(1)).await;
            pump().await;
            assert!(signin.is_finished());
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_handshake_that_goes_quiet_fails_the_connect_and_opens_the_gate() {
        run_local(async {
            let remote = SlowRemote::new(false);
            remote.use_hangs.set(true);
            remote.release_connect();
            let service = gated(&remote);
            service.set_handshake_timeout(Duration::from_secs(10));
            let s = service.clone();
            let connect = tokio::task::spawn_local(async move { s.connect().await });
            let signin = tokio::task::spawn_local(service.signin(access("account")));
            tokio::time::sleep(Duration::from_millis(9_999)).await;
            pump().await;
            assert!(!connect.is_finished());
            tokio::time::sleep(Duration::from_millis(1)).await;
            pump().await;
            assert!(matches!(
                connect.await.unwrap(),
                Err(Error::Timeout(10_000))
            ));
            assert!(signin.is_finished(), "held RPCs stop waiting");
        })
        .await;
    }

    #[tokio::test]
    async fn a_dropped_connect_still_opens_the_gate() {
        run_local(async {
            let remote = SlowRemote::new(false);
            let service = gated(&remote);
            let s = service.clone();
            let connect = tokio::task::spawn_local(async move { s.connect().await });
            pump().await;
            let signin = tokio::task::spawn_local(service.signin(access("account")));
            connect.abort();
            pump().await;
            assert!(signin.is_finished());
        })
        .await;
    }

    #[tokio::test]
    async fn connect_replays_the_session_token_and_calls_on_handshake() {
        run_local(async {
            let remote = SlowRemote::new(false);
            remote.release_connect();
            let service = RemoteDatabaseService::new(config(6), remote.clone());
            service.set_auth_token(Some("tok".into()));
            let shaken = Rc::new(Cell::new(0));
            let s = shaken.clone();
            service.set_on_handshake(Some(Rc::new(move || s.set(s.get() + 1))));
            service.connect().await.unwrap();
            pump().await;
            assert_eq!(remote.calls(), ["connect", "use", "authenticate", "query"]);
            assert_eq!(shaken.get(), 1);
        })
        .await;
    }

    // ---- lanes ---------------------------------------------------------------------

    /// A socket that holds every request until the test answers it, so what is
    /// in flight at once is explicit. `RETURN` probes answer at once.
    #[derive(Default)]
    struct HeldRemote {
        started: RefCell<Vec<String>>,
        held: RefCell<HashMap<String, oneshot::Sender<Result<Vec<StatementResult>>>>>,
        in_flight: Cell<usize>,
        max_in_flight: Cell<usize>,
        session_id: RefCell<Option<String>>,
    }

    impl HeldRemote {
        async fn hold(&self, sql: &str) -> Result<Vec<StatementResult>> {
            self.started.borrow_mut().push(sql.to_string());
            if sql.starts_with("RETURN") {
                let sid = self.session_id.borrow().clone();
                return Ok(vec![StatementResult::ok(match sid {
                    Some(s) if sql.contains("session::id") => Value::String(s),
                    _ => Value::Bool(true),
                })]);
            }
            let (tx, rx) = oneshot::channel();
            self.held.borrow_mut().insert(sql.to_string(), tx);
            self.in_flight.set(self.in_flight.get() + 1);
            self.max_in_flight
                .set(self.max_in_flight.get().max(self.in_flight.get()));
            let out = rx
                .await
                .unwrap_or_else(|_| Err(Error::Network("dropped".into())));
            self.in_flight.set(self.in_flight.get() - 1);
            out
        }

        fn answer(&self, sql: &str) {
            let tx = self.held.borrow_mut().remove(sql).expect("not held");
            let _ = tx.send(Ok(vec![StatementResult::ok(sql)]));
        }

        fn fail(&self, sql: &str) {
            let tx = self.held.borrow_mut().remove(sql).expect("not held");
            let _ = tx.send(Err(Error::Network("socket closed".into())));
        }

        fn started(&self) -> Vec<String> {
            self.started.borrow().clone()
        }
    }

    #[async_trait(?Send)]
    impl RemoteSurrealClient for HeldRemote {
        async fn connect(&self, _: &str) -> Result<()> {
            Ok(())
        }
        async fn use_db(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        async fn authenticate(&self, _: &str) -> Result<Value> {
            Ok(Value::None)
        }
        async fn signin(&self, _: Object) -> Result<Value> {
            Ok("token".into())
        }
        async fn signup(&self, _: Object) -> Result<Value> {
            Ok("token".into())
        }
        async fn invalidate(&self) -> Result<()> {
            Ok(())
        }
        async fn query(&self, sql: &str, _: Option<Object>) -> Result<Vec<Value>> {
            Ok(self
                .hold(sql)
                .await?
                .into_iter()
                .map(|r| r.into_value().unwrap_or_default())
                .collect())
        }
        async fn query_statements(
            &self,
            sql: &str,
            _: Option<Object>,
        ) -> Result<Vec<StatementResult>> {
            self.hold(sql).await
        }
        async fn live(&self, _: &str, _: Option<Object>) -> Result<(String, LiveStream)> {
            Ok(("live".into(), mpsc::unbounded_channel().1))
        }
        async fn kill(&self, _: &str) -> Result<()> {
            Ok(())
        }
        async fn close(&self) {}
    }

    fn held(pool: usize) -> (Rc<HeldRemote>, RemoteDatabaseService) {
        let socket = Rc::new(HeldRemote::default());
        let service = RemoteDatabaseService::new(config(pool), socket.clone());
        (socket, service)
    }

    #[tokio::test]
    async fn reads_overlap() {
        run_local(async {
            let (socket, remote) = held(6);
            let a = tokio::task::spawn_local(remote.query_statements("SELECT * FROM a", None));
            let b = tokio::task::spawn_local(remote.query_statements("SELECT * FROM b", None));
            pump().await;
            assert_eq!(socket.started(), ["SELECT * FROM a", "SELECT * FROM b"]);
            assert_eq!(socket.in_flight.get(), 2);

            socket.answer("SELECT * FROM b");
            pump().await;
            assert!(b.is_finished(), "b does not wait for a");
            assert!(!a.is_finished());

            socket.answer("SELECT * FROM a");
            pump().await;
            let out = a.await.unwrap().unwrap();
            assert_eq!(out, vec![StatementResult::ok("SELECT * FROM a")]);
        })
        .await;
    }

    #[tokio::test]
    async fn reads_are_bounded_by_the_pool_and_admitted_in_order() {
        run_local(async {
            let (socket, remote) = held(2);
            let watches: Vec<_> = ["r1", "r2", "r3", "r4"]
                .iter()
                .map(|t| {
                    tokio::task::spawn_local(remote.query(&format!("SELECT * FROM {t}"), None))
                })
                .collect();
            pump().await;
            assert_eq!(socket.started(), ["SELECT * FROM r1", "SELECT * FROM r2"]);

            socket.answer("SELECT * FROM r2");
            pump().await;
            assert_eq!(socket.started().last().unwrap(), "SELECT * FROM r3");

            // A failed read gives its slot back too.
            socket.fail("SELECT * FROM r1");
            pump().await;
            assert!(watches[0].is_finished());
            assert_eq!(socket.started().last().unwrap(), "SELECT * FROM r4");

            socket.answer("SELECT * FROM r3");
            socket.answer("SELECT * FROM r4");
            pump().await;
            assert!(watches.iter().all(|w| w.is_finished()));
            assert_eq!(socket.max_in_flight.get(), 2);
            let mut watches = watches.into_iter();
            assert!(watches.next().unwrap().await.unwrap().is_err());
        })
        .await;
    }

    #[tokio::test]
    async fn bodies_interactive_reads_and_the_socket_probe_skip_held_registrations() {
        run_local(async {
            let (socket, remote) = held(2);
            // Every read slot held by a registration, more of them queued
            // behind, the way a server building an index for each does.
            for i in 0..4 {
                tokio::task::spawn_local(
                    remote.query_statements(&format!("fn::query::register({i})"), None),
                );
            }
            let read =
                tokio::task::spawn_local(remote.query_statements("SELECT * FROM read", None));
            let body = tokio::task::spawn_local(remote.body_statements("SELECT * FROM body", None));
            let search =
                tokio::task::spawn_local(remote.query_interactive("SELECT * FROM search", None));
            let probe = tokio::task::spawn_local(remote.probe_socket());
            pump().await;
            assert!(!socket.started().contains(&"SELECT * FROM read".to_string()));
            socket.answer("SELECT * FROM body");
            socket.answer("SELECT * FROM search");
            pump().await;
            assert!(body.is_finished(), "a body fetch does not wait");
            assert!(
                search.is_finished(),
                "a read a screen waits on does not wait"
            );
            probe.await.unwrap().unwrap();
            assert!(!read.is_finished(), "a shared-pool read does");
        })
        .await;
    }

    #[tokio::test]
    async fn reads_are_not_held_behind_a_write() {
        run_local(async {
            let (socket, remote) = held(6);
            let write =
                tokio::task::spawn_local(remote.write_statements("UPDATE thing:1 MERGE {}", None));
            let read =
                tokio::task::spawn_local(remote.query_statements("SELECT * FROM thing", None));
            pump().await;
            assert_eq!(
                socket.started(),
                ["UPDATE thing:1 MERGE {}", "SELECT * FROM thing"]
            );

            socket.answer("SELECT * FROM thing");
            pump().await;
            assert!(read.is_finished());
            assert!(!write.is_finished());
            socket.answer("UPDATE thing:1 MERGE {}");
        })
        .await;
    }

    #[tokio::test]
    async fn writes_never_overlap() {
        run_local(async {
            let (socket, remote) = held(6);
            let first =
                tokio::task::spawn_local(remote.write_statements("CREATE ONLY thing:1", None));
            let second =
                tokio::task::spawn_local(remote.write_statements("UPDATE thing:1 MERGE {}", None));
            pump().await;
            assert_eq!(socket.started(), ["CREATE ONLY thing:1"]);

            socket.answer("CREATE ONLY thing:1");
            pump().await;
            assert!(first.is_finished());
            assert_eq!(
                socket.started(),
                ["CREATE ONLY thing:1", "UPDATE thing:1 MERGE {}"]
            );

            socket.answer("UPDATE thing:1 MERGE {}");
            pump().await;
            assert!(second.is_finished());
            assert_eq!(socket.max_in_flight.get(), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn a_failed_write_releases_the_lane() {
        run_local(async {
            let (socket, remote) = held(6);
            let first =
                tokio::task::spawn_local(remote.write_statements("CREATE ONLY thing:1", None));
            let _second =
                tokio::task::spawn_local(remote.write_statements("CREATE ONLY thing:2", None));
            pump().await;

            socket.fail("CREATE ONLY thing:1");
            pump().await;
            assert!(first.await.unwrap().is_err());
            assert_eq!(socket.started().last().unwrap(), "CREATE ONLY thing:2");
            socket.answer("CREATE ONLY thing:2");
        })
        .await;
    }

    #[tokio::test]
    async fn a_caller_that_stops_waiting_keeps_its_slot_until_the_rpc_settles() {
        run_local(async {
            let (socket, remote) = held(1);
            drop(remote.query("SELECT * FROM a", None));
            let b = tokio::task::spawn_local(remote.query("SELECT * FROM b", None));
            pump().await;
            assert_eq!(
                socket.started(),
                ["SELECT * FROM a"],
                "a still runs and holds the slot"
            );
            socket.answer("SELECT * FROM a");
            pump().await;
            assert_eq!(socket.started().last().unwrap(), "SELECT * FROM b");
            socket.answer("SELECT * FROM b");
            pump().await;
            assert!(b.is_finished());
        })
        .await;
    }

    #[tokio::test]
    async fn every_lane_waits_out_an_account_transition() {
        run_local(async {
            let (socket, remote) = held(6);
            let transition = remote.begin_session_transition();
            let _a = tokio::task::spawn_local(remote.query_statements("SELECT * FROM a", None));
            let _b = tokio::task::spawn_local(remote.query_statements("SELECT * FROM b", None));
            let _w = tokio::task::spawn_local(remote.write_statements("CREATE ONLY thing:1", None));
            pump().await;
            assert!(socket.started().is_empty());

            remote.end_session_transition(transition);
            pump().await;
            let mut started = socket.started();
            started.sort();
            assert_eq!(
                started,
                ["CREATE ONLY thing:1", "SELECT * FROM a", "SELECT * FROM b"]
            );
            assert_eq!(socket.in_flight.get(), 3);
        })
        .await;
    }

    #[tokio::test]
    async fn a_stale_transition_does_not_release_the_lanes() {
        run_local(async {
            let (socket, remote) = held(6);
            let first = remote.begin_session_transition();
            let second = remote.begin_session_transition();
            let _a = tokio::task::spawn_local(remote.query_statements("SELECT * FROM a", None));
            remote.end_session_transition(first);
            pump().await;
            assert!(socket.started().is_empty());
            remote.end_session_transition(second);
            pump().await;
            assert_eq!(socket.started(), ["SELECT * FROM a"]);
        })
        .await;
    }

    #[tokio::test]
    async fn every_lane_waits_for_the_handshake() {
        run_local(async {
            let (socket, remote) = held(6);
            remote.arm_connect_gate();
            let _a = tokio::task::spawn_local(remote.query_statements("SELECT * FROM a", None));
            let _w = tokio::task::spawn_local(remote.write_statements("CREATE ONLY thing:1", None));
            pump().await;
            assert!(socket.started().is_empty());

            remote.connect().await.unwrap();
            pump().await;
            let started = socket.started();
            assert!(started.contains(&"SELECT * FROM a".to_string()));
            assert!(started.contains(&"CREATE ONLY thing:1".to_string()));
        })
        .await;
    }

    #[tokio::test]
    async fn a_read_in_flight_when_the_account_changes_is_discarded() {
        run_local(async {
            let (socket, remote) = held(6);
            let read = tokio::task::spawn_local(remote.query("SELECT * FROM a", None));
            pump().await;
            remote.set_auth_token(Some("next-account".into()));
            socket.answer("SELECT * FROM a");
            pump().await;
            assert!(matches!(read.await.unwrap(), Err(Error::AccountChanged)));
        })
        .await;
    }

    #[tokio::test]
    async fn a_queued_read_never_runs_after_the_account_changes() {
        run_local(async {
            let (socket, remote) = held(1);
            let first = tokio::task::spawn_local(remote.query("SELECT * FROM a", None));
            let queued = tokio::task::spawn_local(remote.query("SELECT * FROM b", None));
            pump().await;
            assert_eq!(socket.started(), ["SELECT * FROM a"]);

            remote.set_auth_token(Some("next-account".into()));
            socket.answer("SELECT * FROM a");
            pump().await;
            assert!(matches!(first.await.unwrap(), Err(Error::AccountChanged)));
            assert!(matches!(queued.await.unwrap(), Err(Error::AccountChanged)));
            assert_eq!(
                socket.started(),
                ["SELECT * FROM a"],
                "a request issued for one account must not reach the next"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn probe_fails_while_a_write_hangs() {
        run_local(async {
            let (socket, remote) = held(6);
            let _w = tokio::task::spawn_local(remote.write_statements("CREATE ONLY thing:1", None));
            let probe = tokio::task::spawn_local(remote.probe());
            pump().await;
            assert!(
                !probe.is_finished(),
                "a wedged write lane must not look healthy"
            );

            socket.answer("CREATE ONLY thing:1");
            pump().await;
            assert!(probe.is_finished());
            probe.await.unwrap().unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn probe_passes_while_one_read_hangs() {
        run_local(async {
            let (socket, remote) = held(2);
            let _slow =
                tokio::task::spawn_local(remote.query_statements("SELECT * FROM slow", None));
            let probe = tokio::task::spawn_local(remote.probe());
            pump().await;
            assert!(probe.is_finished());
            probe.await.unwrap().unwrap();
            socket.answer("SELECT * FROM slow");
        })
        .await;
    }

    #[tokio::test]
    async fn session_ids_are_recorded_once_each_and_capped() {
        run_local(async {
            let (socket, remote) = held(6);
            for i in 0..20 {
                *socket.session_id.borrow_mut() = Some(format!("s{}", i.min(17)));
                remote.connect().await.unwrap();
                pump().await;
            }
            let ids = remote.session_ids();
            assert_eq!(ids.len(), MAX_SESSION_IDS);
            assert_eq!(ids.first().unwrap(), "s2");
            assert_eq!(ids.last().unwrap(), "s17");
        })
        .await;
    }

    // ---- the adapter -----------------------------------------------------------------

    /// A client whose LIVE stream the test feeds.
    #[derive(Default)]
    struct LiveRemote {
        sink: RefCell<Option<mpsc::UnboundedSender<LiveMessage>>>,
        lives: RefCell<Vec<(String, Option<Object>)>>,
        kills: RefCell<Vec<String>>,
    }

    #[async_trait(?Send)]
    impl RemoteSurrealClient for LiveRemote {
        async fn connect(&self, _: &str) -> Result<()> {
            Ok(())
        }
        async fn use_db(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        async fn authenticate(&self, _: &str) -> Result<Value> {
            Ok(Value::None)
        }
        async fn signin(&self, _: Object) -> Result<Value> {
            Ok(Value::None)
        }
        async fn signup(&self, _: Object) -> Result<Value> {
            Ok(Value::None)
        }
        async fn invalidate(&self) -> Result<()> {
            Ok(())
        }
        async fn query(&self, _: &str, _: Option<Object>) -> Result<Vec<Value>> {
            Ok(vec![Value::from("sid-1")])
        }
        async fn live(&self, sql: &str, vars: Option<Object>) -> Result<(String, LiveStream)> {
            self.lives.borrow_mut().push((sql.to_string(), vars));
            let (tx, rx) = mpsc::unbounded_channel();
            *self.sink.borrow_mut() = Some(tx);
            Ok(("uuid-1".into(), rx))
        }
        async fn kill(&self, live_id: &str) -> Result<()> {
            self.kills.borrow_mut().push(live_id.to_string());
            Ok(())
        }
        async fn close(&self) {}
    }

    fn edge(action: &str, value: serde_json::Value) -> LiveMessage {
        LiveMessage {
            action: action.into(),
            value: object_from_json(value),
        }
    }

    type Seen = Rc<RefCell<Vec<(Vec<String>, Option<Vec<InlineRow>>)>>>;

    #[tokio::test]
    async fn adapter_routes_edges_to_hashes_with_inline_rows() {
        run_local(async {
            let client = Rc::new(LiveRemote::default());
            let remote = RemoteDatabaseService::new(config(6), client.clone());
            let adapter = RemoteAdapter::new(remote, true);
            let seen: Seen = Rc::default();
            let s = seen.clone();
            let uuid = adapter
                .live(
                    "_00_list_ref_user_a",
                    Rc::new(move |hashes, rows| s.borrow_mut().push((hashes, rows))),
                )
                .await
                .unwrap();
            assert_eq!(uuid, "uuid-1");
            assert_eq!(client.lives.borrow()[0].0, "LIVE SELECT * FROM _00_list_ref_user_a FETCH out");

            let sink = client.sink.borrow().clone().unwrap();
            sink.send(edge(
                "CREATE",
                serde_json::json!({"in": "_00_query:h1", "out": {"id": "thing:1", "n": 1}, "version": 3}),
            ))
            .unwrap();
            sink.send(edge("KILLED", serde_json::json!({"in": "_00_query:h1"}))).unwrap();
            sink.send(edge(
                "DELETE",
                serde_json::json!({"in": "_00_query:h2", "out": {"id": "thing:2"}, "version": 4}),
            ))
            .unwrap();
            sink.send(edge("UPDATE", serde_json::json!({"out": "thing:9"}))).unwrap();
            pump().await;

            let seen = seen.borrow();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[0].0, ["h1"]);
            let rows = seen[0].1.as_ref().unwrap();
            assert_eq!(rows[0].id, "thing:1");
            assert_eq!(rows[0].version, 3);
            assert_eq!(rows[0].record["n"], Value::Int(1));
            assert_eq!(seen[1], (vec!["h2".to_string()], None), "no body lands on DELETE");
        })
        .await;
    }

    #[tokio::test]
    async fn adapter_filters_the_anon_table_by_session_ids_and_kill_stops_forwarding() {
        run_local(async {
            let client = Rc::new(LiveRemote::default());
            let remote = RemoteDatabaseService::new(config(6), client.clone());
            remote.connect().await.unwrap();
            pump().await;
            assert_eq!(remote.session_ids(), ["sid-1"]);
            let adapter = RemoteAdapter::new(remote, false);
            let seen: Seen = Rc::default();
            let s = seen.clone();
            adapter
                .live(
                    ANON_LIST_REF_TABLE,
                    Rc::new(move |hashes, rows| s.borrow_mut().push((hashes, rows))),
                )
                .await
                .unwrap();
            let (sql, vars) = client.lives.borrow()[0].clone();
            assert!(sql.contains("clientId IN $sids"));
            assert_eq!(
                vars.unwrap()["sids"],
                Value::Array(vec![Value::from("sid-1")])
            );

            adapter.kill("uuid-1").await.unwrap();
            assert_eq!(*client.kills.borrow(), ["uuid-1"]);
            let sink = client.sink.borrow().clone().unwrap();
            let _ = sink.send(edge("CREATE", serde_json::json!({"in": "_00_query:h1"})));
            pump().await;
            assert!(seen.borrow().is_empty());
        })
        .await;
    }

    #[test]
    fn edge_helpers() {
        let e = object_from_json(
            serde_json::json!({"in": "_00_query:abc", "out": "thing:1", "version": 2}),
        );
        assert_eq!(hash_of_edge(&e).as_deref(), Some("abc"));
        assert!(row_of_edge(&e).is_none(), "no FETCH out: nothing to land");
        assert!(hash_of_edge(&object_from_json(serde_json::json!({"in": null}))).is_none());
        assert!(hash_of_edge(&object_from_json(serde_json::json!({"in": "nocolon"}))).is_none());
        // A row the session may not read joins as null.
        let unreadable =
            object_from_json(serde_json::json!({"in": "_00_query:a", "out": null, "version": 1}));
        assert!(row_of_edge(&unreadable).is_none());
        let no_id = object_from_json(serde_json::json!({"out": {"n": 1}, "version": 1}));
        assert!(row_of_edge(&no_id).is_none());
        let no_version = object_from_json(serde_json::json!({"out": {"id": "t:1"}}));
        assert!(row_of_edge(&no_version).is_none());
        let float_version =
            object_from_json(serde_json::json!({"out": {"id": "t:1"}, "version": 5.0}));
        assert_eq!(row_of_edge(&float_version).unwrap().version, 5);
    }
}
