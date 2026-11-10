//! [`ChannelManager`]: open channels, the peer overlay, routing by filter.
//!
//! PORT: the `ChannelManager` half of
//! `packages/spooky_core/lib/src/modules/channel/channel.dart`.
//!
//! Single-threaded like the engine. Every call out (a transport, a tap, a
//! state listener) happens with no borrow held, so a transport may call its
//! hooks from inside `want` or `close`, and a listener may reopen or close a
//! channel.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map as JsonMap, Value as Json};

use super::{
    apply_overlay, apply_overlay_row, carries, check_patch, collect_ids, decode_row, encode_row,
    find_row, is_channel_table, normalize_id, parse_message, peer_users, pick_columns,
    prepare_scope, same_value, typed, ChannelAcceptRule, ChannelMessage, ChannelOp, ChannelOpKind,
    ChannelScope, ChannelState, ChannelStatus, ChannelTransport, ChannelsConfig, Hlc, Overlay,
    OverlayEntry, PeerLinkStatus, PeerRef, PreparedScope, TransportHooks,
};
use crate::error::{Error, Result};
use crate::kernel::events::LogLevel;
use crate::modules::query_builder::QueryBuilder;
use crate::runtime::Subscription;
use crate::types::{ColumnSchema, Schema};
use crate::value::{table_of, Object, Row, Value};

/// How peers' creates and deletes reach the store (the engine's peer sagas).
#[async_trait(?Send)]
pub trait ChannelRows {
    /// Insert a peer's row, unconfirmed for `ttl_ms`. False when refused.
    async fn create(&self, table: &str, id: &str, row: Object, ttl_ms: u64) -> Result<bool>;
    /// Hide a row a peer deleted (or undo its unconfirmed create). False when
    /// there was nothing to do.
    async fn delete(&self, table: &str, id: &str, ttl_ms: u64) -> Result<bool>;
    /// The row as the local store holds it, or `None`. What a filter is
    /// checked against.
    async fn read(&self, id: &str) -> Result<Option<Row>>;
}

/// The transport factory as the manager uses it, on its own thread.
pub type LocalTransportFactory = Rc<dyn Fn(Rc<dyn TransportHooks>) -> Rc<dyn ChannelTransport>>;

/// Where the manager's warnings and dropped-message notes go; `tracing`
/// (target `spooky.channel`) when not set.
pub type ChannelLogger = Rc<dyn Fn(LogLevel, &str)>;

/// What a [`ChannelManager`] is built with. Everything is optional: without
/// `rows` peers' creates and deletes are ignored, without `transport` nothing
/// leaves the device.
#[derive(Default)]
pub struct ManagerOptions {
    /// Accept rules and the confirm timeout (`rtc`, `ice_servers` and
    /// `transport` are the caller's to turn into `transport`).
    pub config: Option<ChannelsConfig>,
    pub rows: Option<Rc<dyn ChannelRows>>,
    pub transport: Option<LocalTransportFactory>,
    /// Wall clock in ms (tests).
    pub now: Option<Rc<dyn Fn() -> i64>>,
    /// This device's peer id; random when not set.
    pub peer_id: Option<String>,
    pub logger: Option<ChannelLogger>,
}

const SWEEP_EVERY: Duration = Duration::from_secs(1);
const SELF: &str = "self";

type Listener = Rc<dyn Fn(&ChannelState)>;
type TapCallback = Rc<dyn Fn(&Arc<Vec<Row>>)>;

struct Tap {
    cb: TapCallback,
    raw: Option<Arc<Vec<Row>>>,
    /// Every record id in `raw`, computed on first need.
    ids: Option<BTreeSet<String>>,
}

impl Tap {
    fn ids(&mut self) -> &BTreeSet<String> {
        let raw = &self.raw;
        self.ids
            .get_or_insert_with(|| raw.as_ref().map(|r| collect_ids(r)).unwrap_or_default())
    }
}

struct ChannelData {
    scopes: Vec<ChannelScope>,
    /// Peers as given, kept so a sign-in can drop the new self from them.
    listed: Vec<Value>,
    users: BTreeSet<String>,
    listeners: Vec<(u64, Listener)>,
    next_listener: u64,
    state: ChannelState,
    closed: bool,
}

impl ChannelData {
    fn tables(&self) -> BTreeSet<String> {
        self.scopes.iter().map(|s| s.table.clone()).collect()
    }

    fn has_table(&self, table: &str) -> bool {
        self.scopes.iter().any(|s| s.table == table)
    }
}

type ChannelCell = Rc<RefCell<ChannelData>>;

struct State {
    overlay: Overlay,
    taps: BTreeMap<u64, Tap>,
    next_tap: u64,
    channels: Vec<ChannelCell>,
    connected: BTreeMap<String, PeerRef>,
    user_status: HashMap<String, PeerLinkStatus>,
    /// Unused-argument warnings already given, so a rebuilt query does not
    /// repeat them.
    warned_unused: HashSet<String>,
    clock: Hlc,
    user: Option<String>,
    transport: Option<Rc<dyn ChannelTransport>>,
    sweep: Option<tokio::task::JoinHandle<()>>,
    closed: bool,
}

struct Inner {
    schema: Arc<Schema>,
    accept: BTreeMap<String, ChannelAcceptRule>,
    rows: Option<Rc<dyn ChannelRows>>,
    transport_factory: Option<LocalTransportFactory>,
    now: Rc<dyn Fn() -> i64>,
    confirm_ms: i64,
    peer_id: String,
    enabled: bool,
    logger: Option<ChannelLogger>,
    st: RefCell<State>,
}

/// The peer to peer channels of one client.
#[derive(Clone)]
pub struct ChannelManager(Rc<Inner>);

fn random_peer_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

fn wall_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl ChannelManager {
    pub fn new(schema: Arc<Schema>, options: ManagerOptions) -> ChannelManager {
        let peer_id = options.peer_id.unwrap_or_else(random_peer_id);
        let config = options.config.unwrap_or_default();
        let enabled = schema.tables.values().any(|t| t.channel);
        ChannelManager(Rc::new(Inner {
            accept: config.accept,
            confirm_ms: config.confirm_timeout_ms.min(i64::MAX as u64) as i64,
            schema,
            rows: options.rows,
            transport_factory: options.transport,
            now: options.now.unwrap_or_else(|| Rc::new(wall_ms)),
            enabled,
            logger: options.logger,
            st: RefCell::new(State {
                overlay: Overlay::new(),
                taps: BTreeMap::new(),
                next_tap: 0,
                channels: Vec::new(),
                connected: BTreeMap::new(),
                user_status: HashMap::new(),
                warned_unused: HashSet::new(),
                clock: Hlc::new(0, 0, peer_id.clone()),
                user: None,
                transport: None,
                sweep: None,
                closed: false,
            }),
            peer_id,
        }))
    }

    /// This device's peer id (random per client instance).
    pub fn peer_id(&self) -> &str {
        &self.0.peer_id
    }

    /// True when any table is a channel table; otherwise the client skips all
    /// of this.
    pub fn enabled(&self) -> bool {
        self.0.enabled
    }

    /// The signed-in user, as the channels know it.
    pub fn user(&self) -> Option<String> {
        self.0.st.borrow().user.clone()
    }

    /// The signed-in user changed: peers, their values and the transport start
    /// over.
    pub fn set_user(&self, user: Option<&str>) {
        let inner = &self.0;
        let transport = {
            let mut st = inner.st.borrow_mut();
            if st.user.as_deref() == user {
                return;
            }
            st.user = user.map(str::to_string);
            st.transport.take()
        };
        if let Some(t) = transport {
            t.close();
        }
        let touched: BTreeSet<String> = {
            let mut st = inner.st.borrow_mut();
            st.connected.clear();
            st.user_status.clear();
            let touched = std::mem::take(&mut st.overlay).into_keys().collect();
            let me = st.user.clone();
            for ch in &st.channels {
                let mut ch = ch.borrow_mut();
                ch.users = peer_users(&ch.listed, me.as_deref());
            }
            touched
        };
        inner.sync_wanted();
        inner.emit_for(&touched);
        inner.notify_all();
    }

    /// Compile a query into a channel scope (fails when it cannot be one).
    /// Parts the channel ignores draw one warning per distinct query shape,
    /// unless the query says so itself (`ignore_unused_args()`).
    pub fn scope(&self, q: &QueryBuilder) -> Result<ChannelScope> {
        Ok(self.adopt(prepare_scope(&self.0.schema, q)?))
    }

    /// A scope compiled elsewhere (on another thread), with its warning.
    pub fn adopt(&self, prepared: PreparedScope) -> ChannelScope {
        if !prepared.unused.is_empty() {
            let message = format!(
                "channel query on '{}' has {}, which a channel ignores (it only uses the where filter). Remove them, \
                 or add .ignore_unused_args() to the query to say it is on purpose.",
                prepared.scope.table,
                prepared.unused.join(", ")
            );
            let first = self.0.st.borrow_mut().warned_unused.insert(message.clone());
            if first {
                self.0.log(LogLevel::Warn, &message);
            }
        }
        prepared.scope
    }

    /// Open a channel carrying the rows `scopes` match to `peers` (`table:id`
    /// strings or record ids; your own id is skipped).
    pub fn open(&self, scopes: Vec<ChannelScope>, peers: Vec<Value>) -> Result<ChannelHandle> {
        if scopes.is_empty() {
            return Err(Error::InvalidArgument(
                "A channel needs at least one query.".into(),
            ));
        }
        let users = peer_users(&peers, self.0.st.borrow().user.as_deref());
        let ch = Rc::new(RefCell::new(ChannelData {
            scopes,
            listed: peers,
            users,
            listeners: Vec::new(),
            next_listener: 0,
            state: ChannelState::default(),
            closed: false,
        }));
        self.0.st.borrow_mut().channels.push(ch.clone());
        self.0.sync_wanted();
        let state = self.0.state_of(&ch);
        ch.borrow_mut().state = state;
        Ok(ChannelHandle {
            manager: Rc::downgrade(&self.0),
            channel: ch,
        })
    }

    /// Fails unless `handle` is an open channel of this manager with a query on
    /// `table`.
    pub fn assert_carries(&self, handle: &ChannelHandle, table: &str) -> Result<()> {
        let ch = handle.channel.borrow();
        if !Weak::ptr_eq(&handle.manager, &Rc::downgrade(&self.0)) || ch.closed {
            return Err(Error::other("This channel is closed."));
        }
        if !ch.has_table(table) {
            let tables: Vec<String> = ch.tables().into_iter().collect();
            return Err(Error::InvalidArgument(format!(
                "This channel has no query on '{table}' (it reads {}).",
                tables.join(", ")
            )));
        }
        Ok(())
    }

    /// Follow a subscription: [`TapHandle::feed`] it the raw rows, and `cb`
    /// gets them with the overlay folded in, again whenever the overlay
    /// changes for its rows. Stops when the handle is dropped.
    pub fn tap(&self, cb: impl Fn(&Arc<Vec<Row>>) + 'static) -> TapHandle {
        let mut st = self.0.st.borrow_mut();
        st.next_tap += 1;
        let id = st.next_tap;
        st.taps.insert(
            id,
            Tap {
                cb: Rc::new(cb),
                raw: None,
                ids: None,
            },
        );
        TapHandle {
            manager: Rc::downgrade(&self.0),
            id,
        }
    }

    /// Split an own write on `table` into database fields and peer-only fields.
    pub fn split(&self, table: &str, data: &Row) -> (Row, Option<Row>) {
        match self.0.channel_columns(table) {
            None => (data.clone(), None),
            Some(columns) => super::split_patch(columns, data),
        }
    }

    /// An own update: ephemeral fields go into the overlay (they have no other
    /// home), persistent fields drop any peer value they supersede, and the
    /// whole patch goes to the peers (of `scope`, or of every channel whose
    /// filter the row matches).
    pub fn local(
        &self,
        table: &str,
        id: &str,
        persistent: &Row,
        ephemeral: Option<&Row>,
        scope: Option<&ChannelHandle>,
    ) {
        let inner = &self.0;
        let Some(columns) = inner.channel_columns(table) else {
            return;
        };
        if inner.st.borrow().closed {
            return;
        }
        let rid = normalize_id(table, id);
        let hlc = inner.tick();
        let touched = {
            let mut st = inner.st.borrow_mut();
            let mut touched = false;
            if let Some(fields) = st.overlay.get_mut(&rid) {
                for f in persistent.keys() {
                    touched |= fields.remove(f).is_some();
                }
            }
            if let Some(ephemeral) = ephemeral {
                let map = st.overlay.entry(rid.clone()).or_default();
                for (f, value) in ephemeral {
                    map.insert(
                        f.clone(),
                        OverlayEntry {
                            value: value.clone(),
                            hlc: hlc.clone(),
                            from: SELF.into(),
                            ephemeral: true,
                            expires_at: None,
                        },
                    );
                }
                touched = true;
            }
            if st.overlay.get(&rid).is_some_and(|f| f.is_empty()) {
                st.overlay.remove(&rid);
            }
            touched
        };
        if touched {
            inner.emit_for(&BTreeSet::from([rid.clone()]));
        }
        let mut patch = persistent.clone();
        if let Some(e) = ephemeral {
            patch.extend(e.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        if patch.is_empty() || !inner.can_send(table) {
            return;
        }
        // Sent if the row matches before or after the patch, so a row moving out
        // of a channel's filter still reaches its peers.
        let wire = typed(columns, &patch);
        let (inner, table, scope) = (
            self.0.clone(),
            table.to_string(),
            scope.map(|s| s.channel.clone()),
        );
        tokio::task::spawn_local(async move {
            let row = inner.row_for(&rid).await;
            let mut after = row
                .clone()
                .unwrap_or_else(|| Row::from([("id".to_string(), Value::from(rid.clone()))]));
            after.extend(patch);
            let rows = match row {
                Some(row) => vec![row, after],
                None => vec![after],
            };
            let targets = inner.targets(&table, &rows, scope.as_ref());
            inner.send(&targets, &table, &rid, &wire, hlc, ChannelOpKind::Update);
        });
    }

    /// An own create: its ephemeral fields go into the overlay (the store never
    /// holds them), and the whole row goes to the peers.
    pub fn local_create(&self, table: &str, id: &str, row: &Row, scope: Option<&ChannelHandle>) {
        let inner = &self.0;
        let Some(columns) = inner.channel_columns(table) else {
            return;
        };
        if inner.st.borrow().closed {
            return;
        }
        let rid = normalize_id(table, id);
        let full = pick_columns(columns, row);
        let hlc = inner.tick();
        let ephemeral: Vec<&String> = full
            .keys()
            .filter(|f| columns.get(*f).is_some_and(|c| c.ephemeral))
            .collect();
        if !ephemeral.is_empty() {
            {
                let mut st = inner.st.borrow_mut();
                let map = st.overlay.entry(rid.clone()).or_default();
                for f in ephemeral {
                    map.insert(
                        f.clone(),
                        OverlayEntry {
                            value: full[f].clone(),
                            hlc: hlc.clone(),
                            from: SELF.into(),
                            ephemeral: true,
                            expires_at: None,
                        },
                    );
                }
            }
            inner.emit_for(&BTreeSet::from([rid.clone()]));
        }
        let mut shown = full.clone();
        shown.insert("id".into(), Value::from(rid.clone()));
        let targets = inner.targets(table, &[shown], scope.map(|s| &s.channel));
        inner.send(
            &targets,
            table,
            &rid,
            &typed(columns, &full),
            hlc,
            ChannelOpKind::Create,
        );
    }

    /// An own delete: the peers hide the row at once. Completes once it is
    /// sent; await it before the local delete, which the filter check reads
    /// past.
    pub async fn local_delete(&self, table: &str, id: &str, scope: Option<&ChannelHandle>) {
        let inner = &self.0;
        if inner.channel_columns(table).is_none() || inner.st.borrow().closed {
            return;
        }
        let rid = normalize_id(table, id);
        let hlc = inner.tick();
        if inner.can_send(table) {
            if let Some(row) = inner.row_for(&rid).await {
                let targets = inner.targets(table, &[row], scope.map(|s| &s.channel));
                inner.send(
                    &targets,
                    table,
                    &rid,
                    &Row::new(),
                    hlc,
                    ChannelOpKind::Delete,
                );
            }
        }
        let removed = inner.st.borrow_mut().overlay.remove(&rid).is_some();
        if removed {
            inner.emit_for(&BTreeSet::from([rid]));
        }
    }

    /// A transport delivered a message from `from`.
    pub fn receive(&self, from: &PeerRef, raw: &Json) {
        self.0.clone().receive(from, raw);
    }

    /// A peer device came up (send it our ephemeral state) or went away (drop
    /// its).
    pub fn peer_changed(&self, peer: &PeerRef, up: bool) {
        self.0.clone().peer_changed(peer, up);
    }

    pub fn status_changed(&self, user: &str, status: PeerLinkStatus) {
        self.0.status_changed(user, status);
    }

    /// Close every channel and the transport; drop every peer value.
    pub fn close(&self) {
        let inner = &self.0;
        let transport = {
            let mut st = inner.st.borrow_mut();
            st.closed = true;
            st.transport.take()
        };
        if let Some(t) = transport {
            t.close();
        }
        let mut st = inner.st.borrow_mut();
        if let Some(sweep) = st.sweep.take() {
            sweep.abort();
        }
        st.taps.clear();
        for ch in std::mem::take(&mut st.channels) {
            let mut ch = ch.borrow_mut();
            ch.closed = true;
            ch.listeners.clear();
        }
        st.overlay.clear();
    }

    /// The overlay as it stands (diagnostics and tests).
    pub fn overlay(&self) -> Overlay {
        self.0.st.borrow().overlay.clone()
    }
}

impl Inner {
    fn log(&self, level: LogLevel, message: &str) {
        match &self.logger {
            Some(log) => log(level, message),
            None => match level {
                LogLevel::Debug => tracing::debug!(target: "spooky.channel", "{message}"),
                LogLevel::Info => tracing::info!(target: "spooky.channel", "{message}"),
                LogLevel::Warn => tracing::warn!(target: "spooky.channel", "{message}"),
                LogLevel::Error => tracing::error!(target: "spooky.channel", "{message}"),
            },
        }
    }

    fn drop_msg(&self, from: &PeerRef, reason: &str) {
        self.log(
            LogLevel::Debug,
            &format!("channel message from {} dropped: {reason}", from.user),
        );
    }

    fn channel_columns(&self, table: &str) -> Option<&BTreeMap<String, ColumnSchema>> {
        if !is_channel_table(&self.schema, table) {
            return None;
        }
        self.schema.columns(table)
    }

    fn is_closed(&self) -> bool {
        self.st.borrow().closed
    }

    fn sync_wanted(self: &Rc<Self>) {
        let (wanted, transport) = {
            let st = self.st.borrow();
            if st.closed {
                return;
            }
            let wanted: BTreeSet<String> = st
                .channels
                .iter()
                .flat_map(|ch| ch.borrow().users.iter().cloned().collect::<Vec<_>>())
                .collect();
            (wanted, st.transport.clone())
        };
        let transport = match transport {
            Some(t) => t,
            None => {
                let has_user = self.st.borrow().user.is_some();
                let Some(factory) = self.transport_factory.clone() else {
                    return;
                };
                if wanted.is_empty() || !has_user {
                    return;
                }
                let hooks: Rc<dyn TransportHooks> = Rc::new(Hooks(Rc::downgrade(self)));
                let t = factory(hooks);
                self.st.borrow_mut().transport = Some(t.clone());
                t
            }
        };
        transport.want(&wanted);
    }

    /// Whether an own write on `table` could reach anyone: a transport is up
    /// and an open channel carries the table. Skips the row read otherwise.
    fn can_send(&self, table: &str) -> bool {
        let st = self.st.borrow();
        st.transport.is_some() && st.channels.iter().any(|ch| ch.borrow().has_table(table))
    }

    /// Some open channel lists `user` and has a query on `table` (the cheap
    /// check before any read).
    fn listed_on(&self, table: &str, user: &str) -> bool {
        self.st.borrow().channels.iter().any(|ch| {
            let ch = ch.borrow();
            ch.users.contains(user) && ch.has_table(table)
        })
    }

    /// Some open channel listing `user` has a query on `table` one of `rows`
    /// matches.
    fn matches_for(&self, table: &str, rows: &[Row], user: &str) -> bool {
        self.st.borrow().channels.iter().any(|ch| {
            let ch = ch.borrow();
            ch.users.contains(user) && carries(&ch.scopes, table, rows)
        })
    }

    /// Who an own write goes to: the peers of every open channel (or only
    /// `scope`) whose filter one of `rows` matches. That is what keeps a
    /// message of one conversation away from the participants of another.
    fn targets(&self, table: &str, rows: &[Row], scope: Option<&ChannelCell>) -> BTreeSet<String> {
        let pick: Vec<ChannelCell> = match scope {
            Some(ch) => vec![ch.clone()],
            None => self.st.borrow().channels.clone(),
        };
        let mut users = BTreeSet::new();
        for ch in pick {
            let ch = ch.borrow();
            if ch.closed || !carries(&ch.scopes, table, rows) {
                continue;
            }
            users.extend(ch.users.iter().cloned());
        }
        users
    }

    /// The row as shown (overlay included): from a subscription, else the
    /// store.
    async fn row_for(&self, rid: &str) -> Option<Row> {
        if let Some(shown) = self.row_of(rid) {
            return Some(shown);
        }
        let rows = self.rows.clone()?;
        let stored = rows.read(rid).await.ok().flatten()?;
        let st = self.st.borrow();
        Some(apply_overlay_row(&stored, &st.overlay))
    }

    fn row_of(&self, rid: &str) -> Option<Row> {
        let mut st = self.st.borrow_mut();
        let State { taps, overlay, .. } = &mut *st;
        for tap in taps.values_mut() {
            if tap.raw.is_none() || !tap.ids().contains(rid) {
                continue;
            }
            if let Some(row) = tap.raw.as_ref().and_then(|raw| find_row(raw, rid)) {
                return Some(apply_overlay_row(row, overlay));
            }
        }
        None
    }

    fn send(
        &self,
        users: &BTreeSet<String>,
        table: &str,
        rid: &str,
        patch: &Row,
        hlc: Hlc,
        kind: ChannelOpKind,
    ) {
        let (transport, peers) = {
            let st = self.st.borrow();
            let Some(t) = st.transport.clone() else {
                return;
            };
            if users.is_empty() {
                return;
            }
            let peers: Vec<PeerRef> = st
                .connected
                .values()
                .filter(|p| users.contains(&p.user))
                .cloned()
                .collect();
            (t, peers)
        };
        let message = ChannelMessage {
            kind,
            table: table.to_string(),
            id: rid.to_string(),
            patch: encode_row(patch),
            hlc,
        };
        for peer in peers {
            transport.send(&peer, &message);
        }
    }

    fn receive(self: Rc<Self>, from: &PeerRef, raw: &Json) {
        if self.is_closed() {
            return;
        }
        let Some(msg) = parse_message(raw) else {
            return self.drop_msg(from, "malformed message");
        };
        self.observe(&msg.hlc);
        let Some(columns) = self.channel_columns(&msg.table) else {
            return self.drop_msg(from, &format!("'{}' is not a @channel table", msg.table));
        };
        let rid = normalize_id(&msg.table, &msg.id);
        if table_of(&rid) != msg.table {
            return self.drop_msg(from, "id is not a row of the table");
        }
        if !self.listed_on(&msg.table, &from.user) {
            return self.drop_msg(
                from,
                &format!("no channel on '{}' lists {}", msg.table, from.user),
            );
        }
        match msg.kind {
            ChannelOpKind::Create => self.receive_create(from, msg, rid),
            ChannelOpKind::Delete => {
                let from = from.clone();
                tokio::task::spawn_local(async move {
                    self.receive_delete(&from, &msg.table, &rid).await
                });
            }
            ChannelOpKind::Update => {
                let patch = decode_row(&msg.patch);
                if let Some(problem) = check_patch(columns, &patch, false) {
                    return self.drop_msg(from, &problem);
                }
                let from = from.clone();
                tokio::task::spawn_local(async move {
                    self.receive_update(&from, msg, rid, patch).await
                });
            }
        }
    }

    /// A peer updated a row we hold, inside a channel's filter (before or after
    /// the patch).
    async fn receive_update(
        self: &Rc<Self>,
        from: &PeerRef,
        msg: ChannelMessage,
        rid: String,
        patch: Row,
    ) {
        let row = self.row_for(&rid).await;
        if self.is_closed() {
            return;
        }
        let Some(row) = row else {
            return self.drop_msg(
                from,
                &format!("{rid} is not held here, so no filter can be checked"),
            );
        };
        let mut after = row.clone();
        after.extend(patch.iter().map(|(k, v)| (k.clone(), v.clone())));
        if !self.matches_for(&msg.table, &[row.clone(), after], &from.user) {
            return self.drop_msg(
                from,
                &format!("{rid} is outside every channel listing {}", from.user),
            );
        }
        let op = ChannelOp {
            kind: ChannelOpKind::Update,
            from: from.user.clone(),
            table: msg.table.clone(),
            id: rid.clone(),
            row: Some(row),
            patch: patch.clone(),
        };
        if !self.accepts(&op) {
            return self.drop_msg(from, "refused by the accept rule");
        }
        let Some(columns) = self.channel_columns(&msg.table) else {
            return;
        };
        let expires = (self.now)() + self.confirm_ms;
        let touched = {
            let mut st = self.st.borrow_mut();
            let map = st.overlay.entry(rid.clone()).or_default();
            let mut touched = false;
            for (field, value) in patch {
                if map.get(&field).is_some_and(|prev| prev.hlc >= msg.hlc) {
                    continue;
                }
                let ephemeral = columns.get(&field).is_some_and(|c| c.ephemeral);
                map.insert(
                    field,
                    OverlayEntry {
                        value,
                        hlc: msg.hlc.clone(),
                        from: from.key(),
                        ephemeral,
                        expires_at: (!ephemeral).then_some(expires),
                    },
                );
                touched = true;
            }
            if map.is_empty() {
                st.overlay.remove(&rid);
            }
            touched
        };
        if !touched {
            return;
        }
        self.ensure_sweep();
        self.emit_for(&BTreeSet::from([rid]));
    }

    /// A peer created a row: into the store like an own optimistic create,
    /// unconfirmed.
    fn receive_create(self: Rc<Self>, from: &PeerRef, msg: ChannelMessage, rid: String) {
        let Some(rows) = self.rows.clone() else {
            return self.drop_msg(from, "creates are not wired");
        };
        let Some(columns) = self.channel_columns(&msg.table) else {
            return;
        };
        let mut row = decode_row(&msg.patch);
        row.remove("id");
        if let Some(problem) = check_patch(columns, &row, true) {
            return self.drop_msg(from, &problem);
        }
        let mut shown = row.clone();
        shown.insert("id".into(), Value::from(rid.clone()));
        if !self.matches_for(&msg.table, &[shown], &from.user) {
            return self.drop_msg(
                from,
                &format!(
                    "the new {rid} is outside every channel listing {}",
                    from.user
                ),
            );
        }
        let op = ChannelOp {
            kind: ChannelOpKind::Create,
            from: from.user.clone(),
            table: msg.table.clone(),
            id: rid.clone(),
            row: None,
            patch: row.clone(),
        };
        if !self.accepts(&op) {
            return self.drop_msg(from, "refused by the accept rule");
        }
        let (persistent, ephemeral) = super::split_patch(columns, &row);
        let from_key = from.key();
        let ttl = self.confirm_ms as u64;
        tokio::task::spawn_local(async move {
            match rows.create(&msg.table, &rid, persistent, ttl).await {
                Ok(true) => {}
                Ok(false) => return,
                Err(e) => return self.log(LogLevel::Debug, &format!("peer create failed: {e}")),
            }
            let Some(ephemeral) = ephemeral else { return };
            if self.is_closed() {
                return;
            }
            {
                let mut st = self.st.borrow_mut();
                let map = st.overlay.entry(rid.clone()).or_default();
                for (f, value) in ephemeral {
                    map.insert(
                        f,
                        OverlayEntry {
                            value,
                            hlc: msg.hlc.clone(),
                            from: from_key.clone(),
                            ephemeral: true,
                            expires_at: None,
                        },
                    );
                }
            }
            self.emit_for(&BTreeSet::from([rid]));
        });
    }

    /// A peer deleted a row inside a channel's filter: hide it until the
    /// server confirms (or undo its own create).
    async fn receive_delete(self: &Rc<Self>, from: &PeerRef, table: &str, rid: &str) {
        let Some(rows) = self.rows.clone() else {
            return self.drop_msg(from, "deletes are not wired");
        };
        let row = self.row_for(rid).await;
        if self.is_closed() {
            return;
        }
        let Some(row) = row else {
            return self.drop_msg(from, &format!("{rid} is not held here"));
        };
        if !self.matches_for(table, std::slice::from_ref(&row), &from.user) {
            return self.drop_msg(
                from,
                &format!("{rid} is outside every channel listing {}", from.user),
            );
        }
        let op = ChannelOp {
            kind: ChannelOpKind::Delete,
            from: from.user.clone(),
            table: table.to_string(),
            id: rid.to_string(),
            row: Some(row),
            patch: Row::new(),
        };
        if !self.accepts(&op) {
            return self.drop_msg(from, "refused by the accept rule");
        }
        let removed = self.st.borrow_mut().overlay.remove(rid).is_some();
        if removed {
            self.emit_for(&BTreeSet::from([rid.to_string()]));
        }
        if let Err(e) = rows.delete(table, rid, self.confirm_ms as u64).await {
            self.log(LogLevel::Debug, &format!("peer delete failed: {e}"));
        }
    }

    fn accepts(&self, op: &ChannelOp) -> bool {
        let Some(rule) = self.accept.get(&op.table) else {
            return true;
        };
        match catch_unwind(AssertUnwindSafe(|| rule(op))) {
            Ok(ok) => ok,
            Err(_) => {
                self.log(
                    LogLevel::Warn,
                    "channel accept rule panicked; the write is refused",
                );
                false
            }
        }
    }

    fn peer_changed(self: Rc<Self>, peer: &PeerRef, up: bool) {
        let key = peer.key();
        if up {
            {
                let mut st = self.st.borrow_mut();
                st.connected.insert(key, peer.clone());
                st.user_status
                    .insert(peer.user.clone(), PeerLinkStatus::Connected);
            }
            self.send_snapshot(peer);
        } else {
            let touched = {
                let mut st = self.st.borrow_mut();
                st.connected.remove(&key);
                let mut touched = BTreeSet::new();
                for (rid, fields) in st.overlay.iter_mut() {
                    fields.retain(|_, e| {
                        let gone = e.from == key && e.ephemeral;
                        if gone {
                            touched.insert(rid.clone());
                        }
                        !gone
                    });
                }
                st.overlay.retain(|_, f| !f.is_empty());
                touched
            };
            self.emit_for(&touched);
        }
        self.notify_all();
    }

    fn status_changed(&self, user: &str, status: PeerLinkStatus) {
        {
            let mut st = self.st.borrow_mut();
            if status != PeerLinkStatus::Connected && st.connected.values().any(|p| p.user == user)
            {
                return;
            }
            st.user_status.insert(user.to_string(), status);
        }
        self.notify_all();
    }

    /// Own ephemeral values of rows inside a channel a newly connected peer is
    /// listed on.
    fn send_snapshot(self: &Rc<Self>, peer: &PeerRef) {
        let own: Vec<(String, JsonMap<String, Json>, Hlc)> = {
            let st = self.st.borrow();
            st.overlay
                .iter()
                .filter_map(|(rid, fields)| {
                    let own: Vec<(&String, &OverlayEntry)> =
                        fields.iter().filter(|(_, e)| e.from == SELF).collect();
                    let first = own.first()?;
                    let patch = own
                        .iter()
                        .map(|(f, e)| ((*f).clone(), super::encode_value(&e.value)))
                        .collect();
                    let hlc = own
                        .iter()
                        .map(|(_, e)| &e.hlc)
                        .max()
                        .unwrap_or(&first.1.hlc)
                        .clone();
                    Some((rid.clone(), patch, hlc))
                })
                .collect()
        };
        for (rid, patch, hlc) in own {
            let table = table_of(&rid).to_string();
            if !self.listed_on(&table, &peer.user) {
                continue;
            }
            let (inner, peer) = (self.clone(), peer.clone());
            tokio::task::spawn_local(async move {
                let Some(row) = inner.row_for(&rid).await else {
                    return;
                };
                if !inner.matches_for(&table, &[row], &peer.user) {
                    return;
                }
                let transport = inner.st.borrow().transport.clone();
                if let Some(t) = transport {
                    t.send(
                        &peer,
                        &ChannelMessage {
                            kind: ChannelOpKind::Update,
                            table,
                            id: rid,
                            patch,
                            hlc,
                        },
                    );
                }
            });
        }
    }

    /// Fold the overlay into `tap`'s rows for its callback, and let synced rows
    /// confirm the peer values they now hold.
    fn prepare_emit(st: &mut State, tap_id: u64) -> Option<(TapCallback, Arc<Vec<Row>>)> {
        let tap = st.taps.get(&tap_id)?;
        let raw = tap.raw.clone()?;
        let rows = apply_overlay(&raw, &st.overlay);
        confirm(&mut st.overlay, &raw);
        Some((tap.cb.clone(), rows))
    }

    fn emit_tap(&self, tap_id: u64) {
        let out = Self::prepare_emit(&mut self.st.borrow_mut(), tap_id);
        if let Some((cb, rows)) = out {
            cb(&rows);
        }
    }

    fn emit_for(&self, rids: &BTreeSet<String>) {
        if rids.is_empty() {
            return;
        }
        let calls: Vec<(TapCallback, Arc<Vec<Row>>)> = {
            let mut st = self.st.borrow_mut();
            let hit: Vec<u64> = st
                .taps
                .iter_mut()
                .filter(|(_, tap)| tap.raw.is_some())
                .filter_map(|(id, tap)| {
                    let ids = tap.ids();
                    rids.iter().any(|r| ids.contains(r)).then_some(*id)
                })
                .collect();
            hit.into_iter()
                .filter_map(|id| Self::prepare_emit(&mut st, id))
                .collect()
        };
        for (cb, rows) in calls {
            cb(&rows);
        }
    }

    fn ensure_sweep(self: &Rc<Self>) {
        let mut st = self.st.borrow_mut();
        if st.sweep.is_some() || st.closed {
            return;
        }
        let weak = Rc::downgrade(self);
        st.sweep = Some(tokio::task::spawn_local(async move {
            loop {
                tokio::time::sleep(SWEEP_EVERY).await;
                let Some(inner) = weak.upgrade() else { return };
                if !inner.sweep() {
                    return;
                }
            }
        }));
    }

    /// Drop expired peer values; false once none are left to expire.
    fn sweep(&self) -> bool {
        let now = (self.now)();
        let (touched, pending) = {
            let mut st = self.st.borrow_mut();
            let mut touched = BTreeSet::new();
            let mut pending = false;
            for (rid, fields) in st.overlay.iter_mut() {
                fields.retain(|_, e| match e.expires_at {
                    None => true,
                    Some(at) if at <= now => {
                        touched.insert(rid.clone());
                        false
                    }
                    Some(_) => {
                        pending = true;
                        true
                    }
                });
            }
            st.overlay.retain(|_, f| !f.is_empty());
            if !pending {
                st.sweep = None;
            }
            (touched, pending)
        };
        self.emit_for(&touched);
        pending
    }

    fn tick(&self) -> Hlc {
        let now = (self.now)();
        let mut st = self.st.borrow_mut();
        st.clock = if now > st.clock.ms {
            Hlc::new(now, 0, self.peer_id.clone())
        } else {
            Hlc::new(st.clock.ms, st.clock.counter + 1, self.peer_id.clone())
        };
        st.clock.clone()
    }

    fn observe(&self, h: &Hlc) {
        let mut st = self.st.borrow_mut();
        if h.ms > st.clock.ms || (h.ms == st.clock.ms && h.counter > st.clock.counter) {
            st.clock = Hlc::new(h.ms, h.counter, self.peer_id.clone());
        }
    }

    fn state_of(&self, ch: &ChannelCell) -> ChannelState {
        let st = self.st.borrow();
        let ch = ch.borrow();
        if ch.users.is_empty() {
            return ChannelState::new(ChannelStatus::Idle);
        }
        if st.transport.as_ref().is_some_and(|t| !t.supported()) {
            return ChannelState::new(ChannelStatus::Unsupported);
        }
        let up: Vec<String> = ch
            .users
            .iter()
            .filter(|u| st.connected.values().any(|p| &p.user == *u))
            .cloned()
            .collect();
        if !up.is_empty() {
            return ChannelState {
                status: ChannelStatus::Connected,
                peers: up,
            };
        }
        if ch
            .users
            .iter()
            .all(|u| st.user_status.get(u) == Some(&PeerLinkStatus::Failed))
        {
            return ChannelState::new(ChannelStatus::Failed);
        }
        ChannelState::new(ChannelStatus::Connecting)
    }

    fn notify(&self, ch: &ChannelCell) {
        let next = self.state_of(ch);
        let listeners: Vec<Listener> = {
            let mut data = ch.borrow_mut();
            if data.state == next {
                return;
            }
            data.state = next.clone();
            data.listeners.iter().map(|(_, l)| l.clone()).collect()
        };
        for cb in listeners {
            cb(&next);
        }
    }

    fn notify_all(&self) {
        let channels = self.st.borrow().channels.clone();
        for ch in channels {
            self.notify(&ch);
        }
    }
}

/// Persistent peer values the synced rows already hold are done showing.
fn confirm(overlay: &mut Overlay, raw: &[Row]) {
    if overlay.is_empty() {
        return;
    }
    for rid in collect_ids(raw) {
        let Some(fields) = overlay.get_mut(&rid) else {
            continue;
        };
        let Some(row) = find_row(raw, &rid) else {
            continue;
        };
        fields.retain(|f, e| {
            !(!e.ephemeral && e.from != SELF && row.get(f).is_some_and(|v| same_value(v, &e.value)))
        });
        if fields.is_empty() {
            overlay.remove(&rid);
        }
    }
}

struct Hooks(Weak<Inner>);

impl TransportHooks for Hooks {
    fn self_ref(&self) -> Option<PeerRef> {
        let inner = self.0.upgrade()?;
        let user = inner.st.borrow().user.clone()?;
        Some(PeerRef::new(user, inner.peer_id.clone()))
    }

    fn on_message(&self, from: &PeerRef, message: &Json) {
        if let Some(inner) = self.0.upgrade() {
            inner.receive(from, message);
        }
    }

    fn on_peer(&self, peer: &PeerRef, up: bool) {
        if let Some(inner) = self.0.upgrade() {
            inner.peer_changed(peer, up);
        }
    }

    fn on_status(&self, user: &str, status: PeerLinkStatus) {
        if let Some(inner) = self.0.upgrade() {
            inner.status_changed(user, status);
        }
    }
}

/// An open channel. The engine's `channel(...)` wraps it with scoped writes.
#[derive(Clone)]
pub struct ChannelHandle {
    manager: Weak<Inner>,
    channel: ChannelCell,
}

impl ChannelHandle {
    /// The tables its queries read.
    pub fn tables(&self) -> Vec<String> {
        self.channel.borrow().tables().into_iter().collect()
    }

    pub fn state(&self) -> ChannelState {
        self.channel.borrow().state.clone()
    }

    pub fn is_closed(&self) -> bool {
        self.channel.borrow().closed
    }

    /// Called at once with the current state, then on every change, until the
    /// returned subscription is dropped.
    pub fn subscribe(&self, cb: impl Fn(&ChannelState) + 'static) -> Subscription {
        let state = self.state();
        cb(&state);
        let mut ch = self.channel.borrow_mut();
        if ch.closed {
            return Subscription::noop();
        }
        ch.next_listener += 1;
        let id = ch.next_listener;
        ch.listeners.push((id, Rc::new(cb)));
        let weak = Rc::downgrade(&self.channel);
        Subscription::new(move || {
            if let Some(ch) = weak.upgrade() {
                ch.borrow_mut().listeners.retain(|(i, _)| *i != id);
            }
        })
    }

    /// Replace who it connects to (your own id is skipped).
    pub fn set_peers(&self, peers: Vec<Value>) {
        let Some(inner) = self.manager.upgrade() else {
            return;
        };
        {
            let user = inner.st.borrow().user.clone();
            let mut ch = self.channel.borrow_mut();
            if ch.closed {
                return;
            }
            ch.users = peer_users(&peers, user.as_deref());
            ch.listed = peers;
        }
        inner.sync_wanted();
        inner.notify(&self.channel);
    }

    /// Replace what it carries (the queries' filters changed).
    pub fn set_scopes(&self, scopes: Vec<ChannelScope>) -> Result<()> {
        let mut ch = self.channel.borrow_mut();
        if ch.closed {
            return Ok(());
        }
        if scopes.is_empty() {
            return Err(Error::InvalidArgument(
                "A channel needs at least one query.".into(),
            ));
        }
        ch.scopes = scopes;
        Ok(())
    }

    pub fn close(&self) {
        {
            let mut ch = self.channel.borrow_mut();
            if ch.closed {
                return;
            }
            ch.closed = true;
            ch.listeners.clear();
        }
        let Some(inner) = self.manager.upgrade() else {
            return;
        };
        inner
            .st
            .borrow_mut()
            .channels
            .retain(|c| !Rc::ptr_eq(c, &self.channel));
        inner.sync_wanted();
    }
}

/// A subscription followed by the manager (see [`ChannelManager::tap`]).
pub struct TapHandle {
    manager: Weak<Inner>,
    id: u64,
}

impl TapHandle {
    /// The subscription's raw rows changed.
    pub fn feed(&self, rows: &Arc<Vec<Row>>) {
        let Some(inner) = self.manager.upgrade() else {
            return;
        };
        {
            let mut st = inner.st.borrow_mut();
            let Some(tap) = st.taps.get_mut(&self.id) else {
                return;
            };
            tap.raw = Some(rows.clone());
            tap.ids = None;
        }
        inner.emit_tap(self.id);
    }

    pub fn stop(&self) {
        if let Some(inner) = self.manager.upgrade() {
            inner.st.borrow_mut().taps.remove(&self.id);
        }
    }
}

impl Drop for TapHandle {
    fn drop(&mut self) {
        self.stop();
    }
}
