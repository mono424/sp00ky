//! Peer to peer channels (`-- @channel` tables, `engine.channel(queries, peers)`).
//!
//! PORT: `packages/spooky_core/lib/src/modules/channel/channel.dart`, itself a
//! port of `packages/core/src/modules/channel/index.ts`; the messages, the
//! signaling rows and the WebRTC negotiation are the same on the wire, so a
//! Rust client, a Dart client and a browser client talk to each other.
//!
//! A channel is a filter (the `where` of one or more queries on `@channel`
//! tables) plus the users it connects to. Writes to rows that match the filter
//! reach the users' devices straight over the transport (WebRTC by default),
//! and their clients fold them in before the database has seen them. The filter
//! is evaluated here, in plain Rust, against the row; the query is never
//! registered. Optimistic writes, distributed:
//!
//! - Updates: persistent fields still go through the normal outbox. A peer
//!   shows the sent value until its own synced row catches up to it, or for
//!   `confirm_timeout_ms` at most, so a value the server rejected (or that a
//!   peer made up) falls back to the server's on its own.
//! - `-- @ephemeral` fields never reach the database. They live in memory, last
//!   writer wins, and a peer's values vanish when it disconnects.
//! - Creates go into the peer's store and circuit like its own optimistic
//!   create (minus the outbox); deletes hide the row. Both are undone unless the
//!   server confirms them (see the peer sagas in `mutation/write.rs`).
//!
//! Nothing here enforces permissions beyond identity: the transport proves
//! which user a peer is (the database stamps the signaling rows), a message is
//! only taken from a user listed by an open channel whose filter the row
//! matches, and fields must exist and fit their column type. Everything else is
//! the app's call, through [`ChannelsConfig::accept`].
//!
//! Update and ephemeral values are folded into subscription output only, never
//! into the local store.

mod manager;
pub mod webrtc;

#[cfg(test)]
pub(crate) mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::{Arc, LazyLock};

use base64::Engine as _;
use chrono::{DateTime, SecondsFormat, Utc};
use regex::Regex;
use serde_json::{json, Map as JsonMap, Value as Json};

use crate::error::{Error, Result};
use crate::modules::query_builder::{QueryBuilder, WhereClause, WhereValue};
use crate::types::{ColumnSchema, Schema};
use crate::utils::parse_datetime;
use crate::value::{Object, Row, Value};

pub use manager::{
    ChannelHandle, ChannelLogger, ChannelManager, ChannelRows, LocalTransportFactory,
    ManagerOptions, TapHandle,
};
pub use webrtc::{
    surreal_signal, webrtc_transport, ConnectionStateCallback, DataChannelCallback,
    IceCandidateCallback, OutgoingSignal, RtcDataChannel, RtcDescription, RtcPeerConnection,
    RtcPeerConnectionFactory, Signal, SignalKind, SignalPort, SignalRemote, WebRtcOptions,
    SIGNAL_TABLE,
};

/// Hybrid logical clock: wall ms, counter, peer id. Orders every write (the
/// derived order compares the fields in that order).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hlc {
    pub ms: i64,
    pub counter: i64,
    pub peer: String,
}

impl Hlc {
    pub fn new(ms: i64, counter: i64, peer: impl Into<String>) -> Self {
        Hlc {
            ms,
            counter,
            peer: peer.into(),
        }
    }

    /// `[ms, counter, peer]`, as the browser client sends it.
    pub fn to_json(&self) -> Json {
        json!([self.ms, self.counter, self.peer])
    }
}

/// One device (one client instance) of one user.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerRef {
    pub user: String,
    pub peer: String,
}

impl PeerRef {
    pub fn new(user: impl Into<String>, peer: impl Into<String>) -> Self {
        PeerRef {
            user: user.into(),
            peer: peer.into(),
        }
    }

    /// `user#peer`: unique per device, and what orders the two ends of a link.
    pub fn key(&self) -> String {
        format!("{}#{}", self.user, self.peer)
    }
}

impl std::fmt::Display for PeerRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.user, self.peer)
    }
}

/// What a message does to its row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ChannelOpKind {
    #[default]
    Update,
    Create,
    Delete,
}

impl ChannelOpKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelOpKind::Update => "update",
            ChannelOpKind::Create => "create",
            ChannelOpKind::Delete => "delete",
        }
    }
}

/// The one message channels exchange: a write to one row.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelMessage {
    pub kind: ChannelOpKind,
    pub table: String,
    pub id: String,
    /// Wire-encoded values (see [`encode_value`]): the patch, the whole row for
    /// a create, `{}` for a delete.
    pub patch: JsonMap<String, Json>,
    pub hlc: Hlc,
}

impl ChannelMessage {
    /// The JSON a data channel carries. `kind` is left out for an update, as
    /// the browser client does.
    pub fn to_json(&self) -> Json {
        let mut out = JsonMap::new();
        out.insert("t".into(), Json::from("op"));
        if self.kind != ChannelOpKind::Update {
            out.insert("kind".into(), Json::from(self.kind.as_str()));
        }
        out.insert("table".into(), Json::from(self.table.clone()));
        out.insert("id".into(), Json::from(self.id.clone()));
        out.insert("patch".into(), Json::Object(self.patch.clone()));
        out.insert("hlc".into(), self.hlc.to_json());
        Json::Object(out)
    }
}

/// How a channel is doing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ChannelStatus {
    #[default]
    Idle,
    Connecting,
    Connected,
    Failed,
    Unsupported,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ChannelState {
    pub status: ChannelStatus,
    /// Users with at least one device connected.
    pub peers: Vec<String>,
}

impl ChannelState {
    pub fn new(status: ChannelStatus) -> Self {
        ChannelState {
            status,
            peers: Vec::new(),
        }
    }
}

/// What an `accept` rule decides on.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelOp {
    pub kind: ChannelOpKind,
    /// The sending user, as the database proved it.
    pub from: String,
    pub table: String,
    pub id: String,
    /// The row as this client currently shows it, overlay included; `None` for
    /// a create.
    pub row: Option<Row>,
    /// Decoded field values: the patch, the new row for a create, `{}` for a
    /// delete.
    pub patch: Row,
}

/// Decide whether to apply a peer's write. A rule that panics refuses.
pub type ChannelAcceptRule = Arc<dyn Fn(&ChannelOp) -> bool + Send + Sync>;

// ---- transport ---------------------------------------------------------------------

/// Per user: an attempt is running, a device is connected, or attempts failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PeerLinkStatus {
    Connecting,
    Connected,
    Failed,
}

/// How a transport reports to the channel manager. Calls may arrive from
/// inside [`ChannelTransport::want`] or [`ChannelTransport::close`].
pub trait TransportHooks {
    /// This device, or `None` while signed out.
    fn self_ref(&self) -> Option<PeerRef>;
    fn on_message(&self, from: &PeerRef, message: &Json);
    fn on_peer(&self, peer: &PeerRef, up: bool);
    fn on_status(&self, user: &str, status: PeerLinkStatus);
}

/// Moves messages between devices. WebRTC by default; an app that already has
/// a room (LiveKit, its own relay) can plug that in. [`ChannelTransport::send`]
/// is fire and forget.
pub trait ChannelTransport {
    fn supported(&self) -> bool;
    /// Users this device wants connections to: the union over open channels.
    fn want(&self, users: &BTreeSet<String>);
    fn send(&self, to: &PeerRef, message: &ChannelMessage);
    fn close(&self);
}

/// Builds the transport for one signed-in session. Runs on the engine thread;
/// the transport it returns stays there.
pub type ChannelTransportFactory =
    Arc<dyn Fn(Rc<dyn TransportHooks>) -> Rc<dyn ChannelTransport> + Send + Sync>;

/// Peer to peer channels for `-- @channel` tables (TS `channels` config).
///
/// `Send + Sync` like the rest of [`crate::Sp00kyConfig`]: the thread-safe
/// client moves its config onto the engine thread. The factories run there and
/// may build thread-bound (`Rc`) objects.
#[derive(Clone)]
pub struct ChannelsConfig {
    /// Per table: decide whether to apply a peer's write. Runs after the
    /// built-in checks (sender listed by an open channel whose filter the row
    /// matches, fields exist and fit). Without a rule every such write is
    /// applied.
    pub accept: BTreeMap<String, ChannelAcceptRule>,
    /// STUN/TURN servers for the default WebRTC transport, in the
    /// `RTCConfiguration.iceServers` shape (`{"urls": "stun:..."}`). A public
    /// STUN server when omitted; add TURN for networks that block direct
    /// connections.
    pub ice_servers: Option<Vec<Json>>,
    /// Builds WebRTC peer connections for the default transport. The core has
    /// no WebRTC stack of its own: the app adapts one. Without it (and without
    /// [`ChannelsConfig::transport`]) channels report
    /// [`ChannelStatus::Unsupported`].
    pub rtc: Option<RtcPeerConnectionFactory>,
    /// Replace the WebRTC transport.
    pub transport: Option<ChannelTransportFactory>,
    /// How long a peer's persistent value shows without the server confirming
    /// it.
    pub confirm_timeout_ms: u64,
}

impl Default for ChannelsConfig {
    fn default() -> Self {
        ChannelsConfig {
            accept: BTreeMap::new(),
            ice_servers: None,
            rtc: None,
            transport: None,
            confirm_timeout_ms: 5000,
        }
    }
}

impl ChannelsConfig {
    /// Builder: an accept rule for `table`.
    pub fn accept(
        mut self,
        table: impl Into<String>,
        rule: impl Fn(&ChannelOp) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.accept.insert(table.into(), Arc::new(rule));
        self
    }
}

impl std::fmt::Debug for ChannelsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelsConfig")
            .field("accept", &self.accept.keys().collect::<Vec<_>>())
            .field("ice_servers", &self.ice_servers)
            .field("rtc", &self.rtc.is_some())
            .field("transport", &self.transport.is_some())
            .field("confirm_timeout_ms", &self.confirm_timeout_ms)
            .finish()
    }
}

// ---- pure helpers ----------------------------------------------------------------------

/// `table:id` for a record id in any spelling, or `None`.
pub fn id_key(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => s.contains(':').then(|| s.clone()),
        Value::RecordId(r) => Some(r.to_string()),
        Value::Object(o) if o.contains_key("table") && o.contains_key("id") => {
            let name = match o.get("table") {
                Some(Value::String(s)) => Some(s.clone()),
                Some(Value::Object(t)) => t.get("name").and_then(Value::as_str).map(str::to_string),
                _ => None,
            }?;
            Some(format!(
                "{name}:{}",
                o.get("id").map(Value::to_id_string).unwrap_or_default()
            ))
        }
        _ => None,
    }
}

/// An ISO string with millisecond precision, as JS `Date.toISOString()` writes
/// it: what a browser peer can parse back.
fn iso(d: &DateTime<Utc>) -> String {
    let ms = DateTime::from_timestamp_millis(d.timestamp_millis()).unwrap_or(*d);
    ms.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// JSON-safe form of a field value: record ids, dates and bytes are tagged.
pub fn encode_value(v: &Value) -> Json {
    match v {
        Value::None | Value::Null => Json::Null,
        Value::RecordId(r) => json!({"$rid": r.to_string()}),
        Value::Datetime(d) => json!({"$date": iso(d)}),
        Value::Bytes(b) => json!({"$bytes": base64::engine::general_purpose::STANDARD.encode(b)}),
        Value::Array(a) => Json::Array(a.iter().map(encode_value).collect()),
        Value::Object(o) => Json::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), encode_value(v)))
                .collect(),
        ),
        other => other.to_json(),
    }
}

/// A wire value back in the shape this client's rows hold it: the local store
/// keeps documents as JSON, so record ids are `table:id` strings and dates ISO
/// strings. Bytes come back as [`Value::Bytes`].
pub fn decode_value(v: &Json) -> Value {
    match v {
        Json::Array(a) => Value::Array(a.iter().map(decode_value).collect()),
        Json::Object(o) => {
            if o.len() == 1 {
                if let Some(Json::String(rid)) = o.get("$rid") {
                    return Value::String(rid.clone());
                }
                if let Some(Json::String(date)) = o.get("$date") {
                    return Value::String(
                        parse_datetime(date)
                            .map(|d| iso(&d))
                            .unwrap_or_else(|| date.clone()),
                    );
                }
                if let Some(Json::String(bytes)) = o.get("$bytes") {
                    return match base64::engine::general_purpose::STANDARD.decode(bytes) {
                        Ok(b) => Value::Bytes(b),
                        Err(_) => Value::String(bytes.clone()),
                    };
                }
            }
            Value::Object(
                o.iter()
                    .map(|(k, v)| (k.clone(), decode_value(v)))
                    .collect(),
            )
        }
        other => Value::from(other),
    }
}

/// Equal as values, or as their wire encoding (a record and its string
/// spelling, a date and its ISO string). `None` and `Null` are the same.
pub fn same_value(a: &Value, b: &Value) -> bool {
    if a.is_nullish() && b.is_nullish() {
        return true;
    }
    a == b || encode_value(a) == encode_value(b)
}

fn json_int(v: &Json) -> Option<i64> {
    v.as_i64().or_else(|| v.as_f64().map(|f| f as i64))
}

/// A well-formed `op` message, or `None`.
pub fn parse_message(raw: &Json) -> Option<ChannelMessage> {
    let raw = raw.as_object()?;
    let kind = match raw.get("kind") {
        None | Some(Json::Null) => ChannelOpKind::Update,
        Some(Json::String(k)) if k == "update" => ChannelOpKind::Update,
        Some(Json::String(k)) if k == "create" => ChannelOpKind::Create,
        Some(Json::String(k)) if k == "delete" => ChannelOpKind::Delete,
        _ => return None,
    };
    if raw.get("t").and_then(Json::as_str) != Some("op") {
        return None;
    }
    let table = raw.get("table")?.as_str()?;
    let id = raw.get("id")?.as_str()?;
    let patch = raw.get("patch")?.as_object()?;
    let hlc = raw.get("hlc")?.as_array()?;
    if hlc.len() != 3 || !hlc[0].is_number() || !hlc[1].is_number() {
        return None;
    }
    Some(ChannelMessage {
        kind,
        table: table.to_string(),
        id: id.to_string(),
        patch: patch.clone(),
        hlc: Hlc::new(json_int(&hlc[0])?, json_int(&hlc[1])?, hlc[2].as_str()?),
    })
}

static ARRAY_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^array\s*(?:<\s*(.*?)\s*(?:,\s*\d+\s*)?>)?$").unwrap());
static OPTION_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^option\s*<\s*(.*)\s*>$").unwrap());

/// Whether a decoded value fits a column, loosely (the shapes the client
/// itself writes).
pub fn fits_column(col: &ColumnSchema, v: &Value) -> bool {
    if v.is_nullish() {
        return col.optional;
    }
    if col.record_id {
        return matches!(v, Value::String(_) | Value::RecordId(_));
    }
    if col.date_time {
        return matches!(v, Value::String(_) | Value::Datetime(_));
    }
    fits_type(&col.kind.as_deref().unwrap_or("").trim().to_lowercase(), v)
}

fn fits_type(kind: &str, v: &Value) -> bool {
    if let Some(option) = OPTION_TYPE.captures(kind) {
        return v.is_nullish() || fits_type(&option[1], v);
    }
    if v.is_nullish() {
        return false;
    }
    if let Some(array) = ARRAY_TYPE.captures(kind) {
        let Value::Array(items) = v else { return false };
        return match array.get(1).map(|m| m.as_str()) {
            None | Some("") => true,
            Some(inner) => items.iter().all(|x| fits_type(inner, x)),
        };
    }
    if kind.starts_with("record") {
        return matches!(v, Value::String(_) | Value::RecordId(_));
    }
    match kind {
        "string" | "uuid" => matches!(v, Value::String(_)),
        "int" => match v {
            Value::Int(_) => true,
            Value::Float(f) => f.is_finite() && f.fract() == 0.0,
            _ => false,
        },
        "number" | "float" | "decimal" => match v {
            Value::Int(_) => true,
            Value::Float(f) => f.is_finite(),
            _ => false,
        },
        "bool" => matches!(v, Value::Bool(_)),
        "datetime" => matches!(v, Value::String(_) | Value::Datetime(_)),
        "bytes" => matches!(v, Value::Bytes(_)),
        "object" => matches!(v, Value::Object(_)),
        _ => true,
    }
}

/// Why a peer patch is refused against the table's columns, or `None` when it
/// fits.
pub fn check_patch(
    columns: &BTreeMap<String, ColumnSchema>,
    patch: &Row,
    allow_empty: bool,
) -> Option<String> {
    if patch.is_empty() && !allow_empty {
        return Some("empty patch".into());
    }
    for (field, value) in patch {
        if field == "id" {
            return Some("patches the id".into());
        }
        let Some(col) = columns.get(field) else {
            return Some(format!("unknown field '{field}'"));
        };
        if !fits_column(col, value) {
            return Some(format!("'{field}' does not fit its column type"));
        }
    }
    None
}

/// A patch split into the fields the database gets and the ones only peers
/// get.
pub fn split_patch(columns: &BTreeMap<String, ColumnSchema>, data: &Row) -> (Row, Option<Row>) {
    let mut persistent = Row::new();
    let mut ephemeral: Option<Row> = None;
    for (key, value) in data {
        if columns.get(key).is_some_and(|c| c.ephemeral) {
            ephemeral
                .get_or_insert_with(Row::new)
                .insert(key.clone(), value.clone());
        } else {
            persistent.insert(key.clone(), value.clone());
        }
    }
    (persistent, ephemeral)
}

/// One overlaid field: the value, who wrote it and when.
#[derive(Clone, Debug, PartialEq)]
pub struct OverlayEntry {
    pub value: Value,
    pub hlc: Hlc,
    /// [`PeerRef::key`] of the author, or `self`.
    pub from: String,
    pub ephemeral: bool,
    /// When a persistent peer value stops showing; `None` for ephemeral and own
    /// values.
    pub expires_at: Option<i64>,
}

/// Per record id, per field.
pub type Overlay = BTreeMap<String, BTreeMap<String, OverlayEntry>>;

const OVERLAY_DEPTH: usize = 4;

/// Rows with the overlay folded in, nested related rows included. When nothing
/// changes the same `Arc` comes back, and untouched rows are never copied.
pub fn apply_overlay(rows: &Arc<Vec<Row>>, overlay: &Overlay) -> Arc<Vec<Row>> {
    if overlay.is_empty() {
        return rows.clone();
    }
    let mut next: Option<Vec<Row>> = None;
    for (i, row) in rows.iter().enumerate() {
        if let Some(folded) = fold_row(row, overlay, 0) {
            next.get_or_insert_with(|| rows.as_ref().clone())[i] = folded;
        }
    }
    next.map(Arc::new).unwrap_or_else(|| rows.clone())
}

/// One row with the overlay folded in.
pub fn apply_overlay_row(row: &Row, overlay: &Overlay) -> Row {
    fold_row(row, overlay, 0).unwrap_or_else(|| row.clone())
}

fn fold_list(list: &[Value], overlay: &Overlay, depth: usize) -> Option<Vec<Value>> {
    let mut next: Option<Vec<Value>> = None;
    for (i, x) in list.iter().enumerate() {
        if let Some(y) = fold_value(x, overlay, depth) {
            next.get_or_insert_with(|| list.to_vec())[i] = y;
        }
    }
    next
}

fn fold_value(v: &Value, overlay: &Overlay, depth: usize) -> Option<Value> {
    if depth > OVERLAY_DEPTH {
        return None;
    }
    match v {
        Value::Array(a) => fold_list(a, overlay, depth + 1).map(Value::Array),
        Value::Object(o) => fold_row(o, overlay, depth).map(Value::Object),
        _ => None,
    }
}

/// A map at `depth`, folded; `None` when nothing changed.
fn fold_row(v: &Row, overlay: &Overlay, depth: usize) -> Option<Row> {
    if depth > OVERLAY_DEPTH {
        return None;
    }
    let mut next: Option<Row> = None;
    for (key, x) in v {
        if key == "id" {
            continue;
        }
        if let Some(y) = fold_value(x, overlay, depth + 1) {
            next.get_or_insert_with(|| v.clone()).insert(key.clone(), y);
        }
    }
    let fields = v
        .get("id")
        .and_then(id_key)
        .and_then(|rid| overlay.get(&rid));
    if let Some(fields) = fields {
        for (field, entry) in fields {
            if !same_value(v.get(field).unwrap_or(&Value::Null), &entry.value) {
                next.get_or_insert_with(|| v.clone())
                    .insert(field.clone(), entry.value.clone());
            }
        }
    }
    next
}

/// Every record id in rows, nested related rows included.
pub fn collect_ids(rows: &[Row]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for row in rows {
        collect_map(row, &mut out, 0);
    }
    out
}

fn collect_item(v: &Value, out: &mut BTreeSet<String>, depth: usize) {
    match v {
        Value::Array(a) => collect_list(a, out, depth + 1),
        Value::Object(o) => collect_map(o, out, depth),
        _ => {}
    }
}

fn collect_list(items: &[Value], out: &mut BTreeSet<String>, depth: usize) {
    if depth > OVERLAY_DEPTH {
        return;
    }
    for v in items {
        collect_item(v, out, depth);
    }
}

fn collect_map(o: &Row, out: &mut BTreeSet<String>, depth: usize) {
    if let Some(rid) = o.get("id").and_then(id_key) {
        out.insert(rid);
    }
    for (key, value) in o {
        if key != "id"
            && matches!(value, Value::Array(_) | Value::Object(_))
            && depth < OVERLAY_DEPTH
        {
            collect_item(value, out, depth + 1);
        }
    }
}

/// The row with record id `rid`, nested related rows included.
pub(crate) fn find_row<'a>(rows: &'a [Row], rid: &str) -> Option<&'a Row> {
    rows.iter().find_map(|row| find_in_map(row, rid, 0))
}

fn find_in_item<'a>(v: &'a Value, rid: &str, depth: usize) -> Option<&'a Row> {
    match v {
        Value::Array(a) if depth < OVERLAY_DEPTH => {
            a.iter().find_map(|x| find_in_item(x, rid, depth + 1))
        }
        Value::Object(o) => find_in_map(o, rid, depth),
        _ => None,
    }
}

fn find_in_map<'a>(o: &'a Row, rid: &str, depth: usize) -> Option<&'a Row> {
    if o.get("id").and_then(id_key).as_deref() == Some(rid) {
        return Some(o);
    }
    if depth >= OVERLAY_DEPTH {
        return None;
    }
    o.iter()
        .filter(|(k, v)| *k != "id" && matches!(v, Value::Array(_) | Value::Object(_)))
        .find_map(|(_, v)| find_in_item(v, rid, depth + 1))
}

/// Normalized peer ids, without `self_user`.
pub fn peer_users(peers: &[Value], self_user: Option<&str>) -> BTreeSet<String> {
    peers
        .iter()
        .filter_map(id_key)
        .filter(|k| Some(k.as_str()) != self_user)
        .collect()
}

// ---- filters ----------------------------------------------------------------------------

/// One comparison of a channel filter: `field op value`, or `value op field`
/// when `swap` is set.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelCondition {
    pub field: String,
    pub op: String,
    pub value: Value,
    pub swap: bool,
}

impl ChannelCondition {
    pub fn new(field: impl Into<String>, op: impl Into<String>, value: impl Into<Value>) -> Self {
        ChannelCondition {
            field: field.into(),
            op: op.into(),
            value: value.into(),
            swap: false,
        }
    }

    pub fn swapped(
        field: impl Into<String>,
        op: impl Into<String>,
        value: impl Into<Value>,
    ) -> Self {
        ChannelCondition {
            swap: true,
            ..ChannelCondition::new(field, op, value)
        }
    }
}

/// What a channel carries: the rows of one `@channel` table its filter
/// matches.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelScope {
    pub table: String,
    /// AND-joined, like the query builder's `where`.
    pub where_: Vec<ChannelCondition>,
}

type OpFn = fn(&Value, &Value) -> bool;

fn op_fn(op: &str) -> Option<OpFn> {
    Some(match op {
        "=" | "==" => same_key,
        "!=" => |a, b| !same_key(a, b),
        ">" => |a, b| order(a, b).is_some_and(|c| c.is_gt()),
        ">=" => |a, b| order(a, b).is_some_and(|c| c.is_ge()),
        "<" => |a, b| order(a, b).is_some_and(|c| c.is_lt()),
        "<=" => |a, b| order(a, b).is_some_and(|c| c.is_le()),
        "IN" | "INSIDE" | "∈" => is_in,
        "NOT IN" | "NOTINSIDE" | "∉" => |a, b| !is_in(a, b),
        "CONTAINS" | "∋" => contains,
        "CONTAINSNOT" | "∌" => |a, b| !contains(a, b),
        _ => return None,
    })
}

fn is_in(a: &Value, b: &Value) -> bool {
    matches!(b, Value::Array(items) if items.iter().any(|x| same_key(a, x)))
}

fn contains(a: &Value, b: &Value) -> bool {
    matches!(a, Value::Array(items) if items.iter().any(|x| same_key(x, b)))
}

/// A date compared against a string spelling of one (rows hold ISO strings).
fn date_ms(v: &Value) -> Option<i64> {
    match v {
        Value::Datetime(d) => Some(d.timestamp_millis()),
        Value::String(s) => parse_datetime(s).map(|d| d.timestamp_millis()),
        _ => None,
    }
}

/// Equal by comparable form: record ids by `table:id`, dates by ms, numbers by
/// value, maps and lists structurally.
fn same_key(a: &Value, b: &Value) -> bool {
    if matches!(a, Value::Datetime(_)) || matches!(b, Value::Datetime(_)) {
        return date_ms(a).is_some_and(|x| Some(x) == date_ms(b));
    }
    if a.is_nullish() || b.is_nullish() {
        return a.is_nullish() && b.is_nullish();
    }
    a == b || same_value(a, b)
}

/// An ordering comparison; values of different kinds never compare.
fn order(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    if matches!(a, Value::Datetime(_)) || matches!(b, Value::Datetime(_)) {
        return Some(date_ms(a)?.cmp(&date_ms(b)?));
    }
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::Int(_) | Value::Float(_), Value::Int(_) | Value::Float(_)) => {
            a.as_f64()?.partial_cmp(&b.as_f64()?)
        }
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::RecordId(_) | Value::String(_), Value::RecordId(_) | Value::String(_)) => {
            Some(a.to_id_string().cmp(&b.to_id_string()))
        }
        _ => None,
    }
}

fn path_value<'a>(row: &'a Row, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut v = row.get(parts.next()?)?;
    for part in parts {
        v = v.as_object()?.get(part)?;
    }
    Some(v)
}

fn matches_condition(c: &ChannelCondition, row: &Row) -> bool {
    let Some(op) = op_fn(&c.op) else { return false };
    let field = path_value(row, &c.field).unwrap_or(&Value::Null);
    if c.swap {
        op(&c.value, field)
    } else {
        op(field, &c.value)
    }
}

/// Whether `row` passes a channel filter (a conjunction of comparisons).
pub fn matches_where(where_: &[ChannelCondition], row: &Row) -> bool {
    where_.iter().all(|c| matches_condition(c, row))
}

/// The parts of a channel query that do nothing there (a channel only reads
/// the `where`). Not an error, worth a warning unless `.ignore_unused_args()`.
pub fn unused_args(q: &QueryBuilder) -> Vec<&'static str> {
    let mut out = Vec::new();
    if q.is_one() {
        out.push(".one()");
    }
    if !q.order_fields().is_empty() {
        out.push("orderBy");
    }
    if q.limit_value().is_some() {
        out.push("limit");
    }
    if q.offset_value().is_some() {
        out.push("offset");
    }
    if q.select_fields().iter().any(|f| f != "*") {
        out.push("select");
    }
    out
}

pub fn is_channel_table(schema: &Schema, table: &str) -> bool {
    schema.tables.get(table).is_some_and(|t| t.channel)
}

/// A query as a channel scope, or an [`Error::InvalidArgument`] saying why it
/// cannot be one: the table must be `@channel`, and the filter must be
/// decidable from the row alone (its own fields, literal values, supported
/// operators, no OR groups). Order, limit and projection do not matter to a
/// channel and are ignored.
pub fn compile_scope(schema: &Schema, q: &QueryBuilder) -> Result<ChannelScope> {
    let table = q.table();
    if !is_channel_table(schema, table) {
        return Err(Error::InvalidArgument(format!(
            "Cannot open a channel on '{table}': the table is not marked '-- @channel'. Add '-- @channel' above \
             'DEFINE TABLE {table}' and regenerate the schema."
        )));
    }
    if !q.relations().is_empty() {
        return Err(Error::InvalidArgument(format!(
            "A channel query on '{table}' cannot use .related(): its filter must be decidable from the row alone."
        )));
    }
    let columns = schema.columns(table);
    let mut where_ = Vec::new();
    for clause in q.conditions() {
        let (field, value) = match clause {
            WhereClause::Cond { field, value } => (field, value),
            WhereClause::Or(_) => {
                return Err(Error::InvalidArgument(format!(
                    "A channel query on '{table}' cannot use an OR group: its filter is a plain conjunction."
                )))
            }
        };
        let condition = match value {
            WhereValue::Eq(v) => ChannelCondition::new(field.clone(), "=", v.clone()),
            WhereValue::Op(op) => ChannelCondition {
                field: field.clone(),
                op: op.op.clone(),
                value: op.value.clone(),
                swap: op.swap,
            },
        };
        if op_fn(&condition.op).is_none() {
            return Err(Error::InvalidArgument(format!(
                "A channel query on '{table}' cannot use the operator '{}'.",
                condition.op
            )));
        }
        if let Value::String(s) = &condition.value {
            if s.starts_with('$') {
                return Err(Error::InvalidArgument(format!(
                    "A channel query on '{table}' cannot compare '{field}' with a parameter ({s}); use a value."
                )));
            }
        }
        let root = field.split('.').next().unwrap_or(field);
        let column = columns.and_then(|c| c.get(field.as_str()).or_else(|| c.get(root)));
        // The builder holds this misuse back until `build()`; a channel never
        // builds, so it says it here.
        if column.is_some_and(|c| c.ephemeral) {
            return Err(Error::InvalidArgument(format!(
                "Cannot use '{field}' in where(): it is marked '-- @ephemeral' on {table}, so it only travels \
                 between peers and the database never holds it. Read the field from query results instead."
            )));
        }
        if field.contains('.')
            && columns
                .and_then(|c| c.get(root))
                .is_some_and(|c| c.record_id)
        {
            return Err(Error::InvalidArgument(format!(
                "A channel query on '{table}' cannot filter on '{field}': it follows a record link, which the row \
                 alone does not hold."
            )));
        }
        where_.push(condition);
    }
    Ok(ChannelScope {
        table: table.to_string(),
        where_,
    })
}

/// A compiled scope plus the query parts a channel ignores, ready to cross
/// threads (a [`QueryBuilder`] cannot). [`ChannelManager::adopt`] warns about
/// the ignored parts.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedScope {
    pub scope: ChannelScope,
    pub unused: Vec<&'static str>,
}

/// [`compile_scope`] plus the parts a channel ignores (none when the query
/// says `.ignore_unused_args()`).
pub fn prepare_scope(schema: &Schema, q: &QueryBuilder) -> Result<PreparedScope> {
    Ok(PreparedScope {
        scope: compile_scope(schema, q)?,
        unused: if q.ignores_unused_args() {
            Vec::new()
        } else {
            unused_args(q)
        },
    })
}

/// Whether a channel has a query on `table` that one of `rows` matches.
pub(crate) fn carries(scopes: &[ChannelScope], table: &str, rows: &[Row]) -> bool {
    scopes
        .iter()
        .any(|sc| sc.table == table && rows.iter().any(|r| matches_where(&sc.where_, r)))
}

/// The fields of `row` the table has as columns (no id, no nulls).
pub(crate) fn pick_columns(columns: &BTreeMap<String, ColumnSchema>, row: &Row) -> Row {
    row.iter()
        .filter(|(k, v)| *k != "id" && columns.contains_key(*k) && !v.is_nullish())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Values typed by their column before they go out, so a browser peer gets a
/// record link as a record id and a date as a date (rows hold both as
/// strings).
pub(crate) fn typed(columns: &BTreeMap<String, ColumnSchema>, patch: &Row) -> Row {
    patch
        .iter()
        .map(|(k, v)| {
            let col = columns.get(k);
            let out = match (col, v) {
                (Some(c), Value::String(s)) if c.record_id && s.contains(':') => {
                    Value::RecordId(crate::value::RecordId::parse(s))
                }
                (Some(c), Value::String(s)) if c.date_time => parse_datetime(s)
                    .map(Value::Datetime)
                    .unwrap_or_else(|| v.clone()),
                _ => v.clone(),
            };
            (k.clone(), out)
        })
        .collect()
}

/// `table:id` for an id given with or without its table.
pub(crate) fn normalize_id(table: &str, id: &str) -> String {
    if id.contains(':') {
        id.to_string()
    } else {
        format!("{table}:{id}")
    }
}

/// Wire-encode every value of a row.
pub(crate) fn encode_row(row: &Row) -> JsonMap<String, Json> {
    row.iter()
        .map(|(k, v)| (k.clone(), encode_value(v)))
        .collect()
}

/// Decode every value of a wire patch.
pub(crate) fn decode_row(patch: &JsonMap<String, Json>) -> Object {
    patch
        .iter()
        .map(|(k, v)| (k.clone(), decode_value(v)))
        .collect()
}
