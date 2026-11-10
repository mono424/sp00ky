//! PORT: `packages/spooky_core/test/modules/channel_test.dart`, plus the
//! in-memory transport of `test/channel_net.dart`.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::TimeZone;
use serde_json::{json, Value as Json};

use super::*;
use crate::kernel::events::LogLevel;
use crate::modules::query_builder::QueryOp;
use crate::testing::{row, run_local};
use crate::value::RecordId;

// ---- the in-memory transport -------------------------------------------------------------

thread_local! {
    static NETS: RefCell<HashMap<u64, Rc<NetState>>> = RefCell::new(HashMap::new());
}
static NEXT_NET: AtomicU64 = AtomicU64::new(1);

/// Devices connect as soon as both want each other's user, like the real one.
/// Thread-local, so its factory can sit in a `Send` config while two engines
/// on one thread share it.
#[derive(Clone)]
pub(crate) struct MemoryNet(Rc<NetState>);

pub(crate) struct NetState {
    nodes: RefCell<BTreeMap<String, Rc<Node>>>,
    /// Every message as it went over the wire.
    wire: RefCell<Vec<String>>,
}

struct Node {
    net: Weak<NetState>,
    me: PeerRef,
    hooks: Rc<dyn TransportHooks>,
    wanted: RefCell<BTreeSet<String>>,
    links: RefCell<BTreeSet<String>>,
}

struct NodeTransport(Rc<Node>);

impl MemoryNet {
    pub(crate) fn new() -> MemoryNet {
        let state = Rc::new(NetState {
            nodes: RefCell::new(BTreeMap::new()),
            wire: RefCell::new(Vec::new()),
        });
        MemoryNet(state)
    }

    fn attach(state: &Rc<NetState>, hooks: Rc<dyn TransportHooks>) -> Rc<dyn ChannelTransport> {
        let me = hooks
            .self_ref()
            .expect("a transport is built for a signed-in user");
        let node = Rc::new(Node {
            net: Rc::downgrade(state),
            me: me.clone(),
            hooks,
            wanted: RefCell::new(BTreeSet::new()),
            links: RefCell::new(BTreeSet::new()),
        });
        state.nodes.borrow_mut().insert(me.key(), node.clone());
        Rc::new(NodeTransport(node))
    }

    pub(crate) fn factory(&self) -> LocalTransportFactory {
        let state = self.0.clone();
        Rc::new(move |hooks| Self::attach(&state, hooks))
    }

    /// The factory as a client config holds it.
    pub(crate) fn config_factory(&self) -> ChannelTransportFactory {
        let id = NEXT_NET.fetch_add(1, Ordering::Relaxed);
        NETS.with(|n| n.borrow_mut().insert(id, self.0.clone()));
        Arc::new(move |hooks| {
            let state = NETS
                .with(|n| n.borrow().get(&id).cloned())
                .expect("net on this thread");
            Self::attach(&state, hooks)
        })
    }

    pub(crate) fn wire(&self) -> Vec<String> {
        self.0.wire.borrow().clone()
    }

    pub(crate) fn node_count(&self) -> usize {
        self.0.nodes.borrow().len()
    }
}

fn reconcile(state: &NetState) {
    let nodes: Vec<Rc<Node>> = state.nodes.borrow().values().cloned().collect();
    let mut events = Vec::new();
    for a in &nodes {
        for b in &nodes {
            if Rc::ptr_eq(a, b) {
                continue;
            }
            let kb = b.me.key();
            let mutual =
                a.wanted.borrow().contains(&b.me.user) && b.wanted.borrow().contains(&a.me.user);
            let linked = a.links.borrow().contains(&kb);
            if mutual && !linked {
                a.links.borrow_mut().insert(kb);
                events.push((a.hooks.clone(), b.me.clone(), true));
            } else if !mutual && linked {
                a.links.borrow_mut().remove(&kb);
                events.push((a.hooks.clone(), b.me.clone(), false));
            }
        }
    }
    for (hooks, peer, up) in events {
        hooks.on_peer(&peer, up);
    }
}

impl ChannelTransport for NodeTransport {
    fn supported(&self) -> bool {
        true
    }

    fn want(&self, users: &BTreeSet<String>) {
        *self.0.wanted.borrow_mut() = users.clone();
        if let Some(net) = self.0.net.upgrade() {
            reconcile(&net);
        }
    }

    fn send(&self, to: &PeerRef, message: &ChannelMessage) {
        if !self.0.links.borrow().contains(&to.key()) {
            return;
        }
        let Some(net) = self.0.net.upgrade() else {
            return;
        };
        let text = message.to_json().to_string();
        net.wire.borrow_mut().push(text.clone());
        let target = net.nodes.borrow().get(&to.key()).cloned();
        if let Some(target) = target {
            target
                .hooks
                .on_message(&self.0.me, &serde_json::from_str(&text).unwrap());
        }
    }

    fn close(&self) {
        self.0.wanted.borrow_mut().clear();
        let Some(net) = self.0.net.upgrade() else {
            return;
        };
        reconcile(&net);
        net.nodes.borrow_mut().remove(&self.0.me.key());
    }
}

// ---- fixtures ------------------------------------------------------------------------------

fn col(kind: &str) -> ColumnSchema {
    ColumnSchema::of_type(kind)
}

fn optional(kind: &str) -> ColumnSchema {
    ColumnSchema {
        optional: true,
        ..col(kind)
    }
}

fn ephemeral(kind: &str) -> ColumnSchema {
    ColumnSchema {
        ephemeral: true,
        ..optional(kind)
    }
}

fn record() -> ColumnSchema {
    ColumnSchema {
        record_id: true,
        ..col("record")
    }
}

pub(crate) fn schema() -> Arc<Schema> {
    let mut s = Schema::default()
        .with_table(
            "call",
            [
                ("caller", record()),
                ("callee", record()),
                ("view_id", optional("string")),
                ("view_pos", ephemeral("string")),
                ("count", optional("float")),
            ],
        )
        .with_table(
            "msg",
            [
                ("convo", col("string")),
                ("text", col("string")),
                ("mood", ephemeral("string")),
            ],
        )
        .with_table("note", [("title", col("string"))]);
    s.tables.get_mut("call").unwrap().channel = true;
    s.tables.get_mut("msg").unwrap().channel = true;
    Arc::new(s)
}

fn cols(table: &str) -> BTreeMap<String, ColumnSchema> {
    schema().tables[table].columns.clone()
}

fn entry(value: impl Into<Value>) -> OverlayEntry {
    OverlayEntry {
        value: value.into(),
        hlc: Hlc::new(1, 0, "p"),
        from: "x".into(),
        ephemeral: true,
        expires_at: None,
    }
}

fn q(table: &str) -> QueryBuilder {
    QueryBuilder::new(table).with_schema(schema())
}

fn all_calls() -> QueryBuilder {
    q("call")
}

fn convo(c: &str) -> QueryBuilder {
    q("msg").where_eq("convo", c)
}

/// The engine's peer sagas, as a map: refuses an id it already holds.
#[derive(Clone, Default)]
struct FakeRows(Rc<FakeRowsInner>);

#[derive(Default)]
struct FakeRowsInner {
    store: RefCell<BTreeMap<String, Row>>,
    calls: RefCell<Vec<String>>,
    reads: Cell<usize>,
}

impl FakeRows {
    fn put(&self, id: &str, r: Row) {
        self.0.store.borrow_mut().insert(id.to_string(), r);
    }

    fn get(&self, id: &str) -> Option<Row> {
        self.0.store.borrow().get(id).cloned()
    }

    fn calls(&self) -> Vec<String> {
        self.0.calls.borrow().clone()
    }
}

#[async_trait(?Send)]
impl ChannelRows for FakeRows {
    async fn create(&self, _table: &str, id: &str, r: Object, ttl_ms: u64) -> crate::Result<bool> {
        self.0
            .calls
            .borrow_mut()
            .push(format!("create {id} {ttl_ms}"));
        if self.0.store.borrow().contains_key(id) {
            return Ok(false);
        }
        let mut stored = r;
        stored.insert("id".into(), Value::from(id));
        self.put(id, stored);
        Ok(true)
    }

    async fn delete(&self, _table: &str, id: &str, ttl_ms: u64) -> crate::Result<bool> {
        self.0
            .calls
            .borrow_mut()
            .push(format!("delete {id} {ttl_ms}"));
        Ok(self.0.store.borrow_mut().remove(id).is_some())
    }

    async fn read(&self, id: &str) -> crate::Result<Option<Row>> {
        self.0.reads.set(self.0.reads.get() + 1);
        Ok(self.get(id))
    }
}

#[derive(Default)]
struct DeviceOptions {
    config: Option<ChannelsConfig>,
    now: Option<Rc<dyn Fn() -> i64>>,
    scopes: Option<Vec<QueryBuilder>>,
    peers: Option<Vec<Value>>,
}

/// One device: a manager, an open channel and one subscription to watch.
struct Device {
    m: ChannelManager,
    ch: ChannelHandle,
    tap: TapHandle,
    seen: Rc<RefCell<Vec<Arc<Vec<Row>>>>>,
    fr: FakeRows,
}

impl Device {
    fn new(user: &str, net: &MemoryNet) -> Device {
        Device::with(user, net, DeviceOptions::default())
    }

    fn with(user: &str, net: &MemoryNet, o: DeviceOptions) -> Device {
        let fr = FakeRows::default();
        let m = ChannelManager::new(
            schema(),
            ManagerOptions {
                config: o.config,
                rows: Some(Rc::new(fr.clone())),
                transport: Some(net.factory()),
                now: o.now,
                ..Default::default()
            },
        );
        m.set_user(Some(user));
        let scopes = o
            .scopes
            .unwrap_or_else(|| vec![all_calls(), convo("c1")])
            .iter()
            .map(|s| m.scope(s).unwrap())
            .collect();
        let peers = o
            .peers
            .unwrap_or_else(|| vec!["user:a".into(), "user:b".into()]);
        let ch = m.open(scopes, peers).unwrap();
        let seen: Rc<RefCell<Vec<Arc<Vec<Row>>>>> = Rc::default();
        let s = seen.clone();
        let tap = m.tap(move |rows| s.borrow_mut().push(rows.clone()));
        Device {
            m,
            ch,
            tap,
            seen,
            fr,
        }
    }

    fn feed(&self, rows: Vec<Row>) {
        self.tap.feed(&Arc::new(rows));
    }

    fn last(&self) -> Option<Row> {
        self.seen
            .borrow()
            .last()
            .and_then(|rows| rows.first().cloned())
    }

    fn last_field(&self, field: &str) -> Value {
        self.last()
            .and_then(|r| r.get(field).cloned())
            .unwrap_or(Value::Null)
    }

    fn seen_count(&self) -> usize {
        self.seen.borrow().len()
    }
}

fn call_row(extra: Json) -> Row {
    let mut r = row(json!({
        "id": "call:1",
        "caller": "user:a",
        "callee": "user:b",
        "view_id": "",
        "view_pos": null,
    }));
    r.extend(row(extra));
    r
}

fn msg(extra: Json) -> Row {
    let mut r = row(json!({"id": "msg:1", "convo": "c1", "text": "hi"}));
    r.extend(row(extra));
    r
}

fn op(m: Json) -> Json {
    let mut out = json!({"t": "op", "hlc": [chrono::Utc::now().timestamp_millis(), 0, "x"]});
    for (k, v) in m.as_object().unwrap() {
        out[k] = v.clone();
    }
    out
}

fn peer(user: &str) -> PeerRef {
    PeerRef::new(user, "x")
}

fn empty() -> Row {
    Row::new()
}

/// A one-field row.
fn one(field: &str, value: &str) -> Row {
    Row::from([(field.to_string(), Value::from(value))])
}

async fn flush() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

fn is_null(v: &Value) -> bool {
    v.is_nullish()
}

// ---- pure helpers --------------------------------------------------------------------------

#[test]
fn leaves_rows_untouched_when_the_overlay_is_empty_or_misses_them() {
    let rows = Arc::new(vec![row(json!({"id": "call:1", "view_pos": null}))]);
    assert!(Arc::ptr_eq(&apply_overlay(&rows, &Overlay::new()), &rows));
    let overlay = Overlay::from([(
        "call:2".to_string(),
        BTreeMap::from([("view_pos".to_string(), entry("3"))]),
    )]);
    assert!(Arc::ptr_eq(&apply_overlay(&rows, &overlay), &rows));
}

#[test]
fn folds_overlay_values_into_matching_rows_nested_ones_included() {
    let child = row(json!({"id": "call:c", "view_pos": null}));
    let mut parent = row(json!({"title": "t"}));
    parent.insert("id".into(), Value::RecordId(RecordId::new("note", "n")));
    parent.insert(
        "calls".into(),
        Value::Array(vec![Value::Object(child.clone())]),
    );
    let rows = Arc::new(vec![parent, row(json!({"id": "call:1", "view_pos": null}))]);
    let overlay = Overlay::from([
        (
            "call:c".to_string(),
            BTreeMap::from([("view_pos".to_string(), entry("7"))]),
        ),
        (
            "call:1".to_string(),
            BTreeMap::from([("view_pos".to_string(), entry("9"))]),
        ),
    ]);
    let out = apply_overlay(&rows, &overlay);
    let calls = out[0]["calls"].as_array().unwrap();
    assert_eq!(calls[0].get("view_pos"), Some(&Value::from("7")));
    assert_eq!(out[1]["view_pos"], Value::from("9"));
    // The input is never mutated.
    assert!(rows[0]["calls"].as_array().unwrap()[0]
        .get("view_pos")
        .unwrap()
        .is_nullish());
}

#[test]
fn round_trips_record_ids_dates_and_bytes_into_the_row_shape() {
    let value = Value::Object(Object::from([
        (
            "at".to_string(),
            Value::Datetime(
                chrono::Utc
                    .with_ymd_and_hms(2026, 10, 26, 10, 0, 0)
                    .unwrap(),
            ),
        ),
        (
            "who".to_string(),
            Value::RecordId(RecordId::new("user", "a")),
        ),
        ("blob".to_string(), Value::Bytes(vec![1, 2, 3])),
        (
            "n".to_string(),
            Value::Array(vec![Value::Int(1), Value::from("x")]),
        ),
    ]));
    let wire = encode_value(&value).to_string();
    assert!(
        wire.contains(r#""$date":"2026-10-26T10:00:00.000Z""#),
        "{wire}"
    );
    assert!(wire.contains(r#""$rid":"user:a""#), "{wire}");
    let back = decode_value(&serde_json::from_str(&wire).unwrap());
    assert_eq!(
        back.get("at"),
        Some(&Value::from("2026-10-26T10:00:00.000Z"))
    );
    assert!(matches!(back.get("who"), Some(Value::String(s)) if s == "user:a"));
    assert_eq!(back.get("blob"), Some(&Value::Bytes(vec![1, 2, 3])));
    assert_eq!(
        back.get("n"),
        Some(&Value::Array(vec![Value::Int(1), Value::from("x")]))
    );
}

#[test]
fn reads_what_a_browser_peer_encodes() {
    // `JSON.stringify(encodeValue(...))` from packages/core.
    let back = decode_value(
        &serde_json::from_str(
            r#"{"at":{"$date":"2026-10-26T10:00:00.000Z"},"who":{"$rid":"user:a"},"blob":{"$bytes":"AQID"}}"#,
        )
        .unwrap(),
    );
    assert_eq!(
        back,
        Value::Object(Object::from([
            ("at".to_string(), Value::from("2026-10-26T10:00:00.000Z")),
            ("who".to_string(), Value::from("user:a")),
            ("blob".to_string(), Value::Bytes(vec![1, 2, 3])),
        ]))
    );
}

#[test]
fn checks_patches_against_the_columns() {
    let c = cols("call");
    let check = |j: Json| check_patch(&c, &row(j), false);
    assert_eq!(check(json!({"view_pos": "3"})), None);
    assert_eq!(check(json!({"view_pos": null})), None);
    assert_eq!(check(json!({"count": 2})), None);
    assert_eq!(check(json!({"caller": "user:x"})), None);
    assert!(check(json!({"nope": 1})).unwrap().contains("unknown field"));
    assert!(check(json!({"id": "call:2"})).unwrap().contains("id"));
    assert!(check(json!({"count": "x"})).unwrap().contains("type"));
    assert!(check(json!({"caller": null})).unwrap().contains("type"));
    assert!(check(json!({})).unwrap().contains("empty"));
    assert_eq!(check_patch(&c, &Row::new(), true), None);
}

#[test]
fn fits_array_int_and_option_types() {
    let t = |kind: &str, v: Json| fits_column(&col(kind), &Value::from(v));
    assert!(t("array<string>", json!(["a"])));
    assert!(!t("array<string>", json!([1])));
    assert!(t("array<int, 3>", json!([1, 2])));
    assert!(t("int", json!(2.0)));
    assert!(!t("int", json!(2.5)));
    assert!(t("array<option<string>>", json!([null])));
    assert!(!t("bool", json!("true")));
}

#[test]
fn splits_ephemeral_fields_off_a_write() {
    let c = cols("call");
    let (persistent, eph) = split_patch(&c, &row(json!({"view_id": "a", "view_pos": "1"})));
    assert_eq!(persistent, row(json!({"view_id": "a"})));
    assert_eq!(eph, Some(row(json!({"view_pos": "1"}))));
    let (_, none) = split_patch(&c, &row(json!({"view_id": "a"})));
    assert_eq!(none, None);
}

#[test]
fn normalizes_peers_and_skips_self() {
    let peers = vec![
        Value::RecordId(RecordId::new("user", "a")),
        Value::from("user:b"),
        Value::Null,
        Value::from("not-an-id"),
    ];
    assert_eq!(
        peer_users(&peers, Some("user:a")),
        BTreeSet::from(["user:b".to_string()])
    );
}

#[test]
fn parses_the_message_a_browser_peer_sends() {
    let m = parse_message(
        &serde_json::from_str(
            r#"{"t":"op","table":"call","id":"call:1","patch":{"view_pos":"1"},"hlc":[1730000000000,2,"ab"]}"#,
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(m.kind, ChannelOpKind::Update);
    assert_eq!(m.hlc, Hlc::new(1_730_000_000_000, 2, "ab"));
    assert_eq!(parse_message(&json!({"t": "op", "kind": "nuke"})), None);
    assert_eq!(
        parse_message(
            &json!({"t": "op", "table": "x", "id": "x:1", "patch": [], "hlc": [1, 0, "a"]})
        ),
        None
    );
    let update = ChannelMessage {
        kind: ChannelOpKind::Update,
        table: "call".into(),
        id: "call:1".into(),
        patch: Default::default(),
        hlc: Hlc::new(1, 0, "p"),
    };
    // An update leaves `kind` out, as the browser does.
    assert!(update.to_json().get("kind").is_none());
}

// ---- channel filters ---------------------------------------------------------------------

fn eq(field: &str, value: impl Into<Value>) -> ChannelCondition {
    ChannelCondition::new(field, "=", value)
}

#[test]
fn matches_equality_across_record_id_spellings_dates_and_nested_paths() {
    let user_a = Value::RecordId(RecordId::new("user", "a"));
    assert!(matches_where(
        &[eq("caller", user_a.clone())],
        &row(json!({"caller": "user:a"}))
    ));
    let mut held = Row::new();
    held.insert("caller".into(), Value::RecordId(RecordId::new("user", "b")));
    assert!(!matches_where(&[eq("caller", user_a)], &held));
    let jan = Value::Datetime(chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap());
    // Rows hold dates as ISO strings.
    assert!(matches_where(
        &[eq("at", jan)],
        &row(json!({"at": "2026-01-01T00:00:00.000Z"}))
    ));
    assert!(matches_where(
        &[eq("meta.kind", "x")],
        &row(json!({"meta": {"kind": "x"}}))
    ));
    assert!(!matches_where(
        &[eq("meta.kind", "x")],
        &row(json!({"meta": null}))
    ));
    assert!(matches_where(
        &[eq("tags", Value::Array(vec![Value::from("a")]))],
        &row(json!({"tags": ["a"]}))
    ));
    assert!(matches_where(&[eq("missing", Value::Null)], &Row::new()));
    assert!(matches_where(&[], &Row::new()));
}

#[test]
fn supports_comparisons_membership_and_swapped_operands() {
    let r = row(json!({"n": 5, "s": "b", "tags": ["x", "y"], "who": "user:a"}));
    fn c(field: &str, op: &str, value: impl Into<Value>) -> ChannelCondition {
        ChannelCondition::new(field, op, value)
    }
    let list = |items: &[&str]| Value::Array(items.iter().map(|s| Value::from(*s)).collect());
    assert!(matches_where(&[c("n", ">", 4), c("n", "<=", 5)], &r));
    assert!(!matches_where(&[c("n", "<", 5)], &r));
    assert!(!matches_where(&[c("n", ">=", "a")], &r));
    assert!(matches_where(&[c("n", "!=", 4)], &r));
    assert!(matches_where(&[c("s", "IN", list(&["a", "b"]))], &r));
    assert!(!matches_where(&[c("s", "NOT IN", list(&["a", "b"]))], &r));
    assert!(matches_where(&[c("tags", "CONTAINS", "y")], &r));
    assert!(!matches_where(&[c("tags", "CONTAINSNOT", "y")], &r));
    assert!(matches_where(
        &[c(
            "who",
            "∈",
            Value::Array(vec![Value::RecordId(RecordId::new("user", "a"))])
        )],
        &r
    ));
    assert!(!matches_where(
        &[ChannelCondition::swapped("s", "∈", list(&["b"]))],
        &row(json!({"s": ["b"]}))
    ));
}

#[test]
fn names_the_parts_of_a_query_a_channel_ignores() {
    assert!(unused_args(&q("msg")).is_empty());
    assert_eq!(
        unused_args(
            &q("msg")
                .order_by("text", "ASC")
                .limit(5)
                .offset(2)
                .select(["text"])
        ),
        vec!["orderBy", "limit", "offset", "select"]
    );
    assert_eq!(unused_args(&q("msg").one()), vec![".one()"]);
}

#[test]
fn warns_once_per_query_shape_about_ignored_parts_never_when_told() {
    let logs: Rc<RefCell<Vec<String>>> = Rc::default();
    let l = logs.clone();
    let m = ChannelManager::new(
        schema(),
        ManagerOptions {
            logger: Some(Rc::new(move |level, message| {
                if level == LogLevel::Warn {
                    l.borrow_mut().push(message.to_string());
                }
            })),
            ..Default::default()
        },
    );
    let plan = || q("msg").limit(50).order_by("text", "ASC");
    m.scope(&plan()).unwrap();
    m.scope(&plan()).unwrap();
    assert_eq!(logs.borrow().len(), 1);
    let re = regex::Regex::new(r"orderBy, limit.*\.ignore_unused_args\(\)").unwrap();
    assert!(re.is_match(&logs.borrow()[0]), "{}", logs.borrow()[0]);
    m.scope(&q("msg").one()).unwrap();
    assert_eq!(logs.borrow().len(), 2);
    m.scope(&q("msg").offset(3).ignore_unused_args()).unwrap();
    m.scope(&q("msg")).unwrap();
    assert_eq!(logs.borrow().len(), 2);
}

#[test]
fn compiles_a_query_into_a_scope_refusing_what_the_row_alone_cannot_decide() {
    let s = schema();
    let scope = compile_scope(&s, &convo("c1")).unwrap();
    assert_eq!(scope.table, "msg");
    assert_eq!(scope.where_.len(), 1);
    assert_eq!(scope.where_[0].op, "=");
    assert_eq!(scope.where_[0].value, Value::from("c1"));
    assert!(compile_scope(&s, &q("call")).unwrap().where_.is_empty());
    let ne = compile_scope(&s, &q("msg").where_op("text", QueryOp::new("!=", ""))).unwrap();
    assert_eq!(ne.where_[0].op, "!=");
    let err = |query: QueryBuilder| compile_scope(&s, &query).unwrap_err().to_string();
    assert!(err(q("note")).contains("@channel"));
    assert!(err(q("msg").where_op("text", QueryOp::new("@@", "x"))).contains("operator"));
    assert!(err(q("msg").where_op("convo", QueryOp::new("=", "$auth.id"))).contains("parameter"));
    assert!(err(q("call").where_eq("caller.name", "x")).contains("record link"));
    assert!(err(q("msg").where_or([("convo", WhereValue::Eq("a".into()))])).contains("OR group"));
    assert!(err(q("call").where_eq("view_pos", "1")).contains("@ephemeral"));
}

#[test]
fn the_query_builder_refuses_an_ephemeral_field_in_where_and_order_by() {
    assert!(q("call").where_eq("view_pos", "1").build().is_err());
    assert!(q("call").order_by("view_pos", "ASC").build().is_err());
}

// ---- manager: opening --------------------------------------------------------------------

#[tokio::test]
async fn connects_the_two_parties_and_reports_it() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let states: Rc<RefCell<Vec<ChannelStatus>>> = Rc::default();
        let s = states.clone();
        let _sub = a.ch.subscribe(move |st| s.borrow_mut().push(st.status));
        assert_eq!(a.ch.state().status, ChannelStatus::Connecting);
        let b = Device::new("user:b", &net);
        assert_eq!(
            a.ch.state(),
            ChannelState {
                status: ChannelStatus::Connected,
                peers: vec!["user:b".into()],
            }
        );
        assert_eq!(b.ch.state().peers, vec!["user:a".to_string()]);
        assert_eq!(
            *states.borrow(),
            vec![ChannelStatus::Connecting, ChannelStatus::Connected]
        );
        assert_eq!(a.ch.tables(), vec!["call".to_string(), "msg".to_string()]);
    })
    .await;
}

#[tokio::test]
async fn needs_at_least_one_query() {
    run_local(async {
        let m = ChannelManager::new(schema(), ManagerOptions::default());
        assert!(matches!(
            m.open(vec![], vec![]),
            Err(Error::InvalidArgument(_))
        ));
        let ch = m
            .open(vec![m.scope(&all_calls()).unwrap()], vec![])
            .unwrap();
        assert!(matches!(
            ch.set_scopes(vec![]),
            Err(Error::InvalidArgument(_))
        ));
        ch.set_scopes(vec![m.scope(&convo("c2")).unwrap()]).unwrap();
        assert_eq!(ch.tables(), vec!["msg".to_string()]);
    })
    .await;
}

#[tokio::test]
async fn is_idle_with_nobody_but_yourself_and_skips_you_once_you_sign_in() {
    run_local(async {
        let net = MemoryNet::new();
        let m = ChannelManager::new(
            schema(),
            ManagerOptions {
                transport: Some(net.factory()),
                ..Default::default()
            },
        );
        let ch = m
            .open(
                vec![m.scope(&all_calls()).unwrap()],
                vec!["user:a".into(), "user:b".into()],
            )
            .unwrap();
        assert_eq!(ch.state().status, ChannelStatus::Connecting);
        m.set_user(Some("user:a"));
        assert_eq!(net.node_count(), 1);
        ch.set_peers(vec![Value::RecordId(RecordId::new("user", "a"))]);
        assert_eq!(ch.state().status, ChannelStatus::Idle);
    })
    .await;
}

struct Unsupported;

impl ChannelTransport for Unsupported {
    fn supported(&self) -> bool {
        false
    }
    fn want(&self, _users: &BTreeSet<String>) {}
    fn send(&self, _to: &PeerRef, _message: &ChannelMessage) {}
    fn close(&self) {}
}

#[tokio::test]
async fn is_unsupported_without_a_peer_connection_factory() {
    run_local(async {
        let m = ChannelManager::new(
            schema(),
            ManagerOptions {
                transport: Some(Rc::new(|_| {
                    Rc::new(Unsupported) as Rc<dyn ChannelTransport>
                })),
                ..Default::default()
            },
        );
        m.set_user(Some("user:a"));
        let ch = m
            .open(vec![m.scope(&all_calls()).unwrap()], vec!["user:b".into()])
            .unwrap();
        assert_eq!(ch.state().status, ChannelStatus::Unsupported);
    })
    .await;
}

#[tokio::test]
async fn set_peers_and_close_change_who_it_wants() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::new("user:b", &net);
        a.ch.set_peers(vec!["user:c".into()]);
        assert_eq!(b.ch.state().status, ChannelStatus::Connecting);
        a.ch.set_peers(vec!["user:b".into()]);
        assert_eq!(b.ch.state().status, ChannelStatus::Connected);
        a.ch.close();
        a.ch.close();
        a.ch.set_peers(vec!["user:b".into()]);
        a.ch.set_scopes(vec![]).unwrap();
        assert_eq!(b.ch.state().status, ChannelStatus::Connecting);
        assert!(a.ch.is_closed());
        assert!(a
            .m
            .assert_carries(&a.ch, "call")
            .unwrap_err()
            .to_string()
            .contains("closed"));
    })
    .await;
}

#[tokio::test]
async fn a_sign_out_drops_the_transport_and_the_peer_values() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::new("user:b", &net);
        b.feed(vec![call_row(json!({}))]);
        a.m.local(
            "call",
            "call:1",
            &empty(),
            Some(&one("view_pos", "4")),
            None,
        );
        flush().await;
        assert_eq!(b.last_field("view_pos"), Value::from("4"));
        b.m.set_user(None);
        assert!(is_null(&b.last_field("view_pos")));
        assert_eq!(a.ch.state().status, ChannelStatus::Connecting);
    })
    .await;
}

// ---- manager: updates --------------------------------------------------------------------

#[tokio::test]
async fn sends_ephemeral_values_to_the_peer_and_shows_them_locally_never_as_a_write() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::new("user:b", &net);
        a.feed(vec![call_row(json!({}))]);
        b.feed(vec![call_row(json!({}))]);
        let (persistent, ephemeral) = a.m.split("call", &row(json!({"view_pos": "12"})));
        assert!(persistent.is_empty());
        a.m.local("call", "call:1", &persistent, ephemeral.as_ref(), None);
        assert_eq!(a.last_field("view_pos"), Value::from("12"));
        flush().await;
        assert_eq!(b.last_field("view_pos"), Value::from("12"));
    })
    .await;
}

#[tokio::test]
async fn shows_a_persistent_peer_value_until_the_synced_row_catches_up() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::new("user:b", &net);
        b.feed(vec![call_row(json!({}))]);
        a.m.local("call", "call:1", &one("view_id", "game:x"), None, None);
        flush().await;
        assert_eq!(b.last_field("view_id"), Value::from("game:x"));
        b.feed(vec![call_row(json!({"view_id": "game:x"}))]);
        b.feed(vec![call_row(json!({"view_id": "game:y"}))]);
        assert_eq!(b.last_field("view_id"), Value::from("game:y"));
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn drops_an_unconfirmed_persistent_peer_value_after_the_timeout() {
    run_local(async {
        let now = Rc::new(Cell::new(1000i64));
        let clock = |c: &Rc<Cell<i64>>| {
            let c = c.clone();
            Some(Rc::new(move || c.get()) as Rc<dyn Fn() -> i64>)
        };
        let net = MemoryNet::new();
        let a = Device::with(
            "user:a",
            &net,
            DeviceOptions {
                now: clock(&now),
                ..Default::default()
            },
        );
        let b = Device::with(
            "user:b",
            &net,
            DeviceOptions {
                now: clock(&now),
                config: Some(ChannelsConfig {
                    confirm_timeout_ms: 5000,
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        b.feed(vec![call_row(json!({}))]);
        a.m.local("call", "call:1", &one("view_id", "forged"), None, None);
        flush().await;
        assert_eq!(b.last_field("view_id"), Value::from("forged"));
        now.set(now.get() + 6000);
        tokio::time::advance(Duration::from_secs(1)).await;
        flush().await;
        assert_eq!(b.last_field("view_id"), Value::from(""));
        b.m.close();
        a.m.close();
    })
    .await;
}

#[tokio::test]
async fn ignores_an_unlisted_user_a_non_channel_table_a_row_outside_the_filter_and_a_row_it_does_not_hold(
) {
    run_local(async {
        let net = MemoryNet::new();
        let b = Device::new("user:b", &net);
        b.feed(vec![
            call_row(json!({})),
            msg(json!({"id": "msg:2", "convo": "c2"})),
        ]);
        let from = peer("user:a");
        b.m.receive(
            &peer("user:c"),
            &op(json!({"table": "call", "id": "call:1", "patch": {"view_pos": "1"}})),
        );
        b.m.receive(
            &from,
            &op(json!({"table": "note", "id": "note:1", "patch": {"title": "x"}})),
        );
        b.m.receive(
            &from,
            &op(json!({"table": "msg", "id": "msg:2", "patch": {"text": "x"}})),
        );
        b.m.receive(
            &from,
            &op(json!({"table": "msg", "id": "msg:9", "patch": {"text": "x"}})),
        );
        b.m.receive(
            &from,
            &op(json!({"table": "call", "id": "note:1", "patch": {"view_pos": "1"}})),
        );
        b.m.receive(
            &from,
            &op(json!({"table": "call", "id": "call:1", "patch": {"view_pos": 1}})),
        );
        flush().await;
        assert_eq!(b.seen_count(), 1);
    })
    .await;
}

#[tokio::test]
async fn takes_a_row_moving_out_of_the_filter_it_matched_before_the_patch() {
    run_local(async {
        let net = MemoryNet::new();
        let b = Device::new("user:b", &net);
        b.fr.put("msg:1", msg(json!({})));
        b.feed(vec![msg(json!({}))]);
        b.m.receive(
            &peer("user:a"),
            &op(json!({"table": "msg", "id": "msg:1", "patch": {"convo": "c2"}})),
        );
        flush().await;
        assert_eq!(b.last_field("convo"), Value::from("c2"));
    })
    .await;
}

#[tokio::test]
async fn applies_the_app_accept_rule() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::with(
            "user:b",
            &net,
            DeviceOptions {
                config: Some(ChannelsConfig::default().accept("call", |op| {
                    op.row.as_ref().and_then(|r| r.get("callee"))
                        == Some(&Value::from(op.from.clone()))
                })),
                ..Default::default()
            },
        );
        b.feed(vec![call_row(json!({}))]);
        a.m.local(
            "call",
            "call:1",
            &empty(),
            Some(&one("view_pos", "5")),
            None,
        );
        flush().await;
        assert!(is_null(&b.last_field("view_pos")));
    })
    .await;
}

#[tokio::test]
async fn the_accept_rule_sees_the_sender_and_the_row() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        // Only the callee may move the view: user:a is the caller.
        let b = Device::with(
            "user:b",
            &net,
            DeviceOptions {
                config: Some(ChannelsConfig::default().accept("call", |op| {
                    op.row.as_ref().and_then(|r| r.get("callee"))
                        == Some(&Value::from(op.from.clone()))
                })),
                ..Default::default()
            },
        );
        b.feed(vec![call_row(json!({}))]);
        a.m.local(
            "call",
            "call:1",
            &empty(),
            Some(&one("view_pos", "5")),
            None,
        );
        flush().await;
        assert!(is_null(&b.last_field("view_pos")));
        b.feed(vec![call_row(json!({"callee": "user:a"}))]);
        a.m.local(
            "call",
            "call:1",
            &empty(),
            Some(&one("view_pos", "6")),
            None,
        );
        flush().await;
        assert_eq!(b.last_field("view_pos"), Value::from("6"));
    })
    .await;
}

#[tokio::test]
async fn a_panicking_accept_rule_refuses() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::with(
            "user:b",
            &net,
            DeviceOptions {
                config: Some(ChannelsConfig::default().accept("call", |_| panic!("x"))),
                ..Default::default()
            },
        );
        b.feed(vec![call_row(json!({}))]);
        a.m.local(
            "call",
            "call:1",
            &empty(),
            Some(&one("view_pos", "5")),
            None,
        );
        flush().await;
        assert!(is_null(&b.last_field("view_pos")));
    })
    .await;
}

#[tokio::test]
async fn keeps_the_newest_write_when_two_arrive_out_of_order() {
    run_local(async {
        let net = MemoryNet::new();
        let b = Device::new("user:b", &net);
        b.feed(vec![call_row(json!({}))]);
        let from = peer("user:a");
        b.m.receive(
            &from,
            &json!({"t": "op", "table": "call", "id": "call:1", "patch": {"view_pos": "new"}, "hlc": [2000, 0, "x"]}),
        );
        flush().await;
        b.m.receive(
            &from,
            &json!({"t": "op", "table": "call", "id": "call:1", "patch": {"view_pos": "old"}, "hlc": [1000, 0, "x"]}),
        );
        flush().await;
        assert_eq!(b.last_field("view_pos"), Value::from("new"));
    })
    .await;
}

#[tokio::test]
async fn sends_its_ephemeral_state_to_a_peer_that_connects_later_and_forgets_a_peer_that_leaves() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        a.fr.put("call:1", call_row(json!({})));
        a.m.local(
            "call",
            "call:1",
            &empty(),
            Some(&one("view_pos", "3")),
            None,
        );
        a.m.local("msg", "msg:7", &empty(), Some(&one("mood", "x")), None);
        let b = Device::new("user:b", &net);
        b.feed(vec![call_row(json!({}))]);
        flush().await;
        assert_eq!(b.last_field("view_pos"), Value::from("3"));
        a.m.close();
        assert!(is_null(&b.last_field("view_pos")));
    })
    .await;
}

#[tokio::test]
async fn passes_writes_on_non_channel_tables_through_untouched() {
    run_local(async {
        let m = ChannelManager::new(schema(), ManagerOptions::default());
        let (persistent, eph) = m.split("note", &row(json!({"title": "x"})));
        assert_eq!(persistent, row(json!({"title": "x"})));
        assert_eq!(eph, None);
        m.local("note", "note:1", &row(json!({"title": "x"})), None, None);
        m.local_create("note", "note:1", &Row::new(), None);
        assert!(m.overlay().is_empty());
    })
    .await;
}

#[tokio::test]
async fn an_own_persistent_write_supersedes_a_peer_value_for_the_same_field() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::new("user:b", &net);
        b.feed(vec![call_row(json!({}))]);
        a.m.local("call", "call:1", &one("view_id", "game:a"), None, None);
        flush().await;
        assert_eq!(b.last_field("view_id"), Value::from("game:a"));
        b.m.local("call", "call:1", &one("view_id", "game:b"), None, None);
        b.feed(vec![call_row(json!({"view_id": "game:b"}))]);
        assert_eq!(b.last_field("view_id"), Value::from("game:b"));
    })
    .await;
}

#[tokio::test]
async fn a_record_link_goes_out_as_a_record_id_and_lands_as_a_string() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::new("user:b", &net);
        b.feed(vec![call_row(json!({}))]);
        a.m.local("call", "call:1", &one("caller", "user:z"), None, None);
        flush().await;
        let wire = net.wire();
        assert_eq!(wire.len(), 1);
        // A browser peer reads a RecordId, not a string.
        assert!(
            wire[0].contains(r#""caller":{"$rid":"user:z"}"#),
            "{}",
            wire[0]
        );
        // Rows hold record links as strings.
        assert_eq!(b.last_field("caller"), Value::String("user:z".into()));
    })
    .await;
}

// ---- manager: routing by filter ----------------------------------------------------------

struct ThreeWay {
    a: Device,
    b: Device,
    c: Device,
    to_c: ChannelHandle,
    _net: MemoryNet,
}

fn three_way() -> ThreeWay {
    let net = MemoryNet::new();
    let a = Device::with(
        "user:a",
        &net,
        DeviceOptions {
            scopes: Some(vec![convo("c1")]),
            peers: Some(vec!["user:b".into()]),
            ..Default::default()
        },
    );
    let to_c =
        a.m.open(
            vec![a.m.scope(&convo("c2")).unwrap()],
            vec!["user:c".into()],
        )
        .unwrap();
    let side = |user: &str, c: &str| {
        Device::with(
            user,
            &net,
            DeviceOptions {
                scopes: Some(vec![convo(c)]),
                peers: Some(vec!["user:a".into()]),
                ..Default::default()
            },
        )
    };
    let b = side("user:b", "c1");
    let c = side("user:c", "c2");
    ThreeWay {
        a,
        b,
        c,
        to_c,
        _net: net,
    }
}

#[tokio::test]
async fn sends_a_message_only_to_the_participants_of_its_conversations_channel() {
    run_local(async {
        let t = three_way();
        t.a.m.local_create(
            "msg",
            "msg:1",
            &row(json!({"convo": "c1", "text": "hi", "_00_rv": 1, "junk": 2})),
            None,
        );
        flush().await;
        assert_eq!(t.b.fr.calls(), vec!["create msg:1 5000".to_string()]);
        assert_eq!(
            t.b.fr.get("msg:1"),
            Some(row(json!({"id": "msg:1", "convo": "c1", "text": "hi"})))
        );
        assert!(t.c.fr.calls().is_empty());
    })
    .await;
}

#[tokio::test]
async fn routes_updates_and_deletes_by_the_stored_row_a_row_changing_conversation_reaches_both() {
    run_local(async {
        let t = three_way();
        t.a.fr.put("msg:1", msg(json!({})));
        for d in [&t.b, &t.c] {
            d.fr.put("msg:1", msg(json!({})));
            d.feed(vec![msg(json!({}))]);
        }
        t.a.m
            .local("msg", "msg:1", &row(json!({"text": "edited"})), None, None);
        flush().await;
        assert_eq!(t.b.last_field("text"), Value::from("edited"));
        assert_eq!(t.c.last_field("text"), Value::from("hi"));
        t.a.m.local_delete("msg", "msg:1", None).await;
        flush().await;
        assert_eq!(t.b.fr.calls(), vec!["delete msg:1 5000".to_string()]);
        assert!(t.c.fr.calls().is_empty());

        let moved = three_way();
        moved.a.fr.put("msg:1", msg(json!({})));
        for d in [&moved.b, &moved.c] {
            d.fr.put("msg:1", msg(json!({})));
            d.feed(vec![msg(json!({}))]);
        }
        moved
            .a
            .m
            .local("msg", "msg:1", &row(json!({"convo": "c2"})), None, None);
        flush().await;
        assert_eq!(moved.b.last_field("convo"), Value::from("c2"));
        assert_eq!(moved.c.last_field("convo"), Value::from("c2"));
    })
    .await;
}

#[tokio::test]
async fn a_scoped_write_goes_to_that_channel_only_and_only_for_rows_its_filter_matches() {
    run_local(async {
        let t = three_way();
        t.a.m.local_create(
            "msg",
            "msg:1",
            &row(json!({"convo": "c1", "text": "hi"})),
            Some(&t.to_c),
        );
        t.a.m.local_create(
            "msg",
            "msg:2",
            &row(json!({"convo": "c2", "text": "hi"})),
            Some(&t.to_c),
        );
        flush().await;
        assert!(t.b.fr.calls().is_empty());
        assert_eq!(t.c.fr.calls(), vec!["create msg:2 5000".to_string()]);
        let err = t.a.m.assert_carries(&t.to_c, "call").unwrap_err();
        assert!(
            matches!(&err, Error::InvalidArgument(m) if m.contains("no query on 'call'")),
            "{err}"
        );
        t.a.m.assert_carries(&t.to_c, "msg").unwrap();
    })
    .await;
}

#[tokio::test]
async fn an_update_of_a_row_it_does_not_hold_is_matched_on_the_patch_alone() {
    run_local(async {
        let t = three_way();
        t.b.fr.put("msg:5", msg(json!({"id": "msg:5"})));
        t.b.feed(vec![msg(json!({"id": "msg:5"}))]);
        t.a.m.local(
            "msg",
            "msg:5",
            &row(json!({"convo": "c1", "text": "x"})),
            None,
            None,
        );
        flush().await;
        assert_eq!(t.b.last_field("text"), Value::from("x"));
        t.a.m.local_delete("msg", "msg:404", None).await;
    })
    .await;
}

// ---- manager: creates and deletes -----------------------------------------------------------

#[tokio::test]
async fn carries_ephemeral_fields_of_a_create_to_the_peer_overlay_never_into_its_store() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::new("user:b", &net);
        a.feed(vec![msg(json!({}))]);
        a.m.local_create(
            "msg",
            "msg:1",
            &row(json!({"convo": "c1", "text": "hi", "mood": "happy"})),
            None,
        );
        assert_eq!(a.last_field("mood"), Value::from("happy"));
        flush().await;
        assert_eq!(
            b.fr.get("msg:1"),
            Some(row(json!({"id": "msg:1", "convo": "c1", "text": "hi"})))
        );
        b.feed(vec![msg(json!({}))]);
        assert_eq!(b.last_field("mood"), Value::from("happy"));
    })
    .await;
}

#[tokio::test]
async fn refuses_a_create_from_an_unlisted_user_outside_the_filter_against_the_accept_rule_of_the_wrong_shape_or_kind(
) {
    run_local(async {
        let net = MemoryNet::new();
        let b = Device::with(
            "user:b",
            &net,
            DeviceOptions {
                config: Some(ChannelsConfig::default().accept("msg", |op| {
                    op.kind != ChannelOpKind::Create || op.patch.get("text") != Some(&Value::from("spam"))
                })),
                ..Default::default()
            },
        );
        let send = |user: &str, m: Json| b.m.receive(&peer(user), &op(m));
        send(
            "user:c",
            json!({"kind": "create", "table": "msg", "id": "msg:1", "patch": {"convo": "c1", "text": "hi"}}),
        );
        send(
            "user:a",
            json!({"kind": "create", "table": "msg", "id": "msg:1", "patch": {"convo": "c9", "text": "hi"}}),
        );
        send(
            "user:a",
            json!({"kind": "create", "table": "msg", "id": "msg:1", "patch": {"convo": "c1", "text": "spam"}}),
        );
        send("user:a", json!({"kind": "create", "table": "msg", "id": "msg:1", "patch": {"convo": 1}}));
        send("user:a", json!({"kind": "nuke", "table": "msg", "id": "msg:1", "patch": {}}));
        flush().await;
        assert!(b.fr.calls().is_empty());

        let no_rows = ChannelManager::new(schema(), ManagerOptions::default());
        no_rows.set_user(Some("user:b"));
        let _ch = no_rows
            .open(vec![no_rows.scope(&convo("c1")).unwrap()], vec!["user:a".into()])
            .unwrap();
        no_rows.receive(
            &peer("user:a"),
            &op(json!({"kind": "create", "table": "msg", "id": "msg:1", "patch": {"convo": "c1"}})),
        );
        no_rows.receive(
            &peer("user:a"),
            &op(json!({"kind": "delete", "table": "msg", "id": "msg:1", "patch": {}})),
        );
        flush().await;
    })
    .await;
}

#[tokio::test]
async fn sends_an_own_delete_the_peer_hides_it() {
    run_local(async {
        let net = MemoryNet::new();
        let a = Device::new("user:a", &net);
        let b = Device::new("user:b", &net);
        a.fr.put("msg:1", msg(json!({})));
        b.fr.put("msg:1", msg(json!({})));
        a.m.local_delete("msg", "msg:1", None).await;
        flush().await;
        assert_eq!(b.fr.calls(), vec!["delete msg:1 5000".to_string()]);
    })
    .await;
}

#[tokio::test]
async fn ignores_a_delete_from_an_unlisted_user_outside_the_filter_of_a_row_it_does_not_hold_or_against_the_accept_rule(
) {
    run_local(async {
        let net = MemoryNet::new();
        let b = Device::with(
            "user:b",
            &net,
            DeviceOptions {
                config: Some(ChannelsConfig::default().accept("call", |_| false)),
                ..Default::default()
            },
        );
        b.fr.put("msg:2", msg(json!({"id": "msg:2", "convo": "c2"})));
        b.fr.put("call:1", call_row(json!({})));
        let del = |user: &str, table: &str, id: &str| {
            b.m.receive(
                &peer(user),
                &op(json!({"kind": "delete", "table": table, "id": id, "patch": {}})),
            )
        };
        del("user:c", "msg", "msg:1");
        del("user:a", "msg", "msg:2");
        del("user:a", "msg", "msg:404");
        del("user:a", "call", "call:1");
        flush().await;
        assert!(b.fr.calls().is_empty());
    })
    .await;
}

#[tokio::test]
async fn a_write_with_no_transport_up_never_reads_the_row() {
    run_local(async {
        let fr = FakeRows::default();
        let m = ChannelManager::new(
            schema(),
            ManagerOptions {
                rows: Some(Rc::new(fr.clone())),
                ..Default::default()
            },
        );
        m.local("msg", "msg:1", &row(json!({"text": "x"})), None, None);
        m.local_delete("msg", "msg:1", None).await;
        flush().await;
        assert_eq!(fr.0.reads.get(), 0);
    })
    .await;
}

#[test]
fn hlc_orders_by_ms_then_counter_then_peer() {
    assert!(Hlc::new(1, 5, "z") < Hlc::new(2, 0, "a"));
    assert!(Hlc::new(1, 0, "z") < Hlc::new(1, 1, "a"));
    assert!(Hlc::new(1, 1, "a") < Hlc::new(1, 1, "b"));
}
