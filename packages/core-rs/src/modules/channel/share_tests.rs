//! PORT: `packages/core/src/modules/channel/share.test.ts`: what a query can
//! show (interest), and the manager sharing with the user's own devices. The
//! last block there, two browsers with two tabs each, is the browser's hub;
//! this client's side of it is in `room/tests.rs`.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use chrono::TimeZone;
use serde_json::{json, Value as Json};

use super::tests::{flush, FakeRows, MemoryNet, Sent};
use super::*;
use crate::kernel::events::LogLevel;
use crate::modules::query_builder::QueryOp;
use crate::testing::{row, run_local};
use crate::types::{Cardinality, SchemaRelationship};
use crate::value::RecordId;

fn col(kind: &str) -> ColumnSchema {
    ColumnSchema::of_type(kind)
}

fn optional(kind: &str) -> ColumnSchema {
    ColumnSchema {
        optional: true,
        ..col(kind)
    }
}

fn record(optional: bool) -> ColumnSchema {
    ColumnSchema {
        record_id: true,
        optional,
        ..col("record")
    }
}

fn relation(from: &str, field: &str, to: &str, cardinality: Cardinality) -> SchemaRelationship {
    SchemaRelationship {
        from: from.into(),
        field: field.into(),
        to: to.into(),
        cardinality,
    }
}

fn schema() -> Arc<Schema> {
    let mut s = Schema::default()
        .with_table(
            "note",
            [
                ("folder", col("string")),
                ("title", col("string")),
                ("draft", optional("bool")),
                ("author", record(true)),
                (
                    "at",
                    ColumnSchema {
                        date_time: true,
                        ..optional("datetime")
                    },
                ),
            ],
        )
        .with_table("tag", [("note", record(false)), ("label", col("string"))])
        .with_table("secret", [("text", col("string"))])
        .with_table("call", [("view_id", optional("string"))]);
    s.tables.get_mut("call").unwrap().channel = true;
    s.relationships = vec![
        relation("note", "tags", "tag", Cardinality::Many),
        relation("tag", "x", "secret", Cardinality::One),
    ];
    Arc::new(s)
}

fn q(table: &str) -> QueryBuilder {
    QueryBuilder::new(table).with_schema(schema())
}

fn in_folder(folder: &str) -> QueryBuilder {
    q("note").where_eq("folder", folder)
}

fn eq(field: &str, value: impl Into<Value>) -> WhereNode {
    WhereNode::Cond(ChannelCondition::new(field, "=", value))
}

fn claim(table: &str, where_: Option<Vec<WhereNode>>) -> QueryInterest {
    QueryInterest {
        table: table.into(),
        where_,
    }
}

fn tables<const N: usize>(entries: [(&str, TableInterest); N]) -> Interest {
    Interest::Tables(
        entries
            .into_iter()
            .map(|(table, claim)| (table.to_string(), claim))
            .collect(),
    )
}

fn filters(list: Vec<Vec<WhereNode>>) -> TableInterest {
    TableInterest::Filters(list)
}

// ---- interest ----------------------------------------------------------------------------------

#[test]
fn reads_what_a_query_can_show_from_its_builder() {
    let s = schema();
    assert_eq!(
        query_interest(&s, &in_folder("a")),
        vec![claim("note", Some(vec![eq("folder", "a")]))]
    );
    assert_eq!(
        query_interest(&s, &q("note")),
        vec![claim("note", Some(vec![]))]
    );
    // OR groups and swapped operands keep their shape.
    let either = q("note").where_or([
        ("folder", WhereValue::Eq("a".into())),
        (
            "folder",
            WhereValue::Op(QueryOp::swapped("∈", Value::Array(vec!["b".into()]))),
        ),
    ]);
    assert_eq!(
        query_interest(&s, &either),
        vec![claim(
            "note",
            Some(vec![WhereNode::Or(vec![
                ChannelCondition::new("folder", "=", "a"),
                ChannelCondition::swapped("folder", "∈", Value::Array(vec!["b".into()])),
            ])])
        )]
    );
    // What the row alone cannot decide is the whole table.
    for unsure in [
        q("note").where_op("title", QueryOp::new("@@", "x")),
        q("note").where_eq("author.name", "x"),
        q("note").where_op("folder", QueryOp::new("=", "$missing")),
        q("note").where_or([
            ("folder", WhereValue::Eq("a".into())),
            ("title", WhereValue::Op(QueryOp::new("@@", "x"))),
        ]),
    ] {
        assert_eq!(query_interest(&s, &unsure), vec![claim("note", None)]);
    }
    // Related tables: every row, however deep.
    let related = in_folder("a").related_with("tags", |tags| tags.related("x"));
    let shape: Vec<(String, bool)> = query_interest(&s, &related)
        .into_iter()
        .map(|c| (c.table, c.where_.is_none()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("note".to_string(), false),
            ("tag".to_string(), true),
            ("secret".to_string(), true),
        ]
    );
}

#[test]
fn types_a_filter_value_the_way_the_builder_binds_it() {
    let s = schema();
    // A bare key on `id` is a row of the query's table.
    assert_eq!(
        query_interest(&s, &q("note").where_eq("id", "1")),
        vec![claim("note", Some(vec![eq("id", "note:1")]))]
    );
    // A record link and a date go out typed, so a browser compares them as
    // what they are; a record id and its string spelling are equal here.
    let typed = query_interest(
        &s,
        &q("note")
            .where_eq("author", "user:a")
            .where_op("at", QueryOp::new(">", "2026-01-01T00:00:00Z")),
    );
    let wire = encode_interest(&merge_interest([Some(typed.as_slice())], |_| true));
    assert_eq!(
        wire["v"]["note"][0],
        json!([
            {"field": "author", "op": "=", "value": {"$rid": "user:a"}},
            {"field": "at", "op": ">", "value": {"$date": "2026-01-01T00:00:00.000Z"}},
        ])
    );
}

#[test]
fn merges_the_open_queries_into_one_claim_per_shared_table() {
    let shared = |t: &str| t == "note" || t == "tag";
    let folder = |f: &str| vec![claim("note", Some(vec![eq("folder", f)]))];
    let merge = |list: &[Option<Vec<QueryInterest>>]| {
        merge_interest(list.iter().map(|q| q.as_deref()), shared)
    };
    assert_eq!(merge(&[]), tables([]));
    assert_eq!(merge(&[Some(vec![claim("secret", None)])]), tables([]));
    let mut b_and_tags = folder("b");
    b_and_tags.push(claim("tag", None));
    assert_eq!(
        merge(&[Some(folder("a")), Some(b_and_tags)]),
        tables([
            (
                "note",
                filters(vec![vec![eq("folder", "a")], vec![eq("folder", "b")]])
            ),
            ("tag", TableInterest::All),
        ])
    );
    // The same filter twice is one, and the order of the queries does not
    // show.
    assert_eq!(
        merge(&[Some(folder("b")), Some(folder("a")), Some(folder("b"))]),
        tables([(
            "note",
            filters(vec![vec![eq("folder", "a")], vec![eq("folder", "b")]])
        )])
    );
    // An unfiltered query, or one nobody described, asks for everything.
    assert_eq!(
        merge(&[
            Some(folder("a")),
            Some(vec![claim("note", Some(vec![]))]),
            Some(folder("c")),
        ]),
        tables([("note", TableInterest::All)])
    );
    assert_eq!(merge(&[Some(folder("a")), None]), Interest::All);
    let many: Vec<Option<Vec<QueryInterest>>> =
        (0..30).map(|i| Some(folder(&format!("f{i}")))).collect();
    assert_eq!(merge(&many), tables([("note", TableInterest::All)]));
}

#[test]
fn survives_the_wire_record_ids_and_dates_included() {
    let at = chrono::Utc.timestamp_millis_opt(5).unwrap();
    let interest = tables([
        (
            "note",
            filters(vec![vec![
                eq("author", Value::RecordId(RecordId::new("user", "a"))),
                WhereNode::Or(vec![
                    ChannelCondition::new("folder", "=", "a"),
                    ChannelCondition::swapped("at", ">", Value::Datetime(at)),
                ]),
            ]]),
        ),
        ("tag", TableInterest::All),
    ]);
    // `JSON.stringify(encodeInterest(...))` of the same interest in
    // packages/core, key for key.
    let browser: Json = serde_json::from_str(
        r#"{"t":"interest","v":{"note":[[{"field":"author","op":"=","value":{"$rid":"user:a"}},{"or":[{"field":"folder","op":"=","value":"a"},{"field":"at","op":">","value":{"$date":"1970-01-01T00:00:00.005Z"},"swap":true}]}]],"tag":true}}"#,
    )
    .unwrap();
    assert_eq!(encode_interest(&interest), browser);
    let back = decode_interest(&browser).unwrap();
    assert_eq!(back, interest);
    let shows = |table: &str, rows: &[Json]| {
        let rows: Vec<Row> = rows.iter().cloned().map(row).collect();
        shows_row(&back, table, &rows)
    };
    assert!(shows("note", &[json!({"author": "user:a", "folder": "a"})]));
    assert!(!shows(
        "note",
        &[json!({"author": "user:b", "folder": "a"})]
    ));
    // The swapped date: `5 ms > at`, whatever precision the row spells it in.
    assert!(shows(
        "note",
        &[json!({"author": "user:a", "at": "1970-01-01T00:00:00Z"})]
    ));
    assert!(!shows(
        "note",
        &[json!({"author": "user:a", "at": "1970-01-01T00:00:01Z"})]
    ));
    assert!(shows("tag", &[json!({})]));
    assert!(!shows("secret", &[json!({})]));
    assert_eq!(
        decode_interest(&encode_interest(&Interest::All)),
        Some(Interest::All)
    );
    assert_eq!(
        encode_interest(&Interest::All),
        json!({"t": "interest", "all": true})
    );
    assert!(shows_row(&Interest::All, "anything", &[]));
    // Before or after a write: either is enough.
    assert!(shows(
        "note",
        &[
            json!({"author": "user:b"}),
            json!({"author": "user:a", "folder": "a"})
        ]
    ));
}

#[test]
fn reads_a_filter_it_cannot_evaluate_as_the_whole_table_and_refuses_what_is_not_an_interest() {
    for raw in [
        json!(null),
        json!("x"),
        json!({"t": "op"}),
        json!({"t": "interest"}),
        json!({"t": "interest", "v": []}),
        json!({"t": "interest", "v": null}),
    ] {
        assert_eq!(decode_interest(&raw), None, "{raw}");
    }
    let plain = json!({"field": "x", "op": "=", "value": 1});
    let odd = decode_interest(&json!({
        "t": "interest",
        "v": {
            "a": [[{"field": "x", "op": "constructor", "value": 1}]],
            "b": [[{"field": 1, "op": "=", "value": 1}]],
            "c": ["nope"],
            "d": [[{"or": []}]],
            "e": vec![json!([plain.clone()]); 40],
            "f": [[null]],
            "g": "yes",
            "h": [[plain]],
        },
    }));
    let all = |t: &'static str| (t, TableInterest::All);
    assert_eq!(
        odd,
        Some(tables([
            all("a"),
            all("b"),
            all("c"),
            all("d"),
            all("e"),
            all("f"),
            ("h", filters(vec![vec![eq("x", 1)]])),
        ]))
    );
}

// ---- the manager, sharing with the user's own devices ----------------------------------------

/// What a subscription saw.
struct View {
    seen: Rc<RefCell<Vec<Arc<Vec<Row>>>>>,
    tap: TapHandle,
}

impl View {
    fn field(&self, index: usize, field: &str) -> Value {
        let seen = self.seen.borrow();
        let rows = seen.last().expect("the view was fed");
        rows[index].get(field).cloned().unwrap_or(Value::Null)
    }

    fn column(&self, field: &str) -> Vec<Value> {
        let seen = self.seen.borrow();
        let rows = seen.last().expect("the view was fed");
        rows.iter()
            .map(|r| r.get(field).cloned().unwrap_or(Value::Null))
            .collect()
    }

    fn stop(&self) {
        self.tap.stop();
    }
}

/// One device: a manager signed in as a user, its local rows, and a way to
/// subscribe.
struct Device {
    m: ChannelManager,
    fr: FakeRows,
}

impl Device {
    fn new(user: &str, net: &MemoryNet, config: ChannelsConfig, held: Vec<Row>) -> Device {
        let fr = FakeRows::default();
        for r in held {
            let id = r["id"].as_str().unwrap().to_string();
            fr.put(&id, r);
        }
        let m = ChannelManager::new(
            schema(),
            ManagerOptions {
                config: Some(config),
                rows: Some(Rc::new(fr.clone())),
                transport: Some(net.factory()),
                ..Default::default()
            },
        );
        m.set_user(Some(user));
        Device { m, fr }
    }

    /// Subscribe like a mounted query: its rows are what the store holds of
    /// its table, or of every table for a subscription nobody described.
    fn show(&self, query: Option<&QueryBuilder>) -> View {
        let seen: Rc<RefCell<Vec<Arc<Vec<Row>>>>> = Rc::default();
        let s = seen.clone();
        let tap = self.m.tap_showing(
            move |rows| s.borrow_mut().push(rows.clone()),
            query.map(|q| self.m.interest_of(q)),
        );
        let prefix = query.map(|q| format!("{}:", q.table()));
        let rows: Vec<Row> = self
            .fr
            .all()
            .into_iter()
            .filter(|r| {
                let id = r["id"].as_str().unwrap_or_default();
                prefix.as_deref().is_none_or(|p| id.starts_with(p))
            })
            .collect();
        tap.feed(&Arc::new(rows));
        View { seen, tap }
    }

    fn update(&self, id: &str, patch: Json) {
        let table = id.split(':').next().unwrap();
        self.m.local(table, id, &row(patch), None, None);
    }
}

fn share() -> ChannelsConfig {
    ChannelsConfig::default()
        .share("note")
        .share("tag")
        .share("call")
}

fn note(id: &str, extra: Json) -> Row {
    let mut r = row(json!({"id": format!("note:{id}"), "folder": "a", "title": "t"}));
    r.extend(row(extra));
    r
}

fn tag() -> Row {
    row(json!({"id": "tag:1", "note": "note:1", "label": "l"}))
}

fn ids(sent: &[Sent]) -> Vec<String> {
    sent.iter()
        .map(|s| s.message["id"].as_str().unwrap().to_string())
        .collect()
}

fn op(table: &str, id: &str, patch: Json) -> Json {
    json!({"t": "op", "table": table, "id": id, "patch": patch, "hlc": [9_000_000_000_000_000i64, 0, "x"]})
}

#[tokio::test]
async fn sends_an_update_of_a_shared_table_to_the_other_device_which_shows_it_at_once() {
    run_local(async {
        let net = MemoryNet::new();
        let phone = Device::new("user:a", &net, share(), vec![note("1", json!({}))]);
        let laptop = Device::new("user:a", &net, share(), vec![note("1", json!({}))]);
        let view = laptop.show(Some(&in_folder("a")));
        flush().await;
        assert!(phone.m.sharing());
        assert!(phone.m.enabled());
        assert_eq!(
            phone.m.own_state(),
            ChannelState {
                status: ChannelStatus::Connected,
                peers: vec!["user:a".into()],
            }
        );
        phone.update("note:1", json!({"title": "new"}));
        flush().await;
        assert_eq!(view.field(0, "title"), Value::from("new"));
        // Never in the store: the synced row confirms it, or it expires.
        assert_eq!(laptop.fr.get("note:1").unwrap()["title"], Value::from("t"));
    })
    .await;
}

#[tokio::test]
async fn carries_creates_and_deletes_too() {
    run_local(async {
        let net = MemoryNet::new();
        let phone = Device::new("user:a", &net, share(), vec![note("1", json!({}))]);
        let laptop = Device::new("user:a", &net, share(), vec![note("1", json!({}))]);
        let _view = laptop.show(None);
        flush().await;
        phone.m.local_create(
            "note",
            "note:2",
            &row(json!({"folder": "a", "title": "two"})),
            None,
        );
        phone.m.local_delete("note", "note:1", None).await;
        flush().await;
        assert_eq!(
            laptop.fr.calls(),
            vec!["create note:2 5000", "delete note:1 5000"]
        );
        let created = laptop.fr.get("note:2").unwrap();
        assert_eq!(created["folder"], Value::from("a"));
        assert_eq!(created["title"], Value::from("two"));
    })
    .await;
}

#[tokio::test]
async fn shares_nothing_of_a_table_that_was_not_named() {
    run_local(async {
        let net = MemoryNet::new();
        let config = || ChannelsConfig::default().share("note");
        let secret = || row(json!({"id": "secret:1", "text": "x"}));
        let phone = Device::new("user:a", &net, config(), vec![secret(), tag()]);
        let laptop = Device::new("user:a", &net, config(), vec![secret()]);
        let view = laptop.show(None);
        flush().await;
        phone.update("secret:1", json!({"text": "y"}));
        phone.update("tag:1", json!({"label": "m"}));
        flush().await;
        assert_eq!(net.sent_of("op"), vec![]);
        // And a device takes none either, whoever sends it.
        let hello = net.sent_of("interest").remove(0);
        let (_, peer) = hello.from.split_once('#').unwrap();
        laptop.m.receive(
            &PeerRef::new("user:a", peer),
            &op("secret", "secret:1", json!({"text": "z"})),
        );
        flush().await;
        assert_eq!(view.field(0, "text"), Value::from("x"));
        assert_eq!(laptop.fr.calls(), Vec::<String>::new());
    })
    .await;
}

#[test]
fn warns_about_a_table_the_schema_does_not_have_and_shares_the_rest() {
    let warned: Rc<RefCell<Vec<String>>> = Rc::default();
    let w = warned.clone();
    let m = ChannelManager::new(
        schema(),
        ManagerOptions {
            config: Some(ChannelsConfig::default().share("nope").share("note")),
            logger: Some(Rc::new(move |level, message| {
                if level == LogLevel::Warn {
                    w.borrow_mut().push(message.to_string());
                }
            })),
            ..Default::default()
        },
    );
    assert!(warned.borrow()[0].contains("'nope'"), "{warned:?}");
    assert!(m.sharing());
    let nothing = ChannelsConfig {
        share: Some(ChannelShareConfig::default()),
        ..Default::default()
    };
    let unshared = ChannelManager::new(
        schema(),
        ManagerOptions {
            config: Some(nothing),
            ..Default::default()
        },
    );
    assert!(!unshared.sharing());
    assert_eq!(unshared.own_state(), ChannelState::default());
}

#[tokio::test]
async fn applies_a_rule_given_as_a_function_on_the_device_that_writes_and_on_the_one_that_receives()
{
    run_local(async {
        let net = MemoryNet::new();
        let config = || {
            ChannelsConfig::default()
                .share_rows("note", |row| row.get("draft") != Some(&Value::Bool(true)))
        };
        let held = || {
            vec![
                note("1", json!({"draft": true})),
                note("2", json!({})),
                note("5", json!({"draft": true})),
            ]
        };
        let phone = Device::new("user:a", &net, config(), held());
        let laptop = Device::new("user:a", &net, config(), held());
        let view = laptop.show(None);
        flush().await;
        phone.update("note:1", json!({"title": "still a draft"}));
        phone.update("note:2", json!({"title": "public"}));
        flush().await;
        assert_eq!(ids(&net.sent_of("op")), vec!["note:2"]);
        assert_eq!(
            view.column("title"),
            vec![Value::from("t"), Value::from("public"), Value::from("t")]
        );
        // Publishing a draft: the row after the write passes, so it goes out.
        phone.update("note:1", json!({"draft": false}));
        flush().await;
        assert_eq!(ids(&net.sent_of("op")), vec!["note:2", "note:1"]);
        // A device whose own rule says no drops what another build of the app
        // sent.
        laptop.m.receive(
            &PeerRef::new("user:a", "other"),
            &op("note", "note:5", json!({"title": "forced"})),
        );
        flush().await;
        assert_eq!(view.field(2, "title"), Value::from("t"));
        // A rule that panics shares nothing.
        let bad = Device::new(
            "user:a",
            &net,
            ChannelsConfig::default().share_rows("note", |_| panic!("boom")),
            vec![note("3", json!({}))],
        );
        bad.update("note:3", json!({"title": "x"}));
        flush().await;
        assert_eq!(net.sent_of("op").len(), 2);
    })
    .await;
}

#[tokio::test]
async fn never_shares_with_another_user_and_takes_nothing_shared_from_one() {
    run_local(async {
        let net = MemoryNet::new();
        let ann = Device::new("user:a", &net, share(), vec![note("1", json!({}))]);
        let bob = Device::new("user:b", &net, share(), vec![note("1", json!({}))]);
        let view = bob.show(None);
        flush().await;
        ann.update("note:1", json!({"title": "mine"}));
        flush().await;
        assert_eq!(net.sent(), vec![]);
        let stranger = PeerRef::new("user:a", "x");
        bob.m
            .receive(&stranger, &op("note", "note:1", json!({"title": "mine"})));
        bob.m
            .receive(&stranger, &json!({"t": "interest", "all": true}));
        flush().await;
        assert_eq!(view.field(0, "title"), Value::from("t"));
    })
    .await;
}

#[tokio::test]
async fn follows_the_signed_in_user() {
    run_local(async {
        let net = MemoryNet::new();
        let phone = Device::new("user:a", &net, share(), vec![note("1", json!({}))]);
        let laptop = Device::new("user:a", &net, share(), vec![note("1", json!({}))]);
        let view = laptop.show(None);
        flush().await;
        laptop.m.set_user(Some("user:b"));
        phone.update("note:1", json!({"title": "for a"}));
        flush().await;
        assert_eq!(net.sent_of("op"), vec![]);
        assert_eq!(laptop.m.own_state().status, ChannelStatus::Connecting);
        laptop.m.set_user(None);
        assert_eq!(laptop.m.own_state().status, ChannelStatus::Idle);
        laptop.m.set_user(Some("user:a"));
        flush().await;
        phone.update("note:1", json!({"title": "for a"}));
        flush().await;
        assert_eq!(view.field(0, "title"), Value::from("for a"));
    })
    .await;
}

#[tokio::test]
async fn reports_how_the_own_devices_are_doing_to_a_subscriber() {
    run_local(async {
        let net = MemoryNet::new();
        let phone = Device::new("user:a", &net, share(), vec![]);
        let states: Rc<RefCell<Vec<ChannelStatus>>> = Rc::default();
        let s = states.clone();
        let sub = phone
            .m
            .subscribe_own(move |state| s.borrow_mut().push(state.status));
        let laptop = Device::new("user:a", &net, share(), vec![]);
        flush().await;
        laptop.m.close();
        flush().await;
        assert_eq!(
            *states.borrow(),
            vec![
                ChannelStatus::Connecting,
                ChannelStatus::Connected,
                ChannelStatus::Connecting
            ]
        );
        drop(sub);
        let s = states.clone();
        let plain = ChannelManager::new(schema(), ManagerOptions::default());
        drop(plain.subscribe_own(move |state| s.borrow_mut().push(state.status)));
        assert_eq!(states.borrow().last(), Some(&ChannelStatus::Idle));
    })
    .await;
}

#[tokio::test]
async fn also_reaches_the_own_devices_when_a_write_goes_through_a_channel_to_another_user() {
    run_local(async {
        let net = MemoryNet::new();
        let call = || vec![row(json!({"id": "call:1", "view_id": ""}))];
        let phone = Device::new("user:a", &net, share(), call());
        let laptop = Device::new("user:a", &net, share(), call());
        let bob = Device::new("user:b", &net, ChannelsConfig::default(), call());
        let scope = |d: &Device| vec![d.m.scope(&q("call")).unwrap()];
        let to_bob = phone.m.open(scope(&phone), vec!["user:b".into()]).unwrap();
        let _to_ann = bob.m.open(scope(&bob), vec!["user:a".into()]).unwrap();
        let mine = laptop.show(None);
        let theirs = bob.show(None);
        flush().await;
        phone.m.local(
            "call",
            "call:1",
            &row(json!({"view_id": "v"})),
            None,
            Some(&to_bob),
        );
        flush().await;
        assert_eq!(mine.field(0, "view_id"), Value::from("v"));
        assert_eq!(theirs.field(0, "view_id"), Value::from("v"));
    })
    .await;
}

// ---- the manager, sending only what the other device shows ---------------------------------

#[tokio::test]
async fn skips_a_device_that_shows_nothing_of_the_table_and_one_whose_filter_the_row_is_outside_of()
{
    run_local(async {
        let net = MemoryNet::new();
        let notes = || vec![note("1", json!({})), note("2", json!({"folder": "b"}))];
        let mut held = notes();
        held.push(tag());
        let phone = Device::new("user:a", &net, share(), held);
        let laptop = Device::new("user:a", &net, share(), notes());
        flush().await;
        // Nothing is mounted on the laptop: nothing is sent to it.
        phone.update("note:1", json!({"title": "x"}));
        flush().await;
        assert_eq!(net.sent_of("op"), vec![]);

        let view = laptop.show(Some(&in_folder("a")));
        flush().await;
        phone.update("note:1", json!({"title": "in a"}));
        phone.update("note:2", json!({"title": "in b"}));
        phone.update("tag:1", json!({"label": "m"}));
        flush().await;
        assert_eq!(ids(&net.sent_of("op")), vec!["note:1"]);
        assert_eq!(view.field(0, "title"), Value::from("in a"));

        // A row moving into the folder it shows, or out of it, is sent.
        phone.update("note:2", json!({"folder": "a"}));
        phone.update("note:1", json!({"folder": "z"}));
        flush().await;
        assert_eq!(ids(&net.sent_of("op")), vec!["note:1", "note:2", "note:1"]);
        // So are a create inside it and a delete of a row inside it; outside,
        // neither.
        let create = |id: &str, folder: &str| {
            let new = row(json!({"folder": folder, "title": "n"}));
            phone.m.local_create("note", id, &new, None);
        };
        create("note:3", "a");
        create("note:4", "q");
        phone.m.local_delete("note", "note:2", None).await;
        phone.m.local_delete("note", "note:1", None).await;
        flush().await;
        assert_eq!(
            ids(&net.sent_of("op")),
            vec!["note:1", "note:2", "note:1", "note:3", "note:1"]
        );

        // The query goes away: so does the claim.
        view.stop();
        flush().await;
        create("note:5", "a");
        flush().await;
        assert_eq!(net.sent_of("op").len(), 5);
    })
    .await;
}

#[tokio::test]
async fn sends_everything_of_a_table_read_through_related_and_everything_to_a_subscription_nobody_described(
) {
    run_local(async {
        let net = MemoryNet::new();
        let held = || vec![tag(), note("9", json!({"folder": "zzz"}))];
        let phone = Device::new("user:a", &net, share(), held());
        let laptop = Device::new("user:a", &net, share(), held());
        let with_tags = laptop.show(Some(&in_folder("a").related("tags")));
        flush().await;
        phone.update("tag:1", json!({"label": "m"}));
        phone.update("note:9", json!({"title": "far"}));
        flush().await;
        assert_eq!(ids(&net.sent_of("op")), vec!["tag:1"]);
        with_tags.stop();
        let _all = laptop.show(None);
        flush().await;
        phone.update("note:9", json!({"title": "far"}));
        flush().await;
        assert_eq!(ids(&net.sent_of("op")), vec!["tag:1", "note:9"]);
    })
    .await;
}

#[tokio::test]
async fn announces_once_per_change_and_tells_a_device_that_joins_later() {
    run_local(async {
        let net = MemoryNet::new();
        let held = || vec![note("1", json!({}))];
        let phone = Device::new("user:a", &net, share(), held());
        let laptop = Device::new("user:a", &net, share(), held());
        flush().await;
        let interests = || net.sent_of("interest");
        let folder = |f: &str| json!([{"field": "folder", "op": "=", "value": f}]);
        // Each told the other on meeting.
        assert_eq!(interests().len(), 2);
        let a = laptop.show(Some(&in_folder("a")));
        let b = laptop.show(Some(&in_folder("a")));
        let _c = laptop.show(Some(&in_folder("b")));
        flush().await;
        assert_eq!(interests().len(), 3);
        assert_eq!(
            interests().last().unwrap().message,
            json!({"t": "interest", "v": {"note": [folder("a"), folder("b")]}})
        );
        // One of two queries on the folder goes: the claim is the same, and
        // not said again.
        a.stop();
        a.stop();
        flush().await;
        assert_eq!(interests().len(), 3);
        b.stop();
        flush().await;
        assert_eq!(interests().len(), 4);
        assert_eq!(
            interests().last().unwrap().message["v"],
            json!({"note": [folder("b")]})
        );
        let _d = laptop.show(Some(&in_folder("a")));
        flush().await;
        assert_eq!(interests().len(), 5);
        let tablet = Device::new("user:a", &net, share(), held());
        let view = tablet.show(Some(&in_folder("a")));
        flush().await;
        phone.update("note:1", json!({"title": "all three"}));
        flush().await;
        assert_eq!(view.field(0, "title"), Value::from("all three"));
        let mut to: Vec<String> = net.sent_of("op").into_iter().map(|s| s.to).collect();
        to.sort();
        to.dedup();
        assert_eq!(to.len(), 2);
        assert_eq!(net.sent_of("op").len(), 2);
    })
    .await;
}

#[tokio::test]
async fn forgets_what_a_device_showed_once_it_is_gone() {
    run_local(async {
        let net = MemoryNet::new();
        let held = || vec![note("1", json!({}))];
        let phone = Device::new("user:a", &net, share(), held());
        let laptop = Device::new("user:a", &net, share(), held());
        let _view = laptop.show(Some(&in_folder("nothing")));
        flush().await;
        phone.update("note:1", json!({"title": "x"}));
        flush().await;
        assert_eq!(net.sent_of("op"), vec![]);
        laptop.m.close();
        let again = Device::new("user:a", &net, share(), held());
        let _all = again.show(None);
        flush().await;
        phone.update("note:1", json!({"title": "y"}));
        flush().await;
        assert_eq!(net.sent_of("op").len(), 1);
        // Garbage in place of an interest changes nothing.
        phone.m.receive(
            &PeerRef::new("user:a", "x"),
            &json!({"t": "interest", "v": 7}),
        );
    })
    .await;
}

#[tokio::test]
async fn a_user_with_one_device_pays_nothing_for_a_write() {
    run_local(async {
        let net = MemoryNet::new();
        let phone = Device::new("user:a", &net, share(), vec![note("1", json!({}))]);
        flush().await;
        assert_eq!(phone.m.own_state().status, ChannelStatus::Connecting);
        phone.update("note:1", json!({"title": "x"}));
        phone.m.local_delete("note", "note:1", None).await;
        flush().await;
        // Nobody to send it to: the row is not even read.
        assert_eq!(phone.fr.reads(), 0);
        assert_eq!(net.sent(), vec![]);
    })
    .await;
}

// ---- the two transports as one -----------------------------------------------------------------

struct Named {
    name: &'static str,
    supported: bool,
    log: Rc<RefCell<Vec<String>>>,
}

impl ChannelTransport for Named {
    fn supported(&self) -> bool {
        self.supported
    }

    fn want(&self, users: &BTreeSet<String>) {
        let users: Vec<&str> = users.iter().map(String::as_str).collect();
        self.log
            .borrow_mut()
            .push(format!("{} want {}", self.name, users.join(",")));
    }

    fn send(&self, to: &PeerRef, _message: &ChannelWire) {
        self.log
            .borrow_mut()
            .push(format!("{} send {}", self.name, to.user));
    }

    fn close(&self) {
        self.log.borrow_mut().push(format!("{} close", self.name));
    }
}

struct As(Option<&'static str>);

impl TransportHooks for As {
    fn self_ref(&self) -> Option<PeerRef> {
        self.0.map(|user| PeerRef::new(user, "p"))
    }
    fn on_message(&self, _from: &PeerRef, _message: &Json) {}
    fn on_peer(&self, _peer: &PeerRef, _up: bool) {}
    fn on_status(&self, _user: &str, _status: PeerLinkStatus) {}
}

fn named(
    name: &'static str,
    supported: bool,
    log: &Rc<RefCell<Vec<String>>>,
) -> LocalTransportFactory {
    let log = log.clone();
    Rc::new(move |_| {
        Rc::new(Named {
            name,
            supported,
            log: log.clone(),
        }) as Rc<dyn ChannelTransport>
    })
}

fn users(list: &[&str]) -> BTreeSet<String> {
    list.iter().map(|u| u.to_string()).collect()
}

#[test]
fn reaches_the_own_user_through_the_room_and_everybody_else_through_the_mesh() {
    let log: Rc<RefCell<Vec<String>>> = Rc::default();
    let split = split_transport(Some(named("room", true, &log)), named("mesh", false, &log));
    let t = split(Rc::new(As(Some("user:a"))));
    let all = ChannelWire::Interest(Interest::All);
    t.want(&users(&["user:a", "user:b"]));
    t.send(&PeerRef::new("user:a", "x"), &all);
    t.send(&PeerRef::new("user:b", "y"), &all);
    t.want(&users(&["user:b"]));
    t.close();
    assert_eq!(
        *log.borrow(),
        vec![
            "room want user:a",
            "mesh want user:b",
            "room send user:a",
            "mesh send user:b",
            "room want ",
            "mesh want user:b",
            "room close",
            "mesh close",
        ]
    );
    assert!(t.supported());
    // Each side answers for the users it reaches: the room is there, WebRTC
    // is not.
    assert!(t.supports("user:a"));
    assert!(!t.supports("user:b"));
}

#[test]
fn is_the_mesh_alone_without_a_room() {
    let log: Rc<RefCell<Vec<String>>> = Rc::default();
    let t = split_transport(None, named("mesh", true, &log))(Rc::new(As(Some("user:a"))));
    t.want(&users(&["user:a", "user:b"]));
    t.send(
        &PeerRef::new("user:a", "x"),
        &ChannelWire::Interest(Interest::All),
    );
    t.close();
    assert_eq!(
        *log.borrow(),
        vec!["mesh want user:b", "mesh send user:a", "mesh close"]
    );
    assert!(t.supported());
    // Nothing reaches the user's own devices without a room.
    assert!(!t.supports("user:a"));
    assert!(t.supports("user:b"));
    let signed_out = split_transport(None, named("mesh", false, &log))(Rc::new(As(None)));
    signed_out.want(&users(&["user:a"]));
    assert_eq!(log.borrow().last().unwrap(), "mesh want user:a");
    assert!(!signed_out.supported());
}

#[tokio::test]
async fn a_channel_is_unsupported_only_when_its_own_side_of_the_transport_is() {
    run_local(async {
        let log: Rc<RefCell<Vec<String>>> = Rc::default();
        let m = ChannelManager::new(
            schema(),
            ManagerOptions {
                config: Some(share()),
                transport: Some(split_transport(
                    Some(named("room", true, &log)),
                    named("mesh", false, &log),
                )),
                ..Default::default()
            },
        );
        m.set_user(Some("user:a"));
        let to_bob = m
            .open(vec![m.scope(&q("call")).unwrap()], vec!["user:b".into()])
            .unwrap();
        // The room is there, so the own devices are being looked for; there
        // is no WebRTC stack, so nobody else can be reached.
        assert_eq!(m.own_state().status, ChannelStatus::Connecting);
        assert_eq!(to_bob.state().status, ChannelStatus::Unsupported);
    })
    .await;
}
