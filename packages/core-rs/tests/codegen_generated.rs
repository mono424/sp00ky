//! Compiles the generated goldens (`tests/fixtures/*.rs`, kept fresh by
//! `codegen_goldens.rs`) against the real `spooky_core` API and drives them:
//! the Dart `typed_usage_test.dart` over a fake client, since the Rust client
//! is wired separately. Builds without the `codegen` feature too: generated
//! code needs none.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use spooky_core::modules::query_builder::{QueryBuilder, RelationPlan};
use spooky_core::modules::query_host::QueryHost;
use spooky_core::runtime::{RecordsCallback, Subscription};
use spooky_core::typed::{Datetime, Table, TypedClient};
use spooky_core::value::object_from_json;
use spooky_core::{
    Cardinality, Object, QueryHash, Result, Row, RunOptions, Schema, UpdateOptions, Value,
};

#[allow(dead_code)]
mod app_db {
    include!("fixtures/app_db.rs");
}

#[allow(dead_code)]
mod rich {
    include!("fixtures/rich.rs");
}

/// Records registrations and lets a test push the rows a query would deliver.
#[derive(Default)]
struct Host {
    registered: RefCell<Vec<(String, Object)>>,
    feed: RefCell<Option<RecordsCallback>>,
}

impl Host {
    fn push(&self, rows: serde_json::Value) {
        let rows: Vec<Row> = serde_json::from_value::<Vec<serde_json::Value>>(rows)
            .unwrap()
            .into_iter()
            .map(object_from_json)
            .collect();
        let feed = self.feed.borrow().clone().expect("a live subscription");
        feed(&Arc::new(rows));
    }

    fn last_sql(&self) -> String {
        self.registered.borrow().last().unwrap().0.clone()
    }

    fn last_vars(&self) -> Object {
        self.registered.borrow().last().unwrap().1.clone()
    }
}

#[async_trait(?Send)]
impl QueryHost for Host {
    async fn register_query(
        &self,
        _table: &str,
        surql: &str,
        params: Object,
        _ttl: &str,
        _relations: Vec<RelationPlan>,
    ) -> Result<QueryHash> {
        self.registered
            .borrow_mut()
            .push((surql.to_string(), params));
        Ok("hash".into())
    }

    fn subscribe(&self, _hash: &str, callback: RecordsCallback, immediate: bool) -> Subscription {
        if immediate {
            callback(&Arc::new(Vec::new()));
        }
        *self.feed.borrow_mut() = Some(callback);
        Subscription::noop()
    }
}

/// A [`TypedClient`] that records every write.
struct Client {
    host: Rc<Host>,
    schema: Rc<Schema>,
    calls: RefCell<Vec<(String, String, Object)>>,
}

impl Client {
    fn new(schema: Schema) -> Self {
        Client {
            host: Rc::default(),
            schema: Rc::new(schema),
            calls: RefCell::default(),
        }
    }

    fn record(&self, op: &str, target: String, data: Object) {
        self.calls.borrow_mut().push((op.to_string(), target, data));
    }

    fn last(&self) -> (String, String, Object) {
        self.calls.borrow().last().unwrap().clone()
    }
}

#[async_trait(?Send)]
impl TypedClient for Client {
    fn query(&self, table: &str) -> QueryBuilder {
        QueryBuilder::bound(table, self.host.clone(), self.schema.clone())
    }
    async fn create(&self, id: &str, data: Object) -> Result<Row> {
        self.record("create", id.to_string(), data.clone());
        Ok(data)
    }
    async fn update(
        &self,
        table: &str,
        id: &str,
        data: Object,
        _options: UpdateOptions,
    ) -> Result<Row> {
        self.record("update", format!("{table} {id}"), data.clone());
        Ok(data)
    }
    async fn delete(&self, table: &str, id: &str) -> Result<()> {
        self.record("delete", format!("{table} {id}"), Object::new());
        Ok(())
    }
    async fn run(
        &self,
        backend: &str,
        path: &str,
        payload: Object,
        _options: RunOptions,
    ) -> Result<()> {
        self.record("run", format!("{backend} {path}"), payload);
        Ok(())
    }
    async fn sign_in(&self, access: &str, params: Object) -> Result<()> {
        self.record("sign_in", access.to_string(), params);
        Ok(())
    }
    async fn sign_up(&self, access: &str, params: Object) -> Result<()> {
        self.record("sign_up", access.to_string(), params);
        Ok(())
    }
}

fn obj(v: serde_json::Value) -> Object {
    object_from_json(v)
}

// ---- Dart `typed_usage_test.dart` over the Dart fixture schema ------------------------------

#[tokio::test]
async fn typed_create_and_typed_filtered_watch_yield_typed_models() {
    use app_db::{AppDb, Thread};
    let db = AppDb::new(Client::new(app_db::schema()));

    let emissions: Rc<RefCell<Vec<Vec<Thread>>>> = Rc::default();
    let e = emissions.clone();
    let _sub = db
        .thread()
        .query()
        .where_([Thread::published.eq(true), Thread::score.gte(10)])
        .order_by_desc(Thread::score)
        .watch("10m", move |rows| e.borrow_mut().push(rows.unwrap()))
        .await
        .unwrap();
    assert_eq!(
        db.client.host.last_sql(),
        "SELECT * FROM thread WHERE published = $published AND score >= $score ORDER BY score DESC;"
    );
    assert_eq!(
        db.client.host.last_vars(),
        obj(json!({"published": true, "score": 10}))
    );

    db.thread()
        .create(&Thread {
            id: "thread:a".into(),
            title: "hi".into(),
            score: 20,
            published: true,
        })
        .await
        .unwrap();
    assert_eq!(
        db.client.last(),
        (
            "create".to_string(),
            "thread:a".to_string(),
            obj(json!({"title": "hi", "score": 20, "published": true}))
        ),
        "the id names the record and is not part of the data"
    );

    db.client
        .host
        .push(json!([{"id": "thread:a", "title": "hi", "score": 20, "published": true}]));
    let latest = emissions.borrow().last().unwrap().clone();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].id, "thread:a");
    assert_eq!(latest[0].title, "hi");
    assert_eq!(latest[0].record_id(), "thread:a");
    assert_eq!(Thread::TABLE, "thread");
}

#[tokio::test]
async fn typed_update_via_patch_writes_only_set_fields_and_watch_one_reads_it() {
    use app_db::{AppDb, Thread, ThreadPatch};
    let db = AppDb::new(Client::new(app_db::schema()));

    let seen: Rc<RefCell<Vec<Option<Thread>>>> = Rc::default();
    let s = seen.clone();
    let _sub = db
        .thread()
        .query()
        .where_([Thread::id.eq("thread:x")])
        .watch_one("10m", move |t| s.borrow_mut().push(t.unwrap()))
        .await
        .unwrap();
    assert!(db
        .client
        .host
        .last_sql()
        .ends_with("WHERE id = $id LIMIT 1;"));
    assert!(
        matches!(db.client.host.last_vars()["id"], Value::RecordId(_)),
        "an id condition binds a record"
    );

    db.thread()
        .update(
            "thread:x",
            &ThreadPatch {
                title: Some("v2".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        db.client.last(),
        (
            "update".to_string(),
            "thread thread:x".to_string(),
            obj(json!({"title": "v2"}))
        )
    );

    db.client
        .host
        .push(json!([{"id": "thread:x", "title": "v2", "score": 1, "published": true}]));
    assert_eq!(seen.borrow().last().unwrap().as_ref().unwrap().title, "v2");

    db.thread().delete("thread:x").await.unwrap();
    assert_eq!(db.client.last().1, "thread thread:x");
}

// ---- the richer schema --------------------------------------------------------------------------

#[test]
fn the_rich_runtime_schema() {
    let s = rich::schema();
    let names: Vec<&str> = s.table_names().collect();
    assert_eq!(
        names,
        ["call", "comment", "thread", "user"],
        "nosync and system tables stay out"
    );
    assert!(
        !s.tables["user"].columns.contains_key("password"),
        "a nosync field stays out"
    );
    assert!(s.tables["thread"].columns["author"].record_id);
    assert!(s.tables["thread"].columns["edited_at"].date_time);
    assert!(s.tables["thread"].columns["edited_at"].optional);
    assert!(s.tables["call"].channel);
    assert!(s.tables["call"].columns["cursor_pos"].ephemeral);
    assert!(s.relationships.iter().any(|r| r.from == "thread"
        && r.field == "comments"
        && r.to == "comment"
        && r.cardinality == Cardinality::Many));
    assert_eq!(
        s.access["account"].signup.params.keys().collect::<Vec<_>>(),
        ["display_name", "email", "password"]
    );
    assert_eq!(s.backends["api"].outbox_table, "job_outbox");
    assert!(!s.backends["api"].routes["/spookify"].args["id"].optional);
    assert!(!rich::SURQL_SCHEMA.contains("DEFINE FIELD password"));

    let config = rich::config(spooky_core::DatabaseConfig::new("ns", "db"));
    assert_eq!(config.schema, s);
}

#[test]
fn rich_models_round_trip_through_rows() {
    use rich::{Comment, Thread, ThreadPatch};
    let row = obj(json!({
        "id": "thread:t1",
        "title": "Hello",
        "author": "user:u1",
        "published": true,
        "score": 3,
        "rating": 4.5,
        "tags": ["a", "b"],
        "watchers": ["user:u2"],
        "edited_at": "2026-01-02T03:04:05.678Z",
        "meta": {"color": "red"},
        "type": "post",
        "server_only": "ignored",
    }));
    let thread: Thread = spooky_core::from_row(&row).unwrap();
    assert_eq!(thread.r#type, "post");
    assert_eq!(
        thread.watchers.as_deref(),
        Some(&["user:u2".to_string()][..])
    );
    assert_eq!(
        thread.edited_at.unwrap().to_string(),
        "2026-01-02T03:04:05.678Z"
    );
    assert_eq!(thread.meta.as_ref().unwrap()["color"], Value::from("red"));
    assert!(thread.body.is_none());

    let back = spooky_core::to_value(&thread).unwrap();
    assert_eq!(back.get("type"), Some(&Value::from("post")));
    assert!(
        back.get("body").is_none(),
        "an unset option is left out, not null"
    );

    let patch = spooky_core::to_value(&ThreadPatch {
        edited_at: Datetime::parse("2026-03-01T00:00:00Z"),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        patch,
        Value::from(json!({"edited_at": "2026-03-01T00:00:00.000Z"}))
    );

    let missing: Result<Comment> = spooky_core::from_row(&obj(json!({"id": "comment:c"})));
    assert!(missing.is_err(), "a required field must be present");
}

#[tokio::test]
async fn rich_typed_queries_bind_schema_typed_params() {
    use rich::{AppDb, Comment, Thread};
    let db = AppDb::new(Client::new(rich::schema()));
    db.thread()
        .query()
        .where_([
            Thread::author.eq("user:u1"),
            Thread::edited_at.gt(Datetime::parse("2026-01-01T00:00:00Z").unwrap()),
            Thread::rating.lte(4.5),
            Thread::r#type.ne("draft"),
        ])
        .order_by(Thread::edited_at)
        .limit(20)
        .offset(40)
        .run("5m")
        .await
        .unwrap();
    assert_eq!(
        db.client.host.last_sql(),
        "SELECT * FROM thread WHERE author = $author AND edited_at > $edited_at AND rating <= $rating \
         AND type != $type ORDER BY edited_at ASC LIMIT 20 START 40;"
    );
    let vars = db.client.host.last_vars();
    assert!(
        matches!(vars["author"], Value::RecordId(_)),
        "typed from record<user>"
    );
    assert!(matches!(vars["edited_at"], Value::Datetime(_)));

    db.comment()
        .query()
        .where_([Comment::thread.eq("thread:t1")])
        .related("author")
        .run("5m")
        .await
        .unwrap();
    assert_eq!(
        db.client.host.last_sql(),
        "SELECT *, (SELECT * FROM user WHERE id=$parent.author LIMIT 1)[0] AS author FROM comment WHERE thread = $thread;"
    );
}

#[tokio::test]
async fn rich_auth_and_backend_helpers_call_the_client() {
    let db = rich::AppDb::new(Client::new(rich::schema()));
    db.auth().sign_in_account("a@b.c", "pw").await.unwrap();
    assert_eq!(
        db.client.last(),
        (
            "sign_in".to_string(),
            "account".to_string(),
            obj(json!({"email": "a@b.c", "password": "pw"}))
        )
    );
    db.auth()
        .sign_up_account("a@b.c", "pw", "Ada")
        .await
        .unwrap();
    assert_eq!(db.client.last().2["display_name"], Value::from("Ada"));

    db.run()
        .api()
        .spookify("x", Some(2), None, None, Some(0.5))
        .await
        .unwrap();
    assert_eq!(
        db.client.last(),
        (
            "run".to_string(),
            "api /spookify".to_string(),
            obj(json!({"id": "x", "count": 2, "ratio": 0.5}))
        )
    );
    db.run().api().noargs().await.unwrap();
    assert!(db.client.last().2.is_empty());
    db.run()
        .media_worker()
        .thumbnails_make("covers/a.webp")
        .await
        .unwrap();
    assert_eq!(db.client.last().1, "media-worker /thumbnails/make");
}
