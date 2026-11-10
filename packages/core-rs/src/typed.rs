//! Typed queries over generated models.
//!
//! PORT: `packages/spooky_core/lib/src/typed/{fields,typed_query}.dart`, plus
//! the seams the generated code (`spooky_core::codegen`) targets: [`Table`]
//! for every generated model, [`TypedClient`] for the client a generated
//! `AppDb` drives, and [`Collection`] (the Dart per-table `XCollection`, made
//! generic).
//!
//! Generated code depends on nothing but `spooky_core`: its serde derives go
//! through the re-export [`__serde`].

use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::Rc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde::de::{self, DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Error, Result};
use crate::modules::query_builder::{QueryBuilder, QueryOp, WhereValue};
use crate::runtime::Subscription;
use crate::types::{QueryHash, RunOptions, UpdateOptions};
use crate::utils::parse_datetime;
use crate::value::{from_row, iso8601, to_value, Object, RecordId, Row, Value};

#[doc(hidden)]
pub use serde as __serde;

// ---- datetimes ----------------------------------------------------------------------

/// A UTC datetime as generated models carry it: serialized as the ISO-8601
/// string Dart's `toUtc().toIso8601String()` gives, read back from that string
/// (or from epoch milliseconds).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Datetime(pub DateTime<Utc>);

impl Datetime {
    pub fn now() -> Self {
        Datetime(Utc::now())
    }

    pub fn from_timestamp_millis(ms: i64) -> Option<Self> {
        Utc.timestamp_millis_opt(ms).single().map(Datetime)
    }

    pub fn parse(s: &str) -> Option<Self> {
        parse_datetime(s).map(Datetime)
    }
}

impl Deref for Datetime {
    type Target = DateTime<Utc>;
    fn deref(&self) -> &DateTime<Utc> {
        &self.0
    }
}

impl From<DateTime<Utc>> for Datetime {
    fn from(d: DateTime<Utc>) -> Self {
        Datetime(d)
    }
}

impl From<Datetime> for DateTime<Utc> {
    fn from(d: Datetime) -> Self {
        d.0
    }
}

impl From<Datetime> for Value {
    fn from(d: Datetime) -> Self {
        Value::Datetime(d.0)
    }
}

impl fmt::Display for Datetime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&iso8601(&self.0))
    }
}

impl Serialize for Datetime {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&iso8601(&self.0))
    }
}

impl<'de> Deserialize<'de> for Datetime {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Datetime;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an ISO-8601 datetime or epoch milliseconds")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Datetime, E> {
                Datetime::parse(v).ok_or_else(|| E::custom(format!("not a datetime: {v}")))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Datetime, E> {
                Datetime::from_timestamp_millis(v)
                    .ok_or_else(|| E::custom(format!("not a timestamp: {v}")))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Datetime, E> {
                self.visit_i64(i64::try_from(v).map_err(E::custom)?)
            }
        }
        d.deserialize_any(V)
    }
}

// ---- field tokens -----------------------------------------------------------------------

/// A single typed filter condition produced by a field token. `op` is `None`
/// for equality, otherwise a SurrealQL comparison operator.
#[derive(Clone, Debug, PartialEq)]
pub struct Condition {
    pub field: String,
    pub op: Option<&'static str>,
    pub value: Value,
}

impl Condition {
    fn new(field: &str, op: Option<&'static str>, value: Value) -> Self {
        Condition {
            field: field.to_string(),
            op,
            value,
        }
    }

    /// The builder form: plain equality or a [`QueryOp`].
    pub fn where_value(&self) -> WhereValue {
        match self.op {
            None => WhereValue::Eq(self.value.clone()),
            Some(op) => WhereValue::Op(QueryOp::new(op, self.value.clone())),
        }
    }
}

/// A generated, table-specific field token. Each kind exposes only the
/// operators valid for its type, so an invalid operand is a compile error.
pub trait TypedField {
    /// The declared (wire) field name.
    fn name(&self) -> &'static str;
}

/// Numbers a [`NumField`] compares against.
pub trait Numeric: Into<Value> {}
impl Numeric for i64 {}
impl Numeric for i32 {}
impl Numeric for u32 {}
impl Numeric for usize {}
impl Numeric for f64 {}
impl Numeric for f32 {}

/// What a [`RecordField`] accepts: a [`RecordId`] or its `table:id` string.
pub trait RecordRef: Into<Value> {}
impl RecordRef for RecordId {}
impl RecordRef for String {}
impl RecordRef for &str {}

macro_rules! field_token {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub struct $name(&'static str);

        impl $name {
            pub const fn new(name: &'static str) -> Self {
                $name(name)
            }
        }

        impl TypedField for $name {
            fn name(&self) -> &'static str {
                self.0
            }
        }
    };
}

field_token!(
    /// A `string` / `uuid` field.
    StringField
);
field_token!(
    /// A `bool` field.
    BoolField
);
field_token!(
    /// An `int` / `float` / `number` / `decimal` field.
    NumField
);
field_token!(
    /// A `datetime` field.
    DateTimeField
);
field_token!(
    /// A `record<...>` field. Accepts a [`RecordId`] or its `table:id` string.
    RecordField
);

macro_rules! equality {
    ($name:ident, $arg:ty, $v:ident => $conv:expr) => {
        impl $name {
            pub fn eq(&self, $v: $arg) -> Condition {
                Condition::new(self.0, None, $conv)
            }

            pub fn ne(&self, $v: $arg) -> Condition {
                Condition::new(self.0, Some("!="), $conv)
            }
        }
    };
}

macro_rules! ordered {
    ($name:ident, $arg:ty, $v:ident => $conv:expr) => {
        impl $name {
            pub fn gt(&self, $v: $arg) -> Condition {
                Condition::new(self.0, Some(">"), $conv)
            }

            pub fn gte(&self, $v: $arg) -> Condition {
                Condition::new(self.0, Some(">="), $conv)
            }

            pub fn lt(&self, $v: $arg) -> Condition {
                Condition::new(self.0, Some("<"), $conv)
            }

            pub fn lte(&self, $v: $arg) -> Condition {
                Condition::new(self.0, Some("<="), $conv)
            }
        }
    };
}

equality!(StringField, impl Into<String>, v => Value::String(v.into()));
equality!(BoolField, bool, v => Value::Bool(v));
equality!(NumField, impl Numeric, v => v.into());
ordered!(NumField, impl Numeric, v => v.into());
equality!(DateTimeField, impl Into<Datetime>, v => Value::Datetime(Into::<Datetime>::into(v).0));
ordered!(DateTimeField, impl Into<Datetime>, v => Value::Datetime(Into::<Datetime>::into(v).0));
equality!(RecordField, impl RecordRef, v => v.into());

// ---- typed queries -------------------------------------------------------------------------

/// A typed view over the dynamic [`QueryBuilder`]: filters with
/// [`Condition`]s, orders by [`TypedField`]s and decodes rows into `M`.
///
/// Conditions are AND-joined, one per field (matching the dynamic builder);
/// several conditions on one field collapse to the last.
pub struct TypedQuery<M> {
    builder: QueryBuilder,
    _model: PhantomData<fn() -> M>,
}

impl<M> Clone for TypedQuery<M> {
    fn clone(&self) -> Self {
        TypedQuery::new(self.builder.clone())
    }
}

impl<M> TypedQuery<M> {
    pub fn new(builder: QueryBuilder) -> Self {
        TypedQuery {
            builder,
            _model: PhantomData,
        }
    }

    /// The dynamic query underneath, e.g. for a channel.
    pub fn builder(&self) -> &QueryBuilder {
        &self.builder
    }

    pub fn into_builder(self) -> QueryBuilder {
        self.builder
    }

    fn map(self, f: impl FnOnce(QueryBuilder) -> QueryBuilder) -> Self {
        TypedQuery::new(f(self.builder))
    }

    /// See [`QueryBuilder::ignore_unused_args`].
    pub fn ignore_unused_args(self) -> Self {
        self.map(QueryBuilder::ignore_unused_args)
    }

    /// AND the conditions (Dart `where`).
    pub fn where_(self, conditions: impl IntoIterator<Item = Condition>) -> Self {
        self.map(|mut b| {
            for c in conditions {
                let value = c.where_value();
                b = b.where_cond(c.field, value);
            }
            b
        })
    }

    /// One `(c1 OR c2 ...)` conjunct.
    pub fn where_any(self, conditions: impl IntoIterator<Item = Condition>) -> Self {
        let branches: Vec<(String, WhereValue)> = conditions
            .into_iter()
            .map(|c| {
                let value = c.where_value();
                (c.field, value)
            })
            .collect();
        self.map(|b| b.where_or(branches))
    }

    pub fn order_by(self, field: impl TypedField) -> Self {
        self.map(|b| b.order_by(field.name(), "ASC"))
    }

    pub fn order_by_desc(self, field: impl TypedField) -> Self {
        self.map(|b| b.order_by(field.name(), "DESC"))
    }

    pub fn limit(self, count: usize) -> Self {
        self.map(|b| b.limit(count))
    }

    pub fn offset(self, count: usize) -> Self {
        self.map(|b| b.offset(count))
    }

    /// Join a declared relationship (rows carry it under `field`; give `M` a
    /// field of that name to read it).
    pub fn related(self, field: &str) -> Self {
        self.map(|b| b.related(field))
    }

    pub fn related_with(
        self,
        field: &str,
        modifier: impl FnOnce(QueryBuilder) -> QueryBuilder,
    ) -> Self {
        self.map(|b| b.related_with(field, modifier))
    }

    pub fn build(&self) -> Result<(String, Object)> {
        self.builder.build()
    }

    /// Register the query and return its hash.
    pub async fn run(&self, ttl: &str) -> Result<QueryHash> {
        self.builder.run(ttl).await
    }
}

fn decode_rows<M: DeserializeOwned>(rows: &[Row]) -> Result<Vec<M>> {
    rows.iter().map(from_row).collect()
}

impl<M: DeserializeOwned + 'static> TypedQuery<M> {
    /// Register and watch as typed result lists: `callback` fires with the
    /// current rows at once, then on every change, until the guard drops. A
    /// row that does not decode makes that emission an error.
    pub async fn watch(
        &self,
        ttl: &str,
        callback: impl Fn(Result<Vec<M>>) + 'static,
    ) -> Result<Subscription> {
        self.builder
            .subscribe(ttl, move |rows| callback(decode_rows(rows)))
            .await
    }

    /// Register and watch the single (first) result, `None` when empty.
    pub async fn watch_one(
        self,
        ttl: &str,
        callback: impl Fn(Result<Option<M>>) + 'static,
    ) -> Result<Subscription> {
        let query = self.map(QueryBuilder::one);
        query
            .watch(ttl, move |rows| {
                callback(rows.map(|r| r.into_iter().next()))
            })
            .await
    }
}

// ---- generated-code seams ---------------------------------------------------------------------

/// A generated model: one table's row.
pub trait Table: Serialize + DeserializeOwned + 'static {
    /// The table name.
    const TABLE: &'static str;
    /// The all-optional partial update.
    type Patch: Serialize;
    /// The row's `table:id`.
    fn record_id(&self) -> &str;
}

/// What the generated `AppDb` drives: the client's query and write surface.
/// The client implements it; every method mirrors the Dart `Sp00kyClient`
/// call the Dart emitter's output makes.
#[async_trait(?Send)]
pub trait TypedClient {
    /// A builder bound to the client's query host and schema.
    fn query(&self, table: &str) -> QueryBuilder;
    /// Create `id` (`table:key`) with `data` (no `id` key).
    async fn create(&self, id: &str, data: Object) -> Result<Row>;
    async fn update(
        &self,
        table: &str,
        id: &str,
        data: Object,
        options: UpdateOptions,
    ) -> Result<Row>;
    async fn delete(&self, table: &str, id: &str) -> Result<()>;
    /// Enqueue a backend job (`client.run(backend, path, payload)`).
    async fn run(
        &self,
        backend: &str,
        path: &str,
        payload: Object,
        options: RunOptions,
    ) -> Result<()>;
    async fn sign_in(&self, access: &str, params: Object) -> Result<()>;
    async fn sign_up(&self, access: &str, params: Object) -> Result<()>;
}

#[async_trait(?Send)]
impl<T: TypedClient + ?Sized> TypedClient for Rc<T> {
    fn query(&self, table: &str) -> QueryBuilder {
        (**self).query(table)
    }
    async fn create(&self, id: &str, data: Object) -> Result<Row> {
        (**self).create(id, data).await
    }
    async fn update(
        &self,
        table: &str,
        id: &str,
        data: Object,
        options: UpdateOptions,
    ) -> Result<Row> {
        (**self).update(table, id, data, options).await
    }
    async fn delete(&self, table: &str, id: &str) -> Result<()> {
        (**self).delete(table, id).await
    }
    async fn run(
        &self,
        backend: &str,
        path: &str,
        payload: Object,
        options: RunOptions,
    ) -> Result<()> {
        (**self).run(backend, path, payload, options).await
    }
    async fn sign_in(&self, access: &str, params: Object) -> Result<()> {
        (**self).sign_in(access, params).await
    }
    async fn sign_up(&self, access: &str, params: Object) -> Result<()> {
        (**self).sign_up(access, params).await
    }
}

fn to_object<T: Serialize + ?Sized>(value: &T) -> Result<Object> {
    to_value(value)?
        .into_object()
        .ok_or_else(|| Error::InvalidArgument("a model must serialize to an object".into()))
}

/// The typed collection of one table (Dart `XCollection`).
pub struct Collection<'a, T, C: ?Sized> {
    client: &'a C,
    _table: PhantomData<fn() -> T>,
}

impl<'a, T: Table, C: TypedClient + ?Sized> Collection<'a, T, C> {
    pub fn new(client: &'a C) -> Self {
        Collection {
            client,
            _table: PhantomData,
        }
    }

    pub fn query(&self) -> TypedQuery<T> {
        TypedQuery::new(self.client.query(T::TABLE))
    }

    /// Create the row `model` describes (its `id` names the record).
    pub async fn create(&self, model: &T) -> Result<Row> {
        let mut data = to_object(model)?;
        data.remove("id");
        self.client.create(model.record_id(), data).await
    }

    /// Write the fields `patch` sets.
    pub async fn update(&self, id: &str, patch: &T::Patch) -> Result<Row> {
        self.update_with(id, patch, UpdateOptions::default()).await
    }

    pub async fn update_with(
        &self,
        id: &str,
        patch: &T::Patch,
        options: UpdateOptions,
    ) -> Result<Row> {
        self.client
            .update(T::TABLE, id, to_object(patch)?, options)
            .await
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        self.client.delete(T::TABLE, id).await
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::modules::query_builder::RelationPlan;
    use crate::modules::query_host::QueryHost;
    use crate::runtime::RecordsCallback;
    use crate::testing::run_local;
    use crate::value::object_from_json;

    /// A toy model standing in for a generated one.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Thread {
        id: String,
        title: String,
    }

    /// Field tokens a generator would emit for `thread`.
    #[allow(non_upper_case_globals)]
    impl Thread {
        const id: StringField = StringField::new("id");
        const title: StringField = StringField::new("title");
        const score: NumField = NumField::new("score");
        const published: BoolField = BoolField::new("published");
        const author: RecordField = RecordField::new("author");
        const created_at: DateTimeField = DateTimeField::new("created_at");
    }

    /// Captures what the builder registers and lets a test push rows.
    #[derive(Default)]
    struct Host {
        sql: RefCell<String>,
        vars: RefCell<Object>,
        feed: RefCell<Option<RecordsCallback>>,
    }

    impl Host {
        fn push(&self, rows: serde_json::Value) {
            let rows: Vec<Row> = serde_json::from_value::<Vec<serde_json::Value>>(rows)
                .unwrap()
                .into_iter()
                .map(object_from_json)
                .collect();
            let cb = self.feed.borrow().clone().unwrap();
            cb(&Arc::new(rows));
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
            *self.sql.borrow_mut() = surql.to_string();
            *self.vars.borrow_mut() = params;
            Ok("hash1".into())
        }

        fn subscribe(
            &self,
            _hash: &str,
            callback: RecordsCallback,
            immediate: bool,
        ) -> Subscription {
            if immediate {
                callback(&Arc::new(Vec::new()));
            }
            *self.feed.borrow_mut() = Some(callback);
            Subscription::noop()
        }
    }

    fn typed(host: &Rc<Host>) -> TypedQuery<Thread> {
        TypedQuery::new(QueryBuilder::new("thread").with_host(host.clone()))
    }

    #[tokio::test]
    async fn typed_where_and_order_by_compile_to_the_right_surql_and_vars() {
        let host = Rc::new(Host::default());
        typed(&host)
            .where_([Thread::published.eq(true), Thread::score.gt(10)])
            .order_by_desc(Thread::created_at)
            .limit(5)
            .run("10m")
            .await
            .unwrap();
        assert_eq!(
            *host.sql.borrow(),
            "SELECT * FROM thread WHERE published = $published AND score > $score ORDER BY created_at DESC LIMIT 5;"
        );
        assert_eq!(
            *host.vars.borrow(),
            object_from_json(json!({"published": true, "score": 10}))
        );
    }

    #[test]
    fn eq_uses_a_plain_value_and_comparisons_use_an_operator() {
        assert_eq!(Thread::title.eq("x").op, None);
        assert_eq!(Thread::title.ne("x").op, Some("!="));
        assert_eq!(Thread::score.gt(3).op, Some(">"));
        assert_eq!(Thread::score.lte(9.5).op, Some("<="));
        assert_eq!(Thread::id.name(), "id");
        let at = Datetime::parse("2026-01-02T03:04:05Z").unwrap();
        assert_eq!(Thread::created_at.gte(at).value, Value::Datetime(at.0));
    }

    #[tokio::test]
    async fn record_field_eq_accepts_a_record_id_or_string() {
        let host = Rc::new(Host::default());
        typed(&host)
            .where_([Thread::author.eq(RecordId::new("user", "u1"))])
            .run("10m")
            .await
            .unwrap();
        assert!(matches!(host.vars.borrow()["author"], Value::RecordId(_)));
        typed(&host)
            .where_([Thread::author.eq("user:u2")])
            .run("10m")
            .await
            .unwrap();
        assert_eq!(host.vars.borrow()["author"], Value::from("user:u2"));
    }

    #[tokio::test]
    async fn watch_maps_rows_to_typed_models() {
        run_local(async {
            let host = Rc::new(Host::default());
            let emissions: Rc<RefCell<Vec<Vec<Thread>>>> = Rc::default();
            let e = emissions.clone();
            let _sub = typed(&host)
                .where_([Thread::published.eq(true)])
                .watch("10m", move |rows| e.borrow_mut().push(rows.unwrap()))
                .await
                .unwrap();
            host.push(
                json!([{"id": "thread:a", "title": "hello"}, {"id": "thread:b", "title": "world"}]),
            );
            let last = emissions
                .borrow()
                .last()
                .map(|l| l.iter().map(|t| t.title.clone()).collect::<Vec<_>>());
            assert_eq!(last.unwrap(), ["hello", "world"]);

            let errors = Rc::new(RefCell::new(0));
            let e = errors.clone();
            let _bad = typed(&host)
                .watch("10m", move |rows| {
                    if rows.is_err() {
                        *e.borrow_mut() += 1;
                    }
                })
                .await
                .unwrap();
            host.push(json!([{"id": "thread:c"}]));
            assert_eq!(
                *errors.borrow(),
                1,
                "a row missing a required field is an error emission"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn watch_one_sets_limit_1_and_maps_to_first_or_none() {
        run_local(async {
            let host = Rc::new(Host::default());
            let got: Rc<RefCell<Vec<Option<Thread>>>> = Rc::default();
            let g = got.clone();
            let _sub = typed(&host)
                .where_([Thread::id.eq("thread:a")])
                .watch_one("10m", move |t| g.borrow_mut().push(t.unwrap()))
                .await
                .unwrap();
            host.push(json!([]));
            host.push(json!([{"id": "thread:a", "title": "solo"}]));
            assert!(got.borrow()[0].is_none());
            assert_eq!(got.borrow().last().unwrap().as_ref().unwrap().title, "solo");
            assert!(host.sql.borrow().contains("LIMIT 1"));
        })
        .await;
    }

    #[test]
    fn where_any_is_one_or_group() {
        let (sql, _) = TypedQuery::<Thread>::new(QueryBuilder::new("thread"))
            .where_any([Thread::title.eq("a"), Thread::score.lt(2)])
            .build()
            .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM thread WHERE (title = $title__or0 OR score < $score__or1);"
        );
    }

    #[test]
    fn datetime_round_trips_as_iso_and_reads_millis() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct M {
            at: Datetime,
        }
        let m: M = serde_json::from_value(json!({"at": "2026-01-02T03:04:05.678Z"})).unwrap();
        assert_eq!(
            serde_json::to_value(&m).unwrap(),
            json!({"at": "2026-01-02T03:04:05.678Z"})
        );
        let m: M = serde_json::from_value(json!({"at": 1_000})).unwrap();
        assert_eq!(m.at.to_string(), "1970-01-01T00:00:01.000Z");
        assert!(serde_json::from_value::<M>(json!({"at": "nope"})).is_err());
    }
}
