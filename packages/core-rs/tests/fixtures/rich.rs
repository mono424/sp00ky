// GENERATED CODE - DO NOT EDIT BY HAND.
// Generated from a SurrealQL schema by spooky_core::codegen.

/// The runtime schema: tables, relationships, access methods and backends.
pub fn schema() -> ::spooky_core::Schema {
    #[allow(unused_imports)]
    use ::spooky_core::{AccessDef, AccessMethod, BackendDef, BackendRoute, Cardinality, ColumnSchema, RouteArg, Schema, SchemaRelationship, TableSchema};
    #[allow(unused_imports)]
    use ::std::collections::BTreeMap;
    #[allow(unused_mut)]
    let mut schema = Schema::default();
    schema.tables.insert(
        "user".to_string(),
        TableSchema {
            columns: BTreeMap::from([
                ("email".to_string(), ColumnSchema::of_type("string")),
                ("display_name".to_string(), ColumnSchema { kind: Some("string".to_string()), optional: true, ..Default::default() }),
                ("created_at".to_string(), ColumnSchema { kind: Some("datetime".to_string()), date_time: true, ..Default::default() }),
            ]),
            channel: false,
        },
    );
    schema.tables.insert(
        "thread".to_string(),
        TableSchema {
            columns: BTreeMap::from([
                ("title".to_string(), ColumnSchema::of_type("string")),
                ("author".to_string(), ColumnSchema { kind: Some("record".to_string()), record_id: true, ..Default::default() }),
                ("published".to_string(), ColumnSchema::of_type("bool")),
                ("score".to_string(), ColumnSchema::of_type("int")),
                ("rating".to_string(), ColumnSchema { kind: Some("float".to_string()), optional: true, ..Default::default() }),
                ("tags".to_string(), ColumnSchema::of_type("array<string>")),
                ("watchers".to_string(), ColumnSchema { kind: Some("array<record<user>>".to_string()), optional: true, ..Default::default() }),
                ("edited_at".to_string(), ColumnSchema { kind: Some("datetime".to_string()), date_time: true, optional: true, ..Default::default() }),
                ("meta".to_string(), ColumnSchema { kind: Some("object".to_string()), optional: true, ..Default::default() }),
                ("meta.color".to_string(), ColumnSchema { kind: Some("string".to_string()), optional: true, ..Default::default() }),
                ("type".to_string(), ColumnSchema::of_type("string")),
                ("body".to_string(), ColumnSchema { kind: Some("string".to_string()), optional: true, ..Default::default() }),
            ]),
            channel: false,
        },
    );
    schema.tables.insert(
        "comment".to_string(),
        TableSchema {
            columns: BTreeMap::from([
                ("body".to_string(), ColumnSchema::of_type("string")),
                ("thread".to_string(), ColumnSchema { kind: Some("record".to_string()), record_id: true, ..Default::default() }),
                ("author".to_string(), ColumnSchema { kind: Some("record".to_string()), record_id: true, ..Default::default() }),
                ("created_at".to_string(), ColumnSchema { kind: Some("datetime".to_string()), date_time: true, ..Default::default() }),
            ]),
            channel: false,
        },
    );
    schema.tables.insert(
        "call".to_string(),
        TableSchema {
            columns: BTreeMap::from([
                ("room".to_string(), ColumnSchema::of_type("string")),
                ("cursor_pos".to_string(), ColumnSchema { kind: Some("object".to_string()), optional: true, ephemeral: true, ..Default::default() }),
            ]),
            channel: true,
        },
    );
    let rel = |from: &str, field: &str, to: &str, cardinality: Cardinality| SchemaRelationship {
        from: from.to_string(),
        field: field.to_string(),
        to: to.to_string(),
        cardinality,
    };
    schema.relationships = vec![
        rel("thread", "author", "user", Cardinality::One),
        rel("comment", "thread", "thread", Cardinality::One),
        rel("comment", "author", "user", Cardinality::One),
        rel("user", "threads", "thread", Cardinality::Many),
        rel("thread", "comments", "comment", Cardinality::Many),
        rel("user", "comments", "comment", Cardinality::Many),
    ];
    schema.access.insert(
        "account".to_string(),
        AccessDef {
            signin: AccessMethod { params: BTreeMap::from([("email".to_string(), ColumnSchema::of_type("string")), ("password".to_string(), ColumnSchema::of_type("string"))]) },
            signup: AccessMethod { params: BTreeMap::from([("email".to_string(), ColumnSchema::of_type("string")), ("password".to_string(), ColumnSchema::of_type("string")), ("display_name".to_string(), ColumnSchema::of_type("string"))]) },
        },
    );
    schema.backends.insert(
        "api".to_string(),
        BackendDef {
            outbox_table: "job_outbox".to_string(),
            routes: BTreeMap::from([
                ("/noargs".to_string(), BackendRoute { args: BTreeMap::new() }),
                ("/spookify".to_string(), BackendRoute { args: BTreeMap::from([("count".to_string(), RouteArg { kind: Some("integer".to_string()), optional: true }), ("dry_run".to_string(), RouteArg { kind: Some("boolean".to_string()), optional: true }), ("id".to_string(), RouteArg { kind: Some("string".to_string()), optional: false }), ("options".to_string(), RouteArg { kind: None, optional: true }), ("ratio".to_string(), RouteArg { kind: Some("number".to_string()), optional: true })]) }),
            ]),
        },
    );
    schema.backends.insert(
        "media-worker".to_string(),
        BackendDef {
            outbox_table: "media_job".to_string(),
            routes: BTreeMap::from([
                ("/thumbnails/make".to_string(), BackendRoute { args: BTreeMap::from([("path".to_string(), RouteArg { kind: Some("string".to_string()), optional: false })]) }),
            ]),
        },
    );
    schema
}

/// The schema's SurrealQL with server-only fields stripped: the local cache DDL.
pub const SURQL_SCHEMA: &str = r#"-- A schema that exercises every codegen path: relations both ways, access
-- params, optional and required fields, records, datetimes, arrays, objects,
-- keyword and nested field names, and the channel, ephemeral, crdt, parent and
-- nosync annotations.

DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP (
    CREATE user SET
      email = $email,
      password = crypto::argon2::generate($password),
      display_name = $display_name
  )
  SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(password, $password) )
  DURATION FOR TOKEN 15m, FOR SESSION 30d;

DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD email ON user TYPE string ASSERT string::is::email($value);
DEFINE FIELD display_name ON user TYPE option<string>;
DEFINE FIELD created_at ON user TYPE datetime DEFAULT time::now();

DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD title ON thread TYPE string;
DEFINE FIELD author ON thread TYPE record<user>;
DEFINE FIELD published ON thread TYPE bool DEFAULT false;
DEFINE FIELD score ON thread TYPE int DEFAULT 0;
DEFINE FIELD rating ON thread TYPE option<float>;
DEFINE FIELD tags ON thread TYPE array<string> DEFAULT [];
DEFINE FIELD watchers ON thread TYPE option<array<record<user>>>;
DEFINE FIELD edited_at ON thread TYPE option<datetime>;
DEFINE FIELD meta ON thread TYPE option<object> FLEXIBLE;
DEFINE FIELD meta.color ON thread TYPE option<string>;
DEFINE FIELD type ON thread TYPE string DEFAULT "post";
-- @crdt text
DEFINE FIELD body ON thread TYPE option<string>;

DEFINE TABLE comment SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD body ON comment TYPE string;
DEFINE FIELD thread ON comment TYPE record<thread>; -- @parent
DEFINE FIELD author ON comment TYPE record<user>;
DEFINE FIELD created_at ON comment TYPE datetime
  DEFAULT time::now()
  READONLY;

-- @channel
DEFINE TABLE call SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD room ON call TYPE string;
-- @ephemeral
DEFINE FIELD cursor_pos ON call TYPE option<object>;

-- @nosync
DEFINE TABLE signup_code SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD code ON signup_code TYPE string;
"#;

/// A client config for this schema.
pub fn config(database: ::spooky_core::DatabaseConfig) -> ::spooky_core::Sp00kyConfig {
    ::spooky_core::Sp00kyConfig::new(database, schema(), SURQL_SCHEMA)
}

/// A `user` row.
#[derive(Clone, Debug, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct User {
    /// The record id, `table:key`.
    pub id: String,
    pub email: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub created_at: ::spooky_core::typed::Datetime,
}

/// A partial update of a `user` row: only the fields that are set are written.
#[derive(Clone, Debug, Default, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct UserPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<::spooky_core::typed::Datetime>,
}

impl ::spooky_core::typed::Table for User {
    const TABLE: &'static str = "user";
    type Patch = UserPatch;

    fn record_id(&self) -> &str {
        &self.id
    }
}

/// Typed field tokens for `user`: `User::<field>.eq(..)`.
#[allow(non_upper_case_globals)]
impl User {
    pub const id: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("id");
    pub const email: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("email");
    pub const display_name: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("display_name");
    pub const created_at: ::spooky_core::typed::DateTimeField = ::spooky_core::typed::DateTimeField::new("created_at");
}

/// A `thread` row.
#[derive(Clone, Debug, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct Thread {
    /// The record id, `table:key`.
    pub id: String,
    pub title: String,
    pub author: String,
    pub published: bool,
    pub score: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rating: Option<f64>,
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watchers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_at: Option<::spooky_core::typed::Datetime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<::spooky_core::Object>,
    pub r#type: String,
    /// CRDT (`text`): edited collaboratively; this is the server-written text mirror of the document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

/// A partial update of a `thread` row: only the fields that are set are written.
#[derive(Clone, Debug, Default, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct ThreadPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rating: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watchers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_at: Option<::spooky_core::typed::Datetime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<::spooky_core::Object>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

impl ::spooky_core::typed::Table for Thread {
    const TABLE: &'static str = "thread";
    type Patch = ThreadPatch;

    fn record_id(&self) -> &str {
        &self.id
    }
}

/// Typed field tokens for `thread`: `Thread::<field>.eq(..)`.
#[allow(non_upper_case_globals)]
impl Thread {
    pub const id: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("id");
    pub const title: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("title");
    pub const author: ::spooky_core::typed::RecordField = ::spooky_core::typed::RecordField::new("author");
    pub const published: ::spooky_core::typed::BoolField = ::spooky_core::typed::BoolField::new("published");
    pub const score: ::spooky_core::typed::NumField = ::spooky_core::typed::NumField::new("score");
    pub const rating: ::spooky_core::typed::NumField = ::spooky_core::typed::NumField::new("rating");
    pub const edited_at: ::spooky_core::typed::DateTimeField = ::spooky_core::typed::DateTimeField::new("edited_at");
    pub const r#type: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("type");
    pub const body: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("body");
}

/// A `comment` row.
#[derive(Clone, Debug, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct Comment {
    /// The record id, `table:key`.
    pub id: String,
    pub body: String,
    /// Parent link: the row this one belongs to.
    pub thread: String,
    pub author: String,
    pub created_at: ::spooky_core::typed::Datetime,
}

/// A partial update of a `comment` row: only the fields that are set are written.
#[derive(Clone, Debug, Default, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct CommentPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<::spooky_core::typed::Datetime>,
}

impl ::spooky_core::typed::Table for Comment {
    const TABLE: &'static str = "comment";
    type Patch = CommentPatch;

    fn record_id(&self) -> &str {
        &self.id
    }
}

/// Typed field tokens for `comment`: `Comment::<field>.eq(..)`.
#[allow(non_upper_case_globals)]
impl Comment {
    pub const id: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("id");
    pub const body: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("body");
    pub const thread: ::spooky_core::typed::RecordField = ::spooky_core::typed::RecordField::new("thread");
    pub const author: ::spooky_core::typed::RecordField = ::spooky_core::typed::RecordField::new("author");
    pub const created_at: ::spooky_core::typed::DateTimeField = ::spooky_core::typed::DateTimeField::new("created_at");
}

/// A `call` row.
#[derive(Clone, Debug, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct Call {
    /// The record id, `table:key`.
    pub id: String,
    pub room: String,
    /// Ephemeral: travels between channel peers only and is never stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_pos: Option<::spooky_core::Object>,
}

/// A partial update of a `call` row: only the fields that are set are written.
#[derive(Clone, Debug, Default, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct CallPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_pos: Option<::spooky_core::Object>,
}

impl ::spooky_core::typed::Table for Call {
    const TABLE: &'static str = "call";
    type Patch = CallPatch;

    fn record_id(&self) -> &str {
        &self.id
    }
}

/// Typed field tokens for `call`: `Call::<field>.eq(..)`.
#[allow(non_upper_case_globals)]
impl Call {
    pub const id: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("id");
    pub const room: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("room");
}

/// Typed sign-in and sign-up per `DEFINE ACCESS` (params are strings).
pub struct AuthApi<'a, C: ?Sized>(pub &'a C);

impl<C: ::spooky_core::typed::TypedClient + ?Sized> AuthApi<'_, C> {
    /// `sign_in` through `DEFINE ACCESS account`.
    #[allow(clippy::too_many_arguments)]
    pub async fn sign_in_account(&self, email: &str, password: &str) -> ::spooky_core::Result<()> {
        self.0.sign_in("account", ::spooky_core::Object::from([("email".to_string(), ::spooky_core::Value::from(email)), ("password".to_string(), ::spooky_core::Value::from(password))])).await
    }
    /// `sign_up` through `DEFINE ACCESS account`.
    #[allow(clippy::too_many_arguments)]
    pub async fn sign_up_account(&self, email: &str, password: &str, display_name: &str) -> ::spooky_core::Result<()> {
        self.0.sign_up("account", ::spooky_core::Object::from([("email".to_string(), ::spooky_core::Value::from(email)), ("password".to_string(), ::spooky_core::Value::from(password)), ("display_name".to_string(), ::spooky_core::Value::from(display_name))])).await
    }
}

/// Typed backend jobs: `db.run().<backend>().<route>(..)`.
pub struct Backends<'a, C: ?Sized>(pub &'a C);

impl<'a, C: ::spooky_core::typed::TypedClient + ?Sized> Backends<'a, C> {
    pub fn api(&self) -> ApiBackend<'a, C> {
        ApiBackend(self.0)
    }
    pub fn media_worker(&self) -> MediaWorkerBackend<'a, C> {
        MediaWorkerBackend(self.0)
    }
}

/// The routes of backend `api`.
pub struct ApiBackend<'a, C: ?Sized>(pub &'a C);

impl<C: ::spooky_core::typed::TypedClient + ?Sized> ApiBackend<'_, C> {
    /// `POST /noargs`.
    #[allow(clippy::too_many_arguments)]
    pub async fn noargs(&self) -> ::spooky_core::Result<()> {
        self.0
            .run("api", "/noargs", ::spooky_core::Object::new(), ::spooky_core::RunOptions::default())
            .await
    }

    /// `POST /spookify`.
    #[allow(clippy::too_many_arguments)]
    pub async fn spookify(&self, id: &str, count: Option<i64>, dry_run: Option<bool>, options: Option<::spooky_core::Value>, ratio: Option<f64>) -> ::spooky_core::Result<()> {
        let mut payload = ::spooky_core::Object::new();
        payload.insert("id".to_string(), ::spooky_core::Value::from(id));
        if let Some(v) = count {
            payload.insert("count".to_string(), ::spooky_core::Value::from(v));
        }
        if let Some(v) = dry_run {
            payload.insert("dry_run".to_string(), ::spooky_core::Value::from(v));
        }
        if let Some(v) = options {
            payload.insert("options".to_string(), v);
        }
        if let Some(v) = ratio {
            payload.insert("ratio".to_string(), ::spooky_core::Value::from(v));
        }
        self.0
            .run("api", "/spookify", payload, ::spooky_core::RunOptions::default())
            .await
    }
}

/// The routes of backend `media-worker`.
pub struct MediaWorkerBackend<'a, C: ?Sized>(pub &'a C);

impl<C: ::spooky_core::typed::TypedClient + ?Sized> MediaWorkerBackend<'_, C> {
    /// `POST /thumbnails/make`.
    #[allow(clippy::too_many_arguments)]
    pub async fn thumbnails_make(&self, path: &str) -> ::spooky_core::Result<()> {
        let mut payload = ::spooky_core::Object::new();
        payload.insert("path".to_string(), ::spooky_core::Value::from(path));
        self.0
            .run("media-worker", "/thumbnails/make", payload, ::spooky_core::RunOptions::default())
            .await
    }
}

/// The typed facade over a client (Dart `AppDb`).
pub struct AppDb<C> {
    pub client: C,
}

impl<C: ::spooky_core::typed::TypedClient> AppDb<C> {
    pub fn new(client: C) -> Self {
        AppDb { client }
    }

    pub fn user(&self) -> ::spooky_core::typed::Collection<'_, User, C> {
        ::spooky_core::typed::Collection::new(&self.client)
    }

    pub fn thread(&self) -> ::spooky_core::typed::Collection<'_, Thread, C> {
        ::spooky_core::typed::Collection::new(&self.client)
    }

    pub fn comment(&self) -> ::spooky_core::typed::Collection<'_, Comment, C> {
        ::spooky_core::typed::Collection::new(&self.client)
    }

    pub fn call(&self) -> ::spooky_core::typed::Collection<'_, Call, C> {
        ::spooky_core::typed::Collection::new(&self.client)
    }

    pub fn run(&self) -> Backends<'_, C> {
        Backends(&self.client)
    }

    pub fn auth(&self) -> AuthApi<'_, C> {
        AuthApi(&self.client)
    }
}
