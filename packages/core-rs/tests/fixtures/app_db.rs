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
        "thread".to_string(),
        TableSchema {
            columns: BTreeMap::from([
                ("title".to_string(), ColumnSchema::of_type("string")),
                ("score".to_string(), ColumnSchema::of_type("int")),
                ("published".to_string(), ColumnSchema::of_type("bool")),
            ]),
            channel: false,
        },
    );
    schema
}

/// The schema's SurrealQL with server-only fields stripped: the local cache DDL.
pub const SURQL_SCHEMA: &str = r#"DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD title ON thread TYPE string;
DEFINE FIELD score ON thread TYPE int;
DEFINE FIELD published ON thread TYPE bool;
"#;

/// A client config for this schema.
pub fn config(database: ::spooky_core::DatabaseConfig) -> ::spooky_core::Sp00kyConfig {
    ::spooky_core::Sp00kyConfig::new(database, schema(), SURQL_SCHEMA)
}

/// A `thread` row.
#[derive(Clone, Debug, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct Thread {
    /// The record id, `table:key`.
    pub id: String,
    pub title: String,
    pub score: i64,
    pub published: bool,
}

/// A partial update of a `thread` row: only the fields that are set are written.
#[derive(Clone, Debug, Default, PartialEq, ::spooky_core::typed::__serde::Serialize, ::spooky_core::typed::__serde::Deserialize)]
#[serde(crate = "::spooky_core::typed::__serde")]
pub struct ThreadPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published: Option<bool>,
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
    pub const score: ::spooky_core::typed::NumField = ::spooky_core::typed::NumField::new("score");
    pub const published: ::spooky_core::typed::BoolField = ::spooky_core::typed::BoolField::new("published");
}

/// The typed facade over a client (Dart `AppDb`).
pub struct AppDb<C> {
    pub client: C,
}

impl<C: ::spooky_core::typed::TypedClient> AppDb<C> {
    pub fn new(client: C) -> Self {
        AppDb { client }
    }

    pub fn thread(&self) -> ::spooky_core::typed::Collection<'_, Thread, C> {
        ::spooky_core::typed::Collection::new(&self.client)
    }
}
