//! Parsed schema -> a Rust source file.
//!
//! The Rust counterpart of `packages/spooky_core/lib/src/codegen/dart_emitter.dart`:
//! the runtime schema, the embedded SurrealQL, a serde model and a patch per
//! table, field tokens, and typed auth and backend helpers over
//! [`crate::typed::TypedClient`]. The output names only `::spooky_core` paths,
//! so it compiles with nothing but `spooky_core` as a dependency (no `codegen`
//! feature needed at runtime).

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::LazyLock;

use regex::Regex;

use super::openapi::{ArgKind, OpenApiBackend};
use super::schema_parser::{
    derive_relationships, parse_project, strip_server_only_fields, ParsedField, ParsedTable,
};
use crate::error::{Error, Result};
use crate::types::Cardinality;

const ST: &str = "::spooky_core";
const TY: &str = "::spooky_core::typed";

static ARRAY_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)^(?:array|set)\s*<\s*(.*?)\s*>$").unwrap());
static MAX_LEN_SUFFIX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s*,\s*\d+\s*$").unwrap());

/// Strict and reserved keywords (2021, plus `gen`). `r#` works for all but the
/// few in [`NOT_RAW`].
const KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "try", "type",
    "typeof", "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];
const NOT_RAW: &[&str] = &["crate", "self", "Self", "super", "_"];

/// A Rust string literal for `s`.
fn lit(s: &str) -> String {
    format!("{s:?}")
}

/// A Rust identifier for `name`: keywords raw (`r#type`), invalid characters
/// replaced, a leading digit prefixed.
fn ident(name: &str) -> String {
    if NOT_RAW.contains(&name) {
        return format!("{name}_");
    }
    if KEYWORDS.contains(&name) {
        return format!("r#{name}");
    }
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// Whether serde needs a `rename` for a field written as `ident`.
fn needs_rename(name: &str, ident: &str) -> bool {
    ident.strip_prefix("r#").unwrap_or(ident) != name
}

/// `blog_post` -> `BlogPost`.
fn pascal(name: &str) -> String {
    let mut out: String = name
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut cs = p.chars();
            cs.next()
                .map(|c| c.to_ascii_uppercase().to_string() + cs.as_str())
                .unwrap_or_default()
        })
        .collect();
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, 'T');
    }
    out
}

/// `doThing-now` -> `do_thing_now`, as a method name.
fn snake(name: &str) -> String {
    let mut words: Vec<String> = Vec::new();
    for part in name
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|p| !p.is_empty())
    {
        let mut word = String::new();
        let mut prev_lower = false;
        for c in part.chars() {
            if c.is_ascii_uppercase() && prev_lower && !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
            prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
            word.push(c.to_ascii_lowercase());
        }
        if !word.is_empty() {
            words.push(word);
        }
    }
    ident(&words.join("_"))
}

fn element_type(kind: &str) -> String {
    let t = kind.trim().to_lowercase();
    match t.as_str() {
        "string" | "uuid" => "String".into(),
        "int" => "i64".into(),
        "number" | "float" | "decimal" => "f64".into(),
        "bool" => "bool".into(),
        "datetime" => format!("{TY}::Datetime"),
        _ if t.starts_with("record") => "String".into(),
        _ => format!("{ST}::Value"),
    }
}

/// The model type of a field, without `Option`.
fn base_type(f: &ParsedField) -> String {
    if f.is_record {
        // A record id as its `table:id` string, the JSON shape rows carry.
        return "String".into();
    }
    if f.is_datetime {
        return format!("{TY}::Datetime");
    }
    let t = f.kind.to_lowercase();
    match t.as_str() {
        "string" | "uuid" => "String".into(),
        "int" => "i64".into(),
        "number" | "float" | "decimal" => "f64".into(),
        "bool" => "bool".into(),
        "object" => format!("{ST}::Object"),
        "array" | "set" => format!("Vec<{ST}::Value>"),
        _ => match ARRAY_TYPE.captures(&f.kind) {
            Some(c) => format!("Vec<{}>", element_type(&MAX_LEN_SUFFIX.replace(&c[1], ""))),
            None => format!("{ST}::Value"),
        },
    }
}

/// The field-token type of a filterable field (none for object/array/any).
fn token_type(f: &ParsedField) -> Option<&'static str> {
    if f.is_record {
        return Some("RecordField");
    }
    if f.is_datetime {
        return Some("DateTimeField");
    }
    match f.kind.to_lowercase().as_str() {
        "string" | "uuid" => Some("StringField"),
        "int" | "number" | "float" | "decimal" => Some("NumField"),
        "bool" => Some("BoolField"),
        _ => None,
    }
}

/// The model fields: everything but `id` and nested sub-fields.
fn model_fields(table: &ParsedTable) -> impl Iterator<Item = &ParsedField> {
    table
        .fields
        .iter()
        .filter(|f| f.name != "id" && !f.is_nested())
}

fn field_docs(f: &ParsedField, indent: &str) -> String {
    let mut docs = Vec::new();
    if let Some(kind) = &f.crdt {
        let kind = if kind.is_empty() {
            String::new()
        } else {
            format!(" (`{kind}`)")
        };
        docs.push(format!(
            "CRDT{kind}: edited collaboratively; this is the server-written text mirror of the document."
        ));
    }
    if f.cursor {
        docs.push("Shares cursor presence.".into());
    }
    if f.parent {
        docs.push("Parent link: the row this one belongs to.".into());
    }
    if f.ephemeral {
        docs.push("Ephemeral: travels between channel peers only and is never stored.".into());
    }
    if f.opaque {
        docs.push("Opaque: synced and readable, but cannot be filtered or ordered on.".into());
    }
    docs.iter().map(|d| format!("{indent}/// {d}\n")).collect()
}

fn column_literal(f: &ParsedField) -> String {
    let mut flags = Vec::new();
    if f.is_record {
        flags.push("record_id: true");
    }
    if f.is_datetime {
        flags.push("date_time: true");
    }
    if f.optional {
        flags.push("optional: true");
    }
    // `@ephemeral`: travels only over peer to peer channels, never to the
    // database. The client keeps it out of writes and the query builder rejects
    // it in where/orderBy.
    if f.ephemeral {
        flags.push("ephemeral: true");
    }
    if flags.is_empty() {
        return format!("ColumnSchema::of_type({})", lit(&f.kind));
    }
    format!(
        "ColumnSchema {{ kind: Some({}.to_string()), {}, ..Default::default() }}",
        lit(&f.kind),
        flags.join(", ")
    )
}

fn string_columns(names: &[String]) -> String {
    if names.is_empty() {
        return "BTreeMap::new()".into();
    }
    let entries: Vec<String> = names
        .iter()
        .map(|n| {
            format!(
                "({}.to_string(), ColumnSchema::of_type(\"string\"))",
                lit(n)
            )
        })
        .collect();
    format!("BTreeMap::from([{}])", entries.join(", "))
}

fn emit_schema_fn(
    out: &mut String,
    tables: &[ParsedTable],
    accesses: &[super::ParsedAccess],
    backends: &[OpenApiBackend],
) {
    out.push_str("/// The runtime schema: tables, relationships, access methods and backends.\n");
    let _ = writeln!(out, "pub fn schema() -> {ST}::Schema {{");
    let _ = writeln!(out, "    #[allow(unused_imports)]");
    let _ = writeln!(
        out,
        "    use {ST}::{{AccessDef, AccessMethod, BackendDef, BackendRoute, Cardinality, ColumnSchema, RouteArg, Schema, SchemaRelationship, TableSchema}};"
    );
    let _ = writeln!(out, "    #[allow(unused_imports)]");
    let _ = writeln!(out, "    use ::std::collections::BTreeMap;");
    let _ = writeln!(out, "    #[allow(unused_mut)]");
    let _ = writeln!(out, "    let mut schema = Schema::default();");
    for t in tables {
        let columns = if t.fields.is_empty() {
            "BTreeMap::new()".to_string()
        } else {
            let entries: Vec<String> = t
                .fields
                .iter()
                .map(|f| {
                    format!(
                        "                ({}.to_string(), {}),\n",
                        lit(&f.name),
                        column_literal(f)
                    )
                })
                .collect();
            format!("BTreeMap::from([\n{}            ])", entries.concat())
        };
        let _ = writeln!(
            out,
            "    schema.tables.insert(\n        {}.to_string(),\n        TableSchema {{\n            columns: {columns},\n            channel: {},\n        }},\n    );",
            lit(&t.name),
            t.channel
        );
    }
    let relationships = derive_relationships(tables);
    if !relationships.is_empty() {
        let _ = writeln!(
            out,
            "    let rel = |from: &str, field: &str, to: &str, cardinality: Cardinality| SchemaRelationship {{\n        from: from.to_string(),\n        field: field.to_string(),\n        to: to.to_string(),\n        cardinality,\n    }};"
        );
        let _ = writeln!(out, "    schema.relationships = vec![");
        for r in &relationships {
            let card = match r.cardinality {
                Cardinality::One => "One",
                Cardinality::Many => "Many",
            };
            let _ = writeln!(
                out,
                "        rel({}, {}, {}, Cardinality::{card}),",
                lit(&r.from),
                lit(&r.field),
                lit(&r.to)
            );
        }
        let _ = writeln!(out, "    ];");
    }
    for a in accesses {
        // Keys match `AuthService` method names: `signIn` / `signup`.
        let _ = writeln!(
            out,
            "    schema.access.insert(\n        {}.to_string(),\n        AccessDef {{\n            signin: AccessMethod {{ params: {} }},\n            signup: AccessMethod {{ params: {} }},\n        }},\n    );",
            lit(&a.name),
            string_columns(&a.signin_params),
            string_columns(&a.signup_params)
        );
    }
    for b in backends {
        let routes: Vec<String> = b
            .routes
            .iter()
            .map(|r| {
                let args: Vec<String> = r
                    .args
                    .iter()
                    .map(|a| {
                        let kind = a
                            .kind
                            .json_type()
                            .map(|t| format!("Some({}.to_string())", lit(t)))
                            .unwrap_or_else(|| "None".into());
                        format!(
                            "({}.to_string(), RouteArg {{ kind: {kind}, optional: {} }})",
                            lit(&a.name),
                            a.optional
                        )
                    })
                    .collect();
                let args = if args.is_empty() {
                    "BTreeMap::new()".to_string()
                } else {
                    format!("BTreeMap::from([{}])", args.join(", "))
                };
                format!(
                    "                ({}.to_string(), BackendRoute {{ args: {args} }}),\n",
                    lit(&r.path)
                )
            })
            .collect();
        let routes = if routes.is_empty() {
            "BTreeMap::new()".to_string()
        } else {
            format!("BTreeMap::from([\n{}            ])", routes.concat())
        };
        let _ = writeln!(
            out,
            "    schema.backends.insert(\n        {}.to_string(),\n        BackendDef {{\n            outbox_table: {}.to_string(),\n            routes: {routes},\n        }},\n    );",
            lit(&b.name),
            lit(b.outbox_table.as_deref().unwrap_or(""))
        );
    }
    out.push_str("    schema\n}\n\n");
}

/// A raw string literal holding `s`, with enough `#`s.
fn raw_string(s: &str) -> String {
    let mut hashes = 1;
    while s.contains(&format!("\"{}", "#".repeat(hashes))) {
        hashes += 1;
    }
    let h = "#".repeat(hashes);
    format!("r{h}\"{s}\"{h}")
}

fn serde_attrs(derives: &str) -> String {
    format!("#[derive({derives}, {TY}::__serde::Serialize, {TY}::__serde::Deserialize)]\n#[serde(crate = \"{TY}::__serde\")]\n")
}

fn emit_model(out: &mut String, table: &ParsedTable, name: &str) {
    let fields: Vec<&ParsedField> = model_fields(table).collect();
    let snake_violation = fields
        .iter()
        .any(|f| f.name.chars().any(|c| c.is_ascii_uppercase()));
    let _ = writeln!(out, "/// A `{}` row.", table.name);
    out.push_str(&serde_attrs("Clone, Debug, PartialEq"));
    if snake_violation {
        out.push_str("#[allow(non_snake_case)]\n");
    }
    let _ = writeln!(out, "pub struct {name} {{");
    out.push_str("    /// The record id, `table:key`.\n    pub id: String,\n");
    for f in &fields {
        out.push_str(&field_docs(f, "    "));
        let id = ident(&f.name);
        let base = base_type(f);
        let mut attrs: Vec<String> = Vec::new();
        if needs_rename(&f.name, &id) {
            attrs.push(format!("rename = {}", lit(&f.name)));
        }
        let ty = if f.optional {
            attrs.push("default, skip_serializing_if = \"Option::is_none\"".into());
            format!("Option<{base}>")
        } else {
            if base.ends_with("::Value") {
                // A dynamic value may be absent from the row.
                attrs.push("default".into());
            }
            base
        };
        if !attrs.is_empty() {
            let _ = writeln!(out, "    #[serde({})]", attrs.join(", "));
        }
        let _ = writeln!(out, "    pub {id}: {ty},");
    }
    out.push_str("}\n\n");
}

fn emit_patch(out: &mut String, table: &ParsedTable, name: &str) {
    let fields: Vec<&ParsedField> = model_fields(table).collect();
    let snake_violation = fields
        .iter()
        .any(|f| f.name.chars().any(|c| c.is_ascii_uppercase()));
    let _ = writeln!(
        out,
        "/// A partial update of a `{}` row: only the fields that are set are written.",
        table.name
    );
    out.push_str(&serde_attrs("Clone, Debug, Default, PartialEq"));
    if snake_violation {
        out.push_str("#[allow(non_snake_case)]\n");
    }
    let _ = writeln!(out, "pub struct {name}Patch {{");
    for f in &fields {
        let id = ident(&f.name);
        let rename = if needs_rename(&f.name, &id) {
            format!("rename = {}, ", lit(&f.name))
        } else {
            String::new()
        };
        let _ = writeln!(
            out,
            "    #[serde({rename}default, skip_serializing_if = \"Option::is_none\")]\n    pub {id}: Option<{}>,",
            base_type(f)
        );
    }
    out.push_str("}\n\n");
}

fn emit_table_impls(out: &mut String, table: &ParsedTable, name: &str) {
    let _ = writeln!(
        out,
        "impl {TY}::Table for {name} {{\n    const TABLE: &'static str = {};\n    type Patch = {name}Patch;\n\n    fn record_id(&self) -> &str {{\n        &self.id\n    }}\n}}\n",
        lit(&table.name)
    );
    let _ = writeln!(
        out,
        "/// Typed field tokens for `{}`: `{name}::<field>.eq(..)`.",
        table.name
    );
    out.push_str("#[allow(non_upper_case_globals)]\n");
    let _ = writeln!(out, "impl {name} {{");
    let _ = writeln!(
        out,
        "    pub const id: {TY}::StringField = {TY}::StringField::new(\"id\");"
    );
    for f in model_fields(table) {
        if let Some(token) = token_type(f) {
            let _ = writeln!(
                out,
                "    pub const {}: {TY}::{token} = {TY}::{token}::new({});",
                ident(&f.name),
                lit(&f.name)
            );
        }
    }
    out.push_str("}\n\n");
}

fn arg_type(kind: ArgKind) -> String {
    match kind {
        ArgKind::String => "&str".into(),
        ArgKind::Integer => "i64".into(),
        ArgKind::Number => "f64".into(),
        ArgKind::Boolean => "bool".into(),
        ArgKind::Any => format!("{ST}::Value"),
    }
}

fn emit_auth_api(out: &mut String, accesses: &[super::ParsedAccess]) {
    out.push_str("/// Typed sign-in and sign-up per `DEFINE ACCESS` (params are strings).\n");
    let _ = writeln!(out, "pub struct AuthApi<'a, C: ?Sized>(pub &'a C);\n");
    let _ = writeln!(out, "impl<C: {TY}::TypedClient + ?Sized> AuthApi<'_, C> {{");
    for (i, a) in accesses.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        for (method, call, params) in [
            ("sign_in", "sign_in", &a.signin_params),
            ("sign_up", "sign_up", &a.signup_params),
        ] {
            let args: String = params
                .iter()
                .map(|p| format!(", {}: &str", ident(p)))
                .collect();
            let entries: Vec<String> = params
                .iter()
                .map(|p| format!("({}.to_string(), {ST}::Value::from({}))", lit(p), ident(p)))
                .collect();
            let map = if entries.is_empty() {
                format!("{ST}::Object::new()")
            } else {
                format!("{ST}::Object::from([{}])", entries.join(", "))
            };
            let _ = writeln!(out, "    /// `{call}` through `DEFINE ACCESS {}`.", a.name);
            out.push_str("    #[allow(clippy::too_many_arguments)]\n");
            let _ = writeln!(
                out,
                "    pub async fn {}(&self{args}) -> {ST}::Result<()> {{\n        self.0.{call}({}, {map}).await\n    }}",
                snake(&format!("{method}_{}", a.name)),
                lit(&a.name)
            );
        }
    }
    out.push_str("}\n\n");
}

fn emit_backends(out: &mut String, backends: &[OpenApiBackend]) {
    out.push_str("/// Typed backend jobs: `db.run().<backend>().<route>(..)`.\n");
    let _ = writeln!(out, "pub struct Backends<'a, C: ?Sized>(pub &'a C);\n");
    let _ = writeln!(
        out,
        "impl<'a, C: {TY}::TypedClient + ?Sized> Backends<'a, C> {{"
    );
    for b in backends {
        let _ = writeln!(
            out,
            "    pub fn {}(&self) -> {}Backend<'a, C> {{\n        {}Backend(self.0)\n    }}",
            snake(&b.name),
            pascal(&b.name),
            pascal(&b.name)
        );
    }
    out.push_str("}\n\n");
    for b in backends {
        let name = format!("{}Backend", pascal(&b.name));
        let _ = writeln!(out, "/// The routes of backend `{}`.", b.name);
        let _ = writeln!(out, "pub struct {name}<'a, C: ?Sized>(pub &'a C);\n");
        let _ = writeln!(out, "impl<C: {TY}::TypedClient + ?Sized> {name}<'_, C> {{");
        for (i, r) in b.routes.iter().enumerate() {
            if i > 0 {
                out.push('\n');
            }
            // Required args first, so the call reads in the order that matters.
            let mut args: Vec<_> = r.args.iter().collect();
            args.sort_by_key(|a| a.optional);
            let params: String = args
                .iter()
                .map(|a| {
                    let ty = arg_type(a.kind);
                    let ty = if a.optional {
                        format!("Option<{ty}>")
                    } else {
                        ty
                    };
                    format!(", {}: {ty}", ident(&a.name))
                })
                .collect();
            let mut body = String::new();
            let payload = if r.args.is_empty() {
                format!("{ST}::Object::new()")
            } else {
                let _ = writeln!(body, "        let mut payload = {ST}::Object::new();");
                for a in &args {
                    let id = ident(&a.name);
                    // An untyped arg is a `Value` already.
                    let value = |v: &str| match a.kind {
                        ArgKind::Any => v.to_string(),
                        _ => format!("{ST}::Value::from({v})"),
                    };
                    if a.optional {
                        let _ = writeln!(
                            body,
                            "        if let Some(v) = {id} {{\n            payload.insert({}.to_string(), {});\n        }}",
                            lit(&a.name),
                            value("v")
                        );
                    } else {
                        let _ = writeln!(
                            body,
                            "        payload.insert({}.to_string(), {});",
                            lit(&a.name),
                            value(&id)
                        );
                    }
                }
                "payload".to_string()
            };
            let method = match snake(&r.path) {
                m if m == "_" || m.is_empty() => "call".to_string(),
                m => m,
            };
            let _ = writeln!(out, "    /// `POST {}`.", r.path);
            out.push_str("    #[allow(clippy::too_many_arguments)]\n");
            let _ = writeln!(
                out,
                "    pub async fn {method}(&self{params}) -> {ST}::Result<()> {{\n{body}        self.0\n            .run({}, {}, {payload}, {ST}::RunOptions::default())\n            .await\n    }}",
                lit(&b.name),
                lit(&r.path)
            );
        }
        out.push_str("}\n\n");
    }
}

fn emit_app_db(
    out: &mut String,
    tables: &[(String, &ParsedTable)],
    has_auth: bool,
    has_backends: bool,
) {
    out.push_str("/// The typed facade over a client (Dart `AppDb`).\n");
    out.push_str("pub struct AppDb<C> {\n    pub client: C,\n}\n\n");
    let _ = writeln!(out, "impl<C: {TY}::TypedClient> AppDb<C> {{");
    out.push_str("    pub fn new(client: C) -> Self {\n        AppDb { client }\n    }\n");
    let reserved = ["new", "client", "run", "auth"];
    for (name, t) in tables {
        let mut method = snake(&t.name);
        if reserved.contains(&method.as_str()) {
            method.push_str("_table");
        }
        let _ = writeln!(
            out,
            "\n    pub fn {method}(&self) -> {TY}::Collection<'_, {name}, C> {{\n        {TY}::Collection::new(&self.client)\n    }}"
        );
    }
    if has_backends {
        out.push_str(
            "\n    pub fn run(&self) -> Backends<'_, C> {\n        Backends(&self.client)\n    }\n",
        );
    }
    if has_auth {
        out.push_str(
            "\n    pub fn auth(&self) -> AuthApi<'_, C> {\n        AuthApi(&self.client)\n    }\n",
        );
    }
    out.push_str("}\n");
}

/// Generate the Rust source for a SurrealQL schema and its backends: the
/// runtime `schema()`, `SURQL_SCHEMA`, `config()`, a serde model, a `Patch`
/// and field tokens per table, typed `AuthApi` / `Backends`, and the `AppDb`
/// facade. Fails on a schema the CLI would reject (an `-- @ephemeral` field
/// off a `-- @channel` table) and on two names mapping to one Rust type.
pub fn generate_rust(surql: &str, backends: &[OpenApiBackend]) -> Result<String> {
    let parsed = parse_project(surql)?;
    let tables: Vec<(String, &ParsedTable)> =
        parsed.tables.iter().map(|t| (pascal(&t.name), t)).collect();

    let mut type_names = BTreeSet::from([
        "AppDb".to_string(),
        "AuthApi".to_string(),
        "Backends".to_string(),
    ]);
    let generated = tables
        .iter()
        .flat_map(|(n, _)| [n.clone(), format!("{n}Patch")])
        .chain(
            backends
                .iter()
                .map(|b| format!("{}Backend", pascal(&b.name))),
        );
    for name in generated {
        if !type_names.insert(name.clone()) {
            return Err(Error::InvalidArgument(format!(
                "codegen: two tables or backends map to the Rust type `{name}`"
            )));
        }
    }

    let mut out = String::new();
    out.push_str("// GENERATED CODE - DO NOT EDIT BY HAND.\n");
    out.push_str("// Generated from a SurrealQL schema by spooky_core::codegen.\n\n");
    emit_schema_fn(&mut out, &parsed.tables, &parsed.accesses, backends);
    // The embedded schema defines the CLIENT's local cache tables, so a
    // server-only field must not appear in it: the local table would carry a
    // column the client never receives a value for.
    out.push_str(
        "/// The schema's SurrealQL with server-only fields stripped: the local cache DDL.\n",
    );
    let _ = writeln!(
        out,
        "pub const SURQL_SCHEMA: &str = {};\n",
        raw_string(&strip_server_only_fields(surql))
    );
    let _ = writeln!(
        out,
        "/// A client config for this schema.\npub fn config(database: {ST}::DatabaseConfig) -> {ST}::Sp00kyConfig {{\n    {ST}::Sp00kyConfig::new(database, schema(), SURQL_SCHEMA)\n}}\n"
    );
    for (name, t) in &tables {
        emit_model(&mut out, t, name);
        emit_patch(&mut out, t, name);
        emit_table_impls(&mut out, t, name);
    }
    if !parsed.accesses.is_empty() {
        emit_auth_api(&mut out, &parsed.accesses);
    }
    if !backends.is_empty() {
        emit_backends(&mut out, backends);
    }
    emit_app_db(
        &mut out,
        &tables,
        !parsed.accesses.is_empty(),
        !backends.is_empty(),
    );
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::super::parse_open_api;
    use super::*;

    const SURQL: &str = "
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE FIELD username ON user TYPE string ASSERT $value != NONE;
DEFINE FIELD created_at ON user TYPE option<datetime> DEFAULT time::now();
DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD title ON thread TYPE string;
DEFINE FIELD author ON TABLE thread TYPE record<user>;
DEFINE FIELD score ON thread TYPE int;
DEFINE FIELD tags ON thread TYPE array<string>;
";

    const TYPED_SCHEMA: &str = "
DEFINE ACCESS account ON DATABASE TYPE RECORD
  SIGNUP ( CREATE user SET email = $email, password = $password )
  SIGNIN ( SELECT * FROM user WHERE email = $email );
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD email ON user TYPE string;
DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD title ON thread TYPE string;
DEFINE FIELD author ON thread TYPE record<user>;
DEFINE FIELD score ON thread TYPE int;
DEFINE FIELD created_at ON thread TYPE option<datetime>;
";

    const OPENAPI: &str = "
openapi: 3.1.0
paths:
  /spookify:
    post:
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                id: { type: string }
              required: [id]
";

    /// Parses as a Rust file (the integration tests also compile a golden).
    fn parse(src: &str) -> syn::File {
        syn::parse_file(src).unwrap_or_else(|e| panic!("generated code does not parse: {e}\n{src}"))
    }

    fn generated(surql: &str) -> String {
        let src = generate_rust(surql, &[]).unwrap();
        parse(&src);
        src
    }

    // ---- Dart `codegen_test.dart` ----------------------------------------------------

    #[test]
    fn the_schema_fn_carries_forward_and_reverse_relationships() {
        let src = generated(SURQL);
        assert!(src.contains("schema.relationships = vec!["));
        assert!(src.contains(r#"rel("thread", "author", "user", Cardinality::One),"#));
        assert!(
            src.contains(r#"rel("user", "threads", "thread", Cardinality::Many),"#),
            "the reverse name is the pluralized source table"
        );
    }

    #[test]
    fn a_schema_with_no_record_fields_emits_no_relationships() {
        let src =
            generated("DEFINE TABLE thread SCHEMAFULL;\nDEFINE FIELD title ON thread TYPE string;");
        assert!(!src.contains("schema.relationships"));
        assert!(!src.contains("let rel ="));
    }

    #[test]
    fn the_schema_fn_produces_column_schemas() {
        let src = generated(SURQL);
        assert!(src.contains("schema.tables.insert(\n        \"user\".to_string(),"));
        assert!(src.contains(r#"("username".to_string(), ColumnSchema::of_type("string")),"#));
        assert!(src.contains(
            r#"("created_at".to_string(), ColumnSchema { kind: Some("datetime".to_string()), date_time: true, optional: true, ..Default::default() }),"#
        ));
        assert!(src.contains(
            r#"("author".to_string(), ColumnSchema { kind: Some("record".to_string()), record_id: true, ..Default::default() }),"#
        ));
    }

    #[test]
    fn emits_a_pascal_case_model_per_table_with_typed_fields() {
        let src = generated(SURQL);
        assert!(src.contains("pub struct User {"));
        assert!(src.contains("pub struct Thread {"));
        assert!(src.contains("    pub username: String,"));
        assert!(src.contains(
            "    #[serde(default, skip_serializing_if = \"Option::is_none\")]\n    pub created_at: Option<::spooky_core::typed::Datetime>,"
        ));
        assert!(
            src.contains("    pub author: String,"),
            "a record id as its string"
        );
        assert!(src.contains("    pub score: i64,"));
        assert!(src.contains("    pub tags: Vec<String>,"));
        assert!(src.contains("impl ::spooky_core::typed::Table for Thread {"));
    }

    #[test]
    fn the_surql_is_embedded() {
        let src = generated(SURQL);
        assert!(src.contains("pub const SURQL_SCHEMA: &str = r#\"\nDEFINE TABLE user"));
        assert!(src.contains("pub fn config(database: ::spooky_core::DatabaseConfig)"));
        assert_eq!(raw_string("a\"#b"), "r##\"a\"#b\"##");
    }

    // ---- Dart `codegen_typed_test.dart` ------------------------------------------------

    fn typed_src() -> String {
        let backends = [parse_open_api("api", OPENAPI, None).unwrap()];
        let src = generate_rust(TYPED_SCHEMA, &backends).unwrap();
        parse(&src);
        src
    }

    #[test]
    fn field_tokens_with_the_right_kind_per_field() {
        let src = typed_src();
        assert!(src.contains("impl Thread {"));
        for token in [
            r#"pub const id: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("id");"#,
            r#"pub const title: ::spooky_core::typed::StringField = ::spooky_core::typed::StringField::new("title");"#,
            r#"pub const author: ::spooky_core::typed::RecordField = ::spooky_core::typed::RecordField::new("author");"#,
            r#"pub const score: ::spooky_core::typed::NumField = ::spooky_core::typed::NumField::new("score");"#,
            r#"pub const created_at: ::spooky_core::typed::DateTimeField = ::spooky_core::typed::DateTimeField::new("created_at");"#,
        ] {
            assert!(src.contains(token), "missing {token}");
        }
    }

    #[test]
    fn an_all_optional_patch_that_omits_unset_fields() {
        let src = typed_src();
        assert!(src.contains("pub struct ThreadPatch {"));
        assert!(src.contains(
            "    #[serde(default, skip_serializing_if = \"Option::is_none\")]\n    pub title: Option<String>,"
        ));
    }

    #[test]
    fn typed_auth_methods_per_access() {
        let src = typed_src();
        assert!(src.contains("pub struct AuthApi<'a, C: ?Sized>(pub &'a C);"));
        // SIGNIN references only $email; SIGNUP references $email + $password.
        assert!(src.contains(
            "pub async fn sign_in_account(&self, email: &str) -> ::spooky_core::Result<()> {"
        ));
        assert!(src.contains(
            "pub async fn sign_up_account(&self, email: &str, password: &str) -> ::spooky_core::Result<()> {"
        ));
        assert!(src.contains(r#"self.0.sign_in("account", ::spooky_core::Object::from([("email".to_string(), ::spooky_core::Value::from(email))])).await"#));
        assert!(src.contains(
            r#"schema.access.insert(
        "account".to_string(),"#
        ));
    }

    #[test]
    fn typed_backends_with_required_arg_routes() {
        let src = typed_src();
        assert!(src.contains("pub struct Backends<'a, C: ?Sized>(pub &'a C);"));
        assert!(src.contains("pub fn api(&self) -> ApiBackend<'a, C> {"));
        assert!(src.contains("pub struct ApiBackend<'a, C: ?Sized>(pub &'a C);"));
        assert!(
            src.contains("pub async fn spookify(&self, id: &str) -> ::spooky_core::Result<()> {")
        );
        assert!(src.contains(
            r#".run("api", "/spookify", payload, ::spooky_core::RunOptions::default())"#
        ));
        assert!(src.contains(r#"("/spookify".to_string(), BackendRoute { args: BTreeMap::from([("id".to_string(), RouteArg { kind: Some("string".to_string()), optional: false })]) }),"#));
    }

    #[test]
    fn the_app_db_facade_wires_tables_run_and_auth() {
        let src = typed_src();
        assert!(src.contains("pub struct AppDb<C> {"));
        assert!(src
            .contains("pub fn thread(&self) -> ::spooky_core::typed::Collection<'_, Thread, C> {"));
        assert!(src.contains("pub fn run(&self) -> Backends<'_, C> {"));
        assert!(src.contains("pub fn auth(&self) -> AuthApi<'_, C> {"));
    }

    // ---- names and edge cases ------------------------------------------------------------

    #[test]
    fn names_are_made_valid_rust() {
        assert_eq!(ident("type"), "r#type");
        assert_eq!(ident("self"), "self_");
        assert_eq!(ident("9lives"), "_9lives");
        assert_eq!(ident("a-b"), "a_b");
        assert_eq!(pascal("blog_post"), "BlogPost");
        assert_eq!(pascal("_00_x"), "T00X");
        assert_eq!(snake("/do-thing/now"), "do_thing_now");
        assert_eq!(snake("doThing"), "do_thing");
        assert_eq!(snake("/v2/runJob"), "v2_run_job");
        assert_eq!(snake("type"), "r#type");
    }

    #[test]
    fn keyword_nested_and_dynamic_fields() {
        let src = generated(
            "DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD type ON item TYPE string;
DEFINE FIELD self ON item TYPE option<int>;
DEFINE FIELD meta ON item TYPE object;
DEFINE FIELD meta.color ON item TYPE string;
DEFINE FIELD blob ON item TYPE bytes;
DEFINE FIELD sizes ON item TYPE array<float, 3>;
DEFINE FIELD camelCase ON item TYPE bool;
DEFINE FIELD run ON item TYPE string;",
        );
        assert!(src.contains("    pub r#type: String,"));
        assert!(src.contains("    #[serde(rename = \"self\", default, skip_serializing_if = \"Option::is_none\")]\n    pub self_: Option<i64>,"));
        assert!(src.contains("    pub meta: ::spooky_core::Object,"));
        assert!(
            !src.contains("pub meta_color"),
            "a nested field lives inside its parent"
        );
        assert!(src.contains(r#"("meta.color".to_string(), ColumnSchema::of_type("string")),"#));
        assert!(src.contains("    #[serde(default)]\n    pub blob: ::spooky_core::Value,"));
        assert!(src.contains("    pub sizes: Vec<f64>,"));
        assert!(src.contains("#[allow(non_snake_case)]\npub struct Item {"));
        assert!(src.contains(r#"pub const r#type: ::spooky_core::typed::StringField"#));
    }

    #[test]
    fn colliding_type_names_are_an_error() {
        let err = generate_rust(
            "DEFINE TABLE thread SCHEMAFULL;\nDEFINE TABLE thread_patch SCHEMAFULL;",
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("ThreadPatch"), "{err}");
        assert!(generate_rust(
            "DEFINE TABLE call SCHEMAFULL;\n-- @ephemeral\nDEFINE FIELD p ON call TYPE option<string>;",
            &[]
        )
        .is_err());
    }

    #[test]
    fn channel_ephemeral_and_crdt_fields_are_documented() {
        let src = generated(
            "-- @channel
DEFINE TABLE call SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD view_id ON TABLE call TYPE option<string>;
-- @ephemeral
DEFINE FIELD view_pos ON TABLE call TYPE option<string>;
-- @crdt text
DEFINE FIELD notes ON TABLE call TYPE string;",
        );
        assert!(src.contains("channel: true,"));
        assert!(src.contains(r#"ColumnSchema { kind: Some("string".to_string()), optional: true, ephemeral: true, ..Default::default() }"#));
        assert!(src.contains("/// Ephemeral: travels between channel peers only"));
        assert!(src.contains("/// CRDT (`text`)"));
    }
}
