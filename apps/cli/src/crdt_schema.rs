//! Schema expansion for collaborative (`-- @crdt <kind>`) fields.
//!
//! A CRDT field is edited as a Loro document, but the document never lives in
//! the field. Every client appends its own edits as small update blobs to a
//! per-parent log table, everyone receives the log through the normal view
//! pipeline, and the SSP is the only writer of the canonical state: it folds
//! the log into `_00_crdt_doc` and writes the field itself as plain text.
//!
//! For a parent table `P` with `@crdt` fields this module:
//!
//! - rewrites each annotated `DEFINE FIELD` into a server-written text mirror
//!   (`TYPE string`, a VALUE guard so a record user can never set it, which
//!   keeps the mirror equal to what the fold computed);
//! - adds `<field>__crdt`, the fold's version stamp (`{ rev, upto, state_hash }`)
//!   that reaches clients through ordinary parent sync;
//! - generates `P__crdt_log` (append-only: CREATE by editors, no UPDATE or
//!   DELETE) and, for `@cursor` fields, `P__crdt_presence`;
//! - generates `_00_crdt_doc` (server only, never synced, read one-shot) and
//!   `_00_crdt_peer` (which user owns which Loro peer id);
//! - generates a cleanup event that drops a parent's canonical docs with it.
//!
//! Permissions on the generated tables are the parent's own rules read through
//! the `doc` record link (`author.id = $auth.id` becomes
//! `doc.author.id = $auth.id`), so who may read or write a document's edits is
//! always exactly who may read or update the record.

use anyhow::{bail, Context, Result};
use regex::Regex;
use std::collections::BTreeMap;
use surrealdb_core::sql::{Expression, Function, Ident, Idiom, Part, Subquery, Value};

use crate::annotations::{self, FieldAnnotation};
use crate::parser::{FieldType, SchemaParser};

/// Suffix of the generated append-only edit log of a parent table.
pub const LOG_SUFFIX: &str = "__crdt_log";
/// Suffix of the generated presence (cursor) table of a parent table.
pub const PRESENCE_SUFFIX: &str = "__crdt_presence";
/// Suffix of the per-field fold stamp on the parent table.
pub const META_SUFFIX: &str = "__crdt";
/// The server-only table holding each document's canonical snapshot.
pub const DOC_TABLE: &str = "_00_crdt_doc";
/// Loro peer ids and the user who claimed each. A log row may only carry a
/// peer its author owns, so nobody can author edits under someone else's id.
pub const PEER_TABLE: &str = "_00_crdt_peer";
/// The record link from a generated row to its parent document.
pub const DOC_LINK: &str = "doc";
/// Largest single update blob a client may append.
pub const MAX_DELTA_BYTES: usize = 256 * 1024;
/// Largest presence blob (one session's cursor/awareness state).
pub const MAX_PRESENCE_BYTES: usize = 16 * 1024;
/// Access method of an impersonation session; it may not write edits.
const IMPERSONATE_ACCESS: &str = "_00_impersonate";

/// A table this module generates (edit log or presence) rather than one the
/// author defined.
pub fn is_generated_table(name: &str) -> bool {
    name.ends_with(LOG_SUFFIX) || name.ends_with(PRESENCE_SUFFIX)
}

/// How a field's Loro document is shaped, which decides how the fold renders
/// its text mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrdtKind {
    /// A `LoroText` named after the field.
    Text,
    /// A loro-prosemirror tree (root map `"doc"`); the mirror is its plain text.
    Prosemirror,
}

impl CrdtKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CrdtKind::Text => "text",
            CrdtKind::Prosemirror => "prosemirror",
        }
    }

    fn parse(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("text") {
            "text" => Some(CrdtKind::Text),
            "prosemirror" => Some(CrdtKind::Prosemirror),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CrdtField {
    pub name: String,
    pub kind: CrdtKind,
    pub cursor: bool,
}

#[derive(Debug, Clone)]
pub struct CrdtParent {
    pub table: String,
    pub fields: Vec<CrdtField>,
    /// The parent's SELECT rule rebased onto the `doc` link.
    pub select: String,
    /// The parent's UPDATE rule rebased onto the `doc` link.
    pub update: String,
}

impl CrdtParent {
    pub fn log_table(&self) -> String {
        format!("{}{LOG_SUFFIX}", self.table)
    }

    pub fn presence_table(&self) -> String {
        format!("{}{PRESENCE_SUFFIX}", self.table)
    }

    fn has_cursor(&self) -> bool {
        self.fields.iter().any(|f| f.cursor)
    }
}

/// Which schema the expansion is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// SurrealDB: guards, server stamps, derived permissions, `_00_crdt_doc`.
    Server,
    /// The client's local cache: plain columns, no `_00_crdt_doc`. The local
    /// store only ever holds rows the server already let through.
    Client,
}

/// The result of expanding a user schema.
#[derive(Debug, Clone, Default)]
pub struct Expansion {
    /// The user schema with every `@crdt` field rewritten into its text mirror.
    pub user: String,
    /// Generated tables, fields and events (all `OVERWRITE`, safe to re-apply).
    pub generated: String,
    /// The mirror field definitions alone, as `OVERWRITE` statements, so the
    /// internal-schema step can re-assert the guard after user migrations.
    pub mirror_defs: String,
    pub parents: Vec<CrdtParent>,
}

impl Expansion {
    /// User schema followed by everything generated, the content every schema
    /// pipeline parses (so generated tables get events, markers, changefeeds).
    pub fn source(&self) -> String {
        if self.generated.is_empty() {
            return self.user.clone();
        }
        format!("{}\n\n{}", self.user, self.generated)
    }
}

/// Expand every `@crdt` field in `source`. A schema without any comes back
/// unchanged with nothing generated.
pub fn expand(source: &str, target: Target) -> Result<Expansion> {
    let parents = collect_parents(source)?;
    if parents.is_empty() {
        return Ok(Expansion { user: source.to_string(), ..Default::default() });
    }
    let user = rewrite_mirror_fields(source, &parents)?;
    let mut generated = String::from(
        "-- ==================================================\n\
         -- Generated by sp00ky for `@crdt` fields. Do not edit.\n\
         -- ==================================================\n",
    );
    for parent in &parents {
        generated.push_str(&parent_ddl(parent, target));
    }
    if target == Target::Server {
        generated.push_str(&doc_table_ddl(&parents));
        generated.push_str(&peer_table_ddl());
    }
    let mirror_defs = parents
        .iter()
        .flat_map(|p| p.fields.iter().map(move |f| mirror_definition(&p.table, &f.name, "OVERWRITE ", None)))
        .collect::<Vec<_>>()
        .join("\n");
    Ok(Expansion { user, generated, mirror_defs, parents })
}

/// Refuse a deploy mode that cannot host the fold.
pub fn check_deploy_mode(parents: &[CrdtParent], mode: &crate::backend::DeployMode) -> Result<()> {
    if parents.is_empty() || *mode != crate::backend::DeployMode::Surrealism {
        return Ok(());
    }
    let fields: Vec<String> = parents
        .iter()
        .flat_map(|p| p.fields.iter().map(move |f| format!("{}.{}", p.table, f.name)))
        .collect();
    bail!(
        "`@crdt` fields ({}) need an SSP to fold their edits, and the `surrealism` deploy mode \
         runs none. Use the `singlenode` or `cluster` mode, or remove the annotation.",
        fields.join(", ")
    )
}

fn collect_parents(source: &str) -> Result<Vec<CrdtParent>> {
    let field_annotations = annotations::extract_field_annotations(source);
    let crdt: Vec<(&(String, String), &Vec<FieldAnnotation>)> = field_annotations
        .iter()
        .filter(|(_, anns)| annotations::has_annotation(anns, "crdt"))
        .collect();
    reject_reserved_names(source)?;
    if crdt.is_empty() {
        return Ok(Vec::new());
    }

    let mut parser = SchemaParser::new();
    parser
        .parse_file(source)
        .context("Failed to parse the schema for `@crdt` expansion")?;

    let mut problems = Vec::new();
    let mut by_table: BTreeMap<String, Vec<CrdtField>> = BTreeMap::new();
    for ((table, field), anns) in crdt {
        let at = format!("`{table}.{field}`");
        let value = anns.iter().find(|a| a.name == "crdt").and_then(|a| a.value.as_deref());
        let Some(kind) = CrdtKind::parse(value) else {
            problems.push(format!(
                "  - {at}: unknown CRDT kind `{}`; use `-- @crdt text` or `-- @crdt prosemirror`",
                value.unwrap_or_default()
            ));
            continue;
        };
        if field.contains('.') || field.contains('[') {
            problems.push(format!("  - {at}: `@crdt` is only supported on top-level fields"));
            continue;
        }
        let Some(def) = parser.tables.get(table).and_then(|t| t.fields.get(field)) else {
            problems.push(format!("  - {at}: annotated field is not defined"));
            continue;
        };
        let inner = match &def.field_type {
            FieldType::Option(inner) => inner.as_ref(),
            other => other,
        };
        if !matches!(inner, FieldType::String) {
            problems.push(format!(
                "  - {at}: a `@crdt` field holds the document's plain text, written by the \
                 server, so its TYPE must be `string` (found `{:?}`). The edits live in the \
                 generated `{table}{LOG_SUFFIX}` table.",
                def.field_type
            ));
            continue;
        }
        if def.value.is_some() || def.assert.is_some() {
            problems.push(format!(
                "  - {at}: remove the VALUE/ASSERT clause; the server owns this field's value"
            ));
            continue;
        }
        by_table.entry(table.clone()).or_default().push(CrdtField {
            name: field.clone(),
            kind,
            cursor: annotations::has_annotation(anns, "cursor"),
        });
    }
    if !problems.is_empty() {
        bail!("Schema validation failed for @crdt fields:\n{}", problems.join("\n"));
    }

    let mut parents = Vec::new();
    for (table, mut fields) in by_table {
        fields.sort_by(|a, b| a.name.cmp(&b.name));
        let schema = &parser.tables[&table];
        if schema.is_relation {
            bail!("`@crdt` fields are not supported on relation table `{table}`");
        }
        let rule = |action: &str| -> Result<String> {
            let raw = schema.table_permissions.get(action).map(String::as_str).unwrap_or("false");
            rebase_permission(raw, DOC_LINK)
                .with_context(|| format!("Failed to derive the `{action}` rule of `{table}` for its CRDT tables"))
        };
        parents.push(CrdtParent { select: rule("select")?, update: rule("update")?, table, fields });
    }
    Ok(parents)
}

/// Generated names must not collide with the author's own.
fn reject_reserved_names(source: &str) -> Result<()> {
    let table_re = Regex::new(r"(?i)DEFINE\s+TABLE\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?(\w+)").unwrap();
    let field_re =
        Regex::new(r"(?i)DEFINE\s+FIELD\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?([\w.]+)\s+ON").unwrap();
    for caps in table_re.captures_iter(source) {
        let name = &caps[1];
        if name.ends_with(LOG_SUFFIX) || name.ends_with(PRESENCE_SUFFIX) || name == DOC_TABLE {
            bail!("Table name `{name}` is reserved for generated CRDT tables");
        }
    }
    for caps in field_re.captures_iter(source) {
        if caps[1].ends_with(META_SUFFIX) {
            bail!("Field name `{}` is reserved: the `{META_SUFFIX}` suffix holds CRDT fold stamps", &caps[1]);
        }
    }
    Ok(())
}

// ------------------------------------------------------------------
// Permission rebasing
// ------------------------------------------------------------------

/// Read a parent table rule through a record link on a generated row.
///
/// Every row field the rule names moves under `link` (`published` becomes
/// `doc.published`, a bare `id` becomes `doc`), and `$parent` inside a
/// subquery, which names the row being checked, moves to the same link.
/// Fields inside a subquery belong to the subquery's own table and are left
/// alone. `true`/`false` pass through.
pub fn rebase_permission(rule: &str, link: &str) -> Result<String> {
    let trimmed = rule.trim();
    if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
        return Ok(trimmed.to_ascii_lowercase());
    }
    let value = surrealdb_core::syn::value(trimmed).with_context(|| format!("unparsable rule `{trimmed}`"))?;
    let rendered = rebase_value(value, link).to_string();
    Ok(rebase_parent_param(&rendered, link))
}

// The surrealdb-core AST structs are `#[non_exhaustive]`: their fields are
// public but they cannot be built or destructured here, so nodes are rebased
// in place.
fn rebase_value(mut value: Value, link: &str) -> Value {
    rebase_in_place(&mut value, link);
    value
}

fn rebase_in_place(value: &mut Value, link: &str) {
    match value {
        Value::Idiom(idiom) => rebase_idiom(idiom, link),
        Value::Expression(expr) => match expr.as_mut() {
            Expression::Unary { v, .. } => rebase_in_place(v, link),
            Expression::Binary { l, r, .. } => {
                rebase_in_place(l, link);
                rebase_in_place(r, link);
            }
            _ => {}
        },
        Value::Array(items) => items.0.iter_mut().for_each(|v| rebase_in_place(v, link)),
        Value::Object(map) => map.0.values_mut().for_each(|v| rebase_in_place(v, link)),
        Value::Function(func) => match func.as_mut() {
            Function::Normal(_, args) | Function::Custom(_, args) => {
                args.iter_mut().for_each(|v| rebase_in_place(v, link))
            }
            _ => {}
        },
        Value::Cast(cast) => rebase_in_place(&mut cast.1, link),
        // A parenthesized expression parses as a value subquery: same row.
        Value::Subquery(sub) => {
            if let Subquery::Value(inner) = sub.as_mut() {
                rebase_in_place(inner, link);
            }
        }
        // Statement subqueries scope bare fields to their own table; literals
        // and parameters do not name the row.
        _ => {}
    }
}

fn rebase_idiom(idiom: &mut Idiom, link: &str) {
    if !matches!(idiom.0.first(), Some(Part::Field(_))) {
        return;
    }
    let bare_id = idiom.0.len() == 1 && matches!(&idiom.0[0], Part::Field(f) if f.0 == "id");
    if bare_id {
        idiom.0.clear();
    }
    idiom.0.insert(0, Part::Field(Ident::from(link)));
}

fn rebase_parent_param(rendered: &str, link: &str) -> String {
    // `$parent.id` is the checked row's id: on a generated row that is the
    // parent record the link holds.
    const MARK: &str = "\u{0}PARENT_ID\u{0}";
    let id_re = Regex::new(r"\$parent\.id\b").unwrap();
    let out = id_re.replace_all(rendered, MARK);
    let out = out.replace("$parent.", &format!("$parent.{link}."));
    out.replace(MARK, &format!("$parent.{link}"))
}

// ------------------------------------------------------------------
// Rewriting the annotated fields
// ------------------------------------------------------------------

const MIRROR_TYPE: &str = "string";
/// A record user never sets the mirror: it keeps what the fold wrote. Root
/// (the SSP, a backend, an operator) has no `$auth` and writes freely.
const MIRROR_GUARD: &str = "DEFAULT '' VALUE IF $auth.id != NONE { $before OR '' } ELSE { $value }";

fn mirror_definition(table: &str, field: &str, define_mode: &str, permissions: Option<&str>) -> String {
    let perms = permissions.map(|p| format!(" {p}")).unwrap_or_default();
    format!("DEFINE FIELD {define_mode}{field} ON TABLE {table} TYPE {MIRROR_TYPE} {MIRROR_GUARD}{perms};")
}

/// Replace every annotated `DEFINE FIELD` statement with the mirror
/// definition, keeping its OVERWRITE/IF NOT EXISTS mode and PERMISSIONS.
fn rewrite_mirror_fields(source: &str, parents: &[CrdtParent]) -> Result<String> {
    let mut out = source.to_string();
    for parent in parents {
        for field in &parent.fields {
            let re = Regex::new(&format!(
                r"(?is)DEFINE\s+FIELD\s+(OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?{}\s+ON\s+(?:TABLE\s+)?{}\b([^;]*);",
                regex::escape(&field.name),
                regex::escape(&parent.table),
            ))?;
            let Some(caps) = re.captures(&out) else {
                bail!("Could not locate the DEFINE FIELD of `{}.{}`", parent.table, field.name);
            };
            let mode = caps.get(1).map(|m| m.as_str().split_whitespace().collect::<Vec<_>>().join(" ") + " ");
            let body = caps.get(2).map_or("", |m| m.as_str());
            let permissions = Regex::new(r"(?is)\bPERMISSIONS\b.*$")?
                .find(body)
                .map(|m| m.as_str().split_whitespace().collect::<Vec<_>>().join(" "));
            let replacement = mirror_definition(
                &parent.table,
                &field.name,
                mode.as_deref().unwrap_or(""),
                permissions.as_deref(),
            );
            let range = caps.get(0).expect("whole match").range();
            out.replace_range(range, &replacement);
        }
    }
    Ok(out)
}

// ------------------------------------------------------------------
// Generated tables
// ------------------------------------------------------------------

fn parent_ddl(parent: &CrdtParent, target: Target) -> String {
    let table = &parent.table;
    let log = parent.log_table();
    let fields: Vec<String> = parent.fields.iter().map(|f| format!("'{}'", f.name)).collect();
    let field_list = fields.join(", ");
    let kinds: Vec<String> = parent.fields.iter().map(|f| format!("{}={}", f.name, f.kind.as_str())).collect();
    let mut s = String::new();

    s.push_str(&format!("\n-- `{table}`: fold stamps\n"));
    for field in &parent.fields {
        let guard = match target {
            Target::Server => " VALUE IF $auth.id != NONE { $before } ELSE { $value }",
            Target::Client => "",
        };
        s.push_str(&format!(
            "DEFINE FIELD OVERWRITE {f}{META_SUFFIX} ON TABLE {table} TYPE option<object> FLEXIBLE{guard};\n",
            f = field.name
        ));
    }

    s.push_str(&format!("\n-- `{table}`: append-only Loro edit log\n"));
    match target {
        Target::Server => s.push_str(&format!(
            "DEFINE TABLE OVERWRITE {log} SCHEMAFULL COMMENT 'sp00ky:crdt_log {kinds}'\n\
             \x20 PERMISSIONS\n\
             \x20   FOR select WHERE {select}\n\
             \x20   FOR create WHERE $access != \"{IMPERSONATE_ACCESS}\" AND ({update})\n\
             \x20   FOR update, delete NONE;\n",
            kinds = kinds.join(","),
            select = parent.select,
            update = parent.update,
        )),
        Target::Client => s.push_str(&format!("DEFINE TABLE OVERWRITE {log} SCHEMAFULL PERMISSIONS FULL;\n")),
    }
    s.push_str(&field_lines(
        &log,
        target,
        &[
            ("doc", &format!("record<{table}>"), " READONLY"),
            ("field", "string", &format!(" ASSERT $value IN [{field_list}] READONLY")),
            ("author", "option<record>", " VALUE $before OR $auth.id OR $value READONLY"),
            ("peer", "string", &peer_assert()),
            ("@opaque data", "bytes", &format!(" ASSERT bytes::len($value) <= {MAX_DELTA_BYTES} READONLY")),
            ("size", "int", " VALUE bytes::len(data) READONLY"),
            ("created_at", "datetime", " VALUE $before OR time::now() READONLY"),
        ],
    ));
    s.push_str(&format!("DEFINE INDEX OVERWRITE {log}_doc ON TABLE {log} FIELDS doc, field;\n"));

    if parent.has_cursor() {
        let presence = parent.presence_table();
        s.push_str(&format!("\n-- `{table}`: per-session presence (cursors)\n"));
        match target {
            Target::Server => s.push_str(&format!(
                "DEFINE TABLE OVERWRITE {presence} SCHEMAFULL\n\
                 \x20 PERMISSIONS\n\
                 \x20   FOR select WHERE {select}\n\
                 \x20   FOR create WHERE $access != \"{IMPERSONATE_ACCESS}\" AND ({update})\n\
                 \x20   FOR update WHERE author = $auth.id AND ({update})\n\
                 \x20   FOR delete WHERE author = $auth.id;\n",
                select = parent.select,
                update = parent.update,
            )),
            Target::Client => s.push_str(&format!("DEFINE TABLE OVERWRITE {presence} SCHEMAFULL PERMISSIONS FULL;\n")),
        }
        s.push_str(&field_lines(
            &presence,
            target,
            &[
                ("doc", &format!("record<{table}>"), " READONLY"),
                ("field", "string", &format!(" ASSERT $value IN [{field_list}] READONLY")),
                ("session", "string", " ASSERT string::len($value) > 0 AND string::len($value) <= 64 READONLY"),
                ("author", "option<record>", " VALUE $before OR $auth.id OR $value READONLY"),
                ("@opaque state", "bytes", &format!(" ASSERT bytes::len($value) <= {MAX_PRESENCE_BYTES}")),
                ("at", "datetime", " VALUE time::now()"),
            ],
        ));
        s.push_str(&format!("DEFINE INDEX OVERWRITE {presence}_doc ON TABLE {presence} FIELDS doc, field;\n"));
    }

    if target == Target::Server {
        s.push_str(&format!(
            "\n-- `{table}`: a deleted record takes its canonical documents along; the\n\
             -- SSP sweep removes orphaned log and presence rows.\n\
             DEFINE EVENT OVERWRITE _00_crdt_cleanup ON TABLE {table} WHEN $event = \"DELETE\" THEN {{\n\
             \x20   DELETE {DOC_TABLE} WHERE doc = $before.id;\n\
             }};\n"
        ));
    }
    s
}

/// `DEFINE FIELD` lines; a name prefixed `@opaque ` gets the annotation that
/// keeps its value out of the sync machinery (it still reaches clients).
fn field_lines(table: &str, target: Target, fields: &[(&str, &str, &str)]) -> String {
    let mut s = String::new();
    for (name, ty, server_clauses) in fields {
        let (opaque, name) = match name.strip_prefix("@opaque ") {
            Some(rest) => (true, rest),
            None => (false, *name),
        };
        if opaque {
            s.push_str("-- @opaque\n");
        }
        let (ty, clauses) = match target {
            // The marker the opaque-field pass would add, baked in: the
            // internal-schema step re-applies these OVERWRITE definitions
            // without that pass and must not strip it.
            Target::Server if opaque => (
                ty.to_string(),
                format!("{server_clauses} COMMENT '{}'", ssp_protocol::OPAQUE_FIELD_COMMENT),
            ),
            Target::Server => (ty.to_string(), server_clauses.to_string()),
            Target::Client => (client_type(ty), String::new()),
        };
        s.push_str(&format!("DEFINE FIELD OVERWRITE {name} ON TABLE {table} TYPE {ty}{clauses};\n"));
    }
    s
}

/// A log row's peer must be one its author claimed in `_00_crdt_peer`; root
/// (no `$auth`) is trusted. Every op in the row's blob must come from this
/// peer, which the fold and every client check before importing it (a peer
/// that is not a decimal u64 fails that check, so no format ASSERT here: the
/// function names for one differ between SurrealDB 2 and 3).
fn peer_assert() -> String {
    format!(
        " ASSERT string::len($value) <= 20 \
         AND ($auth.id = NONE OR (SELECT VALUE author FROM ONLY type::record('{PEER_TABLE}', $value)) = $auth.id) READONLY"
    )
}

fn client_type(ty: &str) -> String {
    if ty.starts_with("option<") {
        ty.to_string()
    } else {
        format!("option<{ty}>")
    }
}

fn peer_table_ddl() -> String {
    format!(
        "\n-- Loro peer ids, each claimed by one user (ids are random below 2^53)\n\
         DEFINE TABLE OVERWRITE {PEER_TABLE} SCHEMAFULL\n\
         \x20 PERMISSIONS\n\
         \x20   FOR select WHERE author = $auth.id\n\
         \x20   FOR create WHERE $auth.id != NONE AND $access != \"{IMPERSONATE_ACCESS}\"\n\
         \x20   FOR update, delete NONE;\n\
         DEFINE FIELD OVERWRITE author ON TABLE {PEER_TABLE} TYPE record VALUE $before OR $auth.id READONLY;\n\
         DEFINE FIELD OVERWRITE at ON TABLE {PEER_TABLE} TYPE datetime VALUE $before OR time::now() READONLY;\n"
    )
}

fn doc_table_ddl(parents: &[CrdtParent]) -> String {
    let select = parents
        .iter()
        .map(|p| format!("(doc.tb() = '{}' AND ({}))", p.table, p.select))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "\n-- Canonical documents, written only by the SSP fold\n\
         DEFINE TABLE OVERWRITE {DOC_TABLE} SCHEMAFULL\n\
         \x20 PERMISSIONS\n\
         \x20   FOR select WHERE {select}\n\
         \x20   FOR create, update, delete NONE;\n\
         DEFINE FIELD OVERWRITE doc ON TABLE {DOC_TABLE} TYPE record;\n\
         DEFINE FIELD OVERWRITE field ON TABLE {DOC_TABLE} TYPE string;\n\
         DEFINE FIELD OVERWRITE rev ON TABLE {DOC_TABLE} TYPE int DEFAULT 0;\n\
         DEFINE FIELD OVERWRITE snapshot ON TABLE {DOC_TABLE} TYPE option<bytes>;\n\
         DEFINE FIELD OVERWRITE upto ON TABLE {DOC_TABLE} TYPE option<object> FLEXIBLE;\n\
         DEFINE FIELD OVERWRITE state_hash ON TABLE {DOC_TABLE} TYPE option<string>;\n\
         DEFINE FIELD OVERWRITE lease_by ON TABLE {DOC_TABLE} TYPE option<string>;\n\
         DEFINE FIELD OVERWRITE lease_until ON TABLE {DOC_TABLE} TYPE option<datetime>;\n\
         DEFINE FIELD OVERWRITE folded_at ON TABLE {DOC_TABLE} TYPE option<datetime>;\n\
         DEFINE INDEX OVERWRITE {DOC_TABLE}_doc ON TABLE {DOC_TABLE} FIELDS doc, field UNIQUE;\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREAD: &str = r#"
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD name ON TABLE user TYPE string;

DEFINE TABLE thread SCHEMAFULL
  PERMISSIONS
    FOR select WHERE published = true OR ($access = "account" AND author.id = $auth.id)
    FOR create WHERE $access = "account" AND author.id = $auth.id
    FOR update WHERE $access = "account"
      AND (author.id = $auth.id
           OR $auth.id IN (SELECT VALUE in FROM collaborates_on WHERE out = $parent.id))
    FOR delete WHERE $access = "account" AND author.id = $auth.id
;
DEFINE FIELD title ON TABLE thread TYPE string;

-- @crdt prosemirror
-- @cursor
DEFINE FIELD content ON TABLE thread TYPE string
  PERMISSIONS FOR select WHERE true;

DEFINE FIELD summary ON TABLE thread TYPE option<string>; -- @crdt text
DEFINE FIELD author ON TABLE thread TYPE record<user>;
DEFINE FIELD published ON TABLE thread TYPE bool VALUE $value OR false;

DEFINE TABLE collaborates_on TYPE RELATION IN user OUT thread SCHEMAFULL;
"#;

    #[test]
    fn rebases_bare_fields_and_id_onto_the_link() {
        let rule = rebase_permission(
            r#"published = true OR ($access = "account" AND author.id = $auth.id)"#,
            "doc",
        )
        .unwrap();
        assert!(rule.contains("doc.published = true"), "{rule}");
        assert!(rule.contains("doc.author.id = $auth.id"), "{rule}");
        assert!(!rule.contains(" author.id"), "parenthesized branch rebased too: {rule}");
        assert!(!rule.contains("doc.$access"), "{rule}");
        assert_eq!(rebase_permission("id = $auth.id", "doc").unwrap(), "doc = $auth.id");
        assert_eq!(rebase_permission("true", "doc").unwrap(), "true");
        assert_eq!(rebase_permission("FALSE", "doc").unwrap(), "false");
    }

    #[test]
    fn subquery_fields_stay_put_and_parent_moves_to_the_link() {
        let rule = rebase_permission(
            "author.id = $auth.id OR $auth.id IN (SELECT VALUE in FROM collaborates_on WHERE out = $parent.id AND role = $parent.role)",
            "doc",
        )
        .unwrap();
        assert!(rule.contains("doc.author.id = $auth.id"), "{rule}");
        assert!(rule.contains("SELECT VALUE in FROM collaborates_on"), "{rule}");
        assert!(rule.contains("out = $parent.doc"), "{rule}");
        assert!(rule.contains("role = $parent.doc.role"), "{rule}");
        assert!(!rule.contains("doc.in"), "{rule}");
        assert!(!rule.contains("doc.out"), "{rule}");
    }

    #[test]
    fn functions_and_casts_are_rebased() {
        let rule = rebase_permission("string::len(title) > 0 AND <string>owner = $auth.id", "doc").unwrap();
        assert!(rule.contains("string::len(doc.title)"), "{rule}");
        assert!(rule.contains("<string> doc.owner") || rule.contains("<string>doc.owner"), "{rule}");
    }

    #[test]
    fn no_crdt_fields_means_no_change() {
        let src = "DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD a ON TABLE t TYPE string;\n";
        let exp = expand(src, Target::Server).unwrap();
        assert_eq!(exp.user, src);
        assert!(exp.generated.is_empty());
        assert!(exp.parents.is_empty());
    }

    #[test]
    fn server_expansion_rewrites_mirrors_and_generates_tables() {
        let exp = expand(THREAD, Target::Server).unwrap();
        assert_eq!(exp.parents.len(), 1);
        let p = &exp.parents[0];
        assert_eq!(p.table, "thread");
        let names: Vec<_> = p.fields.iter().map(|f| (f.name.as_str(), f.kind, f.cursor)).collect();
        assert_eq!(
            names,
            vec![("content", CrdtKind::Prosemirror, true), ("summary", CrdtKind::Text, false)]
        );

        // Mirrors: guarded text, author PERMISSIONS kept, annotations kept.
        assert!(exp.user.contains(&format!(
            "DEFINE FIELD content ON TABLE thread TYPE string {MIRROR_GUARD} PERMISSIONS FOR select WHERE true;"
        )));
        assert!(exp.user.contains(&format!(
            "DEFINE FIELD summary ON TABLE thread TYPE string {MIRROR_GUARD}; -- @crdt text"
        )));
        assert!(exp.user.contains("-- @crdt prosemirror\n-- @cursor\nDEFINE FIELD content"));

        let g = &exp.generated;
        assert!(g.contains("DEFINE FIELD OVERWRITE content__crdt ON TABLE thread TYPE option<object> FLEXIBLE VALUE IF $auth.id != NONE { $before } ELSE { $value };"));
        assert!(g.contains("DEFINE TABLE OVERWRITE thread__crdt_log SCHEMAFULL COMMENT 'sp00ky:crdt_log content=prosemirror,summary=text'"));
        assert!(g.contains("FOR select WHERE doc.published = true"), "{g}");
        assert!(g.contains("FOR create WHERE $access != \"_00_impersonate\" AND ("), "{g}");
        assert!(g.contains("out = $parent.doc"), "{g}");
        assert!(g.contains("FOR update, delete NONE;"));
        assert!(g.contains("-- @opaque\nDEFINE FIELD OVERWRITE data ON TABLE thread__crdt_log TYPE bytes ASSERT bytes::len($value) <= 262144 READONLY COMMENT 'sp00ky:opaque';"), "{g}");
        assert!(g.contains("ASSERT $value IN ['content', 'summary'] READONLY"));
        assert!(g.contains("DEFINE TABLE OVERWRITE thread__crdt_presence SCHEMAFULL"));
        assert!(g.contains("DEFINE TABLE OVERWRITE _00_crdt_doc SCHEMAFULL"));
        assert!(g.contains("DEFINE TABLE OVERWRITE _00_crdt_peer SCHEMAFULL"));
        assert!(g.contains("type::record('_00_crdt_peer', $value)) = $auth.id) READONLY;"), "{g}");
        assert!(g.contains("(doc.tb() = 'thread' AND (doc.published = true"), "{g}");
        assert!(g.contains("DEFINE EVENT OVERWRITE _00_crdt_cleanup ON TABLE thread"));
        assert_eq!(exp.mirror_defs.matches("DEFINE FIELD OVERWRITE").count(), 2);
    }

    #[test]
    fn expanded_server_source_parses_and_annotations_follow() {
        let exp = expand(THREAD, Target::Server).unwrap();
        let src = exp.source();
        let mut parser = SchemaParser::new();
        parser.parse_file(&src).expect("expanded schema parses");
        let log = &parser.tables["thread__crdt_log"];
        assert!(log.fields.contains_key("data"));
        let anns = annotations::extract_field_annotations(&src);
        let data = &anns[&("thread__crdt_log".to_string(), "data".to_string())];
        assert!(annotations::has_annotation(data, "opaque"));
        let content = &anns[&("thread".to_string(), "content".to_string())];
        assert!(annotations::has_annotation(content, "crdt"));
    }

    #[test]
    fn client_expansion_is_plain_columns_without_doc_table() {
        let exp = expand(THREAD, Target::Client).unwrap();
        let g = &exp.generated;
        assert!(g.contains("DEFINE TABLE OVERWRITE thread__crdt_log SCHEMAFULL PERMISSIONS FULL;"));
        assert!(g.contains("DEFINE FIELD OVERWRITE data ON TABLE thread__crdt_log TYPE option<bytes>;"));
        assert!(!g.contains("_00_crdt_doc"));
        assert!(!g.contains("_00_crdt_peer"));
        assert!(!g.contains("READONLY"));
        assert!(!g.contains("$auth"));
        let mut parser = SchemaParser::new();
        parser.parse_file(&exp.source()).expect("client expansion parses");
    }

    #[test]
    fn rejects_bad_kinds_types_and_reserved_names() {
        let bad_kind = "DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD a ON TABLE t TYPE string; -- @crdt json\n";
        assert!(expand(bad_kind, Target::Server).unwrap_err().to_string().contains("unknown CRDT kind"));

        let bad_type = "DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD a ON TABLE t TYPE bytes; -- @crdt text\n";
        assert!(expand(bad_type, Target::Server).unwrap_err().to_string().contains("TYPE must be `string`"));

        let value = "DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD a ON TABLE t TYPE string VALUE 'x'; -- @crdt text\n";
        assert!(expand(value, Target::Server).unwrap_err().to_string().contains("VALUE/ASSERT"));

        let reserved = "DEFINE TABLE t__crdt_log SCHEMAFULL;\n";
        assert!(expand(reserved, Target::Server).unwrap_err().to_string().contains("reserved"));

        let reserved_field = "DEFINE TABLE t SCHEMAFULL;\nDEFINE FIELD a__crdt ON TABLE t TYPE string;\n";
        assert!(expand(reserved_field, Target::Server).unwrap_err().to_string().contains("reserved"));
    }

    #[test]
    fn surrealism_mode_is_refused() {
        let exp = expand(THREAD, Target::Server).unwrap();
        assert!(check_deploy_mode(&exp.parents, &crate::backend::DeployMode::Singlenode).is_ok());
        let err = check_deploy_mode(&exp.parents, &crate::backend::DeployMode::Surrealism).unwrap_err();
        assert!(err.to_string().contains("thread.content"));
    }
}
