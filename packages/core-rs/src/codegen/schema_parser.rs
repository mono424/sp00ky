//! SurrealQL schema -> tables, fields, annotations and access methods.
//!
//! PORT: `packages/spooky_core/lib/src/codegen/schema_parser.dart`, with the
//! annotation grammar of the CLI (`apps/cli/src/annotations.rs`) so both
//! parsers agree on what an annotation is and what it attaches to.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use regex::Regex;

use crate::error::{Error, Result};
use crate::modules::relationships::pluralize_table_name;
use crate::types::{Cardinality, SchemaRelationship};

/// A parsed `DEFINE FIELD`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedField {
    /// Field name as declared (kept as-is, e.g. snake_case or `meta.color`).
    pub name: String,
    /// Normalized type with any `option<>` stripped (`string`, `datetime`,
    /// `record`, `array<string>`).
    pub kind: String,
    pub optional: bool,
    pub is_record: bool,
    pub is_datetime: bool,
    /// For `record<x>`, the referenced table; `None` for a bare `record`.
    pub record_table: Option<String>,
    /// `-- @opaque`: synced to the client and readable, but the sync engine
    /// never stores the value, so it cannot be used for server-side filtering
    /// or ordering. Contrast `-- @nosync`, which removes the field entirely.
    pub opaque: bool,
    /// `-- @ephemeral` (on a `-- @channel` table): the value only travels
    /// between clients over peer to peer channels and is never written to the
    /// database.
    pub ephemeral: bool,
    /// `-- @crdt <kind>`: edited collaboratively as a CRDT document; the field
    /// itself is the server-written text mirror of the fold.
    pub crdt: Option<String>,
    /// `-- @cursor`: the CRDT field also shares cursor presence.
    pub cursor: bool,
    /// `-- @parent`: the record link to the row this one belongs to.
    pub parent: bool,
}

impl ParsedField {
    /// A dotted or array-projected sub-field (`meta.color`, `tags.*`), which
    /// lives inside its parent field's value rather than as a column of its own
    /// in a typed model.
    pub fn is_nested(&self) -> bool {
        self.name.contains(['.', '*', '['])
    }
}

/// A parsed `DEFINE TABLE` with its fields.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedTable {
    pub name: String,
    pub fields: Vec<ParsedField>,
    /// `-- @channel`: clients may open peer to peer channels on its rows.
    pub channel: bool,
}

/// A `DEFINE ACCESS` and the `$vars` its SIGNUP / SIGNIN bodies reference.
/// SurrealQL declares no types for these, so they are emitted as strings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedAccess {
    pub name: String,
    pub signup_params: Vec<String>,
    pub signin_params: Vec<String>,
}

/// Tables plus access methods (backends come from OpenAPI specs).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedSchema {
    pub tables: Vec<ParsedTable>,
    pub accesses: Vec<ParsedAccess>,
}

static DEFINE_TABLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^DEFINE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+|OVERWRITE\s+)?([A-Za-z_][A-Za-z0-9_]*)",
    )
    .unwrap()
});

/// The whole statement: `(?s)` so a TYPE followed by clauses on later lines
/// still parses (Dart's `.` stops at a newline, which dropped such fields).
static DEFINE_FIELD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)^DEFINE\s+FIELD\s+(?:IF\s+NOT\s+EXISTS\s+|OVERWRITE\s+)?([A-Za-z_][A-Za-z0-9_.*\[\]]*)\s+ON\s+(?:TABLE\s+)?([A-Za-z_][A-Za-z0-9_]*)\s+TYPE\s+(.*)$",
    )
    .unwrap()
});

/// Line-anchored variant for scanning the RAW schema for annotations, where a
/// statement has not been isolated by splitting on `;` yet.
static DEFINE_FIELD_LINE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^DEFINE\s+FIELD\s+(?:IF\s+NOT\s+EXISTS\s+|OVERWRITE\s+)?([A-Za-z_][A-Za-z0-9_.*\[\]]*)\s+ON\s+(?:TABLE\s+)?([A-Za-z_][A-Za-z0-9_]*)",
    )
    .unwrap()
});

/// Clause keywords that can follow the TYPE expression in a DEFINE FIELD.
static TYPE_TERMINATOR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\s+(ASSERT|DEFAULT|VALUE|PERMISSIONS|READONLY|COMMENT|REFERENCE|FLEXIBLE)\b")
        .unwrap()
});

static DEFINE_ACCESS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)DEFINE\s+ACCESS\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap());

static VAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\$([A-Za-z_][A-Za-z0-9_]*)").unwrap());

static OPTION_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)^option\s*<\s*(.*)\s*>$").unwrap());

static RECORD_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^record\s*<\s*([A-Za-z_][A-Za-z0-9_]*)").unwrap());

static LINE_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"--.*|#.*").unwrap());

/// The CLI's annotation syntax: `-- @name` and at most one value token.
static ANNOTATION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^--\s*@([a-z][a-z0-9_]*)(?:\s+(\S+))?\s*$").unwrap());

static ANNOTATION_START: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^--\s*@[a-z]").unwrap());

/// Once the CLI materializes `-- @nosync`, the server DEFINE TABLE carries this
/// COMMENT instead.
const NOSYNC_TABLE_COMMENT: &str = "sp00ky:nosync";

/// SurrealQL built-in vars that are never user-supplied signup/signin params.
const BUILTIN_VARS: &[&str] = &[
    "auth", "value", "access", "token", "session", "scope", "before", "after", "this", "parent",
    "input", "event",
];

/// Annotations that carry a value (`-- @crdt text`).
const VALUED_ANNOTATIONS: &[&str] = &["crdt"];

/// One annotation comment: its name and value, or `None` for an ordinary
/// comment. A sentence that merely begins with an annotation name is not one
/// (more than one token), nor is a value on a marker that takes none.
fn parse_annotation(comment: &str) -> Option<(String, Option<String>)> {
    let caps = ANNOTATION.captures(comment.trim())?;
    let name = caps[1].to_string();
    let value = caps.get(2).map(|m| m.as_str().to_string());
    if value.is_some() && !VALUED_ANNOTATIONS.contains(&name.as_str()) {
        return None;
    }
    Some((name, value))
}

fn strip_comments(s: &str) -> String {
    LINE_COMMENT.replace_all(s, "").into_owned()
}

/// Annotations resolved to the statement each one precedes (or trails).
#[derive(Default)]
struct Annotations {
    nosync_tables: BTreeSet<String>,
    channel_tables: BTreeSet<String>,
    /// By `"<table>.<field>"`.
    fields: BTreeMap<String, Vec<(String, Option<String>)>>,
}

impl Annotations {
    fn field_has(&self, key: &str, name: &str) -> bool {
        self.fields
            .get(key)
            .is_some_and(|a| a.iter().any(|(n, _)| n == name))
    }

    fn field_value(&self, key: &str, name: &str) -> Option<String> {
        self.fields
            .get(key)
            .and_then(|a| a.iter().find(|(n, _)| n == name))
            .map(|(_, v)| v.clone().unwrap_or_default())
    }
}

/// Resolve annotation comments to the `DEFINE TABLE` / `DEFINE FIELD` that
/// follows them (or, for a single-line field, trails it after its `;`).
///
/// Pending annotations MUST be cleared by any statement, not just a matching
/// one: clearing only on `DEFINE TABLE` let a field-level `-- @nosync` leak
/// forward to the next `DEFINE TABLE`, which was then dropped from the
/// generated types entirely with no warning. A blank line clears too; a
/// non-annotation comment does not, so prose between a marker and its
/// statement is allowed.
fn annotations_from_comments(surql: &str) -> Annotations {
    let mut result = Annotations::default();
    let mut pending: Vec<(String, Option<String>)> = Vec::new();
    for line in surql.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("--") {
            if let Some(ann) = parse_annotation(trimmed) {
                pending.push(ann);
            }
            continue;
        }
        if trimmed.is_empty() {
            pending.clear();
            continue;
        }
        if let Some(table) = DEFINE_TABLE.captures(trimmed) {
            let name = table[1].to_string();
            for (ann, _) in pending.drain(..) {
                match ann.as_str() {
                    "nosync" => {
                        result.nosync_tables.insert(name.clone());
                    }
                    "channel" => {
                        result.channel_tables.insert(name.clone());
                    }
                    _ => {}
                }
            }
            continue;
        }
        if let Some(field) = DEFINE_FIELD_LINE.captures(trimmed) {
            let key = format!("{}.{}", &field[2], &field[1]);
            let mut anns = std::mem::take(&mut pending);
            if let Some(semi) = trimmed.rfind(';') {
                if let Some(trailing) = parse_annotation(trimmed[semi + 1..].trim()) {
                    anns.push(trailing);
                }
            }
            if !anns.is_empty() {
                result.fields.entry(key).or_default().extend(anns);
            }
            continue;
        }
        // Any other statement orphans the annotation.
        pending.clear();
    }
    result
}

/// Parse tables and access methods.
pub fn parse_project(surql: &str) -> Result<ParsedSchema> {
    Ok(ParsedSchema {
        tables: parse_schema(surql)?,
        accesses: parse_accesses(surql),
    })
}

/// Parse a schema into tables, in the order their `DEFINE TABLE` appears, with
/// fields in declaration order. Other statements are ignored.
///
/// Tables marked `@nosync` are server-only and excluded (mirrors the CLI,
/// which strips them too). `_00_`-prefixed system tables are excluded the same
/// way: they are synced and read at runtime but never appear in generated
/// types. Their fields are skipped so a trailing DEFINE FIELD cannot resurrect
/// the table.
pub fn parse_schema(surql: &str) -> Result<Vec<ParsedTable>> {
    let annotations = annotations_from_comments(surql);
    let mut nosync = annotations.nosync_tables.clone();
    let mut order: Vec<String> = Vec::new();
    let mut tables: BTreeMap<String, ParsedTable> = BTreeMap::new();
    let mut table_for = |name: &str, tables: &mut BTreeMap<String, ParsedTable>| {
        if !tables.contains_key(name) {
            order.push(name.to_string());
            tables.insert(
                name.to_string(),
                ParsedTable {
                    name: name.to_string(),
                    ..Default::default()
                },
            );
        }
    };

    // Strip line comments BEFORE splitting on `;`: a `;` inside a `--` comment
    // ("owner of this broadcast (the publisher; = relay stream id)") would
    // otherwise split mid-statement and silently drop the next field.
    for raw in strip_comments(surql).split(';') {
        let stmt = raw.trim();
        if stmt.is_empty() {
            continue;
        }
        if let Some(m) = DEFINE_TABLE.captures(stmt) {
            let name = m[1].to_string();
            if name.starts_with("_00_")
                || nosync.contains(&name)
                || stmt.contains(NOSYNC_TABLE_COMMENT)
            {
                nosync.insert(name);
                continue;
            }
            table_for(&name, &mut tables);
            if annotations.channel_tables.contains(&name) {
                tables.get_mut(&name).unwrap().channel = true;
            }
            continue;
        }
        if let Some(m) = DEFINE_FIELD.captures(stmt) {
            let (field, table) = (m[1].to_string(), m[2].to_string());
            if nosync.contains(&table) {
                continue;
            }
            let key = format!("{table}.{field}");
            // A `-- @nosync` FIELD is server-only: drop it, keep the table. The
            // materialized server form carries `COMMENT 'sp00ky:opaque'`, which
            // the CLI also stamps on `@crdt`/`@opaque` fields, so only the
            // comment form decides this.
            if annotations.field_has(&key, "nosync") {
                continue;
            }
            let mut parsed = build_field(&field, extract_type(&m[3]));
            parsed.opaque = annotations.field_has(&key, "opaque");
            parsed.ephemeral = annotations.field_has(&key, "ephemeral");
            parsed.crdt = annotations.field_value(&key, "crdt");
            parsed.cursor = annotations.field_has(&key, "cursor");
            parsed.parent = annotations.field_has(&key, "parent");
            table_for(&table, &mut tables);
            tables.get_mut(&table).unwrap().fields.push(parsed);
        }
    }

    let result: Vec<ParsedTable> = order.iter().filter_map(|n| tables.remove(n)).collect();
    validate_channels(&result)?;
    Ok(result)
}

/// `-- @channel` / `-- @ephemeral` rules, as the CLI enforces them: an
/// ephemeral field is never written to the database, so its table must be a
/// channel table (the only way the value can travel) and the field must be
/// optional (a SCHEMAFULL create would otherwise demand a value that never
/// comes).
fn validate_channels(tables: &[ParsedTable]) -> Result<()> {
    for table in tables {
        for field in table.fields.iter().filter(|f| f.ephemeral) {
            let at = format!("{}.{}", table.name, field.name);
            if !table.channel {
                return Err(Error::InvalidArgument(format!(
                    "Field '{at}' is `-- @ephemeral` but table '{t}' is not `-- @channel`. Ephemeral values only \
                     travel over channels: add `-- @channel` above `DEFINE TABLE {t}`.",
                    t = table.name
                )));
            }
            if field.opaque {
                return Err(Error::InvalidArgument(format!(
                    "Field '{at}' cannot be both `-- @ephemeral` and `-- @opaque`."
                )));
            }
            if !field.optional {
                return Err(Error::InvalidArgument(format!(
                    "Field '{at}' is `-- @ephemeral`, so the database never receives a value for it. Declare it \
                     optional (`TYPE option<...>`)."
                )));
            }
        }
    }
    Ok(())
}

/// Extract [`ParsedAccess`]es by scanning the full source (access blocks span
/// braces/parens with internal `;`, so they cannot go through the statement
/// splitter).
pub fn parse_accesses(surql: &str) -> Vec<ParsedAccess> {
    let src = strip_comments(surql);
    DEFINE_ACCESS
        .captures_iter(&src)
        .map(|m| {
            let rest = &src[m.get(0).unwrap().end()..];
            ParsedAccess {
                name: m[1].to_string(),
                signup_params: vars_in_section(rest, "SIGNUP"),
                signin_params: vars_in_section(rest, "SIGNIN"),
            }
        })
        .collect()
}

/// `$var` names referenced inside the balanced `{...}` / `(...)` body after
/// `keyword`, minus SurrealQL built-ins, de-duplicated in first-seen order.
fn vars_in_section(block: &str, keyword: &str) -> Vec<String> {
    let Ok(kw) = Regex::new(&format!(r"(?i)\b{keyword}\b")) else {
        return Vec::new();
    };
    let Some(m) = kw.find(block) else {
        return Vec::new();
    };
    let Some(body) = balanced_span(block, m.end()) else {
        return Vec::new();
    };
    let mut seen = BTreeSet::new();
    VAR.captures_iter(body)
        .map(|c| c[1].to_string())
        .filter(|name| !BUILTIN_VARS.contains(&name.as_str()))
        .filter(|name| seen.insert(name.clone()))
        .collect()
}

/// From `from`, skip whitespace to the first `{` or `(`, then return the text
/// up to its matching close (nesting handled). `None` when there is none.
fn balanced_span(s: &str, from: usize) -> Option<&str> {
    let bytes = s.as_bytes();
    let mut i = from;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let open = *bytes.get(i)?;
    let close = match open {
        b'{' => b'}',
        b'(' => b')',
        _ => return None,
    };
    let start = i;
    let mut depth = 0usize;
    for (j, &b) in bytes.iter().enumerate().skip(start) {
        if b == open {
            depth += 1;
        } else if b == close {
            depth -= 1;
            if depth == 0 {
                return Some(&s[start + 1..j]);
            }
        }
    }
    None
}

/// Trim trailing clause keywords (ASSERT/DEFAULT/...) off the TYPE expression.
fn extract_type(after_type: &str) -> &str {
    let end = TYPE_TERMINATOR
        .find(after_type)
        .map(|m| m.start())
        .unwrap_or(after_type.len());
    after_type[..end].trim()
}

fn build_field(name: &str, type_expr: &str) -> ParsedField {
    let mut kind = type_expr.trim().to_string();
    let mut optional = false;
    if let Some(inner) = OPTION_TYPE.captures(&kind).map(|c| c[1].trim().to_string()) {
        optional = true;
        kind = inner;
    }
    let lower = kind.to_lowercase();
    let is_datetime = lower == "datetime";
    let is_record = lower == "record" || lower.starts_with("record<");
    let mut record_table = None;
    if is_record {
        record_table = RECORD_TYPE.captures(&kind).map(|c| c[1].to_string());
        kind = "record".to_string();
    }
    ParsedField {
        name: name.to_string(),
        kind,
        optional,
        is_record,
        is_datetime,
        record_table,
        ..Default::default()
    }
}

/// Remove `-- @nosync` fields (and the annotation comment that marked them)
/// from a schema, for embedding as the client's local cache DDL.
///
/// Only single-line `DEFINE FIELD ...;` statements are handled, which is the
/// form the marker attaches to; a multi-line definition is left in place
/// rather than risking a mangled statement. The annotation comment must go
/// with the field: an orphan `-- @nosync` re-attaches to the next
/// `DEFINE TABLE` and would drop that whole table on the next parse.
pub fn strip_server_only_fields(surql: &str) -> String {
    let annotations = annotations_from_comments(surql);
    if !annotations
        .fields
        .values()
        .flatten()
        .any(|(n, _)| n == "nosync")
    {
        return surql.to_string();
    }
    let mut out: Vec<&str> = Vec::new();
    for line in surql.split('\n') {
        let trimmed = line.trim();
        if let Some(field) = DEFINE_FIELD_LINE.captures(trimmed) {
            let key = format!("{}.{}", &field[2], &field[1]);
            if trimmed.ends_with(';') && annotations.field_has(&key, "nosync") {
                // Drop the contiguous comment block that introduced the field,
                // but only when it holds an annotation: ordinary prose above an
                // unrelated statement must survive. Dropping the whole block
                // (not just the annotation lines) handles `-- @nosync` /
                // `-- prose` / `DEFINE FIELD`, where stopping at the prose would
                // strand the marker and mis-mark the next statement.
                let mut block_start = out.len();
                while block_start > 0 && out[block_start - 1].trim().starts_with("--") {
                    block_start -= 1;
                }
                if out[block_start..]
                    .iter()
                    .any(|l| ANNOTATION_START.is_match(l.trim()))
                {
                    out.truncate(block_start);
                }
                continue;
            }
        }
        out.push(line);
    }
    out.join("\n")
}

/// The schema relationships, matching `apps/cli/src/json_schema.rs` so a Rust
/// client and a JS client generated from one schema expose the same relation
/// names.
///
/// Forward: every `record<x>` field whose target is a known table becomes a
/// `one` relationship under its own field name. Reverse: each of those adds a
/// `many` relationship on the target, named by pluralizing the source table
/// (skipped when the target already declares a field of that name, so an
/// explicit field always wins).
pub fn derive_relationships(tables: &[ParsedTable]) -> Vec<SchemaRelationship> {
    let known: BTreeSet<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    let field_names: BTreeMap<&str, BTreeSet<&str>> = tables
        .iter()
        .map(|t| {
            (
                t.name.as_str(),
                t.fields.iter().map(|f| f.name.as_str()).collect(),
            )
        })
        .collect();
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    let mut add = |rel: SchemaRelationship, out: &mut Vec<SchemaRelationship>| {
        if seen.insert(format!("{}.{}", rel.from, rel.field)) {
            out.push(rel);
        }
    };
    let links: Vec<(&ParsedTable, &ParsedField, &String)> = tables
        .iter()
        .flat_map(|t| t.fields.iter().map(move |f| (t, f)))
        .filter_map(|(t, f)| match &f.record_table {
            Some(target) if f.is_record && known.contains(target.as_str()) => Some((t, f, target)),
            _ => None,
        })
        .collect();
    for &(table, field, target) in &links {
        add(
            SchemaRelationship {
                from: table.name.clone(),
                field: field.name.clone(),
                to: target.clone(),
                cardinality: Cardinality::One,
            },
            &mut out,
        );
    }
    for &(table, _, target) in &links {
        let reverse = pluralize_table_name(&table.name);
        if field_names
            .get(target.as_str())
            .is_some_and(|f| f.contains(reverse.as_str()))
        {
            continue;
        }
        add(
            SchemaRelationship {
                from: target.clone(),
                field: reverse,
                to: table.name.clone(),
                cardinality: Cardinality::Many,
            },
            &mut out,
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::relationships::find_relationship;

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

    fn names(tables: &[ParsedTable]) -> Vec<&str> {
        tables.iter().map(|t| t.name.as_str()).collect()
    }

    fn table<'a>(tables: &'a [ParsedTable], name: &str) -> &'a ParsedTable {
        tables.iter().find(|t| t.name == name).unwrap()
    }

    fn field<'a>(t: &'a ParsedTable, name: &str) -> &'a ParsedField {
        t.fields.iter().find(|f| f.name == name).unwrap()
    }

    fn field_names(t: &ParsedTable) -> Vec<&str> {
        t.fields.iter().map(|f| f.name.as_str()).collect()
    }

    // ---- Dart `codegen_test.dart` --------------------------------------------------

    #[test]
    fn extracts_tables_and_fields_with_types_and_flags() {
        let tables = parse_schema(SURQL).unwrap();
        assert_eq!(names(&tables), ["user", "thread"]);
        let user = table(&tables, "user");
        assert_eq!(field(user, "username").kind, "string");
        assert!(!field(user, "username").optional);
        assert!(field(user, "created_at").is_datetime);
        assert!(field(user, "created_at").optional);
        let thread = table(&tables, "thread");
        assert!(field(thread, "author").is_record);
        assert_eq!(
            field(thread, "author").record_table.as_deref(),
            Some("user")
        );
        assert_eq!(field(thread, "tags").kind, "array<string>");
    }

    // ---- Dart `codegen_parsers_test.dart` ------------------------------------------------

    #[test]
    fn access_vars_exclude_built_ins() {
        let surql = "
        DEFINE ACCESS account ON DATABASE TYPE RECORD
          SIGNUP {
            RETURN CREATE ONLY user SET
              username = $username,
              password = crypto::argon2::generate($password),
              share_pubkey = $share_pubkey;
          }
          SIGNIN ( SELECT * FROM user WHERE username = $username
                   AND crypto::argon2::compare(password, $password) )
          DURATION FOR TOKEN 15m, FOR SESSION 30d;
        DEFINE TABLE user SCHEMAFULL;
      ";
        let accesses = parse_accesses(surql);
        assert_eq!(accesses.len(), 1);
        assert_eq!(accesses[0].name, "account");
        assert_eq!(
            accesses[0].signup_params,
            ["username", "password", "share_pubkey"]
        );
        assert_eq!(accesses[0].signin_params, ["username", "password"]);
    }

    #[test]
    fn nosync_tables_and_their_fields_are_excluded() {
        let surql = "
        DEFINE TABLE user SCHEMAFULL;
        DEFINE FIELD email ON user TYPE string;

        -- @nosync
        DEFINE TABLE signup_code SCHEMAFULL PERMISSIONS NONE;
        DEFINE FIELD code ON signup_code TYPE string;
        DEFINE FIELD used_at ON signup_code TYPE option<datetime>;

        DEFINE TABLE game SCHEMAFULL;
      ";
        assert_eq!(names(&parse_schema(surql).unwrap()), ["user", "game"]);
    }

    #[test]
    fn a_field_level_nosync_drops_only_that_field_never_the_next_table() {
        let surql = "
        DEFINE TABLE user SCHEMAFULL;
        DEFINE FIELD email ON user TYPE string;
        -- @nosync
        DEFINE FIELD secret_token ON user TYPE string;
        DEFINE TABLE game SCHEMAFULL;
        DEFINE FIELD title ON game TYPE string;
      ";
        let tables = parse_schema(surql).unwrap();
        assert_eq!(names(&tables), ["user", "game"]);
        assert_eq!(field_names(table(&tables, "user")), ["email"]);
        assert_eq!(field_names(table(&tables, "game")), ["title"]);
    }

    #[test]
    fn a_blank_line_orphans_an_annotation() {
        let surql = "
        -- @nosync

        DEFINE TABLE game SCHEMAFULL;
      ";
        assert_eq!(names(&parse_schema(surql).unwrap()), ["game"]);
    }

    #[test]
    fn opaque_keeps_the_field_and_flags_it() {
        let surql = "
        DEFINE TABLE user SCHEMAFULL;
        DEFINE FIELD email ON user TYPE string;
        -- @opaque
        DEFINE FIELD avatar_blob ON user TYPE option<bytes>;
      ";
        let tables = parse_schema(surql).unwrap();
        let user = &tables[0];
        assert_eq!(field_names(user), ["email", "avatar_blob"]);
        assert!(!field(user, "email").opaque);
        assert!(field(user, "avatar_blob").opaque);
    }

    #[test]
    fn prose_between_marker_and_field_still_attaches() {
        let surql = "
        DEFINE TABLE user SCHEMAFULL;
        -- @nosync
        -- the API token, never leaves the server
        DEFINE FIELD secret_token ON user TYPE string;
        DEFINE FIELD email ON user TYPE string;
      ";
        assert_eq!(field_names(&parse_schema(surql).unwrap()[0]), ["email"]);
    }

    #[test]
    fn a_sentence_starting_with_an_annotation_name_is_not_one() {
        // CLI grammar: one value token at most, and only `@crdt` takes one.
        let surql = "
        -- @nosync would break the client here, so this table stays synced.
        DEFINE TABLE foo SCHEMAFULL;
        -- @nosync true
        DEFINE TABLE bar SCHEMAFULL;
      ";
        assert_eq!(names(&parse_schema(surql).unwrap()), ["foo", "bar"]);
    }

    #[test]
    fn strip_server_only_fields_removes_the_field_and_its_annotation_block() {
        let surql = "DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD email ON user TYPE string;
-- @nosync
-- the API token
DEFINE FIELD secret_token ON user TYPE string;
DEFINE TABLE game SCHEMAFULL;";
        let out = strip_server_only_fields(surql);
        assert!(!out.contains("secret_token"));
        // The orphan must go too, or re-parsing the embedded schema would mark
        // `game` as nosync and drop it.
        assert!(!out.contains("@nosync"));
        assert!(out.contains("DEFINE TABLE game"));
        assert_eq!(names(&parse_schema(&out).unwrap()), ["user", "game"]);
        assert_eq!(strip_server_only_fields(SURQL), SURQL);
    }

    #[test]
    fn the_materialized_nosync_comment_excludes_a_table() {
        let surql = "
        DEFINE TABLE user SCHEMAFULL;
        DEFINE TABLE signup_code TYPE NORMAL SCHEMAFULL COMMENT 'sp00ky:nosync' PERMISSIONS NONE;
        DEFINE FIELD code ON signup_code TYPE string;
      ";
        assert_eq!(names(&parse_schema(surql).unwrap()), ["user"]);
    }

    #[test]
    fn system_tables_are_excluded() {
        let surql = "
        DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
        DEFINE FIELD title ON thread TYPE string;

        DEFINE TABLE _00_feature_flag SCHEMAFULL PERMISSIONS NONE;
        DEFINE FIELD key ON _00_feature_flag TYPE string;

        DEFINE TABLE _00_user_feature SCHEMAFULL PERMISSIONS FOR select WHERE user = $auth.id;
        DEFINE FIELD variant ON _00_user_feature TYPE string;
      ";
        assert_eq!(names(&parse_schema(surql).unwrap()), ["thread"]);
    }

    #[test]
    fn a_field_after_a_comment_containing_a_semicolon_is_still_parsed() {
        let surql = "
        DEFINE TABLE broadcast SCHEMAFULL
          PERMISSIONS
            FOR select WHERE true
            FOR create, update, delete WHERE $access = \"account\" AND owner = $auth.id
        ;
        -- owner of this broadcast (the publisher; = relay stream id)
        DEFINE FIELD owner ON TABLE broadcast TYPE record<user>;
        DEFINE FIELD enabled ON TABLE broadcast TYPE bool DEFAULT false;
        -- the create stays valid; mirrors stream_presence.moves.
        DEFINE FIELD restream_providers ON TABLE broadcast TYPE array<string> DEFAULT [];
      ";
        let tables = parse_schema(surql).unwrap();
        let broadcast = table(&tables, "broadcast");
        assert_eq!(
            field_names(broadcast),
            ["owner", "enabled", "restream_providers"]
        );
        assert!(field(broadcast, "owner").is_record);
        assert_eq!(
            field(broadcast, "owner").record_table.as_deref(),
            Some("user")
        );
    }

    #[test]
    fn a_multi_line_field_definition_parses() {
        let surql = "DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD body ON t TYPE option<string>
  ASSERT $value == NONE OR string::len($value) < 100
  DEFAULT 'x';";
        let t = &parse_schema(surql).unwrap()[0];
        assert_eq!(field(t, "body").kind, "string");
        assert!(field(t, "body").optional);
    }

    #[test]
    fn channel_marks_the_table_and_ephemeral_the_field() {
        let surql = "
-- @channel
DEFINE TABLE call SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD view_id ON TABLE call TYPE option<string>;
-- @ephemeral
DEFINE FIELD view_pos ON TABLE call TYPE option<string>;

DEFINE TABLE note SCHEMAFULL;
DEFINE FIELD title ON TABLE note TYPE string;
";
        let tables = parse_schema(surql).unwrap();
        let call = table(&tables, "call");
        assert!(call.channel);
        assert!(field(call, "view_pos").ephemeral);
        assert!(!field(call, "view_id").ephemeral);
        assert!(!table(&tables, "note").channel);
    }

    #[test]
    fn ephemeral_needs_a_channel_table_and_an_optional_type() {
        let err = parse_schema(
            "DEFINE TABLE call SCHEMAFULL;\n-- @ephemeral\nDEFINE FIELD view_pos ON TABLE call TYPE option<string>;\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("@channel"), "{err}");
        let err = parse_schema(
            "-- @channel\nDEFINE TABLE call SCHEMAFULL;\n-- @ephemeral\nDEFINE FIELD view_pos ON TABLE call TYPE string;\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("option<"), "{err}");
    }

    #[test]
    fn crdt_cursor_and_parent_annotations_preceding_and_trailing() {
        let surql = "
DEFINE TABLE doc SCHEMAFULL;
-- @crdt text
-- @cursor
DEFINE FIELD body ON doc TYPE string;
DEFINE FIELD owner ON doc TYPE record<user>; -- @parent
DEFINE FIELD title ON doc TYPE string; -- @crdt
";
        let doc = &parse_schema(surql).unwrap()[0];
        assert_eq!(field(doc, "body").crdt.as_deref(), Some("text"));
        assert!(field(doc, "body").cursor);
        assert!(field(doc, "owner").parent);
        assert_eq!(field(doc, "title").crdt.as_deref(), Some(""));
        assert!(!field(doc, "title").parent);
    }

    #[test]
    fn parse_project_returns_tables_and_accesses_together() {
        let surql = "
        DEFINE ACCESS account ON DATABASE TYPE RECORD
          SIGNUP ( CREATE user SET email = $email, password = $password )
          SIGNIN ( SELECT * FROM user WHERE email = $email );
        DEFINE TABLE user SCHEMAFULL;
        DEFINE FIELD email ON user TYPE string;
      ";
        let parsed = parse_project(surql).unwrap();
        assert!(names(&parsed.tables).contains(&"user"));
        assert_eq!(parsed.accesses[0].signup_params, ["email", "password"]);
        assert_eq!(parsed.accesses[0].signin_params, ["email"]);
    }

    // ---- Dart `relations_test.dart`: deriveRelationships ----------------------------------

    const REL_SURQL: &str = "
DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD title ON thread TYPE string;
DEFINE FIELD author ON thread TYPE record<user>;
DEFINE TABLE comment SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD body ON comment TYPE string;
DEFINE FIELD score ON comment TYPE int;
DEFINE FIELD thread ON comment TYPE record<thread>;
DEFINE FIELD author ON comment TYPE record<user>;
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD name ON user TYPE string;
";

    #[test]
    fn a_record_field_becomes_a_forward_one_relation() {
        let rels = derive_relationships(&parse_schema(REL_SURQL).unwrap());
        let rel = find_relationship(&rels, "thread", "author").unwrap();
        assert_eq!(rel.to, "user");
        assert_eq!(rel.cardinality, Cardinality::One);
        assert_eq!(rel.foreign_key_field(), "author");
    }

    #[test]
    fn each_forward_relation_adds_a_pluralized_reverse_many_relation() {
        let rels = derive_relationships(&parse_schema(REL_SURQL).unwrap());
        let rel = find_relationship(&rels, "thread", "comments").unwrap();
        assert_eq!(rel.to, "comment");
        assert_eq!(rel.cardinality, Cardinality::Many);
        assert_eq!(rel.foreign_key_field(), "thread");
        assert_eq!(
            find_relationship(&rels, "user", "threads").unwrap().to,
            "thread"
        );
        assert_eq!(
            find_relationship(&rels, "user", "comments").unwrap().to,
            "comment"
        );
    }

    #[test]
    fn a_record_field_pointing_at_an_unknown_table_is_skipped() {
        let tables = parse_schema(
            "DEFINE TABLE thread SCHEMAFULL;\nDEFINE FIELD owner ON thread TYPE record<ghost>;",
        )
        .unwrap();
        assert!(derive_relationships(&tables).is_empty());
    }

    #[test]
    fn an_explicit_field_wins_over_the_derived_reverse_name() {
        let tables = parse_schema(
            "DEFINE TABLE thread SCHEMAFULL;
DEFINE FIELD author ON thread TYPE record<user>;
DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD threads ON user TYPE string;",
        )
        .unwrap();
        let derived = derive_relationships(&tables);
        assert!(find_relationship(&derived, "user", "threads").is_none());
        assert!(find_relationship(&derived, "thread", "author").is_some());
    }
}
