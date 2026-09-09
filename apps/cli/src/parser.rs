use anyhow::{Context, Result};

use crate::annotations::FieldAnnotation;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use surrealdb_core::dbs::Capabilities;
use surrealdb_core::sql::statements::DefineStatement;
use surrealdb_core::sql::Statement;
use surrealdb_core::syn::parse_with_capabilities;

#[derive(Debug, Clone)]
pub struct TableSchema {
    #[allow(dead_code)]
    pub name: String,
    pub fields: BTreeMap<String, FieldDefinition>,
    pub schemafull: bool,
    pub relationships: Vec<Relationship>, // List of relationships with field info
    pub is_relation: bool,                // Whether this is a relation table
    pub relation_from: Option<String>,    // Source table for relation
    pub relation_to: Option<String>,      // Target table for relation
    /// Per-action WHERE expression for table-level PERMISSIONS, keyed by
    /// action ("select", "create", "update", "delete"). `Permission::Full`
    /// maps to "true"; `Permission::None` maps to "false"; `Specific(expr)`
    /// is the raw expression formatted via Display, with no `WHERE ` prefix.
    /// Used to derive meta-table permissions that mirror the parent's rules.
    pub table_permissions: BTreeMap<String, String>,
    /// True when the table is marked `-- @nosync`: it is excluded from
    /// generated types, relations, and sync events, and the runtime services
    /// skip it during snapshot/bootstrap. The table still exists in the main
    /// DB (and is backed up). Set in `parse_file` from `extract_table_annotations`.
    pub no_sync: bool,
}

#[derive(Debug, Clone)]
pub struct Relationship {
    pub field_name: String,
    pub related_table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessDefinition {
    pub name: String,
    pub signin_params: BTreeMap<String, FieldDefinition>,
    pub signup_params: BTreeMap<String, FieldDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketDefinition {
    pub name: String,
    pub max_size: Option<u64>,
    pub allowed_extensions: Vec<String>,
    pub path_prefix_auth: bool,
    /// Storage backend as authored in the `.surql` (e.g. "memory" or
    /// "file:/buckets/<name>"). Empty when it couldn't be parsed.
    pub backend: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    #[allow(dead_code)]
    pub name: String,
    pub field_type: FieldType,
    pub optional: bool,
    pub assert: Option<String>,
    pub value: Option<String>,
    pub is_record_id: bool,
    /// The record table a UNION field can link to: `string | record<t>`
    /// parses as a string column (its TypeScript type stays `string`, and
    /// writes are not coerced) but carries this so codegen still emits the
    /// `t` relationship and `.related()` can join the row. `None` for a plain
    /// scalar, and for `record<t>` fields, which already say so in
    /// `field_type`. Whitepawn's `game.white`/`game.black` are the case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_ref: Option<String>,
    pub select_permission: Option<String>,
    pub should_strip: bool, // True if field should be excluded from client
    /// `-- @opaque`: the value IS synced to the client (it stays in the client
    /// schema and in generated types, unlike `should_strip`) but it is never
    /// held in an SSP circuit row or the scheduler replica, so it can never be
    /// used for server-side filtering, ordering, joins or permissions.
    ///
    /// Kept separate from `should_strip` precisely because the two differ only
    /// on the client side; both are excluded server-side.
    pub opaque: bool,
    #[serde(skip)]
    pub annotations: Vec<FieldAnnotation>, // Parsed from -- @name value comments
}

impl FieldDefinition {
    /// True when the sync machinery never holds this field's VALUE: `-- @nosync`,
    /// `-- @crdt` or `-- @opaque`. These are excluded from the ingest payload and
    /// from the replica/circuit row scans, so nothing server-side can evaluate
    /// them.
    ///
    /// Deliberately NOT the same as `should_strip`. `should_strip` is also set by
    /// `PERMISSIONS FOR select WHERE false`, which hides a field from the CLIENT
    /// schema while the server keeps the value and the SSP can still filter on it
    /// perfectly well. Conflating the two would reject valid schemas.
    pub fn excluded_from_sync(&self) -> bool {
        ["nosync", "crdt", "opaque"]
            .iter()
            .any(|name| crate::annotations::has_annotation(&self.annotations, name))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Int,
    Float,
    Bool,
    Datetime,
    Duration,
    Bytes,
    Object,
    Array(Box<FieldType>),
    Record(String),
    Option(Box<FieldType>),
    Any,
}

/// True when `expr` reads `field` as a row field.
///
/// Whole-identifier match only, so `secret` does not match `secret_token` or
/// `x.secret`. `$field` / `$auth.field` are parameters, not row fields, so a
/// `$`-prefixed occurrence does not count. String literals are stripped first so
/// `WHERE kind = 'secret'` is not mistaken for a field reference.
fn expression_references_field(expr: &str, field: &str) -> bool {
    let without_literals = {
        let mut out = String::with_capacity(expr.len());
        let mut quote: Option<char> = None;
        for c in expr.chars() {
            match quote {
                Some(q) => {
                    if c == q {
                        quote = None;
                    }
                }
                None => {
                    if c == '\'' || c == '"' {
                        quote = Some(c);
                    } else {
                        out.push(c);
                    }
                }
            }
        }
        out
    };

    let bytes = without_literals.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    for (idx, _) in without_literals.match_indices(field) {
        let before = idx.checked_sub(1).map(|i| bytes[i]);
        let after = bytes.get(idx + field.len()).copied();
        // A preceding `.` or `$` means this is a path segment or a parameter,
        // not a bare reference to this table's field.
        if before.is_some_and(|b| is_ident(b) || b == b'.' || b == b'$') {
            continue;
        }
        if after.is_some_and(is_ident) {
            continue;
        }
        return true;
    }
    false
}

pub struct SchemaParser {
    pub tables: BTreeMap<String, TableSchema>,
    pub access: BTreeMap<String, AccessDefinition>,
    pub buckets: BTreeMap<String, BucketDefinition>,
}

impl SchemaParser {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
            access: BTreeMap::new(),
            buckets: BTreeMap::new(),
        }
    }

    pub fn parse_file(&mut self, content: &str) -> Result<()> {
        // Extract bucket names from the original content before stripping
        self.extract_buckets(content);

        // Pre-process the content to remove EVENT definitions
        // Events may contain syntax that the parser doesn't fully support yet
        let processed_content = Self::remove_events(content);
        // Remove DEFINE BUCKET statements (not supported by surrealdb-core 2.x)
        let processed_content = Self::remove_buckets(&processed_content);
        // Workaround for parser not supporting FOR ALL
        let processed_content =
            processed_content.replace("FOR ALL", "FOR select, create, update, delete");

        // Create capabilities with all features enabled to support experimental syntax like mod::
        let capabilities = Capabilities::all();

        let query = parse_with_capabilities(&processed_content, &capabilities)
            .context("Failed to parse SurrealDB schema file")?;

        self.process_statements(query.0)?;

        // Apply table-level annotations (e.g. `-- @nosync`). Parsed from the raw
        // content because surrealdb-core strips comments during parsing.
        for (table_name, anns) in crate::annotations::extract_table_annotations(content) {
            if crate::annotations::has_annotation(&anns, "nosync") {
                if let Some(table) = self.tables.get_mut(&table_name) {
                    table.no_sync = true;
                }
            }
        }

        // A marker that attaches to nothing is worse than no marker: the author
        // reads `-- @nosync` in the schema and believes the field is server-only.
        crate::annotations::warn_unattached_annotations(content);

        for ((table_name, field_name), anns) in crate::annotations::extract_field_annotations(content) {
            if let Some(table) = self.tables.get_mut(&table_name) {
                if let Some(field) = table.fields.get_mut(&field_name) {
                    field.annotations = anns.clone();
                    if crate::annotations::has_annotation(&anns, "nosync") {
                        field.should_strip = true;
                    }
                    if crate::annotations::has_annotation(&anns, "opaque") {
                        field.opaque = true;
                    }
                }
            }
        }

        // Stripping a parent object field without stripping its declared
        // children leaves `DEFINE FIELD meta.secret` in the client schema
        // pointing at a `meta` that no longer exists — a column that can only
        // ever be empty, since `cleanRecord` drops the whole `meta` key.
        self.propagate_strip_to_child_paths();

        self.validate_excluded_field_usage(content)?;

        Ok(())
    }

    /// Reject a schema that asks the server to evaluate a field whose value the
    /// sync machinery deliberately does not hold (`-- @nosync`, `-- @crdt`,
    /// `-- @opaque` on a `DEFINE FIELD`).
    ///
    /// Two placements are rejected:
    ///
    /// 1. A table `PERMISSIONS` expression. This is the severe one. The SSP
    ///    AND-folds the select permission into every scan of the table, and
    ///    `resolve_field` returns `None` for a key the circuit does not hold, so
    ///    the predicate evaluates false for every row — the whole table reads as
    ///    empty, and the client treats absent membership as deleted.
    /// 2. A `DEFINE INDEX ... FIELDS` list naming a stripped field. The index
    ///    would survive into the client schema pointing at a column
    ///    `filter_schema_for_client` removed; a `UNIQUE` index over a column
    ///    every cached row leaves unset then collides on the second row.
    ///
    /// The SSP enforces (1) again at registration time, but only for a query
    /// that actually reaches it. Failing at deploy is what makes it a schema
    /// error the author sees immediately rather than an empty list in the app.
    fn validate_excluded_field_usage(&self, content: &str) -> Result<()> {
        use regex::Regex;

        let mut problems: Vec<String> = Vec::new();

        for (table_name, table) in &self.tables {
            let excluded: Vec<&String> = table
                .fields
                .iter()
                .filter(|(_, f)| f.excluded_from_sync())
                .map(|(name, _)| name)
                .collect();
            if excluded.is_empty() {
                continue;
            }

            for (action, expr) in &table.table_permissions {
                for field in &excluded {
                    if expression_references_field(expr, field) {
                        problems.push(format!(
                            "  • table '{table_name}': PERMISSIONS FOR {action} references \
                             '{field}', which is excluded from sync (@nosync/@crdt/@opaque). \
                             The sync engine has no value to compare, so this rule matches \
                             NO rows and the whole table reads as empty on every client."
                        ));
                    }
                }
            }
        }

        // `DEFINE INDEX ... [ON TABLE] t FIELDS|COLUMNS a, b`
        let index_re = Regex::new(
            r"(?is)DEFINE\s+INDEX\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(?:TABLE\s+)?(\w+)\s+(?:FIELDS|COLUMNS)\s+([^;]+)",
        )
        .expect("static regex");
        for caps in index_re.captures_iter(content) {
            let (index_name, table_name) = (&caps[1], &caps[2]);
            let Some(table) = self.tables.get(table_name) else {
                continue;
            };
            for raw in caps[3].split(',') {
                // Trim trailing index modifiers (`UNIQUE`, `SEARCH ANALYZER ...`).
                let field = raw.trim().split_whitespace().next().unwrap_or("").trim();
                if field.is_empty() {
                    continue;
                }
                let root = field.split('.').next().unwrap_or(field);
                if let Some(def) = table.fields.get(field).or_else(|| table.fields.get(root)) {
                    if def.should_strip || def.opaque {
                        problems.push(format!(
                            "  • index '{index_name}' on '{table_name}' indexes '{field}', \
                             which is excluded from sync (@nosync/@opaque). Remove the field \
                             from the index, or the annotation from the field."
                        ));
                    }
                }
            }
        }

        if problems.is_empty() {
            return Ok(());
        }
        anyhow::bail!(
            "Schema references fields that are excluded from sync:\n{}",
            problems.join("\n")
        )
    }

    /// Propagate `should_strip` / `opaque` from a parent field to every nested
    /// path declared beneath it (`meta` → `meta.secret`, `meta.a.b`).
    ///
    /// Field names are flat keys in `TableSchema::fields`, so `meta` and
    /// `meta.secret` are independent entries and an annotation on the parent
    /// says nothing about the child.
    fn propagate_strip_to_child_paths(&mut self) {
        for table in self.tables.values_mut() {
            let parents: Vec<(String, bool, bool)> = table
                .fields
                .iter()
                .filter(|(_, f)| f.should_strip || f.opaque)
                .map(|(name, f)| (name.clone(), f.should_strip, f.opaque))
                .collect();

            for (parent, strip, opaque) in parents {
                let prefix = format!("{}.", parent);
                let children: Vec<String> = table
                    .fields
                    .keys()
                    .filter(|name| name.starts_with(&prefix))
                    .cloned()
                    .collect();
                for child in children {
                    if let Some(field) = table.fields.get_mut(&child) {
                        field.should_strip |= strip;
                        field.opaque |= opaque;
                    }
                }
            }
        }
    }

    /// Remove DEFINE EVENT statements from the schema content
    /// This is a workaround for parser limitations with certain EVENT syntax
    fn remove_events(content: &str) -> String {
        let lines: Vec<&str> = content.lines().collect();
        let mut result = Vec::new();
        let mut i = 0;

        while i < lines.len() {
            let line = lines[i];

            // Check if this line starts a DEFINE EVENT
            if line.trim_start().starts_with("DEFINE EVENT") {
                // Skip lines until we find the closing semicolon or brace
                let mut brace_count = 0;
                let mut in_event = true;

                while i < lines.len() && in_event {
                    let current = lines[i];

                    // Count braces
                    for ch in current.chars() {
                        match ch {
                            '{' => brace_count += 1,
                            '}' => {
                                brace_count -= 1;
                                if brace_count == 0 {
                                    // Check if there's a semicolon on this line
                                    if current.contains(';') {
                                        in_event = false;
                                    }
                                }
                            }
                            ';' if brace_count == 0 => {
                                in_event = false;
                            }
                            _ => {}
                        }
                    }

                    i += 1;

                    // Safety check - if we've gone too far without finding the end, break
                    if i > lines.len() {
                        break;
                    }
                }

                // Continue without adding the event lines to result
                continue;
            }

            result.push(line);
            i += 1;
        }

        result.join("\n")
    }

    /// Remove DEFINE BUCKET statements from the schema content
    /// surrealdb-core 2.x does not support DEFINE BUCKET, so we strip them before parsing
    fn remove_buckets(content: &str) -> String {
        let lines: Vec<&str> = content.lines().collect();
        let mut result = Vec::new();
        let mut i = 0;

        while i < lines.len() {
            let line = lines[i];

            if line.trim_start().starts_with("DEFINE BUCKET") {
                // Skip lines until we find the closing semicolon
                while i < lines.len() {
                    if lines[i].contains(';') {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }

            result.push(line);
            i += 1;
        }

        result.join("\n")
    }

    /// Extract bucket definitions from the schema content, including PERMISSIONS constraints
    pub fn extract_buckets(&mut self, content: &str) {
        let name_re =
            Regex::new(r"(?i)DEFINE\s+BUCKET\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?(\w+)")
                .unwrap();

        // Find each DEFINE BUCKET statement and capture from start to terminating ';'
        let block_re = Regex::new(
            r"(?is)DEFINE\s+BUCKET\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?(\w+)([^;]*);",
        )
        .unwrap();

        // The upload limit is a `COMMENT 'sp00ky:maxSize=<bytes>'` annotation, not a
        // permission predicate: the engine evaluates a bucket's PERMISSIONS clause
        // before the upload exists, so `file::head($file).size` there is the size of
        // whatever already sits at that key. Buckets scaffolded before that was
        // understood still carry the old predicate, so it is read as a fallback.
        let max_size_re = Regex::new(r"sp00ky:maxSize=(\d+)").unwrap();
        let legacy_max_size_re =
            Regex::new(r"file::head\(\$file\)\.size\s*<=?\s*(\d+)").unwrap();
        let ext_re = Regex::new(r"string::ends_with\(file::key\(\$file\),\s*'\.(\w+)'\)").unwrap();
        let auth_re = Regex::new(r"string::starts_with\(file::key\(\$file\),.*\$auth").unwrap();
        let backend_re = Regex::new(r#"(?i)BACKEND\s+"([^"]*)""#).unwrap();

        for cap in block_re.captures_iter(content) {
            let name = cap[1].to_string();
            let body = &cap[2];

            let max_size = max_size_re
                .captures(body)
                .or_else(|| legacy_max_size_re.captures(body))
                .and_then(|c| c[1].parse::<u64>().ok());

            let allowed_extensions: Vec<String> = ext_re
                .captures_iter(body)
                .map(|c| c[1].to_string())
                .collect();

            let path_prefix_auth = auth_re.is_match(body);

            let backend = backend_re
                .captures(body)
                .map(|c| c[1].to_string())
                .unwrap_or_default();

            self.buckets.insert(
                name.clone(),
                BucketDefinition {
                    name,
                    max_size,
                    allowed_extensions,
                    path_prefix_auth,
                    backend,
                },
            );
        }

        // Fallback: also match single-line DEFINE BUCKET without PERMISSIONS
        // (the block_re above already handles this, but keep for robustness)
        for cap in name_re.captures_iter(content) {
            let name = cap[1].to_string();
            if !self.buckets.contains_key(&name) {
                self.buckets.insert(
                    name.clone(),
                    BucketDefinition {
                        name,
                        max_size: None,
                        allowed_extensions: Vec::new(),
                        path_prefix_auth: false,
                        backend: String::new(),
                    },
                );
            }
        }
    }

    fn process_statements(&mut self, statements: surrealdb_core::sql::Statements) -> Result<()> {
        for statement in statements.0 {
            match statement {
                Statement::Define(define) => {
                    self.process_define_statement(define)?;
                }
                _ => {
                    // Skip other statement types (scopes, events, etc.)
                }
            }
        }
        Ok(())
    }

    fn process_define_statement(&mut self, define: DefineStatement) -> Result<()> {
        match define {
            DefineStatement::Table(table_def) => {
                let table_name = table_def.name.to_string();
                let schemafull = matches!(table_def.kind, surrealdb_core::sql::TableType::Normal);

                // Check if this is a relation table
                let is_relation =
                    matches!(table_def.kind, surrealdb_core::sql::TableType::Relation(_));
                let (relation_from, relation_to) = if is_relation {
                    if let surrealdb_core::sql::TableType::Relation(rel) = table_def.kind {
                        let from = rel.from.as_ref().and_then(|kind| {
                            // Extract table names from the Kind type
                            if let surrealdb_core::sql::Kind::Record(tables) = kind {
                                if tables.is_empty() {
                                    None
                                } else {
                                    Some(tables[0].to_string())
                                }
                            } else {
                                None
                            }
                        });
                        let to = rel.to.as_ref().and_then(|kind| {
                            // Extract table names from the Kind type
                            if let surrealdb_core::sql::Kind::Record(tables) = kind {
                                if tables.is_empty() {
                                    None
                                } else {
                                    Some(tables[0].to_string())
                                }
                            } else {
                                None
                            }
                        });
                        (from, to)
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                let table_permissions = Self::extract_table_permissions(&table_def.permissions);

                self.tables.insert(
                    table_name.clone(),
                    TableSchema {
                        name: table_name,
                        fields: BTreeMap::new(),
                        schemafull,
                        relationships: Vec::new(),
                        is_relation,
                        relation_from,
                        relation_to,
                        table_permissions,
                        no_sync: false,
                    },
                );
            }
            DefineStatement::Field(field_def) => {
                let table_name = field_def.what.to_string();
                let field_name = field_def.name.to_string();

                let record_ref = field_def.kind.as_ref().and_then(Self::union_record_ref);
                let field_type = if let Some(kind) = field_def.kind {
                    Self::parse_kind(kind)
                } else {
                    FieldType::Any
                };

                let assert_clause = field_def.assert.map(|v| format!("{}", v));
                let value_clause = field_def.value.map(|v| format!("{}", v));

                // Check if this field is a record ID (has Record type)
                let is_record_id = Self::is_field_record_id(&field_type);

                // Extract select permission and determine if field should be stripped
                let (select_permission, should_strip) =
                    Self::parse_permissions(&field_def.permissions);

                let field = FieldDefinition {
                    name: field_name.clone(),
                    field_type: field_type.clone(),
                    optional: false,
                    assert: assert_clause,
                    value: value_clause,
                    is_record_id,
                    record_ref,
                    select_permission: select_permission.clone(),
                    should_strip,
                    opaque: false,
                    annotations: Vec::new(),
                };

                if let Some(table) = self.tables.get_mut(&table_name) {
                    // Extract related table name from Record type
                    if let Some(related_table) = Self::extract_related_table(&field_type) {
                        let relationship = Relationship {
                            field_name: field_name.clone(),
                            related_table: related_table.clone(),
                        };
                        // Check if this exact relationship already exists
                        if !table
                            .relationships
                            .iter()
                            .any(|r| r.field_name == field_name && r.related_table == related_table)
                        {
                            table.relationships.push(relationship);
                        }
                    }
                    table.fields.insert(field_name, field);
                }
            }
            DefineStatement::Access(access_def) => {
                let name = access_def.name.to_string();

                let (signin_params, signup_params) =
                    if let surrealdb_core::sql::AccessType::Record(ref record_access) =
                        access_def.kind
                    {
                        let signin = if let Some(ref si) = record_access.signin {
                            Self::extract_params(&format!("{}", si))
                        } else if let Some(ref auth) = access_def.authenticate {
                            Self::extract_params(&format!("{}", auth))
                        } else {
                            BTreeMap::new()
                        };

                        let signup = if let Some(ref su) = record_access.signup {
                            Self::extract_params(&format!("{}", su))
                        } else {
                            BTreeMap::new()
                        };
                        (signin, signup)
                    } else {
                        let signin = if let Some(ref auth) = access_def.authenticate {
                            Self::extract_params(&format!("{}", auth))
                        } else {
                            BTreeMap::new()
                        };
                        (signin, BTreeMap::new())
                    };

                self.access.insert(
                    name.clone(),
                    AccessDefinition {
                        name,
                        signin_params,
                        signup_params,
                    },
                );
            }
            _ => {
                // Skip other define types (indexes, scopes, etc.)
            }
        }

        Ok(())
    }

    /// For a union kind (`string | record<t>`, or `option<...>` of one), the
    /// single record table it can link to. Unions parse to a scalar
    /// `FieldType` (see `parse_kind`), so without this the link would be lost
    /// and `.related()` on the field would have nothing to join.
    fn union_record_ref(kind: &surrealdb_core::sql::Kind) -> Option<String> {
        use surrealdb_core::sql::Kind;
        match kind {
            Kind::Either(kinds) => {
                let mut records = kinds.iter().filter_map(|k| match k {
                    Kind::Record(tables) if !tables.is_empty() => Some(tables[0].to_string()),
                    _ => None,
                });
                let first = records.next()?;
                // Two different tables would be ambiguous; say nothing.
                if records.any(|t| t != first) {
                    return None;
                }
                Some(first)
            }
            Kind::Option(inner) => Self::union_record_ref(inner),
            _ => None,
        }
    }

    fn parse_kind(kind: surrealdb_core::sql::Kind) -> FieldType {
        use surrealdb_core::sql::Kind;

        match kind {
            Kind::String => FieldType::String,
            Kind::Int => FieldType::Int,
            Kind::Float => FieldType::Float,
            Kind::Bool => FieldType::Bool,
            Kind::Datetime => FieldType::Datetime,
            Kind::Duration => FieldType::Duration,
            Kind::Bytes => FieldType::Bytes,
            Kind::Object => FieldType::Object,
            Kind::Array(inner, _) => FieldType::Array(Box::new(Self::parse_kind(*inner))),
            Kind::Record(tables) => {
                if tables.is_empty() {
                    FieldType::Record("any".to_string())
                } else {
                    FieldType::Record(tables[0].to_string())
                }
            }
            Kind::Option(inner) => FieldType::Option(Box::new(Self::parse_kind(*inner))),
            Kind::Any => FieldType::Any,
            // `string | record<t>`: the column holds either, so the client
            // type is the scalar member; the record half survives as
            // `FieldDefinition::record_ref` (see `union_record_ref`).
            Kind::Either(kinds) => kinds
                .into_iter()
                .find(|k| !matches!(k, Kind::Record(_)))
                .map(Self::parse_kind)
                .unwrap_or(FieldType::Any),
            _ => FieldType::Any,
        }
    }

    /// Check if a field type is a record ID (contains Record type anywhere in the type hierarchy)
    fn is_field_record_id(field_type: &FieldType) -> bool {
        match field_type {
            FieldType::Record(_) => true,
            FieldType::Option(inner) => Self::is_field_record_id(inner),
            FieldType::Array(inner) => Self::is_field_record_id(inner),
            _ => false,
        }
    }

    /// Extract the related table name from a field type (if it's a Record type)
    fn extract_related_table(field_type: &FieldType) -> Option<String> {
        match field_type {
            FieldType::Record(table_name) if table_name != "any" => {
                // Check if this is a junction table and map it to the actual target table
                let actual_table = if table_name == "commented_on" {
                    crate::ui::detail(format!("mapping junction table {} to comment", table_name));
                    "comment".to_string()
                } else if table_name.ends_with("_on") || table_name.contains("relation") {
                    // This is likely a junction table, try to find the actual target
                    // For now, use a simple heuristic
                    crate::ui::detail(format!("junction table detected: {}", table_name));
                    table_name.clone()
                } else {
                    table_name.clone()
                };
                Some(actual_table)
            }
            FieldType::Option(inner) => Self::extract_related_table(inner),
            FieldType::Array(inner) => Self::extract_related_table(inner),
            _ => None,
        }
    }

    fn extract_table_permissions(
        permissions: &surrealdb_core::sql::Permissions,
    ) -> BTreeMap<String, String> {
        use surrealdb_core::sql::Permission;
        let render = |p: &Permission| -> String {
            match p {
                Permission::None => "false".to_string(),
                Permission::Full => "true".to_string(),
                Permission::Specific(v) => format!("{}", v),
                _ => "false".to_string(),
            }
        };
        let mut map = BTreeMap::new();
        map.insert("select".to_string(), render(&permissions.select));
        map.insert("create".to_string(), render(&permissions.create));
        map.insert("update".to_string(), render(&permissions.update));
        map.insert("delete".to_string(), render(&permissions.delete));
        map
    }

    /// Parse field permissions and determine if field should be stripped from client schema
    /// Returns (select_permission_string, should_strip)
    fn parse_permissions(permissions: &surrealdb_core::sql::Permissions) -> (Option<String>, bool) {
        // Convert permissions to string and check for patterns
        let perm_str = format!("{}", permissions);

        // Check if the permission string contains "FOR select WHERE false"
        // or similar patterns that indicate the field should not be readable
        let should_strip = perm_str.contains("FOR select WHERE false")
            || perm_str.contains("FOR select false")
            || (perm_str.contains("SELECT") && perm_str.contains("false"));

        if should_strip {
            crate::ui::detail("field has 'FOR select WHERE false': stripped from client schema");
        }

        (Some(perm_str), should_strip)
    }

    fn extract_params(content: &str) -> BTreeMap<String, FieldDefinition> {
        let mut params = BTreeMap::new();
        let mut excluded_vars = HashSet::new();

        // Regex to find LET definitions: LET $var = ...
        let let_re = Regex::new(r"LET\s+\$(\w+)\s*=").unwrap();
        for cap in let_re.captures_iter(content) {
            excluded_vars.insert(cap[1].to_string());
        }

        // Regex to find all variable usages: $var
        let re = Regex::new(r"\$(\w+)").unwrap();

        for cap in re.captures_iter(content) {
            let name = cap[1].to_string();
            // Skip global variables (simplified check matching previous pattern)
            if name == "auth"
                || name == "access"
                || name == "value"
                || name == "this"
                || name == "before"
                || name == "after"
                || name == "event"
                || excluded_vars.contains(&name)
            {
                continue;
            }

            // Default params to String, required (non-optional)
            params.insert(
                name.clone(),
                FieldDefinition {
                    name: name.clone(),
                    field_type: FieldType::String,
                    optional: false,
                    assert: None,
                    value: None,
                    is_record_id: false,
                    record_ref: None,
                    select_permission: None,
                    should_strip: false,
                    opaque: false,
                    annotations: Vec::new(),
                },
            );
        }
        params
    }
}

#[cfg(test)]
mod excluded_field_usage_tests {
    use super::*;

    fn parse(content: &str) -> Result<SchemaParser> {
        let mut p = SchemaParser::new();
        p.parse_file(content)?;
        Ok(p)
    }

    #[test]
    fn opaque_field_in_select_permission_is_rejected() {
        // The severe case: the SSP AND-folds this into every scan of `user`, and
        // it has no value to compare, so the table reads as empty everywhere.
        let err = parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE secret_token = 'x';
-- @opaque
DEFINE FIELD secret_token ON TABLE user TYPE string;
",
        )
        .err()
        .expect("must reject");
        let msg = err.to_string();
        assert!(msg.contains("secret_token"), "got: {msg}");
        assert!(msg.contains("PERMISSIONS FOR select"), "got: {msg}");
    }

    #[test]
    fn nosync_and_crdt_fields_in_permissions_are_also_rejected() {
        for annotation in ["-- @nosync", "-- @crdt text"] {
            let schema = format!(
                "\
DEFINE TABLE doc SCHEMAFULL PERMISSIONS FOR select WHERE body != NONE;
{annotation}
DEFINE FIELD body ON TABLE doc TYPE string;
"
            );
            assert!(parse(&schema).is_err(), "{annotation} should be rejected");
        }
    }

    #[test]
    fn a_select_denied_field_in_permissions_is_allowed() {
        // `PERMISSIONS FOR select WHERE false` also sets `should_strip`, but the
        // SERVER still holds the value and the SSP can filter on it — only the
        // client schema drops it. Rejecting this would break valid schemas.
        parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE internal_flag = true;
DEFINE FIELD internal_flag ON TABLE user TYPE bool PERMISSIONS FOR select WHERE false;
",
        )
        .expect("must be allowed");
    }

    #[test]
    fn a_similarly_named_field_does_not_trip_the_check() {
        // Whole-identifier match only: `secret` must not match `secret_token`.
        parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE secret_token = 'x';
-- @opaque
DEFINE FIELD secret ON TABLE user TYPE string;
DEFINE FIELD secret_token ON TABLE user TYPE string;
",
        )
        .expect("must be allowed");
    }

    #[test]
    fn a_string_literal_matching_the_field_name_does_not_trip_the_check() {
        parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE kind = 'blob';
-- @opaque
DEFINE FIELD blob ON TABLE user TYPE bytes;
DEFINE FIELD kind ON TABLE user TYPE string;
",
        )
        .expect("literal must not count as a field reference");
    }

    #[test]
    fn a_param_of_the_same_name_does_not_trip_the_check() {
        parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE $blob != NONE;
-- @opaque
DEFINE FIELD blob ON TABLE user TYPE bytes;
",
        )
        .expect("$param must not count as a field reference");
    }

    #[test]
    fn indexing_an_excluded_field_is_rejected() {
        let err = parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;
-- @opaque
DEFINE FIELD blob ON TABLE user TYPE bytes;
DEFINE INDEX idx_blob ON TABLE user FIELDS blob UNIQUE;
",
        )
        .err()
        .expect("must reject");
        assert!(err.to_string().contains("idx_blob"), "got: {err}");
    }

    #[test]
    fn indexing_a_client_stripped_field_is_also_rejected() {
        // A select-denied field IS removed from the client schema, so an index on
        // it dangles there even though the server keeps the value.
        let err = parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD hidden ON TABLE user TYPE string PERMISSIONS FOR select WHERE false;
DEFINE INDEX idx_hidden ON TABLE user FIELDS hidden;
",
        )
        .err()
        .expect("must reject");
        assert!(err.to_string().contains("idx_hidden"), "got: {err}");
    }

    #[test]
    fn indexing_an_ordinary_field_is_allowed() {
        parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD email ON TABLE user TYPE string;
-- @opaque
DEFINE FIELD blob ON TABLE user TYPE bytes;
DEFINE INDEX idx_email ON TABLE user FIELDS email UNIQUE;
",
        )
        .expect("must be allowed");
    }

    #[test]
    fn opaque_does_not_strip_the_field_from_the_client() {
        // The defining difference between @opaque and @nosync.
        let p = parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;
-- @opaque
DEFINE FIELD blob ON TABLE user TYPE bytes;
-- @nosync
DEFINE FIELD secret_token ON TABLE user TYPE string;
",
        )
        .expect("parses");
        let fields = &p.tables.get("user").expect("user table").fields;
        let blob = fields.get("blob").expect("blob field");
        assert!(blob.opaque, "blob must be opaque");
        assert!(!blob.should_strip, "@opaque must NOT strip from the client");
        let secret = fields.get("secret_token").expect("secret_token field");
        assert!(secret.should_strip, "@nosync must strip from the client");
        assert!(!secret.opaque, "@nosync is not @opaque on the client side");
        // Both are excluded server-side, which is what the marker keys on.
        assert!(blob.excluded_from_sync() && secret.excluded_from_sync());
    }

    #[test]
    fn bucket_max_size_comes_from_the_annotation_with_a_legacy_fallback() {
        let mut p = SchemaParser::new();
        p.extract_buckets(
            "\
DEFINE BUCKET IF NOT EXISTS avatars BACKEND \"memory\"
  PERMISSIONS WHERE
    $action NOT IN ['put']
    OR (
      string::ends_with(file::key($file), '.png')
    )
  COMMENT 'sp00ky:maxSize=5242880';
DEFINE BUCKET IF NOT EXISTS legacy BACKEND \"memory\"
  PERMISSIONS WHERE
    $action NOT IN ['put']
    OR (
      file::head($file).size <= 1024
      AND string::ends_with(file::key($file), '.pdf')
    );
DEFINE BUCKET IF NOT EXISTS bare BACKEND \"memory\";
",
        );
        let avatars = p.buckets.get("avatars").expect("avatars bucket");
        assert_eq!(avatars.max_size, Some(5_242_880));
        assert_eq!(avatars.allowed_extensions, vec!["png".to_string()]);
        // Buckets scaffolded before the annotation existed still report a limit.
        assert_eq!(p.buckets.get("legacy").expect("legacy").max_size, Some(1024));
        assert_eq!(p.buckets.get("bare").expect("bare").max_size, None);
    }

    #[test]
    fn stripping_a_parent_field_strips_its_declared_children() {
        let p = parse(
            "\
DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;
-- @nosync
DEFINE FIELD meta ON TABLE user TYPE object;
DEFINE FIELD meta.secret ON TABLE user TYPE string;
",
        )
        .expect("parses");
        let fields = &p.tables.get("user").expect("user table").fields;
        assert!(
            fields.get("meta.secret").expect("child field").should_strip,
            "child of a stripped parent must be stripped too"
        );
    }
}
