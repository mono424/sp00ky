use crate::parser::{FieldType, SchemaParser, TableSchema};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonSchema {
    #[serde(rename = "$schema")]
    pub schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub schema_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<Vec<String>>,
    pub definitions: BTreeMap<String, Value>,
}

pub struct JsonSchemaGenerator;

impl JsonSchemaGenerator {
    pub fn new() -> Self {
        Self
    }

    pub fn generate(&self, parser: &SchemaParser) -> JsonSchema {
        let mut definitions = BTreeMap::new();
        let mut properties = BTreeMap::new();
        let mut required_tables = Vec::new();

        // Tables marked `-- @nosync` are excluded from the generated schema
        // entirely: no definition, no relationships, no relation-table entry,
        // and any relationship pointing AT them is dropped.
        let nosync: std::collections::HashSet<&str> = parser
            .tables
            .iter()
            .filter(|(_, t)| t.no_sync)
            .map(|(n, _)| n.as_str())
            .collect();

        // Build relationships map (from field references)
        let mut relationships: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for (table_name, table_schema) in &parser.tables {
            if table_schema.no_sync {
                continue;
            }
            let relationship_objects: Vec<Value> = table_schema
                .relationships
                .iter()
                .filter(|rel| {
                    if nosync.contains(rel.related_table.as_str()) {
                        eprintln!(
                            "  ⚠ Dropping relationship {}.{} → {} (target table is @nosync)",
                            table_name, rel.field_name, rel.related_table
                        );
                        false
                    } else {
                        true
                    }
                })
                .map(|rel| {
                    json!({
                        "field": rel.field_name,
                        "table": rel.related_table
                    })
                })
                .collect();
            if !relationship_objects.is_empty() {
                relationships.insert(table_name.clone(), relationship_objects);
            }
        }

        // Build reverse relationships
        // For each relationship A.field -> B, create a reverse relationship B.reverse_field -> A[]
        for (from_table, table_schema) in &parser.tables {
            // Generated CRDT log/presence rows are read by the CRDT module, not
            // through `.related()`: no `thread__crdt_logs` column on the parent.
            if table_schema.no_sync || crate::crdt_schema::is_generated_table(from_table) {
                continue;
            }
            for rel in &table_schema.relationships {
                let to_table = &rel.related_table;

                // Skip if the target table doesn't exist (shouldn't happen, but be safe)
                if !parser.tables.contains_key(to_table) {
                    continue;
                }

                // Skip reverse links into a @nosync table.
                if nosync.contains(to_table.as_str()) {
                    continue;
                }

                // Generate reverse field name (pluralize the source table name)
                let reverse_field_name = Self::pluralize_table_name(from_table);

                // Check if this reverse relationship already exists (avoid duplicates)
                let reverse_rels = relationships
                    .entry(to_table.clone())
                    .or_insert_with(Vec::new);
                let already_exists = reverse_rels.iter().any(|r| {
                    if let Some(field) = r.get("field").and_then(|f| f.as_str()) {
                        field == reverse_field_name
                    } else {
                        false
                    }
                });

                if !already_exists {
                    reverse_rels.push(json!({
                        "field": reverse_field_name,
                        "table": from_table,
                        "is_reverse": true,
                        "cardinality": "many"
                    }));
                }
            }
        }

        // Build relation tables map (from TYPE RELATION definitions)
        let mut relation_tables: Vec<Value> = Vec::new();
        for (table_name, table_schema) in &parser.tables {
            if table_schema.no_sync {
                continue;
            }
            if table_schema.is_relation {
                relation_tables.push(json!({
                    "name": table_name,
                    "from": table_schema.relation_from,
                    "to": table_schema.relation_to
                }));
            }
        }

        // Add Relationships definition
        let mut relationship_properties = serde_json::Map::new();
        for (table_name, _related_objects) in &relationships {
            relationship_properties.insert(
                table_name.clone(),
                json!({
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field": {
                                "type": "string",
                                "description": "The field name that creates this relationship"
                            },
                            "table": {
                                "type": "string",
                                "description": "The related table name"
                            }
                        },
                        "required": ["field", "table"]
                    }
                }),
            );
        }

        if !relationship_properties.is_empty() {
            definitions.insert(
                "Relationships".to_string(),
                json!({
                    "type": "object",
                    "properties": relationship_properties
                }),
            );
        }

        // Add RelationTables definition
        if !relation_tables.is_empty() {
            definitions.insert(
                "RelationTables".to_string(),
                json!({
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "The name of the relation table"
                            },
                            "from": {
                                "type": ["string", "null"],
                                "description": "The source table for this relation"
                            },
                            "to": {
                                "type": ["string", "null"],
                                "description": "The target table for this relation"
                            }
                        },
                        "required": ["name"]
                    },
                    "const": relation_tables
                }),
            );
        }

        // Add Access definitions
        let mut access_map = serde_json::Map::new();
        for (name, access_def) in &parser.access {
            let mut signin_params = serde_json::Map::new();
            for (param_name, field_def) in &access_def.signin_params {
                let mut field_schema = self.generate_field_schema(&field_def.field_type);
                if let Value::Object(ref mut obj) = field_schema {
                    obj.insert("optional".to_string(), Value::Bool(field_def.optional));
                    if field_def.is_record_id {
                        obj.insert("x-is-record-id".to_string(), Value::Bool(true));
                    }
                }
                signin_params.insert(param_name.clone(), field_schema);
            }

            let mut signup_params = serde_json::Map::new();
            for (param_name, field_def) in &access_def.signup_params {
                let mut field_schema = self.generate_field_schema(&field_def.field_type);
                if let Value::Object(ref mut obj) = field_schema {
                    obj.insert("optional".to_string(), Value::Bool(field_def.optional));
                    if field_def.is_record_id {
                        obj.insert("x-is-record-id".to_string(), Value::Bool(true));
                    }
                }
                signup_params.insert(param_name.clone(), field_schema);
            }

            access_map.insert(
                name.clone(),
                json!({
                    "signIn": {
                        "params": Value::Object(signin_params)
                    },
                    "signup": {
                        "params": Value::Object(signup_params)
                    }
                }),
            );
        }

        if !access_map.is_empty() {
            definitions.insert(
                "Access".to_string(),
                json!({
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {
                            "signIn": {
                                "type": "object",
                                "properties": {
                                    "params": {
                                        "type": "object",
                                        "additionalProperties": { "type": "object" }
                                    }
                                }
                            },
                            "signup": {
                                "type": "object",
                                "properties": {
                                    "params": {
                                        "type": "object",
                                        "additionalProperties": { "type": "object" }
                                    }
                                }
                            }
                        }
                    },
                    "const": Value::Object(access_map)
                }),
            );
        }

        // Add Buckets definitions (objects with constraints)
        let bucket_objects: Vec<Value> = parser
            .buckets
            .values()
            .map(|bucket| {
                let mut obj = serde_json::Map::new();
                obj.insert("name".to_string(), Value::String(bucket.name.clone()));
                if let Some(max_size) = bucket.max_size {
                    obj.insert("maxSize".to_string(), json!(max_size));
                }
                if !bucket.allowed_extensions.is_empty() {
                    obj.insert(
                        "allowedExtensions".to_string(),
                        Value::Array(
                            bucket
                                .allowed_extensions
                                .iter()
                                .map(|e| Value::String(e.clone()))
                                .collect(),
                        ),
                    );
                }
                if bucket.path_prefix_auth {
                    obj.insert("pathPrefixAuth".to_string(), Value::Bool(true));
                }
                if bucket.backend.eq_ignore_ascii_case(crate::bucket::R2_BACKEND) {
                    obj.insert("backend".to_string(), Value::String(crate::bucket::R2_BACKEND.to_string()));
                }
                Value::Object(obj)
            })
            .collect();
        if !bucket_objects.is_empty() {
            definitions.insert(
                "Buckets".to_string(),
                json!({
                    "type": "array",
                    "const": bucket_objects
                }),
            );
        }

        for (table_name, table_schema) in &parser.tables {
            // Tables marked @nosync are omitted from generated types entirely.
            if table_schema.no_sync {
                continue;
            }
            // Get reverse relationships for this table
            let reverse_rels = relationships.get(table_name).and_then(|rels| {
                let reverse: Vec<&Value> = rels
                    .iter()
                    .filter(|r| {
                        r.get("is_reverse")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    })
                    .collect();
                if reverse.is_empty() {
                    None
                } else {
                    Some(reverse)
                }
            });

            let definition = self.generate_table_definition(table_schema, reverse_rels);
            definitions.insert(table_name.clone(), definition);

            // Add a property that references the definition
            properties.insert(
                table_name.clone(),
                json!({
                    "$ref": format!("#/definitions/{}", table_name)
                }),
            );

            // Mark all table properties as required
            required_tables.push(table_name.clone());
        }

        JsonSchema {
            schema: "http://json-schema.org/draft-07/schema#".to_string(),
            schema_type: Some("object".to_string()),
            properties: Some(properties),
            required: Some(required_tables),
            definitions,
        }
    }

    fn generate_table_definition(
        &self,
        table: &TableSchema,
        reverse_rels: Option<Vec<&Value>>,
    ) -> Value {
        let mut properties = serde_json::Map::new();
        let mut required_fields = Vec::new();

        // Always add 'id' field for SurrealDB records
        properties.insert(
            "id".to_string(),
            json!({
                "type": "string",
                "description": "Record ID",
                "x-is-record-id": true
            }),
        );
        required_fields.push("id".to_string());

        for (field_name, field_def) in &table.fields {
            // Skip fields marked as should_strip (FOR select WHERE false)
            if field_def.should_strip {
                continue;
            }
            // A sub-path field (`errors[*]`, `settings.theme`) types part of a
            // value its parent field already carries; it is no property of the
            // record. As one, it became the TypeScript column `errors[*]: ...`,
            // which does not compile, for every outbox table made from the
            // `spky api add` template. Same rule as `sp00ky::is_sub_path`.
            if field_name.contains('[') || field_name.contains('.') {
                continue;
            }

            let field_schema = self.generate_field_schema(&field_def.field_type);

            let mut field_obj = serde_json::Map::new();

            if let Value::Object(schema_props) = field_schema {
                field_obj.extend(schema_props);
            }

            // Add description with assertions if present
            if let Some(assert) = &field_def.assert {
                field_obj.insert(
                    "description".to_string(),
                    Value::String(format!("Assert: {}", assert)),
                );
            }

            // Add is_record_id flag to metadata
            if field_def.is_record_id {
                field_obj.insert("x-is-record-id".to_string(), Value::Bool(true));
            }

            // A `string | record<t>` union: not a record column, but it links
            // to `t` when it holds a link. Codegen turns this into the
            // relationship that lets `.related()` join the row.
            if let Some(t) = &field_def.record_ref {
                field_obj.insert("x-record-ref".to_string(), Value::String(t.clone()));
            }

            // Add is_datetime flag to metadata
            if matches!(field_def.field_type, FieldType::Datetime)
                || Self::is_field_datetime(&field_def.field_type)
            {
                field_obj.insert("x-is-datetime".to_string(), Value::Bool(true));
            }

            // Add is_bytes flag to metadata so codegen can emit
            // `Uint8Array` instead of `string` for the runtime type. The
            // base JSON-schema `type` stays `string` (with
            // contentEncoding: base64) for spec consumers; this flag is
            // the sp00ky-runtime extension.
            if matches!(field_def.field_type, FieldType::Bytes)
                || Self::is_field_bytes(&field_def.field_type)
            {
                field_obj.insert("x-is-bytes".to_string(), Value::Bool(true));
            }

            // Add annotation metadata (e.g., x-crdt from -- @crdt text)
            for ann in &field_def.annotations {
                let key = format!("x-{}", ann.name);
                let val = ann.value.clone().unwrap_or_default();
                field_obj.insert(key, Value::String(val));
            }

            properties.insert(field_name.clone(), Value::Object(field_obj));

            // Determine if field is required
            if !field_def.optional && !matches!(field_def.field_type, FieldType::Option(_)) {
                // If there's no VALUE clause and field is not optional, it's required
                if field_def.value.is_none() {
                    required_fields.push(field_name.clone());
                }
            }
        }

        // Add reverse relationship fields as virtual fields (for code generation)
        if let Some(reverse_rels) = reverse_rels {
            for rel in reverse_rels {
                if let Some(field_name) = rel.get("field").and_then(|f| f.as_str()) {
                    if let Some(related_table) = rel.get("table").and_then(|t| t.as_str()) {
                        // Add as an array field (reverse relationships are always many-to-one from the target's perspective)
                        properties.insert(
                            field_name.to_string(),
                            json!({
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "description": format!("Record ID of table: {}", related_table),
                                    "x-is-record-id": true
                                },
                                "description": format!("Reverse relationship: array of {} records", related_table),
                                "x-is-reverse-relationship": true
                            })
                        );
                        // Reverse relationship fields are optional (not in required_fields)
                    }
                }
            }
        }

        let mut definition = serde_json::Map::new();
        definition.insert("type".to_string(), Value::String("object".to_string()));
        definition.insert("properties".to_string(), Value::Object(properties));

        if !required_fields.is_empty() {
            definition.insert(
                "required".to_string(),
                Value::Array(required_fields.into_iter().map(Value::String).collect()),
            );
        }

        if table.schemafull {
            definition.insert("additionalProperties".to_string(), Value::Bool(false));
        }

        Value::Object(definition)
    }

    fn generate_field_schema(&self, field_type: &FieldType) -> Value {
        match field_type {
            FieldType::String => json!({
                "type": "string"
            }),
            FieldType::Int => json!({
                "type": "integer"
            }),
            FieldType::Float => json!({
                "type": "number"
            }),
            FieldType::Bool => json!({
                "type": "boolean"
            }),
            FieldType::Datetime => json!({
                "type": "string",
                "format": "date-time"
            }),
            FieldType::Duration => json!({
                "type": "string",
                "description": "ISO 8601 duration"
            }),
            FieldType::Bytes => json!({
                "type": "string",
                "contentEncoding": "base64",
                "description": "Binary blob (transported as base64 in JSON)"
            }),
            FieldType::Object => json!({
                "type": "object",
                "additionalProperties": true,
                "description": "Free-form object (FLEXIBLE on the SurrealDB side)"
            }),
            FieldType::Array(inner) => {
                let items = self.generate_field_schema(inner);
                json!({
                    "type": "array",
                    "items": items
                })
            }
            FieldType::Record(table_name) => {
                if table_name == "any" {
                    json!({
                        "type": "string",
                        "description": "Record ID"
                    })
                } else {
                    json!({
                        "type": "string",
                        "description": format!("Record ID of table: {}", table_name),
                        "pattern": format!("^{}:", table_name)
                    })
                }
            }
            FieldType::Option(inner) => {
                let mut schema = self.generate_field_schema(inner);
                if let Value::Object(ref mut obj) = schema {
                    // Make the field nullable
                    if let Some(Value::String(type_str)) = obj.get("type") {
                        obj.insert("type".to_string(), json!([type_str.clone(), "null"]));
                    } else {
                        // If type is already an array or missing, add null
                        obj.insert("type".to_string(), json!(["string", "null"]));
                    }
                }
                schema
            }
            FieldType::Any => json!({
                "description": "Any type"
            }),
        }
    }

    /// Check if a field type is a datetime (contains Datetime type anywhere in the type hierarchy)
    fn is_field_datetime(field_type: &FieldType) -> bool {
        match field_type {
            FieldType::Datetime => true,
            FieldType::Option(inner) => Self::is_field_datetime(inner),
            FieldType::Array(inner) => Self::is_field_datetime(inner),
            _ => false,
        }
    }

    fn is_field_bytes(field_type: &FieldType) -> bool {
        match field_type {
            FieldType::Bytes => true,
            FieldType::Option(inner) => Self::is_field_bytes(inner),
            FieldType::Array(inner) => Self::is_field_bytes(inner),
            _ => false,
        }
    }

    /// Pluralize a table name for reverse relationship field names
    /// Simple pluralization: add 's' or 'es' based on common rules
    fn pluralize_table_name(table_name: &str) -> String {
        // Handle common irregular plurals
        match table_name {
            "user" => "users".to_string(),
            "person" => "people".to_string(),
            "child" => "children".to_string(),
            "mouse" => "mice".to_string(),
            _ => {
                // Simple pluralization rules
                if table_name.ends_with('s')
                    || table_name.ends_with("sh")
                    || table_name.ends_with("ch")
                    || table_name.ends_with('x')
                    || table_name.ends_with('z')
                {
                    format!("{}es", table_name)
                } else if table_name.ends_with('y') && table_name.len() > 1 {
                    // Check if the letter before 'y' is a consonant
                    let chars: Vec<char> = table_name.chars().collect();
                    let second_last = chars[chars.len() - 2];
                    if !matches!(second_last, 'a' | 'e' | 'i' | 'o' | 'u') {
                        format!("{}ies", &table_name[..table_name.len() - 1])
                    } else {
                        format!("{}s", table_name)
                    }
                } else if table_name.ends_with("fe") {
                    format!("{}ves", &table_name[..table_name.len() - 2])
                } else if table_name.ends_with('f') {
                    format!("{}ves", &table_name[..table_name.len() - 1])
                } else {
                    format!("{}s", table_name)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_or_record_union_keeps_its_link_target() {
        // whitepawn's `game.white`: a plain string during a migration window,
        // a registry link afterwards. The column type must stay `string`
        // (no coercion, no id pattern) but the link target has to survive so
        // codegen emits the relationship `.related('white')` needs.
        let schema = r#"
DEFINE TABLE player_name SCHEMALESS;
DEFINE FIELD name ON TABLE player_name TYPE string;
DEFINE TABLE game SCHEMALESS;
DEFINE FIELD white ON TABLE game TYPE string | record<player_name>;
DEFINE FIELD owner ON TABLE game TYPE record<player_name>;
DEFINE FIELD note ON TABLE game TYPE option<string | record<player_name>>;
DEFINE FIELD mixed ON TABLE game TYPE record<player_name> | record<game>;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();
        let js = JsonSchemaGenerator::new().generate(&parser);
        let props = js
            .definitions
            .get("game")
            .and_then(|d| d.get("properties"))
            .and_then(|p| p.as_object())
            .expect("game properties");

        let white = props.get("white").unwrap();
        assert_eq!(white.get("type").and_then(|t| t.as_str()), Some("string"));
        assert_eq!(
            white.get("x-record-ref").and_then(|t| t.as_str()),
            Some("player_name"),
            "the union's record half must be recorded: {white}"
        );
        assert!(white.get("pattern").is_none(), "a union is not a record column");

        // `option<...>` of the same union carries it too.
        assert_eq!(
            props.get("note").unwrap().get("x-record-ref").and_then(|t| t.as_str()),
            Some("player_name")
        );
        // A real record column already says so through its description.
        assert!(props.get("owner").unwrap().get("x-record-ref").is_none());
        // Two different record targets are ambiguous: no ref.
        assert!(props.get("mixed").unwrap().get("x-record-ref").is_none());
    }

    /// The `spky api add` outbox template defines `errors[*]`; as a property
    /// it became the uncompilable TS column `errors[*]: ...`.
    #[test]
    fn sub_path_fields_are_not_properties() {
        let schema = r#"
DEFINE TABLE job SCHEMAFULL;
DEFINE FIELD errors ON TABLE job TYPE array<object> DEFAULT [];
DEFINE FIELD OVERWRITE errors[*] ON TABLE job TYPE object FLEXIBLE;
DEFINE FIELD settings ON TABLE job TYPE option<object>;
DEFINE FIELD settings.theme ON TABLE job TYPE option<string>;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();
        let js = JsonSchemaGenerator::new().generate(&parser);
        let props = js
            .definitions
            .get("job")
            .and_then(|d| d.get("properties"))
            .and_then(|p| p.as_object())
            .expect("job properties");
        assert!(props.contains_key("errors") && props.contains_key("settings"), "{props:?}");
        assert!(
            !props.keys().any(|k| k.contains('[') || k.contains('.')),
            "sub-path fields leaked: {:?}",
            props.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn nosync_table_omitted_from_definitions_and_relations() {
        let schema = r#"
DEFINE TABLE public SCHEMALESS;
DEFINE FIELD owner ON TABLE public TYPE record<secrets>;

-- @nosync
DEFINE TABLE secrets SCHEMALESS;
DEFINE FIELD token ON TABLE secrets TYPE string;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();

        let js = JsonSchemaGenerator::new().generate(&parser);

        // @nosync table has no definition and no property.
        assert!(js.definitions.contains_key("public"));
        assert!(
            !js.definitions.contains_key("secrets"),
            "nosync table must be omitted"
        );
        assert!(!js.properties.as_ref().unwrap().contains_key("secrets"));

        // No relationship (forward or reverse) references the nosync table.
        if let Some(rels) = js.definitions.get("Relationships") {
            let s = serde_json::to_string(rels).unwrap();
            assert!(
                !s.contains("secrets"),
                "relationship pointing at nosync table must be dropped: {s}"
            );
        }
        // The nosync table is not listed as a relation table either.
        if let Some(rt) = js.definitions.get("RelationTables") {
            let s = serde_json::to_string(rt).unwrap();
            assert!(!s.contains("secrets"));
        }
    }

    #[test]
    fn nosync_field_is_omitted_but_opaque_field_is_emitted_with_its_flag() {
        let schema = r#"
DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD title ON TABLE doc TYPE string;

-- @nosync
DEFINE FIELD import_batch ON TABLE doc TYPE string;

-- @opaque
DEFINE FIELD thumbnail ON TABLE doc TYPE bytes;
"#;
        let mut parser = SchemaParser::new();
        parser.parse_file(schema).unwrap();

        let js = JsonSchemaGenerator::new().generate(&parser);
        let props = js
            .definitions
            .get("doc")
            .and_then(|d| d.get("properties"))
            .and_then(|p| p.as_object())
            .expect("doc properties");

        assert!(props.contains_key("title"));
        assert!(
            !props.contains_key("import_batch"),
            "@nosync field must not reach generated types"
        );
        // @opaque is synced, so it MUST be generated — flagged so the query
        // builder can reject filtering on it.
        let thumbnail = props.get("thumbnail").expect("@opaque field must be kept");
        assert!(
            thumbnail.get("x-opaque").is_some(),
            "@opaque field must carry x-opaque: {thumbnail}"
        );
        assert!(
            thumbnail.get("x-is-bytes").is_some(),
            "type metadata must still be emitted: {thumbnail}"
        );
    }
}
