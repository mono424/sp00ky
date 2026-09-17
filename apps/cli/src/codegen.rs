use crate::backend::BackendDefinition;
use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::fs;
use std::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    JsonSchema,
    Typescript,
    Dart,
    Surql,
}

impl OutputFormat {
    pub fn from_extension(path: &str) -> Option<Self> {
        if path.ends_with(".json") {
            Some(Self::JsonSchema)
        } else if path.ends_with(".ts") {
            Some(Self::Typescript)
        } else if path.ends_with(".dart") {
            Some(Self::Dart)
        } else if path.ends_with(".surql") {
            Some(Self::Surql)
        } else {
            None
        }
    }
}

pub struct CodeGenerator {
    format: OutputFormat,
    include_header: bool,
    include_modules: bool,
    /// `sync.queryAllowlist` of the manifest, emitted as `schema.policy` so the
    /// client SDK can gate its raw-remote escape hatches to match the SSP.
    query_allowlist: crate::backend::QueryAllowlistMode,
    /// `impersonation.enabled`, emitted as `schema.policy.impersonation` so the
    /// DevTools only offer impersonation where the server supports it.
    impersonation: bool,
}

impl CodeGenerator {
    pub fn new(format: OutputFormat, include_header: bool, include_modules: bool) -> Self {
        Self {
            format,
            include_header,
            include_modules,
            query_allowlist: crate::backend::QueryAllowlistMode::Off,
            impersonation: false,
        }
    }

    pub fn with_impersonation(mut self, enabled: bool) -> Self {
        self.impersonation = enabled;
        self
    }

    pub fn with_query_allowlist(mut self, mode: crate::backend::QueryAllowlistMode) -> Self {
        self.query_allowlist = mode;
        self
    }

    pub fn new_with_header(format: OutputFormat, include_header: bool) -> Self {
        Self::new(format, include_header, true) // Default to true for backward compat
    }

    #[allow(dead_code)]
    pub fn generate(&self, json_schema_content: &str, _top_level_name: &str) -> Result<String> {
        self.generate_with_schema(json_schema_content, _top_level_name, None, None, None)
    }

    pub fn generate_with_schema(
        &self,
        json_schema_content: &str,
        _top_level_name: &str,
        raw_schema: Option<&str>,
        sp00ky_events: Option<&str>,
        backend_definitions: Option<&BTreeMap<String, BackendDefinition>>,
    ) -> Result<String> {
        let mut content = match self.format {
            OutputFormat::JsonSchema => json_schema_content.to_string(),
            OutputFormat::Typescript => {
                self.generate_typescript(json_schema_content, backend_definitions)?
            }
            OutputFormat::Dart => self.generate_dart(json_schema_content)?,
            OutputFormat::Surql => {
                let mut schema = String::new();

                if self.include_modules {
                    // Add module definitions section
                    schema.push_str("\n-- ==================================================\n");
                    schema.push_str("-- SURREALISM MODULES\n");
                    schema.push_str("-- ==================================================\n");
                    schema.push_str("\n-- Define bucket for module files\n");
                    schema.push_str(
                        "DEFINE BUCKET IF NOT EXISTS modules BACKEND \"file:/modules\";\n\n",
                    );
                    schema.push_str("-- Define the XOR module\n");
                    schema
                        .push_str("DEFINE MODULE mod::xor AS f\"modules:/xor_module.surli\";\n\n");
                    // TODO: re-enable when ssp API stabilizes
                    // schema.push_str("-- Define the SSP module\n");
                    // schema.push_str("DEFINE MODULE mod::ssp AS f\"modules:/ssp_module.surli\";\n\n");
                }

                // Add the main schema content
                schema.push_str(raw_schema.unwrap_or(""));

                // Add sp00ky events at the end
                if let Some(events) = sp00ky_events {
                    schema.push_str(events);
                }
                schema
            }
        };

        if self.include_header {
            let header = self.generate_header();
            content = format!("{}\n{}", header, content);
        }

        // Add raw schema constant for TypeScript and Dart
        if let Some(schema) = raw_schema {
            let full_schema = if let Some(events) = sp00ky_events {
                format!("{}\n\n{}", schema, events)
            } else {
                schema.to_string()
            };
            content = self.add_schema_constant(content, &full_schema)?;
        }

        Ok(content)
    }

    fn generate_typescript(
        &self,
        json_schema_content: &str,
        backend_definitions: Option<&BTreeMap<String, BackendDefinition>>,
    ) -> Result<String> {
        println!("Generating TypeScript from JSON schema...");

        // Generate only the schema metadata - no interfaces
        println!("Adding schema metadata...");
        let output = self.add_schema_metadata("", json_schema_content, backend_definitions)?;

        Ok(output)
    }

    fn generate_dart(&self, json_schema_content: &str) -> Result<String> {
        self.run_quicktype(json_schema_content, "dart")
    }

    fn generate_header(&self) -> String {
        match self.format {
            OutputFormat::Typescript => {
                "// This file is auto-generated. Do not edit manually.\n// Generated by syncgen - any changes will be overwritten.\n".to_string()
            }
            OutputFormat::Dart => {
                "// This file is auto-generated. Do not edit manually.\n// Generated by syncgen - any changes will be overwritten.\n".to_string()
            }
            OutputFormat::JsonSchema => {
                // JSON doesn't support comments, so we skip the header
                String::new()
            }
            OutputFormat::Surql => {
                "-- This file is auto-generated. Do not edit manually.\n-- Generated by syncgen - any changes will be overwritten.\n".to_string()
            }
        }
    }

    fn run_quicktype(&self, json_schema_content: &str, lang: &str) -> Result<String> {
        // Create a temporary file for the JSON schema
        let temp_dir = std::env::temp_dir();
        let schema_path = temp_dir.join("schema.json");

        fs::write(&schema_path, json_schema_content)
            .context("Failed to write temporary schema file")?;

        // Build arguments based on language
        let mut args = vec!["-y", "quicktype", "--src-lang", "schema", "--lang", lang];

        // Add --just-types for TypeScript to exclude runtime validation code
        if lang == "typescript" {
            args.push("--just-types");
        }

        args.push(schema_path.to_str().unwrap());

        // Run quicktype
        let output = Command::new("npx")
            .args(&args)
            .output()
            .context("Failed to execute quicktype. Make sure npx is installed.")?;

        // Clean up temp file
        let _ = fs::remove_file(&schema_path);

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("quicktype failed: {}", stderr);
        }

        let code =
            String::from_utf8(output.stdout).context("quicktype output is not valid UTF-8")?;

        Ok(code)
    }

    #[allow(dead_code)]
    fn remove_top_level_schema_interface(&self, content: String) -> String {
        let lines: Vec<&str> = content.lines().collect();
        let mut result = Vec::new();
        let mut i = 0;
        let mut skip_until_closing_brace = false;
        let mut brace_count = 0;

        while i < lines.len() {
            let line = lines[i];

            // Check if this is the top-level Schema interface with an exact match
            // We only want to remove "export interface Schema {" not "export interface SchemaFoo {"
            if line.trim() == "export interface Schema {"
                || (line.trim().starts_with("export interface Schema")
                    && (line.trim().ends_with("{") || !line.contains("{")))
            {
                // Check if it's exactly "Schema" and not "SchemaSomething"
                let after_interface = line.split("export interface").nth(1).unwrap_or("");
                let interface_name = after_interface
                    .trim()
                    .split_whitespace()
                    .next()
                    .unwrap_or("");

                if interface_name == "Schema" || interface_name == "Schema{" {
                    // Start skipping lines and track braces
                    skip_until_closing_brace = true;
                    brace_count = 0;

                    // Count braces in the current line
                    for ch in line.chars() {
                        if ch == '{' {
                            brace_count += 1;
                        } else if ch == '}' {
                            brace_count -= 1;
                        }
                    }

                    i += 1;

                    // If we already closed on the same line, we're done
                    if brace_count == 0 && line.contains("}") {
                        skip_until_closing_brace = false;
                        // Skip any trailing empty lines
                        while i < lines.len() && lines[i].trim().is_empty() {
                            i += 1;
                        }
                    }
                    continue;
                }
            }

            if skip_until_closing_brace {
                // Count braces to find the end of the interface
                for ch in line.chars() {
                    if ch == '{' {
                        brace_count += 1;
                    } else if ch == '}' {
                        brace_count -= 1;
                    }
                }

                // If we've closed all braces, we're done with this interface
                if brace_count <= 0 {
                    skip_until_closing_brace = false;
                    i += 1;
                    // Skip any trailing empty lines
                    while i < lines.len() && lines[i].trim().is_empty() {
                        i += 1;
                    }
                    continue;
                }

                i += 1;
                continue;
            }

            result.push(line);
            i += 1;
        }

        result.join("\n")
    }

    #[allow(dead_code)]
    fn add_index_signature_to_interfaces(&self, content: String) -> String {
        // NOTE: This function is now used to REMOVE index signatures, not add them.
        // Index signatures like [property: string]: any are intentionally removed
        // because they break TypeScript's type inference when using mapped types.
        // This allows proper type narrowing in generic contexts like TableQueries<Schema>.

        let lines: Vec<&str> = content.lines().collect();
        let mut result = Vec::new();

        for line in lines {
            // Skip lines that contain index signatures
            if line.trim().starts_with("[property:") && line.contains("]:") {
                continue;
            }
            result.push(line);
        }

        result.join("\n")
    }

    fn add_schema_metadata(
        &self,
        content: &str,
        json_schema_content: &str,
        backend_definitions: Option<&BTreeMap<String, BackendDefinition>>,
    ) -> Result<String> {
        // Parse JSON schema to extract schema information
        let schema: serde_json::Value =
            serde_json::from_str(json_schema_content).context("Failed to parse JSON schema")?;

        // Pre-compute relationships so per-table JSDoc can summarize them
        let table_relationships = self.extract_table_relationships(&schema)?;

        // Build tables array - start without empty line prefix
        let mut tables_lines = vec![
            "export const schema = {".to_string(),
            "  tables: [".to_string(),
        ];

        if let Some(definitions) = schema.get("definitions") {
            if let serde_json::Value::Object(defs_obj) = definitions {
                // Process each table (skip Relationships and RelationTables)
                for (table_name, table_def) in defs_obj {
                    if table_name == "Relationships"
                        || table_name == "RelationTables"
                        || table_name == "Access"
                        || table_name == "Buckets"
                        || table_name.starts_with("_00_")
                    {
                        continue;
                    }

                    // Per-table JSDoc — surfaces relationships and a useQuery example so
                    // hover-docs in IDEs and LLM reads of this file get inline context.
                    for line in
                        self.table_jsdoc_lines(table_name, table_relationships.get(table_name))
                    {
                        tables_lines.push(line);
                    }

                    tables_lines.push("    {".to_string());
                    tables_lines.push(format!("      name: '{}' as const,", table_name));
                    tables_lines.push("      columns: {".to_string());

                    // Extract columns from properties
                    if let Some(props) = table_def.get("properties") {
                        if let serde_json::Value::Object(props_obj) = props {
                            for (col_name, col_def) in props_obj {
                                let col_type = self.map_json_schema_type_to_value_type(col_def);
                                let is_optional = self.is_field_optional(table_def, col_name);
                                let is_record_id = col_def
                                    .get("x-is-record-id")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);
                                let is_datetime = col_def
                                    .get("x-is-datetime")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);
                                let is_bytes = col_def
                                    .get("x-is-bytes")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);
                                let crdt_variant = col_def
                                    .get("x-crdt")
                                    .and_then(|v| v.as_str())
                                    .filter(|s| !s.is_empty());
                                let has_cursor = col_def.get("x-cursor").is_some();
                                let is_parent = col_def.get("x-parent").is_some();
                                let is_opaque = col_def.get("x-opaque").is_some();

                                let clean_col_name = col_name.replace("`", "");

                                // Per-column JSDoc — explains semantics that aren't obvious
                                // from the runtime flags alone (CRDT requires useCrdtField,
                                // @parent fields are auto-populated, etc.).
                                for line in self.column_jsdoc_lines(
                                    &clean_col_name,
                                    col_type,
                                    is_optional,
                                    is_record_id,
                                    is_datetime,
                                    crdt_variant,
                                    is_parent,
                                    is_opaque,
                                ) {
                                    tables_lines.push(line);
                                }

                                let mut flags = Vec::new();
                                if is_record_id {
                                    flags.push("recordId: true".to_string());
                                }
                                if is_datetime {
                                    flags.push("dateTime: true".to_string());
                                }
                                if is_bytes {
                                    flags.push("bytes: true".to_string());
                                }
                                if let Some(crdt) = crdt_variant {
                                    flags.push(format!("crdt: '{}' as const", crdt));
                                }
                                if has_cursor {
                                    flags.push("cursor: true".to_string());
                                }
                                // `@opaque`: synced to the client, but the server
                                // never holds the value, so it can never be
                                // filtered/ordered on. The query builder rejects
                                // it at runtime using this flag.
                                if is_opaque {
                                    flags.push("opaque: true".to_string());
                                }
                                // `array<T>`: `col_type` is already the element
                                // type (via map_json_schema_type_to_value_type),
                                // so flag it so the runtime value is `T[]`.
                                if self.array_element_type(col_def).is_some() {
                                    flags.push("array: true".to_string());
                                }

                                // For `bytes` fields the runtime value is a
                                // Uint8Array, not a string. The JSON-schema
                                // `type` stays `string` (with
                                // contentEncoding=base64) for spec
                                // consumers, so we override here at
                                // codegen time.
                                let runtime_type = if is_bytes { "Uint8Array" } else { col_type };

                                if flags.is_empty() {
                                    tables_lines.push(format!(
                                        "        {}: {{ type: '{}' as const, optional: {} }},",
                                        clean_col_name, runtime_type, is_optional
                                    ));
                                } else {
                                    tables_lines.push(format!(
                                        "        {}: {{ type: '{}' as const, {}, optional: {} }},",
                                        clean_col_name,
                                        runtime_type,
                                        flags.join(", "),
                                        is_optional
                                    ));
                                }
                            }
                        }
                    }

                    tables_lines.push("      },".to_string());
                    tables_lines.push("      primaryKey: ['id'] as const".to_string());
                    tables_lines.push("    },".to_string());
                }
            }
        }

        tables_lines.push("  ],".to_string());

        // Build relationships array (reusing the table_relationships computed above for JSDoc)
        tables_lines.push("  relationships: [".to_string());

        for (table_name, rels) in &table_relationships {
            for (field_name, related_table, cardinality) in rels {
                tables_lines.push("    {".to_string());
                tables_lines.push(format!("      from: '{}' as const,", table_name));
                tables_lines.push(format!("      field: '{}' as const,", field_name));
                tables_lines.push(format!("      to: '{}' as const,", related_table));
                tables_lines.push(format!("      cardinality: '{}' as const", cardinality));
                tables_lines.push("    },".to_string());
            }
        }

        tables_lines.push("  ],".to_string());

        // Build access array
        tables_lines.push("  access: {".to_string());

        if let Some(definitions) = schema.get("definitions") {
            if let Some(access) = definitions.get("Access") {
                if let Some(const_val) = access.get("const") {
                    if let serde_json::Value::Object(map) = const_val {
                        for (name, access_val) in map {
                            if let Ok(val_str) = serde_json::to_string(access_val) {
                                tables_lines.push(format!("    {}: {},", name, val_str));
                            }
                        }
                    }
                }
            }
        }

        tables_lines.push("  },".to_string());

        // Build buckets array (objects with constraints)
        tables_lines.push("  buckets: [".to_string());
        if let Some(definitions) = schema.get("definitions") {
            if let Some(buckets) = definitions.get("Buckets") {
                if let Some(const_val) = buckets.get("const") {
                    if let serde_json::Value::Array(arr) = const_val {
                        for bucket in arr {
                            if let serde_json::Value::Object(obj) = bucket {
                                tables_lines.push("    {".to_string());
                                if let Some(serde_json::Value::String(name)) = obj.get("name") {
                                    tables_lines.push(format!("      name: '{}' as const,", name));
                                }
                                if let Some(max_size) = obj.get("maxSize") {
                                    tables_lines.push(format!("      maxSize: {},", max_size));
                                }
                                if let Some(serde_json::Value::Array(exts)) =
                                    obj.get("allowedExtensions")
                                {
                                    let ext_strs: Vec<String> = exts
                                        .iter()
                                        .filter_map(|e| e.as_str().map(|s| format!("'{}'", s)))
                                        .collect();
                                    tables_lines.push(format!(
                                        "      allowedExtensions: [{}] as const,",
                                        ext_strs.join(", ")
                                    ));
                                }
                                if let Some(serde_json::Value::Bool(auth)) =
                                    obj.get("pathPrefixAuth")
                                {
                                    tables_lines.push(format!("      pathPrefixAuth: {},", auth));
                                }
                                tables_lines.push("    },".to_string());
                            } else if let serde_json::Value::String(s) = bucket {
                                // Backwards compat: plain string bucket
                                tables_lines.push("    {".to_string());
                                tables_lines.push(format!("      name: '{}' as const,", s));
                                tables_lines.push("    },".to_string());
                            }
                        }
                    }
                }
            }
        }
        tables_lines.push("  ],".to_string());

        if let Some(backends) = backend_definitions {
            tables_lines.push("  backends: {".to_string());
            for (backend_name, backend_def) in backends {
                tables_lines.push(format!("    \"{}\": {{", backend_name));

                if let Some(table) = &backend_def.outbox_table {
                    tables_lines.push(format!("      outboxTable: '{}' as const,", table));
                }

                tables_lines.push("      routes: {".to_string());
                for (route_path, route_def) in &backend_def.routes {
                    tables_lines.push(format!("        \"{}\": {{", route_path));
                    tables_lines.push("          args: {".to_string());
                    for (arg_name, arg_def) in &route_def.args {
                        tables_lines.push(format!("            \"{}\": {{", arg_name));
                        tables_lines.push(format!(
                            "              type: '{}' as const,",
                            arg_def.arg_type
                        ));
                        tables_lines.push(format!(
                            "              optional: {} as const",
                            !arg_def.required
                        ));
                        tables_lines.push("            },".to_string());
                    }
                    tables_lines.push("          }".to_string());
                    tables_lines.push("        },".to_string());
                }
                tables_lines.push("      }".to_string());
                tables_lines.push("    },".to_string());
            }
            tables_lines.push("  },".to_string());
        }

        // Sync policy the SSP enforces, mirrored so the client SDK refuses
        // `useRemote`/`remoteQuery`/`queryRaw` unless `allowRawRemote` is set.
        tables_lines.push("  policy: {".to_string());
        tables_lines.push(format!(
            "    queryAllowlist: '{}' as const,",
            self.query_allowlist.as_str()
        ));
        tables_lines.push(format!("    impersonation: {} as const,", self.impersonation));
        tables_lines.push("  },".to_string());
        tables_lines.push("} as const;".to_string());
        tables_lines.push("".to_string());

        // Add derived type
        tables_lines.push("export type SchemaDefinition = typeof schema;".to_string());
        tables_lines.push("".to_string());

        if content.is_empty() {
            Ok(tables_lines.join("\n"))
        } else {
            Ok(format!("{}\n\n{}", content, tables_lines.join("\n")))
        }
    }

    /// Build JSDoc lines for a table entry in the generated `schema.tables` array.
    /// Indented to sit at the `    {` level (4 spaces).
    fn table_jsdoc_lines(
        &self,
        table_name: &str,
        relationships: Option<&Vec<(String, String, String)>>,
    ) -> Vec<String> {
        let mut lines = vec!["    /**".to_string()];
        lines.push(format!("     * `{}` table.", table_name));

        if let Some(rels) = relationships {
            if !rels.is_empty() {
                lines.push("     *".to_string());
                lines.push("     * Relationships:".to_string());
                for (field, target, cardinality) in rels {
                    lines.push(format!(
                        "     * - `{}` → `{}` ({})",
                        field, target, cardinality
                    ));
                }
            }
        }

        lines.push("     *".to_string());
        lines.push("     * @example".to_string());
        lines.push(format!(
            "     * const rows = useQuery(() => db.query('{}').orderBy('id', 'asc').limit(20).build());",
            table_name
        ));
        lines.push("     */".to_string());
        lines
    }

    /// Build JSDoc lines for a column entry inside `columns: { ... }`.
    /// Indented to sit at the `        ` level (8 spaces).
    fn column_jsdoc_lines(
        &self,
        col_name: &str,
        col_type: &str,
        is_optional: bool,
        is_record_id: bool,
        is_datetime: bool,
        crdt_variant: Option<&str>,
        is_parent: bool,
        is_opaque: bool,
    ) -> Vec<String> {
        // Skip JSDoc for plain columns to keep generated files compact —
        // only emit when there's something semantically interesting to say.
        if !is_record_id && !is_datetime && crdt_variant.is_none() && !is_parent && !is_opaque {
            return Vec::new();
        }

        let mut lines = vec!["        /**".to_string()];
        let optional_marker = if is_optional { "?" } else { "" };
        lines.push(format!(
            "         * `{}{}` — {}",
            col_name, optional_marker, col_type
        ));

        if is_record_id {
            lines.push("         *".to_string());
            lines.push("         * Record ID. Pass the full `'<table>:<id>'` string when reading or writing.".to_string());
        }
        if is_datetime {
            lines.push("         *".to_string());
            lines.push("         * ISO-8601 datetime string.".to_string());
        }
        if let Some(variant) = crdt_variant {
            lines.push("         *".to_string());
            lines.push(format!(
                "         * `@crdt {}` — collaborative field. Read with `useCrdtField`, never `useQuery`.",
                variant
            ));
            lines.push("         * Writes should pass `{ debounced: true }` to `db.update` so rapid keystrokes coalesce.".to_string());
        }
        if is_parent {
            lines.push("         *".to_string());
            lines.push("         * `@parent` — auto-populated server-side from the auth context. Do not write from client code.".to_string());
        }
        if is_opaque {
            lines.push("         *".to_string());
            lines.push("         * `@opaque` — synced to the client and readable here, but the sync".to_string());
            lines.push("         * engine never stores the value server-side. It cannot be used in".to_string());
            lines.push("         * `where`, `orderBy`, joins, or table permissions; doing so throws.".to_string());
        }

        lines.push("         */".to_string());
        lines
    }

    fn map_json_schema_type_to_value_type(&self, field_def: &serde_json::Value) -> &'static str {
        // `array<T>` columns: the array itself isn't a scalar ValueType, so map to
        // the ELEMENT type here and let `array_element_type` flag it as an array.
        if let Some(elem) = self.array_element_type(field_def) {
            return elem;
        }
        if let Some(field_type) = field_def.get("type") {
            match field_type {
                serde_json::Value::String(type_str) => match type_str.as_str() {
                    "string" => "string",
                    "number" | "integer" => "number",
                    "boolean" => "boolean",
                    _ => "string",
                },
                serde_json::Value::Array(types) => {
                    // Handle union types like ["string", "null"]
                    for t in types {
                        if let serde_json::Value::String(type_str) = t {
                            if type_str != "null" {
                                return match type_str.as_str() {
                                    "string" => "string",
                                    "number" | "integer" => "number",
                                    "boolean" => "boolean",
                                    _ => "string",
                                };
                            }
                        }
                    }
                    "string"
                }
                _ => "string",
            }
        } else {
            "string"
        }
    }

    /// If `field_def` describes an `array<T>` column (`type: "array"`, or a
    /// nullable `["array", "null"]` union), return the element ValueType (mapped
    /// from the JSON-schema `items`). Returns `None` for non-array columns.
    fn array_element_type(&self, field_def: &serde_json::Value) -> Option<&'static str> {
        let is_array = match field_def.get("type") {
            Some(serde_json::Value::String(s)) => s == "array",
            Some(serde_json::Value::Array(types)) => {
                types.iter().any(|t| t.as_str() == Some("array"))
            }
            _ => false,
        };
        if !is_array {
            return None;
        }
        // Map the element schema; default to `string` when `items` is absent.
        Some(
            field_def
                .get("items")
                .map(|items| self.map_json_schema_type_to_value_type(items))
                .unwrap_or("string"),
        )
    }

    fn is_field_optional(&self, table_def: &serde_json::Value, field_name: &str) -> bool {
        // Check if field is in required array
        if let Some(required) = table_def.get("required") {
            if let serde_json::Value::Array(req_array) = required {
                return !req_array.iter().any(|v| {
                    if let serde_json::Value::String(s) = v {
                        s == field_name
                    } else {
                        false
                    }
                });
            }
        }
        true // If no required array, assume optional
    }

    fn extract_table_relationships(
        &self,
        schema: &serde_json::Value,
    ) -> Result<std::collections::BTreeMap<String, Vec<(String, String, String)>>> {
        let mut table_relationships = std::collections::BTreeMap::new();

        if let Some(defs) = schema.get("definitions") {
            if let serde_json::Value::Object(defs_obj) = defs {
                // The set of real, generated tables. A field that references a
                // table NOT in this set (e.g. a `record<...>` pointing at a
                // @nosync table, which is omitted from the schema) must not
                // produce a relationship.
                let known_tables: std::collections::HashSet<&str> = defs_obj
                    .keys()
                    .filter(|n| {
                        n.as_str() != "Relationships"
                            && n.as_str() != "RelationTables"
                            && n.as_str() != "Access"
                            && n.as_str() != "Buckets"
                            && !n.starts_with("_00_")
                    })
                    .map(|n| n.as_str())
                    .collect();

                for (table_name, table_def) in defs_obj {
                    if table_name == "Relationships"
                        || table_name == "RelationTables"
                        || table_name == "Buckets"
                        || table_name.starts_with("_00_")
                    {
                        continue;
                    }

                    if let Some(props) = table_def.get("properties") {
                        if let serde_json::Value::Object(props_obj) = props {
                            let mut table_rels = Vec::new();

                            for (field_name, field_def) in props_obj {
                                let is_array = if let Some(array_type) = field_def.get("type") {
                                    if array_type == "array" {
                                        true
                                    } else if let serde_json::Value::Array(types) = array_type {
                                        types.iter().any(|t| t == "array")
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                };

                                if is_array {
                                    if let Some(items) = field_def.get("items") {
                                        if let Some(desc) = items.get("description") {
                                            if let Some(desc_str) = desc.as_str() {
                                                if desc_str.starts_with("Record ID of table: ") {
                                                    let related_table = desc_str
                                                        .replace("Record ID of table: ", "");
                                                    let actual_target =
                                                        if related_table == "commented_on" {
                                                            "comment".to_string()
                                                        } else {
                                                            related_table.clone()
                                                        };
                                                    if known_tables.contains(actual_target.as_str())
                                                    {
                                                        table_rels.push((
                                                            field_name.clone(),
                                                            actual_target,
                                                            "many".to_string(),
                                                        ));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    let mut pushed = false;
                                    if let Some(desc) = field_def.get("description") {
                                        if let Some(desc_str) = desc.as_str() {
                                            if desc_str.starts_with("Record ID of table: ") {
                                                let related_table =
                                                    desc_str.replace("Record ID of table: ", "");
                                                if known_tables.contains(related_table.as_str()) {
                                                    table_rels.push((
                                                        field_name.clone(),
                                                        related_table,
                                                        "one".to_string(),
                                                    ));
                                                    pushed = true;
                                                }
                                            }
                                        }
                                    }
                                    // A `string | record<t>` union column links to `t`
                                    // whenever it holds a link (json_schema `x-record-ref`).
                                    // The same to-one relationship, so `.related()` can
                                    // join the row while the column's own type stays
                                    // `string`.
                                    if !pushed {
                                        if let Some(t) =
                                            field_def.get("x-record-ref").and_then(|v| v.as_str())
                                        {
                                            if known_tables.contains(t) {
                                                table_rels.push((
                                                    field_name.clone(),
                                                    t.to_string(),
                                                    "one".to_string(),
                                                ));
                                            }
                                        }
                                    }
                                }
                            }

                            if !table_rels.is_empty() {
                                table_relationships.insert(table_name.clone(), table_rels);
                            }
                        }
                    }
                }
            }
        }

        // Reverse relationships are now added as virtual fields in table definitions,
        // so they will be extracted by the field-based extraction above

        Ok(table_relationships)
    }

    #[allow(dead_code)]
    fn add_relationships_interface(
        &self,
        content: &str,
        json_schema_content: &str,
    ) -> Result<String> {
        // Parse JSON schema to extract relationships
        let schema: serde_json::Value =
            serde_json::from_str(json_schema_content).context("Failed to parse JSON schema")?;

        let relationships = schema
            .get("definitions")
            .and_then(|defs| defs.get("Relationships"))
            .and_then(|rel| rel.get("properties"));

        // Extract relation tables
        let _relation_tables = schema
            .get("definitions")
            .and_then(|defs| defs.get("RelationTables"))
            .and_then(|rel| rel.get("const"));

        if let Some(relationships) = relationships {
            if let serde_json::Value::Object(rel_map) = relationships {
                // First, extract the actual relationship data from the schema
                let table_relationships = schema
                    .get("definitions")
                    .and_then(|defs| {
                        // Collect relationships from each table definition
                        let mut all_rels = std::collections::BTreeMap::new();
                        if let serde_json::Value::Object(defs_obj) = defs {
                            println!("Processing definitions, found {} tables", defs_obj.len());
                            for (table_name, table_def) in defs_obj {
                                if table_name == "Relationships" || table_name == "Buckets" || table_name.starts_with("_00_") {
                                    continue;
                                }
                                // Check if this table has any record fields
                                if let Some(props) = table_def.get("properties") {
                                    if let serde_json::Value::Object(props_obj) = props {
                                        let mut table_rels = Vec::new();
                                        for (field_name, field_def) in props_obj {
                                            // Check for array of records (including nullable arrays)
                                            let is_array = if let Some(array_type) = field_def.get("type") {
                                                if array_type == "array" {
                                                    true
                                                } else if let serde_json::Value::Array(types) = array_type {
                                                    // Handle nullable types like ["array", "null"]
                                                    types.iter().any(|t| t == "array")
                                                } else {
                                                    false
                                                }
                                            } else {
                                                false
                                            };
                                            
                                            // Debug output
                                            println!("Field: {}, Type: {:?}, Is array: {}", field_name, field_def.get("type"), is_array);

                                            if is_array {
                                                if let Some(items) = field_def.get("items") {
                                                    if let Some(desc) = items.get("description") {
                                                        if let Some(desc_str) = desc.as_str() {
                                                            if desc_str.starts_with("Record ID of table: ") {
                                                                let related_table = desc_str.replace("Record ID of table: ", "");
                                                                
                                                                // Check if this is a junction table (relation table)
                                                                // If so, we need to find the actual target table
                                                                let actual_target = if related_table.ends_with("_on") || related_table.contains("relation") {
                                                                    // This is likely a junction table, try to find the actual target
                                                                    // For now, we'll use a simple heuristic: if it's commented_on, target is comment
                                                                    if related_table == "commented_on" {
                                                                        "comment".to_string()
                                                                    } else {
                                                                        related_table.clone()
                                                                    }
                                                                } else {
                                                                    related_table.clone()
                                                                };
                                                                
                                                                // Debug output
                                                                println!("Field: {}, Original table: {}, Actual target: {}", field_name, related_table, actual_target);
                                                                
                                                                // This is a 1:many relationship (array)
                                                                table_rels.push((field_name.clone(), actual_target, "many".to_string()));
                                                            }
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Check if this is a single record field (1:1)
                                                if let Some(desc) = field_def.get("description") {
                                                    if let Some(desc_str) = desc.as_str() {
                                                        if desc_str.starts_with("Record ID of table: ") {
                                                            let related_table = desc_str.replace("Record ID of table: ", "");
                                                            // This is a 1:1 relationship (single value)
                                                            table_rels.push((field_name.clone(), related_table, "one".to_string()));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if !table_rels.is_empty() {
                                            all_rels.insert(table_name.clone(), table_rels);
                                        }
                                    }
                                }
                            }
                        }
                        Some(all_rels)
                    });

                // Now update the interface definitions to use proper relationship types
                let updated_content = if let Some(ref table_rels) = table_relationships {
                    self.update_interface_relationships(content, table_rels)?
                } else {
                    content.to_string()
                };

                // Generate the interface lines
                let mut interface_lines = vec![
                    "/**".to_string(),
                    " * Relationship definition for a single relationship field".to_string(),
                    " */".to_string(),
                    "export interface RelationshipDefinition<Model = any> {".to_string(),
                    "    /** The related model type */".to_string(),
                    "    model: Model;".to_string(),
                    "    /** The related table name */".to_string(),
                    "    table: string;".to_string(),
                    "    /** Whether this is a 1:1 or 1:many relationship */".to_string(),
                    "    cardinality: \"one\" | \"many\";".to_string(),
                    "}".to_string(),
                    "".to_string(),
                    "/**".to_string(),
                    " * Relationships between tables - nested object structure".to_string(),
                    " * Maps each table to its relationship fields with their definitions"
                        .to_string(),
                    " */".to_string(),
                    "export interface Relationships {".to_string(),
                ];

                // Generate nested interface structure
                if let Some(ref table_rels) = table_relationships {
                    for (table_name, rels) in table_rels {
                        if rels.is_empty() {
                            interface_lines.push(format!("    {}: {{}};", table_name));
                        } else {
                            interface_lines.push(format!("    {}: {{", table_name));
                            for (field_name, related_table, cardinality) in rels {
                                interface_lines.push(format!("        {}: {{", field_name));
                                interface_lines.push(format!(
                                    "            model: SchemaDefinition[\"{}\"];",
                                    related_table
                                ));
                                interface_lines
                                    .push(format!("            table: \"{}\";", related_table));
                                interface_lines
                                    .push(format!("            cardinality: \"{}\";", cardinality));
                                interface_lines.push("        };".to_string());
                            }
                            interface_lines.push("    };".to_string());
                        }
                    }
                } else {
                    for (table_name, _rel_data) in rel_map {
                        interface_lines.push(format!("    {}: {{}};", table_name));
                    }
                }

                interface_lines.push("}".to_string());
                interface_lines.push("".to_string());

                // RELATIONSHIPS constant removed - relationships are type-only now
                // The Relationships interface above provides type safety without runtime overhead

                // RELATION_TABLES constant also removed - not needed for runtime

                return Ok(format!(
                    "{}\n\n{}\n",
                    updated_content,
                    interface_lines.join("\n")
                ));
            }
        }

        // No relationships found, return original content
        Ok(content.to_string())
    }

    #[allow(dead_code)]
    fn update_interface_relationships(
        &self,
        content: &str,
        table_relationships: &std::collections::BTreeMap<String, Vec<(String, String, String)>>,
    ) -> Result<String> {
        let lines: Vec<&str> = content.lines().collect();
        let mut result = Vec::new();
        let mut i = 0;

        while i < lines.len() {
            let line = lines[i];

            // Check if this line starts an interface definition
            if line.trim_start().starts_with("export interface ") {
                let interface_name = line
                    .split_whitespace()
                    .nth(2)
                    .unwrap_or("")
                    .trim_end_matches(" {");

                // Check if this interface has relationships we need to update
                if let Some(relationships) = table_relationships.get(interface_name) {
                    result.push(line.to_string());

                    // Find the opening brace
                    let mut brace_found = false;
                    let mut j = i;

                    while j < lines.len() && !brace_found {
                        if lines[j].contains("{") {
                            brace_found = true;

                            // If opening brace is not on the same line, add those lines too
                            if j > i {
                                for k in (i + 1)..=j {
                                    result.push(lines[k].to_string());
                                }
                                i = j;
                            }
                        }
                        j += 1;
                    }

                    // Now find the closing brace and update relationship fields
                    if brace_found {
                        let mut brace_count = 0;
                        let mut started = false;
                        let mut closing_line_idx = i;

                        for k in i..lines.len() {
                            for ch in lines[k].chars() {
                                if ch == '{' {
                                    brace_count += 1;
                                    started = true;
                                } else if ch == '}' {
                                    brace_count -= 1;
                                    if started && brace_count == 0 {
                                        closing_line_idx = k;
                                        break;
                                    }
                                }
                            }
                            if started && brace_count == 0 {
                                break;
                            }
                        }

                        // Process lines within the interface
                        for k in (i + 1)..closing_line_idx {
                            let current_line = lines[k];
                            let mut updated_line = current_line.to_string();

                            // Check if this line contains a relationship field
                            for (field_name, related_table, cardinality) in relationships {
                                if current_line.contains(field_name) {
                                    // Update the field type based on cardinality
                                    if cardinality == "many" {
                                        // For many relationships, use array of related type
                                        if current_line.contains("string[]") {
                                            updated_line = current_line.replace(
                                                "string[]",
                                                &format!(
                                                    "SchemaDefinition[\"{}\"][]",
                                                    related_table
                                                ),
                                            );
                                        } else if current_line.contains("string") {
                                            updated_line = current_line.replace(
                                                "string",
                                                &format!(
                                                    "SchemaDefinition[\"{}\"][]",
                                                    related_table
                                                ),
                                            );
                                        }

                                        // Handle nullable arrays
                                        if current_line.contains("| null") {
                                            updated_line = updated_line.replace(
                                                &format!(
                                                    "SchemaDefinition[\"{}\"][]",
                                                    related_table
                                                ),
                                                &format!(
                                                    "SchemaDefinition[\"{}\"][] | null",
                                                    related_table
                                                ),
                                            );
                                        }
                                    } else {
                                        // For one relationships, use single related type
                                        if current_line.contains("string") {
                                            updated_line = current_line.replace(
                                                "string",
                                                &format!("SchemaDefinition[\"{}\"]", related_table),
                                            );
                                        }

                                        // Handle nullable single values
                                        if current_line.contains("| null") {
                                            updated_line = updated_line.replace(
                                                &format!("SchemaDefinition[\"{}\"]", related_table),
                                                &format!(
                                                    "SchemaDefinition[\"{}\"] | null",
                                                    related_table
                                                ),
                                            );
                                        }
                                    }
                                }
                            }

                            result.push(updated_line);
                        }

                        // Add the closing brace
                        result.push(lines[closing_line_idx].to_string());
                        i = closing_line_idx;
                    }
                } else {
                    // No relationships for this interface, add as-is
                    result.push(line.to_string());
                }
            } else {
                result.push(line.to_string());
            }

            i += 1;
        }

        Ok(result.join("\n"))
    }

    fn add_schema_constant(&self, content: String, schema: &str) -> Result<String> {
        match self.format {
            OutputFormat::Typescript => {
                // Escape the schema content for TypeScript template literal
                let filtered_schema = self.filter_client_schema(schema);
                let escaped_schema = filtered_schema.replace("\\", "\\\\").replace("`", "\\`");

                Ok(format!(
                    "{}\n\n/**\n * The complete SurrealDB schema definition.\n * This constant contains the raw .surql schema file content.\n */\nexport const SURQL_SCHEMA = `{}`;\n",
                    content, escaped_schema
                ))
            }
            OutputFormat::Dart => {
                // Escape the schema content for Dart string literal
                let filtered_schema = self.filter_client_schema(schema);
                let escaped_schema = filtered_schema
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("$", "\\$");

                Ok(format!(
                    "{}\n\n/// The complete SurrealDB schema definition.\n/// This constant contains the raw .surql schema file content.\nconst String SURQL_SCHEMA = \"{}\";\n",
                    content, escaped_schema
                ))
            }
            OutputFormat::JsonSchema | OutputFormat::Surql => {
                // For JSON and Surql, we don't add a schema constant
                Ok(content)
            }
        }
    }

    fn filter_client_schema(&self, schema: &str) -> String {
        let lines: Vec<&str> = schema.lines().collect();
        let mut result = Vec::new();
        let mut i = 0;
        let mut in_define_access = false;
        let mut access_brace_count = 0;
        let mut skip_permissions = false;

        let mut def_type = "TABLE";

        while i < lines.len() {
            let line = lines[i];

            // Strip comments to correctly check for semicolons
            let clean_line = if let Some(idx) = line.find("--") {
                &line[..idx]
            } else if let Some(idx) = line.find("//") {
                &line[..idx]
            } else if let Some(idx) = line.find('#') {
                &line[..idx]
            } else {
                line
            };

            let trimmed = clean_line.trim();

            // Skip empty lines (were just comments or whitespace)
            if trimmed.is_empty() {
                // If we are skipping a permissions block, and we hit an empty line,
                // we should check if the PREVIOUS line ended with semicolon?
                // Or does semicolon end the block immediately?
                // Logic:
                // if trimmed.ends_with(';') { replacement.push(';') } else { skip_permissions = true }
                // So if we are in skip_permissions mode, we look for ';'.
                // If line is empty, just continue.

                // But wait, if we are NOT in skip_permissions mode, we might want to preserve comments?
                // The logical flow below pushes `result.push(line.to_string())` at the end.
                // If I change `trimmed` to be comment-free, I might lose comments in output?
                // The task is to GENERATE valid schema. Comments valid.
                // But for LOGIC CHECKS, use `trimmed`.
                // For OUTPUT, use `line`.

                // Let's use `trimmed` for logic, keep `line` for output unless replacing.

                // BUT if I skip empty lines here, I lose them. I should proceed.
            }

            let _full_trimmed = line.trim();
            // Actually, matching "DEFINE TABLE" should use stripped line?
            // "DEFINE TABLE foo -- comment" -> "DEFINE TABLE foo". Matches.
            // So using `trimmed` (clean) is better.

            // Handle DEFINE BUCKET removal (buckets are server-side only)
            // Skip from DEFINE BUCKET to the terminating semicolon (multi-line)
            if trimmed.to_uppercase().starts_with("DEFINE BUCKET") {
                while i < lines.len() {
                    let bucket_clean = if let Some(idx) = lines[i].find("--") {
                        &lines[i][..idx]
                    } else {
                        lines[i]
                    };
                    if bucket_clean.contains(';') {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }

            // Handle DEFINE ACCESS removal
            if trimmed.to_uppercase().starts_with("DEFINE ACCESS") {
                in_define_access = true;
                access_brace_count = 0;
                // Count braces in the start line
                for ch in trimmed.chars() {
                    if ch == '{' {
                        access_brace_count += 1;
                    } else if ch == '}' {
                        access_brace_count -= 1;
                    }
                }

                if access_brace_count == 0 && trimmed.ends_with(';') {
                    in_define_access = false;
                }
                i += 1;
                continue;
            }

            if in_define_access {
                for ch in trimmed.chars() {
                    if ch == '{' {
                        access_brace_count += 1;
                    } else if ch == '}' {
                        access_brace_count -= 1;
                    }
                }

                if access_brace_count == 0 && trimmed.ends_with(';') {
                    in_define_access = false;
                }
                i += 1;
                continue;
            }

            // Track definition type
            if trimmed.to_uppercase().starts_with("DEFINE TABLE") {
                def_type = "TABLE";
            } else if trimmed.to_uppercase().starts_with("DEFINE FIELD") {
                def_type = "FIELD";
            }

            // Handle PERMISSIONS replacement
            // Match PERMISSIONS at start of line (for TABLE/FIELD).
            //
            // Important: this rewrite runs ONLY for the client-local SurrealKV
            // cache schema. The local DB is single-tenant (one user, one device)
            // and has no $session / $auth context to scope against, so blanket
            // `WHERE true` is correct here. The server schema preserves real
            // permissions — see meta_tables_remote.surql for inheritance rules
            // (e.g. _00_crdt / _00_version follow the linked record's perms).
            if trimmed.to_uppercase().starts_with("PERMISSIONS") {
                let perms = if def_type == "TABLE" {
                    "PERMISSIONS FOR select, create, update, delete WHERE true"
                } else {
                    "PERMISSIONS FOR select, create, update WHERE true"
                };
                let mut replacement = perms.to_string();

                if trimmed.ends_with(';') {
                    replacement.push(';');
                } else {
                    // It's a block, skip subsequent lines properly
                    skip_permissions = true;
                }

                result.push(replacement);
                i += 1;
                continue;
            }

            // Inside a permissions block we're rewriting to `WHERE true`: skip
            // EVERY source line of the block until the line carrying the
            // statement terminator `;`. The block is the tail of a DEFINE
            // TABLE/FIELD statement (PERMISSIONS is the last clause), so the
            // first `;` after it closes the whole block.
            //
            // We must skip continuation lines too, not just lines starting with
            // `FOR`: a clause whose `WHERE` expression wraps across lines (e.g.
            // `FOR update WHERE $access = "account"\n  AND (author.id = $auth.id\n
            // OR $auth.id IN (SELECT … ))`) has continuation lines that don't
            // start with `FOR`. Treating the first such line as the end of the
            // block (the old behaviour) leaked the rest of that clause — and the
            // following clauses — into the client schema, producing an invalid
            // `WHERE true AND (… IN (SELECT …))` the in-browser SSP can't parse.
            if skip_permissions {
                // Blank/comment-only lines carry no terminator; just drop them.
                if trimmed.is_empty() {
                    i += 1;
                    continue;
                }
                // `trimmed` is comment-stripped, so a `;` here is the real
                // statement terminator (WHERE expressions don't contain `;`).
                if trimmed.contains(';') {
                    skip_permissions = false;
                    if let Some(last) = result.last_mut() {
                        if !last.ends_with(';') {
                            last.push(';');
                        }
                    }
                }
                i += 1;
                continue;
            }

            result.push(line.to_string());
            i += 1;
        }

        result.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_record_ref_becomes_a_to_one_relationship() {
        // The JSON schema the CLI emits for `white: string | record<player_name>`
        // carries `x-record-ref` (see json_schema.rs). The relationships array in
        // the generated client schema must list it, so `.related('white')` joins.
        let json = serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Schema",
            "type": "object",
            "properties": { "game": {"$ref": "#/definitions/game"}, "player_name": {"$ref": "#/definitions/player_name"} },
            "definitions": {
                "player_name": { "type": "object", "properties": { "id": {"type": "string"}, "name": {"type": "string"} } },
                "game": { "type": "object", "properties": {
                    "id": {"type": "string"},
                    "white": {"type": "string", "x-record-ref": "player_name"},
                    "owner": {"type": "string", "description": "Record ID of table: player_name", "pattern": "^player_name:"},
                    "ghost": {"type": "string", "x-record-ref": "nowhere"}
                } }
            }
        });
        let gen = CodeGenerator::new(OutputFormat::Typescript, false, false);
        let out = gen.generate(&json.to_string(), "Schema").unwrap();
        assert!(
            out.contains("field: 'white' as const,\n      to: 'player_name' as const,\n      cardinality: 'one' as const"),
            "union link must appear as a to-one relationship:\n{out}"
        );
        assert!(out.contains("field: 'owner' as const"), "plain record links still emitted");
        assert!(!out.contains("'nowhere'"), "a ref to an unknown table is dropped");
    }
}

/// Run the app's query module through `@spooky-sync/query-allowlist` and write
/// the allowlist JSON that deploy/release/dev publish to `_00_query_allowlist`.
///
/// The generator is JavaScript (it has to execute `query.ts`), so this shells
/// out like `run_quicktype`: the app's own `node_modules/.bin` binary when the
/// app depends on the package, else `npx` pinned to this CLI's version.
pub fn run_query_allowlist(
    queries: &std::path::Path,
    schema_ts: &std::path::Path,
    out: &std::path::Path,
    app: &str,
) -> Result<()> {
    anyhow::ensure!(
        queries.exists(),
        "query module not found at {} (clientTypes[].queries)",
        queries.display()
    );
    anyhow::ensure!(
        schema_ts.exists(),
        "generated schema not found at {} (run the typescript clientTypes entry first)",
        schema_ts.display()
    );

    let bin_name = "spooky-query-allowlist";
    let local_bin = queries
        .ancestors()
        .skip(1)
        .map(|dir| dir.join("node_modules").join(".bin").join(bin_name))
        .find(|p| p.exists());

    let mut cmd = match &local_bin {
        Some(bin) => Command::new(bin),
        None => {
            let mut c = Command::new("npx");
            c.arg("-y")
                .arg(format!("@spooky-sync/query-allowlist@{}", env!("CARGO_PKG_VERSION")));
            c
        }
    };
    cmd.arg("--queries")
        .arg(queries)
        .arg("--schema")
        .arg(schema_ts)
        .arg("--out")
        .arg(out)
        .arg("--app")
        .arg(app);
    if let Some(dir) = queries.parent() {
        cmd.current_dir(dir);
    }

    let output = cmd.output().with_context(|| {
        format!(
            "Failed to execute {} (is Node installed, and is @spooky-sync/query-allowlist a devDependency of the app?)",
            local_bin
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "npx @spooky-sync/query-allowlist".to_string())
        )
    })?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stdout.lines().chain(stderr.lines()) {
        if !line.trim().is_empty() {
            println!("    {line}");
        }
    }
    if !output.status.success() {
        anyhow::bail!(
            "query allowlist generation failed for {} (exit {:?})",
            queries.display(),
            output.status.code()
        );
    }
    Ok(())
}
