//! Extracts per-table `PERMISSIONS FOR select` predicates from a schema's
//! SurrealQL so the client circuit can be seeded (the SSP server does the same
//! at boot). Faithful port of `apps/ssp/src/lib.rs::extract_select_permission_text`
//! plus a `DEFINE TABLE` statement splitter (the server gets table defs from a
//! live `INFO FOR DB` query; the client parses them out of `schemaSurql`).

use std::collections::BTreeMap;

/// Built-in `select` permissions for server-provisioned meta tables the client
/// reads through the local view but that never appear in an app's
/// `schemaSurql` (they live in `apps/cli/src/meta_tables_remote.surql`, not the
/// app schema), so [`extract_table_permissions`] cannot reach them.
///
/// A default-deny circuit would otherwise reject the view for these tables.
///
/// `_00_user_feature` is scoped server-side to `user = $auth.id` and only the
/// user's own rows ever sync down (server permission + per-user
/// `_00_list_ref`), so `true` here is the same "trust the server filtered"
/// model as every other synced table. `_00_app_release` is world-readable
/// server-side (root-only writes), so `true` is exact rather than a trust
/// assumption. Extend this list for any future client-readable meta table.
pub const BUILTIN_SYSTEM_PERMISSIONS: &[(&str, &str)] =
    &[("_00_user_feature", "true"), ("_00_app_release", "true")];

/// Parse `{ table: selectWhereText }` from the schema SurrealQL.
///
/// The returned text is what `Circuit::set_permission` expects: a raw WHERE
/// body, or `true` (full / no clause) / `false` (none / deny). A table defined
/// twice keeps its last definition.
pub fn extract_table_permissions(schema_surql: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for def in define_table_statements(schema_surql) {
        let Some(name) = table_name(&def) else {
            continue;
        };
        out.insert(name.to_string(), extract_select_permission_text(&def));
    }
    out
}

/// Split a schema script into individual `DEFINE TABLE ...` statements
/// (terminated by `;`). Other statement kinds are ignored.
fn define_table_statements(schema_surql: &str) -> Vec<String> {
    // Statements are `;`-terminated. A naive split is sufficient because table
    // permission WHERE bodies in these schemas do not contain literal
    // semicolons (SurrealDB statement terminators).
    //
    // Strip full-line `--` comments FIRST: comment lines are not
    // `;`-terminated, so the `-- SECTION` headers that precede each `DEFINE
    // TABLE` would otherwise land at the front of the split chunk, defeating
    // the `DEFINE TABLE` prefix check and leaving every table default-deny.
    strip_line_comments(schema_surql)
        .split(';')
        .map(str::trim)
        .filter(|stmt| !stmt.is_empty() && stmt.to_ascii_uppercase().starts_with("DEFINE TABLE"))
        .map(str::to_string)
        .collect()
}

/// Drop whole-line SurrealQL comments (a line whose first non-whitespace is
/// `--`). Leaves inline content untouched; the schemas this parses keep their
/// comments on their own lines.
fn strip_line_comments(schema_surql: &str) -> String {
    let mut out = String::with_capacity(schema_surql.len());
    for line in schema_surql.split('\n') {
        if line.trim_start().starts_with("--") {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// The table name of a `DEFINE TABLE [IF NOT EXISTS|OVERWRITE] <name> ...`.
fn table_name(define_table: &str) -> Option<&str> {
    // tokens[0]=DEFINE tokens[1]=TABLE, then optional modifiers, then the name.
    define_table.split_whitespace().skip(2).find(|t| {
        !matches!(
            t.to_ascii_uppercase().as_str(),
            "IF" | "NOT" | "EXISTS" | "OVERWRITE"
        )
    })
}

/// Faithful port of `extract_select_permission_text`. Case folding is ASCII
/// only, so byte offsets in the folded copy index the original.
pub fn extract_select_permission_text(define_table: &str) -> String {
    let def = define_table.trim().trim_end_matches(';');
    let upper = def.to_ascii_uppercase();

    let Some(perm_idx) = upper.find("PERMISSIONS") else {
        // No PERMISSIONS clause: SurrealDB defaults to FULL, so allow.
        return "true".into();
    };
    let perm_section = def[perm_idx + "PERMISSIONS".len()..].trim();
    let perm_upper = perm_section.to_ascii_uppercase();

    if perm_upper.starts_with("FULL") {
        return "true".into();
    }
    if perm_upper.starts_with("NONE") {
        return "false".into();
    }

    let lower = perm_section.to_ascii_lowercase();
    let clause_starts: Vec<usize> = lower
        .match_indices("for ")
        .map(|(i, _)| i)
        .filter(|&i| {
            i == 0
                || lower.as_bytes()[i - 1].is_ascii_whitespace()
                || lower.as_bytes()[i - 1] == 0x0b
        })
        .collect();
    if clause_starts.is_empty() {
        // PERMISSIONS clause has no FOR clauses: deny.
        return "false".into();
    }

    for (idx, &start) in clause_starts.iter().enumerate() {
        let end = clause_starts
            .get(idx + 1)
            .copied()
            .unwrap_or(perm_section.len());
        let clause = &perm_section[start..end];
        let lower_clause = clause.to_ascii_lowercase();
        let where_idx = lower_clause.find("where");
        let header = &lower_clause[..where_idx.unwrap_or(lower_clause.len())];
        if !header.contains("select") {
            continue;
        }
        let Some(where_idx) = where_idx else {
            // FOR select with no WHERE: full allow.
            return "true".into();
        };
        let body = clause[where_idx + "where".len()..]
            .trim()
            .trim_end_matches([',', ';'])
            .trim();
        if body.is_empty() {
            return "true".into();
        }
        return body.to_string();
    }
    // No select clause among the FOR clauses: deny.
    "false".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_no_clause_none_where_body() {
        let schema = r#"
        DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE id = $auth.id;
        DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE published = true FOR update WHERE false;
        DEFINE TABLE public SCHEMALESS PERMISSIONS FULL;
        DEFINE TABLE secret SCHEMAFULL PERMISSIONS NONE;
        DEFINE TABLE plain SCHEMAFULL;
      "#;
        let perms = extract_table_permissions(schema);
        assert_eq!(perms["user"], "id = $auth.id");
        assert_eq!(perms["thread"], "published = true");
        assert_eq!(perms["public"], "true");
        assert_eq!(perms["secret"], "false");
        assert_eq!(perms["plain"], "true");
    }

    #[test]
    fn if_not_exists_table_name_is_parsed() {
        let perms = extract_table_permissions(
            "DEFINE TABLE IF NOT EXISTS note SCHEMAFULL PERMISSIONS FOR select WHERE true;",
        );
        assert_eq!(perms["note"], "true");
        let perms = extract_table_permissions(
            "define table overwrite memo permissions for select where x = 1;",
        );
        assert_eq!(perms["memo"], "x = 1");
    }

    #[test]
    fn section_header_comments_before_a_table_do_not_hide_it() {
        // Comment lines are not `;`-terminated, so a `-- HEADER` above a table
        // lands at the front of the split chunk; the parser must strip it or
        // the table is skipped and left default-deny.
        let schema = r#"
        -- ##############
        -- GAME TABLE
        -- ##############
        DEFINE TABLE game SCHEMAFULL
          PERMISSIONS
            FOR select WHERE true
            FOR update, delete, create WHERE $access = "account" AND owner = $auth.id
        ;
      "#;
        assert_eq!(extract_table_permissions(schema)["game"], "true");
    }

    #[test]
    fn select_clause_shapes() {
        let t = extract_select_permission_text;
        assert_eq!(
            t("DEFINE TABLE a PERMISSIONS FOR create, select WHERE x = 1, FOR update WHERE false"),
            "x = 1"
        );
        assert_eq!(
            t("DEFINE TABLE a PERMISSIONS FOR select FOR update WHERE false"),
            "true"
        );
        assert_eq!(
            t("DEFINE TABLE a PERMISSIONS FOR update WHERE false"),
            "false"
        );
        assert_eq!(t("DEFINE TABLE a PERMISSIONS FOR select WHERE ;"), "true");
        assert_eq!(t("DEFINE TABLE a PERMISSIONS something"), "false");
        // Non-ASCII content keeps its offsets.
        assert_eq!(
            t("DEFINE TABLE ß PERMISSIONS FOR select WHERE name = 'ßä'"),
            "name = 'ßä'"
        );
    }
}
