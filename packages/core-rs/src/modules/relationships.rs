//! Schema relationships as `.related()` consumes them.
//!
//! PORT: `packages/spooky_core/lib/src/modules/relationships.dart`. The
//! relationship type itself lives in [`crate::types::SchemaRelationship`]; a
//! hand-written [`crate::Schema`] without any keeps working, `.related()` then
//! warn-skips.

pub use crate::types::{Cardinality, SchemaRelationship};

/// The relationship `field` names on `table`, or `None` when the schema
/// declares none (an unknown relation is skipped rather than fatal, mirroring
/// the TS builder).
pub fn find_relationship<'a>(
    relationships: &'a [SchemaRelationship],
    table: &str,
    field: &str,
) -> Option<&'a SchemaRelationship> {
    relationships
        .iter()
        .find(|r| r.from == table && r.field == field)
}

/// Naive English pluralization for reverse-relationship field names. Must match
/// `pluralize_table_name` in `apps/cli/src/json_schema.rs` exactly, or a Rust
/// client would name its reverse relations differently from the JS client
/// generated off the same schema.
pub fn pluralize_table_name(table: &str) -> String {
    match table {
        "user" => return "users".to_string(),
        "person" => return "people".to_string(),
        "child" => return "children".to_string(),
        "mouse" => return "mice".to_string(),
        _ => {}
    }
    if table.ends_with('s')
        || table.ends_with("sh")
        || table.ends_with("ch")
        || table.ends_with('x')
        || table.ends_with('z')
    {
        return format!("{table}es");
    }
    // Byte indexing like the Dart `table[length - 2]`: table names are ASCII
    // identifiers.
    if table.ends_with('y') && table.len() > 1 {
        let second_last = table.as_bytes()[table.len() - 2];
        if !matches!(second_last, b'a' | b'e' | b'i' | b'o' | b'u') {
            return format!("{}ies", &table[..table.len() - 1]);
        }
        return format!("{table}s");
    }
    if let Some(stem) = table.strip_suffix("fe") {
        return format!("{stem}ves");
    }
    if let Some(stem) = table.strip_suffix('f') {
        return format!("{stem}ves");
    }
    format!("{table}s")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pluralization_matches_the_cli_rules() {
        for (table, plural) in [
            ("user", "users"),
            ("person", "people"),
            ("child", "children"),
            ("mouse", "mice"),
            ("thread", "threads"),
            ("class", "classes"),
            ("match", "matches"),
            ("box", "boxes"),
            ("category", "categories"),
            ("play", "plays"),
            ("shelf", "shelves"),
            ("life", "lives"),
        ] {
            assert_eq!(pluralize_table_name(table), plural, "{table}");
        }
    }

    #[test]
    fn finds_by_table_and_field() {
        let rels = vec![SchemaRelationship {
            from: "thread".into(),
            field: "author".into(),
            to: "user".into(),
            cardinality: Cardinality::One,
        }];
        assert_eq!(
            find_relationship(&rels, "thread", "author").unwrap().to,
            "user"
        );
        assert!(find_relationship(&rels, "user", "author").is_none());
    }
}
