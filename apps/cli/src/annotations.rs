use regex::Regex;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct FieldAnnotation {
    pub name: String,
    pub value: Option<String>,
}

/// Known annotation names. Unknown annotations produce a warning.
const KNOWN_ANNOTATIONS: &[&str] = &["crdt", "cursor", "parent", "nosync", "opaque"];

/// Field-name group shared by every `DEFINE FIELD` matcher here.
///
/// Deliberately wider than `\w+`: SurrealDB field names can be nested paths
/// (`meta.secret`) or array projections (`tags.*`, `tags[*]`). With `\w+` the
/// match failed outright on those, so an annotation above such a field was
/// silently discarded — the author believed the field was excluded from sync
/// while it was fully synced and fully code-generated.
const FIELD_NAME_PATTERN: &str = r"[A-Za-z_][\w.*\[\]]*";

pub fn has_annotation(annotations: &[FieldAnnotation], name: &str) -> bool {
    annotations.iter().any(|a| a.name == name)
}

/// Annotations that carry a value (`-- @crdt text`). Every other annotation is a
/// bare marker.
const VALUED_ANNOTATIONS: &[&str] = &["crdt"];

/// Parse one comment as an annotation, or `None` when it is an ordinary comment.
///
/// The one reader for the `-- @name [value]` syntax. There used to be six copies
/// of the regex, and all six accepted ANY trailing text as the value - so a
/// sentence that merely begins with an annotation name was an annotation:
///
/// ```text
/// -- @nosync would break the client here, so this table stays synced.
/// DEFINE TABLE foo SCHEMAFULL;
/// ```
///
/// silently made `foo` server-only. That is the dangerous direction: the author
/// wrote the opposite of what happened, and nothing said so. It was found the
/// harmless way round, as two "attaches to nothing" warnings on prose lines in a
/// comment block that a blank line happened to orphan.
///
/// An annotation is therefore a marker and at most ONE token. More than one token
/// is a sentence. A single token on a marker that takes no value is not silently
/// dropped either - that is the typo case (`-- @nosync true`), and silence there
/// would mean a table the author believes is server-only is fully synced.
pub fn parse_annotation(comment: &str) -> Option<FieldAnnotation> {
    let (name, value) = annotation_parts(comment)?;
    if !KNOWN_ANNOTATIONS.contains(&name.as_str()) {
        eprintln!(
            "  ⚠ Unknown annotation @{} — known annotations: {}",
            name,
            KNOWN_ANNOTATIONS.join(", ")
        );
    }
    if value.is_some() && !VALUED_ANNOTATIONS.contains(&name.as_str()) {
        eprintln!(
            "  ⚠ Annotation @{name} takes no value, so `{}` is not read as one and \
             is being ignored. Write `-- @{name}` alone on the line.",
            comment.trim()
        );
        return None;
    }
    Some(FieldAnnotation { name, value })
}

/// Whether a line is an annotation comment, by exactly the rule
/// [`parse_annotation`] applies but without its warnings. For callers that ask
/// the question repeatedly about lines extraction has already reported on.
pub fn is_annotation_comment(line: &str) -> bool {
    annotation_parts(line)
        .is_some_and(|(name, value)| value.is_none() || VALUED_ANNOTATIONS.contains(&name.as_str()))
}

/// The syntax itself: `-- @name` and at most one value token.
fn annotation_parts(comment: &str) -> Option<(String, Option<String>)> {
    // `\S+`, not `.+?`: a value is one token, and a second token fails the match.
    let re = Regex::new(r"^--\s*@([a-z][a-z0-9_]*)(?:\s+(\S+))?\s*$").unwrap();
    let caps = re.captures(comment.trim())?;
    Some((caps[1].to_string(), caps.get(2).map(|m| m.as_str().to_string())))
}

/// Rewrite a `DEFINE FIELD` line to use `option<object> FLEXIBLE` when the
/// field carries both `@crdt` and `@cursor` annotations. The CRDT field
/// then stores `{ state, cursors }` as a structured object (the editor
/// writes both halves) instead of a plain snapshot string. Returns `None`
/// when the line doesn't need a rewrite.
pub fn rewrite_crdt_cursor_type(line: &str, annotations: &[FieldAnnotation]) -> Option<String> {
    if !(has_annotation(annotations, "crdt") && has_annotation(annotations, "cursor")) {
        return None;
    }
    let type_pos = line.find("TYPE ")?;
    let before_type = &line[..type_pos + 5];
    let after_type = &line[type_pos + 5..];
    // Take the EARLIEST keyword in the string, not the first in this list:
    // clause orders like `TYPE bool DEFAULT false PERMISSIONS ...` must not
    // stop at PERMISSIONS and drop the intervening `DEFAULT false` clause.
    let type_end = [
        " ASSERT ",
        " VALUE ",
        " PERMISSIONS ",
        " DEFAULT ",
        " READONLY ",
    ]
    .iter()
    .filter_map(|kw| after_type.find(kw))
    .chain(after_type.find(';'))
    .min()
    .unwrap_or(after_type.len());
    let rest = &after_type[type_end..];
    Some(format!("{}option<object> FLEXIBLE{}", before_type, rest))
}

/// Extract field annotations from raw .surql content.
///
/// Must run BEFORE surrealdb-core parsing since that strips comments.
/// Returns: (table_name, field_name) → Vec<FieldAnnotation>
///
/// Supports two placements:
/// 1. Preceding-line: `-- @crdt text` on line(s) before DEFINE FIELD
/// 2. Trailing: `DEFINE FIELD ...; -- @crdt text` after the semicolon
///
/// Association rules:
/// - Blank lines clear pending annotations
/// - Non-annotation comments do NOT clear pending
/// - Annotations inside multi-line DEFINE FIELD bodies are ignored
/// - Unknown annotation names produce a warning (forward-compatible)
pub fn extract_field_annotations(
    content: &str,
) -> BTreeMap<(String, String), Vec<FieldAnnotation>> {
    let define_field_re = Regex::new(&format!(
        r"(?i)DEFINE\s+FIELD\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?({FIELD_NAME_PATTERN})\s+ON\s+(?:TABLE\s+)?(\w+)"
    ))
    .unwrap();

    let mut result: BTreeMap<(String, String), Vec<FieldAnnotation>> = BTreeMap::new();
    let mut pending: Vec<FieldAnnotation> = Vec::new();
    let mut in_define_field = false;
    let mut current_key: Option<(String, String)> = None;

    for line in content.lines() {
        let trimmed = line.trim();

        // === Try to parse as standalone annotation comment ===
        if trimmed.starts_with("--") {
            if let Some(ann) = parse_annotation(trimmed) {
                if !in_define_field {
                    pending.push(ann);
                }
                // Annotations inside multi-line DEFINE FIELD body are ignored
            }
            // Non-annotation comments don't clear pending
            continue;
        }

        // === Blank line: clear pending annotations ===
        if trimmed.is_empty() {
            pending.clear();
            continue;
        }

        // === DEFINE FIELD start ===
        if let Some(caps) = define_field_re.captures(trimmed) {
            let field = caps[1].to_string();
            let table = caps[2].to_string();
            let key = (table, field);

            // Attach pending preceding-line annotations
            if !pending.is_empty() {
                result
                    .entry(key.clone())
                    .or_default()
                    .extend(pending.drain(..));
            }

            // Check for trailing annotation after ';' on this line
            if let Some(semi_pos) = trimmed.rfind(';') {
                let after = trimmed[semi_pos + 1..].trim();
                if let Some(ann) = parse_annotation(after) {
                    result.entry(key.clone()).or_default().push(ann);
                }
                in_define_field = false;
                current_key = None;
            } else {
                // Multi-line statement — track until ';'
                in_define_field = true;
                current_key = Some(key);
            }
            continue;
        }

        // === Continuation of multi-line DEFINE FIELD ===
        if in_define_field {
            if let Some(semi_pos) = trimmed.rfind(';') {
                // Check for trailing annotation on the closing line
                let after = trimmed[semi_pos + 1..].trim();
                if let Some(ann) = parse_annotation(after) {
                    if let Some(key) = &current_key {
                        result.entry(key.clone()).or_default().push(ann);
                    }
                }
                in_define_field = false;
                current_key = None;
            }
            continue;
        }

        // === Any other non-empty line: clear pending ===
        pending.clear();
    }

    result
}

/// An annotation comment that attaches to nothing, as `(line_number, name)`.
///
/// `extract_field_annotations` and `extract_table_annotations` both drop
/// annotations that aren't followed by a `DEFINE FIELD`/`DEFINE TABLE` — a blank
/// line between the comment and the statement is enough. That silence is the
/// dangerous part: the author sees `-- @nosync` in the schema and believes the
/// field is server-only, while the generated code syncs it. Callers surface
/// these as warnings.
pub fn unattached_annotations(content: &str) -> Vec<(usize, String)> {
    let define_re = Regex::new(r"(?i)^DEFINE\s+(FIELD|TABLE)\s+").unwrap();

    let mut out = Vec::new();
    // Annotation comments seen since the last statement, as (line_no, name).
    let mut pending: Vec<(usize, String)> = Vec::new();

    for (idx, line) in content.lines().enumerate() {
        let trimmed = line.trim();

        if trimmed.starts_with("--") {
            if let Some(ann) = parse_annotation(trimmed) {
                pending.push((idx + 1, ann.name));
            }
            continue;
        }

        // A blank line, or any statement that is not a DEFINE FIELD/TABLE,
        // orphans whatever was pending.
        if trimmed.is_empty() || !define_re.is_match(trimmed) {
            out.append(&mut pending);
            continue;
        }

        pending.clear();
    }

    // Trailing annotations at end of file attach to nothing either.
    out.append(&mut pending);
    out
}

/// Print a warning for every annotation comment that attaches to no statement.
pub fn warn_unattached_annotations(content: &str) {
    for (line, name) in unattached_annotations(content) {
        eprintln!(
            "  ⚠ Annotation @{} on line {} attaches to nothing — it must sit \
             directly above a DEFINE FIELD or DEFINE TABLE (no blank line between). \
             It is being ignored.",
            name, line
        );
    }
}

/// Extract table-level annotations from raw .surql content.
///
/// Must run BEFORE surrealdb-core parsing since that strips comments.
/// Returns: table_name → Vec<FieldAnnotation>
///
/// Mirrors `extract_field_annotations` but associates preceding-line
/// `-- @name value` comments with the following `DEFINE TABLE <name>`.
/// Used for `-- @nosync`. Association rules match the field variant:
/// - Blank lines clear pending annotations
/// - Non-annotation comments do NOT clear pending
/// - Trailing `; -- @name` after the statement is also supported
pub fn extract_table_annotations(content: &str) -> BTreeMap<String, Vec<FieldAnnotation>> {
    let define_table_re =
        Regex::new(r"(?i)^DEFINE\s+TABLE\s+(?:OVERWRITE\s+|IF\s+NOT\s+EXISTS\s+)?(\w+)").unwrap();

    let mut result: BTreeMap<String, Vec<FieldAnnotation>> = BTreeMap::new();
    let mut pending: Vec<FieldAnnotation> = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // === Standalone annotation comment ===
        if trimmed.starts_with("--") {
            if let Some(ann) = parse_annotation(trimmed) {
                pending.push(ann);
            }
            // Non-annotation comments don't clear pending
            continue;
        }

        // === Blank line clears pending ===
        if trimmed.is_empty() {
            pending.clear();
            continue;
        }

        // === DEFINE TABLE start ===
        if let Some(caps) = define_table_re.captures(trimmed) {
            let table = caps[1].to_string();

            if !pending.is_empty() {
                result
                    .entry(table.clone())
                    .or_default()
                    .extend(pending.drain(..));
            }

            // Trailing annotation after the closing ';' on the same line
            if let Some(semi_pos) = trimmed.rfind(';') {
                let after = trimmed[semi_pos + 1..].trim();
                if let Some(ann) = parse_annotation(after) {
                    result.entry(table).or_default().push(ann);
                }
            }
            continue;
        }

        // === Any other non-empty line clears pending ===
        pending.clear();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preceding_annotation() {
        let content = r#"
-- @crdt text
DEFINE FIELD content ON TABLE thread TYPE string;
"#;
        let result = extract_field_annotations(content);
        let anns = result.get(&("thread".to_string(), "content".to_string()));
        assert!(anns.is_some());
        let anns = anns.unwrap();
        assert_eq!(anns.len(), 1);
        assert_eq!(anns[0].name, "crdt");
        assert_eq!(anns[0].value, Some("text".to_string()));
    }

    #[test]
    fn test_trailing_annotation() {
        let content = r#"DEFINE FIELD author ON TABLE thread TYPE record<user>; -- @parent"#;
        let result = extract_field_annotations(content);
        let anns = result.get(&("thread".to_string(), "author".to_string()));
        assert!(anns.is_some());
        let anns = anns.unwrap();
        assert_eq!(anns.len(), 1);
        assert_eq!(anns[0].name, "parent");
        assert_eq!(anns[0].value, None);
    }

    #[test]
    fn test_multiline_with_trailing() {
        let content = r#"DEFINE FIELD content ON TABLE thread TYPE string
    ASSERT $value != NONE AND string::len($value) > 0; -- @crdt text"#;
        let result = extract_field_annotations(content);
        let anns = result.get(&("thread".to_string(), "content".to_string()));
        assert!(anns.is_some());
        assert_eq!(anns.unwrap()[0].name, "crdt");
    }

    #[test]
    fn test_blank_line_clears_pending() {
        let content = r#"
-- @crdt text

DEFINE FIELD content ON TABLE thread TYPE string;
"#;
        let result = extract_field_annotations(content);
        let anns = result.get(&("thread".to_string(), "content".to_string()));
        assert!(anns.is_none());
    }

    #[test]
    fn test_multiple_annotations() {
        let content = r#"
-- @crdt text
-- @parent
DEFINE FIELD content ON TABLE thread TYPE string;
"#;
        let result = extract_field_annotations(content);
        let anns = result.get(&("thread".to_string(), "content".to_string()));
        assert!(anns.is_some());
        assert_eq!(anns.unwrap().len(), 2);
    }

    #[test]
    fn test_crdt_cursor_rewrite_preserves_default_before_permissions() {
        // Clause order `DEFAULT ... PERMISSIONS ...` must keep the whole
        // tail (DEFAULT + PERMISSIONS) after the rewritten type rather than
        // stopping at PERMISSIONS and silently dropping the DEFAULT clause.
        let anns = [
            FieldAnnotation {
                name: "crdt".into(),
                value: Some("text".into()),
            },
            FieldAnnotation {
                name: "cursor".into(),
                value: None,
            },
        ];
        let line = "DEFINE FIELD content ON TABLE thread TYPE string DEFAULT {} PERMISSIONS FOR update WHERE true";
        let got = rewrite_crdt_cursor_type(line, &anns).expect("should rewrite");
        assert_eq!(
            got,
            "DEFINE FIELD content ON TABLE thread TYPE option<object> FLEXIBLE DEFAULT {} PERMISSIONS FOR update WHERE true"
        );
    }

    #[test]
    fn a_sentence_that_begins_with_an_annotation_name_is_not_one() {
        // The dangerous direction: the author wrote that the table STAYS synced,
        // and the old any-trailing-text regex silently made it server-only.
        let content = "-- @nosync would break the client here, so this stays synced.\n\
                       DEFINE TABLE foo SCHEMAFULL;\n";
        assert!(
            extract_table_annotations(content).is_empty(),
            "prose must not mark a table nosync"
        );
        // And it is a comment, so it is not reported as an orphaned annotation.
        assert!(unattached_annotations(content).is_empty());
    }

    #[test]
    fn prose_in_a_comment_block_does_not_warn_as_unattached() {
        // The whitepawn shape that surfaced this: doc sentences about @nosync in
        // the block above the real annotation, orphaned by a blank line.
        let content = "-- @nosync (server-only, since 2026-09-07): this table is big.\n\
                       -- @nosync means no ingest events and no circuit rows.\n\
                       \n\
                       -- @nosync\n\
                       DEFINE TABLE analysis SCHEMAFULL;\n";
        assert!(unattached_annotations(content).is_empty(), "prose is not an annotation");
        let anns = extract_table_annotations(content);
        assert!(has_annotation(&anns["analysis"], "nosync"), "the real one still attaches");
    }

    #[test]
    fn a_value_on_a_marker_annotation_is_refused_not_silently_kept() {
        // `-- @nosync true` is the typo case. Reading it as nosync would be a
        // guess; dropping it silently would leave a table the author believes is
        // server-only fully synced. It is refused (with a warning on stderr).
        assert!(parse_annotation("-- @nosync true").is_none());
        assert!(parse_annotation("-- @opaque yes").is_none());
        // A bare marker and a valued annotation both still parse.
        assert_eq!(parse_annotation("-- @nosync").unwrap().name, "nosync");
        let crdt = parse_annotation("-- @crdt text").unwrap();
        assert_eq!((crdt.name.as_str(), crdt.value.as_deref()), ("crdt", Some("text")));
        // A valued annotation followed by a sentence is a sentence.
        assert!(parse_annotation("-- @crdt text is what we use here").is_none());
    }

    #[test]
    fn test_no_false_positives() {
        let content = r#"
-- TODO: @crdt support for this field later
DEFINE FIELD content ON TABLE thread TYPE string;
"#;
        let result = extract_field_annotations(content);
        // "TODO: @crdt..." doesn't match the regex (text before @)
        let anns = result.get(&("thread".to_string(), "content".to_string()));
        assert!(anns.is_none());
    }

    #[test]
    fn test_table_nosync_preceding() {
        let content = r#"
-- @nosync
DEFINE TABLE secrets SCHEMALESS PERMISSIONS FOR select WHERE false;
"#;
        let result = extract_table_annotations(content);
        let anns = result.get("secrets").expect("secrets annotated");
        assert_eq!(anns.len(), 1);
        assert_eq!(anns[0].name, "nosync");
    }

    #[test]
    fn test_table_nosync_trailing() {
        let content = r#"DEFINE TABLE secrets SCHEMALESS; -- @nosync"#;
        let result = extract_table_annotations(content);
        let anns = result.get("secrets").expect("secrets annotated");
        assert_eq!(anns[0].name, "nosync");
    }

    #[test]
    fn test_table_nosync_overwrite_and_blank_line_clears() {
        let content = r#"
-- @nosync
DEFINE TABLE OVERWRITE kept SCHEMALESS;

DEFINE TABLE other SCHEMALESS;
"#;
        let result = extract_table_annotations(content);
        assert!(result.get("kept").is_some());
        // blank line cleared pending before `other`
        assert!(result.get("other").is_none());
    }

    #[test]
    fn test_table_annotation_not_attached_to_field() {
        // A `-- @nosync` above a DEFINE FIELD must not mark a table.
        let content = r#"
-- @nosync
DEFINE FIELD x ON TABLE thing TYPE string;
"#;
        let result = extract_table_annotations(content);
        assert!(result.is_empty());
    }

    #[test]
    fn test_annotation_attaches_to_nested_field_path() {
        // `\w+` could not match `meta.secret`, so the annotation was silently
        // dropped and the field was synced despite being marked.
        let content = r#"
-- @opaque
DEFINE FIELD meta.secret ON TABLE user TYPE string;
"#;
        let result = extract_field_annotations(content);
        let anns = result
            .get(&("user".to_string(), "meta.secret".to_string()))
            .expect("nested field annotated");
        assert_eq!(anns[0].name, "opaque");
    }

    #[test]
    fn test_annotation_attaches_to_array_projection_field() {
        let content = r#"
-- @opaque
DEFINE FIELD tags.* ON TABLE user TYPE string;
"#;
        let result = extract_field_annotations(content);
        assert!(result
            .get(&("user".to_string(), "tags.*".to_string()))
            .is_some());
    }

    #[test]
    fn test_unattached_annotation_is_reported() {
        // Blank line orphans it; `extract_field_annotations` drops it silently.
        let content = "-- @opaque\n\nDEFINE FIELD blob ON TABLE user TYPE bytes;\n";
        assert!(extract_field_annotations(content).is_empty());
        assert_eq!(
            unattached_annotations(content),
            vec![(1, "opaque".to_string())]
        );
    }

    #[test]
    fn test_annotation_before_non_define_statement_is_reported() {
        let content = "-- @opaque\nCREATE user:1 SET x = 1;\n";
        assert_eq!(
            unattached_annotations(content),
            vec![(1, "opaque".to_string())]
        );
    }

    #[test]
    fn test_trailing_annotation_at_eof_is_reported() {
        let content = "DEFINE FIELD blob ON TABLE user TYPE bytes;\n-- @opaque\n";
        assert_eq!(
            unattached_annotations(content),
            vec![(2, "opaque".to_string())]
        );
    }

    #[test]
    fn test_properly_placed_annotations_are_not_reported() {
        let content = "\
-- @nosync
DEFINE TABLE secrets SCHEMALESS;

-- @opaque
-- some prose about the field
DEFINE FIELD blob ON TABLE user TYPE bytes;
";
        assert!(unattached_annotations(content).is_empty());
    }

    #[test]
    fn test_non_annotation_comments_dont_clear_pending() {
        let content = r#"
-- @crdt text
-- This field stores the thread content
DEFINE FIELD content ON TABLE thread TYPE string;
"#;
        let result = extract_field_annotations(content);
        let anns = result.get(&("thread".to_string(), "content".to_string()));
        assert!(anns.is_some());
        assert_eq!(anns.unwrap().len(), 1);
        assert_eq!(anns.unwrap()[0].name, "crdt");
    }
}
