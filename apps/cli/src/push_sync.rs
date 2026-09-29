//! `push:` in sp00ky.yml, from the manifest to `_00_push_config:default`.
//!
//! The block is parsed into `push_core::PushConfig` (the same struct the push
//! engine reads back), checked by `PushConfig::validate` plus the schema-aware
//! checks below (`spky lint`), and written by every migrate/deploy as one row:
//! `spec_json` is the serde JSON of the (possibly default) config, `hash` its
//! sha256. The write is skipped when the stored hash already matches, so an
//! unchanged manifest costs one read.
//!
//! A missing `push:` block writes `PushConfig::default()` (enabled, no rules):
//! removing the block has to stop the rules it used to declare, and direct
//! messages (`_00_push_message`) keep working without any configuration.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use push_core::{DataSpec, Issue, PushConfig, Severity, Target};
use serde_json::Value;

use crate::backend::{DeployMode, Sp00kyConfig};
use crate::parser::{SchemaParser, TableSchema};
use crate::surreal_client::MigrationDB;

/// The one row the engine reads.
pub const CONFIG_RECORD: &str = "_00_push_config:default";

/// The normalized block as the engine will read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub json: String,
    pub hash: String,
}

pub fn spec_of(cfg: &PushConfig) -> Result<Spec> {
    let json = serde_json::to_string(cfg).context("could not serialize the push config")?;
    let hash = crate::migrate::checksum_str(&json);
    Ok(Spec { json, hash })
}

/// A single-quoted SurrealQL string literal holding exactly `s`.
///
/// Only `\` and `'` need escaping inside single quotes: SurrealDB's lexer
/// pushes every other byte through verbatim (raw newlines and UTF-8 included).
/// Escaping every backslash also means no JSON escape inside `s` (`\"`, `\n`,
/// `\u00e9`) is ever reinterpreted, so the stored text is byte-identical to
/// what was hashed. Same rule as `schedules::esc`.
pub fn surql_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The UPSERT that stores `spec`. SET, not CONTENT, so a field the engine may
/// add to the row later is never wiped by a deploy.
pub fn upsert_sql(spec: &Spec) -> String {
    format!(
        "UPSERT {CONFIG_RECORD} SET spec_json = {}, hash = {}, updated_at = time::now();",
        surql_string(&spec.json),
        surql_string(&spec.hash)
    )
}

/// The stored hash, `None` when the row (or the table) does not exist yet or
/// the read fails: all of which mean "write it".
pub fn read_stored_hash(client: &dyn MigrationDB) -> Option<String> {
    let responses = client
        .execute(&format!("SELECT hash FROM {CONFIG_RECORD};"))
        .ok()?;
    for r in responses {
        if r.status != "OK" {
            continue;
        }
        let row = match r.result {
            Some(Value::Array(rows)) => rows.into_iter().next(),
            Some(other @ Value::Object(_)) => Some(other),
            _ => None,
        };
        if let Some(hash) = row
            .as_ref()
            .and_then(|r| r.get("hash"))
            .and_then(Value::as_str)
        {
            return Some(hash.to_string());
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Unchanged,
    Written,
}

/// Validate `cfg` and store it unless the stored hash already matches. An
/// invalid config is refused (and the previous row stays), never written: the
/// engine would reject it anyway, and keeping the last good rules is the
/// lesser surprise.
pub fn sync(client: &dyn MigrationDB, cfg: &PushConfig) -> Result<Outcome> {
    let errors: Vec<String> = cfg
        .validate()
        .into_iter()
        .filter(|i| i.severity == Severity::Error)
        .map(|i| i.to_string())
        .collect();
    if !errors.is_empty() {
        bail!(
            "the push config is invalid (run `spky lint`):\n  {}",
            errors.join("\n  ")
        );
    }
    let spec = spec_of(cfg)?;
    if read_stored_hash(client).as_deref() == Some(spec.hash.as_str()) {
        return Ok(Outcome::Unchanged);
    }
    client
        .execute(&upsert_sql(&spec))
        .with_context(|| format!("failed to write {CONFIG_RECORD}"))?;
    Ok(Outcome::Written)
}

/// The `push:` block of the manifest at `config_path`, strictly parsed.
///
/// Unlike `backend::load_config` this never falls back to a default config on
/// a parse error: a typo in sp00ky.yml must not store "no rules" over the
/// rules a working deploy wrote.
pub fn load(config_path: &Path) -> Result<PushConfig> {
    let content = std::fs::read_to_string(config_path)
        .with_context(|| format!("could not read {}", config_path.display()))?;
    let base_dir = config_path.parent().unwrap_or(Path::new("."));
    let config = crate::backend::parse_config_with_includes(&content, base_dir)
        .with_context(|| format!("failed to parse {}", config_path.display()))?;
    Ok(config.push())
}

/// One line for step output: `3 rules`, `no rules`, `disabled`.
pub fn summary(cfg: &PushConfig) -> String {
    if !cfg.enabled {
        return "disabled".to_string();
    }
    match cfg.rules.len() {
        0 => "no rules".to_string(),
        1 => "1 rule".to_string(),
        n => format!("{n} rules"),
    }
}

// ── Schema-aware checks (`spky lint`) ───────────────────────────────────────

/// Everything `spky lint` reports about `push:`: the block's own validation,
/// the schema checks below, and a rule set no host can deliver.
pub fn lint_issues(config: &Sp00kyConfig, config_path: &Path) -> Vec<Issue> {
    let push = config.push();
    let mut issues = push.validate();
    let active = push.enabled && push.rules.values().any(|r| r.enabled);
    if active && config.mode == Some(DeployMode::Surrealism) {
        issues.push(Issue::warning(
            "push",
            "`mode: surrealism` has no push host (the scheduler or a standalone SSP runs the engine): the rules are stored, but nothing delivers them",
        ));
    }
    if push.rules.is_empty() {
        return issues;
    }
    match schema_tables(config, config_path) {
        Ok(tables) => issues.extend(schema_issues(&push, &tables)),
        Err(e) => issues.push(Issue::warning(
            "push.rules",
            format!("could not read the schema to check the rules' tables: {e:#}"),
        )),
    }
    issues
}

/// The app's tables as the internal schema sees them: the schema file plus
/// what the backends append (outbox tables).
fn schema_tables(
    config: &Sp00kyConfig,
    config_path: &Path,
) -> Result<BTreeMap<String, TableSchema>> {
    let base_dir = config_path.parent().unwrap_or(Path::new("."));
    let schema_path = base_dir.join(config.resolved_schema().schema);
    let mut content = std::fs::read_to_string(&schema_path)
        .with_context(|| format!("could not read {}", schema_path.display()))?;
    let mut processor = crate::backend::BackendProcessor::new();
    if processor.process(config_path).is_ok() {
        content.push('\n');
        content.push_str(&processor.schema_appends);
    }
    let mut parser = SchemaParser::new();
    parser
        .parse_file(&content)
        .with_context(|| format!("could not parse {}", schema_path.display()))?;
    Ok(parser.tables)
}

/// Fields every ingested row carries whatever the schema says.
const IMPLICIT_FIELDS: &[&str] = &["id", "_00_rv"];

/// Warnings that need the schema: a rule on a table that does not exist or
/// never reaches the ingest path, and field paths that name nothing the
/// ingested row carries.
///
/// Relation tables are NOT flagged: they sync like any other table (their
/// ingest payload carries `in` and `out` as record-id strings, see
/// `sp00ky::relation_endpoints`), so a rule on one is fine.
pub fn schema_issues(cfg: &PushConfig, tables: &BTreeMap<String, TableSchema>) -> Vec<Issue> {
    let mut issues = Vec::new();
    for (name, rule) in &cfg.rules {
        let at = format!("push.rules.{name}");
        // `validate()` already errors on these; do not pile a warning on top.
        if rule.table.starts_with("_00_") || rule.table.is_empty() {
            continue;
        }
        let Some(table) = tables.get(&rule.table) else {
            issues.push(Issue::warning(
                format!("{at}.table"),
                format!(
                    "`{}` is not a table in the schema, so this rule never fires",
                    rule.table
                ),
            ));
            continue;
        };
        if table.no_sync {
            issues.push(Issue::warning(
                format!("{at}.table"),
                format!(
                    "`{}` is marked `-- @nosync`: its rows never reach the push engine, so this rule never fires",
                    rule.table
                ),
            ));
            continue;
        }
        for (where_, path) in referenced_paths(rule) {
            let seg = path.split('.').next().unwrap_or_default();
            if IMPLICIT_FIELDS.contains(&seg) {
                continue;
            }
            if table.is_relation && (seg == "in" || seg == "out") {
                continue;
            }
            match table.fields.get(seg) {
                Some(field) if field.excluded_from_sync() => issues.push(Issue::warning(
                    format!("{at}.{where_}"),
                    format!(
                        "`{seg}` is `@nosync`/`@crdt`/`@opaque` on `{}`: it is stripped from ingested rows, so the rule never sees it",
                        rule.table
                    ),
                )),
                Some(_) => {}
                None if table.schemafull => issues.push(Issue::warning(
                    format!("{at}.{where_}"),
                    format!("`{seg}` is not a field of `{}`", rule.table),
                )),
                None => {}
            }
        }
    }
    issues
}

/// `(where in the rule, field path)` for every plain field path a rule reads
/// off the row. Templates are not parsed here.
fn referenced_paths(rule: &push_core::Rule) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for key in rule.when.keys() {
        out.push((format!("when.{key}"), key.clone()));
    }
    for p in rule.once.iter().flatten() {
        out.push(("once".into(), p.clone()));
    }
    if let Target::Fields(fields) = &rule.to {
        for p in fields.iter() {
            out.push(("to".into(), p.clone()));
        }
    }
    for p in rule.except.iter() {
        out.push(("except".into(), p.clone()));
    }
    if let Some(max_age) = &rule.max_age {
        out.push(("maxAge.field".into(), max_age.field.clone()));
    }
    if let Some(DataSpec::Fields(fields)) = &rule.data {
        for p in fields {
            out.push(("data".into(), p.clone()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SchemaParser;
    use crate::surreal_client::{AppliedMigration, SurrealResponse};
    use std::cell::RefCell;

    fn cfg(yaml: &str) -> PushConfig {
        serde_yaml::from_str(yaml).expect("push yaml")
    }

    fn tables(schema: &str) -> BTreeMap<String, TableSchema> {
        let mut p = SchemaParser::new();
        p.parse_file(schema).expect("schema");
        p.tables
    }

    /// Answers the hash read with `stored`, records everything else.
    struct HashDb {
        stored: Option<String>,
        executed: RefCell<Vec<String>>,
    }

    impl MigrationDB for HashDb {
        fn ping(&self) -> Result<()> {
            Ok(())
        }
        fn ensure_ns_db(&self) -> Result<()> {
            Ok(())
        }
        fn ensure_migration_table(&self) -> Result<()> {
            Ok(())
        }
        fn execute(&self, query: &str) -> Result<Vec<SurrealResponse>> {
            self.executed.borrow_mut().push(query.to_string());
            if query.starts_with("SELECT hash FROM _00_push_config") {
                let rows = match &self.stored {
                    Some(h) => serde_json::json!([{ "hash": h }]),
                    None => serde_json::json!([]),
                };
                return Ok(vec![SurrealResponse {
                    status: "OK".into(),
                    result: Some(rows),
                }]);
            }
            Ok(vec![SurrealResponse {
                status: "OK".into(),
                result: None,
            }])
        }
        fn get_applied_migrations(&self) -> Result<Vec<AppliedMigration>> {
            Ok(vec![])
        }
        fn record_migration(&self, _: &str, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        fn update_migration_checksum(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
    }

    const NASTY: &str = r#"
subject: "mailto:o'brien@example.com"
rules:
  new-message:
    table: message
    to: recipient
    topic: "dm:{{conversation | key}}"
    notification:
      title: "{{sender}} says \"hi\" \\ o'clock"
      body: "line one\nline two\ttabbed, caf\u00e9 \u2603 \U0001F600 {{ text | truncate(120) }}"
      url: "/m/{{conversation | key}}?a=1&b='2'"
"#;

    #[test]
    fn a_missing_block_is_the_default_config() {
        let c = crate::backend::parse_config_with_includes("slug: x\n", Path::new(".")).unwrap();
        assert!(c.push.is_none());
        assert_eq!(c.push(), PushConfig::default());
        let spec = spec_of(&c.push()).unwrap();
        assert_eq!(spec.json, "{}", "the default serializes to an empty object");
    }

    #[test]
    fn the_upsert_writes_exactly_the_three_ddl_fields() {
        let spec = spec_of(&cfg(NASTY)).unwrap();
        let sql = upsert_sql(&spec);
        assert!(
            sql.starts_with("UPSERT _00_push_config:default SET spec_json = '"),
            "{sql}"
        );
        assert!(sql.contains(&format!("hash = '{}'", spec.hash)), "{sql}");
        assert!(sql.ends_with("updated_at = time::now();"), "{sql}");
        let ddl = include_str!("push_tables.surql");
        for field in ["spec_json", "hash", "updated_at"] {
            assert!(
                ddl.contains(&format!(
                    "DEFINE FIELD OVERWRITE {field} ON TABLE _00_push_config"
                )),
                "`{field}` must be defined on _00_push_config"
            );
        }
    }

    /// Quotes, backslashes, `{{`, newlines and non-ASCII survive the trip
    /// through SurrealQL's string lexer byte for byte, so the stored text
    /// hashes to the stored hash.
    #[test]
    fn a_nasty_spec_round_trips_through_the_surrealql_lexer() {
        let spec = spec_of(&cfg(NASTY)).unwrap();
        assert!(spec.json.contains("{{"));
        assert!(
            spec.json.contains("\\n"),
            "serde escapes the newline: {}",
            spec.json
        );
        assert!(spec.json.contains('\''));
        assert!(spec.json.contains("\\\""));
        assert!(spec.json.contains('\u{2603}') && spec.json.contains('\u{1F600}'));

        let literal = surql_string(&spec.json);
        let parsed = surrealdb_core::syn::value(&literal).expect("the literal parses");
        let surrealdb_core::sql::Value::Strand(s) = parsed else {
            panic!("not a string: {parsed:?}");
        };
        assert_eq!(s.as_str(), spec.json);
        assert_eq!(crate::migrate::checksum_str(s.as_str()), spec.hash);

        // The whole statement parses too.
        surrealdb_core::syn::parse(&upsert_sql(&spec)).expect("the UPSERT parses");

        // And the JSON the engine reads back is the config that was written.
        let back: PushConfig = serde_json::from_str(s.as_str()).unwrap();
        assert_eq!(back, cfg(NASTY));
    }

    #[test]
    fn an_unchanged_config_is_not_rewritten() {
        let c = cfg(NASTY);
        let spec = spec_of(&c).unwrap();
        let db = HashDb {
            stored: Some(spec.hash.clone()),
            executed: RefCell::new(vec![]),
        };
        assert_eq!(sync(&db, &c).unwrap(), Outcome::Unchanged);
        assert_eq!(db.executed.borrow().len(), 1, "only the hash read");

        let db = HashDb {
            stored: Some("stale".into()),
            executed: RefCell::new(vec![]),
        };
        assert_eq!(sync(&db, &c).unwrap(), Outcome::Written);
        assert_eq!(db.executed.borrow()[1], upsert_sql(&spec));

        let db = HashDb {
            stored: None,
            executed: RefCell::new(vec![]),
        };
        assert_eq!(sync(&db, &PushConfig::default()).unwrap(), Outcome::Written);
        assert!(db.executed.borrow()[1].contains("spec_json = '{}'"));
    }

    #[test]
    fn an_invalid_config_is_never_written() {
        let bad = cfg("subject: ops@example.com\nrules: { r: { table: t, to: u } }");
        let db = HashDb {
            stored: None,
            executed: RefCell::new(vec![]),
        };
        let err = sync(&db, &bad).unwrap_err().to_string();
        assert!(err.contains("push.subject"), "{err}");
        assert!(
            db.executed.borrow().is_empty(),
            "nothing read, nothing written"
        );
    }

    #[test]
    fn a_linked_push_file_is_the_same_as_inline() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("push.yml"),
            "subject: mailto:ops@example.com\nrules:\n  r:\n    table: message\n    to: recipient\n",
        )
        .unwrap();
        let linked =
            crate::backend::parse_config_with_includes("push: ./push.yml\n", dir.path()).unwrap();
        let inline = crate::backend::parse_config_with_includes(
            "push:\n  subject: mailto:ops@example.com\n  rules:\n    r:\n      table: message\n      to: recipient\n",
            dir.path(),
        )
        .unwrap();
        assert_eq!(linked.push, inline.push);
        assert!(linked.push.unwrap().rules.contains_key("r"));

        let err = crate::backend::parse_config_with_includes("push: ./missing.yml\n", dir.path())
            .unwrap_err();
        assert!(format!("{err:#}").contains("missing.yml"), "{err:#}");
    }

    #[test]
    fn schema_checks_flag_missing_nosync_and_stripped() {
        let schema = r#"
DEFINE TABLE message SCHEMAFULL;
DEFINE FIELD recipient ON TABLE message TYPE record<user>;
DEFINE FIELD sender ON TABLE message TYPE record<user>;
-- @opaque
DEFINE FIELD body ON TABLE message TYPE string;
-- @nosync
DEFINE TABLE secret SCHEMAFULL;
DEFINE FIELD owner ON TABLE secret TYPE record<user>;
DEFINE TABLE loose SCHEMALESS;
DEFINE TABLE likes TYPE RELATION IN user OUT post;
"#;
        let c = cfg(r#"
rules:
  ok:
    table: message
    to: recipient
    except: sender
    when: { id: { exists: true } }
  stripped:
    table: message
    to: recipient
    when: { body: { exists: true } }
  typo:
    table: message
    to: recipent
  gone:
    table: mesage
    to: recipient
  hidden:
    table: secret
    to: owner
  schemaless:
    table: loose
    to: whoever
  edge:
    table: likes
    to: out
"#);
        let issues = schema_issues(&c, &tables(schema));
        let find = |rule: &str| {
            issues
                .iter()
                .filter(|i| i.path.starts_with(&format!("push.rules.{rule}.")))
                .collect::<Vec<_>>()
        };
        assert!(find("ok").is_empty(), "{issues:?}");
        assert!(
            find("schemaless").is_empty(),
            "any field goes on a schemaless table: {issues:?}"
        );
        assert!(
            find("edge").is_empty(),
            "relations sync; `out` is on every edge: {issues:?}"
        );
        assert!(
            find("stripped")[0].message.contains("stripped"),
            "{issues:?}"
        );
        assert!(
            find("typo")[0]
                .message
                .contains("`recipent` is not a field"),
            "{issues:?}"
        );
        assert!(
            find("gone")[0].message.contains("not a table"),
            "{issues:?}"
        );
        assert!(find("hidden")[0].message.contains("@nosync"), "{issues:?}");
        assert!(issues.iter().all(|i| i.severity == Severity::Warning));
    }

    /// `spky lint`: block errors, schema warnings from the schema the manifest
    /// names, and a rule set nothing can deliver.
    #[test]
    fn lint_combines_block_schema_and_mode_checks() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("db")).unwrap();
        std::fs::write(
            dir.path().join("db/schema.surql"),
            "DEFINE TABLE message SCHEMAFULL;\nDEFINE FIELD recipient ON TABLE message TYPE string;\n",
        )
        .unwrap();
        let manifest = "mode: surrealism\nschema: { schema: db/schema.surql }\npush:\n  subject: ops@example.com\n  rules:\n    r:\n      table: message\n      to: recipent\n";
        let path = dir.path().join("sp00ky.yml");
        std::fs::write(&path, manifest).unwrap();
        let config = crate::backend::parse_config_with_includes(manifest, dir.path()).unwrap();

        let issues = lint_issues(&config, &path);
        let has = |sev: Severity, path: &str, needle: &str| {
            issues
                .iter()
                .any(|i| i.severity == sev && i.path == path && i.message.contains(needle))
        };
        assert!(
            has(Severity::Error, "push.subject", "mailto:"),
            "{issues:?}"
        );
        assert!(
            has(
                Severity::Warning,
                "push.rules.r.to",
                "`recipent` is not a field"
            ),
            "{issues:?}"
        );
        assert!(has(Severity::Warning, "push", "surrealism"), "{issues:?}");

        // No schema file: one warning instead of the schema checks, no panic.
        std::fs::remove_file(dir.path().join("db/schema.surql")).unwrap();
        let issues = lint_issues(&config, &path);
        assert!(
            issues
                .iter()
                .any(|i| i.message.contains("could not read the schema")),
            "{issues:?}"
        );
    }

    #[test]
    fn summary_reads_naturally() {
        assert_eq!(summary(&PushConfig::default()), "no rules");
        assert_eq!(summary(&cfg("rules: { r: { table: t, to: u } }")), "1 rule");
        assert_eq!(summary(&cfg("enabled: false")), "disabled");
    }
}
