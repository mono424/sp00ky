//! Push `schedules:` / `workflows:` from the manifest into `_00_schedule`.
//!
//! Runs right after the internal schema is applied, on both the deploy and the
//! `spky dev` path, because that is where the CLI already holds a root
//! connection. The rows are the engine's only source of truth — nothing reads
//! sp00ky.yml at runtime.
//!
//! Desired-state, with one hard rule: **deploy writes spec fields and nothing
//! else.** `paused` belongs to the operator and `next_fire_at` / `last_*` to the
//! engine, so a redeploy must not resurrect a schedule someone paused, or reset a
//! clock mid-cycle. The only exception is deliberate: when the spec hash changes,
//! `next_fire_at` is cleared so the engine replans against the new cadence.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::backend::{AppScope, AppType, BackendProcessor, ResolvedRetention, Sp00kyConfig};
use crate::migrate::checksum_str;
use crate::schedule_config::{normalize_schedule, normalize_workflow};
use crate::surreal_client::MigrationDB;

/// What one sync pass did, for the CLI's output.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub created: usize,
    pub updated: usize,
    pub unchanged: usize,
    pub removed: usize,
}

impl SyncReport {
    pub fn is_noop(&self) -> bool {
        self.created == 0 && self.updated == 0 && self.removed == 0
    }

    pub fn summary(&self) -> String {
        format!(
            "{} created, {} updated, {} unchanged, {} removed",
            self.created, self.updated, self.unchanged, self.removed
        )
    }
}

/// Resolve every schedule/workflow to the row the engine reads.
///
/// Returns `(name, spec_row)` pairs. Fails when a definition names a backend or
/// route that doesn't exist — the same check `spky lint` makes, repeated here so
/// a deploy can't write a schedule that could never run.
pub fn resolve_rows(
    config: &Sp00kyConfig,
    processor: &BackendProcessor,
    base_dir: &Path,
) -> Result<Vec<(String, Value)>> {
    let mut rows = Vec::new();
    // Project-wide default, applied to any schedule that doesn't state its own.
    let default_mode = config.default_history_mode();

    for (name, cfg) in &config.schedules {
        let label = format!("schedule '{name}'");
        let table = outbox_table_for(config, &cfg.backend, &label)?;
        check_route(config, processor, base_dir, &cfg.backend, &cfg.route, &label)?;
        rows.push((name.clone(), normalize_schedule(name, cfg, &table, default_mode)?));
    }

    for (name, cfg) in &config.workflows {
        // A workflow's default table comes from whichever backend its first step
        // uses; every step also carries its own when it differs.
        let mut step_tables = BTreeMap::new();
        for (step_name, step) in &cfg.steps {
            let label = format!("workflow '{name}' step '{step_name}'");
            let table = outbox_table_for(config, &step.backend, &label)?;
            check_route(config, processor, base_dir, &step.backend, &step.route, &label)?;
            step_tables.insert(step_name.clone(), table);
        }
        let default_table = cfg
            .steps
            .keys()
            .next()
            .and_then(|first| step_tables.get(first).cloned())
            .unwrap_or_default();
        rows.push((
            name.clone(),
            normalize_workflow(name, cfg, &default_table, &step_tables)?,
        ));
    }

    Ok(rows)
}

/// The outbox table a backend app writes jobs to.
///
/// Read from `apps.<name>.method.table` rather than from `BackendProcessor`,
/// because the processor deliberately skips `scope: devOnly` backends (they carry
/// no schema to apply) — and a dev-only backend is still a perfectly good target
/// for a schedule you run locally.
fn outbox_table_for(config: &Sp00kyConfig, backend: &str, label: &str) -> Result<String> {
    let app = config.apps.get(backend).with_context(|| {
        format!("{label} references backend '{backend}', which is not an app in sp00ky.yml")
    })?;
    if app.app_type != AppType::Backend {
        anyhow::bail!("{label} references '{backend}', which is not a backend app");
    }
    app.method
        .as_ref()
        .and_then(|m| m.table.clone())
        .with_context(|| {
            format!(
                "{label} references backend '{backend}', which has no outbox table \
                 (add `method: {{ type: outbox, table: … }}`)"
            )
        })
}

/// Backends a schedule targets that won't exist in a cloud deployment.
///
/// A `devOnly` backend is never deployed, so its schedule can fire locally but
/// would spawn jobs with nowhere to dispatch them in the cloud. Surfaced as a
/// lint warning rather than an error, because running one locally is legitimate.
pub fn dev_only_targets(config: &Sp00kyConfig) -> Vec<String> {
    let dev_only = |backend: &str| {
        config
            .apps
            .get(backend)
            .is_some_and(|app| app.scope == AppScope::DevOnly)
    };
    let mut out = Vec::new();
    for (name, cfg) in &config.schedules {
        if dev_only(&cfg.backend) {
            out.push(format!("schedule '{name}' targets dev-only backend '{}'", cfg.backend));
        }
    }
    for (name, cfg) in &config.workflows {
        for (step, s) in &cfg.steps {
            if dev_only(&s.backend) {
                out.push(format!(
                    "workflow '{name}' step '{step}' targets dev-only backend '{}'",
                    s.backend
                ));
            }
        }
    }
    out
}

fn check_route(
    config: &Sp00kyConfig,
    processor: &BackendProcessor,
    base_dir: &Path,
    backend: &str,
    route: &str,
    label: &str,
) -> Result<()> {
    // Prefer the processor's map (it already parsed the spec), and fall back to
    // reading the spec directly for backends it skipped — `devOnly` ones, which are
    // exactly the backends a locally-run schedule targets.
    let known: BTreeSet<String> = match processor.backend_definitions.get(backend) {
        Some(def) if !def.routes.is_empty() => def.routes.keys().cloned().collect(),
        _ => match declared_post_routes(config, base_dir, backend) {
            Some(routes) if !routes.is_empty() => routes,
            // No spec to check against: unknown, not empty. Don't reject on
            // missing information.
            _ => return Ok(()),
        },
    };

    if known.contains(route) {
        return Ok(());
    }
    anyhow::bail!(
        "{label} calls '{route}', which backend '{backend}' does not define. Known routes: {}",
        known.iter().cloned().collect::<Vec<_>>().join(", ")
    )
}

/// POST paths declared by a backend's OpenAPI spec.
///
/// A deliberately minimal read: schedules only need to know whether a path
/// exists, while `BackendProcessor` additionally types each route's arguments for
/// codegen. `None` when the app has no spec or it can't be read.
fn declared_post_routes(
    config: &Sp00kyConfig,
    base_dir: &Path,
    backend: &str,
) -> Option<BTreeSet<String>> {
    let spec_rel = config.apps.get(backend)?.spec.as_ref()?;
    let content = std::fs::read_to_string(base_dir.join(spec_rel)).ok()?;
    let spec: openapiv3::OpenAPI = serde_yaml::from_str(&content).ok()?;
    Some(
        spec.paths
            .paths
            .into_iter()
            .filter(|(_, item)| item.as_item().is_some_and(|i| i.post.is_some()))
            .map(|(path, _)| path)
            .collect(),
    )
}

/// Upsert the definitions and delete the ones that are gone.
pub fn sync(
    client: &dyn MigrationDB,
    config: &Sp00kyConfig,
    processor: &BackendProcessor,
    base_dir: &Path,
) -> Result<SyncReport> {
    // Before `resolve_rows`, deliberately. It returns `Err` when a single
    // schedule names a missing backend or an undefined route, and these two
    // writes have nothing to do with schedules — a project-wide policy must not
    // silently stop being written because one schedule entry is malformed.
    sync_retention(client, config, processor)?;
    sync_job_policy(client, config)?;
    sync_job_indexes(client, processor)?;

    let rows = resolve_rows(config, processor, base_dir)?;
    let mut report = SyncReport::default();

    let stored = read_stored_hashes(client)?;

    for (name, spec) in &rows {
        let canonical = serde_json::to_string(spec)?;
        let hash = checksum_str(&canonical);
        let existed = stored.contains_key(name);
        if stored.get(name).map(String::as_str) == Some(hash.as_str()) {
            report.unchanged += 1;
            continue;
        }

        // MERGE, not CONTENT: engine and operator fields on this row must
        // survive. And `next_fire_at = NONE` only here, on a real spec change,
        // so the engine replans the new cadence instead of firing on the old one.
        let sql = format!(
            "UPSERT {id} MERGE {patch}; UPDATE {id} SET next_fire_at = NONE;",
            id = record_literal(name),
            patch = merge_patch(spec, &hash)?,
        );
        client
            .execute(&sql)
            .with_context(|| format!("failed to sync schedule '{name}'"))?;

        if existed {
            report.updated += 1;
        } else {
            report.created += 1;
        }
    }

    // Sweep: anything config-owned that is no longer declared. Run history keeps
    // its own denormalized `schedule_name`, so it outlives the definition.
    let declared: Vec<&str> = rows.iter().map(|(name, _)| name.as_str()).collect();
    for name in stored.keys() {
        if !declared.contains(&name.as_str()) {
            client
                .execute(&format!("DELETE {};", record_literal(name)))
                .with_context(|| format!("failed to remove schedule '{name}'"))?;
            report.removed += 1;
        }
    }

    Ok(report)
}

/// Write the project's retention policy and the list of outbox tables the engine may
/// sweep to `_00_retention:default`.
///
/// The policy lives in the database rather than in an environment variable so that
/// singlenode, cluster, and Cloudflare deployments all read the identical thing
/// without another variable threaded through three different shells — and through
/// the separate cloud control plane, which assembles the SSP's environment
/// server-side. It also means retention can be retuned with one UPDATE and no
/// redeploy, the same way `spky schedules pause` works.
///
/// `job_tables` is the allowlist: the engine prunes nothing that is not named here,
/// so it never has to read sp00ky.yml or guess a table name.
fn sync_retention(
    client: &dyn MigrationDB,
    config: &Sp00kyConfig,
    processor: &BackendProcessor,
) -> Result<()> {
    let r = config.resolved_retention()?;
    let mut tables = crate::schema_builder::outbox_tables(processor);
    tables.sort();
    tables.dedup();
    let sql = retention_patch_sql(&r, &tables)?;
    client.execute(&sql).context("failed to write the retention policy")?;
    Ok(())
}

/// Build the retention UPSERT. Split out so a test can pin the field names against
/// the shipped DDL: the CLI writes them, the DDL defines them, and the engine reads
/// them, and a mismatch in any of the three is silent (an undefined field is
/// rejected on a SCHEMAFULL table; a field the engine doesn't read is ignored).
fn retention_patch_sql(r: &ResolvedRetention, tables: &[String]) -> Result<String> {
    // MERGE so an operator's manual tweak to a field this deploy does not own
    // survives, and so the row's own DEFAULTs still apply on first write.
    Ok(format!(
        "UPSERT _00_retention:default MERGE {{ success_secs: {}, failed_secs: {}, \
         run_success_secs: {}, run_failed_secs: {}, max_rows: {}, run_deadline_secs: {}, \
         job_tables: {} }};",
        r.success_secs,
        r.failed_secs,
        r.run_success_secs,
        r.run_failed_secs,
        r.max_rows,
        r.run_deadline_secs,
        serde_json::to_string(tables)?
    ))
}

/// Write each outbox table's execution limit to `_00_job_policy:⟨table⟩`.
///
/// Same reasoning as [`sync_retention`]: the policy lives in the database so all
/// three shells read the identical thing, and so it can be retuned with one
/// UPDATE and no redeploy — which matters more for a throttle than for
/// retention, because the reason you reach for one is that a backend is falling
/// over right now.
///
/// Only tables that declare `method.concurrency` get a row. An absent row means
/// the default of 1, so a project that never sets the key keeps exactly the
/// serial behavior it had before. There is deliberately no removal sweep: a row
/// for a table that no longer exists is inert, and keeping it preserves the
/// operator's setting if the table comes back.
fn sync_job_policy(client: &dyn MigrationDB, config: &Sp00kyConfig) -> Result<()> {
    for (table, concurrency) in declared_job_limits(config) {
        client
            .execute(&job_policy_upsert_sql(&table, concurrency))
            .with_context(|| format!("failed to write the job policy for '{table}'"))?;
    }
    Ok(())
}

/// Define the outbox tables' indexes, for projects whose schema predates them.
///
/// The scaffold writes these into the table's own `.surql` (`add_api.rs`), but
/// that file is generated ONCE, when `spky add api` runs. Every project created
/// before the indexes existed therefore has a schema that will never grow them
/// on its own, and those are exactly the projects big enough to need them: the
/// SSP dispatcher scans `WHERE status = 'pending'` on every job completion, the
/// retention prune scans `updated_at`, and the admin plane's queue counts scan
/// both.
///
/// `IF NOT EXISTS`, never `OVERWRITE`. `OVERWRITE` would rebuild both indexes on
/// every deploy, which on a table with a million rows is a deploy that looks
/// hung. With `IF NOT EXISTS` the build happens once, on the first deploy after
/// this ships, and costs nothing after that.
///
/// Best-effort per table: a project whose outbox table does not exist yet (a
/// first deploy, where the table is created by the schema batch that runs after
/// this) must not fail the whole deploy over an index.
fn sync_job_indexes(client: &dyn MigrationDB, processor: &BackendProcessor) -> Result<()> {
    let mut tables = crate::schema_builder::outbox_tables(processor);
    tables.sort();
    tables.dedup();
    for table in tables {
        if let Err(e) = client.execute(&job_index_sql(&table)) {
            // A warning, not a failure: the indexes are an optimization, and a
            // deploy that refuses to finish without one is worse than a deploy
            // that is slower than it could be.
            eprintln!("  warning: could not define the indexes on '{table}': {e:#}");
        }
    }
    Ok(())
}

/// The two index definitions for one outbox table. Split out so a test can pin
/// the shape against the scaffold's, which has to say the same thing.
fn job_index_sql(table: &str) -> String {
    format!(
        "DEFINE INDEX IF NOT EXISTS idx_{table}_dispatch ON TABLE {table} COLUMNS status, created_at; \
         DEFINE INDEX IF NOT EXISTS idx_{table}_activity ON TABLE {table} COLUMNS updated_at;"
    )
}

/// `(outbox table, concurrency)` for every backend that declares a limit.
/// Sorted and deduplicated so two apps pointing at one table write once, and so
/// the statement order is stable across runs.
fn declared_job_limits(config: &Sp00kyConfig) -> Vec<(String, u32)> {
    let mut out: BTreeMap<String, u32> = BTreeMap::new();
    for (_, app) in config.backends() {
        let Some(method) = &app.method else { continue };
        let Some(concurrency) = method.concurrency else { continue };
        let Some(table) = method.table.as_ref().filter(|t| !t.is_empty()) else { continue };
        // Two apps on one table is already a misconfiguration; take the tighter
        // bound rather than letting map order decide.
        out.entry(table.clone())
            .and_modify(|v| *v = (*v).min(concurrency))
            .or_insert(concurrency);
    }
    out.into_iter().collect()
}

/// Build one policy UPSERT. Split out so a test can pin the field names against
/// the shipped DDL, the same way [`retention_patch_sql`] is.
fn job_policy_upsert_sql(table: &str, concurrency: u32) -> String {
    // MERGE, not CONTENT, and clamped to >= 1: the DDL asserts `> 0`, and a
    // rejected statement here would fail the deploy over bookkeeping.
    format!(
        "UPSERT _00_job_policy:⟨{}⟩ MERGE {{ concurrency: {} }};",
        table.replace('⟩', ""),
        concurrency.max(1)
    )
}

/// `name -> spec_hash` for every config-owned schedule currently in the DB.
fn read_stored_hashes(client: &dyn MigrationDB) -> Result<BTreeMap<String, String>> {
    let responses = client
        .execute("SELECT name, spec_hash FROM _00_schedule;")
        .context("failed to read existing schedules")?;
    let mut out = BTreeMap::new();
    for response in responses {
        let Some(result) = response.result else { continue };
        let rows = match result {
            Value::Array(rows) => rows,
            other => vec![other],
        };
        for row in rows {
            let Some(name) = row.get("name").and_then(Value::as_str) else { continue };
            let hash = row.get("spec_hash").and_then(Value::as_str).unwrap_or_default();
            out.insert(name.to_string(), hash.to_string());
        }
    }
    Ok(out)
}

/// The `MERGE` object: spec fields plus the hash, and nothing else.
fn merge_patch(spec: &Value, hash: &str) -> Result<String> {
    let mut patch = spec.as_object().cloned().unwrap_or_default();
    patch.insert("spec_hash".into(), Value::String(hash.to_string()));
    debug_assert!(
        !patch.contains_key("paused")
            && !patch.contains_key("next_fire_at")
            && !patch.contains_key("last_fire_at")
            && !patch.contains_key("trigger_requested_at"),
        "deploy must never write operator- or engine-owned fields"
    );
    Ok(serde_json::to_string(&Value::Object(patch))?)
}

/// `_00_schedule:⟨name⟩`. The ⟨⟩ form quotes any key, which matters because
/// schedule names routinely contain hyphens.
pub fn record_literal(name: &str) -> String {
    format!("_00_schedule:⟨{}⟩", name.replace('⟩', ""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_literals_quote_hyphenated_names() {
        assert_eq!(record_literal("game-sync"), "_00_schedule:⟨game-sync⟩");
    }

    /// The whole point of MERGE: a deploy must not touch operator or engine state.
    #[test]
    fn the_merge_patch_carries_only_spec_fields() {
        let spec = serde_json::json!({
            "name": "nightly",
            "kind": "job",
            "cron": "0 3 * * *",
            "target_table": "job",
            "path": "/cleanup",
        });
        let patch = merge_patch(&spec, "abc123").unwrap();
        assert!(patch.contains("\"spec_hash\":\"abc123\""));
        for forbidden in ["paused", "next_fire_at", "last_fire_at", "trigger_requested_at"] {
            assert!(!patch.contains(forbidden), "patch must not mention {forbidden}");
        }
    }

    #[test]
    fn report_summarizes_and_detects_a_noop() {
        let mut report = SyncReport::default();
        report.unchanged = 3;
        assert!(report.is_noop());
        report.updated = 1;
        assert!(!report.is_noop());
        assert_eq!(report.summary(), "0 created, 1 updated, 3 unchanged, 0 removed");
    }
}

#[cfg(test)]
mod retention_sync_tests {
    use super::*;
    use crate::backend::RetentionConfig;

    /// The shipped DDL, so this test is about what actually deploys.
    const SCHEDULE_TABLES: &str = include_str!("schedule_tables.surql");

    /// Every field this UPSERT writes must be defined on `_00_retention`.
    ///
    /// `_00_retention` is SCHEMAFULL, so writing a field the table does not define
    /// fails the whole statement — and it fails at deploy time, on a statement whose
    /// only job is bookkeeping. Pinning the names here means renaming a DDL field
    /// breaks a unit test instead of someone's deploy.
    #[test]
    fn every_field_the_upsert_writes_is_defined_by_the_ddl() {
        let sql = retention_patch_sql(&RetentionConfig::DEFAULTS, &["job".to_string()])
            .expect("builds");
        for field in
            [
                "success_secs",
                "failed_secs",
                "run_success_secs",
                "run_deadline_secs",
                "run_failed_secs",
                "max_rows",
                "job_tables",
            ]
        {
            assert!(sql.contains(&format!("{field}:")), "the UPSERT should write {field}: {sql}");
            assert!(
                SCHEDULE_TABLES.contains(&format!("{field} ON TABLE _00_retention")),
                "`{field}` is written by deploy but not defined on _00_retention"
            );
        }
    }

    /// The row is MERGEd, never CONTENTed: `CONTENT` would drop any field this deploy
    /// does not name, including an operator's manual tweak.
    #[test]
    fn the_policy_is_merged_not_replaced() {
        let sql = retention_patch_sql(&RetentionConfig::DEFAULTS, &[]).expect("builds");
        assert!(sql.starts_with("UPSERT _00_retention:default MERGE"), "{sql}");
        assert!(!sql.contains("CONTENT"));
    }

    /// `job_tables` is the allowlist the engine prunes against, so it has to arrive as
    /// a JSON array of names — quoted, not a bare identifier list.
    #[test]
    fn job_tables_are_written_as_a_quoted_array() {
        let sql =
            retention_patch_sql(&RetentionConfig::DEFAULTS, &["job".into(), "stats_job".into()])
                .expect("builds");
        assert!(sql.contains(r#"job_tables: ["job","stats_job"]"#), "{sql}");

        // No outbox tables at all means an empty allowlist, i.e. prune nothing —
        // never "prune everything".
        let empty = retention_patch_sql(&RetentionConfig::DEFAULTS, &[]).expect("builds");
        assert!(empty.contains("job_tables: []"), "{empty}");
    }
}

#[cfg(test)]
mod job_policy_sync_tests {
    use super::*;

    /// The shipped DDL, so this test is about what actually deploys.
    const SCHEDULE_TABLES: &str = include_str!("schedule_tables.surql");

    /// An `OVERWRITE` here would rebuild both indexes on every single deploy,
    /// which on a large outbox table is a deploy that reads as hung. The whole
    /// point of running this on every deploy is that it is free after the first.
    #[test]
    fn job_indexes_are_defined_once_not_rebuilt_every_deploy() {
        let sql = job_index_sql("job");
        assert!(!sql.contains("OVERWRITE"), "{sql}");
        assert_eq!(sql.matches("IF NOT EXISTS").count(), 2, "{sql}");
    }

    /// The two indexes have to be the ones the three scanners actually ask for:
    /// the dispatcher's drain (`status`, then `created_at`), and the prune and
    /// the jobs listing (`updated_at`).
    #[test]
    fn job_indexes_cover_the_dispatch_and_activity_paths() {
        let sql = job_index_sql("stats_job");
        assert!(sql.contains("ON TABLE stats_job COLUMNS status, created_at"), "{sql}");
        assert!(sql.contains("ON TABLE stats_job COLUMNS updated_at"), "{sql}");
        // Names are per table, or two outbox tables would collide on one index.
        assert!(sql.contains("idx_stats_job_dispatch"), "{sql}");
        assert!(sql.contains("idx_stats_job_activity"), "{sql}");
    }

    /// Deploy and the `spky add api` scaffold must define the SAME indexes: a
    /// new project would otherwise get a different shape from a migrated one,
    /// and only one of the two would be the one the dispatcher is tuned for.
    #[test]
    fn deploy_defines_what_the_scaffold_does() {
        let scaffold = crate::add_api::outbox_template_for_test("job");
        for statement in job_index_sql("job").split(';') {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            assert!(
                scaffold.contains(statement),
                "the scaffold is missing `{statement}`"
            );
        }
    }

    /// Every field this UPSERT writes must be defined on `_00_job_policy`.
    /// SCHEMAFULL rejects the whole statement otherwise, at deploy time, on a
    /// statement whose only job is bookkeeping.
    #[test]
    fn every_field_the_upsert_writes_is_defined_by_the_ddl() {
        let sql = job_policy_upsert_sql("job", 8);
        for field in ["concurrency"] {
            assert!(sql.contains(&format!("{field}:")), "the UPSERT should write {field}: {sql}");
            assert!(
                SCHEDULE_TABLES.contains(&format!("{field} ON TABLE _00_job_policy")),
                "`{field}` is written by deploy but not defined on _00_job_policy"
            );
        }
    }

    /// The reader lives in another crate (`ssp-node`'s job dispatcher), which
    /// cannot `include_str!` this DDL. So pin the field names it reads here, in
    /// the crate that owns the schema — the mirror of
    /// `schedule_core::sql`'s retention-reader test.
    #[test]
    fn the_policy_row_defines_exactly_what_the_dispatcher_reads() {
        for field in ["concurrency"] {
            assert!(
                SCHEDULE_TABLES.contains(&format!("{field} ON TABLE _00_job_policy")),
                "ssp-node's dispatcher reads `{field}`, which _00_job_policy does not define"
            );
        }
    }

    /// MERGE, never CONTENT: an operator's live retune must survive the next deploy.
    #[test]
    fn the_policy_is_merged_not_replaced() {
        let sql = job_policy_upsert_sql("job", 4);
        assert!(sql.starts_with("UPSERT _00_job_policy:⟨job⟩ MERGE"), "{sql}");
        assert!(!sql.contains("CONTENT"));
    }

    /// The DDL asserts `> 0`. A 0 in the manifest must not fail the deploy.
    #[test]
    fn zero_is_clamped_to_one_rather_than_rejected() {
        assert!(job_policy_upsert_sql("job", 0).contains("concurrency: 1"));
    }

    /// ⟨⟩ quoting for the same reason schedule ids use it: table names may
    /// contain characters a bare record id would choke on.
    #[test]
    fn the_table_name_is_quoted() {
        assert!(job_policy_upsert_sql("stats_job", 2).contains("_00_job_policy:⟨stats_job⟩"));
    }
}
