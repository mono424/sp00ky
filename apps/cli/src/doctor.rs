//! `spky doctor` — fast structured diagnostics for a sp00ky project.
//!
//! Designed as the agent feedback loop after a schema or config edit:
//! every check is filesystem-only by default (sub-second), every result
//! has a `fix` hint that the agent can act on, and `--json` emits a
//! stable contract that LLMs can grep.
//!
//! Heavier checks (DB reachability, migration drift via DB) are gated
//! behind explicit flags so the default invocation stays fast.
use anyhow::Result;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::backend::{ClientFormat, Sp00kyConfig, DEFAULT_CONFIG_PATH};
use crate::package_manager;

const PREFIX: &str = "[sp00ky doctor]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Pass,
    Warn,
    Fail,
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Check {
    pub name: &'static str,
    pub severity: Severity,
    pub fix: Option<String>,
    pub detail: Option<String>,
}

impl Check {
    fn pass(name: &'static str, detail: impl Into<Option<String>>) -> Self {
        Self {
            name,
            severity: Severity::Pass,
            fix: None,
            detail: detail.into(),
        }
    }
    fn fail(name: &'static str, fix: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name,
            severity: Severity::Fail,
            fix: Some(fix.into()),
            detail: Some(detail.into()),
        }
    }
    fn warn(name: &'static str, fix: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name,
            severity: Severity::Warn,
            fix: Some(fix.into()),
            detail: Some(detail.into()),
        }
    }
    fn to_json(&self) -> Value {
        let mut obj = json!({
            "name": self.name,
            "severity": self.severity.as_str(),
            // Back-compat: callers that grepped `ok` keep working.
            "ok": self.severity != Severity::Fail,
        });
        if let Some(fix) = &self.fix {
            obj["fix"] = json!(fix);
        }
        if let Some(detail) = &self.detail {
            obj["detail"] = json!(detail);
        }
        obj
    }
}

/// `treat_warn_as_ok`: when true, warnings do not cause exit(1). Used by
/// `create` to surface docker-missing without aborting the scaffold.
pub fn run(emit_json: bool, project_dir: &Path, treat_warn_as_ok: bool) -> Result<()> {
    let mut checks: Vec<Check> = Vec::new();

    // 1. Config existence + parse
    let config_path = project_dir.join(DEFAULT_CONFIG_PATH);
    let config = match check_config(&config_path) {
        Ok((c, check)) => {
            checks.push(check);
            Some(c)
        }
        Err(check) => {
            checks.push(check);
            None
        }
    };

    // 2. Schema source files exist
    if let Some(cfg) = config.as_ref() {
        checks.push(check_schema_source(cfg, project_dir));
        // 3. Codegen freshness for each clientTypes output
        checks.extend(check_codegen_freshness(cfg, project_dir));
        // 4. Migrations dir presence (informational — not a hard failure)
        checks.push(check_migrations_dir(cfg, project_dir));
        // 5. Leftover client-recurring fields on an outbox schema
        checks.extend(check_legacy_recurring_fields(cfg, project_dir));
        // 5b. Security: migrations carrying the redacted-key placeholder
        checks.push(check_redacted_access_keys(cfg, project_dir));
    }

    // 6. Docker availability — warn (not fail). Local-dev users only need
    // docker for `spky migrate create`'s ephemeral DB.
    checks.push(check_docker());

    let has_fail = checks.iter().any(|c| c.severity == Severity::Fail);
    let has_warn = checks.iter().any(|c| c.severity == Severity::Warn);
    let overall_ok = !has_fail && !(has_warn && !treat_warn_as_ok);

    if emit_json {
        let payload = json!({
            "ok": overall_ok,
            "checks": checks.iter().map(Check::to_json).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        print_human(&checks, overall_ok);
    }

    if !overall_ok {
        std::process::exit(1);
    }
    Ok(())
}

fn check_config(config_path: &Path) -> std::result::Result<(Sp00kyConfig, Check), Check> {
    if !config_path.exists() {
        return Err(Check::fail(
            "config",
            "spky create  # scaffold sp00ky.yml",
            format!("{} not found", config_path.display()),
        ));
    }
    let content = match fs::read_to_string(config_path) {
        Ok(c) => c,
        Err(e) => {
            return Err(Check::fail(
                "config",
                "fix file permissions on sp00ky.yml",
                format!("{}: {}", config_path.display(), e),
            ));
        }
    };
    let base_dir = config_path.parent().unwrap_or(Path::new("."));
    match crate::backend::parse_config_with_includes(&content, base_dir) {
        Ok(cfg) => Ok((
            cfg,
            Check::pass("config", Some(format!("{}", config_path.display()))),
        )),
        Err(e) => Err(Check::fail(
            "config",
            "edit sp00ky.yml — see error detail for line/column",
            format!("yaml parse error: {}", e),
        )),
    }
}

fn check_schema_source(cfg: &Sp00kyConfig, project_dir: &Path) -> Check {
    let resolved = crate::backend::ResolvedSchema::from_config(&cfg.schema);
    let schema_path = project_dir.join(&resolved.schema);
    if schema_path.exists() {
        Check::pass("schema-source", Some(format!("{}", schema_path.display())))
    } else {
        Check::fail(
            "schema-source",
            format!("create {}", schema_path.display()),
            format!(
                "schema source not found at {} (configured via `schema:` in sp00ky.yml)",
                schema_path.display()
            ),
        )
    }
}

/// For each `clientTypes` entry: does the output exist, and is its mtime
/// at least as new as the latest `.surql` file under the schema dir?
fn check_codegen_freshness(cfg: &Sp00kyConfig, project_dir: &Path) -> Vec<Check> {
    if cfg.client_types.is_empty() {
        return vec![Check::pass(
            "codegen-fresh",
            Some("no clientTypes configured".to_string()),
        )];
    }

    let resolved = crate::backend::ResolvedSchema::from_config(&cfg.schema);
    let schema_root = project_dir.join(resolved.schema.parent().unwrap_or_else(|| Path::new(".")));
    let newest_source = newest_surql_mtime(&schema_root);

    let mut out = Vec::new();
    for ct in &cfg.client_types {
        let label: &'static str = match ct.format {
            ClientFormat::Typescript => "codegen-fresh:ts",
            ClientFormat::Dart => "codegen-fresh:dart",
        };
        let output_path = project_dir.join(&ct.output);
        if !output_path.exists() {
            out.push(Check::fail(
                label,
                "spky generate".to_string(),
                format!("output missing: {}", output_path.display()),
            ));
            continue;
        }
        let output_mtime = mtime(&output_path);
        match (output_mtime, newest_source) {
            (Some(out_t), Some(src_t)) if out_t < src_t => out.push(Check::fail(
                label,
                "spky generate".to_string(),
                format!(
                    "{} is older than the newest .surql under {}",
                    ct.output,
                    schema_root.display()
                ),
            )),
            _ => out.push(Check::pass(
                label,
                Some(format!("{}", output_path.display())),
            )),
        }
    }
    out
}

fn check_migrations_dir(cfg: &Sp00kyConfig, project_dir: &Path) -> Check {
    let resolved = crate::backend::ResolvedSchema::from_config(&cfg.schema);
    let path = project_dir.join(&resolved.migrations);
    if path.exists() {
        let count = fs::read_dir(&path)
            .map(|d| {
                d.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().map(|x| x == "surql").unwrap_or(false))
                    .count()
            })
            .unwrap_or(0);
        Check::pass(
            "migrations-dir",
            Some(format!("{} ({} migration file(s))", path.display(), count)),
        )
    } else {
        Check::fail(
            "migrations-dir",
            format!("mkdir -p {}", path.display()),
            format!("{} does not exist", path.display()),
        )
    }
}

/// Outbox schemas still carrying the fields the old client-driven recurring API
/// used (`recurring`, `interval`, `next_run_at`).
///
/// Nothing reads them any more — recurring work is declared under `schedules:` and
/// the scheduler creates a fresh job row per cycle. They are harmless as dead
/// columns, but a `recurring = true` row left in the table is not: with nothing to
/// re-arm it, the recovery sweep dispatches it once as a plain job and it then
/// terminalizes. Worth telling people about exactly once, at upgrade time.
fn check_legacy_recurring_fields(cfg: &Sp00kyConfig, project_dir: &Path) -> Vec<Check> {
    const LEGACY: [&str; 3] = ["recurring", "interval", "next_run_at"];
    let mut checks = Vec::new();

    for (name, app) in &cfg.apps {
        let Some(schema_rel) = app.method.as_ref().map(|m| m.schema.clone()) else { continue };
        let Ok(content) = fs::read_to_string(project_dir.join(&schema_rel)) else { continue };
        let found: Vec<&str> = LEGACY
            .iter()
            .copied()
            .filter(|field| {
                content
                    .lines()
                    .filter(|line| !line.trim_start().starts_with("--"))
                    .any(|line| line.contains(&format!("DEFINE FIELD {field} ")))
            })
            .collect();
        if found.is_empty() {
            continue;
        }
        checks.push(Check::warn(
            "outbox schema",
            format!(
                "drop {} from {} and delete any leftover rows: DELETE {} WHERE recurring = true",
                found.join(", "),
                schema_rel,
                app.method.as_ref().and_then(|m| m.table.clone()).unwrap_or_else(|| "job".into()),
            ),
            format!(
                "'{name}' still declares the client-recurring field(s) {} — unused since schedules \
                 moved to `schedules:` in sp00ky.yml",
                found.join(", ")
            ),
        ));
    }
    checks
}

/// A migration carrying `KEY '[REDACTED]'` is a forgeable-token hazard: applying
/// it sets the JWT signing key to the literal string "[REDACTED]", so anyone can
/// forge a session token for any record user. Fails hard — this is a live
/// security bug, not a style nit.
fn check_redacted_access_keys(cfg: &Sp00kyConfig, project_dir: &Path) -> Check {
    let resolved = crate::backend::ResolvedSchema::from_config(&cfg.schema);
    let dir = project_dir.join(&resolved.migrations);
    if !dir.exists() {
        return Check::pass("access-keys", None);
    }
    let mut offenders = Vec::new();
    for m in crate::migrate::scan_migrations(&dir).unwrap_or_default() {
        if let Ok(sql) = fs::read_to_string(&m.path) {
            if crate::migrate::check_no_redacted_key(&sql, "").is_err() {
                offenders.push(format!("{}_{}", m.version, m.name));
            }
        }
    }
    if offenders.is_empty() {
        Check::pass("access-keys", Some("no redacted JWT keys".to_string()))
    } else {
        Check::fail(
            "access-keys",
            "remove the `WITH JWT ... KEY '[REDACTED]'` clause (SurrealDB then generates a \
             random key) or supply an explicit key / {{VAULT}} placeholder, then \
             `spky migrate create` to regenerate; rotate the key on any DB already deployed",
            format!(
                "migration(s) {} apply a DEFINE ACCESS with KEY '[REDACTED]' — this sets the \
                 JWT signing key to the literal \"[REDACTED]\", allowing forged tokens",
                offenders.join(", ")
            ),
        )
    }
}

fn check_docker() -> Check {
    if package_manager::docker_available() {
        Check::pass("docker", Some("daemon reachable".to_string()))
    } else {
        Check::warn(
            "docker",
            "install Docker (https://docs.docker.com/get-docker/) and start the daemon",
            "docker not available — `spky migrate create` needs it for the ephemeral SurrealDB used to diff schemas".to_string(),
        )
    }
}

fn newest_surql_mtime(dir: &Path) -> Option<SystemTime> {
    let mut newest: Option<SystemTime> = None;
    walk(dir, &mut |p| {
        if p.extension().map(|e| e == "surql").unwrap_or(false) {
            if let Some(t) = mtime(p) {
                newest = Some(match newest {
                    Some(n) if n > t => n,
                    _ => t,
                });
            }
        }
    });
    newest
}

fn walk(dir: &Path, visit: &mut dyn FnMut(&Path)) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, visit);
        } else {
            visit(&path);
        }
    }
}

fn mtime(p: &Path) -> Option<SystemTime> {
    fs::metadata(p).and_then(|m| m.modified()).ok()
}

fn print_human(checks: &[Check], ok: bool) {
    println!();
    for c in checks {
        let mark = match c.severity {
            Severity::Pass => "\x1b[32m\u{2713}\x1b[0m",
            Severity::Warn => "\x1b[33m!\x1b[0m",
            Severity::Fail => "\x1b[31m\u{2717}\x1b[0m",
        };
        println!("  {} {}", mark, c.name);
        if let Some(detail) = &c.detail {
            println!("      {}", detail);
        }
        if let Some(fix) = &c.fix {
            println!("      fix: {}", fix);
        }
    }
    println!();
    let fails = checks
        .iter()
        .filter(|c| c.severity == Severity::Fail)
        .count();
    let warns = checks
        .iter()
        .filter(|c| c.severity == Severity::Warn)
        .count();
    if ok && warns == 0 {
        println!("{} OK", PREFIX);
    } else if fails > 0 {
        println!("{} {} failed, {} warning(s)", PREFIX, fails, warns);
    } else {
        println!("{} {} warning(s)", PREFIX, warns);
    }
}

#[allow(dead_code)]
pub fn _unused(_: PathBuf) {} // keep PathBuf import warning quiet across feature flags
