//! `spky flag` — built-in feature flag management.
//!
//! Writes flag definitions to `_00_feature_flag` and materializes per-user
//! assignments into `_00_user_feature` by running the evaluator in-process.
//!
//! Ordinary clients can neither read the definitions nor write assignments:
//! `_00_feature_flag` is invisible to them and `_00_user_feature` is
//! read-your-own. Users listed in `_00_admin` (see `spky admin`) are the one
//! exception — they can flip `enabled` and edit allowlist rules from the
//! DevTools panel via `fn::feature::materialize`. Flag creation and deletion
//! stay root-only, i.e. this command.
//!
//! The percentage-rollout hash here must match `fn::feature::hash` in
//! `apps/cli/src/meta_tables_remote.surql`. Both sides take the first 8
//! hex chars of SHA-256("<key>:<user_id>") as an unsigned int and apply
//! modulo 100, so a CLI write and a later scheduler sweep produce
//! identical bucket assignments.

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

use crate::surreal_client::{MigrationDB, SurrealClient, SurrealResponse};
use crate::{ConnectionArgs, FlagCommands};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

const RULE_ALLOWLIST: &str = "allowlist";
const RULE_ROLLOUT: &str = "rollout";
const PRIORITY_ALLOWLIST: i64 = 10;
const PRIORITY_ROLLOUT: i64 = 50;

pub fn run(action: FlagCommands) -> Result<()> {
    match action {
        FlagCommands::List { conn, config } => list(conn, config),
        FlagCommands::Create {
            key,
            variants,
            default,
            description,
            conn,
            config,
        } => create(key, variants, default, description, conn, config),
        FlagCommands::Delete { key, conn, config } => delete(key, conn, config),
        FlagCommands::Get { key, conn, config } => get(key, conn, config),
        FlagCommands::Enable { key, conn, config } => set_enabled(key, true, conn, config),
        FlagCommands::Disable { key, conn, config } => set_enabled(key, false, conn, config),
        FlagCommands::Set {
            key,
            variant,
            for_user,
            rollout,
            sql,
            conn,
            config,
        } => set_rule(key, variant, for_user, rollout, sql, conn, config),
        FlagCommands::Unset {
            key,
            for_user,
            conn,
            config,
        } => unset_rule(key, for_user, conn, config),
        FlagCommands::Eval {
            key,
            as_user,
            conn,
            config,
        } => eval(key, as_user, conn, config),
    }
}

// =============================================================
// Connection
// =============================================================

pub(crate) fn client_from(conn: ConnectionArgs, config: Option<PathBuf>) -> Result<SurrealClient> {
    // `--cloud` resolves the deployment's SurrealDB URL + root password from
    // Sp00ky Cloud; otherwise the local URL/ns/db come from sp00ky.yml.
    let c = conn.resolve(&config)?;
    Ok(SurrealClient::new(
        &c.url,
        &c.namespace,
        &c.database,
        &c.username,
        &c.password,
    ))
}

// =============================================================
// SurrealQL helpers
// =============================================================

pub(crate) fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

pub(crate) fn first_row(responses: Vec<SurrealResponse>) -> Option<Value> {
    let first = responses.into_iter().next()?;
    let result = first.result?;
    match result {
        Value::Array(mut arr) => arr.drain(..).next(),
        other => Some(other),
    }
}

pub(crate) fn rows(responses: Vec<SurrealResponse>) -> Vec<Value> {
    let first = match responses.into_iter().next() {
        Some(r) => r,
        None => return vec![],
    };
    match first.result {
        Some(Value::Array(arr)) => arr,
        Some(other) => vec![other],
        None => vec![],
    }
}

fn load_flag(client: &SurrealClient, key: &str) -> Result<Value> {
    let query = format!(
        "SELECT * FROM _00_feature_flag WHERE key = '{}' LIMIT 1;",
        esc(key)
    );
    let resp = client.execute(&query).context("Failed to load flag")?;
    first_row(resp).ok_or_else(|| anyhow!("Flag '{}' not found", key))
}

pub(crate) fn load_user_id(client: &SurrealClient, who: &str) -> Result<String> {
    if who.starts_with("user:") {
        return Ok(who.to_string());
    }
    let query = format!(
        "SELECT VALUE id FROM user WHERE username = '{}' LIMIT 1;",
        esc(who)
    );
    let resp = client
        .execute(&query)
        .context("Failed to look up user by username")?;
    let row = first_row(resp).ok_or_else(|| anyhow!("User '{}' not found", who))?;
    match row {
        Value::String(id) => Ok(id),
        Value::Object(map) => map
            .get("id")
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("User '{}' returned unexpected shape", who)),
        other => Err(anyhow!("User lookup returned unexpected value: {}", other)),
    }
}

// =============================================================
// Commands
// =============================================================

fn list(conn: ConnectionArgs, config: Option<PathBuf>) -> Result<()> {
    let client = client_from(conn, config)?;
    let resp = client
        .execute("SELECT key, enabled, variants, default_variant, array::len(rules) AS rule_count FROM _00_feature_flag ORDER BY key ASC;")
        .context("Failed to list flags")?;
    let flags = rows(resp);
    if flags.is_empty() {
        println!("{}No flags defined.{}", DIM, RESET);
        return Ok(());
    }
    println!(
        "{}KEY                    ENABLED  DEFAULT  VARIANTS               RULES{}",
        BOLD, RESET
    );
    for f in flags {
        let key = f.get("key").and_then(Value::as_str).unwrap_or("?");
        let enabled = f.get("enabled").and_then(Value::as_bool).unwrap_or(false);
        let default = f
            .get("default_variant")
            .and_then(Value::as_str)
            .unwrap_or("off");
        let variants = f
            .get("variants")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let rule_count = f.get("rule_count").and_then(Value::as_i64).unwrap_or(0);
        let on = if enabled {
            format!("{}on{}", GREEN, RESET)
        } else {
            format!("{}off{}", YELLOW, RESET)
        };
        println!(
            "{:<22} {:<16}  {:<7}  {:<22} {}",
            key, on, default, variants, rule_count
        );
    }
    Ok(())
}

fn create(
    key: String,
    variants: Option<String>,
    default: Option<String>,
    description: Option<String>,
    conn: ConnectionArgs,
    config: Option<PathBuf>,
) -> Result<()> {
    let client = client_from(conn, config)?;

    let variants: Vec<String> = variants
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_else(|| vec!["off".to_string(), "on".to_string()]);
    if variants.len() < 2 {
        bail!("A flag needs at least two variants (e.g. --variants off,on)");
    }
    let default = default.unwrap_or_else(|| variants[0].clone());
    if !variants.contains(&default) {
        bail!(
            "Default variant '{}' is not in variants [{}]",
            default,
            variants.join(", ")
        );
    }

    let variants_sql = variants
        .iter()
        .map(|v| format!("'{}'", esc(v)))
        .collect::<Vec<_>>()
        .join(", ");

    let description_sql = description
        .map(|d| format!("'{}'", esc(&d)))
        .unwrap_or_else(|| "NONE".to_string());

    let query = format!(
        "CREATE _00_feature_flag SET key = '{}', variants = [{}], default_variant = '{}', description = {}, rules = [], enabled = true;",
        esc(&key),
        variants_sql,
        esc(&default),
        description_sql
    );
    client.execute(&query).context("Failed to create flag")?;

    println!(
        "{}Created flag{} '{}' with variants [{}], default '{}'.",
        GREEN,
        RESET,
        key,
        variants.join(", "),
        default
    );

    materialize(&client, &key)?;
    Ok(())
}

fn delete(key: String, conn: ConnectionArgs, config: Option<PathBuf>) -> Result<()> {
    let client = client_from(conn, config)?;
    let _ = load_flag(&client, &key)?;
    let query = format!(
        "DELETE _00_feature_flag WHERE key = '{}'; DELETE _00_user_feature WHERE key = '{}';",
        esc(&key),
        esc(&key)
    );
    client.execute(&query).context("Failed to delete flag")?;
    println!("{}Deleted flag{} '{}'.", GREEN, RESET, key);
    Ok(())
}

fn get(key: String, conn: ConnectionArgs, config: Option<PathBuf>) -> Result<()> {
    let client = client_from(conn, config)?;
    let flag = load_flag(&client, &key)?;
    println!("{}{}{}", BOLD, key, RESET);
    let pretty = serde_json::to_string_pretty(&flag).unwrap_or_else(|_| flag.to_string());
    println!("{}", pretty);

    let count_query = format!(
        "SELECT count() AS n FROM _00_user_feature WHERE key = '{}' GROUP ALL;",
        esc(&key)
    );
    let count_rows = rows(client.execute(&count_query)?);
    let n = count_rows
        .first()
        .and_then(|r| r.get("n"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    println!("{}Materialized assignments: {}{}", DIM, n, RESET);
    Ok(())
}

fn set_enabled(
    key: String,
    enabled: bool,
    conn: ConnectionArgs,
    config: Option<PathBuf>,
) -> Result<()> {
    let client = client_from(conn, config)?;
    let _ = load_flag(&client, &key)?;
    let query = format!(
        "UPDATE _00_feature_flag SET enabled = {} WHERE key = '{}';",
        if enabled { "true" } else { "false" },
        esc(&key)
    );
    client.execute(&query).context("Failed to update enabled")?;
    println!(
        "{}{}{} flag '{}'.",
        if enabled { GREEN } else { YELLOW },
        if enabled { "Enabled" } else { "Disabled" },
        RESET,
        key
    );
    materialize(&client, &key)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn set_rule(
    key: String,
    variant: String,
    for_user: Option<String>,
    rollout: Option<u32>,
    sql: Option<String>,
    conn: ConnectionArgs,
    config: Option<PathBuf>,
) -> Result<()> {
    let provided = for_user.is_some() as u8 + rollout.is_some() as u8 + sql.is_some() as u8;
    if provided != 1 {
        bail!("Pass exactly one of --for-user / --rollout / --sql");
    }

    let client = client_from(conn, config)?;
    let flag = load_flag(&client, &key)?;
    let variants = flag
        .get("variants")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !variants.contains(&variant) {
        bail!(
            "Variant '{}' is not declared on flag '{}' (declared: [{}])",
            variant,
            key,
            variants.join(", ")
        );
    }

    let mut rules = flag
        .get("rules")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    if let Some(who) = for_user {
        let user_id = load_user_id(&client, &who)?;
        upsert_allowlist(&mut rules, &variant, &user_id);
        println!(
            "{}Allowlisted{} user '{}' for variant '{}' on flag '{}'.",
            GREEN, RESET, who, variant, key
        );
    } else if let Some(pct) = rollout {
        if pct > 100 {
            bail!("--rollout must be between 0 and 100");
        }
        upsert_rollout(&mut rules, &variant, pct as i64);
        println!(
            "{}Set rollout{} for variant '{}' on flag '{}' to {}%.",
            GREEN, RESET, variant, key, pct
        );
    } else if let Some(query) = sql {
        // Expand the SELECT into an explicit list of user ids and fold them into
        // the allowlist rule. Storing the resolved ids (rather than the query)
        // keeps the existing tri-plex evaluator unchanged and survives the
        // re-materialization that enable/disable/other rules trigger — a one-shot
        // write to `_00_user_feature` would be clobbered on the next sweep.
        let ids = select_user_ids(&client, &query)?;
        if ids.is_empty() {
            println!(
                "{}No users matched the query; nothing to assign.{}",
                YELLOW, RESET
            );
            return Ok(());
        }
        preview_ids(&ids, &variant, &key);
        if !crate::ui::consent(&format!("Apply to all {} user(s)?", ids.len()))? {
            println!("{}Aborted; no changes written.{}", YELLOW, RESET);
            return Ok(());
        }
        for id in &ids {
            upsert_allowlist(&mut rules, &variant, id);
        }
        println!(
            "{}Allowlisted{} {} user(s) for variant '{}' on flag '{}'.",
            GREEN,
            RESET,
            ids.len(),
            variant,
            key
        );
    }

    write_rules(&client, &key, &rules)?;
    materialize(&client, &key)?;
    Ok(())
}

/// Run a user-supplied SELECT and collect the `user:` record ids it yields.
/// Accepts both `SELECT id FROM user ...` (objects with an `id`) and
/// `SELECT VALUE id FROM user ...` (bare strings). Read-only: rejects anything
/// that is not a SELECT so a stray write can't run during the "preview" step.
fn select_user_ids(client: &SurrealClient, query: &str) -> Result<Vec<String>> {
    let trimmed = query.trim();
    if !trimmed.to_uppercase().starts_with("SELECT") {
        bail!("--sql must be a SELECT query that returns user ids");
    }
    let resp = client
        .execute(trimmed)
        .context("Failed to run the assignment query")?;
    let (ids, skipped) = extract_user_ids(rows(resp));
    if skipped > 0 {
        println!(
            "{}Skipped {} row(s) that were not `user:` records.{}",
            YELLOW, skipped, RESET
        );
    }
    Ok(ids)
}

/// Pull `user:` ids out of query rows, deduping while preserving order. Returns
/// the ids plus the count of rows that resolved to a non-`user:` id (skipped).
fn extract_user_ids(rows: Vec<Value>) -> (Vec<String>, usize) {
    let mut ids = Vec::new();
    let mut skipped = 0usize;
    for row in rows {
        let id = match row {
            Value::String(s) => Some(s),
            Value::Object(map) => map.get("id").and_then(Value::as_str).map(String::from),
            _ => None,
        };
        match id {
            Some(s) if s.starts_with("user:") => ids.push(s),
            Some(_) => skipped += 1,
            None => {}
        }
    }
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));
    (ids, skipped)
}

/// Print the matched ids (first 10) and the total count before applying.
fn preview_ids(ids: &[String], variant: &str, key: &str) {
    const MAX: usize = 10;
    println!(
        "{}Matched {} user(s){} for variant '{}' on flag '{}':",
        BOLD,
        ids.len(),
        RESET,
        variant,
        key
    );
    for id in ids.iter().take(MAX) {
        println!("  {}{}{}", DIM, id, RESET);
    }
    if ids.len() > MAX {
        println!("  {}... and {} more{}", DIM, ids.len() - MAX, RESET);
    }
}

fn unset_rule(
    key: String,
    for_user: String,
    conn: ConnectionArgs,
    config: Option<PathBuf>,
) -> Result<()> {
    let client = client_from(conn, config)?;
    let flag = load_flag(&client, &key)?;
    let user_id = load_user_id(&client, &for_user)?;
    let mut rules = flag
        .get("rules")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut removed = false;
    for rule in rules.iter_mut() {
        if rule.get("kind").and_then(Value::as_str) != Some(RULE_ALLOWLIST) {
            continue;
        }
        if let Some(Value::Array(users)) = rule.get_mut("users") {
            let before = users.len();
            users.retain(|u| u.as_str() != Some(user_id.as_str()));
            if users.len() != before {
                removed = true;
            }
        }
    }
    rules.retain(|r| {
        if r.get("kind").and_then(Value::as_str) == Some(RULE_ALLOWLIST) {
            r.get("users")
                .and_then(Value::as_array)
                .map(|a| !a.is_empty())
                .unwrap_or(false)
        } else {
            true
        }
    });

    if !removed {
        println!(
            "{}User '{}' was not on any allowlist for flag '{}'.{}",
            YELLOW, for_user, key, RESET
        );
        return Ok(());
    }

    write_rules(&client, &key, &rules)?;
    materialize(&client, &key)?;
    println!(
        "{}Removed{} user '{}' from flag '{}'.",
        GREEN, RESET, for_user, key
    );
    Ok(())
}

fn eval(key: String, as_user: String, conn: ConnectionArgs, config: Option<PathBuf>) -> Result<()> {
    let client = client_from(conn, config)?;
    let flag = load_flag(&client, &key)?;
    let user_id = load_user_id(&client, &as_user)?;
    let (variant, payload) = evaluate_one(&flag, &user_id);
    println!("flag    : {}", key);
    println!("user    : {}", user_id);
    println!("variant : {}", variant);
    if let Some(p) = payload {
        println!(
            "payload : {}",
            serde_json::to_string_pretty(&p).unwrap_or_default()
        );
    }
    Ok(())
}

// =============================================================
// Rule manipulation
// =============================================================

fn upsert_allowlist(rules: &mut Vec<Value>, variant: &str, user_id: &str) {
    for rule in rules.iter_mut() {
        if rule.get("kind").and_then(Value::as_str) == Some(RULE_ALLOWLIST)
            && rule.get("variant").and_then(Value::as_str) == Some(variant)
        {
            if let Some(Value::Array(users)) = rule.get_mut("users") {
                if !users.iter().any(|u| u.as_str() == Some(user_id)) {
                    users.push(Value::String(user_id.to_string()));
                }
                return;
            }
        }
    }
    rules.push(json!({
        "kind": RULE_ALLOWLIST,
        "variant": variant,
        "users": [user_id],
        "priority": PRIORITY_ALLOWLIST,
    }));
}

fn upsert_rollout(rules: &mut Vec<Value>, variant: &str, percent: i64) {
    for rule in rules.iter_mut() {
        if rule.get("kind").and_then(Value::as_str) == Some(RULE_ROLLOUT)
            && rule.get("variant").and_then(Value::as_str) == Some(variant)
        {
            if let Value::Object(map) = rule {
                map.insert("percent".into(), Value::Number(percent.into()));
            }
            return;
        }
    }
    rules.push(json!({
        "kind": RULE_ROLLOUT,
        "variant": variant,
        "percent": percent,
        "priority": PRIORITY_ROLLOUT,
    }));
}

fn write_rules(client: &SurrealClient, key: &str, rules: &[Value]) -> Result<()> {
    let json_str = serde_json::to_string(rules).unwrap_or_else(|_| "[]".to_string());
    let query = format!(
        "UPDATE _00_feature_flag SET rules = {} WHERE key = '{}';",
        json_str,
        esc(key)
    );
    client
        .execute(&query)
        .context("Failed to write updated rules")?;
    Ok(())
}

// =============================================================
// Evaluator (matches SurrealQL fn::feature::hash bucketing)
// =============================================================

fn evaluate_one(flag: &Value, user_id: &str) -> (String, Option<Value>) {
    let default = flag
        .get("default_variant")
        .and_then(Value::as_str)
        .unwrap_or("off")
        .to_string();
    let enabled = flag.get("enabled").and_then(Value::as_bool).unwrap_or(true);
    let payloads = flag.get("payloads").cloned();
    let resolve = |variant: &str| -> Option<Value> {
        payloads.as_ref().and_then(|p| p.get(variant).cloned())
    };

    if !enabled {
        let payload = resolve(&default);
        return (default, payload);
    }

    let key = flag.get("key").and_then(Value::as_str).unwrap_or("");
    let empty: Vec<Value> = vec![];
    let mut rules: Vec<&Value> = flag
        .get("rules")
        .and_then(Value::as_array)
        .unwrap_or(&empty)
        .iter()
        .collect();
    rules.sort_by_key(|r| r.get("priority").and_then(Value::as_i64).unwrap_or(100));

    for rule in rules {
        let kind = rule.get("kind").and_then(Value::as_str).unwrap_or("");
        let variant = match rule.get("variant").and_then(Value::as_str) {
            Some(v) => v.to_string(),
            None => continue,
        };
        match kind {
            "allowlist" => {
                let hit = rule
                    .get("users")
                    .and_then(Value::as_array)
                    .map(|users| users.iter().any(|u| u.as_str() == Some(user_id)))
                    .unwrap_or(false);
                if hit {
                    return (variant.clone(), resolve(&variant));
                }
            }
            "rollout" => {
                let pct = rule.get("percent").and_then(Value::as_i64).unwrap_or(0);
                if pct > 0 && rollout_hash(key, user_id) < pct {
                    return (variant.clone(), resolve(&variant));
                }
            }
            _ => {}
        }
    }

    let payload = resolve(&default);
    (default, payload)
}

/// Stable 0..99 bucket. Must match `fn::feature::hash` in
/// `meta_tables_remote.surql`: SHA-256("<key>:<user_id>"), take the first
/// 8 hex chars as a big-endian u32, modulo 100.
fn rollout_hash(key: &str, user_id: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.update(b":");
    hasher.update(user_id.as_bytes());
    let digest = hasher.finalize();
    let prefix = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    (prefix as i64) % 100
}

// =============================================================
// Materialization
// =============================================================

fn materialize(client: &SurrealClient, key: &str) -> Result<()> {
    let flag = match load_flag(client, key) {
        Ok(f) => f,
        Err(_) => {
            client.execute(&format!(
                "DELETE _00_user_feature WHERE key = '{}';",
                esc(key)
            ))?;
            return Ok(());
        }
    };

    let users = rows(
        client
            .execute("SELECT VALUE id FROM user;")
            .context("Failed to enumerate users for materialization")?,
    );
    if users.is_empty() {
        return Ok(());
    }

    let mut statements: Vec<String> = Vec::with_capacity(users.len());
    for user in users {
        let user_id = match user.as_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let (variant, payload) = evaluate_one(&flag, &user_id);
        let payload_sql = payload
            .map(|p| serde_json::to_string(&p).unwrap_or_else(|_| "NONE".to_string()))
            .unwrap_or_else(|| "NONE".to_string());
        statements.push(format!(
            "UPSERT _00_user_feature SET user = {}, key = '{}', variant = '{}', payload = {} WHERE user = {} AND key = '{}';",
            user_id,
            esc(key),
            esc(&variant),
            payload_sql,
            user_id,
            esc(key),
        ));
    }

    if statements.is_empty() {
        return Ok(());
    }

    let batched = statements.join("\n");
    client
        .execute(&batched)
        .context("Failed to materialize user feature assignments")?;

    println!(
        "{}Materialized{} {} assignment(s) for flag '{}'.",
        DIM,
        RESET,
        statements.len(),
        key
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- rollout_hash --------------------------------------------------

    #[test]
    fn rollout_hash_is_deterministic_and_bounded() {
        for i in 0..1000 {
            let user = format!("user:{}", i);
            let h = rollout_hash("flag-x", &user);
            assert!((0..100).contains(&h), "hash {} out of range", h);
            assert_eq!(h, rollout_hash("flag-x", &user), "not deterministic");
        }
    }

    /// Golden vectors. These exact values are also asserted by the scheduler's
    /// evaluator (`apps/scheduler/src/feature_flags.rs`) and must equal
    /// `fn::feature::hash` in `meta_tables_remote.surql`. If any of the three
    /// implementations drifts, its copy of this test fails — that is the whole
    /// point of pinning concrete numbers rather than just checking determinism.
    #[test]
    fn rollout_hash_matches_golden_vectors() {
        assert_eq!(rollout_hash("checkout-v2", "user:alice"), 8);
        assert_eq!(rollout_hash("checkout-v2", "user:bob"), 62);
        assert_eq!(rollout_hash("flag-x", "user:abc"), 0);
        assert_eq!(rollout_hash("beta", "user:42"), 90);
        assert_eq!(rollout_hash("rollout", "user:zzz"), 17);
    }

    // ---- evaluate_one (must match feature_flags.rs::evaluate_one) -------

    #[test]
    fn disabled_flag_returns_default_regardless_of_rules() {
        let flag = json!({
            "key": "x", "enabled": false, "default_variant": "off",
            "rules": [{ "kind": "allowlist", "variant": "on", "users": ["user:abc"], "priority": 10 }],
        });
        assert_eq!(evaluate_one(&flag, "user:abc").0, "off");
    }

    #[test]
    fn rollout_at_zero_never_matches_and_hundred_always_matches() {
        let zero = json!({
            "key": "x", "enabled": true, "default_variant": "off",
            "rules": [{ "kind": "rollout", "variant": "on", "percent": 0, "priority": 50 }],
        });
        let hundred = json!({
            "key": "x", "enabled": true, "default_variant": "off",
            "rules": [{ "kind": "rollout", "variant": "on", "percent": 100, "priority": 50 }],
        });
        for i in 0..50 {
            let u = format!("user:{}", i);
            assert_eq!(evaluate_one(&zero, &u).0, "off");
            assert_eq!(evaluate_one(&hundred, &u).0, "on");
        }
    }

    #[test]
    fn allowlist_beats_lower_priority_rollout() {
        let flag = json!({
            "key": "x", "enabled": true, "default_variant": "off",
            "rules": [
                { "kind": "rollout", "variant": "on", "percent": 0, "priority": 50 },
                { "kind": "allowlist", "variant": "on", "users": ["user:abc"], "priority": 10 },
            ],
        });
        assert_eq!(evaluate_one(&flag, "user:abc").0, "on");
        assert_eq!(evaluate_one(&flag, "user:xyz").0, "off");
    }

    #[test]
    fn payload_resolves_for_chosen_variant() {
        let flag = json!({
            "key": "x", "enabled": true, "default_variant": "off",
            "rules": [{ "kind": "allowlist", "variant": "treatment", "users": ["user:abc"], "priority": 10 }],
            "payloads": { "treatment": { "copy": "Hello" } },
        });
        let (v, p) = evaluate_one(&flag, "user:abc");
        assert_eq!(v, "treatment");
        assert_eq!(p, Some(json!({ "copy": "Hello" })));
    }

    // ---- builder -> evaluator round-trip -------------------------------
    // Guards that the rule shape the CLI WRITES (`kind`/`users`/`percent`/
    // `priority`) is exactly what the evaluator READS. A drift here is the
    // "schema doc says allowlist/rollout_percent but code uses kind/users/
    // percent" class of bug.

    #[test]
    fn upsert_allowlist_round_trips_through_evaluator() {
        let mut rules: Vec<Value> = vec![];
        upsert_allowlist(&mut rules, "on", "user:abc");
        // Idempotent: same user again does not duplicate.
        upsert_allowlist(&mut rules, "on", "user:abc");
        assert_eq!(rules.len(), 1);
        let users = rules[0]["users"].as_array().unwrap();
        assert_eq!(users.len(), 1, "allowlist must not duplicate a user");

        let flag = json!({ "key": "k", "enabled": true, "default_variant": "off", "rules": rules });
        assert_eq!(evaluate_one(&flag, "user:abc").0, "on");
        assert_eq!(evaluate_one(&flag, "user:other").0, "off");
    }

    #[test]
    fn upsert_rollout_round_trips_and_updates_percent() {
        let mut rules: Vec<Value> = vec![];
        upsert_rollout(&mut rules, "on", 100);
        let flag = json!({ "key": "k", "enabled": true, "default_variant": "off", "rules": rules.clone() });
        assert_eq!(
            evaluate_one(&flag, "user:anyone").0,
            "on",
            "100% must match everyone"
        );

        // Re-upsert lowers it to 0 in place (no duplicate rule).
        upsert_rollout(&mut rules, "on", 0);
        assert_eq!(rules.len(), 1, "rollout re-upsert must update in place");
        let flag0 =
            json!({ "key": "k", "enabled": true, "default_variant": "off", "rules": rules });
        assert_eq!(
            evaluate_one(&flag0, "user:anyone").0,
            "off",
            "0% must match no one"
        );
    }

    #[test]
    fn allowlist_priority_beats_rollout_when_built_via_builders() {
        // Built only through the public builders (so the PRIORITY_* constants,
        // not hand-written numbers, decide precedence).
        let mut rules: Vec<Value> = vec![];
        upsert_rollout(&mut rules, "rolled", 0);
        upsert_allowlist(&mut rules, "listed", "user:abc");
        let flag = json!({ "key": "k", "enabled": true, "default_variant": "off", "rules": rules });
        assert_eq!(evaluate_one(&flag, "user:abc").0, "listed");
    }

    // ---- extract_user_ids ------------------------------------------------

    #[test]
    fn extract_user_ids_handles_value_and_object_rows() {
        // `SELECT VALUE id` -> strings; `SELECT id` -> { id }.
        let rows = vec![json!("user:a"), json!({ "id": "user:b", "age": 30 })];
        let (ids, skipped) = extract_user_ids(rows);
        assert_eq!(ids, vec!["user:a", "user:b"]);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn extract_user_ids_dedupes_preserving_order() {
        let rows = vec![json!("user:a"), json!("user:b"), json!("user:a")];
        let (ids, _) = extract_user_ids(rows);
        assert_eq!(ids, vec!["user:a", "user:b"]);
    }

    #[test]
    fn extract_user_ids_skips_non_user_records() {
        // Ids from another table (or no id at all) are counted as skipped, never
        // allowlisted — assignments key on `user`.
        let rows = vec![
            json!("user:a"),
            json!({ "id": "post:1" }),
            json!({ "name": "no-id" }),
            json!(42),
        ];
        let (ids, skipped) = extract_user_ids(rows);
        assert_eq!(ids, vec!["user:a"]);
        assert_eq!(skipped, 1, "only the post:1 id counts as a skipped record");
    }
}
