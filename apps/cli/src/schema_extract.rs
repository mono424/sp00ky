use anyhow::{bail, Context, Result};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

use crate::migrate;
use crate::schema_diff::{extract_statement_key, split_statements};
use crate::surreal_client::{MigrationDB, SurrealClient};

/// Extract the full schema from a running SurrealDB as DEFINE statements.
///
/// 1. INFO FOR DB → tables, functions, accesses, analyzers, params
/// 2. INFO FOR TABLE <name> for each table → fields, indexes, events
/// Returns a string of all DEFINE statements joined with ";\n".
pub fn extract_schema_from_db(client: &SurrealClient) -> Result<String> {
    let db_info = client.info_for_db()?;
    let mut statements = Vec::new();

    // Extract top-level objects from INFO FOR DB
    // The response has keys like "tables", "functions", "accesses", "analyzers", "params"
    if let Some(obj) = db_info.as_object() {
        // Collect table names for subsequent INFO FOR TABLE calls
        let mut table_names = Vec::new();

        for (section, values) in obj {
            if let Some(inner_obj) = values.as_object() {
                for (name, define_stmt) in inner_obj {
                    // Skip runtime bookkeeping tables that no schema declares:
                    // migration tracking, and the scheduler's own state.
                    if section == "tables" && crate::migrate::is_runtime_state_table(name) {
                        continue;
                    }
                    if let Some(stmt_str) = define_stmt.as_str() {
                        statements.push(ensure_semicolon(stmt_str));
                    }
                    if section == "tables" {
                        table_names.push(name.clone());
                    }
                }
            }
        }

        // For each table, get detailed info (fields, indexes, events)
        for table_name in &table_names {
            let table_info = client.info_for_table(table_name)?;
            if let Some(table_obj) = table_info.as_object() {
                for (section, values) in table_obj {
                    if let Some(inner_obj) = values.as_object() {
                        for (name, define_stmt) in inner_obj {
                            // Skip auto-generated array element sub-fields (e.g. `field.*`).
                            // SurrealDB auto-creates these when defining an array-typed field,
                            // so re-defining them in a migration causes "already exists" errors.
                            if section == "fields" && name.ends_with(".*") {
                                continue;
                            }
                            if let Some(stmt_str) = define_stmt.as_str() {
                                statements.push(ensure_semicolon(stmt_str));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(statements.join("\n"))
}

fn find_free_port() -> Result<u16> {
    let listener =
        TcpListener::bind("127.0.0.1:0").context("Failed to bind to find a free port")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

const SURREALDB_IMAGE: &str = "surrealdb/surrealdb:v3.1.0-beta.3";

/// Ensure the SurrealDB image is locally available, pulling it (with visible
/// progress) on first run. Without this, `docker run` would tacitly pull
/// during startup and we'd race the 6 s health-check budget on cold installs.
fn ensure_image_present(image: &str) -> Result<()> {
    let already_present = Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if already_present {
        return Ok(());
    }

    let step = crate::ui::step("SurrealDB image");
    step.set_message(format!("pulling {} (one-time, ~hundreds of MB)…", image));
    step.note(format!("pulling {} (one-time, ~hundreds of MB)", image));
    let output = Command::new("docker")
        .args(["pull", image])
        .output()
        .context(
            "Failed to invoke `docker pull`. Is Docker installed and running?\n\
             Install Docker from https://docs.docker.com/get-docker/.",
        )?;
    if !output.status.success() {
        step.fail("pull failed");
        bail!(
            "`docker pull {}` failed:\n{}",
            image,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    step.done("pulled");
    Ok(())
}

fn start_ephemeral_surreal_docker(port: u16) -> Result<String> {
    ensure_image_present(SURREALDB_IMAGE)?;

    let container_name = format!("sp00ky-migrate-ephemeral-{}", port);

    Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &container_name,
            "-p",
            &format!("{}:8000", port),
            "-e",
            "SURREAL_CAPS_ALLOW_EXPERIMENTAL=surrealism,files",
            // Bucket migrations with a file backend (`file:/buckets/<name>`)
            // must apply here too, or every drift check fails with "File
            // access denied". Same allowlist as the dev/cloud containers,
            // backed by a throwaway tmpfs since nothing is read back.
            "--tmpfs",
            crate::backend::BUCKET_VOLUME_PATH,
            "-e",
            &format!(
                "SURREAL_BUCKET_FOLDER_ALLOWLIST={}",
                crate::backend::BUCKET_VOLUME_PATH
            ),
            SURREALDB_IMAGE,
            "start",
            "--bind",
            "0.0.0.0:8000",
            "--user",
            "root",
            "--pass",
            "root",
            "--allow-all",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context(
            "Failed to start SurrealDB via Docker. Is Docker installed and running?\n\
             Install Docker from https://docs.docker.com/get-docker/ or provide --url to use a live database.",
        )?
        .wait_with_output()
        .context("Failed to wait for Docker container to start")?;

    Ok(container_name)
}

fn stop_ephemeral_surreal_docker(container_name: &str) {
    let _ = Command::new("docker")
        .args(["rm", "-f", container_name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

fn wait_for_surreal_health(port: u16) -> Result<()> {
    let url = format!("http://127.0.0.1:{}/health", port);
    let max_retries = 30;
    let interval = Duration::from_millis(200);

    for attempt in 1..=max_retries {
        match ureq::get(&url).timeout(Duration::from_secs(2)).call() {
            Ok(resp) if resp.status() == 200 => return Ok(()),
            _ => {
                if attempt == max_retries {
                    bail!(
                        "Ephemeral SurrealDB did not become ready after {} attempts",
                        max_retries
                    );
                }
                thread::sleep(interval);
            }
        }
    }

    unreachable!()
}

fn ensure_semicolon(stmt: &str) -> String {
    let trimmed = stmt.trim();
    if trimmed.ends_with(';') {
        trimmed.to_string()
    } else {
        format!("{};", trimmed)
    }
}

/// SurrealDB's `INFO` output scrubs JWT **signing** keys to the literal
/// `[REDACTED]`. A schema round-tripped through the DB (which we do to get a
/// canonical form for diffing) therefore loses the `{{JWT_PRIVATE_KEY}}`
/// placeholder the source used in `DEFINE ACCESS ... WITH ISSUER KEY '...'`.
/// Left as-is, `migrate create` writes `[REDACTED]` into the generated
/// migration — a value that's meaningless to apply and reads like a leaked key.
///
/// This restores the original token from `source_sql` (matched per access by
/// name) so the migration keeps the placeholder. No-op when nothing was
/// redacted or the source has no matching access. Only the signing key is ever
/// redacted by SurrealDB; the verify (`KEY`) is returned verbatim.
pub fn restore_redacted_keys(extracted: &str, source_sql: &str) -> String {
    if !extracted.contains("[REDACTED]") {
        return extracted.to_string();
    }

    // access name -> signing-key literal (placeholder or value) from the source.
    // Strip comment-only lines first: a `;` inside prose (e.g. "KEY = public;
    // ISSUER KEY = private") would otherwise fragment split_statements.
    let source_code: String = source_sql
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut signing: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for stmt in split_statements(&source_code) {
        if let Some(key) = extract_statement_key(&stmt) {
            if key.kind == "ACCESS" {
                if let Some(tok) = signing_key_literal(&stmt) {
                    signing.insert(key.identity, tok);
                }
            }
        }
    }
    if signing.is_empty() {
        return extracted.to_string();
    }

    let restored: Vec<String> = split_statements(extracted)
        .into_iter()
        .map(|stmt| {
            if !stmt.contains("[REDACTED]") {
                return stmt;
            }
            match extract_statement_key(&stmt) {
                Some(key) if key.kind == "ACCESS" => match signing.get(&key.identity) {
                    Some(tok) => stmt.replace("'[REDACTED]'", &format!("'{}'", tok)),
                    None => stmt,
                },
                _ => stmt,
            }
        })
        .collect();

    restored.join("\n")
}

/// Extract the JWT signing-key string literal from a `DEFINE ACCESS` statement:
/// the value after `ISSUER KEY` (or the older `ISSUER`). Comment lines are
/// dropped first so prose like "ISSUER KEY = the signing key" can't match.
fn signing_key_literal(access_stmt: &str) -> Option<String> {
    let code: String = access_stmt
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join(" ");
    let lower = code.to_lowercase();
    for marker in ["issuer key", "issuer"] {
        let mut from = 0;
        while let Some(rel) = lower[from..].find(marker) {
            let after = from + rel + marker.len();
            let trimmed = code[after..].trim_start();
            if let Some(q) = trimmed.strip_prefix('\'') {
                if let Some(end) = q.find('\'') {
                    return Some(q[..end].to_string());
                }
            }
            from = after;
        }
    }
    None
}

/// Normalize a schema SQL string through an ephemeral SurrealDB instance.
///
/// Applies the given SQL to a fresh database, then extracts the schema back
/// in SurrealDB's canonical format. This ensures consistent formatting for diffing.
pub fn normalize_schema_via_ephemeral_db(schema_sql: &str) -> Result<String> {
    let port = find_free_port()?;
    let container_name = start_ephemeral_surreal_docker(port)?;

    let result = (|| -> Result<String> {
        wait_for_surreal_health(port)?;

        let client = SurrealClient::new(
            &format!("http://127.0.0.1:{}", port),
            "main",
            "main",
            "root",
            "root",
        );
        client.ensure_ns_db()?;
        client.execute(schema_sql)?;

        // Restore any JWT signing key SurrealDB redacted back to the source
        // placeholder, so a generated migration keeps `{{...}}` not `[REDACTED]`.
        Ok(restore_redacted_keys(
            &extract_schema_from_db(&client)?,
            schema_sql,
        ))
    })();

    stop_ephemeral_surreal_docker(&container_name);
    result
}

/// Extract both old and new schemas through a single ephemeral SurrealDB container.
///
/// 1. Creates two databases: `old` and `new`
/// 2. Applies existing migrations to `old`, extracts schema
/// 3. Applies the built new schema SQL to `new`, extracts schema
/// 4. Returns `(old_schema, new_schema)` both in canonical format
pub fn extract_old_and_new_schemas(
    migrations_dir: &Path,
    new_schema_sql: &str,
) -> Result<(String, String)> {
    let port = find_free_port()?;
    let container_name = start_ephemeral_surreal_docker(port)?;

    let result = (|| -> Result<(String, String)> {
        wait_for_surreal_health(port)?;

        // --- Old schema (from migrations) ---
        let old_client = SurrealClient::new(
            &format!("http://127.0.0.1:{}", port),
            "main",
            "old",
            "root",
            "root",
        );
        old_client.ensure_ns_db()?;
        if migrations_dir.exists() {
            // Replay into the throwaway DB silently: this is a diff input,
            // not a real apply, so per-migration progress would only confuse.
            let _quiet = crate::ui::silenced();
            migrate::apply(&old_client, migrations_dir)?;
        }
        // Restore redacted signing keys on BOTH sides from the same source, so an
        // unchanged keyed access canonicalizes identically (no spurious drift) and
        // a real change still emits the `{{...}}` placeholder, never `[REDACTED]`.
        let old_schema =
            restore_redacted_keys(&extract_schema_from_db(&old_client)?, new_schema_sql);

        // --- New schema (from built schema) ---
        let new_client = SurrealClient::new(
            &format!("http://127.0.0.1:{}", port),
            "main",
            "new",
            "root",
            "root",
        );
        new_client.ensure_ns_db()?;
        new_client.execute(new_schema_sql)?;
        let new_schema =
            restore_redacted_keys(&extract_schema_from_db(&new_client)?, new_schema_sql);

        Ok((old_schema, new_schema))
    })();

    stop_ephemeral_surreal_docker(&container_name);
    result
}

/// Extract the expected schema by replaying all migrations through an ephemeral SurrealDB.
///
/// This is the "old schema" half of `extract_old_and_new_schemas()`, extracted as a
/// standalone function for use by `migrate fix`.
pub fn extract_expected_schema_from_migrations(migrations_dir: &Path) -> Result<String> {
    let port = find_free_port()?;
    let container_name = start_ephemeral_surreal_docker(port)?;

    let result = (|| -> Result<String> {
        wait_for_surreal_health(port)?;

        let client = SurrealClient::new(
            &format!("http://127.0.0.1:{}", port),
            "main",
            "main",
            "root",
            "root",
        );
        client.ensure_ns_db()?;

        if migrations_dir.exists() {
            let _quiet = crate::ui::silenced();
            migrate::apply(&client, migrations_dir)?;
        }

        extract_schema_from_db(&client)
    })();

    stop_ephemeral_surreal_docker(&container_name);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_semicolon_adds_when_missing() {
        assert_eq!(ensure_semicolon("DEFINE TABLE user"), "DEFINE TABLE user;");
    }

    #[test]
    fn test_ensure_semicolon_keeps_existing() {
        assert_eq!(ensure_semicolon("DEFINE TABLE user;"), "DEFINE TABLE user;");
    }

    #[test]
    fn test_ensure_semicolon_trims_whitespace() {
        assert_eq!(
            ensure_semicolon("  DEFINE TABLE user  "),
            "DEFINE TABLE user;"
        );
    }

    #[test]
    fn test_find_free_port() {
        let port = find_free_port().unwrap();
        assert!(port > 0);
    }

    #[test]
    fn test_restore_redacted_keys_restores_placeholder() {
        // What SurrealDB's INFO hands back after the source was applied: the
        // verify KEY is kept, the signing ISSUER KEY is scrubbed to [REDACTED].
        let extracted = "DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user) WITH JWT ALGORITHM EDDSA KEY '{{JWT_PUBLIC_KEY}}' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 365d;";
        let source = "DEFINE ACCESS account ON DATABASE TYPE RECORD\n  SIGNIN (SELECT * FROM user)\n  WITH JWT ALGORITHM EDDSA KEY '{{JWT_PUBLIC_KEY}}' WITH ISSUER KEY '{{JWT_PRIVATE_KEY}}'\n  DURATION FOR TOKEN 365d;";
        let out = restore_redacted_keys(extracted, source);
        assert!(
            out.contains("ISSUER KEY '{{JWT_PRIVATE_KEY}}'"),
            "got: {out}"
        );
        assert!(!out.contains("[REDACTED]"), "got: {out}");
    }

    #[test]
    fn test_restore_redacted_keys_noop_without_redaction() {
        let extracted = "DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN (...) WITH JWT ALGORITHM EDDSA KEY '{{JWT_PUBLIC_KEY}}' WITH ISSUER KEY '{{JWT_PRIVATE_KEY}}';";
        let out = restore_redacted_keys(extracted, "DEFINE ACCESS account ON DATABASE;");
        assert_eq!(out, extracted);
    }

    #[test]
    fn test_restore_redacted_keys_older_issuer_syntax() {
        let extracted = "DEFINE ACCESS account ON DATABASE TYPE RECORD ISSUER KEY '[REDACTED]';";
        // Source uses the older `ISSUER '<tok>'` form.
        let source = "DEFINE ACCESS account ON DATABASE TYPE RECORD ISSUER '{{JWT_PRIVATE_KEY}}';";
        let out = restore_redacted_keys(extracted, source);
        assert!(out.contains("'{{JWT_PRIVATE_KEY}}'"), "got: {out}");
    }

    #[test]
    fn test_restore_redacted_keys_ignores_comment_prose() {
        let extracted =
            "DEFINE ACCESS account ON DATABASE TYPE RECORD WITH ISSUER KEY '[REDACTED]';";
        // A comment mentions "ISSUER KEY" in prose before the real clause; the
        // real signing literal must still be the one that's restored.
        let source = "-- KEY = public (verify); ISSUER KEY = the private signing key\nDEFINE ACCESS account ON DATABASE TYPE RECORD WITH ISSUER KEY '{{JWT_PRIVATE_KEY}}';";
        let out = restore_redacted_keys(extracted, source);
        assert!(out.contains("'{{JWT_PRIVATE_KEY}}'"), "got: {out}");
        assert!(!out.contains("[REDACTED]"), "got: {out}");
    }

    #[test]
    fn test_restore_redacted_keys_unknown_access_left_alone() {
        // No matching access in source → can't restore; leave the value as-is
        // rather than guess (the diff/remove path handles a vanished access).
        let extracted = "DEFINE ACCESS other ON DATABASE TYPE RECORD WITH ISSUER KEY '[REDACTED]';";
        let source =
            "DEFINE ACCESS account ON DATABASE TYPE RECORD WITH ISSUER KEY '{{JWT_PRIVATE_KEY}}';";
        let out = restore_redacted_keys(extracted, source);
        assert!(out.contains("[REDACTED]"), "got: {out}");
    }
}
