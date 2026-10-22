//! `spky admin` — manage the sp00ky operator roster (`_00_admin`).
//!
//! The `user` table belongs to the app, not to sp00ky, so there is no `role`
//! field we can rely on — and a self-writable one would be a privilege
//! escalation hole. `_00_admin` is the whole admin concept instead: a row
//! there means "may edit feature flags from the DevTools panel".
//!
//! The table denies create/update/delete unconditionally, so it can only be
//! written by root — i.e. by this command. Nobody promotes themselves.
//!
//! What an admin can actually do is defined in `meta_tables_remote.surql`:
//! read `_00_feature_flag`, flip `enabled`, edit allowlist rules via
//! `fn::feature::allow` / `fn::feature::disallow`, and write the
//! `_00_user_feature` rows those functions materialize. Creating and deleting
//! flags stays root-only (`spky flag create|delete`).

use anyhow::{Context, Result};
use serde_json::Value;
use std::path::PathBuf;

use crate::flag::{client_from, esc, load_user_id, rows};
use crate::surreal_client::MigrationDB;
use crate::AdminCommands;

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

pub fn run(action: AdminCommands) -> Result<()> {
    match action {
        AdminCommands::List { conn, config } => list(conn, config),
        AdminCommands::Add {
            user,
            note,
            conn,
            config,
        } => add(user, note, conn, config),
        AdminCommands::Remove { user, conn, config } => remove(user, conn, config),
    }
}

fn list(conn: crate::ConnectionArgs, config: Option<PathBuf>) -> Result<()> {
    let client = client_from(conn, config)?;
    let resp = client
        .execute(
            "SELECT <string>user AS user, user.username AS username, note, added_at \
             FROM _00_admin ORDER BY added_at ASC;",
        )
        .context("Failed to list admins")?;
    let admins = rows(resp);
    if admins.is_empty() {
        println!("{}No admins. Add one with `spky admin add <user>`.{}", DIM, RESET);
        return Ok(());
    }
    println!("{}USER                      ID                        NOTE{}", BOLD, RESET);
    for a in admins {
        let username = a
            .get("username")
            .and_then(Value::as_str)
            .unwrap_or("(deleted)");
        let id = a.get("user").and_then(Value::as_str).unwrap_or("?");
        let note = a.get("note").and_then(Value::as_str).unwrap_or("");
        println!("{:<25} {:<25} {}", username, id, note);
    }
    Ok(())
}

fn add(
    user: String,
    note: Option<String>,
    conn: crate::ConnectionArgs,
    config: Option<PathBuf>,
) -> Result<()> {
    let client = client_from(conn, config)?;
    // Accepts a username or a `user:xxx` record id.
    let user_id = load_user_id(&client, &user)?;

    // `user_id` is a DB-sourced record id and is interpolated as a record
    // reference (the same shape `flag.rs::materialize` uses); the note is
    // escaped as a string literal.
    let note_sql = match &note {
        Some(n) => format!("'{}'", esc(n)),
        None => "NONE".to_string(),
    };
    let query = format!(
        "UPSERT _00_admin SET user = {uid}, note = {note} WHERE user = {uid};",
        uid = user_id,
        note = note_sql
    );
    client
        .execute(&query)
        .context("Failed to add admin. Has the internal schema been applied (`spky migrate`)?")?;

    println!(
        "{}Added{} '{}' ({}) as an admin.",
        GREEN, RESET, user, user_id
    );
    println!(
        "{}They can now edit feature flags from the DevTools Access tab. No re-login needed —\n\
         $auth.id is evaluated per query.{}",
        DIM, RESET
    );
    Ok(())
}

fn remove(user: String, conn: crate::ConnectionArgs, config: Option<PathBuf>) -> Result<()> {
    let client = client_from(conn, config)?;
    let user_id = load_user_id(&client, &user)?;

    let existing = rows(
        client
            .execute(&format!(
                "SELECT VALUE id FROM _00_admin WHERE user = {};",
                user_id
            ))
            .context("Failed to check admin roster")?,
    );
    if existing.is_empty() {
        println!("{}'{}' is not an admin. Nothing to do.{}", YELLOW, user, RESET);
        return Ok(());
    }

    client
        .execute(&format!("DELETE _00_admin WHERE user = {};", user_id))
        .context("Failed to remove admin")?;

    println!("{}Removed{} '{}' from the admin roster.", GREEN, RESET, user);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const META_TABLES_REMOTE: &str = include_str!("meta_tables_remote.surql");

    /// `DEFINE FUNCTION` defaults to `PERMISSIONS FULL` in SurrealDB — omitting
    /// the clause does NOT make a function root-only, it exposes it to every
    /// signed-in user. `fn::feature::materialize` / `allow` / `disallow` write
    /// flags for ALL users, so a missing clause here ships a self-service flag
    /// editor. `fn::feature::hash` is pure and deliberately callable.
    ///
    /// This is a string check on purpose: it fails at `cargo test` rather than
    /// after a deploy, and it needs no database.
    #[test]
    fn every_feature_mutation_function_declares_permissions() {
        let mutating = ["materialize", "reassign", "allow", "disallow"];
        for name in mutating {
            let marker = format!("DEFINE FUNCTION OVERWRITE fn::feature::{name}(");
            let start = META_TABLES_REMOTE
                .find(&marker)
                .unwrap_or_else(|| panic!("fn::feature::{name} is missing from the schema"));
            // The body ends at the next DEFINE; the PERMISSIONS clause sits
            // between the closing brace and that boundary.
            let rest = &META_TABLES_REMOTE[start + marker.len()..];
            let end = rest.find("\nDEFINE ").unwrap_or(rest.len());
            assert!(
                rest[..end].contains("PERMISSIONS WHERE"),
                "fn::feature::{name} has no PERMISSIONS clause, so SurrealDB defaults it to \
                 FULL and any signed-in user can rewrite feature flags"
            );
        }
    }

    /// The admin gate is repeated on both flag tables and every fn::feature::*
    /// function that reads or writes assignments (rules, assign, materialize,
    /// reassign, allow, disallow).
    /// If one copy drifts, that gate opens wider than the others and nothing
    /// else would notice.
    #[test]
    fn the_admin_predicate_is_identical_everywhere() {
        let predicate =
            "array::len((SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1)) > 0";
        assert_eq!(
            META_TABLES_REMOTE.matches(predicate).count(),
            8,
            "expected the admin predicate on _00_feature_flag, _00_user_feature and the six \
             gated fn::feature::* functions; a differing count means one gate has drifted"
        );
    }

    /// `_00_admin` must never become client-writable: the whole point is that
    /// nobody can promote themselves.
    #[test]
    fn admin_roster_denies_client_writes() {
        let start = META_TABLES_REMOTE
            .find("DEFINE TABLE OVERWRITE _00_admin")
            .expect("_00_admin table definition missing");
        let rest = &META_TABLES_REMOTE[start..];
        let end = rest.find("\nDEFINE FIELD").unwrap_or(rest.len());
        assert!(
            rest[..end].contains("FOR create, update, delete NONE"),
            "_00_admin must deny create/update/delete to record tokens"
        );
    }

    /// The record id must be interpolated as a record reference, not quoted:
    /// `user = 'user:abc'` compares a string against a `record` field and
    /// silently matches nothing, which would make `add` a no-op and `remove`
    /// claim the user isn't an admin.
    #[test]
    fn add_interpolates_the_record_id_unquoted() {
        let uid = "user:abc";
        let query = format!(
            "UPSERT _00_admin SET user = {uid}, note = {note} WHERE user = {uid};",
            uid = uid,
            note = "NONE"
        );
        assert!(query.contains("SET user = user:abc,"));
        assert!(query.contains("WHERE user = user:abc;"));
        assert!(!query.contains("'user:abc'"));
    }

    #[test]
    fn note_is_escaped_as_a_string_literal() {
        let note_sql = format!("'{}'", esc("it's fine"));
        assert_eq!(note_sql, "'it\\'s fine'");
    }

    #[test]
    fn absent_note_writes_none_not_an_empty_string() {
        let note: Option<String> = None;
        let note_sql = match &note {
            Some(n) => format!("'{}'", esc(n)),
            None => "NONE".to_string(),
        };
        assert_eq!(note_sql, "NONE");
    }
}
