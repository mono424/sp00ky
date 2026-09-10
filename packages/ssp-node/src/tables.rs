//! Per-user dedicated table helpers for `_00_query` / `_00_list_ref`.
//!
//! The SurrealDB v3 LIVE-permission path silently filters notifications when
//! the inserter and the LIVE-subscriber are different sessions — even when the
//! subscriber's permission expression would pass for the new row. As a
//! workaround the SSP can be configured (via `SPKY_SSP_REF_MODE=dedicated`) to
//! use a per-user pair of tables `_00_query_user_<id>` and
//! `_00_list_ref_user_<id>` whose permission rule is hardcoded against a
//! literal user record id.
//!
//! Ported onto the [`Db`] port (was `apps/ssp/src/tables.rs`): DDL runs through
//! `db.query`, and errors already surface as `DbError` (the SDK's `.check()`
//! semantics fold into `SurrealSdkDb::query`). `INFO FOR DB` parsing uses the
//! port's flattened-JSON convention (`{"tables": {name: "DEFINE ..."}}`).

use anyhow::{Context, Result};
use serde_json::json;
use std::collections::HashSet;
use tracing::{debug, warn};

use ssp_protocol::{list_ref_table_for, sanitize_user_id, RefMode, ANON_AUTH_ID};

use crate::db_retry::query_retrying;
use crate::ports::Db;

/// Per-user tables this process has already defined, keyed by sanitized uid.
///
/// `ensure_user_tables` used to issue `DEFINE TABLE OVERWRITE` on EVERY
/// registration. A client registering its 70 views after a bootstrap fired 70
/// schema statements against the same table inside a few seconds, and
/// SurrealDB answered some of them with a transaction conflict, which the
/// register handler turned into a 500 for that registration. The DDL is
/// idempotent and the definition only changes with a release, so once per
/// process per user is enough. The drop paths evict the entry so a re-created
/// table is defined again.
static ENSURED_USER_TABLES: std::sync::OnceLock<std::sync::Mutex<HashSet<String>>> =
    std::sync::OnceLock::new();
static ENSURED_ANON_TABLE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn ensured_user_tables() -> &'static std::sync::Mutex<HashSet<String>> {
    ENSURED_USER_TABLES.get_or_init(|| std::sync::Mutex::new(HashSet::new()))
}

fn user_table_ensured(uid: &str) -> bool {
    ensured_user_tables()
        .lock()
        .map(|set| set.contains(uid))
        .unwrap_or(false)
}

fn mark_user_table_ensured(uid: &str) {
    if let Ok(mut set) = ensured_user_tables().lock() {
        set.insert(uid.to_string());
    }
}

fn forget_user_table(uid: &str) {
    if let Ok(mut set) = ensured_user_tables().lock() {
        set.remove(uid);
    }
}

/// Returns the `_00_list_ref` table name for the given mode + user.
pub fn list_ref_table(mode: RefMode, auth_id: &str) -> String {
    list_ref_table_for(mode, auth_id)
}

/// Whether a query failed only because the table it names was never defined.
///
/// The per-user `_00_list_ref_user_<id>` table is created the first time that
/// user registers a view. Deleting a record owned by someone who never did
/// therefore targets a table that does not exist — vacuously nothing to
/// clean, but SurrealDB answers with an error and the orphan-proof delete
/// logged it as one, several times a minute, forever.
pub fn is_missing_table_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("does not exist") && m.contains("table")
}

/// Field DDL shared by the anon and per-user `_00_list_ref_*` tables.
fn list_ref_fields(tbl: &str) -> String {
    format!(
        r#"
DEFINE FIELD OVERWRITE in ON TABLE {tbl} TYPE record;
DEFINE FIELD OVERWRITE out ON TABLE {tbl} TYPE record;
DEFINE FIELD OVERWRITE clientId ON TABLE {tbl} TYPE string;
DEFINE FIELD OVERWRITE auth_id ON TABLE {tbl} TYPE string DEFAULT '';
DEFINE FIELD OVERWRITE version ON TABLE {tbl} TYPE int;
DEFINE FIELD OVERWRITE updatedAt ON TABLE {tbl} TYPE datetime VALUE time::now() READONLY;
DEFINE FIELD OVERWRITE parent ON TABLE {tbl} TYPE option<record<{tbl}>>;
DEFINE FIELD OVERWRITE parent_rel ON TABLE {tbl} TYPE option<string>;
"#,
        tbl = tbl,
    )
}

/// Define the shared `_00_list_ref_anon` table used for unauthenticated
/// clients when `anonymousLiveQueries` is enabled. Unlike the per-user tables
/// it is `SELECT WHERE true` (world-readable): a logged-out session has
/// `$auth = NONE`, so it can't be gated on `$auth.id`. Safe because the edges
/// only reference records the client could already read one-shot (the public
/// tables are `select WHERE true`). Writes stay SSP-only (root bypasses the
/// rule). Independent of `RefMode` and never auto-dropped.
pub async fn ensure_anon_table(db: &dyn Db) -> Result<()> {
    use std::sync::atomic::Ordering;
    if ENSURED_ANON_TABLE.load(Ordering::Acquire) {
        return Ok(());
    }
    let tbl = "_00_list_ref_anon";
    let ddl = format!(
        "DEFINE TABLE OVERWRITE {tbl} SCHEMALESS \
         PERMISSIONS FOR select WHERE true, \
                     FOR create, update WHERE false, \
                     FOR delete WHERE false;{fields}",
        tbl = tbl,
        fields = list_ref_fields(tbl),
    );
    query_retrying(db, &ddl, &[])
        .await
        .context("Failed to ensure anonymous list_ref table")?;
    ENSURED_ANON_TABLE.store(true, Ordering::Release);
    Ok(())
}

/// Issue idempotent `DEFINE TABLE OVERWRITE` + field DDL for the per-user
/// `_00_list_ref_user_<id>` table. No-op in `RefMode::Single`, and when
/// `auth_id` doesn't sanitize cleanly (falls back to the global table, logged
/// at the call site). Idempotent via `OVERWRITE`.
pub async fn ensure_user_tables(db: &dyn Db, mode: RefMode, auth_id: &str) -> Result<()> {
    // Anonymous clients share the dedicated `_00_list_ref_anon` table in both
    // ref modes (the handler only ever passes the sentinel when the flag is on).
    if auth_id == ANON_AUTH_ID {
        return ensure_anon_table(db).await;
    }
    if mode == RefMode::Single {
        return Ok(());
    }
    let Some(uid) = sanitize_user_id(auth_id) else {
        if auth_id.is_empty() {
            // Routine, not a fault: `fn::query::register` sends
            // `<string>($auth.id OR '')`, so any registration issued before
            // the session's auth is established carries ''. It used to warn
            // once per registration, which on a busy client meant dozens of
            // WARN lines a minute for something the register handler now
            // repairs itself (`Node::adopt_view_identity`).
            debug!(
                target: "ssp::tables",
                "registration asserted no identity; per-user tables skipped until one does"
            );
        } else {
            warn!(
                target: "ssp::tables",
                auth_id = %auth_id,
                "auth_id is not a valid table-name segment; dedicated tables skipped, falling back to global"
            );
        }
        return Ok(());
    };

    if user_table_ensured(&uid) {
        return Ok(());
    }

    let tbl = format!("_00_list_ref_user_{}", uid);
    let ddl = format!(
        "DEFINE TABLE OVERWRITE {tbl} SCHEMALESS \
         PERMISSIONS FOR select WHERE $auth.id = user:{uid}, \
                     FOR create, update WHERE false, \
                     FOR delete WHERE false;{fields}",
        tbl = tbl,
        uid = uid,
        fields = list_ref_fields(&tbl),
    );
    // Retried: this DDL is issued from `/view/register`, and a registration
    // burst has every client defining its own table against the same catalog
    // at once. `db_retry`'s own doc names `_00_list_ref_*` as a hot row for
    // exactly this reason — it was just never wired up here, so a lost
    // optimistic-concurrency race 500'd the registration outright and the
    // client's view never came up. Seen on whitepawn: twelve of these inside
    // one second during a re-registration storm.
    query_retrying(db, &ddl, &[])
        .await
        .with_context(|| format!("Failed to ensure per-user list_ref table for {}", auth_id))?;
    mark_user_table_ensured(&uid);
    Ok(())
}

/// Inverse of `ensure_user_tables`: remove the per-user table when the owning
/// `user` record is deleted. No-op in `RefMode::Single` / when `auth_id`
/// doesn't sanitize. Idempotent (`REMOVE TABLE IF EXISTS`).
pub async fn drop_user_tables(db: &dyn Db, mode: RefMode, auth_id: &str) -> Result<()> {
    if mode == RefMode::Single {
        return Ok(());
    }
    let Some(uid) = sanitize_user_id(auth_id) else {
        return Ok(());
    };
    // Forget first: if the REMOVE fails half-way the next registration
    // re-issues the DEFINE, which is harmless; the reverse order could skip
    // a DEFINE the table now needs.
    forget_user_table(&uid);
    let ddl = format!("REMOVE TABLE IF EXISTS _00_list_ref_user_{};", uid);
    db.query(&ddl, &[])
        .await
        .with_context(|| format!("Failed to drop per-user list_ref table for {}", auth_id))?;
    Ok(())
}

/// Drop a user's per-user table iff they have no remaining registered query in
/// `_00_query`. Called after a TTL sweep. No-op in `RefMode::Single`.
pub async fn drop_user_table_if_unused(db: &dyn Db, mode: RefMode, auth_id: &str) -> Result<()> {
    if mode == RefMode::Single {
        return Ok(());
    }
    let results = db
        .query(
            "SELECT VALUE count() FROM _00_query WHERE auth_id = $a GROUP ALL",
            &[("a", json!(auth_id))],
        )
        .await
        .with_context(|| format!("count remaining queries for {}", auth_id))?;
    let count = results
        .first()
        .and_then(first_i64)
        .unwrap_or(0);
    if count == 0 {
        drop_user_tables(db, mode, auth_id).await?;
    }
    Ok(())
}

/// Best-effort: drop every `_00_list_ref_user_<id>` table whose owner has no
/// live query in `_00_query`. No-op in `RefMode::Single`.
pub async fn drop_orphaned_user_tables(db: &dyn Db, mode: RefMode) -> Result<()> {
    if mode == RefMode::Single {
        return Ok(());
    }

    // Flattened JSON: INFO FOR DB → { "tables": { name: "DEFINE ..." } }.
    let info = db.query("INFO FOR DB", &[]).await.context("INFO FOR DB")?;
    let per_user: Vec<String> = info
        .first()
        .and_then(|v| v.get("tables"))
        .and_then(|t| t.as_object())
        .map(|m| {
            m.keys()
                .filter(|n| n.starts_with("_00_list_ref_user_"))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    if per_user.is_empty() {
        return Ok(());
    }

    // Sanitized uids that still back a live query.
    let auth_rows = db
        .query("SELECT VALUE auth_id FROM _00_query", &[])
        .await
        .context("select live auth_ids")?;
    let live_uids: HashSet<String> = auth_rows
        .first()
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .filter_map(sanitize_user_id)
                .collect()
        })
        .unwrap_or_default();

    for tbl in per_user {
        let uid = tbl.trim_start_matches("_00_list_ref_user_");
        if !live_uids.contains(uid) {
            forget_user_table(uid);
            if let Err(e) = db.query(&format!("REMOVE TABLE IF EXISTS {tbl};"), &[]).await {
                warn!(target: "ssp::tables", table = %tbl, error = %e, "drop orphaned per-user table failed");
            }
        }
    }
    Ok(())
}

/// Extract a leading integer from a flattened query result (`GROUP ALL count()`
/// returns `[count]` or a bare number depending on shape).
fn first_i64(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Array(a) => a.first().and_then(|x| x.as_i64()),
        other => other.as_i64(),
    }
}

#[cfg(test)]
mod missing_table_tests {
    use super::is_missing_table_error;

    #[test]
    fn recognises_surrealdb_s_missing_table_wording() {
        // Verbatim from whitepawn's SSP log.
        assert!(is_missing_table_error(
            "query: The table '_00_list_ref_user_b2go6j6xwozr621ve5dm' does not exist"
        ));
    }

    #[test]
    fn leaves_every_other_failure_an_error() {
        assert!(!is_missing_table_error("Transaction conflict: Resource busy"));
        assert!(!is_missing_table_error("The field 'x' does not exist"));
    }
}
#[cfg(test)]
mod ensure_once_tests {
    use super::*;
    use crate::ports::DbError;
    use std::sync::Mutex;

    /// Records every statement it is asked to run and answers nothing.
    #[derive(Default)]
    struct RecordingDb {
        statements: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Db for RecordingDb {
        async fn query(
            &self,
            surql: &str,
            _binds: &[(&str, serde_json::Value)],
        ) -> Result<Vec<serde_json::Value>, DbError> {
            self.statements.lock().unwrap().push(surql.to_string());
            Ok(vec![])
        }
        async fn version(&self) -> Result<String, DbError> {
            Ok("test".into())
        }
    }

    fn ddl_count(db: &RecordingDb, needle: &str) -> usize {
        db.statements
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.contains(needle))
            .count()
    }

    #[tokio::test]
    async fn per_user_ddl_runs_once_per_process_until_the_table_is_dropped() {
        // Unique per test: the cache is process-wide and tests run in parallel.
        let auth_id = "user:ensureonce_a1b2c3";
        let db = RecordingDb::default();

        ensure_user_tables(&db, RefMode::Dedicated, auth_id).await.unwrap();
        ensure_user_tables(&db, RefMode::Dedicated, auth_id).await.unwrap();
        ensure_user_tables(&db, RefMode::Dedicated, auth_id).await.unwrap();
        assert_eq!(
            ddl_count(&db, "DEFINE TABLE OVERWRITE _00_list_ref_user_ensureonce_a1b2c3"),
            1,
            "a burst of registrations issues the DDL once"
        );

        // Dropping the table (user deleted, or TTL sweep found it unused)
        // must make the next registration define it again.
        drop_user_tables(&db, RefMode::Dedicated, auth_id).await.unwrap();
        ensure_user_tables(&db, RefMode::Dedicated, auth_id).await.unwrap();
        assert_eq!(ddl_count(&db, "REMOVE TABLE IF EXISTS _00_list_ref_user_ensureonce_a1b2c3"), 1);
        assert_eq!(
            ddl_count(&db, "DEFINE TABLE OVERWRITE _00_list_ref_user_ensureonce_a1b2c3"),
            2,
            "re-defined after a drop"
        );
    }

    /// A registration burst has every client defining its own per-user table
    /// against the same catalog at once, so this DDL loses
    /// optimistic-concurrency races. Without a retry the loser 500s
    /// `/view/register` and that client's view never comes up.
    #[tokio::test]
    async fn a_write_conflict_on_the_define_is_retried() {
        struct ConflictThenOk {
            attempts: std::sync::atomic::AtomicUsize,
        }
        #[async_trait::async_trait]
        impl Db for ConflictThenOk {
            async fn query(
                &self,
                _surql: &str,
                _binds: &[(&str, serde_json::Value)],
            ) -> Result<Vec<serde_json::Value>, DbError> {
                let n = self.attempts.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                if n == 0 {
                    return Err(DbError::Query(
                        "Transaction conflict: Resource busy: . This transaction can be retried"
                            .into(),
                    ));
                }
                Ok(vec![])
            }
            async fn version(&self) -> Result<String, DbError> {
                Ok("test".into())
            }
        }

        let db = ConflictThenOk {
            attempts: std::sync::atomic::AtomicUsize::new(0),
        };
        ensure_user_tables(&db, RefMode::Dedicated, "user:retried_after_conflict_q1")
            .await
            .expect("a write conflict must be retried, not surfaced as a 500");
        assert_eq!(
            db.attempts.load(std::sync::atomic::Ordering::Acquire),
            2,
            "the DDL should have run twice: the conflict, then the retry"
        );
    }

    #[tokio::test]
    async fn a_failed_define_is_not_remembered() {
        struct FailingDb;
        #[async_trait::async_trait]
        impl Db for FailingDb {
            async fn query(
                &self,
                _surql: &str,
                _binds: &[(&str, serde_json::Value)],
            ) -> Result<Vec<serde_json::Value>, DbError> {
                Err(DbError::Transport("resource busy".into()))
            }
            async fn version(&self) -> Result<String, DbError> {
                Ok("test".into())
            }
        }
        let auth_id = "user:ensureonce_failed_x9";
        assert!(ensure_user_tables(&FailingDb, RefMode::Dedicated, auth_id).await.is_err());

        // The conflict was transient; the next attempt must actually run the DDL.
        let db = RecordingDb::default();
        ensure_user_tables(&db, RefMode::Dedicated, auth_id).await.unwrap();
        assert_eq!(ddl_count(&db, "DEFINE TABLE OVERWRITE _00_list_ref_user_ensureonce_failed_x9"), 1);
    }
}
