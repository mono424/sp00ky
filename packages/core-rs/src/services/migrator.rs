//! Provisions the local store against a schema and migrates on change (TS
//! `LocalMigrator`).
//!
//! SQLite stores documents, so application schema changes need no destructive
//! table migration. Keep cached rows, memberships and queued writes. Rebuild
//! the circuit using the new schema and reconcile rows through normal sync. A
//! kept row lacks any column the old schema did not know; the boot prime
//! resets the versions of such tables so they are fetched again
//! ([`crate::services::column_reset`]).

use sha1::{Digest, Sha1};

use crate::error::Result;
use crate::services::local::LocalDatabaseService;

/// SHA-1 hex of the schema text (TS `sha1`). Used to detect schema changes.
pub fn schema_sha1(schema_surql: &str) -> String {
    hex::encode(Sha1::digest(schema_surql.as_bytes()))
}

/// Provision `local` for `schema_surql`: on a schema change the circuit
/// snapshot is dropped (the circuit is rebuilt from rows under the new schema)
/// and the new hash recorded. Rows, registrations and the outbox stay.
pub fn provision(local: &LocalDatabaseService, schema_surql: &str) -> Result<()> {
    let hash = schema_sha1(schema_surql);
    // The full circuit used to be persisted here as JSON after every ingest.
    // Only the store snapshot is read now; drop the dead row (megabytes on a
    // real store). A no-op once it is gone.
    local.drop_legacy_stream_state()?;

    if local.latest_schema_hash()?.as_deref() == Some(hash.as_str()) {
        tracing::info!("[Provisioning] Schema up to date, skipping migration");
        return Ok(());
    }

    tracing::info!("[Provisioning] Schema changed, rebuilding circuit");
    // Always six fractional digits. Dart writes three when the microseconds
    // are zero, and `.345Z` sorts after `.345678Z`, which can order two hashes
    // recorded in the same millisecond the wrong way round.
    let created_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    local.tx(|| {
        local.clear_snapshot()?;
        local.record_schema_hash(&hash, &created_at)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::row;
    use serde_json::json;

    const SCHEMA_A: &str = "DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;";
    const SCHEMA_B: &str = "DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;\n\
                            DEFINE TABLE note SCHEMAFULL PERMISSIONS FOR select WHERE true;";

    fn db() -> LocalDatabaseService {
        let db = LocalDatabaseService::open_memory().unwrap();
        db.provision().unwrap();
        db
    }

    #[test]
    fn first_provision_records_the_schema_hash() {
        let db = db();
        assert!(db.latest_schema_hash().unwrap().is_none());
        provision(&db, SCHEMA_A).unwrap();
        assert!(db.latest_schema_hash().unwrap().is_some());
    }

    #[test]
    fn re_provisioning_the_same_schema_is_a_no_op() {
        let db = db();
        provision(&db, SCHEMA_A).unwrap();
        db.create("thread:a", row(json!({"title": "x", "_00_rv": 1})))
            .unwrap();
        db.put_query_config(
            "_00_query:h",
            &row(json!({"surql": "SELECT * FROM thread"})),
        )
        .unwrap();
        db.put_snapshot(&[1, 2]).unwrap();

        provision(&db, SCHEMA_A).unwrap();

        assert!(db.get_by_id("thread:a").unwrap().is_some());
        assert!(db.get_query_config("_00_query:h").unwrap().is_some());
        assert!(
            db.get_snapshot().unwrap().is_some(),
            "same schema keeps the snapshot"
        );
    }

    #[test]
    fn schema_upgrades_and_rollbacks_preserve_cached_rows_and_pending_writes() {
        let db = db();
        provision(&db, SCHEMA_A).unwrap();
        db.create("thread:a", row(json!({"title": "x", "_00_rv": 1})))
            .unwrap();
        db.put_query_config(
            "_00_query:h",
            &row(json!({"surql": "SELECT * FROM thread"})),
        )
        .unwrap();
        db.put_mutation(
            "_00_pending_mutations:1",
            &row(json!({"mutationType": "create"})),
        )
        .unwrap();
        db.kv_set("_00_stream_processor_state", "STALE-STATE")
            .unwrap();
        db.kv_set("sp00ky_auth_token", "tok").unwrap();
        db.put_snapshot(&[1, 2]).unwrap();

        provision(&db, SCHEMA_B).unwrap();

        assert!(db.get_by_id("thread:a").unwrap().is_some());
        assert!(db.get_query_config("_00_query:h").unwrap().is_some());
        assert_eq!(db.get_all_mutations().unwrap().len(), 1);
        assert!(db.kv_get("_00_stream_processor_state").unwrap().is_none());
        // The auth token must survive a data-schema migration.
        assert_eq!(
            db.kv_get("sp00ky_auth_token").unwrap().as_deref(),
            Some("tok")
        );
        assert!(
            db.get_snapshot().unwrap().is_none(),
            "a new schema rebuilds the circuit"
        );
        provision(&db, SCHEMA_A).unwrap();
        assert!(db.get_by_id("thread:a").unwrap().is_some());
        assert_eq!(db.get_all_mutations().unwrap().len(), 1);
        assert_eq!(
            db.latest_schema_hash().unwrap(),
            Some(schema_sha1(SCHEMA_A))
        );
    }

    #[test]
    fn schema_hash_is_a_stable_sha1_hex_of_the_schema_text() {
        let db = db();
        provision(&db, SCHEMA_A).unwrap();
        let h1 = db.latest_schema_hash().unwrap().unwrap();
        assert_eq!(h1.len(), 40);
        assert_eq!(h1, schema_sha1(SCHEMA_A));
        assert_eq!(
            schema_sha1("abc"),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }
}
