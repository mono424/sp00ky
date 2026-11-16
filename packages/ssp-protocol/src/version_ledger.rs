//! The `_00_version` ledger: the durable version of every synced record. The
//! generated `_00_<table>_mutation` events bump it inside the writing
//! transaction, and every snapshot, bootstrap and drift scan reads it back as
//! the row's `_00_rv`.
//!
//! Each ledger row is keyed by the record it versions: `_00_version:[game:a]`,
//! built as `type::record('_00_version', [id])`. Every read and write of the
//! ledger is then a direct key access, which no query planner can turn into a
//! scan. Up to 1.0.0-beta.3 ledger rows had random ids and every access went
//! through the UNIQUE index `idx_record_id` on `record_id`. SurrealDB 4.0.1
//! does not plan that lookup as an index read inside an event body when the
//! record id comes from a parameter (`$after.id`), so each synced write scanned
//! the whole ledger: about 14 s per write on a 3M row ledger. See
//! `docs/version-ledger.md`.
//!
//! Rows written before the change ("legacy rows", random string ids) are moved
//! to their keyed id by [`migrate`], which the scheduler (and a standalone SSP)
//! runs at boot, and lazily by the generated events the first time their
//! record is written. The `record_id` field and its UNIQUE index stay: the
//! changefeed tail matches ledger rows to their record by that field, and the
//! index guarantees one ledger row per record while both kinds coexist.

use serde_json::Value;
use std::future::Future;

/// The ledger table.
pub const TABLE: &str = "_00_version";

/// SurrealQL for the keyed ledger row of the record `record_expr` evaluates to.
pub fn keyed_row(record_expr: &str) -> String {
    format!("type::record('{TABLE}', [{record_expr}])")
}

/// Whether a ledger row id, as SurrealDB spells it in JSON
/// (`_00_version:[game:a]`), is keyed by its record. Legacy rows have random
/// string keys (`_00_version:0a1b...`).
pub fn is_keyed_id(id: &str) -> bool {
    id.strip_prefix(TABLE)
        .and_then(|rest| rest.strip_prefix(':'))
        .is_some_and(|key| key.starts_with('['))
}

/// The first ledger row in key order. SurrealDB orders record keys number,
/// string, uuid, array, object (3.x and 4.x alike), so every legacy key sorts
/// before every keyed (array) one: the first row is a legacy one exactly when
/// legacy rows remain. One key read, whatever the ledger's size.
pub const HEAD_QUERY: &str = "SELECT VALUE id FROM _00_version LIMIT 1";

/// Whether legacy rows remain, from the answer to [`HEAD_QUERY`] (an array of
/// at most one id). An empty ledger has none.
pub fn head_is_legacy(head: &Value) -> bool {
    let first = match head {
        Value::Array(ids) => ids.first(),
        other => Some(other),
    };
    match first.and_then(Value::as_str) {
        Some(id) => !is_keyed_id(id),
        None => false,
    }
}

/// One migration step: move up to `batch` legacy rows, in key order after
/// `cursor` (a ledger id as SurrealDB spells it, or the start of the table),
/// to their keyed ids, in one transaction.
///
/// Each legacy row is deleted and its version carried over to the keyed row,
/// never lowered: `math::max` keeps a keyed row an event already bumped
/// higher. Because the read and both writes share the transaction, an event
/// that moves the same row concurrently (its fallback path deletes the legacy
/// row too) makes one of the two transactions fail with a conflict, and the
/// loser retries against the committed state. The legacy row is deleted
/// before the keyed one is written, so the UNIQUE index on `record_id` never
/// holds two rows for one record. A row without a `record_id` is dropped.
///
/// The last statement answers `{ seen, moved, last }`: rows read, legacy rows
/// moved, and the last legacy id moved (the next cursor).
///
/// The cursor matters on SurrealDB 4, where `WHERE id > <id>` seeks: without
/// it every batch would step over the keys of every row moved before it. On
/// SurrealDB 3 a cursor scan starts at the first key either way.
pub fn migration_batch_sql(batch: usize, cursor: Option<&str>) -> String {
    let filter = match cursor {
        Some(id) => format!(" WHERE id > {}", crate::record_id_literal(TABLE, id)),
        None => String::new(),
    };
    format!(
        "BEGIN TRANSACTION;\n\
         LET $spky_batch = (SELECT id, record_id, version FROM {TABLE}{filter} LIMIT {batch});\n\
         LET $spky_legacy = $spky_batch.filter(|$r| !type::is_array(record::id($r.id)));\n\
         FOR $spky_old IN $spky_legacy {{\n\
         \x20   DELETE $spky_old.id;\n\
         \x20   IF $spky_old.record_id != NONE {{\n\
         \x20       UPSERT {keyed} SET record_id = $spky_old.record_id, version = math::max([version ?? 0, $spky_old.version ?? 0]);\n\
         \x20   }};\n\
         }};\n\
         COMMIT TRANSACTION;\n\
         RETURN {{ seen: array::len($spky_batch), moved: array::len($spky_legacy), last: array::last($spky_legacy.id) }};",
        batch = batch.max(1),
        keyed = keyed_row("$spky_old.record_id"),
    )
}

/// What one [`migration_batch_sql`] step did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchOutcome {
    /// Ledger rows the step read.
    pub seen: u64,
    /// Legacy rows it moved to their keyed id.
    pub moved: u64,
    /// The last legacy id it moved: where the next step starts.
    pub last: Option<String>,
}

impl BatchOutcome {
    /// Parse the step's last statement. `None` for an answer of another shape.
    pub fn parse(answer: &Value) -> Option<Self> {
        let obj = answer.as_object()?;
        Some(Self {
            seen: obj.get("seen")?.as_u64()?,
            moved: obj.get("moved")?.as_u64()?,
            last: obj.get("last").and_then(Value::as_str).map(str::to_owned),
        })
    }

    /// Whether the step reached the end of the legacy rows: it read keyed rows
    /// (they sort after every legacy one) or ran out of rows.
    pub fn reached_the_end(&self, batch: usize) -> bool {
        self.moved < self.seen || self.seen < batch as u64
    }
}

/// Whether a failed step lost a write conflict and only needs running again.
/// SurrealDB 3 reads "Transaction conflict: Resource busy" or "Failed to
/// commit transaction due to a read or write conflict. This transaction can
/// be retried"; SurrealDB 4 says "conflict" too.
pub fn is_conflict(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    m.contains("conflict") || m.contains("can be retried") || m.contains("resource busy")
}

/// The error that explains a failed request, out of every statement's error
/// in it: when one statement of a transaction fails, SurrealDB answers every
/// other one with "The query was not executed due to a failed transaction",
/// which says nothing about why (a lost conflict, a UNIQUE violation). `None`
/// for no error at all.
pub fn cause(errors: &[String]) -> Option<String> {
    errors
        .iter()
        .find(|e| !e.contains("not executed due to a failed transaction"))
        .or_else(|| errors.first())
        .cloned()
}

/// Whether a failure says the database has no ledger table at all.
pub fn is_missing_table(message: &str) -> bool {
    message.contains("does not exist") && message.contains(TABLE)
}

/// The database [`migrate`] runs against.
pub trait LedgerDb {
    /// Run `sql` and answer the value of its last statement, or the error of
    /// the first statement that failed.
    fn query_last(&self, sql: &str) -> impl Future<Output = Result<Value, String>> + Send;
    /// Wait `ms` milliseconds.
    fn pause(&self, ms: u64) -> impl Future<Output = ()> + Send;
}

/// How [`migrate`] paces itself.
#[derive(Clone, Debug)]
pub struct MigrationOptions {
    /// Legacy rows moved per transaction.
    pub batch: usize,
    /// Pause between two steps, so the migration leaves the database room for
    /// its other traffic.
    pub pause_ms: u64,
    /// Conflicts one step may lose in a row before the run gives up.
    pub max_conflicts: u32,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        // Measured on a 3M row ledger: a 1000-row step is about 100 ms on
        // SurrealDB 4.0.1 (SurrealKV) and 3.3.5 (RocksDB) alike.
        Self {
            batch: 1000,
            pause_ms: 20,
            max_conflicts: 50,
        }
    }
}

/// Where a [`migrate`] run is.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationProgress {
    /// Legacy rows moved so far.
    pub moved: u64,
    /// Steps committed.
    pub batches: u64,
    /// Steps that lost a write conflict and ran again.
    pub conflicts: u64,
    /// No legacy row remains.
    pub done: bool,
}

/// Move every legacy ledger row to its keyed id, batch by batch.
///
/// Idempotent and resumable: each step reads the ledger as it stands, so a run
/// that stops anywhere leaves a ledger the next run (or the events) finishes,
/// and a run over a migrated ledger reads one row and stops. Safe beside live
/// writes: see [`migration_batch_sql`]. `on_batch` sees the progress after
/// every committed step. A database without the ledger table is done.
pub async fn migrate<D>(
    db: &D,
    opts: &MigrationOptions,
    mut on_batch: impl FnMut(&MigrationProgress) + Send,
) -> Result<MigrationProgress, String>
where
    D: LedgerDb + Sync + ?Sized,
{
    let mut progress = MigrationProgress::default();
    // A migrated ledger (the common case at every boot after the first): one
    // key read.
    match db.query_last(HEAD_QUERY).await {
        Ok(head) if !head_is_legacy(&head) => {
            progress.done = true;
            return Ok(progress);
        }
        Ok(_) => {}
        Err(e) if is_missing_table(&e) => {
            progress.done = true;
            return Ok(progress);
        }
        Err(e) => return Err(e),
    }
    let mut cursor: Option<String> = None;
    // A pass that reaches the end restarts from the first key (three times at
    // most) to pick up a legacy row written behind its cursor: only an older
    // release's event writes one.
    let mut restarts = 0;
    loop {
        let sql = migration_batch_sql(opts.batch, cursor.as_deref());
        let mut conflicts = 0;
        let answer = loop {
            match db.query_last(&sql).await {
                Ok(answer) => break answer,
                Err(e) if is_missing_table(&e) => {
                    progress.done = true;
                    return Ok(progress);
                }
                Err(e) if is_conflict(&e) && conflicts < opts.max_conflicts => {
                    conflicts += 1;
                    progress.conflicts += 1;
                    db.pause(10 * u64::from(conflicts.min(20))).await;
                }
                Err(e) => return Err(e),
            }
        };
        let outcome = BatchOutcome::parse(&answer)
            .ok_or_else(|| format!("unexpected answer to a ledger migration step: {answer}"))?;
        progress.batches += 1;
        progress.moved += outcome.moved;
        if outcome.reached_the_end(opts.batch) {
            let head = db.query_last(HEAD_QUERY).await;
            let legacy_left = match head {
                Ok(head) => head_is_legacy(&head),
                Err(e) if is_missing_table(&e) => false,
                Err(e) => return Err(e),
            };
            if !legacy_left || restarts >= 3 {
                progress.done = !legacy_left;
                on_batch(&progress);
                return Ok(progress);
            }
            restarts += 1;
            cursor = None;
        } else {
            cursor = outcome.last.or(cursor);
        }
        on_batch(&progress);
        if opts.pause_ms > 0 {
            db.pause(opts.pause_ms).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keyed_ids_are_told_from_legacy_ones() {
        assert!(is_keyed_id("_00_version:[game:a]"));
        assert!(is_keyed_id("_00_version:[game:⟨a-b⟩]"));
        assert!(!is_keyed_id("_00_version:0000lx90trdmzayamqhb"));
        assert!(!is_keyed_id("_00_version:42"));
        assert!(!is_keyed_id("game:[a]"));
        assert_eq!(
            keyed_row("$after.id"),
            "type::record('_00_version', [$after.id])"
        );
    }

    #[test]
    fn the_head_says_whether_legacy_rows_remain() {
        assert!(head_is_legacy(&json!(["_00_version:abc"])));
        assert!(!head_is_legacy(&json!(["_00_version:[game:a]"])));
        assert!(!head_is_legacy(&json!([])));
        assert!(!head_is_legacy(&Value::Null));
    }

    #[test]
    fn a_step_reads_after_its_cursor() {
        let first = migration_batch_sql(500, None);
        assert!(first.contains("FROM _00_version LIMIT 500);"), "{first}");
        let next = migration_batch_sql(500, Some("_00_version:abc"));
        assert!(
            next.contains("FROM _00_version WHERE id > _00_version:abc LIMIT 500);"),
            "{next}"
        );
        assert!(next.starts_with("BEGIN TRANSACTION;") && next.contains("COMMIT TRANSACTION;"));
        // The answer is read after the commit: a RETURN inside the
        // transaction would end it early.
        let commit = next.find("COMMIT TRANSACTION;").unwrap();
        assert!(!next[..commit].contains("RETURN"), "{next}");
    }

    #[test]
    fn a_step_answer_parses_and_says_when_the_legacy_rows_ended() {
        let full = BatchOutcome::parse(&json!({ "seen": 3, "moved": 3, "last": "_00_version:c" }))
            .unwrap();
        assert_eq!(full.last.as_deref(), Some("_00_version:c"));
        assert!(!full.reached_the_end(3));
        let into_keyed =
            BatchOutcome::parse(&json!({ "seen": 3, "moved": 1, "last": "_00_version:c" }))
                .unwrap();
        assert!(into_keyed.reached_the_end(3));
        let short = BatchOutcome::parse(&json!({ "seen": 0, "moved": 0, "last": null })).unwrap();
        assert!(short.reached_the_end(3));
        assert_eq!(short.last, None);
        assert_eq!(BatchOutcome::parse(&json!([1])), None);
    }

    #[test]
    fn conflicts_are_retried_and_other_errors_are_not() {
        assert!(is_conflict("Transaction conflict: Resource busy"));
        assert!(is_conflict("Failed to commit transaction due to a read or write conflict. This transaction can be retried"));
        assert!(!is_conflict("Parse error"));
        assert!(is_missing_table("The table '_00_version' does not exist"));
        let generic = "The query was not executed due to a failed transaction".to_string();
        let real = "Transaction conflict: Resource busy".to_string();
        assert_eq!(
            cause(&[generic.clone(), real.clone(), generic.clone()]),
            Some(real)
        );
        assert_eq!(cause(&[generic.clone()]), Some(generic));
        assert_eq!(cause(&[]), None);
        assert!(!is_missing_table("The table 'game' does not exist"));
    }
}
