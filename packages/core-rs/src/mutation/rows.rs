//! The outbox and tray rows, and every pure plan the write side runs: the local
//! write transactions, the remote push batch, the rollback.
//!
//! PORT: `packages/spooky_core/lib/src/mutation/rows.dart`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use regex::Regex;

use crate::kernel::ports::{IngestOp, IngestRecord, LocalOp};
use crate::mutation::mutation_id::PENDING_TABLE;
use crate::state::{OutboxItem, OutboxStatus};
use crate::types::{ColumnSchema, MutationEventType};
use crate::utils::parse_query_params;
use crate::value::{table_of, Object, RecordId, Row, Value};

/// The local tray of rejected mutations.
pub const FAILED_TABLE: &str = "_00_failed_mutations";
/// The `v` this client writes on outbox rows.
pub const PENDING_ROW_VERSION: i64 = 2;

/// How much of a rejected mutation could be undone locally.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevertKind {
    Full,
    Partial,
    None,
}

impl RevertKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RevertKind::Full => "full",
            RevertKind::Partial => "partial",
            RevertKind::None => "none",
        }
    }
}

/// Why a mutation landed in the tray.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailedErrorKind {
    /// The server rejected it.
    Application,
    /// The stored row can never be replayed (a create without its payload).
    Unreplayable,
}

impl FailedErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FailedErrorKind::Application => "application",
            FailedErrorKind::Unreplayable => "unreplayable",
        }
    }
}

/// A number field as an integer (Dart `(x as num).toInt()`).
pub(crate) fn num_i64(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Int(i) => Some(*i),
        Value::Float(f) if f.is_finite() => Some(*f as i64),
        _ => None,
    }
}

/// A log payload from key/value pairs.
pub(crate) fn log_data(pairs: Vec<(&str, Value)>) -> Option<Value> {
    Some(Value::Object(
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    ))
}

/// One `_00_pending_mutations` row as this client writes it (v2).
#[derive(Clone, Debug, PartialEq)]
pub struct PendingMutationRow {
    pub id: String,
    pub mutation_type: MutationEventType,
    pub record_id: String,
    pub table_name: String,
    pub data: Option<Object>,
    pub before_record: Option<Object>,
    pub created_at: i64,
    pub v: i64,
}

impl PendingMutationRow {
    /// The stored document (without `id`; see [`outbox_op`]).
    pub fn to_json(&self) -> Row {
        let mut o = Row::new();
        o.insert("mutationType".into(), self.mutation_type.as_str().into());
        o.insert("recordId".into(), self.record_id.clone().into());
        o.insert("tableName".into(), self.table_name.clone().into());
        if let Some(data) = &self.data {
            o.insert("data".into(), Value::Object(data.clone()));
        }
        if let Some(before) = &self.before_record {
            o.insert("beforeRecord".into(), Value::Object(before.clone()));
        }
        o.insert("createdAt".into(), Value::Int(self.created_at));
        o.insert("v".into(), Value::Int(self.v));
        o
    }

    pub fn with_data(self, data: Option<Object>) -> Self {
        PendingMutationRow { data, ..self }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FailedMutationError {
    pub message: String,
    pub kind: FailedErrorKind,
}

impl FailedMutationError {
    pub fn application(message: impl Into<String>) -> Self {
        FailedMutationError {
            message: message.into(),
            kind: FailedErrorKind::Application,
        }
    }

    pub fn to_json(&self) -> Object {
        let mut o = Object::new();
        o.insert("message".into(), self.message.clone().into());
        o.insert("kind".into(), self.kind.as_str().into());
        o
    }
}

/// One row of the failed-write tray.
#[derive(Clone, Debug, PartialEq)]
pub struct FailedMutationRow {
    /// The pending (`_00_pending_mutations:`) id it was queued under.
    pub id: String,
    pub mutation_type: MutationEventType,
    pub record_id: String,
    pub table_name: String,
    pub data: Option<Object>,
    pub before_record: Option<Object>,
    pub error: FailedMutationError,
    pub attempts: u32,
    pub created_at: i64,
    pub failed_at: i64,
    pub revert: RevertKind,
}

impl FailedMutationRow {
    pub fn to_json(&self) -> Row {
        let mut o = Row::new();
        o.insert("mutationType".into(), self.mutation_type.as_str().into());
        o.insert("recordId".into(), self.record_id.clone().into());
        o.insert("tableName".into(), self.table_name.clone().into());
        if let Some(data) = &self.data {
            o.insert("data".into(), Value::Object(data.clone()));
        }
        o.insert(
            "beforeRecord".into(),
            self.before_record
                .clone()
                .map(Value::Object)
                .unwrap_or(Value::Null),
        );
        o.insert("error".into(), Value::Object(self.error.to_json()));
        o.insert("attempts".into(), Value::Int(i64::from(self.attempts)));
        o.insert("createdAt".into(), Value::Int(self.created_at));
        o.insert("failedAt".into(), Value::Int(self.failed_at));
        o.insert("revert".into(), self.revert.as_str().into());
        o
    }

    /// The pending row this tray entry can be replayed as.
    pub fn to_pending(&self) -> PendingMutationRow {
        PendingMutationRow {
            id: self.id.clone(),
            mutation_type: self.mutation_type,
            record_id: self.record_id.clone(),
            table_name: self.table_name.clone(),
            data: self.data.clone(),
            before_record: self.before_record.clone(),
            created_at: self.created_at,
            v: PENDING_ROW_VERSION,
        }
    }
}

fn parse_type(raw: Option<&Value>) -> Option<MutationEventType> {
    raw.and_then(Value::as_str)
        .and_then(MutationEventType::parse)
}

/// Stable `table:id` string for an id read from the store, SurrealDB's `⟨⟩`
/// escaping removed (outbox ids contain `_`, so a SurrealDB-written row comes
/// back escaped).
pub fn stored_id_string(id: Option<&Value>) -> String {
    if let Some(Value::RecordId(r)) = id {
        return format!("{}:{}", r.table, r.key);
    }
    let raw = id
        .map(Value::to_id_string)
        .unwrap_or_else(|| "null".to_string());
    let Some((table, rest)) = raw.split_once(':') else {
        return raw;
    };
    let rest = rest
        .strip_prefix('⟨')
        .and_then(|r| r.strip_suffix('⟩'))
        .unwrap_or(rest);
    format!("{table}:{rest}")
}

static TS_PREFIX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^⟨?(\d{13})").unwrap());

/// Timestamp prefix of a v1/v2 mutation id, or `None` for a legacy numeric id.
pub fn created_at_from_id(id: &str) -> Option<i64> {
    let raw = id.split_once(':').map(|(_, r)| r).unwrap_or(id);
    TS_PREFIX
        .captures(raw)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
}

/// Read any row shape this table ever had. Legacy rows (`v` missing) lack
/// `tableName`, `createdAt` and `beforeRecord`. A create without `data` is not
/// replayable and returns `None`.
pub fn parse_pending_row(row: &Row) -> Option<PendingMutationRow> {
    let mutation_type = parse_type(row.get("mutationType"))?;
    let record_id = match row.get("recordId")? {
        Value::String(s) => s.clone(),
        r @ Value::RecordId(_) => stored_id_string(Some(r)),
        _ => return None,
    };
    let id = stored_id_string(row.get("id"));
    let data = row.get("data").and_then(Value::as_object).cloned();
    if mutation_type == MutationEventType::Create && data.is_none() {
        return None;
    }
    Some(PendingMutationRow {
        table_name: row
            .get("tableName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| table_of(&record_id).to_string()),
        before_record: row.get("beforeRecord").and_then(Value::as_object).cloned(),
        created_at: num_i64(row.get("createdAt"))
            .or_else(|| created_at_from_id(&id))
            .unwrap_or(0),
        v: num_i64(row.get("v")).unwrap_or(1),
        id,
        mutation_type,
        record_id,
        data,
    })
}

pub fn parse_failed_row(row: &Row) -> Option<FailedMutationRow> {
    let mutation_type = parse_type(row.get("mutationType"))?;
    let record_id = row.get("recordId")?.as_str()?.to_string();
    let err = row.get("error").and_then(Value::as_object);
    let id = stored_id_string(row.get("id")).replacen(
        &format!("{FAILED_TABLE}:"),
        &format!("{PENDING_TABLE}:"),
        1,
    );
    Some(FailedMutationRow {
        id,
        mutation_type,
        table_name: row
            .get("tableName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| table_of(&record_id).to_string()),
        record_id,
        data: row.get("data").and_then(Value::as_object).cloned(),
        before_record: row.get("beforeRecord").and_then(Value::as_object).cloned(),
        error: FailedMutationError {
            message: err
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            kind: if err.and_then(|e| e.get("kind")).and_then(Value::as_str) == Some("unreplayable")
            {
                FailedErrorKind::Unreplayable
            } else {
                FailedErrorKind::Application
            },
        },
        attempts: num_i64(row.get("attempts")).unwrap_or(0).max(0) as u32,
        created_at: num_i64(row.get("createdAt")).unwrap_or(0),
        failed_at: num_i64(row.get("failedAt")).unwrap_or(0),
        revert: match row.get("revert").and_then(Value::as_str) {
            Some("partial") => RevertKind::Partial,
            Some("none") => RevertKind::None,
            _ => RevertKind::Full,
        },
    })
}

/// Re-type a stored row's payload from the schema before it is pushed. The
/// store keeps JSON, so a `record<user>` field comes back as `'user:x'` and a
/// datetime as a string; the server coerces neither. A payload the schema
/// rejects is sent raw, so the server's answer decides its fate rather than a
/// local throw.
pub fn hydrate_row_data(
    row: PendingMutationRow,
    columns: Option<&BTreeMap<String, ColumnSchema>>,
) -> PendingMutationRow {
    let (Some(data), Some(columns)) = (&row.data, columns) else {
        return row;
    };
    match parse_query_params(columns, data) {
        Ok(typed) => row.with_data(Some(typed)),
        Err(_) => row,
    }
}

/// A row read back from the store at boot or bucket switch. The run that
/// queued it may have sent it and died before the answer (a killed app), so
/// its outcome is in doubt.
pub fn to_outbox_item(row: &PendingMutationRow) -> OutboxItem {
    OutboxItem {
        id: row.id.clone(),
        kind: row.mutation_type,
        record_id: row.record_id.clone(),
        table: row.table_name.clone(),
        status: OutboxStatus::Pending,
        acked_at: None,
        attempts: 0,
        in_doubt: true,
    }
}

/// Outbox rows in drain order. Lexicographic id order is chronological order.
pub fn sort_pending_rows(mut rows: Vec<PendingMutationRow>) -> Vec<PendingMutationRow> {
    rows.sort_by(|a, b| a.id.cmp(&b.id));
    rows
}

pub fn sort_failed_rows(mut rows: Vec<FailedMutationRow>) -> Vec<FailedMutationRow> {
    rows.sort_by_key(|r| r.failed_at);
    rows
}

// ---- local write transactions ------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct WritePlanInput {
    pub record_id: String,
    pub mutation_id: String,
    pub table: String,
    pub data: Option<Object>,
    pub before: Option<Object>,
    pub now: i64,
}

fn pending_row(
    input: &WritePlanInput,
    kind: MutationEventType,
    with_data: bool,
    with_before: bool,
) -> PendingMutationRow {
    PendingMutationRow {
        id: input.mutation_id.clone(),
        mutation_type: kind,
        record_id: input.record_id.clone(),
        table_name: input.table.clone(),
        data: with_data.then(|| input.data.clone().unwrap_or_default()),
        before_record: if with_before {
            input.before.clone()
        } else {
            None
        },
        created_at: input.now,
        v: PENDING_ROW_VERSION,
    }
}

/// The outbox row's write. The document carries its own `id`, as the store
/// hands documents back, so any store reads it the same.
pub fn outbox_op(row: &PendingMutationRow) -> LocalOp {
    let mut data = row.to_json();
    data.insert("id".into(), row.id.clone().into());
    LocalOp::put(PENDING_TABLE, row.id.clone(), data)
}

/// CREATE the row and its outbox entry in one transaction.
///
/// `_00_rv` is seeded to 1 because the local schema's DEFAULT does not apply to
/// a document write, so a later update's `_00_rv += 1` reaches 2.
pub fn plan_create_tx(input: &WritePlanInput) -> Vec<LocalOp> {
    let mut row = input.data.clone().unwrap_or_default();
    row.insert("id".into(), input.record_id.clone().into());
    row.insert("_00_rv".into(), Value::Int(1));
    vec![
        LocalOp::put(input.table.clone(), input.record_id.clone(), row),
        outbox_op(&pending_row(input, MutationEventType::Create, true, false)),
    ]
}

/// Bump `_00_rv`, merge the patch, write the outbox row with its `beforeRecord`.
pub fn plan_update_tx(input: &WritePlanInput) -> Vec<LocalOp> {
    vec![
        LocalOp::BumpRv {
            table: input.table.clone(),
            id: input.record_id.clone(),
        },
        LocalOp::merge(
            input.table.clone(),
            input.record_id.clone(),
            input.data.clone().unwrap_or_default(),
            Vec::new(),
        ),
        outbox_op(&pending_row(input, MutationEventType::Update, true, true)),
    ]
}

/// DELETE the row and write the outbox entry with its `beforeRecord`.
pub fn plan_delete_tx(input: &WritePlanInput) -> Vec<LocalOp> {
    vec![
        LocalOp::delete(input.table.clone(), input.record_id.clone()),
        outbox_op(&pending_row(input, MutationEventType::Delete, false, true)),
    ]
}

/// A channel peer's create: the row alone, no outbox entry (the peer's own
/// client pushes it). `_00_rv` stays unset, so the server's body (with its
/// computed fields) still downloads and replaces this one.
pub fn plan_peer_create_tx(table: &str, record_id: &str, data: Object) -> Vec<LocalOp> {
    let mut row = data;
    row.insert("id".into(), record_id.into());
    vec![LocalOp::put(table, record_id, row)]
}

/// Undo a peer's create the server never confirmed: the row alone, no outbox
/// entry.
pub fn plan_peer_undo_tx(table: &str, record_id: &str) -> Vec<LocalOp> {
    vec![LocalOp::delete(table, record_id)]
}

/// Debounced update: bump `_00_rv` and merge now; the outbox row comes later.
pub fn plan_local_only_update_tx(table: &str, record_id: &str, data: Object) -> Vec<LocalOp> {
    vec![
        LocalOp::BumpRv {
            table: table.to_string(),
            id: record_id.to_string(),
        },
        LocalOp::merge(table, record_id, data, Vec::new()),
    ]
}

/// The outbox row of a flushed debounced update, on its own.
pub fn plan_deferred_outbox_row_tx(input: &WritePlanInput) -> Vec<LocalOp> {
    vec![outbox_op(&pending_row(
        input,
        MutationEventType::Update,
        true,
        true,
    ))]
}

// ---- remote push -------------------------------------------------------------

/// What a statement of a [`remote_batch`] throws instead of running, because an
/// earlier statement in the same request failed.
pub const BATCH_SKIPPED: &str = "sp00ky: not run, an earlier statement in this batch failed";

/// The payload a statement sends: null fields are left out, never sent as
/// NULL (SurrealDB rejects NULL for `option<T>`; an unset optional stays NONE).
fn sent_data(row: &PendingMutationRow) -> Vec<(&String, &Value)> {
    row.data
        .iter()
        .flat_map(|d| d.iter())
        .filter(|(_, v)| !v.is_nullish())
        .collect()
}

/// Which fields of the row already on the server must equal the payload of
/// the CREATE at `i` for that row to count as this CREATE's own earlier
/// commit, or `None` when the CREATE must not tolerate an existing row.
///
/// Only a CREATE whose outcome is in doubt tolerates one: elsewhere "already
/// exists" means someone else holds the id, a rejection as before. The check
/// is every payload field except those a later in-doubt row of the same
/// record, which may have committed in the same lost round, could have
/// changed since; those rows run again after it and set them anyway. A later
/// in-doubt DELETE or CREATE of the record may have replaced the row, so then
/// existence is all that is checked. Rows that are not in doubt never ran.
fn match_fields(
    rows: &[PendingMutationRow],
    i: usize,
    in_doubt: &BTreeSet<String>,
) -> Option<Vec<String>> {
    let row = &rows[i];
    if !in_doubt.contains(&row.id) {
        return None;
    }
    let mut changed: BTreeSet<&str> = BTreeSet::new();
    for later in &rows[i + 1..] {
        if later.record_id != row.record_id || !in_doubt.contains(&later.id) {
            continue;
        }
        if later.mutation_type != MutationEventType::Update {
            return Some(Vec::new());
        }
        changed.extend(later.data.iter().flat_map(|d| d.keys()).map(String::as_str));
    }
    Some(
        sent_data(row)
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| !changed.contains(k.as_str()))
            .cloned()
            .collect(),
    )
}

fn remote_statement(
    rows: &[PendingMutationRow],
    i: usize,
    vars: &mut Object,
    in_doubt: &BTreeSet<String>,
) -> String {
    let row = &rows[i];
    vars.insert(
        format!("id{i}"),
        Value::RecordId(RecordId::parse(&row.record_id)),
    );
    match row.mutation_type {
        MutationEventType::Create => {
            let mut sets = Vec::new();
            for (key, value) in sent_data(row) {
                vars.insert(format!("d{i}_{key}"), value.clone());
                sets.push(format!("{key} = $d{i}_{key}"));
            }
            let create = format!("CREATE ONLY $id{i} SET {}", sets.join(", "));
            let Some(fields) = match_fields(rows, i, in_doubt) else {
                return create;
            };
            // The CREATE runs unless the row is there and matches; on a
            // mismatch it fails with SurrealDB's own "already exists", a
            // rejection as before.
            let differs: Vec<String> = std::iter::once(format!("($was{i} = NONE)"))
                .chain(fields.iter().map(|k| format!("($was{i}.{k} != $d{i}_{k})")))
                .collect();
            format!(
                "LET $was{i} = (SELECT * FROM ONLY $id{i}); IF {} {{ {create} }}",
                differs.join(" OR ")
            )
        }
        MutationEventType::Update => {
            let data: Object = sent_data(row)
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            vars.insert(format!("data{i}"), Value::Object(data));
            format!("UPDATE $id{i} MERGE $data{i}")
        }
        MutationEventType::Delete => format!("DELETE $id{i}"),
    }
}

/// Many outbox rows as ONE multi-statement request. Statement `i` answers row
/// `i` and is its own transaction on the server, but the statements are
/// chained: statement `i` binds `$ok<i>` only when it commits, and statement
/// `i + 1` throws [`BATCH_SKIPPED`] without touching anything unless `$ok<i>`
/// is bound (a LET lives for one request only). So the server stops at the
/// first failure: everything before it is committed, nothing after it ran.
///
/// Without the chain, statements after a failed one ran anyway. A retryable
/// failure at `i` left the rows after `i` queued although they had committed;
/// the re-sent CREATE then read "already exists", was taken for a rejection
/// and rolled back locally while the server kept the row. Acking them instead
/// would have let a later write land before an earlier one it may depend on.
///
/// The chain cannot help when the answer itself is lost: the request may have
/// committed any prefix of the batch. Rows named in `in_doubt` (mutation ids)
/// are such rows. An UPDATE MERGE or a DELETE is safe to run twice, a CREATE
/// is not, so an in-doubt CREATE checks the row already there first (see
/// [`match_fields`]) and is a no-op when it is its own: the batch goes on in
/// order, with no "already exists" to cut it.
pub fn remote_batch(rows: &[PendingMutationRow], in_doubt: &BTreeSet<String>) -> (String, Object) {
    let mut vars = Object::new();
    let mut stmts = Vec::with_capacity(rows.len());
    for i in 0..rows.len() {
        let guard = if i == 0 {
            String::new()
        } else {
            format!("IF !$ok{} {{ THROW \"{BATCH_SKIPPED}\" }}; ", i - 1)
        };
        let statement = remote_statement(rows, i, &mut vars, in_doubt);
        stmts.push(format!("LET $ok{i} = {{ {guard}{statement}; true }}"));
    }
    (stmts.join(";\n"), vars)
}

// ---- rollback ----------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct RevertPlan {
    /// Local writes that undo the mutation; empty when nothing can be restored.
    pub ops: Vec<LocalOp>,
    pub circuit: IngestRecord,
    pub revert: RevertKind,
}

/// How to undo a rejected mutation locally. A create is deleted; an update or
/// delete is restored from `beforeRecord` (replace, so the pre-bump `_00_rv`
/// comes back). Without a `beforeRecord` nothing can be restored.
pub fn plan_revert(row: &PendingMutationRow, before: Option<&Object>) -> RevertPlan {
    if row.mutation_type == MutationEventType::Create {
        return RevertPlan {
            ops: vec![LocalOp::delete(
                row.table_name.clone(),
                row.record_id.clone(),
            )],
            circuit: IngestRecord::new(
                row.table_name.clone(),
                IngestOp::Delete,
                row.record_id.clone(),
                row.data.clone().unwrap_or_default(),
            ),
            revert: RevertKind::Full,
        };
    }
    let Some(before) = before else {
        return RevertPlan {
            ops: Vec::new(),
            circuit: IngestRecord::new(
                row.table_name.clone(),
                IngestOp::Update,
                row.record_id.clone(),
                Row::new(),
            ),
            revert: RevertKind::Partial,
        };
    };
    let mut restored = before.clone();
    restored.insert("id".into(), row.record_id.clone().into());
    RevertPlan {
        ops: vec![LocalOp::put(
            row.table_name.clone(),
            row.record_id.clone(),
            restored.clone(),
        )],
        circuit: IngestRecord::new(
            row.table_name.clone(),
            if row.mutation_type == MutationEventType::Delete {
                IngestOp::Create
            } else {
                IngestOp::Update
            },
            row.record_id.clone(),
            restored,
        ),
        revert: RevertKind::Full,
    }
}

/// The tray row id of a mutation.
pub fn failed_record_id(mutation_id: &str) -> String {
    let key = mutation_id
        .split_once(':')
        .map(|(_, k)| k)
        .unwrap_or(mutation_id);
    format!("{FAILED_TABLE}:{key}")
}

pub fn build_failed_row(
    row: &PendingMutationRow,
    error: FailedMutationError,
    before: Option<Object>,
    attempts: u32,
    now: i64,
    revert: RevertKind,
) -> FailedMutationRow {
    FailedMutationRow {
        id: row.id.clone(),
        mutation_type: row.mutation_type,
        record_id: row.record_id.clone(),
        table_name: row.table_name.clone(),
        data: row.data.clone(),
        before_record: before,
        error,
        attempts,
        created_at: row.created_at,
        failed_at: now,
        revert,
    }
}

/// ONE transaction: write the tray row, delete the pending row.
pub fn move_to_failed_tx(failed: &FailedMutationRow) -> Vec<LocalOp> {
    let id = failed_record_id(&failed.id);
    let mut doc = failed.to_json();
    doc.insert("id".into(), id.clone().into());
    vec![
        LocalOp::put(FAILED_TABLE, id, doc),
        delete_pending_row(&failed.id),
    ]
}

pub fn delete_pending_row(mutation_id: &str) -> LocalOp {
    LocalOp::delete(PENDING_TABLE, mutation_id)
}

pub fn delete_failed_row(mutation_id: &str) -> LocalOp {
    LocalOp::delete(FAILED_TABLE, failed_record_id(mutation_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::ports::WriteMode;
    use crate::testing::row;
    use serde_json::json;

    fn put(op: &LocalOp) -> (&str, &str, &Row, WriteMode) {
        match op {
            LocalOp::Put {
                table,
                id,
                data,
                mode,
                ..
            } => (table, id, data, *mode),
            other => panic!("not a put: {other:?}"),
        }
    }

    #[test]
    fn stored_ids_strip_escaping_and_read_the_timestamp() {
        assert_eq!(
            stored_id_string(Some(
                &"_00_pending_mutations:⟨1700000000000_0001_ab⟩".into()
            )),
            "_00_pending_mutations:1700000000000_0001_ab"
        );
        assert_eq!(
            stored_id_string(Some(&Value::RecordId(RecordId::new(
                "_00_pending_mutations",
                "x"
            )))),
            "_00_pending_mutations:x"
        );
        assert_eq!(stored_id_string(Some(&"bare".into())), "bare");
        assert_eq!(
            created_at_from_id("_00_pending_mutations:1700000000000_0001_ab"),
            Some(1_700_000_000_000)
        );
        assert_eq!(
            created_at_from_id("_00_pending_mutations:⟨1700000000000_0001_ab⟩"),
            Some(1_700_000_000_000)
        );
        assert_eq!(created_at_from_id("_00_pending_mutations:123"), None);
    }

    #[test]
    fn reads_v2_rows() {
        let r = parse_pending_row(&row(json!({
            "id": "_00_pending_mutations:1700000000000_0001_ab",
            "mutationType": "update",
            "recordId": "thing:1",
            "tableName": "thing",
            "data": {"a": 1},
            "beforeRecord": {"a": 0},
            "createdAt": 5,
            "v": 2,
        })))
        .unwrap();
        assert_eq!(r.mutation_type, MutationEventType::Update);
        assert_eq!(r.record_id, "thing:1");
        assert_eq!(r.data, Some(row(json!({"a": 1}))));
        assert_eq!(r.before_record, Some(row(json!({"a": 0}))));
        assert_eq!(r.created_at, 5);
        assert_eq!(r.v, 2);
        let item = to_outbox_item(&r);
        assert_eq!(item.status, OutboxStatus::Pending);
        assert_eq!(item.table, "thing");
        assert!(item.in_doubt);
    }

    #[test]
    fn tolerates_legacy_rows_and_rejects_unreplayable_ones() {
        let legacy = parse_pending_row(&row(json!({
            "id": "_00_pending_mutations:1700000000000_0001_ab",
            "mutationType": "delete",
            "recordId": "thing:9",
        })))
        .unwrap();
        assert_eq!(legacy.table_name, "thing");
        assert_eq!(legacy.v, 1);
        assert_eq!(legacy.created_at, 1_700_000_000_000);

        assert!(parse_pending_row(&Row::new()).is_none());
        assert!(parse_pending_row(&row(json!({"mutationType": "nope"}))).is_none());
        assert!(
            parse_pending_row(&row(json!({"mutationType": "create", "recordId": 1}))).is_none()
        );
        // A create with no payload cannot be replayed.
        assert!(parse_pending_row(&row(json!({
            "id": "_00_pending_mutations:1",
            "mutationType": "create",
            "recordId": "thing:1",
        })))
        .is_none());
    }

    #[test]
    fn sorts_the_drain_by_id() {
        let p = |id: &str| PendingMutationRow {
            id: id.into(),
            mutation_type: MutationEventType::Delete,
            record_id: "thing:1".into(),
            table_name: "thing".into(),
            data: None,
            before_record: None,
            created_at: 0,
            v: 2,
        };
        let ids: Vec<String> = sort_pending_rows(vec![p("b"), p("a")])
            .into_iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(ids, ["a", "b"]);
    }

    fn plan_input() -> WritePlanInput {
        WritePlanInput {
            record_id: "thing:1".into(),
            mutation_id: "_00_pending_mutations:m1".into(),
            table: "thing".into(),
            data: Some(row(json!({"a": 1}))),
            before: Some(row(json!({"a": 0}))),
            now: 42,
        }
    }

    #[test]
    fn create_writes_the_row_at_rv_1_plus_the_v2_outbox_entry() {
        let ops = plan_create_tx(&plan_input());
        assert_eq!(ops.len(), 2);
        let (table, id, data, mode) = put(&ops[0]);
        assert_eq!((table, id), ("thing", "thing:1"));
        assert_eq!(*data, row(json!({"a": 1, "id": "thing:1", "_00_rv": 1})));
        assert_eq!(mode, WriteMode::Replace);
        let (table, _, outbox, _) = put(&ops[1]);
        assert_eq!(table, PENDING_TABLE);
        assert_eq!(outbox["mutationType"], Value::from("create"));
        assert_eq!(outbox["v"], Value::Int(PENDING_ROW_VERSION));
        assert_eq!(outbox["createdAt"], Value::Int(42));
        assert!(!outbox.contains_key("beforeRecord"));
    }

    #[test]
    fn update_bumps_rv_merges_and_records_before() {
        let ops = plan_update_tx(&plan_input());
        assert!(matches!(ops[0], LocalOp::BumpRv { .. }));
        assert_eq!(put(&ops[1]).3, WriteMode::Merge);
        let outbox = put(&ops[2]).2;
        assert_eq!(outbox["beforeRecord"], Value::Object(row(json!({"a": 0}))));
        assert_eq!(outbox["mutationType"], Value::from("update"));
    }

    #[test]
    fn delete_removes_the_row_and_records_before() {
        let ops = plan_delete_tx(&plan_input());
        assert!(matches!(ops[0], LocalOp::Delete { .. }));
        let outbox = put(&ops[1]).2;
        assert_eq!(outbox["mutationType"], Value::from("delete"));
        assert_eq!(outbox["beforeRecord"], Value::Object(row(json!({"a": 0}))));
        assert!(!outbox.contains_key("data"));
    }

    #[test]
    fn debounced_update_writes_locally_now_and_the_outbox_row_later() {
        let local = plan_local_only_update_tx("thing", "thing:1", row(json!({"a": 1})));
        assert_eq!(local.len(), 2);
        assert_eq!(put(&local[1]).3, WriteMode::Merge);
        let deferred = plan_deferred_outbox_row_tx(&plan_input());
        assert_eq!(deferred.len(), 1);
        assert_eq!(put(&deferred[0]).0, PENDING_TABLE);
    }

    fn pending(
        id: &str,
        kind: MutationEventType,
        record_id: &str,
        data: Option<serde_json::Value>,
    ) -> PendingMutationRow {
        PendingMutationRow {
            id: id.into(),
            mutation_type: kind,
            record_id: record_id.into(),
            table_name: "thing".into(),
            data: data.map(row),
            before_record: None,
            created_at: 0,
            v: 2,
        }
    }

    #[test]
    fn remote_batch_one_chained_statement_per_row() {
        use MutationEventType::*;
        let (sql, vars) = remote_batch(
            &[
                pending(
                    "_00_pending_mutations:a",
                    Create,
                    "thing:a",
                    Some(json!({"x": 1})),
                ),
                pending(
                    "_00_pending_mutations:b",
                    Update,
                    "thing:b",
                    Some(json!({"y": 2})),
                ),
                pending("_00_pending_mutations:c", Delete, "thing:c", None),
            ],
            &BTreeSet::new(),
        );
        // Chained: each statement runs only if the one before it committed.
        let skip = |prev: usize| format!("IF !$ok{prev} {{ THROW \"{BATCH_SKIPPED}\" }}; ");
        assert_eq!(
            sql.split(";\n").collect::<Vec<_>>(),
            vec![
                "LET $ok0 = { CREATE ONLY $id0 SET x = $d0_x; true }".to_string(),
                format!("LET $ok1 = {{ {}UPDATE $id1 MERGE $data1; true }}", skip(0)),
                format!("LET $ok2 = {{ {}DELETE $id2; true }}", skip(1)),
            ]
        );
        assert_eq!(vars["d0_x"], Value::Int(1));
        assert_eq!(vars["data1"], Value::Object(row(json!({"y": 2}))));
        assert_eq!(vars["id2"], Value::RecordId(RecordId::new("thing", "c")));
    }

    #[test]
    fn null_fields_are_left_out_never_sent_as_null() {
        use MutationEventType::*;
        let (sql, vars) = remote_batch(
            &[
                pending("m1", Create, "thing:1", Some(json!({"x": 1, "gone": null}))),
                pending("m2", Update, "thing:1", Some(json!({"y": null, "z": 2}))),
            ],
            &BTreeSet::new(),
        );
        assert!(!sql.contains("gone"));
        assert!(!vars.contains_key("d0_gone"));
        assert_eq!(vars["data1"], Value::Object(row(json!({"z": 2}))));
    }

    #[test]
    fn an_in_doubt_create_is_a_no_op_when_the_server_row_matches() {
        use MutationEventType::*;
        let was = |i: usize, keys: &[&str], sets: &str| {
            let mut differs = vec![format!("($was{i} = NONE)")];
            differs.extend(keys.iter().map(|k| format!("($was{i}.{k} != $d{i}_{k})")));
            let sets = if sets.is_empty() {
                format!("a = $d{i}_a, b = $d{i}_b")
            } else {
                sets.to_string()
            };
            format!(
                "LET $was{i} = (SELECT * FROM ONLY $id{i}); IF {} {{ CREATE ONLY $id{i} SET {sets} }}",
                differs.join(" OR ")
            )
        };
        let strip =
            Regex::new(r#"^LET \$ok\d+ = \{ (IF !\$ok\d+ \{ THROW "[^"]+" \}; )?"#).unwrap();
        let statement = |sql: &str, i: usize| {
            let s = sql.split(";\n").nth(i).unwrap();
            strip
                .replace(s, "")
                .trim_end_matches("; true }")
                .to_string()
        };
        let (sql, _) = remote_batch(
            &[
                pending("m1", Create, "thing:1", Some(json!({"a": 1, "b": 1}))),
                pending("m2", Update, "thing:1", Some(json!({"b": 2}))),
                pending("m3", Create, "thing:2", Some(json!({"a": 1, "b": 1}))),
                pending("m4", Update, "thing:2", Some(json!({"a": 2}))), // not in doubt: it never ran
                pending("m5", Create, "thing:3", Some(json!({"a": 1, "b": 1}))),
                pending("m6", Delete, "thing:3", None),
                pending("m7", Create, "thing:4", Some(json!({"a": 1, "b": 1}))),
                pending("m8", Update, "thing:9", Some(json!({"a": 2}))), // another record
                pending("m9", Update, "thing:1", None), // no payload: changes nothing
                pending("m10", Create, "thing:5", Some(json!({}))),
            ],
            &["m1", "m2", "m3", "m5", "m6", "m8", "m9", "m10"]
                .into_iter()
                .map(String::from)
                .collect(),
        );
        assert_eq!(statement(&sql, 0), was(0, &["a"], "")); // b may be m2's
        assert_eq!(statement(&sql, 2), was(2, &["a", "b"], ""));
        // m6 may have deleted it and something else re-created it.
        assert_eq!(statement(&sql, 4), was(4, &[], ""));
        assert_eq!(
            statement(&sql, 6),
            "CREATE ONLY $id6 SET a = $d6_a, b = $d6_b"
        ); // strict
        assert_eq!(
            statement(&sql, 9),
            "LET $was9 = (SELECT * FROM ONLY $id9); IF ($was9 = NONE) { CREATE ONLY $id9 SET  }"
        );
    }

    fn rollback_row(
        kind: MutationEventType,
        data: Option<serde_json::Value>,
    ) -> PendingMutationRow {
        PendingMutationRow {
            id: "_00_pending_mutations:m1".into(),
            created_at: 1,
            ..pending("_00_pending_mutations:m1", kind, "thing:1", data)
        }
    }

    #[test]
    fn a_create_reverts_by_delete() {
        let plan = plan_revert(
            &rollback_row(MutationEventType::Create, Some(json!({"a": 1}))),
            None,
        );
        assert_eq!(plan.revert, RevertKind::Full);
        assert!(matches!(plan.ops.as_slice(), [LocalOp::Delete { .. }]));
        assert_eq!(plan.circuit.op, IngestOp::Delete);
    }

    #[test]
    fn update_and_delete_restore_before_or_are_partial_without_it() {
        let before = row(json!({"a": 0, "id": "thing:1"}));
        let restored = plan_revert(
            &rollback_row(MutationEventType::Update, None),
            Some(&before),
        );
        assert_eq!(restored.revert, RevertKind::Full);
        assert_eq!(put(&restored.ops[0]).2["a"], Value::Int(0));
        assert_eq!(restored.circuit.op, IngestOp::Update);

        let undeleted = plan_revert(
            &rollback_row(MutationEventType::Delete, None),
            Some(&before),
        );
        assert_eq!(undeleted.circuit.op, IngestOp::Create);

        let partial = plan_revert(&rollback_row(MutationEventType::Update, None), None);
        assert_eq!(partial.revert, RevertKind::Partial);
        assert!(partial.ops.is_empty());
    }

    #[test]
    fn failed_rows_build_move_delete_parse() {
        let pending = rollback_row(MutationEventType::Update, Some(json!({"a": 1})));
        let failed = build_failed_row(
            &pending,
            FailedMutationError::application("nope"),
            Some(row(json!({"a": 0}))),
            2,
            99,
            RevertKind::Full,
        );
        assert_eq!(failed_record_id(&failed.id), "_00_failed_mutations:m1");
        let ops = move_to_failed_tx(&failed);
        assert_eq!(put(&ops[0]).0, FAILED_TABLE);
        assert!(matches!(&ops[1], LocalOp::Delete { table, .. } if table == PENDING_TABLE));

        let mut doc = failed.to_json();
        doc.insert("id".into(), "_00_failed_mutations:m1".into());
        let parsed = parse_failed_row(&doc).unwrap();
        assert_eq!(parsed.id, "_00_pending_mutations:m1");
        assert_eq!(parsed.error.message, "nope");
        assert_eq!(parsed.error.kind, FailedErrorKind::Application);
        assert_eq!(parsed.attempts, 2);
        assert_eq!(parsed.failed_at, 99);
        assert_eq!(parsed.revert, RevertKind::Full);
        assert_eq!(parsed.to_pending().mutation_type, MutationEventType::Update);

        assert!(
            matches!(delete_failed_row("_00_pending_mutations:m1"), LocalOp::Delete { id, .. } if id == "_00_failed_mutations:m1")
        );
        assert!(
            matches!(delete_pending_row("_00_pending_mutations:m1"), LocalOp::Delete { id, .. } if id == "_00_pending_mutations:m1")
        );
        assert!(parse_failed_row(&Row::new()).is_none());
        assert_eq!(sort_failed_rows(vec![parsed.clone()])[0].id, parsed.id);
    }

    #[test]
    fn hydrate_retypes_record_and_datetime_fields_keeps_unknown_keys() {
        let r = PendingMutationRow {
            data: Some(row(
                json!({"owner": "user:u1", "at": "2026-01-02T03:04:05Z", "extra": 7}),
            )),
            ..rollback_row(MutationEventType::Update, None)
        };
        let mut columns = BTreeMap::new();
        columns.insert(
            "owner".to_string(),
            ColumnSchema {
                record_id: true,
                ..ColumnSchema::of_type("record<user>")
            },
        );
        columns.insert(
            "at".to_string(),
            ColumnSchema {
                date_time: true,
                ..ColumnSchema::of_type("datetime")
            },
        );
        let out = hydrate_row_data(r.clone(), Some(&columns));
        let data = out.data.unwrap();
        assert!(matches!(data["owner"], Value::RecordId(_)));
        assert!(matches!(data["at"], Value::Datetime(_)));
        assert_eq!(data["extra"], Value::Int(7));
        // No schema: unchanged.
        assert_eq!(hydrate_row_data(r.clone(), None), r);
    }
}
