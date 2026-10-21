//! Replaying change notices a node accepted but could not apply at the time.
//!
//! A host that acknowledges `/ingest` before the circuit can take it (a cold
//! Durable Object, a bootstrap in progress, a full publication queue) keeps the
//! notice and replays it once the node is Ready, so a user's write never fails
//! because the SSP was not there to take it.
//!
//! A notice only says "this record changed"; the database says what it is
//! now. Replay therefore re-reads the noticed records and ingests their
//! current state, which is safe against the three ways a raw replay goes
//! wrong: an older event landing after a rebuild already read the newer row
//! (the circuit would stamp it newer and regress the content), an event from a
//! transaction that rolled back after its post, and one record noticed
//! several times. Records the circuit already holds at the DB's version are
//! skipped. Tables whose ingest is a side effect rather than row state are
//! replayed as they came.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Context};
use serde_json::Value;
use ssp_protocol::{IngestBatchRequest, IngestRequest, MAX_INGEST_BATCH_BYTES, MAX_INGEST_BATCH_RECORDS};

use crate::api::{ApiRequest, Method};
use crate::SspNode;

/// Records read back per query.
const FETCH_CHUNK: usize = 500;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    /// Notices handed in.
    pub notices: usize,
    /// Records ingested with their current DB state.
    pub ingested: usize,
    /// Records gone from the DB that the circuit still held, deleted.
    pub deleted: usize,
    /// Records the circuit already held at the DB's version.
    pub current: usize,
    /// Notices of side-effect tables, replayed as they came.
    pub as_is: usize,
}

/// Tables whose ingest is a side effect rather than row state (the query
/// allowlist, direct pushes, other internal tables): their notices are
/// replayed as they came, with their bodies, in order.
pub fn replays_as_is(table: &str) -> bool {
    ssp_protocol::table_excluded_from_sync(table) || table == push_core::engine::MESSAGE_TABLE
}

/// The part of a notice replay needs. A row table's body is dropped (replay
/// reads the record again), which keeps a stored notice to a few dozen bytes.
pub fn compact_notice(mut notice: IngestRequest) -> IngestRequest {
    if !replays_as_is(&notice.table) {
        notice.record = Value::Null;
    }
    notice
}

impl SspNode {
    /// Apply `notices` (in arrival order) by their records' current state.
    /// An error leaves nothing half-known: the caller keeps the notices and
    /// retries, and a second replay of what already landed is a no-op.
    pub async fn replay_notices(&self, notices: Vec<IngestRequest>) -> anyhow::Result<ReplayReport> {
        let mut report = ReplayReport { notices: notices.len(), ..Default::default() };
        let mut seen = HashSet::new();
        let mut by_table: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for notice in notices {
            if replays_as_is(&notice.table) {
                self.replay_ingest("/ingest", serde_json::to_vec(&notice)?).await?;
                report.as_is += 1;
            } else if seen.insert((notice.table.clone(), notice.id.clone())) {
                by_table.entry(notice.table).or_default().push(notice.id);
            }
        }

        let mut changes: Vec<IngestRequest> = Vec::new();
        for (table, ids) in by_table {
            let omit = self
                .processor
                .read()
                .await
                .table_meta(&table)
                .map(|m| ssp_protocol::omit_clause(&m.opaque))
                .unwrap_or_default();
            let mut current: HashMap<String, Value> = HashMap::new();
            for chunk in ids.chunks(FETCH_CHUNK) {
                current.extend(fetch_current(self.platform.db.as_ref(), chunk, &omit).await?);
            }
            let circuit = self.processor.read().await;
            for id in ids {
                let present = circuit.contains(&table, &id);
                match current.remove(&id) {
                    Some(row) => {
                        let rv = row.get("_00_rv").and_then(Value::as_i64);
                        if present && rv.is_some() && circuit.stored_rv(&table, &id) >= rv {
                            report.current += 1;
                            continue;
                        }
                        let op = if present { "UPDATE" } else { "CREATE" };
                        changes.push(IngestRequest { table: table.clone(), op: op.into(), id, record: row, job_assignee: None });
                        report.ingested += 1;
                    }
                    None => {
                        // Gone (or never committed). Delete with the circuit's
                        // own body as the before-image, which the per-user
                        // edge cleanup reads.
                        if let Some(before) = circuit.record(&table, &id) {
                            changes.push(IngestRequest { table: table.clone(), op: "DELETE".into(), id, record: before, job_assignee: None });
                            report.deleted += 1;
                        }
                    }
                }
            }
        }

        // `/ingest/batch` refuses the tables with lifecycle side effects; those
        // go one by one through `/ingest`, the rest in batches.
        let (single, batchable): (Vec<_>, Vec<_>) = changes.into_iter().partition(|c| {
            c.table == "user" || c.table.starts_with("_00_") || self.job_config.job_tables.contains_key(&c.table)
        });
        for change in single {
            self.replay_ingest("/ingest", serde_json::to_vec(&change)?).await?;
        }
        let mut batch: Vec<IngestRequest> = Vec::new();
        let mut bytes = 0usize;
        for change in batchable {
            let size = serde_json::to_vec(&change)?.len();
            if size > MAX_INGEST_BATCH_BYTES * 9 / 10 {
                // Too big to share a batch: alone through `/ingest`.
                self.replay_ingest("/ingest", serde_json::to_vec(&change)?).await?;
                continue;
            }
            if !batch.is_empty() && (batch.len() == MAX_INGEST_BATCH_RECORDS || bytes + size > MAX_INGEST_BATCH_BYTES * 9 / 10) {
                self.replay_batch(std::mem::take(&mut batch)).await?;
                bytes = 0;
            }
            bytes += size;
            batch.push(change);
        }
        if !batch.is_empty() {
            self.replay_batch(batch).await?;
        }
        Ok(report)
    }

    async fn replay_batch(&self, records: Vec<IngestRequest>) -> anyhow::Result<()> {
        let body = serde_json::to_vec(&IngestBatchRequest { records })?;
        self.replay_ingest("/ingest/batch", body).await
    }

    async fn replay_ingest(&self, path: &str, body: Vec<u8>) -> anyhow::Result<()> {
        let req = ApiRequest {
            method: Method::Post,
            path: path.to_string(),
            bearer: Some(self.auth_secret.clone()),
            body: body.into(),
        };
        match self.route(req).await {
            Some(resp) if (200..300).contains(&resp.status) => Ok(()),
            // Refused for what it is, so a retry gets the same answer: drop
            // it rather than hold every later notice behind it.
            Some(resp) if (400..500).contains(&resp.status) => {
                let body = match &resp.body {
                    crate::api::ApiBody::Json(v) => v.to_string(),
                    _ => String::new(),
                };
                tracing::warn!(path, status = resp.status, %body, "replay: notice refused, dropped");
                Ok(())
            }
            Some(resp) => Err(anyhow!("replay {path} answered {}", resp.status)),
            None => Err(anyhow!("replay {path}: no route")),
        }
    }
}

/// The current rows for `ids` (`table:key` strings), keyed by id. A record the
/// DB no longer has is simply absent.
async fn fetch_current(
    db: &dyn crate::ports::Db,
    ids: &[String],
    omit: &str,
) -> anyhow::Result<HashMap<String, Value>> {
    let sql = format!("LET $records = array::map($ids, |$i| type::record($i)); SELECT *{omit} FROM $records;");
    let ids_json = Value::Array(ids.iter().cloned().map(Value::String).collect());
    let results = match db.query(&sql, &[("ids", ids_json)]).await {
        Ok(results) => results,
        // A table nobody wrote since it was dropped: nothing is current.
        Err(e) if e.to_string().contains("does not exist") => return Ok(HashMap::new()),
        Err(e) => return Err(anyhow!("{e}")).context("replay: reading current rows"),
    };
    let rows = results.into_iter().last().unwrap_or(Value::Null);
    Ok(match rows {
        Value::Array(rows) => rows
            .into_iter()
            .filter_map(|row| Some((row.get("id")?.as_str()?.to_string(), row)))
            .collect(),
        _ => HashMap::new(),
    })
}
