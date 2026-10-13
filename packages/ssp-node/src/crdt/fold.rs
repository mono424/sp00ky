//! One fold: turn a document field's edit-log rows into its canonical state.
//!
//! 1. Claim the field's lease on `_00_crdt_doc` (another node may hold it).
//! 2. Read the log rows, apply the acceptance rule, import what passes into
//!    the snapshot (reusing the cached document when its `rev` still matches).
//! 3. If the document changed, publish snapshot + text mirror + fold stamp in
//!    one transaction that compare-and-swaps `rev`.
//! 4. Delete the rows the document now fully holds, and the refused ones.
//!
//! Step 4 runs after the commit and in small chunks: a crash in between leaves
//! rows the next fold re-imports for nothing (import is idempotent), never a
//! row whose ops are missing from the snapshot.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Context, Result};
use base64::alphabet::STANDARD;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use base64::engine::DecodePaddingMode;
use base64::Engine;
use loro::{ExportMode, LoroDoc};
use serde_json::{json, Value};
use tracing::{debug, warn};

use super::acceptance::{self, Accepted};
use super::hash;
use super::mirror::{self, Kind};
use super::{sql, DocKey, FoldConfig};
use crate::ports::{Db, DbError};

/// SurrealDB's `encoding::base64::encode` emits no padding; accept either.
const B64: GeneralPurpose = GeneralPurpose::new(
    &STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// A row older than this whose ops still depend on edits the log never got
/// (a refused or lost predecessor) can never apply: it is refused.
const STRANDED_AFTER_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FoldOutcome {
    /// Another node holds the lease or published first.
    pub lost: bool,
    /// The snapshot changed and a new `rev` was published.
    pub published: bool,
    pub rev: i64,
    /// Rows deleted because the document now holds them.
    pub folded: usize,
    /// Rows deleted because they failed the acceptance rule.
    pub rejected: usize,
    /// Rows kept because ops they depend on have not arrived.
    pub kept: usize,
    /// The read hit `rows_per_fold`: more rows are waiting.
    pub more: bool,
}

/// The cached document of a field, valid while `rev` matches the DB.
pub struct Cached {
    pub rev: i64,
    pub doc: LoroDoc,
}

struct Row {
    id: String,
    verdict: Result<(Vec<u8>, Accepted), acceptance::Rejection>,
    created_ms: Option<i64>,
}

pub async fn fold(
    db: &dyn Db,
    cfg: &FoldConfig,
    me: &str,
    key: &DocKey,
    kind: Kind,
    cached: Option<Cached>,
    now_ms: i64,
) -> Result<(FoldOutcome, Option<Cached>)> {
    let binds = |extra: Vec<(&'static str, Value)>| {
        let mut b: Vec<(&str, Value)> = vec![
            ("doc", json!(key.doc)),
            ("field", json!(key.field)),
            ("me", json!(me)),
        ];
        b.extend(extra);
        b
    };

    // 1. Lease.
    let claim = db
        .query(&sql::claim(), &binds(vec![("lease", json!(format!("{}s", cfg.lease_secs)))]))
        .await
        .context("claim")?;
    let Some(row) = claim.get(sql::CLAIM_RESULT).filter(|v| v.is_object()) else {
        return Ok((FoldOutcome { lost: true, ..Default::default() }, cached));
    };
    let rev = row.get("rev").and_then(Value::as_i64).unwrap_or(0);

    // 2. Document at `rev`, then the log rows.
    let doc = match cached {
        Some(c) if c.rev == rev => c.doc,
        _ => {
            let doc = LoroDoc::new();
            if let Some(b64) = row.get("snapshot").and_then(Value::as_str) {
                let bytes = B64.decode(b64).context("snapshot base64")?;
                doc.import(&bytes).map_err(|e| anyhow!("snapshot import: {e}"))?;
            }
            doc
        }
    };
    let rows = read_rows(db, cfg, key).await?;
    let more = rows.len() >= cfg.rows_per_fold;

    let before = doc.oplog_vv();
    let mut accepted: Vec<(&Row, &Accepted)> = Vec::new();
    let mut refused: Vec<String> = Vec::new();
    for row in &rows {
        let reason = match &row.verdict {
            Err(why) => why.as_str(),
            // A blob that decodes cleanly can still panic on import: try it on
            // a throwaway document before it touches the real one.
            Ok((bytes, acc)) => match acceptance::trial_import(bytes) {
                Ok(()) => {
                    accepted.push((row, acc));
                    continue;
                }
                Err(_) => "import_failed",
            },
        };
        warn!(doc = %key.doc, field = %key.field, row = %row.id, reason, "CRDT log row refused");
        refused.push(row.id.clone());
    }
    let mut imported = Vec::with_capacity(accepted.len());
    for (row, acc) in accepted {
        let Ok((bytes, _)) = &row.verdict else { continue };
        match acceptance::import_guarded(&doc, bytes) {
            Ok(()) => imported.push((row, acc)),
            Err(acceptance::ImportFailure::Error) => refused.push(row.id.clone()),
            Err(acceptance::ImportFailure::Panicked) => {
                // The document may be half-updated and its drop can abort:
                // leak it, refuse the row, and let the next fold start over
                // from the published snapshot.
                std::mem::forget(doc);
                delete_rows(db, cfg, &[row.id.clone()]).await?;
                db.query(&sql::release(), &binds(vec![])).await.context("release")?;
                bail!("CRDT log row {} panicked the document import; refused", row.id);
            }
        }
    }
    let accepted = imported;

    let mut folded = Vec::new();
    let mut kept = 0;
    for (row, acc) in &accepted {
        if acceptance::covered(&doc, acc) {
            folded.push(row.id.clone());
        } else if row.created_ms.is_some_and(|t| now_ms - t > STRANDED_AFTER_MS) {
            warn!(doc = %key.doc, row = %row.id, "CRDT log row stranded on missing predecessors");
            refused.push(row.id.clone());
        } else {
            kept += 1;
        }
    }

    // 3. Publish when the document changed.
    let changed = doc.oplog_vv() != before;
    let mut outcome = FoldOutcome { rev, more, kept, ..Default::default() };
    if changed {
        let snapshot = doc.export(ExportMode::Snapshot).map_err(|e| anyhow!("snapshot export: {e}"))?;
        let text = mirror::render(&doc, &key.field, kind);
        let upto = upto_json(&doc);
        let state_hash = hash::state_hash(&doc);
        let meta = json!({ "rev": rev + 1, "upto": upto, "state_hash": state_hash });
        let commit = db
            .query(
                &sql::commit(&key.field),
                &binds(vec![
                    ("rev", json!(rev)),
                    ("snapshot", json!(B64.encode(&snapshot))),
                    ("upto", upto),
                    ("hash", json!(state_hash)),
                    ("text", json!(text)),
                    ("meta", meta),
                ]),
            )
            .await;
        match commit {
            Ok(_) => {
                outcome.published = true;
                outcome.rev = rev + 1;
            }
            Err(DbError::Query(msg)) if msg.contains(sql::CAS_LOST) => {
                // Someone published `rev + 1` first: our document is stale.
                return Ok((FoldOutcome { lost: true, ..Default::default() }, None));
            }
            Err(e) => return Err(e).context("commit"),
        }
    } else {
        db.query(&sql::release(), &binds(vec![])).await.context("release")?;
    }

    // 4. Drop what the published document holds, and what can never apply.
    outcome.folded = delete_rows(db, cfg, &folded).await?;
    outcome.rejected = delete_rows(db, cfg, &refused).await?;
    debug!(doc = %key.doc, field = %key.field, ?outcome, "CRDT fold");
    Ok((outcome.clone(), Some(Cached { rev: outcome.rev, doc })))
}

async fn read_rows(db: &dyn Db, cfg: &FoldConfig, key: &DocKey) -> Result<Vec<Row>> {
    let binds = [
        ("log", json!(key.log)),
        ("doc", json!(key.doc)),
        ("field", json!(key.field)),
        ("limit", json!(cfg.rows_per_fold)),
    ];
    let res = db.query(sql::ROWS, &binds).await.context("read log rows")?;
    let Some(Value::Array(items)) = res.into_iter().next() else {
        bail!("log rows: unexpected result shape");
    };
    Ok(items
        .into_iter()
        .filter_map(|item| {
            let id = item.get("id")?.as_str()?.to_string();
            let peer = item.get("peer").and_then(Value::as_str).unwrap_or_default();
            let created_ms = item
                .get("created_at")
                .and_then(Value::as_str)
                .and_then(parse_rfc3339_ms);
            let verdict = item
                .get("data")
                .and_then(Value::as_str)
                .and_then(|b64| B64.decode(b64).ok())
                .ok_or(acceptance::Rejection::Empty)
                .and_then(|bytes| {
                    acceptance::check(&bytes, peer, cfg.max_delta_bytes).map(|acc| (bytes, acc))
                });
            Some(Row { id, verdict, created_ms })
        })
        .collect())
}

async fn delete_rows(db: &dyn Db, cfg: &FoldConfig, ids: &[String]) -> Result<usize> {
    for chunk in ids.chunks(cfg.delete_chunk.max(1)) {
        db.query(sql::DELETE_ROWS, &[("ids", json!(chunk))])
            .await
            .context("delete log rows")?;
    }
    Ok(ids.len())
}

/// `{ vv: { "<peer>": <end> }, frontiers: [{ peer: "<peer>", counter }] }`,
/// the shape `LoroDoc.forkAt` and `VersionVector` take in the browser.
fn upto_json(doc: &LoroDoc) -> Value {
    let vv: BTreeMap<String, i32> = doc.oplog_vv().iter().map(|(p, c)| (p.to_string(), *c)).collect();
    let frontiers: Vec<Value> = doc
        .oplog_frontiers()
        .iter()
        .map(|id| json!({ "peer": id.peer.to_string(), "counter": id.counter }))
        .collect();
    json!({ "vv": vv, "frontiers": frontiers })
}

/// Milliseconds since the epoch of an RFC 3339 datetime as SurrealDB renders
/// it (`2026-10-12T23:24:43.107650692Z`). Only the UTC `Z` form is produced.
fn parse_rfc3339_ms(s: &str) -> Option<i64> {
    let (date, time) = s.trim_end_matches('Z').split_once('T')?;
    let mut d = date.split('-').map(|p| p.parse::<i64>().ok());
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    let (hms, frac) = time.split_once('.').unwrap_or((time, "0"));
    let mut t = hms.split(':').map(|p| p.parse::<i64>().ok());
    let (h, min, sec) = (t.next()??, t.next()??, t.next()??);
    let millis = format!("{frac:0<3}")[..3].parse::<i64>().ok()?;
    // Days from civil (Howard Hinnant), proleptic Gregorian.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 24 + h) * 60 + min) * 60_000 + sec * 1000 + millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_surreal_datetimes() {
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339_ms("1970-01-02T00:00:01.5Z"), Some(86_401_500));
        assert_eq!(parse_rfc3339_ms("2026-10-12T23:24:43.107650692Z"), Some(1_791_847_483_107));
        assert_eq!(parse_rfc3339_ms("nonsense"), None);
    }
}
