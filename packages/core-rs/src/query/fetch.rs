//! The unified row fetcher.
//!
//! PORT: `packages/spooky_core/lib/src/query/fetch_saga.dart`.

use std::collections::BTreeMap;

use futures::future::join_all;
use serde_json::json;

use crate::error::Result;
use crate::kernel::constants::backoff_ms;
use crate::kernel::events::{LogLevel, RuntimeEvent};
use crate::kernel::ports::{IngestOp, IngestRecord, LocalOp};
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;
use crate::query::sql;
use crate::state::lifecycle::LifecycleEvent;
use crate::state::reducers::{self as r, SyncPatch};
use crate::state::selectors::plan_fetch;
use crate::utils::{absent_optional_columns, clean_record};
use crate::value::{table_of, Object, RecordId, Row, Value};

/// The unified row fetcher. Runs on the serial `fetch` lane: computes the
/// cross-query set of bodies missing or stale locally, pulls them in parallel
/// chunks, writes and ingests them, and loops until the plan is empty. A row
/// shared by N queries is fetched once. Bodies are never deleted here; ids that
/// left membership simply stop rendering.
pub async fn fetch_rows(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    ctx.wait(|s| s.primed).await?;
    loop {
        let (plan, attempts) = ctx.read(|s| (plan_fetch(s), s.sync.fetch_attempts))?;
        if plan.chunks.is_empty() {
            if attempts != 0 {
                ctx.update(|s| {
                    r::patch_sync(
                        s,
                        SyncPatch {
                            fetch_attempts: Some(0),
                            ..Default::default()
                        },
                    )
                })?;
            }
            return Ok(());
        }
        // The store's epoch, not a mirrored counter: it also moves on a bucket
        // switch, and a stale value makes the store drop every body write.
        let epoch = ctx.local_epoch()?;
        ctx.update(|s| {
            for h in &plan.hashes {
                r::apply_lifecycle(s, h, LifecycleEvent::FetchBegin { background: false });
            }
        })?;
        let pulled = pull_chunks(ctx, env, &plan.chunks, &plan.versions, epoch).await;
        // finally
        ctx.update(|s| {
            for h in &plan.hashes {
                r::apply_lifecycle(s, h, LifecycleEvent::FetchEnd { background: false });
            }
        })?;
        let failed = pulled?;
        ctx.dispatch(RuntimeEvent::SyncOutcome {
            ok: failed == 0,
            error: (failed > 0).then(|| "body fetch failed".to_string()),
        })?;
        if failed > 0 {
            let attempt = ctx.read(|s| s.sync.fetch_attempts)?;
            ctx.update(|s| {
                r::patch_sync(
                    s,
                    SyncPatch {
                        fetch_attempts: Some(attempt + 1),
                        ..Default::default()
                    },
                )
            })?;
            return ctx.timer_set("fetch", backoff_ms(attempt), RuntimeEvent::FetchRows);
        }
    }
}

/// Pull every chunk in parallel and land what came back; the number of chunks
/// that failed.
async fn pull_chunks(
    ctx: &Ctx,
    env: &SagaEnv,
    chunks: &[Vec<String>],
    versions: &BTreeMap<String, i64>,
    epoch: u64,
) -> Result<usize> {
    let requests = chunks.iter().map(|ids| {
        let mut vars = Object::new();
        vars.insert(
            "ids".into(),
            Value::Array(
                ids.iter()
                    .map(|id| Value::RecordId(RecordId::parse(id)))
                    .collect(),
            ),
        );
        ctx.remote_query(sql::body_select(), Some(vars), Some(env.remote_timeout_ms))
    });
    let results = join_all(requests).await;
    let mut failed = 0;
    for (ids, res) in chunks.iter().zip(results) {
        let rows = match res
            .ok()
            .and_then(|stmts| stmts.into_iter().next())
            .and_then(|s| s.into_value())
        {
            Some(Value::Array(items)) => items.into_iter().filter_map(Value::into_object).collect(),
            _ => {
                failed += 1;
                continue;
            }
        };
        if !land_chunk(ctx, env, ids, rows, versions, epoch).await? {
            failed += 1;
        }
    }
    Ok(failed)
}

/// Write one chunk's bodies to the store and the circuit; record their
/// versions. False when the store write failed.
///
/// Shared with the LIVE path: a body that arrives on a notification is landed
/// through here so it is indistinguishable from a fetched one, which is what
/// makes `plan_fetch` stop asking for it.
pub async fn land_chunk(
    ctx: &Ctx,
    env: &SagaEnv,
    requested: &[String],
    rows: Vec<Row>,
    versions: &BTreeMap<String, i64>,
    epoch: u64,
) -> Result<bool> {
    let ids: Vec<String> = rows
        .iter()
        .filter_map(|row| {
            row.get("id")
                .filter(|v| !v.is_nullish())
                .map(Value::to_id_string)
        })
        .collect();
    let known: Vec<bool> =
        ctx.read(|s| ids.iter().map(|id| s.versions.contains_key(id)).collect())?;
    let mut ops = Vec::with_capacity(ids.len());
    let mut ingest = Vec::with_capacity(ids.len());
    let bodies = rows
        .into_iter()
        .filter(|row| row.get("id").is_some_and(|v| !v.is_nullish()));
    for ((row, id), known) in bodies.zip(ids).zip(known) {
        let table = table_of(&id).to_string();
        let version = versions.get(&id).copied().unwrap_or(0);
        let columns = env.columns(&table);
        let mut body = match columns {
            Some(c) if !c.is_empty() => clean_record(c, &row),
            _ => row.clone(),
        };
        body.insert("id".into(), Value::String(id.clone()));
        body.insert("_00_rv".into(), Value::Int(version));
        // The row is the server's whole body, so an optional column it lacks
        // was cleared upstream: the merge removes the local copy instead of
        // keeping it. Named on the op, not as nulls in `body`, which doubles
        // as the circuit record (CREATE/UPDATE replace the circuit's row).
        ops.push(LocalOp::merge(
            &table,
            &id,
            body.clone(),
            absent_optional_columns(columns, &row),
        ));
        let op = if known {
            IngestOp::Update
        } else {
            IngestOp::Create
        };
        ingest.push(IngestRecord::new(table, op, id, body));
    }
    if !ops.is_empty() {
        if let Err(e) = ctx.local_tx(ops, Some(epoch)) {
            ctx.log(
                LogLevel::Warn,
                "body write failed",
                Some(json!({"error": e.to_string()}).into()),
            )?;
            return Ok(false);
        }
        if let Err(e) = ctx.ssp_ingest(ingest) {
            ctx.log(
                LogLevel::Warn,
                "circuit ingest failed",
                Some(json!({"error": e.to_string()}).into()),
            )?;
        }
    }
    // Ids the server did not return (deleted or unreadable upstream) count as
    // known at the requested version, so the plan stops asking for them; their
    // absence from the store is what the render shows.
    let landed: Vec<(String, i64)> = requested
        .iter()
        .map(|id| (id.clone(), versions.get(id).copied().unwrap_or(0)))
        .collect();
    ctx.update(|s| r::set_versions(s, &landed))?;
    Ok(true)
}

#[cfg(test)]
mod tests;
