//! Lifecycle tick (evict, heartbeat), ack pruning and orphan collection.
//!
//! PORT: `packages/spooky_core/lib/src/query/lifecycle_saga.dart`.

use std::collections::BTreeSet;

use serde_json::json;

use crate::error::Result;
use crate::kernel::constants::{
    ACK_GRACE_MS, GC_CHUNK, GC_INTERVAL_MS, TTL_HEARTBEAT_FRACTION, VIEW_RETENTION_MS,
};
use crate::kernel::events::{LogLevel, OutEvent, RuntimeEvent};
use crate::kernel::ports::IngestRecord;
use crate::kernel::Ctx;
use crate::query::env::SagaEnv;
use crate::query::membership::{parse_view_index_row, ViewIndexRow};
use crate::query::sql;
use crate::state::lifecycle::{LifecycleEvent, RemotePhase};
use crate::state::reducers as r;
use crate::state::selectors::{evictable, retained, shortest_ttl_ms};
use crate::state::OutboxStatus;
use crate::types::QueryHash;
use crate::value::{table_of, RecordId};

/// One tick for every query in state: evict the ones nobody has watched for a
/// ttl, heartbeat the rest in one request, notice reclaimed rows. Reschedules
/// itself at half the shortest ttl.
pub async fn lifecycle_tick(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let now = ctx.now()?;
    for hash in ctx.read(|s| evictable(s, now))? {
        evict_query(ctx, &hash).await?;
    }
    let remaining: Vec<(QueryHash, RecordId)> = ctx.read(|s| {
        s.queries
            .values()
            .filter(|e| e.lifecycle.remote == RemotePhase::Registered)
            .map(|e| (e.def.hash.clone(), e.def.id.clone()))
            .collect()
    })?;
    if !remaining.is_empty() {
        if let Err(error) = heartbeat(ctx, env, &remaining, now).await {
            ctx.dispatch(RuntimeEvent::SyncOutcome {
                ok: false,
                error: Some(error.to_string()),
            })?;
        }
    }
    let ttl = ctx.read(shortest_ttl_ms)?.unwrap_or(env.default_ttl_ms);
    ctx.timer_set(
        "lifecycle",
        (ttl as f64 * TTL_HEARTBEAT_FRACTION).floor() as u64,
        RuntimeEvent::LifecycleTick,
    )
}

async fn heartbeat(
    ctx: &Ctx,
    env: &SagaEnv,
    remaining: &[(QueryHash, RecordId)],
    now: i64,
) -> Result<()> {
    let ids: Vec<RecordId> = remaining.iter().map(|(_, id)| id.clone()).collect();
    let (beat, vars) = sql::heartbeat_batch(&ids);
    let results = ctx
        .remote_query(&beat, Some(vars), Some(env.remote_timeout_ms))
        .await?;
    let mut reclaimed = 0;
    for (i, (hash, _)) in remaining.iter().enumerate() {
        let gone = sql::stmt(&results, i)
            .is_some_and(|res| res.is_ok() && sql::heartbeat_row_gone(res.value()));
        if gone {
            reclaimed += 1;
            ctx.update(|s| r::apply_lifecycle(s, hash, LifecycleEvent::RemoteDropped))?;
        }
    }
    ctx.update(|s| r::stamp_heartbeat(s, remaining.iter().map(|(h, _)| h), now))?;
    if reclaimed > 0 {
        ctx.log(
            LogLevel::Warn,
            "query rows reclaimed by the server; re-registering",
            Some(json!({"reclaimed": reclaimed}).into()),
        )?;
        ctx.dispatch(RuntimeEvent::ensure_registered())?;
    }
    ctx.dispatch(RuntimeEvent::SyncOutcome {
        ok: true,
        error: None,
    })
}

/// Free a query's local view and forget it. The server row expires by TTL.
pub async fn evict_query(ctx: &Ctx, hash: &str) -> Result<()> {
    if !ctx.read(|s| s.queries.contains_key(hash))? {
        return Ok(());
    }
    if let Err(error) = ctx.ssp_unregister(hash) {
        ctx.log(
            LogLevel::Debug,
            "unregister failed",
            Some(json!({"hash": hash, "error": error.to_string()}).into()),
        )?;
    }
    ctx.update(|s| r::remove_query(s, hash))?;
    ctx.emit(OutEvent::QueryEvicted {
        hash: hash.to_string(),
    })
}

/// Drop acked outbox items membership never named within the grace window.
pub async fn ack_prune(ctx: &Ctx) -> Result<()> {
    let now = ctx.now()?;
    ctx.update(|s| r::outbox_prune_acked(s, now, ACK_GRACE_MS))?;
    if ctx.read(|s| s.outbox.iter().any(|i| i.status == OutboxStatus::Acked))? {
        ctx.timer_set("ack-prune", ACK_GRACE_MS, RuntimeEvent::AckPrune)?;
    }
    Ok(())
}

/// Orphan collection, `GC_BOOT_DELAY_MS` after boot and then every
/// [`GC_INTERVAL_MS`]. Retires the `_00_view` rows no query in state holds and
/// no server answer has rewritten for [`VIEW_RETENTION_MS`], then deletes the
/// bodies nothing retains (see [`retained`]) from the store and the circuit, a
/// chunk at a time. Each chunk is re-checked against fresh state (a query
/// mounted or a membership committed meanwhile keeps what it names), every
/// write is fenced to the store the sweep started in, and the sweep stops once
/// the bucket moved.
pub async fn gc_tick(ctx: &Ctx) -> Result<()> {
    ctx.wait(|s| s.primed).await?;
    let swept = sweep(ctx).await;
    let logged = match swept {
        Ok(()) => Ok(()),
        Err(error) => ctx.log(
            LogLevel::Warn,
            "orphan gc failed",
            Some(json!({"error": error.to_string()}).into()),
        ),
    };
    // finally
    let armed = ctx.timer_set("gc", GC_INTERVAL_MS, RuntimeEvent::GcTick);
    logged.and(armed)
}

fn read_views(ctx: &Ctx) -> Result<Vec<ViewIndexRow>> {
    Ok(ctx
        .local_get_all(sql::VIEW_TABLE)?
        .iter()
        .map(parse_view_index_row)
        .collect())
}

/// Where the sweep started; it stops once either moves.
struct SweepFence {
    bucket: Option<String>,
    epoch: u64,
}

impl SweepFence {
    fn moved(&self, ctx: &Ctx) -> Result<bool> {
        Ok(ctx.local_epoch()? != self.epoch || ctx.read(|s| s.bucket_id.clone())? != self.bucket)
    }
}

async fn sweep(ctx: &Ctx) -> Result<()> {
    let fence = SweepFence {
        bucket: ctx.read(|s| s.bucket_id.clone())?,
        epoch: ctx.local_epoch()?,
    };
    let rows = read_views(ctx)?;
    let now = ctx.now()?;
    let expired: Vec<String> = rows
        .iter()
        .filter(|row| now - row.updated_at >= VIEW_RETENTION_MS)
        .filter_map(|row| row.key.clone())
        .collect();
    let mut retired = 0;
    for slice in expired.chunks(GC_CHUNK) {
        let chunk = ctx.read(|s| {
            if s.bucket_id != fence.bucket {
                return None;
            }
            let held: BTreeSet<&str> = s
                .queries
                .values()
                .map(|e| e.def.view_key.as_str())
                .collect();
            Some(
                slice
                    .iter()
                    .filter(|key| !held.contains(key.as_str()))
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        })?;
        let Some(chunk) = chunk else { return Ok(()) };
        let settled: Vec<Result<()>> = chunk
            .iter()
            .map(|key| {
                ctx.local_delete(
                    sql::VIEW_TABLE,
                    &sql::view_record_id(key),
                    Some(fence.epoch),
                )
            })
            .collect();
        if fence.moved(ctx)? {
            return Ok(());
        }
        retired += settled.iter().filter(|r| r.is_ok()).count();
    }
    // Read again: a query answered meanwhile has rewritten its row.
    let rows = if retired > 0 { read_views(ctx)? } else { rows };
    let view_ids: BTreeSet<String> = rows.into_iter().flat_map(|row| row.ids).collect();
    if fence.moved(ctx)? {
        return Ok(());
    }
    let removed = collect_orphans(ctx, &fence, &view_ids)?;
    ctx.log(
        LogLevel::Info,
        "orphan gc done",
        Some(json!({"removed": removed, "retiredViews": retired}).into()),
    )
}

fn collect_orphans(ctx: &Ctx, fence: &SweepFence, view_ids: &BTreeSet<String>) -> Result<usize> {
    let candidates: Vec<String> = ctx.read(|s| {
        let keep = retained(s, view_ids);
        s.versions.keys().filter(|id| !keep(id)).cloned().collect()
    })?;
    let mut removed = 0;
    for slice in candidates.chunks(GC_CHUNK) {
        let chunk = ctx.read(|s| {
            if !s.primed || s.bucket_id != fence.bucket {
                return None;
            }
            let keep = retained(s, view_ids);
            Some(
                slice
                    .iter()
                    .filter(|id| s.versions.contains_key(*id) && !keep(id))
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        })?;
        let Some(chunk) = chunk else { break };
        let settled: Vec<bool> = chunk
            .iter()
            .map(|id| {
                ctx.local_delete(table_of(id), id, Some(fence.epoch))
                    .is_ok()
            })
            .collect();
        // A fenced delete reads as done; the circuit and `versions` now belong
        // to another bucket, so they must not hear about it.
        if fence.moved(ctx)? {
            break;
        }
        let done: Vec<String> = chunk
            .into_iter()
            .zip(settled)
            .filter(|(_, ok)| *ok)
            .map(|(id, _)| id)
            .collect();
        if done.is_empty() {
            continue;
        }
        ctx.update(|s| r::delete_versions(s, &done))?;
        ctx.ssp_ingest(done.iter().map(|id| IngestRecord::delete(id)).collect())?;
        removed += done.len();
    }
    // A query may have committed a deleted id between its chunk's check and
    // the delete; with its version gone, the fetch plan pulls it back.
    if removed > 0 {
        ctx.dispatch(RuntimeEvent::FetchRows)?;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests;
