//! The session's LIVE subscription on its `_00_list_ref` table.
//!
//! PORT: `packages/spooky_core/lib/src/sync/live_saga.dart`.
//!
//! An event marks its query's membership dirty and the batched re-read
//! decides. When the subscription was opened with the body joined on, the
//! event ALSO carries the changed row. That is an optimisation on top, never a
//! source of truth: the row is landed exactly as a fetched body would be, so a
//! notification the server drops costs nothing but the round trip it would
//! have saved.

use std::collections::BTreeMap;

use serde_json::json;

use crate::error::Result;
use crate::kernel::events::{LogLevel, RuntimeEvent};
use crate::kernel::Ctx;
use crate::query::env::{list_ref_table, SagaEnv};
use crate::query::fetch::land_chunk;
use crate::query::membership_saga::mark_membership_dirty;
use crate::state::reducers::{self as r, SyncPatch};
use crate::types::{ConnectionState, InlineRow, QueryHash};

fn clear_live() -> SyncPatch {
    SyncPatch {
        live_uuid: Some(None),
        live_table: Some(None),
        ..Default::default()
    }
}

/// Subscribe LIVE on the session's `_00_list_ref` table, replacing a
/// subscription on another table.
pub async fn live_start(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let (table, uuid, current, connection) = ctx.read(|s| {
        (
            list_ref_table(env, s),
            s.sync.live_uuid.clone(),
            s.sync.live_table.clone(),
            s.sync.health.connection,
        )
    })?;
    if uuid.is_some() && current.as_deref() == Some(table.as_str()) {
        return Ok(());
    }
    if let Some(uuid) = uuid.filter(|_| connection == ConnectionState::Connected) {
        if let Err(error) = ctx.remote_kill(&uuid).await {
            ctx.log(
                LogLevel::Debug,
                "kill of the previous live query failed",
                Some(json!({"error": error.to_string()}).into()),
            )?;
        }
    }
    ctx.update(|s| r::patch_sync(s, clear_live()))?;
    match ctx.remote_live(&table).await {
        Ok(uuid) => ctx.update(|s| {
            r::patch_sync(
                s,
                SyncPatch {
                    live_uuid: Some(Some(uuid)),
                    live_table: Some(Some(table)),
                    ..Default::default()
                },
            )
        }),
        Err(error) => ctx.log(
            LogLevel::Warn,
            "live subscription failed; the poll covers membership",
            Some(json!({"table": table, "error": error.to_string()}).into()),
        ),
    }
}

/// The socket dropped: the server-side live query is gone with it.
pub async fn live_invalidate(ctx: &Ctx) -> Result<()> {
    ctx.update(|s| r::patch_sync(s, clear_live()))
}

/// One or more edges of these queries changed on the server.
pub async fn live_change(
    ctx: &Ctx,
    env: &SagaEnv,
    hashes: Vec<QueryHash>,
    rows: Option<Vec<InlineRow>>,
) -> Result<()> {
    let (known, streak) = ctx.read(|s| {
        let known: Vec<QueryHash> = hashes
            .into_iter()
            .filter(|h| s.queries.contains_key(h))
            .collect();
        (known, s.sync.poll_idle_streak)
    })?;
    if known.is_empty() {
        return Ok(());
    }
    // Zeroing the streak only decides the delay the NEXT tick picks:
    // `poll_tick` arms its timer at the end of a tick, so a client that had
    // coasted up to the 5 s cap still waited out that armed timer. When the
    // poll is what catches a change (a notification the server dropped, a row
    // LIVE never carried) the worst case was 5 s even though LIVE had just
    // proved the client was active. Re-arm at the base cadence too, so the
    // safety net is back under us immediately.
    //
    // Only when the poll had actually backed off. A set on the same key
    // replaces the pending timer, so re-arming on every event while LIVE is
    // delivering faster than `poll_base_ms` would push the poll out forever
    // and starve the full-membership reconciliation it exists to provide.
    if streak > 0 {
        ctx.update(|s| {
            r::patch_sync(
                s,
                SyncPatch {
                    poll_idle_streak: Some(0),
                    ..Default::default()
                },
            )
        })?;
        ctx.timer_set("poll", env.poll_base_ms, RuntimeEvent::PollTick)?;
    }
    // Before marking membership dirty: recording the version here is what
    // makes the re-read's fetch plan find nothing left to pull for this row.
    if let Some(rows) = rows.filter(|r| !r.is_empty()) {
        land_inline_rows(ctx, env, rows).await?;
    }
    mark_membership_dirty(ctx, &known).await
}

/// Land bodies that rode in on the notification. Rows already held at this
/// version or newer are dropped: one edit bumps the edge in every view holding
/// the row, so the same body arrives once per view.
async fn land_inline_rows(ctx: &Ctx, env: &SagaEnv, rows: Vec<InlineRow>) -> Result<()> {
    let held: Vec<i64> = ctx.read(|s| {
        rows.iter()
            .map(|r| s.versions.get(&r.id).copied().unwrap_or(-1))
            .collect()
    })?;
    let mut order: Vec<String> = Vec::new();
    let mut fresh: BTreeMap<String, InlineRow> = BTreeMap::new();
    for (row, held) in rows.into_iter().zip(held) {
        if held >= row.version {
            continue;
        }
        match fresh.get(&row.id) {
            Some(prev) if prev.version >= row.version => {}
            Some(_) => {
                fresh.insert(row.id.clone(), row);
            }
            None => {
                order.push(row.id.clone());
                fresh.insert(row.id.clone(), row);
            }
        }
    }
    if fresh.is_empty() {
        return Ok(());
    }
    let picked: Vec<InlineRow> = order.iter().filter_map(|id| fresh.remove(id)).collect();
    let ids: Vec<String> = picked.iter().map(|r| r.id.clone()).collect();
    let versions: BTreeMap<String, i64> =
        picked.iter().map(|r| (r.id.clone(), r.version)).collect();
    let epoch = ctx.local_epoch()?;
    let records = picked.into_iter().map(|r| r.record).collect();
    if !land_chunk(ctx, env, &ids, records, &versions, epoch).await? {
        ctx.log(
            LogLevel::Debug,
            "inline live body not landed; the fetch path will pull it",
            Some(json!({"ids": ids}).into()),
        )?;
    }
    Ok(())
}
