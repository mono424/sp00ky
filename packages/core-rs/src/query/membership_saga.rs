//! Membership sagas: apply a server id-set, recover lost views, batched
//! `_00_list_ref` reads.
//!
//! PORT: `packages/spooky_core/lib/src/query/membership_saga.dart`.

use std::collections::{BTreeMap, BTreeSet};

use futures::future::join_all;
use serde_json::json;

use crate::error::Result;
use crate::kernel::constants::{
    backoff_ms_with, MATERIALIZING_REREAD_LADDER_MS, MEMBERSHIP_COALESCE_MS, MEMBERSHIP_LEADING_MS,
    VIEW_LOST_RETRY_BASE_MS, VIEW_LOST_RETRY_MAX_MS,
};
use crate::kernel::events::{LogLevel, OutEvent, RuntimeEvent};
use crate::kernel::ports::StatementResult;
use crate::kernel::Ctx;
use crate::query::env::{list_ref_table, SagaEnv};
use crate::query::membership::{
    decide_membership_outcome, snapshot_from_single, snapshots_from_batch, suspect_hashes, Answer,
    ListRefSnapshot, MembershipDecisionInput, MembershipOutcome,
};
use crate::query::sql;
use crate::state::lifecycle::{
    is_authoritative, LifecycleEvent, QueryLifecycle, QueryPhase, RemotePhase,
};
use crate::state::reducers as r;
use crate::state::ServerViewState;
use crate::sync::policy::{
    plan_list_ref_poll_chunks, record_version_arrays_equal, ListRefPollCandidate,
};
use crate::types::{QueryHash, RecordVersionArray, ServerViewMeta};
use crate::value::{Object, RecordId, Value};

fn server_view_state(meta: &ServerViewMeta) -> Option<ServerViewState> {
    if !meta.present {
        return None;
    }
    match meta.state.as_deref() {
        Some("materializing") => Some(ServerViewState::Materializing),
        Some("ready") => Some(ServerViewState::Ready),
        _ => None,
    }
}

fn server_view_state_name(state: Option<ServerViewState>) -> Option<&'static str> {
    state.map(|s| match s {
        ServerViewState::Materializing => "materializing",
        ServerViewState::Ready => "ready",
    })
}

/// What `apply_membership` reads off the entry before it decides.
struct Held {
    lifecycle: QueryLifecycle,
    held: usize,
    view_lost_count: u32,
    view_lost_retry_at: Option<i64>,
    view_key: String,
    children: RecordVersionArray,
}

/// Accept (or refuse) a server id-set for one query. The only writer of
/// `remote_array`, the `_00_view` row and the cold -> live transition.
pub async fn apply_membership(
    ctx: &Ctx,
    hash: &str,
    remote_array: RecordVersionArray,
    meta: Option<&ServerViewMeta>,
    verified_removal: bool,
) -> Result<MembershipOutcome> {
    let entry = ctx.read(|s| {
        s.queries.get(hash).map(|e| Held {
            lifecycle: e.lifecycle,
            held: e.remote_array.len(),
            view_lost_count: e.view_lost_count,
            view_lost_retry_at: e.view_lost_retry_at,
            view_key: e.def.view_key.clone(),
            children: e.subquery_remote_array.clone(),
        })
    })?;
    let Some(entry) = entry else {
        return Ok(MembershipOutcome::Ignored);
    };
    if let Some(meta) = meta {
        ctx.update(|s| r::set_server_state(s, hash, server_view_state(meta)))?;
        // The row is there: whatever made it read as gone has passed.
        if meta.present && (entry.view_lost_count > 0 || entry.view_lost_retry_at.is_some()) {
            ctx.update(|s| r::set_view_lost(s, hash, 0, None))?;
        }
    }
    let outcome = decide_membership_outcome(&MembershipDecisionInput {
        phase: entry.lifecycle.phase,
        held: entry.held,
        remote_array: &remote_array,
        meta,
        verified_removal,
    });
    match outcome {
        MembershipOutcome::Ignored => Ok(outcome),
        MembershipOutcome::ViewLost => {
            view_lost(ctx, hash, &entry).await?;
            Ok(outcome)
        }
        MembershipOutcome::Applied => {
            let was_authoritative = is_authoritative(&entry.lifecycle);
            let present = meta.is_none_or(|m| m.present);
            ctx.update(|s| r::commit_membership(s, hash, remote_array.clone(), present))?;
            if !was_authoritative {
                ctx.emit(OutEvent::QueryAuthority {
                    hash: hash.to_string(),
                    known: true,
                })?;
            }
            let now = ctx.now()?;
            // The children as they are now: another read for this query may
            // have committed newer ones since `entry` was read.
            let children = ctx
                .read(|s| s.queries.get(hash).map(|e| e.subquery_remote_array.clone()))?
                .unwrap_or(entry.children);
            write_view_row(ctx, hash, &entry.view_key, &remote_array, &children, now)?;
            ctx.dispatch(RuntimeEvent::FetchRows)?;
            Ok(outcome)
        }
    }
}

/// The server's `_00_query` row read as gone while we hold rows.
async fn view_lost(ctx: &Ctx, hash: &str, entry: &Held) -> Result<()> {
    if entry.lifecycle.phase != QueryPhase::ViewLost {
        ctx.update(|s| r::apply_lifecycle(s, hash, LifecycleEvent::RowMissing))?;
        ctx.emit(OutEvent::QueryViewLost {
            hash: hash.to_string(),
        })?;
    }
    // A recovery is already scheduled: every read in the meantime would
    // otherwise re-register again.
    if entry.view_lost_retry_at.is_some() {
        return Ok(());
    }
    if entry.view_lost_count == 0 {
        ctx.update(|s| {
            r::set_view_lost(s, hash, 1, None);
            if entry.lifecycle.remote != RemotePhase::Unregistered {
                r::apply_lifecycle(s, hash, LifecycleEvent::RemoteDropped);
            }
        })?;
        ctx.dispatch(RuntimeEvent::ensure_registered())?;
        return Ok(());
    }
    // Re-registered already and the row still reads as gone. Under load that
    // repeated every read, each one costing the server a registration while it
    // was still publishing the last, so from here on it is paced.
    let delay = backoff_ms_with(
        entry.view_lost_count - 1,
        VIEW_LOST_RETRY_BASE_MS,
        VIEW_LOST_RETRY_MAX_MS,
    );
    let now = ctx.now()?;
    let attempt = entry.view_lost_count + 1;
    ctx.update(|s| r::set_view_lost(s, hash, attempt, Some(now + delay as i64)))?;
    ctx.log(
        LogLevel::Debug,
        "view still lost; re-registering after a delay",
        Some(json!({"hash": hash, "attempt": attempt, "delayMs": delay}).into()),
    )?;
    ctx.timer_set(
        format!("view-lost:{hash}"),
        delay,
        RuntimeEvent::RecoverLostView {
            hash: hash.to_string(),
        },
    )
}

/// Rewrite the durable `_00_view` row. A failing write is logged: the row is a
/// cache of the server's answer, never a reason to fail the read.
fn write_view_row(
    ctx: &Ctx,
    hash: &str,
    view_key: &str,
    members: &RecordVersionArray,
    children: &RecordVersionArray,
    now: i64,
) -> Result<()> {
    let row = sql::view_row(members, children, true, now);
    if let Err(e) = ctx.local_put(
        sql::VIEW_TABLE,
        &sql::view_record_id(view_key),
        row,
        Default::default(),
        None,
    ) {
        ctx.log(
            LogLevel::Debug,
            "view row write failed",
            Some(json!({"hash": hash, "error": e.to_string()}).into()),
        )?;
    }
    Ok(())
}

/// The paced view-lost re-registration. A no-op when the row has been seen
/// since (the pacing was reset) or the query recovered or went away.
pub async fn recover_lost_view(ctx: &Ctx, hash: &str) -> Result<()> {
    let entry = ctx.read(|s| {
        s.queries
            .get(hash)
            .map(|e| (e.lifecycle, e.view_lost_count, e.view_lost_retry_at))
    })?;
    let Some((lifecycle, count, Some(_))) = entry else {
        return Ok(());
    };
    ctx.update(|s| r::set_view_lost(s, hash, count, None))?;
    if lifecycle.phase != QueryPhase::ViewLost {
        return Ok(());
    }
    if lifecycle.remote != RemotePhase::Unregistered {
        ctx.update(|s| r::apply_lifecycle(s, hash, LifecycleEvent::RemoteDropped))?;
    }
    ctx.dispatch(RuntimeEvent::RegisterRemote {
        hash: hash.to_string(),
        retry: false,
    })
}

/// Replace the subquery child set; bodies follow through the fetch plan. A
/// query with a `_00_view` row (past `cold`) records the children there too,
/// so the orphan collector keeps their bodies across restarts.
pub async fn apply_subquery_children(
    ctx: &Ctx,
    hash: &str,
    children: RecordVersionArray,
) -> Result<()> {
    let entry = ctx.read(|s| {
        s.queries.get(hash).map(|e| {
            (
                record_version_arrays_equal(&e.subquery_remote_array, &children),
                e.lifecycle.phase,
                e.def.view_key.clone(),
                e.remote_array.clone(),
            )
        })
    })?;
    let Some((equal, phase, view_key, members)) = entry else {
        return Ok(());
    };
    if equal {
        return Ok(());
    }
    ctx.update(|s| r::set_subquery_remote_array(s, hash, children.clone()))?;
    if phase != QueryPhase::Cold {
        // No merge upsert locally: rewrite the row from the members committed
        // now, which may be newer than the ones read above.
        let now = ctx.now()?;
        let members = ctx
            .read(|s| s.queries.get(hash).map(|e| e.remote_array.clone()))?
            .unwrap_or(members);
        write_view_row(ctx, hash, &view_key, &members, &children, now)?;
    }
    ctx.dispatch(RuntimeEvent::FetchRows)
}

/// The result of one membership read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MembershipRead {
    /// At least one server id-set was applied.
    pub changed: bool,
    /// At least one request or statement did not answer.
    pub failed: bool,
}

/// One query of a membership read.
struct ReadTarget {
    id: RecordId,
    held: usize,
    last_polled_at: i64,
}

fn single_snapshot(results: &[StatementResult]) -> Option<ListRefSnapshot> {
    snapshot_from_single(
        Answer::of(sql::stmt(results, 0)),
        Answer::of(sql::stmt(results, 1)),
        Answer::of(sql::stmt(results, 2)),
    )
}

/// Read the server membership of many queries in as few round trips as the
/// row budget allows, then apply each changed set. This is the ONLY place a
/// server id-set enters state: registration, the poll, LIVE dirt and view-lost
/// recovery all come through here.
pub async fn read_membership(
    ctx: &Ctx,
    env: &SagaEnv,
    hashes: &[QueryHash],
    force: bool,
) -> Result<MembershipRead> {
    let (targets, table) = ctx.read(|s| {
        let targets: Vec<(QueryHash, ReadTarget)> = hashes
            .iter()
            .filter_map(|h| {
                s.queries.get(h).map(|e| {
                    (
                        h.clone(),
                        ReadTarget {
                            id: e.def.id.clone(),
                            held: e.remote_array.len(),
                            last_polled_at: e.last_polled_at.unwrap_or(0),
                        },
                    )
                })
            })
            .collect();
        (targets, list_ref_table(env, s))
    })?;
    if targets.is_empty() {
        return Ok(MembershipRead::default());
    }
    let now = ctx.now()?;
    let candidates: Vec<ListRefPollCandidate> = targets
        .iter()
        .map(|(hash, t)| ListRefPollCandidate {
            hash: hash.clone(),
            rows: t.held,
            last_polled_at: if force { 0 } else { t.last_polled_at },
        })
        .collect();
    let chunks = plan_list_ref_poll_chunks(&candidates, now);
    let by_hash: BTreeMap<&str, &ReadTarget> =
        targets.iter().map(|(h, t)| (h.as_str(), t)).collect();

    let single_sql = sql::single_snapshot_select(&table);
    let batch_sql = sql::batch_snapshot_select(&table);
    let single = |hash: &str| {
        let mut vars = Object::new();
        vars.insert("in".into(), Value::RecordId(by_hash[hash].id.clone()));
        ctx.remote_query(&single_sql, Some(vars), Some(env.remote_timeout_ms))
    };
    let requests = chunks.iter().map(|chunk| {
        let vars = (chunk.len() > 1).then(|| {
            let mut vars = Object::new();
            vars.insert(
                "ins".into(),
                Value::Array(
                    chunk
                        .iter()
                        .map(|h| Value::RecordId(by_hash[h.as_str()].id.clone()))
                        .collect(),
                ),
            );
            vars
        });
        async {
            match vars {
                None => single(&chunk[0]).await,
                Some(vars) => {
                    ctx.remote_query(&batch_sql, Some(vars), Some(env.remote_timeout_ms))
                        .await
                }
            }
        }
    });
    let results = join_all(requests).await;

    let mut snapshots: Vec<(QueryHash, ListRefSnapshot)> = Vec::new();
    let mut failed = false;
    let mut rereads: Vec<QueryHash> = Vec::new();
    for (chunk, res) in chunks.iter().zip(results) {
        let Ok(statements) = res else {
            failed = true;
            continue;
        };
        if chunk.len() == 1 {
            match single_snapshot(&statements) {
                Some(snap) => snapshots.push((chunk[0].clone(), snap)),
                None => failed = true,
            }
            continue;
        }
        let edges = Answer::of(sql::stmt(&statements, 0));
        let counts = Answer::of(sql::stmt(&statements, 1));
        let edge_count = match edges {
            Answer::Value(Some(Value::Array(items))) => items.len(),
            _ => {
                failed = true;
                continue;
            }
        };
        if matches!(counts, Answer::Failed) {
            failed = true;
            continue;
        }
        let hash_by_id: BTreeMap<String, String> = chunk
            .iter()
            .map(|h| (by_hash[h.as_str()].id.to_string(), h.clone()))
            .collect();
        let batch = snapshots_from_batch(edges, counts, &hash_by_id);
        let held: BTreeMap<String, usize> = chunk
            .iter()
            .map(|h| (h.clone(), by_hash[h.as_str()].held))
            .collect();
        let suspect: BTreeSet<String> = suspect_hashes(&batch, &held, edge_count)
            .into_iter()
            .collect();
        for (hash, snap) in batch {
            if suspect.contains(&hash) {
                rereads.push(hash);
            } else {
                snapshots.push((hash, snap));
            }
        }
    }
    if !rereads.is_empty() {
        let again = join_all(rereads.iter().map(|h| single(h))).await;
        for (hash, res) in rereads.into_iter().zip(again) {
            match res.ok().as_deref().and_then(single_snapshot) {
                Some(snap) => snapshots.push((hash, snap)),
                None => failed = true,
            }
        }
    }

    let mut changed = false;
    let mut still_materializing: Vec<QueryHash> = Vec::new();
    for (hash, snap) in &snapshots {
        let current = ctx.read(|s| {
            s.queries.get(hash).map(|e| {
                (
                    server_view_state_name(e.server_state),
                    record_version_arrays_equal(&snap.primary, &e.remote_array),
                    e.lifecycle.phase,
                )
            })
        })?;
        let Some((current_state, equal, phase)) = current else {
            continue;
        };
        let server_state = if snap.meta.present {
            snap.meta.state.as_deref()
        } else {
            None
        };
        let meta_changed = current_state != server_state;
        if !equal || meta_changed || matches!(phase, QueryPhase::Cold | QueryPhase::Cached) {
            let outcome =
                apply_membership(ctx, hash, snap.primary.clone(), Some(&snap.meta), false).await?;
            if outcome == MembershipOutcome::Applied {
                changed = true;
            }
            if outcome == MembershipOutcome::Ignored && server_state == Some("materializing") {
                still_materializing.push(hash.clone());
            }
        }
        apply_subquery_children(ctx, hash, snap.subquery.clone()).await?;
    }
    let read: Vec<QueryHash> = snapshots.iter().map(|(h, _)| h.clone()).collect();
    ctx.update(|s| {
        r::clear_membership_dirty(s, &read);
        r::stamp_polled(s, &read, now);
    })?;

    // A view whose edges are still in flight: re-read on a short ladder rather
    // than waiting for a backed-off poll tick.
    for hash in &still_materializing {
        let attempt = ctx.read(|s| s.membership_reread.get(hash).copied().unwrap_or(0))?;
        let Some(&delay) = MATERIALIZING_REREAD_LADDER_MS.get(attempt as usize) else {
            ctx.update(|s| r::set_membership_reread(s, hash, None))?;
            continue;
        };
        ctx.update(|s| {
            r::set_membership_reread(s, hash, Some(attempt + 1));
            r::mark_membership_dirty(s, std::iter::once(hash));
        })?;
        ctx.timer_set("membership", delay, RuntimeEvent::ReadDirtyMembership)?;
    }
    ctx.update(|s| {
        for hash in read.iter().filter(|h| !still_materializing.contains(h)) {
            r::set_membership_reread(s, hash, None);
        }
    })?;
    ctx.dispatch(RuntimeEvent::SyncOutcome {
        ok: !failed,
        error: failed.then(|| "membership read failed".to_string()),
    })?;
    Ok(MembershipRead { changed, failed })
}

/// LIVE handler: mark dirty and coalesce into one batched re-read.
///
/// The window is armed ONCE per burst, when the dirty set goes from empty to
/// non-empty. A timer set replaces a pending timer, so re-arming on every
/// event would push the read out for as long as events keep arriving: on a
/// busy `_00_list_ref` table (registrations, an import, another client
/// syncing) the events never stop for a whole 50 ms and the read simply never
/// ran, leaving membership frozen until a poll tick happened to cover it.
///
/// Leading edge: when none of these queries was read within the last
/// [`MEMBERSHIP_COALESCE_MS`], the read goes out after the short
/// [`MEMBERSHIP_LEADING_MS`] instead of waiting the whole window. Dirt arriving
/// while that read is in flight still gets its own trailing read from
/// [`read_dirty_membership`], so the cap of one read per window holds.
pub async fn mark_membership_dirty(ctx: &Ctx, hashes: &[QueryHash]) -> Result<()> {
    let (armed, last_read) = ctx.read(|s| {
        let last = hashes
            .iter()
            .filter_map(|h| s.queries.get(h).and_then(|e| e.last_polled_at))
            .fold(0i64, i64::max);
        (!s.membership_dirty.is_empty(), last)
    })?;
    ctx.update(|s| r::mark_membership_dirty(s, hashes))?;
    if armed {
        return Ok(());
    }
    let now = ctx.now()?;
    let wait = last_read + MEMBERSHIP_COALESCE_MS as i64 - now;
    ctx.timer_set(
        "membership",
        wait.max(MEMBERSHIP_LEADING_MS as i64) as u64,
        RuntimeEvent::ReadDirtyMembership,
    )
}

pub async fn read_dirty_membership(ctx: &Ctx, env: &SagaEnv) -> Result<()> {
    let dirty: Vec<QueryHash> = ctx.read(|s| s.membership_dirty.iter().cloned().collect())?;
    if dirty.is_empty() {
        return Ok(());
    }
    read_membership(ctx, env, &dirty, true).await?;
    // Dirt that arrived while the read was in flight, or a hash the read could
    // not answer, holds the set non-empty, and with the window armed only on
    // the empty -> non-empty edge, nothing else would arm it again.
    if ctx.read(|s| !s.membership_dirty.is_empty())? {
        ctx.timer_set(
            "membership",
            MEMBERSHIP_COALESCE_MS,
            RuntimeEvent::ReadDirtyMembership,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
