//! Materialization: render a query's rows from the local store.
//!
//! PORT: `packages/spooky_core/lib/src/query/materialize.dart` and
//! `materialize_saga.dart`.
//!
//! Divergence from the TypeScript core, where the read is a SurrealQL string or
//! a rendered query plan and the local engine re-evaluates the projection and
//! ordering: sqlite cannot run SurrealQL, so the read is a batched id lookup
//! and the order the circuit (or the server) gave the id-set is kept, except
//! for a windowed query, whose `ORDER BY` is re-applied here because its ids
//! come from the server's `_00_list_ref` rather than the circuit.

use std::sync::Arc;

use serde_json::json;

use crate::error::Result;
use crate::kernel::events::{LogLevel, OutEvent};
use crate::kernel::ports::StreamUpdate;
use crate::kernel::Ctx;
use crate::modules::query_builder::RelationPlan;
use crate::query::relations::{resolve_relations, CtxRelationFetcher};
use crate::query::render::{build_render_ids, resolve_membership, RenderOptions};
use crate::query::window::build_window_materialization;
use crate::state::reducers as r;
use crate::state::selectors::{overlay, settled};
use crate::types::{QueryHash, TimingPhase};
use crate::utils::sort_rows;
use crate::value::{table_of, Row, Value};

pub fn is_windowed(surql: &str) -> bool {
    build_window_materialization(surql).is_some()
}

/// Re-apply a windowed query's own `ORDER BY` to rows resolved by id. No
/// window, or no `ORDER BY`: the rows keep their order.
pub fn apply_window_order(surql: &str, rows: &mut [Row]) {
    if let Some(window) = build_window_materialization(surql) {
        sort_rows(rows, &window.order_by);
    }
}

/// The table a render set belongs to. Ids in one view share a table, so the
/// first id decides.
pub fn table_of_ids<'a>(ids: &'a [String], fallback: &'a str) -> &'a str {
    ids.first().map(|id| table_of(id)).unwrap_or(fallback)
}

/// Structural equality for the "did the rows change" check. Rows that differ
/// only in wire types (a record id and its `table:id` string, a datetime and
/// its ISO string) are equal, as their JSON spelling is.
pub fn rows_equal(a: &[Row], b: &[Row]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if a == b {
        return true;
    }
    let json = |rows: &[Row]| -> Vec<serde_json::Value> {
        rows.iter()
            .map(|r| Value::Object(r.clone()).to_json())
            .collect()
    };
    json(a) == json(b)
}

/// What one materialization reads off the entry.
struct RenderJob {
    ids: Vec<String>,
    table: String,
    surql: String,
    is_window: bool,
    relations: Vec<RelationPlan>,
}

fn read_rows(ctx: &Ctx, job: &RenderJob) -> Result<Vec<Row>> {
    let mut rows = ctx.local_get_many(&job.table, &job.ids)?;
    if job.is_window {
        apply_window_order(&job.surql, &mut rows);
    }
    if !job.relations.is_empty() {
        // `.related()` projections are resolved from the local cache rather
        // than re-evaluated by the store: sqlite cannot run the correlated
        // subquery the surql carries. The rows are owned copies, so the
        // attachment never reaches the stored documents.
        resolve_relations(&mut rows, &job.relations, &CtxRelationFetcher(ctx))?;
    }
    Ok(rows)
}

/// Render one query from state: membership (or the circuit's local window),
/// the outbox overlay, one local read, then tell subscribers if the rows
/// changed. The only writer of `records`. Runs on the `mat:<hash>` lane
/// whenever the query is dirty.
pub async fn materialize(ctx: &Ctx, hash: &str) -> Result<()> {
    let job = ctx.read(|s| {
        let e = s.queries.get(hash)?;
        let ov = overlay(s);
        let is_window = is_windowed(&e.def.surql);
        let membership = resolve_membership(e.lifecycle.phase, &e.remote_array, &e.local_array);
        let ids = build_render_ids(
            membership,
            &e.local_array,
            &ov,
            RenderOptions {
                has_explicit_order: e.def.has_explicit_order,
                is_window,
            },
        );
        Some(RenderJob {
            table: table_of_ids(&ids, &e.def.table_name).to_string(),
            ids,
            surql: e.def.surql.clone(),
            is_window,
            relations: e.def.relations.clone(),
        })
    })?;
    let Some(job) = job else { return Ok(()) };
    let t0 = ctx.now()?;
    let rows = match read_rows(ctx, &job) {
        Ok(rows) => rows,
        Err(e) => {
            ctx.update(|s| {
                r::record_error(s, hash);
                r::clear_dirty(s, hash);
            })?;
            ctx.log(
                LogLevel::Warn,
                "materialize failed",
                Some(json!({"hash": hash, "error": e.to_string()}).into()),
            )?;
            return Ok(());
        }
    };
    let Some(previous) = ctx.read(|s| s.queries.get(hash).map(|e| e.records.clone()))? else {
        return Ok(());
    };
    let t1 = ctx.now()?;
    let changed = !rows_equal(&rows, &previous);
    let records = Arc::new(rows);
    ctx.update(|s| {
        r::set_records(s, hash, records.clone(), changed, Some((t1 - t0) as f64));
        if changed {
            r::stamp_updated(s, hash, t1);
        }
    })?;
    // An empty cold emission cannot establish "not found". Emit it again once
    // membership and bodies have settled, even when [] itself did not change:
    // bindings get a final empty result without the intermediate
    // membership-before-bodies state reading as an absence verdict.
    let settled_empty = records.is_empty() && ctx.read(|s| settled(s, hash))?;
    if changed || settled_empty {
        ctx.emit(OutEvent::QueryRecords {
            hash: hash.to_string(),
            records,
        })?;
    }
    Ok(())
}

/// A view update from the in-process circuit: the query's local id-set moved.
/// State takes the array (which dirties the query); the ingest timings feed
/// telemetry.
pub async fn stream_update(ctx: &Ctx, update: StreamUpdate) -> Result<()> {
    let hash: QueryHash = update.query_hash;
    if !ctx.read(|s| s.queries.contains_key(&hash))? {
        return Ok(());
    }
    let phases = [
        (TimingPhase::SspStoreApply, update.store_apply_ms),
        (TimingPhase::SspCircuitStep, update.circuit_step_ms),
        (TimingPhase::SspTransform, update.transform_ms),
    ];
    ctx.update(|s| {
        r::set_local_array(s, &hash, update.local_array);
        if let Some(ms) = update.materialization_time_ms {
            r::record_ingest(s, &hash, ms);
        }
        for (phase, ms) in phases {
            if let Some(ms) = ms {
                r::record_phase(s, &hash, phase, ms);
            }
        }
    })
}

#[cfg(test)]
mod tests;
