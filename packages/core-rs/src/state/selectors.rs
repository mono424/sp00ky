//! Pure reads over [`ClientState`]. Sagas use them through `ctx.read`.

use std::collections::{BTreeMap, BTreeSet};

use crate::kernel::constants::{FETCH_CHUNK, FETCH_ROUND_CHUNKS};
use crate::modules::query_builder::RelationPlan;
use crate::state::lifecycle::{derive_status, has_server_membership, is_foreground_fetching, QueryPhase, RemotePhase};
use crate::state::{ClientState, OutboxStatus, PeerWriteKind, QueryEntry};
use crate::types::{MutationEventType, PhaseStat, QueryHash, QueryStatus, QueryTimings, RecordVersionArray, TimingPhase};
use crate::value::{Row, Value};

pub fn active_hashes(s: &ClientState) -> Vec<QueryHash> {
    s.queries.keys().cloned().collect()
}

pub fn hashes_for_table(s: &ClientState, table: &str) -> Vec<QueryHash> {
    s.queries
        .iter()
        .filter(|(_, e)| e.def.table_name == table)
        .map(|(h, _)| h.clone())
        .collect()
}

pub fn query_status(s: &ClientState, hash: &str) -> Option<QueryStatus> {
    s.queries.get(hash).map(|e| derive_status(&e.lifecycle))
}

/// The optimistic overlay, derived from the outbox (pending AND acked items)
/// and the channel peers' unconfirmed creates and deletes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Overlay {
    /// Ids with an unsynced or just-acked create/update.
    pub writes: BTreeSet<String>,
    /// Ids with an unsynced or just-acked delete.
    pub deletes: BTreeSet<String>,
}

pub fn overlay(s: &ClientState) -> Overlay {
    let mut o = Overlay::default();
    for item in &s.outbox {
        if item.kind == MutationEventType::Delete {
            o.deletes.insert(item.record_id.clone());
        } else {
            o.writes.insert(item.record_id.clone());
        }
    }
    for (id, w) in &s.peer_writes {
        if w.kind == PeerWriteKind::Delete {
            o.deletes.insert(id.clone());
        } else {
            o.writes.insert(id.clone());
        }
    }
    o
}

pub fn pending_delete_ids(s: &ClientState) -> BTreeSet<&str> {
    s.outbox
        .iter()
        .filter(|i| i.kind == MutationEventType::Delete)
        .map(|i| i.record_id.as_str())
        .collect()
}

pub fn has_acked_writes(s: &ClientState) -> bool {
    s.outbox.iter().any(|i| i.status == OutboxStatus::Acked)
}

pub fn pending_mutation_count(s: &ClientState) -> usize {
    s.outbox.iter().filter(|i| i.status == OutboxStatus::Pending).count()
}

/// Record ids with a local write the server has not acknowledged yet: a queued
/// outbox item, or a debounced patch not flushed to the outbox.
pub fn unsynced_record_ids(s: &ClientState) -> BTreeSet<String> {
    s.outbox
        .iter()
        .filter(|i| i.status == OutboxStatus::Pending)
        .map(|i| i.record_id.clone())
        .chain(s.pending_writes.values().map(|w| w.record_id.clone()))
        .collect()
}

/// Queries with traffic the user is waiting on.
pub fn fetching_query_count(s: &ClientState) -> usize {
    s.queries
        .values()
        .filter(|e| is_foreground_fetching(&e.lifecycle))
        .count()
}

/// Membership entries whose body is missing or older locally.
pub fn needed(s: &ClientState, hash: &str) -> RecordVersionArray {
    let Some(e) = s.queries.get(hash) else {
        return Vec::new();
    };
    if !has_server_membership(&e.lifecycle) {
        return Vec::new();
    }
    let deletes = pending_delete_ids(s);
    e.remote_array
        .iter()
        .filter(|(id, v)| !deletes.contains(id.as_str()) && s.versions.get(id).copied().unwrap_or(-1) < *v)
        .cloned()
        .collect()
}

/// Subquery child bodies missing or stale locally (never part of [`settled`]).
pub fn needed_children(s: &ClientState, hash: &str) -> RecordVersionArray {
    let Some(e) = s.queries.get(hash) else {
        return Vec::new();
    };
    e.subquery_remote_array
        .iter()
        .filter(|(id, v)| s.versions.get(id).copied().unwrap_or(-1) < *v)
        .cloned()
        .collect()
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchPlan {
    /// Queries whose primary membership needs bodies (they flip to fetching).
    pub hashes: Vec<QueryHash>,
    pub chunks: Vec<Vec<String>>,
    /// Highest requested version per id, across every query naming it.
    pub versions: BTreeMap<String, i64>,
}

/// Cross-query, deduped, chunked list of ids to pull from the server: the
/// next round's worth, smallest need first, so one row that was just requested
/// does not wait for another query's thousands. The loop plans again after a
/// round.
pub fn plan_fetch(s: &ClientState) -> FetchPlan {
    plan_fetch_chunked(s, FETCH_CHUNK, FETCH_ROUND_CHUNKS)
}

pub fn plan_fetch_chunked(s: &ClientState, chunk_size: usize, max_chunks: usize) -> FetchPlan {
    let mut versions: BTreeMap<String, i64> = BTreeMap::new();
    let mut order: Vec<String> = Vec::new();
    let mut hashes = Vec::new();
    let mut add = |pairs: RecordVersionArray, versions: &mut BTreeMap<String, i64>| {
        for (id, v) in pairs {
            match versions.get_mut(&id) {
                Some(prev) => *prev = (*prev).max(v),
                None => {
                    order.push(id.clone());
                    versions.insert(id, v);
                }
            }
        }
    };
    let mut wants: Vec<(&QueryHash, RecordVersionArray, RecordVersionArray)> = s
        .queries
        .keys()
        .map(|hash| (hash, needed(s, hash), needed_children(s, hash)))
        .collect();
    // A stable sort: queries needing the same amount keep their registration
    // order.
    wants.sort_by_key(|(_, missing, children)| missing.len() + children.len());
    for (hash, missing, children) in wants {
        if !missing.is_empty() {
            hashes.push(hash.clone());
        }
        add(missing, &mut versions);
        add(children, &mut versions);
    }
    let chunks = order
        .chunks(chunk_size.max(1))
        .take(max_chunks)
        .map(<[String]>::to_vec)
        .collect();
    FetchPlan {
        hashes,
        chunks,
        versions,
    }
}

/// "This query's rows are authoritative and complete": server membership
/// accepted, every body present at its version, nothing waiting to be
/// re-rendered, subscribers told at least once.
pub fn settled(s: &ClientState, hash: &str) -> bool {
    let Some(e) = s.queries.get(hash) else {
        return false;
    };
    e.lifecycle.phase == QueryPhase::Live && needed(s, hash).is_empty() && !s.dirty.contains(hash) && e.lifecycle.notified
}

pub fn settle_failed(s: &ClientState, hash: &str) -> bool {
    s.queries
        .get(hash)
        .is_none_or(|e| e.lifecycle.remote == RemotePhase::Failed)
}

pub fn desired_registrations(s: &ClientState) -> Vec<QueryHash> {
    s.queries
        .iter()
        .filter(|(_, e)| e.lifecycle.remote == RemotePhase::Unregistered)
        .map(|(h, _)| h.clone())
        .collect()
}

/// The bodies the orphan collector must keep: internal rows, what a durable
/// `_00_view` row vouches for (`view_ids`), what any query in state holds as
/// members or children or renders as a joined row, and anything a write is
/// still in flight for.
pub fn retained(s: &ClientState, view_ids: &BTreeSet<String>) -> impl Fn(&str) -> bool {
    let mut held: BTreeSet<String> = BTreeSet::new();
    for e in s.queries.values() {
        held.extend(e.remote_array.iter().map(|(id, _)| id.clone()));
        held.extend(e.subquery_remote_array.iter().map(|(id, _)| id.clone()));
        joined_ids(&e.records, &e.def.relations, &mut held);
    }
    held.extend(s.outbox.iter().map(|i| i.record_id.clone()));
    held.extend(s.pending_writes.values().map(|w| w.record_id.clone()));
    let view_ids = view_ids.clone();
    move |id: &str| id.starts_with("_00_") || view_ids.contains(id) || held.contains(id)
}

/// The ids of the rows `relations` attached to `rows`, at every level.
fn joined_ids(rows: &[Row], relations: &[RelationPlan], out: &mut BTreeSet<String>) {
    for relation in relations {
        let mut children: Vec<Row> = Vec::new();
        for row in rows {
            match row.get(&relation.alias) {
                Some(Value::Object(child)) => children.push(child.clone()),
                Some(Value::Array(items)) => {
                    children.extend(items.iter().filter_map(|v| v.as_object().cloned()));
                }
                _ => {}
            }
        }
        for child in &children {
            if let Some(id) = child.get("id").filter(|v| !v.is_nullish()) {
                out.insert(id.to_id_string());
            }
        }
        joined_ids(&children, &relation.relations, out);
    }
}

pub fn evictable(s: &ClientState, now: i64) -> Vec<QueryHash> {
    s.queries
        .iter()
        .filter(|(_, e)| {
            e.subscribers == 0 && e.last_subscriber_left_at.is_some_and(|left| now - left >= e.def.ttl_ms as i64)
        })
        .map(|(h, _)| h.clone())
        .collect()
}

pub fn shortest_ttl_ms(s: &ClientState) -> Option<u64> {
    s.queries.values().map(|e| e.def.ttl_ms).min()
}

pub fn phase_stat_of(samples: &[f64], last_ms: Option<f64>) -> PhaseStat {
    if samples.is_empty() {
        return PhaseStat {
            last_ms,
            ..Default::default()
        };
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pick = |q: f64| sorted[((q * sorted.len() as f64).floor() as usize).min(sorted.len() - 1)];
    PhaseStat {
        last_ms,
        p50: Some(pick(0.5)),
        p90: Some(pick(0.9)),
        p99: Some(pick(0.99)),
        count: samples.len(),
    }
}

/// Per-query processing-time breakdown.
pub fn phase_timings(e: &QueryEntry) -> QueryTimings {
    let t = &e.telemetry;
    let stat = |p: TimingPhase| {
        phase_stat_of(
            t.phase_samples.get(&p).map(Vec::as_slice).unwrap_or(&[]),
            t.phase_last.get(&p).copied(),
        )
    };
    QueryTimings {
        ssp: phase_stat_of(&t.materialization_samples, t.last_ingest_latency_ms),
        ssp_store_apply: stat(TimingPhase::SspStoreApply),
        ssp_circuit_step: stat(TimingPhase::SspCircuitStep),
        ssp_transform: stat(TimingPhase::SspTransform),
        local_fetch: stat(TimingPhase::LocalFetch),
        remote_fetch: stat(TimingPhase::RemoteFetch),
        frontend: stat(TimingPhase::Frontend),
        registration: t.registration_timings,
        update_count: t.update_count,
        error_count: t.error_count,
    }
}
