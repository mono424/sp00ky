//! Pure state updates. Every reducer is a plain function of `&mut ClientState`;
//! sagas apply them through `ctx.update(|s| ...)`, several in one closure where
//! the Dart core composes them. Reducers that change a query's render inputs
//! add it to `dirty` themselves, which is what drives materialization.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::kernel::constants::TELEMETRY_SAMPLE_WINDOW;
use crate::modules::query_builder::RelationPlan;
use crate::state::lifecycle::{transition, LifecycleEvent, QueryLifecycle};
use crate::state::{
    ClientState, OutboxItem, OutboxStatus, PeerWrite, PendingWrite, QueryEntry, ServerViewState, TabRole,
};
use crate::types::{ConnectionState, QueryHash, RecordVersionArray, RegistrationTimings, SyncHealth, TimingPhase};
use crate::value::{RecordId, Row};

fn with_entry(s: &mut ClientState, hash: &str, f: impl FnOnce(&mut QueryEntry)) -> bool {
    match s.queries.get_mut(hash) {
        Some(e) => {
            f(e);
            true
        }
        None => false,
    }
}

fn hashes_for_table(s: &ClientState, table: &str) -> Vec<QueryHash> {
    s.queries
        .iter()
        .filter(|(_, e)| e.def.table_name == table)
        .map(|(h, _)| h.clone())
        .collect()
}

fn capped(samples: &mut Vec<f64>, ms: f64) {
    samples.push(ms);
    if samples.len() > TELEMETRY_SAMPLE_WINDOW {
        let excess = samples.len() - TELEMETRY_SAMPLE_WINDOW;
        samples.drain(..excess);
    }
}

// ---- queries -------------------------------------------------------------------

pub fn put_query(s: &mut ClientState, entry: QueryEntry) {
    let hash = entry.def.hash.clone();
    s.queries.insert(hash.clone(), entry);
    s.dirty.insert(hash);
}

pub fn remove_query(s: &mut ClientState, hash: &str) {
    if s.queries.remove(hash).is_none() {
        return;
    }
    s.membership_reread.remove(hash);
    s.dirty.remove(hash);
    s.membership_dirty.remove(hash);
}

pub fn apply_lifecycle(s: &mut ClientState, hash: &str, ev: LifecycleEvent) {
    with_entry(s, hash, |e| e.lifecycle = transition(e.lifecycle, ev));
}

pub fn set_server_state(s: &mut ClientState, hash: &str, server_state: Option<ServerViewState>) {
    with_entry(s, hash, |e| e.server_state = server_state);
}

/// Accept a server membership set. Also releases every acked outbox item the
/// set names: membership has caught up with the write, so the overlay's job is
/// done.
pub fn commit_membership(s: &mut ClientState, hash: &str, remote_array: RecordVersionArray, present: bool) {
    let named: BTreeSet<String> = remote_array.iter().map(|(id, _)| id.clone()).collect();
    let found = with_entry(s, hash, |e| {
        e.remote_array = remote_array;
        e.lifecycle = transition(e.lifecycle, LifecycleEvent::MembershipApplied { present });
    });
    if !found {
        return;
    }
    s.outbox
        .retain(|i| !(i.status == OutboxStatus::Acked && named.contains(&i.record_id)));
    s.dirty.insert(hash.to_string());
}

pub fn set_local_array(s: &mut ClientState, hash: &str, local_array: RecordVersionArray) {
    if with_entry(s, hash, |e| e.local_array = local_array) {
        s.dirty.insert(hash.to_string());
    }
}

/// Give a query the `.related()` plan its first registrant did not carry (a
/// preload), and re-materialize it so the joins appear.
pub fn set_relations(s: &mut ClientState, hash: &str, relations: Vec<RelationPlan>) {
    if with_entry(s, hash, |e| e.def.relations = relations) {
        s.dirty.insert(hash.to_string());
    }
}

pub fn set_subquery_remote_array(s: &mut ClientState, hash: &str, array: RecordVersionArray) {
    with_entry(s, hash, |e| e.subquery_remote_array = array);
}

/// What a materialization produced. `changed` is false when the rows equal the
/// last ones (subscribers are not told again).
pub fn set_records(s: &mut ClientState, hash: &str, records: Arc<Vec<Row>>, changed: bool, materialize_ms: Option<f64>) {
    let found = with_entry(s, hash, |e| {
        if changed {
            e.telemetry.update_count += 1;
            e.records = records;
        }
        if let Some(ms) = materialize_ms {
            capped(e.telemetry.phase_samples.entry(TimingPhase::LocalFetch).or_default(), ms);
            e.telemetry.phase_last.insert(TimingPhase::LocalFetch, ms);
        }
        e.lifecycle = transition(e.lifecycle, LifecycleEvent::Notified);
    });
    if found {
        s.dirty.remove(hash);
    }
}

pub fn stamp_updated(s: &mut ClientState, hash: &str, now: i64) {
    with_entry(s, hash, |e| e.telemetry.last_updated_at = Some(now));
}

pub fn record_phase(s: &mut ClientState, hash: &str, phase: TimingPhase, ms: f64) {
    with_entry(s, hash, |e| {
        capped(e.telemetry.phase_samples.entry(phase).or_default(), ms);
        e.telemetry.phase_last.insert(phase, ms);
    });
}

/// Circuit ingest wall time for one update (the `ssp` phase).
pub fn record_ingest(s: &mut ClientState, hash: &str, ms: f64) {
    with_entry(s, hash, |e| {
        e.telemetry.last_ingest_latency_ms = Some(ms);
        capped(&mut e.telemetry.materialization_samples, ms);
    });
}

pub fn record_error(s: &mut ClientState, hash: &str) {
    with_entry(s, hash, |e| e.telemetry.error_count += 1);
}

pub fn set_registration_timings(s: &mut ClientState, hash: &str, timings: RegistrationTimings) {
    with_entry(s, hash, |e| e.telemetry.registration_timings = timings);
}

pub fn bump_register_attempts(s: &mut ClientState, hash: &str) {
    with_entry(s, hash, |e| e.register_attempts += 1);
}

/// View-lost recovery pacing; see `QueryEntry::view_lost_count`.
pub fn set_view_lost(s: &mut ClientState, hash: &str, count: u32, retry_at: Option<i64>) {
    with_entry(s, hash, |e| {
        e.view_lost_count = count;
        e.view_lost_retry_at = retry_at;
    });
}

pub fn reset_register_attempts(s: &mut ClientState, hash: &str) {
    with_entry(s, hash, |e| e.register_attempts = 0);
}

pub fn stamp_heartbeat<'a>(s: &mut ClientState, hashes: impl IntoIterator<Item = &'a QueryHash>, now: i64) {
    for hash in hashes {
        with_entry(s, hash, |e| e.last_heartbeat_at = Some(now));
    }
}

pub fn begin_registering(s: &mut ClientState, hash: &str) {
    s.registering.insert(hash.to_string());
}

pub fn end_registering(s: &mut ClientState, hash: &str) {
    s.registering.remove(hash);
}

pub fn set_membership_reread(s: &mut ClientState, hash: &str, attempt: Option<u32>) {
    match attempt {
        Some(a) => {
            s.membership_reread.insert(hash.to_string(), a);
        }
        None => {
            s.membership_reread.remove(hash);
        }
    }
}

pub fn stamp_polled<'a>(s: &mut ClientState, hashes: impl IntoIterator<Item = &'a QueryHash>, now: i64) {
    for hash in hashes {
        with_entry(s, hash, |e| e.last_polled_at = Some(now));
    }
}

// ---- subscribers ------------------------------------------------------------------

pub fn subscribe_query(s: &mut ClientState, hash: &str) {
    with_entry(s, hash, |e| {
        e.subscribers += 1;
        e.last_subscriber_left_at = None;
    });
}

pub fn unsubscribe_query(s: &mut ClientState, hash: &str, now: i64) {
    with_entry(s, hash, |e| {
        e.subscribers = e.subscribers.saturating_sub(1);
        if e.subscribers == 0 {
            e.last_subscriber_left_at = Some(now);
        }
    });
}

// ---- dirt ----------------------------------------------------------------------

pub fn mark_dirty<'a>(s: &mut ClientState, hashes: impl IntoIterator<Item = &'a QueryHash>) {
    for h in hashes {
        s.dirty.insert(h.clone());
    }
}

pub fn mark_table_dirty(s: &mut ClientState, table: &str) {
    let hashes = hashes_for_table(s, table);
    mark_dirty(s, &hashes);
}

pub fn clear_dirty(s: &mut ClientState, hash: &str) {
    s.dirty.remove(hash);
}

/// Only hashes still in state are marked.
pub fn mark_membership_dirty<'a>(s: &mut ClientState, hashes: impl IntoIterator<Item = &'a QueryHash>) {
    for h in hashes {
        if s.queries.contains_key(h) {
            s.membership_dirty.insert(h.clone());
        }
    }
}

pub fn clear_membership_dirty<'a>(s: &mut ClientState, hashes: impl IntoIterator<Item = &'a QueryHash>) {
    for h in hashes {
        s.membership_dirty.remove(h);
    }
}

// ---- versions -------------------------------------------------------------------

/// Record local body versions; dirties every query whose membership names one
/// of them.
pub fn set_versions(s: &mut ClientState, entries: &[(String, i64)]) {
    let mut changed = BTreeSet::new();
    for (id, v) in entries {
        if s.versions.get(id) != Some(v) {
            s.versions.insert(id.clone(), *v);
            changed.insert(id.as_str());
        }
    }
    if changed.is_empty() {
        return;
    }
    let dirty: Vec<QueryHash> = s
        .queries
        .iter()
        .filter(|(_, e)| {
            e.remote_array.iter().any(|(id, _)| changed.contains(id.as_str()))
                || e.local_array.iter().any(|(id, _)| changed.contains(id.as_str()))
        })
        .map(|(h, _)| h.clone())
        .collect();
    s.dirty.extend(dirty);
}

pub fn delete_versions<'a>(s: &mut ClientState, ids: impl IntoIterator<Item = &'a String>) {
    for id in ids {
        s.versions.remove(id);
    }
}

// ---- outbox ---------------------------------------------------------------------

pub fn outbox_replace(s: &mut ClientState, items: Vec<OutboxItem>) {
    let tables: BTreeSet<String> = s
        .outbox
        .iter()
        .chain(items.iter())
        .map(|i| i.table.clone())
        .collect();
    s.outbox = items;
    for t in tables {
        mark_table_dirty(s, &t);
    }
}

pub fn outbox_push(s: &mut ClientState, item: OutboxItem) {
    let table = item.table.clone();
    s.outbox.push(item);
    mark_table_dirty(s, &table);
}

pub fn outbox_ack(s: &mut ClientState, id: &str, now: i64) {
    if let Some(i) = s.outbox.iter_mut().find(|i| i.id == id) {
        i.status = OutboxStatus::Acked;
        i.acked_at = Some(now);
    }
}

pub fn outbox_remove(s: &mut ClientState, id: &str) {
    if let Some(idx) = s.outbox.iter().position(|i| i.id == id) {
        let item = s.outbox.remove(idx);
        mark_table_dirty(s, &item.table);
    }
}

pub fn outbox_bump_attempts(s: &mut ClientState, id: &str) {
    if let Some(i) = s.outbox.iter_mut().find(|i| i.id == id) {
        i.attempts += 1;
    }
}

/// Mark items whose last push may have committed although no answer came back.
pub fn outbox_mark_in_doubt<'a>(s: &mut ClientState, ids: impl IntoIterator<Item = &'a String>) {
    let marked: BTreeSet<&String> = ids.into_iter().collect();
    for i in s.outbox.iter_mut() {
        if marked.contains(&i.id) {
            i.in_doubt = true;
        }
    }
}

/// Drop acked items older than `grace_ms` (membership never named them).
pub fn outbox_prune_acked(s: &mut ClientState, now: i64, grace_ms: u64) {
    let mut tables = BTreeSet::new();
    s.outbox.retain(|i| {
        let stale = i.status == OutboxStatus::Acked
            && i.acked_at.is_some_and(|at| now - at >= grace_ms as i64);
        if stale {
            tables.insert(i.table.clone());
        }
        !stale
    });
    for t in tables {
        mark_table_dirty(s, &t);
    }
}

/// Merge a debounced update into its pending write (the first `before` wins).
pub fn merge_pending_write(s: &mut ClientState, write: PendingWrite) {
    match s.pending_writes.get_mut(&write.key) {
        Some(prev) => {
            for (k, v) in write.data {
                prev.data.insert(k, v);
            }
        }
        None => {
            s.pending_writes.insert(write.key.clone(), write);
        }
    }
}

pub fn clear_pending_write(s: &mut ClientState, key: &str) {
    s.pending_writes.remove(key);
}

/// A channel peer's create or delete is now showing; its table re-renders.
pub fn set_peer_write(s: &mut ClientState, id: &str, write: PeerWrite) {
    let table = write.table.clone();
    s.peer_writes.insert(id.to_string(), write);
    mark_table_dirty(s, &table);
}

pub fn clear_peer_write(s: &mut ClientState, id: &str) {
    if let Some(w) = s.peer_writes.remove(id) {
        mark_table_dirty(s, &w.table);
    }
}

pub fn set_failed_count(s: &mut ClientState, failed_count: usize) {
    s.failed_count = failed_count;
}

// ---- identity / connection ----------------------------------------------------------

/// Which identity fields to change. `None` leaves a field alone;
/// `Some(None)` clears it.
#[derive(Clone, Debug, Default)]
pub struct Identity {
    pub session_id: Option<Option<String>>,
    pub user_id: Option<Option<String>>,
    pub salt_user_id: Option<Option<String>>,
    pub owner_secret: Option<String>,
    pub pending_bucket: Option<Option<String>>,
    pub bucket_id: Option<Option<String>>,
    pub tab_role: Option<TabRole>,
    pub local_ready: Option<bool>,
    pub primed: Option<bool>,
}

pub fn set_identity(s: &mut ClientState, id: Identity) {
    if let Some(v) = id.session_id {
        s.session_id = v;
    }
    if let Some(v) = id.user_id {
        s.user_id = v;
    }
    if let Some(v) = id.salt_user_id {
        s.salt_user_id = v;
    }
    if let Some(v) = id.owner_secret {
        s.owner_secret = Some(v);
    }
    if let Some(v) = id.pending_bucket {
        s.pending_bucket = v;
    }
    if let Some(v) = id.bucket_id {
        s.bucket_id = v;
    }
    if let Some(v) = id.tab_role {
        s.tab_role = v;
    }
    if let Some(v) = id.local_ready {
        s.local_ready = v;
    }
    if let Some(v) = id.primed {
        s.primed = v;
    }
}

/// Re-home a query after a bucket switch: new store, same hash, fresh sync
/// state.
pub fn rebind_query(
    s: &mut ClientState,
    hash: &str,
    id: RecordId,
    lifecycle: QueryLifecycle,
    remote_array: RecordVersionArray,
    local_array: RecordVersionArray,
) {
    let found = with_entry(s, hash, |e| {
        e.def.id = id;
        e.lifecycle = lifecycle;
        e.remote_array = remote_array;
        e.local_array = local_array;
        e.subquery_remote_array = Vec::new();
        e.records = Arc::new(Vec::new());
        e.server_state = None;
        e.last_heartbeat_at = None;
        e.last_polled_at = None;
        e.register_attempts = 0;
        e.view_lost_count = 0;
        e.view_lost_retry_at = None;
    });
    if found {
        s.dirty.insert(hash.to_string());
    }
}

/// Everything a bucket switch invalidates in one go.
pub fn clear_bucket_state(s: &mut ClientState) {
    s.versions.clear();
    s.outbox.clear();
    s.pending_writes.clear();
    s.peer_writes.clear();
    s.dirty.clear();
    s.membership_dirty.clear();
    s.membership_reread.clear();
    s.primed = false;
}

pub fn set_tab_role(s: &mut ClientState, tab_role: TabRole) {
    s.tab_role = tab_role;
}

pub fn set_connection(s: &mut ClientState, connection: ConnectionState) {
    s.sync.health.connection = connection;
}

pub fn set_health(s: &mut ClientState, health: SyncHealth) {
    if s.sync.health != health {
        s.sync.health = health;
    }
}

/// Which sync fields to change. `None` leaves a field alone; for the optional
/// fields `Some(None)` clears it.
#[derive(Clone, Debug, Default)]
pub struct SyncPatch {
    pub consecutive_failures: Option<u32>,
    pub has_synced_once: Option<bool>,
    pub self_heal_attempts: Option<u32>,
    pub poll_idle_streak: Option<u32>,
    pub last_reconnect_refetch_at: Option<Option<i64>>,
    pub needs_resubscribe: Option<bool>,
    pub fetch_attempts: Option<u32>,
    pub live_uuid: Option<Option<String>>,
    pub live_table: Option<Option<String>>,
}

pub fn patch_sync(s: &mut ClientState, p: SyncPatch) {
    let y = &mut s.sync;
    if let Some(v) = p.consecutive_failures {
        y.consecutive_failures = v;
    }
    if let Some(v) = p.has_synced_once {
        y.has_synced_once = v;
    }
    if let Some(v) = p.self_heal_attempts {
        y.self_heal_attempts = v;
    }
    if let Some(v) = p.poll_idle_streak {
        y.poll_idle_streak = v;
    }
    if let Some(v) = p.last_reconnect_refetch_at {
        y.last_reconnect_refetch_at = v;
    }
    if let Some(v) = p.needs_resubscribe {
        y.needs_resubscribe = v;
    }
    if let Some(v) = p.fetch_attempts {
        y.fetch_attempts = v;
    }
    if let Some(v) = p.live_uuid {
        y.live_uuid = v;
    }
    if let Some(v) = p.live_table {
        y.live_table = v;
    }
}
