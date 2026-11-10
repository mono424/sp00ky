//! Pure sync policy: version-array diffs, the adaptive `_00_list_ref` poll
//! cadence and its chunking, and the health fold.
//!
//! PORT: `packages/spooky_core/lib/src/sync/policy.dart`.

use std::collections::{BTreeMap, HashMap};

use crate::kernel::constants as k;
use crate::types::{RecordVersion, RecordVersionArray, SyncHealth, SyncHealthStatus};
use crate::utils::classify_sync_error;
use crate::value::RecordId;

/// Default cadence for the `_00_list_ref` poll fallback (ms).
pub const DEFAULT_LIST_REF_POLL_INTERVAL_MS: u64 = 500;

/// Ceiling for the adaptive `_00_list_ref` poll backoff (ms). An idle client
/// coasts up to this cadence, so the worst-case catch-up latency for a missed
/// cross-session change stays at 5 s.
pub const LIST_REF_POLL_MAX_INTERVAL_MS: u64 = k::LIST_REF_POLL_MAX_MS;

/// What changed between two version arrays (TS `RecordVersionDiff`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordVersionDiff {
    pub added: Vec<(RecordId, i64)>,
    pub updated: Vec<(RecordId, i64)>,
    pub removed: Vec<RecordId>,
}

impl RecordVersionDiff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.updated.is_empty() && self.removed.is_empty()
    }
}

/// An id -> version map that keeps first-seen order (a Dart map literal: a
/// repeated id keeps its first position and takes the last version).
fn ordered_map(pairs: &[RecordVersion]) -> (Vec<String>, HashMap<&str, i64>) {
    let mut order = Vec::new();
    let mut map: HashMap<&str, i64> = HashMap::new();
    for (id, v) in pairs {
        if map.insert(id.as_str(), *v).is_none() {
            order.push(id.clone());
        }
    }
    (order, map)
}

/// Diff two version arrays into added/updated/removed (TS
/// `diffRecordVersionArray`).
pub fn diff_record_version_array(
    local: &[RecordVersion],
    remote: &[RecordVersion],
) -> RecordVersionDiff {
    let (_, local_map) = ordered_map(local);
    let (remote_order, remote_map) = ordered_map(remote);
    let (local_order, _) = ordered_map(local);
    let mut diff = RecordVersionDiff::default();
    for id in &remote_order {
        let remote_version = remote_map[id.as_str()];
        match local_map.get(id.as_str()) {
            None => diff.added.push((RecordId::parse(id), remote_version)),
            Some(lv) if *lv < remote_version => {
                diff.updated.push((RecordId::parse(id), remote_version))
            }
            Some(_) => {}
        }
    }
    for id in &local_order {
        if !remote_map.contains_key(id.as_str()) {
            diff.removed.push(RecordId::parse(id));
        }
    }
    diff
}

/// Incrementally maintains a local version array and diffs it against the
/// remote array (TS `ArraySyncer`).
#[derive(Clone, Debug)]
pub struct ArraySyncer {
    local: RecordVersionArray,
    remote: RecordVersionArray,
    needs_sort: bool,
}

impl ArraySyncer {
    pub fn new(local: &[RecordVersion], remote: &[RecordVersion]) -> Self {
        let mut local = local.to_vec();
        let mut remote = remote.to_vec();
        local.sort_by(|a, b| a.0.cmp(&b.0));
        remote.sort_by(|a, b| a.0.cmp(&b.0));
        ArraySyncer {
            local,
            remote,
            needs_sort: false,
        }
    }

    pub fn insert(&mut self, record_id: &str, version: i64) {
        self.local.push((record_id.to_string(), version));
        self.needs_sort = true;
    }

    pub fn update(&mut self, record_id: &str, version: i64) {
        for entry in self.local.iter_mut().filter(|(id, _)| id == record_id) {
            entry.1 = version;
            self.needs_sort = true;
        }
    }

    pub fn delete(&mut self, record_id: &str) {
        self.local.retain(|(id, _)| id != record_id);
    }

    pub fn next_set(&mut self) -> RecordVersionDiff {
        if self.needs_sort {
            self.local.sort_by(|a, b| a.0.cmp(&b.0));
            self.needs_sort = false;
        }
        diff_record_version_array(&self.local, &self.remote)
    }
}

/// Build a one-record diff from a LIVE `_00_list_ref` change (TS
/// `createDiffFromDbOp`). Empty when the cached version is already at least
/// `version`.
pub fn create_diff_from_db_op(
    op: &str,
    record_id: &RecordId,
    version: i64,
    versions: Option<&[RecordVersion]>,
) -> RecordVersionDiff {
    let encoded = record_id.to_string();
    let old = versions.and_then(|vs| vs.iter().find(|(id, _)| *id == encoded));
    if old.is_some_and(|(_, v)| *v >= version) {
        return RecordVersionDiff::default();
    }
    let mut diff = RecordVersionDiff::default();
    match op {
        "CREATE" => diff.added.push((record_id.clone(), version)),
        "UPDATE" => diff.updated.push((record_id.clone(), version)),
        _ => diff.removed.push(record_id.clone()),
    }
    diff
}

/// Apply a diff to a version array, returning a new array sorted by id.
pub fn apply_record_version_diff(
    current: &[RecordVersion],
    diff: &RecordVersionDiff,
) -> RecordVersionArray {
    let mut by_id: BTreeMap<String, i64> = current.iter().cloned().collect();
    for id in &diff.removed {
        by_id.remove(&id.to_string());
    }
    for (id, v) in diff.added.iter().chain(diff.updated.iter()) {
        by_id.insert(id.to_string(), *v);
    }
    by_id.into_iter().collect()
}

/// The effective poll interval; non-positive falls back to the default.
pub fn resolve_list_ref_poll_interval(opt: Option<i64>) -> u64 {
    match opt {
        Some(ms) if ms > 0 => ms as u64,
        _ => DEFAULT_LIST_REF_POLL_INTERVAL_MS,
    }
}

/// Adaptive poll delay (TS `listRefPollDelayMs`): stay at `base_ms` while
/// changes are arriving, back off exponentially toward `max_ms` while
/// `_00_list_ref` is quiet. `idle_streak` counts consecutive poll cycles that
/// saw no change; any activity (a poll-detected change or a LIVE event)
/// resets it, snapping the poll straight back to the base cadence.
///
/// Backing off on observed idleness rather than on LIVE liveness alone: the
/// cross-session LIVE-permission gap means LIVE frequently never fires, which
/// left a fully idle client polling every base interval forever.
pub fn list_ref_poll_delay_ms(idle_streak: u32, base_ms: u64, max_ms: u64) -> u64 {
    let cap = max_ms.max(base_ms);
    if idle_streak == 0 {
        return base_ms;
    }
    // Clamp the exponent so a long-idle client cannot overflow.
    let exponent = idle_streak.min(30);
    base_ms.saturating_mul(1u64 << exponent).min(cap)
}

/// Order-insensitive equality of two version arrays (TS
/// `recordVersionArraysEqual`). The `_00_list_ref` SELECT has no `ORDER BY`,
/// so row order can differ between polls with nothing changed; comparing as an
/// id -> version map avoids false "changed" verdicts that would defeat the idle
/// backoff.
pub fn record_version_arrays_equal(a: &[RecordVersion], b: &[RecordVersion]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let by_id: HashMap<&str, i64> = a.iter().map(|(id, v)| (id.as_str(), *v)).collect();
    b.iter().all(|(id, v)| by_id.get(id.as_str()) == Some(v))
}

// ---- poll chunking ----------------------------------------------------------------

/// One active query, as the poll planner sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListRefPollCandidate {
    pub hash: String,
    /// Edges the client currently holds for the query (`remote_array.len()`).
    pub rows: usize,
    /// When the query was last refreshed by the poll; `0` = never.
    pub last_polled_at: i64,
}

/// [`plan_list_ref_poll_chunks_with`] on the default budgets.
pub fn plan_list_ref_poll_chunks(
    candidates: &[ListRefPollCandidate],
    now: i64,
) -> Vec<Vec<String>> {
    plan_list_ref_poll_chunks_with(
        candidates,
        now,
        k::LIST_REF_ROW_BUDGET,
        k::LIST_REF_LARGE_VIEW_EDGES,
        k::LIST_REF_LARGE_VIEW_POLL_MS,
    )
}

/// Split the active queries into the round trips of one poll cycle.
///
/// Every query that is due is refreshed once per cycle, oldest refresh first,
/// packed greedily into chunks of at most `row_budget` known edges so one
/// response never carries a whole page's worth of ids at once. A view with
/// `large_view_rows` or more edges is only due once `large_view_min_age_ms`
/// has passed since its last refresh (LIVE remains its primary path); a chunk
/// always holds at least one query, so a single huge view still refreshes.
pub fn plan_list_ref_poll_chunks_with(
    candidates: &[ListRefPollCandidate],
    now: i64,
    row_budget: usize,
    large_view_rows: usize,
    large_view_min_age_ms: u64,
) -> Vec<Vec<String>> {
    let mut due: Vec<&ListRefPollCandidate> = candidates
        .iter()
        .filter(|c| {
            c.rows < large_view_rows || now - c.last_polled_at >= large_view_min_age_ms as i64
        })
        .collect();
    due.sort_by(|a, b| {
        a.last_polled_at
            .cmp(&b.last_polled_at)
            .then_with(|| a.hash.cmp(&b.hash))
    });
    let mut chunks = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut current_rows = 0usize;
    for c in due {
        let cost = c.rows.max(1);
        if !current.is_empty() && current_rows + cost > row_budget {
            chunks.push(std::mem::take(&mut current));
            current_rows = 0;
        }
        current.push(c.hash.clone());
        current_rows += cost;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

// ---- health -----------------------------------------------------------------------

/// What the health fold reads.
#[derive(Clone, Debug, PartialEq)]
pub struct HealthInput {
    pub health: SyncHealth,
    pub consecutive_failures: u32,
    pub has_synced_once: bool,
}

/// What the health fold produces.
#[derive(Clone, Debug, PartialEq)]
pub struct HealthOutput {
    pub health: SyncHealth,
    pub consecutive_failures: u32,
    pub has_synced_once: bool,
    /// Crossed into degraded on this outcome: start self-heal.
    pub degraded_now: bool,
    /// Left degraded on this outcome: stop self-heal.
    pub recovered_now: bool,
}

impl HealthOutput {
    fn unchanged(input: HealthInput) -> Self {
        HealthOutput {
            health: input.health,
            consecutive_failures: input.consecutive_failures,
            has_synced_once: input.has_synced_once,
            degraded_now: false,
            recovered_now: false,
        }
    }
}

/// Fold one sync round's outcome into health. A single failure is absorbed;
/// `degrade_after` consecutive ones flip to degraded, the next success flips
/// back. `degrade_after == 0` disables reporting.
pub fn next_health(
    input: HealthInput,
    ok: bool,
    error: Option<&str>,
    degrade_after: u32,
) -> HealthOutput {
    if degrade_after == 0 {
        return HealthOutput::unchanged(input);
    }
    if ok {
        if input.consecutive_failures == 0 {
            let mut health = input.health;
            health.ever_connected = true;
            return HealthOutput {
                health,
                consecutive_failures: input.consecutive_failures,
                has_synced_once: true,
                degraded_now: false,
                recovered_now: false,
            };
        }
        let recovered = input.health.status == SyncHealthStatus::Degraded;
        return HealthOutput {
            health: SyncHealth {
                status: SyncHealthStatus::Healthy,
                // Zeroed, unlike the browser core, which spreads the previous
                // snapshot and leaves a healthy report claiming three
                // consecutive failures. The snapshot is what an app renders; a
                // healthy one with a failure count is just wrong.
                consecutive_failures: 0,
                kind: None,
                error: None,
                ever_connected: true,
                connection: input.health.connection,
            },
            consecutive_failures: 0,
            has_synced_once: true,
            degraded_now: false,
            recovered_now: recovered,
        };
    }
    let consecutive_failures = input.consecutive_failures + 1;
    let kind = classify_sync_error(error.unwrap_or("null"));
    let message = error.unwrap_or("unknown error").to_string();
    let degraded_now =
        input.health.status != SyncHealthStatus::Degraded && consecutive_failures >= degrade_after;
    HealthOutput {
        health: SyncHealth {
            status: if degraded_now {
                SyncHealthStatus::Degraded
            } else {
                input.health.status
            },
            consecutive_failures,
            kind: Some(kind.as_str().to_string()),
            error: Some(message),
            ever_connected: input.health.ever_connected,
            connection: input.health.connection,
        },
        consecutive_failures,
        has_synced_once: input.has_synced_once,
        degraded_now,
        recovered_now: false,
    }
}

/// Self-heal backoff: `SELF_HEAL_BASE_MS * 2^attempt`, capped.
pub fn self_heal_delay_ms(attempt: u32) -> u64 {
    k::backoff_ms_with(attempt, k::SELF_HEAL_BASE_MS, k::SELF_HEAL_MAX_MS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ConnectionState;

    fn cand(hash: &str, rows: usize, last_polled_at: i64) -> ListRefPollCandidate {
        ListRefPollCandidate {
            hash: hash.into(),
            rows,
            last_polled_at,
        }
    }

    fn ids(chunks: &[&[&str]]) -> Vec<Vec<String>> {
        chunks
            .iter()
            .map(|c| c.iter().map(|h| h.to_string()).collect())
            .collect()
    }

    #[test]
    fn packs_due_queries_oldest_first_under_the_row_budget() {
        let chunks = plan_list_ref_poll_chunks_with(
            &[
                cand("a", 600, 300),
                cand("b", 600, 100),
                cand("c", 0, 0),
                cand("d", 900, 200),
            ],
            1000,
            1400,
            k::LIST_REF_LARGE_VIEW_EDGES,
            k::LIST_REF_LARGE_VIEW_POLL_MS,
        );
        // c (never polled) first, then b, d, a by age; d does not fit next to b.
        assert_eq!(chunks, ids(&[&["c", "b"], &["d"], &["a"]]));
    }

    #[test]
    fn a_lone_huge_view_always_refreshes() {
        let chunks =
            plan_list_ref_poll_chunks_with(&[cand("big", 10_000, 0)], 100_000, 100, 1000, 15_000);
        assert_eq!(chunks, ids(&[&["big"]]));
    }

    #[test]
    fn defers_large_views_until_their_minimum_age() {
        let large = cand("large", 5000, 990_000);
        let small = cand("small", 10, 990_000);
        let both = [large, small];
        assert_eq!(
            plan_list_ref_poll_chunks_with(&both, 1_000_000, k::LIST_REF_ROW_BUDGET, 1000, 15_000),
            ids(&[&["small"]])
        );
        // Same age: hash order breaks the tie, and the large view fills its own chunk.
        assert_eq!(
            plan_list_ref_poll_chunks_with(&both, 1_006_000, k::LIST_REF_ROW_BUDGET, 1000, 15_000),
            ids(&[&["large"], &["small"]])
        );
        assert!(plan_list_ref_poll_chunks(&[], 1).is_empty());
    }

    #[test]
    fn applies_diffs_sorted_by_id() {
        let current = vec![
            ("t:b".to_string(), 1),
            ("t:a".to_string(), 1),
            ("t:gone".to_string(), 1),
        ];
        let diff = RecordVersionDiff {
            added: vec![(RecordId::new("t", "c"), 1)],
            updated: vec![(RecordId::new("t", "a"), 4)],
            removed: vec![RecordId::new("t", "gone")],
        };
        assert_eq!(
            apply_record_version_diff(&current, &diff),
            vec![
                ("t:a".to_string(), 4),
                ("t:b".to_string(), 1),
                ("t:c".to_string(), 1)
            ]
        );
        assert_eq!(
            apply_record_version_diff(&current[..2], &RecordVersionDiff::default()),
            vec![("t:a".to_string(), 1), ("t:b".to_string(), 1)]
        );
    }

    #[test]
    fn diffs_and_db_ops() {
        let local = vec![
            ("t:1".to_string(), 1),
            ("t:2".to_string(), 5),
            ("t:3".to_string(), 1),
        ];
        let remote = vec![
            ("t:2".to_string(), 6),
            ("t:4".to_string(), 1),
            ("t:1".to_string(), 1),
        ];
        let d = diff_record_version_array(&local, &remote);
        assert_eq!(d.added, vec![(RecordId::new("t", "4"), 1)]);
        assert_eq!(d.updated, vec![(RecordId::new("t", "2"), 6)]);
        assert_eq!(d.removed, vec![RecordId::new("t", "3")]);

        let mut syncer = ArraySyncer::new(&local, &remote);
        syncer.update("t:2", 6);
        syncer.insert("t:4", 1);
        syncer.delete("t:3");
        assert!(syncer.next_set().is_empty());

        let id = RecordId::new("t", "1");
        let held = vec![("t:1".to_string(), 3)];
        assert!(create_diff_from_db_op("UPDATE", &id, 3, Some(&held)).is_empty());
        assert_eq!(
            create_diff_from_db_op("CREATE", &id, 4, Some(&held)).added,
            vec![(id.clone(), 4)]
        );
        assert_eq!(
            create_diff_from_db_op("UPDATE", &id, 4, None).updated,
            vec![(id.clone(), 4)]
        );
        assert_eq!(
            create_diff_from_db_op("DELETE", &id, 4, None).removed,
            vec![id]
        );
    }

    #[test]
    fn poll_delay_backs_off_and_caps() {
        assert_eq!(list_ref_poll_delay_ms(0, 500, 5000), 500);
        assert_eq!(list_ref_poll_delay_ms(1, 500, 5000), 1000);
        assert_eq!(list_ref_poll_delay_ms(3, 500, 5000), 4000);
        assert_eq!(list_ref_poll_delay_ms(4, 500, 5000), 5000);
        assert_eq!(list_ref_poll_delay_ms(400, 500, 5000), 5000);
        // A base above the cap wins.
        assert_eq!(list_ref_poll_delay_ms(2, 8000, 5000), 8000);
        assert_eq!(resolve_list_ref_poll_interval(None), 500);
        assert_eq!(resolve_list_ref_poll_interval(Some(-1)), 500);
        assert_eq!(resolve_list_ref_poll_interval(Some(750)), 750);
    }

    #[test]
    fn version_arrays_compare_order_insensitively() {
        let a = vec![("t:1".to_string(), 1), ("t:2".to_string(), 2)];
        let b = vec![("t:2".to_string(), 2), ("t:1".to_string(), 1)];
        assert!(record_version_arrays_equal(&a, &b));
        assert!(!record_version_arrays_equal(&a, &b[..1]));
        assert!(!record_version_arrays_equal(
            &a,
            &[("t:2".to_string(), 3), ("t:1".to_string(), 1)]
        ));
    }

    fn input(status: SyncHealthStatus, consecutive_failures: u32) -> HealthInput {
        HealthInput {
            health: SyncHealth {
                status,
                consecutive_failures,
                ..Default::default()
            },
            consecutive_failures,
            has_synced_once: false,
        }
    }

    #[test]
    fn disabled_health_reports_nothing() {
        let out = next_health(input(SyncHealthStatus::Healthy, 0), false, Some("boom"), 0);
        assert_eq!(out.health.status, SyncHealthStatus::Healthy);
        assert_eq!(out.consecutive_failures, 0);
        assert!(!out.degraded_now);
    }

    #[test]
    fn a_run_of_failures_degrades_and_the_next_success_recovers() {
        let out = next_health(
            input(SyncHealthStatus::Healthy, 0),
            false,
            Some("socket closed"),
            3,
        );
        assert_eq!(out.consecutive_failures, 1);
        assert_eq!(out.health.status, SyncHealthStatus::Healthy);
        assert_eq!(out.health.kind.as_deref(), Some("network"));
        assert!(!out.degraded_now);

        let out = next_health(
            input(SyncHealthStatus::Healthy, 1),
            false,
            Some("socket closed"),
            3,
        );
        assert!(!out.degraded_now);

        let out = next_health(
            input(SyncHealthStatus::Healthy, 2),
            false,
            Some("socket closed"),
            3,
        );
        assert_eq!(out.consecutive_failures, 3);
        assert_eq!(out.health.status, SyncHealthStatus::Degraded);
        assert!(out.degraded_now);

        let recovered = next_health(input(SyncHealthStatus::Degraded, 3), true, None, 3);
        assert_eq!(recovered.health.status, SyncHealthStatus::Healthy);
        assert_eq!(recovered.consecutive_failures, 0);
        assert!(recovered.recovered_now);
        assert!(recovered.has_synced_once);
        assert!(recovered.health.ever_connected);
        assert!(recovered.health.kind.is_none());
    }

    #[test]
    fn a_clean_success_latches_ever_connected() {
        let out = next_health(input(SyncHealthStatus::Healthy, 0), true, None, 3);
        assert!(out.has_synced_once);
        assert!(out.health.ever_connected);
        assert!(!out.recovered_now);
    }

    #[test]
    fn the_transport_state_survives_a_health_fold() {
        let out = next_health(
            HealthInput {
                health: SyncHealth {
                    ever_connected: true,
                    connection: ConnectionState::Connected,
                    ..Default::default()
                },
                consecutive_failures: 2,
                has_synced_once: true,
            },
            false,
            Some("boom"),
            3,
        );
        assert_eq!(out.health.connection, ConnectionState::Connected);
        assert_eq!(out.health.status, SyncHealthStatus::Degraded);
        assert_eq!(out.health.kind.as_deref(), Some("application"));
    }

    #[test]
    fn self_heal_doubles_and_caps() {
        assert_eq!(self_heal_delay_ms(0), k::SELF_HEAL_BASE_MS);
        assert_eq!(self_heal_delay_ms(1), k::SELF_HEAL_BASE_MS * 2);
        assert_eq!(self_heal_delay_ms(3), k::SELF_HEAL_BASE_MS * 8);
        assert_eq!(self_heal_delay_ms(50), k::SELF_HEAL_MAX_MS);
    }
}
