//! PORT: `packages/spooky_core/test/state/reducers_test.dart`.
//!
//! The Dart reducers return a new state and are checked for identity on a
//! no-op; the Rust ones mutate in place, so a no-op is checked as "the state
//! equals what it was".

use std::sync::Arc;

use serde_json::json;
use spooky_core::state::lifecycle::{
    seed_lifecycle, LifecycleEvent, QueryLifecycle, QueryPhase, RemotePhase,
};
use spooky_core::state::reducers::{self as r, Identity, SyncPatch};
use spooky_core::state::{
    ClientState, OutboxStatus, PendingWrite, QueryEntry, ServerViewState, TabRole,
};
use spooky_core::testing::{build_definition, build_entry, build_outbox_item, row};
use spooky_core::types::{
    ConnectionState, MutationEventType, RegistrationTimings, SyncHealthStatus, TimingPhase,
};
use spooky_core::value::{RecordId, Row};

fn e(hash: &str) -> QueryEntry {
    e_on(hash, "thing")
}

fn e_on(hash: &str, table: &str) -> QueryEntry {
    build_entry(build_definition(hash, table))
}

/// Dart `buildState(entries)`: the entries, `dirty` cleared.
fn state(entries: Vec<QueryEntry>) -> ClientState {
    let mut s = ClientState::empty("tab-a");
    for entry in entries {
        r::put_query(&mut s, entry);
    }
    s.dirty.clear();
    s
}

fn acked(id: &str, record_id: &str, at: i64) -> spooky_core::state::OutboxItem {
    let mut i = build_outbox_item(id, MutationEventType::Create, record_id);
    i.status = OutboxStatus::Acked;
    i.acked_at = Some(at);
    i
}

fn no_op(s: &ClientState, f: impl FnOnce(&mut ClientState)) {
    let mut next = s.clone();
    f(&mut next);
    assert_eq!(&next, s);
}

#[test]
fn put_query_adds_and_dirties_remove_query_clears_dirt() {
    let mut s = ClientState::empty("t");
    r::put_query(&mut s, e("a"));
    assert!(s.queries.contains_key("a"));
    assert!(s.dirty.contains("a"));
    r::mark_membership_dirty(&mut s, &["a".to_string()]);
    r::remove_query(&mut s, "a");
    assert!(s.queries.is_empty());
    assert!(s.dirty.is_empty());
    assert!(s.membership_dirty.is_empty());
    no_op(&s, |s| r::remove_query(s, "zzz"));
}

#[test]
fn reducers_on_an_unknown_hash_change_nothing() {
    let s = state(vec![e("a")]);
    no_op(&s, |s| r::apply_lifecycle(s, "x", LifecycleEvent::Notified));
    no_op(&s, |s| r::set_local_array(s, "x", Vec::new()));
    no_op(&s, |s| {
        r::commit_membership(s, "x", vec![("thing:1".into(), 1)], true)
    });
    no_op(&s, |s| r::subscribe_query(s, "x"));
    no_op(&s, |s| {
        r::set_records(s, "x", Arc::new(Vec::new()), true, Some(1.0))
    });
}

#[test]
fn apply_lifecycle_and_set_server_state() {
    let mut s = state(vec![e("a")]);
    r::apply_lifecycle(&mut s, "a", LifecycleEvent::RemoteRegistering);
    assert_eq!(s.queries["a"].lifecycle.remote, RemotePhase::Registering);
    r::set_server_state(&mut s, "a", Some(ServerViewState::Ready));
    assert_eq!(s.queries["a"].server_state, Some(ServerViewState::Ready));
    no_op(&s, |s| {
        r::set_server_state(s, "a", Some(ServerViewState::Ready))
    });
}

#[test]
fn commit_membership_sets_the_array_flips_live_dirties_and_releases_acked_items_it_names() {
    let mut entry = e("a");
    entry.lifecycle = seed_lifecycle(true);
    let mut s = state(vec![entry]);
    r::outbox_replace(
        &mut s,
        vec![
            acked("m1", "thing:1", 1),
            acked("m2", "thing:2", 1),
            build_outbox_item("m3", MutationEventType::Create, "thing:1"),
        ],
    );
    s.dirty.clear();
    r::commit_membership(&mut s, "a", vec![("thing:1".into(), 2)], true);
    let entry = &s.queries["a"];
    assert_eq!(entry.remote_array, vec![("thing:1".to_string(), 2)]);
    assert_eq!(entry.lifecycle.phase, QueryPhase::Live);
    assert!(s.dirty.contains("a"));
    let ids: Vec<&str> = s.outbox.iter().map(|i| i.id.as_str()).collect();
    assert_eq!(ids, ["m2", "m3"]);
}

#[test]
fn set_local_array_dirties_set_subquery_remote_array_does_not() {
    let mut s = state(vec![e("a")]);
    r::set_local_array(&mut s, "a", vec![("thing:1".into(), 1)]);
    assert!(s.dirty.contains("a"));
    s.dirty.clear();
    r::set_subquery_remote_array(&mut s, "a", vec![("child:1".into(), 1)]);
    assert!(s.dirty.is_empty());
    assert_eq!(
        s.queries["a"].subquery_remote_array,
        vec![("child:1".to_string(), 1)]
    );
}

#[test]
fn set_records_clears_dirt_counts_changes_samples_timing_marks_notified() {
    let mut s = state(vec![e("a")]);
    r::mark_dirty(&mut s, &["a".to_string()]);
    let rows: Arc<Vec<Row>> = Arc::new(vec![row(json!({"id": "thing:1"}))]);
    r::set_records(&mut s, "a", rows.clone(), true, Some(12.0));
    let en = &s.queries["a"];
    assert!(Arc::ptr_eq(&en.records, &rows));
    assert_eq!(en.telemetry.update_count, 1);
    assert_eq!(
        en.telemetry.phase_samples[&TimingPhase::LocalFetch],
        vec![12.0]
    );
    assert_eq!(en.telemetry.phase_last[&TimingPhase::LocalFetch], 12.0);
    assert!(en.lifecycle.notified);
    assert!(!s.dirty.contains("a"));

    r::set_records(
        &mut s,
        "a",
        Arc::new(vec![row(json!({"id": "other"}))]),
        false,
        None,
    );
    let en = &s.queries["a"];
    assert!(Arc::ptr_eq(&en.records, &rows));
    assert_eq!(en.telemetry.update_count, 1);
    assert_eq!(
        en.telemetry.phase_samples[&TimingPhase::LocalFetch],
        vec![12.0]
    );
}

#[test]
fn sample_windows_are_capped() {
    let mut s = state(vec![e("a")]);
    for i in 0..105 {
        r::record_phase(&mut s, "a", TimingPhase::LocalFetch, f64::from(i));
    }
    let t = &s.queries["a"].telemetry;
    assert_eq!(t.phase_samples[&TimingPhase::LocalFetch].len(), 100);
    assert_eq!(t.phase_last[&TimingPhase::LocalFetch], 104.0);
    for i in 0..105 {
        r::set_records(&mut s, "a", Arc::new(Vec::new()), false, Some(f64::from(i)));
    }
    assert_eq!(
        s.queries["a"].telemetry.phase_samples[&TimingPhase::LocalFetch].len(),
        100
    );
    for i in 0..105 {
        r::record_ingest(&mut s, "a", f64::from(i));
    }
    assert_eq!(s.queries["a"].telemetry.materialization_samples.len(), 100);
    assert_eq!(s.queries["a"].telemetry.last_ingest_latency_ms, Some(104.0));
}

#[test]
fn telemetry_helpers() {
    let mut s = state(vec![e("a")]);
    r::stamp_updated(&mut s, "a", 5);
    r::record_error(&mut s, "a");
    r::set_registration_timings(
        &mut s,
        "a",
        RegistrationTimings {
            parse_ms: Some(1.0),
            plan_ms: Some(2.0),
            snapshot_ms: Some(3.0),
            wall_ms: Some(4.0),
        },
    );
    r::bump_register_attempts(&mut s, "a");
    r::bump_register_attempts(&mut s, "a");
    r::stamp_heartbeat(&mut s, &["a".to_string(), "missing".to_string()], 9);
    r::stamp_polled(&mut s, &["a".to_string()], 11);
    let en = &s.queries["a"];
    assert_eq!(en.telemetry.last_updated_at, Some(5));
    assert_eq!(en.telemetry.error_count, 1);
    assert_eq!(en.telemetry.registration_timings.wall_ms, Some(4.0));
    assert_eq!(en.register_attempts, 2);
    assert_eq!(en.last_heartbeat_at, Some(9));
    assert_eq!(en.last_polled_at, Some(11));
    r::reset_register_attempts(&mut s, "a");
    assert_eq!(s.queries["a"].register_attempts, 0);
    no_op(&s, |s| r::reset_register_attempts(s, "a"));
}

#[test]
fn subscribe_and_unsubscribe_track_the_eviction_clock() {
    let mut s = state(vec![e("a")]);
    r::subscribe_query(&mut s, "a");
    r::subscribe_query(&mut s, "a");
    assert_eq!(s.queries["a"].subscribers, 2);
    r::unsubscribe_query(&mut s, "a", 100);
    assert_eq!(s.queries["a"].last_subscriber_left_at, None);
    r::unsubscribe_query(&mut s, "a", 200);
    assert_eq!(s.queries["a"].last_subscriber_left_at, Some(200));
    r::unsubscribe_query(&mut s, "a", 300);
    assert_eq!(s.queries["a"].subscribers, 0);
    r::subscribe_query(&mut s, "a");
    assert_eq!(s.queries["a"].last_subscriber_left_at, None);
}

#[test]
fn tracks_in_flight_registrations_and_reread_attempts() {
    let mut s = state(vec![e("a")]);
    r::begin_registering(&mut s, "x");
    assert!(s.registering.contains("x"));
    r::end_registering(&mut s, "x");
    assert!(s.registering.is_empty());
    no_op(&s, |s| r::end_registering(s, "x"));
    r::set_membership_reread(&mut s, "a", Some(1));
    assert_eq!(s.membership_reread.get("a"), Some(&1));
    no_op(&s, |s| r::set_membership_reread(s, "zzz", None));
    let mut removed = s.clone();
    r::remove_query(&mut removed, "a");
    assert!(removed.membership_reread.is_empty());
    r::set_membership_reread(&mut s, "a", None);
    assert!(s.membership_reread.is_empty());
}

#[test]
fn mark_dirty_clear_dirty_mark_table_dirty() {
    let s0 = state(vec![e_on("a", "t1"), e_on("b", "t2")]);
    no_op(&s0, |s| r::mark_dirty(s, &[] as &[String]));
    let mut s = s0.clone();
    r::mark_table_dirty(&mut s, "t1");
    assert_eq!(s.dirty.iter().collect::<Vec<_>>(), ["a"]);
    r::clear_dirty(&mut s, "a");
    assert!(s.dirty.is_empty());
    no_op(&s, |s| r::clear_dirty(s, "a"));
}

#[test]
fn membership_dirt_only_for_known_hashes() {
    let s0 = state(vec![e("a")]);
    no_op(&s0, |s| r::mark_membership_dirty(s, &["nope".to_string()]));
    let mut s = s0.clone();
    r::mark_membership_dirty(&mut s, &["a".to_string(), "nope".to_string()]);
    assert_eq!(s.membership_dirty.iter().collect::<Vec<_>>(), ["a"]);
    r::clear_membership_dirty(&mut s, &["a".to_string()]);
    assert!(s.membership_dirty.is_empty());
}

#[test]
fn set_versions_dirties_queries_naming_the_id_delete_versions_removes() {
    let mut a = e("a");
    a.remote_array = vec![("thing:1".into(), 1)];
    let mut b = e("b");
    b.local_array = vec![("thing:2".into(), 1)];
    let s0 = state(vec![a, b, e("c")]);
    no_op(&s0, |s| r::set_versions(s, &[]));
    let mut s = s0.clone();
    r::set_versions(
        &mut s,
        &[
            ("thing:1".into(), 1),
            ("thing:2".into(), 1),
            ("thing:9".into(), 1),
        ],
    );
    assert_eq!(s.dirty.iter().collect::<Vec<_>>(), ["a", "b"]);
    no_op(&s, |s| r::set_versions(s, &[("thing:1".into(), 1)]));
    r::delete_versions(&mut s, &["thing:9".to_string(), "nope".to_string()]);
    assert!(!s.versions.contains_key("thing:9"));
    no_op(&s, |s| r::delete_versions(s, &["nope".to_string()]));
}

#[test]
fn outbox_push_ack_bump_remove_replace_dirty_the_table() {
    let mut s = state(vec![e("a")]);
    r::outbox_push(
        &mut s,
        build_outbox_item("m1", MutationEventType::Create, "thing:1"),
    );
    assert!(s.dirty.contains("a"));
    s.dirty.clear();
    r::outbox_ack(&mut s, "m1", 50);
    assert_eq!(s.outbox[0].status, OutboxStatus::Acked);
    assert_eq!(s.outbox[0].acked_at, Some(50));
    assert!(s.dirty.is_empty());
    no_op(&s, |s| r::outbox_ack(s, "nope", 1));
    r::outbox_bump_attempts(&mut s, "m1");
    assert_eq!(s.outbox[0].attempts, 1);
    no_op(&s, |s| r::outbox_bump_attempts(s, "nope"));
    let mut doubt = s.clone();
    r::outbox_mark_in_doubt(&mut doubt, &["m1".to_string(), "nope".to_string()]);
    assert!(doubt.outbox[0].in_doubt);
    assert_eq!(doubt.outbox[0].attempts, 1);
    no_op(&doubt, |s| r::outbox_mark_in_doubt(s, &["m1".to_string()]));
    r::outbox_remove(&mut s, "m1");
    assert!(s.outbox.is_empty());
    assert!(s.dirty.contains("a"));
    no_op(&s, |s| r::outbox_remove(s, "m1"));
    s.dirty.clear();
    r::outbox_replace(
        &mut s,
        vec![build_outbox_item("x", MutationEventType::Create, "thing:1")],
    );
    assert_eq!(s.outbox.len(), 1);
    assert!(s.dirty.contains("a"));
}

#[test]
fn outbox_prune_acked_drops_only_expired_acked_items() {
    let mut s = state(vec![e("a")]);
    r::outbox_replace(
        &mut s,
        vec![
            acked("old", "thing:1", 0),
            acked("fresh", "thing:1", 90),
            build_outbox_item("pending", MutationEventType::Create, "thing:1"),
        ],
    );
    s.dirty.clear();
    r::outbox_prune_acked(&mut s, 100, 50);
    let ids: Vec<&str> = s.outbox.iter().map(|i| i.id.as_str()).collect();
    assert_eq!(ids, ["fresh", "pending"]);
    assert!(s.dirty.contains("a"));
    no_op(&s, |s| r::outbox_prune_acked(s, 100, 50));
}

#[test]
fn pending_writes_merge_per_key_and_clear() {
    let mut s = state(Vec::new());
    r::merge_pending_write(
        &mut s,
        PendingWrite {
            key: "k".into(),
            table: "thing".into(),
            record_id: "thing:1".into(),
            data: row(json!({"a": 1})),
            before: Some(row(json!({"a": 0}))),
            first_at: 1,
        },
    );
    r::merge_pending_write(
        &mut s,
        PendingWrite {
            key: "k".into(),
            table: "thing".into(),
            record_id: "thing:1".into(),
            data: row(json!({"b": 2})),
            before: None,
            first_at: 9,
        },
    );
    let merged = &s.pending_writes["k"];
    assert_eq!(merged.data, row(json!({"a": 1, "b": 2})));
    assert_eq!(merged.before, Some(row(json!({"a": 0}))));
    assert_eq!(merged.first_at, 1);
    r::clear_pending_write(&mut s, "k");
    assert!(s.pending_writes.is_empty());
    no_op(&s, |s| r::clear_pending_write(s, "k"));
}

#[test]
fn set_failed_count() {
    let mut s = state(Vec::new());
    r::set_failed_count(&mut s, 2);
    assert_eq!(s.failed_count, 2);
    no_op(&s, |s| r::set_failed_count(s, 2));
}

#[test]
fn rebind_query_swaps_the_id_and_sync_state_clear_bucket_state_wipes_slices() {
    let mut entry = e("a");
    entry.lifecycle = QueryLifecycle {
        phase: QueryPhase::Live,
        remote: RemotePhase::Registered,
        fetch_depth: 0,
        background_depth: 0,
        notified: true,
    };
    entry.remote_array = vec![("t:1".into(), 1)];
    entry.records = Arc::new(vec![row(json!({"id": "t:1"}))]);
    entry.server_state = Some(ServerViewState::Ready);
    entry.register_attempts = 2;
    let mut s = state(vec![entry]);
    r::set_versions(&mut s, &[("t:1".into(), 1)]);
    r::outbox_replace(
        &mut s,
        vec![build_outbox_item(
            "m1",
            MutationEventType::Create,
            "thing:1",
        )],
    );
    s.dirty.clear();
    let id = RecordId::new("_00_query", "new");
    r::rebind_query(
        &mut s,
        "a",
        id.clone(),
        seed_lifecycle(true),
        vec![("t:2".into(), 1)],
        Vec::new(),
    );
    let en = &s.queries["a"];
    assert_eq!(en.def.id, id);
    assert_eq!(en.lifecycle.phase, QueryPhase::Cached);
    assert_eq!(en.remote_array, vec![("t:2".to_string(), 1)]);
    assert!(en.records.is_empty());
    assert_eq!(en.server_state, None);
    assert_eq!(en.register_attempts, 0);
    assert!(s.dirty.contains("a"));
    let lifecycle = en.lifecycle;
    no_op(&s, |s| {
        r::rebind_query(s, "zz", id.clone(), lifecycle, Vec::new(), Vec::new())
    });
    r::clear_bucket_state(&mut s);
    assert!(s.versions.is_empty());
    assert!(s.outbox.is_empty());
    assert!(s.dirty.is_empty());
    assert!(!s.primed);
}

#[test]
fn identity_connection_and_sync_fields() {
    let mut s = state(Vec::new());
    r::set_identity(
        &mut s,
        Identity {
            session_id: Some(Some("s".into())),
            user_id: Some(Some("u".into())),
            bucket_id: Some(Some("b".into())),
            ..Default::default()
        },
    );
    assert_eq!(s.session_id.as_deref(), Some("s"));
    assert_eq!(s.user_id.as_deref(), Some("u"));
    assert_eq!(s.bucket_id.as_deref(), Some("b"));
    r::set_tab_role(&mut s, TabRole::Leader);
    no_op(&s, |s| r::set_tab_role(s, TabRole::Leader));
    r::set_connection(&mut s, ConnectionState::Connected);
    assert_eq!(s.sync.health.connection, ConnectionState::Connected);
    no_op(&s, |s| r::set_connection(s, ConnectionState::Connected));
    let mut degraded = s.sync.health.clone();
    degraded.status = SyncHealthStatus::Degraded;
    r::set_health(&mut s, degraded);
    assert_eq!(s.sync.health.status, SyncHealthStatus::Degraded);
    r::patch_sync(
        &mut s,
        SyncPatch {
            poll_idle_streak: Some(4),
            ..Default::default()
        },
    );
    assert_eq!(s.sync.poll_idle_streak, 4);
    r::set_failed_count(&mut s, 1);
    r::set_failed_count(&mut s, 3);
    assert_eq!(s.failed_count, 3);
}

#[test]
fn set_identity_tells_leave_alone_from_set_to_none() {
    let mut s = state(Vec::new());
    r::set_identity(
        &mut s,
        Identity {
            user_id: Some(Some("u".into())),
            salt_user_id: Some(Some("u".into())),
            ..Default::default()
        },
    );
    r::set_identity(
        &mut s,
        Identity {
            local_ready: Some(true),
            ..Default::default()
        },
    );
    assert_eq!(s.user_id.as_deref(), Some("u"));
    r::set_identity(
        &mut s,
        Identity {
            user_id: Some(None),
            ..Default::default()
        },
    );
    assert_eq!(s.user_id, None);
    assert_eq!(s.salt_user_id.as_deref(), Some("u"));
}
