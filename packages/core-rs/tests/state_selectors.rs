//! PORT: `packages/spooky_core/test/state/selectors_test.dart`.

use serde_json::json;
use spooky_core::state::lifecycle::{LifecycleEvent, QueryLifecycle, QueryPhase, RemotePhase};
use spooky_core::state::reducers as r;
use spooky_core::state::selectors as sel;
use spooky_core::state::{ClientState, OutboxItem, OutboxStatus, PendingWrite, QueryEntry};
use spooky_core::testing::{build_definition, build_entry, build_outbox_item, row};
use spooky_core::types::{MutationEventType, QueryStatus, RecordVersionArray, TimingPhase};

fn life(phase: QueryPhase) -> QueryLifecycle {
    QueryLifecycle {
        phase,
        remote: RemotePhase::Unregistered,
        fetch_depth: 0,
        background_depth: 0,
        notified: false,
    }
}

fn e(hash: &str) -> QueryEntry {
    build_entry(build_definition(hash, "thing"))
}

fn e_with(
    hash: &str,
    lifecycle: QueryLifecycle,
    remote: RecordVersionArray,
    children: RecordVersionArray,
) -> QueryEntry {
    QueryEntry {
        lifecycle,
        remote_array: remote,
        subquery_remote_array: children,
        ..e(hash)
    }
}

fn pairs(items: &[(&str, i64)]) -> RecordVersionArray {
    items.iter().map(|(id, v)| (id.to_string(), *v)).collect()
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

fn item(id: &str, kind: MutationEventType, record_id: &str, acked_at: Option<i64>) -> OutboxItem {
    let mut i = build_outbox_item(id, kind, record_id);
    if let Some(at) = acked_at {
        i.status = OutboxStatus::Acked;
        i.acked_at = Some(at);
    }
    i
}

#[test]
fn basic_lookups() {
    let mut a = e("a");
    a.def.table_name = "t1".into();
    let mut b = e("b");
    b.def.table_name = "t2".into();
    let s = state(vec![a, b]);
    assert_eq!(s.queries.get("a").map(|e| e.def.hash.as_str()), Some("a"));
    assert_eq!(sel::active_hashes(&s), ["a", "b"]);
    assert_eq!(sel::hashes_for_table(&s, "t2"), ["b"]);
    assert_eq!(sel::query_status(&s, "a"), Some(QueryStatus::Idle));
    assert_eq!(sel::query_status(&s, "zz"), None);
}

#[test]
fn unsynced_ids_are_pending_items_and_debounced_patches_not_acked() {
    let mut s = state(Vec::new());
    r::outbox_replace(
        &mut s,
        vec![
            item("1", MutationEventType::Create, "thing:1", None),
            item("2", MutationEventType::Update, "thing:2", Some(1)),
        ],
    );
    r::merge_pending_write(
        &mut s,
        PendingWrite {
            key: "thing:3".into(),
            table: "thing".into(),
            record_id: "thing:3".into(),
            data: row(json!({"n": 1})),
            before: None,
            first_at: 0,
        },
    );
    assert_eq!(
        sel::unsynced_record_ids(&s).into_iter().collect::<Vec<_>>(),
        ["thing:1", "thing:3"]
    );
}

#[test]
fn overlay_derives_writes_and_deletes_from_pending_and_acked_items() {
    let mut s = state(Vec::new());
    r::outbox_replace(
        &mut s,
        vec![
            item("1", MutationEventType::Create, "thing:1", None),
            item("2", MutationEventType::Update, "thing:2", Some(1)),
            item("3", MutationEventType::Delete, "thing:3", None),
        ],
    );
    let o = sel::overlay(&s);
    assert_eq!(
        o.writes.into_iter().collect::<Vec<_>>(),
        ["thing:1", "thing:2"]
    );
    assert_eq!(o.deletes.into_iter().collect::<Vec<_>>(), ["thing:3"]);
    assert_eq!(
        sel::pending_delete_ids(&s).into_iter().collect::<Vec<_>>(),
        ["thing:3"]
    );
    assert!(sel::has_acked_writes(&s));
    assert_eq!(sel::pending_mutation_count(&s), 2);
}

#[test]
fn needed_compares_versions_and_skips_pending_deletes() {
    let mut s = state(vec![e_with(
        "a",
        life(QueryPhase::Live),
        pairs(&[("thing:1", 2), ("thing:2", 1), ("thing:3", 1)]),
        Vec::new(),
    )]);
    r::set_versions(&mut s, &pairs(&[("thing:1", 2), ("thing:2", 0)]));
    r::outbox_replace(
        &mut s,
        vec![item("m1", MutationEventType::Delete, "thing:3", None)],
    );
    assert_eq!(sel::needed(&s, "a"), pairs(&[("thing:2", 1)]));
    assert!(sel::needed(&s, "missing").is_empty());
    let cached = state(vec![e_with(
        "c",
        life(QueryPhase::Cached),
        pairs(&[("thing:9", 1)]),
        Vec::new(),
    )]);
    assert!(sel::needed(&cached, "c").is_empty());
}

#[test]
fn plan_fetch_puts_the_smallest_need_first() {
    let live = life(QueryPhase::Live);
    let games: Vec<(String, i64)> = (0..5).map(|i| (format!("game:{i}"), 1)).collect();
    let s = state(vec![
        e_with("big", live, games, Vec::new()),
        e_with("linkup", live, pairs(&[("friendship:1", 1)]), Vec::new()),
    ]);
    assert_eq!(
        sel::plan_fetch_chunked(&s, 2, usize::MAX).chunks[0],
        ["friendship:1", "game:0"]
    );
}

#[test]
fn plan_fetch_stops_at_a_round_and_the_next_plan_picks_up_the_rest() {
    let games: Vec<(String, i64)> = (0..10).map(|i| (format!("game:{i}"), 1)).collect();
    let mut s = state(vec![e_with(
        "big",
        life(QueryPhase::Live),
        games,
        Vec::new(),
    )]);
    let plan = sel::plan_fetch_chunked(&s, 2, 3);
    assert_eq!(plan.chunks.len(), 3);
    let landed: RecordVersionArray = plan
        .chunks
        .iter()
        .flatten()
        .map(|id| (id.clone(), 1))
        .collect();
    r::set_versions(&mut s, &landed);
    assert_eq!(
        sel::plan_fetch_chunked(&s, 2, 3).chunks,
        vec![vec!["game:6", "game:7"], vec!["game:8", "game:9"]]
    );
}

#[test]
fn plan_fetch_dedupes_includes_subquery_children_and_chunks() {
    let live = life(QueryPhase::Live);
    let s = state(vec![
        e_with(
            "a",
            live,
            pairs(&[("thing:1", 1), ("thing:2", 3)]),
            Vec::new(),
        ),
        e_with(
            "b",
            live,
            pairs(&[("thing:2", 1), ("thing:3", 1)]),
            pairs(&[("child:1", 2)]),
        ),
        e_with(
            "c",
            live,
            Vec::new(),
            pairs(&[("child:1", 1), ("child:2", 1)]),
        ),
        e_with(
            "d",
            life(QueryPhase::Cached),
            pairs(&[("thing:9", 1)]),
            Vec::new(),
        ),
    ]);
    let plan = sel::plan_fetch_chunked(&s, 2, usize::MAX);
    assert_eq!(plan.hashes, ["a", "b"]);
    // `a` and `c` need two rows each, `b` three: smallest first, ties in
    // registration order.
    assert_eq!(
        plan.chunks,
        vec![
            vec!["thing:1".to_string(), "thing:2".to_string()],
            vec!["child:1".to_string(), "child:2".to_string()],
            vec!["thing:3".to_string()],
        ]
    );
    assert_eq!(plan.versions["thing:2"], 3);
    assert_eq!(plan.versions["child:1"], 2);
    assert!(sel::plan_fetch(&state(Vec::new())).chunks.is_empty());
    assert!(sel::needed_children(&s, "missing").is_empty());
    let mut fetched = s.clone();
    r::set_versions(&mut fetched, &pairs(&[("child:1", 2)]));
    assert!(sel::needed_children(&fetched, "b").is_empty());
}

#[test]
fn settled_requires_live_complete_clean_notified() {
    let base = e_with(
        "a",
        QueryLifecycle {
            notified: true,
            ..life(QueryPhase::Live)
        },
        pairs(&[("thing:1", 1)]),
        Vec::new(),
    );
    let mut s = state(vec![base]);
    r::set_versions(&mut s, &pairs(&[("thing:1", 1)]));
    s.dirty.clear();
    assert!(sel::settled(&s, "a"));

    let mut dirty = s.clone();
    r::mark_dirty(&mut dirty, &["a".to_string()]);
    assert!(!sel::settled(&dirty, "a"));
    let mut stale = s.clone();
    r::set_versions(&mut stale, &pairs(&[("thing:1", 0)]));
    stale.dirty.clear();
    assert!(!sel::settled(&stale, "a"));
    let cached = state(vec![e_with(
        "a",
        QueryLifecycle {
            notified: true,
            ..life(QueryPhase::Cached)
        },
        Vec::new(),
        Vec::new(),
    )]);
    assert!(!sel::settled(&cached, "a"));
    let unnotified = state(vec![e_with(
        "a",
        life(QueryPhase::Live),
        Vec::new(),
        Vec::new(),
    )]);
    assert!(!sel::settled(&unnotified, "a"));
    assert!(!sel::settled(&s, "missing"));
    assert!(sel::settle_failed(&s, "missing"));
    assert!(!sel::settle_failed(&s, "a"));
    let mut failed = s.clone();
    r::apply_lifecycle(&mut failed, "a", LifecycleEvent::RemoteFailed);
    assert!(sel::settle_failed(&failed, "a"));
}

#[test]
fn fetching_query_count_counts_entries_with_fetch_depth() {
    let s = state(vec![
        e_with(
            "a",
            QueryLifecycle {
                fetch_depth: 2,
                ..life(QueryPhase::Cold)
            },
            Vec::new(),
            Vec::new(),
        ),
        e("b"),
    ]);
    assert_eq!(sel::fetching_query_count(&s), 1);
}

#[test]
fn fetching_query_count_leaves_out_background_reverification() {
    let s = state(vec![
        e_with(
            "verifying",
            QueryLifecycle {
                fetch_depth: 1,
                background_depth: 1,
                ..life(QueryPhase::Cached)
            },
            Vec::new(),
            Vec::new(),
        ),
        e_with(
            "downloading",
            QueryLifecycle {
                fetch_depth: 2,
                background_depth: 1,
                ..life(QueryPhase::Live)
            },
            Vec::new(),
            Vec::new(),
        ),
    ]);
    assert_eq!(sel::fetching_query_count(&s), 1);
}

#[test]
fn desired_registrations_lists_unregistered_queries() {
    let with_remote = |hash: &str, remote: RemotePhase| QueryEntry {
        lifecycle: QueryLifecycle {
            remote,
            ..life(QueryPhase::Cold)
        },
        ..e(hash)
    };
    let s = state(vec![
        e("a"),
        with_remote("b", RemotePhase::Registered),
        with_remote("c", RemotePhase::Registering),
    ]);
    assert_eq!(sel::desired_registrations(&s), ["a"]);
}

#[test]
fn evictable_respects_subscribers_and_ttl() {
    let entry = |hash: &str, subscribers: u32, left: Option<i64>| {
        let mut q = e(hash);
        q.def.ttl_ms = 100;
        q.subscribers = subscribers;
        q.last_subscriber_left_at = left;
        q
    };
    let s = state(vec![
        entry("gone", 0, Some(0)),
        entry("fresh", 0, Some(950)),
        entry("watched", 1, Some(0)),
        entry("never", 0, None),
    ]);
    assert_eq!(sel::evictable(&s, 1000), ["gone"]);
}

#[test]
fn shortest_ttl_ms() {
    assert_eq!(sel::shortest_ttl_ms(&state(Vec::new())), None);
    let mut a = e("a");
    a.def.ttl_ms = 500;
    let mut b = e("b");
    b.def.ttl_ms = 100;
    assert_eq!(sel::shortest_ttl_ms(&state(vec![a, b])), Some(100));
}

#[test]
fn phase_timings_summarize_every_phase_with_none_before_the_first_sample() {
    let cold = sel::phase_timings(&e("a"));
    assert_eq!(cold.ssp.last_ms, None);
    assert_eq!(cold.ssp.p50, None);
    assert_eq!(cold.ssp.count, 0);
    assert_eq!(cold.local_fetch.count, 0);

    let mut s = state(vec![e("a")]);
    for ms in [5.0, 1.0, 9.0, 3.0] {
        r::record_ingest(&mut s, "a", ms);
    }
    r::record_phase(&mut s, "a", TimingPhase::RemoteFetch, 7.0);
    r::record_error(&mut s, "a");
    let t = sel::phase_timings(&s.queries["a"]);
    assert_eq!(t.ssp.last_ms, Some(3.0));
    assert_eq!(t.ssp.p50, Some(5.0));
    assert_eq!(t.ssp.p90, Some(9.0));
    assert_eq!(t.ssp.p99, Some(9.0));
    assert_eq!(t.ssp.count, 4);
    assert_eq!(t.remote_fetch.last_ms, Some(7.0));
    assert_eq!(t.remote_fetch.p50, Some(7.0));
    assert_eq!(t.error_count, 1);
    assert_eq!(t.registration.parse_ms, None);
}
