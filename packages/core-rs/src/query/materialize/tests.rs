use serde_json::json;

use super::*;
use crate::modules::query_builder::RelationPlan;
use crate::query::test_support::*;
use crate::state::lifecycle::{is_authoritative, QueryPhase, RemotePhase};
use crate::state::{ClientState, QueryEntry};
use crate::testing::{build_outbox_item, row, run_local, TestRig};
use crate::types::{Cardinality, MutationEventType};
use crate::value::{Object, RecordId};

async fn mat(rig: &TestRig, hash: &str) {
    let hash = hash.to_string();
    rig.run(|ctx| async move { materialize(&ctx, &hash).await })
        .await
        .unwrap();
}

fn dirty(hash: &'static str) -> impl FnOnce(&mut ClientState) {
    move |s| {
        s.dirty.insert(hash.to_string());
    }
}

fn record_ids(rig: &TestRig, hash: &str) -> Vec<String> {
    rig.state().queries[hash]
        .records
        .iter()
        .map(|r| r["id"].to_id_string())
        .collect()
}

fn with_surql(mut e: QueryEntry, surql: &str) -> QueryEntry {
    e.def.surql = surql.to_string();
    e
}

fn plan(alias: &str, table: &str, cardinality: Cardinality, fk: &str) -> RelationPlan {
    RelationPlan {
        alias: alias.into(),
        table: table.into(),
        cardinality,
        foreign_key_field: fk.into(),
        select: Vec::new(),
        where_: Object::new(),
        order_by: Vec::new(),
        limit: None,
        relations: Vec::new(),
    }
}

// ---- helpers --------------------------------------------------------------------------

#[test]
fn table_of_ids_reads_the_first_id_else_the_fallback() {
    assert_eq!(table_of_ids(&["thing:1".to_string()], "other"), "thing");
    assert_eq!(table_of_ids(&[], "other"), "other");
}

#[test]
fn only_an_offset_query_is_a_window() {
    assert!(!is_windowed("SELECT * FROM thing"));
    assert!(!is_windowed("SELECT * FROM thing LIMIT 10"));
    assert!(!is_windowed("SELECT * FROM thing LIMIT 10 START 0"));
    assert!(is_windowed("SELECT * FROM thing LIMIT 10 START 10"));
}

#[test]
fn reapplies_a_window_order_by_to_rows_resolved_by_id() {
    let rows = || {
        vec![
            row(json!({"id": "thing:1", "n": 3})),
            row(json!({"id": "thing:2", "n": 1})),
            row(json!({"id": "thing:3", "n": 2})),
        ]
    };
    let mut sorted = rows();
    apply_window_order(
        "SELECT * FROM thing ORDER BY n ASC LIMIT 10 START 10",
        &mut sorted,
    );
    let n: Vec<i64> = sorted.iter().map(|r| r["n"].as_i64().unwrap()).collect();
    assert_eq!(n, [1, 2, 3]);
    // No window, or no ORDER BY: the circuit's order is kept.
    for surql in [
        "SELECT * FROM thing ORDER BY n",
        "SELECT * FROM thing LIMIT 10 START 10",
    ] {
        let mut kept = rows();
        apply_window_order(surql, &mut kept);
        assert_eq!(kept, rows());
    }
}

#[test]
fn rows_equal_compares_structurally_across_wire_types() {
    assert!(rows_equal(&[], &[]));
    assert!(rows_equal(&[row(json!({"a": 1}))], &[row(json!({"a": 1}))]));
    assert!(!rows_equal(
        &[row(json!({"a": 1}))],
        &[row(json!({"a": 2}))]
    ));
    assert!(!rows_equal(&[row(json!({"a": 1}))], &[]));
    let dt = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let mut typed = Row::new();
    typed.insert("r".into(), Value::RecordId(RecordId::new("user", "u1")));
    typed.insert("d".into(), Value::Datetime(dt));
    let wire = row(json!({"r": "user:u1", "d": "2026-01-01T00:00:00.000Z"}));
    assert!(rows_equal(&[typed.clone()], &[typed.clone()]));
    assert!(rows_equal(&[typed], &[wire]));
}

// ---- materialize ------------------------------------------------------------------------

#[tokio::test]
async fn an_unchanged_empty_result_is_emitted_only_once_settled() {
    run_local(async {
        for phase in [QueryPhase::Cold, QueryPhase::Live] {
            let (rig, _) = rig_of(
                vec![entry("empty", phase, RemotePhase::Unregistered, &[])],
                dirty("empty"),
            );
            let (_sub, seen) = events(&rig);
            mat(&rig, "empty").await;
            let expected = if phase == QueryPhase::Live { 1 } else { 0 };
            assert_eq!(count_kind(&seen, "query:records"), expected, "{phase:?}");
        }
    })
    .await;
}

#[tokio::test]
async fn no_entry_nothing_happens() {
    run_local(async {
        let (rig, _) = new_rig();
        mat(&rig, "nope").await;
        assert!(rig.log.calls().is_empty());
    })
    .await;
}

#[tokio::test]
async fn live_renders_membership_clears_dirt_and_notifies_once() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:1", 1)],
            )],
            dirty("a"),
        );
        rig.local.seed(
            "thing",
            "thing:1",
            row(json!({"id": "thing:1", "title": "a"})),
        );
        let (_sub, seen) = events(&rig);
        mat(&rig, "a").await;
        let read = rig.log.named("local.getMany");
        assert_eq!(read.len(), 1);
        assert_eq!(
            read[0].args,
            format!("{:?}", ("thing", &["thing:1".to_string()][..]))
        );
        let st = rig.state();
        let e = &st.queries["a"];
        assert_eq!(
            *e.records,
            vec![row(json!({"id": "thing:1", "title": "a"}))]
        );
        assert!(st.dirty.is_empty());
        assert!(e.lifecycle.notified);
        assert_eq!(e.telemetry.update_count, 1);
        assert_eq!(e.telemetry.last_updated_at, Some(rig.now.get()));
        assert!(e
            .telemetry
            .phase_last
            .contains_key(&TimingPhase::LocalFetch));
        assert_eq!(count_kind(&seen, "query:records"), 1);
    })
    .await;
}

#[tokio::test]
async fn subscribers_receive_the_rendered_rows() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:1", 1)],
            )],
            |_| {},
        );
        rig.local
            .seed("thing", "thing:1", row(json!({"id": "thing:1"})));
        let got = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let g2 = got.clone();
        let _sub = rig
            .runtime
            .subscribe("a", move |rows| g2.borrow_mut().push(rows.len()), false);
        mat(&rig, "a").await;
        assert_eq!(*got.borrow(), vec![1]);
    })
    .await;
}

#[tokio::test]
async fn a_cold_query_renders_the_circuits_local_window() {
    run_local(async {
        // Divergence from the browser client, which re-runs the query's own
        // predicate against the local store: sqlite cannot run SurrealQL.
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Cold,
                RemotePhase::Unregistered,
                &[("thing:9", 1)],
            )],
            |s| s.queries.get_mut("a").unwrap().local_array = pairs(&[("thing:1", 1)]),
        );
        rig.local
            .seed("thing", "thing:1", row(json!({"id": "thing:1"})));
        rig.local
            .seed("thing", "thing:9", row(json!({"id": "thing:9"})));
        mat(&rig, "a").await;
        assert_eq!(record_ids(&rig, "a"), ["thing:1"]);
    })
    .await;
}

#[tokio::test]
async fn the_outbox_overlay_adds_local_writes_and_hides_local_deletes() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:1", 1), ("thing:3", 1)],
            )],
            |s| {
                s.queries.get_mut("a").unwrap().local_array = pairs(&[("thing:2", 1)]);
                s.outbox.push(build_outbox_item(
                    "m1",
                    MutationEventType::Create,
                    "thing:2",
                ));
                s.outbox.push(build_outbox_item(
                    "m2",
                    MutationEventType::Delete,
                    "thing:3",
                ));
            },
        );
        for id in ["thing:1", "thing:2", "thing:3"] {
            rig.local.seed("thing", id, row(json!({"id": id})));
        }
        mat(&rig, "a").await;
        assert_eq!(record_ids(&rig, "a"), ["thing:1", "thing:2"]);
    })
    .await;
}

#[tokio::test]
async fn an_unchanged_read_notifies_nobody_but_still_clears_the_dirt() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:1", 1)],
            )],
            dirty("a"),
        );
        rig.local
            .seed("thing", "thing:1", row(json!({"id": "thing:1"})));
        mat(&rig, "a").await;
        let (_sub, seen) = events(&rig);
        rig.runtime.update(|s| {
            s.dirty.insert("a".into());
        });
        mat(&rig, "a").await;
        assert_eq!(count_kind(&seen, "query:records"), 0);
        let st = rig.state();
        assert!(st.dirty.is_empty());
        assert_eq!(st.queries["a"].telemetry.update_count, 1);
    })
    .await;
}

#[tokio::test]
async fn a_failing_read_counts_an_error_and_clears_the_dirt() {
    run_local(async {
        let (rig, hooks) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:1", 1)],
            )],
            dirty("a"),
        );
        Hooks::set(&hooks.fail_get_many, "store closed");
        let (_sub, seen) = events(&rig);
        mat(&rig, "a").await;
        let st = rig.state();
        assert_eq!(st.queries["a"].telemetry.error_count, 1);
        assert!(st.dirty.is_empty());
        let logs = logs(&seen);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].0, LogLevel::Warn);
        assert_eq!(count_kind(&seen, "query:records"), 0);
    })
    .await;
}

#[tokio::test]
async fn a_windowed_query_reapplies_its_own_order_by() {
    run_local(async {
        let e = with_surql(
            entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:1", 1), ("thing:2", 1)],
            ),
            "SELECT * FROM thing ORDER BY n ASC LIMIT 10 START 10",
        );
        let (rig, _) = rig_of(vec![e], |_| {});
        rig.local
            .seed("thing", "thing:1", row(json!({"id": "thing:1", "n": 2})));
        rig.local
            .seed("thing", "thing:2", row(json!({"id": "thing:2", "n": 1})));
        mat(&rig, "a").await;
        let n: Vec<i64> = rig.state().queries["a"]
            .records
            .iter()
            .map(|r| r["n"].as_i64().unwrap())
            .collect();
        assert_eq!(n, [1, 2]);
    })
    .await;
}

#[tokio::test]
async fn a_window_keeps_the_servers_id_order_others_sort_by_id() {
    run_local(async {
        let window = with_surql(
            entry(
                "w",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:b", 1), ("thing:a", 1)],
            ),
            "SELECT * FROM thing LIMIT 10 START 10",
        );
        let plain = entry(
            "p",
            QueryPhase::Live,
            RemotePhase::Unregistered,
            &[("thing:b", 1), ("thing:a", 1)],
        );
        let (rig, _) = rig_of(vec![window, plain], |_| {});
        rig.local
            .seed("thing", "thing:a", row(json!({"id": "thing:a"})));
        rig.local
            .seed("thing", "thing:b", row(json!({"id": "thing:b"})));
        mat(&rig, "w").await;
        mat(&rig, "p").await;
        assert_eq!(record_ids(&rig, "w"), ["thing:b", "thing:a"]);
        assert_eq!(record_ids(&rig, "p"), ["thing:a", "thing:b"]);
    })
    .await;
}

// ---- .related() from the local cache --------------------------------------------------

/// A new query paints cached rows, joins included, before the server answers,
/// and picks a joined row up once it lands locally.
#[tokio::test]
async fn a_cold_query_resolves_its_relations_from_the_local_cache() {
    run_local(async {
        let mut e = entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[]);
        e.local_array = pairs(&[("user:alice", 1)]);
        e.def.table_name = "user".into();
        e.def.relations = vec![plan("mentor", "user", Cardinality::One, "mentor")];
        let (rig, _) = rig_of(vec![e], |_| {});
        rig.local.seed(
            "user",
            "user:alice",
            row(
                json!({"id": "user:alice", "username": "alice", "mentor": "user:bob", "_00_rv": 1}),
            ),
        );
        mat(&rig, "a").await;
        let st = rig.state();
        let rows = &st.queries["a"].records;
        assert_eq!(rows[0]["username"], Value::from("alice"));
        assert_eq!(rows[0]["mentor"], Value::Null);
        assert!(!is_authoritative(&st.queries["a"].lifecycle));

        rig.local.seed(
            "user",
            "user:bob",
            row(json!({"id": "user:bob", "username": "bob"})),
        );
        mat(&rig, "a").await;
        let rows = rig.state().queries["a"].records.clone();
        assert_eq!(rows[0]["mentor"].get("username"), Some(&Value::from("bob")));
    })
    .await;
}

#[tokio::test]
async fn materializing_relations_never_writes_the_alias_into_the_store() {
    run_local(async {
        let mut e = entry(
            "a",
            QueryPhase::Live,
            RemotePhase::Unregistered,
            &[("thing:t1", 1)],
        );
        e.def.relations = vec![plan("comments", "comment", Cardinality::Many, "thing")];
        let (rig, _) = rig_of(vec![e], |_| {});
        rig.local.seed(
            "thing",
            "thing:t1",
            row(json!({"id": "thing:t1", "title": "first"})),
        );
        rig.local.seed(
            "comment",
            "comment:c1",
            row(json!({"id": "comment:c1", "thing": "thing:t1", "body": "hi"})),
        );
        mat(&rig, "a").await;
        let rows = rig.state().queries["a"].records.clone();
        assert_eq!(rows[0]["comments"].as_array().unwrap().len(), 1);
        assert!(!rig
            .local
            .row("thing", "thing:t1")
            .unwrap()
            .contains_key("comments"));
    })
    .await;
}

#[tokio::test]
async fn a_relation_cycle_fails_the_render_not_the_saga() {
    run_local(async {
        fn chain(depth: usize) -> RelationPlan {
            let mut p = plan("self", "thing", Cardinality::One, "id");
            if depth > 0 {
                p.relations = vec![chain(depth - 1)];
            }
            p
        }
        let mut e = entry(
            "a",
            QueryPhase::Live,
            RemotePhase::Unregistered,
            &[("thing:1", 1)],
        );
        e.def.relations = vec![chain(crate::query::relations::MAX_RELATION_DEPTH)];
        let (rig, _) = rig_of(vec![e], dirty("a"));
        rig.local
            .seed("thing", "thing:1", row(json!({"id": "thing:1"})));
        mat(&rig, "a").await;
        let st = rig.state();
        assert_eq!(st.queries["a"].telemetry.error_count, 1);
        assert!(st.dirty.is_empty());
    })
    .await;
}

// ---- the runtime drives it -----------------------------------------------------------

#[tokio::test]
async fn a_dirty_query_materializes_when_its_timer_fires() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:1", 1)],
            )],
            |_| {},
        );
        rig.local
            .seed("thing", "thing:1", row(json!({"id": "thing:1"})));
        rig.runtime
            .update(|s| crate::state::reducers::mark_dirty(s, &["a".to_string()]));
        assert!(rig.timers.fire("mat:a"));
        rig.settle().await;
        assert_eq!(record_ids(&rig, "a"), ["thing:1"]);
    })
    .await;
}

// ---- stream_update ----------------------------------------------------------------------

#[tokio::test]
async fn a_stream_update_takes_the_id_set_dirties_and_records_timings() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let update = StreamUpdate {
            query_hash: "a".into(),
            local_array: pairs(&[("thing:1", 1)]),
            materialization_time_ms: Some(4.0),
            store_apply_ms: Some(1.0),
            circuit_step_ms: Some(2.0),
            transform_ms: Some(3.0),
            ..Default::default()
        };
        rig.run(|ctx| async move { stream_update(&ctx, update).await })
            .await
            .unwrap();
        let st = rig.state();
        let e = &st.queries["a"];
        assert_eq!(e.local_array, pairs(&[("thing:1", 1)]));
        assert!(st.dirty.contains("a"));
        let t = &e.telemetry;
        assert_eq!(t.last_ingest_latency_ms, Some(4.0));
        assert_eq!(t.phase_last.get(&TimingPhase::SspStoreApply), Some(&1.0));
        assert_eq!(t.phase_last.get(&TimingPhase::SspCircuitStep), Some(&2.0));
        assert_eq!(t.phase_last.get(&TimingPhase::SspTransform), Some(&3.0));
        assert!(rig.timers.has("mat:a"), "the runtime schedules the render");
    })
    .await;
}

#[tokio::test]
async fn a_stream_update_for_an_unknown_hash_is_ignored() {
    run_local(async {
        let (rig, _) = new_rig();
        let update = StreamUpdate {
            query_hash: "nope".into(),
            ..Default::default()
        };
        rig.run(|ctx| async move { stream_update(&ctx, update).await })
            .await
            .unwrap();
        assert!(rig.state().dirty.is_empty());
    })
    .await;
}
