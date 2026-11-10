use std::sync::Arc;

use serde_json::json;

use super::*;
use crate::error::Error;
use crate::kernel::ports::IngestOp;
use crate::modules::query_builder::RelationPlan;
use crate::query::test_support::*;
use crate::state::lifecycle::QueryPhase;
use crate::state::reducers::Identity;
use crate::state::{ClientState, QueryEntry};
use crate::testing::{build_outbox_item, results, row, run_local, TestRig};
use crate::types::{Cardinality, MutationEventType};
use crate::value::{Object, Value};

const DAY: i64 = 24 * 60 * 60 * 1000;
const NOW: i64 = 1_700_000_000_000;

fn registered(hash: &str) -> QueryEntry {
    entry(hash, QueryPhase::Live, RemotePhase::Registered, &[])
}

async fn tick(rig: &TestRig) {
    let env = rig.env();
    rig.run(|ctx| async move { lifecycle_tick(&ctx, &env).await })
        .await
        .unwrap();
}

async fn gc(rig: &TestRig) {
    rig.run(|ctx| async move { gc_tick(&ctx).await })
        .await
        .unwrap();
}

// ---- lifecycle_tick -------------------------------------------------------------------

#[tokio::test]
async fn evicts_idle_queries_heartbeats_the_rest_and_reschedules_at_half_the_ttl() {
    run_local(async {
        let mut idle = entry("idle", QueryPhase::Cold, RemotePhase::Unregistered, &[]);
        idle.def.ttl_ms = 100;
        idle.last_subscriber_left_at = Some(0);
        let mut kept = registered("kept");
        kept.def.ttl_ms = 400;
        kept.subscribers = 1;
        let (rig, _) = rig_of(vec![idle, kept], |_| {});
        rig.set_now(1000);
        let sent = answer(&rig, |_, _| Ok(results(vec![Ok(json!([1]))])));
        let (_sub, seen) = events(&rig);
        tick(&rig).await;
        let st = rig.state();
        assert_eq!(st.queries.keys().collect::<Vec<_>>(), ["kept"]);
        assert!(seen
            .borrow()
            .iter()
            .any(|e| matches!(e, OutEvent::QueryEvicted { hash } if hash == "idle")));
        assert_eq!(*rig.ssp.unregistered.borrow(), ["idle"]);
        assert_eq!(st.queries["kept"].last_heartbeat_at, Some(1000));
        assert_eq!(rig.timers.delay("lifecycle"), Some(200));
        assert_eq!(sent.borrow()[0].sql, "fn::query::heartbeat($id0)");
        rig.settle().await;
        assert!(rig.state().sync.has_synced_once);
    })
    .await;
}

#[tokio::test]
async fn a_throwing_unregister_still_evicts() {
    run_local(async {
        let mut idle = entry("idle", QueryPhase::Cold, RemotePhase::Unregistered, &[]);
        idle.def.ttl_ms = 100;
        idle.last_subscriber_left_at = Some(0);
        let (rig, hooks) = rig_of(vec![idle], |_| {});
        Hooks::set(&hooks.fail_unregister, "circuit gone");
        rig.set_now(1000);
        let (_sub, seen) = events(&rig);
        tick(&rig).await;
        assert!(rig.state().queries.is_empty());
        let logs = logs(&seen);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].0, LogLevel::Debug);
    })
    .await;
}

#[tokio::test]
async fn a_watched_or_young_query_is_not_evicted() {
    run_local(async {
        let mut young = entry("young", QueryPhase::Cold, RemotePhase::Unregistered, &[]);
        young.def.ttl_ms = 100;
        young.last_subscriber_left_at = Some(950);
        let mut watched = entry("watched", QueryPhase::Cold, RemotePhase::Unregistered, &[]);
        watched.def.ttl_ms = 100;
        watched.subscribers = 1;
        let (rig, _) = rig_of(vec![young, watched], |_| {});
        rig.set_now(1000);
        tick(&rig).await;
        assert_eq!(rig.state().queries.len(), 2);
    })
    .await;
}

#[tokio::test]
async fn a_reclaimed_row_drops_the_registration_and_reregisters() {
    run_local(async {
        let (rig, _) = rig_of(vec![registered("a")], |_| {});
        // An empty answer means the server reclaimed the view row.
        let sent = answer(&rig, |sql, _| {
            Ok(if sql.starts_with("fn::query::heartbeat") {
                results(vec![Ok(json!([]))])
            } else {
                Vec::new()
            })
        });
        let (_sub, seen) = events(&rig);
        tick(&rig).await;
        assert_eq!(
            rig.state().queries["a"].lifecycle.remote,
            RemotePhase::Unregistered
        );
        let logs = logs(&seen);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].0, LogLevel::Warn);
        rig.settle().await;
        assert!(
            sent.borrow()
                .iter()
                .any(|s| s.sql.starts_with("fn::query::register")),
            "EnsureRegistered re-registers"
        );
    })
    .await;
}

#[tokio::test]
async fn a_failed_beat_reports_and_empty_state_uses_the_default_ttl() {
    run_local(async {
        let (rig, _) = rig_of(vec![registered("a")], |_| {});
        answer(&rig, |_, _| Err(Error::Network("socket closed".into())));
        tick(&rig).await;
        assert!(
            rig.timers.has("lifecycle"),
            "a failed beat still reschedules"
        );
        rig.settle().await;
        assert_eq!(rig.state().sync.consecutive_failures, 1);

        let (rig, _) = new_rig();
        let sent = record(&rig);
        tick(&rig).await;
        assert_eq!(rig.timers.delay("lifecycle"), Some(300_000));
        assert!(sent.borrow().is_empty());
    })
    .await;
}

#[tokio::test]
async fn the_heartbeat_is_one_request_for_every_registered_view() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![
                registered("a"),
                registered("b"),
                entry("c", QueryPhase::Cold, RemotePhase::Unregistered, &[]),
            ],
            |_| {},
        );
        let sent = answer(&rig, |_, _| {
            Ok(results(vec![Ok(json!([1])), Ok(json!([1]))]))
        });
        tick(&rig).await;
        let sent = sent.borrow();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].sql.split(";\n").count(), 2);
        let keys: Vec<&String> = sent[0].vars.as_ref().unwrap().keys().collect();
        assert_eq!(keys, ["id0", "id1"]);
        let st = rig.state();
        assert!(st.queries["c"].last_heartbeat_at.is_none());
    })
    .await;
}

#[tokio::test]
async fn the_tick_reschedules_itself_through_the_runtime() {
    run_local(async {
        let (rig, _) = rig_of(vec![registered("a")], |_| {});
        let sent = answer(&rig, |_, _| Ok(results(vec![Ok(json!([1]))])));
        tick(&rig).await;
        assert!(rig.timers.fire("lifecycle"));
        rig.settle().await;
        assert_eq!(sent.borrow().len(), 2);
    })
    .await;
}

// ---- ack_prune ----------------------------------------------------------------------

#[tokio::test]
async fn ack_prune_drops_expired_acked_items_and_rearms_while_some_remain() {
    run_local(async {
        let acked = |id: &str, at: i64| {
            let mut i = build_outbox_item(id, MutationEventType::Update, &format!("thing:{id}"));
            i.status = OutboxStatus::Acked;
            i.acked_at = Some(at);
            i
        };
        let (rig, _) = rig_of(Vec::new(), |s| {
            s.outbox = vec![acked("old", 0), acked("fresh", 99_000)];
        });
        rig.set_now(100_000);
        rig.run(|ctx| async move { ack_prune(&ctx).await })
            .await
            .unwrap();
        let ids: Vec<String> = rig.state().outbox.iter().map(|i| i.id.clone()).collect();
        assert_eq!(ids, ["fresh"]);
        assert_eq!(rig.timers.delay("ack-prune"), Some(30_000));

        let (rig, _) = new_rig();
        rig.run(|ctx| async move { ack_prune(&ctx).await })
            .await
            .unwrap();
        assert!(rig.timers.pending().is_empty());
    })
    .await;
}

// ---- gc_tick ------------------------------------------------------------------------

fn gc_state(versions: &[&str]) -> impl FnOnce(&mut ClientState) {
    let versions: Vec<String> = versions.iter().map(|s| s.to_string()).collect();
    move |s| {
        primed(s);
        for id in versions {
            s.versions.insert(id, 1);
        }
    }
}

/// Seed a body row for every id, and the view rows.
fn seed(rig: &TestRig, ids: &[&str], views: &[serde_json::Value]) {
    for id in ids {
        rig.local
            .seed(table_of(id), id, row(json!({"id": id, "_00_rv": 1})));
    }
    for view in views {
        let id = view["id"].as_str().unwrap();
        rig.local.seed("_00_view", id, row(view.clone()));
    }
}

fn deleted(rig: &TestRig) -> Vec<String> {
    rig.log
        .named("local.delete")
        .into_iter()
        .map(|c| c.args.split('"').nth(3).unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn gc_deletes_bodies_no_view_query_or_outbox_item_retains() {
    run_local(async {
        let ids = ["thing:keep", "thing:pending", "thing:orphan", "_00_view:x"];
        let (rig, _) = rig_of(Vec::new(), |s| {
            gc_state(&ids)(s);
            s.outbox.push(build_outbox_item(
                "m1",
                MutationEventType::Update,
                "thing:pending",
            ));
            s.sync.fetch_attempts = 5;
        });
        seed(
            &rig,
            &ids[..3],
            &[json!({"id": "_00_view:x", "ids": [["thing:keep", 1]], "updatedAt": NOW})],
        );
        let (_sub, seen) = events(&rig);
        gc(&rig).await;
        assert_eq!(
            deleted(&rig),
            ["thing:orphan"],
            "internal rows and anything a view or the outbox names stay"
        );
        assert!(rig.local.row("thing", "thing:orphan").is_none());
        assert!(rig.local.row("thing", "thing:keep").is_some());
        let st = rig.state();
        assert!(!st.versions.contains_key("thing:orphan"));
        assert_eq!(st.versions.len(), 3);
        let ingested = rig.ssp.ingested.borrow().clone();
        assert_eq!(ingested.len(), 1);
        assert_eq!(
            (ingested[0].op, ingested[0].id.as_str()),
            (IngestOp::Delete, "thing:orphan")
        );
        assert_eq!(rig.timers.delay("gc"), Some(60 * 60 * 1000));
        let logs = logs(&seen);
        assert_eq!(
            logs.last().unwrap().2,
            Some(Value::from(json!({"removed": 1, "retiredViews": 0})))
        );
        // FetchRows was dispatched: its empty plan resets the attempt counter.
        rig.settle().await;
        assert_eq!(rig.state().sync.fetch_attempts, 0);
    })
    .await;
}

#[tokio::test]
async fn gc_keeps_the_subquery_children_a_view_row_records() {
    run_local(async {
        let ids = ["thing:1", "user:a", "user:gone"];
        let (rig, _) = rig_of(Vec::new(), gc_state(&ids));
        seed(
            &rig,
            &ids,
            &[json!({"id": "_00_view:list", "ids": [["thing:1", 1]], "children": [["user:a", 1]], "updatedAt": NOW})],
        );
        gc(&rig).await;
        assert_eq!(deleted(&rig), ["user:gone"]);
    })
    .await;
}

#[tokio::test]
async fn gc_keeps_what_a_query_in_state_holds_members_and_children() {
    run_local(async {
        let ids = ["thing:1", "user:a", "user:gone"];
        let mut e = entry("a", QueryPhase::Live, RemotePhase::Registered, &[("thing:1", 1)]);
        e.subquery_remote_array = pairs(&[("user:a", 1)]);
        let (rig, _) = rig_of(vec![e], gc_state(&ids));
        seed(&rig, &ids, &[]);
        gc(&rig).await;
        assert_eq!(
            deleted(&rig),
            ["user:gone"],
            "a view row written before children were recorded must not cost the bodies a live query has just been answered with"
        );
    })
    .await;
}

#[tokio::test]
async fn gc_keeps_the_joined_rows_a_query_renders_before_any_answer() {
    run_local(async {
        let ids = ["thing:1", "user:a", "user:gone"];
        let mut e = entry("a", QueryPhase::Cached, RemotePhase::Unregistered, &[]);
        e.def.relations = vec![RelationPlan {
            alias: "owner".into(),
            table: "user".into(),
            cardinality: Cardinality::One,
            foreign_key_field: "owner".into(),
            select: Vec::new(),
            where_: Object::new(),
            order_by: Vec::new(),
            limit: None,
            relations: Vec::new(),
        }];
        e.records = Arc::new(vec![row(
            json!({"id": "thing:1", "owner": {"id": "user:a", "username": "a"}}),
        )]);
        let (rig, _) = rig_of(vec![e], gc_state(&ids));
        seed(
            &rig,
            &ids,
            &[json!({"id": "_00_view:list", "ids": [["thing:1", 1]], "updatedAt": NOW})],
        );
        gc(&rig).await;
        assert_eq!(
            deleted(&rig),
            ["user:gone"],
            "a view row from before children were recorded, offline"
        );
    })
    .await;
}

#[tokio::test]
async fn gc_retires_stale_unheld_view_rows_and_collects_what_only_they_named() {
    run_local(async {
        let ids = ["thing:old", "thing:held", "thing:new"];
        let mut held = entry("h", QueryPhase::Cached, RemotePhase::Unregistered, &[]);
        held.def.view_key = "held".into();
        let (rig, _) = rig_of(vec![held], gc_state(&ids));
        seed(
            &rig,
            &ids,
            &[
                json!({"id": "_00_view:stale", "ids": [["thing:old", 1]], "updatedAt": NOW - 15 * DAY}),
                json!({"id": "_00_view:held", "ids": [["thing:held", 1]], "updatedAt": NOW - 15 * DAY}),
                json!({"id": "_00_view:fresh", "ids": [["thing:new", 1]], "updatedAt": NOW - DAY}),
            ],
        );
        let (_sub, seen) = events(&rig);
        gc(&rig).await;
        assert_eq!(
            deleted(&rig),
            ["_00_view:stale", "thing:old"],
            "a held view row stays however old, a fresh one stays"
        );
        assert!(rig.local.row("_00_view", "_00_view:held").is_some());
        let logs = logs(&seen);
        assert_eq!(logs.last().unwrap().2, Some(Value::from(json!({"removed": 1, "retiredViews": 1}))));
    })
    .await;
}

#[tokio::test]
async fn gc_keeps_the_row_of_a_query_mounted_while_the_rows_were_read() {
    run_local(async {
        let (rig, hooks) = rig_of(Vec::new(), gc_state(&["thing:old"]));
        seed(
            &rig,
            &["thing:old"],
            &[json!({"id": "_00_view:stale", "ids": [["thing:old", 1]], "updatedAt": NOW - 15 * DAY})],
        );
        hooks.on_get_all(|rt, table| {
            if table == "_00_view" {
                let mut e = entry("m", QueryPhase::Cold, RemotePhase::Unregistered, &[]);
                e.def.view_key = "stale".into();
                rt.update(|s| crate::state::reducers::put_query(s, e));
            }
        });
        gc(&rig).await;
        assert!(deleted(&rig).is_empty());
    })
    .await;
}

#[tokio::test]
async fn gc_stops_when_the_bucket_moved_under_the_sweep() {
    run_local(async {
        let (rig, hooks) = rig_of(Vec::new(), |s| {
            gc_state(&["thing:orphan"])(s);
            s.bucket_id = Some("u1".into());
        });
        seed(&rig, &["thing:orphan"], &[]);
        hooks.on_get_all(|rt, _| {
            rt.update(|s| {
                crate::state::reducers::set_identity(
                    s,
                    Identity {
                        bucket_id: Some(Some("u2".into())),
                        ..Default::default()
                    },
                )
            })
        });
        gc(&rig).await;
        assert!(deleted(&rig).is_empty());
        assert!(rig.timers.has("gc"));
    })
    .await;
}

#[tokio::test]
async fn gc_stops_when_the_store_was_replaced_mid_sweep() {
    run_local(async {
        let (rig, hooks) = rig_of(Vec::new(), gc_state(&["thing:a", "thing:b"]));
        seed(&rig, &["thing:a", "thing:b"], &[]);
        // The store is swapped while the view rows are read: nothing of the
        // old bucket may be deleted, and the circuit must not hear of it.
        let local = rig.local.clone();
        hooks.on_get_all(move |_, _| local.bump_epoch());
        gc(&rig).await;
        assert!(deleted(&rig).is_empty());
        assert!(rig.ssp.ingested.borrow().is_empty());
        assert_eq!(rig.state().versions.len(), 2);
    })
    .await;
}

#[tokio::test]
async fn a_failing_sweep_is_logged_and_still_reschedules() {
    run_local(async {
        let (rig, hooks) = rig_of(Vec::new(), primed);
        Hooks::set(&hooks.fail_get_all, "store closed");
        let (_sub, seen) = events(&rig);
        gc(&rig).await;
        let logs = logs(&seen);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].0, LogLevel::Warn);
        assert!(rig.timers.has("gc"));
    })
    .await;
}

#[tokio::test]
async fn gc_waits_for_the_prime() {
    run_local(async {
        let (rig, _) = new_rig();
        let rt = rig.runtime.clone();
        let task = tokio::task::spawn_local(async move {
            rt.run(|ctx| async move { gc_tick(&ctx).await }).await
        });
        rig.settle().await;
        assert!(!task.is_finished());
        rig.runtime.update(primed);
        task.await.unwrap().unwrap();
        assert!(rig.timers.has("gc"));
    })
    .await;
}
