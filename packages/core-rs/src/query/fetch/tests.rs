use std::sync::Arc;

use serde_json::json;

use super::*;
use crate::error::Error;
use crate::kernel::ports::StatementResult;
use crate::query::test_support::*;
use crate::state::lifecycle::{QueryPhase, RemotePhase};
use crate::state::ClientState;
use crate::testing::{results, row, run_local, TestRig};
use crate::types::{ColumnSchema, Schema};

fn live(hash: &str, remote: &[(&str, i64)]) -> crate::state::QueryEntry {
    entry(hash, QueryPhase::Live, RemotePhase::Registered, remote)
}

fn body(id: &str) -> serde_json::Value {
    json!({"id": id, "title": "t", "serverOnly": 0})
}

fn bodies(ids: &[&str]) -> Vec<StatementResult> {
    results(vec![Ok(serde_json::Value::Array(
        ids.iter().map(|id| body(id)).collect(),
    ))])
}

async fn fetch(rig: &TestRig) {
    let env = rig.env();
    rig.run(|ctx| async move { fetch_rows(&ctx, &env).await })
        .await
        .unwrap();
}

#[tokio::test]
async fn waits_for_the_prime() {
    run_local(async {
        let (rig, _) = rig_of(vec![live("a", &[("thing:1", 1)])], |_| {});
        let sent = answer(&rig, |_, _| Ok(bodies(&["thing:1"])));
        let rt = rig.runtime.clone();
        let env = rig.env();
        let task = tokio::task::spawn_local(async move {
            rt.run(|ctx| async move { fetch_rows(&ctx, &env).await })
                .await
        });
        rig.settle().await;
        assert!(!task.is_finished());
        assert!(sent.borrow().is_empty());
        rig.runtime.update(primed);
        task.await.unwrap().unwrap();
        assert_eq!(sent.borrow().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn an_empty_plan_resets_the_attempts_and_returns() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), |s| {
            primed(s);
            s.sync.fetch_attempts = 3;
        });
        let sent = record(&rig);
        fetch(&rig).await;
        assert!(sent.borrow().is_empty());
        assert_eq!(rig.state().sync.fetch_attempts, 0);
    })
    .await;
}

#[tokio::test]
async fn fetches_the_deduped_set_once_writes_and_ingests_bodies() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![
                live("a", &[("thing:1", 1), ("thing:2", 1)]),
                live("b", &[("thing:2", 3)]),
            ],
            primed,
        );
        let sent = answer(&rig, |_, _| Ok(bodies(&["thing:1", "thing:2"])));
        fetch(&rig).await;
        {
            let sent = sent.borrow();
            assert_eq!(sent.len(), 1, "a row shared by two queries is fetched once");
            assert_eq!(sent[0].sql, "SELECT * FROM $ids");
            let ids = sent[0].vars.as_ref().unwrap()["ids"]
                .as_array()
                .unwrap()
                .clone();
            assert_eq!(ids.len(), 2);
            assert!(ids.iter().all(|v| matches!(v, Value::RecordId(_))));
        }
        assert_eq!(rig.log.count("local.tx"), 1);
        // The highest requested version wins and lands on the body.
        let second = rig.local.row("thing", "thing:2").unwrap();
        assert_eq!(second["_00_rv"], Value::Int(3));
        assert!(
            !second.contains_key("serverOnly"),
            "the schema strips undeclared fields"
        );
        let st = rig.state();
        assert_eq!(
            versions(&rig),
            [("thing:1".to_string(), 1), ("thing:2".to_string(), 3)].into()
        );
        assert_eq!(rig.log.count("ssp.ingest"), 1);
        assert!(rig
            .ssp
            .ingested
            .borrow()
            .iter()
            .all(|r| r.op == IngestOp::Create));
        for e in st.queries.values() {
            assert_eq!(e.lifecycle.fetch_depth, 0);
        }
        rig.settle().await;
        assert!(rig.state().sync.has_synced_once);
    })
    .await;
}

#[tokio::test]
async fn a_body_already_known_is_ingested_as_an_update() {
    run_local(async {
        let (rig, _) = rig_of(vec![live("a", &[("thing:1", 2)])], |s| {
            primed(s);
            s.versions.insert("thing:1".into(), 1);
        });
        answer(&rig, |_, _| Ok(bodies(&["thing:1"])));
        fetch(&rig).await;
        let ingested = rig.ssp.ingested.borrow();
        assert_eq!(ingested.len(), 1);
        assert_eq!(ingested[0].op, IngestOp::Update);
        assert_eq!(ingested[0].record["_00_rv"], Value::Int(2));
    })
    .await;
}

#[tokio::test]
async fn a_body_the_server_never_returns_is_remembered_at_the_asked_version() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![live("a", &[("thing:1", 1), ("thing:gone", 4)])],
            primed,
        );
        let sent = answer(&rig, |_, _| Ok(bodies(&["thing:1"])));
        fetch(&rig).await;
        // Without this the plan would ask for `thing:gone` forever.
        assert_eq!(versions(&rig).get("thing:gone"), Some(&4));
        assert_eq!(sent.borrow().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn failures_back_off_and_stop_the_loop() {
    run_local(async {
        let failures: Vec<Box<dyn Fn() -> crate::error::Result<Vec<StatementResult>>>> = vec![
            Box::new(|| Err(Error::Network("socket closed".into()))),
            Box::new(|| Ok(results(vec![Err("permission denied")]))),
            Box::new(|| Ok(results(vec![Ok(json!("not a list"))]))),
            Box::new(|| Ok(Vec::new())),
        ];
        for fail in failures {
            let (rig, _) = rig_of(vec![live("a", &[("thing:1", 1)])], primed);
            let fail = std::rc::Rc::new(fail);
            let sent = answer(&rig, move |_, _| fail());
            fetch(&rig).await;
            let st = rig.state();
            assert_eq!(st.sync.fetch_attempts, 1);
            assert_eq!(rig.timers.delay("fetch"), Some(500));
            assert_eq!(st.queries["a"].lifecycle.fetch_depth, 0);
            assert_eq!(sent.borrow().len(), 1, "the loop stops");
            rig.settle().await;
            assert_eq!(rig.state().sync.consecutive_failures, 1);

            // The armed retry backs off further when it fails again.
            assert!(rig.timers.fire("fetch"));
            rig.settle().await;
            assert_eq!(rig.state().sync.fetch_attempts, 2);
            assert_eq!(rig.timers.delay("fetch"), Some(1000));
        }
    })
    .await;
}

#[tokio::test]
async fn a_failing_local_write_is_a_failure_a_failing_ingest_only_logged() {
    run_local(async {
        let (rig, _) = rig_of(vec![live("a", &[("thing:1", 1)])], primed);
        answer(&rig, |_, _| Ok(bodies(&["thing:1"])));
        *rig.local.fail_writes.borrow_mut() = Some("disk full".into());
        fetch(&rig).await;
        assert!(rig.timers.has("fetch"));
        assert!(versions(&rig).is_empty());

        let (rig, hooks) = rig_of(vec![live("a", &[("thing:1", 1)])], primed);
        answer(&rig, |_, _| Ok(bodies(&["thing:1"])));
        Hooks::set(&hooks.fail_ingest, "circuit busy");
        let (_sub, seen) = events(&rig);
        fetch(&rig).await;
        assert!(!rig.timers.has("fetch"));
        assert_eq!(versions(&rig).get("thing:1"), Some(&1));
        let logs = logs(&seen);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].0, LogLevel::Warn);
        assert_eq!(logs[0].1, "circuit ingest failed");
    })
    .await;
}

#[tokio::test]
async fn subquery_children_are_fetched_without_holding_fetching() {
    run_local(async {
        let (rig, _) = rig_of(vec![live("a", &[])], |s| {
            primed(s);
            s.queries.get_mut("a").unwrap().subquery_remote_array = pairs(&[("child:1", 2)]);
        });
        let sent = answer(&rig, |_, _| {
            Ok(results(vec![Ok(json!([{"id": "child:1", "name": "c"}]))]))
        });
        fetch(&rig).await;
        assert_eq!(sent.borrow().len(), 1);
        assert_eq!(versions(&rig).get("child:1"), Some(&2));
        // `child` declares no columns, so the body is stored whole.
        assert_eq!(
            rig.local.row("child", "child:1").unwrap()["name"],
            Value::from("c")
        );
    })
    .await;
}

fn optional_schema() -> SagaEnv {
    let schema = Schema::default().with_table(
        "thing",
        [
            ("title", ColumnSchema::of_type("string")),
            (
                "club",
                ColumnSchema {
                    record_id: true,
                    optional: true,
                    ..Default::default()
                },
            ),
            (
                "note",
                ColumnSchema {
                    kind: Some("string".into()),
                    optional: true,
                    ..Default::default()
                },
            ),
        ],
    );
    SagaEnv::new(Arc::new(schema))
}

#[tokio::test]
async fn names_absent_optional_columns_on_the_merge_op_not_in_the_body() {
    run_local(async {
        let mut state = ClientState::empty("tab");
        primed(&mut state);
        let e = live("a", &[("thing:1", 2)]);
        state.queries.insert("a".into(), e);
        let (rig, _) = rig_with(optional_schema(), state);
        answer(&rig, |_, _| {
            Ok(results(vec![Ok(
                json!([{"id": "thing:1", "title": "t", "note": "n"}]),
            )]))
        });
        fetch(&rig).await;
        let tx = rig.log.named("local.tx");
        assert_eq!(tx.len(), 1);
        assert!(tx[0].args.contains("mode: Merge"), "{}", tx[0].args);
        assert!(tx[0].args.contains("clear: [\"club\"]"), "{}", tx[0].args);
        let ingested = rig.ssp.ingested.borrow();
        assert!(
            !ingested[0].record.contains_key("club"),
            "the body doubles as the circuit record: no null placeholders"
        );
    })
    .await;
}

#[tokio::test]
async fn a_landed_body_removes_a_field_cleared_upstream_from_the_store() {
    run_local(async {
        let mut state = ClientState::empty("tab");
        primed(&mut state);
        state.queries.insert("a".into(), live("a", &[("thing:1", 2)]));
        let (rig, _) = rig_with(optional_schema(), state);
        rig.local.seed(
            "thing",
            "thing:1",
            row(json!({"id": "thing:1", "title": "old", "club": "club:9", "_00_rv": 1, "_00_crdt": "blob"})),
        );
        answer(&rig, |_, _| Ok(results(vec![Ok(json!([{"id": "thing:1", "title": "new"}]))])));
        fetch(&rig).await;
        let stored = rig.local.row("thing", "thing:1").unwrap();
        assert!(!stored.contains_key("club"));
        assert_eq!(stored["title"], Value::from("new"));
        assert_eq!(stored["_00_rv"], Value::Int(2));
        assert_eq!(stored["_00_crdt"], Value::from("blob"), "local bookkeeping survives");
    })
    .await;
}

#[tokio::test]
async fn land_chunk_is_fenced_to_the_epoch_it_was_planned_for() {
    run_local(async {
        let (rig, _) = new_rig();
        let env = rig.env();
        let stale = crate::kernel::ports::LocalPort::epoch(&rig.local);
        rig.local.bump_epoch();
        let rows = vec![row(json!({"id": "thing:1", "title": "t"}))];
        let versions_in: BTreeMap<String, i64> = [("thing:1".to_string(), 1)].into();
        rig.run(|ctx| async move {
            land_chunk(
                &ctx,
                &env,
                &["thing:1".to_string()],
                rows,
                &versions_in,
                stale,
            )
            .await
        })
        .await
        .unwrap();
        assert!(
            rig.local.row("thing", "thing:1").is_none(),
            "the write was dropped"
        );
    })
    .await;
}
