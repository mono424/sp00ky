use std::cell::RefCell;
use std::rc::Rc;

use serde_json::json;

use super::*;
use crate::error::Error;
use crate::kernel::events::OutEvent;
use crate::modules::query_builder::RelationPlan;
use crate::query::test_support::*;
use crate::state::lifecycle::{is_authoritative, QueryPhase, RemotePhase};
use crate::state::selectors::settled;
use crate::state::ClientState;
use crate::testing::{results, row, run_local};
use crate::types::{Cardinality, QueryStatus};
use crate::value::{sha256_hex, Value};

fn input() -> RegisterInput {
    RegisterInput {
        table_name: "thing".into(),
        surql: "SELECT * FROM thing".into(),
        params: Object::new(),
        ttl: "10m".into(),
        has_explicit_order: false,
        relations: Vec::new(),
    }
}

fn view_key() -> String {
    let i = input();
    sha256_hex(&view_key_input(&QueryKeyInput {
        surql: &i.surql,
        params: &i.params,
    }))
}

fn owner_plan() -> RelationPlan {
    RelationPlan {
        alias: "owner".into(),
        table: "user".into(),
        cardinality: Cardinality::One,
        foreign_key_field: "owner".into(),
        select: Vec::new(),
        where_: Object::new(),
        order_by: Vec::new(),
        limit: None,
        relations: Vec::new(),
    }
}

fn is_register(sent: &Sent) -> bool {
    sent.sql.starts_with("fn::query::register")
}

fn registered_hashes(sent: &SentLog) -> Vec<String> {
    sent.borrow()
        .iter()
        .filter(|s| is_register(s))
        .map(|s| s.vars.as_ref().unwrap()["in"].to_id_string())
        .collect()
}

async fn local(rig: &crate::testing::TestRig, input: RegisterInput) -> Result<QueryHash> {
    let env = rig.env();
    rig.run(|ctx| async move { register_local(&ctx, &env, input).await })
        .await
}

async fn remote(rig: &crate::testing::TestRig, hash: &str, retry: bool) {
    let env = rig.env();
    let hash = hash.to_string();
    rig.run(|ctx| async move { register_remote(&ctx, &env, &hash, retry).await })
        .await
        .unwrap();
}

async fn ensure(rig: &crate::testing::TestRig, require_auth: bool, attempt: u32) {
    let env = rig.env();
    rig.run(|ctx| async move { ensure_registered(&ctx, &env, require_auth, attempt).await })
        .await
        .unwrap();
}

// ---- register_local -------------------------------------------------------------

#[tokio::test]
async fn cold_registration_builds_an_entry_and_registers_remotely() {
    run_local(async {
        let (rig, _) = new_rig();
        rig.ssp.local_arrays(|_| vec![("thing:1".into(), 1)]);
        let sent = record(&rig);
        let hash = local(&rig, input()).await.unwrap();
        let st = rig.state();
        let e = &st.queries[&hash];
        assert_eq!(e.lifecycle.phase, QueryPhase::Cold);
        assert_eq!(e.local_array, pairs(&[("thing:1", 1)]));
        assert!(e.remote_array.is_empty());
        assert_eq!(e.def.table_name, "thing");
        assert_eq!(e.def.ttl_ms, 600_000);
        assert_eq!(e.def.id.to_string(), format!("_00_query:{hash}"));
        assert_eq!(e.def.view_key, view_key());
        assert_eq!(e.telemetry.registration_timings.parse_ms, Some(1.0));
        assert_eq!(e.last_subscriber_left_at, Some(rig.now.get()));
        assert!(st.registering.is_empty());
        // The durable row is read by (table, id), not by a SurrealQL string.
        let read = rig.log.named("local.get");
        assert_eq!(read.len(), 1);
        assert!(read[0].args.contains("\"_00_view\""), "{}", read[0].args);
        // EnsureRegistered was dispatched, not awaited.
        assert!(sent.borrow().is_empty());
        rig.settle().await;
        assert_eq!(registered_hashes(&sent), vec![format!("_00_query:{hash}")]);
    })
    .await;
}

#[tokio::test]
async fn a_durable_view_row_seeds_membership() {
    run_local(async {
        let (rig, _) = new_rig();
        rig.local.seed(
            "_00_view",
            &sql::view_record_id(&view_key()),
            row(json!({"ids": [["thing:1", 2]], "confirmed": true})),
        );
        let hash = local(&rig, input()).await.unwrap();
        let e = &rig.state().queries[&hash];
        assert_eq!(e.lifecycle.phase, QueryPhase::Cached);
        assert_eq!(e.remote_array, pairs(&[("thing:1", 2)]));
        assert!(is_authoritative(&e.lifecycle));
    })
    .await;
}

#[tokio::test]
async fn an_unconfirmed_empty_row_or_a_failed_read_stays_cold() {
    run_local(async {
        let (rig, _) = new_rig();
        rig.local.seed(
            "_00_view",
            &sql::view_record_id(&view_key()),
            row(json!({"ids": [], "confirmed": false})),
        );
        let hash = local(&rig, input()).await.unwrap();
        assert_eq!(rig.state().queries[&hash].lifecycle.phase, QueryPhase::Cold);

        let (rig, hooks) = new_rig();
        rig.local.seed(
            "_00_view",
            &sql::view_record_id(&view_key()),
            row(json!({"ids": [["thing:1", 2]], "confirmed": true})),
        );
        Hooks::set(&hooks.fail_get, "store is busy");
        let hash = local(&rig, input()).await.unwrap();
        assert_eq!(rig.state().queries[&hash].lifecycle.phase, QueryPhase::Cold);
    })
    .await;
}

#[tokio::test]
async fn an_active_query_returns_without_registering_again() {
    run_local(async {
        let (rig, _) = new_rig();
        let first = local(&rig, input()).await.unwrap();
        let second = local(&rig, input()).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(rig.log.count("ssp.register"), 1);
    })
    .await;
}

#[tokio::test]
async fn the_session_salts_the_hash_but_not_the_view_key() {
    run_local(async {
        let (rig, _) = new_rig();
        let anon = local(&rig, input()).await.unwrap();
        rig.runtime.update(|s| s.session_id = Some("s1".into()));
        let salted = local(&rig, input()).await.unwrap();
        assert_ne!(anon, salted);
        let st = rig.state();
        assert_eq!(
            st.queries[&anon].def.view_key,
            st.queries[&salted].def.view_key
        );
    })
    .await;
}

#[tokio::test]
async fn a_plan_less_first_registration_takes_the_plan_of_a_later_one() {
    run_local(async {
        let (rig, _) = new_rig();
        // A preload registers first and carries no plan.
        let hash = local(&rig, input()).await.unwrap();
        rig.runtime.update(|s| s.dirty.clear());
        let with_plan = RegisterInput {
            relations: vec![owner_plan()],
            ..input()
        };
        assert_eq!(local(&rig, with_plan).await.unwrap(), hash);
        assert_eq!(rig.log.count("ssp.register"), 1);
        let st = rig.state();
        assert_eq!(st.queries[&hash].def.relations, vec![owner_plan()]);
        assert!(
            st.dirty.contains(&hash),
            "re-materialize so the joins appear"
        );

        // A plan already there is never replaced by a plan-less registrant.
        rig.runtime.update(|s| s.dirty.clear());
        local(&rig, input()).await.unwrap();
        let st = rig.state();
        assert_eq!(st.queries[&hash].def.relations, vec![owner_plan()]);
        assert!(st.dirty.is_empty());
    })
    .await;
}

#[tokio::test]
async fn a_registrant_waits_for_one_in_flight() {
    run_local(async {
        let (rig, _) = new_rig();
        let i = input();
        let hash = sha256_hex(&query_hash_input(
            &QueryKeyInput {
                surql: &i.surql,
                params: &i.params,
            },
            None,
        ));
        rig.runtime.update(|s| r::begin_registering(s, &hash));
        let rt = rig.runtime.clone();
        let env = rig.env();
        let waiting = tokio::task::spawn_local(async move {
            rt.run(|ctx| async move { register_local(&ctx, &env, input()).await })
                .await
        });
        rig.settle().await;
        assert!(!waiting.is_finished());
        rig.runtime.update(|s| {
            r::put_query(
                s,
                entry(&hash, QueryPhase::Cold, RemotePhase::Unregistered, &[]),
            );
            r::end_registering(s, &hash);
        });
        assert_eq!(waiting.await.unwrap().unwrap(), hash);
        assert_eq!(rig.log.count("ssp.register"), 0);
    })
    .await;
}

#[tokio::test]
async fn a_failing_circuit_registration_clears_the_marker_and_fails() {
    run_local(async {
        let (rig, hooks) = new_rig();
        Hooks::set(&hooks.fail_register, "circuit denied");
        assert!(local(&rig, input()).await.is_err());
        let st = rig.state();
        assert!(st.registering.is_empty());
        assert!(st.queries.is_empty());
    })
    .await;
}

// ---- ensure_registered --------------------------------------------------------------

#[tokio::test]
async fn ensure_registers_unregistered_queries_only() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![
                entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[]),
                entry("b", QueryPhase::Live, RemotePhase::Registered, &[]),
            ],
            |_| {},
        );
        let sent = record(&rig);
        ensure(&rig, false, 0).await;
        rig.settle().await;
        assert_eq!(registered_hashes(&sent), vec!["_00_query:a"]);
    })
    .await;
}

#[tokio::test]
async fn require_auth_probes_the_identity_and_retries_on_a_timer() {
    run_local(async {
        let signed_in = |s: &mut ClientState| s.user_id = Some("user:u1".into());
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            signed_in,
        );
        let sent = answer(&rig, |_, _| Ok(results(vec![Ok(serde_json::Value::Null)])));
        ensure(&rig, true, 0).await;
        assert_eq!(
            rig.timers.delay("ensure-registered"),
            Some(AUTH_READY_RETRY_MS)
        );
        rig.settle().await;
        assert_eq!(sent.borrow().len(), 1);
        assert_eq!(sent.borrow()[0].sql, "RETURN $auth.id");
        // The armed retry probes again.
        assert!(rig.timers.fire("ensure-registered"));
        rig.settle().await;
        assert_eq!(sent.borrow().len(), 2);
        assert!(registered_hashes(&sent).is_empty());

        // Out of attempts: logged, no timer.
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            signed_in,
        );
        answer(&rig, |_, _| Err(Error::Network("offline".into())));
        let (_sub, seen) = events(&rig);
        ensure(&rig, true, AUTH_READY_MAX_ATTEMPTS - 1).await;
        assert!(!rig.timers.has("ensure-registered"));
        let warns = logs(&seen);
        assert_eq!(warns.len(), 1);
        assert_eq!(warns[0].0, LogLevel::Warn);

        // A visible identity registers.
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            signed_in,
        );
        let sent = answer(&rig, |sql, _| {
            Ok(if sql == "RETURN $auth.id" {
                results(vec![Ok(json!("user:u1"))])
            } else {
                Vec::new()
            })
        });
        ensure(&rig, true, 0).await;
        rig.settle().await;
        assert_eq!(registered_hashes(&sent), vec!["_00_query:a"]);
    })
    .await;
}

#[tokio::test]
async fn an_anonymous_session_skips_the_identity_gate() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let sent = record(&rig);
        ensure(&rig, true, 0).await;
        rig.settle().await;
        assert!(sent.borrow().iter().all(|s| s.sql != "RETURN $auth.id"));
        assert_eq!(registered_hashes(&sent), vec!["_00_query:a"]);
    })
    .await;
}

// ---- register_remote ----------------------------------------------------------------

#[tokio::test]
async fn a_cold_registration_is_foreground_a_cached_or_live_one_background() {
    run_local(async {
        for (phase, background) in [
            (QueryPhase::Cold, 0),
            (QueryPhase::Cached, 1),
            (QueryPhase::Live, 1),
        ] {
            let (rig, _) = rig_of(
                vec![entry("a", phase, RemotePhase::Unregistered, &[])],
                |_| {},
            );
            let seen: Rc<RefCell<Vec<(u32, u32)>>> = Rc::default();
            let s2 = seen.clone();
            let weak = rig.runtime.downgrade();
            answer(&rig, move |_, _| {
                let l = weak.upgrade().unwrap().read(|s| s.queries["a"].lifecycle);
                s2.borrow_mut().push((l.fetch_depth, l.background_depth));
                Ok(register_answer(&[("thing:1", 1)], ready_meta(1), &[]))
            });
            remote(&rig, "a", false).await;
            assert_eq!(*seen.borrow(), vec![(1, background)], "{phase:?}");
            let l = rig.state().queries["a"].lifecycle;
            assert_eq!((l.fetch_depth, l.background_depth), (0, 0));
        }
    })
    .await;
}

#[tokio::test]
async fn sends_the_owner_secret_beside_params_not_inside_them() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| s.owner_secret = Some("owner-k".into()),
        );
        let sent = answer(&rig, |_, _| Ok(register_answer(&[], ready_meta(0), &[])));
        remote(&rig, "a", false).await;
        let sent = sent.borrow();
        let config = sent[0].vars.as_ref().unwrap()["config"]
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(config["owner"], Value::from("owner-k"));
        assert!(!config["params"].as_object().unwrap().contains_key("owner"));
        assert!(matches!(
            sent[0].vars.as_ref().unwrap()["in"],
            Value::RecordId(_)
        ));
    })
    .await;
}

#[tokio::test]
async fn registers_applies_membership_and_children_and_flips_registered() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        answer(&rig, |_, _| {
            Ok(register_answer(
                &[("thing:1", 1)],
                ready_meta(1),
                &[("child:1", 1)],
            ))
        });
        let (_sub, seen) = events(&rig);
        remote(&rig, "a", false).await;
        let st = rig.state();
        let e = &st.queries["a"];
        assert_eq!(e.lifecycle.remote, RemotePhase::Registered);
        assert_eq!(e.lifecycle.phase, QueryPhase::Live);
        assert_eq!(e.remote_array, pairs(&[("thing:1", 1)]));
        assert_eq!(e.subquery_remote_array, pairs(&[("child:1", 1)]));
        assert_eq!(e.lifecycle.fetch_depth, 0, "begin/end are balanced");
        assert!(seen
            .borrow()
            .iter()
            .any(|e| matches!(e, OutEvent::QueryAuthority { known: true, .. })));
        // The durable row's last write carries the children, so the orphan
        // collector keeps their bodies.
        let view = rig.local.row("_00_view", "_00_view:view-a").unwrap();
        assert_eq!(view["confirmed"], Value::Bool(true));
        assert_eq!(view["ids"], Value::from(json!([["thing:1", 1]])));
        assert_eq!(view["children"], Value::from(json!([["child:1", 1]])));
        rig.settle().await;
        assert!(rig.state().sync.has_synced_once, "the round reported ok");
    })
    .await;
}

#[tokio::test]
async fn guards_skip_missing_registered_failed_and_in_flight_queries() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let sent = record(&rig);
        remote(&rig, "nope", false).await;
        assert!(sent.borrow().is_empty());

        for phase in [
            RemotePhase::Registered,
            RemotePhase::Failed,
            RemotePhase::Registering,
        ] {
            let (rig, _) = rig_of(vec![entry("a", QueryPhase::Live, phase, &[])], |_| {});
            let sent = record(&rig);
            remote(&rig, "a", false).await;
            assert!(sent.borrow().is_empty(), "{phase:?}");
        }

        // The armed retry gets past the in-flight guard.
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Registering, &[])],
            |_| {},
        );
        let sent = record(&rig);
        remote(&rig, "a", true).await;
        assert_eq!(sent.borrow().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn a_materializing_answer_schedules_the_reread_ladder() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        answer(&rig, |_, _| {
            Ok(register_answer(
                &[],
                json!({"rowCount": 3, "state": "materializing"}),
                &[],
            ))
        });
        remote(&rig, "a", false).await;
        let st = rig.state();
        assert!(st.membership_dirty.contains("a"));
        assert_eq!(st.membership_reread.get("a"), Some(&1));
        assert_eq!(rig.timers.delay("membership"), Some(150));
        assert_eq!(st.queries["a"].lifecycle.phase, QueryPhase::Cold);
    })
    .await;
}

#[tokio::test]
async fn errors_back_off_and_the_budget_exhausts_to_failed() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let sent = answer(&rig, |_, _| Err(Error::Network("socket closed".into())));
        remote(&rig, "a", false).await;
        let st = rig.state();
        assert_eq!(st.queries["a"].register_attempts, 1);
        // The phase stays `registering`: the armed retry passes `retry: true`,
        // which is what gets past the in-flight guard.
        assert_eq!(st.queries["a"].lifecycle.remote, RemotePhase::Registering);
        assert_eq!(st.queries["a"].lifecycle.fetch_depth, 0);
        assert_eq!(rig.timers.delay("register:a"), Some(1000));
        rig.settle().await;
        assert_eq!(rig.state().sync.consecutive_failures, 1);
        assert_eq!(rig.state().sync.health.kind.as_deref(), Some("network"));

        // Firing the retry registers again and backs off further.
        assert!(rig.timers.fire("register:a"));
        rig.settle().await;
        assert_eq!(sent.borrow().len(), 2);
        assert_eq!(rig.state().queries["a"].register_attempts, 2);
        assert_eq!(rig.timers.delay("register:a"), Some(2000));

        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| {
                s.queries.get_mut("a").unwrap().register_attempts = REGISTER_MAX_RETRIES - 1;
            },
        );
        answer(&rig, |_, _| Err(Error::Network("socket closed".into())));
        remote(&rig, "a", false).await;
        assert_eq!(
            rig.state().queries["a"].lifecycle.remote,
            RemotePhase::Failed
        );
        assert!(!rig.timers.has("register:a"));
    })
    .await;
}

#[tokio::test]
async fn a_failed_register_statement_is_an_error_even_with_ok_edges() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        answer(&rig, |_, _| {
            let mut out = results(vec![Err("permission denied")]);
            out.extend(snapshot(&[("thing:1", 1)], ready_meta(1), &[]));
            Ok(out)
        });
        remote(&rig, "a", false).await;
        let e = &rig.state().queries["a"];
        assert_eq!(e.register_attempts, 1);
        assert!(e.remote_array.is_empty());
    })
    .await;
}

#[tokio::test]
async fn an_unanswered_read_back_rereads_instead_of_reading_a_lost_row() {
    run_local(async {
        for read_back in [
            vec![Ok(edges(&[("thing:1", 1)])), Err("timeout"), Ok(json!([]))],
            vec![
                Ok(edges(&[("thing:1", 1)])),
                Ok(ready_meta(1)),
                Err("no children"),
            ],
        ] {
            let (rig, _) = rig_of(
                vec![entry(
                    "a",
                    QueryPhase::ViewLost,
                    RemotePhase::Unregistered,
                    &[("thing:1", 1)],
                )],
                |_| {},
            );
            let read_back = Rc::new(read_back);
            let sent = answer(&rig, move |_, _| {
                let mut out = results(vec![Ok(serde_json::Value::Null)]);
                out.extend(results((*read_back).clone()));
                Ok(out)
            });
            remote(&rig, "a", false).await;
            let st = rig.state();
            let e = &st.queries["a"];
            assert_eq!(e.lifecycle.phase, QueryPhase::ViewLost);
            assert_eq!(e.lifecycle.remote, RemotePhase::Registered);
            assert_eq!(e.register_attempts, 0);
            assert!(st.membership_dirty.contains("a"));
            assert!(rig.timers.has("membership"));
            rig.settle().await;
            assert_eq!(sent.borrow().len(), 1, "no re-registration");
            assert_eq!(rig.state().sync.consecutive_failures, 1);
            // The armed read is the membership re-read.
            assert!(rig.timers.fire("membership"));
            rig.settle().await;
            assert!(sent.borrow()[1]
                .sql
                .starts_with("SELECT out, version FROM $in->"));
        }
    })
    .await;
}

// ---- the settled contract (end to end through the runtime) -----------------------------

/// A registration holds `fetching` across its whole remote cycle and settles
/// only once its rows have landed and rendered.
#[tokio::test]
async fn a_registration_settles_once_its_rows_have_landed() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), primed);
        answer(&rig, |sql, _| {
            Ok(if sql.starts_with("fn::query::register") {
                register_answer(&[("thing:a", 1)], ready_meta(1), &[])
            } else if sql == sql::body_select() {
                results(vec![Ok(json!([{"id": "thing:a", "title": "from server"}]))])
            } else {
                Vec::new()
            })
        });
        let hash = local(&rig, input()).await.unwrap();
        assert!(!rig
            .runtime
            .read(|s| is_authoritative(&s.queries[&hash].lifecycle)));
        let observed: Rc<RefCell<Vec<QueryStatus>>> = Rc::default();
        let o2 = observed.clone();
        let _status =
            rig.runtime
                .subscribe_status(&hash, move |st| o2.borrow_mut().push(st), false);
        for _ in 0..5 {
            rig.settle().await;
            rig.timers.fire_prefix("mat:");
        }
        rig.settle().await;
        assert!(observed.borrow().contains(&QueryStatus::Fetching));
        assert_eq!(observed.borrow().last(), Some(&QueryStatus::Idle));
        let st = rig.state();
        assert!(is_authoritative(&st.queries[&hash].lifecycle));
        assert!(settled(&st, &hash));
        assert_eq!(st.queries[&hash].records.len(), 1);
        assert_eq!(
            st.queries[&hash].records[0]["title"],
            Value::from("from server")
        );
    })
    .await;
}

#[tokio::test]
async fn a_confirmed_empty_result_is_authoritative_and_settles() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), primed);
        answer(&rig, |sql, _| {
            Ok(if sql.starts_with("fn::query::register") {
                register_answer(&[], ready_meta(0), &[])
            } else {
                Vec::new()
            })
        });
        let (_sub, seen) = events(&rig);
        let hash = local(&rig, input()).await.unwrap();
        for _ in 0..3 {
            rig.settle().await;
            rig.timers.fire_prefix("mat:");
        }
        rig.settle().await;
        let st = rig.state();
        assert!(is_authoritative(&st.queries[&hash].lifecycle));
        assert!(settled(&st, &hash));
        assert!(st.queries[&hash].records.is_empty());
        // The settled empty set is emitted, so a binding can stop loading.
        assert!(count_kind(&seen, "query:records") >= 1);
    })
    .await;
}

#[tokio::test]
async fn an_unpublished_view_is_not_read_as_empty() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), primed);
        answer(&rig, |sql, _| {
            Ok(if sql.starts_with("fn::query::register") {
                register_answer(&[], serde_json::Value::Null, &[])
            } else {
                Vec::new()
            })
        });
        let hash = local(&rig, input()).await.unwrap();
        for _ in 0..3 {
            rig.settle().await;
            rig.timers.fire_prefix("mat:");
        }
        let st = rig.state();
        assert!(!is_authoritative(&st.queries[&hash].lifecycle));
        assert_eq!(st.queries[&hash].lifecycle.phase, QueryPhase::Cold);
    })
    .await;
}
