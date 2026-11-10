use std::cell::Cell;
use std::rc::Rc;

use serde_json::json;

use super::*;
use crate::error::Error;
use crate::kernel::ports::TimerPort;
use crate::query::test_support::*;
use crate::state::ClientState;
use crate::testing::{build_outbox_item, results, run_local, TestRig};
use crate::types::MutationEventType;

async fn apply(
    rig: &TestRig,
    hash: &str,
    remote: &[(&str, i64)],
    meta: Option<ServerViewMeta>,
    verified: bool,
) -> MembershipOutcome {
    let hash = hash.to_string();
    let remote = pairs(remote);
    rig.run(
        |ctx| async move { apply_membership(&ctx, &hash, remote, meta.as_ref(), verified).await },
    )
    .await
    .unwrap()
}

async fn read(rig: &TestRig, hashes: &[&str]) -> MembershipRead {
    let env = rig.env();
    let hashes: Vec<String> = hashes.iter().map(|h| h.to_string()).collect();
    rig.run(|ctx| async move { read_membership(&ctx, &env, &hashes, true).await })
        .await
        .unwrap()
}

fn lost(count: u32, retry_at: Option<i64>) -> impl FnOnce(&mut ClientState) {
    move |s| {
        let e = s.queries.get_mut("a").unwrap();
        e.view_lost_count = count;
        e.view_lost_retry_at = retry_at;
    }
}

fn register_requests(sent: &SentLog) -> usize {
    sent.borrow()
        .iter()
        .filter(|s| s.sql.starts_with("fn::query::register"))
        .count()
}

fn body_requests(sent: &SentLog) -> usize {
    sent.borrow()
        .iter()
        .filter(|s| s.sql == sql::body_select())
        .count()
}

fn absent() -> Option<ServerViewMeta> {
    Some(ServerViewMeta::ABSENT)
}

// ---- apply_membership -----------------------------------------------------------

#[tokio::test]
async fn an_unknown_hash_or_an_unexplained_empty_set_is_ignored() {
    run_local(async {
        let (rig, _) = new_rig();
        assert_eq!(
            apply(&rig, "nope", &[], None, false).await,
            MembershipOutcome::Ignored
        );

        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let (_sub, seen) = events(&rig);
        assert_eq!(
            apply(&rig, "a", &[], None, false).await,
            MembershipOutcome::Ignored
        );
        assert!(seen.borrow().is_empty());
    })
    .await;
}

#[tokio::test]
async fn view_lost_flips_once_drops_the_registration_and_reregisters() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            |_| {},
        );
        let sent = record(&rig);
        let (_sub, seen) = events(&rig);
        assert_eq!(
            apply(&rig, "a", &[], absent(), false).await,
            MembershipOutcome::ViewLost
        );
        let st = rig.state();
        let e = &st.queries["a"];
        assert_eq!(e.lifecycle.phase, QueryPhase::ViewLost);
        assert_eq!(e.lifecycle.remote, RemotePhase::Unregistered);
        assert_eq!(e.view_lost_count, 1);
        // The rows are KEPT: a lost view is not an empty one.
        assert_eq!(e.remote_array, pairs(&[("thing:1", 1)]));
        assert_eq!(count_kind(&seen, "query:view-lost"), 1);
        rig.settle().await;
        assert_eq!(register_requests(&sent), 1, "EnsureRegistered re-registers");
    })
    .await;
}

#[tokio::test]
async fn view_lost_again_before_the_row_is_seen_is_paced() {
    run_local(async {
        let lost_entry = || {
            entry(
                "a",
                QueryPhase::ViewLost,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )
        };
        let (rig, _) = rig_of(vec![lost_entry()], lost(1, None));
        let sent = record(&rig);
        let (_sub, seen) = events(&rig);
        rig.set_now(1000);
        assert_eq!(
            apply(&rig, "a", &[], None, false).await,
            MembershipOutcome::ViewLost
        );
        let e = rig.state().queries["a"].clone();
        assert_eq!(e.view_lost_count, 2);
        assert_eq!(e.view_lost_retry_at, Some(2000));
        assert_eq!(e.lifecycle.remote, RemotePhase::Registered);
        assert_eq!(rig.timers.delay("view-lost:a"), Some(1000));
        assert_eq!(count_kind(&seen, "query:view-lost"), 0);
        rig.settle().await;
        assert_eq!(register_requests(&sent), 0);

        // Reads in between do nothing: the recovery is already scheduled.
        TimerPort::clear(&rig.timers, "view-lost:a");
        rig.set_now(1500);
        apply(&rig, "a", &[], None, false).await;
        assert!(!rig.timers.has("view-lost:a"));
        assert_eq!(rig.state().queries["a"].view_lost_retry_at, Some(2000));

        // The paced recovery re-registers.
        let (rig, _) = rig_of(vec![lost_entry()], lost(3, None));
        apply(&rig, "a", &[], None, false).await;
        assert_eq!(rig.timers.delay("view-lost:a"), Some(4000));
    })
    .await;
}

#[tokio::test]
async fn seeing_the_row_again_resets_the_pacing() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::ViewLost,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            lost(3, Some(9000)),
        );
        let meta = ServerViewMeta {
            present: true,
            row_count: Some(1),
            state: Some("materializing".into()),
        };
        assert_eq!(
            apply(&rig, "a", &[], Some(meta), false).await,
            MembershipOutcome::Ignored
        );
        let e = &rig.state().queries["a"];
        assert_eq!(e.view_lost_count, 0);
        assert_eq!(e.view_lost_retry_at, None);
        assert_eq!(e.server_state, Some(ServerViewState::Materializing));
    })
    .await;
}

#[tokio::test]
async fn recover_lost_view_reregisters_only_while_still_lost_and_scheduled() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::ViewLost,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            lost(2, Some(2000)),
        );
        let sent = record(&rig);
        rig.run(|ctx| async move { recover_lost_view(&ctx, "a").await })
            .await
            .unwrap();
        let e = rig.state().queries["a"].clone();
        assert_eq!(e.view_lost_retry_at, None);
        assert_eq!(e.view_lost_count, 2);
        assert_eq!(e.lifecycle.remote, RemotePhase::Unregistered);
        rig.settle().await;
        assert_eq!(register_requests(&sent), 1);

        // Through the timer the view-lost path armed.
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::ViewLost,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            lost(1, None),
        );
        let sent = record(&rig);
        apply(&rig, "a", &[], None, false).await;
        assert!(rig.timers.fire("view-lost:a"));
        rig.settle().await;
        assert_eq!(register_requests(&sent), 1);

        for (phase, retry_at) in [(QueryPhase::ViewLost, None), (QueryPhase::Live, Some(2000))] {
            let (rig, _) = rig_of(
                vec![entry(
                    "a",
                    phase,
                    RemotePhase::Registered,
                    &[("thing:1", 1)],
                )],
                lost(2, retry_at),
            );
            let sent = record(&rig);
            rig.run(|ctx| async move { recover_lost_view(&ctx, "a").await })
                .await
                .unwrap();
            rig.settle().await;
            assert_eq!(register_requests(&sent), 0, "{phase:?}");
            assert_eq!(
                rig.state().queries["a"].lifecycle.remote,
                RemotePhase::Registered
            );
        }
    })
    .await;
}

#[tokio::test]
async fn applied_commits_writes_the_view_row_flips_authority_once_and_fetches() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| {
                primed(s);
                let mut acked = build_outbox_item("m1", MutationEventType::Update, "thing:1");
                acked.status = crate::state::OutboxStatus::Acked;
                acked.acked_at = Some(1);
                s.outbox.push(acked);
            },
        );
        let sent = record(&rig);
        let (_sub, seen) = events(&rig);
        let meta = ServerViewMeta {
            present: true,
            row_count: Some(1),
            state: Some("ready".into()),
        };
        assert_eq!(
            apply(&rig, "a", &[("thing:1", 2)], Some(meta.clone()), false).await,
            MembershipOutcome::Applied
        );
        let st = rig.state();
        assert_eq!(st.queries["a"].lifecycle.phase, QueryPhase::Live);
        assert_eq!(st.queries["a"].server_state, Some(ServerViewState::Ready));
        // Membership caught up with the acked write, so the overlay releases it.
        assert!(st.outbox.is_empty());
        assert_eq!(count_kind(&seen, "query:authority"), 1);
        let view = rig.local.row("_00_view", "_00_view:view-a").unwrap();
        assert_eq!(view["ids"], Value::from(json!([["thing:1", 2]])));
        assert_eq!(view["children"], Value::from(json!([])));
        assert_eq!(view["confirmed"], Value::Bool(true));
        assert_eq!(view["updatedAt"], Value::Int(rig.now.get()));

        // Already authoritative: no second flip.
        apply(&rig, "a", &[("thing:1", 3)], Some(meta), false).await;
        assert_eq!(count_kind(&seen, "query:authority"), 1);
        rig.settle().await;
        assert!(body_requests(&sent) >= 1, "FetchRows pulls the new member");
    })
    .await;
}

#[tokio::test]
async fn the_view_row_records_the_children_the_query_holds() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Cached,
                RemotePhase::Unregistered,
                &[],
            )],
            |s| {
                s.queries.get_mut("a").unwrap().subquery_remote_array = pairs(&[("user:a", 2)]);
            },
        );
        apply(&rig, "a", &[("thing:1", 1)], None, false).await;
        let view = rig.local.row("_00_view", "_00_view:view-a").unwrap();
        assert_eq!(view["children"], Value::from(json!([["user:a", 2]])));
    })
    .await;
}

#[tokio::test]
async fn a_verified_removal_applies_an_empty_set_with_no_server_row() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            |_| {},
        );
        assert_eq!(
            apply(&rig, "a", &[], None, true).await,
            MembershipOutcome::Applied
        );
        assert!(rig.state().queries["a"].remote_array.is_empty());
    })
    .await;
}

#[tokio::test]
async fn a_known_empty_ready_view_applies() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            |_| {},
        );
        let meta = ServerViewMeta {
            present: true,
            row_count: Some(0),
            state: Some("ready".into()),
        };
        assert_eq!(
            apply(&rig, "a", &[], Some(meta), false).await,
            MembershipOutcome::Applied
        );
        assert!(rig.state().queries["a"].remote_array.is_empty());
    })
    .await;
}

#[tokio::test]
async fn a_failing_view_row_write_is_logged_not_fatal() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        *rig.local.fail_writes.borrow_mut() = Some("disk full".into());
        let (_sub, seen) = events(&rig);
        assert_eq!(
            apply(&rig, "a", &[("thing:1", 1)], None, false).await,
            MembershipOutcome::Applied
        );
        let logs = logs(&seen);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].0, LogLevel::Debug);
        assert_eq!(logs[0].1, "view row write failed");
    })
    .await;
}

// ---- apply_subquery_children --------------------------------------------------------

#[tokio::test]
async fn children_are_a_no_op_when_equal_otherwise_set_and_fetched() {
    run_local(async {
        let state = |s: &mut ClientState| {
            primed(s);
            s.queries.get_mut("a").unwrap().subquery_remote_array = pairs(&[("child:1", 1)]);
            s.versions.insert("child:1".into(), 1);
        };
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            state,
        );
        let sent = record(&rig);
        rig.run(
            |ctx| async move { apply_subquery_children(&ctx, "a", pairs(&[("child:1", 1)])).await },
        )
        .await
        .unwrap();
        rig.settle().await;
        assert!(sent.borrow().is_empty());

        rig.run(
            |ctx| async move { apply_subquery_children(&ctx, "a", pairs(&[("child:2", 1)])).await },
        )
        .await
        .unwrap();
        assert_eq!(
            rig.state().queries["a"].subquery_remote_array,
            pairs(&[("child:2", 1)])
        );
        assert!(
            rig.local.row("_00_view", "_00_view:view-a").is_none(),
            "a cold query has no view row to record children in"
        );
        rig.settle().await;
        assert_eq!(body_requests(&sent), 1, "the new child's body is pulled");
    })
    .await;
}

#[tokio::test]
async fn past_cold_the_children_are_written_to_the_view_row() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Unregistered,
                &[("thing:1", 4)],
            )],
            |_| {},
        );
        rig.run(
            |ctx| async move { apply_subquery_children(&ctx, "a", pairs(&[("user:b", 1)])).await },
        )
        .await
        .unwrap();
        let view = rig.local.row("_00_view", "_00_view:view-a").unwrap();
        assert_eq!(view["ids"], Value::from(json!([["thing:1", 4]])));
        assert_eq!(view["children"], Value::from(json!([["user:b", 1]])));
        assert_eq!(view["confirmed"], Value::Bool(true));
    })
    .await;
}

// ---- read_membership -----------------------------------------------------------------

#[tokio::test]
async fn nothing_to_read() {
    run_local(async {
        let (rig, _) = new_rig();
        let sent = record(&rig);
        let out = read(&rig, &["nope"]).await;
        assert_eq!(out, MembershipRead::default());
        assert!(sent.borrow().is_empty());
    })
    .await;
}

#[tokio::test]
async fn a_single_query_uses_the_single_shape_and_clears_its_dirt() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| {
                s.membership_dirty.insert("a".into());
            },
        );
        let sent = answer(&rig, |_, _| {
            Ok(snapshot(&[("thing:1", 1)], ready_meta(1), &[]))
        });
        let out = read(&rig, &["a"]).await;
        assert!(out.changed);
        assert!(!out.failed);
        {
            let sent = sent.borrow();
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].sql.split(";\n").count(), 3);
            assert_eq!(sent[0].sql, sql::single_snapshot_select("_00_list_ref"));
            assert!(matches!(
                sent[0].vars.as_ref().unwrap()["in"],
                Value::RecordId(_)
            ));
        }
        let st = rig.state();
        assert!(st.membership_dirty.is_empty());
        assert_eq!(st.queries["a"].last_polled_at, Some(rig.now.get()));
        assert_eq!(st.queries["a"].lifecycle.phase, QueryPhase::Live);
        rig.settle().await;
        assert!(rig.state().sync.has_synced_once);
    })
    .await;
}

#[tokio::test]
async fn reads_the_signed_in_users_table() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| {
                s.user_id = Some("user:u1".into());
            },
        );
        let sent = answer(&rig, |_, _| Ok(snapshot(&[], ready_meta(0), &[])));
        read(&rig, &["a"]).await;
        assert!(sent.borrow()[0].sql.contains("$in->_00_list_ref_user_u1 "));
    })
    .await;
}

#[tokio::test]
async fn an_errored_meta_statement_is_a_failed_read_not_a_lost_row() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            |_| {},
        );
        let sent = answer(&rig, |_, _| {
            Ok(results(vec![
                Ok(json!([])),
                Err("transaction failed"),
                Ok(json!([])),
            ]))
        });
        let out = read(&rig, &["a"]).await;
        assert!(out.failed);
        let e = &rig.state().queries["a"];
        assert_eq!(e.lifecycle.phase, QueryPhase::Live);
        assert_eq!(e.lifecycle.remote, RemotePhase::Registered);
        rig.settle().await;
        assert_eq!(register_requests(&sent), 0);
        assert_eq!(rig.state().sync.consecutive_failures, 1);
    })
    .await;
}

#[tokio::test]
async fn a_batch_whose_meta_statement_errored_fails_for_all_of_it() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![
                entry("a", QueryPhase::Cached, RemotePhase::Registered, &[]),
                entry("b", QueryPhase::Cached, RemotePhase::Registered, &[]),
            ],
            |_| {},
        );
        let sent = answer(&rig, |_, _| {
            Ok(results(vec![Ok(json!([])), Err("timeout")]))
        });
        let out = read(&rig, &["a", "b"]).await;
        assert!(out.failed);
        assert_eq!(sent.borrow().len(), 1);
        assert_eq!(
            sent.borrow()[0].sql,
            sql::batch_snapshot_select("_00_list_ref")
        );
        for h in ["a", "b"] {
            assert_eq!(rig.state().queries[h].lifecycle.phase, QueryPhase::Cached);
        }
        rig.settle().await;
        assert_eq!(register_requests(&sent), 0);
    })
    .await;
}

#[tokio::test]
async fn an_equal_set_on_a_live_query_applies_nothing_a_cached_one_still_flips() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            |s| s.queries.get_mut("a").unwrap().server_state = Some(ServerViewState::Ready),
        );
        answer(&rig, |_, _| {
            Ok(snapshot(&[("thing:1", 1)], ready_meta(1), &[]))
        });
        let out = read(&rig, &["a"]).await;
        assert!(!out.changed);
        assert_eq!(rig.log.count("local.put"), 0);

        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Cached,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            |_| {},
        );
        answer(&rig, |_, _| {
            Ok(snapshot(&[("thing:1", 1)], ready_meta(1), &[]))
        });
        let out = read(&rig, &["a"]).await;
        assert!(out.changed);
        assert_eq!(rig.state().queries["a"].lifecycle.phase, QueryPhase::Live);
    })
    .await;
}

#[tokio::test]
async fn many_queries_share_one_batch_and_suspects_are_reread_singly() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![
                entry(
                    "a",
                    QueryPhase::Live,
                    RemotePhase::Registered,
                    &[("thing:1", 1)],
                ),
                entry(
                    "b",
                    QueryPhase::Live,
                    RemotePhase::Registered,
                    &[("thing:2", 1)],
                ),
            ],
            |_| {},
        );
        let calls = Rc::new(Cell::new(0));
        let c2 = calls.clone();
        answer(&rig, move |sql, _| {
            c2.set(c2.get() + 1);
            if sql.contains("$ins->") {
                // `b`'s `_00_query` row did not come back: suspect.
                return Ok(results(vec![
                    Ok(json!([{"in": "_00_query:a", "out": "thing:1", "version": 2}])),
                    Ok(json!([{"id": "_00_query:a", "rowCount": 1, "state": "ready"}])),
                ]));
            }
            Ok(snapshot(&[("thing:2", 3)], ready_meta(1), &[]))
        });
        let out = read(&rig, &["a", "b"]).await;
        assert_eq!(calls.get(), 2, "one batch plus one single re-read");
        assert!(out.changed);
        assert!(!out.failed);
        let st = rig.state();
        assert_eq!(st.queries["a"].remote_array, pairs(&[("thing:1", 2)]));
        assert_eq!(st.queries["b"].remote_array, pairs(&[("thing:2", 3)]));
    })
    .await;
}

#[tokio::test]
async fn a_failed_chunk_reports_failed() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        answer(&rig, |_, _| Err(Error::Network("socket closed".into())));
        assert!(read(&rig, &["a"]).await.failed);
        rig.settle().await;
        assert_eq!(rig.state().sync.consecutive_failures, 1);
        assert!(rig.state().queries["a"].last_polled_at.is_none());
    })
    .await;
}

#[tokio::test]
async fn a_non_array_primary_statement_is_skipped_not_believed() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            |_| {},
        );
        answer(&rig, |_, _| {
            Ok(results(vec![
                Ok(json!(null)),
                Ok(json!(null)),
                Ok(json!(null)),
            ]))
        });
        let out = read(&rig, &["a"]).await;
        assert!(out.failed);
        assert_eq!(
            rig.state().queries["a"].remote_array,
            pairs(&[("thing:1", 1)])
        );
    })
    .await;
}

#[tokio::test]
async fn materializing_views_walk_the_reread_ladder_then_stop() {
    run_local(async {
        for (attempt, delay) in [
            (0u32, Some(150)),
            (1, Some(400)),
            (2, Some(1000)),
            (3, None),
        ] {
            let (rig, _) = rig_of(
                vec![entry(
                    "a",
                    QueryPhase::Live,
                    RemotePhase::Registered,
                    &[("thing:1", 1)],
                )],
                |s| {
                    s.membership_reread.insert("a".into(), attempt);
                },
            );
            answer(&rig, |_, _| {
                Ok(snapshot(
                    &[],
                    json!({"rowCount": 1, "state": "materializing"}),
                    &[],
                ))
            });
            read(&rig, &["a"]).await;
            assert_eq!(rig.timers.delay("membership"), delay, "attempt {attempt}");
            let st = rig.state();
            match delay {
                Some(_) => {
                    assert_eq!(st.membership_reread.get("a"), Some(&(attempt + 1)));
                    assert!(st.membership_dirty.contains("a"));
                }
                None => assert!(st.membership_reread.is_empty()),
            }
        }
    })
    .await;
}

#[tokio::test]
async fn an_answered_view_leaves_the_ladder() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry(
                "a",
                QueryPhase::Live,
                RemotePhase::Registered,
                &[("thing:1", 1)],
            )],
            |s| {
                s.membership_reread.insert("a".into(), 2);
            },
        );
        answer(&rig, |_, _| {
            Ok(snapshot(&[("thing:1", 1)], ready_meta(1), &[]))
        });
        read(&rig, &["a"]).await;
        assert!(rig.state().membership_reread.is_empty());
        assert!(!rig.timers.has("membership"));
    })
    .await;
}

// ---- membership dirt ----------------------------------------------------------------

#[tokio::test]
async fn the_coalesce_window_is_armed_once_per_burst() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let mark =
            || rig.run(|ctx| async move { mark_membership_dirty(&ctx, &["a".to_string()]).await });
        mark().await.unwrap();
        assert_eq!(rig.timers.delay("membership"), Some(MEMBERSHIP_LEADING_MS));
        TimerPort::clear(&rig.timers, "membership");
        mark().await.unwrap();
        assert!(
            !rig.timers.has("membership"),
            "re-arming per event would push the read out forever"
        );
    })
    .await;
}

#[tokio::test]
async fn a_recent_read_waits_out_the_rest_of_the_window() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| {
                s.queries.get_mut("a").unwrap().last_polled_at = Some(1_700_000_000_000 - 10);
            },
        );
        rig.run(|ctx| async move { mark_membership_dirty(&ctx, &["a".to_string()]).await })
            .await
            .unwrap();
        assert_eq!(
            rig.timers.delay("membership"),
            Some(MEMBERSHIP_COALESCE_MS - 10)
        );
    })
    .await;
}

#[tokio::test]
async fn unknown_hashes_are_not_marked_but_the_window_arms() {
    run_local(async {
        let (rig, _) = new_rig();
        rig.run(|ctx| async move { mark_membership_dirty(&ctx, &["nope".to_string()]).await })
            .await
            .unwrap();
        assert!(rig.state().membership_dirty.is_empty());
        assert!(
            rig.timers.has("membership"),
            "armed on the empty -> non-empty edge only"
        );
    })
    .await;
}

#[tokio::test]
async fn read_dirty_is_a_no_op_when_clean_and_rearms_for_leftovers() {
    run_local(async {
        let (rig, _) = new_rig();
        let sent = record(&rig);
        let env = rig.env();
        rig.run(|ctx| async move { read_dirty_membership(&ctx, &env).await })
            .await
            .unwrap();
        assert!(sent.borrow().is_empty());

        // 'b' is dirty but unknown, so the read cannot clear it.
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| {
                s.membership_dirty.insert("a".into());
                s.membership_dirty.insert("b".into());
            },
        );
        answer(&rig, |_, _| Ok(snapshot(&[], ready_meta(0), &[])));
        let env = rig.env();
        rig.run(|ctx| async move { read_dirty_membership(&ctx, &env).await })
            .await
            .unwrap();
        let dirty: Vec<String> = rig.state().membership_dirty.into_iter().collect();
        assert_eq!(dirty, ["b"]);
        assert_eq!(rig.timers.delay("membership"), Some(MEMBERSHIP_COALESCE_MS));
    })
    .await;
}

#[tokio::test]
async fn the_armed_window_runs_the_batched_read() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let sent = answer(&rig, |_, _| {
            Ok(snapshot(&[("thing:1", 1)], ready_meta(1), &[]))
        });
        rig.run(|ctx| async move { mark_membership_dirty(&ctx, &["a".to_string()]).await })
            .await
            .unwrap();
        assert!(rig.timers.fire("membership"));
        rig.settle().await;
        assert_eq!(sent.borrow().len(), 1);
        let st = rig.state();
        assert!(st.membership_dirty.is_empty());
        assert_eq!(st.queries["a"].remote_array, pairs(&[("thing:1", 1)]));
    })
    .await;
}
