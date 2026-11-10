//! Tests for the connection, LIVE and poll sagas.

use serde_json::json;

use crate::error::Error;
use crate::kernel::constants::MEMBERSHIP_LEADING_MS;
use crate::kernel::events::{LogLevel, OutEvent};
use crate::query::test_support::*;
use crate::state::lifecycle::{QueryPhase, RemotePhase};
use crate::state::{OutboxStatus, ServerViewState};
use crate::sync::{connection, live, poll};
use crate::testing::{build_outbox_item, results, row, run_local, TestRig};
use crate::types::{ConnectionState, InlineRow, MutationEventType, SyncHealth, SyncHealthStatus};
use crate::value::Value;

fn registered(hash: &str, remote: &[(&str, i64)]) -> crate::state::QueryEntry {
    entry(hash, QueryPhase::Live, RemotePhase::Registered, remote)
}

async fn poll_tick(rig: &TestRig) {
    let env = rig.env();
    rig.run(|ctx| async move { poll::poll_tick(&ctx, &env).await })
        .await
        .unwrap();
}

async fn live_start(rig: &TestRig) {
    let env = rig.env();
    rig.run(|ctx| async move { live::live_start(&ctx, &env).await })
        .await
        .unwrap();
}

async fn live_change(rig: &TestRig, hashes: &[&str], rows: Option<Vec<InlineRow>>) {
    let env = rig.env();
    let hashes = hashes.iter().map(|h| h.to_string()).collect();
    rig.run(|ctx| async move { live::live_change(&ctx, &env, hashes, rows).await })
        .await
        .unwrap();
}

async fn connection_changed(rig: &TestRig, state: ConnectionState) {
    let env = rig.env();
    rig.run(|ctx| async move { connection::connection_changed(&ctx, &env, state).await })
        .await
        .unwrap();
}

async fn sync_outcome(rig: &TestRig, ok: bool, error: Option<&str>) {
    let env = rig.env();
    let error = error.map(str::to_string);
    rig.run(|ctx| async move { connection::sync_outcome(&ctx, &env, ok, error).await })
        .await
        .unwrap();
}

async fn self_heal(rig: &TestRig) {
    let env = rig.env();
    rig.run(|ctx| async move { connection::self_heal_tick(&ctx, &env).await })
        .await
        .unwrap();
}

fn count_sql(sent: &SentLog, prefix: &str) -> usize {
    sent.borrow()
        .iter()
        .filter(|s| s.sql.starts_with(prefix))
        .count()
}

// ---- poll_tick ------------------------------------------------------------------------

#[tokio::test]
async fn with_no_queries_the_poll_probes_and_reports_the_round() {
    run_local(async {
        let (rig, _) = new_rig();
        let sent = record(&rig);
        poll_tick(&rig).await;
        assert_eq!(sent.borrow()[0].sql, "RETURN true");
        rig.settle().await;
        assert!(rig.state().sync.has_synced_once);

        let (rig, _) = new_rig();
        answer(&rig, |_, _| Err(Error::Network("socket closed".into())));
        poll_tick(&rig).await;
        rig.settle().await;
        assert_eq!(rig.state().sync.consecutive_failures, 1);
    })
    .await;
}

#[tokio::test]
async fn a_quiet_round_backs_the_cadence_off_and_a_change_snaps_it_back() {
    run_local(async {
        let (rig, _) = rig_of(vec![registered("a", &[("thing:1", 1)])], |s| {
            s.queries.get_mut("a").unwrap().server_state = Some(ServerViewState::Ready);
        });
        answer(&rig, |_, _| {
            Ok(snapshot(&[("thing:1", 1)], ready_meta(1), &[]))
        });
        poll_tick(&rig).await;
        assert_eq!(rig.state().sync.poll_idle_streak, 1);
        assert_eq!(rig.timers.delay("poll"), Some(1000));

        rig.runtime.update(|s| s.sync.poll_idle_streak = 4);
        answer(&rig, |_, _| {
            Ok(snapshot(&[("thing:2", 1)], ready_meta(1), &[]))
        });
        poll_tick(&rig).await;
        assert_eq!(rig.state().sync.poll_idle_streak, 0);
        assert_eq!(rig.timers.delay("poll"), Some(500));
        assert_eq!(
            rig.state().queries["a"].remote_array,
            pairs(&[("thing:2", 1)])
        );
    })
    .await;
}

#[tokio::test]
async fn the_poll_reads_only_due_views() {
    run_local(async {
        // A large view polled a moment ago rides on LIVE.
        let large: Vec<(String, i64)> = (0..1200).map(|i| (format!("thing:{i}"), 1)).collect();
        let (rig, _) = rig_of(
            vec![registered("big", &[]), registered("small", &[])],
            |s| {
                let big = s.queries.get_mut("big").unwrap();
                big.remote_array = large;
                big.last_polled_at = Some(1_700_000_000_000 - 1000);
                s.queries.get_mut("small").unwrap().last_polled_at = Some(1_700_000_000_000 - 1000);
            },
        );
        let sent = answer(&rig, |_, _| Ok(snapshot(&[], ready_meta(0), &[])));
        poll_tick(&rig).await;
        let sent = sent.borrow();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].vars.as_ref().unwrap()["in"].to_id_string(),
            "_00_query:small"
        );
    })
    .await;
}

#[tokio::test]
async fn an_acked_write_waiting_for_membership_holds_the_base_cadence() {
    run_local(async {
        let (rig, _) = rig_of(vec![registered("a", &[])], |s| {
            s.queries.get_mut("a").unwrap().server_state = Some(ServerViewState::Ready);
            s.sync.poll_idle_streak = 3;
            let mut item = build_outbox_item("m1", MutationEventType::Update, "thing:9");
            item.status = OutboxStatus::Acked;
            item.acked_at = Some(1);
            s.outbox.push(item);
        });
        answer(&rig, |_, _| Ok(snapshot(&[], ready_meta(0), &[])));
        poll_tick(&rig).await;
        assert_eq!(rig.state().sync.poll_idle_streak, 0);
        assert_eq!(rig.timers.delay("poll"), Some(500));
    })
    .await;
}

#[tokio::test]
async fn the_poll_rearms_itself_through_the_runtime() {
    run_local(async {
        let (rig, _) = new_rig();
        let sent = record(&rig);
        poll_tick(&rig).await;
        assert!(rig.timers.fire("poll"));
        rig.settle().await;
        assert_eq!(count_sql(&sent, "RETURN true"), 2);
        assert_eq!(rig.timers.delay("poll"), Some(2000));
    })
    .await;
}

// ---- live_start / live_invalidate --------------------------------------------------------

#[tokio::test]
async fn live_subscribes_on_the_session_table_and_records_the_uuid() {
    run_local(async {
        let (rig, _) = new_rig();
        live_start(&rig).await;
        assert_eq!(rig.log.named("remote.live")[0].args, "\"_00_list_ref\"");
        let st = rig.state();
        assert_eq!(st.sync.live_uuid.as_deref(), Some("live-uuid"));
        assert_eq!(st.sync.live_table.as_deref(), Some("_00_list_ref"));

        // Already on the right table: no-op.
        live_start(&rig).await;
        assert_eq!(rig.log.count("remote.live"), 1);
    })
    .await;
}

#[tokio::test]
async fn live_follows_the_signed_in_user_and_kills_the_old_subscription_when_connected() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), |s| {
            s.sync.live_uuid = Some("old".into());
            s.sync.live_table = Some("_00_list_ref".into());
            s.sync.health.connection = ConnectionState::Connected;
            s.user_id = Some("user:u1".into());
        });
        live_start(&rig).await;
        assert_eq!(*rig.remote.killed.borrow(), ["old"]);
        assert_eq!(
            rig.log.named("remote.live")[0].args,
            "\"_00_list_ref_user_u1\""
        );
        assert_eq!(rig.state().sync.live_uuid.as_deref(), Some("live-uuid"));

        // Disconnected: the old query died with the socket, nothing to kill.
        let (rig, _) = rig_of(Vec::new(), |s| {
            s.sync.live_uuid = Some("old".into());
            s.sync.live_table = Some("_00_list_ref_user_u9".into());
        });
        live_start(&rig).await;
        assert!(rig.remote.killed.borrow().is_empty());
    })
    .await;
}

#[tokio::test]
async fn a_failing_subscribe_leaves_the_poll_to_cover_membership() {
    run_local(async {
        let (rig, _) = new_rig();
        *rig.remote.fail_live.borrow_mut() = Some("no permission".into());
        let (_sub, seen) = events(&rig);
        live_start(&rig).await;
        assert!(rig.state().sync.live_uuid.is_none());
        let logs = logs(&seen);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].0, LogLevel::Warn);
    })
    .await;
}

#[tokio::test]
async fn live_invalidate_clears_the_bookkeeping() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), |s| {
            s.sync.live_uuid = Some("u".into());
            s.sync.live_table = Some("t".into());
        });
        rig.run(|ctx| async move { live::live_invalidate(&ctx).await })
            .await
            .unwrap();
        let st = rig.state();
        assert!(st.sync.live_uuid.is_none());
        assert!(st.sync.live_table.is_none());
    })
    .await;
}

// ---- live_change ----------------------------------------------------------------------

#[tokio::test]
async fn a_change_marks_known_hashes_dirty_and_ignores_the_rest() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        live_change(&rig, &["a", "nope"], None).await;
        let dirty: Vec<String> = rig.state().membership_dirty.into_iter().collect();
        assert_eq!(dirty, ["a"]);
        assert_eq!(rig.timers.delay("membership"), Some(MEMBERSHIP_LEADING_MS));

        let (rig, _) = new_rig();
        live_change(&rig, &["nope"], None).await;
        assert!(
            !rig.timers.has("membership"),
            "nothing known: nothing to read"
        );
    })
    .await;
}

#[tokio::test]
async fn a_change_pulls_a_backed_off_poll_back_to_base_and_leaves_a_base_one_alone() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| s.sync.poll_idle_streak = 5,
        );
        live_change(&rig, &["a"], None).await;
        assert_eq!(rig.state().sync.poll_idle_streak, 0);
        assert_eq!(rig.timers.delay("poll"), Some(500));

        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        live_change(&rig, &["a"], None).await;
        assert!(
            !rig.timers.has("poll"),
            "re-arming per event would starve the reconciliation poll"
        );
    })
    .await;
}

#[tokio::test]
async fn a_joined_body_lands_so_the_fetch_plan_has_nothing_left_to_pull() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let rows = vec![
            InlineRow {
                id: "thing:1".into(),
                version: 3,
                record: row(json!({"id": "thing:1", "title": "pushed"})),
            },
            // The same body once per view holding it; the newest wins.
            InlineRow {
                id: "thing:1".into(),
                version: 2,
                record: row(json!({"id": "thing:1", "title": "older"})),
            },
        ];
        live_change(&rig, &["a"], Some(rows)).await;
        assert_eq!(rig.state().versions.get("thing:1"), Some(&3));
        let stored = rig.local.row("thing", "thing:1").unwrap();
        assert_eq!(stored["title"], Value::from("pushed"));
        assert_eq!(stored["_00_rv"], Value::Int(3));
        assert_eq!(rig.log.count("local.tx"), 1);
        assert_eq!(rig.log.count("ssp.ingest"), 1);
        assert!(rig.state().membership_dirty.contains("a"));
    })
    .await;
}

#[tokio::test]
async fn a_body_already_held_at_that_version_or_newer_is_ignored() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |s| {
                s.versions.insert("thing:1".into(), 3);
            },
        );
        let rows = vec![InlineRow {
            id: "thing:1".into(),
            version: 2,
            record: row(json!({"id": "thing:1"})),
        }];
        live_change(&rig, &["a"], Some(rows)).await;
        assert_eq!(rig.log.count("local.tx"), 0);
    })
    .await;
}

#[tokio::test]
async fn a_live_notification_reaches_the_change_saga_through_the_runtime() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Cold, RemotePhase::Unregistered, &[])],
            |_| {},
        );
        let sent = answer(&rig, |_, _| {
            Ok(snapshot(&[("thing:1", 1)], ready_meta(1), &[]))
        });
        live_start(&rig).await;
        rig.remote.push_live(vec!["a".into()], None);
        rig.settle().await;
        assert!(rig.state().membership_dirty.contains("a"));
        assert!(rig.timers.fire("membership"));
        rig.settle().await;
        assert_eq!(sent.borrow().len(), 1);
        assert_eq!(
            rig.state().queries["a"].remote_array,
            pairs(&[("thing:1", 1)])
        );
    })
    .await;
}

// ---- connection_changed ------------------------------------------------------------------

#[tokio::test]
async fn a_drop_arms_resubscribe_and_invalidates_live() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), |s| {
            s.sync.live_uuid = Some("u".into());
            s.sync.live_table = Some("t".into());
        });
        let sent = record(&rig);
        connection_changed(&rig, ConnectionState::Disconnected).await;
        let st = rig.state();
        assert!(st.sync.needs_resubscribe);
        assert!(st.sync.live_uuid.is_none());
        assert_eq!(st.sync.health.connection, ConnectionState::Disconnected);
        rig.settle().await;
        assert!(sent.borrow().is_empty());
        assert_eq!(rig.log.count("remote.live"), 0);
    })
    .await;
}

#[tokio::test]
async fn the_initial_connect_only_records_the_state() {
    run_local(async {
        let (rig, _) = new_rig();
        let (_sub, seen) = events(&rig);
        connection_changed(&rig, ConnectionState::Connected).await;
        assert_eq!(rig.state().sync.health.connection, ConnectionState::Connected);
        rig.settle().await;
        assert_eq!(rig.log.count("remote.live"), 0);
        assert!(seen
            .borrow()
            .iter()
            .any(|e| matches!(e, OutEvent::HealthChanged { health } if health.connection == ConnectionState::Connected)));
    })
    .await;
}

#[tokio::test]
async fn a_reconnect_drops_every_registration_and_redrives_once_per_window() {
    run_local(async {
        let (rig, _) = rig_of(vec![registered("a", &[])], |s| {
            s.user_id = Some("user:u1".into())
        });
        let sent = answer(&rig, |sql, _| {
            Ok(if sql == "RETURN $auth.id" {
                results(vec![Ok(json!("user:u1"))])
            } else if sql.starts_with("fn::query::register") {
                register_answer(&[], ready_meta(0), &[])
            } else {
                Vec::new()
            })
        });
        connection_changed(&rig, ConnectionState::Disconnected).await;
        rig.set_now(100_000);
        connection_changed(&rig, ConnectionState::Connected).await;
        let st = rig.state();
        assert_eq!(st.queries["a"].lifecycle.remote, RemotePhase::Unregistered);
        assert!(!st.sync.needs_resubscribe);
        assert_eq!(st.sync.last_reconnect_refetch_at, Some(100_000));
        rig.settle().await;
        // EnsureRegistered went through the identity gate, then registered;
        // LiveStart resubscribed.
        assert_eq!(sent.borrow()[0].sql, "RETURN $auth.id");
        assert_eq!(count_sql(&sent, "fn::query::register"), 1);
        assert_eq!(rig.log.count("remote.live"), 1);
        assert_eq!(
            rig.state().queries["a"].lifecycle.remote,
            RemotePhase::Registered
        );

        // A second reconnect inside the cooldown restarts LIVE but does not
        // re-register.
        connection_changed(&rig, ConnectionState::Disconnected).await;
        rig.set_now(105_000);
        connection_changed(&rig, ConnectionState::Connected).await;
        let st = rig.state();
        assert!(!st.sync.needs_resubscribe);
        assert_eq!(st.sync.last_reconnect_refetch_at, Some(100_000));
        assert_eq!(st.queries["a"].lifecycle.remote, RemotePhase::Registered);
        rig.settle().await;
        assert_eq!(count_sql(&sent, "fn::query::register"), 1);
        assert_eq!(rig.log.count("remote.live"), 2);
    })
    .await;
}

#[tokio::test]
async fn a_reconnect_inside_the_cooldown_restarts_the_live_the_drop_killed() {
    run_local(async {
        let (rig, _) = rig_of(vec![registered("a", &[])], |s| {
            s.sync.live_uuid = Some("u".into());
            s.sync.live_table = Some("t".into());
            s.sync.last_reconnect_refetch_at = Some(1000);
        });
        let sent = record(&rig);
        connection_changed(&rig, ConnectionState::Disconnected).await;
        assert!(rig.state().sync.live_uuid.is_none());
        rig.set_now(3000);
        connection_changed(&rig, ConnectionState::Connected).await;
        rig.settle().await;
        assert_eq!(rig.log.count("remote.live"), 1);
        assert_eq!(count_sql(&sent, "fn::query::register"), 0);
        assert_eq!(
            rig.state().queries["a"].lifecycle.remote,
            RemotePhase::Registered
        );
    })
    .await;
}

// ---- sync_outcome / self_heal_tick ----------------------------------------------------------

#[tokio::test]
async fn degrades_after_the_threshold_and_arms_self_heal_recovery_disarms_it() {
    run_local(async {
        let (rig, _) = new_rig();
        let (_sub, seen) = events(&rig);
        for _ in 0..2 {
            sync_outcome(&rig, false, Some("socket closed")).await;
            assert!(!rig.timers.has("heal"));
        }
        sync_outcome(&rig, false, Some("socket closed")).await;
        let st = rig.state();
        assert_eq!(st.sync.health.status, SyncHealthStatus::Degraded);
        assert_eq!(st.sync.health.kind.as_deref(), Some("network"));
        assert_eq!(st.sync.self_heal_attempts, 0);
        assert_eq!(rig.timers.delay("heal"), Some(2000));
        assert!(seen.borrow().iter().any(|e| matches!(
            e,
            OutEvent::HealthChanged { health } if health.status == SyncHealthStatus::Degraded
        )));

        sync_outcome(&rig, true, None).await;
        let st = rig.state();
        assert_eq!(st.sync.health.status, SyncHealthStatus::Healthy);
        assert_eq!(st.sync.consecutive_failures, 0);
        assert!(st.sync.has_synced_once);
        assert!(!rig.timers.has("heal"));
    })
    .await;
}

#[tokio::test]
async fn degrade_after_zero_never_degrades() {
    run_local(async {
        let mut env = env();
        env.degrade_after = 0;
        let (rig, _) = rig_with(env, crate::state::ClientState::empty("tab"));
        for _ in 0..5 {
            sync_outcome(&rig, false, Some("socket closed")).await;
        }
        assert_eq!(rig.state().sync.health.status, SyncHealthStatus::Healthy);
        assert!(!rig.timers.has("heal"));
    })
    .await;
}

fn degraded(s: &mut crate::state::ClientState) {
    s.sync.health = SyncHealth {
        status: SyncHealthStatus::Degraded,
        consecutive_failures: 3,
        ever_connected: true,
        ..Default::default()
    };
}

#[tokio::test]
async fn self_heal_does_nothing_while_healthy() {
    run_local(async {
        let (rig, _) = new_rig();
        let sent = record(&rig);
        self_heal(&rig).await;
        assert!(rig.timers.pending().is_empty());
        rig.settle().await;
        assert!(sent.borrow().is_empty());
    })
    .await;
}

#[tokio::test]
async fn self_heal_drains_first() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), |s| {
            degraded(s);
            s.outbox.push(build_outbox_item(
                "m1",
                MutationEventType::Create,
                "thing:1",
            ));
        });
        let sent = record(&rig);
        self_heal(&rig).await;
        assert_eq!(rig.state().sync.self_heal_attempts, 1);
        assert_eq!(rig.timers.delay("heal"), Some(4000));
        rig.settle().await;
        assert!(
            sent.borrow().is_empty(),
            "no probe and no registration while writes wait"
        );
    })
    .await;
}

#[tokio::test]
async fn self_heal_then_reregisters_failed_ones_included() {
    run_local(async {
        let (rig, _) = rig_of(
            vec![entry("a", QueryPhase::Live, RemotePhase::Failed, &[])],
            |s| {
                degraded(s);
                s.user_id = Some("user:u1".into());
            },
        );
        let sent = record(&rig);
        self_heal(&rig).await;
        assert_eq!(
            rig.state().queries["a"].lifecycle.remote,
            RemotePhase::Unregistered
        );
        rig.settle().await;
        assert_eq!(
            sent.borrow()[0].sql,
            "RETURN $auth.id",
            "behind the identity gate"
        );
    })
    .await;
}

#[tokio::test]
async fn self_heal_else_probes_and_always_rearms() {
    run_local(async {
        let (rig, _) = rig_of(Vec::new(), degraded);
        let sent = record(&rig);
        self_heal(&rig).await;
        assert_eq!(sent.borrow()[0].sql, "RETURN true");
        assert!(rig.timers.has("heal"));
        rig.settle().await;
        assert!(rig.state().sync.has_synced_once);

        // The armed tick heals again on a longer backoff.
        assert!(rig.timers.fire("heal"));
        rig.settle().await;
        assert_eq!(rig.state().sync.self_heal_attempts, 2);
        assert_eq!(rig.timers.delay("heal"), Some(8000));
    })
    .await;
}
