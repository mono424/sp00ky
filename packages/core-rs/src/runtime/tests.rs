use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::error::Error;
use crate::kernel::events::OutEvent;
use crate::state::lifecycle::{seed_lifecycle, LifecycleEvent};
use crate::state::reducers as r;
use crate::testing::{build_definition, build_entry, run_local, TestRig};
use crate::types::QueryStatus;
use crate::value::Row;

#[tokio::test]
async fn serial_lane_runs_in_arrival_order() {
    run_local(async {
        let rig = TestRig::new();
        let order = Rc::new(RefCell::new(Vec::new()));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let rt = rig.runtime.clone();
        let o1 = order.clone();
        let first = tokio::task::spawn_local(async move {
            rt.run_serial("k", |_ctx| async move {
                rx.await.ok();
                o1.borrow_mut().push(1);
                Ok(())
            })
            .await
        });
        tokio::task::yield_now().await;
        let rt = rig.runtime.clone();
        let o2 = order.clone();
        let second = tokio::task::spawn_local(async move {
            rt.run_serial("k", |_ctx| async move {
                o2.borrow_mut().push(2);
                Ok(())
            })
            .await
        });
        tokio::task::yield_now().await;
        assert!(order.borrow().is_empty(), "second must wait for first");
        tx.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(*order.borrow(), vec![1, 2]);
    })
    .await;
}

#[tokio::test]
async fn waiters_resolve_after_update() {
    run_local(async {
        let rig = TestRig::new();
        let rt = rig.runtime.clone();
        let waiting = tokio::task::spawn_local(async move { rt.ctx().wait(|s| s.primed).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        rig.runtime.update(|s| s.primed = true);
        waiting.await.unwrap().unwrap();
    })
    .await;
}

#[tokio::test]
async fn dispose_fails_waiters() {
    run_local(async {
        let rig = TestRig::new();
        let rt = rig.runtime.clone();
        let waiting = tokio::task::spawn_local(async move { rt.ctx().wait(|s| s.primed).await });
        tokio::task::yield_now().await;
        rig.runtime.dispose();
        assert!(matches!(waiting.await.unwrap(), Err(Error::Closed)));
    })
    .await;
}

#[tokio::test]
async fn fenced_saga_stops_after_bucket_switch() {
    run_local(async {
        let rig = TestRig::new();
        let local = rig.local.clone();
        let out = rig
            .runtime
            .run(|ctx| async move {
                ctx.now()?;
                local.bump_epoch();
                ctx.now()?;
                Ok(())
            })
            .await;
        assert!(matches!(out, Err(Error::AccountChanged)));
        // Unfenced runs keep going.
        let local = rig.local.clone();
        rig.run(|ctx| async move {
            local.bump_epoch();
            ctx.now().map(|_| ())
        })
        .await
        .unwrap();
    })
    .await;
}

#[tokio::test]
async fn epoch_fenced_writes_are_dropped() {
    run_local(async {
        let rig = TestRig::new();
        let ctx = rig.ctx();
        let stale = ctx.local_epoch().unwrap();
        rig.local.bump_epoch();
        ctx.local_put("t", "t:1", Row::new(), Default::default(), Some(stale)).unwrap();
        assert!(rig.local.row("t", "t:1").is_none());
        ctx.local_put("t", "t:1", Row::new(), Default::default(), None).unwrap();
        assert!(rig.local.row("t", "t:1").is_some());
    })
    .await;
}

#[tokio::test]
async fn dirty_queries_schedule_materialize_with_leading_edge() {
    run_local(async {
        let rig = TestRig::new();
        rig.runtime.update(|s| r::put_query(s, build_entry(build_definition("h1", "thing"))));
        assert_eq!(rig.timers.delay("mat:h1"), Some(0));
        // A second update while scheduled does not reschedule.
        rig.runtime.update(|s| r::mark_dirty(s, &["h1".to_string()]));
        assert_eq!(rig.timers.pending().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn record_subscribers_count_and_receive() {
    run_local(async {
        let rig = TestRig::new();
        rig.runtime.update(|s| r::put_query(s, build_entry(build_definition("h1", "thing"))));
        let seen = Rc::new(RefCell::new(0usize));
        let s2 = seen.clone();
        let sub = rig.runtime.subscribe("h1", move |rows| *s2.borrow_mut() += rows.len() + 1, true);
        assert_eq!(*seen.borrow(), 1, "immediate replay of the empty set");
        assert_eq!(rig.state().queries["h1"].subscribers, 1);
        rig.runtime.emit(OutEvent::QueryRecords {
            hash: "h1".into(),
            records: Arc::new(vec![Row::new(), Row::new()]),
        });
        assert_eq!(*seen.borrow(), 4);
        drop(sub);
        let st = rig.state();
        assert_eq!(st.queries["h1"].subscribers, 0);
        assert_eq!(st.queries["h1"].last_subscriber_left_at, Some(rig.now.get()));
    })
    .await;
}

#[tokio::test]
async fn status_subscribers_fire_on_transitions_only() {
    run_local(async {
        let rig = TestRig::new();
        rig.runtime.update(|s| {
            let mut e = build_entry(build_definition("h1", "thing"));
            e.lifecycle = seed_lifecycle(false);
            r::put_query(s, e)
        });
        let seen = Rc::new(RefCell::new(Vec::new()));
        let s2 = seen.clone();
        let _sub = rig.runtime.subscribe_status("h1", move |st| s2.borrow_mut().push(st), false);
        rig.runtime.update(|s| r::apply_lifecycle(s, "h1", LifecycleEvent::FetchBegin { background: false }));
        rig.runtime.update(|s| r::apply_lifecycle(s, "h1", LifecycleEvent::FetchBegin { background: false }));
        rig.runtime.update(|s| r::apply_lifecycle(s, "h1", LifecycleEvent::FetchEnd { background: false }));
        rig.runtime.update(|s| r::apply_lifecycle(s, "h1", LifecycleEvent::FetchEnd { background: false }));
        assert_eq!(*seen.borrow(), vec![QueryStatus::Fetching, QueryStatus::Idle]);
    })
    .await;
}

#[tokio::test]
async fn activity_and_health_events() {
    run_local(async {
        let rig = TestRig::new();
        let kinds = Rc::new(RefCell::new(Vec::new()));
        let k2 = kinds.clone();
        let _l = rig.runtime.on("*", move |e| k2.borrow_mut().push(e.kind()));
        rig.runtime.update(|s| {
            r::outbox_push(s, crate::testing::build_outbox_item("m1", crate::types::MutationEventType::Create, "thing:1"))
        });
        rig.runtime.update(|s| r::set_connection(s, crate::types::ConnectionState::Connected));
        let kinds = kinds.borrow();
        assert!(kinds.contains(&"activity:changed"));
        assert!(kinds.contains(&"unsynced:changed"));
        assert!(kinds.contains(&"health:changed"));
    })
    .await;
}

#[tokio::test]
async fn dedupe_lane_does_not_join_a_run_of_another_account() {
    use futures::future::FutureExt;
    run_local(async {
        let rig = TestRig::new();
        let runs = std::rc::Rc::new(RefCell::new(0usize));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let rt = rig.runtime.clone();
        let stale = tokio::task::spawn_local(async move {
            rt.run_dedupe(
                "k".into(),
                false,
                Box::new(move |ctx| {
                    async move {
                        rx.await.ok();
                        ctx.check()
                    }
                    .boxed_local()
                }),
            )
            .await
        });
        tokio::task::yield_now().await;

        // Same account: the second request joins the running one.
        let r2 = runs.clone();
        let rt = rig.runtime.clone();
        let joined = tokio::task::spawn_local(async move {
            rt.run_dedupe(
                "k".into(),
                false,
                Box::new(move |_ctx| {
                    *r2.borrow_mut() += 1;
                    async { Ok(()) }.boxed_local()
                }),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert_eq!(*runs.borrow(), 0, "a same-account request must join");

        // Another account: the request runs on its own.
        rig.local.bump_epoch();
        let r3 = runs.clone();
        rig.runtime
            .run_dedupe(
                "k".into(),
                false,
                Box::new(move |_ctx| {
                    *r3.borrow_mut() += 1;
                    async { Ok(()) }.boxed_local()
                }),
            )
            .await
            .unwrap();
        assert_eq!(*runs.borrow(), 1, "a post-switch request must not join the stale run");

        tx.send(()).ok();
        assert!(matches!(stale.await.unwrap(), Err(Error::AccountChanged)));
        assert!(matches!(joined.await.unwrap(), Err(Error::AccountChanged)));
    })
    .await;
}
