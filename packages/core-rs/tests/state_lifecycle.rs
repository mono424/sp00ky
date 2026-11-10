//! PORT: `packages/spooky_core/test/state/lifecycle_test.dart`.

use spooky_core::state::lifecycle::{
    derive_status, has_server_membership, is_authoritative, is_foreground_fetching, seed_lifecycle,
    transition, try_transition, LifecycleEvent, QueryLifecycle, QueryPhase, RemotePhase,
};
use spooky_core::types::QueryStatus;

use LifecycleEvent::*;

fn at(phase: QueryPhase) -> QueryLifecycle {
    QueryLifecycle {
        phase,
        remote: RemotePhase::Unregistered,
        fetch_depth: 0,
        background_depth: 0,
        notified: false,
    }
}

#[test]
fn seed() {
    assert_eq!(
        transition(
            at(QueryPhase::Live),
            Seed {
                resolved_before: true
            }
        )
        .phase,
        QueryPhase::Cached
    );
    assert_eq!(
        transition(
            at(QueryPhase::Live),
            Seed {
                resolved_before: false
            }
        )
        .phase,
        QueryPhase::Cold
    );
}

#[test]
fn membership_applied_moves_every_phase_to_live() {
    for p in [QueryPhase::Cold, QueryPhase::Cached, QueryPhase::Live] {
        assert_eq!(
            transition(at(p), MembershipApplied { present: true }).phase,
            QueryPhase::Live
        );
        assert_eq!(
            transition(at(p), MembershipApplied { present: false }).phase,
            QueryPhase::Live
        );
    }
    assert_eq!(
        transition(
            at(QueryPhase::ViewLost),
            MembershipApplied { present: true }
        )
        .phase,
        QueryPhase::Live
    );
}

#[test]
fn view_lost_needs_a_present_row_to_recover() {
    let lost = at(QueryPhase::ViewLost);
    assert!(try_transition(lost, MembershipApplied { present: false }).is_err());
    // The reducer form logs and keeps the lifecycle as it was.
    assert_eq!(transition(lost, MembershipApplied { present: false }), lost);
}

#[test]
fn row_missing_cold_stays_cold_everything_else_goes_view_lost() {
    assert_eq!(
        transition(at(QueryPhase::Cold), RowMissing).phase,
        QueryPhase::Cold
    );
    assert_eq!(
        transition(at(QueryPhase::Cached), RowMissing).phase,
        QueryPhase::ViewLost
    );
    assert_eq!(
        transition(at(QueryPhase::Live), RowMissing).phase,
        QueryPhase::ViewLost
    );
    assert_eq!(
        transition(at(QueryPhase::ViewLost), RowMissing).phase,
        QueryPhase::ViewLost
    );
}

#[test]
fn bucket_switch_reseeds_and_clears_fetch_depth() {
    let l = transition(
        QueryLifecycle {
            fetch_depth: 3,
            notified: true,
            ..at(QueryPhase::Live)
        },
        BucketSwitch {
            resolved_before: true,
        },
    );
    assert_eq!(l.phase, QueryPhase::Cached);
    assert_eq!(l.remote, RemotePhase::Unregistered);
    assert_eq!(l.fetch_depth, 0);
    assert!(!l.notified);
}

#[test]
fn remote_transitions() {
    let mut l = at(QueryPhase::Cached);
    l = transition(l, RemoteRegistering);
    assert_eq!(l.remote, RemotePhase::Registering);
    l = transition(l, RemoteRegistered);
    assert_eq!(l.remote, RemotePhase::Registered);
    l = transition(l, Notified);
    assert!(l.notified);
    l = transition(l, RemoteDropped);
    assert_eq!(l.remote, RemotePhase::Unregistered);
    assert!(!l.notified);
    l = transition(l, RemoteFailed);
    assert_eq!(l.remote, RemotePhase::Failed);
}

#[test]
fn fetch_depth_is_a_refcount_that_never_goes_negative() {
    let mut l = at(QueryPhase::Live);
    l = transition(l, FetchBegin { background: false });
    l = transition(l, FetchBegin { background: false });
    assert_eq!(derive_status(&l), QueryStatus::Fetching);
    l = transition(l, FetchEnd { background: false });
    assert_eq!(derive_status(&l), QueryStatus::Fetching);
    l = transition(l, FetchEnd { background: false });
    assert_eq!(derive_status(&l), QueryStatus::Idle);
    assert_eq!(transition(l, FetchEnd { background: false }), l);
}

#[test]
fn background_cycles_hold_fetching_but_are_not_foreground() {
    let mut l = at(QueryPhase::Cached);
    l = transition(l, FetchBegin { background: true });
    assert_eq!(derive_status(&l), QueryStatus::Fetching);
    assert!(!is_foreground_fetching(&l));
    l = transition(l, FetchBegin { background: false });
    assert!(is_foreground_fetching(&l));
    l = transition(l, FetchEnd { background: false });
    assert_eq!((l.fetch_depth, l.background_depth), (1, 1));
    assert!(!is_foreground_fetching(&l));
    l = transition(l, FetchEnd { background: true });
    assert_eq!((l.fetch_depth, l.background_depth), (0, 0));
    assert_eq!(derive_status(&l), QueryStatus::Idle);
}

#[test]
fn an_unbalanced_end_never_leaves_more_background_cycles_than_cycles() {
    let l = transition(
        QueryLifecycle {
            fetch_depth: 1,
            background_depth: 1,
            ..at(QueryPhase::Live)
        },
        FetchEnd { background: false },
    );
    assert_eq!((l.fetch_depth, l.background_depth), (0, 0));
}

#[test]
fn notified_is_idempotent() {
    let l = transition(at(QueryPhase::Live), Notified);
    assert_eq!(transition(l, Notified), l);
}

#[test]
fn authority_and_server_membership_follow_the_phase() {
    assert!(!is_authoritative(&at(QueryPhase::Cold)));
    assert!(is_authoritative(&at(QueryPhase::Cached)));
    assert!(!has_server_membership(&at(QueryPhase::Cached)));
    assert!(has_server_membership(&at(QueryPhase::Live)));
    assert!(has_server_membership(&at(QueryPhase::ViewLost)));
    assert_eq!(seed_lifecycle(true).phase, QueryPhase::Cached);
}

#[test]
fn invariants_over_random_event_sequences() {
    let events = [
        MembershipApplied { present: true },
        RowMissing,
        RemoteRegistering,
        RemoteRegistered,
        RemoteDropped,
        FetchBegin { background: false },
        FetchEnd { background: false },
        FetchBegin { background: true },
        FetchEnd { background: true },
        Notified,
        BucketSwitch {
            resolved_before: false,
        },
        Seed {
            resolved_before: true,
        },
    ];
    // Deterministic LCG so the sequence is reproducible.
    let mut seed: u64 = 42;
    let mut rand = || {
        seed = (seed * 1_664_525 + 1_013_904_223) % 4_294_967_296;
        seed as f64 / 4_294_967_296.0
    };
    for _ in 0..200 {
        let mut l = seed_lifecycle(rand() < 0.5);
        for _ in 0..25 {
            let ev = events[(rand() * events.len() as f64).floor() as usize];
            let before = l;
            let Ok(next) = try_transition(l, ev) else {
                continue;
            };
            l = next;
            assert!(l.background_depth <= l.fetch_depth);
            if ev == RowMissing && before.phase == QueryPhase::Cold {
                assert_eq!(l.phase, QueryPhase::Cold);
            }
            if l.phase == QueryPhase::ViewLost {
                assert_ne!(before.phase, QueryPhase::Cold);
            }
        }
    }
}
