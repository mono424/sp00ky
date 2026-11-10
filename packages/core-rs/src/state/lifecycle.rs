//! The query lifecycle as one explicit machine.
//!
//! [`QueryPhase`] answers "where do this query's rows come from":
//!
//! - `Cold`: never resolved on this device. Rows come from the circuit's
//!   local window, bindings show a loader.
//! - `Cached`: a durable `_00_view` row was found. Rows come from that id-set.
//! - `Live`: a server membership set was accepted this session.
//! - `ViewLost`: the server's `_00_query` row vanished while we held
//!   membership. Rows are kept, a re-registration is under way.
//!
//! `remote` tracks the server-side registration, `fetch_depth` is the refcount
//! behind `status: fetching`, and `notified` says whether subscribers received
//! at least one materialization this registration. `background_depth` is the
//! part of `fetch_depth` that re-verifies rows the query already shows: it
//! holds `fetching`, but the activity indicator leaves it out.

use crate::types::QueryStatus;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryPhase {
    Cold,
    Cached,
    Live,
    ViewLost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemotePhase {
    Unregistered,
    Registering,
    Registered,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryLifecycle {
    pub phase: QueryPhase,
    pub remote: RemotePhase,
    pub fetch_depth: u32,
    pub background_depth: u32,
    pub notified: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleEvent {
    Seed { resolved_before: bool },
    MembershipApplied { present: bool },
    RowMissing,
    RemoteRegistering,
    RemoteRegistered,
    RemoteFailed,
    RemoteDropped,
    FetchBegin { background: bool },
    FetchEnd { background: bool },
    Notified,
    BucketSwitch { resolved_before: bool },
}

/// An impossible `(lifecycle, event)` pair: a bug in a saga.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("impossible lifecycle transition: {lifecycle:?} + {event:?}")]
pub struct LifecycleError {
    pub lifecycle: QueryLifecycle,
    pub event: LifecycleEvent,
}

pub fn seed_lifecycle(resolved_before: bool) -> QueryLifecycle {
    QueryLifecycle {
        phase: if resolved_before {
            QueryPhase::Cached
        } else {
            QueryPhase::Cold
        },
        remote: RemotePhase::Unregistered,
        fetch_depth: 0,
        background_depth: 0,
        notified: false,
    }
}

/// Total on every pair the sagas can produce; an error on the rest.
pub fn try_transition(l: QueryLifecycle, ev: LifecycleEvent) -> Result<QueryLifecycle, LifecycleError> {
    use LifecycleEvent::*;
    Ok(match ev {
        Seed { resolved_before } => seed_lifecycle(resolved_before),
        MembershipApplied { present } => {
            if l.phase == QueryPhase::ViewLost && !present {
                return Err(LifecycleError { lifecycle: l, event: ev });
            }
            QueryLifecycle {
                phase: QueryPhase::Live,
                ..l
            }
        }
        // A cold query holds nothing the server could have lost: the phase
        // does not move.
        RowMissing => {
            if l.phase == QueryPhase::Cold {
                l
            } else {
                QueryLifecycle {
                    phase: QueryPhase::ViewLost,
                    ..l
                }
            }
        }
        RemoteRegistering => QueryLifecycle {
            remote: RemotePhase::Registering,
            ..l
        },
        RemoteRegistered => QueryLifecycle {
            remote: RemotePhase::Registered,
            ..l
        },
        RemoteFailed => QueryLifecycle {
            remote: RemotePhase::Failed,
            ..l
        },
        RemoteDropped => QueryLifecycle {
            remote: RemotePhase::Unregistered,
            notified: false,
            ..l
        },
        FetchBegin { background } => QueryLifecycle {
            fetch_depth: l.fetch_depth + 1,
            background_depth: l.background_depth + u32::from(background),
            ..l
        },
        FetchEnd { background } => {
            if l.fetch_depth == 0 {
                return Ok(l);
            }
            let fetch_depth = l.fetch_depth - 1;
            // Never more background cycles than cycles: an unbalanced end clamps.
            let dec = u32::from(background && l.background_depth > 0);
            let background_depth = fetch_depth.min(l.background_depth - dec);
            QueryLifecycle {
                fetch_depth,
                background_depth,
                ..l
            }
        }
        Notified => {
            if l.notified {
                l
            } else {
                QueryLifecycle { notified: true, ..l }
            }
        }
        BucketSwitch { resolved_before } => seed_lifecycle(resolved_before),
    })
}

/// [`try_transition`] for reducers: an impossible pair is logged and leaves
/// the lifecycle as it was.
pub fn transition(l: QueryLifecycle, ev: LifecycleEvent) -> QueryLifecycle {
    try_transition(l, ev).unwrap_or_else(|e| {
        tracing::error!(target: "spooky", "{e}");
        l
    })
}

pub fn is_authoritative(l: &QueryLifecycle) -> bool {
    l.phase != QueryPhase::Cold
}

pub fn has_server_membership(l: &QueryLifecycle) -> bool {
    matches!(l.phase, QueryPhase::Live | QueryPhase::ViewLost)
}

pub fn derive_status(l: &QueryLifecycle) -> QueryStatus {
    if l.fetch_depth > 0 {
        QueryStatus::Fetching
    } else {
        QueryStatus::Idle
    }
}

/// A fetch cycle the user is waiting on: rows to download, or a first
/// registration.
pub fn is_foreground_fetching(l: &QueryLifecycle) -> bool {
    l.fetch_depth > l.background_depth
}

#[cfg(test)]
mod tests {
    use super::*;
    use LifecycleEvent::*;

    #[test]
    fn seed_and_membership() {
        let l = seed_lifecycle(false);
        assert_eq!(l.phase, QueryPhase::Cold);
        let l = transition(l, MembershipApplied { present: true });
        assert_eq!(l.phase, QueryPhase::Live);
        assert!(is_authoritative(&l));
    }

    #[test]
    fn view_lost_needs_present() {
        let l = transition(seed_lifecycle(true), RowMissing);
        assert_eq!(l.phase, QueryPhase::ViewLost);
        assert!(try_transition(l, MembershipApplied { present: false }).is_err());
        assert_eq!(transition(seed_lifecycle(false), RowMissing).phase, QueryPhase::Cold);
    }

    #[test]
    fn fetch_depth_clamps() {
        let l = seed_lifecycle(true);
        let l = transition(l, FetchBegin { background: true });
        let l = transition(l, FetchBegin { background: false });
        assert_eq!((l.fetch_depth, l.background_depth), (2, 1));
        assert!(is_foreground_fetching(&l));
        let l = transition(l, FetchEnd { background: false });
        assert_eq!((l.fetch_depth, l.background_depth), (1, 1));
        assert!(!is_foreground_fetching(&l));
        assert_eq!(derive_status(&l), QueryStatus::Fetching);
        let l = transition(l, FetchEnd { background: true });
        let l = transition(l, FetchEnd { background: true });
        assert_eq!((l.fetch_depth, l.background_depth), (0, 0));
    }

    #[test]
    fn dropped_resets_notified() {
        let l = transition(seed_lifecycle(true), Notified);
        assert!(l.notified);
        let l = transition(l, RemoteDropped);
        assert!(!l.notified);
        assert_eq!(l.remote, RemotePhase::Unregistered);
    }
}
