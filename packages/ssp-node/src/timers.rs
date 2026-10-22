//! Pure timer-multiplexing state machine.
//!
//! A Durable Object has exactly ONE alarm; the node needs many logical
//! timers ([`TimerKind`]). `TimerMux` is the mux: a deadline table with
//! replace-on-reschedule semantics, serializable so the CF shell can persist
//! it in DO storage across evictions and re-arm `set_alarm(next_deadline())`
//! after every mutation. Pure data — no IO, fully unit-testable on any
//! target. The VM shell doesn't need it (tokio can hold one task per timer),
//! but nothing stops a VM scheduler adapter from using it too.

use crate::ports::TimerKind;
use serde::{Deserialize, Serialize};

/// How long past its time an armed alarm may stay unfired before
/// [`TimerMux::alarm_to_arm`] sets it again.
pub const ALARM_LOST_AFTER_MS: u64 = 30_000;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TimerMux {
    /// (kind, due_at_epoch_ms). Small (single digits of entries) — linear
    /// scans beat heap bookkeeping at this size and keep serde trivial.
    entries: Vec<(TimerKind, u64)>,
}

impl TimerMux {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace the deadline for `kind`.
    pub fn schedule(&mut self, kind: TimerKind, at_epoch_ms: u64) {
        if let Some(entry) = self.entries.iter_mut().find(|(k, _)| *k == kind) {
            entry.1 = at_epoch_ms;
        } else {
            self.entries.push((kind, at_epoch_ms));
        }
    }

    /// Schedule `kind` at `at_epoch_ms` unless it is already pending at or
    /// before that time (the `Scheduler::ensure` contract). Returns whether
    /// the table changed.
    pub fn schedule_min(&mut self, kind: TimerKind, at_epoch_ms: u64) -> bool {
        match self.entries.iter_mut().find(|(k, _)| *k == kind) {
            Some(entry) if entry.1 <= at_epoch_ms => false,
            Some(entry) => {
                entry.1 = at_epoch_ms;
                true
            }
            None => {
                self.entries.push((kind, at_epoch_ms));
                true
            }
        }
    }

    pub fn cancel(&mut self, kind: &TimerKind) {
        self.entries.retain(|(k, _)| k != kind);
    }

    /// Earliest pending deadline — what the shell arms the platform alarm to.
    /// `None` = no pending timers, alarm can be cleared.
    pub fn next_deadline(&self) -> Option<u64> {
        self.entries.iter().map(|(_, at)| *at).min()
    }

    /// Remove and return every timer due at `now` (ordered by deadline).
    /// The shell dispatches each to `on_timer(kind)`, then re-arms the alarm
    /// to [`Self::next_deadline`] — handlers may have re-armed their kind.
    pub fn pop_due(&mut self, now_epoch_ms: u64) -> Vec<TimerKind> {
        let mut due: Vec<(TimerKind, u64)> = Vec::new();
        self.entries.retain(|(kind, at)| {
            if *at <= now_epoch_ms {
                due.push((kind.clone(), *at));
                false
            } else {
                true
            }
        });
        due.sort_by_key(|(_, at)| *at);
        due.into_iter().map(|(kind, _)| kind).collect()
    }

    /// The deadline to set the platform alarm to, or `None` when the armed
    /// alarm (`armed_at`) already covers the next deadline. A host calls it
    /// after any timer call, even one that changed nothing: an alarm
    /// invocation that dies between consuming the alarm and re-arming it
    /// leaves timers pending and none armed, and `schedule_min` on a kind that
    /// is already (over)due changes nothing. On Cloudflare that held the CRDT
    /// fold tick for 45 s under e2e load (2026-10-22). An armed alarm more than
    /// [`ALARM_LOST_AFTER_MS`] past its time is taken as lost too.
    pub fn alarm_to_arm(&self, armed_at: Option<u64>, now_epoch_ms: u64) -> Option<u64> {
        let next = self.next_deadline()?;
        match armed_at {
            Some(at) if at == next && now_epoch_ms <= at.saturating_add(ALARM_LOST_AFTER_MS) => None,
            _ => Some(next),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_replaces_deadline_for_same_kind() {
        let mut mux = TimerMux::new();
        mux.schedule(TimerKind::TtlCleanup, 1000);
        mux.schedule(TimerKind::TtlCleanup, 2000);
        assert_eq!(mux.len(), 1);
        assert_eq!(mux.next_deadline(), Some(2000));
    }

    #[test]
    fn schedule_min_never_postpones_a_pending_timer() {
        let mut mux = TimerMux::new();
        assert!(mux.schedule_min(TimerKind::TtlCleanup, 1000), "absent: added");
        assert!(!mux.schedule_min(TimerKind::TtlCleanup, 5000), "later: kept at 1000");
        assert_eq!(mux.next_deadline(), Some(1000));
        assert!(mux.schedule_min(TimerKind::TtlCleanup, 500), "earlier: moved");
        assert_eq!(mux.next_deadline(), Some(500));
        assert!(!mux.schedule_min(TimerKind::TtlCleanup, 500), "same: unchanged");
    }

    #[test]
    fn a_missing_or_lost_alarm_is_armed_again_even_when_nothing_changed() {
        let mut mux = TimerMux::new();
        assert_eq!(mux.alarm_to_arm(None, 0), None, "nothing pending, nothing to arm");
        mux.schedule(TimerKind::CrdtFoldTick, 1_000);
        mux.schedule(TimerKind::TtlCleanup, 5_000);
        assert_eq!(mux.alarm_to_arm(None, 0), Some(1_000), "pending and none armed");
        assert_eq!(mux.alarm_to_arm(Some(1_000), 900), None, "armed at the next deadline");
        assert_eq!(mux.alarm_to_arm(Some(1_000), 1_000 + ALARM_LOST_AFTER_MS), None, "late, not yet lost");
        assert_eq!(mux.alarm_to_arm(Some(1_000), 1_001 + ALARM_LOST_AFTER_MS), Some(1_000), "lost: armed again");
        assert_eq!(mux.alarm_to_arm(Some(5_000), 0), Some(1_000), "armed for a later timer");

        // The alarm consumed, the invocation gone before it re-armed: the fold
        // tick is still pending and overdue, and ensuring it again changes nothing.
        assert!(!mux.schedule_min(TimerKind::CrdtFoldTick, 3_000));
        assert_eq!(mux.alarm_to_arm(None, 3_000), Some(1_000));
    }

    #[test]
    fn parameterized_kinds_are_distinct_timers() {
        let mut mux = TimerMux::new();
        mux.schedule(TimerKind::JobRetry { id: "a".into() }, 100);
        mux.schedule(TimerKind::JobRetry { id: "b".into() }, 200);
        mux.schedule(TimerKind::JobRetry { id: "a".into() }, 300); // replaces "a"
        assert_eq!(mux.len(), 2);
        assert_eq!(mux.next_deadline(), Some(200));
    }

    #[test]
    fn pop_due_returns_ordered_and_keeps_future() {
        let mut mux = TimerMux::new();
        mux.schedule(TimerKind::EdgeFlush, 300);
        mux.schedule(TimerKind::TtlCleanup, 100);
        mux.schedule(TimerKind::JobRecoverySweep, 200);
        mux.schedule(TimerKind::BackendHealth, 900);

        let due = mux.pop_due(300);
        assert_eq!(
            due,
            vec![
                TimerKind::TtlCleanup,
                TimerKind::JobRecoverySweep,
                TimerKind::EdgeFlush
            ]
        );
        assert_eq!(mux.next_deadline(), Some(900));

        // Re-arm inside "on_timer" — the recurring pattern.
        mux.schedule(TimerKind::TtlCleanup, 400);
        assert_eq!(mux.next_deadline(), Some(400));
    }

    #[test]
    fn cancel_removes_only_target() {
        let mut mux = TimerMux::new();
        mux.schedule(TimerKind::DelayedJob { id: "x".into() }, 100);
        mux.schedule(TimerKind::DelayedJob { id: "y".into() }, 100);
        mux.cancel(&TimerKind::DelayedJob { id: "x".into() });
        assert_eq!(mux.pop_due(100), vec![TimerKind::DelayedJob { id: "y".into() }]);
        assert!(mux.is_empty());
        assert_eq!(mux.next_deadline(), None);
    }

    #[test]
    fn survives_serde_round_trip() {
        let mut mux = TimerMux::new();
        mux.schedule(TimerKind::BootstrapRetry { attempt: 3 }, 500);
        mux.schedule(TimerKind::DbResignin, 900_000);

        let json = serde_json::to_string(&mux).unwrap();
        let mut restored: TimerMux = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.next_deadline(), Some(500));
        assert_eq!(
            restored.pop_due(500),
            vec![TimerKind::BootstrapRetry { attempt: 3 }]
        );
    }

    #[test]
    fn timer_kind_serde_round_trip() {
        for kind in [
            TimerKind::JobRecoverySweep,
            TimerKind::TtlCleanup,
            TimerKind::EdgeFlush,
            TimerKind::BackendHealth,
            TimerKind::DbResignin,
            TimerKind::DelayedJob { id: "j1".into() },
            TimerKind::JobRetry { id: "j2".into() },
            TimerKind::BootstrapRetry { attempt: 7 },
        ] {
            let json = serde_json::to_string(&kind).unwrap();
            let back: TimerKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind, back, "round trip failed for {json}");
        }
    }
}
