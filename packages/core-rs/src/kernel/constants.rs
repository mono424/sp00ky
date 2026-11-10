//! Every timing constant of the saga core in one place. Sagas never create a
//! timer of their own; they call `ctx.timer_set` with one of these delays, so a
//! test can pin the schedule. Values are copied from
//! `packages/core/src/kernel/constants.ts` so the Rust, Dart and TypeScript
//! clients back off identically against the same backend.

/// Minimum gap between two materializations of one query.
pub const MATERIALIZE_DEBOUNCE_MS: u64 = 50;

/// Minimum gap between two batched edge re-reads driven by LIVE dirt.
pub const MEMBERSHIP_COALESCE_MS: u64 = 50;

/// Leading wait before the first re-read of a burst.
pub const MEMBERSHIP_LEADING_MS: u64 = 5;

/// Re-read ladder while the server reports a view as `materializing`.
pub const MATERIALIZING_REREAD_LADDER_MS: [u64; 3] = [150, 400, 1000];

/// How long an acked outbox item stays in the overlay when membership never
/// names it.
pub const ACK_GRACE_MS: u64 = 30_000;

/// Ids per body-fetch statement.
pub const FETCH_CHUNK: usize = 500;

/// Outbox statements per push request.
pub const OUTBOX_BATCH_SIZE: usize = 50;

/// Push deadline per outbox batch.
pub const PUSH_TIMEOUT_MS: u64 = 30_000;

/// Exponential backoff for outbox / fetch / registration retries.
pub const RETRY_BASE_MS: u64 = 500;
pub const RETRY_MAX_MS: u64 = 15_000;

/// Registration attempts before a query is reported as failed to settle.
pub const REGISTER_MAX_RETRIES: u32 = 3;

/// Pacing of repeated view-lost recovery.
pub const VIEW_LOST_RETRY_BASE_MS: u64 = 1000;
pub const VIEW_LOST_RETRY_MAX_MS: u64 = 30_000;

/// `_00_list_ref` poll cadence (fallback when LIVE is quiet).
pub const LIST_REF_POLL_BASE_MS: u64 = 500;
pub const LIST_REF_POLL_MAX_MS: u64 = 5000;
pub const LIST_REF_ROW_BUDGET: usize = 1500;
pub const LIST_REF_LARGE_VIEW_EDGES: usize = 1000;
pub const LIST_REF_LARGE_VIEW_POLL_MS: u64 = 15_000;

/// Self-heal cadence while sync health is degraded.
pub const SELF_HEAL_BASE_MS: u64 = 2000;
pub const SELF_HEAL_MAX_MS: u64 = 30_000;

/// Consecutive failed rounds before health flips to degraded.
pub const DEGRADE_AFTER_FAILURES: u32 = 3;

/// Burst coalescing of reconnects.
pub const RECONNECT_REFETCH_COOLDOWN_MS: u64 = 10_000;

/// Waiting for `$auth.id` to be visible after a reconnect.
pub const AUTH_READY_RETRY_MS: u64 = 500;
pub const AUTH_READY_MAX_ATTEMPTS: u32 = 10;

/// Heartbeat at this fraction of the shortest ttl in state.
pub const TTL_HEARTBEAT_FRACTION: f64 = 0.5;

/// Orphan body collection: first sweep this long after boot, then every
/// [`GC_INTERVAL_MS`], deleting [`GC_CHUNK`] bodies per step.
pub const GC_BOOT_DELAY_MS: u64 = 30_000;
pub const GC_INTERVAL_MS: u64 = 60 * 60 * 1000;
pub const GC_CHUNK: usize = 200;

/// A `_00_view` row no query has resolved for this long is retired.
pub const VIEW_RETENTION_MS: i64 = 14 * 24 * 60 * 60 * 1000;

/// Rolling telemetry sample window per query.
pub const TELEMETRY_SAMPLE_WINDOW: usize = 100;

/// `base * 2^attempt`, capped at `max`.
pub fn backoff_ms_with(attempt: u32, base: u64, max: u64) -> u64 {
    let n = attempt.min(30);
    base.saturating_mul(1u64 << n).min(max)
}

/// [`backoff_ms_with`] on the retry defaults.
pub fn backoff_ms(attempt: u32) -> u64 {
    backoff_ms_with(attempt, RETRY_BASE_MS, RETRY_MAX_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_caps() {
        assert_eq!(backoff_ms(0), 500);
        assert_eq!(backoff_ms(1), 1000);
        assert_eq!(backoff_ms(5), 15_000);
        assert_eq!(backoff_ms(40), 15_000);
    }
}
