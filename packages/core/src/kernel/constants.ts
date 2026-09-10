/**
 * Every timing constant of the saga core in one place. Sagas never call
 * `setTimeout`; they yield `timer.set` effects with one of these delays, so a
 * test can pin the schedule by asserting the effect log.
 */

/** Trailing coalesce for materialization after a state change. */
export const MATERIALIZE_DEBOUNCE_MS = 50;
/** Coalesce window for LIVE membership dirt before the batched edge re-read. */
export const MEMBERSHIP_COALESCE_MS = 50;
/** Re-read ladder while the server reports a view as `materializing`. */
export const MATERIALIZING_REREAD_LADDER_MS: readonly number[] = [150, 400, 1000];
/** How long an acked outbox item stays in the overlay when membership never names it. */
export const ACK_GRACE_MS = 30_000;
/** Ids per body-fetch statement. */
export const FETCH_CHUNK = 500;
/** Outbox statements per push request. */
export const OUTBOX_BATCH_SIZE = 50;
/** Push deadline per outbox batch. */
export const PUSH_TIMEOUT_MS = 30_000;
/** Exponential backoff for outbox / fetch / registration retries. */
export const RETRY_BASE_MS = 500;
export const RETRY_MAX_MS = 15_000;
/** Registration attempts before a query is reported as failed to settle. */
export const REGISTER_MAX_RETRIES = 3;
/** `_00_list_ref` poll cadence (fallback when LIVE is quiet). */
export const LIST_REF_POLL_BASE_MS = 500;
export const LIST_REF_POLL_MAX_MS = 5_000;
export const LIST_REF_ROW_BUDGET = 1_500;
export const LIST_REF_LARGE_VIEW_EDGES = 1_000;
export const LIST_REF_LARGE_VIEW_POLL_MS = 15_000;
/** Self-heal cadence while sync health is degraded. */
export const SELF_HEAL_BASE_MS = 2_000;
export const SELF_HEAL_MAX_MS = 30_000;
/** Consecutive failed rounds before health flips to degraded. */
export const DEGRADE_AFTER_FAILURES = 3;
/** Burst coalescing of reconnects. */
export const RECONNECT_REFETCH_COOLDOWN_MS = 10_000;
/** Waiting for `$auth.id` to be visible after a reconnect. */
export const AUTH_READY_RETRY_MS = 500;
export const AUTH_READY_MAX_ATTEMPTS = 10;
/** Heartbeat at this fraction of the shortest ttl in state. */
export const TTL_HEARTBEAT_FRACTION = 0.5;
/** Orphan body garbage collection cadence. */
export const GC_INTERVAL_MS = 7 * 24 * 60 * 60 * 1000;
/** Remote adapter slot limit. */
export const REMOTE_CONCURRENCY = 16;
/** Rolling telemetry sample window per query. */
export const TELEMETRY_SAMPLE_WINDOW = 100;

export function backoffMs(attempt: number, base = RETRY_BASE_MS, max = RETRY_MAX_MS): number {
  const n = Math.max(0, Math.min(attempt, 30));
  return Math.min(max, base * 2 ** n);
}
