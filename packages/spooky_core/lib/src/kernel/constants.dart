/// Every timing constant of the saga core in one place. Sagas never create a
/// `Timer`; they yield `timer.set` effects with one of these delays, so a test
/// can pin the schedule by asserting the effect log.
///
/// Values are copied verbatim from `packages/core/src/kernel/constants.ts` so
/// the Dart and TypeScript clients back off identically against the same
/// backend.
library;

/// Trailing coalesce for materialization after a state change.
const int materializeDebounceMs = 50;

/// Coalesce window for LIVE membership dirt before the batched edge re-read.
const int membershipCoalesceMs = 50;

/// Re-read ladder while the server reports a view as `materializing`.
const List<int> materializingRereadLadderMs = [150, 400, 1000];

/// How long an acked outbox item stays in the overlay when membership never
/// names it.
const int ackGraceMs = 30000;

/// Ids per body-fetch statement.
const int fetchChunk = 500;

/// Outbox statements per push request.
const int outboxBatchSize = 50;

/// Push deadline per outbox batch.
const int pushTimeoutMs = 30000;

/// Exponential backoff for outbox / fetch / registration retries.
const int retryBaseMs = 500;
const int retryMaxMs = 15000;

/// Registration attempts before a query is reported as failed to settle.
const int registerMaxRetries = 3;

/// `_00_list_ref` poll cadence (fallback when LIVE is quiet).
const int listRefPollBaseMs = 500;
const int listRefPollMaxMs = 5000;
const int listRefRowBudget = 1500;
const int listRefLargeViewEdges = 1000;
const int listRefLargeViewPollMs = 15000;

/// Self-heal cadence while sync health is degraded.
const int selfHealBaseMs = 2000;
const int selfHealMaxMs = 30000;

/// Consecutive failed rounds before health flips to degraded.
const int degradeAfterFailures = 3;

/// Burst coalescing of reconnects.
const int reconnectRefetchCooldownMs = 10000;

/// Waiting for `$auth.id` to be visible after a reconnect.
const int authReadyRetryMs = 500;
const int authReadyMaxAttempts = 10;

/// Heartbeat at this fraction of the shortest ttl in state.
const double ttlHeartbeatFraction = 0.5;

/// Orphan body garbage collection cadence.
const int gcIntervalMs = 7 * 24 * 60 * 60 * 1000;

/// Rolling telemetry sample window per query.
const int telemetrySampleWindow = 100;

int backoffMs(int attempt, {int base = retryBaseMs, int max = retryMaxMs}) {
  final n = attempt.clamp(0, 30);
  final scaled = base * (1 << n);
  return scaled < max ? scaled : max;
}
