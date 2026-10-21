const _networkErrorPatterns = [
  'connection',
  // Session fencing cancels transport work, never rejects the user's write.
  'account changed',
  // Surreal's ConnectionUnavailableError reads "You must be connected to a
  // SurrealDB instance..." — it contains "connected", not "connection", so it
  // slips past the pattern above. The WS client throws it while the socket is
  // down but reconnect hasn't fired yet, so it's the canonical error on an
  // idle-dropped socket: classify it as network so the mutation is re-queued
  // rather than rolled back and dropped.
  'must be connected',
  'connectionunavailable',
  'timeout',
  'timed out',
  'websocket',
  'fetch failed',
  'disconnected',
  'socket',
  'network',
  'econnrefused',
  'econnreset',
  'enotfound',
  'epipe',
  'abort',
  // Transient server-side states that are NOT the mutation's fault and clear
  // on their own, so the outbox must retry them rather than roll the write
  // back. Seen 2026-09-08 on whitepawn: a 1200-game import lost 752 games and
  // ~1500 player_name rows because every queued CREATE that hit one of these
  // during a DB stall + WebSocket reconnect was classified `application`,
  // rolled back locally and discarded - silent data loss.
  //
  // - SurrealDB optimistic-concurrency conflicts: "Transaction conflict:
  //   Resource busy: . This transaction can be retried".
  // - A socket whose namespace/identity has not been re-applied yet after the
  //   SDK's own reconnect handshake: "Specify a namespace to use" /
  //   "Specify a database to use". The statement is fine; the session is not
  //   ready.
  'transaction conflict',
  'resource busy',
  'can be retried',
  'specify a namespace',
  'specify a database',
];

/// A write SurrealDB refused because sp00ky's OWN ingest post failed: every
/// synced table's `_00_<table>_mutation` / `_00_<table>_delete` event posts to
/// the scheduler or SSP inside the write's transaction
/// (`sp00ky.rs::ingest_post`), so a scheduler that is down, slow past the
/// event TIMEOUT, or answering 5xx aborts the write. That is an outage, not
/// the mutation's fault, and it clears on its own; classified `application`
/// it was rolled back and discarded.
///
/// Wording on SurrealDB 3.1.5 and 3.3.4 (the event name may be nested inside
/// an app event that wrote to a synced table):
///   "Error while processing event _00_item_mutation: There was an error
///    processing a remote HTTP request: error sending request for url (...)"
///    (refused connection or TIMEOUT), "...: 503 Service Unavailable" (5xx),
///   "Error while processing event _00_item_mutation: failed to lookup
///    address information: ..." (DNS).
///
/// Only `_00_` events count: an app's own event posting to its own endpoint
/// stays `application`, as does a 4xx from `/ingest` other than 408/429 (a
/// malformed or oversized payload, a secret mismatch), which no retry fixes
/// and which would otherwise hold the outbox's serial lane forever.
final _sp00kyEventPostFailed = RegExp(
    r'error while processing event _00_[^:\s]*: (?:there was an error processing a remote http request: (?:error sending request|5\d\d\b|408\b|429\b)|failed to lookup address information)');

/// Classify a sync error as recoverable (`network`) or terminal
/// (`application`). Faithful port of TS `classifySyncError`.
String classifySyncError(Object? error) {
  final message =
      (error is Error ? error.toString() : error.toString()).toLowerCase();
  if (_sp00kyEventPostFailed.hasMatch(message)) return 'network';
  for (final pattern in _networkErrorPatterns) {
    if (message.contains(pattern)) {
      return 'network';
    }
  }
  return 'application';
}
