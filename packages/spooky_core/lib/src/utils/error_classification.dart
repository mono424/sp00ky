const _networkErrorPatterns = [
  'connection',
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

/// Classify a sync error as recoverable (`network`) or terminal
/// (`application`). Faithful port of TS `classifySyncError`.
String classifySyncError(Object? error) {
  final message =
      (error is Error ? error.toString() : error.toString()).toLowerCase();
  for (final pattern in _networkErrorPatterns) {
    if (message.contains(pattern)) {
      return 'network';
    }
  }
  return 'application';
}
