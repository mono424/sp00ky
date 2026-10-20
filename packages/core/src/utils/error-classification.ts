const NETWORK_ERROR_PATTERNS = [
  'connection',
  // surreal's ConnectionUnavailableError reads "You must be connected to a
  // SurrealDB instance..." — it contains "connected", not "connection", so it
  // slips past the pattern above. The WS engine throws it synchronously from
  // `send()` while the socket is down but reconnect hasn't fired yet, so it's the
  // canonical error on an idle-dropped socket; classify it as network.
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

export function classifySyncError(error: unknown): 'network' | 'application' {
  const message =
    error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

  for (const pattern of NETWORK_ERROR_PATTERNS) {
    if (message.includes(pattern)) {
      return 'network';
    }
  }

  return 'application';
}

/**
 * Whether the server refused the SESSION rather than the statement: its own
 * wording for a token it will not accept ("There was a problem with
 * authentication"), which is also what an app's AUTHENTICATE clause and
 * sp00ky's session guard throw, and a session past its `DURATION FOR SESSION`.
 */
export function isRefusedSessionError(error: unknown): boolean {
  const message =
    error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  return message.includes('problem with authentication') || message.includes('session has expired');
}
