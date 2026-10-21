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

/**
 * A write SurrealDB refused because sp00ky's OWN ingest post failed: every
 * synced table's `_00_<table>_mutation` / `_00_<table>_delete` event posts to
 * the scheduler or SSP inside the write's transaction (`sp00ky.rs::ingest_post`),
 * so a scheduler that is down, slow past the event TIMEOUT, or answering 5xx
 * aborts the write. That is an outage, not the mutation's fault, and it clears
 * on its own; classified `application` it was rolled back and discarded, so
 * every write queued during a scheduler restart or a cold SSP was lost.
 *
 * Wording on SurrealDB 3.1.5 and 3.3.4 (the event name may be nested inside an
 * app event that wrote to a synced table):
 *   "Error while processing event _00_item_mutation: There was an error
 *    processing a remote HTTP request: error sending request for url (...)"
 *    (refused connection or TIMEOUT), "...: 503 Service Unavailable" (5xx),
 *   "Error while processing event _00_item_mutation: failed to lookup address
 *    information: ..." (DNS).
 *
 * Only `_00_` events count: an app's own event posting to its own endpoint
 * stays `application`, as does a 4xx from `/ingest` other than 408/429 (a
 * malformed or oversized payload, a secret mismatch), which no retry fixes and
 * which would otherwise hold the outbox's serial lane forever.
 */
const SP00KY_EVENT_POST_FAILED =
  /error while processing event _00_[^:\s]*: (?:there was an error processing a remote http request: (?:error sending request|5\d\d\b|408\b|429\b)|failed to lookup address information)/;

export function classifySyncError(error: unknown): 'network' | 'application' {
  const message =
    error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

  if (SP00KY_EVENT_POST_FAILED.test(message)) return 'network';

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
