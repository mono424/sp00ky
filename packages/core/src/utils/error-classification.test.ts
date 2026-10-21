import { describe, it, expect } from 'vitest';
import { classifySyncError, isRefusedSessionError } from './error-classification';

describe('classifySyncError', () => {
  it('classifies surreal ConnectionUnavailableError as network', () => {
    // Thrown synchronously by the WS engine's send() while the socket is down
    // but reconnect hasn't fired. Message contains "connected", not "connection".
    const err = new Error(
      'You must be connected to a SurrealDB instance before performing this operation'
    );
    expect(classifySyncError(err)).toBe('network');
  });

  it('classifies surreal CallTerminatedError as network', () => {
    const err = new Error(
      'The call has been terminated because the connection was closed'
    );
    expect(classifySyncError(err)).toBe('network');
  });

  it.each([
    'WebSocket connection failed',
    'fetch failed',
    'connect ECONNREFUSED 127.0.0.1:8666',
    'request timed out',
    'The operation was aborted',
    'socket hang up',
  ])('classifies %j as network', (message) => {
    expect(classifySyncError(new Error(message))).toBe('network');
  });

  it.each([
    // SurrealDB optimistic-concurrency conflict: retryable by definition.
    'Transaction conflict: Resource busy: . This transaction can be retried',
    // Session state not re-applied yet after the SDK's reconnect handshake.
    'Specify a namespace to use',
    'Specify a database to use',
  ])('classifies transient server state %j as network (retry, never rollback)', (message) => {
    expect(classifySyncError(new Error(message))).toBe('network');
  });

  // Verbatim from SurrealDB 3.1.5 and 3.3.4: the write was refused because
  // sp00ky's own ingest event could not reach the scheduler / SSP. The outbox
  // must keep these queued, never roll them back (see ingest-outage.integration.test.ts).
  it.each([
    // Refused connection and a post past the event TIMEOUT read the same.
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: error sending request for url (http://scheduler:9667/ingest)',
    'Error while processing event _00_item_delete: There was an error processing a remote HTTP request: error sending request for url (http://host.docker.internal:50466/ingest)',
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 500 Internal Server Error',
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 502 Bad Gateway',
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 503 Service Unavailable',
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 408 Request Timeout',
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 429 Too Many Requests',
    'Error while processing event _00_item_mutation: failed to lookup address information: Name or service not known',
    // An app event that wrote to a synced table carries the inner event.
    'Error while processing event app_fanout: Error while processing event _00_b_mutation: There was an error processing a remote HTTP request: 503 Service Unavailable',
  ])('classifies a failed sp00ky ingest post %j as network (retry, never rollback)', (message) => {
    expect(classifySyncError(new Error(message))).toBe('network');
  });

  it.each([
    'Permission denied',
    'There was a problem with the database: record already exists',
    'Parse error: unexpected token',
    "Couldn't coerce value for field `title` of `item:1`: Expected `string` but found `1`",
    // An app's own event: its endpoint, its THROW, its business.
    'Error while processing event notify_crm: There was an error processing a remote HTTP request: 500 Internal Server Error',
    'Error while processing event notify_crm: There was an error processing a remote HTTP request: error sending request for url (http://crm.example/hook)',
    'Error while processing event cf_guard: An error occurred: conformance guard: write refused',
    // A THROW inside a sp00ky event is not a failed post.
    'Error while processing event _00_item_mutation: An error occurred: inner',
    // A 4xx from /ingest no retry fixes: payload rejected, too large, secret mismatch.
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 400 Bad Request',
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 401 Unauthorized',
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 413 Payload Too Large',
    'Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: 422 Unprocessable Entity',
  ])('classifies %j as application', (message) => {
    expect(classifySyncError(new Error(message))).toBe('application');
  });

  it('handles non-Error values', () => {
    expect(classifySyncError('connection reset')).toBe('network');
    expect(classifySyncError('boom')).toBe('application');
  });
});

describe('isRefusedSessionError', () => {
  it.each([
    'There was a problem with authentication',
    'An error occurred: There was a problem with authentication: this session has ended, sign in again',
    'The session has expired',
  ])('takes %j as a refused session', (message) => {
    expect(isRefusedSessionError(new Error(message))).toBe(true);
  });

  it.each([
    'There was a problem with the database: Invalid token',
    'There was a problem with the underlying connection',
    'Permission denied',
  ])('does not take %j as one', (message) => {
    expect(isRefusedSessionError(new Error(message))).toBe(false);
  });
});
