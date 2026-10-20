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

  it.each([
    'Permission denied',
    'There was a problem with the database: record already exists',
    'Parse error: unexpected token',
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
