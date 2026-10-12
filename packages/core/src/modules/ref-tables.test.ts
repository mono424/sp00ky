import { describe, it, expect } from 'vitest';
import { RecordId } from 'surrealdb';
import { ANON_LIST_REF_TABLE, ANON_USER_ID, bucketIdForUser, listRefTableFor, sanitizeUserId } from './ref-tables';

describe('listRefTableFor', () => {
  it('returns global table in single mode regardless of user', () => {
    expect(listRefTableFor('single', 'user:abc')).toBe('_00_list_ref');
    expect(listRefTableFor('single', null)).toBe('_00_list_ref');
    expect(listRefTableFor('single', undefined)).toBe('_00_list_ref');
  });

  it('returns per-user table in dedicated mode with valid user id', () => {
    expect(listRefTableFor('dedicated', 'user:abc')).toBe(
      '_00_list_ref_user_abc'
    );
  });

  it('accepts a RecordId object in dedicated mode', () => {
    const rid = new RecordId('user', 'def');
    expect(listRefTableFor('dedicated', rid)).toBe('_00_list_ref_user_def');
  });

  it('falls back to global in dedicated mode when user id is missing', () => {
    expect(listRefTableFor('dedicated', null)).toBe('_00_list_ref');
    expect(listRefTableFor('dedicated', undefined)).toBe('_00_list_ref');
  });

  it('falls back to global in dedicated mode when user id has invalid chars', () => {
    // SurrealDB table identifiers only accept alphanumerics + underscore.
    expect(listRefTableFor('dedicated', 'user:abc-with-dash')).toBe(
      '_00_list_ref'
    );
    expect(listRefTableFor('dedicated', 'user:abc.dot')).toBe('_00_list_ref');
  });

  it('routes the anon sentinel to the shared anon table in both modes', () => {
    expect(listRefTableFor('dedicated', ANON_USER_ID)).toBe('_00_list_ref_anon');
    expect(listRefTableFor('single', ANON_USER_ID)).toBe('_00_list_ref_anon');
    // The LIVE builder keys its per-session filter off this constant.
    expect(ANON_LIST_REF_TABLE).toBe('_00_list_ref_anon');
    // A real user whose id sanitizes to "anon" still carries the user: prefix,
    // so it never collides with the bare sentinel.
    expect(listRefTableFor('dedicated', 'user:anon')).toBe(
      '_00_list_ref_user_anon'
    );
  });
});

describe('sanitizeUserId', () => {
  it('strips the user: prefix', () => {
    expect(sanitizeUserId('user:abc123')).toBe('abc123');
  });

  it('accepts plain ids without the user: prefix', () => {
    expect(sanitizeUserId('abc123')).toBe('abc123');
  });

  it('accepts RecordId objects', () => {
    expect(sanitizeUserId(new RecordId('user', 'xyz'))).toBe('xyz');
  });

  it('returns null for invalid id shapes', () => {
    expect(sanitizeUserId(null)).toBeNull();
    expect(sanitizeUserId(undefined)).toBeNull();
    expect(sanitizeUserId('user:')).toBeNull();
    expect(sanitizeUserId('user:has-dash')).toBeNull();
  });
});

describe('bucketIdForUser', () => {
  it('routes signed-out sessions to the anon bucket', () => {
    expect(bucketIdForUser(null)).toBe(ANON_USER_ID);
    expect(bucketIdForUser(undefined)).toBe(ANON_USER_ID);
    expect(bucketIdForUser(ANON_USER_ID)).toBe(ANON_USER_ID);
  });

  it('uses the sanitized id for valid users', () => {
    expect(bucketIdForUser('user:abc123')).toBe('abc123');
    expect(bucketIdForUser(new RecordId('user', 'xyz'))).toBe('xyz');
  });

  it('gives unsanitizable ids a deterministic per-user bucket, never anon', () => {
    const a = bucketIdForUser('user:has-dash');
    const b = bucketIdForUser('user:has-dash');
    const c = bucketIdForUser('user:other-dash');
    // Falling back to the shared anon bucket here would recreate the
    // cross-user local-cache leak this helper exists to prevent.
    expect(a).not.toBe(ANON_USER_ID);
    expect(a).toBe(b);
    expect(a).not.toBe(c);
    expect(a).toMatch(/^u[0-9a-f]+$/);
  });
});
