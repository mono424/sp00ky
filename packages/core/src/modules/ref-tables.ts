// Client-side mirror of `packages/ssp-protocol/src/lib.rs`'s
// `query_table_for` / `list_ref_table_for`. Same naming convention so
// the LIVE subscription, the initial-fetch read, and the SSP's writes
// all land on the same table.
//
// The mode is currently hardcoded to `dedicated` because that's the
// only mode the e2e suite exercises and threading the value through
// codegen wasn't necessary to land the cross-session fix. If single
// mode ever needs to be exposed from the TS client too, the SSP server
// already reads it from `SPKY_SSP_REF_MODE`; add a matching codegen
// export then.

import { cyrb53 } from '@spooky-sync/query-builder';

export type RefMode = 'single' | 'dedicated';

/**
 * Sentinel user id for unauthenticated clients when anonymous live queries are
 * enabled. Mirrors `ssp_protocol::ANON_AUTH_ID`. It carries no `user:` prefix
 * so it can never collide with a real user id (those arrive as `user:<id>`);
 * both sides resolve it to the shared `_00_list_ref_anon` table.
 */
export const ANON_USER_ID = 'anon';

/** The one `_00_list_ref` table every signed-out session shares. */
export const ANON_LIST_REF_TABLE = '_00_list_ref_anon';

/**
 * Default ref-storage mode for this client build. Mirrors the SSP's
 * default (`RefMode::Dedicated`) so cross-session sync works out of the
 * box.
 */
export const DEFAULT_REF_MODE: RefMode = 'dedicated';

/**
 * Sanitize a user record id (e.g. `"user:abc"`) into the segment that
 * goes into a dedicated table name (e.g. `"abc"`). Returns `null` if
 * the id is missing the `user:` prefix or contains characters that
 * aren't valid in a SurrealDB table identifier — the server-side
 * `ssp_protocol::sanitize_user_id` uses the same predicate.
 *
 * Accepts both string ids (`"user:abc"`) and SurrealDB `RecordId`
 * objects (which only stringify cleanly via `.toString()`), since
 * `AuthService` passes the record-id object as-is to its subscribers.
 */
export function sanitizeUserId(userId: unknown): string | null {
  if (userId === null || userId === undefined) return null;
  const asString =
    typeof userId === 'string'
      ? userId
      : typeof (userId as { toString?: unknown }).toString === 'function'
        ? (userId as { toString: () => string }).toString()
        : null;
  if (!asString) return null;
  const raw = asString.startsWith('user:') ? asString.slice('user:'.length) : asString;
  if (raw.length === 0) return null;
  if (!/^[A-Za-z0-9_]+$/.test(raw)) return null;
  return raw;
}

/**
 * Resolve the LOCAL storage bucket id for a user. Every user gets their own
 * IndexedDB-backed local store (`indxdb://sp00ky-<bucketId>`) so cached rows,
 * query state, and the mutation outbox never leak across accounts on a shared
 * device. Signed-out sessions share the `anon` bucket.
 *
 * An id that fails sanitization still gets a DETERMINISTIC per-user bucket
 * (cyrb53 hex of the raw id) — falling back to `anon` here would put an
 * authenticated user in the shared bucket and recreate the cross-user leak.
 */
export function bucketIdForUser(userId: unknown): string {
  if (userId === null || userId === undefined || userId === ANON_USER_ID) return ANON_USER_ID;
  const uid = sanitizeUserId(userId);
  if (uid) return uid;
  return `u${cyrb53(String(userId)).toString(16)}`;
}

/**
 * Returns the `_00_list_ref` table name for `(mode, userId)`. Falls
 * back to the global `_00_list_ref` when sanitization fails or in
 * single mode.
 */
export function listRefTableFor(mode: RefMode, userId: unknown): string {
  // Anonymous clients (flag-enabled) share one dedicated table in both modes —
  // checked before the mode split so it never lands on the per-user or the
  // auth-gated global table. Matches `ssp_protocol::list_ref_table_for`.
  if (userId === ANON_USER_ID) return ANON_LIST_REF_TABLE;
  if (mode === 'single') return '_00_list_ref';
  const uid = sanitizeUserId(userId);
  return uid ? `_00_list_ref_user_${uid}` : '_00_list_ref';
}
