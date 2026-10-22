/**
 * Feature flag administration for the DevTools panel.
 *
 * Two independent capabilities, deliberately kept apart:
 *
 * - **Local overrides** force a variant in THIS browser. No auth, no network,
 *   works signed out and offline. Delegated straight to `FeatureFlagModule`.
 * - **Remote changes** flip a flag for EVERY user. These require the caller to
 *   be listed in `_00_admin` (`spky admin add <user>`); SurrealDB enforces
 *   that, not this file. A non-admin's SELECT returns `[]` rather than an
 *   error, and the `fn::feature::*` calls are hard-denied.
 *
 * Everything remote goes through `RemoteDatabaseService` with BOUND params.
 * The panel's generic `runQuery` bridge is not usable here: it interpolates
 * the SurrealQL into an `inspectedWindow.eval` string (so a DB-sourced flag
 * key or record id would be concatenated into code) and it rejects after 10s,
 * which a materialize over a real user table will exceed.
 *
 * Definitions live in `_00_feature_flag`, which is NOT synced to clients, so
 * flags must be read remotely. Per-user assignments live in `_00_user_feature`,
 * which IS live-synced, so those come from the local store for free.
 */

import type { Logger } from '../../services/logger/index';
import { parseRecordIdString } from '../../utils/index';

/**
 * The only thing this service needs from a database. Structural rather than
 * `AbstractDatabaseService`, because the local side is a `LocalStore` (which
 * wraps a database rather than extending it) and both must satisfy it.
 */
export interface FlagQueryable {
  query<T extends unknown[]>(query: string, vars?: Record<string, unknown>): Promise<T>;
}

/** A targeting rule as written by `spky flag` / `fn::feature::allow`. */
export interface DevToolsFlagRule {
  kind: 'allowlist' | 'rollout' | string;
  variant: string;
  /** Allowlist only. Stored as record-id STRINGS, not records. */
  users?: string[];
  /** Rollout only, 0..100. */
  percent?: number;
  priority: number;
}

export interface DevToolsFlagRow {
  key: string;
  description?: string;
  variants: string[];
  default_variant: string;
  enabled: boolean;
  payloads?: Record<string, unknown>;
  rules: DevToolsFlagRule[];
  updated_at?: string;
  /** Derived: the variant the signed-in user is allowlisted into, if any. */
  selfAllowlistedVariant?: string;
}

export interface DevToolsFlagAssignment {
  key: string;
  variant: string;
  payload?: unknown;
}

export interface DevToolsFlagOverride {
  variant: string;
  payload?: unknown;
}

export interface DevToolsFlagsSnapshot {
  at: number;
  /** Null when signed out. */
  userId: string | null;
  /** True when `_00_admin` has a row for the signed-in user. */
  isAdmin: boolean;
  /** Empty for non-admins — SurrealDB filters the rows, it does not error. */
  flags: DevToolsFlagRow[];
  /** From the LOCAL `_00_user_feature`: what this browser actually resolves. */
  assignments: DevToolsFlagAssignment[];
  /** Local-only forced variants, page-origin localStorage. */
  overrides: Record<string, DevToolsFlagOverride>;
  /**
   * Set when the internal schema predates this feature (no `_00_admin` table),
   * or the remote read failed. The panel shows it instead of a bare empty
   * state, so "not migrated" doesn't look like "you're not an admin".
   */
  error?: string;
}

export interface DevToolsFlagResult {
  success: boolean;
  error?: string;
  /** Users re-evaluated by `fn::feature::materialize`. */
  users?: number;
}

/** The slice of `FeatureFlagModule` this service needs. */
export interface LocalOverrideStore {
  setLocalOverride(key: string, variant: string | null, payload?: unknown): void;
  clearLocalOverrides(): void;
  getLocalOverrides(): Record<string, DevToolsFlagOverride>;
}

export interface FlagsAdminDeps {
  remote: FlagQueryable;
  local: FlagQueryable;
  logger: Logger;
  /** Resolves the signed-in user's record id, or null. */
  currentUserId: () => string | null;
  /** Set once the client has built its FeatureFlagModule. */
  overrides: () => LocalOverrideStore | null;
}

/**
 * SurrealDB's SDK returns one entry per statement; each entry is the rows for
 * that statement, but older/local shapes wrap them as `{ status, result }`.
 * `getTableData` in `index.ts` unwraps the same three shapes inline — this is
 * that logic, reusable.
 */
function statementRows(result: unknown, index = 0): unknown[] {
  if (!Array.isArray(result)) return [];
  const entry = result[index];
  if (Array.isArray(entry)) return entry;
  if (entry && typeof entry === 'object' && 'result' in entry) {
    const inner = (entry as { result?: unknown }).result;
    return Array.isArray(inner) ? inner : inner === undefined || inner === null ? [] : [inner];
  }
  return entry === undefined || entry === null ? [] : [entry];
}

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Row shape guards. `key` is the only field the panel truly can't do without. */
function hasStringKey(row: unknown): row is { key: string } {
  return !!row && typeof row === 'object' && typeof (row as { key?: unknown }).key === 'string';
}

function isAssignment(row: unknown): row is DevToolsFlagAssignment {
  return hasStringKey(row);
}

function isFlagRow(row: unknown): row is DevToolsFlagRow {
  return hasStringKey(row);
}

export class FlagsAdminService {
  constructor(private deps: FlagsAdminDeps) {}

  /**
   * Everything the Access tab renders, in one round trip per source.
   *
   * Each section fails independently: a remote read that throws downgrades to
   * `isAdmin: false` plus an `error`, while local assignments and overrides
   * still render. Signing out must not blank the whole tab.
   */
  async getFlags(): Promise<DevToolsFlagsSnapshot> {
    const userId = this.deps.currentUserId();
    const snapshot: DevToolsFlagsSnapshot = {
      at: Date.now(),
      userId,
      isAdmin: false,
      flags: [],
      assignments: [],
      overrides: this.deps.overrides()?.getLocalOverrides() ?? {},
    };

    // Local assignments: available signed out and offline.
    try {
      const rows = statementRows(
        await this.deps.local.query('SELECT key, variant, payload FROM _00_user_feature')
      );
      snapshot.assignments = rows.filter(isAssignment).map((r) => ({
        key: r.key,
        variant: r.variant,
        payload: r.payload,
      }));
    } catch (err) {
      this.deps.logger.debug(
        { err, Category: 'sp00ky-client::FlagsAdminService::getFlags' },
        'Local feature assignments unavailable'
      );
    }

    if (!userId) return snapshot;

    try {
      // Self-scoped by `_00_admin`'s own select rule: an admin sees exactly
      // their own row, a non-admin sees nothing. The roster never leaks.
      const admin = statementRows(
        await this.deps.remote.query('SELECT VALUE id FROM _00_admin WHERE user = $auth.id LIMIT 1')
      );
      snapshot.isAdmin = admin.length > 0;
    } catch (err) {
      // A missing `_00_admin` table means the deployment hasn't applied the
      // internal schema yet. Say so — otherwise it reads as "not an admin".
      snapshot.error = `Could not check admin status: ${message(err)}. If this deployment predates the Access tab, run \`spky migrate\` (or redeploy) to apply the internal schema.`;
      return snapshot;
    }

    if (!snapshot.isAdmin) return snapshot;

    try {
      const rows = statementRows(
        await this.deps.remote.query(
          'SELECT key, description, variants, default_variant, enabled, payloads, rules, updated_at ' +
            'FROM _00_feature_flag ORDER BY key ASC'
        )
      );
      snapshot.flags = rows
        .filter(isFlagRow)
        .map((flag) => ({
          ...flag,
          rules: Array.isArray(flag.rules) ? flag.rules : [],
          variants: Array.isArray(flag.variants) ? flag.variants : [],
          selfAllowlistedVariant: selfAllowlistedVariant(flag, userId),
        }));
    } catch (err) {
      snapshot.error = `Could not read feature flags: ${message(err)}`;
    }

    return snapshot;
  }

  /**
   * Flip a flag's global `enabled` bit for EVERY user, then re-materialize.
   *
   * Both statements are one request so they share a transaction: if the
   * materialize fails, the `enabled` change rolls back rather than leaving the
   * definition and the assignments disagreeing.
   */
  async setFlagEnabled(key: string, enabled: boolean): Promise<DevToolsFlagResult> {
    return this.mutate(
      'UPDATE _00_feature_flag SET enabled = $enabled WHERE key = $key; ' +
        'RETURN fn::feature::materialize($key);',
      { key, enabled },
      1
    );
  }

  /**
   * Add or remove a user from `$key`'s allowlist for `$variant`, then
   * re-evaluate that one user (nobody else's variant can change). Defaults to the signed-in user, so the common case
   * ("turn this on for me, for real") needs no user picker.
   */
  async setFlagUserVariant(
    key: string,
    variant: string,
    remove: boolean,
    userId?: string
  ): Promise<DevToolsFlagResult> {
    const target = userId ?? this.deps.currentUserId();
    if (!target) return { success: false, error: 'Not signed in' };

    // `fn::feature::allow/disallow` declare `$user: record`. A string binding
    // is rejected by the type check, so parse it into a real RecordId.
    let user;
    try {
      user = parseRecordIdString(target);
    } catch (err) {
      return { success: false, error: `Invalid user id '${target}': ${message(err)}` };
    }

    return remove
      ? this.mutate('RETURN fn::feature::disallow($key, $user);', { key, user }, 0)
      : this.mutate('RETURN fn::feature::allow($key, $variant, $user);', { key, variant, user }, 0);
  }

  setLocalFlagOverride(
    key: string,
    variant: string | null,
    payload?: unknown
  ): { overrides: Record<string, DevToolsFlagOverride> } {
    const store = this.deps.overrides();
    store?.setLocalOverride(key, variant, payload);
    return { overrides: store?.getLocalOverrides() ?? {} };
  }

  clearLocalFlagOverrides(): { overrides: Record<string, DevToolsFlagOverride> } {
    const store = this.deps.overrides();
    store?.clearLocalOverrides();
    return { overrides: store?.getLocalOverrides() ?? {} };
  }

  /**
   * Run a remote mutation, reporting the materialize count from `$index`.
   *
   * Retries on a transaction conflict. `fn::feature::allow` / `disallow` are
   * read-modify-write over `_00_feature_flag.rules`, so two admins acting on
   * the same flag at once collide. SurrealDB detects this and fails the loser
   * with "Transaction conflict ... can be retried" rather than losing the
   * write — verified against 3.1 — so nothing is silently dropped. Retrying
   * turns that into the outcome the user expected instead of a raw engine
   * error they can do nothing with.
   */
  private async mutate(
    sql: string,
    vars: Record<string, unknown>,
    index: number
  ): Promise<DevToolsFlagResult> {
    let lastError: unknown;
    for (let attempt = 0; attempt < MUTATE_ATTEMPTS; attempt++) {
      try {
        const result = await this.deps.remote.query(sql, vars);
        const materialized = statementRows(result, index)[0] as { users?: number } | undefined;
        return { success: true, users: materialized?.users };
      } catch (err) {
        lastError = err;
        if (!isRetryableConflict(err) || attempt === MUTATE_ATTEMPTS - 1) break;
        // Staggered so two collided clients don't line up again on the retry.
        await sleep(40 * (attempt + 1) + Math.random() * 40);
      }
    }

    this.deps.logger.warn(
      { err: lastError, sql, Category: 'sp00ky-client::FlagsAdminService::mutate' },
      'Feature flag mutation failed'
    );
    return { success: false, error: message(lastError) };
  }
}

const MUTATE_ATTEMPTS = 3;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function isRetryableConflict(err: unknown): boolean {
  return /transaction (write )?conflict/i.test(message(err));
}

/**
 * Which variant, if any, this user is explicitly allowlisted into.
 *
 * `rules[].users` holds record-id strings (`flag.rs` serialises them as JSON),
 * so compare as strings. Lets the panel show "you're on the list" without
 * making the user reason about a raw rules blob.
 */
function selfAllowlistedVariant(flag: DevToolsFlagRow, userId: string): string | undefined {
  const rules = Array.isArray(flag.rules) ? flag.rules : [];
  const hit = rules.find(
    (rule) =>
      rule?.kind === 'allowlist' &&
      Array.isArray(rule.users) &&
      rule.users.some((u) => String(u) === userId)
  );
  return hit?.variant;
}
