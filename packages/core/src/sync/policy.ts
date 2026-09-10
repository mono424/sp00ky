import type { RecordId } from 'surrealdb';
import type { RecordVersionArray, RecordVersionDiff } from '../types';
import { parseRecordIdString, encodeRecordId } from '../utils/index';
import type { SyncHealth } from '../types';
import { classifySyncError } from '../utils/error-classification';
import { SELF_HEAL_BASE_MS, SELF_HEAL_MAX_MS } from '../kernel/constants';

export class ArraySyncer {
  private localArray: RecordVersionArray;
  private remoteArray: RecordVersionArray;
  private needsSort = false;

  constructor(localArray: RecordVersionArray, remoteArray: RecordVersionArray) {
    this.remoteArray = remoteArray.toSorted((a: [string, number], b: [string, number]) => a[0].localeCompare(b[0]));
    this.localArray = localArray.toSorted((a: [string, number], b: [string, number]) => a[0].localeCompare(b[0]));
  }

  /**
   * Inserts an item into the local array
   */
  insert(recordId: string, version: number) {
    this.localArray.push([recordId, version]);
    this.needsSort = true;
  }

  /**
   * Updates the current local RecordVersionArray state.
   */
  update(recordId: string, version: number) {
    this.localArray = this.localArray.map((record) => {
      if (record[0] === recordId) {
        this.needsSort = true;
        return [recordId, version];
      }
      return record;
    });
  }

  /**
   * Deletes an item from the local array
   */
  delete(recordId: string) {
    this.localArray = this.localArray.filter((record) => record[0] !== recordId);
  }

  /**
   * Returns the difference between the local and remote arrays.
   * Includes sets of added, updated, and removed records.
   */
  nextSet(): RecordVersionDiff | null {
    if (this.needsSort) {
      this.localArray.sort((a, b) => a[0].localeCompare(b[0]));
      this.needsSort = false;
    }
    const diff = diffRecordVersionArray(this.localArray, this.remoteArray);
    return diff;
  }
}

export function diffRecordVersionArray(
  local: RecordVersionArray | null,
  remote: RecordVersionArray | null
): RecordVersionDiff {
  const localArray = local || [];
  const remoteArray = remote || [];

  // Convert arrays to Maps for O(1) lookup
  const localMap = new Map<string, number>(localArray);
  const remoteMap = new Map<string, number>(remoteArray);

  const added: string[] = [];
  const updated: string[] = [];
  const removed: string[] = [];

  // Find added and updated records
  for (const [recordId, remoteVersion] of remoteMap) {
    const localVersion = localMap.get(recordId);

    if (localVersion === undefined) {
      // Record exists in remote but not in local
      added.push(recordId);
    } else if (localVersion < remoteVersion) {
      // Record exists in both but remote has newer version
      updated.push(recordId);
    }
  }

  // Find removed records
  for (const [recordId] of localMap) {
    if (!remoteMap.has(recordId)) {
      removed.push(recordId);
    }
  }

  return {
    added: added.map((id) => ({
      id: parseRecordIdString(id),
      // oxlint-disable-next-line no-non-null-assertion
      version: remoteMap.get(id)!,
    })),
    updated: updated.map((id) => ({
      id: parseRecordIdString(id),
      // oxlint-disable-next-line no-non-null-assertion
      version: remoteMap.get(id)!,
    })),
    removed: removed.map(parseRecordIdString),
  };
}

/**
 * Applies a RecordVersionDiff to a RecordVersionArray and returns a new sorted array.
 */
export function applyRecordVersionDiff(
  current: RecordVersionArray,
  diff: RecordVersionDiff
): RecordVersionArray {
  const currentMap = new Map(current);

  // Apply removals
  for (const id of diff.removed) {
    currentMap.delete(encodeRecordId(id));
  }

  // Apply additions
  for (const item of diff.added) {
    currentMap.set(encodeRecordId(item.id), item.version);
  }

  // Apply updates
  for (const item of diff.updated) {
    currentMap.set(encodeRecordId(item.id), item.version);
  }

  return Array.from(currentMap).toSorted((a: [string, number], b: [string, number]) => a[0].localeCompare(b[0]));
}

export function createDiffFromDbOp(
  op: 'CREATE' | 'UPDATE' | 'DELETE',
  recordId: RecordId,
  version: number,
  versions?: RecordVersionArray
): RecordVersionDiff {
  const old = versions?.find((record) => record[0] === encodeRecordId(recordId));

  if (old && old[1] >= version) {
    return {
      added: [],
      updated: [],
      removed: [],
    };
  }

  if (op === 'CREATE') {
    return {
      added: [{ id: recordId, version }],
      updated: [],
      removed: [],
    };
  } else if (op === 'UPDATE') {
    return {
      added: [],
      updated: [{ id: recordId, version }],
      removed: [],
    };
  } else {
    return {
      added: [],
      updated: [],
      removed: [recordId],
    };
  }
}

/**
 * Default cadence for the `_00_list_ref_user_<id>` poll fallback. The
 * poll is the safety net for SurrealDB v3's occasionally-dropped LIVE
 * deliveries; 500ms is aggressive enough to feel real-time on the
 * happy path while keeping the per-session query load bounded.
 */
export const DEFAULT_LIST_REF_POLL_INTERVAL_MS = 500;

/**
 * Build the SurrealQL select that powers both the initial-fetch and
 * the periodic poll of `_00_list_ref[_user_<id>]`. The `parent IS NONE`
 * predicate excludes subquery entries (rows with `parent_rel` set)
 * because the client's `RecordVersionArray` only tracks primary rows;
 * including subquery rows would surface them as spurious "added"
 * diffs every tick.
 */
export function buildListRefSelect(table: string): string {
  return `SELECT out, version FROM ${table} WHERE in = $in AND parent IS NONE`;
}

/**
 * Build the select that says whether an EMPTY id-set means "this query has no
 * rows" or "the server has not published them yet".
 *
 * The SSP writes `rowCount` onto the `_00_query` row in the same statement that
 * registers the view — synchronously, and BEFORE it hands the view's initial
 * edges to the coalescing edge flusher. So the two are not interchangeable:
 * `rowCount > 0` with no edges is the flush window, and only `rowCount === 0`
 * is a genuinely empty query. Polling the edges alone cannot tell those apart
 * no matter how long it waits, which is why this is read alongside them rather
 * than a retry counter.
 *
 * `state` rides along: the SSP writes `materializing` when it hands a full
 * publish to its edge flusher and flips it to `ready` INSIDE the transaction
 * that writes the edges. So `ready` + no edges + `rowCount 0` is a real empty
 * result, `materializing` + no edges is "still publishing", and a missing row
 * (the whole object is `NONE`) is "the server no longer has this view". A
 * server that predates the marker returns `state: NONE` and callers fall back
 * to `rowCount` alone.
 *
 * Returns `NONE` (→ null) when the row is not readable or does not exist yet;
 * callers must treat that as "unknown", not as zero.
 */
export function buildQueryRowCountSelect(): string {
  return 'SELECT VALUE { rowCount: rowCount, state: state } FROM ONLY $in';
}

/**
 * Build the SurrealQL select for a query's SUBQUERY child edges — the
 * mirror of {@link buildListRefSelect}. `.related()` queries register a
 * correlated subquery; the SSP materializes each matched child as a
 * `_00_list_ref` edge tagged with `parent`/`parent_rel` (see
 * `apps/ssp` edge writer). `parent IS NONE` (the primary select) drops
 * these, so their bodies never reach the local cache and a cold-reload
 * re-materialization of the correlated surql yields empty related
 * fields. This `parent IS NOT NONE` variant pulls the child `out`+`version`
 * pairs (any nesting depth) so we can sync their bodies into the local
 * store SEPARATELY from the primary window array.
 */
export function buildSubqueryListRefSelect(table: string): string {
  return `SELECT out, version FROM ${table} WHERE in = $in AND parent IS NOT NONE`;
}

/**
 * One round trip for the edges of MANY queries: primary and subquery rows
 * together, split client-side on `parent`. Replaces two selects per query per
 * poll tick, which on a page with 20+ live queries was ~50 serialized round
 * trips per cycle and kept the remote busy full-time.
 */
export function buildListRefBatchSelect(table: string): string {
  return `SELECT in, out, version, parent FROM ${table} WHERE in IN $ins`;
}

/**
 * `rowCount` for many `_00_query` rows at once, keyed by id so the caller does
 * not depend on the array order. Same semantics as
 * {@link buildQueryRowCountSelect}: a row the session cannot read comes back
 * without an id and must be treated as "unknown".
 */
export function buildQueryRowCountBatchSelect(): string {
  return 'SELECT VALUE { id: id, rowCount: rowCount, state: state } FROM $ins';
}

/** Known edges one poll round trip may carry before the cycle is split. */
export const LIST_REF_POLL_ROW_BUDGET = 1_500;
/** A view at or past this many edges is "large": it rides on LIVE and is only
 *  re-polled every {@link LIST_REF_POLL_LARGE_VIEW_MIN_AGE_MS}. */
export const LIST_REF_POLL_LARGE_VIEW_ROWS = 1_000;
export const LIST_REF_POLL_LARGE_VIEW_MIN_AGE_MS = 15_000;

export interface ListRefPollCandidate {
  hash: string;
  /** Edges the client currently holds for the query (`remoteArray.length`). */
  rows: number;
  /** When the query was last refreshed by the poll; `0` = never. */
  lastPolledAt: number;
}

/**
 * Split the active queries into the round trips of one poll cycle.
 *
 * Every query that is due is refreshed once per cycle, oldest refresh first,
 * packed greedily into chunks of at most `rowBudget` known edges so one
 * response never carries a whole page's worth of ids at once. A view with
 * `largeViewRows` or more edges is only due once `largeViewMinAgeMs` has
 * passed since its last refresh (LIVE remains its primary path); a chunk
 * always holds at least one query, so a single huge view still refreshes.
 */
export function planListRefPollChunks(
  candidates: ListRefPollCandidate[],
  opts: { now: number; rowBudget?: number; largeViewRows?: number; largeViewMinAgeMs?: number }
): string[][] {
  const {
    now,
    rowBudget = LIST_REF_POLL_ROW_BUDGET,
    largeViewRows = LIST_REF_POLL_LARGE_VIEW_ROWS,
    largeViewMinAgeMs = LIST_REF_POLL_LARGE_VIEW_MIN_AGE_MS,
  } = opts;
  const due = candidates
    .filter((c) => c.rows < largeViewRows || now - c.lastPolledAt >= largeViewMinAgeMs)
    .sort(
      (a, b) => a.lastPolledAt - b.lastPolledAt || (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0)
    );
  const chunks: string[][] = [];
  let current: string[] = [];
  let currentRows = 0;
  for (const c of due) {
    const cost = Math.max(1, c.rows);
    if (current.length > 0 && currentRows + cost > rowBudget) {
      chunks.push(current);
      current = [];
      currentRows = 0;
    }
    current.push(c.hash);
    currentRows += cost;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

/**
 * Resolve the effective list-ref poll interval. Negative or zero
 * values fall back to the default — accepting them would either
 * disable polling silently or busy-loop the event loop.
 */
export function resolveListRefPollInterval(opt?: number): number {
  if (typeof opt !== 'number' || !Number.isFinite(opt) || opt <= 0) {
    return DEFAULT_LIST_REF_POLL_INTERVAL_MS;
  }
  return opt;
}

/**
 * When the LIVE feed has delivered an event within this window, treat
 * the LIVE subscription as healthy and back the poll off to
 * {@link LIVE_HEALTHY_POLL_INTERVAL_MS}. As soon as LIVE quiets for
 * longer than this, the poll snaps back to the aggressive default.
 */
export const LIVE_HEALTHY_COOLDOWN_MS = 5_000;

/**
 * Poll interval while LIVE is delivering events. The aggressive
 * default 500ms costs ~120 queries / minute / session — wasted work
 * when LIVE is already covering us. 5s keeps the safety net in place
 * at 1/10th the load.
 */
export const LIVE_HEALTHY_POLL_INTERVAL_MS = 5_000;

/**
 * Pick the next poll delay based on whether LIVE has been healthy
 * recently. If a LIVE event fired within `cooldownMs`, use the slow
 * (`healthyIntervalMs`) cadence; otherwise the fast (`baseIntervalMs`)
 * cadence. Pure so it's unit-testable; `Sp00kySync.startListRefPoll`
 * calls it from a self-rescheduling timer.
 *
 * The healthy interval is clamped to at least `baseIntervalMs` so
 * configuring an aggressive base (e.g. 100ms) never gets implicitly
 * widened by this helper.
 *
 * @deprecated Superseded by {@link listRefPollDelayMs}, which backs the
 * poll off based on observed change activity (LIVE *or* poll-detected)
 * rather than LIVE liveness alone — the cross-session LIVE-permission gap
 * means LIVE often never fires, so this helper would keep the poll pinned
 * at the aggressive base forever even on a fully idle page. Kept (and
 * tested) for reference.
 */
export function nextPollDelayMs(args: {
  now: number;
  lastLiveEventAt: number | null;
  baseIntervalMs: number;
  cooldownMs?: number;
  healthyIntervalMs?: number;
}): number {
  const {
    now,
    lastLiveEventAt,
    baseIntervalMs,
    cooldownMs = LIVE_HEALTHY_COOLDOWN_MS,
    healthyIntervalMs = LIVE_HEALTHY_POLL_INTERVAL_MS,
  } = args;
  if (lastLiveEventAt === null) return baseIntervalMs;
  const sinceLive = now - lastLiveEventAt;
  if (sinceLive < 0 || sinceLive >= cooldownMs) return baseIntervalMs;
  return Math.max(healthyIntervalMs, baseIntervalMs);
}

/**
 * Ceiling for the adaptive list_ref poll backoff. An idle page (no LIVE
 * events, no poll-detected list_ref changes) coasts up to this cadence;
 * the existing healthy-LIVE safety net runs at the same 5s, so this keeps
 * the worst-case catch-up latency for a missed cross-session change at the
 * cadence the codebase already treats as acceptable.
 */
export const LIST_REF_POLL_MAX_INTERVAL_MS = 5_000;

/**
 * Adaptive poll delay: stay at the responsive `baseIntervalMs` while
 * changes are arriving, and exponentially back off toward `maxIntervalMs`
 * while the `_00_list_ref` is quiet.
 *
 * `idleStreak` is the count of consecutive poll cycles that observed *no*
 * change. `Sp00kySync` resets it to 0 whenever a poll detects a real
 * remoteArray change OR a LIVE event lands, so any activity snaps the poll
 * straight back to `baseIntervalMs`. A streak of 0 (something just
 * happened) → base; otherwise `base * 2^streak` capped at `maxIntervalMs`.
 *
 * This replaces {@link nextPollDelayMs}: the old helper slowed the poll
 * only while LIVE was *delivering*, but the cross-session LIVE-permission
 * gap means LIVE frequently never fires here, so it left a fully idle page
 * polling every `base` ms forever (the "continuous queries while idle"
 * symptom). Backing off on observed idleness instead covers the
 * LIVE-healthy case for free (LIVE applies the change → the next poll sees
 * nothing new → the streak grows → it backs off).
 */
export function listRefPollDelayMs(args: {
  idleStreak: number;
  baseIntervalMs: number;
  maxIntervalMs?: number;
}): number {
  const { idleStreak, baseIntervalMs, maxIntervalMs = LIST_REF_POLL_MAX_INTERVAL_MS } = args;
  const cap = Math.max(baseIntervalMs, maxIntervalMs);
  if (idleStreak <= 0) return baseIntervalMs;
  // 2^streak grows fast; clamp the exponent so it can't overflow on a
  // page left idle for a very long time.
  const exponent = Math.min(idleStreak, 30);
  return Math.min(baseIntervalMs * 2 ** exponent, cap);
}

/**
 * Order-insensitive equality for two `RecordVersionArray`s (each a list of
 * `[recordIdString, version]`). The `_00_list_ref` SELECT has no `ORDER
 * BY`, so row order can differ between polls without anything having
 * actually changed — comparing as an id→version map avoids false
 * "changed" verdicts that would defeat the idle backoff. Record ids are
 * unique within a query's list_ref, so a map is a faithful representation.
 */
export function recordVersionArraysEqual(
  a: RecordVersionArray,
  b: RecordVersionArray
): boolean {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  const byId = new Map<string, number>();
  for (const [id, version] of a) byId.set(id, version);
  for (const [id, version] of b) {
    // `get` returns undefined for a missing id, which never === a number.
    if (byId.get(id) !== version) return false;
  }
  return true;
}

// ---- health ------------------------------------------------------------------


export interface HealthInput {
  health: SyncHealth;
  consecutiveFailures: number;
  hasSyncedOnce: boolean;
}

export interface HealthOutput extends HealthInput {
  /** Crossed into degraded on this outcome: start self-heal. */
  degradedNow: boolean;
  /** Left degraded on this outcome: stop self-heal. */
  recoveredNow: boolean;
}

/**
 * Fold one sync round's outcome into health. A single failure is absorbed;
 * `degradeAfter` consecutive ones flip to degraded, the next success flips
 * back. `degradeAfter <= 0` disables reporting.
 */
export function nextHealth(input: HealthInput, ok: boolean, error: unknown, degradeAfter: number): HealthOutput {
  const base = { ...input, degradedNow: false, recoveredNow: false };
  if (degradeAfter <= 0) return base;
  if (ok) {
    const everConnected = true;
    if (input.consecutiveFailures === 0) {
      return { ...base, hasSyncedOnce: true, health: { ...input.health, everConnected } };
    }
    const recovered = input.health.status === 'degraded';
    return {
      ...base,
      hasSyncedOnce: true,
      consecutiveFailures: 0,
      health: { ...input.health, status: 'healthy', kind: undefined, error: undefined, everConnected },
      recoveredNow: recovered,
    };
  }
  const consecutiveFailures = input.consecutiveFailures + 1;
  const kind = classifySyncError(error);
  const message = error instanceof Error ? error.message : String(error);
  if (input.health.status !== 'degraded' && consecutiveFailures >= degradeAfter) {
    return {
      ...base,
      consecutiveFailures,
      health: { ...input.health, status: 'degraded', consecutiveFailures, kind, error: message },
      degradedNow: true,
    };
  }
  return {
    ...base,
    consecutiveFailures,
    health: { ...input.health, consecutiveFailures, kind, error: message },
  };
}

export const selfHealDelayMs = (attempt: number): number =>
  Math.min(SELF_HEAL_MAX_MS, SELF_HEAL_BASE_MS * 2 ** Math.min(attempt, 30));
