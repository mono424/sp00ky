import type { QueryPlan } from '@spooky-sync/query-builder';
import type { RecordId } from 'surrealdb';
import type { Effect } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { encodeRecordId, parseRecordIdString } from '../utils/index';
import type { QueryEntry } from '../state/client-state';
import type { Overlay } from '../state/selectors';
import { buildRenderIds, paintsCold, resolveMembership } from './render-set';
import { buildIdSetPlan, buildIdSetSurql, buildWindowMaterialization } from './window-query';

export interface MaterializeSource {
  surql: string;
  params: Record<string, unknown>;
  plan?: QueryPlan;
}

export const isWindowed = (surql: string): boolean => buildWindowMaterialization(surql) !== null;

/**
 * The one read that materializes a query. `ids` is the render set (membership
 * with the overlay applied); `null` means "no membership yet, scan the local
 * store with the query's own predicate".
 */
export function materializeEffect(src: MaterializeSource, ids: string[] | null, paintKey?: string): Effect {
  if (ids !== null) {
    const parsed = ids.map((id) => parseRecordIdString(id));
    if (src.plan) return fx.local.select(buildIdSetPlan(src.plan, parsed), src.params, paintKey);
    const idSet = buildIdSetSurql(src.surql);
    if (idSet) return fx.local.query(idSet.query, { ...src.params, __win: parsed });
    return fx.local.query(src.surql, src.params);
  }
  if (src.plan) return fx.local.select(src.plan, src.params, paintKey);
  return fx.local.query(src.surql, src.params);
}

/**
 * The peek that goes with {@link materializeEffect}: what the read named
 * `paintKey` answered in an earlier page load, for a store that kept it.
 * `null` for a query without a plan, which no paint cache covers.
 */
export function paintEffect(src: MaterializeSource, ids: string[] | null, paintKey: string): Effect | null {
  if (!src.plan) return null;
  const plan = ids === null ? src.plan : buildIdSetPlan(src.plan, ids.map((id) => parseRecordIdString(id)));
  return fx.local.peekSelect(plan, src.params, paintKey);
}

/** What one render of a query reads, and which of the rows it gets back may show. */
export interface RenderRead {
  /** The render set, or `null` for a scan with the query's own predicate. */
  ids: string[] | null;
  shows(rows: Record<string, unknown>[]): Record<string, unknown>[];
}

/**
 * Membership (or a cold scan) with the outbox overlay applied. `known` is the
 * set of ids some durable view vouches for: before the server has answered
 * (`cold`) only those bodies paint (see `paintsCold`).
 */
export function renderRead(
  entry: Pick<QueryEntry, 'def' | 'lifecycle' | 'remoteArray' | 'localArray'>,
  ov: Overlay,
  known: ReadonlySet<string>
): RenderRead {
  const isWindow = isWindowed(entry.def.surql);
  const paints = (id: string | null) => id === null || paintsCold(id, known, ov);
  const coldWindow = entry.lifecycle.phase === 'cold' && isWindow;
  const membership = resolveMembership({
    phase: entry.lifecycle.phase,
    remoteArray: entry.remoteArray,
    localArray: coldWindow ? entry.localArray.filter(([id]) => paints(id)) : entry.localArray,
    isWindow,
  });
  const ids = membership
    ? buildRenderIds(membership, entry.localArray, ov, {
        hasExplicitOrder: (entry.def.plan?.orderBy?.length ?? 0) > 0,
        isWindow,
      })
    : null;
  return {
    ids,
    // A peer's delete leaves the row in the store until it is confirmed, so
    // the predicate scan must drop it here (an own delete removed it already).
    shows: (rows) =>
      ids !== null
        ? rows
        : rows.filter((row) => {
            const key = rowKey(row);
            return paints(key) && !(key !== null && ov.deletes.has(key));
          }),
  };
}

/** Rows out of either read effect's result. */
export function rowsFromResult(effect: Effect, result: unknown): Record<string, unknown>[] {
  if (effect.kind === 'local.select') return (result as Record<string, unknown>[] | undefined) ?? [];
  const first = Array.isArray(result) ? result[0] : undefined;
  return Array.isArray(first) ? (first as Record<string, unknown>[]) : [];
}

/**
 * A row's encoded record id: a string on the SQLite engine, a `RecordId` on
 * the SurrealDB one. `null` for a row that carries none (a projection
 * without `id`, a `SELECT VALUE`).
 */
export function rowKey(row: unknown): string | null {
  const id = row !== null && typeof row === 'object' ? (row as { id?: unknown }).id : undefined;
  if (typeof id === 'string') return id;
  if (id !== null && typeof id === 'object' && 'table' in id && 'id' in id) return encodeRecordId(id as RecordId<string>);
  return null;
}

/** Cheap structural equality for the "did the rows change" check. */
export function rowsEqual(a: ReadonlyArray<unknown>, b: ReadonlyArray<unknown>): boolean {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  return JSON.stringify(a) === JSON.stringify(b);
}
