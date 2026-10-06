import type { QueryPlan } from '@spooky-sync/query-builder';
import type { RecordId } from 'surrealdb';
import type { Effect } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { encodeRecordId, parseRecordIdString } from '../utils/index';
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
export function materializeEffect(src: MaterializeSource, ids: string[] | null): Effect {
  if (ids !== null) {
    const parsed = ids.map((id) => parseRecordIdString(id));
    if (src.plan) return fx.local.select(buildIdSetPlan(src.plan, parsed), src.params);
    const idSet = buildIdSetSurql(src.surql);
    if (idSet) return fx.local.query(idSet.query, { ...src.params, __win: parsed });
    return fx.local.query(src.surql, src.params);
  }
  if (src.plan) return fx.local.select(src.plan, src.params);
  return fx.local.query(src.surql, src.params);
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
