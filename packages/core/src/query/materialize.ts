import type { QueryPlan } from '@spooky-sync/query-builder';
import type { Effect } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { parseRecordIdString } from '../utils/index';
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

/** Cheap structural equality for the "did the rows change" check. */
export function rowsEqual(a: ReadonlyArray<unknown>, b: ReadonlyArray<unknown>): boolean {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  return JSON.stringify(a) === JSON.stringify(b);
}
