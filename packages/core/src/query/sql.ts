import { RecordId } from 'surrealdb';
import type { RecordVersionArray } from '../types';
import type { Vars } from '../kernel/effects';
import { surql } from '../utils/surql';
import type { SealedQuery } from '../utils/surql';

/** Every SurrealQL statement the query side sends, as pure builders. */

export const VIEW_TABLE = '_00_view';
export const LEGACY_VIEW_TABLE = '_00_window';

export const viewRecordId = (viewKey: string): RecordId<string> => new RecordId(VIEW_TABLE, viewKey);

export const viewRow = (ids: RecordVersionArray, confirmed: boolean, now: number): Record<string, unknown> => ({
  ids,
  confirmed,
  updatedAt: now,
});

/** Copy every legacy `_00_window` row into `_00_view` once. */
export const readLegacyViewRows = (): string => `SELECT * FROM ${LEGACY_VIEW_TABLE}`;
export const countViewRows = (): string => `SELECT count() FROM ${VIEW_TABLE} GROUP ALL`;

export const listRefSelect = (table: string): string =>
  `SELECT out, version FROM ${table} WHERE in = $in AND parent IS NONE`;
export const subqueryListRefSelect = (table: string): string =>
  `SELECT out, version FROM ${table} WHERE in = $in AND parent IS NOT NONE`;
export const queryRowCountSelect = (): string => 'SELECT VALUE { rowCount: rowCount, state: state } FROM ONLY $in';
export const listRefBatchSelect = (table: string): string =>
  `SELECT in, out, version, parent FROM ${table} WHERE in IN $ins`;
export const queryRowCountBatchSelect = (): string => 'SELECT VALUE { id: id, rowCount: rowCount, state: state } FROM $ins';

/** The single-query read: edges, meta, subquery children in one request. */
export const singleSnapshotSelect = (table: string): string =>
  `${listRefSelect(table)};\n${queryRowCountSelect()};\n${subqueryListRefSelect(table)}`;
/** The many-query read: all edges + all metas in one request. */
export const batchSnapshotSelect = (table: string): string =>
  `${listRefBatchSelect(table)};\n${queryRowCountBatchSelect()}`;

export interface RegisterPayload {
  id: RecordId<string>;
  surql: string;
  params: Record<string, unknown>;
  ttl: string;
}

/** Register + read back edges/meta/children in ONE request. */
export const registerSelect = (table: string): string =>
  `fn::query::register($config);\n${singleSnapshotSelect(table)}`;
export const registerVars = (payload: RegisterPayload): Vars => ({ config: payload, in: payload.id });

/** One heartbeat statement per view id; result index i answers id i. */
export function heartbeatBatch(ids: ReadonlyArray<RecordId<string>>): { sql: string; vars: Vars } {
  const vars: Vars = {};
  const stmts = ids.map((id, i) => {
    vars[`id${i}`] = id;
    return `fn::query::heartbeat($id${i})`;
  });
  return { sql: stmts.join(';\n'), vars };
}

/** A heartbeat answer whose statement matched no row: the view was reclaimed. */
export const heartbeatRowGone = (result: unknown): boolean => Array.isArray(result) && result.length === 0;

export const bodySelect = (): string => 'SELECT * FROM $ids';

/** Local transaction that MERGEs many fetched bodies. */
export function upsertBodiesTx(records: ReadonlyArray<{ id: unknown; content: Record<string, unknown> }>): {
  query: SealedQuery<unknown>;
  vars: Vars;
} {
  const vars: Vars = {};
  const stmts = records.map((r, i) => {
    vars[`id${i}`] = r.id;
    vars[`content${i}`] = r.content;
    return surql.upsertMerge(`id${i}`, `content${i}`);
  });
  return { query: surql.seal<void>(surql.tx(stmts)) as SealedQuery<unknown>, vars };
}
