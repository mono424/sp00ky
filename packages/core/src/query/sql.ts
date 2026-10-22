import { RecordId } from 'surrealdb';
import type { RecordVersionArray } from '../types';
import type { Vars } from '../kernel/effects';
import { surql } from '../utils/surql';
import type { SealedQuery } from '../utils/surql';
import { ANON_LIST_REF_TABLE } from '../modules/ref-tables';

/** Every SurrealQL statement the query side sends, as pure builders. */

export const VIEW_TABLE = '_00_view';
export const LEGACY_VIEW_TABLE = '_00_window';

export const viewRecordId = (viewKey: string): RecordId<string> => new RecordId(VIEW_TABLE, viewKey);

/**
 * The durable `_00_view` row: the confirmed membership plus the subquery
 * children, which the GC must keep and a cold scan may paint as much as the
 * members.
 */
export const viewRow = (
  ids: RecordVersionArray,
  children: RecordVersionArray,
  confirmed: boolean,
  now: number
): Record<string, unknown> => ({
  ids,
  children,
  confirmed,
  updatedAt: now,
});

/** Every `_00_view` row: the durable view index and the GC read it. */
export const readViewRows = (): string => `SELECT * FROM ${VIEW_TABLE}`;

/** Copy every legacy `_00_window` row into `_00_view` once. */
export const readLegacyViewRows = (): string => `SELECT * FROM ${LEGACY_VIEW_TABLE}`;
export const countViewRows = (): string => `SELECT count() FROM ${VIEW_TABLE} GROUP ALL`;

// Edges are read through the graph (`$in->table`): the `_00_list_ref*` tables
// have no index on `in`, so `WHERE in = $in` scanned the whole table, which on
// the shared `_00_list_ref_anon` is every signed-out visitor's edges.
export const listRefSelect = (table: string): string =>
  `SELECT out, version FROM $in->${table} WHERE parent IS NONE`;
export const subqueryListRefSelect = (table: string): string =>
  `SELECT out, version FROM $in->${table} WHERE parent IS NOT NONE`;
export const queryRowCountSelect = (): string => 'SELECT VALUE { rowCount: rowCount, state: state } FROM ONLY $in';
export const listRefBatchSelect = (table: string): string =>
  `SELECT in, out, version, parent FROM $ins->${table}`;
export const queryRowCountBatchSelect = (): string => 'SELECT VALUE { id: id, rowCount: rowCount, state: state } FROM $ins';

/** The single-query read: edges, meta, subquery children in one request. */
export const singleSnapshotSelect = (table: string): string =>
  `${listRefSelect(table)};\n${queryRowCountSelect()};\n${subqueryListRefSelect(table)}`;
/** The many-query read: all edges + all metas in one request. */
export const batchSnapshotSelect = (table: string): string =>
  `${listRefBatchSelect(table)};\n${queryRowCountBatchSelect()}`;

/**
 * The session's one LIVE subscription on its `_00_list_ref` table.
 *
 * `inlineBodies` adds `FETCH out`, which resolves the edge's target into the
 * notification so the body arrives with the doorbell.
 *
 * On the anon table it is filtered to this client's own edges. Every
 * signed-out session shares that table, so unfiltered each one received (and
 * the server fetched a body for) every anonymous edge change of every other
 * session, and the client then dropped all but its own. An edge carries the
 * `session::id()` of whichever socket last registered its view, which is a
 * socket of this client but not necessarily the current one: a view stays
 * registered across a reconnect, and a reload keeps its salt and its views,
 * so their older edges still name an earlier socket. `sessionIds` are those
 * earlier sockets. The current one is matched by `session::id()` itself,
 * which SurrealDB evaluates in the subscriber's own session, so the filter
 * holds before the client has learned its new id.
 *
 * The cast is load-bearing: `session::id()` is a uuid, `clientId` a string,
 * and without it nothing matches. Per-user tables need no filter.
 */
export function listRefLive(
  table: string,
  opts: { inlineBodies: boolean; sessionIds: ReadonlyArray<string> }
): { sql: string; vars?: Vars } {
  const join = opts.inlineBodies ? ' FETCH out' : '';
  if (table !== ANON_LIST_REF_TABLE) return { sql: `LIVE SELECT * FROM ${table}${join}` };
  return {
    sql: `LIVE SELECT * FROM ${table} WHERE clientId = <string>session::id() OR clientId IN $sids${join}`,
    vars: { sids: [...opts.sessionIds] },
  };
}

export interface RegisterPayload {
  id: RecordId<string>;
  surql: string;
  params: Record<string, unknown>;
  ttl: string;
  /**
   * The registering page's secret. `fn::query::register` forwards only its
   * hash, and the SSP lets a registration onto an existing anonymous view
   * only with the hash that view was registered with: every signed-out
   * visitor is `anon`, and anyone can read an anonymous view's id. Kept out
   * of `params`, which name the computation the SSP shares between views.
   */
  owner?: string;
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

/**
 * Hand back view ids this page no longer answers to. Per id,
 * `fn::query::unsubscribe` drops this socket's entry and deletes the row when
 * nobody else watches it; `fn::query::leave` then drops the entries of this
 * page's earlier sockets (a reconnect re-registered under a new session id)
 * and pulls the expiry in when none remain. Result index 2i answers the
 * unsubscribe of id i, 2i + 1 its leave.
 */
export function releaseBatch(ids: ReadonlyArray<RecordId<string>>, sessionIds: ReadonlyArray<string>): { sql: string; vars: Vars } {
  const vars: Vars = { sids: [...sessionIds] };
  const stmts = ids.flatMap((id, i) => {
    vars[`id${i}`] = id;
    return [`fn::query::unsubscribe($id${i})`, `fn::query::leave($id${i}, $sids)`];
  });
  return { sql: stmts.join(';\n'), vars };
}

/** A heartbeat answer whose statement matched no row: the view was reclaimed. */
export const heartbeatRowGone = (result: unknown): boolean => Array.isArray(result) && result.length === 0;

// The SDK escapes an all-digit key as `⟨…⟩`; both forms parse back to the same id.
const QUERY_ID = /^_00_query:(?:[0-9a-f]{64}|⟨[0-9a-f]{64}⟩)$/;
const SESSION_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * The pagehide release: one bare statement for `beaconSql`, which has no params
 * channel, so ids are inlined. Only shapes the client minted itself get in, a
 * `_00_query:<sha256>` id and a UUID session id, so nothing else is ever
 * interpolated. `null` when there is nothing the server could act on: with no
 * session ids it could not tell this page's entries from anyone else's.
 */
export function leaveViewsBeacon(ids: ReadonlyArray<unknown>, sessionIds: ReadonlyArray<string>): string | null {
  const list = ids.map((id) => String(id)).filter((id) => QUERY_ID.test(id));
  const sids = sessionIds.filter((sid) => SESSION_ID.test(sid));
  if (list.length === 0 || sids.length === 0) return null;
  const sidList = sids.map((sid) => `'${sid}'`).join(', ');
  return `FOR $id IN [${list.join(', ')}] { LET $_left = fn::query::leave($id, [${sidList}]); };`;
}

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
