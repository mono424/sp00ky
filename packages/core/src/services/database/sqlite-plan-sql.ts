import type { WhereComparison, WhereNode } from '@spooky-sync/query-builder';
import { stableKey } from './relation-resolver';
import type { OrderBy, Row } from './cache-engine';

/**
 * SQL rendering + value (de)serialization for the SQLite cache backend.
 * Extracted from `sqlite-cache-engine.ts` so BOTH sides of the worker boundary
 * can use it: the engine (main thread) for the legacy/shim paths, and
 * `sqlite-worker.ts` for worker-side plan execution (`select`). Pure module —
 * no DOM, no worker, no engine imports — mirroring `plan-render.ts`.
 */

export function renderOrderSql(orderBy: OrderBy): string {
  return ` ORDER BY ${orderBy
    .map(([f, d]) => `json_extract(data, '$.${f}') ${d === 'desc' ? 'DESC' : 'ASC'}`)
    .join(', ')}`;
}

export function comparisonSql(
  c: WhereComparison,
  bind: unknown[],
  params: Record<string, unknown>
): string {
  const lhs = c.field === 'id' ? 'id' : `json_extract(data, '$.${c.field}')`;
  // Prefer the query's own param so a filter materializes from `params` (the
  // query's identity), not a baked literal. A pure `$`-ref has no `value`; a
  // slave-mode node keeps `value` as a fallback for a param absent from params.
  const useParam =
    c.paramRef !== undefined &&
    (c.value === undefined || Object.prototype.hasOwnProperty.call(params, c.paramRef));
  const value = useParam ? params[c.paramRef!] : c.value;
  bind.push(scalar(value));
  const op = c.op === '!=' ? '!=' : c.op;
  return c.swap ? `? ${op} ${lhs}` : `${lhs} ${op} ?`;
}

export function renderWhereSql(
  nodes: WhereNode[],
  bind: unknown[],
  params: Record<string, unknown>
): string {
  return nodes
    .map((node) => {
      if ('or' in node) {
        return `(${node.or.map((c) => comparisonSql(c, bind, params)).join(' OR ')})`;
      }
      return comparisonSql(node, bind, params);
    })
    .join(' AND ');
}

// ==================== value (de)serialization ====================

/** A comparable scalar for SQL binding: record links → `table:id`, everything
 *  else passed through (numbers/strings/bools). */
export function scalar(value: unknown): unknown {
  if (value == null) return null;
  if (typeof value === 'object') return stableKey(value);
  return value;
}

export function serializeRow(row: Row): string {
  return JSON.stringify(row, (_k, v) => {
    if (v instanceof Uint8Array) return { __u8: toBase64(v) };
    if (v && typeof v === 'object') {
      const rid = v as { tb?: unknown; id?: unknown };
      if (rid.tb !== undefined && rid.id !== undefined) return stableKey(v);
    }
    return v;
  });
}

export function reviveRow(json: string): Row {
  // Fast path: the per-key reviver is only needed to rebuild `Uint8Array`s from
  // `{__u8}` tags. Most rows (e.g. game bodies) have none — a plain parse avoids
  // invoking a JS callback for every key of every row on the read hot path.
  if (json.indexOf('"__u8"') === -1) return JSON.parse(json);
  return JSON.parse(json, (_k, v) => {
    if (v && typeof v === 'object' && typeof (v as any).__u8 === 'string') {
      return fromBase64((v as any).__u8);
    }
    return v;
  });
}

/**
 * SELECT-clause expression yielding the row's `data` narrowed to `fields` (plus
 * `id`), so SQLite never returns — and neither side ever parses — the fields the
 * caller did not ask for. On a game list that is every row's `pgn`, which was
 * being read, parsed and thrown away 50 rows at a time for eight rendered fields.
 *
 * Byte-identical to running {@link project} over the fully-parsed row, which is
 * what both paths did before, and verified so against real SQLite:
 * `json_each` walks the keys the row ACTUALLY has, so an ABSENT key stays absent
 * rather than becoming an explicit null — the difference `json_object`/
 * `json_extract` would have introduced. A stored null, nested objects and
 * arrays, and `{__u8}` blob tags all round-trip unchanged, because
 * `json_group_object` understands `json_each`'s `value` column as JSON rather
 * than as text. `COALESCE` covers a row sharing none of the requested keys,
 * where the subquery yields NULL and `reviveRow` would throw.
 *
 * The one value `je.value` does NOT carry back is a TOP-LEVEL boolean. SQLite
 * has no boolean type, so `json_each` hands `true`/`false` out as the integers
 * 1/0 and `json_group_object` writes integers back. A projected row therefore
 * read `published: 1` where the same row read whole read `published: true`,
 * and every `=== true` in an app turned false the moment a screen painted from
 * the local cache. (A boolean NESTED in an object or array survives: those come
 * back as whole JSON documents, untouched.) `je.type` still knows what it was,
 * so the two boolean types are rebuilt as JSON literals on the way out.
 *
 * Deliberately unaliased, so the emitted statement keeps the exact shape the
 * callers already produce (`FROM "t" WHERE id IN (…)`).
 *
 * Binds one parameter per key, pushed onto `bind` — these land in the SELECT
 * clause, so they must be bound BEFORE any WHERE parameters.
 */
export function projectedDataSql(fields: string[], bind: unknown[]): string {
  const keys = ['id', ...fields];
  for (const k of keys) bind.push(k);
  const placeholders = keys.map(() => '?').join(', ');
  // `json('true')` / `json('false')` return JSON-subtyped values, so
  // `json_group_object` embeds them as the literals `true` / `false` rather
  // than as quoted text.
  const value =
    `CASE je.type WHEN 'true' THEN json('true') ` +
    `WHEN 'false' THEN json('false') ELSE je.value END`;
  return (
    `COALESCE((SELECT json_group_object(je.key, ${value}) ` +
    `FROM json_each(data) je WHERE je.key IN (${placeholders})), '{}') AS data`
  );
}

export function project(row: Row, fields: string[]): Row {
  const out: Row = {};
  for (const f of ['id', ...fields]) if (f in row) out[f] = row[f];
  return out;
}

export function toBase64(bytes: Uint8Array): string {
  let bin = '';
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return typeof btoa !== 'undefined' ? btoa(bin) : Buffer.from(bytes).toString('base64');
}

export function fromBase64(b64: string): Uint8Array {
  if (typeof atob !== 'undefined') {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  return new Uint8Array(Buffer.from(b64, 'base64'));
}
