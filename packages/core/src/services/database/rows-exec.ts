/**
 * What the row store's worker does with a request: the client's verbs, run
 * against a {@link RowStore}.
 *
 * # Bodies are JSON text, and stay text as long as they can
 *
 * A row's body is the same JSON the SQLite engine stores (`serializeRow`):
 * record links as `table:id` strings, bytes as `{"__u8": base64}`. JSON
 * because the engine's own parser is several times faster than any decoder
 * written in JavaScript, and that parser is the whole read path here:
 *
 * - a read that needs no look inside the rows (a window by id, the common
 *   case) never parses them in the worker. The bodies are copied out of the
 *   file between `[`, `,` and `]`, the buffer is transferred, and the page
 *   runs one `JSON.parse` over the lot;
 * - a scan reads the file in ranges of megabytes, blanks the bytes between
 *   two bodies in the buffer (whitespace, one comma) and parses each range in
 *   one call. No per-row decode, no copy.
 *
 * # Same answers as the SQLite engine
 *
 * Filters, ordering and merges follow what that engine's SQL does, so a query
 * reads the same on either: `json_extract` storage classes (a boolean is 1 or
 * 0, an object is its JSON text), NULL never matching, numbers below text,
 * text in UTF-8 byte order, `json_patch` (RFC 7396) for merges. Two
 * differences, both where SQL leaves the order open: rows that tie on an
 * ORDER BY, and the children of an unordered `.related()`, come back in id
 * order here and in insertion order there.
 */
import type { QueryPlan, WhereComparison, WhereNode } from '@spooky-sync/query-builder';
import { resolveRelations, stableKey } from './relation-resolver';
import { project, scalar } from './sqlite-plan-sql';
import type { OrderBy, RelationFetch, Row, RowFetcher } from './cache-engine';
import {
  FLAG_RAW,
  FLAG_U8,
  cmpText,
  runId,
  type Entry,
  type RowStore,
  type Run,
  type StoreOp,
  type StoreStats,
} from './rows-store';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** A value as SQLite holds it once `json_extract` or a bind produced it. */
type SqlValue = null | number | string;

/** Rows on their way to the page: JSON text of an array, as bytes. */
export interface RowBytes {
  buf: ArrayBuffer;
  /** Some body carries `{"__u8": …}` tags the page has to revive. */
  u8: boolean;
}

/** One write, as the page sends it: bodies are ranges of the batch's buffer. */
export type WireOp =
  | ['put', table: string, id: string, rv: number, flags: number, off: number, len: number]
  | ['merge', table: string, id: string, rv: number, flags: number, off: number, len: number, patchOff: number, patchLen: number]
  | ['del', table: string, id: string]
  | ['clear', table: string];

/** A foreign-key index: which rows of a table hold which value in one field. */
interface FkIndex {
  byKey: Map<string, Set<string>>;
  keyOf: Map<string, string>;
  /** Ids written since the index last looked at them. */
  stale: Set<string>;
}

const MAX_GAP = 4096;
const MAX_RANGE = 4 << 20;
/** Past this many unseen writes an index is rebuilt on next use instead of patched. */
const MAX_STALE = 4096;
const U8_TAG = '"__u8"';

// ---- SQLite value semantics --------------------------------------------------

function walk(row: unknown, field: string): unknown {
  if (field.indexOf('.') === -1 && field.indexOf('[') === -1) {
    return row !== null && typeof row === 'object' ? (row as Row)[field] : undefined;
  }
  let cur: unknown = row;
  for (const part of field.split('.')) {
    const m = /^([^[]*)((?:\[\d+\])*)$/.exec(part);
    if (!m) return undefined;
    if (m[1] !== '') {
      if (cur === null || typeof cur !== 'object' || Array.isArray(cur)) return undefined;
      cur = (cur as Row)[m[1]];
    }
    for (const idx of m[2].matchAll(/\[(\d+)\]/g)) {
      if (!Array.isArray(cur)) return undefined;
      cur = cur[Number(idx[1])];
    }
  }
  return cur;
}

/** `json_extract(data, '$.<field>')`, or the id column for `id`. */
function extract(row: Row, field: string): SqlValue {
  const v = walk(row, field);
  if (v === null || v === undefined) return null;
  if (typeof v === 'number' || typeof v === 'string') return v;
  if (typeof v === 'boolean') return v ? 1 : 0;
  return JSON.stringify(v);
}

/** A bound parameter, after `scalar()` and SQLite's binding rules. */
function bound(value: unknown, forId: boolean): SqlValue {
  const v = scalar(value);
  if (v === null || v === undefined) return null;
  if (typeof v === 'boolean') return v ? 1 : 0;
  if (typeof v === 'bigint') return Number(v);
  // The id column has TEXT affinity, which a number compared to it takes on.
  if (typeof v === 'number') return forId ? String(v) : v;
  return String(v);
}

function sqlCompare(a: number | string, b: number | string): number {
  const an = typeof a === 'number';
  const bn = typeof b === 'number';
  if (an && bn) return (a as number) < (b as number) ? -1 : (a as number) > (b as number) ? 1 : 0;
  if (an) return -1;
  if (bn) return 1;
  return cmpText(a as string, b as string);
}

function compileComparison(c: WhereComparison, params: Record<string, unknown>): (row: Row) => boolean {
  const useParam =
    c.paramRef !== undefined && (c.value === undefined || Object.prototype.hasOwnProperty.call(params, c.paramRef));
  const rhs = bound(useParam ? params[c.paramRef!] : c.value, c.field === 'id');
  const field = c.field;
  const sign = c.swap ? -1 : 1;
  let test: (cmp: number) => boolean;
  switch (c.op) {
    case '=':
      test = (cmp) => cmp === 0;
      break;
    case '!=':
      test = (cmp) => cmp !== 0;
      break;
    case '<':
      test = (cmp) => cmp < 0;
      break;
    case '<=':
      test = (cmp) => cmp <= 0;
      break;
    case '>':
      test = (cmp) => cmp > 0;
      break;
    case '>=':
      test = (cmp) => cmp >= 0;
      break;
    default:
      throw new Error(`rows: unsupported comparison operator ${String(c.op)}`);
  }
  // A comparison with NULL is NULL, which no WHERE lets through.
  if (rhs === null) return () => false;
  return (row) => {
    const lhs = extract(row, field);
    return lhs !== null && test(sign * sqlCompare(lhs, rhs));
  };
}

/** The filter a WHERE describes; `null` when there is none. */
export function compileWhere(
  nodes: WhereNode[] | undefined,
  params: Record<string, unknown>
): ((row: Row) => boolean) | null {
  if (!nodes || nodes.length === 0) return null;
  const tests = nodes.map((node) => {
    if ('or' in node) {
      const any = node.or.map((c) => compileComparison(c, params));
      return (row: Row) => any.some((t) => t(row));
    }
    return compileComparison(node, params);
  });
  return tests.length === 1 ? tests[0] : (row) => tests.every((t) => t(row));
}

/** Sort as `ORDER BY json_extract(…)` does; ties fall back to the id. */
export function sortSql(rows: Row[], orderBy: OrderBy): Row[] {
  const keyed = rows.map((row) => ({ row, keys: orderBy.map(([field]) => extract(row, field)) }));
  keyed.sort((a, b) => {
    for (let i = 0; i < orderBy.length; i++) {
      const x = a.keys[i];
      const y = b.keys[i];
      // NULL is the smallest value.
      const cmp = x === null ? (y === null ? 0 : -1) : y === null ? 1 : sqlCompare(x, y);
      if (cmp !== 0) return orderBy[i][1] === 'desc' ? -cmp : cmp;
    }
    return cmpText(String(a.row.id), String(b.row.id));
  });
  return keyed.map((k) => k.row);
}

/** `json_patch(target, patch)`: RFC 7396. A `null` member removes its key. */
export function mergePatch(target: unknown, patch: unknown): unknown {
  if (patch === null || typeof patch !== 'object' || Array.isArray(patch)) return patch;
  const out: Row =
    target !== null && typeof target === 'object' && !Array.isArray(target) ? (target as Row) : {};
  for (const key of Object.keys(patch as Row)) {
    const value = (patch as Row)[key];
    if (value === null) delete out[key];
    else out[key] = mergePatch(out[key], value);
  }
  return out;
}

// ---- bytes in, bytes out -----------------------------------------------------

function toBytes(rows: unknown): RowBytes {
  const text = JSON.stringify(rows);
  const bytes = encoder.encode(text);
  return { buf: bytes.buffer as ArrayBuffer, u8: text.indexOf(U8_TAG) !== -1 };
}

/** The page's side of {@link RowBytes}: one parse for the whole answer. */
export function parseRowBytes<T = Row[]>(res: { buf: ArrayBuffer | null; u8?: boolean }, revive: (b64: string) => Uint8Array): T {
  if (!res.buf) return null as T;
  const text = decoder.decode(new Uint8Array(res.buf));
  if (!res.u8) return JSON.parse(text) as T;
  return JSON.parse(text, (_k, v) =>
    v && typeof v === 'object' && typeof (v as { __u8?: unknown }).__u8 === 'string' ? revive((v as { __u8: string }).__u8) : v
  ) as T;
}

export class RowsExec {
  private fk = new Map<string, Map<string, FkIndex>>();
  private scratch = new Uint8Array(256 << 10);
  /** Relation fetches of the request in flight, for the engine's stats. */
  private relationFetches = 0;

  constructor(private store: RowStore) {}

  // ---- reading bodies ------------------------------------------------------

  /** The bodies of `entries` as one JSON array, straight from the file. */
  private concat(entries: Entry[]): { bytes: Uint8Array; u8: boolean } {
    let total = 2;
    for (const e of entries) total += e.len + 1;
    if (entries.length > 0) total--;
    const bytes = new Uint8Array(new ArrayBuffer(Math.max(total, 2)));
    let p = 0;
    let u8 = false;
    bytes[p++] = 0x5b;
    for (let i = 0; i < entries.length; i++) {
      const e = entries[i];
      if (i > 0) bytes[p++] = 0x2c;
      this.store.readInto(e.off, e.len, bytes, p);
      p += e.len;
      if (e.flags & FLAG_U8) u8 = true;
    }
    bytes[p] = 0x5d;
    return { bytes, u8 };
  }

  private raw(entries: Entry[]): RowBytes {
    const { bytes, u8 } = this.concat(entries);
    return { buf: bytes.buffer as ArrayBuffer, u8 };
  }

  private parse(entries: Entry[]): Row[] {
    if (entries.length === 0) return [];
    return JSON.parse(decoder.decode(this.concat(entries).bytes)) as Row[];
  }

  private findAll(table: string, keys: string[]): Entry[] {
    const out: Entry[] = [];
    for (const key of keys) {
      const entry = this.store.find(table, key);
      if (entry && !(entry.flags & FLAG_RAW)) out.push(entry);
    }
    return out;
  }

  /**
   * Visit every row of a table. The file is read in offset order, a range of
   * megabytes at a time, and each range is parsed in one call: the bytes
   * between two bodies (record heads, dead rows) are blanked in the buffer and
   * one of them becomes the comma. `visit` returns false to stop.
   */
  private scan(table: string, visit: (row: Row, at: number, run: Run) => boolean | void): void {
    const run = this.store.ordered(table);
    if (run.n === 0) return;
    const order = run.byOff ?? (run.byOff = byOffset(run));
    let k = 0;
    while (k < order.length) {
      const first = k;
      const start = run.off[order[k]] - 1;
      let end = run.off[order[k]] + run.len[order[k]];
      k++;
      while (k < order.length) {
        const i = order[k];
        if (run.off[i] - end > MAX_GAP || run.off[i] + run.len[i] - start > MAX_RANGE) break;
        end = run.off[i] + run.len[i];
        k++;
      }
      const size = end - start + 1;
      if (this.scratch.length < size) this.scratch = new Uint8Array(size);
      const buf = this.scratch;
      this.store.readInto(start, end - start, buf, 0);
      buf[0] = 0x5b;
      buf[size - 1] = 0x5d;
      let p = run.off[order[first]] + run.len[order[first]] - start;
      for (let j = first + 1; j < k; j++) {
        const rel = run.off[order[j]] - start;
        buf[p] = 0x2c;
        buf.fill(0x20, p + 1, rel);
        p = rel + run.len[order[j]];
      }
      const rows = JSON.parse(decoder.decode(buf.subarray(0, size))) as Row[];
      for (let j = first; j < k; j++) {
        if (visit(rows[j - first], order[j], run) === false) return;
      }
    }
    if (this.scratch.length > MAX_RANGE * 2) this.scratch = new Uint8Array(256 << 10);
  }

  // ---- select --------------------------------------------------------------

  /** Rows matching `where`, as (row, entry) in the order SQL would give them. */
  private filtered(
    table: string,
    where: WhereNode[] | undefined,
    params: Record<string, unknown>,
    orderBy: OrderBy | undefined,
    limit: number | undefined,
    offset: number | undefined
  ): { rows: Row[]; entries: Entry[] } {
    const test = compileWhere(where, params);
    const ordered = orderBy !== undefined && orderBy.length > 0;
    const from = offset ?? 0;
    const to = limit === undefined ? undefined : from + limit;
    if (!ordered && to !== undefined) return this.firstInIdOrder(table, test, from, to);
    const hits: { row: Row; at: number }[] = [];
    let run: Run | null = null;
    this.scan(table, (row, at, r) => {
      run = r;
      if (!test || test(row)) hits.push({ row, at });
    });
    let picked: { row: Row; at: number }[];
    if (ordered) {
      const position = new Map(hits.map((h) => [h.row, h.at]));
      picked = sortSql(
        hits.map((h) => h.row),
        orderBy!
      )
        .slice(from, to)
        .map((row) => ({ row, at: position.get(row)! }));
    } else {
      // The scan walks the file; the answer is in id order, which is run order.
      picked = hits.sort((a, b) => a.at - b.at).slice(from, to);
    }
    const r = run as Run | null;
    return {
      rows: picked.map((h) => h.row),
      entries: r ? picked.map((h) => ({ off: r.off[h.at], len: r.len[h.at], rv: r.rv[h.at], flags: r.flags[h.at] })) : [],
    };
  }

  /**
   * The matches numbered `from` to `to` in id order, reading no further than
   * it has to: an unordered query with a LIMIT stops at its last row instead
   * of walking the table.
   */
  private firstInIdOrder(
    table: string,
    test: ((row: Row) => boolean) | null,
    from: number,
    to: number
  ): { rows: Row[]; entries: Entry[] } {
    const run = this.store.ordered(table);
    const rows: Row[] = [];
    const entries: Entry[] = [];
    let i = 0;
    let step = 64;
    while (i < run.n && rows.length < to) {
      const chunk: Entry[] = [];
      for (; i < run.n && chunk.length < step; i++) {
        if (run.flags[i] & FLAG_RAW) continue;
        chunk.push({ off: run.off[i], len: run.len[i], rv: run.rv[i], flags: run.flags[i] });
      }
      const parsed = this.parse(chunk);
      for (let j = 0; j < parsed.length && rows.length < to; j++) {
        if (test && !test(parsed[j])) continue;
        rows.push(parsed[j]);
        entries.push(chunk[j]);
      }
      step = Math.min(step * 2, 2048);
    }
    return { rows: rows.slice(from), entries: entries.slice(from) };
  }

  async select(plan: QueryPlan, params: Record<string, unknown>): Promise<RowBytes & { relationFetches: number }> {
    this.relationFetches = 0;
    const shaped = plan.select !== undefined || (plan.relations !== undefined && plan.relations.length > 0);
    let rows: Row[];
    if (plan.ids) {
      const entries = this.findAll(plan.table, windowOrder(plan.ids.map(stableKey)));
      const ordered = plan.orderBy !== undefined && plan.orderBy.length > 0;
      if (!shaped && !ordered) return { ...this.raw(entries), relationFetches: 0 };
      rows = this.parse(entries);
      if (ordered) rows = sortSql(rows, plan.orderBy!);
    } else {
      const hit = this.filtered(plan.table, plan.where, params, plan.orderBy, plan.limit, plan.offset);
      if (!shaped) return { ...this.raw(hit.entries), relationFetches: 0 };
      rows = hit.rows;
    }
    if (plan.select) rows = rows.map((row) => project(row, plan.select!));
    const fetcher: RowFetcher = {
      fetchRelation: (req) => {
        this.relationFetches++;
        return Promise.resolve(this.relationRows(req));
      },
    };
    await resolveRelations(rows, plan.relations, fetcher);
    return { ...toBytes(rows), relationFetches: this.relationFetches };
  }

  byIds(table: string, ids: string[], select?: string[], orderBy?: OrderBy): RowBytes {
    const entries = this.findAll(table, windowOrder(ids));
    const ordered = orderBy !== undefined && orderBy.length > 0;
    if (!select && !ordered) return this.raw(entries);
    let rows = this.parse(entries);
    if (ordered) rows = sortSql(rows, orderBy!);
    return toBytes(select ? rows.map((row) => project(row, select)) : rows);
  }

  get(table: string, id: string): { buf: ArrayBuffer | null; u8: boolean } {
    const hit = this.store.get(table, id);
    if (!hit || hit.entry.flags & FLAG_RAW) return { buf: null, u8: false };
    return { buf: hit.body.buffer as ArrayBuffer, u8: (hit.entry.flags & FLAG_U8) !== 0 };
  }

  selectTable(
    table: string,
    where: WhereNode[] | undefined,
    orderBy: OrderBy | undefined,
    limit: number | undefined,
    start: number | undefined
  ): RowBytes {
    return this.raw(this.filtered(table, where, {}, orderBy, limit, start).entries);
  }

  count(table: string, where: WhereNode[] | undefined): number {
    const test = compileWhere(where, {});
    if (!test) return this.store.count(table);
    let n = 0;
    this.scan(table, (row) => {
      if (test(row)) n++;
    });
    return n;
  }

  // ---- relations -----------------------------------------------------------

  relation(req: RelationFetch): RowBytes {
    return toBytes(this.relationRows(req));
  }

  private relationRows(req: RelationFetch): Row[] {
    const keys = [...new Set(req.keys.map(stableKey))];
    let ids: string[];
    if (req.matchField === 'id') ids = keys;
    else {
      const index = this.fkIndex(req.table, req.matchField);
      ids = [];
      for (const key of keys) {
        const set = index.byKey.get(key);
        if (set) for (const id of set) ids.push(id);
      }
    }
    // Probed in id order, as an index lookup would return them.
    ids.sort(cmpText);
    let rows = this.parse(this.findAll(req.table, ids));
    const test = compileWhere(req.where, {});
    if (test) rows = rows.filter(test);
    if (req.orderBy && req.orderBy.length > 0) rows = sortSql(rows, req.orderBy);
    return req.select ? rows.map((row) => project(row, req.select!)) : rows;
  }

  /**
   * The index over `field` of `table`, built by one scan on first use and
   * brought up to date from the ids written since. Only string values are
   * indexed: the keys of a relation are record ids, and SQL's `IN` over text
   * keys never matches a number either.
   */
  private fkIndex(table: string, field: string): FkIndex {
    let perTable = this.fk.get(table);
    if (!perTable) this.fk.set(table, (perTable = new Map()));
    let index = perTable.get(field);
    if (!index) {
      index = { byKey: new Map(), keyOf: new Map(), stale: new Set() };
      perTable.set(field, index);
      const built = index;
      this.scan(table, (row) => indexRow(built, String(row.id), walk(row, field)));
      return index;
    }
    if (index.stale.size > 0) {
      const ids = [...index.stale];
      index.stale.clear();
      const fresh = new Map<string, unknown>();
      for (const row of this.parse(this.findAll(table, ids))) fresh.set(String(row.id), walk(row, field));
      for (const id of ids) {
        const old = index.keyOf.get(id);
        if (old !== undefined) {
          index.byKey.get(old)?.delete(id);
          index.keyOf.delete(id);
        }
        indexRow(index, id, fresh.get(id));
      }
    }
    return index;
  }

  private touched(table: string, id: string | null): void {
    const perTable = this.fk.get(table);
    if (!perTable) return;
    if (id === null) {
      this.fk.delete(table);
      return;
    }
    for (const [field, index] of perTable) {
      index.stale.add(id);
      if (index.stale.size > MAX_STALE) perTable.delete(field);
    }
  }

  // ---- writes --------------------------------------------------------------

  /**
   * Apply one batch from the page. A merge reads the stored row, folds the
   * patch in and writes the result; a row that is not there yet takes the
   * patch's insert text as it came, without a parse.
   */
  write(ops: WireOp[], buf: ArrayBuffer | null, sync: boolean): void {
    const bytes = buf ? new Uint8Array(buf) : new Uint8Array(0);
    const out: StoreOp[] = [];
    // What this batch has already done to a row, for a later op on the same row.
    const pending = new Map<string, Uint8Array | null>();
    const cleared = new Set<string>();
    for (const op of ops) {
      const table = op[1];
      if (op[0] === 'clear') {
        out.push({ t: 'clear', table });
        cleared.add(table);
        for (const key of [...pending.keys()]) if (key.startsWith(table + '\n')) pending.delete(key);
        this.touched(table, null);
        continue;
      }
      const id = op[2];
      const key = table + '\n' + id;
      this.touched(table, id);
      if (op[0] === 'del') {
        out.push({ t: 'del', table, id });
        pending.set(key, null);
        continue;
      }
      const insert = bytes.subarray(op[5], op[5] + op[6]);
      if (op[0] === 'put') {
        out.push({ t: 'put', table, id, rv: op[3], flags: op[4], body: insert });
        pending.set(key, insert);
        continue;
      }
      let existing: Uint8Array | null | undefined = pending.get(key);
      if (existing === undefined) existing = cleared.has(table) ? null : (this.store.get(table, id)?.body ?? null);
      if (existing === null) {
        out.push({ t: 'put', table, id, rv: op[3], flags: op[4], body: insert });
        pending.set(key, insert);
        continue;
      }
      const patch = op[8] > 0 ? bytes.subarray(op[7], op[7] + op[8]) : insert;
      const merged = mergePatch(JSON.parse(decoder.decode(existing)), JSON.parse(decoder.decode(patch))) as Row;
      const text = JSON.stringify(merged);
      const body = encoder.encode(text);
      out.push({
        t: 'put',
        table,
        id,
        rv: Number(merged?._00_rv) || 0,
        flags: text.indexOf(U8_TAG) !== -1 ? FLAG_U8 : 0,
        body,
      });
      pending.set(key, body);
    }
    this.store.write(out, sync);
  }

  // ---- the rest ------------------------------------------------------------

  versions(tables: string[]): Record<string, [string, number][]> {
    const out: Record<string, [string, number][]> = {};
    for (const table of tables) out[table] = this.store.versions(table);
    return out;
  }

  /** Set the cached `_00_rv` of every row of `tables` to 0, except `keep`. Returns the rows rewritten. */
  resetVersions(tables: string[], keep: string[]): number {
    const kept = new Set(keep);
    const out: StoreOp[] = [];
    for (const table of tables) {
      this.scan(table, (row, at, run) => {
        const rv = extract(row, '_00_rv');
        if (rv === null || rv === 0 || kept.has(String(row.id))) return;
        row._00_rv = 0;
        out.push({ t: 'put', table, id: runId(run, at), rv: 0, flags: run.flags[at], body: encoder.encode(JSON.stringify(row)) });
      });
    }
    this.store.write(out);
    return out.length;
  }

  /** A raw blob and the JSON row beside it: the circuit snapshot's two halves. */
  getBlob(table: string, key: string): { bytes: ArrayBuffer | null; meta: string | null } {
    const blob = this.store.get(table, key);
    const meta = this.store.get(table, `${key}:meta`);
    return {
      bytes: blob ? (blob.body.buffer as ArrayBuffer) : null,
      meta: meta ? decoder.decode(meta.body) : null,
    };
  }

  putBlob(table: string, key: string, bytes: ArrayBuffer, meta: string): void {
    this.store.write([
      { t: 'put', table, id: key, rv: 0, flags: FLAG_RAW, body: new Uint8Array(bytes) },
      { t: 'put', table, id: `${key}:meta`, rv: 0, flags: FLAG_RAW, body: encoder.encode(meta) },
    ]);
  }

  deleteBlob(table: string, key: string): void {
    this.store.write([
      { t: 'del', table, id: key },
      { t: 'del', table, id: `${key}:meta` },
    ]);
  }
}

/**
 * The ids of a window, each once, in the order the SQLite engine restores:
 * by position in the list, a repeated id counting where it appears last.
 */
function windowOrder(keys: string[]): string[] {
  const position = new Map(keys.map((key, i) => [key, i]));
  if (position.size === keys.length) return keys;
  return [...position.keys()].sort((a, b) => position.get(a)! - position.get(b)!);
}

function indexRow(index: FkIndex, id: string, value: unknown): void {
  if (typeof value !== 'string') return;
  let set = index.byKey.get(value);
  if (!set) index.byKey.set(value, (set = new Set()));
  set.add(id);
  index.keyOf.set(id, value);
}

/** Row positions of a run ordered by where their bodies lie, skipping raw blobs. */
function byOffset(run: Run): Uint32Array {
  let json = 0;
  let ascending = true;
  let last = -1;
  for (let i = 0; i < run.n; i++) {
    if (run.flags[i] & FLAG_RAW) continue;
    json++;
    if (run.off[i] < last) ascending = false;
    last = run.off[i];
  }
  const order = new Uint32Array(json);
  let k = 0;
  for (let i = 0; i < run.n; i++) if (!(run.flags[i] & FLAG_RAW)) order[k++] = i;
  // A compacted table already lies in id order.
  if (!ascending) order.sort((a, b) => run.off[a] - run.off[b]);
  return order;
}

/** Data ops a follower tab may send over its port; the rest belong to the owner. */
export const ROWS_CLIENT_OPS: ReadonlySet<string> = new Set([
  'select',
  'by-ids',
  'get',
  'relation',
  'select-table',
  'count',
  'write',
  'versions',
  'reset-versions',
  'tables',
  'blob-get',
  'blob-put',
  'blob-del',
]);

export interface RowsReply {
  result: Record<string, unknown>;
  transfer?: Transferable[];
}

const withBytes = (rows: { buf: ArrayBuffer | null; u8: boolean }, extra: Record<string, unknown> = {}): RowsReply => ({
  result: { ...rows, ...extra },
  transfer: rows.buf ? [rows.buf] : undefined,
});

/**
 * Answer one data message. Kept apart from the worker so the engine's tests
 * can run the same code in-process. Returns `null` for a message it does not
 * know.
 */
// oxlint-disable-next-line typescript/no-explicit-any
export async function handleRowsOp(exec: RowsExec, store: RowStore, type: string, payload: any): Promise<RowsReply | null> {
  switch (type) {
    case 'select': {
      const { relationFetches, ...rows } = await exec.select(payload.plan, payload.params ?? {});
      return withBytes(rows, { relationFetches });
    }
    case 'by-ids':
      return withBytes(exec.byIds(payload.table, payload.ids, payload.select, payload.orderBy));
    case 'get':
      return withBytes(exec.get(payload.table, payload.id));
    case 'relation':
      return withBytes(exec.relation(payload));
    case 'select-table':
      return withBytes(exec.selectTable(payload.table, payload.where, payload.orderBy, payload.limit, payload.start));
    case 'count':
      return { result: { n: exec.count(payload.table, payload.where) } };
    case 'write':
      exec.write(payload.ops, payload.buf ?? null, payload.sync === true);
      return { result: {} };
    case 'versions':
      return { result: { versions: exec.versions(payload.tables) } };
    case 'reset-versions':
      return { result: { rows: exec.resetVersions(payload.tables, payload.keep ?? []) } };
    case 'tables':
      return { result: { tables: store.tableNames() } };
    case 'blob-get': {
      const blob = exec.getBlob(payload.table, payload.key);
      return { result: blob, transfer: blob.bytes ? [blob.bytes] : undefined };
    }
    case 'blob-put':
      exec.putBlob(payload.table, payload.key, payload.bytes, payload.meta);
      return { result: {} };
    case 'blob-del':
      exec.deleteBlob(payload.table, payload.key);
      return { result: {} };
    default:
      return null;
  }
}

/**
 * The log a store may carry past its last checkpoint while it is busy: what
 * the next open would have to replay if the tab died right now. Past either
 * bound the index is persisted at once instead of at the next quiet moment.
 */
export const CHECKPOINT_TAIL_BYTES = 8 << 20;
export const CHECKPOINT_TAIL_OPS = 20_000;

/**
 * What the store should do now that it has a quiet moment: persist its index
 * when writes are waiting on it, compact when dead bytes outweigh live ones.
 */
export function maintenance(stats: StoreStats): 'checkpoint' | 'compact' | null {
  if (stats.tailBytes > 0) return 'checkpoint';
  if (stats.garbageBytes > Math.max(4 << 20, stats.liveBytes)) return 'compact';
  return null;
}
