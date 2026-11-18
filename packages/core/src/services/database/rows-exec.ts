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
 * # Indexes are tables
 *
 * A secondary index over `field` of `table` is a hidden table in the same
 * store whose ids are `value\0rowId` and whose bodies are empty. Ids are kept
 * sorted, so "every row whose field is X" is one search and a short walk, and
 * the index is written in the same atomic batch as the rows it describes: it
 * cannot disagree with them, and it is on disk for the next session. Two ways
 * a field gets one: it holds record links (found as rows are written, so a
 * `.related()` never waits for a build), or a query filters on it with `=`
 * (built by one scan the first time, kept up from then on). A field with a
 * handful of distinct values is left alone: a scan reads its matches faster.
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

const MAX_GAP = 4096;
const MAX_RANGE = 4 << 20;
/** Tables the store keeps for itself start with this; no client table can. */
const HIDDEN = '\u0001';
const META_TABLE = `${HIDDEN}meta`;
const INDEXES_ROW = 'indexes';
const SEP = '\u0000';
const MAX_KEY_CHARS = 200;
/** An index build writes its entries in batches of this many. */
const INDEX_BATCH = 20_000;
/** A value that addresses a record: `table:id`, and not a URL. */
const LINK = /^[A-Za-z_][A-Za-z0-9_]*:(?!\/\/).{1,80}$/s;
const NO_ENTRY = new Uint8Array(0);

const indexTable = (table: string, field: string): string => `${HIDDEN}i${HIDDEN}${table}${HIDDEN}${field}`;
/** Tables that get indexes: the app's own. The client's `_00_` tables are small and read whole. */
const isUserTable = (table: string): boolean => !table.startsWith('_00_') && !table.startsWith(HIDDEN);

/** The tables a client may see: everything but the store's own. */
export const visibleTables = (store: RowStore): string[] => store.tableNames().filter((t) => !t.startsWith(HIDDEN));

/** A value as an index key, or `null` when it is not indexed (NULL, very long, or holding the separator). */
function indexKey(value: SqlValue): string | null {
  if (value === null) return null;
  const key = typeof value === 'number' ? `n${value}` : `s${value}`;
  return key.length > MAX_KEY_CHARS || key.includes(SEP) ? null : key;
}
const U8_TAG = '"__u8"';

// ---- SQLite value semantics --------------------------------------------------

function walk(row: unknown, field: string): unknown {
  if (!field.includes('.') && !field.includes('[')) {
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

/** The value a comparison binds: the query's own param when it has one, else the baked literal. */
function rhsOf(c: WhereComparison, params: Record<string, unknown>): SqlValue {
  const useParam =
    c.paramRef !== undefined && (c.value === undefined || Object.prototype.hasOwnProperty.call(params, c.paramRef));
  return bound(useParam ? params[c.paramRef!] : c.value, c.field === 'id');
}

function compileComparison(c: WhereComparison, params: Record<string, unknown>): (row: Row) => boolean {
  const rhs = rhsOf(c, params);
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
  return { buf: bytes.buffer as ArrayBuffer, u8: text.includes(U8_TAG) };
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
  /** table -> field -> `true` indexed, `false` looked at and left to scans. */
  private indexes = new Map<string, Map<string, boolean>>();
  /** table -> fields seen holding something that is not a link. Not persisted: a guess that saves a regex per row. */
  private plain = new Map<string, Set<string>>();
  private scratch = new Uint8Array(256 << 10);
  /** Relation fetches of the request in flight, for the engine's stats. */
  private relationFetches = 0;

  constructor(private store: RowStore) {
    const saved = store.get(META_TABLE, INDEXES_ROW);
    if (!saved) return;
    try {
      const parsed = JSON.parse(decoder.decode(saved.body)) as Record<string, Record<string, boolean>>;
      for (const [table, fields] of Object.entries(parsed)) this.indexes.set(table, new Map(Object.entries(fields)));
    } catch {
      // An unreadable registry only costs the indexes: they are rebuilt on use.
    }
  }

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

  // ---- indexes -------------------------------------------------------------

  private fieldsOf(table: string): string[] {
    const fields = this.indexes.get(table);
    if (!fields) return [];
    const out: string[] = [];
    for (const [field, on] of fields) if (on) out.push(field);
    return out;
  }

  private keysOf(fields: string[], row: Row | null): (string | null)[] {
    return fields.map((field) => (row ? indexKey(extract(row, field)) : null));
  }

  /** Move a row's index entries from its old keys to its new ones. */
  private reindex(
    table: string,
    id: string,
    fields: string[],
    before: (string | null)[],
    after: (string | null)[],
    out: StoreOp[]
  ): void {
    for (let i = 0; i < fields.length; i++) {
      if (before[i] === after[i]) continue;
      const index = indexTable(table, fields[i]);
      if (before[i] !== null) out.push({ t: 'del', table: index, id: before[i] + SEP + id });
      if (after[i] !== null) out.push({ t: 'put', table: index, id: after[i] + SEP + id, rv: 0, flags: FLAG_RAW, body: NO_ENTRY });
    }
  }

  private registryOp(): StoreOp {
    const all: Record<string, Record<string, boolean>> = {};
    for (const [table, fields] of this.indexes) all[table] = Object.fromEntries(fields);
    return { t: 'put', table: META_TABLE, id: INDEXES_ROW, rv: 0, flags: FLAG_RAW, body: encoder.encode(JSON.stringify(all)) };
  }

  private mark(table: string, field: string, on: boolean): void {
    let fields = this.indexes.get(table);
    if (!fields) this.indexes.set(table, (fields = new Map()));
    fields.set(field, on);
  }

  /** Fields of `row` that hold a record link and have no index yet: they get one now. */
  private discover(table: string, row: Row, added: [string, string][]): void {
    const fields = this.indexes.get(table);
    let plain = this.plain.get(table);
    for (const field in row) {
      const value = row[field];
      if (typeof value !== 'string' || field === 'id' || fields?.has(field) || plain?.has(field)) continue;
      if (LINK.test(value)) {
        this.mark(table, field, true);
        added.push([table, field]);
      } else {
        if (!plain) this.plain.set(table, (plain = new Set()));
        plain.add(field);
      }
    }
  }

  /**
   * Whether `field` of `table` has an index, building it if it has none yet:
   * one scan, written to the store, kept up by every write after. Without
   * `force`, a field with too few distinct values to be worth one is turned
   * down, and that answer is remembered too.
   */
  private ensureIndex(table: string, field: string, force: boolean): boolean {
    const known = this.indexes.get(table)?.get(field);
    if (known === true) return true;
    if (known === false && !force) return false;
    const index = indexTable(table, field);
    const puts: StoreOp[] = [];
    const distinct = new Set<string>();
    let rows = 0;
    this.scan(table, (row) => {
      rows++;
      const key = indexKey(extract(row, field));
      if (key === null) return;
      distinct.add(key);
      puts.push({ t: 'put', table: index, id: key + SEP + String(row.id), rv: 0, flags: FLAG_RAW, body: NO_ENTRY });
    });
    // With fewer than 8 values each one matches an eighth of the table or more.
    const worth = force || rows < 64 || distinct.size >= 8;
    this.mark(table, field, worth);
    if (!worth) {
      this.store.write([this.registryOp()]);
      return false;
    }
    for (let i = 0; i < puts.length; i += INDEX_BATCH) this.store.write(puts.slice(i, i + INDEX_BATCH));
    this.store.write([this.registryOp()]);
    return true;
  }

  private lookup(table: string, field: string, key: string): string[] {
    return this.store.prefix(indexTable(table, field), key + SEP);
  }

  // ---- select --------------------------------------------------------------

  /**
   * The rows a filter can possibly match, when an index can say so without
   * reading the table: an `id = …`, or an `=` on an indexed field. In id
   * order. `null` means read the table.
   */
  private narrow(table: string, where: WhereNode[] | undefined, params: Record<string, unknown>): Entry[] | null {
    if (!where) return null;
    for (const node of where) {
      if ('or' in node || node.op !== '=') continue;
      const value = rhsOf(node, params);
      // `= NULL` is never true, and it is ANDed with the rest.
      if (value === null) return [];
      if (node.field === 'id') return this.findAll(table, [String(value)]);
      if (!isUserTable(table)) continue;
      const key = indexKey(value);
      if (key === null || !this.ensureIndex(table, node.field, false)) continue;
      const ids = this.lookup(table, node.field, key);
      // Past a third of the table a scan reads the matches faster than this many point reads.
      if (ids.length > Math.max(256, this.store.count(table) / 3)) continue;
      return this.findAll(table, ids);
    }
    return null;
  }

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
    const narrowed = this.narrow(table, where, params);
    if (!narrowed && !ordered && to !== undefined) return this.firstInIdOrder(table, test, from, to);

    // Every match, in id order.
    let rows: Row[] = [];
    let entries: Entry[] = [];
    if (narrowed) {
      const parsed = this.parse(narrowed);
      for (let i = 0; i < parsed.length; i++) {
        if (test && !test(parsed[i])) continue;
        rows.push(parsed[i]);
        entries.push(narrowed[i]);
      }
    } else {
      const hits: { row: Row; at: number }[] = [];
      let seen: Run | null = null;
      this.scan(table, (row, at, run) => {
        seen = run;
        if (!test || test(row)) hits.push({ row, at });
      });
      // The scan walks the file; run order is id order.
      hits.sort((a, b) => a.at - b.at);
      const run = seen as Run | null;
      rows = hits.map((h) => h.row);
      entries = run ? hits.map((h) => ({ off: run.off[h.at], len: run.len[h.at], rv: run.rv[h.at], flags: run.flags[h.at] })) : [];
    }
    if (ordered) {
      const entryOf = new Map<Row, Entry>();
      for (let i = 0; i < rows.length; i++) entryOf.set(rows[i], entries[i]);
      rows = sortSql(rows, orderBy!).slice(from, to);
      return { rows, entries: rows.map((row) => entryOf.get(row)!) };
    }
    return { rows: rows.slice(from, to), entries: entries.slice(from, to) };
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
    if (!where || where.length === 0) return this.store.count(table);
    return this.filtered(table, where, {}, undefined, undefined, undefined).rows.length;
  }

  // ---- relations -----------------------------------------------------------

  relation(req: RelationFetch): RowBytes {
    return toBytes(this.relationRows(req));
  }

  private relationRows(req: RelationFetch): Row[] {
    const keys = [...new Set(req.keys.map(stableKey))];
    let rows: Row[];
    if (req.matchField === 'id') {
      // Probed in id order, as an index lookup would return them.
      rows = this.parse(this.findAll(req.table, keys.sort(cmpText)));
    } else {
      const indexKeys = keys.map((key) => indexKey(key));
      if (isUserTable(req.table) && indexKeys.every((k) => k !== null) && this.ensureIndex(req.table, req.matchField, true)) {
        const ids: string[] = [];
        for (const key of indexKeys) for (const id of this.lookup(req.table, req.matchField, key!)) ids.push(id);
        rows = this.parse(this.findAll(req.table, ids.sort(cmpText)));
      } else {
        // Only string values match: the keys are record ids, and SQL's `IN`
        // over text keys never matches a number either.
        const wanted = new Set(keys);
        const hits: { row: Row; at: number }[] = [];
        this.scan(req.table, (row, at) => {
          const value = walk(row, req.matchField);
          if (typeof value === 'string' && wanted.has(value)) hits.push({ row, at });
        });
        rows = hits.sort((a, b) => a.at - b.at).map((h) => h.row);
      }
    }
    const test = compileWhere(req.where, {});
    if (test) rows = rows.filter(test);
    if (req.orderBy && req.orderBy.length > 0) rows = sortSql(rows, req.orderBy);
    return req.select ? rows.map((row) => project(row, req.select!)) : rows;
  }

  // ---- writes --------------------------------------------------------------

  /**
   * Apply one batch from the page. A merge reads the stored row, folds the
   * patch in and writes the result; a row that is not there yet takes the
   * patch's insert text as it came. Index entries of the app's tables move in
   * the same batch as the rows they point at.
   */
  write(ops: WireOp[], buf: ArrayBuffer | null, sync: boolean): void {
    const bytes = buf ? new Uint8Array(buf) : new Uint8Array(0);
    const out: StoreOp[] = [];
    // What this batch has already done to a row, for a later op on the same row.
    const pending = new Map<string, Uint8Array | null>();
    const cleared = new Set<string>();
    // The app's rows as this batch leaves them, for an index that appears partway through it.
    const touched = new Map<string, Map<string, Row | null>>();
    const added: [string, string][] = [];
    const note = (table: string, id: string, row: Row | null): void => {
      let rows = touched.get(table);
      if (!rows) touched.set(table, (rows = new Map()));
      rows.set(id, row);
    };
    for (const op of ops) {
      const table = op[1];
      const user = isUserTable(table);
      if (op[0] === 'clear') {
        out.push({ t: 'clear', table });
        if (user) for (const field of this.fieldsOf(table)) out.push({ t: 'clear', table: indexTable(table, field) });
        cleared.add(table);
        for (const key of [...pending.keys()]) if (key.startsWith(table + '\n')) pending.delete(key);
        touched.get(table)?.clear();
        continue;
      }
      const id = op[2];
      const key = table + '\n' + id;
      const stored = (): Uint8Array | null => {
        const earlier = pending.get(key);
        if (earlier !== undefined) return earlier;
        return cleared.has(table) ? null : (this.store.get(table, id)?.body ?? null);
      };
      if (op[0] === 'del') {
        const fields = user ? this.fieldsOf(table) : [];
        if (fields.length > 0) {
          const old = stored();
          if (old) this.reindex(table, id, fields, this.keysOf(fields, JSON.parse(decoder.decode(old)) as Row), this.keysOf(fields, null), out);
        }
        out.push({ t: 'del', table, id });
        pending.set(key, null);
        if (user) note(table, id, null);
        continue;
      }
      const insert = bytes.subarray(op[5], op[5] + op[6]);
      let body = insert;
      let rv = op[3];
      let flags = op[4];
      let next: Row | null = null;
      let fields = user ? this.fieldsOf(table) : [];
      const old = op[0] === 'merge' || fields.length > 0 ? stored() : null;
      const previous = old ? (JSON.parse(decoder.decode(old)) as Row) : null;
      // Read before the merge below reuses the object.
      const before = this.keysOf(fields, previous);
      if (op[0] === 'merge' && previous) {
        const patch = op[8] > 0 ? bytes.subarray(op[7], op[7] + op[8]) : insert;
        next = mergePatch(previous, JSON.parse(decoder.decode(patch))) as Row;
        const text = JSON.stringify(next);
        body = encoder.encode(text);
        rv = Number(next?._00_rv) || 0;
        flags = text.includes(U8_TAG) ? FLAG_U8 : 0;
      } else if (user) {
        next = JSON.parse(decoder.decode(insert)) as Row;
      }
      if (user && next) {
        const known = added.length;
        this.discover(table, next, added);
        if (added.length > known) {
          // An index born on this row comes after the ones there were, and has no old entry for it.
          fields = this.fieldsOf(table);
          while (before.length < fields.length) before.push(null);
        }
        this.reindex(table, id, fields, before, this.keysOf(fields, next), out);
        note(table, id, next);
      }
      out.push({ t: 'put', table, id, rv, flags, body });
      pending.set(key, body);
    }
    // An index that appeared in this batch still lacks the rows that were there before it.
    for (const [table, field] of added) {
      const index = indexTable(table, field);
      const mine = touched.get(table);
      const put = (id: string, row: Row): void => {
        const k = indexKey(extract(row, field));
        if (k !== null) out.push({ t: 'put', table: index, id: k + SEP + id, rv: 0, flags: FLAG_RAW, body: NO_ENTRY });
      };
      if (!cleared.has(table)) {
        this.scan(table, (row) => {
          const id = String(row.id);
          if (!mine?.has(id)) put(id, row);
        });
      }
      if (mine) for (const [id, row] of mine) if (row) put(id, row);
    }
    if (added.length > 0) out.push(this.registryOp());
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
    let rewritten = 0;
    for (const table of tables) {
      const fields = isUserTable(table) ? this.fieldsOf(table) : [];
      this.scan(table, (row, at, run) => {
        const rv = extract(row, '_00_rv');
        if (rv === null || rv === 0 || kept.has(String(row.id))) return;
        const id = runId(run, at);
        const before = this.keysOf(fields, row);
        row._00_rv = 0;
        this.reindex(table, id, fields, before, this.keysOf(fields, row), out);
        out.push({ t: 'put', table, id, rv: 0, flags: run.flags[at], body: encoder.encode(JSON.stringify(row)) });
        rewritten++;
      });
    }
    this.store.write(out);
    return rewritten;
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

  /**
   * The tables to read ahead, in the order worth reading them: the app's own
   * first (its first query is by id or over a window of one of them), then the
   * indexes over them.
   */
  warmOrder(): string[] {
    const names = this.store.tableNames();
    return [...names.filter((name) => !name.startsWith(HIDDEN)), ...names.filter((name) => name.startsWith(HIDDEN))];
  }

  /** Read every table's index into memory, so the first query does not wait for it. */
  warm(): void {
    for (const table of this.warmOrder()) this.store.ordered(table);
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

/**
 * Version of what the page and the worker say to each other. A worker that
 * answers `open` with another number was built from another release (a
 * prewarmed worker started from a URL remembered across a deploy) and is
 * replaced by a fresh one.
 */
export const ROWS_PROTOCOL = 1;

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
      return { result: { tables: visibleTables(store) } };
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
  // Dead bytes may reach half the live ones: the file is at most 1.5 times its rows.
  if (stats.garbageBytes > Math.max(4 << 20, stats.liveBytes / 2)) return 'compact';
  return null;
}
