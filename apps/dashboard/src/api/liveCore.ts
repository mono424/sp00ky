/**
 * The pure half of the live protocol, the reader's side of
 * `ssp_protocol::admin_live` (Rust). No imports, no DOM: it is tested with
 * plain `node` against the same golden vectors the Rust side is
 * (`packages/ssp-protocol/tests/admin_live_vectors.json`), so the two cannot
 * drift apart silently.
 *
 * Three shapes:
 * - `doc`: a JSON object, patched with an RFC 7396 merge patch. A missing
 *   member means null.
 * - `window`: `{rows, meta}`, rows keyed by one field. A patch carries `up`
 *   (changed or new rows, whole), `rm` (removed keys), `order` (only when
 *   applying `up`/`rm` alone would not reproduce it) and a merge patch of
 *   `meta`.
 * - `series`: `{items, keep, meta}`, items keyed by a growing number. A patch
 *   appends; the reader trims to `keep`.
 */

export type Shape = 'doc' | 'window' | 'series';

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type JsonObject = { [key: string]: Json };

export class ApplyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ApplyError';
  }
}

function isObject(v: unknown): v is JsonObject {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/** Drop `null` object members at every depth: the canonical form a host diffs. */
export function canonical(v: Json): Json {
  if (Array.isArray(v)) return v.map(canonical);
  if (isObject(v)) {
    const out: JsonObject = {};
    for (const [k, x] of Object.entries(v)) if (x !== null) out[k] = canonical(x);
    return out;
  }
  return v;
}

/** RFC 7396 `MergePatch(target, patch)`. Returns the result; never mutates. */
export function mergeApply(target: Json | undefined, patch: Json): Json {
  if (!isObject(patch)) return patch;
  const out: JsonObject = isObject(target) ? { ...target } : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v === null) delete out[k];
    else out[k] = mergeApply(out[k], v);
  }
  return out;
}

/** A row's key as the host spells it: strings verbatim, anything else as JSON. */
export function keyOf(row: Json, key: string): string | undefined {
  if (!isObject(row)) return undefined;
  const v = row[key];
  if (v === undefined || v === null) return undefined;
  return typeof v === 'string' ? v : JSON.stringify(v);
}

export interface WindowPatch {
  up?: JsonObject[];
  rm?: string[];
  order?: string[];
  meta?: Json;
}

export interface SeriesPatch {
  append?: JsonObject[];
  meta?: Json;
}

function applyWindow(key: string, state: Json, patch: Json): Json {
  if (!isObject(patch)) throw new ApplyError('window patch must be an object');
  const p = patch as WindowPatch;
  const base = isObject(state) ? state : {};
  const rows = Array.isArray(base.rows) ? base.rows : [];
  const order: string[] = [];
  const byKey = new Map<string, Json>();
  for (const row of rows) {
    const k = keyOf(row, key);
    if (k === undefined) throw new ApplyError('row without a key');
    order.push(k);
    byKey.set(k, row);
  }
  let kept = order;
  if (p.rm) {
    const gone = new Set(p.rm);
    kept = order.filter((k) => !gone.has(k));
    for (const k of gone) byKey.delete(k);
  }
  for (const row of p.up ?? []) {
    const k = keyOf(row, key);
    if (k === undefined) throw new ApplyError('row without a key');
    if (!byKey.has(k)) kept.push(k);
    byKey.set(k, row);
  }
  if (p.order) {
    if (p.order.length !== byKey.size || p.order.some((k) => !byKey.has(k))) {
      throw new ApplyError('order names rows the window does not hold');
    }
    kept = p.order;
  }
  const out: JsonObject = { ...base, rows: kept.map((k) => byKey.get(k) as Json) };
  if (p.meta !== undefined) out.meta = mergeApply(isObject(base.meta) ? base.meta : {}, p.meta);
  return out;
}

function seriesKey(item: Json, key: string): number | undefined {
  if (!isObject(item)) return undefined;
  const v = item[key];
  return typeof v === 'number' ? v : undefined;
}

function applySeries(key: string, state: Json, patch: Json): Json {
  if (!isObject(patch)) throw new ApplyError('series patch must be an object');
  const p = patch as SeriesPatch;
  const base = isObject(state) ? state : {};
  const keep = typeof base.keep === 'number' ? base.keep : Infinity;
  const items = Array.isArray(base.items) ? [...base.items] : [];
  let last = items.length ? seriesKey(items[items.length - 1], key) : undefined;
  for (const item of p.append ?? []) {
    const k = seriesKey(item, key);
    if (k === undefined) throw new ApplyError('item without a key');
    if (last !== undefined && k <= last) throw new ApplyError('series went backwards');
    last = k;
    items.push(item);
  }
  if (items.length > keep) items.splice(0, items.length - keep);
  const out: JsonObject = { ...base, items };
  if (p.meta !== undefined) out.meta = mergeApply(isObject(base.meta) ? base.meta : {}, p.meta);
  return out;
}

/** Apply `patch` to `state` (a canonical state of `shape`). Never mutates. */
export function applyPatch(shape: Shape, key: string, state: Json, patch: Json): Json {
  switch (shape) {
    case 'doc':
      return mergeApply(state, patch);
    case 'window':
      return applyWindow(key, state, patch);
    case 'series':
      return applySeries(key, state, patch);
  }
}

/** Order-insensitive deep equality for JSON values. */
export function jsonEqual(a: Json | undefined, b: Json | undefined): boolean {
  if (a === b) return true;
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((x, i) => jsonEqual(x, b[i]));
  }
  if (isObject(a) && isObject(b)) {
    const ka = Object.keys(a);
    if (ka.length !== Object.keys(b).length) return false;
    return ka.every((k) => k in b && jsonEqual(a[k], b[k]));
  }
  return false;
}

/** A topic's instance key: the name, plus its arguments with sorted keys. */
export function topicKey(topic: string, args?: Json): string {
  if (args === undefined || args === null) return topic;
  if (isObject(args) && Object.keys(args).length === 0) return topic;
  return `${topic}?${canonicalJson(args)}`;
}

/** JSON with object keys sorted at every depth, matching the Rust host. */
export function canonicalJson(v: Json): string {
  const sorted = (x: Json): Json => {
    if (Array.isArray(x)) return x.map(sorted);
    if (isObject(x)) {
      const out: JsonObject = {};
      for (const k of Object.keys(x).sort()) out[k] = sorted(x[k]);
      return out;
    }
    return x;
  };
  return JSON.stringify(sorted(v));
}
