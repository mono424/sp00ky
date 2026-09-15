import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { createJiti } from 'jiti';
import { QueryBuilder } from '@spooky-sync/query-builder';
import { isArgProxy, makeArgProxy } from './proxy.js';

export { isArgProxy, makeArgProxy } from './proxy.js';

export type WhereMode = 'any' | 'static';

export interface AllowlistEntry {
  name: string;
  surql: string;
  whereMode: WhereMode;
}

export interface Allowlist {
  generator: 1;
  app: string;
  sourceHash: string;
  entries: AllowlistEntry[];
}

export interface GenerateOptions {
  /** Absolute path to the app's query module (the `q*` exports). */
  queries: string;
  /** Absolute path to the generated schema module (`export const schema`). */
  schema: string;
  /** App name written into the output; defaults to `default`. */
  app?: string;
}

export interface GenerateResult {
  allowlist: Allowlist;
  /** `q*` exports the generator could not turn into an entry. Non-empty means failure. */
  errors: { name: string; error: string }[];
  warnings: string[];
}

/** Marker the `where` wrapper writes into a builder's options when it saw a proxy. */
const WHERE_ANY = '__whereAny';
const WRAPPED = Symbol.for('@spooky-sync/query-allowlist:where-wrapped');

/** How many proxies beyond `fn.length - 1` a `q*` is handed, for rest args. */
const EXTRA_ARGS = 4;

const R_ONLY = ['preload', 'useRemote', 'remoteQuery', 'run', 'create', 'update', 'delete', 'live'];

/**
 * `QueryBuilder.prototype.where` is wrapped once per process: when the whole
 * `where` argument is an arg proxy (`(db, where) => db.query(t).where(where)`)
 * the builder cannot know the predicate, so the entry is `whereMode: 'any'`.
 * The spread inside `where()` sees no own keys on the proxy, so the surql has
 * no WHERE clause from it; the marker rides in `options` through `.one()`'s
 * `{ ...this.options }` copy and `build()` into the `InnerQuery`.
 */
function installWhereWrapper(): void {
  const proto = QueryBuilder.prototype as any;
  if (proto[WRAPPED]) return;
  const original = proto.where;
  proto.where = function (this: any, conditions: unknown) {
    if (isArgProxy(conditions)) {
      this.getOptions()[WHERE_ANY] = true;
    }
    return original.call(this, conditions);
  };
  // `LIMIT ${limit}` / `START ${offset}` stringify with the `string` hint, so a
  // proxy handed straight to `.limit(page)` would render as `LIMIT proxy:page`.
  // Substitute the same `1` the proxy yields in arithmetic (`window * page`).
  for (const method of ['limit', 'offset'] as const) {
    const orig = proto[method];
    proto[method] = function (this: any, count: unknown) {
      return orig.call(this, isArgProxy(count) ? 1 : count);
    };
  }
  proto[WRAPPED] = true;
}

interface RecordingContext {
  /** Table substituted for a proxied `db.query(<proxy>)`; undefined on the probe pass. */
  fanoutTable: string | undefined;
  sawProxiedTable: boolean;
}

function makeRecordingDb(schema: any, ctx: RecordingContext, probeTable: string) {
  const db: Record<string, unknown> = {
    query(table: unknown) {
      let name: string;
      if (isArgProxy(table)) {
        ctx.sawProxiedTable = true;
        name = ctx.fanoutTable ?? probeTable;
      } else {
        name = String(table);
      }
      return new QueryBuilder(schema, name as never, () => undefined, {});
    },
  };
  for (const member of R_ONLY) {
    db[member] = () => {
      throw new Error(`r*-only API called from a q* builder: db.${member}()`);
    };
  }
  return db;
}

function argProxies(fn: Function): unknown[] {
  const count = Math.max(0, fn.length - 1) + EXTRA_ARGS;
  return Array.from({ length: count }, (_, i) => makeArgProxy(`arg${i}`));
}

function surqlOf(result: unknown, name: string): { surql: string; whereAny: boolean } {
  if (result && typeof (result as any).then === 'function') {
    throw new Error(`${name} returned a Promise; q* must be pure/sync (r* reads go elsewhere)`);
  }
  const inner = (result as any)?.innerQuery;
  const query = inner?.selectQuery?.query;
  if (typeof query !== 'string') {
    throw new Error(`${name} did not return a built query (expected .build() result with innerQuery)`);
  }
  const options = typeof inner.getOptions === 'function' ? inner.getOptions() : {};
  return { surql: query, whereAny: options?.[WHERE_ANY] === true };
}

async function loadModule(jitiBase: string, path: string): Promise<Record<string, unknown>> {
  const jiti = createJiti(jitiBase, { fsCache: false, moduleCache: false, interopDefault: true });
  const mod = await jiti.import<Record<string, unknown>>(pathToFileURL(path).href);
  return mod;
}

function tableNames(schema: any): string[] {
  const tables = schema?.tables;
  if (!Array.isArray(tables)) {
    throw new Error('schema module must export `schema` with a `tables` array');
  }
  return tables.map((t: any) => String(t.name));
}

function readStringList(mod: Record<string, unknown>, key: string): Set<string> {
  const value = mod[key];
  if (value === undefined) return new Set();
  if (!Array.isArray(value) || value.some((v) => typeof v !== 'string')) {
    throw new Error(`${key} must be a string[]`);
  }
  return new Set(value as string[]);
}

export async function generateAllowlist(opts: GenerateOptions): Promise<GenerateResult> {
  installWhereWrapper();

  const source = readFileSync(opts.queries);
  const sourceHash = createHash('sha256').update(source).digest('hex');

  const schemaModule = await loadModule(pathToFileURL(opts.schema).href, opts.schema);
  const schema = (schemaModule.schema ?? schemaModule.default) as any;
  const tables = tableNames(schema);
  if (tables.length === 0) throw new Error('schema has no tables');

  const queryModule = await loadModule(pathToFileURL(opts.queries).href, opts.queries);

  const errors: GenerateResult['errors'] = [];
  const warnings: string[] = [];
  const skip = readStringList(queryModule, 'allowlistSkip');
  const forceAny = readStringList(queryModule, 'allowlistWhereAny');
  const samplesRaw = queryModule.allowlistSamples;
  const samples: Record<string, unknown[][]> =
    samplesRaw && typeof samplesRaw === 'object' ? (samplesRaw as Record<string, unknown[][]>) : {};

  const collected: AllowlistEntry[] = [];

  const record = (name: string, fn: Function, args: unknown[], base: string) => {
    const ctx: RecordingContext = { fanoutTable: undefined, sawProxiedTable: false };
    const probe = fn(makeRecordingDb(schema, ctx, tables[0]), ...args);
    const modeOf = (whereAny: boolean): WhereMode =>
      whereAny || forceAny.has(base) ? 'any' : 'static';

    if (!ctx.sawProxiedTable) {
      const { surql, whereAny } = surqlOf(probe, name);
      collected.push({ name, surql, whereMode: modeOf(whereAny) });
      return;
    }

    warnings.push(`${name}: db.query() called with a runtime table name; fanned out over ${tables.length} tables`);
    for (const table of tables) {
      const fanCtx: RecordingContext = { fanoutTable: table, sawProxiedTable: false };
      const result = fn(makeRecordingDb(schema, fanCtx, table), ...args);
      const { surql, whereAny } = surqlOf(result, `${name}[${table}]`);
      collected.push({ name: `${name}[${table}]`, surql, whereMode: modeOf(whereAny) });
    }
  };

  for (const [name, value] of Object.entries(queryModule)) {
    if (!/^q[A-Z]/.test(name) || typeof value !== 'function') continue;
    if (skip.has(name)) {
      warnings.push(`${name}: skipped (allowlistSkip)`);
      continue;
    }
    const fn = value as Function;
    try {
      record(name, fn, argProxies(fn), name);
    } catch (e) {
      errors.push({ name, error: e instanceof Error ? e.message : String(e) });
    }

    const rows = samples[name];
    if (!rows) continue;
    if (!Array.isArray(rows)) {
      errors.push({ name, error: 'allowlistSamples entry must be an array of argument arrays' });
      continue;
    }
    rows.forEach((row, i) => {
      const sampleName = `${name}#${i}`;
      try {
        if (!Array.isArray(row)) throw new Error('sample must be an argument array');
        record(sampleName, fn, row, name);
      } catch (e) {
        errors.push({ name: sampleName, error: e instanceof Error ? e.message : String(e) });
      }
    });
  }

  for (const name of Object.keys(samples)) {
    if (!(name in queryModule)) errors.push({ name, error: 'allowlistSamples names an export that does not exist' });
  }

  collected.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const bySurql = new Map<string, AllowlistEntry>();
  for (const entry of collected) {
    const existing = bySurql.get(entry.surql);
    if (!existing) {
      bySurql.set(entry.surql, { ...entry });
    } else if (entry.whereMode === 'any' && existing.whereMode !== 'any') {
      // Same text, but one caller supplies its predicate at runtime: the
      // permissive mode is the only one that admits both.
      existing.whereMode = 'any';
    }
  }
  const entries = [...bySurql.values()].sort((a, b) =>
    a.name < b.name ? -1 : a.name > b.name ? 1 : 0
  );

  return {
    allowlist: { generator: 1, app: opts.app ?? 'default', sourceHash, entries },
    errors,
    warnings,
  };
}
