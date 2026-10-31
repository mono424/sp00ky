/**
 * Live topics as TanStack DB collections.
 *
 * Every topic the host serves becomes one collection, fed by a custom sync
 * adapter over the {@link LiveClient}:
 *
 * - a **window** topic's rows are the collection's rows, keyed like the host
 *   keys them, so screens filter and sort with live queries instead of
 *   hand-written loops (`useLiveQuery` from `@tanstack/solid-db`);
 * - a **series** topic's items are rows keyed by their growing key, trimmed to
 *   the host's `keep`;
 * - a **doc** topic is one row, `{ id: 'doc', data }`.
 *
 * Window and series `meta` (totals, counters) rides along as the collection's
 * `utils.meta()` signal.
 *
 * # Caching
 *
 * With persistence (see `persistence.ts`) a collection's rows, its meta and
 * its resume cursor `{boot, seq}` are written in the same transaction. On the
 * next visit the rows paint before the socket opens, the collection reports
 * ready at once, and the subscription carries the cursor: a host that still
 * has the patches since then sends only those. A host that restarted (new
 * `boot`) or has moved on answers with a snapshot, which replaces the rows
 * atomically (`begin({ snapshot: true })`), so the cache can never leave a
 * stale row behind.
 */

import { createSignal, type Accessor } from 'solid-js';
import { createCollection, type Collection } from '@tanstack/solid-db';
import type { PersistedCollectionPersistence } from '@tanstack/browser-db-sqlite-persistence';
import type { LiveClient, TopicSink } from './live';
import {
  keyOf,
  mergeApply,
  topicKey,
  type Json,
  type JsonObject,
  type SeriesPatch,
  type Shape,
  type WindowPatch,
} from './liveCore';

/** Bumped whenever the cached row shape changes: a mismatch drops the cache. */
const CACHE_SCHEMA = 1;

export interface TopicSpec {
  topic: string;
  args?: Json;
  shape: Shape;
  /** Row key field (window) or growing item key (series). */
  key: string;
  /** Keep this topic out of the on-disk cache (high churn, or sensitive). */
  ephemeral?: boolean;
}

/** The host's topic catalog, as the dashboard reads it. */
export const TOPICS = {
  overview: { topic: 'overview', shape: 'doc', key: 'id' },
  presence: { topic: 'presence', shape: 'doc', key: 'id' },
  syncMetrics: { topic: 'metrics', args: { name: 'sync' }, shape: 'series', key: 'ts' },
  presenceMetrics: { topic: 'metrics', args: { name: 'presence' }, shape: 'series', key: 'ts' },
  jobMetrics: { topic: 'metrics', args: { name: 'jobs' }, shape: 'series', key: 't' },
  jobs: { topic: 'jobs', shape: 'window', key: 'id' },
  runs: { topic: 'runs', shape: 'window', key: 'id' },
  incidents: { topic: 'incidents', shape: 'window', key: 'id' },
  operations: { topic: 'operations', shape: 'window', key: 'id' },
  // Log lines are the most churn and the most likely to carry something
  // sensitive; they are never written to disk.
  logs: { topic: 'logs', shape: 'series', key: 'n', ephemeral: true },
} as const satisfies Record<string, TopicSpec>;

export type Row = JsonObject;
export type DocRow<T = JsonObject> = { id: 'doc'; data: T };

export interface TopicUtils {
  /** Window/series meta; `{}` for a doc. */
  meta: Accessor<JsonObject>;
  [key: string]: unknown;
}

export type TopicCollection = Collection<Row, string | number, TopicUtils>;

interface Cursor {
  boot: string;
  seq: number;
}

/** What the sync adapter needs from the collection metadata API. */
interface MetaStore {
  get(key: string): unknown;
  set(key: string, value: unknown): void;
}

function memoryStore(): MetaStore {
  const m = new Map<string, unknown>();
  return { get: (k) => m.get(k), set: (k, v) => void m.set(k, v) };
}

function isObject(v: unknown): v is JsonObject {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function rowKey(spec: TopicSpec, row: Row): string | number {
  if (spec.shape === 'doc') return 'doc';
  if (spec.shape === 'series') return row[spec.key] as number;
  return keyOf(row, spec.key) ?? '';
}

/** The rows a full topic state stands for. */
function rowsOf(spec: TopicSpec, data: Json): Row[] {
  if (spec.shape === 'doc') return [{ id: 'doc', data: isObject(data) ? data : {} }];
  const list = isObject(data) ? data[spec.shape === 'window' ? 'rows' : 'items'] : undefined;
  return Array.isArray(list) ? list.filter(isObject) : [];
}

function liveCollectionOptions(client: LiveClient, spec: TopicSpec, id: string) {
  const [meta, setMeta] = createSignal<JsonObject>({});

  return {
    id,
    getKey: (row: Row) => rowKey(spec, row),
    // Keep a collection a while after its last screen closes, so going back
    // to a page is instant rather than a resubscribe.
    gcTime: 5 * 60_000,
    utils: { meta } satisfies TopicUtils,
    sync: {
      rowUpdateMode: 'full' as const,
      sync: (params: {
        collection: Collection<Row, string | number, TopicUtils>;
        begin: (o?: { snapshot?: boolean }) => void;
        write: (m: { type: 'insert' | 'update'; value: Row } | { type: 'delete'; key: string | number }) => void;
        commit: () => unknown;
        markReady: () => void;
        fail: (e: unknown) => void;
        reportError: (e: unknown) => void;
        reportHealthy: (e?: unknown) => void;
        metadata?: { collection: MetaStore };
      }) => {
        const { collection, begin, write, commit, markReady } = params;
        const store = params.metadata?.collection ?? memoryStore();
        let ready = false;
        const becomeReady = () => {
          if (ready) return;
          ready = true;
          markReady();
        };

        const cursor = (): Cursor | undefined => {
          const c = store.get('cursor');
          return isObject(c) && typeof c.boot === 'string' && typeof c.seq === 'number'
            ? { boot: c.boot, seq: c.seq }
            : undefined;
        };
        const stored = store.get('meta');
        if (isObject(stored)) setMeta(stored);
        // A cached state paints now; the socket only refines it.
        if (cursor()) becomeReady();

        const keep = (): number => {
          const k = store.get('keep');
          return typeof k === 'number' ? k : Infinity;
        };

        const sink: TopicSink = {
          since: (boot) => {
            const c = cursor();
            return c && c.boot === boot ? c.seq : undefined;
          },

          snap: (boot, seq, data) => {
            begin({ snapshot: true });
            for (const row of rowsOf(spec, data)) write({ type: 'insert', value: row });
            const nextMeta = isObject(data) && isObject(data.meta) ? data.meta : {};
            store.set('cursor', { boot, seq });
            store.set('meta', nextMeta);
            if (spec.shape === 'series' && isObject(data) && typeof data.keep === 'number') {
              store.set('keep', data.keep);
            }
            commit();
            setMeta(nextMeta);
            becomeReady();
          },

          diff: (boot, from, seq, patch) => {
            const c = cursor();
            if (!c || c.boot !== boot || c.seq !== from) return false;
            begin();
            let nextMeta: JsonObject | undefined;
            if (spec.shape === 'doc') {
              const current = (collection.get('doc') as DocRow | undefined)?.data ?? {};
              const data = mergeApply(current, patch);
              write({
                type: collection.has('doc') ? 'update' : 'insert',
                value: { id: 'doc', data: isObject(data) ? data : {} },
              });
            } else if (spec.shape === 'window') {
              const p = patch as WindowPatch;
              for (const k of p.rm ?? []) if (collection.has(k)) write({ type: 'delete', key: k });
              for (const row of p.up ?? []) {
                const k = rowKey(spec, row);
                write({ type: collection.has(k) ? 'update' : 'insert', value: row });
              }
              // `order` is the host's display order. Collections are keyed, not
              // ordered: screens sort by a row field in their live query.
              if (p.meta !== undefined) nextMeta = mergeApply(meta(), p.meta) as JsonObject;
            } else {
              const p = patch as SeriesPatch;
              const appended = p.append ?? [];
              // Trim to `keep` across what is held plus what arrives, oldest
              // first, without inserting an item only to delete it.
              const held = [...collection.keys()].map(Number).sort((a, b) => a - b);
              const excess = held.length + appended.length - keep();
              let skip = 0;
              if (excess > 0) {
                held.slice(0, excess).forEach((k) => write({ type: 'delete', key: k }));
                skip = Math.max(0, excess - held.length);
              }
              appended.slice(skip).forEach((item) => write({ type: 'insert', value: item }));
              if (p.meta !== undefined) nextMeta = mergeApply(meta(), p.meta) as JsonObject;
            }
            store.set('cursor', { boot, seq });
            if (nextMeta) store.set('meta', nextMeta);
            commit();
            if (nextMeta) setMeta(nextMeta);
            return true;
          },

          error: (message) => {
            const err = new Error(message);
            if (ready) params.reportError(err);
            else params.fail(err);
          },
        };

        return client.subscribe(spec.topic, spec.args, sink);
      },
    },
  };
}

/** One collection per topic per host, shared by every screen that reads it. */
export class TopicCollections {
  private readonly made = new Map<string, TopicCollection>();

  constructor(
    private readonly client: LiveClient,
    /** Distinguishes hosts in the shared cache: an endpoint, or a project id. */
    private readonly scope: string,
    private readonly persistence: PersistedCollectionPersistence | null,
  ) {}

  get(spec: TopicSpec): TopicCollection {
    const key = topicKey(spec.topic, spec.args);
    const existing = this.made.get(key);
    if (existing) return existing;
    const id = `spky:${this.scope}:${key}`;
    const options = liveCollectionOptions(this.client, spec, id);
    const collection = (
      this.persistence && !spec.ephemeral
        ? createCollection(
            persistedOptions({ ...options, persistence: this.persistence, schemaVersion: CACHE_SCHEMA }),
          )
        : createCollection(options as never)
    ) as unknown as TopicCollection;
    this.made.set(key, collection);
    return collection;
  }

  /** Stop every topic and drop the collections (sign-out, host switch). */
  async dispose(): Promise<void> {
    const all = [...this.made.values()];
    this.made.clear();
    await Promise.all(all.map((c) => c.cleanup().catch(() => undefined)));
  }
}

/**
 * `persistedCollectionOptions`, loaded with the persistence module so the
 * wasm-backed code stays out of the bundle where no cache can run. The
 * module is already imported by the time a persistence object exists.
 */
let persistedOptions: (o: unknown) => never = () => {
  throw new Error('persistence module not loaded');
};

export async function loadPersistedOptions(): Promise<void> {
  const mod = await import('@tanstack/browser-db-sqlite-persistence');
  persistedOptions = mod.persistedCollectionOptions as unknown as (o: unknown) => never;
}
