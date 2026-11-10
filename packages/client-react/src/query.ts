import { useEffect, useState, useSyncExternalStore } from 'react';
import type {
  ColumnSchema,
  FinalQuery,
  QueryResult,
  SchemaStructure,
  TableNames,
} from '@spooky-sync/query-builder';
import type {
  PreloadOptions as CorePreloadOptions,
  Sp00kyQueryResultPromise,
} from '@spooky-sync/core';
import { SyncedDb } from './db';
import { useDb, useOptionalDb } from './provider';

// ---------------------------------------------------------------------------
// Pure part: structural sharing and the query state machine.
// ---------------------------------------------------------------------------

const isPlainObject = (v: unknown): v is Record<string, unknown> => {
  if (v === null || typeof v !== 'object') return false;
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
};

/**
 * Value equality for the leaves `share` does not descend into. The decoder
 * hands out a fresh RecordId / Date / Uuid / Duration for every record-link
 * and datetime column on every emission, so by identity every such field
 * "changed" every time. Two instances of the same class that print alike are
 * the same value. A class whose `toString` is the default `[object X]`
 * (ArrayBuffer, Map, ...) cannot be compared that way and counts as changed.
 */
export function sameValue(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a == null || b == null || typeof a !== 'object' || typeof b !== 'object') return false;
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
  const ctor = (a as object).constructor;
  if (!ctor || ctor !== (b as object).constructor || ctor === Object || ctor === Array)
    return false;
  const text = String(a);
  return !text.startsWith('[object ') && text === String(b);
}

const keyOf = (v: unknown): string | undefined => {
  if (!isPlainObject(v)) return undefined;
  const id = v.id;
  return id === undefined || id === null ? undefined : String(id);
};

/**
 * Structural sharing between two emissions: returns `prev` itself where
 * `next` is deep-equal to it, and a new value that reuses every unchanged
 * subtree otherwise. Array items with an `id` are matched by id, so a row keeps
 * its object identity across reorders, inserts and deletes; `React.memo` rows
 * and `useMemo` deps keyed on a row only re-run when that row changed.
 *
 * Never mutates either argument: the engine may hand the same row objects to
 * several subscribers.
 */
export function share<T>(prev: unknown, next: T): T {
  if (prev === next) return next;
  if (Array.isArray(prev) && Array.isArray(next)) {
    const byKey = new Map<string, unknown>();
    for (const item of prev) {
      const k = keyOf(item);
      if (k !== undefined) byKey.set(k, item);
    }
    let same = prev.length === next.length;
    const out = next.map((item, i) => {
      const k = keyOf(item);
      const base = k !== undefined ? byKey.get(k) : prev[i];
      const v = base === undefined ? item : share(base, item);
      if (v !== prev[i]) same = false;
      return v;
    });
    return (same ? prev : out) as T;
  }
  if (isPlainObject(prev) && isPlainObject(next)) {
    const keys = Object.keys(next);
    let same = keys.length === Object.keys(prev).length;
    const out: Record<string, unknown> = {};
    for (const k of keys) {
      const v = k in prev ? share(prev[k], next[k]) : next[k];
      if (v !== prev[k] || !(k in prev)) same = false;
      out[k] = v;
    }
    return (same ? prev : out) as T;
  }
  return (sameValue(prev, next) ? prev : next) as T;
}

/** What the hook renders from. One immutable value per change. */
export interface QueryState {
  /** Last result: rows for a list, a row or `null` for `.one()`, `null` before any. */
  data: unknown;
  error: Error | undefined;
  /** Server membership known and painted for the current identity (core authority). */
  isAuthoritative: boolean;
  /** An emission WITH data was applied for the current identity. */
  hasPainted: boolean;
  isFetching: boolean;
}

export type QueryEvent =
  | { type: 'reset' }
  | { type: 'rows'; rows: Record<string, unknown>[]; isOne: boolean }
  | { type: 'status'; fetching: boolean }
  | { type: 'authority'; known: boolean }
  | { type: 'error'; error: Error };

export const initialQueryState: QueryState = {
  data: null,
  error: undefined,
  isAuthoritative: false,
  hasPainted: false,
  isFetching: false,
};

/**
 * The query state machine, the same contract as the Solid 2 binding:
 *
 * - `reset` (a new identity) clears authority, the paint latch and the error.
 *   The previous `data` stays until the new identity emits, so growing a
 *   `.limit()` does not flash an empty list.
 * - `rows` applies every emission as-is (the core only emits what it decided
 *   the query holds); one WITH data ends the cold load on its own.
 * - `authority` is the channel that ends a cold load whose honest answer is
 *   zero rows. A flip back to false is a bucket switch (another principal):
 *   what is painted is the previous one's, so the paint latch goes too.
 *
 * Returns `state` itself when nothing changed, so React skips the render.
 */
export function reduceQuery(state: QueryState, event: QueryEvent): QueryState {
  switch (event.type) {
    case 'reset':
      if (!state.isAuthoritative && !state.hasPainted && !state.error && !state.isFetching) {
        return state;
      }
      return {
        ...state,
        isAuthoritative: false,
        hasPainted: false,
        error: undefined,
        isFetching: false,
      };
    case 'rows': {
      const next = event.isOne ? (event.rows[0] ?? null) : event.rows;
      const hasRows = event.isOne ? next !== null && next !== undefined : event.rows.length > 0;
      const data = share(state.data, next);
      const hasPainted = state.hasPainted || hasRows;
      if (data === state.data && hasPainted === state.hasPainted) return state;
      return { ...state, data, hasPainted };
    }
    case 'status':
      return event.fetching === state.isFetching ? state : { ...state, isFetching: event.fetching };
    case 'authority': {
      const hasPainted = event.known ? state.hasPainted : false;
      if (event.known === state.isAuthoritative && hasPainted === state.hasPainted) return state;
      return { ...state, isAuthoritative: event.known, hasPainted };
    }
    case 'error':
      return { ...state, error: event.error };
  }
}

// ---------------------------------------------------------------------------
// Imperative shell: one engine subscription per hook, feeding the reducer.
// ---------------------------------------------------------------------------

type AnyFinalQuery = FinalQuery<any, any, any, any, any, Sp00kyQueryResultPromise>;
type Sp00ky = ReturnType<SyncedDb<any>['getSp00ky']>;

class QueryStore {
  private state: QueryState = initialQueryState;
  private listeners = new Set<() => void>();
  /** Registered hash of the attached query, for opt-in deregistration on unmount. */
  activeHash: string | undefined;

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = () => this.state;

  private dispatch(event: QueryEvent) {
    const next = reduceQuery(this.state, event);
    if (next === this.state) return;
    this.state = next;
    for (const l of this.listeners) l();
  }

  /** Clear the identity flags: the query went away or was disabled. */
  reset() {
    this.dispatch({ type: 'reset' });
  }

  /**
   * Register `query` and mirror its rows, fetch status and authority until the
   * returned cleanup runs. Registration can fail (canonically the SSP
   * answering 503 NOT_READY while it bootstraps): that surfaces as `error`, and
   * the sync scheduler retries the registration underneath.
   */
  attach(sp00ky: Sp00ky, query: AnyFinalQuery): () => void {
    this.dispatch({ type: 'reset' });
    let disposed = false;
    const cleanups: (() => void)[] = [];
    // `sp00ky.subscribe` resolves its unsubscribe asynchronously; the status
    // and authority subscriptions return one directly. If teardown already
    // happened while the promise was in flight, unsubscribe at once.
    const addCleanup = (c: (() => void) | Promise<() => void>) => {
      if (typeof c === 'function') {
        if (disposed) c();
        else cleanups.push(c);
        return;
      }
      void c.then(
        (fn) => {
          if (disposed) fn();
          else cleanups.push(fn);
        },
        () => {}
      );
    };

    query.run().then(
      ({ hash }: { hash: string }) => {
        if (disposed) return;
        this.activeHash = hash;
        addCleanup(
          sp00ky.subscribeQueryStatus(
            hash,
            (status) => {
              if (!disposed) this.dispatch({ type: 'status', fetching: status === 'fetching' });
            },
            { immediate: true }
          )
        );
        addCleanup(
          sp00ky.subscribe(
            hash,
            (rows) => {
              if (disposed) return;
              const t0 = performance.now();
              this.dispatch({ type: 'rows', rows, isOne: query.isOne });
              sp00ky.reportFrontendTiming(hash, performance.now() - t0);
            },
            { immediate: true }
          )
        );
        // Subscribed after the rows, so the immediate deliveries keep the
        // core's order: authority flips only after the rows it covers.
        addCleanup(
          sp00ky.subscribeQueryAuthority(
            hash,
            (known) => {
              if (!disposed) this.dispatch({ type: 'authority', known });
            },
            { immediate: true }
          )
        );
      },
      (err: unknown) => {
        if (disposed) return;
        this.dispatch({
          type: 'error',
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }
    );

    return () => {
      disposed = true;
      for (const c of cleanups) c();
    };
  }
}

// ---------------------------------------------------------------------------
// Hooks.
// ---------------------------------------------------------------------------

type QueryArg<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
> = FinalQuery<S, TableName, T, RelatedFields, IsOne, Sp00kyQueryResultPromise> | null | undefined;

export interface QueryOptions {
  /** Only register while true (default `true`). `false` keeps the last data. */
  enabled?: boolean;
  /**
   * Tear down the query (remote `_00_query` view + local WASM view) when this
   * hook unmounts and no other subscriber remains, instead of keeping it
   * resident for cheap re-subscription. Use for viewport-windowed lists that
   * mount and unmount a query per scroll window. Trade-off: scrolling back to
   * a torn-down window re-registers it.
   */
  deregisterOnCleanup?: boolean;
}

export interface UseQueryResult<TData> {
  /**
   * The result. `[]` / `null` before the first emission, then each live
   * emission with structural sharing: unchanged rows keep their object
   * identity, so memoized rows only re-render when their own data changed.
   */
  data: TData;
  /** Registration or sync error (e.g. SSP 503 during bootstrap). Never thrown. */
  error: Error | undefined;
  /**
   * True only on a COLD first load: nothing painted for this identity, the
   * server has not answered, and registration has not failed. A paint from the
   * local cache ends it, and so does the server answering with zero rows.
   */
  isLoading: boolean;
  /** A fetch cycle is in flight (registration, a sync round). */
  isFetching: boolean;
  /**
   * `data` holds the rendered rows of a known membership for this identity:
   * the server's answer, or the membership an earlier session left on this
   * device. From here `data` is an answer, not a cache guess.
   */
  isAuthoritative: boolean;
  /** `data` holds something: rows for a list, a row for `.one()`. */
  hasData: boolean;
  /** Authoritative AND no data: the server said "no rows". Render empty states on this. */
  isEmpty: boolean;
  /**
   * Authoritative AND idle. The gate for trusting a short window as the end of
   * a list, and for "create it if it is missing". Not monotonic: a later sync
   * round re-enters `isFetching`.
   */
  isSettled: boolean;
}

/**
 * What `data` holds: a list is never null (`[]` until the first emission), a
 * `.one()` row is `null` until it exists.
 */
export type QueryData<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
> = IsOne extends true
  ? QueryResult<S, TableName, RelatedFields, true> | null
  : QueryResult<S, TableName, RelatedFields, false>;

const EMPTY_LIST: readonly never[] = Object.freeze([]);

/**
 * Live query. Pass a built query (`db.query('post')...build()`), or `null` /
 * `undefined` to skip. The query is re-registered only when its identity hash
 * (surql + vars) changes, so building it inline on every render is fine.
 *
 * ```tsx
 * const posts = useQuery(db.query('post').orderBy('created_at', 'desc').limit(20).build());
 * ```
 */
export function useQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
  TData = QueryData<S, TableName, RelatedFields, IsOne>,
>(
  query: QueryArg<S, TableName, T, RelatedFields, IsOne>,
  options?: QueryOptions
): UseQueryResult<TData>;
export function useQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
  TData = QueryData<S, TableName, RelatedFields, IsOne>,
>(
  db: SyncedDb<S>,
  query: QueryArg<S, TableName, T, RelatedFields, IsOne>,
  options?: QueryOptions
): UseQueryResult<TData>;
export function useQuery(
  dbOrQuery: SyncedDb<any> | AnyFinalQuery | null | undefined,
  queryOrOptions?: AnyFinalQuery | QueryOptions | null,
  maybeOptions?: QueryOptions
): UseQueryResult<unknown> {
  const explicit = dbOrQuery instanceof SyncedDb ? dbOrQuery : null;
  // Hooks run unconditionally; `useContext` is cheap and the explicit db wins.
  const contextDb = useOptionalDb();
  const db = explicit ?? contextDb;
  if (!db) {
    throw new Error(
      'useQuery: no db argument and no <Sp00kyProvider> above. ' +
        'Pass a SyncedDb or wrap your app in <Sp00kyProvider>.'
    );
  }
  const query = (explicit ? queryOrOptions : dbOrQuery) as AnyFinalQuery | null | undefined;
  const options = (explicit ? maybeOptions : queryOrOptions) as QueryOptions | undefined;

  const [store] = useState(() => new QueryStore());
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);

  const enabled = options?.enabled ?? true;
  const identity = enabled && query ? query.hash : null;

  useEffect(() => {
    if (identity === null || !query) {
      store.reset();
      return;
    }
    return store.attach(db.getSp00ky(), query);
    // `query` is read from the render whose identity changed: same hash, same query.
  }, [store, db, identity]);

  const deregister = options?.deregisterOnCleanup ?? false;
  useEffect(() => {
    if (!deregister) return;
    // Declared after the attach effect, so on unmount its cleanup runs second:
    // this hook's callback is already gone and deregisterQuery's refcount guard
    // sees the true remaining-subscriber count.
    return () => {
      if (store.activeHash) db.getSp00ky().deregisterQuery(store.activeHash);
    };
  }, [store, db, deregister]);

  const isOne = query?.isOne;
  let data = state.data;
  if (isOne === false && !Array.isArray(data)) data = EMPTY_LIST;
  else if (isOne === true && Array.isArray(data)) data = null;

  const hasData = Array.isArray(data) ? data.length > 0 : data !== null && data !== undefined;
  return {
    data,
    error: state.error,
    isLoading: !state.isAuthoritative && !state.hasPainted && state.error === undefined,
    isFetching: state.isFetching,
    isAuthoritative: state.isAuthoritative,
    hasData,
    isEmpty: state.isAuthoritative && !hasData,
    isSettled: state.isAuthoritative && !state.isFetching,
  };
}

export type UsePreloadOptions = CorePreloadOptions & {
  /** Only preload while true (default `true`). */
  enabled?: boolean;
};

/**
 * Fire-and-forget prewarm: registers `query` via `db.preload` (a live query
 * nobody subscribes to, evicted a ttl after registration unless a view mounts
 * it first) whenever its identity changes. Nothing to clean up.
 *
 * Typical use: inside a list row, preload the detail query the user is likely
 * to open next, so navigation paints from cache instead of the network.
 */
export function usePreload(
  query: AnyFinalQuery | null | undefined,
  options?: UsePreloadOptions
): void {
  const db = useDb();
  const enabled = options?.enabled ?? true;
  const identity = enabled && query ? query.hash : null;
  const signal = options?.signal;
  useEffect(() => {
    if (identity === null || !query) return;
    void db
      .getSp00ky()
      .preload(query, { signal })
      .catch(() => {});
  }, [db, identity]);
}
