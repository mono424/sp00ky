import { untrack } from 'svelte';
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
import { SyncedDb } from './db.js';
import { read, toError, useDb } from './context.js';
import type { MaybeGetter } from './types.js';

// ---------------------------------------------------------------------------
// Pure part: structural sharing between emissions.
// NOTE: keep in sync with packages/client-react/src/query.ts (`share`).
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
 * its object identity across reorders, inserts and deletes.
 *
 * That is what makes `$state.raw` fine-grained enough here: a keyed `{#each}`
 * hands an unchanged row the same object, so its block does not update, and a
 * deep-equal emission is the same value, so nothing reading `data` re-runs.
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

// ---------------------------------------------------------------------------
// useQuery
// ---------------------------------------------------------------------------

type AnyFinalQuery = FinalQuery<any, any, any, any, any, Sp00kyQueryResultPromise>;

type BuiltQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
> = FinalQuery<S, TableName, T, RelatedFields, IsOne, Sp00kyQueryResultPromise>;

type QueryArg<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
> = MaybeGetter<BuiltQuery<S, TableName, T, RelatedFields, IsOne> | null | undefined>;

export interface QueryOptions {
  /** Only register while true (default `true`). `false` keeps the last data. */
  enabled?: MaybeGetter<boolean>;
  /**
   * Tear down the query (remote `_00_query` view + local WASM view) when this
   * component is destroyed and no other subscriber remains, instead of keeping
   * it resident for cheap re-subscription. Use for viewport-windowed lists that
   * mount and unmount a query per scroll window. Trade-off: scrolling back to
   * a torn-down window re-registers it.
   */
  deregisterOnCleanup?: boolean;
}

/**
 * A live query. Every field is a reactive getter: read it as `posts.data`,
 * `posts.isLoading` in markup, `$derived` or `$effect`. Do not destructure it,
 * a destructured field is a one-time copy.
 */
export interface QueryState<TData> {
  /**
   * The result. `[]` / `null` before the first emission, then each live
   * emission with structural sharing: unchanged rows keep their object
   * identity, so a keyed `{#each}` only updates the rows that changed.
   */
  readonly data: TData;
  /** Registration or sync error (e.g. SSP 503 during bootstrap). Never thrown. */
  readonly error: Error | undefined;
  /**
   * True only on a COLD first load: nothing painted for this identity, the
   * server has not answered, and registration has not failed. A paint from the
   * local cache ends it, and so does the server answering with zero rows.
   */
  readonly isLoading: boolean;
  /** A fetch cycle is in flight (registration, a sync round). */
  readonly isFetching: boolean;
  /**
   * `data` holds the rendered rows of a known membership for this identity:
   * the server's answer, or the membership an earlier session left on this
   * device. From here `data` is an answer, not a cache guess.
   */
  readonly isAuthoritative: boolean;
  /** `data` holds something: rows for a list, a row for `.one()`. */
  readonly hasData: boolean;
  /** Authoritative AND no data: the server said "no rows". Render empty states on this. */
  readonly isEmpty: boolean;
  /**
   * Authoritative AND idle. The gate for trusting a short window as the end of
   * a list, and for "create it if it is missing". Not monotonic: a later sync
   * round re-enters `isFetching`.
   */
  readonly isSettled: boolean;
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
 * Live query. Pass a getter that builds the query, so it follows the state it
 * reads (`() => db.query('post').where({ author: userId }).build()`); return
 * `null` / `undefined` to skip. The query is re-registered only when its
 * identity hash (surql + vars) changes, not on every rebuild.
 *
 * Call it while a component initialises (it reads context and owns effects
 * that end with the component), or pass the client first:
 * `useQuery(db, () => ..., options)`.
 *
 * ```svelte
 * <script lang="ts">
 *   const db = useDb<typeof schema>();
 *   const posts = useQuery(() => db.query('post').orderBy('created_at', 'desc').limit(20).build());
 * </script>
 *
 * {#each posts.data as post (String(post.id))}<p>{post.title}</p>{/each}
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
): QueryState<TData>;
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
): QueryState<TData>;
export function useQuery(
  dbOrQuery: SyncedDb<any> | MaybeGetter<AnyFinalQuery | null | undefined>,
  queryOrOptions?: MaybeGetter<AnyFinalQuery | null | undefined> | QueryOptions,
  maybeOptions?: QueryOptions
): QueryState<unknown> {
  const explicit = dbOrQuery instanceof SyncedDb ? dbOrQuery : null;
  const db = explicit ?? useDb();
  const queryArg = (explicit ? queryOrOptions : dbOrQuery) as MaybeGetter<
    AnyFinalQuery | null | undefined
  >;
  const options = (explicit ? maybeOptions : queryOrOptions) as QueryOptions | undefined;
  const sp00ky = db.getSp00ky();

  // Rows are plain objects (`$state.raw`, never deep proxies): they reach
  // `db.update(...)`, workers and third-party code as the engine produced them.
  let data = $state.raw<unknown>(null);
  let error = $state.raw<Error | undefined>(undefined);
  // Server membership known and painted for the current identity (core authority).
  let isAuthoritative = $state(false);
  // An emission WITH data was applied for the current identity. With
  // `isAuthoritative` this ends the cold load: a cached paint ends it without
  // the server, a server answer ends it without rows.
  let hasPainted = $state(false);
  let isFetching = $state(false);
  // Registered hash of the attached query, for opt-in deregistration.
  let activeHash: string | undefined;

  // The built query (a new object whenever what the getter reads changes)…
  const resolved = $derived.by(() => {
    const enabled = read(options?.enabled ?? true);
    return enabled ? (read(queryArg) ?? null) : null;
  });
  // …and its identity. Primitives, so the attach effect below only re-runs
  // when the query really changed, not on every rebuild of the same one.
  const identity = $derived(resolved ? String(resolved.hash) : null);
  const isOne = $derived(resolved?.isOne);

  $effect(() => {
    const id = identity;
    // A new identity starts clean: authority, the paint latch and a previous
    // identity's error reset. `data` stays until the new identity emits, so
    // growing a `.limit()` does not flash an empty list.
    untrack(() => {
      isAuthoritative = false;
      hasPainted = false;
      isFetching = false;
      error = undefined;
    });
    if (id === null) return;
    const query = untrack(() => resolved);
    if (!query) return;

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

    // Registration can fail, canonically the SSP answering 503 NOT_READY
    // while it bootstraps. That surfaces as `error`; the sync scheduler retries
    // the registration underneath, so a transient failure still recovers.
    query.run().then(
      ({ hash }: { hash: string }) => {
        if (disposed) return;
        activeHash = hash;
        addCleanup(
          sp00ky.subscribeQueryStatus(
            hash,
            (status) => {
              if (!disposed) isFetching = status === 'fetching';
            },
            { immediate: true }
          )
        );
        addCleanup(
          sp00ky.subscribe(
            hash,
            (rows: Record<string, unknown>[]) => {
              if (disposed) return;
              const t0 = performance.now();
              // Every emission is applied as-is: the core only emits what it
              // decided the query holds. One WITH data ends the cold load.
              const next = query.isOne ? (rows[0] ?? null) : rows;
              data = share(
                untrack(() => data),
                next
              );
              if (query.isOne ? next !== null && next !== undefined : rows.length > 0) {
                hasPainted = true;
              }
              sp00ky.reportFrontendTiming(hash, performance.now() - t0);
            },
            { immediate: true }
          )
        );
        // Subscribed after the rows, so the immediate deliveries keep the
        // core's order: authority flips only after the rows it covers. This is
        // the channel that ends a cold load whose honest answer is zero rows.
        // A flip back to false is a bucket switch (another principal): what is
        // painted is the previous one's, so the paint latch goes with it.
        addCleanup(
          sp00ky.subscribeQueryAuthority(
            hash,
            (known) => {
              if (disposed) return;
              isAuthoritative = known;
              if (!known) hasPainted = false;
            },
            { immediate: true }
          )
        );
      },
      (err: unknown) => {
        if (!disposed) error = toError(err);
      }
    );

    return () => {
      disposed = true;
      for (const c of cleanups) c();
    };
  });

  if (options?.deregisterOnCleanup) {
    // Declared after the attach effect, so on destroy its teardown runs second:
    // this hook's callback is already gone and deregisterQuery's refcount
    // guard sees the true remaining-subscriber count.
    $effect(() => () => {
      if (activeHash) sp00ky.deregisterQuery(activeHash);
    });
  }

  const current = () => {
    const v = data;
    if (isOne === false && !Array.isArray(v)) return EMPTY_LIST;
    if (isOne === true && Array.isArray(v)) return null;
    return v;
  };
  const hasData = () => {
    const v = current();
    return Array.isArray(v) ? v.length > 0 : v !== null && v !== undefined;
  };

  return {
    get data() {
      return current();
    },
    get error() {
      return error;
    },
    get isLoading() {
      return !isAuthoritative && !hasPainted && error === undefined;
    },
    get isFetching() {
      return isFetching;
    },
    get isAuthoritative() {
      return isAuthoritative;
    },
    get hasData() {
      return hasData();
    },
    get isEmpty() {
      return isAuthoritative && !hasData();
    },
    get isSettled() {
      return isAuthoritative && !isFetching;
    },
  };
}

export interface PreloadOptions extends CorePreloadOptions {
  /** Only preload while true (default `true`). */
  enabled?: MaybeGetter<boolean>;
}

/**
 * Fire-and-forget prewarm: registers the query via `db.preload` (a live query
 * nobody subscribes to, evicted a ttl after registration unless a view mounts
 * it first) whenever its identity changes. Nothing to clean up.
 *
 * Typical use: inside a list row, preload the detail query the user is likely
 * to open next, so navigation paints from cache instead of the network.
 */
export function usePreload(
  query: MaybeGetter<AnyFinalQuery | null | undefined>,
  options?: PreloadOptions
): void;
export function usePreload(
  db: SyncedDb<any>,
  query: MaybeGetter<AnyFinalQuery | null | undefined>,
  options?: PreloadOptions
): void;
export function usePreload(
  dbOrQuery: SyncedDb<any> | MaybeGetter<AnyFinalQuery | null | undefined>,
  queryOrOptions?: MaybeGetter<AnyFinalQuery | null | undefined> | PreloadOptions,
  maybeOptions?: PreloadOptions
): void {
  const explicit = dbOrQuery instanceof SyncedDb ? dbOrQuery : null;
  const db = explicit ?? useDb();
  const queryArg = (explicit ? queryOrOptions : dbOrQuery) as MaybeGetter<
    AnyFinalQuery | null | undefined
  >;
  const options = (explicit ? maybeOptions : queryOrOptions) as PreloadOptions | undefined;

  const resolved = $derived.by(() =>
    read(options?.enabled ?? true) ? (read(queryArg) ?? null) : null
  );
  const identity = $derived(resolved ? String(resolved.hash) : null);

  $effect(() => {
    if (identity === null) return;
    const query = untrack(() => resolved);
    if (!query) return;
    void db
      .getSp00ky()
      .preload(query, { signal: options?.signal })
      .catch(() => {});
  });
}
