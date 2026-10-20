import type {
  ColumnSchema,
  FinalQuery,
  SchemaStructure,
  TableNames,
  QueryResult,
} from '@spooky-sync/query-builder';
import {
  createEffect,
  createMemo,
  createSignal,
  createStore,
  onCleanup,
  type Accessor,
} from 'solid-js';
import { SyncedDb } from '..';
import type { Sp00kyQueryResultPromise } from '@spooky-sync/core';
import { useDb } from './context';
import { mergeRows } from './merge-rows';

type QueryArg<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
> =
  | FinalQuery<S, TableName, T, RelatedFields, IsOne, Sp00kyQueryResultPromise>
  | (() =>
      | FinalQuery<S, TableName, T, RelatedFields, IsOne, Sp00kyQueryResultPromise>
      | null
      | undefined);

export type QueryOptions = {
  enabled?: () => boolean;
  /**
   * Tear down the query (remote `_00_query` view + local WASM view) when this
   * hook is disposed and no other subscriber remains, instead of keeping it
   * resident for cheap re-subscription. Use for viewport-windowed lists that
   * mount/unmount a query per scroll window and want off-screen windows
   * cancelled. Trade-off: scrolling back to a torn-down window re-registers it.
   */
  deregisterOnCleanup?: boolean;
};

export type CreateQueryResult<TData> = {
  /**
   * Reactive result. Never suspends and never throws: born as an empty
   * committed value (`[]` / `null`) and reconciled in place (keyed by `id`) on
   * every live emission — unchanged rows keep identity, and coarse readers
   * (`<For>`) are notified on add/remove/reorder.
   */
  data: Accessor<TData>;
  /**
   * Suspending read of the same result for `<Loading>` users: throws Solid's
   * not-ready protocol until the query has delivered its first real result
   * (or errored, in which case it returns the empty value and `error()` is
   * set). Read this inside a `<Loading>` boundary.
   */
  ready: Accessor<TData>;
  error: Accessor<Error | undefined>;
  /**
   * True only on a COLD first load: nothing has painted for this identity,
   * the server has not answered yet, and registration has not failed. A query
   * that painted from the local cache is not loading, however long the server
   * takes; a query the server answered with zero rows is not loading either
   * (see `isEmpty`). Resets when the query identity changes.
   */
  isLoading: Accessor<boolean>;
  /** True while a fetch cycle is in flight (registration, a sync round). */
  isFetching: Accessor<boolean>;
  /**
   * True once `data()` holds the rendered rows of a known membership for
   * this identity: the server's answer (once its rows are local), or the
   * membership an earlier session left on this device. Knowing the set is
   * not enough: right after a sign-in re-seed, on boot, or while a server
   * answer's rows still download, the set is known but `data()` is not
   * painted yet, and this stays false. From then on `data()` is an answer, not
   * a cache guess. Latched until the identity changes (or a bucket switch
   * resets it).
   */
  isAuthoritative: Accessor<boolean>;
  /** Whether `data()` currently holds anything: rows for a list, a row for `one()`. */
  hasData: Accessor<boolean>;
  /**
   * `isAuthoritative()` and the answer is "no rows". The signal to render an
   * empty state on; a bare `!hasData()` cannot tell empty from not-yet-loaded.
   */
  isEmpty: Accessor<boolean>;
  /**
   * True once `isAuthoritative()` AND no fetch cycle is in flight. While
   * settled, `data()` is the answer: a windowed query returning fewer rows
   * than its LIMIT really is the end of the list, and a missing row is
   * missing. The registration that re-confirms a cached answer after a reload
   * or sign-in is a fetch cycle, so while it runs this waits for the server;
   * with nothing in flight (offline) a cached answer settles. Not monotonic: a
   * later sync round re-enters `isFetching`; latch it if a verdict must stick.
   */
  isSettled: Accessor<boolean>;
};

// Overload: context-based (no explicit db)
export function createQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
  TData = QueryResult<S, TableName, RelatedFields, IsOne> | null,
>(
  finalQuery: QueryArg<S, TableName, T, RelatedFields, IsOne>,
  options?: QueryOptions
): CreateQueryResult<TData>;

// Overload: explicit db
export function createQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
  TData = QueryResult<S, TableName, RelatedFields, IsOne> | null,
>(
  db: SyncedDb<S>,
  finalQuery: QueryArg<S, TableName, T, RelatedFields, IsOne>,
  options?: QueryOptions
): CreateQueryResult<TData>;

// Implementation
export function createQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends {
    columns: Record<string, ColumnSchema>;
  },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
  TData = QueryResult<S, TableName, RelatedFields, IsOne> | null,
>(
  dbOrQuery: SyncedDb<S> | QueryArg<S, TableName, T, RelatedFields, IsOne>,
  queryOrOptions?: QueryArg<S, TableName, T, RelatedFields, IsOne> | QueryOptions,
  maybeOptions?: QueryOptions
): CreateQueryResult<TData> {
  let db: SyncedDb<S>;
  let finalQuery: QueryArg<S, TableName, T, RelatedFields, IsOne>;
  let options: QueryOptions | undefined;

  if (dbOrQuery instanceof SyncedDb) {
    db = dbOrQuery;
    finalQuery = queryOrOptions as QueryArg<S, TableName, T, RelatedFields, IsOne>;
    options = maybeOptions;
  } else {
    db = useDb<S>();
    finalQuery = dbOrQuery;
    options = queryOrOptions as QueryOptions | undefined;
  }

  const sp00ky = db.getSp00ky();

  // Status channel. Written from subscription callbacks and generator
  // continuations, which run outside any tracking scope — `ownedWrite` opts
  // these signals out of Solid 2's owned-scope write guard.
  const [error, setError] = createSignal<Error | undefined>(undefined, { ownedWrite: true });
  // Server membership known for the current identity (core `membershipKnown`).
  const [isAuthoritative, setIsAuthoritative] = createSignal(false, { ownedWrite: true });
  // An emission WITH data has been applied for the current identity. Together
  // with `isAuthoritative` this is what ends the cold-load state: a cached
  // paint ends it without the server, a server answer ends it without rows.
  const [hasPainted, setHasPainted] = createSignal(false, { ownedWrite: true });
  const [isFetching, setIsFetching] = createSignal(false, { ownedWrite: true });

  // The hash of the currently-installed subscription, for opt-in deregister on
  // dispose (see `deregisterOnCleanup`).
  let activeHash: string | undefined;

  // Results live in a plain store, written from the engine's subscription
  // callback and reconciled in place keyed by `id`, so unchanged rows keep
  // identity and coarse readers (`<For>`) are notified on add/remove/reorder.
  //
  // NOT an async-generator projection. A live query's generator never returns:
  // it awaits the next emission forever, which leaves its node permanently
  // PENDING. Solid 2 holds a navigation transition until every pending node in
  // the new tree settles, so with a generator here the first client-side
  // navigation into a screen that opens a query never commits: the URL and
  // effects update while the old DOM stays on screen, with no error and no
  // <Loading> fallback that can rescue it. Subscription writes settle
  // immediately, which is what the Solid 1 binding did too.
  //
  // Wrapped in an object so `one()` queries (row object or null) and list
  // queries share one store shape; `mergeRows` keys `value`'s contents by id.
  const [store, setStore] = createStore<{ value: TData }>({ value: null as TData });

  // Identity of the installed subscription, so a superseded run cannot write
  // results for a query the caller has already moved off.
  let runId = 0;

  // Woken by the first result or error, for the suspending `ready()` read.
  let readyWaiters: (() => void)[] = [];
  const wakeReady = () => {
    const waiters = readyWaiters;
    readyWaiters = [];
    for (const w of waiters) w();
  };

  // Tracked: the query identity and the `enabled` gate. Untracked apply: the
  // registration + subscription, whose teardown is the returned cleanup.
  createEffect(
    () => {
      const enabled = options?.enabled?.() ?? true;
      const query = typeof finalQuery === 'function' ? finalQuery() : finalQuery;
      return { enabled, query };
    },
    ({ enabled, query }) => {
      const myRun = ++runId;
      // A new identity starts clean: a previous identity's failure must not
      // keep this one out of its loading state.
      setIsAuthoritative(false);
      setHasPainted(false);
      setError(undefined);
      if (!enabled || !query) return;

      const cleanups: (() => void)[] = [];
      let disposed = false;
      // `sp00ky.subscribe` resolves its unsubscribe asynchronously; the status
      // subscription returns one directly. Accept both, and if teardown already
      // happened while the promise was in flight, unsubscribe immediately.
      const addCleanup = (c: (() => void) | Promise<(() => void) | undefined> | undefined) => {
        if (!c) return;
        if (typeof c === 'function') {
          if (disposed) c();
          else cleanups.push(c);
          return;
        }
        void Promise.resolve(c).then((fn) => {
          if (typeof fn !== 'function') return;
          if (disposed) fn();
          else cleanups.push(fn);
        });
      };

      /**
       * Registration can fail — the canonical case is the SSP answering 503
       * NOT_READY while it bootstraps. Surface it as `error()` instead of
       * throwing into the graph: the sync scheduler retries the registration
       * underneath, so a transient failure still recovers, and a spinner
       * driven by `isLoading()` resolves via `error()`.
       */
      query
        .run()
        .then(({ hash }: { hash: string }) => {
          if (disposed || myRun !== runId) return;
          activeHash = hash;

          // Mirror the query's fetch status so the UI can show a "loading
          // more" state while the sync engine pulls records in the background.
          addCleanup(
            sp00ky.subscribeQueryStatus(hash, (status) => setIsFetching(status === 'fetching'), {
              immediate: true,
            })
          );

          addCleanup(
            sp00ky.subscribe(
              hash,
              (rows: Record<string, any>[]) => {
                if (disposed || myRun !== runId) return;
                const queryData = (query.isOne ? (rows[0] ?? null) : rows) as TData;
                // Every emission is applied as-is: the core only emits what
                // it has decided the query holds (cached rows on mount, the
                // server's set once membership is known), and it never emits
                // a wrongful `[]` for a lost or still-publishing view. An
                // emission WITH data ends the cold-load state on its own.
                const hasRows = query.isOne
                  ? queryData !== null && queryData !== undefined
                  : rows.length > 0;
                if (hasRows) {
                  setHasPainted(true);
                  wakeReady();
                }

                const t0 = performance.now();
                setStore((s) => {
                  if (query.isOne || queryData === null || !Array.isArray(s.value)) {
                    s.value = queryData;
                  } else {
                    mergeRows(s.value as any[], queryData as any[]);
                  }
                });
                sp00ky.reportFrontendTiming(hash, performance.now() - t0);
              },
              { immediate: true }
            )
          );

          // Mirror whether the rows of a known membership are painted. This
          // is the channel that ends a cold load for a query whose honest
          // answer is zero rows: the core emits no second `[]` for it, but it
          // does flip authority once the render of the known set ran, always
          // after the rows that render emitted, so `data()` is current when
          // this reads true (subscribed after the rows so the immediate
          // deliveries keep that order too). A flip back to false is a bucket
          // switch (a different principal): what is on screen is the previous
          // one's until the new store's render replaces it, so the paint
          // latch goes with it.
          addCleanup(
            sp00ky.subscribeQueryAuthority(
              hash,
              (known) => {
                if (disposed || myRun !== runId) return;
                setIsAuthoritative(known);
                if (known) wakeReady();
                else setHasPainted(false);
              },
              { immediate: true }
            )
          );
        })
        .catch((err: unknown) => {
          if (disposed || myRun !== runId) return;
          setError(err instanceof Error ? err : new Error(String(err)));
          wakeReady();
        });

      return () => {
        disposed = true;
        for (const c of cleanups) c();
      };
    }
  );

  // Fallback empty value served before the first emission of a list query.
  const emptyList = [] as unknown as TData;

  const data: Accessor<TData> = () => {
    const v = store.value;
    if (v === null || v === undefined) {
      const query = typeof finalQuery === 'function' ? finalQuery() : finalQuery;
      if (query && !query.isOne) return emptyList;
    }
    return v as TData;
  };

  // Suspending read: pends until the first real result (or error). Reading it
  // inside <Loading> integrates with Solid 2's boundary protocol; `data` stays
  // non-throwing.
  //
  // `lazy` matters, and so does the promise actually resolving: an unresolved
  // async node stays PENDING, every queue flush carries pending nodes forward,
  // and a navigation transition waits on them. Lazy means only a query whose
  // `ready()` is read creates the node at all, and the deferred below is
  // settled by the same emission (or authority flip) that ends `isLoading`.
  const readyGate = createMemo(
    async (): Promise<true> => {
      if (isAuthoritative() || hasPainted() || error()) return true;
      await new Promise<void>((resolve) => readyWaiters.push(resolve));
      return true;
    },
    { lazy: true }
  );
  const ready: Accessor<TData> = () => {
    readyGate();
    return data();
  };

  // Tear down the live subscription when the hook's owner is disposed. The
  // projection's own onCleanup (inside the compute) already unsubscribes; this
  // hook-scope cleanup only handles the opt-in query deregistration.
  onCleanup(() => {
    // Opt-in: cancel the query once this hook (its last subscriber) is gone.
    // The compute's cleanup removed this hook's callback, so deregisterQuery's
    // refcount guard sees the true remaining-subscriber count.
    if (options?.deregisterOnCleanup && activeHash) {
      sp00ky.deregisterQuery(activeHash);
    }
  });

  const hasData = () => {
    const v = store.value as unknown;
    return Array.isArray(v) ? v.length > 0 : v !== null && v !== undefined;
  };
  const isLoading = () => !isAuthoritative() && !hasPainted() && error() === undefined;
  const isEmpty = () => isAuthoritative() && !hasData();
  const isSettled = () => isAuthoritative() && !isFetching();

  return {
    data,
    ready,
    error,
    isLoading,
    isFetching,
    isAuthoritative,
    hasData,
    isEmpty,
    isSettled,
  };
}

/** @deprecated Renamed `createQuery` in the Solid 2 binding. */
export const useQuery = createQuery;
