import type {
  ColumnSchema,
  FinalQuery,
  SchemaStructure,
  TableNames,
  QueryResult,
} from '@spooky-sync/query-builder';
import { createEffect, createSignal, onCleanup, useContext } from 'solid-js';
import { createStore, reconcile } from 'solid-js/store';
import { SyncedDb } from '..';
import type { Sp00kyQueryResultPromise } from '@spooky-sync/core';
import { Sp00kyContext } from './context';

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

type QueryOptions = {
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

// Overload: context-based (no explicit db)
export function useQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
  TData = QueryResult<S, TableName, RelatedFields, IsOne> | null,
>(
  finalQuery: QueryArg<S, TableName, T, RelatedFields, IsOne>,
  options?: QueryOptions,
): {
  data: () => TData | undefined;
  error: () => Error | undefined;
  isLoading: () => boolean;
  isFetching: () => boolean;
  isSettled: () => boolean;
};

// Overload: explicit db (backward-compatible)
export function useQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
  TData = QueryResult<S, TableName, RelatedFields, IsOne> | null,
>(
  db: SyncedDb<S>,
  finalQuery: QueryArg<S, TableName, T, RelatedFields, IsOne>,
  options?: QueryOptions,
): {
  data: () => TData | undefined;
  error: () => Error | undefined;
  isLoading: () => boolean;
  isFetching: () => boolean;
  isSettled: () => boolean;
};

// Implementation
export function useQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends {
    columns: Record<string, ColumnSchema>;
  },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
  TData = QueryResult<S, TableName, RelatedFields, IsOne> | null,
>(
  dbOrQuery:
    | SyncedDb<S>
    | QueryArg<S, TableName, T, RelatedFields, IsOne>,
  queryOrOptions?:
    | QueryArg<S, TableName, T, RelatedFields, IsOne>
    | QueryOptions,
  maybeOptions?: QueryOptions,
) {
  let db: SyncedDb<S>;
  let finalQuery: QueryArg<S, TableName, T, RelatedFields, IsOne>;
  let options: QueryOptions | undefined;

  if (dbOrQuery instanceof SyncedDb) {
    // Explicit db overload: useQuery(db, query, options?)
    db = dbOrQuery;
    finalQuery = queryOrOptions as QueryArg<S, TableName, T, RelatedFields, IsOne>;
    options = maybeOptions;
  } else {
    // Context-based overload: useQuery(query, options?)
    const contextDb = useContext(Sp00kyContext);
    if (!contextDb) {
      throw new Error(
        'useQuery: No db argument provided and no Sp00kyContext found. ' +
        'Either pass a SyncedDb instance or wrap your app in <Sp00kyProvider>.'
      );
    }
    db = contextDb as SyncedDb<S>;
    finalQuery = dbOrQuery;
    options = queryOrOptions as QueryOptions | undefined;
  }

  const [error, setError] = createSignal<Error | undefined>(undefined);
  const [isFetched, setIsFetched] = createSignal(false);
  const [isFetching, setIsFetching] = createSignal(false);
  // Results live in a store (not a signal) so consecutive live-query emissions
  // are merged with `reconcile`: unchanged rows keep their object identity and
  // changed rows are mutated in place. That keeps Solid's reference-keyed `<For>`
  // rows — and any `useQuery` subscriptions mounted inside them — alive across
  // updates, instead of tearing every row down and re-registering its queries.
  const [state, setState] = createStore<{ value: TData | undefined }>({ value: undefined });
  // `reconcile` (below) merges each emission into `state.value` IN PLACE, keeping
  // the array reference stable. That's ideal for granular per-row reactivity, but
  // it means a *coarse* reader of `data()` — `<For each={data()}>`, or an effect
  // that copies the whole array elsewhere (e.g. GameList's windowed store) — is
  // NOT re-run when rows are added/removed/reordered within a same-length result
  // (the classic case: deleting a row in a windowed list shifts the next one in,
  // so length stays 50 and the array ref never changes). Bump a version on every
  // emission and read it in `data()` so every consumer re-runs on any change while
  // reconcile still preserves row identity underneath.
  const [version, setVersion] = createSignal(0);
  const data = () => {
    version();
    return state.value;
  };

  let prevQueryString: string | undefined;
  // Monotonic token for each subscription generation. Bumped whenever the query
  // identity changes or the hook is disposed, so a slow async `initQuery`
  // continuation can detect it was superseded and avoid installing a stale (and
  // leaked) subscription.
  let runId = 0;
  let activeUnsub: (() => void) | undefined;
  // The hash of the currently-installed subscription, for opt-in deregister on
  // dispose (see `deregisterOnCleanup`).
  let activeHash: string | undefined;

  const teardownActive = () => {
    activeUnsub?.();
    activeUnsub = undefined;
  };

  const sp00ky = db.getSp00ky();

  /**
   * Registration can fail — the canonical case is the SSP answering 503
   * NOT_READY while it bootstraps. Nothing here used to catch that: the
   * rejection escaped as an unhandled promise, `isFetched` stayed false, and
   * `isLoading()` therefore stayed true FOREVER, which is what a spinner that
   * never resolves actually was. Surface it as `error()` instead; the sync
   * scheduler retries the registration underneath, so a transient failure
   * still recovers on its own.
   */
  const initQuery = async (
    query: FinalQuery<S, TableName, T, RelatedFields, IsOne, Sp00kyQueryResultPromise>,
    myRun: number
  ) => {
    try {
      await subscribeQuery(query, myRun);
    } catch (err) {
      // A superseded run's failure is not this subscription's problem.
      if (myRun !== runId) return;
      setError(err instanceof Error ? err : new Error(String(err)));
    }
  };

  const subscribeQuery = async (
    query: FinalQuery<S, TableName, T, RelatedFields, IsOne, Sp00kyQueryResultPromise>,
    myRun: number
  ) => {
    const { hash } = await query.run();
    // A newer query identity (or disposal) won the race while we awaited run().
    if (myRun !== runId) return;
    activeHash = hash;
    setError(undefined);

    let isFirstCall = true;
    const unsub = await sp00ky.subscribe(
      hash,
      (e) => {
        const queryData = (query.isOne ? e[0] : e) as TData;
        // Merge into the store by record id: unchanged rows keep their identity,
        // changed rows update in place. Replaces wholesale for `one()`/null.
        // Time the reconcile → report as the "frontend" phase for DevTools/MCP.
        const reconcileStart = performance.now();
        setState('value', reconcile(queryData as any, { key: 'id' }));
        // Notify coarse `data()` readers (see the `version` note above): reconcile
        // keeps the array ref stable, so this is what re-runs `<For>`/copy-effects
        // on add/remove/reorder.
        setVersion((v) => v + 1);
        sp00ky.reportFrontendTiming(hash, performance.now() - reconcileStart);
        // The first (immediate) callback with no data likely means the local DB
        // hasn't synced yet — don't mark as fetched so UI shows loading state
        const hasData = query.isOne ? queryData !== null && queryData !== undefined : (e as any[]).length > 0;
        if (!isFirstCall || hasData) {
          setIsFetched(true);
        }
        isFirstCall = false;
      },
      { immediate: true }
    );

    // A result that is empty and stays empty (a new account's list) produces
    // no second callback above, so `isFetched` also follows the server: once
    // its membership for the query is known and no fetch is in flight, the
    // (possibly empty) result is the answer. Without this the hook sat in
    // `isLoading()` until a reload.
    let known = false;
    let idle = false;
    const settleFromServer = () => {
      if (known && idle && myRun === runId) setIsFetched(true);
    };

    // Mirror the query's fetch status so the UI can show a "loading more"
    // state while the sync engine pulls missing records in the background.
    const unsubStatus = sp00ky.subscribeQueryStatus(
      hash,
      (status) => {
        setIsFetching(status === 'fetching');
        idle = status === 'idle';
        settleFromServer();
      },
      { immediate: true }
    );
    const unsubAuthority = sp00ky.subscribeQueryAuthority(
      hash,
      (k) => {
        known = k;
        settleFromServer();
      },
      { immediate: true }
    );

    const teardown = () => {
      unsub();
      unsubStatus();
      unsubAuthority();
    };

    // Superseded while awaiting subscribe()? Don't leak — tear down immediately.
    if (myRun !== runId) {
      teardown();
      return;
    }
    activeUnsub = teardown;
  };

  createEffect(() => {
    const enabled = options?.enabled?.() ?? true;

    // If disabled, clear error and don't run query
    if (!enabled) {
      setError(undefined);
      return;
    }

    // Init Query
    const query = typeof finalQuery === 'function' ? finalQuery() : finalQuery;
    if (!query) {
      return;
    }

    // Dedup on the query's stable identity hash (cyrb53 of surql + vars), not a
    // full `JSON.stringify` of the FinalQuery (which walks the whole schema +
    // inner query on every reactive tick and isn't guaranteed stable). When the
    // identity is unchanged we keep the existing subscription alive.
    const queryString = String(query.hash);
    if (queryString === prevQueryString) {
      return;
    }
    prevQueryString = queryString;

    // New query identity → supersede the previous subscription and start fresh.
    const myRun = ++runId;
    teardownActive();
    setIsFetched(false);
    // A new identity starts clean: a previous identity's failure must not keep
    // this one out of its loading state.
    setError(undefined);
    void initQuery(query, myRun);
  });

  // Tear down the live subscription when the hook's owner is disposed. Registered
  // on the hook (component) scope rather than inside the effect, so an effect
  // re-run that early-returns (unchanged query) doesn't clean up the still-valid
  // subscription. Bumping runId also invalidates any in-flight initQuery.
  onCleanup(() => {
    runId++;
    teardownActive();
    // Opt-in: cancel the query once this hook (its last subscriber) is gone.
    // teardownActive() above already removed this hook's callback, so
    // deregisterQuery's refcount guard sees the true remaining-subscriber count.
    if (options?.deregisterOnCleanup && activeHash) {
      sp00ky.deregisterQuery(activeHash);
    }
  });

  const isLoading = () => {
    return !isFetched() && error() === undefined;
  };

  // True once the query has delivered a result AND no fetch cycle is in flight
  // (registration + initial sync included — the core holds `fetching` across
  // the whole registration and flushes debounced results before flipping back
  // to idle). While settled, the results are authoritative: a windowed query
  // returning fewer rows than its LIMIT really is the end of the list, so
  // virtualized lists may size themselves to it without the scrollbar jumping
  // when a still-syncing window transiently reports short. Resets to false
  // whenever the query identity changes.
  const isSettled = () => isFetched() && !isFetching();

  return {
    data,
    error,
    isLoading,
    isFetching,
    isSettled,
  };
}
