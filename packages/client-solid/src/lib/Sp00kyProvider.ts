import type { JSX } from 'solid-js';
import {
  createSignal,
  onMount,
  onCleanup,
  createComponent,
  createMemo,
  mergeProps,
} from 'solid-js';
import type { SchemaStructure } from '@spooky/query-builder';
import type { SyncedDbConfig } from '../types';
import { SyncedDb } from '../index';
import { Sp00kyContext } from './context';

export interface Sp00kyProviderProps<S extends SchemaStructure> {
  config: SyncedDbConfig<S>;
  /** Rendered until the client is initialized (and `preload` resolved). Not rendered with `eager`. */
  fallback?: JSX.Element;
  /**
   * Render `children` at once, before the client is initialized. With
   * `localEngine: 'rows'`, `useQuery` then renders the rows a query showed the
   * last time the app was open, on the very first render; the query itself
   * starts as soon as the client is ready and corrects them.
   *
   * What changes for the tree: it mounts on a client that is not open yet.
   * Queries and writes made early wait for it; everything that reports state
   * (auth, feature flags, sync status) reports "not known yet" until then, so
   * gate what a signed-out visitor must not see on `useDb().whenReady()` or
   * `db.isReady`. `preload` no longer holds the UI back, and `fallback` is
   * never shown. Read once, when the provider is created. In a server render
   * the provider still renders the fallback: this is for client-rendered
   * trees.
   */
  eager?: boolean;
  onError?: (error: Error) => void;
  onReady?: (db: SyncedDb<S>) => void;
  /**
   * Prewarm data into the local cache before revealing the UI. Runs after
   * `init()`; the `fallback` stays visible until it resolves. Use awaitable
   * `db.preload(...)` calls here to gate first-load on essential data (e.g.
   * config). On warm loads preload returns instantly, so there's no perceptible
   * gate after the first run. Best-effort: a rejection is caught and the UI is
   * revealed anyway.
   */
  preload?: (db: SyncedDb<S>) => Promise<void>;
  children: JSX.Element;
}

export function Sp00kyProvider<S extends SchemaStructure>(
  props: Sp00kyProviderProps<S>
): JSX.Element {
  const merged = mergeProps(
    {
      fallback: undefined as JSX.Element | undefined,
    },
    props
  );

  // Eager: the instance the tree mounts on, from the first render. Building it
  // opens nothing (its core client comes with its first use).
  const early =
    merged.eager && typeof window !== 'undefined'
      ? new SyncedDb<S>(merged.config, { eager: true })
      : undefined;

  const [db, setDb] = createSignal<SyncedDb<S> | undefined>(early);

  // `onMount` is async, so a dispose can land mid-init. Only that narrow race is
  // handled here: an instance whose init finished AFTER the provider was already
  // gone is closed, because nothing will ever reference it.
  //
  // A live, mounted client is deliberately NOT closed on cleanup. Doing that
  // nulls `SyncedDb.sp00ky`, so every later `create`/`update`/`delete` throws
  // "SyncedDb not initialized" while reads keep rendering from state that is
  // already subscribed — i.e. mutations die silently and the app looks fine. In
  // a host app the provider wraps the whole tree and only unmounts with the
  // page, where the browser reclaims the worker anyway, so the leak this was
  // meant to fix is worth far less than that risk.
  let disposed = false;

  onCleanup(() => {
    disposed = true;
  });

  onMount(async () => {
    try {
      const instance = early ?? new SyncedDb<S>(merged.config);
      await instance.init();
      if (disposed) {
        // An eager instance was in the tree's hands before it was initialized,
        // so it is a mounted client like any other: not closed (see above).
        if (!early) await instance.close();
        return;
      }
      // Gate first-load UI on prewarmed data. Best-effort: never let a preload
      // failure keep the app stuck on the fallback. With `eager` the UI is
      // already up, so this only warms the cache.
      if (merged.preload) {
        try {
          await merged.preload(instance);
        } catch (e) {
          // oxlint-disable-next-line no-console
          console.error('Sp00kyProvider: preload failed; revealing UI anyway', e);
        }
      }
      setDb(() => instance);
      merged.onReady?.(instance);
    } catch (e) {
      const error = e instanceof Error ? e : new Error(String(e));
      if (merged.onError) {
        merged.onError(error);
      } else {
        // oxlint-disable-next-line no-console
        console.error('Sp00kyProvider: Failed to initialize database', error);
      }
    }
  });

  const content = createMemo(() => {
    const instance = db();
    if (!instance) return merged.fallback;
    return createComponent(Sp00kyContext.Provider, {
      value: instance,
      get children() {
        return merged.children;
      },
    });
  });

  return content as unknown as JSX.Element;
}
