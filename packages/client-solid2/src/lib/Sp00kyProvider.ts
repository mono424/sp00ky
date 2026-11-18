import type { Element } from 'solid-js';
import {
  createSignal,
  onSettled,
  onCleanup,
  createComponent,
  createMemo,
  merge,
  untrack,
} from 'solid-js';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { SyncedDbConfig } from '../types';
import { SyncedDb } from '../index';
import { Sp00kyContext } from './context';

export interface Sp00kyProviderProps<S extends SchemaStructure> {
  config: SyncedDbConfig<S>;
  /** Shown until the client is initialized (and `preload` resolved). Never shown with `eager`. */
  fallback?: Element;
  /**
   * Render `children` at once, before the client is initialized. With
   * `localEngine: 'rows'`, `createQuery` then returns, on its very first read,
   * the rows a query showed the last time the app was open; the query itself
   * starts as soon as the client is ready and corrects them.
   *
   * What changes for the tree: it mounts on a client that is not open yet.
   * Queries and writes made early wait for it; everything that reports state
   * (auth, feature flags, sync status) reports "not known yet" until then, so
   * gate what a signed-out visitor must not see on `useDb().whenReady()` or
   * `db.isReady`. `preload` no longer holds the UI back, and `fallback` is
   * never shown. Read once, when the provider is created. In a server render
   * the provider still renders the fallback: this is for client-rendered trees.
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
   * revealed anyway. With `eager` it still runs after `init()`, but the UI is
   * on screen already.
   */
  preload?: (db: SyncedDb<S>) => Promise<void>;
  children: Element;
}

export function Sp00kyProvider<S extends SchemaStructure>(
  props: Sp00kyProviderProps<S>
): Element {
  const merged = merge({ fallback: undefined as Element | undefined }, props);

  // Eager: the instance the tree mounts on, from the first render. Building it
  // opens nothing (its core client comes with its first use); `init()` still
  // runs below. Browser only, so a server render keeps the fallback.
  const early = untrack(() =>
    merged.eager && typeof window !== 'undefined'
      ? new SyncedDb<S>(merged.config, { eager: true })
      : undefined
  );

  // Written from the async init continuation — outside any tracking scope.
  const [db, setDb] = createSignal<SyncedDb<S> | undefined>(early, { ownedWrite: true });

  // Init is async, so a dispose can land mid-init. Only that narrow race is
  // handled here: an instance whose init finished AFTER the provider was
  // already gone is closed, because nothing will ever reference it.
  //
  // A live, mounted client is deliberately NOT closed on cleanup. Doing that
  // nulls `SyncedDb.sp00ky`, so every later `create`/`update`/`delete` throws
  // "SyncedDb not initialized" while reads keep rendering from state that is
  // already subscribed — i.e. mutations die silently and the app looks fine. In
  // a host app the provider wraps the whole tree and only unmounts with the
  // page, where the browser reclaims the worker anyway, so the leak this was
  // meant to fix is worth far less than that risk. An eager instance is in the
  // tree's hands before it is initialized, so it is never closed either.
  let disposed = false;

  onCleanup(() => {
    disposed = true;
  });

  const start = () => {
    void (async () => {
      try {
        const instance = early ?? new SyncedDb<S>(merged.config);
        await instance.init();
        if (disposed) {
          if (!early) await instance.close();
          return;
        }
        // Gate first-load UI on prewarmed data. Best-effort: never let a
        // preload failure keep the app stuck on the fallback.
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
    })();
  };
  // Eager: start now. `onSettled` waits for the first render's pending async
  // reads, and in an eager tree such a read may itself be waiting for the
  // client (`db.whenReady()`): held back until it settled, the client would
  // never open. Otherwise `onSettled`, which replaces Solid 1's `onMount`.
  if (early) start();
  else onSettled(start);

  const content = createMemo(() => {
    const instance = db();
    if (!instance) return merged.fallback;
    // Solid 2: the context object IS the provider component.
    return createComponent(Sp00kyContext, {
      value: instance,
      get children() {
        return merged.children;
      },
    });
  });

  return content as unknown as Element;
}
