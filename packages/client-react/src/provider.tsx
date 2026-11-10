import {
  createContext,
  useContext,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { SyncedDbConfig } from './types';
import { SyncedDb } from './db';

/** `useLayoutEffect` in the browser, `useEffect` on the server (no warning, same order). */
export const useIsomorphicLayoutEffect =
  typeof window !== 'undefined' ? useLayoutEffect : useEffect;

/** Ref that always holds the latest value, for callbacks read from async continuations. */
export function useLatest<T>(value: T): { readonly current: T } {
  const ref = useRef(value);
  useIsomorphicLayoutEffect(() => {
    ref.current = value;
  });
  return ref;
}

const Sp00kyContext = createContext<SyncedDb<any> | null>(null);

/** The provider's `SyncedDb`, or `null` outside one. */
export function useOptionalDb(): SyncedDb<any> | null {
  return useContext(Sp00kyContext);
}

/**
 * The `SyncedDb` the nearest `<Sp00kyProvider>` initialized. Throws outside a
 * provider; every hook in this package goes through it.
 */
export function useDb<S extends SchemaStructure>(): SyncedDb<S> {
  const db = useContext(Sp00kyContext);
  if (!db) {
    throw new Error(
      'useDb must be used within a <Sp00kyProvider>. Wrap your app in <Sp00kyProvider config={...}>.'
    );
  }
  return db as SyncedDb<S>;
}

export interface Sp00kyProviderProps<S extends SchemaStructure> {
  /**
   * Client config. Read ONCE, when the provider mounts: changing it later does
   * not rebuild the client. To switch databases, remount the provider with a
   * new `key`.
   */
  config: SyncedDbConfig<S>;
  /** Rendered until the client is initialized (and `preload` resolved). */
  fallback?: ReactNode;
  onError?: (error: Error) => void;
  onReady?: (db: SyncedDb<S>) => void;
  /**
   * Prewarm data into the local cache before revealing the UI. Runs after
   * `init()`; the `fallback` stays visible until it resolves. Use awaitable
   * `db.preload(...)` calls here to gate first-load on essential data (e.g.
   * config). On warm loads preload returns instantly. Best-effort: a rejection
   * is caught and the UI is revealed anyway.
   */
  preload?: (db: SyncedDb<S>) => Promise<void>;
  children?: ReactNode;
}

/** One client per mounted provider, kept across StrictMode's simulated remount. */
interface Boot<S extends SchemaStructure> {
  instance: SyncedDb<S>;
  init: Promise<void>;
  /** Handed to the tree: from here on it is never closed (see the cleanup below). */
  revealed: boolean;
  disposeTimer?: ReturnType<typeof setTimeout>;
}

/**
 * Initializes a sp00ky client and provides it to `useDb()` and every hook
 * below. Renders `fallback` until the client is ready.
 *
 * The client is created in an effect, never during render, so a server render
 * only produces the fallback and StrictMode's double render creates nothing.
 */
export function Sp00kyProvider<S extends SchemaStructure>(props: Sp00kyProviderProps<S>) {
  const [db, setDb] = useState<SyncedDb<S> | null>(null);
  const boot = useRef<Boot<S> | null>(null);
  const callbacks = useLatest({
    onError: props.onError,
    onReady: props.onReady,
    preload: props.preload,
  });
  // Read once, at mount: see the `config` prop docs.
  const config = useRef(props.config).current;

  useEffect(() => {
    let active = true;
    let current = boot.current;
    if (current?.disposeTimer !== undefined) {
      // StrictMode re-ran this effect right after its cleanup: keep the client.
      clearTimeout(current.disposeTimer);
      current.disposeTimer = undefined;
    }
    if (!current) {
      const instance = new SyncedDb<S>(config);
      current = boot.current = { instance, init: instance.init(), revealed: false };
    }
    const b = current;

    b.init.then(
      async () => {
        if (!active) return;
        // Gate first-load UI on prewarmed data. Best-effort: never let a
        // preload failure keep the app stuck on the fallback.
        const preload = callbacks.current.preload;
        if (preload && !b.revealed) {
          try {
            await preload(b.instance);
          } catch (e) {
            // oxlint-disable-next-line no-console
            console.error('Sp00kyProvider: preload failed; revealing UI anyway', e);
          }
        }
        if (!active) return;
        const first = !b.revealed;
        b.revealed = true;
        setDb(b.instance);
        if (first) callbacks.current.onReady?.(b.instance);
      },
      (e: unknown) => {
        if (!active) return;
        const error = e instanceof Error ? e : new Error(String(e));
        if (callbacks.current.onError) {
          callbacks.current.onError(error);
        } else {
          // oxlint-disable-next-line no-console
          console.error('Sp00kyProvider: Failed to initialize database', error);
        }
      }
    );

    return () => {
      active = false;
      // StrictMode runs this cleanup and the effect again in the same commit,
      // so wait a task before deciding the provider is really gone. Only an
      // instance that was never revealed is closed then (init raced the
      // unmount). A revealed client is deliberately NOT closed: that nulls the
      // core client, so any write still in flight from the old tree (an
      // unmount handler, a pending submit) would throw "SyncedDb not
      // initialized" while the UI looks fine. The provider normally wraps the
      // whole app and only unmounts with the page, where the browser reclaims
      // the client anyway.
      b.disposeTimer = setTimeout(() => {
        b.disposeTimer = undefined;
        if (boot.current !== b) return;
        boot.current = null;
        if (!b.revealed) {
          b.init.then(
            () => b.instance.close(),
            () => {}
          );
        }
      }, 0);
    };
    // Mount-only by design: config and callbacks are read through refs.
  }, []);

  if (!db) return <>{props.fallback ?? null}</>;
  return <Sp00kyContext.Provider value={db}>{props.children}</Sp00kyContext.Provider>;
}
