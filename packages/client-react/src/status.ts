import { useCallback, useEffect, useMemo, useState, useSyncExternalStore } from 'react';
import {
  semverGt,
  type AppReleaseOptions,
  type AppReleaseSnapshot,
  type ConnectionState,
  type FeatureFlagOptions,
  type ImpersonationInfo,
  type StorageHealth,
  type StorageHealthStatus,
  type SyncHealth,
  type SyncHealthStatus,
} from '@spooky-sync/core';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import { SyncedDb } from './db';
import { useDb, useLatest, useOptionalDb } from './provider';

/** Shallow equality for the snapshot objects the engine re-creates on every callback. */
function shallowEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object') return false;
  const ka = Object.keys(a);
  if (ka.length !== Object.keys(b).length) return false;
  return ka.every((k) => Object.is((a as any)[k], (b as any)[k]));
}

/**
 * React view over an engine subscribe-callback API. The engine's subscribe
 * methods fire immediately with the current value, and `read` seeds the
 * first render so nothing flashes before that lands. `deps` identify the
 * source: a change re-reads and re-subscribes.
 */
export function useEngineValue<T>(
  subscribe: (cb: (value: T) => void) => () => void,
  read: () => T,
  deps: readonly unknown[]
): T {
  const source = useMemo(() => {
    let value = read();
    return {
      subscribe: (onChange: () => void) =>
        subscribe((next) => {
          if (shallowEqual(value, next)) return;
          value = next;
          onChange();
        }),
      getSnapshot: () => value,
    };
    // The caller's deps identify the source.
  }, deps);
  return useSyncExternalStore(source.subscribe, source.getSnapshot, source.getSnapshot);
}

/**
 * Count of locally committed mutations the server has not acknowledged yet.
 * Drive an "unsaved changes" indicator off this.
 */
export function usePendingMutations(): number {
  const db = useDb();
  return useEngineValue(
    (cb) => db.subscribeToPendingMutations(cb),
    () => db.pendingMutationCount,
    [db]
  );
}

export interface UseSyncStatus {
  /** Full health snapshot. */
  health: SyncHealth;
  /** `'healthy'` | `'degraded'`. */
  status: SyncHealthStatus;
  isHealthy: boolean;
  /** `true` once sync has failed for a sustained run: drive a banner off this. */
  isDegraded: boolean;
  /** `true` once at least one sync round has succeeded this session. */
  everConnected: boolean;
  /**
   * `true` only for a real lost connection: degraded AFTER a first successful
   * sync. Stays `false` during the initial "connecting" phase.
   */
  isOffline: boolean;
  /**
   * Transport state of the remote WebSocket. Flips the instant the socket
   * drops, unlike `status`, which only degrades after a sustained run of failed
   * sync rounds: drive a "reconnecting..." affordance off this.
   */
  connection: ConnectionState;
  /** `true` while the connection is being re-established. */
  isReconnecting: boolean;
}

/**
 * Sync health for a "can't reach the server" banner. Single sync failures are
 * absorbed by the retry; `isDegraded` only turns on once failures persist for
 * `syncHealth.degradeAfterConsecutiveFailures` rounds (default 3).
 */
export function useSyncStatus(): UseSyncStatus {
  const db = useDb();
  const health = useEngineValue<SyncHealth>(
    (cb) => db.subscribeToSyncHealth(cb),
    () => db.syncHealth,
    [db]
  );
  return {
    health,
    status: health.status,
    isHealthy: health.status === 'healthy',
    isDegraded: health.status === 'degraded',
    everConnected: health.everConnected,
    isOffline: health.status === 'degraded' && health.everConnected,
    connection: health.connection,
    isReconnecting: health.connection === 'reconnecting',
  };
}

export interface UseSyncActivityOptions {
  /** How long queries must be fetching before `isDownloading` turns on. Default 200 ms. */
  downloadDelayMs?: number;
  /** `isUploading` needs MORE than this many queued mutations. Default 0. */
  uploadThreshold?: number;
  /** How long the outbox must stay above `uploadThreshold`. Default 150 ms. */
  uploadDelayMs?: number;
}

export interface UseSyncActivity {
  /** Queries with traffic the user waits on (core `fetchingQueryCount`). */
  fetchingQueries: number;
  /** Locally committed writes the server has not acknowledged yet. */
  pendingMutations: number;
  /** Fetching for longer than `downloadDelayMs`: a "downloading" mark. */
  isDownloading: boolean;
  /** More than `uploadThreshold` writes queued for longer than `uploadDelayMs`. */
  isUploading: boolean;
}

/** `busy` once it has held for `delayMs`; off the same render it clears. */
function useDelayedOn(busy: boolean, delayMs: number): boolean {
  const [on, setOn] = useState(false);
  useEffect(() => {
    if (!busy) {
      setOn(false);
      return;
    }
    if (delayMs <= 0) {
      setOn(true);
      return;
    }
    const timer = setTimeout(() => setOn(true), delayMs);
    return () => clearTimeout(timer);
  }, [busy, delayMs]);
  return busy && on;
}

/**
 * The two directions of sync traffic, for an indicator in the app chrome. Both
 * flags are debounced ON by their delay and drop at once, so a local-first
 * page that answers from cache and confirms within a few ms never flickers.
 */
export function useSyncActivity<S extends SchemaStructure = any>(
  dbOrOptions?: SyncedDb<S> | UseSyncActivityOptions,
  maybeOptions?: UseSyncActivityOptions
): UseSyncActivity {
  const explicitDb = dbOrOptions instanceof SyncedDb ? dbOrOptions : undefined;
  const options = (explicitDb ? maybeOptions : (dbOrOptions as UseSyncActivityOptions)) ?? {};
  const contextDb = useOptionalDb();
  const db = explicitDb ?? contextDb;
  if (!db)
    throw new Error(
      'useSyncActivity must be used within a <Sp00kyProvider>, or pass the SyncedDb.'
    );

  const fetchingQueries = useEngineValue(
    (cb) => db.subscribeToFetchActivity(cb),
    () => db.fetchingQueryCount,
    [db]
  );
  const pendingMutations = useEngineValue(
    (cb) => db.subscribeToPendingMutations(cb),
    () => db.pendingMutationCount,
    [db]
  );
  const isDownloading = useDelayedOn(fetchingQueries > 0, options.downloadDelayMs ?? 200);
  const isUploading = useDelayedOn(
    pendingMutations > (options.uploadThreshold ?? 0),
    options.uploadDelayMs ?? 150
  );
  return { fetchingQueries, pendingMutations, isDownloading, isUploading };
}

export interface UseStorageStatus {
  /** Full durability snapshot. */
  health: StorageHealth;
  /** `'unknown'` | `'persistent'` | `'memory'`. */
  status: StorageHealthStatus;
  /** `true` when the local store survives a reload. */
  isPersistent: boolean;
  /**
   * `true` only when durable storage was requested and could NOT be opened, so
   * local writes die on reload. Drive a warning off this, not off `status`.
   */
  isMemoryFallback: boolean;
}

/** How durable the LOCAL cache is, for a "no local storage" warning. */
export function useStorageStatus(): UseStorageStatus {
  const db = useDb();
  const health = useEngineValue<StorageHealth>(
    (cb) => db.subscribeToStorageHealth(cb),
    () => db.storageHealth,
    [db]
  );
  return {
    health,
    status: health.status,
    isPersistent: health.status === 'persistent',
    isMemoryFallback: health.fallback,
  };
}

export interface UseImpersonationOptions {
  /**
   * This component renders the app's own impersonation banner. Required with
   * `impersonationBanner: { mode: 'custom' }`: it acknowledges the banner, so
   * the client does not fall back to its built-in one.
   */
  rendersBanner?: boolean;
}

export interface UseImpersonation {
  /** The active impersonation, or `null`. */
  impersonation: ImpersonationInfo | null;
  isImpersonating: boolean;
  /** Return to the admin session. */
  stop: () => Promise<void>;
}

/** Observe admin impersonation (started from the DevTools). */
export function useImpersonation(options: UseImpersonationOptions = {}): UseImpersonation {
  const db = useDb();
  const impersonation = useEngineValue<ImpersonationInfo | null>(
    (cb) => db.auth.subscribeImpersonation(cb),
    () => db.auth.impersonation,
    [db]
  );
  // Re-acknowledged per session: a new impersonation restarts the client's
  // fallback timer, and this component is what cancels it.
  useEffect(() => {
    if (options.rendersBanner && impersonation) db.acknowledgeImpersonationBanner();
  }, [db, options.rendersBanner, impersonation]);
  const stop = useCallback(() => db.auth.stopImpersonating(), [db]);
  return { impersonation, isImpersonating: impersonation !== null, stop };
}

export interface UseFeatureFlag {
  variant: string | undefined;
  payload: unknown | undefined;
  /** The resolved variant exists and is not `'off'`. */
  enabled: boolean;
}

/**
 * A feature flag for the signed-in user. Updates live when the assignment in
 * `_00_user_feature` changes (e.g. `spky flag enable <key>`).
 */
export function useFeatureFlag(key: string, options?: FeatureFlagOptions): UseFeatureFlag {
  const db = useDb();
  const fallback = options?.fallback;
  const ttl = options?.ttl;
  const state = useEngineValue<{ variant: string | undefined; payload: unknown }>(
    (cb) => {
      // A handle per subscription, closed with it: the module shares one live
      // query across all handles, so this is cheap.
      const handle = db.getSp00ky().feature(key, { fallback, ttl });
      const off = handle.subscribe((s) =>
        cb({ variant: s.variant ?? fallback, payload: s.payload })
      );
      return () => {
        off();
        handle.close();
      };
    },
    () => {
      // Seed the first render from the module's current answer.
      const peek = db.getSp00ky().feature(key, { fallback, ttl });
      const value = { variant: peek.variant(), payload: peek.payload() };
      peek.close();
      return value;
    },
    [db, key, fallback, ttl]
  );
  return {
    variant: state.variant,
    payload: state.payload,
    enabled: state.variant !== undefined && state.variant !== 'off',
  };
}

export interface UseAppReleaseOptions extends AppReleaseOptions {
  /** App name from sp00ky.yml, e.g. `web`. */
  app: string;
  /** The running build's version (X.Y.Z), typically baked in at build time. */
  currentVersion: string;
}

export interface UseAppRelease {
  /** Latest announced version for the app, or undefined when no row exists. */
  latestVersion: string | undefined;
  /** Announced version is semver-newer than the running build. */
  updateAvailable: boolean;
  /** The newer release asks clients to update/reload without prompting. */
  mandatory: boolean;
  /** The newer release asks reloads to clear service-worker caches first. */
  cacheBust: boolean;
  /**
   * Reload onto the announced release: `location.reload()`, or with cache-bust
   * clear CacheStorage, nudge the service worker to update, and navigate with a
   * `?cb=` token. The service worker is deliberately NOT unregistered.
   */
  reload: () => Promise<void>;
}

async function reloadForSnapshot(snapshot: AppReleaseSnapshot): Promise<void> {
  if (typeof window === 'undefined') return;
  if (snapshot.cacheBust) {
    try {
      if (window.caches) {
        const keys = await window.caches.keys();
        await Promise.all(keys.map((k) => window.caches.delete(k)));
      }
      if (navigator.serviceWorker) {
        const regs = await navigator.serviceWorker.getRegistrations();
        for (const r of regs) r.update().catch(() => {});
      }
      window.location.href = window.location.pathname + '?cb=' + Date.now();
      return;
    } catch {
      /* fall through to a plain reload */
    }
  }
  window.location.reload();
}

/**
 * The app's announced release (`_00_app_release:<app>`, written by
 * `spky deploy` / `spky release`) compared against the running build.
 */
export function useAppRelease(options: UseAppReleaseOptions): UseAppRelease {
  const db = useDb();
  const { app, ttl, currentVersion } = options;
  const snapshot = useEngineValue<AppReleaseSnapshot>(
    (cb) => {
      const handle = db.getSp00ky().appRelease(app, { ttl });
      const off = handle.subscribe(cb);
      return () => {
        off();
        handle.close();
      };
    },
    () => {
      const peek = db.getSp00ky().appRelease(app, { ttl });
      const value = peek.snapshot();
      peek.close();
      return value;
    },
    [db, app, ttl]
  );
  const updateAvailable = semverGt(snapshot.version, currentVersion);
  const latest = useLatest(snapshot);
  const reload = useCallback(() => reloadForSnapshot(latest.current), [latest]);
  return {
    latestVersion: snapshot.version,
    updateAvailable,
    mandatory: updateAvailable && snapshot.mandatory,
    cacheBust: snapshot.cacheBust,
    reload,
  };
}

export interface Submission<Args extends unknown[], R> {
  /** Run the wrapped async fn. Concurrent submits share the pending flag. Never rejects. */
  submit: (...args: Args) => Promise<R | undefined>;
  /** At least one submit is in flight. */
  pending: boolean;
  /** Error from the most recent settled submit, cleared on the next submit. */
  error: Error | undefined;
  /** Result of the most recent successful submit. */
  result: R | undefined;
  clearError: () => void;
}

/**
 * Pending/error state around a mutation, for a button spinner. The engine is
 * already optimistic local-first (a write commits locally and re-renders
 * through live queries before it syncs), so there is no optimistic layer here.
 * An error means the LOCAL commit failed; sync failures surface through
 * `useSyncStatus` and `usePendingMutations`.
 *
 * `submit` is stable across renders and always calls the latest `fn`.
 */
export function useSubmission<Args extends unknown[], R>(
  fn: (...args: Args) => Promise<R>
): Submission<Args, R> {
  const latest = useLatest(fn);
  const [inFlight, setInFlight] = useState(0);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [result, setResult] = useState<R | undefined>(undefined);

  const submit = useCallback(
    async (...args: Args): Promise<R | undefined> => {
      setError(undefined);
      setInFlight((n) => n + 1);
      try {
        const r = await latest.current(...args);
        setResult(() => r);
        return r;
      } catch (e) {
        setError(e instanceof Error ? e : new Error(String(e)));
        return undefined;
      } finally {
        setInFlight((n) => n - 1);
      }
    },
    [latest]
  );
  const clearError = useCallback(() => setError(undefined), []);
  return { submit, pending: inFlight > 0, error, result, clearError };
}
