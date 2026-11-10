import { createSubscriber } from 'svelte/reactivity';
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
import type { SyncedDb } from './db.js';
import { toError, useDb } from './context.js';

/**
 * Make an engine value reactive without copying it into `$state`: reading the
 * returned function inside an effect, `$derived` or markup subscribes to the
 * engine (once, however many readers), and the subscription ends when the last
 * reader goes away. Outside a reactive scope it costs nothing, and the caller
 * reads the engine's own current value (`db.syncHealth`, ...), so nothing can
 * go stale. This is Svelte's `createSubscriber`, the primitive behind
 * `MediaQuery` and `fromStore`.
 *
 * The engine's subscribe APIs call back immediately with the current value;
 * that first call is dropped, the reader is about to read it anyway.
 */
function engineSubscriber(subscribe: (onChange: () => void) => () => void): () => void {
  return createSubscriber((update) => {
    let starting = true;
    const off = subscribe(() => {
      if (!starting) update();
    });
    starting = false;
    return off;
  });
}

/** A single reactive value, read as `.current` (the `svelte/reactivity` convention). */
export interface ReactiveValue<T> {
  readonly current: T;
}

const isDb = (v: unknown): v is SyncedDb<any> =>
  typeof v === 'object' && v !== null && typeof (v as SyncedDb<any>).getSp00ky === 'function';

/**
 * Count of locally committed mutations not acknowledged by the server yet.
 * Drive an "unsaved changes" indicator off `.current`.
 */
export function usePendingMutations(db: SyncedDb<any> = useDb()): ReactiveValue<number> {
  const subscribe = engineSubscriber((cb) => db.subscribeToPendingMutations(() => cb()));
  return {
    get current() {
      subscribe();
      return db.pendingMutationCount;
    },
  };
}

export interface UseSyncStatus {
  /** Full health snapshot. */
  readonly health: SyncHealth;
  /** `'healthy'` | `'degraded'`. */
  readonly status: SyncHealthStatus;
  readonly isHealthy: boolean;
  /** `true` once sync has failed for a sustained run. Drive a banner off this. */
  readonly isDegraded: boolean;
  /** `true` once at least one sync round has succeeded this session. */
  readonly everConnected: boolean;
  /**
   * `true` only for a real lost connection: degraded AFTER a first successful
   * sync. Stays `false` during the initial "connecting" phase.
   */
  readonly isOffline: boolean;
  /**
   * Transport state of the remote WebSocket. Flips the instant the socket
   * drops, unlike `status`, which only degrades after a sustained run of failed
   * sync rounds.
   */
  readonly connection: ConnectionState;
  /** `true` while the connection is being re-established. Usually still healthy. */
  readonly isReconnecting: boolean;
}

/**
 * Observe sync health for a "can't reach the server" banner / indicator.
 * Individual sync failures are absorbed by the retry; `isDegraded` only turns
 * on once failures persist (core `syncHealth.degradeAfterConsecutiveFailures`,
 * default 3) and turns off on the next successful round.
 *
 * Needs no component: with the client passed explicitly it also works in a
 * shared `.svelte.ts` module.
 */
export function useSyncStatus(db: SyncedDb<any> = useDb()): UseSyncStatus {
  const subscribe = engineSubscriber((cb) => db.subscribeToSyncHealth(() => cb()));
  const health = (): SyncHealth => {
    subscribe();
    return db.syncHealth;
  };
  return {
    get health() {
      return health();
    },
    get status() {
      return health().status;
    },
    get isHealthy() {
      return health().status === 'healthy';
    },
    get isDegraded() {
      return health().status === 'degraded';
    },
    get everConnected() {
      return health().everConnected;
    },
    get isOffline() {
      const h = health();
      return h.status === 'degraded' && h.everConnected;
    },
    get connection() {
      return health().connection;
    },
    get isReconnecting() {
      return health().connection === 'reconnecting';
    },
  };
}

export interface UseStorageStatus {
  /** Full durability snapshot. */
  readonly health: StorageHealth;
  /** `'unknown'` | `'persistent'` | `'memory'`. */
  readonly status: StorageHealthStatus;
  /** `true` when the local store survives a reload. */
  readonly isPersistent: boolean;
  /**
   * `true` only when durable storage was requested and could NOT be opened, so
   * local writes die on reload. Drive a warning off this, not off `status`: a
   * store configured as in-memory reports `'memory'` too, by choice.
   */
  readonly isMemoryFallback: boolean;
}

/**
 * Observe how durable the LOCAL cache is, for a "no local storage" warning.
 * Under `localEngine: 'sqlite'` + `store: 'indexeddb'` only one client per
 * bucket can hold the OPFS store open; a second tab runs in memory.
 */
export function useStorageStatus(db: SyncedDb<any> = useDb()): UseStorageStatus {
  const subscribe = engineSubscriber((cb) => db.subscribeToStorageHealth(() => cb()));
  const health = (): StorageHealth => {
    subscribe();
    return db.storageHealth;
  };
  return {
    get health() {
      return health();
    },
    get status() {
      return health().status;
    },
    get isPersistent() {
      return health().status === 'persistent';
    },
    get isMemoryFallback() {
      return health().fallback;
    },
  };
}

export interface UseSyncActivityOptions {
  /**
   * How long queries must be fetching before `isDownloading` turns on. Filters
   * the sub-frame fetches a warm cache produces. Default 200 ms.
   */
  downloadDelayMs?: number;
  /** `isUploading` turns on once MORE than this many writes are queued. Default 0. */
  uploadThreshold?: number;
  /**
   * How long the outbox must stay above `uploadThreshold` before `isUploading`
   * turns on: a write acknowledged within a round trip never lights it.
   * Default 150 ms.
   */
  uploadDelayMs?: number;
}

export interface UseSyncActivity {
  /** Queries with traffic the user waits on (core `fetchingQueryCount`). */
  readonly fetchingQueries: number;
  /** Locally committed writes the server has not acknowledged yet. */
  readonly pendingMutations: number;
  /** Fetching for longer than `downloadDelayMs`. Drives a "downloading" mark. */
  readonly isDownloading: boolean;
  /** More than `uploadThreshold` writes queued for longer than `uploadDelayMs`. */
  readonly isUploading: boolean;
}

/**
 * The two directions of sync traffic, for an indicator in the app chrome.
 * `isDownloading` and `isUploading` are debounced ON by their delay and off at
 * once, so a page that answers from cache and confirms with the server in a few
 * milliseconds never flickers. Owns timers, so call it while a component
 * initialises (or inside `$effect.root`).
 */
export function useSyncActivity(options?: UseSyncActivityOptions): UseSyncActivity;
export function useSyncActivity(
  db: SyncedDb<any>,
  options?: UseSyncActivityOptions
): UseSyncActivity;
export function useSyncActivity(
  dbOrOptions?: SyncedDb<any> | UseSyncActivityOptions,
  maybeOptions?: UseSyncActivityOptions
): UseSyncActivity {
  const db = isDb(dbOrOptions) ? dbOrOptions : useDb();
  const options = (isDb(dbOrOptions) ? maybeOptions : dbOrOptions) ?? {};

  const fetchingSub = engineSubscriber((cb) => db.subscribeToFetchActivity(() => cb()));
  const pendingSub = engineSubscriber((cb) => db.subscribeToPendingMutations(() => cb()));
  const fetchingQueries = () => {
    fetchingSub();
    return db.fetchingQueryCount;
  };
  const pendingMutations = () => {
    pendingSub();
    return db.pendingMutationCount;
  };

  const isDownloading = delayedOn(() => fetchingQueries() > 0, options.downloadDelayMs ?? 200);
  const isUploading = delayedOn(
    () => pendingMutations() > (options.uploadThreshold ?? 0),
    options.uploadDelayMs ?? 150
  );

  return {
    get fetchingQueries() {
      return fetchingQueries();
    },
    get pendingMutations() {
      return pendingMutations();
    },
    get isDownloading() {
      return isDownloading();
    },
    get isUploading() {
      return isUploading();
    },
  };
}

/**
 * `busy` held for `delayMs`, dropped the instant it clears. A burst too short
 * to see never flickers, and neither direction can latch on after the work is
 * done.
 */
function delayedOn(busy: () => boolean, delayMs: number): () => boolean {
  let on = $state(false);
  // A boolean, so a count moving 1 -> 2 does not restart the timer.
  const isBusy = $derived(busy());
  $effect(() => {
    if (!isBusy) {
      on = false;
      return;
    }
    if (delayMs <= 0) {
      on = true;
      return;
    }
    const timer = setTimeout(() => {
      on = true;
    }, delayMs);
    return () => clearTimeout(timer);
  });
  return () => on;
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
  readonly impersonation: ImpersonationInfo | null;
  readonly isImpersonating: boolean;
  /** Return to the admin session. */
  stop: () => Promise<void>;
}

/**
 * Observe admin impersonation (started from the DevTools). The client already
 * shows its own warning bar with a Stop button; use this for anything extra,
 * such as hiding actions an admin should not take on someone's behalf.
 */
export function useImpersonation(
  options: UseImpersonationOptions = {},
  db: SyncedDb<any> = useDb()
): UseImpersonation {
  const subscribe = engineSubscriber((cb) => db.auth.subscribeImpersonation(() => cb()));
  const impersonation = (): ImpersonationInfo | null => {
    subscribe();
    return db.auth.impersonation;
  };
  if (options.rendersBanner) {
    // Re-acknowledged per session: a new impersonation restarts the client's
    // fallback timer, and this component is what cancels it.
    $effect(() => {
      if (impersonation()) db.acknowledgeImpersonationBanner();
    });
  }
  return {
    get impersonation() {
      return impersonation();
    },
    get isImpersonating() {
      return impersonation() !== null;
    },
    stop: () => db.auth.stopImpersonating(),
  };
}

export interface UseFeatureFlag {
  /** Resolved variant (`options.fallback` when unassigned). */
  readonly variant: string | undefined;
  readonly payload: unknown;
  /** The variant is set and not `'off'`. */
  readonly enabled: boolean;
}

/**
 * A feature flag for the signed-in user. Updates whenever the
 * server-materialized assignment in `_00_user_feature` changes, so
 * `spky flag enable <key>` reaches the UI without a refresh. The handle closes
 * with the component.
 */
export function useFeatureFlag(
  key: string,
  options?: FeatureFlagOptions,
  db: SyncedDb<any> = useDb()
): UseFeatureFlag {
  const handle = db.getSp00ky().feature(key, options);
  $effect(() => () => handle.close());
  const subscribe = engineSubscriber((cb) => handle.subscribe(() => cb()));
  return {
    get variant() {
      subscribe();
      return handle.variant();
    },
    get payload() {
      subscribe();
      return handle.payload();
    },
    get enabled() {
      subscribe();
      return handle.enabled();
    },
  };
}

export interface UseAppReleaseOptions extends AppReleaseOptions {
  /** App name from sp00ky.yml, e.g. `web`. */
  app: string;
  /**
   * The running build's version (X.Y.Z), typically baked in at build time
   * (e.g. a vite `define` from package.json).
   */
  currentVersion: string;
}

export interface UseAppRelease {
  /** Latest announced version for the app, or undefined when no row exists. */
  readonly latestVersion: string | undefined;
  /** Announced version is semver-newer than the running build. */
  readonly updateAvailable: boolean;
  /** The newer release asks clients to update/reload without prompting. */
  readonly mandatory: boolean;
  /** The newer release asks reloads to clear service-worker caches first. */
  readonly cacheBust: boolean;
  /**
   * Reload onto the announced release. Plain `location.reload()` normally;
   * with cache-bust, CacheStorage is cleared, the service worker is nudged to
   * update (never unregistered) and navigation carries a `?cb=` token.
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
 * Observe the app's announced release (`_00_app_release:<app>`, written by
 * `spky deploy` / `spky release`) and compare it with the running build, for a
 * "new version available, Reload" notice. Guard an automatic `reload()` on
 * `mandatory` against loops with a per-version marker.
 */
export function useAppRelease(
  options: UseAppReleaseOptions,
  db: SyncedDb<any> = useDb()
): UseAppRelease {
  const handle = db.getSp00ky().appRelease(options.app, { ttl: options.ttl });
  $effect(() => () => handle.close());
  const subscribe = engineSubscriber((cb) => handle.subscribe(() => cb()));
  const snapshot = (): AppReleaseSnapshot => {
    subscribe();
    return handle.snapshot();
  };
  const updateAvailable = () => semverGt(snapshot().version, options.currentVersion);
  return {
    get latestVersion() {
      return snapshot().version;
    },
    get updateAvailable() {
      return updateAvailable();
    },
    get mandatory() {
      return updateAvailable() && snapshot().mandatory;
    },
    get cacheBust() {
      return snapshot().cacheBust;
    },
    reload: () => reloadForSnapshot(handle.snapshot()),
  };
}

export interface Submission<Args extends unknown[], R> {
  /** Run the wrapped async fn. Never rejects; a failure lands in `error`. */
  submit: (...args: Args) => Promise<R | undefined>;
  /** True while at least one submit is in flight. */
  readonly pending: boolean;
  /** Error from the most recent settled submit, cleared on the next submit. */
  readonly error: Error | undefined;
  /** Result of the most recent successful submit. */
  readonly result: R | undefined;
  clearError: () => void;
}

/**
 * Button pending / error state around a mutation. Deliberately no optimistic
 * layer: the engine already commits writes locally and re-renders through live
 * queries before it syncs. An `error` here means the LOCAL commit failed; sync
 * failures surface through `useSyncStatus` and `usePendingMutations`. Needs no
 * component and no provider.
 */
export function useSubmission<Args extends unknown[], R>(
  fn: (...args: Args) => Promise<R>
): Submission<Args, R> {
  let inFlight = $state(0);
  let error = $state.raw<Error | undefined>(undefined);
  let result = $state.raw<R | undefined>(undefined);
  return {
    submit: async (...args: Args): Promise<R | undefined> => {
      error = undefined;
      inFlight += 1;
      try {
        const r = await fn(...args);
        result = r;
        return r;
      } catch (e) {
        error = toError(e);
        return undefined;
      } finally {
        inFlight -= 1;
      }
    },
    get pending() {
      return inFlight > 0;
    },
    get error() {
      return error;
    },
    get result() {
      return result;
    },
    clearError: () => {
      error = undefined;
    },
  };
}
