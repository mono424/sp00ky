import { computed, DestroyRef, inject, type Signal, signal } from '@angular/core';
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
import { engineDb, inContext, toError } from './internal';
import type { InjectOptions } from './types';

/**
 * A signal over an engine value. The engine's subscribe callback only bumps a
 * version; the signal reads the engine's own current value (`db.syncHealth`,
 * ...), so nothing can go stale. `equal: () => false` because the engine may
 * hand back the same snapshot object after changing it; the derived booleans
 * below are compared by value, so readers still only wake on real changes.
 * The subscription ends with the injection context.
 */
function engineSignal<T>(
  current: () => T,
  subscribe: (onChange: () => void) => () => void
): Signal<T> {
  const version = signal(0);
  inject(DestroyRef).onDestroy(subscribe(() => version.update((v) => v + 1)));
  return computed(
    () => {
      version();
      return current();
    },
    { equal: () => false }
  );
}

/**
 * Count of locally committed mutations not acknowledged by the server yet.
 * Drive an "unsaved changes" indicator off it.
 */
export function injectPendingMutations(options?: InjectOptions): Signal<number> {
  return inContext(injectPendingMutations, options?.injector, () => {
    const db = engineDb('injectPendingMutations');
    const count = engineSignal(
      () => db.pendingMutationCount,
      (cb) => db.subscribeToPendingMutations(() => cb())
    );
    return computed(() => count());
  });
}

export interface SyncStatus {
  /** Full health snapshot. */
  readonly health: Signal<SyncHealth>;
  /** `'healthy'` | `'degraded'`. */
  readonly status: Signal<SyncHealthStatus>;
  readonly isHealthy: Signal<boolean>;
  /** `true` once sync has failed for a sustained run. Drive a banner off this. */
  readonly isDegraded: Signal<boolean>;
  /** `true` once at least one sync round has succeeded this session. */
  readonly everConnected: Signal<boolean>;
  /**
   * `true` only for a real lost connection: degraded AFTER a first successful
   * sync. Stays `false` during the initial "connecting" phase.
   */
  readonly isOffline: Signal<boolean>;
  /**
   * Transport state of the remote WebSocket. Flips the instant the socket
   * drops, unlike `status`, which only degrades after a sustained run of failed
   * sync rounds.
   */
  readonly connection: Signal<ConnectionState>;
  /** `true` while the connection is being re-established. Usually still healthy. */
  readonly isReconnecting: Signal<boolean>;
}

/**
 * Observe sync health for a "can't reach the server" banner / indicator.
 * Individual sync failures are absorbed by the retry; `isDegraded` only turns
 * on once failures persist (core `syncHealth.degradeAfterConsecutiveFailures`,
 * default 3) and turns off on the next successful round.
 *
 * Needs no component: in a service (`providedIn: 'root'`) it lives as long as
 * the app.
 */
export function injectSyncStatus(options?: InjectOptions): SyncStatus {
  return inContext(injectSyncStatus, options?.injector, () => {
    const db = engineDb('injectSyncStatus');
    const health = engineSignal(
      () => db.syncHealth,
      (cb) => db.subscribeToSyncHealth(() => cb())
    );
    return {
      health,
      status: computed(() => health().status),
      isHealthy: computed(() => health().status === 'healthy'),
      isDegraded: computed(() => health().status === 'degraded'),
      everConnected: computed(() => health().everConnected),
      isOffline: computed(() => {
        const h = health();
        return h.status === 'degraded' && h.everConnected;
      }),
      connection: computed(() => health().connection),
      isReconnecting: computed(() => health().connection === 'reconnecting'),
    };
  });
}

export interface StorageStatus {
  /** Full durability snapshot. */
  readonly health: Signal<StorageHealth>;
  /** `'unknown'` | `'persistent'` | `'memory'`. */
  readonly status: Signal<StorageHealthStatus>;
  /** `true` when the local store survives a reload. */
  readonly isPersistent: Signal<boolean>;
  /**
   * `true` only when durable storage was requested and could NOT be opened, so
   * local writes die on reload. Drive a warning off this, not off `status`: a
   * store configured as in-memory reports `'memory'` too, by choice.
   */
  readonly isMemoryFallback: Signal<boolean>;
}

/**
 * Observe how durable the LOCAL cache is, for a "no local storage" warning.
 * Under `localEngine: 'sqlite'` + `store: 'indexeddb'` only one client per
 * bucket can hold the OPFS store open; a second tab runs in memory.
 */
export function injectStorageStatus(options?: InjectOptions): StorageStatus {
  return inContext(injectStorageStatus, options?.injector, () => {
    const db = engineDb('injectStorageStatus');
    const health = engineSignal(
      () => db.storageHealth,
      (cb) => db.subscribeToStorageHealth(() => cb())
    );
    return {
      health,
      status: computed(() => health().status),
      isPersistent: computed(() => health().status === 'persistent'),
      isMemoryFallback: computed(() => health().fallback),
    };
  });
}

export interface SyncActivityOptions extends InjectOptions {
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

export interface SyncActivity {
  /** Queries with traffic the user waits on (core `fetchingQueryCount`). */
  readonly fetchingQueries: Signal<number>;
  /** Locally committed writes the server has not acknowledged yet. */
  readonly pendingMutations: Signal<number>;
  /** Fetching for longer than `downloadDelayMs`. Drives a "downloading" mark. */
  readonly isDownloading: Signal<boolean>;
  /** More than `uploadThreshold` writes queued for longer than `uploadDelayMs`. */
  readonly isUploading: Signal<boolean>;
}

/**
 * The two directions of sync traffic, for an indicator in the app chrome.
 * `isDownloading` and `isUploading` are debounced ON by their delay and off at
 * once, so a page that answers from cache and confirms with the server in a few
 * milliseconds never flickers. Timers end with the injection context.
 */
export function injectSyncActivity(options: SyncActivityOptions = {}): SyncActivity {
  return inContext(injectSyncActivity, options.injector, () => {
    const db = engineDb('injectSyncActivity');
    const destroyRef = inject(DestroyRef);

    const downloading = delayedOn(options.downloadDelayMs ?? 200, destroyRef);
    const uploading = delayedOn(options.uploadDelayMs ?? 150, destroyRef);
    const threshold = options.uploadThreshold ?? 0;

    const fetchingQueries = engineSignal(
      () => db.fetchingQueryCount,
      (cb) =>
        db.subscribeToFetchActivity((n) => {
          downloading.set(n > 0);
          cb();
        })
    );
    const pendingMutations = engineSignal(
      () => db.pendingMutationCount,
      (cb) =>
        db.subscribeToPendingMutations((n) => {
          uploading.set(n > threshold);
          cb();
        })
    );
    // Seed both from the current counts, whether or not the engine called back
    // immediately.
    downloading.set(db.fetchingQueryCount > 0);
    uploading.set(db.pendingMutationCount > threshold);

    return {
      fetchingQueries: computed(() => fetchingQueries()),
      pendingMutations: computed(() => pendingMutations()),
      isDownloading: downloading.on,
      isUploading: uploading.on,
    };
  });
}

/**
 * A flag that turns on once `busy` has held for `delayMs` and off the instant
 * it clears. A burst too short to see never flickers, and neither direction can
 * latch on after the work is done. Driven from engine callbacks, not effects,
 * so it works in services too.
 */
function delayedOn(
  delayMs: number,
  destroyRef: DestroyRef
): { on: Signal<boolean>; set: (busy: boolean) => void } {
  const on = signal(false);
  let busy = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const stop = () => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  destroyRef.onDestroy(stop);
  return {
    on: on.asReadonly(),
    set: (next) => {
      // A boolean, so a count moving 1 -> 2 does not restart the timer.
      if (next === busy) return;
      busy = next;
      stop();
      if (!next) on.set(false);
      else if (delayMs <= 0) on.set(true);
      else timer = setTimeout(() => on.set(true), delayMs);
    },
  };
}

export interface ImpersonationOptions extends InjectOptions {
  /**
   * This component renders the app's own impersonation banner. Required with
   * `impersonationBanner: { mode: 'custom' }`: it acknowledges the banner, so
   * the client does not fall back to its built-in one.
   */
  rendersBanner?: boolean;
}

export interface Impersonation {
  /** The active impersonation, or `null`. */
  readonly impersonation: Signal<ImpersonationInfo | null>;
  readonly isImpersonating: Signal<boolean>;
  /** Return to the admin session. */
  stop: () => Promise<void>;
}

/**
 * Observe admin impersonation (started from the DevTools). The client already
 * shows its own warning bar with a Stop button; use this for anything extra,
 * such as hiding actions an admin should not take on someone's behalf.
 */
export function injectImpersonation(options: ImpersonationOptions = {}): Impersonation {
  return inContext(injectImpersonation, options.injector, () => {
    const db = engineDb('injectImpersonation');
    const impersonation = engineSignal(
      () => db.auth.impersonation,
      (cb) =>
        db.auth.subscribeImpersonation((info) => {
          // Re-acknowledged per session: a new impersonation restarts the
          // client's fallback timer, and this banner is what cancels it.
          if (info && options.rendersBanner) db.acknowledgeImpersonationBanner();
          cb();
        })
    );
    return {
      impersonation,
      isImpersonating: computed(() => impersonation() !== null),
      stop: () => db.auth.stopImpersonating(),
    };
  });
}

export interface FeatureFlag {
  /** Resolved variant (`options.fallback` when unassigned). */
  readonly variant: Signal<string | undefined>;
  readonly payload: Signal<unknown>;
  /** The variant is set and not `'off'`. */
  readonly enabled: Signal<boolean>;
}

/**
 * A feature flag for the signed-in user. Updates whenever the
 * server-materialized assignment in `_00_user_feature` changes, so
 * `spky flag enable <key>` reaches the UI without a refresh. The handle closes
 * with the injection context.
 */
export function injectFeatureFlag(
  key: string,
  options: FeatureFlagOptions & InjectOptions = {}
): FeatureFlag {
  return inContext(injectFeatureFlag, options.injector, () => {
    const { injector: _, ...flagOptions } = options;
    const handle = engineDb('injectFeatureFlag').getSp00ky().feature(key, flagOptions);
    inject(DestroyRef).onDestroy(() => handle.close());
    const snapshot = engineSignal(
      () => handle,
      (cb) => handle.subscribe(() => cb())
    );
    return {
      variant: computed(() => snapshot().variant()),
      payload: computed(() => snapshot().payload()),
      enabled: computed(() => snapshot().enabled()),
    };
  });
}

export interface AppReleaseConfig extends AppReleaseOptions, InjectOptions {
  /** App name from sp00ky.yml, e.g. `web`. */
  app: string;
  /**
   * The running build's version (X.Y.Z), typically baked in at build time
   * (e.g. from package.json through an `environment` file).
   */
  currentVersion: string;
}

export interface AppRelease {
  /** Latest announced version for the app, or undefined when no row exists. */
  readonly latestVersion: Signal<string | undefined>;
  /** Announced version is semver-newer than the running build. */
  readonly updateAvailable: Signal<boolean>;
  /** The newer release asks clients to update/reload without prompting. */
  readonly mandatory: Signal<boolean>;
  /** The newer release asks reloads to clear service-worker caches first. */
  readonly cacheBust: Signal<boolean>;
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
export function injectAppRelease(config: AppReleaseConfig): AppRelease {
  return inContext(injectAppRelease, config.injector, () => {
    const handle = engineDb('injectAppRelease')
      .getSp00ky()
      .appRelease(config.app, { ttl: config.ttl });
    inject(DestroyRef).onDestroy(() => handle.close());
    const snapshot = engineSignal(
      () => handle.snapshot(),
      (cb) => handle.subscribe(() => cb())
    );
    const updateAvailable = computed(() => semverGt(snapshot().version, config.currentVersion));
    return {
      latestVersion: computed(() => snapshot().version),
      updateAvailable,
      mandatory: computed(() => updateAvailable() && snapshot().mandatory),
      cacheBust: computed(() => snapshot().cacheBust),
      reload: () => reloadForSnapshot(handle.snapshot()),
    };
  });
}

export interface Submission<Args extends unknown[], R> {
  /** Run the wrapped async fn. Never rejects; a failure lands in `error`. */
  submit: (...args: Args) => Promise<R | undefined>;
  /** True while at least one submit is in flight. */
  readonly pending: Signal<boolean>;
  /** Error from the most recent settled submit, cleared on the next submit. */
  readonly error: Signal<Error | undefined>;
  /** Result of the most recent successful submit. */
  readonly result: Signal<R | undefined>;
  clearError: () => void;
}

/**
 * Button pending / error state around a mutation. Deliberately no optimistic
 * layer: the engine already commits writes locally and re-renders through live
 * queries before it syncs. An `error` here means the LOCAL commit failed; sync
 * failures surface through `injectSyncStatus` and `injectPendingMutations`.
 * Needs no injection context: it only holds signals.
 */
export function injectSubmission<Args extends unknown[], R>(
  fn: (...args: Args) => Promise<R>
): Submission<Args, R> {
  const inFlight = signal(0);
  const error = signal<Error | undefined>(undefined);
  const result = signal<R | undefined>(undefined);
  return {
    submit: async (...args: Args): Promise<R | undefined> => {
      error.set(undefined);
      inFlight.update((n) => n + 1);
      try {
        const r = await fn(...args);
        result.set(r);
        return r;
      } catch (e) {
        error.set(toError(e));
        return undefined;
      } finally {
        inFlight.update((n) => n - 1);
      }
    },
    pending: computed(() => inFlight() > 0),
    error: error.asReadonly(),
    result: result.asReadonly(),
    clearError: () => error.set(undefined),
  };
}
