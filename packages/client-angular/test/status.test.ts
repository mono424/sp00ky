import { createEnvironmentInjector, EnvironmentInjector } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  injectFeatureFlag,
  injectImpersonation,
  injectPendingMutations,
  injectSubmission,
  injectSyncActivity,
  injectSyncStatus,
  SyncedDb,
} from '@spooky-sync/client-angular';
import { inject, provideDb } from './helpers';

/** A db whose pending-mutation and fetch counts the test sets. */
function statusDb() {
  let pending = 0;
  let fetching = 0;
  let health = { status: 'healthy', everConnected: true, connection: 'connected' } as any;
  const pendingSubs = new Set<(n: number) => void>();
  const fetchSubs = new Set<(n: number) => void>();
  const healthSubs = new Set<(h: any) => void>();
  const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
  Object.defineProperties(db, {
    pendingMutationCount: { get: () => pending },
    fetchingQueryCount: { get: () => fetching },
    syncHealth: { get: () => health },
  });
  const sub = <T>(set: Set<(v: T) => void>, now: () => T) =>
    vi.fn((cb: (v: T) => void) => {
      set.add(cb);
      cb(now());
      return () => set.delete(cb);
    });
  (db as any).subscribeToPendingMutations = sub(pendingSubs, () => pending);
  (db as any).subscribeToFetchActivity = sub(fetchSubs, () => fetching);
  (db as any).subscribeToSyncHealth = sub(healthSubs, () => health);
  return {
    db,
    setPending(n: number) {
      pending = n;
      for (const cb of pendingSubs) cb(n);
    },
    setFetching(n: number) {
      fetching = n;
      for (const cb of fetchSubs) cb(n);
    },
    setHealth(h: any) {
      health = h;
      for (const cb of healthSubs) cb(h);
    },
    pendingSubs,
    healthSubs,
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('engine-backed status', () => {
  it('injectPendingMutations follows the engine and unsubscribes with its injector', () => {
    const s = statusDb();
    provideDb(s.db);
    const child = createEnvironmentInjector([], TestBed.inject(EnvironmentInjector));
    const pending = injectPendingMutations({ injector: child });
    expect(pending()).toBe(0);
    s.setPending(2);
    expect(pending()).toBe(2);
    expect(s.pendingSubs.size).toBe(1);
    child.destroy();
    expect(s.pendingSubs.size).toBe(0);
  });

  it('injectSyncStatus derives the flags from the current health', () => {
    const s = statusDb();
    provideDb(s.db);
    const sync = inject(() => injectSyncStatus());
    expect(sync.isHealthy()).toBe(true);
    expect(sync.isOffline()).toBe(false);
    s.setHealth({ status: 'degraded', everConnected: true, connection: 'reconnecting' });
    expect(sync.isOffline()).toBe(true);
    expect(sync.isReconnecting()).toBe(true);
    expect(sync.status()).toBe('degraded');
  });

  it('notifies even when the engine hands back the same snapshot object', () => {
    const s = statusDb();
    provideDb(s.db);
    const sync = inject(() => injectSyncStatus());
    const shared = { status: 'healthy', everConnected: false, connection: 'connected' };
    s.setHealth(shared);
    expect(sync.everConnected()).toBe(false);
    shared.everConnected = true;
    s.setHealth(shared);
    expect(sync.everConnected()).toBe(true);
  });

  it('injectSyncActivity debounces on and drops off at once', () => {
    vi.useFakeTimers();
    const s = statusDb();
    provideDb(s.db);
    const activity = inject(() => injectSyncActivity({ downloadDelayMs: 200 }));
    s.setFetching(1);
    expect(activity.fetchingQueries()).toBe(1);
    expect(activity.isDownloading()).toBe(false);
    vi.advanceTimersByTime(199);
    expect(activity.isDownloading()).toBe(false);
    s.setFetching(2);
    vi.advanceTimersByTime(1);
    expect(activity.isDownloading()).toBe(true);
    s.setFetching(0);
    expect(activity.isDownloading()).toBe(false);

    s.setPending(1);
    vi.advanceTimersByTime(100);
    s.setPending(0);
    vi.advanceTimersByTime(500);
    expect(activity.isUploading()).toBe(false);
    s.setPending(3);
    vi.advanceTimersByTime(150);
    expect(activity.isUploading()).toBe(true);
    expect(activity.pendingMutations()).toBe(3);
  });

  it('injectImpersonation acknowledges the banner when it renders one', () => {
    let info: any = null;
    const subs = new Set<(i: any) => void>();
    const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
    const acknowledge = vi.fn();
    const stop = vi.fn(async () => {});
    Object.defineProperty(db, 'auth', {
      get: () => ({
        get impersonation() {
          return info;
        },
        subscribeImpersonation(cb: (i: any) => void) {
          subs.add(cb);
          cb(info);
          return () => subs.delete(cb);
        },
        stopImpersonating: stop,
      }),
    });
    (db as any).acknowledgeImpersonationBanner = acknowledge;
    provideDb(db);
    const imp = inject(() => injectImpersonation({ rendersBanner: true }));
    expect(imp.isImpersonating()).toBe(false);
    expect(acknowledge).not.toHaveBeenCalled();
    info = { userId: 'user:1' };
    for (const cb of subs) cb(info);
    expect(imp.isImpersonating()).toBe(true);
    expect(acknowledge).toHaveBeenCalledTimes(1);
    void imp.stop();
    expect(stop).toHaveBeenCalled();
  });

  it('injectFeatureFlag reads the handle and closes it with its injector', () => {
    let variant: string | undefined = undefined;
    const subs = new Set<() => void>();
    const handle = {
      variant: () => variant,
      payload: () => (variant ? { v: variant } : undefined),
      enabled: () => variant !== undefined && variant !== 'off',
      subscribe: (cb: () => void) => {
        subs.add(cb);
        cb();
        return () => subs.delete(cb);
      },
      close: vi.fn(),
    };
    const feature = vi.fn(() => handle);
    const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
    (db as any).sp00ky = { feature };
    provideDb(db);
    const child = createEnvironmentInjector([], TestBed.inject(EnvironmentInjector));
    const flag = injectFeatureFlag('new-checkout', { fallback: 'off', injector: child });
    expect(feature).toHaveBeenCalledWith('new-checkout', { fallback: 'off' });
    expect(flag.enabled()).toBe(false);
    variant = 'on';
    for (const cb of subs) cb();
    expect(flag.enabled()).toBe(true);
    expect(flag.variant()).toBe('on');
    child.destroy();
    expect(handle.close).toHaveBeenCalled();
  });
});

describe('injectSubmission', () => {
  it('tracks pending, result and error without an injection context', async () => {
    let fail = false;
    const save = injectSubmission(async (title: string) => {
      if (fail) throw new Error('nope');
      return title.toUpperCase();
    });
    const p = save.submit('hi');
    expect(save.pending()).toBe(true);
    await expect(p).resolves.toBe('HI');
    expect(save.pending()).toBe(false);
    expect(save.result()).toBe('HI');

    fail = true;
    await expect(save.submit('x')).resolves.toBeUndefined();
    expect(save.error()?.message).toBe('nope');
    save.clearError();
    expect(save.error()).toBeUndefined();
  });
});
