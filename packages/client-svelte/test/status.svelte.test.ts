import { describe, expect, it, vi } from 'vitest';
import { flushSync } from 'svelte';
import { SyncedDb } from '../src/db.js';
import {
  useAppRelease,
  useFeatureFlag,
  useImpersonation,
  usePendingMutations,
  useStorageStatus,
  useSubmission,
  useSyncActivity,
  useSyncStatus,
} from '../src/status.svelte.js';
import { useChannel, useCrdtField } from '../src/realtime.svelte.js';
import { mockEngine } from './helpers.js';

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
  };
}

describe('engine-backed status', () => {
  it('subscribes while read in an effect, updates, and unsubscribes after', async () => {
    const s = statusDb();
    const seen: number[] = [];
    const destroy = $effect.root(() => {
      const pending = usePendingMutations(s.db);
      $effect(() => {
        seen.push(pending.current);
      });
    });
    flushSync();
    expect(s.pendingSubs.size).toBe(1);
    s.setPending(2);
    flushSync();
    s.setPending(3);
    flushSync();
    expect(seen).toEqual([0, 2, 3]);
    destroy();
    await new Promise((r) => setTimeout(r, 0));
    expect(s.pendingSubs.size).toBe(0);
  });

  it('reads the current value outside any effect without subscribing', () => {
    const s = statusDb();
    const sync = useSyncStatus(s.db);
    expect(sync.isHealthy).toBe(true);
    s.setHealth({ status: 'degraded', everConnected: true, connection: 'reconnecting' });
    expect(sync.isOffline).toBe(true);
    expect(sync.isReconnecting).toBe(true);
    expect(s.pendingSubs.size).toBe(0);
  });

  it('useSyncActivity debounces on and drops off at once', () => {
    vi.useFakeTimers();
    try {
      const s = statusDb();
      let activity!: ReturnType<typeof useSyncActivity>;
      const destroy = $effect.root(() => {
        activity = useSyncActivity(s.db, { downloadDelayMs: 200 });
        $effect(() => {
          void activity.isDownloading;
        });
      });
      flushSync();
      s.setFetching(1);
      flushSync();
      vi.advanceTimersByTime(150);
      flushSync();
      expect(activity.isDownloading).toBe(false);
      s.setFetching(2); // still busy: must not restart the delay
      flushSync();
      vi.advanceTimersByTime(60);
      flushSync();
      expect(activity.isDownloading).toBe(true);
      s.setFetching(0);
      flushSync();
      expect(activity.isDownloading).toBe(false);
      destroy();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('useSubmission', () => {
  it('tracks pending, result and error, never rejects', async () => {
    let fail = false;
    const sub = useSubmission(async (n: number) => {
      await Promise.resolve();
      if (fail) throw new Error('local commit failed');
      return n * 2;
    });
    const p = sub.submit(2);
    expect(sub.pending).toBe(true);
    expect(await p).toBe(4);
    expect(sub.pending).toBe(false);
    expect(sub.result).toBe(4);
    fail = true;
    expect(await sub.submit(1)).toBeUndefined();
    expect(sub.error?.message).toBe('local commit failed');
    sub.clearError();
    expect(sub.error).toBeUndefined();
  });
});

describe('SyncedDb writes', () => {
  it('unwraps a $state payload before it reaches the core', async () => {
    const eng = mockEngine();
    const draft = $state({ title: 'Hi', tags: ['a'] });
    await eng.db.create('post:1', draft);
    const sent = eng.sp00ky.create.mock.calls[0][1];
    expect(sent).toEqual({ title: 'Hi', tags: ['a'] });
    // A Svelte proxy cannot be structured-cloned; the copy can.
    expect(() => structuredClone(draft)).toThrow();
    expect(() => structuredClone(sent)).not.toThrow();
  });
});

describe('SyncedDb before init()', () => {
  // The real core client: creating it opens nothing.
  const config = {
    database: { namespace: 'n', database: 'd', endpoint: 'ws://127.0.0.1:9/rpc', store: 'memory' },
    schema: {
      tables: [{ name: 'post', channel: true, columns: { title: { type: 'string' } } }],
      relationships: [],
      backends: {},
      buckets: [],
    },
    schemaSurql: '',
    logLevel: 'silent',
  } as any;

  it('throws, as it always did', async () => {
    const db = new SyncedDb<any>(config);
    expect(db.isReady).toBe(false);
    expect(() => db.getSp00ky()).toThrow('SyncedDb not initialized');
    expect(() => db.query('post')).toThrow('SyncedDb not initialized');
    expect(() => db.syncHealth).toThrow('SyncedDb not initialized');
    expect(() => db.auth).toThrow('SyncedDb not initialized');
    await expect(db.create('post:1', {})).rejects.toThrow('SyncedDb not initialized');
    expect(db.peekQuery({} as any)).toBeNull();
  });

  it('eager: the runes are created on it and report nothing known yet', () => {
    const db = new SyncedDb<any>(config, { eager: true });
    const seen: Record<string, unknown> = {};
    const destroy = $effect.root(() => {
      const sync = useSyncStatus(db);
      const storage = useStorageStatus(db);
      const pending = usePendingMutations(db);
      const activity = useSyncActivity(db);
      const impersonation = useImpersonation({ rendersBanner: true }, db);
      const flag = useFeatureFlag('beta', { fallback: 'off' }, db);
      const release = useAppRelease({ app: 'web', currentVersion: '1.0.0' }, db);
      const field = useCrdtField('post', () => null, 'title', db);
      const channel = useChannel(
        () => db.query('post').where({ title: 'x' }),
        () => ['user:a', 'user:b'],
        db
      );
      $effect(() => {
        seen.sync = [sync.status, sync.everConnected, sync.isOffline, sync.connection];
        seen.storage = storage.status;
        seen.pending = pending.current;
        seen.activity = [activity.fetchingQueries, activity.isDownloading, activity.isUploading];
        seen.impersonating = impersonation.isImpersonating;
        seen.flag = [flag.variant, flag.enabled];
        seen.release = [release.latestVersion, release.updateAvailable];
        seen.field = field.current;
        seen.channelPeers = channel.peers;
      });
    });
    flushSync();
    expect(seen).toEqual({
      sync: ['healthy', false, false, 'disconnected'],
      storage: 'unknown',
      pending: 0,
      activity: [0, false, false],
      impersonating: false,
      flag: ['off', false],
      release: [undefined, false],
      field: null,
      channelPeers: [],
    });
    expect(db.isReady).toBe(false);
    // Signed out until `init()` has restored a session.
    let userId: string | null | undefined;
    db.auth.subscribe((id) => (userId = id))();
    expect(userId).toBeNull();
    destroy();
  });
});
