import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { createRoot, flush } from 'solid-js';
import { useSyncActivity } from '../use-sync-activity';
import { SyncedDb } from '../../index';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));
const settle = async () => {
  await tick();
  flush();
  await tick();
};

/** A SyncedDb double exposing just the two channels the hook subscribes to. */
function mockDb(initialFetching = 0, initialPending = 0) {
  let fetchingCb: ((n: number) => void) | undefined;
  let pendingCb: ((n: number) => void) | undefined;
  const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
  Object.defineProperty(db, 'fetchingQueryCount', { get: () => initialFetching });
  Object.defineProperty(db, 'pendingMutationCount', { get: () => initialPending });
  (db as any).subscribeToFetchActivity = vi.fn((cb: (n: number) => void) => {
    fetchingCb = cb;
    cb(initialFetching);
    return () => {
      fetchingCb = undefined;
    };
  });
  (db as any).subscribeToPendingMutations = vi.fn((cb: (n: number) => void) => {
    pendingCb = cb;
    cb(initialPending);
    return () => {
      pendingCb = undefined;
    };
  });
  return {
    db,
    setFetching: (n: number) => fetchingCb?.(n),
    setPending: (n: number) => pendingCb?.(n),
  };
}

describe('useSyncActivity', () => {
  beforeEach(() => vi.useFakeTimers({ shouldAdvanceTime: true }));
  afterEach(() => vi.useRealTimers());

  it('turns isDownloading on only after the delay, and off at once', async () => {
    const m = mockDb();
    await createRoot(async (dispose) => {
      const a = useSyncActivity(m.db, { downloadDelayMs: 200 });
      await settle();
      expect(a.fetchingQueries()).toBe(0);
      expect(a.isDownloading()).toBe(false);

      m.setFetching(2);
      await settle();
      expect(a.fetchingQueries()).toBe(2);
      expect(a.isDownloading()).toBe(false);
      await vi.advanceTimersByTimeAsync(150);
      flush();
      expect(a.isDownloading()).toBe(false);
      await vi.advanceTimersByTimeAsync(60);
      flush();
      expect(a.isDownloading()).toBe(true);

      m.setFetching(0);
      await settle();
      expect(a.isDownloading()).toBe(false);
      dispose();
    });
  });

  it('a fetch shorter than the delay never shows', async () => {
    const m = mockDb();
    await createRoot(async (dispose) => {
      const a = useSyncActivity(m.db, { downloadDelayMs: 200 });
      await settle();
      m.setFetching(1);
      await settle();
      await vi.advanceTimersByTimeAsync(100);
      m.setFetching(0);
      await settle();
      await vi.advanceTimersByTimeAsync(300);
      flush();
      expect(a.isDownloading()).toBe(false);
      dispose();
    });
  });

  it('turns isUploading on after the delay for any queued write, and off at once', async () => {
    const m = mockDb(0, 1);
    await createRoot(async (dispose) => {
      const a = useSyncActivity(m.db, { uploadDelayMs: 150 });
      await settle();
      expect(a.pendingMutations()).toBe(1);
      expect(a.isUploading()).toBe(false);
      await vi.advanceTimersByTimeAsync(200);
      flush();
      expect(a.isUploading()).toBe(true);
      m.setPending(0);
      await settle();
      expect(a.isUploading()).toBe(false);
      dispose();
    });
  });

  it('a write acknowledged inside the delay never shows', async () => {
    const m = mockDb(0, 0);
    await createRoot(async (dispose) => {
      const a = useSyncActivity(m.db, { uploadDelayMs: 150 });
      await settle();
      m.setPending(1);
      await settle();
      await vi.advanceTimersByTimeAsync(80);
      m.setPending(0);
      await settle();
      await vi.advanceTimersByTimeAsync(300);
      flush();
      expect(a.isUploading()).toBe(false);
      dispose();
    });
  });

  it('uploadThreshold holds the mark back until a backlog builds', async () => {
    const m = mockDb(0, 1);
    await createRoot(async (dispose) => {
      const a = useSyncActivity(m.db, { uploadThreshold: 1, uploadDelayMs: 0 });
      await settle();
      expect(a.isUploading()).toBe(false);
      m.setPending(2);
      await settle();
      expect(a.isUploading()).toBe(true);
      dispose();
    });
  });
});
