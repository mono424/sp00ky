import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, StrictMode, type ReactNode } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { SyncedDb } from './db';
import { Sp00kyProvider, useDb } from './provider';
import { useQuery } from './query';
import { useSubmission, useSyncActivity, useSyncStatus } from './status';
import { useDownloadFile } from './files';

(globalThis as any).IS_REACT_ACT_ENVIRONMENT = true;

const tick = () => new Promise<void>((r) => setTimeout(r, 0));
const flush = () =>
  act(async () => {
    await tick();
    await tick();
  });

const mounted: Root[] = [];
afterEach(() => {
  for (const root of mounted.splice(0)) act(() => root.unmount());
  vi.restoreAllMocks();
});

function render(ui: ReactNode) {
  const container = document.createElement('div');
  const root = createRoot(container);
  mounted.push(root);
  act(() => root.render(ui));
  return {
    container,
    rerender: (next: ReactNode) => act(() => root.render(next)),
    unmount: () => {
      mounted.splice(mounted.indexOf(root), 1);
      act(() => root.unmount());
    },
  };
}

/** Render a hook; `result.current` is its latest return value. */
function renderHook<P, T>(hook: (props: P) => T, props: P, wrap = (n: ReactNode) => n) {
  const result = { current: undefined as T, renders: 0 };
  function Probe(p: { value: P }) {
    result.current = hook(p.value);
    result.renders++;
    return null;
  }
  const view = render(wrap(<Probe value={props} />));
  return {
    result,
    rerender: (next: P) => view.rerender(wrap(<Probe value={next} />)),
    unmount: view.unmount,
  };
}

type Emission = Record<string, any>[];

function mockEngine() {
  const subs = new Map<string, (e: Emission) => void>();
  const statusSubs = new Map<string, (s: string) => void>();
  const authoritySubs = new Map<string, (known: boolean) => void>();
  const unsubscribed: string[] = [];
  const sp00ky = {
    subscribe: vi.fn(async (hash: string, cb: (e: Emission) => void) => {
      subs.set(hash, cb);
      return () => {
        subs.delete(hash);
        unsubscribed.push(hash);
      };
    }),
    subscribeQueryStatus: vi.fn(
      (hash: string, cb: (s: string) => void, o?: { immediate?: boolean }) => {
        statusSubs.set(hash, cb);
        if (o?.immediate) cb('idle');
        return () => statusSubs.delete(hash);
      }
    ),
    subscribeQueryAuthority: vi.fn(
      (hash: string, cb: (k: boolean) => void, o?: { immediate?: boolean }) => {
        authoritySubs.set(hash, cb);
        if (o?.immediate) cb(false);
        return () => authoritySubs.delete(hash);
      }
    ),
    deregisterQuery: vi.fn(),
    reportFrontendTiming: vi.fn(),
    preload: vi.fn(async () => {}),
  };
  const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
  (db as any).getSp00ky = () => sp00ky;
  return {
    db,
    sp00ky,
    emit: (hash: string, e: Emission) => act(() => subs.get(hash)?.(e)),
    setStatus: (hash: string, s: string) => act(() => statusSubs.get(hash)?.(s)),
    setAuthority: (hash: string, known: boolean) => act(() => authoritySubs.get(hash)?.(known)),
    unsubscribed,
    activeSubs: () => [...subs.keys()],
  };
}

/** A fresh query object per call, like building one inline on every render. */
function mockQuery(hash: string, isOne = false, run?: () => Promise<{ hash: string }>) {
  return { hash, isOne, run: run ?? (async () => ({ hash })) } as any;
}

describe('useQuery', () => {
  it('serves [] at once, then live rows, keeping unchanged rows by identity', async () => {
    const eng = mockEngine();
    const { result, unmount } = renderHook(
      (h: string) => useQuery(eng.db, mockQuery(h)),
      'h1' as string
    );
    expect(result.current.data).toEqual([]);
    expect(result.current.isLoading).toBe(true);

    await flush();
    eng.emit('h1', [
      { id: 'a', n: 1 },
      { id: 'b', n: 2 },
    ]);
    expect((result.current.data as any[]).map((r) => r.id)).toEqual(['a', 'b']);
    expect(result.current.isLoading).toBe(false);
    expect(result.current.isSettled).toBe(false);

    eng.setAuthority('h1', true);
    expect(result.current.isSettled).toBe(true);
    expect(result.current.isAuthoritative).toBe(true);

    const [rowA, rowB] = result.current.data as any[];
    eng.emit('h1', [
      { id: 'a', n: 99 },
      { id: 'b', n: 2 },
    ]);
    const [nextA, nextB] = result.current.data as any[];
    expect(nextA).not.toBe(rowA);
    expect(nextA.n).toBe(99);
    expect(nextB).toBe(rowB);

    unmount();
    expect(eng.unsubscribed).toContain('h1');
  });

  it('a deep-equal emission does not re-render', async () => {
    const eng = mockEngine();
    const { result } = renderHook(() => useQuery(eng.db, mockQuery('h1')), null);
    await flush();
    eng.emit('h1', [{ id: 'a', n: 1 }]);
    const renders = result.renders;
    eng.emit('h1', [{ id: 'a', n: 1 }]);
    expect(result.renders).toBe(renders);
  });

  it('the server answering with no rows ends loading as empty', async () => {
    const eng = mockEngine();
    const { result } = renderHook(() => useQuery(eng.db, mockQuery('h1')), null);
    await flush();
    eng.emit('h1', []);
    expect(result.current.isLoading).toBe(true);
    expect(result.current.isEmpty).toBe(false);
    eng.setAuthority('h1', true);
    expect(result.current.isLoading).toBe(false);
    expect(result.current.isEmpty).toBe(true);
  });

  it('re-registers only when the hash changes, keeping the previous rows meanwhile', async () => {
    const eng = mockEngine();
    const { result, rerender } = renderHook(
      (h: string) => useQuery(eng.db, mockQuery(h)),
      'h1' as string
    );
    await flush();
    eng.emit('h1', [{ id: 'a' }]);
    eng.setAuthority('h1', true);

    rerender('h1'); // new query object, same identity
    await flush();
    expect(eng.sp00ky.subscribe).toHaveBeenCalledTimes(1);

    rerender('h2');
    await flush();
    expect(eng.unsubscribed).toEqual(['h1']);
    expect(eng.activeSubs()).toEqual(['h2']);
    expect((result.current.data as any[])[0].id).toBe('a');
    expect(result.current.isAuthoritative).toBe(false);
    expect(result.current.isLoading).toBe(true);

    eng.emit('h2', [{ id: 'z' }]);
    expect((result.current.data as any[])[0].id).toBe('z');
  });

  it('one() serves null, then the row', async () => {
    const eng = mockEngine();
    const { result } = renderHook(() => useQuery(eng.db, mockQuery('h1', true)), null);
    expect(result.current.data).toBeNull();
    await flush();
    eng.emit('h1', [{ id: 'a' }]);
    expect(result.current.data).toEqual({ id: 'a' });
    expect(result.current.hasData).toBe(true);
  });

  it('surfaces a failed registration as error and stops loading', async () => {
    const eng = mockEngine();
    const failing = mockQuery('h1', false, async () => {
      throw new Error('503 NOT_READY');
    });
    const { result } = renderHook(() => useQuery(eng.db, failing), null);
    await flush();
    expect(result.current.error?.message).toBe('503 NOT_READY');
    expect(result.current.isLoading).toBe(false);
  });

  it('null and enabled:false register nothing', async () => {
    const eng = mockEngine();
    renderHook(() => useQuery(eng.db, null), null);
    renderHook(() => useQuery(eng.db, mockQuery('h1'), { enabled: false }), null);
    await flush();
    expect(eng.sp00ky.subscribe).not.toHaveBeenCalled();
  });

  it('deregisterOnCleanup deregisters after unsubscribing', async () => {
    const eng = mockEngine();
    const { unmount } = renderHook(
      () => useQuery(eng.db, mockQuery('h1'), { deregisterOnCleanup: true }),
      null
    );
    await flush();
    unmount();
    expect(eng.unsubscribed).toEqual(['h1']);
    expect(eng.sp00ky.deregisterQuery).toHaveBeenCalledWith('h1');
  });

  it('StrictMode ends with exactly one live subscription', async () => {
    const eng = mockEngine();
    const { result } = renderHook(
      () => useQuery(eng.db, mockQuery('h1')),
      null,
      (n) => <StrictMode>{n}</StrictMode>
    );
    await flush();
    expect(eng.activeSubs()).toEqual(['h1']);
    eng.emit('h1', [{ id: 'a' }]);
    expect((result.current.data as any[]).length).toBe(1);
  });
});

describe('Sp00kyProvider', () => {
  it('shows the fallback, inits once under StrictMode, then provides the db', async () => {
    // Slower than StrictMode's remount, so a client the remount failed to
    // reclaim would be closed before it is revealed.
    const init = vi
      .spyOn(SyncedDb.prototype, 'init')
      .mockImplementation(() => new Promise<void>((r) => setTimeout(r, 5)));
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const onReady = vi.fn();
    const preload = vi.fn(async () => {});
    function Child() {
      const db = useDb();
      return <span>{db instanceof SyncedDb ? 'ready' : 'no'}</span>;
    }
    const view = render(
      <StrictMode>
        <Sp00kyProvider config={{} as any} fallback="loading" onReady={onReady} preload={preload}>
          <Child />
        </Sp00kyProvider>
      </StrictMode>
    );
    expect(view.container.textContent).toBe('loading');
    await act(async () => {
      await new Promise((r) => setTimeout(r, 20));
    });
    expect(view.container.textContent).toBe('ready');
    expect(init).toHaveBeenCalledTimes(1);
    expect(preload).toHaveBeenCalledTimes(1);
    expect(onReady).toHaveBeenCalledTimes(1);
    await flush();
    expect(close).not.toHaveBeenCalled();
  });

  it('closes a client whose init outlived the provider', async () => {
    let resolveInit!: () => void;
    vi.spyOn(SyncedDb.prototype, 'init').mockImplementation(
      () => new Promise<void>((r) => (resolveInit = r))
    );
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const view = render(<Sp00kyProvider config={{} as any}>x</Sp00kyProvider>);
    view.unmount();
    await flush();
    resolveInit();
    await flush();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('useDb outside a provider throws a helpful error', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    expect(() => renderHook(() => useDb(), null)).toThrow(/Sp00kyProvider/);
  });
});

describe('status hooks', () => {
  function statusDb() {
    let healthCb: ((h: any) => void) | undefined;
    let pendingCb: ((n: number) => void) | undefined;
    let fetchCb: ((n: number) => void) | undefined;
    const health = { status: 'healthy', everConnected: false, connection: 'connected' };
    const db = Object.create(SyncedDb.prototype) as any;
    Object.defineProperties(db, {
      syncHealth: { get: () => health },
      pendingMutationCount: { get: () => 0 },
      fetchingQueryCount: { get: () => 0 },
    });
    db.subscribeToSyncHealth = (cb: any) => ((healthCb = cb), cb({ ...health }), () => {});
    db.subscribeToPendingMutations = (cb: any) => ((pendingCb = cb), cb(0), () => {});
    db.subscribeToFetchActivity = (cb: any) => ((fetchCb = cb), cb(0), () => {});
    return {
      db: db as SyncedDb<any>,
      setHealth: (h: any) => act(() => healthCb?.(h)),
      setPending: (n: number) => act(() => pendingCb?.(n)),
      setFetching: (n: number) => act(() => fetchCb?.(n)),
    };
  }

  it('useSyncStatus follows the health subscription, through the provider', async () => {
    let healthCb: ((h: any) => void) | undefined;
    const health = { status: 'healthy', everConnected: false, connection: 'connected' } as any;
    vi.spyOn(SyncedDb.prototype, 'init').mockResolvedValue();
    vi.spyOn(SyncedDb.prototype, 'syncHealth', 'get').mockReturnValue(health);
    vi.spyOn(SyncedDb.prototype, 'subscribeToSyncHealth').mockImplementation((cb) => {
      healthCb = cb;
      cb({ ...health });
      return () => {};
    });
    const { result } = renderHook(
      () => useSyncStatus(),
      null,
      (n) => <Sp00kyProvider config={{} as any}>{n}</Sp00kyProvider>
    );
    await flush();
    expect(result.current.isHealthy).toBe(true);
    const renders = result.renders;
    act(() => healthCb?.({ ...health }));
    expect(result.renders).toBe(renders); // shallow-equal snapshot: no render
    act(() => healthCb?.({ status: 'degraded', everConnected: true, connection: 'reconnecting' }));
    expect(result.current.isOffline).toBe(true);
    expect(result.current.isReconnecting).toBe(true);
  });

  it('useSyncActivity turns on after its delay and off at once', async () => {
    vi.useFakeTimers();
    try {
      const s = statusDb();
      const { result } = renderHook(() => useSyncActivity(s.db, { uploadDelayMs: 100 }), null);
      s.setPending(2);
      expect(result.current.pendingMutations).toBe(2);
      expect(result.current.isUploading).toBe(false);
      act(() => {
        vi.advanceTimersByTime(100);
      });
      expect(result.current.isUploading).toBe(true);
      s.setPending(0);
      expect(result.current.isUploading).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('useSubmission', () => {
  it('tracks pending, result and error, and never rejects', async () => {
    let finish!: (v: string) => void;
    const fn = vi.fn(
      (x: string) =>
        new Promise<string>((resolve, reject) => {
          if (x === 'bad') reject(new Error('local commit failed'));
          else finish = resolve;
        })
    );
    const { result } = renderHook(() => useSubmission(fn), null);
    let p!: Promise<string | undefined>;
    act(() => {
      p = result.current.submit('ok');
    });
    expect(result.current.pending).toBe(true);
    await act(async () => {
      finish('done');
      await p;
    });
    expect(result.current.pending).toBe(false);
    expect(result.current.result).toBe('done');

    await act(async () => {
      await expect(result.current.submit('bad')).resolves.toBeUndefined();
    });
    expect(result.current.error?.message).toBe('local commit failed');
  });
});

describe('useDownloadFile', () => {
  it('leases the shared url and releases it when the path changes or unmounts', async () => {
    const release = vi.fn();
    const url = vi.fn(async (path: string) => ({ url: 'blob:' + path, release }));
    const db = Object.create(SyncedDb.prototype) as any;
    db.bucket = () => ({ url });
    const { result, rerender, unmount } = renderHook(
      (path: string | null) => useDownloadFile(db as SyncedDb<any>, 'files' as never, path),
      'a.png' as string | null
    );
    expect(result.current.isLoading).toBe(true);
    await flush();
    expect(result.current.url).toBe('blob:a.png');

    rerender(null);
    expect(result.current.url).toBeNull();
    expect(release).toHaveBeenCalledTimes(1);

    rerender('b.png');
    await flush();
    expect(result.current.url).toBe('blob:b.png');
    unmount();
    expect(release).toHaveBeenCalledTimes(2);
  });
});
