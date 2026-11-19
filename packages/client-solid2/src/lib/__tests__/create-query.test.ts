import { describe, expect, it, vi } from 'vitest';
import { createEffect, createRoot, createSignal, flush } from 'solid-js';
import { createQuery } from '../create-query';
import { SyncedDb } from '../../index';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));
const settle = async () => {
  await tick();
  flush();
  await tick();
};

type Emission = Record<string, any>[];

function mockEngine() {
  const subs = new Map<string, (e: Emission) => void>();
  const statusSubs = new Map<string, (s: string) => void>();
  const authoritySubs = new Map<string, (known: boolean) => void>();
  // Rows the core already holds for a query: an immediate subscriber gets them at once.
  const held = new Map<string, Emission>();
  const unsubscribed: string[] = [];
  const deregistered: string[] = [];
  const sp00ky = {
    subscribeQueryAuthority: vi.fn(
      (hash: string, cb: (known: boolean) => void, o?: { immediate?: boolean }) => {
        authoritySubs.set(hash, cb);
        if (o?.immediate) cb(false);
        return () => authoritySubs.delete(hash);
      }
    ),
    subscribe: vi.fn(async (hash: string, cb: (e: Emission) => void, o?: { immediate?: boolean }) => {
      subs.set(hash, cb);
      if (o?.immediate && held.has(hash)) cb(held.get(hash)!);
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
    deregisterQuery: vi.fn((hash: string) => deregistered.push(hash)),
    reportFrontendTiming: vi.fn(),
  };
  const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
  (db as any).getSp00ky = () => sp00ky;
  return {
    db,
    sp00ky,
    held,
    emit: (hash: string, e: Emission) => subs.get(hash)?.(e),
    setStatus: (hash: string, s: string) => statusSubs.get(hash)?.(s),
    setAuthority: (hash: string, known: boolean) => authoritySubs.get(hash)?.(known),
    unsubscribed,
    deregistered,
    hasSub: (hash: string) => subs.has(hash),
  };
}

function mockQuery(hash: string, isOne = false) {
  return {
    hash,
    isOne,
    run: async () => ({ hash }),
  } as any;
}

/**
 * The client an eager provider hands out before `init()` is done: not ready,
 * with `painted(hash)` as the rows its store kept from the last session.
 */
function eagerEngine(painted: (hash: string) => Emission | null = () => [{ id: 'a', n: 1 }]) {
  const eng = mockEngine();
  const peekQuery = vi.fn((q: { hash: string }) => painted(q.hash));
  Object.assign(eng.sp00ky, { isLocalReady: () => false, peekQuery });
  // `SyncedDb.peekQuery` itself runs: it only answers on an eager instance.
  (eng.db as any).eager = true;
  return { ...eng, peekQuery };
}

describe('createQuery', () => {
  it('serves empty data immediately, then live emissions with row identity kept', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();

      expect(q.data()).toEqual([]); // committed empty, no suspension
      expect(q.isLoading()).toBe(true);

      await settle();
      eng.emit('h1', [
        { id: 'a', n: 1 },
        { id: 'b', n: 2 },
      ]);
      await settle();

      expect(q.data().map((r: any) => r.id)).toEqual(['a', 'b']);
      expect(q.isLoading()).toBe(false);
      // Rows alone do not settle a query; the server's membership does.
      expect(q.isSettled()).toBe(false);
      eng.setAuthority('h1', true);
      await settle();
      expect(q.isSettled()).toBe(true);

      const rowA = q.data()[0];
      eng.emit('h1', [
        { id: 'a', n: 99 },
        { id: 'b', n: 2 },
      ]);
      await settle();
      expect(q.data()[0]).toBe(rowA); // identity preserved via keyed reconcile
      expect(q.data()[0].n).toBe(99);

      dispose();
      await settle();
      expect(eng.unsubscribed).toContain('h1');
    });
  });

  it('an empty emission before the server answers keeps loading', async () => {
    // Nothing cached and nothing heard from the server: the only honest state
    // is "loading". A second `[]` does not change that; only authority does.
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      eng.emit('h1', []);
      eng.emit('h1', []);
      await settle();
      expect(q.isLoading()).toBe(true);
      expect(q.hasData()).toBe(false);
      expect(q.isEmpty()).toBe(false);
      expect(q.isAuthoritative()).toBe(false);
      dispose();
    });
  });

  it('the server answering with no rows ends loading as empty', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      eng.emit('h1', []);
      eng.setAuthority('h1', true);
      await settle();
      expect(q.isLoading()).toBe(false);
      expect(q.isEmpty()).toBe(true);
      expect(q.isAuthoritative()).toBe(true);
      expect(q.data()).toEqual([]);
      dispose();
    });
  });

  it('cached rows end loading before the server answers, without settling', async () => {
    // Local-first paint: rows from the store are shown at once. They are not
    // authoritative yet, so a windowed list must not read them as the end.
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      eng.emit('h1', [{ id: 'a' }]);
      await settle();
      expect(q.isLoading()).toBe(false);
      expect(q.hasData()).toBe(true);
      expect(q.isAuthoritative()).toBe(false);
      expect(q.isSettled()).toBe(false);
      expect(q.isEmpty()).toBe(false);

      eng.setAuthority('h1', true);
      await settle();
      expect(q.isSettled()).toBe(true);
      dispose();
    });
  });

  it('a one() query the server answered as absent is empty, not loading', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, true, any>(eng.db, mockQuery('h1', true));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      eng.emit('h1', []);
      eng.setAuthority('h1', true);
      await settle();
      expect(q.isLoading()).toBe(false);
      expect(q.isEmpty()).toBe(true);
      expect(q.data()).toBeNull();
      dispose();
    });
  });

  it('losing authority with no rows (a bucket switch) returns to loading', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      eng.emit('h1', [{ id: 'a' }]);
      eng.setAuthority('h1', true);
      await settle();
      expect(q.isLoading()).toBe(false);

      // The rebind empties the rows and resets authority for the new principal.
      eng.emit('h1', []);
      eng.setAuthority('h1', false);
      await settle();
      expect(q.isLoading()).toBe(true);
      expect(q.isEmpty()).toBe(false);
      dispose();
    });
  });

  it('one() queries yield the row or null', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, true, any>(eng.db, mockQuery('h1', true));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      expect(q.data()).toBe(null);
      eng.emit('h1', [{ id: 'a', n: 1 }]);
      await settle();
      expect(q.data()?.n).toBe(1);
      expect(q.isLoading()).toBe(false);
      dispose();
    });
  });

  it('isFetching mirrors query status; isSettled composes both', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      eng.setStatus('h1', 'fetching');
      flush();
      expect(q.isFetching()).toBe(true);

      eng.emit('h1', [{ id: 'a' }]);
      eng.setAuthority('h1', true);
      await settle();
      expect(q.isSettled()).toBe(false); // authoritative but still fetching

      eng.setStatus('h1', 'idle');
      flush();
      expect(q.isSettled()).toBe(true);
      dispose();
    });
  });

  it('registration failure surfaces via error(), resolving isLoading', async () => {
    const eng = mockEngine();
    const failing = {
      hash: 'hx',
      isOne: false,
      run: async () => {
        throw new Error('SSP NOT_READY');
      },
    } as any;
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, failing);
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      expect(q.error()?.message).toBe('SSP NOT_READY');
      expect(q.isLoading()).toBe(false); // spinner must resolve
      expect(q.data()).toEqual([]);
      dispose();
    });
  });

  it('reactive query thunk: identity change re-subscribes and clears state', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const [id, setId] = createSignal('h1');
      const q = createQuery<any, any, any, any, false, any>(eng.db, () => mockQuery(id()));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      eng.emit('h1', [{ id: 'a' }]);
      await settle();
      expect(q.data().map((r: any) => r.id)).toEqual(['a']);

      setId('h2');
      flush();
      await settle();
      await settle();

      expect(eng.unsubscribed).toContain('h1'); // superseded subscription torn down
      expect(eng.hasSub('h2')).toBe(true);
      expect(q.isLoading()).toBe(true); // reset for the new identity

      eng.emit('h2', [{ id: 'z' }]);
      await settle();
      expect(q.data().map((r: any) => r.id)).toEqual(['z']);
      dispose();
    });
  });

  it('enabled=false runs no query; flipping true starts it', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const [enabled, setEnabled] = createSignal(false);
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'), {
        enabled,
      });
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();
      expect(eng.sp00ky.subscribe).not.toHaveBeenCalled();

      setEnabled(true);
      flush();
      await settle();
      expect(eng.hasSub('h1')).toBe(true);
      dispose();
    });
  });

  it('deregisterOnCleanup deregisters the active hash on dispose', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'), {
        deregisterOnCleanup: true,
      });
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();
      expect(eng.hasSub('h1')).toBe(true);

      dispose();
      await settle();
      expect(eng.unsubscribed).toContain('h1');
      expect(eng.deregistered).toEqual(['h1']);
    });
  });

  it('reports frontend timing per emission', async () => {
    const eng = mockEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();
      eng.emit('h1', [{ id: 'a' }]);
      await settle();
      expect(eng.sp00ky.reportFrontendTiming).toHaveBeenCalledWith('h1', expect.any(Number));
      dispose();
    });
  });
});

describe('createQuery on a client that is not ready yet (an eager provider)', () => {
  it('paints the rows the query showed last time on the first read, then the answer', async () => {
    const eng = eagerEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      // The very first read, before any effect has run.
      expect(q.data()).toEqual([{ id: 'a', n: 1 }]);
      expect(q.isLoading()).toBe(false);
      expect(q.hasData()).toBe(true);
      expect(q.isAuthoritative()).toBe(false);
      expect(q.isSettled()).toBe(false);
      expect(q.isEmpty()).toBe(false);

      let reads = 0;
      createEffect(
        () => {
          reads++;
          return q.data().map((r: any) => r.n);
        },
        () => {}
      );
      flush();
      const rowA = q.data()[0];
      await settle();
      // Attaching did not swap them out for a loading state, nor read them twice.
      expect(q.isLoading()).toBe(false);
      expect(q.data()[0]).toBe(rowA);
      expect(eng.peekQuery).toHaveBeenCalledTimes(1);
      expect(eng.hasSub('h1')).toBe(true);
      expect(reads).toBe(1);

      // The query answers with the same rows: nothing changes on screen.
      eng.emit('h1', [{ id: 'a', n: 1 }]);
      await settle();
      expect(q.data()).toEqual([{ id: 'a', n: 1 }]);
      expect(q.data()[0]).toBe(rowA);
      expect(reads).toBe(1);
      expect(q.isLoading()).toBe(false);
      expect(q.isAuthoritative()).toBe(false);

      // And with other rows: they win.
      eng.emit('h1', [
        { id: 'a', n: 2 },
        { id: 'b', n: 3 },
      ]);
      eng.setAuthority('h1', true);
      await settle();
      expect(q.data()).toEqual([
        { id: 'a', n: 2 },
        { id: 'b', n: 3 },
      ]);
      expect(q.data()[0]).toBe(rowA);
      expect(q.isSettled()).toBe(true);
      dispose();
    });
  });

  it('a one() query is painted with its row', async () => {
    const eng = eagerEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, true, any>(eng.db, mockQuery('h1', true));
      expect(q.data()).toEqual({ id: 'a', n: 1 });
      expect(q.isLoading()).toBe(false);
      expect(q.hasData()).toBe(true);
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();
      eng.emit('h1', [{ id: 'a', n: 2 }]);
      await settle();
      expect(q.data()?.n).toBe(2);
      dispose();
    });
  });

  it('paints nothing once the client is ready, when the store kept nothing, or when the peek fails', async () => {
    const cold = async (eng: ReturnType<typeof eagerEngine>) => {
      await createRoot(async (dispose) => {
        const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
        expect(q.data()).toEqual([]);
        expect(q.isLoading()).toBe(true);
        createEffect(
          () => q.data(),
          () => {}
        );
        flush();
        await settle();
        expect(q.data()).toEqual([]);
        expect(q.isLoading()).toBe(true);
        expect(eng.hasSub('h1')).toBe(true);
        dispose();
      });
    };

    const ready = eagerEngine();
    (ready.sp00ky as any).isLocalReady = () => true;
    await cold(ready);
    // The core hands the same rows to the first subscribe callback then.
    expect(ready.peekQuery).not.toHaveBeenCalled();

    await cold(eagerEngine(() => null));
    await cold(eagerEngine(() => []));
    await cold(
      eagerEngine(() => {
        throw new Error('storage');
      })
    );

    // Not an eager instance: `SyncedDb.peekQuery` answers nothing.
    const plain = eagerEngine();
    (plain.db as any).eager = false;
    await cold(plain);
    expect(plain.peekQuery).not.toHaveBeenCalled();
  });

  it('a disabled query paints nothing', async () => {
    const eng = eagerEngine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'), {
        enabled: () => false,
      });
      expect(q.data()).toEqual([]);
      expect(q.hasData()).toBe(false);
      expect(eng.peekQuery).not.toHaveBeenCalled();
      dispose();
    });
  });

  it('a new identity, while the client is still not ready, is painted from its own rows', async () => {
    const kept: Record<string, Emission> = { h1: [{ id: 'a' }], h2: [{ id: 'z' }] };
    const eng = eagerEngine((hash) => kept[hash] ?? null);
    await createRoot(async (dispose) => {
      const [id, setId] = createSignal('h1');
      const q = createQuery<any, any, any, any, false, any>(eng.db, () => mockQuery(id()));
      expect(q.data().map((r: any) => r.id)).toEqual(['a']);
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      await settle();

      setId('h2');
      flush();
      expect(q.data().map((r: any) => r.id)).toEqual(['z']);
      expect(q.isLoading()).toBe(false);
      expect(q.isAuthoritative()).toBe(false);

      // Nothing kept for this one: a cold load, as on a ready client.
      setId('h3');
      flush();
      expect(q.isLoading()).toBe(true);
      dispose();
    });
  });
});

describe('createQuery on rows the core already holds', () => {
  it('stays out of loading when "not answered yet" follows the first rows', async () => {
    // A fresh subscription delivers the rows the core holds (its store's copy
    // from the last session), then the authority subscription its immediate
    // `false`. That `false` is not a flip back and must not unpaint them.
    const eng = mockEngine();
    eng.held.set('h1', [{ id: 'a' }]);
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(eng.db, mockQuery('h1'));
      createEffect(
        () => q.data(),
        () => {}
      );
      flush();
      expect(q.isLoading()).toBe(true);
      await settle();

      expect(q.data().map((r: any) => r.id)).toEqual(['a']);
      expect(q.isLoading()).toBe(false);
      expect(q.hasData()).toBe(true);
      expect(q.isAuthoritative()).toBe(false);
      expect(q.isSettled()).toBe(false);

      // A flip back (another principal) still takes the latch.
      eng.setAuthority('h1', true);
      await settle();
      eng.emit('h1', []);
      eng.setAuthority('h1', false);
      await settle();
      expect(q.isLoading()).toBe(true);
      dispose();
    });
  });
});
