import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import { useQuery, usePreload } from '../src/query.svelte.js';
import { eagerEngine, mockEngine, mockQuery, tick } from './helpers.js';

/** Run `fn` in an effect root, like a component's init. Returns the result and a destroy. */
function root<T>(fn: () => T): { value: T; destroy: () => void } {
  let value!: T;
  const destroy = $effect.root(() => {
    value = fn();
  });
  flushSync();
  return { value, destroy };
}

describe('useQuery', () => {
  it('serves [] and isLoading, then live rows with identity kept', async () => {
    const eng = mockEngine();
    const { value: q, destroy } = root(() => useQuery(eng.db, mockQuery('h1')));
    expect(q.data).toEqual([]);
    expect(q.isLoading).toBe(true);

    await tick();
    eng.emit('h1', [{ id: 'post:a', title: 'A' }]);
    expect(q.data).toEqual([{ id: 'post:a', title: 'A' }]);
    expect(q.isLoading).toBe(false);
    expect(q.hasData).toBe(true);
    expect(q.isAuthoritative).toBe(false);

    const first = (q.data as any[])[0];
    eng.emit('h1', [
      { id: 'post:a', title: 'A' },
      { id: 'post:b', title: 'B' },
    ]);
    expect((q.data as any[])[0]).toBe(first);
    expect(q.data).toHaveLength(2);
    destroy();
  });

  it('a deep-equal emission does not re-run readers', async () => {
    const eng = mockEngine();
    let runs = 0;
    const destroy = $effect.root(() => {
      const q = useQuery(eng.db, mockQuery('h1'));
      $effect(() => {
        void q.data;
        runs++;
      });
    });
    flushSync();
    await tick();
    eng.emit('h1', [{ id: 'post:a', title: 'A' }]);
    flushSync();
    const after = runs;
    eng.emit('h1', [{ id: 'post:a', title: 'A' }]);
    flushSync();
    expect(runs).toBe(after);
    destroy();
  });

  it('authority ends a cold load with zero rows as isEmpty, and isSettled follows fetching', async () => {
    const eng = mockEngine();
    const { value: q, destroy } = root(() => useQuery(eng.db, mockQuery('h1')));
    await tick();
    eng.setStatus('h1', 'fetching');
    eng.setAuthority('h1', true);
    expect(q.isLoading).toBe(false);
    expect(q.isEmpty).toBe(true);
    expect(q.isSettled).toBe(false);
    eng.setStatus('h1', 'idle');
    expect(q.isSettled).toBe(true);
    destroy();
  });

  it('a .one() query yields the row or null', async () => {
    const eng = mockEngine();
    const { value: q, destroy } = root(() => useQuery(eng.db, mockQuery('h1', true)));
    expect(q.data).toBeNull();
    await tick();
    eng.emit('h1', [{ id: 'post:a' }]);
    expect(q.data).toEqual({ id: 'post:a' });
    destroy();
  });

  it('re-registers only on identity change; old data stays while flags reset', async () => {
    const eng = mockEngine();
    let limit = $state(20);
    let rebuilds = 0;
    const { value: q, destroy } = root(() =>
      useQuery(eng.db, () => {
        rebuilds++;
        return mockQuery('h' + limit);
      })
    );
    await tick();
    eng.emit('h20', [{ id: 'post:a' }]);
    eng.setAuthority('h20', true);
    expect(q.isAuthoritative).toBe(true);

    limit = 40;
    flushSync();
    expect(q.isAuthoritative).toBe(false);
    expect(q.data).toEqual([{ id: 'post:a' }]);
    await tick();
    expect(eng.unsubscribed).toContain('h20');
    expect(eng.hasSub('h40')).toBe(true);
    expect(eng.sp00ky.subscribe).toHaveBeenCalledTimes(2);
    expect(rebuilds).toBeGreaterThan(1);
    destroy();
  });

  it('skips a null query and an enabled=false gate', async () => {
    const eng = mockEngine();
    let enabled = $state(false);
    const { value: q, destroy } = root(() =>
      useQuery(eng.db, mockQuery('h1'), { enabled: () => enabled })
    );
    await tick();
    expect(eng.sp00ky.subscribe).not.toHaveBeenCalled();
    expect(q.isLoading).toBe(true);
    enabled = true;
    flushSync();
    await tick();
    expect(eng.hasSub('h1')).toBe(true);
    destroy();

    const { destroy: d2 } = root(() => useQuery(eng.db, () => null));
    await tick();
    expect(eng.sp00ky.subscribe).toHaveBeenCalledTimes(1);
    d2();
  });

  it('surfaces a registration failure as error, never throws', async () => {
    const eng = mockEngine();
    const { value: q, destroy } = root(() =>
      useQuery(eng.db, mockQuery('h1', false, new Error('503 NOT_READY')))
    );
    await tick();
    expect(q.error?.message).toBe('503 NOT_READY');
    expect(q.isLoading).toBe(false);
    destroy();
  });

  it('unsubscribes on destroy, and deregisters only when asked', async () => {
    const eng = mockEngine();
    const a = root(() => useQuery(eng.db, mockQuery('h1')));
    const b = root(() => useQuery(eng.db, mockQuery('h2'), { deregisterOnCleanup: true }));
    await tick();
    a.destroy();
    b.destroy();
    expect(eng.unsubscribed).toEqual(['h1', 'h2']);
    expect(eng.deregistered).toEqual(['h2']);
  });

  it('keeps isLoading false when the first subscribe hands over rows, then authority says not answered yet', async () => {
    const eng = mockEngine();
    eng.hold('h1', [{ id: 'post:a', n: 1 }]);
    const { value: q, destroy } = root(() => useQuery(eng.db, mockQuery('h1')));
    await tick();
    // What a fresh subscription delivers: its rows, then authority `false`.
    expect(eng.sp00ky.subscribeQueryAuthority).toHaveBeenCalledOnce();
    expect(q.data).toEqual([{ id: 'post:a', n: 1 }]);
    expect(q.isAuthoritative).toBe(false);
    expect(q.isLoading).toBe(false);
    // A flip back (another principal) still takes the paint latch.
    eng.setAuthority('h1', true);
    expect(q.isSettled).toBe(true);
    eng.setAuthority('h1', false);
    expect(q.isLoading).toBe(true);
    destroy();
  });
});

describe('useQuery on a client that is not ready yet (an eager provider)', () => {
  const painted = [{ id: 'post:a', n: 1 }];

  it('serves the rows the query showed last time on the first read, then the answer', async () => {
    const eng = eagerEngine(painted);
    // Like the core: `run()` waits for the client to open.
    let open!: () => void;
    const opened = new Promise<void>((r) => (open = r));
    const query = { ...mockQuery('h1'), run: () => opened.then(() => ({ hash: 'h1' })) };
    let q!: ReturnType<typeof useQuery>;
    let first: unknown;
    let renders = 0;
    const destroy = $effect.root(() => {
      q = useQuery(eng.db, query);
      // Before any effect has run: what the markup reads while it renders.
      first = q.data;
      expect(q.isLoading).toBe(false);
      $effect(() => {
        void q.data;
        void q.isLoading;
        renders++;
      });
    });
    expect(first).toEqual(painted);
    expect(q.hasData).toBe(true);
    expect(q.isAuthoritative).toBe(false);
    expect(q.isSettled).toBe(false);

    // Attaching did not swap them out for a loading state, and neither does
    // waiting for the client.
    flushSync();
    await tick();
    expect(eng.sp00ky.subscribe).not.toHaveBeenCalled();
    expect(q.isLoading).toBe(false);
    expect(q.data).toBe(first);

    // The client opens and the query answers with the same rows: nothing
    // changes on screen.
    eng.hold('h1', [{ id: 'post:a', n: 1 }]);
    open();
    await tick();
    flushSync();
    expect(eng.hasSub('h1')).toBe(true);
    expect(q.data).toBe(first);
    expect(q.isLoading).toBe(false);
    expect(renders).toBe(1);
    // And with other rows: they win.
    eng.emit('h1', [{ id: 'post:a', n: 2 }]);
    expect(q.data).toEqual([{ id: 'post:a', n: 2 }]);
    destroy();
  });

  it('gives a .one() query its row', () => {
    const eng = eagerEngine(painted);
    const { value: q, destroy } = root(() => useQuery(eng.db, mockQuery('h1', true)));
    expect(q.data).toEqual(painted[0]);
    expect(q.isLoading).toBe(false);
    destroy();
  });

  it('paints the new identity when the query changes before the client is ready', () => {
    const eng = eagerEngine(null);
    eng.sp00ky.peekQuery.mockImplementation((query) =>
      query.hash === 'h1'
        ? [{ id: 'post:a', n: 1 }]
        : query.hash === 'h2'
          ? [{ id: 'post:b', n: 2 }]
          : null
    );
    let page = $state(1);
    const { value: q, destroy } = root(() => useQuery(eng.db, () => mockQuery('h' + page)));
    expect(q.data).toEqual([{ id: 'post:a', n: 1 }]);
    page = 2;
    flushSync();
    expect(q.data).toEqual([{ id: 'post:b', n: 2 }]);
    expect(q.isLoading).toBe(false);
    // Nothing kept for this one: the previous rows stay, as a loading state.
    page = 3;
    flushSync();
    expect(q.data).toEqual([{ id: 'post:b', n: 2 }]);
    expect(q.isLoading).toBe(true);
    destroy();
  });

  it('paints nothing once the client is ready, when the store kept nothing, or without eager', async () => {
    const cold = async (eng: ReturnType<typeof mockEngine>) => {
      const { value: q, destroy } = root(() => useQuery(eng.db, mockQuery('h1')));
      await tick();
      expect(q.data).toEqual([]);
      expect(q.isLoading).toBe(true);
      destroy();
    };

    const ready = eagerEngine(painted);
    ready.sp00ky.isLocalReady.mockReturnValue(true);
    await cold(ready);
    expect(ready.sp00ky.peekQuery).not.toHaveBeenCalled();

    await cold(eagerEngine(null));
    await cold(eagerEngine([]));

    // A peek that fails paints nothing either.
    const broken = eagerEngine(painted);
    broken.sp00ky.peekQuery.mockImplementation(() => {
      throw new Error('paint cache unreadable');
    });
    await cold(broken);

    // Not eager: `SyncedDb.peekQuery` never asks the core.
    const plain = eagerEngine(painted);
    (plain.db as any).eager = false;
    await cold(plain);
    expect(plain.sp00ky.peekQuery).not.toHaveBeenCalled();
  });
});

describe('usePreload', () => {
  it('preloads once per identity', async () => {
    const eng = mockEngine();
    let id = $state('a');
    let other = $state(0);
    const { destroy } = root(() =>
      usePreload(eng.db, () => {
        void other;
        return mockQuery('p' + id);
      })
    );
    other = 1;
    flushSync();
    id = 'b';
    flushSync();
    await tick();
    expect(eng.preloaded).toEqual(['pa', 'pb']);
    destroy();
  });
});
