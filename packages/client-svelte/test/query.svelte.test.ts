import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import { useQuery, usePreload } from '../src/query.svelte.js';
import { mockEngine, mockQuery, tick } from './helpers.js';

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
