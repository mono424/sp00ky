import {
  createEnvironmentInjector,
  effect,
  EnvironmentInjector,
  PLATFORM_ID,
  signal,
} from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { describe, expect, it, vi } from 'vitest';
import { injectPreload, injectQuery } from '@spooky-sync/client-angular';
import { inject, mockEngine, mockQuery, provideDb, tick } from './helpers';

describe('injectQuery', () => {
  it('serves [] and isLoading, then live rows with identity kept', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const q = inject(() => injectQuery(mockQuery('h1')));
    expect(q.data()).toEqual([]);
    expect(q.isLoading()).toBe(true);

    await tick();
    eng.emit('h1', [{ id: 'post:a', title: 'A' }]);
    expect(q.data()).toEqual([{ id: 'post:a', title: 'A' }]);
    expect(q.isLoading()).toBe(false);
    expect(q.hasData()).toBe(true);
    expect(q.isAuthoritative()).toBe(false);

    const first = (q.data() as any[])[0];
    eng.emit('h1', [
      { id: 'post:a', title: 'A' },
      { id: 'post:b', title: 'B' },
    ]);
    expect((q.data() as any[])[0]).toBe(first);
    expect(q.data()).toHaveLength(2);
  });

  it('a deep-equal emission does not notify readers', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    let runs = 0;
    const q = inject(() => {
      const query = injectQuery(mockQuery('h1'));
      effect(() => {
        query.data();
        runs++;
      });
      return query;
    });
    await tick();
    eng.emit('h1', [{ id: 'post:a', title: 'A' }]);
    TestBed.tick();
    const after = runs;
    const before = q.data();
    eng.emit('h1', [{ id: 'post:a', title: 'A' }]);
    TestBed.tick();
    expect(q.data()).toBe(before);
    expect(runs).toBe(after);
  });

  it('authority ends a cold load with zero rows as isEmpty, and isSettled follows fetching', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const q = inject(() => injectQuery(mockQuery('h1')));
    await tick();
    eng.setStatus('h1', 'fetching');
    eng.setAuthority('h1', true);
    expect(q.isLoading()).toBe(false);
    expect(q.isEmpty()).toBe(true);
    expect(q.isSettled()).toBe(false);
    eng.setStatus('h1', 'idle');
    expect(q.isSettled()).toBe(true);
  });

  it('a .one() query yields the row or null', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const q = inject(() => injectQuery(mockQuery('h1', true)));
    expect(q.data()).toBeNull();
    await tick();
    eng.emit('h1', [{ id: 'post:a' }]);
    expect(q.data()).toEqual({ id: 'post:a' });
    expect(q.hasData()).toBe(true);
  });

  it('a registration failure surfaces as error, never thrown', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const q = inject(() => injectQuery(mockQuery('h1', false, new Error('503 NOT_READY'))));
    await tick();
    expect(q.error()?.message).toBe('503 NOT_READY');
    expect(q.isLoading()).toBe(false);
  });

  it('follows a signal: re-registers on a new identity, keeps data until it emits', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const hash = signal('h1');
    const q = inject(() => injectQuery(() => mockQuery(hash())));
    await tick();
    eng.emit('h1', [{ id: 'post:a' }]);
    eng.setAuthority('h1', true);
    expect(q.isAuthoritative()).toBe(true);

    hash.set('h2');
    TestBed.tick();
    await tick();
    expect(eng.unsubscribed).toContain('h1');
    expect(eng.hasSub('h2')).toBe(true);
    expect(q.data()).toEqual([{ id: 'post:a' }]);
    expect(q.isAuthoritative()).toBe(false);

    eng.emit('h2', [{ id: 'post:b' }]);
    expect(q.data()).toEqual([{ id: 'post:b' }]);
  });

  it('a rebuild with the same identity does not re-register', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const limit = signal(1);
    const run = vi.fn(async () => ({ hash: 'h1' }));
    inject(() =>
      injectQuery(() => {
        limit();
        return { hash: 'h1', isOne: false, run } as any;
      })
    );
    await tick();
    limit.set(2);
    TestBed.tick();
    await tick();
    expect(run).toHaveBeenCalledTimes(1);
  });

  it('returning null or enabled: false skips the query', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const enabled = signal(false);
    inject(() => injectQuery(() => null));
    inject(() => injectQuery(mockQuery('h2'), { enabled }));
    await tick();
    expect(eng.sp00ky.subscribe).not.toHaveBeenCalled();
    enabled.set(true);
    TestBed.tick();
    await tick();
    expect(eng.hasSub('h2')).toBe(true);
  });

  it('ends with its injector, and deregisters only when asked', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const parent = TestBed.inject(EnvironmentInjector);
    const kept = createEnvironmentInjector([], parent);
    const dropped = createEnvironmentInjector([], parent);
    injectQuery(mockQuery('h1'), { injector: kept });
    injectQuery(mockQuery('h2'), { injector: dropped, deregisterOnCleanup: true });
    TestBed.tick();
    await tick();
    kept.destroy();
    dropped.destroy();
    expect(eng.unsubscribed).toEqual(expect.arrayContaining(['h1', 'h2']));
    expect(eng.deregistered).toEqual(['h2']);
  });

  it('refuses to run outside an injection context without an injector', () => {
    expect(() => injectQuery(mockQuery('h1'))).toThrow(/injection context/);
  });

  it('refuses to run during a server render', () => {
    const eng = mockEngine();
    provideDb(eng.db, [{ provide: PLATFORM_ID, useValue: 'server' }]);
    expect(() => inject(() => injectQuery(mockQuery('h1')))).toThrow(/browser only/);
  });
});

describe('injectPreload', () => {
  it('preloads once per identity', async () => {
    const eng = mockEngine();
    provideDb(eng.db);
    const hash = signal('h1');
    inject(() => injectPreload(() => mockQuery(hash())));
    await tick();
    hash.set('h1');
    TestBed.tick();
    hash.set('h2');
    TestBed.tick();
    await tick();
    expect(eng.preloaded).toEqual(['h1', 'h2']);
  });
});
