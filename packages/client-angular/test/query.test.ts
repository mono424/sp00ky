import {
  Component,
  createEnvironmentInjector,
  effect,
  EnvironmentInjector,
  type OnInit,
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

  it('rows in the first callback, then "not answered yet": not loading', async () => {
    const eng = mockEngine();
    eng.hold('h1', [{ id: 'post:a' }]);
    provideDb(eng.db);
    const q = inject(() => injectQuery(mockQuery('h1')));
    await tick();
    expect(q.data()).toEqual([{ id: 'post:a' }]);
    expect(q.isAuthoritative()).toBe(false);
    expect(q.isLoading()).toBe(false);
    // A flip back (another principal) still takes the paint latch.
    eng.setAuthority('h1', true);
    eng.setAuthority('h1', false);
    expect(q.isLoading()).toBe(true);
  });
});

describe('injectQuery on a client that is not ready yet (eager)', () => {
  const painted = [{ id: 'a', n: 1 }];

  function notReady(rows: (hash: string) => Record<string, any>[] | null = () => painted) {
    const eng = mockEngine();
    eng.notReady(rows);
    provideDb(eng.db);
    return eng;
  }

  it('has the rows the query showed last time on the first read, then the answer', async () => {
    const eng = notReady();
    let runs = 0;
    // Not flushed yet: this is the read a first render makes.
    const q = TestBed.runInInjectionContext(() => {
      const query = injectQuery(mockQuery('h1'));
      effect(() => {
        query.data();
        runs++;
      });
      return query;
    });
    expect(q.data()).toEqual(painted);
    expect(q.isLoading()).toBe(false);
    expect(q.hasData()).toBe(true);
    expect(q.isAuthoritative()).toBe(false);
    expect(q.isSettled()).toBe(false);
    expect(q.isEmpty()).toBe(false);
    const first = q.data();

    TestBed.tick();
    await tick();
    // Attaching, and its "not answered yet", did not swap them for a loading state.
    expect(eng.hasSub('h1')).toBe(true);
    expect(q.isLoading()).toBe(false);
    expect(q.data()).toBe(first);

    // The query answers with the same rows: nothing changes on screen.
    eng.emit('h1', [{ id: 'a', n: 1 }]);
    TestBed.tick();
    expect(q.data()).toBe(first);
    expect(q.isLoading()).toBe(false);
    expect(runs).toBe(1);

    // And with other rows: they win.
    eng.emit('h1', [{ id: 'a', n: 2 }]);
    expect(q.data()).toEqual([{ id: 'a', n: 2 }]);
    expect(q.isAuthoritative()).toBe(false);
  });

  it('keeps them through the first rows of the query itself and its "not answered yet"', async () => {
    const eng = notReady();
    // What the core hands a subscriber once the client is open: the same rows.
    eng.hold('h1', [{ id: 'a', n: 1 }]);
    const q = TestBed.runInInjectionContext(() => injectQuery(mockQuery('h1')));
    const first = q.data();
    TestBed.tick();
    await tick();
    expect(q.data()).toBe(first);
    expect(q.isLoading()).toBe(false);
    expect(q.isAuthoritative()).toBe(false);
  });

  it('a component has them in its first change detection, and the query function is not run early', () => {
    notReady((hash) => (hash === 'h1' ? painted : null));
    const inInit: unknown[] = [];

    @Component({
      selector: 'test-list',
      template: `@if (rows.isLoading()) {
          loading
        } @else {
          @for (row of rows.data(); track row.id) {
            <i>{{ row.n }}</i>
          }
        }`,
    })
    class List implements OnInit {
      // Run here, in the field initializer, this function would throw: it
      // reads a field declared below, as it may read a required input.
      readonly rows = injectQuery(() => mockQuery(this.hash()));
      readonly hash = signal('h1');

      ngOnInit(): void {
        // Earlier than the query's own effect.
        inInit.push(this.rows.data());
      }
    }

    @Component({ imports: [List], template: `<test-list />` })
    class Host {}

    const fixture = TestBed.createComponent(Host);
    fixture.detectChanges();
    expect(inInit).toEqual([painted]);
    expect(fixture.nativeElement.textContent.trim()).toBe('1');
  });

  it('gives a .one() query its row', () => {
    notReady();
    const q = inject(() => injectQuery(mockQuery('h1', true)));
    expect(q.data()).toEqual(painted[0]);
    expect(q.hasData()).toBe(true);
    expect(q.isLoading()).toBe(false);
  });

  it('paints nothing once the client is ready', async () => {
    const eng = notReady();
    eng.sp00ky.isLocalReady.mockReturnValue(true);
    const q = inject(() => injectQuery(mockQuery('h1')));
    await tick();
    expect(q.data()).toEqual([]);
    expect(q.isLoading()).toBe(true);
    expect(eng.sp00ky.peekQuery).not.toHaveBeenCalled();
  });

  it('paints nothing when the store kept nothing, or the peek fails', async () => {
    notReady((hash) => {
      if (hash === 'h1') return null;
      if (hash === 'h2') return [];
      throw new Error('no paint cache');
    });
    for (const hash of ['h1', 'h2', 'h3']) {
      const q = inject(() => injectQuery(mockQuery(hash)));
      await tick();
      expect(q.data()).toEqual([]);
      expect(q.isLoading()).toBe(true);
    }
  });

  it('a query that is skipped paints nothing', () => {
    const eng = notReady();
    const q = inject(() => injectQuery(mockQuery('h1'), { enabled: false }));
    expect(q.hasData()).toBe(false);
    expect(eng.sp00ky.peekQuery).not.toHaveBeenCalled();
  });

  it('isLoading() stays readable while the query function cannot run yet', () => {
    notReady();
    // A required input, read before Angular has set it, throws like this.
    const isSet = signal(false);
    const q = TestBed.runInInjectionContext(() =>
      injectQuery(() => {
        if (!isSet()) throw new Error('NG0950');
        return mockQuery('h1');
      })
    );
    expect(q.isLoading()).toBe(true);
    isSet.set(true);
    expect(q.data()).toEqual(painted);
    expect(q.isLoading()).toBe(false);
  });

  it('paints a new identity from its own rows, and keeps the previous ones when it has none', async () => {
    const other = [{ id: 'b', n: 2 }];
    notReady((hash) => (hash === 'h1' ? painted : hash === 'h2' ? other : null));
    const hash = signal('h1');
    const q = inject(() => injectQuery(() => mockQuery(hash())));
    expect(q.data()).toEqual(painted);

    hash.set('h2');
    expect(q.data()).toEqual(other);
    TestBed.tick();
    await tick();
    expect(q.data()).toEqual(other);
    expect(q.isLoading()).toBe(false);

    // Nothing kept for h3: as on any identity change, the previous rows stay
    // until it emits, and it is a loading state.
    hash.set('h3');
    TestBed.tick();
    await tick();
    expect(q.data()).toEqual(other);
    expect(q.isLoading()).toBe(true);
  });

  it('an emission without rows replaces the painted ones without a loading state', async () => {
    const eng = notReady();
    const q = inject(() => injectQuery(mockQuery('h1')));
    await tick();
    eng.emit('h1', []);
    expect(q.data()).toEqual([]);
    expect(q.isLoading()).toBe(false);
    expect(q.isEmpty()).toBe(false);
    eng.setAuthority('h1', true);
    expect(q.isEmpty()).toBe(true);
  });

  it('a failed init surfaces as error and leaves the painted rows', async () => {
    notReady();
    // What `run()` does on a client whose init failed.
    const q = inject(() => injectQuery(mockQuery('h1', false, new Error('init failed'))));
    await tick();
    expect(q.error()?.message).toBe('init failed');
    expect(q.data()).toEqual(painted);
    expect(q.isLoading()).toBe(false);
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
