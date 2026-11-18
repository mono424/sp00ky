import { afterEach, describe, expect, it, vi } from 'vitest';
import { createComponent, createRoot, createSignal } from 'solid-js';

// `new Sp00kyClient(config)` builds the whole engine. These tests only need to
// see when the binding creates one and what it asks of it.
const core = vi.hoisted(() => {
  class FakeClient {
    static made: FakeClient[] = [];
    ready = false;
    private settle!: () => void;
    private opened = new Promise<void>((resolve) => (this.settle = resolve));
    constructor(readonly config: unknown) {
      FakeClient.made.push(this);
    }
    async init() {
      this.ready = true;
      this.settle();
    }
    whenReady() {
      return this.opened;
    }
    peekQuery = vi.fn((): Record<string, unknown>[] | null => [{ id: 'a' }]);
    create = vi.fn(async () => ({}));
    close = vi.fn(async () => {});
  }
  return { FakeClient };
});
vi.mock('@spooky-sync/core', async (original) => ({
  ...(await original<typeof import('@spooky-sync/core')>()),
  Sp00kyClient: core.FakeClient,
}));

import { SyncedDb, Sp00kyProvider, useDb, useQuery } from '../../index';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));
const settle = async () => {
  await tick();
  await tick();
};

afterEach(() => {
  core.FakeClient.made.length = 0;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

type Emission = Record<string, any>[];

/**
 * A core client as `useQuery` sees it. `ready: false` is a client that is not
 * initialized yet, `painted` is what its store kept for the query, `initial`
 * is what the first, immediate subscribe callback delivers.
 */
function mockEngine(opts: { ready?: boolean; painted?: Emission | null; initial?: Emission } = {}) {
  const subs = new Map<string, (e: Emission) => void>();
  const statusSubs = new Map<string, (s: string) => void>();
  const authoritySubs = new Map<string, (known: boolean) => void>();
  const sp00ky = {
    isLocalReady: () => opts.ready ?? true,
    peekQuery: vi.fn((_query: { hash: string }): Emission | null => opts.painted ?? null),
    subscribe: vi.fn(
      async (hash: string, cb: (e: Emission) => void, o?: { immediate?: boolean }) => {
        subs.set(hash, cb);
        if (o?.immediate && opts.initial) cb(opts.initial);
        return () => subs.delete(hash);
      }
    ),
    subscribeQueryStatus: vi.fn(
      (hash: string, cb: (s: string) => void, o?: { immediate?: boolean }) => {
        statusSubs.set(hash, cb);
        if (o?.immediate) cb('fetching');
        return () => statusSubs.delete(hash);
      }
    ),
    subscribeQueryAuthority: vi.fn(
      (hash: string, cb: (known: boolean) => void, o?: { immediate?: boolean }) => {
        authoritySubs.set(hash, cb);
        if (o?.immediate) cb(false);
        return () => authoritySubs.delete(hash);
      }
    ),
    deregisterQuery: vi.fn(),
    reportFrontendTiming: vi.fn(),
  };
  const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
  (db as any).getSp00ky = () => sp00ky;
  // What `new SyncedDb(config, { eager: true })` is before `init()` is done.
  (db as any).eager = true;
  return {
    db,
    sp00ky,
    emit: (hash: string, e: Emission) => subs.get(hash)?.(e),
    setStatus: (hash: string, s: string) => statusSubs.get(hash)?.(s),
    setAuthority: (hash: string, known: boolean) => authoritySubs.get(hash)?.(known),
  };
}

function mockQuery(hash: string, isOne = false) {
  return { hash, isOne, run: async () => ({ hash }) } as any;
}

type Query = ReturnType<typeof useQuery<any, any, any, any, boolean, any>>;

/**
 * Create a query in a root. `first` sees what the first render reads: it runs
 * before any effect has (they run as the root is done being set up).
 */
function mountQuery(make: () => Query, first?: (q: Query) => void) {
  return createRoot((dispose) => {
    const q = make();
    first?.(q);
    return { q, dispose };
  });
}

describe('useQuery on a client that is not ready yet (an eager provider)', () => {
  const painted = [{ id: 'a', n: 1 }];

  it('has the rows the query showed last time on the first read, then the answer', async () => {
    const eng = mockEngine({ ready: false, painted });
    let row: unknown;
    const { q, dispose } = mountQuery(
      () => useQuery(eng.db, mockQuery('h1')),
      (q) => {
        expect(q.data()).toEqual(painted);
        expect(q.isLoading()).toBe(false);
        expect(q.isSettled()).toBe(false);
        row = q.data()[0];
      }
    );

    await settle();
    // Subscribing did not swap them for a loading state, nor peek again.
    expect(eng.sp00ky.subscribe).toHaveBeenCalledTimes(1);
    expect(eng.sp00ky.peekQuery).toHaveBeenCalledTimes(1);
    expect(q.isLoading()).toBe(false);
    expect(q.isSettled()).toBe(false);
    expect(q.data()[0]).toBe(row);

    // The query answers with the same rows: the row on screen is kept.
    eng.emit('h1', [{ id: 'a', n: 1 }]);
    expect(q.data()[0]).toBe(row);
    expect(q.data()).toEqual(painted);
    // And with other rows: they win.
    eng.emit('h1', [{ id: 'b', n: 2 }]);
    expect(q.data()).toEqual([{ id: 'b', n: 2 }]);
    expect(q.isLoading()).toBe(false);
    eng.setStatus('h1', 'idle');
    expect(q.isSettled()).toBe(true);
    dispose();
  });

  it('gives a `.one()` query its row', () => {
    const eng = mockEngine({ ready: false, painted });
    const { dispose } = mountQuery(
      () => useQuery(eng.db, mockQuery('h1', true)),
      (q) => {
        expect(q.data()).toEqual(painted[0]);
        expect(q.isLoading()).toBe(false);
      }
    );
    dispose();
  });

  it('paints nothing once the client is ready, when the store kept nothing, or when the peek fails', async () => {
    const ready = mockEngine({ ready: true, painted });
    const empty = mockEngine({ ready: false, painted: null });
    const failing = mockEngine({ ready: false, painted });
    failing.sp00ky.peekQuery.mockImplementation(() => {
      throw new Error('storage');
    });
    // Not an eager instance: `SyncedDb.peekQuery` answers null.
    const plain = mockEngine({ ready: false, painted });
    (plain.db as any).eager = false;

    for (const eng of [ready, empty, failing, plain]) {
      const loading = (q: Query) => {
        expect(q.data()).toBeUndefined();
        expect(q.isLoading()).toBe(true);
      };
      const { q, dispose } = mountQuery(() => useQuery(eng.db, mockQuery('h1')), loading);
      await settle();
      loading(q);
      expect(eng.sp00ky.subscribe).toHaveBeenCalledTimes(1);
      dispose();
    }
    expect(ready.sp00ky.peekQuery).not.toHaveBeenCalled();
    expect(plain.sp00ky.peekQuery).not.toHaveBeenCalled();
  });

  it('paints the new identity when the query changes before the client is ready', async () => {
    const eng = mockEngine({ ready: false });
    eng.sp00ky.peekQuery.mockImplementation((query) =>
      query.hash === 'h1' ? painted : query.hash === 'h2' ? [{ id: 'b', n: 2 }] : null
    );
    const [hash, setHash] = createSignal('h1');
    const { q, dispose } = mountQuery(() => useQuery(eng.db, () => mockQuery(hash())));
    expect(q.data()).toEqual(painted);
    await settle();

    setHash('h2');
    expect(q.data()).toEqual([{ id: 'b', n: 2 }]);
    expect(q.isLoading()).toBe(false);
    expect(q.isSettled()).toBe(false);

    // Nothing kept for this one: it loads, as any new identity does.
    setHash('h3');
    expect(q.isLoading()).toBe(true);
    dispose();
  });

  it('a disabled query paints nothing', () => {
    const eng = mockEngine({ ready: false, painted });
    const { dispose } = mountQuery(
      () => useQuery(eng.db, mockQuery('h1'), { enabled: () => false }),
      (q) => expect(q.data()).toBeUndefined()
    );
    expect(eng.sp00ky.peekQuery).not.toHaveBeenCalled();
    dispose();
  });
});

describe('useQuery when the first subscribe callback already has rows', () => {
  it('is not loading after the authority subscription says "not answered yet"', async () => {
    // What the core delivers for a query whose rows the store kept: rows in
    // the immediate subscribe callback, then authority's immediate `false`.
    const eng = mockEngine({ ready: true, initial: [{ id: 'a', n: 1 }] });
    const { q, dispose } = mountQuery(() => useQuery(eng.db, mockQuery('h1')));
    expect(q.isLoading()).toBe(true);
    await settle();
    expect(eng.sp00ky.subscribeQueryAuthority).toHaveBeenCalledTimes(1);
    expect(q.data()).toEqual([{ id: 'a', n: 1 }]);
    expect(q.isLoading()).toBe(false);
    // Said again later, it still takes nothing from rows that are on screen.
    eng.setAuthority('h1', false);
    expect(q.isLoading()).toBe(false);
    dispose();
  });
});

describe('SyncedDb', () => {
  const config = { database: {} } as any;
  const query = mockQuery('h1');

  it('throws before init, as it always did, unless it is eager', async () => {
    const db = new SyncedDb<any>(config);
    expect(() => db.getSp00ky()).toThrow('SyncedDb not initialized');
    expect(() => db.syncHealth).toThrow('SyncedDb not initialized');
    await expect(db.create('thing:1', {})).rejects.toThrow('SyncedDb not initialized');
    expect(db.isReady).toBe(false);
    // Never a peek on an instance the tree only gets once it is ready.
    expect(db.peekQuery(query)).toBeNull();
    expect(core.FakeClient.made).toHaveLength(0);

    await db.init();
    expect(db.isReady).toBe(true);
    expect(db.peekQuery(query)).toBeNull();
    expect(core.FakeClient.made[0].peekQuery).not.toHaveBeenCalled();
  });

  it('eager: the core client comes with its first use and init() opens that one', async () => {
    const db = new SyncedDb<any>(config, { eager: true });
    expect(core.FakeClient.made).toHaveLength(0);
    const client = db.getSp00ky() as unknown as InstanceType<typeof core.FakeClient>;
    expect(core.FakeClient.made).toHaveLength(1);
    expect(client.ready).toBe(false);
    expect(db.isReady).toBe(false);

    expect(db.peekQuery(query)).toEqual([{ id: 'a' }]);
    await db.create('thing:1', { a: 1 });
    expect(client.create).toHaveBeenCalledWith('thing:1', { a: 1 });

    let ready = false;
    void db.whenReady().then(() => (ready = true));
    await settle();
    expect(ready).toBe(false);

    await db.init();
    await settle();
    expect(core.FakeClient.made).toHaveLength(1);
    expect(db.getSp00ky()).toBe(client);
    expect(client.ready).toBe(true);
    expect(db.isReady).toBe(true);
    expect(ready).toBe(true);
    // From here on a query's own first value carries the rows.
    expect(db.peekQuery(query)).toBeNull();
  });
});

describe('Sp00kyProvider', () => {
  /** What a JSX expression would end up rendering. */
  const resolve = (value: unknown): unknown => {
    while (typeof value === 'function') value = (value as () => unknown)();
    return value;
  };

  function mount(props: { eager?: boolean; onReady?: () => void; preload?: () => Promise<void> }) {
    const seen: { db?: SyncedDb<any> } = {};
    const Child = () => {
      seen.db = useDb();
      return 'child' as any;
    };
    const root = createRoot((dispose) => ({
      dispose,
      view: createComponent(Sp00kyProvider, {
        config: { database: {} } as any,
        fallback: 'loading' as any,
        ...props,
        get children() {
          return createComponent(Child, {});
        },
      }),
    }));
    return { ...root, seen };
  }

  /** An `init()` the test finishes by hand. */
  function heldInit() {
    let release!: () => void;
    const spy = vi
      .spyOn(SyncedDb.prototype, 'init')
      .mockImplementation(() => new Promise<void>((r) => (release = r)));
    return { spy, release: () => release() };
  }

  it('eager: renders the children at once, on an instance that is initialized behind them', async () => {
    vi.stubGlobal('window', {});
    const init = heldInit();
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const onReady = vi.fn();
    const preload = vi.fn(async () => {});

    const { view, seen, dispose } = mount({ eager: true, onReady, preload });
    // No fallback, no waiting: the tree is there on the first render.
    expect(resolve(view)).toBe('child');
    expect(seen.db).toBeInstanceOf(SyncedDb);
    expect(seen.db!.isReady).toBe(false);
    // `init()` ran on mount, as without `eager`.
    expect(init.spy).toHaveBeenCalledTimes(1);
    expect(init.spy.mock.instances[0]).toBe(seen.db);
    expect(onReady).not.toHaveBeenCalled();
    expect(preload).not.toHaveBeenCalled();

    init.release();
    await settle();
    expect(preload).toHaveBeenCalledWith(seen.db);
    expect(onReady).toHaveBeenCalledWith(seen.db);
    expect(resolve(view)).toBe('child');

    dispose();
    await settle();
    // In the tree's hands from the start: never closed under it.
    expect(close).not.toHaveBeenCalled();
  });

  it('eager: an instance whose init outlives the provider is not closed either', async () => {
    vi.stubGlobal('window', {});
    const init = heldInit();
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const onReady = vi.fn();

    const { dispose } = mount({ eager: true, onReady });
    dispose();
    init.release();
    await settle();
    expect(close).not.toHaveBeenCalled();
    expect(onReady).not.toHaveBeenCalled();
  });

  it('without eager: the fallback until init is done, and an init that outlives it is closed', async () => {
    vi.stubGlobal('window', {});
    const init = heldInit();
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const onReady = vi.fn();

    const first = mount({ onReady });
    expect(resolve(first.view)).toBe('loading');
    expect(first.seen.db).toBeUndefined();
    init.release();
    await settle();
    expect(onReady).toHaveBeenCalledTimes(1);
    expect(resolve(first.view)).toBe('child');
    first.dispose();
    expect(close).not.toHaveBeenCalled();

    const second = mount({ onReady });
    second.dispose();
    init.release();
    await settle();
    expect(close).toHaveBeenCalledTimes(1);
    expect(onReady).toHaveBeenCalledTimes(1);
  });

  it('eager in a server render: the fallback', async () => {
    // No `window` in the node test environment, as on a server.
    const init = heldInit();
    const { view, seen, dispose } = mount({ eager: true });
    expect(resolve(view)).toBe('loading');
    expect(seen.db).toBeUndefined();
    dispose();
    init.release();
    await settle();
  });
});
