import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  children,
  createComponent,
  createRenderEffect,
  createRoot,
  flush,
  untrack,
} from 'solid-js';
import { SyncedDb } from '../../index';
import { Sp00kyProvider } from '../Sp00kyProvider';
import { Sp00kyContext, useDb, usePendingMutations } from '../context';
import { createQuery } from '../create-query';
import { createPreload } from '../create-preload';
import { useAppRelease } from '../use-app-release';
import { useChannel } from '../use-channel';
import { useFeatureFlag } from '../use-feature-flag';
import { useImpersonation } from '../use-impersonation';
import { useStorageStatus } from '../use-storage-status';
import { useSyncActivity } from '../use-sync-activity';
import { useSyncStatus } from '../use-sync-status';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));
const settle = async () => {
  await tick();
  flush();
  await tick();
};

// The last suite builds a real core client from this, and never opens it.
const config = {
  database: { namespace: 'n', database: 'd', endpoint: 'ws://127.0.0.1:1/rpc' },
  schema: {
    tables: [{ name: 'thing', columns: { a: { type: 'string' } }, channel: true }],
    relationships: [],
    backends: {},
    buckets: [],
  },
  schemaSurql: '',
  logLevel: 'silent',
} as any;

/**
 * Render `component` with `child` as its children, and collect what it
 * renders. `child` runs untracked, as a component's body does.
 */
function mount(component: unknown, props: Record<string, unknown>, child: () => unknown) {
  const rendered: unknown[] = [];
  const dispose = createRoot((dispose) => {
    const el = createComponent(component as any, {
      ...props,
      get children() {
        return untrack(child);
      },
    });
    const resolved = children(() => el as any);
    createRenderEffect(
      () => resolved(),
      (v) => void rendered.push(v)
    );
    return dispose;
  });
  flush();
  return { rendered, dispose };
}

const provider = (props: Record<string, unknown>, child: () => unknown) =>
  mount(Sp00kyProvider, { config, fallback: 'loading', ...props }, child);

/**
 * `init` held open by the test, so the provider never builds a core client.
 * The eager branch asks for a browser: a bare `window` is all it looks at.
 */
function setup({ browser = true } = {}) {
  if (browser) vi.stubGlobal('window', {});
  let resolveInit!: () => void;
  const init = vi
    .spyOn(SyncedDb.prototype, 'init')
    .mockImplementation(() => new Promise<void>((r) => (resolveInit = r)));
  const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
  return { init, close, resolveInit: () => resolveInit() };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('Sp00kyProvider', () => {
  it('eager: renders the children at once, on an instance that is initialized behind them', async () => {
    const { init, close, resolveInit } = setup();
    const onReady = vi.fn();
    const preload = vi.fn(async () => {});
    let seen: SyncedDb<any> | undefined;
    const { rendered, dispose } = provider({ eager: true, onReady, preload }, () => {
      seen = useDb();
      return `child ready=${seen.isReady}`;
    });
    // No fallback, no waiting: the tree is there on the first render.
    expect(rendered).toEqual(['child ready=false']);
    expect(seen).toBeInstanceOf(SyncedDb);
    expect(onReady).not.toHaveBeenCalled();
    await settle();
    expect(init).toHaveBeenCalledTimes(1);
    expect(init.mock.instances[0]).toBe(seen);
    expect(preload).not.toHaveBeenCalled();

    resolveInit();
    await settle();
    expect(preload).toHaveBeenCalledTimes(1);
    expect(onReady).toHaveBeenCalledTimes(1);
    expect(onReady).toHaveBeenCalledWith(seen);
    // The same instance all along: the children were not rendered again.
    expect(rendered).toEqual(['child ready=false']);

    dispose();
    await settle();
    // In the tree's hands from the start: never closed under it.
    expect(close).not.toHaveBeenCalled();
  });

  it('eager: an instance whose init outlives the provider is not closed, and not announced', async () => {
    const { close, resolveInit } = setup();
    const onReady = vi.fn();
    const { dispose } = provider({ eager: true, onReady }, () => 'child');
    await settle();
    dispose();
    resolveInit();
    await settle();
    expect(close).not.toHaveBeenCalled();
    expect(onReady).not.toHaveBeenCalled();
  });

  it('eager: a failed init reaches onError and leaves the children up', async () => {
    setup();
    vi.spyOn(SyncedDb.prototype, 'init').mockRejectedValue(new Error('no store'));
    const onError = vi.fn();
    const { rendered } = provider({ eager: true, onError }, () => 'child');
    await settle();
    expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: 'no store' }));
    expect(rendered).toEqual(['child']);
  });

  it('without eager: the fallback until init and preload are done', async () => {
    const { init, resolveInit } = setup();
    const onReady = vi.fn();
    let releasePreload!: () => void;
    const preload = vi.fn(() => new Promise<void>((r) => (releasePreload = r)));
    const { rendered } = provider(
      { onReady, preload },
      () => `child ready=${useDb() instanceof SyncedDb}`
    );
    expect(rendered).toEqual(['loading']);
    await settle();
    expect(init).toHaveBeenCalledTimes(1);
    resolveInit();
    await settle();
    // `preload` still holds the UI back here.
    expect(rendered).toEqual(['loading']);
    releasePreload();
    await settle();
    expect(rendered).toEqual(['loading', 'child ready=true']);
    expect(onReady).toHaveBeenCalledTimes(1);
  });

  it('without eager: an instance whose init outlives the provider is closed', async () => {
    const { close, resolveInit } = setup();
    const { dispose } = provider({}, () => 'child');
    await settle();
    dispose();
    resolveInit();
    await settle();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('eager outside a browser renders the fallback, like a provider without it', async () => {
    setup({ browser: false });
    const { rendered } = provider({ eager: true }, () => 'child');
    expect(rendered).toEqual(['loading']);
  });
});

describe('primitives created on an eager instance that is not initialized yet', () => {
  it('do not throw, and report their not-known-yet values', async () => {
    // What an eager provider puts in context, on a real core client.
    const eager = new SyncedDb<any>(config, { eager: true });
    let out: Record<string, unknown> | undefined;
    const { rendered, dispose } = mount(Sp00kyContext, { value: eager }, () => {
      const db = useDb<any>();
      const query = createQuery<any, any, any, any, false, any>(db.query('thing').build());
      createPreload(db.query('thing').limit(1).build());
      const pending = usePendingMutations();
      const flag = useFeatureFlag('beta');
      const release = useAppRelease({ app: 'web', currentVersion: '1.0.0' });
      const sync = useSyncStatus();
      const storage = useStorageStatus();
      const impersonation = useImpersonation({ rendersBanner: true });
      const activity = useSyncActivity();
      const channel = useChannel(db.query('thing').where({ a: 'x' }), ['user:1']);
      out = {
        ready: db.isReady,
        rows: query.data(),
        loading: query.isLoading(),
        pending: pending(),
        flag: flag.enabled(),
        variant: flag.variant(),
        update: release.updateAvailable(),
        status: sync.status(),
        everConnected: sync.everConnected(),
        offline: sync.isOffline(),
        storage: storage.status(),
        impersonating: impersonation.isImpersonating(),
        fetching: activity.fetchingQueries(),
        downloading: activity.isDownloading(),
        uploading: activity.isUploading(),
        channel: typeof channel.status(),
        bucket: typeof db.bucket('files').put,
      };
      return 'child';
    });
    expect(rendered).toEqual(['child']);
    expect(out).toEqual({
      ready: false,
      rows: [],
      loading: true,
      pending: 0,
      flag: false,
      variant: undefined,
      update: false,
      status: 'healthy',
      everConnected: false,
      offline: false,
      storage: 'unknown',
      impersonating: false,
      fetching: 0,
      downloading: false,
      uploading: false,
      channel: 'string',
      bucket: 'function',
    });
    // Their effects and subscriptions start on the unopened client too.
    await settle();
    dispose();
    await settle();
  });

  it('a plain instance still throws before init, as it always did', () => {
    const db = new SyncedDb<any>(config);
    expect(db.isReady).toBe(false);
    expect(() => db.getSp00ky()).toThrow('SyncedDb not initialized');
    expect(() => db.query('thing')).toThrow('SyncedDb not initialized');
    expect(() => db.auth).toThrow('SyncedDb not initialized');
    expect(() => db.pendingMutationCount).toThrow('SyncedDb not initialized');
    expect(() => db.subscribeToSyncHealth(() => {})).toThrow('SyncedDb not initialized');
    return Promise.all([
      expect(db.create('thing:1', {})).rejects.toThrow('SyncedDb not initialized'),
      expect(db.update('thing', 'thing:1', {})).rejects.toThrow('SyncedDb not initialized'),
      // The instance is checked before the selector, as before.
      expect(db.delete('thing', {} as any)).rejects.toThrow('SyncedDb not initialized'),
    ]);
  });

  it('an eager instance creates its core client on first use, and peeks only until it is ready', () => {
    const db = new SyncedDb<any>(config, { eager: true });
    const sp00ky = db.getSp00ky();
    expect(db.getSp00ky()).toBe(sp00ky);
    expect(db.isReady).toBe(false);
    const query = db.query('thing').build();
    const peek = vi.spyOn(sp00ky, 'peekQuery').mockReturnValue([{ id: 'thing:1' }]);
    expect(db.peekQuery(query)).toEqual([{ id: 'thing:1' }]);
    (db as any)._initialized = true;
    expect(db.isReady).toBe(true);
    expect(db.peekQuery(query)).toBeNull();
    expect(peek).toHaveBeenCalledTimes(1);
    // A plain instance never peeks.
    expect(new SyncedDb<any>(config).peekQuery(query)).toBeNull();
  });
});
