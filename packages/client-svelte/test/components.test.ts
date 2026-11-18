import { afterEach, describe, expect, it, vi } from 'vitest';
import { flushSync, mount, unmount } from 'svelte';
import { SyncedDb } from '../src/db.js';
import App from './fixtures/App.svelte';
import Orphan from './fixtures/Orphan.svelte';
import Image from './fixtures/Image.svelte';
import { tick } from './helpers.js';

afterEach(() => {
  vi.restoreAllMocks();
  document.body.innerHTML = '';
});

describe('Sp00kyProvider', () => {
  it('shows the fallback until init + preload finish, then renders children with the db', async () => {
    let releasePreload!: () => void;
    const init = vi.spyOn(SyncedDb.prototype, 'init').mockResolvedValue();
    const onReady = vi.fn();
    const app = mount(App, {
      target: document.body,
      props: {
        preload: () => new Promise<void>((r) => (releasePreload = r)),
        onReady,
      },
    });
    flushSync();
    expect(document.querySelector('.fallback')).not.toBeNull();
    await tick();
    expect(init).toHaveBeenCalledOnce();
    expect(document.querySelector('.child')).toBeNull();

    releasePreload();
    await tick();
    flushSync();
    expect(document.querySelector('.fallback')).toBeNull();
    expect(document.querySelector('.child')?.textContent).toBe('SyncedDb');
    expect(onReady).toHaveBeenCalledOnce();
    unmount(app);
  });

  it('closes a client whose init outlived the provider', async () => {
    let finishInit!: () => void;
    vi.spyOn(SyncedDb.prototype, 'init').mockImplementation(
      () => new Promise<void>((r) => (finishInit = r))
    );
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const app = mount(App, { target: document.body, props: {} });
    flushSync();
    unmount(app);
    finishInit();
    await tick();
    expect(close).toHaveBeenCalledOnce();
  });

  it('eager: renders the children at once, on an instance that is initialized behind them', async () => {
    let finishInit!: () => void;
    const init = vi
      .spyOn(SyncedDb.prototype, 'init')
      .mockImplementation(() => new Promise<void>((r) => (finishInit = r)));
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const onReady = vi.fn();
    const preload = vi.fn(async () => {});
    const app = mount(App, { target: document.body, props: { eager: true, preload, onReady } });
    // No fallback, no waiting: the tree is there before anything was awaited,
    // and `useDb()` in it found the instance.
    const child = document.querySelector<HTMLElement>('.child');
    expect(child?.textContent).toBe('SyncedDb');
    expect(child?.dataset.ready).toBe('false');
    expect(document.querySelector('.fallback')).toBeNull();
    flushSync();
    await tick();
    expect(init).toHaveBeenCalledOnce();
    expect(preload).not.toHaveBeenCalled();
    expect(onReady).not.toHaveBeenCalled();

    finishInit();
    await tick();
    flushSync();
    expect(preload).toHaveBeenCalledOnce();
    expect(onReady).toHaveBeenCalledOnce();
    // The instance that was initialized is the one the tree mounted on.
    expect(onReady.mock.calls[0][0]).toBe(init.mock.contexts[0]);
    expect(init).toHaveBeenCalledOnce();
    expect(document.querySelector('.child')).toBe(child);
    unmount(app);
    await tick();
    // In the tree's hands from the start: never closed under it.
    expect(close).not.toHaveBeenCalled();
  });

  it('eager: keeps the children up when init fails, and never closes the instance', async () => {
    let failInit!: (e: Error) => void;
    vi.spyOn(SyncedDb.prototype, 'init').mockImplementation(
      () => new Promise<void>((_, reject) => (failInit = reject))
    );
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const onError = vi.fn();
    const onReady = vi.fn();
    const app = mount(App, { target: document.body, props: { eager: true, onError, onReady } });
    flushSync();
    failInit(new Error('no storage'));
    await tick();
    flushSync();
    expect(onError.mock.calls[0][0].message).toBe('no storage');
    expect(onReady).not.toHaveBeenCalled();
    expect(document.querySelector('.child')).not.toBeNull();

    // Init outliving the provider closes a client nobody saw. This one was seen.
    let finishInit!: () => void;
    vi.spyOn(SyncedDb.prototype, 'init').mockImplementation(
      () => new Promise<void>((r) => (finishInit = r))
    );
    const second = mount(App, { target: document.body, props: { eager: true, onReady } });
    flushSync();
    unmount(second);
    finishInit();
    await tick();
    expect(onReady).not.toHaveBeenCalled();
    expect(close).not.toHaveBeenCalled();
    unmount(app);
  });

  it('useDb outside a provider throws an actionable error', () => {
    expect(() => mount(Orphan, { target: document.body })).toThrow(/Sp00kyProvider/);
  });
});

describe('BucketImage', () => {
  it('layers fallback and blurhash, sets src from the lease, and releases it on destroy', async () => {
    const release = vi.fn();
    const bucket = {
      blurhash: vi.fn(async () => 'LEHV6nWB2yk8pyo0adR*.7kCMdnj'),
      url: vi.fn(async (path: string) => ({ url: 'blob:' + path, release })),
    };
    const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
    (db as any).bucket = () => bucket;

    const app = mount(Image, { target: document.body, props: { db, path: 'a.png' } });
    flushSync();
    expect(document.querySelector('.frame .plate')).not.toBeNull();
    expect(document.querySelector('.frame canvas')).not.toBeNull();
    await tick();
    flushSync();
    const img = document.querySelector('.frame img') as HTMLImageElement;
    expect(img.getAttribute('src')).toBe('blob:a.png');
    expect(img.alt).toBe('cover');
    expect(bucket.blurhash).toHaveBeenCalledWith('a.png');
    unmount(app);
    expect(release).toHaveBeenCalledOnce();
  });
});
