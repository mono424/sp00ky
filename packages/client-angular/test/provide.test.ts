import { ApplicationInitStatus, PLATFORM_ID, provideZonelessChangeDetection } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Sp00kyClient } from '@spooky-sync/core';
import {
  injectAppRelease,
  injectBucketImage,
  injectCrdtField,
  injectDb,
  injectDownloadFile,
  injectFeatureFlag,
  injectFileUpload,
  injectImpersonation,
  injectPendingMutations,
  injectPreload,
  injectQuery,
  injectStorageStatus,
  injectSyncActivity,
  injectSyncStatus,
  provideSp00ky,
  SyncedDb,
} from '@spooky-sync/client-angular';
import { inject, provideDb, tick } from './helpers';

const config = { schema: { tables: [], relationships: [], backends: {} } } as any;

/** Enough of a config to create a core client. Creating one opens nothing. */
const realConfig = {
  schema: {
    tables: [{ name: 'post', columns: { id: { type: 'string' }, title: { type: 'string' } } }],
    relationships: [],
    backends: {},
  },
  schemaSurql: '',
  logLevel: 'silent',
  database: { namespace: 'main', database: 'app', endpoint: 'ws://localhost:1/rpc' },
} as any;

afterEach(() => vi.restoreAllMocks());

function setup(providers: any[]) {
  TestBed.configureTestingModule({ providers: [provideZonelessChangeDetection(), ...providers] });
  return TestBed.inject(ApplicationInitStatus).donePromise;
}

describe('provideSp00ky', () => {
  it('starts the client and runs preload before the app renders', async () => {
    const order: string[] = [];
    vi.spyOn(SyncedDb.prototype, 'init').mockImplementation(async () => {
      order.push('init');
    });
    const preload = vi.fn(async () => {
      order.push('preload');
    });
    await setup([provideSp00ky(config, { preload })]);
    const db = TestBed.inject(SyncedDb);
    expect(order).toEqual(['init', 'preload']);
    expect(preload).toHaveBeenCalledWith(db);
    expect(TestBed.runInInjectionContext(() => injectDb())).toBe(db);
  });

  it('a failed preload is logged, not fatal', async () => {
    vi.spyOn(SyncedDb.prototype, 'init').mockResolvedValue();
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    await setup([provideSp00ky(config, { preload: () => Promise.reject(new Error('x')) })]);
    expect(error).toHaveBeenCalled();
  });

  it('builds the config in the injection context and closes the client on teardown', async () => {
    vi.spyOn(SyncedDb.prototype, 'init').mockResolvedValue();
    const close = vi.spyOn(SyncedDb.prototype, 'close').mockResolvedValue();
    const factory = vi.fn(() => config);
    await setup([provideSp00ky(factory)]);
    TestBed.inject(SyncedDb);
    expect(factory).toHaveBeenCalledTimes(1);
    TestBed.resetTestingModule();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('never starts the client during a server render', async () => {
    const init = vi.spyOn(SyncedDb.prototype, 'init').mockResolvedValue();
    await setup([provideSp00ky(config), { provide: PLATFORM_ID, useValue: 'server' }]);
    expect(init).not.toHaveBeenCalled();
  });

  it('a failed init fails bootstrap', async () => {
    vi.spyOn(SyncedDb.prototype, 'init').mockRejectedValue(new Error('no store'));
    await expect(setup([provideSp00ky(config)])).rejects.toThrow('no store');
  });
});

describe('provideSp00ky, eager', () => {
  it('does not block bootstrap, and starts the client once behind it', async () => {
    let finishInit!: () => void;
    const init = vi
      .spyOn(SyncedDb.prototype, 'init')
      .mockImplementation(() => new Promise<void>((resolve) => (finishInit = resolve)));
    const preload = vi.fn(async () => {});
    // Done although init is not: the app renders from here.
    await setup([provideSp00ky(realConfig, { eager: true, preload })]);
    const db = TestBed.inject(SyncedDb);
    expect(init).toHaveBeenCalledTimes(1);
    expect(preload).not.toHaveBeenCalled();
    expect(db.isReady).toBe(false);
    // Usable at once. An instance that is not eager throws here.
    expect(db.pendingMutationCount).toBe(0);

    finishInit();
    await tick();
    expect(init).toHaveBeenCalledTimes(1);
    expect(preload).toHaveBeenCalledTimes(1);
    expect(preload).toHaveBeenCalledWith(db);
  });

  it('a failed init is logged, and fails neither bootstrap nor a promise nobody holds', async () => {
    vi.spyOn(SyncedDb.prototype, 'init').mockRejectedValue(new Error('no store'));
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const preload = vi.fn(async () => {});
    await setup([provideSp00ky(config, { eager: true, preload })]);
    await tick();
    expect(error).toHaveBeenCalledWith(
      expect.stringContaining('failed to start'),
      expect.any(Error)
    );
    expect(preload).not.toHaveBeenCalled();
  });

  it('a failed preload is logged', async () => {
    vi.spyOn(SyncedDb.prototype, 'init').mockResolvedValue();
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    await setup([
      provideSp00ky(config, { eager: true, preload: () => Promise.reject(new Error('x')) }),
    ]);
    await tick();
    expect(error).toHaveBeenCalledWith(
      expect.stringContaining('preload failed'),
      expect.any(Error)
    );
  });

  it('a server render neither starts the client nor gets one that can be used', async () => {
    const init = vi.spyOn(SyncedDb.prototype, 'init').mockResolvedValue();
    await setup([
      provideSp00ky(realConfig, { eager: true }),
      { provide: PLATFORM_ID, useValue: 'server' },
    ]);
    expect(init).not.toHaveBeenCalled();
    expect(() => TestBed.inject(SyncedDb).getSp00ky()).toThrow('SyncedDb not initialized');
  });
});

describe('SyncedDb', () => {
  it('throws when used before init, as it always did', () => {
    const db = new SyncedDb<any>(realConfig);
    expect(() => db.getSp00ky()).toThrow('SyncedDb not initialized');
    expect(() => db.query('post')).toThrow('SyncedDb not initialized');
    expect(() => db.pendingMutationCount).toThrow('SyncedDb not initialized');
    expect(db.isReady).toBe(false);
    expect(db.peekQuery({} as any)).toBeNull();
  });

  it('eager: usable before init, on a core client that is not open', async () => {
    const db = new SyncedDb<any>(realConfig, { eager: true });
    const sp00ky = db.getSp00ky();
    expect(db.getSp00ky()).toBe(sp00ky);
    expect(db.isReady).toBe(false);
    expect(sp00ky.isLocalReady()).toBe(false);
    // State reads report their defaults.
    expect(db.pendingMutationCount).toBe(0);
    expect(db.fetchingQueryCount).toBe(0);
    expect(db.syncHealth.everConnected).toBe(false);
    expect(db.storageHealth.status).toBe('unknown');
    expect(db.auth.impersonation).toBeNull();
    // This store keeps no paint cache: nothing to peek.
    const query = db.query('post').build();
    expect(db.peekQuery(query)).toBeNull();

    // A query and a write made early wait for init. Here the client closes first.
    const settled: string[] = [];
    const run = query.run().then(
      () => settled.push('run'),
      (e: Error) => settled.push(e.message)
    );
    const create = db.create('post:a', { title: 'A' }).then(
      () => settled.push('create'),
      (e: Error) => settled.push(e.message)
    );
    const ready = db.whenReady();
    await tick();
    expect(settled).toEqual([]);
    await db.close();
    await Promise.all([run, create]);
    expect(settled).toHaveLength(2);
    expect(settled[0]).toMatch(/closed before it was ready/);
    expect(settled[1]).toMatch(/closed before it was ready/);
    await expect(ready).rejects.toThrow(/closed before it was ready/);
  });

  it('eager: init opens the client that was created early, and ends the peeking', async () => {
    const init = vi.spyOn(Sp00kyClient.prototype, 'init').mockResolvedValue();
    const db = new SyncedDb<any>(realConfig, { eager: true });
    const sp00ky = db.getSp00ky();
    const peek = vi.spyOn(sp00ky, 'peekQuery').mockReturnValue([{ id: 'post:a' }]);
    const query = db.query('post').build();
    expect(db.peekQuery(query)).toEqual([{ id: 'post:a' }]);

    await db.init();
    await db.init();
    expect(init).toHaveBeenCalledTimes(1);
    expect(init.mock.contexts[0]).toBe(sp00ky);
    expect(db.getSp00ky()).toBe(sp00ky);
    expect(db.isReady).toBe(true);
    expect(db.peekQuery(query)).toBeNull();
    expect(peek).toHaveBeenCalledTimes(1);
    await db.close();
  });
});

describe('inject functions on an eager client that is not initialized yet', () => {
  it('can all be created, and report their not-known-yet values', async () => {
    const db = new SyncedDb<any>(realConfig, { eager: true });
    provideDb(db);
    const made = inject(() => ({
      pending: injectPendingMutations(),
      sync: injectSyncStatus(),
      storage: injectStorageStatus(),
      activity: injectSyncActivity(),
      impersonation: injectImpersonation(),
      flag: injectFeatureFlag('new-editor', { fallback: 'off' }),
      release: injectAppRelease({ app: 'web', currentVersion: '1.0.0' }),
      field: injectCrdtField('post', null, 'title'),
      upload: injectFileUpload('avatars'),
      file: injectDownloadFile('avatars', null),
      image: injectBucketImage('avatars', null),
      preload: injectPreload(() => db.query('post').build()),
      query: injectQuery(() => db.query('post').build()),
    }));
    await tick();
    expect(made.pending()).toBe(0);
    expect(made.sync.status()).toBe('healthy');
    expect(made.sync.everConnected()).toBe(false);
    expect(made.storage.status()).toBe('unknown');
    expect(made.activity.isDownloading()).toBe(false);
    expect(made.activity.isUploading()).toBe(false);
    expect(made.impersonation.isImpersonating()).toBe(false);
    expect(made.flag.variant()).toBe('off');
    expect(made.release.updateAvailable()).toBe(false);
    expect(made.field()).toBeNull();
    expect(made.upload.isUploading()).toBe(false);
    expect(made.file.url()).toBeNull();
    expect(made.image.url()).toBeNull();
    // Waiting for init, not failed.
    expect(made.query.isLoading()).toBe(true);
    expect(made.query.error()).toBeUndefined();
    await db.close();
  });
});

describe('injectDb', () => {
  it('explains a missing provider', () => {
    TestBed.configureTestingModule({ providers: [provideZonelessChangeDetection()] });
    expect(() => TestBed.runInInjectionContext(() => injectDb())).toThrow(/provideSp00ky/);
  });
});
