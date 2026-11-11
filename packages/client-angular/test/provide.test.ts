import { ApplicationInitStatus, PLATFORM_ID, provideZonelessChangeDetection } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { injectDb, provideSp00ky, SyncedDb } from '@spooky-sync/client-angular';

const config = { schema: { tables: [], relationships: [], backends: {} } } as any;

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
});

describe('injectDb', () => {
  it('explains a missing provider', () => {
    TestBed.configureTestingModule({ providers: [provideZonelessChangeDetection()] });
    expect(() => TestBed.runInInjectionContext(() => injectDb())).toThrow(/provideSp00ky/);
  });
});
