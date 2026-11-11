import {
  assertInInjectionContext,
  inject,
  Injector,
  PLATFORM_ID,
  runInInjectionContext,
} from '@angular/core';
import { isPlatformBrowser } from '@angular/common';
import { SyncedDb } from './db';
import type { MaybeSignal } from './types';

/** Read a `MaybeSignal`, tracking whatever the signal or function reads. */
export function read<T>(value: MaybeSignal<T>): T {
  return typeof value === 'function' ? (value as () => T)() : value;
}

export function toError(e: unknown): Error {
  return e instanceof Error ? e : new Error(String(e));
}

/**
 * Run `fn` in the caller's injection context, or in `injector` when one is
 * given. `caller` names the public function in Angular's NG0203 error.
 */
export function inContext<T>(caller: Function, injector: Injector | undefined, fn: () => T): T {
  if (injector) return runInInjectionContext(injector, fn);
  assertInInjectionContext(caller);
  return fn();
}

/** The provided client, or a readable error. Must run in an injection context. */
export function providedDb(caller: string): SyncedDb<any> {
  const db = inject(SyncedDb, { optional: true });
  if (!db) {
    throw new Error(
      `${caller}: no SyncedDb is provided. Add provideSp00ky(config) to the application ` +
        'providers (app.config.ts), or provide a SyncedDb yourself ({ provide: SyncedDb, useValue: db }).'
    );
  }
  return db;
}

/**
 * The client, for a function that talks to the engine. The engine is
 * browser-only (WebAssembly, IndexedDB / OPFS, workers), and `provideSp00ky`
 * does not start it during a server render, so these functions refuse to run
 * there instead of failing deep inside the engine.
 */
export function engineDb(caller: string): SyncedDb<any> {
  const db = providedDb(caller);
  if (!isPlatformBrowser(inject(PLATFORM_ID))) {
    throw new Error(
      `${caller} runs in the browser only: the sp00ky client does not start during a server render. ` +
        'Render this component on the client: wrap it in a @defer block, or give its route ' +
        'renderMode: RenderMode.Client.'
    );
  }
  return db;
}
