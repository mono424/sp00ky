import {
  DestroyRef,
  type EnvironmentProviders,
  inject,
  makeEnvironmentProviders,
  PLATFORM_ID,
  provideAppInitializer,
} from '@angular/core';
import { isPlatformBrowser } from '@angular/common';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import { SyncedDb } from './db';
import { inContext, providedDb } from './internal';
import type { InjectOptions, Sp00kyOptions, SyncedDbConfig } from './types';

/**
 * Provide the sp00ky client to the application:
 *
 * ```ts
 * // app.config.ts
 * export const appConfig: ApplicationConfig = {
 *   providers: [provideSp00ky(dbConfig)],
 * };
 * ```
 *
 * The client starts in an app initializer, so the app renders once it is
 * ready and every `inject*` function can use it straight away. Whatever is in
 * the root element of `index.html` stays on screen until then. If `init()`
 * fails, bootstrap fails: handle it on `bootstrapApplication(...).catch(...)`.
 *
 * `config` may also be a factory, run in the injection context, to build the
 * config from other providers. During a server render the client is created
 * but never started (see `injectQuery`).
 *
 * The client is closed when the application is destroyed.
 */
export function provideSp00ky<S extends SchemaStructure>(
  config: SyncedDbConfig<S> | (() => SyncedDbConfig<S>),
  options: Sp00kyOptions<S> = {}
): EnvironmentProviders {
  return makeEnvironmentProviders([
    {
      provide: SyncedDb,
      useFactory: () => {
        const db = new SyncedDb<S>(typeof config === 'function' ? config() : config);
        inject(DestroyRef).onDestroy(() => void db.close());
        return db;
      },
    },
    provideAppInitializer(() => {
      if (!isPlatformBrowser(inject(PLATFORM_ID))) return;
      return start(inject(SyncedDb) as SyncedDb<S>, options.preload);
    }),
  ]);
}

async function start<S extends SchemaStructure>(
  db: SyncedDb<S>,
  preload: Sp00kyOptions<S>['preload']
): Promise<void> {
  await db.init();
  if (!preload) return;
  try {
    await preload(db);
  } catch (e) {
    // oxlint-disable-next-line no-console
    console.error('provideSp00ky: preload failed; starting the app anyway', e);
  }
}

/**
 * The application's `SyncedDb`, typed with your schema:
 * `private readonly db = injectDb<typeof schema>();`. Same instance as
 * `inject(SyncedDb)`.
 */
export function injectDb<S extends SchemaStructure>(options?: InjectOptions): SyncedDb<S> {
  return inContext(injectDb, options?.injector, () => providedDb('injectDb') as SyncedDb<S>);
}
