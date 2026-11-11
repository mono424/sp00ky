---
name: sp00ky-angular
description: >-
  Angular (signals) integration for the Sp00ky reactive local-first SurrealDB
  framework. Use when an Angular 21+ app uses sp00ky: provideSp00ky in
  app.config.ts, injectQuery with signal inputs for live data, mutations via
  plain db calls or injectSubmission, status functions (sync, storage, feature
  flags, app release), CRDT fields, peer to peer channels, file
  upload/download and sp00ky-bucket-image, plus the Angular CLI build settings
  (worker entry, wasm assets, prebundle exclusions). For React apps use the
  sp00ky-react skill, for Svelte sp00ky-svelte, for Solid sp00ky-solid or
  sp00ky-solid2.
metadata:
  author: sp00ky-sync
  version: '0.0.1'
---

# Sp00ky Angular Client

`@spooky-sync/client-angular` targets Angular 21+ in the browser (zoneless or zone.js). `inject*` functions return read-only signals and run in an injection context; inputs that follow state are signals or functions. `injectQuery` rows are structurally shared between live emissions: rows matched by `id` keep their object identity, a deep-equal emission does not notify.

## Setup

```ts
// app.config.ts
export const appConfig: ApplicationConfig = {
  providers: [
    provideZonelessChangeDetection(),
    provideSp00ky({
      schema,
      schemaSurql: SURQL_SCHEMA,
      logLevel: 'info',
      database: {
        namespace: 'main',
        database: 'app',
        endpoint: 'ws://localhost:8666/rpc',
        store: 'indexeddb',
        workers: {
          surreal: () =>
            new Worker(new URL('./sp00ky-surreal.worker', import.meta.url), { type: 'module' }),
        },
      },
    }),
  ],
};
```

The client starts in an app initializer; the app renders once it is ready. Angular CLI apps also need: `src/app/sp00ky-surreal.worker.ts` containing `import '@surrealdb/wasm/worker';` (install `@surrealdb/wasm`), `webWorkerTsConfig`, `assets` copying `ssp_wasm_bg.wasm` (`@spooky-sync/ssp-wasm/pkg`), `index_bg.wasm` (`@surrealdb/wasm-native/dist`), `tabs-broker-worker.js` (`@spooky-sync/core/dist`) and `sqlite3.wasm` + `sqlite3-opfs-async-proxy.js` (`@sqlite.org/sqlite-wasm/dist`) to `/`, and `serve.options.prebundle.exclude` for `@spooky-sync/client-angular`, `@spooky-sync/core`, `@spooky-sync/ssp-wasm`, `@surrealdb/wasm`, `@surrealdb/wasm-native`, `@sqlite.org/sqlite-wasm`. With pnpm, `public-hoist-pattern[]` the wasm packages. Without these, init hangs on a 404 `worker-agent.mjs`.

## Queries

```ts
export class PostList {
  private readonly db = injectDb<typeof schema>();
  readonly authorId = input<string | null>(null);
  protected readonly posts = injectQuery(() => {
    const author = this.authorId();
    return author ? this.db.query('post').where({ author }).limit(20).build() : null;
  });
}
```

```html
@if (posts.isLoading()) { <app-spinner /> } @else if (posts.isEmpty()) { <app-empty /> } @else {
@for (p of posts.data(); track p.id) { <app-post-row [post]="p" /> } }
```

- Pass a function; it re-runs when the signals it reads change, and the query re-registers only when its identity (surql + vars) changes. Return `null` to skip.
- Result signals: `data`, `error`, `isLoading` (cold first load only), `isFetching`, `isAuthoritative`, `hasData`, `isEmpty` (server said "no rows"), `isSettled` (authoritative and idle).
- Options: `{ enabled?, deregisterOnCleanup?, injector? }`. `injectPreload(() => query)` warms the cache for a likely-next screen.

## Mutations

Plain awaited calls; the engine is optimistic local-first:

```ts
await this.db.create('post:' + crypto.randomUUID(), { title });
await this.db.update('post', id, { title });
await this.db.delete('post', row.id);
await this.db.run('backend', 'route', payload);
```

Button state: `save = injectSubmission(fn)` gives `submit`, `pending()`, `error()`, `result()`, `clearError`.

## Functions

`injectDb`, `injectPendingMutations()`, `injectSyncStatus`, `injectSyncActivity`, `injectStorageStatus`, `injectImpersonation`, `injectFeatureFlag`, `injectAppRelease`, `injectCrdtField(table, this.id, field)()`, `injectChannel(() => query, () => peers)`, `injectFileUpload(bucket)`, `injectDownloadFile(bucket, this.path)`, `injectBlurhash`, `injectBucketImage` (with `<img [sp00kyImageGate]="image">`), plus `<sp00ky-bucket-image>` (projected placeholder) and `<canvas [sp00kyBlurhash]>`. Tests: `{ provide: SyncedDb, useValue: fakeDb }`.

## Gotchas

- `inject*` run in field initializers / constructors (or pass `{ injector }`), not in `ngOnInit` or handlers.
- Call the signals: `posts.data()`.
- Required inputs are read inside the query function, never in a field initializer.
- SSR: engine-backed components go in `@defer` or on `RenderMode.Client` routes.
- No suspense mode; branch on `isLoading()`.

Full API: `node_modules/@spooky-sync/client-angular/AGENTS.md`.
