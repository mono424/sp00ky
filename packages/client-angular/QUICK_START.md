# Quick start: Sp00ky with Angular

The same content as the docs' [Angular guide](https://mono424.github.io/sp00ky/docs/guide/angular).

## 1. Install

```bash
pnpm add @spooky-sync/core @spooky-sync/client-angular @surrealdb/wasm
```

With pnpm, hoist the packages whose `.wasm` files the build copies (step 2), so their paths exist:

```ini
# .npmrc
public-hoist-pattern[]=@spooky-sync/ssp-wasm
public-hoist-pattern[]=@surrealdb/wasm-native
public-hoist-pattern[]=@sqlite.org/sqlite-wasm
```

## 2. Configure the Angular CLI

The core client starts web workers and loads `.wasm` files relative to its own modules. The Angular
CLI's builder does not bundle a worker started inside a dependency, and does not copy a `.wasm`
file a dependency references, so the app does both.

A worker entry for the local database engine:

```ts
// src/app/sp00ky-surreal.worker.ts
import '@surrealdb/wasm/worker';
```

Using `localEngine: 'sqlite'`? Add a second one, `src/app/sp00ky-sqlite.worker.ts`, containing
`import '@spooky-sync/core/sqlite-worker';`.

A TypeScript config for workers (exclude `src/**/*.worker.ts` from `tsconfig.app.json`):

```json
// tsconfig.worker.json
{
  "extends": "./tsconfig.json",
  "compilerOptions": { "outDir": "./out-tsc/worker", "lib": ["es2022", "webworker"], "types": [] },
  "include": ["src/**/*.worker.ts"]
}
```

`angular.json`, under `projects.<app>.architect`:

```json
"build": {
  "builder": "@angular/build:application",
  "options": {
    "webWorkerTsConfig": "tsconfig.worker.json",
    "allowedCommonJsDependencies": ["pino"],
    "assets": [
      { "glob": "**/*", "input": "public" },
      { "glob": "ssp_wasm_bg.wasm", "input": "node_modules/@spooky-sync/ssp-wasm/pkg", "output": "/" },
      { "glob": "index_bg.wasm", "input": "node_modules/@surrealdb/wasm-native/dist", "output": "/" },
      { "glob": "tabs-broker-worker.js", "input": "node_modules/@spooky-sync/core/dist", "output": "/" },
      { "glob": "{sqlite3.wasm,sqlite3-opfs-async-proxy.js}", "input": "node_modules/@sqlite.org/sqlite-wasm/dist", "output": "/" }
    ]
  }
},
"serve": {
  "builder": "@angular/build:dev-server",
  "options": {
    "prebundle": {
      "exclude": [
        "@spooky-sync/client-angular", "@spooky-sync/core", "@spooky-sync/ssp-wasm",
        "@surrealdb/wasm", "@surrealdb/wasm-native", "@sqlite.org/sqlite-wasm"
      ]
    }
  }
}
```

Pre-bundled by the dev server, those packages look for their `.wasm` files in the wrong folder.

## 3. Generate types

```bash
spky generate
```

## 4. Configure the client

```ts
// src/app/db.ts
import type { SyncedDbConfig } from '@spooky-sync/client-angular';
import { schema, SURQL_SCHEMA } from './schema.gen';

export const dbConfig: SyncedDbConfig<typeof schema> = {
  logLevel: 'info',
  schema,
  schemaSurql: SURQL_SCHEMA,
  database: {
    namespace: 'main',
    database: 'main',
    endpoint: 'ws://localhost:8666/rpc',
    store: 'indexeddb',
    workers: {
      surreal: () =>
        new Worker(new URL('./sp00ky-surreal.worker', import.meta.url), { type: 'module' }),
      // localEngine: 'sqlite' only:
      // sqlite: () => new Worker(new URL('./sp00ky-sqlite.worker', import.meta.url), { type: 'module' }),
    },
  },
};
```

## 5. Provide the client

```ts
// src/app/app.config.ts
export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideZonelessChangeDetection(),
    provideRouter(routes),
    provideSp00ky(dbConfig, {
      preload: (db) => db.preload(db.query('config').build()), // optional, best-effort
    }),
  ],
};
```

The client starts in an app initializer; the app renders once it is ready, and the root element
of `index.html` keeps its content until then (`<app-root>Initializing…</app-root>`). If it cannot
start, bootstrap fails: handle it on `bootstrapApplication(...).catch(...)`.

## 6. Query

```ts
@Component({
  selector: 'app-post-list',
  templateUrl: './post-list.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PostList {
  private readonly db = injectDb<typeof schema>();
  readonly authorId = input<string | null>(null);

  protected readonly posts = injectQuery(() => {
    const author = this.authorId();
    return author
      ? this.db.query('post').where({ author }).orderBy('created_at', 'desc').limit(20).build()
      : null; // skip until the input exists
  });
}
```

```html
@if (posts.isLoading()) {
<p>Loading…</p>
} @else if (posts.isEmpty()) {
<p>No posts yet.</p>
} @else { @for (post of posts.data(); track post.id) { <app-post-row [post]="post" /> } }
```

Result signals: `data`, `error`, `isLoading` (cold first load only), `isFetching`,
`isAuthoritative`, `hasData`, `isEmpty` (server said "no rows"), `isSettled` (authoritative and
idle). Options: `{ enabled?, deregisterOnCleanup?, injector? }`. `injectPreload(() => query)` warms
the cache for a likely-next screen.

## 7. Mutate

```ts
await this.db.create('post:' + crypto.randomUUID(), { title });
await this.db.update('post', id, { title });
await this.db.delete('post', row.id);
await this.db.run('backend', 'sendMail', { to });

protected readonly save = injectSubmission((title: string) =>
  this.db.create('post:' + crypto.randomUUID(), { title })
);
// <button [disabled]="save.pending()" (click)="save.submit(title())">Save</button>
```

## 8. Status, files, realtime

```ts
readonly sync = injectSyncStatus();          // sync.isDegraded(), sync.isOffline()
readonly pending = injectPendingMutations(); // pending()
readonly flag = injectFeatureFlag('new-checkout'); // flag.enabled()
readonly body = injectCrdtField('post', this.postId, 'body'); // body()?.getDoc()
readonly files = injectFileUpload('avatars'); // files.upload(path, file)
```

```html
<sp00ky-bucket-image bucket="avatars" [path]="avatar()" class="h-12 w-12 rounded-full">
  <div class="h-full w-full bg-neutral-200"></div>
</sp00ky-bucket-image>
```

## Server-side rendering

During a server render `provideSp00ky` never starts the client and the engine-backed `inject*`
functions throw. Wrap sp00ky-backed components in `@defer { ... } @placeholder { ... }`, or give
their routes `renderMode: RenderMode.Client`.
