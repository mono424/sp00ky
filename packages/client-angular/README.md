# @spooky-sync/client-angular

Angular bindings for the Sp00ky reactive local-first SurrealDB framework, built on signals. Needs
Angular 21 or newer; zoneless and zone.js apps both work. Browser only: the core client runs its
engines as WebAssembly in web workers with IndexedDB / OPFS storage, so during a server render the
client is never started (render sp00ky-backed components in `@defer` or on `RenderMode.Client`
routes).

```bash
pnpm add @spooky-sync/core @spooky-sync/client-angular @surrealdb/wasm
```

```ts
// app.config.ts
export const appConfig: ApplicationConfig = {
  providers: [provideZonelessChangeDetection(), provideSp00ky(dbConfig)],
};

// post-list.ts
@Component({
  selector: 'app-post-list',
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './post-list.html',
})
export class PostList {
  private readonly db = injectDb<typeof schema>();
  protected readonly posts = injectQuery(() =>
    this.db.query('post').orderBy('created_at', 'desc').limit(20).build()
  );
}
```

```html
<!-- post-list.html -->
@if (posts.isLoading()) {
<p>Loading…</p>
} @else if (posts.isEmpty()) {
<p>No posts yet.</p>
} @else { @for (post of posts.data(); track post.id) {
<p>{{ post.title }}</p>
} }
```

What "native" means here:

- Signals, no RxJS. Every result field is a read-only `Signal` (`posts.data()`,
  `posts.isLoading()`); inputs that follow state are signals or functions
  (`() => this.db.query(...).build()`, an `input()`).
- `provideSp00ky(config)` is an environment provider: it starts the client in an app initializer,
  so the app renders once the client is ready. `SyncedDb` is the DI token (`injectDb()`,
  `inject(SyncedDb)`).
- `inject*` functions run in an injection context and end with it through `DestroyRef`; each takes
  `{ injector }` to run anywhere else. Status functions need no component, so they also live in
  `providedIn: 'root'` services.
- `injectQuery` structurally shares each live emission with the previous one: rows matched by `id`
  keep their object identity, and a deep-equal emission does not notify.
- `<sp00ky-bucket-image>`, `canvas[sp00kyBlurhash]` and `img[sp00kyImageGate]` are standalone,
  `OnPush`, signal-input components / directives doing their DOM work in `afterRenderEffect`.
- Mutations stay plain `await db.create(...)` calls: the engine is already optimistic local-first.
  `injectSubmission` only adds button pending and error state.

The query flags (`isLoading`, `isFetching`, `isAuthoritative`, `hasData`, `isEmpty`, `isSettled`)
follow the same contract as `@spooky-sync/client-react`, `client-svelte` and `client-solid2`.

**Angular CLI apps need a few build settings**: the CLI does not bundle a worker started inside a
dependency, nor copy the `.wasm` files a dependency references. The app bundles a one-line worker
entry (`import '@surrealdb/wasm/worker';`) and hands it to the client through
`database.workers.surreal`, copies four `.wasm` / worker files with `assets`, and excludes the
client's packages from the dev server's `prebundle`. `QUICK_START.md` has every line.

The package is built with ng-packagr (Angular Package Format, partial compilation). See
`QUICK_START.md` for a walkthrough, `AGENTS.md` for the full API, and the
[Angular guide](https://mono424.github.io/sp00ky/docs/guide/angular).

```bash
pnpm --filter @spooky-sync/client-angular test        # builds, then runs vitest on the build
pnpm --filter @spooky-sync/client-angular type-check  # ngc strict templates + test types
pnpm --filter @spooky-sync/client-angular build       # ng-packagr -> dist/ (published from there)
```
