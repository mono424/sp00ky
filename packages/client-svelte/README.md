# @spooky-sync/client-svelte

Svelte 5 bindings for the Sp00ky reactive local-first SurrealDB framework, built on runes. Needs
Svelte 5.29 or newer (attachments); Svelte 4 is not supported. Browser only: the core client runs
its engines as WebAssembly with IndexedDB / OPFS storage, so in SvelteKit turn server rendering off
(`export const ssr = false` in the root `+layout.ts`).

```bash
pnpm add @spooky-sync/core @spooky-sync/client-svelte
```

```svelte
<!-- Posts.svelte -->
<script lang="ts">
  import { useDb, useQuery } from '@spooky-sync/client-svelte';

  const db = useDb<typeof schema>();
  const posts = useQuery(() => db.query('post').orderBy('created_at', 'desc').limit(20).build());
</script>

{#if posts.isLoading}
  <p>Loading…</p>
{:else if posts.isEmpty}
  <p>No posts yet.</p>
{:else}
  <ul>
    {#each posts.data as post (String(post.id))}<li>{post.title}</li>{/each}
  </ul>
{/if}

<!-- +layout.svelte -->
<Sp00kyProvider config={dbConfig}>
  {#snippet fallback()}<Splash />{/snippet}
  {@render children()}
</Sp00kyProvider>
```

What "native" means here:

- Runes, no stores. A hook returns an object whose fields are reactive getters (`posts.data`,
  `posts.isLoading`); inputs that follow state are getters (`() => db.query(...).build()`).
- `useQuery` keeps rows in `$state.raw` with structural sharing between live emissions: rows are
  matched by `id`, unchanged rows keep their object identity (a keyed `{#each}` only updates what
  changed), and a deep-equal emission changes nothing. Rows are plain objects, never proxies.
- Status hooks (`useSyncStatus`, `usePendingMutations`, `useStorageStatus`, ...) are built on
  `createSubscriber`: subscribed only while read, always the engine's current value.
- Registrations, CRDT fields, channels and file leases live in `$effect` and end with the component.
- Writes copy `$state` proxies before they reach the core's workers, leaving `RecordId`, `Date` and
  bytes as they are.
- Mutations stay plain `await db.create(...)` calls: the engine is already optimistic local-first.
  `useSubmission` only adds button pending and error state.

The query flags (`isLoading`, `isFetching`, `isAuthoritative`, `hasData`, `isEmpty`, `isSettled`)
follow the same contract as `@spooky-sync/client-react` and `client-solid2`.

The package ships Svelte source (`.svelte`, `.svelte.js`) built by `svelte-package`, compiled by the
app's own Svelte compiler. See `QUICK_START.md` for a walkthrough, `AGENTS.md` for the full API, and
the [Svelte guide](https://mono424.github.io/sp00ky/docs/guide/svelte).

```bash
pnpm --filter @spooky-sync/client-svelte test
pnpm --filter @spooky-sync/client-svelte type-check
pnpm --filter @spooky-sync/client-svelte build
```
