# Quick start: Sp00ky with Svelte 5

The same content as the docs' [Svelte guide](https://mono424.github.io/sp00ky/docs/guide/svelte).

## 1. Install

```bash
pnpm add @spooky-sync/core @spooky-sync/client-svelte
pnpm add -D vite-plugin-wasm
```

## 2. Configure Vite

The core client loads its engines as WebAssembly and starts web workers, both found next to its own files. Keep the client's packages out of dependency pre-bundling (pre-bundled, they look in the wrong folder), pre-bundle the core's CommonJS dependencies instead, and build for `esnext`:

```ts
// vite.config.ts
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte'; // SvelteKit: sveltekit() from '@sveltejs/kit/vite'
import wasm from 'vite-plugin-wasm';

export default defineConfig({
  plugins: [svelte(), wasm()],
  optimizeDeps: {
    // Served as is: pre-bundled, these look for their .wasm files and
    // workers in the wrong folder.
    exclude: [
      '@spooky-sync/client-svelte',
      '@spooky-sync/core',
      '@spooky-sync/ssp-wasm',
      '@surrealdb/wasm',
      '@surrealdb/wasm-native',
      '@sqlite.org/sqlite-wasm',
    ],
    // The excluded core's CommonJS dependencies, pre-bundled for it.
    include: ['@spooky-sync/core > pino', '@spooky-sync/core > fast-json-patch'],
  },
  // The client uses top-level await.
  build: { target: 'esnext' },
});
```

In SvelteKit the client is browser-only, so turn server rendering off:

```ts
// src/routes/+layout.ts
export const ssr = false;
```

## 3. Generate types and configure the client

```bash
spky generate
```

```ts
// src/lib/db.ts
import type { SyncedDbConfig } from '@spooky-sync/client-svelte';
import { schema, SURQL_SCHEMA } from './schema.gen';

export const dbConfig: SyncedDbConfig<typeof schema> = {
  schema,
  schemaSurql: SURQL_SCHEMA,
  database: {
    namespace: 'main',
    database: 'main',
    endpoint: 'ws://localhost:8666/rpc', // spky dev
    store: 'indexeddb',
  },
};
```

## 4. Mount the provider

```svelte
<!-- src/routes/+layout.svelte -->
<script lang="ts">
  import { Sp00kyProvider } from '@spooky-sync/client-svelte';
  import { dbConfig } from '$lib/db';

  let { children } = $props();
</script>

<Sp00kyProvider config={dbConfig} onError={(e) => console.error(e)}>
  {#snippet fallback()}<div>Initializing database…</div>{/snippet}
  {@render children()}
</Sp00kyProvider>
```

`config` is read once at mount (wrap the provider in `{#key}` to switch databases). `preload` is
awaited before the children render. The client is created in `onMount`, so a server render only
shows the fallback.

## 5. Query

```svelte
<script lang="ts">
  import { useDb, useQuery } from '@spooky-sync/client-svelte';
  import type { schema } from '$lib/schema.gen';

  let { authorId }: { authorId: string | null } = $props();
  const db = useDb<typeof schema>();

  // A getter: re-read when `authorId` changes, re-registered only when the
  // query's identity (surql + vars) changes. Returning null skips.
  const posts = useQuery(() =>
    authorId ? db.query('post').where({ author: authorId }).limit(20).build() : null
  );
</script>

{#if posts.isLoading}
  <p>Loading…</p>
{:else if posts.isEmpty}
  <p>Nothing here.</p>
{:else}
  {#each posts.data as post (String(post.id))}<p>{post.title}</p>{/each}
{/if}
```

Fields are getters: never `const { data } = useQuery(...)`, that copy never updates.

## 6. Write

```svelte
<script lang="ts">
  import { useDb, useSubmission } from '@spooky-sync/client-svelte';

  const db = useDb<typeof schema>();
  let title = $state('');
  const save = useSubmission((t: string) =>
    db.create('post:' + crypto.randomUUID(), { title: t })
  );
</script>

<input bind:value={title} />
<button disabled={save.pending} onclick={() => save.submit(title)}>Save</button>
{#if save.error}<p>{save.error.message}</p>{/if}
```

The write commits locally first; the live query above re-renders before the server confirms.

## 7. Status

```svelte
<script lang="ts">
  import { useSyncStatus, usePendingMutations } from '@spooky-sync/client-svelte';

  const sync = useSyncStatus();
  const pending = usePendingMutations();
</script>

{#if sync.isDegraded}<div>Offline, {pending.current} change(s) queued.</div>{/if}
```
