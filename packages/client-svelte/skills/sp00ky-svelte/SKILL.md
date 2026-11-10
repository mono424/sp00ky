---
name: sp00ky-svelte
description: >-
  Svelte 5 (runes) integration for the Sp00ky reactive local-first SurrealDB
  framework. Use when a Svelte 5 or SvelteKit app uses sp00ky: setting up
  Sp00kyProvider, useQuery with getter inputs for live data, mutations via
  plain db calls or useSubmission, status hooks (sync, storage, feature flags,
  app release), CRDT fields, peer to peer channels, file upload/download and
  BucketImage. Not for Svelte 4; for React apps use the sp00ky-react skill, for
  Solid apps sp00ky-solid or sp00ky-solid2.
metadata:
  author: sp00ky-sync
  version: '0.0.1'
---

# Sp00ky Svelte Client

`@spooky-sync/client-svelte` targets Svelte 5.29+ in the browser. Hooks return objects whose fields are reactive getters; inputs that follow state are getters. `useQuery` rows are structurally shared between live emissions: rows matched by `id` keep their object identity, a deep-equal emission changes nothing.

## Setup

```svelte
<!-- src/routes/+layout.svelte -->
<script lang="ts">
  import { Sp00kyProvider } from '@spooky-sync/client-svelte';
  let { children } = $props();
</script>

<Sp00kyProvider config={{ database: {...}, schema, schemaSurql }}>
  {#snippet fallback()}<Splash />{/snippet}
  {@render children()}
</Sp00kyProvider>
```

`config` is read once at mount. SvelteKit: `export const ssr = false` in the root `+layout.ts` (the core is browser-only). Vite needs `vite-plugin-wasm`, `vite-plugin-top-level-await`, `build.target: 'esnext'` and `optimizeDeps.exclude: ['@surrealdb/wasm', '@surrealdb/wasm-native', '@sqlite.org/sqlite-wasm']`.

## Queries

```svelte
<script lang="ts">
  const db = useDb<typeof schema>();
  const posts = useQuery(() => db.query('post').orderBy('created_at', 'desc').limit(20).build());
</script>

{#if posts.isLoading}<Spinner />
{:else if posts.isEmpty}<Empty />
{:else}
  {#each posts.data as p (String(p.id))}<PostRow post={p} />{/each}
{/if}
```

- Pass a getter; it is re-read when the state it reads changes, and the query re-registers only when its identity (surql + vars) changes.
- Return `null` to skip: `useQuery(() => (id ? db.query('post').where({ id }).one().build() : null))`.
- Result getters: `data`, `error`, `isLoading` (cold first load only), `isFetching`, `isAuthoritative`, `hasData`, `isEmpty` (server said "no rows"), `isSettled` (authoritative and idle).
- Never destructure a result; read `posts.data` where it is used.
- Options: `{ enabled?: boolean | (() => boolean), deregisterOnCleanup?: boolean }`.
- `usePreload(() => query)` warms the cache for a likely-next screen.

## Mutations

Plain awaited calls; the engine is optimistic local-first. `$state` payloads are fine (copied before the workers):

```ts
await db.create('post:' + crypto.randomUUID(), { title });
await db.update('post', id, { title });
await db.delete('post', row.id);
await db.run('backend', 'route', payload);
```

Button state: `const save = useSubmission(fn)` gives `submit`, `pending`, `error`, `result`, `clearError`.

## Hooks

`useDb`, `usePendingMutations().current`, `useSyncStatus`, `useSyncActivity`, `useStorageStatus`, `useImpersonation`, `useFeatureFlag`, `useAppRelease`, `useCrdtField(table, () => id, field).current`, `useChannel(() => query, () => peers)`, `useFileUpload(bucket)`, `useDownloadFile(bucket, () => path)`, `useBlurhash`, `useBucketImage` (with the `{@attach image.gate}` attachment), plus `<BucketImage>` (`class`, `imgClass`, `fallback` snippet) and `<Blurhash>`. `setDbContext(db)` provides a self-created client instead of the provider.

## Gotchas

- Hooks run during component init (top of `<script>`), not in handlers or after `await`.
- No suspense mode; branch on `isLoading`.
- Svelte 4 is not supported.
- Row ids are `RecordId`; key with `String(row.id)`.
- Every hook needs `<Sp00kyProvider>` (or `setDbContext`) above it, except the `(db, ...)` forms.

Full API: `node_modules/@spooky-sync/client-svelte/AGENTS.md`.
