---
name: sp00ky-react
description: >-
  React integration for the Sp00ky reactive local-first SurrealDB framework.
  Use when a React (18 or 19, browser) app uses sp00ky: setting up
  Sp00kyProvider, useQuery for live data, mutations via plain db calls or
  useSubmission, status hooks (sync, storage, feature flags, app release),
  CRDT fields, peer to peer channels, file upload/download and BucketImage.
  Not for React Native (the core runs on WebAssembly); for Solid apps use the
  sp00ky-solid or sp00ky-solid2 skill.
metadata:
  author: sp00ky-sync
  version: '0.0.1'
---

# Sp00ky React Client

`@spooky-sync/client-react` targets React 18 and 19 in the browser. Hooks return plain values and read through `useSyncExternalStore`. `useQuery` results are structurally shared between live emissions: rows matched by `id` keep their object identity, a deep-equal emission does not re-render.

## Setup

```tsx
import { Sp00kyProvider } from '@spooky-sync/client-react';

<Sp00kyProvider config={{ database: {...}, schema, schemaSurql }} fallback={<Splash />}>
  <MyApp />
</Sp00kyProvider>
```

`config` is read once at mount. Vite needs `vite-plugin-wasm`, `vite-plugin-top-level-await`, `build.target: 'esnext'` and `optimizeDeps.exclude: ['@surrealdb/wasm', '@surrealdb/wasm-native', '@sqlite.org/sqlite-wasm']`.

## Queries

```tsx
const db = useDb<typeof schema>();
const posts = useQuery(db.query('post').orderBy('created_at', 'desc').limit(20).build());

if (posts.isLoading) return <Spinner />;
if (posts.isEmpty) return <Empty />;
return posts.data.map((p) => <PostRow key={String(p.id)} post={p} />);
```

- Build the query inline; the hook re-registers only when its identity (surql + vars) changes.
- `null` skips: `useQuery(id ? db.query('post').where({ id }).one().build() : null)`.
- Result: `data`, `error`, `isLoading` (cold first load only), `isFetching`, `isAuthoritative`, `hasData`, `isEmpty` (server said "no rows"), `isSettled` (authoritative and idle).
- Options: `{ enabled?: boolean, deregisterOnCleanup?: boolean }`.
- `usePreload(query)` warms the cache for a likely-next screen.

## Mutations

Plain awaited calls; the engine is optimistic local-first:

```ts
await db.create('post:' + crypto.randomUUID(), { title });
await db.update('post', id, { title });
await db.delete('post', row.id);
await db.run('backend', 'route', payload);
```

Button state: `const save = useSubmission(fn)` gives `submit`, `pending`, `error`, `result`, `clearError`.

## Hooks

`useDb`, `usePendingMutations`, `useSyncStatus`, `useSyncActivity`, `useStorageStatus`, `useImpersonation`, `useFeatureFlag`, `useAppRelease`, `useCrdtField(table, id, field)`, `useChannel(query, peers)`, `useFileUpload(bucket)`, `useDownloadFile(bucket, path)`, `useBlurhash`, `useBucketImage`, plus `<BucketImage>` and `<Blurhash>` components.

## Gotchas

- No Suspense mode; branch on `isLoading`.
- React Native is not supported.
- Row ids are `RecordId`; key with `String(row.id)`.
- Every hook needs `<Sp00kyProvider>` above it, except the `(db, ...)` overloads.

Full API: `node_modules/@spooky-sync/client-react/AGENTS.md`.
