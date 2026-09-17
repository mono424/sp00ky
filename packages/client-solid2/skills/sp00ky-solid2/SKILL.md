---
name: sp00ky-solid2
description: >-
  Solid 2.0 native integration for the Sp00ky reactive local-first SurrealDB
  framework. Use when an app is on solid-js 2.x (rc or later): setting up
  Sp00kyProvider, createQuery for reactive data (accessor or <Loading>
  suspension style), mutations via plain db calls or createSubmission, status
  hooks, file upload/download. For Solid 1.x apps use the sp00ky-solid skill
  (@spooky-sync/client-solid) instead.
metadata:
  author: sp00ky-sync
  version: "0.0.1"
---

# Sp00ky Solid 2 Client

`@spooky-sync/client-solid2` targets **solid-js 2.0** (`^2.0.0-rc.0`, peer with `@solidjs/signals`; apps also need `@solidjs/web` and `jsxImportSource: "@solidjs/web"`). Coordinated RC: all three Solid packages on matching versions.

Internally the binding is Solid-2 native: query results live in a plain store, written from the engine's live subscription and merged keyed by `id` (row identity preserved, coarse `<For>` readers notified). It is deliberately NOT an async-generator projection: a live query's generator never returns, which leaves its node permanently pending, and Solid 2 holds every navigation transition until the new tree's pending nodes settle - the first navigation into a screen that opens a query would never commit. Status hooks are async-iterable-backed memos with `loadingValue`; teardown runs from the effect's returned cleanup.

## Setup

```tsx
import { Sp00kyProvider } from '@spooky-sync/client-solid2';

<Sp00kyProvider config={{ database: {...}, schema, schemaSurql }} fallback={<Splash />}>
  <MyApp />
</Sp00kyProvider>
```

Props identical to client-solid: `config`, `fallback`, `preload` (awaited gate before revealing UI), `onReady`, `onError`.

## createQuery

```tsx
const posts = createQuery(db.query('post').orderBy('createdAt', 'desc').limit(20).build());
// or reactive: createQuery(() => id() ? db.query('post').where({ id: id() }).one().build() : null)
```

Returns `{ data, ready, error, isLoading, isFetching, isAuthoritative, hasData, isEmpty, isSettled }`:

- `data()` — non-suspending; `[]`/`null` before first result, keyed-reconciled live rows after.
- `ready()` — suspending read for `<Loading fallback={...}>` (Solid 2's renamed Suspense).
- `error()` — registration failure (SSP 503 NOT_READY etc.); never thrown into the tree, sync retries underneath.
- `isAuthoritative()` — server membership known for this identity (latched until it changes).
- `hasData()` — `data()` holds rows / the `.one()` row.
- `isEmpty()` — authoritative AND no data: the server said "no rows". Empty states go on this, never on `!hasData()`.
- `isSettled()` — authoritative AND not fetching; gate windowed-list end detection on this. Latch it for a sticky verdict.
- Options: `enabled?: () => boolean`, `deregisterOnCleanup?: boolean`.
- `useQuery` is a deprecated alias of `createQuery`.

Rows are Solid 2 store proxies; class instances (RecordId) are proxied with bound methods. `db.delete('table', row.id)` works; for surrealdb APIs that check `instanceof`, unwrap with `snapshot(row)` first.

## Mutations

Engine is optimistic local-first end to end (local commit → live re-emit → outbox sync; `db.run()` creates an outbox job row `status: 'pending'`). Use plain `db.create/update/delete/run`. For UI pending/error state:

```tsx
const save = createSubmission((t: string) => db.create(`post:${crypto.randomUUID()}`, { title: t }));
save.submit(...); save.pending(); save.error(); save.result(); save.clearError();
```

No `action()`/`createOptimisticStore` layer — deliberate; see create-submission.ts.

## Hooks

Same shapes as client-solid: `useDb`, `usePendingMutations`, `useSyncStatus`, `useImpersonation`, `useSyncActivity` (`isDownloading` / `isUploading` / `fetchingQueries` / `pendingMutations`, for a traffic indicator), `useStorageStatus`, `useFeatureFlag`, `useAppRelease`, `useCrdtField`, `useFileUpload`, `useDownloadFile`, `createPreload`.

## Testing note

Under node, `solid-js`'s `node` export condition resolves the SSR build where user effects never run. Vitest configs must set `resolve.conditions: ['browser', 'development']` (see this package's vitest.config.ts).
