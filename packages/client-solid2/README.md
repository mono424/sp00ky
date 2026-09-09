# @spooky-sync/client-solid2

Solid **2.0** native bindings for the Sp00ky reactive local-first SurrealDB framework.

Counterpart of `@spooky-sync/client-solid` (Solid 1.x); the two coexist until Solid 2.0 is stable. Peer deps: `solid-js@^2.0.0-rc.0` + `@solidjs/signals@^2.0.0-rc.0` (coordinated RC — pin matching versions, apps also need `@solidjs/web`).

What "native" means here:

- Query results are a plain store written from the engine's live subscription and merged keyed by `id`: row identity preserved, coarse `<For>` readers notified, only changed fields written. Not an async-generator projection - see `create-query.ts` for why a never-returning generator breaks navigation transitions.
- `createQuery` exposes both worlds: non-suspending accessors (`data`, `isLoading`, `isFetching`, `isAuthoritative`, `hasData`, `isEmpty`, `isSettled`, `error`) and a suspending `ready()` for `<Loading>` boundaries. Born committed, so local-first cache paints never suspend. `isLoading()` is true only on a cold first load (nothing cached, server not heard yet); `isEmpty()` is the server saying "no rows".
- Status hooks (`useSyncStatus`, `useSyncActivity`, `useStorageStatus`, `usePendingMutations`, feature flags, app release) are async-iterable-backed memos with `loadingValue`. `useSyncActivity` is the traffic indicator pair: `isDownloading()` (queries fetching for longer than 200 ms) and `isUploading()` (writes queued for longer than 150 ms), plus the raw `fetchingQueries()` / `pendingMutations()` counts.
- Mutations stay plain async calls — the engine is already optimistic local-first end to end (local commit → live re-emit → outbox sync). `createSubmission` adds button pending/error state.

See `QUICK_START.md` for usage and the migration table from client-solid, and `src/lib/__tests__/rc-semantics.test.ts` for the probed Solid 2 rc contracts this package depends on (run them after every Solid version bump).

```bash
pnpm --filter @spooky-sync/client-solid2 test
pnpm --filter @spooky-sync/client-solid2 build
```
