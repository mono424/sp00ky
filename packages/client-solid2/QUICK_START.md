# @spooky-sync/client-solid2 — Quick Start

Solid **2.0** native bindings for the Sp00ky reactive local-first SurrealDB framework. This is the Solid 2 counterpart of `@spooky-sync/client-solid` (which stays on Solid 1.x); the two packages coexist until Solid 2.0 is stable and apps migrate.

## Install

```bash
pnpm add @spooky-sync/client-solid2 solid-js@2.0.0-rc.0 @solidjs/web@2.0.0-rc.0 @solidjs/signals@2.0.0-rc.0
```

> **Coordinated RC**: `solid-js`, `@solidjs/web`, and `@solidjs/signals` must be on matching `2.0.0-rc.x` versions. Pin exact versions; this package's semantics probes (`rc-semantics.test.ts`) catch drift on bumps. `jsxImportSource` is `@solidjs/web` in Solid 2.

## Provider

```tsx
import { Sp00kyProvider } from '@spooky-sync/client-solid2';
import { schema } from './generated/schema';
import schemaSurql from './generated/schema.surql?raw';

function App() {
  return (
    <Sp00kyProvider
      config={{
        database: { endpoint: 'ws://localhost:8000', namespace: 'my_ns', database: 'my_db', store: 'indexeddb' },
        schema,
        schemaSurql,
      }}
      fallback={<div>Loading database…</div>}
      preload={async (db) => { await db.preload(db.query('config').build()); }}
    >
      <MyApp />
    </Sp00kyProvider>
  );
}
```

Same props as client-solid's provider (`config`, `fallback`, `preload`, `onReady`, `onError`). A live mounted client is deliberately not closed on unmount (see source comment).

## createQuery

`createQuery` replaces `useQuery` (which remains as a deprecated alias). Same overloads: `(query, options?)` with context, or `(db, query, options?)`.

```tsx
import { createQuery, useDb } from '@spooky-sync/client-solid2';

function PostList() {
  const db = useDb();
  const posts = createQuery(db.query('post').orderBy('createdAt', 'desc').limit(20).build());

  // Accessor style — data() never suspends, born as [] and live thereafter:
  return (
    <Show when={!posts.isLoading()} fallback={<div>Loading…</div>}>
      <For each={posts.data()}>{(post) => <PostRow post={post} />}</For>
    </Show>
  );
}
```

Reactive inputs: pass a thunk, read your signals inside it.

```tsx
const post = createQuery(() => (postId() ? db.query('post').where({ id: postId() }).one().build() : null));
```

### Suspension style (`<Loading>`)

`q.ready()` reads the same data but participates in Solid 2's boundary protocol: it pends until the first real result.

```tsx
import { Loading } from 'solid-js';

<Loading fallback={<Skeleton />}>
  <For each={posts.ready()}>{(post) => <PostRow post={post} />}</For>
</Loading>
```

### Result surface

| Accessor | Meaning |
|---|---|
| `data()` | Rows (or row/`null` for `.one()`). Never suspends, never throws. Keyed-reconciled in place: unchanged rows keep identity, `<For>` is notified on add/remove/reorder. |
| `ready()` | Same data, suspends into the nearest `<Loading>` until first result or error. |
| `error()` | Registration/sync error (e.g. SSP 503 during bootstrap). Never thrown into the render tree; the sync scheduler retries underneath. |
| `isLoading()` | Cold first load only: nothing painted for this identity yet, server not heard, no error. A cached paint ends it; so does the server answering with zero rows. |
| `isFetching()` | Sync engine is pulling records for this query. |
| `isAuthoritative()` | Server membership is known for this identity (registration/poll landed, or a previous session's membership was read on boot). Latched until the identity changes. |
| `hasData()` | `data()` holds rows (or the `.one()` row). |
| `isEmpty()` | Authoritative AND no data — the server said "no rows". Render empty states on this. |
| `isSettled()` | Authoritative AND idle — windowed lists may trust a short result as the true end of the list. Re-enters on later sync rounds; latch it for a sticky verdict. |

Options: `{ enabled?: () => boolean, deregisterOnCleanup?: boolean }` — both as in client-solid.

### Gotcha: rows are store proxies

Solid 2 stores wrap class instances too, and serve methods bound. A `RecordId` read out of a row works for display and for `db.delete('table', row.id)`, but if you pass whole row objects back into surrealdb APIs that check `instanceof`, unwrap first with `snapshot(row)` (exported by `solid-js`).

## Mutations

Engine writes are already optimistic local-first (local commit → live queries re-emit → outbox sync; `db.run()` is itself an outbox job row with `status: 'pending'`). Plain calls:

```ts
await db.create('post:xyz', { title: 'Hi' });
await db.update('post', 'post:xyz', { title: 'Edited' });
await db.delete('post', row.id);
await db.run('backend', 'sendMail', { to });
```

For button pending/error state, wrap with `createSubmission`:

```tsx
const save = createSubmission((title: string) => db.create(`post:${crypto.randomUUID()}`, { title }));
<button disabled={save.pending()} onClick={() => save.submit(title())}>Save</button>
```

Track a backend job's progress by querying its outbox row: `createQuery(() => db.query('job_outbox').where({ id: jobId() }).one().build())`.

## Status hooks

Same shapes as client-solid, rebuilt on async-iterable-backed memos: `useSyncStatus`, `useStorageStatus`, `usePendingMutations`, `useSyncActivity` (new: `isDownloading()` after 200 ms of fetching, `isUploading()` after 150 ms of a queued write, for a traffic indicator), `useFeatureFlag`, `useAppRelease`, `useCrdtField`, `useFileUpload`, `useDownloadFile`, `createPreload`.

## Migrating from client-solid

| client-solid (Solid 1) | client-solid2 (Solid 2) |
|---|---|
| `useQuery(q)` | `createQuery(q)` (alias `useQuery` kept) |
| `data()` may be `undefined` pre-fetch | `data()` is `[]` / `null` pre-fetch |
| No suspension | `ready()` + `<Loading>` |
| `useDb()` throws custom error | same API (Solid 2 context throws on missing provider) |
| everything else | unchanged call signatures |
