---
name: sp00ky-solid
description: >-
  SolidJS integration for the Sp00ky reactive local-first SurrealDB framework.
  Use when setting up Sp00kyProvider, using useQuery for reactive data, building
  queries with QueryBuilder in SolidJS components, handling mutations, auth,
  file uploads/downloads, or working with Sp00ky types like Model and RecordId.
metadata:
  author: sp00ky-sync
  version: "0.0.1"
---

# Sp00ky SolidJS Client

`@spooky-sync/client-solid` provides SolidJS bindings for the Sp00ky framework. It wraps `@spooky-sync/core` with a context provider, reactive `useQuery` hook, and file operation hooks.

## Setup

```tsx
import { Sp00kyProvider } from '@spooky-sync/client-solid';
import { schema } from './generated/schema';
import schemaSurql from './generated/schema.surql?raw';

function App() {
  return (
    <Sp00kyProvider
      config={{
        database: {
          endpoint: 'ws://localhost:8000',
          namespace: 'my_ns',
          database: 'my_db',
          store: 'indexeddb',
        },
        schema,
        schemaSurql,
        logLevel: 'info',
      }}
      fallback={<div>Loading database...</div>}
      onReady={(db) => console.log('DB ready')}
      onError={(err) => console.error('DB failed', err)}
    >
      <MyApp />
    </Sp00kyProvider>
  );
}
```

### Sp00kyProvider Props

| Prop | Type | Description |
|------|------|-------------|
| `config` | `SyncedDbConfig<S>` | Same as `Sp00kyConfig` from core |
| `fallback` | `JSX.Element` | Shown while the database is initializing |
| `preload` | `(db: SyncedDb<S>) => Promise<void>` | Prewarm essential data before revealing the UI. Runs after `init()`; `fallback` stays until it resolves. See [Preload](#preload). |
| `onReady` | `(db: SyncedDb<S>) => void` | Called when initialization succeeds |
| `onError` | `(error: Error) => void` | Called if initialization fails |
| `children` | `JSX.Element` | App content, rendered after init |

## useQuery

The primary hook for reactive data fetching. Queries automatically re-subscribe when inputs change.

### Context-based usage (recommended)

```tsx
import { useQuery } from '@spooky-sync/client-solid';
import { QueryBuilder } from '@spooky-sync/query-builder';
import { schema } from './generated/schema';

function PostList() {
  const db = useDb();

  // Static query
  const posts = useQuery(
    db.query('post').orderBy('createdAt', 'desc').limit(20).build()
  );

  return (
    <Show when={!posts.isLoading()} fallback={<div>Loading...</div>}>
      <For each={posts.data()}>
        {(post) => <div>{post.title}</div>}
      </For>
    </Show>
  );
}
```

### Reactive queries (function form)

Wrap the query in a function to make it reactive to signal changes:

```tsx
function UserPosts(props: { userId: string }) {
  const db = useDb();

  // Query re-runs when props.userId changes
  const posts = useQuery(
    () => db.query('post')
      .where({ author: props.userId })
      .related('author')
      .build()
  );

  return <For each={posts.data()}>{(post) => <div>{post.title}</div>}</For>;
}
```

### Conditional queries

Use the `enabled` option to conditionally run queries:

```tsx
const [userId, setUserId] = createSignal<string | null>(null);

const user = useQuery(
  () => userId()
    ? db.query('user').where({ id: userId()! }).one().build()
    : undefined,
  { enabled: () => userId() !== null }
);
```

### Return value

| Property | Type | Description |
|----------|------|-------------|
| `data` | `() => T \| undefined` | Reactive accessor for query results |
| `error` | `() => Error \| undefined` | Reactive accessor for errors |
| `isLoading` | `() => boolean` | `true` until first data arrives |

### Explicit db overload

You can also pass the `SyncedDb` instance directly (legacy):

```tsx
const posts = useQuery(db, db.query('post').build());
```

## Preload

Prewarm a query's results (and any embedded `.related()` children) into the local cache so a
later `useQuery` for the same data paints instantly instead of waiting on the network. Preload
does NOT register a live view — it's a one-shot snapshot; the data freshens on use, when the
real `useQuery` mounts.

### `db.preload(query, options?)` — awaitable, a registered query

```tsx
// Never resolved on this device: the promise AWAITS the server's membership
// and every record, so you can block on it.
// Resolved before: returns instantly, no network.
await db.preload(db.query('config').build());
```

Behavior:

- **Never resolved** — no durable membership for this query in the current bucket → the query is
  registered and the promise resolves once the server's membership and every record are local.
- **Resolved before** — a `_00_view` row exists → resolves immediately and the rows paint from the
  local store. Survives reloads; a bucket switch re-reads the new bucket's rows.
- The query stays registered and live; a view that mounts the same query attaches to it. Nothing
  mounts it → it is evicted a ttl after registration.

`options`:

| Option | Type | Description |
|--------|------|-------------|
| `signal` | `AbortSignal` | Abort a cold wait (the registration continues in the background). |
| `refresh` / `staleTime` | — | Deprecated, ignored. |

### Blocking first-load: `Sp00kyProvider` `preload` prop

Gate the whole app on essential data. The `fallback` stays visible until preload resolves; on
warm loads it's instant, so there's no perceptible gate after the first run.

```tsx
<Sp00kyProvider
  config={config}
  fallback={<Splash />}
  preload={(db) => db.preload(db.query('config').build())}
>
  <App />
</Sp00kyProvider>
```

### `createPreload(query, options?)` — reactive, fire-and-forget

For prewarming data the user is likely to open next (e.g. the detail view for each row in a
list). Reactive and non-blocking; mirrors `useQuery`'s overloads and `enabled` option, plus
`signal`.

```tsx
import { createPreload } from '@spooky-sync/client-solid';

// Inside a list row — warm the detail query so navigation is instant:
createPreload(() =>
  db.query('thread').where({ id: thread.id })
    .related('author')
    .related('comments', (q) => q.related('author').orderBy('created_at', 'desc').limit(3))
    .one().build()
);
```

## useDb

Access the `SyncedDb` instance from context:

```tsx
import { useDb } from '@spooky-sync/client-solid';

function MyComponent() {
  const db = useDb();
  // db.query(), db.create(), db.update(), db.delete(), db.auth, etc.
}
```

## Mutations

Use the `SyncedDb` instance (from `useDb()`) for mutations:

```tsx
const db = useDb();

// Create
await db.create('post:abc', { title: 'Hello', body: 'World', author: 'user:alice' });

// Update
await db.update('post', 'post:abc', { title: 'Updated' });

// Update with debounce
await db.update('post', 'post:abc', { body: newText }, {
  debounced: { key: 'recordId_x_fields', delay: 300 },
});

// Delete
await db.delete('post', 'post:abc');
```

## Authentication

```tsx
const db = useDb();

await db.auth.signUp('user_access', { email, password, name });
await db.auth.signIn('user_access', { email, password });
await db.auth.signOut();

// Subscribe to auth state
const unsub = db.auth.subscribe((userId) => { ... });
```

## File Upload & Download

See [references/file-hooks.md](references/file-hooks.md) for details.

```tsx
import { useFileUpload, useDownloadFile } from '@spooky-sync/client-solid';

// Upload
const { upload, isUploading, error } = useFileUpload('avatars');
await upload('alice/photo.png', file);

// Download (reactive)
const { url, isLoading } = useDownloadFile('avatars', () => user()?.avatarPath);
```

## Backend Runs

Use `db.run()` to trigger server-side operations via the outbox pattern. See the `sp00ky-core` skill for full details on `db.run()` and how it works.

### Basic Usage

```tsx
const db = useDb();
await db.run('api', '/spookify', { id: threadId });
```

### Entity Linking with `assignedTo`

Pass `assignedTo` to link the job to an entity. This enables permission scoping and lets you query job status via relationships:

```tsx
const db = useDb();

// Trigger backend run linked to a thread
await db.run('api', '/spookify', { id: threadData.id }, {
  assignedTo: threadData.id,  // Links the job record to this thread
});
```

### Tracking Job Status Reactively

Use `.related()` to include jobs in your query, then reactively track their status:

```tsx
// Query a thread with its latest spookify job
const threadResult = useQuery(() =>
  db.query('thread')
    .where({ id: `thread:${threadId}` })
    .related('jobs', (q) =>
      q.where({ path: '/spookify' }).orderBy('created_at', 'desc').limit(1)
    )
    .one()
    .build()
);

const thread = () => threadResult.data();

// Check if a job is in progress
const isJobLoading = () =>
  ['pending', 'processing'].includes(thread()?.jobs?.[0]?.status ?? '');

// Use in UI
<Show when={isJobLoading()}>
  <span>Processing...</span>
</Show>
```

The job's `status` field transitions through: `pending` → `processing` → `success` | `failed`. Since the job record syncs reactively, your UI updates automatically as the backend processes the job.

## Key Re-exports

The package re-exports commonly needed types:

```typescript
import { RecordId, Uuid, useQuery, createPreload } from '@spooky-sync/client-solid';
import type {
  Model, GenericModel, QueryResult, TableModel, TableNames, GetTable,
  PreloadOptions, PreloadRefresh,
} from '@spooky-sync/client-solid';
```
