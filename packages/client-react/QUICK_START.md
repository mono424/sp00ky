# Quick start: Sp00ky with React

The same content as the docs' [React guide](https://mono424.github.io/sp00ky/docs/guide/react).

## 1. Install

```bash
pnpm add @spooky-sync/core @spooky-sync/client-react
pnpm add -D vite-plugin-wasm
```

## 2. Configure Vite

The core client loads its engines as WebAssembly and starts web workers, both found next to its own files. Keep the client's packages out of dependency pre-bundling (pre-bundled, they look in the wrong folder), pre-bundle the core's CommonJS dependencies instead, and build for `esnext`:

```ts
// vite.config.ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import wasm from 'vite-plugin-wasm';

export default defineConfig({
  plugins: [react(), wasm()],
  optimizeDeps: {
    // Served as is: pre-bundled, these look for their .wasm files and
    // workers in the wrong folder.
    exclude: [
      '@spooky-sync/client-react',
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

## 3. Generate types and configure the client

```bash
spky generate
```

```ts
// src/db.ts
import type { SyncedDbConfig } from '@spooky-sync/client-react';
import { schema, SURQL_SCHEMA } from './schema.gen';

export const dbConfig: SyncedDbConfig<typeof schema> = {
  schema,
  schemaSurql: SURQL_SCHEMA,
  database: {
    namespace: 'main',
    database: 'main',
    endpoint: 'ws://localhost:8666/rpc',
    store: 'indexeddb',
  },
};
```

## 4. Mount the provider

```tsx
import { Sp00kyProvider } from '@spooky-sync/client-react';
import { dbConfig } from './db';

export default function App() {
  return (
    <Sp00kyProvider
      config={dbConfig}
      fallback={<div>Initializing database…</div>}
      preload={async (db) => {
        // Optional: gate the first paint on essential data.
        await db.preload(db.query('config').build());
      }}
      onError={(err) => console.error(err)}
    >
      <MainContent />
    </Sp00kyProvider>
  );
}
```

`config` is read once, when the provider mounts. To switch databases, remount it with a new `key`.

## 5. Query

```tsx
import { useDb, useQuery } from '@spooky-sync/client-react';
import type { schema } from './schema.gen';

function PostList() {
  const db = useDb<typeof schema>();
  // Build inline: the hook re-registers only when the query's identity changes.
  const posts = useQuery(
    db.query('post').related('author').orderBy('created_at', 'desc').limit(20).build()
  );

  if (posts.isLoading) return <div>Loading…</div>;
  if (posts.isEmpty) return <div>No posts yet.</div>;
  return (
    <ul>
      {posts.data.map((post) => (
        <li key={String(post.id)}>{post.title}</li>
      ))}
    </ul>
  );
}
```

Pass `null` (or `{ enabled: false }`) to skip a query:

```tsx
const post = useQuery(postId ? db.query('post').where({ id: postId }).one().build() : null);
```

| Field             | Meaning                                                                        |
| ----------------- | ------------------------------------------------------------------------------ |
| `data`            | Rows, or the row / `null` for `.one()`. `[]` / `null` before the first result. |
| `error`           | Registration or sync error. Never thrown; the scheduler retries.               |
| `isLoading`       | Cold first load only. A cached paint or an empty server answer ends it.        |
| `isFetching`      | A fetch cycle is in flight.                                                    |
| `isAuthoritative` | `data` is an answer (a known membership is painted), not a cache guess.        |
| `hasData`         | `data` holds rows / a row.                                                     |
| `isEmpty`         | Authoritative and no data. Render empty states on this.                        |
| `isSettled`       | Authoritative and idle.                                                        |

## 6. Mutate

```ts
await db.create('post:' + crypto.randomUUID(), { title: 'Hi' });
await db.update('post', 'post:xyz', { title: 'Edited' });
await db.delete('post', row.id);
await db.run('backend', 'sendMail', { to });
```

For a button spinner:

```tsx
const save = useSubmission((title: string) => db.create('post:' + crypto.randomUUID(), { title }));
<button disabled={save.pending} onClick={() => save.submit(title)}>
  Save
</button>;
{
  save.error && <p>{save.error.message}</p>;
}
```

## 7. Status

```tsx
const sync = useSyncStatus();
const pending = usePendingMutations();
if (sync.isDegraded) return <Banner>Offline, {pending} change(s) queued.</Banner>;
```

## Coming from the Solid bindings

| client-solid / client-solid2                  | client-react                         |
| --------------------------------------------- | ------------------------------------ |
| `useQuery(() => q)` / `createQuery(() => q)`  | `useQuery(q)`, `null` to skip        |
| `posts.data()`                                | `posts.data`                         |
| `{ enabled: () => boolean }`                  | `{ enabled: boolean }`               |
| `ready()` + `<Loading>`                       | not available: branch on `isLoading` |
| `createSubmission`                            | `useSubmission`                      |
| `createPreload(() => q)`                      | `usePreload(q)`                      |
| accessor arguments (`() => id`, `() => path`) | plain values                         |
| `class`                                       | `className`                          |
