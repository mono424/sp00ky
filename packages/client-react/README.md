# @spooky-sync/client-react

React bindings for the Sp00ky reactive local-first SurrealDB framework. React 18 and 19, in the
browser (React DOM). The core client runs its engines as WebAssembly with IndexedDB / OPFS storage,
so React Native is not supported; use the Flutter client for native mobile apps.

```bash
pnpm add @spooky-sync/core @spooky-sync/client-react
```

```tsx
import { Sp00kyProvider, useDb, useQuery } from '@spooky-sync/client-react';

function Posts() {
  const db = useDb<typeof schema>();
  const posts = useQuery(db.query('post').orderBy('created_at', 'desc').limit(20).build());
  if (posts.isLoading) return <p>Loading…</p>;
  if (posts.isEmpty) return <p>No posts yet.</p>;
  return (
    <ul>
      {posts.data.map((p) => (
        <li key={String(p.id)}>{p.title}</li>
      ))}
    </ul>
  );
}

<Sp00kyProvider config={dbConfig} fallback={<Splash />}>
  <Posts />
</Sp00kyProvider>;
```

What "native" means here:

- Every hook reads through `useSyncExternalStore`: tear-free under concurrent rendering, and a hook
  re-renders only when the value it returns changed.
- `useQuery` results are structurally shared between live emissions. Rows are matched by `id`,
  unchanged rows keep their object identity, and a deep-equal emission does not re-render.
  `React.memo` rows work as expected.
- The client, query registrations, CRDT fields, channels and file leases are opened in effects and
  released in their cleanups, never during render. StrictMode's double mount ends with one of each.
- Mutations stay plain `await db.create(...)` calls: the engine is already optimistic local-first.
  `useSubmission` only adds button pending and error state.
- The bundle starts with `'use client'`, so a Next.js App Router layout can render the provider.

The query flags (`isLoading`, `isFetching`, `isAuthoritative`, `hasData`, `isEmpty`, `isSettled`)
follow the same contract as `@spooky-sync/client-solid2`, as plain values.

See `QUICK_START.md` for a walkthrough, `AGENTS.md` for the full API, and the
[React guide](https://mono424.github.io/sp00ky/docs/guide/react).

```bash
pnpm --filter @spooky-sync/client-react test
pnpm --filter @spooky-sync/client-react build
```
