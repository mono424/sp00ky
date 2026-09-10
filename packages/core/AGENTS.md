# `@spooky-sync/core` — agent guide

## What this package is

The sync engine. A `Sp00kyClient<S>` owns a local database (memory or IndexedDB), a remote SurrealDB connection, a mutation queue, and a CRDT manager. It's framework-agnostic — UI bindings live in `@spooky-sync/client-solid` (or future `client-react`, etc.). Most app code touches this package indirectly through a UI binding's `useDb()` hook.

## Mental model

```
.surql schema
   │  spky generate
   ▼
schema.gen.ts (typed schema + SURQL_SCHEMA constant)
   │  passed to Sp00kyConfig
   ▼
Sp00kyClient<S>  ── facade: every method = one saga run, selector or subscription
       │
       ├── Runtime (client/runtime.ts): one immutable ClientState, lanes, timers,
       │     subscriptions, dirty -> materialize scheduling. `query:status`,
       │     `activity:changed` and `health:changed` are all notified from here,
       │     off state transitions - never emitted by a saga, so a change made
       │     outside a sync round (a socket drop, say) still reaches subscribers
       ├── Sagas (query/ mutation/ sync/ boot/): pure generators yielding effects
       └── Interpreter (kernel/interpreter.ts) -> adapters (services/):
             local store · remote socket · SSP wasm circuit · tabs broker · blobs
```

Local writes are applied optimistically (row + outbox entry in one local
transaction), rendered through the outbox overlay, and drained to the remote in
batches. Server membership arrives through one batched edge read (registration,
poll, LIVE dirt) and bodies are fetched once across all queries. Rows are always
re-materialized from state, never patched in place.

## Key exports (`src/index.ts`)

- `Sp00kyClient<S>` — main class. Methods: `init()`, `create(id, payload)`, `update(table, id, payload, options?)`, `delete(table, id)`, `query(table, opts?)`, `preload(query, { signal? })`, `run(backend, route, payload)`, `bucket(name)`, `remoteQuery(sql, vars?)`, `useRemote(fn)`, `authenticate(token)`, `auth.signOut()`. Traffic indicators: `pendingMutationCount` / `subscribeToPendingMutations(cb)` (outbox depth), `fetchingQueryCount` / `subscribeToFetchActivity(cb)`. Failed-writes tray: `failedMutationCount`, `subscribeToFailedMutations(cb)`, `listFailedMutations()`, `retryFailedMutation(id)`, `discardFailedMutation(id)`.
- `FailedMutation` — a row of the `_00_failed_mutations` tray (`mutationType`, `recordId`, `data`, `beforeRecord`, `error`, `revert`).
- `BucketHandle` — file storage handle (`put`, `get`, `delete`, `exists`), plus the cache-aware `read`, `url`, `pin`/`unpin`, `evict`, `prefetch`. `get` is always remote; `read`/`url` go through the blob cache.
- `services/blobs/` — durable cache for bucket bytes: OPFS holds the files, `_00_blob` holds a manifest that reconcile rebuilds from disk (so a wiped local store costs metadata, not the offline cache). Nothing expires on a timer; eviction is LRU under a byte budget only, skipping pinned and on-screen entries. Config: `blobCache` in `types.ts`.
- `AuthService` — token management, sign-in/sign-out events.
- `CrdtManager`, `CrdtField`, `cursorColorFromName`, `CURSOR_COLORS` — Loro-CRDT integration.
- Types: `Sp00kyConfig` (client-solid wraps it as `SyncedDbConfig`), `QueryTimeToLive`, `PersistenceClient`, `StoreType`, `UpdateOptions`, `RunOptions`, `PreloadOptions`.
- Subpath: `@spooky-sync/core/otel` — `createOtelTransmit(endpoint)` for piping pino logs to OpenTelemetry.

## Common gotchas

- **Use UI hooks, not the client directly.** In a Solid app, call `useDb()` from `@spooky-sync/client-solid`. Touch `Sp00kyClient` only inside `provider.client` for advanced flows.
- **Mutations are optimistic.** `db.create` / `db.update` / `db.delete` return immediately; the queue drains in the background. Inspect progress via `pendingMutationCount`.
- **Record IDs are full strings.** `db.create('thread:abc123', {...})` — *not* `db.create('thread', { id: 'abc123' })`. The first arg is `'<table>:<id>'`. Generate IDs with `Uuid` from `surrealdb` (re-exported by `client-solid`).
- **`db.update` takes `(table, id, payload, options?)`**. The `debounced` option (`{ debounced: true }` or `{ debounced: { delay, key } }`) coalesces rapid updates — use it for CRDT text/title fields.
- **`db.remoteQuery(sql, vars?)` is the one-shot remote read.** It runs one SurrealQL statement through the client's remote path (connect gate, per-statement timeout, concurrency limit) and returns the SurrealDB result array; results are not synced into the local cache. Use it for counts and other reads the query builder can't express.
- **`db.useRemote(fn)` hands out the bare SDK client.** Same cache bypass, but it skips the connect gate (a call during a reconnect fails with "Specify a namespace to use"). Reserve it for SDK features `remoteQuery` cannot reach (transactions, `live`).
- **`@parent` columns are auto-populated.** Defined in the `.surql` schema as `record<user>` with the `-- @parent` annotation; never write them yourself — they're set from the auth context server-side.
- **`db.preload` is a registered query.** Resolved before on this device (a `_00_view` row exists) it returns at once; never resolved it waits until the server's membership and every body are local. It holds no subscriber, so it is evicted a ttl after registration unless a view mounts the same query. `refresh`/`staleTime` are ignored; pass `signal` to abort a cold wait.
- **A rejected write is not lost.** Application errors roll the local row back (create/update/delete) and move the mutation to `_00_failed_mutations`; the tray API above lists, retries (as a fresh write) or discards it. Network errors keep the row queued.

## Module map (under `src/`)

- `kernel/` — `Effect` union, saga driver + lanes, constants, interpreter.
- `state/` — `ClientState`, lifecycle machine, reducers (they mark queries dirty), selectors (`overlay`, `needed`, `planFetch`, `settled`, `evictable`, `phaseTimings`).
- `query/` — keys, membership rules, render set, SQL, and the sagas: `register`, `membership`, `fetch`, `materialize`, `lifecycle`.
- `mutation/` — outbox rows (v2: `tableName`, `beforeRecord`, `createdAt`), `write`, `push` (drain + rollback), `tray`, jobs.
- `sync/` — `poll`, `live`, `connection` (health, self-heal), `tabs` relay, pure policy.
- `boot/` — `boot`, `auth-flip`, `bucket-switch`, `preload`.
- `client/` — `runtime`, `router` (event -> saga + lane), `services` (adapters), `sp00ky-client` (facade), `bucket-handle`.
- `testing/` — `runPure` (drive a saga with canned effect results), fake adapters/services, state builders.
- `modules/` — auth, crdt, feature-flag, app-release, devtools, ref-tables (unchanged adapters).
- `services/` — local store engines, remote socket + supervisor, SSP wasm wrapper, tabs broker, blobs, persistence, logger.

Rules: nothing under `kernel/state/query/mutation/sync/boot` imports `services/` or touches `Date`, `setTimeout`, `crypto`; those directories are gated at 100% coverage. Legacy services are reached through the typed `service` effect (`kernel/effects.ts`, `ServiceCalls`).

## Pointers

- UI bindings: `node_modules/@spooky-sync/client-solid/AGENTS.md`
- Query builder DSL: `node_modules/@spooky-sync/query-builder/AGENTS.md`
- Schema codegen / migrations: `node_modules/@spooky-sync/cli/AGENTS.md`
- Live introspection: `node_modules/@spooky-sync/devtools-mcp/AGENTS.md`
