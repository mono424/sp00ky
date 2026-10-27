# `@spooky-sync/query-builder` — agent guide

## What this package is

The type-safe SurrealQL query builder used by `@spooky-sync/core` and `@spooky-sync/client-solid`. It encodes the user's `.surql` schema as a TypeScript type and rejects invalid table names, fields, and relationship traversals at compile time. It also serializes builder chains into SurQL strings for execution.

You usually don't import this directly — `db.query('thread')` returns a `QueryBuilder` and the package's types are re-exported from `@spooky-sync/client-solid`. Reach for it when writing generic helpers, type utilities, or non-Solid bindings.

## DSL overview

```ts
db.query('thread')                              // QueryBuilder<Schema, 'thread', ...>
  .where({ active: true })                      // partial-equality filter
  .select('id', 'title', 'created_at')          // omit for SELECT *
  .related('author')                            // follow a record-link relationship
  .related('comments', { orderBy: { created_at: 'desc' } })
  .orderBy('created_at', 'desc')
  .limit(20)
  .offset(0)
  .build();                                     // → FinalQuery (call .run() or hand to useQuery)
```

`build()` returns a `FinalQuery`. `useQuery(() => ...build())` calls `.run()` for you and tracks the factory reactively. Standalone consumers call `.run()` themselves; the executor returns whatever the binding configures (in client-solid: a `Sp00kyQueryResultPromise`).

## Key exports (`src/index.ts`)

- **Builders**: `QueryBuilder`, `InnerQuery`, `FinalQuery`, `Executor`, `buildQueryFromOptions`, `cyrb53` (hash function used to dedupe queries).
- **Schema type helpers** (load-bearing — these power the type-safety in every consumer):
  - `SchemaStructure` — the shape of the generated `schema` object.
  - `TableNames<S>` — union of valid table name literals.
  - `GetTable<S, T>` / `TableModel<T>` — row type for table `T`.
  - `TableRelationships<S, T>` / `RelationshipFieldsFromSchema<S, T>` — the typed relationship surface.
  - `BackendNames<S>` / `BackendRoutes<S, B>` / `RoutePayload<S, B, R>` — for `db.run(...)`.
  - `BucketNames<S>` / `BucketConfig<S, B>` / `BucketDefinitionSchema`.
  - `AccessDefinition`, `TypeNameToTypeMap`.
- **Query result types**: `QueryResult`, `RelatedFieldsMap`, `BuildResultModelOne`, `BuildResultModelMany`, `WithRelated`, `GetCardinality`.
- **Modifier helpers**: `QueryModifier`, `QueryModifierBuilder`, `SchemaAwareQueryModifier`, `SchemaAwareQueryModifierBuilder`.

## Builder method surface

- `.where(partial)` — equality filters; `partial` is `Partial<TableModel<...>>`.
- `.select(...fields)` — narrow returned columns. Calling twice throws.
- `.orderBy(field, 'asc' | 'desc')`.
- `.limit(n)`, `.offset(n)`.
- `.ignoreUnusedArgs()`: marks that parts of the query do nothing where it is used, on purpose (a peer to peer channel, `useChannel(query, peers)`, only reads the `where`; without this its `orderBy` / `limit` / `offset` / `select` / `.one()` draw a warning). Changes neither surql nor hash.
- `.related(field, modifier?)` — pull in a related record (or array) by field name. `modifier` is a builder callback for nested filters / selects on the related table. Three signatures: `(field)`, `(field, modifier)`, `(field, cardinality, modifier)`.
- `.build()` — produces a `FinalQuery`.
- `FinalQuery.run()` — executes via the configured executor.
- `FinalQuery.selectLive()` — returns the LIVE SELECT `QueryInfo` for subscriptions.
- `FinalQuery.buildUpdateQuery(patches)` / `buildDeleteQuery()` — used by `db.update` / `db.delete` internally.

## Common gotchas

- **`select()` is exclusive.** Either select specific fields or omit the call for `*` — don't chain `.select(...)` more than once.
- **Relationships must exist in the schema.** `related('foo')` is a type error if `foo` isn't a relationship column. Run `spky generate` after editing the `.surql` to refresh the type.
- **`where` is equality-only.** For `>`, `<`, `IN`, free-form predicates, use `db.remoteQuery(sql, vars)` (one-shot, through the client's remote path) or `db.queryRaw(sql, params, ttl)` and write SurQL directly.
- **Record-ID strings are auto-parsed.** Passing `"thread:abc"` into a `where` matches a `RecordId('thread', 'abc')`. Don't double-wrap.
- **The serialized SurQL is hashable.** `cyrb53` over the query string drives cache keys — two builders that produce identical SurQL share a cache entry.

## Pointers

- Sync engine (executor implementation): `node_modules/@spooky-sync/core/AGENTS.md`
- Reactive consumption: `node_modules/@spooky-sync/client-solid/AGENTS.md`
- Schema authoring: `node_modules/@spooky-sync/cli/AGENTS.md`
