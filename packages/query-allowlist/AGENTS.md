# `@spooky-sync/query-allowlist` - agent guide

## What this package is

A generator, not a runtime. It loads an app's `q*` query module and its
`schema.gen.ts` with `jiti`, calls every `q*` export against a recording `db`
whose `query()` is the real `QueryBuilder` from `@spooky-sync/query-builder`,
and writes `{ generator, app, sourceHash, entries[{ name, surql, whereMode }] }`.
The CLI (`src/cli.ts`) is a thin wrapper over `generateAllowlist()` in
`src/index.ts`.

## Invariants

- **No normalisation, no hashing here.** `surql` is the builder's
  `innerQuery.selectQuery.query` verbatim. The Rust SSP owns canonicalisation;
  `sourceHash` is a plain sha256 of the query file bytes and is only compared
  for equality.
- **Arguments are proxies** (`src/proxy.ts`). They must survive any use a pure
  `q*` makes of an argument: `String(x)`, `x * n`, `x.map`, `x.since.getTime()`,
  spread. Deterministic by construction: `number` hint -> `1`, everything else
  -> `proxy:<path>`, `has` -> false, `ownKeys` -> []. The target is an arrow
  function (an ordinary function's non-configurable `prototype` breaks the
  `ownKeys: []` invariant).
- **Where-any detection** wraps `QueryBuilder.prototype.where` once per
  process and stamps `getOptions().__whereAny = true` when the argument is a
  proxy. The flag survives `.one()`'s options spread and is read back from
  `finalQuery.innerQuery.getOptions()`. Subquery modifier builders
  (`.related(f, r => r.where(...))`) are a private class and are not wrapped.
- **Sidecars** are optional exports of the query module: `allowlistSamples`
  (real args -> `name#i` static entries), `allowlistWhereAny`, `allowlistSkip`.
- `r*` members on the recording db throw, so a misfiled remote read fails the
  run instead of vanishing from the allowlist.
- Runtime-chosen tables (`db.query(table)` with a proxy) fan out over
  `schema.tables[].name` as `name[table]`.

## Known limits (report, do not paper over)

- A `q*` that picks a predicate branch on its argument
  (`opts.collections.length ? _or : owner`) records only the branch the proxy
  takes (arrays are length 1, strings are truthy). Cover the other branch
  with `allowlistSamples`.
- A proxy reaching a subquery `where` is inlined as the literal
  `"proxy:<path>"`; that entry can never match a real query. Use samples.

## Tests

`pnpm --filter @spooky-sync/query-allowlist test` (vitest). Fixtures live in
`test/fixtures/`; the CLI smoke test runs `src/cli.ts` under `jiti/register`.
