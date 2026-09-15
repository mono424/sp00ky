# @spooky-sync/query-allowlist

Runs an app's query module (the `q*` builder functions) against a recording
client and writes the raw-SurrealQL allowlist that the SSP enforces when
`sync.queryAllowlist` is `warn` or `enforce`.

```
spooky-query-allowlist --queries src/lib/query.ts --schema src/schema.gen.ts --out allowlist.json [--app web]
```

Exit 0 and the JSON is written; exit 1 with one line per `q*` export that could
not be recorded.

## Output

```json
{
  "generator": 1,
  "app": "web",
  "sourceHash": "<sha256 hex of the query module bytes>",
  "entries": [
    { "name": "qGamesWindow", "surql": "SELECT id, white FROM game ORDER BY sort_index asc LIMIT 1 START 1;", "whereMode": "any" }
  ]
}
```

`surql` is exactly what `@spooky-sync/query-builder` emits, unchanged. The SSP
normalises and hashes; this package never does.

`whereMode` is `any` when the whole `where` argument came from the caller
(`(db, where) => db.query('game').where(where)`): the predicate is unknown at
generation time, so the surql carries no WHERE clause from it and the SSP
accepts any predicate on that shape. It is `static` when every `where` key is
spelled in the module.

## How a `q*` is run

Each export named `q[A-Z]...` is called as `fn(db, ...proxies)` with
`fn.length - 1` argument proxies plus four spares for rest parameters. A proxy
is a stand-in that survives whatever the function does on the way to the
builder: it stringifies to `proxy:<path>`, is `1` in arithmetic, iterates as a
one-element array, spreads to nothing, and returns nested proxies for any
property or call. A value reaching `.where()` binds as a plain `$param`; a
value reaching `.limit()` / `.offset()` renders as `1`.

`db.query(<proxy>)` (a table chosen at runtime) fans out to one entry per
schema table, named `qName[<table>]`.

`db.useRemote`, `db.preload` and the mutation members throw, so an `r*` read
misfiled as a `q*` is reported rather than silently missing.

## Sidecar exports in the query module

- `allowlistSamples: Record<string, unknown[][]>`: extra real argument arrays
  per export; each produces a `static` entry named `qName#<i>`. Use it when a
  builder chooses between predicates at runtime (`opts.collections.length ?
  _or : owner`), which a proxy can only take one branch of.
- `allowlistWhereAny: string[]`: force `whereMode: 'any'` for those names.
- `allowlistSkip: string[]`: do not run those exports at all.

Entries are deduped by surql text (the alphabetically first name wins; if any
duplicate is `any`, the kept entry is `any`) and sorted by name.

## Programmatic API

```ts
import { generateAllowlist } from '@spooky-sync/query-allowlist';
const { allowlist, errors, warnings } = await generateAllowlist({ queries, schema, app: 'web' });
```
