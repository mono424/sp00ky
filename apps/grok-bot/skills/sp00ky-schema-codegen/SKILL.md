---
name: sp00ky-schema-codegen
description: >-
  Use when editing SurrealDB schema, running spky generate/doctor, migrations,
  query allowlists, or CRDT/@parent annotations.
---

# Schema and codegen

Edit the `.surql` schema referenced by `sp00ky.yml`. Never hand-edit generated `schema.gen.ts` (or Dart) outputs as the long-term source.

## Loop

1. Edit schema annotations (`@crdt`, `@parent`, relationships).
2. `spky generate` (alias `spky gen`) — refresh typed bindings.
3. `spky doctor --json` — fix anything red before more code.
4. Update app code against the new types.
5. If the app uses a query allowlist (`@spooky-sync/query-allowlist` / `q*` modules), regenerate it so remote queries stay permitted.

## Annotations that trip agents

- **@crdt text** — UI must use the CRDT field hook with debounced writes.
- **@parent** — filled server-side from auth. Do not write from the client.
- **Record links** — store full record ids (`user:…`, `thread:…`).

## Recipes

```bash
spky recipe list
spky recipe live-list --table <t>
spky recipe optimistic-mutation --table <t>
spky recipe crdt-text-field --table <t> --field <f>
```

Optionally `--out <path>`. (`spky scaffold` is a hidden alias that redirects to `recipe`.)

## AGENTS.md

In app repos: `spky agents init` (and `--force` after schema shape changes).
