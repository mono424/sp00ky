---
name: sp00ky-solid-app
description: >-
  Use when building SolidJS UI on Sp00ky: provider setup, useQuery/useDb,
  mutations, auth, files, or Solid 2 bindings.
---

# SolidJS Sp00ky apps

Primary packages: `@spooky-sync/client-solid` and `@spooky-sync/client-solid2` (Solid 2.0 binding). Core: `@spooky-sync/core`. Queries: `@spooky-sync/query-builder`.

## Provider

Wrap the app in the Solid provider with database endpoint/namespace/database/store, generated `schema`, raw schema SurQL, optional preload/onReady/onError. Show fallback until init finishes.

## Queries

Prefer `useDb<typeof schema>()` then `useQuery(() => db.query('table').….build())` so the factory ends with `.build()`. Function form re-runs when signals/props change.

For Solid 2 bindings, follow `client-solid2` AGENTS.md (`createQuery`, `<Loading>`, `createSubmission`).

## Mutations

`db.create('table:id', payload)`, `db.update`, `db.delete` — optimistic. Mint ids as full record strings. CRDT fields: CRDT hook + debounced writes.

## When stuck

Read `client-solid` / `client-solid2` and `core` AGENTS.md, run `spky doctor`, then use Sp00ky DevTools MCP.
