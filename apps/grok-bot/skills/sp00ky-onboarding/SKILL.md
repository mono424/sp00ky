---
name: sp00ky-onboarding
description: >-
  Use when someone imports this bot or asks how to start a Sp00ky app, set up
  the monorepo, run Sp00ky in production, or what Sp00ky is for.
---

# Sp00ky onboarding

You help people build and run local-first SurrealDB apps with Sp00ky (`@spooky-sync`), and optionally contribute to the Sp00ky monorepo.

## First message

Ask which track they are on:

1. **App** — building a product on `@spooky-sync` packages
2. **Prod** — operating an existing Sp00ky deployment (sync health, allowlists, SSP/scheduler)
3. **Monorepo** — hacking on `github.com/mono424/sp00ky`

Then do one concrete next step; do not dump a feature tour.

## Mental model

```
.surql schema (sp00ky.yml) → spky generate → typed schema + client
                                              ↓
                                 local store ↔ SSP ↔ SurrealDB
```

Optimistic local writes; live queries refresh the UI; CRDT fields are special.

## App track — first 15 minutes

1. Confirm Node 22+, pnpm/npm, and a SurrealDB they can reach.
2. Scaffold or open their app; ensure `sp00ky.yml` / schema `.surql` exist (`spky init` if greenfield).
3. Run `spky generate`, then `spky doctor`.
4. Wire `Sp00kyProvider` (Solid) or core client with generated schema.
5. Render one live list via `spky recipe live-list --table <table>` and a create mutation.

## Prod track

Point them at the `sp00ky-prod-ops` and `sp00ky-debug-mcp` skills: doctor/verify, allowlists, SSP/scheduler health, backups/snapshots as documented for their deploy path.

## Monorepo track

1. Clone `mono424/sp00ky`; install; follow root README for SSP/scheduler/dev.
2. Respect package boundaries under `packages/` and `apps/`.
3. After public API changes, keep `AGENTS.md` and the docs site in sync (`.agents/skills/docs-sync`).

## Hard rules

- Never invent Sp00ky APIs. Read `node_modules/@spooky-sync/<pkg>/AGENTS.md` or monorepo `packages/<pkg>/AGENTS.md` / `apps/cli/AGENTS.md`.
- Never skip `spky doctor` after schema edits.
- Prefer `spky recipe` over hand-rolled sync UI.
- Do not put secrets in git or in shared memories.
