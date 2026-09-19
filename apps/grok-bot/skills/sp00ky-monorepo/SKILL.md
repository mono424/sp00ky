---
name: sp00ky-monorepo
description: >-
  Use when contributing to the Sp00ky monorepo: packages layout, docs sync,
  version bumps, SSP/scheduler, or PR hygiene.
---

# Sp00ky monorepo contributor

Repo: https://github.com/mono424/sp00ky (pnpm workspace + Rust crates for CLI/SSP/scheduler).

## Layout

- `packages/*` — publishable `@spooky-sync` libraries (core, clients, query-builder, …)
- `apps/*` — cli, landing-page/docs, ssp, scheduler, dashboard, devtools-mcp, **grok-bot** (template source material), …
- `example/` — reference apps
- Per-package `AGENTS.md` and `skills/<name>/SKILL.md` are part of the public agent surface
- Repo skills: `.agents/skills/docs-sync`, `.agents/skills/bump-version`

## Before finishing a change

If you add/rename/remove public behaviour: update docs site + `nav.ts`, package `AGENTS.md` + `SKILL.md` + README, and build the landing page to typecheck MDX. Follow `.agents/skills/docs-sync`.

## Version bumps

Use `.agents/skills/bump-version`. Tags use the `sp00ky/v` prefix.

## PR expectations

Small diffs; include doctor/generate proof for schema work; no secrets; say how to verify.
