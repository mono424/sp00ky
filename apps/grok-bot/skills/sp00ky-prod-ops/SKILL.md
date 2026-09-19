---
name: sp00ky-prod-ops
description: >-
  Use when operating Sp00ky in production: doctor/verify drift, query allowlists,
  SSP/scheduler health, deploys, snapshots/backups, and incident triage.
---

# Sp00ky production ops

Help operators keep a Sp00ky deployment healthy. Prefer read-only diagnosis first; never run destructive commands without explicit confirmation.

## Health loop

1. `spky doctor --json` (or project-equivalent CI job) — codegen freshness, config validity, migration state.
2. `spky verify` — SSP/scheduler snapshot vs upstream SurrealDB; understand drift before `--fix`.
3. Check SSP and scheduler HTTP health/metrics endpoints documented in the deploy docs (`/docs/cloud/deploying`, SSP/scheduler reference).
4. Confirm query allowlist coverage for any new client queries (`@spooky-sync/query-allowlist`).

## Common prod jobs

| Job | Starting point |
|---|---|
| Sync stuck / clients stale | DevTools MCP active queries + SSP logs; then `spky verify` |
| Schema change rollout | migrate create/apply → generate → doctor → redeploy SSP/clients |
| Allowlist miss (403 / not allowlisted) | regenerate allowlist from `q*` modules; redeploy |
| Scheduler drift | `spky verify`; only `--fix` with operator approval |
| Backup / restore | follow `/docs/cloud/backups` for `spky backup` and restore steps |

## Safety

- No force-pushes, no prod data deletes, no `--fix` / reclone without the owner confirming.
- Do not paste secrets into chat or memories.
- If Cloud/admin MCP is available for their deploy, use it; otherwise stay on CLI + docs + DevTools MCP.
