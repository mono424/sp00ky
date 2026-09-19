# Sp00ky Grok Bot (marketplace)

Source of truth for the public **Sp00ky** Grok Bot marketplace template.

This is not a runtime npm library. It holds the scrubbed profile, memories, and skills that should be packed into a public Grok Bot template for people building and running Sp00ky (`@spooky-sync`) apps — including production ops.

## Layout

```
apps/grok-bot/
  README.md
  BOT.md                 # storefront profile + publish notes
  package.json           # private workspace marker only
  memories/              # scrubbed profile/log facts for templates
  skills/
    sp00ky-onboarding/
    sp00ky-schema-codegen/
    sp00ky-solid-app/
    sp00ky-debug-mcp/
    sp00ky-prod-ops/
    sp00ky-monorepo/
```

## Related in-repo agent surface

- Per-package `AGENTS.md` and `packages/*/skills/*/SKILL.md`
- Repo skills: `.agents/skills/docs-sync`, `.agents/skills/bump-version`
- Docs: [`/docs/reference/ai-agents`](../landing-page/src/pages/docs/reference/ai-agents.mdx)
- Live introspection: `@spooky-sync/devtools-mcp` (`spky mcp`)

## Publish flow (Grok Bot)

1. Keep skills/memories aligned with package `AGENTS.md` / CLI reality (`spky generate`, `spky doctor`, `spky recipe`, `sp00ky.yml`).
2. In Grok Bot, stage a **public** template from these recipes (profile + skills + memories).
3. Marketplace plugins only pack if they are marketplace connectors. **DevTools MCP does not pack** — keep setup instructions in `sp00ky-debug-mcp` so importers wire `npx -y @spooky-sync/devtools-mcp` themselves.
4. Review the publish card (no secrets, no private paths), then publish.

## Verify

```bash
test -f apps/grok-bot/BOT.md
find apps/grok-bot/skills -name SKILL.md
```
