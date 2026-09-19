# Sp00ky Grok Bot template

Published docs: [Grok Bot template recipe](../landing-page/src/pages/docs/reference/grok-bot.mdx) (site path `/docs/reference/grok-bot`).

Source material for a public **Sp00ky** Grok Bot template.

This is not a runtime npm library. It holds a profile draft, reference facts, and skill drafts for people building and running Sp00ky (`@spooky-sync`) apps. Grok Bot shares a Bot through a public template link; these files are not an automatically importable bundle.

## Layout

```
apps/grok-bot/
  README.md
  BOT.md                 # Bot profile draft + sharing notes
  memories/              # reference facts to incorporate into shared copy
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

## Share flow (Grok Bot)

1. Keep the drafts aligned with package `AGENTS.md` and CLI behavior (`spky generate`, `spky doctor`, `spky recipe`, `sp00ky.yml`).
2. Create a Bot in Grok Bot. Use `BOT.md` for its profile, save and enable the relevant skills, and put essential facts from `memories/profile.md` in the description or skills. Learned memory is not part of the shared configuration.
3. Open **Share → Create template**, choose **Public link**, and review **View template details** before copying the link. Remove secrets, private paths, and customer data.
4. The template does not provision Sp00ky DevTools MCP. Developers using a compatible MCP client can configure `npx -y @spooky-sync/devtools-mcp` or run `spky mcp` in their app.

See [Grok Bot's sharing guide](https://docs.x.ai/grok-bot/bots#share-a-bot) for the current sharing controls.

## Verify

```bash
test -f apps/grok-bot/BOT.md
find apps/grok-bot/skills -name SKILL.md
```
