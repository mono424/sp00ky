---
name: sp00ky-debug-mcp
description: >-
  Use when queries look stale, sync stalls, auth is wrong, or you need live
  schema/data from a running Sp00ky app.
---

# Debug with Sp00ky DevTools MCP

## Wire-up

```json
{
  "mcpServers": {
    "spooky-devtools": {
      "command": "npx",
      "args": ["-y", "@spooky-sync/devtools-mcp"]
    }
  }
}
```

Or from an app: `spky mcp`. Prefer the browser DevTools extension bridge when a tab is open; otherwise configure SurrealDB env vars for direct DB fallback (see `@spooky-sync/devtools-mcp` AGENTS.md).

**Marketplace note:** this MCP is **not** a Grok Bot marketplace plugin. Importers must wire it themselves.

## Investigation order

1. List connections / pick the right tab.
2. Describe schema or list tables before writing SurQL.
3. Get active queries + timings.
4. Get events / auth state.
5. Lint query then run against local or remote.

## Rules

- MCP mutations bypass the local mutation queue — fixtures/debug only.
- Direct-DB mode cannot see in-memory extension state.
- Cloud MCP and scheduler admin MCP (if deployed) are separate products — say so when the user asks for cluster ops.
