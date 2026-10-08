# `@spooky-sync/devtools-mcp` (`sp00ky-mcp`) — agent guide

## What this package is

A Model Context Protocol server that gives an AI assistant live, structured access to a running sp00ky app. It bridges to either:

1. **The Sp00ky DevTools browser extension** (via WebSocket on the side channel `bridge.ts` opens), so the assistant sees the *exact* state the user's tab has — local cache, active live queries, event history, auth state.
2. **A direct SurrealDB connection** (set `SURREAL_URL`, `SURREAL_USER`, `SURREAL_PASS`, `SURREAL_NS`, `SURREAL_DB`) — useful when there's no browser tab attached.

Most tools transparently fall through: extension first, raw DB as a fallback. A handful (auth state, event history clear) require the extension.

## Run it

```bash
npx @spooky-sync/devtools-mcp
# or, from inside an app, via the CLI passthrough:
spky mcp
```

Wire into Claude / your agent via the MCP server config (`.mcp.json` at the repo root or your client's equivalent).

## Tools exposed

### Connection / state

- **`list_connections`** — which browser tabs are currently bridged.
- **`get_state` `[tabId]`** — full devtools state: events, queries, auth, database tables (extension only).
- **`get_auth_state` `[tabId]`** — current auth subject + scope (extension only).
- **`get_active_queries` `[tabId]`** — registered live queries, their last result hashes, and a per-query `timings` breakdown.
- **`get_query_timings` `[tabId]`** — per-query processing-time breakdown (SSP parse/plan/snapshot + store-apply/circuit-step/transform, local & remote record fetch, frontend reconcile; each as last/p50/p90/p99), sorted slowest-first. For debugging perf.
- **`get_events` `[eventType] [limit]`** — recent event log, optionally filtered.
- **`get_mutations` `[status] [includeTray=true] [tabId]`** — the outbox (pending / retrying with attempts), writes that left it since DevTools attached (synced with latency, or rolled back with the server error), debounced updates, tab role + connection, and the failed-writes tray rows with payload and before image (extension only).
- **`get_logs` `[minLevel] [contains] [after] [limit=100] [captureLevel] [tabId]`** — the client's own log lines (last 500 kept in the page), each with its structured fields. `after` = a previous `head` reads only new lines; `captureLevel: 'debug'` records debug lines without printing them to the console, for the tab's session (extension only).
- **`clear_history` `[tabId]`** — wipe the in-tab event log (extension only).

### Database (works against extension OR direct DB)

- **`run_query` `query` `[target=local|remote]` `[tabId]`** — execute arbitrary SurQL.
- **`list_tables` `[tabId]`** — names of tables defined in the running schema.
- **`get_table_data` `tableName` `[limit]` `[tabId]`** — `SELECT * LIMIT n` shortcut.
- **`update_table_row` `recordId` `updates` `[tableName] [tabId]`** — `UPDATE recordId MERGE updates`.
- **`delete_table_row` `recordId` `[tableName] [tabId]`** — `DELETE recordId`.

### Resources

- `sp00ky://state` — full state JSON (extension only).
- `sp00ky://tables` — table list.

## How an agent should use this

- **Before writing a query against a schema you haven't seen:** call `list_tables` and `get_table_data` with `limit: 1` on each table to learn the columns.
- **After making a mutation through `db.update`:** call `get_mutations` to confirm it synced (or read why it was rolled back), then `get_active_queries` to confirm subscribed queries refreshed.
- **When something fails without an obvious error:** `get_logs` with `minLevel: 'warn'`; set `captureLevel: 'debug'`, reproduce, and read again with `after` set to the earlier `head`.
- **When troubleshooting "why isn't this query updating?":** `get_active_queries` shows the registered SurQL and last result hash; `get_events` shows whether new records ingested.
- **For schema spelunking from outside the browser:** point `SURREAL_URL` at the dev DB and use `run_query` with `INFO FOR DB;`.

## Common gotchas

- **Mutations through this MCP bypass the local mutation queue.** `update_table_row` calls remote SurrealDB directly (or the extension's bridge). The local cache will see the change only when SSP pushes the live update back. For testing in-app, prefer driving mutations through the page itself.
- **Tab selection matters.** Multi-tab dev: pass `tabId` explicitly or you get whichever tab connected first.
- **Direct-DB fallback has a smaller capability set.** Anything that reads in-memory devtools state (auth, events, active queries) only works with the extension.

## Pointers

- Sync engine that emits the events this server reads: `node_modules/@spooky-sync/core/AGENTS.md` → `src/modules/devtools/`
- CLI passthrough: `node_modules/@spooky-sync/cli/AGENTS.md` (`spky mcp`)
