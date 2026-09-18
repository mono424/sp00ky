# Sp00ky DevTools

The Chrome DevTools extension for Sp00ky apps. It adds a panel called **00**
next to Elements and Network and inspects the live client in the inspected page.

User-facing documentation, with screenshots of every tab, lives at
[`/docs/reference/devtools`](../landing-page/src/pages/docs/reference/devtools.mdx).

## What the panel shows

| Tab | Shows |
| --- | --- |
| Queries | Every live query, its status, update count, payload size, and a per-query detail panel (SurrealQL, variables, rows, timings). |
| Timing | All queries against all pipeline phases (SSP / local / remote / frontend) as p90s, slowest first. |
| Database | A paginated, editable table browser over the local cache or the remote database. |
| Storage | Engine and store, OPFS and persistence health, shared-tab ownership, origin quota, bucket file cache, OPFS files, SQLite worker stats, per-table row counts. |
| Access | The session, admin impersonation, and every feature flag with browser-local overrides and server-wide controls. |
| Stack | Frontend versus backend versions with drift detection, every SSP / scheduler / backend entity, and the end-to-end sync heartbeat. |
| MCP | The bridge that hands the same state to an AI assistant via `@spooky-sync/devtools-mcp`. |
| Events | The client event log, filterable by type. |

The toolbar carries the connection dot (and the frame picker, when a tab runs
more than one client), the heartbeat badge, a scoped Refresh and Clear.

## Development

```bash
pnpm install
pnpm build          # tsc + vite, output in dist/
pnpm dev            # the same build, in watch mode
```

Load it in Chrome: `chrome://extensions` → **Developer mode** → **Load
unpacked** → select `apps/devtools/dist`. Then open DevTools on a page running
a Sp00ky app and pick the **00** panel.

Releases are published to the Chrome Web Store by
`.github/workflows/chrome-publish.yml` on every `sp00ky/v*` tag, which injects
the tag into `manifest.json` first.

## Documentation screenshots

```bash
pnpm screenshots
```

Drives the real panel against a frozen fixture and writes one PNG per tab into
the docs site. See [`screenshots/README.md`](./screenshots/README.md).

## How it connects

The panel never imports the client. It reaches it through four processes:

1. **`content.ts`** is injected into every frame, injects `page-script.ts` into
   the page, and relays messages between the page and the background script.
2. **`page-script.ts`** runs in the page's own world, where `window.__00__`
   exists. It answers the async ops (run a query, read storage diagnostics,
   read or write flags, start or stop an impersonation) and posts correlated
   responses back.
3. **`background.ts`** is the service worker. It keeps the per-tab frame
   registry, routes messages to the right panel port, and hosts the MCP bridge.
4. **`panel.tsx`** is the Solid app in `devtools.html`'s panel. It uses exactly
   three Chrome APIs: `chrome.runtime.connect`,
   `chrome.devtools.inspectedWindow.eval` and `.tabId`.

Most reads from the main document go through `eval` into the page; a non-main
frame is unreachable that way when it is cross-origin, so the same request
travels the content-script channel instead with the same `requestId`.

## Requirements for an app

None. Every `Sp00kyClient` exposes `window.__00__` when it is constructed, and
the push channel stays dormant until a panel or the MCP bridge handshakes with
the page, so an app nobody is inspecting pays nothing for it.

## Layout

```
apps/devtools/
├── src/
│   ├── devtools.ts            # creates the panel
│   ├── panel.tsx              # panel entry point
│   ├── App.tsx                # tab shell
│   ├── background.ts          # service worker + MCP bridge
│   ├── content.ts             # content script relay
│   ├── page-script.ts         # page-world bridge to window.__00__
│   ├── context/               # DevToolsContext: state, ops, refresh
│   ├── components/            # one folder per tab
│   ├── hooks/                 # chrome connection, host-page eval, theme
│   └── types/devtools.ts      # shapes mirrored from core (kept in sync by hand)
├── screenshots/               # docs screenshot harness
├── public/                    # devtools.html, panel.html, icons
└── manifest.json
```

`types/devtools.ts` deliberately does not import from `@spooky-sync/core`: the
extension ships to the Chrome Web Store independently of the client, so the
shapes are mirrored and kept in step by hand.
