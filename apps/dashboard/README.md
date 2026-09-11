# Sp00ky Admin Dashboard

The operator console a scheduler serves at `/admin`. Solid.js, no runtime
dependencies beyond `solid-js` and `@solidjs/router`.

Full documentation: [Admin dashboard](https://sp00ky.dev/docs/reference/admin-dashboard).

## What it shows

- **Overview** — a bento of tiles: sync round trip as the hero, SSPs ready,
  backends healthy, ingest lag and the scheduler's details beside it; who is
  connected (users / sessions / views, each with its history, plus shared /
  slow / errored); then the SSP and backend fleets in full
- **SSPs** — per-processor state, with a bootstrap progress bar
- **Backends** — health status and a ~30-minute response-time history
- **Views** — every registered live query, filterable and sortable, with a
  detail page carrying its SurrealQL, params, subscribers, materialization
  percentiles and SSP memory footprint
- **Workflows** — runs updating live over SSE, each carrying its own failure
  reason; per-step detail expands to the step's cause and its job's whole attempt
  history; cancel / retry-from-failed / rerun
- **Schedules** — definitions with next and last fire, why each recent fire ended
  as it did, a callout naming the run whose key is suppressing fires,
  pause/resume and run-now
- **Backups** — catalog, schedule and retention (from Sp00ky Cloud when linked),
  create, and a staged restore
- **Logs** — live tail of scheduler or SSP output
- **Access** — mint a scoped token for an AI agent, with the `claude mcp add`,
  Cursor and VS Code snippets, and the tool list the scheduler's MCP server reports

Every SSP and the scheduler carry an action menu (restart, clean restart,
reload, reclone, and the cloud-only image upgrade / volume wipe).

## Development

```bash
pnpm install
pnpm --filter @spooky-sync/dashboard dev     # http://localhost:4300
```

The dev server is not served by a scheduler, so the app comes up in
**standalone** mode: the sign-in form shows an endpoint field. Point it at a
running scheduler's admin port (`http://localhost:9668`).

To run it the way it actually ships:

```bash
pnpm --filter @spooky-sync/dashboard build
SPKY_ADMIN_DIR=apps/dashboard/dist cargo run -p scheduler
# http://localhost:9668/admin
```

## Shape of the code

| Path | Role |
| --- | --- |
| `src/api/client.ts` | Endpoint resolution, bearer token, `fetch`-based SSE reader |
| `src/api/types.ts` | Wire types for `/admin/api` |
| `src/lib/format.ts` | Value formatters |
| `src/lib/status.ts` | Status → colour, in one place |
| `src/components/Chrome.tsx` | Page head, panels, metric rail, pills, key/value |
| `src/components/Timeline.tsx` | The run Gantt — the workflow page's centrepiece |
| `src/components/Sparkline.tsx` | Latency/response-time chart |
| `src/components/Actions.tsx` | Split-button menus, the confirm dialog, toasts, the activity strip, stepper |
| `src/components/ClusterActions.tsx` | The SSP and scheduler restart menus and their wording |
| `src/lib/runActions.ts` | Workflow, job and schedule actions |
| `src/components/` | Shell, bootstrap progress |
| `src/routes/` | One file per view |
| `src/styles/theme.css` | Design tokens and every rule |
| `src/styles/fonts.css` | The vendored fonts (`src/assets/fonts`) |

## Conventions worth keeping

- **Green and red are reserved for status.** Nothing decorative uses them, which
  is what keeps a red dot meaning something is wrong. Go through
  `lib/status.ts` rather than picking a class inline.
- **No charting library.** `components/Sparkline.tsx` is hand-rolled SVG. A
  failed probe is drawn as a *gap*, not a full-height bar — an outage is the
  absence of a measurement, not the slowest possible one.
- **Fonts ship in the bundle.** The dashboard is served by a scheduler on a
  private network, so a request to a font CDN would hang. Space Grotesk (labels,
  prose) and JetBrains Mono (every figure and identifier) are vendored as
  variable woff2 subsets under `src/assets/fonts` and declared in
  `styles/fonts.css`; nothing may reference a remote font.
- **The overview is a bento, not a stack.** `Bento` is a 12-column grid and
  every `Tile` declares its span (`span`, `rows`), so the composition is a
  decision rather than a side effect of `auto-fit`. Rows stretch, and a tile
  puts its plot or footer in a `.tile-end` block so a band of tiles shares one
  bottom edge. A tile's `tone` is the only thing that colours it: `warn` and
  `bad` glow in the corner, `ok` is plain, because fine is the default.
- **Green and red are still reserved; the accent is steel blue.** Only tokens
  declared in `:root` may be referenced from TSX, and the accent is
  `var(--accent)`, never a colour literal.
- **The timeline must not draw work that did not happen.** `_00_step_run`
  defaults `created_at` at row creation, so a `blocked` step has a timestamp;
  plotting from it invents a bar. See `hasStarted` in `WorkflowDetail.tsx`.
- **Responsive by breakpoint, not by device.** Below 860px the sidebar becomes
  an off-canvas drawer; below 560px tables turn into stacked records via
  `data-label` on each `<td>` (a horizontally scrolling table on a phone hides
  the columns that matter). A table that should keep scrolling instead opts out
  with `class="keep-table"`. Add `data-label` to any new cell, and
  `data-empty={!value}` to ones worth omitting entirely when blank.
- **Nav active state comes from the router**, via `<A activeClass end>`. Do not
  hand-roll `location.pathname === href`: the router prefixes every href with
  the `/admin` base, so a manual compare silently matches nothing.
- **`type::string(NONE)` is the string `"NONE"`, not null.** Any nullable column
  a query stringifies — timestamps and record links alike — must go through
  `isAbsent` / `orNull` in `lib/format.ts`, or an unset link becomes a dead
  "open →".
- **One poll for the whole app.** `App.tsx` polls `/overview` and passes it
  down. Views should not add their own poll for data that is already there.
- **Only tokens declared in `:root` may be referenced from TSX.** A missing
  custom property fails silently: the shape simply does not draw. The
  sparkline shipped invisible for a release because of `var(--accent)`.
- **Actions go through `runAction`** (`components/Actions.tsx`): confirm if
  asked, call, then a toast with the server's own sentence. Destructive modes
  require a typed word, and the word is the consequence (`clean`, `restore`),
  never "yes". An option that is not available here is listed muted with the
  reason (`disabledReason`), never hidden.
- **Every action is asynchronous from the operator's seat.** An SSP exits on
  its next heartbeat, a reclone takes minutes. The scheduler reports these as
  operations on `/overview`, and `ActivityStrip` is where they are watched;
  do not invent a spinner that ends when the request returns.
- **A restart is survivable.** `waitForScheduler` in `api/client.ts` waits for
  `/admin/api/config` to go down and come back, then the router remounts on the
  same URL. Only a 401 sends the app to login.
- **Only the plane's own 401 is a sign-out.** `request()` ejects the session
  when the body is exactly `{"error": "Not signed in"}`, the sentence
  `require_session` answers with. Any other 401 (a control plane refusing the
  scheduler's credentials, an SSP) is relayed as an ordinary error toast; a
  backup that failed upstream once logged the operator out over it.
- **Menus are portaled and keyed.** `ActionMenu` renders into `document.body`
  at fixed coordinates, because a menu inside `.table-scroll` was clipped by
  the panel. Its open state is a module-level signal keyed by `menuId`, not
  component state: a menu in a polled table row is re-created on every
  `/overview` poll, and local state would close it every three seconds. Rows
  must pass a stable `menuId`.
- **The Access page shows a token once.** `/tokens` returns the signed token
  and nothing stores it; the page renders the `claude mcp add`, Cursor and VS
  Code snippets from that one response and never asks the server again.
- **`EventSource` is unusable here** — it cannot send an `Authorization`
  header, and we deliberately do not use cookies. `openStream()` in
  `api/client.ts` reads the `fetch` body and parses the event stream by hand.
- **The presence numbers ride the overview poll.** The scheduler samples
  `_00_query` on its own timer and folds the totals into `/overview`, so the
  sidebar count, the presence rail and all three count charts cost no request
  of their own. Only the Views tab fetches, because only it takes filters. Do
  not add a second poll for the rollup.
- **Presence decays; say so.** A client refreshes its liveness on a
  `0.9 × ttl` timer (~9 minutes at the default `10m`), so a closed tab fades
  out of the counts over minutes. Every surface that shows the number also
  shows why it lags — a per-row `expires`, or the note on the page. Do not
  quietly relabel it "online".
- **Separate scales, not one axis.** Views outnumber users by one to two orders
  of magnitude, so a shared y-axis flattens the users line onto the baseline.
  `Sparkline` takes a `format` prop precisely so counts can reuse it without a
  charting library.
- **`.bento` and `.grid-4` stretch; `.grid-2`/`.grid-3` do not.** A band of
  chart tiles is one row, and three different heights in it read as three
  unrelated boxes. The two-and-three-column grids deliberately keep
  `align-items: start` — stretching a panel whose sparkline has no samples
  leaves it in a tall void.
- **The card is the scroll container.** `Shell` renders the routed page inside
  `.main`, one rounded card inset from the dark frame; the shell grid uses
  `minmax(0, 1fr)` rows so the card scrolls itself rather than the window, and
  each page's `PageHead` is sticky to the card's top edge.
- **`formatRelativeTime`, `formatDuration`, `formatMs` and `formatBytes` are
  duplicated** from `apps/devtools/src/utils/formatters.ts`. That app is a
  Chrome extension with no package exports, so a workspace import is not
  available; keep the two in step by hand.
