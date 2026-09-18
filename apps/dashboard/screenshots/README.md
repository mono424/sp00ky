# Dashboard screenshots

The screenshots on [`/docs/reference/admin-dashboard`](../../landing-page/src/pages/docs/reference/admin-dashboard.mdx).

```bash
pnpm --filter @spooky-sync/dashboard screenshots
```

The engine, the flags and the browser frame are shared:
[`tools/screenshots/README.md`](../../../tools/screenshots/README.md). This
folder is only what is particular to the dashboard.

## `fixtures.mjs`

One invented project ("acme") with a plausible cluster behind it: a scheduler,
two SSPs, two backends, presence, an outbox, schedules, workflow runs, views,
incidents and a backup catalog. Nothing is read from a real deployment.

`ROUTES` maps `METHOD /path` (without the `/admin/api` prefix) to the JSON
answered. `STREAMS` does the same for the three screens that read Server-Sent
Events rather than polling (jobs, workflows and logs), whose frames are
delivered in one go.

Shapes mirror `src/api/types.ts`. A screen rendering a dash, an empty section or
`NaN` means this file and the types have drifted, and the types are right.

## `capture.mjs`

The dashboard reaches its scheduler through exactly one place (`src/api/client.ts`:
`fetch(baseUrl + '/admin/api' + path)` with a bearer token from localStorage), so
standing in for a whole cluster is one route handler plus one seeded token. The
bundle runs unmodified.

Two details worth knowing before changing it:

- **The page is served from `https://acme-admin.spky.cloud`**, not from the
  capture server's address. The dashboard shows its own origin back to the
  operator (the MCP endpoint on the Access page), so a random localhost port
  both looked wrong and changed that screenshot on every run.
- **An unanswered request is reported, not silently 404'd.** Adding a screen
  that fetches something new prints the missing `METHOD /path` at the end of the
  run, which is how the fixture list was built in the first place.

## When the dashboard changes

- **A route was added.** Add a `SHOTS` entry, run, and add whatever endpoints
  the run reports as unanswered.
- **A response shape changed.** Update the matching fixture constant.
