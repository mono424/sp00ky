# Documentation screenshots

The engine behind every UI screenshot on the docs site. Two apps use it today:

| App                | Command                                            | Publishes to      |
| ------------------ | -------------------------------------------------- | ----------------- |
| DevTools extension | `pnpm --filter @spooky-sync/devtools screenshots`  | `/docs/devtools/` |
| Admin dashboard    | `pnpm --filter @spooky-sync/dashboard screenshots` | `/docs/admin/`    |

PNGs land in `apps/landing-page/public/docs/<app>/`, alongside a `shots.json`
manifest listing what each file shows.

## Why a harness and not a screenshot key

A hand-taken screenshot needs a running backend, real data that cannot be
published, and a person to retake all of them every time a tab moves. These are
taken from the real bundle driven by a frozen fixture, so regenerating after a
UI change is one command, and an unchanged screen regenerates a byte-identical
PNG.

None of the data is read from a real deployment. Both fixtures invent one
project and populate it.

## The two passes

**Pass one** photographs the app on its own, at a per-shot viewport height.

**Pass two** loads `frame.html` with that picture in it and photographs the
browser window drawn around it. `frame.html` has two modes:

- `page`: a plain browser window, which is what the dashboard is.
- `devtools`: the window plus a sliver of the page being inspected and the
  DevTools tab strip, which is where the extension's panel actually lives. The
  sliver is `siteShare` (5%) of the finished image, floored at `siteMin`.

The window is captured as an element, so its rounded corners come out
transparent and the docs page's background shows through them.

Because the chrome is ordinary HTML, restyling it is one edit to `frame.html`
and a rerun: no app is recaptured for it. `--bare` skips it entirely.

## Flags

Handled by the engine for every app.

| Flag          | Effect                                                    |
| ------------- | --------------------------------------------------------- |
| `--no-build`  | Reuse the existing `dist/` instead of rebuilding first.   |
| `--only=a,b`  | Capture only these shots.                                 |
| `--out=<dir>` | Write somewhere other than the docs site's public folder. |
| `--bare`      | Skip the browser frame and publish the raw capture.       |

## Writing a config

`capture(cfg)` takes what the app needs and nothing else. See
`apps/devtools/screenshots/capture.mjs` (a fixture script injected ahead of the
bundle, shots that click a tab) and `apps/dashboard/screenshots/capture.mjs`
(an API answered by route interception, shots that navigate to a route). The
JSDoc on `capture` in `engine.mjs` lists every field.

The two hooks that matter:

- `prepare({ page, context, origin })` runs once, before any shot: register
  routes, seed storage, freeze a clock.
- `open({ page, shot, origin })` runs per shot and leaves the app on the screen
  to photograph.

## When a screenshot looks wrong

- **Cropped.** Raise that shot's `height`, set `autoHeight`, or park it on the
  interesting section with `scrollTo` / `scrollToText`.
- **A dash, `NaN`, or an empty section.** The fixture and the app's types have
  drifted, and the types are right.
- **Different on every run.** Something un-pinned reached the page: a clock, a
  random id, or the capture server's own port. All three are already pinned;
  a new one needs the same treatment.
- **The frame's proportions moved.** The config's `chromeHeight` / `barHeight`
  describe `frame.html`. The run measures the rendered frame and fails loudly
  when they disagree, which is the error you are reading.

## Playwright

Not a dependency of any package: the monorepo already installs it for
`example/e2e`, and these are a local tool rather than part of any build. The
engine resolves whichever copy the workspace has. Its Chromium build is
downloaded once:

```bash
pnpm --filter @example/e2e exec playwright install chromium
```
