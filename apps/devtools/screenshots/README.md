# Documentation screenshots

`capture.mjs` produces the DevTools screenshots used by the docs site
(`/docs/reference/devtools`). Regenerate them whenever the panel's UI changes:

```bash
pnpm --filter @spooky-sync/devtools screenshots
```

PNGs land in `apps/landing-page/public/docs/devtools/`, alongside a `shots.json`
manifest listing what each file shows.

## How it works

### Pass one: the panel

The panel talks to exactly three Chrome APIs (`chrome.runtime.connect`,
`chrome.devtools.inspectedWindow.eval` and `.tabId`) and otherwise reaches the
inspected page through `window.__00__` plus a few CustomEvents that
`page-script.ts` answers.

`fixture.js` plays all of those roles at once (background script, content script
and page script) against a frozen dataset, so `dist/panel.js` runs unmodified
with no browser extension, no backend and no app. The clock is pinned too, so a
regenerated screenshot is byte-identical when nothing changed.

All of the data is invented, for a fictional team-chat app. Nothing is read from
a real deployment.

### Pass two: the browser window

A panel on its own is a rectangle of UI with no context. The second pass loads
`frame.html` with that rectangle in it and photographs the browser window drawn
around it: title bar, omnibox, a sliver of the page being inspected, and the
DevTools tab strip with **00** selected.

The sliver is `SITE_SHARE` (5%) of the finished image, floored at `SITE_MIN` so
it never stops reading as a page. Both live in `capture.mjs`, next to
`CHROME_HEIGHT` and `DT_BAR_HEIGHT`, which describe `frame.html`'s own chrome.
The capture measures the rendered frame and fails loudly if those two drift
apart, so restyling the chrome can never silently resize every page sliver.

Because the chrome is ordinary HTML, restyling it costs one edit to
`frame.html` and a rerun. No panel is recaptured for it, and `--bare` skips it
entirely when you want the panel on its own.

## Flags

| Flag          | Effect                                                    |
| ------------- | --------------------------------------------------------- |
| `--no-build`  | Reuse the existing `dist/` instead of rebuilding first.   |
| `--only=a,b`  | Capture only these shots.                                 |
| `--out=<dir>` | Write somewhere other than the docs site's public folder. |
| `--bare`      | Skip the browser frame and publish the raw panel.         |

## When the panel changes

- **A tab was added, renamed or removed.** Add or edit its entry in `SHOTS`
  (the `tab` field is the button label from `Tabs.tsx`), then add the image to
  `apps/landing-page/src/pages/docs/reference/devtools.mdx`.
- **A tab gained a section and its screenshot now looks cropped.** Raise that
  shot's `height`, or park it on the new section with `scrollTo` /
  `scrollToText`.
- **The browser chrome should look different.** Edit `frame.html` and rerun.
  If you change its heights, update `CHROME_HEIGHT` / `DT_BAR_HEIGHT` in
  `capture.mjs`; the run tells you when they no longer match.
- **A message or state shape changed.** Update the matching fixture constant.
  If a screenshot suddenly renders `—`, `NaN` or an empty section, the fixture
  and the panel disagree about a field name; the panel is right.

## Playwright

Playwright is not a dependency of this package. The monorepo already installs it
for `example/e2e`, and the script resolves whichever copy the workspace has, so
the screenshots stay a local tool rather than a build dependency. Its Chromium
build is downloaded once:

```bash
pnpm --filter @example/e2e exec playwright install chromium
```
