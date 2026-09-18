# DevTools screenshots

The screenshots on [`/docs/reference/devtools`](../../landing-page/src/pages/docs/reference/devtools.mdx).

```bash
pnpm --filter @spooky-sync/devtools screenshots
```

The engine, the flags and the browser frame are shared:
[`tools/screenshots/README.md`](../../../tools/screenshots/README.md). This
folder is only what is particular to the panel.

## `fixture.js`

The panel talks to exactly three Chrome APIs (`chrome.runtime.connect`,
`chrome.devtools.inspectedWindow.eval` and `.tabId`) and otherwise reaches the
inspected page through `window.__00__` plus a few CustomEvents that
`page-script.ts` answers.

`fixture.js` plays all of those roles at once (background script, content script
and page script) against a frozen dataset, so `dist/panel.js` runs unmodified
with no browser extension, no backend and no app. The clock is pinned too.

The data is invented, for a fictional team-chat app.

## `capture.mjs`

One entry per tab, each naming the tab button to click. Keep the list in step
with `Tabs.tsx`: a tab added there and not here simply goes undocumented.

## When the panel changes

- **A tab was added, renamed or removed.** Edit `SHOTS`, then add the image to
  `devtools.mdx`.
- **A message or state shape changed.** Update the matching fixture constant.
  If a screenshot renders `—`, `NaN` or an empty section, the fixture and the
  panel disagree about a field name; the panel is right.
