/**
 * Regenerate the DevTools screenshots used by the docs site.
 *
 *   pnpm --filter @spooky-sync/devtools screenshots
 *
 * The engine (`tools/screenshots/engine.mjs`) does the building, serving,
 * sizing and framing. This file is only what is particular to the panel: the
 * fixture injected ahead of the bundle, and which tab each shot opens.
 *
 * Flags: --no-build, --only=a,b, --out=<dir>, --bare
 */
import { readFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { run } from '../../../tools/screenshots/engine.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const EXT = resolve(HERE, '..');
const DIST = join(EXT, 'dist');

// `panel.html` gets the fixture ahead of the bundle. The fixture stands in for
// Chrome and for the inspected page, so `dist/panel.js` runs unmodified with no
// extension, no backend and no app.
const panelHtml = await readFile(join(DIST, 'panel.html'), 'utf-8');
const harness = panelHtml.replace(
  /<script\b/,
  '<script src="__fixture__.js"></script>\n    <script'
);
if (harness === panelHtml) throw new Error('could not inject the fixture into panel.html');
const fixture = await readFile(join(HERE, 'fixture.js'), 'utf-8');

/**
 * Each shot names the tab to open and, when a screen only becomes interesting
 * after a click, what to click. Keep the list in step with `Tabs.tsx`.
 *
 * `scrollTo` (with `scrollToText`) parks a long tab on the section worth
 * showing instead of publishing one unreadably tall image.
 *
 * `height` is the viewport height for that shot: a panel with six rows in it
 * should not be published with 400px of empty space under it. `autoHeight`
 * additionally grows the viewport to whatever the tab's scroll container
 * actually needs, so a tab that gains a section does not get cropped the next
 * time these are regenerated.
 */
const SHOTS = [
  {
    name: 'queries',
    tab: 'Queries',
    height: 380,
    caption: 'Every live query with its status, update count and payload size.',
  },
  {
    name: 'queries-detail',
    tab: 'Queries',
    height: 460,
    caption: 'The detail panel for one query: text, variables, rows and timings.',
    async setup(page) {
      await page.click('tr[data-query-hash="814233901"]');
      await page.click('.detail-tab:has-text("Query")');
    },
  },
  {
    name: 'timing',
    tab: 'Timing',
    height: 300,
    caption: 'Per-phase p90 for every query, slowest first.',
  },
  {
    name: 'database',
    tab: 'Database',
    height: 430,
    caption: 'Browse and edit the local cache or the remote database.',
    async setup(page) {
      await page.click('.table-item:has(.table-item-name:text-is("message"))');
    },
  },
  {
    name: 'storage',
    tab: 'Storage',
    height: 690,
    caption:
      'Which local engine is running, whether it is really persistent, and who owns the store.',
  },
  {
    name: 'storage-files',
    tab: 'Storage',
    height: 760,
    scrollTo: '.mcp-section:has(h3)',
    scrollToText: 'Bucket file cache',
    caption: 'The bucket file cache and what the store actually holds on disk, down to the file.',
  },
  {
    name: 'access',
    tab: 'Access',
    height: 700,
    caption: 'Who the page is signed in as, and impersonating another user from the panel.',
  },
  {
    name: 'access-flags',
    tab: 'Access',
    height: 700,
    scrollTo: '.mcp-section:has(h3)',
    scrollToText: 'Flags',
    caption: 'Every feature flag the client has seen, with a browser-local override.',
  },
  {
    name: 'stack',
    tab: 'Stack',
    height: 600,
    caption: 'Frontend versus backend versions, live entities and the e2e heartbeat.',
  },
  {
    name: 'mcp',
    tab: 'MCP',
    autoHeight: true,
    caption: 'The MCP bridge that lets an AI assistant read the same state.',
  },
  {
    name: 'events',
    tab: 'Events',
    height: 440,
    caption: 'The client event log, filtered by type.',
  },
];

run({
  name: 'the extension',
  generatedBy: 'apps/devtools/screenshots/capture.mjs',
  root: EXT,
  dist: DIST,
  distEntry: 'panel.html',
  out: join(EXT, '../landing-page/public/docs/devtools'),

  // Wide enough that all eight tabs fit on the toolbar without the overflow
  // chevron.
  width: 1280,
  scale: 2,
  defaultHeight: 620,
  // Let the on-demand fetches (tables, storage, flags) settle.
  settleMs: 600,
  overflow: '.tab-content.active',

  // The panel lives docked under the page it is inspecting, so that is how it
  // is published: a sliver of the app, then the DevTools tab strip, then the
  // panel. The sliver is 5% of the finished image, floored so it never stops
  // reading as a page.
  frame: {
    devtools: true,
    title: 'Acme Chat',
    url: 'chat.acme.test',
    chromeHeight: 78,
    barHeight: 31,
    siteShare: 0.05,
    siteMin: 44,
  },

  route: (path) =>
    path === '/' || path === '/panel.html'
      ? { body: harness }
      : path === '/__fixture__.js'
        ? { body: fixture, type: 'text/javascript; charset=utf-8' }
        : null,

  async open({ page, shot, origin }) {
    await page.goto(`${origin}/panel.html`, { waitUntil: 'networkidle' });
    await page.waitForSelector('.tabs .tab-btn');
    await page.click(`.tab-btn:text-is("${shot.tab}")`);
  },

  shots: SHOTS,
});
