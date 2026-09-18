/**
 * Regenerate the DevTools screenshots used by the docs site.
 *
 *   pnpm --filter @spooky-sync/devtools screenshots
 *
 * It builds the extension, serves `dist/` with `screenshots/fixture.js` injected
 * ahead of the panel bundle (the fixture stands in for Chrome and for the
 * inspected page), then drives the real panel with Playwright.
 *
 * Capture runs in two passes. The first photographs the panel on its own. The
 * second loads `frame.html` with that picture in it and photographs the browser
 * window drawn around it, so the published image shows the panel where a reader
 * actually meets it: docked under the page it is inspecting. The chrome is
 * ordinary HTML, so restyling it costs nothing and recaptures nothing.
 *
 * Flags:
 *   --no-build     reuse the existing dist/
 *   --only=a,b     capture only these shots
 *   --out=<dir>    write somewhere else
 *   --bare         skip the browser frame and publish the raw panel
 */
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { readFile, writeFile, mkdir, readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, extname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const EXT = resolve(HERE, '..');
const DIST = join(EXT, 'dist');

/**
 * Playwright is not a dependency of this package: the monorepo already installs
 * it for `example/e2e`, and the screenshots are a local tool, not part of any
 * build. Resolve whichever copy the workspace has.
 */
async function loadChromium() {
  const roots = [
    import.meta.url,
    join(EXT, '../../example/e2e/package.json'),
    join(EXT, '../../package.json'),
  ];
  for (const from of roots) {
    try {
      const entry = createRequire(from).resolve('@playwright/test');
      const mod = await import(pathToFileURL(entry).href);
      const chromium = mod.chromium ?? mod.default?.chromium;
      if (chromium) return chromium;
    } catch {
      /* try the next root */
    }
  }
  throw new Error(
    'Playwright not found. Install it once with: pnpm --filter @example/e2e exec playwright install chromium'
  );
}

const args = process.argv.slice(2);
const flag = (name) =>
  args
    .find((a) => a.startsWith(`--${name}=`))
    ?.split('=')
    .slice(1)
    .join('=');

const OUT = resolve(flag('out') ?? join(EXT, '../landing-page/public/docs/devtools'));
const BARE = args.includes('--bare');
const ONLY = flag('only')
  ?.split(',')
  .map((s) => s.trim())
  .filter(Boolean);

// Wide enough that all eight tabs fit on the toolbar without the overflow
// chevron. Height is per shot: a panel with six rows in it should not be
// published with 400px of empty space under them.
const WIDTH = 1280;
const SCALE = 2;
const DEFAULT_HEIGHT = 620;
// A screenshot taller than this stops being readable in a docs page.
const MAX_HEIGHT = 1600;

// The inspected page keeps this share of the framed image. It is a sliver on
// purpose: enough to place the panel under a real page, not enough to compete
// with it for attention.
const SITE_SHARE = 0.05;
// ...but never less than this, or the sliver stops reading as a page at all.
const SITE_MIN = 44;
// Browser chrome above the page (title bar + toolbar) and the DevTools tab
// strip below it. Kept in step with `frame.html` by the assertion in `frame()`.
const CHROME_HEIGHT = 78;
const DT_BAR_HEIGHT = 31;

const SITE_TITLE = 'Acme Chat';
const SITE_URL = 'chat.acme.test';

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
    autoHeight: true,
    tab: 'MCP',
    caption: 'The MCP bridge that lets an AI assistant read the same state.',
  },
  {
    name: 'events',
    tab: 'Events',
    height: 440,
    caption: 'The client event log, filtered by type.',
  },
];

/** Serve `dist/`, with `panel.html` rewritten to load the fixture first. */
async function serve() {
  const types = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.map': 'application/json',
    '.png': 'image/png',
    '.json': 'application/json',
  };

  const panelHtml = await readFile(join(DIST, 'panel.html'), 'utf-8');
  const harness = panelHtml.replace(
    /<script\b/,
    '<script src="__fixture__.js"></script>\n    <script'
  );
  if (harness === panelHtml) throw new Error('could not inject the fixture into panel.html');
  const fixture = await readFile(join(HERE, 'fixture.js'), 'utf-8');
  const frameHtml = await readFile(join(HERE, 'frame.html'), 'utf-8');

  // Raw panel captures live here between the two passes, never on disk: the
  // frame page loads them straight back out of memory.
  const raw = new Map();

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://127.0.0.1');
    const path = decodeURIComponent(url.pathname);
    try {
      if (path === '/__raw__') {
        const body = raw.get(url.searchParams.get('shot'));
        if (!body) throw new Error('no such capture');
        res.writeHead(200, { 'content-type': types['.png'] });
        res.end(body);
        return;
      }
      if (path === '/__frame__') {
        const q = url.searchParams;
        const html = frameHtml
          .replaceAll('{{IMG}}', `/__raw__?shot=${encodeURIComponent(q.get('shot') ?? '')}`)
          .replaceAll('{{IMG_HEIGHT}}', String(Number(q.get('height')) || 0))
          .replaceAll('{{SITE_HEIGHT}}', String(Number(q.get('site')) || SITE_MIN))
          .replaceAll('{{TITLE}}', SITE_TITLE)
          .replaceAll('{{URL}}', SITE_URL);
        res.writeHead(200, { 'content-type': types['.html'] });
        res.end(html);
        return;
      }
      if (path === '/' || path === '/panel.html') {
        res.writeHead(200, { 'content-type': types['.html'] });
        res.end(harness);
        return;
      }
      if (path === '/__fixture__.js') {
        res.writeHead(200, { 'content-type': types['.js'] });
        res.end(fixture);
        return;
      }
      const file = join(DIST, path);
      if (!file.startsWith(DIST)) throw new Error('outside dist');
      const body = await readFile(file);
      res.writeHead(200, { 'content-type': types[extname(file)] ?? 'application/octet-stream' });
      res.end(body);
    } catch {
      res.writeHead(404).end('not found');
    }
  });

  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const origin = `http://127.0.0.1:${server.address().port}`;
  return { server, raw, url: `${origin}/panel.html`, origin };
}

/**
 * The page sliver is a share of the FINAL image, so it has to be solved for
 * rather than taken off the panel height:
 *
 *   site = SITE_SHARE * (chrome + site + panel)
 */
function siteHeight(panelHeight) {
  const fixed = CHROME_HEIGHT + DT_BAR_HEIGHT + panelHeight;
  return Math.max(SITE_MIN, Math.round((SITE_SHARE * fixed) / (1 - SITE_SHARE)));
}

/**
 * Pass two: draw the browser window around each captured panel.
 *
 * The window is screenshotted as an element, so its rounded corners come out
 * transparent and the docs page's own background shows through them.
 */
async function frameAll(context, origin, panels) {
  const page = await context.newPage();
  page.on('pageerror', (e) => console.error('  frame error:', e.message));

  // oxlint-disable no-await-in-loop -- one page, reused per shot
  for (const { name, height } of panels) {
    const site = siteHeight(height);
    await page.setViewportSize({
      width: WIDTH + 80,
      height: CHROME_HEIGHT + DT_BAR_HEIGHT + site + height + 80,
    });
    await page.goto(
      `${origin}/__frame__?shot=${encodeURIComponent(name)}&height=${height}&site=${site}`,
      { waitUntil: 'networkidle' }
    );

    const window = page.locator('.window');
    // The chrome constants feed `siteHeight`, so a change to `frame.html` that
    // nobody mirrored here would silently shift every page sliver. Catch it.
    const measured = await window.evaluate((el, imgHeight) => {
      const panel = el.querySelector('.panel').getBoundingClientRect().height;
      const site = el.querySelector('.site').getBoundingClientRect().height;
      return { chrome: el.getBoundingClientRect().height - panel - site, panel, imgHeight };
    }, height);
    if (Math.abs(measured.chrome - (CHROME_HEIGHT + DT_BAR_HEIGHT)) > 1) {
      throw new Error(
        `frame.html chrome is ${measured.chrome}px, but capture.mjs assumes ` +
          `${CHROME_HEIGHT + DT_BAR_HEIGHT}px. Update CHROME_HEIGHT / DT_BAR_HEIGHT.`
      );
    }

    await writeFile(join(OUT, `${name}.png`), await window.screenshot({ omitBackground: true }));
    console.log(`  ▣ ${name}.png`);
  }
  // oxlint-enable no-await-in-loop

  await page.close();
}

async function main() {
  if (!args.includes('--no-build')) {
    console.log('building the extension…');
    execFileSync('pnpm', ['build'], { cwd: EXT, stdio: 'inherit' });
  }
  if (!existsSync(join(DIST, 'panel.html'))) {
    throw new Error('dist/panel.html is missing; run without --no-build');
  }

  const { server, raw, url, origin } = await serve();
  await mkdir(OUT, { recursive: true });

  // The docs site renders dark only, so the panel is captured in its dark theme
  // (it follows `prefers-color-scheme`).
  const chromium = await loadChromium();
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: WIDTH, height: DEFAULT_HEIGHT },
    deviceScaleFactor: SCALE,
    colorScheme: 'dark',
    reducedMotion: 'reduce',
  });
  const page = await context.newPage();
  page.on('pageerror', (e) => console.error('  page error:', e.message));

  const shots = SHOTS.filter((s) => !ONLY || ONLY.includes(s.name));
  const written = [];
  /** Raw captures waiting for pass two: `{ name, height }` in CSS pixels. */
  const panels = [];

  // oxlint-disable no-await-in-loop -- the shots share one page; they have to run in order
  for (const shot of shots) {
    await page.setViewportSize({ width: WIDTH, height: shot.height ?? DEFAULT_HEIGHT });
    await page.goto(url, { waitUntil: 'networkidle' });
    await page.waitForSelector('.tabs .tab-btn');
    await page.click(`.tab-btn:text-is("${shot.tab}")`);
    if (shot.scrollTo) {
      await page
        .locator(shot.scrollTo, { hasText: shot.scrollToText })
        .first()
        .evaluate((el) => el.scrollIntoView({ block: 'start' }));
    }
    if (shot.setup) await shot.setup(page);
    // Let the on-demand fetches (tables, storage, flags) settle.
    await page.waitForTimeout(600);

    let height = shot.height ?? DEFAULT_HEIGHT;
    if (shot.autoHeight) {
      // The panel nests its scrollers (`.tab-content` is clipped; the tab's own
      // container scrolls), so grow by the largest overflow found anywhere in
      // the active tab rather than assuming which element scrolls.
      const overflow = await page.evaluate(() => {
        const body = document.querySelector('.tab-content.active');
        if (!body) return 0;
        let worst = 0;
        for (const el of [body, ...body.querySelectorAll('*')]) {
          worst = Math.max(worst, el.scrollHeight - el.clientHeight);
        }
        return Math.ceil(worst);
      });
      height = Math.min(height + overflow + 8, MAX_HEIGHT);
      await page.setViewportSize({ width: WIDTH, height });
      await page.waitForTimeout(250);
    }

    const shotBuffer = await page.screenshot();
    if (BARE) {
      await writeFile(join(OUT, `${shot.name}.png`), shotBuffer);
    } else {
      raw.set(shot.name, shotBuffer);
      panels.push({ name: shot.name, height });
    }
    written.push(shot.name);
    console.log(`  ✓ ${shot.name}`);
  }
  // oxlint-enable no-await-in-loop

  if (!BARE) await frameAll(context, origin, panels);

  await browser.close();
  server.close();

  // A manifest so the docs page and this script cannot disagree about which
  // files exist or what they show.
  await writeFile(
    join(OUT, 'shots.json'),
    `${JSON.stringify(
      {
        generatedBy: 'apps/devtools/screenshots/capture.mjs',
        width: WIDTH,
        deviceScaleFactor: SCALE,
        theme: 'dark',
        frame: BARE ? null : { chrome: 'frame.html', siteShare: SITE_SHARE, site: SITE_TITLE },
        shots: SHOTS.map(({ name, tab, caption }) => ({ name, tab, caption, file: `${name}.png` })),
      },
      null,
      2
    )}\n`
  );

  console.log(`\n${written.length} screenshot(s) → ${OUT}`);
  console.log((await readdir(OUT)).join('  '));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
