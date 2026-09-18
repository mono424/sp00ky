/**
 * The screenshot engine shared by every app that publishes UI screenshots to
 * the docs site.
 *
 * An app supplies a config (see `capture()` below) describing what to build,
 * what to serve, how to feed it fixture data and which screens to photograph.
 * Everything that is the same for all of them lives here: the static server,
 * the Playwright boot, per-shot viewport sizing, the browser window drawn
 * around each capture, and the manifest written beside the images.
 *
 * Capture runs in two passes. The first photographs the app on its own. The
 * second loads `frame.html` with that picture in it and photographs the browser
 * window drawn around it. The chrome is ordinary HTML, so restyling it
 * recaptures nothing.
 *
 * Flags, handled here for every caller:
 *   --no-build     reuse the existing dist/
 *   --only=a,b     capture only these shots
 *   --out=<dir>    write somewhere else
 *   --bare         skip the browser frame and publish the raw capture
 */
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { readFile, writeFile, mkdir, readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, extname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, '../..');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.map': 'application/json',
  '.json': 'application/json',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
  '.woff2': 'font/woff2',
  '.woff': 'font/woff',
  '.ico': 'image/x-icon',
};

/** A capture taller than this stops being readable in a docs page. */
const MAX_HEIGHT = 1600;

/**
 * Playwright is not a dependency of any of these packages: the monorepo already
 * installs it for `example/e2e`, and the screenshots are a local tool, not part
 * of any build. Resolve whichever copy the workspace has.
 */
async function loadChromium() {
  const roots = [
    import.meta.url,
    join(REPO, 'example/e2e/package.json'),
    join(REPO, 'package.json'),
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

function parseArgs(argv) {
  const flag = (name) =>
    argv
      .find((a) => a.startsWith(`--${name}=`))
      ?.split('=')
      .slice(1)
      .join('=');
  return {
    build: !argv.includes('--no-build'),
    bare: argv.includes('--bare'),
    out: flag('out'),
    only: flag('only')
      ?.split(',')
      .map((s) => s.trim())
      .filter(Boolean),
  };
}

/**
 * The page sliver in a DevTools frame is a share of the FINAL image, so it has
 * to be solved for rather than taken off the capture height:
 *
 *   site = share * (chrome + site + capture)
 */
function siteHeight(frame, captureHeight) {
  if (!frame.devtools) return 0;
  const fixed = frame.chromeHeight + frame.barHeight + captureHeight;
  return Math.max(frame.siteMin, Math.round((frame.siteShare * fixed) / (1 - frame.siteShare)));
}

/** Serve the app's `dist/`, the frame page, and the in-memory raw captures. */
async function serve(cfg, frameHtml) {
  // Raw captures live here between the two passes, never on disk: the frame
  // page loads them straight back out of memory.
  const raw = new Map();

  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://127.0.0.1');
    const path = decodeURIComponent(url.pathname);
    try {
      if (path === '/__raw__') {
        const body = raw.get(url.searchParams.get('shot'));
        if (!body) throw new Error('no such capture');
        res.writeHead(200, { 'content-type': MIME['.png'] });
        res.end(body);
        return;
      }
      if (path === '/__frame__') {
        const q = url.searchParams;
        const html = frameHtml
          .replaceAll('{{IMG}}', `/__raw__?shot=${encodeURIComponent(q.get('shot') ?? '')}`)
          .replaceAll('{{IMG_HEIGHT}}', String(Number(q.get('height')) || 0))
          .replaceAll('{{SITE_HEIGHT}}', String(Number(q.get('site')) || 0))
          .replaceAll('{{MODE}}', cfg.frame.devtools ? 'devtools' : 'page')
          .replaceAll('{{TITLE}}', cfg.frame.title)
          .replaceAll('{{URL}}', cfg.frame.url)
          .replaceAll('{{ICON}}', cfg.frame.icon ?? '');
        res.writeHead(200, { 'content-type': MIME['.html'] });
        res.end(html);
        return;
      }

      // App-specific routes (a fixture script, an index rewritten to load it).
      const extra = await cfg.route?.(path);
      if (extra) {
        res.writeHead(200, { 'content-type': extra.type ?? MIME['.html'] });
        res.end(extra.body);
        return;
      }

      const file = join(cfg.dist, cfg.rewrite ? cfg.rewrite(path) : path);
      if (!file.startsWith(cfg.dist)) throw new Error('outside dist');
      const body = await readFile(file);
      res.writeHead(200, { 'content-type': MIME[extname(file)] ?? 'application/octet-stream' });
      res.end(body);
    } catch {
      res.writeHead(404).end('not found');
    }
  });

  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  return { server, raw, origin: `http://127.0.0.1:${server.address().port}` };
}

/**
 * Pass two: draw the browser window around each capture.
 *
 * The window is screenshotted as an element, so its rounded corners come out
 * transparent and the docs page's own background shows through them.
 */
async function frameAll(cfg, context, origin, captures, out) {
  const { frame } = cfg;
  const page = await context.newPage();
  page.on('pageerror', (e) => console.error('  frame error:', e.message));

  // oxlint-disable no-await-in-loop -- one page, reused per shot
  for (const { name, height } of captures) {
    const site = siteHeight(frame, height);
    const chrome = frame.chromeHeight + (frame.devtools ? frame.barHeight : 0);
    await page.setViewportSize({
      width: cfg.width + 80,
      height: chrome + site + height + 80,
    });
    await page.goto(
      `${origin}/__frame__?shot=${encodeURIComponent(name)}&height=${height}&site=${site}`,
      { waitUntil: 'networkidle' }
    );

    const window = page.locator('.window');
    // The chrome heights feed `siteHeight` and the viewport above, so a change
    // to `frame.html` that nobody mirrored in the config would silently shift
    // every capture. Catch it.
    const measured = await window.evaluate((el) => {
      const height = (sel) => el.querySelector(sel)?.getBoundingClientRect().height ?? 0;
      return el.getBoundingClientRect().height - height('.capture') - height('.site');
    });
    if (Math.abs(measured - chrome) > 1) {
      throw new Error(
        `frame.html chrome is ${measured}px, but the config assumes ${chrome}px. ` +
          'Update frame.chromeHeight / frame.barHeight.'
      );
    }

    await writeFile(join(out, `${name}.png`), await window.screenshot({ omitBackground: true }));
    console.log(`  ▣ ${name}.png`);
  }
  // oxlint-enable no-await-in-loop

  await page.close();
}

/**
 * Run a capture.
 *
 * @param cfg.name          What is being photographed, for the log line.
 * @param cfg.root          Package directory; `pnpm build` runs here.
 * @param cfg.dist          Built output to serve.
 * @param cfg.distEntry     A file that must exist in `dist` for it to be usable.
 * @param cfg.out           Default output directory for the PNGs.
 * @param cfg.width         Capture width in CSS pixels.
 * @param cfg.scale         Device scale factor.
 * @param cfg.defaultHeight Viewport height for a shot that names none.
 * @param cfg.settleMs      Pause before capturing, for in-flight fetches.
 * @param cfg.frame         Browser window: `{ devtools, title, url, icon,
 *                          chromeHeight, barHeight, siteShare, siteMin }`.
 * @param cfg.shots         `{ name, caption, height?, autoHeight?, ... }[]`.
 * @param cfg.route         Optional extra server route: path -> `{body, type}`.
 * @param cfg.rewrite       Optional request path -> path within `dist`.
 * @param cfg.prepare       Optional `({ context, origin })` before any shot. Register
 *                          routes and init scripts on the CONTEXT: each shot gets
 *                          its own page.
 * @param cfg.open          `({ page, shot, origin })`: navigate to the screen.
 * @param cfg.overflow      Optional selector whose overflow `autoHeight` measures.
 */
export async function capture(cfg) {
  const argv = process.argv.slice(2);
  const args = parseArgs(argv);
  const out = resolve(args.out ?? cfg.out);

  if (args.build) {
    console.log(`building ${cfg.name}…`);
    execFileSync('pnpm', ['build'], { cwd: cfg.root, stdio: 'inherit' });
  }
  if (!existsSync(join(cfg.dist, cfg.distEntry))) {
    throw new Error(`${cfg.distEntry} is missing from dist; run without --no-build`);
  }

  const frameHtml = await readFile(join(HERE, 'frame.html'), 'utf-8');
  const { server, raw, origin } = await serve(cfg, frameHtml);
  await mkdir(out, { recursive: true });

  // The docs site renders dark only, so everything is captured in its dark
  // theme (both apps follow `prefers-color-scheme`).
  const chromium = await loadChromium();
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: cfg.width, height: cfg.defaultHeight },
    deviceScaleFactor: cfg.scale,
    colorScheme: 'dark',
    reducedMotion: 'reduce',
  });
  // Routes and init scripts are registered on the CONTEXT, not on a page,
  // because every shot gets a page of its own below.
  await cfg.prepare?.({ context, origin });

  const shots = cfg.shots.filter((s) => !args.only || args.only.includes(s.name));
  const captures = [];

  // oxlint-disable no-await-in-loop -- the shots share one page; they run in order
  for (const shot of shots) {
    // A fresh page per shot. Reusing one carried state between screens (a
    // fetched list already in a signal, a debounce still pending), which made
    // `--only=x` produce a different image from the same shot in a full run.
    const page = await context.newPage();
    page.on('pageerror', (e) => console.error('  page error:', e.message));

    let height = shot.height ?? cfg.defaultHeight;
    await page.setViewportSize({ width: cfg.width, height });
    await cfg.open({ page, shot, origin });

    if (shot.scrollTo) {
      await page
        .locator(shot.scrollTo, { hasText: shot.scrollToText })
        .first()
        .evaluate((el) => el.scrollIntoView({ block: 'start' }));
    }
    if (shot.setup) await shot.setup(page);
    await page.waitForTimeout(shot.settleMs ?? cfg.settleMs ?? 500);

    if (shot.autoHeight) {
      // Both apps nest their scrollers, so grow by the largest overflow found
      // anywhere in the screen rather than assuming which element scrolls.
      const overflow = await page.evaluate(
        (sel) => {
          const body = sel ? document.querySelector(sel) : document.body;
          if (!body) return 0;
          let worst = 0;
          for (const el of [body, ...body.querySelectorAll('*')]) {
            worst = Math.max(worst, el.scrollHeight - el.clientHeight);
          }
          return Math.ceil(worst);
        },
        shot.overflow ?? cfg.overflow ?? null
      );
      height = Math.min(height + overflow + 8, MAX_HEIGHT);
      await page.setViewportSize({ width: cfg.width, height });
      await page.waitForTimeout(250);
    }

    const buffer = await page.screenshot();
    if (args.bare) {
      await writeFile(join(out, `${shot.name}.png`), buffer);
    } else {
      raw.set(shot.name, buffer);
      captures.push({ name: shot.name, height });
    }
    await page.close();
    console.log(`  ✓ ${shot.name}`);
  }
  // oxlint-enable no-await-in-loop

  if (!args.bare) await frameAll(cfg, context, origin, captures, out);

  await browser.close();
  server.close();

  // A manifest so the docs pages and this script cannot disagree about which
  // files exist or what they show.
  await writeFile(
    join(out, 'shots.json'),
    `${JSON.stringify(
      {
        generatedBy: cfg.generatedBy,
        width: cfg.width,
        deviceScaleFactor: cfg.scale,
        theme: 'dark',
        frame: args.bare ? null : { chrome: 'tools/screenshots/frame.html', ...cfg.frame },
        shots: cfg.shots.map(({ name, caption }) => ({ name, caption, file: `${name}.png` })),
      },
      null,
      2
    )}\n`
  );

  console.log(`\n${shots.length} screenshot(s) → ${out}`);
  console.log((await readdir(out)).join('  '));
}

/** Wrap `capture` so a failure exits non-zero with a readable message. */
export function run(cfg) {
  capture(cfg).catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
