/**
 * Regenerate the admin dashboard screenshots used by the docs site.
 *
 *   pnpm --filter @spooky-sync/dashboard screenshots
 *
 * The engine (`tools/screenshots/engine.mjs`) does the building, serving,
 * sizing and framing. This file is only what is particular to the dashboard:
 * answering its API from `fixtures.mjs`, signing it in, and which route each
 * shot opens.
 *
 * The dashboard reaches its scheduler through exactly one place
 * (`src/api/client.ts`: `fetch(`${baseUrl}/admin/api${path}`)` with a bearer
 * token from localStorage), so standing in for a whole cluster is one route
 * handler plus one seeded token. The bundle runs unmodified.
 *
 * Flags: --no-build, --only=a,b, --out=<dir>, --bare
 */
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { run } from '../../../tools/screenshots/engine.mjs';
import { ROUTES, STREAMS, TOKEN, NOW } from './fixtures.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const APP = resolve(HERE, '..');
const DIST = join(APP, 'dist');

/**
 * The origin the browser thinks it is on.
 *
 * The dashboard shows its own address back to the operator (the MCP endpoint on
 * the Access page is `location.origin + '/admin/api/mcp'`), so capturing it on
 * `127.0.0.1:<random>` both looked wrong and made that screenshot change on
 * every run. Everything is served through Playwright's router anyway, so the
 * page may as well be on the address the frame's URL bar claims.
 */
const ORIGIN = 'https://acme-admin.spky.cloud';

/** Requests the fixtures do not answer, reported once at the end. */
const missing = new Set();
process.on('exit', () => {
  if (missing.size === 0) return;
  console.error('\nUnanswered API requests (add them to fixtures.mjs):');
  for (const key of [...missing].sort()) console.error(`  ${key}`);
});

const SHOTS = [
  {
    name: 'overview',
    route: '/',
    height: 1040,
    caption: 'The overview: cluster health, sync latency, presence and activity at a glance.',
  },
  {
    name: 'ssps',
    route: '/ssps',
    height: 560,
    caption: 'Every SSP with its status, view count, lag and publication backlog.',
  },
  {
    name: 'views',
    route: '/views',
    height: 810,
    caption: 'The registered views, who subscribes to them and what they cost.',
  },
  {
    name: 'jobs',
    route: '/jobs',
    height: 850,
    caption: 'The outbox: queued, running and failed jobs, with kill and retry.',
  },
  {
    name: 'schedules',
    route: '/schedules',
    height: 470,
    caption: 'Cron schedules, their next run, and pause, resume or trigger.',
  },
  {
    name: 'workflows',
    route: '/workflows',
    height: 620,
    caption: 'Workflow runs as they happen, with cancel, rerun and retry.',
  },
  {
    name: 'incidents',
    route: '/incidents',
    height: 790,
    caption: 'Lag, heartbeat failures, restarts and recoveries as one timeline.',
  },
  {
    name: 'backends',
    route: '/backends',
    height: 560,
    caption: 'Your own services, their health checks and response times.',
  },
  {
    name: 'backups',
    route: '/backups',
    height: 860,
    caption: 'The backup catalog, its schedule, and restoring from a snapshot.',
  },
  {
    name: 'logs',
    route: '/logs',
    height: 620,
    caption: 'Live logs from the scheduler, an SSP or one of your backends.',
  },
  {
    name: 'access',
    route: '/access',
    height: 900,
    caption: 'Who may sign in, and the long-lived tokens issued for MCP.',
  },
];

run({
  name: 'the dashboard',
  generatedBy: 'apps/dashboard/screenshots/capture.mjs',
  root: APP,
  dist: DIST,
  distEntry: 'index.html',
  out: join(APP, '../landing-page/public/docs/admin'),

  width: 1440,
  scale: 2,
  defaultHeight: 800,
  settleMs: 700,

  // The dashboard is a whole page, so it gets a plain browser window: no
  // DevTools strip, no sliver of anything else.
  frame: {
    devtools: false,
    title: 'Sp00ky Admin',
    url: ORIGIN.replace(/^https?:\/\//, ''),
    chromeHeight: 78,
    barHeight: 0,
  },

  // Vite builds with `base: '/admin/'` because a scheduler serves the bundle
  // there. Assets keep their path; every other `/admin/*` URL is a client route
  // and gets index.html, which is the SPA fallback the scheduler also does.
  rewrite: (path) => {
    if (path.startsWith('/admin/assets/')) return path.slice('/admin'.length);
    if (path === '/admin' || path.startsWith('/admin/')) return '/index.html';
    return path === '/' ? '/index.html' : path;
  },

  async prepare({ context, origin }) {
    // Everything under the public origin comes from the local static server.
    // Registered first so the API route below, added later, wins for /admin/api.
    await context.route(`${ORIGIN}/**`, async (route) => {
      const path = new URL(route.request().url()).pathname;
      await route.fulfill({ response: await route.fetch({ url: `${origin}${path}` }) });
    });

    // A token keyed by the empty base URL: embedded is same-origin, which is
    // what a scheduler serving /admin looks like.
    await context.addInitScript(
      ([token, now]) => {
        try {
          localStorage.setItem('spky.token:', token);
        } catch {
          /* ignore */
        }
        // Freeze the clock so every "2m ago" is stable between runs.
        const RealDate = Date;
        Date.now = () => now;
        // eslint-disable-next-line no-global-assign
        globalThis.Date = class extends RealDate {
          constructor(...a) {
            super(...(a.length ? a : [now]));
          }
          static now() {
            return now;
          }
        };
      },
      [TOKEN, NOW]
    );

    await context.route('**/admin/api/**', async (route) => {
      const url = new URL(route.request().url());
      const path = url.pathname.replace(/^.*\/admin\/api/, '') || '/';
      const key = `${route.request().method()} ${path}`;

      // The jobs, workflows and logs screens read Server-Sent Events rather
      // than polling. `client.ts` reads the stream with fetch + a reader, so a
      // body of frames delivered in one go paints exactly what a live stream's
      // first frames would; the screens then show "disconnected", which is why
      // each one is captured before that matters.
      const stream = STREAMS[key];
      if (stream !== undefined) {
        await route.fulfill({ contentType: 'text/event-stream', body: stream });
        return;
      }

      const body = ROUTES[key];
      if (body === undefined) {
        missing.add(key);
        await route.fulfill({ status: 404, json: { error: `no fixture for ${key}` } });
        return;
      }
      await route.fulfill({ json: body });
    });
  },

  async open({ page, shot }) {
    await page.goto(`${ORIGIN}/admin${shot.route === '/' ? '' : shot.route}`, {
      waitUntil: 'networkidle',
    });
    await page.waitForSelector('.shell, .login-wrap');
  },

  shots: SHOTS,
});
