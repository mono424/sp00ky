/**
 * Serve the built dashboard against the screenshot fixtures, for a browser.
 *
 *   pnpm --filter @spooky-sync/dashboard build
 *   node apps/dashboard/screenshots/serve.mjs          # http://localhost:4310/admin/
 *
 * The same stand-in cluster `capture.mjs` photographs, without Playwright: the
 * bundle from `dist/` at `/admin/`, and every `/admin/api/*` answered from
 * `fixtures.mjs`. The page signs itself in with the fixture token on first
 * load. Unlike the screenshots the clock is not frozen, so "ago" stamps drift.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ROUTES, STREAMS, TOKEN } from './fixtures.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const DIST = resolve(HERE, '..', 'dist');
const PORT = Number(process.env.PORT ?? 4310);

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.svg': 'image/svg+xml',
  '.woff2': 'font/woff2',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

// Seeds the fixture token (keyed by the empty base URL, i.e. same-origin, as a
// scheduler serving /admin looks) before the app reads it.
const SEED = `<script>try{localStorage.setItem('spky.token:',${JSON.stringify(TOKEN)})}catch{}</script>`;

async function sendFile(res, path) {
  const body = await readFile(path);
  const type = TYPES[extname(path)] ?? 'application/octet-stream';
  res.writeHead(200, { 'content-type': type });
  res.end(type.startsWith('text/html') ? body.toString().replace('<head>', `<head>${SEED}`) : body);
}

createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost');
  if (url.pathname.startsWith('/admin/api')) {
    const key = `${req.method} ${url.pathname.replace(/^\/admin\/api/, '') || '/'}`;
    if (STREAMS[key] !== undefined) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.end(STREAMS[key]);
      return;
    }
    const body = ROUTES[key];
    res.writeHead(body === undefined ? 404 : 200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body ?? { error: `no fixture for ${key}` }));
    return;
  }
  if (!url.pathname.startsWith('/admin')) {
    res.writeHead(302, { location: '/admin/' });
    res.end();
    return;
  }
  const rel = normalize(url.pathname.replace(/^\/admin/, '') || '/');
  const file = join(DIST, rel);
  try {
    if (!file.startsWith(DIST) || rel.endsWith('/')) throw new Error('index');
    await sendFile(res, file);
  } catch {
    // Client-side routes fall back to the app shell, like the scheduler does.
    await sendFile(res, join(DIST, 'index.html')).catch(() => {
      res.writeHead(500);
      res.end('dist/ is missing: run `pnpm --filter @spooky-sync/dashboard build` first');
    });
  }
}).listen(PORT, () => console.log(`dashboard fixtures on http://localhost:${PORT}/admin/`));
