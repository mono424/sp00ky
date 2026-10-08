/**
 * Serve the built panel against the screenshot fixture, for a browser.
 *
 *   pnpm --filter @spooky-sync/devtools build
 *   node apps/devtools/screenshots/serve.mjs          # http://localhost:4320/
 *
 * The same stand-in page `capture.mjs` photographs, without Playwright:
 * `dist/panel.html` with `fixture.js` injected ahead of the bundle, so the
 * panel runs with no extension, no backend and no app. The fixture pins the
 * clock, exactly as in the screenshots.
 */
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { dirname, extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const DIST = resolve(HERE, '..', 'dist');
const PORT = Number(process.env.PORT ?? 4320);

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
};

createServer(async (req, res) => {
  const path = new URL(req.url ?? '/', 'http://localhost').pathname;
  try {
    if (path === '/' || path === '/panel.html') {
      const html = await readFile(join(DIST, 'panel.html'), 'utf-8');
      res.writeHead(200, { 'content-type': TYPES['.html'] });
      res.end(html.replace(/<script\b/, '<script src="/__fixture__.js"></script>\n    <script'));
      return;
    }
    if (path === '/__fixture__.js') {
      res.writeHead(200, { 'content-type': TYPES['.js'] });
      res.end(await readFile(join(HERE, 'fixture.js')));
      return;
    }
    const file = join(DIST, normalize(path));
    if (!file.startsWith(DIST)) throw new Error('outside dist');
    const body = await readFile(file);
    res.writeHead(200, { 'content-type': TYPES[extname(file)] ?? 'application/octet-stream' });
    res.end(body);
  } catch {
    res.writeHead(404);
    res.end('not found (run `pnpm --filter @spooky-sync/devtools build` first)');
  }
}).listen(PORT, () => console.log(`devtools panel fixture on http://localhost:${PORT}/`));
