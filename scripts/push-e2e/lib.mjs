// Shared setup for the Web Push browser checks (see README.md).
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export const HERE = path.dirname(new URL(import.meta.url).pathname);
export const ROOT = path.resolve(HERE, '../..');
export const WEB = path.join(HERE, 'web');
export const PROJECT = path.join(HERE, 'project');
export const SPKY = path.join(ROOT, 'target/debug/spky');
export const PORT = Number(process.env.PUSH_E2E_PORT ?? 5391);
export const DB = process.env.PUSH_E2E_DB ?? 'ws://localhost:8666/rpc';
export const NS = { namespace: 'main', database: 'main' };

export const { chromium } = createRequire(path.join(ROOT, 'example/e2e/package.json'))('@playwright/test');
const sdk = await import(createRequire(path.join(ROOT, 'package.json')).resolve('surrealdb'));
export const Surreal = sdk.Surreal ?? sdk.default?.Surreal ?? sdk.default;

export const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);

/** The built `@spooky-sync/core` dist next to the pages, and the live worker bundled. */
export function prepareWeb() {
  const dist = path.join(ROOT, 'packages/core/dist');
  if (!fs.existsSync(path.join(dist, 'sw.js'))) throw new Error('build packages/core first (pnpm --filter @spooky-sync/core build)');
  const link = path.join(WEB, 'core');
  fs.rmSync(link, { force: true, recursive: true });
  fs.symlinkSync(dist, link);
  // The live worker imports the `surrealdb` SDK, which a service worker cannot
  // resolve as a bare specifier: bundle it.
  const pnpm = path.join(ROOT, 'node_modules/.pnpm');
  const esbuildDir = fs.readdirSync(pnpm).filter((d) => d.startsWith('esbuild@')).sort().pop();
  const esbuild = path.join(pnpm, esbuildDir, 'node_modules/esbuild/bin/esbuild');
  execFileSync(esbuild, ['sw.js', '--bundle', '--format=esm', '--outfile=sw.bundle.js', '--platform=browser', '--log-level=error'], { cwd: path.join(WEB, 'live') });
}

export function serve() {
  const types = { '.html': 'text/html', '.js': 'text/javascript' };
  return http.createServer((q, r) => {
    let p = new URL(q.url, 'http://x').pathname;
    if (p.endsWith('/')) p += 'index.html';
    const f = path.join(WEB, decodeURIComponent(p));
    fs.readFile(f, (err, buf) => {
      if (err) { r.writeHead(404); r.end(); return; }
      r.writeHead(200, { 'content-type': types[path.extname(f)] || 'application/octet-stream' });
      r.end(buf);
    });
  }).listen(PORT);
}

/** Chrome refuses the Push API in incognito, and a Playwright context is one: use a real profile. */
export async function browser() {
  const profile = path.join(os.tmpdir(), 'sp00ky-push-e2e-profile');
  const ctx = await chromium.launchPersistentContext(profile, { channel: 'chrome', headless: !process.env.HEADFUL });
  await ctx.grantPermissions(['notifications'], { origin: `http://localhost:${PORT}` });
  return ctx;
}

export async function user(prefix) {
  const db = new Surreal();
  await db.connect(DB, NS);
  const name = prefix + '_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  const t = await db.signup({ ...NS, access: 'account', variables: { username: name, password: 'pw-' + name } });
  const [me] = await db.query('RETURN $auth.id');
  return { db, name, id: String(me), token: typeof t === 'string' ? t : (t.access ?? t.token) };
}
