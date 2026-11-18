import { defineConfig } from 'vite';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
// A file without imports, made to be loaded by a build script like this one.
import { ROWS_PREWARM_SNIPPET } from '../../packages/core/src/services/database/rows-prewarm';

const here = path.dirname(fileURLToPath(import.meta.url));

// The benchmark drives the engines from core's SOURCE (as the example app
// does), so the workers it measures are the ones in this checkout.
// V8 compiles a function the first time it is called, on the thread that
// calls it. `//# allFunctionsCalledOnLoad` as a file's first line (Chrome 136+)
// asks for the whole file to be compiled while it loads, off that thread. A
// minifier drops the comment, so it goes back on after one has run.
// BENCH_COMPILE_HINT=0 builds without, to measure what it buys.
const COMPILE_HINT = '//# allFunctionsCalledOnLoad\n';
const compileHint = {
  name: 'sp00ky-compile-hint',
  generateBundle(_options: unknown, bundle: Record<string, { type: string; code?: string }>) {
    if (process.env.BENCH_COMPILE_HINT === '0') return;
    for (const file of Object.values(bundle)) {
      if (file.type === 'chunk' && typeof file.code === 'string' && !file.code.startsWith(COMPILE_HINT)) file.code = COMPILE_HINT + file.code;
    }
  },
};

// For the page's "starting the worker" test: a copy of the row worker without
// the hint beside the built one, and a file saying where both are.
const workerCopies = {
  name: 'bench-worker-copies',
  generateBundle(this: { emitFile(file: { type: 'asset'; fileName: string; source: string }): void }, _options: unknown, bundle: Record<string, { type: string; fileName: string; source?: string | Uint8Array; code?: string }>) {
    const worker = Object.values(bundle).find((file) => /rows-worker-[\w-]+\.js$/.test(file.fileName));
    if (!worker) return;
    const raw = worker.type === 'asset' ? worker.source : worker.code;
    const code = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
    const plain = code.startsWith(COMPILE_HINT) ? code.slice(COMPILE_HINT.length) : code;
    this.emitFile({ type: 'asset', fileName: 'assets/rows-worker-plain.js', source: plain });
    this.emitFile({ type: 'asset', fileName: 'assets/workers.json', source: JSON.stringify({ hint: `/${worker.fileName}`, plain: '/assets/rows-worker-plain.js' }) });
  },
};

export default defineConfig({
  plugins: [
    workerCopies,
    {
      // The row store's head script, as an app would add it (see the client config docs).
      name: 'sp00ky-prewarm',
      transformIndexHtml: () => [{ tag: 'script', children: ROWS_PREWARM_SNIPPET, injectTo: 'head-prepend' }],
    },
  ],
  resolve: {
    alias: [
      { find: '@core', replacement: path.resolve(here, '../../packages/core/src') },
      // `local.ts` reaches the SurrealDB wasm engine through a dynamic import
      // nothing here ever runs. Keep its 6 MB out of the build.
      { find: /^@surrealdb\/wasm$/, replacement: path.resolve(here, 'src/empty.ts') },
      // One copy of the SDK, core's: the codec comparison uses its CBOR codec.
      { find: /^surrealdb$/, replacement: path.resolve(here, '../../packages/core/node_modules/surrealdb/dist/surrealdb.mjs') },
    ],
  },
  server: { port: 4410, strictPort: true, fs: { allow: [path.resolve(here, '../..')] } },
  preview: { port: 4410, strictPort: true },
  worker: { format: 'es', plugins: () => [compileHint] },
  optimizeDeps: { exclude: ['@sqlite.org/sqlite-wasm'] },
  build: { target: 'esnext', sourcemap: false },
});
