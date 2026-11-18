import { defineConfig } from 'vite';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
// A file without imports, made to be loaded by a build script like this one.
import { ROWS_PREWARM_SNIPPET } from '../../packages/core/src/services/database/rows-prewarm';

const here = path.dirname(fileURLToPath(import.meta.url));

// The benchmark drives the engines from core's SOURCE (as the example app
// does), so the workers it measures are the ones in this checkout.
export default defineConfig({
  plugins: [
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
  worker: { format: 'es' },
  optimizeDeps: { exclude: ['@sqlite.org/sqlite-wasm'] },
  build: { target: 'esnext', sourcemap: false },
});
