import { defineConfig } from 'tsdown';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

// Resolve real package versions at build time so the DevTools can report the
// actual bundled frontend versions (used to detect frontend/backend drift).
const corePkg = require('./package.json');
const coreVersion: string = corePkg.version;
// ssp-wasm has no `exports` map; resolve its package.json directly, falling
// back to the relative workspace path if module resolution can't find it.
let wasmVersion: string;
try {
  wasmVersion = require('@spooky-sync/ssp-wasm/package.json').version;
} catch {
  wasmVersion = require('../ssp-wasm/package.json').version;
}
// Report the SurrealDB WASM ENGINE version (`@surrealdb/wasm`) — the engine the
// in-browser local DB actually runs — NOT the `surrealdb` JS client library
// version. The WASM engine is the one that must stay compatible with the server
// engine, so it's the meaningful number to compare in DevTools. Prefer the
// actually-installed version; fall back to our pinned range if its `exports`
// map blocks importing its package.json.
let surrealVersion: string;
try {
  surrealVersion = require('@surrealdb/wasm/package.json').version;
} catch {
  surrealVersion = String(corePkg.dependencies?.['@surrealdb/wasm'] ?? 'unknown').replace(
    /^[\^~]/,
    ''
  );
}

// tsdown 0.12.x forwards a top-level `define` to rolldown's inputOptions, which
// rejects it — so we do the substitution ourselves with a tiny transform
// plugin. Only our own modules reference the `__SP00KY_*__` identifiers, and
// the early `includes` check skips everything else.
const replacements: Record<string, string> = {
  __SP00KY_CORE_VERSION__: JSON.stringify(coreVersion),
  __SP00KY_WASM_VERSION__: JSON.stringify(wasmVersion),
  __SP00KY_SURREAL_VERSION__: JSON.stringify(surrealVersion),
};

const versionDefinePlugin = {
  name: 'sp00ky-version-define',
  transform(code: string) {
    if (!code.includes('__SP00KY_')) return null;
    let out = code;
    for (const [token, value] of Object.entries(replacements)) {
      out = out.split(token).join(value);
    }
    return out;
  },
};

// Worker URLs are written as `./<name>.ts` in source so Vite's src-bundling
// consumers (the example app aliases core to `src`) resolve them. In the
// published build each worker is emitted at `dist/<name>.js`, so rewrite the
// references to `.js` here — otherwise the flat `dist/index.js` carries
// dangling `.ts` URLs that a consumer's bundler can't resolve.
const workerUrlPlugin = {
  name: 'sp00ky-worker-url',
  transform(code: string) {
    if (
      !code.includes('sqlite-worker.ts') &&
      !code.includes('rows-worker.ts') &&
      !code.includes('tabs-broker-worker.ts')
    ) {
      return null;
    }
    return code
      .split('sqlite-worker.ts')
      .join('sqlite-worker.js')
      .split('rows-worker.ts')
      .join('rows-worker.js')
      .split('tabs-broker-worker.ts')
      .join('tabs-broker-worker.js');
  },
};

// V8 compiles a function the first time it is called, on the thread calling
// it. `//# allFunctionsCalledOnLoad` as a file's FIRST line (Chrome 136+) has
// the whole file compiled while it loads, off that thread, and kept in the
// code cache. Only for the row store's worker: a small file of which every
// function runs within its first requests, and which a page is waiting for.
// Measured: 0.6 to 1.5 ms off the worker's first `open`. An app's bundler
// that minifies the worker drops the comment; the docs say how to keep it.
const COMPILE_HINT = '//# allFunctionsCalledOnLoad\n';
const compileHintPlugin = {
  name: 'sp00ky-compile-hint',
  generateBundle(_options: unknown, bundle: Record<string, { type: string; code?: string }>) {
    for (const [name, file] of Object.entries(bundle)) {
      if (name !== 'rows-worker.js') continue;
      if (file.type === 'chunk' && typeof file.code === 'string' && !file.code.startsWith(COMPILE_HINT)) file.code = COMPILE_HINT + file.code;
    }
  },
};

export default defineConfig([
  {
  // `sqlite-worker` is emitted at `dist/sqlite-worker.js` (top level, NOT under
  // services/database) so the `new URL('./sqlite-worker.js', import.meta.url)`
  // in `SqliteCacheEngine` — which gets bundled into the flat `dist/index.js` —
  // resolves correctly. It (and `@sqlite.org/sqlite-wasm`) load lazily in the
  // Worker; never pulled into the main bundle unless `localEngine: 'sqlite'`.
  entry: {
    index: 'src/index.ts',
    'otel/index': 'src/otel/index.ts',
    'sqlite-worker': 'src/services/database/sqlite-worker.ts',
    // The shared-tabs broker MUST stay a self-contained emit (no chunk
    // imports): bundlers cannot trace `new SharedWorker(url)` graphs, so a
    // relative chunk import would 404 in consumers. The source keeps zero
    // runtime imports and scripts/check-broker-bundle.mjs enforces it.
    'tabs-broker-worker': 'src/services/tabs/tabs-broker-worker.ts',
  },
  format: ['esm'],
  dts: true,
  clean: true,
  hash: false,
  plugins: [versionDefinePlugin, workerUrlPlugin],
  },
  // The wasm-free entries a service worker, a dedicated worker or Node can
  // import: `/pure` (protocol builders), `/live` (the live feed over the
  // pure-JS `surrealdb` SDK) and `/sw` (push handlers). Built separately so
  // they never share a chunk with `index` (whose graph holds the wasm engines)
  // and so `dist/index.js` stays one flat file. They may share chunks among
  // themselves; scripts/check-worker-entries.mjs walks those too and fails
  // the build on any wasm/pino import or top-level window/document access.
  {
    entry: {
      pure: 'src/pure.ts',
      live: 'src/live/index.ts',
      sw: 'src/sw/index.ts',
    },
    format: ['esm'],
    dts: true,
    clean: false,
    hash: false,
    // Own prefix so a chunk of this build never overwrites one of the first
    // (both write `dist/`; `types.d.ts` collided).
    // The entries' own declaration files arrive as chunks named `<entry>.d`
    // and must keep their plain name.
    outputOptions: {
      chunkFileNames: (chunk: { name: string }) =>
        ['pure.d', 'live.d', 'sw.d'].includes(chunk.name) ? '[name].js' : 'worker-[name].js',
    },
    plugins: [versionDefinePlugin],
  },
  // The row store's worker (`localEngine: 'rows'`), at `dist/rows-worker.js`
  // like the SQLite one. A build of its own so the file holds everything the
  // worker runs and nothing else: the compile hint on its first line then
  // covers the store and its queries, and no page ever loads hinted code.
  {
    entry: { 'rows-worker': 'src/services/database/rows-worker.ts' },
    format: ['esm'],
    dts: true,
    clean: false,
    hash: false,
    outputOptions: {
      chunkFileNames: (chunk: { name: string }) => (chunk.name === 'rows-worker.d' ? '[name].js' : 'rows-worker-[name].js'),
    },
    plugins: [versionDefinePlugin, compileHintPlugin],
  },
]);
