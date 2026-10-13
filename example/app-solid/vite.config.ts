import { defineConfig } from 'vite';
import solid from 'vite-plugin-solid';
import wasm from 'vite-plugin-wasm';
import topLevelAwait from 'vite-plugin-top-level-await';
import { VitePWA } from 'vite-plugin-pwa';
import path from 'path';
import { createRequire } from 'module';

// This app resolves @spooky-sync/core from SOURCE (see resolve.alias below), so
// core's own tsdown `define` for these version globals never runs. Bake them
// here too — otherwise the DevTools/version panel reports 'unknown'. Mirrors
// packages/core/tsdown.config.ts.
const require = createRequire(import.meta.url);
const coreVersion: string = require('../../packages/core/package.json').version;
let wasmVersion = coreVersion;
try {
  wasmVersion = require('@spooky-sync/ssp-wasm/package.json').version;
} catch {
  try {
    wasmVersion = require('../../packages/ssp-wasm/package.json').version;
  } catch {
    /* keep fallback */
  }
}
let surrealVersion = 'unknown';
try {
  surrealVersion = require('@surrealdb/wasm/package.json').version;
} catch {
  /* keep fallback */
}

export default defineConfig({
  define: {
    __SP00KY_CORE_VERSION__: JSON.stringify(coreVersion),
    __SP00KY_WASM_VERSION__: JSON.stringify(wasmVersion),
    __SP00KY_SURREAL_VERSION__: JSON.stringify(surrealVersion),
  },
  plugins: [
    solid(),
    wasm(),
    topLevelAwait(),
    VitePWA({
      registerType: 'autoUpdate',
      includeAssets: ['favicon.png', 'apple-touch-icon.png'],
      manifest: {
        name: 'sp00ky threads',
        short_name: 'sp00ky threads',
        description: 'The live demo app for sp00ky: local-first, real-time sync for SurrealDB',
        theme_color: '#09090B',
        background_color: '#09090B',
        display: 'standalone',
        start_url: '/',
        scope: '/',
        icons: [
          { src: '/icon-192.png', sizes: '192x192', type: 'image/png' },
          { src: '/icon-512.png', sizes: '512x512', type: 'image/png' },
          { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' },
        ],
      },
      workbox: {
        // The SurrealDB WASM bundle is ~6 MB; raise the precache size cap.
        maximumFileSizeToCacheInBytes: 16 * 1024 * 1024,
        globPatterns: ['**/*.{js,css,html,svg,png,ico,wasm,woff,woff2}'],
        navigateFallback: '/index.html',
        runtimeCaching: [
          {
            urlPattern: /^https:\/\/fonts\.googleapis\.com\/.*/i,
            handler: 'CacheFirst',
            options: {
              cacheName: 'google-fonts-stylesheets',
              expiration: { maxEntries: 10, maxAgeSeconds: 60 * 60 * 24 * 365 },
            },
          },
          {
            urlPattern: /^https:\/\/fonts\.gstatic\.com\/.*/i,
            handler: 'CacheFirst',
            options: {
              cacheName: 'google-fonts-webfonts',
              expiration: { maxEntries: 30, maxAgeSeconds: 60 * 60 * 24 * 365 },
              cacheableResponse: { statuses: [0, 200] },
            },
          },
        ],
      },
      devOptions: {
        enabled: false,
      },
    }),
  ],
  resolve: {
    alias: {
      '@spooky-sync/client-solid': path.resolve(__dirname, '../../packages/client-solid/src/index.ts'),
      '@spooky-sync/core/otel': path.resolve(__dirname, '../../packages/core/src/otel/index.ts'),
      '@spooky-sync/core': path.resolve(__dirname, '../../packages/core/src/index.ts'),
      '@spooky-sync/query-builder': path.resolve(__dirname, '../../packages/query-builder/src/index.ts'),
    },
  },
  server: {
    port: 3006,
    proxy: {
      '/v1/logs': {
        target: 'http://localhost:4318',
        changeOrigin: true,
        secure: false,
      },
    },
  },
  build: {
    target: 'esnext',
  },
  optimizeDeps: {
    // @sqlite.org/sqlite-wasm: esbuild pre-bundling breaks the module's own
    // `sqlite3.wasm` URL lookup in dev (the fetch returns index.html and wasm
    // instantiation dies on "expected magic word"). Serve it raw.
    exclude: ['@surrealdb/wasm', '@sqlite.org/sqlite-wasm'],
    esbuildOptions: {
      target: 'esnext',
    },
  },
  esbuild: {
    supported: {
      'top-level-await': true,
    },
  },
});
