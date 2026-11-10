import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  external: [
    'surrealdb',
    '@surrealdb/wasm',
    'react',
    'react/jsx-runtime',
    '@spooky-sync/core',
    '@spooky-sync/query-builder',
  ],
  clean: true,
  hash: false,
  sourcemap: true,
  target: 'es2020',
  // Every export is a hook or a client component: mark the bundle so a
  // Next.js App Router server component can render <Sp00kyProvider> directly.
  outputOptions: {
    banner: "'use client';",
  },
});
