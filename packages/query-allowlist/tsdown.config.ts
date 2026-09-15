import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: ['src/index.ts', 'src/cli.ts'],
  format: ['esm'],
  dts: true,
  external: ['jiti', '@spooky-sync/query-builder'],
  clean: true,
  hash: false,
  sourcemap: true,
});
