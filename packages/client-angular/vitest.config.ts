import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

// Tests run against the ng-packagr build (`pnpm test` builds first), the
// partially compiled output an app actually installs. Angular's JIT compiler
// links its declarations at runtime, signal inputs included, which source
// files compiled by esbuild alone would not support.
export default defineConfig({
  resolve: {
    alias: {
      '@spooky-sync/client-angular': fileURLToPath(
        new URL('./dist/fesm2022/spooky-sync-client-angular.mjs', import.meta.url)
      ),
    },
  },
  test: {
    environment: 'happy-dom',
    include: ['test/**/*.test.ts'],
    setupFiles: ['test/setup.ts'],
  },
});
