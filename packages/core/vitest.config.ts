import { defineConfig } from 'vitest/config';

// Thresholds start at the numbers the suite hits today and are ratcheted per
// directory as the saga core lands (see the plan's commit 10). Exclusions:
//   *.fixture.ts / src/testing/**   test-only harness code
//   sqlite-worker.ts                runs inside a Worker (sqlite-wasm + OPFS +
//                                   postMessage globals); covered by the sqlite
//                                   integration tests and the build check
//   client/services.ts              constructs the real adapters (SurrealDB
//                                   WASM store, socket, SharedWorker broker,
//                                   OPFS blob cache); exercised by the example
//                                   app and the WhitePawn A/B, not by vitest
export default defineConfig({
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts'],
    coverage: {
      provider: 'v8',
      all: true,
      reporter: ['text-summary', 'lcov'],
      include: ['src/**/*.ts'],
      exclude: [
        'src/**/*.test.ts',
        'src/**/*.d.ts',
        'src/**/*.fixture.ts',
        'src/testing/**',
        'src/services/database/sqlite-worker.ts',
        'src/client/services.ts',
      ],
      thresholds: {
        // Global numbers are held back by the legacy adapters (devtools,
        // sqlite engine, tabs broker, blobs, crdt); the saga core below is 100%.
        lines: 75,
        functions: 82,
        branches: 87,
        statements: 75,
        'src/kernel/**': { lines: 100, functions: 100, branches: 100, statements: 100 },
        'src/state/**': { lines: 100, functions: 100, branches: 100, statements: 100 },
        'src/query/**': { lines: 100, functions: 100, branches: 100, statements: 100 },
        'src/mutation/**': { lines: 100, functions: 100, branches: 100, statements: 100 },
        'src/sync/**': { lines: 100, functions: 100, branches: 100, statements: 100 },
        'src/boot/**': { lines: 100, functions: 100, branches: 100, statements: 100 },
        // The facade's shared-tabs host callbacks need a SharedWorker broker to fire.
        'src/client/**': { lines: 93, functions: 92, branches: 89, statements: 93 },
      },
    },
  },
});
