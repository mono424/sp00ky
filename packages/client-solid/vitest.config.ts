import { defineConfig } from 'vitest/config';

export default defineConfig({
  // solid-js's `node` export condition resolves the SSR build, where effects
  // never run. Force the browser dev build so tests see client reactivity.
  resolve: {
    conditions: ['browser', 'development'],
  },
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts'],
  },
});
