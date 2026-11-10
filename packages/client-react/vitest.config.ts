import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Hooks are rendered with react-dom into a real (happy-dom) document, so
    // effects, refs and useSyncExternalStore run exactly as in a browser.
    environment: 'happy-dom',
    include: ['src/**/*.test.{ts,tsx}'],
  },
});
