import { defineConfig } from 'vite';
import solid from 'vite-plugin-solid';

// `--mode console` builds the Sp00ky Cloud console (console.sp00ky.cloud): the
// same app at the root of its own host, with the cloud's workspace and project
// chrome. The default build is the scheduler-hosted dashboard under /admin.
export default defineConfig(({ mode }) => ({
  plugins: [solid()],
  // The scheduler serves this bundle under /admin, so every asset URL has to
  // be prefixed. Relative would also work embedded but breaks the SPA fallback
  // for a deep link like /admin/workflows/abc, where the browser resolves
  // assets against /admin/workflows/.
  base: mode === 'console' ? '/' : '/admin/',
  build: {
    target: 'es2020',
    outDir: mode === 'console' ? 'dist-console' : 'dist',
    emptyOutDir: true,
    // One chunk. The dashboard is small and is served from a scheduler that
    // may be behind a slow link; a single request beats a waterfall.
    chunkSizeWarningLimit: 1200,
  },
  server: {
    port: mode === 'console' ? 4400 : 4300,
  },
}));
