// Bundles `collections.adapter.ts` (Solid + TanStack DB behind extensionless
// imports node cannot resolve itself) into one ESM file for `node --test`.
//
// Rollup, through Vite, not esbuild: `@tanstack/db` has module cycles that
// esbuild breaks with lazy init, which leaves classes undefined at first use.
// Solid's browser build, because its server build has no reactivity; nothing
// here needs a DOM.
import { build } from 'vite';
import { fileURLToPath } from 'node:url';

const here = (p) => fileURLToPath(new URL(p, import.meta.url));

await build({
  configFile: false,
  logLevel: 'warn',
  resolve: { conditions: ['browser'] },
  ssr: { noExternal: true, resolve: { conditions: ['browser'], externalConditions: ['browser'] } },
  build: {
    ssr: here('./collections.adapter.ts'),
    // `dist/` is ignored repo-wide; node's runner skips node_modules.
    outDir: here('./dist'),
    emptyOutDir: false,
    target: 'node22',
    rollupOptions: { output: { format: 'es', entryFileNames: 'collections.adapter.test.mjs' } },
  },
});
