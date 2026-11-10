import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/vite-plugin-svelte').SvelteConfig} */
export default {
  // Strips TypeScript from `<script lang="ts">` when `svelte-package` emits
  // the published `.svelte` files; runes are left for the app's compiler.
  preprocess: vitePreprocess({ script: true }),
};
