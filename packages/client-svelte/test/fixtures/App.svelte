<script lang="ts">
  import { Sp00kyProvider, type SyncedDb } from '../../src/index.js';
  import Child from './Child.svelte';

  let {
    eager,
    preload,
    onReady,
    onError,
  }: {
    eager?: boolean;
    preload?: (db: SyncedDb<any>) => Promise<void>;
    onReady?: (db: SyncedDb<any>) => void;
    onError?: (error: Error) => void;
  } = $props();
</script>

<Sp00kyProvider config={{} as any} {eager} {preload} {onReady} {onError}>
  {#snippet fallback()}<p class="fallback">loading</p>{/snippet}
  <Child />
</Sp00kyProvider>
