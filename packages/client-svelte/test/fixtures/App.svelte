<script lang="ts">
  import { Sp00kyProvider, type SyncedDb } from '../../src/index.js';
  import Child from './Child.svelte';

  let {
    preload,
    onReady,
  }: { preload?: (db: SyncedDb<any>) => Promise<void>; onReady?: (db: SyncedDb<any>) => void } =
    $props();
</script>

<Sp00kyProvider config={{} as any} {preload} {onReady}>
  {#snippet fallback()}<p class="fallback">loading</p>{/snippet}
  <Child />
</Sp00kyProvider>
