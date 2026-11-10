<script lang="ts">
  import { untrack } from 'svelte';
  import { BucketImage, setDbContext, type SyncedDb } from '../../src/index.js';

  let { db, path }: { db: SyncedDb<any>; path: string | null } = $props();
  setDbContext(untrack(() => db));
</script>

<BucketImage bucket="covers" {path} alt="cover" class="frame">
  {#snippet fallback()}<span class="plate"></span>{/snippet}
</BucketImage>
