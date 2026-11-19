<script lang="ts">
  import { useDb } from '../../src/index.js';

  const db = useDb();
</script>

<p class="child" data-ready={String(db.isReady)}>{db.constructor.name}</p>
