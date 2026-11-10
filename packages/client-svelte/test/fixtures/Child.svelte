<script lang="ts">
  import { useDb } from '../../src/index.js';

  const db = useDb();
</script>

<p class="child">{db.constructor.name}</p>
