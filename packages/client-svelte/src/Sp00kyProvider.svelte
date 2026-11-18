<script lang="ts" generics="S extends SchemaStructure">
  import { onMount, untrack } from 'svelte';
  import type { SchemaStructure } from '@spooky-sync/query-builder';
  import { SyncedDb } from './db.js';
  import { setDbContext } from './context.js';
  import type { Sp00kyProviderProps } from './types.js';

  let { config, fallback, eager, onError, onReady, preload, children }: Sp00kyProviderProps<S> =
    $props();

  // Eager: the instance the tree mounts on, in context before the children
  // initialise. Building it opens nothing (its core client comes with its first
  // use, `init()` still runs in `onMount`). Read once, like `config`; never on
  // the server, where the client cannot run.
  const early = untrack(() =>
    eager && typeof window !== 'undefined' ? new SyncedDb<S>(config, { eager: true }) : undefined
  );

  let db = $state.raw<SyncedDb<S> | undefined>(early);

  setDbContext({
    get db() {
      return db;
    },
  });

  // `onMount` never runs during SSR, so a server render only produces the
  // fallback and the client (WebAssembly, IndexedDB, workers) is browser-only.
  //
  // Init is async, so the provider can be destroyed mid-init: only that race
  // closes the client, because nothing will ever reference it. A client that
  // was handed to the tree is deliberately NOT closed on destroy: that nulls
  // its core, so every later write throws "SyncedDb not initialized" while
  // reads keep rendering, i.e. mutations die silently. The provider wraps the
  // whole app and goes with the page, where the browser reclaims it anyway.
  // An eager instance is in the tree's hands before it is initialized, so it
  // is never closed either.
  onMount(() => {
    let destroyed = false;
    void (async () => {
      try {
        const instance = early ?? new SyncedDb<S>(config);
        await instance.init();
        if (destroyed) {
          if (!early) await instance.close();
          return;
        }
        if (preload) {
          try {
            await preload(instance);
          } catch (e) {
            // oxlint-disable-next-line no-console
            console.error('Sp00kyProvider: preload failed; revealing UI anyway', e);
          }
        }
        if (destroyed) {
          if (!early) await instance.close();
          return;
        }
        db = instance;
        onReady?.(instance);
      } catch (e) {
        const error = e instanceof Error ? e : new Error(String(e));
        if (onError) onError(error);
        // oxlint-disable-next-line no-console
        else console.error('Sp00kyProvider: Failed to initialize database', error);
      }
    })();
    return () => {
      destroyed = true;
    };
  });
</script>

{#if db}
  {@render children()}
{:else}
  {@render fallback?.()}
{/if}
