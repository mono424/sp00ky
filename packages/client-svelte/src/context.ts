import { getContext, hasContext, setContext } from 'svelte';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { SyncedDb } from './db.js';
import type { MaybeGetter } from './types.js';

const KEY = Symbol.for('@spooky-sync/client-svelte/db');

/**
 * What the provider puts in context. A holder, not the client itself: context
 * is set synchronously while the provider initialises, the client is ready
 * later, and the children (the only readers) render after it is.
 */
interface DbHolder {
  readonly db: SyncedDb<any> | undefined;
}

/**
 * Put a client in context for the components below, instead of rendering
 * `<Sp00kyProvider>`. For apps that create and `init()` the `SyncedDb`
 * themselves (a SvelteKit `+layout.ts`, a test). Call it while a component
 * initialises, like `setContext`.
 */
export function setDbContext(db: SyncedDb<any> | DbHolder): void {
  setContext<DbHolder>(KEY, 'getSp00ky' in db ? { db } : db);
}

/**
 * The `SyncedDb` of the nearest `<Sp00kyProvider>`. Like every context read it
 * must run while a component initialises (top level of `<script>`, or a
 * function called from there), not in an event handler or after an `await`.
 */
export function useDb<S extends SchemaStructure>(): SyncedDb<S> {
  const db = hasContext(KEY) ? getContext<DbHolder>(KEY).db : undefined;
  if (!db) {
    throw new Error(
      'useDb must be used within a <Sp00kyProvider>. Wrap your app in <Sp00kyProvider config={...}>, ' +
        'or pass the SyncedDb explicitly.'
    );
  }
  return db as SyncedDb<S>;
}

/** Read a `MaybeGetter`, tracking whatever the getter reads. */
export function read<T>(value: MaybeGetter<T>): T {
  return typeof value === 'function' ? (value as () => T)() : value;
}

export function toError(e: unknown): Error {
  return e instanceof Error ? e : new Error(String(e));
}
