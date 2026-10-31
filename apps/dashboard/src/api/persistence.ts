/**
 * Where the dashboard's live collections are cached between visits.
 *
 * TanStack DB's browser persistence: SQLite (wa-sqlite) in the Origin
 * Private File System, in a worker, with a coordinator so several tabs share
 * one database safely. A collection's rows and its resume cursor (`{boot,
 * seq}`) commit in one transaction, so a reload paints the last state at
 * once and then asks the host only for what changed since.
 *
 * OPFS needs a secure context. A scheduler reached over plain http by IP
 * (the embedded dashboard in a private network) has none, and there the
 * collections simply live in memory: same code, no cache. The wasm and the
 * worker are only fetched when persistence can actually run.
 *
 * The cache holds operator data (job payloads, incident detail), so it is
 * wiped on sign-out: see {@link clearPersistence}.
 */

import type { PersistedCollectionPersistence } from '@tanstack/browser-db-sqlite-persistence';

const DATABASE = 'spky-dashboard.sqlite';
const CHANNEL = 'spky-dashboard';

interface Opened {
  persistence: PersistedCollectionPersistence;
  close: () => Promise<void>;
}

let opening: Promise<Opened | null> | null = null;

/** Whether this page can persist at all. */
export function persistenceSupported(): boolean {
  return (
    typeof window !== 'undefined' &&
    window.isSecureContext &&
    typeof Worker !== 'undefined' &&
    typeof navigator.storage?.getDirectory === 'function'
  );
}

async function open(): Promise<Opened | null> {
  if (!persistenceSupported()) return null;
  try {
    const mod = await import('@tanstack/browser-db-sqlite-persistence');
    const database = await mod.openBrowserWASQLiteOPFSDatabase({ databaseName: DATABASE });
    const coordinator = new mod.BrowserCollectionCoordinator({ dbName: CHANNEL });
    const persistence = mod.createBrowserWASQLitePersistence({ database, coordinator });
    return {
      persistence,
      close: async () => {
        coordinator.dispose();
        await database.close?.();
      },
    };
  } catch (err) {
    // A browser without OPFS sync handles, a private window, a full disk:
    // never fatal, the dashboard just starts cold.
    console.warn('Dashboard cache unavailable; running without it', err);
    return null;
  }
}

/** The shared persistence, or null when this page cannot persist. */
export async function getPersistence(): Promise<PersistedCollectionPersistence | null> {
  opening ??= open();
  return (await opening)?.persistence ?? null;
}

/**
 * Close the database and delete its files. Called on sign-out, before any
 * collection for the next session is created.
 */
export async function clearPersistence(): Promise<void> {
  const opened = await opening;
  opening = null;
  await opened?.close().catch(() => undefined);
  if (!persistenceSupported()) return;
  try {
    const root = await navigator.storage.getDirectory();
    const names: string[] = [];
    // `keys()` is on every OPFS directory handle; the DOM typings lag behind.
    for await (const name of (root as unknown as { keys(): AsyncIterable<string> }).keys()) {
      if (name.startsWith(DATABASE)) names.push(name);
    }
    await Promise.all(names.map((n) => root.removeEntry(n, { recursive: true }).catch(() => undefined)));
  } catch {
    /* nothing cached, or storage already gone */
  }
}
