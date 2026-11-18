/**
 * Storage diagnostics for the DevTools Storage tab: what engine backs the
 * local cache, whether it actually persists, how much of the device's quota
 * the origin uses, and what is physically sitting in OPFS. Assembled by
 * `DevToolsService.getStorageInfo()`; everything here is JSON-safe.
 */

export interface OpfsEntry {
  /** Path relative to the OPFS root, e.g. `.sp00ky-anon/0000000000000001`. */
  path: string;
  kind: 'file' | 'directory';
  /** Absent when the file's size can't be read (e.g. an exclusive sync access
   *  handle is held on it — exactly the case during SAHPool contention). */
  size?: number;
}

/** Engine-side numbers only the engine can produce (worker round-trips). */
export interface EngineStorageDiagnostics {
  engine: 'sqlite' | 'rows';
  bucketId: string;
  useOpfs: boolean;
  workerSelectConfigured: boolean;
  /** `false` while configured `true` means the runtime downgraded to the
   *  legacy multi-hop select (stale cached worker bundle). */
  workerSelectEffective: boolean;
  /** page_count * page_size. */
  dbSizeBytes?: number;
  /** freelist_count * page_size — reclaimable via VACUUM. On the row store:
   *  dead bytes in the log, reclaimed by its next compaction. */
  freelistBytes?: number;
  tableCounts?: { table: string; rows: number }[];
  error?: string;
}

/**
 * Shared-tabs coordination state. Reported ONLY when `sharedTabs: true` was
 * configured (apps that never asked for it get `null`, so the panel shows
 * nothing). `active: false` with a `reason` is itself the useful signal: the
 * app asked to share one store and this tab is not, so it owns or contends for
 * the OPFS pool alone.
 */
export interface SharedTabsInfo {
  active: boolean;
  /** Why inactive: a capability gate reason, or 'fell-back' when the broker
   *  was reachable but no role landed (election timeout, rejected tab). */
  reason?: string;
  role?: 'solo' | 'leader' | 'follower';
  /** This tab's id (also the suffix of every mutation id it mints). */
  tabId?: string;
  /** Monotonic id of the current leadership term; embedded in the worker lock. */
  leadershipId?: number;
  /** Who owns the store: this tab when leader, else the leader's tab id. */
  leaderTabId?: string | null;
  /** Leader only: attached follower tabs. */
  followers?: number;
  /** Leader only: ingest batches relayed to followers so far. */
  relayedBatches?: number;
}

export interface StorageInfo {
  at: number;
  engine: { kind: 'surrealdb' | 'sqlite' | 'rows' | 'custom'; store: string; bucketId: string };
  health: { status: 'unknown' | 'persistent' | 'memory'; fallback: boolean; error?: string };
  /** Shared-tabs state, or null when the feature was never requested. */
  tabs?: SharedTabsInfo | null;
  browser: {
    /** `navigator.storage.persisted()` — whether the origin's storage is
     *  exempt from eviction (unrelated to the OPFS pool lock). */
    persisted?: boolean;
    usage?: number;
    quota?: number;
    /** Chrome-only per-system breakdown from `estimate()`. */
    usageDetails?: Record<string, number>;
    error?: string;
  };
  opfs: {
    supported: boolean;
    entries: OpfsEntry[];
    totalBytes: number;
    truncated: boolean;
    error?: string;
  };
  /**
   * Bucket-file cache counters. `evicted*` is cumulative for this tab's
   * session, so a number that climbs while `totalBytes` sits at the budget is
   * the signal that `blobCache.maxBytes` is too small for the working set.
   */
  blobs?: BlobCacheInfo;
  /** Snapshot of `globalThis.__sqliteStats` (SQLite engine only). */
  sqliteStats?: Record<string, unknown>;
  engineDiagnostics?: EngineStorageDiagnostics;
}

export interface BlobCacheInfo {
  entries: number;
  totalBytes: number;
  budgetBytes: number;
  pinnedBytes: number;
  evictedEntries: number;
  evictedBytes: number;
  /** Manifest rows rebuilt from disk at the last reconcile. */
  reconciledEntries: number;
  hits: number;
  misses: number;
  /** False when OPFS is unwritable or the quota was exhausted — the cache is
   *  running in per-tab memory and nothing survives a reload. */
  persistent: boolean;
  /** Over budget with only pinned or on-screen entries left: new files are no
   *  longer written rather than pinned ones being discarded. */
  persistPaused: boolean;
}

/**
 * Recursively list the origin's OPFS. Sizes come from `handle.getFile()`,
 * which throws for a file another context holds an exclusive sync access
 * handle on — SAHPool does exactly that for its whole pool, so a locked file
 * (size omitted) is a live "who has the pool" signal, not a failure.
 */
export async function walkOpfs(maxEntries = 2000, maxDepth = 8): Promise<StorageInfo['opfs']> {
  const nav = typeof navigator !== 'undefined' ? navigator : undefined;
  if (!nav?.storage?.getDirectory) {
    return { supported: false, entries: [], totalBytes: 0, truncated: false };
  }
  const entries: OpfsEntry[] = [];
  let totalBytes = 0;
  let truncated = false;
  try {
    const root = await nav.storage.getDirectory();
    const walk = async (dir: FileSystemDirectoryHandle, prefix: string, depth: number) => {
      if (depth > maxDepth) return;
      // entries() is standard; older lib.dom typings may lack it.
      for await (const [name, handle] of (dir as any).entries() as AsyncIterable<
        [string, FileSystemHandle]
      >) {
        if (entries.length >= maxEntries) {
          truncated = true;
          return;
        }
        const path = prefix ? `${prefix}/${name}` : name;
        if (handle.kind === 'directory') {
          entries.push({ path, kind: 'directory' });
          await walk(handle as FileSystemDirectoryHandle, path, depth + 1);
        } else {
          let size: number | undefined;
          try {
            size = (await (handle as FileSystemFileHandle).getFile()).size;
            totalBytes += size;
          } catch {
            // Locked by an exclusive access handle (e.g. a live SAHPool).
          }
          const entry: OpfsEntry = { path, kind: 'file' };
          if (size !== undefined) entry.size = size;
          entries.push(entry);
        }
      }
    };
    await walk(root, '', 0);
    entries.sort((a, b) => a.path.localeCompare(b.path));
    return { supported: true, entries, totalBytes, truncated };
  } catch (e) {
    return {
      supported: true,
      entries,
      totalBytes,
      truncated,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}
