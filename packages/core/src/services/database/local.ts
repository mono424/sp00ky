import type { Diagnostic} from 'surrealdb';
import { DEFAULT_LOCAL_OP_TIMEOUT_MS, LocalOpTimeoutError } from './errors';
import { applyDiagnostics, DateTime, RecordId, Surreal } from 'surrealdb';
import type { Sp00kyConfig } from '../../types';
import type { Logger } from '../logger/index';
import { AbstractDatabaseService } from './database';
import { createDatabaseEventSystem, DatabaseEventTypes } from './events/index';
import { encodeRecordId } from '../../utils/index';
import { ANON_USER_ID } from '../../modules/ref-tables';
import type { SealedQuery } from '../../utils/surql';

/** Thrown when a query carries an `epoch` from before a bucket switch. The
 *  caller's chain read from the previous user's store; its write must be
 *  dropped, not applied to the new bucket. */
export class StaleEpochError extends Error {
  constructor() {
    super('Local store epoch changed (bucket switch); stale write dropped');
    this.name = 'StaleEpochError';
  }
}

/** Store URL for a local bucket. One IndexedDB store per user (`anon` for
 *  signed-out) so cached rows never leak across accounts on a shared device. */
export function bucketStoreUrl(bucketId: string): string {
  return `indxdb://${bucketStoreName(bucketId)}`;
}

/** The IndexedDB database name SurrealDB-WASM derives from the store URL. */
export function bucketStoreName(bucketId: string): string {
  return `sp00ky-${bucketId}`;
}

/** Shared codec: mirrors SurrealDB RecordId/DateTime into our own encodings. */
const localCodecOptions = {
  valueDecodeVisitor(value: unknown) {
    if (value instanceof RecordId) {
      return encodeRecordId(value);
    }

    if (value instanceof DateTime) {
      return value.toDate();
    }

    return value;
  },
};

/**
 * Engine-less Surreal client used as the constructor placeholder. Building this
 * pulls in NO `@surrealdb/wasm` — the ~6 MB wasm engine is deferred to
 * {@link createLocalSurrealClient}, which runs lazily in `connect`/`switchStore`.
 * `connect()` replaces `this.client` with an engine-backed client before any
 * query runs, so this bare client is never actually opened against a store.
 */
function createBareSurrealClient(): Surreal {
  return new Surreal({ codecOptions: localCodecOptions });
}

/**
 * Engine-backed client. Dynamically imports `@surrealdb/wasm` so the wasm engine
 * only enters the graph as a separate chunk fetched on first connect (module-
 * cached thereafter — `switchStore`'s await is instant).
 */
async function createLocalSurrealClient(logger: Logger): Promise<Surreal> {
  const { createWasmWorkerEngines } = await import('@surrealdb/wasm');
  return new Surreal({
    codecOptions: localCodecOptions,
    engines: applyDiagnostics(
      createWasmWorkerEngines(),
      ({ key, type, phase, ...other }: Diagnostic) => {
        if (phase === 'progress' || phase === 'after') {
          logger.trace(
            {
              ...other,
              key,
              type,
              phase,
              service: 'surrealdb:local',
              Category: 'sp00ky-client::LocalDatabaseService::diagnostics',
            },
            `Local SurrealDB diagnostics captured ${type}:${phase}`
          );
        }
      }
    ),
  });
}

export class LocalDatabaseService extends AbstractDatabaseService {
  private config: Sp00kyConfig<any>['database'];
  protected eventType = DatabaseEventTypes.LocalQuery;

  /** Bucket currently open. Set by `connect`/`switchStore`. */
  private bucketId: string = ANON_USER_ID;
  /**
   * Monotonic store generation. Bumped on every `switchStore`. Async chains
   * that read from the store, await something remote, and then write back
   * (sync poll, SSP stream updates) capture this at chain start and drop
   * their write when it no longer matches — a stale-epoch write would land
   * another user's data in the new bucket.
   */
  private storeEpoch = 0;
  /** Gate that `query()`/`execute()` await; closed for the switch window. */
  private gate: Promise<void> = Promise.resolve();
  /** The incoming client while a switch is in flight (for unload cleanup). */
  private pendingSwitchClient: Surreal | null = null;

  constructor(config: Sp00kyConfig<any>['database'], logger: Logger) {
    const events = createDatabaseEventSystem();
    // Placeholder client with no wasm engine; `connect()` swaps in the real
    // engine-backed client (built lazily) before any query is issued.
    super(createBareSurrealClient(), logger, events);
    this.config = config;
    this.queryTimeoutMs = Math.max(0, config.localOpTimeoutMs ?? DEFAULT_LOCAL_OP_TIMEOUT_MS);
  }

  protected override timeoutError(query: string): Error {
    return new LocalOpTimeoutError(query.slice(0, 80), this.queryTimeoutMs);
  }

  getConfig(): Sp00kyConfig<any>['database'] {
    return this.config;
  }

  get currentBucketId(): string {
    return this.bucketId;
  }

  get epoch(): number {
    return this.storeEpoch;
  }

  /**
   * Close the query gate for a bucket switch. Every `query()`/`execute()`
   * issued after this waits until the returned release fn runs — so work
   * triggered mid-switch (sibling auth subscribers registering queries)
   * lands on the NEW bucket instead of racing the swap. The migrator uses
   * `queryUngated()` to provision the new bucket while the gate is closed.
   */
  beginSwitch(): () => void {
    let release!: () => void;
    this.gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    return release;
  }

  override async query<T extends unknown[]>(
    query: string,
    vars?: Record<string, unknown>,
    opts?: { epoch?: number }
  ): Promise<T> {
    await this.gate;
    // A write whose async chain started before a bucket switch must not land
    // in the new bucket — that would be another user's data. Callers on such
    // chains pass the epoch they captured at chain start; mismatches throw.
    if (opts?.epoch !== undefined && opts.epoch !== this.storeEpoch) {
      throw new StaleEpochError();
    }
    return super.query(query, vars);
  }

  override async execute<T>(
    query: SealedQuery<T>,
    vars?: Record<string, unknown>,
    opts?: { epoch?: number }
  ): Promise<T> {
    const raw = await this.query<unknown[]>(query.sql, vars, opts);
    return query.extract(raw);
  }

  /** Gate-bypassing query — ONLY for the switch path itself (schema
   *  provisioning must run while the gate is closed, or it deadlocks). */
  queryUngated<T extends unknown[]>(query: string, vars?: Record<string, unknown>): Promise<T> {
    return super.query(query, vars);
  }

  async connect(bucketId: string = ANON_USER_ID): Promise<void> {
    const { namespace, database } = this.getConfig();
    const store = this.getConfig().store ?? 'memory';
    this.bucketId = bucketId;
    const storeUrl = store === 'memory' ? 'mem://' : bucketStoreUrl(bucketId);
    this.logger.info(
      { namespace, database, storeUrl, Category: 'sp00ky-client::LocalDatabaseService::connect' },
      'Connecting to local database'
    );

    this.registerUnloadClose();
    // Build the real engine-backed client lazily here (first `@surrealdb/wasm`
    // load), replacing the constructor's engine-less placeholder before any
    // query runs.
    this.client = await createLocalSurrealClient(this.logger);
    await this.openWithRecovery(this.client, storeUrl, namespace, database, bucketId, store);
  }

  /**
   * Switch the local store to another user's bucket. Opens the NEW bucket on a
   * second client first (with the same 3-tier recovery), then atomically swaps
   * `this.client` and closes the old one — a failed open never leaves the
   * service on a dead client. Bumps the store epoch so in-flight old-bucket
   * async chains can detect they're stale.
   *
   * Callers own the drain/rebind choreography (close the gate, quiesce sync +
   * timers BEFORE calling this; re-provision + rebind AFTER).
   */
  async switchStore(bucketId: string): Promise<void> {
    if (bucketId === this.bucketId) return;
    const { namespace, database } = this.getConfig();
    const store = this.getConfig().store ?? 'memory';
    this.storeEpoch++;

    if (store === 'memory') {
      // mem:// has no per-user persistence; close + reopen the same client
      // yields a fresh empty store, which is exactly the reset we want.
      try {
        await this.client.close();
      } catch {
        /* ignore */
      }
      await this.openStore(this.client, 'mem://', namespace, database);
      this.bucketId = bucketId;
      this.logger.info(
        { bucketId, Category: 'sp00ky-client::LocalDatabaseService::switchStore' },
        'Reset in-memory local store for bucket switch'
      );
      return;
    }

    const next = await createLocalSurrealClient(this.logger);
    this.pendingSwitchClient = next;
    try {
      await this.openWithRecovery(
        next,
        bucketStoreUrl(bucketId),
        namespace,
        database,
        bucketId,
        store
      );
    } finally {
      this.pendingSwitchClient = null;
    }

    const old = this.client;
    this.client = next;
    this.bucketId = bucketId;
    try {
      await old.close();
    } catch {
      /* best-effort — the old store's handle is released on unload regardless */
    }
    this.logger.info(
      { bucketId, Category: 'sp00ky-client::LocalDatabaseService::switchStore' },
      'Switched local store bucket'
    );
  }

  /** See `LocalStore.dropBucket`. A memory store keeps nothing to delete. */
  async dropBucket(bucketId: string): Promise<void> {
    if (bucketId === this.bucketId) throw new Error('Cannot drop the open bucket.');
    if ((this.getConfig().store ?? 'memory') === 'memory') return;
    await dropLocalIndexedDbStores(this.logger, bucketStoreName(bucketId));
  }

  /**
   * Open `storeUrl` on `client` with tiered recovery:
   * tier 1 retries the same store (transient idb-handle races — preserves the
   * cache), tier 2 drops THIS bucket's IndexedDB store and reconnects fresh,
   * tier 3 falls back to `mem://` for the session. Only ever drops the bucket
   * being opened — other users' buckets hold their own caches AND un-pushed
   * mutation outboxes, which must survive another bucket's corruption.
   */
  private async openWithRecovery(
    client: Surreal,
    storeUrl: string,
    namespace: string,
    database: string,
    bucketId: string,
    store: string
  ): Promise<void> {
    try {
      await this.openStore(client, storeUrl, namespace, database);
      this.logger.info(
        { Category: 'sp00ky-client::LocalDatabaseService::connect' },
        'Connected to local database'
      );
      return;
    } catch (err) {
      // A persistent (IndexedDB) local store can fail to open if it was left
      // corrupt or version-incompatible by a prior session/crash/engine bump.
      // The local store is only a cache (everything re-syncs from the server),
      // so recover by dropping it and reconnecting rather than bricking startup.
      if (store === 'memory' || !isLocalStoreOpenError(err)) {
        this.logger.error(
          { err, Category: 'sp00ky-client::LocalDatabaseService::connect' },
          'Failed to connect to local database'
        );
        throw err;
      }
      this.logger.warn(
        { err, Category: 'sp00ky-client::LocalDatabaseService::connect' },
        'Local IndexedDB store failed to open; retrying before clearing'
      );
    }

    // Tier 1 — RETRY the SAME store WITHOUT dropping. The idb open/`use` failure
    // is often transient (a not-yet-released handle from the previous page, or a
    // first-open WAL-recovery race), not real corruption. Closing and reopening
    // frequently succeeds — and crucially PRESERVES the cache, so a warm load
    // stays warm. Dropping the store every time (the old behavior) silently wiped
    // the cache on every reload, making warm loads as slow as cold ones.
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        await client.close();
      } catch {
        /* ignore */
      }
      await delay(150 * attempt);
      try {
        await this.openStore(client, storeUrl, namespace, database);
        this.logger.info(
          { attempt, Category: 'sp00ky-client::LocalDatabaseService::connect' },
          'Connected to local database on retry (cache preserved)'
        );
        return;
      } catch (retryErr) {
        this.logger.warn(
          { err: retryErr, attempt, Category: 'sp00ky-client::LocalDatabaseService::connect' },
          'Local store retry failed'
        );
      }
    }

    // Tier 2 — the store is genuinely unopenable; drop THIS bucket and reconnect
    // fresh. This loses the bucket's cache (re-syncs from the server), so it's
    // the last resort before in-memory.
    try {
      await client.close();
    } catch {
      /* ignore — closing a half-open connection is best-effort */
    }
    await dropLocalIndexedDbStores(this.logger, bucketStoreName(bucketId));

    try {
      await this.openStore(client, storeUrl, namespace, database);
      this.logger.info(
        { Category: 'sp00ky-client::LocalDatabaseService::connect' },
        'Reconnected to local database after clearing the corrupt store'
      );
    } catch (retryErr) {
      // Last resort: run in-memory so the app still loads. No local persistence
      // this session; the freshly-dropped IndexedDB is recreated cleanly next
      // load, and all data re-syncs from the server regardless.
      this.logger.error(
        { err: retryErr, Category: 'sp00ky-client::LocalDatabaseService::connect' },
        'Local store still failing after clear; falling back to in-memory'
      );
      try {
        await client.close();
      } catch {
        /* ignore */
      }
      await this.openStore(client, 'mem://', namespace, database);
      this.logger.warn(
        { Category: 'sp00ky-client::LocalDatabaseService::connect' },
        'Connected to local database (in-memory fallback)'
      );
    }
  }

  private unloadCloseRegistered = false;

  /**
   * Close the local DB on page unload so the SurrealDB-WASM worker releases its
   * IndexedDB connection cleanly. Without this, the previous page's connection
   * lingers; the next load's `client.connect` opens the store but the first
   * write transaction in `client.use` hits an "IndexedDB error" — which then
   * (mis)triggered the corrupt-store recovery and WIPED the cache on every
   * reload, making warm loads as slow as cold ones. `pagehide` is the reliable
   * unload signal (fires on bfcache + normal navigation); `close()` is async but
   * the WASM worker initiates the IndexedDB connection teardown synchronously.
   * Also closes a mid-switch incoming client so its fresh handle doesn't linger.
   */
  private registerUnloadClose(): void {
    if (this.unloadCloseRegistered || typeof window === 'undefined') return;
    this.unloadCloseRegistered = true;
    const close = () => {
      try {
        void this.client.close();
      } catch {
        /* best-effort */
      }
      try {
        void this.pendingSwitchClient?.close();
      } catch {
        /* best-effort */
      }
    };
    window.addEventListener('pagehide', close);
    window.addEventListener('beforeunload', close);
  }

  private async openStore(
    client: Surreal,
    storeUrl: string,
    namespace: string,
    database: string
  ): Promise<void> {
    this.logger.debug(
      { storeUrl, Category: 'sp00ky-client::LocalDatabaseService::connect' },
      '[LocalDatabaseService] Calling client.connect'
    );
    await client.connect(storeUrl, {});
    this.logger.debug(
      { namespace, database, Category: 'sp00ky-client::LocalDatabaseService::connect' },
      '[LocalDatabaseService] client.connect returned. Calling client.use'
    );
    await client.use({ namespace, database });
    this.logger.debug(
      { Category: 'sp00ky-client::LocalDatabaseService::connect' },
      '[LocalDatabaseService] client.use returned'
    );
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** True for the SurrealDB-WASM error raised when its IndexedDB-backed key-value
 *  store can't be opened (corrupt / version-incompatible / blocked). Exported
 *  for unit testing the error-message match. */
export function isLocalStoreOpenError(err: unknown): boolean {
  const msg = (err instanceof Error ? err.message : String(err)).toLowerCase();
  return (
    msg.includes('indexeddb') ||
    msg.includes('idb error') ||
    msg.includes('key-value store')
  );
}

/** Best-effort delete of ONE bucket's IndexedDB store(s). SurrealDB-WASM backs
 *  `indxdb://<name>` with one or more IndexedDB databases whose names include
 *  `<name>`. Scoped to the given store name — never wipe by the bare `sp00ky`
 *  substring, that would take every user's bucket (and their un-pushed mutation
 *  outboxes) down with one corrupt store. Resolves even on error/blocked so
 *  startup can proceed. No-op outside a browser. Exported for unit tests. */
export async function dropLocalIndexedDbStores(logger: Logger, storeName: string): Promise<void> {
  if (typeof indexedDB === 'undefined') return;
  try {
    let names: string[] = [];
    if (typeof indexedDB.databases === 'function') {
      const dbs = await indexedDB.databases();
      names = dbs
        .map((d) => d.name)
        .filter((n): n is string => !!n && matchesBucketStore(n, storeName));
    }
    // Fall back to the known store name if enumeration is unavailable/empty.
    if (names.length === 0) names = [storeName];
    await Promise.all(names.map(deleteIndexedDb));
    logger.info(
      { names, Category: 'sp00ky-client::LocalDatabaseService::connect' },
      'Cleared local IndexedDB store(s)'
    );
  } catch (e) {
    logger.warn(
      { err: e, Category: 'sp00ky-client::LocalDatabaseService::connect' },
      'Failed to enumerate/clear IndexedDB; proceeding anyway'
    );
  }
}

/** True when idb database `name` belongs to the bucket store `storeName` —
 *  exact match or a derived name (`<storeName>`, `<storeName>-*`, `*<storeName>*`
 *  with a non-alphanumeric boundary so `sp00ky-abc` never matches
 *  `sp00ky-abcdef`'s store). Exported for unit tests. */
export function matchesBucketStore(name: string, storeName: string): boolean {
  const lower = name.toLowerCase();
  const target = storeName.toLowerCase();
  const idx = lower.indexOf(target);
  if (idx === -1) return false;
  const after = lower[idx + target.length];
  return after === undefined || !/[a-z0-9_]/.test(after);
}

/** Nuke EVERY sp00ky local bucket on this device (manual full reset only —
 *  the automated corruption recovery is scoped to one bucket). */
export async function dropAllSp00kyIndexedDbStores(logger: Logger): Promise<void> {
  if (typeof indexedDB === 'undefined') return;
  try {
    let names: string[] = [];
    if (typeof indexedDB.databases === 'function') {
      const dbs = await indexedDB.databases();
      names = dbs
        .map((d) => d.name)
        .filter((n): n is string => !!n && n.toLowerCase().includes('sp00ky'));
    }
    if (names.length === 0) names = ['sp00ky'];
    await Promise.all(names.map(deleteIndexedDb));
    logger.info(
      { names, Category: 'sp00ky-client::LocalDatabaseService::reset' },
      'Cleared ALL sp00ky IndexedDB stores'
    );
  } catch (e) {
    logger.warn(
      { err: e, Category: 'sp00ky-client::LocalDatabaseService::reset' },
      'Failed to enumerate/clear IndexedDB; proceeding anyway'
    );
  }
}

function deleteIndexedDb(name: string): Promise<void> {
  return new Promise((resolve) => {
    try {
      const req = indexedDB.deleteDatabase(name);
      req.onsuccess = () => resolve();
      req.onerror = () => resolve();
      req.onblocked = () => resolve();
    } catch {
      resolve();
    }
  });
}
