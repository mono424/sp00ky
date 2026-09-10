import { applyPatch, type Operation } from 'fast-json-patch';
import { DEFAULT_LOCAL_OP_TIMEOUT_MS, LocalOpTimeoutError } from './errors';
import { withTimeout } from '../../utils/index';
import type { QueryPlan, RelationPlan, WhereNode } from '@spooky-sync/query-builder';
import {
  renderOrderSql,
  renderWhereSql,
  reviveRow,
  serializeRow,
  project,
  projectedDataSql,
} from './sqlite-plan-sql';
import type { Logger } from '../logger/index';
import type { Sp00kyConfig, StorageHealth } from '../../types';
import type { SnapshotMeta, StoredSnapshot } from './cache-engine';
import type { SealedQuery } from '../../utils/surql';
import { resolveRelations, stableKey } from './relation-resolver';
import {
  createDatabaseEventSystem,
  DatabaseEventTypes,
  type DatabaseEventSystem,
} from './events/index';
import { StaleEpochError } from './local';
import { translateSurql, tableOf, setPath, getPath, type SqlOp } from './surql-translate';
import {
  BrokerPortClosedError,
  PortSqliteTransport,
  WorkerSqliteTransport,
  type SqliteTransport,
} from './sqlite-transport';
import { PROMOTION_OPEN_OPTIONS } from './sqlite-open';
import type { EngineTx, Id, LocalStore, OrderBy, RelationFetch, Row } from './cache-engine';
import type { EngineStorageDiagnostics } from '../../modules/devtools/storage-info';

/**
 * The statement result a pure-write op contributes to a query's results array.
 * Single source of truth shared by the per-op path (`execOp`) and the batched
 * fast path in `query()`, so the two can never diverge: a caller that reads a
 * statement's output sees the same shape whether or not the transaction took the
 * batch fast path. In particular `create()` compiles to an all-upsert tx
 * (`createSet` + `createMutation`) and reads `resultIndex:0` for the new row and
 * its id — the fast path previously returned empty arrays there, so the row (and
 * its id) was lost and the reconcile crashed in `encodeRecordId`.
 *
 * An upsert echoes the written row (`{...data, id}`) with no read-back — the
 * full merged row is only materialized for a LET-wrapped upsert (see the 'let'
 * case). delete/deleteAll yield `[]`; noop yields `null`.
 */
/**
 * The `_00_*` internal tables the client relies on. The LocalMigrator DEFINEs
 * them, but every DEFINE lowers to a noop on the SQLite engine, so they must be
 * created physically at open (see `openInternal`) or a read-before-first-write
 * on a fresh bucket throws "no such table". Keep in sync with the systemSchema
 * block in `local-migrator.ts`.
 */
const SNAPSHOT_TABLE = '_00_circuit_snapshot';

const SYSTEM_TABLES = [
  '_00_stream_processor_state',
  // In-browser circuit snapshot: one BLOB row (`circuit`) + a JSON meta row.
  // Excluded from DevTools' table listing, since its data column is not JSON.
  '_00_circuit_snapshot',
  '_00_view',
  '_00_window',
  '_00_failed_mutations',
  '_00_schema',
  '_00_pending_mutations',
  // Blob cache manifest. Read before its first write on every boot (reconcile
  // asks for the ids it found in OPFS), so it has to exist up front like the
  // rest — otherwise the very first reconcile throws and the cache runs cold.
  '_00_blob',
  // Server-written, synced-down meta tables (see meta_tables_client.surql).
  // DEFINE is a noop on this engine, so without seeding them here their synced
  // rows have no local table to land in: feature flags silently fall back to
  // defaults and app-release update notifications never show.
  '_00_user_feature',
  '_00_app_release',
] as const;

export function pureWriteOpResult(op: SqlOp): unknown {
  switch (op.kind) {
    case 'upsert':
      return { ...op.data, id: stableKey(op.id) };
    case 'noop':
      return null;
    default:
      return [];
  }
}

/**
 * Local cache backend on official SQLite-WASM in a dedicated Worker (see
 * `sqlite-worker.ts`), with OPFS SAHPool persistence. Storage model: one table
 * per schema table, `id TEXT PRIMARY KEY, data TEXT` where `data` is the row as
 * JSON. Filtering/ordering use `json_extract`. Relations are decomposed by the
 * shared {@link resolveRelations} — identical to the SurrealDB backend.
 *
 * Value normalization (JSON round-trip):
 * - `Uint8Array`/bytes → `{ "__u8": <base64> }` (CRDT snapshots survive).
 * - Record links / ids → their `table:id` string form (so `json_extract`
 *   comparisons and `IN` matching are consistent). NOTE: link fields therefore
 *   read back as strings, not `RecordId` instances — the one shape difference
 *   from the SurrealDB backend, to be closed with schema-driven revival + an
 *   oracle E2E in the browser.
 */
export class SqliteCacheEngine implements LocalStore {
  /** The wire to the SQLite worker: an owned dedicated Worker (solo/leader) or
   *  a MessagePort into another tab's worker (follower). See sqlite-transport. */
  private transport: SqliteTransport | null = null;
  private storeEpoch = 0;
  private knownTables = new Set<string>();
  private useOpfs: boolean;
  /** Whether `select` runs as one worker round-trip (plan executed in-worker).
   *  Flipped off at runtime if the worker script predates the `select` op
   *  (stale cached bundle) — degrade to the legacy multi-hop path, don't break. */
  private workerSelect: boolean;
  /** What `workerSelect` was at construction, so DevTools can tell a runtime
   *  downgrade (configured true, effective false) from a configured-off. */
  private workerSelectConfigured: boolean;
  private events: DatabaseEventSystem = createDatabaseEventSystem();
  private bucketId = 'anon';
  /** Deadline for one worker round trip; see `localOpTimeoutMs`. */
  private readonly localOpTimeoutMs: number;
  /** Durability of the local store, set on every open. A plain Set of callbacks
   *  rather than a `DatabaseEventSystem` event: this changes at most once per
   *  open, and the typed event map is about query traffic. */
  private storageHealthValue: StorageHealth = { status: 'unknown', fallback: false };
  private storageHealthSubs = new Set<(health: StorageHealth) => void>();
  /** Schemaless — tables are created lazily on first write; no migrator. */
  readonly usesSurqlSchema = false;

  readonly engineKind = 'sqlite' as const;

  /** Shared-tabs mode: the engine's transport is swapped at runtime by the
   *  TabsCoordinator (owner worker as leader, leader's port as follower). */
  private shared: boolean;
  /** Leaderless parking (shared mode): ops entering the opQueue await this
   *  gate until a new role lands or the timeout rejects them. */
  private roleGate: { promise: Promise<void>; release: () => void } | null = null;
  private roleGateTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private config: Sp00kyConfig<any>['database'],
    private logger: Logger,
    opts: { useOpfs?: boolean; workerSelect?: boolean; shared?: boolean } = {}
  ) {
    this.localOpTimeoutMs = Math.max(0, config.localOpTimeoutMs ?? DEFAULT_LOCAL_OP_TIMEOUT_MS);
    this.useOpfs = opts.useOpfs ?? true;
    this.workerSelect = opts.workerSelect ?? config.workerSelect ?? true;
    this.workerSelectConfigured = this.workerSelect;
    this.shared = opts.shared ?? false;
  }

  get epoch(): number {
    return this.storeEpoch;
  }

  get currentBucketId(): string {
    return this.bucketId;
  }

  get storageHealth(): StorageHealth {
    return this.storageHealthValue;
  }

  /** Fires immediately with the current snapshot (the store opens during
   *  `connect()`, before app components mount, so a late subscriber must still
   *  learn a fallback happened), then on every change. */
  subscribeToStorageHealth(cb: (health: StorageHealth) => void): () => void {
    cb(this.storageHealthValue);
    this.storageHealthSubs.add(cb);
    return () => {
      this.storageHealthSubs.delete(cb);
    };
  }

  private setStorageHealth(health: StorageHealth): void {
    this.storageHealthValue = health;
    for (const cb of this.storageHealthSubs) cb(health);
  }

  getConfig(): Sp00kyConfig<any>['database'] {
    return this.config;
  }

  /**
   * Storage numbers for the DevTools Storage tab. Uses {@link call} so the
   * reads serialize with regular traffic (no SQLITE_BUSY). Never throws — the
   * worker may be mid bucket-switch; a failure lands in `error` instead.
   */
  async getStorageDiagnostics(opts?: { tableCounts?: boolean }): Promise<EngineStorageDiagnostics> {
    const diag: EngineStorageDiagnostics = {
      engine: 'sqlite',
      bucketId: this.bucketId,
      useOpfs: this.useOpfs,
      workerSelectConfigured: this.workerSelectConfigured,
      workerSelectEffective: this.workerSelect,
    };
    try {
      const { rows } = await this.call<{ rows: { bytes: number; freelist: number }[] }>('exec', {
        sql:
          'SELECT (SELECT * FROM pragma_page_count()) * (SELECT * FROM pragma_page_size()) AS bytes, ' +
          '(SELECT * FROM pragma_freelist_count()) * (SELECT * FROM pragma_page_size()) AS freelist',
      });
      diag.dbSizeBytes = rows?.[0]?.bytes;
      diag.freelistBytes = rows?.[0]?.freelist;
      if (opts?.tableCounts) {
        const { rows: tables } = await this.call<{ rows: { name: string }[] }>('exec', {
          sql: "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        });
        const names = (tables ?? []).map((r) => r.name).filter((n) => n !== SNAPSHOT_TABLE);
        if (names.length) {
          // Names come from sqlite_master itself; double-quoting is enough.
          const sql = names
            .map((n) => `SELECT '${n.replace(/'/g, "''")}' AS t, COUNT(*) AS n FROM "${n.replace(/"/g, '""')}"`)
            .join(' UNION ALL ');
          const { rows: counts } = await this.call<{ rows: { t: string; n: number }[] }>('exec', {
            sql,
          });
          diag.tableCounts = (counts ?? []).map((r) => ({ table: r.t, rows: r.n }));
        } else {
          diag.tableCounts = [];
        }
      }
    } catch (e) {
      diag.error = e instanceof Error ? e.message : String(e);
    }
    return diag;
  }

  getEvents(): DatabaseEventSystem {
    return this.events;
  }

  getClient(): unknown {
    throw new Error('SqliteCacheEngine has no SurrealDB client (getClient is unavailable).');
  }

  /** LocalStore alias; SQLite has no in-flight gate, so this maps to a rebuild. */
  switchStore(bucketId: string): Promise<void> {
    return this.switchBucket(bucketId);
  }

  /** SQLite has no switch gate window; the epoch bump alone fences stale writes. */
  beginSwitch(): () => void {
    return () => {};
  }

  // ---- worker plumbing -----------------------------------------------------

  /** Serializes every worker op so reads/writes never overlap at the VFS layer
   *  (overlapping ops trip SQLITE_BUSY). Mirrors the SurrealDB engine's
   *  single-flight query queue. (The worker keeps its own chain too, for
   *  multi-client mode; this one additionally provides the queue-wait stat and
   *  the boot/switch atomicity below.) */
  private opQueue: Promise<unknown> = Promise.resolve();

  private call<T = any>(type: string, payload?: unknown): Promise<T> {
    const enqueuedAt = performance.now();
    const run = async () => {
      // Leaderless window (shared mode): park behind the role gate instead of
      // failing; a new leader releases it, the timeout rejects it.
      if (this.roleGate) await this.roleGate.promise;
      // Time spent waiting behind other ops in the queue, not doing work.
      getStats().queueWaitMs += performance.now() - enqueuedAt;
      return this.rawCall<T>(type, payload);
    };
    const result = this.opQueue.then(run, run);
    // Keep the chain alive regardless of individual failures.
    this.opQueue = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  /**
   * Ops dispatched to the worker and not yet answered. Role transitions run on
   * their own chain (see {@link transitionChain}), so they can start while the
   * opQueue still has an op at the worker; tearing the transport down under it
   * would reject that op — and its caller may be a query whose only fetch this
   * was. {@link drainInFlight} lets a deliberate teardown wait them out.
   */
  private inFlightCalls = new Set<Promise<unknown>>();

  /** Wait for dispatched ops to answer before a deliberate transport teardown.
   *  Bounded: a wedged worker must not block the role change forever. */
  private async drainInFlight(timeoutMs = 2_000): Promise<void> {
    if (this.inFlightCalls.size === 0) return;
    const settled = Promise.all([...this.inFlightCalls].map((p) => p.catch(() => undefined)));
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        settled,
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, timeoutMs);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private rawCall<T = any>(type: string, payload?: unknown): Promise<T> {
    if (!this.transport) throw new Error('SqliteCacheEngine: not connected');
    // --- instrumentation: live, inspectable via `globalThis.__sqliteStats` ---
    const s = getStats();
    s.roundTrips++;
    s.byType[type] = (s.byType[type] ?? 0) + 1;
    if (type === 'batch' && Array.isArray(payload)) {
      s.batchStatements += payload.length;
      s.maxBatch = Math.max(s.maxBatch, payload.length);
    }
    s.inFlight++;
    s.maxInFlight = Math.max(s.maxInFlight, s.inFlight);
    if (this.transport.kind === 'port') s.proxiedOps = (s.proxiedOps ?? 0) + 1;
    const sentAt = performance.now();
    // Track the dispatch, not the returned promise: attaching a handler to
    // `result` would mark it handled and swallow the unhandled-rejection
    // reports that surface a caller which forgot to catch.
    let settle!: () => void;
    const tracked = new Promise<void>((res) => {
      settle = res;
    });
    this.inFlightCalls.add(tracked);
    const finish = () => {
      this.inFlightCalls.delete(tracked);
      settle();
    };
    // Deadline per op. The transport parks a call until the worker replies,
    // and a worker starved behind a long op (or wedged on an unbounded lock
    // check) answered never; every caller up the stack then waited forever.
    // Reject the CALLER only: the op is still queued in the worker and its
    // late reply is dropped by the transport (no pending entry left to
    // resolve). Deliberately no teardown here - a slow worker is not a dead
    // one, and killing it mid-write would cost more than the wait.
    const timeoutMs = this.localOpTimeoutMs;
    const result = withTimeout(
      this.transport.call<T>(type, payload),
      timeoutMs,
      () => new LocalOpTimeoutError(type, timeoutMs)
    ).then(
      (v: T) => {
        finish();
        s.inFlight--;
        // Split the round-trip: `wt` is time inside the worker's handler,
        // the remainder is postMessage + scheduling overhead.
        const wt = (v as { wt?: unknown } | null)?.wt;
        if (typeof wt === 'number') {
          s.workerMs += wt;
          s.rpcOverheadMs += Math.max(0, performance.now() - sentAt - wt);
        }
        return v;
      },
      (e: unknown) => {
        finish();
        s.inFlight--;
        if (e instanceof LocalOpTimeoutError) {
          s.timeouts = (s.timeouts ?? 0) + 1;
          this.logger.error(
            { type, timeoutMs, Category: 'sp00ky-client::SqliteCacheEngine::rawCall' },
            'Local store op did not answer within its deadline; rejecting the caller'
          );
        }
        throw e;
      }
    );
    return result;
  }

  /**
   * Spawn the worker and open `bucketId`'s DB. Uses {@link rawCall} (NOT
   * {@link call}) so it can run as the body of an already-queued opQueue entry
   * without re-queuing onto itself. Callers must run it through the opQueue.
   */
  /** Seam for tests: swap in a fake transport instead of a real Worker. */
  protected createTransport(): SqliteTransport {
    return new WorkerSqliteTransport(this.logger);
  }

  private async openInternal(
    bucketId: string,
    extras?: {
      workerLockName?: string;
      openOptions?: { maxAttempts?: number; backoffMs?: number[]; disallowMemoryFallback?: boolean };
    }
  ): Promise<void> {
    if (!this.transport || this.transport.kind !== 'worker' || !this.transport.connected) {
      this.transport = this.createTransport();
      if (this.transport instanceof WorkerSqliteTransport) {
        // The worker fencing itself (leadership stolen from a frozen tab that
        // thawed) is a leader-loss: park ops until the broker re-adopts us.
        this.transport.onLockLost = (reason) => {
          this.transport = null;
          this.storeEpoch++;
          this.openRoleGate(`worker fenced: ${reason}`);
        };
      }
    }
    // Seed the `_00_*` system tables as part of `open` (worker-side, one round
    // trip). The LocalMigrator DEFINEs them, but `translateSurql` lowers every
    // DEFINE to a noop on this engine (SQLite has no DDL vocabulary), so
    // provisioning never actually creates them — they were only made lazily on
    // first WRITE. A fresh bucket (e.g. right after signup) that READS one first
    // (the sync layer selects `_00_query` before any row lands) hit
    // "no such table: _00_query" and the client wedged on "Loading database".
    // Creating them inside `open` guarantees any access order is safe without
    // adding ops to the engine's queue.
    // `opfsError` is absent from a worker bundle older than this field, which
    // just reads as "no reason given" rather than breaking the open.
    const { persisted, opfsError } = await this.rawCall<{
      persisted: boolean;
      opfsError?: string;
    }>('open', {
      dbName: bucketId,
      useOpfs: this.useOpfs,
      systemTables: SYSTEM_TABLES,
      ...(extras?.workerLockName ? { workerLockName: extras.workerLockName } : {}),
      ...(extras?.openOptions ? { openOptions: extras.openOptions } : {}),
    });
    this.knownTables.clear();
    for (const t of SYSTEM_TABLES) this.knownTables.add(t);
    this.bucketId = bucketId;
    // Durability was requested but could not be had: the store is in RAM, so it
    // loses local writes on reload and can OOM a wasm-heavy renderer. Report it
    // (the worker also console.errors, since host apps may run pino at `fatal`)
    // and publish it so the app can warn the user.
    const fellBack = this.useOpfs && !persisted;
    // Omit `error` rather than setting it to `undefined`: the devtools
    // serializer renders an undefined value as the STRING 'undefined'.
    const health: StorageHealth = {
      status: persisted ? 'persistent' : 'memory',
      fallback: fellBack,
    };
    if (fellBack && opfsError) health.error = opfsError;
    if (this.shared) health.role = this.roleLabel;
    this.setStorageHealth(health);
    const stats = getStats();
    stats.persisted = persisted;
    if (fellBack && opfsError) stats.opfsError = opfsError;
    else delete stats.opfsError;
    if (fellBack) {
      this.logger.error(
        { bucketId, opfsError, Category: 'sp00ky-client::SqliteCacheEngine::connect' },
        'SQLite OPFS persistence failed; store is IN MEMORY and will not survive reload'
      );
    } else {
      this.logger.info(
        { bucketId, persisted, Category: 'sp00ky-client::SqliteCacheEngine::connect' },
        persisted ? 'SQLite OPFS store opened' : 'SQLite in-memory store opened (as configured)'
      );
    }
  }

  /** Enqueue `fn` as a single serialized opQueue entry (mirrors {@link call}'s
   *  chaining) so it can't interleave with reads/writes at the worker. */
  private enqueue<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.opQueue.then(fn, fn);
    this.opQueue = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  async connect(bucketId: string): Promise<void> {
    // Serialize through the opQueue so an op racing boot can't dispatch to a
    // half-open worker.
    await this.enqueue(() => this.openInternal(bucketId));
  }

  async switchBucket(bucketId: string): Promise<void> {
    this.storeEpoch++;
    // Run close → terminate → reopen as ONE opQueue entry. Previously `close`
    // and the new `open` were separate entries, so a query's `exec` (e.g. the
    // query re-registration fired by an auth/bucket change) could slot in
    // between and dispatch to the just-closed worker → "sqlite: DB not open".
    // As a single entry, every other op runs fully before the close or after
    // the reopen — never against a closed DB.
    await this.enqueue(async () => {
      if (this.transport) {
        try {
          await this.rawCall('close');
        } catch {
          /* ignore */
        }
        this.transport.close('bucket switch');
        this.transport = null;
      }
      await this.openInternal(bucketId);
    });
  }

  async close(): Promise<void> {
    if (!this.transport) return;
    try {
      await this.call('close');
    } catch {
      /* ignore */
    }
    this.transport.close('engine closed');
    this.transport = null;
  }

  // ---- shared-tabs role modes ------------------------------------------------
  // The TabsCoordinator drives these; solo mode never touches them. The engine
  // object is never replaced across role changes, so `storeEpoch` stays one
  // monotonic per-tab counter and every existing fencing consumer keeps
  // working unchanged.

  /** Which role the current transport represents, for StorageHealth. */
  private roleLabel: 'leader' | 'follower' | 'solo' = 'solo';
  /** True once this engine has had a usable store at least once; role changes
   *  after that point invalidate in-flight reads and must bump the epoch. */
  private hadStore = false;

  /**
   * Role transitions run on their OWN chain, never on the opQueue: parked ops
   * sit INSIDE opQueue entries waiting for the role gate, so a transition
   * queued behind them could never run to release them (deadlock). Transitions
   * are safe off-queue because in-flight ops on a dead transport were already
   * rejected, parked ops only resume after the transition completes, and the
   * worker serializes everything worker-side anyway.
   */
  private transitionChain: Promise<unknown> = Promise.resolve();

  private chainTransition<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.transitionChain.then(fn, fn);
    this.transitionChain = result.then(
      () => undefined,
      () => undefined
    );
    return result;
  }

  private openRoleGate(reason: string): void {
    if (this.roleGate) return;
    let release!: () => void;
    let rejectFn!: (e: Error) => void;
    const promise = new Promise<void>((res, rej) => {
      release = res;
      rejectFn = rej;
    });
    // Swallow the timeout rejection for waiters that already resolved.
    promise.catch(() => {});
    this.roleGate = { promise, release };
    this.roleGateTimer = setTimeout(() => {
      // Nothing adopted us in time: reject the parked ops (retryable) and drop
      // the gate so later calls fail fast with 'not connected'.
      rejectFn(new BrokerPortClosedError(`no leader adopted this tab: ${reason}`));
      this.roleGate = null;
      this.roleGateTimer = null;
    }, 20_000);
  }

  private closeRoleGate(): void {
    if (this.roleGateTimer) clearTimeout(this.roleGateTimer);
    this.roleGateTimer = null;
    this.roleGate?.release();
    this.roleGate = null;
  }

  /**
   * Become the store owner (leader). Boot and failover share this path; a
   * failover (a previous transport existed) bumps the epoch FIRST so every
   * in-flight chain that captured the old epoch fences itself. `resumeHeld`
   * keeps the live worker after a broker restart and only rolls the
   * per-leadership lock forward.
   */
  async adoptOwner(
    bucketId: string,
    opts: {
      workerLockName: string;
      allowMemoryFallback: boolean;
      resumeHeld: boolean;
    }
  ): Promise<StorageHealth> {
    this.roleLabel = 'leader';
    return this.chainTransition(async () => {
      if (
        opts.resumeHeld &&
        this.transport?.kind === 'worker' &&
        this.transport.connected &&
        this.bucketId === bucketId
      ) {
        await this.rawCall('relock', { workerLockName: opts.workerLockName });
        this.closeRoleGate();
        return this.storageHealthValue;
      }
      if (this.transport) await this.drainInFlight();
      if (this.hadStore) this.storeEpoch++;
      if (this.transport) {
        try {
          if (this.transport.kind === 'worker') await this.rawCall('close');
        } catch {
          /* ignore */
        }
        this.transport.close('adopting ownership', new BrokerPortClosedError('adopting ownership'));
        this.transport = null;
      }
      await this.openInternal(bucketId, {
        workerLockName: opts.workerLockName,
        openOptions: {
          ...PROMOTION_OPEN_OPTIONS,
          disallowMemoryFallback: !opts.allowMemoryFallback,
        },
      });
      getStats().roleChanges = (getStats().roleChanges ?? 0) + 1;
      this.hadStore = true;
      this.closeRoleGate();
      return this.storageHealthValue;
    });
  }

  /** Attach to a leader's worker through `dbPort` (follower). */
  async adoptAttached(
    dbPort: MessagePort,
    snapshot: { bucketId: string; storageHealth: StorageHealth },
    onPortDead: (reason: string) => void
  ): Promise<void> {
    this.roleLabel = 'follower';
    return this.chainTransition(async () => {
      if (this.transport) await this.drainInFlight();
      if (this.hadStore) this.storeEpoch++;
      this.transport?.close(
        'adopting leader port',
        new BrokerPortClosedError('adopting leader port')
      );
      this.transport = new PortSqliteTransport(dbPort, onPortDead, this.logger);
      this.bucketId = snapshot.bucketId;
      // The shared store exists and is seeded; mirror the owner's bookkeeping.
      this.knownTables.clear();
      for (const t of SYSTEM_TABLES) this.knownTables.add(t);
      const health: StorageHealth = { ...snapshot.storageHealth, role: 'follower' };
      this.setStorageHealth(health);
      const stats = getStats();
      stats.persisted = snapshot.storageHealth.status === 'persistent';
      stats.roleChanges = (stats.roleChanges ?? 0) + 1;
      delete stats.opfsError;
      this.hadStore = true;
      this.closeRoleGate();
    });
  }

  /** Demoted while owning the store (zombie thaw, stale promotion): tear the
   *  worker down so its OPFS handles free up, then park until re-adopted. */
  async releaseOwnership(): Promise<void> {
    await this.chainTransition(async () => {
      if (this.transport) {
        // Let ops already at the worker answer first. Without this they die
        // with the transport — a bucket switch (moveToBucket → teardownLeader)
        // runs while the opQueue may still have a query's fetch in flight, and
        // that fetch's caller is not necessarily prepared to retry.
        await this.drainInFlight();
        try {
          if (this.transport.kind === 'worker') {
            await (this.transport as WorkerSqliteTransport).shutdown();
          }
        } catch {
          /* worker may already be fenced/dead */
        }
        // Anything still pending (drain timed out) is a deliberate teardown,
        // not a crash: fail it the way a follower's lost port does, so the
        // error text is honest and callers can treat it as retryable.
        this.transport.close(
          'ownership released',
          new BrokerPortClosedError('ownership released')
        );
        this.transport = null;
      }
      this.storeEpoch++;
      this.openRoleGate('ownership released');
    });
  }

  /** The leader (or its port) died. Called from the port-dead callback and the
   *  coordinator; NOT enqueued, so in-flight ops reject immediately instead of
   *  waiting behind whatever is stuck. */
  onLeaderLost(reason: string): void {
    if (this.transport?.kind === 'port') {
      this.transport.close(reason);
      this.transport = null;
    }
    this.storeEpoch++;
    this.openRoleGate(reason);
  }

  /** Leader side: forward a follower's dbPort into the owned worker. */
  async exposeClientPort(clientId: string, port: MessagePort): Promise<void> {
    if (this.transport?.kind !== 'worker') {
      throw new Error('SqliteCacheEngine: not the store owner');
    }
    await (this.transport as WorkerSqliteTransport).addClientPort(clientId, port);
  }

  async removeClientPort(clientId: string): Promise<void> {
    if (this.transport?.kind !== 'worker') return;
    await (this.transport as WorkerSqliteTransport).removeClientPort(clientId);
  }

  /** Graceful pagehide as owner: release OPFS handles NOW so the next leader
   *  does not race the browser's worker GC. */
  async shutdownOwnedWorker(): Promise<void> {
    if (this.transport?.kind !== 'worker') return;
    try {
      await (this.transport as WorkerSqliteTransport).shutdown();
    } catch {
      /* ignore */
    }
    this.transport.close('shutdown');
    this.transport = null;
  }

  private async ensureTable(table: string): Promise<void> {
    if (this.knownTables.has(table)) return;
    await this.call('run', {
      sql: `CREATE TABLE IF NOT EXISTS "${table}" (id TEXT PRIMARY KEY, data TEXT NOT NULL)`,
    });
    this.knownTables.add(table);
  }

  private async execRows(sql: string, bind: unknown[]): Promise<Row[]> {
    const { rows } = await this.call<{ rows: { data: string }[] }>('exec', { sql, bind });
    const s = getStats();
    const t0 = performance.now();
    const out = (rows ?? []).map((r) => {
      s.bytesParsed += r.data.length;
      return reviveRow(r.data);
    });
    s.parseMs += performance.now() - t0;
    s.rowsParsed += out.length;
    return out;
  }

  // ---- reads ---------------------------------------------------------------

  async select(plan: QueryPlan, params: Record<string, unknown> = {}): Promise<Row[]> {
    if (this.workerSelect) {
      // ONE round-trip: the worker executes the whole plan (table creation,
      // base select, relation tree, JSON parse) and returns structured-clone
      // rows. The legacy path below pays a postMessage hop per table/relation
      // level plus main-thread parsing — the dominant first-load cost.
      //
      // Normalize before postMessage: class-instance VALUES (RecordId & co) →
      // their `stableKey` string. structuredClone strips a class instance to a
      // bare plain object — and surrealdb's RecordId keeps its data behind
      // getters (no own properties), so it clones to `{}`: the worker would
      // filter on garbage. Applies to params AND to values baked inside the
      // plan (where nodes, relation sub-wheres, window ids).
      // Param KEYS must pass through untouched — `comparisonSql` resolves
      // `paramRef` via hasOwnProperty, and a dropped key silently falls back to
      // the baked literal (the crossed-results class fixed in aa4af79b).
      const normPlan = normalizePlanForClone(plan);
      const normParams: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(params)) normParams[k] = toCloneSafe(v);
      try {
        const res = await this.call<{ rows: Row[]; relationFetches?: number }>('select', {
          plan: normPlan,
          params: normParams,
        });
        getStats().relationFetches += res.relationFetches ?? 0;
        return res.rows ?? [];
      } catch (err) {
        // Stale worker script without the 'select' op: fall back for good.
        if (err instanceof Error && err.message.includes('unknown message')) {
          this.workerSelect = false;
          this.logger.warn(
            { err, Category: 'sp00ky-client::SqliteCacheEngine::select' },
            'Worker lacks select op (stale script?) — falling back to multi-hop select'
          );
        } else {
          throw err;
        }
      }
    }
    return this.selectLegacy(plan, params);
  }

  /** Pre-worker-select path: one worker round-trip per table/relation level,
   *  rows parsed on the main thread. Kept as the `workerSelect:false` escape
   *  hatch and the stale-worker fallback. */
  private async selectLegacy(
    plan: QueryPlan,
    params: Record<string, unknown> = {}
  ): Promise<Row[]> {
    // Window materialization: base rows are exactly `plan.ids`, ordered.
    if (plan.ids) {
      const result = await this.selectByIds(plan.table, plan.ids, {
        select: plan.select,
        orderBy: plan.orderBy,
      });
      await resolveRelations(result, plan.relations, this);
      return result;
    }
    await this.ensureTable(plan.table);
    const bind: unknown[] = [];
    // Projection binds land in the SELECT clause, ahead of any WHERE binds.
    const proj = plan.select ? projectedDataSql(plan.select, bind) : 'data';
    let sql = `SELECT ${proj} FROM "${plan.table}"`;
    if (plan.where && plan.where.length > 0) {
      sql += ` WHERE ${renderWhereSql(plan.where, bind, params)}`;
    }
    if (plan.orderBy && plan.orderBy.length > 0) sql += renderOrderSql(plan.orderBy);
    // Deterministic fallback, in parity with `sqlite-select.ts`: without it an
    // unordered query renders in insertion order here and in membership order
    // after the server answers, which reshuffles the list on screen.
    else sql += ` ORDER BY id`;
    if (plan.limit !== undefined) sql += ` LIMIT ${Number(plan.limit)}`;
    if (plan.offset !== undefined) sql += ` OFFSET ${Number(plan.offset)}`;
    const rows = await this.execRows(sql, bind);
    await resolveRelations(rows, plan.relations, this);
    return rows;
  }

  async fetchRelation(req: RelationFetch): Promise<Row[]> {
    getStats().relationFetches++;
    await this.ensureTable(req.table);
    const keys = req.keys.map(stableKey);
    const placeholders = keys.map(() => '?').join(', ');
    const bind: unknown[] = [...keys];
    const lhs = req.matchField === 'id' ? 'id' : `json_extract(data, '$.${req.matchField}')`;
    let sql = `SELECT data FROM "${req.table}" WHERE ${lhs} IN (${placeholders})`;
    if (req.where && req.where.length > 0) {
      sql += ` AND ${renderWhereSql(req.where, bind, {})}`;
    }
    if (req.orderBy && req.orderBy.length > 0) sql += renderOrderSql(req.orderBy);
    const rows = await this.execRows(sql, bind);
    return req.select ? rows.map((r) => project(r, req.select!)) : rows;
  }

  async selectByIds(
    table: string,
    ids: Id[],
    opts?: { select?: string[]; orderBy?: OrderBy }
  ): Promise<Row[]> {
    if (ids.length === 0) return [];
    await this.ensureTable(table);
    const keys = ids.map(stableKey);
    const placeholders = keys.map(() => '?').join(', ');
    // Projection binds land in the SELECT clause, so they go first. Must stay
    // byte-identical to the worker path in sqlite-select.ts.
    const bind: unknown[] = [];
    const dataCol = opts?.select ? projectedDataSql(opts.select, bind) : 'data';
    bind.push(...keys);
    let sql = `SELECT ${dataCol} FROM "${table}" WHERE id IN (${placeholders})`;
    if (opts?.orderBy && opts.orderBy.length > 0) sql += renderOrderSql(opts.orderBy);
    let rows = await this.execRows(sql, bind);
    if (!opts?.orderBy || opts.orderBy.length === 0) {
      const pos = new Map(keys.map((k, i) => [k, i]));
      rows = rows.sort((a, b) => (pos.get(stableKey(a.id)) ?? 0) - (pos.get(stableKey(b.id)) ?? 0));
    }
    return rows;
  }

  async getById(table: string, id: Id): Promise<Row | null> {
    await this.ensureTable(table);
    const rows = await this.execRows(`SELECT data FROM "${table}" WHERE id = ?`, [stableKey(id)]);
    return rows[0] ?? null;
  }

  /**
   * `(id, _00_rv)` of every row per table, straight off the worker: no body
   * parse, one round trip per table. A table that does not exist yet reads as
   * empty rather than failing the whole scan.
   */
  async scanVersions(tables: string[]): Promise<Record<string, [string, number][]>> {
    const out: Record<string, [string, number][]> = {};
    for (const table of tables) {
      try {
        const { rows } = await this.call<{ rows: { id: string; rv: unknown }[] }>('exec', {
          sql: `SELECT id, json_extract(data, '$._00_rv') AS rv FROM "${table.replace(/"/g, '""')}"`,
        });
        out[table] = (rows ?? []).map((r) => [r.id, Number(r.rv) || 0]);
      } catch {
        out[table] = [];
      }
    }
    return out;
  }

  async getSnapshot(key: string): Promise<StoredSnapshot | null> {
    const { rows } = await this.call<{ rows: { id: string; data: unknown }[] }>('exec', {
      sql: `SELECT id, data FROM "${SNAPSHOT_TABLE}" WHERE id IN (?, ?)`,
      bind: [key, `${key}:meta`],
    });
    let bytes: Uint8Array | null = null;
    let meta: SnapshotMeta | null = null;
    for (const r of rows ?? []) {
      if (r.id === key) {
        if (r.data instanceof Uint8Array) bytes = r.data;
        else if (r.data instanceof ArrayBuffer) bytes = new Uint8Array(r.data);
        else if (typeof r.data === 'string') bytes = new TextEncoder().encode(r.data);
      } else if (typeof r.data === 'string') {
        try {
          meta = JSON.parse(r.data) as SnapshotMeta;
        } catch {
          meta = null;
        }
      }
    }
    return bytes && meta ? { bytes, meta } : null;
  }

  async putSnapshot(key: string, bytes: Uint8Array, meta: SnapshotMeta): Promise<void> {
    // One transaction, so a reader never sees new bytes with old meta.
    await this.call('batch', [
      {
        sql: `INSERT OR REPLACE INTO "${SNAPSHOT_TABLE}" (id, data) VALUES (?, ?)`,
        bind: [key, bytes],
      },
      {
        sql: `INSERT OR REPLACE INTO "${SNAPSHOT_TABLE}" (id, data) VALUES (?, ?)`,
        bind: [`${key}:meta`, JSON.stringify(meta)],
      },
    ]);
  }

  async deleteSnapshot(key: string): Promise<void> {
    await this.call('run', {
      sql: `DELETE FROM "${SNAPSHOT_TABLE}" WHERE id IN (?, ?)`,
      bind: [key, `${key}:meta`],
    });
  }

  // ---- writes --------------------------------------------------------------

  async upsert(table: string, id: Id, data: Row, mode: 'replace' | 'merge'): Promise<void> {
    await this.ensureTable(table);
    const key = stableKey(id);
    if (mode === 'merge') {
      // Merge in-SQL via json_patch (RFC7396 = MERGE semantics): on insert store
      // the row, on conflict shallow-merge. Serialize once, reuse for VALUES and
      // the patch. No read-modify-write round-trip. (RFC7396: null deletes key.)
      const full = serializeRow({ ...data, id: key });
      await this.call('run', {
        sql: `INSERT INTO "${table}"(id, data) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET data = json_patch(data, ?)`,
        bind: [key, full, full],
      });
      return;
    }
    await this.call('run', {
      sql: `INSERT INTO "${table}"(id, data) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data`,
      bind: [key, serializeRow({ ...data, id: key })],
    });
  }

  async patch(table: string, id: Id, patches: unknown[]): Promise<void> {
    // RFC6902 (fast-json-patch) applied read-modify-write — SQLite's json_patch
    // is RFC7396 merge-patch and would misinterpret op arrays.
    const existing = (await this.getById(table, id)) ?? { id: stableKey(id) };
    const next = applyPatch(existing, patches as Operation[]).newDocument as Row;
    await this.upsert(table, id, next, 'replace');
  }

  async delete(table: string, id: Id): Promise<void> {
    await this.ensureTable(table);
    await this.call('run', { sql: `DELETE FROM "${table}" WHERE id = ?`, bind: [stableKey(id)] });
  }

  // ---- SurrealQL-vocabulary shim (LocalStore compatibility) ---------------

  /**
   * Execute a raw SurrealQL statement by translating the client's bounded
   * vocabulary to verbs (see `surql-translate.ts`). Returns results shaped like
   * SurrealDB's `.query()` (one element per statement; a tx prepends a `null`
   * begin-result so `surql.seal` extraction lines up). Epoch-fences writes.
   */
  async query<T extends unknown[]>(
    sql: string,
    vars: Record<string, unknown> = {},
    opts?: { epoch?: number }
  ): Promise<T> {
    if (opts?.epoch !== undefined && opts.epoch !== this.storeEpoch) throw new StaleEpochError();
    const start = performance.now();
    try {
      const { transaction, ops } = translateSurql(sql, vars);
      let shaped: unknown[];
      // FAST PATH: a pure-write transaction (bulk sync-down is one
      // `tx([upsertMerge…])`) compiles to a SINGLE worker `batch` message run in
      // one SQLite transaction — instead of 1-2 worker round-trips PER row. This
      // is the dominant sync-down cost; per-op execution here caused the churn
      // OOM. Mixed txs (LET/RETURN single-record mutations) keep the per-op path.
      if (
        transaction &&
        ops.every(
          (o) =>
            o.kind === 'upsert' ||
            o.kind === 'delete' ||
            o.kind === 'deleteAll' ||
            o.kind === 'noop'
        )
      ) {
        await this.runWriteBatch(ops);
        // Shape each statement's result the SAME as the per-op path (`execOp`),
        // so a caller reading a statement's output still works after taking the
        // batch fast path. A single `create()` compiles to an all-upsert tx
        // (createSet + createMutation) and reads `resultIndex:0` for the new
        // row + its id; returning empty arrays here dropped that row (id became
        // undefined → the reconcile crashed in `encodeRecordId`). The
        // single-batch write is kept — this only rebuilds the return value.
        shaped = [null, ...ops.map(pureWriteOpResult)];
      } else {
        const results: unknown[] = [];
        // Per-query scope holds `LET $var = (...)` bindings for later statements
        // (e.g. `RETURN { target: $updated }`).
        const scope: Record<string, unknown> = {};
        for (const op of ops) results.push(await this.execOp(op, scope, vars));
        shaped = transaction ? [null, ...results] : results;
      }
      this.events.emit(DatabaseEventTypes.LocalQuery, {
        query: sql,
        vars,
        duration: performance.now() - start,
        success: true,
        timestamp: Date.now(),
      });
      return shaped as unknown as T;
    } catch (err) {
      this.events.emit(DatabaseEventTypes.LocalQuery, {
        query: sql,
        vars,
        duration: performance.now() - start,
        success: false,
        error: err instanceof Error ? err.message : String(err),
        timestamp: Date.now(),
      });
      throw err;
    }
  }

  async execute<R>(
    query: SealedQuery<R>,
    vars?: Record<string, unknown>,
    opts?: { epoch?: number }
  ): Promise<R> {
    const raw = await this.query<unknown[]>(query.sql, vars, opts);
    return query.extract(raw);
  }

  queryUngated<T extends unknown[]>(sql: string, vars?: Record<string, unknown>): Promise<T> {
    return this.query<T>(sql, vars);
  }

  private async execOp(
    op: SqlOp,
    scope: Record<string, unknown>,
    vars: Record<string, unknown>
  ): Promise<unknown> {
    switch (op.kind) {
      case 'getById': {
        const row = await this.getById(tableOf(op.id), op.id);
        if (op.value) return row ? (row[op.value] ?? null) : null;
        return row ? (op.select ? project(row, op.select) : row) : null;
      }
      case 'selectByIds': {
        if (op.ids.length === 0) return [];
        let rows = await this.selectByIds(tableOf(op.ids[0]), op.ids, {
          select: op.select,
          orderBy: op.orderBy,
        });
        // Windowed in JS, not SQL: the id list is already the whole result set,
        // and its order is restored after the fetch (see selectByIds).
        if (op.start !== undefined || op.limit !== undefined) {
          const from = op.start ?? 0;
          rows = rows.slice(from, op.limit === undefined ? undefined : from + op.limit);
        }
        if (op.value) return rows.map((r) => r[op.value!]);
        return rows;
      }
      case 'selectTable': {
        const rows = await this.rawSelectTable(op.table, op.where, op.orderBy, {
          limit: op.limit,
          start: op.start,
        });
        if (op.value) return rows.map((r) => r[op.value!]);
        return op.select ? rows.map((r) => project(r, op.select!)) : rows;
      }
      case 'count': {
        await this.ensureTable(op.table);
        const bind: unknown[] = [];
        let sql = `SELECT COUNT(*) AS n FROM "${op.table}"`;
        if (op.where && op.where.length > 0) sql += ` WHERE ${renderWhereSql(op.where, bind, {})}`;
        const { rows } = await this.call<{ rows: { n: number }[] }>('exec', { sql, bind });
        // `GROUP ALL` collapses to a single `{ count }` row on SurrealDB; match
        // it exactly so callers can read `rows[0].count` on either engine.
        return [{ count: rows?.[0]?.n ?? 0 }];
      }
      case 'infoForDb': {
        const { rows } = await this.call<{ rows: { name: string }[] }>('exec', {
          sql: "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        });
        // SurrealDB answers with the DEFINE statement per table. Nothing here
        // parses it (the DevTools explorer only reads the keys), so a truthful
        // schemaless stand-in beats inventing field definitions.
        const tables: Record<string, string> = {};
        for (const r of rows ?? []) tables[r.name] = `DEFINE TABLE ${r.name} SCHEMALESS`;
        return { tables, analyzers: {}, functions: {}, params: {}, users: {} };
      }
      case 'upsert':
        await this.upsert(tableOf(op.id), op.id, op.data, op.mode);
        // Cheap return — no read-back. The full merged row is only needed by a
        // LET-wrapped upsert, which reads it back in the 'let' case below. This
        // avoids an extra worker round-trip + full-row parse on EVERY sync-down
        // write (the hot path under rapid churn). Shared with the batch fast
        // path so the two never drift.
        return pureWriteOpResult(op);
      case 'updateSet': {
        const existing = (await this.getById(tableOf(op.id), op.id)) ?? { id: stableKey(op.id) };
        for (const { path, op: setOp, value } of op.sets) {
          if (setOp === '+=' || setOp === '-=') {
            const cur = Number(getPath(existing, path) ?? 0);
            const delta = Number(value ?? 0);
            setPath(existing, path, setOp === '+=' ? cur + delta : cur - delta);
          } else {
            setPath(existing, path, value);
          }
        }
        await this.upsert(tableOf(op.id), op.id, existing, 'replace');
        return op.returnNone ? null : existing;
      }
      case 'delete':
        await this.delete(tableOf(op.id), op.id);
        return pureWriteOpResult(op);
      case 'deleteAll':
        await this.ensureTable(op.table);
        await this.call('run', { sql: `DELETE FROM "${op.table}"` });
        return pureWriteOpResult(op);
      case 'let': {
        let result = await this.execOp(op.inner, scope, vars);
        // A LET-bound UPSERT must expose the FULL merged row (e.g.
        // `RETURN { target: $updated }`), so read it back here — only here,
        // not on every upsert.
        if (op.inner.kind === 'upsert') {
          result = (await this.getById(tableOf(op.inner.id), op.inner.id)) ?? result;
        }
        scope[op.var] = result;
        return result;
      }
      case 'return': {
        const obj: Row = {};
        for (const { key, var: v } of op.entries) {
          obj[key] = v in scope ? scope[v] : vars[v];
        }
        return obj;
      }
      case 'noop':
        return pureWriteOpResult(op);
    }
  }

  /**
   * Compile a pure-write op list to ONE worker `batch` (single SQLite
   * transaction). Ensures each touched table exists, then one statement per op.
   * Merges happen in-SQL via json_patch — no read-back round-trips.
   */
  private async runWriteBatch(ops: SqlOp[]): Promise<void> {
    const stmts: { sql: string; bind?: unknown[] }[] = [];
    const tables = new Set<string>();
    const ensure = (t: string) => {
      if (!tables.has(t)) {
        tables.add(t);
        this.knownTables.add(t);
        stmts.push({
          sql: `CREATE TABLE IF NOT EXISTS "${t}" (id TEXT PRIMARY KEY, data TEXT NOT NULL)`,
        });
      }
    };
    for (const op of ops) {
      if (op.kind === 'upsert') {
        const t = tableOf(op.id);
        const key = stableKey(op.id);
        ensure(t);
        if (op.mode === 'merge') {
          // Serialize ONCE and reuse for both VALUES (fresh insert) and the
          // json_patch (merge). Patching with `id` is a harmless no-op set, so
          // the full row doubles as the delta — halves per-row stringify cost.
          const full = serializeRow({ ...op.data, id: key });
          stmts.push({
            sql: `INSERT INTO "${t}"(id, data) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET data = json_patch(data, ?)`,
            bind: [key, full, full],
          });
        } else {
          stmts.push({
            sql: `INSERT INTO "${t}"(id, data) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data`,
            bind: [key, serializeRow({ ...op.data, id: key })],
          });
        }
      } else if (op.kind === 'delete') {
        const t = tableOf(op.id);
        ensure(t);
        stmts.push({ sql: `DELETE FROM "${t}" WHERE id = ?`, bind: [stableKey(op.id)] });
      } else if (op.kind === 'deleteAll') {
        ensure(op.table);
        stmts.push({ sql: `DELETE FROM "${op.table}"` });
      }
    }
    if (stmts.length > 0) await this.call('batch', stmts);
  }

  private async rawSelectTable(
    table: string,
    where?: WhereNode[],
    orderBy?: OrderBy,
    window?: { limit?: number; start?: number }
  ): Promise<Row[]> {
    await this.ensureTable(table);
    const bind: unknown[] = [];
    let sql = `SELECT data FROM "${table}"`;
    if (where && where.length > 0) sql += ` WHERE ${renderWhereSql(where, bind, {})}`;
    if (orderBy && orderBy.length > 0) sql += renderOrderSql(orderBy);
    // SQLite has no bare OFFSET, so a START without a LIMIT needs `LIMIT -1`
    // (its documented "no limit" sentinel) to stay valid SQL.
    if (window?.limit !== undefined) sql += ` LIMIT ${Number(window.limit)}`;
    else if (window?.start !== undefined) sql += ' LIMIT -1';
    if (window?.start !== undefined) sql += ` OFFSET ${Number(window.start)}`;
    return this.execRows(sql, bind);
  }

  async transaction<T>(fn: (tx: EngineTx) => Promise<T>): Promise<T> {
    // Verbs run in order on the single worker message channel. `patch` needs a
    // read-modify-write round-trip, so a single BEGIN/COMMIT batch cannot wrap
    // the whole closure; sequential execution is sufficient for the current
    // single-record write sites.
    const tx: EngineTx = {
      upsert: (t, id, data, mode) => this.upsert(t, id, data, mode),
      patch: (t, id, p) => this.patch(t, id, p),
      delete: (t, id) => this.delete(t, id),
    };
    return fn(tx);
  }
}

// ==================== instrumentation ====================

interface SqliteStats {
  roundTrips: number;
  batchStatements: number;
  maxBatch: number;
  inFlight: number;
  maxInFlight: number;
  byType: Record<string, number>;
  /** Worker round trips that hit `localOpTimeoutMs` (see rawCall). */
  timeouts?: number;
  /** Time ops spent waiting behind the opQueue before dispatch. */
  queueWaitMs: number;
  /** Time inside the worker's message handler (actual SQLite work). */
  workerMs: number;
  /** Round-trip time minus workerMs: postMessage + scheduling overhead. */
  rpcOverheadMs: number;
  /** Main-thread JSON parse/revive of returned rows. */
  parseMs: number;
  rowsParsed: number;
  bytesParsed: number;
  /** Relation-resolver fan-out fetches (one worker round-trip each). */
  relationFetches: number;
  /** Whether the open store is OPFS-backed. `false` here with an `opfsError`
   *  means the whole dataset is sitting in RAM. Optional so it stays absent
   *  until the first open (and is skipped by the backfill loop below). */
  persisted?: boolean;
  /** Why OPFS persistence failed, when it did. */
  opfsError?: string;
  /** Shared-tabs follower: ops that crossed the leader's MessagePort. */
  proxiedOps?: number;
  /** Times this tab's engine changed hands (promotions + attachments). */
  roleChanges?: number;
}

const EMPTY_STATS: SqliteStats = {
  roundTrips: 0,
  batchStatements: 0,
  maxBatch: 0,
  inFlight: 0,
  maxInFlight: 0,
  byType: {},
  queueWaitMs: 0,
  workerMs: 0,
  rpcOverheadMs: 0,
  parseMs: 0,
  rowsParsed: 0,
  bytesParsed: 0,
  relationFetches: 0,
};

/** Live stats, inspectable in the browser console via `__sqliteStats`. Counts
 *  worker round-trips (the sync-down cost driver), batch sizes, queue depth
 *  (`maxInFlight`), and the latency split of each round-trip (queue wait vs
 *  worker time vs RPC overhead vs main-thread row parsing) so first-load cost
 *  can be measured rather than guessed. */
function getStats(): SqliteStats {
  const g = globalThis as unknown as { __sqliteStats?: SqliteStats };
  if (!g.__sqliteStats) {
    g.__sqliteStats = { ...EMPTY_STATS, byType: {} };
  } else {
    // Backfill fields added since the object was created (HMR / older bundle).
    for (const [k, v] of Object.entries(EMPTY_STATS)) {
      if ((g.__sqliteStats as any)[k] === undefined) (g.__sqliteStats as any)[k] = v;
    }
  }
  return g.__sqliteStats;
}

// SQL rendering + row (de)serialization live in `sqlite-plan-sql.ts`, shared
// with the worker (worker-side plan execution renders the same SQL).

// ==================== structured-clone normalization ====================

/**
 * Make a bind/param value safe to cross the worker boundary. structuredClone
 * keeps plain data intact but strips a CLASS instance to a bare object — and
 * surrealdb's `RecordId` stores its fields behind getters (zero own
 * properties), so it clones to `{}`. Convert such instances to their
 * `stableKey` string (the exact value `scalar()` would bind on the main
 * thread), leave everything clone-representable untouched.
 */
function toCloneSafe(v: unknown): unknown {
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v) || v instanceof Uint8Array || v instanceof Date) return v;
  const proto = Object.getPrototypeOf(v);
  if (proto === Object.prototype || proto === null) return v;
  return stableKey(v);
}

function normalizeWhereForClone(nodes: WhereNode[] | undefined): WhereNode[] | undefined {
  if (!nodes) return nodes;
  return nodes.map((n) =>
    'or' in n
      ? { or: n.or.map((c) => ({ ...c, value: toCloneSafe(c.value) })) }
      : { ...n, value: toCloneSafe(n.value) }
  );
}

function normalizeRelationForClone(r: RelationPlan): RelationPlan {
  return {
    ...r,
    where: normalizeWhereForClone(r.where),
    relations: r.relations?.map(normalizeRelationForClone),
  };
}

/** Normalize every baked value a plan carries (where trees, window ids) for
 *  the postMessage to the worker's `select` op. */
function normalizePlanForClone(plan: QueryPlan): QueryPlan {
  return {
    ...plan,
    ids: plan.ids?.map(stableKey),
    where: normalizeWhereForClone(plan.where),
    relations: plan.relations?.map(normalizeRelationForClone),
  };
}
