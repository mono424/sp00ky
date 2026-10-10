// oxlint-disable-next-line no-named-as-default -- WASM module default export convention
import init, { Sp00kyProcessor } from '@spooky-sync/ssp-wasm';
import type { EventDefinition, EventSystem } from '../../events/index';
import type { Logger } from 'pino';
import type { LocalStore } from '../database/index';
import type { WasmProcessor, WasmStreamUpdate } from './wasm-types';
import type { Duration } from 'surrealdb';
import type { QueryTimeToLive, RecordVersionArray } from '../../types';
import { encodeRecordId } from '../../utils/index';
import { recordIdKey } from '../database/relation-resolver';

// Simple interface for query plan registration (replaces Incantation class)
interface QueryPlanConfig {
  queryHash: string;
  surql: string;
  params: Record<string, any>;
  ttl: QueryTimeToLive | Duration;
  lastActiveAt: Date;
  localArray: RecordVersionArray;
  remoteArray: RecordVersionArray;
  meta: {
    tableName: string;
    involvedTables?: string[];
  };
}

// Define the shape of an update from the Wasm module
// Matches MaterializedViewUpdate struct
export interface StreamUpdate {
  queryHash: string;
  localArray: RecordVersionArray;
  op?: 'CREATE' | 'UPDATE' | 'DELETE'; // Operation type for conditional debouncing
  /**
   * Client-internal: not from the circuit. A membership-only change that
   * needed no fetch is re-materialized through this same path so it cannot
   * race a real update (DataModule.scheduleRematerialize). Carries the last
   * known `localArray`; consumers that describe an INGEST (persist, metrics,
   * devtools events) skip it.
   */
  synthetic?: boolean;
  /**
   * End-to-end ingest latency for the WASM call that produced this update,
   * in milliseconds. Populated by StreamProcessorService.ingest. Undefined
   * for the initial register_view snapshot.
   */
  materializationTimeMs?: number;
  /** SSP internal sub-phase timings (ms) for this ingest, from the WASM binding. */
  storeApplyMs?: number;
  circuitStepMs?: number;
  transformMs?: number;
  /**
   * One-shot registration timings (ms). Only set on the StreamUpdate returned
   * by `registerQueryPlan` (the register_view snapshot), not on ingest updates.
   */
  registration?: { parseMs: number; planMs: number; snapshotMs: number };
}

// Define events map (kept for DevTools compatibility)
export type StreamProcessorEvents = {
  stream_update: EventDefinition<'stream_update', StreamUpdate[]>;
};
/**
 * Interface for receiving stream updates directly.
 * Implemented by DataManager and DevToolsService for direct coupling.
 */
export interface StreamUpdateReceiver {
  onStreamUpdate(update: StreamUpdate): void;
}

/** One row change in the shape `ingestMany` consumes. */
export interface IngestRecord {
  table: string;
  /** `MERGE` overlays the given fields on the stored row (projection widening). */
  op: 'CREATE' | 'UPDATE' | 'DELETE' | 'MERGE';
  id: string;
  record: any;
}

/**
 * What the boot-time prime needs from the client: which tables to walk, how
 * to recognise a snapshot written under a different schema, and which rows'
 * local `_00_rv` must not be reported as the server's.
 */
export interface CircuitPrimeContext {
  tables: string[];
  schemaHash: string;
  /** Encoded ids with an unsettled local mutation (their `_00_rv` was bumped
   *  locally and may exceed the server's next version). */
  pendingIds: Set<string>;
  /** Receives every `(id, rv)` the prime put into the circuit, per table, so
   *  the sync layer can skip re-downloading bodies it already has. */
  onVersions?: (table: string, entries: [string, number][]) => void;
}

/** Storage key of the circuit snapshot inside the local store. */
export const CIRCUIT_SNAPSHOT_KEY = 'circuit';
/** Bump when the bytes `load_store_state` reads change shape. */
export const CIRCUIT_SNAPSHOT_FORMAT = 1;
/** Rows per `ingest_many` call. Bounds the transient the wasm side allocates
 *  to parse a batch (measured: one 7700-row call of 20 KB bodies peaked at
 *  500 MB, 128-row chunks at 335 MB, and under projection at 24 MB). */
const INGEST_CHUNK = 128;
/** Ids per `selectByIds` when priming bodies out of the local store. */
const PRIME_CHUNK = 256;
/** Rows the SurrealDB (main-thread, IndexedDB) engine is allowed to prime
 *  through; above this the circuit boots empty as it always did. */
const SURREAL_PRIME_ROW_CAP = 20_000;
/** Below this many changed rows the checkpoint timer stays quiet. */
const CHECKPOINT_MIN_ROWS = 50;

/** Stable `table:id` for a row id that may already be a string. */
function idString(id: unknown): string {
  return typeof id === 'string' ? id : encodeRecordId(id as any);
}

function chunks<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

export class StreamProcessorService {
  private logger: Logger;
  private processor: WasmProcessor | undefined;
  private isInitialized = false;
  private receivers: StreamUpdateReceiver[] = [];
  // When true, `notifyUpdates` coalesces updates into `batchBuffer` (keyed by
  // queryHash) instead of dispatching them. Used to collapse the per-record
  // stream updates produced by a batched ingest into a single notification per
  // query, so the UI updates once after the whole batch rather than row-by-row.
  private batching = false;
  private batchBuffer: Map<string, StreamUpdate> = new Map();
  // Current session's auth identity, injected into every `register_view`'s
  // params so the in-browser SSP can resolve `$auth`/`$access` in table
  // permission predicates (mirrors the server's `fn::query::register`). Empty
  // strings when logged out — a non-null `auth` keeps `permission_inject` from
  // rejecting `$auth`-gated tables; the predicate just degrades to its public
  // branch. Set via `setSessionAuth` on every auth state change.
  private sessionAuth: { authId: string; access: string } = { authId: '', access: '' };
  // Bumped by `reset()`. Anything captured before a reset (a checkpoint's
  // bytes, a prime's chunk, a widening read) must not land on the NEW
  // processor, which belongs to a different bucket.
  private stateGeneration = 0;
  // Shared-tabs: follower circuits are in-memory ONLY. Only the tab that owns
  // the store writes its snapshot; a promoted follower flips this back on.
  private persistState = true;
  // Snapshot persistence. Store-only bytes, written by the checkpoint timer
  // and on the page going hidden, never per ingest: `save_store_state` walks
  // every row. The snapshot is what lets a reload restore the circuit and
  // step in only what changed since (`reconcile`), instead of re-downloading
  // and re-ingesting the whole working set.
  private persistCircuit = false;
  private checkpointMs = 30_000;
  private checkpointTimer: ReturnType<typeof setInterval> | null = null;
  private snapshotDirty = false;
  private dirtyRows = 0;
  private hideHandler: (() => void) | null = null;
  private checkpointInFlight: Promise<void> | null = null;
  // Keep only the fields registered plans evaluate per stored row. The client
  // renders bodies from the local store, so a 20 KB game row in the circuit
  // is ~150 B of predicate/sort fields under projection.
  private projection = true;
  // Prime state: `primed` resolves when the boot-time prime (snapshot restore
  // + reconcile, or a full read of the local store) has finished, aborted or
  // been skipped. Sync waits on it before its first diff.
  private primed: Promise<void> = Promise.resolve();
  private schemaHash: string | null = null;
  // Projection widening, serialised: fields a new view evaluates that stored
  // rows lack are merged in table by table.
  private widenQueue: Promise<void> = Promise.resolve();
  private widenPending: Map<string, Set<string>> = new Map();

  constructor(
    public events: EventSystem<StreamProcessorEvents>,
    private db: LocalStore,
    logger: Logger
  ) {
    this.logger = logger.child({ name: 'StreamProcessorService' });
  }

  /**
   * Add a receiver for stream updates.
   * Multiple receivers can be registered (DataManager, DevTools, etc.)
   */
  addReceiver(receiver: StreamUpdateReceiver) {
    this.receivers.push(receiver);
  }

  private notifyUpdates(updates: StreamUpdate[]) {
    if (this.batching) {
      // Coalesce by queryHash instead of dispatching. The WASM `result_data`
      // (localArray) is the full materialized array, so last-write-wins
      // already reflects every prior ingest in the batch. We sum the
      // materialization times so the single recorded sample reflects the
      // batch's total work, and emit `op: 'CREATE'` on flush so the coalesced
      // update takes DataModule's immediate (non-debounced) path.
      for (const update of updates) {
        const prev = this.batchBuffer.get(update.queryHash);
        const sum = (a?: number, b?: number) => (a ?? 0) + (b ?? 0);
        this.batchBuffer.set(update.queryHash, {
          ...update,
          op: 'CREATE',
          materializationTimeMs: sum(prev?.materializationTimeMs, update.materializationTimeMs),
          storeApplyMs: sum(prev?.storeApplyMs, update.storeApplyMs),
          circuitStepMs: sum(prev?.circuitStepMs, update.circuitStepMs),
          transformMs: sum(prev?.transformMs, update.transformMs),
        });
      }
      return;
    }

    this.dispatchUpdates(updates);
  }

  private dispatchUpdates(updates: StreamUpdate[]) {
    for (const update of updates) {
      for (const receiver of this.receivers) {
        receiver.onStreamUpdate(update);
      }
    }
  }

  /**
   * Ingest a batch of record changes, firing one coalesced `StreamUpdate` per
   * affected query once every record has been ingested. Use this whenever
   * multiple records land at once (sync fetching N rows, the boot prime).
   *
   * The batch is fed to the wasm side in chunks of {@link INGEST_CHUNK}: one
   * circuit step per chunk (a step walks every registered view, so per-record
   * ingest paid that fixed cost N times), but never the whole batch at once,
   * because the wasm side has to hold every parsed row of a call at the same
   * time and wasm32 dlmalloc never returns that peak.
   *
   * Returns the records that were ingested. A chunk that fails is reported
   * and skipped, not retried (a retry would double-apply whatever the failed
   * step already committed), and the remaining chunks still run.
   */
  ingestMany(records: IngestRecord[]): IngestRecord[] {
    if (records.length === 0) return [];

    if (!this.processor) {
      this.logger.warn(
        { Category: 'sp00ky-client::StreamProcessorService::ingestMany' },
        'Not initialized, skipping ingest'
      );
      return [];
    }

    const bulkIngest = this.processor.ingest_many;
    if (typeof bulkIngest !== 'function') {
      // Stale wasm build: per-record steps, still coalesced into one update.
      this.beginCoalescing();
      try {
        for (const record of records) {
          this.ingest(record.table, record.op, record.id, record.record);
        }
      } finally {
        this.flushCoalescing();
      }
      return records;
    }

    const ingested: IngestRecord[] = [];
    this.beginCoalescing();
    try {
      for (const chunk of chunks(records, INGEST_CHUNK)) {
        try {
          const items = chunk.map((record) => ({
            table: record.table,
            op: record.op,
            id: record.id,
            record: this.normalizeValue(record.record),
          }));
          const t0 = performance.now();
          const rawUpdates = bulkIngest.call(this.processor, items) ?? [];
          const materializationTimeMs = performance.now() - t0;
          if (rawUpdates.length > 0) {
            this.notifyUpdates(
              rawUpdates.map((u: WasmStreamUpdate) => ({
                queryHash: u.query_id,
                localArray: u.result_data,
                op: 'CREATE' as const,
                materializationTimeMs,
                storeApplyMs: u.timing_store_apply_ms,
                circuitStepMs: u.timing_circuit_step_ms,
                transformMs: u.timing_transform_ms,
              }))
            );
          }
          ingested.push(...chunk);
        } catch (e) {
          this.logger.error(
            { error: e, count: chunk.length, Category: 'sp00ky-client::StreamProcessorService::ingestMany' },
            'Ingesting chunk into ssp failed'
          );
        }
      }
    } finally {
      this.flushCoalescing();
    }
    this.logger.debug(
      { count: records.length, ingested: ingested.length, Category: 'sp00ky-client::StreamProcessorService::ingestMany' },
      'Ingested batch into ssp'
    );
    this.markSnapshotDirty(ingested.length);
    return ingested;
  }

  /**
   * Open a coalescing window. While open, the per-record stream updates
   * emitted by `ingest` are buffered (one entry per queryHash) instead of
   * dispatched. Always paired with `flushCoalescing()` in a try/finally by
   * `ingestMany` so the window always closes — otherwise the processor stays
   * stuck buffering forever.
   *
   * No-op if a window is already open (nested batches aren't expected here).
   */
  private beginCoalescing() {
    if (this.batching) return;
    this.batching = true;
    this.batchBuffer.clear();
  }

  /**
   * Close the coalescing window and flush: dispatch one coalesced
   * `StreamUpdate` per buffered queryHash, then persist processor state once
   * for the whole batch (instead of once per ingest).
   */
  private flushCoalescing() {
    if (!this.batching) return;
    this.batching = false;
    const buffered = Array.from(this.batchBuffer.values());
    this.batchBuffer.clear();
    if (buffered.length > 0) {
      this.dispatchUpdates(buffered);
    }
  }

  /**
   * Initialize the WASM module and processor.
   * This must be called before using other methods.
   */
  async init() {
    if (this.isInitialized) return;

    this.logger.info(
      { Category: 'sp00ky-client::StreamProcessorService::init' },
      'Initializing WASM...'
    );
    try {
      await init(); // Initialize the WASM module (web target)
      // We cast the generated Sp00kyProcessor to our interface which is safer
      this.processor = new Sp00kyProcessor() as unknown as WasmProcessor;
      this.applyProjection();

      this.isInitialized = true;
      this.logger.info(
        { Category: 'sp00ky-client::StreamProcessorService::init' },
        'Initialized successfully'
      );
    } catch (e) {
      this.logger.error(
        { error: e, Category: 'sp00ky-client::StreamProcessorService::init' },
        'Failed to initialize'
      );
      throw e;
    }
  }

  /**
   * Drop the current WASM processor and start a fresh, empty circuit. Used on
   * local-bucket switches: the old circuit holds the previous user's rows AND
   * views registered with the previous `$auth` context, so neither may survive.
   * Deliberately loads nothing: the snapshot in the store being swapped away
   * from belongs to the previous bucket; the caller primes the new bucket's
   * circuit (`primeFromLocal`) once its store is open, and the DataModule
   * rebind re-registers every live view against this fresh processor. Caller
   * must re-seed `setPermissions` afterwards (a fresh circuit default-denies
   * every table).
   */
  async reset(): Promise<void> {
    if (!this.isInitialized) return;
    this.stateGeneration++;
    this.batching = false;
    this.batchBuffer.clear();
    this.snapshotDirty = false;
    this.dirtyRows = 0;
    this.widenPending.clear();
    const previous = this.processor;
    this.processor = new Sp00kyProcessor() as unknown as WasmProcessor;
    this.applyProjection();
    this.freeProcessor(previous);
    this.logger.info(
      { Category: 'sp00ky-client::StreamProcessorService::reset' },
      'Stream processor reset (fresh circuit)'
    );
  }

  /**
   * Release the wasm circuit and stop checkpointing. Call when the client is
   * torn down; a recreated client (provider remount, HMR) would otherwise stack
   * one full circuit per instance.
   */
  dispose(): void {
    this.stopCheckpoints();
    const previous = this.processor;
    this.processor = undefined;
    this.isInitialized = false;
    this.batching = false;
    this.batchBuffer.clear();
    this.receivers = [];
    this.freeProcessor(previous);
  }

  /**
   * Explicitly run the wasm-bindgen destructor. Guarded: stale wasm builds may
   * not expose `free`, and a double free must not take the app down.
   */
  private freeProcessor(processor: WasmProcessor | undefined): void {
    if (!processor || typeof processor.free !== 'function') return;
    try {
      processor.free();
    } catch (e) {
      this.logger.debug(
        { error: e, Category: 'sp00ky-client::StreamProcessorService::freeProcessor' },
        'Failed to free previous wasm circuit'
      );
    }
  }

  /** Toggle circuit-state persistence (shared-tabs follower/leader role). */
  setPersistenceEnabled(enabled: boolean): void {
    this.persistState = enabled;
    if (!enabled) this.stopCheckpoints();
  }

  /**
   * Snapshot persistence (`persistCircuit`). When on, the circuit's store is
   * written to the local store on a checkpoint interval and when the page
   * goes hidden, and restored by {@link primeFromLocal} on the next boot.
   */
  configureCircuitPersistence(enabled: boolean, checkpointMs?: number): void {
    this.persistCircuit = enabled;
    if (checkpointMs && checkpointMs > 0) this.checkpointMs = checkpointMs;
    if (!enabled) this.stopCheckpoints();
  }

  /**
   * Field projection (`circuitProjection`, default on). Takes effect on the
   * next processor (`init`/`reset`) and on rows written after that.
   */
  configureProjection(enabled: boolean): void {
    this.projection = enabled;
    this.applyProjection();
  }

  private applyProjection(): void {
    if (!this.processor || typeof this.processor.set_projection !== 'function') return;
    try {
      this.processor.set_projection(this.projection);
    } catch (e) {
      this.logger.warn(
        { error: e, Category: 'sp00ky-client::StreamProcessorService::applyProjection' },
        'set_projection failed'
      );
    }
  }

  /** Resolves once the boot-time prime has finished (or was skipped). */
  whenPrimed(): Promise<void> {
    return this.primed;
  }

  /**
   * Fill the circuit from the LOCAL store, in the background.
   *
   * With a usable snapshot: install it under whatever views have registered
   * meanwhile (`load_store_state` re-primes them), then `reconcile` each table
   * against the store's `(id, rv)` list so rows deleted since the checkpoint
   * are stepped out and only rows added or changed since are read back and
   * ingested. Without one: read every row and ingest it, chunked.
   *
   * Either way the circuit ends up equal to the local store without touching
   * the network, so the first sync diff is a real delta rather than "fetch
   * everything". The returned promise never rejects; `whenPrimed` gates on it.
   */
  primeFromLocal(ctx: CircuitPrimeContext): Promise<void> {
    this.schemaHash = ctx.schemaHash;
    const run = this.runPrime(ctx).catch((e) => {
      this.logger.warn(
        { error: e, Category: 'sp00ky-client::StreamProcessorService::primeFromLocal' },
        'Circuit prime failed; the circuit fills from sync instead'
      );
    });
    this.primed = run;
    return run;
  }

  private async runPrime(ctx: CircuitPrimeContext): Promise<void> {
    const processor = this.processor;
    if (!processor || typeof this.db.scanVersions !== 'function') return;
    const generation = this.stateGeneration;
    const epoch = this.db.epoch;
    // A bucket switch replaces the processor and the store under us; every
    // step re-checks so a stale chunk never lands in the new bucket.
    const alive = () =>
      this.processor === processor && generation === this.stateGeneration && epoch === this.db.epoch;
    const t0 = performance.now();

    let restored = false;
    if (
      this.persistCircuit &&
      typeof this.db.getSnapshot === 'function' &&
      typeof processor.load_store_state === 'function'
    ) {
      const snapshot = await this.db.getSnapshot(CIRCUIT_SNAPSHOT_KEY);
      if (!alive()) return;
      if (snapshot) {
        const { meta } = snapshot;
        if (meta.formatVersion !== CIRCUIT_SNAPSHOT_FORMAT || meta.schemaHash !== ctx.schemaHash) {
          this.logger.info(
            { meta, Category: 'sp00ky-client::StreamProcessorService::primeFromLocal' },
            'Circuit snapshot is from another format or schema; priming from rows'
          );
        } else {
          try {
            const tLoad = performance.now();
            const updates = processor.load_store_state(snapshot.bytes);
            this.dispatchWasmUpdates(updates);
            restored = true;
            this.logger.info(
              {
                bytes: snapshot.bytes.byteLength,
                views: updates.length,
                loadMs: Math.round(performance.now() - tLoad),
                Category: 'sp00ky-client::StreamProcessorService::primeFromLocal',
              },
              'Circuit snapshot restored'
            );
          } catch (e) {
            this.logger.warn(
              { error: e, Category: 'sp00ky-client::StreamProcessorService::primeFromLocal' },
              'Circuit snapshot unreadable; priming from rows'
            );
          }
        }
      }
    }

    // Read the versions AFTER the snapshot: a batch the leader writes between
    // the two can only make the store side newer, which reconcile handles.
    const versions = await this.db.scanVersions(ctx.tables);
    if (!alive()) return;
    const total = Object.values(versions).reduce((n, v) => n + v.length, 0);
    if (!restored && this.db.engineKind !== 'sqlite' && total > SURREAL_PRIME_ROW_CAP) {
      this.logger.warn(
        { total, cap: SURREAL_PRIME_ROW_CAP, Category: 'sp00ky-client::StreamProcessorService::primeFromLocal' },
        'Too many cached rows to prime through the main-thread engine; circuit fills from sync'
      );
      return;
    }

    let ingested = 0;
    let fetched = 0;
    let deleted = 0;
    for (const table of ctx.tables) {
      const entries = versions[table] ?? [];
      let toFetch: string[];
      if (restored && typeof processor.reconcile === 'function') {
        const result = processor.reconcile(table, entries);
        this.dispatchWasmUpdates(result.updates);
        deleted += result.deleted;
        toFetch = result.fetch;
      } else {
        toFetch = entries.map(([id]) => id);
      }
      for (const chunk of chunks(toFetch, PRIME_CHUNK)) {
        const rows = await this.db.selectByIds(table, chunk);
        if (!alive()) return;
        fetched += rows.length;
        ingested += this.ingestMany(
          rows.map((row) => ({
            table,
            op: 'CREATE' as const,
            id: idString(row.id),
            record: row,
          }))
        ).length;
      }
      if (entries.length > 0) {
        ctx.onVersions?.(
          table,
          entries.filter(([id]) => !ctx.pendingIds.has(id))
        );
      }
    }
    // A prime from rows leaves the store with no snapshot to fall back on;
    // let the next checkpoint write one even if nothing else changes.
    if (!restored && ingested > 0) this.markSnapshotDirty(Math.max(ingested, CHECKPOINT_MIN_ROWS));
    this.logger.info(
      {
        restored,
        rows: total,
        fetched,
        ingested,
        deleted,
        ms: Math.round(performance.now() - t0),
        Category: 'sp00ky-client::StreamProcessorService::primeFromLocal',
      },
      'Circuit primed from the local store'
    );
  }

  /** Publish wasm updates produced outside an ingest (restore, reconcile). */
  private dispatchWasmUpdates(updates: WasmStreamUpdate[] | undefined): void {
    if (!updates || updates.length === 0) return;
    this.dispatchUpdates(
      updates.map((u) => ({
        queryHash: u.query_id,
        localArray: u.result_data,
        op: 'CREATE' as const,
      }))
    );
  }

  /**
   * Record that the circuit changed by `rows` rows. Cheap; the snapshot is
   * deferred to the checkpoint timer and skipped entirely when `persistCircuit`
   * is off.
   */
  private markSnapshotDirty(rows = 1): void {
    if (!this.persistCircuit || !this.persistState) return;
    this.snapshotDirty = true;
    this.dirtyRows += rows;
    this.startCheckpoints();
  }

  private startCheckpoints(): void {
    if (this.checkpointTimer) return;
    this.checkpointTimer = setInterval(() => {
      if (!this.snapshotDirty || this.dirtyRows < CHECKPOINT_MIN_ROWS) return;
      void this.checkpoint('interval');
    }, this.checkpointMs);
    // Node/test environments have no `window`; the interval alone is enough there.
    if (typeof window !== 'undefined' && !this.hideHandler) {
      // `visibilitychange: hidden` is the reliable last chance: a 100 MB write
      // cannot finish inside `pagehide`, which is kept only as best effort.
      this.hideHandler = () => {
        if (!this.snapshotDirty) return;
        if (typeof document !== 'undefined' && document.visibilityState === 'visible') return;
        void this.checkpoint('hidden');
      };
      window.addEventListener('visibilitychange', this.hideHandler);
      window.addEventListener('pagehide', this.hideHandler);
    }
  }

  /** Stop checkpointing and drop the visibility listeners. */
  stopCheckpoints(): void {
    if (this.checkpointTimer) {
      clearInterval(this.checkpointTimer);
      this.checkpointTimer = null;
    }
    if (this.hideHandler && typeof window !== 'undefined') {
      window.removeEventListener('visibilitychange', this.hideHandler);
      window.removeEventListener('pagehide', this.hideHandler);
    }
    this.hideHandler = null;
    this.snapshotDirty = false;
    this.dirtyRows = 0;
  }

  /**
   * Write the circuit's store to the local store as a snapshot. Compacts the
   * row arena first when dead bytes outweigh live ones. Serialised: a second
   * call while one is in flight joins it. No-op unless persistence is on, this
   * tab owns the store, and the engine can hold a snapshot.
   */
  checkpoint(reason = 'manual'): Promise<void> {
    if (this.checkpointInFlight) return this.checkpointInFlight;
    const run = this.runCheckpoint(reason).finally(() => {
      this.checkpointInFlight = null;
    });
    this.checkpointInFlight = run;
    return run;
  }

  private async runCheckpoint(reason: string): Promise<void> {
    const processor = this.processor;
    if (!processor || !this.persistState || !this.persistCircuit || !this.schemaHash) return;
    if (typeof this.db.putSnapshot !== 'function' || typeof processor.save_store_state !== 'function') {
      return;
    }
    const generation = this.stateGeneration;
    const epoch = this.db.epoch;
    const rows = this.dirtyRows;
    this.snapshotDirty = false;
    this.dirtyRows = 0;
    try {
      const t0 = performance.now();
      let reclaimed = 0;
      if (
        typeof processor.compact === 'function' &&
        typeof processor.dead_bytes === 'function' &&
        typeof processor.live_bytes === 'function'
      ) {
        const dead = processor.dead_bytes();
        if (dead > 1_000_000 && dead > processor.live_bytes()) reclaimed = processor.compact();
      }
      const bytes = processor.save_store_state();
      const meta = {
        formatVersion: CIRCUIT_SNAPSHOT_FORMAT,
        schemaHash: this.schemaHash,
        savedAt: Date.now(),
        maxRv: typeof processor.max_row_versions === 'function' ? processor.max_row_versions() : undefined,
      };
      // A reset or bucket switch landed while we serialised: this snapshot
      // describes the OLD bucket's circuit and must not be written into the
      // new bucket's store.
      if (generation !== this.stateGeneration || epoch !== this.db.epoch) return;
      await this.db.putSnapshot(CIRCUIT_SNAPSHOT_KEY, bytes, meta);
      this.logger.info(
        {
          reason,
          rows,
          bytes: bytes.byteLength,
          reclaimed,
          ms: Math.round(performance.now() - t0),
          Category: 'sp00ky-client::StreamProcessorService::checkpoint',
        },
        'Circuit snapshot written'
      );
    } catch (e) {
      this.logger.warn(
        { error: e, reason, Category: 'sp00ky-client::StreamProcessorService::checkpoint' },
        'Circuit snapshot failed'
      );
    }
  }

  /**
   * Projection widening: a newly registered view evaluates fields that rows
   * already in the circuit were stored without. Merge just those fields in,
   * table by table, from the local store. The view registered against what
   * was present and converges as the merges step through.
   */
  private scheduleWiden(missing: Record<string, string[]>): void {
    for (const [table, fields] of Object.entries(missing)) {
      const set = this.widenPending.get(table) ?? new Set<string>();
      for (const f of fields) set.add(f);
      this.widenPending.set(table, set);
    }
    this.widenQueue = this.widenQueue.then(() => this.runWiden()).catch((e) => {
      this.logger.warn(
        { error: e, Category: 'sp00ky-client::StreamProcessorService::widen' },
        'Projection widening failed'
      );
    });
  }

  private async runWiden(): Promise<void> {
    const processor = this.processor;
    if (!processor || typeof this.db.scanVersions !== 'function') {
      this.widenPending.clear();
      return;
    }
    const generation = this.stateGeneration;
    const epoch = this.db.epoch;
    while (this.widenPending.size > 0) {
      const [table, fields] = this.widenPending.entries().next().value as [string, Set<string>];
      this.widenPending.delete(table);
      const select = Array.from(fields);
      const versions = await this.db.scanVersions([table]);
      if (this.processor !== processor || generation !== this.stateGeneration || epoch !== this.db.epoch) return;
      const ids = (versions[table] ?? []).map(([id]) => id);
      let merged = 0;
      for (const chunk of chunks(ids, PRIME_CHUNK)) {
        const rows = await this.db.selectByIds(table, chunk, { select });
        if (this.processor !== processor || generation !== this.stateGeneration || epoch !== this.db.epoch) return;
        merged += this.ingestMany(
          rows.map((row) => ({
            table,
            op: 'MERGE' as const,
            id: idString(row.id),
            record: row,
          }))
        ).length;
      }
      this.logger.info(
        { table, fields: select, merged, Category: 'sp00ky-client::StreamProcessorService::widen' },
        'Widened projected rows with newly evaluated fields'
      );
    }
  }

  /**
   * Seed per-table `select` permission predicates ({ [table]: whereText }).
   * Must run after the processor exists and before any `register_view`, else
   * non-`_00_` tables are default-denied and registration fails.
   */
  setPermissions(permissions: Record<string, string>) {
    if (!this.processor) return;
    if (typeof this.processor.set_permissions !== 'function') {
      this.logger.warn(
        { Category: 'sp00ky-client::StreamProcessorService::setPermissions' },
        'set_permissions not found on processor (stale WASM build?)'
      );
      return;
    }
    this.processor.set_permissions(permissions);
    this.logger.info(
      {
        tables: Object.keys(permissions).length,
        Category: 'sp00ky-client::StreamProcessorService::setPermissions',
      },
      'Seeded table permissions'
    );
  }

  /**
   * Set the current session's auth identity for permission injection,
   * mirroring the server's `fn::query::register`
   * (`object::extend(params, { auth: { id: $auth.id }, access: $access })`).
   * Stored as strings (empty when logged out) and applied to every
   * `register_view` in {@link registerQueryPlan}. Must be set before a
   * `$auth`-gated query registers (and re-set on auth state changes), or the
   * in-browser SSP's `permission_inject` rejects it with
   * "requires $auth but registration params lack it".
   */
  setSessionAuth(authId: string | null, access: string | null) {
    this.sessionAuth = { authId: authId ?? '', access: access ?? '' };
    this.logger.debug(
      {
        authId: this.sessionAuth.authId,
        access: this.sessionAuth.access,
        Category: 'sp00ky-client::StreamProcessorService::setSessionAuth',
      },
      'Session auth context updated'
    );
  }

  /**
   * Ingest a record change into the processor.
   * Emits 'stream_update' event if materialized views are affected.
   * @param isOptimistic true = local mutation (increment versions), false = remote sync (keep versions)
   */
  ingest(
    table: string,
    op: IngestRecord['op'],
    id: string,
    record: any
  ): WasmStreamUpdate[] {
    this.logger.debug(
      {
        table,
        op,
        id,
        Category: 'sp00ky-client::StreamProcessorService::ingest',
      },
      'Ingesting into ssp'
    );

    if (!this.processor) {
      this.logger.warn(
        { Category: 'sp00ky-client::StreamProcessorService::ingest' },
        'Not initialized, skipping ingest'
      );
      return [];
    }

    try {
      const normalizedRecord = this.normalizeValue(record);

      const t0 = performance.now();
      const rawUpdates = this.processor.ingest(table, op, id, normalizedRecord);
      const materializationTimeMs = performance.now() - t0;
      this.logger.debug(
        {
          table,
          op,
          id,
          rawUpdates: rawUpdates.length,
          materializationTimeMs,
          Category: 'sp00ky-client::StreamProcessorService::ingest',
        },
        'Ingesting into ssp done'
      );

      if (rawUpdates && Array.isArray(rawUpdates) && rawUpdates.length > 0) {
        const updates: StreamUpdate[] = rawUpdates.map((u: WasmStreamUpdate) => ({
          queryHash: u.query_id,
          localArray: u.result_data,
          // A MERGE is a content update as far as consumers are concerned.
          op: op === 'MERGE' ? 'UPDATE' : op,
          materializationTimeMs,
          storeApplyMs: u.timing_store_apply_ms,
          circuitStepMs: u.timing_circuit_step_ms,
          transformMs: u.timing_transform_ms,
        }));
        // Direct handler call instead of event
        this.notifyUpdates(updates);
      }
      // While batching (inside `ingestMany`), `flushCoalescing` marks dirty once
      // for the whole batch, skip the redundant per-record mark here.
      if (!this.batching) {
        this.markSnapshotDirty();
      }
      return rawUpdates;
    } catch (e) {
      this.logger.error(
        { error: e, Category: 'sp00ky-client::StreamProcessorService::ingest' },
        'Ingesting into ssp failed'
      );
    }
    return [];
  }

  /**
   * Register a new query plan.
   * Emits 'stream_update' with the initial result.
   */
  registerQueryPlan(queryPlan: QueryPlanConfig) {
    if (!this.processor) {
      this.logger.warn(
        { Category: 'sp00ky-client::StreamProcessorService::registerQueryPlan' },
        'Not initialized, skipping registration'
      );
      return;
    }

    this.logger.debug(
      {
        queryHash: queryPlan.queryHash,
        surql: queryPlan.surql,
        params: queryPlan.params,
        Category: 'sp00ky-client::StreamProcessorService::registerQueryPlan',
      },
      'Registering query plan'
    );

    try {
      const normalizedParams = this.normalizeValue(queryPlan.params);

      // Mirror the server's `fn::query::register` auth injection so the
      // in-browser SSP can resolve `$auth`/`$access` in a table's permission
      // predicate. Without this, any `$auth`-gated table (e.g. `thread`) is
      // rejected by `permission_inject` and its local view never materializes.
      // Injected only into the params handed to the in-browser SSP — never into
      // the persisted `queryState.config.params` / query hash / server payload
      // (the server does its own injection), so the view id stays shared.
      const paramsWithAuth = {
        ...(normalizedParams as Record<string, unknown>),
        auth: { id: this.sessionAuth.authId },
        access: this.sessionAuth.access,
      };

      const initialUpdate = this.processor.register_view({
        id: queryPlan.queryHash,
        surql: queryPlan.surql,
        params: paramsWithAuth,
        clientId: 'local',
        ttl: queryPlan.ttl.toString(),
        lastActiveAt: new Date().toISOString(),
      });

      this.logger.debug(
        { initialUpdate, Category: 'sp00ky-client::StreamProcessorService::registerQueryPlan' },
        'register_view result'
      );

      if (!initialUpdate) {
        throw new Error('Failed to register query plan');
      }
      const update: StreamUpdate = {
        queryHash: initialUpdate.query_id,
        localArray: initialUpdate.result_data,
        registration: {
          parseMs: initialUpdate.timing_parse_ms ?? 0,
          planMs: initialUpdate.timing_plan_ms ?? 0,
          snapshotMs: initialUpdate.timing_snapshot_ms ?? 0,
        },
      };
      if (initialUpdate.missing_fields && Object.keys(initialUpdate.missing_fields).length > 0) {
        this.scheduleWiden(initialUpdate.missing_fields);
      }
      this.logger.debug(
        {
          queryHash: queryPlan.queryHash,
          surql: queryPlan.surql,
          params: queryPlan.params,
          Category: 'sp00ky-client::StreamProcessorService::registerQueryPlan',
        },
        'Registered query plan'
      );
      return update;
    } catch (e) {
      this.logger.error(
        { error: e, Category: 'sp00ky-client::StreamProcessorService::registerQueryPlan' },
        'Error registering query plan'
      );
      throw e;
    }
  }

  /**
   * Unregister a query plan by ID.
   */
  unregisterQueryPlan(queryHash: string) {
    if (!this.processor) return;
    try {
      this.processor.unregister_view(queryHash);
    } catch (e) {
      this.logger.error(
        { error: e, Category: 'sp00ky-client::StreamProcessorService::unregisterQueryPlan' },
        'Error unregistering query plan'
      );
    }
  }

  private normalizeValue(value: any): any {
    if (value === null || value === undefined) return value;

    if (typeof value === 'object') {
      // CRDT snapshots arrive as `Uint8Array` (or `ArrayBuffer` /
      // typed-array views). `serde_wasm_bindgen::from_value` rejects
      // those when deserializing into `serde_json::Value` (JSON has no
      // binary variant), and the SSP can't filter on opaque bytes
      // anyway. Replace with `null` so the row still flows through the
      // ingest path with its other columns intact, and downstream
      // predicates referencing the bytes column simply don't match.
      if (
        value instanceof Uint8Array ||
        value instanceof ArrayBuffer ||
        ArrayBuffer.isView(value)
      ) {
        return null;
      }

      // RecordId detection using duck typing (constructor.name may be minified)
      // SurrealDB's RecordId has: table (getter returning Table), id, and toString()
      // Check for table getter that has its own toString AND id property
      const hasTable = 'table' in value && typeof value.table?.toString === 'function';
      const hasId = 'id' in value;
      const hasToString = typeof value.toString === 'function';
      const isNotPlainObject = value.constructor !== Object;

      if (hasTable && hasId && hasToString && isNotPlainObject) {
        // The raw `table:id`, as the store spells it. `toString()` is escaped
        // SurrealQL (SDK 2.1.0: `game:⟨476g…⟩`); only a complex key uses it.
        const result = recordIdKey(value) ?? value.toString();
        this.logger.trace(
          { result, Category: 'sp00ky-client::StreamProcessorService::normalizeValue' },
          'RecordId detected'
        );
        return result;
      }

      // Fallback: old check for objects with tb and id (some internal representations)
      if ('tb' in value && 'id' in value && !('table' in value)) {
        return `${value.tb}:${value.id}`;
      }

      // Handle arrays recursively
      if (Array.isArray(value)) {
        return value.map((v) => this.normalizeValue(v));
      }

      // Handle plain objects recursively
      if (value.constructor === Object) {
        const out: any = {};
        for (const k in value) {
          out[k] = this.normalizeValue(value[k]);
        }
        return out;
      }
    }
    return value;
  }
}
