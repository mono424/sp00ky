import type { LocalStore, RemoteDatabaseService } from '../../services/database/index';
import type { Logger } from '../../services/logger/index';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import { RecordId } from 'surrealdb';
import type { StreamUpdate, StreamUpdateReceiver } from '../../services/stream-processor/index';
import { encodeRecordId } from '../../utils/index';

// DevTools interfaces (matching extension expectations)
export interface DevToolsEvent {
  id: number;
  timestamp: number;
  eventType: string;
  payload: any;
}

import type { QueryState, QueryTimings } from '../../types';
import type { AuthService } from '../auth/index';
import { AuthEventTypes } from '../auth/events/index';
import {
  type BackendInfo,
  emptyBackendInfo,
  parseBackendInfo,
  UNAVAILABLE,
} from './versions';
import { walkOpfs, type BlobCacheInfo, type SharedTabsInfo, type StorageInfo } from './storage-info';
import { FlagsAdminService, type LocalOverrideStore } from './flags';

// Real bundled frontend versions, injected at build time by tsdown's
// version-define plugin (see tsdown.config.ts). The `typeof` guard keeps these
// from throwing a ReferenceError when a downstream app bundles core from source
// (where the plugin never runs); in that case they fall back to 'unknown' and
// DevTools simply reports an unknown frontend version instead of crashing.
const CORE_VERSION =
  typeof __SP00KY_CORE_VERSION__ !== 'undefined' ? __SP00KY_CORE_VERSION__ : 'unknown';
const WASM_VERSION =
  typeof __SP00KY_WASM_VERSION__ !== 'undefined' ? __SP00KY_WASM_VERSION__ : 'unknown';
const SURREAL_VERSION =
  typeof __SP00KY_SURREAL_VERSION__ !== 'undefined' ? __SP00KY_SURREAL_VERSION__ : 'unknown';

/** What DevTools reads about live queries; the runtime derives it from state. */
export interface DevToolsQuerySource {
  getActiveQueries(): QueryState[];
  getQueryById(id: RecordId<string>): QueryState | undefined;
  phaseTimings(q: QueryState): QueryTimings;
}

export class DevToolsService implements StreamUpdateReceiver {
  private eventsHistory: DevToolsEvent[] = [];
  private eventIdCounter = 0;
  // Real bundled frontend version (injected at build time via tsdown `define`).
  private version = CORE_VERSION;
  // Backend stack info (versions + per-entity status), read via the
  // `fn::spooky::info()` SurrealQL function; empty/'unavailable' until resolved.
  private backendInfo: BackendInfo = emptyBackendInfo();
  // Dormant until a devtools consumer (extension panel or MCP) handshakes via
  // `SP00KY_DEVTOOLS_CONNECT`. While false, `notifyDevTools()`/`addEvent()` do no
  // work, so prod pays zero serialization/postMessage cost for an unwatched panel.
  // `window.__00__.getState()` stays live regardless, so the panel's first paint
  // (the on-demand GET_STATE pull) still works before the push channel turns on.
  private enabled = false;

  // A state push serializes EVERY active query's full record set (see
  // `getActiveQueries`) and postMessage clones it again, so its cost scales with
  // the whole client dataset — and it is triggered per event, including one per
  // local DB query (`DATABASE_LOCAL_QUERY` → `logEvent`). Unthrottled, a single
  // page load's few hundred local queries turn a handful of MB of rows into GBs
  // of short-lived large-object garbage and OOM the renderer (V8
  // "young object promotion failed"). Coalesce instead: push immediately when
  // idle, then at most once per window, always serializing the LATEST state.
  private static readonly NOTIFY_MIN_INTERVAL_MS = 250;
  private notifyTimer: ReturnType<typeof setTimeout> | null = null;
  private lastNotifyAt = 0;
  /** How many ids a pushed state carries per view; the rest is on demand. */
  private static readonly STATE_IDS_CAP = 200;
  /** devtools numeric hash -> the query's `_00_query` id, for on-demand rows. */
  private hashToQuery = new Map<number, unknown>();

  /** Shared-tabs snapshot for the panel, wired by Sp00kyClient whenever the
   *  feature was REQUESTED (so an inactive/degraded tab still reports why). */
  private tabsInfoProvider: (() => SharedTabsInfo | null) | null = null;

  setTabsInfoProvider(provider: () => SharedTabsInfo | null): void {
    this.tabsInfoProvider = provider;
  }

  /** Blob cache counters for the panel, wired by Sp00kyClient. */
  private blobInfoProvider: (() => BlobCacheInfo) | null = null;

  setBlobInfoProvider(provider: () => BlobCacheInfo): void {
    this.blobInfoProvider = provider;
  }

  // Full local table list (incl. internal `_00_*`), enumerated from the local DB
  // via our own reliable `service.query` — the DevTools panel's page-eval query
  // bridge is unreliable under load, so the panel relies on this instead.
  private localTables: string[] = [];
  private localTablesFetching = false;
  private localTablesAt = 0;

  // Feature flag admin, backing the panel's Access tab. The local-override
  // store is injected later (`setFeatureFlagOverrides`) because the
  // FeatureFlagModule is built after this service.
  private featureOverrides: LocalOverrideStore | null = null;
  private readonly flagsAdmin: FlagsAdminService;

  constructor(
    private databaseService: LocalStore,
    private remoteDatabaseService: RemoteDatabaseService,
    private logger: Logger,
    private schema: SchemaStructure,
    private authService: AuthService<SchemaStructure>,
    private dataManager?: DevToolsQuerySource
  ) {
    this.flagsAdmin = new FlagsAdminService({
      remote: this.remoteDatabaseService,
      local: this.databaseService,
      logger: this.logger,
      currentUserId: () => {
        const id = this.authService.currentUser?.id;
        if (!id) return null;
        return id instanceof RecordId ? encodeRecordId(id) : String(id);
      },
      overrides: () => this.featureOverrides,
    });

    this.exposeToWindow();

    // Stay dormant until a devtools consumer announces itself. The extension's
    // page-script posts this once it detects `window.__00__`; the panel can also
    // disconnect to return us to dormant. Until then we skip all serialization.
    if (typeof window !== 'undefined') {
      window.addEventListener('message', (e) => {
        if (e.source !== window) return;
        const type = (e.data as { type?: string } | undefined)?.type;
        if (type === 'SP00KY_DEVTOOLS_CONNECT') {
          this.enabled = true;
          this.refreshLocalTables();
          this.notifyDevTools();
        } else if (type === 'SP00KY_DEVTOOLS_DISCONNECT') {
          this.enabled = false;
        }
      });
    }

    // Subscribe to auth events. The initial fire-and-forget version fetch (below)
    // races the remote connection; on the free plan the remote DB (SurrealDB
    // Cloud) has no guest access, so `fn::spooky::info()` is only callable once
    // signed in. Re-fetch when auth resolves — until the versions actually land —
    // instead of leaving them 'unavailable' forever.
    this.authService.eventSystem.subscribe(AuthEventTypes.AuthStateChanged, () => {
      if (this.authService.isAuthenticated && this.backendInfo.versions.ssp === UNAVAILABLE) {
        void this.refreshBackendVersions();
      } else {
        this.notifyDevTools();
      }
    });

    // Push state when the local store reports its durability (the open happens
    // during connect, typically before a panel attaches, so this mostly matters
    // for a later bucket switch that loses OPFS).
    this.databaseService.subscribeToStorageHealth?.(() => this.notifyDevTools());

    // Fire-and-forget backend version discovery; re-push state when it lands.
    void this.refreshBackendVersions();

    this.logger.debug({ Category: 'sp00ky-client::DevToolsService::init' }, 'Service initialized');
  }

  /**
   * Re-read backend stack info via the `fn::spooky::info()` SurrealQL function
   * over the open remote connection (no HTTP/CORS), then notify the panel.
   * Never throws: on failure the info stays empty/'unavailable'.
   */
  private async refreshBackendVersions(): Promise<void> {
    try {
      // `RETURN fn::spooky::info()` → one statement result: the /info entity array.
      const result = await this.remoteDatabaseService.query<unknown[]>(
        'RETURN fn::spooky::info()'
      );
      const first = Array.isArray(result) ? result[0] : result;
      this.backendInfo = parseBackendInfo(first);
    } catch (err) {
      this.logger.debug(
        { err, Category: 'sp00ky-client::DevToolsService::versions' },
        'fn::spooky::info() unavailable; backend versions stay unavailable'
      );
      this.backendInfo = emptyBackendInfo();
    }
    this.notifyDevTools();
  }

  // Get active queries directly from DataManager (single source of truth)
  private getActiveQueries(): Map<number, any> {
    const result = new Map<number, any>();
    if (!this.dataManager) return result;

    const queries = this.dataManager.getActiveQueries();
    queries.forEach((q) => {
      const queryHash = this.hashString(encodeRecordId(q.config.id));
      const createdAt =
        q.config.lastActiveAt instanceof Date
          ? q.config.lastActiveAt.getTime()
          : new Date(q.config.lastActiveAt || Date.now()).getTime();
      this.hashToQuery.set(queryHash, q.config.id);
      const localArray = q.config.localArray ?? [];
      const remoteArray = q.config.remoteArray ?? [];
      result.set(queryHash, {
        queryHash,
        status: 'active',
        // Runtime fetch status, distinct from the `status: 'active'`
        // registration flag above. `fetchStatus` is 'idle' | 'fetching'.
        fetchStatus: q.status,
        isFetching: q.status === 'fetching',
        createdAt,
        // Real last-update time; before the first update it equals createdAt.
        // (Previously Date.now(), which reset the column on every state push.)
        lastUpdate: q.lastUpdatedAt ?? createdAt,
        updateCount: q.updateCount,
        ttl: q.config.ttl,
        query: q.config.surql,
        variables: q.config.params || {},
        dataSize: q.records?.length || 0,
        // Counts and (capped) ids only. The rows themselves used to ride along
        // here: every push then deep-cloned every view's records twice
        // (serialize + postMessage), on a 4k-row client dataset up to four
        // times a second, from inside the ingest call stack. The panel pulls
        // rows on demand through `getQueryRows` instead.
        localCount: localArray.length,
        remoteCount: remoteArray.length,
        localIds: localArray.slice(0, DevToolsService.STATE_IDS_CAP).map(([id]) => id),
        remoteIds: remoteArray.slice(0, DevToolsService.STATE_IDS_CAP).map(([id]) => id),
        idsTruncated:
          localArray.length > DevToolsService.STATE_IDS_CAP ||
          remoteArray.length > DevToolsService.STATE_IDS_CAP,
        // Membership state, so "why is this list empty" is answerable from
        // the panel: is the server's set known, has a non-empty one been seen
        // this session, how many empty reads were ignored.
        membershipKnown: q.config.membershipKnown === true,
        authoritative: q.config.membershipKnown === true,
        viewLost: q.viewLost === true,
        serverState: q.serverState ?? null,
        // Detailed per-phase processing-time breakdown (SSP sub-phases, local/
        // remote record fetch, frontend reconcile, registration). Flows to both
        // the DevTools panel and the MCP (which returns activeQueries verbatim).
        timings: this.dataManager!.phaseTimings(q),
      });
    });
    return result;
  }

  public onQueryInitialized(payload: any) {
    this.logger.debug(
      { payload, Category: 'sp00ky-client::DevToolsService::onQueryInitialized' },
      'QueryInitialized'
    );
    const queryHash = this.hashString(payload.queryId.toString());

    this.addEvent('QUERY_REQUEST_INIT', {
      queryHash,
      query: payload.sql,
      variables: {},
    });
    this.notifyDevTools();
  }

  public onQueryUpdated(payload: any) {
    this.logger.debug(
      {
        id: payload.queryId?.toString(),
        Category: 'sp00ky-client::DevToolsService::onQueryUpdated',
      },
      'QueryUpdated'
    );
    const queryHash = this.hashString(payload.queryId.toString());

    this.addEvent('QUERY_UPDATED', {
      queryHash,
      recordCount: Array.isArray(payload.records) ? payload.records.length : 0,
    });
    this.notifyDevTools();
  }

  public onStreamUpdate(update: StreamUpdate) {
    // A synthetic re-materialize is not an ingest (DataModule.scheduleRematerialize).
    if (update.synthetic) return;
    this.logger.debug(
      { queryHash: update.queryHash, Category: 'sp00ky-client::DevToolsService::onStreamUpdate' },
      'StreamUpdate'
    );
    // Counts and timings, not the `localArray` itself: that is one [id, version]
    // pair per row of the view, serialized on every single update.
    this.addEvent('STREAM_UPDATE', {
      queryHash: update.queryHash,
      op: update.op,
      localCount: update.localArray?.length ?? 0,
      materializationTimeMs: update.materializationTimeMs,
      storeApplyMs: update.storeApplyMs,
      circuitStepMs: update.circuitStepMs,
      transformMs: update.transformMs,
    });
    this.notifyDevTools();
  }

  public onMutation(payload: any[]) {
    const payloads = payload;
    payloads.forEach((p) => {
      this.addEvent('MUTATION_REQUEST_EXECUTION', {
        mutation: {
          type: p.type ?? 'create',
          // Field names only; a payload body (a PGN, a document) is not
          // something to clone on every write.
          fields: 'data' in p && p.data && typeof p.data === 'object' ? Object.keys(p.data) : [],
          selector: encodeRecordId(p.record_id),
        },
      });
    });
    this.notifyDevTools();
  }

  private hashString(str: string): number {
    let hash = 0;
    if (str.length === 0) return hash;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
  }

  public logEvent(eventType: string, payload: any) {
    this.addEvent(eventType, payload);
    this.notifyDevTools();
  }

  private addEvent(eventType: string, payload: any) {
    // No consumer attached → skip recording (and the recursive serialize it does).
    if (!this.enabled) return;
    this.eventsHistory.push({
      id: this.eventIdCounter++,
      timestamp: Date.now(),
      eventType,
      payload: this.serializeForDevTools(payload),
    });
    if (this.eventsHistory.length > 100) this.eventsHistory.shift();
  }

  /** Unwrap a SurrealDB `INFO FOR DB` result to its `{ tables, ... }` object. */
  private unwrapInfo(res: any): any {
    if (!Array.isArray(res) || !res[0]) return null;
    const first = res[0];
    if (first && typeof first === 'object' && 'result' in first) return first.result;
    if (Array.isArray(first)) return first[0];
    return first;
  }

  /**
   * Refresh the cached full local-table list from `INFO FOR DB`. Fire-and-forget
   * and throttled — called from getState() so the panel gets every table
   * (including internal `_00_*`) without running its own (flaky) queries.
   */
  private refreshLocalTables(): void {
    if (this.localTablesFetching) return;
    const now = Date.now();
    if (now - this.localTablesAt < 30_000) return;
    this.localTablesFetching = true;
    void this.databaseService
      .query<any>('INFO FOR DB')
      .then((res) => {
        const info = this.unwrapInfo(res);
        this.localTablesAt = Date.now();
        if (info && info.tables) {
          // The circuit snapshot table holds a BLOB, not JSON rows; the
          // explorer cannot render it and has nothing to show for it.
          const names = Object.keys(info.tables).filter((n) => n !== '_00_circuit_snapshot');
          const changed =
            names.length !== this.localTables.length ||
            names.some((n, i) => n !== this.localTables[i]);
          this.localTables = names;
          if (changed) this.notifyDevTools();
        }
      })
      .catch(() => {
        // Ignore — fall back to the declared app schema below.
      })
      .finally(() => {
        this.localTablesFetching = false;
      });
  }

  private getState(opts: { refreshTables?: boolean } = {}) {
    // The local-table list (`INFO FOR DB`, a local round trip) is refreshed on
    // an explicit pull only, never by the push path: pushes follow every sync
    // event, and a local query per push competed with the app's own writes for
    // the single local op queue.
    if (opts.refreshTables) this.refreshLocalTables();
    return this.serializeForDevTools({
      eventsHistory: [...this.eventsHistory],
      activeQueries: Object.fromEntries(this.getActiveQueries()),
      auth: {
        authenticated: this.authService.isAuthenticated,
        userId: this.authService.currentUser?.id,
      },
      version: this.version,
      versions: {
        frontend: {
          core: CORE_VERSION,
          wasm: WASM_VERSION,
          surrealdb: SURREAL_VERSION,
        },
        backend: this.backendInfo.versions,
        entities: this.backendInfo.entities,
      },
      database: {
        // Prefer the live local-table list (includes internal `_00_*`); fall
        // back to the declared app schema until the first enumeration lands.
        tables: this.localTables.length
          ? this.localTables
          : this.schema.tables.map((t) => t.name),
        tableData: {},
        // Which backend answers "Local". The Database explorer labels its source
        // picker with it and explains translation failures against `sqlite`,
        // whose SurrealQL vocabulary is a bounded subset.
        engine: this.databaseService.engineKind ?? 'custom',
        // Durability of the local store. `fallback: true` means persistence was
        // requested but the dataset is actually sitting in RAM.
        storage: this.databaseService.storageHealth ?? { status: 'unknown', fallback: false },
        // Shared-tabs role state (null when the feature is off / fell back).
        tabs: this.tabsInfoProvider?.() ?? null,
      },
    });
  }

  /**
   * Full storage diagnostics for the DevTools Storage tab. Every section is
   * gathered independently and failures land in that section's `error` field,
   * so one broken source (a mid-switch worker, a browser without OPFS) never
   * blanks the whole panel.
   */
  public async getStorageInfo(opts?: { tableCounts?: boolean }): Promise<StorageInfo> {
    const nav = typeof navigator !== 'undefined' ? navigator : undefined;

    const info: StorageInfo = {
      at: Date.now(),
      engine: {
        kind: this.databaseService.engineKind ?? 'custom',
        store: this.databaseService.getConfig()?.store ?? 'memory',
        bucketId: this.databaseService.currentBucketId,
      },
      health: this.databaseService.storageHealth ?? { status: 'unknown', fallback: false },
      tabs: this.tabsInfoProvider?.() ?? null,
      browser: {},
      opfs: { supported: false, entries: [], totalBytes: 0, truncated: false },
    };

    try {
      if (nav?.storage?.estimate) {
        const est = await nav.storage.estimate();
        info.browser.usage = est.usage;
        info.browser.quota = est.quota;
        // Chrome-only per-storage-system breakdown; absent elsewhere.
        const details = (est as any).usageDetails;
        if (details && typeof details === 'object') info.browser.usageDetails = details;
      }
      if (nav?.storage?.persisted) {
        info.browser.persisted = await nav.storage.persisted();
      }
    } catch (e) {
      info.browser.error = e instanceof Error ? e.message : String(e);
    }

    info.opfs = await walkOpfs();

    try {
      info.blobs = this.blobInfoProvider?.();
    } catch (e) {
      this.logger.warn(
        { err: e, Category: 'sp00ky-client::DevToolsService::getStorageInfo' },
        'Blob cache diagnostics failed'
      );
    }

    const stats = (globalThis as any).__sqliteStats;
    if (stats && typeof stats === 'object') {
      info.sqliteStats = { ...stats, byType: { ...(stats.byType ?? {}) } };
    }

    try {
      info.engineDiagnostics = await this.databaseService.getStorageDiagnostics?.(opts);
    } catch (e) {
      this.logger.warn(
        { err: e, Category: 'sp00ky-client::DevToolsService::getStorageInfo' },
        'Engine storage diagnostics failed'
      );
    }

    return this.serializeForDevTools(info);
  }

  /** Ask the browser to exempt this origin's storage from eviction. */
  public async requestPersistentStorage(): Promise<{ granted: boolean }> {
    try {
      const granted = (await navigator.storage?.persist?.()) ?? false;
      return { granted };
    } catch {
      return { granted: false };
    }
  }

  /**
   * Request a state push. Coalesced (see {@link NOTIFY_MIN_INTERVAL_MS}): the
   * first call after an idle period pushes straight away so the panel stays
   * responsive, and any calls during the window collapse into ONE trailing push
   * that serializes the state as of the flush, not as of the request. Callers
   * stay fire-and-forget.
   */
  private notifyDevTools() {
    // No consumer attached → no getState() serialization, no postMessage broadcast.
    if (!this.enabled) return;
    if (typeof window === 'undefined') return;
    // A trailing push is already queued; it will carry this change too.
    if (this.notifyTimer !== null) return;

    // Always a macrotask, never inline: this is called from inside the ingest
    // and mutation call stacks (onStreamUpdate / onMutation), i.e. inside the
    // `await db.create(...)` the app is waiting on. A push that serializes the
    // state right there charged every write for the panel's refresh.
    const waited = Date.now() - this.lastNotifyAt;
    const delay = Math.max(0, DevToolsService.NOTIFY_MIN_INTERVAL_MS - waited);
    this.notifyTimer = setTimeout(() => {
      this.notifyTimer = null;
      // Still gated on `enabled`: the panel may have disconnected while queued.
      if (this.enabled) this.flushNotify();
    }, delay);
  }

  private flushNotify() {
    this.lastNotifyAt = Date.now();
    window.postMessage(
      {
        type: 'SP00KY_STATE_CHANGED',
        source: 'sp00ky-devtools-page',
        state: this.getState(),
      },
      '*'
    );
  }

  private serializeForDevTools(data: any, seen = new WeakSet<object>()): any {
    if (data === undefined) {
      return 'undefined';
    }

    if (data === null) {
      return null;
    }

    if (data instanceof RecordId) {
      return data.toString();
    }

    if (Array.isArray(data)) {
      if (seen.has(data)) {
        return '[Circular Array]';
      }
      seen.add(data);
      return data.map((item) => this.serializeForDevTools(item, seen));
    }

    if (typeof data === 'bigint') {
      return data.toString();
    }

    if (data instanceof Date) {
      return data.toISOString();
    }

    if (typeof data === 'object') {
      if (seen.has(data)) {
        return '[Circular Object]';
      }
      seen.add(data);

      const result: Record<string, any> = {};
      for (const key in data) {
        if (Object.prototype.hasOwnProperty.call(data, key)) {
          // Skip absent optional fields: recursing them would emit the STRING
          // 'undefined' (the top-level mapping below), which panels then have
          // to filter back out (see 3d84fe8a).
          if (data[key] === undefined) continue;
          result[key] = this.serializeForDevTools(data[key], seen);
        }
      }
      return result;
    }

    return data;
  }

  /**
   * Hand the FeatureFlagModule to the Access tab so it can read and write local
   * overrides. Called from `Sp00kyClient` once both are constructed; until then
   * the override methods are no-ops that report an empty map.
   */
  public setFeatureFlagOverrides(store: LocalOverrideStore): void {
    this.featureOverrides = store;
  }

  private exposeToWindow() {
    if (typeof window !== 'undefined') {
      (window as any).__00__ = {
        version: this.version,
        getState: () => this.getState({ refreshTables: true }),
        // The rows of ONE view, on demand. The pushed state carries counts and
        // capped ids only (see getActiveQueries); the panel's Data tab and the
        // MCP fetch the rows here when somebody actually looks at them.
        getQueryRows: (queryHash: number) => {
          const id = this.hashToQuery.get(Number(queryHash));
          const q = id !== undefined ? this.dataManager?.getQueryById(id as any) : undefined;
          if (!q) return null;
          return this.serializeForDevTools({
            queryHash: Number(queryHash),
            data: q.records,
            localArray: q.config.localArray,
            remoteArray: q.config.remoteArray,
          });
        },
        // ---- Feature flags (Access tab) --------------------------------
        // Remote reads/writes are admin-gated by SurrealDB, not here: a
        // non-admin gets an empty flag list, and the `fn::feature::*` calls
        // are denied outright. The override methods are purely local and
        // work signed out.
        getFlags: () => this.flagsAdmin.getFlags(),
        setFlagEnabled: (key: string, enabled: boolean) =>
          this.flagsAdmin.setFlagEnabled(key, enabled),
        setFlagUserVariant: (key: string, variant: string, remove: boolean, userId?: string) =>
          this.flagsAdmin.setFlagUserVariant(key, variant, remove, userId),
        setLocalFlagOverride: (key: string, variant: string | null, payload?: unknown) =>
          this.flagsAdmin.setLocalFlagOverride(key, variant, payload),
        clearLocalFlagOverrides: () => this.flagsAdmin.clearLocalFlagOverrides(),
        clearHistory: () => {
          this.eventsHistory = [];
          this.notifyDevTools();
        },
        refreshVersions: () => this.refreshBackendVersions(),
        getStorageInfo: (opts?: { tableCounts?: boolean }) => this.getStorageInfo(opts),
        requestPersistentStorage: () => this.requestPersistentStorage(),
        getTableData: async (tableName: string) => {
          try {
            // Returns the first statement result as T.
            // SurrealDB query returns [Result1, Result2...].
            // We want the records from the first result.
            const result = await this.databaseService.query<any>(`SELECT * FROM ${tableName}`);

            let records: any[] = [];

            if (Array.isArray(result) && result.length > 0) {
              const first = result[0];
              if (Array.isArray(first)) {
                // Legacy or flattened format: [[records]]
                records = first;
              } else if (
                first &&
                typeof first === 'object' &&
                'result' in first &&
                'status' in first
              ) {
                // SurrealDB 2.0 format: [{ result: [...records], status: 'OK', ... }]
                records = Array.isArray(first.result) ? first.result : [];
              } else {
                // Fallback: assume result is the array of records itself
                records = result;
              }
            } else if (Array.isArray(result)) {
              // Empty array
              records = [];
            }

            return this.serializeForDevTools(records) || [];
          } catch (e) {
            this.logger.error(
              { err: e, Category: 'sp00ky-client::DevToolsService::exposeToWindow' },
              'Failed to get table data'
            );
            return [];
          }
        },
        updateTableRow: async (
          tableName: string,
          recordId: string,
          updates: Record<string, unknown>
        ) => {
          try {
            await this.databaseService.query(`UPDATE ${recordId} MERGE $updates`, { updates });
            return { success: true };
          } catch (e: any) {
            return { success: false, error: e.message };
          }
        },
        deleteTableRow: async (tableName: string, recordId: string) => {
          try {
            await this.databaseService.query(`DELETE ${recordId}`);
            return { success: true };
          } catch (e: any) {
            return { success: false, error: e.message };
          }
        },
        runQuery: async (query: string, target: 'local' | 'remote' = 'local') => {
          try {
            this.logger.debug(
              { query, target, Category: 'sp00ky-client::DevToolsService::runQuery' },
              'Running query (START)'
            );
            const service = target === 'remote' ? this.remoteDatabaseService : this.databaseService;

            const startTime = Date.now();
            const result = await service.query<any>(query);
            const queryTime = Date.now() - startTime;

            this.logger.debug(
              {
                query,
                time: queryTime,
                resultType: typeof result,
                isArray: Array.isArray(result),
                Category: 'sp00ky-client::DevToolsService::runQuery',
              },
              'Database returned result'
            );

            // Serialize the result for DevTools
            const serializeStart = Date.now();
            const serialized = this.serializeForDevTools(result);
            const serializeTime = Date.now() - serializeStart;

            this.logger.debug(
              {
                serializeTime,
                serializedLength: JSON.stringify(serialized).length,
                Category: 'sp00ky-client::DevToolsService::runQuery',
              },
              'Serialization complete'
            );

            return {
              success: true,
              data: serialized,
              target,
            };
          } catch (e: any) {
            this.logger.error(
              { err: e, query, target, Category: 'sp00ky-client::DevToolsService::runQuery' },
              'Query execution failed'
            );
            // Ensure we always return a string for error
            const errorMessage =
              e instanceof Error ? e.message : typeof e === 'string' ? e : JSON.stringify(e);
            return { success: false, error: errorMessage || 'Unknown occurred' };
          }
        },
      };

      window.postMessage(
        {
          type: 'SP00KY_DETECTED',
          source: 'sp00ky-devtools-page',
          data: { version: this.version, detected: true },
        },
        '*'
      );

      // Dispatch custom event so the devtools page-script can detect late initialization
      window.dispatchEvent(new CustomEvent('sp00ky:init'));
    }
  }
}
