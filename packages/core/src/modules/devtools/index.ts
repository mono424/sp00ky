import type { LocalStore, RemoteDatabaseService } from '../../services/database/index';
import type { Level } from 'pino';
import type { LogRead, LogTap, Logger } from '../../services/logger/index';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import { RecordId } from 'surrealdb';
import type { StreamUpdate, StreamUpdateReceiver } from '../../services/stream-processor/index';
import { encodeRecordId } from '../../utils/index';

import type { MutationEventType, QueryState, QueryTimings } from '../../types';
import type { ClientState } from '../../state/client-state';
import {
  parsePendingRow,
  parseStoredRecordId,
  storedIdString,
  type FailedMutationRow,
  type PendingMutationRow,
} from '../../mutation/rows';
import type {
  AuthService,
  ActiveImpersonation,
  ImpersonationCandidate,
  ImpersonationInfo,
} from '../auth/index';
import { AuthEventTypes } from '../auth/events/index';
import {
  type BackendInfo,
  emptyBackendInfo,
  parseBackendInfo,
  UNAVAILABLE,
} from './versions';
import { walkOpfs, type BlobCacheInfo, type SharedTabsInfo, type StorageInfo } from './storage-info';
import { FlagsAdminService, type LocalOverrideStore } from './flags';
import {
  buildMutationsState,
  queuedAtOf,
  trimHistory,
  type DevToolsMutationsState,
  type MutationHistoryEntry,
} from './mutations';

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

export type ImpersonationOp = 'status' | 'listUsers' | 'start' | 'stop';

/** What the Mutations tab reads and does; the client wires it to the runtime. */
export interface DevToolsMutationSource {
  state(): ClientState;
  listFailed(): Promise<FailedMutationRow[]>;
  retryFailed(mutationId: string): Promise<boolean>;
  discardFailed(mutationId: string): Promise<boolean>;
}

export type MutationOp = 'listFailed' | 'get' | 'retry' | 'discard' | 'clearHistory';

export interface MutationOpResult {
  success: boolean;
  error?: string;
  failed?: FailedMutationRow[];
  /** `get`: the stored outbox row, or null once it left the outbox. */
  row?: PendingMutationRow | null;
}

export type LogOp = 'read' | 'setCaptureLevel' | 'clear';

export type LogOpResult = { success: boolean; error?: string } & Partial<LogRead>;

export interface ImpersonationOpResult {
  success: boolean;
  error?: string;
  /** Whether the generated schema says the project enabled impersonation. */
  enabled?: boolean;
  isAdmin?: boolean;
  current?: ImpersonationInfo | null;
  active?: ActiveImpersonation[];
  users?: ImpersonationCandidate[];
}

export class DevToolsService implements StreamUpdateReceiver {
  // Real bundled frontend version (injected at build time via tsdown `define`).
  private version = CORE_VERSION;
  // Backend stack info (versions + per-entity status), read via the
  // `fn::spooky::info()` SurrealQL function; empty/'unavailable' until resolved.
  private backendInfo: BackendInfo = emptyBackendInfo();
  // Dormant until a devtools consumer (extension panel or MCP) handshakes via
  // `SP00KY_DEVTOOLS_CONNECT`. While false, `notifyDevTools()` and the mutation
  // history do no work, so prod pays zero serialization/postMessage cost for an
  // unwatched panel.
  // `window.__00__.getState()` stays live regardless, so the panel's first paint
  // (the on-demand GET_STATE pull) still works before the push channel turns on.
  private enabled = false;

  // A state push serializes EVERY active query's full record set (see
  // `getActiveQueries`) and postMessage clones it again, so its cost scales with
  // the whole client dataset — and it is requested on every sync event (it used
  // to be one per local DB query, when those were recorded as events).
  // Unthrottled, a page load's burst of requests turned a handful of MB of rows
  // into GBs of short-lived large-object garbage and OOMed the renderer (V8
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

  /** Outbox, debounced writes and the failed tray, wired by Sp00kyClient. */
  private mutationSource: DevToolsMutationSource | null = null;
  /**
   * Mutations seen since a consumer attached, by id, oldest first. The outbox
   * only knows what is still queued; this is what lets the panel show a write
   * as synced or rolled back after it left.
   */
  private mutationHistory = new Map<string, MutationHistoryEntry>();
  private static readonly MUTATIONS_CAP = 200;

  setMutationSource(source: DevToolsMutationSource): void {
    this.mutationSource = source;
  }

  /**
   * The page's log buffer. It records whether or not anybody is watching (so
   * a panel opened late still sees the boot); while a consumer is attached,
   * new lines go out as small `SP00KY_LOGS` deltas, never inside the state
   * push, which would re-serialize the whole buffer every time.
   */
  private logTap: LogTap | null = null;
  /** `seq` of the newest line already pushed. */
  private logCursor = 0;
  private logTimer: ReturnType<typeof setTimeout> | null = null;
  private lastLogPushAt = 0;

  setLogTap(tap: LogTap): void {
    this.logTap = tap;
    this.logCursor = tap.head;
    tap.subscribe(() => this.scheduleLogPush());
  }

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
          // The consumer pulls the backlog itself (`logOp('read')`); pushes
          // carry only what arrives after this point.
          this.logCursor = this.logTap?.head ?? 0;
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

  public onStreamUpdate(update: StreamUpdate) {
    // A synthetic re-materialize is not an ingest (DataModule.scheduleRematerialize).
    if (update.synthetic) return;
    // Counts and timings, not the `localArray` itself: that is one [id, version]
    // pair per row of the view. The Logs tab shows this line at debug.
    this.logger.debug(
      {
        queryHash: update.queryHash,
        op: update.op,
        localCount: update.localArray?.length ?? 0,
        materializationTimeMs: update.materializationTimeMs,
        storeApplyMs: update.storeApplyMs,
        circuitStepMs: update.circuitStepMs,
        transformMs: update.transformMs,
        Category: 'sp00ky-client::DevToolsService::onStreamUpdate',
      },
      'StreamUpdate'
    );
    this.notifyDevTools();
  }

  public onMutation(payload: any[]) {
    for (const p of payload) this.recordQueued(p);
    this.notifyDevTools();
  }

  /** A write entered the outbox. */
  private recordQueued(p: any): void {
    if (!this.enabled || !p?.mutation_id) return;
    const id = storedIdString(p.mutation_id);
    const recordId = p.record_id ? encodeRecordId(p.record_id) : '';
    this.mutationHistory.set(id, {
      id,
      op: (p.type ?? 'create') as MutationEventType,
      recordId,
      table: typeof p.tableName === 'string' ? p.tableName : recordId.split(':')[0],
      fields: p.data && typeof p.data === 'object' ? Object.keys(p.data) : [],
      queuedAt: queuedAtOf(id) || Date.now(),
    });
    trimHistory(this.mutationHistory, DevToolsService.MUTATIONS_CAP);
  }

  /** The server answered a write: accepted (`synced`) or rejected (`rolled-back`). */
  public onMutationOutcome(e: {
    mutationId: string;
    recordId: string;
    eventType: string;
    status: 'synced' | 'rolled-back';
    error?: string;
  }): void {
    if (!this.enabled) return;
    const prev = this.mutationHistory.get(e.mutationId);
    this.mutationHistory.set(e.mutationId, {
      ...(prev ?? {
        id: e.mutationId,
        op: e.eventType as MutationEventType,
        recordId: e.recordId,
        table: e.recordId.split(':')[0],
        queuedAt: queuedAtOf(e.mutationId),
      }),
      outcome: { status: e.status, at: Date.now(), error: e.error },
    });
    trimHistory(this.mutationHistory, DevToolsService.MUTATIONS_CAP);
    this.notifyDevTools();
  }

  /** Something the panel shows moved (a query's status, the outbox, sync health): re-push. */
  public onStateChanged(): void {
    this.notifyDevTools();
  }

  private mutationsState(): DevToolsMutationsState | null {
    const source = this.mutationSource;
    if (!source) return null;
    const s = source.state();
    return buildMutationsState({
      outbox: s.outbox,
      pendingWrites: s.pendingWrites.values(),
      failedCount: s.failedCount,
      tabRole: s.tabRole,
      health: s.sync.health,
      history: this.mutationHistory.values(),
      cap: DevToolsService.MUTATIONS_CAP,
    });
  }

  /**
   * The Mutations tab's on-demand half: the failed tray (a local read), one
   * queued row's payload, and the tray's retry / discard.
   */
  public async mutationOp(op: MutationOp, args: Record<string, unknown>): Promise<MutationOpResult> {
    const source = this.mutationSource;
    if (!source) return { success: false, error: 'this client does not expose its mutations' };
    const id = String(args.id ?? '');
    try {
      switch (op) {
        case 'listFailed':
          return this.serializeForDevTools({ success: true, failed: await source.listFailed() });
        case 'get': {
          const res = await this.databaseService.query<any>('SELECT * FROM $ids', {
            ids: [parseStoredRecordId(id)],
          });
          const raw = Array.isArray(res?.[0]) ? res[0][0] : undefined;
          return this.serializeForDevTools({ success: true, row: raw ? parsePendingRow(raw) : null });
        }
        case 'retry':
          return (await source.retryFailed(id))
            ? { success: true }
            : { success: false, error: `${id} is not in the failed tray` };
        case 'discard':
          return (await source.discardFailed(id))
            ? { success: true }
            : { success: false, error: `${id} is not in the failed tray` };
        case 'clearHistory':
          this.mutationHistory.clear();
          this.notifyDevTools();
          return { success: true };
        default:
          return { success: false, error: `Unknown mutation op: ${String(op)}` };
      }
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : String(e) };
    }
  }

  /** The Logs tab: read the buffer, change what it records, empty it. */
  public logOp(op: LogOp, args: Record<string, unknown>): LogOpResult {
    const tap = this.logTap;
    if (!tap) return { success: false, error: 'this client does not capture its logs' };
    try {
      switch (op) {
        case 'read': {
          const limit = typeof args.limit === 'number' ? args.limit : undefined;
          return { success: true, ...tap.read(Number(args.after ?? 0), limit) };
        }
        case 'setCaptureLevel':
          tap.setCaptureLevel(String(args.level) as Level);
          this.notifyDevTools();
          return { success: true, captureLevel: tap.captureLevel, consoleLevel: tap.consoleLevel };
        case 'clear':
          tap.clear();
          return { success: true, head: tap.head };
        default:
          return { success: false, error: `Unknown log op: ${String(op)}` };
      }
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : String(e) };
    }
  }

  /** Same coalescing as {@link notifyDevTools}, for the log delta channel. */
  private scheduleLogPush(): void {
    if (!this.enabled || typeof window === 'undefined' || this.logTimer !== null) return;
    const waited = Date.now() - this.lastLogPushAt;
    const delay = Math.max(0, DevToolsService.NOTIFY_MIN_INTERVAL_MS - waited);
    this.logTimer = setTimeout(() => {
      this.logTimer = null;
      if (this.enabled) this.flushLogs();
    }, delay);
  }

  private flushLogs(): void {
    const tap = this.logTap;
    if (!tap) return;
    this.lastLogPushAt = Date.now();
    const delta = tap.read(this.logCursor);
    this.logCursor = delta.head;
    if (delta.entries.length === 0) return;
    window.postMessage(
      {
        type: 'SP00KY_LOGS',
        source: 'sp00ky-devtools-page',
        entries: delta.entries,
        head: delta.head,
        dropped: delta.dropped,
      },
      '*'
    );
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
      // The event log is gone (the Logs and Mutations tabs carry what it did);
      // still sent empty because panels before canary.291 read it to tell this
      // state shape apart from their own.
      eventsHistory: [],
      activeQueries: Object.fromEntries(this.getActiveQueries()),
      auth: {
        authenticated: this.authService.isAuthenticated,
        userId: this.authService.currentUser?.id,
        impersonation: this.authService.impersonation,
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
      // Outbox, debounced writes, tray count and recent outcomes. Small by
      // construction (capped entries, field names only, never payloads).
      mutations: this.mutationsState(),
      // Log buffer metadata only; the lines travel as `SP00KY_LOGS` deltas and
      // through `logOp('read')`.
      logs: this.logTap
        ? {
            head: this.logTap.head,
            consoleLevel: this.logTap.consoleLevel,
            captureLevel: this.logTap.captureLevel,
          }
        : null,
    });
  }

  /**
   * The Access tab's impersonation controls. Errors come back as
   * `{ success: false, error }` with the server's own message, so the panel
   * can show why (not an admin, feature disabled, target is an admin).
   */
  public async impersonationOp(
    op: ImpersonationOp,
    args: Record<string, unknown>
  ): Promise<ImpersonationOpResult> {
    const enabled = this.schema.policy?.impersonation === true;
    try {
      switch (op) {
        case 'status': {
          const current = this.authService.impersonation;
          const snapshot = await this.flagsAdmin.getFlags().catch(() => null);
          const isAdmin = snapshot?.isAdmin === true;
          const active =
            enabled && isAdmin && !current
              ? await this.authService.listActiveImpersonations().catch(() => [])
              : [];
          return this.serializeForDevTools({ success: true, enabled, isAdmin, current, active });
        }
        case 'listUsers':
          return this.serializeForDevTools({
            success: true,
            users: await this.authService.searchImpersonationTargets(String(args.search ?? '')),
          });
        case 'start': {
          const current = await this.authService.impersonate(
            String(args.target ?? ''),
            String(args.reason ?? '')
          );
          return this.serializeForDevTools({ success: true, current });
        }
        case 'stop':
          await this.authService.stopImpersonating();
          return { success: true, current: null };
        default:
          return { success: false, error: `Unknown impersonation op: ${String(op)}` };
      }
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : String(e) };
    }
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
        // ---- Impersonation (Access tab) --------------------------------
        // Every op is authorized by SurrealDB (`fn::_00_impersonate::*` are
        // admin-only and absent unless the project enabled the feature);
        // nothing here grants anything.
        impersonationOp: (op: ImpersonationOp, args?: Record<string, unknown>) =>
          this.impersonationOp(op, args ?? {}),
        // ---- Mutations + Logs tabs ---------------------------------------
        mutationOp: (op: MutationOp, args?: Record<string, unknown>) => this.mutationOp(op, args ?? {}),
        logOp: (op: LogOp, args?: Record<string, unknown>) => this.logOp(op, args ?? {}),
        setFlagEnabled: (key: string, enabled: boolean) =>
          this.flagsAdmin.setFlagEnabled(key, enabled),
        setFlagUserVariant: (key: string, variant: string, remove: boolean, userId?: string) =>
          this.flagsAdmin.setFlagUserVariant(key, variant, remove, userId),
        setLocalFlagOverride: (key: string, variant: string | null, payload?: unknown) =>
          this.flagsAdmin.setLocalFlagOverride(key, variant, payload),
        clearLocalFlagOverrides: () => this.flagsAdmin.clearLocalFlagOverrides(),
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
