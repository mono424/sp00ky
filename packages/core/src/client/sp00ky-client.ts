import type { Surreal } from 'surrealdb';
import type {
  BackendNames,
  BackendRoutes,
  FinalQuery,
  GetTable,
  InnerQuery,
  QueryOptions,
  RoutePayload,
  SchemaStructure,
  TableModel,
  TableNames,
  BucketNames,
} from '@spooky-sync/query-builder';
import { QueryBuilder } from '@spooky-sync/query-builder';
import type {
  PreloadOptions,
  QueryAuthorityCallback,
  QueryStatusCallback,
  QueryTimeToLive,
  RunOptions,
  Sp00kyConfig,
  Sp00kyQueryResultPromise,
  StorageHealth,
  SyncHealth,
  UpdateOptions,
} from '../types';
import type { TabRole } from '../services/tabs/protocol';
import { StreamProcessorService } from '../services/stream-processor/index';
import type { StreamUpdateReceiver } from '../services/stream-processor/index';
import { AuthService } from '../modules/auth/index';
import { CrdtField } from '../modules/crdt/index';
import { FeatureFlagModule, FeatureFlagHandle } from '../modules/feature-flag/index';
import type { FeatureFlagOptions, FeatureFlagOverride } from '../modules/feature-flag/index';
import { AppReleaseModule, AppReleaseHandle } from '../modules/app-release/index';
import type { AppReleaseOptions } from '../modules/app-release/index';
import { DevToolsService } from '../modules/devtools/index';
import type { DevToolsQuerySource } from '../modules/devtools/index';
import { parseQueryParams, generateId } from '../utils/index';
import type { OutEvent, RuntimeEvent } from '../kernel/events';
import type { ClientState } from '../state/client-state';
import * as R from '../state/reducers';
import { pendingMutationCount, fetchingQueryCount, phaseTimings, toQueryState } from '../state/selectors';
import { defaultEnv, type SagaEnv } from '../query/env';
import { registerLocal, type RegisterInput } from '../query/register.saga';
import { evictQuery } from '../query/lifecycle.saga';
import { write } from '../mutation/write.saga';
import { discardFailed, listFailed, retryFailed } from '../mutation/tray.saga';
import type { FailedMutationRow } from '../mutation/rows';
import { buildJobRecord } from '../mutation/jobs';
import { boot } from '../boot/boot.saga';
import { preload } from '../boot/preload.saga';
import { DEGRADE_AFTER_FAILURES, OUTBOX_BATCH_SIZE, PUSH_TIMEOUT_MS } from '../kernel/constants';
import { Runtime } from './runtime';
import { createAdapters, createServices, createTabsCoordinator, type ServiceHost, type Services } from './services';
import { BucketHandle } from './bucket-handle';
import type { LeaderSyncHub, SyncForwarder } from '../services/tabs/coordinator';

const UNKNOWN_STORAGE_HEALTH: StorageHealth = Object.freeze({ status: 'unknown', fallback: false });

export interface Sp00kyClientDeps<S extends SchemaStructure> {
  services?: Services<S>;
  runtime?: Runtime;
  env?: Partial<SagaEnv>;
}

/**
 * The public client. Every method is a thin translation into a saga run, a
 * state read, or a subscription on the runtime; the engine's behaviour lives
 * in the sagas.
 */
export class Sp00kyClient<S extends SchemaStructure> {
  public readonly auth: AuthService<S>;
  public readonly streamProcessor: StreamProcessorService;

  private readonly services: Services<S>;
  private readonly env: SagaEnv;
  private readonly runtime: Runtime;
  private readonly featureFlags: FeatureFlagModule<S>;
  private readonly appReleases: AppReleaseModule<S>;
  private readonly devTools: DevToolsService;
  private hub: LeaderSyncHub | null = null;
  private forwarder: SyncForwarder | null = null;
  private detachWindow: (() => void) | null = null;
  private sharedActive = false;

  constructor(
    private readonly config: Sp00kyConfig<S>,
    deps: Sp00kyClientDeps<S> = {}
  ) {
    this.services = deps.services ?? createServices(config);
    const s = this.services;
    this.env = defaultEnv(config.schema, {
      anonLive: config.enableAnonymousLiveQueries === true,
      remoteTimeoutMs: Math.max(0, config.database.queryTimeoutMs ?? 60_000),
      pushTimeoutMs: config.pushTimeoutMs ?? PUSH_TIMEOUT_MS,
      outboxBatchSize: OUTBOX_BATCH_SIZE,
      degradeAfter: config.syncHealth === false ? 0 : (config.syncHealth?.degradeAfterConsecutiveFailures ?? DEGRADE_AFTER_FAILURES),
      materializeDebounceMs: config.streamDebounceTime ?? 50,
      pollBaseMs: config.refSyncIntervalMs && config.refSyncIntervalMs > 0 ? config.refSyncIntervalMs : 500,
      ...deps.env,
    });
    const host: ServiceHost = {
      dispatch: (event) => void this.runtime.dispatch(event),
      setTabsChannel: ({ hub, forwarder }) => {
        this.hub = hub;
        this.forwarder = forwarder;
        this.sharedActive = hub !== null || forwarder !== null;
      },
      initModules: () => undefined,
      detachWindow: () => this.detachWindow?.(),
      setDetachWindow: (fn) => (this.detachWindow = fn),
    };
    if (!deps.runtime && s.tabs === null && s.tabsUnsupportedReason === undefined) {
      s.tabs = createTabsCoordinator(config, s, host);
    }
    this.runtime =
      deps.runtime ??
      new Runtime({
        env: this.env,
        adapters: createAdapters(config, s, host, () => [this.featureFlags, this.appReleases]),
        logger: s.logger,
        tabId: s.tabId,
      });
    this.auth = s.auth;
    this.streamProcessor = s.streamProcessor;

    const liveQueryPort = {
      query: (table: string, surql: string, params: Record<string, unknown>, ttl: QueryTimeToLive) =>
        this.runtime.run(registerLocal(this.env, { tableName: table, surql, params, ttl })),
      subscribe: (hash: string, cb: (records: Record<string, any>[]) => void, options?: { immediate?: boolean }) =>
        this.runtime.subscribe(hash, cb, options),
    };
    const remoteRegisterPort = { enqueueDownEvent: () => void this.runtime.dispatch({ type: 'EnsureRegistered' }) };
    this.featureFlags = new FeatureFlagModule<S>({ dataModule: liveQueryPort, sync: remoteRegisterPort, auth: s.auth, logger: s.logger });
    this.appReleases = new AppReleaseModule<S>({ dataModule: liveQueryPort, sync: remoteRegisterPort, auth: s.auth, logger: s.logger });

    const querySource: DevToolsQuerySource = {
      getActiveQueries: () => [...this.runtime.state.queries.values()].map(toQueryState),
      getQueryById: (id) => {
        const entry = [...this.runtime.state.queries.values()].find((e) => String(e.def.id) === String(id));
        return entry ? toQueryState(entry) : undefined;
      },
      phaseTimings: (q) => {
        const entry = this.runtime.state.queries.get(q.config.id.id as string) ?? [...this.runtime.state.queries.values()].find((e) => String(e.def.id) === String(q.config.id));
        return entry ? phaseTimings(entry) : ({} as never);
      },
    };
    this.devTools = new DevToolsService(s.local, s.remote, s.logger, config.schema, s.auth, querySource);
    this.devTools.setFeatureFlagOverrides(this.featureFlags);
    s.streamProcessor.addReceiver(this.devTools);
    this.devTools.setBlobInfoProvider(() => s.blobs.stats());
    if (config.sharedTabs) {
      this.devTools.setTabsInfoProvider(() => {
        const c = s.tabs;
        if (!this.sharedActive || !c) return { active: false, reason: s.tabsUnsupportedReason ?? 'fell-back' };
        return {
          active: true,
          role: c.role,
          tabId: c.tabId,
          leadershipId: c.leadershipId,
          leaderTabId: c.role === 'leader' ? c.tabId : c.leaderTabId,
          ...(this.hub ? { followers: this.hub.followerCount, relayedBatches: this.hub.relayedBatches } : {}),
        };
      });
    }
    this.wireAdapters();
  }

  /** Adapter callbacks become runtime events; runtime events reach the legacy services. */
  private wireAdapters(): void {
    const s = this.services;
    const receiver: StreamUpdateReceiver = { onStreamUpdate: (update) => void this.runtime.dispatch({ type: 'StreamUpdate', update }) };
    s.streamProcessor.addReceiver(receiver);
    s.connectionSupervisor.subscribe((state) => void this.runtime.dispatch({ type: 'ConnectionChanged', state }));
    s.auth.subscribe((userId) => void this.runtime.dispatch({ type: 'AuthFlip', userId }));
    this.runtime.on('tabs:broadcast', (e) => {
      if (e.type !== 'tabs:broadcast') return;
      const msg = e.message as { type: string; records?: unknown[]; mutationId?: string };
      if (this.hub) {
        if (msg.type === 'ingest') this.hub.relayIngest(msg.records as never);
        else this.hub.broadcast(msg as never);
      } else if (this.forwarder) {
        if (msg.type === 'ingest') this.forwarder.ingest(msg.records as never);
        else if (msg.type === 'outbox-changed' && msg.mutationId) this.forwarder.mutationEnqueued(msg.mutationId);
      }
    });
    this.runtime.on('tabs:sendTo', (e) => {
      if (e.type === 'tabs:sendTo' && this.hub) this.hub.sendTo(e.tabId, e.message as never);
    });
    this.runtime.on('*', (e) => this.forwardToDevTools(e));
    s.local.getEvents().subscribe('DATABASE_LOCAL_QUERY', (event: any) => this.devTools.logEvent('LOCAL_QUERY', event.payload));
    s.remote.getEvents().subscribe('DATABASE_REMOTE_QUERY', (event: any) => this.devTools.logEvent('REMOTE_QUERY', event.payload));
  }

  private forwardToDevTools(e: OutEvent): void {
    switch (e.type) {
      case 'query:status':
        this.devTools.logEvent('QUERY_STATUS_CHANGED', { queryHash: e.hash, status: e.status });
        break;
      case 'query:authority':
        this.devTools.logEvent('QUERY_AUTHORITY_CHANGED', { queryHash: e.hash, known: e.known });
        break;
      case 'query:view-lost':
        this.devTools.logEvent('QUERY_VIEW_LOST', { queryHash: e.hash });
        break;
      case 'mutation:event':
        this.devTools.onMutation([e.event]);
        break;
      case 'devtools':
        this.devTools.logEvent(e.name, e.data);
        break;
      default:
        break;
    }
  }

  // ---- lifecycle ---------------------------------------------------------------

  async init(): Promise<void> {
    await this.runtime.run(boot(this.env, { sharedTabs: this.services.tabs !== null }));
  }

  async close(): Promise<void> {
    const s = this.services;
    s.connectionSupervisor.dispose();
    this.runtime.dispose();
    await this.featureFlags.closeAll();
    await this.appReleases.closeAll();
    s.crdt.closeAll();
    s.crdt.dispose();
    await s.blobs.close().catch(() => {});
    await s.streamProcessor.checkpoint('close').catch(() => {});
    if (s.tabs) await s.tabs.stop();
    this.detachWindow?.();
    this.detachWindow = null;
    await s.local.close();
    await s.remote.close();
    s.streamProcessor.dispose();
  }

  isLocalReady(): boolean {
    return this.runtime.state.localReady;
  }

  // ---- queries -----------------------------------------------------------------

  query<Table extends TableNames<S>>(
    table: Table,
    options: QueryOptions<TableModel<GetTable<S, Table>>, false>,
    ttl: QueryTimeToLive = '10m'
  ): QueryBuilder<S, Table, Sp00kyQueryResultPromise> {
    return new QueryBuilder<S, Table, Sp00kyQueryResultPromise>(this.config.schema, table, async (q) => ({ hash: await this.initQuery(table, q, ttl) }), options);
  }

  private registerInput(table: string, q: InnerQuery<any, any, any>, ttl: QueryTimeToLive): RegisterInput {
    const tableSchema = this.config.schema.tables.find((t) => t.name === table);
    if (!tableSchema) throw new Error(`Table ${table} not found`);
    return {
      tableName: table,
      surql: q.selectQuery.query,
      params: parseQueryParams(tableSchema.columns, q.selectQuery.vars ?? {}),
      ttl,
      plan: q.selectQuery.plan,
    };
  }

  private initQuery(table: string, q: InnerQuery<any, any, any>, ttl: QueryTimeToLive): Promise<string> {
    return this.runtime.run(registerLocal(this.env, this.registerInput(table, q, ttl)));
  }

  async queryRaw(sql: string, params: Record<string, any>, ttl: QueryTimeToLive): Promise<string> {
    const tableName = sql.split('FROM ')[1]?.split(' ')[0] ?? '';
    return this.runtime.run(registerLocal(this.env, { tableName, surql: sql, params, ttl }));
  }

  /**
   * Preload = register a query nobody subscribes to. Resolved before on this
   * device: returns at once. Never resolved: resolves when the server's
   * membership and every body are local. `signal` aborts the wait.
   */
  async preload(finalQuery: FinalQuery<S, any, any, any, any, any>, options?: PreloadOptions): Promise<void> {
    const q = finalQuery.innerQuery;
    await this.runtime.run(preload(this.env, this.registerInput(q.tableName, q, '10m')), { signal: options?.signal });
  }

  async subscribe(queryHash: string, callback: (records: Record<string, any>[]) => void, options?: { immediate?: boolean }): Promise<() => void> {
    return this.runtime.subscribe(queryHash, callback, options);
  }

  deregisterQuery(queryHash: string): void {
    const entry = this.runtime.state.queries.get(queryHash);
    if (!entry || entry.subscribers > 0) return;
    void this.runtime.run(evictQuery(queryHash), { lane: { kind: 'serial', key: `mat:${queryHash}` } });
  }

  subscribeQueryStatus(queryHash: string, callback: QueryStatusCallback, options?: { immediate?: boolean }): () => void {
    return this.runtime.subscribeStatus(queryHash, callback, options);
  }

  subscribeQueryAuthority(queryHash: string, callback: QueryAuthorityCallback, options?: { immediate?: boolean }): () => void {
    return this.runtime.subscribeAuthority(queryHash, callback, options);
  }

  reportFrontendTiming(queryHash: string, ms: number): void {
    if (Number.isFinite(ms)) this.runtime.update(R.recordPhase(queryHash, 'frontend', ms));
  }

  // ---- mutations ---------------------------------------------------------------

  async create<T extends Record<string, unknown>>(id: string, data: T): Promise<T> {
    const out = await this.runtime.run(write(this.env, { kind: 'create', recordId: id, data }));
    return (out.record ?? data) as T;
  }

  async update<T extends Record<string, unknown>>(_table: string, id: string, data: Partial<T>, options?: UpdateOptions): Promise<T> {
    const out = await this.runtime.run(write(this.env, { kind: 'update', recordId: id, data: data as Record<string, unknown>, options }));
    return (out.record ?? data) as T;
  }

  async delete(_table: string, id: string): Promise<void> {
    await this.runtime.run(write(this.env, { kind: 'delete', recordId: id }));
  }

  async run<B extends BackendNames<S>, R extends BackendRoutes<S, B>>(backend: B, path: R, payload: RoutePayload<S, B, R>, options?: RunOptions): Promise<void> {
    const { tableName, record } = buildJobRecord(this.config.schema, String(backend), String(path), payload as Record<string, unknown>, options);
    await this.create(`${tableName}:${generateId()}`, record);
  }

  // ---- failed-writes tray -------------------------------------------------------

  get failedMutationCount(): number {
    return this.runtime.state.failedCount;
  }

  subscribeToFailedMutations(cb: (count: number) => void): () => void {
    cb(this.runtime.state.failedCount);
    return this.runtime.on('tray:changed', (e) => {
      if (e.type === 'tray:changed') cb(e.count);
    });
  }

  listFailedMutations(): Promise<FailedMutationRow[]> {
    return this.runtime.run(listFailed());
  }

  async retryFailedMutation(mutationId: string): Promise<boolean> {
    return this.runtime.run(retryFailed(this.env, mutationId), { lane: { kind: 'serial', key: 'tray' } });
  }

  async discardFailedMutation(mutationId: string): Promise<boolean> {
    return this.runtime.run(discardFailed(mutationId), { lane: { kind: 'serial', key: 'tray' } });
  }

  // ---- observability -----------------------------------------------------------

  get tabRole(): TabRole | null {
    return this.sharedActive && this.services.tabs ? this.services.tabs.role : null;
  }

  get remoteClient(): Surreal {
    return this.services.remote.getClient();
  }

  get localClient(): unknown {
    return this.services.local.getClient();
  }

  get pendingMutationCount(): number {
    return pendingMutationCount(this.runtime.state);
  }

  /** Kept for the e2e suite; the saga core has no LIVE retry ladder. */
  get liveRetryCount(): number {
    return 0;
  }

  subscribeToPendingMutations(cb: (count: number) => void): () => void {
    cb(this.pendingMutationCount);
    return this.runtime.on('activity:changed', (e) => {
      if (e.type === 'activity:changed') cb(e.pending);
    });
  }

  get fetchingQueryCount(): number {
    return fetchingQueryCount(this.runtime.state);
  }

  subscribeToFetchActivity(cb: (fetching: number) => void): () => void {
    cb(this.fetchingQueryCount);
    return this.runtime.on('activity:changed', (e) => {
      if (e.type === 'activity:changed') cb(e.fetching);
    });
  }

  get syncHealth(): SyncHealth {
    return this.runtime.state.sync.health;
  }

  subscribeToSyncHealth(cb: (health: SyncHealth) => void): () => void {
    cb(this.syncHealth);
    return this.runtime.on('health:changed', (e) => {
      if (e.type === 'health:changed') cb(e.health);
    });
  }

  get storageHealth(): StorageHealth {
    return this.services.local.storageHealth ?? UNKNOWN_STORAGE_HEALTH;
  }

  subscribeToStorageHealth(cb: (health: StorageHealth) => void): () => void {
    if (this.services.local.subscribeToStorageHealth) return this.services.local.subscribeToStorageHealth(cb);
    cb(UNKNOWN_STORAGE_HEALTH);
    return () => {};
  }

  /** The engine state, read-only. For tests and DevTools. */
  get state(): ClientState {
    return this.runtime.state;
  }

  /** Feed an event into the engine. For adapters, tests and DevTools. */
  dispatch(event: RuntimeEvent): Promise<void> {
    return this.runtime.dispatch(event);
  }

  // ---- auth / remote / crdt / flags / buckets ------------------------------------

  authenticate(token: string) {
    this.services.remote.setAuthToken(token);
    return this.services.remote.getClient().authenticate(token);
  }

  deauthenticate() {
    return this.services.remote.getClient().invalidate();
  }

  async useRemote<T>(fn: (client: Surreal) => Promise<T> | T): Promise<T> {
    return fn(this.services.remote.getClient());
  }

  async remoteQuery<T extends unknown[]>(sql: string, vars?: Record<string, unknown>): Promise<T> {
    return this.services.remote.query<T>(sql, vars);
  }

  feature(key: string, options?: FeatureFlagOptions): FeatureFlagHandle {
    return this.featureFlags.feature(key, options);
  }

  setFeatureOverride(key: string, variant: string | null, payload?: unknown): void {
    this.featureFlags.setLocalOverride(key, variant, payload);
  }

  clearFeatureOverrides(): void {
    this.featureFlags.clearLocalOverrides();
  }

  getFeatureOverrides(): Record<string, FeatureFlagOverride> {
    return this.featureFlags.getLocalOverrides();
  }

  appRelease(app: string, options?: AppReleaseOptions): AppReleaseHandle {
    return this.appReleases.release(app, options);
  }

  async openCrdtField(table: string, recordId: string, field: string, fallbackText?: string): Promise<CrdtField> {
    return this.services.crdt.open(table, recordId, field, fallbackText);
  }

  closeCrdtField(table: string, recordId: string, field: string): void {
    this.services.crdt.close(table, recordId, field);
  }

  bucket<B extends BucketNames<S>>(name: B): BucketHandle {
    return new BucketHandle(name, this.services.remote, this.services.blobs, { blurhash: this.config.blurhash, logger: this.services.logger });
  }

  getBlobCacheStats() {
    return this.services.blobs.stats();
  }
}

