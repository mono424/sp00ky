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
  ChannelPeer,
  ChannelTableNames,
} from '@spooky-sync/query-builder';
import type { ChannelQuery } from '../types';
import { QueryBuilder } from '@spooky-sync/query-builder';
import type {
  PreloadOptions,
  QueryAuthorityCallback,
  QueryStatusCallback,
  QueryTimeToLive,
  RunOptions,
  Sp00kyChannel,
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
import { WebPushModule } from '../modules/web-push/index';
import { ChannelManager, type ChannelHandle, type ChannelState, type ChannelTransportFactory } from '../modules/channel/index';
import { surrealSignal, webrtcTransport } from '../modules/channel/webrtc';
import { roomLink, splitTransport } from '../modules/channel/room';
import { hubTransport } from '../modules/channel/hub';
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
import { peerCreate, peerDelete, readRow, write } from '../mutation/write.saga';
import { discardFailed, listFailed, retryFailed } from '../mutation/tray.saga';
import type { FailedMutationRow } from '../mutation/rows';
import { buildJobRecord } from '../mutation/jobs';
import { boot } from '../boot/boot.saga';
import { preload } from '../boot/preload.saga';
import { DEGRADE_AFTER_FAILURES, OUTBOX_BATCH_SIZE, PUSH_TIMEOUT_MS } from '../kernel/constants';
import { Runtime } from './runtime';
import { createAdapters, createServices, createTabsCoordinator, readBootBucketHint, type ServiceHost, type Services } from './services';
import { ANON_USER_ID } from '../modules/ref-tables';
import { paintKeyOf } from '../query/materialize';
import { BucketHandle, bucketSettings } from './bucket-handle';
import { ImpersonationBanner } from '../modules/auth/impersonation-banner';
import type { LeaderSyncHub, SyncForwarder } from '../services/tabs/coordinator';

const UNKNOWN_STORAGE_HEALTH: StorageHealth = Object.freeze({ status: 'unknown', fallback: false });
/** Longest SurrealQL a statement log line shows as its message. */
const STATEMENT_MSG_MAX = 160;

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
/** A promise and the two ways to settle it; settling twice does nothing. */
function deferred(): { promise: Promise<void>; resolve: () => void; reject: (error: unknown) => void } {
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

export class Sp00kyClient<S extends SchemaStructure> {
  public readonly auth: AuthService<S>;
  public readonly streamProcessor: StreamProcessorService;
  /** Web Push for this browser: subscribe, devices, self-pushes, service worker bridge. */
  public readonly webPush: WebPushModule;
  /** Peer to peer channels of `-- @channel` tables. */
  private readonly channels: ChannelManager;

  private readonly services: Services<S>;
  private readonly env: SagaEnv;
  private readonly runtime: Runtime;
  private readonly featureFlags: FeatureFlagModule<S>;
  private readonly appReleases: AppReleaseModule<S>;
  /** Teardown for subscriptions the client itself owns. */
  private readonly disposers: Array<() => void> = [];
  /** Assigned in `wireAdapters`, which the constructor always runs. */
  private impersonationBanner!: ImpersonationBanner;
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
      // Feature flags and app releases register here, from boot (which has a
      // session by the time it asks) and from hooks. A hook on a client that
      // is not open yet has no session to register under: it waits, where it
      // used to fail and only be tried again at the next sign-in.
      query: async (table: string, surql: string, params: Record<string, unknown>, ttl: QueryTimeToLive) => {
        if (this.runtime.state.sessionId === null) await this.ready.promise;
        return this.runtime.run(registerLocal(this.env, { tableName: table, surql, params, ttl }));
      },
      subscribe: (hash: string, cb: (records: Record<string, any>[]) => void, options?: { immediate?: boolean }) =>
        this.runtime.subscribe(hash, cb, options),
    };
    const remoteRegisterPort = { enqueueDownEvent: () => void this.runtime.dispatch({ type: 'EnsureRegistered' }) };
    // CRDT edit logs ride ordinary queries and the ordinary outbox.
    s.crdt.attach({
      watch: async (table, where, onRows) => {
        const { hash } = await (this.query as any)(table, {}).where(where).build().run();
        const stopRows = this.runtime.subscribe(hash, onRows, { immediate: true });
        let known = false;
        let idle = false;
        let done!: () => void;
        const settled = new Promise<void>((resolve) => (done = resolve));
        const check = () => known && idle && done();
        const stopAuthority = this.runtime.subscribeAuthority(hash, (k) => ((known = k), check()), { immediate: true });
        const stopStatus = this.runtime.subscribeStatus(hash, (st) => ((idle = st === 'idle'), check()), { immediate: true });
        return {
          stop: () => {
            stopRows();
            stopAuthority();
            stopStatus();
          },
          settled,
        };
      },
      create: (id, data) => this.create(id, data),
    });
    this.featureFlags = new FeatureFlagModule<S>({ dataModule: liveQueryPort, sync: remoteRegisterPort, auth: s.auth, logger: s.logger });
    this.appReleases = new AppReleaseModule<S>({ dataModule: liveQueryPort, sync: remoteRegisterPort, auth: s.auth, logger: s.logger });
    this.webPush = new WebPushModule({
      remote: s.remote,
      auth: s.auth,
      persistence: s.persistence,
      logger: s.logger,
      database: config.database,
      connection: s.connectionSupervisor,
      config: config.webPush,
    });
    this.disposers.push(this.webPush.attach());

    const channelsConfig = config.channels;
    const relay = typeof channelsConfig?.relay === 'string' ? { url: channelsConfig.relay } : channelsConfig?.relay;
    // Between users: a WebRTC link per pair of tabs, signaled through the database.
    const mesh: ChannelTransportFactory = (hooks) =>
      webrtcTransport(hooks, {
        signal: surrealSignal(s.remote, s.connectionSupervisor, s.logger),
        iceServers: channelsConfig?.iceServers,
        logger: s.logger,
      });
    // Between the user's own devices: one link per browser to their room on the relay.
    const room: ChannelTransportFactory | null = relay
      ? (hooks) =>
          hubTransport(hooks, {
            scope: (user) => `${config.database.namespace}/${config.database.database}/${user}`,
            link: (linkHooks) =>
              roomLink(linkHooks, {
                url: relay.url,
                direct: relay.direct,
                token: () => s.remote.getAuthToken(),
                iceServers: channelsConfig?.iceServers,
                logger: s.logger,
              }),
            logger: s.logger,
          })
      : null;
    this.channels = new ChannelManager(config.schema, {
      config: channelsConfig,
      logger: s.logger,
      rows: {
        create: (_table, id, row, ttlMs) => this.runtime.run(peerCreate(this.env, { recordId: id, data: row, ttlMs })),
        delete: (_table, id, ttlMs) => this.runtime.run(peerDelete(this.env, { recordId: id, ttlMs })),
        read: (id) => this.runtime.run(readRow(id)),
      },
      transport: channelsConfig?.transport ?? splitTransport(room, mesh),
    });
    if (this.channels.sharing && !relay && !channelsConfig?.transport) {
      s.logger.warn(
        { Category: 'sp00ky-client::channel' },
        'channels.share is set without channels.relay: nothing reaches your other devices until a relay address is given'
      );
    }
    if (this.channels.enabled) this.disposers.push(s.auth.subscribe((userId) => this.channels.setUser(userId)));

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
    this.devTools.setMutationSource({
      state: () => this.runtime.state,
      listFailed: () => this.listFailedMutations(),
      retryFailed: (id) => this.retryFailedMutation(id),
      discardFailed: (id) => this.discardFailedMutation(id),
    });
    if (s.logTap) this.devTools.setLogTap(s.logTap);
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
    // Unobserved, a failed boot must not also be an unhandled rejection.
    this.ready.promise.catch(() => {});
    // The bucket `boot` is about to open, so the store can paint from it now
    // (`peekQuery`). Alone in the store only: see `LocalStore.expectBucket`.
    if (!s.tabs) s.local.expectBucket?.(readBootBucketHint() ?? ANON_USER_ID);
  }

  /** Adapter callbacks become runtime events; runtime events reach the legacy services. */
  private wireAdapters(): void {
    const s = this.services;
    const receiver: StreamUpdateReceiver = { onStreamUpdate: (update) => void this.runtime.dispatch({ type: 'StreamUpdate', update }) };
    s.streamProcessor.addReceiver(receiver);
    s.connectionSupervisor.subscribe((state) => void this.runtime.dispatch({ type: 'ConnectionChanged', state }));
    // Changes only. The immediate answer here is always "signed out" (nothing
    // is restored before `init()`), and boot owns the first identity: it opens
    // the bucket the hint names and announces the restored session itself. As
    // a flip, that answer switched to the anon bucket while boot was still
    // opening the user's, so two clients provisioned at once, boot failed, and
    // a persistent store never got past the provider's fallback on reload.
    s.auth.subscribe((userId) => void this.runtime.dispatch({ type: 'AuthFlip', userId }), { immediate: false });
    // Whoever is looking at the page should be able to tell, and leave. The
    // app can restyle this, replace it (`mode: 'custom'`, with a fallback if
    // its own never renders) or take the job over entirely (`mode: 'none'`).
    this.impersonationBanner = new ImpersonationBanner(
      () => s.auth.stopImpersonating(),
      this.config.impersonationBanner
    );
    const banner = this.impersonationBanner;
    this.disposers.push(
      s.auth.subscribeImpersonation((info) => banner.update(info)),
      () => banner.unmount()
    );
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
    // One debug line per finished statement, for both stores and every engine
    // (the SQLite engine logs none of its own). Guarded: this is the hottest
    // path there is, and at the default `info` the line is never written.
    // The message is the SurrealQL itself, so the Logs tab reads like a query
    // trail; a long statement is cut there and kept whole in `query`.
    const logStatement = (store: 'local' | 'remote') => (event: any) => {
      if (!s.logger.isLevelEnabled('debug')) return;
      const q = event.payload ?? {};
      const sql = String(q.query ?? '').replace(/\s+/g, ' ').trim();
      const cut = sql.length > STATEMENT_MSG_MAX;
      s.logger.debug(
        {
          store,
          ...(cut ? { query: q.query } : {}),
          vars: q.vars,
          ms: Math.round((q.duration ?? 0) * 10) / 10,
          ok: q.success !== false,
          error: q.error,
          Category: `sp00ky-client::${store}`,
        },
        cut ? `${sql.slice(0, STATEMENT_MSG_MAX)}…` : sql
      );
    };
    s.local.getEvents().subscribe('DATABASE_LOCAL_QUERY', logStatement('local'));
    s.remote.getEvents().subscribe('DATABASE_REMOTE_QUERY', logStatement('remote'));
  }

  /** Runtime events the DevTools panel follows; query transitions also go to the log. */
  private forwardToDevTools(e: OutEvent): void {
    const log = this.services.logger;
    switch (e.type) {
      case 'query:status':
        log.debug({ hash: e.hash, status: e.status, Category: 'sp00ky-client::query' }, 'Query status');
        this.devTools.onStateChanged();
        break;
      case 'query:authority':
        log.debug({ hash: e.hash, known: e.known, Category: 'sp00ky-client::query' }, e.known ? 'Query membership known' : 'Query membership unknown');
        this.devTools.onStateChanged();
        break;
      case 'query:view-lost':
        log.info({ hash: e.hash, Category: 'sp00ky-client::query' }, 'Query view lost on the server; re-registering');
        this.devTools.onStateChanged();
        break;
      case 'mutation:event':
        this.devTools.onMutation([e.event]);
        break;
      case 'mutation:settled':
        this.devTools.onMutationOutcome({ mutationId: e.mutationId, recordId: e.recordId, eventType: e.eventType, status: 'synced' });
        break;
      case 'mutation:rolled-back':
        this.devTools.onMutationOutcome({ mutationId: e.mutationId, recordId: e.recordId, eventType: e.eventType, status: 'rolled-back', error: e.error });
        break;
      case 'tray:changed':
      case 'health:changed':
      case 'activity:changed':
        this.devTools.onStateChanged();
        break;
      default:
        break;
    }
  }

  // ---- lifecycle ---------------------------------------------------------------

  /** Settled by `init`: what a call made before the client is open waits on. */
  private ready = deferred();

  async init(): Promise<void> {
    try {
      await this.runtime.run(boot(this.env, { sharedTabs: this.services.tabs !== null }));
      this.ready.resolve();
    } catch (error) {
      this.ready.reject(error);
      throw error;
    }
  }

  /**
   * Resolves once `init()` has finished, and rejects with its error if it
   * fails. A page that renders before the client is open (a provider's
   * `eager` mode) awaits this for whatever must not run earlier.
   */
  whenReady(): Promise<void> {
    return this.ready.promise;
  }

  /**
   * Queries and writes need the open client: its store, its session. Asked
   * for earlier, they wait for `init()` to finish, so a component may register
   * a query on its first render and need not know whether the client is open.
   */
  private opened(): Promise<void> | undefined {
    return this.runtime.state.localReady ? undefined : this.ready.promise;
  }

  async close(): Promise<void> {
    this.ready.reject(new Error('Sp00kyClient: closed before it was ready'));
    const s = this.services;
    s.connectionSupervisor.dispose();
    for (const dispose of this.disposers.splice(0)) dispose();
    this.channels.close();
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
    return new QueryBuilder<S, Table, Sp00kyQueryResultPromise>(
      this.config.schema,
      table,
      async (q) => ({ hash: await this.initQuery(table, q, ttl) }),
      options
    );
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

  private async initQuery(table: string, q: InnerQuery<any, any, any>, ttl: QueryTimeToLive): Promise<string> {
    await this.opened();
    return this.runtime.run(registerLocal(this.env, this.registerInput(table, q, ttl)));
  }

  /**
   * The rows this query rendered the last time this browser had the app open,
   * handed back at once and with no need for `init()` to have run: something
   * for a component to render on its first pass, where it would have rendered
   * an empty list. `null` when the local store keeps no such copy (only
   * `localEngine: 'rows'` with its paint cache does, and not with shared
   * tabs), or has none for this query.
   *
   * They are rows to paint from, not a result. The query still has to be run
   * and subscribed to; what that delivers is the answer, and it replaces these
   * rows when it differs.
   */
  peekQuery(finalQuery: FinalQuery<S, any, any, any, any, any>): Record<string, any>[] | null {
    try {
      const q = finalQuery.innerQuery;
      const input = this.registerInput(q.tableName, q, '10m');
      if (!input.plan) return null;
      // The membership render's rows only: a cold scan's are filtered by state the closed client does not have.
      return this.services.local.peekSelect?.(input.plan, input.params, { paintKey: paintKeyOf(input, []) }) ?? null;
    } catch {
      return null;
    }
  }

  /**
   * The raw remote escape hatches bypass the query builder, so their shapes
   * can never be on the server's query allowlist. When the generated schema
   * says the allowlist is on, refuse them unless the app opted in explicitly.
   */
  private assertRawRemoteAllowed(method: string): void {
    const mode = this.config.schema.policy?.queryAllowlist;
    if ((mode === 'warn' || mode === 'enforce') && !this.config.allowRawRemote) {
      throw new Error(
        `sp00ky: ${method} is disabled because sync.queryAllowlist is on for this schema; pass allowRawRemote: true in the client config to opt in`
      );
    }
  }

  async queryRaw(sql: string, params: Record<string, any>, ttl: QueryTimeToLive): Promise<string> {
    this.assertRawRemoteAllowed('queryRaw');
    await this.opened();
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
    await this.opened();
    await this.runtime.run(preload(this.env, this.registerInput(q.tableName, q, '10m')), { signal: options?.signal });
  }

  async subscribe(queryHash: string, callback: (records: Record<string, any>[]) => void, options?: { immediate?: boolean }): Promise<() => void> {
    if (!this.channels.enabled) return this.runtime.subscribe(queryHash, callback, options);
    // Channel values (peer updates, ephemeral fields) are folded into what
    // subscribers see, never into the store.
    // With `channels.share`, the user's other devices are told what this
    // query can show, and only send what it has a use for.
    const def = this.channels.sharing ? this.runtime.state.queries.get(queryHash)?.def : undefined;
    const tap = this.channels.tap(callback, def ? this.channels.interestOf(def.plan, def.params) : null);
    const stop = this.runtime.subscribe(queryHash, tap.feed, options);
    return () => {
      stop();
      tap.stop();
    };
  }

  /**
   * Open a peer to peer channel to `peers` (your own id is skipped; a
   * connection forms only when both sides list each other) for the rows
   * `queries` match, each a query on a `-- @channel` table. Only its `where`
   * counts, evaluated against each row; the query is never registered. Writes
   * to matching rows (plain or the channel's own) reach the peers. Close it
   * when done.
   */
  channel<T extends ChannelTableNames<S>>(
    queries: ChannelQuery<S, T> | ReadonlyArray<ChannelQuery<S, T>>,
    peers: Iterable<ChannelPeer>
  ): Sp00kyChannel<S, T> {
    const handle = this.channels.open(this.channelScopes(queries), peers);
    const scoped = (table: string) => {
      this.channels.assertCarries(handle, table);
      return handle;
    };
    return {
      tables: handle.tables as () => ReadonlyArray<T>,
      state: handle.state,
      subscribe: handle.subscribe,
      setPeers: handle.setPeers,
      setQueries: (next) => handle.setScopes(this.channelScopes(next)),
      close: handle.close,
      // Like every write, these wait for a client that is not open yet. A
      // table the channel does not carry is still refused at once, in the call.
      update: ((table: string, id: string, data: Record<string, unknown>, options?: UpdateOptions) => {
        const scope = scoped(table);
        return Promise.resolve(this.opened()).then(() => this.writeUpdate(table, id, data, options, scope));
      }) as never,
      create: ((id: string, data: Record<string, unknown>) => {
        const scope = scoped(id.slice(0, id.indexOf(':')));
        return Promise.resolve(this.opened()).then(() => this.writeCreate(id, data, scope));
      }) as never,
      delete: (table, id) => {
        const scope = scoped(table);
        return Promise.resolve(this.opened()).then(() => this.writeDelete(table, id, scope));
      },
    };
  }

  /**
   * The link to this user's other devices (`channels.share`): `connected`
   * with the user's id in `peers` while at least one other tab of theirs, in
   * another browser, is in the room. `idle` when nothing is shared or nobody
   * is signed in.
   */
  shareState(): ChannelState {
    return this.channels.ownState();
  }

  /** Called at once with `shareState()`, then on every change. */
  subscribeShareState(callback: (state: ChannelState) => void): () => void {
    return this.channels.subscribeOwn(callback);
  }

  private channelScopes(queries: ChannelQuery<S, any> | ReadonlyArray<ChannelQuery<S, any>>) {
    const list = (Array.isArray(queries) ? queries : [queries]) as ReadonlyArray<ChannelQuery<S, any>>;
    return list.map((q) => {
      const final = 'build' in q && typeof q.build === 'function' ? q.build() : (q as FinalQuery<S, any, any, any, any, any>);
      const plan = final.innerQuery?.selectQuery?.plan;
      if (!plan) throw new Error('channel(): pass a query, e.g. db.query(\'message\').where({ conversation })');
      return this.channels.scope(plan, { isOne: final.isOne, ignoreUnused: final.ignoresUnusedArgs });
    });
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
    await this.opened();
    return (await this.writeCreate(id, data)) as T;
  }

  async update<T extends Record<string, unknown>>(table: string, id: string, data: Partial<T>, options?: UpdateOptions): Promise<T> {
    await this.opened();
    return (await this.writeUpdate(table, id, data as Record<string, unknown>, options)) as T;
  }

  async delete(table: string, id: string): Promise<void> {
    await this.opened();
    await this.writeDelete(table, id);
  }

  // Peers go first in all three: the local write costs a transaction they need
  // not wait for. `@ephemeral` fields never reach the database.

  private async writeCreate(id: string, data: Record<string, unknown>, scope?: ChannelHandle): Promise<Record<string, unknown>> {
    const table = id.slice(0, id.indexOf(':'));
    const { persistent, ephemeral } = this.channels.split(table, data);
    this.channels.localCreate(table, id, data, scope);
    const out = await this.runtime.run(write(this.env, { kind: 'create', recordId: id, data: persistent }));
    return { ...(out.record ?? persistent), ...(ephemeral ?? {}) };
  }

  private async writeUpdate(
    table: string,
    id: string,
    data: Record<string, unknown>,
    options?: UpdateOptions,
    scope?: ChannelHandle
  ): Promise<Record<string, unknown>> {
    const tableName = table || id.slice(0, id.indexOf(':'));
    const { persistent, ephemeral } = this.channels.split(tableName, data);
    this.channels.local(tableName, id, persistent, ephemeral, scope);
    if (ephemeral && Object.keys(persistent).length === 0) return data;
    const out = await this.runtime.run(write(this.env, { kind: 'update', recordId: id, data: persistent, options }));
    return { ...(out.record ?? persistent), ...(ephemeral ?? {}) };
  }

  private async writeDelete(table: string, id: string, scope?: ChannelHandle): Promise<void> {
    // Awaited: the filter is checked against the row, which the delete removes.
    await this.channels.localDelete(table || id.slice(0, id.indexOf(':')), id, scope);
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
    this.assertRawRemoteAllowed('useRemote');
    return fn(this.services.remote.getClient());
  }

  async remoteQuery<T extends unknown[]>(sql: string, vars?: Record<string, unknown>): Promise<T> {
    this.assertRawRemoteAllowed('remoteQuery');
    return this.services.remote.query<T>(sql, vars);
  }

  /**
   * Tell the client that the app's own impersonation banner is on screen.
   * Only meaningful with `impersonationBanner: { mode: 'custom' }`, where it
   * cancels the fallback that would otherwise show the built-in banner. Call
   * it while the custom banner is rendered (the Solid `useImpersonation`
   * hook does it for you with `rendersBanner: true`).
   */
  acknowledgeImpersonationBanner(): void {
    this.impersonationBanner.acknowledge();
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

  /** Open a `@crdt` field for editing. `draft` (from {@link createCrdtDraft})
   *  hands over edits made before the record existed. */
  async openCrdtField(table: string, recordId: string, field: string, draft?: CrdtField): Promise<CrdtField> {
    // Opening reads this device's unsent edits from the store: it has to be open.
    await this.opened();
    return this.services.crdt.open(table, recordId, field, draft);
  }

  /** A detached `@crdt` document for a record not created yet; see
   *  {@link openCrdtField}. */
  async createCrdtDraft(table: string, field: string): Promise<CrdtField> {
    return this.services.crdt.createDraft(table, field);
  }

  /** Close a `@crdt` field. The last close resolves once its unsent edits
   *  are queued in the outbox; offline, once they are saved on the device. */
  closeCrdtField(table: string, recordId: string, field: string): Promise<void> {
    return this.services.crdt.close(table, recordId, field);
  }

  bucket<B extends BucketNames<S>>(name: B): BucketHandle {
    return new BucketHandle(name, this.services.remote, this.services.blobs, bucketSettings(this.config, name, this.services.logger), () => this.opened());
  }

  getBlobCacheStats() {
    return this.services.blobs.stats();
  }
}

