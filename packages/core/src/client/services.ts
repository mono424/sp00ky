import { RecordId, type Uuid } from 'surrealdb';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { Sp00kyConfig, PersistenceClient } from '../types';
import type { Logger } from '../services/logger/index';
import { createLogger } from '../services/logger/index';
import { ConnectionSupervisor, LocalMigrator, RemoteDatabaseService, createLocalEngine } from '../services/database/index';
import type { LocalStore } from '../services/database/index';
import { StreamProcessorService } from '../services/stream-processor/index';
import type { StreamUpdate } from '../services/stream-processor/index';
import { extractSelectPermissions } from '../services/stream-processor/permissions';
import { EventSystem } from '../events/index';
import { AuthService } from '../modules/auth/index';
import { CrdtManager } from '../modules/crdt/index';
import { preloadLoro } from '../modules/crdt/loro-loader';
import { LocalStoragePersistenceClient } from '../services/persistence/localstorage';
import { SurrealDBPersistenceClient } from '../services/persistence/surrealdb';
import { ResilientPersistenceClient } from '../services/persistence/resilient';
import { ANON_USER_ID } from '../modules/ref-tables';
import { detectSharedTabsSupport } from '../services/tabs/support';
import { TabsCoordinator, type CoordinatorHooks } from '../services/tabs/coordinator';
import type { LeaderSyncHub, SyncForwarder } from '../services/tabs/coordinator';
import { computeTabsFingerprint, hash53 } from '../services/tabs/protocol';
import type { SqliteCacheEngine } from '../services/database/sqlite-cache-engine';
import type { BlobCache } from '../services/blobs/index';
import { MemoryBlobStore, createBlobCache, resolveBlobBudget } from '../services/blobs/index';
import type { Adapters } from '../kernel/interpreter';
import type { RuntimeEvent } from '../kernel/events';
import type { ServiceCalls } from '../kernel/effects';
import { encodeRecordId } from '../utils/index';
import { mintMutationId } from '../mutation/mutation-id';
import { BucketHandle, bucketContentToBlob } from './bucket-handle';

const LAST_BUCKET_KEY = 'sp00ky:last_bucket';

export function readBootBucketHint(): string | null {
  try {
    return typeof localStorage !== 'undefined' ? localStorage.getItem(LAST_BUCKET_KEY) : null;
  } catch {
    return null;
  }
}

export function writeBootBucketHint(bucketId: string): void {
  try {
    if (typeof localStorage !== 'undefined') localStorage.setItem(LAST_BUCKET_KEY, bucketId);
  } catch {
    /* private-mode storage errors: boot falls back to the anon bucket */
  }
}

export async function sha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

export function mintSalt(): string {
  const c = (globalThis as { crypto?: Crypto }).crypto;
  if (c?.randomUUID) return c.randomUUID();
  return `s${Date.now().toString(36)}${Math.random().toString(36).slice(2, 12)}`;
}

/** Everything the facade talks to besides the runtime. */
export interface Services<S extends SchemaStructure> {
  logger: Logger;
  local: LocalStore;
  remote: RemoteDatabaseService;
  connectionSupervisor: ConnectionSupervisor;
  blobs: BlobCache;
  persistence: PersistenceClient;
  streamProcessor: StreamProcessorService;
  migrator: LocalMigrator;
  crdt: CrdtManager;
  auth: AuthService<S>;
  tabs: TabsCoordinator | null;
  tabsUnsupportedReason: string | undefined;
  tabId: string;
}

/** What the services need back from the runtime once it exists. */
export interface ServiceHost {
  dispatch(event: RuntimeEvent): void;
  /** Called by the tabs coordinator hooks with the hub / forwarder to relay through. */
  setTabsChannel(channel: { hub: LeaderSyncHub | null; forwarder: SyncForwarder | null }): void;
  /** Late-bound modules the boot saga starts (feature flags, app releases). */
  initModules(): void;
  detachWindow(): void;
  setDetachWindow(fn: () => void): void;
}

export function createServices<S extends SchemaStructure>(config: Sp00kyConfig<S>): Services<S> {
  const logger = createLogger(config.logLevel ?? 'info', config.otelTransmit);
  if (config.crdt) void preloadLoro();
  const tabsSupport = detectSharedTabsSupport(config);
  const local = createLocalEngine(config.localEngine, config.database, logger, { shared: tabsSupport.supported });
  const remote = new RemoteDatabaseService(config.database, logger);
  const connectionSupervisor = new ConnectionSupervisor(remote, logger);
  const rawBucket = (name: string) => new BucketHandle(name, remote, null, { blurhash: config.blurhash, logger });
  const blobs = createBlobCache({
    local,
    namespace: ANON_USER_ID,
    logger,
    fetchRemote: async (key) => bucketContentToBlob(await rawBucket(key.bucket).get(key.path)),
    headRemote: (key) => rawBucket(key.bucket).head(key.path),
    maxBytes: config.blobCache?.maxBytes,
    store: config.blobCache?.enabled === false ? new MemoryBlobStore(ANON_USER_ID) : undefined,
  });
  let persistence: PersistenceClient;
  if (config.persistenceClient === 'surrealdb') persistence = new SurrealDBPersistenceClient(local, logger);
  else if (config.persistenceClient === 'localstorage' || !config.persistenceClient) persistence = new LocalStoragePersistenceClient(logger);
  else persistence = config.persistenceClient;
  persistence = new ResilientPersistenceClient(persistence, logger);
  const streamProcessor = new StreamProcessorService(new EventSystem(['stream_update']), local, logger);
  streamProcessor.configureCircuitPersistence(config.persistCircuit ?? config.localEngine === 'sqlite', config.circuitCheckpointMs);
  streamProcessor.configureProjection(config.circuitProjection ?? true);
  const migrator = new LocalMigrator(local, logger);
  const crdt = new CrdtManager(config.schema, local, remote, logger, config.crdtDebounceMs ?? 500);
  const auth = new AuthService(config.schema, remote, persistence, logger);
  const tabId = typeof crypto !== 'undefined' && crypto.randomUUID ? crypto.randomUUID() : `tab_${Math.random().toString(36).slice(2)}`;
  return {
    logger,
    local,
    remote,
    connectionSupervisor,
    blobs,
    persistence,
    streamProcessor,
    migrator,
    crdt,
    auth,
    tabs: null,
    tabsUnsupportedReason: tabsSupport.supported ? undefined : (tabsSupport as { reason: string }).reason,
    tabId,
  };
}

/** The shared-tabs coordinator, wired so role changes and relays become runtime events. */
export function createTabsCoordinator<S extends SchemaStructure>(config: Sp00kyConfig<S>, s: Services<S>, host: ServiceHost): TabsCoordinator {
  const engine = s.local as SqliteCacheEngine;
  const hooks: CoordinatorHooks = {
    adoptOwner: (bucketId, opts) =>
      engine.adoptOwner(bucketId, { workerLockName: opts.workerLockName, allowMemoryFallback: opts.allowMemoryFallback, resumeHeld: opts.resumeHeld }),
    adoptAttached: (dbPort, snapshot) => engine.adoptAttached(dbPort, snapshot, (reason) => engine.onLeaderLost(reason)),
    releaseOwnership: () => engine.releaseOwnership(),
    onLeaderLost: (reason) => engine.onLeaderLost(reason),
    exposeClientPort: (clientId, port) => engine.exposeClientPort(clientId, port),
    removeClientPort: (clientId) => engine.removeClientPort(clientId),
    becomeSyncLeader: (hub) => {
      s.streamProcessor.setPersistenceEnabled(true);
      hub.onFollowerMessage = (_tabId, msg) => host.dispatch({ type: 'TabMessage', message: followerToRuntime(msg) });
      host.setTabsChannel({ hub, forwarder: null });
      host.dispatch({ type: 'TabRole', role: 'leader' });
    },
    resumeSyncLeaderDuties: async () => {
      host.dispatch({ type: 'Drain' });
      host.dispatch({ type: 'LiveStart' });
    },
    becomeSyncFollower: (forwarder) => {
      s.streamProcessor.setPersistenceEnabled(false);
      forwarder.onLeaderMessage = (msg) => host.dispatch({ type: 'TabMessage', message: leaderToRuntime(msg) });
      host.setTabsChannel({ hub: null, forwarder });
      host.dispatch({ type: 'TabRole', role: 'follower' });
    },
    becomeSyncSolo: () => {
      s.streamProcessor.setPersistenceEnabled(true);
      host.setTabsChannel({ hub: null, forwarder: null });
      host.dispatch({ type: 'TabRole', role: 'solo' });
    },
    currentStorageHealth: () => s.local.storageHealth ?? { status: 'unknown', fallback: false },
  };
  return new TabsCoordinator({
    tabId: s.tabId,
    fingerprint: computeTabsFingerprint({
      coreVersion: typeof __SP00KY_CORE_VERSION__ !== 'undefined' ? __SP00KY_CORE_VERSION__ : 'unknown',
      schemaHash: hash53(config.schemaSurql),
      endpoint: config.database.endpoint ?? '',
      namespace: config.database.namespace,
      database: config.database.database,
    }),
    hooks,
    logger: s.logger,
    onLeaderPageHide: () => void engine.shutdownOwnedWorker(),
  });
}

/** Wire-protocol messages become the runtime's tab messages. */
export function followerToRuntime(msg: { type: string; [k: string]: unknown }): unknown {
  switch (msg.type) {
    case 'mutation-enqueued':
      return { type: 'outbox-changed', mutationId: msg.mutationId };
    case 'ingest':
      return { type: 'ingest', records: msg.tuples };
    case 'request-poll':
      return { type: 'membership-dirty', hashes: [] };
    default:
      return msg;
  }
}

export function leaderToRuntime(msg: { type: string; [k: string]: unknown }): unknown {
  switch (msg.type) {
    case 'ingest-relay':
      return { type: 'ingest', records: msg.tuples };
    default:
      return msg;
  }
}

/** The hash a `_00_list_ref` edge belongs to: the id part of its `in` (`_00_query:<hash>`). */
export function hashOfEdge(value: unknown): string | null {
  const inId = (value as { in?: unknown } | null)?.in;
  if (!inId) return null;
  const str = inId instanceof RecordId ? String(inId.id) : String(inId).replace(/^_00_query:/, '');
  return str.length > 0 ? str : null;
}

/** Build the adapters the interpreter drives from the services. */
export function createAdapters<S extends SchemaStructure>(config: Sp00kyConfig<S>, s: Services<S>, host: ServiceHost, lateModules: () => { init(): void }[]): Adapters {
  const timers = new Map<string, ReturnType<typeof setTimeout>>();
  const services: ServiceCalls = {
    'hint.read': readBootBucketHint,
    'hint.write': writeBootBucketHint,
    'local.connect': (bucketId) => s.local.connect(bucketId),
    'local.switchStore': (bucketId) => s.local.switchStore(bucketId),
    'local.beginSwitch': () => s.local.beginSwitch(),
    'local.currentBucketId': () => s.local.currentBucketId,
    'local.usesSurqlSchema': () => s.local.usesSurqlSchema,
    'migrator.provision': () => s.migrator.provision(config.schemaSurql),
    'blobs.start': async (bucketId) => {
      s.blobs.setMaxBytes(await resolveBlobBudget(config.blobCache?.maxBytes));
      await s.blobs.start(bucketId);
    },
    'blobs.setNamespace': (bucketId) => s.blobs.setNamespace(bucketId),
    'blobs.clear': () => s.blobs.clear(),
    'ssp.init': () => s.streamProcessor.init(),
    'ssp.setPermissions': () => s.streamProcessor.setPermissions(extractSelectPermissions(config.schemaSurql)),
    'ssp.setSessionAuth': (authId, access) => s.streamProcessor.setSessionAuth(authId, access),
    'ssp.prime': (pendingIds) =>
      s.streamProcessor.primeFromLocal({
        tables: [...config.schema.tables.map((t) => t.name), '_00_user_feature', '_00_app_release'],
        schemaHash: String(hash53(config.schemaSurql)),
        pendingIds: new Set(pendingIds),
        onVersions: (_table, entries) => host.dispatch({ type: 'VersionsPrimed', entries }),
      }),
    'ssp.reset': () => s.streamProcessor.reset(),
    'ssp.setPersistence': (enabled) => s.streamProcessor.setPersistenceEnabled(enabled),
    'auth.restoreSession': () => s.auth.restoreSessionFromToken(),
    'auth.init': () => s.auth.init(),
    'auth.sessionAuthId': () => {
      const id = s.auth.currentUser?.id;
      if (!id) return null;
      return typeof id === 'string' ? id : encodeRecordId(id);
    },
    'auth.access': () => s.auth.access,
    'auth.token': () => s.auth.token,
    'auth.currentUser': () => (s.auth.currentUser as Record<string, unknown> | null) ?? null,
    'remote.connect': () => s.remote.connect(),
    'remote.releaseViews': (ids) => {
      const list = ids
        .map((id) => String(id))
        .filter((id) => /^_00_query:[0-9a-f]{64}$/.test(id))
        .join(', ');
      if (list) s.remote.beaconSql(`FOR $id IN [${list}] { LET $_released = fn::query::unsubscribe($id); };`);
    },
    'supervisor.start': () => s.connectionSupervisor.start(),
    'tabs.start': (bucketId) => {
      if (!s.tabs) throw new Error('shared tabs unavailable');
      return s.tabs.start(bucketId);
    },
    'tabs.moveToBucket': (bucketId) => {
      if (!s.tabs) throw new Error('shared tabs unavailable');
      return s.tabs.moveToBucket(bucketId);
    },
    'crdt.setSessionId': (id) => s.crdt.setSessionId(id),
    'crdt.closeAll': (flush) => s.crdt.closeAll({ flush }),
    'persistence.set': (key, value) => s.persistence.set(key, value),
    'features.init': () => {
      for (const m of lateModules()) m.init();
    },
    'releases.init': () => undefined,
    'window.attach': () => {
      if (typeof window === 'undefined' || typeof window.addEventListener !== 'function') return;
      const onPageHide = (event: PageTransitionEvent) => {
        if (event.persisted) return;
        host.dispatch({ type: 'PageHide' });
      };
      const onWake = () => {
        if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return;
        host.dispatch({ type: 'HeartbeatNow' });
      };
      window.addEventListener('pagehide', onPageHide);
      window.addEventListener('online', onWake);
      if (typeof document !== 'undefined') document.addEventListener('visibilitychange', onWake);
      host.setDetachWindow(() => {
        window.removeEventListener('pagehide', onPageHide);
        window.removeEventListener('online', onWake);
        if (typeof document !== 'undefined') document.removeEventListener('visibilitychange', onWake);
      });
    },
  };
  return {
    local: s.local,
    remote: {
      queryResponses: (sql, vars) => s.remote.queryResponses(sql, vars),
      live: async (table, onChange) => {
        const [uuid] = await s.remote.query<[Uuid]>(`LIVE SELECT * FROM ${table}`);
        const live = await s.remote.getClient().liveOf(uuid);
        live.subscribe((message) => {
          if (message.action === 'KILLED') return;
          const hash = hashOfEdge(message.value);
          if (hash) onChange([hash]);
        });
        return String(uuid);
      },
      kill: async (uuid) => {
        await s.remote.query('KILL $u', { u: uuid });
      },
    },
    ssp: {
      registerQueryPlan: (plan) => s.streamProcessor.registerQueryPlan(plan) as StreamUpdate | undefined,
      unregisterQueryPlan: (hash) => s.streamProcessor.unregisterQueryPlan(hash),
      ingestMany: (records) => s.streamProcessor.ingestMany(records),
    },
    timers: {
      set: (key, ms, fire) => {
        const prev = timers.get(key);
        if (prev) clearTimeout(prev);
        timers.set(
          key,
          setTimeout(() => {
            timers.delete(key);
            fire();
          }, ms)
        );
      },
      clear: (key) => {
        const prev = timers.get(key);
        if (prev) clearTimeout(prev);
        timers.delete(key);
      },
    },
    clock: { now: () => Date.now() },
    ids: { mutation: () => mintMutationId(s.tabId), salt: mintSalt },
    hash: sha256Hex,
    services,
  };
}
