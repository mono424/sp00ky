import type { RecordId, SchemaStructure, QueryPlan } from '@spooky-sync/query-builder';
import type { Level, LoggerOptions } from 'pino';
import type { PushEventOptions } from './events/index';
import type { LocalEngineChoice } from './services/database/cache-engine';

export type { Level };

/**
 * A pino browser transmit object for forwarding logs to an external sink (e.g. OpenTelemetry).
 */
export type PinoTransmit = NonNullable<NonNullable<LoggerOptions['browser']>['transmit']>;

/**
 * The type of storage backend to use for the local database.
 * - 'memory': In-memory storage (transient).
 * - 'indexeddb': IndexedDB storage (persistent).
 */
export type StoreType = 'memory' | 'indexeddb';

/**
 * Interface for a custom persistence client.
 * Allows providing a custom storage mechanism for the local database.
 */
export interface PersistenceClient {
  /**
   * Sets a value in the storage.
   * @param key The key to set.
   * @param value The value to store.
   */
  set<T>(key: string, value: T): Promise<void>;
  /**
   * Gets a value from the storage.
   * @param key The key to retrieve.
   * @returns The stored value or null if not found.
   */
  get<T>(key: string): Promise<T | null>;
  /**
   * Removes a value from the storage.
   * @param key The key to remove.
   */
  remove(key: string): Promise<void>;
}

/**
 * Supported Time-To-Live (TTL) values for cached queries.
 * Format: number + unit (m=minutes, h=hours, d=days).
 */
export type QueryTimeToLive =
  | '1m'
  | '5m'
  | '10m'
  | '15m'
  | '20m'
  | '25m'
  | '30m'
  | '1h'
  | '2h'
  | '3h'
  | '4h'
  | '5h'
  | '6h'
  | '7h'
  | '8h'
  | '9h'
  | '10h'
  | '11h'
  | '12h'
  | '1d';

/**
 * Refresh behavior for `preload` when the data is already cached locally (warm).
 * The FIRST load (cold) always fetches + blocks regardless.
 * - `onUse` (default): do nothing when warm — the data freshens on use, when the
 *   real `useQuery` mounts and registers its live view. No network on load.
 * - `background`: return instantly, but kick a one-time silent refetch.
 * - `stale`: like `background`, but only if the cached copy is older than
 *   `staleTime`.
 */
export type PreloadRefresh = 'onUse' | 'background' | 'stale';

export interface PreloadOptions {
  /** Abort a cold preload that is still waiting for the server. */
  signal?: AbortSignal;
  /** @deprecated Preload is a registered query now; a warm preload never refetches by itself. Ignored. */
  refresh?: PreloadRefresh;
  /** @deprecated Ignored. */
  staleTime?: QueryTimeToLive;
}

/**
 * Result object returned when a query is registered or executed.
 */
export interface Sp00kyQueryResult {
  /** The unique hash identifier for the query. */
  hash: string;
}

export type Sp00kyQueryResultPromise = Promise<Sp00kyQueryResult>;

export interface EventSubscriptionOptions {
  priority?: number;
}

/**
 * Configuration options for the Sp00ky client.
 * @template S The schema structure type.
 */
export interface Sp00kyConfig<S extends SchemaStructure> {
  /** Database connection configuration. */
  database: {
    /** The SurrealDB endpoint URL. */
    endpoint?: string;
    /** The namespace to use. */
    namespace: string;
    /** The database name. */
    database: string;
    /** The local store type implementation. */
    store?: StoreType;
    /** Authentication token. */
    token?: string;
    /**
     * SQLite engine only: execute `select` plans (base rows + relation tree +
     * row parsing) inside the worker as ONE round-trip instead of one hop per
     * table/relation level. Defaults to true; set false to force the legacy
     * multi-hop path (escape hatch while the worker-side path beds in).
     */
    workerSelect?: boolean;
    /**
     * WebSocket reconnect + liveness tuning. All fields optional; the defaults
     * keep the connection alive indefinitely without configuration. See
     * {@link ReconnectConfig}.
     */
    reconnect?: ReconnectConfig;
    /**
     * Deadline (ms) for every remote RPC. Remote queries are serialized through
     * a single promise chain, so one call that never settles (half-open socket:
     * the WebSocket looks open, the peer is gone, no `close` event fires) would
     * otherwise wedge ALL later remote traffic behind it — including the sync
     * poll's own health probe, leaving health pinned at `healthy` with no
     * banner and no self-heal. The deadline turns that into an ordinary network
     * failure the queue retries. `0` disables. Defaults to `60_000`.
     */
    queryTimeoutMs?: number;
    /**
     * Deadline (ms) for every LOCAL store operation (a SQLite worker round trip,
     * or a query on the in-process surrealdb engine). Local ops are serialized
     * too, and one that never answered - a worker starved behind a long select,
     * a lock verification with no clock - left every `db.create`/`db.update`
     * promise pending for the tab's lifetime. On expiry the call rejects with
     * `LocalOpTimeoutError` (the op itself keeps running in the engine and is
     * not retried). `0` disables. Defaults to `30_000`.
     */
    localOpTimeoutMs?: number;
  };
  /** The schema definition. */
  schema: S;
  /** The compiled SURQL schema string. */
  schemaSurql: string;
  /** Logging level. */
  logLevel: Level;
  /**
   * Persistence client to use.
   * Can be a custom implementation, 'surrealdb' (default), or 'localstorage'.
   */
  persistenceClient?: PersistenceClient | 'surrealdb' | 'localstorage';
  /**
   * Local cache engine backend. `'surrealdb'` (default) uses the in-browser
   * SurrealDB-WASM store; `'sqlite'` uses official SQLite-WASM in a Worker with
   * OPFS persistence; or pass a custom {@link LocalCacheEngine}. The local cache
   * is a passive queryable store — reactivity is driven by the remote SSP, not
   * this engine. See `services/database/cache-engine.ts`.
   */
  localEngine?: LocalEngineChoice;
  /**
   * Durable cache for bucket file bytes, in OPFS. Enabled by default wherever
   * OPFS is writable; elsewhere the cache degrades to per-tab memory, which is
   * how bucket reads behaved before it existed.
   *
   * Nothing in this cache expires on a timer — an image whose row is still in
   * the local store has to stay available offline. Bytes are only dropped when
   * the app invalidates the path (`bucket.put`/`bucket.delete`), when boot
   * reconcile finds no file behind a row, or when the cache is over budget, in
   * which case the least-recently-used unpinned entries go first. See
   * `services/blobs/blob-cache.ts`.
   */
  blobCache?: {
    /** Default `true`. `false` restores per-tab, non-persistent caching. */
    enabled?: boolean;
    /** Byte budget. Defaults to `min(512 MB, quota × 0.25)` from
     *  `navigator.storage.estimate()`. */
    maxBytes?: number;
    /**
     * Delete the signed-out user's cached bytes on `signOut()`. Default
     * `false`, matching the local store: cached files are namespaced per local
     * bucket, so signing back in is warm and no user can read another's cache.
     * Turn on for shared devices.
     */
    clearOnSignOut?: boolean;
  };
  /**
   * Share ONE durable local store across all tabs of this origin (default
   * `false`). Requires `localEngine: 'sqlite'`. A SharedWorker broker elects a
   * leader tab per bucket via Web Locks; the leader owns the OPFS SQLite
   * worker and the sync loop, and follower tabs read/write the same store over
   * MessagePorts, so every tab is durable instead of only the first one.
   *
   * Falls back to solo mode (exactly the flag-off behavior, including the
   * later-tabs in-memory fallback reported via {@link StorageHealth}) whenever
   * SharedWorker, Web Locks, or MessageChannel are unavailable, the engine is
   * not sqlite, or the broker rejects the tab (mixed app versions).
   *
   * Failover: when the leader tab closes or freezes, a follower is promoted
   * within seconds; queries briefly refetch and mutations are never lost once
   * their local write resolved (the shared outbox survives in the store).
   * Inspect via `window.__00__.getState().database.tabs` and `__sqliteStats`.
   */
  sharedTabs?: boolean;
  /**
   * Persist the in-browser SSP circuit's store as a snapshot in the local
   * store, so a reload restores it and only steps in what changed since.
   * Defaults to `true` under `localEngine: 'sqlite'` (the OPFS-backed store
   * can hold the bytes) and `false` otherwise.
   *
   * Without a snapshot the circuit is primed by reading every cached row back
   * out of the local store; either way a reload never re-downloads the working
   * set, which is what an empty circuit used to force: every id in the
   * server's list_ref classified as missing.
   *
   * Snapshots are store-only (views are re-registered per session) and written
   * on a checkpoint interval ({@link circuitCheckpointMs}) and when the page
   * goes hidden, never per ingest or per query registration.
   */
  persistCircuit?: boolean;
  /**
   * Checkpoint interval in milliseconds for {@link persistCircuit}. Defaults to
   * 30000. Ignored when `persistCircuit` is off.
   */
  circuitCheckpointMs?: number;
  /**
   * Keep only the fields registered queries evaluate (filter predicates, join
   * keys, sort keys, plus `id`/`_00_rv`) per row in the in-browser circuit.
   * Default `true`. The circuit only ever reads those fields; bodies are
   * rendered from the local store. Measured on 7700 rows with 20 KB bodies:
   * the wasm heap peak went from ~500 MB to ~24 MB and the snapshot from
   * 156 MB to 1.2 MB. A query that evaluates a field earlier rows were kept
   * without has that field merged in from the local store on registration.
   */
  circuitProjection?: boolean;
  /** A pino browser transmit object for forwarding logs (e.g. via @spooky-sync/core/otel). */
  otelTransmit?: PinoTransmit;
  /**
   * Debounce time in milliseconds for stream updates (the client-side SSP
   * aggregation throttle — coalesces the in-browser StreamProcessor's
   * per-record updates per query before notifying readers).
   * Defaults to 50ms.
   */
  streamDebounceTime?: number;
  /**
   * Debounce time in milliseconds for syncing collaborative (CRDT) field
   * changes to the remote database. Local writes happen immediately on
   * every keystroke (so reload/offline works), but the remote UPSERT is
   * coalesced over this window. Lower = snappier remote propagation +
   * more network traffic; higher = less traffic + more lag for other
   * collaborators. Defaults to 500ms.
   */
  crdtDebounceMs?: number;
  /**
   * Enable collaborative CRDT fields. When `true`, the `loro-crdt` engine is
   * preloaded at client startup (fetched as a separate chunk on page load) so
   * the first `openCrdtField` is instant. When omitted/`false`, loro is never
   * loaded unless a CRDT field is explicitly opened — keeping the loro chunk
   * out of apps that don't use collaboration. Defaults to `false`.
   */
  crdt?: boolean;
  /**
   * Cadence (ms) for the `_00_list_ref` poll that catches cross-session
   * UPDATEs the SurrealDB v3 LIVE-permission gap drops. Lower = faster
   * convergence + more query load; higher = the inverse. Non-positive
   * values fall back to the default (500ms).
   */
  refSyncIntervalMs?: number;
  /**
   * OPT-IN instant-hydrate for cold queries: when enabled and a query is
   * registered with no server result yet, its surql also runs directly on the
   * remote (one-shot, in the background, OFF the paint path) so rows can land
   * before the full register lifecycle completes. Hydrated rows carry their
   * `_00_rv` versions so the registration's `syncRecords` skips re-pulling
   * unchanged bodies. Regardless of this flag, `useQuery` always resolves and
   * paints from the local cache immediately — however the rows got there
   * (preload, prior sync). Default `false`: the register lifecycle
   * (`fn::query::register` → `_00_list_ref` → record sync) is the single
   * freshness path and no duplicate one-shot fetches are made.
   */
  instantHydrate?: boolean;
  /**
   * Enable realtime sync while signed out. When `true`, the client starts its
   * `_00_list_ref` poll (and a LIVE subscription) against the shared
   * `_00_list_ref_anon` table even with no authenticated user, so a logged-out
   * page gets live `useQuery` updates over world-readable tables. Requires the
   * server to be deployed with `anonymousLiveQueries: true` in `sp00ky.yml`
   * (this flag must match it). Defaults to `false`: anonymous clients can read
   * one-shot but never sync live.
   */
  enableAnonymousLiveQueries?: boolean;
  /**
   * Surface sustained sync failures as a "degraded" health status that the app
   * can observe via `subscribeToSyncHealth` (or the client-solid
   * `useSyncStatus` hook) to render a "can't reach the server" banner.
   *
   * Individual failures — a transient remote 500 on query registration, a
   * dropped WebSocket, etc. — are always swallowed and retried; they never
   * throw at the app. This only controls when a *run* of consecutive failures
   * is reported. Status flips back to `healthy` on the next successful sync
   * round. Defaults to `{ degradeAfterConsecutiveFailures: 3 }`; pass `false`
   * (or `degradeAfterConsecutiveFailures: 0`) to never report degraded.
   */
  syncHealth?: SyncHealthConfig | false;
  /**
   * Automatic blurhash placeholders for bucket image uploads. On every
   * `bucket.put` of an image path (by extension: webp/png/jpg/jpeg/gif/avif/bmp)
   * the client computes a blurhash and stores it as a tiny sidecar object
   * `<path>.bh` in the same bucket, best-effort. Read it back with
   * `bucket.blurhash(path)` (or the client-solid `useBucketImage`/`BucketImage`
   * helpers) to paint a placeholder until the image is decoded.
   *
   * `true` (the default) enables with 4x3 components; pass
   * `{ componentX, componentY }` to tune detail, or `false` to disable.
   * A per-call `put(path, content, { blurhash })` option overrides this.
   */
  blurhash?: boolean | { componentX?: number; componentY?: number };
  /**
   * Deadline (ms) for a single outgoing mutation push. Tighter than
   * {@link Sp00kyConfig.database.queryTimeoutMs} because the up-queue drains
   * one mutation at a time behind an `isSyncingUp` flag: a push that never
   * settles stops every later mutation for the session, with no retry and no
   * error. On expiry the push is treated as a network failure and re-queued.
   * `0` disables. Defaults to `30_000`.
   */
  pushTimeoutMs?: number;
  /**
   * Max time a single down event (`register`/`sync`/`cleanup`) may take before
   * it is retried. Mirror of {@link pushTimeoutMs} for the read side.
   * Defaults to 30000; `0` disables the timeout.
   */
  downTimeoutMs?: number;
}

/** Tunables for sync-health reporting. See {@link Sp00kyConfig.syncHealth}. */
export interface SyncHealthConfig {
  /**
   * Number of consecutive failed sync rounds (up or down) before the status
   * flips from `healthy` to `degraded`. A single transient failure is absorbed
   * by the retry; only a sustained run trips the banner. Defaults to `3`. `0`
   * disables degraded reporting entirely.
   */
  degradeAfterConsecutiveFailures?: number;
}

/**
 * Tunables for WebSocket reconnect and liveness detection. See
 * {@link Sp00kyConfig.database.reconnect}.
 *
 * Two independent mechanisms cooperate here. The SurrealDB SDK reconnects on
 * its own after a socket `close` (`attempts` / `retryDelayMax`), and a
 * supervisor above it re-opens the connection from scratch whenever the SDK
 * gives up or its post-reconnect handshake fails — the SDK terminates the
 * engine permanently in that case, so a supervisor is required, not optional.
 * The heartbeat covers the third case: a socket that never closes at all.
 */
export interface ReconnectConfig {
  /**
   * SDK reconnect attempts after a socket close. `-1` retries forever.
   * Defaults to `-1` (the SDK's own default is `5`, which caps recovery at a
   * ~62s outage and then gives up for the life of the page).
   */
  attempts?: number;
  /** Cap on the SDK's exponential backoff delay. Defaults to `15_000`. */
  retryDelayMax?: number;
  /**
   * Cadence of the application-level liveness probe (`RETURN true`) that
   * detects a half-open socket the transport never reports as closed.
   * `0` disables the heartbeat. Defaults to `20_000`.
   */
  heartbeatIntervalMs?: number;
  /**
   * Deadline for a heartbeat response. Exceeding it means the socket is dead
   * regardless of what its `readyState` claims, so the connection is torn down
   * and rebuilt. Defaults to `10_000`.
   */
  heartbeatTimeoutMs?: number;
  /**
   * Cap on the supervisor's own backoff between `connect()` retries once the
   * SDK has given up. Defaults to `15_000`.
   */
  superviseRetryDelayMaxMs?: number;
}

/**
 * Transport-level connection state, independent of {@link SyncHealthStatus}.
 *
 * These answer different questions: `connection` is about the socket,
 * `status` is about whether sync rounds are succeeding. A `connected` socket
 * can still be `degraded` (server erroring), and a `reconnecting` socket is
 * usually still `healthy` for the first few seconds.
 */
export type ConnectionState = 'connecting' | 'connected' | 'reconnecting' | 'disconnected';

export type SyncHealthStatus = 'healthy' | 'degraded';

/** Snapshot of sync health delivered to `subscribeToSyncHealth` subscribers. */
export interface SyncHealth {
  /** `'degraded'` once consecutive failures cross the configured threshold. */
  status: SyncHealthStatus;
  /** Consecutive failed sync rounds at the moment of this report. */
  consecutiveFailures: number;
  /** Classification of the most recent failure (only set while `degraded`). */
  kind?: 'network' | 'application';
  /** Message of the most recent failure (only set while `degraded`). */
  error?: string;
  /**
   * `true` once at least one sync round has succeeded this session. Lets a UI
   * distinguish a first-time "connecting" phase (never reached the server yet,
   * so a cold-start failure run is expected) from a real lost connection after
   * a working session. Never resets back to `false` once set.
   */
  everConnected: boolean;
  /**
   * Live transport state of the remote WebSocket. Distinct from `status`: this
   * one flips the instant the socket drops, whereas `status` only degrades
   * after a sustained run of failed sync rounds. Use it to show "reconnecting…"
   * immediately without waiting for the degrade threshold.
   */
  connection: ConnectionState;
}

export type StorageHealthStatus = 'unknown' | 'persistent' | 'memory';

/**
 * Durability of the LOCAL cache, delivered to `subscribeToStorageHealth`
 * subscribers. Separate from {@link SyncHealth}: that one is about reaching the
 * server, this one is about whether the local store survives a reload.
 *
 * Under `localEngine: 'sqlite'` the durable store is the OPFS SAHPool VFS,
 * which only one client per bucket can hold open. When it can't be opened (a
 * second tab of the app already has it, an insecure context, a full pool) the
 * engine keeps working against an in-memory DB, which holds the whole dataset
 * in RAM and loses local writes on reload. `fallback` marks exactly that case,
 * so a UI can warn about it.
 */
export interface StorageHealth {
  /** `'unknown'` until the local cache has opened, or for engines that don't report. */
  status: StorageHealthStatus;
  /**
   * `true` only when durable storage was REQUESTED and could not be opened.
   * Stays `false` for a configured-in-memory store (`store: 'memory'`), which
   * is a choice rather than a failure, so a UI can key off this alone.
   */
  fallback: boolean;
  /** Reason durable storage failed (only set while `fallback` is `true`). */
  error?: string;
  /**
   * Shared-tabs role, set only when `sharedTabs` is active: `'leader'` owns
   * the OPFS worker, `'follower'` shares it over a MessagePort (its data IS
   * durable, hence `status: 'persistent'`), `'solo'` fell back to the
   * single-tab behavior. Absent entirely when the feature is off.
   */
  role?: 'leader' | 'follower' | 'solo';
}

export type QueryHash = string;

// Flat array format: [[record-id, version], [record-id, version], ...]
export type RecordVersionArray = Array<[string, number]>;

/**
 * Represents the difference between two record version sets.
 * Used for synchronizing local and remote states.
 */
export interface RecordVersionDiff {
  /** List of records added. */
  added: Array<{ id: RecordId<string>; version: number }>;
  /** List of records updated. */
  updated: Array<{ id: RecordId<string>; version: number }>;
  /** List of record IDs removed. */
  removed: RecordId<string>[];
}

/**
 * Configuration for a specific query instance.
 * Stores metadata about the query's state, parameters, and versioning.
 */
export interface QueryConfig {
  /** The unique ID of the query config record. */
  id: RecordId<string>;
  /** The SURQL query string. */
  surql: string;
  /**
   * Engine-neutral plan for `surql` (in-memory only; not persisted to
   * `_00_query`). Present when the query came from the query-builder. Non-
   * SurrealQL local engines (SQLite) materialize via `engine.select(plan)`
   * instead of re-running `surql`, which they cannot parse.
   */
  plan?: QueryPlan;
  /** Parameters used in the query. */
  params: Record<string, any>;
  /** The version array representing the local state of results. */
  localArray: RecordVersionArray;
  /** The version array representing the remote (server) state of results. */
  remoteArray: RecordVersionArray;
  /**
   * In-memory only (never persisted to `_00_query`): version array of the
   * subquery CHILD rows pulled via `parent IS NOT NONE` edges, so the
   * child-body sync is idempotent across polls. Kept separate from
   * `remoteArray` so related child rows never enter the primary window /
   * `rowCount` / `localArray`.
   */
  subqueryRemoteArray?: RecordVersionArray;
  /**
   * Whether authoritative membership (`remoteArray`) has ever been established
   * for this query — either fetched from `_00_list_ref` this session, or read
   * back from the durable `_00_view` row on a cold start.
   *
   * Tri-state matters: "known and empty" must render an empty list, while
   * "never established" has to fall back to a predicate scan of the local store
   * so a query first run on this device still paints offline. A
   * `remoteArray.length === 0` check cannot tell those apart.
   *
   * On a cold start it is seeded from the durable `_00_view` row when that
   * row is non-empty, or empty but `confirmed` (the server reported zero rows
   * for the query). An unconfirmed empty row is ignored, so a device poisoned
   * by an old client that mirrored unflushed reads still self-heals.
   */
  membershipKnown?: boolean;
  /**
   * Key of this query's durable `_00_view` membership row: a hash of
   * `{surql, params}` WITHOUT the `session::id()` salt that `id` carries, so it
   * survives a reload (which mints a new session id) and a bucket switch.
   * In-memory only.
   */
  membershipKey?: string;
  /** Time-To-Live for this query. */
  ttl: QueryTimeToLive;
  /** Timestamp when the query was last accessed/active. */
  lastActiveAt: Date;
  /** The name of the table this query targets (if applicable). */
  tableName: string;
}

export type QueryConfigRecord = QueryConfig & { id: string };

/**
 * Runtime fetch status of a live query.
 * - `idle`: registered, initial sync completed, and not currently fetching
 *   missing records — the materialized rows are authoritative (a windowed
 *   query's short result really is the end of the list).
 * - `fetching`: the query is registering (a query is born `fetching` until its
 *   initial remote sync completes) or the sync engine is fetching/ingesting
 *   missing records for it. Any pending debounced result is flushed BEFORE the
 *   flip back to `idle`, so idle status never races ahead of the rows.
 */
export type QueryStatus = 'idle' | 'fetching';

/**
 * What the server's `_00_query` row says about a view, read alongside its
 * `_00_list_ref` edges (registration, the poll, a targeted re-read).
 *
 * - `present: false`: the row is not there (or not readable). The SSP no
 *   longer has this view: a TTL sweep, an SSP reset, a scheduler wipe. An
 *   empty edge set read at the same time is NOT "no rows", it is "no view".
 * - `state: 'materializing'`: the SSP has (re)computed the set and its edges
 *   are in flight; an empty read is "not landed yet".
 * - `state: 'ready'`: the edges are committed (flipped in the same
 *   transaction), so `rowCount === 0` with no edges is a real empty result.
 * - `state: null`: a server that predates the marker; only `rowCount` can be
 *   used, with its `> 0` = flush window / `0` = empty ambiguity.
 */
export interface ServerViewMeta {
  present: boolean;
  /** `_00_query.rowCount`; `null` when unreadable. */
  rowCount: number | null;
  state: 'materializing' | 'ready' | null;
}

/** Outcome of handing a server id-set to `DataModule.updateQueryRemoteArray`. */
export type MembershipOutcome = 'applied' | 'ignored' | 'view-lost';

/**
 * Internal state of a live query.
 */
export interface QueryState {
  /** The configuration for this query. */
  config: QueryConfig;
  /** The current cached records for this query. */
  records: Record<string, any>[];
  /** Set once `applyHydration` has run for this query, so the cold instant-hydrate
   * path fires at most once per query (see DataModule.isCold/applyHydration). */
  hydrated?: boolean;
  /** Set once a server-side membership set has been accepted this session
   * (`updateQueryRemoteArray`). The `register` down-event is enqueued before
   * the instant-hydrate, so a hydrate that finishes later must not prime
   * membership over the authoritative set. Ephemeral. */
  serverMembership?: boolean;
  /**
   * Set when the server's `_00_query` row for this query went missing while
   * we held membership (TTL sweep, SSP reset, scheduler wipe). The view is
   * being re-registered; until a readable row comes back, empty reads are
   * NOT the server's answer and nothing is dropped. Ephemeral.
   */
  viewLost?: boolean;
  /** Last `_00_query.state` read from the server, for DevTools. Ephemeral. */
  serverState?: 'materializing' | 'ready' | null;
  /** Set once `notifyQuerySynced` has emitted for this registration lifetime.
   * Ephemeral (unlike the persisted `updateCount`), so a re-registered query
   * always emits at least once even when its records are unchanged — otherwise
   * an empty re-registered window would never notify and stay "loading". */
  syncNotified?: boolean;
  /** Timer for TTL expiration. */
  ttlTimer: NodeJS.Timeout | null;
  /** TTL duration in milliseconds. */
  ttlDurationMs: number;
  /** When the last TTL heartbeat for this query was sent (ms epoch); `undefined` before the first. */
  lastHeartbeatAt?: number;
  /** Number of times the query has been updated. */
  updateCount: number;
  /** Timestamp (ms) of the last user-visible update, or null before the first
   * one. Surfaced to DevTools as `lastUpdate` — must NOT be stamped on read. */
  lastUpdatedAt: number | null;
  /**
   * Rolling window of the most recent materialization-step latencies (ms).
   * Capped at MATERIALIZATION_SAMPLE_WINDOW; used to recompute p55/p90/p99
   * before each persist to `_00_query`. Samples themselves are not persisted.
   */
  materializationSamples: number[];
  /** Most recent end-to-end ingest latency in ms, or null until the first ingest. */
  lastIngestLatencyMs: number | null;
  /** Cumulative count of ingest/materialization errors observed for this query. */
  errorCount: number;
  /**
   * Ephemeral runtime fetch status. Not persisted to `_00_query`; observable
   * via DevTools and the `useQuery` hook. `fetching` while the sync engine is
   * pulling missing records for this query, otherwise `idle`.
   */
  status: QueryStatus;
  /**
   * Rolling per-phase timing samples (ms), in addition to `materializationSamples`
   * (which holds the SSP whole-ingest wall time). Keyed by `TimingPhase` minus
   * `ssp`. Not persisted — surfaced live to DevTools + MCP via `phaseTimings`.
   */
  phaseSamples: Record<string, number[]>;
  /** Most recent sample (ms) per phase, or null. */
  phaseLast: Record<string, number | null>;
  /** One-shot SSP registration timings (ms). */
  registrationTimings: RegistrationTimings;
}

/** Cap on the rolling materialization-sample window kept per query in memory. */
export const MATERIALIZATION_SAMPLE_WINDOW = 100;

/** Timed processing phases surfaced per query. `ssp` is the WASM-ingest wall
 *  time; the `ssp*` phases are its internal breakdown from the SSP binding. */
export type TimingPhase =
  | 'ssp'
  | 'sspStoreApply'
  | 'sspCircuitStep'
  | 'sspTransform'
  | 'localFetch'
  | 'remoteFetch'
  | 'frontend';

/** One-shot registration timings (ms), captured once when a query registers. */
export interface RegistrationTimings {
  /** SSP surql→plan parse + permission injection. */
  parseMs: number | null;
  /** SSP operator-DAG build. */
  planMs: number | null;
  /** SSP initial snapshot evaluation. */
  snapshotMs: number | null;
  /** Wall time of `cache.registerQuery` (register_view round-trip). */
  wallMs: number | null;
}

/** Percentile summary for one timed phase, surfaced to DevTools + MCP. */
export interface PhaseStat {
  lastMs: number | null;
  p50: number | null;
  p90: number | null;
  p99: number | null;
  count: number;
}

/** Per-query processing-time breakdown surfaced via DevTools panel + MCP. */
export interface QueryTimings {
  ssp: PhaseStat;
  sspStoreApply: PhaseStat;
  sspCircuitStep: PhaseStat;
  sspTransform: PhaseStat;
  localFetch: PhaseStat;
  remoteFetch: PhaseStat;
  frontend: PhaseStat;
  registration: RegistrationTimings;
  updateCount: number;
  errorCount: number;
}

// Callback types
export type QueryUpdateCallback = (records: Record<string, any>[]) => void;
export type QueryStatusCallback = (status: QueryStatus) => void;
/**
 * Observer for a query's authority flips: `true` once server membership is
 * known for it (registration, poll, durable seed), `false` when it is reset
 * (bucket switch). What the bindings' `isAuthoritative()` mirrors.
 */
export type QueryAuthorityCallback = (known: boolean) => void;
export type MutationCallback = (mutations: MutationEvent[]) => void;

export type MutationEventType = 'create' | 'update' | 'delete';

// Mutation event for sync
/**
 * Represents a mutation event (create, update, delete) to be synchronized.
 */
export interface MutationEvent {
  /** Example: 'create', 'update', or 'delete'. */
  type: MutationEventType;
  /** unique id of the mutation */
  mutation_id: RecordId<string>;
  /** The ID of the record being mutated. */
  record_id: RecordId<string>;
  /** The data payload for create/update operations. */
  data?: any;
  /** The full record data (optional context). */
  record?: any;
  /** Options for the mutation event (e.g., debounce settings). */
  options?: PushEventOptions;
  /** Timestamp when the event was created. */
  createdAt: Date;
}

/**
 * Options for run operations.
 */
export interface RunOptions {
  assignedTo?: string;
  max_retries?: number;
  retry_strategy?: 'linear' | 'exponential';
  /** Timeout in seconds for the backend HTTP call. Only used if the backend allows timeout override. */
  timeout?: number;
  /**
   * Minimum delay in milliseconds before the job is eligible to run. While
   * delayed the job stays pending (enqueued) and can still be killed.
   */
  delay?: number;
}

/**
 * Options for update operations.
 */
export interface UpdateOptions {
  /**
   * Debounce configuration for the update.
   * If boolean, enables default debounce behavior.
   */
  debounced?: boolean | DebounceOptions;
}

/**
 * Configuration options for debouncing updates.
 */
export interface DebounceOptions {
  /**
   * The key to use for debouncing.
   * - 'recordId': Debounce based on the specific record ID. WARNING: IT WILL ONLY ACCEPT THE LATEST CHANGE AND DOES *NOT* MERGE THE PREVIOUS ONCES. IF YOU ARE UNSURE JUST USE 'recordId_x_fields'.
   * - 'recordId_x_fields': Debounce based on record ID and specific fields.
   */
  key?: 'recordId' | 'recordId_x_fields';
  /** The debounce delay in milliseconds. */
  delay?: number;
}
