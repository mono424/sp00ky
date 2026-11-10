import type { SyncedDbConfig } from './types.js';
import {
  Sp00kyClient,
  type Sp00kyQueryResultPromise,
  type AuthService,
  type BucketHandle,
  type UpdateOptions,
  type RunOptions,
  type SyncHealth,
  type StorageHealth,
  type PreloadOptions,
  type WebPushModule,
  type Sp00kyChannel,
  type ChannelQuery,
} from '@spooky-sync/core';
import type {
  ChannelPeer,
  ChannelTableNames,
  GetTable,
  QueryBuilder,
  SchemaStructure,
  TableModel,
  TableNames,
  FinalQuery,
  InnerQuery,
  BackendNames,
  BackendRoutes,
  RoutePayload,
  BucketNames,
  BucketDefinitionSchema,
} from '@spooky-sync/query-builder';
import { RecordId, type Surreal } from 'surrealdb';

/**
 * Deep copy of the plain objects and arrays in `value`, everything else
 * (RecordId, Date, Uint8Array, any class instance) passed through as is.
 *
 * A Svelte `$state` object or array is a Proxy, and an app very often hands
 * one straight to a write (`db.create(id, draft)` with `let draft = $state({})`).
 * A Proxy cannot cross `postMessage` to the sqlite and shared-tabs workers:
 * structuredClone throws DataCloneError. `$state.snapshot` is the official
 * unwrap, but it turns anything with a `toJSON` into its JSON form, which
 * would write a RecordId link as a string. Svelte only proxies plain objects
 * and arrays, so copying exactly those (read through the proxy) is enough.
 */
export function plain<T>(value: T): T {
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(plain) as T;
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return value;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(value)) out[key] = plain((value as Record<string, unknown>)[key]);
  return out as T;
}

/**
 * SyncedDb - a thin wrapper around the sp00ky core client for Svelte.
 * Delegates all logic to the underlying `Sp00kyClient` instance.
 *
 * NOTE: keep in sync with packages/client-react/src/db.ts and
 * packages/client-solid2/src/index.ts (SyncedDb). Copied rather than shared so
 * each binding's dependency graph stays its own. Svelte-only difference: write
 * payloads and query vars go through `plain()`, because a `$state` object is a
 * Proxy that cannot reach the workers.
 */
export class SyncedDb<S extends SchemaStructure> {
  private config: SyncedDbConfig<S>;
  private sp00ky: Sp00kyClient<S> | null = null;
  private _initialized = false;

  constructor(config: SyncedDbConfig<S>) {
    this.config = config;
  }

  public getSp00ky(): Sp00kyClient<S> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky;
  }

  /** Initialize the core client. */
  async init(): Promise<void> {
    if (this._initialized) return;
    this.sp00ky = new Sp00kyClient<S>(this.config);
    await this.sp00ky.init();
    this._initialized = true;
  }

  /**
   * Tear down the client: leaves the tabs broker, closes the local store and
   * remote socket, and frees the wasm circuit.
   */
  async close(): Promise<void> {
    const instance = this.sp00ky;
    this.sp00ky = null;
    this._initialized = false;
    if (instance) await instance.close();
  }

  /** Create a new record in the database. */
  async create(id: string, payload: Record<string, unknown>): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.create(id, plain(payload));
  }

  /** Update an existing record in the database. */
  async update<TName extends TableNames<S>>(
    tableName: TName,
    recordId: string,
    payload: Partial<TableModel<GetTable<S, TName>>>,
    options?: UpdateOptions
  ): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.update(
      tableName as string,
      recordId,
      plain(payload) as Record<string, unknown>,
      options
    );
  }

  /** Delete an existing record in the database. */
  async delete<TName extends TableNames<S>>(
    tableName: TName,
    selector: string | RecordId | InnerQuery<GetTable<S, TName>, boolean>
  ): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    // Accept a `"table:id"` string OR a RecordId: live-query rows carry their
    // `id` as a RecordId, so callers can pass `db.delete('game', row.id)`
    // directly. Build the canonical string from the raw id part (not
    // `RecordId.toString()`, which escapes special chars) so it round-trips
    // through the engine's `parseRecordIdString`. InnerQuery selectors are not
    // supported yet. (Cross-package RecordId instances match by constructor name.)
    const isRecordId =
      selector instanceof RecordId || (selector as any)?.constructor?.name === 'RecordId';
    let id: string;
    if (typeof selector === 'string') {
      id = selector;
    } else if (isRecordId) {
      id = `${tableName as string}:${(selector as RecordId).id}`;
    } else {
      throw new Error('Only string ID or RecordId selectors are supported currently with core');
    }
    await this.sp00ky.delete(tableName as string, id);
  }

  /**
   * Preload/prewarm a built query into the local cache without a component
   * subscribing to it, so a later `useQuery` for the same data paints
   * instantly. Best-effort.
   */
  public async preload(
    finalQuery: FinalQuery<S, any, any, any, any, Sp00kyQueryResultPromise>,
    options?: PreloadOptions
  ): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.preload(finalQuery, options);
  }

  /** Start a query on `table`. Finish it with `.build()` and pass it to `useQuery`. */
  public query<TName extends TableNames<S>>(
    table: TName
  ): QueryBuilder<S, TName, Sp00kyQueryResultPromise, {}, false> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.query(table, {});
  }

  /**
   * Open a peer to peer channel to `peers` for the rows `query` matches (a
   * query on a `-- @channel` table; only its `where` counts). Prefer the
   * `useChannel` hook, which closes it with the component.
   */
  public channel<T extends ChannelTableNames<S>>(
    query: ChannelQuery<S, T> | ReadonlyArray<ChannelQuery<S, T>>,
    peers: Iterable<ChannelPeer>
  ): Sp00kyChannel<S, T> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.channel(query, peers);
  }

  /** Run a backend operation (writes an outbox job row). */
  public async run<B extends BackendNames<S>, R extends BackendRoutes<S, B>>(
    backend: B,
    path: R,
    payload: RoutePayload<S, B, R>,
    options?: RunOptions
  ): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.run(backend, path, plain(payload), options);
  }

  /** Authenticate the remote connection with a token. */
  public async authenticate(token: string): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.authenticate(token);
  }

  /**
   * Tell the client that this app's own impersonation banner is on screen.
   * Only needed with `impersonationBanner: { mode: 'custom' }`; the
   * `useImpersonation({ rendersBanner: true })` hook calls it for you.
   */
  public acknowledgeImpersonationBanner(): void {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    this.sp00ky.acknowledgeImpersonationBanner();
  }

  /** Sign out, clear session and local storage. */
  public async signOut(): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.auth.signOut();
  }

  /** Execute a function with direct access to the remote database connection. */
  public async useRemote<T>(fn: (db: Surreal) => T | Promise<T>): Promise<T> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return await this.sp00ky.useRemote(fn);
  }

  /**
   * Run one SurrealQL statement against the REMOTE database, results shaped
   * like the SDK's own `.query()` (one entry per statement). Nothing is cached
   * locally. Prefer this over `useRemote(s => s.query(...))` for a plain read:
   * the bare SDK client that callback receives skips the connect gate, so a
   * statement issued while the socket is being rebuilt fails with "Specify a
   * namespace to use" instead of waiting for the session.
   */
  public async remoteQuery<T extends unknown[] = unknown[]>(
    sql: string,
    vars?: Record<string, unknown>
  ): Promise<T> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return await this.sp00ky.remoteQuery<T>(sql, vars ? plain(vars) : undefined);
  }

  /** Access the remote database service directly. */
  get remote(): Sp00kyClient<S>['remoteClient'] {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.remoteClient;
  }

  /** Access the local database service directly. */
  get local(): Sp00kyClient<S>['localClient'] {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.localClient;
  }

  /** Access the auth service. */
  get auth(): AuthService<S> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.auth;
  }

  get pendingMutationCount(): number {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.pendingMutationCount;
  }

  /** Diagnostic, see `Sp00kyClient.liveRetryCount`. */
  get liveRetryCount(): number {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.liveRetryCount;
  }

  subscribeToPendingMutations(cb: (count: number) => void): () => void {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.subscribeToPendingMutations(cb);
  }

  /** Queries mid-fetch right now. See {@link useSyncActivity}. */
  get fetchingQueryCount(): number {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.fetchingQueryCount;
  }

  /**
   * Observe how many queries are fetching from the server. Fires immediately
   * and on every change. Prefer the `useSyncActivity` hook in components.
   */
  subscribeToFetchActivity(cb: (fetching: number) => void): () => void {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.subscribeToFetchActivity(cb);
  }

  /** Current sync-health snapshot. See {@link useSyncStatus}. */
  get syncHealth(): SyncHealth {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.syncHealth;
  }

  /**
   * Observe sync health. Fires immediately with the current status and again
   * on every healthy/degraded transition. Prefer the `useSyncStatus` hook in
   * components; this is the imperative escape hatch.
   */
  subscribeToSyncHealth(cb: (health: SyncHealth) => void): () => void {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.subscribeToSyncHealth(cb);
  }

  /** Current local-store durability snapshot. See {@link useStorageStatus}. */
  get storageHealth(): StorageHealth {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.storageHealth;
  }

  /**
   * Observe local-store durability. Fires immediately with the current
   * snapshot and again on change. Prefer the `useStorageStatus` hook.
   */
  subscribeToStorageHealth(cb: (health: StorageHealth) => void): () => void {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.subscribeToStorageHealth(cb);
  }

  /**
   * Web Push for this browser: `support()`, `subscribe()` (from a click),
   * `devices()`, `notify()` / `test()` (push to yourself), `bridge()` for the
   * service worker. See the push-notifications docs page.
   */
  get webPush(): WebPushModule {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.webPush;
  }

  bucket<B extends BucketNames<S>>(name: B): BucketHandle {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.bucket(name);
  }

  getBucketConfig(name: string): BucketDefinitionSchema | undefined {
    return this.config.schema.buckets?.find((b) => b.name === name);
  }
}
