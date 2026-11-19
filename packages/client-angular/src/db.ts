import type { SyncedDbConfig } from './types';
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
 * SyncedDb - a thin wrapper around the sp00ky core client for Angular.
 * Delegates all logic to the underlying `Sp00kyClient` instance.
 *
 * It is also the DI token: `provideSp00ky(config)` provides one instance per
 * application, and `inject(SyncedDb)` / `injectDb()` return it.
 *
 * NOTE: keep in sync with packages/client-react/src/db.ts,
 * packages/client-svelte/src/db.ts and packages/client-solid2/src/index.ts
 * (SyncedDb). Copied rather than shared so each binding's dependency graph
 * stays its own. Angular rows are plain objects (signals never proxy), so
 * unlike the Svelte and Solid 2 copies nothing is unwrapped before a write.
 */
export class SyncedDb<S extends SchemaStructure> {
  private config: SyncedDbConfig<S>;
  private sp00ky: Sp00kyClient<S> | null = null;
  private _initialized = false;
  private eager: boolean;

  /**
   * `eager`: the instance may be used before `init()` has run, which is what
   * `provideSp00ky(config, { eager: true })` provides, so the app renders at
   * once. Its core client is then created on first use; queries and writes
   * asked for early wait for `init()`, and `peekQuery` has rows to render
   * meanwhile. Without it, using the instance before `init()` throws, as it
   * always did.
   */
  constructor(config: SyncedDbConfig<S>, options: { eager?: boolean } = {}) {
    this.config = config;
    this.eager = options.eager === true;
  }

  public getSp00ky(): Sp00kyClient<S> {
    if (!this.sp00ky) {
      if (!this.eager) throw new Error('SyncedDb not initialized');
      // Creating the client opens nothing: that is `init()`.
      this.sp00ky = new Sp00kyClient<S>(this.config);
    }
    return this.sp00ky;
  }

  /** Initialize the core client. */
  async init(): Promise<void> {
    if (this._initialized) return;
    this.sp00ky ??= new Sp00kyClient<S>(this.config);
    try {
      await this.sp00ky.init();
    } catch (error) {
      this.ready.reject(error);
      throw error;
    }
    this._initialized = true;
    this.ready.resolve();
  }

  /** Settled by `init()` (or by `close()`): what `whenReady()` hands out, whether or not a client exists yet. */
  private ready = SyncedDb.gate();

  private static gate(): { promise: Promise<void>; resolve: () => void; reject: (error: unknown) => void } {
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<void>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    // Unobserved, a failed init must not also be an unhandled rejection.
    promise.catch(() => {});
    return { promise, resolve, reject };
  }

  /** True once `init()` has finished. */
  get isReady(): boolean {
    return this._initialized;
  }

  /**
   * Resolves once `init()` has finished, and rejects if it fails. In an eager
   * app, await it for whatever must not run on a client that is not open yet.
   */
  whenReady(): Promise<void> {
    return this.ready.promise;
  }

  /**
   * The rows `finalQuery` rendered the last time this browser had the app
   * open, at once, for a first read that comes before `init()` is done.
   * `null` when there are none, when this instance is not eager, and once the
   * client is ready (a query's own first value carries them from then on).
   * Needs `localEngine: 'rows'` (its paint cache). `injectQuery` calls this;
   * an app rarely has to.
   */
  peekQuery(
    finalQuery: FinalQuery<S, any, any, any, any, Sp00kyQueryResultPromise>
  ): Record<string, unknown>[] | null {
    if (this._initialized || !this.eager) return null;
    return this.getSp00ky().peekQuery(finalQuery);
  }

  /**
   * Tear down the client: leaves the tabs broker, closes the local store and
   * remote socket, and frees the wasm circuit.
   */
  async close(): Promise<void> {
    const instance = this.sp00ky;
    this.sp00ky = null;
    this._initialized = false;
    // Whoever still waits for a client that is going away must not wait forever.
    this.ready.reject(new Error('SyncedDb: closed before it was ready'));
    this.ready = SyncedDb.gate();
    if (instance) await instance.close();
  }

  /** Create a new record in the database. */
  async create(id: string, payload: Record<string, unknown>): Promise<void> {
    await this.getSp00ky().create(id, payload);
  }

  /** Update an existing record in the database. */
  async update<TName extends TableNames<S>>(
    tableName: TName,
    recordId: string,
    payload: Partial<TableModel<GetTable<S, TName>>>,
    options?: UpdateOptions
  ): Promise<void> {
    await this.getSp00ky().update(
      tableName as string,
      recordId,
      payload as Record<string, unknown>,
      options
    );
  }

  /** Delete an existing record in the database. */
  async delete<TName extends TableNames<S>>(
    tableName: TName,
    selector: string | RecordId | InnerQuery<GetTable<S, TName>, boolean>
  ): Promise<void> {
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
    await this.getSp00ky().delete(tableName as string, id);
  }

  /**
   * Preload/prewarm a built query into the local cache without a component
   * subscribing to it, so a later `injectQuery` for the same data paints
   * instantly. Best-effort.
   */
  public async preload(
    finalQuery: FinalQuery<S, any, any, any, any, Sp00kyQueryResultPromise>,
    options?: PreloadOptions
  ): Promise<void> {
    await this.getSp00ky().preload(finalQuery, options);
  }

  /** Start a query on `table`. Finish it with `.build()` and pass it to `injectQuery`. */
  public query<TName extends TableNames<S>>(
    table: TName
  ): QueryBuilder<S, TName, Sp00kyQueryResultPromise, {}, false> {
    return this.getSp00ky().query(table, {});
  }

  /**
   * Open a peer to peer channel to `peers` for the rows `query` matches (a
   * query on a `-- @channel` table; only its `where` counts). Prefer the
   * `injectChannel` function, which closes it with the component.
   */
  public channel<T extends ChannelTableNames<S>>(
    query: ChannelQuery<S, T> | ReadonlyArray<ChannelQuery<S, T>>,
    peers: Iterable<ChannelPeer>
  ): Sp00kyChannel<S, T> {
    return this.getSp00ky().channel(query, peers);
  }

  /** Run a backend operation (writes an outbox job row). */
  public async run<B extends BackendNames<S>, R extends BackendRoutes<S, B>>(
    backend: B,
    path: R,
    payload: RoutePayload<S, B, R>,
    options?: RunOptions
  ): Promise<void> {
    await this.getSp00ky().run(backend, path, payload, options);
  }

  /** Authenticate the remote connection with a token. */
  public async authenticate(token: string): Promise<void> {
    await this.getSp00ky().authenticate(token);
  }

  /**
   * Tell the client that this app's own impersonation banner is on screen.
   * Only needed with `impersonationBanner: { mode: 'custom' }`; the
   * `injectImpersonation({ rendersBanner: true })` calls it for you.
   */
  public acknowledgeImpersonationBanner(): void {
    this.getSp00ky().acknowledgeImpersonationBanner();
  }

  /** Sign out, clear session and local storage. */
  public async signOut(): Promise<void> {
    await this.getSp00ky().auth.signOut();
  }

  /** Execute a function with direct access to the remote database connection. */
  public async useRemote<T>(fn: (db: Surreal) => T | Promise<T>): Promise<T> {
    return await this.getSp00ky().useRemote(fn);
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
    return await this.getSp00ky().remoteQuery<T>(sql, vars);
  }

  /** Access the remote database service directly. */
  get remote(): Sp00kyClient<S>['remoteClient'] {
    return this.getSp00ky().remoteClient;
  }

  /** Access the local database service directly. */
  get local(): Sp00kyClient<S>['localClient'] {
    return this.getSp00ky().localClient;
  }

  /** Access the auth service. */
  get auth(): AuthService<S> {
    return this.getSp00ky().auth;
  }

  get pendingMutationCount(): number {
    return this.getSp00ky().pendingMutationCount;
  }

  /** Diagnostic, see `Sp00kyClient.liveRetryCount`. */
  get liveRetryCount(): number {
    return this.getSp00ky().liveRetryCount;
  }

  subscribeToPendingMutations(cb: (count: number) => void): () => void {
    return this.getSp00ky().subscribeToPendingMutations(cb);
  }

  /** Queries mid-fetch right now. See {@link injectSyncActivity}. */
  get fetchingQueryCount(): number {
    return this.getSp00ky().fetchingQueryCount;
  }

  /**
   * Observe how many queries are fetching from the server. Fires immediately
   * and on every change. Prefer `injectSyncActivity` in components.
   */
  subscribeToFetchActivity(cb: (fetching: number) => void): () => void {
    return this.getSp00ky().subscribeToFetchActivity(cb);
  }

  /** Current sync-health snapshot. See {@link injectSyncStatus}. */
  get syncHealth(): SyncHealth {
    return this.getSp00ky().syncHealth;
  }

  /**
   * Observe sync health. Fires immediately with the current status and again
   * on every healthy/degraded transition. Prefer `injectSyncStatus` in
   * components; this is the imperative escape hatch.
   */
  subscribeToSyncHealth(cb: (health: SyncHealth) => void): () => void {
    return this.getSp00ky().subscribeToSyncHealth(cb);
  }

  /** Current local-store durability snapshot. See {@link injectStorageStatus}. */
  get storageHealth(): StorageHealth {
    return this.getSp00ky().storageHealth;
  }

  /**
   * Observe local-store durability. Fires immediately with the current
   * snapshot and again on change. Prefer `injectStorageStatus`.
   */
  subscribeToStorageHealth(cb: (health: StorageHealth) => void): () => void {
    return this.getSp00ky().subscribeToStorageHealth(cb);
  }

  /**
   * Web Push for this browser: `support()`, `subscribe()` (from a click),
   * `devices()`, `notify()` / `test()` (push to yourself), `bridge()` for the
   * service worker. See the push-notifications docs page.
   */
  get webPush(): WebPushModule {
    return this.getSp00ky().webPush;
  }

  bucket<B extends BucketNames<S>>(name: B): BucketHandle {
    return this.getSp00ky().bucket(name);
  }

  getBucketConfig(name: string): BucketDefinitionSchema | undefined {
    return this.config.schema.buckets?.find((b) => b.name === name);
  }
}
