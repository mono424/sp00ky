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
  type PreloadRefresh,
} from '@spooky-sync/core';

import type {
  GetTable,
  QueryBuilder,
  SchemaStructure,
  TableModel,
  TableNames,
  QueryResult,
  RelatedFieldsMap,
  RelationshipFieldsFromSchema,
  GetRelationship,
  RelatedFieldMapEntry,
  FinalQuery,
  InnerQuery,
  BackendNames,
  BackendRoutes,
  RoutePayload,
  BucketNames,
  BucketDefinitionSchema,
  QueryModifier,
  QueryModifierBuilder,
  QueryInfo,
  RelationshipsMetadata,
  RelationshipDefinition,
  InferRelatedModelFromMetadata,
  GetCardinality,
} from '@spooky-sync/query-builder';

import { RecordId, Uuid, type Surreal } from 'surrealdb';
import { snapshot } from 'solid-js';
export { RecordId, Uuid };
export type { Model, GenericModel, GenericSchema, ModelPayload } from './lib/models';
export { createQuery, useQuery, type CreateQueryResult, type QueryOptions } from './lib/create-query';
export { createPreload } from './lib/create-preload';
export type { PreloadOptions, PreloadRefresh } from '@spooky-sync/core';
export { useSyncStatus, type UseSyncStatus } from './lib/use-sync-status';
export {
  useSyncActivity,
  type UseSyncActivity,
  type UseSyncActivityOptions,
} from './lib/use-sync-activity';
export type {
  SyncHealth,
  SyncHealthStatus,
  SyncHealthConfig,
  ConnectionState,
  ReconnectConfig,
} from '@spooky-sync/core';
export { useStorageStatus, type UseStorageStatus } from './lib/use-storage-status';
export type { StorageHealth, StorageHealthStatus } from '@spooky-sync/core';
export { useCrdtField } from './lib/use-crdt-field';
export { useFeatureFlag, type UseFeatureFlag } from './lib/use-feature-flag';
export {
  useAppRelease,
  type UseAppRelease,
  type UseAppReleaseOptions,
} from './lib/use-app-release';
export { useFileUpload, type FileUploadResult } from './lib/use-file-upload';
export {
  useDownloadFile,
  type UseDownloadFileOptions,
  type UseDownloadFileResult,
} from './lib/use-download-file';
export { useBlurhash, type UseBlurhashResult } from './lib/use-blurhash';
export {
  useBucketImage,
  type UseBucketImageOptions,
  type UseBucketImageResult,
} from './lib/use-bucket-image';
export { Blurhash, type BlurhashProps } from './lib/Blurhash';
export { BucketImage, type BucketImageProps } from './lib/BucketImage';
export { Sp00kyProvider, type Sp00kyProviderProps } from './lib/Sp00kyProvider';
export { useDb, usePendingMutations } from './lib/context';
export { createSubmission, type Submission } from './lib/create-submission';
export { conflate } from './lib/conflate';
export { fromSubscription } from './lib/from-subscription';

// Re-export query builder types for convenience
export type {
  QueryModifier,
  QueryModifierBuilder,
  QueryInfo,
  RelationshipsMetadata,
  RelationshipDefinition,
  InferRelatedModelFromMetadata,
  GetCardinality,
  GetTable,
  TableModel,
  TableNames,
  QueryResult,
};

export type RelationshipField<
  Schema extends SchemaStructure,
  TableName extends TableNames<Schema>,
  Field extends RelationshipFieldsFromSchema<Schema, TableName>,
> = GetRelationship<Schema, TableName, Field>;

export type RelatedFieldsTableScoped<
  Schema extends SchemaStructure,
  TableName extends TableNames<Schema>,
  RelatedFields extends RelationshipFieldsFromSchema<Schema, TableName> =
    RelationshipFieldsFromSchema<Schema, TableName>,
> = {
  [K in RelatedFields]: {
    to: RelationshipField<Schema, TableName, K>['to'];
    relatedFields: RelatedFieldsMap;
    cardinality: RelationshipField<Schema, TableName, K>['cardinality'];
  };
};

export type InferModel<
  Schema extends SchemaStructure,
  TableName extends TableNames<Schema>,
  RelatedFields extends RelatedFieldsTableScoped<Schema, TableName>,
> = QueryResult<Schema, TableName, RelatedFields, true>;

export type WithRelated<Field extends string, RelatedFields extends RelatedFieldsMap = {}> = {
  [K in Field]: Omit<RelatedFieldMapEntry, 'relatedFields'> & {
    relatedFields: RelatedFields;
  };
};

export type WithRelatedMany<Field extends string, RelatedFields extends RelatedFieldsMap = {}> = {
  [K in Field]: {
    to: Field;
    relatedFields: RelatedFields;
    cardinality: 'many';
  };
};

/**
 * SyncedDb - A thin wrapper around sp00ky-ts for Solid.js integration.
 * Delegates all logic to the underlying sp00ky-ts instance.
 *
 * NOTE: keep in sync with packages/client-solid/src/index.ts (SyncedDb).
 * Copied rather than shared so this package's dependency graph never pulls
 * in solid-js 1.x; fold the two together once client-solid moves to Solid 2.
 * Solid-2-only differences: `delete` accepts 'bound RecordId', and write
 * payloads go through `snapshot()` (see `unproxy` below).
 */

/**
 * Solid 2 stores wrap every object read out of them (query rows, their nested
 * arrays, `createStore` docs) in a Proxy. Those proxies cannot cross
 * `postMessage` (the sqlite and shared-tabs workers): structuredClone throws
 * DataCloneError. `snapshot()` returns the underlying plain value for store
 * proxies and passes anything else through untouched.
 */
function unproxy<T>(value: T): T {
  if (value === null || typeof value !== 'object') return value;
  return snapshot(value as object) as T;
}
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

  /**
   * Initialize the sp00ky-ts instance
   */
  async init(): Promise<void> {
    if (this._initialized) return;
    this.sp00ky = new Sp00kyClient<S>(this.config);
    await this.sp00ky.init();
    this._initialized = true;
  }

  /**
   * Tear down the client: leaves the tabs broker, closes the local store and
   * remote socket, and frees the wasm circuit. Without this a remounted provider
   * (or an HMR reload) strands a whole client, and the abandoned wasm heaps stay
   * resident because V8 cannot see how much wasm memory a dropped wrapper holds.
   */
  async close(): Promise<void> {
    const instance = this.sp00ky;
    this.sp00ky = null;
    this._initialized = false;
    if (instance) await instance.close();
  }

  /**
   * Create a new record in the database
   */
  async create(id: string, payload: Record<string, unknown>): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.create(id, unproxy(payload) as Record<string, unknown>);
  }

  /**
   * Update an existing record in the database
   */
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
      unproxy(payload) as Record<string, unknown>,
      options
    );
  }

  /**
   * Delete an existing record in the database
   */
  async delete<TName extends TableNames<S>>(
    tableName: TName,
    selector: string | RecordId | InnerQuery<GetTable<S, TName>, boolean>
  ): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    // Accept a `"table:id"` string OR a RecordId — live-query rows carry their
    // `id` as a RecordId, so callers can pass `db.delete('game', row.id)`
    // directly. Build the canonical string from the raw id part (not
    // `RecordId.toString()`, which escapes special chars) so it round-trips
    // through the engine's `parseRecordIdString`. InnerQuery selectors are not
    // supported yet. A RecordId read out of a Solid 2 query row is a store
    // proxy that hides its prototype (no instanceof, no constructor.name, on
    // rc.1), so unwrap it first; cross-package RecordId instances then match
    // by constructor name ('bound RecordId' covers rc.0 proxies).
    const raw = unproxy(selector);
    const ctorName = (raw as any)?.constructor?.name;
    const isRecordId =
      raw instanceof RecordId || ctorName === 'RecordId' || ctorName === 'bound RecordId';
    let id: string;
    if (typeof raw === 'string') {
      id = raw;
    } else if (isRecordId) {
      id = `${tableName as string}:${(raw as RecordId).id}`;
    } else {
      throw new Error('Only string ID or RecordId selectors are supported currently with core');
    }
    await this.sp00ky.delete(tableName as string, id);
  }

  /**
   * Preload/prewarm a built query into the local cache without registering a
   * live view. Fetches once and stores the rows (+ embedded related children)
   * locally so a later `createQuery` for the same data paints instantly. Best-effort.
   */
  public async preload(
    finalQuery: FinalQuery<S, any, any, any, any, Sp00kyQueryResultPromise>,
    options?: PreloadOptions
  ): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.preload(finalQuery, options);
  }

  /**
   * Query data from the database
   */
  public query<TName extends TableNames<S>>(
    table: TName
  ): QueryBuilder<S, TName, Sp00kyQueryResultPromise, {}, false> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.query(table, {});
  }

  /**
   * Run a backend operation
   */
  public async run<B extends BackendNames<S>, R extends BackendRoutes<S, B>>(
    backend: B,
    path: R,
    payload: RoutePayload<S, B, R>,
    options?: RunOptions
  ): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.run(backend, path, unproxy(payload), options);
  }

  /**
   * Sign out, clear session and local storage
   */
  public async signOut(): Promise<void> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    await this.sp00ky.auth.signOut();
  }

  /**
   * Execute a function with direct access to the remote database connection
   */
  public async useRemote<T>(fn: (db: Surreal) => T | Promise<T>): Promise<T> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return await this.sp00ky.useRemote(fn);
  }

  /**
   * Run one SurrealQL statement against the REMOTE database, results shaped
   * like the SDK's own `.query()` (one entry per statement).
   *
   * Prefer this over `useRemote(s => s.query(...))` for a plain read: the bare
   * SDK client the callback receives bypasses the client's connect gate, so a
   * statement issued while the socket is being rebuilt fails with "Specify a
   * namespace to use" instead of waiting for the session to be applied. It is
   * also bounded by the remote service's concurrency limit and query timeout.
   *
   * Vars go through `unproxy` for the same reason writes do: a value read out
   * of a Solid 2 store is a Proxy, and one bound as a query parameter reaches
   * the CBOR encoder (and, under shared tabs, `postMessage`) as something it
   * cannot serialize.
   */
  public async remoteQuery<T extends unknown[] = unknown[]>(
    sql: string,
    vars?: Record<string, unknown>
  ): Promise<T> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return await this.sp00ky.remoteQuery<T>(
      sql,
      vars ? (unproxy(vars) as Record<string, unknown>) : undefined
    );
  }
  /**
   * Access the remote database service directly
   */
  get remote(): Sp00kyClient<S>['remoteClient'] {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.remoteClient;
  }

  /**
   * Access the local database service directly
   */
  get local(): Sp00kyClient<S>['localClient'] {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.localClient;
  }

  /**
   * Access the auth service
   */
  get auth(): AuthService<S> {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.auth;
  }

  get pendingMutationCount(): number {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.pendingMutationCount;
  }

  /** Diagnostic — see `Sp00kyClient.liveRetryCount`. */
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
   * on every healthy↔degraded transition. Prefer the `useSyncStatus` hook in
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
   * Observe local-store durability. Fires immediately with the current snapshot
   * and again on change. Prefer the `useStorageStatus` hook in components; this
   * is the imperative escape hatch.
   */
  subscribeToStorageHealth(cb: (health: StorageHealth) => void): () => void {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.subscribeToStorageHealth(cb);
  }

  bucket<B extends BucketNames<S>>(name: B): BucketHandle {
    if (!this.sp00ky) throw new Error('SyncedDb not initialized');
    return this.sp00ky.bucket(name);
  }

  getBucketConfig(name: string): BucketDefinitionSchema | undefined {
    return this.config.schema.buckets?.find((b) => b.name === name);
  }
}

export * from './types';
