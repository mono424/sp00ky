import type { Snippet } from 'svelte';
import type { RecordId } from 'surrealdb';
import type { Sp00kyConfig } from '@spooky-sync/core';
import type {
  GenericSchema,
  GetTable,
  SchemaStructure,
  TableModel,
  TableNames,
} from '@spooky-sync/query-builder';
import type { SyncedDb } from './db.js';

export type { GenericModel, GenericSchema } from '@spooky-sync/query-builder';

export type Model<T> = T;
export type ModelPayload<T> = T & { id: RecordId };

/**
 * A value, or a getter that returns it. Svelte only tracks state that is read
 * inside a reactive scope, so an input that should follow `$state` / `$props`
 * is passed as a getter (`() => id`); a plain value is read once.
 */
export type MaybeGetter<T> = T | (() => T);

/** Options for database provisioning. */
export interface ProvisionOptions {
  /** Force re-provision even if schema already exists */
  force?: boolean;
}

export type CacheStrategy = 'memory' | 'indexeddb';

/** Infer Schema type (Record<TableName, Model>) from the schema const. */
export type InferSchemaFromConst<S extends SchemaStructure> = {
  [K in TableNames<S>]: TableModel<GetTable<S, K>>;
};

/**
 * Infer Relationships type from the schema const's relationships array,
 * converted from array format to nested object format.
 */
export type InferRelationshipsFromConst<S extends SchemaStructure, Schema extends GenericSchema> = {
  [TableName in TableNames<S>]: {
    [Rel in Extract<S['relationships'][number], { from: TableName }> as Rel['field']]: {
      model: Rel['to'] extends keyof Schema ? Schema[Rel['to']] : any;
      table: Rel['to'];
      cardinality: Rel['cardinality'];
    };
  };
};

// Prettify helper expands types for better intellisense
type Prettify<T> = { [K in keyof T]: T[K] } & {};

export type SyncedDbConfig<S extends SchemaStructure> = Prettify<Sp00kyConfig<S>>;

export interface Sp00kyProviderProps<S extends SchemaStructure> {
  /** Client config. Read once when the provider mounts; wrap it in `{#key}` to switch databases. */
  config: SyncedDbConfig<S>;
  /** Rendered until the client is ready (and on the server). */
  fallback?: Snippet;
  /** Init failed. Without it the error is logged. */
  onError?: (error: Error) => void;
  /** The client is ready, right before the children render. */
  onReady?: (db: SyncedDb<S>) => void;
  /**
   * Prewarm data into the local cache before revealing the UI. Runs after
   * `init()`; the `fallback` stays up until it resolves. Best-effort: a
   * rejection is logged and the UI is revealed anyway.
   */
  preload?: (db: SyncedDb<S>) => Promise<void>;
  children: Snippet;
}
