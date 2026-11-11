import type { Injector } from '@angular/core';
import type { RecordId } from 'surrealdb';
import type { Sp00kyConfig } from '@spooky-sync/core';
import type {
  GenericSchema,
  GetTable,
  SchemaStructure,
  TableModel,
  TableNames,
} from '@spooky-sync/query-builder';
import type { SyncedDb } from './db';

export type { GenericModel, GenericSchema } from '@spooky-sync/query-builder';

export type Model<T> = T;
export type ModelPayload<T> = T & { id: RecordId };

/**
 * A value, a signal, or a function that reads signals. Angular tracks the
 * signals read inside it, so an input that should follow component state is
 * passed as a signal (`this.postId`, an `input()`) or a function
 * (`() => this.postId()`); a plain value is fixed.
 */
export type MaybeSignal<T> = T | (() => T);

/**
 * Every `inject*` function must run in an injection context (a constructor, a
 * field initializer, a factory). Pass `injector` to call one anywhere else,
 * like Angular's own `effect()` and `toSignal()`.
 */
export interface InjectOptions {
  injector?: Injector;
}

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

/** Second argument of `provideSp00ky`. */
export interface Sp00kyOptions<S extends SchemaStructure> {
  /**
   * Prewarm data into the local cache before the app renders. Runs after
   * `init()`, inside the same app initializer. Best-effort: a rejection is
   * logged and the app starts anyway.
   */
  preload?: (db: SyncedDb<S>) => Promise<void>;
}
