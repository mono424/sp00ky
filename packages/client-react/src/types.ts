import type { RecordId } from 'surrealdb';
import type { Sp00kyConfig } from '@spooky-sync/core';
import type {
  GenericSchema,
  GetTable,
  SchemaStructure,
  TableModel,
  TableNames,
} from '@spooky-sync/query-builder';

export type { GenericModel, GenericSchema } from '@spooky-sync/query-builder';

export type Model<T> = T;
export type ModelPayload<T> = T & { id: RecordId };

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
