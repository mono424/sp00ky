// Import new schema types
export type {
  ValueType,
  ColumnSchema,
  TableSchemaMetadata,
  Cardinality,
  RelationshipMetadata,
  SchemaMetadataStructure,
  AccessDefinition,
} from './table-schema';

// Model types (backward compatibility)
export type GenericModel = Record<string, any>;
export type GenericSchema = Record<string, GenericModel>;

/**
 * Helper to constrain related field names based on relationships metadata
 */
export type RelatedField<T extends string, R> = GetRelationshipFields<T, R> & string;

// Query interfaces
export interface QueryInfo {
  query: string;
  hash: number;
  vars?: Record<string, unknown>;
  /**
   * Engine-neutral description of the same SELECT, used by non-SurrealQL local
   * cache backends (e.g. SQLite) that cannot parse the `query` string. Only
   * populated for `SELECT` (undefined for LIVE/UPDATE/DELETE). See `QueryPlan`.
   */
  plan?: QueryPlan;
}

/**
 * A single WHERE comparison. `value` is the resolved value (string IDs already
 * converted to `RecordId`); when `paramRef` is set the condition references an
 * existing query param verbatim (`$name`) instead of an inline value. `swap`
 * flips the operands (`value op field`), mirroring `ComparisonOp._swap`.
 */
export interface WhereComparison {
  field: string;
  op: ComparisonOp['_op'];
  value: unknown;
  paramRef?: string;
  swap?: boolean;
}

/** A parenthesised `(c1 OR c2 …)` group, from a `_or` fragment. */
export interface WhereOr {
  or: WhereComparison[];
}

/**
 * Engine-neutral WHERE: a top-level conjunction (AND) of comparisons and/or
 * OR-groups. Mirrors `buildQueryFromOptions`'s condition assembly exactly.
 */
export type WhereNode = WhereComparison | WhereOr;

/**
 * Engine-neutral description of a SELECT query. Backends render it to their own
 * dialect (SurrealQL, SQLite, …). Relations are resolved by the caller via
 * level-ordered decomposition rather than nested projection, so `relations`
 * carries the tree rather than a flattened subquery string.
 */
export interface QueryPlan {
  table: string;
  /** Projection field names; undefined means all (`*`). */
  select?: string[];
  where?: WhereNode[];
  orderBy?: [field: string, direction: 'asc' | 'desc'][];
  limit?: number;
  offset?: number;
  relations?: RelationPlan[];
  /**
   * Window materialization: when set, the base rows are EXACTLY these record
   * ids (the window the SSP already computed), ignoring `where`/`limit`/
   * `offset`. `orderBy`, `select` and `relations` still apply. Set by
   * {@link buildWindowMaterializationPlan}; see `window-query.ts`.
   */
  ids?: unknown[];
}

/**
 * One `.related()` edge in a {@link QueryPlan}. Correlation:
 * - `one`  → parent[`foreignKeyField`] = child.id  (attach `bucket[0] ?? null`)
 * - `many` → child[`foreignKeyField`] = parent.id   (attach `bucket`)
 * `limit`/`orderBy` are applied PER PARENT during decomposition.
 */
export interface RelationPlan {
  alias: string;
  table: string;
  cardinality: 'one' | 'many';
  foreignKeyField: string;
  select?: string[];
  where?: WhereNode[];
  orderBy?: [field: string, direction: 'asc' | 'desc'][];
  limit?: number;
  relations?: RelationPlan[];
}

export interface RelatedQuery {
  /** The name of the related table to query */
  relatedTable: string;
  /** The alias for this subquery result (defaults to relatedTable name) */
  alias?: string;
  /** Optional query modifier for the subquery */
  modifier?: SchemaAwareQueryModifier<SchemaStructure, string>;
  /** The cardinality of the relationship */
  cardinality: 'one' | 'many';
}

/**
 * Comparison-operator descriptor for a single WHERE field, e.g.
 * `{ _op: '<=', _val: 5 }` → `field <= $field`. A `$`-prefixed string `_val`
 * references an existing query param verbatim; `_swap: true` flips the operands
 * (`$val _op field`). Plain values still mean equality (`field = $field`).
 */
export interface ComparisonOp {
  _op: '=' | '!=' | '>' | '>=' | '<' | '<=' | (string & {});
  _val: unknown;
  _swap?: boolean;
}

/** A single WHERE field value: an equality value or a comparison descriptor. */
export type WhereFieldValue<V> = V | ComparisonOp;

/** A flat conjunction of field conditions (equality or comparison). */
export type WhereConditions<TModel extends GenericModel> = {
  [K in keyof TModel]?: WhereFieldValue<TModel[K]>;
};

/**
 * WHERE input for `.where()`. Supports equality (`{ field: value }`), comparison
 * operators (`{ field: { _op, _val } }`), and a single top-level `_or` group of
 * condition fragments that compile to a parenthesised `(... OR ...)` conjunct —
 * e.g. `{ _or: [{ white: x }, { black: x }] }` → `(white = $white__or0 OR black = $black__or1)`.
 * Backward-compatible with plain `Partial<TModel>` equality objects.
 */
export type WhereInput<TModel extends GenericModel> = WhereConditions<TModel> & {
  _or?: WhereConditions<TModel>[];
};

export interface QueryOptions<TModel extends GenericModel, IsOne extends boolean> {
  select?: ((keyof TModel & string) | '*')[];
  where?: WhereInput<TModel>;
  limit?: number;
  offset?: number;
  orderBy?: Partial<Record<keyof TModel, 'asc' | 'desc'>>;
  /** Related tables to include via subqueries */
  related?: RelatedQuery[];
  isOne?: IsOne;
  /**
   * Set by `.ignoreUnusedArgs()`: the caller knows some parts of this query
   * are ignored where it is used (a channel only reads the `where`), so no
   * warning is wanted. Not part of the query: never changes surql or hash.
   */
  ignoreUnusedArgs?: boolean;
}

/** A user a channel connects to: a record id, its string form, or nothing. */
export type ChannelPeer = string | { toString(): string } | null | undefined;


export type LiveQueryOptions<TModel extends GenericModel> = Omit<
  QueryOptions<TModel, boolean>,
  'orderBy'
>;

// Import schema types for schema-aware modifiers
import type {
  SchemaStructure,
  TableNames,
  GetTable,
  TableModel,
  TableRelationships,
  GetRelationship,
} from './table-schema';

// Query modifier type for related queries - now schema-aware!
export type QueryModifier<TModel extends GenericModel> = (
  builder: QueryModifierBuilder<TModel>
) => QueryModifierBuilder<TModel>;

// Schema-aware query modifier that knows about relationships
export type SchemaAwareQueryModifier<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  RelatedFields extends Record<string, any> = {},
> = (
  builder: SchemaAwareQueryModifierBuilder<S, TableName, {}>
) => SchemaAwareQueryModifierBuilder<S, TableName, RelatedFields>;

// Simplified query builder interface for modifying subqueries
export interface QueryModifierBuilder<TModel extends GenericModel> {
  where(conditions: WhereInput<TModel>): this;
  select(...fields: ((keyof TModel & string) | '*')[]): this;
  limit(count: number): this;
  offset(count: number): this;
  orderBy(field: keyof TModel & string, direction?: 'asc' | 'desc'): this;
  related<Field extends string>(relatedField: Field, modifier?: QueryModifier<any>): this;
  _getOptions(): QueryOptions<TModel, boolean>;
}

// Schema-aware query builder interface that understands relationships
export interface SchemaAwareQueryModifierBuilder<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  RelatedFields extends Record<string, any> = {},
> {
  where(conditions: WhereInput<TableModel<GetTable<S, TableName>>>): this;
  select(...fields: ((keyof TableModel<GetTable<S, TableName>> & string) | '*')[]): this;
  limit(count: number): this;
  offset(count: number): this;
  orderBy(
    field: keyof TableModel<GetTable<S, TableName>> & string,
    direction?: 'asc' | 'desc'
  ): this;
  related<
    Field extends TableRelationships<S, TableName>['field'],
    Rel extends GetRelationship<S, TableName, Field>,
    RelatedFields2 extends Record<string, any> = {},
  >(
    relatedField: Field,
    modifier?: SchemaAwareQueryModifier<S, Rel['to'], RelatedFields2>
  ): SchemaAwareQueryModifierBuilder<
    S,
    TableName,
    RelatedFields & {
      [K in Field]: {
        to: Rel['to'];
        cardinality: Rel['cardinality'];
        relatedFields: RelatedFields2;
      };
    }
  >;
  _getOptions(): QueryOptions<TableModel<GetTable<S, TableName>>, boolean>;
}

/**
 * Extract fields from a model that are relationship fields (string or string[])
 * Excludes common non-relationship fields like id, created_at, updated_at, etc.
 */
export type RelationshipFields<TModel extends GenericModel> = {
  [K in keyof TModel]: K extends 'id' | 'created_at' | 'updated_at' | 'deleted_at'
    ? never
    : TModel[K] extends string | string[] | null | undefined
      ? K
      : never;
}[keyof TModel];

/**
 * Helper type to infer the related model type from a field name using Relationships metadata
 * Simplified to directly access the nested structure
 */
export type InferRelatedModelFromMetadata<
  // oxlint-disable-next-line no-unused-vars -- Schema is used as a generic constraint
  Schema extends GenericSchema,
  TableName extends string,
  FieldName extends string,
  Relationships,
> =
  Relationships extends Record<string, Record<string, RelationshipDefinition>>
    ? TableName extends keyof Relationships
      ? FieldName extends keyof Relationships[TableName]
        ? Relationships[TableName][FieldName]['model']
        : any
      : any
    : any;

/**
 * Get cardinality for a relationship field from metadata
 * Simplified to directly access the nested structure
 */
export type GetCardinality<TableName extends string, FieldName extends string, Relationships> =
  Relationships extends Record<string, Record<string, RelationshipDefinition>>
    ? TableName extends keyof Relationships
      ? FieldName extends keyof Relationships[TableName]
        ? Relationships[TableName][FieldName]['cardinality']
        : 'many'
      : 'many'
    : 'many';

/**
 * Type that transforms a Model by replacing a field with its related records
 * Uses Relationships metadata to determine cardinality and related table
 */
export type WithRelated<
  Schema extends GenericSchema,
  TModel extends Record<string, any>,
  TableName extends string,
  FieldName extends string,
  Relationships,
> = FieldName extends keyof TModel
  ? Omit<TModel, FieldName> & {
      [K in FieldName]: GetCardinality<TableName, FieldName, Relationships> extends 'one'
        ? InferRelatedModelFromMetadata<Schema, TableName, K, Relationships> | null
        : InferRelatedModelFromMetadata<Schema, TableName, K, Relationships>[] | null;
    }
  : TModel;

/**
 * Type to extract relationship fields from Relationships metadata
 * Now simplified to just get the keys of the nested object
 */
export type GetRelationshipFields<TableName extends string, Relationships> =
  Relationships extends Record<string, Record<string, any>>
    ? TableName extends keyof Relationships
      ? keyof Relationships[TableName] & string
      : never
    : never;

/**
 * Relationship metadata structure - now a nested object for better type safety
 * Example:
 * {
 *   thread: {
 *     author: { model: Schema["user"], table: "user", cardinality: "one" },
 *     comments: { model: Schema["comment"], table: "comment", cardinality: "many" }
 *   }
 * }
 */
export interface RelationshipDefinition<Model = any> {
  /** The related model type */
  model: Model;
  /** The related table name */
  table: string;
  /** Whether this is a 1:1 or 1:many relationship */
  cardinality: 'one' | 'many';
}

export type RelationshipsMetadata = Record<string, Record<string, RelationshipDefinition>>;
