/**
 * Supported value types in the schema
 */
export type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json' | 'Uint8Array';

/**
 * Column metadata defining the type and optionality of a field
 */
/**
 * CRDT types supported by Sp00ky's Loro integration
 */
export type CrdtType = 'text' | 'map' | 'list' | 'counter';

export interface ColumnSchema {
  readonly type: ValueType;
  readonly optional: boolean;
  readonly dateTime?: boolean;
  readonly recordId?: boolean;
  readonly crdt?: CrdtType;
  readonly cursor?: boolean;
  /** True for `TYPE bytes` columns. Runtime values are `Uint8Array`. */
  readonly bytes?: boolean;
  /**
   * True for `TYPE array<...>` columns. `type` then names the ELEMENT type, so
   * the runtime value is `ElementType[]` (e.g. `array<string>` → `string[]`).
   */
  readonly array?: boolean;
  /**
   * True for `-- @opaque` columns: the value IS synced to the client and can be
   * read from a query result, but the sync engine never stores it server-side.
   *
   * That makes it unusable for anything the server has to evaluate — `where`,
   * `orderBy`, joins, aggregates, table permissions — because the SSP has no
   * value to evaluate against. A predicate on such a column would appear to work
   * locally (the local cache does hold the value) while matching nothing
   * server-side, so the query builder rejects it outright instead of letting the
   * two diverge silently.
   */
  readonly opaque?: boolean;
}

/**
 * Table metadata containing columns and primary key information
 */
export interface TableSchemaMetadata {
  readonly name: string;
  readonly columns: {
    readonly [columnName: string]: ColumnSchema;
  };
  readonly primaryKey: readonly string[];
}

/**
 * Cardinality of a relationship: one-to-one or one-to-many
 */
export type Cardinality = 'one' | 'many';

/**
 * Relationship metadata defining how tables relate to each other
 */
export interface RelationshipMetadata {
  readonly model: string;
  readonly table: string;
  readonly cardinality: Cardinality;
}

/**
 * Complete schema metadata structure
 * Maps table names to their schemas and relationships
 */
export interface SchemaMetadataStructure {
  readonly tables: {
    readonly [tableName: string]: TableSchemaMetadata;
  };
  readonly relationships: {
    readonly [tableName: string]: {
      readonly [fieldName: string]: RelationshipMetadata;
    };
  };
}

/**
 * Type mapping from ValueType to TypeScript types
 */
export type TypeNameToTypeMap = {
  string: string;
  number: number;
  boolean: boolean;
  null: null;
  json: unknown;
  Uint8Array: Uint8Array;
};

/**
 * The element/base TS type of a column, wrapping in an array for `array: true`
 * columns (where `type` names the element type).
 */
export type ColumnBaseTSType<T extends ColumnSchema> = T extends { array: true }
  ? TypeNameToTypeMap[T['type']][]
  : TypeNameToTypeMap[T['type']];

/**
 * Convert a column type to its TypeScript type
 */
export type ColumnToTSType<T extends ColumnSchema> = T extends {
  optional: true;
}
  ? ColumnBaseTSType<T> | null
  : ColumnBaseTSType<T>;

/**
 * Helper to extract relationship field names for a table
 */
export type RelationshipFieldNames<
  Metadata extends SchemaMetadataStructure,
  TableName extends keyof Metadata['relationships'] & string,
> = keyof Metadata['relationships'][TableName] & string;

/**
 * Helper to get cardinality for a specific relationship
 */
export type GetRelationshipCardinality<
  Metadata extends SchemaMetadataStructure,
  TableName extends keyof Metadata['relationships'] & string,
  FieldName extends keyof Metadata['relationships'][TableName] & string,
> = Metadata['relationships'][TableName][FieldName]['cardinality'];

/**
 * Helper to get related table name for a relationship
 */
export type GetRelatedTable<
  Metadata extends SchemaMetadataStructure,
  TableName extends keyof Metadata['relationships'] & string,
  FieldName extends keyof Metadata['relationships'][TableName] & string,
> = Metadata['relationships'][TableName][FieldName]['table'];

// ============================================================================
// ACCESS SCHEMA HELPERS
// ============================================================================

export interface AccessDefinition {
  readonly signIn: {
    readonly params: Record<string, ColumnSchema>;
  };
  readonly signup: {
    readonly params: Record<string, ColumnSchema>;
  };
}

// ============================================================================
// BUCKET SCHEMA HELPERS
// ============================================================================

export interface BucketDefinitionSchema {
  readonly name: string;
  readonly maxSize?: number;
  readonly allowedExtensions?: readonly string[];
  readonly pathPrefixAuth?: boolean;
}

// ============================================================================
// ARRAY-BASED SCHEMA TYPE HELPERS
// ============================================================================

/**
 * Base schema structure for array-based schemas
 */
export interface SchemaStructure {
  readonly tables: readonly {
    readonly name: string;
    readonly columns: Record<string, ColumnSchema>;
    readonly primaryKey: readonly string[];
  }[];
  readonly relationships: readonly {
    readonly from: string;
    readonly field: string;
    readonly to: string;
    readonly cardinality: Cardinality;
  }[];
  readonly backends: Record<string, HTTPOutboxBackendDefinition>;
  readonly access?: Record<string, AccessDefinition>;
  readonly buckets?: readonly BucketDefinitionSchema[];
  /**
   * Server-side policy the CLI codegen copies from `sp00ky.yml` into the
   * generated `schema` constant. `queryAllowlist` mirrors `sync.queryAllowlist`:
   * `warn` / `enforce` mean the SSP only accepts query shapes collected at
   * `spky generate` time, so the client refuses raw remote calls unless
   * `allowRawRemote` is set. `impersonation` mirrors `impersonation.enabled`;
   * the server enforces it, the client only uses it to offer the feature.
   */
  readonly policy?: {
    readonly queryAllowlist?: 'off' | 'warn' | 'enforce';
    readonly impersonation?: boolean;
  };
}

export interface HTTPOutboxBackendDefinition {
  readonly outboxTable: string;
  readonly routes: Record<string, HTTPBackendRouteDefinition>;
}

export interface HTTPBackendRouteDefinition {
  readonly args: Record<string, HTTPBackendRouteArgsDefinition>;
}

export interface HTTPBackendRouteArgsDefinition {
  readonly type: ValueType;
  readonly optional: boolean;
}

// ============================================================================
// BACKEND / RUN TYPE HELPERS
// ============================================================================

/** Extract all bucket names from the schema */
export type BucketNames<S extends SchemaStructure> =
  S['buckets'] extends readonly BucketDefinitionSchema[]
    ? S['buckets'][number]['name']
    : never;

/** Look up a bucket's config by name */
export type BucketConfig<S extends SchemaStructure, B extends BucketNames<S>> =
  S['buckets'] extends readonly BucketDefinitionSchema[]
    ? Extract<S['buckets'][number], { name: B }>
    : never;

/** Extract all backend names from the schema */
export type BackendNames<S extends SchemaStructure> = keyof S['backends'] & string;

/** Extract all route paths for a specific backend */
export type BackendRoutes<
  S extends SchemaStructure,
  B extends BackendNames<S>,
> = keyof S['backends'][B]['routes'] & string;

/** Build the typed payload for a specific route */
export type RoutePayload<
  S extends SchemaStructure,
  B extends BackendNames<S>,
  R extends BackendRoutes<S, B>,
> = [keyof RouteArgsDef<S, B, R>] extends [never]
  ? Record<string, never>
  : Prettify<
      { [K in RequiredRouteArgKeys<S, B, R>]: TypeNameToTypeMap[RouteArgsDef<S, B, R>[K]['type']] }
      &
      { [K in OptionalRouteArgKeys<S, B, R>]?: TypeNameToTypeMap[RouteArgsDef<S, B, R>[K]['type']] }
    >;

type RouteArgsDef<
  S extends SchemaStructure,
  B extends BackendNames<S>,
  R extends BackendRoutes<S, B>,
> = S['backends'][B]['routes'][R]['args'];

type RequiredRouteArgKeys<
  S extends SchemaStructure,
  B extends BackendNames<S>,
  R extends BackendRoutes<S, B>,
> = {
  [K in keyof RouteArgsDef<S, B, R>]: RouteArgsDef<S, B, R>[K]['optional'] extends true ? never : K;
}[keyof RouteArgsDef<S, B, R>] & string;

type OptionalRouteArgKeys<
  S extends SchemaStructure,
  B extends BackendNames<S>,
  R extends BackendRoutes<S, B>,
> = {
  [K in keyof RouteArgsDef<S, B, R>]: RouteArgsDef<S, B, R>[K]['optional'] extends true ? K : never;
}[keyof RouteArgsDef<S, B, R>] & string;

type Prettify<T> = { [K in keyof T]: T[K] } & {};

/**
 * Extract a specific table by name from the schema tables array
 */
export type GetTable<S extends SchemaStructure, Name extends TableNames<S>> = Extract<
  S['tables'][number],
  { name: Name }
>;

/**
 * Extract all table names from the schema
 */
export type TableNames<S extends SchemaStructure> = S['tables'][number]['name'];

/**
 * Extract all field names from a type
 */
export type TableFieldNames<T extends { columns: Record<string, ColumnSchema> }> =
  keyof T['columns'] & string;

/**
 * Convert table schema columns to a TypeScript model type
 */
export type TableModel<T extends { columns: Record<string, ColumnSchema> }> = {
  [K in keyof T['columns']]: ColumnToTSType<T['columns'][K]>;
};

/**
 * Extract all relationships for a specific table from relationships array
 */
export type TableRelationships<S extends SchemaStructure, TableName extends string> = Extract<
  S['relationships'][number],
  { from: TableName }
>;

/**
 * Get relationship field names for a table
 */
export type RelationshipFields<
  S extends SchemaStructure,
  TableName extends string,
> = TableRelationships<S, TableName>['field'];

/**
 * Get specific relationship by table and field
 */
export type GetRelationship<
  S extends SchemaStructure,
  TableName extends string,
  Field extends string,
> = Extract<Extract<S['relationships'][number], { from: TableName }>, { field: Field }>;

/**
 * Convert array-based schema to indexed format (for internal compatibility)
 */
export type SchemaToIndexed<S extends SchemaStructure> = {
  tables: {
    [K in S['tables'][number]['name']]: Extract<S['tables'][number], { name: K }>;
  };
  relationships: {
    [K in S['tables'][number]['name']]: {
      [R in Extract<S['relationships'][number], { from: K }>['field']]: Extract<
        Extract<S['relationships'][number], { from: K }>,
        { field: R }
      >;
    };
  };
};
