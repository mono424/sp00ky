import { RecordId } from 'surrealdb';
import type {
  GenericModel,
  QueryInfo,
  QueryOptions,
  QueryModifier,
  RelatedQuery,
  SchemaAwareQueryModifier,
  SchemaAwareQueryModifierBuilder,
  WhereInput,
  QueryPlan,
  RelationPlan,
  WhereNode,
  WhereComparison,
  ComparisonOp,
} from './types';
import type {
  TableNames,
  GetTable,
  TableModel,
  TableRelationships,
  GetRelationship,
  SchemaStructure,
  TableFieldNames,
  ColumnSchema,
} from './table-schema';

/**
 * Reject a query clause that references an `-- @opaque` column.
 *
 * An `@opaque` value is synced down to the client but never held server-side, so
 * the SSP has nothing to evaluate a predicate against. Left unchecked, the
 * failure is silent and asymmetric: the local cache DOES hold the value, so the
 * clause filters correctly on screen while the server-side membership set it is
 * compared against was computed without it. The result reads as rows randomly
 * appearing and vanishing rather than as an error, so fail loudly at the call
 * site instead.
 *
 * `clause` names the offending API ('where', 'orderBy', …) for the message.
 */
function assertNotOpaque(
  schema: SchemaStructure,
  tableName: string,
  clause: string,
  fields: readonly string[]
): void {
  const table = schema.tables.find((t) => t.name === tableName);
  if (!table) return;
  for (const field of fields) {
    // A nested path (`meta.secret`) is checked against its root, since the column
    // flag lives on `meta`. Comparison operators use `{ field: { _op, _val } }`
    // rather than a name suffix, so the key is always the bare column name.
    const column: ColumnSchema | undefined =
      table.columns[field] ?? table.columns[field.split('.')[0]];
    if (column?.ephemeral) {
      throw new Error(
        `Cannot use '${field}' in ${clause}(): it is marked '-- @ephemeral' on ` +
          `${tableName}, so it only travels between peers and the database never ` +
          `holds it. Read the field from query results instead.`
      );
    }
    if (column?.opaque) {
      throw new Error(
        `Cannot use '${field}' in ${clause}(): it is marked '-- @opaque' on ` +
          `${tableName}, so the sync engine never stores its value and cannot ` +
          `evaluate it. Read the field from query results instead, or remove ` +
          `'-- @opaque' from the schema if you need to query on it.`
      );
    }
  }
}

/** Every column name a `where` clause touches, including `_or` branches. */
function whereFieldNames(conditions: Record<string, unknown>): string[] {
  const out: string[] = [];
  for (const [key, value] of Object.entries(conditions)) {
    if (key === '_or') {
      for (const branch of (value ?? []) as Record<string, unknown>[]) {
        out.push(...whereFieldNames(branch ?? {}));
      }
      continue;
    }
    out.push(key);
  }
  return out;
}

/**
 * Parse a string ID to RecordId
 * - If it's in the format "table:id", use it as-is
 * - If it's just an ID without ":", prepend the table name
 * @param value - The value to parse (could be a string ID)
 * @param tableName - The table name to use if the ID doesn't contain ":"
 * @param fieldName - The field name to determine if this is an ID field
 */
function parseStringToRecordId(value: unknown, tableName?: string, fieldName?: string): unknown {
  if (typeof value !== 'string') return value;

  // If it already contains ":", parse it as a full record ID
  if (value.includes(':')) {
    const [table, ...idParts] = value.split(':');
    const id = idParts.join(':'); // Handle IDs that contain colons
    return new RecordId(table, id);
  }

  // If this is an "id" field and we have a table name, prepend it
  if (fieldName === 'id' && tableName) {
    return new RecordId(tableName, value);
  }

  // Otherwise, return as-is (it might not be an ID at all)
  return value;
}

/**
 * Recursively parse string IDs to RecordId in an object
 * @param obj - The object to parse
 * @param tableName - The table name to use for ID fields without ":"
 */
function parseObjectIdsToRecordId(obj: unknown, tableName?: string): unknown {
  if (obj === null || obj === undefined) return obj;

  if (typeof obj === 'string') {
    return parseStringToRecordId(obj, tableName);
  }

  if (Array.isArray(obj)) {
    return obj.map((item) => parseObjectIdsToRecordId(item, tableName));
  }

  if (typeof obj === 'object' && obj.constructor === Object) {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      // Parse recursively, passing the field name to identify ID fields
      result[key] =
        typeof value === 'string'
          ? parseStringToRecordId(value, tableName, key)
          : parseObjectIdsToRecordId(value, tableName);
    }
    return result;
  }

  return obj;
}

export type Executor<T extends { columns: Record<string, ColumnSchema> }, R = void> = (
  query: InnerQuery<T, boolean>
) => R;

export class InnerQuery<
  T extends { columns: Record<string, ColumnSchema> },
  IsOne extends boolean,
  R = void,
> {
  private _hash: number;
  private _mainQuery: QueryInfo;
  private _selectQuery: QueryInfo;
  private _selectLiveQuery: QueryInfo;
  private _subqueries: InnerQuery<{ columns: Record<string, ColumnSchema> }, boolean>[];

  constructor(
    private readonly _tableName: string,
    private readonly options: QueryOptions<TableModel<T>, IsOne>,
    private readonly schema: SchemaStructure,
    private readonly executor: Executor<any, R>
  ) {
    this._selectQuery = buildQueryFromOptions('SELECT', this._tableName, this.options, this.schema);

    this._mainQuery = buildQueryFromOptions(
      'SELECT',
      this._tableName,
      { ...this.options, related: [] },
      this.schema
    );

    this._hash = this._selectQuery.hash;

    this._selectLiveQuery = buildQueryFromOptions(
      'LIVE SELECT',
      this._tableName,
      this.options,
      this.schema
    );

    this._subqueries = extractSubqueryQueryInfos(
      schema,
      this._tableName,
      this.options,
      this.executor
    );
  }

  get mainQuery(): QueryInfo {
    return this._mainQuery;
  }

  get subqueries(): InnerQuery<{ columns: Record<string, ColumnSchema> }, boolean>[] {
    return this._subqueries;
  }

  get selectQuery(): QueryInfo {
    return this._selectQuery;
  }

  get selectLiveQuery(): QueryInfo {
    return this._selectLiveQuery;
  }

  get tableName(): string {
    return this._tableName;
  }

  get hash(): number {
    return this._hash;
  }

  get isOne(): boolean {
    return this.options.isOne ?? false;
  }

  public run(): R {
    return this.executor(this);
  }


  public buildUpdateQuery(patches: any[]): QueryInfo {
    return buildQueryFromOptions('UPDATE', this._tableName, this.options, this.schema, patches);
  }

  public buildDeleteQuery(): QueryInfo {
    return buildQueryFromOptions('DELETE', this._tableName, this.options, this.schema);
  }

  public getOptions(): QueryOptions<TableModel<T>, IsOne> {
    return this.options;
  }
}

/**
 * Helper type to get the model type for a related table
 */
type _GetRelatedModel<S extends SchemaStructure, RelatedTableName extends string> =
  RelatedTableName extends TableNames<S> ? TableModel<GetTable<S, RelatedTableName>> : never;

/**
 * Helper type to extract field names from RelatedFields
 */
export type ExtractFieldNames<RelatedFields extends RelatedFieldsMap> = keyof RelatedFields;

export type RelatedFieldMapEntry = {
  to: string;
  cardinality: 'one' | 'many';
  relatedFields: RelatedFieldsMap;
};

export type RelatedFieldsMap = Record<string, RelatedFieldMapEntry>;

/**
 * Helper type to build the related fields object based on accumulated relationships
 */
export type BuildRelatedFields<
  S extends SchemaStructure,
  RelatedFields extends RelatedFieldsMap,
> = {
  [K in keyof RelatedFields]: QueryResult<
    S,
    RelatedFields[K]['to'],
    RelatedFields[K]['relatedFields'],
    RelatedFields[K]['cardinality'] extends 'one' ? true : false
  >;
};

export type BuildResultModelOne<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  RelatedFields extends RelatedFieldsMap,
> = Omit<TableModel<GetTable<S, TableName>>, ExtractFieldNames<RelatedFields>> &
  BuildRelatedFields<S, RelatedFields>;

export type BuildResultModelMany<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  RelatedFields extends RelatedFieldsMap,
> = (Omit<TableModel<GetTable<S, TableName>>, ExtractFieldNames<RelatedFields>> &
  BuildRelatedFields<S, RelatedFields>)[];

/**
 * The final result type combining base model with related fields
 * Excludes related field keys from the base model to avoid type conflicts
 */
export type QueryResult<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  RelatedFields extends RelatedFieldsMap,
  IsOne extends boolean,
> = IsOne extends true
  ? BuildResultModelOne<S, TableName, RelatedFields>
  : BuildResultModelMany<S, TableName, RelatedFields>;

export class FinalQuery<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  // oxlint-disable-next-line no-unused-vars -- RelatedFields is used externally for type inference
  RelatedFields extends RelatedFieldsMap,
  IsOne extends boolean,
  R = void,
> {
  private _innerQuery: InnerQuery<T, IsOne, R>;

  constructor(
    private readonly tableName: TableName,
    private readonly options: QueryOptions<TableModel<T>, IsOne>,
    private readonly schema: S,
    private readonly executor: Executor<T, R>
  ) {
    this._innerQuery = new InnerQuery<T, IsOne, R>(
      this.tableName,
      this.options,
      this.schema,
      this.executor
    );
  }

  run(): R {
    return this.executor(this._innerQuery);
  }

  buildUpdateQuery(patches: any[]): QueryInfo {
    return this._innerQuery.buildUpdateQuery(patches);
  }

  buildDeleteQuery(): QueryInfo {
    return this._innerQuery.buildDeleteQuery();
  }

  selectLive(): QueryInfo {
    return this._innerQuery.selectLiveQuery;
  }

  get innerQuery(): InnerQuery<T, IsOne, R> {
    return this._innerQuery;
  }

  get isOne(): boolean {
    return this.options.isOne ?? false;
  }

  get hash(): number {
    return this._innerQuery.hash;
  }

  /** Whether `.ignoreUnusedArgs()` was called on the builder. */
  get ignoresUnusedArgs(): boolean {
    return this.options.ignoreUnusedArgs === true;
  }
}

/**
 * Schema-aware query modifier builder implementation
 * This version provides full type safety for nested relationships
 */
class SchemaAwareQueryModifierBuilderImpl<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  RelatedFields extends RelatedFieldsMap = {},
> implements SchemaAwareQueryModifierBuilder<S, TableName, RelatedFields> {
  private options: QueryOptions<TableModel<GetTable<S, TableName>>, boolean> = {};

  constructor(
    private readonly tableName: TableName,
    private readonly schema: S
  ) {}

  where(conditions: WhereInput<TableModel<GetTable<S, TableName>>>): this {
    assertNotOpaque(
      this.schema,
      this.tableName,
      'where',
      whereFieldNames(conditions as Record<string, unknown>)
    );
    this.options.where = { ...this.options.where, ...conditions };
    return this;
  }

  select(...fields: ((keyof TableModel<GetTable<S, TableName>> & string) | '*')[]): this {
    if (this.options.select) {
      throw new Error('Select can only be called once per query');
    }
    this.options.select = fields;
    return this;
  }

  limit(count: number): this {
    this.options.limit = count;
    return this;
  }

  offset(count: number): this {
    this.options.offset = count;
    return this;
  }

  orderBy(
    field: keyof TableModel<GetTable<S, TableName>> & string,
    direction: 'asc' | 'desc' = 'asc'
  ): this {
    assertNotOpaque(this.schema, this.tableName, 'orderBy', [field]);
    this.options.orderBy = {
      ...this.options.orderBy,
      [field]: direction,
    } as Partial<Record<keyof TableModel<GetTable<S, TableName>>, 'asc' | 'desc'>>;
    return this;
  }

  // Schema-aware implementation for nested relationships with full type inference
  related<
    Field extends TableRelationships<S, TableName>['field'],
    Rel extends GetRelationship<S, TableName, Field>,
    RelatedFields2 extends RelatedFieldsMap = {},
  >(
    relatedField: Field,
    modifier?: SchemaAwareQueryModifier<S, Rel['to'], RelatedFields2>
  ): SchemaAwareQueryModifierBuilderImpl<
    S,
    TableName,
    RelatedFields & {
      [K in Field]: {
        to: Rel['to'];
        cardinality: Rel['cardinality'];
        relatedFields: RelatedFields2;
      };
    }
  > {
    if (!this.options.related) {
      this.options.related = [];
    }

    const exists = this.options.related.some((r) => (r.alias || r.relatedTable) === relatedField);

    if (!exists) {
      // Look up the relationship from schema
      const relationship = this.schema.relationships.find(
        (r) => r.from === this.tableName && r.field === relatedField
      );

      if (!relationship) {
        // No such relationship in the client schema — e.g. a table owned by a
        // devOnly backend (the outbox `job`) that a free/Cloudflare deployment
        // never provisions, so codegen omits it + its relationships. Skip the
        // projection instead of throwing, which would take the whole query
        // (and its other `.related()` siblings — author, comments) down.
        // Mirrors the server's "unpermitted subquery → empty" degradation.
        if (typeof console !== 'undefined') {
          console.warn(
            `[sp00ky] .related('${String(relatedField)}') skipped — no such relationship on '${this.tableName}' in the client schema`
          );
        }
        return this as any;
      }

      const relatedTable = relationship.to;
      const cardinality = relationship.cardinality;
      const foreignKeyField = cardinality === 'many' ? this.tableName : relatedField;

      this.options.related.push({
        relatedTable,
        alias: relatedField as string,
        modifier: modifier as QueryModifier<GenericModel>,
        cardinality,
        foreignKeyField: foreignKeyField as string,
      } as RelatedQuery & { foreignKeyField: string });
    }
    return this as any;
  }

  _getOptions(): QueryOptions<TableModel<GetTable<S, TableName>>, boolean> {
    return this.options;
  }
}

/**
 * Fluent query builder for constructing queries with chainable methods
 * Now with full type inference from schema constant AND related field accumulation!
 */
export class QueryBuilder<
  const S extends SchemaStructure,
  const TableName extends TableNames<S>,
  const R = void,
  const RelatedFields extends RelatedFieldsMap = {},
  const IsOne extends boolean = false,
> {
  constructor(
    private readonly schema: S,
    private readonly tableName: TableName,
    private readonly executer: Executor<GetTable<S, TableName>, R> = () => undefined as R,
    private options: QueryOptions<TableModel<GetTable<S, TableName>>, IsOne> = {}
  ) {}

  /**
   * Add additional where conditions
   */
  where(
    conditions: WhereInput<TableModel<GetTable<S, TableName>>>
  ): QueryBuilder<S, TableName, R, RelatedFields, IsOne> {
    assertNotOpaque(
      this.schema,
      this.tableName,
      'where',
      whereFieldNames(conditions as Record<string, unknown>)
    );
    this.options.where = { ...this.options.where, ...conditions };
    return this;
  }

  /**
   * Specify fields to select
   */
  select(
    ...fields: ((keyof TableModel<GetTable<S, TableName>> & string) | '*')[]
  ): QueryBuilder<S, TableName, R, RelatedFields, IsOne> {
    if (this.options.select) {
      throw new Error('Select can only be called once per query');
    }
    this.options.select = fields;
    return this;
  }

  /**
   * Add ordering to the query (only for non-live queries)
   */
  orderBy(
    field: TableFieldNames<GetTable<S, TableName>>,
    direction: 'asc' | 'desc' = 'asc'
  ): QueryBuilder<S, TableName, R, RelatedFields, IsOne> {
    assertNotOpaque(this.schema, this.tableName, 'orderBy', [field as string]);
    this.options.orderBy = {
      ...this.options.orderBy,
      [field]: direction,
    } as Partial<Record<keyof TableModel<GetTable<S, TableName>>, 'asc' | 'desc'>>;
    return this;
  }

  /**
   * Add limit to the query (only for non-live queries)
   */
  limit(count: number): QueryBuilder<S, TableName, R, RelatedFields, IsOne> {
    this.options.limit = count;
    return this;
  }

  /**
   * Add offset to the query (only for non-live queries)
   */
  offset(count: number): QueryBuilder<S, TableName, R, RelatedFields, IsOne> {
    this.options.offset = count;
    return this;
  }

  /**
   * Say that parts of this query are ignored where it is used, on purpose.
   * A channel (`useChannel(query, peers)`) only reads the `where`, so
   * `orderBy`, `limit`, `offset`, `select` and `.one()` do nothing there and
   * draw a warning; this silences it while showing the reader the query
   * carries arguments that do not apply (handy when one query object serves
   * both a list and a channel).
   */
  ignoreUnusedArgs(): QueryBuilder<S, TableName, R, RelatedFields, IsOne> {
    this.options.ignoreUnusedArgs = true;
    return this;
  }

  one(): QueryBuilder<S, TableName, R, RelatedFields, true> {
    return new QueryBuilder<S, TableName, R, RelatedFields, true>(
      this.schema,
      this.tableName,
      this.executer,
      { ...this.options, isOne: true }
    );
  }

  /**
   * Include related data via subqueries
   * Field and cardinality are validated against schema relationships
   * Now accumulates the related field in the type!
   */
  related<
    Field extends TableRelationships<S, TableName>['field'],
    Rel extends GetRelationship<S, TableName, Field>,
    RelatedFields2 extends RelatedFieldsMap = {},
  >(
    field: Field,
    modifierOrCardinality?:
      | SchemaAwareQueryModifier<S, Rel['to'], RelatedFields2>
      | Rel['cardinality'],
    modifier?: SchemaAwareQueryModifier<S, Rel['to'], RelatedFields2>
  ): QueryBuilder<
    S,
    TableName,
    R,
    RelatedFields & {
      [K in Field]: {
        to: Rel['to'];
        cardinality: Rel['cardinality'];
        relatedFields: RelatedFields2;
      };
    },
    IsOne
  > {
    if (!this.options.related) {
      this.options.related = [];
    }

    // Check if field already exists
    const exists = this.options.related.some((r) => (r.alias || r.relatedTable) === field);

    if (exists) {
      return this as any;
    }

    // Look up relationship metadata from schema
    const relationship = this.schema.relationships.find(
      (r) => r.from === this.tableName && r.field === field
    );

    if (!relationship) {
      // See the note on the other `.related()` overload: skip an unknown
      // relationship (warn) rather than throwing, so a table absent from the
      // client schema (e.g. the free-plan `job` outbox) can't crash the query.
      if (typeof console !== 'undefined') {
        console.warn(
          `[sp00ky] .related('${String(field)}') skipped — no such relationship on '${this.tableName}' in the client schema`
        );
      }
      return this as any;
    }

    // Determine cardinality and modifier based on arguments
    let actualCardinality: 'one' | 'many';
    let actualModifier: SchemaAwareQueryModifier<S, Rel['to']> | undefined;

    if (typeof modifierOrCardinality === 'function') {
      // Signature: related(field, modifier)
      actualCardinality = relationship.cardinality;
      actualModifier = modifierOrCardinality;
    } else if (modifierOrCardinality === 'one' || modifierOrCardinality === 'many') {
      // Signature: related(field, cardinality, modifier)
      actualCardinality = modifierOrCardinality;
      actualModifier = modifier;
    } else {
      // Signature: related(field)
      actualCardinality = relationship.cardinality;
      actualModifier = undefined;
    }

    // Determine foreign key field based on cardinality
    let foreignKeyField: string =
      actualCardinality === 'many' ? (this.tableName as string) : (field as string);

    if (actualCardinality === 'many') {
      // For one-to-many, we need to find the field on the child table that points back to the parent
      // We look for a relationship from Child -> Parent
      const reverseRelationships = this.schema.relationships.filter(
        (r) => r.from === relationship.to && r.to === this.tableName && r.cardinality === 'one'
      );

      if (reverseRelationships.length > 0) {
        // Prioritize field that matches parent table name
        const exactMatch = reverseRelationships.find((r) => r.field === this.tableName);
        if (exactMatch) {
          foreignKeyField = exactMatch.field;
        } else {
          foreignKeyField = reverseRelationships[0].field;
        }
      } else {
        // Fallback heuristics
        if (this.tableName.startsWith(`${relationship.to}_`)) {
          // If parent table is "game_database" and child is "game", try "database"
          foreignKeyField = this.tableName.slice(relationship.to.length + 1);
        }
      }
    }

    // Cast the schema-aware modifier to the runtime type
    // At runtime, QueryModifierBuilderImpl will work correctly with the schema
    const wrappedModifier = actualModifier as QueryModifier<GenericModel> | undefined;

    this.options.related.push({
      relatedTable: relationship.to,
      alias: field as string,
      modifier: wrappedModifier,
      cardinality: actualCardinality,
      foreignKeyField: foreignKeyField as any,
    } as RelatedQuery & { foreignKeyField: string });

    return this as any;
  }

  /**
   * Get the current query options
   */
  getOptions(): QueryOptions<TableModel<GetTable<S, TableName>>, IsOne> {
    return this.options;
  }

  /**
   * Build query methods for SELECT and LIVE SELECT (custom implementation)
   * @returns FinalQuery object with select() method for custom usage
   */
  build(): FinalQuery<S, TableName, GetTable<S, TableName>, RelatedFields, IsOne, R> {
    return new FinalQuery<S, TableName, GetTable<S, TableName>, RelatedFields, IsOne, R>(
      this.tableName,
      this.options,
      this.schema,
      this.executer
    );
  }
}

/**
 * The surql param name an `_or` branch condition binds under: the branch FIELD,
 * a `__or` marker and the branch's position. The position alone makes it unique
 * within the query; the field prefix is what lets a consumer type the value by
 * looking the column up (see {@link baseFieldOfParam}). Non-identifier
 * characters (a nested path like `database.owner`) are folded to `_` so the
 * result is a legal param name.
 */
export function orParamName(field: string, index: number): string {
  return `${field.replace(/[^A-Za-z0-9_]/g, '_')}__or${index}`;
}

/**
 * Inverse of {@link orParamName}: the field a param name was built from, or the
 * name itself when it is a plain top-level param (`field = $field`). Lets a
 * consumer resolve `white__or0` back to the `white` column.
 */
export function baseFieldOfParam(name: string): string {
  const m = /^(.+)__or\d+$/.exec(name);
  return m ? m[1] : name;
}

export function cyrb53(str: string, seed: number = 0): number {
  let h1 = 0xdeadbeef ^ seed,
    h2 = 0x41c6ce57 ^ seed;
  for (let i = 0, ch; i < str.length; i++) {
    ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
  h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
  h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);

  return 4294967296 * (2097151 & h2) + (h1 >>> 0);
}

export function extractSubqueryQueryInfos<S extends SchemaStructure>(
  schema: S,
  parentTableName: string,
  options: QueryOptions<GenericModel, boolean>,
  executer: Executor<{ columns: Record<string, ColumnSchema> }>
): InnerQuery<{ columns: Record<string, ColumnSchema> }, boolean>[] {
  if (!options.related) {
    return [];
  }

  return options.related.map((rel) => {
    // Get base options from modifier
    const subOptions =
      rel
        .modifier?.(new SchemaAwareQueryModifierBuilderImpl(rel.relatedTable, schema))
        ._getOptions() ?? {};

    // Find relationship to determine how to filter
    const relationship = schema.relationships.find(
      (r) => r.from === parentTableName && r.field === rel.alias
    );

    if (relationship) {
      // Determine foreign key field
      // rel.alias is guaranteed to be defined if relationship is found (matched r.field)
      // oxlint-disable-next-line no-non-null-assertion -- alias is guaranteed defined when relationship is found
      let foreignKeyField = rel.alias!;

      if (relationship.cardinality === 'many') {
        // For one-to-many, we need to find the field on the child table that points back to the parent
        // We look for a relationship from Child -> Parent
        const reverseRelationships = schema.relationships.filter(
          (r) => r.from === rel.relatedTable && r.to === parentTableName && r.cardinality === 'one'
        );

        if (reverseRelationships.length > 0) {
          // Prioritize field that matches parent table name
          const exactMatch = reverseRelationships.find((r) => r.field === parentTableName);
          if (exactMatch) {
            foreignKeyField = exactMatch.field;
          } else {
            foreignKeyField = reverseRelationships[0].field;
          }
        } else {
          // Fallback heuristics
          if (parentTableName.startsWith(`${rel.relatedTable}_`)) {
            // If parent table is "game_database" and child is "game", try "database"
            foreignKeyField = parentTableName.slice(rel.relatedTable.length + 1);
          } else {
            // Default to parent table name
            foreignKeyField = parentTableName;
          }
        }
      }

      // Add parent filter to where clause
      subOptions.where = subOptions.where || {};

      if (relationship.cardinality === 'many') {
        // One-to-Many: Child has foreign key to parent
        // WHERE $parentIds ∋ child.parent_id
        (subOptions.where as any)[foreignKeyField] = { _op: '∋', _val: '$parentIds', _swap: true };
      } else {
        // One-to-One: Parent has foreign key to child
        // WHERE $parent_<foreignKeyField> ∋ child.id
        // We use a dynamic variable name derived from the foreign key field on the parent
        (subOptions.where as any).id = {
          _op: '∋',
          _val: `$parent_${foreignKeyField}`,
          _swap: true,
        };
      }
    }

    return new InnerQuery(rel.relatedTable, subOptions, schema, executer);
  });
}

/**
 * Build a query string from query options
 * @param method - The query method (SELECT or LIVE SELECT)
 * @param tableName - The table name to query
 * @param options - The query options (where, select, orderBy, etc.)
 * @param schema - Optional schema for resolving nested relationships
 * @returns QueryInfo with the generated SQL and variables
 */
export function buildQueryFromOptions<TModel extends GenericModel, IsOne extends boolean>(
  method: 'SELECT' | 'LIVE SELECT' | 'LIVE SELECT DIFF' | 'UPDATE' | 'DELETE',
  tableName: string,
  options: QueryOptions<TModel, IsOne>,
  schema: SchemaStructure,
  patches?: any[]
): QueryInfo {
  if (options.isOne) {
    options.limit = 1;
  }
  const isLiveQuery = method === 'LIVE SELECT' || method === 'LIVE SELECT DIFF';

  // Parse where conditions to convert string IDs to RecordId
  const parsedWhere = options.where
    ? parseObjectIdsToRecordId(options.where, tableName)
    : undefined;

  // Build SELECT clause
  let selectClause = '*';

  if (method === 'LIVE SELECT DIFF') {
    selectClause = '';
  } else {
    if (options.select && options.select.length > 0) {
      selectClause = options.select.join(', ');
    }
  }

  // Build related subqueries (fetch clauses)
  let fetchClauses = '';
  if (!isLiveQuery && options.related && options.related.length > 0) {
    const subqueries = options.related.map((rel) => buildSubquery(rel, schema));
    fetchClauses = ', ' + subqueries.join(', ');
  }

  // Start building the query
  let query = '';

  if (method === 'UPDATE') {
    query = `UPDATE ${tableName}`;
  } else if (method === 'DELETE') {
    query = `DELETE FROM ${tableName}`;
  } else {
    query = `${method}${selectClause ? ` ${selectClause}` : ''}${fetchClauses} FROM ${tableName}`;
  }

  // Build WHERE clause
  const vars: Record<string, unknown> = {};
  if (parsedWhere && Object.keys(parsedWhere).length > 0) {
    const conditions: string[] = [];

    // Build a single condition for `field`, binding its value under `varName`.
    // Supports operator objects `{ _op, _val, _swap }` (e.g. `{ _op: '<=', _val:
    // 5 }`); a `$`-prefixed string `_val` references an existing param verbatim.
    // Plain values mean equality (`field = $varName`).
    const buildCondition = (field: string, value: unknown, varName: string): string => {
      if (value && typeof value === 'object' && '_op' in value && '_val' in value) {
        const { _op, _val, _swap } = value as { _op: string; _val: unknown; _swap?: boolean };
        let rightSide: string;
        if (typeof _val === 'string' && _val.startsWith('$')) {
          rightSide = _val;
        } else {
          vars[varName] = _val;
          rightSide = `$${varName}`;
        }
        return _swap ? `${rightSide} ${_op} ${field}` : `${field} ${_op} ${rightSide}`;
      }
      vars[varName] = value;
      return `${field} = $${varName}`;
    };

    for (const [key, value] of Object.entries(parsedWhere)) {
      // OR-group: `{ _or: [ {field: val}, {field: {_op,_val}}, ... ] }` compiles
      // to one parenthesised `(c1 OR c2 ...)` conjunct. Each branch condition gets
      // a unique, position-indexed param name (`white__or0`, `black__or1`, …) so
      // it never collides with a top-level condition on the same field (e.g. a
      // `white = $white` filter alongside an opponent `_or` on white/black) -
      // keeping the surql + vars, and thus the query hash, stable and
      // deterministic. The FIELD is part of the name on purpose: the consumer
      // types a param by looking its name up in the table's columns
      // (`parseQueryParams` in @spooky-sync/core), and a name it cannot resolve
      // used to be dropped from the registration, which left `$or0` unbound and
      // every `_or` query matching nothing.
      if (key === '_or' && Array.isArray(value)) {
        const orParts: string[] = [];
        let i = 0;
        for (const branch of value) {
          if (branch && typeof branch === 'object') {
            for (const [bField, bVal] of Object.entries(branch as Record<string, unknown>)) {
              orParts.push(buildCondition(bField, bVal, orParamName(bField, i++)));
            }
          }
        }
        if (orParts.length > 0) conditions.push(`(${orParts.join(' OR ')})`);
        continue;
      }

      conditions.push(buildCondition(key, value, key));
    }

    if (conditions.length > 0) query += ` WHERE ${conditions.join(' AND ')}`;
  }

  // Add PATCH for UPDATE
  if (method === 'UPDATE' && patches) {
    query += ` PATCH ${JSON.stringify(patches)}`;
  }

  // Add ORDER BY, LIMIT, START only for non-live queries and non-update/delete queries (unless supported)
  // SurrealDB UPDATE/DELETE supports WHERE, but LIMIT/START/ORDER BY might be restricted or behave differently.
  // For now, let's allow them if they are set, as SurrealDB supports them for DELETE/UPDATE.
  if (!isLiveQuery) {
    if (options.orderBy && Object.keys(options.orderBy).length > 0) {
      const orderClauses = Object.entries(options.orderBy).map(
        ([field, direction]) => `${field} ${direction}`
      );
      query += ` ORDER BY ${orderClauses.join(', ')}`;
    }

    if (options.limit !== undefined) {
      query += ` LIMIT ${options.limit}`;
    }

    if (options.offset !== undefined) {
      query += ` START ${options.offset}`;
    }
  }

  query += ';';

  return {
    query,
    hash: cyrb53(
      `${query}::${Object.entries(vars)
        .map(([key, value]) => `${key}=${value}`)
        .join('&')}`,
      0
    ),
    vars: Object.keys(vars).length > 0 ? vars : undefined,
    // Engine-neutral plan mirrors the SELECT above for non-SurrealQL backends.
    // Only SELECT carries a plan; the isOne→limit=1 mutation above is already
    // reflected in `options.limit`, so the plan sees it too.
    plan: method === 'SELECT' ? buildQueryPlan(tableName, options, schema) : undefined,
  };
}

/**
 * Build the engine-neutral {@link QueryPlan} for a SELECT. Mirrors the string
 * assembly in {@link buildQueryFromOptions} / {@link buildSubquery} exactly so a
 * non-SurrealQL backend produces results identical to the SurrealQL path.
 */
function buildQueryPlan<TModel extends GenericModel, IsOne extends boolean>(
  tableName: string,
  options: QueryOptions<TModel, IsOne>,
  schema: SchemaStructure
): QueryPlan {
  const plan: QueryPlan = { table: tableName };

  if (options.select && options.select.length > 0 && !options.select.includes('*')) {
    plan.select = options.select.filter((f) => f !== '*') as string[];
  }

  const parsedWhere = options.where
    ? (parseObjectIdsToRecordId(options.where, tableName) as Record<string, unknown>)
    : undefined;
  if (parsedWhere && Object.keys(parsedWhere).length > 0) {
    // slaveToParams: top-level filters materialize from `params` (the query's
    // identity), not a baked literal — see buildWhereNodes. Prevents a query's
    // rows ever coming from a different query's plan.
    const nodes = buildWhereNodes(parsedWhere, true);
    if (nodes.length > 0) plan.where = nodes;
  }

  if (options.orderBy && Object.keys(options.orderBy).length > 0) {
    plan.orderBy = Object.entries(options.orderBy).map(
      ([field, direction]) => [field, direction as 'asc' | 'desc']
    );
  }

  if (options.limit !== undefined) plan.limit = options.limit;
  if (options.offset !== undefined) plan.offset = options.offset;

  if (options.related && options.related.length > 0) {
    plan.relations = options.related.map((rel) => buildRelationPlan(rel, schema));
  }

  return plan;
}

/**
 * Engine-neutral counterpart of {@link buildSubquery}. Resolves the same
 * cardinality / foreign-key / nested-relation metadata but returns a structured
 * {@link RelationPlan} instead of a SurrealQL subquery string.
 */
function buildRelationPlan(
  rel: RelatedQuery & { foreignKeyField?: string },
  schema: SchemaStructure
): RelationPlan {
  const { relatedTable, alias, modifier, cardinality } = rel;
  // Same fallback chain as buildSubquery (`rel.foreignKeyField || alias`); the
  // top-level foreignKeyField is already reverse-resolved by `.related()`.
  const foreignKeyField = (rel.foreignKeyField || alias || relatedTable) as string;

  const plan: RelationPlan = {
    alias: (alias || relatedTable) as string,
    table: relatedTable,
    cardinality,
    foreignKeyField,
  };

  if (modifier) {
    const modifierBuilder = new SchemaAwareQueryModifierBuilderImpl(relatedTable, schema);
    modifier(modifierBuilder as any);
    const subOptions = modifierBuilder._getOptions();

    if (subOptions.select && subOptions.select.length > 0 && !subOptions.select.includes('*')) {
      plan.select = subOptions.select.filter((f) => f !== '*') as string[];
    }

    if (subOptions.where && Object.keys(subOptions.where).length > 0) {
      const parsedSubWhere = parseObjectIdsToRecordId(subOptions.where, relatedTable) as Record<
        string,
        unknown
      >;
      const nodes = buildWhereNodes(parsedSubWhere);
      if (nodes.length > 0) plan.where = nodes;
    }

    if (subOptions.orderBy && Object.keys(subOptions.orderBy).length > 0) {
      plan.orderBy = Object.entries(subOptions.orderBy).map(
        ([field, direction]) => [field, direction as 'asc' | 'desc']
      );
    }

    if (subOptions.limit !== undefined) plan.limit = subOptions.limit;

    // Nested relations: re-resolve exactly as buildSubquery does — the child's
    // foreignKeyField comes from the relatedTable-based lookup, not the reverse
    // heuristic used for top-level relations.
    if (subOptions.related && subOptions.related.length > 0) {
      const resolvedNestedRels = subOptions.related.map((nestedRel) => {
        const relationship = schema.relationships.find(
          (r) => r.from === relatedTable && r.field === nestedRel.alias
        );
        if (relationship) {
          const nestedForeignKeyField =
            relationship.cardinality === 'many' ? relatedTable : (nestedRel.alias as string);
          return {
            ...nestedRel,
            relatedTable: relationship.to,
            cardinality: relationship.cardinality,
            foreignKeyField: nestedForeignKeyField,
          } as RelatedQuery & { foreignKeyField: string };
        }
        return nestedRel;
      });
      plan.relations = resolvedNestedRels.map((nestedRel) => buildRelationPlan(nestedRel, schema));
    }
  }

  // one-to-one gets an implicit per-parent LIMIT 1 (matches buildSubquery).
  if (cardinality === 'one' && plan.limit === undefined) {
    plan.limit = 1;
  }

  return plan;
}

/**
 * Convert a parsed WHERE object (string IDs already → RecordId) into the
 * engine-neutral {@link WhereNode}[] conjunction. Mirrors the `_or` / comparison
 * / equality handling in {@link buildQueryFromOptions}. A `$`-prefixed `_val`
 * becomes a `paramRef` (with the leading `$` stripped).
 */
/**
 * @param slaveToParams When true, top-level plain/operator-literal comparisons
 *   ALSO carry a `paramRef` equal to the field name — the same var name
 *   `buildQueryFromOptions` binds the value under (`field = $field`). The
 *   engines then materialize by reading `params[field]` (falling back to the
 *   baked `value` when the param is absent), so a query's rows are slaved to
 *   its `params` (its identity) and can never come from a different query's
 *   baked plan. Only safe at the TOP LEVEL, where the field is a schema column
 *   that survives `parseQueryParams` and the caller passes `params`. NOT used
 *   for relation sub-wheres (rendered with a params-less ctx) or `_or` branches
 *   (bound under synthetic `white__or0` names) - those stay baked.
 */
function buildWhereNodes(
  parsedWhere: Record<string, unknown>,
  slaveToParams = false
): WhereNode[] {
  const toComparison = (field: string, value: unknown, slave: boolean): WhereComparison => {
    if (value && typeof value === 'object' && '_op' in value && '_val' in value) {
      const { _op, _val, _swap } = value as ComparisonOp;
      if (typeof _val === 'string' && _val.startsWith('$')) {
        return { field, op: _op, value: undefined, paramRef: _val.slice(1), swap: _swap };
      }
      // Literal operand: keep `value` as a fallback and add `paramRef: field`
      // (slave mode) so materialization reads the query's own `params[field]`.
      return slave
        ? { field, op: _op, value: _val, paramRef: field, swap: _swap }
        : { field, op: _op, value: _val, swap: _swap };
    }
    return slave ? { field, op: '=', value, paramRef: field } : { field, op: '=', value };
  };

  const nodes: WhereNode[] = [];
  for (const [key, value] of Object.entries(parsedWhere)) {
    if (key === '_or' && Array.isArray(value)) {
      const or: WhereComparison[] = [];
      for (const branch of value) {
        if (branch && typeof branch === 'object') {
          for (const [bField, bVal] of Object.entries(branch as Record<string, unknown>)) {
            // OR branches bind under synthetic `white__or0` names (see
            // orParamName / buildQueryFromOptions); the plan keeps them baked.
            or.push(toComparison(bField, bVal, false));
          }
        }
      }
      if (or.length > 0) nodes.push({ or });
      continue;
    }
    nodes.push(toComparison(key, value, slaveToParams));
  }
  return nodes;
}

/**
 * Build a subquery for a related field
 */
function buildSubquery(
  rel: RelatedQuery & { foreignKeyField?: string },
  schema: SchemaStructure
): string {
  const { relatedTable, alias, modifier, cardinality } = rel;
  const foreignKeyField = rel.foreignKeyField || alias;

  let subquerySelect = '*';
  let subqueryWhere = '';
  let subqueryOrderBy = '';
  let subqueryLimit = '';

  // If there's a modifier, apply it to get the sub-options
  if (modifier) {
    const modifierBuilder = new SchemaAwareQueryModifierBuilderImpl(relatedTable, schema);
    modifier(modifierBuilder);
    const subOptions = modifierBuilder._getOptions();

    // Build sub-select
    if (subOptions.select && subOptions.select.length > 0) {
      subquerySelect = subOptions.select.join(', ');
    }

    // Build sub-where
    if (subOptions.where && Object.keys(subOptions.where).length > 0) {
      const parsedSubWhere = parseObjectIdsToRecordId(subOptions.where, relatedTable) as Record<
        string,
        unknown
      >;
      const conditions = Object.entries(parsedSubWhere).map(([key, value]) => {
        if (value instanceof RecordId) {
          return `${key} = ${value.toString()}`;
        }
        return `${key} = ${JSON.stringify(value)}`;
      });
      subqueryWhere = ` AND ${conditions.join(' AND ')}`;
    }

    // Build sub-orderBy
    if (subOptions.orderBy && Object.keys(subOptions.orderBy).length > 0) {
      const orderClauses = Object.entries(subOptions.orderBy).map(
        ([field, direction]) => `${field} ${direction}`
      );
      subqueryOrderBy = ` ORDER BY ${orderClauses.join(', ')}`;
    }

    // Build sub-limit
    if (subOptions.limit !== undefined) {
      subqueryLimit = ` LIMIT ${subOptions.limit}`;
    }

    // Handle nested relationships
    if (subOptions.related && subOptions.related.length > 0) {
      // Resolve nested relationship metadata if schema is available
      const resolvedNestedRels = subOptions.related.map((nestedRel) => {
        if (schema) {
          // Look up the actual relationship metadata from schema
          const relationship = schema.relationships.find(
            (r) => r.from === relatedTable && r.field === nestedRel.alias
          );

          if (relationship) {
            // Use the resolved table name and add foreign key field
            const nestedForeignKeyField =
              relationship.cardinality === 'many' ? relatedTable : nestedRel.alias;

            return {
              ...nestedRel,
              relatedTable: relationship.to,
              cardinality: relationship.cardinality,
              foreignKeyField: nestedForeignKeyField,
            } as RelatedQuery & { foreignKeyField: string };
          }
        }
        return nestedRel;
      });

      const nestedSubqueries = resolvedNestedRels.map((nestedRel) =>
        buildSubquery(nestedRel, schema)
      );
      subquerySelect += ', ' + nestedSubqueries.join(', ');
    }
  }

  // Determine the WHERE condition based on cardinality
  let whereCondition: string;
  if (cardinality === 'one') {
    // For one-to-one, the related table's id matches parent's foreign key field
    whereCondition = `WHERE id=$parent.${foreignKeyField}`;
    // Add LIMIT 1 for one-to-one relationships if not already set
    if (!subqueryLimit) {
      subqueryLimit = ' LIMIT 1';
    }
  } else {
    // For one-to-many, the related table has a foreign key field pointing to parent's id
    whereCondition = `WHERE ${foreignKeyField}=$parent.id`;
  }

  // Build the complete subquery
  let subquery = `(SELECT ${subquerySelect} FROM ${relatedTable} ${whereCondition}${subqueryWhere}${subqueryOrderBy}${subqueryLimit})`;

  // For one-to-one relationships, select the first element
  if (cardinality === 'one') {
    subquery += '[0]';
  }

  subquery += ` AS ${alias}`;

  return subquery;
}
