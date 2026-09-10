import type {
  ColumnSchema,
  FinalQuery,
  SchemaStructure,
  TableNames,
} from '@spooky-sync/query-builder';
import { createEffect } from 'solid-js';
import { SyncedDb } from '..';
import type {
  Sp00kyQueryResultPromise,
  PreloadOptions as CorePreloadOptions,
} from '@spooky-sync/core';
import { useDb } from './context';

type PreloadArg<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
> =
  | FinalQuery<S, TableName, T, RelatedFields, IsOne, Sp00kyQueryResultPromise>
  | (() =>
      | FinalQuery<S, TableName, T, RelatedFields, IsOne, Sp00kyQueryResultPromise>
      | null
      | undefined);

type PreloadOptions = CorePreloadOptions & {
  /** Only preload while this returns true (defaults to always). */
  enabled?: () => boolean;
};

// Overload: context-based (no explicit db)
export function createPreload<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
>(
  finalQuery: PreloadArg<S, TableName, T, RelatedFields, IsOne>,
  options?: PreloadOptions
): void;

// Overload: explicit db
export function createPreload<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
>(
  db: SyncedDb<S>,
  finalQuery: PreloadArg<S, TableName, T, RelatedFields, IsOne>,
  options?: PreloadOptions
): void;

/**
 * Reactive, fire-and-forget prewarm. Resolves the query (calling it if it's a
 * function so it tracks reactive deps), dedupes on the query's stable identity
 * hash, and registers it via `db.preload`: a live query nobody subscribes to,
 * evicted a ttl after it was registered unless a view mounts it first. Nothing
 * to clean up here.
 *
 * Typical use: inside a list row, preload the detail query the user is likely
 * to open next, so navigation paints from cache instead of the network.
 */
export function createPreload<
  S extends SchemaStructure,
  TableName extends TableNames<S>,
  T extends { columns: Record<string, ColumnSchema> },
  RelatedFields extends Record<string, any>,
  IsOne extends boolean,
>(
  dbOrQuery: SyncedDb<S> | PreloadArg<S, TableName, T, RelatedFields, IsOne>,
  queryOrOptions?: PreloadArg<S, TableName, T, RelatedFields, IsOne> | PreloadOptions,
  maybeOptions?: PreloadOptions
): void {
  let db: SyncedDb<S>;
  let finalQuery: PreloadArg<S, TableName, T, RelatedFields, IsOne>;
  let options: PreloadOptions | undefined;

  if (dbOrQuery instanceof SyncedDb) {
    db = dbOrQuery;
    finalQuery = queryOrOptions as PreloadArg<S, TableName, T, RelatedFields, IsOne>;
    options = maybeOptions;
  } else {
    db = useDb<S>();
    finalQuery = dbOrQuery;
    options = queryOrOptions as PreloadOptions | undefined;
  }

  let prevHash: number | undefined;

  // Two-arg Solid 2 effect: compute resolves the query (tracking its reactive
  // deps) and dedupes on the identity hash; the untracked apply fires the
  // preload. Returning `undefined` from compute skips nothing — apply guards.
  createEffect(
    () => {
      if (!(options?.enabled?.() ?? true)) return undefined;
      const query = typeof finalQuery === 'function' ? finalQuery() : finalQuery;
      if (!query) return undefined;
      // Dedupe on the query's stable identity hash so a reactive re-run with
      // an unchanged query doesn't refetch (the core also dedupes per session).
      if (query.hash === prevHash) return undefined;
      prevHash = query.hash;
      return query;
    },
    (query) => {
      if (!query) return;
      void db
        .getSp00ky()
        .preload(query, { signal: options?.signal });
    }
  );
}
