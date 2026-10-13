import type { MutationEventType } from '../types';

// ==================== TYPES ====================

export interface TxQuery {
  readonly __brand: 'TxQuery';
  readonly sql: string;
  readonly statementCount: number;
}

export interface SealOptions {
  /** 0-based index of the inner statement to extract. Default: last statement. */
  resultIndex?: number;
}

export interface SealedQuery<T = void> {
  readonly sql: string;
  readonly extract: (results: unknown[]) => T;
}

export interface SurqlHelper {
  seal(query: string): string;
  seal<T = void>(query: TxQuery, options?: SealOptions): SealedQuery<T>;
  tx(queries: string[]): TxQuery;
  selectById(idVar: string, returnValues: string[]): string;
  selectByFieldsAnd(
    table: string,
    whereVar: ({ field: string; variable: string } | string)[],
    returnValues: ({ field: string; alias: string } | string)[]
  ): string;
  create(idVar: string, dataVar: string): string;
  createSet(
    idVar: string,
    keyDataVars: ({ key: string; variable: string } | { statement: string } | string)[]
  ): string;
  upsert(idVar: string, dataVar: string): string;
  upsertMerge(idVar: string, dataVar: string): string;
  updateMerge(idVar: string, dataVar: string): string;
  updateSet(
    idVar: string,
    keyDataVar: ({ key: string; variable: string } | { statement: string } | string)[]
  ): string;
  delete(idVar: string): string;
  let(name: string, query: string): string;
  createMutation(
    t: MutationEventType,
    mutationIdVar: string,
    recordIdVar: string,
    dataVar?: string,
    beforeRecordVar?: string
  ): string;
  returnObject(entries: { key: string; variable: string }[]): string;
}

// ==================== IMPLEMENTATION ====================

export const surql: SurqlHelper = {
  seal(query: string | TxQuery, options?: SealOptions): any {
    if (typeof query === 'string') {
      return `${query};`;
    }

    // TxQuery path
    const txQuery = query;
    const idx = options?.resultIndex ?? txQuery.statementCount - 1;
    const sql = `${txQuery.sql};`;
    return {
      sql,
      extract(results: unknown[]): unknown {
        // +1 to skip the BEGIN null at index 0
        return results[idx + 1];
      },
    } satisfies SealedQuery<unknown>;
  },

  tx(queries: string[]): TxQuery {
    return {
      __brand: 'TxQuery' as const,
      sql: `BEGIN TRANSACTION;\n${queries.join(';')};\nCOMMIT TRANSACTION`,
      statementCount: queries.length,
    };
  },

  selectById(idVar: string, returnValues: string[]) {
    return `SELECT ${returnValues.join(',')} FROM ONLY $${idVar}`;
  },

  selectByFieldsAnd(
    table: string,
    whereVar: ({ field: string; variable: string } | string)[],
    returnValues: ({ field: string; alias: string } | string)[]
  ) {
    return `SELECT ${returnValues
      .map((rv) => (typeof rv === 'string' ? rv : `${rv.field} as ${rv.alias}`))
      .join(',')} FROM ${table} WHERE ${whereVar
      .map((wv) => (typeof wv === 'string' ? `${wv} = $${wv}` : `${wv.field} = $${wv.variable}`))
      .join(' AND ')}`;
  },

  create(idVar: string, dataVar: string) {
    return `CREATE ONLY $${idVar} CONTENT $${dataVar}`;
  },

  createSet(
    idVar: string,
    keyDataVars: ({ key: string; variable: string } | { statement: string } | string)[]
  ) {
    return `CREATE ONLY $${idVar} SET ${keyDataVars
      .map((keyDataVar) =>
        typeof keyDataVar === 'string'
          ? `${keyDataVar} = $${keyDataVar}`
          : 'statement' in keyDataVar
            ? keyDataVar.statement
            : `${keyDataVar.key} = $${keyDataVar.variable}`
      )
      .join(', ')}`;
  },

  upsert(idVar: string, dataVar: string) {
    return `UPSERT ONLY $${idVar} REPLACE $${dataVar}`;
  },

  // Use this for sync-down ingestion: the remote payload can omit local-only
  // fields injected by the CLI's local-schema setup, and REPLACE would wipe
  // them on every round-trip.
  upsertMerge(idVar: string, dataVar: string) {
    return `UPSERT ONLY $${idVar} MERGE $${dataVar}`;
  },

  updateMerge(idVar: string, dataVar: string) {
    return `UPDATE ONLY $${idVar} MERGE $${dataVar}`;
  },

  updateSet(
    idVar: string,
    keyDataVar: ({ key: string; variable: string } | { statement: string } | string)[]
  ) {
    return `UPDATE $${idVar} SET ${keyDataVar
      .map((kdv) =>
        typeof kdv === 'string'
          ? `${kdv} = $${kdv}`
          : 'statement' in kdv
            ? kdv.statement
            : `${kdv.key} = $${kdv.variable}`
      )
      .join(', ')}`;
  },

  delete(idVar: string) {
    return `DELETE $${idVar}`;
  },

  let(name: string, query: string) {
    return `LET $${name} = (${query})`;
  },

  createMutation(
    t: MutationEventType,
    mutationIdVar: string,
    recordIdVar: string,
    dataVar?: string,
    beforeRecordVar?: string
  ) {
    switch (t) {
      case 'create':
        // `data` is REQUIRED here, not decorative. The outbox row is the only
        // copy of a pending create once the in-memory UpEvent is gone (reload,
        // or a shared-tabs follower whose row is replayed by the leader). This
        // used to drop `dataVar` on the floor, so every replayed create arrived
        // with `data: undefined` and `processUpEvent` threw on
        // `Object.keys(event.data)` before it ever reached the network: the
        // create was unsendable AND it sat at the head of the queue blocking
        // every later mutation. The payload was simply never persisted, so
        // those creates were unrecoverable.
        return dataVar
          ? `CREATE ONLY $${mutationIdVar} SET mutationType = 'create', recordId = $${recordIdVar}, data = $${dataVar}`
          : `CREATE ONLY $${mutationIdVar} SET mutationType = 'create', recordId = $${recordIdVar}`;
      case 'update': {
        let stmt = `CREATE ONLY $${mutationIdVar} SET mutationType = 'update', recordId = $${recordIdVar}, data = $${dataVar}`;
        if (beforeRecordVar) {
          stmt += `, beforeRecord = $${beforeRecordVar}`;
        }
        return stmt;
      }
      case 'delete':
        return `CREATE ONLY $${mutationIdVar} SET mutationType = 'delete', recordId = $${recordIdVar}`;
    }
  },

  returnObject(entries: { key: string; variable: string }[]) {
    return `RETURN {${entries.map(({ key, variable }) => `${key}: $${variable}`).join(',')}}`;
  },
};
