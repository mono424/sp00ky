import { RecordId } from 'surrealdb';
import type { MutationEventType } from '../types';
import type { Vars } from '../kernel/effects';
import type { IngestRecord } from '../services/stream-processor/index';
import type { OutboxItem } from '../state/client-state';
import { surql } from '../utils/surql';
import type { SealedQuery } from '../utils/surql';
import { extractTablePart, parseRecordIdString } from '../utils/index';
import { parseQueryParams } from '../utils/parser';

export const PENDING_TABLE = '_00_pending_mutations';
export const FAILED_TABLE = '_00_failed_mutations';
export const PENDING_ROW_VERSION = 2;

/** One `_00_pending_mutations` row as this client writes it (v2). */
export interface PendingMutationRow {
  id: string;
  mutationType: MutationEventType;
  recordId: string;
  tableName: string;
  data?: Record<string, unknown>;
  beforeRecord?: Record<string, unknown> | null;
  createdAt: number;
  v: number;
}

export interface FailedMutationRow {
  id: string;
  mutationType: MutationEventType;
  recordId: string;
  tableName: string;
  data?: Record<string, unknown>;
  beforeRecord?: Record<string, unknown> | null;
  error: { message: string; kind: 'application' | 'unreplayable' };
  attempts: number;
  createdAt: number;
  failedAt: number;
  revert: 'full' | 'partial' | 'none';
}

/**
 * Parse a record id READ BACK FROM THE STORE, stripping SurrealDB's `⟨⟩`
 * escaping (outbox ids contain `_`, so they come back escaped).
 */
export function parseStoredRecordId(id: string): RecordId<string> {
  const [table, ...rest] = id.split(':');
  let raw = rest.join(':');
  if (raw.startsWith('⟨') && raw.endsWith('⟩')) raw = raw.slice(1, -1);
  return new RecordId(table, raw);
}

/** Stable `table:id` string for an id read from the store, escaping removed. */
export const storedIdString = (id: unknown): string => {
  if (id instanceof RecordId) return `${id.table}:${String(id.id)}`;
  const rid = parseStoredRecordId(String(id));
  return `${rid.table}:${rid.id}`;
};

/** Timestamp prefix of a v1/v2 mutation id, or `null` for a legacy numeric id. */
export function createdAtFromId(id: string): number | null {
  const raw = id.includes(':') ? id.slice(id.indexOf(':') + 1) : id;
  const m = /^⟨?(\d{13})/.exec(raw);
  return m ? Number(m[1]) : null;
}

/**
 * Read any row shape this table ever had. Legacy rows (`v` missing) lack
 * `tableName`, `createdAt` and `beforeRecord`. A create without `data` is not
 * replayable and returns `null`.
 */
export function parsePendingRow(row: unknown): PendingMutationRow | null {
  if (!row || typeof row !== 'object') return null;
  const r = row as Record<string, unknown>;
  const type = r.mutationType;
  if (type !== 'create' && type !== 'update' && type !== 'delete') return null;
  if (typeof r.recordId !== 'string' && !(r.recordId instanceof RecordId)) return null;
  const id = storedIdString(r.id);
  const recordId = typeof r.recordId === 'string' ? r.recordId : storedIdString(r.recordId);
  const data = r.data && typeof r.data === 'object' ? (r.data as Record<string, unknown>) : undefined;
  if (type === 'create' && !data) return null;
  return {
    id,
    mutationType: type,
    recordId,
    tableName: typeof r.tableName === 'string' ? r.tableName : extractTablePart(recordId),
    data,
    beforeRecord:
      r.beforeRecord && typeof r.beforeRecord === 'object' ? (r.beforeRecord as Record<string, unknown>) : undefined,
    createdAt: typeof r.createdAt === 'number' ? r.createdAt : (createdAtFromId(id) ?? 0),
    v: typeof r.v === 'number' ? r.v : 1,
  };
}

/**
 * Re-type a stored row's payload from the schema before it is pushed. The
 * store keeps JSON, so a `record<user>` field comes back as `'user:x'` and a
 * datetime as a string; the server coerces neither. Unknown keys stay as they
 * are, and a payload the schema rejects is sent raw so the server's answer,
 * not a local throw, decides its fate.
 */
export function hydrateRowData(row: PendingMutationRow, columns: Record<string, unknown> | null): PendingMutationRow {
  if (!row.data || !columns) return row;
  try {
    return { ...row, data: parseQueryParams(columns as never, row.data) };
  } catch {
    return row;
  }
}

export const toOutboxItem = (row: PendingMutationRow): OutboxItem => ({
  id: row.id,
  type: row.mutationType,
  recordId: row.recordId,
  table: row.tableName,
  status: 'pending',
  ackedAt: null,
  attempts: 0,
});

export const loadPendingRows = (): string => `SELECT * FROM ${PENDING_TABLE} ORDER BY id ASC`;
export const loadFailedRows = (): string => `SELECT * FROM ${FAILED_TABLE} ORDER BY failedAt ASC`;

// ---- local write transactions ------------------------------------------------

export interface LocalTx {
  query: SealedQuery<unknown>;
  vars: Vars;
}

const pendingInsert = (type: MutationEventType, withData: boolean, withBefore: boolean): string => {
  const fields = [
    `mutationType = '${type}'`,
    'recordId = $id',
    'tableName = $table',
    'createdAt = $createdAt',
    `v = ${PENDING_ROW_VERSION}`,
  ];
  if (withData) fields.push('data = $data');
  if (withBefore) fields.push('beforeRecord = $before');
  return `CREATE ONLY $mid SET ${fields.join(', ')}`;
};

export interface WritePlanInput {
  recordId: RecordId<string>;
  mutationId: RecordId<string>;
  table: string;
  data?: Record<string, unknown>;
  before?: Record<string, unknown> | null;
  now: number;
}

/** `CREATE` the row and its outbox entry in one transaction; result index 0 is the row. */
export function planCreateTx(input: WritePlanInput): LocalTx {
  const data = input.data ?? {};
  const keys = Object.keys(data).map((key) => ({ key, variable: `data_${key}` }));
  const vars: Vars = { id: input.recordId, mid: input.mutationId, table: input.table, createdAt: input.now, data };
  for (const { key, variable } of keys) vars[variable] = data[key];
  return {
    query: surql.seal<unknown>(surql.tx([surql.createSet('id', keys), pendingInsert('create', true, false)]), {
      resultIndex: 0,
    }),
    vars,
  };
}

/** Bump `_00_rv`, MERGE the patch, write the outbox row (with `beforeRecord`); returns `{ target }`. */
export function planUpdateTx(input: WritePlanInput): LocalTx {
  return {
    query: surql.seal<unknown>(
      surql.tx([
        surql.updateSet('id', [{ statement: '_00_rv += 1' }]),
        surql.let('updated', surql.updateMerge('id', 'data')),
        pendingInsert('update', true, true),
        surql.returnObject([{ key: 'target', variable: 'updated' }]),
      ])
    ),
    vars: {
      id: input.recordId,
      mid: input.mutationId,
      table: input.table,
      createdAt: input.now,
      data: input.data ?? {},
      before: input.before ?? null,
    },
  };
}

/** DELETE the row and write the outbox entry (with `beforeRecord`). */
export function planDeleteTx(input: WritePlanInput): LocalTx {
  return {
    query: surql.seal<unknown>(surql.tx([surql.delete('id'), pendingInsert('delete', false, true)])),
    vars: {
      id: input.recordId,
      mid: input.mutationId,
      table: input.table,
      createdAt: input.now,
      before: input.before ?? null,
    },
  };
}

/** Debounced update: bump `_00_rv` and MERGE now; the outbox row comes later. */
export function planLocalOnlyUpdateTx(input: { recordId: RecordId<string>; data: Record<string, unknown> }): LocalTx {
  return {
    query: surql.seal<unknown>(
      surql.tx([
        surql.updateSet('id', [{ statement: '_00_rv += 1' }]),
        surql.let('updated', surql.updateMerge('id', 'data')),
        surql.returnObject([{ key: 'target', variable: 'updated' }]),
      ])
    ),
    vars: { id: input.recordId, data: input.data },
  };
}

/** The outbox row of a flushed debounced update, on its own. */
export function planDeferredOutboxRowTx(input: WritePlanInput): LocalTx {
  return {
    query: surql.seal<unknown>(surql.tx([pendingInsert('update', true, true)])),
    vars: {
      id: input.recordId,
      mid: input.mutationId,
      table: input.table,
      createdAt: input.now,
      data: input.data ?? {},
      before: input.before ?? null,
    },
  };
}

// ---- remote push -------------------------------------------------------------

export interface RemoteBatch {
  sql: string;
  vars: Vars;
}

/**
 * Many outbox rows as ONE multi-statement request. Statement i answers row i.
 * Each statement is its own transaction on the server, as before.
 */
export function remoteBatch(rows: ReadonlyArray<PendingMutationRow>): RemoteBatch {
  const vars: Vars = {};
  const stmts = rows.map((row, i) => {
    const rid = parseRecordIdString(row.recordId);
    vars[`id${i}`] = rid;
    switch (row.mutationType) {
      case 'create': {
        const data = row.data ?? {};
        const sets = Object.keys(data).map((key) => {
          vars[`d${i}_${key}`] = data[key];
          return `${key} = $d${i}_${key}`;
        });
        return `CREATE ONLY $id${i} SET ${sets.join(', ')}`;
      }
      case 'update':
        vars[`data${i}`] = row.data ?? {};
        return `UPDATE $id${i} MERGE $data${i}`;
      case 'delete':
        return `DELETE $id${i}`;
    }
  });
  return { sql: stmts.join(';\n'), vars };
}

// ---- rollback ----------------------------------------------------------------

export interface RevertPlan {
  tx: LocalTx | null;
  circuit: IngestRecord;
  revert: FailedMutationRow['revert'];
}

/**
 * How to undo a rejected mutation locally. A create is deleted; an update or
 * delete is restored from `beforeRecord` (REPLACE, so the pre-bump `_00_rv`
 * comes back). Without a `beforeRecord` nothing can be restored.
 */
export function planRevert(row: PendingMutationRow, before: Record<string, unknown> | null): RevertPlan {
  const rid = parseRecordIdString(row.recordId);
  if (row.mutationType === 'create') {
    return {
      tx: { query: surql.seal<unknown>(surql.tx([surql.delete('id')])), vars: { id: rid } },
      circuit: { table: row.tableName, op: 'DELETE', id: row.recordId, record: row.data ?? {} },
      revert: 'full',
    };
  }
  if (!before) {
    return {
      tx: null,
      circuit: { table: row.tableName, op: 'UPDATE', id: row.recordId, record: {} },
      revert: 'partial',
    };
  }
  const { id: _id, ...content } = before;
  return {
    tx: { query: surql.seal<unknown>(surql.tx([surql.upsert('id', 'content')])), vars: { id: rid, content } },
    circuit: {
      table: row.tableName,
      op: row.mutationType === 'delete' ? 'CREATE' : 'UPDATE',
      id: row.recordId,
      record: { ...before, id: rid },
    },
    revert: 'full',
  };
}

export const failedRecordId = (mutationId: string): RecordId<string> =>
  new RecordId(FAILED_TABLE, mutationId.slice(mutationId.indexOf(':') + 1));

export function buildFailedRow(
  row: PendingMutationRow,
  error: { message: string; kind: 'application' | 'unreplayable' },
  before: Record<string, unknown> | null,
  attempts: number,
  now: number,
  revert: FailedMutationRow['revert']
): FailedMutationRow {
  return {
    id: row.id,
    mutationType: row.mutationType,
    recordId: row.recordId,
    tableName: row.tableName,
    data: row.data,
    beforeRecord: before,
    error,
    attempts,
    createdAt: row.createdAt,
    failedAt: now,
    revert,
  };
}

/** ONE transaction: write the tray row, delete the pending row. */
export function moveToFailedTx(failed: FailedMutationRow): LocalTx {
  const { id: _id, ...content } = failed;
  return {
    query: surql.seal<unknown>(surql.tx([surql.create('fid', 'failed'), surql.delete('mid')])),
    vars: { fid: failedRecordId(failed.id), failed: content, mid: parseStoredRecordId(failed.id) },
  };
}

export const deletePendingRow = (mutationId: string): { sql: string; vars: Vars } => ({
  sql: 'DELETE $mid',
  vars: { mid: parseStoredRecordId(mutationId) },
});

export const deleteFailedRow = (mutationId: string): { sql: string; vars: Vars } => ({
  sql: 'DELETE $fid',
  vars: { fid: failedRecordId(mutationId) },
});

export function parseFailedRow(row: unknown): FailedMutationRow | null {
  if (!row || typeof row !== 'object') return null;
  const r = row as Record<string, unknown>;
  const type = r.mutationType;
  if (type !== 'create' && type !== 'update' && type !== 'delete') return null;
  if (typeof r.recordId !== 'string') return null;
  const err = (r.error ?? {}) as Record<string, unknown>;
  return {
    id: storedIdString(r.id).replace(`${FAILED_TABLE}:`, `${PENDING_TABLE}:`),
    mutationType: type,
    recordId: r.recordId,
    tableName: typeof r.tableName === 'string' ? r.tableName : extractTablePart(r.recordId),
    data: r.data && typeof r.data === 'object' ? (r.data as Record<string, unknown>) : undefined,
    beforeRecord:
      r.beforeRecord && typeof r.beforeRecord === 'object' ? (r.beforeRecord as Record<string, unknown>) : null,
    error: {
      message: typeof err.message === 'string' ? err.message : 'unknown',
      kind: err.kind === 'unreplayable' ? 'unreplayable' : 'application',
    },
    attempts: typeof r.attempts === 'number' ? r.attempts : 0,
    createdAt: typeof r.createdAt === 'number' ? r.createdAt : 0,
    failedAt: typeof r.failedAt === 'number' ? r.failedAt : 0,
    revert: r.revert === 'partial' || r.revert === 'none' ? r.revert : 'full',
  };
}
