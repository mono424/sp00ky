import type { GetTable, SchemaStructure, TableModel, TableNames } from '@spooky-sync/query-builder';
import { Uuid, RecordId, Duration } from 'surrealdb';
import type { Logger } from '../services/logger/index';
import type { QueryTimeToLive } from '../types';

export * from './surql';
export * from './parser';
export * from './error-classification';

// ==================== RECORDID UTILITIES ====================

export const compareRecordIds = (
  a: RecordId<string> | string,
  b: RecordId<string> | string
): boolean => {
  const nA = a instanceof RecordId ? encodeRecordId(a) : a;
  const nB = b instanceof RecordId ? encodeRecordId(b) : b;
  return nA === nB;
};

export const encodeRecordId = (recordId: RecordId<string>): string => {
  return `${recordId.table.name}:${recordId.id}`;
};

export const extractIdPart = (id: string | RecordId<string>): string => {
  if (typeof id === 'string') {
    return id.split(':').slice(1).join(':');
  }
  // RecordId.id can be string, number, object, or array
  const idValue = id.id;
  if (typeof idValue === 'string') {
    return idValue;
  }
  // For other types (number, object, array), convert to string
  return String(idValue);
};

export const extractTablePart = (id: string | RecordId<string>): string => {
  if (typeof id === 'string') {
    return id.split(':')[0];
  }
  return id.table.name;
};

export const parseRecordIdString = (id: string): RecordId<string> => {
  const [table, ...idParts] = id.split(':');
  return new RecordId(table, idParts.join(':'));
};

export function generateId(): string {
  return Uuid.v4().toString().replace(/-/g, '');
}

export function generateNewTableId<S extends SchemaStructure, T extends TableNames<S>>(
  tableName: T
): RecordId {
  return new RecordId(tableName, generateId());
}

// ==================== SCHEMA ENCODING/DECODING ====================

export function decodeFromSp00ky<S extends SchemaStructure, T extends TableNames<S>>(
  schema: S,
  tableName: T,
  record: TableModel<GetTable<S, T>>
): TableModel<GetTable<S, T>> {
  const table = schema.tables.find((t) => t.name === tableName);
  if (!table) {
    throw new Error(`Table ${tableName} not found in schema`);
  }

  const encoded = { ...record } as any;

  for (const field of Object.keys(table.columns)) {
    const column = table.columns[field] as any;
    const relation = schema.relationships.find((r) => r.from === tableName && r.field === field);
    if ((column.recordId || relation) && encoded[field] !== null && encoded[field] !== undefined) {
      if (encoded[field] instanceof RecordId) {
        encoded[field] = `${encoded[field].table.name}:${encoded[field].id}`;
      } else if (
        relation &&
        (encoded[field] instanceof Object || Array.isArray(encoded[field]))
      ) {
        if (Array.isArray(encoded[field])) {
          encoded[field] = encoded[field].map((item) =>
            decodeFromSp00ky(schema, relation.to, item)
          );
        } else {
          encoded[field] = decodeFromSp00ky(schema, relation.to, encoded[field]);
        }
      }
    }
  }

  return encoded as TableModel<GetTable<S, T>>;
}

// ==================== TIME/DURATION UTILITIES ====================

/**
 * Read the millisecond count off a surrealdb `Duration`. Different `surrealdb`
 * releases expose it under `milliseconds` (current) or the older private
 * `_milliseconds` field, so read whichever is set; returns 0 when neither is.
 */
function durationMillis(duration: Duration): number {
  const d = duration as { milliseconds?: number | bigint; _milliseconds?: number | bigint };
  return Number(d.milliseconds || d._milliseconds || 0);
}

/**
 * Parse duration string or Duration object to milliseconds
 */
export function parseDuration(duration: QueryTimeToLive | Duration): number {
  if (duration instanceof Duration) {
    const ms = durationMillis(duration);
    if (ms) return ms;
    const str = duration.toString();
    if (str !== '[object Object]') return parseDuration(str as QueryTimeToLive);
    return 600000;
  }

  if (typeof duration === 'bigint') {
    return Number(duration);
  }

  if (typeof duration !== 'string') return 600000;

  const match = duration.match(/^(\d+)([smh])$/);
  if (!match) return 600000;
  const val = Number.parseInt(match[1], 10);
  const unit = match[2];
  switch (unit) {
    case 's':
      return val * 1000;
    case 'h':
      return val * 3600000;
    case 'm':
    default:
      return val * 60000;
  }
}

// ==================== FILE UTILITIES ====================

export async function fileToUint8Array(file: File | Blob): Promise<Uint8Array> {
  const buffer = await file.arrayBuffer();
  return new Uint8Array(buffer);
}

// ==================== TEXT UTILITIES ====================

/**
 * Convert plain text to simple HTML paragraphs.
 * Useful for seeding a rich-text editor (e.g. TipTap/ProseMirror) with fallback content.
 */
export function textToHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .split('\n').map((l) => `<p>${l || '<br>'}</p>`).join('');
}

// ==================== DATABASE UTILITIES ====================

/**
 * Helper for retrying DB operations with exponential backoff
 */
export async function withRetry<T>(
  logger: Logger,
  operation: () => Promise<T>,
  retries = 3,
  delayMs = 100
): Promise<T> {
  let lastError;
  for (let i = 0; i < retries; i++) {
    try {
      return await operation();
    } catch (err: any) {
      lastError = err;
      // A deadline expiry is explicitly not retryable: the op is still running
      // in the engine, and a retry only queues a second copy behind it.
      if (err?.retryable === false) throw err;
      if (
        err?.message?.includes('Can not open transaction') ||
        err?.message?.includes('transaction') ||
        err?.message?.includes('Database is busy')
      ) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn(
          {
            attempt: i + 1,
            retries,
            error: msg,
            Category: 'sp00ky-client::utils::withRetry',
          },
          'Retrying DB operation'
        );
        await new Promise((res) => setTimeout(res, delayMs * (i + 1)));
        continue;
      }
      throw err;
    }
  }
  throw lastError;
}

/**
 * Reject after `timeoutMs` if `promise` hasn't settled.
 *
 * Exists because a WebSocket RPC has no deadline of its own: the SurrealDB SDK
 * parks each call in a pending map and only rejects it when the socket reports
 * a close. On a half-open socket (peer gone, no `close` event, `readyState`
 * still OPEN) the call never settles at all, which wedges anything serialized
 * behind it.
 *
 * `message` MUST contain "timed out" so `classifySyncError` treats the
 * rejection as `network` — that's what makes the sync queues retry the
 * operation instead of rolling the mutation back as an application error.
 *
 * Non-positive `timeoutMs` returns `promise` unchanged. The underlying promise
 * is NOT cancelled (nothing can cancel an in-flight RPC); its eventual
 * settlement is absorbed, so attach no expectations to it after a timeout.
 */
export function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  message: string | (() => Error)
): Promise<T> {
  if (!(timeoutMs > 0)) return promise;
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(typeof message === 'function' ? message() : new Error(message)),
      timeoutMs
    );
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      }
    );
  });
}
