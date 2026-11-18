import type { QueryPlan, WhereNode } from '@spooky-sync/query-builder';
import type { EngineStorageDiagnostics } from '../../modules/devtools/storage-info';
import type { Id, OrderBy, RelationFetch, Row, SnapshotMeta, StoredSnapshot } from './cache-engine';
import { stableKey } from './relation-resolver';
import { fromBase64, serializeMergeRow, serializeRow } from './sqlite-plan-sql';
import { COLUMNS_ROW_ID, planColumnReset } from './sqlite-column-reset';
import {
  SNAPSHOT_TABLE,
  SqliteCacheEngine,
  normalizePlanForClone,
  normalizeWhereForClone,
  toCloneSafe,
} from './sqlite-cache-engine';
import { WorkerSqliteTransport, type SqliteTransport } from './sqlite-transport';
import { tableOf, type SqlOp } from './surql-translate';
import { parseRowBytes, type WireOp } from './rows-exec';
import { FLAG_RAW, FLAG_U8, bucketDirName, type StoreStats } from './rows-store';

/**
 * Tables whose rows exist nowhere else: writes the user made that the server
 * has not confirmed. A batch that touches one is flushed to disk before the
 * write resolves. Everything else is a copy of server state and is flushed a
 * moment later, in the worker's own time: losing it to a power cut costs a
 * refetch, not a user's work.
 */
export const ROWS_DURABLE_TABLES: ReadonlySet<string> = new Set([
  '_00_pending_mutations',
  '_00_pending_writes',
  '_00_failed_mutations',
  '_00_crdt_local',
]);

const SCHEMA_TABLE = '_00_schema';
const encoder = new TextEncoder();
const decoder = new TextDecoder();

type RowsReply = { buf: ArrayBuffer | null; u8?: boolean };

/** One write, with the table and key already resolved. */
export type WriteItem =
  | { kind: 'upsert'; table: string; key: string; data: Row; mode: 'replace' | 'merge' }
  | { kind: 'delete'; table: string; key: string }
  | { kind: 'clear'; table: string };

/** The writes of a translated statement list; the table is the id's own. */
function itemsOf(ops: SqlOp[]): WriteItem[] {
  const items: WriteItem[] = [];
  for (const op of ops) {
    if (op.kind === 'noop') continue;
    if (op.kind === 'deleteAll') items.push({ kind: 'clear', table: op.table });
    else if (op.kind === 'delete') items.push({ kind: 'delete', table: tableOf(op.id), key: stableKey(op.id) });
    else if (op.kind === 'upsert') {
      items.push({ kind: 'upsert', table: tableOf(op.id), key: stableKey(op.id), data: op.data, mode: op.mode });
    } else throw new Error(`RowsCacheEngine: ${op.kind} is not a write`);
  }
  return items;
}

/**
 * Serialize a batch of writes for the worker: every row's JSON text in one
 * buffer, which is transferred, and one small tuple per op saying where its
 * text lies. The text is what `SqliteCacheEngine` stores, byte for byte.
 */
export function packWrites(items: WriteItem[]): { wire: WireOp[]; buf: ArrayBuffer | null; sync: boolean } {
  const wire: WireOp[] = [];
  let bytes = new Uint8Array(0);
  let at = 0;
  let sync = false;
  const append = (text: string): [off: number, len: number] => {
    // `encodeInto` needs room for the worst case, three bytes per UTF-16 unit.
    const need = at + text.length * 3;
    if (bytes.length < need) {
      const grown = new Uint8Array(Math.max(need, bytes.length * 2, 1024));
      grown.set(bytes.subarray(0, at));
      bytes = grown;
    }
    const written = encoder.encodeInto(text, bytes.subarray(at)).written;
    const off = at;
    at += written;
    return [off, written];
  };
  for (const op of items) {
    const table = op.table;
    if (ROWS_DURABLE_TABLES.has(table)) sync = true;
    if (op.kind === 'clear') {
      wire.push(['clear', table]);
      continue;
    }
    const key = op.key;
    if (op.kind === 'delete') {
      wire.push(['del', table, key]);
      continue;
    }
    const row: Row = { ...op.data, id: key };
    const rv = Number(row._00_rv) || 0;
    if (op.mode === 'replace') {
      const text = serializeRow(row);
      const [off, len] = append(text);
      wire.push(['put', table, key, rv, text.indexOf('"__u8"') === -1 ? 0 : FLAG_U8, off, len]);
      continue;
    }
    const { insert, patch } = serializeMergeRow(row);
    const [off, len] = append(insert);
    // The patch differs from the row only when a field is being cleared.
    const [patchOff, patchLen] = patch === insert ? [0, 0] : append(patch);
    wire.push(['merge', table, key, rv, insert.indexOf('"__u8"') === -1 ? 0 : FLAG_U8, off, len, patchOff, patchLen]);
  }
  return { wire, buf: at > 0 ? (bytes.buffer as ArrayBuffer) : null, sync };
}

/**
 * Local cache backend on sp00ky's own row store (`rows-store.ts`), in a
 * dedicated Worker (`rows-worker.ts`) on OPFS.
 *
 * It answers every verb the way {@link SqliteCacheEngine} does (same stored
 * JSON, same filter and ordering rules, checked against real SQLite in
 * `rows-exec.integration.test.ts`), so an app can switch between the two with
 * `localEngine` alone. What differs is underneath: no SQL is rendered, rows
 * cross the worker boundary as JSON text in a transferred buffer and are
 * parsed once, on the side that reads them.
 *
 * It extends the SQLite engine for everything that is about the worker rather
 * than about SQL: the serialized op queue, epoch fencing, bucket switches, the
 * shared-tabs roles, the SurrealQL-vocabulary shim. Every method that renders
 * SQL there is replaced here by a message to the row worker.
 */
export class RowsCacheEngine extends SqliteCacheEngine {
  override readonly engineKind = 'rows' as const;

  protected override createTransport(): SqliteTransport {
    return new WorkerSqliteTransport(
      this.logger,
      // Same shape as the SQLite worker's URL: `.ts` for bundlers that build
      // from source, rewritten to `./rows-worker.js` in the published build.
      this.config.workers?.rows ?? (() => new Worker(new URL('./rows-worker.ts', import.meta.url), { type: 'module' }))
    );
  }

  /** Tables need no creating: a table is the rows written to it. */
  protected override async ensureTable(): Promise<void> {}

  private async rows(type: string, payload: unknown): Promise<Row[]> {
    return parseRowBytes<Row[]>(await this.call<RowsReply>(type, payload), fromBase64) ?? [];
  }

  // ---- reads ---------------------------------------------------------------

  override async select(plan: QueryPlan, params: Record<string, unknown> = {}): Promise<Row[]> {
    // Class instances (RecordId) do not survive structured clone; see the
    // SQLite engine's `select` for why the KEYS of `params` must all stay.
    const normParams: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(params)) normParams[k] = toCloneSafe(v);
    return this.rows('select', { plan: normalizePlanForClone(plan), params: normParams });
  }

  override async fetchRelation(req: RelationFetch): Promise<Row[]> {
    return this.rows('relation', {
      table: req.table,
      matchField: req.matchField,
      keys: req.keys.map(stableKey),
      where: normalizeWhereForClone(req.where),
      orderBy: req.orderBy,
      select: req.select,
    });
  }

  override async selectByIds(
    table: string,
    ids: Id[],
    opts?: { select?: string[]; orderBy?: OrderBy }
  ): Promise<Row[]> {
    if (ids.length === 0) return [];
    return this.rows('by-ids', { table, ids: ids.map(stableKey), select: opts?.select, orderBy: opts?.orderBy });
  }

  override async getById(table: string, id: Id): Promise<Row | null> {
    return parseRowBytes<Row | null>(await this.call<RowsReply>('get', { table, id: stableKey(id) }), fromBase64);
  }

  override async scanVersions(tables: string[]): Promise<Record<string, [string, number][]>> {
    return (await this.call<{ versions: Record<string, [string, number][]> }>('versions', { tables })).versions;
  }

  /**
   * See `sqlite-column-reset.ts`. The recorded column sets live in one raw row
   * of `_00_schema`, which no row read ever returns.
   */
  override async resetChangedTables(fingerprints: Record<string, string>, keep: Set<string>): Promise<string[]> {
    const { bytes } = await this.call<{ bytes: ArrayBuffer | null }>('blob-get', { table: SCHEMA_TABLE, key: COLUMNS_ROW_ID });
    let stored: Record<string, string> | null = null;
    try {
      if (bytes) stored = JSON.parse(decoder.decode(new Uint8Array(bytes))) as Record<string, string>;
    } catch {
      stored = null;
    }
    const tables = await this.listTables();
    const plan = planColumnReset(stored, fingerprints, new Set(tables), keep);
    if (plan.statements.length === 0) return plan.changed;
    if (plan.changed.length > 0) await this.call('reset-versions', { tables: plan.changed, keep: [...keep] });
    // Recorded last: a tab that dies between the two redoes the reset, which is harmless.
    const body = encoder.encode(JSON.stringify(fingerprints));
    const buf = body.buffer as ArrayBuffer;
    await this.call(
      'write',
      { ops: [['put', SCHEMA_TABLE, COLUMNS_ROW_ID, 0, FLAG_RAW, 0, body.length]] satisfies WireOp[], buf, sync: false },
      [buf]
    );
    return plan.changed;
  }

  override async getSnapshot(key: string): Promise<StoredSnapshot | null> {
    const { bytes, meta } = await this.call<{ bytes: ArrayBuffer | null; meta: string | null }>('blob-get', {
      table: SNAPSHOT_TABLE,
      key,
    });
    if (!bytes || !meta) return null;
    try {
      return { bytes: new Uint8Array(bytes), meta: JSON.parse(meta) as SnapshotMeta };
    } catch {
      return null;
    }
  }

  override async putSnapshot(key: string, bytes: Uint8Array, meta: SnapshotMeta): Promise<void> {
    // A copy, so the transfer does not take the caller's buffer away.
    const buf = bytes.slice().buffer as ArrayBuffer;
    await this.call('blob-put', { table: SNAPSHOT_TABLE, key, bytes: buf, meta: JSON.stringify(meta) }, [buf]);
  }

  override async deleteSnapshot(key: string): Promise<void> {
    await this.call('blob-del', { table: SNAPSHOT_TABLE, key });
  }

  // ---- writes --------------------------------------------------------------

  override async upsert(table: string, id: Id, data: Row, mode: 'replace' | 'merge'): Promise<void> {
    await this.write([{ kind: 'upsert', table, key: stableKey(id), data, mode }]);
  }

  override async delete(table: string, id: Id): Promise<void> {
    await this.write([{ kind: 'delete', table, key: stableKey(id) }]);
  }

  /** One message, one atomic batch in the store, however many rows. */
  private async write(items: WriteItem[]): Promise<void> {
    const { wire, buf, sync } = packWrites(items);
    if (wire.length === 0) return;
    await this.call('write', { ops: wire, buf, sync }, buf ? [buf] : undefined);
  }

  protected override runWriteBatch(ops: SqlOp[]): Promise<void> {
    return this.write(itemsOf(ops));
  }

  protected override async rawSelectTable(
    table: string,
    where?: WhereNode[],
    orderBy?: OrderBy,
    window?: { limit?: number; start?: number }
  ): Promise<Row[]> {
    return this.rows('select-table', {
      table,
      where: normalizeWhereForClone(where),
      orderBy,
      limit: window?.limit,
      start: window?.start,
    });
  }

  protected override async countRows(table: string, where?: WhereNode[]): Promise<number> {
    return (await this.call<{ n: number }>('count', { table, where: normalizeWhereForClone(where) })).n;
  }

  protected override async listTables(): Promise<string[]> {
    return (await this.call<{ tables: string[] }>('tables')).tables;
  }

  protected override async deleteAllRows(table: string): Promise<void> {
    await this.write([{ kind: 'clear', table }]);
  }

  // ---- housekeeping --------------------------------------------------------

  override async getStorageDiagnostics(opts?: { tableCounts?: boolean }): Promise<EngineStorageDiagnostics> {
    const diag: EngineStorageDiagnostics = {
      engine: 'rows',
      bucketId: this.bucketId,
      useOpfs: this.useOpfs,
      workerSelectConfigured: true,
      workerSelectEffective: true,
    };
    try {
      const { stats, tableCounts } = await this.call<{
        stats: StoreStats;
        tableCounts?: { table: string; rows: number }[];
      }>('diag', { tableCounts: opts?.tableCounts === true });
      diag.dbSizeBytes = stats.fileBytes;
      diag.freelistBytes = stats.garbageBytes;
      if (tableCounts) diag.tableCounts = tableCounts.filter((t) => t.table !== SNAPSHOT_TABLE);
    } catch (e) {
      diag.error = e instanceof Error ? e.message : String(e);
    }
    return diag;
  }

  /** See `LocalStore.dropBucket`. Fails harmlessly while a worker holds the files. */
  override async dropBucket(bucketId: string): Promise<void> {
    if (bucketId === this.bucketId) throw new Error('Cannot drop the open bucket.');
    const storage = typeof navigator !== 'undefined' ? navigator.storage : undefined;
    if (!storage?.getDirectory) return;
    const root = await storage.getDirectory();
    try {
      await root.removeEntry(bucketDirName(bucketId), { recursive: true });
    } catch (error) {
      if ((error as DOMException)?.name !== 'NotFoundError') throw error;
    }
  }
}
