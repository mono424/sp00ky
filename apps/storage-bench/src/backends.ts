/**
 * The three backends under test, behind one interface.
 *
 * - `sqlite` and `rows` are the client's real engines, driven through the
 *   methods and statements the client itself uses: a sync-down batch is the
 *   `BEGIN … UPSERT ONLY $id MERGE $data … COMMIT` transaction the sync layer
 *   sends, a screen is `select(plan)`. What is timed is what an app pays, the
 *   worker round trip and the parse on the page included.
 * - `indexeddb` is written for this benchmark, as a careful app would use
 *   IndexedDB directly on the page: one object store per table keyed by id,
 *   an index on each foreign key and on the filter the list uses, a side
 *   store of versions so reading them never loads a body, and structured
 *   clone doing the encoding. It gets every advantage the other two do not
 *   give themselves: neither engine has an index besides the primary key.
 */
import { SqliteCacheEngine } from '@core/services/database/sqlite-cache-engine';
import { RowsCacheEngine } from '@core/services/database/rows-cache-engine';
import { mergePatch } from '@core/services/database/rows-exec';
import { surql } from '@core/utils/surql';
import { RecordId } from 'surrealdb';
import type { Row } from './data';

export const BUCKET = 'bench';

export interface Backend {
  readonly name: string;
  open(): Promise<void>;
  close(): Promise<void>;
  /** Delete everything this backend stores. Call while closed. */
  wipe(): Promise<void>;
  /** One atomic batch of rows, merged into whatever is stored: a sync-down. */
  bulk(table: string, rows: Row[]): Promise<void>;
  /** Merge one row. */
  put(table: string, row: Row): Promise<void>;
  /** A user's write: the row and its outbox entry, in one transaction, durable. */
  userWrite(row: Row, mutationId: string): Promise<void>;
  get(table: string, id: string): Promise<Row | null>;
  getMany(table: string, ids: string[], fields?: string[]): Promise<Row[]>;
  /**
   * The read a query's first render makes in the client: the games of its
   * render set, by id, as one select the client names after the query.
   */
  firstQuery(ids: string[]): Promise<Row[]>;
  /**
   * What `firstQuery` answered in the last page load, synchronously and
   * without waiting for `open`: the rows the client seeds a new query with,
   * so its first render is not empty. Only the row store keeps them.
   */
  peekFirstQuery?(ids: string[]): Row[] | null;
  /** `WHERE field = value ORDER BY orderBy DESC LIMIT limit`. */
  where(table: string, field: string, value: unknown, orderBy: string, limit: number): Promise<Row[]>;
  /** Rows of `table` whose `field` is one of `keys`. */
  children(table: string, field: string, keys: string[]): Promise<Row[]>;
  /** A screen: games by id, each with its white player and its top 3 comments. */
  screen(ids: string[]): Promise<Row[]>;
  versions(table: string): Promise<[string, number][]>;
  /** Bytes on disk, when the backend can tell. */
  size(): Promise<number | null>;
}

const logger = {
  info() {},
  warn() {},
  error(...args: unknown[]) {
    console.error('[engine]', ...args);
  },
  debug() {},
  trace() {},
  child() {
    return logger;
  },
};

type Engine = SqliteCacheEngine & {
  peekSelect?: (plan: QueryPlan, params: Record<string, unknown>, opts: { paintKey: string }) => Row[] | null;
};
type QueryPlan = Parameters<SqliteCacheEngine['select']>[0];

/** The name the client gives a query's render read: its view key. */
const FIRST_QUERY = { paintKey: 'bench:first-screen' };
/** A query's rows by id, as `materialize` asks for them. */
const firstPlan = (ids: string[]): QueryPlan => ({ table: 'game', ids: ids.map((id) => new RecordId('game', id.slice(id.indexOf(':') + 1))) });


/** Sealed `tx([upsertMerge × n])`, cached per batch size: the text only depends on n. */
const bulkStatements = new Map<number, ReturnType<typeof surql.seal>>();

function bulkStatement(n: number): ReturnType<typeof surql.seal> {
  let sealed = bulkStatements.get(n);
  if (!sealed) {
    const statements: string[] = [];
    for (let i = 0; i < n; i++) statements.push(surql.upsertMerge(`i${i}`, `d${i}`));
    sealed = surql.seal(surql.tx(statements));
    bulkStatements.set(n, sealed);
  }
  return sealed;
}

/** One sync-down batch into an engine: the transaction the client's sync layer sends. */
export async function engineBulk(engine: SqliteCacheEngine, rows: Row[]): Promise<void> {
  const vars: Record<string, unknown> = {};
  for (let i = 0; i < rows.length; i++) {
    const { id, ...data } = rows[i];
    vars[`i${i}`] = id;
    vars[`d${i}`] = data;
  }
  await engine.execute(bulkStatement(rows.length), vars);
}

const createNote = surql.seal(
  surql.tx([
    surql.createSet('id', [
      { key: 'title', variable: 'data_title' },
      { key: 'body', variable: 'data_body' },
      { key: 'created_ms', variable: 'data_created_ms' },
    ]),
    surql.createMutation('create', 'mid', 'id', 'data'),
  ]),
  { resultIndex: 0 }
);

class EngineBackend implements Backend {
  private engine: Engine | null = null;

  constructor(
    readonly name: string,
    private make: () => Engine
  ) {}

  private get e(): Engine {
    if (!this.engine) throw new Error(`${this.name}: not open`);
    return this.engine;
  }

  async open(): Promise<void> {
    this.engine = this.make();
    await this.engine.connect(BUCKET);
  }

  async close(): Promise<void> {
    await this.engine?.close();
    this.engine = null;
  }

  async wipe(): Promise<void> {
    // A worker the page head started for this load holds the files: let go first.
    const g = globalThis as { __sp00kyRowsPrewarm?: { worker: Worker } };
    g.__sp00kyRowsPrewarm?.worker.terminate();
    delete g.__sp00kyRowsPrewarm;
    localStorage.removeItem('sp00ky.rows.prewarm');
    await new Promise((resolve) => setTimeout(resolve, 50));
    // A fresh, unconnected engine: its own bucket is not the one being dropped.
    await this.make().dropBucket(BUCKET);
  }

  async bulk(_table: string, rows: Row[]): Promise<void> {
    const vars: Record<string, unknown> = {};
    for (let i = 0; i < rows.length; i++) {
      const { id, ...data } = rows[i];
      vars[`i${i}`] = id;
      vars[`d${i}`] = data;
    }
    await this.e.execute(bulkStatement(rows.length), vars);
  }

  async put(table: string, row: Row): Promise<void> {
    const { id, ...data } = row;
    await this.e.upsert(table, id, data, 'merge');
  }

  async userWrite(row: Row, mutationId: string): Promise<void> {
    await this.e.execute(createNote, {
      id: row.id,
      mid: `_00_pending_mutations:${mutationId}`,
      data_title: row.title,
      data_body: row.body,
      data_created_ms: row.created_ms,
      data: { title: row.title, body: row.body, created_ms: row.created_ms },
    });
  }

  get(table: string, id: string): Promise<Row | null> {
    return this.e.getById(table, id);
  }

  getMany(table: string, ids: string[], fields?: string[]): Promise<Row[]> {
    return this.e.selectByIds(table, ids, fields ? { select: fields } : undefined);
  }

  firstQuery(ids: string[]): Promise<Row[]> {
    return this.e.select(firstPlan(ids), {}, FIRST_QUERY);
  }

  peekFirstQuery(ids: string[]): Row[] | null {
    return this.engine?.peekSelect?.(firstPlan(ids), {}, FIRST_QUERY) ?? null;
  }

  where(table: string, field: string, value: unknown, orderBy: string, limit: number): Promise<Row[]> {
    return this.e.select({ table, where: [{ field, op: '=', value }], orderBy: [[orderBy, 'desc']], limit });
  }

  children(table: string, field: string, keys: string[]): Promise<Row[]> {
    return this.e.fetchRelation({ table, matchField: field, keys });
  }

  screen(ids: string[]): Promise<Row[]> {
    return this.e.select({
      table: 'game',
      ids,
      relations: [
        { alias: 'white', table: 'user', cardinality: 'one', foreignKeyField: 'white' },
        { alias: 'comments', table: 'comment', cardinality: 'many', foreignKeyField: 'game', orderBy: [['rank', 'asc']], limit: 3 },
      ],
    });
  }

  async versions(table: string): Promise<[string, number][]> {
    return (await this.e.scanVersions([table]))[table];
  }

  async size(): Promise<number | null> {
    return (await this.e.getStorageDiagnostics()).dbSizeBytes ?? null;
  }
}

// ---- IndexedDB ----------------------------------------------------------------

const TABLES = ['game', 'comment', 'user', 'note', '_00_pending_mutations'] as const;
const INDEXES: Record<string, string[]> = { game: ['collection'], comment: ['game'] };
const DB_NAME = 'sp00ky-storage-bench';

const done = (tx: IDBTransaction): Promise<void> =>
  new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error ?? new Error('IndexedDB transaction aborted'));
  });

const project = (row: Row, fields: string[]): Row => {
  const out: Row = {};
  for (const f of ['id', ...fields]) if (f in row) out[f] = row[f];
  return out;
};

class IdbBackend implements Backend {
  readonly name = 'indexeddb';
  private db: IDBDatabase | null = null;

  private get d(): IDBDatabase {
    if (!this.db) throw new Error('indexeddb: not open');
    return this.db;
  }

  async open(): Promise<void> {
    // A connection the page's head already asked for, if it did.
    const early = (globalThis as { __benchIdb?: Promise<IDBDatabase> }).__benchIdb;
    delete (globalThis as { __benchIdb?: Promise<IDBDatabase> }).__benchIdb;
    if (early) {
      try {
        this.db = await early;
        return;
      } catch {
        /* open it the ordinary way */
      }
    }
    await new Promise<void>((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        for (const table of TABLES) {
          const store = req.result.createObjectStore(table, { keyPath: 'id' });
          for (const field of INDEXES[table] ?? []) store.createIndex(field, field);
          req.result.createObjectStore(`${table}__rv`);
        }
      };
      req.onsuccess = () => {
        this.db = req.result;
        resolve();
      };
      req.onerror = () => reject(req.error);
    });
    localStorage.setItem('bench:idb-preopen', '1');
  }

  async close(): Promise<void> {
    this.db?.close();
    this.db = null;
  }

  async wipe(): Promise<void> {
    localStorage.removeItem('bench:idb-preopen');
    // A connection the page head opened for this load would block the delete.
    const g = globalThis as { __benchIdb?: Promise<IDBDatabase> };
    const early = g.__benchIdb;
    delete g.__benchIdb;
    if (early) {
      try {
        (await early).close();
      } catch {
        /* never opened */
      }
    }
    await new Promise<void>((resolve, reject) => {
      const req = indexedDB.deleteDatabase(DB_NAME);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
      req.onblocked = () => reject(new Error('indexeddb: delete blocked by an open connection'));
    });
  }

  /** Read, merge, write back: what MERGE means, and the only way IndexedDB can do it. */
  private merge(tx: IDBTransaction, table: string, row: Row): void {
    const store = tx.objectStore(table);
    const versions = tx.objectStore(`${table}__rv`);
    const read = store.get(row.id as string);
    read.onsuccess = () => {
      const merged = read.result ? (mergePatch(read.result, row) as Row) : row;
      store.put(merged);
      versions.put(Number(merged._00_rv) || 0, row.id as string);
    };
  }

  async bulk(table: string, rows: Row[]): Promise<void> {
    const tx = this.d.transaction([table, `${table}__rv`], 'readwrite');
    for (const row of rows) this.merge(tx, table, row);
    await done(tx);
  }

  async put(table: string, row: Row): Promise<void> {
    const tx = this.d.transaction([table, `${table}__rv`], 'readwrite');
    this.merge(tx, table, row);
    await done(tx);
  }

  async userWrite(row: Row, mutationId: string): Promise<void> {
    // `strict`: the transaction resolves once the bytes are on disk.
    const tx = this.d.transaction(['note', 'note__rv', '_00_pending_mutations'], 'readwrite', { durability: 'strict' });
    tx.objectStore('note').put(row);
    tx.objectStore('note__rv').put(0, row.id as string);
    tx.objectStore('_00_pending_mutations').put({
      id: `_00_pending_mutations:${mutationId}`,
      mutationType: 'create',
      recordId: row.id,
      data: { title: row.title, body: row.body, created_ms: row.created_ms },
    });
    await done(tx);
  }

  async get(table: string, id: string): Promise<Row | null> {
    const tx = this.d.transaction(table, 'readonly');
    const req = tx.objectStore(table).get(id);
    await done(tx);
    return (req.result as Row | undefined) ?? null;
  }

  firstQuery(ids: string[]): Promise<Row[]> {
    return this.getMany('game', ids);
  }

  async getMany(table: string, ids: string[], fields?: string[]): Promise<Row[]> {
    const tx = this.d.transaction(table, 'readonly');
    const store = tx.objectStore(table);
    const reqs = ids.map((id) => store.get(id));
    await done(tx);
    const rows = reqs.map((r) => r.result as Row | undefined).filter((r): r is Row => r !== undefined);
    return fields ? rows.map((r) => project(r, fields)) : rows;
  }

  async where(table: string, field: string, value: unknown, orderBy: string, limit: number): Promise<Row[]> {
    const tx = this.d.transaction(table, 'readonly');
    const store = tx.objectStore(table);
    const indexed = store.indexNames.contains(field);
    const req = indexed ? store.index(field).getAll(value as IDBValidKey) : store.getAll();
    await done(tx);
    let rows = req.result as Row[];
    if (!indexed) rows = rows.filter((r) => r[field] === value);
    return rows.sort((a, b) => (b[orderBy] as number) - (a[orderBy] as number)).slice(0, limit);
  }

  async children(table: string, field: string, keys: string[]): Promise<Row[]> {
    const tx = this.d.transaction(table, 'readonly');
    const index = tx.objectStore(table).index(field);
    const reqs = keys.map((key) => index.getAll(key));
    await done(tx);
    return reqs.flatMap((r) => r.result as Row[]);
  }

  async screen(ids: string[]): Promise<Row[]> {
    const tx = this.d.transaction(['game', 'comment', 'user'], 'readonly');
    const games = tx.objectStore('game');
    const users = tx.objectStore('user');
    const byGame = tx.objectStore('comment').index('game');
    const out: Row[] = [];
    for (const id of ids) {
      const read = games.get(id);
      const comments = byGame.getAll(id);
      read.onsuccess = () => {
        const game = read.result as Row | undefined;
        if (!game) return;
        out.push(game);
        const white = users.get(game.white as string);
        white.onsuccess = () => {
          game.white = white.result ?? null;
        };
        comments.onsuccess = () => {
          game.comments = (comments.result as Row[]).sort((a, b) => (a.rank as number) - (b.rank as number)).slice(0, 3);
        };
      };
    }
    await done(tx);
    const position = new Map(ids.map((id, i) => [id, i]));
    return out.sort((a, b) => position.get(a.id as string)! - position.get(b.id as string)!);
  }

  async versions(table: string): Promise<[string, number][]> {
    const tx = this.d.transaction(`${table}__rv`, 'readonly');
    const store = tx.objectStore(`${table}__rv`);
    const keys = store.getAllKeys();
    const values = store.getAll();
    await done(tx);
    return (keys.result as string[]).map((id, i) => [id, values.result[i] as number]);
  }

  async size(): Promise<number | null> {
    const estimate = (await navigator.storage.estimate()) as StorageEstimate & { usageDetails?: { indexedDB?: number } };
    return estimate.usageDetails?.indexedDB ?? null;
  }
}

/** A row store engine in a shared-tabs role, for the two-tab test (`tabs.ts`). */
export const sharedRowsEngine = (followerReads: 'leader' | 'direct'): RowsCacheEngine =>
  new RowsCacheEngine({ store: 'indexeddb', followerReads } as never, logger as never, { useOpfs: true, shared: true });

export const BACKENDS: Record<string, () => Backend> = {
  indexeddb: () => new IdbBackend(),
  sqlite: () =>
    new EngineBackend('sqlite', () => new SqliteCacheEngine({ store: 'indexeddb' } as never, logger as never, { useOpfs: true })),
  rows: () =>
    new EngineBackend('rows', () => new RowsCacheEngine({ store: 'indexeddb' } as never, logger as never, { useOpfs: true })),
};
