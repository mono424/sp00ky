import type { QueryPlan, WhereNode } from '@spooky-sync/query-builder';
import type { EngineStorageDiagnostics } from '../../modules/devtools/storage-info';
import type { StorageHealth } from '../../types';
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
import { ROWS_PROTOCOL, ROWS_READER_OPS, parseRowBytes, type WireOp } from './rows-exec';
import { FLAG_RAW, FLAG_U8, bucketDirName, type StoreStats } from './rows-store';
import {
  ROWS_AHEAD_OPS,
  ROWS_PREWARM_KEY,
  ReadAhead,
  aheadKey,
  jsonSafe,
  takePrewarmed,
  type PrewarmRecord,
} from './rows-prewarm';

export { ROWS_PREWARM_KEY, ROWS_PREWARM_SNIPPET, prewarmRows } from './rows-prewarm';

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

/** How long after connecting a read counts as one the app makes at startup. */
const PAINT_CAPTURE_MS = 3000;

// What the next page load asks the worker ahead of the app (see `rows-prewarm.ts`).
// Bounded: the answers wait in the page's memory until the app asks or writes.
const AHEAD_MAX_READS = 24;
const AHEAD_MAX_REPLY_BYTES = 512 << 10;
const AHEAD_MAX_TOTAL_BYTES = 2 << 20;
const AHEAD_MAX_REQUEST_CHARS = 32_000;
/** How long a read waits for an answer that was asked ahead before asking again itself. */
const AHEAD_WAIT_MS = 250;
/** How long a follower's own reader may take over one read before the leader is asked instead. */
const READER_OP_TIMEOUT_MS = 2000;

/** A timer nothing should wait for (a Node process, a test run). */
function background<T>(timer: T): T {
  (timer as { unref?: () => void }).unref?.();
  return timer;
}

/** Worker ops that leave the store as it was: none of them makes an answer read ahead stale. */
const ROWS_READ_OPS: ReadonlySet<string> = new Set([
  'select',
  'by-ids',
  'get',
  'relation',
  'select-table',
  'count',
  'versions',
  'tables',
  'blob-get',
  'diag',
  'warm',
]);
const PAINT_MAX_KEYS = 64;
const PAINT_MAX_ENTRY_CHARS = 256_000;
const PAINT_FLUSH_MS = 1500;
/** What a kept read starts with: this layout. A copy that starts otherwise is another release's, and is not read. */
const PAINT_FORMAT = '2';

interface PaintEntry {
  tables: string[];
  /** {@link PAINT_FORMAT}, the header (see {@link paintHead}), a newline, then the rows as JSON text. */
  text: string;
  dirty: boolean;
}

/** A table and its mark (see {@link PaintCache}) at the time a read of it was kept. */
type PaintMark = [table: string, mark: string | null];

/** A kept read's header: whether it holds bytes to revive, the marks it was read under, and where its rows start. */
function paintHead(text: string): { u8: boolean; marks: PaintMark[]; body: number } | null {
  if (text[0] !== PAINT_FORMAT) return null;
  const end = text.indexOf('\n');
  if (end < 0) return null;
  try {
    const [u8, marks] = JSON.parse(text.slice(1, end)) as [number, PaintMark[]];
    return Array.isArray(marks) ? { u8: u8 === 1, marks, body: end + 1 } : null;
  } catch {
    return null;
  }
}

function hashKey(text: string): string {
  let a = 0x811c9dc5;
  let b = 0x01000193;
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    a = Math.imul(a ^ c, 0x01000193);
    b = Math.imul(b + c, 0x85ebca6b) ^ (b >>> 13);
  }
  return (a >>> 0).toString(36) + (b >>> 0).toString(36);
}

/**
 * The rows the app read at startup last time, kept in localStorage: the one
 * place a page can read synchronously, before a worker exists or a file is
 * open. {@link RowsCacheEngine.peekByIds} and `peekSelect` answer from it in
 * a fraction of a millisecond, so a screen can paint what it showed before
 * while the store opens.
 *
 * What it holds is a copy, and the contract says so: a peek is something to
 * paint from, the read that follows is the answer.
 *
 * Several tabs keep copies in the same place and write to the same store
 * (shared tabs), so a copy says what it was read under. Every table has a
 * mark in localStorage, and a tab that writes to a table leaves a new one
 * there, once before the write is sent and once after the store has taken it.
 * A copy carries the marks its tables had when it was read and is handed out
 * only while they still stand, whichever tab wrote. The same marks tell a tab
 * that a read of its own was overtaken by another tab's write (see
 * {@link stamp}). What is left is a tab that dies between a write and its
 * second mark, a write given up on (its deadline passed) that the store takes
 * after all, and a browser that dies before a mark reaches the disk: the read
 * that follows the peek then corrects the screen a moment later.
 *
 * A tab that is alone in the store (no shared tabs) leaves no marks: nobody
 * else writes to it, and dropping its own copies as it writes is enough.
 */
export class PaintCache {
  private entries = new Map<string, PaintEntry>();
  /** Reads this session made at startup: the ones worth keeping a copy of, whenever they run again. */
  private known = new Set<string>();
  /** table -> keys, for the entries of earlier sessions and of other tabs too. */
  private byTable: Map<string, Set<string>> | null = null;
  /** table -> writes this tab knows of: its own, and another tab's each time the table's mark was found moved. */
  private writes = new Map<string, number>();
  /** table -> its mark as this tab last read it. */
  private seen = new Map<string, string | null>();
  /** table -> the mark this tab last left there. */
  private mine = new Map<string, string>();
  private readonly tab = Math.random().toString(36).slice(2, 10);
  private marked = 0;
  /** Moves with the bucket, so a read sent under one is never kept under the next. */
  private era = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private bucket = '';
  private since = 0;

  /** `shared`: other tabs write to the same store, so writes are marked for them. */
  constructor(
    private maxChars: number,
    private shared = true
  ) {
    if (typeof addEventListener === 'function') {
      // localStorage is synchronous, so the last state can still be saved as the page goes.
      addEventListener('pagehide', () => this.flush());
    }
  }

  /** The bucket about to be opened: peeks answer from it until `start` says which one it is. */
  expect(bucket: string): void {
    if (!this.bucket) this.bucket = bucket;
  }

  /** A new session on `bucket`: reads from now on are startup reads for a while. */
  start(bucket: string): void {
    if (bucket !== this.bucket) {
      this.flush();
      this.forget();
    }
    this.bucket = bucket;
    this.since = Date.now();
  }

  /**
   * {@link start} for a tab that is handed its store more than once (shared
   * tabs: a new role over the same bucket). Only the first hand-over of a
   * bucket starts a session; what the tab reads after a later one is not what
   * it reads at startup.
   */
  adopt(bucket: string): void {
    if (bucket !== this.bucket || this.since === 0) this.start(bucket);
  }

  /** The store turned out to live in memory: nothing read from it is worth showing next time. */
  stop(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.forget();
    this.bucket = '';
    this.since = 0;
  }

  private forget(): void {
    this.entries.clear();
    this.known.clear();
    this.byTable = null;
    this.seen.clear();
    this.mine.clear();
    this.era++;
  }

  private name(key: string): string {
    return `sp00ky.paint.${this.bucket}.${key}`;
  }

  /** Where `table`'s mark is kept. A key is a hash and has no dot, so the two never meet. */
  private markName(table: string): string {
    return `sp00ky.paint.${this.bucket}.t.${table}`;
  }

  private index(): Map<string, Set<string>> {
    if (this.byTable) return this.byTable;
    this.byTable = new Map();
    this.readIndex();
    return this.byTable;
  }

  /** Take in the keys kept by earlier sessions, and by other tabs since this one last looked. */
  private readIndex(): void {
    try {
      const saved = JSON.parse(localStorage.getItem(`sp00ky.paint.${this.bucket}`) ?? '{}') as Record<string, string[]>;
      for (const [key, tables] of Object.entries(saved)) if (Array.isArray(tables)) this.link(key, tables);
    } catch {
      /* an unreadable index only means nothing gets dropped that is not there */
    }
  }

  private link(key: string, tables: string[]): void {
    const index = this.index();
    for (const table of tables) {
      let keys = index.get(table);
      if (!keys) index.set(table, (keys = new Set()));
      keys.add(key);
    }
  }

  /**
   * Read `table`'s mark. One that moved, and not to what this tab left there,
   * is another tab's write.
   */
  private look(table: string): void {
    let now: string | null = null;
    try {
      now = localStorage.getItem(this.markName(table));
    } catch {
      /* no storage: no other tab to hear from */
    }
    if (now === (this.seen.get(table) ?? null)) return;
    this.seen.set(table, now);
    if (now !== this.mine.get(table)) this.writes.set(table, (this.writes.get(table) ?? 0) + 1);
  }

  /** Leave a new mark on `table`: every copy read under the old one is void, in every tab. */
  private leave(table: string): void {
    if (!this.shared) return;
    // Another tab's write is counted before its mark is covered.
    this.look(table);
    let mark: string | null = `${this.tab}.${++this.marked}`;
    try {
      try {
        localStorage.setItem(this.markName(table), mark);
      } catch {
        // No room for it. The copies are what fills the room and what the mark
        // protects: without them it fits, and there is nothing left to go stale.
        this.empty();
        localStorage.setItem(this.markName(table), mark);
      }
    } catch {
      // Still none (the storage is someone else's, or gone). Taking the old mark away says as much.
      mark = null;
      try {
        localStorage.removeItem(this.markName(table));
      } catch {
        /* no storage: no other tab reads it either */
      }
    }
    if (mark === null) this.mine.delete(table);
    else this.mine.set(table, mark);
    this.seen.set(table, mark);
  }

  /** Every copy kept for this bucket, by any tab, whether the index knows it or not. Marks stay. */
  private copies(): string[] {
    const prefix = `sp00ky.paint.${this.bucket}.`;
    const out: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const name = localStorage.key(i);
      if (name !== null && name.startsWith(prefix) && !name.startsWith(prefix + 't.')) out.push(name.slice(prefix.length));
    }
    return out;
  }

  /** Let go of every copy of this bucket, this tab's and the others'. */
  private empty(): void {
    this.entries.clear();
    this.byTable = new Map();
    try {
      for (const key of this.copies()) localStorage.removeItem(this.name(key));
      localStorage.removeItem(`sp00ky.paint.${this.bucket}`);
    } catch {
      /* no storage */
    }
  }

  /** Whether no tab has written to these tables since they were read. Throws without storage. */
  private holds(marks: PaintMark[]): boolean {
    for (const [table, mark] of marks) if (localStorage.getItem(this.markName(table)) !== mark) return false;
    return true;
  }

  get(key: string, revive: (b64: string) => Uint8Array): Row[] | null {
    if (!this.bucket) return null;
    try {
      // This tab's own copy first. When that one no longer stands, another tab may have kept a newer one.
      const own = this.entries.get(key)?.text;
      for (const text of own ? [own, null] : [null]) {
        const kept = text ?? localStorage.getItem(this.name(key));
        const head = kept ? paintHead(kept) : null;
        if (!kept || !head || !this.holds(head.marks)) continue;
        const rows = kept.slice(head.body);
        if (!head.u8) return JSON.parse(rows) as Row[];
        return JSON.parse(rows, (_k, v) =>
          v && typeof v === 'object' && typeof (v as { __u8?: unknown }).__u8 === 'string' ? revive((v as { __u8: string }).__u8) : v
        ) as Row[];
      }
      return null;
    } catch {
      return null;
    }
  }

  /**
   * What is known of the writes to `tables`: taken when a read is sent,
   * checked when it answers. It moves with every write this tab sends and
   * with every write of another tab that this one has seen the mark of.
   */
  stamp(tables: string[]): string {
    if (!this.bucket) return '';
    let n = 0;
    for (const table of tables) {
      this.look(table);
      n += this.writes.get(table) ?? 0;
    }
    return `${this.era}.${n}`;
  }

  /**
   * A read just answered: keep its text if it is a startup read, or one kept
   * before. `sentAt` is {@link stamp} from when it was sent; a write to its
   * tables since then means the answer may already be old, and it is let go.
   */
  note(key: string, tables: string[], rows: string, u8: boolean, sentAt: string): void {
    if (!this.bucket || sentAt !== this.stamp(tables)) return;
    if (!this.known.has(key)) {
      if (Date.now() - this.since > PAINT_CAPTURE_MS || this.known.size >= PAINT_MAX_KEYS) return;
      this.known.add(key);
    }
    if (rows.length > PAINT_MAX_ENTRY_CHARS) return;
    // `stamp` has just read every one of these marks.
    const head = JSON.stringify([u8 ? 1 : 0, tables.map((table): PaintMark => [table, this.seen.get(table) ?? null])]);
    this.entries.set(key, { tables, text: `${PAINT_FORMAT}${head}\n${rows}`, dirty: true });
    this.link(key, tables);
    this.timer ??= setTimeout(() => this.flush(), PAINT_FLUSH_MS);
  }

  /**
   * This tab writes to `tables`: what was read from them is no longer what
   * they hold. Called as the write is sent, and again with `done` once the
   * store has answered it. The first call drops this tab's copies and fails
   * its reads still on their way. The second only leaves a new mark: a tab
   * that read between the two (it saw the first mark, the store had not yet
   * taken the write) holds a copy that mark makes void, while this tab's own
   * reads, queued behind its write and answered after it, are kept.
   */
  touch(tables: Iterable<string>, done = false): void {
    if (!this.bucket) return;
    const index = done ? null : this.index();
    for (const table of tables) {
      this.leave(table);
      if (!index) continue;
      this.writes.set(table, (this.writes.get(table) ?? 0) + 1);
      const keys = index.get(table);
      if (!keys || keys.size === 0) continue;
      for (const key of keys) {
        this.entries.delete(key);
        try {
          localStorage.removeItem(this.name(key));
        } catch {
          /* ignore */
        }
      }
      keys.clear();
    }
  }

  /**
   * Write what changed, newest reads first, up to the budget, and clear out
   * what no longer stands: the budget is one for all tabs of the bucket.
   */
  flush(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (!this.bucket) return;
    const known = this.byTable !== null;
    const index = this.index();
    if (known) this.readIndex();
    try {
      let used = 0;
      const kept: Record<string, string[]> = {};
      for (const [key, entry] of [...this.entries].reverse()) {
        // Written to by another tab since: whatever sits under the key now is judged below.
        if (!this.holds(paintHead(entry.text)?.marks ?? [])) {
          this.entries.delete(key);
          continue;
        }
        if (used + entry.text.length > this.maxChars) {
          this.entries.delete(key);
          localStorage.removeItem(this.name(key));
          for (const keys of index.values()) keys.delete(key);
          continue;
        }
        if (entry.dirty) {
          try {
            localStorage.setItem(this.name(key), entry.text);
            entry.dirty = false;
          } catch {
            // No room for this one. It must not cost the others their place in the index.
            this.entries.delete(key);
            localStorage.removeItem(this.name(key));
            for (const keys of index.values()) keys.delete(key);
            continue;
          }
        }
        used += entry.text.length;
        kept[key] = entry.tables;
      }
      // Copies this tab does not hold: other tabs', earlier sessions' that this
      // one never read again, and any the index lost track of (two tabs saving
      // at once), which nothing else would ever let go of.
      const others = new Set<string>();
      for (const keys of index.values()) for (const key of keys) if (!(key in kept)) others.add(key);
      // Without marks nothing says whether such a stray is still true: it goes.
      const strays = new Set<string>();
      for (const key of this.copies()) {
        if (key in kept || others.has(key)) continue;
        if (this.shared) others.add(key);
        else strays.add(key);
      }
      for (const key of strays) localStorage.removeItem(this.name(key));
      for (const key of others) {
        const text = localStorage.getItem(this.name(key));
        const head = text ? paintHead(text) : null;
        if (!text || !head || !this.holds(head.marks) || used + text.length > this.maxChars) {
          if (text) localStorage.removeItem(this.name(key));
          for (const keys of index.values()) keys.delete(key);
          continue;
        }
        used += text.length;
        const tables = head.marks.map((mark) => mark[0]);
        kept[key] = tables;
        this.link(key, tables);
      }
      localStorage.setItem(`sp00ky.paint.${this.bucket}`, JSON.stringify(kept));
    } catch {
      // Out of quota, or no storage: painting from the cache is a bonus, not a need.
    }
  }

  /** Forget everything kept for `bucket`. */
  static drop(bucket: string): void {
    try {
      const prefix = `sp00ky.paint.${bucket}`;
      const mine: string[] = [];
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key !== null && (key === prefix || key.startsWith(prefix + '.'))) mine.push(key);
      }
      for (const key of mine) localStorage.removeItem(key);
    } catch {
      /* ignore */
    }
  }
}

/** Every table a plan reads: its own and its relations'. */
function tablesOf(plan: QueryPlan): string[] {
  const out = new Set<string>([plan.table]);
  const walk = (relations: QueryPlan['relations']): void => {
    for (const relation of relations ?? []) {
      out.add(relation.table);
      walk(relation.relations);
    }
  };
  walk(plan.relations);
  return [...out];
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

type RowsReply = { buf: ArrayBuffer | null; u8?: boolean };

const byIdsKey = (table: string, ids: Id[], opts?: { select?: string[]; orderBy?: OrderBy }): string =>
  hashKey('i' + JSON.stringify([table, ids.map(stableKey), opts?.select ?? null, opts?.orderBy ?? null]));

function parseRowText(text: string, u8: boolean): Row[] {
  if (!u8) return JSON.parse(text) as Row[];
  return JSON.parse(text, (_k, v) =>
    v && typeof v === 'object' && typeof (v as { __u8?: unknown }).__u8 === 'string' ? fromBase64((v as { __u8: string }).__u8) : v
  ) as Row[];
}
type OpenReply = { persisted: boolean; opfsError?: string; proto?: number; workerUrl?: string };

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
      wire.push(['put', table, key, rv, text.includes('"__u8"') ? FLAG_U8 : 0, off, len]);
      continue;
    }
    const { insert, patch } = serializeMergeRow(row);
    const [off, len] = append(insert);
    // The patch differs from the row only when a field is being cleared.
    const [patchOff, patchLen] = patch === insert ? [0, 0] : append(patch);
    wire.push(['merge', table, key, rv, insert.includes('"__u8"') ? FLAG_U8 : 0, off, len, patchOff, patchLen]);
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
  /** The transport in use wraps a worker `prewarmRows` started. */
  private adopted = false;
  /** Drops what the adopted worker read ahead of the app, while any of it is still held. */
  private forgetAhead: (() => void) | null = null;
  /** Last-seen startup reads, for {@link peekByIds} / {@link peekSelect}. Off for a memory store. */
  private paint: PaintCache | null =
    this.useOpfs && this.config.paintCache !== false && typeof localStorage !== 'undefined' ? new PaintCache(1_000_000, this.shared) : null;

  protected override createTransport(): SqliteTransport {
    const pre = takePrewarmed();
    if (pre?.failed) pre.worker.terminate();
    this.adopted = pre !== null && !pre.failed;
    const transport = new WorkerSqliteTransport(
      this.logger,
      this.adopted
        ? () => pre!.worker
        : // Same shape as the SQLite worker's URL: `.ts` for bundlers that build
          // from source, rewritten to `./rows-worker.js` in the published build.
          (this.config.workers?.rows ?? (() => new Worker(new URL('./rows-worker.ts', import.meta.url), { type: 'module' })))
    );
    // What the worker was asked ahead of the app, for as long as it holds true.
    let ahead = this.adopted ? new ReadAhead(pre!, AHEAD_WAIT_MS) : null;
    const forget = (): void => {
      ahead?.drop();
      ahead = null;
    };
    this.forgetAhead = forget;
    if (ahead) background(setTimeout(forget, PAINT_CAPTURE_MS));
    let opens = 0;
    // Every open is checked: a worker from another release must not be talked to.
    const call = transport.call.bind(transport);
    transport.call = async <T>(type: string, payload?: unknown, transfer?: Transferable[]): Promise<T> => {
      if (type === 'open') {
        // The prewarm's work stands only for the first open, of the store it
        // was started on. Under a role's lock too (a shared-tabs leader): the
        // worker keeps the store it has open, and until a follower's port is
        // handed to it (`exposeClientPort`) nobody else writes through it.
        const asked = payload as { dbName: string; useOpfs: boolean; workerLockName?: string; share?: boolean };
        // A leader whose followers read the file themselves opens it for sharing.
        if (asked.workerLockName && asked.useOpfs && this.config.followerReads === 'direct') {
          asked.share = true;
        }
        const mine = ahead !== null && opens++ === 0 && asked.useOpfs && asked.dbName === pre!.bucket;
        const answered = pre?.opened as (OpenReply & { ok?: boolean }) | undefined;
        if (pre) pre.opened = undefined;
        if (!mine) forget();
        else if (answered?.persisted && !asked.workerLockName && !this.shared) {
          // The worker answered this very open already: no need to ask again.
          // Never with shared tabs: there the open is also what takes the
          // role's lock, and what tells the worker that its store, which it
          // would give up to another tab's leader, has an owner.
          this.opened(transport, asked, answered);
          return answered as T;
        }
      } else if (ahead && ROWS_AHEAD_OPS.has(type)) {
        const early = ahead.take(type, payload);
        const got = early ? await early : null;
        if (got) {
          const { id: _id, ok: _ok, error: _error, ...rest } = got;
          return rest as T;
        }
      } else if (!ROWS_READ_OPS.has(type)) forget();
      const reply = await call<T>(type, payload, transfer);
      if (type === 'open') this.opened(transport, payload as { dbName: string; useOpfs: boolean }, reply as OpenReply);
      return reply;
    };
    return transport;
  }

  private opened(transport: SqliteTransport, asked: { dbName: string; useOpfs: boolean }, reply: OpenReply): void {
    if (reply.proto !== ROWS_PROTOCOL) {
      transport.close('row worker speaks another protocol');
      throw new Error(`RowsCacheEngine: worker protocol ${String(reply.proto)}, expected ${ROWS_PROTOCOL}`);
    }
    // Leave the next page load what it needs to start this worker ahead of the app.
    try {
      if (asked.useOpfs && reply.persisted && reply.workerUrl) {
        const old = JSON.parse(localStorage.getItem(ROWS_PREWARM_KEY) ?? 'null') as Partial<PrewarmRecord> | null;
        const record: PrewarmRecord = { url: reply.workerUrl, bucket: asked.dbName };
        // The reads to ask ahead belong to a bucket: another user's are not kept.
        if (old?.bucket === asked.dbName && Array.isArray(old.reads)) record.reads = old.reads;
        localStorage.setItem(ROWS_PREWARM_KEY, JSON.stringify(record));
      }
    } catch {
      // No localStorage (a worker, a locked-down page): nothing to prewarm from.
    }
  }

  /**
   * A prewarmed worker is a guess. When it turns out wrong (it failed to
   * load, or it is another release's), drop it and connect again with a
   * worker of our own.
   */
  override async connect(bucketId: string): Promise<void> {
    // Known before the worker answers, so a peek can be asked for right away.
    this.bucketId = bucketId;
    this.paint?.start(bucketId);
    try {
      await super.connect(bucketId);
      this.sessionBegan();
    } catch (error) {
      if (!this.adopted) throw error;
      this.adopted = false;
      try {
        localStorage.removeItem(ROWS_PREWARM_KEY);
      } catch {
        /* ignore */
      }
      this.logger.warn(
        { error, Category: 'sp00ky-client::RowsCacheEngine::connect' },
        'Prewarmed row worker was unusable; starting a fresh one'
      );
      await this.close();
      await super.connect(bucketId);
      this.sessionBegan();
    }
  }

  override async switchBucket(bucketId: string): Promise<void> {
    this.dropReader('bucket switch');
    await super.switchBucket(bucketId);
    this.paint?.start(bucketId);
    this.sessionBegan();
  }

  /**
   * The store is open. One that fell back to memory keeps no copies: its rows
   * are gone with the tab, and it must not write over what a tab with the
   * real store has kept.
   */
  private sessionBegan(): void {
    if (this.useOpfs && this.storageHealth.status !== 'persistent') {
      this.paint?.stop();
      return;
    }
    this.beginStartupReads();
  }

  /** A follower reads through the leader's worker; one started ahead for this tab is not needed. */
  override async adoptAttached(
    dbPort: MessagePort,
    snapshot: { bucketId: string; storageHealth: StorageHealth },
    onPortDead: (reason: string) => void
  ): Promise<void> {
    takePrewarmed()?.worker.terminate();
    this.dropReader('attaching to a leader');
    const durable = this.useOpfs && snapshot.storageHealth.status === 'persistent';
    // Before the port is in use, as in `connect`: a peek needs no store.
    if (durable) this.paint?.adopt(snapshot.bucketId);
    await super.adoptAttached(dbPort, snapshot, onPortDead);
    this.sessionBegan();
    if (this.config.followerReads === 'direct' && durable) await this.startReader(snapshot.bucketId);
  }

  // ---- a follower's own reader ------------------------------------------------
  // With `followerReads: 'direct'` a follower answers its reads from a worker
  // of its own, on a reader of the store's file (`RowStore` reader mode). The
  // reader is an optimization and nothing depends on it: when it cannot be
  // had, or fails, the read goes to the leader as it would have.

  /** The follower's reader worker, while this engine follows and has one. */
  private reader: SqliteTransport | null = null;

  private async startReader(bucketId: string): Promise<void> {
    let transport: SqliteTransport | null = null;
    // Every change of role bumps the epoch: the same epoch later means still this follower.
    const asFollower = this.epoch;
    try {
      transport = new WorkerSqliteTransport(
        this.logger,
        this.config.workers?.rows ?? (() => new Worker(new URL('./rows-worker.ts', import.meta.url), { type: 'module' }))
      );
      const reply = await transport.call<OpenReply>('open', {
        dbName: bucketId,
        useOpfs: true,
        reader: true,
        openOptions: { maxAttempts: 2, disallowMemoryFallback: true },
      });
      if (reply.proto !== ROWS_PROTOCOL || !reply.persisted) throw new Error('reader worker is not this release\'s');
      // The role may have moved on while the worker started.
      if (this.epoch !== asFollower || this.bucketId !== bucketId) throw new Error('no longer a follower of this store');
      this.reader = transport;
    } catch (error) {
      transport?.close('reader unavailable');
      this.logger.info(
        { error: String(error), Category: 'sp00ky-client::RowsCacheEngine::reader' },
        'No direct reads for this follower tab; it reads through the leader'
      );
    }
  }

  private dropReader(reason: string): void {
    const reader = this.reader;
    this.reader = null;
    reader?.close(reason);
  }

  /** True while this follower answers its reads from the store's file itself. */
  get readsDirectly(): boolean {
    return this.reader !== null;
  }

  protected override rawCall<T = unknown>(type: string, payload?: unknown, transfer?: Transferable[]): Promise<T> {
    const reader = this.reader;
    if (!reader || !ROWS_READER_OPS.has(type)) return super.rawCall<T>(type, payload, transfer);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error('reader timed out')), READER_OP_TIMEOUT_MS);
    });
    return Promise.race([reader.call<T>(type, payload), late])
      .catch((error: unknown) => {
        if (this.reader === reader) {
          this.dropReader('read failed');
          this.logger.warn(
            { error: String(error), type, Category: 'sp00ky-client::RowsCacheEngine::reader' },
            'Direct read failed; this tab reads through the leader from here on'
          );
        }
        return super.rawCall<T>(type, payload, transfer);
      })
      .finally(() => clearTimeout(timer));
  }

  /**
   * A leader's store is this tab's own worker, as alone: one started ahead of
   * the app is adopted (`createTransport`), and replaced when it turns out to
   * be another release's.
   */
  override async adoptOwner(...args: Parameters<SqliteCacheEngine['adoptOwner']>): ReturnType<SqliteCacheEngine['adoptOwner']> {
    this.dropReader('becoming the leader');
    this.paint?.adopt(args[0]);
    let health: StorageHealth;
    try {
      health = await super.adoptOwner(...args);
    } catch (error) {
      // Only when the worker itself is the trouble: a lock or a file that
      // cannot be had would fail a second worker the same way.
      if (!this.adopted || !String(error).includes('worker protocol')) throw error;
      this.adopted = false;
      try {
        localStorage.removeItem(ROWS_PREWARM_KEY);
      } catch {
        /* ignore */
      }
      this.logger.warn(
        { error, Category: 'sp00ky-client::RowsCacheEngine::adoptOwner' },
        'Prewarmed row worker was unusable; starting a fresh one'
      );
      health = await super.adoptOwner(...args);
    }
    this.sessionBegan();
    return health;
  }

  /** Another tab is about to write through this worker: what it read ahead of the app no longer stands. */
  override async exposeClientPort(clientId: string, port: MessagePort): Promise<void> {
    this.forgetAhead?.();
    return super.exposeClientPort(clientId, port);
  }

  override async releaseOwnership(): Promise<void> {
    this.dropReader('releasing the store');
    return super.releaseOwnership();
  }

  override onLeaderLost(reason: string): void {
    // The file is still there and still whole, but writes have nowhere to go:
    // read through the next leader, with a reader started against it.
    this.dropReader('leader lost');
    super.onLeaderLost(reason);
  }

  override async close(): Promise<void> {
    this.dropReader('engine closed');
    return super.close();
  }

  /** Tables need no creating: a table is the rows written to it. */
  protected override async ensureTable(): Promise<void> {}

  // ---- read ahead -----------------------------------------------------------
  // The reads of a session's first seconds are saved, and the next page load's
  // prewarm asks them again before the app exists (`rows-prewarm.ts`).

  private startupReads: Map<string, [string, unknown]> | null = null;
  private startupBytes = 0;
  private startupUntil = 0;
  private startupTimer: ReturnType<typeof setTimeout> | null = null;
  private startupHooked = false;
  /** The bucket whose startup reads were last begun. */
  private startupBucket: string | null = null;

  private beginStartupReads(): void {
    if (!this.useOpfs || typeof localStorage === 'undefined') return;
    // With shared tabs a tab is handed its store again with every change of
    // role. Only the first time is a session's start.
    if (this.shared && this.startupBucket === this.bucketId) return;
    this.startupBucket = this.bucketId;
    if (!this.startupHooked && typeof addEventListener === 'function') {
      // localStorage is synchronous, so the list can still be saved as the page goes.
      addEventListener('pagehide', () => this.saveStartupReads());
      this.startupHooked = true;
    }
    this.startupReads = new Map();
    this.startupBytes = 0;
    this.startupUntil = performance.now() + PAINT_CAPTURE_MS;
    if (this.startupTimer) clearTimeout(this.startupTimer);
    this.startupTimer = background(setTimeout(() => this.saveStartupReads(), PAINT_CAPTURE_MS + 50));
  }

  private noteStartupRead(type: string, payload: unknown, replyBytes: number): void {
    const reads = this.startupReads;
    if (!reads || performance.now() > this.startupUntil || !ROWS_AHEAD_OPS.has(type)) return;
    if (reads.size >= AHEAD_MAX_READS || replyBytes > AHEAD_MAX_REPLY_BYTES) return;
    if (this.startupBytes + replyBytes > AHEAD_MAX_TOTAL_BYTES || !jsonSafe(payload)) return;
    const key = aheadKey(type, payload);
    if (key.length > AHEAD_MAX_REQUEST_CHARS || reads.has(key)) return;
    reads.set(key, [type, payload]);
    this.startupBytes += replyBytes;
  }

  /** Leave this session's startup reads for the next page load. Also runs as the page goes. */
  private saveStartupReads(): void {
    const reads = this.startupReads;
    if (!reads) return;
    try {
      const saved = JSON.parse(localStorage.getItem(ROWS_PREWARM_KEY) ?? 'null') as Partial<PrewarmRecord> | null;
      if (!saved?.url || saved.bucket !== this.bucketId) return;
      localStorage.setItem(ROWS_PREWARM_KEY, JSON.stringify({ url: saved.url, bucket: saved.bucket, reads: [...reads.values()] }));
    } catch {
      // No storage, or no room: the next load asks nothing ahead.
    }
  }

  /** A read that answers rows. `paint` names it for the paint cache, when it is worth keeping. */
  private async rows(type: string, payload: unknown, paint?: { key: string; tables: string[] }): Promise<Row[]> {
    const sentAt = paint && this.paint ? this.paint.stamp(paint.tables) : '';
    const reply = await this.call<RowsReply>(type, payload);
    this.noteStartupRead(type, payload, reply.buf?.byteLength ?? 0);
    if (!reply.buf) return [];
    const text = decoder.decode(new Uint8Array(reply.buf));
    if (paint) this.paint?.note(paint.key, paint.tables, text, reply.u8 === true, sentAt);
    return parseRowText(text, reply.u8 === true);
  }

  // ---- reads ---------------------------------------------------------------

  private selectPayload(plan: QueryPlan, params: Record<string, unknown>): { plan: QueryPlan; params: Record<string, unknown> } {
    // Class instances (RecordId) do not survive structured clone; see the
    // SQLite engine's `select` for why the KEYS of `params` must all stay.
    const normParams: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(params)) normParams[k] = toCloneSafe(v);
    return { plan: normalizePlanForClone(plan), params: normParams };
  }

  /**
   * The paint cache's name for a select. A caller that reads the same thing
   * again and again under changing arguments (a query whose id set moves)
   * names it itself with `paintKey`, so the copy kept is always its latest
   * answer; without one a read is told apart by its plan and parameters.
   */
  private selectKey(payload: unknown, opts?: { paintKey?: string }): string {
    return opts?.paintKey === undefined ? hashKey('s' + JSON.stringify(payload)) : hashKey('q' + opts.paintKey);
  }

  override async select(plan: QueryPlan, params: Record<string, unknown> = {}, opts?: { paintKey?: string }): Promise<Row[]> {
    const payload = this.selectPayload(plan, params);
    return this.rows('select', payload, this.paint ? { key: this.selectKey(payload, opts), tables: tablesOf(plan) } : undefined);
  }

  /**
   * What `select(plan, params)` answered the last time the app asked at
   * startup, synchronously, or `null`. Available as soon as `connect` was
   * CALLED (or a role was handed over, or `expectBucket` said which bucket):
   * it reads localStorage, not the store. Paint from it, then run the real
   * `select`, which is the answer. A copy is not handed out once any tab has
   * written to a table it read (see {@link PaintCache}).
   */
  peekSelect(plan: QueryPlan, params: Record<string, unknown> = {}, opts?: { paintKey?: string }): Row[] | null {
    if (!this.paint) return null;
    // A named read needs no payload to find its copy.
    const payload = opts?.paintKey === undefined ? this.selectPayload(plan, params) : null;
    return this.paint.get(this.selectKey(payload, opts), fromBase64);
  }

  /** See `LocalStore.expectBucket`. */
  expectBucket(bucketId: string): void {
    this.paint?.expect(bucketId);
  }

  /** {@link peekSelect} for `selectByIds`. */
  peekByIds(table: string, ids: Id[], opts?: { select?: string[]; orderBy?: OrderBy }): Row[] | null {
    return this.paint?.get(byIdsKey(table, ids, opts), fromBase64) ?? null;
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
    return this.rows(
      'by-ids',
      { table, ids: ids.map(stableKey), select: opts?.select, orderBy: opts?.orderBy },
      this.paint ? { key: byIdsKey(table, ids, opts), tables: [table] } : undefined
    );
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
    this.paint?.touch(plan.changed);
    try {
      if (plan.changed.length > 0) await this.call('reset-versions', { tables: plan.changed, keep: [...keep] });
    } finally {
      this.paint?.touch(plan.changed, true);
    }
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
    const tables = new Set(items.map((item) => item.table));
    this.paint?.touch(tables);
    try {
      await this.call('write', { ops: wire, buf, sync }, buf ? [buf] : undefined);
    } finally {
      // Whether or not it was taken: other tabs are told once more, now that it is over.
      this.paint?.touch(tables, true);
    }
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
    PaintCache.drop(bucketId);
    try {
      // Nothing to start ahead for a store that is gone.
      const saved = JSON.parse(localStorage.getItem(ROWS_PREWARM_KEY) ?? 'null') as Partial<PrewarmRecord> | null;
      if (saved?.bucket === bucketId) localStorage.removeItem(ROWS_PREWARM_KEY);
    } catch {
      /* no storage */
    }
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
