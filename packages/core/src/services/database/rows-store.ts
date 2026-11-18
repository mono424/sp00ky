/**
 * The row store: sp00ky's own storage engine for the local cache.
 *
 * One append-only file per bucket, read and written through a synchronous
 * random-access handle (an OPFS `FileSystemSyncAccessHandle` in a worker, a
 * byte array in tests and in memory mode). It stores what the client keeps,
 * rows addressed by `(table, id)` with a version, and nothing a general
 * database needs beyond that: no SQL, no pages, no B-tree.
 *
 * # Layout
 *
 * ```text
 * 0      superblock slot 0     two slots written alternately; the valid one
 * 256    superblock slot 1     with the higher seq wins, so a torn write of
 *                              one never loses the other
 * 4096   extents               [u32 type][u32 len][u32 hash][u32 0] payload,
 *                              padded to 8 bytes
 * ```
 *
 * Three kinds of extent:
 * - a **batch**: the records of one atomic write. A batch counts only when it
 *   lies whole inside the file and its hash matches, which is what makes a
 *   multi-row write all-or-nothing without a journal;
 * - a **table block**: one table's index, sorted by id, as flat typed arrays;
 * - a **directory**: where every table's block lies.
 *
 * The superblock names the latest directory and the offset it covers. Opening
 * the store reads the superblock and the directory, then replays the batches
 * written after that offset: the cost follows the writes since the last
 * checkpoint, never the size of the store. A table's block is read on first
 * touch, in one read, and used in place: its arrays are views on the bytes,
 * so nothing is parsed per row.
 *
 * # Index
 *
 * Per table, an immutable sorted run plus a map of the writes since it was
 * built. A point read checks the map, then binary-searches the run. Ordered
 * reads fold the map into a new run first. A checkpoint appends the folded
 * runs and a directory and flips the superblock.
 *
 * # Bodies
 *
 * A body is opaque bytes to this module. The client stores JSON text, because
 * the engine's native parser outruns any decoder written in JavaScript; see
 * `rows-exec.ts`.
 *
 * # Trust
 *
 * The log is the truth and the index is a cache of it: a missing, torn or
 * mismatching index is rebuilt by scanning the log. A torn tail is cut off at
 * the last whole batch. Space freed by updates and deletes comes back through
 * {@link RowStore.compact}, which copies the live rows into the second file
 * and switches over with one superblock write.
 */

/** Synchronous random-access file. */
export interface SyncFile {
  read(buffer: Uint8Array, at: number): number;
  write(buffer: Uint8Array, at: number): number;
  size(): number;
  truncate(size: number): void;
  flush(): void;
  close(): void;
}

/** Opens the store's two files by name. `b` is only asked for when needed. */
export interface StoreIo {
  open(name: 'a' | 'b'): Promise<SyncFile> | SyncFile;
}

/** Where one row's body lies, and what the index knows without reading it. */
export interface Entry {
  off: number;
  len: number;
  rv: number;
  flags: number;
}

export type StoreOp =
  | { t: 'put'; table: string; id: string; rv: number; flags: number; body: Uint8Array }
  | { t: 'del'; table: string; id: string }
  | { t: 'clear'; table: string };

/** A table's rows in id order: parallel arrays, plus the ids as one heap. */
export interface Run {
  n: number;
  off: Float64Array;
  rv: Float64Array;
  len: Uint32Array;
  idEnd: Uint32Array;
  flags: Uint8Array;
  heap: Uint8Array;
  /** The bytes the arrays are views on; what a checkpoint writes. */
  buf: Uint8Array;
  /** Sum of the body lengths. */
  live: number;
  /** Row positions ordered by file offset, built on first scan. */
  byOff?: Uint32Array;
}

export interface StoreStats {
  file: 'a' | 'b';
  fileBytes: number;
  liveBytes: number;
  indexBytes: number;
  garbageBytes: number;
  tailBytes: number;
  /** Records the next open would replay. */
  tailOps: number;
  tables: number;
  rebuilt: boolean;
}

/** The OPFS directory that holds a bucket's two files. */
export const bucketDirName = (bucketId: string): string =>
  `sp00ky-rows-${bucketId.replace(/[^A-Za-z0-9_.-]/g, '_')}`;

/** Body holds `{"__u8": base64}` tags the reader has to turn back into bytes. */
export const FLAG_U8 = 1;
/** Body is raw bytes, not JSON. */
export const FLAG_RAW = 2;

const MAGIC = 0x524b5053; // "SPKR"
const VERSION = 1;
const SB_SIZE = 256;
const SB_USED = 64;
const HEADER_END = 4096;
const XH = 16;
const X_BATCH = 1;
const X_BLOCK = 2;
const X_DIR = 3;
const R_PUT = 1;
const R_DEL = 2;
const R_CLEAR = 3;
const R_TABLE = 4;
const ACTIVE = 1;
const MOVED = 2;
/** Bytes per row in a block, besides its id. */
const ROW_BYTES = 25;
/** Bytes a put record adds around its id and body. */
const RECORD_BYTES = 18;
const MAX_ID_BYTES = 0xffff;
const COMPACT_CHUNK = 4 << 20;
/** A batch buffer larger than this is dropped after the write instead of kept. */
const SCRATCH_KEEP = 8 << 20;

const LITTLE_ENDIAN = new Uint8Array(new Uint32Array([1]).buffer)[0] === 1;
const encoder = new TextEncoder();
const decoder = new TextDecoder();
const align8 = (n: number): number => (n + 7) & ~7;

/**
 * 32-bit hash of a byte range. Not cryptographic: it tells a whole write from
 * a torn one. Reads a word at a time where the bytes allow it.
 */
export function hash32(buf: Uint8Array, start = 0, end = buf.length): number {
  let h = (0x9e3779b1 ^ (end - start)) | 0;
  let i = start;
  const words = (end - start) >>> 2;
  if (LITTLE_ENDIAN && ((buf.byteOffset + start) & 3) === 0) {
    const w = new Uint32Array(buf.buffer, buf.byteOffset + start, words);
    for (let k = 0; k < words; k++) {
      h = Math.imul(h ^ w[k], 0x85ebca6b);
      h = (h << 13) | (h >>> 19);
    }
    i = start + words * 4;
  } else {
    for (const stop = start + words * 4; i < stop; i += 4) {
      const x = buf[i] | (buf[i + 1] << 8) | (buf[i + 2] << 16) | (buf[i + 3] << 24);
      h = Math.imul(h ^ x, 0x85ebca6b);
      h = (h << 13) | (h >>> 19);
    }
  }
  for (; i < end; i++) {
    h = Math.imul(h ^ buf[i], 0xc2b2ae35);
    h = (h << 7) | (h >>> 25);
  }
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

/** Order of two byte ranges, as `memcmp` then length. */
function cmpBytes(
  a: Uint8Array,
  aStart: number,
  aEnd: number,
  b: Uint8Array,
  bStart: number,
  bEnd: number
): number {
  const n = Math.min(aEnd - aStart, bEnd - bStart);
  for (let i = 0; i < n; i++) {
    const d = a[aStart + i] - b[bStart + i];
    if (d !== 0) return d;
  }
  return aEnd - aStart - (bEnd - bStart);
}

/**
 * Order of two strings by code point, which is the order of their UTF-8
 * bytes. JavaScript's own comparison is by UTF-16 unit and puts a surrogate
 * pair below U+E000, where UTF-8 puts it above.
 */
export function cmpText(a: string, b: string): number {
  if (a === b) return 0;
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    let x = a.charCodeAt(i);
    let y = b.charCodeAt(i);
    if (x === y) continue;
    if (x >= 0xd800 && y >= 0xd800) {
      x = x >= 0xe000 ? x - 0x800 : x + 0x2000;
      y = y >= 0xe000 ? y - 0x800 : y + 0x2000;
    }
    return x - y;
  }
  return a.length - b.length;
}

// oxlint-disable-next-line no-control-regex
const NON_ASCII = /[^\x00-\x7f]/;

function emptyRun(): Run {
  return runFromBuffer(new Uint8Array(0), 0, 0, 0);
}

function runFromBuffer(buf: Uint8Array, n: number, heapLen: number, live: number): Run {
  const b = buf.buffer;
  const o = buf.byteOffset;
  return {
    n,
    off: new Float64Array(b, o, n),
    rv: new Float64Array(b, o + 8 * n, n),
    len: new Uint32Array(b, o + 16 * n, n),
    idEnd: new Uint32Array(b, o + 20 * n, n),
    flags: new Uint8Array(b, o + 24 * n, n),
    heap: new Uint8Array(b, o + ROW_BYTES * n, heapLen),
    buf,
    live,
  };
}

const idStart = (run: Run, i: number): number => (i === 0 ? 0 : run.idEnd[i - 1]);

/** The id of row `i` of a run. */
export function runId(run: Run, i: number): string {
  return decoder.decode(run.heap.subarray(idStart(run, i), run.idEnd[i]));
}

let probe = new Uint8Array(256);

/** UTF-8 of `id` in the shared probe buffer; returns its length. */
function encodeProbe(id: string): number {
  if (probe.length < id.length * 3) probe = new Uint8Array(id.length * 3);
  return encoder.encodeInto(id, probe).written;
}

function runFind(run: Run, id: string): number {
  if (run.n === 0) return -1;
  const len = encodeProbe(id);
  let lo = 0;
  let hi = run.n - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const c = cmpBytes(run.heap, idStart(run, mid), run.idEnd[mid], probe, 0, len);
    if (c === 0) return mid;
    if (c < 0) lo = mid + 1;
    else hi = mid - 1;
  }
  return -1;
}

/** `base` with `delta` applied: overrides replace, `null` removes. */
function mergeRun(base: Run, delta: Map<string, Entry | null>): Run {
  const keys = [...delta.keys()];
  let ascii = true;
  let chars = 0;
  for (const k of keys) {
    chars += k.length;
    if (ascii && NON_ASCII.test(k)) ascii = false;
  }
  if (ascii) keys.sort();
  else keys.sort(cmpText);

  const upper = base.n + keys.length;
  const off = new Float64Array(upper);
  const rv = new Float64Array(upper);
  const len = new Uint32Array(upper);
  const idEnd = new Uint32Array(upper);
  const flags = new Uint8Array(upper);
  const heap = new Uint8Array(base.heap.length + chars * 3);
  let n = 0;
  let h = 0;
  let live = 0;
  let i = 0;
  let j = 0;
  let keyLen = keys.length > 0 ? encodeProbe(keys[0]) : 0;
  while (i < base.n || j < keys.length) {
    let c: number;
    if (i >= base.n) c = 1;
    else if (j >= keys.length) c = -1;
    else c = cmpBytes(base.heap, idStart(base, i), base.idEnd[i], probe, 0, keyLen);
    if (c < 0) {
      const s = idStart(base, i);
      const e = base.idEnd[i];
      heap.set(base.heap.subarray(s, e), h);
      h += e - s;
      off[n] = base.off[i];
      rv[n] = base.rv[i];
      len[n] = base.len[i];
      flags[n] = base.flags[i];
      idEnd[n] = h;
      live += base.len[i];
      n++;
      i++;
      continue;
    }
    if (c === 0) i++;
    const entry = delta.get(keys[j]);
    if (entry) {
      heap.set(probe.subarray(0, keyLen), h);
      h += keyLen;
      off[n] = entry.off;
      rv[n] = entry.rv;
      len[n] = entry.len;
      flags[n] = entry.flags;
      idEnd[n] = h;
      live += entry.len;
      n++;
    }
    j++;
    if (j < keys.length) keyLen = encodeProbe(keys[j]);
  }

  const buf = new Uint8Array(new ArrayBuffer(ROW_BYTES * n + h));
  const run = runFromBuffer(buf, n, h, live);
  run.off.set(off.subarray(0, n));
  run.rv.set(rv.subarray(0, n));
  run.len.set(len.subarray(0, n));
  run.idEnd.set(idEnd.subarray(0, n));
  run.flags.set(flags.subarray(0, n));
  run.heap.set(heap.subarray(0, h));
  return run;
}

interface Block {
  off: number;
  len: number;
  n: number;
  heap: number;
  sum: number;
  live: number;
}

interface Table {
  name: string;
  tid: number;
  /** The persisted run, when there is one. */
  block: Block | null;
  /** The loaded run; `null` until the table is first read. */
  run: Run | null;
  /** Writes since `run` was built. */
  delta: Map<string, Entry | null>;
  /** `run` is newer than `block`. */
  unsaved: boolean;
}

interface Superblock {
  seq: number;
  state: number;
  dirOff: number;
  dirLen: number;
  dirSum: number;
  covered: number;
  gen: number;
}

function readSuperblock(file: SyncFile): Superblock | null {
  if (file.size() < SB_SIZE) return null;
  const buf = new Uint8Array(SB_SIZE * 2);
  file.read(buf, 0);
  let best: Superblock | null = null;
  for (let slot = 0; slot < 2; slot++) {
    const view = new DataView(buf.buffer, slot * SB_SIZE, SB_USED);
    if (view.getUint32(0, true) !== MAGIC || view.getUint32(4, true) !== VERSION) continue;
    if (view.getUint32(60, true) !== hash32(buf, slot * SB_SIZE, slot * SB_SIZE + 60)) continue;
    // Index blocks are raw typed arrays: only the byte order they were written in reads them.
    if ((view.getUint32(20, true) === 1) !== LITTLE_ENDIAN) continue;
    const sb: Superblock = {
      seq: view.getFloat64(8, true),
      state: view.getUint32(16, true),
      dirOff: view.getFloat64(24, true),
      dirLen: view.getUint32(32, true),
      dirSum: view.getUint32(36, true),
      covered: view.getFloat64(40, true),
      gen: view.getFloat64(48, true),
    };
    if (!best || sb.seq > best.seq) best = sb;
  }
  return best;
}

function writeSuperblock(file: SyncFile, sb: Superblock): void {
  const buf = new Uint8Array(SB_SIZE);
  const view = new DataView(buf.buffer);
  view.setUint32(0, MAGIC, true);
  view.setUint32(4, VERSION, true);
  view.setFloat64(8, sb.seq, true);
  view.setUint32(16, sb.state, true);
  view.setUint32(20, LITTLE_ENDIAN ? 1 : 0, true);
  view.setFloat64(24, sb.dirOff, true);
  view.setUint32(32, sb.dirLen, true);
  view.setUint32(36, sb.dirSum, true);
  view.setFloat64(40, sb.covered, true);
  view.setFloat64(48, sb.gen, true);
  view.setUint32(60, hash32(buf, 0, 60), true);
  file.write(buf, (sb.seq % 2) * SB_SIZE);
}

/** An index block whose bytes are not the ones a checkpoint wrote. */
class IndexMismatch extends Error {
  constructor(table: string) {
    super(`rows: index block of ${table} does not match its hash`);
    this.name = 'IndexMismatch';
  }
}

export class RowStore {
  private tables = new Map<string, Table>();
  private byTid = new Map<number, Table>();
  private nextTid = 1;
  private scratch = new Uint8Array(64 << 10);
  private dirBytes = 0;
  private tailOps = 0;
  private needsFlush = false;
  private rebuilt = false;
  private closed = false;

  private constructor(
    private io: StoreIo,
    private file: SyncFile,
    private name: 'a' | 'b',
    private other: SyncFile | null,
    private sb: Superblock,
    /** End of the last whole extent: where the next one goes. */
    private end: number
  ) {}

  // ---- open ----------------------------------------------------------------

  static async open(io: StoreIo): Promise<RowStore> {
    const a = await io.open('a');
    let file = a;
    let name: 'a' | 'b' = 'a';
    let other: SyncFile | null = null;
    let sb = readSuperblock(a);
    if (!sb || sb.state !== ACTIVE) {
      // Either a new store, or a compaction moved the rows to `b`.
      const b = await io.open('b');
      const sbB = readSuperblock(b);
      if (sbB && sbB.state === ACTIVE) {
        file = b;
        name = 'b';
        other = a;
        sb = sbB;
      } else {
        other = b;
        // No file claims to be live. Take whichever still holds extents and
        // rebuild from them; a store that never held any starts empty.
        if (a.size() <= HEADER_END && b.size() > HEADER_END) {
          file = b;
          name = 'b';
          other = a;
        }
        sb = null;
      }
    }
    const fresh = sb === null;
    const store = new RowStore(
      io,
      file,
      name,
      other,
      sb ?? { seq: 0, state: ACTIVE, dirOff: 0, dirLen: 0, dirSum: 0, covered: HEADER_END, gen: 0 },
      HEADER_END
    );
    if (fresh || !store.loadDirectory()) store.resetIndex();
    store.replay(store.sb.covered);
    if (fresh) store.commitSuperblock(0, 0, 0, store.sb.covered);
    return store;
  }

  /** Read the directory the superblock names. False when it cannot be trusted. */
  private loadDirectory(): boolean {
    const { dirOff, dirLen, dirSum, covered } = this.sb;
    if (covered > Math.max(this.file.size(), HEADER_END)) return false;
    if (dirOff === 0) return true;
    if (dirOff + dirLen > this.file.size()) return false;
    const buf = new Uint8Array(dirLen);
    this.file.read(buf, dirOff);
    if (hash32(buf) !== dirSum) return false;
    let dir: { nextTid: number; tables: (Block & { name: string; tid: number })[] };
    try {
      dir = JSON.parse(decoder.decode(buf));
    } catch {
      return false;
    }
    this.nextTid = dir.nextTid;
    this.dirBytes = dirLen + XH;
    for (const t of dir.tables) {
      const table: Table = {
        name: t.name,
        tid: t.tid,
        block: t.n > 0 ? { off: t.off, len: t.len, n: t.n, heap: t.heap, sum: t.sum, live: t.live } : null,
        run: null,
        delta: new Map(),
        unsaved: false,
      };
      this.tables.set(t.name, table);
      this.byTid.set(t.tid, table);
    }
    return true;
  }

  /** Forget the index; the next replay starts from the first extent. */
  private resetIndex(): void {
    this.tables.clear();
    this.byTid.clear();
    this.nextTid = 1;
    this.dirBytes = 0;
    this.tailOps = 0;
    this.rebuilt = this.file.size() > HEADER_END;
    this.sb = { ...this.sb, dirOff: 0, dirLen: 0, dirSum: 0, covered: HEADER_END };
  }

  /** Apply every whole batch from `from` on, and cut the file after the last. */
  private replay(from: number): void {
    const size = this.file.size();
    let pos = from;
    const head = new Uint8Array(XH);
    const headView = new DataView(head.buffer);
    while (pos + XH <= size) {
      this.file.read(head, pos);
      const type = headView.getUint32(0, true);
      const len = headView.getUint32(4, true);
      if (type < X_BATCH || type > X_DIR || pos + XH + len > size) break;
      if (type === X_BATCH) {
        const payload = len <= this.scratch.length ? this.scratch.subarray(0, len) : new Uint8Array(len);
        this.file.read(payload, pos + XH);
        if (hash32(payload) !== headView.getUint32(8, true)) break;
        this.applyBatch(payload, pos + XH);
      }
      pos = align8(pos + XH + len);
    }
    this.end = pos;
    if (size > this.end) this.file.truncate(this.end);
  }

  private applyBatch(buf: Uint8Array, fileOff: number): void {
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    let p = 0;
    while (p < buf.length) {
      this.tailOps++;
      const kind = buf[p];
      const flags = buf[p + 1];
      const tid = view.getUint16(p + 2, true);
      if (kind === R_TABLE) {
        const nameLen = view.getUint16(p + 4, true);
        const name = decoder.decode(buf.subarray(p + 6, p + 6 + nameLen));
        if (!this.byTid.has(tid)) this.addTable(name, tid);
        p += 6 + nameLen;
        continue;
      }
      const table = this.byTid.get(tid);
      if (!table) throw new Error(`rows: batch names unknown table ${tid}`);
      if (kind === R_CLEAR) {
        this.clearTable(table);
        p += 4;
        continue;
      }
      const idLen = view.getUint16(p + 4, true);
      const id = decoder.decode(buf.subarray(p + 6, p + 6 + idLen));
      p += 6 + idLen;
      if (kind === R_DEL) {
        table.delta.set(id, null);
        continue;
      }
      if (kind !== R_PUT) throw new Error(`rows: unknown record kind ${kind}`);
      const rv = view.getFloat64(p, true);
      const len = view.getUint32(p + 8, true);
      table.delta.set(id, { off: fileOff + p + 12, len, rv, flags });
      p += 12 + len;
    }
  }

  private addTable(name: string, tid: number): Table {
    const table: Table = { name, tid, block: null, run: null, delta: new Map(), unsaved: false };
    this.tables.set(name, table);
    this.byTid.set(tid, table);
    if (tid >= this.nextTid) this.nextTid = tid + 1;
    return table;
  }

  private clearTable(table: Table): void {
    table.delta.clear();
    table.unsaved = table.unsaved || table.block !== null;
    table.block = null;
    table.run = emptyRun();
  }

  // ---- reads ---------------------------------------------------------------

  /** The table's run, read from its block on first use. */
  private loadRun(table: Table): Run {
    if (table.run) return table.run;
    const block = table.block;
    if (!block) return (table.run = emptyRun());
    const buf = new Uint8Array(new ArrayBuffer(block.len));
    this.file.read(buf, block.off);
    if (hash32(buf) !== block.sum) throw new IndexMismatch(table.name);
    return (table.run = runFromBuffer(buf, block.n, block.heap, block.live));
  }

  /**
   * Run `fn`; when it finds an index block that does not match what was
   * written, rebuild the index from the log and run it again. The log still
   * holds every row, so the answer is the same, only later.
   */
  private trusted<T>(fn: () => T): T {
    try {
      return fn();
    } catch (error) {
      if (!(error instanceof IndexMismatch)) throw error;
      this.resetIndex();
      this.rebuilt = true;
      this.replay(HEADER_END);
      return fn();
    }
  }

  /** The table's rows in id order, with every write folded in. */
  ordered(name: string): Run {
    return this.trusted(() => {
      const table = this.tables.get(name);
      if (!table) return emptyRun();
      const run = this.loadRun(table);
      if (table.delta.size === 0) return run;
      table.run = mergeRun(run, table.delta);
      table.delta.clear();
      table.unsaved = true;
      return table.run;
    });
  }

  find(name: string, id: string): Entry | null {
    return this.trusted(() => {
      const table = this.tables.get(name);
      if (!table) return null;
      const pending = table.delta.get(id);
      if (pending !== undefined) return pending;
      const run = this.loadRun(table);
      const i = runFind(run, id);
      return i < 0 ? null : { off: run.off[i], len: run.len[i], rv: run.rv[i], flags: run.flags[i] };
    });
  }

  /** Read `len` bytes at `off` into `into`, starting at `at`. */
  readInto(off: number, len: number, into: Uint8Array, at: number): void {
    this.file.read(into.subarray(at, at + len), off);
  }

  get(name: string, id: string): { body: Uint8Array; entry: Entry } | null {
    const entry = this.find(name, id);
    if (!entry) return null;
    const body = new Uint8Array(entry.len);
    this.file.read(body, entry.off);
    return { body, entry };
  }

  count(name: string): number {
    return this.ordered(name).n;
  }

  /** `(id, version)` of every row, from the index alone. */
  versions(name: string): [string, number][] {
    const run = this.ordered(name);
    const out: [string, number][] = new Array(run.n);
    for (let i = 0; i < run.n; i++) out[i] = [runId(run, i), run.rv[i]];
    return out;
  }

  tableNames(): string[] {
    return [...this.tables.keys()].sort();
  }

  // ---- writes --------------------------------------------------------------

  private table(name: string): { table: Table; isNew: boolean } {
    const existing = this.tables.get(name);
    if (existing) return { table: existing, isNew: false };
    return { table: this.addTable(name, this.nextTid), isNew: true };
  }

  /**
   * Apply `ops` as one atomic batch. `sync` flushes before returning; without
   * it the bytes are with the operating system (they survive the tab, not a
   * power cut) until the next {@link flush}.
   */
  write(ops: StoreOp[], sync = false): void {
    if (this.closed) throw new Error('rows: store is closed');
    if (ops.length === 0) return;
    let bound = XH + 8;
    for (const op of ops) {
      bound += 12 + op.table.length * 3;
      if (op.t !== 'clear') bound += op.id.length * 3 + 6;
      if (op.t === 'put') bound += op.body.length + 12;
    }
    if (this.scratch.length < bound) this.scratch = new Uint8Array(Math.max(bound, this.scratch.length * 2));
    const buf = this.scratch;
    const view = new DataView(buf.buffer);
    let p = XH;
    const applied: (() => void)[] = [];
    const created: Table[] = [];
    const base = this.end;
    for (const op of ops) {
      const { table, isNew } = this.table(op.table);
      if (isNew) {
        created.push(table);
        buf[p] = R_TABLE;
        buf[p + 1] = 0;
        view.setUint16(p + 2, table.tid, true);
        const written = encoder.encodeInto(table.name, buf.subarray(p + 6)).written;
        view.setUint16(p + 4, written, true);
        p += 6 + written;
      }
      if (op.t === 'clear') {
        buf[p] = R_CLEAR;
        buf[p + 1] = 0;
        view.setUint16(p + 2, table.tid, true);
        p += 4;
        applied.push(() => this.clearTable(table));
        continue;
      }
      const idLen = encoder.encodeInto(op.id, buf.subarray(p + 6)).written;
      if (idLen > MAX_ID_BYTES) throw new Error('rows: id longer than 65535 bytes');
      view.setUint16(p + 2, table.tid, true);
      view.setUint16(p + 4, idLen, true);
      if (op.t === 'del') {
        buf[p] = R_DEL;
        buf[p + 1] = 0;
        p += 6 + idLen;
        const id = op.id;
        applied.push(() => table.delta.set(id, null));
        continue;
      }
      buf[p] = R_PUT;
      buf[p + 1] = op.flags;
      p += 6 + idLen;
      view.setFloat64(p, op.rv, true);
      view.setUint32(p + 8, op.body.length, true);
      buf.set(op.body, p + 12);
      const entry: Entry = { off: base + p + 12, len: op.body.length, rv: op.rv, flags: op.flags };
      const id = op.id;
      applied.push(() => table.delta.set(id, entry));
      p += 12 + op.body.length;
    }
    try {
      this.appendExtent(X_BATCH, buf, p);
    } catch (error) {
      // Nothing was appended (a full disk, a lost handle): the tables this
      // batch would have introduced were never named in the log.
      for (const table of created) {
        this.tables.delete(table.name);
        this.byTid.delete(table.tid);
      }
      if (created.length > 0) this.nextTid = Math.min(...created.map((t) => t.tid));
      throw error;
    }
    // The index moves only once the bytes are in the file.
    for (const apply of applied) apply();
    this.tailOps += ops.length;
    if (this.scratch.length > SCRATCH_KEEP) this.scratch = new Uint8Array(64 << 10);
    this.needsFlush = true;
    if (sync) this.flush();
  }

  /**
   * Frame `buf[XH..p)` as one extent and append it. `buf` must leave its first
   * `XH` bytes free and have room for the padding.
   */
  private appendExtent(type: number, buf: Uint8Array, p: number): number {
    const len = p - XH;
    const total = align8(p);
    buf.fill(0, p, total);
    const view = new DataView(buf.buffer, buf.byteOffset, XH);
    view.setUint32(0, type, true);
    view.setUint32(4, len, true);
    view.setUint32(8, hash32(buf, XH, p), true);
    view.setUint32(12, 0, true);
    this.file.write(buf.subarray(0, total), this.end);
    const at = this.end + XH;
    this.end += total;
    return at;
  }

  flush(): void {
    if (!this.needsFlush) return;
    this.file.flush();
    this.needsFlush = false;
  }

  // ---- checkpoint ----------------------------------------------------------

  /** Bytes of batches the next open would have to replay. */
  get tailBytes(): number {
    return this.end - this.sb.covered;
  }

  /**
   * Persist the index: append the run of every table written since the last
   * checkpoint, then a directory, then flip the superblock. Returns false when
   * there was nothing to persist.
   */
  checkpoint(): boolean {
    return this.trusted(() => this.checkpointOnce());
  }

  private checkpointOnce(): boolean {
    if (this.closed) return false;
    let dirty = this.end > this.sb.covered;
    for (const table of this.tables.values()) dirty = dirty || table.delta.size > 0 || table.unsaved;
    if (!dirty) return false;
    for (const table of this.tables.values()) {
      if (table.delta.size === 0 && !table.unsaved) continue;
      const run = this.ordered(table.name);
      table.block = run.n > 0 ? this.appendBlock(run) : null;
      table.unsaved = false;
    }
    const { dirOff, dirLen, dirSum } = this.appendDirectory();
    this.file.flush();
    this.commitSuperblock(dirOff, dirLen, dirSum, this.end);
    return true;
  }

  private appendBlock(run: Run): Block {
    const head = new Uint8Array(XH);
    const view = new DataView(head.buffer);
    const sum = hash32(run.buf);
    view.setUint32(0, X_BLOCK, true);
    view.setUint32(4, run.buf.length, true);
    view.setUint32(8, sum, true);
    this.file.write(head, this.end);
    const off = this.end + XH;
    this.file.write(run.buf, off);
    this.end = align8(off + run.buf.length);
    // Leave no hole behind a block whose length is not a multiple of 8.
    if (this.file.size() < this.end) this.file.truncate(this.end);
    return { off, len: run.buf.length, n: run.n, heap: run.heap.length, sum, live: run.live };
  }

  private appendDirectory(): { dirOff: number; dirLen: number; dirSum: number } {
    const tables = [...this.tables.values()].map((t) => ({
      name: t.name,
      tid: t.tid,
      ...(t.block ?? { off: 0, len: 0, n: 0, heap: 0, sum: 0, live: 0 }),
    }));
    const body = encoder.encode(JSON.stringify({ nextTid: this.nextTid, tables }));
    const buf = new Uint8Array(align8(XH + body.length));
    buf.set(body, XH);
    const dirOff = this.appendExtent(X_DIR, buf, XH + body.length);
    this.dirBytes = body.length + XH;
    return { dirOff, dirLen: body.length, dirSum: hash32(body) };
  }

  private commitSuperblock(dirOff: number, dirLen: number, dirSum: number, covered: number): void {
    this.sb = { ...this.sb, seq: this.sb.seq + 1, state: ACTIVE, dirOff, dirLen, dirSum, covered };
    writeSuperblock(this.file, this.sb);
    this.file.flush();
    this.needsFlush = false;
    if (covered === this.end) this.tailOps = 0;
  }

  // ---- compaction ----------------------------------------------------------

  stats(): StoreStats {
    let liveBytes = 0;
    let indexBytes = this.dirBytes;
    for (const table of this.tables.values()) {
      const block = table.block;
      // A live row costs its body plus its record header and id in the log.
      const run = table.run;
      if (run) liveBytes += run.live + RECORD_BYTES * run.n + run.heap.length;
      else if (block) liveBytes += block.live + RECORD_BYTES * block.n + block.heap;
      if (block) indexBytes += block.len + XH;
    }
    const fileBytes = this.end;
    return {
      file: this.name,
      fileBytes,
      liveBytes,
      indexBytes,
      garbageBytes: Math.max(0, fileBytes - HEADER_END - liveBytes - indexBytes),
      tailBytes: this.tailBytes,
      tailOps: this.tailOps,
      tables: this.tables.size,
      rebuilt: this.rebuilt,
    };
  }

  /**
   * Copy every live row into the other file, in id order, and switch to it.
   * The switch is one superblock write in the old file; a crash before it
   * leaves the old file as it was, a crash after it finds the new one whole.
   */
  async compact(): Promise<void> {
    if (this.closed) throw new Error('rows: store is closed');
    // Load every run up front, so a mismatching block is rebuilt before any byte is copied.
    for (const name of [...this.tables.keys()]) this.ordered(name);
    const src = this.file;
    const dst = this.other ?? (await this.io.open(this.name === 'a' ? 'b' : 'a'));
    dst.truncate(0);

    const next = new RowStore(
      this.io,
      dst,
      this.name === 'a' ? 'b' : 'a',
      src,
      { seq: 0, state: ACTIVE, dirOff: 0, dirLen: 0, dirSum: 0, covered: HEADER_END, gen: this.sb.gen + 1 },
      HEADER_END
    );
    const chunk = new Uint8Array(COMPACT_CHUNK + XH + 8);
    const view = new DataView(chunk.buffer);
    for (const table of this.tables.values()) {
      const run = this.ordered(table.name);
      const copy = next.addTable(table.name, table.tid);
      const moved = new Uint8Array(new ArrayBuffer(run.buf.length));
      moved.set(run.buf);
      const out = runFromBuffer(moved, run.n, run.heap.length, run.live);
      let p = XH;
      let first = 0;
      const emit = (): void => {
        if (p === XH) return;
        const at = next.appendExtent(X_BATCH, chunk, p);
        // Offsets were recorded relative to the chunk; make them absolute.
        for (let k = first; k < pending; k++) out.off[k] += at - XH;
        first = pending;
        p = XH;
      };
      let pending = 0;
      // The table's name travels with its rows, so a scan of this file alone can rebuild it.
      chunk[p] = R_TABLE;
      chunk[p + 1] = 0;
      view.setUint16(p + 2, table.tid, true);
      const nameLen = encoder.encodeInto(table.name, chunk.subarray(p + 6)).written;
      view.setUint16(p + 4, nameLen, true);
      p += 6 + nameLen;
      for (let i = 0; i < run.n; i++) {
        const s = idStart(run, i);
        const idLen = run.idEnd[i] - s;
        const size = RECORD_BYTES + idLen + run.len[i];
        if (size > COMPACT_CHUNK) throw new Error('rows: row larger than a compaction chunk');
        if (p + size > COMPACT_CHUNK + XH) emit();
        chunk[p] = R_PUT;
        chunk[p + 1] = run.flags[i];
        view.setUint16(p + 2, table.tid, true);
        view.setUint16(p + 4, idLen, true);
        chunk.set(run.heap.subarray(s, s + idLen), p + 6);
        p += 6 + idLen;
        view.setFloat64(p, run.rv[i], true);
        view.setUint32(p + 8, run.len[i], true);
        src.read(chunk.subarray(p + 12, p + 12 + run.len[i]), run.off[i]);
        out.off[i] = p + 12;
        p += 12 + run.len[i];
        pending = i + 1;
      }
      emit();
      copy.run = out;
      copy.block = out.n > 0 ? next.appendBlock(out) : null;
    }
    next.nextTid = this.nextTid;
    const { dirOff, dirLen, dirSum } = next.appendDirectory();
    dst.flush();
    next.commitSuperblock(dirOff, dirLen, dirSum, next.end);

    // The commit: from here on an open finds `dst`.
    writeSuperblock(src, { ...this.sb, seq: this.sb.seq + 1, state: MOVED });
    src.flush();
    src.truncate(HEADER_END);

    this.file = dst;
    this.other = src;
    this.name = next.name;
    this.sb = next.sb;
    this.end = next.end;
    this.tables = next.tables;
    this.byTid = next.byTid;
    this.dirBytes = next.dirBytes;
    this.tailOps = 0;
    this.needsFlush = false;
  }

  close(): void {
    if (this.closed) return;
    this.flush();
    this.closed = true;
    this.file.close();
    this.other?.close();
  }
}

/** A {@link SyncFile} held in memory: tests, and the store's memory mode. */
export class MemoryFile implements SyncFile {
  private bytes = new Uint8Array(64 << 10);
  private length = 0;
  flushes = 0;

  read(buffer: Uint8Array, at: number): number {
    const n = Math.max(0, Math.min(buffer.length, this.length - at));
    if (n > 0) buffer.set(this.bytes.subarray(at, at + n));
    if (n < buffer.length) buffer.fill(0, n);
    return n;
  }

  write(buffer: Uint8Array, at: number): number {
    const end = at + buffer.length;
    if (end > this.bytes.length) {
      const grown = new Uint8Array(Math.max(end, this.bytes.length * 2));
      grown.set(this.bytes.subarray(0, this.length));
      this.bytes = grown;
    }
    if (at > this.length) this.bytes.fill(0, this.length, at);
    this.bytes.set(buffer, at);
    if (end > this.length) this.length = end;
    return buffer.length;
  }

  size(): number {
    return this.length;
  }

  truncate(size: number): void {
    if (size > this.bytes.length) {
      const grown = new Uint8Array(size);
      grown.set(this.bytes.subarray(0, this.length));
      this.bytes = grown;
    }
    if (size > this.length) this.bytes.fill(0, this.length, size);
    this.length = size;
  }

  flush(): void {
    this.flushes++;
  }

  close(): void {}

  /** The file's bytes, for tests that damage them. */
  snapshot(): Uint8Array {
    return this.bytes.slice(0, this.length);
  }

  static from(bytes: Uint8Array): MemoryFile {
    const file = new MemoryFile();
    file.write(bytes, 0);
    return file;
  }
}
