/// <reference lib="webworker" />
/**
 * Dedicated Web Worker that owns a bucket's row store (`rows-store.ts`): the
 * OPFS sync access handles it reads and writes through exist only in a
 * dedicated worker, and are exclusive, so one worker per bucket holds them and
 * every tab's reads and writes go through it.
 *
 * It speaks the request/response protocol of the SQLite worker (requests keyed
 * by `id`, replies on the channel the request came in on), with the same
 * lifecycle messages, so the engine's transport, its shared-tabs roles and the
 * leader fencing work unchanged:
 *
 *   { id, type: 'open', payload: { dbName, useOpfs, workerLockName?, openOptions?, share?, reader? } }
 *        -> { id, ok, persisted, opfsError? }
 *   { id, type: 'close' | 'shutdown' | 'relock' | 'add-client' | 'remove-client' | 'diag' }
 *   every other type is a data op, answered by `handleRowsOp` (rows-exec.ts)
 *
 * Rows travel as JSON text in a transferred buffer, in both directions.
 *
 * Housekeeping runs on the same chain as the ops, in the quiet after them:
 * unflushed writes are flushed within {@link FLUSH_LAG_MS}, the index is
 * persisted after {@link IDLE_MS} without traffic (and at once when the log
 * written since the last checkpoint grows past its bound), and dead bytes are
 * compacted away when they outweigh the live ones.
 */
import { verifyLockStillHeld as verifyLockStillHeldBounded } from './sqlite-lock-verify';
import { MemoryFile, RowStore, bucketDirName, type StoreIo, type SyncFile } from './rows-store';
import {
  CHECKPOINT_TAIL_BYTES,
  CHECKPOINT_TAIL_OPS,
  ROWS_CLIENT_OPS,
  ROWS_PROTOCOL,
  RowsExec,
  handleReaderOp,
  handleRowsOp,
  maintenance,
  visibleTables,
} from './rows-exec';
import { ROWS_AHEAD_ID, ROWS_AHEAD_OPS } from './rows-prewarm';

type PostTarget = Pick<MessagePort, 'postMessage'>;

interface OpenOptions {
  maxAttempts?: number;
  backoffMs?: number[];
  disallowMemoryFallback?: boolean;
}

/**
 * Retry schedule for an open that finds the files held. The usual holder is
 * this tab's previous worker, whose handles the browser releases a moment
 * after a reload, so the first retries come within milliseconds: a reload
 * should not wait a quarter second for a file that is free after five. The
 * whole ramp still gives another tab about as long as the SQLite worker does
 * (750 ms) before falling back to memory.
 */
const DEFAULT_BACKOFF_MS = [4, 8, 16, 32, 64, 128, 250, 250];
const DEFAULT_MAX_ATTEMPTS = DEFAULT_BACKOFF_MS.length + 1;

/** How long a write may sit with the operating system before it is flushed. */
const FLUSH_LAG_MS = 100;
/** Quiet time after which the index is persisted and the store tidied. */
const IDLE_MS = 500;

class OpfsFile implements SyncFile {
  constructor(private access: FileSystemSyncAccessHandle) {}
  read(buffer: Uint8Array, at: number): number {
    return this.access.read(buffer, { at });
  }
  write(buffer: Uint8Array, at: number): number {
    const written = this.access.write(buffer, { at });
    // A short write is a full disk: fail the batch rather than store part of it.
    if (written !== buffer.length) throw new Error(`rows: wrote ${written} of ${buffer.length} bytes (storage full?)`);
    return written;
  }
  size(): number {
    return this.access.getSize();
  }
  truncate(size: number): void {
    this.access.truncate(size);
  }
  flush(): void {
    this.access.flush();
  }
  close(): void {
    this.access.close();
  }
}

let store: RowStore | null = null;
let exec: RowsExec | null = null;
/** The open store is a reader's: it answers reads beside another worker's writes and does no housekeeping. */
let reading = false;
/** What the open store was opened as, so a second `open` for the same thing is a no-op. */
let opened: { dbName: string; useOpfs: boolean; mode: FileMode; result: { persisted: boolean; opfsError?: string } } | null = null;
/** Handles held for the open store, closed with it. */
let openFiles: SyncFile[] = [];

/**
 * How the store's files are opened.
 *   `own`    the default: one worker holds them, any other fails to open.
 *   `share`  a shared-tabs leader that lets followers read the file beside it.
 *   `reader` such a follower: it only reads (`RowStore` in reader mode).
 * The last two use `readwrite-unsafe` handles (Chrome 121+), which several
 * workers can hold on one file. Nothing but the leader's Web Lock then stands
 * between two writers, which is why sharing is something an app asks for.
 */
type FileMode = 'own' | 'share' | 'reader';

/**
 * Whether this browser opens a second `readwrite-unsafe` handle on a file. One
 * that does not know the option ignores it and hands out the exclusive handle,
 * so the only way to know is to try, on a file of no importance.
 */
async function canShareFiles(dir: FileSystemDirectoryHandle): Promise<boolean> {
  const handles: FileSystemSyncAccessHandle[] = [];
  try {
    const probe = await dir.getFileHandle('probe', { create: true });
    // oxlint-disable-next-line typescript/no-explicit-any
    const open = (): Promise<FileSystemSyncAccessHandle> => (probe as any).createSyncAccessHandle({ mode: 'readwrite-unsafe' });
    handles.push(await open());
    handles.push(await open());
    return true;
  } catch {
    return false;
  } finally {
    for (const handle of handles) handle.close();
  }
}

async function opfsIo(bucketId: string, mode: FileMode = 'own'): Promise<StoreIo> {
  const root = await navigator.storage.getDirectory();
  const dir = await root.getDirectoryHandle(bucketDirName(bucketId), { create: true });
  let shared = false;
  if (mode !== 'own') {
    shared = await canShareFiles(dir);
    // A leader without the option leads as before; a reader has nothing to read with.
    if (!shared && mode === 'reader') throw new Error('Missing required OPFS APIs: shared access handles');
  }
  return {
    open: async (name) => {
      const handle = await dir.getFileHandle(name, { create: true });
      // oxlint-disable-next-line typescript/no-explicit-any
      const access = await (shared ? (handle as any).createSyncAccessHandle({ mode: 'readwrite-unsafe' }) : handle.createSyncAccessHandle());
      const file = new OpfsFile(access as FileSystemSyncAccessHandle);
      openFiles.push(file);
      return file;
    },
  };
}

function memoryIo(): StoreIo {
  const files = { a: new MemoryFile(), b: new MemoryFile() };
  return { open: (name) => files[name] };
}

function closeFiles(): void {
  for (const file of openFiles) {
    try {
      file.close();
    } catch {
      /* already closed */
    }
  }
  openFiles = [];
}

const errMessage = (e: unknown): string =>
  e instanceof Error ? (e.name && e.name !== 'Error' ? `${e.name}: ${e.message}` : e.message) : String(e);

/**
 * Open `bucketId`'s store. Sync access handles are exclusive, so a second
 * worker on the same bucket (another tab that is still closing) fails here:
 * retry, then fall back to memory and say so loudly, as the SQLite worker
 * does. A caller that must not run in memory (a shared-tabs leader) gets the
 * error instead.
 */
async function openStore(
  bucketId: string,
  useOpfs: boolean,
  opts: OpenOptions = {},
  mode: FileMode = 'own'
): Promise<{ persisted: boolean; opfsError?: string }> {
  reading = false;
  if (!useOpfs) {
    store = await RowStore.open(memoryIo());
    exec = new RowsExec(store);
    return { persisted: false };
  }
  const maxAttempts = Math.max(1, opts.maxAttempts ?? DEFAULT_MAX_ATTEMPTS);
  const backoffMs = opts.backoffMs ?? DEFAULT_BACKOFF_MS;
  let lastError = 'unknown error';
  let attempts = 0;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    attempts = attempt;
    try {
      if (!navigator.storage?.getDirectory) throw new Error('Missing required OPFS APIs');
      store = await RowStore.open(await opfsIo(bucketId, mode), { reader: mode === 'reader' });
      exec = new RowsExec(store, mode === 'reader');
      reading = mode === 'reader';
      return { persisted: true };
    } catch (e) {
      closeFiles();
      lastError = errMessage(e);
      if (attempt === maxAttempts || lastError.includes('Missing required OPFS APIs')) break;
      await new Promise((resolve) => setTimeout(resolve, backoffMs[Math.min(attempt - 1, backoffMs.length - 1)] ?? 0));
    }
  }
  // A reader in memory would read nothing of the leader's: it has no fallback.
  if (opts.disallowMemoryFallback || mode === 'reader') throw new Error(`opfs-unavailable: ${lastError} (after ${attempts} attempts)`);
  // Deliberately console, not a logger: host apps set their own log level, and
  // losing durability must never be filtered into silence.
  // oxlint-disable-next-line no-console
  console.error(
    `[sp00ky] OPFS persistence unavailable for "${bucketId}" after ${attempts} attempt${attempts === 1 ? '' : 's'}: ` +
      `${lastError}. The local row store is running IN MEMORY, which keeps the whole dataset in RAM and loses ` +
      'every local write on reload. The usual cause is another tab of this app holding the storage lock, so ' +
      'closing the other tabs and reloading restores persistence.'
  );
  store = await RowStore.open(memoryIo());
  exec = new RowsExec(store);
  return { persisted: false, opfsError: lastError };
}

/** Persist the index and let go of the files. */
function closeStore(): void {
  clearTimers();
  try {
    store?.checkpoint();
  } catch {
    /* the log already holds every write; the next open replays it */
  }
  try {
    store?.close();
  } catch {
    /* ignore */
  }
  store = null;
  exec = null;
  opened = null;
  reading = false;
  warmQueue = null;
  closeFiles();
}

// ==================== housekeeping ====================

let flushTimer: ReturnType<typeof setTimeout> | null = null;
let idleTimer: ReturnType<typeof setTimeout> | null = null;

function clearTimers(): void {
  if (flushTimer) clearTimeout(flushTimer);
  if (idleTimer) clearTimeout(idleTimer);
  flushTimer = null;
  idleTimer = null;
}

function afterOp(wrote: boolean): void {
  if (!store) return;
  if (wrote) {
    const stats = store.stats();
    // Bound what the next open has to replay, even under constant writing.
    if (stats.tailBytes > CHECKPOINT_TAIL_BYTES || stats.tailOps > CHECKPOINT_TAIL_OPS) store.checkpoint();
    else if (!flushTimer) {
      flushTimer = setTimeout(() => {
        flushTimer = null;
        enqueue(() => store?.flush());
      }, FLUSH_LAG_MS);
    }
  }
  if (idleTimer) clearTimeout(idleTimer);
  idleTimer = setTimeout(() => {
    idleTimer = null;
    enqueue(tidy);
  }, IDLE_MS);
}

async function tidy(): Promise<void> {
  if (!store) return;
  store.flush();
  // A checkpoint, then at most a compaction and the checkpoint after it.
  for (let round = 0; round < 3 && store; round++) {
    const step = maintenance(store.stats());
    if (!step) break;
    if (step === 'checkpoint') store.checkpoint();
    else await store.compact();
  }
}

// ==================== leader fencing (worker web lock) ====================
// Same scheme as the SQLite worker: a Web Lock named per leadership, held for
// the store's lifetime. Losing it without having released it means the broker
// gave the bucket to another tab, and this worker must never write again.

let fenced = false;

interface HeldLock {
  name: string;
  release: () => void;
}

let currentLock: HeldLock | null = null;

function workerLocks(): LockManager | null {
  const nav = (globalThis as { navigator?: { locks?: LockManager } }).navigator;
  return nav?.locks && typeof nav.locks.request === 'function' ? nav.locks : null;
}

function acquireWorkerLock(name: string): Promise<HeldLock | null> {
  const locks = workerLocks();
  if (!locks) return Promise.resolve({ name, release() {} });
  return new Promise<HeldLock | null>((resolve) => {
    let granted = false;
    let intentional = false;
    let releaseGrant: (() => void) | null = null;
    locks
      .request(name, { mode: 'exclusive', ifAvailable: true }, (lock) => {
        if (!lock) {
          resolve(null);
          return;
        }
        granted = true;
        resolve({
          name,
          release() {
            intentional = true;
            releaseGrant?.();
          },
        });
        return new Promise<void>((release) => {
          releaseGrant = release;
        });
      })
      .then(
        () => {
          if (granted && !intentional) fence('worker lock stolen');
        },
        () => {
          if (granted && !intentional) fence('worker lock request failed');
        }
      );
  });
}

function releaseHeldLock(): void {
  currentLock?.release();
  currentLock = null;
}

/** Terminal: close the files without another write, tell the owner, stop. */
function fence(reason: string): void {
  if (fenced) return;
  fenced = true;
  // oxlint-disable-next-line no-console
  console.error(
    `[sp00ky] row store worker fenced (${reason}): leadership was taken over, ` +
      'closing the store and refusing further ops.'
  );
  clearTimers();
  // No checkpoint: the new leader owns the file now.
  store = null;
  exec = null;
  opened = null;
  closeFiles();
  try {
    (self as unknown as Worker).postMessage({ type: 'lock-lost', reason });
  } catch {
    /* owner gone */
  }
  releaseHeldLock();
  self.close();
}

// A frozen tab's worker resumes with its queue intact, and a queued write could
// run before the lock-steal callback does. After a long gap, ops wait for a
// lock check first. See the SQLite worker for the reasoning behind the bound.
const FREEZE_SUSPECT_MS = 10_000;
let lastTickAt = performance.now();
let thawVerification: Promise<void> | null = null;

function noteTick(): void {
  const now = performance.now();
  const gap = now - lastTickAt;
  lastTickAt = now;
  if (gap > FREEZE_SUSPECT_MS && currentLock && !fenced && !thawVerification) {
    thawVerification = verifyLockStillHeldBounded(
      // oxlint-disable-next-line typescript/no-explicit-any
      workerLocks() as any,
      currentLock?.name,
      (reason) => Promise.resolve(fence(reason)),
      undefined,
      // oxlint-disable-next-line no-console
      (msg) => console.warn(`[rows-worker] ${msg}`)
    ).finally(() => {
      thawVerification = null;
    });
  }
}
setInterval(noteTick, 1000);

// ==================== dispatch ====================

const clients = new Map<string, MessagePort>();

/** One chain for every op from every channel, housekeeping included. */
let opChain: Promise<unknown> = Promise.resolve();

/** Ops queued or running, and when the last one finished. */
let pendingOps = 0;
let lastOpAt = 0;

function enqueue<T>(fn: () => T | Promise<T>): Promise<T> {
  pendingOps++;
  const result = opChain.then(fn, fn);
  const settle = (): void => {
    pendingOps--;
    lastOpAt = performance.now();
  };
  opChain = result.then(
    () => settle(),
    () => settle()
  );
  return result;
}

// ---- warming ----
// Reading every index at once took 15 to 20 ms on an 80,000 row store, and a
// request that arrived meanwhile waited for all of it: the page's first read
// came back slower than with no warming at all. So a table is read per turn,
// and only once the worker has been left alone for a moment. A request never
// waits for more than the one table being read.

const WARM_QUIET_MS = 15;
/** Tables whose index is still to be read; null when there is nothing to warm. */
let warmQueue: string[] | null = null;
let warmTimer: ReturnType<typeof setTimeout> | null = null;

function warmSoon(delay: number): void {
  if (warmTimer === null) warmTimer = setTimeout(warmStep, delay);
}

function warmStep(): void {
  warmTimer = null;
  if (!warmQueue || !store || fenced) {
    warmQueue = null;
    return;
  }
  if (pendingOps > 0 || performance.now() - lastOpAt < WARM_QUIET_MS) return warmSoon(WARM_QUIET_MS);
  const table = warmQueue.shift();
  if (table === undefined) {
    warmQueue = null;
    return;
  }
  try {
    // No op is running (the count is zero), so this reads beside none.
    store.ordered(table);
  } catch {
    /* whatever is wrong with this index, the read that needs it reports */
  }
  warmSoon(0);
}

// oxlint-disable-next-line typescript/no-explicit-any
async function handle(type: string, payload: any, source: 'owner' | 'client'): Promise<{ result: object; transfer?: Transferable[] }> {
  if (fenced) throw new Error('rows: fenced, leadership was taken over');
  if (source === 'client' && !ROWS_CLIENT_OPS.has(type)) {
    throw new Error(`rows worker: op ${type} is not allowed on a client port`);
  }
  switch (type) {
    case 'open': {
      if (payload.workerLockName) {
        const lock = await acquireWorkerLock(payload.workerLockName);
        if (!lock) throw new Error('worker-lock-unavailable');
        releaseHeldLock();
        currentLock = lock;
      }
      // The page may have started this worker and its store ahead of itself
      // (`prewarmRows`): the store it asks for is then already open.
      const mode: FileMode = payload.reader ? 'reader' : payload.share ? 'share' : 'own';
      const same =
        store !== null && opened !== null && opened.dbName === payload.dbName && opened.useOpfs === payload.useOpfs && opened.mode === mode;
      if (!same) {
        closeStore();
        const result = await openStore(payload.dbName, payload.useOpfs, payload.openOptions, mode);
        opened = { dbName: payload.dbName, useOpfs: payload.useOpfs, mode, result };
      }
      return { result: { ...opened!.result, proto: ROWS_PROTOCOL, workerUrl: self.location.href } };
    }
    case 'warm':
      // Read the tables' indexes ahead of the page, so its first query finds
      // them in memory. One table at a time and only while nothing is asked
      // for: see `warmStep`.
      warmQueue = exec ? exec.warmOrder() : null;
      warmSoon(WARM_QUIET_MS);
      return { result: {} };
    case 'relock': {
      const lock = await acquireWorkerLock(payload.workerLockName);
      if (!lock) throw new Error('worker-lock-unavailable');
      releaseHeldLock();
      currentLock = lock;
      return { result: {} };
    }
    case 'close':
      closeStore();
      releaseHeldLock();
      return { result: {} };
    case 'shutdown':
      // Graceful pagehide: persist the index and release the handles now, so
      // the next leader's open does not wait for this worker to be collected.
      closeStore();
      releaseHeldLock();
      queueMicrotask(() => self.close());
      return { result: {} };
    case 'remove-client': {
      clients.get(payload.clientId)?.close();
      clients.delete(payload.clientId);
      return { result: {} };
    }
    case 'diag': {
      if (!store) throw new Error('rows: store not open');
      store.refresh();
      const stats = store.stats();
      const tableCounts = payload?.tableCounts
        ? visibleTables(store).map((table) => ({ table, rows: store!.count(table) }))
        : undefined;
      return { result: { stats, tableCounts } };
    }
    default: {
      if (!store || !exec) throw new Error('rows: store not open');
      if (reading) {
        const answer = await handleReaderOp(exec, store, type, payload);
        if (!answer) throw new Error(`rows worker: unknown message ${type}`);
        return answer;
      }
      const reply = await handleRowsOp(exec, store, type, payload);
      if (!reply) throw new Error(`rows worker: unknown message ${type}`);
      afterOp(type === 'write' || type === 'reset-versions' || type === 'blob-put' || type === 'blob-del');
      return reply;
    }
  }
}

// oxlint-disable-next-line typescript/no-explicit-any
function dispatch(target: PostTarget, data: any, source: 'owner' | 'client'): void {
  const { id, type, payload } = data ?? {};
  noteTick();
  // What a page head replays from localStorage (`prewarmRows`) is a read, whatever the storage says.
  if (typeof id === 'number' && id <= ROWS_AHEAD_ID && !ROWS_AHEAD_OPS.has(type)) {
    target.postMessage({ id, ok: false, error: 'rows worker: only reads may be asked ahead' });
    return;
  }
  void enqueue(async () => {
    const t0 = performance.now();
    try {
      if (thawVerification) await thawVerification;
      const { result, transfer } = await handle(type, payload, source);
      // `wt` is the time spent in here, so the page can tell work from transit.
      target.postMessage({ id, ok: true, wt: performance.now() - t0, ...result }, transfer ?? []);
    } catch (err) {
      target.postMessage({ id, ok: false, error: err instanceof Error ? err.message : String(err) });
    }
  });
}

self.onmessage = (ev: MessageEvent) => {
  const data = ev.data ?? {};
  // Handled off the chain: registering a port must not wait behind a long op,
  // or the follower's first requests would deadlock its attach.
  if (data.type === 'add-client') {
    const port = ev.ports?.[0];
    const clientId = data.payload?.clientId as string | undefined;
    if (!port || !clientId) {
      (self as unknown as Worker).postMessage({
        id: data.id,
        ok: false,
        error: 'rows worker: add-client needs a clientId and a transferred port',
      });
      return;
    }
    clients.get(clientId)?.close();
    clients.set(clientId, port);
    port.onmessage = (pe: MessageEvent) => dispatch(port, pe.data, 'client');
    port.start?.();
    (self as unknown as Worker).postMessage({ id: data.id, ok: true, wt: 0 });
    return;
  }
  dispatch(self as unknown as Worker, data, 'owner');
};
