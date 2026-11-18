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
 *   { id, type: 'open', payload: { dbName, useOpfs, workerLockName?, openOptions? } }
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
import { CHECKPOINT_TAIL_BYTES, CHECKPOINT_TAIL_OPS, ROWS_CLIENT_OPS, RowsExec, handleRowsOp, maintenance } from './rows-exec';

type PostTarget = Pick<MessagePort, 'postMessage'>;

interface OpenOptions {
  maxAttempts?: number;
  backoffMs?: number[];
  disallowMemoryFallback?: boolean;
}

/** How long a write may sit with the operating system before it is flushed. */
const FLUSH_LAG_MS = 100;
/** Quiet time after which the index is persisted and the store tidied. */
const IDLE_MS = 500;

class OpfsFile implements SyncFile {
  constructor(private handle: FileSystemSyncAccessHandle) {}
  read(buffer: Uint8Array, at: number): number {
    return this.handle.read(buffer, { at });
  }
  write(buffer: Uint8Array, at: number): number {
    const written = this.handle.write(buffer, { at });
    // A short write is a full disk: fail the batch rather than store part of it.
    if (written !== buffer.length) throw new Error(`rows: wrote ${written} of ${buffer.length} bytes (storage full?)`);
    return written;
  }
  size(): number {
    return this.handle.getSize();
  }
  truncate(size: number): void {
    this.handle.truncate(size);
  }
  flush(): void {
    this.handle.flush();
  }
  close(): void {
    this.handle.close();
  }
}

let store: RowStore | null = null;
let exec: RowsExec | null = null;
/** Handles held for the open store, closed with it. */
let openFiles: SyncFile[] = [];

async function opfsIo(bucketId: string): Promise<StoreIo> {
  const root = await navigator.storage.getDirectory();
  const dir = await root.getDirectoryHandle(bucketDirName(bucketId), { create: true });
  return {
    open: async (name) => {
      const handle = await (await dir.getFileHandle(name, { create: true })).createSyncAccessHandle();
      const file = new OpfsFile(handle);
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
  opts: OpenOptions = {}
): Promise<{ persisted: boolean; opfsError?: string }> {
  if (!useOpfs) {
    store = await RowStore.open(memoryIo());
    exec = new RowsExec(store);
    return { persisted: false };
  }
  const maxAttempts = Math.max(1, opts.maxAttempts ?? 3);
  const backoffMs = opts.backoffMs ?? [250, 500];
  let lastError = 'unknown error';
  let attempts = 0;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    attempts = attempt;
    try {
      if (!navigator.storage?.getDirectory) throw new Error('Missing required OPFS APIs');
      store = await RowStore.open(await opfsIo(bucketId));
      exec = new RowsExec(store);
      return { persisted: true };
    } catch (e) {
      closeFiles();
      lastError = errMessage(e);
      if (attempt === maxAttempts || lastError.includes('Missing required OPFS APIs')) break;
      await new Promise((resolve) => setTimeout(resolve, backoffMs[Math.min(attempt - 1, backoffMs.length - 1)] ?? 0));
    }
  }
  if (opts.disallowMemoryFallback) throw new Error(`opfs-unavailable: ${lastError} (after ${attempts} attempts)`);
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
  for (let step = maintenance(store.stats()); step && store; step = maintenance(store.stats())) {
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

function enqueue<T>(fn: () => T | Promise<T>): Promise<T> {
  const result = opChain.then(fn, fn);
  opChain = result.then(
    () => undefined,
    () => undefined
  );
  return result;
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
      closeStore();
      return { result: await openStore(payload.dbName, payload.useOpfs, payload.openOptions) };
    }
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
      const stats = store.stats();
      const tableCounts = payload?.tableCounts
        ? store.tableNames().map((table) => ({ table, rows: store!.count(table) }))
        : undefined;
      return { result: { stats, tableCounts } };
    }
    default: {
      if (!store || !exec) throw new Error('rows: store not open');
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
