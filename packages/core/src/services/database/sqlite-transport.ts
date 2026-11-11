/**
 * The wire between `SqliteCacheEngine` and the SQLite worker, extracted so the
 * engine can swap it at runtime without touching any SQL logic. Two shapes:
 *
 * - {@link WorkerSqliteTransport}: owns a dedicated Worker (solo mode, and the
 *   leader tab in shared-tabs mode). Controls the worker's lifecycle and can
 *   forward follower MessagePorts into it (`add-client`).
 * - {@link PortSqliteTransport}: speaks the same request/response protocol over
 *   a MessagePort handed out by the leader (follower tabs). No lifecycle
 *   control; the port dying surfaces through `onPortDead`.
 *
 * Both keep the pending-map semantics the engine had inline before: requests
 * keyed by a locally-minted numeric id, one resolve/reject per id, `failAll`
 * rejects everything in flight (worker crash, port loss, role change).
 */
import type { Logger } from '../logger/index';

export interface SqliteTransport {
  readonly kind: 'worker' | 'port';
  /** True while the transport can carry a request. */
  readonly connected: boolean;
  call<T = unknown>(type: string, payload?: unknown): Promise<T>;
  /** Reject every pending request with `reason`. Safe to call repeatedly. */
  failAll(reason: string): void;
  /** failAll + release the underlying channel. Terminal. `err` overrides the
   *  transport's own error shape — used for a deliberate teardown (role change)
   *  so callers see a retryable "transport lost", not "the worker crashed". */
  close(reason?: string, err?: Error): void;
}

/** Thrown into pending follower calls when the leader (or its port) goes away.
 *  Callers treat it like a transient error: the op may be retried once a new
 *  leader is attached; nothing was necessarily executed. */
export class BrokerPortClosedError extends Error {
  readonly retryable = true;
  constructor(reason: string) {
    super(`sqlite transport lost: ${reason}`);
    this.name = 'BrokerPortClosedError';
  }
}

interface Pending {
  resolve: (v: any) => void;
  reject: (e: unknown) => void;
}

/** Shared request/response bookkeeping over any postMessage-shaped channel. */
abstract class BaseTransport implements SqliteTransport {
  abstract readonly kind: 'worker' | 'port';
  protected pending = new Map<number, Pending>();
  protected seq = 0;
  protected closed = false;

  constructor(protected logger: Logger) {}

  get connected(): boolean {
    return !this.closed;
  }

  protected abstract post(msg: unknown, transfer?: Transferable[]): void;
  protected abstract makeError(reason: string): Error;

  protected handleMessage(data: any): void {
    const { id, ok, error, ...rest } = data ?? {};
    const p = this.pending.get(id);
    if (!p) return;
    this.pending.delete(id);
    if (ok) p.resolve(rest);
    else p.reject(new Error(error));
  }

  call<T = unknown>(type: string, payload?: unknown): Promise<T> {
    if (this.closed) return Promise.reject(this.makeError('transport closed'));
    const id = ++this.seq;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      try {
        this.post({ id, type, payload });
      } catch (e) {
        this.pending.delete(id);
        reject(e);
      }
    });
  }

  failAll(reason: string, err?: Error): void {
    if (this.pending.size === 0) return;
    const e = err ?? this.makeError(reason);
    for (const [, p] of this.pending) p.reject(e);
    this.pending.clear();
  }

  close(reason = 'closed', err?: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.failAll(reason, err);
  }
}

export class WorkerSqliteTransport extends BaseTransport {
  readonly kind = 'worker' as const;
  private worker: Worker;
  /** Fired when the worker reports it fenced itself after a leadership steal
   *  (see sqlite-worker.ts). The engine must treat the store as gone. */
  onLockLost: ((reason: string) => void) | null = null;

  constructor(logger: Logger, createWorker?: () => Worker) {
    super(logger);
    // Source references the `.ts` so the monorepo's src-bundling consumers
    // (e.g. the example app, which aliases `@spooky-sync/core` to `src`)
    // resolve it — Vite handles `.ts` workers. For the published package, the
    // tsdown build rewrites this to `./sqlite-worker.js` (the top-level
    // emitted entry; see tsdown.config.ts), which the flat `dist/index.js`
    // resolves. The worker (+ `@sqlite.org/sqlite-wasm`) still loads lazily —
    // only when `localEngine: 'sqlite'` is used.
    // `createWorker` (`database.workers.sqlite`) is for bundlers that do not
    // bundle this expression from a dependency (the Angular CLI); it starts
    // the same entry, `@spooky-sync/core/sqlite-worker`, bundled by the app.
    this.worker =
      createWorker?.() ??
      new Worker(new URL('./sqlite-worker.ts', import.meta.url), { type: 'module' });
    this.worker.onmessage = (ev: MessageEvent) => {
      // Unsolicited worker-initiated notification, not a reply.
      if (ev.data?.type === 'lock-lost') {
        this.logger.error(
          { reason: ev.data.reason, Category: 'sp00ky-client::SqliteCacheEngine::worker' },
          'SQLite worker fenced after leadership loss'
        );
        this.failAll('worker fenced');
        this.onLockLost?.(String(ev.data.reason ?? 'lock-lost'));
        return;
      }
      this.handleMessage(ev.data);
    };
    // Surface a worker crash (wasm abort / OOM) instead of leaving every
    // pending call hung forever — reject them all with a clear error, AND
    // close: `failAll` alone left `connected` true, so every later call was
    // posted to the dead worker and parked in `pending` for good. Closed, the
    // engine sees `!connected` and spawns a fresh worker on its next open.
    const crash = (msg: string) => {
      this.logger.error(
        { err: this.makeError(msg), Category: 'sp00ky-client::SqliteCacheEngine::worker' },
        'Worker error'
      );
      this.close(msg);
    };
    this.worker.onerror = (e: ErrorEvent) => crash(e.message || 'onerror');
    this.worker.onmessageerror = () => crash('messageerror');
  }

  protected post(msg: unknown, transfer?: Transferable[]): void {
    if (transfer) this.worker.postMessage(msg, transfer);
    else this.worker.postMessage(msg);
  }

  protected makeError(reason: string): Error {
    return new Error(`SQLite worker crashed: ${reason}`);
  }

  /** Forward a follower's MessagePort into the worker as an extra client. */
  addClientPort(clientId: string, port: MessagePort): Promise<void> {
    if (this.closed) return Promise.reject(this.makeError('transport closed'));
    const id = ++this.seq;
    return new Promise<void>((resolve, reject) => {
      this.pending.set(id, { resolve: () => resolve(), reject });
      this.worker.postMessage({ id, type: 'add-client', payload: { clientId } }, [port]);
    });
  }

  removeClientPort(clientId: string): Promise<void> {
    return this.call('remove-client', { clientId }).then(() => undefined);
  }

  /** Ask the worker to close + pauseVfs + self-close (graceful pagehide). */
  shutdown(): Promise<void> {
    return this.call('shutdown').then(() => undefined);
  }

  close(reason = 'closed', err?: Error): void {
    if (this.closed) return;
    super.close(reason, err);
    this.worker.terminate();
  }
}

export class PortSqliteTransport extends BaseTransport {
  readonly kind = 'port' as const;

  constructor(
    private port: MessagePort,
    private onPortDead: (reason: string) => void,
    logger: Logger
  ) {
    super(logger);
    port.onmessage = (ev: MessageEvent) => this.handleMessage(ev.data);
    port.onmessageerror = () => this.dead('messageerror');
    port.start?.();
  }

  /** The broker/coordinator learned the leader is gone; the port itself has no
   *  close event, so the coordinator calls this explicitly. */
  markDead(reason: string): void {
    this.dead(reason);
  }

  private dead(reason: string, err?: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.failAll(reason, err);
    try {
      this.port.close();
    } catch {
      /* ignore */
    }
    this.onPortDead(reason);
  }

  protected post(msg: unknown): void {
    this.port.postMessage(msg);
  }

  protected makeError(reason: string): Error {
    return new BrokerPortClosedError(reason);
  }

  close(reason = 'closed', err?: Error): void {
    this.dead(reason, err);
  }
}
