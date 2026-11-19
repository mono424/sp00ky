/**
 * Test-only: run the row worker's ops in-process behind the engine's
 * transport, in place of a real Worker. Requests and replies go through
 * `structuredClone` with their transfer lists, so a test sees what a real
 * `postMessage` would do to them: class instances lose their prototype and a
 * transferred buffer is gone from the sender's side.
 *
 * The lifecycle messages (`open`, `close`, `diag`) are answered here the way
 * `rows-worker.ts` answers them, minus OPFS and Web Locks: a bucket's files
 * are `MemoryFile`s kept in `disks`, so a reopen finds what was written.
 */
import type { SqliteTransport } from './sqlite-transport';
import { MemoryFile, RowStore } from './rows-store';
import { ROWS_PROTOCOL, RowsExec, handleReaderOp, handleRowsOp, visibleTables } from './rows-exec';

export type RowsDisks = Map<string, { a: MemoryFile; b: MemoryFile }>;

export interface RowsHost {
  disks: RowsDisks;
  /** Every message the engine sent, in order. */
  calls: { type: string; payload: any }[];
  /** The store behind the open bucket, for assertions on the file itself. */
  store(): RowStore | null;
  /** The protocol number `open` answers with; a test changes it to play an old worker. */
  proto: number;
  /** The worker's answers themselves, for a second way in (a follower's port). */
  answer: RowsAnswer;
}

/** What `rows-worker.ts` answers, for a caller that is not an engine (a fake Worker). */
export type RowsAnswer = (type: string, payload: any) => Promise<{ result: object; transfer?: Transferable[] }>;

/**
 * A `Worker` whose messages are answered in-process: what the real transport
 * needs to run unchanged. `proto` plays a worker built from another release.
 */
export class FakeRowsWorker {
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onerror: ((ev: { message: string }) => void) | null = null;
  onmessageerror: (() => void) | null = null;
  terminated = false;
  posted: { type: string; id: number }[] = [];
  private answer: RowsAnswer;
  /** One message at a time, as the worker's own queue has it. */
  private chain: Promise<unknown> = Promise.resolve();

  constructor(disks: RowsDisks, private proto?: number) {
    this.answer = rowsAnswers(disks, { proto: ROWS_PROTOCOL } as RowsHost);
  }

  postMessage(msg: { id: number; type: string; payload: unknown }, transfer?: Transferable[]): void {
    this.posted.push({ type: msg.type, id: msg.id });
    const payload = structuredClone(msg.payload, { transfer: transfer ?? [] });
    this.chain = this.chain
      .then(() => this.answer(msg.type, payload))
      .then(
        ({ result, transfer: back }) => {
          const reply = structuredClone(result, { transfer: back ?? [] }) as Record<string, unknown>;
          if (msg.type === 'open' && this.proto !== undefined) reply.proto = this.proto;
          this.onmessage?.({ data: { id: msg.id, ok: true, wt: 0, ...reply } });
        },
        (error: unknown) => this.onmessage?.({ data: { id: msg.id, ok: false, error: String(error) } })
      );
  }

  terminate(): void {
    this.terminated = true;
  }
}

export function inProcessRows(engine: unknown, disks: RowsDisks = new Map()): RowsHost {
  const host = { disks, calls: [], store: () => null, proto: ROWS_PROTOCOL } as unknown as RowsHost;
  const answer = rowsAnswers(disks, host);
  host.answer = answer;

  (engine as { createTransport: () => SqliteTransport }).createTransport = () => {
    let closed = false;
    return {
      kind: 'worker',
      get connected() {
        return !closed;
      },
      call: async <T>(type: string, payload?: unknown, transfer?: Transferable[]) => {
        const received = structuredClone(payload, { transfer: transfer ?? [] });
        host.calls.push({ type, payload: received });
        const { result, transfer: back } = await answer(type, received);
        return structuredClone(result, { transfer: back ?? [] }) as T;
      },
      failAll() {},
      close() {
        closed = true;
      },
    } as SqliteTransport;
  };
  return host;
}

/** The worker's answers over `disks`. `host.store` is pointed at the open store. */
function rowsAnswers(disks: RowsDisks, host: RowsHost): RowsAnswer {
  let store: RowStore | null = null;
  let exec: RowsExec | null = null;
  let openedAs: string | null = null;
  let reading = false;
  host.store = () => store;
  return async (type, payload) => {
    switch (type) {
      case 'open': {
        // As in the worker: a second open of the open bucket changes nothing.
        const key = `${payload.dbName}${payload.reader ? ' (reader)' : ''}`;
        if (!store || openedAs !== key) {
          store?.checkpoint();
          if (!disks.has(payload.dbName)) disks.set(payload.dbName, { a: new MemoryFile(), b: new MemoryFile() });
          const files = disks.get(payload.dbName)!;
          reading = payload.reader === true;
          store = await RowStore.open({ open: (name) => files[name] }, { reader: reading });
          exec = new RowsExec(store, reading);
          openedAs = key;
        }
        return { result: { persisted: payload.useOpfs === true, proto: host.proto, workerUrl: 'https://app.test/assets/rows-worker.js' } };
      }
      case 'warm':
        exec?.warm();
        return { result: {} };
      case 'close':
      case 'shutdown':
        store?.checkpoint();
        store = null;
        exec = null;
        openedAs = null;
        return { result: {} };
      case 'relock':
        return { result: {} };
      case 'diag': {
        if (!store) throw new Error('rows: store not open');
        const open = store;
        return {
          result: {
            stats: open.stats(),
            tableCounts: payload?.tableCounts ? visibleTables(open).map((table) => ({ table, rows: open.count(table) })) : undefined,
          },
        };
      }
      default: {
        if (!store || !exec) throw new Error('rows: store not open');
        const reply = reading ? await handleReaderOp(exec, store, type, payload) : await handleRowsOp(exec, store, type, payload);
        if (!reply) throw new Error(`rows worker: unknown message ${type}`);
        return reply;
      }
    }
  };
}
