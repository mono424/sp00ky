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
import { RowsExec, handleRowsOp } from './rows-exec';

export type RowsDisks = Map<string, { a: MemoryFile; b: MemoryFile }>;

export interface RowsHost {
  disks: RowsDisks;
  /** Every message the engine sent, in order. */
  calls: { type: string; payload: any }[];
  /** The store behind the open bucket, for assertions on the file itself. */
  store(): RowStore | null;
}

export function inProcessRows(engine: unknown, disks: RowsDisks = new Map()): RowsHost {
  let store: RowStore | null = null;
  let exec: RowsExec | null = null;
  const host: RowsHost = { disks, calls: [], store: () => store };

  const answer = async (type: string, payload: any): Promise<{ result: object; transfer?: Transferable[] }> => {
    switch (type) {
      case 'open': {
        store?.checkpoint();
        if (!disks.has(payload.dbName)) disks.set(payload.dbName, { a: new MemoryFile(), b: new MemoryFile() });
        const files = disks.get(payload.dbName)!;
        store = await RowStore.open({ open: (name) => files[name] });
        exec = new RowsExec(store);
        return { result: { persisted: payload.useOpfs === true } };
      }
      case 'close':
      case 'shutdown':
        store?.checkpoint();
        store = null;
        exec = null;
        return { result: {} };
      case 'relock':
        return { result: {} };
      case 'diag': {
        if (!store) throw new Error('rows: store not open');
        const open = store;
        return {
          result: {
            stats: open.stats(),
            tableCounts: payload?.tableCounts ? open.tableNames().map((table) => ({ table, rows: open.count(table) })) : undefined,
          },
        };
      }
      default: {
        if (!store || !exec) throw new Error('rows: store not open');
        const reply = await handleRowsOp(exec, store, type, payload);
        if (!reply) throw new Error(`rows worker: unknown message ${type}`);
        return reply;
      }
    }
  };

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
