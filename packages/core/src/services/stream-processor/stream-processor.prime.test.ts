import { describe, it, expect, vi, beforeEach } from 'vitest';

// The boot-time prime: the circuit fills from the LOCAL store (a snapshot plus
// a reconcile against `(id, rv)`, or every cached row) instead of from the
// network. Before this, a reload left the circuit empty, every id in the
// server's list_ref classified as `added`, and the whole working set was
// re-downloaded, re-written and re-ingested.

const processorInstances: any[] = [];

vi.mock('@spooky-sync/ssp-wasm', () => ({
  default: vi.fn(async () => {}),
  Sp00kyProcessor: vi.fn(() => {
    const instance = {
      ingest: vi.fn(() => []),
      ingest_many: vi.fn(() => []),
      register_view: vi.fn(() => ({ query_id: 'q1', result_hash: '', result_data: [] })),
      unregister_view: vi.fn(),
      set_permissions: vi.fn(),
      set_projection: vi.fn(),
      save_store_state: vi.fn(() => new Uint8Array([1])),
      load_store_state: vi.fn(() => [{ query_id: 'q1', result_hash: '', result_data: [['thing:a', 1]] }]),
      reconcile: vi.fn(() => ({ fetch: [], deleted: 0, updates: [] })),
      free: vi.fn(),
    };
    processorInstances.push(instance);
    return instance;
  }),
}));

import { StreamProcessorService, CIRCUIT_SNAPSHOT_FORMAT } from './index';
import type { StreamUpdate } from './index';

const silentLogger = {
  child: () => silentLogger,
  trace: () => {},
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
} as any;

function row(id: string, rv: number) {
  return { id, _00_rv: rv, title: id };
}

function makeService(opts: { snapshot?: { bytes: Uint8Array; meta: any } | null; engineKind?: string } = {}) {
  const versions: Record<string, [string, number][]> = {
    thing: [
      ['thing:a', 1],
      ['thing:b', 2],
    ],
    other: [['other:x', 5]],
  };
  const db = {
    epoch: 0,
    engineKind: opts.engineKind ?? 'sqlite',
    scanVersions: vi.fn(async (tables: string[]) =>
      Object.fromEntries(tables.map((t) => [t, versions[t] ?? []]))
    ),
    selectByIds: vi.fn(async (table: string, ids: string[]) =>
      ids.map((id) => row(id, versions[table]?.find(([i]) => i === id)?.[1] ?? 0))
    ),
    getSnapshot: vi.fn(async () => opts.snapshot ?? null),
    putSnapshot: vi.fn(async () => {}),
  };
  const service = new StreamProcessorService({} as any, db as any, silentLogger);
  service.configureCircuitPersistence(true);
  const received: StreamUpdate[] = [];
  service.addReceiver({ onStreamUpdate: (u) => received.push(u) });
  return { service, db, received };
}

const goodMeta = { formatVersion: CIRCUIT_SNAPSHOT_FORMAT, schemaHash: 'h', savedAt: 1 };
const ctx = () => ({
  tables: ['thing', 'other'],
  schemaHash: 'h',
  pendingIds: new Set<string>(),
  onVersions: vi.fn(),
});

beforeEach(() => {
  processorInstances.length = 0;
});

describe('StreamProcessorService.primeFromLocal', () => {
  it('without a snapshot, ingests every cached row and reports their versions', async () => {
    const { service, db } = makeService();
    await service.init();
    const c = ctx();
    await service.primeFromLocal(c);
    const p = processorInstances[0];
    expect(p.load_store_state).not.toHaveBeenCalled();
    expect(db.selectByIds).toHaveBeenCalledWith('thing', ['thing:a', 'thing:b']);
    expect(db.selectByIds).toHaveBeenCalledWith('other', ['other:x']);
    const items = p.ingest_many.mock.calls.flatMap((call: any[]) => call[0]);
    expect(items.map((i: any) => [i.table, i.op, i.id])).toEqual([
      ['thing', 'CREATE', 'thing:a'],
      ['thing', 'CREATE', 'thing:b'],
      ['other', 'CREATE', 'other:x'],
    ]);
    expect(c.onVersions).toHaveBeenCalledWith('thing', [
      ['thing:a', 1],
      ['thing:b', 2],
    ]);
    await expect(service.whenPrimed()).resolves.toBeUndefined();
  });

  it('resets tables whose columns changed BEFORE reading the versions it reports', async () => {
    const { service, db } = makeService();
    const order: string[] = [];
    // The engine zeroes `thing`'s cached versions (a column was added).
    const reset = vi.fn(async () => {
      order.push('reset');
      db.scanVersions.mockImplementation(async (tables: string[]) => {
        order.push('scan');
        return Object.fromEntries(tables.map((t) => [t, t === 'thing' ? [['thing:a', 0], ['thing:b', 0]] : [['other:x', 5]]]));
      });
      return ['thing'];
    });
    (db as any).resetChangedTables = reset;
    await service.init();
    const c = { ...ctx(), pendingIds: new Set(['thing:b']), columnFingerprints: { thing: 'layout,title', other: 'title' } };
    await service.primeFromLocal(c);
    expect(reset).toHaveBeenCalledWith({ thing: 'layout,title', other: 'title' }, new Set(['thing:b']));
    expect(order).toEqual(['reset', 'scan']);
    // Reported as stale, so the fetch plan pulls the bodies again.
    expect(c.onVersions).toHaveBeenCalledWith('thing', [['thing:a', 0]]);
  });

  it('keeps priming when the column check fails', async () => {
    const { service, db } = makeService();
    (db as any).resetChangedTables = vi.fn(async () => {
      throw new Error('worker gone');
    });
    await service.init();
    const c = { ...ctx(), columnFingerprints: { thing: 'title' } };
    await service.primeFromLocal(c);
    expect(c.onVersions).toHaveBeenCalledWith('thing', [
      ['thing:a', 1],
      ['thing:b', 2],
    ]);
  });

  it('with a snapshot, restores it under the registered views and only fetches what reconcile asks for', async () => {
    const { service, db, received } = makeService({ snapshot: { bytes: new Uint8Array([7]), meta: goodMeta } });
    await service.init();
    const p = processorInstances[0];
    p.reconcile.mockImplementation((table: string) =>
      table === 'thing'
        ? { fetch: ['thing:b'], deleted: 1, updates: [{ query_id: 'q1', result_hash: '', result_data: [] }] }
        : { fetch: [], deleted: 0, updates: [] }
    );
    await service.primeFromLocal(ctx());
    expect(p.load_store_state).toHaveBeenCalledWith(new Uint8Array([7]));
    // The restore's re-primed view result and reconcile's retraction reached
    // the receivers.
    expect(received.map((u) => u.queryHash)).toEqual(['q1', 'q1']);
    expect(p.reconcile).toHaveBeenCalledWith('thing', [
      ['thing:a', 1],
      ['thing:b', 2],
    ]);
    expect(db.selectByIds).toHaveBeenCalledTimes(1);
    expect(db.selectByIds).toHaveBeenCalledWith('thing', ['thing:b']);
    const items = p.ingest_many.mock.calls.flatMap((call: any[]) => call[0]);
    expect(items.map((i: any) => i.id)).toEqual(['thing:b']);
  });

  it('ignores a snapshot written under another schema or format', async () => {
    const { service } = makeService({
      snapshot: { bytes: new Uint8Array([7]), meta: { ...goodMeta, schemaHash: 'other' } },
    });
    await service.init();
    await service.primeFromLocal(ctx());
    expect(processorInstances[0].load_store_state).not.toHaveBeenCalled();
    expect(processorInstances[0].ingest_many).toHaveBeenCalled();
  });

  it('leaves out ids with a pending local mutation when reporting versions', async () => {
    const { service } = makeService();
    await service.init();
    const c = ctx();
    c.pendingIds.add('thing:b');
    await service.primeFromLocal(c);
    expect(c.onVersions).toHaveBeenCalledWith('thing', [['thing:a', 1]]);
  });

  it('aborts when the store epoch moves under it (bucket switch)', async () => {
    const { service, db } = makeService();
    await service.init();
    db.scanVersions.mockImplementation(async () => {
      db.epoch++;
      return { thing: [['thing:a', 1]], other: [] };
    });
    const c = ctx();
    await service.primeFromLocal(c);
    expect(db.selectByIds).not.toHaveBeenCalled();
    expect(processorInstances[0].ingest_many).not.toHaveBeenCalled();
    expect(c.onVersions).not.toHaveBeenCalled();
  });

  it('is a no-op on an engine without scanVersions', async () => {
    const service = new StreamProcessorService({} as any, {} as any, silentLogger);
    await service.init();
    await service.primeFromLocal(ctx());
    expect(processorInstances[0].ingest_many).not.toHaveBeenCalled();
  });

  it('widens projected rows when a new view evaluates fields they lack', async () => {
    const { service, db } = makeService();
    await service.init();
    const p = processorInstances[0];
    p.register_view.mockReturnValue({
      query_id: 'q2',
      result_hash: '',
      result_data: [],
      missing_fields: { thing: ['score'] },
    });
    service.registerQueryPlan({
      queryHash: 'q2',
      surql: 'SELECT * FROM thing ORDER BY score;',
      params: {},
      ttl: '10m',
      lastActiveAt: new Date(),
      localArray: [],
      remoteArray: [],
      meta: { tableName: 'thing' },
    } as any);
    await vi.waitFor(() => expect(p.ingest_many).toHaveBeenCalled());
    expect(db.selectByIds).toHaveBeenCalledWith('thing', ['thing:a', 'thing:b'], { select: ['score'] });
    const items = p.ingest_many.mock.calls.flatMap((call: any[]) => call[0]);
    expect(items.map((i: any) => i.op)).toEqual(['MERGE', 'MERGE']);
  });
});
