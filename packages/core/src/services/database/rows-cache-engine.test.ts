import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { RecordId } from 'surrealdb';
import type { QueryPlan } from '@spooky-sync/query-builder';
import {
  PaintCache,
  RowsCacheEngine,
  ROWS_DURABLE_TABLES,
  ROWS_PREWARM_KEY,
  ROWS_PREWARM_SNIPPET,
  packWrites,
  prewarmRows,
} from './rows-cache-engine';
import { createLocalEngine } from './engine-factory';
import { FakeRowsWorker, inProcessRows, type RowsDisks, type RowsHost } from './rows-transport.fixture';
import { ROWS_CLIENT_OPS, maintenance } from './rows-exec';
import type { StoreStats } from './rows-store';
import { columnFingerprints } from './sqlite-column-reset';
import { surql } from '../../utils/surql';
import type { Row } from './cache-engine';

const logger = {
  info() {},
  warn() {},
  error() {},
  debug() {},
  trace() {},
  child() {
    return logger;
  },
} as any;

let engine: RowsCacheEngine;
let host: RowsHost;

async function boot(disks?: RowsDisks, bucket = 'user_a'): Promise<void> {
  engine = new RowsCacheEngine({ store: 'indexeddb' } as any, logger, { useOpfs: true });
  host = inProcessRows(engine, disks);
  await engine.connect(bucket);
}

beforeEach(() => boot());

describe('RowsCacheEngine: verbs', () => {
  it('is what `localEngine: "rows"` builds', () => {
    const built = createLocalEngine('rows', { store: 'indexeddb' } as any, logger);
    expect(built).toBeInstanceOf(RowsCacheEngine);
    expect(built.engineKind).toBe('rows');
    expect(built.usesSurqlSchema).toBe(false);
  });

  it('writes, reads, merges and deletes rows', async () => {
    await engine.upsert('game', 'game:a', { title: 'Alpha', moves: 3, _00_rv: 2 }, 'replace');
    await engine.upsert('game', new RecordId('game', 'b'), { title: 'Beta', white: new RecordId('user', '1') }, 'replace');
    expect(await engine.getById('game', 'game:a')).toEqual({ id: 'game:a', title: 'Alpha', moves: 3, _00_rv: 2 });
    // A record link is stored, and read back, as its `table:id` string.
    expect(await engine.getById('game', new RecordId('game', 'b'))).toEqual({ id: 'game:b', title: 'Beta', white: 'user:1' });
    expect(await engine.getById('game', 'game:none')).toBeNull();

    await engine.upsert('game', 'game:a', { moves: 4, extra: { k: 1 }, title: undefined }, 'merge');
    expect(await engine.getById('game', 'game:a')).toEqual({ id: 'game:a', moves: 4, _00_rv: 2, extra: { k: 1 } });

    await engine.delete('game', 'game:a');
    expect(await engine.getById('game', 'game:a')).toBeNull();
    expect(await engine.selectByIds('game', ['game:a', 'game:b'])).toEqual([{ id: 'game:b', title: 'Beta', white: 'user:1' }]);
  });

  it('keeps bytes as bytes', async () => {
    const state = new Uint8Array([1, 2, 3, 254, 255]);
    await engine.upsert('doc', 'doc:1', { state, sdk: state.slice().buffer, n: 1 }, 'replace');
    const row = (await engine.getById('doc', 'doc:1'))!;
    expect(row.state).toEqual(state);
    // The SDK hands bytes over as an ArrayBuffer; they come back as a Uint8Array.
    expect(row.sdk).toEqual(state);
    expect((await engine.selectByIds('doc', ['doc:1']))[0].state).toEqual(state);
    expect((await engine.select({ table: 'doc' }))[0].state).toEqual(state);
  });

  it('selects by plan with record ids in the filter, the params and the window', async () => {
    await engine.execute(
      surql.seal(surql.tx([surql.upsertMerge('a', 'da'), surql.upsertMerge('b', 'db'), surql.upsertMerge('c', 'dc')])),
      {
        a: new RecordId('game', 'a'),
        da: { white: new RecordId('user', '1'), moves: 10 },
        b: new RecordId('game', 'b'),
        db: { white: new RecordId('user', '2'), moves: 20 },
        c: new RecordId('game', 'c'),
        dc: { white: new RecordId('user', '1'), moves: 30 },
      }
    );
    const byWhite: QueryPlan = { table: 'game', where: [{ field: 'white', op: '=', value: new RecordId('user', '1') }] };
    expect((await engine.select(byWhite)).map((r) => r.id)).toEqual(['game:a', 'game:c']);
    const byParam: QueryPlan = { table: 'game', where: [{ field: 'white', op: '=', value: undefined, paramRef: 'who' }] };
    expect((await engine.select(byParam, { who: new RecordId('user', '2') })).map((r) => r.id)).toEqual(['game:b']);
    const window: QueryPlan = { table: 'game', ids: [new RecordId('game', 'c'), 'game:a'], select: ['moves'] };
    expect(await engine.select(window)).toEqual([
      { id: 'game:c', moves: 30 },
      { id: 'game:a', moves: 10 },
    ]);
  });

  it('resolves relations in one round trip', async () => {
    await engine.upsert('user', 'user:1', { name: 'Ada' }, 'replace');
    await engine.upsert('game', 'game:a', { white: 'user:1' }, 'replace');
    await engine.upsert('comment', 'comment:1', { game: 'game:a', rank: 2 }, 'replace');
    await engine.upsert('comment', 'comment:2', { game: 'game:a', rank: 1 }, 'replace');
    host.calls.length = 0;
    const rows = await engine.select({
      table: 'game',
      relations: [
        { alias: 'white', table: 'user', cardinality: 'one', foreignKeyField: 'white' },
        { alias: 'comments', table: 'comment', cardinality: 'many', foreignKeyField: 'game', orderBy: [['rank', 'asc']] },
      ],
    });
    expect(rows).toEqual([
      {
        id: 'game:a',
        white: { id: 'user:1', name: 'Ada' },
        comments: [
          { id: 'comment:2', game: 'game:a', rank: 1 },
          { id: 'comment:1', game: 'game:a', rank: 2 },
        ],
      },
    ]);
    expect(host.calls.map((c) => c.type)).toEqual(['select']);
    expect(await engine.fetchRelation({ table: 'comment', matchField: 'game', keys: [new RecordId('game', 'a')], orderBy: [['rank', 'desc']] })).toEqual([
      { id: 'comment:1', game: 'game:a', rank: 2 },
      { id: 'comment:2', game: 'game:a', rank: 1 },
    ]);
  });

  it('applies RFC 6902 patches and runs a transaction of verbs', async () => {
    await engine.upsert('game', 'game:a', { tags: ['x'], n: 1 }, 'replace');
    await engine.patch('game', 'game:a', [
      { op: 'add', path: '/tags/-', value: 'y' },
      { op: 'replace', path: '/n', value: 2 },
    ]);
    expect(await engine.getById('game', 'game:a')).toEqual({ id: 'game:a', tags: ['x', 'y'], n: 2 });
    await engine.transaction(async (tx) => {
      await tx.upsert('game', 'game:b', { n: 1 }, 'replace');
      await tx.patch('game', 'game:b', [{ op: 'add', path: '/m', value: true }]);
      await tx.delete('game', 'game:a');
    });
    expect(await engine.select({ table: 'game' })).toEqual([{ id: 'game:b', n: 1, m: true }]);
  });
});

describe('RowsCacheEngine: the SurrealQL statements the client emits', () => {
  it('creates a row and its outbox entry in one atomic, flushed batch', async () => {
    const file = host.disks.get('user_a')!.a;
    const flushes = file.flushes;
    host.calls.length = 0;
    const sealed = surql.seal(
      surql.tx([
        surql.createSet('id', [
          { key: 'provider', variable: 'data_provider' },
          { key: 'username', variable: 'data_username' },
        ]),
        surql.createMutation('create', 'mid', 'id', 'data'),
      ]),
      { resultIndex: 0 }
    );
    const created = (await engine.execute(sealed, {
      id: new RecordId('connection', 'CONN_abc'),
      mid: new RecordId('_00_pending_mutations', '1'),
      data_provider: 'chesscom',
      data_username: 'hikaru',
      data: { provider: 'chesscom', username: 'hikaru' },
    })) as unknown as Row;
    expect(created.id).toBe('connection:CONN_abc');
    expect(created.provider).toBe('chesscom');
    // One message for both rows, and the outbox makes it a flushed write.
    expect(host.calls.map((c) => c.type)).toEqual(['write']);
    expect(host.calls[0].payload.sync).toBe(true);
    expect(host.calls[0].payload.ops.length).toBe(2);
    expect(file.flushes).toBe(flushes + 1);
    expect(await engine.getById('connection', 'connection:CONN_abc')).toEqual({
      id: 'connection:CONN_abc',
      provider: 'chesscom',
      username: 'hikaru',
    });
    const [pending] = await engine.query<[Row[]]>('SELECT * FROM _00_pending_mutations ORDER BY id ASC');
    expect(pending.length).toBe(1);
    expect(pending[0].id).toBe('_00_pending_mutations:1');
  });

  it('does not flush a write that only copies server state', async () => {
    const file = host.disks.get('user_a')!.a;
    const flushes = file.flushes;
    await engine.execute(surql.seal(surql.tx([surql.upsertMerge('id', 'data')])), { id: new RecordId('game', 'a'), data: { n: 1 } });
    expect(host.calls.at(-1)!.payload.sync).toBe(false);
    expect(file.flushes).toBe(flushes);
    expect(ROWS_DURABLE_TABLES.has('_00_pending_mutations')).toBe(true);
  });

  it('answers the raw selects, counts and INFO', async () => {
    for (let i = 0; i < 6; i++) {
      await engine.upsert('game_insight', `game_insight:${i}`, { tag: i % 2 ? 'odd' : 'even', n: i }, 'replace');
    }
    const [all] = await engine.query<[Row[]]>('SELECT * FROM game_insight');
    expect(all.map((r) => r.n)).toEqual([0, 1, 2, 3, 4, 5]);
    const [window] = await engine.query<[Row[]]>('SELECT * FROM game_insight LIMIT 2 START 3');
    expect(window.map((r) => r.n)).toEqual([3, 4]);
    const [filtered] = await engine.query<[Row[]]>("SELECT * FROM game_insight WHERE tag = 'odd' ORDER BY n DESC");
    expect(filtered.map((r) => r.n)).toEqual([5, 3, 1]);
    const [values] = await engine.query<[unknown[]]>('SELECT VALUE n FROM game_insight WHERE n >= 4');
    expect(values).toEqual([4, 5]);
    const [fields] = await engine.query<[Row[]]>('SELECT tag, n FROM game_insight LIMIT 1');
    expect(fields).toEqual([{ id: 'game_insight:0', tag: 'even', n: 0 }]);
    expect((await engine.query<[{ count: number }[]]>('SELECT count() FROM game_insight GROUP ALL'))[0]).toEqual([{ count: 6 }]);
    expect((await engine.query<[{ count: number }[]]>("SELECT count() FROM game_insight WHERE tag = 'odd' GROUP ALL"))[0]).toEqual([{ count: 3 }]);
    expect((await engine.query<[{ count: number }[]]>('SELECT count() FROM empty GROUP ALL'))[0]).toEqual([{ count: 0 }]);
    const [info] = await engine.query<[{ tables: Record<string, string> }]>('INFO FOR DB');
    expect(Object.keys(info.tables)).toEqual(['game_insight']);

    const [one] = await engine.query<[Row | null]>('SELECT * FROM ONLY $id', { id: new RecordId('game_insight', '2') });
    expect(one).toEqual({ id: 'game_insight:2', tag: 'even', n: 2 });
    const [some] = await engine.query<[Row[]]>('SELECT * FROM $ids', {
      ids: [new RecordId('game_insight', '5'), new RecordId('game_insight', 'x'), new RecordId('game_insight', '1')],
    });
    expect(some.map((r) => r.n)).toEqual([5, 1]);

    await engine.query('DELETE $id', { id: new RecordId('game_insight', '0') });
    expect((await engine.query<[{ count: number }[]]>('SELECT count() FROM game_insight GROUP ALL'))[0]).toEqual([{ count: 5 }]);
    await engine.query('DELETE game_insight');
    expect((await engine.query<[Row[]]>('SELECT * FROM game_insight'))[0]).toEqual([]);
  });

  it('runs a LET / UPDATE SET / RETURN transaction against the stored row', async () => {
    await engine.upsert('counter', 'counter:1', { n: 5, label: 'old' }, 'replace');
    const res = await engine.query<unknown[]>(
      'BEGIN TRANSACTION; LET $updated = (UPDATE $id SET n += $by, label = $label); RETURN { target: $updated }; COMMIT TRANSACTION;',
      { id: new RecordId('counter', '1'), by: 3, label: 'new' }
    );
    expect(res.at(-1)).toEqual({ target: { id: 'counter:1', n: 8, label: 'new' } });
    expect(await engine.getById('counter', 'counter:1')).toEqual({ id: 'counter:1', n: 8, label: 'new' });
  });

  it('refuses a write under a stale epoch', async () => {
    const epoch = engine.epoch;
    await engine.switchBucket('user_b');
    await expect(engine.query('DELETE game', {}, { epoch })).rejects.toThrow();
  });
});

describe('RowsCacheEngine: store lifecycle', () => {
  it('keeps each bucket apart and finds its rows again after a reopen', async () => {
    const disks: RowsDisks = new Map();
    await boot(disks, 'user_a');
    await engine.upsert('game', 'game:a', { owner: 'a' }, 'replace');
    await engine.switchBucket('user_b');
    expect(engine.currentBucketId).toBe('user_b');
    expect(await engine.getById('game', 'game:a')).toBeNull();
    await engine.upsert('game', 'game:b', { owner: 'b' }, 'replace');
    await engine.close();

    await boot(disks, 'user_a');
    expect(await engine.select({ table: 'game' })).toEqual([{ id: 'game:a', owner: 'a' }]);
    await engine.switchBucket('user_b');
    expect(await engine.select({ table: 'game' })).toEqual([{ id: 'game:b', owner: 'b' }]);
  });

  it('reports a durable store and its numbers', async () => {
    expect(engine.storageHealth).toEqual({ status: 'persistent', fallback: false });
    await engine.upsert('game', 'game:a', { n: 1 }, 'replace');
    await engine.upsert('user', 'user:1', { n: 1 }, 'replace');
    await engine.putSnapshot('circuit', new Uint8Array([1, 2, 3]), { formatVersion: 1, schemaHash: 'h', savedAt: 1 });
    const diag = await engine.getStorageDiagnostics({ tableCounts: true });
    expect(diag.engine).toBe('rows');
    expect(diag.bucketId).toBe('user_a');
    expect(diag.dbSizeBytes).toBeGreaterThan(4096);
    expect(diag.tableCounts).toEqual([
      { table: 'game', rows: 1 },
      { table: 'user', rows: 1 },
    ]);
    expect((await engine.getStorageDiagnostics()).tableCounts).toBeUndefined();
    expect(() => engine.getClient()).toThrow();
  });

  it('stores the circuit snapshot without taking the caller\'s bytes', async () => {
    const bytes = new Uint8Array(100_000);
    for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 13) & 255;
    const meta = { formatVersion: 3, schemaHash: 'abc', savedAt: 1700000000000, maxRv: { game: 9 } };
    await engine.putSnapshot('circuit', bytes, meta);
    expect(bytes.byteLength).toBe(100_000);
    const stored = (await engine.getSnapshot('circuit'))!;
    expect(stored.bytes).toEqual(bytes);
    expect(stored.meta).toEqual(meta);
    expect(await engine.getSnapshot('other')).toBeNull();
    // The snapshot's rows are no row a query returns.
    expect((await engine.query<[Row[]]>('SELECT * FROM _00_circuit_snapshot'))[0]).toEqual([]);
    await engine.deleteSnapshot('circuit');
    expect(await engine.getSnapshot('circuit')).toBeNull();
  });

  it('scans versions from the index and resets them when a table\'s columns change', async () => {
    await engine.upsert('game', 'game:a', { _00_rv: 4, title: 'a' }, 'replace');
    await engine.upsert('game', 'game:b', { _00_rv: 7, title: 'b' }, 'replace');
    await engine.upsert('user', 'user:1', { _00_rv: 2 }, 'replace');
    expect(await engine.scanVersions(['game', 'user', 'absent'])).toEqual({
      game: [
        ['game:a', 4],
        ['game:b', 7],
      ],
      user: [['user:1', 2]],
      absent: [],
    });

    const v1 = columnFingerprints([
      { name: 'game', columns: { title: 1 } },
      { name: 'user', columns: { name: 1 } },
    ]);
    // Nothing recorded yet: every table that holds rows is reset once.
    expect((await engine.resetChangedTables(v1, new Set(['game:b']))).sort()).toEqual(['game', 'user']);
    expect((await engine.scanVersions(['game', 'user']))).toEqual({
      game: [
        ['game:a', 0],
        ['game:b', 7],
      ],
      user: [['user:1', 0]],
    });
    expect(await engine.getById('game', 'game:a')).toEqual({ id: 'game:a', _00_rv: 0, title: 'a' });

    await engine.upsert('game', 'game:a', { _00_rv: 5 }, 'merge');
    await engine.upsert('user', 'user:1', { _00_rv: 3 }, 'merge');
    // Same columns: nothing to do.
    expect(await engine.resetChangedTables(v1, new Set())).toEqual([]);
    const v2 = columnFingerprints([
      { name: 'game', columns: { title: 1, layout: 1 } },
      { name: 'user', columns: { name: 1 } },
    ]);
    expect(await engine.resetChangedTables(v2, new Set())).toEqual(['game']);
    expect(await engine.scanVersions(['game', 'user'])).toEqual({
      game: [
        ['game:a', 0],
        ['game:b', 0],
      ],
      user: [['user:1', 3]],
    });
    // The recorded sets are not a row.
    expect((await engine.query<[Row[]]>('SELECT * FROM _00_schema'))[0]).toEqual([]);
  });

  it('serves a follower over a port and survives the leader handing over', async () => {
    const disks: RowsDisks = new Map();
    await boot(disks);
    const health = await engine.adoptOwner('user_a', { workerLockName: 'lock-1', allowMemoryFallback: false, resumeHeld: false });
    expect(health.role ?? 'leader').toBe('leader');
    await engine.upsert('game', 'game:a', { n: 1 }, 'replace');
    const before = engine.epoch;
    await engine.adoptOwner('user_a', { workerLockName: 'lock-2', allowMemoryFallback: false, resumeHeld: false });
    expect(engine.epoch).toBeGreaterThan(before);
    expect(await engine.getById('game', 'game:a')).toEqual({ id: 'game:a', n: 1 });
  });
});

describe('a follower that reads the store\'s file itself', () => {
  let disks: RowsDisks;
  let leader: RowsCacheEngine;
  let host: ReturnType<typeof inProcessRows>;
  /** Workers followers started for reading. */
  let readers: FakeRowsWorker[];
  const attached = { bucketId: 'user_a', storageHealth: { status: 'persistent', fallback: false } as const };

  const follower = (config: Record<string, unknown> = { followerReads: 'direct' }): RowsCacheEngine =>
    new RowsCacheEngine(
      {
        store: 'indexeddb',
        ...config,
        workers: {
          rows: () => {
            const worker = new FakeRowsWorker(disks);
            readers.push(worker);
            return worker as unknown as Worker;
          },
        },
      } as any,
      logger,
      { useOpfs: true, shared: true }
    );

  /** A port into the leader's worker, as the tabs broker hands one to a follower. */
  const leaderPort = () => {
    const port = {
      onmessage: null as null | ((ev: { data: unknown }) => void),
      onmessageerror: null,
      posted: [] as string[],
      start() {},
      close() {},
      postMessage(msg: { id: number; type: string; payload: unknown }, transfer?: Transferable[]) {
        port.posted.push(msg.type);
        const payload = structuredClone(msg.payload, { transfer: transfer ?? [] });
        void host.answer(msg.type, payload).then(
          ({ result, transfer: back }) =>
            port.onmessage?.({ data: { id: msg.id, ok: true, wt: 0, ...(structuredClone(result, { transfer: back ?? [] }) as object) } }),
          (error: unknown) => port.onmessage?.({ data: { id: msg.id, ok: false, error: String(error) } })
        );
      },
    };
    return port;
  };
  const attach = async (tab: RowsCacheEngine) => {
    const port = leaderPort();
    await tab.adoptAttached(port as unknown as MessagePort, attached, () => {});
    return port;
  };

  beforeEach(async () => {
    disks = new Map();
    readers = [];
    leader = new RowsCacheEngine({ store: 'indexeddb', followerReads: 'direct' } as any, logger, { useOpfs: true, shared: true });
    host = inProcessRows(leader, disks);
    await leader.adoptOwner('user_a', { workerLockName: 'lock-1', allowMemoryFallback: false, resumeHeld: false });
    await leader.upsert('game', 'game:a', { n: 1, white: 'user:1' }, 'replace');
  });

  it('reads from its own worker, writes through the leader, and sees every write at once', async () => {
    const tab = follower();
    const port = await attach(tab);
    expect(tab.readsDirectly).toBe(true);
    expect(readers[0].posted.map((m) => m.type)).toEqual(['open']);
    expect(await tab.getById('game', 'game:a')).toEqual({ id: 'game:a', n: 1, white: 'user:1' });

    await tab.upsert('game', 'game:b', { n: 2, white: 'user:1' }, 'replace');
    expect((await tab.selectByIds('game', ['game:a', 'game:b'])).map((r) => r.n)).toEqual([1, 2]);
    // Another tab's write, and the leader's own.
    await leader.upsert('game', 'game:c', { n: 3, white: 'user:2' }, 'replace');
    await leader.delete('game', 'game:a');
    expect((await tab.select({ table: 'game' })).map((r) => r.id)).toEqual(['game:b', 'game:c']);
    expect(await tab.scanVersions(['game'])).toEqual(await leader.scanVersions(['game']));

    // Only the write crossed to the leader.
    expect(port.posted).toEqual(['write']);
    expect(readers[0].posted.map((m) => m.type)).toEqual(['open', 'get', 'by-ids', 'select', 'versions']);
  });

  it('uses the indexes the leader keeps and builds none of its own', async () => {
    for (let i = 0; i < 80; i++) await leader.upsert('game', `game:x${i}`, { n: i, white: `user:${i % 20}` }, 'replace');
    const byWhite: QueryPlan = { table: 'game', where: [{ field: 'white', op: '=', value: 'user:7' }] };
    const byN: QueryPlan = { table: 'game', where: [{ field: 'n', op: '=', value: 7 }] };
    const tab = follower();
    const port = await attach(tab);
    const size = (): number => disks.get('user_a')!.a.size() + disks.get('user_a')!.b.size();
    const before = size();
    // A field nobody indexed yet: the follower scans, and writes nothing to find out.
    expect((await tab.select(byN)).map((r) => r.id)).toEqual(['game:x7']);
    expect(size()).toBe(before);
    expect(await tab.select(byWhite)).toEqual(await leader.select(byWhite));
    // The leader builds the index for `n`; the follower reads through it from then on.
    expect((await leader.select(byN)).map((r) => r.id)).toEqual(['game:x7']);
    expect(size()).toBeGreaterThan(before);
    expect((await tab.select(byN)).map((r) => r.id)).toEqual(['game:x7']);
    expect(port.posted).toEqual([]);
  });

  it('follows the rows through a compaction of the leader\'s', async () => {
    const tab = follower();
    await attach(tab);
    for (let i = 0; i < 60; i++) await leader.upsert('game', `game:x${i}`, { n: i, pad: 'p'.repeat(300) }, 'replace');
    expect((await tab.select({ table: 'game' })).length).toBe(61);
    for (let i = 0; i < 50; i++) await leader.delete('game', `game:x${i}`);
    await host.store()!.compact();
    expect(host.store()!.stats().file).toBe('b');
    expect(await tab.select({ table: 'game' })).toEqual(await leader.select({ table: 'game' }));
    await leader.upsert('game', 'game:z', { n: 99 }, 'replace');
    expect(await tab.getById('game', 'game:z')).toEqual({ id: 'game:z', n: 99 });
  });

  it('reads through the leader when it is not asked to read directly, or cannot', async () => {
    const plain = follower({});
    const plainPort = await attach(plain);
    expect(plain.readsDirectly).toBe(false);
    expect(readers).toEqual([]);
    expect((await plain.getById('game', 'game:a'))?.n).toBe(1);
    expect(plainPort.posted).toEqual(['get']);

    // A browser without shared handles: the reader's open fails.
    const tab = new RowsCacheEngine(
      {
        store: 'indexeddb',
        followerReads: 'direct',
        workers: {
          rows: () => {
            const worker = new FakeRowsWorker(disks);
            worker.postMessage = (msg: { id: number }) =>
              queueMicrotask(() => worker.onmessage?.({ data: { id: msg.id, ok: false, error: 'opfs-unavailable: shared access handles' } }));
            readers.push(worker);
            return worker as unknown as Worker;
          },
        },
      } as any,
      logger,
      { useOpfs: true, shared: true }
    );
    const port = await attach(tab);
    expect(tab.readsDirectly).toBe(false);
    expect(readers[0].terminated).toBe(true);
    expect((await tab.getById('game', 'game:a'))?.n).toBe(1);
    expect(port.posted).toEqual(['get']);
  });

  it('asks the leader when its reader fails, and from then on', async () => {
    const tab = follower();
    const port = await attach(tab);
    expect((await tab.getById('game', 'game:a'))?.n).toBe(1);
    readers[0].postMessage = () => readers[0].onerror?.({ message: 'out of memory' });
    expect((await tab.getById('game', 'game:a'))?.n).toBe(1);
    expect(tab.readsDirectly).toBe(false);
    expect(readers[0].terminated).toBe(true);
    expect((await tab.selectByIds('game', ['game:a'])).length).toBe(1);
    expect(port.posted).toEqual(['get', 'by-ids']);
  });

  it('lets go of the reader when the role changes', async () => {
    const tab = follower();
    await attach(tab);
    tab.onLeaderLost('leader tab closed');
    expect(tab.readsDirectly).toBe(false);
    expect(readers[0].terminated).toBe(true);
    // A new leader, a new reader.
    await attach(tab);
    expect(tab.readsDirectly).toBe(true);
    expect(readers.length).toBe(2);
    await tab.close();
    expect(readers[1].terminated).toBe(true);
  });
});

describe('packWrites', () => {
  it('lays every row\'s text in one buffer and says where', () => {
    const { wire, buf, sync } = packWrites([
      { kind: 'upsert', table: 'game', key: 'game:a', data: { title: 'Épée \u{1f600}', _00_rv: 3 }, mode: 'replace' },
      { kind: 'upsert', table: 'game', key: 'game:b', data: { n: 1, gone: undefined, bytes: new Uint8Array([7]) }, mode: 'merge' },
      { kind: 'delete', table: 'game', key: 'game:c' },
      { kind: 'clear', table: 'old' },
    ]);
    expect(sync).toBe(false);
    const text = (off: number, len: number): string => new TextDecoder().decode(new Uint8Array(buf!, off, len));
    expect(wire[0].slice(0, 5)).toEqual(['put', 'game', 'game:a', 3, 0]);
    expect(JSON.parse(text(wire[0][5] as number, wire[0][6] as number))).toEqual({ title: 'Épée \u{1f600}', _00_rv: 3, id: 'game:a' });
    // A merge that clears a field carries a patch beside the row to insert.
    expect(wire[1].slice(0, 5)).toEqual(['merge', 'game', 'game:b', 0, 1]);
    expect(JSON.parse(text(wire[1][5] as number, wire[1][6] as number))).toEqual({ n: 1, bytes: { __u8: 'Bw==' }, id: 'game:b' });
    expect(JSON.parse(text(wire[1][7] as number, wire[1][8] as number))).toEqual({ n: 1, bytes: { __u8: 'Bw==' }, id: 'game:b', gone: null });
    expect(wire[2]).toEqual(['del', 'game', 'game:c']);
    expect(wire[3]).toEqual(['clear', 'old']);
  });

  it('asks for a flush when a table of unsent work is written, deleted from or cleared', () => {
    expect(packWrites([{ kind: 'upsert', table: '_00_pending_mutations', key: '_00_pending_mutations:1', data: {}, mode: 'replace' }]).sync).toBe(true);
    expect(packWrites([{ kind: 'delete', table: '_00_pending_writes', key: 'x' }]).sync).toBe(true);
    expect(packWrites([{ kind: 'clear', table: '_00_crdt_local' }]).sync).toBe(true);
    expect(packWrites([{ kind: 'delete', table: 'game', key: 'x' }])).toEqual({ wire: [['del', 'game', 'x']], buf: null, sync: false });
    expect(packWrites([])).toEqual({ wire: [], buf: null, sync: false });
  });
});

describe('row store housekeeping', () => {
  const stats = (over: Partial<StoreStats>): StoreStats => ({
    file: 'a',
    fileBytes: 0,
    liveBytes: 0,
    indexBytes: 0,
    garbageBytes: 0,
    tailBytes: 0,
    tailOps: 0,
    tables: 0,
    rebuilt: false,
    ...over,
  });

  it('persists the index first, then compacts once dead bytes pass half the live ones', () => {
    expect(maintenance(stats({}))).toBeNull();
    expect(maintenance(stats({ tailBytes: 1, garbageBytes: 1 << 30 }))).toBe('checkpoint');
    // Small stores are left alone however fragmented.
    expect(maintenance(stats({ liveBytes: 1 << 20, garbageBytes: 3 << 20 }))).toBeNull();
    expect(maintenance(stats({ liveBytes: 40 << 20, garbageBytes: 19 << 20 }))).toBeNull();
    expect(maintenance(stats({ liveBytes: 40 << 20, garbageBytes: 21 << 20 }))).toBe('compact');
  });

  it('compacts a rewritten store down to its rows and keeps serving it', async () => {
    const row = (i: number, round: number): Row => ({ n: i, round, pad: 'x'.repeat(2000) });
    for (let round = 0; round < 6; round++) {
      const vars: Record<string, unknown> = {};
      const statements: string[] = [];
      for (let i = 0; i < 500; i++) {
        vars[`i${i}`] = `game:${i}`;
        vars[`d${i}`] = row(i, round);
        statements.push(surql.upsertMerge(`i${i}`, `d${i}`));
      }
      await engine.execute(surql.seal(surql.tx(statements)), vars);
    }
    const store = host.store()!;
    store.checkpoint();
    const before = store.stats();
    expect(maintenance(before)).toBe('compact');
    await store.compact();
    const after = store.stats();
    expect(maintenance(after)).toBeNull();
    expect(after.fileBytes).toBeLessThan(before.fileBytes / 4);
    expect(await engine.getById('game', 'game:499')).toEqual({ id: 'game:499', ...row(499, 5) });
    expect((await engine.getStorageDiagnostics()).freelistBytes).toBeLessThan(64 << 10);
  });

  it('lets a follower tab read and write, and nothing else', () => {
    for (const op of ['select', 'by-ids', 'get', 'relation', 'write', 'versions']) expect(ROWS_CLIENT_OPS.has(op)).toBe(true);
    for (const op of ['open', 'close', 'shutdown', 'relock', 'diag']) expect(ROWS_CLIENT_OPS.has(op)).toBe(false);
  });
});

describe('prewarming the row worker', () => {
  const g = globalThis as Record<string, unknown>;
  let stored: Map<string, string>;
  let disks: RowsDisks;
  /** Workers started through `new Worker(url)`, i.e. by the prewarm. */
  let started: { url: string; worker: FakeRowsWorker }[];
  /** Workers the engine started itself. */
  let own: FakeRowsWorker[];
  let prewarmedProto: number | undefined;

  const makeEngine = (): RowsCacheEngine =>
    new RowsCacheEngine(
      {
        store: 'indexeddb',
        workers: {
          rows: () => {
            const worker = new FakeRowsWorker(disks);
            own.push(worker);
            return worker as unknown as Worker;
          },
        },
      } as any,
      logger,
      { useOpfs: true }
    );

  beforeEach(() => {
    stored = new Map();
    disks = new Map();
    started = [];
    own = [];
    prewarmedProto = undefined;
    g.localStorage = {
      getItem: (k: string) => stored.get(k) ?? null,
      setItem: (k: string, v: string) => void stored.set(k, v),
      removeItem: (k: string) => void stored.delete(k),
    };
    g.Worker = class {
      constructor(url: string) {
        const worker = new FakeRowsWorker(disks, prewarmedProto);
        started.push({ url, worker });
        return worker as unknown as object;
      }
    };
  });

  afterEach(() => {
    delete g.localStorage;
    delete g.Worker;
    delete g.__sp00kyRowsPrewarm;
  });

  it('does nothing on a first visit, and learns the worker from the first open', async () => {
    prewarmRows();
    expect(started).toEqual([]);
    const first = makeEngine();
    await first.connect('user_a');
    expect(own.length).toBe(1);
    expect(JSON.parse(stored.get(ROWS_PREWARM_KEY)!)).toEqual({ url: 'https://app.test/assets/rows-worker.js', bucket: 'user_a' });
    await first.upsert('game', 'game:a', { n: 1 }, 'replace');
    await first.close();
  });

  it('hands the engine a worker that already has the store open', async () => {
    const first = makeEngine();
    await first.connect('user_a');
    await first.upsert('game', 'game:a', { n: 1 }, 'replace');
    await first.close();
    own.length = 0;

    prewarmRows();
    prewarmRows();
    expect(started.map((s) => s.url)).toEqual(['https://app.test/assets/rows-worker.js']);
    expect(started[0].worker.posted.map((m) => m.type)).toEqual(['open', 'warm']);
    const second = makeEngine();
    await second.connect('user_a');
    // The engine started no worker of its own, and nothing is left for a later engine to take.
    expect(own).toEqual([]);
    expect(g.__sp00kyRowsPrewarm).toBeUndefined();
    expect(await second.getById('game', 'game:a')).toEqual({ id: 'game:a', n: 1 });
    expect(second.storageHealth.status).toBe('persistent');
  });

  it('does not ask an adopted worker to open what it has already answered for', async () => {
    const first = makeEngine();
    await first.connect('user_a');
    await first.upsert('game', 'game:a', { n: 1 }, 'replace');
    await first.close();
    own.length = 0;

    prewarmRows();
    // The worker answers while the bundle loads, long before the client connects.
    await new Promise((resolve) => setTimeout(resolve, 0));
    const second = makeEngine();
    await second.connect('user_a');
    expect(own).toEqual([]);
    expect(started[0].worker.posted.map((m) => m.type)).toEqual(['open', 'warm']);
    expect(second.storageHealth.status).toBe('persistent');
    expect(await second.getById('game', 'game:a')).toEqual({ id: 'game:a', n: 1 });
    expect(started[0].worker.posted.map((m) => m.type)).toEqual(['open', 'warm', 'get']);
  });

  it('asks a worker prewarmed on another store to open the one it wants', async () => {
    const first = makeEngine();
    await first.connect('user_a');
    await first.upsert('game', 'game:a', { n: 1 }, 'replace');
    await first.close();

    prewarmRows();
    await new Promise((resolve) => setTimeout(resolve, 0));
    const second = makeEngine();
    await second.connect('user_b');
    expect(started[0].worker.posted.map((m) => m.type)).toEqual(['open', 'warm', 'open']);
    expect(await second.getById('game', 'game:a')).toBeNull();
  });

  describe('reading ahead', () => {
    const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
    const save = (e: RowsCacheEngine): void => (e as unknown as { saveStartupReads(): void }).saveStartupReads();
    const types = (): string[] => started[0].worker.posted.map((m) => m.type);

    /** A session that reads two games at startup, and what it leaves for the next page load. */
    const firstSession = async (): Promise<void> => {
      const first = makeEngine();
      await first.connect('user_a');
      await first.upsert('game', 'game:a', { n: 1 }, 'replace');
      await first.upsert('game', 'game:b', { n: 2 }, 'replace');
      expect(await first.selectByIds('game', ['game:a', 'game:b'])).toEqual([
        { id: 'game:a', n: 1 },
        { id: 'game:b', n: 2 },
      ]);
      save(first);
      await first.close();
      own.length = 0;
    };

    it('saves the reads of a session start and asks them again from the page head', async () => {
      await firstSession();
      expect(JSON.parse(stored.get(ROWS_PREWARM_KEY)!).reads).toEqual([['by-ids', { table: 'game', ids: ['game:a', 'game:b'] }]]);
      prewarmRows();
      expect(started[0].worker.posted).toEqual([
        { type: 'open', id: -1 },
        { type: 'by-ids', id: -10 },
        { type: 'warm', id: -2 },
      ]);
    });

    it('answers the first read from what the worker already sent, without a trip', async () => {
      await firstSession();
      prewarmRows();
      await tick();
      const second = makeEngine();
      await second.connect('user_a');
      expect(await second.selectByIds('game', ['game:a', 'game:b'])).toEqual([
        { id: 'game:a', n: 1 },
        { id: 'game:b', n: 2 },
      ]);
      expect(types()).toEqual(['open', 'by-ids', 'warm']);
      // An answer is used once: the same read again is asked of the worker.
      expect((await second.selectByIds('game', ['game:a', 'game:b'])).length).toBe(2);
      expect(types()).toEqual(['open', 'by-ids', 'warm', 'by-ids']);
      // And another read never matches.
      expect(await second.selectByIds('game', ['game:a'])).toEqual([{ id: 'game:a', n: 1 }]);
      expect(types()).toEqual(['open', 'by-ids', 'warm', 'by-ids', 'by-ids']);
    });

    it('waits for an answer that is still on its way, and does not ask twice', async () => {
      await firstSession();
      prewarmRows();
      const second = makeEngine();
      await second.connect('user_a');
      expect((await second.selectByIds('game', ['game:a', 'game:b'])).map((r) => r.n)).toEqual([1, 2]);
      // The open had not been answered yet, so that one was asked; the read was not.
      expect(types()).toEqual(['open', 'by-ids', 'warm', 'open']);
    });

    it('forgets what was read ahead the moment the session writes', async () => {
      await firstSession();
      prewarmRows();
      await tick();
      const second = makeEngine();
      await second.connect('user_a');
      await second.upsert('game', 'game:a', { n: 9 }, 'replace');
      expect((await second.selectByIds('game', ['game:a', 'game:b'])).map((r) => r.n)).toEqual([9, 2]);
      expect(types()).toEqual(['open', 'by-ids', 'warm', 'write', 'by-ids']);
    });

    it('asks nothing ahead for another bucket, and keeps no reads across a change of bucket', async () => {
      await firstSession();
      prewarmRows();
      await tick();
      const second = makeEngine();
      await second.connect('user_b');
      expect(await second.selectByIds('game', ['game:a', 'game:b'])).toEqual([]);
      expect(types()).toEqual(['open', 'by-ids', 'warm', 'open', 'by-ids']);
      expect(JSON.parse(stored.get(ROWS_PREWARM_KEY)!)).toEqual({ url: 'https://app.test/assets/rows-worker.js', bucket: 'user_b' });
    });

    it('does not save a read whose payload JSON would change', async () => {
      const first = makeEngine();
      await first.connect('user_a');
      await first.upsert('game', 'game:a', { n: 1 }, 'replace');
      const byParam: QueryPlan = { table: 'game', where: [{ field: 'n', op: '=', value: undefined, paramRef: 'n' }] };
      await first.select(byParam, { n: 1 });
      await first.select(byParam, { n: new Date(0) });
      save(first);
      const reads = JSON.parse(stored.get(ROWS_PREWARM_KEY)!).reads as [string, { params: unknown }][];
      expect(reads.map((r) => r[1].params)).toEqual([{ n: 1 }]);
      await first.close();
    });
  });

  describe('with shared tabs', () => {
    const tick = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
    const save = (e: RowsCacheEngine): void => (e as unknown as { saveStartupReads(): void }).saveStartupReads();
    const types = (): string[] => started[0].worker.posted.map((m) => m.type);
    const lead = { workerLockName: 'lock-1', allowMemoryFallback: false, resumeHeld: false };
    const sharedEngine = (): RowsCacheEngine =>
      new RowsCacheEngine(
        {
          store: 'indexeddb',
          workers: {
            rows: () => {
              const worker = new FakeRowsWorker(disks);
              own.push(worker);
              return worker as unknown as Worker;
            },
          },
        } as any,
        logger,
        { useOpfs: true, shared: true }
      );

    /** A leader's session that reads two games at startup, and what it leaves for the next page load. */
    const firstSession = async (): Promise<void> => {
      const first = sharedEngine();
      await first.adoptOwner('user_a', lead);
      await first.upsert('game', 'game:a', { n: 1 }, 'replace');
      await first.upsert('game', 'game:b', { n: 2 }, 'replace');
      expect((await first.selectByIds('game', ['game:a', 'game:b'])).length).toBe(2);
      save(first);
      await first.close();
      own.length = 0;
    };

    it('saves a leader\'s startup reads, and answers the next leader\'s first read from the page head', async () => {
      await firstSession();
      expect(JSON.parse(stored.get(ROWS_PREWARM_KEY)!).reads).toEqual([['by-ids', { table: 'game', ids: ['game:a', 'game:b'] }]]);
      prewarmRows();
      await tick();
      const second = sharedEngine();
      await second.adoptOwner('user_a', lead);
      // Its own worker was not needed. The open is asked: it is what takes the role's lock.
      expect(own).toEqual([]);
      expect(types()).toEqual(['open', 'by-ids', 'warm', 'open']);
      expect((await second.selectByIds('game', ['game:a', 'game:b'])).map((r) => r.n)).toEqual([1, 2]);
      expect(types()).toEqual(['open', 'by-ids', 'warm', 'open']);
    });

    it('forgets what was read ahead once another tab can write through the worker', async () => {
      await firstSession();
      prewarmRows();
      await tick();
      const second = sharedEngine();
      await second.adoptOwner('user_a', lead);
      const { port1 } = new MessageChannel();
      // The fake worker knows no ports; what matters is what the engine did before asking.
      await second.exposeClientPort('tab-2', port1).catch(() => {});
      port1.close();
      expect((await second.selectByIds('game', ['game:a', 'game:b'])).map((r) => r.n)).toEqual([1, 2]);
      expect(types().filter((t) => t === 'by-ids').length).toBe(2);
    });

    it('saves the startup reads of a session once, whatever roles follow', async () => {
      await firstSession();
      const tab = sharedEngine();
      await tab.adoptOwner('user_a', lead);
      await tab.selectByIds('game', ['game:a']);
      save(tab);
      expect(JSON.parse(stored.get(ROWS_PREWARM_KEY)!).reads).toEqual([['by-ids', { table: 'game', ids: ['game:a'] }]]);
      // Leadership moves on and comes back, long after startup: what the tab reads then is not a startup read.
      (tab as unknown as { startupUntil: number }).startupUntil = 0;
      await tab.adoptOwner('user_a', { ...lead, workerLockName: 'lock-2' });
      await tab.selectByIds('game', ['game:b']);
      save(tab);
      expect(JSON.parse(stored.get(ROWS_PREWARM_KEY)!).reads).toEqual([['by-ids', { table: 'game', ids: ['game:a'] }]]);
      await tab.close();
    });

    it('asks for the open even when the worker answered it, in a tab that may share the store', async () => {
      await firstSession();
      prewarmRows();
      await tick();
      // Shared tabs were not to be had and the tab opens the store alone.
      const solo = sharedEngine();
      await solo.connect('user_a');
      expect(types()).toEqual(['open', 'by-ids', 'warm', 'open']);
      expect(await solo.getById('game', 'game:a')).toEqual({ id: 'game:a', n: 1 });
    });

    const gaveAway = { id: -1, ok: false, error: 'rows worker: gave the store to the tab that leads' };

    it('takes back its open and everything read ahead when the worker gave the store away', async () => {
      for (const start of [prewarmRows, () => void new Function(ROWS_PREWARM_SNIPPET)()]) {
        disks = new Map();
        stored = new Map();
        started.length = 0;
        delete g.__sp00kyRowsPrewarm;
        await firstSession();
        start();
        await tick();
        const pre = g.__sp00kyRowsPrewarm as { opened?: unknown; reads: unknown[]; replies: Record<number, unknown> };
        expect(pre.opened).toBeDefined();
        expect(Object.keys(pre.replies).length).toBe(1);
        // Another tab leads: this page's worker closes its store and says so.
        started[0].worker.postMessage({ id: -5, type: 'close', payload: undefined });
        await tick();
        started[0].worker.onmessage?.({ data: gaveAway });
        expect(pre.opened).toBeUndefined();
        expect(pre.reads).toEqual([]);
        expect(pre.replies).toEqual({});
        // That tab writes, and is gone before this page has a role. (It has a worker of its own.)
        delete g.__sp00kyRowsPrewarm;
        const other = sharedEngine();
        await other.adoptOwner('user_a', { ...lead, workerLockName: 'lock-other' });
        await other.upsert('game', 'game:a', { n: 100 }, 'merge');
        await other.close();
        own.length = 0;
        g.__sp00kyRowsPrewarm = pre;

        const second = sharedEngine();
        await second.adoptOwner('user_a', lead);
        // The read is asked of the worker, and answers what the other tab wrote.
        expect((await second.selectByIds('game', ['game:a', 'game:b'])).map((r) => r.n)).toEqual([100, 2]);
        // On the worker its head script started: one read asked ahead, one asked now.
        expect(own).toEqual([]);
        expect(types().filter((t) => t === 'by-ids').length).toBe(2);
        await second.close();
      }
    });

    it('drops what was read ahead when the store is given away after the worker was adopted', async () => {
      await firstSession();
      prewarmRows();
      await tick();
      const second = sharedEngine();
      await second.adoptOwner('user_a', lead);
      // The hand-over was under way when the page adopted the worker: its word arrives now.
      started[0].worker.onmessage?.({ data: gaveAway });
      expect((await second.selectByIds('game', ['game:a', 'game:b'])).map((r) => r.n)).toEqual([1, 2]);
      expect(types().filter((t) => t === 'by-ids').length).toBe(2);
    });

    it('starts its own worker when the one started ahead is another release', async () => {
      stored.set(ROWS_PREWARM_KEY, JSON.stringify({ url: 'https://app.test/assets/rows-worker-OLD.js', bucket: 'user_a' }));
      prewarmedProto = 0;
      prewarmRows();
      const engine = sharedEngine();
      await engine.adoptOwner('user_a', lead);
      expect(started[0].worker.terminated).toBe(true);
      expect(own.length).toBe(1);
      await engine.upsert('game', 'game:a', { n: 2 }, 'replace');
      expect(await engine.getById('game', 'game:a')).toEqual({ id: 'game:a', n: 2 });
      expect(JSON.parse(stored.get(ROWS_PREWARM_KEY)!).url).toBe('https://app.test/assets/rows-worker.js');
    });
  });

  it('starts its own worker when the prewarmed one is another release', async () => {
    stored.set(ROWS_PREWARM_KEY, JSON.stringify({ url: 'https://app.test/assets/rows-worker-OLD.js', bucket: 'user_a' }));
    prewarmedProto = 0;
    prewarmRows();
    const engine = makeEngine();
    await engine.connect('user_a');
    expect(started[0].worker.terminated).toBe(true);
    expect(own.length).toBe(1);
    await engine.upsert('game', 'game:a', { n: 2 }, 'replace');
    expect(await engine.getById('game', 'game:a')).toEqual({ id: 'game:a', n: 2 });
    // The next load prewarms the worker that works.
    expect(JSON.parse(stored.get(ROWS_PREWARM_KEY)!).url).toBe('https://app.test/assets/rows-worker.js');
  });

  it('drops a prewarmed worker that failed to start', async () => {
    stored.set(ROWS_PREWARM_KEY, JSON.stringify({ url: 'https://app.test/gone.js', bucket: 'user_a' }));
    prewarmRows();
    started[0].worker.onerror?.({ message: '404' });
    const engine = makeEngine();
    await engine.connect('user_a');
    expect(started[0].worker.terminated).toBe(true);
    expect(own.length).toBe(1);
  });

  it('ships the same thing as a script for the page head', () => {
    stored.set(ROWS_PREWARM_KEY, JSON.stringify({ url: 'https://app.test/assets/rows-worker.js', bucket: 'user_b' }));
    new Function(ROWS_PREWARM_SNIPPET)();
    expect(started.map((s) => s.url)).toEqual(['https://app.test/assets/rows-worker.js']);
    expect((g.__sp00kyRowsPrewarm as { bucket: string }).bucket).toBe('user_b');
    expect(started[0].worker.posted).toEqual([
      { type: 'open', id: -1 },
      { type: 'warm', id: -2 },
    ]);
  });

  it('keeps the answer to its open in the head script too', async () => {
    stored.set(ROWS_PREWARM_KEY, JSON.stringify({ url: 'https://app.test/assets/rows-worker.js', bucket: 'user_b' }));
    new Function(ROWS_PREWARM_SNIPPET)();
    await new Promise((resolve) => setTimeout(resolve, 0));
    const engine = makeEngine();
    await engine.connect('user_b');
    expect(started[0].worker.posted.map((m) => m.type)).toEqual(['open', 'warm']);
    expect(engine.storageHealth.status).toBe('persistent');
  });
});

describe('painting from the last session', () => {
  const g = globalThis as Record<string, unknown>;
  let stored: Map<string, string>;
  let disks: RowsDisks;
  const flush = (e: RowsCacheEngine): void => (e as unknown as { paint: { flush(): void } }).paint.flush();
  const fresh = (config: Record<string, unknown> = {}, useOpfs = true): RowsCacheEngine => {
    const next = new RowsCacheEngine({ store: 'indexeddb', ...config } as any, logger, { useOpfs });
    inProcessRows(next, disks);
    return next;
  };
  const ids = ['game:b', 'game:a'];

  beforeEach(async () => {
    stored = new Map();
    disks = new Map();
    g.localStorage = {
      getItem: (k: string) => stored.get(k) ?? null,
      setItem: (k: string, v: string) => void stored.set(k, v),
      removeItem: (k: string) => void stored.delete(k),
      key: (i: number) => [...stored.keys()][i] ?? null,
      get length() {
        return stored.size;
      },
    };
    engine = fresh();
    await engine.connect('user_a');
    await engine.upsert('game', 'game:a', { n: 1, white: 'user:1' }, 'replace');
    await engine.upsert('game', 'game:b', { n: 2, blob: new Uint8Array([1, 2]) }, 'replace');
    await engine.upsert('user', 'user:1', { name: 'Ada' }, 'replace');
  });

  afterEach(() => {
    delete g.localStorage;
  });

  it('hands back a startup read before the store is open', async () => {
    expect(engine.peekByIds('game', ids)).toBeNull();
    const rows = await engine.selectByIds('game', ids);
    flush(engine);
    await engine.close();

    const next = fresh();
    const opening = next.connect('user_a');
    // Nothing has been awaited: the worker has not answered, the peek has.
    expect(next.peekByIds('game', ids)).toEqual(rows);
    expect(next.peekByIds('game', ids)![0].blob).toEqual(new Uint8Array([1, 2]));
    expect(next.peekByIds('game', ['game:a'])).toBeNull();
    expect(next.peekByIds('game', ids, { select: ['n'] })).toBeNull();
    await opening;
    expect(await next.selectByIds('game', ids)).toEqual(rows);
  });

  it('keeps a named read under its name, whatever its arguments were', async () => {
    const whole: QueryPlan = { table: 'game' };
    const one: QueryPlan = { table: 'game', ids: [new RecordId('game', 'a')] };
    // A query whose id set moved between two renders: one name, the latest answer.
    await engine.select(whole, {}, { paintKey: 'view-1' });
    const latest = await engine.select(one, {}, { paintKey: 'view-1' });
    expect(latest.map((r) => r.id)).toEqual(['game:a']);
    flush(engine);
    await engine.close();

    const next = fresh();
    void next.connect('user_a');
    expect(next.peekSelect(whole, {}, { paintKey: 'view-1' })).toEqual(latest);
    expect(next.peekSelect(whole, {}, { paintKey: 'view-2' })).toBeNull();
    // The unnamed read of the same plan is another thing.
    expect(next.peekSelect(whole)).toBeNull();
  });

  it('answers before connect once it is told which bucket is about to open', async () => {
    const plan: QueryPlan = { table: 'game' };
    const rows = await engine.select(plan, {}, { paintKey: 'view-1' });
    flush(engine);
    await engine.close();

    const next = fresh();
    // Nothing says whose rows these would be.
    expect(next.peekSelect(plan, {}, { paintKey: 'view-1' })).toBeNull();
    next.expectBucket('user_a');
    expect(next.peekSelect(plan, {}, { paintKey: 'view-1' })).toEqual(rows);
    // The first word stands until the store is opened.
    next.expectBucket('user_b');
    expect(next.peekSelect(plan, {}, { paintKey: 'view-1' })).toEqual(rows);
    // Opened on another bucket after all: none of the first one's rows are handed out.
    await next.connect('user_b');
    expect(next.peekSelect(plan, {}, { paintKey: 'view-1' })).toBeNull();
    await next.close();

    // A tab that shares the store with others is told the same way, before it has a role.
    const shared = new RowsCacheEngine({ store: 'indexeddb' } as any, logger, { useOpfs: true, shared: true });
    shared.expectBucket('user_a');
    expect(shared.peekSelect(plan, {}, { paintKey: 'view-1' })).toEqual(rows);
  });

  it('lets go of a copy the moment its table is written, and takes the next read of the same thing', async () => {
    await engine.selectByIds('game', ids);
    flush(engine);
    expect([...stored.keys()].some((k) => k.startsWith('sp00ky.paint.user_a.'))).toBe(true);
    await engine.upsert('game', 'game:a', { n: 10 }, 'merge');
    expect(engine.peekByIds('game', ids)).toBeNull();
    // Nothing is left but the marks of the tables written to.
    expect([...stored.keys()].filter((k) => k.startsWith('sp00ky.paint.user_a.') && !k.startsWith('sp00ky.paint.user_a.t.'))).toEqual([]);
    // A write to another table leaves it alone.
    const again = await engine.selectByIds('game', ids);
    await engine.upsert('user', 'user:1', { name: 'Ada L.' }, 'merge');
    flush(engine);
    expect(engine.peekByIds('game', ids)).toEqual(again);
    expect(again[1].n).toBe(10);
  });

  it('never keeps a read that a write overtook', async () => {
    const reading = engine.selectByIds('game', ids);
    const writing = engine.upsert('game', 'game:a', { n: 99 }, 'merge');
    await Promise.all([reading, writing]);
    flush(engine);
    expect(engine.peekByIds('game', ids)).toBeNull();
  });

  it('keeps a select with relations, and drops it when any table it read is written', async () => {
    const plan: QueryPlan = {
      table: 'game',
      ids: ['game:a'],
      relations: [{ alias: 'white', table: 'user', cardinality: 'one', foreignKeyField: 'white' }],
    };
    const rows = await engine.select(plan);
    flush(engine);
    expect(engine.peekSelect(plan)).toEqual(rows);
    expect((engine.peekSelect(plan)![0].white as Row).name).toBe('Ada');
    await engine.upsert('user', 'user:1', { name: 'Grace' }, 'merge');
    expect(engine.peekSelect(plan)).toBeNull();
  });

  it('keeps each bucket apart and forgets a dropped one', async () => {
    await engine.selectByIds('game', ids);
    await engine.switchBucket('user_b');
    expect(engine.peekByIds('game', ids)).toBeNull();
    await engine.dropBucket('user_a');
    expect([...stored.keys()].filter((k) => k.startsWith('sp00ky.paint.user_a'))).toEqual([]);
  });

  it('is off for a memory store and when configured off', async () => {
    for (const quiet of [fresh({}, false), fresh({ paintCache: false })]) {
      await quiet.connect('user_a');
      await quiet.selectByIds('game', ids);
      expect(quiet.peekByIds('game', ids)).toBeNull();
    }
  });
});

describe('painting with shared tabs', () => {
  const g = globalThis as Record<string, unknown>;
  let stored: Map<string, string>;
  let disks: RowsDisks;
  let leader: RowsCacheEngine;
  let leaderHost: RowsHost;
  const ids = ['game:b', 'game:a'];
  const flush = (e: RowsCacheEngine): void => (e as unknown as { paint: { flush(): void } }).paint.flush();
  const shared = (config: Record<string, unknown> = {}): RowsCacheEngine =>
    new RowsCacheEngine({ store: 'indexeddb', ...config } as any, logger, { useOpfs: true, shared: true });
  const attached = { bucketId: 'user_a', storageHealth: { status: 'persistent', fallback: false } as const };

  /** A follower tab: its ops go into the leader's worker through a port. */
  const follow = async (health: { status: 'persistent' | 'memory'; fallback: boolean } = attached.storageHealth): Promise<RowsCacheEngine> => {
    const tab = shared();
    const port = {
      onmessage: null as null | ((ev: { data: unknown }) => void),
      onmessageerror: null,
      start() {},
      close() {},
      postMessage(msg: { id: number; type: string; payload: unknown }, transfer?: Transferable[]) {
        const payload = structuredClone(msg.payload, { transfer: transfer ?? [] });
        void leaderHost.answer(msg.type, payload).then(
          ({ result, transfer: back }) =>
            port.onmessage?.({ data: { id: msg.id, ok: true, wt: 0, ...(structuredClone(result, { transfer: back ?? [] }) as object) } }),
          (error: unknown) => port.onmessage?.({ data: { id: msg.id, ok: false, error: String(error) } })
        );
      },
    };
    await tab.adoptAttached(port as unknown as MessagePort, { ...attached, storageHealth: health }, () => {});
    return tab;
  };

  beforeEach(async () => {
    stored = new Map();
    disks = new Map();
    g.localStorage = {
      getItem: (k: string) => stored.get(k) ?? null,
      setItem: (k: string, v: string) => void stored.set(k, v),
      removeItem: (k: string) => void stored.delete(k),
      key: (i: number) => [...stored.keys()][i] ?? null,
      get length() {
        return stored.size;
      },
    };
    leader = shared();
    leaderHost = inProcessRows(leader, disks);
    await leader.adoptOwner('user_a', { workerLockName: 'lock-1', allowMemoryFallback: false, resumeHeld: false });
    await leader.upsert('game', 'game:a', { n: 1 }, 'replace');
    await leader.upsert('game', 'game:b', { n: 2 }, 'replace');
    await leader.upsert('user', 'user:1', { name: 'Ada' }, 'replace');
  });

  afterEach(() => {
    delete g.localStorage;
  });

  it('keeps what a leader and a follower read at startup, for whichever tab opens next', async () => {
    const tab = await follow();
    const games = await leader.selectByIds('game', ids);
    const users = await tab.selectByIds('user', ['user:1']);
    flush(leader);
    flush(tab);

    // The next page load, before it has any role.
    const next = shared();
    next.expectBucket('user_a');
    expect(next.peekByIds('game', ids)).toEqual(games);
    expect(next.peekByIds('user', ['user:1'])).toEqual(users);
    // And each tab has the other's copy.
    expect(tab.peekByIds('game', ids)).toEqual(games);
    expect(leader.peekByIds('user', ['user:1'])).toEqual(users);
  });

  it('hands out no copy of a table another tab has written to since', async () => {
    const tab = await follow();
    await tab.selectByIds('game', ids);
    await tab.selectByIds('user', ['user:1']);
    flush(tab);
    expect(tab.peekByIds('game', ids)).not.toBeNull();

    await leader.upsert('game', 'game:a', { n: 10 }, 'merge');
    // Not in the tab that holds it in memory, not in one that would read it from storage.
    expect(tab.peekByIds('game', ids)).toBeNull();
    const next = shared();
    next.expectBucket('user_a');
    expect(next.peekByIds('game', ids)).toBeNull();
    // The other table's copy stands.
    expect(next.peekByIds('user', ['user:1'])).not.toBeNull();

    // The follower's own write voids the leader's copy the same way.
    await leader.selectByIds('user', ['user:1']);
    await tab.upsert('user', 'user:1', { name: 'Grace' }, 'merge');
    expect(leader.peekByIds('user', ['user:1'])).toBeNull();
    // The next read of the same thing is kept again.
    const again = await tab.selectByIds('game', ids);
    expect(again[1].n).toBe(10);
    flush(tab);
    expect(next.peekByIds('game', ids)).toEqual(again);
  });

  it('does not keep a read that another tab\'s write overtook', async () => {
    const tab = await follow();
    const kept = (e: RowsCacheEngine): number => (e as unknown as { paint: { entries: Map<string, unknown> } }).paint.entries.size;
    const reading = tab.selectByIds('game', ids);
    const writing = leader.upsert('game', 'game:a', { n: 99 }, 'merge');
    await Promise.all([reading, writing]);
    // Not kept at all: its marks alone would not say so had the read come back after the leader's second mark.
    expect(kept(tab)).toBe(0);
    flush(tab);
    expect(tab.peekByIds('game', ids)).toBeNull();
    expect([...stored.keys()].filter((k) => k.startsWith('sp00ky.paint.user_a.') && !k.includes('.t.'))).toEqual([]);
  });

  it('does not write a copy that went stale over one another tab took since', async () => {
    const tab = await follow();
    await tab.selectByIds('game', ids);
    await leader.upsert('game', 'game:a', { n: 5 }, 'merge');
    const fresh = await leader.selectByIds('game', ids);
    flush(leader);
    // The follower's copy is the older one, and still unsaved.
    flush(tab);
    const next = shared();
    next.expectBucket('user_a');
    expect(next.peekByIds('game', ids)).toEqual(fresh);
  });

  it('starts a session with the first role only', async () => {
    const tab = await follow();
    await tab.selectByIds('game', ids);
    const paint = (tab as unknown as { paint: { since: number } }).paint;
    const since = paint.since;
    expect(since).toBeGreaterThan(0);
    paint.since = since - 10_000;
    // Promoted later: reads from here on are not what the tab reads at startup.
    inProcessRows(tab, disks);
    await leader.close();
    await tab.adoptOwner('user_a', { workerLockName: 'lock-2', allowMemoryFallback: false, resumeHeld: false });
    expect(paint.since).toBe(since - 10_000);
    await tab.selectByIds('user', ['user:1']);
    expect(tab.peekByIds('user', ['user:1'])).toBeNull();
    // What it kept at startup it keeps renewing.
    await tab.upsert('game', 'game:a', { n: 7 }, 'merge');
    const again = await tab.selectByIds('game', ids);
    expect(tab.peekByIds('game', ids)).toEqual(again);
  });

  it('keeps nothing while the shared store lives in memory', async () => {
    const tab = await follow({ status: 'memory', fallback: true });
    await tab.selectByIds('game', ids);
    flush(tab);
    expect(tab.peekByIds('game', ids)).toBeNull();
    expect([...stored.keys()].filter((k) => k.startsWith('sp00ky.paint.user_a.') && !k.includes('.t.'))).toEqual([]);
    // Nor does a leader that was granted a store in memory.
    const lost = shared();
    inProcessRows(lost, new Map());
    // The worker could not have the files and was allowed to open in memory.
    const seam = lost as unknown as { createTransport: () => { call: (type: string, ...rest: unknown[]) => Promise<object> } };
    const make = seam.createTransport;
    seam.createTransport = () => {
      const transport = make();
      const call = transport.call;
      transport.call = async (type, ...rest) => {
        const reply = await call(type, ...rest);
        return type === 'open' ? { ...reply, persisted: false, opfsError: 'held by another tab' } : reply;
      };
      return transport;
    };
    await lost.adoptOwner('user_m', { workerLockName: 'lock-m', allowMemoryFallback: true, resumeHeld: false });
    expect(lost.storageHealth.status).toBe('memory');
    await lost.upsert('game', 'game:m', { n: 1 }, 'replace');
    await lost.selectByIds('game', ['game:m']);
    flush(lost);
    expect(lost.peekByIds('game', ['game:m'])).toBeNull();
    expect([...stored.keys()].filter((k) => k.startsWith('sp00ky.paint.user_m'))).toEqual([]);
  });
});

describe('PaintCache marks', () => {
  const g = globalThis as Record<string, unknown>;
  let stored: Map<string, string>;
  const rows = JSON.stringify([{ id: 'game:a', n: 1 }]);
  const none = (b64: string): Uint8Array => new Uint8Array(b64.length);
  const open = (max = 1_000_000): PaintCache => {
    const cache = new PaintCache(max);
    cache.start('user_a');
    return cache;
  };

  beforeEach(() => {
    stored = new Map();
    g.localStorage = {
      getItem: (k: string) => stored.get(k) ?? null,
      setItem: (k: string, v: string) => void stored.set(k, v),
      removeItem: (k: string) => void stored.delete(k),
      key: (i: number) => [...stored.keys()][i] ?? null,
      get length() {
        return stored.size;
      },
    };
  });

  afterEach(() => {
    delete g.localStorage;
  });

  it('keeps a tab\'s own read that was queued behind its write', () => {
    const a = open();
    a.touch(['game']);
    const sent = a.stamp(['game']);
    // The store answers the write, then the read.
    a.touch(['game'], true);
    a.note('k', ['game'], rows, false, sent);
    expect(a.get('k', none)).toEqual(JSON.parse(rows));
    // A read sent before the write is another matter.
    const early = a.stamp(['game']);
    a.touch(['game']);
    a.touch(['game'], true);
    a.note('k2', ['game'], rows, false, early);
    expect(a.get('k2', none)).toBeNull();
  });

  it('voids the copy a tab took between another tab\'s write being sent and being taken', () => {
    const a = open();
    const b = open();
    a.touch(['game']);
    // B reads after A's first mark, and the store answers it before A's write.
    const sent = b.stamp(['game']);
    b.note('k', ['game'], rows, false, sent);
    expect(b.get('k', none)).not.toBeNull();
    a.touch(['game'], true);
    expect(b.get('k', none)).toBeNull();
    b.flush();
    expect(stored.has('sp00ky.paint.user_a.k')).toBe(false);
  });

  it('counts another tab\'s write against a read on its way, even under a mark of its own', () => {
    const a = open();
    const b = open();
    const sent = b.stamp(['game']);
    a.touch(['game']);
    a.touch(['game'], true);
    // B writes too before its read answers: A's mark is covered, not forgotten.
    b.touch(['user']);
    b.note('k', ['game'], rows, false, sent);
    expect(b.get('k', none)).toBeNull();
    const again = b.stamp(['game']);
    b.note('k', ['game'], rows, false, again);
    expect(b.get('k', none)).not.toBeNull();
  });

  it('keeps one budget for all tabs and clears out what no longer stands', () => {
    const big = JSON.stringify([{ id: 'game:a', pad: 'p'.repeat(400) }]);
    const a = open(1000);
    const b = open(1000);
    a.note('a1', ['game'], big, false, a.stamp(['game']));
    a.flush();
    b.note('b1', ['user'], big, false, b.stamp(['user']));
    b.note('b2', ['user'], big, false, b.stamp(['user']));
    b.flush();
    // B's two fill the budget: A's is the one that goes.
    expect(stored.has('sp00ky.paint.user_a.a1')).toBe(false);
    expect(Object.keys(JSON.parse(stored.get('sp00ky.paint.user_a')!)).sort()).toEqual(['b1', 'b2']);

    // A copy from another release, and one whose table was written by a tab that is gone.
    stored.set('sp00ky.paint.user_a.old', '0[{"id":"game:z"}]');
    stored.set('sp00ky.paint.user_a', JSON.stringify({ ...JSON.parse(stored.get('sp00ky.paint.user_a')!), old: ['game'] }));
    const c = open(1000);
    expect(c.get('old', none)).toBeNull();
    stored.set('sp00ky.paint.user_a.t.user', 'gone.1');
    expect(c.get('b1', none)).toBeNull();
    c.flush();
    expect([...stored.keys()].filter((k) => !k.includes('.t.')).sort()).toEqual(['sp00ky.paint.user_a']);
    expect(JSON.parse(stored.get('sp00ky.paint.user_a')!)).toEqual({});
  });

  it('tells the other tabs even when there is no room for a mark', () => {
    const a = open();
    const b = open();
    a.touch(['game']);
    a.touch(['game'], true);
    b.note('k', ['game'], rows, false, b.stamp(['game']));
    expect(b.get('k', none)).not.toBeNull();
    const setItem = (g.localStorage as { setItem: unknown }).setItem;
    (g.localStorage as { setItem: unknown }).setItem = () => {
      throw new Error('QuotaExceededError');
    };
    a.touch(['game']);
    (g.localStorage as { setItem: unknown }).setItem = setItem;
    expect(stored.has('sp00ky.paint.user_a.t.game')).toBe(false);
    expect(b.get('k', none)).toBeNull();
  });

  it('makes room for a mark by letting the copies go, on a table that never had one too', () => {
    const a = open();
    const b = open();
    b.note('k', ['game'], rows, false, b.stamp(['game']));
    b.note('u', ['user'], rows, false, b.stamp(['user']));
    b.flush();
    expect(stored.has('sp00ky.paint.user_a.t.game')).toBe(false);
    // Full: a new key does not fit, an existing one can still be removed.
    const setItem = (g.localStorage as { setItem: (k: string, v: string) => void }).setItem;
    (g.localStorage as { setItem: unknown }).setItem = (k: string, v: string) => {
      if ([...stored.keys()].some((name) => /^sp00ky\.paint\.user_a\.[^.]+$/.test(name))) throw new Error('QuotaExceededError');
      setItem(k, v);
    };
    a.touch(['game']);
    (g.localStorage as { setItem: unknown }).setItem = setItem;
    // The copies went, the mark is there, and no tab hands out what it held.
    expect(stored.has('sp00ky.paint.user_a.t.game')).toBe(true);
    expect([...stored.keys()].filter((name) => /^sp00ky\.paint\.user_a\.[^.]+$/.test(name))).toEqual([]);
    expect(b.get('k', none)).toBeNull();
  });

  it('saves the copies that fit when one does not, and lets go of copies the index lost', () => {
    const a = open();
    a.note('fits', ['game'], rows, false, a.stamp(['game']));
    a.note('too-big', ['user'], rows, false, a.stamp(['user']));
    const setItem = (g.localStorage as { setItem: (k: string, v: string) => void }).setItem;
    (g.localStorage as { setItem: unknown }).setItem = (k: string, v: string) => {
      if (k.endsWith('.too-big')) throw new Error('QuotaExceededError');
      setItem(k, v);
    };
    a.flush();
    (g.localStorage as { setItem: unknown }).setItem = setItem;
    expect(Object.keys(JSON.parse(stored.get('sp00ky.paint.user_a')!))).toEqual(['fits']);
    expect(a.get('fits', none)).not.toBeNull();

    // A copy two tabs saving at once left out of the index. Its table is written, then some tab saves.
    const stray = stored.get('sp00ky.paint.user_a.fits')!;
    stored.set('sp00ky.paint.user_a.stray', stray);
    const b = open();
    b.touch(['game']);
    b.touch(['game'], true);
    b.flush();
    expect(stored.has('sp00ky.paint.user_a.stray')).toBe(false);
    // One that still stands is taken into the index, and goes with the next write to its table.
    b.note('again', ['game'], rows, false, b.stamp(['game']));
    b.flush();
    stored.set('sp00ky.paint.user_a.stray2', stored.get('sp00ky.paint.user_a.again')!);
    const c = open();
    c.flush();
    expect(Object.keys(JSON.parse(stored.get('sp00ky.paint.user_a')!)).sort()).toEqual(['again', 'stray2']);
    c.touch(['game']);
    expect(stored.has('sp00ky.paint.user_a.stray2')).toBe(false);
  });

  it('hands out another tab\'s newer copy when its own no longer stands', () => {
    const a = open();
    const b = open();
    b.note('k', ['game'], rows, false, b.stamp(['game']));
    a.touch(['game']);
    a.touch(['game'], true);
    const newer = JSON.stringify([{ id: 'game:a', n: 2 }]);
    a.note('k', ['game'], newer, false, a.stamp(['game']));
    a.flush();
    expect(b.get('k', none)).toEqual(JSON.parse(newer));
  });

  it('leaves no marks, and touches storage for no write, in a tab that is alone in the store', () => {
    const solo = new PaintCache(1_000_000, false);
    solo.start('user_a');
    let calls = 0;
    const real = g.localStorage as Record<string, unknown>;
    g.localStorage = new Proxy(real, {
      get: (target, prop) => {
        const value = target[prop as string];
        if (typeof value !== 'function') return value;
        return (...args: unknown[]) => {
          calls++;
          return (value as (...a: unknown[]) => unknown)(...args);
        };
      },
    });
    for (let i = 0; i < 100; i++) {
      solo.touch(['game']);
      solo.touch(['game'], true);
    }
    // The index is read once; after that a write costs nothing while no copy of its table is kept.
    expect(calls).toBeLessThanOrEqual(1);
    expect([...stored.keys()].filter((k) => k.includes('.t.'))).toEqual([]);
    // Its own writes still drop its copies, and an overtaken read is still not kept.
    const early = solo.stamp(['game']);
    solo.note('k', ['game'], rows, false, solo.stamp(['game']));
    solo.flush();
    expect(solo.get('k', none)).not.toBeNull();
    solo.touch(['game']);
    expect(solo.get('k', none)).toBeNull();
    solo.note('k', ['game'], rows, false, early);
    expect(solo.get('k', none)).toBeNull();
    // A copy it cannot account for (not in the index) is not kept on trust.
    solo.note('k', ['game'], rows, false, solo.stamp(['game']));
    solo.flush();
    stored.set('sp00ky.paint.user_a.stray', stored.get('sp00ky.paint.user_a.k')!);
    solo.note('k', ['game'], rows, false, solo.stamp(['game']));
    solo.flush();
    expect(stored.has('sp00ky.paint.user_a.stray')).toBe(false);
    g.localStorage = real;
  });

  it('never keeps a read under the bucket that came after it', () => {
    const a = open();
    const sent = a.stamp(['game']);
    a.start('user_b');
    a.note('k', ['game'], rows, false, sent);
    expect(a.get('k', none)).toBeNull();
    a.stop();
    expect(a.stamp(['game'])).toBe('');
    a.touch(['game']);
    expect([...stored.keys()].filter((k) => k.includes('user_b'))).toEqual([]);
  });
});
