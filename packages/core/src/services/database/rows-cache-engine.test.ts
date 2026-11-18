import { describe, it, expect, beforeEach } from 'vitest';
import { RecordId } from 'surrealdb';
import type { QueryPlan } from '@spooky-sync/query-builder';
import { RowsCacheEngine, ROWS_DURABLE_TABLES, packWrites } from './rows-cache-engine';
import { createLocalEngine } from './engine-factory';
import { inProcessRows, type RowsDisks, type RowsHost } from './rows-transport.fixture';
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
