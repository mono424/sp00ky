import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import type { QueryPlan, WhereComparison, WhereNode } from '@spooky-sync/query-builder';
import { executeSelect, type SelectDb } from './sqlite-select';
import { fromBase64, renderOrderSql, renderWhereSql, reviveRow, serializeMergeRow, serializeRow } from './sqlite-plan-sql';
import type { OrderBy, Row } from './cache-engine';
import { MemoryFile, RowStore } from './rows-store';
import { RowsExec, parseRowBytes, visibleTables, type WireOp } from './rows-exec';

/**
 * The row store has to answer a query the way the SQLite engine does, since an
 * app may run on either. These tests load the same rows into both, a real
 * in-memory SQLite (the build the SQLite worker loads) and a row store, run
 * the same plan against each and compare the answers: hand-picked plans for
 * every feature, then a few hundred random filters over values of every type.
 */

let db: any;
let selectDb: SelectDb;
let store: RowStore;
let exec: RowsExec;

const enc = new TextEncoder();

/** What the engine sends for a batch of rows: one buffer, one op per row. */
function pack(table: string, rows: Row[], mode: 'put' | 'merge'): { ops: WireOp[]; buf: ArrayBuffer } {
  const parts: Uint8Array[] = [];
  const ops: WireOp[] = [];
  let at = 0;
  for (const row of rows) {
    const { insert, patch } = mode === 'merge' ? serializeMergeRow(row) : { insert: serializeRow(row), patch: '' };
    const body = enc.encode(insert);
    const flags = insert.includes('"__u8"') ? 1 : 0;
    const rv = Number(row._00_rv) || 0;
    parts.push(body);
    if (mode === 'put') ops.push(['put', table, String(row.id), rv, flags, at, body.length]);
    else if (patch === insert) ops.push(['merge', table, String(row.id), rv, flags, at, body.length, 0, 0]);
    else {
      const patchBytes = enc.encode(patch);
      parts.push(patchBytes);
      ops.push(['merge', table, String(row.id), rv, flags, at, body.length, at + body.length, patchBytes.length]);
      at += patchBytes.length;
    }
    at += body.length;
  }
  const buf = new Uint8Array(at);
  let p = 0;
  for (const part of parts) {
    buf.set(part, p);
    p += part.length;
  }
  return { ops, buf: buf.buffer };
}

function seed(table: string, rows: Row[]): void {
  db.exec({ sql: `CREATE TABLE IF NOT EXISTS "${table}" (id TEXT PRIMARY KEY, data TEXT NOT NULL)` });
  selectDb.knownTables.add(table);
  for (const row of rows) {
    db.exec({ sql: `INSERT INTO "${table}"(id, data) VALUES(?, ?)`, bind: [row.id, serializeRow(row)] });
  }
  const { ops, buf } = pack(table, rows, 'put');
  exec.write(ops, buf, false);
}

async function both(plan: QueryPlan, params: Record<string, unknown> = {}): Promise<{ sqlite: Row[]; rows: Row[] }> {
  const sqlite = (await executeSelect(plan, params, selectDb)).rows;
  const rows = parseRowBytes<Row[]>(await exec.select(plan, params), fromBase64);
  return { sqlite, rows };
}

async function same(plan: QueryPlan, params: Record<string, unknown> = {}): Promise<Row[]> {
  const { sqlite, rows } = await both(plan, params);
  expect(rows, JSON.stringify(plan)).toEqual(sqlite);
  return rows;
}

const GAMES: Row[] = [
  { id: 'game:a', title: 'Alpha', moves: 34, rating: 2140.5, rated: true, white: 'user:1', black: 'user:2', tags: ['blitz', 'rated'], meta: { opening: 'B90', depth: 3 }, _00_rv: 4 },
  { id: 'game:b', title: 'beta', moves: 12, rating: 1800, rated: false, white: 'user:2', black: 'user:1', tags: [], meta: { opening: 'C42' }, _00_rv: 1 },
  { id: 'game:c', title: 'Gamma', moves: 0, rating: null, rated: true, white: 'user:3', black: null, _00_rv: 9 },
  { id: 'game:d', title: '10', moves: 10, rating: -3.25, white: 'Magnus', black: 'user:1', meta: { opening: null, depth: 0 }, _00_rv: 2 },
  { id: 'game:e', title: 'Épée', moves: 77, rating: 1e3, rated: false, white: 'user:1', black: 'user:3', blob: new Uint8Array([0, 1, 2, 250, 255]), _00_rv: 0 },
  { id: 'game:f', title: '\u{1f600} grin', moves: 5, rating: 2140.5, rated: true, white: 'user:9', black: 'user:2', meta: { opening: 'A00', depth: 12 } },
  { id: 'game:g', title: '￿ high', moves: '34', rating: '2140.5', rated: 1, white: 'user:1', black: 'user:2', tags: ['x'], _00_rv: 3 },
  { id: 'game:h', title: '', moves: 34, rating: 0, rated: 0, white: 'user:2', black: 'user:3', meta: 'not an object', _00_rv: 6 },
  { id: 'game:10', title: 'zeta', moves: 1.5, rating: 99, rated: true, white: 'user:3', black: 'user:1', tags: ['blitz'] },
  { id: 'game:9', title: 'Zeta', moves: -1, rating: 100, rated: null, white: 'user:1', black: 'user:1', meta: { opening: 'B90', depth: 3 } },
];
const USERS: Row[] = [
  { id: 'user:1', name: 'Ada', elo: 2100, club: 'club:x' },
  { id: 'user:2', name: 'Bob', elo: 1900, club: 'club:y' },
  { id: 'user:3', name: 'Cyd', elo: 2300, club: null },
];
const CLUBS: Row[] = [
  { id: 'club:x', name: 'Xenon' },
  { id: 'club:y', name: 'Yttrium' },
];
const COMMENTS: Row[] = Array.from({ length: 40 }, (_, i) => ({
  id: `comment:${String(i).padStart(2, '0')}`,
  game: i % 7 === 6 ? null : `game:${'abcdefgh'[i % 8]}`,
  author: `user:${(i % 3) + 1}`,
  rank: (i * 37) % 11,
  hidden: i % 5 === 0,
  text: `comment ${i}`,
}));

beforeAll(async () => {
  const sqlite3: any = await sqlite3InitModule();
  db = new sqlite3.oo1.DB(':memory:', 'c');
  selectDb = {
    exec: (sql, bind) => db.exec({ sql, bind, rowMode: 'object', returnValue: 'resultRows' }) as { data: string }[],
    run: (sql, bind) => {
      db.exec({ sql, bind });
    },
    knownTables: new Set<string>(),
  };
  const files = { a: new MemoryFile(), b: new MemoryFile() };
  store = await RowStore.open({ open: (name) => files[name] });
  exec = new RowsExec(store);
  seed('game', GAMES);
  seed('user', USERS);
  seed('club', CLUBS);
  seed('comment', COMMENTS);
});

afterAll(() => {
  db?.close();
  store?.close();
});

describe('row store vs SQLite: select', () => {
  it('reads a whole table in id order', async () => {
    const rows = await same({ table: 'game' });
    expect(rows.map((r) => r.id)).toEqual(
      GAMES.map((g) => String(g.id)).sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)))
    );
    expect(rows.find((r) => r.id === 'game:e')?.blob).toEqual(new Uint8Array([0, 1, 2, 250, 255]));
  });

  it('reads a table that does not exist as empty', async () => {
    await same({ table: 'nothing_here' });
    await same({ table: 'nothing_here', where: [{ field: 'a', op: '=', value: 1 }] });
  });

  it('filters by every operator and value type', async () => {
    const values = [34, '34', 0, 1, true, false, null, 'user:1', 2140.5, '2140.5', '', 'Alpha', 'beta', -1, 1.5, 1000];
    const fields = ['moves', 'rating', 'rated', 'title', 'white', 'black', 'id', 'missing', 'meta.depth', 'meta.opening', 'tags', 'meta'];
    for (const field of fields) {
      for (const op of ['=', '!=', '<', '<=', '>', '>=']) {
        for (const value of values) {
          await same({ table: 'game', where: [{ field, op, value }] });
          await same({ table: 'game', where: [{ field, op, value, swap: true }] });
        }
      }
    }
  });

  it('matches a record link whether it is bound as a string or an object', async () => {
    const link = { tb: 'user', id: '1' };
    const rows = await same({ table: 'game', where: [{ field: 'white', op: '=', value: link }] });
    expect(rows.length).toBe(4);
    await same({ table: 'game', where: [{ field: 'id', op: '=', value: { tb: 'game', id: 'a' } }] });
  });

  it('combines AND with OR groups', async () => {
    await same({
      table: 'game',
      where: [
        { or: [{ field: 'white', op: '=', value: 'user:1' }, { field: 'black', op: '=', value: 'user:1' }] },
        { field: 'moves', op: '>', value: 5 },
      ],
    });
    await same({ table: 'game', where: [{ or: [{ field: 'rated', op: '=', value: true }, { field: 'rating', op: '<', value: 0 }] }] });
  });

  it('prefers the query param over the baked value', async () => {
    const where: WhereNode[] = [{ field: 'white', op: '=', value: 'user:2', paramRef: 'white' }];
    expect((await same({ table: 'game', where }, { white: 'user:1' })).length).toBe(4);
    expect((await same({ table: 'game', where }, {})).length).toBe(2);
    await same({ table: 'game', where: [{ field: 'moves', op: '>=', value: undefined, paramRef: 'min' }] }, { min: 30 });
    await same({ table: 'game', where: [{ field: 'moves', op: '>=', value: undefined, paramRef: 'min' }] }, {});
  });

  it('orders by any field, either way, with a window', async () => {
    for (const field of ['moves', 'rating', 'rated', 'title', 'white', 'missing', 'meta.depth', 'tags']) {
      for (const dir of ['asc', 'desc'] as const) {
        // The id breaks ties, which SQL leaves in no particular order.
        const orderBy: OrderBy = [
          [field, dir],
          ['id', 'asc'],
        ];
        await same({ table: 'game', orderBy });
        await same({ table: 'game', orderBy, limit: 3 });
        await same({ table: 'game', orderBy, limit: 4, offset: 3 });
        await same({ table: 'game', orderBy, where: [{ field: 'moves', op: '>', value: 0 }], limit: 2, offset: 1 });
      }
    }
  });

  it('windows an unordered query in id order', async () => {
    await same({ table: 'comment', limit: 7 });
    await same({ table: 'comment', limit: 7, offset: 30 });
    await same({ table: 'comment', limit: 100, offset: 39 });
    await same({ table: 'comment', limit: 0 });
    await same({ table: 'comment', where: [{ field: 'hidden', op: '=', value: true }], limit: 3, offset: 2 });
    await same({ table: 'comment', where: [{ field: 'rank', op: '>', value: 100 }], limit: 3 });
  });

  it('materializes a window of ids in the order given', async () => {
    const ids = ['game:h', 'game:a', 'game:missing', 'game:10', 'game:a', 'game:e'];
    const rows = await same({ table: 'game', ids });
    // A repeated id counts where it appears last, as in the SQLite engine.
    expect(rows.map((r) => r.id)).toEqual(['game:h', 'game:10', 'game:a', 'game:e']);
    await same({ table: 'game', ids: [] });
    await same({ table: 'game', ids: ['game:nope'] });
    await same({ table: 'game', ids, orderBy: [['moves', 'desc'], ['id', 'asc']] });
    await same({ table: 'game', ids: [{ tb: 'game', id: 'c' }, 'game:b'] });
    // `where`, `limit` and `offset` do not apply to a window.
    await same({ table: 'game', ids, where: [{ field: 'moves', op: '=', value: -5 }], limit: 1, offset: 1 });
  });

  it('projects fields, keeping booleans and absent keys as they are', async () => {
    const rows = await same({ table: 'game', select: ['title', 'rated', 'meta', 'blob', 'missing'] });
    expect(rows.find((r) => r.id === 'game:a')).toEqual({ id: 'game:a', title: 'Alpha', rated: true, meta: { opening: 'B90', depth: 3 } });
    expect(rows.find((r) => r.id === 'game:d')).toEqual({ id: 'game:d', title: '10', meta: { opening: null, depth: 0 } });
    await same({ table: 'game', ids: ['game:e', 'game:b'], select: ['blob', 'rated'] });
    await same({ table: 'game', select: ['moves'], orderBy: [['rating', 'desc'], ['id', 'asc']], limit: 4 });
  });

  it('resolves to-one and to-many relations, nested', async () => {
    const plan: QueryPlan = {
      table: 'game',
      orderBy: [['id', 'asc']],
      relations: [
        {
          alias: 'white',
          table: 'user',
          cardinality: 'one',
          foreignKeyField: 'white',
          relations: [{ alias: 'club', table: 'club', cardinality: 'one', foreignKeyField: 'club' }],
        },
        { alias: 'black', table: 'user', cardinality: 'one', foreignKeyField: 'black', select: ['name'] },
        {
          alias: 'comments',
          table: 'comment',
          cardinality: 'many',
          foreignKeyField: 'game',
          where: [{ field: 'hidden', op: '=', value: false }],
          orderBy: [['rank', 'desc'], ['id', 'asc']],
          limit: 3,
          relations: [{ alias: 'author', table: 'user', cardinality: 'one', foreignKeyField: 'author', select: ['name'] }],
        },
      ],
    };
    const rows = await same(plan);
    const a = rows.find((r) => r.id === 'game:a')!;
    expect((a.white as Row).name).toBe('Ada');
    expect(((a.white as Row).club as Row).name).toBe('Xenon');
    expect((a.comments as Row[]).length).toBe(3);
    // A value that is not a link stays what it was; a dangling link reads null.
    expect(rows.find((r) => r.id === 'game:d')!.white).toBe('Magnus');
    expect(rows.find((r) => r.id === 'game:f')!.white).toBeNull();
    await same({ ...plan, ids: ['game:c', 'game:a'], orderBy: undefined });
    await same({ ...plan, select: ['white', 'title'], limit: 4 });
  });

  it('returns the children of an unordered to-many relation as the same set', async () => {
    const plan: QueryPlan = {
      table: 'game',
      relations: [{ alias: 'comments', table: 'comment', cardinality: 'many', foreignKeyField: 'game' }],
    };
    const { sqlite, rows } = await both(plan);
    const byId = (list: Row[]): Row[] => [...list].sort((x, y) => String(x.id).localeCompare(String(y.id)));
    expect(rows.map((r) => ({ ...r, comments: byId(r.comments as Row[]) }))).toEqual(
      sqlite.map((r) => ({ ...r, comments: byId(r.comments as Row[]) }))
    );
  });

  it('keeps a relation right after the child table is written to', async () => {
    const plan: QueryPlan = {
      table: 'user',
      relations: [{ alias: 'comments', table: 'comment', cardinality: 'many', foreignKeyField: 'author', orderBy: [['id', 'asc']] }],
    };
    await same(plan);
    const moved: Row = { ...COMMENTS[0], author: 'user:3', text: 'moved' };
    const added: Row = { id: 'comment:zz', game: 'game:a', author: 'user:2', rank: 1, hidden: false, text: 'new' };
    for (const row of [moved, added]) {
      db.exec({ sql: `INSERT INTO "comment"(id, data) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET data = excluded.data`, bind: [row.id, serializeRow(row)] });
    }
    const { ops, buf } = pack('comment', [moved, added], 'put');
    exec.write(ops, buf, false);
    db.exec({ sql: `DELETE FROM "comment" WHERE id = ?`, bind: ['comment:01'] });
    exec.write([['del', 'comment', 'comment:01']], null, false);
    const rows = await same(plan);
    expect((rows[2].comments as Row[]).some((c) => c.text === 'moved')).toBe(true);
    expect((rows[1].comments as Row[]).some((c) => c.id === 'comment:01')).toBe(false);
  });

  it('agrees on a few hundred random filters', async () => {
    let seedValue = 20261117;
    const rnd = (n: number): number => {
      seedValue = (Math.imul(seedValue, 1103515245) + 12345) & 0x7fffffff;
      return seedValue % n;
    };
    const fields = ['moves', 'rating', 'rated', 'title', 'white', 'black', 'id', 'meta.depth', 'meta.opening', 'missing', '_00_rv'];
    const ops = ['=', '!=', '<', '<=', '>', '>='];
    const pool: unknown[] = [0, 1, 34, -1, 1.5, 2140.5, 1000, '34', '10', 'user:1', 'user:2', 'game:c', 'B90', '', 'Alpha', 'zeta', true, false, null, 3];
    const comparison = (): WhereComparison => ({
      field: fields[rnd(fields.length)],
      op: ops[rnd(ops.length)],
      value: pool[rnd(pool.length)],
      ...(rnd(6) === 0 ? { swap: true } : {}),
    });
    for (let i = 0; i < 400; i++) {
      const where: WhereNode[] = [];
      for (let k = rnd(3) + 1; k > 0; k--) {
        where.push(rnd(4) === 0 ? { or: [comparison(), comparison()] } : comparison());
      }
      const plan: QueryPlan = { table: 'game', where };
      if (rnd(3) === 0) plan.orderBy = [[fields[rnd(fields.length)], rnd(2) ? 'asc' : 'desc'], ['id', 'asc']];
      if (rnd(3) === 0) plan.limit = rnd(5);
      if (plan.limit !== undefined && rnd(2) === 0) plan.offset = rnd(4);
      await same(plan);
    }
  });
});

describe('row store vs SQLite: the raw-statement verbs', () => {
  const sqliteRows = (table: string, where?: WhereNode[], orderBy?: OrderBy, limit?: number, start?: number): Row[] => {
    const bind: unknown[] = [];
    let sql = `SELECT data FROM "${table}"`;
    if (where?.length) sql += ` WHERE ${renderWhereSql(where, bind, {})}`;
    sql += orderBy?.length ? renderOrderSql(orderBy) : ' ORDER BY id';
    if (limit !== undefined) sql += ` LIMIT ${limit}`;
    else if (start !== undefined) sql += ' LIMIT -1';
    if (start !== undefined) sql += ` OFFSET ${start}`;
    return selectDb.exec(sql, bind).map((r) => reviveRow(r.data));
  };

  it('selects a table with a filter, an order and a window', () => {
    const cases: [WhereNode[] | undefined, OrderBy | undefined, number | undefined, number | undefined][] = [
      [undefined, undefined, undefined, undefined],
      [[{ field: 'rated', op: '=', value: true }], undefined, undefined, undefined],
      [undefined, [['moves', 'desc'], ['id', 'asc']], 3, undefined],
      [undefined, [['title', 'asc']], 4, 2],
      [undefined, undefined, undefined, 6],
      [[{ field: 'moves', op: '>', value: 5 }], [['rating', 'asc'], ['id', 'desc']], 2, 1],
    ];
    for (const [where, orderBy, limit, start] of cases) {
      const rows = parseRowBytes<Row[]>(exec.selectTable('game', where, orderBy, limit, start), fromBase64);
      expect(rows).toEqual(sqliteRows('game', where, orderBy, limit, start));
    }
  });

  it('counts, with and without a filter', () => {
    const count = (where?: WhereNode[]): number => {
      const bind: unknown[] = [];
      const sql = `SELECT COUNT(*) AS n FROM "game"` + (where ? ` WHERE ${renderWhereSql(where, bind, {})}` : '');
      return (db.exec({ sql, bind, rowMode: 'object', returnValue: 'resultRows' }) as { n: number }[])[0].n;
    };
    expect(exec.count('game', undefined)).toBe(count());
    for (const where of [
      [{ field: 'rated', op: '=', value: true }],
      [{ field: 'moves', op: '>=', value: 34 }],
      [{ field: 'missing', op: '!=', value: 1 }],
    ] as WhereNode[][]) {
      expect(exec.count('game', where)).toBe(count(where));
    }
    expect(exec.count('never_written', undefined)).toBe(0);
  });

  it('reads one row, or null', () => {
    expect(parseRowBytes<Row | null>(exec.get('game', 'game:e'), fromBase64)).toEqual(sqliteRows('game', [{ field: 'id', op: '=', value: 'game:e' }])[0]);
    expect(parseRowBytes<Row | null>(exec.get('game', 'game:none'), fromBase64)).toBeNull();
    expect(parseRowBytes<Row | null>(exec.get('none', 'x'), fromBase64)).toBeNull();
  });

  it('reads the versions from the index alone', () => {
    const rows = db.exec({
      sql: `SELECT id, json_extract(data, '$._00_rv') AS rv FROM "game" ORDER BY id`,
      rowMode: 'object',
      returnValue: 'resultRows',
    }) as { id: string; rv: unknown }[];
    expect(exec.versions(['game', 'nope'])).toEqual({ game: rows.map((r) => [r.id, Number(r.rv) || 0]), nope: [] });
  });
});

describe('row store vs SQLite: merge', () => {
  const merged = (table: string, id: string): Row | null => {
    const rows = selectDb.exec(`SELECT data FROM "${table}" WHERE id = ?`, [id]);
    return rows[0] ? reviveRow(rows[0].data) : null;
  };

  function mergeBoth(table: string, row: Row): void {
    const { insert, patch } = serializeMergeRow(row);
    db.exec({
      sql: `INSERT INTO "${table}"(id, data) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET data = json_patch(data, ?)`,
      bind: [row.id, insert, patch],
    });
    const { ops, buf } = pack(table, [row], 'merge');
    exec.write(ops, buf, false);
  }

  beforeAll(() => {
    db.exec({ sql: `CREATE TABLE IF NOT EXISTS "doc" (id TEXT PRIMARY KEY, data TEXT NOT NULL)` });
    selectDb.knownTables.add('doc');
  });

  it('folds a patch into the stored row as json_patch does', () => {
    const steps: Row[] = [
      { id: 'doc:1', a: 1, nested: { x: 1, y: { z: [1, 2] } }, list: [1, 2, 3], gone: 'soon', _00_rv: 1 },
      { id: 'doc:1', a: 2, nested: { y: { w: true } }, _00_rv: 2 },
      { id: 'doc:1', nested: { x: null }, list: [9], gone: undefined, _00_rv: 3 },
      { id: 'doc:1', nested: 'now a string', extra: { deep: { deeper: null, kept: 0 } } },
      { id: 'doc:1', nested: { fresh: 1 }, blob: new Uint8Array([1, 2, 3]), a: null },
      { id: 'doc:1', blob: new Uint8Array([9]), extra: { deep: { kept: false } }, _00_rv: 10 },
    ];
    for (const step of steps) {
      mergeBoth('doc', step);
      expect(parseRowBytes<Row>(exec.get('doc', 'doc:1'), fromBase64)).toEqual(merged('doc', 'doc:1'));
    }
    expect(store.find('doc', 'doc:1')?.rv).toBe(10);
    expect(store.find('doc', 'doc:1')?.flags).toBe(1);
  });

  it('keeps a first write as it came, nulls included', () => {
    mergeBoth('doc', { id: 'doc:2', kept: null, cleared: undefined, n: 1 });
    expect(parseRowBytes<Row>(exec.get('doc', 'doc:2'), fromBase64)).toEqual(merged('doc', 'doc:2'));
    expect(merged('doc', 'doc:2')).toEqual({ id: 'doc:2', kept: null, n: 1 });
  });

  it('merges against an earlier op of the same batch', () => {
    const first = { id: 'doc:3', a: 1, b: { c: 1 } };
    const second = { id: 'doc:3', b: { d: 2 }, e: 5 };
    for (const row of [first, second]) {
      const { insert, patch } = serializeMergeRow(row);
      db.exec({
        sql: `INSERT INTO "doc"(id, data) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET data = json_patch(data, ?)`,
        bind: [row.id, insert, patch],
      });
    }
    const a = pack('doc', [first], 'merge');
    const b = pack('doc', [second], 'merge');
    const buf = new Uint8Array(a.buf.byteLength + b.buf.byteLength);
    buf.set(new Uint8Array(a.buf));
    buf.set(new Uint8Array(b.buf), a.buf.byteLength);
    const shifted = b.ops.map((op) => (op[0] === 'merge' ? ([...op.slice(0, 5), (op[5] as number) + a.buf.byteLength, ...op.slice(6)] as WireOp) : op));
    exec.write([...a.ops, ...shifted], buf.buffer, false);
    expect(parseRowBytes<Row>(exec.get('doc', 'doc:3'), fromBase64)).toEqual(merged('doc', 'doc:3'));
    expect(merged('doc', 'doc:3')).toEqual({ id: 'doc:3', a: 1, b: { c: 1, d: 2 }, e: 5 });
  });

  it('treats a merge after a delete or a clear in the same batch as a first write', () => {
    mergeBoth('doc', { id: 'doc:4', old: true });
    const fresh = pack('doc', [{ id: 'doc:4', fresh: true }], 'merge');
    exec.write([['del', 'doc', 'doc:4'], ...fresh.ops], fresh.buf, false);
    expect(parseRowBytes<Row>(exec.get('doc', 'doc:4'), fromBase64)).toEqual({ id: 'doc:4', fresh: true });
    const again = pack('doc', [{ id: 'doc:4', again: true }], 'merge');
    exec.write([['clear', 'doc'], ...again.ops], again.buf, false);
    expect(parseRowBytes<Row[]>(exec.selectTable('doc', undefined, undefined, undefined, undefined), fromBase64)).toEqual([{ id: 'doc:4', again: true }]);
  });
});

describe('row store: version reset and blobs', () => {
  it('resets every cached version except the kept rows', () => {
    const before = exec.versions(['user']).user;
    expect(before.every(([, rv]) => rv === 0)).toBe(true);
    const { ops, buf } = pack('versioned', [
      { id: 'versioned:1', _00_rv: 5, v: 1 },
      { id: 'versioned:2', _00_rv: 7, v: 2 },
      { id: 'versioned:3', v: 3 },
      { id: 'versioned:4', _00_rv: 0, v: 4 },
    ], 'put');
    exec.write(ops, buf, false);
    expect(exec.resetVersions(['versioned', 'absent'], ['versioned:2'])).toBe(1);
    expect(exec.versions(['versioned']).versioned).toEqual([
      ['versioned:1', 0],
      ['versioned:2', 7],
      ['versioned:3', 0],
      ['versioned:4', 0],
    ]);
    const rows = parseRowBytes<Row[]>(exec.selectTable('versioned', undefined, undefined, undefined, undefined), fromBase64);
    expect(rows).toEqual([
      { id: 'versioned:1', _00_rv: 0, v: 1 },
      { id: 'versioned:2', _00_rv: 7, v: 2 },
      { id: 'versioned:3', v: 3 },
      { id: 'versioned:4', _00_rv: 0, v: 4 },
    ]);
  });

  it('stores a blob beside its meta and keeps both out of row reads', () => {
    const bytes = new Uint8Array(70_000);
    for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 7) & 255;
    exec.putBlob('_00_circuit_snapshot', 'circuit', bytes.slice().buffer, JSON.stringify({ formatVersion: 3 }));
    const got = exec.getBlob('_00_circuit_snapshot', 'circuit');
    expect(new Uint8Array(got.bytes!)).toEqual(bytes);
    expect(JSON.parse(got.meta!)).toEqual({ formatVersion: 3 });
    expect(parseRowBytes<Row[]>(exec.selectTable('_00_circuit_snapshot', undefined, undefined, undefined, undefined), fromBase64)).toEqual([]);
    expect(exec.count('_00_circuit_snapshot', [{ field: 'a', op: '=', value: 1 }])).toBe(0);
    exec.deleteBlob('_00_circuit_snapshot', 'circuit');
    expect(exec.getBlob('_00_circuit_snapshot', 'circuit')).toEqual({ bytes: null, meta: null });
  });
});

describe('row store vs SQLite: indexes', () => {
  const hidden = (): string[] => store.tableNames().filter((t) => t.startsWith('\u0001i'));

  it('indexes record links as rows are written, and filtered fields on first use', async () => {
    // `white`, `black` and the comments' links were found while seeding.
    expect(hidden()).toEqual(expect.arrayContaining(['\u0001i\u0001game\u0001white', '\u0001i\u0001comment\u0001game', '\u0001i\u0001comment\u0001author']));
    // `title` got one when a query filtered on it; nothing ever filtered on `blob`.
    expect(hidden()).toContain('\u0001i\u0001game\u0001title');
    expect(hidden()).not.toContain('\u0001i\u0001game\u0001blob');
    // A URL is not a link.
    const { ops, buf } = pack('site', [{ id: 'site:1', url: 'https://example.com/a', owner: 'user:1' }], 'put');
    exec.write(ops, buf, false);
    expect(hidden()).toContain('\u0001i\u0001site\u0001owner');
    expect(hidden()).not.toContain('\u0001i\u0001site\u0001url');
    // The client's own tables are read whole.
    const internal = pack('_00_view', [{ id: '_00_view:1', owner: 'user:1' }], 'put');
    exec.write(internal.ops, internal.buf, false);
    expect(hidden().some((t) => t.includes('_00_view'))).toBe(false);
  });

  it('stays right through inserts, updates, deletes and clears, and across a reopen', async () => {
    let seedValue = 4242;
    const rnd = (n: number): number => {
      seedValue = (Math.imul(seedValue, 1103515245) + 12345) & 0x7fffffff;
      return seedValue % n;
    };
    db.exec({ sql: `CREATE TABLE IF NOT EXISTS "churn" (id TEXT PRIMARY KEY, data TEXT NOT NULL)` });
    selectDb.knownTables.add('churn');
    const owners = Array.from({ length: 12 }, (_, i) => `user:${i}`);
    const kinds = Array.from({ length: 20 }, (_, i) => `kind-${i}`);
    const check = async (): Promise<void> => {
      for (const owner of [owners[rnd(12)], owners[rnd(12)], 'user:none']) {
        await same({ table: 'churn', where: [{ field: 'owner', op: '=', value: owner }] });
        await same({ table: 'churn', where: [{ field: 'owner', op: '=', value: owner }, { field: 'n', op: '>', value: 40 }], orderBy: [['n', 'desc'], ['id', 'asc']], limit: 5 });
      }
      await same({ table: 'churn', where: [{ field: 'kind', op: '=', value: kinds[rnd(20)] }] });
      await same({ table: 'churn', where: [{ field: 'flag', op: '=', value: true }], limit: 7 });
      await same({ table: 'churn', where: [{ field: 'n', op: '=', value: rnd(100) }] });
      await same({ table: 'churn', where: [{ field: 'id', op: '=', value: `churn:${rnd(150)}` }] });
      await same({ table: 'churn', where: [{ field: 'owner', op: '=', value: null }] });
      const keys = [owners[rnd(12)], owners[rnd(12)], owners[rnd(12)]];
      const mine = parseRowBytes<Row[]>(exec.relation({ table: 'churn', matchField: 'owner', keys, orderBy: [['id', 'asc']] }), fromBase64);
      const bind = keys.map(() => '?').join(', ');
      const theirs = selectDb.exec(`SELECT data FROM "churn" WHERE json_extract(data, '$.owner') IN (${bind}) ORDER BY id`, keys).map((r) => reviveRow(r.data));
      expect(mine).toEqual(theirs);
    };
    for (let step = 0; step < 500; step++) {
      const id = `churn:${rnd(150)}`;
      const roll = rnd(100);
      if (roll < 2) {
        db.exec({ sql: `DELETE FROM "churn"` });
        exec.write([['clear', 'churn']], null, false);
      } else if (roll < 20) {
        db.exec({ sql: `DELETE FROM "churn" WHERE id = ?`, bind: [id] });
        exec.write([['del', 'churn', id]], null, false);
      } else {
        const row: Row = { id, n: rnd(100) };
        if (rnd(4) !== 0) row.owner = rnd(10) === 0 ? null : owners[rnd(12)];
        if (rnd(3) !== 0) row.kind = kinds[rnd(20)];
        if (rnd(3) === 0) row.flag = rnd(2) === 0;
        const { insert, patch } = serializeMergeRow(row);
        db.exec({
          sql: `INSERT INTO "churn"(id, data) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET data = json_patch(data, ?)`,
          bind: [id, insert, patch],
        });
        const packed = pack('churn', [row], rnd(5) === 0 ? 'put' : 'merge');
        if (packed.ops[0][0] === 'put') db.exec({ sql: `UPDATE "churn" SET data = ? WHERE id = ?`, bind: [insert, id] });
        exec.write(packed.ops, packed.buf, false);
      }
      if (step % 50 === 49) await check();
      if (step === 250) {
        store.checkpoint();
        exec = new RowsExec(store);
      }
    }
    await check();
    // `owner` (links) and `kind` (20 values) are indexed; `flag` (2 values) is left to scans.
    expect(hidden()).toEqual(expect.arrayContaining(['\u0001i\u0001churn\u0001owner', '\u0001i\u0001churn\u0001kind']));
  });

  it('hands out only the tables a client wrote', () => {
    expect(visibleTables(store).some((t) => t.startsWith('\u0001'))).toBe(false);
    expect(visibleTables(store)).toContain('game');
  });
});
