import { describe, it, expect, beforeAll } from 'vitest';
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import { SqliteCacheEngine } from './sqlite-cache-engine';
import { stubTransport } from './sqlite-transport.fixture';
import { serializeRow } from './sqlite-plan-sql';
import type { Row } from './cache-engine';

/**
 * Integration: the DevTools Database explorer's statements run end-to-end
 * (translate → engine SQL → REAL in-memory SQLite, the same
 * @sqlite.org/sqlite-wasm build the worker loads). The unit test asserts the
 * translation; this asserts the SQL actually executes and returns the right
 * rows — the paging window in particular is easy to render into invalid SQLite
 * (there is no bare OFFSET).
 */

let db: any;

function makeLogger(): any {
  const noop = () => {};
  const l: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
  l.child = () => l;
  return l;
}

/** An engine whose transport is a real SQLite database. */
function realEngine(): SqliteCacheEngine {
  const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger());
  stubTransport(engine, (type, payload: any) => {
    switch (type) {
      case 'open':
        for (const t of payload.systemTables ?? []) {
          db.exec({
            sql: `CREATE TABLE IF NOT EXISTS "${t}" (id TEXT PRIMARY KEY, data TEXT NOT NULL)`,
          });
        }
        return { persisted: true };
      case 'exec':
        return {
          rows: db.exec({
            sql: payload.sql,
            bind: payload.bind,
            rowMode: 'object',
            returnValue: 'resultRows',
          }),
        };
      case 'run':
        db.exec({ sql: payload.sql, bind: payload.bind });
        return {};
      case 'batch':
        for (const stmt of payload as { sql: string; bind?: unknown[] }[]) {
          db.exec({ sql: stmt.sql, bind: stmt.bind });
        }
        return {};
      default:
        return {};
    }
  });
  return engine;
}

function seed(table: string, rows: Row[]): void {
  db.exec({
    sql: `CREATE TABLE IF NOT EXISTS "${table}" (id TEXT PRIMARY KEY, data TEXT NOT NULL)`,
  });
  for (const row of rows) {
    db.exec({
      sql: `INSERT INTO "${table}"(id, data) VALUES(?, ?)`,
      bind: [row.id, serializeRow(row)],
    });
  }
}

beforeAll(async () => {
  const sqlite3: any = await sqlite3InitModule();
  db = new sqlite3.oo1.DB(':memory:', 'c');
  seed(
    'game_insight',
    Array.from({ length: 5 }, (_, i) => ({
      id: `game_insight:${i}`,
      n: i,
      tag: i % 2 ? 'odd' : 'even',
    }))
  );
});

describe('DevTools explorer against real SQLite', () => {
  it('pages a table with LIMIT/START', async () => {
    const engine = realEngine();
    await engine.connect('anon');

    const [page1] = await engine.query<[Row[]]>('SELECT * FROM game_insight LIMIT 2 START 0');
    const [page2] = await engine.query<[Row[]]>('SELECT * FROM game_insight LIMIT 2 START 2');
    const [tail] = await engine.query<[Row[]]>('SELECT * FROM game_insight LIMIT 20 START 4');

    expect(page1.map((r) => r.id)).toEqual(['game_insight:0', 'game_insight:1']);
    expect(page2.map((r) => r.id)).toEqual(['game_insight:2', 'game_insight:3']);
    expect(tail.map((r) => r.id)).toEqual(['game_insight:4']);
  });

  it('counts rows, with and without a filter', async () => {
    const engine = realEngine();
    await engine.connect('anon');

    const [all] = await engine.query<[{ count: number }[]]>(
      'SELECT count() FROM game_insight GROUP ALL'
    );
    const [odd] = await engine.query<[{ count: number }[]]>(
      "SELECT count() FROM game_insight WHERE tag = 'odd' GROUP ALL"
    );

    expect(all).toEqual([{ count: 5 }]);
    expect(odd).toEqual([{ count: 2 }]);
  });

  it('lists tables via INFO FOR DB', async () => {
    const engine = realEngine();
    await engine.connect('anon');

    const [info] = await engine.query<[{ tables: Record<string, string> }]>('INFO FOR DB');

    expect(Object.keys(info.tables)).toContain('game_insight');
    // Seeded system tables show up too — the panel's "internal" toggle filters
    // them, the engine must not pre-filter.
    expect(Object.keys(info.tables)).toContain('_00_view');
    expect(Object.keys(info.tables).some((t) => t.startsWith('sqlite_'))).toBe(false);
  });

  it('edits and deletes a row by literal record id', async () => {
    const engine = realEngine();
    await engine.connect('anon');

    await engine.query('UPDATE game_insight:1 MERGE $updates', { updates: { tag: 'edited' } });
    const [edited] = await engine.query<[Row[]]>('SELECT * FROM game_insight LIMIT 20 START 1');
    expect(edited[0]).toMatchObject({ id: 'game_insight:1', n: 1, tag: 'edited' });

    await engine.query('DELETE game_insight:1');
    const [after] = await engine.query<[{ count: number }[]]>(
      'SELECT count() FROM game_insight GROUP ALL'
    );
    expect(after).toEqual([{ count: 4 }]);
  });
});
