import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import { COLUMNS_ROW_ID, columnFingerprints, planColumnReset } from './sqlite-column-reset';

/**
 * A body landed under an older schema lacks the columns that schema did not
 * know, at the server's version. Without a reset the next build never fetches
 * it again (WhitePawn's puzzles stuck on "Preparing" for want of `layout`).
 * These run the planned statements against a REAL in-memory SQLite, the same
 * build the worker loads.
 */

let db: any;

const rows = (sql: string, bind?: unknown[]) =>
  db.exec({ sql, bind, rowMode: 'object', returnValue: 'resultRows' }) as Record<string, unknown>[];

const versions = (table: string) =>
  Object.fromEntries(
    rows(`SELECT id, json_extract(data, '$._00_rv') AS rv FROM "${table}"`).map((r) => [r.id, r.rv])
  );

const storedSets = () => {
  const r = rows(`SELECT data FROM "_00_schema" WHERE id = ?`, [COLUMNS_ROW_ID]);
  return r[0] ? (JSON.parse(r[0].data as string) as Record<string, string>) : null;
};

const existing = () => new Set(rows(`SELECT name FROM sqlite_master WHERE type = 'table'`).map((r) => r.name as string));

function run(current: Record<string, string>, keep: string[] = []) {
  const plan = planColumnReset(storedSets(), current, existing(), keep);
  db.exec({ sql: 'BEGIN' });
  for (const s of plan.statements) db.exec({ sql: s.sql, bind: s.bind });
  db.exec({ sql: 'COMMIT' });
  return plan;
}

beforeEach(async () => {
  const sqlite3: any = await sqlite3InitModule();
  db = new sqlite3.oo1.DB(':memory:', 'c');
  for (const t of ['_00_schema', 'puzzle', 'game']) {
    db.exec({ sql: `CREATE TABLE "${t}" (id TEXT PRIMARY KEY, data TEXT NOT NULL)` });
  }
  const put = (t: string, id: string, body: object) =>
    db.exec({ sql: `INSERT INTO "${t}"(id, data) VALUES(?, ?)`, bind: [id, JSON.stringify(body)] });
  // Landed by the old build: no `layout`, at the server's version 6.
  put('puzzle', 'puzzle:a', { title: 'A', _00_rv: 6 });
  put('puzzle', 'puzzle:b', { title: 'B', _00_rv: 6 });
  put('game', 'game:1', { white: 'x', _00_rv: 3 });
});

afterEach(() => db?.close());

const OLD = { puzzle: 'title', game: 'white' };
const NEW = { puzzle: 'layout,title', game: 'white' };

describe('column reset on the SQLite cache', () => {
  it('fingerprints are the sorted declared columns', () => {
    expect(columnFingerprints([{ name: 'puzzle', columns: { title: {}, layout: {} } }])).toEqual({
      puzzle: 'layout,title',
    });
  });

  it('first boot with no recorded sets resets every cached table once', () => {
    const plan = run(NEW);
    expect(plan.changed.sort()).toEqual(['game', 'puzzle']);
    expect(versions('puzzle')).toEqual({ 'puzzle:a': 0, 'puzzle:b': 0 });
    expect(versions('game')).toEqual({ 'game:1': 0 });
    expect(storedSets()).toEqual(NEW);
    // Bodies stay: they keep rendering until the refetch lands.
    expect(rows(`SELECT data FROM puzzle WHERE id = 'puzzle:a'`)[0].data).toContain('"title":"A"');

    // The next boot of the same build changes nothing.
    const again = run(NEW);
    expect(again.statements).toEqual([]);
  });

  it('a new column resets only that table', () => {
    run(OLD);
    db.exec({ sql: `UPDATE puzzle SET data = json_set(data, '$._00_rv', 6)` });
    db.exec({ sql: `UPDATE game SET data = json_set(data, '$._00_rv', 3)` });

    const plan = run(NEW);
    expect(plan.changed).toEqual(['puzzle']);
    expect(versions('puzzle')).toEqual({ 'puzzle:a': 0, 'puzzle:b': 0 });
    expect(versions('game')).toEqual({ 'game:1': 3 });
  });

  it('keeps the version of a row with an unsettled local write', () => {
    const plan = run(NEW, ['puzzle:b', 'game:9']);
    expect(plan.changed).toContain('puzzle');
    expect(versions('puzzle')).toEqual({ 'puzzle:a': 0, 'puzzle:b': 6 });
  });

  it('records a table the store has no rows for without touching it', () => {
    const plan = run({ ...NEW, puzzle_chapter: 'puzzle,sort,title' });
    expect(plan.changed).not.toContain('puzzle_chapter');
    expect(storedSets()).toEqual({ ...NEW, puzzle_chapter: 'puzzle,sort,title' });
  });
});
