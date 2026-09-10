import { describe, it, expect } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { projectedDataSql, project, reviveRow, serializeRow } from './sqlite-plan-sql';
import type { Row } from './cache-engine';

/**
 * `projectedDataSql` is the one change in this area that alters what SQLite
 * actually returns, and every row read goes through it — so it is verified
 * against a REAL SQLite, not the scripted responder the parity tests use.
 *
 * The contract: byte-identical to parsing the whole row and running `project`
 * over it, which is what both engine paths did before. The traps it has to
 * clear are an ABSENT key (must stay absent, not become an explicit null — the
 * difference `json_object`/`json_extract` would have introduced), a STORED
 * null, nested objects/arrays, the `{__u8}` tag that carries binary, and a
 * TOP-LEVEL boolean (SQLite has no boolean type, so an unguarded projection
 * hands `true` back as the integer 1).
 */

function makeDb(rows: Row[]) {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE "game" (id TEXT PRIMARY KEY, data TEXT NOT NULL)');
  const ins = db.prepare('INSERT INTO "game" (id,data) VALUES (?,?)');
  for (const r of rows) ins.run(String(r.id), serializeRow(r));
  return db;
}

/** What the code did before: read the whole row, parse it, then project. */
function referenceRows(db: DatabaseSync, ids: string[], fields: string[]): Row[] {
  const ph = ids.map(() => '?').join(', ');
  return db
    .prepare(`SELECT data FROM "game" WHERE id IN (${ph}) ORDER BY id ASC`)
    .all(...ids)
    .map((r) => project(reviveRow((r as { data: string }).data), fields));
}

/** What the code does now: let SQLite narrow the row before it is ever parsed. */
function projectedRows(db: DatabaseSync, ids: string[], fields: string[]): Row[] {
  const bind: unknown[] = [];
  const col = projectedDataSql(fields, bind);
  bind.push(...ids);
  const ph = ids.map(() => '?').join(', ');
  return db
    .prepare(`SELECT ${col} FROM "game" WHERE id IN (${ph}) ORDER BY id ASC`)
    .all(...(bind as string[]))
    .map((r) => reviveRow((r as { data: string }).data));
}

const FIXTURE: Row[] = [
  { id: 'game:1', white: 'player_name:PN_a', pgn: '[Event "x"]\n1. e4 e5', sort_index: -5 },
  { id: 'game:2', white: null, pgn: 'heavy', sort_index: -3 }, // STORED null
  { id: 'game:3', pgn: 'heavy', sort_index: -4 }, // 'white' ABSENT
  { id: 'game:4', white: 'w', pgn: 'heavy', sort_index: -1, meta: { a: 1, b: [2, 3] } },
  { id: 'game:5', white: 'w', pgn: 'heavy', sort_index: 0, rated: true, draw: false },
];
const IDS = ['game:1', 'game:2', 'game:3', 'game:4', 'game:5'];
const FIELDS = ['white', 'sort_index', 'meta', 'rated', 'draw'];

describe('projectedDataSql against real SQLite', () => {
  it('is byte-identical to parse-then-project', () => {
    const db = makeDb(FIXTURE);
    expect(projectedRows(db, IDS, FIELDS)).toEqual(referenceRows(db, IDS, FIELDS));
  });

  it('never returns the fields that were not asked for', () => {
    // The point of the change: `pgn` must not leave SQLite, so it is never
    // parsed, never crosses the worker boundary, never reaches the store.
    const db = makeDb(FIXTURE);
    for (const row of projectedRows(db, IDS, FIELDS)) expect('pgn' in row).toBe(false);
  });

  it('keeps an absent key absent and a stored null null', () => {
    const db = makeDb(FIXTURE);
    const byId = new Map(projectedRows(db, IDS, FIELDS).map((r) => [r.id, r]));
    expect('white' in byId.get('game:3')!).toBe(false); // absent stays absent
    expect('white' in byId.get('game:2')!).toBe(true); // stored null is a real key
    expect(byId.get('game:2')!.white).toBeNull();
  });

  it('round-trips nested objects and arrays as JSON, not as strings', () => {
    const db = makeDb(FIXTURE);
    const row = projectedRows(db, IDS, FIELDS).find((r) => r.id === 'game:4')!;
    expect(row.meta).toEqual({ a: 1, b: [2, 3] });
  });

  it('round-trips binary through the {__u8} tag', () => {
    const db = makeDb([{ id: 'game:9', blob: new Uint8Array([0, 1, 2]), pgn: 'heavy' }]);
    const [row] = projectedRows(db, ['game:9'], ['blob']);
    expect(row!.blob).toEqual(new Uint8Array([0, 1, 2]));
  });

  it('always includes id, even when it is not in the field list', () => {
    const db = makeDb(FIXTURE);
    for (const row of projectedRows(db, IDS, ['sort_index'])) expect(row.id).toBeTruthy();
  });

  it('round-trips a top-level boolean as a boolean, not as 1/0', () => {
    // The bug this guards: `json_each` yields SQLite integers for JSON booleans,
    // so a projected row read `rated: 1` and every `row.rated === true` in a
    // consuming app went false as soon as the screen painted from the cache.
    // WhitePawn's /explore detail page read exactly that as "this collection was
    // unpublished or deleted by its author".
    const db = makeDb(FIXTURE);
    const row = projectedRows(db, IDS, FIELDS).find((r) => r.id === 'game:5')!;
    expect(row.rated).toBe(true);
    expect(row.draw).toBe(false);
  });

  it('leaves a boolean nested in an object or array alone', () => {
    const db = makeDb([{ id: 'game:7', flags: { on: true, off: false }, list: [true, false] }]);
    const [row] = projectedRows(db, ['game:7'], ['flags', 'list']);
    expect(row!.flags).toEqual({ on: true, off: false });
    expect(row!.list).toEqual([true, false]);
  });

  it('yields an empty object, not NULL, for a row sharing none of the keys', () => {
    // Without COALESCE the subquery returns NULL here and reviveRow throws.
    const db = makeDb([{ id: 'game:8', pgn: 'heavy' }]);
    expect(projectedRows(db, ['game:8'], ['nothing_matches'])).toEqual([{ id: 'game:8' }]);
  });
});
