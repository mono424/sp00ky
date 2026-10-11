import assert from 'node:assert/strict';
import test from 'node:test';
import {
  expectedGameIds,
  gameId,
  gameRecord,
  gameSql,
  gameWindowParams,
  gameWindowQuery,
  insertGamesSql,
  presetCases,
  scrollOffsets,
  seedWhitepawn,
  WHITEPAWN_PAGE,
} from './whitepawn.js';

test('synthetic records are deterministic, linked and PGN payloads vary', () => {
  assert.deepEqual(gameRecord(123, 1000, true), gameRecord(123, 1000, true));
  assert.equal(gameId(1), 'game:g000000001');
  const compact = gameRecord(1, 1000);
  const heavy = gameRecord(1, 1000, true);
  assert.equal(compact.pgn, undefined);
  assert.ok(heavy.pgn!.length >= 1024);
  assert.ok(heavy.pgn!.length < 1400);
  assert.notEqual(heavy.pgn, gameRecord(2, 1000, true).pgn);
  assert.match(compact.white, /^player_name:p\d{3}$/);
  assert.match(gameSql(compact), /date: d"2026-01-01T00:00:00.000Z"/);
});

test('distribution is 80% main with nineteen small collections and tied sort keys', () => {
  const counts = new Map<string, number>();
  for (let index = 1; index <= 1900; index++) {
    const row = gameRecord(index, 1900);
    counts.set(row.database, (counts.get(row.database) ?? 0) + 1);
  }
  assert.equal(counts.get('game_database:main'), 1520);
  assert.equal(counts.size, 20);
  for (const [collection, count] of counts) {
    if (collection !== 'game_database:main') assert.equal(count, 20);
  }
  assert.equal(gameRecord(1, 1900).sort_index, gameRecord(4, 1900).sort_index);
  assert.ok(gameRecord(5, 1900).sort_index > gameRecord(4, 1900).sort_index);
});

test('window oracle matches independently sorted rows at boundaries and for filters', () => {
  const rows = Array.from({ length: 1900 }, (_, index) => gameRecord(index + 1, 1900));
  for (const opts of [{}, { filtered: true }, { collection: 'game_database:c19' }]) {
    const matching = rows
      .filter(
        (row) =>
          row.database === (opts.collection ?? 'game_database:main') &&
          (!opts.filtered || (row.result === '1-0' && row.speed === 'blitz'))
      )
      .sort((a, b) => a.sort_index - b.sort_index || a.id.localeCompare(b.id));
    for (const offset of [0, 49, 50, 100, 1499, 1520, 2000]) {
      assert.deepEqual(
        expectedGameIds(1900, offset, opts),
        matching.slice(offset, offset + WHITEPAWN_PAGE).map((row) => row.id)
      );
    }
  }
});

test('batches are bounded and count actual serialized bodies', () => {
  const batch = insertGamesSql(1, 5, 100);
  const bytes = Array.from({ length: 5 }, (_, index) =>
    Buffer.byteLength(JSON.stringify(gameRecord(index + 1, 100)))
  ).reduce((sum, bytes) => sum + bytes, 0);
  assert.equal(batch.serializedBytes, bytes);
  assert.equal(batch.mainCount, 4);
  assert.match(batch.sql, /^INSERT INTO game \[/);
  assert.throws(() => insertGamesSql(1, 501, 1000), /cannot exceed/);
});

test('an already aborted seed stops before opening a database connection', async () => {
  const controller = new AbortController();
  const reason = new Error('Benchmark interrupted');
  controller.abort(reason);
  await assert.rejects(
    seedWhitepawn(
      {} as Parameters<typeof seedWhitepawn>[0],
      10_000,
      false,
      undefined,
      controller.signal
    ),
    (error) => error === reason
  );
});

test('window queries use projections, two related names, stable order and fixed limit', () => {
  const query = gameWindowQuery(50);
  assert.match(
    query,
    /SELECT id, white, black, date, result, eco, source, avatar, sort_index, status, database,/
  );
  assert.match(query, /WHERE id=\$parent.white LIMIT 1\)\[0\] AS white/);
  assert.match(query, /WHERE id=\$parent.black LIMIT 1\)\[0\] AS black/);
  assert.match(query, /ORDER BY sort_index ASC, id ASC LIMIT 50 START 50;/);
  assert.match(query, /WHERE database = \$database ORDER BY/);
  assert.doesNotMatch(query, /WHERE database = game_database:/);
  assert.doesNotMatch(query, /\bpgn\b|SELECT \*/);
  assert.match(gameWindowQuery(0, { filtered: true }), /result = "1-0" AND speed = "blitz"/);
  assert.throws(
    () => gameWindowQuery(0, { collection: 'game_database:main; DELETE game' }),
    /Unknown synthetic collection/
  );
});

test('record-valued collection filters are bound separately from the query shape', () => {
  assert.deepEqual(gameWindowParams(), { database: 'game_database:main' });
  assert.deepEqual(gameWindowParams({ collection: 'game_database:c19' }), {
    database: 'game_database:c19',
  });
  assert.equal(gameWindowQuery(0), gameWindowQuery(0, { collection: 'game_database:c19' }));
  assert.throws(
    () => gameWindowParams({ collection: 'game_database:missing' }),
    /Unknown synthetic collection/
  );
});

test('scroll trace reaches large offsets, exceeds retained windows and revisits zero', () => {
  const offsets = scrollOffsets(1_000_000);
  assert.equal(offsets.length, 100);
  assert.equal(offsets[0], 0);
  assert.equal(offsets.at(-1), 0);
  assert.ok(new Set(offsets).size > 20);
  assert.ok(Math.max(...offsets) >= 799_000);
  assert.ok(
    offsets.every((offset) => offset >= 0 && offset % WHITEPAWN_PAGE === 0 && offset < 800_000)
  );
  assert.deepEqual(scrollOffsets(1), Array(100).fill(0));
  assert.deepEqual(scrollOffsets(1_000_000), offsets);
});

test('profiles cover smoke, standard, and default-large 10M rows and validate sizes', () => {
  assert.deepEqual(presetCases('smoke'), [
    { name: 'metadata-10000', rows: 10_000, pgnPayload: false },
    { name: 'pgn-10000', rows: 10_000, pgnPayload: true },
  ]);
  assert.deepEqual(presetCases('standard'), [
    { name: 'metadata-100000', rows: 100_000, pgnPayload: false },
    { name: 'metadata-1000000', rows: 1_000_000, pgnPayload: false },
    { name: 'pgn-1000000', rows: 1_000_000, pgnPayload: true },
  ]);
  assert.deepEqual(presetCases('large').at(-1), {
    name: 'metadata-10000000',
    rows: 10_000_000,
    pgnPayload: false,
  });
  assert.throws(() => gameRecord(0, 10), /index/);
  assert.throws(() => scrollOffsets(-1), /rows/);
  assert.throws(() => gameWindowQuery(0.5), /offset/);
});
