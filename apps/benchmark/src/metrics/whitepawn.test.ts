import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { RecordId, type Surreal } from 'surrealdb';
import {
  expectMutationPublication,
  expectedFromRows,
  MAX_RETAINED_QUERIES,
  percentileSummary,
  publicationMatches,
  readPublication,
  readRetainedQueries,
  retainedQueriesMatch,
  snapshotExpected,
  summarizePhase,
  waitPublication,
  waitRetainedQueries,
  writeWhitePawnReport,
  type ExpectedPublication,
  type PublishedSnapshot,
  type RunReport,
} from './whitepawn.js';

const expected: ExpectedPublication = {
  rootIds: ['game:a', 'game:b'],
  childIds: ['player_name:white', 'player_name:black'],
  versions: { 'game:a': 2, 'game:b': 3, 'player_name:white': 5, 'player_name:black': 4 },
  childLinks: {
    'player_name:white': [
      { rootId: 'game:a', alias: 'white' },
      { rootId: 'game:b', alias: 'white' },
    ],
    'player_name:black': [
      { rootId: 'game:a', alias: 'black' },
      { rootId: 'game:b', alias: 'black' },
    ],
  },
};

function published(): PublishedSnapshot {
  return {
    state: 'ready',
    rowCount: 2,
    overflow: false,
    edges: [
      { id: '_00_list_ref_user_bench:r1', out: 'game:a', version: 2 },
      { id: '_00_list_ref_user_bench:r2', out: 'game:b', version: 3 },
      {
        id: '_00_list_ref_user_bench:c1',
        out: 'player_name:white',
        version: 5,
        parent: '_00_list_ref_user_bench:r2',
        parentRel: 'white',
      },
      {
        id: '_00_list_ref_user_bench:c2',
        out: 'player_name:black',
        version: 4,
        parent: '_00_list_ref_user_bench:r1',
        parentRel: 'black',
      },
    ],
  };
}

test('publication oracle accepts deduplicated children with any correct parent', () => {
  assert.deepEqual(publicationMatches(published(), expected, { initial: true }), {
    matches: true,
    reasons: [],
  });
});

test('mutation correlation requires a new publication beyond an already-higher committed version', () => {
  const committed = published();
  committed.edges[0]!.version = 7;
  const next = expectMutationPublication(expected, committed, 'game:a');
  assert.equal(next.versions['game:a'], 8);
  assert.equal(expected.versions['game:a'], 2);
  assert.equal(publicationMatches(committed, next).matches, false);
  committed.edges[0]!.version = 8;
  assert.equal(publicationMatches(committed, next).matches, true);
});

test('joined-name mutation correlation advances only the touched child version', () => {
  const committed = published();
  committed.edges[2]!.version = 6;
  const next = expectMutationPublication(expected, committed, 'player_name:white');
  assert.equal(next.versions['player_name:white'], 7);
  assert.equal(next.versions['game:a'], expected.versions['game:a']);
  assert.equal(next.versions['game:b'], expected.versions['game:b']);
  assert.equal(next.versions['player_name:black'], expected.versions['player_name:black']);
  assert.deepEqual(next.rootIds, expected.rootIds);
  assert.deepEqual(next.childIds, expected.childIds);
  assert.equal(publicationMatches(committed, next).matches, false);
  committed.edges[2]!.version = 7;
  assert.equal(publicationMatches(committed, next).matches, true);
});

test('mutation correlation rejects missing or duplicate touched publication edges', () => {
  const missing = published();
  missing.edges.splice(2, 1);
  assert.throws(() => expectMutationPublication(expected, missing, 'player_name:white'), /found 0/);
  const duplicate = published();
  duplicate.edges.push({ ...duplicate.edges[0]!, id: '_00_list_ref_user_bench:duplicate' });
  assert.throws(() => expectMutationPublication(expected, duplicate, 'game:a'), /found 2/);
  assert.throws(
    () => expectMutationPublication(expected, published(), 'game:missing'),
    /Missing source mutation baseline/
  );
});

test('ready metadata does not hide wrong membership, duplicate edges, or stale versions', () => {
  const missing = published();
  missing.edges.splice(0, 1);
  assert.equal(publicationMatches(missing, expected, { initial: true }).matches, false);
  const duplicate = published();
  duplicate.edges.push({ ...duplicate.edges[0]!, id: '_00_list_ref_user_bench:duplicate' });
  assert.match(publicationMatches(duplicate, expected).reasons.join(' '), /Duplicate root/);
  const staleGame = published();
  staleGame.edges[0]!.version = 1;
  assert.match(publicationMatches(staleGame, expected).reasons.join(' '), /version for game:a/);
  const staleName = published();
  staleName.edges[2]!.version = 4;
  assert.match(
    publicationMatches(staleName, expected).reasons.join(' '),
    /version for player_name:white/
  );
});

test('publication oracle rejects wrong aliases, dangling parents, and overflow', () => {
  const wrongAlias = published();
  wrongAlias.edges[2]!.parentRel = 'black';
  assert.equal(publicationMatches(wrongAlias, expected).matches, false);
  const dangling = published();
  dangling.edges[2]!.parent = '_00_list_ref_user_bench:missing';
  assert.match(
    publicationMatches(dangling, expected).reasons.join(' '),
    /Invalid published parent/
  );
  const overflow = published();
  overflow.overflow = true;
  assert.equal(publicationMatches(overflow, expected).matches, false);
});

test('initial publication requires ready state while incremental checks rely on edges', () => {
  const actual = published();
  actual.state = 'materializing';
  assert.equal(publicationMatches(actual, expected, { initial: true }).matches, false);
  assert.equal(
    publicationMatches(
      { state: null, rowCount: null, edges: [], overflow: false },
      { rootIds: [], childIds: [], versions: {} }
    ).matches,
    false
  );
  assert.equal(publicationMatches(actual, expected).matches, true);
  actual.state = 'ready';
  actual.rowCount = 0;
  assert.equal(publicationMatches(actual, expected, { initial: true }).matches, false);
});

test('nearest-rank percentiles preserve raw order and distinguish empty samples', () => {
  const values = [9, 1, 8, 2, 7];
  assert.deepEqual(percentileSummary(values), {
    n: 5,
    invalid: 0,
    min: 1,
    max: 9,
    mean: 5.4,
    p50: 7,
    p95: 9,
    p99: 9,
  });
  assert.deepEqual(values, [9, 1, 8, 2, 7]);
  assert.equal(percentileSummary([]).p95, null);
  assert.equal(percentileSummary([NaN, -1, 0]).invalid, 2);
});

test('failed samples never contribute fabricated latency percentiles', () => {
  const summary = summarizePhase({
    samples: [
      { name: 'complete', publishedReadyMs: 9 },
      { name: 'timeout', failed: true, publishedReadyMs: 0 },
      { name: 'failed', status: 'failed', publishedReadyMs: 1 },
    ],
  });
  assert.equal(summary.publishedReadyMs!.n, 1);
  assert.equal(summary.publishedReadyMs!.p50, 9);
  assert.equal(
    summarizePhase({ samples: [{ name: 'timeout', failed: true, publishedReadyMs: 0 }] })
      .publishedReadyMs!.p50,
    null
  );
});

test('partial reports retain completed samples and failed, limited, and pending cases', async () => {
  const outDir = fs.mkdtempSync(path.join(os.tmpdir(), 'whitepawn-report-'));
  try {
    const report: RunReport = {
      suite: 'whitepawn',
      startedAt: '2026-10-11T00:00:00.000Z',
      profile: 'smoke',
      config: {},
      env: {},
      cases: [
        {
          name: 'small',
          rows: 100,
          status: 'running',
          phase: 'registration',
          phases: {
            registration: {
              samples: [
                { name: 'a', publishedReadyMs: 9 },
                { name: 'b', publishedReadyMs: 1 },
              ],
            },
          },
        },
        { name: 'large', rows: 1000, status: 'pending', phases: {} },
      ],
    };
    await writeWhitePawnReport(outDir, report);
    report.cases[0]!.status = 'failed';
    report.cases[0]!.error = 'publication timeout';
    report.cases.push({
      name: 'huge',
      rows: 10_000_000,
      status: 'resource-limited',
      error: 'memory cap',
      phases: {},
    });
    report.status = 'failed';
    report.validationIssue =
      'Historical run retained evicted query metadata; capacity results are invalid.';
    await writeWhitePawnReport(outDir, report);
    const samples = JSON.parse(fs.readFileSync(path.join(outDir, 'samples.json'), 'utf8'));
    assert.deepEqual(
      samples.cases[0].phases.registration.samples.map(
        (sample: { publishedReadyMs: number }) => sample.publishedReadyMs
      ),
      [9, 1]
    );
    assert.equal(samples.summaries[0].phases.registration.publishedReadyMs.n, 2);
    const run = JSON.parse(fs.readFileSync(path.join(outDir, 'run.json'), 'utf8'));
    assert.deepEqual(
      run.cases.map((entry: { status: string }) => entry.status),
      ['failed', 'pending', 'resource-limited']
    );
    const summary = fs.readFileSync(path.join(outDir, 'summary.md'), 'utf8');
    assert.match(summary, /publication timeout/);
    assert.match(summary, /resource-limited/);
    assert.match(
      summary,
      /\*\*VALIDATION ISSUE: Historical run retained evicted query metadata; capacity results are invalid\.\*\*/
    );
    assert.ok(summary.indexOf('VALIDATION ISSUE') < summary.indexOf('| Case |'));
    assert.equal(run.validationIssue, report.validationIssue);
    assert.equal(samples.validationIssue, report.validationIssue);
    assert.equal(
      fs.readdirSync(outDir).some((name) => name.endsWith('.tmp')),
      false
    );
  } finally {
    fs.rmSync(outDir, { recursive: true, force: true });
  }
});

test('Markdown report exposes startup, throughput, resources, backlog, memory, and sharing', async () => {
  const outDir = fs.mkdtempSync(path.join(os.tmpdir(), 'whitepawn-telemetry-'));
  const MiB = 1024 ** 2;
  try {
    const report: RunReport = {
      suite: 'whitepawn',
      startedAt: '2026-10-11T00:00:00.000Z',
      profile: 'smoke',
      config: { pollMs: 10 },
      env: {},
      cases: [
        {
          name: 'compact',
          rows: 100_000,
          status: 'running',
          seed: {
            games: 100_000,
            players: 256,
            collections: 20,
            mainCount: 80_000,
            serializedBytes: 50 * MiB,
          },
          seedMs: 4321,
          databaseCounts: [[{ count: 100_000 }], [{ count: 100_276 }]],
          startup: {
            databaseReadyMs: 111,
            schedulerReadyMs: 222,
            sspReadyMs: 333,
            backendReadyMs: 444,
          },
          observedResources: {
            diskBytes: 10 * MiB,
            hostPeakBytes: 20 * MiB,
            dockerPeakBytes: 30 * MiB,
            basis: 'observed',
          },
          processSamples: [
            {
              entity: 'scheduler',
              samples: [
                { t: 1, rssBytes: MiB, cpuPercent: 10 },
                { t: 2, rssBytes: 2 * MiB, cpuPercent: 30 },
              ],
            },
          ],
          databaseSamples: [{ t: 1, memoryBytes: 30 * MiB, cpuPercent: 7 }],
          schedulerSamples: [
            {
              t: 1,
              metrics: {
                scheduler: { pending_events: 7, lag: 8, total_queries: 20, ready_ssps: 1 },
              },
              info: [
                {
                  entity: 'ssp',
                  publication: {
                    pending_batches: 2,
                    pending_operations: 3,
                    pending_bytes: MiB,
                    oldest_age_ms: 456,
                    parked_batches: 4,
                    overload_total: 5,
                  },
                },
              ],
            },
          ],
          memoryMeasurements: [
            {
              label: 'bootstrapped',
              value: {
                store_bytes: 40 * MiB,
                query_bytes: 5 * MiB,
                total_bytes: 45 * MiB,
                graphs: 2,
                subscribers: 3,
                merging: true,
              },
            },
            {
              label: 'after-import',
              value: {
                store_bytes: 40 * MiB,
                query_bytes: 6 * MiB,
                total_bytes: 46 * MiB,
                graphs: 19,
                subscribers: 0,
                merging: true,
              },
            },
          ],
          phases: {
            scroll: {
              samples: [],
              retention: {
                snapshot: {
                  queryIds: Array.from({ length: 20 }, (_, i) => `_00_query:secret${i}`),
                  overflow: false,
                },
                polls: 2,
                elapsedMs: 12.5,
              },
            },
            'final-verification': {
              samples: [],
              retention: {
                snapshot: { queryIds: ['_00_query:retained'], overflow: false },
                polls: 1,
                elapsedMs: 5,
              },
            },
            steady: {
              samples: [{ name: 'batch', writeAckMs: 2 }],
              requestedRowsPerSec: 50,
              achievedRowsPerSec: 49.5,
              completedRows: 100,
              failedRows: 0,
              elapsedMs: 2020,
              catchupAfterOracleMs: 12,
              sourceOracleMs: 123,
            },
            mutations: {
              samples: [
                { name: 'joined-name', publishedReadyMs: 15 },
                { name: 'joined-name', failed: true, publishedReadyMs: 0 },
              ],
            },
            'filtered-and-shared': {
              samples: [],
              retention: {
                snapshot: { queryIds: ['_00_query:retained'], overflow: false },
                polls: 1,
                elapsedMs: 6,
              },
              sharing: {
                graphsBefore: 17,
                graphsAfter: 17,
                subscribersBefore: 17,
                subscribersAfter: 18,
              },
            },
          },
        },
      ],
    };
    await writeWhitePawnReport(outDir, report);
    const summary = fs.readFileSync(path.join(outDir, 'summary.md'), 'utf8');
    assert.match(
      summary,
      /100,000 \| 256 \| 20 \| 80,000 \| 50.00 MiB \| 4321.00 \| 111.00 \| 222.00 \| 333.00 \| 444.00/
    );
    assert.match(
      summary,
      /compact \| steady \| 50.00 \| 49.50 \| 100 \| 0 \| 2020.00 \| 12.00 \| 123.00/
    );
    assert.match(summary, /compact \| 100,000 \| 100,276/);
    assert.match(summary, /metadata is counted separately from source games/);
    assert.match(summary, /compact \| scheduler \| 2 \| 2.00 MiB \| 20.00 \| 30.00/);
    assert.match(summary, /compact \| observed \| 10.00 MiB \| 20.00 MiB \| 30.00 MiB \| observed/);
    assert.match(summary, /compact \| 1 \| 7 \| 7 \| 8 \| 8 \| 20 \| 1 \| 0/);
    assert.match(summary, /compact \| 1 \| 2 \| 3 \| 1.00 MiB \| 456.00 \| 4 \| 5/);
    assert.match(
      summary,
      /compact \| bootstrapped \| 40.00 MiB \| 5.00 MiB \| 45.00 MiB \| 2 \| 3 \| true/
    );
    assert.match(summary, /compact \| 17 \| 17 \| 17 \| 18 \| 19 \| 0/);
    assert.match(
      summary,
      /Initial graph sharing passed; merged followers decreased after import from 18 to 0/
    );
    assert.match(summary, /Later memory measurements include the observed 19 graphs/);
    assert.match(summary, /they do not identify its cause/);
    assert.match(summary, /mutations \/ joined-name \| publishedReadyMs \| 1 \| 1 \| 0 \| 15.00/);
    assert.match(summary, /every 10.00 ms plus database request time/);
    assert.match(summary, /does not measure sync propagation/);
    assert.match(summary, /source oracle time is reported separately/);
    assert.match(summary, /Merged followers before \| Merged followers after/);
    assert.match(summary, /excludes graph owners/);
    assert.match(summary, /does not assert that scheduler or publication queues are empty/);
    assert.match(summary, /compact \| scroll \| 20 \| 20 \| false \| 2 \| 12.50/);
    assert.match(summary, /compact \| filtered-and-shared \| 1 \| 20 \| false \| 1 \| 6.00/);
    assert.match(summary, /compact \| final-verification \| 1 \| 20 \| false \| 1 \| 5.00/);
    assert.match(summary, /exact metadata ID set against retained driver registrations/);
    assert.doesNotMatch(summary, /_00_query:secret/);
    assert.match(summary, /Browser rendering and client download time are outside/);
    const measurements = report.cases[0]!.memoryMeasurements as Array<{
      label: string;
      value: { subscribers: number };
    }>;
    measurements[1]!.value.subscribers = 18;
    await writeWhitePawnReport(outDir, report);
    assert.doesNotMatch(
      fs.readFileSync(path.join(outDir, 'summary.md'), 'utf8'),
      /followers decreased after import/
    );
    measurements.pop();
    await writeWhitePawnReport(outDir, report);
    const withoutAfterImport = fs.readFileSync(path.join(outDir, 'summary.md'), 'utf8');
    assert.doesNotMatch(withoutAfterImport, /followers decreased after import/);
    assert.match(withoutAfterImport, /compact \| 17 \| 17 \| 17 \| 18 \| — \| —/);
  } finally {
    fs.rmSync(outDir, { recursive: true, force: true });
  }
});

function database(query: (...args: unknown[]) => unknown): Pick<Surreal, 'query'> {
  return { query } as unknown as Pick<Surreal, 'query'>;
}

test('retained query metadata uses a bounded full-table read and normalizes typed SDK ids', async () => {
  const db = database(async (sql, bindings) => {
    assert.equal(String(sql), 'SELECT VALUE id FROM _00_query LIMIT $limit TIMEOUT 5s;');
    assert.deepEqual(bindings, { limit: 21 });
    return [[new RecordId('_00_query', 'a'), '_00_query:b']];
  });
  const snapshot = await readRetainedQueries(db);
  assert.deepEqual(snapshot, { queryIds: ['_00_query:a', '_00_query:b'], overflow: false });
  assert.deepEqual(retainedQueriesMatch(snapshot, [new RecordId('_00_query', 'b'), 'a']), {
    matches: true,
    reasons: [],
  });
});

test('retention requires exact membership and rejects the 21st metadata sentinel', async () => {
  assert.equal(MAX_RETAINED_QUERIES, 20);
  const ids = Array.from({ length: 20 }, (_, i) => `_00_query:q${i}`);
  assert.equal(retainedQueriesMatch({ queryIds: ids, overflow: false }, ids).matches, true);
  const wrong = { queryIds: [...ids.slice(1), '_00_query:ghost'], overflow: false };
  assert.match(retainedQueriesMatch(wrong, ids).reasons.join(';'), /differs from the driver/);
  const db = database(async () => [[...ids, new RecordId('_00_query', 'ghost')]]);
  const overflow = await readRetainedQueries(db);
  assert.equal(overflow.overflow, true);
  assert.match(retainedQueriesMatch(overflow, ids).reasons.join(';'), /21-row overflow sentinel/);
  assert.throws(
    () => retainedQueriesMatch({ queryIds: [], overflow: false }, [...ids, 'q20']),
    /20-query bound/
  );
  assert.throws(
    () => retainedQueriesMatch({ queryIds: [], overflow: false }, ['a', '_00_query:a']),
    /duplicates/
  );
  assert.throws(
    () => retainedQueriesMatch({ queryIds: [], overflow: false }, [new RecordId('game', 'a')]),
    /Invalid retained query record id/
  );
});

test('retention polling waits for exact durable eviction and bounds hanging queries', async () => {
  let polls = 0;
  const db = database(async () => {
    polls++;
    return [polls === 1 ? ['_00_query:retained', '_00_query:evicted'] : ['_00_query:retained']];
  });
  const observed = await waitRetainedQueries(db, ['retained'], { timeoutMs: 500, pollMs: 1 });
  assert.equal(observed.polls, 2);
  assert.deepEqual(observed.snapshot.queryIds, ['_00_query:retained']);
  const hanging = database(() => new Promise(() => {}));
  const start = performance.now();
  await assert.rejects(
    waitRetainedQueries(hanging, [], { timeoutMs: 15, pollMs: 1 }),
    /Retained query metadata timed out/
  );
  assert.ok(performance.now() - start < 500);
  const controller = new AbortController();
  const waiting = waitRetainedQueries(hanging, [], {
    timeoutMs: 1000,
    pollMs: 1,
    signal: controller.signal,
  });
  controller.abort(new Error('Stopped retention check'));
  await assert.rejects(waiting, /Stopped retention check/);
  const alreadyStopped = new AbortController();
  alreadyStopped.abort(new Error('Already stopped'));
  await assert.rejects(
    readRetainedQueries(
      database(() => assert.fail('Query after cancellation')),
      {
        signal: alreadyStopped.signal,
      }
    ),
    /Already stopped/
  );
});

test('source snapshot normalizes SDK ids and uses one bounded version lookup', async () => {
  const calls: Array<{ sql: string; bindings: Record<string, unknown> | undefined }> = [];
  const db = database(async (sql, bindings) => {
    calls.push({ sql: String(sql), bindings: bindings as Record<string, unknown> | undefined });
    if (calls.length === 1)
      return [
        [
          {
            id: new RecordId('game', 'a'),
            white: { id: new RecordId('player_name', 'white'), name: 'White' },
            black: { id: new RecordId('player_name', 'black') },
          },
          {
            id: new RecordId('game', 'b'),
            white: { id: new RecordId('player_name', 'white') },
            black: { id: new RecordId('player_name', 'black') },
          },
        ],
      ];
    return [
      [
        ...Object.entries(expected.versions).map(([id, version]) => {
          const [table, key] = id.split(':');
          return { record_id: new RecordId(table!, key!), version };
        }),
      ],
    ];
  });
  const snapshot = await snapshotExpected(db, 'SELECT * FROM game LIMIT 50;');
  assert.deepEqual(snapshot.rootIds, expected.rootIds);
  assert.deepEqual(snapshot.childIds, expected.childIds);
  assert.deepEqual(snapshot.versions, expected.versions);
  assert.deepEqual(snapshot.childLinks, expected.childLinks);
  assert.equal(snapshot.rows?.[0]?.id, 'game:a');
  assert.equal(calls.length, 2);
  assert.match(calls[1]!.sql, /record_id IN \$records LIMIT \$limit/);
  assert.equal(calls[1]!.bindings?.limit, 5);
  assert.ok((calls[1]!.bindings?.records as unknown[]).every((id) => id instanceof RecordId));
});

test('source snapshot preserves bound query text and typed database record bindings', async () => {
  const surql = 'SELECT id FROM game WHERE database = $database LIMIT 50 START 0;';
  const bindings = { database: new RecordId('game_database', 'main') };
  const calls: Array<{ sql: unknown; bindings: unknown }> = [];
  const db = database(async (sql, actualBindings) => {
    calls.push({ sql, bindings: actualBindings });
    if (calls.length === 1) return [[{ id: new RecordId('game', 'a') }]];
    return [[{ record_id: new RecordId('game', 'a'), version: 2 }]];
  });
  const snapshot = await snapshotExpected(db, surql, bindings);
  assert.deepEqual(snapshot.rootIds, ['game:a']);
  assert.equal(snapshot.versions['game:a'], 2);
  assert.equal(calls[0]!.sql, surql);
  assert.equal(calls[0]!.bindings, bindings);
  assert.ok((calls[0]!.bindings as typeof bindings).database instanceof RecordId);
  assert.equal((calls[0]!.bindings as typeof bindings).database.toString(), 'game_database:main');
  assert.notEqual(calls[1]!.bindings, bindings);
  assert.equal('database' in (calls[1]!.bindings as Record<string, unknown>), false);
});

test('future expectations can be prepared without source reads using overridden versions', async () => {
  const db = database(() => {
    throw new Error('Unexpected query inside timing preparation');
  });
  const snapshot = await expectedFromRows(
    db,
    [{ id: 'game:future', white: { id: 'player_name:white' }, black: { id: 'player_name:white' } }],
    { 'game:future': 1, 'player_name:white': 6 }
  );
  assert.deepEqual(snapshot.childIds, ['player_name:white']);
  assert.equal(snapshot.versions['game:future'], 1);
});

test('publication reads use typed query ids, graph adjacency, and overflow sentinel', async () => {
  const db = database(async (sql, bindings) => {
    assert.match(String(sql), /FROM \$query->_00_list_ref_user_bench LIMIT \$limit/);
    assert.ok((bindings as Record<string, unknown>).query instanceof RecordId);
    assert.equal(((bindings as Record<string, unknown>).query as RecordId).id, 'sample');
    assert.equal((bindings as Record<string, unknown>).limit, 151);
    return [
      [{ state: 'ready', rowCount: 1 }],
      [
        {
          id: new RecordId('_00_list_ref_user_bench', 'r1'),
          out: new RecordId('game', 'a'),
          version: 2,
        },
      ],
    ];
  });
  const result = await readPublication(db, 'bench:window:sample');
  assert.equal(result.edges[0]!.out, 'game:a');
  assert.equal(result.overflow, false);
});

test('publication waiting respects abort and deadlines even if the database hangs', async () => {
  const hanging = database(() => new Promise(() => {}));
  const controller = new AbortController();
  const waiting = waitPublication(hanging, 'q', expected, {
    timeoutMs: 1000,
    pollMs: 1,
    signal: controller.signal,
  });
  controller.abort(new Error('Stopped by user'));
  await assert.rejects(waiting, /Stopped by user/);
  const start = performance.now();
  await assert.rejects(
    waitPublication(hanging, 'q', expected, { timeoutMs: 15, pollMs: 1 }),
    /Publication timed out/
  );
  assert.ok(performance.now() - start < 500);
});
