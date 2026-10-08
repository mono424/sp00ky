import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import { RecordId, Surreal } from 'surrealdb';
import { defaultEnv } from '../query/env';
import { runPure } from '../testing/run-pure';
import { buildState } from '../testing/build';
import type { ClientState } from '../state/client-state';
import type { SealedQuery } from '../utils/surql';
import type { Vars } from '../kernel/effects';
import { SqliteCacheEngine } from '../services/database/sqlite-cache-engine';
import { stubTransport } from '../services/database/sqlite-transport.fixture';
import { PENDING_WRITE_ORPHAN_MS } from '../kernel/constants';
import { loadPendingRows, parsePendingRow } from './rows';
import { mintMutationId } from './mutation-id';
import { adoptPendingWrites, flushWrite, write } from './write.saga';

/**
 * Integration: a debounced update must survive the page going away inside its
 * delay. These run the real sagas and execute their transactions on BOTH local
 * engines: a real in-memory SQLite behind the SqliteCacheEngine, and a real
 * SurrealDB-WASM `mem://` store whose `_00_pending_writes` table was never
 * defined (a store provisioned before the table existed).
 *
 * On each: the patch is mirrored to `_00_pending_writes` beside the row; a
 * timely flush moves it into the outbox; and when the page died first, a fresh
 * page (empty memory) adopts the mirror into a replayable outbox row.
 */

const env = defaultEnv({ tables: [{ name: 'doc', columns: { title: { type: 'string', optional: true } } }] } as any);
const doc = new RecordId('doc', 'd1');

interface Engine {
  query(sql: string, vars?: Vars): Promise<unknown>;
  execute(query: SealedQuery<unknown>, vars: Vars): Promise<unknown>;
}

const run = <R>(engine: Engine, saga: Generator<any, R, any>, state: ClientState, now: number) =>
  runPure(saga, {
    state,
    now,
    handlers: {
      'local.query': (e: any) => engine.query(e.sql, e.vars),
      'local.execute': (e: any) => engine.execute(e.query, e.vars),
      'ssp.ingest': () => undefined,
      id: () => mintMutationId(state.tabId),
    },
  });

async function scenario(engine: Engine, readRow: () => Promise<Record<string, unknown> | null>): Promise<void> {
  const typed = async (title: string, state: ClientState, now: number) =>
    run(engine, write(env, { kind: 'update', recordId: 'doc:d1', data: { title }, options: { debounced: { delay: 1_000 } } }), state, now);
  const mirrors = async () => ((await engine.query('SELECT * FROM _00_pending_writes')) as unknown[][])[0] ?? [];
  const outbox = async () => (((await engine.query(loadPendingRows())) as unknown[][])[0] ?? []).map(parsePendingRow);

  // Booting with nothing to adopt is a no-op, even before the table exists.
  const boot = await run(engine, adoptPendingWrites(), buildState(), 0);
  expect(boot.dispatched).toEqual([]);

  // Typing inside the delay: the row changes at once, the patch is mirrored beside it.
  const a = await typed('He', buildState(), 1_000);
  const b = await typed('Hello', a.state, 1_100);
  expect((await readRow())?.title).toBe('Hello');
  expect(await mirrors()).toHaveLength(1);
  expect(await outbox()).toEqual([]);

  // The timer fires: the patch moves into the outbox, the mirror is gone.
  const flushed = await run(engine, flushWrite(env, 'doc:d1::title'), b.state, 2_100);
  expect(await mirrors()).toEqual([]);
  expect(await outbox()).toEqual([expect.objectContaining({ mutationType: 'update', recordId: 'doc:d1', data: { title: 'Hello' } })]);
  await engine.query('DELETE _00_pending_mutations');

  // Typing again, then the page dies before the flush: memory is gone, the mirror is not.
  await typed('Hello world', flushed.state, 3_000);
  const reloaded = { ...buildState(), tabId: 'tab-after-reload' };
  const early = await run(engine, adoptPendingWrites(), reloaded, 3_100);
  expect(early.timers.get('adopt-writes')).toEqual({ ms: 4_000 + PENDING_WRITE_ORPHAN_MS - 3_100, event: { type: 'AdoptPendingWrites' } });
  expect(await outbox()).toEqual([]);

  const adopted = await run(engine, adoptPendingWrites(), reloaded, 4_000 + PENDING_WRITE_ORPHAN_MS);
  expect(adopted.dispatched).toEqual([{ type: 'Drain' }]);
  expect(await mirrors()).toEqual([]);
  const rows = await outbox();
  expect(rows).toEqual([expect.objectContaining({ mutationType: 'update', recordId: 'doc:d1', data: { title: 'Hello world' } })]);
  expect(adopted.state.outbox.map((i) => i.id)).toEqual([rows[0]!.id]);
  expect((await readRow())?.title).toBe('Hello world');
}

const noop = () => {};

describe('debounced writes survive a dead page: SQLite engine', () => {
  let db: any;

  beforeAll(async () => {
    const sqlite3: any = await sqlite3InitModule();
    db = new sqlite3.oo1.DB(':memory:', 'c');
  });

  afterAll(() => db?.close());

  it('mirrors, flushes, and adopts', async () => {
    const logger: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
    logger.child = () => logger;
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, logger);
    stubTransport(engine, (type, payload: any) => {
      switch (type) {
        case 'open':
          // As the worker does: system tables exist from the open on.
          for (const t of payload.systemTables as string[]) db.exec({ sql: `CREATE TABLE IF NOT EXISTS "${t}" (id TEXT PRIMARY KEY, data TEXT NOT NULL)` });
          return { persisted: true };
        case 'exec':
          return { rows: db.exec({ sql: payload.sql, bind: payload.bind, rowMode: 'object', returnValue: 'resultRows' }) };
        case 'run':
          db.exec({ sql: payload.sql, bind: payload.bind });
          return {};
        case 'batch':
          for (const stmt of payload as { sql: string; bind?: unknown[] }[]) db.exec({ sql: stmt.sql, bind: stmt.bind });
          return {};
        default:
          return {};
      }
    });
    await engine.connect('anon');
    await engine.upsert('doc', doc, { title: '' }, 'replace');
    await scenario(
      { query: (sql, vars) => engine.query(sql, vars), execute: (q, v) => engine.execute(q, v) },
      async () => ((await engine.getById('doc', doc)) as Record<string, unknown>) ?? null
    );
  });
});

describe('debounced writes survive a dead page: SurrealDB-WASM engine', () => {
  let db: Surreal;

  beforeAll(async () => {
    // The wasm engine loads its binary with `fetch(new URL(..., import.meta.url))`,
    // and Node's fetch has no `file:` scheme: serve that one read from disk.
    const realFetch = globalThis.fetch;
    vi.stubGlobal('fetch', async (input: unknown, init?: RequestInit) => {
      const url = input instanceof URL ? input : new URL(String(input));
      if (url.protocol === 'file:') return new Response(await readFile(fileURLToPath(url)));
      return realFetch(input as RequestInfo, init);
    });
    const { createWasmEngines } = await import('@surrealdb/wasm');
    db = new Surreal({ engines: createWasmEngines() });
    await db.connect('mem://');
    await db.use({ namespace: 'n', database: 'd' });
    await db.query(`
      DEFINE TABLE doc SCHEMAFULL;
      DEFINE FIELD title ON doc TYPE option<string>;
      DEFINE FIELD _00_rv ON doc TYPE option<int>;
      DEFINE TABLE _00_pending_mutations SCHEMALESS;
      CREATE doc:d1 SET title = '';
    `);
  }, 60_000);

  afterAll(async () => {
    await db?.close();
    vi.unstubAllGlobals();
  });

  it('mirrors, flushes, and adopts', async () => {
    await scenario(
      {
        query: (sql, vars) => db.query(sql, vars) as unknown as Promise<unknown>,
        execute: async (q, v) => q.extract((await db.query(q.sql, v)) as unknown[]),
      },
      async () => {
        const [row] = (await db.query('SELECT * FROM ONLY $id', { id: doc })) as [Record<string, unknown>];
        return row ?? null;
      }
    );
  });
});
