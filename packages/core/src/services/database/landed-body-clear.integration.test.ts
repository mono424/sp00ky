import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import { RecordId, Surreal } from 'surrealdb';
import { landChunk } from '../../query/fetch.saga';
import { defaultEnv } from '../../query/env';
import { runPure } from '../../testing/run-pure';
import { buildState } from '../../testing/build';
import type { Vars } from '../../kernel/effects';
import type { SealedQuery } from '../../utils/surql';
import { SqliteCacheEngine } from './sqlite-cache-engine';
import { stubTransport } from './sqlite-transport.fixture';
import type { Row } from './cache-engine';

/**
 * Integration: a field cleared on the server (`UPDATE broadcast:x UNSET club`)
 * is ABSENT from the body the server returns, and the body is landed with
 * MERGE, which keeps every key it does not name. These land real bodies
 * through `landChunk` and run the exact transaction it emits on BOTH local
 * engines: a real in-memory SQLite (the worker's @sqlite.org/sqlite-wasm
 * build) behind the SqliteCacheEngine, and a real SurrealDB-WASM `mem://`
 * store provisioned with a typed schema, as the local migrator does.
 *
 * On each: the cleared optional field is gone, local `_00_*` bookkeeping
 * survives (still a MERGE, not a REPLACE), and a required field the body
 * lacks is kept rather than cleared.
 */

const env = defaultEnv({
  tables: [
    {
      name: 'broadcast',
      columns: {
        title: { type: 'string', optional: false },
        secret: { type: 'string', optional: false },
        club: { type: 'string', recordId: true, optional: true },
        note: { type: 'string', optional: true },
      },
    },
  ],
} as any);

type Exec = (query: SealedQuery<unknown>, vars: Vars) => Promise<unknown>;

/** Land `rows` at `version` through the real saga, executing its write with `exec`. */
async function land(
  exec: Exec,
  rows: Array<Record<string, unknown>>,
  version: number
): Promise<void> {
  const ids = rows.map((r) => String(r.id));
  const out = await runPure(
    landChunk(env, ids, rows, new Map(ids.map((id) => [id, version] as const)), buildState(), 0),
    {
      handlers: {
        'local.execute': (e: any) => exec(e.query, e.vars),
        'ssp.ingest': () => undefined,
      },
    }
  );
  expect(out.emitted).not.toContainEqual(expect.objectContaining({ message: 'body write failed' }));
  expect(out.result).toBe(true);
}

const x = new RecordId('broadcast', 'x');
const fresh = new RecordId('broadcast', 'fresh');
const club = new RecordId('club', 'c1');

const noop = () => {};

// ==================== SQLite ====================

describe('landed body clears an absent optional field: SQLite engine', () => {
  let db: any;

  function realEngine(): SqliteCacheEngine {
    const logger: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
    logger.child = () => logger;
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, logger);
    stubTransport(engine, (type, payload: any) => {
      switch (type) {
        case 'open':
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
          for (const stmt of payload as { sql: string; bind?: unknown[] }[])
            db.exec({ sql: stmt.sql, bind: stmt.bind });
          return {};
        default:
          return {};
      }
    });
    return engine;
  }

  beforeAll(async () => {
    const sqlite3: any = await sqlite3InitModule();
    db = new sqlite3.oo1.DB(':memory:', 'c');
  });

  afterAll(() => db?.close());

  it('removes the cleared key on the batched fast path, keeps _00_* and required fields', async () => {
    const engine = realEngine();
    await engine.connect('anon');
    const exec: Exec = (q, v) => engine.execute(q, v);

    await land(exec, [{ id: x, title: 't1', secret: 's', club, note: 'n' }], 1);
    await engine.upsert('broadcast', x, { _00_local: 'keep' }, 'merge');
    const before = (await engine.getById('broadcast', x)) as Row;
    expect(before.club).toBeDefined();

    // The server unset `club`; `secret` is hidden from this reader (required,
    // so its absence is not a clear).
    await land(exec, [{ id: x, title: 't2', note: 'n' }], 2);
    const after = (await engine.getById('broadcast', x)) as Row;
    expect('club' in after).toBe(false);
    expect(after).toMatchObject({
      id: 'broadcast:x',
      title: 't2',
      note: 'n',
      secret: 's',
      _00_local: 'keep',
      _00_rv: 2,
    });

    // A fresh row stores no placeholder for the cleared key.
    await land(exec, [{ id: fresh, title: 'f', secret: 's' }], 1);
    const inserted = (await engine.getById('broadcast', fresh)) as Row;
    expect(Object.keys(inserted).toSorted()).toEqual(['_00_rv', 'id', 'secret', 'title']);
  });

  it('removes the cleared key on the single-statement merge path too', async () => {
    const engine = realEngine();
    await engine.connect('anon');

    const y = new RecordId('broadcast', 'y');
    await engine.query('UPSERT ONLY $id MERGE $c', {
      id: y,
      c: { club, note: 'n2', _00_local: 'keep' },
    });
    expect((await engine.getById('broadcast', y))?.club).toBeDefined();
    await engine.query('UPSERT ONLY $id MERGE $c', { id: y, c: { club: undefined, note: 'n3' } });
    const row = (await engine.getById('broadcast', y)) as Row;
    expect('club' in row).toBe(false);
    expect(row.note).toBe('n3');
    expect(row._00_local).toBe('keep');
  });
});

// ==================== SurrealDB-WASM ====================

describe('landed body clears an absent optional field: SurrealDB-WASM engine', () => {
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
      DEFINE TABLE club SCHEMAFULL;
      DEFINE TABLE broadcast SCHEMAFULL;
      DEFINE FIELD title ON broadcast TYPE string;
      DEFINE FIELD secret ON broadcast TYPE string;
      DEFINE FIELD club ON broadcast TYPE option<record<club>>;
      DEFINE FIELD note ON broadcast TYPE option<string>;
      DEFINE FIELD _00_rv ON broadcast TYPE option<int>;
      DEFINE FIELD _00_local ON broadcast TYPE option<string>;
    `);
  }, 60_000);

  afterAll(async () => {
    await db?.close();
    vi.unstubAllGlobals();
  });

  const exec: Exec = (q, v) => db.query(q.sql, v) as unknown as Promise<unknown>;
  const read = async (id: RecordId<string>): Promise<Record<string, unknown>> => {
    const [row] = (await db.query('SELECT * FROM ONLY $id', { id })) as [Record<string, unknown>];
    return row;
  };

  it('the undefined clear reaches SurrealDB as NONE and the MERGE removes the field', async () => {
    await land(exec, [{ id: x, title: 't1', secret: 's', club, note: 'n' }], 1);
    await db.query("UPDATE $id SET _00_local = 'keep'", { id: x });
    expect(String((await read(x)).club)).toBe('club:c1');

    // Would fail the whole transaction if the clear were NULL (a coercion error
    // on `option<record<club>>`) or named the required `secret` as NONE.
    await land(exec, [{ id: x, title: 't2', note: 'n' }], 2);
    const after = await read(x);
    expect('club' in after).toBe(false);
    expect(after).toMatchObject({
      title: 't2',
      note: 'n',
      secret: 's',
      _00_local: 'keep',
      _00_rv: 2,
    });

    await land(exec, [{ id: fresh, title: 'f', secret: 's' }], 1);
    expect(Object.keys(await read(fresh)).toSorted()).toEqual(['_00_rv', 'id', 'secret', 'title']);
  });

  it('why the clear is NONE on optional columns only: NULL and a required NONE both fail', async () => {
    const z = new RecordId('broadcast', 'z');
    await db.query('CREATE $id CONTENT $c', { id: z, c: { title: 't', secret: 's', club } });
    await expect(
      db.query('UPSERT ONLY $id MERGE $c', { id: z, c: { club: null } })
    ).rejects.toThrow(/club/);
    await expect(
      db.query('UPSERT ONLY $id MERGE $c', { id: z, c: { secret: undefined } })
    ).rejects.toThrow(/secret/);
  });
});
