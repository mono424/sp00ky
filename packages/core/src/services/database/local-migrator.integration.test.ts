import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { RecordId, Surreal } from 'surrealdb';
import { LocalMigrator } from './local-migrator';
import * as rows from '../../mutation/rows';

/**
 * Integration: the client schema the CLI emits, provisioned by the real
 * LocalMigrator into a real SurrealDB-WASM store, then every local write the
 * outbox makes. The meta tables are read from the CLI's source file, so this
 * cannot drift from what `spky generate` ships; the per-table `_00_rv` field
 * is appended the way the codegen does it.
 *
 * `_00_pending_mutations` used to be SCHEMAFULL there, without the fields the
 * client writes (`tableName`, `createdAt`, `v`, `beforeRecord`): SurrealDB
 * failed every create, update and delete transaction on this engine.
 */

const META = new URL('../../../../../apps/cli/src/meta_tables_client.surql', import.meta.url);

async function clientSchema(userSchema: string): Promise<string> {
  const schema = `${userSchema}\n${await readFile(fileURLToPath(META), 'utf8')}`;
  const tables = [...schema.matchAll(/DEFINE TABLE (?:IF NOT EXISTS )?(\w+)/g)].map((m) => m[1]);
  return schema + tables.map((t) => `\nDEFINE FIELD _00_rv ON TABLE ${t} TYPE int DEFAULT 0 PERMISSIONS FOR select, create, update WHERE true;`).join('');
}

const DOC_V1 = `DEFINE TABLE doc SCHEMAFULL PERMISSIONS FOR select, create, update, delete WHERE true;
DEFINE FIELD title ON TABLE doc TYPE option<string>;`;
const DOC_V2 = `${DOC_V1}
DEFINE FIELD body ON TABLE doc TYPE option<string>;`;

const doc = new RecordId('doc', 'd1');
const mid = (n: string) => new RecordId('_00_pending_mutations', n);
const noop = () => {};

describe('client schema on the SurrealDB-WASM engine', () => {
  let db: Surreal;
  let migrator: LocalMigrator;

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
    const logger: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
    logger.child = () => logger;
    const store: any = { queryUngated: (sql: string, vars?: Record<string, unknown>) => db.query(sql, vars), getConfig: () => ({ database: 'd' }) };
    migrator = new LocalMigrator(store, logger);
    await migrator.provision(await clientSchema(DOC_V1));
  }, 60_000);

  afterAll(async () => {
    await db?.close();
    vi.unstubAllGlobals();
  });

  const exec = (tx: rows.LocalTx) => db.query(tx.query.sql, tx.vars);
  const outbox = async () => ((await db.query(rows.loadPendingRows())) as unknown[][])[0].map(rows.parsePendingRow);

  it('every outbox write commits', async () => {
    await exec(rows.planCreateTx({ recordId: doc, mutationId: mid('1'), table: 'doc', data: { title: 'a' }, now: 1 }));
    await exec(rows.planUpdateTx({ recordId: doc, mutationId: mid('2'), table: 'doc', data: { title: 'b' }, before: { id: doc, title: 'a' }, now: 2 }));
    const wid = new RecordId('_00_pending_writes', 'w1');
    await exec(rows.planLocalOnlyUpdateTx({ recordId: doc, data: { title: 'c' }, mirror: { id: wid, row: { recordId: doc, tableName: 'doc', data: { title: 'c' }, beforeRecord: null, flushBy: 9 } } }));
    await exec(rows.planDeferredOutboxRowTx({ recordId: doc, mutationId: mid('3'), table: 'doc', data: { title: 'c' }, before: null, now: 3, mirrorId: wid }));
    await exec(rows.planDeleteTx({ recordId: doc, mutationId: mid('4'), table: 'doc', before: { id: doc, title: 'c' }, now: 4 }));

    const pending = await outbox();
    expect(pending.map((r) => [r?.mutationType, r?.tableName, r?.v])).toEqual([
      ['create', 'doc', 2],
      ['update', 'doc', 2],
      ['update', 'doc', 2],
      ['delete', 'doc', 2],
    ]);
    expect(pending[1]?.beforeRecord).toEqual({ id: doc, title: 'a' });

    // A rejected one moves to the tray; an accepted one is deleted.
    await exec(rows.moveToFailedTx(rows.buildFailedRow(pending[1]!, { message: 'denied', kind: 'application' }, pending[1]!.beforeRecord ?? null, 1, 5, 'full')));
    const del = rows.deletePendingRow(pending[0]!.id);
    await db.query(del.sql, del.vars);
    expect((await outbox()).map((r) => r?.mutationType)).toEqual(['update', 'delete']);
    const [tray] = (await db.query(rows.loadFailedRows())) as [Array<Record<string, unknown>>];
    expect(tray).toEqual([expect.objectContaining({ mutationType: 'update', tableName: 'doc', error: { message: 'denied', kind: 'application' } })]);
  });

  it('a schema change keeps the outbox, the tray and pending writes, and drops the cache', async () => {
    await exec(rows.planLocalOnlyUpdateTx({ recordId: doc, data: { title: 'd' }, mirror: { id: new RecordId('_00_pending_writes', 'w2'), row: { recordId: doc, tableName: 'doc', data: { title: 'd' }, beforeRecord: null, flushBy: 9 } } }));
    const ids = async () =>
      ((await db.query('SELECT VALUE id FROM _00_pending_mutations; SELECT VALUE id FROM _00_failed_mutations; SELECT VALUE id FROM _00_pending_writes')) as RecordId[][]).map((r) =>
        r.map(String).sort()
      );
    const before = await ids();
    expect(before.map((r) => r.length)).toEqual([2, 1, 1]);

    await migrator.provision(await clientSchema(DOC_V2));
    expect(await ids()).toEqual(before);
    expect(((await db.query('SELECT * FROM doc')) as unknown[][])[0]).toEqual([]);
    await exec(rows.planCreateTx({ recordId: new RecordId('doc', 'd2'), mutationId: mid('5'), table: 'doc', data: { title: 'e', body: 'x' }, now: 6 }));
    expect(await outbox()).toHaveLength(3);
  });
});
