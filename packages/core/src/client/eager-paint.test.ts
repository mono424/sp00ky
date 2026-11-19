import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Sp00kyClient } from './sp00ky-client';
import { RowsCacheEngine } from '../services/database/rows-cache-engine';
import { inProcessRows, type RowsDisks } from '../services/database/rows-transport.fixture';
import { paintKeyOf } from '../query/materialize';

/**
 * The whole way from one page load to the next, with the real client and the
 * real row store engine: a query renders, the engine keeps what it read, and
 * the next page's client, built and not yet initialized, hands those rows to
 * a component synchronously.
 */
describe('painting a query before init (row store)', () => {
  const g = globalThis as Record<string, unknown>;
  let stored: Map<string, string>;
  const schema = { tables: [{ name: 'thing', columns: { n: {} } }] } as any;
  const config = {
    database: { namespace: 'n', database: 'd', store: 'indexeddb' },
    localEngine: 'rows',
    schema,
    schemaSurql: '',
    logLevel: 'silent',
  } as any;
  const logger = { info() {}, warn() {}, error() {}, debug() {}, trace() {}, child: () => logger } as any;

  beforeEach(() => {
    stored = new Map();
    g.localStorage = {
      getItem: (k: string) => stored.get(k) ?? null,
      setItem: (k: string, v: string) => void stored.set(k, v),
      removeItem: (k: string) => void stored.delete(k),
      key: (i: number) => [...stored.keys()][i] ?? null,
      get length() {
        return stored.size;
      },
    };
  });

  afterEach(() => {
    delete g.localStorage;
  });

  /** The first page load: the engine as the client drives it for one query's membership render. */
  async function lastSession(bucket: string, surql: string): Promise<Record<string, unknown>[]> {
    const disks: RowsDisks = new Map();
    const engine = new RowsCacheEngine(config.database, logger, { useOpfs: true });
    inProcessRows(engine, disks);
    await engine.connect(bucket);
    await engine.upsert('thing', 'thing:a', { n: 1 }, 'replace');
    await engine.upsert('thing', 'thing:b', { n: 2 }, 'replace');
    // What `materialize` does: the render set by id, named after the query.
    const rows = await engine.select({ table: 'thing' }, {}, { paintKey: paintKeyOf({ surql, params: {} }, []) });
    (engine as unknown as { paint: { flush(): void } }).paint.flush();
    await engine.close();
    return rows;
  }

  it('hands a component the rows the query showed last time, with the client not opened', async () => {
    stored.set('sp00ky:last_bucket', 'user_a');
    const probe = new Sp00kyClient<any>(config);
    const surql = probe.query('thing', {}).build().innerQuery.selectQuery.query;
    const rows = await lastSession('user_a', surql);
    expect(rows.map((r) => r.id)).toEqual(['thing:a', 'thing:b']);

    // The next page load: a client that has been built and nothing more.
    const client = new Sp00kyClient<any>(config);
    expect(client.isLocalReady()).toBe(false);
    expect(client.peekQuery(client.query('thing', {}).build())).toEqual(rows);
    // Another query has no copy.
    expect(client.peekQuery(client.query('thing', {}).where({ n: 1 }).build())).toBeNull();
  });

  it('paints nothing from another user\'s bucket', async () => {
    stored.set('sp00ky:last_bucket', 'user_a');
    const surql = new Sp00kyClient<any>(config).query('thing', {}).build().innerQuery.selectQuery.query;
    await lastSession('user_a', surql);
    // The browser was last used by someone else: boot will open their bucket.
    stored.set('sp00ky:last_bucket', 'user_b');
    const client = new Sp00kyClient<any>(config);
    expect(client.peekQuery(client.query('thing', {}).build())).toBeNull();
  });
});
