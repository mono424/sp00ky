import { describe, expect, it, vi } from 'vitest';
import { RecordId, Uuid } from 'surrealdb';
import { QueryBuilder } from '@spooky-sync/query-builder';
import {
  createLiveFeed,
  liveEndpoint,
  liveQueryInput,
  LiveFeedError,
  type LiveSurreal,
} from './index';
import { Sp00kyClient } from '../client/sp00ky-client';
import { Runtime } from '../client/runtime';
import { fakeAdapters } from '../testing/adapters';
import { fakeServiceBundle } from '../testing/fake-services';
import { runPure } from '../testing/run-pure';
import { defaultEnv } from '../query/env';
import { registerLocal, registerRemote } from '../query/register.saga';
import { emptyState } from '../state/client-state';
import { queryKey } from '../pure';
import { queryHashInput } from '../query/hash';
import { sha256Hex } from '../utils/sha256';

const schema = {
  tables: [
    {
      name: 'notification',
      columns: { user: { recordId: true }, read: {}, created_at: { dateTime: true } },
    },
  ],
  relationships: [],
} as any;

function jwt(claims: Record<string, unknown>): string {
  const b64 = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url');
  return `${b64({ alg: 'HS512', typ: 'JWT' })}.${b64(claims)}.sig`;
}
const aliceToken = jwt({
  ID: 'user:alice',
  AC: 'account',
  exp: Math.floor(Date.now() / 1000) + 3600,
});

const n1 = new RecordId('notification', 'n1');
const n2 = new RecordId('notification', 'n2');
const n3 = new RecordId('notification', 'n3');
const body = (rid: RecordId, extra: Record<string, unknown> = {}) => ({
  id: rid,
  user: new RecordId('user', 'alice'),
  read: false,
  title: `t-${rid.id}`,
  ...extra,
});

/** A scripted stand-in for the `surrealdb` SDK client. */
class FakeSurreal {
  calls: Array<{ sql: string; vars?: Record<string, any> }> = [];
  edges: Array<{ out: RecordId; version: number }> = [];
  bodies = new Map<string, Record<string, unknown>>();
  meta: { rowCount: number; state: string } | null = { rowCount: 0, state: 'ready' };
  liveCb: ((m: { action: string; value: unknown }) => void) | null = null;
  killed = false;
  closed = false;
  authError: Error | null = null;
  heartbeatGone = false;
  liveId = new Uuid('018f0000-0000-7000-8000-000000000001');
  connect = vi.fn(async () => true);
  use = vi.fn(async () => true);
  authenticate = vi.fn(async () => {
    if (this.authError) throw this.authError;
    return true;
  });
  close = vi.fn(async () => {
    this.closed = true;
  });

  query(sql: string, vars?: Record<string, any>): Promise<unknown[]> {
    this.calls.push({ sql, vars });
    return Promise.resolve(this.answer(sql, vars));
  }

  answer(sql: string, vars?: Record<string, any>): unknown[] {
    const snapshot = () => [this.edges, this.meta, []];
    if (sql.startsWith('fn::query::register')) return [null, ...snapshot()];
    if (sql.startsWith('SELECT out, version FROM')) return snapshot();
    if (sql === 'SELECT * FROM $ids') {
      const rows = (vars!.ids as RecordId[])
        .map((rid) => this.bodies.get(`${rid.table.name}:${rid.id}`))
        .filter(Boolean);
      return [rows];
    }
    if (sql.startsWith('LIVE SELECT')) return [this.liveId];
    if (sql.startsWith('fn::query::heartbeat')) return [this.heartbeatGone ? [] : [{ id: 'q' }]];
    if (sql.startsWith('fn::query::unsubscribe'))
      return sql.split(';').map(() => ({ released: true }));
    if (sql.startsWith('KILL')) return [null];
    if (sql.startsWith('RETURN')) return ['answer'];
    throw new Error(`unscripted: ${sql}`);
  }

  liveOf(_id: unknown) {
    return Promise.resolve({
      subscribe: (cb: (m: { action: string; value: unknown }) => void) => {
        this.liveCb = cb;
        return () => {
          this.liveCb = null;
        };
      },
      kill: async () => {
        this.killed = true;
      },
    });
  }

  events = new Map<string, Array<() => void>>();
  subscribe(event: string, cb: () => void): () => void {
    this.events.set(event, [...(this.events.get(event) ?? []), cb]);
    return () =>
      this.events.set(
        event,
        (this.events.get(event) ?? []).filter((x) => x !== cb)
      );
  }
  fire(event: string): void {
    for (const cb of this.events.get(event) ?? []) cb();
  }

  emit(action: string, value: unknown): void {
    this.liveCb?.({ action, value });
  }

  put(
    rid: RecordId,
    version: number,
    extra: Record<string, unknown> = {}
  ): Record<string, unknown> {
    const row = body(rid, extra);
    this.bodies.set(`${rid.table.name}:${rid.id}`, row);
    this.edges = [
      ...this.edges.filter((e) => String(e.out.id) !== String(rid.id)),
      { out: rid, version },
    ];
    return row;
  }
}

const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));

function feedWith(fake: FakeSurreal, over: Partial<Parameters<typeof createLiveFeed>[0]> = {}) {
  return createLiveFeed({
    endpoint: 'https://db.example.com',
    namespace: 'ns',
    database: 'db',
    token: aliceToken,
    sessionId: 'sess',
    surreal: fake as unknown as LiveSurreal,
    ...over,
  });
}

describe('createLiveFeed', () => {
  it('registers exactly what the full client sends for the same query and session', async () => {
    const q = new QueryBuilder(schema, 'notification')
      .where({ user: 'user:alice', read: false } as any)
      .build();

    // The full client: its own registerInput, then the register sagas.
    const services = fakeServiceBundle<any>();
    const a = fakeAdapters({ services: { 'remote.connect': () => new Promise(() => {}) } as any });
    const runtime = new Runtime({
      env: defaultEnv(schema),
      adapters: a.adapters,
      logger: services.logger,
      tabId: 't',
    });
    const client = new Sp00kyClient<any>(
      {
        database: { namespace: 'ns', database: 'db' },
        schema,
        schemaSurql: '',
        logLevel: 'silent',
      } as any,
      {
        services,
        runtime,
      }
    );
    const input = (client as any).registerInput('notification', q.innerQuery, '10m');
    const env = defaultEnv(schema);
    const local = await runPure(registerLocal(env, input), {
      state: { ...emptyState({ tabId: 't' }), sessionId: 'sess', userId: 'user:alice' },
      handlers: {
        'local.getById': () => null,
        'ssp.register': () => ({
          localArray: [],
          timings: { parseMs: 0, planMs: 0, snapshotMs: 0, wallMs: 0 },
        }),
      },
    });
    let sent: { sql: string; vars: Record<string, unknown> } | null = null;
    await runPure(registerRemote(env, local.result), {
      state: local.state,
      handlers: {
        'remote.query': (e: any) => {
          sent = { sql: e.sql, vars: e.vars };
          throw new Error('captured');
        },
      },
    });

    const fake = new FakeSurreal();
    const feed = feedWith(fake);
    const sub = feed.subscribe(q, { onSet: () => {} });
    await sub.ready;
    const reg = fake.calls.find((c) => c.sql.startsWith('fn::query::register'))!;
    expect(sent).not.toBeNull();
    expect(reg.sql).toBe(sent!.sql);
    expect(reg.vars).toEqual(sent!.vars);
    expect(reg.sql).toContain('FROM _00_list_ref_user_alice');
    expect((reg.vars!.config as any).params.user).toBeInstanceOf(RecordId);
    expect(sub.key).toBe(local.result);
    expect(await queryKey({ surql: input.surql, params: input.params }, 'sess')).toBe(local.result);
    expect(feed.userId).toBe('user:alice');
    expect(fake.connect).toHaveBeenCalledWith('wss://db.example.com/rpc', expect.anything());
    expect(fake.use).toHaveBeenCalledWith({ namespace: 'ns', database: 'db' });
    expect(fake.authenticate).toHaveBeenCalledWith(aliceToken);
    await feed.close();
    await client.close();
  });

  it('loads the first set, lands FETCH bodies from LIVE, fetches bare ids, drops deletes', async () => {
    const fake = new FakeSurreal();
    const r1 = fake.put(n1, 1);
    const feed = feedWith(fake);
    const sets: unknown[][] = [];
    const changes: any[] = [];
    const sub = feed.subscribe(
      { surql: 'SELECT * FROM notification WHERE user = $auth.id;' },
      { onSet: (rows) => sets.push(rows), onChange: (c) => changes.push(c) }
    );
    expect(await sub.ready).toEqual([r1]);
    expect(sets).toEqual([[r1]]);
    expect(
      fake.calls.some((c) => c.sql === 'LIVE SELECT * FROM _00_list_ref_user_alice FETCH out')
    ).toBe(true);
    const hash = sub.key!;
    const inRid = new RecordId('_00_query', hash);

    // Inline body (FETCH out): no extra read.
    const reads = fake.calls.length;
    const r2 = body(n2, { title: 'inline' });
    fake.emit('CREATE', { in: inRid, out: r2, version: 1 });
    await tick(30);
    expect(fake.calls.length).toBe(reads);
    expect(sets.at(-1)).toEqual([r1, r2]);
    expect(changes.at(-1)).toEqual({ added: [r2], updated: [], removed: [] });

    // An older version of a held row is ignored; a newer one updates.
    fake.emit('UPDATE', { in: inRid, out: body(n2, { title: 'stale' }), version: 1 });
    const r2b = fake.put(n2, 2, { title: 'newer' });
    fake.emit('UPDATE', { in: inRid, out: r2b, version: 2 });
    await tick(30);
    expect(changes.at(-1)).toEqual({ added: [], updated: [r2b], removed: [] });

    // Bare id (a server that ignored FETCH): the body is read.
    const r3 = fake.put(n3, 4);
    fake.emit('CREATE', { in: inRid, out: n3, version: 4 });
    await tick(60);
    expect(fake.calls.at(-1)!.sql).toBe('SELECT * FROM $ids');
    expect(changes.at(-1)).toEqual({ added: [r3], updated: [], removed: [] });

    // Another view's edge and subquery children are not ours.
    fake.emit('CREATE', {
      in: new RecordId('_00_query', 'other'),
      out: body(new RecordId('notification', 'x')),
      version: 1,
    });
    fake.emit('CREATE', {
      in: inRid,
      out: body(new RecordId('notification', 'child')),
      version: 1,
      parent: 'notification:n1',
    });
    await tick(30);
    expect(sub.rows()).toHaveLength(3);

    fake.edges = fake.edges.filter((e) => e.out.id !== 'n1');
    fake.emit('DELETE', { in: inRid, out: n1, version: 1 });
    await tick(30);
    expect(changes.at(-1)).toEqual({ added: [], updated: [], removed: [r1] });
    expect(sub.rows()).toEqual([r2b, r3]);

    // `out: null` (unreadable or deleted row): membership is re-read and diffed.
    fake.edges = fake.edges.filter((e) => e.out.id !== 'n3');
    fake.emit('DELETE', { in: inRid, out: null, version: 4 });
    await tick(120);
    expect(sub.rows()).toEqual([r2b]);
    expect(await feed.idle({ timeoutMs: 2000, quietMs: 20 })).toBe(true);
    await feed.close();
  });

  it('shares one view between identical subscriptions and releases it with the last one', async () => {
    const fake = new FakeSurreal();
    fake.put(n1, 1);
    const feed = feedWith(fake);
    const a = feed.subscribe({ surql: 'SELECT * FROM notification;' }, { onSet: () => {} });
    await a.ready;
    const seen: unknown[][] = [];
    const b = feed.subscribe(
      { surql: 'SELECT * FROM notification;' },
      { onSet: (rows) => seen.push(rows) }
    );
    await b.ready;
    expect(seen).toHaveLength(1);
    expect(fake.calls.filter((c) => c.sql.startsWith('fn::query::register'))).toHaveLength(1);
    await a.unsubscribe();
    expect(fake.calls.some((c) => c.sql.startsWith('fn::query::unsubscribe'))).toBe(false);
    await b.unsubscribe();
    const release = fake.calls.find((c) => c.sql.startsWith('fn::query::unsubscribe'))!;
    expect(release.vars!.id).toEqual(new RecordId('_00_query', b.key!));
    await feed.close();
  });

  it('close() kills LIVE, releases every view, closes the socket; idle waits for the first set', async () => {
    const fake = new FakeSurreal();
    fake.put(n1, 1);
    const feed = feedWith(fake, { token: async () => aliceToken });
    const one = feed.subscribe({ surql: 'SELECT * FROM notification;' }, { onSet: () => {} });
    const two = feed.subscribe(
      { surql: 'SELECT * FROM notification WHERE read = false;' },
      { onSet: () => {} }
    );
    expect(await feed.idle({ timeoutMs: 3000, quietMs: 20 })).toBe(true);
    expect(one.rows()).toHaveLength(1);
    await feed.close();
    expect(fake.killed).toBe(true);
    const release = fake.calls.find((c) => c.sql.includes('fn::query::unsubscribe($id0)'))!;
    expect(release.sql).toContain('fn::query::unsubscribe($id1)');
    expect([release.vars!.id0, release.vars!.id1]).toEqual(
      expect.arrayContaining([
        new RecordId('_00_query', one.key!),
        new RecordId('_00_query', two.key!),
      ])
    );
    expect(fake.closed).toBe(true);
    expect(feed.closed).toBe(true);
    await feed.close();
    await expect(feed.query('RETURN 1')).rejects.toBeInstanceOf(LiveFeedError);
    expect(await feed.idle()).toBe(false);
  });

  it('a refused token calls onUnauthorized and rejects the subscription', async () => {
    const fake = new FakeSurreal();
    fake.authError = new Error('There was a problem with authentication');
    const onUnauthorized = vi.fn();
    const onError = vi.fn();
    const feed = feedWith(fake, { onUnauthorized });
    const sub = feed.subscribe(
      { surql: 'SELECT * FROM notification;' },
      { onSet: () => {}, onError }
    );
    await expect(sub.ready).rejects.toMatchObject({ code: 'unauthorized' });
    expect(onUnauthorized).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(fake.closed).toBe(true);
  });

  it('anonymous feeds use the anon list_ref table; query() runs on the feed connection', async () => {
    const fake = new FakeSurreal();
    const feed = feedWith(fake, { token: undefined });
    expect(await feed.query('RETURN fn::push::info()')).toEqual(['answer']);
    expect(fake.authenticate).not.toHaveBeenCalled();
    const sub = feed.subscribe({ surql: 'SELECT * FROM notification;' }, { onSet: () => {} });
    await sub.ready;
    expect(feed.listRefTable).toBe('_00_list_ref_anon');
    await feed.close();
  });

  it('heartbeats and re-registers a view the server reclaimed', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      const fake = new FakeSurreal();
      fake.put(n1, 1);
      const feed = feedWith(fake, { ttl: '20s' });
      const sub = feed.subscribe({ surql: 'SELECT * FROM notification;' }, { onSet: () => {} });
      await sub.ready;
      fake.heartbeatGone = true;
      await vi.advanceTimersByTimeAsync(10_000);
      expect(fake.calls.some((c) => c.sql.startsWith('fn::query::heartbeat($id0)'))).toBe(true);
      expect(fake.calls.filter((c) => c.sql.startsWith('fn::query::register'))).toHaveLength(2);
      await feed.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it('after a reconnect: LIVE again and every view re-read (changes in the gap are not lost)', async () => {
    const fake = new FakeSurreal();
    fake.put(n1, 1);
    const feed = feedWith(fake);
    const changes: any[] = [];
    const sub = feed.subscribe(
      { surql: 'SELECT * FROM notification;' },
      { onSet: () => {}, onChange: (c) => changes.push(c) }
    );
    await sub.ready;
    fake.fire('disconnected');
    const r2 = fake.put(n2, 1);
    fake.fire('connected');
    await tick(120);
    expect(fake.calls.filter((c) => c.sql.startsWith('LIVE SELECT'))).toHaveLength(2);
    expect(changes.at(-1)).toEqual({ added: [r2], updated: [], removed: [] });
    await feed.close();
    expect([...fake.events.values()].every((l) => l.length === 0)).toBe(true);
  });

  it('falls back to a plain LIVE (bodies read per edge) when the server refuses FETCH', async () => {
    const fake = new FakeSurreal();
    fake.put(n1, 1);
    const answer = fake.answer.bind(fake);
    fake.answer = (sql, vars) => {
      if (sql.endsWith('FETCH out')) throw new Error('Parse error: FETCH not supported on LIVE');
      return answer(sql, vars);
    };
    const onError = vi.fn();
    const feed = feedWith(fake, { onError });
    const sub = feed.subscribe({ surql: 'SELECT * FROM notification;' }, { onSet: () => {} });
    await sub.ready;
    expect(onError).toHaveBeenCalledTimes(1);
    expect(fake.calls.filter((c) => c.sql.startsWith('LIVE SELECT')).map((c) => c.sql)).toEqual([
      'LIVE SELECT * FROM _00_list_ref_user_alice FETCH out',
      'LIVE SELECT * FROM _00_list_ref_user_alice',
    ]);
    const r2 = fake.put(n2, 1);
    fake.emit('CREATE', { in: new RecordId('_00_query', sub.key!), out: n2, version: 1 });
    await tick(60);
    expect(sub.rows()).toEqual([body(n1), r2]);
    await feed.close();
  });

  it('waits out a materializing view before delivering the first set', async () => {
    const fake = new FakeSurreal();
    fake.meta = { rowCount: 1, state: 'materializing' };
    const feed = feedWith(fake);
    const sub = feed.subscribe({ surql: 'SELECT * FROM notification;' }, { onSet: () => {} });
    setTimeout(() => {
      fake.put(n1, 1);
      fake.meta = { rowCount: 1, state: 'ready' };
    }, 200);
    expect(await sub.ready).toHaveLength(1);
    await feed.close();
  });
});

describe('live feed helpers', () => {
  it('normalizes endpoints and query inputs', async () => {
    expect(liveEndpoint('https://h.example')).toBe('wss://h.example/rpc');
    expect(liveEndpoint('http://localhost:8000/rpc')).toBe('ws://localhost:8000/rpc');
    expect(liveEndpoint('wss://h/rpc')).toBe('wss://h/rpc');
    expect(liveEndpoint('not a url')).toBe('not a url');
    expect(liveQueryInput({ surql: 'SELECT * FROM thing WHERE a = $a', params: { a: 1 } })).toEqual(
      {
        table: 'thing',
        surql: 'SELECT * FROM thing WHERE a = $a',
        params: { a: 1 },
      }
    );
    const inner = new QueryBuilder(schema, 'notification')
      .where({ user: 'user:bob' } as any)
      .build().innerQuery;
    const fromInner = liveQueryInput(inner as any);
    expect(fromInner.table).toBe('notification');
    expect(fromInner.params.user).toBeInstanceOf(RecordId);
    expect(() => liveQueryInput({} as any)).toThrow(TypeError);
    expect(await queryKey({ surql: 'x', params: {} }, null)).toBe(
      await sha256Hex(queryHashInput({ surql: 'x', params: {} }, null))
    );
  });
});
