import { Socket } from 'node:net';
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { RecordId, Surreal } from 'surrealdb';
import { liveEndpoint } from '../live/index';
import {
  heartbeatBatch,
  heartbeatRowGone,
  leaveViewsBeacon,
  queryRowCountBatchSelect,
  queryRowCountSelect,
} from './sql';

/**
 * Every signed-out visitor shares the one `anon` identity, so `_00_query`
 * tells their views apart by session: an anonymous row answers only to the
 * sessions in its `subscribers`. A second visitor must not be able to list,
 * read, rewrite, plant or delete the first one's view, while the first keeps
 * its heartbeat and its registration read-back, also after a reconnect.
 *
 * Runs the shipped `_00_query` block and `fn::query::*` functions from
 * `apps/cli/src` against a real SurrealDB, because the guarantee is the
 * engine's evaluation of the table and field permissions. Boot one first, e.g.:
 *   docker run -d --name spooky-surreal-it -p 18011:8000 \
 *     surrealdb/surrealdb:v3.1.5 start --user root --pass root --allow-all memory
 *
 * Override the endpoint with SURREAL_IT_ENDPOINT. Skips if unreachable.
 */
const endpoint = liveEndpoint(process.env.SURREAL_IT_ENDPOINT ?? 'ws://127.0.0.1:18011');
const NS = 'it';

const cliSource = (file: string) =>
  readFileSync(fileURLToPath(new URL(`../../../../apps/cli/src/${file}`, import.meta.url)), 'utf8');

/** Just the `_00_query` block: the rest of the file needs tables and modules this test does not. */
function queryTableSchema(): string {
  const meta = cliSource('meta_tables_remote.surql');
  const start = meta.indexOf('DEFINE TABLE OVERWRITE _00_query SCHEMALESS');
  const end = meta.indexOf('-- SPOOKY LIST REF');
  if (start < 0 || end < start)
    throw new Error('_00_query block not found in meta_tables_remote.surql');
  return meta.slice(start, end);
}

const ACCESS = `
DEFINE TABLE OVERWRITE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD
  SIGNUP (CREATE user SET name = $name)
  SIGNIN (SELECT * FROM user WHERE name = $name)
  DURATION FOR TOKEN 1h, FOR SESSION 1h;
`;

/** What the SSP writes, as root, when a session registers a view (`node.rs`, cold path). */
const SSP_REGISTER = `
UPSERT $id SET clientId = $sid, auth_id = $owner, surql = 'SELECT * FROM item',
  params = { auth: { id: $authParam }, access: '' }, ttl = 1h, lastActiveAt = time::now(),
  rowCount = 3, state = 'ready',
  subscribers = array::append(
    array::filter(subscribers ?? [], |$s| <string>$s.id != $sid AND <datetime>$s.seenAt + 1h > time::now()),
    { id: $sid, seenAt: time::now() })`;

/** A `_00_query:<sha256>` id, the shape the client mints (and the pagehide beacon accepts). */
const viewId = () =>
  new RecordId('_00_query', createHash('sha256').update(randomUUID()).digest('hex'));

describe('anonymous _00_query isolation (real SurrealDB)', () => {
  let root: Surreal;
  let database = '';
  let reachable = false;
  const open: Surreal[] = [];

  // A plain TCP probe: the SDK never rejects a connect to a refused port.
  beforeAll(async () => {
    const url = new URL(endpoint);
    reachable = await new Promise<boolean>((resolve) => {
      const socket = new Socket();
      const done = (ok: boolean) => {
        socket.destroy();
        resolve(ok);
      };
      socket.setTimeout(1_000, () => done(false));
      socket.once('error', () => done(false));
      socket.connect(Number(url.port || 80), url.hostname, () => done(true));
    });
  });

  beforeEach(async () => {
    if (!reachable) return;
    database = `anon_query_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    root = new Surreal();
    await root.connect(endpoint, { reconnect: false });
    await root.signin({ username: 'root', password: 'root' });
    await root.use({ namespace: NS, database });
    await root.query(ACCESS);
    await root.query(queryTableSchema());
    await root.query(cliSource('functions_remote.surql'));
  });

  afterEach(async () => {
    if (!reachable) return;
    for (const db of open.splice(0)) await db.close().catch(() => undefined);
    await root.query(`REMOVE DATABASE ${database}`).catch(() => undefined);
    await root.close();
  });

  /** A signed-out socket and its `session::id()`. */
  async function guest(): Promise<[Surreal, string]> {
    const db = new Surreal();
    open.push(db);
    await db.connect(endpoint, { reconnect: false });
    await db.use({ namespace: NS, database });
    const [sid] = (await db.query('RETURN <string>session::id()')) as [string];
    return [db, sid];
  }

  /** A signed-in socket, its `session::id()`, `$auth.id` and access token. */
  async function member(name: string): Promise<[Surreal, string, string, string]> {
    const db = new Surreal();
    open.push(db);
    await db.connect(endpoint, { reconnect: false });
    await db.use({ namespace: NS, database });
    const tokens = (await db.signup({
      namespace: NS,
      database,
      access: 'account',
      variables: { name },
    })) as string | { access: string };
    const [sid, authId] = (await db.query(
      'RETURN <string>session::id(); RETURN <string>$auth.id'
    )) as [string, string];
    return [db, sid, authId, typeof tokens === 'string' ? tokens : tokens.access];
  }

  /** `beaconSql`'s request: a fresh HTTP session, Bearer token only when signed in. */
  async function beacon(sql: string, token?: string): Promise<void> {
    const url = new URL(endpoint);
    url.protocol = url.protocol === 'wss:' ? 'https:' : 'http:';
    url.pathname = url.pathname.replace(/\/rpc\/?$/, '') + '/sql';
    const headers: Record<string, string> = {
      Accept: 'application/json',
      'Content-Type': 'text/plain',
      'surreal-ns': NS,
      'surreal-db': database,
    };
    if (token) headers.Authorization = `Bearer ${token}`;
    const res = await fetch(url, { method: 'POST', headers, body: sql });
    expect(res.ok).toBe(true);
  }

  const register = (id: RecordId<string>, sid: string, owner = 'anon') =>
    root.query(SSP_REGISTER, { id, sid, owner, authParam: owner === 'anon' ? '' : owner });
  const row = async (id: RecordId<string>) =>
    ((await root.query('SELECT * FROM ONLY $id', { id })) as [Record<string, any> | undefined])[0];
  const count = async () =>
    ((await root.query('SELECT VALUE id FROM _00_query')) as [unknown[]])[0].length;

  /** The client's register read-back, as `registerSelect` sends it after `fn::query::register`. */
  const readBack = async (db: Surreal, id: RecordId<string>) =>
    ((await db.query(queryRowCountSelect(), { in: id })) as [unknown])[0] ?? null;
  /** The client's lifecycle tick: did the heartbeat find the row? */
  const heartbeatFound = async (db: Surreal, id: RecordId<string>) => {
    const beat = heartbeatBatch([id]);
    const [result] = (await db.query(beat.sql, beat.vars)) as [unknown];
    return !heartbeatRowGone(result);
  };

  it("a second guest cannot list, read, rewrite, release or delete the first guest's view", async (ctx) => {
    if (!reachable) ctx.skip();
    const [a, sidA] = await guest();
    const [b, sidB] = await guest();
    const qa = viewId();
    await register(qa, sidA);
    const before = await row(qa);

    expect(await b.query('SELECT id, surql FROM _00_query')).toEqual([[]]);
    expect(await readBack(b, qa)).toBeNull();
    // The membership batch read. 3.0.x answers a hidden row with an empty
    // object, which the client skips for want of an `id`.
    const [batch] = (await b.query(queryRowCountBatchSelect(), { ins: [qa] })) as [
      Array<{ id?: unknown }>,
    ];
    expect(batch.filter((r) => r?.id)).toEqual([]);

    expect(
      await b.query(`UPDATE $id SET surql = 'SELECT * FROM secret', ttl = 1000w`, { id: qa })
    ).toEqual([[]]);
    expect(await heartbeatFound(b, qa)).toBe(false);
    expect(await b.query('RETURN fn::query::unsubscribe($id)', { id: qa })).toEqual([
      { released: false, remaining: 0 },
    ]);
    // Session ids are not secret (they ride on `_00_list_ref_anon`), so naming
    // the owner's must not help, over the socket or over the beacon's HTTP path.
    await b.query('RETURN fn::query::leave($id, [$sid])', { id: qa, sid: sidA });
    await beacon(leaveViewsBeacon([qa], [sidA])!);
    await b.query('DELETE $id', { id: qa });

    expect(await row(qa)).toEqual(before);
    expect(await count()).toBe(1);
    // ...and the owner never noticed.
    expect(await readBack(a, qa)).toEqual({ rowCount: 3, state: 'ready' });
    expect(sidB).not.toBe(sidA);
  });

  it('no caller can plant a row, so nothing the SSP re-registers at boot comes from a client', async (ctx) => {
    if (!reachable) ctx.skip();
    const [g, sidG] = await guest();
    const [u, , authU] = await member('alice');
    const planted = (sid: string, owner: string) =>
      `SET auth_id = '${owner}', surql = 'SELECT * FROM item', params = {}, ttl = 1h,
           subscribers = [{ id: '${sid}', seenAt: time::now() }]`;

    await g.query(`CREATE _00_query:g1 ${planted(sidG, 'anon')}`);
    await g.query(`UPSERT _00_query:g2 ${planted(sidG, 'anon')}`);
    await g.query(
      `INSERT INTO _00_query { id: 'g3', auth_id: 'anon', ttl: 1h, subscribers: [{ id: '${sidG}' }] }`
    );
    await u.query(`CREATE _00_query:u1 ${planted('x', authU)}`);
    await u.query(`UPSERT _00_query:u2 ${planted('x', authU)}`);
    expect(await count()).toBe(0);
  });

  it('the owner cannot rewrite what the SSP trusts on its own row either', async (ctx) => {
    if (!reachable) ctx.skip();
    const [g, sidG] = await guest();
    const [u, sidU, authU] = await member('alice');
    const qg = viewId();
    const qu = viewId();
    await register(qg, sidG);
    await register(qu, sidU, authU);
    // One field per statement: a write the table rule rejects as a whole would
    // hide whether the field itself is locked. `params.auth.id` is the identity
    // the SSP injects into permission predicates when it rebuilds the view at
    // boot, `surql` skips the allowlist there, and turning a user's row into an
    // anonymous one would publish its edges to the world-readable anon table.
    const tampers = (owner: string): Record<string, string> => ({
      surql: `surql = 'SELECT * FROM secret'`,
      params: `params = { auth: { id: 'user:victim' }, access: 'account' }`,
      ttl: 'ttl = 1000w',
      auth_id: `auth_id = '${owner === 'anon' ? 'user:victim' : 'anon'}'`,
      clientId: `clientId = 'forged'`,
      rowCount: 'rowCount = 0',
      state: `state = 'materializing'`,
      lastActiveAt: 'lastActiveAt = time::now() + 1000w',
    });

    for (const [db, id, owner] of [
      [g, qg, 'anon'],
      [u, qu, authU],
    ] as const) {
      for (const [field, set] of Object.entries(tampers(owner))) {
        const before = await row(id);
        await db.query(`UPDATE $id SET ${set}`, { id });
        expect((await row(id))?.[field], `${owner} ${field}`).toEqual(before?.[field]);
      }
    }
  });

  it('the owning guest keeps its heartbeat and read-back, and after a reconnect gets them back by registering again', async (ctx) => {
    if (!reachable) ctx.skip();
    const [a, sidA] = await guest();
    const [b] = await guest();
    const qa = viewId();
    await register(qa, sidA);

    expect(await readBack(a, qa)).toEqual({ rowCount: 3, state: 'ready' });
    const [batch] = (await a.query(queryRowCountBatchSelect(), { ins: [qa] })) as [
      Array<{ id?: unknown; rowCount?: number }>,
    ];
    expect(batch.map((r) => r.rowCount)).toEqual([3]);
    const stamped = (await row(qa))?.lastActiveAt;
    expect(await heartbeatFound(a, qa)).toBe(true);
    expect((await row(qa))?.lastActiveAt).not.toEqual(stamped);

    // A reconnect is a new session. Until it registers again its heartbeat
    // finds nothing, which the client reads as a reclaimed view and
    // re-registers; the SSP's registration then adds the new session.
    await a.close();
    const [a2, sidA2] = await guest();
    expect(await heartbeatFound(a2, qa)).toBe(false);
    expect(await readBack(a2, qa)).toBeNull();
    await register(qa, sidA2);
    expect(await heartbeatFound(a2, qa)).toBe(true);
    expect(await readBack(a2, qa)).toEqual({ rowCount: 3, state: 'ready' });
    expect((await row(qa))?.subscribers.map((s: { id: string }) => s.id)).toEqual([sidA, sidA2]);

    // The bystander is still out.
    expect(await heartbeatFound(b, qa)).toBe(false);
    expect(await readBack(b, qa)).toBeNull();
  });

  it('the owning guest can still release its view', async (ctx) => {
    if (!reachable) ctx.skip();
    const [a, sidA] = await guest();
    const qa = viewId();
    await register(qa, sidA);
    expect(await a.query('RETURN fn::query::unsubscribe($id)', { id: qa })).toEqual([
      { released: true, remaining: 0 },
    ]);
    expect(await row(qa)).toBeUndefined();
  });

  it("a signed-in user's views are untouched: other tabs share them, the pagehide leave still pulls the expiry in", async (ctx) => {
    if (!reachable) ctx.skip();
    const [u, sidU, authU, token] = await member('alice');
    const [bob] = await member('bob');
    const [g] = await guest();
    const qu = viewId();
    await register(qu, sidU, authU);

    // A second tab of the same user is not a subscriber yet and still gets in.
    const tab2 = new Surreal();
    open.push(tab2);
    await tab2.connect(endpoint, { reconnect: false });
    await tab2.use({ namespace: NS, database });
    await tab2.authenticate(token);
    expect(await heartbeatFound(tab2, qu)).toBe(true);
    expect(await readBack(tab2, qu)).toEqual({ rowCount: 3, state: 'ready' });

    expect(await heartbeatFound(bob, qu)).toBe(false);
    expect(await readBack(g, qu)).toBeNull();

    // The beacon names both tabs' sessions over a fresh HTTP session.
    const [sid2] = (await tab2.query('RETURN <string>session::id()')) as [string];
    await beacon(leaveViewsBeacon([qu], [sidU, sid2])!, token);
    const [left] = (await root.query(
      'SELECT VALUE duration::secs((lastActiveAt + ttl) - time::now()) FROM ONLY $id',
      {
        id: qu,
      }
    )) as [number];
    expect(left).toBeGreaterThan(0);
    expect(left).toBeLessThanOrEqual(60);
    expect((await row(qu))?.subscribers).toEqual([]);
  });
});
