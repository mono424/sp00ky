import { createServer, type Server } from 'node:http';
import { Socket, type AddressInfo } from 'node:net';
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
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
 *
 * The registration tests also run the shipped `fn::query::register`, whose
 * `http::post` must reach a stand-in SSP this test serves. SurrealDB reaches it
 * at SURREAL_IT_CALLBACK_HOST (default `host.docker.internal`, the host as seen
 * from Docker Desktop); those tests skip when it cannot.
 */
const endpoint = liveEndpoint(process.env.SURREAL_IT_ENDPOINT ?? 'ws://127.0.0.1:18011');
const callbackHost = process.env.SURREAL_IT_CALLBACK_HOST ?? 'host.docker.internal';
const NS = 'it';

const cliSource = (file: string) =>
  readFileSync(fileURLToPath(new URL(`../../../../apps/cli/src/${file}`, import.meta.url)), 'utf8');

/**
 * The SET clause ssp-node puts first in every registration write, read out of
 * `packages/ssp-node/src/node.rs` so this test runs the shipped text.
 */
function ownerGuard(): string {
  const src = readFileSync(
    fileURLToPath(new URL('../../../ssp-node/src/node.rs', import.meta.url)),
    'utf8'
  );
  const m = /pub const OWNER_GUARD: &str = "((?:[^"\\]|\\.)*)";/s.exec(src);
  if (!m) throw new Error('OWNER_GUARD not found in ssp-node/src/node.rs');
  // A Rust `\` at a line end drops the newline and the next line's indent.
  return m[1].replace(/\\\n\s*/g, '');
}

const sha256 = (text: string) => createHash('sha256').update(text).digest('hex');

/**
 * Just the session guard (its rule's user branch asks it) and the `_00_query`
 * block: the rest of the file needs tables and modules this test does not.
 */
function queryTableSchema(): string {
  const meta = cliSource('meta_tables_remote.surql');
  const guard = meta.indexOf('-- SPOOKY SESSION GUARD');
  const start = meta.indexOf('DEFINE TABLE OVERWRITE _00_query SCHEMALESS');
  const end = meta.indexOf('-- SPOOKY LIST REF');
  if (guard < 0 || start < guard || end < start)
    throw new Error('_00_query block not found in meta_tables_remote.surql');
  return meta.slice(guard, meta.indexOf('-- SPOOKY INCANTATION')) + meta.slice(start, end);
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

/**
 * What the SSP writes, as root, when a session joins an anonymous view that
 * already exists (`join_registered_view`), with the shipped ownership guard.
 */
const SSP_JOIN = () => `
UPSERT type::record($id) SET ${ownerGuard()}, clientId = $sid, auth_id = auth_id ?? 'anon',
  surql = surql ?? 'SELECT * FROM item', params = params ?? { auth: { id: '' }, access: '' },
  ttl = ttl ?? 1h, lastActiveAt = time::now(), rowCount = rowCount ?? 3, state = state ?? 'ready',
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

  // The stand-in SSP: records what `fn::query::register` posts and answers it
  // as ssp-node's warm join does (409 `owner_mismatch` when the guard throws).
  const posted: Array<Record<string, unknown>> = [];
  let ssp: Server | null = null;
  let sspUrl = '';
  let sspReachable: boolean | null = null;

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
    ssp = createServer(async (req, res) => {
      let text = '';
      for await (const chunk of req) text += chunk;
      const body = JSON.parse(text) as Record<string, unknown>;
      posted.push(body);
      if (body.probe) return void res.writeHead(200).end('null');
      try {
        await root.query(SSP_JOIN(), {
          id: body.id,
          sid: body.clientId,
          ownerHash: body.ownerHash ?? '',
        });
        res.writeHead(200, { 'content-type': 'application/json' }).end('null');
      } catch (error) {
        const refused = String(error).includes('owner_mismatch');
        res
          .writeHead(refused ? 409 : 500, { 'content-type': 'application/json' })
          .end(
            JSON.stringify({
              code: refused ? 'owner_mismatch' : 'db_error',
              message: String(error),
            })
          );
      }
    });
    await new Promise<void>((resolve) => ssp!.listen(0, '0.0.0.0', resolve));
    sspUrl = `http://${callbackHost}:${(ssp.address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    await new Promise((resolve) => ssp?.close(resolve));
  });

  /** Load the shipped `fn::query::register` against the stand-in; false when SurrealDB cannot reach it. */
  async function registerFunctions(): Promise<boolean> {
    if (sspReachable === null) {
      sspReachable = await root
        .query('RETURN http::post($url, { probe: true })', { url: `${sspUrl}/probe` })
        .then(
          () => true,
          () => false
        );
    }
    if (!sspReachable) return false;
    await root.query(
      cliSource('functions_remote_singlenode.surql')
        .replaceAll('{{ENDPOINT}}', sspUrl)
        .replaceAll('{{SECRET}}', 'it-secret')
    );
    return true;
  }

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

  it("a guest who read another guest's view id cannot join it through fn::query::register; the owner's reconnect can", async (ctx) => {
    if (!reachable) ctx.skip();
    if (!(await registerFunctions())) ctx.skip();
    const [a, sidA] = await guest();
    const [b] = await guest();
    const qa = viewId();
    const register = (db: Surreal, owner?: string) =>
      db.query('fn::query::register($config)', {
        config: {
          id: qa,
          surql: 'SELECT * FROM item',
          params: {},
          ttl: '1h',
          ...(owner ? { owner } : {}),
        },
      });

    await register(a, 'secret-a');
    const sent = posted.at(-1)!;
    expect(sent).toMatchObject({ clientId: sidA, ownerHash: sha256('secret-a') });
    // Only the hash leaves the database, and never inside `params` (the SSP's
    // merge key is built from them).
    expect(JSON.stringify(sent)).not.toContain('secret-a');
    expect(String(sent.params)).not.toContain(sha256('secret-a'));
    const before = await row(qa);
    expect(before?.ownerHash).toBe(sha256('secret-a'));

    // Anonymous view ids are world-readable on `_00_list_ref_anon`. With its
    // own secret or with none, the stranger's registration is refused, so it
    // never lands in `subscribers` and its DELETE still matches nothing.
    await expect(register(b, 'secret-b')).rejects.toThrow();
    await expect(register(b)).rejects.toThrow();
    await b.query('DELETE $id', { id: qa });
    expect(await row(qa)).toEqual(before);
    expect(await readBack(b, qa)).toBeNull();

    // The owner's reconnect: a new socket, the same page secret.
    await a.close();
    const [a2, sidA2] = await guest();
    expect(await heartbeatFound(a2, qa)).toBe(false);
    await register(a2, 'secret-a');
    expect((await row(qa))?.subscribers.map((s: { id: string }) => s.id)).toEqual([sidA, sidA2]);
    expect(await heartbeatFound(a2, qa)).toBe(true);
    expect(await readBack(a2, qa)).toEqual({ rowCount: 3, state: 'ready' });
  });

  it('no caller can read or rewrite the owner hash, not even the owner', async (ctx) => {
    if (!reachable) ctx.skip();
    const [g, sidG] = await guest();
    const qg = viewId();
    await register(qg, sidG);
    await root.query(`UPDATE $id SET ownerHash = 'stored'`, { id: qg });

    const [own] = (await g.query('SELECT * FROM ONLY $id', { id: qg })) as [
      Record<string, unknown>,
    ];
    expect(own.subscribers).toBeDefined();
    expect(own).not.toHaveProperty('ownerHash');
    for (const set of [`ownerHash = 'mine'`, 'ownerHash = NONE']) {
      await g.query(`UPDATE $id SET ${set}`, { id: qg });
      expect((await row(qg))?.ownerHash).toBe('stored');
    }
    // The heartbeat still writes what it needs to around the frozen field.
    expect(await heartbeatFound(g, qg)).toBe(true);
  });

  // Without `anonymousLiveQueries` the SSP stores a signed-out registration
  // with no identity at all (`auth_id = ''`). The rule admitted only `'anon'`
  // rows to their subscribers, so such a page could not read its own row back
  // and its preload never settled.
  it("a signed-out view stored with no identity (anonymous live queries off) answers to its own session, and to nobody else", async (ctx) => {
    if (!reachable) ctx.skip();
    const [a, sidA] = await guest();
    const [b] = await guest();
    const [u] = await member('alice');
    const qa = viewId();
    await register(qa, sidA, '');
    expect((await row(qa))?.auth_id).toBe('');

    expect(await readBack(a, qa)).toEqual({ rowCount: 3, state: 'ready' });
    expect(await heartbeatFound(a, qa)).toBe(true);

    const before = await row(qa);
    for (const other of [b, u]) {
      expect(await readBack(other, qa)).toBeNull();
      expect(await heartbeatFound(other, qa)).toBe(false);
      expect(await other.query('SELECT id FROM _00_query')).toEqual([[]]);
      await other.query('DELETE $id', { id: qa });
    }
    expect(await row(qa)).toEqual(before);

    expect(await a.query('RETURN fn::query::unsubscribe($id)', { id: qa })).toEqual([
      { released: true, remaining: 0 },
    ]);
    expect(await row(qa)).toBeUndefined();
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
