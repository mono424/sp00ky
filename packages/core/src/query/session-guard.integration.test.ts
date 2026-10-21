import { Socket } from 'node:net';
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { RecordId, Surreal } from 'surrealdb';
import { liveEndpoint } from '../live/index';
import { isRefusedSessionError } from '../utils/error-classification';
import { heartbeatBatch, heartbeatRowGone, queryRowCountSelect } from './sql';

/**
 * The app's session guard (`sessionGuard` in sp00ky.yml, the SESSION GUARD
 * block of `meta_tables_remote.surql`) ends sync for a WebSocket that is still
 * open. SurrealDB checks a token only when a socket authenticates, so before
 * the guard a socket that authenticated before a password reset kept reading
 * its user's edges, heartbeating its views and registering new ones until it
 * happened to reconnect.
 *
 * Runs the shipped guard, `_00_query` and `_00_list_ref` blocks, the
 * `fn::query::*` functions and the per-user edge-table rule ssp-node defines,
 * against a real SurrealDB, because the guarantee is the engine's evaluation
 * of those rules, LIVE notifications included. Boot one first, e.g.:
 *   docker run -d --name spooky-surreal-it -p 18011:8000 \
 *     surrealdb/surrealdb:v3.1.5 start --user root --pass root --allow-all memory
 *
 * Override the endpoint with SURREAL_IT_ENDPOINT. Skips if unreachable.
 */
const endpoint = liveEndpoint(process.env.SURREAL_IT_ENDPOINT ?? 'ws://127.0.0.1:18011');
const NS = 'it';

/** The guard WhitePawn ships: a password reset stamps `session_epoch`. */
const GUARD = '$auth.session_epoch = NONE OR ($token.iat ?? 0) >= $auth.session_epoch';

const repoFile = (path: string) =>
  readFileSync(fileURLToPath(new URL(`../../../../${path}`, import.meta.url)), 'utf8');
const cliSource = (file: string) => repoFile(`apps/cli/src/${file}`);

function slice(src: string, from: string, to: string): string {
  const start = src.indexOf(from);
  const end = src.indexOf(to, start);
  if (start < 0 || end < start) throw new Error(`block ${from} .. ${to} not found`);
  return src.slice(start, end);
}

/**
 * The SESSION GUARD block, with `expr` put in the way `spky migrate` does
 * (`schema_builder::apply_session_guard`, whose marker is read from the Rust
 * source so the two cannot drift).
 */
function guardSchema(expr?: string): string {
  const block = slice(
    cliSource('meta_tables_remote.surql'),
    '-- SPOOKY SESSION GUARD',
    '-- SPOOKY INCANTATION'
  );
  const m = /pub const SESSION_GUARD_DEFAULT: &str = "([^"]+)";/.exec(
    repoFile('apps/cli/src/schema_builder.rs')
  );
  if (!m || !block.includes(m[1])) throw new Error('session guard marker not found');
  return expr ? block.replace(m[1], `RETURN (${expr}) == true;`) : block;
}

/** Just the tables this test needs: the rest of the file needs modules. */
const queryTableSchema = () =>
  slice(
    cliSource('meta_tables_remote.surql'),
    'DEFINE TABLE OVERWRITE _00_query SCHEMALESS',
    '-- SPOOKY LIST REF'
  );
const listRefSchema = () =>
  slice(
    cliSource('meta_tables_remote.surql'),
    'DEFINE TABLE OVERWRITE _00_list_ref SCHEMALESS',
    '-- SPOOKY STREAM PROCESSOR STATE'
  );

/** The rule ssp-node puts on `_00_list_ref_user_<uid>` (`user_list_ref_ddl` in tables.rs). */
function userListRefTable(uid: string): string {
  const m = /PERMISSIONS FOR select WHERE (\$auth\.id = user:\{uid\}[^,]*),/.exec(
    repoFile('packages/ssp-node/src/tables.rs')
  );
  if (!m) throw new Error('per-user list_ref rule not found in ssp-node/src/tables.rs');
  return `DEFINE TABLE OVERWRITE _00_list_ref_user_${uid} SCHEMALESS
    PERMISSIONS FOR select WHERE ${m[1].replace('{uid}', uid)}, FOR create, update, delete WHERE false;`;
}

/** An app schema: a hidden `session_epoch` and an app table that asks the guard too. */
const APP = `
DEFINE TABLE OVERWRITE user SCHEMAFULL PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE FIELD OVERWRITE name ON TABLE user TYPE string;
DEFINE FIELD OVERWRITE session_epoch ON TABLE user TYPE option<int>
  PERMISSIONS FOR select WHERE false, FOR create, update WHERE false;
DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD
  SIGNUP (CREATE ONLY type::record('user', $name) SET name = $name)
  SIGNIN (SELECT * FROM user WHERE name = $name)
  DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE OVERWRITE game SCHEMALESS
  PERMISSIONS FOR select WHERE owner = $auth.id AND fn::sp00ky::session_live();
CREATE game:1 SET owner = user:alice;
`;

/** What the SSP writes, as root, when a session registers a view (`node.rs`, cold path). */
const SSP_REGISTER = `
UPSERT $id SET clientId = $sid, auth_id = 'user:alice', surql = 'SELECT * FROM game',
  params = { auth: { id: 'user:alice' }, access: 'account' }, ttl = 1h, lastActiveAt = time::now(),
  rowCount = 1, state = 'ready',
  subscribers = array::append(
    array::filter(subscribers ?? [], |$s| <string>$s.id != $sid AND <datetime>$s.seenAt + 1h > time::now()),
    { id: $sid, seenAt: time::now() })`;

const viewId = () =>
  new RecordId('_00_query', createHash('sha256').update(randomUUID()).digest('hex'));

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('session guard ends sync for an open socket (real SurrealDB)', () => {
  let root: Surreal;
  let database = '';
  let reachable = false;
  const open: Surreal[] = [];

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

  /** Root, a fresh database, the app and sp00ky's rules with `guard` in place. */
  async function boot(guard?: string): Promise<void> {
    database = `session_guard_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    root = new Surreal();
    await root.connect(endpoint, { reconnect: false });
    await root.signin({ username: 'root', password: 'root' });
    await root.use({ namespace: NS, database });
    await root.query(guardSchema(guard));
    await root.query(APP);
    await root.query(queryTableSchema());
    await root.query(listRefSchema());
    await root.query(userListRefTable('alice'));
    await root.query(cliSource('functions_remote.surql'));
    // The guard throws before the post; a live session's post fails on this
    // closed port, which is not a refused session.
    await root.query(
      cliSource('functions_remote_singlenode.surql')
        .replaceAll('{{ENDPOINT}}', 'http://127.0.0.1:9')
        .replaceAll('{{SECRET}}', 'it-secret')
    );
  }

  afterEach(async () => {
    if (!reachable || !root) return;
    for (const db of open.splice(0)) await db.close().catch(() => undefined);
    await root.query(`REMOVE DATABASE ${database}`).catch(() => undefined);
    await root.close();
  });

  /** Alice on a socket of her own, and that socket's `session::id()`. */
  async function alice(how: 'signup' | 'signin'): Promise<[Surreal, string]> {
    const db = new Surreal();
    open.push(db);
    await db.connect(endpoint, { reconnect: false });
    await db.use({ namespace: NS, database });
    await db[how]({ namespace: NS, database, access: 'account', variables: { name: 'alice' } });
    const [sid] = (await db.query('RETURN <string>session::id()')) as [string];
    return [db, sid];
  }

  /** What a password reset does: every token issued before now stops counting. */
  async function resetPassword(stale: Surreal): Promise<void> {
    const [iat] = (await stale.query('RETURN $token.iat')) as [number];
    const epoch = iat + 1;
    await root.query('UPDATE user:alice SET session_epoch = $epoch', { epoch });
    // `iat` has second precision: the next sign-in must land at or past the epoch.
    while (Date.now() / 1000 < epoch) await sleep(50);
  }

  /** LIVE on the per-user edge table; returns the notifications seen so far. */
  async function watchEdges(db: Surreal): Promise<unknown[]> {
    const seen: unknown[] = [];
    const [id] = (await db.query('LIVE SELECT * FROM _00_list_ref_user_alice')) as [unknown];
    const stream = await db.liveOf(id as never);
    // Typed by the SDK (`LiveMessage`), not a narrower hand-written shape:
    // the hand-written one failed `tsc` and so the core Test job.
    stream.subscribe((message) => {
      if (message.action !== 'KILLED') seen.push(message.value);
    });
    return seen;
  }

  const edge = (n: number) =>
    root.query(
      `CREATE type::record('_00_list_ref_user_alice', $n) SET in = _00_query:q, out = game:1, version = $n`,
      { n }
    );

  /** The client's register read-back, as `registerSelect` sends it. */
  const readBack = async (db: Surreal, id: RecordId<string>) =>
    ((await db.query(queryRowCountSelect(), { in: id })) as [unknown])[0] ?? null;

  const heartbeatFound = async (db: Surreal, id: RecordId<string>) => {
    const beat = heartbeatBatch([id]);
    const [result] = (await db.query(beat.sql, beat.vars)) as [unknown];
    return !heartbeatRowGone(result);
  };

  const registerError = async (db: Surreal): Promise<unknown> =>
    db
      .query('RETURN fn::query::register($config)', {
        config: { id: viewId(), surql: 'SELECT * FROM game', params: {}, ttl: '1h' },
      })
      .then(
        () => null,
        (error: unknown) => error
      );

  it('a socket from before the reset stops receiving edges, a fresh sign-in keeps them', async (ctx) => {
    if (!reachable) ctx.skip();
    await boot(GUARD);
    const [stale] = await alice('signup');
    const staleSeen = await watchEdges(stale);
    await edge(1);
    await expect.poll(() => staleSeen.length).toBe(1);

    await resetPassword(stale);
    const [fresh] = await alice('signin');
    const freshSeen = await watchEdges(fresh);
    await edge(2);
    await edge(3);

    await expect.poll(() => freshSeen.length).toBe(2);
    await sleep(200);
    expect(staleSeen.length).toBe(1);
  });

  it('a socket from before the reset reads none of its user\'s edges, views or rows', async (ctx) => {
    if (!reachable) ctx.skip();
    await boot(GUARD);
    const [stale, staleSid] = await alice('signup');
    await edge(1);
    await root.query(
      `CREATE _00_list_ref:1 SET in = _00_query:q, out = game:1, clientId = 'x', auth_id = 'user:alice', version = 1`
    );
    await resetPassword(stale);
    const [fresh, freshSid] = await alice('signin');
    const q = viewId();
    await root.query(SSP_REGISTER, { id: q, sid: staleSid });
    await root.query(SSP_REGISTER, { id: q, sid: freshSid });

    for (const table of ['_00_list_ref_user_alice', '_00_list_ref', 'game']) {
      expect(await stale.query(`SELECT VALUE id FROM ${table}`), table).toEqual([[]]);
      expect(((await fresh.query(`SELECT VALUE id FROM ${table}`)) as [unknown[]])[0], table).toHaveLength(1);
    }
    expect(await readBack(stale, q)).toBeNull();
    expect(await heartbeatFound(stale, q)).toBe(false);
    expect(await readBack(fresh, q)).toEqual({ rowCount: 1, state: 'ready' });
    expect(await heartbeatFound(fresh, q)).toBe(true);
  });

  it('fn::query::register refuses the stale socket as a refused session, and only that one', async (ctx) => {
    if (!reachable) ctx.skip();
    await boot(GUARD);
    const [stale] = await alice('signup');
    expect(isRefusedSessionError(await registerError(stale))).toBe(false);

    await resetPassword(stale);
    const [fresh] = await alice('signin');

    const refused = await registerError(stale);
    expect(isRefusedSessionError(refused), String(refused)).toBe(true);
    expect(isRefusedSessionError(await registerError(fresh))).toBe(false);
  });

  it('without a sessionGuard every session stays live, as before the guard', async (ctx) => {
    if (!reachable) ctx.skip();
    await boot();
    const [stale, staleSid] = await alice('signup');
    await edge(1);
    const q = viewId();
    await root.query(SSP_REGISTER, { id: q, sid: staleSid });
    await resetPassword(stale);

    expect(((await stale.query('SELECT VALUE id FROM _00_list_ref_user_alice')) as [unknown[]])[0]).toHaveLength(1);
    expect(await heartbeatFound(stale, q)).toBe(true);
    expect(isRefusedSessionError(await registerError(stale))).toBe(false);
  });

  it('a database without the probe row lets every registration through', async (ctx) => {
    if (!reachable) ctx.skip();
    await boot(GUARD);
    const [stale] = await alice('signup');
    await resetPassword(stale);
    await root.query('DELETE _00_session_guard:probe');

    expect(isRefusedSessionError(await registerError(stale))).toBe(false);
  });
});
