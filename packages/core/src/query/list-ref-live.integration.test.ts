import { Socket } from 'node:net';
import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { Surreal, type Uuid } from 'surrealdb';
import { ANON_LIST_REF_TABLE } from '../modules/ref-tables';
import { liveEndpoint } from '../live/index';
import { hashOfEdge } from '../utils/edges';
import { listRefLive } from './sql';

/**
 * Every signed-out session shares `_00_list_ref_anon`. Each one's LIVE must
 * carry only its own edges, including after a reconnect, when its views' edges
 * still name the socket that registered them.
 *
 * Runs against a real SurrealDB, because the filter rests on how SurrealDB
 * evaluates a LIVE WHERE (`session::id()` in the subscriber's session, a bound
 * param kept with the LIVE). Boot one first, e.g.:
 *   docker run -d --name spooky-surreal-it -p 18011:8000 \
 *     surrealdb/surrealdb:v3.1.5 start --user root --pass root --allow-all memory
 *
 * Needs SurrealDB 3.1 or later: 3.0.x sends no LIVE notification for a write
 * that follows a `LET` in the same request, which is how the SSP writes every
 * edge (and how these tests write them).
 *
 * Override the endpoint with SURREAL_IT_ENDPOINT. Skips if unreachable. The
 * Dart twin is `packages/spooky_core/test/integration/anon_live_isolation_test.dart`.
 */
const endpoint = liveEndpoint(process.env.SURREAL_IT_ENDPOINT ?? 'ws://127.0.0.1:18011');
const NS = 'it';

// What the SSP and the CLI define (`ensure_anon_table`, `_00_query`).
const SCHEMA = `
DEFINE TABLE OVERWRITE item SCHEMALESS PERMISSIONS FOR select WHERE true;
DEFINE TABLE OVERWRITE _00_query SCHEMALESS
  PERMISSIONS FOR select, create, update, delete
    WHERE auth_id = 'anon' OR ($auth.id != NONE AND auth_id = <string>$auth.id);
DEFINE TABLE OVERWRITE _00_list_ref_anon SCHEMALESS
  PERMISSIONS FOR select WHERE true, FOR create, update WHERE false, FOR delete WHERE false;
DEFINE FIELD OVERWRITE in ON TABLE _00_list_ref_anon TYPE record;
DEFINE FIELD OVERWRITE out ON TABLE _00_list_ref_anon TYPE record;
DEFINE FIELD OVERWRITE clientId ON TABLE _00_list_ref_anon TYPE string;
DEFINE FIELD OVERWRITE auth_id ON TABLE _00_list_ref_anon TYPE string DEFAULT '';
DEFINE FIELD OVERWRITE version ON TABLE _00_list_ref_anon TYPE int;
CREATE item:1 SET name = 'one';
CREATE item:2 SET name = 'two';
CREATE item:3 SET name = 'three';
`;

// The SSP's own statements (`build_edge_batch`): the edge takes the view's
// clientId at write time.
const VIEW =
  "LET $from0 = type::record('_00_query', $q); LET $cid0 = (SELECT VALUE clientId FROM $from0 LIMIT 1)[0];";

interface Heard {
  action: string;
  value: Record<string, unknown>;
}

const waitFor = async (pred: () => boolean, ms = 3_000): Promise<void> => {
  const deadline = Date.now() + ms;
  while (!pred() && Date.now() < deadline) await new Promise((r) => setTimeout(r, 25));
};
const settle = () => new Promise((r) => setTimeout(r, 300));

describe('anonymous list_ref LIVE (real SurrealDB)', () => {
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
    database = `anon_live_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    root = new Surreal();
    await root.connect(endpoint, { reconnect: false });
    await root.signin({ username: 'root', password: 'root' });
    await root.use({ namespace: NS, database });
    await root.query(SCHEMA);
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

  /** Open the client's anonymous LIVE and collect what it hears. */
  async function listen(db: Surreal, earlier: string[] = []): Promise<Heard[]> {
    const stmt = listRefLive(ANON_LIST_REF_TABLE, { inlineBodies: true, sessionIds: earlier });
    const [uuid] = (await db.query(stmt.sql, stmt.vars)) as [Uuid];
    const live = await db.liveOf(uuid);
    const got: Heard[] = [];
    live.subscribe((message) => {
      if (message.action !== 'KILLED')
        got.push({ action: message.action, value: message.value as Record<string, unknown> });
    });
    return got;
  }

  const register = (q: string, sid: string) =>
    root.query("UPSERT type::record('_00_query', $q) SET clientId = $sid, auth_id = 'anon'", {
      q,
      sid,
    });
  const relate = (q: string, item: string) =>
    root.query(
      `${VIEW} RELATE $from0->_00_list_ref_anon->${item} SET version = 1, clientId = $cid0, auth_id = 'anon'`,
      { q }
    );
  const bump = (q: string, item: string, version: number, restamp = true) =>
    root.query(
      `${VIEW} UPDATE (SELECT VALUE id FROM ${item}<-_00_list_ref_anon WHERE in = $from0) SET version = ${version}${restamp ? ', clientId = $cid0 ?? clientId' : ''} RETURN NONE`,
      { q }
    );
  const remove = (q: string, item: string) =>
    root.query(
      `${VIEW} DELETE (SELECT VALUE id FROM ${item}<-_00_list_ref_anon WHERE in = $from0)`,
      { q }
    );

  const heard = (got: Heard[]) =>
    got
      .map((m) => `${m.action} ${hashOfEdge(m.value)} ${(m.value.out as { name?: string }).name}`)
      .join(', ');

  it("two anonymous sessions do not hear each other's edges", async (ctx) => {
    if (!reachable) ctx.skip();
    const [a, sidA] = await guest();
    const [b, sidB] = await guest();
    await register('qa', sidA);
    await register('qb', sidB);
    const gotA = await listen(a);
    const gotB = await listen(b);

    await relate('qa', 'item:1');
    await relate('qb', 'item:2');
    await bump('qa', 'item:1', 2);
    await waitFor(() => gotA.length >= 2 && gotB.length >= 1);
    await settle();

    // The body still rides along (`FETCH out`).
    expect(heard(gotA)).toBe('CREATE qa one, UPDATE qa one');
    expect(heard(gotB)).toBe('CREATE qb two');
  });

  it('after a reconnect the new socket still hears the edges its old one registered, and a bystander hears none of it', async (ctx) => {
    if (!reachable) ctx.skip();
    const [a, sidA] = await guest();
    const [b, sidB] = await guest();
    await register('qa', sidA);
    await register('qb', sidB);
    await relate('qa', 'item:1');
    await relate('qa', 'item:2');
    const gotB = await listen(b);

    // New socket, new session id. The view stays registered under the old one
    // until the client re-registers it.
    await a.close();
    const [a2, sidA2] = await guest();
    const gotA2 = await listen(a2, [sidA]);

    // An SSP that does not restamp: the edge keeps naming the old socket.
    await bump('qa', 'item:1', 2, false);
    await waitFor(() => gotA2.length >= 1);
    expect(heard(gotA2)).toBe('UPDATE qa one');

    // Re-registered: the view, and from now on each edge it bumps, names the
    // new socket. An edge nothing bumped still names the old one when it goes.
    await register('qa', sidA2);
    await bump('qa', 'item:1', 3);
    await remove('qa', 'item:2');
    await relate('qa', 'item:3');
    await waitFor(() => gotA2.length >= 4);
    await settle();
    expect(heard(gotA2)).toBe('UPDATE qa one, UPDATE qa one, DELETE qa two, CREATE qa three');
    expect(gotB).toEqual([]);
  });
});
