// Spike S8, WebSocket half: LIVE SELECT ... FETCH out, KILL and record-user
// LIVE permissions, through the surrealdb JS SDK the clients ship with.
//
// Run by the Rust binary after its probes (it creates the users and tables
// this file reads), one JSON line per probe on stdout:
//   {"group":"live","id":"live.x","outcome":"pass|fail|known-bug|skip","detail":"...","ms":12}
// Standalone: set the CF_* variables main.rs passes and run
//   node --experimental-strip-types live.ts

import { readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { Surreal } from 'surrealdb';

const env = (k: string): string => process.env[k] ?? '';
const URL = env('CF_WS_URL');
const NS = env('CF_NS');
const DB = env('CF_DB');
const ALICE_ID = env('CF_ALICE_ID');

// Node 22's built-in WebSocket stalls the SDK handshake (see
// docs/surrealdb-bugs/repro/ws-row-cache-stale.mjs), so use the `ws` package
// from the pnpm store when it is there.
function installWs(): void {
  try {
    const store = join(process.cwd(), '../../../node_modules/.pnpm');
    const dir = readdirSync(store).find((d) => d.startsWith('ws@8'));
    if (dir) {
      const req = createRequire(import.meta.url);
      (globalThis as any).WebSocket = req(join(store, dir, 'node_modules/ws'));
    }
  } catch {
    // fall back to the runtime's WebSocket
  }
}

function out(id: string, outcome: string, detail: string, started: number): void {
  console.log(JSON.stringify({ group: 'live', id, outcome, detail, ms: Date.now() - started }));
}

async function probe(id: string, fn: () => Promise<[string, string]>): Promise<void> {
  const started = Date.now();
  try {
    const [outcome, detail] = await withTimeout(fn(), 20000, id);
    out(id, outcome, detail, started);
  } catch (e: any) {
    out(id, 'fail', String(e?.message ?? e), started);
  }
}

function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  return Promise.race([p, new Promise<T>((_, rej) => setTimeout(() => rej(new Error(`${what} timed out after ${ms}ms`)), ms))]);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function root(): Promise<Surreal> {
  const db = new Surreal();
  const token = env('CF_ROOT_TOKEN');
  await db.connect(URL, {
    namespace: NS,
    database: DB,
    authentication: token ? token : { username: env('CF_ROOT_USER'), password: env('CF_ROOT_PASS') },
  });
  return db;
}

async function recordUser(who: 'ALICE' | 'BOB'): Promise<Surreal> {
  const db = new Surreal();
  await db.connect(URL, { namespace: NS, database: DB });
  await db.signin({ namespace: NS, database: DB, access: 'account', variables: { username: env(`CF_${who}_USER`), password: env(`CF_${who}_PASS`) } } as any);
  return db;
}

async function q<T = unknown>(db: Surreal, sql: string, vars?: Record<string, unknown>): Promise<T[]> {
  return (await db.query(sql, vars).collect()) as T[];
}

/** Start a raw `LIVE SELECT`, collect its messages. */
async function liveRaw(db: Surreal, sql: string) {
  const [id] = await q<any>(db, sql);
  const messages: any[] = [];
  const sub = await db.liveOf(id);
  sub.subscribe((m: any) => messages.push(m));
  return { id, messages, sub };
}

async function waitFor(pred: () => boolean, ms: number): Promise<boolean> {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (pred()) return true;
    await sleep(25);
  }
  return pred();
}

const outOf = (m: any) => m?.value?.out ?? m?.result?.out;
const versionOf = (m: any) => m?.value?.version ?? m?.result?.version;

async function main(): Promise<void> {
  installWs();
  let admin: Surreal;
  try {
    admin = await root();
  } catch (e: any) {
    out('live.ws_connect', 'fail', `root WebSocket connect failed: ${e?.message ?? e}`, Date.now());
    return;
  }
  out('live.ws_connect', 'pass', `root session over ${URL}`, Date.now());
  await q(admin, "CREATE _00_query:cflive SET surql = 'SELECT * FROM cf_thread', ttl = 30m, auth_id = $aid, clientId = 'live';", { aid: ALICE_ID });

  await probe('live.session_id_stable', async () => {
    const [a] = await q<string>(admin, 'RETURN <string>session::id();');
    const [b] = await q<string>(admin, 'RETURN <string>session::id();');
    return [a && a === b ? 'pass' : 'fail', `session::id() over one WebSocket: ${a} then ${b}`];
  });

  // Root LIVE with FETCH out, the inline-bodies shape, then KILL.
  let killed: { sub: any; id: any; messages: any[] } | null = null;
  await probe('live.select_fetch_out', async () => {
    const live = await liveRaw(admin, 'LIVE SELECT * FROM _00_list_ref FETCH out;');
    killed = live;
    const writer = await root();
    await q(writer, 'RELATE _00_query:cflive->_00_list_ref->cf_thread:t1 SET version = 9, clientId = "live", auth_id = $aid;', { aid: ALICE_ID });
    await waitFor(() => live.messages.length > 0, 5000);
    await writer.close();
    const body = outOf(live.messages[0]);
    const inline = body && typeof body === 'object' && 'title' in body;
    return [inline ? 'pass' : 'fail', `${live.messages.length} notification(s); out = ${JSON.stringify(body)}`];
  });

  await probe('live.kill', async () => {
    if (!killed) return ['skip', 'needs live.select_fetch_out'];
    const live = killed as { id: any; messages: any[] };
    await q(admin, 'KILL $id;', { id: live.id });
    const before = live.messages.length;
    await q(admin, 'RELATE _00_query:cflive->_00_list_ref->cf_thread:t1 SET version = 10, clientId = "live", auth_id = $aid;', { aid: ALICE_ID });
    await sleep(1500);
    return [live.messages.length === before ? 'pass' : 'fail', `notifications after KILL: ${live.messages.length - before}`];
  });

  // Record user LIVE on the permission-gated edge table, writer is another
  // (root) session: the cross-session case docs/surrealdb-bugs says drops.
  await probe('live.record_user_cross_session', async () => {
    const alice = await recordUser('ALICE');
    const live = await liveRaw(alice, 'LIVE SELECT * FROM _00_list_ref FETCH out;');
    await q(admin, 'RELATE _00_query:cflive->_00_list_ref->cf_thread:t1 SET version = 11, clientId = "live", auth_id = $aid;', { aid: ALICE_ID });
    await q(admin, 'RELATE _00_query:cflive->_00_list_ref->cf_thread:t1 SET version = 12, clientId = "live", auth_id = "user:someone_else";');
    await waitFor(() => live.messages.length > 0, 5000);
    await sleep(500);
    const versions = live.messages.map(versionOf);
    const body = outOf(live.messages[0]);
    await alice.close();
    if (versions.includes(12)) return ['fail', `received another user's edge: ${JSON.stringify(versions)}`];
    if (!versions.includes(11)) return ['known-bug', 'own edge written by another session never delivered (docs/surrealdb-bugs/live-permission-cross-session-drop.md)'];
    return ['pass', `own edge delivered with inline body ${JSON.stringify(body)}; other user's edge filtered`];
  });

  // docs/surrealdb-bugs/ws-row-cache-stale-after-update.md, standalone shape.
  await probe('regress.ws_row_fresh_after_update', async () => {
    const bob = await recordUser('BOB');
    const [before] = await q<any>(bob, 'SELECT VALUE title FROM ONLY cf_thread:t1;');
    await q(admin, "UPDATE cf_thread:t1 SET title = 'renamed-over-ws';");
    const [after] = await q<any>(bob, 'SELECT VALUE title FROM ONLY cf_thread:t1;');
    await bob.close();
    return [after === 'renamed-over-ws' ? 'pass' : 'known-bug', `bob read ${JSON.stringify(before)} then ${JSON.stringify(after)} after another session's UPDATE`];
  });

  await admin.close();
}

main()
  .catch((e) => out('live.ws', 'fail', String(e?.message ?? e), Date.now()))
  .finally(() => process.exit(0));
