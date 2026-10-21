import { createServer, type Server } from 'node:http';
import { Socket } from 'node:net';
import { readFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { RecordId, Surreal } from 'surrealdb';
import { liveEndpoint } from '../live/index';
import { defaultEnv } from '../query/env';
import { RemoteDatabaseService } from '../services/database/remote';
import { buildState } from '../testing/build';
import { runPure } from '../testing/run-pure';
import type { ClientState } from '../state/client-state';
import type { OutEvent } from '../kernel/events';
import type { StatementResult } from '../kernel/effects';
import { mintMutationId } from './mutation-id';
import { drain } from './push.saga';
import { BATCH_SKIPPED } from './rows';
import { write } from './write.saga';

/**
 * Integration: a queued write that SurrealDB refuses because sp00ky's OWN
 * ingest post failed (scheduler/SSP down, slow past the event TIMEOUT, or
 * answering 5xx) must stay in the outbox and land once `/ingest` is back. It
 * used to read as an application error, so the client rolled the write back
 * and discarded it: silent data loss on every scheduler outage.
 *
 * Runs the real `write` and `drain` sagas, the client's real remote adapter,
 * and the `http::post` line `spky migrate` generates (read from the Rust
 * source, so the two cannot drift) against a real SurrealDB, with an HTTP sink
 * in this process playing the scheduler. Boot one first, e.g.:
 *   docker run -d --name spooky-surreal-it -p 18011:8000 \
 *     surrealdb/surrealdb:v3.1.5 start --user root --pass root --allow-all memory
 *
 * Override the endpoint with SURREAL_IT_ENDPOINT and the host SurrealDB reaches
 * this process at with SURREAL_IT_SINK_HOST (default `host.docker.internal`;
 * `127.0.0.1` for a SurrealDB running on the host). Skips if unreachable.
 */
const endpoint = liveEndpoint(process.env.SURREAL_IT_ENDPOINT ?? 'ws://127.0.0.1:18011');
const sinkHost = process.env.SURREAL_IT_SINK_HOST ?? 'host.docker.internal';
const NS = 'it';
/** Short so the hang case stays quick; the shipped value is 10s. */
const EVENT_TIMEOUT_SECS = 2;

const cliSource = (file: string) =>
  readFileSync(fileURLToPath(new URL(`../../../../apps/cli/src/${file}`, import.meta.url)), 'utf8');

/** The `/ingest` post every generated event makes (`sp00ky.rs::ingest_post`). */
function ingestPost(): string {
  const src = cliSource('sp00ky.rs');
  const m = /fn ingest_post\(\) -> String \{\s*format!\(\s*"(.+?)\\n",/s.exec(src);
  if (!m) throw new Error('ingest_post format string not found');
  if (!src.includes('"DEFINE EVENT OVERWRITE _00_{}_mutation ON TABLE {}\\n"')) {
    throw new Error('the mutation event is no longer named _00_<table>_mutation');
  }
  return m[1]
    .replaceAll('\\"', '"')
    .replaceAll('{{', '{')
    .replaceAll('}}', '}')
    .replace('{}', String(EVENT_TIMEOUT_SECS));
}

/** `flap` answers the next post 503, then switches itself back to `ok`. */
type SinkMode = 'ok' | '500' | '503' | 'hang' | 'flap';

/** Plays the scheduler's `/ingest` (and an app's own third-party endpoint). */
function startSink(): Promise<{ server: Server; port: number; mode: { current: SinkMode }; hits: string[] }> {
  const mode = { current: 'ok' as SinkMode };
  const hits: string[] = [];
  const server = createServer((req, res) => {
    req.resume();
    hits.push(`${req.method} ${req.url} ${mode.current}`);
    if (mode.current === 'hang') return; // never answers; SurrealDB's TIMEOUT fires
    if (mode.current === 'flap') {
      mode.current = 'ok';
      res.writeHead(503);
      res.end();
      return;
    }
    if (mode.current === 'ok') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end('{}');
      return;
    }
    res.writeHead(Number(mode.current));
    res.end();
  });
  return new Promise((resolve) =>
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      resolve({ server, port: typeof address === 'object' && address ? address.port : 0, mode, hits });
    })
  );
}

/** A port nothing listens on: a refused connection. */
async function closedPort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return typeof address === 'object' && address ? address.port : 1;
}

const silent: any = { debug() {}, info() {}, warn() {}, error() {}, trace() {} };
silent.child = () => silent;

const env = defaultEnv({
  tables: [
    { name: 'item', columns: { title: { type: 'string', optional: true } } },
    { name: 'lead', columns: { title: { type: 'string', optional: true } } },
  ],
} as any);

describe('a write refused by a failed ingest post is retried, not rolled back (real SurrealDB)', () => {
  let reachable = false;
  let local: Surreal;
  let remote: RemoteDatabaseService;
  let sink: Awaited<ReturnType<typeof startSink>>;
  let sinkUrl = '';
  let database = '';
  /** Every per-statement error the remote answered, in order. */
  const statementErrors: string[] = [];

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
    if (!reachable) return;

    sink = await startSink();
    sinkUrl = `http://${sinkHost}:${sink.port}`;

    // The wasm engine loads its binary with `fetch(new URL(..., import.meta.url))`,
    // and Node's fetch has no `file:` scheme: serve that one read from disk.
    const realFetch = globalThis.fetch;
    vi.stubGlobal('fetch', async (input: unknown, init?: RequestInit) => {
      const target = input instanceof URL ? input : new URL(String(input));
      if (target.protocol === 'file:') return new Response(await readFile(fileURLToPath(target)));
      return realFetch(input as RequestInfo, init);
    });
    const { createWasmEngines } = await import('@surrealdb/wasm');
    local = new Surreal({ engines: createWasmEngines() });
    await local.connect('mem://');
    await local.use({ namespace: 'n', database: 'd' });
    await local.query('DEFINE TABLE item SCHEMALESS; DEFINE TABLE lead SCHEMALESS; DEFINE TABLE _00_pending_mutations SCHEMALESS;');

    database = `ingest_outage_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    remote = new RemoteDatabaseService({ endpoint, namespace: NS, database } as any, silent);
    await remote.connect();
    await remote.signin({ username: 'root', password: 'root' });
    await remote.query(`
      DEFINE PARAM OVERWRITE $sp00ky_endpoint VALUE '${sinkUrl}' PERMISSIONS NONE;
      DEFINE PARAM OVERWRITE $sp00ky_secret VALUE 'it-secret' PERMISSIONS NONE;
      DEFINE TABLE item SCHEMALESS;
      DEFINE EVENT OVERWRITE _00_item_mutation ON TABLE item
      WHEN $before != $after AND $event != "DELETE"
      THEN {
          LET $payload = { table: 'item', op: $event, id: <string>$after.id, record: $after, hash: "" };
      ${ingestPost()}
      };
      DEFINE TABLE lead SCHEMALESS;
    `);
  }, 60_000);

  afterAll(async () => {
    if (!reachable) return;
    sink.mode.current = 'ok';
    await remote?.query(`REMOVE DATABASE ${database}`).catch(() => undefined);
    await remote?.close().catch(() => undefined);
    await local?.close().catch(() => undefined);
    sink?.server.closeAllConnections();
    await new Promise<void>((resolve) => sink?.server.close(() => resolve()));
    vi.unstubAllGlobals();
  });

  const run = (saga: Generator<any, any, any>, state: ClientState) =>
    runPure(saga, {
      state,
      now: Date.now(),
      handlers: {
        'local.query': (e: any) => local.query(e.sql, e.vars) as unknown as Promise<unknown>,
        'local.execute': async (e: any) => e.query.extract((await local.query(e.query.sql, e.vars)) as unknown[]),
        'remote.query': async (e: any) => {
          const results = (await remote.queryResponses(e.sql, e.vars)) as StatementResult[];
          for (const r of results) if (r.status === 'ERR') statementErrors.push(r.error);
          return results;
        },
        'ssp.ingest': () => undefined,
        id: () => mintMutationId(state.tabId),
      },
    });

  const remoteRow = async (id: RecordId<string>) =>
    ((await remote.query('SELECT * FROM ONLY $id', { id })) as [Record<string, unknown> | null])[0] ?? null;
  const localRow = async (id: RecordId<string>) =>
    ((await local.query('SELECT * FROM ONLY $id', { id })) as [Record<string, unknown> | null])[0] ?? null;
  const rolledBack = (emitted: OutEvent[]) => emitted.filter((e) => e.type === 'mutation:rolled-back');
  const rid = (recordId: string) => new RecordId('item', recordId.slice('item:'.length));
  const setEndpoint = (url: string) =>
    remote.query(`DEFINE PARAM OVERWRITE $sp00ky_endpoint VALUE '${url}' PERMISSIONS NONE`);

  /** Queue creates locally, the way `db.create` does, and return the state with them in the outbox. */
  async function queueCreate(...recordIds: string[]): Promise<ClientState> {
    let state = buildState();
    for (const recordId of recordIds) {
      state = (await run(write(env, { kind: 'create', recordId, data: { title: recordId } }), state)).state;
    }
    expect(state.outbox).toHaveLength(recordIds.length);
    return state;
  }

  it.for([
    ['a refused connection', 'refused'],
    ['a post that hangs past the event TIMEOUT', 'hang'],
    ['a 500 from /ingest', '500'],
    ['a 503 from a cold SSP', '503'],
  ] as const)('%s: the write stays queued, then lands once /ingest answers', { timeout: 30_000 }, async ([, failure], ctx) => {
    if (!reachable) ctx.skip();
    const id = new RecordId('item', `outage_${failure}`);
    const state = await queueCreate(`item:outage_${failure}`);
    const errorsBefore = statementErrors.length;

    await setEndpoint(failure === 'refused' ? `http://${sinkHost}:${await closedPort()}` : sinkUrl);
    sink.mode.current = failure === 'refused' ? 'ok' : failure;
    const failed = await run(drain(env), state);

    const seen = statementErrors.slice(errorsBefore);
    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatch(
      /Error while processing event _00_item_mutation: There was an error processing a remote HTTP request: /
    );
    expect(rolledBack(failed.emitted)).toEqual([]);
    expect(failed.state.outbox).toEqual([expect.objectContaining({ recordId: `item:outage_${failure}`, attempts: 1 })]);
    expect(failed.timers.get('outbox')).toEqual({ ms: expect.any(Number), event: { type: 'Drain' } });
    expect(await localRow(id)).toMatchObject({ title: `item:outage_${failure}` });
    expect(await remoteRow(id)).toBeNull();

    // The scheduler is back: the retry lands the write exactly once.
    await setEndpoint(sinkUrl);
    sink.mode.current = 'ok';
    const landed = await run(drain(env), failed.state);
    expect(rolledBack(landed.emitted)).toEqual([]);
    expect(landed.emitted).toContainEqual(expect.objectContaining({ type: 'mutation:settled', recordId: `item:outage_${failure}` }));
    expect(landed.state.outbox.filter((i) => i.status === 'pending')).toEqual([]);
    expect(await remoteRow(id)).toMatchObject({ title: `item:outage_${failure}` });
  });

  it('an ingest post that fails once mid-batch: the batch stops there, then every write lands once', { timeout: 30_000 }, async (ctx) => {
    if (!reachable) ctx.skip();
    const tag = Math.random().toString(36).slice(2, 8);
    const [a, b] = [`item:flap_a_${tag}`, `item:flap_b_${tag}`];
    const state = await queueCreate(a, b);
    const errorsBefore = statementErrors.length;

    // The first post (item a's) answers 503, every later one 200. Statements
    // used to run independently, so b committed while a was refused, stayed
    // queued behind a's cut, and its re-sent CREATE then failed "already
    // exists" and was rolled back locally while the server kept the row.
    await setEndpoint(sinkUrl);
    sink.mode.current = 'flap';
    const cut = await run(drain(env), state);
    const seen = statementErrors.slice(errorsBefore);
    expect(seen).toHaveLength(2);
    expect(seen[0]).toMatch(/Error while processing event _00_item_mutation: .*503 Service Unavailable/);
    expect(seen[1]).toContain(BATCH_SKIPPED);
    expect(rolledBack(cut.emitted)).toEqual([]);
    expect(cut.state.outbox.map((i) => [i.recordId, i.status])).toEqual([
      [a, 'pending'],
      [b, 'pending'],
    ]);
    expect(await remoteRow(rid(a))).toBeNull();
    expect(await remoteRow(rid(b))).toBeNull();

    const landed = await run(drain(env), cut.state);
    expect(statementErrors.slice(errorsBefore + 2)).toEqual([]);
    expect(rolledBack(landed.emitted)).toEqual([]);
    expect(landed.emitted.filter((e) => e.type === 'mutation:settled').map((e: any) => e.recordId)).toEqual([a, b]);
    expect(landed.state.outbox.filter((i) => i.status === 'pending')).toEqual([]);
    expect(landed.state.failedCount).toBe(0);
    for (const id of [a, b]) {
      expect(await remoteRow(rid(id))).toMatchObject({ title: id });
      expect(await localRow(rid(id))).toMatchObject({ title: id });
    }
  });

  it.for([
    [
      "a failing http::post in the app's own event",
      `DEFINE EVENT OVERWRITE notify_crm ON TABLE lead THEN { SELECT * FROM http::post('${'SINK'}/crm', {}) TIMEOUT 2s; };`,
      /Error while processing event notify_crm: There was an error processing a remote HTTP request: 500/,
    ],
    [
      "a THROW from the app's own event",
      `DEFINE EVENT OVERWRITE notify_crm ON TABLE lead THEN { THROW 'lead refused'; };`,
      /lead refused/,
    ],
  ] as const)('%s still rolls the write back', { timeout: 30_000 }, async ([, ddl, expected], ctx) => {
    if (!reachable) ctx.skip();
    await setEndpoint(sinkUrl);
    await remote.query(ddl.replace('SINK', sinkUrl));
    sink.mode.current = '500';
    const recordId = `lead:${Math.random().toString(36).slice(2, 8)}`;
    const state = await queueCreate(recordId);
    const res = await run(drain(env), state);
    sink.mode.current = 'ok';
    expect(rolledBack(res.emitted)).toEqual([expect.objectContaining({ recordId, error: expect.stringMatching(expected) })]);
    expect(res.state.outbox).toEqual([]);
  });
});
