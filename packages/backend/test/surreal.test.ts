// The SurrealDB store against a real server. Skipped unless
// SPKY_TEST_SURREAL_URL points at one (root/root), for example:
//
//   docker run -d --rm --name wf-test-surreal -p 18920:8000 surrealdb/surrealdb:v3.1.5 start --user root --pass root memory
//   SPKY_TEST_SURREAL_URL=http://127.0.0.1:18920 pnpm --filter @spooky-sync/backend test
//
// Each run gets its own database with the shipped internal schema
// (apps/cli/src/schedule_tables.surql) and the example app's outbox table.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { beforeAll, describe, expect, it } from 'vitest';
import {
  FencedError,
  SurrealStore,
  WorkflowEntrypoint,
  httpQuery,
  rpcUrl,
  withWorkflows,
  type Query,
  type StepRow,
  type WorkflowEvent,
  type WorkflowStep,
} from '../src/index';

const URL_ = process.env.SPKY_TEST_SURREAL_URL;

describe('httpQuery', () => {
  /** A fake /rpc endpoint: answers each request with the next queued body. */
  const fakeFetch = (bodies: unknown[], seen: any[] = []) =>
    (async (_url: any, init: any) => {
      seen.push(JSON.parse(init.body));
      return new Response(JSON.stringify(bodies.shift()), { status: 200 });
    }) as typeof fetch;

  it('normalizes the endpoint and sends binds as LET statements', async () => {
    expect(rpcUrl('ws://db:8000/rpc')).toBe('http://db:8000/rpc');
    expect(rpcUrl('https://db.example.com/')).toBe('https://db.example.com/rpc');
    const seen: any[] = [];
    const q = httpQuery(
      { url: 'http://db', ns: 'n', db: 'd' },
      fakeFetch([{ result: [{ status: 'OK', result: null }, { status: 'OK', result: 7 }] }], seen),
    );
    expect(await q('RETURN $x;', { x: 'user:abc' })).toEqual([7]);
    expect(seen[0].params[0]).toBe('LET $x = "user:abc";\nRETURN $x;');
  });

  it('retries a request that lost a write race, and reports a fence', async () => {
    const conflict = { result: [{ status: 'ERR', result: 'Transaction conflict: Resource busy. This transaction can be retried' }] };
    const q = httpQuery(
      { url: 'http://db', ns: 'n', db: 'd' },
      fakeFetch([conflict, conflict, { result: [{ status: 'OK', result: 'ok' }] }]),
    );
    expect(await q('UPDATE x:1 SET a = 1;')).toEqual(['ok']);
    const fenced = httpQuery(
      { url: 'http://db', ns: 'n', db: 'd' },
      fakeFetch([{ result: [{ status: 'ERR', result: 'An error occurred: spky:fenced' }] }]),
    );
    await expect(fenced('x')).rejects.toBeInstanceOf(FencedError);
  });
});
const root = (p: string) => fileURLToPath(new URL(`../../../${p}`, import.meta.url));

describe.skipIf(!URL_)('SurrealStore on SurrealDB', () => {
  const ns = 'spky_test';
  const db = `wf_${Date.now().toString(36)}`;
  let q: Query;
  let store: SurrealStore;

  beforeAll(async () => {
    const admin = httpQuery({ url: URL_!, ns, db, user: 'root', pass: 'root' });
    await admin(`DEFINE NAMESPACE IF NOT EXISTS ${ns}; USE NS ${ns}; DEFINE DATABASE IF NOT EXISTS ${db};`);
    q = httpQuery({ url: URL_!, ns, db, user: 'root', pass: 'root' });
    await q(readFileSync(root('apps/cli/src/schedule_tables.surql'), 'utf8'));
    await q(readFileSync(root('example/schema/src/outbox/api.surql'), 'utf8'));
    store = new SurrealStore(q);
  });

  const create = (id: string, wake = `w${id}`, params?: unknown) =>
    store.create({
      id,
      binding: 'FLOW',
      className: 'Flow',
      outbox: 'job',
      path: '/__spky/workflow/FLOW',
      params: params === undefined ? undefined : JSON.stringify(params),
      wake,
    });

  it('creates the instance and its first job in the outbox', async () => {
    await create('c1', 'tok1', { id: 'user:abc', at: '2026-10-22T10:00:00Z', s: 'é "q" \\ \n' });
    const [job] = (await q(`SELECT * FROM job:dwf_tok1;`)) as any[][];
    expect(job[0]).toMatchObject({
      path: '/__spky/workflow/FLOW',
      payload: { instance: 'c1', wake: 'tok1' },
      delay: 0,
      max_retries: 10,
      retry_strategy: 'exponential',
      status: 'pending',
    });
    const claim = await store.claim('c1', 'tok1', 30_000);
    expect(claim.kind).toBe('claimed');
    if (claim.kind !== 'claimed') return;
    // Strings shaped like a record id or a datetime stay strings.
    expect(JSON.parse(claim.instance.params!)).toEqual({ id: 'user:abc', at: '2026-10-22T10:00:00Z', s: 'é "q" \\ \n' });
    expect(Math.abs(claim.dbNow - Date.now())).toBeLessThan(5_000);
    await expect(create('c1')).rejects.toThrow(/already exists/);
  });

  it('claims once: busy while leased, stale for an old token, done when finished', async () => {
    await create('c2', 'a');
    const first = await store.claim('c2', 'a', 30_000);
    expect(first.kind).toBe('claimed');
    expect((await store.claim('c2', 'a', 30_000)).kind).toBe('busy');
    expect((await store.claim('c2', 'old', 30_000)).kind).toBe('stale');
    expect((await store.claim('nope', 'a', 30_000)).kind).toBe('gone');
    if (first.kind !== 'claimed') return;
    const step: StepRow = { name: 's', n: 0, kind: 'do', status: 'success', attempts: 1, output: '{"b":1,"a":2}', startedAt: Date.now() };
    await store.saveStep('c2', first.epoch, step, { stepsDone: 1, currentStep: 's' });
    await expect(store.saveStep('c2', first.epoch + 1, step, { stepsDone: 1 })).rejects.toBeInstanceOf(FencedError);
    const [loaded] = await store.loadSteps('c2');
    // Output is text: the key order survives.
    expect(loaded).toMatchObject({ name: 's', n: 0, status: 'success', attempts: 1, output: '{"b":1,"a":2}' });
    await store.finish('c2', first.epoch, { status: 'complete', output: '"ok"' }, { stepsDone: 1 }, Date.now());
    expect((await store.claim('c2', 'a', 30_000)).kind).toBe('done');
    expect(await store.status('c2')).toEqual({ status: 'complete', output: '"ok"' });
  });

  it('hands over with a new token and a delayed job, due now when nudged', async () => {
    await create('c3', 'x');
    const claim = await store.claim('c3', 'x', 30_000);
    if (claim.kind !== 'claimed') throw new Error(claim.kind);
    await store.finish('c3', claim.epoch, { status: 'suspend', dueAt: Date.now() + 60_000, claimedWake: 'x', newWake: 'y' }, { stepsDone: 0 }, Date.now());
    const [[late]] = (await q(`SELECT delay, payload FROM job:dwf_y;`)) as any[][];
    expect(late.delay).toBeGreaterThan(55_000);
    expect(late.payload).toEqual({ instance: 'c3', wake: 'y' });

    const again = await store.claim('c3', 'y', 30_000);
    if (again.kind !== 'claimed') throw new Error(again.kind);
    expect(await store.sendEvent('c3', 'poke', '{"n":1}', 'z')).toBe(true);
    await store.finish('c3', again.epoch, { status: 'suspend', dueAt: Date.now() + 60_000, claimedWake: 'y', newWake: 'w' }, { stepsDone: 0 }, Date.now());
    const [[nudged]] = (await q(`SELECT delay FROM job:dwf_w;`)) as any[][];
    expect(nudged.delay).toBe(0);
    expect(await store.findEvent('c3', 'poke')).toMatchObject({ type: 'poke', payload: '{"n":1}' });
    expect(await store.terminate('c3', 't')).toBe(true);
    expect((await store.claim('c3', 'w', 30_000)).kind).toBe('done');
  });

  it('runs a workflow end to end through the outbox table', async () => {
    let flaky = 1;
    class Flow extends WorkflowEntrypoint<any, { n: number }> {
      async run(event: WorkflowEvent<{ n: number }>, step: WorkflowStep) {
        const a = await step.do('a', async () => event.payload.n + 1);
        const b = await step.do('b', { retries: { limit: 2, delay: 300 } }, async () => {
          if (flaky-- > 0) throw new Error('flaky');
          return a * 2;
        });
        await step.sleep('nap', '3 seconds');
        const ev = await step.waitForEvent<{ ok: boolean }>('approve', { type: 'approved', timeout: '1 minute' });
        return { a, b, ok: ev.payload.ok };
      }
    }
    const wf = withWorkflows(() => new Response('app'), { FLOW: Flow }, { query: () => q, outbox: 'job' });
    const env = wf.env({});
    const inst = await env.FLOW.create({ id: 'e2e', params: { n: 1 } });
    await inst.sendEvent({ type: 'approved', payload: { ok: true } });
    const log = await drainOutbox(q, wf.fetch, 'e2e', 20_000);
    expect(await inst.status()).toEqual({ status: 'complete', output: { a: 2, b: 4, ok: true } });
    expect(log.map((l) => l.outcome)).toEqual(['stale', 'suspended', 'complete']);
    const steps = await store.loadSteps('e2e');
    expect(steps.map((s) => `${s.name}:${s.status}:${s.attempts}`).sort()).toEqual([
      'a:success:1',
      'approve:success:0',
      'b:success:2',
      'nap:success:0',
    ]);
  }, 30_000);

  it('answers 503 while another run holds the lease', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    class Slow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        return step.do('slow', async () => (await gate, 'done'));
      }
    }
    const wf = withWorkflows(() => new Response('app'), { SLOW: Slow }, { query: () => q, outbox: 'job' });
    const inst = await wf.env({}).SLOW.create({ id: 'busy' });
    const [[job]] = (await q(`SELECT payload FROM job WHERE payload.instance = 'busy';`)) as any[][];
    const post = () =>
      wf.fetch(new Request('http://b/__spky/workflow/SLOW', { method: 'POST', body: JSON.stringify(job.payload) }), {});
    const first = post();
    await new Promise((r) => setTimeout(r, 300));
    expect((await post()).status).toBe(503);
    release();
    expect(await (await first).json()).toMatchObject({ outcome: 'complete' });
    expect(await inst.status()).toEqual({ status: 'complete', output: 'done' });
  });
});

interface Logged {
  status: number;
  outcome?: string;
}

/** A minimal job runner over one instance's rows in `job`: due pending rows oldest first, 5xx retried. */
async function drainOutbox(q: Query, fetchFn: (r: Request, env?: any) => Promise<Response>, instance: string, maxMs: number) {
  const log: Logged[] = [];
  const end = Date.now() + maxMs;
  while (Date.now() < end) {
    const [due, [pending]] = (await q(
      `SELECT id, path, payload, retries, created_at FROM job
       WHERE status = 'pending' AND payload.instance = $instance
         AND (created_at + <duration>(string::concat(<string>(delay ?? 0), 'ms'))) <= time::now()
       ORDER BY created_at ASC LIMIT 1;
       SELECT count() AS n FROM job WHERE status = 'pending' AND payload.instance = $instance GROUP ALL;`,
      { instance },
    )) as any[][];
    const job = due[0];
    if (!job) {
      if (!pending?.n) return log;
      await new Promise((r) => setTimeout(r, 100));
      continue;
    }
    await q(`UPDATE type::record($id) SET status = 'processing';`, { id: job.id });
    const res = await fetchFn(
      new Request(`http://backend${job.path}`, {
        method: 'POST',
        body: JSON.stringify(job.payload),
        headers: { 'x-spky-job-timeout-ms': '10000' },
      }),
      {},
    );
    const body = (await res.json()) as any;
    log.push({ status: res.status, outcome: body.outcome });
    if (res.ok) {
      await q(`UPDATE type::record($id) SET status = 'success';`, { id: job.id });
    } else {
      const delay = Date.now() - Date.parse(job.created_at) + 200 * 2 ** (job.retries + 1);
      await q(`UPDATE type::record($id) SET status = 'pending', delay = $delay;`, { id: job.id, delay });
    }
  }
  throw new Error(`outbox not drained in ${maxMs} ms: ${JSON.stringify(log)}`);
}
