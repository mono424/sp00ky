import { describe, expect, it } from 'vitest';
import {
  MemoryStore,
  NonRetryableError,
  WorkflowEntrypoint,
  budgetMs,
  retryDelayMs,
  toMs,
  withWorkflows,
  type MemoryJob,
  type WorkflowClass,
  type WorkflowEvent,
  type WorkflowStep,
} from '../src/index';

// =============================================================
// Harness: a fake clock, the memory store and a fake job runner
// =============================================================

interface Logged {
  job: string;
  status: number;
  outcome?: string;
  due_at?: string;
  error?: string;
}

function harness(classes: Record<string, WorkflowClass>, opts: { token?: string } = {}) {
  let clock = Date.UTC(2026, 9, 22, 12, 0, 0);
  const now = () => clock;
  const store = new MemoryStore(now);
  const app = async (req: Request, env: any) => {
    const url = new URL(req.url);
    if (url.pathname === '/start') {
      const body = (await req.json()) as any;
      const instance = await env[body.binding].create({ id: body.id, params: body.params });
      return Response.json({ id: instance.id });
    }
    return new Response('not found', { status: 404 });
  };
  const wf = withWorkflows(app, classes, {
    store: () => store,
    token: opts.token,
    run: {
      now,
      sleep: async (ms) => {
        clock += ms;
      },
      setInterval: () => 0,
      clearInterval: () => {},
    },
  });
  const env = wf.env({});

  const post = (job: MemoryJob, timeoutMs = 10_000, auth?: string) =>
    wf.fetch(
      new Request(`http://backend${job.path}`, {
        method: 'POST',
        body: JSON.stringify(job.payload),
        headers: { 'x-spky-job-timeout-ms': String(timeoutMs), ...(auth ? { authorization: auth } : {}) },
      }),
      {},
    );

  /**
   * Play the job runner: run due jobs oldest first, jump the clock to the next
   * one (at most `horizonMs` ahead), retry 5xx with exponential backoff.
   */
  const drain = async (o: { timeoutMs?: number; maxRuns?: number; horizonMs?: number } = {}) => {
    const log: Logged[] = [];
    for (let runs = 0; ; runs++) {
      if (runs > (o.maxRuns ?? 200)) throw new Error('too many runs');
      const pending = store.jobs.filter((j) => j.status === 'pending');
      if (pending.length === 0) return log;
      const next = Math.min(...pending.map((j) => j.createdAt + j.delay));
      if (next > clock + (o.horizonMs ?? Number.POSITIVE_INFINITY)) return log;
      if (next > clock) clock = next;
      const job = pending.find((j) => j.createdAt + j.delay <= clock)!;
      job.status = 'processing';
      const res = await post(job, o.timeoutMs);
      const body = (await res.json()) as any;
      log.push({ job: job.id, status: res.status, ...body });
      if (res.ok) {
        job.status = 'success';
      } else if (++job.retries < job.maxRetries) {
        job.status = 'pending';
        job.delay = clock - job.createdAt + 200 * 2 ** job.retries;
      } else {
        job.status = 'failed';
      }
    }
  };

  return {
    store,
    env,
    wf,
    post,
    drain,
    now,
    advance: (ms: number) => {
      clock += ms;
    },
    instance: (id: string) => store.instances.get(id)!,
    stepRows: (id: string) => [...(store.steps.get(id)?.values() ?? [])],
  };
}

function deferred<T = void>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

const tick = () => new Promise((r) => setTimeout(r, 0));

// =============================================================
// Pure helpers
// =============================================================

describe('durations and backoff', () => {
  it("parses Cloudflare's duration forms", () => {
    expect(toMs(1500)).toBe(1500);
    expect(toMs('10 seconds')).toBe(10_000);
    expect(toMs('1 second')).toBe(1_000);
    expect(toMs('2 minutes')).toBe(120_000);
    expect(toMs('1 hour')).toBe(3_600_000);
    expect(toMs('1 day')).toBe(86_400_000);
    expect(toMs('15 minutes')).toBe(900_000);
    expect(toMs('500 ms')).toBe(500);
    expect(toMs('3 days')).toBe(259_200_000);
    expect(() => toMs('soon')).toThrow(/invalid duration/);
  });

  it('computes constant, linear and exponential delays', () => {
    expect(retryDelayMs({ delay: '10 seconds', backoff: 'constant' }, 3)).toBe(10_000);
    expect(retryDelayMs({ delay: '10 seconds', backoff: 'linear' }, 3)).toBe(30_000);
    expect(retryDelayMs({ delay: '10 seconds', backoff: 'exponential' }, 1)).toBe(10_000);
    expect(retryDelayMs({ delay: '10 seconds', backoff: 'exponential' }, 3)).toBe(40_000);
  });

  it('keeps a margin below the job deadline', () => {
    expect(budgetMs(undefined)).toBe(8_000);
    expect(budgetMs(10_000)).toBe(8_000);
    expect(budgetMs(60_000)).toBe(48_000);
    expect(budgetMs(300_000)).toBe(60_000);
    expect(budgetMs(2_000)).toBe(1_000);
  });
});

// =============================================================
// Replay
// =============================================================

describe('replay', () => {
  it('runs every step once and stores the output', async () => {
    const calls: string[] = [];
    class Flow extends WorkflowEntrypoint<unknown, { n: number }> {
      async run(event: WorkflowEvent<{ n: number }>, step: WorkflowStep) {
        const a = await step.do('a', async () => (calls.push('a'), event.payload.n + 1));
        const b = await step.do('b', async () => (calls.push('b'), a * 10));
        return { a, b, id: event.instanceId };
      }
    }
    const h = harness({ FLOW: Flow });
    const inst = await h.env.FLOW.create({ id: 'one', params: { n: 1 } });
    expect(inst.id).toBe('one');
    expect(await inst.status()).toEqual({ status: 'queued' });
    await h.drain();
    expect(calls).toEqual(['a', 'b']);
    expect(await inst.status()).toEqual({ status: 'complete', output: { a: 2, b: 20, id: 'one' } });
    expect(h.instance('one').stepsDone).toBe(2);
  });

  it('hands over when the budget is spent and replays stored steps for free', async () => {
    const calls: string[] = [];
    let h!: ReturnType<typeof harness>;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        const out: number[] = [];
        for (const name of ['a', 'b', 'c']) {
          out.push(
            await step.do(name, async () => {
              calls.push(name);
              h.advance(1_500); // each step takes longer than the 1 s budget
              return out.length;
            }),
          );
        }
        return out;
      }
    }
    h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'budget' });
    const log = await h.drain({ timeoutMs: 2_000 });
    expect(log.map((l) => l.outcome)).toEqual(['suspended', 'suspended', 'complete']);
    expect(calls).toEqual(['a', 'b', 'c']);
    expect(h.instance('budget').output).toBe('[0,1,2]');
  });

  it('keys repeated names by occurrence, so loops may reuse a name', async () => {
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        const seen: number[] = [];
        for (let i = 0; i < 3; i++) seen.push(await step.do('poll', async () => i));
        return seen;
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'loop' });
    await h.drain();
    expect(h.instance('loop').output).toBe('[0,1,2]');
    expect(h.stepRows('loop').map((s) => `${s.name}#${s.n}`)).toEqual(['poll#0', 'poll#1', 'poll#2']);
  });

  it('gives the first run and every replay the same JSON round trip', async () => {
    const seen: unknown[] = [];
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        const v = await step.do('date', async () => ({ b: 1, at: new Date(0), gone: undefined }));
        seen.push(v);
        await step.sleep('later', '1 hour');
        return v;
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'json' });
    await h.drain();
    expect(seen).toEqual([
      { b: 1, at: '1970-01-01T00:00:00.000Z' },
      { b: 1, at: '1970-01-01T00:00:00.000Z' },
    ]);
  });

  it('runs parallel steps and waits for an executing sibling before handing over', async () => {
    const calls: string[] = [];
    let fails = 1;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        const [a, b] = await Promise.all([
          step.do('a', { retries: { limit: 2, delay: '10 seconds' } }, async () => {
            calls.push('a');
            if (fails-- > 0) throw new Error('flaky');
            return 'A';
          }),
          step.do('b', async () => (calls.push('b'), 'B')),
        ]);
        return a + b;
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'par' });
    const log = await h.drain();
    expect(log.map((l) => l.outcome)).toEqual(['suspended', 'complete']);
    expect(calls).toEqual(['a', 'b', 'a']);
    expect(h.instance('par').output).toBe('"AB"');
  });
});

// =============================================================
// Retries
// =============================================================

describe('retries', () => {
  it('retries with backoff through new jobs and remembers the attempt count', async () => {
    let attempts = 0;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        return step.do('flaky', { retries: { limit: 3, delay: '10 seconds', backoff: 'exponential' } }, async (ctx) => {
          attempts++;
          if (ctx.attempt < 3) throw new Error(`boom ${ctx.attempt}`);
          return ctx.attempt;
        });
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'retry' });
    await h.drain();
    expect(attempts).toBe(3);
    expect(h.store.jobs.map((j) => j.delay)).toEqual([0, 10_000, 20_000]);
    expect(h.instance('retry').output).toBe('3');
    const [row] = h.stepRows('retry');
    expect(row).toMatchObject({ status: 'success', attempts: 3 });
  });

  it('retries a short backoff inside the run', async () => {
    let attempts = 0;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        return step.do('quick', { retries: { limit: 2, delay: 500 } }, async () => {
          if (++attempts < 2) throw new Error('once');
          return 'ok';
        });
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'inline' });
    const log = await h.drain();
    expect(log.map((l) => l.outcome)).toEqual(['complete']);
    expect(attempts).toBe(2);
  });

  it('hands an exhausted step to run(), whose catch can still run steps', async () => {
    class UserError extends Error {}
    let kinds: string[] = [];
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        try {
          await step.do('doomed', { retries: { limit: 1, delay: '1 minute' } }, async () => {
            throw new UserError('nope');
          });
          return 'unreachable';
        } catch (err) {
          // The same reconstructed error on the failing run and every replay.
          kinds.push(`${(err as Error).name}:${err instanceof UserError}`);
          await step.do('mark failed', async () => 'marked');
          return { failed: (err as Error).message };
        }
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'exhausted' });
    await h.drain();
    expect(h.instance('exhausted').status).toBe('complete');
    expect(h.instance('exhausted').output).toBe('{"failed":"nope"}');
    expect(kinds).toEqual(['Error:false']);
    expect(h.stepRows('exhausted').find((s) => s.name === 'doomed')).toMatchObject({ status: 'failed', attempts: 2 });
  });

  it('does not retry a NonRetryableError and errors the instance when uncaught', async () => {
    let attempts = 0;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        await step.do('fatal', async () => {
          attempts++;
          throw new NonRetryableError('bad input');
        });
      }
    }
    const h = harness({ FLOW: Flow });
    const inst = await h.env.FLOW.create({ id: 'fatal' });
    await h.drain();
    expect(attempts).toBe(1);
    expect(await inst.status()).toEqual({ status: 'errored', error: { name: 'NonRetryableError', message: 'bad input' } });
  });

  it('fails a step whose result is over the size limit, without retrying', async () => {
    let attempts = 0;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        return step.do('big', async () => (attempts++, 'x'.repeat(1024 * 1024 + 1)));
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'big' });
    await h.drain();
    expect(attempts).toBe(1);
    expect(h.instance('big').status).toBe('errored');
    expect(h.instance('big').error?.message).toMatch(/store it in a bucket/);
  });
});

// =============================================================
// Sleeps and events
// =============================================================

describe('sleep and waitForEvent', () => {
  it('sleeps through a delayed job, and short sleeps inline', async () => {
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        await step.sleep('short', 1_000);
        await step.sleep('long', '1 hour');
        return 'awake';
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'sleepy' });
    const start = h.now();
    const log = await h.drain();
    expect(log.map((l) => l.outcome)).toEqual(['suspended', 'complete']);
    expect(h.store.jobs.map((j) => j.delay)).toEqual([0, 3_600_000]);
    expect(h.now() - start).toBeGreaterThanOrEqual(3_601_000);
    expect(h.instance('sleepy').output).toBe('"awake"');
  });

  it('consumes an event sent before the wait, and one sent during it', async () => {
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        const first = await step.waitForEvent<{ n: number }>('first', { type: 'ping' });
        const second = await step.waitForEvent<{ n: number }>('second', { type: 'ping', timeout: '1 hour' });
        return [first.payload.n, second.payload.n, first.type, first.timestamp instanceof Date];
      }
    }
    const h = harness({ FLOW: Flow });
    const inst = await h.env.FLOW.create({ id: 'events' });
    await inst.sendEvent({ type: 'ping', payload: { n: 1 } });
    const first = await h.drain({ horizonMs: 0 });
    expect(first.map((l) => l.outcome)).toEqual(['stale', 'suspended']);
    await inst.sendEvent({ type: 'ping', payload: { n: 2 } });
    await h.drain();
    expect(h.instance('events').output).toBe('[1,2,"ping",true]');
    expect(h.store.events.every((e) => e.step !== undefined)).toBe(true);
  });

  it('throws when the event does not come in time', async () => {
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        try {
          await step.waitForEvent('approval', { type: 'approved', timeout: '1 hour' });
          return 'approved';
        } catch (e) {
          return `timeout:${(e as Error).name}`;
        }
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'late' });
    await h.drain();
    expect(h.instance('late').output).toBe('"timeout:WorkflowTimeoutError"');
  });
});

// =============================================================
// Concurrency, crashes and fencing
// =============================================================

describe('one run at a time', () => {
  it('answers 503 to a second job while a run holds the lease, then stale', async () => {
    const gate = deferred();
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        await step.do('slow', async () => gate.promise);
        await step.sleep('nap', '1 hour');
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'busy' });
    const job = h.store.jobs[0];
    const first = h.post(job);
    await tick();
    const second = await h.post(job);
    expect(second.status).toBe(503);
    expect(await second.json()).toMatchObject({ outcome: 'busy' });
    gate.resolve();
    expect(await (await first).json()).toMatchObject({ outcome: 'suspended' });
    // The first job's token is spent: a late retry of it does nothing.
    const late = await h.post(job);
    expect(late.status).toBe(200);
    expect(await late.json()).toMatchObject({ outcome: 'stale' });
  });

  it('runs a step again after a crash between the step and its persist', async () => {
    let calls = 0;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        return step.do('charge', async () => ++calls);
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'crash' });
    h.store.failNext.add('saveStep');
    const log = await h.drain();
    // 500 for the lost write, 503 while the dead run's lease lasts, then the replay.
    expect(log[0]).toMatchObject({ status: 500 });
    expect(log.slice(1, -1).every((l) => l.status === 503)).toBe(true);
    expect(log.at(-1)).toMatchObject({ status: 200, outcome: 'complete' });
    expect(calls).toBe(2);
    expect(h.stepRows('crash')).toHaveLength(1);
    expect(h.instance('crash').output).toBe('2');
  });

  it('fences a hung run once another job took the instance over', async () => {
    const gate = deferred<string>();
    let calls = 0;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        return step.do('work', async () => (++calls === 1 ? gate.promise : 'second'));
      }
    }
    const h = harness({ FLOW: Flow });
    await h.env.FLOW.create({ id: 'hung' });
    const job = h.store.jobs[0];
    const hung = h.post(job);
    await tick();
    h.advance(31_000); // no renewals in the harness: the lease runs out
    const takeover = await h.post(job);
    expect(await takeover.json()).toMatchObject({ outcome: 'complete' });
    gate.resolve('first');
    expect(await (await hung).json()).toMatchObject({ outcome: 'fenced' });
    expect(h.instance('hung').output).toBe('"second"');
    expect(h.stepRows('hung')[0]).toMatchObject({ status: 'success', output: '"second"' });
  });

  it('stops a run that is terminated under it', async () => {
    const gate = deferred();
    let after = 0;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        await step.do('slow', async () => gate.promise);
        await step.do('after', async () => ++after);
      }
    }
    const h = harness({ FLOW: Flow });
    const inst = await h.env.FLOW.create({ id: 'term' });
    const run = h.post(h.store.jobs[0]);
    await tick();
    await inst.terminate();
    gate.resolve();
    expect(await (await run).json()).toMatchObject({ outcome: 'fenced' });
    expect(after).toBe(0);
    expect(await inst.status()).toEqual({ status: 'terminated' });
    // No job was scheduled after the create job: nothing will run it again.
    expect(h.store.jobs).toHaveLength(1);
  });

  it('wakes at once when an event arrives while the run holds the lease', async () => {
    const gate = deferred();
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        await step.do('work', async () => gate.promise);
        await step.sleep('nap', '1 hour');
      }
    }
    const h = harness({ FLOW: Flow });
    const inst = await h.env.FLOW.create({ id: 'nudge' });
    const run = h.post(h.store.jobs[0]);
    await tick();
    await inst.sendEvent({ type: 'poke' });
    const nudge = h.store.jobs.at(-1)!;
    expect((await h.post(nudge)).status).toBe(503);
    gate.resolve();
    await run;
    // The run saw the token change under it and scheduled itself now, not in an hour.
    expect(h.store.jobs.at(-1)!.delay).toBe(0);
  });

  it('restarts from scratch with a fresh token', async () => {
    let calls = 0;
    class Flow extends WorkflowEntrypoint {
      async run(_e: WorkflowEvent, step: WorkflowStep) {
        return step.do('count', async () => ++calls);
      }
    }
    const h = harness({ FLOW: Flow });
    const inst = await h.env.FLOW.create({ id: 'again' });
    await h.drain();
    await inst.restart();
    expect((await inst.status()).status).toBe('queued');
    await h.drain();
    expect(calls).toBe(2);
    expect(await inst.status()).toEqual({ status: 'complete', output: 2 });
  });
});

// =============================================================
// The wrapper
// =============================================================

describe('withWorkflows', () => {
  class Noop extends WorkflowEntrypoint {
    async run() {
      return 'ok';
    }
  }

  it('puts the binding on env for the app and creates through it', async () => {
    const h = harness({ NOOP: Noop });
    const res = await h.wf.fetch(
      new Request('http://backend/start', { method: 'POST', body: JSON.stringify({ binding: 'NOOP', id: 'from-app' }) }),
      {},
    );
    expect(await res.json()).toEqual({ id: 'from-app' });
    expect(h.store.jobs[0]).toMatchObject({ outbox: 'job', path: '/__spky/workflow/NOOP', delay: 0 });
    await expect(h.env.NOOP.create({ id: 'from-app' })).rejects.toThrow(/already exists/);
    await expect(h.env.NOOP.create({ id: 'bad id!' })).rejects.toThrow(/invalid workflow instance id/);
    await expect(h.env.NOOP.get('missing')).rejects.toThrow(/not found/);
  });

  it('requires the bearer token when one is configured', async () => {
    const h = harness({ NOOP: Noop }, { token: 'secret' });
    await h.env.NOOP.create({ id: 't' });
    const job = h.store.jobs[0];
    expect((await h.post(job)).status).toBe(401);
    expect((await h.post(job, 10_000, 'Bearer secret')).status).toBe(200);
  });

  it('answers unknown bindings and bad bodies without touching the store', async () => {
    const h = harness({ NOOP: Noop });
    const post = (path: string, body: string) => h.wf.fetch(new Request(`http://b${path}`, { method: 'POST', body }), {});
    expect((await post('/__spky/workflow/NOPE', '{}')).status).toBe(404);
    expect((await post('/__spky/workflow/NOOP', 'nope')).status).toBe(400);
    expect((await post('/__spky/workflow/NOOP', '{"instance":1}')).status).toBe(400);
  });
});
