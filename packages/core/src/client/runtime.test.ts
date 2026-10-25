import { describe, expect, it } from 'vitest';
import { fakeAdapters } from '../testing/adapters';
import { buildEntry, buildState } from '../testing/build';
import { defaultEnv } from '../query/env';
import { fx } from '../kernel/effects';
import type { Saga } from '../kernel/saga';
import * as R from '../state/reducers';
import { Runtime } from './runtime';
import { rebindQueries } from '../boot/bucket-switch.saga';
import { registerLocal } from '../query/register.saga';

const env = defaultEnv({ tables: [] } as any, { materializeDebounceMs: 5 });
const noop = () => {};
const logs: unknown[] = [];
const logger: any = { debug: noop, info: noop, warn: noop, trace: noop, error: (o: unknown) => logs.push(o), child: () => logger };
const make = (over: Parameters<typeof fakeAdapters>[0] = {}, initialState = buildState()) => {
  const a = fakeAdapters(over);
  const rt = new Runtime({ env, adapters: a.adapters, logger, tabId: 'tab-a', initialState });
  return { a, rt };
};
const tick = () => new Promise((r) => setTimeout(r, 0));

describe('Runtime lanes', () => {
  it('serial lanes run one at a time in order; dedupe lanes join the running saga', async () => {
    const { rt } = make();
    const order: string[] = [];
    function* step(name: string, wait: Promise<void>): Saga<string> {
      order.push(`${name}:start`);
      yield fx.state.wait(() => true);
      await0(wait);
      order.push(`${name}:end`);
      return name;
    }
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const await0 = (_p: Promise<void>) => undefined;
    const a = rt.run(step('a', gate), { lane: { kind: 'serial', key: 'x' } });
    const b = rt.run(step('b', gate), { lane: { kind: 'serial', key: 'x' } });
    release();
    expect(await a).toBe('a');
    expect(await b).toBe('b');
    expect(order).toEqual(['a:start', 'a:end', 'b:start', 'b:end']);
    let n = 0;
    function* counted(): Saga<number> {
      n++;
      yield fx.state.wait(() => true);
      return n;
    }
    const d1 = rt.run(counted(), { lane: { kind: 'dedupe', key: 'd' } });
    const d2 = rt.run(counted(), { lane: { kind: 'dedupe', key: 'd' } });
    expect(await d1).toBe(1);
    expect(await d2).toBe(1);
    expect(n).toBe(1);
    const d3 = await rt.run(counted(), { lane: { kind: 'dedupe', key: 'd' } });
    expect(d3).toBe(2);
  });
  it('a failing saga on a serial lane does not block the next one; dispatch logs and never rejects', async () => {
    const { rt } = make();
    function* boom(): Saga<void> {
      yield fx.state.wait(() => true);
      throw new Error('nope');
    }
    function* fine(): Saga<string> {
      yield fx.state.wait(() => true);
      return 'ok';
    }
    await expect(rt.run(boom(), { lane: { kind: 'serial', key: 'k' } })).rejects.toThrow('nope');
    expect(await rt.run(fine(), { lane: { kind: 'serial', key: 'k' } })).toBe('ok');
    logs.length = 0;
    await rt.dispatch({ type: 'Materialize', hash: 'missing' });
    await rt.dispatch({ type: 'ReadMembership', hashes: [] });
    expect(logs).toEqual([]);
  });
});

describe('Runtime state hooks', () => {
  it('dirty queries are materialized after the debounce, once per burst', async () => {
    const { a, rt } = make(
      { local: { select: async () => [{ id: 'thing:1' }] } },
      buildState([buildEntry({ def: { hash: 'q', plan: { table: 'thing' } as any } })], R.reloadViews(new Map([['v', ['thing:1']]])))
    );
    const seen: unknown[] = [];
    rt.subscribe('q', (rows) => seen.push(rows));
    rt.update(R.markDirty(['q']));
    rt.update(R.markDirty(['q']));
    expect(a.timers.pending.has('mat:q')).toBe(true);
    a.timers.fire('mat:q');
    await tick();
    await tick();
    expect(seen).toEqual([[{ id: 'thing:1' }]]);
    expect(rt.state.dirty.has('q')).toBe(false);
  });
  it('materializes the first change at once, and holds the next one to the window', async () => {
    const { a, rt } = make(
      { local: { select: async () => [{ id: 'thing:1' }] } },
      buildState([buildEntry({ def: { hash: 'q', plan: { table: 'thing' } as any } })], R.reloadViews(new Map([['v', ['thing:1']]])))
    );
    rt.update(R.markDirty(['q']));
    expect(a.timers.pending.get('mat:q')?.ms).toBe(0);
    a.timers.fire('mat:q');
    await tick();
    await tick();
    a.advance(2);
    rt.update(R.markDirty(['q']));
    expect(a.timers.pending.get('mat:q')?.ms).toBe(3);
    a.timers.fire('mat:q');
    await tick();
    await tick();
    a.advance(50);
    rt.update(R.markDirty(['q']));
    expect(a.timers.pending.get('mat:q')?.ms).toBe(0);
  });
  it('the bucket-switch window: a re-seeded query never reads "known and idle" before its rows are painted', async () => {
    // A sign-in: the query was registered in the anon bucket (cold, nothing
    // painted), then the switch re-seeds it from the user's durable view.
    // Before the fix the re-seed announced authority with `records` still
    // empty, so a binding's `isSettled() && !row` held for a beat and the app
    // created a row the user already had.
    const { a, rt } = make(
      {
        local: {
          getById: async () => ({ ids: [['thing:1', 1]], confirmed: true }),
          select: async (plan: any) => (plan.ids ?? []).map((id: any) => ({ id: `${id.table}:${id.id}` })),
        },
      },
      { ...buildState([buildEntry({ def: { hash: 'q', plan: { table: 'thing' } as any } })]), sessionId: 's' }
    );
    // What a binding derives `isSettled()` and `data()` from.
    let known = false;
    let idle = false;
    let rows: unknown[] = [];
    const verdicts: string[] = [];
    const look = () => {
      if (known && idle) verdicts.push(rows.length > 0 ? 'settled with rows' : 'settled empty');
    };
    rt.subscribe('q', (r) => ((rows = r), look()), { immediate: true });
    rt.subscribeAuthority('q', (k) => ((known = k), look()), { immediate: true });
    rt.subscribeStatus('q', (st) => ((idle = st === 'idle'), look()), { immediate: true });

    await rt.run(rebindQueries());
    expect(rt.state.queries.get('q')!.lifecycle).toMatchObject({ phase: 'cached', fetchDepth: 0, answered: false });
    expect(idle).toBe(true);
    expect(known).toBe(false);

    a.timers.fire('mat:q');
    await tick();
    await tick();
    expect(rows).toEqual([{ id: 'thing:1' }]);
    expect(known).toBe(true);
    expect(verdicts).toEqual(['settled with rows']);
  });
  it('status and authority subscriptions fire on transitions, immediate delivers the current value', async () => {
    const { rt } = make({}, buildState([buildEntry({ def: { hash: 'q' } })]));
    const statuses: string[] = [];
    const auths: boolean[] = [];
    const offS = rt.subscribeStatus('q', (s) => statuses.push(s), { immediate: true });
    const offA = rt.subscribeAuthority('q', (k) => auths.push(k), { immediate: true });
    const offA2 = rt.subscribeAuthority('q', () => auths.push(false));
    rt.update(R.applyLifecycle('q', { type: 'fetch-begin' }));
    rt.update(R.applyLifecycle('q', { type: 'fetch-begin' }));
    rt.update(R.applyLifecycle('q', { type: 'fetch-end' }));
    rt.update(R.applyLifecycle('q', { type: 'fetch-end' }));
    rt.emit({ type: 'query:authority', hash: 'q', known: true });
    expect(statuses).toEqual(['idle', 'fetching', 'idle']);
    expect(auths).toEqual([false, true, false]);
    offA2();
    offA();
    rt.emit({ type: 'query:authority', hash: 'q', known: false });
    expect(auths).toHaveLength(3);
    offS();
    rt.update(R.applyLifecycle('q', { type: 'fetch-begin' }));
    expect(statuses).toHaveLength(3);
    const missing: string[] = [];
    rt.subscribeStatus('zz', (s) => missing.push(s), { immediate: true });
    rt.subscribeAuthority('zz', () => missing.push('x'), { immediate: true });
    expect(missing).toEqual([]);
  });
  it('notifies health on a transport change, and only on a real move', () => {
    const { rt } = make();
    const seen: string[] = [];
    rt.on('health:changed', (e) => seen.push(e.type === 'health:changed' ? e.health.connection : '?'));
    rt.update(R.setConnection('connected'));
    rt.update(R.setConnection('connected'));
    rt.update(R.setConnection('reconnecting'));
    expect(seen).toEqual(['connected', 'reconnecting']);
    // A health snapshot rebuilt with the same values must not re-notify.
    rt.update(R.setHealth({ ...rt.state.sync.health }));
    expect(seen).toHaveLength(2);
    rt.update(R.setHealth({ ...rt.state.sync.health, status: 'degraded' }));
    expect(seen).toHaveLength(3);
  });
  it('activity, listeners, records subscription bookkeeping, waitFor and dispose', async () => {
    const { a, rt } = make({}, buildState([buildEntry({ def: { hash: 'q' } })]));
    const events: string[] = [];
    const off = rt.on('activity:changed', (e) => events.push(JSON.stringify(e)));
    rt.on('*', (e) => events.push(`*${e.type}`));
    rt.update(R.applyLifecycle('q', { type: 'fetch-begin' }));
    expect(events).toEqual(['{"type":"activity:changed","fetching":1,"pending":0}', '*activity:changed']);
    off();
    const rows: unknown[] = [];
    const unsub = rt.subscribe('q', (r) => rows.push(r), { immediate: true });
    expect(rows).toEqual([[]]);
    expect(rt.state.queries.get('q')!.subscribers).toBe(1);
    rt.emit({ type: 'query:records', hash: 'q', records: [{ id: 1 }] });
    expect(rows).toHaveLength(2);
    unsub();
    unsub();
    expect(rt.state.queries.get('q')!.subscribers).toBe(0);
    expect(rt.state.queries.get('q')!.lastSubscriberLeftAt).toBe(1_700_000_000_000);
    rt.emit({ type: 'log', level: 'warn', message: 'x' });
    rt.subscribe('q', () => {
      throw new Error('subscriber bug');
    });
    rt.emit({ type: 'query:records', hash: 'q', records: [] });
    expect(logs.at(-1)).toMatchObject({ error: new Error('subscriber bug') });
    await expect(rt.waitFor(() => true)).resolves.toBeUndefined();
    const ctrl = new AbortController();
    ctrl.abort();
    await expect(rt.waitFor(() => false, ctrl.signal)).rejects.toThrow('aborted');
    const ctrl2 = new AbortController();
    const p = rt.waitFor(() => false, ctrl2.signal);
    ctrl2.abort();
    await expect(p).rejects.toThrow('aborted');
    await expect(
      rt.waitFor(() => {
        throw new Error('bad selector');
      })
    ).rejects.toThrow('bad selector');
    let calls = 0;
    const throwingLater = rt.waitFor(() => {
      calls++;
      if (calls > 1) throw new Error('bad selector later');
      return false;
    });
    rt.update(R.setFailedCount(1));
    await expect(throwingLater).rejects.toThrow('bad selector later');
    const pending = rt.waitFor(() => false);
    a.timers.pending.clear();
    rt.update(R.markDirty(['q']));
    expect(a.timers.pending.has('mat:q')).toBe(true);
    rt.dispose();
    await expect(pending).rejects.toThrow('disposed');
    expect(a.timers.pending.size).toBe(0);
    await rt.dispatch({ type: 'PollTick' });
    expect(rt.update(R.setFailedCount(1))).toBeUndefined();
  });
  it('a disposed runtime ignores late timer fires', async () => {
    const { a, rt } = make({}, buildState([buildEntry({ def: { hash: 'q' } })]));
    rt.update(R.markDirty(['q']));
    const fire = a.timers.pending.get('mat:q')!.fire;
    rt.dispose();
    fire();
    await tick();
    expect(rt.state.dirty.has('q')).toBe(true);
  });
});

describe('Runtime sign-up', () => {
  it('the page ends up owning one remote row per query when its registration races the auth flip', async () => {
    // threads-e2e, canary.350: the list mounts as the sign-up resolves and
    // registers under the anonymous salt on a socket already signed in; the
    // flip re-keys it while that request is out. The fake server keeps the
    // `_00_query` rows the way `fn::query::register` / `unsubscribe` do.
    const rows = new Set<string>();
    let land!: () => void;
    const held = new Promise<void>((r) => (land = r));
    let first = true;
    const ok = (result: unknown) => ({ status: 'OK' as const, result });
    let bucket = 'anon';
    const { a, rt } = make(
      {
        remote: {
          queryResponses: async (sql: string, vars?: Record<string, any>) => {
            if (sql.startsWith('fn::query::register($config)')) {
              rows.add(String(vars!.config.id));
              if (first) {
                first = false;
                await held;
              }
              return [ok(null), ok([]), ok({ rowCount: 0, state: 'ready' }), ok([])];
            }
            if (sql.includes('fn::query::unsubscribe')) {
              for (const [k, v] of Object.entries(vars ?? {})) if (k.startsWith('id')) rows.delete(String(v));
              return [];
            }
            return [];
          },
        },
        services: {
          'auth.sessionAuthId': () => 'user:u1',
          'auth.access': () => 'account',
          'local.currentBucketId': () => bucket,
          'local.beginSwitch': () => () => undefined,
          'local.switchStore': async (target: string) => void (bucket = target),
          'local.usesSurqlSchema': () => false,
          'auth.currentUser': () => null,
          'remote.sessionIds': () => ['s1'],
        },
      },
      { ...buildState(), sessionId: 'anon-salt', saltUserId: null, bucketId: 'anon', primed: true }
    );
    const hash = await rt.run(registerLocal(env, { tableName: 'thread', surql: 'SELECT * FROM thread', params: {}, ttl: '10m' }));
    await tick();
    expect(rows.size).toBe(1);
    const anonId = [...rows][0];

    await rt.dispatch({ type: 'AuthFlip', userId: 'user:u1' });
    land();
    for (let i = 0; i < 10; i++) {
      await tick();
      a.timers.fire(`register:${hash}`);
    }

    const entry = rt.state.queries.get(hash)!;
    expect(String(entry.def.id)).not.toBe(anonId);
    expect([...rows]).toEqual([String(entry.def.id)]);
    expect(entry.lifecycle.remote).toBe('registered');
  });
});
