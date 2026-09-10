import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure, sha256Hex } from '../testing/run-pure';
import { buildEntry, buildState } from '../testing/build';
import { defaultEnv } from './env';
import { queryHashInput } from './hash';
import { ensureRegistered, registerLocal, registerRemote } from './register.saga';
import * as R from '../state/reducers';
import { emptyState } from '../state/client-state';
import type { StatementResult } from '../kernel/effects';

const env = defaultEnv({ tables: [{ name: 'thing', columns: {} }] } as any);
const input = { tableName: 'thing', surql: 'SELECT * FROM thing', params: {}, ttl: '10m' as const };
const sspOk = () => ({ localArray: [['thing:9', 1]], timings: { parseMs: 1, planMs: 1, snapshotMs: 1, wallMs: 1 } });
const okStmt = (result: unknown): StatementResult => ({ status: 'OK', result });

describe('registerLocal', () => {
  it('cold: no view row -> cold entry, SSP view, EnsureRegistered', async () => {
    const state = { ...emptyState({ tabId: 't' }), sessionId: 'sess' };
    const out = await runPure(registerLocal(env, input), {
      state,
      handlers: { 'local.getById': () => null, 'ssp.register': sspOk },
    });
    const hash = await sha256Hex(queryHashInput(input, 'sess'));
    expect(out.result).toBe(hash);
    const entry = out.state.queries.get(hash)!;
    expect(entry.lifecycle.phase).toBe('cold');
    expect(entry.remoteArray).toEqual([]);
    expect(entry.localArray).toEqual([['thing:9', 1]]);
    expect(entry.def.ttlMs).toBe(600_000);
    expect(entry.telemetry.registrationTimings.wallMs).toBe(1);
    expect(out.state.dirty.has(hash)).toBe(true);
    expect(out.state.registering.size).toBe(0);
    expect(out.dispatched).toEqual([{ type: 'EnsureRegistered' }]);
    const reg = out.log.find((e) => e.kind === 'ssp.register');
    expect(reg).toMatchObject({ plan: { queryHash: hash, tableName: 'thing', ttl: '10m' } });
  });

  it('cached: a durable view row seeds membership; unconfirmed empty stays cold; a failing read is cold', async () => {
    const cached = await runPure(registerLocal(env, input), {
      handlers: { 'local.getById': () => ({ ids: [['thing:1', 2]], confirmed: false }), 'ssp.register': sspOk },
    });
    const e1 = cached.state.queries.get(cached.result)!;
    expect(e1.lifecycle.phase).toBe('cached');
    expect(e1.remoteArray).toEqual([['thing:1', 2]]);
    const confirmedEmpty = await runPure(registerLocal(env, input), {
      handlers: { 'local.getById': () => ({ ids: [], confirmed: true }), 'ssp.register': sspOk },
    });
    expect(confirmedEmpty.state.queries.get(confirmedEmpty.result)!.lifecycle.phase).toBe('cached');
    const unconfirmed = await runPure(registerLocal(env, input), {
      handlers: { 'local.getById': () => ({ ids: [], confirmed: false }), 'ssp.register': sspOk },
    });
    expect(unconfirmed.state.queries.get(unconfirmed.result)!.lifecycle.phase).toBe('cold');
    const failing = await runPure(registerLocal(env, input), {
      handlers: {
        'local.getById': () => {
          throw new Error('no table');
        },
        'ssp.register': sspOk,
      },
    });
    expect(failing.state.queries.get(failing.result)!.lifecycle.phase).toBe('cold');
  });

  it('dedupes: active returns immediately, pending waits for the other registration', async () => {
    const hash = await sha256Hex(queryHashInput(input, null));
    const active = buildState([buildEntry({ def: { hash } })]);
    const a = await runPure(registerLocal(env, input), { state: active });
    expect(a.result).toBe(hash);
    expect(a.log.filter((e) => e.kind === 'ssp.register')).toHaveLength(0);
    const pending = R.beginRegistering(hash)(emptyState({ tabId: 't' }));
    const p = await runPure(registerLocal(env, input), {
      state: pending,
      handlers: { 'state.wait': (_e, ctx) => void (ctx.state = R.endRegistering(hash)(ctx.state)) },
    });
    expect(p.result).toBe(hash);
    expect(p.log.some((e) => e.kind === 'state.wait')).toBe(true);
  });

  it('a failing SSP registration clears the in-flight marker and rethrows', async () => {
    await expect(
      runPure(registerLocal(env, input), {
        handlers: {
          'local.getById': () => null,
          'ssp.register': () => {
            throw new Error('wasm down');
          },
        },
      })
    ).rejects.toThrow('wasm down');
  });
});

describe('ensureRegistered', () => {
  it('dispatches RegisterRemote for every unregistered query only', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' } }), buildEntry({ def: { hash: 'b' }, lifecycle: { remote: 'registered' } })]);
    const out = await runPure(ensureRegistered(env), { state: s });
    expect(out.dispatched).toEqual([{ type: 'RegisterRemote', hash: 'a' }]);
    const none = await runPure(ensureRegistered(env), { state: buildState() });
    expect(none.dispatched).toEqual([]);
  });
  it('with requireAuth: probes $auth.id, retries on a timer, gives up after the budget, skips when anonymous', async () => {
    const s = { ...buildState([buildEntry({ def: { hash: 'a' } })]), userId: 'user:1' };
    const authed = await runPure(ensureRegistered(env, { requireAuth: true }), {
      state: s,
      handlers: { 'remote.query': () => [okStmt('user:1')] },
    });
    expect(authed.dispatched).toEqual([{ type: 'RegisterRemote', hash: 'a' }]);
    const notYet = await runPure(ensureRegistered(env, { requireAuth: true, attempt: 2 }), {
      state: s,
      handlers: { 'remote.query': () => [okStmt(null)] },
    });
    expect(notYet.dispatched).toEqual([]);
    expect(notYet.timers.get('ensure-registered')).toEqual({ ms: 500, event: { type: 'EnsureRegistered', requireAuth: true, attempt: 3 } });
    const thrown = await runPure(ensureRegistered(env, { requireAuth: true }), {
      state: s,
      handlers: {
        'remote.query': () => {
          throw new Error('offline');
        },
      },
    });
    expect(thrown.timers.has('ensure-registered')).toBe(true);
    const exhausted = await runPure(ensureRegistered(env, { requireAuth: true, attempt: 9 }), {
      state: s,
      handlers: { 'remote.query': () => [okStmt(null)] },
    });
    expect(exhausted.timers.size).toBe(0);
    expect(exhausted.emitted).toEqual([expect.objectContaining({ type: 'log', level: 'warn' })]);
    const anon = await runPure(ensureRegistered(env, { requireAuth: true }), { state: { ...s, userId: null } });
    expect(anon.dispatched).toEqual([{ type: 'RegisterRemote', hash: 'a' }]);
  });
});

describe('registerRemote', () => {
  const entry = buildEntry({ def: { hash: 'a' }, lifecycle: { phase: 'cold' } });
  const happy = () => [
    okStmt(null),
    okStmt([{ out: new RecordId('thing', '1'), version: 1 }]),
    okStmt({ rowCount: 1, state: 'ready' }),
    okStmt([{ out: new RecordId('child', 'c'), version: 1 }]),
  ];
  it('registers, applies membership + children, flips registered, fetch-begin/end balanced', async () => {
    const out = await runPure(registerRemote(env, 'a'), {
      state: buildState([entry]),
      handlers: { 'remote.query': happy, 'local.upsert': () => undefined },
    });
    const e = out.state.queries.get('a')!;
    expect(e.lifecycle).toMatchObject({ phase: 'live', remote: 'registered', fetchDepth: 0 });
    expect(e.remoteArray).toEqual([['thing:1', 1]]);
    expect(e.subqueryRemoteArray).toEqual([['child:c', 1]]);
    expect(e.serverState).toBe('ready');
    const req = out.log.find((x) => x.kind === 'remote.query') as any;
    expect(req.sql.startsWith('fn::query::register($config);')).toBe(true);
    expect(req.vars.config).toMatchObject({ surql: entry.def.surql, ttl: '10m' });
    expect(out.dispatched).toEqual([{ type: 'FetchRows' }, { type: 'FetchRows' }, { type: 'SyncOutcome', ok: true }]);
    expect(out.emitted).toContainEqual({ type: 'query:authority', hash: 'a', known: true });
  });
  it('guards: missing entry, already registered / failed, registering without retry', async () => {
    const none = await runPure(registerRemote(env, 'zzz'), { state: buildState([]) });
    expect(none.log.filter((x) => x.kind === 'remote.query')).toHaveLength(0);
    for (const remote of ['registered', 'failed', 'registering'] as const) {
      const out = await runPure(registerRemote(env, 'a'), { state: buildState([buildEntry({ def: { hash: 'a' }, lifecycle: { remote } })]) });
      expect(out.log.filter((x) => x.kind === 'remote.query')).toHaveLength(0);
    }
    const retry = await runPure(registerRemote(env, 'a', true), {
      state: buildState([buildEntry({ def: { hash: 'a' }, lifecycle: { remote: 'registering' } })]),
      handlers: { 'remote.query': happy, 'local.upsert': () => undefined },
    });
    expect(retry.state.queries.get('a')!.lifecycle.remote).toBe('registered');
  });
  it('materializing answer schedules the re-read ladder', async () => {
    const out = await runPure(registerRemote(env, 'a'), {
      state: buildState([entry]),
      handlers: { 'remote.query': () => [okStmt(null), okStmt([]), okStmt({ rowCount: 3, state: 'materializing' }), okStmt([])] },
    });
    expect(out.state.membershipDirty.has('a')).toBe(true);
    expect(out.state.membershipReread.get('a')).toBe(1);
    expect(out.timers.get('membership')).toEqual({ ms: 150, event: { type: 'ReadDirtyMembership' } });
    expect(out.state.queries.get('a')!.lifecycle).toMatchObject({ phase: 'cold', remote: 'registered' });
  });
  it('errors: transport failure backs off, statement errors count as failures, budget exhausts to failed', async () => {
    const boom = () => {
      throw new Error('socket closed');
    };
    const first = await runPure(registerRemote(env, 'a'), { state: buildState([entry]), handlers: { 'remote.query': boom } });
    expect(first.state.queries.get('a')!.lifecycle).toMatchObject({ remote: 'registering', fetchDepth: 0 });
    expect(first.state.queries.get('a')!.registerAttempts).toBe(1);
    expect(first.timers.get('register:a')).toEqual({ ms: 1000, event: { type: 'RegisterRemote', hash: 'a', retry: true } });
    expect(first.dispatched).toContainEqual({ type: 'SyncOutcome', ok: false, error: new Error('socket closed') });
    const errStmt = await runPure(registerRemote(env, 'a'), {
      state: buildState([entry]),
      handlers: { 'remote.query': () => [{ status: 'ERR', error: 'denied' }] },
    });
    expect(errStmt.state.queries.get('a')!.registerAttempts).toBe(1);
    const edgesErr = await runPure(registerRemote(env, 'a'), {
      state: buildState([entry]),
      handlers: { 'remote.query': () => [okStmt(null), { status: 'ERR', error: 'no table' }, okStmt(null), okStmt(null)] },
    });
    expect(edgesErr.state.queries.get('a')!.registerAttempts).toBe(1);
    const noArray = await runPure(registerRemote(env, 'a'), {
      state: buildState([entry]),
      handlers: { 'remote.query': () => [okStmt(null), okStmt('nope'), okStmt(null), okStmt(null)] },
    });
    expect(noArray.state.queries.get('a')!.registerAttempts).toBe(1);
    const empty = await runPure(registerRemote(env, 'a'), { state: buildState([entry]), handlers: { 'remote.query': () => [] } });
    expect(empty.state.queries.get('a')!.registerAttempts).toBe(1);
    const last = await runPure(registerRemote(env, 'a'), {
      state: buildState([buildEntry({ def: { hash: 'a' }, registerAttempts: 2 })]),
      handlers: { 'remote.query': boom },
    });
    expect(last.state.queries.get('a')!.lifecycle.remote).toBe('failed');
    expect(last.timers.size).toBe(0);
  });
  it('tolerates ERR meta/children statements: edges still apply, missing meta reads as no row', async () => {
    const out = await runPure(registerRemote(env, 'a'), {
      state: buildState([entry]),
      handlers: {
        'remote.query': () => [okStmt(null), okStmt([{ out: new RecordId('thing', '1'), version: 1 }]), { status: 'ERR', error: 'meta' }, { status: 'ERR', error: 'kids' }],
        'local.upsert': () => undefined,
      },
    });
    const e = out.state.queries.get('a')!;
    expect(e.lifecycle).toMatchObject({ phase: 'live', remote: 'registered' });
    expect(e.serverState).toBeNull();
    expect(e.subqueryRemoteArray).toEqual([]);
  });
  it('stops when the entry disappears mid-flight', async () => {
    const out = await runPure(registerRemote(env, 'a'), {
      state: buildState([entry]),
      handlers: {
        'remote.query': (_e, ctx) => {
          ctx.state = R.removeQuery('a')(ctx.state);
          return happy();
        },
      },
    });
    expect(out.state.queries.has('a')).toBe(false);
    expect(out.dispatched).toEqual([]);
    const gone = await runPure(registerRemote(env, 'a'), {
      state: buildState([entry]),
      handlers: {
        'remote.query': (_e, ctx) => {
          ctx.state = R.removeQuery('a')(ctx.state);
          throw new Error('x');
        },
      },
    });
    expect(gone.dispatched).toEqual([]);
  });
});
