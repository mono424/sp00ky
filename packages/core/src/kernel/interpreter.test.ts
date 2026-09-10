import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { fakeAdapters, fakeHost } from '../testing/adapters';
import { createInterpreter } from './interpreter';
import { fx } from './effects';
import { setFailedCount } from '../state/reducers';

describe('interpreter', () => {
  it('routes store, remote, ssp and service effects to the adapters', async () => {
    const a = fakeAdapters({
      remote: { queryResponses: async () => [{ status: 'OK', result: 1 }] },
      ssp: {
        registerQueryPlan: (plan) => ({ queryHash: plan.queryHash, localArray: [['t:1', 1]], registration: { parseMs: 1, planMs: 2, snapshotMs: 3 } }),
      },
      services: { 'hint.read': () => 'u1' },
    });
    const h = fakeHost();
    const run = createInterpreter(a.adapters, h.host);
    const sealed = { sql: 'SELECT 1;', extract: (r: unknown[]) => r };
    await run(fx.local.query('SELECT 1', { a: 1 }, 2));
    await run(fx.local.query('SELECT 2'));
    await run(fx.local.execute(sealed, {}, 1));
    await run(fx.local.select({ table: 't' } as any, {}));
    await run(fx.local.getById('t', 'x'));
    await run(fx.local.upsert('t', 'x', { a: 1 }, 'merge'));
    await run(fx.local.delete('t', 'x'));
    expect(await run(fx.local.epoch())).toBe(0);
    expect(await run(fx.remote.query('RETURN 1'))).toEqual([{ status: 'OK', result: 1 }]);
    expect(await run(fx.remote.live('_00_list_ref'))).toBe('live-uuid');
    await run(fx.remote.kill('live-uuid'));
    const reg = await run(fx.ssp.register({ queryHash: 'h', surql: 's', params: {}, ttl: '10m', tableName: 't' }));
    expect(reg).toEqual({ localArray: [['t:1', 1]], timings: { parseMs: 1, planMs: 2, snapshotMs: 3, wallMs: null } });
    await run(fx.ssp.unregister('h'));
    await run(fx.ssp.ingest([]));
    expect(await run(fx.service('hint.read'))).toBe('u1');
    expect(a.names()).toEqual([
      'local.query', 'local.query', 'local.execute', 'local.select', 'local.getById', 'local.upsert', 'local.delete',
      'remote.live', 'remote.kill', 'ssp.unregister', 'ssp.ingest', 'service.hint.read',
    ]);
    expect(a.calls[0]).toEqual(['local.query', ['SELECT 1', { a: 1 }]]);
  });

  it('ssp.register without a result throws; ssp.register uses default timings when the SSP reports none', async () => {
    const empty = fakeAdapters({ ssp: { registerQueryPlan: () => undefined } });
    const h = fakeHost();
    await expect(createInterpreter(empty.adapters, h.host)(fx.ssp.register({ queryHash: 'h', surql: 's', params: {}, ttl: '1m', tableName: 't' }))).rejects.toThrow(/not initialized/);
    const bare = fakeAdapters();
    const reg = await createInterpreter(bare.adapters, h.host)(fx.ssp.register({ queryHash: 'h', surql: 's', params: {}, ttl: '1m', tableName: 't' }));
    expect(reg).toEqual({ localArray: [], timings: { parseMs: null, planMs: null, snapshotMs: null, wallMs: null } });
  });

  it('remote.query honours the per-effect timeout', async () => {
    const slow = fakeAdapters({ remote: { queryResponses: () => new Promise(() => {}) } });
    const h = fakeHost();
    await expect(createInterpreter(slow.adapters, h.host)(fx.remote.query('SLOW', undefined, 5))).rejects.toThrow(/timed out/);
  });

  it('state, wait, clock, ids, hash, timers, emit, dispatch, all', async () => {
    const a = fakeAdapters({ now: 42 });
    const h = fakeHost();
    const run = createInterpreter(a.adapters, h.host);
    expect(await run(fx.state.read((s) => s.tabId))).toBe('tab-a');
    await run(fx.state.update(setFailedCount(2)));
    expect(h.state.failedCount).toBe(2);
    await run(fx.state.wait((s) => s.failedCount === 2));
    const waiting = run(fx.state.wait((s) => s.failedCount === 3));
    h.host.setState(setFailedCount(3)(h.state));
    await waiting;
    const ctrl = new AbortController();
    const aborted = run(fx.state.wait(() => false), { signal: ctrl.signal });
    ctrl.abort();
    await expect(aborted).rejects.toThrow('aborted');
    expect(await run(fx.now())).toBe(42);
    expect(String(await run(fx.id('mutation')))).toMatch(/^_00_pending_mutations:/);
    expect(String(await run(fx.id('salt')))).toMatch(/^salt-/);
    expect(await run(fx.hash('abc'))).toMatch(/^ba7816bf/);
    await run(fx.timer.set('k', 10, { type: 'Drain' }));
    expect(a.timers.pending.has('k')).toBe(true);
    a.timers.fire('k');
    expect(h.dispatched).toEqual([{ type: 'Drain' }]);
    await run(fx.timer.set('j', 1, { type: 'FetchRows' }));
    await run(fx.timer.clear('j'));
    expect(a.timers.pending.has('j')).toBe(false);
    await run(fx.emit({ type: 'tray:changed', count: 1 }));
    expect(h.emitted).toEqual([{ type: 'tray:changed', count: 1 }]);
    await run(fx.dispatch({ type: 'PollTick' }));
    expect(h.dispatched.at(-1)).toEqual({ type: 'PollTick' });
    const settled = await run(fx.all([fx.now(), fx.remote.query('x'), fx.service('hint.read')]));
    expect(settled).toEqual([{ ok: true, value: 42 }, { ok: true, value: [] }, { ok: true, value: undefined }]);
    const failing = fakeAdapters({
      remote: {
        queryResponses: async () => {
          throw new Error('boom');
        },
      },
    });
    const mixed = await createInterpreter(failing.adapters, h.host)(fx.all([fx.remote.query('x'), fx.now()]));
    expect(mixed).toEqual([{ ok: false, error: new Error('boom') }, { ok: true, value: 1_700_000_000_000 }]);
  });

  it('live events reach the host as LiveChange dispatches', async () => {
    let onChange: ((h: string[]) => void) | null = null;
    const a = fakeAdapters({
      remote: {
        live: async (_t, cb) => {
          onChange = cb;
          return 'u';
        },
      },
    });
    const h = fakeHost();
    await createInterpreter(a.adapters, h.host)(fx.remote.live('_00_list_ref'));
    onChange!(['a', 'b']);
    expect(h.dispatched).toEqual([{ type: 'LiveChange', hashes: ['a', 'b'] }]);
  });
});
