import { describe, expect, it } from 'vitest';
import { fx } from './effects';

describe('effect constructors', () => {
  it('build every effect kind as plain data', () => {
    const sealed = { sql: 'x;', extract: (r: unknown[]) => r[0] };
    const plan = { table: 't' } as any;
    const cases: Array<[ReturnType<typeof fx.now>, string]> = [
      [fx.local.query('SELECT 1', { a: 1 }, 3), 'local.query'],
      [fx.local.select(plan, { b: 2 }), 'local.select'],
      [fx.local.execute(sealed, {}, 1), 'local.execute'],
      [fx.local.getById('t', 'x'), 'local.getById'],
      [fx.local.upsert('t', 'x', { a: 1 }, 'merge'), 'local.upsert'],
      [fx.local.delete('t', 'x'), 'local.delete'],
      [fx.remote.query('RETURN true', undefined, 10), 'remote.query'],
      [fx.remote.live('_00_list_ref'), 'remote.live'],
      [fx.remote.kill('u'), 'remote.kill'],
      [fx.ssp.register({ queryHash: 'h', surql: 's', params: {}, ttl: '10m', tableName: 't' }), 'ssp.register'],
      [fx.ssp.unregister('h'), 'ssp.unregister'],
      [fx.ssp.ingest([]), 'ssp.ingest'],
      [fx.timer.set('k', 5, { type: 'Drain' }), 'timer.set'],
      [fx.timer.clear('k'), 'timer.clear'],
      [fx.state.read((s) => s.tabId), 'state.read'],
      [fx.state.update((s) => s), 'state.update'],
      [fx.state.wait((s) => s.primed), 'state.wait'],
      [fx.now(), 'now'],
      [fx.id('mutation'), 'id'],
      [fx.hash('abc'), 'hash'],
      [fx.emit({ type: 'tray:changed', count: 1 }), 'emit'],
      [fx.dispatch({ type: 'FetchRows' }), 'dispatch'],
      [fx.all([fx.now()]), 'all'],
      [fx.service('local.connect', 'anon'), 'service'],
    ];
    for (const [effect, kind] of cases) expect(effect.kind).toBe(kind);
    expect(fx.local.query('SELECT 1', { a: 1 }, 3)).toEqual({ kind: 'local.query', sql: 'SELECT 1', vars: { a: 1 }, epoch: 3 });
    expect(fx.local.epoch()).toEqual({ kind: 'local.epoch' });
    expect(fx.timer.set('k', 5, { type: 'Drain' })).toEqual({ kind: 'timer.set', key: 'k', ms: 5, event: { type: 'Drain' } });
    expect(fx.service('hint.write', 'u1')).toEqual({ kind: 'service', name: 'hint.write', args: ['u1'] });
  });
});
