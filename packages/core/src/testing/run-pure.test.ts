import { describe, expect, it } from 'vitest';
import { fx } from '../kernel/effects';
import type { Saga } from '../kernel/saga';
import { bySqlPrefix, runPure, UnhandledEffectError } from './run-pure';
import { setFailedCount } from '../state/reducers';

describe('runPure', () => {
  it('handles state, clock, ids, hash, timers, emit, dispatch', async () => {
    function* saga(): Saga<Record<string, unknown>> {
      const before = (yield fx.state.read((s) => s.failedCount)) as number;
      yield fx.state.update(setFailedCount(3));
      const after = (yield fx.state.read((s) => s.failedCount)) as number;
      const now = (yield fx.now()) as number;
      const id1 = (yield fx.id('mutation')) as string;
      const id2 = (yield fx.id('salt')) as string;
      const hash = (yield fx.hash('abc')) as string;
      yield fx.timer.set('k', 10, { type: 'Drain' });
      yield fx.timer.set('k', 20, { type: 'Drain' });
      yield fx.timer.set('j', 5, { type: 'FetchRows' });
      yield fx.timer.clear('j');
      yield fx.emit({ type: 'tray:changed', count: 1 });
      yield fx.dispatch({ type: 'FetchRows' });
      return { before, after, now, id1, id2, hash };
    }
    const out = await runPure(saga(), { now: 5 });
    expect(out.result).toEqual({
      before: 0,
      after: 3,
      now: 5,
      id1: 'mutation-1',
      id2: 'salt-2',
      hash: 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
    });
    expect(out.state.failedCount).toBe(3);
    expect([...out.timers.entries()]).toEqual([['k', { ms: 20, event: { type: 'Drain' } }]]);
    expect(out.emitted).toEqual([{ type: 'tray:changed', count: 1 }]);
    expect(out.dispatched).toEqual([{ type: 'FetchRows' }]);
    expect(out.log.map((e) => e.kind)[0]).toBe('state.read');
  });

  it('state.wait passes when the predicate holds and throws when it would block', async () => {
    function* ok(): Saga<string> {
      yield fx.state.wait((s) => s.failedCount === 0);
      return 'passed';
    }
    await expect(runPure(ok())).resolves.toMatchObject({ result: 'passed' });
    function* blocked(): Saga<void> {
      yield fx.state.wait((s) => s.failedCount === 9);
    }
    await expect(runPure(blocked())).rejects.toThrow(/would block/);
  });

  it('routes adapter effects to handlers and throws on unhandled ones', async () => {
    function* saga(): Saga<unknown> {
      return yield fx.remote.query('RETURN true');
    }
    const out = await runPure(saga(), { handlers: { 'remote.query': () => [{ status: 'OK', result: true }] } });
    expect(out.result).toEqual([{ status: 'OK', result: true }]);
    await expect(runPure(saga())).rejects.toBeInstanceOf(UnhandledEffectError);
  });

  it('all returns settled results in order and keeps going after a failure', async () => {
    function* saga(): Saga<unknown> {
      return yield fx.all([fx.remote.query('A'), fx.remote.query('B'), fx.now()]);
    }
    const out = await runPure(saga(), {
      now: 1,
      handlers: {
        'remote.query': (e) => {
          if ((e as { sql: string }).sql === 'B') throw new Error('nope');
          return 'a';
        },
      },
    });
    expect(out.result).toEqual([
      { ok: true, value: 'a' },
      { ok: false, error: new Error('nope') },
      { ok: true, value: 1 },
    ]);
  });

  it('bySqlPrefix answers by prefix and rejects unknown SQL', async () => {
    const handler = bySqlPrefix([
      ['SELECT', () => 'rows'],
      ['DELETE', () => 'gone'],
    ]);
    expect(handler(fx.local.query('SELECT * FROM x'), {} as never)).toBe('rows');
    expect(handler(fx.local.query('DELETE x'), {} as never)).toBe('gone');
    expect(() => handler(fx.local.query('UPDATE x'), {} as never)).toThrow(/no scripted answer/);
    expect(() => handler(fx.now(), {} as never)).toThrow(/no scripted answer/);
  });
});
