import { describe, expect, it } from 'vitest';
import type { Effect } from './effects';
import { fx } from './effects';
import { acquire, emptyLanes, release, runSaga } from './saga';
import type { Saga } from './saga';

describe('runSaga', () => {
  it('threads effect results back into the generator in order', async () => {
    const seen: Effect[] = [];
    function* saga(): Saga<number> {
      const a = (yield fx.now()) as number;
      const b = (yield fx.hash('x')) as number;
      return a + b;
    }
    const result = await runSaga(saga(), async (e) => {
      seen.push(e);
      return e.kind === 'now' ? 1 : 2;
    });
    expect(result).toBe(3);
    expect(seen.map((e) => e.kind)).toEqual(['now', 'hash']);
  });

  it('throws a rejected effect into the saga so it can catch it', async () => {
    function* saga(): Saga<string> {
      try {
        yield fx.now();
        return 'unreachable';
      } catch (err) {
        return `caught:${(err as Error).message}`;
      }
    }
    await expect(runSaga(saga(), async () => Promise.reject(new Error('boom')))).resolves.toBe('caught:boom');
  });

  it('propagates an error the saga does not catch', async () => {
    function* saga(): Saga<void> {
      yield fx.now();
    }
    await expect(runSaga(saga(), async () => Promise.reject(new Error('boom')))).rejects.toThrow('boom');
  });

  it('returns immediately for a saga that yields nothing', async () => {
    // oxlint-disable-next-line require-yield
    function* saga(): Saga<number> {
      return 7;
    }
    await expect(runSaga(saga(), async () => undefined)).resolves.toBe(7);
  });
});

describe('lanes', () => {
  it('serial: first acquires, second waits, release hands over, final release frees', () => {
    const lane = { kind: 'serial', key: 'outbox' } as const;
    const a = acquire(emptyLanes(), lane);
    expect(a.decision).toBe('start');
    const b = acquire(a.state, lane);
    expect(b.decision).toBe('wait');
    const c = acquire(b.state, lane);
    expect(c.decision).toBe('wait');
    expect(c.state.waiting.get('outbox')).toBe(2);
    const r1 = release(c.state, 'outbox');
    expect(r1.startNext).toBe(true);
    expect(r1.state.waiting.get('outbox')).toBe(1);
    expect(r1.state.running.has('outbox')).toBe(true);
    const r2 = release(r1.state, 'outbox');
    expect(r2.startNext).toBe(true);
    expect(r2.state.waiting.has('outbox')).toBe(false);
    const r3 = release(r2.state, 'outbox');
    expect(r3.startNext).toBe(false);
    expect(r3.state.running.has('outbox')).toBe(false);
  });

  it('dedupe: a request during a run joins it and does not queue', () => {
    const lane = { kind: 'dedupe', key: 'mat:h' } as const;
    const a = acquire(emptyLanes(), lane);
    const b = acquire(a.state, lane);
    expect(b.decision).toBe('join');
    expect(b.state).toBe(a.state);
    const r = release(b.state, 'mat:h');
    expect(r.startNext).toBe(false);
    expect(r.state.running.size).toBe(0);
  });

  it('keys are independent', () => {
    const a = acquire(emptyLanes(), { kind: 'serial', key: 'x' });
    const b = acquire(a.state, { kind: 'serial', key: 'y' });
    expect(b.decision).toBe('start');
  });
});
