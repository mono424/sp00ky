import { describe, expect, it } from 'vitest';
import type { LifecycleEvent, QueryLifecycle, QueryPhase } from './lifecycle';
import { deriveStatus, hasServerMembership, isAnswered, isAuthoritative, isForegroundFetching, LifecycleError, seedLifecycle, transition } from './lifecycle';

const at = (phase: QueryPhase, over: Partial<QueryLifecycle> = {}): QueryLifecycle => ({
  phase,
  remote: 'unregistered',
  fetchDepth: 0,
  backgroundDepth: 0,
  notified: false,
  answered: false,
  ...over,
});

describe('transition table (phase axis)', () => {
  it('seed', () => {
    expect(transition(at('live'), { type: 'seed', resolvedBefore: true }).phase).toBe('cached');
    expect(transition(at('live'), { type: 'seed', resolvedBefore: false }).phase).toBe('cold');
  });
  it('membership-applied moves every phase to live', () => {
    for (const p of ['cold', 'cached', 'live'] as const) {
      expect(transition(at(p), { type: 'membership-applied', present: true }).phase).toBe('live');
      expect(transition(at(p), { type: 'membership-applied', present: false }).phase).toBe('live');
    }
    expect(transition(at('view-lost'), { type: 'membership-applied', present: true }).phase).toBe('live');
  });
  it('view-lost needs a present row to recover', () => {
    expect(() => transition(at('view-lost'), { type: 'membership-applied', present: false })).toThrow(LifecycleError);
  });
  it('row-missing: cold stays cold, everything else goes view-lost', () => {
    expect(transition(at('cold'), { type: 'row-missing' }).phase).toBe('cold');
    expect(transition(at('cached'), { type: 'row-missing' }).phase).toBe('view-lost');
    expect(transition(at('live'), { type: 'row-missing' }).phase).toBe('view-lost');
    expect(transition(at('view-lost'), { type: 'row-missing' }).phase).toBe('view-lost');
  });
  it('bucket-switch reseeds and clears fetch depth', () => {
    const l = transition(at('live', { fetchDepth: 3, notified: true, answered: true }), { type: 'bucket-switch', resolvedBefore: true });
    expect(l).toEqual({ phase: 'cached', remote: 'unregistered', fetchDepth: 0, backgroundDepth: 0, notified: false, answered: false });
  });
});

describe('answered', () => {
  it('a seed is never answered, not even a cached one: its rows are not painted yet', () => {
    expect(seedLifecycle(true)).toMatchObject({ phase: 'cached', answered: false });
    expect(isAnswered(seedLifecycle(true))).toBe(false);
    expect(isAuthoritative(seedLifecycle(true))).toBe(true);
  });
  it('only a render of a known membership answers; a cold render is a guess', () => {
    expect(transition(at('cold'), { type: 'notified', answered: true }).answered).toBe(false);
    expect(transition(at('cached'), { type: 'notified' }).answered).toBe(false);
    expect(transition(at('cached'), { type: 'notified', answered: false })).toMatchObject({ notified: true, answered: false });
    for (const p of ['cached', 'live', 'view-lost'] as const) {
      expect(transition(at(p), { type: 'notified', answered: true })).toMatchObject({ notified: true, answered: true });
    }
  });
  it('latched through membership, a reconnect and a lost view; cleared by a re-seed', () => {
    let l = transition(at('cached'), { type: 'notified', answered: true });
    l = transition(l, { type: 'membership-applied', present: true });
    expect(l).toMatchObject({ phase: 'live', answered: true });
    l = transition(l, { type: 'notified', answered: false });
    expect(l.answered).toBe(true);
    l = transition(l, { type: 'remote-dropped' });
    expect(l).toMatchObject({ notified: false, answered: true });
    l = transition(l, { type: 'row-missing' });
    expect(l).toMatchObject({ phase: 'view-lost', answered: true });
    expect(transition(l, { type: 'bucket-switch', resolvedBefore: true }).answered).toBe(false);
    expect(transition(l, { type: 'seed', resolvedBefore: true }).answered).toBe(false);
  });
  it('a server set applied to a cold query is not an answer until its render', () => {
    const l = transition(at('cold'), { type: 'membership-applied', present: true });
    expect(l).toMatchObject({ phase: 'live', answered: false });
    expect(isAuthoritative(l)).toBe(true);
    expect(isAnswered(l)).toBe(false);
  });
  it('an unchanged notified keeps identity', () => {
    const l = transition(at('live'), { type: 'notified', answered: true });
    expect(transition(l, { type: 'notified', answered: true })).toBe(l);
    expect(transition(l, { type: 'notified' })).toBe(l);
  });
});

describe('remote / fetch / notified axes', () => {
  it('remote transitions', () => {
    let l = at('cached');
    l = transition(l, { type: 'remote-registering' });
    expect(l.remote).toBe('registering');
    l = transition(l, { type: 'remote-registered' });
    expect(l.remote).toBe('registered');
    l = transition(l, { type: 'notified' });
    expect(l.notified).toBe(true);
    l = transition(l, { type: 'remote-dropped' });
    expect(l).toMatchObject({ remote: 'unregistered', notified: false });
    l = transition(l, { type: 'remote-failed' });
    expect(l.remote).toBe('failed');
  });
  it('fetch depth is a refcount that never goes negative', () => {
    let l = at('live');
    l = transition(l, { type: 'fetch-begin' });
    l = transition(l, { type: 'fetch-begin' });
    expect(deriveStatus(l)).toBe('fetching');
    l = transition(l, { type: 'fetch-end' });
    expect(deriveStatus(l)).toBe('fetching');
    l = transition(l, { type: 'fetch-end' });
    expect(deriveStatus(l)).toBe('idle');
    const same = transition(l, { type: 'fetch-end' });
    expect(same).toBe(l);
  });
  it('background cycles hold fetching but are not foreground', () => {
    let l = at('cached');
    l = transition(l, { type: 'fetch-begin', background: true });
    expect(deriveStatus(l)).toBe('fetching');
    expect(isForegroundFetching(l)).toBe(false);
    l = transition(l, { type: 'fetch-begin' });
    expect(isForegroundFetching(l)).toBe(true);
    l = transition(l, { type: 'fetch-end' });
    expect(l).toMatchObject({ fetchDepth: 1, backgroundDepth: 1 });
    expect(isForegroundFetching(l)).toBe(false);
    l = transition(l, { type: 'fetch-end', background: true });
    expect(l).toMatchObject({ fetchDepth: 0, backgroundDepth: 0 });
    expect(deriveStatus(l)).toBe('idle');
  });
  it('an unbalanced end never leaves more background cycles than cycles', () => {
    const l = transition(at('live', { fetchDepth: 1, backgroundDepth: 1 }), { type: 'fetch-end' });
    expect(l).toMatchObject({ fetchDepth: 0, backgroundDepth: 0 });
  });
  it('notified is idempotent', () => {
    const l = transition(at('live'), { type: 'notified' });
    expect(transition(l, { type: 'notified' })).toBe(l);
  });
});

describe('derived predicates', () => {
  it('authority and server membership follow the phase', () => {
    expect(isAuthoritative(at('cold'))).toBe(false);
    expect(isAuthoritative(at('cached'))).toBe(true);
    expect(hasServerMembership(at('cached'))).toBe(false);
    expect(hasServerMembership(at('live'))).toBe(true);
    expect(hasServerMembership(at('view-lost'))).toBe(true);
    expect(seedLifecycle(true).phase).toBe('cached');
  });
});

describe('invariants over random event sequences', () => {
  const events: LifecycleEvent[] = [
    { type: 'membership-applied', present: true },
    { type: 'row-missing' },
    { type: 'remote-registering' },
    { type: 'remote-registered' },
    { type: 'remote-dropped' },
    { type: 'fetch-begin' },
    { type: 'fetch-end' },
    { type: 'fetch-begin', background: true },
    { type: 'fetch-end', background: true },
    { type: 'notified' },
    { type: 'notified', answered: true },
    { type: 'bucket-switch', resolvedBefore: false },
    { type: 'seed', resolvedBefore: true },
  ];
  // Deterministic LCG so the sequence is reproducible.
  let seed = 42;
  const rand = () => (seed = (seed * 1664525 + 1013904223) % 2 ** 32) / 2 ** 32;

  it('0 <= backgroundDepth <= fetchDepth, notified only after live-or-cached, view-lost never from cold', () => {
    for (let run = 0; run < 200; run++) {
      let l = seedLifecycle(rand() < 0.5);
      for (let i = 0; i < 25; i++) {
        const ev = events[Math.floor(rand() * events.length)];
        const before = l;
        try {
          l = transition(l, ev);
        } catch (err) {
          expect(err).toBeInstanceOf(LifecycleError);
          continue;
        }
        expect(l.fetchDepth).toBeGreaterThanOrEqual(0);
        expect(l.backgroundDepth).toBeGreaterThanOrEqual(0);
        expect(l.backgroundDepth).toBeLessThanOrEqual(l.fetchDepth);
        if (ev.type === 'row-missing' && before.phase === 'cold') expect(l.phase).toBe('cold');
        if (l.phase === 'view-lost') expect(before.phase).not.toBe('cold');
        if (l.answered) expect(l.phase).not.toBe('cold');
      }
    }
  });
});
