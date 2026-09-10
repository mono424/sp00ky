import { describe, expect, it } from 'vitest';
import type { LifecycleEvent, QueryLifecycle, QueryPhase } from './lifecycle';
import { deriveStatus, hasServerMembership, isAuthoritative, LifecycleError, seedLifecycle, transition } from './lifecycle';

const at = (phase: QueryPhase, over: Partial<QueryLifecycle> = {}): QueryLifecycle => ({
  phase,
  remote: 'unregistered',
  fetchDepth: 0,
  notified: false,
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
    const l = transition(at('live', { fetchDepth: 3, notified: true }), { type: 'bucket-switch', resolvedBefore: true });
    expect(l).toEqual({ phase: 'cached', remote: 'unregistered', fetchDepth: 0, notified: false });
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
    { type: 'notified' },
    { type: 'bucket-switch', resolvedBefore: false },
    { type: 'seed', resolvedBefore: true },
  ];
  // Deterministic LCG so the sequence is reproducible.
  let seed = 42;
  const rand = () => (seed = (seed * 1664525 + 1013904223) % 2 ** 32) / 2 ** 32;

  it('fetchDepth >= 0, notified only after live-or-cached, view-lost never from cold', () => {
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
        if (ev.type === 'row-missing' && before.phase === 'cold') expect(l.phase).toBe('cold');
        if (l.phase === 'view-lost') expect(before.phase).not.toBe('cold');
      }
    }
  });
});
