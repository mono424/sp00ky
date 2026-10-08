import { describe, it, expect } from 'vitest';
import type { OutboxItem, PendingWrite } from '../../state/client-state';
import { initialHealth } from '../../state/client-state';
import { buildMutationsState, trimHistory, type MutationHistoryEntry } from './mutations';

const id = (ts: number, n = 1) => `_00_pending_mutations:${String(ts).padStart(13, '0')}_${String(n).padStart(4, '0')}_tab`;

const item = (over: Partial<OutboxItem> & { id: string }): OutboxItem => ({
  type: 'update',
  recordId: 'thread:a',
  table: 'thread',
  status: 'pending',
  ackedAt: null,
  attempts: 0,
  ...over,
});

const build = (over: Partial<Parameters<typeof buildMutationsState>[0]> = {}) =>
  buildMutationsState({
    outbox: [],
    pendingWrites: [],
    failedCount: 0,
    tabRole: 'solo',
    health: initialHealth('connected'),
    history: [],
    cap: 200,
    ...over,
  });

describe('buildMutationsState', () => {
  it('reads status off the outbox, queued time off the id', () => {
    const s = build({
      outbox: [
        item({ id: id(1000) }),
        item({ id: id(2000), attempts: 3 }),
        item({ id: id(3000), status: 'acked', ackedAt: 3500 }),
      ],
    });
    expect(s.entries.map((e) => [e.queuedAt, e.status])).toEqual([
      [3000, 'synced'],
      [2000, 'retrying'],
      [1000, 'pending'],
    ]);
    expect(s.entries[0].settledAt).toBe(3500);
    expect(s.counts).toMatchObject({ pending: 1, retrying: 1, synced: 1, rolledBack: 0 });
  });

  it('keeps what left the outbox, and the live outbox wins over history', () => {
    const history: MutationHistoryEntry[] = [
      { id: id(1000), op: 'create', recordId: 'thread:a', table: 'thread', fields: ['title'], queuedAt: 1000, outcome: { status: 'synced', at: 1100 } },
      { id: id(2000), op: 'update', recordId: 'thread:b', table: 'thread', fields: ['body'], queuedAt: 2000, outcome: { status: 'rolled-back', at: 2100, error: 'denied' } },
      { id: id(3000), op: 'update', recordId: 'thread:c', table: 'thread', fields: ['x'], queuedAt: 3000 },
      { id: id(4000), op: 'delete', recordId: 'thread:d', table: 'thread', queuedAt: 4000 },
    ];
    const s = build({ history, outbox: [item({ id: id(3000), recordId: 'thread:c', attempts: 1 })] });
    const byId = new Map(s.entries.map((e) => [e.id, e]));
    expect(byId.get(id(1000))).toMatchObject({ status: 'synced', settledAt: 1100, fields: ['title'] });
    expect(byId.get(id(2000))).toMatchObject({ status: 'rolled-back', error: 'denied' });
    expect(byId.get(id(3000))).toMatchObject({ status: 'retrying', attempts: 1, fields: ['x'] });
    expect(byId.get(id(4000))).toMatchObject({ status: 'dropped' });
  });

  it('caps entries but counts and totals everything', () => {
    const outbox = Array.from({ length: 5 }, (_, i) => item({ id: id(1000 + i) }));
    const s = build({ outbox, cap: 2 });
    expect(s.entries.map((e) => e.queuedAt)).toEqual([1004, 1003]);
    expect(s.total).toBe(5);
    expect(s.counts.pending).toBe(5);
  });

  it('lists debounced writes and carries role, health and tray count', () => {
    const w: PendingWrite = { key: 'k', table: 'doc', recordId: 'doc:1', data: { title: 't', body: 'b' }, before: null, firstAt: 42, mirrorId: null };
    const s = build({
      pendingWrites: [w],
      failedCount: 3,
      tabRole: 'follower',
      health: { ...initialHealth('reconnecting'), status: 'degraded', consecutiveFailures: 4, error: 'socket closed' },
    });
    expect(s.debounced).toEqual([{ key: 'k', recordId: 'doc:1', table: 'doc', fields: ['title', 'body'], since: 42, durable: false }]);
    expect(s).toMatchObject({ role: 'follower', connection: 'reconnecting', health: 'degraded', consecutiveFailures: 4, lastError: 'socket closed' });
    expect(s.counts).toMatchObject({ failed: 3, debounced: 1 });
  });
});

describe('trimHistory', () => {
  it('drops the oldest entries first', () => {
    const h = new Map<string, MutationHistoryEntry>();
    for (const n of [1, 2, 3]) h.set(`m${n}`, { id: `m${n}`, op: 'create', recordId: 'a:1', table: 'a', queuedAt: n });
    trimHistory(h, 2);
    expect([...h.keys()]).toEqual(['m2', 'm3']);
  });
});
