import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from './env';
import { ackPrune, evictQuery, gcTick, lifecycleTick } from './lifecycle.saga';
import type { StatementResult } from '../kernel/effects';

const env = defaultEnv({ tables: [] } as any);
const ok = (result: unknown): StatementResult => ({ status: 'OK', result });

describe('lifecycleTick', () => {
  it('evicts idle queries (even when the SSP unregister throws), heartbeats the registered rest, reschedules at half the shortest ttl', async () => {
    const now = 1_700_000_000_000;
    const s = buildState([
      buildEntry({ def: { hash: 'idle', ttlMs: 100 }, lastSubscriberLeftAt: now - 200, lifecycle: { remote: 'registered' } }),
      buildEntry({ def: { hash: 'idle2', ttlMs: 100 }, lastSubscriberLeftAt: now - 200 }),
      buildEntry({ def: { hash: 'kept', ttlMs: 1000, id: new RecordId('_00_query', 'kept') }, subscribers: 1, lifecycle: { remote: 'registered' } }),
      buildEntry({ def: { hash: 'unreg', ttlMs: 500 }, subscribers: 1 }),
    ]);
    let unregisters = 0;
    const out = await runPure(lifecycleTick(env), {
      state: s,
      now,
      handlers: {
        'ssp.unregister': () => {
          unregisters++;
          if (unregisters === 1) throw new Error('gone');
        },
        'remote.query': (e: any) => {
          expect(e.sql).toBe('fn::query::heartbeat($id0)');
          expect(e.vars.id0).toEqual(new RecordId('_00_query', 'kept'));
          return [ok([{ id: 'x' }])];
        },
      },
    });
    expect([...out.state.queries.keys()]).toEqual(['kept', 'unreg']);
    expect(out.emitted.filter((e) => e.type === 'query:evicted')).toHaveLength(2);
    expect(out.state.queries.get('kept')!.lastHeartbeatAt).toBe(now);
    expect(out.dispatched).toEqual([{ type: 'SyncOutcome', ok: true }]);
    expect(out.timers.get('lifecycle')).toEqual({ ms: 250, event: { type: 'LifecycleTick' } });
  });
  it('a reclaimed row drops the registration and re-registers; a failed beat reports; empty state uses the default ttl', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' }, subscribers: 1, lifecycle: { remote: 'registered' } })]);
    const reclaimed = await runPure(lifecycleTick(env), { state: s, handlers: { 'remote.query': () => [ok([])] } });
    expect(reclaimed.state.queries.get('a')!.lifecycle.remote).toBe('unregistered');
    expect(reclaimed.dispatched).toEqual([{ type: 'EnsureRegistered' }, { type: 'SyncOutcome', ok: true }]);
    const failed = await runPure(lifecycleTick(env), {
      state: s,
      handlers: {
        'remote.query': () => {
          throw new Error('offline');
        },
      },
    });
    expect(failed.dispatched).toEqual([{ type: 'SyncOutcome', ok: false, error: new Error('offline') }]);
    const empty = await runPure(lifecycleTick(env), { state: buildState() });
    expect(empty.timers.get('lifecycle')!.ms).toBe(300_000);
  });
});

describe('ackPrune', () => {
  it('drops expired acked items and re-arms while some remain', async () => {
    const now = 100_000;
    const s = buildState([], R.outboxReplace([
      buildOutboxItem({ id: 'old', status: 'acked', ackedAt: now - 31_000 }),
      buildOutboxItem({ id: 'fresh', status: 'acked', ackedAt: now - 1000 }),
    ]));
    const out = await runPure(ackPrune(), { state: s, now });
    expect(out.state.outbox.map((i) => i.id)).toEqual(['fresh']);
    expect(out.timers.get('ack-prune')).toEqual({ ms: 30_000, event: { type: 'AckPrune' } });
    const done = await runPure(ackPrune(), { state: out.state, now: now + 60_000 });
    expect(done.timers.size).toBe(0);
  });
});

describe('gcTick', () => {
  it('deletes bodies no view names and no outbox item touches; keeps _00_ rows; reschedules', async () => {
    const s = buildState(
      [],
      R.setVersions([['thing:keep', 1], ['thing:gone', 1], ['thing:pending', 1], ['_00_view:x', 1], ['thing:failed', 1]]),
      R.outboxReplace([buildOutboxItem({ recordId: 'thing:pending' })])
    );
    const deleted: string[] = [];
    const out = await runPure(gcTick(), {
      state: s,
      handlers: {
        'local.query': () => [[{ ids: [['thing:keep', 1]] }, { ids: 'bad' }]],
        'local.delete': (e: any) => {
          deleted.push(e.id);
          if (e.id === 'thing:failed') throw new Error('busy');
        },
        'ssp.ingest': () => undefined,
      },
    });
    expect(deleted).toEqual(['thing:gone', 'thing:failed']);
    expect([...out.state.versions.keys()]).toEqual(['thing:keep', 'thing:pending', '_00_view:x', 'thing:failed']);
    const ingest = out.log.find((e) => e.kind === 'ssp.ingest') as any;
    expect(ingest.records).toEqual([{ table: 'thing', op: 'DELETE', id: 'thing:gone', record: {} }]);
    expect(out.timers.get('gc')!.event).toEqual({ type: 'GcTick' });
    const failing = await runPure(gcTick(), {
      state: s,
      handlers: {
        'local.query': () => {
          throw new Error('no table');
        },
      },
    });
    expect(failing.emitted).toEqual([expect.objectContaining({ message: 'orphan gc failed' })]);
    expect(failing.timers.has('gc')).toBe(true);
    const nothing = await runPure(gcTick(), { state: buildState(), handlers: { 'local.query': () => [] } });
    expect(nothing.log.filter((e) => e.kind === 'ssp.ingest')).toHaveLength(0);
  });
});

describe('evictQuery', () => {
  it('is a no-op for unknown hashes', async () => {
    const out = await runPure(evictQuery('zz'), { state: buildState() });
    expect(out.emitted).toEqual([]);
  });
});
