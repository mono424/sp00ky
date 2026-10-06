import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from './env';
import { ackPrune, evictQuery, gcTick, lifecycleTick, loadViews } from './lifecycle.saga';
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

describe('loadViews', () => {
  it('fills the index from every _00_view row (members and children); a failed read leaves it empty', async () => {
    const rows = [
      { id: '_00_view:a', ids: [['t:1', 1]], children: [['c:1', 1], 'junk'] },
      { id: new RecordId('_00_view', 'b'), ids: [] },
      { id: '', ids: [] },
      { ids: [['t:no-key', 1]] },
      null,
    ];
    const out = await runPure(loadViews(), { state: buildState(), handlers: { 'local.query': () => [rows] } });
    expect(out.state.views).toEqual(new Map([['a', ['t:1', 'c:1']], ['b', []]]));
    const odd = await runPure(loadViews(), { state: buildState(), handlers: { 'local.query': () => undefined } });
    expect(odd.state.views.size).toBe(0);
    const failed = await runPure(loadViews(), {
      state: buildState(),
      handlers: {
        'local.query': () => {
          throw new Error('no table');
        },
      },
    });
    expect(failed.state.views.size).toBe(0);
    expect(failed.emitted).toEqual([expect.objectContaining({ level: 'warn', message: 'view index read failed' })]);
  });
});

describe('gcTick', () => {
  const day = 24 * 60 * 60 * 1000;
  const now = 100 * day;
  const ready = R.setIdentity({ primed: true, bucketId: 'u1' });
  const deletedIds = (log: Array<{ kind: string }>) =>
    log.filter((e: any) => e.kind === 'local.delete').map((e: any) => (e.table === '_00_view' ? `view:${e.id.id}` : e.id));

  it('retires stale view rows, deletes the bodies nothing retains from the store and the circuit, re-fetches, reschedules', async () => {
    const s = buildState(
      [buildEntry({ def: { hash: 'h', viewKey: 'held' }, lifecycle: { phase: 'cached' }, remoteArray: [['thing:mem', 1]] })],
      ready,
      R.setVersions(
        ['thing:keep', 'child:1', 'thing:stale', 'thing:held', 'thing:stuck', 'thing:mem', 'thing:gone', 'thing:pending', '_00_view:x', 'thing:failed'].map(
          (id) => [id, 1] as const
        )
      ),
      R.outboxReplace([buildOutboxItem({ recordId: 'thing:pending' })])
    );
    const rows = [
      { id: '_00_view:fresh', ids: [['thing:keep', 1]], children: [['child:1', 1]], updatedAt: now - day },
      // Nobody resolved this query for 15 days: the row is retired and stops vouching.
      { id: new RecordId('_00_view', 'old'), ids: [['thing:stale', 1]], updatedAt: now - 15 * day },
      // As old, but a query in state holds it.
      { id: '_00_view:held', ids: [['thing:held', 1]] },
      // As old, but its delete fails: it keeps vouching.
      { id: '_00_view:stuck', ids: [['thing:stuck', 1]], updatedAt: 0 },
      { ids: 'bad' },
    ];
    const out = await runPure(gcTick(), {
      state: s,
      now,
      handlers: {
        'local.query': (e: any) => {
          expect(e.sql).toBe('SELECT * FROM _00_view');
          return [rows];
        },
        'local.delete': (e: any) => {
          if (e.id === 'thing:failed' || e.id?.id === 'stuck') throw new Error('busy');
        },
        'ssp.ingest': () => undefined,
      },
    });
    expect(deletedIds(out.log)).toEqual(['view:old', 'view:stuck', 'thing:stale', 'thing:gone', 'thing:failed']);
    expect(out.state.views).toEqual(
      new Map([
        ['fresh', ['thing:keep', 'child:1']],
        ['held', ['thing:held']],
        ['stuck', ['thing:stuck']],
      ])
    );
    expect([...out.state.versions.keys()]).toEqual(['thing:keep', 'child:1', 'thing:held', 'thing:stuck', 'thing:mem', 'thing:pending', '_00_view:x', 'thing:failed']);
    const ingest = out.log.find((e) => e.kind === 'ssp.ingest') as any;
    expect(ingest.records).toEqual([
      { table: 'thing', op: 'DELETE', id: 'thing:stale', record: {} },
      { table: 'thing', op: 'DELETE', id: 'thing:gone', record: {} },
    ]);
    // Solo: nobody to relay to.
    expect(out.emitted.filter((e) => e.type === 'tabs:broadcast')).toEqual([]);
    expect(out.emitted).toContainEqual(expect.objectContaining({ message: 'orphan gc done', data: { removed: 2, retiredViews: 1 } }));
    expect(out.dispatched).toEqual([{ type: 'FetchRows' }]);
    expect(out.timers.get('gc')).toEqual({ ms: 60 * 60 * 1000, event: { type: 'GcTick' } });
  });

  it('waits for the circuit prime (the versions); a leader relays its deletes to the other tabs', async () => {
    const s = buildState([], R.setIdentity({ bucketId: 'u1' }), R.setTabRole('leader'), R.setVersions([['thing:gone', 1]]));
    const out = await runPure(gcTick(), {
      state: s,
      handlers: {
        'state.wait': (e: any, ctx) => {
          expect(e.until(ctx.state)).toBe(false);
          ctx.state = R.setIdentity({ primed: true })(ctx.state);
        },
        'local.query': () => [[]],
        'local.delete': () => undefined,
        'ssp.ingest': () => undefined,
      },
    });
    expect(out.emitted).toContainEqual({
      type: 'tabs:broadcast',
      message: { type: 'ingest', records: [{ table: 'thing', op: 'DELETE', id: 'thing:gone', record: {} }] },
    });
  });

  it('a follower only refreshes its index: no view row or body is deleted', async () => {
    const s = buildState([], ready, R.setTabRole('follower'), R.setVersions([['thing:gone', 1]]));
    const out = await runPure(gcTick(), {
      state: s,
      now,
      handlers: { 'local.query': () => [[{ id: '_00_view:old', ids: [['thing:x', 1]], updatedAt: 0 }]] },
    });
    expect(out.state.views).toEqual(new Map([['old', ['thing:x']]]));
    expect(deletedIds(out.log)).toEqual([]);
    expect(out.state.versions.has('thing:gone')).toBe(true);
    expect(out.timers.has('gc')).toBe(true);
  });

  it('each chunk is re-checked against fresh state, and the sweep stops when the bucket moves', async () => {
    const ids = Array.from({ length: 250 }, (_, i) => `thing:o${i}`);
    const s = buildState([], ready, R.setVersions(ids.map((id) => [id, 1] as const)));
    const handlers = (onFirst: (ctx: any) => void) => ({
      'local.query': () => [[]],
      'local.delete': (e: any, ctx: any) => {
        if (e.id === 'thing:o0') onFirst(ctx);
      },
      'ssp.ingest': () => undefined,
    });
    // Meanwhile a query commits o210 and o220's body goes another way: neither is deleted.
    const recheck = await runPure(gcTick(), {
      state: s,
      handlers: handlers((ctx) => {
        ctx.state = R.compose(R.putQuery(buildEntry({ def: { hash: 'q' }, remoteArray: [['thing:o210', 1]] })), R.deleteVersions(['thing:o220']))(ctx.state);
      }),
    });
    const removed = deletedIds(recheck.log);
    expect(removed).toHaveLength(248);
    expect(removed).not.toContain('thing:o210');
    expect(removed).not.toContain('thing:o220');
    expect(recheck.log.filter((e) => e.kind === 'ssp.ingest')).toHaveLength(2);
    const moved = await runPure(gcTick(), { state: s, handlers: handlers((ctx) => (ctx.state = R.setIdentity({ bucketId: 'u2' })(ctx.state))) });
    expect(deletedIds(moved.log)).toHaveLength(200);
    expect(moved.timers.has('gc')).toBe(true);
  });

  it('failures are logged and the tick still reschedules; a sweep that removes nothing asks for nothing', async () => {
    const s = buildState([], ready, R.setVersions([['thing:gone', 1]]));
    const readFails = await runPure(gcTick(), {
      state: s,
      handlers: {
        'local.query': () => {
          throw new Error('no table');
        },
      },
    });
    expect(readFails.emitted).toEqual([expect.objectContaining({ message: 'orphan gc failed' })]);
    expect(readFails.timers.has('gc')).toBe(true);
    const ingestFails = await runPure(gcTick(), {
      state: s,
      handlers: {
        'local.query': () => [[]],
        'local.delete': () => undefined,
        'ssp.ingest': () => {
          throw new Error('wasm');
        },
      },
    });
    expect(ingestFails.emitted).toEqual([expect.objectContaining({ message: 'orphan gc failed' })]);
    expect(ingestFails.timers.has('gc')).toBe(true);
    const deleteFails = await runPure(gcTick(), {
      state: s,
      handlers: {
        'local.query': () => [[]],
        'local.delete': () => {
          throw new Error('busy');
        },
      },
    });
    expect(deleteFails.log.filter((e) => e.kind === 'ssp.ingest')).toHaveLength(0);
    expect(deleteFails.dispatched).toEqual([]);
    expect(deleteFails.emitted).toContainEqual(expect.objectContaining({ message: 'orphan gc done', data: { removed: 0, retiredViews: 0 } }));
    const nothing = await runPure(gcTick(), { state: buildState([], ready), handlers: { 'local.query': () => [] } });
    expect(nothing.log.filter((e) => e.kind === 'local.delete')).toHaveLength(0);
    expect(nothing.dispatched).toEqual([]);
  });
});

describe('evictQuery', () => {
  it('is a no-op for unknown hashes', async () => {
    const out = await runPure(evictQuery('zz'), { state: buildState() });
    expect(out.emitted).toEqual([]);
  });
});
