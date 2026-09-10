import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from './env';
import { applyMembership, applySubqueryChildren, markMembershipDirty, readDirtyMembership, readMembership } from './membership.saga';
import type { StatementResult } from '../kernel/effects';

const env = defaultEnv({ tables: [] } as any);
const ok = (result: unknown): StatementResult => ({ status: 'OK', result });
const ready = (rowCount: number) => ({ present: true, rowCount, state: 'ready' as const });

describe('applyMembership', () => {
  it('ignored: unknown hash, or an unexplained empty', async () => {
    expect((await runPure(applyMembership('zz', [['t:1', 1]]))).result).toBe('ignored');
    const s = buildState([buildEntry({ def: { hash: 'a' }, lifecycle: { phase: 'cold' } })]);
    const out = await runPure(applyMembership('a', [], { present: true, rowCount: 2, state: 'ready' }), { state: s });
    expect(out.result).toBe('ignored');
    expect(out.state.queries.get('a')!.serverState).toBe('ready');
  });
  it('view-lost: flips once, drops the registration, re-registers', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' }, lifecycle: { phase: 'live', remote: 'registered' }, remoteArray: [['t:1', 1]] })]);
    const out = await runPure(applyMembership('a', [], { present: false, rowCount: null, state: null }), { state: s });
    expect(out.result).toBe('view-lost');
    expect(out.state.queries.get('a')!.lifecycle).toMatchObject({ phase: 'view-lost', remote: 'unregistered' });
    expect(out.emitted).toEqual([{ type: 'query:view-lost', hash: 'a' }]);
    expect(out.dispatched).toEqual([{ type: 'EnsureRegistered' }]);
    const again = await runPure(applyMembership('a', []), { state: out.state });
    expect(again.emitted).toEqual([]);
    expect(again.dispatched).toEqual([{ type: 'EnsureRegistered' }]);
  });
  it('applied: commits, writes the view row, flips authority once, releases acked items, asks for bodies', async () => {
    const s = buildState(
      [buildEntry({ def: { hash: 'a', viewKey: 'vk' }, lifecycle: { phase: 'cold' } })],
      R.outboxReplace([buildOutboxItem({ id: 'm', recordId: 't:1', status: 'acked', ackedAt: 1 })])
    );
    const out = await runPure(applyMembership('a', [['t:1', 1]], ready(1)), { state: s, now: 42, handlers: { 'local.upsert': () => undefined } });
    expect(out.result).toBe('applied');
    expect(out.state.queries.get('a')!.lifecycle.phase).toBe('live');
    expect(out.state.outbox).toEqual([]);
    expect(out.emitted).toEqual([{ type: 'query:authority', hash: 'a', known: true }]);
    expect(out.log.find((e) => e.kind === 'local.upsert')).toEqual({
      kind: 'local.upsert',
      table: '_00_view',
      id: new RecordId('_00_view', 'vk'),
      data: { ids: [['t:1', 1]], confirmed: true, updatedAt: 42 },
      mode: 'replace',
    });
    expect(out.dispatched).toEqual([{ type: 'FetchRows' }]);
    const second = await runPure(applyMembership('a', [['t:1', 2]], ready(1)), {
      state: out.state,
      handlers: {
        'local.upsert': () => {
          throw new Error('disk');
        },
      },
    });
    expect(second.emitted).toEqual([expect.objectContaining({ type: 'log', level: 'debug' })]);
  });
  it('verified removal applies an empty set without a row', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' }, lifecycle: { phase: 'live' }, remoteArray: [['t:1', 1]] })]);
    const out = await runPure(applyMembership('a', [], undefined, true), { state: s, handlers: { 'local.upsert': () => undefined } });
    expect(out.result).toBe('applied');
    expect(out.state.queries.get('a')!.remoteArray).toEqual([]);
  });
});

describe('applySubqueryChildren', () => {
  it('no-op when equal or unknown; otherwise sets and asks for bodies', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' }, subqueryRemoteArray: [['c:1', 1]] })]);
    expect((await runPure(applySubqueryChildren('a', [['c:1', 1]]), { state: s })).dispatched).toEqual([]);
    expect((await runPure(applySubqueryChildren('zz', []), { state: s })).dispatched).toEqual([]);
    const out = await runPure(applySubqueryChildren('a', [['c:2', 1]]), { state: s });
    expect(out.state.queries.get('a')!.subqueryRemoteArray).toEqual([['c:2', 1]]);
    expect(out.dispatched).toEqual([{ type: 'FetchRows' }]);
  });
});

describe('readMembership', () => {
  const qid = (h: string) => new RecordId('_00_query', h);
  const out1 = (id: string, v = 1) => ({ out: new RecordId('thing', id), version: v });
  it('nothing to read', async () => {
    expect((await runPure(readMembership(env, ['nope']), { state: buildState() })).result).toEqual({ changed: false, failed: false });
  });
  it('single query: one single-shape request; applies a changed set and clears dirt', async () => {
    const s = R.markMembershipDirty(['a'])(buildState([buildEntry({ def: { hash: 'a', id: qid('a') }, lifecycle: { phase: 'live' }, remoteArray: [['thing:1', 1]] })]));
    const out = await runPure(readMembership(env, ['a']), {
      state: s,
      handlers: {
        'remote.query': (e: any) => {
          expect(e.sql.startsWith('SELECT out, version FROM _00_list_ref WHERE in = $in')).toBe(true);
          return [ok([out1('1'), out1('2')]), ok({ rowCount: 2, state: 'ready' }), ok([])];
        },
        'local.upsert': () => undefined,
      },
    });
    expect(out.result).toEqual({ changed: true, failed: false });
    expect(out.state.queries.get('a')!.remoteArray).toEqual([['thing:1', 1], ['thing:2', 1]]);
    expect(out.state.membershipDirty.size).toBe(0);
    expect(out.dispatched.at(-1)).toEqual({ type: 'SyncOutcome', ok: true, error: undefined });
  });
  it('equal set on a live query applies nothing; a cached query with the same set still flips to live', async () => {
    const live = buildState([buildEntry({ def: { hash: 'a', id: qid('a') }, lifecycle: { phase: 'live' }, remoteArray: [['thing:1', 1]], serverState: 'ready' })]);
    const handlers = { 'remote.query': () => [ok([out1('1')]), ok({ rowCount: 1, state: 'ready' }), ok([])], 'local.upsert': () => undefined };
    const a = await runPure(readMembership(env, ['a']), { state: live, handlers });
    expect(a.result.changed).toBe(false);
    expect(a.log.filter((e) => e.kind === 'local.upsert')).toHaveLength(0);
    const cached = buildState([buildEntry({ def: { hash: 'a', id: qid('a') }, lifecycle: { phase: 'cached' }, remoteArray: [['thing:1', 1]], serverState: 'ready' })]);
    const b = await runPure(readMembership(env, ['a']), { state: cached, handlers });
    expect(b.state.queries.get('a')!.lifecycle.phase).toBe('live');
  });
  it('many queries: one batch request; suspect entries are re-read singly; children applied', async () => {
    const s = buildState([
      buildEntry({ def: { hash: 'a', id: qid('a') }, lifecycle: { phase: 'live' }, remoteArray: [['thing:1', 1]] }),
      buildEntry({ def: { hash: 'b', id: qid('b') }, lifecycle: { phase: 'live' }, remoteArray: [['thing:2', 1]] }),
    ]);
    const calls: string[] = [];
    const out = await runPure(readMembership(env, ['a', 'b']), {
      state: s,
      handlers: {
        'remote.query': (e: any) => {
          calls.push(e.sql.split('\n')[0]);
          if (e.sql.includes('IN $ins')) {
            return [
              ok([{ in: qid('a'), out: new RecordId('thing', '1'), version: 1 }, { in: qid('a'), out: new RecordId('child', 'x'), version: 1, parent: qid('a') }]),
              ok([{ id: qid('a'), rowCount: 1, state: 'ready' }]),
            ];
          }
          return [ok([out1('2'), out1('3')]), ok({ rowCount: 2, state: 'ready' }), ok([])];
        },
        'local.upsert': () => undefined,
      },
    });
    expect(calls).toEqual(['SELECT in, out, version, parent FROM _00_list_ref WHERE in IN $ins;', 'SELECT out, version FROM _00_list_ref WHERE in = $in AND parent IS NONE;']);
    expect(out.state.queries.get('a')!.subqueryRemoteArray).toEqual([['child:x', 1]]);
    expect(out.state.queries.get('b')!.remoteArray).toEqual([['thing:2', 1], ['thing:3', 1]]);
    expect(out.result.changed).toBe(true);
  });
  it('failures: a failed chunk or re-read reports failed; a non-array primary is skipped', async () => {
    const s = buildState([
      buildEntry({ def: { hash: 'a', id: qid('a') }, lifecycle: { phase: 'live' }, remoteArray: [['thing:1', 1]] }),
      buildEntry({ def: { hash: 'b', id: qid('b') }, lifecycle: { phase: 'live' }, remoteArray: [['thing:2', 1]] }),
    ]);
    const chunkFail = await runPure(readMembership(env, ['a', 'b']), {
      state: s,
      handlers: {
        'remote.query': () => {
          throw new Error('offline');
        },
      },
    });
    expect(chunkFail.result).toEqual({ changed: false, failed: true });
    expect(chunkFail.dispatched.at(-1)).toMatchObject({ type: 'SyncOutcome', ok: false });
    let n = 0;
    const rereadFail = await runPure(readMembership(env, ['a', 'b']), {
      state: s,
      handlers: {
        'remote.query': (e: any) => {
          n++;
          if (e.sql.includes('IN $ins')) return [ok([]), ok([])];
          throw new Error('offline');
        },
      },
    });
    expect(rereadFail.result.failed).toBe(true);
    expect(n).toBe(3);
    const single = buildState([buildEntry({ def: { hash: 'a', id: qid('a') }, lifecycle: { phase: 'live' }, remoteArray: [['thing:1', 1]] })]);
    const notArray = await runPure(readMembership(env, ['a']), { state: single, handlers: { 'remote.query': () => [ok('x'), ok(null), ok(null)] } });
    expect(notArray.result).toEqual({ changed: false, failed: false });
    const batchNotArray = await runPure(readMembership(env, ['a', 'b']), { state: s, handlers: { 'remote.query': () => [ok(null), ok(null)] } });
    expect(batchNotArray.result).toEqual({ changed: false, failed: false });
    const undefinedResult = await runPure(readMembership(env, ['a']), { state: single, handlers: { 'remote.query': () => undefined } });
    expect(undefinedResult.result).toEqual({ changed: false, failed: false });
    const errStmt = await runPure(readMembership(env, ['a']), { state: single, handlers: { 'remote.query': () => [{ status: 'ERR', error: 'x' }, ok(null), ok(null)] } });
    expect(errStmt.result).toEqual({ changed: false, failed: false });
  });
  it('materializing views walk the re-read ladder then stop', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a', id: qid('a') }, lifecycle: { phase: 'cold' } })]);
    const mat = { 'remote.query': () => [ok([]), ok({ rowCount: 2, state: 'materializing' }), ok([])] };
    const r1 = await runPure(readMembership(env, ['a']), { state: s, handlers: mat });
    expect(r1.state.membershipReread.get('a')).toBe(1);
    expect(r1.state.membershipDirty.has('a')).toBe(true);
    expect(r1.timers.get('membership')).toEqual({ ms: 150, event: { type: 'ReadDirtyMembership' } });
    const r2 = await runPure(readMembership(env, ['a']), { state: r1.state, handlers: mat });
    expect(r2.timers.get('membership')!.ms).toBe(400);
    const r3 = await runPure(readMembership(env, ['a']), { state: r2.state, handlers: mat });
    expect(r3.timers.get('membership')!.ms).toBe(1000);
    const r4 = await runPure(readMembership(env, ['a']), { state: r3.state, handlers: mat });
    expect(r4.timers.size).toBe(0);
    expect(r4.state.membershipReread.has('a')).toBe(false);
    const landed = await runPure(readMembership(env, ['a']), {
      state: r2.state,
      handlers: { 'remote.query': () => [ok([out1('1')]), ok({ rowCount: 1, state: 'ready' }), ok([])], 'local.upsert': () => undefined },
    });
    expect(landed.state.membershipReread.has('a')).toBe(false);
  });
  it('skips entries that vanish between the read and the apply', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a', id: qid('a') }, lifecycle: { phase: 'live' }, remoteArray: [] })]);
    const out = await runPure(readMembership(env, ['a']), {
      state: s,
      handlers: {
        'remote.query': (_e, ctx) => {
          ctx.state = R.removeQuery('a')(ctx.state);
          return [ok([out1('1')]), ok({ rowCount: 1, state: 'ready' }), ok([])];
        },
      },
    });
    expect(out.result.changed).toBe(false);
  });
});

describe('dirty helpers', () => {
  it('markMembershipDirty coalesces into one timer; readDirtyMembership is a no-op when clean', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' } })]);
    const out = await runPure(markMembershipDirty(['a']), { state: s });
    expect(out.state.membershipDirty.has('a')).toBe(true);
    expect(out.timers.get('membership')).toEqual({ ms: 50, event: { type: 'ReadDirtyMembership' } });
    const clean = await runPure(readDirtyMembership(env), { state: s });
    expect(clean.log.filter((e) => e.kind === 'remote.query')).toHaveLength(0);
    const dirty = await runPure(readDirtyMembership(env), {
      state: out.state,
      handlers: { 'remote.query': () => [ok([]), ok(null), ok([])] },
    });
    expect(dirty.log.filter((e) => e.kind === 'remote.query')).toHaveLength(1);
  });

  it('arms the window once per burst, and again for dirt the read left behind', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' } }), buildEntry({ def: { hash: 'b' } })]);
    const first = await runPure(markMembershipDirty(['a']), { state: s });
    // A second event inside the same window must NOT push the read out: with
    // `timer.set` replacing the pending timer, re-arming on every LIVE event
    // starved the read on a busy table.
    const second = await runPure(markMembershipDirty(['b']), { state: first.state });
    expect(second.timers.has('membership')).toBe(false);
    expect([...second.state.membershipDirty]).toEqual(['a', 'b']);

    // A read that cannot answer a hash leaves it dirty, and re-arms for it.
    const stuck = await runPure(readDirtyMembership(env), {
      state: second.state,
      handlers: { 'remote.query': () => { throw new Error('offline'); } },
    });
    expect(stuck.state.membershipDirty.size).toBe(2);
    expect(stuck.timers.get('membership')).toEqual({ ms: 50, event: { type: 'ReadDirtyMembership' } });
  });
});
