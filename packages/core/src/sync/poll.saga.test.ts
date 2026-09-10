import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { pollTick } from './poll.saga';

const env = defaultEnv({ tables: [] } as any);
const ok = (result: unknown) => ({ status: 'OK', result });

describe('pollTick', () => {
  it('with no queries: probes connectivity, backs off while idle', async () => {
    const out = await runPure(pollTick(env), { state: R.patchSync({ pollIdleStreak: 2 })(buildState()), handlers: { 'remote.query': () => [ok(true)] } });
    expect(out.dispatched).toEqual([{ type: 'SyncOutcome', ok: true }]);
    expect(out.state.sync.pollIdleStreak).toBe(3);
    expect(out.timers.get('poll')).toEqual({ ms: 4000, event: { type: 'PollTick' } });
    const failed = await runPure(pollTick(env), {
      state: buildState(),
      handlers: {
        'remote.query': () => {
          throw new Error('offline');
        },
      },
    });
    expect(failed.dispatched).toEqual([{ type: 'SyncOutcome', ok: false, error: new Error('offline') }]);
  });
  it('with queries: reads membership; a change or an acked write resets the streak', async () => {
    const entry = buildEntry({ def: { hash: 'a', id: new RecordId('_00_query', 'a') }, lifecycle: { phase: 'live' }, serverState: 'ready' });
    const changed = await runPure(pollTick(env), {
      state: R.patchSync({ pollIdleStreak: 5 })(buildState([entry])),
      handlers: {
        'remote.query': () => [ok([{ out: new RecordId('thing', '1'), version: 1 }]), ok({ rowCount: 1, state: 'ready' }), ok([])],
        'local.upsert': () => undefined,
      },
    });
    expect(changed.state.sync.pollIdleStreak).toBe(0);
    expect(changed.timers.get('poll')!.ms).toBe(500);
    const quiet = await runPure(pollTick(env), {
      state: R.patchSync({ pollIdleStreak: 1 })(buildState([entry])),
      handlers: { 'remote.query': () => [ok([]), ok({ rowCount: 0, state: 'ready' }), ok([])], 'local.upsert': () => undefined },
    });
    expect(quiet.state.sync.pollIdleStreak).toBe(2);
    expect(quiet.timers.get('poll')!.ms).toBe(2000);
    const acked = await runPure(pollTick(env), {
      state: buildState([entry], R.patchSync({ pollIdleStreak: 4 }), R.outboxReplace([buildOutboxItem({ status: 'acked', ackedAt: 1 })])),
      handlers: { 'remote.query': () => [ok([]), ok({ rowCount: 0, state: 'ready' }), ok([])], 'local.upsert': () => undefined },
    });
    expect(acked.state.sync.pollIdleStreak).toBe(0);
  });
});
