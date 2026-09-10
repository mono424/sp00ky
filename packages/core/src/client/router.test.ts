import { describe, expect, it } from 'vitest';
import type { RuntimeEvent } from '../kernel/events';
import { defaultEnv } from '../query/env';
import { route } from './router';

const env = defaultEnv({ tables: [] } as any);

describe('route', () => {
  it('maps every event to a saga and the documented lane', () => {
    const cases: Array<[RuntimeEvent, string | undefined]> = [
      [{ type: 'EnsureRegistered' }, 'serial:ensure'],
      [{ type: 'RegisterRemote', hash: 'h' }, 'dedupe:register:h'],
      [{ type: 'ReadDirtyMembership' }, 'serial:membership'],
      [{ type: 'ReadMembership', hashes: ['h'] }, 'serial:membership'],
      [{ type: 'FetchRows' }, 'serial:fetch'],
      [{ type: 'Materialize', hash: 'h' }, 'serial:mat:h'],
      [{ type: 'MaterializeDirty' }, undefined],
      [{ type: 'StreamUpdate', update: { queryHash: 'h', localArray: [] } }, 'serial:stream:h'],
      [{ type: 'LifecycleTick' }, 'dedupe:lifecycle'],
      [{ type: 'HeartbeatNow' }, 'dedupe:lifecycle'],
      [{ type: 'AckPrune' }, 'dedupe:ack-prune'],
      [{ type: 'GcTick' }, 'dedupe:gc'],
      [{ type: 'Drain' }, 'serial:outbox'],
      [{ type: 'FlushWrite', key: 'k' }, 'serial:outbox-write'],
      [{ type: 'PollTick' }, 'dedupe:poll'],
      [{ type: 'SelfHealTick' }, 'dedupe:heal'],
      [{ type: 'SyncOutcome', ok: true }, 'serial:health'],
      [{ type: 'ConnectionChanged', state: 'connected' }, 'serial:connection'],
      [{ type: 'LiveStart' }, 'serial:live'],
      [{ type: 'LiveChange', hashes: [] }, 'serial:live-change'],
      [{ type: 'TabRole', role: 'leader' }, 'serial:tabs'],
      [{ type: 'TabMessage', message: {} }, 'serial:tabs'],
      [{ type: 'StartRemote' }, 'dedupe:start-remote'],
      [{ type: 'PrimeCircuit' }, 'dedupe:prime'],
      [{ type: 'VersionsPrimed', entries: [] }, undefined],
      [{ type: 'WarmBlobs' }, 'dedupe:blobs'],
      [{ type: 'AuthFlip', userId: null }, 'serial:bucket'],
      [{ type: 'BucketSwitch', target: 'x' }, 'serial:bucket'],
      [{ type: 'PageHide' }, undefined],
    ];
    for (const [event, lane] of cases) {
      const target = route(env, event);
      expect(typeof target.saga.next).toBe('function');
      expect(target.lane ? `${target.lane.kind}:${target.lane.key}` : undefined).toBe(lane);
      target.saga.return(undefined);
    }
  });
  it('MaterializeDirty fans out one Materialize per dirty hash', async () => {
    const { runPure } = await import('../testing/run-pure');
    const { buildEntry, buildState } = await import('../testing/build');
    const R = await import('../state/reducers');
    const s = R.markDirty(['a', 'b'])(buildState([buildEntry({ def: { hash: 'a' } }), buildEntry({ def: { hash: 'b' } })]));
    const out = await runPure(route(env, { type: 'MaterializeDirty' }).saga, { state: s });
    expect(out.dispatched).toEqual([{ type: 'Materialize', hash: 'a' }, { type: 'Materialize', hash: 'b' }]);
  });
});
