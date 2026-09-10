import { describe, expect, it } from 'vitest';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { liveChange, liveInvalidate, liveStart } from './live.saga';

const env = defaultEnv({ tables: [] } as any);

describe('liveStart', () => {
  it('subscribes on the session table and records the uuid; no-op when already on it; followers never subscribe', async () => {
    const out = await runPure(liveStart(env), { state: { ...buildState(), userId: 'user:abc' }, handlers: { 'remote.live': (e: any) => `uuid-${e.table}` } });
    expect(out.state.sync).toMatchObject({ liveUuid: 'uuid-_00_list_ref_user_abc', liveTable: '_00_list_ref_user_abc' });
    const same = await runPure(liveStart(env), { state: out.state });
    expect(same.log.filter((e) => e.kind === 'remote.live')).toHaveLength(0);
    const follower = await runPure(liveStart(env), { state: { ...buildState(), tabRole: 'follower' } });
    expect(follower.log.filter((e) => e.kind === 'remote.live')).toHaveLength(0);
  });
  it('kills the previous subscription when connected (ignoring kill errors), tolerates a failing subscribe', async () => {
    const prev = R.compose(R.patchSync({ liveUuid: 'old', liveTable: '_00_list_ref' }), R.setConnection('connected'))({ ...buildState(), userId: 'user:x' });
    const killed: string[] = [];
    const out = await runPure(liveStart(env), {
      state: prev,
      handlers: {
        'remote.kill': (e: any) => {
          killed.push(e.uuid);
          throw new Error('gone');
        },
        'remote.live': () => 'new',
      },
    });
    expect(killed).toEqual(['old']);
    expect(out.state.sync.liveUuid).toBe('new');
    const clean = await runPure(liveStart(env), { state: prev, handlers: { 'remote.kill': () => undefined, 'remote.live': () => 'new2' } });
    expect(clean.state.sync.liveUuid).toBe('new2');
    expect(clean.emitted).toEqual([]);
    const offline = R.patchSync({ liveUuid: 'old', liveTable: '_00_list_ref' })({ ...buildState(), userId: 'user:x' });
    const noKill = await runPure(liveStart(env), {
      state: offline,
      handlers: {
        'remote.live': () => {
          throw new Error('not connected');
        },
      },
    });
    expect(noKill.log.filter((e) => e.kind === 'remote.kill')).toHaveLength(0);
    expect(noKill.state.sync.liveUuid).toBeNull();
    expect(noKill.emitted).toEqual([expect.objectContaining({ level: 'warn' })]);
  });
  it('liveInvalidate clears the bookkeeping', async () => {
    const out = await runPure(liveInvalidate(), { state: R.patchSync({ liveUuid: 'u', liveTable: 't' })(buildState()) });
    expect(out.state.sync).toMatchObject({ liveUuid: null, liveTable: null });
  });
});

describe('liveChange', () => {
  it('marks known hashes dirty, resets the poll streak, relays as leader; ignores unknown hashes', async () => {
    const s = R.patchSync({ pollIdleStreak: 3 })(buildState([buildEntry({ def: { hash: 'a' } })]));
    const out = await runPure(liveChange(['a', 'zz']), { state: { ...s, tabRole: 'leader' } });
    expect([...out.state.membershipDirty]).toEqual(['a']);
    expect(out.state.sync.pollIdleStreak).toBe(0);
    expect(out.timers.get('membership')).toEqual({ ms: 50, event: { type: 'ReadDirtyMembership' } });
    expect(out.emitted).toEqual([{ type: 'tabs:broadcast', message: { type: 'membership-dirty', hashes: ['a'] } }]);
    const solo = await runPure(liveChange(['a']), { state: s });
    expect(solo.emitted).toEqual([]);
    const none = await runPure(liveChange(['zz']), { state: s });
    expect(none.timers.size).toBe(0);
  });
});
