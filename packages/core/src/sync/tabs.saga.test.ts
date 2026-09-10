import { describe, expect, it } from 'vitest';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { setRole, tabMessage } from './tabs.saga';

const env = defaultEnv({ tables: [] } as any);

describe('setRole', () => {
  it('leader: loads the outbox and starts live/poll/registrations; follower: stops its timers; same role is a no-op', async () => {
    const leader = await runPure(setRole(env, 'leader'), { state: buildState(), handlers: { 'local.query': () => [] } });
    expect(leader.state.tabRole).toBe('leader');
    expect(leader.dispatched).toEqual([{ type: 'LiveStart' }, { type: 'PollTick' }, { type: 'EnsureRegistered' }]);
    const follower = await runPure(setRole(env, 'follower'), { state: R.patchSync({ liveUuid: 'u' })(buildState()) });
    expect(follower.state.tabRole).toBe('follower');
    expect(follower.state.sync.liveUuid).toBeNull();
    expect(follower.log.filter((e) => e.kind === 'timer.clear').map((e) => (e as any).key)).toEqual(['poll', 'outbox']);
    const same = await runPure(setRole(env, 'solo'), { state: buildState() });
    expect(same.log).toHaveLength(1);
  });
});

describe('tabMessage', () => {
  const s = () => buildState([buildEntry({ def: { hash: 'a', tableName: 'thing' } })], R.outboxReplace([buildOutboxItem({ id: 'm1', recordId: 'thing:1' })]));
  it('ignores junk', async () => {
    for (const junk of [null, 'x', {}, { type: 'weird' }, { type: 'ingest', records: [] }, { type: 'membership-dirty' }, { type: 'failed-mutations-changed' }]) {
      const out = await runPure(tabMessage(env, junk), { state: s() });
      expect(out.emitted).toEqual([]);
    }
  });
  it('ingest: feeds the circuit, tracks versions, dirties tables; a failing ingest is logged', async () => {
    const msg = {
      type: 'ingest',
      records: [
        { table: 'thing', op: 'CREATE', id: 'thing:2', record: { _00_rv: 3 } },
        { table: 'thing', op: 'DELETE', id: 'thing:1', record: {} },
        { table: 'other', op: 'UPDATE', id: 'other:1', record: {} },
      ],
    };
    const out = await runPure(tabMessage(env, msg), { state: R.setVersions([['thing:1', 1]])(s()), handlers: { 'ssp.ingest': () => undefined } });
    expect(out.state.versions.get('thing:2')).toBe(3);
    expect(out.state.versions.get('other:1')).toBe(0);
    expect(out.state.versions.has('thing:1')).toBe(false);
    expect(out.state.dirty.has('a')).toBe(true);
    const failing = await runPure(tabMessage(env, msg), {
      state: s(),
      handlers: {
        'ssp.ingest': () => {
          throw new Error('wasm');
        },
      },
    });
    expect(failing.emitted).toEqual([expect.objectContaining({ level: 'warn' })]);
  });
  it('membership-dirty, outbox-changed (leader only), settled, rolled-back, tray count', async () => {
    const dirty = await runPure(tabMessage(env, { type: 'membership-dirty', hashes: ['a'] }), { state: { ...s(), tabRole: 'follower' } });
    expect(dirty.state.membershipDirty.has('a')).toBe(true);
    const ob = await runPure(tabMessage(env, { type: 'outbox-changed', mutationId: 'm' }), { state: { ...s(), tabRole: 'leader' }, handlers: { 'local.query': () => [] } });
    expect(ob.log.some((e) => e.kind === 'local.query')).toBe(true);
    const obFollower = await runPure(tabMessage(env, { type: 'outbox-changed', mutationId: 'm' }), { state: { ...s(), tabRole: 'follower' } });
    expect(obFollower.log.some((e) => e.kind === 'local.query')).toBe(false);
    const settled = await runPure(tabMessage(env, { type: 'mutation-settled', mutationId: 'm1', recordId: 'thing:1', eventType: 'create' }), { state: s(), now: 9 });
    expect(settled.state.outbox[0]).toMatchObject({ status: 'acked', ackedAt: 9 });
    expect(settled.emitted).toEqual([{ type: 'mutation:settled', mutationId: 'm1', recordId: 'thing:1', eventType: 'create' }]);
    const rolled = await runPure(tabMessage(env, { type: 'mutation-rolled-back', mutationId: 'm1', recordId: 'thing:1', eventType: 'create', error: 'denied' }), { state: s() });
    expect(rolled.state.outbox).toEqual([]);
    expect(rolled.state.dirty.has('a')).toBe(true);
    expect(rolled.emitted[0]).toMatchObject({ type: 'mutation:rolled-back', error: 'denied' });
    const tray = await runPure(tabMessage(env, { type: 'failed-mutations-changed', count: 4 }), { state: s() });
    expect(tray.state.failedCount).toBe(4);
    expect(tray.emitted).toEqual([{ type: 'tray:changed', count: 4 }]);
  });
});
