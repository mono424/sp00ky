import { describe, expect, it } from 'vitest';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { connectionChanged, selfHealTick, syncOutcome } from './connection.saga';

const env = defaultEnv({ tables: [] } as any);

describe('connectionChanged', () => {
  it('a drop arms resubscribe and invalidates LIVE; the initial connect does nothing else', async () => {
    const s = R.patchSync({ liveUuid: 'u', liveTable: 't' })(buildState());
    const dropped = await runPure(connectionChanged(env, 'reconnecting'), { state: s });
    expect(dropped.state.sync).toMatchObject({ needsResubscribe: true, liveUuid: null, health: { connection: 'reconnecting' } });
    const first = await runPure(connectionChanged(env, 'connected'), { state: buildState() });
    expect(first.dispatched).toEqual([]);
    const connecting = await runPure(connectionChanged(env, 'connecting'), { state: dropped.state });
    expect(connecting.dispatched).toEqual([]);
  });
  it('reconnect after a drop drops every registration and re-drives, once per cooldown window', async () => {
    const s = R.patchSync({ needsResubscribe: true })(
      buildState([
        buildEntry({ def: { hash: 'a' }, lifecycle: { remote: 'registered' } }),
        buildEntry({ def: { hash: 'b' }, lifecycle: { remote: 'failed' } }),
        buildEntry({ def: { hash: 'c' } }),
      ])
    );
    const out = await runPure(connectionChanged(env, 'connected'), { state: s, now: 1000 });
    expect([...out.state.queries.values()].map((e) => e.lifecycle.remote)).toEqual(['unregistered', 'unregistered', 'unregistered']);
    expect(out.state.sync).toMatchObject({ needsResubscribe: false, lastReconnectRefetchAt: 1000 });
    expect(out.dispatched).toEqual([{ type: 'EnsureRegistered', requireAuth: true }, { type: 'LiveStart' }, { type: 'Drain' }]);
    const burst = await runPure(connectionChanged(env, 'connected'), { state: R.patchSync({ needsResubscribe: true })(out.state), now: 5000 });
    expect(burst.dispatched).toEqual([]);
    expect(burst.state.sync.needsResubscribe).toBe(false);
    const later = await runPure(connectionChanged(env, 'connected'), { state: R.patchSync({ needsResubscribe: true })(out.state), now: 20_000 });
    expect(later.dispatched).toHaveLength(3);
  });
});

describe('syncOutcome', () => {
  it('degrades after the threshold (arming self-heal), recovers on success (disarming it)', async () => {
    let state = buildState();
    for (let i = 0; i < 2; i++) state = (await runPure(syncOutcome(env, false, new Error('socket')), { state })).state;
    expect(state.sync.health.status).toBe('healthy');
    const third = await runPure(syncOutcome(env, false, new Error('socket')), { state });
    expect(third.state.sync.health.status).toBe('degraded');
    // The saga emits nothing: the runtime notifies `health:changed` off the
    // health object's identity, which `setHealth` only replaces on a real move.
    expect(third.emitted).toEqual([]);
    expect(third.timers.get('heal')).toEqual({ ms: 2000, event: { type: 'SelfHealTick' } });
    expect(third.state.sync.selfHealAttempts).toBe(0);
    const back = await runPure(syncOutcome(env, true), { state: third.state });
    expect(back.state.sync.health.status).toBe('healthy');
    expect(back.state.sync.health).not.toBe(third.state.sync.health);
    expect(back.log.some((e) => e.kind === 'timer.clear' && (e as any).key === 'heal')).toBe(true);
    const quiet = await runPure(syncOutcome(env, true), { state: back.state });
    expect(quiet.state.sync.health).toBe(back.state.sync.health);
  });
});

describe('selfHealTick', () => {
  const degraded = (s: ReturnType<typeof buildState>) =>
    R.compose(R.setHealth({ ...s.sync.health, status: 'degraded' }), R.patchSync({ selfHealAttempts: 1 }))(s);
  it('does nothing when healthy; drains first, then re-registers (failed included), else probes; always re-arms', async () => {
    expect((await runPure(selfHealTick(env), { state: buildState() })).timers.size).toBe(0);
    const withOutbox = await runPure(selfHealTick(env), { state: degraded(buildState([], R.outboxReplace([buildOutboxItem()]))) });
    expect(withOutbox.dispatched).toEqual([{ type: 'Drain' }]);
    expect(withOutbox.timers.get('heal')).toEqual({ ms: 8000, event: { type: 'SelfHealTick' } });
    const withFailed = await runPure(selfHealTick(env), { state: degraded(buildState([buildEntry({ def: { hash: 'a' }, lifecycle: { remote: 'failed' } })])) });
    expect(withFailed.state.queries.get('a')!.lifecycle.remote).toBe('unregistered');
    expect(withFailed.dispatched).toEqual([{ type: 'EnsureRegistered', requireAuth: true }]);
    const withDesired = await runPure(selfHealTick(env), { state: degraded(buildState([buildEntry({ def: { hash: 'a' } })])) });
    expect(withDesired.dispatched).toEqual([{ type: 'EnsureRegistered', requireAuth: true }]);
    const probe = await runPure(selfHealTick(env), { state: degraded(buildState()), handlers: { 'remote.query': () => [{ status: 'OK', result: true }] } });
    expect(probe.dispatched).toEqual([{ type: 'SyncOutcome', ok: true }]);
    const probeFail = await runPure(selfHealTick(env), {
      state: degraded(buildState()),
      handlers: {
        'remote.query': () => {
          throw new Error('offline');
        },
      },
    });
    expect(probeFail.dispatched).toEqual([{ type: 'SyncOutcome', ok: false, error: new Error('offline') }]);
  });
});
