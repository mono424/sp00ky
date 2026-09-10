import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { fakeServices } from '../testing/services';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { bucketSwitch, rebindQueries } from './bucket-switch.saga';

const env = defaultEnv({ tables: [] } as any);
const base = (over: Partial<ReturnType<typeof buildState>> = {}) => ({ ...buildState(), pendingBucket: 'u2', bucketId: 'u1', ...over });

describe('bucketSwitch', () => {
  it('steps aside when superseded or already on target (releasing the gate)', async () => {
    const released: number[] = [];
    const svc = fakeServices({ 'local.currentBucketId': () => 'u1' });
    await runPure(bucketSwitch(env, 'u2', () => void released.push(1)), { state: { ...base(), pendingBucket: 'u3' }, handlers: { service: svc.handler } });
    await runPure(bucketSwitch(env, 'u1', () => void released.push(2)), { state: base({ pendingBucket: 'u1' }), handlers: { service: svc.handler } });
    expect(released).toEqual([1, 2]);
    expect(svc.names()).toEqual(['local.currentBucketId', 'local.currentBucketId']);
  });
  it('solo: drain, swap, provision, blobs, SSP reset, prime, token, rebind, outbox, re-register', async () => {
    const svc = fakeServices({
      'local.currentBucketId': () => 'u1',
      'local.beginSwitch': () => () => undefined,
      'local.usesSurqlSchema': () => true,
      'auth.sessionAuthId': () => 'user:u2',
      'auth.access': () => 'account',
      'auth.token': () => 'jwt',
    });
    const s = base();
    const withQ = R.compose(
      R.putQuery(buildEntry({ def: { hash: 'a', viewKey: 'vk', surql: 'SELECT * FROM thing' }, lifecycle: { phase: 'live', remote: 'registered' }, remoteArray: [['t:1', 1]] })),
      R.outboxReplace([buildOutboxItem()]),
      R.setVersions([['t:1', 1]]),
      R.markMembershipDirty(['a'])
    )({ ...s, sessionId: 'sess2' });
    const out = await runPure(bucketSwitch(env, 'u2'), {
      state: withQ,
      handlers: {
        service: svc.handler,
        'local.getById': () => ({ ids: [['t:9', 1]], confirmed: true }),
        'ssp.register': () => ({ localArray: [['t:9', 1]], timings: {} }),
        'local.query': () => [],
      },
    });
    expect(svc.names()).toEqual([
      'local.currentBucketId',
      'local.beginSwitch',
      'crdt.closeAll',
      'local.switchStore',
      'local.usesSurqlSchema',
      'migrator.provision',
      'blobs.setNamespace',
      'ssp.reset',
      'ssp.setPermissions',
      'auth.sessionAuthId',
      'auth.access',
      'ssp.setSessionAuth',
      'auth.token',
      'persistence.set',
    ]);
    expect(out.log.filter((e) => e.kind === 'timer.clear').map((e) => (e as any).key)).toEqual(['poll', 'outbox', 'membership', 'fetch', 'ack-prune']);
    expect(out.state).toMatchObject({ bucketId: 'u2', primed: false, outbox: [] });
    expect(out.state.versions.size).toBe(0);
    expect(out.state.membershipDirty.size).toBe(0);
    const e = out.state.queries.get('a')!;
    expect(e.lifecycle).toMatchObject({ phase: 'cached', remote: 'unregistered' });
    expect(e.remoteArray).toEqual([['t:9', 1]]);
    expect(e.localArray).toEqual([['t:9', 1]]);
    expect(String(e.def.id.table)).toBe('_00_query');
    // PollTick is load-bearing: the switch cleared the poll timer, and only the
    // tick re-arms it, so a sign-in without it leaves the client on LIVE alone.
    expect(out.dispatched.map((d) => d.type)).toEqual(['PrimeCircuit', 'EnsureRegistered', 'LiveStart', 'PollTick', 'Drain']);
  });
  it('shared tabs: moves namespaces, falls back to solo on failure; blob clear on sign-out; failures are logged and the gate always reopens', async () => {
    let gateReleased = false;
    const moved = fakeServices({
      'local.currentBucketId': () => 'u1',
      'tabs.moveToBucket': async () => 'follower',
      'auth.sessionAuthId': () => null,
      'auth.access': () => null,
      'auth.token': () => null,
    });
    const out = await runPure(bucketSwitch(env, 'u2', () => void (gateReleased = true)), {
      state: { ...base(), tabRole: 'leader' },
      handlers: { service: moved.handler, 'local.query': () => [] },
    });
    expect(out.state.tabRole).toBe('follower');
    expect(gateReleased).toBe(true);
    expect(moved.names()).not.toContain('local.switchStore');
    const failing = fakeServices({
      'local.currentBucketId': () => 'u1',
      'local.beginSwitch': () => () => undefined,
      'tabs.moveToBucket': async () => {
        throw new Error('broker');
      },
      'auth.sessionAuthId': () => null,
      'auth.access': () => null,
      'auth.token': () => 'jwt',
      'persistence.set': async () => {
        throw new Error('kv');
      },
      'blobs.setNamespace': async () => {
        throw new Error('opfs');
      },
    });
    const fb = await runPure(bucketSwitch(env, 'anon', null, { clearBlobsOnSignOut: true }), {
      state: { ...base({ pendingBucket: 'anon' }), tabRole: 'leader' },
      handlers: { service: failing.handler, 'local.query': () => [] },
    });
    expect(fb.state.tabRole).toBe('solo');
    expect(failing.names()).toContain('local.beginSwitch');
    expect(failing.names()).toContain('ssp.setPersistence');
    expect(failing.names()).toContain('blobs.clear');
    expect(fb.emitted.filter((e) => e.type === 'log')).toHaveLength(3);
  });
  it('a failing swap still reopens the gate and rethrows', async () => {
    let released = false;
    const svc = fakeServices({
      'local.currentBucketId': () => 'u1',
      'local.switchStore': async () => {
        throw new Error('disk');
      },
    });
    await expect(runPure(bucketSwitch(env, 'u2', () => void (released = true)), { state: base(), handlers: { service: svc.handler } })).rejects.toThrow('disk');
    expect(released).toBe(true);
  });
});

describe('rebindQueries', () => {
  it('re-seeds from the new store, rebuilds the local view (tolerating failures), emits authority flips', async () => {
    const s = R.compose(
      R.putQuery(buildEntry({ def: { hash: 'a' }, lifecycle: { phase: 'live' } })),
      R.putQuery(buildEntry({ def: { hash: 'b' }, lifecycle: { phase: 'cold' } }))
    )({ ...buildState(), sessionId: 's' });
    const out = await runPure(rebindQueries(), {
      state: s,
      handlers: {
        'local.getById': (e: any) => {
          if (String((e.id as RecordId).id) === 'view-b') return { ids: [['t:1', 1]], confirmed: true };
          throw new Error('missing');
        },
        'ssp.register': (e: any) => {
          if (e.plan.queryHash === 'a') throw new Error('wasm');
          return { localArray: [['t:1', 1]], timings: {} };
        },
      },
    });
    expect(out.state.queries.get('a')!.lifecycle.phase).toBe('cold');
    expect(out.state.queries.get('b')!.lifecycle.phase).toBe('cached');
    expect(out.emitted.filter((e) => e.type === 'query:authority')).toEqual([
      { type: 'query:authority', hash: 'a', known: false },
      { type: 'query:authority', hash: 'b', known: true },
    ]);
    expect(out.emitted.some((e) => e.type === 'log')).toBe(true);
  });
});
