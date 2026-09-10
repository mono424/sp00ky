import { describe, expect, it } from 'vitest';
import * as R from './reducers';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import { emptyState } from './client-state';

const e = (hash: string, over: Parameters<typeof buildEntry>[0] = {}) => buildEntry({ ...over, def: { hash, ...over.def } });

describe('query reducers', () => {
  it('putQuery adds and dirties; removeQuery clears dirt', () => {
    let s = R.putQuery(e('a'))(emptyState({ tabId: 't' }));
    expect(s.queries.has('a')).toBe(true);
    expect(s.dirty.has('a')).toBe(true);
    s = R.markMembershipDirty(['a'])(s);
    s = R.removeQuery('a')(s);
    expect(s.queries.size).toBe(0);
    expect(s.dirty.size).toBe(0);
    expect(s.membershipDirty.size).toBe(0);
    expect(R.removeQuery('zzz')(s)).toBe(s);
  });

  it('reducers on an unknown hash are identity', () => {
    const s = buildState([e('a')]);
    expect(R.applyLifecycle('x', { type: 'notified' })(s)).toBe(s);
    expect(R.setLocalArray('x', [])(s)).toBe(s);
    expect(R.commitMembership('x', [['thing:1', 1]], true)(s)).toBe(s);
    expect(R.subscribe('x')(s)).toBe(s);
    expect(R.setRecords('x', [], true, 1)(s)).toBe(s);
  });

  it('applyLifecycle / setServerState', () => {
    const s0 = buildState([e('a')]);
    const s1 = R.applyLifecycle('a', { type: 'remote-registering' })(s0);
    expect(s1.queries.get('a')!.lifecycle.remote).toBe('registering');
    const s2 = R.setServerState('a', 'ready')(s1);
    expect(s2.queries.get('a')!.serverState).toBe('ready');
    expect(R.setServerState('a', 'ready')(s2)).toBe(s2);
  });

  it('commitMembership sets the array, flips live, dirties, and releases acked items it names', () => {
    const s0 = buildState(
      [e('a', { lifecycle: { phase: 'cached' } })],
      R.outboxReplace([
        buildOutboxItem({ id: 'm1', recordId: 'thing:1', status: 'acked', ackedAt: 1 }),
        buildOutboxItem({ id: 'm2', recordId: 'thing:2', status: 'acked', ackedAt: 1 }),
        buildOutboxItem({ id: 'm3', recordId: 'thing:1', status: 'pending' }),
      ])
    );
    const s1 = R.commitMembership('a', [['thing:1', 2]], true)({ ...s0, dirty: new Set() });
    const entry = s1.queries.get('a')!;
    expect(entry.remoteArray).toEqual([['thing:1', 2]]);
    expect(entry.lifecycle.phase).toBe('live');
    expect(s1.dirty.has('a')).toBe(true);
    expect(s1.outbox.map((i) => i.id)).toEqual(['m2', 'm3']);
  });

  it('setLocalArray dirties; setSubqueryRemoteArray does not', () => {
    const s0 = buildState([e('a')]);
    const s1 = R.setLocalArray('a', [['thing:1', 1]])(s0);
    expect(s1.dirty.has('a')).toBe(true);
    const s2 = R.setSubqueryRemoteArray('a', [['child:1', 1]])({ ...s1, dirty: new Set() });
    expect(s2.dirty.size).toBe(0);
    expect(s2.queries.get('a')!.subqueryRemoteArray).toEqual([['child:1', 1]]);
  });

  it('setRecords clears dirt, counts changes, samples timing, marks notified', () => {
    const s0 = R.markDirty(['a'])(buildState([e('a')]));
    const rows = [{ id: 'thing:1' }];
    const s1 = R.setRecords('a', rows, true, 12)(s0);
    const en = s1.queries.get('a')!;
    expect(en.records).toBe(rows);
    expect(en.telemetry.updateCount).toBe(1);
    expect(en.telemetry.phaseSamples.localFetch).toEqual([12]);
    expect(en.telemetry.phaseLast.localFetch).toBe(12);
    expect(en.lifecycle.notified).toBe(true);
    expect(s1.dirty.has('a')).toBe(false);
    const s2 = R.setRecords('a', [{ id: 'other' }], false, null)(s1);
    expect(s2.queries.get('a')!.records).toBe(rows);
    expect(s2.queries.get('a')!.telemetry.updateCount).toBe(1);
    expect(s2.queries.get('a')!.telemetry.phaseSamples.localFetch).toEqual([12]);
  });

  it('sample windows are capped', () => {
    let s = buildState([e('a')]);
    for (let i = 0; i < 105; i++) s = R.recordPhase('a', 'localFetch', i)(s);
    const t = s.queries.get('a')!.telemetry;
    expect(t.phaseSamples.localFetch).toHaveLength(100);
    expect(t.phaseLast.localFetch).toBe(104);
    for (let i = 0; i < 105; i++) s = R.setRecords('a', [], false, i)(s);
    expect(s.queries.get('a')!.telemetry.phaseSamples.localFetch).toHaveLength(100);
    for (let i = 0; i < 105; i++) s = R.recordIngest('a', i)(s);
    expect(s.queries.get('a')!.telemetry.materializationSamples).toHaveLength(100);
    expect(s.queries.get('a')!.telemetry.lastIngestLatencyMs).toBe(104);
  });

  it('telemetry helpers', () => {
    let s = buildState([e('a')]);
    s = R.stampUpdated('a', 5)(s);
    s = R.recordError('a')(s);
    s = R.setRegistrationTimings('a', { parseMs: 1, planMs: 2, snapshotMs: 3, wallMs: 4 })(s);
    s = R.bumpRegisterAttempts('a')(s);
    s = R.bumpRegisterAttempts('a')(s);
    s = R.stampHeartbeat(['a', 'missing'], 9)(s);
    s = R.stampPolled(['a'], 11)(s);
    const en = s.queries.get('a')!;
    expect(en.telemetry.lastUpdatedAt).toBe(5);
    expect(en.telemetry.errorCount).toBe(1);
    expect(en.telemetry.registrationTimings.wallMs).toBe(4);
    expect(en.registerAttempts).toBe(2);
    expect(en.lastHeartbeatAt).toBe(9);
    expect(en.lastPolledAt).toBe(11);
    const reset = R.resetRegisterAttempts('a')(s);
    expect(reset.queries.get('a')!.registerAttempts).toBe(0);
    expect(R.resetRegisterAttempts('a')(reset)).toBe(reset);
  });

  it('subscribe / unsubscribe track the eviction clock', () => {
    let s = buildState([e('a')]);
    s = R.subscribe('a')(s);
    s = R.subscribe('a')(s);
    expect(s.queries.get('a')!.subscribers).toBe(2);
    s = R.unsubscribe('a', 100)(s);
    expect(s.queries.get('a')!.lastSubscriberLeftAt).toBeNull();
    s = R.unsubscribe('a', 200)(s);
    expect(s.queries.get('a')!.lastSubscriberLeftAt).toBe(200);
    s = R.unsubscribe('a', 300)(s);
    expect(s.queries.get('a')!.subscribers).toBe(0);
    s = R.subscribe('a')(s);
    expect(s.queries.get('a')!.lastSubscriberLeftAt).toBeNull();
  });
});

describe('registering / reread bookkeeping', () => {
  it('tracks in-flight local registrations and reread attempts; removeQuery clears rereads', () => {
    let s = buildState([e('a')]);
    s = R.beginRegistering('x')(s);
    expect(s.registering.has('x')).toBe(true);
    s = R.endRegistering('x')(s);
    expect(s.registering.size).toBe(0);
    expect(R.endRegistering('x')(s)).toBe(s);
    s = R.setMembershipReread('a', 1)(s);
    expect(s.membershipReread.get('a')).toBe(1);
    expect(R.setMembershipReread('zzz', null)(s)).toBe(s);
    const cleared = R.removeQuery('a')(s);
    expect(cleared.membershipReread.size).toBe(0);
    s = R.setMembershipReread('a', null)(s);
    expect(s.membershipReread.size).toBe(0);
  });
});

describe('dirt reducers', () => {
  it('markDirty / clearDirty / markTableDirty', () => {
    const s0 = buildState([e('a', { def: { tableName: 't1' } }), e('b', { def: { tableName: 't2' } })]);
    expect(R.markDirty([])(s0)).toBe(s0);
    const s1 = R.markTableDirty('t1')(s0);
    expect([...s1.dirty]).toEqual(['a']);
    const s2 = R.clearDirty('a')(s1);
    expect(s2.dirty.size).toBe(0);
    expect(R.clearDirty('a')(s2)).toBe(s2);
  });
  it('membership dirt only for known hashes', () => {
    const s0 = buildState([e('a')]);
    expect(R.markMembershipDirty(['nope'])(s0)).toBe(s0);
    const s1 = R.markMembershipDirty(['a', 'nope'])(s0);
    expect([...s1.membershipDirty]).toEqual(['a']);
    expect(R.clearMembershipDirty(['a'])(s1).membershipDirty.size).toBe(0);
  });
});

describe('versions', () => {
  it('setVersions dirties queries naming the id, ignores unchanged, deleteVersions removes', () => {
    const s0 = buildState([
      e('a', { remoteArray: [['thing:1', 1]] }),
      e('b', { localArray: [['thing:2', 1]] }),
      e('c'),
    ]);
    expect(R.setVersions([])(s0)).toBe(s0);
    const s1 = R.setVersions([['thing:1', 1], ['thing:2', 1], ['thing:9', 1]])(s0);
    expect([...s1.dirty].sort()).toEqual(['a', 'b']);
    expect(R.setVersions([['thing:1', 1]])(s1)).toBe(s1);
    const s2 = R.deleteVersions(['thing:9', 'nope'])(s1);
    expect(s2.versions.has('thing:9')).toBe(false);
    expect(R.deleteVersions(['nope'])(s2)).toBe(s2);
  });
});

describe('outbox', () => {
  it('push / ack / bump / remove / replace dirty the table', () => {
    const s0 = buildState([e('a', { def: { tableName: 'thing' } })]);
    const s1 = R.outboxPush(buildOutboxItem({ id: 'm1' }))(s0);
    expect(s1.dirty.has('a')).toBe(true);
    const s2 = R.outboxAck('m1', 50)({ ...s1, dirty: new Set() });
    expect(s2.outbox[0]).toMatchObject({ status: 'acked', ackedAt: 50 });
    expect(s2.dirty.size).toBe(0);
    expect(R.outboxAck('nope', 1)(s2)).toBe(s2);
    const s3 = R.outboxBumpAttempts('m1')(s2);
    expect(s3.outbox[0].attempts).toBe(1);
    expect(R.outboxBumpAttempts('nope')(s3)).toBe(s3);
    const s4 = R.outboxRemove('m1')(s3);
    expect(s4.outbox).toEqual([]);
    expect(s4.dirty.has('a')).toBe(true);
    expect(R.outboxRemove('m1')(s4)).toBe(s4);
    const s5 = R.outboxReplace([buildOutboxItem({ id: 'x', table: 'thing' })])({ ...s4, dirty: new Set() });
    expect(s5.outbox).toHaveLength(1);
    expect(s5.dirty.has('a')).toBe(true);
  });
  it('outboxPruneAcked drops only expired acked items', () => {
    const s0 = buildState(
      [e('a', { def: { tableName: 'thing' } })],
      R.outboxReplace([
        buildOutboxItem({ id: 'old', status: 'acked', ackedAt: 0 }),
        buildOutboxItem({ id: 'fresh', status: 'acked', ackedAt: 90 }),
        buildOutboxItem({ id: 'pending' }),
      ])
    );
    const s1 = R.outboxPruneAcked(100, 50)({ ...s0, dirty: new Set() });
    expect(s1.outbox.map((i) => i.id)).toEqual(['fresh', 'pending']);
    expect(s1.dirty.has('a')).toBe(true);
    expect(R.outboxPruneAcked(100, 50)(s1)).toBe(s1);
  });
  it('pending writes merge per key and clear', () => {
    const w = { key: 'k', table: 'thing', recordId: 'thing:1', data: { a: 1 }, before: { a: 0 }, firstAt: 1 };
    let s = R.mergePendingWrite(w)(buildState());
    s = R.mergePendingWrite({ ...w, data: { b: 2 }, before: null, firstAt: 9 })(s);
    expect(s.pendingWrites.get('k')).toEqual({ ...w, data: { a: 1, b: 2 } });
    s = R.clearPendingWrite('k')(s);
    expect(s.pendingWrites.size).toBe(0);
    expect(R.clearPendingWrite('k')(s)).toBe(s);
  });
  it('setFailedCount', () => {
    const s0 = buildState();
    const s1 = R.setFailedCount(2)(s0);
    expect(s1.failedCount).toBe(2);
    expect(R.setFailedCount(2)(s1)).toBe(s1);
  });
});

describe('bucket switch reducers', () => {
  it('rebindQuery keeps the hash, swaps the id and sync state, dirties; clearBucketState wipes per-bucket slices', () => {
    const s0 = buildState(
      [e('a', { lifecycle: { phase: 'live', remote: 'registered', notified: true }, remoteArray: [['t:1', 1]], records: [{ id: 't:1' }], serverState: 'ready', registerAttempts: 2 })],
      R.setVersions([['t:1', 1]]),
      R.outboxReplace([buildOutboxItem()])
    );
    const id = { table: '_00_query', id: 'new' } as any;
    const s1 = R.rebindQuery('a', { id, lifecycle: { phase: 'cached', remote: 'unregistered', fetchDepth: 0, notified: false }, remoteArray: [['t:2', 1]], localArray: [] })({ ...s0, dirty: new Set() });
    const en = s1.queries.get('a')!;
    expect(en.def.id).toBe(id);
    expect(en.lifecycle.phase).toBe('cached');
    expect(en.remoteArray).toEqual([['t:2', 1]]);
    expect(en.records).toEqual([]);
    expect(en.serverState).toBeNull();
    expect(en.registerAttempts).toBe(0);
    expect(s1.dirty.has('a')).toBe(true);
    expect(R.rebindQuery('zz', { id, lifecycle: en.lifecycle, remoteArray: [], localArray: [] })(s1)).toBe(s1);
    const s2 = R.clearBucketState()(s1);
    expect(s2.versions.size).toBe(0);
    expect(s2.outbox).toEqual([]);
    expect(s2.dirty.size).toBe(0);
    expect(s2.primed).toBe(false);
  });
});

describe('identity / connection / compose', () => {
  it('sets fields and short-circuits on no change', () => {
    const s0 = buildState();
    const s1 = R.setIdentity({ sessionId: 's', userId: 'u', bucketId: 'b' })(s0);
    expect(s1).toMatchObject({ sessionId: 's', userId: 'u', bucketId: 'b' });
    const s2 = R.setTabRole('leader')(s1);
    expect(R.setTabRole('leader')(s2)).toBe(s2);
    const s3 = R.setConnection('connected')(s2);
    expect(s3.sync.health.connection).toBe('connected');
    expect(R.setConnection('connected')(s3)).toBe(s3);
    const s4 = R.setHealth({ ...s3.sync.health, status: 'degraded' })(s3);
    expect(s4.sync.health.status).toBe('degraded');
    const s5 = R.patchSync({ pollIdleStreak: 4 })(s4);
    expect(s5.sync.pollIdleStreak).toBe(4);
    const s6 = R.compose(R.setFailedCount(1), R.setFailedCount(3))(s5);
    expect(s6.failedCount).toBe(3);
  });
});
