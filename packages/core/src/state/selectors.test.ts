import { describe, expect, it } from 'vitest';
import * as S from './selectors';
import * as R from './reducers';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';

const e = (hash: string, over: Parameters<typeof buildEntry>[0] = {}) => buildEntry({ ...over, def: { hash, ...over.def } });

describe('basic lookups', () => {
  it('queryByHash / activeHashes / hashesForTable / queryStatus', () => {
    const s = buildState([e('a', { def: { tableName: 't1' } }), e('b', { def: { tableName: 't2' } })]);
    expect(S.queryByHash(s, 'a')?.def.hash).toBe('a');
    expect(S.activeHashes(s)).toEqual(['a', 'b']);
    expect(S.hashesForTable(s, 't2')).toEqual(['b']);
    expect(S.queryStatus(s, 'a')).toBe('idle');
    expect(S.queryStatus(s, 'zz')).toBeUndefined();
  });
});

describe('overlay and outbox counts', () => {
  it('derives writes/deletes from pending and acked items', () => {
    const s = buildState(
      [],
      R.outboxReplace([
        buildOutboxItem({ id: '1', type: 'create', recordId: 'thing:1' }),
        buildOutboxItem({ id: '2', type: 'update', recordId: 'thing:2', status: 'acked', ackedAt: 1 }),
        buildOutboxItem({ id: '3', type: 'delete', recordId: 'thing:3' }),
      ])
    );
    const o = S.overlay(s);
    expect([...o.writes].sort()).toEqual(['thing:1', 'thing:2']);
    expect([...o.deletes]).toEqual(['thing:3']);
    expect([...S.pendingDeleteIds(s)]).toEqual(['thing:3']);
    expect(S.hasAckedWrites(s)).toBe(true);
    expect(S.pendingMutationCount(s)).toBe(2);
  });
});

describe('needed / planFetch / settled', () => {
  const live = { phase: 'live' } as const;
  it('needed compares membership versions against local versions and skips pending deletes', () => {
    const s = buildState(
      [e('a', { lifecycle: live, remoteArray: [['thing:1', 2], ['thing:2', 1], ['thing:3', 1]] })],
      R.setVersions([['thing:1', 2], ['thing:2', 0]]),
      R.outboxReplace([buildOutboxItem({ type: 'delete', recordId: 'thing:3' })])
    );
    expect(S.needed(s, 'a')).toEqual([['thing:2', 1]]);
    expect(S.needed(s, 'missing')).toEqual([]);
    const cached = buildState([e('c', { lifecycle: { phase: 'cached' }, remoteArray: [['thing:9', 1]] })]);
    expect(S.needed(cached, 'c')).toEqual([]);
  });
  it('planFetch dedupes across queries (highest version wins), includes subquery children, chunks', () => {
    const s = buildState([
      e('a', { lifecycle: live, remoteArray: [['thing:1', 1], ['thing:2', 3]] }),
      e('b', { lifecycle: live, remoteArray: [['thing:2', 1], ['thing:3', 1]], subqueryRemoteArray: [['child:1', 2]] }),
      e('c', { lifecycle: live, subqueryRemoteArray: [['child:1', 1], ['child:2', 1]] }),
      e('d', { lifecycle: { phase: 'cached' }, remoteArray: [['thing:9', 1]] }),
    ]);
    const plan = S.planFetch(s, 2);
    expect(plan.hashes).toEqual(['a', 'b']);
    expect(plan.chunks).toEqual([['thing:1', 'thing:2'], ['thing:3', 'child:1'], ['child:2']]);
    expect(plan.versions.get('thing:2')).toBe(3);
    expect(plan.versions.get('child:1')).toBe(2);
    expect(S.planFetch(buildState()).chunks).toEqual([]);
    expect(S.neededChildren(s, 'missing')).toEqual([]);
    expect(S.neededChildren(R.setVersions([['child:1', 2]])(s), 'b')).toEqual([]);
  });
  it('settled requires live, complete, clean, notified', () => {
    const base = e('a', { lifecycle: { ...live, notified: true }, remoteArray: [['thing:1', 1]] });
    const s = { ...buildState([base], R.setVersions([['thing:1', 1]])), dirty: new Set<string>() };
    expect(S.settled(s, 'a')).toBe(true);
    expect(S.settled(R.markDirty(['a'])(s), 'a')).toBe(false);
    expect(S.settled(R.setVersions([['thing:1', 0]])(s), 'a')).toBe(false);
    expect(S.settled(buildState([e('a', { lifecycle: { phase: 'cached', notified: true } })]), 'a')).toBe(false);
    expect(S.settled(buildState([e('a', { lifecycle: live })]), 'a')).toBe(false);
    expect(S.settled(s, 'missing')).toBe(false);
    expect(S.settleFailed(s, 'missing')).toBe(true);
    expect(S.settleFailed(s, 'a')).toBe(false);
    expect(S.settleFailed(R.applyLifecycle('a', { type: 'remote-failed' })(s), 'a')).toBe(true);
  });
  it('fetchingQueryCount counts entries with fetch depth', () => {
    const s = buildState([e('a', { lifecycle: { fetchDepth: 2 } }), e('b')]);
    expect(S.fetchingQueryCount(s)).toBe(1);
  });
});

describe('registration / eviction / ttl', () => {
  it('desiredRegistrations lists unregistered queries', () => {
    const s = buildState([e('a'), e('b', { lifecycle: { remote: 'registered' } }), e('c', { lifecycle: { remote: 'registering' } })]);
    expect(S.desiredRegistrations(s)).toEqual(['a']);
  });
  it('evictable respects subscribers and ttl', () => {
    const s = buildState([
      e('gone', { lastSubscriberLeftAt: 0, def: { ttlMs: 100 } }),
      e('fresh', { lastSubscriberLeftAt: 950, def: { ttlMs: 100 } }),
      e('watched', { subscribers: 1, lastSubscriberLeftAt: 0, def: { ttlMs: 100 } }),
      e('never', { def: { ttlMs: 100 } }),
    ]);
    expect(S.evictable(s, 1000)).toEqual(['gone']);
  });
  it('shortestTtlMs', () => {
    expect(S.shortestTtlMs(buildState())).toBeNull();
    expect(S.shortestTtlMs(buildState([e('a', { def: { ttlMs: 500 } }), e('b', { def: { ttlMs: 100 } })]))).toBe(100);
  });
});

describe('toQueryState', () => {
  it('projects the legacy shape with derived flags', () => {
    const entry = e('a', {
      lifecycle: { phase: 'view-lost', fetchDepth: 1, notified: true },
      remoteArray: [['thing:1', 1]],
      subqueryRemoteArray: [['child:1', 1]],
      records: [{ id: 'thing:1' }],
      serverState: 'ready',
      lastHeartbeatAt: 123,
    });
    const qs = S.toQueryState(entry);
    expect(qs.config.membershipKnown).toBe(true);
    expect(qs.config.membershipKey).toBe('view-a');
    expect(qs.viewLost).toBe(true);
    expect(qs.serverMembership).toBe(true);
    expect(qs.status).toBe('fetching');
    expect(qs.syncNotified).toBe(true);
    expect(qs.config.subqueryRemoteArray).toEqual([['child:1', 1]]);
    expect(qs.config.lastActiveAt.getTime()).toBe(123);
    expect(qs.records).toEqual([{ id: 'thing:1' }]);
    const cold = S.toQueryState(e('b'));
    expect(cold.config.membershipKnown).toBe(false);
    expect(cold.config.subqueryRemoteArray).toBeUndefined();
    expect(cold.lastHeartbeatAt).toBeUndefined();
    expect(cold.config.lastActiveAt.getTime()).toBe(1_700_000_000_000);
  });
});

describe('phaseTimings', () => {
  it('summarizes every phase, with nulls before the first sample', () => {
    const cold = S.phaseTimings(e('a'));
    expect(cold.ssp).toEqual({ lastMs: null, p50: null, p90: null, p99: null, count: 0 });
    expect(cold.localFetch.count).toBe(0);
    let s = buildState([e('a')]);
    for (const ms of [5, 1, 9, 3]) s = R.recordIngest('a', ms)(s);
    s = R.recordPhase('a', 'remoteFetch', 7)(s);
    s = R.recordError('a')(s);
    const t = S.phaseTimings(s.queries.get('a')!);
    expect(t.ssp).toEqual({ lastMs: 3, p50: 5, p90: 9, p99: 9, count: 4 });
    expect(t.remoteFetch).toEqual({ lastMs: 7, p50: 7, p90: 7, p99: 7, count: 1 });
    expect(t.errorCount).toBe(1);
    expect(t.registration.parseMs).toBeNull();
  });
});
