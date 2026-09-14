import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/state/selectors.dart' as sel;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

QueryLifecycle life({
  QueryPhase phase = QueryPhase.cold,
  RemotePhase remote = RemotePhase.unregistered,
  int fetchDepth = 0,
  bool notified = false,
}) =>
    QueryLifecycle(
        phase: phase,
        remote: remote,
        fetchDepth: fetchDepth,
        notified: notified);

QueryEntry e(
  String hash, {
  QueryLifecycle? lifecycle,
  RecordVersionArray remoteArray = const [],
  RecordVersionArray subqueryRemoteArray = const [],
  int subscribers = 0,
  int? lastSubscriberLeftAt,
  String tableName = 'thing',
  int ttlMs = 600000,
}) =>
    buildEntry(
      def: buildDefinition(hash: hash, tableName: tableName, ttlMs: ttlMs),
      lifecycle: lifecycle,
      remoteArray: remoteArray,
      subqueryRemoteArray: subqueryRemoteArray,
      subscribers: subscribers,
      lastSubscriberLeftAt: lastSubscriberLeftAt,
    );

void main() {
  group('basic lookups', () {
    test('queryByHash / activeHashes / hashesForTable / queryStatus', () {
      final s = buildState([e('a', tableName: 't1'), e('b', tableName: 't2')]);
      expect(sel.queryByHash(s, 'a')?.def.hash, 'a');
      expect(sel.activeHashes(s), ['a', 'b']);
      expect(sel.hashesForTable(s, 't2'), ['b']);
      expect(sel.queryStatus(s, 'a'), QueryStatus.idle);
      expect(sel.queryStatus(s, 'zz'), isNull);
    });
  });

  group('overlay and outbox counts', () {
    test('unsynced ids: pending outbox items and debounced patches, not acked',
        () {
      final s = buildState(const [], [
        r.outboxReplace([
          buildOutboxItem(id: '1', recordId: 'thing:1'),
          buildOutboxItem(
              id: '2',
              type: MutationEventType.update,
              recordId: 'thing:2',
              status: OutboxStatus.acked,
              ackedAt: 1),
        ]),
        r.mergePendingWrite(const PendingWrite(
          key: 'thing:3',
          table: 'thing',
          recordId: 'thing:3',
          data: {'n': 1},
          before: null,
          firstAt: 0,
        )),
      ]);
      expect(sel.unsyncedRecordIds(s).toList()..sort(), ['thing:1', 'thing:3']);
    });

    test('derives writes/deletes from pending and acked items', () {
      final s = buildState(const [], [
        r.outboxReplace([
          buildOutboxItem(
              id: '1', type: MutationEventType.create, recordId: 'thing:1'),
          buildOutboxItem(
              id: '2',
              type: MutationEventType.update,
              recordId: 'thing:2',
              status: OutboxStatus.acked,
              ackedAt: 1),
          buildOutboxItem(
              id: '3', type: MutationEventType.delete, recordId: 'thing:3'),
        ]),
      ]);
      final o = sel.overlay(s);
      expect(o.writes.toList()..sort(), ['thing:1', 'thing:2']);
      expect(o.deletes.toList(), ['thing:3']);
      expect(sel.pendingDeleteIds(s).toList(), ['thing:3']);
      expect(sel.hasAckedWrites(s), isTrue);
      expect(sel.pendingMutationCount(s), 2);
    });
  });

  group('needed / planFetch / settled', () {
    final live = life(phase: QueryPhase.live);

    test('needed compares versions and skips pending deletes', () {
      final s = buildState([
        e('a', lifecycle: live, remoteArray: [
          ('thing:1', 2),
          ('thing:2', 1),
          ('thing:3', 1),
        ]),
      ], [
        r.setVersions([('thing:1', 2), ('thing:2', 0)]),
        r.outboxReplace([
          buildOutboxItem(type: MutationEventType.delete, recordId: 'thing:3'),
        ]),
      ]);
      expect(sel.needed(s, 'a'), [('thing:2', 1)]);
      expect(sel.needed(s, 'missing'), isEmpty);
      final cached = buildState([
        e('c',
            lifecycle: life(phase: QueryPhase.cached),
            remoteArray: [('thing:9', 1)]),
      ]);
      expect(sel.needed(cached, 'c'), isEmpty);
    });

    test('planFetch dedupes, includes subquery children, chunks', () {
      final s = buildState([
        e('a', lifecycle: live, remoteArray: [('thing:1', 1), ('thing:2', 3)]),
        e('b',
            lifecycle: live,
            remoteArray: [('thing:2', 1), ('thing:3', 1)],
            subqueryRemoteArray: [('child:1', 2)]),
        e('c',
            lifecycle: live,
            subqueryRemoteArray: [('child:1', 1), ('child:2', 1)]),
        e('d',
            lifecycle: life(phase: QueryPhase.cached),
            remoteArray: [('thing:9', 1)]),
      ]);
      final plan = sel.planFetch(s, chunkSize: 2);
      expect(plan.hashes, ['a', 'b']);
      expect(plan.chunks, [
        ['thing:1', 'thing:2'],
        ['thing:3', 'child:1'],
        ['child:2'],
      ]);
      expect(plan.versions['thing:2'], 3);
      expect(plan.versions['child:1'], 2);
      expect(sel.planFetch(buildState()).chunks, isEmpty);
      expect(sel.neededChildren(s, 'missing'), isEmpty);
      expect(
          sel.neededChildren(r.setVersions([('child:1', 2)])(s), 'b'), isEmpty);
    });

    test('settled requires live, complete, clean, notified', () {
      final base = e('a',
          lifecycle: life(phase: QueryPhase.live, notified: true),
          remoteArray: [('thing:1', 1)]);
      final s = buildState([
        base
      ], [
        r.setVersions([('thing:1', 1)]),
      ]).copyWith(dirty: const {});
      expect(sel.settled(s, 'a'), isTrue);
      expect(sel.settled(r.markDirty(['a'])(s), 'a'), isFalse);
      expect(sel.settled(r.setVersions([('thing:1', 0)])(s), 'a'), isFalse);
      expect(
          sel.settled(
              buildState([
                e('a',
                    lifecycle: life(phase: QueryPhase.cached, notified: true))
              ]),
              'a'),
          isFalse);
      expect(sel.settled(buildState([e('a', lifecycle: live)]), 'a'), isFalse);
      expect(sel.settled(s, 'missing'), isFalse);
      expect(sel.settleFailed(s, 'missing'), isTrue);
      expect(sel.settleFailed(s, 'a'), isFalse);
      expect(
          sel.settleFailed(
              r.applyLifecycle('a', const RemoteFailedEvent())(s), 'a'),
          isTrue);
    });

    test('fetchingQueryCount counts entries with fetch depth', () {
      final s = buildState([e('a', lifecycle: life(fetchDepth: 2)), e('b')]);
      expect(sel.fetchingQueryCount(s), 1);
    });
  });

  group('registration / eviction / ttl', () {
    test('desiredRegistrations lists unregistered queries', () {
      final s = buildState([
        e('a'),
        e('b', lifecycle: life(remote: RemotePhase.registered)),
        e('c', lifecycle: life(remote: RemotePhase.registering)),
      ]);
      expect(sel.desiredRegistrations(s), ['a']);
    });

    test('evictable respects subscribers and ttl', () {
      final s = buildState([
        e('gone', lastSubscriberLeftAt: 0, ttlMs: 100),
        e('fresh', lastSubscriberLeftAt: 950, ttlMs: 100),
        e('watched', subscribers: 1, lastSubscriberLeftAt: 0, ttlMs: 100),
        e('never', ttlMs: 100),
      ]);
      expect(sel.evictable(s, 1000), ['gone']);
    });

    test('shortestTtlMs', () {
      expect(sel.shortestTtlMs(buildState()), isNull);
      expect(
          sel.shortestTtlMs(
              buildState([e('a', ttlMs: 500), e('b', ttlMs: 100)])),
          100);
    });
  });

  group('phaseTimings', () {
    test('summarizes every phase, with nulls before the first sample', () {
      final cold = sel.phaseTimings(e('a'));
      expect(cold.ssp.lastMs, isNull);
      expect(cold.ssp.p50, isNull);
      expect(cold.ssp.count, 0);
      expect(cold.localFetch.count, 0);

      var s = buildState([e('a')]);
      for (final ms in [5.0, 1.0, 9.0, 3.0]) {
        s = r.recordIngest('a', ms)(s);
      }
      s = r.recordPhase('a', TimingPhase.remoteFetch, 7)(s);
      s = r.recordError('a')(s);
      final t = sel.phaseTimings(s.queries['a']!);
      expect(t.ssp.lastMs, 3.0);
      expect(t.ssp.p50, 5.0);
      expect(t.ssp.p90, 9.0);
      expect(t.ssp.p99, 9.0);
      expect(t.ssp.count, 4);
      expect(t.remoteFetch.lastMs, 7.0);
      expect(t.remoteFetch.p50, 7.0);
      expect(t.errorCount, 1);
      expect(t.registration.parseMs, isNull);
    });
  });
}
