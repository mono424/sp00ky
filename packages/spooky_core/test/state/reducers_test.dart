import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/surreal/value.dart';
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

QueryEntry e(
  String hash, {
  QueryLifecycle? lifecycle,
  RecordVersionArray remoteArray = const [],
  RecordVersionArray localArray = const [],
  List<Row> records = const [],
  ServerViewState? serverState,
  int registerAttempts = 0,
  String tableName = 'thing',
}) =>
    buildEntry(
      def: buildDefinition(hash: hash, tableName: tableName),
      lifecycle: lifecycle,
      remoteArray: remoteArray,
      localArray: localArray,
      records: records,
      serverState: serverState,
      registerAttempts: registerAttempts,
    );

void main() {
  group('query reducers', () {
    test('putQuery adds and dirties; removeQuery clears dirt', () {
      var s = r.putQuery(e('a'))(emptyState(tabId: 't'));
      expect(s.queries.containsKey('a'), isTrue);
      expect(s.dirty.contains('a'), isTrue);
      s = r.markMembershipDirty(['a'])(s);
      s = r.removeQuery('a')(s);
      expect(s.queries, isEmpty);
      expect(s.dirty, isEmpty);
      expect(s.membershipDirty, isEmpty);
      expect(identical(r.removeQuery('zzz')(s), s), isTrue);
    });

    test('reducers on an unknown hash are identity', () {
      final s = buildState([e('a')]);
      expect(identical(r.applyLifecycle('x', const NotifiedEvent())(s), s),
          isTrue);
      expect(identical(r.setLocalArray('x', const [])(s), s), isTrue);
      expect(identical(r.commitMembership('x', [('thing:1', 1)], true)(s), s),
          isTrue);
      expect(identical(r.subscribeQuery('x')(s), s), isTrue);
      expect(identical(r.setRecords('x', const [], true, 1)(s), s), isTrue);
    });

    test('applyLifecycle / setServerState', () {
      final s0 = buildState([e('a')]);
      final s1 = r.applyLifecycle('a', const RemoteRegisteringEvent())(s0);
      expect(s1.queries['a']!.lifecycle.remote, RemotePhase.registering);
      final s2 = r.setServerState('a', ServerViewState.ready)(s1);
      expect(s2.queries['a']!.serverState, ServerViewState.ready);
      expect(identical(r.setServerState('a', ServerViewState.ready)(s2), s2),
          isTrue);
    });

    test(
        'commitMembership sets the array, flips live, dirties, and releases acked items it names',
        () {
      final s0 = buildState([
        e('a', lifecycle: seedLifecycle(true)),
      ], [
        r.outboxReplace([
          buildOutboxItem(
              id: 'm1',
              recordId: 'thing:1',
              status: OutboxStatus.acked,
              ackedAt: 1),
          buildOutboxItem(
              id: 'm2',
              recordId: 'thing:2',
              status: OutboxStatus.acked,
              ackedAt: 1),
          buildOutboxItem(id: 'm3', recordId: 'thing:1'),
        ]),
      ]);
      final s1 = r.commitMembership(
          'a', [('thing:1', 2)], true)(s0.copyWith(dirty: const {}));
      final entry = s1.queries['a']!;
      expect(entry.remoteArray, [('thing:1', 2)]);
      expect(entry.lifecycle.phase, QueryPhase.live);
      expect(s1.dirty.contains('a'), isTrue);
      expect(s1.outbox.map((i) => i.id), ['m2', 'm3']);
    });

    test('setLocalArray dirties; setSubqueryRemoteArray does not', () {
      final s0 = buildState([e('a')]);
      final s1 = r.setLocalArray('a', [('thing:1', 1)])(s0);
      expect(s1.dirty.contains('a'), isTrue);
      final s2 = r.setSubqueryRemoteArray('a', [('child:1', 1)])(
          s1.copyWith(dirty: const {}));
      expect(s2.dirty, isEmpty);
      expect(s2.queries['a']!.subqueryRemoteArray, [('child:1', 1)]);
    });

    test(
        'setRecords clears dirt, counts changes, samples timing, marks notified',
        () {
      final s0 = r.markDirty(['a'])(buildState([e('a')]));
      final rows = <Row>[
        {'id': 'thing:1'}
      ];
      final s1 = r.setRecords('a', rows, true, 12)(s0);
      final en = s1.queries['a']!;
      expect(identical(en.records, rows), isTrue);
      expect(en.telemetry.updateCount, 1);
      expect(en.telemetry.phaseSamples[TimingPhase.localFetch], [12.0]);
      expect(en.telemetry.phaseLast[TimingPhase.localFetch], 12.0);
      expect(en.lifecycle.notified, isTrue);
      expect(s1.dirty.contains('a'), isFalse);
      final s2 = r.setRecords(
          'a',
          [
            {'id': 'other'}
          ],
          false,
          null)(s1);
      expect(identical(s2.queries['a']!.records, rows), isTrue);
      expect(s2.queries['a']!.telemetry.updateCount, 1);
      expect(s2.queries['a']!.telemetry.phaseSamples[TimingPhase.localFetch],
          [12.0]);
    });

    test('sample windows are capped', () {
      var s = buildState([e('a')]);
      for (var i = 0; i < 105; i++) {
        s = r.recordPhase('a', TimingPhase.localFetch, i.toDouble())(s);
      }
      final t = s.queries['a']!.telemetry;
      expect(t.phaseSamples[TimingPhase.localFetch], hasLength(100));
      expect(t.phaseLast[TimingPhase.localFetch], 104.0);
      for (var i = 0; i < 105; i++) {
        s = r.setRecords('a', const [], false, i.toDouble())(s);
      }
      expect(s.queries['a']!.telemetry.phaseSamples[TimingPhase.localFetch],
          hasLength(100));
      for (var i = 0; i < 105; i++) {
        s = r.recordIngest('a', i.toDouble())(s);
      }
      expect(s.queries['a']!.telemetry.materializationSamples, hasLength(100));
      expect(s.queries['a']!.telemetry.lastIngestLatencyMs, 104.0);
    });

    test('telemetry helpers', () {
      var s = buildState([e('a')]);
      s = r.stampUpdated('a', 5)(s);
      s = r.recordError('a')(s);
      s = r.setRegistrationTimings(
          'a',
          const RegistrationTimings(
              parseMs: 1, planMs: 2, snapshotMs: 3, wallMs: 4))(s);
      s = r.bumpRegisterAttempts('a')(s);
      s = r.bumpRegisterAttempts('a')(s);
      s = r.stampHeartbeat(['a', 'missing'], 9)(s);
      s = r.stampPolled(['a'], 11)(s);
      final en = s.queries['a']!;
      expect(en.telemetry.lastUpdatedAt, 5);
      expect(en.telemetry.errorCount, 1);
      expect(en.telemetry.registrationTimings.wallMs, 4);
      expect(en.registerAttempts, 2);
      expect(en.lastHeartbeatAt, 9);
      expect(en.lastPolledAt, 11);
      final reset = r.resetRegisterAttempts('a')(s);
      expect(reset.queries['a']!.registerAttempts, 0);
      expect(identical(r.resetRegisterAttempts('a')(reset), reset), isTrue);
    });

    test('subscribe / unsubscribe track the eviction clock', () {
      var s = buildState([e('a')]);
      s = r.subscribeQuery('a')(s);
      s = r.subscribeQuery('a')(s);
      expect(s.queries['a']!.subscribers, 2);
      s = r.unsubscribeQuery('a', 100)(s);
      expect(s.queries['a']!.lastSubscriberLeftAt, isNull);
      s = r.unsubscribeQuery('a', 200)(s);
      expect(s.queries['a']!.lastSubscriberLeftAt, 200);
      s = r.unsubscribeQuery('a', 300)(s);
      expect(s.queries['a']!.subscribers, 0);
      s = r.subscribeQuery('a')(s);
      expect(s.queries['a']!.lastSubscriberLeftAt, isNull);
    });
  });

  group('registering / reread bookkeeping', () {
    test('tracks in-flight registrations and reread attempts', () {
      var s = buildState([e('a')]);
      s = r.beginRegistering('x')(s);
      expect(s.registering.contains('x'), isTrue);
      s = r.endRegistering('x')(s);
      expect(s.registering, isEmpty);
      expect(identical(r.endRegistering('x')(s), s), isTrue);
      s = r.setMembershipReread('a', 1)(s);
      expect(s.membershipReread['a'], 1);
      expect(identical(r.setMembershipReread('zzz', null)(s), s), isTrue);
      expect(r.removeQuery('a')(s).membershipReread, isEmpty);
      s = r.setMembershipReread('a', null)(s);
      expect(s.membershipReread, isEmpty);
    });
  });

  group('dirt reducers', () {
    test('markDirty / clearDirty / markTableDirty', () {
      final s0 = buildState([
        e('a', tableName: 't1'),
        e('b', tableName: 't2'),
      ]);
      expect(identical(r.markDirty(const [])(s0), s0), isTrue);
      final s1 = r.markTableDirty('t1')(s0);
      expect(s1.dirty.toList(), ['a']);
      final s2 = r.clearDirty('a')(s1);
      expect(s2.dirty, isEmpty);
      expect(identical(r.clearDirty('a')(s2), s2), isTrue);
    });

    test('membership dirt only for known hashes', () {
      final s0 = buildState([e('a')]);
      expect(identical(r.markMembershipDirty(['nope'])(s0), s0), isTrue);
      final s1 = r.markMembershipDirty(['a', 'nope'])(s0);
      expect(s1.membershipDirty.toList(), ['a']);
      expect(r.clearMembershipDirty(['a'])(s1).membershipDirty, isEmpty);
    });
  });

  group('versions', () {
    test('setVersions dirties queries naming the id; deleteVersions removes',
        () {
      final s0 = buildState([
        e('a', remoteArray: [('thing:1', 1)]),
        e('b', localArray: [('thing:2', 1)]),
        e('c'),
      ]);
      expect(identical(r.setVersions(const [])(s0), s0), isTrue);
      final s1 =
          r.setVersions([('thing:1', 1), ('thing:2', 1), ('thing:9', 1)])(s0);
      expect(s1.dirty.toList()..sort(), ['a', 'b']);
      expect(identical(r.setVersions([('thing:1', 1)])(s1), s1), isTrue);
      final s2 = r.deleteVersions(['thing:9', 'nope'])(s1);
      expect(s2.versions.containsKey('thing:9'), isFalse);
      expect(identical(r.deleteVersions(['nope'])(s2), s2), isTrue);
    });
  });

  group('outbox', () {
    test('push / ack / bump / remove / replace dirty the table', () {
      final s0 = buildState([e('a')]);
      final s1 = r.outboxPush(buildOutboxItem(id: 'm1'))(s0);
      expect(s1.dirty.contains('a'), isTrue);
      final s2 = r.outboxAck('m1', 50)(s1.copyWith(dirty: const {}));
      expect(s2.outbox[0].status, OutboxStatus.acked);
      expect(s2.outbox[0].ackedAt, 50);
      expect(s2.dirty, isEmpty);
      expect(identical(r.outboxAck('nope', 1)(s2), s2), isTrue);
      final s3 = r.outboxBumpAttempts('m1')(s2);
      expect(s3.outbox[0].attempts, 1);
      expect(identical(r.outboxBumpAttempts('nope')(s3), s3), isTrue);
      final s4 = r.outboxRemove('m1')(s3);
      expect(s4.outbox, isEmpty);
      expect(s4.dirty.contains('a'), isTrue);
      expect(identical(r.outboxRemove('m1')(s4), s4), isTrue);
      final s5 = r.outboxReplace([buildOutboxItem(id: 'x')])(
          s4.copyWith(dirty: const {}));
      expect(s5.outbox, hasLength(1));
      expect(s5.dirty.contains('a'), isTrue);
    });

    test('outboxPruneAcked drops only expired acked items', () {
      final s0 = buildState([
        e('a')
      ], [
        r.outboxReplace([
          buildOutboxItem(id: 'old', status: OutboxStatus.acked, ackedAt: 0),
          buildOutboxItem(id: 'fresh', status: OutboxStatus.acked, ackedAt: 90),
          buildOutboxItem(id: 'pending'),
        ]),
      ]);
      final s1 = r.outboxPruneAcked(100, 50)(s0.copyWith(dirty: const {}));
      expect(s1.outbox.map((i) => i.id), ['fresh', 'pending']);
      expect(s1.dirty.contains('a'), isTrue);
      expect(identical(r.outboxPruneAcked(100, 50)(s1), s1), isTrue);
    });

    test('pending writes merge per key and clear', () {
      const w = PendingWrite(
        key: 'k',
        table: 'thing',
        recordId: 'thing:1',
        data: {'a': 1},
        before: {'a': 0},
        firstAt: 1,
      );
      var s = r.mergePendingWrite(w)(buildState());
      s = r.mergePendingWrite(const PendingWrite(
        key: 'k',
        table: 'thing',
        recordId: 'thing:1',
        data: {'b': 2},
        before: null,
        firstAt: 9,
      ))(s);
      final merged = s.pendingWrites['k']!;
      expect(merged.data, {'a': 1, 'b': 2});
      expect(merged.before, {'a': 0});
      expect(merged.firstAt, 1);
      s = r.clearPendingWrite('k')(s);
      expect(s.pendingWrites, isEmpty);
      expect(identical(r.clearPendingWrite('k')(s), s), isTrue);
    });

    test('setFailedCount', () {
      final s0 = buildState();
      final s1 = r.setFailedCount(2)(s0);
      expect(s1.failedCount, 2);
      expect(identical(r.setFailedCount(2)(s1), s1), isTrue);
    });
  });

  group('bucket switch reducers', () {
    test(
        'rebindQuery swaps the id and sync state; clearBucketState wipes slices',
        () {
      final s0 = buildState([
        e('a',
            lifecycle: const QueryLifecycle(
                phase: QueryPhase.live,
                remote: RemotePhase.registered,
                fetchDepth: 0,
                notified: true),
            remoteArray: [('t:1', 1)],
            records: [
              {'id': 't:1'}
            ],
            serverState: ServerViewState.ready,
            registerAttempts: 2),
      ], [
        r.setVersions([('t:1', 1)]),
        r.outboxReplace([buildOutboxItem()]),
      ]);
      final id = RecordId('_00_query', 'new');
      final s1 = r.rebindQuery('a',
          id: id,
          lifecycle: seedLifecycle(true),
          remoteArray: [('t:2', 1)],
          localArray: const [])(s0.copyWith(dirty: const {}));
      final en = s1.queries['a']!;
      expect(en.def.id, id);
      expect(en.lifecycle.phase, QueryPhase.cached);
      expect(en.remoteArray, [('t:2', 1)]);
      expect(en.records, isEmpty);
      expect(en.serverState, isNull);
      expect(en.registerAttempts, 0);
      expect(s1.dirty.contains('a'), isTrue);
      expect(
        identical(
            r.rebindQuery('zz',
                id: id,
                lifecycle: en.lifecycle,
                remoteArray: const [],
                localArray: const [])(s1),
            s1),
        isTrue,
      );
      final s2 = r.clearBucketState()(s1);
      expect(s2.versions, isEmpty);
      expect(s2.outbox, isEmpty);
      expect(s2.dirty, isEmpty);
      expect(s2.primed, isFalse);
    });
  });

  group('identity / connection / compose', () {
    test('sets fields and short-circuits on no change', () {
      final s0 = buildState();
      final s1 = r.setIdentity(sessionId: 's', userId: 'u', bucketId: 'b')(s0);
      expect(s1.sessionId, 's');
      expect(s1.userId, 'u');
      expect(s1.bucketId, 'b');
      final s2 = r.setTabRole(TabRole.leader)(s1);
      expect(identical(r.setTabRole(TabRole.leader)(s2), s2), isTrue);
      final s3 = r.setConnection(ConnectionState.connected)(s2);
      expect(s3.sync.health.connection, ConnectionState.connected);
      expect(identical(r.setConnection(ConnectionState.connected)(s3), s3),
          isTrue);
      final s4 = r.setHealth(
          s3.sync.health.copyWith(status: SyncHealthStatus.degraded))(s3);
      expect(s4.sync.health.status, SyncHealthStatus.degraded);
      final s5 = r.patchSync(pollIdleStreak: 4)(s4);
      expect(s5.sync.pollIdleStreak, 4);
      final s6 = r.compose([r.setFailedCount(1), r.setFailedCount(3)])(s5);
      expect(s6.failedCount, 3);
    });

    test('setIdentity tells "leave alone" from "set to null"', () {
      final s0 = r.setIdentity(userId: 'u', saltUserId: 'u')(buildState());
      final s1 = r.setIdentity(localReady: true)(s0);
      expect(s1.userId, 'u');
      final s2 = r.setIdentity(userId: null)(s1);
      expect(s2.userId, isNull);
      expect(s2.saltUserId, 'u');
    });
  });
}
