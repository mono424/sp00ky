import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/query/lifecycle_saga.dart';
import 'package:spooky_core/src/query/sql.dart' as sql;
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

const registered = QueryLifecycle(
    phase: QueryPhase.live,
    remote: RemotePhase.registered,
    fetchDepth: 0,
    notified: false);

void main() {
  group('lifecycleTick', () {
    test(
        'evicts idle queries, heartbeats the rest, reschedules at half the ttl',
        () async {
      final out = await runPure<void>(
        (ctx) => lifecycleTick(ctx, env()),
        now: 1000,
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'idle', ttlMs: 100),
              lastSubscriberLeftAt: 0),
          buildEntry(
              def: buildDefinition(hash: 'kept', ttlMs: 400),
              lifecycle: registered,
              subscribers: 1),
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) => [
                const StatementResult.ok([1])
              ],
        }),
      );
      expect(out.state.queries.keys, ['kept']);
      expect(out.emitted.whereType<QueryEvictedEvent>().single.hash, 'idle');
      expect(out.state.queries['kept']!.lastHeartbeatAt, 1000);
      expect(out.timers['lifecycle']!.ms, 200);
      expect(out.dispatched.whereType<SyncOutcome>().single.ok, isTrue);
    });

    test('an SSP unregister that throws still evicts the query', () async {
      final out = await runPure<void>(
        (ctx) => lifecycleTick(ctx, env()),
        now: 1000,
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'idle', ttlMs: 100),
              lastSubscriberLeftAt: 0)
        ]),
        handlers: defaults(over: {
          'ssp.unregister': (_, __) => throw StateError('circuit gone'),
        }),
      );
      expect(out.state.queries, isEmpty);
      expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.debug);
    });

    test('a reclaimed row drops the registration and re-registers', () async {
      final out = await runPure<void>(
        (ctx) => lifecycleTick(ctx, env()),
        state: buildState([
          buildEntry(def: buildDefinition(hash: 'a'), lifecycle: registered)
        ]),
        handlers: defaults(over: {
          // An empty answer means the server reclaimed the view row.
          'remote.query': (_, __) => [const StatementResult.ok(<Object>[])],
        }),
      );
      expect(
          out.state.queries['a']!.lifecycle.remote, RemotePhase.unregistered);
      expect(out.dispatched.whereType<EnsureRegistered>(), hasLength(1));
      expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.warn);
    });

    test('a failed beat reports; empty state falls back to the default ttl',
        () async {
      final failed = await runPure<void>(
        (ctx) => lifecycleTick(ctx, env()),
        state: buildState([
          buildEntry(def: buildDefinition(hash: 'a'), lifecycle: registered)
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) => throw StateError('socket closed'),
        }),
      );
      expect(failed.dispatched.whereType<SyncOutcome>().single.ok, isFalse);

      final empty = await runPure<void>(
        (ctx) => lifecycleTick(ctx, env()),
        handlers: defaults(),
      );
      expect(empty.timers['lifecycle']!.ms, 300000);
      expect(empty.ofKind('remote.query'), isEmpty);
    });

    test('the heartbeat is one request for every registered view', () async {
      final out = await runPure<void>(
        (ctx) => lifecycleTick(ctx, env()),
        state: buildState([
          buildEntry(def: buildDefinition(hash: 'a'), lifecycle: registered),
          buildEntry(def: buildDefinition(hash: 'b'), lifecycle: registered),
          buildEntry(def: buildDefinition(hash: 'c')),
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) => [
                const StatementResult.ok([1]),
                const StatementResult.ok([1]),
              ],
        }),
      );
      final sent = out.ofKind('remote.query').single as RemoteQuery;
      expect(sent.sql.split(';\n'), hasLength(2));
      expect(sent.vars!.keys, ['id0', 'id1']);
    });
  });

  group('ackPrune', () {
    test('drops expired acked items and re-arms while some remain', () async {
      final out = await runPure<void>(
        ackPrune,
        now: 100000,
        state: buildState([
          buildEntry(def: buildDefinition(hash: 'a'))
        ], [
          r.outboxReplace([
            buildOutboxItem(id: 'old', status: OutboxStatus.acked, ackedAt: 0),
            buildOutboxItem(
                id: 'fresh', status: OutboxStatus.acked, ackedAt: 99000),
          ]),
        ]),
        handlers: defaults(),
      );
      expect(out.state.outbox.map((i) => i.id), ['fresh']);
      expect(out.timers['ack-prune']!.ms, 30000);
    });

    test('nothing acked: no timer', () async {
      final out = await runPure<void>(ackPrune, handlers: defaults());
      expect(out.timers, isEmpty);
    });
  });

  group('gcTick', () {
    test('deletes bodies no view names and no outbox item touches', () async {
      final out = await runPure<void>(
        gcTick,
        state: buildState([], [
          r.setVersions([
            ('thing:keep', 1),
            ('thing:pending', 1),
            ('thing:orphan', 1),
            ('_00_view:x', 1),
          ]),
          r.outboxReplace([buildOutboxItem(recordId: 'thing:pending')]),
        ]),
        handlers: defaults(over: {
          'local.getAll': (e, __) => (e as LocalGetAll).table == sql.viewTable
              ? [
                  {
                    'ids': [
                      ['thing:keep', 1]
                    ]
                  }
                ]
              : <Map<String, dynamic>>[],
        }),
      );
      final deleted =
          out.ofKind('local.delete').map((e) => (e as LocalDelete).id).toList();
      expect(deleted, ['thing:orphan'],
          reason: 'internal rows and anything a view or the outbox names stay');
      expect(out.state.versions.containsKey('thing:orphan'), isFalse);
      expect(out.ofKind('ssp.ingest'), hasLength(1));
      expect(out.timers['gc']!.ms, 604800000);
    });

    test('a failing sweep is logged and still reschedules', () async {
      final out = await runPure<void>(
        gcTick,
        handlers: defaults(over: {
          'local.getAll': (_, __) => throw StateError('store closed'),
        }),
      );
      expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.warn);
      expect(out.timers['gc'], isNotNull);
    });
  });
}
