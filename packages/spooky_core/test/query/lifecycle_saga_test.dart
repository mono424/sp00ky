import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/modules/query_builder.dart' show RelationPlan;
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
    test('evicts idle queries, heartbeats the rest, reschedules at half the ttl',
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
      expect(out.state.queries['a']!.lifecycle.remote,
          RemotePhase.unregistered);
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
            buildOutboxItem(
                id: 'old', status: OutboxStatus.acked, ackedAt: 0),
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
    const day = 24 * 60 * 60 * 1000;
    const now = 1700000000000;

    /// A view table holding [views] (deletes remove rows), every other table
    /// empty.
    Map<String, EffectHandler> viewHandlers(List<Map<String, dynamic>> views) {
      final table = [...views];
      return defaults(over: {
        'local.getAll': (e, __) => (e as LocalGetAll).table == sql.viewTable
            ? [...table]
            : <Map<String, dynamic>>[],
        'local.delete': (e, __) {
          final del = e as LocalDelete;
          if (del.table == sql.viewTable) {
            table.removeWhere((row) => row['id'] == del.id);
          }
          return null;
        },
      });
    }

    List<String> deletedIds(RunPureResult<void> out) =>
        out.ofKind('local.delete').map((e) => (e as LocalDelete).id).toList();

    test('deletes bodies no view, query or outbox item retains', () async {
      final out = await runPure<void>(
        gcTick,
        now: now,
        state: buildState([], [
          r.setIdentity(primed: true),
          r.setVersions([
            ('thing:keep', 1),
            ('thing:pending', 1),
            ('thing:orphan', 1),
            ('_00_view:x', 1),
          ]),
          r.outboxReplace([buildOutboxItem(recordId: 'thing:pending')]),
        ]),
        handlers: viewHandlers([
          {
            'id': '_00_view:x',
            'ids': [
              ['thing:keep', 1]
            ],
            'updatedAt': now,
          }
        ]),
      );
      expect(deletedIds(out), ['thing:orphan'],
          reason: 'internal rows and anything a view or the outbox names stay');
      expect(out.state.versions.containsKey('thing:orphan'), isFalse);
      expect(out.ofKind('ssp.ingest'), hasLength(1));
      expect(out.dispatched.map((e) => e.type), ['FetchRows']);
      expect(out.timers['gc']!.ms, 60 * 60 * 1000);
    });

    test('keeps the subquery children a view row records', () async {
      final out = await runPure<void>(
        gcTick,
        now: now,
        state: buildState([], [
          r.setIdentity(primed: true),
          r.setVersions([('thing:1', 1), ('user:a', 1), ('user:gone', 1)]),
        ]),
        handlers: viewHandlers([
          {
            'id': '_00_view:list',
            'ids': [
              ['thing:1', 1]
            ],
            'children': [
              ['user:a', 1]
            ],
            'updatedAt': now,
          }
        ]),
      );
      expect(deletedIds(out), ['user:gone']);
    });

    test('keeps what a query in state holds, members and children', () async {
      final out = await runPure<void>(
        gcTick,
        now: now,
        state: buildState([
          buildEntry(
            remoteArray: const [('thing:1', 1)],
            subqueryRemoteArray: const [('user:a', 1)],
          ),
        ], [
          r.setIdentity(primed: true),
          r.setVersions([('thing:1', 1), ('user:a', 1), ('user:gone', 1)]),
        ]),
        handlers: viewHandlers(const []),
      );
      expect(deletedIds(out), ['user:gone'],
          reason: 'a view row written before children were recorded must not '
              'cost the bodies a live query has just been answered with');
    });

    test('keeps the joined rows a query in state renders, before any answer',
        () async {
      final out = await runPure<void>(
        gcTick,
        now: now,
        state: buildState([
          buildEntry(
            def: buildDefinition().withRelations([
              RelationPlan(
                  alias: 'owner',
                  table: 'user',
                  cardinality: 'one',
                  foreignKeyField: 'owner'),
            ]),
            records: [
              {
                'id': 'thing:1',
                'owner': {'id': 'user:a', 'username': 'a'},
              },
            ],
          ),
        ], [
          r.setIdentity(primed: true),
          r.setVersions([('thing:1', 1), ('user:a', 1), ('user:gone', 1)]),
        ]),
        handlers: viewHandlers([
          {
            'id': '_00_view:list',
            'ids': [
              ['thing:1', 1]
            ],
            'updatedAt': now,
          }
        ]),
      );
      expect(deletedIds(out), ['user:gone'],
          reason: 'a view row from before children were recorded, offline');
    });

    test('retires stale unheld view rows and collects what only they named',
        () async {
      final out = await runPure<void>(
        gcTick,
        now: now,
        state: buildState([
          buildEntry(def: buildDefinition(hash: 'h', viewKey: 'held')),
        ], [
          r.setIdentity(primed: true),
          r.setVersions(
              [('thing:old', 1), ('thing:held', 1), ('thing:new', 1)]),
        ]),
        handlers: viewHandlers([
          {
            'id': '_00_view:stale',
            'ids': [
              ['thing:old', 1]
            ],
            'updatedAt': now - 15 * day,
          },
          {
            'id': '_00_view:held',
            'ids': [
              ['thing:held', 1]
            ],
            'updatedAt': now - 15 * day,
          },
          {
            'id': '_00_view:fresh',
            'ids': [
              ['thing:new', 1]
            ],
            'updatedAt': now - day,
          },
        ]),
      );
      expect(deletedIds(out), ['_00_view:stale', 'thing:old'],
          reason: 'a held view row stays however old, a fresh one stays');
      expect(out.emitted.whereType<LogEvent>().last.data,
          {'removed': 1, 'retiredViews': 1});
    });

    test('a query mounted while the rows were read keeps its row', () async {
      final out = await runPure<void>(
        gcTick,
        now: now,
        state: buildState([], [
          r.setIdentity(primed: true),
          r.setVersions([('thing:old', 1)]),
        ]),
        handlers: defaults(over: {
          'local.getAll': (e, ctx) {
            if ((e as LocalGetAll).table != sql.viewTable) {
              return <Map<String, dynamic>>[];
            }
            ctx.state = r.putQuery(buildEntry(
                def: buildDefinition(hash: 'm', viewKey: 'stale')))(ctx.state);
            return [
              {
                'id': '_00_view:stale',
                'ids': [
                  ['thing:old', 1]
                ],
                'updatedAt': now - 15 * day,
              }
            ];
          },
        }),
      );
      expect(deletedIds(out), isEmpty);
    });

    test('stops when the bucket moved under the sweep', () async {
      final out = await runPure<void>(
        gcTick,
        now: now,
        state: buildState([], [
          r.setIdentity(primed: true, bucketId: 'u1'),
          r.setVersions([('thing:orphan', 1)]),
        ]),
        handlers: defaults(over: {
          'local.getAll': (_, ctx) {
            ctx.state = r.setIdentity(bucketId: 'u2')(ctx.state);
            return <Map<String, dynamic>>[];
          },
        }),
      );
      expect(deletedIds(out), isEmpty);
      expect(out.timers['gc'], isNotNull);
    });

    test('a failing sweep is logged and still reschedules', () async {
      final out = await runPure<void>(
        gcTick,
        state: buildState([], [r.setIdentity(primed: true)]),
        handlers: defaults(over: {
          'local.getAll': (_, __) => throw StateError('store closed'),
        }),
      );
      expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.warn);
      expect(out.timers['gc'], isNotNull);
    });
  });
}
