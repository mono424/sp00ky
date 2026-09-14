import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/mutation/mutation_id.dart' show pendingTable;
import 'package:spooky_core/src/mutation/push_saga.dart';
import 'package:spooky_core/src/mutation/rows.dart';
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

Map<String, dynamic> pendingDoc(
  String id, {
  String type = 'create',
  String recordId = 'thing:1',
  Map<String, dynamic>? data,
  Map<String, dynamic>? before,
}) =>
    {
      'id': '$pendingTable:$id',
      'mutationType': type,
      'recordId': recordId,
      'tableName': 'thing',
      if (data != null || type == 'create') 'data': data ?? {'title': 'x'},
      if (before != null) 'beforeRecord': before,
      'createdAt': 1,
      'v': 2,
    };

EffectHandler storeWith(Map<String, Map<String, dynamic>> docs) =>
    (e, __) => docs[(e as LocalGet).id];

void main() {
  group('drain', () {
    test('pushes in FIFO order, acks each accepted row, deletes its row',
        () async {
      final docs = {
        '$pendingTable:m1': pendingDoc('m1'),
        '$pendingTable:m2': pendingDoc('m2', recordId: 'thing:2'),
      };
      final out = await runPure<void>(
        (ctx) => drain(ctx, env()),
        state: buildState([], [
          r.outboxReplace([
            buildOutboxItem(id: '$pendingTable:m1'),
            buildOutboxItem(id: '$pendingTable:m2', recordId: 'thing:2'),
          ]),
        ]),
        handlers: defaults(over: {
          'local.get': storeWith(docs),
          'remote.query': (_, __) => [
                const StatementResult.ok(null),
                const StatementResult.ok(null),
              ],
        }),
      );
      final sent = out.ofKind('remote.query').single as RemoteQuery;
      expect(sent.sql.split(';\n'), hasLength(2));
      expect(out.state.outbox.map((i) => i.status),
          everyElement(OutboxStatus.acked));
      expect(out.ofKind('local.tx'), hasLength(2));
      expect(out.emitted.whereType<MutationSettledEvent>(), hasLength(2));
      expect(out.timers['ack-prune']!.ms, 30000);
      expect(out.dispatched.whereType<SyncOutcome>().single.ok, isTrue);
    });

    test('a rejected statement rolls back only that row', () async {
      final docs = {
        '$pendingTable:m1': pendingDoc('m1'),
        '$pendingTable:m2': pendingDoc('m2', recordId: 'thing:2'),
      };
      final out = await runPure<void>(
        (ctx) => drain(ctx, env()),
        state: buildState([], [
          r.outboxReplace([
            buildOutboxItem(id: '$pendingTable:m1'),
            buildOutboxItem(id: '$pendingTable:m2', recordId: 'thing:2'),
          ]),
        ]),
        handlers: defaults(over: {
          'local.get': storeWith(docs),
          'remote.query': (_, __) => [
                const StatementResult.err('permission denied'),
                const StatementResult.ok(null),
              ],
        }),
      );
      expect(out.state.outbox.map((i) => i.id), ['$pendingTable:m2']);
      expect(out.state.outbox.single.status, OutboxStatus.acked);
      expect(out.state.failedCount, 1);
      expect(out.emitted.whereType<MutationRolledBackEvent>(), hasLength(1));
    });

    test(
        'a transient server state keeps the tail queued instead of dropping it',
        () async {
      // The 2026-09-08 import loss: these read as application errors before,
      // so every queued CREATE behind a stalled database was rolled back.
      for (final message in const [
        'Transaction conflict: Resource busy',
        'Specify a namespace to use',
      ]) {
        final out = await runPure<void>(
          (ctx) => drain(ctx, env()),
          state: buildState([], [
            r.outboxReplace([buildOutboxItem(id: '$pendingTable:m1')]),
          ]),
          handlers: defaults(over: {
            'local.get': storeWith({'$pendingTable:m1': pendingDoc('m1')}),
            'remote.query': (_, __) => [StatementResult.err(message)],
          }),
        );
        expect(out.state.outbox, hasLength(1), reason: message);
        expect(out.state.failedCount, 0, reason: message);
        expect(out.timers['outbox'], isNotNull, reason: message);
      }
    });

    test('a thrown transport failure backs off and stops', () async {
      final out = await runPure<void>(
        (ctx) => drain(ctx, env()),
        state: buildState([], [
          r.outboxReplace([buildOutboxItem(id: '$pendingTable:m1')]),
        ]),
        handlers: defaults(over: {
          'local.get': storeWith({'$pendingTable:m1': pendingDoc('m1')}),
          'remote.query': (_, __) => throw StateError('socket closed'),
        }),
      );
      expect(out.state.outbox.single.attempts, 1);
      expect(out.timers['outbox']!.ms, 500);
      expect(out.state.failedCount, 0);
    });

    test('a short result list is a cut: the rest stays queued', () async {
      final docs = {
        '$pendingTable:m1': pendingDoc('m1'),
        '$pendingTable:m2': pendingDoc('m2', recordId: 'thing:2'),
      };
      final out = await runPure<void>(
        (ctx) => drain(ctx, env()),
        state: buildState([], [
          r.outboxReplace([
            buildOutboxItem(id: '$pendingTable:m1'),
            buildOutboxItem(id: '$pendingTable:m2', recordId: 'thing:2'),
          ]),
        ]),
        handlers: defaults(over: {
          'local.get': storeWith(docs),
          'remote.query': (_, __) => [const StatementResult.ok(null)],
        }),
      );
      expect(out.state.outbox.map((i) => i.status),
          [OutboxStatus.acked, OutboxStatus.pending]);
      expect(out.timers['outbox'], isNotNull);
      expect(out.dispatched.whereType<SyncOutcome>().single.ok, isFalse);
    });

    test('a request rejected as a whole rolls back the head and continues',
        () async {
      final out = await runPure<void>(
        (ctx) => drain(ctx, env()),
        state: buildState([], [
          r.outboxReplace([buildOutboxItem(id: '$pendingTable:m1')]),
        ]),
        handlers: defaults(over: {
          'local.get': storeWith({'$pendingTable:m1': pendingDoc('m1')}),
          'remote.query': (_, __) => throw StateError('parse error in query'),
        }),
      );
      expect(out.state.outbox, isEmpty);
      expect(out.state.failedCount, 1);
    });

    test('a pending row the store no longer has is dropped from the outbox',
        () async {
      final out = await runPure<void>(
        (ctx) => drain(ctx, env()),
        state: buildState([], [
          r.outboxReplace([buildOutboxItem(id: '$pendingTable:gone')]),
        ]),
        handlers: defaults(),
      );
      expect(out.state.outbox, isEmpty);
      expect(out.ofKind('remote.query'), isEmpty);
    });

    test('an empty outbox does nothing', () async {
      final out =
          await runPure<void>((ctx) => drain(ctx, env()), handlers: defaults());
      expect(out.log, hasLength(1));
    });

    test('stored payloads are re-typed from the schema before pushing',
        () async {
      final out = await runPure<void>(
        (ctx) => drain(ctx, env()),
        state: buildState([], [
          r.outboxReplace([buildOutboxItem(id: '$pendingTable:m1')]),
        ]),
        handlers: defaults(over: {
          'local.get': storeWith({
            '$pendingTable:m1':
                pendingDoc('m1', type: 'update', data: {'owner': 'user:u1'})
          }),
          'remote.query': (_, __) => [const StatementResult.ok(null)],
        }),
      );
      final sent = out.ofKind('remote.query').single as RemoteQuery;
      expect(sent.vars!['data0'], isA<Map>());
      expect((sent.vars!['data0'] as Map)['owner'].runtimeType.toString(),
          'RecordId');
    });
  });

  group('rollback', () {
    PendingMutationRow row(MutationEventType type,
            {Map<String, dynamic>? before}) =>
        PendingMutationRow(
          id: '$pendingTable:m1',
          mutationType: type,
          recordId: 'thing:1',
          tableName: 'thing',
          data: const {'title': 'x'},
          beforeRecord: before,
          createdAt: 1,
          v: 2,
        );

    const err =
        FailedMutationError(message: 'nope', kind: FailedErrorKind.application);

    test('a create: tray move first, then the local delete and circuit DELETE',
        () async {
      final out = await runPure<void>(
        (ctx) => rollback(ctx, env(), row(MutationEventType.create), err),
        state: buildState([
          buildEntry(def: buildDefinition(hash: 'a'))
        ], [
          r.setVersions([('thing:1', 1)]),
          r.outboxReplace([buildOutboxItem(id: '$pendingTable:m1')]),
        ]),
        handlers: defaults(),
      );
      final txs = out.ofKind('local.tx').cast<LocalTx>().toList();
      expect(txs, hasLength(2));
      expect((txs[0].ops[0] as PutOp).table, failedTable,
          reason: 'the tray row is written before anything is undone');
      expect(txs[1].ops.single, isA<DeleteOp>());
      expect(out.state.versions.containsKey('thing:1'), isFalse);
      expect(out.state.outbox, isEmpty);
      expect(out.state.failedCount, 1);
      expect(out.state.dirty, contains('a'));
      expect(out.emitted.whereType<TrayChangedEvent>().single.count, 1);
    });

    test('an update restores beforeRecord and its version', () async {
      final out = await runPure<void>(
        (ctx) => rollback(
            ctx,
            env(),
            row(MutationEventType.update,
                before: {'id': 'thing:1', 'title': 'old', '_00_rv': 4}),
            err),
        handlers: defaults(),
      );
      final restore = out.ofKind('local.tx').cast<LocalTx>().last;
      expect((restore.ops.single as PutOp).data['title'], 'old');
      expect(out.state.versions['thing:1'], 4);
    });

    test('a legacy row with no beforeRecord asks the server for it', () async {
      final out = await runPure<void>(
        (ctx) => rollback(ctx, env(), row(MutationEventType.update), err),
        handlers: defaults(over: {
          'remote.query': (_, __) => [
                const StatementResult.ok(
                    {'id': 'thing:1', 'title': 'server', '_00_rv': 9})
              ],
        }),
      );
      expect(out.state.versions['thing:1'], 9);
    });

    test('an unreachable server leaves the revert partial, not wrong',
        () async {
      final out = await runPure<void>(
        (ctx) => rollback(ctx, env(), row(MutationEventType.update), err),
        handlers: defaults(over: {
          'remote.query': (_, __) => throw StateError('socket closed'),
        }),
      );
      // Only the tray move ran: nothing can be restored without a before row.
      expect(out.ofKind('local.tx'), hasLength(1));
      expect(out.state.failedCount, 1);
    });

    test('every local step is best-effort', () async {
      final out = await runPure<void>(
        (ctx) => rollback(ctx, env(), row(MutationEventType.create), err),
        handlers: defaults(over: {
          'local.tx': (_, __) => throw StateError('disk full'),
          'ssp.ingest': (_, __) => throw StateError('circuit gone'),
        }),
      );
      expect(out.state.failedCount, 1);
      expect(out.emitted.whereType<LogEvent>(), hasLength(3));
    });
  });

  group('loadOutbox', () {
    test('mirrors pending rows, trays unreplayable ones, starts a drain',
        () async {
      final out = await runPure<void>(
        (ctx) => loadOutbox(ctx, env()),
        handlers: defaults(over: {
          'local.getAll': (e, __) => (e as LocalGetAll).table == pendingTable
              ? [
                  pendingDoc('m2'),
                  pendingDoc('m1'),
                  // A create with no payload can never be replayed.
                  {
                    'id': '$pendingTable:m3',
                    'mutationType': 'create',
                    'recordId': 'thing:3',
                  },
                ]
              : <Map<String, dynamic>>[],
        }),
      );
      expect(out.state.outbox.map((i) => i.id),
          ['$pendingTable:m1', '$pendingTable:m2'],
          reason: 'the drain order is id order');
      expect(out.emitted.whereType<MutationRolledBackEvent>(), hasLength(1));
      expect(out.dispatched.whereType<Drain>(), hasLength(1));
    });

    test('an empty outbox starts no drain', () async {
      final out = await runPure<void>(
        (ctx) => loadOutbox(ctx, env()),
        handlers: defaults(),
      );
      expect(out.dispatched, isEmpty);
    });
  });
}
