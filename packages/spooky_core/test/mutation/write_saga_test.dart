import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/mutation/mutation_id.dart' show pendingTable;
import 'package:spooky_core/src/mutation/write_saga.dart';
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/surreal/value.dart';
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

void main() {
  test('rejects a table the schema does not know', () async {
    await expectLater(
      runPure<WriteResult>(
        (ctx) => write(
            ctx,
            env(),
            const WriteInput(
                kind: MutationEventType.create, recordId: 'ghost:1', data: {})),
        handlers: defaults(),
      ),
      throwsArgumentError,
    );
  });

  test('create: local tx, outbox item, circuit ingest at version 1, drain',
      () async {
    final out = await runPure<WriteResult>(
      (ctx) => write(
          ctx,
          env(),
          const WriteInput(
            kind: MutationEventType.create,
            recordId: 'thing:1',
            data: {'title': 'hello', 'owner': 'user:u1'},
          )),
      state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
      handlers: defaults(over: {
        'local.get': (_, __) => {'id': 'thing:1', 'title': 'hello', '_00_rv': 1},
      }),
    );
    final tx = out.ofKind('local.tx').single as LocalTx;
    final row = tx.ops[0] as PutOp;
    expect(row.data['_00_rv'], 1);
    // The schema re-types the record link before it is stored.
    expect(row.data['owner'], isA<RecordId>());
    final outbox = tx.ops[1] as PutOp;
    expect(outbox.table, pendingTable);
    expect(outbox.data['mutationType'], 'create');

    expect(out.state.outbox.single.id, out.result.mutationId);
    expect(out.state.outbox.single.status, OutboxStatus.pending);
    expect(out.state.versions['thing:1'], 1);
    expect(out.state.dirty, contains('a'),
        reason: 'the overlay changed, so the table re-renders');
    expect(out.emitted.whereType<MutationEmittedEvent>(), hasLength(1));
    expect(out.dispatched.single, isA<Drain>());
  });

  test('update reads the previous row and takes the version off the new one',
      () async {
    final out = await runPure<WriteResult>(
      (ctx) => write(
          ctx,
          env(),
          const WriteInput(
            kind: MutationEventType.update,
            recordId: 'thing:1',
            data: {'title': 'renamed'},
          )),
      handlers: defaults(over: {
        'local.get': (_, __) =>
            {'id': 'thing:1', 'title': 'renamed', '_00_rv': 7},
      }),
    );
    final tx = out.ofKind('local.tx').single as LocalTx;
    expect(tx.ops[0], isA<BumpRvOp>());
    expect((tx.ops[1] as PutOp).mode, WriteMode.merge);
    expect((tx.ops[2] as PutOp).data['beforeRecord'], isNotNull);
    expect(out.state.versions['thing:1'], 7);
  });

  test('delete feeds the circuit with the row as it was', () async {
    final out = await runPure<WriteResult>(
      (ctx) => write(
          ctx,
          env(),
          const WriteInput(
              kind: MutationEventType.delete, recordId: 'thing:1')),
      handlers: defaults(over: {
        'local.get': (_, __) => {'id': 'thing:1', 'title': 'gone', '_00_rv': 3},
      }),
    );
    expect(out.result.record, isNull);
    expect(out.state.versions.containsKey('thing:1'), isFalse);
    final tx = out.ofKind('local.tx').single as LocalTx;
    expect(tx.ops[0], isA<DeleteOp>());
  });

  test('a failing circuit ingest is logged; the local write still stands',
      () async {
    final out = await runPure<WriteResult>(
      (ctx) => write(
          ctx,
          env(),
          const WriteInput(
              kind: MutationEventType.create,
              recordId: 'thing:1',
              data: {'title': 'x'})),
      handlers: defaults(over: {
        'ssp.ingest': (_, __) => throw StateError('circuit busy'),
      }),
    );
    expect(out.state.outbox, hasLength(1));
    expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.error);
  });

  group('debounced update', () {
    test('applies locally, merges the patch per key, arms the flush timer',
        () async {
      const input = WriteInput(
        kind: MutationEventType.update,
        recordId: 'thing:1',
        data: {'title': 'a'},
        options: UpdateOptions(debounced: true),
      );
      final first = await runPure<WriteResult>(
        (ctx) => write(ctx, env(), input),
        handlers: defaults(over: {
          'local.get': (_, __) => {'id': 'thing:1', '_00_rv': 2},
        }),
      );
      expect(first.result.mutationId, '');
      expect(first.state.outbox, isEmpty,
          reason: 'the outbox row is written on flush, not now');
      final key = first.state.pendingWrites.keys.single;
      expect(first.timers['debounce:$key']!.ms, 300);
      expect((first.timers['debounce:$key']!.event as FlushWrite).key, key);

      final second = await runPure<WriteResult>(
        (ctx) => write(
            ctx,
            env(),
            const WriteInput(
              kind: MutationEventType.update,
              recordId: 'thing:1',
              data: {'title': 'b'},
              options: UpdateOptions(
                  debounced: DebounceOptions(
                      key: DebounceKey.recordId, delay: 50)),
            )),
        state: first.state,
        handlers: defaults(over: {
          'local.get': (_, __) => {'id': 'thing:1', '_00_rv': 3},
        }),
      );
      expect(second.timers['debounce:thing:1']!.ms, 50);
    });

    test('the first beforeRecord survives the merge', () async {
      const input = WriteInput(
        kind: MutationEventType.update,
        recordId: 'thing:1',
        data: {'title': 'a'},
        options: UpdateOptions(
            debounced: DebounceOptions(key: DebounceKey.recordId)),
      );
      final first = await runPure<WriteResult>(
        (ctx) => write(ctx, env(), input),
        handlers: defaults(over: {
          'local.get': (_, __) => {'id': 'thing:1', 'title': 'original'},
        }),
      );
      final second = await runPure<WriteResult>(
        (ctx) => write(ctx, env(), input),
        state: first.state,
        handlers: defaults(over: {
          'local.get': (_, __) => {'id': 'thing:1', 'title': 'a'},
        }),
      );
      expect(second.state.pendingWrites['thing:1']!.before!['title'],
          'original');
      // The only read is the post-write read-back: a merged write never
      // re-reads the before row, which is what keeps the original.
      expect(second.ofKind('local.get'), hasLength(1));
      expect(second.log.indexWhere((e) => e.kind == 'local.tx'),
          lessThan(second.log.indexWhere((e) => e.kind == 'local.get')));
    });

    test('flush writes exactly one outbox row and drains', () async {
      final out = await runPure<void>(
        (ctx) => flushWrite(ctx, env(), 'k'),
        state: r.mergePendingWrite(const PendingWrite(
          key: 'k',
          table: 'thing',
          recordId: 'thing:1',
          data: {'title': 'final'},
          before: {'title': 'original'},
          firstAt: 1,
        ))(buildState()),
        handlers: defaults(),
      );
      expect(out.state.pendingWrites, isEmpty);
      final tx = out.ofKind('local.tx').single as LocalTx;
      expect(tx.ops, hasLength(1));
      expect((tx.ops.single as PutOp).data['data'], {'title': 'final'});
      expect(out.state.outbox.single.type, MutationEventType.update);
      expect(out.dispatched.single, isA<Drain>());
    });

    test('flushing an unknown key is a no-op', () async {
      final out = await runPure<void>(
        (ctx) => flushWrite(ctx, env(), 'nope'),
        handlers: defaults(),
      );
      expect(out.ofKind('local.tx'), isEmpty);
    });
  });
}
