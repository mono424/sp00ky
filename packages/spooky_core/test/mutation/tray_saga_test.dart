import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/mutation/mutation_id.dart' show pendingTable;
import 'package:spooky_core/src/mutation/rows.dart';
import 'package:spooky_core/src/mutation/tray_saga.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

Map<String, dynamic> failedDoc(String id, {int failedAt = 1}) => {
      'id': '$failedTable:$id',
      'mutationType': 'update',
      'recordId': 'thing:1',
      'tableName': 'thing',
      'data': {'title': 'x'},
      'beforeRecord': {'id': 'thing:1', 'title': 'old'},
      'error': {'message': 'nope', 'kind': 'application'},
      'attempts': 1,
      'createdAt': 1,
      'failedAt': failedAt,
      'revert': 'full',
    };

void main() {
  test('lists parsed rows oldest first, skipping junk', () async {
    final out = await runPure<List<FailedMutationRow>>(
      listFailed,
      handlers: defaults(over: {
        'local.getAll': (_, __) => [
              failedDoc('b', failedAt: 20),
              failedDoc('a', failedAt: 10),
              {'garbage': true},
            ],
      }),
    );
    expect(out.result.map((f) => f.failedAt), [10, 20]);
    expect(out.result.first.id, '$pendingTable:a');
  });

  test('retry re-applies the write with a new id and drops the tray row',
      () async {
    final out = await runPure<bool>(
      (ctx) => retryFailed(ctx, env(), '$pendingTable:a'),
      state: r.setFailedCount(1)(buildState()),
      handlers: defaults(over: {
        'local.get': (e, __) => (e as LocalGet).table == failedTable
            ? failedDoc('a')
            : {'id': 'thing:1', '_00_rv': 2},
      }),
    );
    expect(out.result, isTrue);
    expect(out.state.outbox, hasLength(1));
    expect(out.state.outbox.single.id, isNot('$pendingTable:a'),
        reason: 'a retry is a new write, with a fresh mutation id');
    expect(out.state.failedCount, 0);
    expect(out.emitted.whereType<TrayChangedEvent>().single.count, 0);
    final deletes = out
        .ofKind('local.tx')
        .cast<LocalTx>()
        .expand((t) => t.ops)
        .whereType<DeleteOp>();
    expect(deletes.map((d) => d.table), contains(failedTable));
  });

  test('discard drops the row and never goes below zero', () async {
    final out = await runPure<bool>(
      (ctx) => discardFailed(ctx, '$pendingTable:a'),
      handlers: defaults(over: {
        'local.get': (_, __) => failedDoc('a'),
      }),
    );
    expect(out.result, isTrue);
    expect(out.state.failedCount, 0);
    expect(out.ofKind('local.tx'), hasLength(1));
  });

  test('a missing row returns false and changes nothing', () async {
    final retry = await runPure<bool>(
      (ctx) => retryFailed(ctx, env(), '$pendingTable:gone'),
      handlers: defaults(),
    );
    expect(retry.result, isFalse);
    expect(retry.ofKind('local.tx'), isEmpty);

    final discard = await runPure<bool>(
      (ctx) => discardFailed(ctx, '$pendingTable:gone'),
      handlers: defaults(),
    );
    expect(discard.result, isFalse);
  });
}
