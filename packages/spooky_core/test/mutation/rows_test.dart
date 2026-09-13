import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/mutation/mutation_id.dart';
import 'package:spooky_core/src/mutation/rows.dart';
import 'package:spooky_core/src/services/stream_processor/stream_processor_service.dart'
    show IngestOp;
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:spooky_core/src/utils/parser.dart' show ColumnSchema;
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

void main() {
  group('stored ids', () {
    test('strips the escaping and reads the timestamp prefix', () {
      expect(storedIdString('_00_pending_mutations:⟨1700000000000_0001_ab⟩'),
          '_00_pending_mutations:1700000000000_0001_ab');
      expect(storedIdString(RecordId('_00_pending_mutations', 'x')),
          '_00_pending_mutations:x');
      expect(storedIdString('bare'), 'bare');
      expect(createdAtFromId('_00_pending_mutations:1700000000000_0001_ab'),
          1700000000000);
      expect(createdAtFromId('_00_pending_mutations:⟨1700000000000_0001_ab⟩'),
          1700000000000);
      expect(createdAtFromId('_00_pending_mutations:123'), isNull);
    });

    test('mutation ids sort chronologically and name their client', () {
      final a = mintMutationId('tabA');
      final b = mintMutationId('tabA');
      expect(a.compareTo(b), lessThan(0));
      expect(mutationOwnerClientId(a), 'tabA');
      expect(mutationOwnerClientId('_00_pending_mutations:1700000000000'),
          isNull);
    });
  });

  group('parsePendingRow', () {
    test('reads v2 rows', () {
      final row = parsePendingRow({
        'id': '_00_pending_mutations:1700000000000_0001_ab',
        'mutationType': 'update',
        'recordId': 'thing:1',
        'tableName': 'thing',
        'data': {'a': 1},
        'beforeRecord': {'a': 0},
        'createdAt': 5,
        'v': 2,
      })!;
      expect(row.mutationType, MutationEventType.update);
      expect(row.recordId, 'thing:1');
      expect(row.data, {'a': 1});
      expect(row.beforeRecord, {'a': 0});
      expect(row.createdAt, 5);
      expect(row.v, 2);
      expect(toOutboxItem(row).status, OutboxStatus.pending);
      expect(toOutboxItem(row).table, 'thing');
    });

    test('tolerates legacy rows and rejects unreplayable ones', () {
      final legacy = parsePendingRow({
        'id': '_00_pending_mutations:1700000000000_0001_ab',
        'mutationType': 'delete',
        'recordId': 'thing:9',
      })!;
      expect(legacy.tableName, 'thing');
      expect(legacy.v, 1);
      expect(legacy.createdAt, 1700000000000);

      expect(parsePendingRow(null), isNull);
      expect(parsePendingRow({'mutationType': 'nope'}), isNull);
      expect(parsePendingRow({'mutationType': 'create', 'recordId': 1}), isNull);
      // A create with no payload cannot be replayed.
      expect(
          parsePendingRow({
            'id': '_00_pending_mutations:1',
            'mutationType': 'create',
            'recordId': 'thing:1'
          }),
          isNull);
    });

    test('sorts the drain by id, the tray by failure time', () {
      PendingMutationRow p(String id) => PendingMutationRow(
            id: id,
            mutationType: MutationEventType.delete,
            recordId: 'thing:1',
            tableName: 'thing',
            createdAt: 0,
            v: 2,
          );
      expect(sortPendingRows([p('b'), p('a')]).map((r) => r.id), ['a', 'b']);
    });
  });

  group('local write transactions', () {
    const input = WritePlanInput(
      recordId: 'thing:1',
      mutationId: '_00_pending_mutations:m1',
      table: 'thing',
      data: {'a': 1},
      before: {'a': 0},
      now: 42,
    );

    test('create writes the row seeded at rv 1 plus the v2 outbox entry', () {
      final ops = planCreateTx(input);
      expect(ops, hasLength(2));
      final row = ops[0] as PutOp;
      expect(row.table, 'thing');
      expect(row.id, 'thing:1');
      expect(row.data, {'a': 1, 'id': 'thing:1', '_00_rv': 1});
      expect(row.mode, WriteMode.replace);
      final outbox = ops[1] as PutOp;
      expect(outbox.table, pendingTable);
      expect(outbox.data['mutationType'], 'create');
      expect(outbox.data['v'], pendingRowVersion);
      expect(outbox.data['createdAt'], 42);
      expect(outbox.data.containsKey('beforeRecord'), isFalse);
    });

    test('update bumps rv, merges, and records beforeRecord', () {
      final ops = planUpdateTx(input);
      expect(ops[0], isA<BumpRvOp>());
      expect((ops[1] as PutOp).mode, WriteMode.merge);
      expect((ops[2] as PutOp).data['beforeRecord'], {'a': 0});
      expect((ops[2] as PutOp).data['mutationType'], 'update');
    });

    test('delete removes the row and records beforeRecord', () {
      final ops = planDeleteTx(input);
      expect(ops[0], isA<DeleteOp>());
      expect((ops[1] as PutOp).data['mutationType'], 'delete');
      expect((ops[1] as PutOp).data['beforeRecord'], {'a': 0});
      expect((ops[1] as PutOp).data.containsKey('data'), isFalse);
    });

    test('debounced update writes locally now and the outbox row later', () {
      final local = planLocalOnlyUpdateTx('thing', 'thing:1', {'a': 1});
      expect(local, hasLength(2));
      expect(local.whereType<PutOp>().single.mode, WriteMode.merge);
      final deferred = planDeferredOutboxRowTx(input);
      expect(deferred, hasLength(1));
      expect((deferred.single as PutOp).table, pendingTable);
    });
  });

  group('remoteBatch', () {
    test('one statement per row, indexed vars, mixed types', () {
      PendingMutationRow row(MutationEventType type, String id,
              [Map<String, dynamic>? data]) =>
          PendingMutationRow(
            id: '_00_pending_mutations:$id',
            mutationType: type,
            recordId: 'thing:$id',
            tableName: 'thing',
            data: data,
            createdAt: 0,
            v: 2,
          );
      final batch = remoteBatch([
        row(MutationEventType.create, 'a', {'x': 1}),
        row(MutationEventType.update, 'b', {'y': 2}),
        row(MutationEventType.delete, 'c'),
      ]);
      expect(batch.sql.split(';\n'), [
        'CREATE ONLY \$id0 SET x = \$d0_x',
        'UPDATE \$id1 MERGE \$data1',
        'DELETE \$id2',
      ]);
      expect(batch.vars['d0_x'], 1);
      expect(batch.vars['data1'], {'y': 2});
      expect((batch.vars['id2'] as RecordId).id, 'c');
    });
  });

  group('rollback plans', () {
    PendingMutationRow row(MutationEventType type,
            {Map<String, dynamic>? data}) =>
        PendingMutationRow(
          id: '_00_pending_mutations:m1',
          mutationType: type,
          recordId: 'thing:1',
          tableName: 'thing',
          data: data,
          createdAt: 1,
          v: 2,
        );

    test('a create reverts by delete', () {
      final plan = planRevert(row(MutationEventType.create, data: {'a': 1}), null);
      expect(plan.revert, RevertKind.full);
      expect(plan.ops.single, isA<DeleteOp>());
      expect(plan.circuit.op, IngestOp.delete);
    });

    test('update and delete restore beforeRecord, or are partial without it', () {
      final restored =
          planRevert(row(MutationEventType.update), {'a': 0, 'id': 'thing:1'});
      expect(restored.revert, RevertKind.full);
      expect((restored.ops.single as PutOp).data['a'], 0);
      expect(restored.circuit.op, IngestOp.update);

      final undeleted =
          planRevert(row(MutationEventType.delete), {'a': 0, 'id': 'thing:1'});
      expect(undeleted.circuit.op, IngestOp.create);

      final partial = planRevert(row(MutationEventType.update), null);
      expect(partial.revert, RevertKind.partial);
      expect(partial.ops, isEmpty);
    });

    test('failed rows: build, move, delete, parse', () {
      final pending = row(MutationEventType.update, data: {'a': 1});
      final failed = buildFailedRow(
        pending,
        const FailedMutationError(
            message: 'nope', kind: FailedErrorKind.application),
        {'a': 0},
        2,
        99,
        RevertKind.full,
      );
      expect(failedRecordId(failed.id), '_00_failed_mutations:m1');
      final ops = moveToFailedTx(failed);
      expect((ops[0] as PutOp).table, failedTable);
      expect((ops[1] as DeleteOp).table, pendingTable);

      final parsed = parseFailedRow({
        'id': '_00_failed_mutations:m1',
        ...failed.toJson(),
      })!;
      expect(parsed.id, '_00_pending_mutations:m1');
      expect(parsed.error.message, 'nope');
      expect(parsed.error.kind, FailedErrorKind.application);
      expect(parsed.attempts, 2);
      expect(parsed.failedAt, 99);
      expect(parsed.revert, RevertKind.full);
      expect(parsed.toPending().mutationType, MutationEventType.update);

      expect((deleteFailedRow('_00_pending_mutations:m1') as DeleteOp).id,
          '_00_failed_mutations:m1');
      expect((deletePendingRow('_00_pending_mutations:m1') as DeleteOp).id,
          '_00_pending_mutations:m1');
      expect(parseFailedRow(null), isNull);
      expect(sortFailedRows([parsed]).single.id, parsed.id);
    });
  });

  group('hydrateRowData', () {
    test('re-types record and datetime fields, keeps unknown keys', () {
      final row = PendingMutationRow(
        id: '_00_pending_mutations:m1',
        mutationType: MutationEventType.update,
        recordId: 'thing:1',
        tableName: 'thing',
        data: const {
          'owner': 'user:u1',
          'at': '2026-01-02T03:04:05Z',
          'extra': 7
        },
        createdAt: 0,
        v: 2,
      );
      const columns = <String, ColumnSchema>{
        'owner': ColumnSchema(recordId: true, type: 'record<user>'),
        'at': ColumnSchema(dateTime: true, type: 'datetime'),
      };
      final out = hydrateRowData(row, columns);
      expect(out.data!['owner'], isA<RecordId>());
      expect(out.data!['at'], isA<DateTime>());
      expect(out.data!['extra'], 7);
      // No data or no schema: unchanged.
      expect(identical(hydrateRowData(row, null), row), isTrue);
    });
  });
}
