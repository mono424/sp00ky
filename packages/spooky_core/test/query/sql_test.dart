import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/query/sql.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:test/test.dart';

void main() {
  group('query sql builders', () {
    test('edge and meta selects are byte-identical to the old helpers', () {
      expect(listRefSelect('_00_list_ref_user_u1'),
          'SELECT out, version FROM _00_list_ref_user_u1 WHERE in = \$in AND parent IS NONE');
      expect(subqueryListRefSelect('_00_list_ref'),
          'SELECT out, version FROM _00_list_ref WHERE in = \$in AND parent IS NOT NONE');
      expect(queryRowCountSelect(),
          'SELECT VALUE { rowCount: rowCount, state: state } FROM ONLY \$in');
      expect(listRefBatchSelect('_00_list_ref'),
          'SELECT in, out, version, parent FROM _00_list_ref WHERE in IN \$ins');
      expect(queryRowCountBatchSelect(),
          'SELECT VALUE { id: id, rowCount: rowCount, state: state } FROM \$ins');
    });

    test('composes the single, batch and register requests', () {
      expect(singleSnapshotSelect('t').split(';\n'), hasLength(3));
      expect(batchSnapshotSelect('t').split(';\n'), hasLength(2));
      final reg = registerSelect('t');
      expect(reg.startsWith('fn::query::register(\$config);\n'), isTrue);
      expect(reg.split(';\n'), hasLength(4));

      final id = RecordId('_00_query', 'h1');
      final vars = registerVars(RegisterPayload(
          id: id, surql: 'SELECT 1', params: const {'a': 1}, ttl: '10m'));
      expect(vars['in'], id);
      expect((vars['config'] as Map)['ttl'], '10m');
      expect((vars['config'] as Map)['id'], id);
    });

    test('view rows and ids', () {
      expect(viewRecordId('k1'), '_00_view:k1');
      final row = viewRow([('t:1', 2)], true, 99);
      expect(row['ids'], [
        ['t:1', 2]
      ]);
      expect(row['confirmed'], isTrue);
      expect(row['updatedAt'], 99);
      expect(decodeViewIds(row['ids']), [('t:1', 2)]);
      expect(decodeViewIds('nope'), isEmpty);
    });

    test('heartbeat batch answers per index and detects reclaimed rows', () {
      final ids = [RecordId('_00_query', 'a'), RecordId('_00_query', 'b')];
      final batch = heartbeatBatch(ids);
      expect(batch.sql,
          'fn::query::heartbeat(\$id0);\nfn::query::heartbeat(\$id1)');
      expect(batch.vars['id0'], ids[0]);
      expect(batch.vars['id1'], ids[1]);
      expect(heartbeatRowGone(const []), isTrue);
      expect(heartbeatRowGone(const [1]), isFalse);
      expect(heartbeatRowGone(null), isFalse);
      expect(heartbeatBatch(const []).sql, '');
    });

    test('parseListRefRows and stmt', () {
      expect(
        parseListRefRows([
          {'out': 'thing:1', 'version': 2},
          {'out': 'thing:2'},
        ]),
        [('thing:1', 2), ('thing:2', 0)],
      );
      expect(parseListRefRows('nope'), isEmpty);
      const results = [StatementResult.ok(1)];
      expect(stmt(results, 0)?.result, 1);
      expect(stmt(results, 1), isNull);
    });
  });
}
