import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/query/materialize.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:test/test.dart';

void main() {
  group('materializeEffect', () {
    test('resolves the render set by id against the query table', () {
      final effect =
          materializeEffect('thing', ['thing:1', 'thing:2']) as LocalGetMany;
      expect(effect.table, 'thing');
      expect(effect.ids, ['thing:1', 'thing:2']);
      expect(effect.kind, 'local.getMany');
    });

    test('tableOfIds reads the table off the first id, else the fallback', () {
      expect(tableOfIds(const ['thing:1'], 'other'), 'thing');
      expect(tableOfIds(const [], 'other'), 'other');
    });
  });

  group('isWindowed / applyWindowOrder', () {
    test('only an offset query is a window', () {
      expect(isWindowed('SELECT * FROM thing'), isFalse);
      expect(isWindowed('SELECT * FROM thing LIMIT 10'), isFalse);
      expect(isWindowed('SELECT * FROM thing LIMIT 10 START 0'), isFalse);
      expect(isWindowed('SELECT * FROM thing LIMIT 10 START 10'), isTrue);
    });

    test('re-applies a window ORDER BY to rows resolved by id', () {
      final rows = [
        {'id': 'thing:1', 'n': 3},
        {'id': 'thing:2', 'n': 1},
        {'id': 'thing:3', 'n': 2},
      ];
      final sorted = applyWindowOrder(
          'SELECT * FROM thing ORDER BY n ASC LIMIT 10 START 10', rows);
      expect(sorted.map((r) => r['n']), [1, 2, 3]);
      // No window, or no ORDER BY: the circuit's order is kept.
      expect(identical(applyWindowOrder('SELECT * FROM thing', rows), rows),
          isTrue);
      expect(
          identical(
              applyWindowOrder('SELECT * FROM thing LIMIT 10 START 10', rows),
              rows),
          isTrue);
    });
  });

  group('rowsEqual', () {
    test('compares structurally and survives wire types', () {
      expect(rowsEqual(const [], const []), isTrue);
      expect(
          rowsEqual([
            {'a': 1}
          ], [
            {'a': 1}
          ]),
          isTrue);
      expect(
          rowsEqual([
            {'a': 1}
          ], [
            {'a': 2}
          ]),
          isFalse);
      expect(
          rowsEqual([
            {'a': 1}
          ], const []),
          isFalse);
      final rows = [
        {'r': RecordId('user', 'u1'), 'd': DateTime.utc(2026)}
      ];
      expect(rowsEqual(rows, rows), isTrue);
      expect(
          rowsEqual(rows, [
            {'r': RecordId('user', 'u1'), 'd': DateTime.utc(2026)}
          ]),
          isTrue);
    });
  });
}
