import 'package:spooky_core/src/query/window_query.dart';
import 'package:spooky_core/src/utils/sort_rows.dart';
import 'package:test/test.dart';

/// Port of the TS `window-query` spec. The Dart materialization returns the
/// parsed `ORDER BY` instead of a rewritten query string (sqlite can't run
/// SurrealQL), so the assertions check detection plus the extracted order.
void main() {
  group('buildWindowMaterialization', () {
    test('detects an offset window and keeps its ORDER BY', () {
      final r = buildWindowMaterialization(
          'SELECT * FROM game WHERE database = \$database ORDER BY sort_index asc, date desc LIMIT 30 START 30;');
      expect(r, isNotNull);
      expect(r!.orderBy, [('sort_index', 'asc'), ('date', 'desc')]);
    });

    test('returns null for START 0 (an offset-free window is not windowed)',
        () {
      expect(
        buildWindowMaterialization(
            'SELECT * FROM game WHERE database = \$database ORDER BY date desc LIMIT 30 START 0;'),
        isNull,
      );
    });

    test('returns null when there is no START clause', () {
      expect(
        buildWindowMaterialization(
            'SELECT * FROM game WHERE database = \$database LIMIT 30;'),
        isNull,
      );
      expect(buildWindowMaterialization('SELECT * FROM game;'), isNull);
    });

    test('returns null without a FROM clause', () {
      expect(buildWindowMaterialization('RETURN 1 START 5'), isNull);
    });

    test('an empty ORDER BY keeps the id-set order', () {
      final r =
          buildWindowMaterialization('SELECT * FROM game LIMIT 30 START 30;');
      expect(r, isNotNull);
      expect(r!.orderBy, isEmpty);
    });

    test('defaults a bare ORDER BY field to ascending', () {
      final r = buildWindowMaterialization(
          'SELECT * FROM game ORDER BY date LIMIT 10 START 10');
      expect(r!.orderBy, [('date', 'asc')]);
    });

    test('ignores clauses inside subqueries (paren-aware)', () {
      final r = buildWindowMaterialization(
          'SELECT *, (SELECT * FROM comment WHERE game = \$parent.id ORDER BY created_at desc LIMIT 5) AS comments '
          'FROM game ORDER BY sort_index asc LIMIT 30 START 90;');
      expect(r, isNotNull);
      expect(r!.orderBy, [('sort_index', 'asc')],
          reason: 'the subquery ORDER BY must not leak into the window order');
    });

    test('does not treat a START inside a string literal as the offset', () {
      final r = buildWindowMaterialization(
          "SELECT * FROM game WHERE note = 'LIMIT 30 START 30' ORDER BY date desc LIMIT 30 START 30;");
      expect(r, isNotNull);
      expect(r!.orderBy, [('date', 'desc')]);
    });

    test('a START only inside a string literal is not a window', () {
      expect(
        buildWindowMaterialization(
            "SELECT * FROM game WHERE note = 'START 30' LIMIT 30;"),
        isNull,
      );
    });

    test('tolerates a trailing semicolon after ORDER BY', () {
      final r = buildWindowMaterialization(
          'SELECT * FROM game START 5 ORDER BY date desc;');
      expect(r!.orderBy, [('date', 'desc')]);
    });

    test('case-insensitive keywords', () {
      final r = buildWindowMaterialization(
          'select * from game order by date DESC limit 10 start 20');
      expect(r!.orderBy, [('date', 'desc')]);
    });
  });

  group('sortRows', () {
    Map<String, dynamic> row(String id, Object? sort, [Object? date]) =>
        {'id': id, 'sort': sort, 'date': date};

    test('sorts ascending by default and descending on request', () {
      final rows = [row('c', 3), row('a', 1), row('b', 2)];
      expect(sortRows(rows, [('sort', 'asc')]).map((r) => r['id']),
          ['a', 'b', 'c']);
      expect(sortRows(rows, [('sort', 'desc')]).map((r) => r['id']),
          ['c', 'b', 'a']);
    });

    test('applies multiple keys in order', () {
      final rows = [
        row('a', 1, 'z'),
        row('b', 1, 'y'),
        row('c', 0, 'x'),
      ];
      expect(
        sortRows(rows, [('sort', 'asc'), ('date', 'asc')]).map((r) => r['id']),
        ['c', 'b', 'a'],
      );
    });

    test('nulls sort last ascending', () {
      final rows = [row('a', null), row('b', 1)];
      expect(sortRows(rows, [('sort', 'asc')]).map((r) => r['id']), ['b', 'a']);
    });

    test('equal keys keep input order (stable)', () {
      final rows = [row('a', 1), row('b', 1), row('c', 1)];
      expect(sortRows(rows, [('sort', 'asc')]).map((r) => r['id']),
          ['a', 'b', 'c']);
    });

    test('an empty ORDER BY leaves the rows untouched', () {
      final rows = [row('c', 3), row('a', 1)];
      expect(sortRows(rows, const []).map((r) => r['id']), ['c', 'a']);
    });

    test('compares numbers numerically, not lexically', () {
      final rows = [row('a', 10), row('b', 9)];
      expect(sortRows(rows, [('sort', 'asc')]).map((r) => r['id']), ['b', 'a']);
    });
  });
}
