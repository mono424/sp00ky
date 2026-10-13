import 'package:spooky_core/src/services/database/column_reset.dart';
import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/utils/parser.dart' show ColumnSchema;
import 'package:test/test.dart';

// A body landed under an older schema lacks the columns that schema did not
// know, at the server's version. Without a reset the next build never fetches
// it again (WhitePawn's puzzles stuck on "Preparing" for want of `layout`).
void main() {
  final logger = SpookyLogger.root('test');
  late LocalDatabaseService db;

  const oldSets = {'puzzle': 'title', 'game': 'white'};
  const newSets = {'puzzle': 'layout,title', 'game': 'white'};

  Map<String, int> versions(String table) => {
        for (final (id, rv) in db.scanVersions([table])[table]!) id: rv
      };

  setUp(() {
    db = LocalDatabaseService.open(logger);
    db.provision();
    // Landed by the old build: no `layout`, at the server's version 6.
    db.create('puzzle:a', {'title': 'A', '_00_rv': 6});
    db.create('puzzle:b', {'title': 'B', '_00_rv': 6});
    db.create('game:1', {'white': 'x', '_00_rv': 3});
  });
  tearDown(() => db.close());

  test('fingerprints are the sorted declared columns', () {
    final schema = <String, dynamic>{
      'puzzle': {
        'columns': {
          'title': const ColumnSchema(),
          'layout': const ColumnSchema(optional: true),
        },
      },
    };
    expect(columnFingerprints(schema, ['puzzle', 'missing']),
        {'puzzle': 'layout,title'});
  });

  test('first boot with no recorded sets resets every cached table once', () {
    final reset = db.resetChangedTables(newSets, {});
    expect(reset..sort(), ['game', 'puzzle']);
    expect(versions('puzzle'), {'puzzle:a': 0, 'puzzle:b': 0});
    expect(versions('game'), {'game:1': 0});
    // The body stays, and its own `_00_rv` agrees with the column.
    expect(db.getById('puzzle:a'), containsPair('title', 'A'));
    expect(db.getById('puzzle:a'), containsPair('_00_rv', 0));

    // The next boot of the same build changes nothing.
    db.create('puzzle:a', {'title': 'A', 'layout': 'mirror', '_00_rv': 6});
    expect(db.resetChangedTables(newSets, {}), isEmpty);
    expect(versions('puzzle')['puzzle:a'], 6);
  });

  test('a new column resets only that table', () {
    db.resetChangedTables(oldSets, {});
    db.create('puzzle:a', {'title': 'A', '_00_rv': 6});
    db.create('puzzle:b', {'title': 'B', '_00_rv': 6});
    db.create('game:1', {'white': 'x', '_00_rv': 3});

    expect(db.resetChangedTables(newSets, {}), ['puzzle']);
    expect(versions('puzzle'), {'puzzle:a': 0, 'puzzle:b': 0});
    expect(versions('game'), {'game:1': 3});
  });

  test('keeps the version of a row with an unsettled local write', () {
    db.resetChangedTables(newSets, {'puzzle:b', 'game:9'});
    expect(versions('puzzle'), {'puzzle:a': 0, 'puzzle:b': 6});
  });
}
