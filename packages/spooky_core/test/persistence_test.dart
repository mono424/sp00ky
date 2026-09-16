import 'dart:io';

import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/persistence/sqlite_persistence.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

void main() {
  final logger = SpookyLogger.root('test');

  group('SqlitePersistenceClient', () {
    late LocalDatabaseService db;
    late SqlitePersistenceClient persistence;

    setUp(() {
      db = LocalDatabaseService.open(logger);
      db.provision();
      persistence = SqlitePersistenceClient(() => db);
    });
    tearDown(() => db.close());

    test('set / get / remove round-trip', () async {
      expect(await persistence.get<String>('k'), isNull);
      await persistence.set('k', 'value');
      expect(await persistence.get<String>('k'), 'value');
      await persistence.remove('k');
      expect(await persistence.get<String>('k'), isNull);
    });

    test('preserves non-string values', () async {
      await persistence.set('n', 42);
      await persistence.set('m', {'a': 1});
      expect(await persistence.get<int>('n'), 42);
      expect(await persistence.get<Map<String, dynamic>>('m'), {'a': 1});
    });
  });

  group('persistence survives restart (file-backed)', () {
    late Directory tmp;
    late String dbPath;

    setUp(() {
      tmp = Directory.systemTemp.createTempSync('spooky_persist_');
      dbPath = '${tmp.path}/test.db';
    });
    tearDown(() => tmp.deleteSync(recursive: true));

    test('a value written then reopened is still present', () async {
      final db1 = LocalDatabaseService.open(logger,
          store: StoreType.indexeddb, path: dbPath);
      db1.provision();
      await SqlitePersistenceClient(() => db1)
          .set('sp00ky_auth_token', 'tok-123');
      db1.close();

      final db2 = LocalDatabaseService.open(logger,
          store: StoreType.indexeddb, path: dbPath);
      db2.provision();
      final restored = await SqlitePersistenceClient(() => db2)
          .get<String>('sp00ky_auth_token');
      db2.close();

      expect(restored, 'tok-123');
    });
  });
}
