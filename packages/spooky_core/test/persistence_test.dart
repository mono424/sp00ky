import 'dart:io';

import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/persistence/sqlite_persistence.dart';
import 'package:spooky_core/src/services/stream_processor/stream_processor_service.dart';
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
      await SqlitePersistenceClient(() => db1).set('sp00ky_auth_token', 'tok-123');
      db1.close();

      final db2 = LocalDatabaseService.open(logger,
          store: StoreType.indexeddb, path: dbPath);
      db2.provision();
      final restored =
          await SqlitePersistenceClient(() => db2).get<String>('sp00ky_auth_token');
      db2.close();

      expect(restored, 'tok-123');
    });

    test('stream-processor state restores across a restart', () async {
      // Session 1: register a view, ingest a row, persist state to sqlite.
      final db1 = LocalDatabaseService.open(logger,
          store: StoreType.indexeddb, path: dbPath);
      db1.provision();
      final sp1 = StreamProcessorService(SqlitePersistenceClient(() => db1), logger);
      await sp1.init();
      sp1.seedPermissionsFromSchema(
          'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;');
      sp1.registerQueryPlan(QueryPlanConfig(
        queryHash: 'q1',
        surql: 'SELECT * FROM thread',
        params: {},
        ttl: '10m',
        lastActiveAt: DateTime.utc(2026),
      ));
      sp1.ingest(
          'thread', 'CREATE', 'thread:a', {'id': 'thread:a', 'title': 't'});
      await sp1.saveState();
      await sp1.close();
      db1.close();

      // Session 2: a fresh processor must restore the registered view from
      // persisted state, so a matching ingest still produces an update for q1.
      final db2 = LocalDatabaseService.open(logger,
          store: StoreType.indexeddb, path: dbPath);
      db2.provision();
      final sp2 = StreamProcessorService(SqlitePersistenceClient(() => db2), logger);
      await sp2.init(); // loadState() from sqlite

      final updates = sp2.ingest(
          'thread', 'CREATE', 'thread:b', {'id': 'thread:b', 'title': 'u'});
      await sp2.close();
      db2.close();

      expect(updates.any((u) => u.queryHash == 'q1'), isTrue,
          reason: 'view q1 should have been restored from persisted state');
    });
  });
}
