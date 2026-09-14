import 'dart:io';

import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/database/local_migrator.dart';
import 'package:test/test.dart';

void main() {
  for (final worker in [false, true]) {
    test(
        'new profile queries read cached rows (${worker ? 'worker' : 'in-process'})',
        () async {
      final dir = Directory.systemTemp.createTempSync('cold-cached-query');
      addTearDown(() => dir.deleteSync(recursive: true));
      final path = '${dir.path}/cache.db';
      final store = LocalDatabaseService.open(SpookyLogger.root('fixture'),
          store: StoreType.indexeddb, path: '${dir.path}/cache.anon.db');
      store.provision();
      await LocalMigrator(store, SpookyLogger.root('fixture'))
          .provision('DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;'
              'DEFINE FIELD username ON user TYPE string;');
      store.putDoc('user', 'user:alice', {
        'id': 'user:alice',
        'username': 'alice',
        'mentor': 'user:bob',
        '_00_rv': 1,
      });
      // A different query originally synced this row. Neither the search nor
      // the profile has a membership snapshot, and there are no pending writes.
      store.putDoc('_00_view', '_00_view:earlier', {
        'ids': [
          ['user:alice', 1]
        ],
        'confirmed': true,
      });
      store.close();
      final config = Sp00kyConfig(
        database: DatabaseConfig(
            namespace: 'test',
            database: 'test',
            store: StoreType.indexeddb,
            localDbPath: path,
            endpoint: 'ws://127.0.0.1:1/rpc'),
        schema: {
          'user': {
            'columns': {'username': const ColumnSchema(type: 'string')}
          }
        },
        schemaSurql: 'DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;'
            'DEFINE FIELD username ON user TYPE string;',
      );
      final Sp00kyClient client =
          worker ? Sp00kyClient(config) : InProcessSp00kyClient(config);
      addTearDown(client.close);
      await client.init();
      expect(client.pendingMutationCount, 0);
      for (final sql in [
        'SELECT * FROM user WHERE username = \$name LIMIT 1',
      ]) {
        final hash = await client.queryRaw(sql, {
          'name': 'alice'
        }, relations: [
          RelationPlan(
              alias: 'mentor',
              table: 'user',
              cardinality: 'one',
              foreignKeyField: 'mentor'),
        ]);
        final rows = await client
            .subscribeStream(hash)
            .firstWhere((r) => r.isNotEmpty)
            .timeout(const Duration(seconds: 2));
        expect(rows.single['username'], 'alice');
        expect(client.isQueryAuthoritative(hash), false);
        expect(rows.single['mentor'], isNull);
        final enriched = client
            .subscribeStream(hash)
            .firstWhere((r) => r.isNotEmpty && r.single['mentor'] is Map);
        await client.create('user:bob', {'username': 'bob'});
        expect(
            (await enriched.timeout(const Duration(seconds: 2)))
                .single['mentor']['username'],
            'bob');
      }
    });
  }
}
