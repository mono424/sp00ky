import 'dart:io';

import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/kernel/events.dart' show GcTick;
import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/database/local_migrator.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:test/test.dart';

const _schemaSurql = 'DEFINE TABLE user SCHEMAFULL PERMISSIONS FULL;'
    'DEFINE FIELD username ON user TYPE string;'
    'DEFINE TABLE conversation SCHEMAFULL PERMISSIONS FULL;'
    'DEFINE FIELD user_a ON conversation TYPE record<user>;';

/// A restart must not cost a list its joined rows: the chat list's peer names
/// live in `.related()` children, which no view names as members.
void main() {
  test('related children survive a restart and the orphan sweep', () async {
    final dir = Directory.systemTemp.createTempSync('gc-children');
    addTearDown(() => dir.deleteSync(recursive: true));
    final store = LocalDatabaseService.open(SpookyLogger.root('fixture'),
        store: StoreType.indexeddb, path: '${dir.path}/cache.anon.db');
    store.provision();
    await LocalMigrator(store, SpookyLogger.root('fixture'))
        .provision(_schemaSurql);
    store.putDoc('conversation', 'conversation:c1',
        {'id': 'conversation:c1', 'user_a': 'user:alice', '_00_rv': 1});
    store.putDoc('user', 'user:alice',
        {'id': 'user:alice', 'username': 'alice', '_00_rv': 1});
    store.putDoc(
        'user', 'user:zed', {'id': 'user:zed', 'username': 'zed', '_00_rv': 1});
    // What the list query's last session left: its members and its children.
    store.putDoc('_00_view', '_00_view:list', {
      'ids': [
        ['conversation:c1', 1]
      ],
      'children': [
        ['user:alice', 1]
      ],
      'confirmed': true,
      'updatedAt': DateTime.now().millisecondsSinceEpoch,
    });
    store.close();

    final client = InProcessSp00kyClient(Sp00kyConfig(
      database: DatabaseConfig(
          namespace: 'test',
          database: 'test',
          store: StoreType.indexeddb,
          localDbPath: '${dir.path}/cache.db',
          endpoint: 'ws://127.0.0.1:1/rpc'),
      schema: {
        'user': {
          'columns': {'username': const ColumnSchema(type: 'string')}
        },
        'conversation': {
          'columns': {
            'user_a': const ColumnSchema(recordId: true, type: 'record<user>')
          }
        },
      },
      schemaSurql: _schemaSurql,
    ));
    addTearDown(client.close);
    await client.init();

    final hash = await client.queryRaw('SELECT * FROM conversation', {},
        relations: [
          RelationPlan(
              alias: 'user_a',
              table: 'user',
              cardinality: 'one',
              foreignKeyField: 'user_a'),
        ]);
    final rows = await client
        .subscribeStream(hash)
        .firstWhere((r) => r.isNotEmpty)
        .timeout(const Duration(seconds: 2));
    expect(rows.single['user_a'], isA<Map>(),
        reason: 'the joined row paints from cache in the first frame');
    expect((rows.single['user_a'] as Map)['username'], 'alice');

    await client.dispatch(const GcTick());
    expect(client.state.versions.containsKey('user:alice'), isTrue);
    expect(client.state.versions.containsKey('conversation:c1'), isTrue);
    expect(client.state.versions.containsKey('user:zed'), isFalse,
        reason: 'a body nothing vouches for is still collected');
  });
}
