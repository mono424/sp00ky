import 'dart:convert';
import 'dart:io';
import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:test/test.dart';
import '../auth_service_test.dart' show FakeAuthRemote;

class Accounts extends FakeAuthRemote {
  @override
  Future<dynamic> signin(Map<String, dynamic> params) async {
    final name = (params['variables'] as Map)['name'];
    authUser = {'id': 'user:$name'};
    return 'h.${base64Url.encode(utf8.encode(jsonEncode({
          'ID': 'user:$name',
          'AC': 'account'
        })))}.s';
  }

  @override
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) {
    if (sql.contains(r'$auth.id')) return super.query(sql, vars);
    throw const SocketException('Sync unavailable');
  }
}

void main() {
  test('awaited account changes rebind queries and preserve each outbox',
      () async {
    final dir = await Directory.systemTemp.createTemp('core-accounts-');
    final client = InProcessSp00kyClient(
        Sp00kyConfig(
          database: DatabaseConfig(
              endpoint: 'ws://unused',
              namespace: 'test',
              database: 'test',
              store: StoreType.indexeddb,
              localDbPath: '${dir.path}/cache.db'),
          schema: {
            'thread': {
              'columns': {'title': const ColumnSchema(type: 'string')}
            },
            'access': {
              'account': {
                'signIn': {
                  'params': {'name': {}}
                }
              }
            },
          },
          schemaSurql: 'DEFINE TABLE thread PERMISSIONS FULL;',
          reconnect: const ReconnectConfig(connectTimeoutMs: 50),
        ),
        remoteClient: Accounts());
    try {
      await client.init();
      await client.auth.signIn('account', {'name': 'alice'});
      final hash = await client.queryRaw('SELECT * FROM thread', {});
      final stream = client.subscribeStream(hash);
      final off = client.auth.subscribe((id) {
        expect(client.state.bucketId, id?.split(':').last ?? 'anon');
      });
      await client.create('thread:a', {'title': 'Alice'});
      await stream.firstWhere((r) => r.any((row) => row['title'] == 'Alice'));
      await client.auth.signIn('account', {'name': 'bob'});
      expect(client.localStore.getById('thread:a'), isNull);
      expect(client.state.queries[hash]!.records, isEmpty);
      await client.create('thread:b', {'title': 'Bob'});
      await client.auth.signIn('account', {'name': 'alice'});
      expect(client.localStore.getById('thread:a')?['title'], 'Alice');
      expect(client.localStore.getById('thread:b'), isNull);
      expect(client.pendingMutationCount, 1);
      expect(client.localStore.getSnapshot(), isNotNull);
      expect(() => client.auth.currentUser!['id'] = 'user:wrong',
          throwsUnsupportedError);
      off();
      await client.auth.signOut();
      expect(client.state.bucketId, 'anon');
    } finally {
      await client.close();
      await dir.delete(recursive: true);
    }
  });
}
