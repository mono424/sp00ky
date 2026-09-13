@Tags(['integration'])
library;

import 'dart:async';
import 'dart:io';
import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:test/test.dart';

/// Uses the existing main/main account + SSP development stack, without an
/// injected transport. SURREAL_DEV_WS must refer to a disposable test stack.
void main() {
  final endpoint = Platform.environment['SURREAL_DEV_WS'];
  for (final background in [false, true]) {
    test(
        '${background ? "background" : "in-process"}: cache, live updates, offline drain and failed tray',
        () async {
      if (endpoint == null) {
        markTestSkipped(
            'Set SURREAL_DEV_WS to a disposable account + SSP stack');
        return;
      }
      final dir = await Directory.systemTemp.createTemp('core-real-cache-');
      final root = WebSocketSurrealClient();
      await root.connect(endpoint);
      await root.signin({'user': 'root', 'pass': 'root'});
      await root.use(namespace: 'main', database: 'main');
      final unique = DateTime.now().microsecondsSinceEpoch;
      final email = 'cache_$unique@e2e.test';
      final created = await root.query(
        r'CREATE ONLY user SET email = $email, username = $username, password = crypto::argon2::generate($pw)',
        {'email': email, 'username': 'cache_$unique', 'pw': 'local-password'},
      );
      final id = (created.first as Map)['id'].toString();
      Sp00kyConfig config(String remote) => Sp00kyConfig(
            database: DatabaseConfig(
                endpoint: remote,
                namespace: 'main',
                database: 'main',
                store: StoreType.indexeddb,
                localDbPath: '${dir.path}/cache.db'),
            schema: {
              'user': {
                'columns': {
                  'email': const ColumnSchema(type: 'string'),
                  'username': const ColumnSchema(type: 'string'),
                  'password': const ColumnSchema(type: 'string'),
                }
              },
              'access': {
                'account': {
                  'signIn': {
                    'params': {'email': {}, 'password': {}}
                  }
                }
              },
            },
            schemaSurql: 'DEFINE TABLE user PERMISSIONS FOR select WHERE true;',
            reconnect: const ReconnectConfig(connectTimeoutMs: 200),
          );
      Sp00kyClient build(String remote) => background
          ? Sp00kyClient(config(remote))
          : InProcessSp00kyClient(config(remote));
      var client = build(endpoint);
      try {
        await client.init();
        await client.auth
            .signIn('account', {'email': email, 'password': 'local-password'});
        expect(client.auth.currentUser?['id'].toString(), id);
        final sql = r'SELECT * FROM user WHERE id = $id';
        final params = {'id': RecordId.parse(id)};
        var stream = await client.queryStream(sql, params);
        expect(
            (await stream
                    .firstWhere((rows) => rows.isNotEmpty)
                    .timeout(const Duration(seconds: 10)))
                .single['email'],
            email);
        final live = stream.firstWhere(
            (rows) => rows.any((r) => r['username'] == 'server_$unique'));
        await root.query(r'UPDATE $id SET username = $name',
            {'id': RecordId.parse(id), 'name': 'server_$unique'});
        await live.timeout(const Duration(seconds: 10));
        await client.checkpoint();
        await client.close();

        client = build('ws://127.0.0.1:1/rpc');
        await client.init().timeout(const Duration(seconds: 2));
        expect(client.auth.isAuthenticated, true);
        stream = await client.queryStream(sql, params);
        expect(
            (await stream.firstWhere((r) => r.isNotEmpty)).single['username'],
            'server_$unique');
        await client.update('user', id, {'username': 'offline_$unique'});
        expect(client.pendingMutationCount, greaterThan(0));
        await client.close();

        client = build(endpoint);
        await client.init();
        await until(() async => client.pendingMutationCount == 0);
        final result = await root.query(r'SELECT * FROM ONLY $id', params);
        expect((result.first as Map)['username'], 'offline_$unique');
        // An empty password violates the backend assertion. It must remain
        // inspectable and recoverable rather than disappearing from the outbox.
        await client.update('user', id, {'password': ''});
        await until(() async => client.failedMutationCount == 1);
        final failed = await client.listFailedMutations();
        expect(failed, hasLength(1));
        expect(await client.retryFailedMutation(failed.single.id), true);
        await until(() async => client.failedMutationCount == 1);
        final rejectedAgain = await client.listFailedMutations();
        expect(
            await client.discardFailedMutation(rejectedAgain.single.id), true);
        expect(client.failedMutationCount, 0);
      } finally {
        await client.close();
        try {
          await root.query(r'DELETE $id', {'id': RecordId.parse(id)});
        } catch (_) {}
        await root.close();
        await dir.delete(recursive: true);
      }
    }, timeout: const Timeout(Duration(seconds: 45)));
  }
}

Future<void> until(Future<bool> Function() predicate) async {
  final end = DateTime.now().add(const Duration(seconds: 10));
  while (!await predicate()) {
    if (DateTime.now().isAfter(end)) fail('Sync condition did not settle');
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
}
