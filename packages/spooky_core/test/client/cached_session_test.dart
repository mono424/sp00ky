import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:crypto/crypto.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/database/local_migrator.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/persistence/session_persistence.dart';
import 'package:test/test.dart';
import 'worker_parity_test.dart' show fixture;

void main() {
  test(
      'legacy signed-in cache is immediately readable without a server across restarts',
      () async {
    final dir = Directory.systemTemp.createTempSync('spooky-signed-cache');
    addTearDown(() => dir.deleteSync(recursive: true));
    final base = '${dir.path}/store.db';
    final logger = SpookyLogger.root('test');
    LocalDatabaseService store(String bucket) {
      final db = LocalDatabaseService.open(logger,
          store: StoreType.indexeddb,
          path: SessionPersistence.bucketPath(base, bucket));
      db.provision();
      return db;
    }

    final anon = store('anon');
    anon.kvSet('sp00ky_boot_bucket', jsonEncode('cached'));
    anon.close();
    final cached = store('cached');
    await LocalMigrator(cached, logger).provision(fixture().schemaSurql);
    final token =
        'h.${base64Url.encode(utf8.encode('{"ID":"user:cached","AC":"account"}'))}.s';
    cached.kvSet('sp00ky_auth_token', jsonEncode(token));
    cached.putDoc(
        'thread', 'thread:cached', {'title': 'from cache', '_00_rv': 1});
    final viewKey = sha256
        .convert(utf8.encode(
            jsonEncode({'surql': 'SELECT * FROM thread', 'params': {}})))
        .toString();
    cached.putDoc('_00_view', '_00_view:$viewKey', {
      'ids': [
        ['thread:cached', 1]
      ],
      'confirmed': true
    });
    cached.close();
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final sockets = <WebSocket>[];
    server.listen((request) async {
      final socket = await WebSocketTransformer.upgrade(request);
      sockets.add(socket);
      socket.listen((_) {}); // Keep connection and verification unresolved.
    });
    addTearDown(() async {
      for (final socket in sockets) {
        await socket.close();
      }
      await server.close(force: true);
    });
    for (var i = 0; i < 2; i++) {
      final c = Sp00kyClient(
          fixture(path: base, endpoint: 'ws://127.0.0.1:${server.port}/rpc'));
      await c.init().timeout(const Duration(seconds: 2));
      expect(c.auth.currentUser?['id'], 'user:cached');
      expect((await c.inspectState()).bucketId, 'cached');
      final rows = await c.queryStream('SELECT * FROM thread', {});
      expect(
          (await rows
                  .firstWhere((r) => r.isNotEmpty)
                  .timeout(const Duration(seconds: 2)))
              .firstWhere(
                  (r) => r['id'].toString() == 'thread:cached')['title'],
          'from cache');
      await Future<void>.delayed(const Duration(milliseconds: 250));
      expect(c.auth.isAuthenticated, true);
      if (i == 0) await c.create('thread:pending', {'title': 'queued'});
      expect(c.pendingMutationCount, 1);
      await c.checkpoint();
      await c.close();
    }
  });
  test('checkpoint and concurrent local writes preserve rows', () async {
    final c = InProcessSp00kyClient(fixture());
    addTearDown(c.close);
    await c.init();
    // Account-store fencing is exercised directly in runtime tests; this also
    // checks that the native store survives parallel local work and inspection.
    await Future.wait([
      c.create('thread:a', {'title': 'a'}),
      c.checkpoint()
    ]);
    expect(c.localStore.getById('thread:a'), isNotNull);
  });
}
