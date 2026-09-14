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

/// A returning account must be visible to the host BEFORE the local boot has
/// finished: the identity comes from the cached token alone, the rows follow.
void main() {
  late String base;
  setUp(() async {
    final dir = Directory.systemTemp.createTempSync('spooky-early-session');
    addTearDown(() => dir.deleteSync(recursive: true));
    base = '${dir.path}/store.db';
    final logger = SpookyLogger.root('test');
    final session = LocalDatabaseService.open(logger,
        store: StoreType.indexeddb,
        path: SessionPersistence.bucketPath(base, 'session'));
    session.provision();
    final token =
        'h.${base64Url.encode(utf8.encode('{"ID":"user:cached","AC":"account"}'))}.s';
    session.kvSet(
        'session_v1', jsonEncode({'token': token, 'bucket': 'cached'}));
    session.close();
    final cached = LocalDatabaseService.open(logger,
        store: StoreType.indexeddb,
        path: SessionPersistence.bucketPath(base, 'cached'));
    cached.provision();
    await LocalMigrator(cached, logger).provision(fixture().schemaSurql);
    cached.putDoc('thread', 'thread:cached', {'title': 'c', '_00_rv': 1});
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
  });

  Future<void> check(Sp00kyClient c) async {
    final seen = <(String?, bool)>[];
    if (c is InProcessSp00kyClient) {
      // No auth surface before init in-process; the hook is the signal.
      c.onSessionRestored = () =>
          seen.add((c.auth.currentUser?['id']?.toString(), c.isLocalReady));
    } else {
      c.auth.subscribe((uid) => seen.add((uid, c.isLocalReady)));
      expect(seen, [(null, false)]);
    }
    await c.init().timeout(const Duration(seconds: 5));
    expect(c.isLocalReady, true);
    final first = seen.firstWhere((s) => s.$1 == 'user:cached');
    expect(first.$2, false,
        reason: 'the restored id must arrive before local boot completes');
    expect(c.auth.currentUser?['id'], 'user:cached');
    expect(c.auth.isAuthenticated, true);
    expect(c.auth.isLoading, false);
    final rows = await c.queryStream('SELECT * FROM thread', {});
    expect(
        (await rows
                .firstWhere((r) => r.isNotEmpty)
                .timeout(const Duration(seconds: 2)))
            .first['id']
            .toString(),
        'thread:cached');
    await c.close();
  }

  test('worker client publishes the restored session before the prime',
      () async {
    await check(
        Sp00kyClient(fixture(path: base, endpoint: 'ws://127.0.0.1:1/rpc')));
  });

  test('in-process client publishes the restored session before the prime',
      () async {
    await check(InProcessSp00kyClient(
        fixture(path: base, endpoint: 'ws://127.0.0.1:1/rpc')));
  });
}
