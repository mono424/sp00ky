import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

/// A client whose server cannot be reached must still boot, still paint from
/// the local store, and still shut down.
///
/// Both halves of that were broken: `WebSocketChannel.ready` has no timeout, so
/// a black-holed endpoint parked the revive loop inside one attempt forever,
/// and closing a socket that never finished opening hung the process. Found by
/// WhitePawn's `mobile_native_libs_test`, which points the engine at a
/// guaranteed-dead port on purpose.
void main() {
  InProcessSp00kyClient build(String endpoint) =>
      InProcessSp00kyClient(Sp00kyConfig(
        database:
            DatabaseConfig(endpoint: endpoint, namespace: 't', database: 't'),
        schema: {
          'thread': {
            'columns': {'title': const ColumnSchema(type: 'string')},
          },
        },
        schemaSurql: 'DEFINE TABLE thread PERMISSIONS FOR select WHERE true;',
        persistenceClient: MemoryPersistenceClient(),
        reconnect: const ReconnectConfig(connectTimeoutMs: 300),
      ));

  test('boot is local-only, so an unreachable server never blocks it',
      () async {
    final client = build('ws://127.0.0.1:1/rpc');
    final sw = Stopwatch()..start();
    await client.init().timeout(const Duration(seconds: 5));
    expect(sw.elapsedMilliseconds, lessThan(3000));
    expect(client.isLocalReady, isTrue);

    // The local half works with no server at all.
    await client.create('thread:a', {'title': 'offline'});
    expect(client.localStore.getById('thread:a'), isNotNull);
    expect(client.pendingMutationCount, 1,
        reason: 'the write waits in the outbox rather than being rolled back');

    await client.close().timeout(const Duration(seconds: 5));
  });

  test('a black-holed endpoint is given up on and retried, not parked in',
      () async {
    // 203.0.113.0/24 is TEST-NET-3: routable-looking and guaranteed dead, so
    // the socket hangs rather than being refused.
    final client = build('ws://203.0.113.1:9/rpc');
    await client.init().timeout(const Duration(seconds: 5));
    expect(client.isLocalReady, isTrue);
    // Long enough for several bounded attempts to come and go.
    await Future<void>.delayed(const Duration(seconds: 2));
    expect(client.syncHealth.connection, isNot(ConnectionState.connected));
    await client.close().timeout(const Duration(seconds: 5));
  });
}
