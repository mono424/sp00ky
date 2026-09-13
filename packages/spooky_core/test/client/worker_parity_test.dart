import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'package:spooky_core/src/client/worker_client.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/advanced.dart';
import 'package:test/test.dart';

Sp00kyConfig fixture({String? path, String? endpoint}) => Sp00kyConfig(
      database: DatabaseConfig(
          namespace: 'test',
          database: 'test',
          endpoint: endpoint,
          store: path == null ? StoreType.memory : StoreType.indexeddb,
          localDbPath: path),
      schema: {
        'thread': {
          'columns': {'title': const ColumnSchema(type: 'string')}
        }
      },
      schemaSurql: 'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FULL;'
          'DEFINE FIELD title ON thread TYPE string;',
      reconnect: const ReconnectConfig(connectTimeoutMs: 200),
    );
void main() {
  for (final worker in [false, true]) {
    group(worker ? 'background default' : 'in-process', () {
      Sp00kyClient build(Sp00kyConfig config) =>
          worker ? Sp00kyClient(config) : InProcessSp00kyClient(config);
      test('queries, mutations, subscriptions, counters and restart agree',
          () async {
        final dir = Directory.systemTemp.createTempSync('spooky-parity');
        addTearDown(() => dir.deleteSync(recursive: true));
        final config = fixture(path: '${dir.path}/store.db');
        var c = build(config);
        await Future.wait([c.init(), c.init()]);
        expect(c.isLocalReady, true);
        final pending = <int>[];
        final off = c.subscribeToPendingMutations(pending.add);
        await c.create('thread:a', {'title': 'first'});
        expect(c.pendingMutationCount, 1);
        expect(c.unsyncedRecordIds, contains('thread:a'));
        expect(pending, contains(1));
        final hash = await c.queryRaw('SELECT * FROM thread', {});
        final stream = c.subscribeStream(hash);
        expect((await stream.firstWhere((r) => r.isNotEmpty)).single['title'],
            'first');
        await c.update('thread', 'thread:a', {'title': 'second'});
        expect(
            (await stream.firstWhere(
                    (r) => r.any((row) => row['title'] == 'second')))
                .single['title'],
            'second');
        expect(await c.queryStatusStream(hash).first, isA<QueryStatus>());
        expect(await c.queryAuthorityStream(hash).first, isA<bool>());
        expect(await c.listFailedMutations(), isEmpty);
        expect(c.failedMutationCount, 0);
        expect((await c.inspectState()).queries, contains(hash));
        c.reportFrontendTiming(hash, 2);
        await c.checkpoint();
        expect(c.queryTimings(hash), isNotNull);
        off();
        await c.close();
        c = build(config);
        addTearDown(c.close);
        await c.init();
        final restored = await c.queryStream('SELECT * FROM thread', {});
        expect((await restored.firstWhere((r) => r.isNotEmpty)).single['title'],
            'second');
        final empty = restored.firstWhere((r) => r.isEmpty);
        await c.delete('thread', 'thread:a');
        expect(await empty, isEmpty);
      });
      test('cold remote request cannot block cached reads', () async {
        final c = build(fixture(endpoint: 'ws://127.0.0.1:1/rpc'));
        addTearDown(c.close);
        await c.init();
        final remote =
            c.queryRemote('RETURN true').catchError((_) => <dynamic>[]);
        await c.create('thread:local', {'title': 'cached'});
        final stream = await c.queryStream('SELECT * FROM thread', {});
        expect(
            await stream
                .firstWhere((r) => r.isNotEmpty)
                .timeout(const Duration(seconds: 2)),
            hasLength(1));
        await remote;
      });
    });
  }
  test('worker closes before init and rejects subsequent calls', () async {
    final c = Sp00kyClient(fixture());
    await c.close();
    await expectLater(c.init(), throwsStateError);
  });
  test('worker startup failure is surfaced and cleanup completes', () async {
    final dir = Directory.systemTemp.createTempSync('spooky-failure');
    addTearDown(() => dir.deleteSync(recursive: true));
    final c = Sp00kyClient(fixture(path: '${dir.path}/missing/store.db'));
    await expectLater(c.init(), throwsA(isA<WorkerFailure>()));
    await c.close();
  });
  test('unexpected worker exit fails requests and terminates streams',
      () async {
    final c = _ExitingWorker(fixture());
    await c.init();
    final ended = Completer<void>();
    final errors = <Object>[];
    c
        .subscribeStream('query')
        .listen((_) {}, onError: errors.add, onDone: ended.complete);
    await expectLater(c.inspectState(), throwsStateError);
    await ended.future.timeout(const Duration(seconds: 2));
    expect(errors, isNotEmpty);
    await expectLater(
        c.create('thread:a', {'title': 'after exit'}), throwsStateError);
    await c.close();
  });
  test('close cancels a pending network request without waiting for the server',
      () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final sockets = <WebSocket>[];
    server.listen((request) async {
      final socket = await WebSocketTransformer.upgrade(request);
      sockets.add(socket);
      socket.listen((_) {}); // Deliberately never answer the RPC handshake.
    });
    final c =
        Sp00kyClient(fixture(endpoint: 'ws://127.0.0.1:${server.port}/rpc'));
    await c.init();
    final pending =
        expectLater(c.queryRemote('RETURN true'), throwsA(anything));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    await c.close().timeout(const Duration(seconds: 3));
    await pending;
    for (final socket in sockets) {
      await socket.close();
    }
    await server.close(force: true);
  });
}

class _ExitingWorker extends WorkerSp00kyClient {
  _ExitingWorker(super.config);
  @override
  Future<Isolate> spawnWorker(SendPort output) =>
      Isolate.spawn(_exitAfterRequest, output, onExit: output, onError: output);
}

void _exitAfterRequest(SendPort output) {
  final port = ReceivePort();
  output.send(port.sendPort);
  port.listen((_) => Isolate.exit());
}
