import 'dart:async';
import 'dart:io';

import 'package:spooky_core/src/services/database/connection_supervisor.dart';
import 'package:spooky_core/src/services/database/remote_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/surreal/cbor_codec.dart';
import 'package:spooky_core/src/surreal/remote_client.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

/// A real WebSocket server: the supervisor tests fake `isConnected`, which is
/// exactly what hid a closed socket reading as open.
class _Server {
  late HttpServer _http;
  final sockets = <WebSocket>[];
  int connections = 0;

  /// Sockets up to this number read everything and answer nothing: a peer that
  /// vanished without a FIN.
  int silentUpTo = 0;

  /// New sockets also stay silent, for a handshake that never answers.
  bool silentForever = false;

  Future<void> start() async {
    _http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    _http.listen((req) async {
      if (!WebSocketTransformer.isUpgradeRequest(req)) {
        req.response.statusCode = 400;
        await req.response.close();
        return;
      }
      final WebSocket ws;
      try {
        ws = await WebSocketTransformer.upgrade(req,
            protocolSelector: (_) => 'cbor');
      } catch (_) {
        return;
      }
      final mine = ++connections;
      sockets.add(ws);
      ws.listen((raw) {
        if (silentForever || mine <= silentUpTo) return;
        final msg = surrealCborDecode(raw as List<int>) as Map;
        // A frame can still arrive after closeSockets() started the close.
        if (!sockets.contains(ws)) return;
        ws.add(surrealCborEncode({
          'id': msg['id'],
          'result': [
            {'status': 'OK', 'result': true}
          ],
        }));
      }, onError: (_) {});
    });
  }

  String get url => 'ws://127.0.0.1:${_http.port}';

  Future<void> closeSockets() async {
    final closing = [...sockets];
    sockets.clear();
    for (final s in closing) {
      await s.close();
    }
  }

  Future<void> stop() => _http.close(force: true);
}

class _Rig {
  _Rig(this.server);
  final _Server server;
  final client = WebSocketSurrealClient();
  late final RemoteDatabaseService remote;
  late final ConnectionSupervisor supervisor;

  Future<void> start() async {
    final logger = SpookyLogger.root('recovery');
    remote = RemoteDatabaseService(
      DatabaseConfig(endpoint: server.url, namespace: 'm', database: 'm'),
      client,
      logger,
    )..armConnectGate();
    await remote.connect();
    supervisor = ConnectionSupervisor(
      reconnect: remote.connect,
      probe: remote.probe,
      wakeProbe: remote.probeSocket,
      forceClose: remote.forceClose,
      isConnected: () => remote.isConnected,
      onConnected: client.onConnected,
      onDisconnected: client.onDisconnected,
      logger: logger,
    )..start();
  }

  Future<void> stop() async {
    await supervisor.dispose();
    await client.close();
    await server.stop();
  }
}

Future<void> until(bool Function() ok, Duration within, String what) async {
  final clock = Stopwatch()..start();
  while (!ok()) {
    if (clock.elapsed > within) fail('$what not within $within');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void main() {
  late _Server server;
  late _Rig rig;

  setUp(() async {
    server = _Server();
    await server.start();
    rig = _Rig(server);
  });

  tearDown(() => rig.stop());

  test('a socket the peer closed is re-opened with no wake at all', () async {
    await rig.start();
    await server.closeSockets();
    await until(() => server.connections == 2, const Duration(seconds: 4),
        'a reconnect');
    await until(() => rig.supervisor.connection == ConnectionState.connected,
        const Duration(seconds: 1), 'connected');
    expect(await rig.remote.query('RETURN true'), isNotEmpty);
  });

  test('after a wake a half-open socket is replaced in seconds', () async {
    await rig.start();
    server.silentUpTo = server.connections;
    final clock = Stopwatch()..start();
    final stuck = expectLater(
        rig.remote.queryInteractive('RETURN true'), throwsA(isA<StateError>()));
    rig.supervisor.wake('resumed');
    await until(() => server.connections == 2, const Duration(seconds: 5),
        'a reconnect');
    await stuck;
    expect(clock.elapsed, lessThan(const Duration(seconds: 5)),
        reason: 'the heartbeat rules alone take 25 s');
    await until(() => rig.supervisor.connection == ConnectionState.connected,
        const Duration(seconds: 1), 'connected');
    expect(await rig.remote.queryInteractive('RETURN true'), isNotEmpty);
  });

  test('a healthy socket survives a wake', () async {
    await rig.start();
    rig.supervisor.wake('resumed');
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(server.connections, 1);
    expect(rig.supervisor.connection, ConnectionState.connected);
  });

  test('a handshake that never answers fails the connect, not the client',
      () async {
    await rig.start();
    await rig.supervisor.dispose();
    server.silentForever = true;
    rig.remote.handshakeTimeout = const Duration(milliseconds: 200);
    await expectLater(rig.remote.connect(), throwsA(isA<TimeoutException>()));
    expect(rig.remote.isConnected, isFalse);
    // The gate is open again, so a read fails now instead of waiting on it.
    await expectLater(rig.remote.query('RETURN true'), throwsA(isA<StateError>()))
        .timeout(const Duration(seconds: 1));
  });
}
