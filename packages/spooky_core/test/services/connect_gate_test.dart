import 'dart:async';

import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/kernel/effects.dart' show StatementResult;
import 'package:spooky_core/src/services/database/remote_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/surreal/remote_client.dart';
import 'package:test/test.dart';

/// Records the order of RPCs and lets the test hold `connect` open, so the
/// window between "boot returned" and "the handshake finished" is explicit.
class _SlowRemote implements RemoteSurrealClient, StatementAwareRemote {
  final calls = <String>[];
  final releaseConnect = Completer<void>();

  @override
  Future<void> connect(String endpoint) async {
    calls.add('connect');
    await releaseConnect.future;
  }

  @override
  Future<void> use({required String namespace, required String database}) async {
    calls.add('use');
  }

  @override
  Future<dynamic> signup(Map<String, dynamic> params) async {
    calls.add('signup');
    return 'token';
  }

  @override
  Future<dynamic> signin(Map<String, dynamic> params) async {
    calls.add('signin');
    return 'token';
  }

  @override
  Future<dynamic> authenticate(String token) async {
    calls.add('authenticate');
    return null;
  }

  @override
  Future<void> invalidate() async => calls.add('invalidate');

  @override
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) async {
    calls.add('query');
    return const [];
  }

  @override
  Future<List<StatementResult>> queryStatements(String sql,
      [Map<String, dynamic>? vars]) async {
    calls.add('queryStatements');
    return const [];
  }

  @override
  Future<(String, Stream<LiveMessage>)> live(String sql,
          [Map<String, dynamic>? vars]) async =>
      ('live', const Stream<LiveMessage>.empty());

  @override
  Future<void> kill(String liveId) async {}

  @override
  Stream<void> get onConnected => const Stream<void>.empty();

  @override
  Stream<void> get onDisconnected => const Stream<void>.empty();

  @override
  Future<void> close() async => calls.add('close');
}

void main() {
  group('connect gate', () {
    late _SlowRemote remote;
    late RemoteDatabaseService service;

    setUp(() {
      remote = _SlowRemote();
      service = RemoteDatabaseService(
        const DatabaseConfig(
            endpoint: 'ws://example.invalid', namespace: 'main', database: 'main'),
        remote,
        SpookyLogger.root('test'),
      );
      service.armConnectGate();
    });

    test('an RPC issued before the handshake waits for it', () async {
      // Boot is local-first: it returns before the network half has run, so the
      // app can call signUp while `connect` is still in flight. Without the gate
      // the RPC reaches the server with no namespace selected, which SurrealDB
      // reports as "There was a problem with signing up".
      unawaited(service.connect());
      await pumpEventQueue();
      expect(remote.calls, ['connect'], reason: 'connect is still in flight');

      var signedUp = false;
      unawaited(service
          .signup({'access': 'account', 'variables': <String, dynamic>{}}).then(
              (_) => signedUp = true));
      await pumpEventQueue();
      expect(signedUp, isFalse);
      expect(remote.calls, ['connect'],
          reason: 'signup must not ride a socket that has no namespace yet');

      remote.releaseConnect.complete();
      await pumpEventQueue();

      expect(signedUp, isTrue);
      expect(remote.calls, ['connect', 'use', 'signup']);
    });

    test('a failed connect releases the waiters rather than wedging them',
        () async {
      final failing = _FailingRemote();
      final svc = RemoteDatabaseService(
        const DatabaseConfig(
            endpoint: 'ws://example.invalid', namespace: 'main', database: 'main'),
        failing,
        SpookyLogger.root('test'),
      );
      svc.armConnectGate();

      await expectLater(svc.connect(), throwsA(isA<StateError>()));
      // The attempt is over: a held RPC goes ahead and fails as a network
      // error, which the outbox and the registrations retry. Waiting forever
      // would wedge them instead.
      await expectLater(
          svc.signup(const {'access': 'account'}), throwsA(isA<StateError>()));
    });

    test('close releases the waiters', () async {
      unawaited(service.connect());
      await pumpEventQueue();
      final pending = service.signin(const {'access': 'account'});
      await service.close();
      await expectLater(pending, completion('token'));
    });
  });
}

class _FailingRemote extends _SlowRemote {
  @override
  Future<void> connect(String endpoint) async {
    calls.add('connect');
    throw StateError('boom');
  }

  @override
  Future<dynamic> signup(Map<String, dynamic> params) async {
    throw StateError('not connected');
  }
}
