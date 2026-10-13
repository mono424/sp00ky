import 'dart:async';
import 'dart:math' as math;

import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/kernel/effects.dart' show StatementResult;
import 'package:spooky_core/src/services/database/remote_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/surreal/remote_client.dart';
import 'package:test/test.dart';

/// A socket that holds every request until the test answers it, so what is in
/// flight at once is explicit. `RETURN` probes answer at once.
class _HeldRemote implements RemoteSurrealClient, StatementAwareRemote {
  /// Every request that reached the socket, in order.
  final started = <String>[];
  final _held = <String, Completer<List<StatementResult>>>{};
  int inFlight = 0;
  int maxInFlight = 0;

  Future<List<StatementResult>> _hold(String sql) async {
    started.add(sql);
    if (sql.startsWith('RETURN')) return const [StatementResult.ok(true)];
    final answer = Completer<List<StatementResult>>();
    _held[sql] = answer;
    inFlight++;
    maxInFlight = math.max(maxInFlight, inFlight);
    try {
      return await answer.future;
    } finally {
      inFlight--;
    }
  }

  void answer(String sql) =>
      _held.remove(sql)!.complete([StatementResult.ok(sql)]);

  void fail(String sql) =>
      _held.remove(sql)!.completeError(StateError('socket closed'));

  @override
  Future<List<StatementResult>> queryStatements(String sql,
          [Map<String, dynamic>? vars]) =>
      _hold(sql);

  @override
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) async =>
      [for (final r in await _hold(sql)) r.result];

  @override
  Future<void> connect(String endpoint) async {}

  @override
  Future<void> use(
      {required String namespace, required String database}) async {}

  @override
  Future<dynamic> authenticate(String token) async => null;

  @override
  Future<dynamic> signin(Map<String, dynamic> params) async => 'token';

  @override
  Future<dynamic> signup(Map<String, dynamic> params) async => 'token';

  @override
  Future<void> invalidate() async {}

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
  Future<void> close() async {}
}

/// Tracks whether a future has settled without awaiting it.
class _Watch<T> {
  _Watch(Future<T> future) {
    future.then((v) {
      value = v;
      done = true;
    }, onError: (Object e) {
      error = e;
      done = true;
    });
  }

  bool done = false;
  T? value;
  Object? error;
}

void main() {
  late _HeldRemote socket;

  RemoteDatabaseService service({int pool = 6}) => RemoteDatabaseService(
        DatabaseConfig(
          endpoint: 'ws://example.invalid',
          namespace: 'main',
          database: 'main',
          maxConcurrentQueries: pool,
        ),
        socket,
        SpookyLogger.root('test'),
      );

  setUp(() => socket = _HeldRemote());

  group('reads', () {
    test('overlap: the second is sent before the first answers', () async {
      final remote = service();
      final a = _Watch(remote.queryStatements('SELECT * FROM a'));
      final b = _Watch(remote.queryStatements('SELECT * FROM b'));
      await pumpEventQueue();

      expect(socket.started, ['SELECT * FROM a', 'SELECT * FROM b']);
      expect(socket.inFlight, 2);

      socket.answer('SELECT * FROM b');
      await pumpEventQueue();
      expect(b.done, isTrue, reason: 'b does not wait for a');
      expect(a.done, isFalse);

      socket.answer('SELECT * FROM a');
      await pumpEventQueue();
      expect(a.value!.single.result, 'SELECT * FROM a');
    });

    test('are bounded by the pool and admitted in order', () async {
      final remote = service(pool: 2);
      final watches = [
        for (final t in ['r1', 'r2', 'r3', 'r4'])
          _Watch(remote.query('SELECT * FROM $t'))
      ];
      await pumpEventQueue();
      expect(socket.started, ['SELECT * FROM r1', 'SELECT * FROM r2']);

      socket.answer('SELECT * FROM r2');
      await pumpEventQueue();
      expect(socket.started.last, 'SELECT * FROM r3');

      // A failed read gives its slot back too.
      socket.fail('SELECT * FROM r1');
      await pumpEventQueue();
      expect(watches[0].error, isA<StateError>());
      expect(socket.started.last, 'SELECT * FROM r4');

      socket.answer('SELECT * FROM r3');
      socket.answer('SELECT * FROM r4');
      await pumpEventQueue();
      expect(watches.every((w) => w.done), isTrue);
      expect(socket.maxInFlight, 2);
    });

    test('are not held behind a write', () async {
      final remote = service();
      final write = _Watch(remote.writeStatements('UPDATE thing:1 MERGE {}'));
      final read = _Watch(remote.queryStatements('SELECT * FROM thing'));
      await pumpEventQueue();
      expect(
          socket.started, ['UPDATE thing:1 MERGE {}', 'SELECT * FROM thing']);

      socket.answer('SELECT * FROM thing');
      await pumpEventQueue();
      expect(read.done, isTrue);
      expect(write.done, isFalse);
      socket.answer('UPDATE thing:1 MERGE {}');
    });
  });

  group('writes', () {
    test('never overlap: the second is sent only after the first settles',
        () async {
      final remote = service();
      final first = _Watch(remote.writeStatements('CREATE ONLY thing:1'));
      final second = _Watch(remote.writeStatements('UPDATE thing:1 MERGE {}'));
      await pumpEventQueue();
      expect(socket.started, ['CREATE ONLY thing:1']);

      socket.answer('CREATE ONLY thing:1');
      await pumpEventQueue();
      expect(first.done, isTrue);
      expect(
          socket.started, ['CREATE ONLY thing:1', 'UPDATE thing:1 MERGE {}']);

      socket.answer('UPDATE thing:1 MERGE {}');
      await pumpEventQueue();
      expect(second.done, isTrue);
      expect(socket.maxInFlight, 1);
    });

    test('a failed write releases the lane for the next', () async {
      final remote = service();
      final first = _Watch(remote.writeStatements('CREATE ONLY thing:1'));
      _Watch(remote.writeStatements('CREATE ONLY thing:2'));
      await pumpEventQueue();

      socket.fail('CREATE ONLY thing:1');
      await pumpEventQueue();
      expect(first.error, isA<StateError>());
      expect(socket.started.last, 'CREATE ONLY thing:2');
      socket.answer('CREATE ONLY thing:2');
    });
  });

  group('gates', () {
    test('every lane waits out an account transition, then runs concurrently',
        () async {
      final remote = service();
      final transition = remote.beginSessionTransition();
      _Watch(remote.queryStatements('SELECT * FROM a'));
      _Watch(remote.queryStatements('SELECT * FROM b'));
      _Watch(remote.writeStatements('CREATE ONLY thing:1'));
      await pumpEventQueue();
      expect(socket.started, isEmpty);

      remote.endSessionTransition(transition);
      await pumpEventQueue();
      expect(socket.started, unorderedEquals(
          ['SELECT * FROM a', 'SELECT * FROM b', 'CREATE ONLY thing:1']));
      expect(socket.inFlight, 3);
    });

    test('every lane waits for the handshake', () async {
      final remote = service()..armConnectGate();
      _Watch(remote.queryStatements('SELECT * FROM a'));
      _Watch(remote.writeStatements('CREATE ONLY thing:1'));
      await pumpEventQueue();
      expect(socket.started, isEmpty);

      await remote.connect();
      await pumpEventQueue();
      expect(socket.started,
          containsAll(['SELECT * FROM a', 'CREATE ONLY thing:1']));
    });

    test('a read in flight when the account changes is discarded', () async {
      final remote = service();
      final read = _Watch(remote.query('SELECT * FROM a'));
      await pumpEventQueue();
      remote.setAuthToken('next-account');
      socket.answer('SELECT * FROM a');
      await pumpEventQueue();
      expect(read.error, isA<StateError>());
    });

    test('a read still waiting for a slot when the account changes never runs',
        () async {
      final remote = service(pool: 1);
      final first = _Watch(remote.query('SELECT * FROM a'));
      final queued = _Watch(remote.query('SELECT * FROM b'));
      await pumpEventQueue();
      expect(socket.started, ['SELECT * FROM a']);

      remote.setAuthToken('next-account');
      socket.answer('SELECT * FROM a');
      await pumpEventQueue();
      expect(first.error, isA<StateError>());
      expect(queued.error, isA<StateError>());
      expect(socket.started, ['SELECT * FROM a'],
          reason: 'a request issued for one account must not reach the next');
    });
  });

  group('probe', () {
    test('fails while a write hangs, so the supervisor tears the socket down',
        () async {
      final remote = service();
      _Watch(remote.writeStatements('CREATE ONLY thing:1'));
      final probe = _Watch(remote.probe());
      await pumpEventQueue();
      expect(probe.done, isFalse,
          reason: 'a wedged write lane must not look healthy');

      socket.answer('CREATE ONLY thing:1');
      await pumpEventQueue();
      expect(probe.done, isTrue);
      expect(probe.error, isNull);
    });

    test('passes while one read hangs and the pool still moves', () async {
      final remote = service(pool: 2);
      _Watch(remote.queryStatements('SELECT * FROM slow'));
      final probe = _Watch(remote.probe());
      await pumpEventQueue();
      expect(probe.done, isTrue);
      expect(probe.error, isNull);
      socket.answer('SELECT * FROM slow');
    });
  });
}
