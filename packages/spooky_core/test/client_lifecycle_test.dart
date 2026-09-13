import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:test/test.dart';

import 'fake_remote.dart';

/// Records the raw auth RPCs `authenticate`/`deauthenticate` forward.
class _RecordingRemote extends FakeRemote {
  final List<String> authTokens = [];
  int invalidateCount = 0;

  @override
  Future<dynamic> authenticate(String token) async {
    authTokens.add(token);
    return null;
  }

  @override
  Future<void> invalidate() async => invalidateCount++;
}

/// Local-only InProcessSp00kyClient surface: init, queries as Streams, callback
/// subscribe, mutation counts, CRDT stubs, and double-init/close safety.
void main() {
  final schema = {
    'thread': {
      'columns': {'title': const ColumnSchema(type: 'string')},
    },
  };
  const schemaSurql = 'DEFINE TABLE thread PERMISSIONS FOR select WHERE true;';

  late InProcessSp00kyClient client;
  setUp(() async {
    client = InProcessSp00kyClient(Sp00kyConfig(
      database: const DatabaseConfig(namespace: 't', database: 't'),
      schema: schema,
      schemaSurql: schemaSurql,
    ));
    await client.init();
  });
  tearDown(() => client.close());

  test('init is idempotent', () async {
    await client.init(); // second call is a no-op
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    expect(hash, isNotEmpty);
  });

  test('queryStream registers and emits; create propagates', () async {
    final stream = await client.queryStream('SELECT * FROM thread', {});
    final emissions = <List<Map<String, dynamic>>>[];
    final sub = stream.listen(emissions.add);
    await client.create('thread:a', {'title': 'hi'});
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(emissions.last.map((r) => r['id']), contains('thread:a'));
    await sub.cancel();
  });

  test('callback subscribe receives the current set immediately', () async {
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    await client.create('thread:a', {'title': 'hi'});
    await Future<void>.delayed(const Duration(milliseconds: 150));

    List<Map<String, dynamic>>? latest;
    final off = client.subscribe(hash, (r) => latest = r, immediate: true);
    expect(latest, isNotNull);
    expect(latest!.any((r) => r['id'] == 'thread:a'), isTrue);
    off();
  });

  test('multiple Stream listeners on one hash both receive updates', () async {
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    final a = <List<Map<String, dynamic>>>[];
    final b = <List<Map<String, dynamic>>>[];
    final s1 = client.subscribeStream(hash).listen(a.add);
    final s2 = client.subscribeStream(hash).listen(b.add);
    await client.create('thread:a', {'title': 'hi'});
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(a.last.any((r) => r['id'] == 'thread:a'), isTrue);
    expect(b.last.any((r) => r['id'] == 'thread:a'), isTrue);
    await s1.cancel();
    await s2.cancel();
  });

  test('pendingMutationCount is 0 without a remote sync', () {
    expect(client.pendingMutationCount, 0);
    expect(client.fetchingQueryCount, 0);
  });

  test('CRDT entry points are stubbed (deferred)', () {
    expect(() => client.openCrdtField('thread', 'thread:a', 'content'),
        throwsA(isA<UnimplementedError>()));
    expect(() => client.closeCrdtField('thread', 'thread:a', 'content'),
        throwsA(isA<UnimplementedError>()));
  });

  test('auth getter throws without a remote endpoint', () {
    expect(() => client.auth, throwsA(isA<StateError>()));
  });

  test('queryRaw rejects an unparseable query', () {
    expect(
        () => client.queryRaw('RETURN 1', {}), throwsA(isA<ArgumentError>()));
  });

  test('authenticate/deauthenticate throw without a remote endpoint', () {
    expect(() => client.authenticate('tok'), throwsA(isA<StateError>()));
    expect(() => client.deauthenticate(), throwsA(isA<StateError>()));
  });

  test('reportFrontendTiming records a frontend phase sample', () async {
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    expect(client.queryTimings(hash)!.frontend.count, 0);

    client.reportFrontendTiming(hash, 4);
    client.reportFrontendTiming(hash, 8);
    // Non-finite samples are dropped.
    client.reportFrontendTiming(hash, double.nan);

    final stat = client.queryTimings(hash)!.frontend;
    expect(stat.count, 2);
    expect(stat.lastMs, 8);
    expect(stat.p50, isNotNull);
    // Unknown hashes are a no-op rather than an error.
    client.reportFrontendTiming('nope', 1);
    expect(client.queryTimings('nope'), isNull);
  });

  test('frontend phase window caps at materializationSampleWindow', () async {
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    for (var i = 0; i < materializationSampleWindow + 10; i++) {
      client.reportFrontendTiming(hash, i.toDouble());
    }
    expect(
        client.queryTimings(hash)!.frontend.count, materializationSampleWindow);
  });

  group('with a remote endpoint', () {
    late _RecordingRemote remote;
    late InProcessSp00kyClient remoteClient;

    setUp(() async {
      remote = _RecordingRemote();
      remoteClient = InProcessSp00kyClient(
        Sp00kyConfig(
          database: const DatabaseConfig(
              endpoint: 'ws://x', namespace: 't', database: 't'),
          schema: schema,
          schemaSurql: schemaSurql,
        ),
        remoteClient: remote,
      );
      await remoteClient.init();
    });
    tearDown(() => remoteClient.close());

    test('authenticate/deauthenticate forward the raw RPCs', () async {
      await remoteClient.authenticate('tok-1');
      // At least once: the token is also remembered for the transport, so a
      // socket rebuilt later comes back authenticated rather than anonymous.
      expect(remote.authTokens, contains('tok-1'));
      await remoteClient.deauthenticate();
      expect(remote.invalidateCount, 1);
    });

    test('a fresh query is not authoritative until the server answers',
        () async {
      final hash = await remoteClient.queryRaw('SELECT * FROM thread', {});
      expect(remoteClient.isQueryAuthoritative(hash), isFalse);
      expect(remoteClient.isQuerySettled(hash), isFalse);
    });

    test('appRelease returns a handle that starts empty', () async {
      final handle = remoteClient.appRelease('web');
      expect(handle.app, 'web');
      expect(handle.version(), isNull);
      expect(handle.updateAvailable('1.0.0'), isFalse);
      expect(handle.mandatory, isFalse);
      expect(handle.cacheBust, isFalse);
      handle.close();
    });

    test('syncHealthStream replays the current snapshot on listen', () async {
      final seen = <SyncHealth>[];
      final sub = remoteClient.syncHealthStream().listen(seen.add);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(seen, isNotEmpty);
      expect(seen.first.status, SyncHealthStatus.healthy);
      await sub.cancel();
    });
  });

  test('feature() and appRelease() require a remote endpoint', () {
    expect(() => client.feature('flag'), throwsA(isA<StateError>()));
    expect(() => client.appRelease('web'), throwsA(isA<StateError>()));
  });

  test('local-only sync health is healthy and never connected', () {
    expect(client.syncHealth.status, SyncHealthStatus.healthy);
    expect(client.syncHealth.isDegraded, isFalse);
    // Nothing to reach, so nothing ever connected: an app tells a cold start
    // apart from a lost connection with this rather than with the status.
    expect(client.syncHealth.everConnected, isFalse);

    final seen = <SyncHealth>[];
    final off = client.subscribeToSyncHealth(seen.add);
    expect(seen, hasLength(1));
    off();
  });

  test('a local-only query starts idle (nothing would settle it)', () async {
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    expect(client.state.queries[hash]!.lifecycle.fetchDepth, 0);
    expect(client.isQuerySettled(hash), isFalse);
  });
}
