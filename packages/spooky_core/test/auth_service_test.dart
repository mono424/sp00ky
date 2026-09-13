import 'dart:async';

import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/database/remote_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

/// Fake remote client recording auth calls and answering the `$auth.id` fetch.
class FakeAuthRemote implements RemoteSurrealClient {
  Map<String, dynamic>? authUser; // returned by SELECT * FROM ONLY $auth.id
  bool authenticated = false;
  bool invalidated = false;
  Map<String, dynamic>? lastSignin;
  Map<String, dynamic>? lastSignup;

  @override
  Future<dynamic> authenticate(String token) async => authenticated = true;
  @override
  Future<void> invalidate() async => invalidated = true;
  @override
  Future<dynamic> signin(Map<String, dynamic> params) async {
    lastSignin = params;
    return {'access': 'signed-in-token'};
  }

  @override
  Future<dynamic> signup(Map<String, dynamic> params) async {
    lastSignup = params;
    return {'access': 'signed-up-token'};
  }

  @override
  Future<List<dynamic>> query(String sql, [Map<String, dynamic>? vars]) async {
    if (sql.contains(r'$auth.id')) {
      return [
        authUser == null ? <dynamic>[] : [authUser]
      ];
    }
    return [null];
  }

  @override
  Future<void> connect(String endpoint) async {}
  @override
  Future<void> use(
      {required String namespace, required String database}) async {}
  @override
  Future<(String, Stream<LiveMessage>)> live(String sql,
          [Map<String, dynamic>? vars]) async =>
      ('l', const Stream<LiveMessage>.empty());
  @override
  Future<void> kill(String liveId) async {}
  @override
  Stream<void> get onConnected => const Stream.empty();
  @override
  Stream<void> get onDisconnected => const Stream.empty();
  @override
  Future<void> close() async {}
}

void main() {
  final logger = SpookyLogger.root('test');

  late FakeAuthRemote remoteClient;
  late RemoteDatabaseService remote;
  late MemoryPersistenceClient persistence;

  // schema with an `account` access for signIn/signUp validation.
  final schema = {
    'access': {
      'account': {
        'signIn': {
          'params': {
            'email': {'optional': false},
            'password': {'optional': false},
          }
        },
        'signup': {
          'params': {
            'email': {'optional': false},
          }
        },
      }
    }
  };

  AuthService build() => AuthService(schema, remote, persistence, logger);

  setUp(() {
    remoteClient = FakeAuthRemote();
    remote = RemoteDatabaseService(
      const DatabaseConfig(namespace: 'n', database: 'd'),
      remoteClient,
      logger,
    );
    persistence = MemoryPersistenceClient();
  });

  test('check() with no token leaves unauthenticated', () async {
    final auth = build();
    await auth.init();
    expect(auth.isAuthenticated, isFalse);
    expect(auth.isLoading, isFalse);
  });

  test('check() with a stored token hydrates the user', () async {
    await persistence.set('sp00ky_auth_token', 'tok');
    remoteClient.authUser = {'id': 'user:1', 'email': 'a@b.c'};
    final auth = build();
    await auth.init();
    expect(auth.isAuthenticated, isTrue);
    expect(auth.currentUser!['id'], 'user:1');
    expect(remoteClient.authenticated, isTrue);
  });

  test('subscribe fires immediately with current user, then on change',
      () async {
    await persistence.set('sp00ky_auth_token', 'tok');
    remoteClient.authUser = {'id': 'user:1'};
    final auth = build();
    await auth.init();

    final seen = <String?>[];
    auth.subscribe(seen.add);
    expect(seen, ['user:1']); // immediate

    await auth.signOut();
    await Future<void>.delayed(Duration.zero);
    expect(seen.last, isNull); // notified on sign-out
  });

  test('signOut clears state, removes token, invalidates', () async {
    await persistence.set('sp00ky_auth_token', 'tok');
    remoteClient.authUser = {'id': 'user:1'};
    final auth = build();
    await auth.init();

    await auth.signOut();
    expect(auth.isAuthenticated, isFalse);
    expect(auth.currentUser, isNull);
    expect(remoteClient.invalidated, isTrue);
    expect(await persistence.get<String>('sp00ky_auth_token'), isNull);
  });

  test('signIn validates required params then authenticates', () async {
    remoteClient.authUser = {'id': 'user:9'};
    final auth = build();

    await expectLater(
      auth.signIn('account', {'email': 'a@b.c'}), // missing password
      throwsA(isA<StateError>()),
    );

    await auth.signIn('account', {'email': 'a@b.c', 'password': 'pw'});
    expect(remoteClient.lastSignin!['access'], 'account');
    expect(auth.isAuthenticated, isTrue);
  });

  test('signUp validates required params', () async {
    final auth = build();
    await expectLater(
      auth.signUp('account', {}), // missing email
      throwsA(isA<StateError>()),
    );
  });

  test('unknown access name throws', () async {
    final auth = build();
    await expectLater(
      auth.signIn('nope', {'email': 'a', 'password': 'b'}),
      throwsA(isA<StateError>()),
    );
  });

  group('signOut flushes the outbox first', () {
    test('the hook runs while the session is still valid', () async {
      // Sign-out flips the bucket, and a bucket switch abandons the outgoing
      // outbox in the old store. Pushing after the token is cleared would run
      // the statements unauthenticated and come back as rejections, which roll
      // the writes back, so the flush has to happen before any of that.
      final auth = build();
      auth.currentUser = {'id': 'user:a'};
      auth.token = 'tok';
      auth.isAuthenticated = true;
      remoteClient.invalidated = false;

      String? tokenDuringFlush;
      var invalidatedDuringFlush = true;
      auth.onBeforeSignOut = () async {
        tokenDuringFlush = auth.token;
        invalidatedDuringFlush = remoteClient.invalidated;
      };

      await auth.signOut();

      expect(tokenDuringFlush, 'tok');
      expect(invalidatedDuringFlush, isFalse);
      expect(auth.token, isNull);
      expect(auth.isAuthenticated, isFalse);
      expect(remoteClient.invalidated, isTrue);
    });

    test('a failing flush never blocks signing out', () async {
      final auth = build();
      auth.token = 'tok';
      auth.isAuthenticated = true;
      auth.onBeforeSignOut = () async => throw StateError('server gone');

      await auth.signOut();

      expect(auth.token, isNull);
      expect(auth.isAuthenticated, isFalse);
    });
  });
}
