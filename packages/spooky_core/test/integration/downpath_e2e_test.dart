@Tags(['integration'])
library;

import 'dart:io';

import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

/// True down-path test against the running ssp stack: a record created from a
/// SEPARATE root connection must reach an authenticated client ONLY via
/// ssp materialization -> _00_list_ref_user_<id> -> LIVE -> fetch -> Stream.
///
/// Targets the dev stack (SurrealDB ws://127.0.0.1:8666, ns/db main/main, the
/// `account` RECORD access). Override with SURREAL_DEV_WS. Skips if unreachable.
void main() {
  final endpoint =
      Platform.environment['SURREAL_DEV_WS'] ?? 'ws://127.0.0.1:8666';
  const ns = 'main';
  const db = 'main';

  // Minimal schema for the local circuit: `user` is world-readable.
  const schemaSurql = 'DEFINE TABLE user PERMISSIONS FOR select WHERE true;';
  final schema = {
    'user': {
      'columns': {
        'email': const ColumnSchema(type: 'string'),
      },
    },
  };

  late WebSocketSurrealClient root;
  String? token;
  String? userId;
  final createdIds = <String>[];

  setUp(() async {
    root = WebSocketSurrealClient();
    try {
      await root.connect(endpoint).timeout(const Duration(seconds: 3));
      await root.signin({'user': 'root', 'pass': 'root'});
      await root.use(namespace: ns, database: db);
      // Sign up a fresh user via the `account` access to get a user token.
      final email = 'dp_${DateTime.now().microsecondsSinceEpoch}@e2e.test';
      final sc = WebSocketSurrealClient();
      await sc.connect(endpoint);
      await sc.use(namespace: ns, database: db);
      token = (await sc.signup({
        'access': 'account',
        'variables': {'email': email, 'password': 'pw-12345'},
      })) as String?;
      // Resolve the user's record id.
      final who = await sc.query(r'SELECT VALUE id FROM ONLY $auth.id');
      userId = who.isNotEmpty ? who.first?.toString() : null;
      if (userId != null) createdIds.add(userId!); // clean up the test user
      await sc.close();
    } catch (e) {
      await root.close();
      markTestSkipped('Dev stack not reachable at $endpoint: $e');
      return;
    }
  });

  tearDown(() async {
    for (final id in createdIds) {
      try {
        await root.query('DELETE \$id', {'id': RecordId.parse(id)});
      } catch (_) {}
    }
    await root.close();
  });

  test('externally-created record reaches the client via the down-path',
      () async {
    if (token == null || userId == null) {
      markTestSkipped('signup did not yield a token/user');
      return;
    }

    // Pre-seed the token so AuthService hydrates the user and starts the
    // _00_list_ref_user_<id> LIVE subscription.
    final persistence = MemoryPersistenceClient();
    await persistence.set('sp00ky_auth_token', token);

    final client = Sp00kyClient(Sp00kyConfig(
      database: DatabaseConfig(endpoint: endpoint, namespace: ns, database: db),
      schema: schema,
      schemaSurql: schemaSurql,
      persistenceClient: persistence,
    ));
    await client.init();
    addTearDown(client.close);

    // Wait for auth and the LIVE subscription to come up.
    await Future<void>.delayed(const Duration(seconds: 1));

    // Register a query for the user's own record (created via signup on a
    // SEPARATE connection — the client never ingested it locally) and
    // subscribe. It can only arrive via the ssp down-path:
    // register -> ssp materializes -> _00_list_ref_user_<id> -> fetch -> Stream.
    final hash = await client.queryRaw(
      r'SELECT * FROM user WHERE id = $me',
      {'me': RecordId.parse(userId!)},
    );
    final emissions = <List<Map<String, dynamic>>>[];
    final sub = client.subscribeStream(hash).listen(emissions.add);

    Future<bool> sawUser() async {
      final deadline = DateTime.now().add(const Duration(seconds: 15));
      while (DateTime.now().isBefore(deadline)) {
        if (emissions
            .expand((e) => e)
            .any((r) => r['id'].toString() == userId)) {
          return true;
        }
        await Future<void>.delayed(const Duration(milliseconds: 200));
      }
      return false;
    }

    expect(await sawUser(), isTrue,
        reason: 'the user record (created out-of-band at signup) should arrive '
            'via the ssp down-path');

    // Now exercise a LIVE change: update the record from the root connection
    // and confirm the new value propagates down to the client.
    final newEmail =
        'updated_${DateTime.now().microsecondsSinceEpoch}@e2e.test';
    await root.query(r'UPDATE $id SET email = $e',
        {'id': RecordId.parse(userId!), 'e': newEmail});

    final liveDeadline = DateTime.now().add(const Duration(seconds: 15));
    var sawUpdate = false;
    while (DateTime.now().isBefore(liveDeadline)) {
      if (emissions
          .expand((e) => e)
          .any((r) => r['id'].toString() == userId && r['email'] == newEmail)) {
        sawUpdate = true;
        break;
      }
      await Future<void>.delayed(const Duration(milliseconds: 200));
    }
    expect(sawUpdate, isTrue,
        reason: 'a root-side UPDATE should propagate via the LIVE down-path');

    await sub.cancel();
  }, timeout: const Timeout(Duration(seconds: 60)));
}
