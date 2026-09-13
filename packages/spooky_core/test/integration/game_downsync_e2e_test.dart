@Tags(['integration'])
library;

import 'dart:io';

import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

/// True down-path test for a RECORD-LINK-FILTERED live query — the WhitePawn
/// history case: `SELECT * FROM game WHERE database = $db` where `database` is a
/// `record<game_database>`. Games created out-of-band (as if from the website)
/// must reach an authenticated client via the ssp down-path:
/// register -> ssp materializes -> _00_list_ref_user_<id> -> LIVE -> fetch -> Stream.
///
/// This is the scenario the JSON-RPC client silently broke: a record-id param
/// went over the wire as a `"table:id"` string, so SurrealDB matched nothing
/// (list_ref read + record fetch returned empty) and the query emitted 0 rows.
///
/// Targets the dev stack (SurrealDB ws://127.0.0.1:8666, ns/db main/main, the
/// `account` RECORD access). Override with SURREAL_DEV_WS. Skips if unreachable.
void main() {
  final endpoint =
      Platform.environment['SURREAL_DEV_WS'] ?? 'ws://127.0.0.1:8666';
  const ns = 'main';
  const db = 'main';

  // Minimal local-circuit schema: both tables world-readable so the circuit
  // doesn't default-deny. Columns live in the [schema] map below.
  const schemaSurql = '''
DEFINE TABLE game_database PERMISSIONS FOR select WHERE true;
DEFINE TABLE game PERMISSIONS FOR select WHERE true;
''';
  final schema = {
    'game_database': {
      'columns': {
        'name': const ColumnSchema(type: 'string'),
        'owner': const ColumnSchema(type: 'record', recordId: true),
        'source': const ColumnSchema(type: 'string', optional: true),
      },
    },
    'game': {
      'columns': {
        'database': const ColumnSchema(type: 'record', recordId: true),
        'owner': const ColumnSchema(type: 'record', recordId: true),
        'white': const ColumnSchema(type: 'string'),
        'black': const ColumnSchema(type: 'string'),
        'status': const ColumnSchema(type: 'string'),
        'pgn': const ColumnSchema(type: 'string'),
        'result': const ColumnSchema(type: 'string'),
        'date': const ColumnSchema(type: 'datetime', dateTime: true),
        'sort_index': const ColumnSchema(type: 'int'),
        'source': const ColumnSchema(type: 'string', optional: true),
        'source_id': const ColumnSchema(type: 'string', optional: true),
      },
    },
  };

  late WebSocketSurrealClient root;
  String? token;
  String? userId;
  String? dbId; // the "Mobile" game_database record id
  final createdIds = <String>[]; // cleaned up in tearDown (root)

  setUp(() async {
    root = WebSocketSurrealClient();
    try {
      await root.connect(endpoint).timeout(const Duration(seconds: 3));
      await root.signin({'user': 'root', 'pass': 'root'});
      await root.use(namespace: ns, database: db);

      // Created as ROOT and signed in, rather than signed up: the deployed
      // schema may gate SIGNUP (an invite code, a trial window) and that is the
      // app's business, not this test's.
      const password = 'pw-12345';
      final email = 'gd_${DateTime.now().microsecondsSinceEpoch}@e2e.test';
      final created = await root.query(
        r'CREATE ONLY user SET email = $email, '
        r'password = crypto::argon2::generate($password)',
        {'email': email, 'password': password},
      );
      final row = created.isNotEmpty ? created.first : null;
      userId = row is Map ? row['id']?.toString() : null;
      if (userId == null) throw StateError('could not create the test user');
      createdIds.add(userId!);

      final sc = WebSocketSurrealClient();
      await sc.connect(endpoint);
      await sc.use(namespace: ns, database: db);
      token = (await sc.signin({
        'access': 'account',
        'variables': {'email': email, 'password': password},
      })) as String?;
      await sc.close();
    } catch (e) {
      await root.close();
      markTestSkipped('Dev stack not reachable at $endpoint: $e');
      return;
    }
  });

  tearDown(() async {
    // Delete games first, then collection, then user.
    for (final id in createdIds.reversed) {
      try {
        await root.query('DELETE type::record(\$id)', {'id': id});
      } catch (_) {}
    }
    await root.close();
  });

  // Create a record via root (bypasses the account permission) and return its
  // id. Record fields are cast with type::record so real SurrealDB stores them
  // as links.
  Future<String> rootCreate(String sql, Map<String, dynamic> vars) async {
    final res = await root.query(sql, vars);
    final first = res.isNotEmpty ? res.first : null;
    final rec = first is List ? (first.isNotEmpty ? first.first : null) : first;
    final id = (rec as Map)['id'].toString();
    createdIds.add(id);
    return id;
  }

  test('record-link-filtered query down-syncs existing + live add/delete',
      () async {
    if (token == null || userId == null) {
      markTestSkipped('signup did not yield a token/user');
      return;
    }

    // --- seed out-of-band (as if created on the website) -------------------
    dbId = await rootCreate(
      "CREATE game_database SET name = 'Mobile', icon_type = 'database', "
      "color = '#56CCF2', owner = type::record(\$owner), source = 'mobile'",
      {'owner': userId},
    );

    Future<String> seedGame(int i) => rootCreate(
          "CREATE game SET database = type::record(\$db), "
          "owner = type::record(\$owner), white = \$w, black = \$b, "
          "status = 'Finished', pgn = '1. e4 e5', result = '1-0', "
          "date = time::now(), sort_index = \$si, source = 'mobile', "
          "source_id = \$sid",
          {
            'db': dbId,
            'owner': userId,
            'w': 'White$i',
            'b': 'Black$i',
            'si': -i,
            'sid': '$i',
          },
        );

    final seeded = <String>[for (var i = 0; i < 3; i++) await seedGame(i)];

    // --- bring up the authenticated client + register the filtered query ---
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
    await Future<void>.delayed(const Duration(seconds: 1)); // auth + LIVE up

    final hash = await client.queryRaw(
      r'SELECT * FROM game WHERE database = $db ORDER BY sort_index ASC',
      {'db': RecordId.parse(dbId!)},
    );
    final emissions = <List<Map<String, dynamic>>>[];
    final sub = client.subscribeStream(hash).listen(emissions.add);
    addTearDown(sub.cancel);

    Set<String> latestIds() => emissions.isEmpty
        ? <String>{}
        : emissions.last.map((r) => r['id'].toString()).toSet();

    Future<bool> waitUntil(bool Function() pred,
        {int seconds = 20}) async {
      final deadline = DateTime.now().add(Duration(seconds: seconds));
      while (DateTime.now().isBefore(deadline)) {
        if (pred()) return true;
        await Future<void>.delayed(const Duration(milliseconds: 200));
      }
      return pred();
    }

    // (a) the 3 pre-existing games arrive via the down-path.
    final sawAll = await waitUntil(() => seeded.every(latestIds().contains));
    expect(sawAll, isTrue,
        reason: 'all 3 pre-existing games should down-sync into the query; '
            'got ${latestIds()}');

    // (b) realtime ADD: a 4th game created out-of-band appears.
    final added = await seedGame(99);
    final sawAdd = await waitUntil(() => latestIds().contains(added));
    expect(sawAdd, isTrue,
        reason: 'a newly-created game should appear in the live query');

    // (c) realtime DELETE: removing it out-of-band drops it from the window.
    // Retried: the SSP is re-materializing the same row, and SurrealDB answers
    // a racing write with a retryable transaction conflict.
    await _withRetry(
        () => root.query('DELETE type::record(\$id)', {'id': added}));
    createdIds.remove(added);
    final sawDelete = await waitUntil(() => !latestIds().contains(added));
    expect(sawDelete, isTrue,
        reason: 'a deleted game should disappear from the live query');

    // The original 3 are still present after the add/delete churn.
    expect(seeded.every(latestIds().contains), isTrue,
        reason: 'the original games should remain; got ${latestIds()}');
  }, timeout: const Timeout(Duration(seconds: 90)));
}

/// Retry a root write that a concurrent SSP transaction can conflict with.
Future<T> _withRetry<T>(Future<T> Function() run, {int attempts = 5}) async {
  Object? last;
  for (var i = 0; i < attempts; i++) {
    try {
      return await run();
    } catch (e) {
      last = e;
      if (!e.toString().toLowerCase().contains('conflict')) rethrow;
      await Future<void>.delayed(Duration(milliseconds: 100 * (i + 1)));
    }
  }
  throw StateError('write kept conflicting: $last');
}
