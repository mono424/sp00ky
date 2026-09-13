@Tags(['integration'])
library;

import 'package:spooky_core/advanced.dart';

import 'dart:io';

import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

/// End-to-end feature-flag test against the running ssp stack (Dart counterpart
/// of example/e2e/tests/feature-flag-realtime.spec.ts).
///
/// Proves the reactive path: a signed-in client subscribed via the new
/// `client.feature(key)` (Dart `FeatureFlagModule`) must see a variant
/// assigned/changed by the root (scheduler/CLI) write path WITHOUT
/// re-subscribing. `_00_user_feature` rides the ordinary scoped live-sync path,
/// so this exercises:
///   1. the Dart feature-flag client support (previously absent — the gap), and
///   2. the ssp `_00_user_feature_mutation` ingest events delivering root writes
///      to the user's `_00_list_ref_user_<id>` -> LIVE -> Stream, through the
///      local circuit (the meta table is permitted via the built-in seed).
///
/// Targets the dev stack (SurrealDB ws://127.0.0.1:8666, ns/db main/main, the
/// `account` RECORD access). Override with SURREAL_DEV_WS. Skips if unreachable.
void main() {
  final endpoint =
      Platform.environment['SURREAL_DEV_WS'] ?? 'ws://127.0.0.1:8666';
  // The dev stack's database is project-specific (the example app uses
  // `main`/`example`); override via env so this runs against any deployment.
  final ns = Platform.environment['SURREAL_DEV_NS'] ?? 'main';
  final db = Platform.environment['SURREAL_DEV_DB'] ?? 'main';

  // The app schema knows nothing about the `_00_user_feature` meta table — that
  // is the whole point: its circuit permission comes from a built-in seed, not
  // from this schema.
  const schemaSurql = 'DEFINE TABLE user PERMISSIONS FOR select WHERE true;';
  final schema = {
    'user': {
      'columns': {'email': const ColumnSchema(type: 'string')},
    },
  };

  // Unique key per run so concurrent/repeat runs don't collide on assignments.
  final flagKey = 'ffdart_${DateTime.now().microsecondsSinceEpoch}';

  late WebSocketSurrealClient root;
  String? token;
  String? userId;
  var reachable = false;
  final createdUserIds = <String>[];

  setUp(() async {
    // Fast TCP probe first: the WS client doesn't fast-fail a refused port, so
    // without this the suite would hang ~3 min then time out instead of
    // skipping cleanly when no stack is up.
    if (!await _portReachable(endpoint)) {
      markTestSkipped(
          'Dev stack not reachable at $endpoint — start it with `spky dev` in example/.');
      return;
    }
    reachable = true;
    root = WebSocketSurrealClient();
    try {
      await root.connect(endpoint).timeout(const Duration(seconds: 3));
      await root.signin({'user': 'root', 'pass': 'root'});
      await root.use(namespace: ns, database: db);

      // Created as ROOT and signed in, rather than signed up: the deployed
      // schema may gate SIGNUP (an invite code, a trial window) and that is the
      // app's business, not this test's.
      const password = 'pw-12345';
      final email = 'ff_${DateTime.now().microsecondsSinceEpoch}@e2e.test';
      final created = await root.query(
        r'CREATE ONLY user SET email = $email, '
        r'password = crypto::argon2::generate($password)',
        {'email': email, 'password': password},
      );
      final row = created.isNotEmpty ? created.first : null;
      userId = row is Map ? row['id']?.toString() : null;
      if (userId == null) throw StateError('could not create the test user');
      createdUserIds.add(userId!);

      final sc = WebSocketSurrealClient();
      await sc.connect(endpoint);
      await sc.use(namespace: ns, database: db);
      token = (await sc.signin({
        'access': 'account',
        'variables': {'email': email, 'password': password},
      })) as String?;
      await sc.close();

      // A flag definition the client can never read (PERMISSIONS NONE), enabled
      // with an 'off' default — realistic setup.
      await root.query(
        r'CREATE _00_feature_flag SET key = $k, variants = ["off", "on"], '
        r'default_variant = "off", rules = [], enabled = true',
        {'k': flagKey},
      );
    } catch (e) {
      await root.close();
      markTestSkipped('Dev stack not reachable at $endpoint: $e');
      return;
    }
  });

  tearDown(() async {
    if (!reachable) return; // root was never initialized (skipped in setUp)
    try {
      await root
          .query(r'DELETE _00_user_feature WHERE key = $k', {'k': flagKey});
      await root
          .query(r'DELETE _00_feature_flag WHERE key = $k', {'k': flagKey});
    } catch (_) {}
    for (final id in createdUserIds) {
      try {
        await root.query(r'DELETE $id', {'id': RecordId.parse(id)});
      } catch (_) {}
    }
    await root.close();
  });

  // Upsert this user's assignment from the root write path (the scheduler/CLI
  // path), which fires the `_00_user_feature_mutation` ingest event.
  Future<void> setVariant(String variant) async {
    // SET must precede WHERE (the order `materialize` in apps/cli/src/flag.rs
    // uses); `UPSERT ... WHERE ... SET ...` is rejected on SurrealDB v3.
    await root.query(
      r'UPSERT _00_user_feature SET user = $u, key = $k, variant = $v '
      r'WHERE user = $u AND key = $k',
      {'u': RecordId.parse(userId!), 'k': flagKey, 'v': variant},
    );
  }

  test('subscribed client sees the variant flip on/off live', () async {
    if (!reachable) return; // already skipped in setUp
    if (token == null || userId == null) {
      markTestSkipped('signup did not yield a token/user');
      return;
    }

    final persistence = MemoryPersistenceClient();
    await persistence.set('sp00ky_auth_token', token);

    final client = InProcessSp00kyClient(Sp00kyConfig(
      database: DatabaseConfig(endpoint: endpoint, namespace: ns, database: db),
      schema: schema,
      schemaSurql: schemaSurql,
      persistenceClient: persistence,
    ));
    await client.init();
    addTearDown(client.close);

    // Let auth hydrate and the per-user LIVE subscription come up before the
    // shared feature query registers (so it lands on _00_list_ref_user_<id>).
    await Future<void>.delayed(const Duration(seconds: 1));

    final flag = client.feature(flagKey, fallback: 'off');
    addTearDown(flag.close);

    Future<bool> waitForVariant(String want) async {
      final deadline = DateTime.now().add(const Duration(seconds: 20));
      while (DateTime.now().isBefore(deadline)) {
        if (flag.variant() == want) return true;
        await Future<void>.delayed(const Duration(milliseconds: 200));
      }
      return flag.variant() == want;
    }

    // No assignment yet -> fallback / default experience.
    expect(await waitForVariant('off'), isTrue,
        reason: 'unassigned flag resolves to its fallback');
    expect(flag.enabled(), isFalse);

    // Server enables the flag AFTER the client subscribed — the reactive case.
    await setVariant('on');
    expect(await waitForVariant('on'), isTrue,
        reason: 'a root-written variant must reach the live subscription');
    expect(flag.enabled(), isTrue);

    // Server disables it — should flip back live without re-subscribing.
    await setVariant('off');
    expect(await waitForVariant('off'), isTrue,
        reason: 'a root-written flip back to off must propagate live');
    expect(flag.enabled(), isFalse);
  }, timeout: const Timeout(Duration(seconds: 90)));
}

/// Quick "is anything listening?" check. A raw [Socket] fast-fails a refused
/// port (unlike the WS client), so the suite can skip in ~1s instead of hanging.
Future<bool> _portReachable(String wsEndpoint) async {
  final uri = Uri.parse(wsEndpoint);
  final port = uri.hasPort ? uri.port : 8666;
  try {
    final socket = await Socket.connect(uri.host, port,
        timeout: const Duration(seconds: 1));
    socket.destroy();
    return true;
  } catch (_) {
    return false;
  }
}
