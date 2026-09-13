@Tags(['integration'])
library;

import 'package:spooky_core/advanced.dart';

import 'dart:io';

import 'package:spooky_core/spooky_core.dart';
import 'package:test/test.dart';

/// End-to-end mutation sync (up-path) through the real InProcessSp00kyClient against a
/// live SurrealDB: a local create flows through the up-queue and lands in the
/// remote database. (The down-path register/list_ref needs the ssp server and
/// is out of scope here.)
void main() {
  final endpoint =
      Platform.environment['SURREAL_IT_ENDPOINT'] ?? 'ws://127.0.0.1:18011';

  final schema = {
    'thread': {
      'columns': {'title': const ColumnSchema(type: 'string')},
    },
  };
  const schemaSurql =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';

  late InProcessSp00kyClient client;
  late _RootVerifier verifier;

  setUp(() async {
    verifier = _RootVerifier();
    try {
      await verifier.connect(endpoint).timeout(const Duration(seconds: 3));
      await verifier.use(namespace: 'e2e', database: 'e2e');
      await verifier.query('REMOVE TABLE IF EXISTS thread');
    } catch (e) {
      await verifier.close();
      markTestSkipped('SurrealDB not reachable at $endpoint: $e');
      return;
    }

    client = InProcessSp00kyClient(
      Sp00kyConfig(
        database: const DatabaseConfig(
            endpoint: '', namespace: 'e2e', database: 'e2e'),
        schema: schema,
        schemaSurql: schemaSurql,
        persistenceClient: 'memory',
      ),
      remoteClient: _RootClient(endpoint),
    );
    await client.init();
  });

  tearDown(() async {
    await client.close();
    try {
      await verifier.query('REMOVE TABLE IF EXISTS thread');
    } catch (_) {}
    await verifier.close();
  });

  test('local create propagates to the remote database', () async {
    await client.create('thread:e2e1', {'title': 'hello-remote'});

    // Drain the up-queue against the real server.
    await _waitFor(() async {
      final rows = await verifier.rows('SELECT * FROM thread');
      return rows.any((r) => r['title'] == 'hello-remote');
    }, const Duration(seconds: 5));

    final rows = await verifier.rows('SELECT * FROM thread');
    expect(rows.map((r) => r['title']), contains('hello-remote'));
  });

  test('local update propagates to the remote database', () async {
    await client.create('thread:e2e2', {'title': 'v1'});
    await _waitFor(() async {
      final rows = await verifier.rows('SELECT * FROM thread');
      return rows.any((r) => r['id'].toString() == 'thread:e2e2');
    }, const Duration(seconds: 5));

    await client.update('thread', 'thread:e2e2', {'title': 'v2'});
    await _waitFor(() async {
      final rows = await verifier.rows('SELECT * FROM thread');
      return rows.any((r) => r['title'] == 'v2');
    }, const Duration(seconds: 5));

    final rows = await verifier.rows('SELECT * FROM thread');
    expect(rows.firstWhere((r) => r['id'].toString() == 'thread:e2e2')['title'],
        'v2');
  });
}

/// A WebSocket client that signs in as root on connect (so the up-path's
/// writes are permitted on the bare server).
class _RootClient extends WebSocketSurrealClient {
  _RootClient(this._endpoint);
  final String _endpoint;

  @override
  Future<void> connect(String endpoint) async {
    await super.connect(_endpoint);
    await signin({'user': 'root', 'pass': 'root'});
  }
}

/// Root-authenticated client used by the test to read/clean remote state.
class _RootVerifier extends WebSocketSurrealClient {
  @override
  Future<void> connect(String endpoint) async {
    await super.connect(endpoint);
    await signin({'user': 'root', 'pass': 'root'});
  }

  Future<List<Map<String, dynamic>>> rows(String sql) async {
    try {
      final results = await query(sql);
      final first = results.isNotEmpty ? results.first : null;
      return (first as List?)?.cast<Map<String, dynamic>>() ?? [];
    } catch (e) {
      // v3 errors on SELECT from a not-yet-created table; treat as empty.
      if (e.toString().contains('does not exist')) return [];
      rethrow;
    }
  }
}

Future<void> _waitFor(Future<bool> Function() pred, Duration timeout) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    if (await pred()) return;
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
}
