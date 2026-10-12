@Tags(['integration'])
library;

import 'dart:async';
import 'dart:io';

import 'package:spooky_core/src/client/services.dart' show hashOfEdge;
import 'package:spooky_core/src/modules/ref_tables.dart';
import 'package:spooky_core/src/query/sql.dart';
import 'package:spooky_core/src/surreal/remote_client.dart';
import 'package:test/test.dart';

/// Every signed-out session shares `_00_list_ref_anon`. Each one's LIVE must
/// carry only its own edges, including after a reconnect, when its views'
/// edges still name the socket that registered them.
///
/// Runs against a real SurrealDB, because the filter rests on how SurrealDB
/// evaluates a LIVE WHERE (`session::id()` in the subscriber's session, a
/// bound param kept with the LIVE). Boot one first, e.g.:
///   docker run -d --name spooky-surreal-it -p 18011:8000 \
///     surrealdb/surrealdb:v3.1.5 start --user root --pass root --allow-all memory
///
/// Needs SurrealDB 3.1 or later: 3.0.x sends no LIVE notification for a write
/// that follows a `LET` in the same request, which is how the SSP writes every
/// edge (and how these tests write them).
///
/// Override the endpoint with SURREAL_IT_ENDPOINT. Skips if unreachable. The
/// TypeScript twin is `packages/core/src/query/list-ref-live.integration.test.ts`.
void main() {
  final endpoint =
      Platform.environment['SURREAL_IT_ENDPOINT'] ?? 'ws://127.0.0.1:18011';
  final database = 'anon_live_${DateTime.now().microsecondsSinceEpoch}';

  late WebSocketSurrealClient root;
  final open = <WebSocketSurrealClient>[];
  // `markTestSkipped` in setUp does not stop the body; each test checks this.
  var reachable = false;

  // What the SSP and the CLI define (`ensure_anon_table`, `_00_query`).
  const schema = '''
DEFINE TABLE OVERWRITE item SCHEMALESS PERMISSIONS FOR select WHERE true;
DEFINE TABLE OVERWRITE _00_query SCHEMALESS
  PERMISSIONS FOR select, create, update, delete
    WHERE auth_id = 'anon' OR (\$auth.id != NONE AND auth_id = <string>\$auth.id);
DEFINE TABLE OVERWRITE _00_list_ref_anon SCHEMALESS
  PERMISSIONS FOR select WHERE true, FOR create, update WHERE false, FOR delete WHERE false;
DEFINE FIELD OVERWRITE in ON TABLE _00_list_ref_anon TYPE record;
DEFINE FIELD OVERWRITE out ON TABLE _00_list_ref_anon TYPE record;
DEFINE FIELD OVERWRITE clientId ON TABLE _00_list_ref_anon TYPE string;
DEFINE FIELD OVERWRITE auth_id ON TABLE _00_list_ref_anon TYPE string DEFAULT '';
DEFINE FIELD OVERWRITE version ON TABLE _00_list_ref_anon TYPE int;
CREATE item:1 SET name = 'one';
CREATE item:2 SET name = 'two';
CREATE item:3 SET name = 'three';
''';

  setUp(() async {
    root = WebSocketSurrealClient();
    reachable = false;
    try {
      await root.connect(endpoint).timeout(const Duration(seconds: 3));
      await root.signin({'user': 'root', 'pass': 'root'});
      await root.use(namespace: 'it', database: database);
      await root.query(schema);
      reachable = true;
    } catch (e) {
      await root.close();
      markTestSkipped('SurrealDB not reachable at $endpoint: $e');
      return;
    }
  });

  tearDown(() async {
    if (!reachable) return;
    for (final c in open) {
      await c.close();
    }
    open.clear();
    try {
      await root.query('REMOVE DATABASE $database');
    } catch (_) {}
    await root.close();
  });

  /// A signed-out socket and its `session::id()`.
  Future<(WebSocketSurrealClient, String)> guest() async {
    final c = WebSocketSurrealClient();
    open.add(c);
    await c.connect(endpoint);
    await c.use(namespace: 'it', database: database);
    final sid = (await c.query('RETURN <string>session::id()')).first as String;
    return (c, sid);
  }

  /// Open the client's anonymous LIVE and collect what it hears.
  Future<List<LiveMessage>> listen(WebSocketSurrealClient c,
      {List<String> earlier = const []}) async {
    final stmt =
        listRefLive(anonListRefTable, inlineBodies: true, sessionIds: earlier);
    final (_, stream) = await c.live(stmt.sql, stmt.vars);
    final got = <LiveMessage>[];
    stream.listen(got.add);
    return got;
  }

  // The SSP's own statements (`build_edge_batch`): the edge takes the
  // view's clientId at write time.
  const view = r"LET $from0 = type::record('_00_query', $q); "
      r'LET $cid0 = (SELECT VALUE clientId FROM $from0 LIMIT 1)[0]; ';
  Future<void> register(String q, String sid) => root.query(
      r"UPSERT type::record('_00_query', $q) SET clientId = $sid, auth_id = 'anon'",
      {'q': q, 'sid': sid});
  Future<void> relate(String q, String item) => root.query(
      '$view RELATE \$from0->_00_list_ref_anon->$item '
      r"SET version = 1, clientId = $cid0, auth_id = 'anon'",
      {'q': q});
  Future<void> bump(String q, String item, int version,
          {bool restamp = true}) =>
      root.query(
          '$view UPDATE (SELECT VALUE id FROM $item<-_00_list_ref_anon WHERE in = \$from0) '
          'SET version = $version${restamp ? r', clientId = $cid0 ?? clientId' : ''} RETURN NONE',
          {'q': q});
  Future<void> remove(String q, String item) => root.query(
      '$view DELETE (SELECT VALUE id FROM $item<-_00_list_ref_anon WHERE in = \$from0)',
      {'q': q});

  String heard(List<LiveMessage> got) => got
      .map((m) =>
          '${m.action} ${hashOfEdge(m.value)} ${(m.value['out'] as Map)['name']}')
      .join(', ');

  test('two anonymous sessions do not hear each other\'s edges', () async {
    if (!reachable) return;
    final (a, sidA) = await guest();
    final (b, sidB) = await guest();
    await register('qa', sidA);
    await register('qb', sidB);
    final gotA = await listen(a);
    final gotB = await listen(b);

    await relate('qa', 'item:1');
    await relate('qb', 'item:2');
    await bump('qa', 'item:1', 2);
    await _waitFor(() => gotA.length >= 2 && gotB.isNotEmpty);
    await Future<void>.delayed(const Duration(milliseconds: 300));

    // The body still rides along (`FETCH out`).
    expect(heard(gotA), 'CREATE qa one, UPDATE qa one');
    expect(heard(gotB), 'CREATE qb two');
  });

  test(
      'after a reconnect the new socket still hears the edges its old one '
      'registered, and a bystander hears none of it', () async {
    if (!reachable) return;
    final (a, sidA) = await guest();
    final (b, sidB) = await guest();
    await register('qa', sidA);
    await register('qb', sidB);
    await relate('qa', 'item:1');
    await relate('qa', 'item:2');
    final gotB = await listen(b);

    // New socket, new session id. The view stays registered under the old
    // one until the client re-registers it.
    await a.close();
    final (a2, sidA2) = await guest();
    final gotA2 = await listen(a2, earlier: [sidA]);

    // An SSP that does not restamp: the edge keeps naming the old socket.
    await bump('qa', 'item:1', 2, restamp: false);
    await _waitFor(() => gotA2.isNotEmpty);
    expect(heard(gotA2), 'UPDATE qa one');

    // Re-registered: the view, and from now on each edge it bumps, names the
    // new socket. An edge nothing bumped still names the old one when it goes.
    await register('qa', sidA2);
    await bump('qa', 'item:1', 3);
    await remove('qa', 'item:2');
    await relate('qa', 'item:3');
    await _waitFor(() => gotA2.length >= 4);
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(heard(gotA2),
        'UPDATE qa one, UPDATE qa one, DELETE qa two, CREATE qa three');
    expect(gotB, isEmpty);
  });
}

Future<void> _waitFor(bool Function() pred,
    [Duration timeout = const Duration(seconds: 3)]) async {
  final deadline = DateTime.now().add(timeout);
  while (!pred() && DateTime.now().isBefore(deadline)) {
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
}
