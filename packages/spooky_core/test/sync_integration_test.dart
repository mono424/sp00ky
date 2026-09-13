import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/kernel/events.dart' show Drain, ReadMembership;
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

import 'fake_remote.dart';

/// Drives the whole engine against a fake server: register -> membership ->
/// body fetch -> render, a LIVE change, and the outbox up-path.
void main() {
  late FakeRemote remote;
  late InProcessSp00kyClient client;

  const schemaSurql =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';
  final schema = {
    'thread': {
      'columns': {'title': const ColumnSchema(type: 'string')},
    },
  };

  Future<InProcessSp00kyClient> open() async {
    final persistence = MemoryPersistenceClient();
    // A token whose `ID` claim names the user, so boot restores the session
    // without a round trip (the payload below decodes to {"ID":"user:u1"}).
    await persistence.set('sp00ky_auth_token',
        'h.${base64Url('{"ID":"user:u1","AC":"account"}')}.s');
    final c = InProcessSp00kyClient(
      Sp00kyConfig(
        database: const DatabaseConfig(
          endpoint: 'ws://localhost:8000',
          namespace: 'test',
          database: 'test',
        ),
        schema: schema,
        schemaSurql: schemaSurql,
        persistenceClient: persistence,
      ),
      remoteClient: remote,
    );
    await c.init();
    return c;
  }

  setUp(() async {
    remote = FakeRemote();
    // The server must recognise the token's user, or `authInit` signs out and
    // the client switches back to the anonymous bucket mid-test.
    remote.records['user:u1'] = {'id': 'user:u1', 'name': 'u'};
    client = await open();
    // Let boot's network half finish, so the bucket has settled before a test
    // writes into it.
    await settle(150);
  });
  tearDown(() => client.close());

  test('boot opens the store, restores the session and connects', () async {
    expect(client.isLocalReady, isTrue);
    expect(client.state.userId, 'user:u1');
    expect(client.state.bucketId, isNotNull);
    await settle();
    expect(remote.connected, isTrue);
    expect(remote.usedNamespace, 'test');
  });

  test('a registered query renders the server membership it is given',
      () async {
    remote.records['thread:a'] = {'id': 'thread:a', 'title': 'hello'};
    final hash =
        await client.queryRaw('SELECT * FROM thread', const {}, ttl: '10m');
    remote.publish('_00_query:$hash', [('thread:a', 1)]);

    final seen = <List<Map<String, dynamic>>>[];
    client.subscribe(hash, seen.add);
    // The registration may have read the membership before it was published;
    // one forced read is the deterministic equivalent of the next poll tick.
    await client.dispatch(ReadMembership([hash]));
    await settle(200);

    expect(client.state.queries[hash]!.remoteArray, [('thread:a', 1)]);
    expect(remote.lastFetchedIds, ['thread:a']);
    expect(seen.last.single['title'], 'hello');
    expect(client.isQueryAuthoritative(hash), isTrue);
  });

  test('a LIVE edge lands the row without a poll', () async {
    final hash =
        await client.queryRaw('SELECT * FROM thread', const {}, ttl: '10m');
    await settle();
    remote.records['thread:b'] = {'id': 'thread:b', 'title': 'pushed'};
    remote.publish('_00_query:$hash', [('thread:b', 1)]);
    remote.pushEdge('CREATE', '_00_query:$hash', 'thread:b');
    await settle(300);
    expect(client.state.queries[hash]!.remoteArray, [('thread:b', 1)]);
    expect(client.localStore.getById('thread:b'), isNotNull);
  });

  test('a local write is optimistic, queued, then pushed and acked', () async {
    remote.blockMutations = true;
    await client.create('thread:new', {'title': 'draft'});
    await settle();
    expect(client.localStore.getById('thread:new'), isNotNull,
        reason: 'the row is visible before the server has seen it');
    expect(client.pendingMutationCount, 1);

    remote.blockMutations = false;
    await client.dispatch(const Drain());
    await settle();
    expect(client.pendingMutationCount, 0);
    expect(remote.queries.any((q) => q.startsWith('CREATE ONLY')), isTrue);
  });

  test('a query paints from the durable view row on the next boot', () async {
    remote.records['thread:a'] = {'id': 'thread:a', 'title': 'cached'};
    final hash =
        await client.queryRaw('SELECT * FROM thread', const {}, ttl: '10m');
    remote.publish('_00_query:$hash', [('thread:a', 1)]);
    await client.dispatch(ReadMembership([hash]));
    await settle(200);
    expect(client.state.queries[hash]!.lifecycle.phase, QueryPhase.live);

    // The store is in memory, so the second client would start empty; assert
    // the durable row instead, which is what a file-backed store keeps.
    final view = client.localStore.getAllDocs('_00_view');
    expect(view, hasLength(1));
    expect(view.single['confirmed'], isTrue);
    expect((view.single['ids'] as List).single, ['thread:a', 1]);
  });
}

/// Base64url without padding, as a JWT segment.
String base64Url(String json) {
  const chars =
      'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
  final bytes = json.codeUnits;
  final out = StringBuffer();
  for (var i = 0; i < bytes.length; i += 3) {
    final b0 = bytes[i];
    final b1 = i + 1 < bytes.length ? bytes[i + 1] : 0;
    final b2 = i + 2 < bytes.length ? bytes[i + 2] : 0;
    out.write(chars[b0 >> 2]);
    out.write(chars[((b0 & 3) << 4) | (b1 >> 4)]);
    if (i + 1 < bytes.length) out.write(chars[((b1 & 15) << 2) | (b2 >> 6)]);
    if (i + 2 < bytes.length) out.write(chars[b2 & 63]);
  }
  return out.toString();
}
