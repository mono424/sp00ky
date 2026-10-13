import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

import 'fake_remote.dart';

/// Preload is a registered query nobody subscribes to.
///
/// It replaces the old one-shot fetch plus `_00_preload` freshness marker: the
/// registration IS the freshness path, so a preloaded query and the view that
/// later mounts it are the same entry, and the second one paints from cache.
void main() {
  const schemaSurql =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';
  final schema = {
    'thread': {
      'columns': {'title': const ColumnSchema(type: 'string')},
    },
  };

  late FakeRemote remote;
  late InProcessSp00kyClient client;

  // Signed out, so the server sends membership only with anonymous live
  // queries on; these clients have them unless a test says otherwise.
  Future<InProcessSp00kyClient> start({bool anonLive = true}) async {
    final c = InProcessSp00kyClient(
      Sp00kyConfig(
        database: const DatabaseConfig(
            endpoint: 'ws://x', namespace: 't', database: 't'),
        schema: schema,
        schemaSurql: schemaSurql,
        persistenceClient: MemoryPersistenceClient(),
        refSyncIntervalMs: 3600000,
        enableAnonymousLiveQueries: anonLive,
      ),
      remoteClient: remote,
    );
    await c.init();
    await settle();
    return c;
  }

  setUp(() async {
    remote = FakeRemote();
    remote.records['thread:a'] = {
      'id': 'thread:a',
      'title': 'preloaded',
      '_00_rv': 1,
    };
    remote.defaultMembership = [('thread:a', 1)];
    client = await start();
  });
  tearDown(() => client.close());

  test('a cold preload resolves only once its rows are local', () async {
    await client.preload('SELECT * FROM thread', const {});
    expect(client.localStore.getById('thread:a'), isNotNull,
        reason: 'preload is awaited: the caller can hold the UI on it');
  });

  test('the query that mounts afterwards is the same entry, already resolved',
      () async {
    await client.preload('SELECT * FROM thread', const {});
    final before = client.state.queries.length;

    final hash = await client.queryRaw('SELECT * FROM thread', const {});
    expect(client.state.queries, hasLength(before),
        reason: 'the preloaded entry is reused, not registered again');
    expect(client.isQueryAuthoritative(hash), isTrue);
    expect(client.state.queries[hash]!.records.single['title'], 'preloaded');
  });

  test('a second preload of the same query returns immediately', () async {
    await client.preload('SELECT * FROM thread', const {});
    final sent = remote.queries.length;
    await client.preload('SELECT * FROM thread', const {});
    expect(remote.queries.length, sent,
        reason: 'an entry that already exists needs no round trip');
  });

  test(
      'signed out without anonymous live queries, a cold preload returns '
      'at once: no membership can reach the session, so waiting would hang',
      () async {
    // As on the server: a guest may not read the global `_00_list_ref`, so
    // the view reports one row and never shows it.
    remote.hiddenEdgeTables.add('_00_list_ref');
    final signedOut = await start(anonLive: false);
    addTearDown(signedOut.close);
    await signedOut.preload(
        'SELECT * FROM thread', const {}).timeout(const Duration(seconds: 2));
    expect(signedOut.state.queries, hasLength(1),
        reason: 'it is still registered, for when the session signs in');
  });

  test('a preload whose registration fails reports rather than hanging',
      () async {
    remote.offline = true;
    await expectLater(
      client.preload('SELECT * FROM thread', const {'x': 1}),
      throwsA(isA<PreloadFailedError>()),
    );
  });
}
