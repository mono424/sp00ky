import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

import 'fake_remote.dart';

/// The settled contract: a query holds `fetching` across its WHOLE registration
/// and only flips to `idle` after its rows have landed. A consumer that treats
/// `idle` as "this window is complete" (a virtualized list sizing itself to a
/// short page) depends on that ordering.
void main() {
  late FakeRemote remote;
  late Sp00kyClient client;

  const schemaSurql =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';
  final schema = {
    'thread': {
      'columns': {'title': const ColumnSchema(type: 'string')},
    },
  };

  setUp(() async {
    remote = FakeRemote();
    final persistence = MemoryPersistenceClient();
    await persistence.set('sp00ky_auth_token', 'tok');
    remote.records['user:u1'] = {'id': 'user:u1'};
    client = Sp00kyClient(
      Sp00kyConfig(
        database: const DatabaseConfig(
          endpoint: 'ws://localhost:8000',
          namespace: 'test',
          database: 'test',
        ),
        schema: schema,
        schemaSurql: schemaSurql,
        persistenceClient: persistence,
        // Keep the poll from racing extra fetch cycles into these assertions.
        refSyncIntervalMs: 60000,
      ),
      remoteClient: remote,
    );
    await client.init();
    await _settle();
  });
  tearDown(() => client.close());

  test('a registration settles once its rows have landed', () async {
    remote.records['thread:a'] = {
      'id': 'thread:a',
      'title': 'from server',
      '_00_rv': 1,
    };
    remote.defaultMembership = [('thread:a', 1)];

    final hash = await client.queryRaw('SELECT * FROM thread', {});
    expect(client.isQueryAuthoritative(hash), isFalse,
        reason: 'nothing has come back from the server yet');
    expect(client.isQuerySettled(hash), isFalse);

    // Snapshot the records visible at each status change: `idle` must not
    // arrive before the fetched row is materialized.
    final observed = <(QueryStatus, int)>[];
    client.subscribeQueryStatus(
      hash,
      (s) => observed.add((s, client.state.queries[hash]?.records.length ?? 0)),
    );
    await _settle();

    expect(observed.map((o) => o.$1), contains(QueryStatus.fetching),
        reason: 'the registration holds `fetching` while it is in flight');
    expect(observed.last.$1, QueryStatus.idle);
    // `idle` alone only means "no fetch in flight"; it is `settled` that means
    // "this window is complete", because it also waits for the render.
    expect(client.isQueryAuthoritative(hash), isTrue);
    expect(client.isQuerySettled(hash), isTrue);
    expect(client.state.queries[hash]!.records, hasLength(1));
  });

  test('a confirmed-empty result is authoritative, and settles', () async {
    // The server's `_00_query` row says `rowCount: 0, state: ready`, which is
    // what tells a real empty result apart from a view that has not published
    // its edges yet.
    remote.defaultMembership = const [];

    final hash = await client.queryRaw('SELECT * FROM thread', {});
    await _settle();

    // An empty result emits nothing (the rows did not change), so a binding
    // stops loading on AUTHORITY rather than on an emission. That is what tells
    // "no rows" apart from "no answer yet".
    expect(client.isQueryAuthoritative(hash), isTrue);
    expect(client.isQuerySettled(hash), isTrue);
    expect(client.state.queries[hash]!.records, isEmpty);
  });

  test('an unpublished view is NOT read as empty', () async {
    // No `_00_query` row and no edges: the server has not answered for this
    // query at all. Reading that as "no rows" is what makes a list blank.
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    await _settle();
    expect(client.isQueryAuthoritative(hash), isFalse);
    expect(client.state.queries[hash]!.lifecycle.phase, QueryPhase.cold);
  });
}

Future<void> _settle() =>
    Future<void>.delayed(const Duration(milliseconds: 300));
