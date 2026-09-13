import 'dart:io';

import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:test/test.dart';

/// The circuit is filled from the LOCAL store on boot, so a warm client starts
/// with the rows it already has and the first sync diff is a real delta rather
/// than "fetch everything".
void main() {
  const schemaSurql =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';
  final schema = {
    'thread': {
      'columns': {'title': const ColumnSchema(type: 'string')},
    },
  };

  late Directory dir;
  setUp(() => dir = Directory.systemTemp.createTempSync('spooky-prime'));
  tearDown(() => dir.deleteSync(recursive: true));

  Future<Sp00kyClient> open() async {
    final c = Sp00kyClient(Sp00kyConfig(
      database: DatabaseConfig(
        namespace: 't',
        database: 't',
        store: StoreType.indexeddb,
        localDbPath: '${dir.path}/spooky.db',
      ),
      schema: schema,
      schemaSurql: schemaSurql,
      persistenceClient: MemoryPersistenceClient(),
    ));
    await c.init();
    return c;
  }

  test('a restarted client sees its rows without touching the network',
      () async {
    var client = await open();
    await client.create('thread:a', {'title': 'kept'});
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(client.localStore.getById('thread:a'), isNotNull);
    await client.close();

    // A snapshot was written on close, so the next boot restores from it.
    client = await open();
    addTearDown(client.close);
    expect(client.localStore.getSnapshot(), isNotNull);
    expect(client.state.primed, isTrue);

    // The query's local window comes from the primed circuit, not the network.
    final hash = await client.queryRaw('SELECT * FROM thread', const {});
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(client.state.queries[hash]!.localArray.map((e) => e.$1),
        contains('thread:a'));
    expect(client.state.queries[hash]!.records.single['title'], 'kept');
  });

  test('a missing snapshot primes from the rows instead', () async {
    var client = await open();
    await client.create('thread:a', {'title': 'kept'});
    await Future<void>.delayed(const Duration(milliseconds: 150));
    await client.close();

    // Simulate a process that died before it could checkpoint.
    final wiped = await open();
    wiped.localStore.clearSnapshot();
    await wiped.close();

    client = await open();
    addTearDown(client.close);
    final hash = await client.queryRaw('SELECT * FROM thread', const {});
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(client.state.queries[hash]!.records.single['title'], 'kept');
  });
}
