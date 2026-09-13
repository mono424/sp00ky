import 'package:spooky_core/advanced.dart';
import 'dart:io';
import 'dart:typed_data';
import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';

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

  Future<InProcessSp00kyClient> open() async {
    final c = InProcessSp00kyClient(Sp00kyConfig(
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

  for (final mode in ['missing', 'corrupt', 'stale']) {
    test('$mode snapshot rebuilds from authoritative SQLite rows', () async {
      var client = await open();
      await client.create('thread:a', {'title': 'original'});
      await client.create('thread:b', {'title': 'deleted'});
      await client.checkpoint();
      await client.close();

      // Edit the actual closed store, with no client close that could replace
      // the altered snapshot. This models writes after an older checkpoint.
      final raw = LocalDatabaseService.open(SpookyLogger.root('test'),
          store: StoreType.indexeddb, path: '${dir.path}/spooky.anon.db');
      raw.provision();
      if (mode == 'missing') raw.clearSnapshot();
      if (mode == 'corrupt') raw.putSnapshot(Uint8List.fromList([1, 2, 3]));
      raw.putDoc('thread', 'thread:a', {'title': 'latest', '_00_rv': 5});
      raw.deleteDoc('thread', 'thread:b');
      raw.close();

      client = await open();
      addTearDown(client.close);
      final stream = await client.queryStream('SELECT * FROM thread', {});
      final rows = await stream.firstWhere((r) => r.isNotEmpty);
      expect(rows, hasLength(1));
      expect(rows.single['title'], 'latest');
    });
  }
}
