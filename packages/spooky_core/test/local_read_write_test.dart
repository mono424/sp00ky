import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:test/test.dart';

/// End-to-end local-first path: init -> register query -> Stream emits ->
/// create/update/delete drive reactive Stream updates through the FFI DBSP
/// processor and sqlite materialization.
void main() {
  late InProcessSp00kyClient client;

  const schemaSurql =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';

  final schema = {
    'thread': {
      'columns': {
        'title': const ColumnSchema(type: 'string'),
      },
    },
  };

  setUp(() async {
    client = InProcessSp00kyClient(Sp00kyConfig(
      database: const DatabaseConfig(namespace: 'test', database: 'test'),
      schema: schema,
      schemaSurql: schemaSurql,
    ));
    await client.init();
  });

  tearDown(() => client.close());

  test('query Stream emits initial empty then reflects a create', () async {
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    final stream = client.subscribeStream(hash);

    final emissions = <List<Map<String, dynamic>>>[];
    final sub = stream.listen(emissions.add);

    // First listen replays the (empty) current result set.
    await Future<void>.delayed(const Duration(milliseconds: 10));
    expect(emissions.first, isEmpty);

    await client.create('thread:a', {'title': 'hello'});
    await Future<void>.delayed(const Duration(milliseconds: 150));

    final latest = emissions.last;
    expect(latest, hasLength(1));
    expect(latest.first['title'], 'hello');
    expect(latest.first['id'], 'thread:a');

    await sub.cancel();
  });

  test('update is reflected in the Stream', () async {
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    await client.create('thread:b', {'title': 'first'});

    final emissions = <List<Map<String, dynamic>>>[];
    final sub = client.subscribeStream(hash).listen(emissions.add);
    await Future<void>.delayed(const Duration(milliseconds: 20));

    await client.update('thread', 'thread:b', {'title': 'second'});
    await Future<void>.delayed(const Duration(milliseconds: 150));

    final latest = emissions.last;
    expect(latest.first['title'], 'second');
    expect(latest.first['_00_rv'], 2);

    await sub.cancel();
  });

  test('delete removes the record from the Stream', () async {
    final hash = await client.queryRaw('SELECT * FROM thread', {});
    await client.create('thread:c', {'title': 'x'});

    final emissions = <List<Map<String, dynamic>>>[];
    final sub = client.subscribeStream(hash).listen(emissions.add);
    await Future<void>.delayed(const Duration(milliseconds: 20));

    await client.delete('thread', 'thread:c');
    await Future<void>.delayed(const Duration(milliseconds: 150));

    expect(emissions.last, isEmpty);
    await sub.cancel();
  });

  test('mutations are recorded in the pending-mutations outbox', () async {
    await client.create('thread:d', {'title': 'q'});
    final mutations = client.localStore.getAllMutations();
    expect(mutations, isNotEmpty);
    expect(mutations.last['mutationType'], 'create');
    expect(mutations.last['recordId'], 'thread:d');
  });
}
