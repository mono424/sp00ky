import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/database/local_migrator.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:test/test.dart';

void main() {
  final logger = SpookyLogger.root('test');
  late LocalDatabaseService db;
  late LocalMigrator migrator;

  const schemaA =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;';
  const schemaB =
      'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;\n'
      'DEFINE TABLE note SCHEMAFULL PERMISSIONS FOR select WHERE true;';

  setUp(() {
    db = LocalDatabaseService.open(logger);
    db.provision();
    migrator = LocalMigrator(db, logger);
  });
  tearDown(() => db.close());

  test('first provision records the schema hash', () async {
    expect(db.latestSchemaHash(), isNull);
    await migrator.provision(schemaA);
    expect(db.latestSchemaHash(), isNotNull);
  });

  test('re-provisioning the same schema is a no-op (keeps local data)',
      () async {
    await migrator.provision(schemaA);
    db.create('thread:a', {'title': 'x', '_00_rv': 1});
    db.putQueryConfig('_00_query:h', {'surql': 'SELECT * FROM thread'});

    await migrator.provision(schemaA); // same schema -> no wipe

    expect(db.getById('thread:a'), isNotNull);
    expect(db.getQueryConfig('_00_query:h'), isNotNull);
  });

  test('schema upgrades and rollbacks preserve cached rows and pending writes',
      () async {
    await migrator.provision(schemaA);
    db.create('thread:a', {'title': 'x', '_00_rv': 1});
    db.putQueryConfig('_00_query:h', {'surql': 'SELECT * FROM thread'});
    db.putMutation('_00_pending_mutations:1', {'mutationType': 'create'});
    db.kvSet('_00_stream_processor_state', 'STALE-STATE');
    db.kvSet('sp00ky_auth_token', 'tok'); // independent of data schema

    await migrator.provision(schemaB);

    expect(db.getById('thread:a'), isNotNull);
    expect(db.getQueryConfig('_00_query:h'), isNotNull);
    expect(db.getAllMutations(), hasLength(1));
    expect(db.kvGet('_00_stream_processor_state'), isNull);
    // Auth token must survive a data-schema migration.
    expect(db.kvGet('sp00ky_auth_token'), 'tok');
    await migrator.provision(schemaA);
    expect(db.getById('thread:a'), isNotNull);
    expect(db.getAllMutations(), hasLength(1));
  });

  test('schema hash is a stable SHA-1 hex of the schema text', () async {
    await migrator.provision(schemaA);
    final h1 = db.latestSchemaHash();
    expect(h1, hasLength(40)); // SHA-1 hex
    expect(h1, schemaSha1(schemaA));
  });
}
