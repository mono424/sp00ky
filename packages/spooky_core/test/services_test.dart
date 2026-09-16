import 'package:spooky_core/src/ffi/stream_update.dart';
import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/stream_processor/permission_extractor.dart';
import 'package:spooky_core/src/services/stream_processor/stream_processor_service.dart';
import 'package:test/test.dart';

void main() {
  final logger = SpookyLogger.root('test');

  group('extractTablePermissions', () {
    test('FULL / no clause / NONE / WHERE body', () {
      const schema = '''
        DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE id = \$auth.id;
        DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE published = true FOR update WHERE false;
        DEFINE TABLE public SCHEMALESS PERMISSIONS FULL;
        DEFINE TABLE secret SCHEMAFULL PERMISSIONS NONE;
        DEFINE TABLE plain SCHEMAFULL;
      ''';
      final perms = extractTablePermissions(schema);
      expect(perms['user'], 'id = \$auth.id');
      expect(perms['thread'], 'published = true');
      expect(perms['public'], 'true');
      expect(perms['secret'], 'false');
      expect(perms['plain'], 'true');
    });

    test('IF NOT EXISTS table name is parsed', () {
      const schema =
          'DEFINE TABLE IF NOT EXISTS note SCHEMAFULL PERMISSIONS FOR select WHERE true;';
      final perms = extractTablePermissions(schema);
      expect(perms['note'], 'true');
    });

    test('-- section-header comments before a table do not hide it', () {
      // Comment lines aren't `;`-terminated, so a `-- HEADER` above a table
      // lands at the front of the split chunk; the parser must strip it or the
      // table is skipped and left default-deny.
      const schema = '''
        -- ##############
        -- GAME TABLE
        -- ##############
        DEFINE TABLE game SCHEMAFULL
          PERMISSIONS
            FOR select WHERE true
            FOR update, delete, create WHERE \$access = "account" AND owner = \$auth.id
        ;
      ''';
      final perms = extractTablePermissions(schema);
      expect(perms['game'], 'true');
    });
  });

  group('LocalDatabaseService (sqlite)', () {
    late LocalDatabaseService db;
    setUp(() {
      db = LocalDatabaseService.open(logger);
      db.provision();
    });
    tearDown(() => db.close());

    test('create / getById / getAll', () {
      db.create('thread:a', {'title': 'hello', '_00_rv': 1});
      final got = db.getById('thread:a');
      expect(got!['title'], 'hello');
      expect(got['id'], 'thread:a');
      expect(db.getAll('thread'), hasLength(1));
    });

    test('upsertMerge preserves omitted local-only fields', () {
      db.create(
          'thread:a', {'title': 'hello', '_00_crdt': 'blob', '_00_rv': 1});
      db.upsertMerge('thread:a', {'title': 'updated', '_00_rv': 2});
      final got = db.getById('thread:a')!;
      expect(got['title'], 'updated');
      expect(got['_00_crdt'], 'blob'); // preserved
    });

    test('incrementRv and delete', () {
      db.create('thread:a', {'title': 'x', '_00_rv': 1});
      db.incrementRv('thread:a');
      expect(db.getById('thread:a')!['_00_rv'], 2);
      db.delete('thread:a');
      expect(db.getById('thread:a'), isNull);
    });

    test('query config round-trip', () {
      db.putQueryConfig('_00_query:h', {'surql': 'SELECT * FROM thread'});
      expect(
          db.getQueryConfig('_00_query:h')!['surql'], 'SELECT * FROM thread');
    });

    test('legacy full-circuit state is dropped', () {
      db.kvSet('_00_stream_processor_state', 'x' * 1024);
      db.dropLegacyStreamState();
      expect(db.kvGet('_00_stream_processor_state'), isNull);
      db.dropLegacyStreamState(); // idempotent
    });

    test('tx rolls back on throw', () {
      expect(
        () => db.tx(() {
          db.create('thread:a', {'title': 'x', '_00_rv': 1});
          throw StateError('boom');
        }),
        throwsStateError,
      );
      expect(db.getById('thread:a'), isNull);
    });
  });

  group('StreamProcessorService', () {
    test('seeds permissions then registers + ingests through receivers',
        () async {
      final sp = StreamProcessorService(logger);
      await sp.init();
      sp.seedPermissionsFromSchema(
          'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;');

      final captured = <StreamUpdate>[];
      sp.addReceiver(_CapturingReceiver(captured.add));

      final initial = sp.registerQueryPlan(QueryPlanConfig(
        queryHash: 'q1',
        surql: 'SELECT * FROM thread',
        params: {},
        ttl: '10m',
        lastActiveAt: DateTime.utc(2026),
      ));
      expect(initial, isNotNull);

      sp.ingest(
          'thread', 'CREATE', 'thread:a', {'id': 'thread:a', 'title': 't'});
      expect(captured, isNotEmpty);
      expect(captured.last.localArray.map((e) => e.$1), contains('thread:a'));
      expect(captured.last.materializationTimeMs, isNotNull);

      await sp.close();
    });
  });
}

class _CapturingReceiver implements StreamUpdateReceiver {
  _CapturingReceiver(this._onUpdate);
  final void Function(StreamUpdate) _onUpdate;
  @override
  void onStreamUpdate(StreamUpdate update) => _onUpdate(update);
}
