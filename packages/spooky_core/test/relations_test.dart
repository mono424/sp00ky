import 'package:spooky_core/advanced.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/codegen/dart_emitter.dart';
import 'package:spooky_core/src/codegen/schema_parser.dart';
import 'package:spooky_core/src/modules/relationships.dart';
import 'package:spooky_core/src/query/relation_resolver.dart';
import 'package:spooky_core/src/services/database/local_database_service.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/persistence/memory_persistence.dart';
import 'package:spooky_core/src/utils/sort_rows.dart' show stableKey;
import 'package:test/test.dart';

/// A [RelationFetcher] straight over a store, so the resolver's own rules can
/// be tested without an engine around them.
class _StoreFetcher implements RelationFetcher {
  _StoreFetcher(this._local);
  final LocalDatabaseService _local;

  @override
  Future<List<Map<String, dynamic>>> fetchRelation({
    required String table,
    required String matchField,
    required List<Object?> keys,
  }) async {
    final wanted = {for (final key in keys) stableKey(key)};
    return [
      for (final row in _local.getAll(table))
        if (wanted.contains(stableKey(row[matchField]))) row,
    ];
  }
}

/// `.related()` end to end: relationships derived from the schema, the emitted
/// correlated subqueries, and the local-cache resolver that attaches joined rows
/// (the Dart stand-in for SurrealQL's nested projections).
void main() {
  final logger = SpookyLogger.root('test');

  const schemaSurql = '''
DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD title ON thread TYPE string;
DEFINE FIELD author ON thread TYPE record<user>;
DEFINE TABLE comment SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD body ON comment TYPE string;
DEFINE FIELD score ON comment TYPE int;
DEFINE FIELD thread ON comment TYPE record<thread>;
DEFINE FIELD author ON comment TYPE record<user>;
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;
DEFINE FIELD name ON user TYPE string;
''';

  /// The runtime schema map a generated client ships, including relationships.
  final schema = <String, dynamic>{
    'thread': {
      'columns': {
        'title': const ColumnSchema(type: 'string'),
        'author': const ColumnSchema(type: 'record', recordId: true),
      },
    },
    'comment': {
      'columns': {
        'body': const ColumnSchema(type: 'string'),
        'score': const ColumnSchema(type: 'int'),
        'thread': const ColumnSchema(type: 'record', recordId: true),
        'author': const ColumnSchema(type: 'record', recordId: true),
      },
    },
    'user': {
      'columns': {'name': const ColumnSchema(type: 'string')},
    },
    'relationships': [
      {'from': 'thread', 'field': 'author', 'to': 'user', 'cardinality': 'one'},
      {
        'from': 'comment',
        'field': 'thread',
        'to': 'thread',
        'cardinality': 'one'
      },
      {
        'from': 'comment',
        'field': 'author',
        'to': 'user',
        'cardinality': 'one'
      },
      {
        'from': 'user',
        'field': 'threads',
        'to': 'thread',
        'cardinality': 'many'
      },
      {
        'from': 'thread',
        'field': 'comments',
        'to': 'comment',
        'cardinality': 'many'
      },
      {
        'from': 'user',
        'field': 'comments',
        'to': 'comment',
        'cardinality': 'many'
      },
    ],
  };

  group('deriveRelationships', () {
    late List<SchemaRelationship> rels;
    setUp(() {
      rels = deriveRelationships(parseSchema(schemaSurql));
    });

    SchemaRelationship? find(String from, String field) =>
        findRelationship(rels, from, field);

    test('a record<x> field becomes a forward one-relation', () {
      final rel = find('thread', 'author');
      expect(rel, isNotNull);
      expect(rel!.to, 'user');
      expect(rel.cardinality, 'one');
      expect(rel.foreignKeyField, 'author',
          reason: 'a one-relation reads the parent field of the same name');
    });

    test('each forward relation adds a pluralized reverse many-relation', () {
      final rel = find('thread', 'comments');
      expect(rel, isNotNull);
      expect(rel!.to, 'comment');
      expect(rel.cardinality, 'many');
      expect(rel.foreignKeyField, 'thread',
          reason:
              'a many-relation matches the child field named after the parent');
      expect(find('user', 'threads')?.to, 'thread');
      expect(find('user', 'comments')?.to, 'comment');
    });

    test('a record field pointing at an unknown table is skipped', () {
      final tables = parseSchema('''
DEFINE TABLE thread SCHEMAFULL;
DEFINE FIELD owner ON thread TYPE record<ghost>;
''');
      expect(deriveRelationships(tables), isEmpty);
    });

    test('an explicit field wins over the derived reverse name', () {
      // `user` already declares a `threads` field, so the reverse relation must
      // not shadow it.
      final tables = parseSchema('''
DEFINE TABLE thread SCHEMAFULL;
DEFINE FIELD author ON thread TYPE record<user>;
DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD threads ON user TYPE string;
''');
      final derived = deriveRelationships(tables);
      expect(findRelationship(derived, 'user', 'threads'), isNull);
      expect(findRelationship(derived, 'thread', 'author'), isNotNull);
    });

    test('pluralization matches the CLI rules', () {
      expect(pluralizeTableName('user'), 'users');
      expect(pluralizeTableName('person'), 'people');
      expect(pluralizeTableName('child'), 'children');
      expect(pluralizeTableName('mouse'), 'mice');
      expect(pluralizeTableName('thread'), 'threads');
      expect(pluralizeTableName('class'), 'classes');
      expect(pluralizeTableName('match'), 'matches');
      expect(pluralizeTableName('box'), 'boxes');
      expect(pluralizeTableName('category'), 'categories');
      expect(pluralizeTableName('play'), 'plays');
      expect(pluralizeTableName('shelf'), 'shelves');
      expect(pluralizeTableName('life'), 'lives');
    });
  });

  group('subquery emission', () {
    QueryBuilder builder(String table) =>
        QueryBuilder(table, schema: schema, logger: logger);

    test('a one-relation correlates on the parent key and takes the first row',
        () {
      final (sql, _) = builder('thread').related('author').build();
      expect(
        sql,
        'SELECT *, (SELECT * FROM user WHERE id=\$parent.author LIMIT 1)[0] AS author FROM thread;',
      );
    });

    test('a many-relation correlates on the child back-reference', () {
      final (sql, _) = builder('thread').related('comments').build();
      expect(
        sql,
        'SELECT *, (SELECT * FROM comment WHERE thread=\$parent.id) AS comments FROM thread;',
      );
    });

    test('a modifier shapes select / where / orderBy / limit', () {
      final (sql, _) = builder('thread')
          .related(
            'comments',
            (c) => c
                .select(['id', 'body'])
                .where({'score': 5})
                .orderBy('score', 'DESC')
                .limit(3),
          )
          .build();
      expect(
        sql,
        'SELECT *, (SELECT id, body FROM comment WHERE thread=\$parent.id AND score = 5 ORDER BY score DESC LIMIT 3) AS comments FROM thread;',
      );
    });

    test('a record-id literal in a sub-where is not quoted', () {
      final (sql, _) = builder('thread')
          .related('comments', (c) => c.where({'author': 'user:u1'}))
          .build();
      expect(sql, contains('AND author = user:u1'));
      expect(sql, isNot(contains('"user:u1"')));
    });

    test('nested relations nest their subqueries', () {
      final (sql, _) = builder('thread')
          .related('comments', (c) => c.related('author'))
          .build();
      expect(
        sql,
        'SELECT *, (SELECT *, (SELECT * FROM user WHERE id=\$parent.author LIMIT 1)[0] AS author '
        'FROM comment WHERE thread=\$parent.id) AS comments FROM thread;',
      );
    });

    test('relations compose with the parent where / order / window', () {
      final (sql, vars) = builder('thread')
          .related('author')
          .where({'title': 'x'})
          .orderBy('title')
          .limit(2)
          .offset(2)
          .build();
      expect(sql, startsWith('SELECT *, (SELECT * FROM user'));
      expect(sql, contains('FROM thread WHERE title = \$title'));
      expect(sql, endsWith('ORDER BY title ASC LIMIT 2 START 2;'));
      expect(vars, {'title': 'x'});
    });

    test('an unknown relation is skipped, not fatal', () {
      final (sql, _) =
          builder('thread').related('nope').related('author').build();
      expect(sql, isNot(contains('nope')));
      expect(sql, contains('AS author'),
          reason: 'a sibling relation must survive an unknown one');
    });

    test('a repeated relation is added once', () {
      final b = builder('thread').related('author').related('author');
      expect(b.relations, hasLength(1));
    });

    test('a schema without relationships skips every relation', () {
      final (sql, _) = QueryBuilder('thread', schema: const {}, logger: logger)
          .related('author')
          .build();
      expect(sql, 'SELECT * FROM thread;');
    });
  });

  group('resolver', () {
    late LocalDatabaseService local;
    late _StoreFetcher fetcher;

    setUp(() {
      local = LocalDatabaseService.open(logger)..provision();
      fetcher = _StoreFetcher(local);
      local.create('user:u1', {'name': 'Ada'});
      local.create('user:u2', {'name': 'Linus'});
      local.create('thread:t1', {'title': 'first', 'author': 'user:u1'});
      local.create('thread:t2', {'title': 'second', 'author': 'user:u2'});
      local.create('thread:t3', {'title': 'orphan'});
      for (final (id, thread, score, author) in [
        ('comment:c1', 'thread:t1', 3, 'user:u1'),
        ('comment:c2', 'thread:t1', 1, 'user:u2'),
        ('comment:c3', 'thread:t1', 2, 'user:u1'),
        ('comment:c4', 'thread:t2', 9, 'user:u2'),
      ]) {
        local.create(id, {
          'body': id,
          'thread': thread,
          'score': score,
          'author': author,
        });
      }
    });
    tearDown(() => local.close());

    RelationPlan plan(
      String alias,
      String table,
      String cardinality,
      String fk, {
      Map<String, Object?> where = const {},
      List<(String, String)> orderBy = const [],
      int? limit,
      List<RelationPlan> relations = const [],
    }) =>
        RelationPlan(
          alias: alias,
          table: table,
          cardinality: cardinality,
          foreignKeyField: fk,
          where: where,
          orderBy: orderBy,
          limit: limit,
          relations: relations,
        );

    List<Map<String, dynamic>> threads(
            [List<String> ids = const ['thread:t1']]) =>
        [
          for (final id in ids) {...local.getById(id)!}
        ];

    test('a one-relation attaches the single row', () async {
      final rows = threads();
      await resolveRelations(
          rows, [plan('author', 'user', 'one', 'author')], fetcher);
      expect((rows.single['author'] as Map)['name'], 'Ada');
    });

    test('a one-relation with no foreign key attaches null', () async {
      final rows = threads(['thread:t3']);
      await resolveRelations(
          rows, [plan('author', 'user', 'one', 'author')], fetcher);
      expect(rows.single['author'], isNull);
    });

    test('a many-relation attaches the matching children only', () async {
      final rows = threads(['thread:t1', 'thread:t2']);
      await resolveRelations(
          rows, [plan('comments', 'comment', 'many', 'thread')], fetcher);
      expect((rows[0]['comments'] as List).map((c) => c['id']),
          containsAll(['comment:c1', 'comment:c2', 'comment:c3']));
      expect((rows[1]['comments'] as List).map((c) => c['id']), ['comment:c4']);
    });

    test('a many-relation with no children attaches an empty list', () async {
      final rows = threads(['thread:t3']);
      await resolveRelations(
          rows, [plan('comments', 'comment', 'many', 'thread')], fetcher);
      expect(rows.single['comments'], isEmpty);
    });

    test('order and limit apply PER parent', () async {
      final rows = threads(['thread:t1', 'thread:t2']);
      await resolveRelations(
        rows,
        [
          plan('comments', 'comment', 'many', 'thread',
              orderBy: [('score', 'desc')], limit: 2)
        ],
        fetcher,
      );
      // Top 2 of thread:t1 by score, not the global top 2 (which would be c4).
      expect((rows[0]['comments'] as List).map((c) => c['id']),
          ['comment:c1', 'comment:c3']);
      expect((rows[1]['comments'] as List).map((c) => c['id']), ['comment:c4']);
    });

    test('a sub-where filters the children', () async {
      final rows = threads();
      await resolveRelations(
        rows,
        [
          plan('comments', 'comment', 'many', 'thread',
              where: {'author': 'user:u1'})
        ],
        fetcher,
      );
      expect((rows.single['comments'] as List).map((c) => c['id']),
          containsAll(['comment:c1', 'comment:c3']));
      expect((rows.single['comments'] as List), hasLength(2));
    });

    test('nested relations resolve a second level', () async {
      final rows = threads();
      await resolveRelations(
        rows,
        [
          plan('comments', 'comment', 'many', 'thread',
              orderBy: [('score', 'asc')],
              relations: [plan('author', 'user', 'one', 'author')])
        ],
        fetcher,
      );
      final comments = rows.single['comments'] as List;
      expect(((comments.first as Map)['author'] as Map)['name'], 'Linus');
    });

    test('the alias lands last in key order', () async {
      final rows = threads();
      await resolveRelations(
          rows, [plan('author', 'user', 'one', 'author')], fetcher);
      expect(rows.single.keys.last, 'author');
    });

    test('nesting past the depth cap throws RelationCycleError', () async {
      // Self-join on `id`, so every level keeps matching and the recursion can
      // actually reach the cap (a chain that runs out of children just stops).
      RelationPlan chain(int depth) => plan(
            'self',
            'thread',
            'one',
            'id',
            relations: depth == 0 ? const [] : [chain(depth - 1)],
          );
      expect(
        () => resolveRelations(threads(), [chain(maxRelationDepth)], fetcher),
        throwsA(isA<RelationCycleError>()),
      );
      // One level under the cap is fine.
      expect(
        () =>
            resolveRelations(threads(), [chain(maxRelationDepth - 2)], fetcher),
        returnsNormally,
      );
    });

    test('an empty parent set or plan is a no-op', () async {
      await resolveRelations(
          [], [plan('author', 'user', 'one', 'author')], fetcher);
      final rows = threads();
      await resolveRelations(rows, const [], fetcher);
      // `author` is a real column, so it stays the raw foreign key rather than
      // being replaced by a resolved row.
      expect(rows.single['author'], 'user:u1');
    });
  });

  group('the engine resolves a query\'s relations', () {
    late InProcessSp00kyClient client;

    setUp(() async {
      client = InProcessSp00kyClient(
        Sp00kyConfig(
          database: const DatabaseConfig(namespace: 't', database: 't'),
          schema: schema,
          schemaSurql: schemaSurql,
          persistenceClient: MemoryPersistenceClient(),
        ),
      );
      await client.init();
    });
    tearDown(() => client.close());

    test('a registered query attaches its relations to every row', () async {
      await client.create('user:u1', {'name': 'Ada'});
      await client.create('thread:t1', {'title': 'first', 'author': 'user:u1'});
      await client.create('comment:c1', {
        'body': 'hi',
        'thread': 'thread:t1',
        'score': 1,
        'author': 'user:u1',
      });

      final hash = await client
          .query('thread')
          .related('author')
          .related('comments')
          .run();
      await Future<void>.delayed(const Duration(milliseconds: 250));

      final row = client.state.queries[hash]!.records.single;
      expect((row['author'] as Map)['name'], 'Ada');
      expect((row['comments'] as List).single['id'], 'comment:c1');
    });

    test('materializing does not mutate the cached document', () async {
      await client.create('user:u1', {'name': 'Ada'});
      await client.create('thread:t1', {'title': 'first', 'author': 'user:u1'});

      final hash = await client.query('thread').related('comments').run();
      await Future<void>.delayed(const Duration(milliseconds: 250));

      expect(client.state.queries[hash]!.records.single['comments'], isEmpty);
      expect(client.localStore.getById('thread:t1')!.containsKey('comments'),
          isFalse,
          reason: 'the alias must not be written back into the store');
    });
  });
}
