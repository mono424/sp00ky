import 'package:spooky_core/spooky_core.dart';
import 'package:test/test.dart';

void main() {
  group('QueryBuilder SURQL generation', () {
    test('default select all', () {
      final (sql, vars) = QueryBuilder('thread').build();
      expect(sql, 'SELECT * FROM thread;');
      expect(vars, isEmpty);
    });

    test('explicit select fields', () {
      final (sql, _) =
          QueryBuilder('thread').select(['title', 'author']).build();
      expect(sql, 'SELECT title, author FROM thread;');
    });

    test('equality where', () {
      final (sql, vars) =
          QueryBuilder('thread').where({'published': true}).build();
      expect(sql, 'SELECT * FROM thread WHERE published = \$published;');
      expect(vars, {'published': true});
    });

    test('operator where', () {
      final (sql, vars) =
          QueryBuilder('thread').where({'age': QueryOp('>', 18)}).build();
      expect(sql, 'SELECT * FROM thread WHERE age > \$age;');
      expect(vars, {'age': 18});
    });

    test('swapped operator where', () {
      final (sql, _) = QueryBuilder('thread')
          .where({'tags': QueryOp('CONTAINS', 'x', swap: true)}).build();
      expect(sql, 'SELECT * FROM thread WHERE \$tags CONTAINS tags;');
    });

    test('multiple conditions are AND-joined', () {
      final (sql, vars) = QueryBuilder('thread')
          .where({'published': true, 'score': QueryOp('>=', 10)}).build();
      expect(sql,
          'SELECT * FROM thread WHERE published = \$published AND score >= \$score;');
      expect(vars, {'published': true, 'score': 10});
    });

    test('orderBy + limit + offset', () {
      final (sql, _) = QueryBuilder('thread')
          .orderBy('createdAt', 'DESC')
          .limit(10)
          .offset(5)
          .build();
      expect(sql,
          'SELECT * FROM thread ORDER BY createdAt DESC LIMIT 10 START 5;');
    });

    test('one() forces LIMIT 1', () {
      final (sql, _) = QueryBuilder('thread').one().build();
      expect(sql, 'SELECT * FROM thread LIMIT 1;');
    });

    test('full clause ordering: WHERE then ORDER BY then LIMIT', () {
      final (sql, _) = QueryBuilder('thread')
          .where({'published': true})
          .orderBy('createdAt', 'ASC')
          .limit(3)
          .build();
      expect(
        sql,
        'SELECT * FROM thread WHERE published = \$published '
        'ORDER BY createdAt ASC LIMIT 3;',
      );
    });
  });

  group('QueryBuilder integration with the client', () {
    late Sp00kyClient client;
    setUp(() async {
      client = Sp00kyClient(Sp00kyConfig(
        database: const DatabaseConfig(namespace: 't', database: 't'),
        schema: {
          'thread': {'columns': <String, dynamic>{}}
        },
        schemaSurql:
            'DEFINE TABLE thread SCHEMAFULL PERMISSIONS FOR select WHERE true;',
      ));
      await client.init();
    });
    tearDown(() => client.close());

    test('query(table).stream() registers and emits matching records',
        () async {
      final stream = await client.query('thread').stream();
      final emissions = <List<Map<String, dynamic>>>[];
      final sub = stream.listen(emissions.add);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      await client.create('thread:a', {'title': 'hi'});
      await Future<void>.delayed(const Duration(milliseconds: 150));

      expect(emissions.last.map((r) => r['id']), contains('thread:a'));
      await sub.cancel();
    });
  });
}
