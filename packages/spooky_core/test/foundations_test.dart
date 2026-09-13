import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:spooky_core/src/events/event_system.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:spooky_core/src/types.dart';
import 'package:spooky_core/src/utils/duration_utils.dart';
import 'package:spooky_core/src/utils/parser.dart';
import 'package:spooky_core/src/utils/record_id_utils.dart';
import 'package:spooky_core/src/utils/surql.dart';
import 'package:test/test.dart';

void main() {
  group('parseQueryParams', () {
    const columns = {
      'title': ColumnSchema(type: 'string'),
      'allow_duplicate': ColumnSchema(type: 'bool', optional: true),
      'created_at': ColumnSchema(type: 'datetime', dateTime: true),
    };

    test('drops unset optionals rather than sending SurrealDB a NULL', () {
      // A generated model's `toJson` always writes the key, so an unset
      // `option<bool>` arrives as null. Sent as NULL the server rejects the
      // whole write; omitted, the field is NONE, which is what unset means.
      final parsed = parseQueryParams(columns, {
        'title': 'hi',
        'allow_duplicate': null,
      });
      expect(parsed.containsKey('allow_duplicate'), isFalse);
      expect(parsed['title'], 'hi');
    });

    test('keeps keys the schema does not declare', () {
      final parsed = parseQueryParams(columns, {'extra': 7});
      expect(parsed['extra'], 7);
    });

    test('coerces declared columns', () {
      final parsed =
          parseQueryParams(columns, {'created_at': '2026-09-13T00:00:00Z'});
      expect(parsed['created_at'], isA<DateTime>());
    });
  });

  group('RecordId', () {
    test('encode / parse round-trip', () {
      final r = RecordId('thread', 'abc');
      expect(encodeRecordId(r), 'thread:abc');
      final parsed = parseRecordIdString('thread:abc');
      expect(parsed.table, 'thread');
      expect(parsed.id, 'abc');
    });

    test('id part keeps embedded colons', () {
      expect(extractIdPart('thread:a:b'), 'a:b');
      expect(extractTablePart('thread:a:b'), 'thread');
      final parsed = parseRecordIdString('thread:a:b');
      expect(parsed.id, 'a:b');
    });

    test('compareRecordIds across string/RecordId', () {
      expect(compareRecordIds(RecordId('t', '1'), 't:1'), isTrue);
      expect(compareRecordIds('t:1', 't:2'), isFalse);
    });

    test('generateId strips hyphens and is 32 chars', () {
      final id = generateId();
      expect(id.contains('-'), isFalse);
      expect(id.length, 32);
    });
  });

  group('parseDuration', () {
    test('units', () {
      expect(parseDuration('10m'), 600000);
      expect(parseDuration('30s'), 30000);
      expect(parseDuration('2h'), 7200000);
    });
    test('fallbacks to 10m on bad input', () {
      expect(parseDuration('1d'), 600000); // 'd' not in [smh] -> fallback
      expect(parseDuration('nonsense'), 600000);
    });
    test('SurrealDuration delegates to string', () {
      expect(parseDuration(const SurrealDuration('5m')), 300000);
    });
  });

  group('surql builders', () {
    test('selectById / create / delete', () {
      expect(surql.selectById('id', ['*']), 'SELECT * FROM ONLY \$id');
      expect(surql.create('id', 'data'), 'CREATE ONLY \$id CONTENT \$data');
      expect(surql.delete('id'), 'DELETE \$id');
      expect(surql.upsertMerge('id', 'data'), 'UPSERT ONLY \$id MERGE \$data');
    });

    test('createSet with mixed items', () {
      final sql = surql.createSet('id', [
        SetItem.field('title'),
        SetItem.keyVar('author', 'authorVar'),
        SetItem.statement('createdAt = time::now()'),
      ]);
      expect(
        sql,
        'CREATE ONLY \$id SET title = \$title, author = \$authorVar, createdAt = time::now()',
      );
    });

    test('seal single appends semicolon', () {
      expect(surql.seal('SELECT * FROM x'), 'SELECT * FROM x;');
    });

    test('sealTx extract skips the BEGIN null', () {
      final tx = surql.tx(['SELECT 1', 'SELECT 2']);
      expect(tx.statementCount, 2);
      final sealed = surql.sealTx(tx);
      // results[0] is the BEGIN null; last statement is at index 2.
      expect(sealed.extract([null, 'one', 'two']), 'two');
    });

    test('createMutation update with beforeRecord', () {
      final sql = surql.createMutation(
        MutationEventType.update,
        'mid',
        'rid',
        dataVar: 'data',
        beforeRecordVar: 'before',
      );
      expect(
        sql,
        "CREATE ONLY \$mid SET mutationType = 'update', recordId = \$rid, data = \$data, beforeRecord = \$before",
      );
    });
  });

  group('parser', () {
    final schema = {
      'author': const ColumnSchema(recordId: true),
      'createdAt': const ColumnSchema(dateTime: true),
      'title': const ColumnSchema(type: 'string'),
    };

    test('cleanRecord keeps id, _00_*, and schema columns', () {
      final cleaned = cleanRecord(schema, {
        'id': 'thread:1',
        'title': 'hi',
        '_00_rv': 3,
        'serverOnly': 'x',
      });
      expect(cleaned.keys, containsAll(['id', 'title', '_00_rv']));
      expect(cleaned.containsKey('serverOnly'), isFalse);
    });

    test('parseParams coerces recordId and dateTime', () {
      final parsed = parseParams(schema, {
        'author': 'user:42',
        'createdAt': '2026-01-01T00:00:00.000Z',
        'title': 'hi',
      });
      expect(parsed['author'], isA<RecordId>());
      expect((parsed['author'] as RecordId).table, 'user');
      expect(parsed['createdAt'], isA<DateTime>());
      expect(parsed['title'], 'hi');
    });
  });

  group('EventSystem', () {
    test('emit delivers on microtask, not synchronously', () async {
      final es = EventSystem(['ping']);
      final received = <dynamic>[];
      es.subscribe('ping', (e) => received.add(e.payload));
      es.emit('ping', 1);
      expect(received, isEmpty); // batched
      await Future<void>.value();
      expect(received, [1]);
    });

    test('immediately replays last event', () async {
      final es = EventSystem(['ping']);
      es.emit('ping', 'first');
      await Future<void>.value();
      final seen = <dynamic>[];
      es.subscribe('ping', (e) => seen.add(e.payload),
          const EventSubscriptionOptions(immediately: true));
      expect(seen, ['first']);
    });

    test('once unsubscribes after first delivery', () async {
      final es = EventSystem(['ping']);
      var count = 0;
      es.subscribe(
          'ping', (_) => count++, const EventSubscriptionOptions(once: true));
      es.emit('ping', 1);
      es.emit('ping', 2);
      await Future<void>.value();
      expect(count, 1);
    });

    test('debounced coalesces to the last event', () {
      fakeAsync((async) {
        final es = EventSystem(['ping']);
        final seen = <dynamic>[];
        es.subscribe('ping', (e) => seen.add(e.payload));
        es.addEvent(
            const SpookyEvent('ping', 'a'),
            const PushEventOptions(
                debounced: DebouncedConfig(key: 'k', delay: 100)));
        es.addEvent(
            const SpookyEvent('ping', 'b'),
            const PushEventOptions(
                debounced: DebouncedConfig(key: 'k', delay: 100)));
        async.elapse(const Duration(milliseconds: 100));
        expect(seen, ['b']);
      });
    });
  });
}
