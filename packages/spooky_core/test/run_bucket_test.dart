import 'dart:convert';

import 'package:spooky_core/spooky_core.dart';
import 'package:test/test.dart';

import 'fake_remote.dart';

void main() {
  group('run() backend outbox', () {
    late Sp00kyClient client;

    final schema = {
      // The outbox table must declare the columns the outbox record uses,
      // else create()'s schema-aware param filtering would drop them.
      'job_outbox': {
        'columns': {
          'path': const ColumnSchema(type: 'string'),
          'payload': const ColumnSchema(type: 'string'),
          'status': const ColumnSchema(type: 'string'),
          'max_retries': const ColumnSchema(type: 'int'),
          'retry_strategy': const ColumnSchema(type: 'string'),
          'assigned_to': const ColumnSchema(type: 'string'),
          'timeout': const ColumnSchema(type: 'int'),
        },
      },
      'backends': {
        'jobs': {
          'outboxTable': 'job_outbox',
          'routes': {
            'process': {
              'args': {
                'url': {'optional': false},
              },
            },
          },
        },
      },
    };

    setUp(() async {
      client = Sp00kyClient(Sp00kyConfig(
        database: const DatabaseConfig(namespace: 't', database: 't'),
        schema: schema,
        schemaSurql:
            'DEFINE TABLE job_outbox SCHEMAFULL PERMISSIONS FOR select WHERE true;',
      ));
      await client.init();
    });
    tearDown(() => client.close());

    test('enqueues an outbox record with the route + JSON payload', () async {
      await client.run('jobs', 'process', {'url': 'http://x'});
      final rows = client.localStore.getAll('job_outbox');
      expect(rows, hasLength(1));
      expect(rows.first['path'], 'process');
      expect(jsonDecode(rows.first['payload'] as String), {'url': 'http://x'});
      expect(rows.first['max_retries'], 3);
      expect(rows.first['retry_strategy'], 'linear');
    });

    test('seeds status pending on the optimistic row', () async {
      await client.run('jobs', 'process', {'url': 'http://x'});
      expect(client.localStore.getAll('job_outbox').first['status'], 'pending');
    });

    test('throws on a missing required arg', () async {
      expect(
        () => client.run('jobs', 'process', {}),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('honors RunOptions (retries / strategy / assignee)', () async {
      await client.run('jobs', 'process', {'url': 'u'},
          options: const RunOptions(
              maxRetries: 5, retryStrategy: 'exponential', assignedTo: 'w1'));
      final row = client.localStore.getAll('job_outbox').first;
      expect(row['max_retries'], 5);
      expect(row['retry_strategy'], 'exponential');
      expect(row['assigned_to'], 'w1');
    });
  });

  group('bucket() file storage', () {
    late FakeRemote remote;
    late Sp00kyClient client;

    setUp(() async {
      remote = FakeRemote();
      client = Sp00kyClient(
        Sp00kyConfig(
          database: const DatabaseConfig(
              endpoint: 'ws://x', namespace: 't', database: 't'),
          schema: const {},
          schemaSurql: '',
        ),
        remoteClient: remote,
      );
      await client.init();
    });
    tearDown(() => client.close());

    test('put / get / exists / delete / list issue the file SURQL', () async {
      final bucket = client.bucket('avatars');
      await bucket.put('a.png', 'data');
      await bucket.get('a.png');
      await bucket.exists('a.png');
      await bucket.delete('a.png');
      await bucket.copy('a.png', 'b.png');
      await bucket.rename('a.png', 'c.png');
      await bucket.list('sub/');

      final q = remote.queries.join('\n');
      expect(q, contains('f"avatars:/a.png".put(\$content)'));
      expect(q, contains('f"avatars:/a.png".get()'));
      expect(q, contains('f"avatars:/a.png".exists()'));
      expect(q, contains('f"avatars:/a.png".delete()'));
      expect(q, contains('f"avatars:/a.png".copy(\$target)'));
      expect(q, contains('f"avatars:/a.png".rename(\$target)'));
      expect(q, contains('f"avatars:/sub/".list()'));
    });
  });
}
