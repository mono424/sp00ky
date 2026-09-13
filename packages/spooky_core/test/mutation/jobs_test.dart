import 'dart:convert';

import 'package:spooky_core/src/mutation/jobs.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

void main() {
  const schema = <String, dynamic>{
    'backends': {
      'api': {
        'outboxTable': '_job_api',
        'routes': {
          '/spookify': {
            'args': {
              'id': {'optional': false},
              'note': {'optional': true},
            }
          }
        }
      }
    }
  };

  test('builds the job row and resolves the outbox table', () {
    final out = buildJobRecord(schema, 'api', '/spookify', {'id': 'thing:1'});
    expect(out.tableName, '_job_api');
    expect(out.record['path'], '/spookify');
    expect(out.record['status'], 'pending');
    expect(out.record['max_retries'], 3);
    expect(out.record['retry_strategy'], 'linear');
    expect(jsonDecode(out.record['payload'] as String),
        {'id': 'thing:1', 'note': null});
  });

  test('carries the options that are set, and only those', () {
    final out = buildJobRecord(
      schema,
      'api',
      '/spookify',
      {'id': 'thing:1'},
      options: const RunOptions(
          maxRetries: 7, retryStrategy: 'exponential', delay: 500),
    );
    expect(out.record['max_retries'], 7);
    expect(out.record['retry_strategy'], 'exponential');
    expect(out.record['delay'], 500);
    expect(out.record.containsKey('timeout'), isFalse);
    expect(out.record.containsKey('assigned_to'), isFalse);
  });

  test('rejects an unknown backend, route, or missing required argument', () {
    expect(() => buildJobRecord(schema, 'nope', '/spookify', const {}),
        throwsArgumentError);
    expect(() => buildJobRecord(schema, 'api', '/nope', const {}),
        throwsArgumentError);
    expect(() => buildJobRecord(schema, 'api', '/spookify', const {}),
        throwsArgumentError);
    expect(
        () => buildJobRecord(const {
              'backends': {
                'api': {
                  'routes': {'/x': <String, dynamic>{}}
                }
              }
            }, 'api', '/x', const {}),
        throwsArgumentError);
  });
}
