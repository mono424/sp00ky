import 'package:fake_async/fake_async.dart';
import 'package:spooky_core/spooky_core.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/utils/error_classification.dart';
import 'package:spooky_core/src/utils/retry.dart';
import 'package:spooky_core/src/utils/surql.dart';
import 'package:test/test.dart';

void main() {
  group('RecordId equality', () {
    test('equal by table + stringified id; usable as map key', () {
      expect(RecordId('t', '1'), RecordId('t', '1'));
      expect(RecordId('t', 1), RecordId('t', '1')); // id stringified
      expect(RecordId('t', '1'), isNot(RecordId('t', '2')));
      final set = {RecordId('t', '1'), RecordId('t', '1')};
      expect(set, hasLength(1));
    });
    test('parse keeps embedded colons in the id', () {
      final r = RecordId.parse('t:a:b:c');
      expect(r.table, 't');
      expect(r.id, 'a:b:c');
    });
  });

  group('surql extra builders', () {
    test('selectByFieldsAnd', () {
      final sql = surql.selectByFieldsAnd(
        'thread',
        [WhereItem.fieldVar('author', 'a'), WhereItem.field('published')],
        [ReturnItem.raw('id'), ReturnItem.aliased('title', 't')],
      );
      expect(sql,
          'SELECT id,title as t FROM thread WHERE author = \$a AND published = \$published');
    });
    test('updateMerge / upsert / let / returnObject', () {
      expect(surql.updateMerge('id', 'data'), 'UPDATE ONLY \$id MERGE \$data');
      expect(surql.upsert('id', 'data'), 'UPSERT ONLY \$id REPLACE \$data');
      expect(surql.let('x', 'SELECT 1'), 'LET \$x = (SELECT 1)');
      expect(surql.returnObject([('target', 'updated')]),
          'RETURN {target: \$updated}');
    });
    test('sealTx with explicit resultIndex skips the BEGIN null', () {
      final tx = surql.tx(['CREATE a', 'CREATE b', 'CREATE c']);
      final sealed = surql.sealTx(tx, resultIndex: 0);
      // index 0 -> results[1] (results[0] is the BEGIN null)
      expect(sealed.extract([null, 'first', 'second', 'third']), 'first');
    });
    test('createMutation delete shape', () {
      expect(
        surql.createMutation(MutationEventType.delete, 'mid', 'rid'),
        "CREATE ONLY \$mid SET mutationType = 'delete', recordId = \$rid",
      );
    });
  });

  group('classifySyncError', () {
    test('network patterns', () {
      expect(classifySyncError(Exception('Connection refused')), 'network');
      expect(classifySyncError('socket timeout'), 'network');
      expect(classifySyncError('WebSocket disconnected'), 'network');
    });
    test('surreal ConnectionUnavailableError is network, not application', () {
      // Reads "connected", not "connection" — the canonical error on an
      // idle-dropped socket. Misclassifying it rolls the mutation back and drops
      // it instead of re-queuing for the reconnect.
      expect(
        classifySyncError(Exception(
            'You must be connected to a SurrealDB instance before performing this operation')),
        'network',
      );
      expect(classifySyncError('ConnectionUnavailableError'), 'network');
    });
    test('transient server states are network, so the outbox retries them', () {
      // 2026-09-08 on whitepawn: a 1200-game import lost 752 games because each
      // of these was classified `application`, rolled back locally and dropped.
      expect(
        classifySyncError(Exception(
            'Transaction conflict: Resource busy: . This transaction can be retried')),
        'network',
      );
      expect(classifySyncError('Specify a namespace to use'), 'network');
      expect(classifySyncError('Specify a database to use'), 'network');
    });
    test('a failed sp00ky ingest post is network, so it is retried', () {
      // Verbatim from SurrealDB 3.1.5 and 3.3.4: the write was refused because
      // sp00ky's own ingest event could not reach the scheduler / SSP.
      const prefix = 'Error while processing event _00_item_mutation: '
          'There was an error processing a remote HTTP request: ';
      for (final message in [
        // Refused connection and a post past the event TIMEOUT read the same.
        '${prefix}error sending request for url (http://scheduler:9667/ingest)',
        '${prefix}500 Internal Server Error',
        '${prefix}502 Bad Gateway',
        '${prefix}503 Service Unavailable',
        '${prefix}408 Request Timeout',
        '${prefix}429 Too Many Requests',
        'Error while processing event _00_item_delete: There was an error '
            'processing a remote HTTP request: error sending request for url '
            '(http://host.docker.internal:50466/ingest)',
        'Error while processing event _00_item_mutation: failed to lookup '
            'address information: Name or service not known',
        // An app event that wrote to a synced table carries the inner event.
        'Error while processing event app_fanout: Error while processing '
            'event _00_b_mutation: There was an error processing a remote HTTP '
            'request: 503 Service Unavailable',
      ]) {
        expect(classifySyncError(Exception(message)), 'network',
            reason: message);
      }
    });
    test('everything else is application', () {
      expect(classifySyncError(StateError('bad data')), 'application');
      expect(classifySyncError('permission denied'), 'application');
      for (final message in [
        "Couldn't coerce value for field `title` of `item:1`: "
            'Expected `string` but found `1`',
        // An app's own event: its endpoint, its THROW, its business.
        'Error while processing event notify_crm: There was an error '
            'processing a remote HTTP request: 500 Internal Server Error',
        'Error while processing event notify_crm: There was an error '
            'processing a remote HTTP request: error sending request for url '
            '(http://crm.example/hook)',
        'Error while processing event cf_guard: An error occurred: '
            'conformance guard: write refused',
        // A THROW inside a sp00ky event is not a failed post.
        'Error while processing event _00_item_mutation: An error occurred: inner',
        // A 4xx from /ingest no retry fixes.
        'Error while processing event _00_item_mutation: There was an error '
            'processing a remote HTTP request: 400 Bad Request',
        'Error while processing event _00_item_mutation: There was an error '
            'processing a remote HTTP request: 401 Unauthorized',
        'Error while processing event _00_item_mutation: There was an error '
            'processing a remote HTTP request: 413 Payload Too Large',
      ]) {
        expect(classifySyncError(Exception(message)), 'application',
            reason: message);
      }
    });
  });

  group('withRetry', () {
    final logger = SpookyLogger.root('test');
    test('retries on transaction conflicts then succeeds', () {
      fakeAsync((async) {
        var attempts = 0;
        int? result;
        withRetry<int>(logger, () async {
          attempts++;
          if (attempts < 3) throw Exception('Database is busy');
          return 42;
        }, delayMs: 10)
            .then((v) => result = v);
        async.elapse(const Duration(milliseconds: 100));
        async.flushMicrotasks();
        expect(result, 42);
        expect(attempts, 3);
      });
    });
    test('rethrows non-transient errors immediately', () async {
      var attempts = 0;
      await expectLater(
        withRetry<int>(logger, () async {
          attempts++;
          throw StateError('permanent');
        }),
        throwsA(isA<StateError>()),
      );
      expect(attempts, 1); // no retry
    });
  });

  group('parseDuration edge cases', () {
    test('hours and seconds', () {
      expect(parseDuration('2h'), 7200000);
      expect(parseDuration('45s'), 45000);
    });
    test('unknown unit / garbage -> 10m default', () {
      expect(parseDuration('1d'), 600000);
      expect(parseDuration('???'), 600000);
    });
  });
}
