import 'dart:convert';

import 'package:crypto/crypto.dart';
import 'package:spooky_core/src/query/hash.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:test/test.dart';

void main() {
  group('hash inputs (golden against the previous DataModule formulas)', () {
    const input = QueryKeyInput(surql: 'SELECT * FROM thing', params: {'a': 1});

    test('the salted key is {surql, params, sessionId} in that key order', () {
      expect(queryHashInput(input, 's1'),
          '{"surql":"SELECT * FROM thing","params":{"a":1},"sessionId":"s1"}');
      expect(queryHashInput(input, null),
          '{"surql":"SELECT * FROM thing","params":{"a":1},"sessionId":null}');
    });

    test('the view key is the same input minus the salt', () {
      expect(viewKeyInput(input),
          '{"surql":"SELECT * FROM thing","params":{"a":1}}');
    });

    test('RecordId and DateTime params serialize as they do on the wire', () {
      final withRid = QueryKeyInput(surql: 'S', params: {
        'r': RecordId('user', 'u1'),
        'd': DateTime.utc(2026, 1, 2, 3, 4, 5),
      });
      expect(viewKeyInput(withRid),
          '{"surql":"S","params":{"r":"user:u1","d":"2026-01-02T03:04:05.000Z"}}');
    });

    test('the same query hashes the same across two clients of one session',
        () {
      final a = sha256.convert(utf8.encode(queryHashInput(input, 's1')));
      final b = sha256.convert(utf8.encode(queryHashInput(input, 's1')));
      expect(a.toString(), b.toString());
      final other = sha256.convert(utf8.encode(queryHashInput(input, 's2')));
      expect(other.toString(), isNot(a.toString()));
    });
  });
}
