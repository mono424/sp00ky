import 'package:cbor/cbor.dart';
import 'package:spooky_core/src/surreal/cbor_codec.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:test/test.dart';

// Read a field from a raw-decoded CBOR map without relying on CborValue key
// equality (look up by the key's string form).
CborValue _rawField(List<int> bytes, String key) {
  final m = cborDecode(bytes) as CborMap;
  for (final e in m.entries) {
    if (e.key is CborString && (e.key as CborString).toString() == key) {
      return e.value;
    }
  }
  fail('no field "$key" in decoded map');
}

void main() {
  group('surreal cbor codec', () {
    test('RecordId is encoded as a real record (tag 8), decoded to "table:id"',
        () {
      final bytes = surrealCborEncode({'id': RecordId('game', 'abc')});
      // On the wire it's a tagged record, NOT a string — the whole point.
      expect(_rawField(bytes, 'id').tags, contains(SurrealCborTag.recordId));
      // Normalized decode gives the "table:id" string downstream expects.
      expect((surrealCborDecode(bytes) as Map)['id'], 'game:abc');
    });

    test('DateTime is encoded as custom datetime (tag 12), decoded to ISO', () {
      final dt = DateTime.utc(2026, 1, 2, 3, 4, 5, 6);
      final bytes = surrealCborEncode({'when': dt});
      expect(_rawField(bytes, 'when').tags,
          contains(SurrealCborTag.customDatetime));
      expect((surrealCborDecode(bytes) as Map)['when'], dt.toIso8601String());
    });

    test('primitives + nested maps/lists round-trip', () {
      final v = {
        's': 'hi',
        'n': 42,
        'big': -1735689600, // negative sort_index-style int
        'f': 1.5,
        'b': true,
        'nil': null,
        'list': [1, 'two', RecordId('t', 'x')],
        'nested': {'a': RecordId('u', '1')},
      };
      final out = surrealCborDecode(surrealCborEncode(v)) as Map;
      expect(out['s'], 'hi');
      expect(out['n'], 42);
      expect(out['big'], -1735689600);
      expect(out['f'], 1.5);
      expect(out['b'], true);
      expect(out['nil'], isNull);
      expect((out['list'] as List)[2], 't:x');
      expect((out['nested'] as Map)['a'], 'u:1');
    });

    test('RPC envelope shape (id/method/params) round-trips', () {
      final bytes = surrealCborEncode({
        'id': '1',
        'method': 'query',
        'params': [
          r'SELECT * FROM $id',
          {'id': RecordId('thing', 'x')},
        ],
      });
      final out = surrealCborDecode(bytes) as Map;
      expect(out['method'], 'query');
      expect(((out['params'] as List)[1] as Map)['id'], 'thing:x');
    });
  });
}
