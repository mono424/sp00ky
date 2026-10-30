import 'dart:typed_data';

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
      expect(
          _rawField(bytes, 'when').tags, contains(SurrealCborTag.customDatetime));
      expect((surrealCborDecode(bytes) as Map)['when'], dt.toIso8601String());
    });

    test('SurrealDuration is sent as [seconds, nanoseconds] (tag 14)', () {
      // SurrealDB 4 answers 400 to the string form (tag 13).
      List<int> parts(String text) {
        final raw = _rawField(
            surrealCborEncode({'d': SurrealDuration(text)}), 'd') as CborList;
        expect(raw.tags, contains(SurrealCborTag.customDuration));
        return raw.map((e) => (e as CborInt).toInt()).toList();
      }

      expect(parts('5m'), [300, 0]);
      expect(parts('1h30m'), [5400, 0]);
      expect(parts('1s250ms'), [1, 250000000]);
      expect(parts('2w1d'), [1296000, 0]);
      expect(parts('1y'), [31536000, 0]);
      expect(parts('7ns'), [0, 7]);
    });

    test('a duration it cannot read keeps the string form (tag 13)', () {
      final raw = _rawField(
          surrealCborEncode({'d': const SurrealDuration('soon')}), 'd');
      expect(raw.tags, contains(SurrealCborTag.stringDuration));
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

    test('Uint8List round-trips as a CBOR byte string, not an int array', () {
      final payload = Uint8List.fromList([0, 137, 80, 78, 255]);
      final bytes = surrealCborEncode({'content': payload});
      expect(_rawField(bytes, 'content'), isA<CborBytes>());
      final out = surrealCborDecode(bytes) as Map;
      expect(out['content'], isA<Uint8List>());
      expect(out['content'], payload);
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
