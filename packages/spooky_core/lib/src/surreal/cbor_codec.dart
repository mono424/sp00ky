import 'dart:typed_data';

import 'package:cbor/cbor.dart';

import 'value.dart';

/// SurrealDB's CBOR protocol carries records, datetimes, etc. as custom tags
/// (mirrors `surrealdb.js`). The JSON-RPC wire could only send a record id as a
/// plain string, which SurrealDB won't match against a `record<…>` field — so
/// record-filtered queries (`WHERE database = $db`, list_ref `WHERE in = $in`,
/// `SELECT * FROM $ids`) silently matched nothing. CBOR sends real records.
///
/// On the way OUT we map [RecordId]/[DateTime] to their tagged forms; on the way
/// IN we normalize back to the SAME shapes the JSON path produced — records as
/// `"table:id"` strings, datetimes as ISO-8601 strings — so all downstream code
/// (Game.fromJson, list_ref parsing, the cache's jsonEncode, …) is unchanged.
class SurrealCborTag {
  static const int specDatetime = 0; // RFC3339 string (CBOR standard tag 0)
  static const int epochDatetime = 1; // epoch seconds (CBOR standard tag 1)
  static const int none = 6;
  static const int table = 7;
  static const int recordId = 8; // [table, id]
  static const int stringUuid = 9;
  static const int stringDecimal = 10;
  static const int customDatetime = 12; // [seconds, nanoseconds]
  static const int stringDuration = 13;
  static const int customDuration = 14; // [seconds, nanoseconds]
  static const int specUuid = 37; // 16 raw bytes
}

/// Encode a Dart value to SurrealDB-flavored CBOR bytes.
List<int> surrealCborEncode(Object? value) => cborEncode(valueToCbor(value));

/// Decode SurrealDB CBOR bytes to normalized Dart values (records → strings,
/// datetimes → ISO strings, maps → `Map<String, dynamic>`).
Object? surrealCborDecode(List<int> bytes) => cborToValue(cborDecode(bytes));

/// Dart → [CborValue] with SurrealDB tags for [RecordId]/[DateTime].
CborValue valueToCbor(Object? v) {
  if (v == null) return const CborNull();
  if (v is CborValue) return v;
  if (v is RecordId) {
    final id = v.id;
    final idCbor = id is int ? CborSmallInt(id) : CborString(id.toString());
    return CborList([CborString(v.table), idCbor],
        tags: const [SurrealCborTag.recordId]);
  }
  if (v is DateTime) {
    final us = v.toUtc().microsecondsSinceEpoch;
    final secs = us ~/ Duration.microsecondsPerSecond;
    final nanos = (us % Duration.microsecondsPerSecond) * 1000;
    return CborList(
      [CborInt(BigInt.from(secs)), CborSmallInt(nanos)],
      tags: const [SurrealCborTag.customDatetime],
    );
  }
  if (v is SurrealDuration) {
    return CborString(v.value, tags: const [SurrealCborTag.stringDuration]);
  }
  if (v is bool) return CborBool(v);
  if (v is int) return CborInt(BigInt.from(v));
  if (v is double) return CborFloat(v);
  if (v is String) return CborString(v);
  // A byte buffer is a SurrealDB `bytes` value, not an array of ints: the
  // latter is what `f"...".put()` would store, and it is not a file.
  if (v is Uint8List) return CborBytes(v);
  if (v is List) return CborList(v.map(valueToCbor).toList());
  if (v is Map) {
    return CborMap({
      for (final e in v.entries) valueToCbor(e.key): valueToCbor(e.value),
    });
  }
  return CborString(v.toString());
}

/// [CborValue] → normalized Dart value (inverse of [valueToCbor]).
Object? cborToValue(CborValue v) {
  final tags = v.tags;

  if (tags.contains(SurrealCborTag.none)) return null;

  if (tags.contains(SurrealCborTag.recordId)) {
    if (v is CborList && v.length >= 2) {
      return '${cborToValue(v[0])}:${cborToValue(v[1])}';
    }
    if (v is CborString) return v.toString(); // StringRecordId form
  }

  if (tags.contains(SurrealCborTag.customDatetime) && v is CborList) {
    if (v.length >= 2 && v[0] is CborInt && v[1] is CborInt) {
      final secs = (v[0] as CborInt).toInt();
      final nanos = (v[1] as CborInt).toInt();
      final us = secs * Duration.microsecondsPerSecond + nanos ~/ 1000;
      return DateTime.fromMicrosecondsSinceEpoch(us, isUtc: true)
          .toIso8601String();
    }
  }

  if (tags.contains(SurrealCborTag.specDatetime) ||
      tags.contains(SurrealCborTag.epochDatetime)) {
    if (v is CborString) return v.toString(); // RFC3339
    if (v is CborInt) {
      return DateTime.fromMillisecondsSinceEpoch(
              v.toInt() * Duration.millisecondsPerSecond,
              isUtc: true)
          .toIso8601String();
    }
  }

  if ((tags.contains(SurrealCborTag.table) ||
          tags.contains(SurrealCborTag.stringUuid) ||
          tags.contains(SurrealCborTag.stringDecimal) ||
          tags.contains(SurrealCborTag.stringDuration)) &&
      v is CborString) {
    return v.toString();
  }

  // Binary UUID (tag 37, 16 bytes) — e.g. a LIVE query id. Render canonical so
  // it round-trips back to SurrealDB (KILL) and matches notification ids.
  if (tags.contains(SurrealCborTag.specUuid) && v is CborBytes) {
    return _bytesToUuid(v.bytes);
  }

  // Untagged / unhandled tag — convert by structural type.
  if (v is CborNull || v is CborUndefined) return null;
  if (v is CborBool) return v.value;
  if (v is CborInt) return v.toInt();
  if (v is CborFloat) return v.value;
  if (v is CborString) return v.toString();
  if (v is CborBytes) {
    final bytes = v.bytes;
    return bytes is Uint8List ? bytes : Uint8List.fromList(bytes);
  }
  if (v is CborList) return v.map(cborToValue).toList();
  if (v is CborMap) {
    final out = <String, dynamic>{};
    v.forEach((key, val) => out[cborToValue(key).toString()] = cborToValue(val));
    return out;
  }
  // A DateTime can slip through via the package's tag-0/1 auto-decoding.
  final obj = v.toObject();
  if (obj is DateTime) return obj.toUtc().toIso8601String();
  return obj;
}

/// Format 16 raw bytes as a canonical UUID string (`8-4-4-4-12`). Non-16-byte
/// input falls back to plain hex.
String _bytesToUuid(List<int> bytes) {
  final hex = bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
  if (hex.length != 32) return hex;
  return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-'
      '${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
}
