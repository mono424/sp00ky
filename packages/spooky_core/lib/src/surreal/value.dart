import 'dart:math';

import 'package:uuid/uuid.dart';

const _uuid = Uuid();

/// A SurrealDB record identifier (`table:id`).
///
/// Mirrors the `RecordId` from the SurrealDB JS SDK closely enough for the
/// core's needs. The `id` part may be a string, number, or other primitive;
/// it is kept as [Object] and stringified where the JS code does `String(id)`.
class RecordId {
  const RecordId(this.table, this.id);

  /// Parse a `table:id` string. The id part keeps any further colons, matching
  /// the TS `parseRecordIdString` (`table`, then `idParts.join(':')`).
  factory RecordId.parse(String id) {
    final parts = id.split(':');
    final table = parts.first;
    final idPart = parts.skip(1).join(':');
    return RecordId(table, idPart);
  }

  final String table;
  final Object id;

  /// `table:id` (matches TS `encodeRecordId`).
  String encode() => '$table:$id';

  @override
  String toString() => encode();

  @override
  bool operator ==(Object other) =>
      other is RecordId &&
      other.table == table &&
      other.id.toString() == id.toString();

  @override
  int get hashCode => Object.hash(table, id.toString());
}

/// A SurrealDB duration value (e.g. `10m`). Kept as the raw string form; use
/// [parseDuration] (in duration_utils) to convert to milliseconds.
class SurrealDuration {
  const SurrealDuration(this.value);
  final String value;
  @override
  String toString() => value;
}

/// UUID v4 with hyphens stripped (matches TS `generateId`).
String generateId() => _uuid.v4().replaceAll('-', '');

/// 32 random bytes, hex: the secret a client proves it owns its anonymous
/// views with (`$config.owner` on `fn::query::register`; TS
/// `mintOwnerSecret`).
String generateOwnerSecret() {
  final rng = Random.secure();
  return [
    for (var i = 0; i < 32; i++)
      rng.nextInt(256).toRadixString(16).padLeft(2, '0'),
  ].join();
}

/// A fresh `RecordId` for [tableName] with a generated id (TS `generateNewTableId`).
RecordId generateNewTableId(String tableName) =>
    RecordId(tableName, generateId());
