import '../surreal/value.dart';

/// Strip values `jsonEncode` cannot encode ([RecordId], [DateTime],
/// [SurrealDuration]) down to the string forms they take on the wire, so a
/// structure round-trips through JSON unchanged.
///
/// Used for the query-key hashes (the hash must be stable across a reload) and
/// for the local store's document encoding.
dynamic jsonSafe(dynamic value) {
  if (value is RecordId) return value.encode();
  if (value is DateTime) return value.toUtc().toIso8601String();
  if (value is SurrealDuration) return value.toString();
  if (value is Map) {
    return value.map((k, v) => MapEntry(k.toString(), jsonSafe(v)));
  }
  if (value is List) return value.map(jsonSafe).toList();
  return value;
}
