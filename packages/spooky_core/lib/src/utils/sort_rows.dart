import '../surreal/value.dart';

/// One `ORDER BY` term: field name and direction (`asc` / `desc`).
typedef OrderByTerm = (String field, String direction);

/// Stable string key for a value, used for both ordering and relation matching
/// (TS `stableKey`). A [RecordId] and its `table:id` string form must collapse to
/// the same key so a foreign key stored either way still matches.
String stableKey(Object? value) {
  if (value == null) return ' null';
  if (value is String) return value;
  if (value is num || value is bool) return value.toString();
  if (value is RecordId) return value.encode();
  if (value is DateTime) return value.toUtc().toIso8601String();
  return value.toString();
}

/// Stable, multi-key sort matching SurrealQL `ORDER BY` closely enough for cache
/// display order (TS `sortRows`): ascending nulls last, type-aware compare, and a
/// stable tiebreak (original index) so equal rows keep input order.
List<Map<String, dynamic>> sortRows(
  List<Map<String, dynamic>> rows,
  List<OrderByTerm> orderBy,
) {
  if (orderBy.isEmpty) return rows;
  final indexed = [
    for (var i = 0; i < rows.length; i++) (rows[i], i),
  ];
  indexed.sort((a, b) {
    for (final (field, direction) in orderBy) {
      final cmp = compareValues(a.$1[field], b.$1[field]);
      // `.related()` plans keep the builder's spelling (`DESC`).
      if (cmp != 0) return direction.toLowerCase() == 'desc' ? -cmp : cmp;
    }
    return a.$2 - b.$2; // stable tiebreak
  });
  return [for (final entry in indexed) entry.$1];
}

/// Compare two field values the way [sortRows] orders them (TS `compareValues`).
int compareValues(Object? a, Object? b) {
  if (a == null && b == null) return 0;
  if (a == null) return 1; // nulls sort last (ascending)
  if (b == null) return -1;
  if (a is num && b is num) return a.compareTo(b);
  return stableKey(a).compareTo(stableKey(b));
}
