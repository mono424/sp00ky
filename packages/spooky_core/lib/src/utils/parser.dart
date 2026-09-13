import '../surreal/value.dart';
import 'record_id_utils.dart';

/// The first statement's result list from a SurrealDB multi-statement response.
///
/// SurrealDB returns `[<stmt0 result>, <stmt1 result>, ...]`; a `SELECT`'s rows
/// live in `results[0]` as a `List`. Returns an empty list when the response is
/// empty or the first result isn't a list. Callers `.cast<Map<String,
/// dynamic>>()` when they need typed rows.
List<dynamic> firstRows(List<dynamic> results) =>
    (results.isNotEmpty && results.first is List)
        ? results.first as List
        : const [];

/// Minimal column schema shape the parser needs (subset of the TS
/// `ColumnSchema` from query-builder).
class ColumnSchema {
  const ColumnSchema({
    this.recordId = false,
    this.dateTime = false,
    this.type,
    this.optional = false,
  });

  final bool recordId;
  final bool dateTime;
  final String? type;
  final bool optional;
}

/// Strip server-only fields: keep `id`, any `_00_*` metadata, and declared
/// schema columns (TS `cleanRecord`).
Map<String, dynamic> cleanRecord(
  Map<String, ColumnSchema> tableSchema,
  Map<String, dynamic> record,
) {
  final cleaned = <String, dynamic>{};
  for (final entry in record.entries) {
    final key = entry.key;
    if (key == 'id' || key.startsWith('_00_') || tableSchema.containsKey(key)) {
      cleaned[key] = entry.value;
    }
  }
  return cleaned;
}

/// Coerce params to their schema column types (TS `parseParams`).
Map<String, dynamic> parseParams(
  Map<String, ColumnSchema> tableSchema,
  Map<String, dynamic> params,
) {
  final parsed = <String, dynamic>{};
  for (final entry in params.entries) {
    final column = tableSchema[entry.key];
    if (column != null && entry.value != null) {
      parsed[entry.key] = _parseValue(entry.key, column, entry.value);
    }
  }
  return parsed;
}

/// The schema field an `OR`-group parameter was minted from: the query builder
/// names the n-th alternative of a field `field__or<n>` so each binding is
/// distinct, and both spellings must re-type through the same column
/// (TS `baseFieldOfParam`).
final _orSuffix = RegExp(r'^(.+)__or\d+$');

String baseFieldOfParam(String name) =>
    _orSuffix.firstMatch(name)?.group(1) ?? name;

/// Re-type values from the schema, KEEPING keys the schema does not declare
/// (TS `parseQueryParams`).
///
/// This is the record-payload variant of [parseParams]: an outbox payload or a
/// create/update body may legitimately carry fields the local schema does not
/// know about, and dropping them would lose the write. [parseParams] keeps its
/// narrower "declared columns only" behaviour for query parameters.
Map<String, dynamic> parseQueryParams(
  Map<String, ColumnSchema> tableSchema,
  Map<String, dynamic> params,
) {
  final parsed = <String, dynamic>{};
  for (final entry in params.entries) {
    // `null` is Dart's only way to say "not set", where the browser core has
    // `undefined` (which TS drops here for the same reason). Keeping it would
    // send SurrealDB a NULL, and an `option<T>` field rejects NULL: an unset
    // optional on a generated model - whose `toJson` always writes the key -
    // failed the whole write with "Expected `none | bool` but found `NULL`".
    // Omitting the key leaves the field NONE, which is what unset means.
    if (entry.value == null) continue;
    final column =
        tableSchema[entry.key] ?? tableSchema[baseFieldOfParam(entry.key)];
    parsed[entry.key] = column == null
        ? entry.value
        : _parseValue(entry.key, column, entry.value);
  }
  return parsed;
}

dynamic _parseValue(String name, ColumnSchema column, dynamic value) {
  if (column.recordId) {
    if (value is RecordId) return value;
    if (value is String) return parseRecordIdString(value);
    throw ArgumentError('Invalid value for $name: $value');
  }
  if (column.dateTime) {
    if (value is DateTime) return value;
    if (value is int) return DateTime.fromMillisecondsSinceEpoch(value);
    if (value is String) return DateTime.parse(value);
    throw ArgumentError('Invalid value for $name: $value');
  }
  return value;
}
