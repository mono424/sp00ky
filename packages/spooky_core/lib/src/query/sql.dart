import '../kernel/effects.dart' show StatementResult;
import '../surreal/value.dart';
import '../types.dart';

/// Every statement the query side sends, as pure builders.
///
/// The local half of `packages/core/src/query/sql.ts` is not here: sqlite
/// cannot run SurrealQL, so the `_00_view` row is addressed through the
/// `local.*` effects by `(table, id)` instead of by a SurrealQL string.

const String viewTable = '_00_view';
const String legacyViewTable = '_00_window';

/// Record id of the durable local membership row for [viewKey].
String viewRecordId(String viewKey) => '$viewTable:$viewKey';

/// The durable local membership row's document.
Map<String, dynamic> viewRow(
        RecordVersionArray ids, bool confirmed, int now) =>
    {
      'ids': [
        for (final (id, version) in ids) [id, version]
      ],
      'confirmed': confirmed,
      'updatedAt': now,
    };

/// Read a `_00_view` / `_00_window` document back into id/version pairs.
RecordVersionArray decodeViewIds(Object? raw) {
  if (raw is! List) return const [];
  final out = <RecordVersion>[];
  for (final entry in raw) {
    if (entry is List && entry.length >= 2) {
      out.add((entry[0].toString(), (entry[1] as num).toInt()));
    }
  }
  return out;
}

// ---- remote reads -----------------------------------------------------------

String listRefSelect(String table) =>
    'SELECT out, version FROM $table WHERE in = \$in AND parent IS NONE';

/// A `.related()` query's SUBQUERY child edges. The SSP tags each matched child
/// edge with `parent`/`parent_rel` at any nesting depth, so `parent IS NOT NONE`
/// is exactly the complement of [listRefSelect]'s primary window.
String subqueryListRefSelect(String table) =>
    'SELECT out, version FROM $table WHERE in = \$in AND parent IS NOT NONE';

String queryRowCountSelect() =>
    'SELECT VALUE { rowCount: rowCount, state: state } FROM ONLY \$in';

String listRefBatchSelect(String table) =>
    'SELECT in, out, version, parent FROM $table WHERE in IN \$ins';

String queryRowCountBatchSelect() =>
    'SELECT VALUE { id: id, rowCount: rowCount, state: state } FROM \$ins';

/// The single-query read: edges, meta and subquery children in one request.
String singleSnapshotSelect(String table) =>
    '${listRefSelect(table)};\n${queryRowCountSelect()};\n${subqueryListRefSelect(table)}';

/// The many-query read: all edges plus all metas in one request.
String batchSnapshotSelect(String table) =>
    '${listRefBatchSelect(table)};\n${queryRowCountBatchSelect()}';

class RegisterPayload {
  const RegisterPayload({
    required this.id,
    required this.surql,
    required this.params,
    required this.ttl,
  });

  final RecordId id;
  final String surql;
  final Map<String, dynamic> params;
  final QueryTimeToLive ttl;

  Map<String, dynamic> toJson() => {
        'id': id,
        'surql': surql,
        'params': params,
        'ttl': ttl,
      };
}

/// Register plus read back edges/meta/children in ONE request.
String registerSelect(String table) =>
    'fn::query::register(\$config);\n${singleSnapshotSelect(table)}';

Map<String, dynamic> registerVars(RegisterPayload payload) => {
      'config': payload.toJson(),
      'in': payload.id,
    };

/// One heartbeat statement per view id; result index `i` answers id `i`.
({String sql, Map<String, dynamic> vars}) heartbeatBatch(List<RecordId> ids) {
  final vars = <String, dynamic>{};
  final stmts = <String>[];
  for (var i = 0; i < ids.length; i++) {
    vars['id$i'] = ids[i];
    stmts.add('fn::query::heartbeat(\$id$i)');
  }
  return (sql: stmts.join(';\n'), vars: vars);
}

/// A heartbeat answer whose statement matched no row: the view was reclaimed.
bool heartbeatRowGone(Object? result) => result is List && result.isEmpty;

String bodySelect() => 'SELECT * FROM \$ids';

/// Release a view the client no longer holds.
String unsubscribeSelect() => 'fn::query::unsubscribe(\$id)';

// ---- result shaping ---------------------------------------------------------

/// `[out, version]` pairs from a `_00_list_ref` select result.
RecordVersionArray parseListRefRows(Object? result) {
  if (result is! List) return const [];
  final out = <RecordVersion>[];
  for (final item in result) {
    if (item is Map) {
      out.add((
        item['out'].toString(),
        ((item['version'] as num?) ?? 0).toInt(),
      ));
    }
  }
  return out;
}

/// The statement at [index], or null when the response was shorter.
StatementResult? stmt(List<StatementResult> results, int index) =>
    index < results.length ? results[index] : null;
