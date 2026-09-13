import 'dart:convert';

import '../kernel/effects.dart';
import '../state/client_state.dart' show Row;
import '../utils/json_safe.dart';
import '../utils/record_id_utils.dart';
import '../utils/sort_rows.dart';
import 'window_query.dart';

bool isWindowed(String surql) => buildWindowMaterialization(surql) != null;

/// The one read that materializes a query: resolve the render set by id.
///
/// Divergence from `packages/core/src/query/materialize.ts`: there the effect is
/// a SurrealQL string or a rendered `QueryPlan`, so the local engine re-evaluates
/// the query's projection and ordering. sqlite cannot run SurrealQL, so the Dart
/// read is a batched id lookup and the ordering the circuit already applied is
/// kept - except for a windowed query, whose `ORDER BY` is re-applied here
/// because its ids come from the server's `_00_list_ref` rather than the circuit.
Effect<List<Row>> materializeEffect(String table, List<String> ids) =>
    Fx.localGetMany(table, ids);

/// Re-apply a windowed query's own `ORDER BY` to rows resolved by id.
List<Row> applyWindowOrder(String surql, List<Row> rows) {
  final window = buildWindowMaterialization(surql);
  if (window == null || window.orderBy.isEmpty) return rows;
  return sortRows(rows, window.orderBy);
}

/// The logical table a render set belongs to. Ids in one view always share a
/// table, so the first id decides; an empty set never reaches the store.
String tableOfIds(List<String> ids, String fallback) =>
    ids.isEmpty ? fallback : extractTablePart(ids.first);

/// Cheap structural equality for the "did the rows change" check.
bool rowsEqual(List<Object?> a, List<Object?> b) {
  if (identical(a, b)) return true;
  if (a.length != b.length) return false;
  return jsonEncode(a, toEncodable: jsonSafe) ==
      jsonEncode(b, toEncodable: jsonSafe);
}
