import 'dart:convert';

import '../utils/json_safe.dart';

/// Hash inputs for the two query keys. Both are `sha256(json(input))`: the
/// salted key names the remote `_00_query` row, the unsalted one names the
/// durable `_00_view` row.
///
/// Key order matches the JavaScript object literal (`surql`, `params`, then
/// `sessionId`) so a Dart and a JS client agree on the key for the same query.
class QueryKeyInput {
  const QueryKeyInput({required this.surql, required this.params});
  final String surql;
  final Map<String, dynamic> params;
}

/// Session-salted: two clients of one user must not share a remote view.
String queryHashInput(QueryKeyInput input, String? sessionId) => jsonEncode({
      'surql': input.surql,
      'params': jsonSafe(input.params),
      'sessionId': sessionId,
    });

/// Session-independent: survives restart and bucket switch.
String viewKeyInput(QueryKeyInput input) => jsonEncode({
      'surql': input.surql,
      'params': jsonSafe(input.params),
    });
