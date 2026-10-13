import '../kernel/effects.dart' show StatementResult;
import '../modules/ref_tables.dart' show anonListRefTable;
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

/// The durable local membership row's document. [children] are the
/// `.related()` subquery rows the view vouches for: the orphan collector keeps
/// them as it keeps the members.
Map<String, dynamic> viewRow(RecordVersionArray ids,
        RecordVersionArray children, bool confirmed, int now) =>
    {
      'ids': [
        for (final (id, version) in ids) [id, version]
      ],
      'children': [
        for (final (id, version) in children) [id, version]
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

/// A query's primary window edges, read through the graph (`$in->table`). The
/// `_00_list_ref*` tables have no index on `in`, so `WHERE in = $in` scanned
/// the whole table, which on the shared `_00_list_ref_anon` is every signed-out
/// visitor's edges. The edge table's select permission still applies.
String listRefSelect(String table) =>
    'SELECT out, version FROM \$in->$table WHERE parent IS NONE';

/// A `.related()` query's SUBQUERY child edges. The SSP tags each matched child
/// edge with `parent`/`parent_rel` at any nesting depth, so `parent IS NOT NONE`
/// is exactly the complement of [listRefSelect]'s primary window.
String subqueryListRefSelect(String table) =>
    'SELECT out, version FROM \$in->$table WHERE parent IS NOT NONE';

String queryRowCountSelect() =>
    'SELECT VALUE { rowCount: rowCount, state: state } FROM ONLY \$in';

/// Every edge of many queries in one flat list; `in` names the query.
String listRefBatchSelect(String table) =>
    'SELECT in, out, version, parent FROM \$ins->$table';

String queryRowCountBatchSelect() =>
    'SELECT VALUE { id: id, rowCount: rowCount, state: state } FROM \$ins';

/// The single-query read: edges, meta and subquery children in one request.
String singleSnapshotSelect(String table) =>
    '${listRefSelect(table)};\n${queryRowCountSelect()};\n${subqueryListRefSelect(table)}';

/// The many-query read: all edges plus all metas in one request.
String batchSnapshotSelect(String table) =>
    '${listRefBatchSelect(table)};\n${queryRowCountBatchSelect()}';

/// The session's one LIVE subscription on its `_00_list_ref` table.
///
/// [inlineBodies] adds `FETCH out`, which resolves the edge's target into the
/// notification so the body arrives with the doorbell.
///
/// On the anon table it is filtered to this client's own edges. Every
/// signed-out session shares that table, so unfiltered each one received (and
/// the server fetched a body for) every anonymous edge change of every other
/// session, and the client then dropped all but its own. An edge carries the
/// `session::id()` of whichever socket last registered its view, which is a
/// socket of this client but not necessarily the current one: a view stays
/// registered across a reconnect, so its older edges still name an earlier
/// socket. [sessionIds] are those earlier sockets. The current one is matched
/// by `session::id()` itself, which SurrealDB evaluates in the subscriber's own
/// session, so the filter holds before the client has learned its new id.
///
/// The cast is load-bearing: `session::id()` is a uuid, `clientId` a string,
/// and without it nothing matches. Per-user tables need no filter. Mirrors
/// `listRefLive` in `packages/core/src/query/sql.ts`.
({String sql, Map<String, dynamic>? vars}) listRefLive(String table,
    {required bool inlineBodies, required List<String> sessionIds}) {
  final join = inlineBodies ? ' FETCH out' : '';
  if (table != anonListRefTable) {
    return (sql: 'LIVE SELECT * FROM $table$join', vars: null);
  }
  return (
    sql: 'LIVE SELECT * FROM $table WHERE clientId = <string>session::id() '
        'OR clientId IN \$sids$join',
    vars: {'sids': List<String>.of(sessionIds)},
  );
}

class RegisterPayload {
  const RegisterPayload({
    required this.id,
    required this.surql,
    required this.params,
    required this.ttl,
    this.owner,
  });

  final RecordId id;
  final String surql;
  final Map<String, dynamic> params;
  final QueryTimeToLive ttl;

  /// The registering client's secret. `fn::query::register` forwards only its
  /// hash, and the SSP lets a registration onto an existing anonymous view
  /// only with the hash that view was registered with: every signed-out
  /// visitor is `anon`, and anyone can read an anonymous view's id. Kept out
  /// of [params], which name the computation the SSP shares between views.
  final String? owner;

  Map<String, dynamic> toJson() => {
        'id': id,
        'surql': surql,
        'params': params,
        'ttl': ttl,
        if (owner != null) 'owner': owner,
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
