import '../kernel/effects.dart';
import '../kernel/saga.dart';
import '../modules/query_builder.dart' show RelationPlan;
import '../utils/sort_rows.dart';

/// Nesting depth beyond which a relation tree is treated as cyclic
/// (TS `MAX_RELATION_DEPTH`).
const int maxRelationDepth = 12;

/// Thrown when a `.related()` tree nests past [maxRelationDepth]
/// (TS `RelationCycleError`).
class RelationCycleError extends Error {
  RelationCycleError(this.path);
  final List<String> path;

  @override
  String toString() =>
      'RelationCycleError: relation nesting exceeded $maxRelationDepth at ${path.join(' -> ')}';
}

/// Fetches candidate child rows for one relation level.
abstract interface class RelationFetcher {
  /// Rows of [table] whose [matchField] is one of [keys].
  Future<List<Map<String, dynamic>>> fetchRelation({
    required String table,
    required String matchField,
    required List<Object?> keys,
  });
}

/// [RelationFetcher] over the local store, through the effect context.
///
/// Divergence from the browser core, which pushes the match into a local
/// SurrealQL / SQLite query: this scans the logical table and filters in Dart.
/// The local store is a document table keyed only by `(table, id)`, so a field
/// match has no index to use either way. Fine for a client-sized cache.
class CtxRelationFetcher implements RelationFetcher {
  const CtxRelationFetcher(this._ctx);
  final Ctx _ctx;

  @override
  Future<List<Map<String, dynamic>>> fetchRelation({
    required String table,
    required String matchField,
    required List<Object?> keys,
  }) async {
    final wanted = {for (final key in keys) stableKey(key)};
    final rows = await _ctx(Fx.localGetAll(table));
    return [
      for (final row in rows)
        if (wanted.contains(stableKey(row[matchField]))) row,
    ];
  }
}

/// Resolve a query's `.related()` tree by level-ordered, batched fan-out — the
/// engine-neutral replacement for SurrealQL's nested `(SELECT …) AS alias`
/// projections (TS `resolveRelations`). One fetch per relation per level, never
/// per row.
///
/// Correlation mirrors the emitted subquery:
/// - `one`  -> parent[foreignKeyField] == child.id, attach the first match or null
/// - `many` -> child[foreignKeyField] == parent.id, attach the list
///
/// [parents] is mutated in place, each alias appended LAST so key order matches
/// SurrealQL's `SELECT *, <sub> AS alias`.
///
/// Throws [RelationCycleError] when nesting exceeds [maxRelationDepth].
Future<void> resolveRelations(
  List<Map<String, dynamic>> parents,
  List<RelationPlan> relations,
  RelationFetcher fetcher, {
  int depth = 0,
  List<String> path = const [],
}) async {
  if (relations.isEmpty || parents.isEmpty) return;
  if (depth >= maxRelationDepth) {
    throw RelationCycleError([...path, relations.map((r) => r.alias).join('|')]);
  }
  for (final relation in relations) {
    await _resolveOne(parents, relation, fetcher, depth, path);
  }
}

Future<void> _resolveOne(
  List<Map<String, dynamic>> parents,
  RelationPlan relation,
  RelationFetcher fetcher,
  int depth,
  List<String> path,
) async {
  final isOne = relation.isOne;
  // The child field to match parent keys against.
  final matchField = isOne ? 'id' : relation.foreignKeyField;
  Object? parentKeyOf(Map<String, dynamic> parent) =>
      isOne ? parent[relation.foreignKeyField] : parent['id'];

  // Distinct, non-null correlation keys: an absent foreign key contributes
  // nothing, so it never triggers a spurious match-everything fetch.
  final keys = <String, Object?>{};
  for (final parent in parents) {
    final key = parentKeyOf(parent);
    if (key == null) continue;
    keys.putIfAbsent(stableKey(key), () => key);
  }

  final grouped = <String, List<Map<String, dynamic>>>{};
  if (keys.isNotEmpty) {
    final children = await fetcher.fetchRelation(
      table: relation.table,
      matchField: matchField,
      keys: keys.values.toList(),
    );

    // Recurse BEFORE grouping: each child is itself a parent for its own
    // relations, and resolving the deduped child set once keeps nested fan-out
    // at O(depth) batches. Children later dropped by a per-parent limit carry
    // resolved nested data harmlessly — they reach no output row.
    await resolveRelations(
      children,
      relation.relations,
      fetcher,
      depth: depth + 1,
      path: [...path, relation.alias],
    );

    for (final child in children) {
      grouped.putIfAbsent(stableKey(child[matchField]), () => []).add(child);
    }
  }

  for (final parent in parents) {
    final key = parentKeyOf(parent);
    var bucket = key == null
        ? <Map<String, dynamic>>[]
        : [...?grouped[stableKey(key)]];

    // Filter, then order, then limit — PER PARENT. A shared batch fetch can't
    // express "top N per parent", so the shaping happens here.
    if (relation.where.isNotEmpty) {
      bucket = [
        for (final row in bucket)
          if (_matchesWhere(row, relation.where)) row,
      ];
    }
    if (relation.orderBy.isNotEmpty) {
      bucket = sortRows(bucket, relation.orderBy);
    }
    final limit = relation.limit;
    if (limit != null && bucket.length > limit) {
      bucket = bucket.sublist(0, limit);
    }

    // Remove first so a re-resolved alias lands LAST in key order, matching
    // SurrealQL's `SELECT *, <sub> AS alias`.
    parent.remove(relation.alias);
    parent[relation.alias] = isOne ? (bucket.isEmpty ? null : bucket.first) : bucket;
  }
}

bool _matchesWhere(Map<String, dynamic> row, Map<String, Object?> where) {
  for (final entry in where.entries) {
    if (stableKey(row[entry.key]) != stableKey(entry.value)) return false;
  }
  return true;
}
