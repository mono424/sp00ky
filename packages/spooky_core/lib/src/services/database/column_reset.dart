import '../../utils/parser.dart' show ColumnSchema;

/// Cached bodies outlive a schema change on this store, and that loses columns
/// for good (TS `sqlite-column-reset.ts`).
///
/// A body is landed through `cleanRecord`, which keeps only the columns the
/// running app's schema declares, and is stored at the server's `_00_rv`. The
/// store keeps rows across schema changes (see `LocalMigrator`), so a build
/// that fetched a row after the server added and backfilled a column stored it
/// WITHOUT that column at the new version. The next build knows the column,
/// but its local version equals the server's, so the fetch plan never asks for
/// the row again and the column stays missing until something else bumps the
/// row.
///
/// So the store remembers each table's column set (`_00_kv`, key
/// [columnSetsKey]). On boot, before the prime reads the versions, a table
/// whose set changed, or every table when no set was recorded yet, gets its
/// cached `_00_rv` reset to 0. The bodies stay and keep rendering; any view
/// that names one of those rows sees it as stale and fetches it again, whole.
/// Rows with an unsettled local write keep their version.

/// `_00_kv` key holding the recorded column sets.
const columnSetsKey = '_00_column_sets';

/// The column set of every table in [tables], as `cleanRecord` sees it: the
/// sorted declared column names (`columnsFor`).
Map<String, String> columnFingerprints(
    Map<String, dynamic> schema, Iterable<String> tables) {
  final out = <String, String>{};
  for (final table in tables) {
    final entry = schema[table];
    if (entry is! Map) continue;
    final columns =
        entry['columns'] is Map ? entry['columns'] as Map : entry;
    final names = [
      for (final e in columns.entries)
        if (e.value is ColumnSchema) e.key as String
    ]..sort();
    out[table] = names.join(',');
  }
  return out;
}

/// The tables of [current] whose rows need their versions reset, given the
/// [stored] sets (null when none were recorded) and the tables that have rows.
List<String> changedTables(Map<String, String>? stored,
    Map<String, String> current, Set<String> tablesWithRows) {
  return [
    for (final e in current.entries)
      if ((stored == null || stored[e.key] != e.value) &&
          tablesWithRows.contains(e.key))
        e.key
  ];
}

/// Whether [stored] already equals [current].
bool sameColumnSets(Map<String, String>? stored, Map<String, String> current) {
  if (stored == null || stored.length != current.length) return false;
  for (final e in current.entries) {
    if (stored[e.key] != e.value) return false;
  }
  return true;
}
