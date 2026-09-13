import '../utils/sort_rows.dart';

/// How to materialize a windowed (`LIMIT n START m`, m>0) query's rows.
///
/// Divergence from the TS core: there, `buildWindowMaterialization` rewrites the
/// surql into `SELECT <projection> FROM $__win <ORDER BY>` and re-runs it against
/// the local SurrealDB. sqlite can't run SurrealQL, so the Dart port materializes
/// the id-set with [LocalDatabaseService.getById] and re-applies the parsed
/// [orderBy] in Dart via [sortRows]. The `SELECT` projection is therefore ignored
/// (full documents are returned) — the same as the non-windowed Dart read path.
class WindowMaterialization {
  const WindowMaterialization({required this.orderBy});

  /// The query's top-level `ORDER BY` terms, empty when it has none (then the
  /// id-set's own order is kept).
  final List<OrderByTerm> orderBy;
}

/// Detect a windowed query and extract what materializing it needs
/// (TS `buildWindowMaterialization`).
///
/// Returns null for non-offset queries (`START` absent or 0), so only the broken
/// case changes behavior: for those the caller keeps the normal id-set path.
///
/// Why windowed queries need this at all: rows are materialized from the DBSP
/// view's `localArray`, but the local store is shared and, with sparse windowing,
/// holds only some pages. Re-deriving page 2 from whatever is resident locally
/// returns the wrong rows (or none — the "page 2 returns 0 rows" bug), so the
/// window is taken from the server's authoritative `_00_list_ref` instead.
WindowMaterialization? buildWindowMaterialization(String surql) {
  final clauses = _scanTopLevelClauses(surql);
  if (clauses.startValue == null || clauses.startValue! <= 0) return null;
  if (clauses.fromIndex == null) return null;
  return WindowMaterialization(orderBy: _parseOrderBy(surql, clauses));
}

/// Slice out the top-level `ORDER BY` clause and parse it into field/direction
/// pairs. Ends at whichever of `LIMIT` / `START` / `;` / end-of-string comes next.
List<OrderByTerm> _parseOrderBy(String surql, _TopLevelClauses clauses) {
  final start = clauses.orderByIndex;
  if (start == null) return const [];
  final ends = [
    clauses.limitIndex,
    clauses.startIndex,
    clauses.semicolonIndex,
    surql.length,
  ].whereType<int>().where((n) => n > start).toList()
    ..sort();
  final clause = surql.substring(start, ends.first);
  // Drop the leading `ORDER BY` keyword itself.
  final body = clause.replaceFirst(
      RegExp(r'^ORDER\s+BY\s+', caseSensitive: false), '');

  final terms = <OrderByTerm>[];
  for (final raw in body.split(',')) {
    final parts = raw.trim().split(RegExp(r'\s+'));
    if (parts.isEmpty || parts.first.isEmpty) continue;
    final field = parts.first;
    // SurrealQL allows collation modifiers (COLLATE / NUMERIC) between the field
    // and the direction; the direction is whichever DESC appears in the term.
    final desc = parts.skip(1).any((p) => p.toUpperCase() == 'DESC');
    terms.add((field, desc ? 'desc' : 'asc'));
  }
  return terms;
}

class _TopLevelClauses {
  int? fromIndex;
  int? orderByIndex;
  int? limitIndex;
  int? startIndex;
  int? startValue;
  int? semicolonIndex;
}

/// Single pass over the query tracking paren depth and single-quoted strings, so
/// clauses inside subqueries (which live in parens) are ignored: only the
/// outermost (depth-0) clause keywords are recorded (TS `scanTopLevelClauses`).
_TopLevelClauses _scanTopLevelClauses(String sql) {
  final out = _TopLevelClauses();
  var depth = 0;
  var inStr = false;

  for (var i = 0; i < sql.length; i++) {
    final ch = sql[i];
    if (inStr) {
      if (ch == r'\') {
        i++; // skip the escaped char
      } else if (ch == "'") {
        inStr = false;
      }
      continue;
    }
    if (ch == "'") {
      inStr = true;
      continue;
    }
    if (ch == '(') {
      depth++;
      continue;
    }
    if (ch == ')') {
      depth--;
      continue;
    }
    if (depth != 0) continue;
    if (ch == ';' && out.semicolonIndex == null) {
      out.semicolonIndex = i;
      continue;
    }

    // Only test keywords at a word boundary.
    if (!_isWordBoundary(sql, i)) continue;
    if (out.fromIndex == null && _matchKeyword(sql, i, 'FROM')) {
      out.fromIndex = i;
      continue;
    }
    // FROM must come before the trailing clauses; only record them once seen.
    if (out.fromIndex == null) continue;
    if (out.orderByIndex == null && _matchKeyword(sql, i, 'ORDER BY')) {
      out.orderByIndex = i;
      continue;
    }
    if (out.limitIndex == null && _matchKeyword(sql, i, 'LIMIT')) {
      out.limitIndex = i;
      continue;
    }
    if (out.startIndex == null && _matchKeyword(sql, i, 'START')) {
      out.startIndex = i;
      out.startValue = _readNumberAfter(sql, i + 'START'.length);
      continue;
    }
  }
  return out;
}

final _wordChar = RegExp(r'[A-Za-z0-9_]');
final _whitespace = RegExp(r'\s');

bool _isWordBoundary(String sql, int i) =>
    i == 0 || !_wordChar.hasMatch(sql[i - 1]);

/// Case-insensitive keyword match where internal whitespace (e.g. `ORDER BY`)
/// matches any run of whitespace, and the keyword ends on a non-word char.
bool _matchKeyword(String sql, int i, String keyword) {
  final parts = keyword.split(' ');
  var pos = i;
  for (var p = 0; p < parts.length; p++) {
    final word = parts[p];
    if (pos + word.length > sql.length) return false;
    if (sql.substring(pos, pos + word.length).toUpperCase() != word) {
      return false;
    }
    pos += word.length;
    if (p < parts.length - 1) {
      final wsStart = pos;
      while (pos < sql.length && _whitespace.hasMatch(sql[pos])) {
        pos++;
      }
      if (pos == wsStart) return false; // required whitespace
    }
  }
  return pos >= sql.length || !_wordChar.hasMatch(sql[pos]);
}

int? _readNumberAfter(String sql, int from) {
  var pos = from;
  while (pos < sql.length && _whitespace.hasMatch(sql[pos])) {
    pos++;
  }
  final match = RegExp(r'^\d+').firstMatch(sql.substring(pos));
  return match == null ? null : int.parse(match.group(0)!);
}
