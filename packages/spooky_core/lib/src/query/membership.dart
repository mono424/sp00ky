import '../state/lifecycle.dart' show QueryPhase;
import '../types.dart';

/// What a membership read may do with the id-set it got back.
enum MembershipOutcome {
  /// The set is the server's answer; commit it.
  applied,

  /// The server's `_00_query` row is gone while we hold rows.
  viewLost,

  /// The read proves nothing; keep what we have.
  ignored,
}

/// The durable `_00_view` row: the last server-confirmed membership.
class DurableView {
  const DurableView({required this.ids, required this.confirmed});
  final RecordVersionArray ids;
  final bool confirmed;
}

DurableView? parseViewRow(Map<String, dynamic>? row) {
  if (row == null) return null;
  final ids = row['ids'];
  if (ids is! List) return null;
  final pairs = <RecordVersion>[];
  for (final entry in ids) {
    if (entry is List && entry.length >= 2) {
      pairs.add((entry[0].toString(), (entry[1] as num).toInt()));
    }
  }
  return DurableView(ids: pairs, confirmed: row['confirmed'] == true);
}

/// "Resolved before": the server answered this query on this device at some
/// point. An unconfirmed empty row cannot be told apart from one written before
/// the marker existed, so it does not count.
bool isResolvedBefore(DurableView? view) =>
    view != null && (view.ids.isNotEmpty || view.confirmed);

ServerViewMeta metaFromRow(Object? row) {
  if (row is! Map) return ServerViewMeta.absent;
  final rawState = row['state'];
  final state = rawState == 'materializing' || rawState == 'ready'
      ? rawState as String
      : null;
  final rowCount = row['rowCount'];
  return ServerViewMeta(
    present: true,
    rowCount: rowCount is num ? rowCount.toInt() : null,
    state: state,
  );
}

/// Collapse duplicate `(id, version)` pairs to one per id, keeping the highest
/// version.
RecordVersionArray dedupeRecordVersions(RecordVersionArray pairs) {
  if (pairs.length < 2) return pairs;
  final best = <String, int>{};
  for (final (id, version) in pairs) {
    final prev = best[id];
    if (prev == null || version > prev) best[id] = version;
  }
  if (best.length == pairs.length) return pairs;
  return [for (final e in best.entries) (e.key, e.value)];
}

class MembershipDecisionInput {
  const MembershipDecisionInput({
    required this.phase,
    required this.held,
    required this.remoteArray,
    this.meta,
    this.verifiedRemoval = false,
  });

  final QueryPhase phase;

  /// Number of ids currently held as membership.
  final int held;
  final RecordVersionArray remoteArray;
  final ServerViewMeta? meta;
  final bool verifiedRemoval;
}

/// Whether a server id-set may be applied. A NON-EMPTY set is always the
/// server's answer. An EMPTY set is believed only when the server stands behind
/// it: the `_00_query` row is present, `ready` (or pre-`state`), and reports
/// zero rows. A missing row while we hold membership is a lost view.
MembershipOutcome decideMembershipOutcome(MembershipDecisionInput input) {
  if (input.remoteArray.isNotEmpty || input.verifiedRemoval) {
    return MembershipOutcome.applied;
  }
  final meta = input.meta;
  if (meta == null || !meta.present) {
    return input.held > 0 || input.phase != QueryPhase.cold
        ? MembershipOutcome.viewLost
        : MembershipOutcome.ignored;
  }
  final knownEmpty = meta.rowCount == 0 && meta.state != 'materializing';
  return knownEmpty ? MembershipOutcome.applied : MembershipOutcome.ignored;
}

// ---- list_ref snapshots ------------------------------------------------------

/// A statement that did not answer (an ERR, or no result at all). Kept apart
/// from a `null` result, which is the server saying "no such row": reading a
/// failed statement as a missing row turned every timeout under load into a
/// lost view and re-registered it, over and over (whitepawn, 2026-09-16).
const Object statementFailed = _StatementFailed();

class _StatementFailed {
  const _StatementFailed();
}

class ListRefSnapshot {
  ListRefSnapshot({
    required this.primary,
    required this.subquery,
    required this.meta,
  });

  RecordVersionArray primary;
  RecordVersionArray subquery;
  ServerViewMeta meta;
}

RecordVersionArray _toPairs(Object? rows) {
  if (rows is! List) return const [];
  final pairs = <RecordVersion>[
    for (final r in rows)
      if (r is Map)
        (r['out'].toString(), ((r['version'] as num?) ?? 0).toInt()),
  ];
  return dedupeRecordVersions(pairs);
}

/// Fold the single-query statement batch (edges, meta, children) into a
/// snapshot. Returns null when any of the three did not answer (the edges not
/// a list, or [statementFailed]), which the caller treats as a failed read:
/// half an answer is not one, and a missing meta would read as a lost view.
ListRefSnapshot? snapshotFromSingle(
    Object? items, Object? metaRow, Object? children) {
  if (items is! List ||
      identical(metaRow, statementFailed) ||
      identical(children, statementFailed)) {
    return null;
  }
  return ListRefSnapshot(
    primary: _toPairs(items),
    subquery: _toPairs(children),
    meta: metaFromRow(metaRow),
  );
}

/// Fold the many-query statement batch into one snapshot per hash. Every hash
/// in [hashById] gets a snapshot; a query whose row did not come back reads
/// `present: false`. Empty when either statement did not answer.
Map<String, ListRefSnapshot> snapshotsFromBatch(
  Object? edges,
  Object? counts,
  Map<String, String> hashById,
) {
  final out = <String, ListRefSnapshot>{};
  if (edges is! List || identical(counts, statementFailed)) return out;
  for (final hash in hashById.values) {
    out[hash] = ListRefSnapshot(
      primary: [],
      subquery: [],
      meta: ServerViewMeta.absent,
    );
  }
  for (final row in edges) {
    if (row is! Map) continue;
    final hash = hashById[row['in'].toString()];
    if (hash == null) continue;
    final snapshot = out[hash]!;
    final pair =
        (row['out'].toString(), ((row['version'] as num?) ?? 0).toInt());
    if (row['parent'] == null) {
      snapshot.primary.add(pair);
    } else {
      snapshot.subquery.add(pair);
    }
  }
  for (final snapshot in out.values) {
    snapshot.primary = dedupeRecordVersions(snapshot.primary);
    snapshot.subquery = dedupeRecordVersions(snapshot.subquery);
  }
  if (counts is List) {
    for (final count in counts) {
      if (count is! Map || count['id'] == null) continue;
      final hash = hashById[count['id'].toString()];
      if (hash == null) continue;
      out[hash]!.meta = metaFromRow(count);
    }
  }
  return out;
}

/// A batch that must not be believed at face value: a query we hold rows for
/// whose `_00_query` row did not come back, or no edge at all while the server
/// still reports rows for a held query. Such hashes are re-read one at a time.
List<String> suspectHashes(
  Map<String, ListRefSnapshot> snapshots,
  Map<String, int> heldCounts,
  int edgeCount,
) {
  final out = <String>[];
  for (final entry in snapshots.entries) {
    final held = heldCounts[entry.key] ?? 0;
    if (held == 0) continue;
    final meta = entry.value.meta;
    if (!meta.present || (edgeCount == 0 && (meta.rowCount ?? 0) > 0)) {
      out.add(entry.key);
    }
  }
  return out;
}

/// A stored `_00_view` row as the orphan collector sees it: its view key (null
/// when the row carries no id, so it can never be retired), every id it vouches
/// for (members and subquery children) and when a server answer last wrote it.
typedef ViewIndexRow = ({String? key, Set<String> ids, int updatedAt});

ViewIndexRow parseViewIndexRow(Map<String, dynamic> row) {
  final id = row['id']?.toString();
  final ids = <String>{};
  for (final field in const ['ids', 'children']) {
    final raw = row[field];
    if (raw is! List) continue;
    for (final entry in raw) {
      if (entry is List && entry.isNotEmpty) ids.add(entry.first.toString());
    }
  }
  final updatedAt = row['updatedAt'];
  return (
    key: id?.substring(id.indexOf(':') + 1),
    ids: ids,
    updatedAt: updatedAt is num ? updatedAt.toInt() : 0,
  );
}
