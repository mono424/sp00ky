import 'dart:math' as math;

import '../kernel/constants.dart';
import '../surreal/value.dart';
import '../types.dart';
import '../utils/error_classification.dart';
import '../utils/record_id_utils.dart';

/// Default cadence for the `_00_list_ref` poll fallback (ms).
const int defaultListRefPollIntervalMs = 500;

/// LIVE-healthy cooldown window (ms): a LIVE event within this window backs the
/// poll off to [liveHealthyPollIntervalMs].
const int liveHealthyCooldownMs = 5000;

/// Poll interval while LIVE is delivering events (ms).
const int liveHealthyPollIntervalMs = 5000;

/// Incrementally maintains a local version array and diffs it against the
/// remote array (TS `ArraySyncer`).
class ArraySyncer {
  ArraySyncer(RecordVersionArray localArray, RecordVersionArray remoteArray)
      : _remoteArray = [...remoteArray]..sort((a, b) => a.$1.compareTo(b.$1)),
        _localArray = [...localArray]..sort((a, b) => a.$1.compareTo(b.$1));

  RecordVersionArray _localArray;
  final RecordVersionArray _remoteArray;
  bool _needsSort = false;

  void insert(String recordId, int version) {
    _localArray.add((recordId, version));
    _needsSort = true;
  }

  void update(String recordId, int version) {
    _localArray = _localArray.map((record) {
      if (record.$1 == recordId) {
        _needsSort = true;
        return (recordId, version);
      }
      return record;
    }).toList();
  }

  void delete(String recordId) {
    _localArray = _localArray.where((record) => record.$1 != recordId).toList();
  }

  RecordVersionDiff? nextSet() {
    if (_needsSort) {
      _localArray.sort((a, b) => a.$1.compareTo(b.$1));
      _needsSort = false;
    }
    return diffRecordVersionArray(_localArray, _remoteArray);
  }
}

/// Diff two version arrays into added/updated/removed (TS `diffRecordVersionArray`).
RecordVersionDiff diffRecordVersionArray(
  RecordVersionArray? local,
  RecordVersionArray? remote,
) {
  final localMap = {for (final e in local ?? const []) e.$1: e.$2};
  final remoteMap = {for (final e in remote ?? const []) e.$1: e.$2};

  final added = <String>[];
  final updated = <String>[];
  final removed = <String>[];

  remoteMap.forEach((recordId, remoteVersion) {
    final localVersion = localMap[recordId];
    if (localVersion == null) {
      added.add(recordId);
    } else if (localVersion < remoteVersion) {
      updated.add(recordId);
    }
  });

  for (final recordId in localMap.keys) {
    if (!remoteMap.containsKey(recordId)) removed.add(recordId);
  }

  return RecordVersionDiff(
    added: added
        .map<({RecordId id, int version})>(
            (id) => (id: parseRecordIdString(id), version: remoteMap[id]!))
        .toList(),
    updated: updated
        .map<({RecordId id, int version})>(
            (id) => (id: parseRecordIdString(id), version: remoteMap[id]!))
        .toList(),
    removed: removed.map(parseRecordIdString).toList(),
  );
}

/// Build a one-record-op diff from a LIVE `_00_list_ref` change
/// (TS `createDiffFromDbOp`). Returns an empty diff if the cached version is
/// already at least [version].
RecordVersionDiff createDiffFromDbOp(
  String op,
  RecordId recordId,
  int version, [
  RecordVersionArray? versions,
]) {
  final encoded = encodeRecordId(recordId);
  final old = versions?.where((r) => r.$1 == encoded).firstOrNull;
  if (old != null && old.$2 >= version) {
    return RecordVersionDiff(added: [], updated: [], removed: []);
  }
  switch (op) {
    case 'CREATE':
      return RecordVersionDiff(
          added: [(id: recordId, version: version)], updated: [], removed: []);
    case 'UPDATE':
      return RecordVersionDiff(
          added: [], updated: [(id: recordId, version: version)], removed: []);
    default:
      return RecordVersionDiff(added: [], updated: [], removed: [recordId]);
  }
}

/// Resolve the effective poll interval; non-positive falls back to the default.
int resolveListRefPollInterval(int? opt) {
  if (opt == null || opt <= 0) return defaultListRefPollIntervalMs;
  return opt;
}

/// Ceiling for the adaptive `_00_list_ref` poll backoff (ms)
/// (TS `LIST_REF_POLL_MAX_INTERVAL_MS`). An idle client coasts up to this
/// cadence, so the worst-case catch-up latency for a missed cross-session
/// change stays at the 5s the codebase already treats as acceptable.
const int listRefPollMaxIntervalMs = 5000;

/// Adaptive poll delay (TS `listRefPollDelayMs`): stay at the responsive
/// [baseIntervalMs] while changes are arriving, and exponentially back off
/// toward [maxIntervalMs] while `_00_list_ref` is quiet.
///
/// [idleStreak] is the count of consecutive poll cycles that observed no
/// change. [Sp00kySync] resets it to 0 whenever a poll detects a real
/// remoteArray change OR a LIVE event lands, so any activity snaps the poll
/// straight back to [baseIntervalMs].
///
/// This supersedes [nextPollDelayMs], which slowed the poll only while LIVE was
/// delivering; the cross-session LIVE-permission gap means LIVE frequently never
/// fires, which left a fully idle client polling every base interval forever.
/// Backing off on observed idleness covers the LIVE-healthy case for free (LIVE
/// applies the change, the next poll sees nothing new, the streak grows).
int listRefPollDelayMs({
  required int idleStreak,
  required int baseIntervalMs,
  int maxIntervalMs = listRefPollMaxIntervalMs,
}) {
  final cap =
      maxIntervalMs > baseIntervalMs ? maxIntervalMs : baseIntervalMs;
  if (idleStreak <= 0) return baseIntervalMs;
  // Clamp the exponent so a long-idle client can't overflow.
  final exponent = idleStreak > 30 ? 30 : idleStreak;
  final delay = baseIntervalMs * (1 << exponent);
  return delay < cap ? delay : cap;
}

/// Order-insensitive equality for two [RecordVersionArray]s
/// (TS `recordVersionArraysEqual`). The `_00_list_ref` SELECT has no
/// `ORDER BY`, so row order can differ between polls without anything having
/// changed; comparing as an id -> version map avoids false "changed" verdicts
/// that would defeat the idle backoff. Record ids are unique within a query's
/// list_ref, so a map is a faithful representation.
bool recordVersionArraysEqual(RecordVersionArray a, RecordVersionArray b) {
  if (a.length != b.length) return false;
  final byId = {for (final e in a) e.$1: e.$2};
  for (final e in b) {
    if (byId[e.$1] != e.$2) return false;
  }
  return true;
}

/// Pick the next poll delay based on LIVE health (TS `nextPollDelayMs`). Pure
/// for unit-testing.
///
/// Superseded by [listRefPollDelayMs], which backs off on observed change
/// activity (LIVE *or* poll-detected) rather than LIVE liveness alone. Kept
/// (and tested) for reference, matching the TS core.
@Deprecated('Use listRefPollDelayMs')
int nextPollDelayMs({
  required int now,
  required int? lastLiveEventAt,
  required int baseIntervalMs,
  int cooldownMs = liveHealthyCooldownMs,
  int healthyIntervalMs = liveHealthyPollIntervalMs,
}) {
  if (lastLiveEventAt == null) return baseIntervalMs;
  final sinceLive = now - lastLiveEventAt;
  if (sinceLive < 0 || sinceLive >= cooldownMs) return baseIntervalMs;
  return healthyIntervalMs > baseIntervalMs
      ? healthyIntervalMs
      : baseIntervalMs;
}

extension _FirstOrNull<E> on Iterable<E> {
  E? get firstOrNull {
    final it = iterator;
    return it.moveNext() ? it.current : null;
  }
}

// ---- poll chunking -----------------------------------------------------------

/// Known edges one poll round trip may carry before the cycle is split.
const int listRefPollRowBudget = listRefRowBudget;

/// A view at or past this many edges is "large": it rides on LIVE and is only
/// re-polled every [listRefPollLargeViewMinAgeMs].
const int listRefPollLargeViewRows = listRefLargeViewEdges;
const int listRefPollLargeViewMinAgeMs = listRefLargeViewPollMs;

class ListRefPollCandidate {
  const ListRefPollCandidate({
    required this.hash,
    required this.rows,
    required this.lastPolledAt,
  });

  final String hash;

  /// Edges the client currently holds for the query (`remoteArray.length`).
  final int rows;

  /// When the query was last refreshed by the poll; `0` = never.
  final int lastPolledAt;
}

/// Split the active queries into the round trips of one poll cycle.
///
/// Every query that is due is refreshed once per cycle, oldest refresh first,
/// packed greedily into chunks of at most [rowBudget] known edges so one
/// response never carries a whole page's worth of ids at once. A view with
/// [largeViewRows] or more edges is only due once [largeViewMinAgeMs] has
/// passed since its last refresh (LIVE remains its primary path); a chunk
/// always holds at least one query, so a single huge view still refreshes.
List<List<String>> planListRefPollChunks(
  List<ListRefPollCandidate> candidates, {
  required int now,
  int rowBudget = listRefPollRowBudget,
  int largeViewRows = listRefPollLargeViewRows,
  int largeViewMinAgeMs = listRefPollLargeViewMinAgeMs,
}) {
  final due = candidates
      .where((c) =>
          c.rows < largeViewRows || now - c.lastPolledAt >= largeViewMinAgeMs)
      .toList()
    ..sort((a, b) {
      final byAge = a.lastPolledAt.compareTo(b.lastPolledAt);
      return byAge != 0 ? byAge : a.hash.compareTo(b.hash);
    });
  final chunks = <List<String>>[];
  var current = <String>[];
  var currentRows = 0;
  for (final c in due) {
    final cost = c.rows < 1 ? 1 : c.rows;
    if (current.isNotEmpty && currentRows + cost > rowBudget) {
      chunks.add(current);
      current = [];
      currentRows = 0;
    }
    current.add(c.hash);
    currentRows += cost;
  }
  if (current.isNotEmpty) chunks.add(current);
  return chunks;
}

// ---- diff application --------------------------------------------------------

/// Apply a [RecordVersionDiff] to a [RecordVersionArray], returning a new sorted
/// array.
RecordVersionArray applyRecordVersionDiff(
    RecordVersionArray current, RecordVersionDiff diff) {
  final byId = {for (final (id, v) in current) id: v};
  for (final id in diff.removed) {
    byId.remove(encodeRecordId(id));
  }
  for (final item in diff.added) {
    byId[encodeRecordId(item.id)] = item.version;
  }
  for (final item in diff.updated) {
    byId[encodeRecordId(item.id)] = item.version;
  }
  final out = [for (final e in byId.entries) (e.key, e.value)]
    ..sort((a, b) => a.$1.compareTo(b.$1));
  return out;
}

// ---- health ------------------------------------------------------------------

class HealthInput {
  const HealthInput({
    required this.health,
    required this.consecutiveFailures,
    required this.hasSyncedOnce,
  });

  final SyncHealth health;
  final int consecutiveFailures;
  final bool hasSyncedOnce;
}

class HealthOutput extends HealthInput {
  const HealthOutput({
    required super.health,
    required super.consecutiveFailures,
    required super.hasSyncedOnce,
    this.degradedNow = false,
    this.recoveredNow = false,
  });

  /// Crossed into degraded on this outcome: start self-heal.
  final bool degradedNow;

  /// Left degraded on this outcome: stop self-heal.
  final bool recoveredNow;
}

/// Fold one sync round's outcome into health. A single failure is absorbed;
/// [degradeAfter] consecutive ones flip to degraded, the next success flips
/// back. `degradeAfter <= 0` disables reporting.
HealthOutput nextHealth(
    HealthInput input, bool ok, Object? error, int degradeAfter) {
  if (degradeAfter <= 0) {
    return HealthOutput(
      health: input.health,
      consecutiveFailures: input.consecutiveFailures,
      hasSyncedOnce: input.hasSyncedOnce,
    );
  }
  if (ok) {
    if (input.consecutiveFailures == 0) {
      return HealthOutput(
        health: input.health.copyWith(everConnected: true),
        consecutiveFailures: input.consecutiveFailures,
        hasSyncedOnce: true,
      );
    }
    final recovered = input.health.status == SyncHealthStatus.degraded;
    return HealthOutput(
      health: SyncHealth(
        status: SyncHealthStatus.healthy,
        consecutiveFailures: input.health.consecutiveFailures,
        everConnected: true,
        connection: input.health.connection,
      ),
      consecutiveFailures: 0,
      hasSyncedOnce: true,
      recoveredNow: recovered,
    );
  }
  final consecutiveFailures = input.consecutiveFailures + 1;
  final kind = classifySyncError(error);
  final message = error?.toString() ?? 'unknown error';
  final degradedNow = input.health.status != SyncHealthStatus.degraded &&
      consecutiveFailures >= degradeAfter;
  return HealthOutput(
    health: SyncHealth(
      status: degradedNow ? SyncHealthStatus.degraded : input.health.status,
      consecutiveFailures: consecutiveFailures,
      kind: kind,
      error: message,
      everConnected: input.health.everConnected,
      connection: input.health.connection,
    ),
    consecutiveFailures: consecutiveFailures,
    hasSyncedOnce: input.hasSyncedOnce,
    degradedNow: degradedNow,
  );
}

int selfHealDelayMs(int attempt) =>
    math.min(selfHealMaxMs, selfHealBaseMs * (1 << math.min(attempt, 30)));
