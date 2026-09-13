import 'dart:math' as math;

import '../kernel/constants.dart';
import '../types.dart';
import 'client_state.dart';
import 'lifecycle.dart';

/// Pure reads over [ClientState]. Sagas use them through `Fx.stateRead`.

QueryEntry? queryByHash(ClientState s, QueryHash hash) => s.queries[hash];

List<QueryHash> activeHashes(ClientState s) => s.queries.keys.toList();

List<QueryHash> hashesForTable(ClientState s, String table) => [
      for (final e in s.queries.entries)
        if (e.value.def.tableName == table) e.key,
    ];

QueryStatus? queryStatus(ClientState s, QueryHash hash) {
  final e = s.queries[hash];
  return e == null ? null : deriveStatus(e.lifecycle);
}

class Overlay {
  const Overlay({required this.writes, required this.deletes});

  /// Ids with an unsynced or just-acked create/update.
  final Set<String> writes;

  /// Ids with an unsynced or just-acked delete.
  final Set<String> deletes;
}

/// The optimistic overlay, derived from the outbox (pending AND acked items).
Overlay overlay(ClientState s) {
  final writes = <String>{};
  final deletes = <String>{};
  for (final item in s.outbox) {
    if (item.type == MutationEventType.delete) {
      deletes.add(item.recordId);
    } else {
      writes.add(item.recordId);
    }
  }
  return Overlay(writes: writes, deletes: deletes);
}

Set<String> pendingDeleteIds(ClientState s) => {
      for (final i in s.outbox)
        if (i.type == MutationEventType.delete) i.recordId,
    };

bool hasAckedWrites(ClientState s) =>
    s.outbox.any((i) => i.status == OutboxStatus.acked);

int pendingMutationCount(ClientState s) =>
    s.outbox.where((i) => i.status == OutboxStatus.pending).length;

/// Record ids with a local write the server has not acknowledged yet: a queued
/// outbox item, or a debounced patch not flushed to the outbox. An acked item
/// is synced, so it does not count.
Set<String> unsyncedRecordIds(ClientState s) => {
      for (final i in s.outbox)
        if (i.status == OutboxStatus.pending) i.recordId,
      for (final w in s.pendingWrites.values) w.recordId,
    };

int fetchingQueryCount(ClientState s) =>
    s.queries.values.where((e) => e.lifecycle.fetchDepth > 0).length;

/// Membership entries whose body is missing or older locally.
RecordVersionArray needed(ClientState s, QueryHash hash) {
  final e = s.queries[hash];
  if (e == null || !hasServerMembership(e.lifecycle)) return const [];
  final deletes = pendingDeleteIds(s);
  return e.remoteArray
      .where((rv) =>
          !deletes.contains(rv.$1) && (s.versions[rv.$1] ?? -1) < rv.$2)
      .toList();
}

/// Subquery child bodies missing or stale locally (never part of [settled]).
RecordVersionArray neededChildren(ClientState s, QueryHash hash) {
  final e = s.queries[hash];
  if (e == null || e.subqueryRemoteArray.isEmpty) return const [];
  return e.subqueryRemoteArray
      .where((rv) => (s.versions[rv.$1] ?? -1) < rv.$2)
      .toList();
}

class FetchPlan {
  const FetchPlan({
    required this.hashes,
    required this.chunks,
    required this.versions,
  });

  /// Queries whose primary membership needs bodies (they flip to `fetching`).
  final List<QueryHash> hashes;
  final List<List<String>> chunks;

  /// Highest requested version per id, across every query naming it.
  final Map<String, int> versions;
}

/// Cross-query, deduped, chunked list of ids to pull from the server.
FetchPlan planFetch(ClientState s, {int chunkSize = fetchChunk}) {
  final versions = <String, int>{};
  final hashes = <QueryHash>[];
  void add(RecordVersionArray pairs) {
    for (final (id, v) in pairs) {
      versions[id] = math.max(v, versions[id] ?? -1);
    }
  }

  for (final hash in s.queries.keys) {
    final missing = needed(s, hash);
    if (missing.isNotEmpty) hashes.add(hash);
    add(missing);
    add(neededChildren(s, hash));
  }
  final all = versions.keys.toList();
  final chunks = <List<String>>[];
  for (var i = 0; i < all.length; i += chunkSize) {
    chunks.add(all.sublist(i, math.min(i + chunkSize, all.length)));
  }
  return FetchPlan(hashes: hashes, chunks: chunks, versions: versions);
}

/// "This query's rows are authoritative and complete": server membership
/// accepted, every body present at its version, nothing waiting to be
/// re-rendered, subscribers told at least once.
bool settled(ClientState s, QueryHash hash) {
  final e = s.queries[hash];
  if (e == null || e.lifecycle.phase != QueryPhase.live) return false;
  return needed(s, hash).isEmpty &&
      !s.dirty.contains(hash) &&
      e.lifecycle.notified;
}

bool settleFailed(ClientState s, QueryHash hash) {
  final e = s.queries[hash];
  return e == null || e.lifecycle.remote == RemotePhase.failed;
}

List<QueryHash> desiredRegistrations(ClientState s) => [
      for (final e in s.queries.entries)
        if (e.value.lifecycle.remote == RemotePhase.unregistered) e.key,
    ];

List<QueryHash> evictable(ClientState s, int now) => [
      for (final e in s.queries.entries)
        if (e.value.subscribers == 0 &&
            e.value.lastSubscriberLeftAt != null &&
            now - e.value.lastSubscriberLeftAt! >= e.value.def.ttlMs)
          e.key,
    ];

int? shortestTtlMs(ClientState s) {
  int? min;
  for (final e in s.queries.values) {
    min = min == null ? e.def.ttlMs : math.min(min, e.def.ttlMs);
  }
  return min;
}

PhaseStat phaseStatOf(List<double> samples, double? lastMs) {
  if (samples.isEmpty) {
    return PhaseStat(lastMs: lastMs, count: 0);
  }
  final sorted = [...samples]..sort();
  double pick(double q) =>
      sorted[math.min(sorted.length - 1, (q * sorted.length).floor())];
  return PhaseStat(
    lastMs: lastMs,
    p50: pick(0.5),
    p90: pick(0.9),
    p99: pick(0.99),
    count: samples.length,
  );
}

/// Per-query processing-time breakdown.
QueryTimings phaseTimings(QueryEntry e) {
  final t = e.telemetry;
  PhaseStat stat(String phase) =>
      phaseStatOf(t.phaseSamples[phase] ?? const [], t.phaseLast[phase]);
  return QueryTimings(
    ssp: phaseStatOf(t.materializationSamples, t.lastIngestLatencyMs),
    sspStoreApply: stat(TimingPhase.sspStoreApply),
    sspCircuitStep: stat(TimingPhase.sspCircuitStep),
    sspTransform: stat(TimingPhase.sspTransform),
    localFetch: stat(TimingPhase.localFetch),
    remoteFetch: stat(TimingPhase.remoteFetch),
    frontend: stat(TimingPhase.frontend),
    registration: t.registrationTimings,
    updateCount: t.updateCount,
    errorCount: t.errorCount,
  );
}
