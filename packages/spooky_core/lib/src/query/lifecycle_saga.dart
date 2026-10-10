import 'dart:math' as math;

import '../kernel/constants.dart';
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../services/stream_processor/stream_processor_service.dart'
    show IngestOp, IngestRecord;
import '../state/client_state.dart';
import '../state/lifecycle.dart';
import '../state/reducers.dart' as r;
import '../state/selectors.dart' show evictable, retained, shortestTtlMs;
import '../utils/record_id_utils.dart';
import 'env.dart';
import 'membership.dart' show parseViewIndexRow;
import 'sql.dart' as sql;

/// One tick for every query in state: evict the ones nobody has watched for a
/// ttl, heartbeat the rest in one request, notice reclaimed rows. Reschedules
/// itself at half the shortest ttl.
Future<void> lifecycleTick(Ctx ctx, SagaEnv env) async {
  final now = await ctx(Fx.now());
  final state = await ctx(Fx.stateRead((s) => s));
  for (final hash in evictable(state, now)) {
    await evictQuery(ctx, hash);
  }
  final remaining = await ctx(Fx.stateRead((s) => [
        for (final e in s.queries.values)
          if (e.lifecycle.remote == RemotePhase.registered) e
      ]));
  if (remaining.isNotEmpty) {
    final beat = sql.heartbeatBatch([for (final e in remaining) e.def.id]);
    try {
      final results = await ctx(Fx.remoteQuery(beat.sql,
          vars: beat.vars, timeoutMs: env.remoteTimeoutMs));
      var reclaimed = 0;
      for (var i = 0; i < remaining.length; i++) {
        final res = sql.stmt(results, i);
        if (res != null && res.isOk && sql.heartbeatRowGone(res.result)) {
          reclaimed++;
          await ctx(Fx.stateUpdate(r.applyLifecycle(
              remaining[i].def.hash, const RemoteDroppedEvent())));
        }
      }
      await ctx(Fx.stateUpdate(
          r.stampHeartbeat([for (final e in remaining) e.def.hash], now)));
      if (reclaimed > 0) {
        await ctx(Fx.log(
            LogLevel.warn,
            'query rows reclaimed by the server; re-registering',
            {'reclaimed': reclaimed}));
        await ctx(Fx.dispatch(const EnsureRegistered()));
      }
      await ctx(Fx.dispatch(const SyncOutcome(true)));
    } catch (error) {
      await ctx(Fx.dispatch(SyncOutcome(false, error)));
    }
  }
  final ttl = await ctx(Fx.stateRead(shortestTtlMs));
  await ctx(Fx.timerSet(
      'lifecycle',
      ((ttl ?? env.defaultTtlMs) * ttlHeartbeatFraction).floor(),
      const LifecycleTick()));
}

/// Free a query's local view and forget it. The server row expires by TTL.
Future<void> evictQuery(Ctx ctx, String hash) async {
  final exists = await ctx(Fx.stateRead((s) => s.queries.containsKey(hash)));
  if (!exists) return;
  try {
    await ctx(Fx.sspUnregister(hash));
  } catch (error) {
    await ctx(Fx.log(LogLevel.debug, 'unregister failed',
        {'hash': hash, 'error': error}));
  }
  await ctx(Fx.stateUpdate(r.removeQuery(hash)));
  await ctx(Fx.emit(QueryEvictedEvent(hash)));
}

/// Drop acked outbox items membership never named within the grace window.
Future<void> ackPrune(Ctx ctx) async {
  final now = await ctx(Fx.now());
  await ctx(Fx.stateUpdate(r.outboxPruneAcked(now, ackGraceMs)));
  final stillAcked = await ctx(Fx.stateRead(
      (s) => s.outbox.any((i) => i.status == OutboxStatus.acked)));
  if (stillAcked) {
    await ctx(Fx.timerSet('ack-prune', ackGraceMs, const AckPrune()));
  }
}

/// Orphan collection, [gcBootDelayMs] after boot and then every
/// [gcIntervalMs]. Retires the `_00_view` rows no query in state holds and no
/// server answer has rewritten for [viewRetentionMs], then deletes the bodies
/// nothing retains (see [retained]) from the store and the circuit, a chunk at
/// a time. Each chunk is re-checked against fresh state (a membership committed
/// meanwhile may name an id again) and the sweep stops if the bucket moved.
Future<void> gcTick(Ctx ctx) async {
  await ctx(Fx.stateWait((s) => s.primed));
  try {
    // The bucket the view rows are read from: a sweep that finds another one
    // in place stops instead of judging its bodies by these rows.
    final bucket = await ctx(Fx.stateRead((s) => s.bucketId));
    final rows = [
      for (final row in await ctx(Fx.localGetAll(sql.viewTable)))
        parseViewIndexRow(row)
    ];
    final held = await ctx(Fx.stateRead((s) => s.bucketId != bucket
        ? null
        : {for (final e in s.queries.values) e.def.viewKey}));
    if (held == null) return;
    final now = await ctx(Fx.now());
    final expired = [
      for (final row in rows)
        if (row.key != null &&
            !held.contains(row.key) &&
            now - row.updatedAt >= viewRetentionMs)
          row.key!
    ];
    final retired = <String>{};
    for (var i = 0; i < expired.length; i += gcChunk) {
      final chunk = expired.sublist(i, math.min(i + gcChunk, expired.length));
      final settled = await ctx(Fx.all([
        for (final key in chunk)
          Fx.localDelete(sql.viewTable, sql.viewRecordId(key))
      ]));
      for (var j = 0; j < chunk.length; j++) {
        if (settled[j].ok) retired.add(chunk[j]);
      }
    }
    final viewIds = <String>{
      for (final row in rows)
        if (!retired.contains(row.key)) ...row.ids
    };
    final removed = await _collectOrphans(ctx, bucket, viewIds);
    await ctx(Fx.log(LogLevel.info, 'orphan gc done',
        {'removed': removed, 'retiredViews': retired.length}));
  } catch (error) {
    await ctx(Fx.log(LogLevel.warn, 'orphan gc failed', {'error': error}));
  } finally {
    await ctx(Fx.timerSet('gc', gcIntervalMs, const GcTick()));
  }
}

Future<int> _collectOrphans(
    Ctx ctx, String? bucket, Set<String> viewIds) async {
  final candidates = await ctx(Fx.stateRead((s) {
    final keep = retained(s, viewIds);
    return [
      for (final id in s.versions.keys)
        if (!keep(id)) id
    ];
  }));
  var removed = 0;
  for (var i = 0; i < candidates.length; i += gcChunk) {
    final slice =
        candidates.sublist(i, math.min(i + gcChunk, candidates.length));
    final chunk = await ctx(Fx.stateRead((s) {
      if (!s.primed || s.bucketId != bucket) return null;
      final keep = retained(s, viewIds);
      return [
        for (final id in slice)
          if (s.versions.containsKey(id) && !keep(id)) id
      ];
    }));
    if (chunk == null) break;
    final settled = await ctx(Fx.all(
        [for (final id in chunk) Fx.localDelete(extractTablePart(id), id)]));
    final done = [
      for (var j = 0; j < chunk.length; j++)
        if (settled[j].ok) chunk[j]
    ];
    if (done.isEmpty) continue;
    await ctx(Fx.stateUpdate(r.deleteVersions(done)));
    await ctx(Fx.sspIngest([
      for (final id in done)
        IngestRecord(
          table: extractTablePart(id),
          op: IngestOp.delete,
          id: id,
          record: const {},
        )
    ]));
    removed += done.length;
  }
  // A query may have committed a deleted id between its chunk's check and the
  // delete; with its version gone, the fetch plan pulls it back.
  if (removed > 0) await ctx(Fx.dispatch(const FetchRows()));
  return removed;
}
