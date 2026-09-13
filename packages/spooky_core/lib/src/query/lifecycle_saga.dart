import '../kernel/constants.dart';
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../services/stream_processor/stream_processor_service.dart'
    show IngestOp, IngestRecord;
import '../state/client_state.dart';
import '../state/lifecycle.dart';
import '../state/reducers.dart' as r;
import '../state/selectors.dart' show evictable, shortestTtlMs;
import '../utils/record_id_utils.dart';
import 'env.dart';
import 'membership.dart' show decodeIdsOfView;
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

/// Weekly orphan collection: bodies the store holds that no `_00_view` row
/// names and no outbox item touches are invisible; delete them in chunks.
Future<void> gcTick(Ctx ctx) async {
  try {
    final views = await ctx(Fx.localGetAll(sql.viewTable));
    final keep = <String>{};
    for (final row in views) {
      keep.addAll(decodeIdsOfView(row));
    }
    final state = await ctx(Fx.stateRead((s) => s));
    for (final item in state.outbox) {
      keep.add(item.recordId);
    }
    final orphans = [
      for (final id in state.versions.keys)
        if (!keep.contains(id) && !id.startsWith('_00_')) id
    ];
    for (var i = 0; i < orphans.length; i += 200) {
      final slice = orphans.sublist(
          i, i + 200 > orphans.length ? orphans.length : i + 200);
      final settled = await ctx(Fx.all([
        for (final id in slice) Fx.localDelete(extractTablePart(id), id)
      ]));
      final done = [
        for (var j = 0; j < slice.length; j++)
          if (settled[j].ok) slice[j]
      ];
      await ctx(Fx.stateUpdate(r.deleteVersions(done)));
      if (done.isNotEmpty) {
        await ctx(Fx.sspIngest([
          for (final id in done)
            IngestRecord(
              table: extractTablePart(id),
              op: IngestOp.delete,
              id: id,
              record: const {},
            )
        ]));
      }
    }
    await ctx(Fx.log(
        LogLevel.info, 'orphan gc done', {'removed': orphans.length}));
  } catch (error) {
    await ctx(Fx.log(LogLevel.warn, 'orphan gc failed', {'error': error}));
  }
  await ctx(Fx.timerSet('gc', gcIntervalMs, const GcTick()));
}
