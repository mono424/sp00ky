import '../kernel/constants.dart';
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../query/env.dart';
import '../state/client_state.dart';
import '../state/reducers.dart' as r;
import '../types.dart';
import '../utils/error_classification.dart';
import '../utils/record_id_utils.dart';
import 'mutation_id.dart' show pendingTable;
import 'rows.dart';

/// Boot or bucket switch: mirror the outbox table into state. Rows that can
/// never be replayed (a create without its payload) go straight to the tray.
Future<void> loadOutbox(Ctx ctx, SagaEnv env) async {
  final raw = await ctx(Fx.localGetAll(pendingTable));
  final replayable = <PendingMutationRow>[];
  final unreplayable = <Map<String, dynamic>>[];
  for (final row in raw) {
    final parsed = parsePendingRow(row);
    if (parsed != null) {
      replayable.add(parsed);
    } else {
      unreplayable.add(row);
    }
  }
  // Drain order is id order, which is mint order.
  final items = [for (final row in sortPendingRows(replayable)) toOutboxItem(row)];
  await ctx(Fx.stateUpdate(r.outboxReplace(items)));
  for (final row in unreplayable) {
    final recordId = (row['recordId'] ?? '').toString();
    await rollback(
      ctx,
      env,
      PendingMutationRow(
        id: (row['id'] ?? '').toString(),
        mutationType: MutationEventType.create,
        recordId: recordId,
        tableName: (row['tableName'] ?? extractTablePart(recordId)).toString(),
        createdAt: 0,
        v: 1,
      ),
      const FailedMutationError(
        message: 'pending mutation is not replayable',
        kind: FailedErrorKind.unreplayable,
      ),
    );
  }
  await refreshFailedCount(ctx);
  if (items.isNotEmpty) await ctx(Fx.dispatch(const Drain()));
}

Future<void> refreshFailedCount(Ctx ctx) async {
  try {
    final rows = await ctx(Fx.localGetAll(failedTable));
    await ctx(Fx.stateUpdate(r.setFailedCount(rows.length)));
  } catch (_) {
    // The tray table may not exist yet on a fresh store; the count stays as is.
  }
}

/// Drain the outbox: one serial lane, FIFO, up to [SagaEnv.outboxBatchSize]
/// statements per request, per-statement outcome. Accepted rows are acked (they
/// stay in the overlay until membership names them), rejected rows are rolled
/// back, a transport failure leaves the unsent tail queued behind a backoff
/// timer.
Future<void> drain(Ctx ctx, SagaEnv env) async {
  // A client with no endpoint has nothing to push to. Draining anyway would
  // fail every statement in a way the classifier reads as a rejection, and a
  // rejection rolls the write back: the local-first write would delete itself.
  if (!env.hasRemote) return;
  for (;;) {
    final state = await ctx(Fx.stateRead((s) => s));
    final pending =
        state.outbox.where((i) => i.status == OutboxStatus.pending).toList();
    final batch = pending.take(env.outboxBatchSize).toList();
    if (batch.isEmpty) return;

    final rows = <PendingMutationRow>[];
    for (final item in batch) {
      final raw = await ctx(Fx.localGet(pendingTable, item.id));
      final parsed = raw == null ? null : parsePendingRow(raw);
      if (parsed == null) {
        // The row is already gone from the store.
        await ctx(Fx.stateUpdate(r.outboxRemove(item.id)));
        continue;
      }
      rows.add(hydrateRowData(parsed, columnsFor(env, parsed.tableName)));
    }
    if (rows.isEmpty) continue;

    final req = remoteBatch(rows);
    List<StatementResult> results;
    try {
      results = await ctx(Fx.remoteQuery(req.sql,
          vars: req.vars, timeoutMs: env.pushTimeoutMs));
    } catch (error) {
      if (classifySyncError(error) == 'application') {
        // The whole request was rejected before any statement ran: treat the
        // head as rejected and keep the rest for the next round.
        await rollback(
          ctx,
          env,
          rows.first,
          FailedMutationError(
              message: error.toString(), kind: FailedErrorKind.application),
        );
        await ctx(Fx.dispatch(SyncOutcome(false, error)));
        continue;
      }
      final attempts =
          batch.firstWhere((i) => i.id == rows.first.id).attempts;
      await ctx(Fx.stateUpdate(r.outboxBumpAttempts(rows.first.id)));
      await ctx(Fx.dispatch(SyncOutcome(false, error)));
      await ctx(Fx.timerSet('outbox', backoffMs(attempts), const Drain()));
      return;
    }

    final now = await ctx(Fx.now());
    int? cutAt;
    for (var i = 0; i < rows.length; i++) {
      final row = rows[i];
      final res = i < results.length ? results[i] : null;
      if (res == null) {
        // Statements after a transport-level cut: they never ran.
        cutAt = i;
        break;
      }
      if (res.isOk) {
        try {
          await ctx(Fx.localTx([deletePendingRow(row.id)]));
        } catch (error) {
          await ctx(Fx.log(
              LogLevel.error,
              'outbox row delete failed after a successful push',
              {'id': row.id, 'error': error}));
        }
        await ctx(Fx.stateUpdate(r.outboxAck(row.id, now)));
        await ctx(Fx.emit(MutationSettledEvent(
          mutationId: row.id,
          recordId: row.recordId,
          eventType: row.mutationType,
        )));
        continue;
      }
      if (classifySyncError(res.error) == 'network') {
        cutAt = i;
        break;
      }
      await rollback(
        ctx,
        env,
        row,
        FailedMutationError(
            message: res.error ?? 'unknown', kind: FailedErrorKind.application),
      );
    }
    await ctx(Fx.timerSet('ack-prune', ackGraceMs, const AckPrune()));
    await ctx(Fx.dispatch(
        SyncOutcome(cutAt == null, cutAt == null ? null : 'push interrupted')));
    if (cutAt != null) {
      final cut = rows[cutAt];
      final attempts = batch.firstWhere((i) => i.id == cut.id).attempts;
      await ctx(Fx.stateUpdate(r.outboxBumpAttempts(cut.id)));
      await ctx(Fx.timerSet('outbox', backoffMs(attempts), const Drain()));
      return;
    }
  }
}

/// Undo one rejected mutation. Order matters: the tray row is written and the
/// pending row deleted in ONE transaction before any local revert, so a crash
/// mid-way leaves the mutation in the tray, never silently re-applied.
Future<void> rollback(
  Ctx ctx,
  SagaEnv env,
  PendingMutationRow row,
  FailedMutationError error,
) async {
  final attempts = await ctx(Fx.stateRead((s) {
    for (final i in s.outbox) {
      if (i.id == row.id) return i.attempts;
    }
    return 0;
  }));
  final now = await ctx(Fx.now());
  var before = row.beforeRecord;
  if (before == null && row.mutationType != MutationEventType.create) {
    // Legacy row: nothing captured locally, so ask the server for it.
    try {
      final res = await ctx(Fx.remoteQuery('SELECT * FROM ONLY \$id',
          vars: {'id': parseRecordIdString(row.recordId)},
          timeoutMs: env.remoteTimeoutMs));
      final first = res.isEmpty ? null : res.first;
      before = first != null && first.isOk && first.result is Map
          ? Map<String, dynamic>.from(first.result as Map)
          : null;
    } catch (_) {
      before = null;
    }
  }
  final plan = planRevert(row, before);
  final failed =
      buildFailedRow(row, error, before, attempts, now, plan.revert);
  try {
    await ctx(Fx.localTx(moveToFailedTx(failed)));
  } catch (e) {
    await ctx(Fx.log(LogLevel.error,
        'failed to move a rejected mutation to the tray', {'id': row.id, 'error': e}));
  }
  final failedCount = await ctx(Fx.stateRead((s) => s.failedCount));
  await ctx(Fx.stateUpdate(r.compose([
    r.outboxRemove(row.id),
    r.setFailedCount(failedCount + 1),
  ])));
  if (plan.ops.isNotEmpty) {
    try {
      await ctx(Fx.localTx(plan.ops));
    } catch (e) {
      await ctx(Fx.log(
          LogLevel.error, 'local revert failed', {'id': row.id, 'error': e}));
    }
  }
  try {
    await ctx(Fx.sspIngest([plan.circuit]));
  } catch (e) {
    await ctx(Fx.log(
        LogLevel.warn, 'circuit revert failed', {'id': row.id, 'error': e}));
  }
  if (plan.circuit.op.name == 'delete') {
    await ctx(Fx.stateUpdate(r.deleteVersions([row.recordId])));
  } else if (before?['_00_rv'] is num) {
    await ctx(Fx.stateUpdate(r.setVersions(
        [(row.recordId, (before!['_00_rv'] as num).toInt())])));
  }
  await ctx(Fx.stateUpdate(r.markTableDirty(row.tableName)));
  await ctx(Fx.emit(MutationRolledBackEvent(
    mutationId: row.id,
    recordId: row.recordId,
    eventType: row.mutationType,
    error: error.message,
  )));
  final count = await ctx(Fx.stateRead((s) => s.failedCount));
  await ctx(Fx.emit(TrayChangedEvent(count)));
}
