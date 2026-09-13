import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../query/env.dart';
import '../state/reducers.dart' as r;
import 'rows.dart';
import 'write_saga.dart';

/// Every rejected mutation still in the tray, oldest first.
Future<List<FailedMutationRow>> listFailed(Ctx ctx) async {
  final raw = await ctx(Fx.localGetAll(failedTable));
  return sortFailedRows(
      [for (final row in raw) parseFailedRow(row)].whereType<FailedMutationRow>().toList());
}

Future<FailedMutationRow?> _loadFailed(Ctx ctx, String mutationId) async {
  final raw = await ctx(Fx.localGet(failedTable, failedRecordId(mutationId)));
  return raw == null ? null : parseFailedRow(raw);
}

Future<void> _dropFailed(Ctx ctx, String mutationId) async {
  await ctx(Fx.localTx([deleteFailedRow(mutationId)]));
  final current = await ctx(Fx.stateRead((s) => s.failedCount));
  final count = current - 1 < 0 ? 0 : current - 1;
  await ctx(Fx.stateUpdate(r.setFailedCount(count)));
  await ctx(Fx.emit(TrayChangedEvent(count)));
}

/// Re-apply a rejected mutation as a NEW optimistic write (fresh mutation id,
/// the optimistic local change comes back, the outbox drains), then drop the
/// tray row.
Future<bool> retryFailed(Ctx ctx, SagaEnv env, String mutationId) async {
  final failed = await _loadFailed(ctx, mutationId);
  if (failed == null) return false;
  await write(
      ctx,
      env,
      WriteInput(
        kind: failed.mutationType,
        recordId: failed.recordId,
        data: failed.data,
      ));
  await _dropFailed(ctx, mutationId);
  return true;
}

Future<bool> discardFailed(Ctx ctx, String mutationId) async {
  final failed = await _loadFailed(ctx, mutationId);
  if (failed == null) return false;
  await _dropFailed(ctx, mutationId);
  return true;
}
