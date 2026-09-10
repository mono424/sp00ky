import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import * as R from '../state/reducers';
import type { SagaEnv } from '../query/env';
import { deleteFailedRow, failedRecordId, loadFailedRows, parseFailedRow, type FailedMutationRow } from './rows';
import { write } from './write.saga';

/** Every rejected mutation still in the tray, oldest first. */
export function* listFailed(): Saga<FailedMutationRow[]> {
  const res = (yield fx.local.query(loadFailedRows())) as unknown[];
  const raw = Array.isArray(res?.[0]) ? (res[0] as unknown[]) : [];
  return raw.map(parseFailedRow).filter((r): r is FailedMutationRow => r !== null);
}

function* loadFailed(mutationId: string): Saga<FailedMutationRow | null> {
  const res = (yield fx.local.query('SELECT * FROM $ids', { ids: [failedRecordId(mutationId)] })) as unknown[];
  const raw = Array.isArray(res?.[0]) ? (res[0] as unknown[])[0] : undefined;
  return raw ? parseFailedRow(raw) : null;
}

function* dropFailed(mutationId: string): Saga<void> {
  const del = deleteFailedRow(mutationId);
  yield fx.local.query(del.sql, del.vars);
  const count = Math.max(0, ((yield fx.state.read((s) => s.failedCount)) as number) - 1);
  yield fx.state.update(R.setFailedCount(count));
  yield fx.emit({ type: 'tray:changed', count });
  yield fx.emit({ type: 'tabs:broadcast', message: { type: 'failed-mutations-changed', count } });
}

/**
 * Re-apply a rejected mutation as a NEW optimistic write (fresh mutation id,
 * the optimistic local change comes back, the outbox drains), then drop the
 * tray row.
 */
export function* retryFailed(env: SagaEnv, mutationId: string): Saga<boolean> {
  const failed = yield* loadFailed(mutationId);
  if (!failed) return false;
  yield* write(env, { kind: failed.mutationType, recordId: failed.recordId, data: failed.data });
  yield* dropFailed(mutationId);
  return true;
}

export function* discardFailed(mutationId: string): Saga<boolean> {
  const failed = yield* loadFailed(mutationId);
  if (!failed) return false;
  yield* dropFailed(mutationId);
  return true;
}
