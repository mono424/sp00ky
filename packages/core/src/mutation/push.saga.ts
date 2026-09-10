import type { Saga } from '../kernel/saga';
import type { StatementResult } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { ACK_GRACE_MS, backoffMs } from '../kernel/constants';
import type { ClientState, OutboxItem } from '../state/client-state';
import * as R from '../state/reducers';
import { classifySyncError } from '../utils/error-classification';
import type { SagaEnv } from '../query/env';
import { columnsFor } from '../query/env';
import { mutationOwnerTabId } from './mutation-id';
import {
  buildFailedRow,
  deletePendingRow,
  hydrateRowData,
  loadPendingRows,
  moveToFailedTx,
  parsePendingRow,
  parseStoredRecordId,
  planRevert,
  remoteBatch,
  toOutboxItem,
  FAILED_TABLE,
  type PendingMutationRow,
} from './rows';

/**
 * Boot / bucket switch: mirror the outbox table into state. Rows that can
 * never be replayed (a create without its payload) go straight to the tray.
 */
export function* loadOutbox(env: SagaEnv): Saga<void> {
  const res = (yield fx.local.query(loadPendingRows())) as unknown[];
  const raw = Array.isArray(res?.[0]) ? (res[0] as unknown[]) : [];
  const items: OutboxItem[] = [];
  const unreplayable: Array<Record<string, unknown>> = [];
  for (const row of raw) {
    const parsed = parsePendingRow(row);
    if (parsed) items.push(toOutboxItem(parsed));
    else if (row && typeof row === 'object') unreplayable.push(row as Record<string, unknown>);
  }
  yield fx.state.update(R.outboxReplace(items));
  for (const row of unreplayable) {
    const stub: PendingMutationRow = {
      id: String(row.id ?? ''),
      mutationType: (row.mutationType as PendingMutationRow['mutationType']) ?? 'create',
      recordId: String(row.recordId ?? ''),
      tableName: String(row.tableName ?? String(row.recordId ?? '').split(':')[0]),
      createdAt: 0,
      v: 1,
    };
    yield* rollback(env, stub, { message: 'pending mutation is not replayable', kind: 'unreplayable' });
  }
  yield* refreshFailedCount();
  if (items.length > 0) yield fx.dispatch({ type: 'Drain' });
}

export function* refreshFailedCount(): Saga<void> {
  try {
    const res = (yield fx.local.query(`SELECT count() FROM ${FAILED_TABLE} GROUP ALL`)) as unknown[];
    const row = Array.isArray(res?.[0]) ? (res[0] as Array<{ count?: number }>)[0] : undefined;
    yield fx.state.update(R.setFailedCount(typeof row?.count === 'number' ? row.count : 0));
  } catch {
    // The tray table may not exist yet on a fresh store; the count stays as is.
  }
}

const ERR = (r: StatementResult | undefined): string | null => (r && r.status === 'ERR' ? r.error : null);

/**
 * Drain the outbox: one serial lane, FIFO, up to `outboxBatchSize` statements
 * per request, per-statement outcome. Accepted rows are acked (they stay in
 * the overlay until membership names them), rejected rows are rolled back,
 * a transport failure leaves the unsent tail queued behind a backoff timer.
 */
export function* drain(env: SagaEnv): Saga<void> {
  for (;;) {
    const state = (yield fx.state.read((s) => s)) as ClientState;
    if (state.tabRole === 'follower') return;
    const batch = state.outbox.filter((i) => i.status === 'pending').slice(0, env.outboxBatchSize);
    if (batch.length === 0) return;
    const rowsRes = (yield fx.local.query('SELECT * FROM $ids', { ids: batch.map((i) => parseStoredRecordId(i.id)) })) as unknown[];
    const rawRows = Array.isArray(rowsRes?.[0]) ? (rowsRes[0] as unknown[]) : [];
    const byId = new Map<string, PendingMutationRow>();
    for (const raw of rawRows) {
      const parsed = parsePendingRow(raw);
      if (parsed) byId.set(parsed.id, parsed);
    }
    const rows: PendingMutationRow[] = [];
    for (const item of batch) {
      const row = byId.get(item.id);
      if (row) rows.push(hydrateRowData(row, columnsFor(env, row.tableName)));
      else yield fx.state.update(R.outboxRemove(item.id)); // row already gone from the store
    }
    if (rows.length === 0) continue;

    const req = remoteBatch(rows);
    let results: StatementResult[];
    try {
      const raw = (yield fx.remote.query(req.sql, req.vars, env.pushTimeoutMs)) as unknown;
      // A response without statement results is a transport fault: nothing
      // ran, everything stays queued (handled as `!r` below).
      results = Array.isArray(raw) ? (raw as StatementResult[]) : [];
    } catch (error) {
      if (classifySyncError(error) === 'application') {
        // The whole request was rejected before any statement ran: treat the
        // head as rejected, keep the rest for the next round.
        yield* rollback(env, rows[0], { message: error instanceof Error ? error.message : String(error), kind: 'application' });
        yield fx.dispatch({ type: 'SyncOutcome', ok: false, error });
        continue;
      }
      const attempts = batch.find((i) => i.id === rows[0].id)!.attempts;
      yield fx.state.update(R.outboxBumpAttempts(rows[0].id));
      yield fx.dispatch({ type: 'SyncOutcome', ok: false, error });
      yield fx.timer.set('outbox', backoffMs(attempts), { type: 'Drain' });
      return;
    }

    const now = (yield fx.now()) as number;
    let cutAt: number | null = null;
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const r = results[i];
      if (!r) {
        // Statements after a transport-level cut: they never ran.
        cutAt = i;
        break;
      }
      const err = ERR(r);
      if (err === null) {
        const del = deletePendingRow(row.id);
        try {
          yield fx.local.query(del.sql, del.vars);
        } catch (error) {
          yield fx.emit({ type: 'log', level: 'error', message: 'outbox row delete failed after a successful push', data: { id: row.id, error } });
        }
        yield fx.state.update(R.outboxAck(row.id, now));
        yield fx.emit({ type: 'mutation:settled', mutationId: row.id, recordId: row.recordId, eventType: row.mutationType });
        yield fx.emit({ type: 'tabs:broadcast', message: { type: 'mutation-settled', mutationId: row.id, recordId: row.recordId, eventType: row.mutationType } });
        continue;
      }
      if (classifySyncError(new Error(err)) === 'network') {
        cutAt = i;
        break;
      }
      yield* rollback(env, row, { message: err, kind: 'application' });
    }
    yield fx.timer.set('ack-prune', ACK_GRACE_MS, { type: 'AckPrune' });
    yield fx.dispatch({ type: 'SyncOutcome', ok: cutAt === null, error: cutAt === null ? undefined : 'push interrupted' });
    if (cutAt !== null) {
      const cut = rows[cutAt];
      const attempts = batch.find((i) => i.id === cut.id)!.attempts;
      yield fx.state.update(R.outboxBumpAttempts(cut.id));
      yield fx.timer.set('outbox', backoffMs(attempts), { type: 'Drain' });
      return;
    }
  }
}

/**
 * Undo one rejected mutation. Order matters: the tray row is written and the
 * pending row deleted in ONE transaction before any local revert, so a crash
 * mid-way leaves the mutation in the tray, never silently re-applied.
 */
export function* rollback(
  env: SagaEnv,
  row: PendingMutationRow,
  error: { message: string; kind: 'application' | 'unreplayable' }
): Saga<void> {
  const attempts = ((yield fx.state.read((s) => s.outbox.find((i) => i.id === row.id)?.attempts)) as number | undefined) ?? 0;
  const now = (yield fx.now()) as number;
  let before: Record<string, unknown> | null = row.beforeRecord ?? null;
  if (row.beforeRecord === undefined && row.mutationType !== 'create') {
    // Legacy row: nothing captured locally, ask the server for the row.
    try {
      const res = (yield fx.remote.query('SELECT * FROM ONLY $id', { id: parseStoredRecordId(row.recordId) }, env.remoteTimeoutMs)) as StatementResult[];
      const r = res?.[0];
      before = r && r.status === 'OK' && r.result && typeof r.result === 'object' ? (r.result as Record<string, unknown>) : null;
    } catch {
      before = null;
    }
  }
  const plan = planRevert(row, before);
  const failed = buildFailedRow(row, error, before, attempts, now, plan.revert);
  const move = moveToFailedTx(failed);
  try {
    yield fx.local.execute(move.query, move.vars);
  } catch (err) {
    yield fx.emit({ type: 'log', level: 'error', message: 'failed to move a rejected mutation to the tray', data: { id: row.id, err } });
  }
  yield fx.state.update(R.compose(R.outboxRemove(row.id), R.setFailedCount((yield fx.state.read((s) => s.failedCount)) as number + 1)));
  if (plan.tx) {
    try {
      yield fx.local.execute(plan.tx.query, plan.tx.vars);
    } catch (err) {
      yield fx.emit({ type: 'log', level: 'error', message: 'local revert failed', data: { id: row.id, err } });
    }
  }
  try {
    yield fx.ssp.ingest([plan.circuit]);
  } catch (err) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'circuit revert failed', data: { id: row.id, err } });
  }
  const roleNow = (yield fx.state.read((s) => s.tabRole)) as ClientState['tabRole'];
  if (roleNow !== 'solo') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'ingest', records: [plan.circuit] } });
  if (plan.circuit.op === 'DELETE') yield fx.state.update(R.deleteVersions([row.recordId]));
  else if (before && typeof before._00_rv === 'number') yield fx.state.update(R.setVersions([[row.recordId, before._00_rv]]));
  yield fx.state.update(R.markTableDirty(row.tableName));
  const rolledBack = { type: 'mutation-rolled-back', mutationId: row.id, recordId: row.recordId, eventType: row.mutationType, error: error.message };
  yield fx.emit({ type: 'mutation:rolled-back', mutationId: row.id, recordId: row.recordId, eventType: row.mutationType, error: error.message });
  const [tabId, role] = (yield fx.state.read((s) => [s.tabId, s.tabRole])) as [string, ClientState['tabRole']];
  const owner = mutationOwnerTabId(row.id);
  if (role === 'leader' && owner && owner !== tabId) yield fx.emit({ type: 'tabs:sendTo', tabId: owner, message: rolledBack });
  const count = (yield fx.state.read((s) => s.failedCount)) as number;
  yield fx.emit({ type: 'tray:changed', count });
  if (role === 'leader') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'failed-mutations-changed', count } });
}
