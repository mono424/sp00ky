import { RecordId } from 'surrealdb';
import type { MutationEventType, UpdateOptions } from '../types';
import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import type { ClientState, PendingWrite } from '../state/client-state';
import * as R from '../state/reducers';
import { parseParams } from '../utils/parser';
import { extractTablePart, parseRecordIdString } from '../utils/index';
import type { SagaEnv } from '../query/env';
import { columnsFor } from '../query/env';
import { planCreateTx, planDeferredOutboxRowTx, planDeleteTx, planLocalOnlyUpdateTx, planUpdateTx, type LocalTx } from './rows';

export interface WriteInput {
  kind: MutationEventType;
  recordId: string;
  data?: Record<string, unknown>;
  options?: UpdateOptions;
}

export interface WriteResult {
  mutationId: string;
  record: Record<string, unknown> | null;
}

const debounceKey = (input: WriteInput): string | null => {
  const d = input.options?.debounced;
  if (!d) return null;
  const opts = d === true ? {} : d;
  const keyMode = opts.key ?? 'recordId_x_fields';
  return keyMode === 'recordId'
    ? input.recordId
    : `${input.recordId}::${Object.keys(input.data ?? {}).sort().join(',')}`;
};
const debounceDelay = (input: WriteInput): number => {
  const d = input.options?.debounced;
  return d && d !== true && typeof d.delay === 'number' ? d.delay : 300;
};

/**
 * One optimistic write: read the previous row (update/delete), run the local
 * transaction that writes the row AND its outbox entry, publish the outbox
 * item (which puts the id into the overlay and dirties the table's queries),
 * feed the circuit, and start a drain. A debounced update applies locally at
 * once but accumulates into one outbox row written on flush.
 */
export function* write(env: SagaEnv, input: WriteInput): Saga<WriteResult> {
  const table = extractTablePart(input.recordId);
  const columns = columnsFor(env, table);
  if (!columns) throw new Error(`Table ${table} not found`);
  const rid = parseRecordIdString(input.recordId);
  const params = input.data ? parseParams(columns as never, input.data) : undefined;
  const key = input.kind === 'update' ? debounceKey(input) : null;
  if (key !== null) return yield* debouncedUpdate(input, key, rid, table, params ?? {});
  const mutationId = (yield fx.id('mutation')) as string;
  const now = (yield fx.now()) as number;
  let before: Record<string, unknown> | null = null;
  if (input.kind !== 'create') before = yield* readBefore(rid);
  const tx = planTx(input.kind, { recordId: rid, mutationId: parseRecordIdString(mutationId), table, data: params, before, now });
  return yield* commitWrite(input.kind, tx, { mutationId, recordId: input.recordId, table, before, now });
}

function planTx(kind: MutationEventType, planInput: Parameters<typeof planCreateTx>[0]): LocalTx {
  if (kind === 'create') return planCreateTx(planInput);
  if (kind === 'update') return planUpdateTx(planInput);
  return planDeleteTx(planInput);
}

function* readBefore(rid: RecordId<string>): Saga<Record<string, unknown> | null> {
  const res = (yield fx.local.query('SELECT * FROM ONLY $id', { id: rid })) as unknown[];
  const row = Array.isArray(res) ? res[0] : null;
  return row && typeof row === 'object' && !Array.isArray(row) ? (row as Record<string, unknown>) : null;
}

interface CommitCtx {
  mutationId: string;
  recordId: string;
  table: string;
  before: Record<string, unknown> | null;
  now: number;
}

function* commitWrite(kind: MutationEventType, tx: LocalTx, ctx: CommitCtx): Saga<WriteResult> {
  const result = (yield fx.local.execute(tx.query, tx.vars)) as unknown;
  const target = kind === 'update' ? (result as { target?: unknown } | undefined)?.target : result;
  const record = kind === 'delete' || !target || typeof target !== 'object' ? null : (target as Record<string, unknown>);
  yield fx.state.update(
    R.outboxPush({ id: ctx.mutationId, type: kind, recordId: ctx.recordId, table: ctx.table, status: 'pending', ackedAt: null, attempts: 0 })
  );
  const version = kind === 'create' ? 1 : typeof record?._00_rv === 'number' ? (record._00_rv as number) : ((ctx.before?._00_rv as number) ?? 0) + 1;
  const circuitRecord = kind === 'delete' ? (ctx.before ?? {}) : { ...record, _00_rv: version };
  const circuitTuple = { table: ctx.table, op: kind.toUpperCase() as 'CREATE' | 'UPDATE' | 'DELETE', id: ctx.recordId, record: circuitRecord };
  try {
    yield fx.ssp.ingest([circuitTuple]);
    if (kind === 'delete') yield fx.state.update(R.deleteVersions([ctx.recordId]));
    else yield fx.state.update(R.setVersions([[ctx.recordId, version]]));
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'error', message: 'circuit ingest failed after a local write', data: { recordId: ctx.recordId, error } });
  }
  yield fx.emit({
    type: 'mutation:event',
    event: { type: kind, mutation_id: parseRecordIdString(ctx.mutationId), record_id: parseRecordIdString(ctx.recordId), data: tx.vars.data, record, beforeRecord: ctx.before ?? undefined, tableName: ctx.table },
  });
  const role = (yield fx.state.read((s) => s.tabRole)) as ClientState['tabRole'];
  if (role !== 'solo') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'ingest', records: [circuitTuple] } });
  if (role === 'follower') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'outbox-changed', mutationId: ctx.mutationId } });
  else yield fx.dispatch({ type: 'Drain' });
  return { mutationId: ctx.mutationId, record };
}

/** Apply locally now, remember the merged patch, write the outbox row on flush. */
function* debouncedUpdate(
  input: WriteInput,
  key: string,
  rid: RecordId<string>,
  table: string,
  params: Record<string, unknown>
): Saga<WriteResult> {
  const existing = (yield fx.state.read((s) => s.pendingWrites.get(key))) as PendingWrite | undefined;
  const now = (yield fx.now()) as number;
  const before = existing ? existing.before : yield* readBefore(rid);
  const local = planLocalOnlyUpdateTx({ recordId: rid, data: params });
  const res = (yield fx.local.execute(local.query, local.vars)) as { target?: Record<string, unknown> } | undefined;
  const record = res?.target ?? null;
  yield fx.state.update(R.mergePendingWrite({ key, table, recordId: input.recordId, data: params, before, firstAt: existing?.firstAt ?? now }));
  yield fx.state.update(R.markTableDirty(table));
  const version = typeof record?._00_rv === 'number' ? (record._00_rv as number) : 0;
  const tuple = { table, op: 'UPDATE' as const, id: input.recordId, record: { ...record, _00_rv: version } };
  try {
    yield fx.ssp.ingest([tuple]);
    yield fx.state.update(R.setVersions([[input.recordId, version]]));
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'error', message: 'circuit ingest failed after a local write', data: { recordId: input.recordId, error } });
  }
  const role = (yield fx.state.read((s) => s.tabRole)) as ClientState['tabRole'];
  if (role !== 'solo') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'ingest', records: [tuple] } });
  yield fx.timer.set(`debounce:${key}`, debounceDelay(input), { type: 'FlushWrite', key });
  return { mutationId: '', record };
}

/** Timer target: turn the accumulated patch into one outbox row and drain. */
export function* flushWrite(env: SagaEnv, key: string): Saga<void> {
  const pending = (yield fx.state.read((s) => s.pendingWrites.get(key))) as PendingWrite | undefined;
  if (!pending) return;
  yield fx.state.update(R.clearPendingWrite(key));
  const mutationId = (yield fx.id('mutation')) as string;
  const now = (yield fx.now()) as number;
  const rid = parseRecordIdString(pending.recordId);
  const insert = planDeferredOutboxRowTx({
    recordId: rid,
    mutationId: parseRecordIdString(mutationId),
    table: pending.table,
    data: { ...pending.data },
    before: pending.before,
    now,
  });
  yield fx.local.execute(insert.query, insert.vars);
  yield fx.state.update(R.outboxPush({ id: mutationId, type: 'update', recordId: pending.recordId, table: pending.table, status: 'pending', ackedAt: null, attempts: 0 }));
  yield fx.emit({
    type: 'mutation:event',
    event: { type: 'update', mutation_id: parseRecordIdString(mutationId), record_id: rid, data: { ...pending.data }, beforeRecord: pending.before ?? undefined, tableName: pending.table },
  });
  const role = (yield fx.state.read((s) => s.tabRole)) as ClientState['tabRole'];
  if (role === 'follower') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'outbox-changed', mutationId } });
  else yield fx.dispatch({ type: 'Drain' });
}

