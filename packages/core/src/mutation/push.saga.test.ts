import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { drain, loadOutbox, refreshFailedCount, rollback } from './push.saga';
import type { StatementResult } from '../kernel/effects';
import type { PendingMutationRow } from './rows';

const env = defaultEnv({ tables: [{ name: 'thing', columns: {} }] } as any, { outboxBatchSize: 2 });
const ok = (result: unknown = null): StatementResult => ({ status: 'OK', result });
const err = (error: string): StatementResult => ({ status: 'ERR', error });
const pendingRow = (n: number, type: PendingMutationRow['mutationType'] = 'create', extra: Partial<PendingMutationRow> = {}) => ({
  id: `_00_pending_mutations:m${n}`,
  mutationType: type,
  recordId: `thing:${n}`,
  tableName: 'thing',
  data: type === 'delete' ? undefined : { a: n },
  beforeRecord: type === 'create' ? undefined : { id: `thing:${n}`, a: 0, _00_rv: 7 },
  createdAt: 1,
  v: 2,
  ...extra,
});
const items = (...ns: number[]) => ns.map((n) => buildOutboxItem({ id: `_00_pending_mutations:m${n}`, recordId: `thing:${n}`, type: 'create' }));
const stateWith = (rows: number[], ...extra: Parameters<typeof buildState>[1][]) =>
  buildState([buildEntry({ def: { hash: 'q', tableName: 'thing' } })], R.outboxReplace(items(...rows)), ...extra);

describe('drain', () => {
  it('re-types stored payloads from the schema before pushing (a stored record field is a string)', async () => {
    const typedEnv = defaultEnv({ tables: [{ name: 'thing', columns: { owner: { recordId: true } } }] } as any, { outboxBatchSize: 2 });
    let vars: any = null;
    await runPure(drain(typedEnv), {
      state: stateWith([1]),
      handlers: {
        'local.query': (e: any) => (e.sql === 'SELECT * FROM $ids' ? [[pendingRow(1, 'create', { data: { owner: 'user:u1' } })]] : []),
        'remote.query': (e: any) => ((vars = e.vars), [ok()]),
      },
    });
    expect(vars.d0_owner).toEqual(new RecordId('user', 'u1'));
  });

  it('pushes batches in FIFO order, acks each accepted row, deletes its pending row, then loops', async () => {
    const pushed: string[] = [];
    const out = await runPure(drain(env), {
      state: stateWith([1, 2, 3]),
      now: 500,
      handlers: {
        'local.query': (e: any) => {
          if (e.sql === 'SELECT * FROM $ids') return [e.vars.ids.map((id: RecordId) => pendingRow(Number(String(id.id).slice(1))))];
          return [];
        },
        'remote.query': (e: any) => {
          pushed.push(e.sql);
          return e.sql.split(';\n').map(() => ok());
        },
      },
    });
    expect(pushed).toEqual(['CREATE ONLY $id0 SET a = $d0_a;\nCREATE ONLY $id1 SET a = $d1_a', 'CREATE ONLY $id0 SET a = $d0_a']);
    expect(out.state.outbox.every((i) => i.status === 'acked' && i.ackedAt === 500)).toBe(true);
    expect(out.log.filter((e) => e.kind === 'local.query' && (e as any).sql === 'DELETE $mid')).toHaveLength(3);
    expect(out.emitted.filter((e) => e.type === 'mutation:settled')).toHaveLength(3);
    expect(out.timers.get('ack-prune')).toEqual({ ms: 30_000, event: { type: 'AckPrune' } });
    expect(out.dispatched.filter((d) => d.type === 'SyncOutcome')).toEqual([{ type: 'SyncOutcome', ok: true, error: undefined }, { type: 'SyncOutcome', ok: true, error: undefined }]);
  });

  it('a rejected statement rolls back only that row; the rest of the batch is acked', async () => {
    const out = await runPure(drain(env), {
      state: stateWith([1, 2]),
      handlers: {
        'local.query': (e: any) => (e.sql === 'SELECT * FROM $ids' ? [[pendingRow(1), pendingRow(2)]] : []),
        'remote.query': () => [err('permission denied'), ok()],
        'local.execute': () => undefined,
        'ssp.ingest': () => undefined,
      },
    });
    expect(out.state.outbox.map((i) => [i.id, i.status])).toEqual([['_00_pending_mutations:m2', 'acked']]);
    expect(out.state.failedCount).toBe(1);
    expect(out.emitted).toContainEqual(expect.objectContaining({ type: 'mutation:rolled-back', mutationId: '_00_pending_mutations:m1', error: 'permission denied' }));
    expect(out.dispatched.at(-1)).toEqual({ type: 'SyncOutcome', ok: true, error: undefined });
  });

  it('transport failures keep the tail queued behind a backoff timer (thrown, or a network ERR mid-batch, or a short result list)', async () => {
    const thrown = await runPure(drain(env), {
      state: stateWith([1, 2]),
      handlers: {
        'local.query': () => [[pendingRow(1), pendingRow(2)]],
        'remote.query': () => {
          throw new Error('socket closed');
        },
      },
    });
    expect(thrown.state.outbox.map((i) => i.status)).toEqual(['pending', 'pending']);
    expect(thrown.state.outbox[0].attempts).toBe(1);
    expect(thrown.timers.get('outbox')).toEqual({ ms: 500, event: { type: 'Drain' } });
    expect(thrown.dispatched).toEqual([{ type: 'SyncOutcome', ok: false, error: new Error('socket closed') }]);
    const mid = await runPure(drain(env), {
      state: stateWith([1, 2]),
      handlers: { 'local.query': () => [[pendingRow(1), pendingRow(2)]], 'remote.query': () => [ok(), err('connection reset')] },
    });
    expect(mid.state.outbox.map((i) => [i.status, i.attempts])).toEqual([['acked', 0], ['pending', 1]]);
    expect(mid.timers.get('outbox')!.ms).toBe(500);
    const short = await runPure(drain(env), {
      state: stateWith([1, 2]),
      handlers: { 'local.query': () => [[pendingRow(1), pendingRow(2)]], 'remote.query': () => [ok()] },
    });
    expect(short.state.outbox.map((i) => i.status)).toEqual(['acked', 'pending']);
    const undefinedRows = await runPure(drain(env), { state: stateWith([1]), handlers: { 'local.query': () => undefined } });
    expect(undefinedRows.state.outbox).toEqual([]);
    const notArray = await runPure(drain(env), {
      state: stateWith([1]),
      handlers: { 'local.query': () => [[pendingRow(1)]], 'remote.query': () => 'nope' },
    });
    expect(notArray.timers.has('outbox')).toBe(true);
  });

  it('a request rejected as a whole with an application error rolls back the head and continues', async () => {
    let calls = 0;
    const out = await runPure(drain(env), {
      state: stateWith([1, 2]),
      handlers: {
        'local.query': (e: any) => (e.sql === 'SELECT * FROM $ids' ? [e.vars.ids.map((id: RecordId) => pendingRow(Number(String(id.id).slice(1))))] : []),
        'remote.query': () => {
          calls++;
          if (calls === 1) throw 'Parse error at line 1';
          return [ok()];
        },
        'local.execute': () => undefined,
        'ssp.ingest': () => undefined,
      },
    });
    expect(calls).toBe(2);
    expect(out.state.failedCount).toBe(1);
    expect(out.state.outbox.map((i) => [i.id, i.status])).toEqual([['_00_pending_mutations:m2', 'acked']]);
    const asError = await runPure(drain(env), {
      state: stateWith([1]),
      handlers: {
        'local.query': () => [[pendingRow(1)]],
        'remote.query': () => {
          throw new Error('Parse error at line 1');
        },
        'local.execute': (e: any) => {
          if (e.vars.failed) expect(e.vars.failed.error.message).toBe('Parse error at line 1');
        },
        'ssp.ingest': () => undefined,
      },
    });
    expect(asError.state.failedCount).toBe(1);
  });

  it('skips pending rows the store no longer has, does nothing as a follower or when empty, tolerates a failed row delete', async () => {
    const gone = await runPure(drain(env), { state: stateWith([1]), handlers: { 'local.query': () => [[]] } });
    expect(gone.state.outbox).toEqual([]);
    expect(gone.log.filter((e) => e.kind === 'remote.query')).toHaveLength(0);
    const follower = await runPure(drain(env), { state: { ...stateWith([1]), tabRole: 'follower' } });
    expect(follower.log.filter((e) => e.kind === 'local.query')).toHaveLength(0);
    const empty = await runPure(drain(env), { state: buildState() });
    expect(empty.log.filter((e) => e.kind === 'local.query')).toHaveLength(0);
    const delFail = await runPure(drain(env), {
      state: stateWith([1]),
      handlers: {
        'local.query': (e: any) => {
          if (e.sql === 'DELETE $mid') throw new Error('locked');
          return [[pendingRow(1)]];
        },
        'remote.query': () => [ok()],
      },
    });
    expect(delFail.state.outbox[0].status).toBe('acked');
    expect(delFail.emitted).toContainEqual(expect.objectContaining({ level: 'error' }));
  });
});

describe('rollback', () => {
  it('create: tray move first, local delete, circuit DELETE, version dropped, owner relay when leading', async () => {
    const order: string[] = [];
    const state = { ...stateWith([1]), tabRole: 'leader' as const, tabId: 'tab-a' };
    const row = pendingRow(1, 'create', { id: '_00_pending_mutations:1700000000000_0001_tabz' });
    const out = await runPure(rollback(env, row, { message: 'denied', kind: 'application' }), {
      state: R.setVersions([['thing:1', 1]])(state),
      handlers: {
        'local.execute': (e: any) => void order.push(e.query.sql.includes('_00_failed') || e.query.sql.includes('CREATE ONLY $fid') ? 'tray' : 'revert'),
        'ssp.ingest': (e: any) => void order.push(`circuit:${e.records[0].op}`),
      },
    });
    expect(order).toEqual(['tray', 'revert', 'circuit:DELETE']);
    expect(out.state.versions.has('thing:1')).toBe(false);
    expect(out.state.failedCount).toBe(1);
    expect(out.state.dirty.has('q')).toBe(true);
    expect(out.emitted).toContainEqual({ type: 'tabs:sendTo', tabId: 'tabz', message: expect.objectContaining({ type: 'mutation-rolled-back' }) });
    expect(out.emitted).toContainEqual({ type: 'tabs:broadcast', message: { type: 'ingest', records: [expect.objectContaining({ op: 'DELETE' })] } });
    expect(out.emitted).toContainEqual({ type: 'tray:changed', count: 1 });
    expect(out.emitted).toContainEqual({ type: 'tabs:broadcast', message: { type: 'failed-mutations-changed', count: 1 } });
  });
  it('update / delete restore beforeRecord and its version; legacy rows read the server row (or end partial)', async () => {
    const upd = await runPure(rollback(env, pendingRow(1, 'update'), { message: 'x', kind: 'application' }), {
      state: stateWith([1]),
      handlers: { 'local.execute': () => undefined, 'ssp.ingest': (e: any) => expect(e.records[0].op).toBe('UPDATE') },
    });
    expect(upd.state.versions.get('thing:1')).toBe(7);
    const del = await runPure(rollback(env, pendingRow(1, 'delete'), { message: 'x', kind: 'application' }), {
      state: stateWith([1]),
      handlers: { 'local.execute': () => undefined, 'ssp.ingest': (e: any) => expect(e.records[0].op).toBe('CREATE') },
    });
    expect(del.state.versions.get('thing:1')).toBe(7);
    const legacy = await runPure(rollback(env, pendingRow(1, 'update', { beforeRecord: undefined }), { message: 'x', kind: 'application' }), {
      state: stateWith([1]),
      handlers: {
        'remote.query': () => [ok({ id: new RecordId('thing', '1'), a: 9, _00_rv: 2 })],
        'local.execute': (e: any) => {
          if (e.query.sql.includes('CREATE ONLY $fid')) expect(e.vars.failed.revert).toBe('full');
        },
        'ssp.ingest': () => undefined,
      },
    });
    expect(legacy.state.versions.get('thing:1')).toBe(2);
    const partial = await runPure(rollback(env, pendingRow(1, 'update', { beforeRecord: undefined }), { message: 'x', kind: 'application' }), {
      state: stateWith([1]),
      handlers: {
        'remote.query': () => {
          throw new Error('offline');
        },
        'local.execute': (e: any) => expect(e.vars.failed.revert).toBe('partial'),
        'ssp.ingest': () => undefined,
      },
    });
    expect(partial.log.filter((e) => e.kind === 'local.execute')).toHaveLength(1);
    const errRow = await runPure(rollback(env, pendingRow(1, 'update', { beforeRecord: undefined }), { message: 'x', kind: 'application' }), {
      state: stateWith([1]),
      handlers: { 'remote.query': () => [err('nope')], 'local.execute': () => undefined, 'ssp.ingest': () => undefined },
    });
    expect(errRow.state.failedCount).toBe(1);
  });
  it('every local step is best-effort: tray move, revert and circuit failures are logged', async () => {
    const out = await runPure(rollback(env, pendingRow(1, 'update'), { message: 'x', kind: 'unreplayable' }), {
      state: stateWith([1]),
      handlers: {
        'local.execute': () => {
          throw new Error('disk');
        },
        'ssp.ingest': () => {
          throw new Error('wasm');
        },
      },
    });
    expect(out.emitted.filter((e) => e.type === 'log')).toHaveLength(3);
    expect(out.state.outbox).toEqual([]);
  });
});

describe('loadOutbox / refreshFailedCount', () => {
  it('mirrors pending rows, sends unreplayable ones to the tray, refreshes the count, starts a drain', async () => {
    const out = await runPure(loadOutbox(env), {
      state: buildState(),
      handlers: {
        'local.query': (e: any) => {
          if (e.sql.startsWith('SELECT * FROM _00_pending_mutations'))
            return [[pendingRow(1), { id: '_00_pending_mutations:bad', mutationType: 'create', recordId: 'thing:9' }, { tableName: 'thing' }, { mutationType: 'delete' }, 'junk']];
          if (e.sql.startsWith('SELECT count()')) return [[{ count: 4 }]];
          return [];
        },
        'local.execute': () => undefined,
        'ssp.ingest': () => undefined,
      },
    });
    expect(out.state.outbox.map((i) => i.id)).toEqual(['_00_pending_mutations:m1']);
    expect(out.emitted).toContainEqual(expect.objectContaining({ type: 'mutation:rolled-back', mutationId: '_00_pending_mutations:bad' }));
    expect(out.emitted).toContainEqual(expect.objectContaining({ type: 'mutation:rolled-back', mutationId: '' }));
    expect(out.state.failedCount).toBe(4);
    expect(out.dispatched).toEqual([{ type: 'Drain' }]);
    const empty = await runPure(loadOutbox(env), { state: buildState(), handlers: { 'local.query': () => [] } });
    expect(empty.dispatched).toEqual([]);
    const missingTable = await runPure(refreshFailedCount(), {
      state: R.setFailedCount(2)(buildState()),
      handlers: {
        'local.query': () => {
          throw new Error('no table');
        },
      },
    });
    expect(missingTable.state.failedCount).toBe(2);
    const noRows = await runPure(refreshFailedCount(), { state: R.setFailedCount(2)(buildState()), handlers: { 'local.query': () => [[]] } });
    expect(noRows.state.failedCount).toBe(0);
  });
});
