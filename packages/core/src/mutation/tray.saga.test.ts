import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { discardFailed, listFailed, retryFailed } from './tray.saga';

const env = defaultEnv({ tables: [{ name: 'thing', columns: { a: {} } }] } as any);
const failedRow = {
  id: '_00_failed_mutations:m1',
  mutationType: 'create',
  recordId: 'thing:1',
  tableName: 'thing',
  data: { a: 1 },
  error: { message: 'denied', kind: 'application' },
  attempts: 1,
  createdAt: 1,
  failedAt: 2,
  revert: 'full',
};

describe('tray', () => {
  it('lists parsed rows, skipping junk', async () => {
    const out = await runPure(listFailed(), { handlers: { 'local.query': () => [[failedRow, { nope: true }]] } });
    expect(out.result).toEqual([expect.objectContaining({ id: '_00_pending_mutations:m1', mutationType: 'create' })]);
    expect((await runPure(listFailed(), { handlers: { 'local.query': () => [] } })).result).toEqual([]);
  });
  it('retry re-applies the write with a new id, drops the tray row, updates the count; missing rows return false', async () => {
    const out = await runPure(retryFailed(env, '_00_pending_mutations:m1'), {
      state: R.setFailedCount(2)(buildState()),
      handlers: {
        'local.query': (e: any) => {
          if (e.sql === 'SELECT * FROM $ids') {
            expect(e.vars.ids).toEqual([new RecordId('_00_failed_mutations', 'm1')]);
            return [[failedRow]];
          }
          if (e.sql === 'DELETE $fid') return [];
          return [];
        },
        'local.execute': () => ({ id: new RecordId('thing', '1'), a: 1 }),
        'ssp.ingest': () => undefined,
      },
    });
    expect(out.result).toBe(true);
    expect(out.state.outbox).toEqual([expect.objectContaining({ id: 'mutation-1', type: 'create', recordId: 'thing:1' })]);
    expect(out.state.failedCount).toBe(1);
    expect(out.emitted).toContainEqual({ type: 'tray:changed', count: 1 });
    expect(out.dispatched).toContainEqual({ type: 'Drain' });
    const missing = await runPure(retryFailed(env, '_00_pending_mutations:zz'), { handlers: { 'local.query': () => [[]] } });
    expect(missing.result).toBe(false);
  });
  it('discard drops the row and never goes below zero', async () => {
    const out = await runPure(discardFailed('_00_pending_mutations:m1'), {
      state: buildState(),
      handlers: { 'local.query': (e: any) => (e.sql === 'SELECT * FROM $ids' ? [[failedRow]] : []) },
    });
    expect(out.result).toBe(true);
    expect(out.state.failedCount).toBe(0);
    expect(out.emitted).toContainEqual({ type: 'tabs:broadcast', message: { type: 'failed-mutations-changed', count: 0 } });
    expect((await runPure(discardFailed('nope'), { handlers: { 'local.query': () => [] } })).result).toBe(false);
  });
});
