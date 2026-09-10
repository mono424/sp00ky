import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildState } from '../testing/build';
import { defaultEnv } from '../query/env';
import { flushWrite, write } from './write.saga';

const env = defaultEnv({ tables: [{ name: 'thing', columns: { a: {}, b: {} } }] } as any);
const rid = new RecordId('thing', '1');
const withQuery = () => buildState([buildEntry({ def: { hash: 'q', tableName: 'thing' } })]);

describe('write', () => {
  it('rejects unknown tables', async () => {
    await expect(runPure(write(env, { kind: 'create', recordId: 'nope:1', data: {} }))).rejects.toThrow('Table nope not found');
  });

  it('create: local tx, outbox item, circuit ingest, version 1, event, drain; dirties the table', async () => {
    const out = await runPure(write(env, { kind: 'create', recordId: 'thing:1', data: { a: 1, junk: 2 } }), {
      state: withQuery(),
      handlers: {
        'local.execute': (e: any) => {
          expect(e.query.sql).toContain("mutationType = 'create'");
          expect(e.vars.data).toEqual({ a: 1 });
          return { id: rid, a: 1 };
        },
        'ssp.ingest': (e: any) => {
          expect(e.records).toEqual([{ table: 'thing', op: 'CREATE', id: 'thing:1', record: { id: rid, a: 1, _00_rv: 1 } }]);
        },
      },
    });
    expect(out.result).toEqual({ mutationId: 'mutation-1', record: { id: rid, a: 1 } });
    expect(out.state.outbox).toEqual([{ id: 'mutation-1', type: 'create', recordId: 'thing:1', table: 'thing', status: 'pending', ackedAt: null, attempts: 0 }]);
    expect(out.state.versions.get('thing:1')).toBe(1);
    expect(out.state.dirty.has('q')).toBe(true);
    expect(out.emitted[0]).toMatchObject({ type: 'mutation:event', event: { type: 'create', tableName: 'thing' } });
    expect(out.dispatched).toEqual([{ type: 'Drain' }]);
    expect(out.log.filter((e) => e.kind === 'local.query')).toHaveLength(0);
  });

  it('update: reads before, tx with beforeRecord, version from the returned row; delete: before feeds the circuit', async () => {
    const upd = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { a: 2 } }), {
      state: withQuery(),
      handlers: {
        'local.query': () => [{ id: rid, a: 1, _00_rv: 4 }],
        'local.execute': (e: any) => {
          expect(e.vars.before).toEqual({ id: rid, a: 1, _00_rv: 4 });
          return { target: { id: rid, a: 2, _00_rv: 5 } };
        },
        'ssp.ingest': (e: any) => expect(e.records[0]).toMatchObject({ op: 'UPDATE', record: { a: 2, _00_rv: 5 } }),
      },
    });
    expect(upd.result.record).toEqual({ id: rid, a: 2, _00_rv: 5 });
    expect(upd.state.versions.get('thing:1')).toBe(5);
    expect(upd.emitted[0]).toMatchObject({ event: { beforeRecord: { a: 1 } } });
    const noRow = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { a: 2 } }), {
      state: withQuery(),
      handlers: { 'local.query': () => [null], 'local.execute': () => ({ target: null }), 'ssp.ingest': () => undefined },
    });
    expect(noRow.result.record).toBeNull();
    expect(noRow.state.versions.get('thing:1')).toBe(1);
    const del = await runPure(write(env, { kind: 'delete', recordId: 'thing:1' }), {
      state: buildState([], (s) => ({ ...s, versions: new Map([['thing:1', 3]]) })),
      handlers: {
        'local.query': () => [{ id: rid, a: 1, _00_rv: 3 }],
        'local.execute': () => undefined,
        'ssp.ingest': (e: any) => expect(e.records[0]).toEqual({ table: 'thing', op: 'DELETE', id: 'thing:1', record: { id: rid, a: 1, _00_rv: 3 } }),
      },
    });
    expect(del.result.record).toBeNull();
    expect(del.state.versions.has('thing:1')).toBe(false);
    expect(del.state.outbox[0].type).toBe('delete');
    const delNoRow = await runPure(write(env, { kind: 'delete', recordId: 'thing:1' }), {
      state: buildState(),
      handlers: {
        'local.query': () => [[]],
        'local.execute': () => undefined,
        'ssp.ingest': (e: any) => expect(e.records[0].record).toEqual({}),
      },
    });
    expect(delNoRow.emitted[0]).toMatchObject({ event: { beforeRecord: undefined } });
  });

  it('a failing circuit ingest is logged, the write still stands; a follower broadcasts instead of draining', async () => {
    const out = await runPure(write(env, { kind: 'create', recordId: 'thing:1', data: {} }), {
      state: { ...buildState(), tabRole: 'follower' },
      handlers: {
        'local.execute': () => ({ id: rid }),
        'ssp.ingest': () => {
          throw new Error('wasm');
        },
      },
    });
    expect(out.emitted.some((e) => e.type === 'log' && e.level === 'error')).toBe(true);
    expect(out.emitted).toContainEqual({ type: 'tabs:broadcast', message: { type: 'outbox-changed', mutationId: 'mutation-1' } });
    expect(out.emitted).toContainEqual({ type: 'tabs:broadcast', message: { type: 'ingest', records: [expect.objectContaining({ id: 'thing:1', op: 'CREATE' })] } });
    expect(out.dispatched).toEqual([]);
    expect(out.state.outbox).toHaveLength(1);
  });

  it('debounced update: applies locally, merges the patch per key, arms the flush timer; flush writes one outbox row', async () => {
    const first = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { a: 1 }, options: { debounced: true } }), {
      state: withQuery(),
      handlers: {
        'local.query': () => [{ id: rid, a: 0, _00_rv: 1 }],
        'local.execute': () => ({ target: { id: rid, a: 1, _00_rv: 2 } }),
        'ssp.ingest': () => undefined,
      },
    });
    expect(first.result).toEqual({ mutationId: '', record: { id: rid, a: 1, _00_rv: 2 } });
    expect(first.state.outbox).toEqual([]);
    const key = 'thing:1::a';
    expect(first.state.pendingWrites.get(key)).toMatchObject({ data: { a: 1 }, before: { a: 0 } });
    expect(first.timers.get(`debounce:${key}`)).toEqual({ ms: 300, event: { type: 'FlushWrite', key } });
    expect(first.state.dirty.has('q')).toBe(true);
    const second = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { a: 5 }, options: { debounced: { delay: 50, key: 'recordId_x_fields' } } }), {
      state: first.state,
      handlers: {
        'local.execute': () => ({ target: { id: rid, a: 5, _00_rv: 3 } }),
        'ssp.ingest': () => {
          throw new Error('wasm');
        },
      },
    });
    expect(second.log.filter((e) => e.kind === 'local.query')).toHaveLength(0);
    expect(second.state.pendingWrites.get(key)).toMatchObject({ data: { a: 5 }, before: { a: 0 } });
    expect(second.timers.get(`debounce:${key}`)!.ms).toBe(50);
    const byRecord = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { b: 1 }, options: { debounced: { key: 'recordId' } } }), {
      state: withQuery(),
      handlers: { 'local.query': () => [], 'local.execute': () => undefined, 'ssp.ingest': () => undefined },
    });
    expect(byRecord.state.pendingWrites.has('thing:1')).toBe(true);
    expect(byRecord.result.record).toBeNull();
    const noData = await runPure(write(env, { kind: 'update', recordId: 'thing:1', options: { debounced: true } }), {
      state: withQuery(),
      handlers: { 'local.query': () => undefined, 'local.execute': () => undefined, 'ssp.ingest': () => undefined },
    });
    expect(noData.state.pendingWrites.has('thing:1::')).toBe(true);
    const flushedNoBefore = await runPure(flushWrite(env, 'thing:1'), { state: byRecord.state, handlers: { 'local.execute': () => undefined } });
    expect(flushedNoBefore.emitted[0]).toMatchObject({ event: { beforeRecord: undefined } });
    const flushed = await runPure(flushWrite(env, key), {
      state: second.state,
      handlers: {
        'local.execute': (e: any) => {
          expect(e.query.sql).toContain("mutationType = 'update'");
          expect(e.vars).toMatchObject({ data: { a: 5 }, before: { id: rid, a: 0, _00_rv: 1 }, table: 'thing' });
        },
      },
    });
    expect(flushed.state.pendingWrites.size).toBe(0);
    expect(flushed.state.outbox).toEqual([expect.objectContaining({ id: 'mutation-1', type: 'update', recordId: 'thing:1' })]);
    expect(flushed.emitted[0]).toMatchObject({ type: 'mutation:event', event: { type: 'update', data: { a: 5 } } });
    expect(flushed.dispatched).toEqual([{ type: 'Drain' }]);
    const nothing = await runPure(flushWrite(env, 'missing'), { state: buildState() });
    expect(nothing.dispatched).toEqual([]);
    const follower = await runPure(flushWrite(env, key), { state: { ...second.state, tabRole: 'follower' }, handlers: { 'local.execute': () => undefined } });
    expect(follower.emitted).toContainEqual(expect.objectContaining({ type: 'tabs:broadcast' }));
    const leaderDebounced = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { a: 1 }, options: { debounced: true } }), {
      state: { ...withQuery(), tabRole: 'leader' },
      handlers: { 'local.query': () => [], 'local.execute': () => undefined, 'ssp.ingest': () => undefined },
    });
    expect(leaderDebounced.emitted).toContainEqual({ type: 'tabs:broadcast', message: { type: 'ingest', records: [expect.objectContaining({ op: 'UPDATE' })] } });
  });
});
