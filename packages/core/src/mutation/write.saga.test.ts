import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import { defaultEnv } from '../query/env';
import { adoptPendingWrites, flushPendingWrites, flushWrite, peerCreate, peerDelete, peerWriteExpired, readRow, write } from './write.saga';
import * as R from '../state/reducers';

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

  it('flushOnHide (default): the merged patch is mirrored in the same tx, and the flush moves it into the outbox', async () => {
    const now = 1_000;
    const first = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { a: 1 }, options: { debounced: true } }), {
      state: withQuery(),
      now,
      handlers: {
        'local.query': () => [{ id: rid, a: 0, _00_rv: 1 }],
        'local.execute': (e: any) => {
          expect(e.query.sql).toContain('UPSERT ONLY $wid REPLACE $wrow');
          expect(e.vars.wid).toEqual(new RecordId('_00_pending_writes', 'mutation-1'));
          expect(e.vars.wrow).toEqual({ recordId: rid, tableName: 'thing', data: { a: 1 }, beforeRecord: { id: rid, a: 0, _00_rv: 1 }, flushBy: now + 300 });
          return { target: { id: rid, a: 1, _00_rv: 2 } };
        },
        'ssp.ingest': () => undefined,
      },
    });
    const key = 'thing:1::a';
    expect(first.state.pendingWrites.get(key)!.mirrorId).toBe('_00_pending_writes:mutation-1');
    const second = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { a: 7 }, options: { debounced: { delay: 50 } } }), {
      state: first.state,
      now: now + 10,
      handlers: {
        'local.execute': (e: any) => {
          expect(e.vars.wid).toEqual(new RecordId('_00_pending_writes', 'mutation-1'));
          expect(e.vars.wrow).toMatchObject({ data: { a: 7 }, flushBy: now + 60 });
          return { target: { id: rid, a: 7, _00_rv: 3 } };
        },
        'ssp.ingest': () => undefined,
      },
    });
    expect(second.log.filter((e) => e.kind === 'id')).toHaveLength(0);
    const flushed = await runPure(flushWrite(env, key), {
      state: second.state,
      handlers: {
        'local.execute': (e: any) => {
          expect(e.query.sql).toContain('DELETE $wid');
          expect(e.vars).toMatchObject({ data: { a: 7 }, wid: new RecordId('_00_pending_writes', 'mutation-1') });
        },
      },
    });
    expect(flushed.state.pendingWrites.size).toBe(0);
    expect(flushed.state.outbox).toHaveLength(1);
    const armed: unknown[] = [];
    const closed = runPure(flushWrite(env, key), {
      state: second.state,
      handlers: {
        'local.execute': () => {
          throw new Error('store closed');
        },
        'timer.set': (e: any) => void armed.push([e.key, e.ms, e.event]),
      },
    });
    await expect(closed).rejects.toThrow('store closed');
    expect(armed).toEqual([['adopt-writes', 3_000, { type: 'AdoptPendingWrites' }]]);
  });

  it('flushOnHide: false keeps the patch in memory only', async () => {
    const out = await runPure(write(env, { kind: 'update', recordId: 'thing:1', data: { a: 1 }, options: { debounced: { flushOnHide: false } } }), {
      state: withQuery(),
      handlers: {
        'local.query': () => [],
        'local.execute': (e: any) => {
          expect(e.query.sql).not.toContain('UPSERT');
          expect(e.vars.wid).toBeUndefined();
        },
        'ssp.ingest': () => undefined,
      },
    });
    expect(out.state.pendingWrites.get('thing:1::a')!.mirrorId).toBeNull();
    const flushed = await runPure(flushWrite(env, 'thing:1::a'), {
      state: out.state,
      handlers: { 'local.execute': (e: any) => expect(e.query.sql).not.toContain('DELETE') },
    });
    expect(flushed.state.outbox).toHaveLength(1);
  });
});

describe('flushPendingWrites', () => {
  it('flushes every mirrored pending write now, leaves memory-only ones to their timer', async () => {
    const base = { table: 'thing', recordId: 'thing:1', data: { a: 1 }, before: null, firstAt: 0 };
    const state = R.compose(
      R.mergePendingWrite({ ...base, key: 'k1', mirrorId: '_00_pending_writes:1' }),
      R.mergePendingWrite({ ...base, key: 'k2', mirrorId: null }),
      R.mergePendingWrite({ ...base, key: 'k3', mirrorId: '_00_pending_writes:3' })
    )(buildState());
    const out = await runPure(flushPendingWrites(), { state });
    expect(out.log.filter((e) => e.kind === 'timer.clear').map((e: any) => e.key)).toEqual(['debounce:k1', 'debounce:k3']);
    expect(out.dispatched).toEqual([{ type: 'FlushWrite', key: 'k1' }, { type: 'FlushWrite', key: 'k3' }]);
    const none = await runPure(flushPendingWrites(), { state: buildState() });
    expect(none.dispatched).toEqual([]);
  });
});

describe('adoptPendingWrites', () => {
  const now = 100_000;
  const row = (id: string, flushBy: number) => ({ id: `_00_pending_writes:${id}`, recordId: rid, tableName: 'thing', data: { a: 1 }, beforeRecord: { a: 0 }, flushBy });
  const held = R.mergePendingWrite({ key: 'k', table: 'thing', recordId: 'thing:1', data: {}, before: null, firstAt: 0, mirrorId: '_00_pending_writes:mine' })(buildState());

  it('moves overdue rows nobody here holds into the outbox, re-checks rows that may still be live', async () => {
    const moved: any[] = [];
    const out = await runPure(adoptPendingWrites(), {
      state: held,
      now,
      handlers: {
        'local.query': (e: any) => {
          expect(e.sql).toBe('SELECT * FROM _00_pending_writes');
          return [[row('mine', 0), row('dead', now - 3_000), row('live', now + 500), row('other-live', now + 2_000), 'junk']];
        },
        'local.execute': (e: any) => {
          moved.push(e.vars);
        },
      },
    });
    expect(moved).toHaveLength(1);
    expect(moved[0]).toMatchObject({ id: rid, table: 'thing', data: { a: 1 }, before: { a: 0 }, createdAt: now, wid: new RecordId('_00_pending_writes', 'dead') });
    expect(out.state.outbox).toEqual([{ id: 'mutation-1', type: 'update', recordId: 'thing:1', table: 'thing', status: 'pending', ackedAt: null, attempts: 0 }]);
    expect(out.dispatched).toEqual([{ type: 'Drain' }]);
    expect(out.timers.get('adopt-writes')).toEqual({ ms: 3_500, event: { type: 'AdoptPendingWrites' } });
  });

  it('a follower nudges the leader instead; a failed move is logged and skipped; no table means nothing to do', async () => {
    const follower = await runPure(adoptPendingWrites(), { state: { ...buildState(), tabRole: 'follower' }, now, handlers: { 'local.query': () => [[row('dead', 0)]] } });
    expect(follower.log.some((e) => e.kind === 'local.execute')).toBe(false);
    expect(follower.emitted).toEqual([{ type: 'tabs:broadcast', message: { type: 'outbox-changed', mutationId: '' } }]);
    const quietFollower = await runPure(adoptPendingWrites(), { state: { ...held, tabRole: 'follower' }, now, handlers: { 'local.query': () => [[row('mine', 0)]] } });
    expect(quietFollower.emitted).toEqual([]);
    const failing = await runPure(adoptPendingWrites(), {
      state: buildState(),
      now,
      handlers: {
        'local.query': () => [[row('dead', 0)]],
        'local.execute': () => {
          throw new Error('closed');
        },
      },
    });
    expect(failing.state.outbox).toEqual([]);
    expect(failing.dispatched).toEqual([]);
    expect(failing.emitted).toContainEqual(expect.objectContaining({ type: 'log', level: 'warn' }));
    const noTable = await runPure(adoptPendingWrites(), {
      state: buildState(),
      handlers: {
        'local.query': () => {
          throw new Error('no table');
        },
      },
    });
    expect(noTable.dispatched).toEqual([]);
    const empty = await runPure(adoptPendingWrites(), { state: buildState(), handlers: { 'local.query': () => [[]] } });
    expect(empty.timers.size).toBe(0);
  });
});

describe('channel peer writes', () => {
  const row = { id: rid, a: 1 };
  const local = (existing: unknown) => ({ 'local.query': () => [existing] });

  it('readRow: the stored row, or null', async () => {
    expect((await runPure(readRow('thing:1'), { handlers: local(row) })).result).toEqual(row);
    expect((await runPure(readRow('thing:1'), { handlers: local(null) })).result).toBeNull();
  });

  it('peerCreate: inserts without an outbox entry, feeds the circuit unversioned, shows it, arms the expiry', async () => {
    const out = await runPure(peerCreate(env, { recordId: 'thing:1', data: { a: 1, junk: 2 }, ttlMs: 5000 }), {
      state: withQuery(),
      handlers: {
        ...local(null),
        'local.execute': (e: any) => {
          expect(e.query.sql).not.toContain('mutationType');
          expect(e.vars).toEqual({ id: rid, data_a: 1 });
          return row;
        },
        'ssp.ingest': (e: any) => expect(e.records).toEqual([{ table: 'thing', op: 'CREATE', id: 'thing:1', record: { ...row, _00_rv: 0 } }]),
      },
    });
    expect(out.result).toBe(true);
    expect(out.state.outbox).toEqual([]);
    expect(out.state.versions.has('thing:1')).toBe(false);
    expect(out.state.peerWrites.get('thing:1')).toEqual({ kind: 'create', table: 'thing', expiresAt: 1_700_000_005_000 });
    expect(out.state.dirty.has('q')).toBe(true);
    expect(out.timers.get('peer:thing:1')).toEqual({ ms: 5000, event: { type: 'PeerWriteExpired', id: 'thing:1' } });
  });

  it('peerCreate: refuses unknown tables, confirmed rows, own pending writes and a second create', async () => {
    expect((await runPure(peerCreate(env, { recordId: 'nope:1', ttlMs: 1 }))).result).toBe(false);
    const confirmed = buildState([], (s) => ({ ...s, versions: new Map([['thing:1', 2]]) }));
    expect((await runPure(peerCreate(env, { recordId: 'thing:1', ttlMs: 1 }), { state: confirmed })).result).toBe(false);
    const own = buildState([], R.outboxReplace([buildOutboxItem({ id: 'm', type: 'update', recordId: 'thing:1' })]));
    expect((await runPure(peerCreate(env, { recordId: 'thing:1', ttlMs: 1 }), { state: own })).result).toBe(false);
    const twice = R.setPeerWrite('thing:1', { kind: 'create', table: 'thing', expiresAt: 1 })(buildState());
    expect((await runPure(peerCreate(env, { recordId: 'thing:1', ttlMs: 1 }), { state: twice })).result).toBe(false);
  });

  it("peerCreate: an existing unconfirmed row (another tab's copy) is shown without a second insert", async () => {
    const out = await runPure(peerCreate(env, { recordId: 'thing:1', ttlMs: 10 }), {
      state: withQuery(),
      handlers: { ...local(row), 'ssp.ingest': () => undefined },
    });
    expect(out.result).toBe(true);
    expect(out.log.some((e) => e.kind === 'local.execute')).toBe(false);
  });

  it('peerCreate: a store refusal or an empty answer applies nothing; a circuit failure is logged and still shows', async () => {
    const refused = await runPure(peerCreate(env, { recordId: 'thing:1', ttlMs: 10 }), {
      handlers: { ...local(null), 'local.execute': () => { throw new Error('schema'); } },
    });
    expect(refused.result).toBe(false);
    expect(refused.emitted[0]).toMatchObject({ type: 'log', level: 'debug' });
    const empty = await runPure(peerCreate(env, { recordId: 'thing:1', ttlMs: 10 }), { handlers: { ...local(null), 'local.execute': () => [] } });
    expect(empty.result).toBe(false);
    const circuit = await runPure(peerCreate(env, { recordId: 'thing:1', data: { a: 1 }, ttlMs: 10 }), {
      handlers: { ...local(null), 'local.execute': () => row, 'ssp.ingest': () => { throw new Error('wasm'); } },
    });
    expect(circuit.result).toBe(true);
    expect(circuit.emitted[0]).toMatchObject({ type: 'log', level: 'error' });
  });

  it('peerDelete: hides an existing row, keeps it in the store, arms the expiry', async () => {
    const out = await runPure(peerDelete(env, { recordId: 'thing:1', ttlMs: 3000 }), { state: withQuery(), handlers: local(row) });
    expect(out.result).toBe(true);
    expect(out.state.peerWrites.get('thing:1')).toMatchObject({ kind: 'delete' });
    expect(out.log.some((e) => e.kind === 'local.execute')).toBe(false);
    expect(out.timers.get('peer:thing:1')?.ms).toBe(3000);
    expect((await runPure(peerDelete(env, { recordId: 'thing:1', ttlMs: 1 }), { handlers: local(null) })).result).toBe(false);
    expect((await runPure(peerDelete(env, { recordId: 'nope:1', ttlMs: 1 }))).result).toBe(false);
  });

  it("peerDelete: of the peer's own unconfirmed create undoes it; of a confirmed one hides it", async () => {
    const created = R.setPeerWrite('thing:1', { kind: 'create', table: 'thing', expiresAt: 1 })(withQuery());
    const undo = await runPure(peerDelete(env, { recordId: 'thing:1', ttlMs: 1 }), {
      state: created,
      handlers: {
        ...local(row),
        'local.execute': (e: any) => expect(e.query.sql).toContain('DELETE'),
        'ssp.ingest': (e: any) => expect(e.records).toEqual([{ table: 'thing', op: 'DELETE', id: 'thing:1', record: row }]),
      },
    });
    expect(undo.result).toBe(true);
    expect(undo.state.peerWrites.size).toBe(0);
    const confirmed = { ...created, versions: new Map([['thing:1', 1]]) };
    const hide = await runPure(peerDelete(env, { recordId: 'thing:1', ttlMs: 1 }), { state: confirmed, handlers: local(row) });
    expect(hide.state.peerWrites.get('thing:1')).toMatchObject({ kind: 'delete' });
  });

  it('peerWriteExpired: undoes an unconfirmed create, keeps a confirmed or own one, forgets a delete', async () => {
    expect((await runPure(peerWriteExpired('thing:1'))).log.filter((e) => e.kind !== 'state.read')).toEqual([]);
    const created = R.setPeerWrite('thing:1', { kind: 'create', table: 'thing', expiresAt: 1 })(withQuery());
    const undone = await runPure(peerWriteExpired('thing:1'), {
      state: created,
      handlers: { ...local(null), 'local.execute': () => undefined },
    });
    expect(undone.state.peerWrites.size).toBe(0);
    expect(undone.log.some((e) => e.kind === 'ssp.ingest')).toBe(false);
    const failed = await runPure(peerWriteExpired('thing:1'), {
      state: created,
      handlers: { ...local(row), 'local.execute': () => { throw new Error('locked'); } },
    });
    expect(failed.emitted[0]).toMatchObject({ type: 'log', level: 'warn' });
    expect(failed.state.peerWrites.size).toBe(0);
    const confirmed = await runPure(peerWriteExpired('thing:1'), { state: { ...created, versions: new Map([['thing:1', 1]]) } });
    expect(confirmed.state.peerWrites.size).toBe(0);
    expect(confirmed.log.some((e) => e.kind === 'local.execute')).toBe(false);
    const own = await runPure(peerWriteExpired('thing:1'), {
      state: R.outboxReplace([buildOutboxItem({ id: 'm', type: 'update', recordId: 'thing:1' })])(created),
    });
    expect(own.state.peerWrites.size).toBe(0);
    const deleted = await runPure(peerWriteExpired('thing:1'), {
      state: R.setPeerWrite('thing:1', { kind: 'delete', table: 'thing', expiresAt: 1 })(withQuery()),
    });
    expect(deleted.state.peerWrites.size).toBe(0);
  });
});
