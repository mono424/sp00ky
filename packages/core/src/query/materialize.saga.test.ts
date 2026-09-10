import { describe, expect, it } from 'vitest';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { materialize, streamUpdate } from './materialize.saga';

describe('materialize', () => {
  const plan = { table: 'thing' } as any;
  it('no entry: nothing', async () => {
    const out = await runPure(materialize('zz'), { state: buildState() });
    expect(out.log.filter((e) => e.kind !== 'state.read')).toHaveLength(0);
  });
  it('cold: predicate scan through the plan; rows land, dirt clears, subscribers hear once', async () => {
    const s = R.markDirty(['a'])(buildState([buildEntry({ def: { hash: 'a', plan } })]));
    const out = await runPure(materialize('a'), { state: s, handlers: { 'local.select': (e: any) => (e.plan.ids ? [] : [{ id: 'thing:1' }]) } });
    const en = out.state.queries.get('a')!;
    expect(en.records).toEqual([{ id: 'thing:1' }]);
    expect(en.lifecycle.notified).toBe(true);
    expect(en.telemetry.updateCount).toBe(1);
    expect(en.telemetry.lastUpdatedAt).toBe(1_700_000_000_000);
    expect(out.state.dirty.has('a')).toBe(false);
    expect(out.emitted).toEqual([{ type: 'query:records', hash: 'a', records: [{ id: 'thing:1' }] }]);
    const again = await runPure(materialize('a'), { state: out.state, handlers: { 'local.select': () => [{ id: 'thing:1' }] } });
    expect(again.emitted).toEqual([]);
    expect(again.state.queries.get('a')!.telemetry.updateCount).toBe(1);
  });
  it('cold window renders the SSP local window; live renders membership with the overlay', async () => {
    const win = buildState([buildEntry({ def: { hash: 'w', plan: { ...plan, offset: 5 }, surql: 'SELECT * FROM thing LIMIT 5 START 5' }, localArray: [['thing:7', 1]] })]);
    const w = await runPure(materialize('w'), { state: win, handlers: { 'local.select': (e: any) => e.plan.ids.map((id: any) => ({ id: `${id.table}:${id.id}` })) } });
    expect(w.state.queries.get('w')!.records).toEqual([{ id: 'thing:7' }]);
    const live = buildState(
      [buildEntry({ def: { hash: 'a', plan }, lifecycle: { phase: 'live' }, remoteArray: [['thing:2', 1], ['thing:1', 1]], localArray: [['thing:9', 1]] })],
      R.outboxReplace([buildOutboxItem({ id: 'c', type: 'create', recordId: 'thing:9' }), buildOutboxItem({ id: 'd', type: 'delete', recordId: 'thing:2' })])
    );
    const l = await runPure(materialize('a'), { state: live, handlers: { 'local.select': (e: any) => e.plan.ids.map((id: any) => ({ id: `${id.table}:${id.id}` })) } });
    expect(l.state.queries.get('a')!.records).toEqual([{ id: 'thing:1' }, { id: 'thing:9' }]);
    const ordered = buildState([buildEntry({ def: { hash: 'o', plan: { ...plan, orderBy: [['a', 'asc']] } }, lifecycle: { phase: 'live' }, remoteArray: [['thing:2', 1], ['thing:1', 1]] })]);
    const o = await runPure(materialize('o'), { state: ordered, handlers: { 'local.select': (e: any) => e.plan.ids.map((id: any) => ({ id: `${id.table}:${id.id}` })) } });
    expect(o.state.queries.get('o')!.records).toEqual([{ id: 'thing:2' }, { id: 'thing:1' }]);
  });
  it('a failing read counts an error and clears dirt; an entry removed mid-read is left alone', async () => {
    const s = R.markDirty(['a'])(buildState([buildEntry({ def: { hash: 'a', plan } })]));
    const fail = await runPure(materialize('a'), {
      state: s,
      handlers: {
        'local.select': () => {
          throw new Error('locked');
        },
      },
    });
    expect(fail.state.queries.get('a')!.telemetry.errorCount).toBe(1);
    expect(fail.state.dirty.has('a')).toBe(false);
    expect(fail.emitted).toEqual([expect.objectContaining({ level: 'warn' })]);
    const gone = await runPure(materialize('a'), {
      state: s,
      handlers: {
        'local.select': (_e, ctx) => {
          ctx.state = R.removeQuery('a')(ctx.state);
          return [];
        },
      },
    });
    expect(gone.emitted).toEqual([]);
  });
});

describe('streamUpdate', () => {
  it('takes the local id-set (dirtying the query) and records ingest timings; unknown hashes are ignored', async () => {
    const s = buildState([buildEntry({ def: { hash: 'a' } })]);
    const out = await runPure(streamUpdate({ queryHash: 'a', localArray: [['thing:1', 1]], op: 'CREATE', materializationTimeMs: 4, storeApplyMs: 1, circuitStepMs: 2 }), { state: s });
    const en = out.state.queries.get('a')!;
    expect(en.localArray).toEqual([['thing:1', 1]]);
    expect(out.state.dirty.has('a')).toBe(true);
    expect(en.telemetry.lastIngestLatencyMs).toBe(4);
    expect(en.telemetry.materializationSamples).toEqual([4]);
    expect(en.telemetry.phaseLast).toEqual({ sspStoreApply: 1, sspCircuitStep: 2 });
    const bare = await runPure(streamUpdate({ queryHash: 'a', localArray: [] }), { state: s });
    expect(bare.state.queries.get('a')!.telemetry.lastIngestLatencyMs).toBeNull();
    const unknown = await runPure(streamUpdate({ queryHash: 'zz', localArray: [] }), { state: s });
    expect(unknown.log.filter((e) => e.kind === 'state.update')).toHaveLength(0);
  });
});
