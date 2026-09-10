import { describe, expect, it } from 'vitest';
import { RecordId } from 'surrealdb';
import { runPure } from '../testing/run-pure';
import { buildEntry, buildOutboxItem, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from './env';
import { fetchRows } from './fetch.saga';
import type { StatementResult } from '../kernel/effects';

const env = defaultEnv({ tables: [{ name: 'thing', columns: { a: {} } }] } as any);
const ok = (result: unknown): StatementResult => ({ status: 'OK', result });
const live = { phase: 'live' } as const;
const primed = (s: ReturnType<typeof buildState>) => ({ ...s, primed: true });

describe('fetchRows', () => {
  it('waits for the prime and returns on an empty plan (resetting the attempt counter)', async () => {
    const s = R.patchSync({ fetchAttempts: 3 })(buildState());
    const out = await runPure(fetchRows(env), { state: s, handlers: { 'state.wait': (_e, ctx) => void (ctx.state = { ...ctx.state, primed: true }) } });
    expect(out.state.sync.fetchAttempts).toBe(0);
    expect(out.log.filter((e) => e.kind === 'remote.query')).toHaveLength(0);
  });

  it('fetches the deduped set once, writes + ingests bodies, records versions, balances fetch depth', async () => {
    const s = primed(
      buildState(
        [
          buildEntry({ def: { hash: 'a' }, lifecycle: live, remoteArray: [['thing:1', 2], ['thing:2', 1]] }),
          buildEntry({ def: { hash: 'b' }, lifecycle: live, remoteArray: [['thing:2', 1], ['thing:3', 1]] }),
        ],
        R.setVersions([['thing:3', 1], ['thing:1', 1]]),
        R.outboxReplace([buildOutboxItem({ type: 'delete', recordId: 'thing:2' })])
      )
    );
    const depths: number[] = [];
    const out = await runPure(fetchRows(env), {
      state: s,
      epoch: 7,
      handlers: {
        'remote.query': (e: any, ctx) => {
          depths.push(ctx.state.queries.get('a')!.lifecycle.fetchDepth);
          expect(e.sql).toBe('SELECT * FROM $ids');
          expect(e.vars.ids).toEqual([new RecordId('thing', '1')]);
          return [ok([{ id: new RecordId('thing', '1'), a: 'x', junk: true }])];
        },
        'local.execute': () => undefined,
        'ssp.ingest': () => undefined,
      },
    });
    expect(depths).toEqual([1]);
    expect(out.state.queries.get('a')!.lifecycle.fetchDepth).toBe(0);
    expect(out.state.versions.get('thing:1')).toBe(2);
    const exec = out.log.find((e) => e.kind === 'local.execute') as any;
    expect(exec.vars.content0).toEqual({ a: 'x', _00_rv: 2 });
    expect(exec.epoch).toBe(7);
    expect(out.log.findIndex((e) => e.kind === 'local.epoch')).toBeLessThan(out.log.findIndex((e) => e.kind === 'all'));
    const ingest = out.log.find((e) => e.kind === 'ssp.ingest') as any;
    expect(ingest.records).toEqual([{ table: 'thing', op: 'UPDATE', id: 'thing:1', record: { id: new RecordId('thing', '1'), a: 'x', _00_rv: 2 } }]);
    expect(out.dispatched).toEqual([{ type: 'SyncOutcome', ok: true, error: undefined }]);
    expect(out.state.dirty.has('a')).toBe(true);
  });

  it('a body the server does not return is remembered at the requested version so the plan converges', async () => {
    const s = primed(buildState([buildEntry({ def: { hash: 'a' }, lifecycle: live, remoteArray: [['thing:1', 1], ['other:2', 1]] })]));
    let calls = 0;
    const out = await runPure(fetchRows(env), {
      state: s,
      handlers: {
        'remote.query': () => {
          calls++;
          return [ok([{ id: new RecordId('other', '2'), z: 1 }, { nope: true }])];
        },
        'local.execute': () => undefined,
        'ssp.ingest': () => undefined,
      },
    });
    expect(calls).toBe(1);
    expect(out.state.versions.get('thing:1')).toBe(1);
    expect(out.state.versions.get('other:2')).toBe(1);
    const extra = await runPure(fetchRows(env), {
      state: s,
      handlers: {
        'remote.query': () => [ok([{ id: new RecordId('thing', '1') }, { id: new RecordId('thing', 'unrequested') }])],
        'local.execute': () => undefined,
        'ssp.ingest': (e: any) => {
          expect(e.records[1]).toMatchObject({ id: 'thing:unrequested', record: { _00_rv: 0 } });
        },
      },
    });
    expect(extra.state.versions.has('thing:unrequested')).toBe(false);
    const ingest = out.log.find((e) => e.kind === 'ssp.ingest') as any;
    expect(ingest.records[0]).toMatchObject({ table: 'other', op: 'CREATE', record: { z: 1, _00_rv: 1 } });
  });

  it('failures back off: transport error, ERR statement, non-array result, local write error; ingest errors are logged only', async () => {
    const s = primed(buildState([buildEntry({ def: { hash: 'a' }, lifecycle: live, remoteArray: [['thing:1', 1]] })]));
    const expectBackoff = (out: Awaited<ReturnType<typeof runPure>>, attempts: number) => {
      expect(out.state.sync.fetchAttempts).toBe(attempts);
      expect(out.timers.get('fetch')).toEqual({ ms: 500 * 2 ** (attempts - 1), event: { type: 'FetchRows' } });
      expect(out.state.queries.get('a')!.lifecycle.fetchDepth).toBe(0);
      expect(out.dispatched.at(-1)).toMatchObject({ type: 'SyncOutcome', ok: false });
    };
    expectBackoff(await runPure(fetchRows(env), { state: s, handlers: { 'remote.query': () => { throw new Error('offline'); } } }), 1);
    expectBackoff(await runPure(fetchRows(env), { state: s, handlers: { 'remote.query': () => [{ status: 'ERR', error: 'x' }] } }), 1);
    expectBackoff(await runPure(fetchRows(env), { state: s, handlers: { 'remote.query': () => [ok('nope')] } }), 1);
    expectBackoff(
      await runPure(fetchRows(env), {
        state: R.patchSync({ fetchAttempts: 1 })(s),
        handlers: {
          'remote.query': () => [ok([{ id: new RecordId('thing', '1') }])],
          'local.execute': () => {
            throw new Error('stale epoch');
          },
        },
      }),
      2
    );
    const ingestFail = await runPure(fetchRows(env), {
      state: s,
      handlers: {
        'remote.query': () => [ok([{ id: new RecordId('thing', '1') }])],
        'local.execute': () => undefined,
        'ssp.ingest': () => {
          throw new Error('wasm');
        },
      },
    });
    expect(ingestFail.state.sync.fetchAttempts).toBe(0);
    expect(ingestFail.emitted).toContainEqual(expect.objectContaining({ message: 'circuit ingest failed' }));
  });

  it('a leader relays landed bodies to followers', async () => {
    const s = { ...primed(buildState([buildEntry({ def: { hash: 'a' }, lifecycle: live, remoteArray: [['thing:1', 1]] })])), tabRole: 'leader' as const };
    const out = await runPure(fetchRows(env), {
      state: s,
      handlers: { 'remote.query': () => [ok([{ id: new RecordId('thing', '1') }])], 'local.execute': () => undefined, 'ssp.ingest': () => undefined },
    });
    expect(out.emitted).toContainEqual({ type: 'tabs:broadcast', message: { type: 'ingest', records: [expect.objectContaining({ id: 'thing:1' })] } });
  });

  it('loops until the plan is empty', async () => {
    const s = primed(buildState([buildEntry({ def: { hash: 'a' }, lifecycle: live, remoteArray: [['thing:1', 1]] })]));
    let calls = 0;
    const out = await runPure(fetchRows(env), {
      state: s,
      handlers: {
        'remote.query': (_e, ctx) => {
          calls++;
          if (calls === 1) ctx.state = R.commitMembership('a', [['thing:1', 1], ['thing:2', 1]], true)(ctx.state);
          return [ok([])];
        },
      },
    });
    expect(calls).toBe(2);
    expect(out.state.versions.get('thing:2')).toBe(1);
  });
});
