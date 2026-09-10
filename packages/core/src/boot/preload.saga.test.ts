import { describe, expect, it } from 'vitest';
import { runPure, sha256Hex } from '../testing/run-pure';
import { buildEntry, buildState } from '../testing/build';
import * as R from '../state/reducers';
import { defaultEnv } from '../query/env';
import { queryHashInput } from '../query/hash';
import { preload, PreloadFailedError } from './preload.saga';

const env = defaultEnv({ tables: [{ name: 'thing', columns: {} }] } as any);
const input = { tableName: 'thing', surql: 'SELECT * FROM thing', params: {}, ttl: '10m' as const };
const sspOk = () => ({ localArray: [], timings: { parseMs: 0, planMs: 0, snapshotMs: 0, wallMs: 0 } });

describe('preload', () => {
  it('resolved before: returns without waiting and with no remote effect', async () => {
    const out = await runPure(preload(env, input), {
      handlers: { 'local.getById': () => ({ ids: [['thing:1', 1]], confirmed: true }), 'ssp.register': sspOk },
    });
    expect(out.result.waited).toBe(false);
    expect(out.log.filter((e) => e.kind === 'remote.query')).toHaveLength(0);
    expect(out.log.filter((e) => e.kind === 'state.wait')).toHaveLength(0);
  });
  it('never resolved: waits for settled (membership + bodies + first render), or fails when the registration failed', async () => {
    const hash = await sha256Hex(queryHashInput(input, null));
    const settleIt = (ctx: any) => {
      ctx.state = R.compose(
        R.commitMembership(hash, [['thing:1', 1]], true),
        R.setVersions([['thing:1', 1]]),
        R.setRecords(hash, [{ id: 'thing:1' }], true, 1)
      )(ctx.state);
    };
    const out = await runPure(preload(env, input), {
      handlers: { 'local.getById': () => null, 'ssp.register': sspOk, 'state.wait': (_e, ctx) => settleIt(ctx) },
    });
    expect(out.result).toEqual({ hash, waited: true });
    const failed = runPure(preload(env, input), {
      handlers: {
        'local.getById': () => null,
        'ssp.register': sspOk,
        'state.wait': (_e, ctx) => void (ctx.state = R.applyLifecycle(hash, { type: 'remote-failed' })(ctx.state)),
      },
    });
    await expect(failed).rejects.toBeInstanceOf(PreloadFailedError);
    const gone = runPure(preload(env, input), {
      handlers: { 'local.getById': () => null, 'ssp.register': sspOk, 'state.wait': (_e, ctx) => void (ctx.state = R.removeQuery(hash)(ctx.state)) },
    });
    await expect(gone).rejects.toThrow(/could not settle/);
    const already = buildState([buildEntry({ def: { hash }, lifecycle: { phase: 'cached' } })]);
    const cached = await runPure(preload(env, input), { state: already });
    expect(cached.result.waited).toBe(false);
  });
});
