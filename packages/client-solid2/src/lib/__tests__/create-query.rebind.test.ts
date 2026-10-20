import { describe, expect, it } from 'vitest';
import { createEffect, createRoot, flush } from 'solid-js';
import { createQuery } from '../create-query';
import { SyncedDb } from '../../index';
// The real engine underneath, not a mock: the window this guards is the order
// in which the core announces authority and rows, which a mock would script.
import { Runtime } from '../../../../core/src/client/runtime';
import { rebindQueries } from '../../../../core/src/boot/bucket-switch.saga';
import { defaultEnv } from '../../../../core/src/query/env';
import { fakeAdapters } from '../../../../core/src/testing/adapters';
import { buildEntry, buildState } from '../../../../core/src/testing/build';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));
const settle = async () => {
  await tick();
  flush();
  await tick();
};

const noop = () => {};
const logger: any = {
  debug: noop,
  info: noop,
  warn: noop,
  trace: noop,
  error: noop,
  child: () => logger,
};

/** A core Runtime whose `_00_view` holds one row for query `q`, behind a SyncedDb. */
function engine() {
  const a = fakeAdapters({
    local: {
      getById: async () => ({ ids: [['thing:1', 1]], confirmed: true }),
      select: async (plan: any) =>
        (plan.ids ?? []).map((id: any) => ({ id: `${id.table}:${id.id}`, name: 'mine' })),
    },
  });
  const rt = new Runtime({
    env: defaultEnv({ tables: [] } as any, { materializeDebounceMs: 5 }),
    adapters: a.adapters,
    logger,
    tabId: 'tab-a',
    // Registered while still in the anon bucket: cold, nothing painted.
    initialState: {
      ...buildState([buildEntry({ def: { hash: 'q', plan: { table: 'thing' } as any } })]),
      sessionId: 's',
    },
  });
  const sp00ky = {
    subscribe: async (
      hash: string,
      cb: (rows: Record<string, any>[]) => void,
      o?: { immediate?: boolean }
    ) => rt.subscribe(hash, cb, o),
    subscribeQueryAuthority: (
      hash: string,
      cb: (known: boolean) => void,
      o?: { immediate?: boolean }
    ) => rt.subscribeAuthority(hash, cb, o),
    subscribeQueryStatus: (hash: string, cb: (s: any) => void, o?: { immediate?: boolean }) =>
      rt.subscribeStatus(hash, cb, o),
    deregisterQuery: noop,
    reportFrontendTiming: noop,
  };
  const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
  (db as any).getSp00ky = () => sp00ky;
  return { a, rt, db };
}

describe('createQuery across a bucket switch (sign-in)', () => {
  it('never reads settled, authoritative or empty while the re-seeded rows are unpainted', async () => {
    const { a, rt, db } = engine();
    await createRoot(async (dispose) => {
      const q = createQuery<any, any, any, any, false, any>(db, {
        hash: 'q',
        isOne: false,
        run: async () => ({ hash: 'q' }),
      } as any);
      // Every state an effect could act on, e.g. "settled and no row: create one".
      const seen: string[] = [];
      createEffect(
        () => ({
          settled: q.isSettled(),
          known: q.isAuthoritative(),
          empty: q.isEmpty(),
          rows: q.data().length,
        }),
        (v) =>
          void seen.push(
            `${v.settled ? 'settled' : '-'} ${v.known ? 'known' : '-'} ${v.empty ? 'empty' : '-'} ${v.rows}`
          )
      );
      flush();
      await settle();
      expect(q.isLoading()).toBe(true);

      // The AuthFlip's switch: re-seeded as `cached` from the user's durable
      // view, `records` not painted, no fetch cycle in flight.
      await rt.run(rebindQueries());
      await settle();
      expect(rt.state.queries.get('q')!.lifecycle).toMatchObject({
        phase: 'cached',
        fetchDepth: 0,
      });
      expect(q.isFetching()).toBe(false);
      expect(q.data()).toEqual([]);
      expect(q.isAuthoritative()).toBe(false);
      expect(q.isSettled()).toBe(false);
      expect(q.isEmpty()).toBe(false);
      expect(q.isLoading()).toBe(true);

      // The debounced render paints the durable rows; authority follows them.
      a.timers.fire('mat:q');
      await settle();
      expect(q.data().map((r: any) => r.id)).toEqual(['thing:1']);
      expect(q.isAuthoritative()).toBe(true);
      expect(q.isSettled()).toBe(true);
      expect(q.isEmpty()).toBe(false);

      expect(seen.filter((s) => s.startsWith('settled') && s.endsWith(' 0'))).toEqual([]);
      expect(seen.filter((s) => s.includes('empty'))).toEqual([]);
      dispose();
    });
  });
});
