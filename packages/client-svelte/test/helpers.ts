import { vi } from 'vitest';
import { SyncedDb } from '../src/db.js';

export const tick = () => new Promise<void>((r) => setTimeout(r, 0));

type Emission = Record<string, any>[];
type Immediate = { immediate?: boolean };

/** A SyncedDb whose core is a fake engine the test drives by hand. */
export function mockEngine() {
  const subs = new Map<string, (e: Emission) => void>();
  const statusSubs = new Map<string, (s: string) => void>();
  const authoritySubs = new Map<string, (known: boolean) => void>();
  const unsubscribed: string[] = [];
  const deregistered: string[] = [];
  const preloaded: string[] = [];
  const sp00ky = {
    subscribeQueryAuthority: vi.fn<
      (hash: string, cb: (known: boolean) => void, o?: Immediate) => () => void
    >((hash, cb, o) => {
      authoritySubs.set(hash, cb);
      if (o?.immediate) cb(false);
      return () => authoritySubs.delete(hash);
    }),
    subscribe: vi.fn<
      (hash: string, cb: (e: Emission) => void, o?: Immediate) => Promise<() => void>
    >(async (hash, cb) => {
      subs.set(hash, cb);
      return () => {
        subs.delete(hash);
        unsubscribed.push(hash);
      };
    }),
    subscribeQueryStatus: vi.fn<
      (hash: string, cb: (s: string) => void, o?: Immediate) => () => void
    >((hash, cb, o) => {
      statusSubs.set(hash, cb);
      if (o?.immediate) cb('idle');
      return () => statusSubs.delete(hash);
    }),
    deregisterQuery: vi.fn<(hash: string) => void>((hash) => {
      deregistered.push(hash);
    }),
    reportFrontendTiming: vi.fn<(hash: string, ms: number) => void>(),
    preload: vi.fn<(q: { hash: string }) => Promise<void>>(async (q) => {
      preloaded.push(q.hash);
    }),
    create: vi.fn<(id: string, payload: Record<string, unknown>) => Promise<void>>(async () => {}),
  };
  const db = Object.create(SyncedDb.prototype) as SyncedDb<any>;
  (db as any).sp00ky = sp00ky;
  return {
    db,
    sp00ky,
    emit: (hash: string, e: Emission) => subs.get(hash)?.(e),
    setStatus: (hash: string, s: string) => statusSubs.get(hash)?.(s),
    setAuthority: (hash: string, known: boolean) => authoritySubs.get(hash)?.(known),
    unsubscribed,
    deregistered,
    preloaded,
    hasSub: (hash: string) => subs.has(hash),
  };
}

export function mockQuery(hash: string, isOne = false, fail?: Error) {
  return {
    hash,
    isOne,
    run: async () => {
      if (fail) throw fail;
      return { hash };
    },
  } as any;
}
