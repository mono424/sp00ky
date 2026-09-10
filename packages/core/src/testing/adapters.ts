import type { Adapters, InterpreterHost } from '../kernel/interpreter';
import type { ServiceCalls } from '../kernel/effects';
import type { OutEvent, RuntimeEvent } from '../kernel/events';
import type { ClientState } from '../state/client-state';
import { emptyState } from '../state/client-state';
import { sha256Hex } from './run-pure';

export interface FakeTimers {
  pending: Map<string, { ms: number; fire: () => void }>;
  fire(key: string): void;
  fireAll(): void;
}

/**
 * Recording adapters for interpreter / runtime / facade tests. Every call is
 * logged; behaviour is scripted by overriding the methods you care about.
 */
export function fakeAdapters(over: {
  local?: Partial<Adapters['local']>;
  remote?: Partial<Adapters['remote']>;
  ssp?: Partial<Adapters['ssp']>;
  services?: Partial<ServiceCalls>;
  now?: number;
} = {}) {
  const calls: Array<[string, unknown[]]> = [];
  const log = (name: string, ...args: unknown[]) => calls.push([name, args]);
  let now = over.now ?? 1_700_000_000_000;
  let epoch = 0;
  const timers: FakeTimers = {
    pending: new Map(),
    fire(key) {
      const t = timers.pending.get(key);
      if (!t) return;
      timers.pending.delete(key);
      t.fire();
    },
    fireAll() {
      for (const key of [...timers.pending.keys()]) timers.fire(key);
    },
  };
  const services = new Proxy({} as ServiceCalls, {
    get(_t, name: string) {
      return (...args: unknown[]) => {
        log(`service.${name}`, ...args);
        const fn = (over.services as Record<string, (...a: unknown[]) => unknown> | undefined)?.[name];
        return fn ? fn(...args) : undefined;
      };
    },
  });
  let ids = 0;
  const adapters: Adapters = {
    local: {
      get epoch() {
        return epoch;
      },
      query: (async (sql: string, vars?: Record<string, unknown>) => (log('local.query', sql, vars), [])) as Adapters['local']['query'],
      execute: (async (q: { sql: string }, vars?: Record<string, unknown>) => (log('local.execute', q.sql, vars), undefined)) as Adapters['local']['execute'],
      select: async (plan, params) => (log('local.select', plan, params), []),
      getById: async (table, id) => (log('local.getById', table, id), null),
      upsert: async (table, id, data, mode) => void log('local.upsert', table, id, data, mode),
      delete: async (table, id) => void log('local.delete', table, id),
      ...over.local,
    },
    remote: {
      queryResponses: async (sql, vars) => (log('remote.query', sql, vars), []),
      live: async (table) => (log('remote.live', table), 'live-uuid'),
      kill: async (uuid) => void log('remote.kill', uuid),
      ...over.remote,
    },
    ssp: {
      registerQueryPlan: (plan) => (log('ssp.register', plan.queryHash), { queryHash: plan.queryHash, localArray: [] }),
      unregisterQueryPlan: (hash) => void log('ssp.unregister', hash),
      ingestMany: (records) => (log('ssp.ingest', records), records),
      ...over.ssp,
    },
    timers: {
      set: (key, ms, fire) => void timers.pending.set(key, { ms, fire }),
      clear: (key) => void timers.pending.delete(key),
    },
    clock: { now: () => now },
    ids: { mutation: () => `_00_pending_mutations:${String(++ids).padStart(13, '0')}_0001_tab`, salt: () => `salt-${++ids}` },
    hash: sha256Hex,
    services,
  };
  return {
    adapters,
    calls,
    timers,
    names: () => calls.map(([n]) => n),
    advance(ms: number) {
      now += ms;
    },
    bumpEpoch() {
      epoch += 1;
    },
  };
}

/** A minimal host: holds state, resolves waits when the predicate holds after an update. */
export function fakeHost(initial: ClientState = emptyState({ tabId: 'tab-a' })) {
  let state = initial;
  const emitted: OutEvent[] = [];
  const dispatched: RuntimeEvent[] = [];
  const waiters = new Set<{ until: (s: ClientState) => boolean; resolve: () => void; reject: (e: unknown) => void }>();
  const check = () => {
    for (const w of [...waiters]) {
      if (w.until(state)) {
        waiters.delete(w);
        w.resolve();
      }
    }
  };
  const host: InterpreterHost = {
    getState: () => state,
    setState: (next) => {
      state = next;
      check();
    },
    waitFor: (until, signal) =>
      new Promise<void>((resolve, reject) => {
        if (until(state)) return resolve();
        const w = { until, resolve, reject };
        waiters.add(w);
        signal?.addEventListener('abort', () => {
          waiters.delete(w);
          reject(new Error('aborted'));
        });
      }),
    emit: (e) => void emitted.push(e),
    dispatch: (e) => void dispatched.push(e),
  };
  return { host, emitted, dispatched, get state() { return state; }, waiters };
}
