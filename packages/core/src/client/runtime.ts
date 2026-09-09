import type { Logger } from '../services/logger/index';
import type { QueryAuthorityCallback, QueryHash, QueryStatus, QueryStatusCallback, QueryUpdateCallback } from '../types';
import type { OutEvent, RuntimeEvent } from '../kernel/events';
import type { Adapters, Interpreter } from '../kernel/interpreter';
import { createInterpreter } from '../kernel/interpreter';
import type { Lane, Saga } from '../kernel/saga';
import { runSaga } from '../kernel/saga';
import type { ClientState } from '../state/client-state';
import { emptyState } from '../state/client-state';
import { deriveStatus, isAuthoritative } from '../state/lifecycle';
import * as R from '../state/reducers';
import { fetchingQueryCount, pendingMutationCount } from '../state/selectors';
import type { SagaEnv } from '../query/env';
import { route } from './router';

interface Waiter {
  until: (s: ClientState) => boolean;
  resolve: () => void;
  reject: (e: unknown) => void;
}

export interface RuntimeOptions {
  env: SagaEnv;
  adapters: Adapters;
  logger: Logger;
  tabId: string;
  initialState?: ClientState;
}

/**
 * The one effectful object of the engine: holds the state, runs sagas on
 * lanes, fires timers, fans events out to subscribers and schedules
 * materialization for dirty queries. No decision logic lives here.
 */
export class Runtime {
  private stateValue: ClientState;
  private readonly interpret: Interpreter;
  private readonly env: SagaEnv;
  private readonly adapters: Adapters;
  private readonly logger: Logger;
  private readonly serialLanes = new Map<string, Promise<unknown>>();
  private readonly dedupeLanes = new Map<string, Promise<unknown>>();
  private readonly waiters = new Set<Waiter>();
  private readonly recordSubs = new Map<QueryHash, Set<QueryUpdateCallback>>();
  private readonly statusSubs = new Map<QueryHash, Set<QueryStatusCallback>>();
  private readonly authoritySubs = new Map<QueryHash, Set<QueryAuthorityCallback>>();
  private readonly lastStatus = new Map<QueryHash, QueryStatus>();
  private readonly listeners = new Map<string, Set<(event: OutEvent) => void>>();
  private readonly scheduledMaterialize = new Set<QueryHash>();
  private readonly timerKeys = new Set<string>();
  private lastActivity = { fetching: 0, pending: 0 };
  private lastHealth: ClientState['sync']['health'];
  private disposed = false;

  constructor(opts: RuntimeOptions) {
    this.env = opts.env;
    this.logger = opts.logger;
    this.stateValue = opts.initialState ?? emptyState({ tabId: opts.tabId });
    this.lastHealth = this.stateValue.sync.health;
    const timers = opts.adapters.timers;
    this.adapters = {
      ...opts.adapters,
      timers: {
        set: (key, ms, fire) => {
          this.timerKeys.add(key);
          timers.set(key, ms, () => {
            this.timerKeys.delete(key);
            if (!this.disposed) fire();
          });
        },
        clear: (key) => {
          this.timerKeys.delete(key);
          timers.clear(key);
        },
      },
    };
    this.interpret = createInterpreter(this.adapters, {
      getState: () => this.stateValue,
      setState: (next) => this.setState(next),
      waitFor: (until, signal) => this.waitFor(until, signal),
      emit: (event) => this.emit(event),
      dispatch: (event) => void this.dispatch(event),
    });
  }

  get state(): ClientState {
    return this.stateValue;
  }

  /** Run a saga, optionally on a lane. Rejections propagate to the caller. */
  run<R>(saga: Saga<R>, opts: { lane?: Lane; signal?: AbortSignal } = {}): Promise<R> {
    const exec = () => runSaga(saga, (effect) => this.interpret(effect, { signal: opts.signal }));
    const lane = opts.lane;
    if (!lane) return exec();
    if (lane.kind === 'dedupe') {
      const running = this.dedupeLanes.get(lane.key);
      if (running) return running as Promise<R>;
      const p = exec().finally(() => {
        if (this.dedupeLanes.get(lane.key) === p) this.dedupeLanes.delete(lane.key);
      });
      this.dedupeLanes.set(lane.key, p);
      return p;
    }
    const prev = this.serialLanes.get(lane.key) ?? Promise.resolve();
    const p = prev.then(exec, exec);
    const tail = p.catch(() => undefined);
    this.serialLanes.set(lane.key, tail);
    void tail.then(() => {
      if (this.serialLanes.get(lane.key) === tail) this.serialLanes.delete(lane.key);
    });
    return p;
  }

  /** Route an event to its saga. Never rejects: failures are logged. */
  dispatch(event: RuntimeEvent): Promise<void> {
    if (this.disposed) return Promise.resolve();
    const target = route(this.env, event);
    return this.run(target.saga, { lane: target.lane }).then(
      () => undefined,
      (error) => {
        this.logger.error({ error, event: event.type, Category: 'sp00ky-client::Runtime::dispatch' }, 'saga failed');
      }
    );
  }

  private setState(next: ClientState): void {
    const prev = this.stateValue;
    if (next === prev) return;
    this.stateValue = next;
    for (const w of [...this.waiters]) {
      let ok = false;
      try {
        ok = w.until(next);
      } catch (error) {
        this.waiters.delete(w);
        w.reject(error);
        continue;
      }
      if (ok) {
        this.waiters.delete(w);
        w.resolve();
      }
    }
    for (const hash of next.dirty) {
      if (this.scheduledMaterialize.has(hash)) continue;
      this.scheduledMaterialize.add(hash);
      this.adapters.timers.set(`mat:${hash}`, this.env.materializeDebounceMs, () => {
        this.scheduledMaterialize.delete(hash);
        void this.dispatch({ type: 'Materialize', hash });
      });
    }
    for (const [hash, subs] of this.statusSubs) {
      const entry = next.queries.get(hash);
      if (!entry) continue;
      const status = deriveStatus(entry.lifecycle);
      if (this.lastStatus.get(hash) === status) continue;
      this.lastStatus.set(hash, status);
      for (const cb of subs) this.safely(() => cb(status));
      this.notify({ type: 'query:status', hash, status });
    }
    const activity = { fetching: fetchingQueryCount(next), pending: pendingMutationCount(next) };
    if (activity.fetching !== this.lastActivity.fetching || activity.pending !== this.lastActivity.pending) {
      this.lastActivity = activity;
      this.notify({ type: 'activity:changed', ...activity });
    }
    // Health is notified from here, not from the saga that folds a sync round:
    // `connection` moves through `setConnection` on a transport event with no
    // round attached, and a subscriber that only heard the round would show a
    // stale socket state forever. `setHealth` keeps the identity stable while
    // the values are unchanged, so this fires exactly on real transitions.
    if (next.sync.health !== this.lastHealth) {
      this.lastHealth = next.sync.health;
      this.notify({ type: 'health:changed', health: next.sync.health });
    }
  }

  waitFor(until: (s: ClientState) => boolean, signal?: AbortSignal): Promise<void> {
    try {
      if (until(this.stateValue)) return Promise.resolve();
    } catch (error) {
      return Promise.reject(error);
    }
    if (signal?.aborted) return Promise.reject(new Error('aborted'));
    return new Promise<void>((resolve, reject) => {
      const waiter: Waiter = { until, resolve, reject };
      this.waiters.add(waiter);
      signal?.addEventListener(
        'abort',
        () => {
          if (this.waiters.delete(waiter)) reject(new Error('aborted'));
        },
        { once: true }
      );
    });
  }

  /** Deliver an outbound event to its subscribers and listeners. */
  emit(event: OutEvent): void {
    switch (event.type) {
      case 'query:records':
        for (const cb of this.recordSubs.get(event.hash) ?? []) this.safely(() => cb(event.records as Record<string, any>[]));
        break;
      case 'query:authority':
        for (const cb of this.authoritySubs.get(event.hash) ?? []) this.safely(() => cb(event.known));
        break;
      case 'log':
        this.logger[event.level](Object.assign({}, event.data as object | undefined, { Category: 'sp00ky-client::saga' }), event.message);
        break;
      default:
        break;
    }
    this.notify(event);
  }

  private notify(event: OutEvent): void {
    for (const cb of this.listeners.get(event.type) ?? []) this.safely(() => cb(event));
    for (const cb of this.listeners.get('*') ?? []) this.safely(() => cb(event));
  }

  private safely(fn: () => void): void {
    try {
      fn();
    } catch (error) {
      this.logger.error({ error, Category: 'sp00ky-client::Runtime::subscriber' }, 'subscriber threw');
    }
  }

  /** Observe outbound events by type (`'*'` for all). */
  on(type: OutEvent['type'] | '*', cb: (event: OutEvent) => void): () => void {
    const set = this.listeners.get(type) ?? new Set();
    set.add(cb);
    this.listeners.set(type, set);
    return () => void set.delete(cb);
  }

  subscribe(hash: QueryHash, cb: QueryUpdateCallback, options: { immediate?: boolean } = {}): () => void {
    const set = this.recordSubs.get(hash) ?? new Set();
    set.add(cb);
    this.recordSubs.set(hash, set);
    this.setState(R.subscribe(hash)(this.stateValue));
    if (options.immediate) {
      const entry = this.stateValue.queries.get(hash);
      if (entry) this.safely(() => cb(entry.records as Record<string, any>[]));
    }
    return () => {
      if (!set.delete(cb)) return;
      if (set.size === 0) this.recordSubs.delete(hash);
      this.setState(R.unsubscribe(hash, this.adapters.clock.now())(this.stateValue));
    };
  }

  subscribeStatus(hash: QueryHash, cb: QueryStatusCallback, options: { immediate?: boolean } = {}): () => void {
    const set = this.statusSubs.get(hash) ?? new Set();
    set.add(cb);
    this.statusSubs.set(hash, set);
    const entry = this.stateValue.queries.get(hash);
    if (entry) {
      const status = deriveStatus(entry.lifecycle);
      this.lastStatus.set(hash, status);
      if (options.immediate) this.safely(() => cb(status));
    }
    return () => {
      set.delete(cb);
      if (set.size === 0) {
        this.statusSubs.delete(hash);
        this.lastStatus.delete(hash);
      }
    };
  }

  subscribeAuthority(hash: QueryHash, cb: QueryAuthorityCallback, options: { immediate?: boolean } = {}): () => void {
    const set = this.authoritySubs.get(hash) ?? new Set();
    set.add(cb);
    this.authoritySubs.set(hash, set);
    if (options.immediate) {
      const entry = this.stateValue.queries.get(hash);
      if (entry) this.safely(() => cb(isAuthoritative(entry.lifecycle)));
    }
    return () => {
      set.delete(cb);
      if (set.size === 0) this.authoritySubs.delete(hash);
    };
  }

  /** Apply a reducer outside a saga (facade conveniences such as timing reports). */
  update(reducer: (s: ClientState) => ClientState): void {
    this.setState(reducer(this.stateValue));
  }

  dispose(): void {
    this.disposed = true;
    for (const key of [...this.timerKeys]) this.adapters.timers.clear(key);
    for (const w of this.waiters) w.reject(new Error('runtime disposed'));
    this.waiters.clear();
  }
}
