import { Duration } from 'surrealdb';
import type { LocalStore } from '../services/database/cache-engine';
import type { IngestRecord, StreamUpdate } from '../services/stream-processor/index';
import type { ClientState } from '../state/client-state';
import { withTimeout } from '../utils/index';
import type { Effect, RegisterResult, ServiceCalls, Settled, StatementResult } from './effects';
import type { OutEvent, RuntimeEvent } from './events';

/** The five adapters plus the pure ports, as the interpreter sees them. */
export interface Adapters {
  local: Pick<LocalStore, 'query' | 'execute' | 'select' | 'getById' | 'upsert' | 'delete'> & { readonly epoch: number };
  remote: {
    queryResponses(sql: string, vars?: Record<string, unknown>): Promise<StatementResult[]>;
    /** Subscribe to `LIVE SELECT * FROM <table>`; the callback receives the changed query hashes. */
    live(table: string, onChange: (hashes: string[]) => void): Promise<string>;
    kill(uuid: string): Promise<void>;
  };
  ssp: {
    registerQueryPlan(plan: {
      queryHash: string;
      surql: string;
      params: Record<string, unknown>;
      ttl: Duration;
      lastActiveAt: Date;
      localArray: [];
      remoteArray: [];
      meta: { tableName: string; involvedTables?: string[] };
    }): StreamUpdate | undefined;
    unregisterQueryPlan(hash: string): void;
    ingestMany(records: IngestRecord[]): unknown;
  };
  timers: { set(key: string, ms: number, fire: () => void): void; clear(key: string): void };
  clock: { now(): number };
  ids: { mutation(): string; salt(): string };
  hash(input: string): Promise<string>;
  services: ServiceCalls;
}

/** What the interpreter needs from the runtime: state, waiting, emitting, dispatching. */
export interface InterpreterHost {
  getState(): ClientState;
  setState(next: ClientState): void;
  waitFor(until: (s: ClientState) => boolean, signal?: AbortSignal): Promise<void>;
  emit(event: OutEvent): void;
  dispatch(event: RuntimeEvent): void;
}

export interface RunContext {
  signal?: AbortSignal;
}

export type Interpreter = (effect: Effect, ctx?: RunContext) => Promise<unknown>;

/** Effects become adapter calls here and nowhere else. */
export function createInterpreter(adapters: Adapters, host: InterpreterHost): Interpreter {
  const interpret: Interpreter = async (effect, ctx) => {
    switch (effect.kind) {
      case 'local.query':
        return adapters.local.query(effect.sql, effect.vars, effect.epoch === undefined ? undefined : { epoch: effect.epoch });
      case 'local.select':
        return adapters.local.select(effect.plan, effect.params);
      case 'local.execute':
        return adapters.local.execute(effect.query, effect.vars, effect.epoch === undefined ? undefined : { epoch: effect.epoch });
      case 'local.getById':
        return adapters.local.getById(effect.table, effect.id);
      case 'local.upsert':
        return adapters.local.upsert(effect.table, effect.id, effect.data, effect.mode);
      case 'local.delete':
        return adapters.local.delete(effect.table, effect.id);
      case 'local.epoch':
        return adapters.local.epoch;
      case 'remote.query': {
        const pending = adapters.remote.queryResponses(effect.sql, effect.vars);
        return effect.timeoutMs
          ? withTimeout(pending, effect.timeoutMs, () => new Error(`Remote request timed out after ${effect.timeoutMs}ms`))
          : pending;
      }
      case 'remote.live':
        return adapters.remote.live(effect.table, (hashes) => host.dispatch({ type: 'LiveChange', hashes }));
      case 'remote.kill':
        return adapters.remote.kill(effect.uuid);
      case 'ssp.register': {
        const update = adapters.ssp.registerQueryPlan({
          queryHash: effect.plan.queryHash,
          surql: effect.plan.surql,
          params: effect.plan.params,
          ttl: new Duration(effect.plan.ttl),
          lastActiveAt: new Date(adapters.clock.now()),
          localArray: [],
          remoteArray: [],
          meta: { tableName: effect.plan.tableName, involvedTables: effect.plan.involvedTables },
        });
        if (!update) throw new Error('Stream processor is not initialized');
        const result: RegisterResult = {
          localArray: update.localArray,
          timings: {
            parseMs: update.registration?.parseMs ?? null,
            planMs: update.registration?.planMs ?? null,
            snapshotMs: update.registration?.snapshotMs ?? null,
            wallMs: null,
          },
        };
        return result;
      }
      case 'ssp.unregister':
        return adapters.ssp.unregisterQueryPlan(effect.hash);
      case 'ssp.ingest':
        return adapters.ssp.ingestMany(effect.records);
      case 'timer.set':
        return adapters.timers.set(effect.key, effect.ms, () => host.dispatch(effect.event));
      case 'timer.clear':
        return adapters.timers.clear(effect.key);
      case 'state.read':
        return effect.select(host.getState());
      case 'state.update': {
        const next = effect.fn(host.getState());
        host.setState(next);
        return next;
      }
      case 'state.wait':
        return host.waitFor(effect.until, ctx?.signal);
      case 'now':
        return adapters.clock.now();
      case 'id':
        return effect.scope === 'mutation' ? adapters.ids.mutation() : adapters.ids.salt();
      case 'hash':
        return adapters.hash(effect.input);
      case 'emit':
        return host.emit(effect.event);
      case 'dispatch':
        return host.dispatch(effect.event);
      case 'all': {
        const settled = await Promise.allSettled(effect.effects.map((e) => interpret(e, ctx)));
        return settled.map((r): Settled => (r.status === 'fulfilled' ? { ok: true, value: r.value } : { ok: false, error: r.reason }));
      }
      case 'service': {
        const fn = adapters.services[effect.name] as (...args: unknown[]) => unknown;
        return fn(...effect.args);
      }
    }
  };
  return interpret;
}
