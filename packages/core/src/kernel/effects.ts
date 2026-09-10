import type { QueryPlan } from '@spooky-sync/query-builder';
import type { SealedQuery } from '../utils/surql';
import type { ClientState } from '../state/client-state';
import type { IngestRecord } from '../services/stream-processor/index';
import type { RecordVersionArray, RegistrationTimings } from '../types';
import type { OutEvent, RuntimeEvent } from './events';

export type Vars = Record<string, unknown>;

/** What the in-browser SSP needs to build a local view. */
export interface RegisterPlan {
  queryHash: string;
  surql: string;
  params: Vars;
  ttl: string;
  tableName: string;
  involvedTables?: string[];
}
export interface RegisterResult {
  localArray: RecordVersionArray;
  timings: RegistrationTimings;
}

/** One statement's outcome of a multi-statement remote request. */
export type StatementResult =
  | { status: 'OK'; result: unknown }
  | { status: 'ERR'; error: string };

/** One entry of an `all` fan-out result (allSettled semantics). */
export type Settled<T = unknown> = { ok: true; value: T } | { ok: false; error: unknown };

/**
 * Effects are data. A saga yields one and gets its result back on the next
 * `next()`; the interpreter (client/runtime) is the only place they execute.
 */
export type Effect =
  | { kind: 'local.query'; sql: string; vars?: Vars; epoch?: number }
  | { kind: 'local.select'; plan: QueryPlan; params?: Vars }
  | { kind: 'local.execute'; query: SealedQuery<unknown>; vars?: Vars; epoch?: number }
  | { kind: 'local.getById'; table: string; id: unknown }
  | { kind: 'local.upsert'; table: string; id: unknown; data: Record<string, unknown>; mode: 'replace' | 'merge' }
  | { kind: 'local.delete'; table: string; id: unknown }
  | { kind: 'local.epoch' }
  | { kind: 'remote.query'; sql: string; vars?: Vars; timeoutMs?: number }
  | { kind: 'remote.live'; table: string }
  | { kind: 'remote.kill'; uuid: string }
  | { kind: 'ssp.register'; plan: RegisterPlan }
  | { kind: 'ssp.unregister'; hash: string }
  | { kind: 'ssp.ingest'; records: IngestRecord[] }
  | { kind: 'timer.set'; key: string; ms: number; event: RuntimeEvent }
  | { kind: 'timer.clear'; key: string }
  | { kind: 'state.read'; select: (s: ClientState) => unknown }
  | { kind: 'state.update'; fn: (s: ClientState) => ClientState }
  | { kind: 'state.wait'; until: (s: ClientState) => boolean }
  | { kind: 'now' }
  | { kind: 'id'; scope: 'mutation' | 'salt' }
  | { kind: 'hash'; input: string }
  | { kind: 'emit'; event: OutEvent }
  | { kind: 'dispatch'; event: RuntimeEvent }
  | { kind: 'all'; effects: Effect[] }
  | { kind: 'service'; name: ServiceName; args: unknown[] };

export type EffectKind = Effect['kind'];

/**
 * Calls into the legacy services that are adapters, not state: auth, tabs,
 * blobs, migrator, crdt, persistence, the SSP lifecycle, the remote socket.
 * A saga names the call; the runtime binds it to the real service.
 */
export interface ServiceCalls {
  'hint.read': () => string | null;
  'hint.write': (bucketId: string) => void;
  'local.connect': (bucketId: string) => Promise<void>;
  'local.switchStore': (bucketId: string) => Promise<void>;
  'local.beginSwitch': () => () => void;
  'local.currentBucketId': () => string;
  'local.usesSurqlSchema': () => boolean;
  'migrator.provision': () => Promise<void>;
  'blobs.start': (bucketId: string) => Promise<void>;
  'blobs.setNamespace': (bucketId: string) => Promise<void>;
  'blobs.clear': () => Promise<void>;
  'ssp.init': () => Promise<void>;
  'ssp.setPermissions': () => void;
  'ssp.setSessionAuth': (authId: string | null, access: string | null) => void;
  'ssp.prime': (pendingIds: string[]) => Promise<void>;
  'ssp.reset': () => Promise<void>;
  'ssp.setPersistence': (enabled: boolean) => void;
  'auth.restoreSession': () => Promise<string | null>;
  'auth.init': () => Promise<void>;
  'auth.sessionAuthId': () => string | null;
  'auth.access': () => string | null;
  'auth.token': () => string | null;
  'auth.currentUser': () => Record<string, unknown> | null;
  'remote.connect': () => Promise<void>;
  'remote.releaseViews': (ids: unknown[]) => void;
  'supervisor.start': () => void;
  'tabs.start': (bucketId: string) => Promise<'solo' | 'leader' | 'follower'>;
  'tabs.moveToBucket': (bucketId: string) => Promise<'solo' | 'leader' | 'follower'>;
  'crdt.setSessionId': (sessionId: string) => void;
  'crdt.closeAll': (flush: boolean) => void;
  'persistence.set': (key: string, value: unknown) => Promise<void>;
  'features.init': () => void;
  'releases.init': () => void;
  'window.attach': () => void;
}
export type ServiceName = keyof ServiceCalls;

/** Typed constructors. Each is one line so the saga reads like the plan. */
export const fx = {
  local: {
    query: (sql: string, vars?: Vars, epoch?: number): Effect => ({ kind: 'local.query', sql, vars, epoch }),
    select: (plan: QueryPlan, params?: Vars): Effect => ({ kind: 'local.select', plan, params }),
    execute: (query: SealedQuery<unknown>, vars?: Vars, epoch?: number): Effect => ({
      kind: 'local.execute',
      query,
      vars,
      epoch,
    }),
    getById: (table: string, id: unknown): Effect => ({ kind: 'local.getById', table, id }),
    upsert: (table: string, id: unknown, data: Record<string, unknown>, mode: 'replace' | 'merge'): Effect => ({
      kind: 'local.upsert',
      table,
      id,
      data,
      mode,
    }),
    delete: (table: string, id: unknown): Effect => ({ kind: 'local.delete', table, id }),
    /** The store's current epoch; a write fenced with it is dropped after a bucket switch or transport failover. */
    epoch: (): Effect => ({ kind: 'local.epoch' }),
  },
  remote: {
    query: (sql: string, vars?: Vars, timeoutMs?: number): Effect => ({ kind: 'remote.query', sql, vars, timeoutMs }),
    /** Subscribe to `LIVE SELECT * FROM <table>`; resolves to the live uuid. */
    live: (table: string): Effect => ({ kind: 'remote.live', table }),
    kill: (uuid: string): Effect => ({ kind: 'remote.kill', uuid }),
  },
  ssp: {
    register: (plan: RegisterPlan): Effect => ({ kind: 'ssp.register', plan }),
    unregister: (hash: string): Effect => ({ kind: 'ssp.unregister', hash }),
    ingest: (records: IngestRecord[]): Effect => ({ kind: 'ssp.ingest', records }),
  },
  timer: {
    set: (key: string, ms: number, event: RuntimeEvent): Effect => ({ kind: 'timer.set', key, ms, event }),
    clear: (key: string): Effect => ({ kind: 'timer.clear', key }),
  },
  state: {
    read: <T>(select: (s: ClientState) => T): Effect => ({ kind: 'state.read', select }),
    update: (fn: (s: ClientState) => ClientState): Effect => ({ kind: 'state.update', fn }),
    /** Suspend until `until(state)` holds (checked after every state.update). */
    wait: (until: (s: ClientState) => boolean): Effect => ({ kind: 'state.wait', until }),
  },
  now: (): Effect => ({ kind: 'now' }),
  id: (scope: 'mutation' | 'salt'): Effect => ({ kind: 'id', scope }),
  hash: (input: string): Effect => ({ kind: 'hash', input }),
  emit: (event: OutEvent): Effect => ({ kind: 'emit', event }),
  dispatch: (event: RuntimeEvent): Effect => ({ kind: 'dispatch', event }),
  all: (effects: Effect[]): Effect => ({ kind: 'all', effects }),
  service: <N extends ServiceName>(name: N, ...args: Parameters<ServiceCalls[N]>): Effect => ({ kind: 'service', name, args }),
} as const;
