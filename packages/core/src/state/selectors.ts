import type { PhaseStat, QueryHash, QueryState, QueryStatus, QueryTimings, RecordVersionArray } from '../types';
import type { ClientState, QueryEntry } from './client-state';
import { deriveStatus, hasServerMembership, isAuthoritative } from './lifecycle';
import { FETCH_CHUNK } from '../kernel/constants';

/** Pure reads over `ClientState`. Sagas use them through `state.read`. */

export const queryByHash = (s: ClientState, hash: QueryHash): QueryEntry | undefined => s.queries.get(hash);

export const activeHashes = (s: ClientState): QueryHash[] => [...s.queries.keys()];

export const hashesForTable = (s: ClientState, table: string): QueryHash[] =>
  [...s.queries].filter(([, e]) => e.def.tableName === table).map(([h]) => h);

export const queryStatus = (s: ClientState, hash: QueryHash): QueryStatus | undefined => {
  const e = s.queries.get(hash);
  return e ? deriveStatus(e.lifecycle) : undefined;
};

export interface Overlay {
  /** Ids with an unsynced or just-acked create/update. */
  readonly writes: ReadonlySet<string>;
  /** Ids with an unsynced or just-acked delete. */
  readonly deletes: ReadonlySet<string>;
}

/** The optimistic overlay, derived from the outbox (pending AND acked items). */
export function overlay(s: ClientState): Overlay {
  const writes = new Set<string>();
  const deletes = new Set<string>();
  for (const item of s.outbox) {
    if (item.type === 'delete') deletes.add(item.recordId);
    else writes.add(item.recordId);
  }
  return { writes, deletes };
}

export const pendingDeleteIds = (s: ClientState): ReadonlySet<string> =>
  new Set(s.outbox.filter((i) => i.type === 'delete').map((i) => i.recordId));

export const hasAckedWrites = (s: ClientState): boolean => s.outbox.some((i) => i.status === 'acked');

export const pendingMutationCount = (s: ClientState): number =>
  s.outbox.filter((i) => i.status === 'pending').length;

export const fetchingQueryCount = (s: ClientState): number =>
  [...s.queries.values()].filter((e) => e.lifecycle.fetchDepth > 0).length;

/** Membership entries whose body is missing or older locally. */
export function needed(s: ClientState, hash: QueryHash): RecordVersionArray {
  const e = s.queries.get(hash);
  if (!e || !hasServerMembership(e.lifecycle)) return [];
  const deletes = pendingDeleteIds(s);
  return e.remoteArray.filter(([id, v]) => !deletes.has(id) && (s.versions.get(id) ?? -1) < v);
}

/** Subquery child bodies missing or stale locally (never part of `settled`). */
export function neededChildren(s: ClientState, hash: QueryHash): RecordVersionArray {
  const e = s.queries.get(hash);
  if (!e || e.subqueryRemoteArray.length === 0) return [];
  return e.subqueryRemoteArray.filter(([id, v]) => (s.versions.get(id) ?? -1) < v);
}

export interface FetchPlan {
  /** Queries whose primary membership needs bodies (they flip to `fetching`). */
  readonly hashes: QueryHash[];
  readonly chunks: string[][];
  /** Highest requested version per id, across every query naming it. */
  readonly versions: ReadonlyMap<string, number>;
}

/** Cross-query, deduped, chunked list of ids to pull from the server. */
export function planFetch(s: ClientState, chunkSize = FETCH_CHUNK): FetchPlan {
  const versions = new Map<string, number>();
  const hashes: QueryHash[] = [];
  const add = (pairs: RecordVersionArray) => {
    for (const [id, v] of pairs) versions.set(id, Math.max(v, versions.get(id) ?? -1));
  };
  for (const hash of s.queries.keys()) {
    const missing = needed(s, hash);
    if (missing.length > 0) hashes.push(hash);
    add(missing);
    add(neededChildren(s, hash));
  }
  const all = [...versions.keys()];
  const chunks: string[][] = [];
  for (let i = 0; i < all.length; i += chunkSize) chunks.push(all.slice(i, i + chunkSize));
  return { hashes, chunks, versions };
}

/**
 * "This query's rows are authoritative and complete": server membership
 * accepted, every body present at its version, nothing waiting to be
 * re-rendered, subscribers told at least once.
 */
export function settled(s: ClientState, hash: QueryHash): boolean {
  const e = s.queries.get(hash);
  if (!e || e.lifecycle.phase !== 'live') return false;
  return needed(s, hash).length === 0 && !s.dirty.has(hash) && e.lifecycle.notified;
}

export const settleFailed = (s: ClientState, hash: QueryHash): boolean => {
  const e = s.queries.get(hash);
  return !e || e.lifecycle.remote === 'failed';
};

export const desiredRegistrations = (s: ClientState): QueryHash[] =>
  [...s.queries].filter(([, e]) => e.lifecycle.remote === 'unregistered').map(([h]) => h);

export const evictable = (s: ClientState, now: number): QueryHash[] =>
  [...s.queries]
    .filter(
      ([, e]) => e.subscribers === 0 && e.lastSubscriberLeftAt !== null && now - e.lastSubscriberLeftAt >= e.def.ttlMs
    )
    .map(([h]) => h);

export const shortestTtlMs = (s: ClientState): number | null => {
  let min: number | null = null;
  for (const e of s.queries.values()) min = min === null ? e.def.ttlMs : Math.min(min, e.def.ttlMs);
  return min;
};

/** Legacy `QueryState` shape for DevTools and the public type surface. */
export function toQueryState(e: QueryEntry): QueryState {
  const l = e.lifecycle;
  return {
    config: {
      id: e.def.id,
      surql: e.def.surql,
      plan: e.def.plan,
      params: { ...e.def.params },
      localArray: [...e.localArray],
      remoteArray: [...e.remoteArray],
      subqueryRemoteArray: e.subqueryRemoteArray.length ? [...e.subqueryRemoteArray] : undefined,
      membershipKnown: isAuthoritative(l),
      membershipKey: e.def.viewKey,
      ttl: e.def.ttl,
      lastActiveAt: new Date(e.lastHeartbeatAt ?? e.def.createdAt),
      tableName: e.def.tableName,
    },
    records: [...e.records] as Record<string, any>[],
    serverMembership: hasServerMembership(l),
    viewLost: l.phase === 'view-lost',
    serverState: e.serverState,
    syncNotified: l.notified,
    ttlTimer: null,
    ttlDurationMs: e.def.ttlMs,
    lastHeartbeatAt: e.lastHeartbeatAt ?? undefined,
    updateCount: e.telemetry.updateCount,
    lastUpdatedAt: e.telemetry.lastUpdatedAt,
    materializationSamples: [...e.telemetry.materializationSamples],
    lastIngestLatencyMs: e.telemetry.lastIngestLatencyMs,
    errorCount: e.telemetry.errorCount,
    status: deriveStatus(l),
    phaseSamples: Object.fromEntries(Object.entries(e.telemetry.phaseSamples).map(([k, v]) => [k, [...v]])),
    phaseLast: { ...e.telemetry.phaseLast },
    registrationTimings: e.telemetry.registrationTimings,
  };
}

function phaseStatOf(samples: ReadonlyArray<number>, lastMs: number | null): PhaseStat {
  if (samples.length === 0) return { lastMs, p50: null, p90: null, p99: null, count: 0 };
  const sorted = [...samples].sort((a, b) => a - b);
  const pick = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
  return { lastMs, p50: pick(0.5), p90: pick(0.9), p99: pick(0.99), count: samples.length };
}

/** Per-query processing-time breakdown for DevTools and the MCP. */
export function phaseTimings(e: QueryEntry): QueryTimings {
  const t = e.telemetry;
  const stat = (phase: string) => phaseStatOf(t.phaseSamples[phase] ?? [], t.phaseLast[phase] ?? null);
  return {
    ssp: phaseStatOf(t.materializationSamples, t.lastIngestLatencyMs),
    sspStoreApply: stat('sspStoreApply'),
    sspCircuitStep: stat('sspCircuitStep'),
    sspTransform: stat('sspTransform'),
    localFetch: stat('localFetch'),
    remoteFetch: stat('remoteFetch'),
    frontend: stat('frontend'),
    registration: t.registrationTimings,
    updateCount: t.updateCount,
    errorCount: t.errorCount,
  };
}
