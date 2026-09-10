import { RecordId } from 'surrealdb';
import type { ClientState, OutboxItem, QueryDefinition, QueryEntry } from '../state/client-state';
import { emptyState, emptyTelemetry } from '../state/client-state';
import { seedLifecycle } from '../state/lifecycle';
import { compose, putQuery } from '../state/reducers';
import type { Reducer } from '../state/reducers';

type DeepPartial<T> = { [K in keyof T]?: T[K] extends object ? DeepPartial<T[K]> | T[K] : T[K] };

export function buildDefinition(over: Partial<QueryDefinition> = {}): QueryDefinition {
  const hash = over.hash ?? 'h1';
  return {
    id: over.id ?? (new RecordId('_00_query', hash) as any),
    hash,
    viewKey: over.viewKey ?? `view-${hash}`,
    surql: over.surql ?? 'SELECT * FROM thing',
    params: over.params ?? {},
    plan: over.plan,
    ttl: over.ttl ?? '10m',
    ttlMs: over.ttlMs ?? 600_000,
    tableName: over.tableName ?? 'thing',
    createdAt: over.createdAt ?? 1_700_000_000_000,
  };
}

export function buildEntry(over: DeepPartial<QueryEntry> & { def?: Partial<QueryDefinition> } = {}): QueryEntry {
  const def = buildDefinition(over.def ?? {});
  return {
    def,
    lifecycle: { ...seedLifecycle(false), ...(over.lifecycle as object) },
    remoteArray: (over.remoteArray as QueryEntry['remoteArray']) ?? [],
    localArray: (over.localArray as QueryEntry['localArray']) ?? [],
    subqueryRemoteArray: (over.subqueryRemoteArray as QueryEntry['subqueryRemoteArray']) ?? [],
    records: (over.records as QueryEntry['records']) ?? [],
    serverState: over.serverState ?? null,
    subscribers: over.subscribers ?? 0,
    lastSubscriberLeftAt: over.lastSubscriberLeftAt ?? null,
    lastHeartbeatAt: over.lastHeartbeatAt ?? null,
    lastPolledAt: over.lastPolledAt ?? null,
    registerAttempts: over.registerAttempts ?? 0,
    telemetry: { ...emptyTelemetry(), ...(over.telemetry as object) },
  };
}

export function buildOutboxItem(over: Partial<OutboxItem> = {}): OutboxItem {
  return {
    id: over.id ?? 'm1',
    type: over.type ?? 'create',
    recordId: over.recordId ?? 'thing:1',
    table: over.table ?? 'thing',
    status: over.status ?? 'pending',
    ackedAt: over.ackedAt ?? null,
    attempts: over.attempts ?? 0,
  };
}

/** A state holding the given entries, with `dirty` cleared (as after materialization). */
export function buildState(entries: QueryEntry[] = [], ...extra: Reducer[]): ClientState {
  const base = compose(...entries.map(putQuery))(emptyState({ tabId: 'tab-a' }));
  return compose(...extra)({ ...base, dirty: new Set() });
}
