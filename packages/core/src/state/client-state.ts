import type { QueryPlan, RecordId } from '@spooky-sync/query-builder';
import type {
  ConnectionState,
  MutationEventType,
  QueryHash,
  QueryTimeToLive,
  RecordVersionArray,
  RegistrationTimings,
  SyncHealth,
} from '../types';
import type { QueryLifecycle } from './lifecycle';
import type { LaneState } from '../kernel/saga';
import { emptyLanes } from '../kernel/saga';

export type Row = Record<string, unknown>;

/** Everything a query is, minus anything that changes after registration. */
export interface QueryDefinition {
  /** Session-salted `_00_query:<hash>` record id: the remote view id. */
  readonly id: RecordId<string>;
  readonly hash: QueryHash;
  /** Unsalted `sha256({surql, params})`: key of the durable `_00_view` row. */
  readonly viewKey: string;
  readonly surql: string;
  readonly params: Readonly<Record<string, unknown>>;
  readonly plan?: QueryPlan;
  readonly ttl: QueryTimeToLive;
  readonly ttlMs: number;
  readonly tableName: string;
  readonly createdAt: number;
}

export type ServerViewState = 'materializing' | 'ready' | null;

export interface QueryTelemetry {
  readonly updateCount: number;
  readonly errorCount: number;
  readonly lastUpdatedAt: number | null;
  readonly materializationSamples: ReadonlyArray<number>;
  readonly lastIngestLatencyMs: number | null;
  readonly phaseSamples: Readonly<Record<string, ReadonlyArray<number>>>;
  readonly phaseLast: Readonly<Record<string, number | null>>;
  readonly registrationTimings: RegistrationTimings;
}

export interface QueryEntry {
  readonly def: QueryDefinition;
  readonly lifecycle: QueryLifecycle;
  /** Server membership (ids + versions) or the durable seed. */
  readonly remoteArray: RecordVersionArray;
  /** The in-browser SSP view's id-set: local truth for "does this row match". */
  readonly localArray: RecordVersionArray;
  readonly subqueryRemoteArray: RecordVersionArray;
  readonly records: ReadonlyArray<Row>;
  readonly serverState: ServerViewState;
  readonly subscribers: number;
  readonly lastSubscriberLeftAt: number | null;
  readonly lastHeartbeatAt: number | null;
  readonly lastPolledAt: number | null;
  readonly registerAttempts: number;
  readonly telemetry: QueryTelemetry;
}

export type OutboxStatus = 'pending' | 'acked';

/** In-memory mirror of one `_00_pending_mutations` row plus its push progress. */
export interface OutboxItem {
  readonly id: string;
  readonly type: MutationEventType;
  readonly recordId: string;
  readonly table: string;
  readonly status: OutboxStatus;
  readonly ackedAt: number | null;
  readonly attempts: number;
}

export type TabRole = 'solo' | 'leader' | 'follower';

/** A debounced update accumulating locally until its flush writes the outbox row. */
export interface PendingWrite {
  readonly key: string;
  readonly table: string;
  readonly recordId: string;
  readonly data: Readonly<Record<string, unknown>>;
  readonly before: Readonly<Record<string, unknown>> | null;
  readonly firstAt: number;
}

export interface SyncSlice {
  readonly health: SyncHealth;
  readonly consecutiveFailures: number;
  readonly hasSyncedOnce: boolean;
  readonly selfHealAttempts: number;
  readonly pollIdleStreak: number;
  readonly lastReconnectRefetchAt: number | null;
  readonly needsResubscribe: boolean;
  readonly fetchAttempts: number;
  readonly liveUuid: string | null;
  readonly liveTable: string | null;
  readonly lanes: LaneState;
}

export interface ClientState {
  readonly sessionId: string | null;
  readonly userId: string | null;
  /** The principal the current session salt was minted for. */
  readonly saltUserId: string | null;
  /** Latest bucket-switch target; an older switch that wakes up to a newer target skips. */
  readonly pendingBucket: string | null;
  readonly tabId: string;
  readonly tabRole: TabRole;
  readonly bucketId: string | null;
  readonly localReady: boolean;
  readonly primed: boolean;
  readonly queries: ReadonlyMap<QueryHash, QueryEntry>;
  /** Hashes whose local registration is in flight (dedupes concurrent `query()` calls). */
  readonly registering: ReadonlySet<QueryHash>;
  /** Re-read attempts per hash while the server reports a view as `materializing`. */
  readonly membershipReread: ReadonlyMap<QueryHash, number>;
  /** Local body versions (`_00_rv`) by encoded record id. */
  readonly versions: ReadonlyMap<string, number>;
  readonly outbox: ReadonlyArray<OutboxItem>;
  readonly pendingWrites: ReadonlyMap<string, PendingWrite>;
  readonly failedCount: number;
  /** Queries whose render inputs changed since their last materialization. */
  readonly dirty: ReadonlySet<QueryHash>;
  /** Queries whose server membership must be re-read. */
  readonly membershipDirty: ReadonlySet<QueryHash>;
  readonly sync: SyncSlice;
}

export const emptyTelemetry = (): QueryTelemetry => ({
  updateCount: 0,
  errorCount: 0,
  lastUpdatedAt: null,
  materializationSamples: [],
  lastIngestLatencyMs: null,
  phaseSamples: {},
  phaseLast: {},
  registrationTimings: { parseMs: null, planMs: null, snapshotMs: null, wallMs: null },
});

export const initialHealth = (connection: ConnectionState = 'disconnected'): SyncHealth => ({
  status: 'healthy',
  consecutiveFailures: 0,
  everConnected: false,
  connection,
});

export function emptyState(init: { tabId: string }): ClientState {
  return {
    sessionId: null,
    userId: null,
    saltUserId: null,
    pendingBucket: null,
    tabId: init.tabId,
    tabRole: 'solo',
    bucketId: null,
    localReady: false,
    primed: false,
    queries: new Map(),
    registering: new Set(),
    membershipReread: new Map(),
    versions: new Map(),
    outbox: [],
    pendingWrites: new Map(),
    failedCount: 0,
    dirty: new Set(),
    membershipDirty: new Set(),
    sync: {
      health: initialHealth(),
      consecutiveFailures: 0,
      hasSyncedOnce: false,
      selfHealAttempts: 0,
      pollIdleStreak: 0,
      lastReconnectRefetchAt: null,
      needsResubscribe: false,
      fetchAttempts: 0,
      liveUuid: null,
      liveTable: null,
      lanes: emptyLanes(),
    },
  };
}
