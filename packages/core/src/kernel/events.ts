import type { ConnectionState, QueryHash, QueryStatus, SyncHealth } from '../types';
import type { StreamUpdate } from '../services/stream-processor/index';

/**
 * Inbound events. Everything that can start a saga arrives as one of these
 * through the runtime router: public API calls, adapter callbacks, timers,
 * and `dispatch` effects from other sagas.
 */
export type RuntimeEvent =
  | { type: 'EnsureRegistered'; requireAuth?: boolean; attempt?: number }
  | { type: 'RegisterRemote'; hash: QueryHash; retry?: boolean }
  | { type: 'SyncOutcome'; ok: boolean; error?: unknown }
  | { type: 'AckPrune' }
  | { type: 'ReadDirtyMembership' }
  | { type: 'ReadMembership'; hashes: QueryHash[] }
  | { type: 'FetchRows' }
  | { type: 'Materialize'; hash: QueryHash }
  | { type: 'MaterializeDirty' }
  | { type: 'LifecycleTick' }
  | { type: 'GcTick' }
  | { type: 'Drain' }
  | { type: 'FlushWrite'; key: string }
  | { type: 'PollTick' }
  | { type: 'SelfHealTick' }
  | { type: 'HeartbeatNow' }
  | { type: 'StartRemote' }
  | { type: 'PrimeCircuit' }
  | { type: 'VersionsPrimed'; entries: Array<readonly [string, number]> }
  | { type: 'WarmBlobs' }
  | { type: 'AuthFlip'; userId: string | null }
  | { type: 'BucketSwitch'; target: string }
  | { type: 'PageHide' }
  | { type: 'ConnectionChanged'; state: ConnectionState }
  | { type: 'StreamUpdate'; update: StreamUpdate }
  | { type: 'LiveStart' }
  | { type: 'LiveChange'; hashes: QueryHash[] }
  | { type: 'TabRole'; role: 'solo' | 'leader' | 'follower' }
  | { type: 'TabMessage'; message: unknown };

export type OutEvent =
  | { type: 'query:records'; hash: QueryHash; records: ReadonlyArray<Record<string, unknown>> }
  | { type: 'query:status'; hash: QueryHash; status: QueryStatus }
  | { type: 'query:authority'; hash: QueryHash; known: boolean }
  | { type: 'query:view-lost'; hash: QueryHash }
  | { type: 'query:evicted'; hash: QueryHash }
  | { type: 'mutation:event'; event: unknown }
  | { type: 'mutation:settled'; mutationId: string; recordId: string; eventType: string }
  | { type: 'mutation:rolled-back'; mutationId: string; recordId: string; eventType: string; error: string }
  | { type: 'tray:changed'; count: number }
  | { type: 'health:changed'; health: SyncHealth }
  | { type: 'activity:changed'; fetching: number; pending: number }
  | { type: 'tabs:broadcast'; message: unknown }
  | { type: 'tabs:sendTo'; tabId: string; message: unknown }
  | { type: 'devtools'; name: string; data?: unknown }
  | { type: 'log'; level: 'debug' | 'info' | 'warn' | 'error'; message: string; data?: unknown };
