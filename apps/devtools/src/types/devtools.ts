// Core DevTools Types - matches the backend DevToolsService structure

export interface BackendDevToolsState {
  eventsHistory: BackendEvent[];
  activeQueries: Record<number, ActiveQuery>;
  auth: BackendAuthState;
  version: string;
  versions?: VersionsState;
  database: DatabaseState;
}

// One end-to-end sync-pipeline probe cycle, as recorded by the scheduler.
// `ms` is null for a failed cycle; skipped cycles produce no sample at all,
// so a gap in the window means "not probed", never "0ms".
export interface HeartbeatSample {
  ts: number;
  ms: number | null;
  ok: boolean;
}

// E2E heartbeat state on the scheduler entity. The scheduler writes a probe
// row upstream and times the full round trip (DB event → ingest → broadcast →
// SSP circuit step); `samples` is its rolling window of recent cycles.
export interface HeartbeatInfo {
  enabled: boolean;
  stale: boolean;
  /** Nothing to measure right now (e.g. no ready SSPs mid-bootstrap). The
   *  latency is historical: nothing failed, but nothing is being served. */
  blocked?: boolean;
  blocked_reason?: string | null;
  last_e2e_ms: number | null;
  last_ok_epoch_ms: number | null;
  consecutive_failures: number;
  interval_secs: number;
  samples?: HeartbeatSample[];
}

// A single stack entity as reported by the backend `/info` (via
// `fn::spooky::info()`): one per ssp / scheduler / backend.
export interface BackendEntity {
  entity: string;
  id?: string;
  ip?: string | null;
  status?: string;
  version?: string;
  surrealdb_version?: string;
  uptime_seconds?: number;
  views?: number;
  /** Scheduler entity only. */
  heartbeat?: HeartbeatInfo;
  [key: string]: unknown;
}

// Frontend-vs-backend versions of the stack components. Unknown/unreachable
// values are reported as the string 'unavailable'. `entities` carries the full
// per-entity stack info for display.
export interface VersionsState {
  frontend: {
    core: string;
    wasm: string;
    surrealdb: string;
  };
  backend: {
    ssp: string;
    scheduler: string;
    surrealdb: string;
  };
  entities: BackendEntity[];
}

export interface BackendEvent {
  id: number;
  timestamp: number;
  eventType: string;
  payload: any;
}

export interface BackendAuthState {
  authenticated: boolean;
  userId?: string;
  timestamp?: number;
}

// Frontend DevTools State - normalized for UI
export interface DevToolsState {
  events: Sp00kyEvent[];
  activeQueries: ActiveQuery[];
  auth: AuthState;
  database: DatabaseState;
  versions: VersionsState;
}

export interface Sp00kyEvent {
  type: string;
  timestamp: number;
  data: unknown;
}

// Per-query processing-time breakdown (mirrors core's QueryTimings). Each phase
// is a rolling-window summary; `registration` is a one-shot per query.
export interface PhaseStat {
  lastMs: number | null;
  p50: number | null;
  p90: number | null;
  p99: number | null;
  count: number;
}

export interface RegistrationTimings {
  parseMs: number | null;
  planMs: number | null;
  snapshotMs: number | null;
  wallMs: number | null;
}

export interface QueryTimings {
  ssp: PhaseStat;
  sspStoreApply: PhaseStat;
  sspCircuitStep: PhaseStat;
  sspTransform: PhaseStat;
  localFetch: PhaseStat;
  remoteFetch: PhaseStat;
  frontend: PhaseStat;
  registration: RegistrationTimings;
  updateCount: number;
  errorCount: number;
}

export interface ActiveQuery {
  queryHash: number;
  status: 'initializing' | 'active' | 'updating' | 'destroyed';
  createdAt: number;
  lastUpdate: number;
  updateCount: number;
  dataSize?: number;
  /** Query cache Time-To-Live (e.g. '10m'), if known. */
  ttl?: string;
  query?: string;
  variables?: Record<string, unknown>;
  listenerCount?: number;
  connectedQueries?: number[];
  dataHash?: number;
  /** Rows: NOT in the pushed state; filled by `getQueryRows` when the Data tab is opened. */
  data?: any;
  localHash?: string;
  localArray?: any;
  remoteHash?: string;
  remoteArray?: any;
  /** Membership sizes, always pushed. */
  localCount?: number;
  remoteCount?: number;
  /** First ids of each membership array (capped; see `idsTruncated`). */
  localIds?: string[];
  remoteIds?: string[];
  idsTruncated?: boolean;
  /** Detailed per-phase processing-time breakdown (DevTools/MCP debugging). */
  timings?: QueryTimings;
}

// A single point on the Queries-tab timeline strip. Accumulated panel-side
// (the backend only reports the latest `lastUpdate` per query, not a history).
export interface QueryMark {
  queryHash: number;
  timestamp: number;
  kind: 'registered' | 'updated';
}

export interface AuthState {
  isAuthenticated: boolean;
  user: {
    email?: string;
    roles?: string[];
  } | null;
  lastAuthCheck: number;
}

export interface DatabaseState {
  tables: string[];
  /** Tables that exist on the remote DB (enumerated on demand). */
  remoteTables?: string[];
  /** Local cache backend: 'surreal' (WASM) | 'sqlite' | 'custom'. */
  engine?: string;
  tableData: Record<string, Record<string, unknown>[]>;
  schema?: Record<string, string[]>; // table -> column names
  /** Durability of the local store. `fallback: true` means persistence was
   *  requested but the dataset is actually only in RAM. */
  storage?: {
    status: 'unknown' | 'persistent' | 'memory';
    fallback: boolean;
    error?: string;
    role?: 'leader' | 'follower' | 'solo';
  };
  /** Shared-tabs state; null when the feature was never requested. */
  tabs?: SharedTabsInfo | null;
}

// Chrome Extension Message Types

export interface ChromeMessage {
  type: string;
  data?: unknown;
  state?: DevToolsState;
  tableName?: string;
  /**
   * Which frame this concerns. Inbound: the frame that produced the message
   * (stamped by the background script). Outbound: the frame the background
   * should deliver to. 0 is the main document.
   */
  frameId?: number;
  frameUrl?: string;
  /** SP00KY_FRAME_URL only: the frame's new URL after a route change. */
  url?: string;
  /** SP00KY_FRAMES only: every frame in the tab that announced a client. */
  frames?: Sp00kyFrame[];
}

/**
 * One Sp00ky client in the inspected tab. A tab has one per frame that runs
 * the library — the main document plus any iframe (an embedded app, a preview)
 * — and the panel inspects exactly one at a time.
 */
export interface Sp00kyFrame {
  /** 0 is the main document. */
  frameId: number;
  url: string;
  /** Client version, for spotting a frame on a different build. */
  version?: string;
}

export interface Sp00kyTableDataResponse {
  type: 'SP00KY_TABLE_DATA_RESPONSE';
  source: 'sp00ky-devtools-page';
  tableName: string;
  data: Record<string, unknown>[];
}

// UI State Types

export type TabType =
  | 'events'
  | 'queries'
  | 'database'
  | 'storage'
  // Auth + feature flags, merged: identity and what that identity is allowed
  // to see are one story.
  | 'access'
  | 'mcp'
  | 'versions'
  | 'timing';

// Access tab types — mirrored from packages/core/src/modules/devtools/flags.ts
// (the extension deliberately doesn't import core types; keep the shapes in
// sync by hand).

export interface FlagRule {
  kind: 'allowlist' | 'rollout' | string;
  variant: string;
  /** Allowlist only. Record-id strings, not records. */
  users?: string[];
  /** Rollout only, 0..100. */
  percent?: number;
  priority: number;
}

export interface FlagRow {
  key: string;
  description?: string;
  variants: string[];
  default_variant: string;
  enabled: boolean;
  payloads?: Record<string, unknown>;
  rules: FlagRule[];
  updated_at?: string;
  /** The variant the signed-in user is explicitly allowlisted into, if any. */
  selfAllowlistedVariant?: string;
}

export interface FlagAssignment {
  key: string;
  variant: string;
  payload?: unknown;
}

/** A variant forced in this browser only. Never sent to the server. */
export interface FlagOverride {
  variant: string;
  payload?: unknown;
}

/** Mirrors core's `ImpersonationInfo`; dates arrive serialized. */
export interface ImpersonationInfo {
  session: string;
  target: string;
  admin: string;
  tokenExpiresAt: string | null;
}

/** `window.__00__.impersonationOp('status')`. */
export interface ImpersonationStatus {
  /** The generated schema says the project enabled impersonation. */
  enabled?: boolean;
  isAdmin?: boolean;
  current?: ImpersonationInfo | null;
  active?: Array<{ session: string; target: string; admin: string; reason: string; expires_at: string }>;
}

/** A row of the user picker. Extra fields are the project's `searchFields`. */
export interface ImpersonationUser {
  id: string;
  is_admin: boolean;
  [field: string]: unknown;
}

export interface FlagsSnapshot {
  at: number;
  userId: string | null;
  isAdmin: boolean;
  /** Empty for non-admins — SurrealDB filters the rows rather than erroring. */
  flags: FlagRow[];
  assignments: FlagAssignment[];
  overrides: Record<string, FlagOverride>;
  /** Set when the schema predates the Access tab, or the remote read failed. */
  error?: string;
}

export interface FlagMutationResult {
  success: boolean;
  error?: string;
  /** Users re-evaluated by `fn::feature::materialize`. */
  users?: number;
}

// Storage tab types — mirrored from packages/core/src/modules/devtools/storage-info.ts
// (the extension deliberately doesn't import core types).

export interface OpfsEntry {
  path: string;
  kind: 'file' | 'directory';
  /** Absent when the file is locked by an exclusive access handle (live SAHPool). */
  size?: number;
}

/** Shared-tabs coordination state (sharedTabs: true). `active: false` with a
 *  reason means the app asked to share one store but this tab is running
 *  alone. Mirrors core's SharedTabsInfo. */
export interface SharedTabsInfo {
  active: boolean;
  reason?: string;
  role?: 'solo' | 'leader' | 'follower';
  tabId?: string;
  leadershipId?: number;
  leaderTabId?: string | null;
  followers?: number;
  relayedBatches?: number;
}

export interface EngineStorageDiagnostics {
  engine: 'sqlite';
  bucketId: string;
  useOpfs: boolean;
  workerSelectConfigured: boolean;
  workerSelectEffective: boolean;
  dbSizeBytes?: number;
  freelistBytes?: number;
  tableCounts?: { table: string; rows: number }[];
  error?: string;
}

export interface StorageInfo {
  at: number;
  engine: { kind: 'surrealdb' | 'sqlite' | 'custom'; store: string; bucketId: string };
  health: {
    status: 'unknown' | 'persistent' | 'memory';
    fallback: boolean;
    error?: string;
    role?: 'leader' | 'follower' | 'solo';
  };
  /** Shared-tabs state, or null when the feature was never requested. */
  tabs?: SharedTabsInfo | null;
  browser: {
    persisted?: boolean;
    usage?: number;
    quota?: number;
    usageDetails?: Record<string, number>;
    error?: string;
  };
  opfs: {
    supported: boolean;
    entries: OpfsEntry[];
    totalBytes: number;
    truncated: boolean;
    error?: string;
  };
  blobs?: BlobCacheInfo;
  sqliteStats?: Record<string, unknown>;
  engineDiagnostics?: EngineStorageDiagnostics;
}

/** Mirror of the core `BlobCacheInfo` — bucket-file cache counters. */
export interface BlobCacheInfo {
  entries: number;
  totalBytes: number;
  budgetBytes: number;
  pinnedBytes: number;
  evictedEntries: number;
  evictedBytes: number;
  reconciledEntries: number;
  hits: number;
  misses: number;
  persistent: boolean;
  persistPaused: boolean;
}

// Shared default so adapters/stores can fall back when versions are absent.
export const DEFAULT_VERSIONS: VersionsState = {
  frontend: { core: 'unavailable', wasm: 'unavailable', surrealdb: 'unavailable' },
  backend: { ssp: 'unavailable', scheduler: 'unavailable', surrealdb: 'unavailable' },
  entities: [],
};

export interface UIState {
  activeTab: TabType;
  selectedQueryHash: number | null;
  selectedTable: string | null;
  theme: 'light' | 'dark' | 'auto';
}

// Utility Types

export type EvalResult<T> = T | { error: string };

export interface TableColumn {
  key: string;
  label: string;
  type?: string;
}
