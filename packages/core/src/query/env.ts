import type { SchemaStructure } from '@spooky-sync/query-builder';
import type { RefMode } from '../modules/ref-tables';
import { ANON_USER_ID, DEFAULT_REF_MODE, listRefTableFor } from '../modules/ref-tables';
import type { ClientState } from '../state/client-state';
import {
  DEGRADE_AFTER_FAILURES,
  LIST_REF_POLL_BASE_MS,
  MATERIALIZE_DEBOUNCE_MS,
  OUTBOX_BATCH_SIZE,
  PUSH_TIMEOUT_MS,
} from '../kernel/constants';

/**
 * Static configuration the sagas read. Plain data, built once by the runtime
 * from `Sp00kyConfig`; sagas never see the config object itself.
 */
export interface SagaEnv {
  readonly schema: SchemaStructure;
  readonly refMode: RefMode;
  readonly anonLive: boolean;
  readonly remoteTimeoutMs: number;
  readonly pushTimeoutMs: number;
  readonly outboxBatchSize: number;
  readonly degradeAfter: number;
  readonly materializeDebounceMs: number;
  readonly pollBaseMs: number;
  readonly defaultTtlMs: number;
}

export function defaultEnv(schema: SchemaStructure, over: Partial<SagaEnv> = {}): SagaEnv {
  return {
    schema,
    refMode: DEFAULT_REF_MODE,
    anonLive: false,
    remoteTimeoutMs: 60_000,
    pushTimeoutMs: PUSH_TIMEOUT_MS,
    outboxBatchSize: OUTBOX_BATCH_SIZE,
    degradeAfter: DEGRADE_AFTER_FAILURES,
    materializeDebounceMs: MATERIALIZE_DEBOUNCE_MS,
    pollBaseMs: LIST_REF_POLL_BASE_MS,
    defaultTtlMs: 600_000,
    ...over,
  };
}

/** The `_00_list_ref` table this session reads: per-user, anonymous, or global. */
export function listRefTable(env: SagaEnv, s: ClientState): string {
  const userId = s.userId ?? (env.anonLive ? ANON_USER_ID : null);
  return listRefTableFor(env.refMode, userId);
}

/**
 * Whether this session can be sent a view's membership at all. A signed-out
 * session reads edges only from `_00_list_ref_anon`, and only with
 * `enableAnonymousLiveQueries`: without it the SSP files its views under no
 * identity in `_00_list_ref`, whose rule shows those edges to no caller. Such
 * a view can register and report its row count, but never its rows.
 */
export const receivesMembership = (env: SagaEnv, s: ClientState): boolean => s.userId !== null || env.anonLive;

export function columnsFor(env: SagaEnv, table: string): Record<string, unknown> | null {
  const found = env.schema.tables.find((t) => t.name === table);
  if (found) return found.columns as Record<string, unknown>;
  return table.startsWith('_00_') ? {} : null;
}
