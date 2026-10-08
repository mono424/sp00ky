import type { ConnectionState, MutationEventType, SyncHealth } from '../../types';
import type { OutboxItem, PendingWrite, TabRole } from '../../state/client-state';
import { createdAtFromId } from '../../mutation/rows';

/**
 * Where a mutation is, as the DevTools Mutations tab shows it:
 * - `pending`     in the outbox, not sent yet (or sent and not answered)
 * - `retrying`    in the outbox after at least one failed push
 * - `synced`      the server accepted it
 * - `rolled-back` the server rejected it; the local change was undone and the
 *                 row moved to the failed-writes tray
 * - `dropped`     left the outbox without an answer this tab saw (a bucket
 *                 switch, a sign-out, or its row was already gone)
 */
export type DevToolsMutationStatus = 'pending' | 'retrying' | 'synced' | 'rolled-back' | 'dropped';

export interface DevToolsMutation {
  /** `_00_pending_mutations:<ts>_<seq>_<tab>`, escaping removed. */
  id: string;
  op: MutationEventType;
  recordId: string;
  table: string;
  /** Field names of the payload (the values stay in the page; see `mutationOp('get')`). */
  fields?: string[];
  status: DevToolsMutationStatus;
  attempts: number;
  queuedAt: number;
  settledAt?: number;
  error?: string;
}

/** What DevTools remembers about one mutation after it left the outbox. */
export interface MutationHistoryEntry {
  id: string;
  op: MutationEventType;
  recordId: string;
  table: string;
  fields?: string[];
  queuedAt: number;
  outcome?: { status: 'synced' | 'rolled-back'; at: number; error?: string };
}

export interface DevToolsDebouncedWrite {
  key: string;
  recordId: string;
  table: string;
  fields: string[];
  since: number;
  /** Mirrored to `_00_pending_writes`, so a reload inside the delay keeps it. */
  durable: boolean;
}

export interface DevToolsMutationsState {
  /** `follower` tabs never drain: the leader pushes their writes. */
  role: TabRole;
  connection: ConnectionState;
  health: SyncHealth['status'];
  consecutiveFailures: number;
  lastError?: string;
  counts: {
    pending: number;
    retrying: number;
    debounced: number;
    /** Rows in the persistent failed-writes tray, any session. */
    failed: number;
    /** This session, since DevTools attached. */
    synced: number;
    rolledBack: number;
  };
  /** Newest first, capped at `cap`. */
  entries: DevToolsMutation[];
  total: number;
  debounced: DevToolsDebouncedWrite[];
}

export interface MutationsInput {
  outbox: ReadonlyArray<OutboxItem>;
  pendingWrites: Iterable<PendingWrite>;
  failedCount: number;
  tabRole: TabRole;
  health: SyncHealth;
  history: Iterable<MutationHistoryEntry>;
  cap: number;
}

/** When a mutation was queued, from the timestamp its id starts with (0 for a legacy id). */
export const queuedAtOf = (id: string): number => createdAtFromId(id) ?? 0;

/** The tab's mutation picture: the live outbox over what DevTools saw leave it. */
export function buildMutationsState(input: MutationsInput): DevToolsMutationsState {
  const byId = new Map<string, DevToolsMutation>();
  for (const h of input.history) {
    byId.set(h.id, {
      id: h.id,
      op: h.op,
      recordId: h.recordId,
      table: h.table,
      fields: h.fields,
      status: h.outcome ? h.outcome.status : 'dropped',
      attempts: 0,
      queuedAt: h.queuedAt,
      settledAt: h.outcome?.at,
      error: h.outcome?.error,
    });
  }
  for (const item of input.outbox) {
    const seen = byId.get(item.id);
    const status: DevToolsMutationStatus =
      item.status === 'acked' ? 'synced' : item.attempts > 0 ? 'retrying' : 'pending';
    byId.set(item.id, {
      id: item.id,
      op: item.type,
      recordId: item.recordId,
      table: item.table,
      fields: seen?.fields,
      status,
      attempts: item.attempts,
      queuedAt: seen?.queuedAt || queuedAtOf(item.id),
      settledAt: item.ackedAt ?? seen?.settledAt,
    });
  }

  const all = [...byId.values()].toSorted((a, b) => b.queuedAt - a.queuedAt || (a.id < b.id ? 1 : -1));
  const count = (s: DevToolsMutationStatus) => all.filter((m) => m.status === s).length;
  const debounced = [...input.pendingWrites].map((w) => ({
    key: w.key,
    recordId: w.recordId,
    table: w.table,
    fields: Object.keys(w.data),
    since: w.firstAt,
    durable: w.mirrorId !== null,
  }));

  return {
    role: input.tabRole,
    connection: input.health.connection,
    health: input.health.status,
    consecutiveFailures: input.health.consecutiveFailures,
    lastError: input.health.error,
    counts: {
      pending: count('pending'),
      retrying: count('retrying'),
      debounced: debounced.length,
      failed: input.failedCount,
      synced: count('synced'),
      rolledBack: count('rolled-back'),
    },
    entries: all.slice(0, input.cap),
    total: all.length,
    debounced,
  };
}

/** Keep the newest `cap` entries of an insertion-ordered history map. */
export function trimHistory(history: Map<string, MutationHistoryEntry>, cap: number): void {
  for (const id of history.keys()) {
    if (history.size <= cap) return;
    history.delete(id);
  }
}
