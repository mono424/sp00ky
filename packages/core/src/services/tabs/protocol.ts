/**
 * Shared-tabs protocol: the message vocabulary between browser tabs, the
 * SharedWorker broker (`tabs-broker-worker.ts`), and, once a leader exists,
 * between follower tabs and the leader over per-follower MessagePorts.
 *
 * Model (adapted from Jazz's browser broker, see the shared-tabs plan):
 * - The broker is CONTROL PLANE only. It elects one leader tab per namespace
 *   (fingerprint + bucketId), mints the MessageChannel pairs that connect each
 *   follower to the leader, and monitors liveness. No data flows through it.
 * - Per follower there are TWO ports: `dbPort` (raw sqlite ops, forwarded into
 *   the leader's dedicated worker, so reads/writes bypass the leader's main
 *   thread) and `syncPort` (sync control RPC + ingest relay, handled by the
 *   leader main thread).
 * - Every broker-to-tab message carries `brokerInstanceId`; a change means the
 *   browser restarted the SharedWorker and the tab must re-hello.
 *
 * The broker worker script must stay self-contained (bundlers cannot reliably
 * trace `new SharedWorker(url)` graphs), so it does NOT import this module at
 * runtime; it duplicates the few constants it needs and this file stays the
 * single source of truth for tab-side code and for tests.
 */
import type { StorageHealth } from '../../types';

export const TABS_PROTOCOL_VERSION = 1;

export type TabId = string;
export type TabVisibility = 'visible' | 'hidden';
export type TabRole = 'solo' | 'leader' | 'follower';

// ---- timing constants (broker worker duplicates these; keep in sync) -------

/** Broker pings every tab at this cadence. */
export const PING_INTERVAL_MS = 5000;
/** A tab that has not ponged for this long is presumed dead and evicted.
 *  MUST stay above the sqlite worker's freeze-suspect threshold (10s): a
 *  freeze shorter than that never triggers a steal, which is what makes the
 *  worker-side thaw gate sufficient. */
export const PONG_TIMEOUT_MS = 15_000;
/** How long an election waits for a dead leader's Web Locks to free up before
 *  stealing them. */
export const FORCE_TAKEOVER_TIMEOUT_MS = 1000;
/** A tab whose promotion failed is not re-nominated for this long. */
export const LEADER_FAILURE_BACKOFF_MS = 1000;
/** Follower attachment retry: initial delay, doubling per attempt, capped. */
export const ATTACH_RETRY_INITIAL_MS = 1000;
export const ATTACH_RETRY_MAX_MS = 30_000;
/** After this many consecutive failed elections, the broker allows the next
 *  leader to open in memory (reported as degraded) rather than leaving the
 *  namespace leaderless forever. Counts EVERY failure reason, not just
 *  opfs-unavailable: any reason that keeps recurring leaves every tab timing
 *  out in `start()` and falling back to solo, which is strictly worse than one
 *  shared in-memory store. */
export const FAILED_CYCLES_BEFORE_MEMORY = 3;

// ---- identity ---------------------------------------------------------------

export interface TabsFingerprintInput {
  coreVersion: string;
  /** Hash over the app schema's table names + field names. */
  schemaHash: string;
  endpoint: string;
  namespace: string;
  database: string;
}

/** Deterministic JSON: objects with sorted keys, so equal inputs always
 *  produce equal fingerprints regardless of construction order. */
export function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const keys = Object.keys(value as Record<string, unknown>).sort();
  const body = keys
    .map((k) => `${JSON.stringify(k)}:${stableStringify((value as Record<string, unknown>)[k])}`)
    .join(',');
  return `{${body}}`;
}

/** cyrb53: tiny, fast, good-enough 53-bit hash for identity strings. */
export function hash53(str: string, seed = 0): string {
  let h1 = 0xdeadbeef ^ seed;
  let h2 = 0x41c6ce57 ^ seed;
  for (let i = 0; i < str.length; i++) {
    const ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36);
}

export function computeTabsFingerprint(input: TabsFingerprintInput): string {
  return hash53(stableStringify({ v: TABS_PROTOCOL_VERSION, ...input }));
}

/** Lock names. The worker lock embeds the leadershipId, so after a steal the
 *  exact name is never re-acquired by anyone: "not held" is an unambiguous
 *  fencing check for a thawed worker, with no clientId resolution needed. */
export function tabLockName(fingerprint: string, bucketId: string): string {
  return `sp00ky-tabs:${fingerprint}:${bucketId}:tab`;
}
export function workerLockName(fingerprint: string, bucketId: string, leadershipId: number): string {
  return `sp00ky-tabs:${fingerprint}:${bucketId}:worker:${leadershipId}`;
}

// ---- election ---------------------------------------------------------------

export interface LeaderCandidate {
  tabId: TabId;
  visibility: TabVisibility;
  lastVisibleAt: number;
}

/** Visible tabs beat hidden ones; within the pool the most recently visible
 *  wins; ties break to the lexicographically greater tabId so every observer
 *  picks the same winner. (Duplicated in the broker worker.) */
export function selectLeaderCandidate<C extends LeaderCandidate>(candidates: C[]): C | null {
  if (candidates.length === 0) return null;
  const visible = candidates.filter((c) => c.visibility === 'visible');
  const pool = visible.length > 0 ? visible : candidates;
  return pool.reduce((best, c) => {
    if (c.lastVisibleAt !== best.lastVisibleAt) {
      return c.lastVisibleAt > best.lastVisibleAt ? c : best;
    }
    return c.tabId > best.tabId ? c : best;
  });
}

// ---- control plane ----------------------------------------------------------

/** A leader that survived a broker restart announces what it still holds, so
 *  the new broker instance re-promotes it instead of forcing a pool reopen. */
export interface HeldLeadership {
  leadershipId: number;
  workerLockName: string;
}

export type TabToBrokerMessage =
  | {
      type: 'hello';
      tabId: TabId;
      fingerprint: string;
      bucketId: string;
      visibility: TabVisibility;
      heldLeadership: HeldLeadership | null;
    }
  | { type: 'visibility'; tabId: TabId; bucketId: string; visibility: TabVisibility }
  | { type: 'leader-ready'; tabId: TabId; bucketId: string; leadershipId: number }
  | { type: 'leader-failed'; tabId: TabId; bucketId: string; leadershipId: number; reason: string }
  | {
      type: 'follower-port-attached';
      tabId: TabId;
      bucketId: string;
      leadershipId: number;
      followerTabId: TabId;
    }
  | { type: 'follower-port-closed'; tabId: TabId; bucketId: string; followerTabId: TabId }
  | { type: 'shutdown'; tabId: TabId; bucketId: string }
  | { type: 'pong'; tabId: TabId };

export type BrokerUnsupportedReason = 'fingerprint-mismatch' | 'protocol-version';

export type BrokerToTabMessage =
  | { type: 'broker-hello'; brokerInstanceId: string }
  | { type: 'ping'; brokerInstanceId: string }
  | {
      type: 'become-leader';
      brokerInstanceId: string;
      leadershipId: number;
      /** True when the previous leader's locks may need stealing. */
      forceTakeover: boolean;
      /** True only after repeated opfs-unavailable failures: the leader may
       *  open in memory and report degraded instead of failing again. */
      allowMemoryFallback: boolean;
      /** Set when the broker is re-promoting a surviving leader after its own
       *  restart; the tab keeps its worker and just rolls the lock forward. */
      resumeHeld: boolean;
    }
  | { type: 'demote'; brokerInstanceId: string; leadershipId: number }
  | { type: 'leader-ready'; brokerInstanceId: string; leadershipId: number; leaderTabId: TabId }
  // ev.ports = [dbPort, syncPort] on both attach messages.
  | {
      type: 'attach-follower-ports';
      brokerInstanceId: string;
      leadershipId: number;
      followerTabId: TabId;
    }
  | {
      type: 'use-follower-ports';
      brokerInstanceId: string;
      leadershipId: number;
      leaderTabId: TabId;
    }
  | { type: 'close-follower-ports'; brokerInstanceId: string; leadershipId: number }
  | { type: 'unsupported'; brokerInstanceId: string; reason: BrokerUnsupportedReason };

// ---- data plane (syncPort) --------------------------------------------------
// Deliberately narrow. Followers keep their OWN remote WebSocket, so they
// register/heartbeat/deregister their queries and run the list_ref poll
// themselves; the leader owns only what must be singular: the outbox drain
// and the one list_ref LIVE subscription (whose events it relays).

/** Matches `CacheIngestTuple` (modules/cache): exactly what `ingestMany`
 *  consumes, so relayed batches feed follower circuits without reshaping. */
export interface IngestTuple {
  table: string;
  op: 'CREATE' | 'UPDATE' | 'DELETE';
  id: string;
  record: Record<string, unknown>;
}

export type FollowerToLeaderMessage =
  | { type: 'sync-hello'; tabId: TabId }
  /** The follower committed an outbox row (through the shared store) and the
   *  leader should drain it. Idempotent; a new leader's loadFromDatabase is
   *  the backstop for a notify lost in a failover window. */
  | { type: 'mutation-enqueued'; mutationId: string }
  | { type: 'request-poll' }
  /** An optimistic write this follower committed to the SHARED store and
   *  ingested into its own circuit. The leader ingests it (no DB write, the
   *  row is already there) and fans it out to every OTHER follower as
   *  `ingest-relay`, so a follower's write lands in every tab in one hop
   *  instead of after the server round-trip. */
  | { type: 'ingest'; tuples: IngestTuple[] };

export type LeaderToFollowerMessage =
  | { type: 'db-ready'; leadershipId: number; bucketId: string; storageHealth: StorageHealth }
  /** Every ingest the leader's CacheModule committed, so follower circuits
   *  stay live without their own fetch. seq detects gaps. */
  | { type: 'ingest-relay'; tuples: IngestTuple[]; leadershipId: number; seq: number }
  /** A `_00_list_ref` LIVE event, relayed verbatim. Each follower resolves the
   *  queryId against its own DataModule and ignores foreign queries. */
  | {
      type: 'list-ref-change';
      action: 'CREATE' | 'UPDATE' | 'DELETE';
      queryId: string;
      recordId: string;
      version: number;
      parent: boolean;
    }
  /** The leader's drain rolled back a mutation owned by this tab. */
  | {
      type: 'mutation-rolled-back';
      mutationId: string;
      recordId: string;
      eventType: 'create' | 'update' | 'delete';
      error: string;
    }
  /** The leader's drain pushed a mutation and deleted its outbox row from the
   *  SHARED store. Every follower keeps the row in its overlay until its
   *  `_00_list_ref` membership arrives. */
  | {
      type: 'mutation-settled';
      mutationId: string;
      recordId: string;
      eventType: 'create' | 'update' | 'delete';
    }
  /** The leader's LIVE saw edges of these queries change; followers re-read
   *  membership for the ones they hold. */
  | { type: 'membership-dirty'; hashes: string[] }
  /** The failed-writes tray changed size on the shared store. */
  | { type: 'failed-mutations-changed'; count: number };
