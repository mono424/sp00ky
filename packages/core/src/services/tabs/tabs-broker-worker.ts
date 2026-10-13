/// <reference lib="webworker" />
/**
 * SharedWorker broker for shared-tabs mode: elects ONE leader tab per
 * namespace (fingerprint + bucketId), mints the MessageChannel pairs that
 * connect each follower tab to the leader, and monitors tab liveness. Control
 * plane only; no application data ever flows through here.
 *
 * SELF-CONTAINED ON PURPOSE: bundlers cannot reliably trace
 * `new SharedWorker(url)` module graphs, so this file has NO runtime imports
 * (type-only imports erase at build time). The few constants shared with the
 * tab side are duplicated from `protocol.ts`; keep them in sync. A build check
 * (`scripts/check-broker-bundle.mjs`) fails the build if an import statement
 * survives into the emitted file.
 *
 * Lifecycle notes:
 * - `brokerInstanceId` identifies THIS in-memory instance. The browser may
 *   kill and restart a SharedWorker at any time; tabs detect the id change
 *   (or missed pings) and re-hello. A surviving leader re-announces its held
 *   leadership so it is re-promoted without reopening the OPFS pool.
 * - Leadership ids are monotonic per instance. Web Lock names embed them
 *   (see protocol.ts workerLockName), so a steal permanently retires a name.
 * - The broker never touches Web Locks for itself; it only asks TABS to
 *   acquire/steal, except for the availability probe in
 *   `waitForPreviousLeaderLocks` which acquires-and-releases to test freedom.
 */
import type {
  BrokerToTabMessage,
  HeldLeadership,
  TabToBrokerMessage,
  TabVisibility,
} from './protocol';

// ---- constants duplicated from protocol.ts (keep in sync) -------------------
const PING_INTERVAL_MS = 5000;
const PONG_TIMEOUT_MS = 15_000;
/** How long a tab has to answer `become-leader` with `leader-ready`. A tab
 *  that hangs mid-promotion sends neither `leader-ready` nor `leader-failed`,
 *  and keeps answering pings, so nothing else here would notice: the
 *  namespace would keep a leader that is not `ready`, mint no follower ports,
 *  and refuse to re-elect (`electIfNeeded` bails while `ns.leader` is set)
 *  until every tab was reloaded. Treat the silence as a failed promotion. */
const PROMOTION_TIMEOUT_MS = 20_000;
const FORCE_TAKEOVER_TIMEOUT_MS = 1000;
const LEADER_FAILURE_BACKOFF_MS = 1000;
const FAILED_CYCLES_BEFORE_MEMORY = 3;

interface BrokerTab {
  port: MessagePort;
  tabId: string;
  fingerprint: string;
  bucketId: string;
  visibility: TabVisibility;
  lastVisibleAt: number;
  lastPongAt: number;
  heldLeadership: HeldLeadership | null;
}

interface Leader {
  tabId: string;
  leadershipId: number;
  ready: boolean;
}

interface Namespace {
  key: string;
  fingerprint: string;
  bucketId: string;
  tabs: Map<string, BrokerTab>;
  leader: Leader | null;
  /** Tabs whose promotion recently failed: tabId -> retry-not-before. */
  failedUntil: Map<string, number>;
  /** Consecutive elections whose promotion failed, for any reason. */
  failedCycles: number;
  /** Single-flight guard for the async election path. */
  electing: boolean;
  /** Pending follower attach state: followerTabId -> retry bookkeeping. */
  attachRetry: Map<string, { count: number; timer: ReturnType<typeof setTimeout> | null }>;
  /** Aborts the queued Web Lock request watching the current leader's tab
   *  lock (granted = the leader tab died; locks release instantly on death,
   *  unlike the pong timeout). */
  tabLockMonitor: AbortController | null;
  /** Deadline for the pending `become-leader` to report `leader-ready`. */
  promotionTimer: ReturnType<typeof setTimeout> | null;
}

const brokerInstanceId =
  typeof crypto !== 'undefined' && crypto.randomUUID
    ? crypto.randomUUID()
    : `bk_${Math.random().toString(36).slice(2)}`;

const namespaces = new Map<string, Namespace>();
/** Reverse index: which namespace a port belongs to (for pong routing). */
const portTab = new Map<MessagePort, { ns: Namespace; tabId: string }>();
/** The FIRST fingerprint wins for the whole broker instance; a tab from a
 *  different build gets `unsupported` and runs solo. Bucket ids may differ. */
let canonicalFingerprint: string | null = null;

let pingTimer: ReturnType<typeof setInterval> | null = null;

function post(port: MessagePort, msg: BrokerToTabMessage, transfer?: Transferable[]): void {
  try {
    if (transfer) port.postMessage(msg, transfer);
    else port.postMessage(msg);
  } catch {
    /* dead port; eviction will catch up */
  }
}

function nsKey(fingerprint: string, bucketId: string): string {
  return `${fingerprint}::${bucketId}`;
}

function getNamespace(fingerprint: string, bucketId: string): Namespace {
  const key = nsKey(fingerprint, bucketId);
  let ns = namespaces.get(key);
  if (!ns) {
    ns = {
      key,
      fingerprint,
      bucketId,
      tabs: new Map(),
      leader: null,
      failedUntil: new Map(),
      failedCycles: 0,
      electing: false,
      attachRetry: new Map(),
      tabLockMonitor: null,
      promotionTimer: null,
    };
    namespaces.set(key, ns);
  }
  return ns;
}

// Monotonic per broker instance (shared across namespaces; only monotonicity
// matters, not density).
let nextLeadershipId = 0;

// ---- election ----------------------------------------------------------------

/** Duplicated from protocol.ts selectLeaderCandidate; keep in sync. */
function pickCandidate(ns: Namespace): BrokerTab | null {
  const now = Date.now();
  const eligible = [...ns.tabs.values()].filter((t) => {
    const until = ns.failedUntil.get(t.tabId) ?? 0;
    if (until > now) return false;
    ns.failedUntil.delete(t.tabId);
    return true;
  });
  if (eligible.length === 0) return null;
  // A tab that still holds a live fenced worker (broker restart, rehello)
  // beats everyone: re-promoting it avoids reopening the OPFS pool at all.
  const holding = eligible.filter((t) => t.heldLeadership !== null);
  const base = holding.length > 0 ? holding : eligible;
  const visible = base.filter((t) => t.visibility === 'visible');
  const pool = visible.length > 0 ? visible : base;
  return pool.reduce((best, t) => {
    if (t.lastVisibleAt !== best.lastVisibleAt) {
      return t.lastVisibleAt > best.lastVisibleAt ? t : best;
    }
    return t.tabId > best.tabId ? t : best;
  });
}

interface ClearedLeader {
  tabId: string;
  leadershipId: number;
  workerLock: string | null;
}

function clearLeader(
  ns: Namespace,
  opts: { demote: boolean; removeTab: boolean }
): ClearedLeader | null {
  const leader = ns.leader;
  if (!leader) return null;
  ns.leader = null;
  clearPromotionTimer(ns);
  ns.tabLockMonitor?.abort();
  ns.tabLockMonitor = null;
  const tab = ns.tabs.get(leader.tabId);
  const workerLock = tab?.heldLeadership?.workerLockName ?? null;
  if (tab && opts.demote) {
    post(tab.port, { type: 'demote', brokerInstanceId, leadershipId: leader.leadershipId });
  }
  // Its port stays open: the only caller is the tab-lock monitor, and a lock
  // released is not always a dead tab. A leader that signs in or out drops
  // its lock before re-helloing into the new bucket, and closing the port
  // here swallowed that hello, leaving the tab roleless until its watchdog
  // reconnected. A dead tab's port is gone anyway.
  if (opts.removeTab && tab) removeTab(ns, leader.tabId, { notifyLeader: false, closePort: false });
  // Followers must drop their ports; new ones are minted after re-election.
  for (const t of ns.tabs.values()) {
    if (t.tabId !== leader.tabId) {
      post(t.port, {
        type: 'close-follower-ports',
        brokerInstanceId,
        leadershipId: leader.leadershipId,
      });
    }
  }
  for (const [, retry] of ns.attachRetry) if (retry.timer) clearTimeout(retry.timer);
  ns.attachRetry.clear();
  return { tabId: leader.tabId, leadershipId: leader.leadershipId, workerLock };
}

function clearPromotionTimer(ns: Namespace): void {
  if (ns.promotionTimer) clearTimeout(ns.promotionTimer);
  ns.promotionTimer = null;
}

/**
 * A promotion did not work out: back the tab off, clear the leadership, and
 * re-elect. Shared by the tab's own `leader-failed` report and by the
 * {@link PROMOTION_TIMEOUT_MS} deadline, so a tab that hangs mid-promotion
 * costs the namespace the same 1s backoff as one that fails loudly, instead of
 * owning it forever.
 *
 * Every reason counts toward `failedCycles`. Gating that on 'opfs-unavailable'
 * left any other recurring failure (e.g. a tab lock nobody frees) looping at
 * the backoff interval forever, with allowMemoryFallback never granted, so
 * every tab timed out in start() and booted solo - each one then contending
 * for the OPFS pool on its own, which is exactly what shared-tabs exists to
 * stop.
 */
function failPromotion(
  ns: Namespace,
  tabId: string,
  leadershipId: number,
  opts: { timedOut: boolean }
): void {
  if (ns.leader?.tabId !== tabId || ns.leader.leadershipId !== leadershipId) return;
  ns.failedCycles += 1;
  ns.failedUntil.set(tabId, Date.now() + LEADER_FAILURE_BACKOFF_MS);
  const previous = clearLeader(ns, { demote: false, removeTab: false });
  const tab = ns.tabs.get(tabId);
  if (tab) tab.heldLeadership = null;
  // A tab that reported `leader-failed` has already given back its tab lock and
  // any worker it opened, so there is nothing for the next candidate to wait
  // on. A tab that went silent may have opened the OPFS pool and never told us,
  // so hand the next election the lock name it would be holding: the probe in
  // `waitForPreviousLeaderLocks` returns immediately when it is free, and
  // steals it when it is not. `heldLeadership` is deliberately NOT used for
  // this - it means "confirmed leader" and drives the broker-restart resume.
  const stranded =
    opts.timedOut && previous
      ? {
          ...previous,
          workerLock: `sp00ky-tabs:${ns.fingerprint}:${ns.bucketId}:worker:${leadershipId}`,
        }
      : previous;
  electIfNeeded(ns, stranded);
}

/**
 * Fast crash detection: queue a Web Lock request on the leader's tab lock.
 * Locks release the instant their holder's tab dies, so being GRANTED means
 * the leader is gone from this bucket (dead, or moved to another bucket on
 * sign-in/out); release immediately (return from the callback) and re-elect.
 * The leadershipId guard drops stale grants after a normal handoff.
 */
function startTabLockMonitor(ns: Namespace, leadershipId: number): void {
  const locks = (navigator as { locks?: LockManager }).locks;
  if (!locks) return;
  ns.tabLockMonitor?.abort();
  const controller = new AbortController();
  ns.tabLockMonitor = controller;
  const name = `sp00ky-tabs:${ns.fingerprint}:${ns.bucketId}:tab`;
  locks
    .request(name, { mode: 'exclusive', signal: controller.signal }, () => {
      if (ns.leader?.leadershipId !== leadershipId) return;
      const previous = clearLeader(ns, { demote: true, removeTab: true });
      electIfNeeded(ns, previous);
    })
    .catch(() => {
      /* aborted (normal handoff) or Web Locks hiccup; pong timeout covers it */
    });
}

/** Probe whether a lock name is free by acquiring-and-releasing it. */
async function lockIsFree(name: string): Promise<boolean> {
  const locks = (navigator as { locks?: LockManager }).locks;
  if (!locks) return true;
  try {
    return await locks.request(name, { mode: 'exclusive', ifAvailable: true }, (lock) =>
      Promise.resolve(lock !== null)
    );
  } catch {
    return true;
  }
}

async function stealLock(name: string): Promise<void> {
  const locks = (navigator as { locks?: LockManager }).locks;
  if (!locks) return;
  try {
    await locks.request(name, { mode: 'exclusive', steal: true }, () => Promise.resolve());
  } catch {
    /* best effort */
  }
}

/**
 * Before promoting a replacement, give the previous leader's locks a chance to
 * free naturally (dead tab: released instantly; frozen tab: never). Returns
 * whether a steal was needed, which the new leader passes to its worker open.
 */
async function waitForPreviousLeaderLocks(previous: ClearedLeader | null): Promise<boolean> {
  if (!previous?.workerLock) return false;
  if (await lockIsFree(previous.workerLock)) return false;
  await new Promise((r) => setTimeout(r, FORCE_TAKEOVER_TIMEOUT_MS));
  if (await lockIsFree(previous.workerLock)) return false;
  await stealLock(previous.workerLock);
  return true;
}

function electIfNeeded(ns: Namespace, previous: ClearedLeader | null = null): void {
  if (ns.electing || ns.leader || ns.tabs.size === 0) return;
  ns.electing = true;
  void (async () => {
    try {
      const forceTakeover = await waitForPreviousLeaderLocks(previous);
      // State may have moved while we waited.
      if (ns.leader || ns.tabs.size === 0) return;
      const candidate = pickCandidate(ns);
      if (!candidate) {
        // Everyone is in failure backoff; retry when the earliest expires.
        const soonest = Math.min(...[...ns.failedUntil.values()], Date.now() + LEADER_FAILURE_BACKOFF_MS);
        setTimeout(() => electIfNeeded(ns), Math.max(50, soonest - Date.now()));
        return;
      }
      // A tab that survived a broker restart and still holds its lock keeps
      // its worker; it only rolls the lock name forward to the new id.
      const resumeHeld = candidate.heldLeadership !== null;
      const leadershipId = ++nextLeadershipId;
      ns.leader = { tabId: candidate.tabId, leadershipId, ready: false };
      post(candidate.port, {
        type: 'become-leader',
        brokerInstanceId,
        leadershipId,
        forceTakeover,
        allowMemoryFallback: ns.failedCycles >= FAILED_CYCLES_BEFORE_MEMORY,
        resumeHeld,
      });
      clearPromotionTimer(ns);
      ns.promotionTimer = setTimeout(() => {
        ns.promotionTimer = null;
        failPromotion(ns, candidate.tabId, leadershipId, { timedOut: true });
      }, PROMOTION_TIMEOUT_MS);
    } finally {
      ns.electing = false;
    }
  })();
}

// ---- follower attachment ------------------------------------------------------

function assignFollowerPorts(ns: Namespace): void {
  const leader = ns.leader;
  if (!leader || !leader.ready) return;
  const leaderTab = ns.tabs.get(leader.tabId);
  if (!leaderTab) return;
  for (const tab of ns.tabs.values()) {
    if (tab.tabId === leader.tabId) continue;
    if (ns.attachRetry.has(tab.tabId)) continue;
    mintPorts(ns, leaderTab, tab, leader.leadershipId);
  }
}

function mintPorts(ns: Namespace, leaderTab: BrokerTab, follower: BrokerTab, leadershipId: number): void {
  const db = new MessageChannel();
  const sync = new MessageChannel();
  post(
    leaderTab.port,
    {
      type: 'attach-follower-ports',
      brokerInstanceId,
      leadershipId,
      followerTabId: follower.tabId,
    },
    [db.port1, sync.port1]
  );
  post(
    follower.port,
    { type: 'use-follower-ports', brokerInstanceId, leadershipId, leaderTabId: leaderTab.tabId },
    [db.port2, sync.port2]
  );
  // Re-mint with backoff until the leader confirms (or leadership changes).
  // 1s doubling to 30s, duplicated from protocol constants.
  const state = ns.attachRetry.get(follower.tabId) ?? { count: 0, timer: null };
  const delay = Math.min(1000 * 2 ** state.count, 30_000);
  state.count += 1;
  state.timer = setTimeout(() => {
    const cur = ns.attachRetry.get(follower.tabId);
    if (!cur) return;
    cur.timer = null;
    const stillLeader = ns.leader && ns.leader.leadershipId === leadershipId && ns.leader.ready;
    const stillHere = ns.tabs.get(follower.tabId);
    const leaderNow = stillLeader ? ns.tabs.get(ns.leader!.tabId) : undefined;
    if (stillLeader && stillHere && leaderNow) mintPorts(ns, leaderNow, stillHere, leadershipId);
    else ns.attachRetry.delete(follower.tabId);
  }, delay);
  ns.attachRetry.set(follower.tabId, state);
}

function followerAttached(ns: Namespace, followerTabId: string): void {
  const retry = ns.attachRetry.get(followerTabId);
  if (retry?.timer) clearTimeout(retry.timer);
  ns.attachRetry.delete(followerTabId);
}

// ---- liveness -----------------------------------------------------------------

function ensurePingTimer(): void {
  if (pingTimer) return;
  pingTimer = setInterval(() => {
    const now = Date.now();
    for (const ns of namespaces.values()) {
      for (const tab of [...ns.tabs.values()]) {
        if (now - tab.lastPongAt > PONG_TIMEOUT_MS) {
          evictTab(ns, tab.tabId, 'missed pongs');
          continue;
        }
        post(tab.port, { type: 'ping', brokerInstanceId });
      }
    }
    stopPingTimerIfIdle();
  }, PING_INTERVAL_MS);
}

function stopPingTimerIfIdle(): void {
  if (pingTimer && [...namespaces.values()].every((ns) => ns.tabs.size === 0)) {
    clearInterval(pingTimer);
    pingTimer = null;
    namespaces.clear();
    canonicalFingerprint = null;
  }
}

function removeTab(
  ns: Namespace,
  tabId: string,
  opts: { notifyLeader: boolean; closePort?: boolean }
): void {
  const tab = ns.tabs.get(tabId);
  if (!tab) return;
  ns.tabs.delete(tabId);
  portTab.delete(tab.port);
  // A re-hello (bucket switch, reconnect) re-homes the tab on the SAME port;
  // closing it would sever the still-connected tab. Only a real departure
  // (shutdown, eviction) closes.
  if (opts.closePort !== false) {
    try {
      tab.port.close();
    } catch {
      /* ignore */
    }
  }
  const retry = ns.attachRetry.get(tabId);
  if (retry?.timer) clearTimeout(retry.timer);
  ns.attachRetry.delete(tabId);
  if (opts.notifyLeader && ns.leader && ns.leader.tabId !== tabId) {
    const leaderTab = ns.tabs.get(ns.leader.tabId);
    if (leaderTab) {
      post(leaderTab.port, {
        type: 'close-follower-ports',
        brokerInstanceId,
        leadershipId: ns.leader.leadershipId,
      });
    }
  }
}

function evictTab(
  ns: Namespace,
  tabId: string,
  reason: string,
  opts: { closePort?: boolean } = {}
): void {
  const wasLeader = ns.leader?.tabId === tabId;
  const previous = wasLeader ? clearLeader(ns, { demote: true, removeTab: false }) : null;
  removeTab(ns, tabId, { notifyLeader: !wasLeader, closePort: opts.closePort });
  void reason;
  if (wasLeader) electIfNeeded(ns, previous);
  stopPingTimerIfIdle();
}

// ---- message handling -----------------------------------------------------------

function handleTabMessage(port: MessagePort, msg: TabToBrokerMessage, ports: readonly MessagePort[]): void {
  void ports;
  if (msg.type === 'hello') {
    if (canonicalFingerprint === null) canonicalFingerprint = msg.fingerprint;
    if (msg.fingerprint !== canonicalFingerprint) {
      post(port, { type: 'unsupported', brokerInstanceId, reason: 'fingerprint-mismatch' });
      return;
    }
    // Drop stale state for this tab EVERYWHERE first. A re-hello is either a
    // reconnect (same namespace) or a bucket switch (leave old namespace,
    // join the new one); in both cases exactly one entry may remain, and if
    // the tab led its old namespace that namespace needs a new leader now,
    // not after a 15s pong eviction.
    for (const other of [...namespaces.values()]) {
      if (other.tabs.has(msg.tabId)) evictTab(other, msg.tabId, 'rehello', { closePort: false });
    }
    // Only now look the namespace up: evicting the broker's last tab resets it
    // as idle (all namespaces and the fingerprint dropped), and a namespace
    // fetched before that would be orphaned, never pinged or found again. A
    // lone tab signing in lost its leader that way until its watchdog
    // reconnected.
    canonicalFingerprint ??= msg.fingerprint;
    const ns = getNamespace(msg.fingerprint, msg.bucketId);
    const tab: BrokerTab = {
      port,
      tabId: msg.tabId,
      fingerprint: msg.fingerprint,
      bucketId: msg.bucketId,
      visibility: msg.visibility,
      lastVisibleAt: msg.visibility === 'visible' ? Date.now() : 0,
      lastPongAt: Date.now(),
      heldLeadership: msg.heldLeadership,
    };
    ns.tabs.set(msg.tabId, tab);
    portTab.set(port, { ns, tabId: msg.tabId });
    ensurePingTimer();
    post(port, { type: 'broker-hello', brokerInstanceId });
    if (ns.leader?.ready) {
      post(port, {
        type: 'leader-ready',
        brokerInstanceId,
        leadershipId: ns.leader.leadershipId,
        leaderTabId: ns.leader.tabId,
      });
      assignFollowerPorts(ns);
    } else {
      electIfNeeded(ns);
    }
    return;
  }

  const entry = portTab.get(port);
  if (!entry) return;
  const { ns } = entry;

  switch (msg.type) {
    case 'pong': {
      const tab = ns.tabs.get(msg.tabId);
      if (tab) tab.lastPongAt = Date.now();
      break;
    }
    case 'visibility': {
      const tab = ns.tabs.get(msg.tabId);
      if (tab) {
        tab.visibility = msg.visibility;
        if (msg.visibility === 'visible') tab.lastVisibleAt = Date.now();
        tab.lastPongAt = Date.now();
      }
      break;
    }
    case 'leader-ready': {
      if (ns.leader?.tabId !== msg.tabId || ns.leader.leadershipId !== msg.leadershipId) {
        // Stale promotion (a newer election superseded it): demote it.
        post(port, { type: 'demote', brokerInstanceId, leadershipId: msg.leadershipId });
        break;
      }
      ns.leader.ready = true;
      ns.failedCycles = 0;
      clearPromotionTimer(ns);
      startTabLockMonitor(ns, msg.leadershipId);
      const tab = ns.tabs.get(msg.tabId);
      if (tab) {
        // The leader now holds the per-leadership worker lock; remember it so
        // a broker restart (or its next takeover) can find it.
        tab.heldLeadership = {
          leadershipId: msg.leadershipId,
          workerLockName: `sp00ky-tabs:${ns.fingerprint}:${ns.bucketId}:worker:${msg.leadershipId}`,
        };
      }
      for (const t of ns.tabs.values()) {
        post(t.port, {
          type: 'leader-ready',
          brokerInstanceId,
          leadershipId: msg.leadershipId,
          leaderTabId: msg.tabId,
        });
      }
      assignFollowerPorts(ns);
      break;
    }
    case 'leader-failed': {
      failPromotion(ns, msg.tabId, msg.leadershipId, { timedOut: false });
      break;
    }
    case 'follower-port-attached': {
      // Leader confirmed the follower's ports are live: stop the re-mint loop.
      if (ns.leader?.tabId === msg.tabId && ns.leader.leadershipId === msg.leadershipId) {
        followerAttached(ns, msg.followerTabId);
      }
      break;
    }
    case 'follower-port-closed': {
      // Leader reports a follower's data port died: re-mint on the retry path.
      if (ns.leader?.tabId !== msg.tabId) break;
      followerAttached(ns, msg.followerTabId);
      const leaderTab = ns.tabs.get(ns.leader.tabId);
      const follower = ns.tabs.get(msg.followerTabId);
      if (leaderTab && follower && ns.leader.ready) {
        mintPorts(ns, leaderTab, follower, ns.leader.leadershipId);
      }
      break;
    }
    case 'shutdown': {
      evictTab(ns, msg.tabId, 'shutdown');
      break;
    }
  }
}

/** Attach a connecting tab's port. Exported for the node test harness, which
 *  drives the broker with fake MessagePort pairs instead of a SharedWorker. */
export function handleConnect(port: MessagePort): void {
  port.onmessage = (ev: MessageEvent) => {
    handleTabMessage(port, ev.data as TabToBrokerMessage, (ev.ports ?? []) as MessagePort[]);
  };
  port.onmessageerror = () => {
    const entry = portTab.get(port);
    if (entry) evictTab(entry.ns, entry.tabId, 'messageerror');
  };
  port.start?.();
}

/** Test-only: wipe module state so each test starts with a fresh broker. */
export function __resetBrokerForTests(): void {
  if (pingTimer) clearInterval(pingTimer);
  pingTimer = null;
  for (const ns of namespaces.values()) {
    for (const [, retry] of ns.attachRetry) if (retry.timer) clearTimeout(retry.timer);
  }
  namespaces.clear();
  portTab.clear();
  canonicalFingerprint = null;
  nextLeadershipId = 0;
}

// `self` only exists in a real worker scope; the node test harness imports
// this module and calls handleConnect directly.
if (typeof self !== 'undefined') {
  (self as unknown as SharedWorkerGlobalScope).onconnect = (event: MessageEvent) => {
    const port = event.ports[0];
    if (port) handleConnect(port);
  };
}
