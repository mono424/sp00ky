/**
 * TabsCoordinator: the per-tab role state machine for shared-tabs mode. Sits
 * between the broker client (election events, ports) and the rest of the
 * client (engine + sync), which it drives exclusively through
 * {@link CoordinatorHooks} so this module depends on neither concrete class.
 *
 * Roles:
 * - leader: owns the sqlite worker (OPFS) and the sync loop; serves follower
 *   ports handed over by the broker.
 * - follower: LocalStore ops go over dbPort into the leader's worker; sync
 *   concerns are forwarded over syncPort (see {@link SyncForwarder}).
 * - solo: broker unavailable/rejected; exactly the pre-shared-tabs behavior.
 *
 * Bucket switches are namespace moves: `moveToBucket` re-hellos under the new
 * bucketId and the ordinary election machinery assigns the new role there.
 */
import type { Logger } from '../logger/index';
import type { StorageHealth } from '../../types';
import { TabBrokerClient } from './broker-client';
import { acquireLeaderTabLock, type LeaderLockHandle } from './leader-locks';
import {
  tabLockName,
  workerLockName,
  type FollowerToLeaderMessage,
  type IngestTuple,
  type LeaderToFollowerMessage,
  type TabId,
  type TabRole,
} from './protocol';

/** How long boot waits for a role (election + attach) before going solo. */
const START_TIMEOUT_MS = 15_000;

export interface CoordinatorHooks {
  /** Become the store owner: spawn the worker, open the pool under the given
   *  per-leadership lock. Returns the resulting health for db-ready relays.
   *  `resumeHeld` = keep the existing live worker (broker-restart path). */
  adoptOwner(
    bucketId: string,
    opts: { workerLockName: string; allowMemoryFallback: boolean; forceTakeover: boolean; resumeHeld: boolean }
  ): Promise<StorageHealth>;
  /** Attach to a leader's worker through `dbPort`. */
  adoptAttached(
    dbPort: MessagePort,
    snapshot: { bucketId: string; storageHealth: StorageHealth; leadershipId: number }
  ): Promise<void>;
  /** We were leader and got demoted (zombie thaw / stale promotion): tear the
   *  owned worker down so its SAH handles free up. */
  releaseOwnership(): Promise<void>;
  /** The leader (or its port) is gone; queue/park ops until a new role lands. */
  onLeaderLost(reason: string): void;
  /** Leader side: forward a follower's dbPort into the owned worker. */
  exposeClientPort(clientId: string, port: MessagePort): Promise<void>;
  removeClientPort(clientId: string): Promise<void>;
  /** Sync layer role changes (implemented in the sync module). */
  /** Synchronous leader WIRING. Must not block: the role is published and
   *  `leader-ready` is sent right after it returns. */
  becomeSyncLeader(hub: LeaderSyncHub): void;
  /** The blocking half of the leader handover (outbox drain, LIVE restart),
   *  run in the background once the tab is already a working leader. */
  resumeSyncLeaderDuties(): Promise<void>;
  becomeSyncFollower(forwarder: SyncForwarder): void;
  becomeSyncSolo(): void;
  /** Current storage health, for db-ready sent to late-joining followers. */
  currentStorageHealth(): StorageHealth;
}

// ---- leader side: per-follower syncPort hub ---------------------------------

export interface FollowerChannel {
  tabId: TabId;
  send(msg: LeaderToFollowerMessage): void;
}

/** Leader-side fan-out surface handed to the sync layer. The sync router
 *  (modules/sync/tab-router.ts) registers itself as the message handler. */
export class LeaderSyncHub {
  private followers = new Map<TabId, MessagePort>();
  private seq = 0;
  onFollowerMessage:
    | ((tabId: TabId, msg: FollowerToLeaderMessage) => void)
    | null = null;
  onFollowerDetached: ((tabId: TabId) => void) | null = null;

  constructor(
    readonly leadershipId: number,
    private logger: Logger
  ) {}

  attach(tabId: TabId, port: MessagePort): void {
    this.detach(tabId);
    this.followers.set(tabId, port);
    port.onmessage = (ev: MessageEvent) => {
      this.onFollowerMessage?.(tabId, ev.data as FollowerToLeaderMessage);
    };
    port.onmessageerror = () => this.detach(tabId);
    port.start?.();
  }

  detach(tabId: TabId): void {
    const port = this.followers.get(tabId);
    if (!port) return;
    this.followers.delete(tabId);
    try {
      port.close();
    } catch {
      /* ignore */
    }
    this.onFollowerDetached?.(tabId);
  }

  detachAll(): void {
    for (const tabId of [...this.followers.keys()]) this.detach(tabId);
  }

  sendTo(tabId: TabId, msg: LeaderToFollowerMessage): void {
    try {
      this.followers.get(tabId)?.postMessage(msg);
    } catch {
      /* dead port; broker re-mints */
    }
  }

  broadcast(msg: LeaderToFollowerMessage, exceptTabId?: TabId): void {
    for (const [tabId, port] of this.followers) {
      if (tabId === exceptTabId) continue;
      try {
        port.postMessage(msg);
      } catch {
        /* ignore */
      }
    }
  }

  /** Stamped ingest relay; seq lets followers detect gaps. */
  relayIngest(tuples: IngestTuple[], exceptTabId?: TabId): void {
    if (this.followers.size === 0) return;
    this.broadcast(
      { type: 'ingest-relay', tuples, leadershipId: this.leadershipId, seq: ++this.seq },
      exceptTabId
    );
  }

  get followerCount(): number {
    return this.followers.size;
  }

  get relayedBatches(): number {
    return this.seq;
  }
}

// ---- follower side: syncPort forwarder ---------------------------------------

/** Follower half of the syncPort. Queues while detached (leaderless window)
 *  and flushes on rebind; a lost-in-flight mutation notify is additionally
 *  backstopped by the new leader reloading the shared outbox from the store. */
export class SyncForwarder {
  private port: MessagePort | null = null;
  private queued: FollowerToLeaderMessage[] = [];
  onLeaderMessage: ((msg: LeaderToFollowerMessage) => void) | null = null;

  constructor(private tabId: TabId) {}

  rebind(port: MessagePort): void {
    this.unbind();
    this.port = port;
    port.onmessage = (ev: MessageEvent) => {
      this.onLeaderMessage?.(ev.data as LeaderToFollowerMessage);
    };
    port.start?.();
    this.post({ type: 'sync-hello', tabId: this.tabId });
    const backlog = this.queued;
    this.queued = [];
    for (const msg of backlog) this.post(msg);
  }

  unbind(): void {
    if (this.port) {
      try {
        this.port.close();
      } catch {
        /* ignore */
      }
    }
    this.port = null;
  }

  private post(msg: FollowerToLeaderMessage): void {
    if (!this.port) {
      this.queued.push(msg);
      return;
    }
    try {
      this.port.postMessage(msg);
    } catch {
      this.queued.push(msg);
    }
  }

  mutationEnqueued(mutationId: string): void {
    this.post({ type: 'mutation-enqueued', mutationId });
  }
  /** An optimistic write this tab just ingested. Deliberately NOT queued while
   *  detached: a new leader primes its circuit from the shared store, which
   *  already holds the row, and replaying a stale tuple at it later would put
   *  an older `_00_rv` in its version memo. */
  ingest(tuples: IngestTuple[]): void {
    if (!this.port) return;
    try {
      this.port.postMessage({ type: 'ingest', tuples });
    } catch {
      /* dead port; broker re-mints */
    }
  }
  requestPoll(): void {
    this.post({ type: 'request-poll' });
  }
}

// ---- the coordinator ----------------------------------------------------------

export class TabsCoordinator {
  role: TabRole = 'solo';
  leadershipId = 0;
  leaderTabId: TabId | null = null;
  brokerRole: 'pending' | TabRole = 'pending';
  /** Wall time from `become-leader` to `leader-ready` on the last promotion.
   *  Reported in the DevTools role-change event: a slow store adopt (OPFS pool
   *  contention after the previous leader died) shows up here as seconds. */
  lastPromotionMs: number | null = null;
  private fingerprint: string;
  private bucketId: string;
  private broker: TabBrokerClient;
  private hub: LeaderSyncHub | null = null;
  private forwarder: SyncForwarder | null = null;
  private roleListeners = new Set<(role: TabRole) => void>();
  private startResolve: ((role: TabRole) => void) | null = null;
  private startReject: ((e: Error) => void) | null = null;
  /** Ports received before start() resolves or between roles. */
  private promotionChain: Promise<void> = Promise.resolve();
  private closed = false;

  constructor(
    private deps: {
      tabId: TabId;
      fingerprint: string;
      hooks: CoordinatorHooks;
      logger: Logger;
      /** Fired on pagehide while this tab leads: last-chance OPFS release. */
      onLeaderPageHide?: () => void;
    }
  ) {
    this.fingerprint = deps.fingerprint;
    this.bucketId = 'anon';
    // The URL is built HERE (same directory as the worker source) so the
    // published flat bundle's rewritten './tabs-broker-worker.js' resolves at
    // the dist top level, exactly like the sqlite worker URL does.
    this.broker = new TabBrokerClient(
      new URL('./tabs-broker-worker.ts', import.meta.url),
      deps.tabId,
      {
        onBecomeLeader: (msg) => this.enqueue(() => this.promote(msg)),
        onDemote: (leadershipId) => this.enqueue(() => this.demote(leadershipId)),
        onLeaderReady: (leadershipId, leaderTabId) => {
          this.leaderTabId = leaderTabId;
          void leadershipId;
        },
        onAttachFollowerPorts: (followerTabId, leadershipId, dbPort, syncPort) =>
          this.enqueue(() => this.serveFollower(followerTabId, leadershipId, dbPort, syncPort)),
        onUseFollowerPorts: (leaderTabId, leadershipId, dbPort, syncPort) =>
          this.enqueue(() => this.attachToLeader(leaderTabId, leadershipId, dbPort, syncPort)),
        onCloseFollowerPorts: (leadershipId) =>
          this.enqueue(() => this.handleLeaderGone(leadershipId)),
        onUnsupported: () => this.fallbackToSolo('broker rejected this tab'),
        onBrokerRestarted: () => {
          // Direct MessageChannels survive a broker restart; roles get
          // re-confirmed by the fresh election (heldLeadership fast path).
        },
      },
      deps.logger
    );
    if (typeof window !== 'undefined' && deps.onLeaderPageHide) {
      window.addEventListener('pagehide', () => {
        if (this.role === 'leader') deps.onLeaderPageHide?.();
      });
    }
  }

  /** Serialize role transitions; each is small but async (worker opens). */
  private enqueue(fn: () => Promise<void>): void {
    this.promotionChain = this.promotionChain.then(fn, fn).catch((e) => {
      this.deps.logger.error(
        { err: e, Category: 'sp00ky-client::TabsCoordinator' },
        'Role transition failed'
      );
    });
  }

  onRoleChange(cb: (role: TabRole) => void): () => void {
    this.roleListeners.add(cb);
    return () => this.roleListeners.delete(cb);
  }

  private setRole(role: TabRole): void {
    if (this.role !== role) {
      this.role = role;
      this.deps.logger.info(
        {
          role,
          leadershipId: this.leadershipId,
          promotionMs: role === 'leader' ? this.lastPromotionMs : undefined,
          Category: 'sp00ky-client::TabsCoordinator',
        },
        'Tab role changed'
      );
      for (const cb of this.roleListeners) cb(role);
    }
    if (this.startResolve && role !== 'solo') {
      this.startResolve(role);
      this.startResolve = null;
    }
  }

  /** Connect the broker and resolve once this tab has a usable store role.
   *  Rejects when no role lands in time; the caller then boots solo. */
  start(bucketId: string): Promise<TabRole> {
    this.bucketId = bucketId;
    return new Promise<TabRole>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.startResolve = null;
        this.startReject = null;
        reject(new Error('shared-tabs: no role assigned in time'));
      }, START_TIMEOUT_MS);
      this.startResolve = (role) => {
        clearTimeout(timeout);
        this.startReject = null;
        resolve(role);
      };
      this.startReject = (e) => {
        clearTimeout(timeout);
        this.startResolve = null;
        reject(e);
      };
      this.broker
        .connect({
          fingerprint: this.fingerprint,
          bucketId,
          heldLeadership: () =>
            this.role === 'leader'
              ? {
                  leadershipId: this.leadershipId,
                  workerLockName: workerLockName(this.fingerprint, this.bucketId, this.leadershipId),
                }
              : null,
        })
        .catch((e) => {
          this.startReject?.(e instanceof Error ? e : new Error(String(e)));
        });
    });
  }

  /** Bucket switch: leave the old namespace, join the new one. Resolves when
   *  a role lands in the new namespace. */
  moveToBucket(bucketId: string): Promise<TabRole> {
    return new Promise<TabRole>((resolve, reject) => {
      this.enqueue(async () => {
        // Tear down the old role locally; the broker's rehello handling evicts
        // us from the old namespace and re-elects there.
        if (this.role === 'leader') await this.teardownLeader();
        else if (this.role === 'follower') this.teardownFollower('bucket switch');
        // Ownership ended with the old bucket. Keeping `leader` here makes
        // attachToLeader reject the new bucket's follower ports when another
        // tab already owns it, leaving sign-in waiting until a reload.
        this.setRole('solo');
        this.leaderTabId = null;
        this.bucketId = bucketId;
        const timeout = setTimeout(() => {
          this.startResolve = null;
          this.startReject = null;
          reject(new Error('shared-tabs: no role assigned after bucket switch'));
        }, START_TIMEOUT_MS);
        this.startResolve = (role) => {
          clearTimeout(timeout);
          this.startReject = null;
          resolve(role);
        };
        this.startReject = (e) => {
          clearTimeout(timeout);
          this.startResolve = null;
          reject(e);
        };
        await this.broker.rehello({
          fingerprint: this.fingerprint,
          bucketId,
          heldLeadership: () => null,
        });
      });
    });
  }

  // ---- transitions -----------------------------------------------------------

  private async promote(msg: {
    leadershipId: number;
    forceTakeover: boolean;
    allowMemoryFallback: boolean;
    resumeHeld: boolean;
  }): Promise<void> {
    if (this.closed) return;
    if (msg.leadershipId <= this.leadershipId && !msg.resumeHeld) return;
    const previousRole = this.role;
    const startedAt = Date.now();
    try {
      if (previousRole === 'follower') this.teardownFollower('promoted');
      this.leadershipId = msg.leadershipId;
      // The tab lock is the broker's CRASH detector: it queues a request on
      // this name, and being granted means this tab died (locks release on tab
      // death instantly, unlike the 15s pong timeout). Steal only when the
      // broker said the previous holder is a frozen zombie.
      const lock = await acquireLeaderTabLock(tabLockName(this.fingerprint, this.bucketId), {
        steal: msg.forceTakeover,
      });
      if (!lock) throw new Error('leader tab lock unavailable');
      this.tabLock?.release();
      this.tabLock = lock;
      lock.onLost(() => {
        // Stolen from under us (we were presumed dead): resign.
        this.enqueue(async () => {
          if (this.leadershipId !== msg.leadershipId || this.role !== 'leader') return;
          await this.teardownLeader();
          this.deps.hooks.onLeaderLost('tab lock stolen');
        });
      });
      const health = await this.deps.hooks.adoptOwner(this.bucketId, {
        workerLockName: workerLockName(this.fingerprint, this.bucketId, msg.leadershipId),
        allowMemoryFallback: msg.allowMemoryFallback,
        forceTakeover: msg.forceTakeover,
        resumeHeld: msg.resumeHeld,
      });
      void health;
      this.hub = new LeaderSyncHub(msg.leadershipId, this.deps.logger);
      // Owning the store IS being the leader: it unparks this tab's ops and is
      // everything a follower's ports need. Publish the role and tell the
      // broker BEFORE any sync work. Waiting on the outbox drain and the LIVE
      // restart here once left a tab that owned the OPFS pool but never sent
      // `leader-ready`, so the broker minted no follower ports and re-elected
      // no one (`assignFollowerPorts` and `electIfNeeded` both bail on that
      // state) and the whole namespace stayed leaderless until a reload. Those
      // duties are best-effort anyway: the list_ref poll covers a missing LIVE.
      this.deps.hooks.becomeSyncLeader(this.hub);
      this.lastPromotionMs = Date.now() - startedAt;
      this.setRole('leader');
      this.broker.send({
        type: 'leader-ready',
        tabId: this.deps.tabId,
        bucketId: this.bucketId,
        leadershipId: msg.leadershipId,
      });
      // Off the promotion chain on purpose: a hang here must not block the
      // next role transition (demote, bucket switch) for the life of the tab.
      void this.deps.hooks.resumeSyncLeaderDuties().catch((e) => {
        this.deps.logger.error(
          { err: e, Category: 'sp00ky-client::TabsCoordinator' },
          'Leader duties failed after promotion'
        );
      });
    } catch (e) {
      const reason = e instanceof Error ? e.message : String(e);
      this.deps.logger.error(
        { err: e, Category: 'sp00ky-client::TabsCoordinator' },
        'Promotion failed'
      );
      this.hub?.detachAll();
      this.hub = null;
      // Give back everything this attempt claimed. The broker does NOT demote a
      // tab whose promotion failed (leader-failed clears leadership without a
      // demote), so nothing else ever frees these. The tab lock name is shared
      // per namespace, so keeping it after failing to lead makes EVERY later
      // election in this namespace fail with 'leader tab lock unavailable' —
      // one OPFS-busy promotion would wedge the whole app into solo mode.
      if (previousRole === 'leader') await this.deps.hooks.releaseOwnership();
      this.tabLock?.release();
      this.tabLock = null;
      this.broker.send({
        type: 'leader-failed',
        tabId: this.deps.tabId,
        bucketId: this.bucketId,
        leadershipId: msg.leadershipId,
        reason,
      });
    }
  }

  private async demote(leadershipId: number): Promise<void> {
    if (this.role !== 'leader' || leadershipId !== this.leadershipId) return;
    await this.teardownLeader();
    this.deps.hooks.onLeaderLost('demoted');
    // Stay roleless; the broker sends use-follower-ports (or become-leader)
    // for whatever comes next.
  }

  private tabLock: LeaderLockHandle | null = null;

  private async teardownLeader(): Promise<void> {
    this.hub?.detachAll();
    this.hub = null;
    await this.deps.hooks.releaseOwnership();
    this.tabLock?.release();
    this.tabLock = null;
  }

  private teardownFollower(reason: string): void {
    this.forwarder?.unbind();
    this.deps.hooks.onLeaderLost(reason);
  }

  private async serveFollower(
    followerTabId: TabId,
    leadershipId: number,
    dbPort: MessagePort,
    syncPort: MessagePort
  ): Promise<void> {
    if (this.role !== 'leader' || leadershipId !== this.leadershipId || !this.hub) {
      dbPort.close();
      syncPort.close();
      return;
    }
    await this.deps.hooks.exposeClientPort(followerTabId, dbPort);
    this.hub.attach(followerTabId, syncPort);
    this.hub.sendTo(followerTabId, {
      type: 'db-ready',
      leadershipId,
      bucketId: this.bucketId,
      storageHealth: this.deps.hooks.currentStorageHealth(),
    });
    this.broker.send({
      type: 'follower-port-attached',
      tabId: this.deps.tabId,
      bucketId: this.bucketId,
      leadershipId,
      followerTabId,
    });
  }

  private async attachToLeader(
    leaderTabId: TabId,
    leadershipId: number,
    dbPort: MessagePort,
    syncPort: MessagePort
  ): Promise<void> {
    if (this.closed || this.role === 'leader') {
      dbPort.close();
      syncPort.close();
      return;
    }
    this.leaderTabId = leaderTabId;
    this.leadershipId = leadershipId;
    if (!this.forwarder) this.forwarder = new SyncForwarder(this.deps.tabId);
    // db-ready arrives on the syncPort and carries the snapshot the engine
    // needs; bind sync first, adopt the store on receipt.
    const forwarder = this.forwarder;
    await new Promise<void>((resolve) => {
      let adopted = false;
      let attached = false;
      // Relay traffic that lands between `db-ready` and the sync hooks being
      // installed (the store adopt is async). Held and replayed, not dropped:
      // on a first attach there is no previous handler, so an ingest-relay or
      // settled notice in that window used to vanish.
      const pending: LeaderToFollowerMessage[] = [];
      const previousHandler = forwarder.onLeaderMessage;
      forwarder.onLeaderMessage = (msg) => {
        if (msg.type === 'db-ready' && !adopted) {
          adopted = true;
          void this.deps.hooks
            .adoptAttached(dbPort, {
              bucketId: msg.bucketId,
              storageHealth: msg.storageHealth,
              leadershipId: msg.leadershipId,
            })
            .then(() => {
              this.deps.hooks.becomeSyncFollower(forwarder);
              attached = true;
              const backlog = pending.splice(0);
              for (const m of backlog) forwarder.onLeaderMessage?.(m);
              this.setRole('follower');
              resolve();
            });
          return;
        }
        if (!attached) {
          pending.push(msg);
          return;
        }
        previousHandler?.(msg);
      };
      forwarder.rebind(syncPort);
    });
  }

  private async handleLeaderGone(leadershipId: number): Promise<void> {
    if (this.role === 'leader') return;
    if (leadershipId < this.leadershipId) return;
    // Detached limbo: the engine parks ops via onLeaderLost and the role stays
    // 'follower' (this tab is still in shared mode, just between leaders). The
    // next use-follower-ports or become-leader resolves it either way.
    this.teardownFollower('leader gone');
  }

  private fallbackToSolo(reason: string): void {
    this.deps.logger.warn(
      { reason, Category: 'sp00ky-client::TabsCoordinator' },
      'Shared-tabs unavailable; running solo'
    );
    this.deps.hooks.becomeSyncSolo();
    this.setRole('solo');
    // start() treats solo as a rejection so the caller boots the plain path.
    this.startReject?.(new Error(`shared-tabs unavailable: ${reason}`));
  }

  get syncHub(): LeaderSyncHub | null {
    return this.hub;
  }
  get syncForwarder(): SyncForwarder | null {
    return this.forwarder;
  }
  get tabId(): TabId {
    return this.deps.tabId;
  }

  async stop(): Promise<void> {
    this.closed = true;
    if (this.role === 'leader') await this.teardownLeader();
    else this.forwarder?.unbind();
    this.broker.send({ type: 'shutdown', tabId: this.deps.tabId, bucketId: this.bucketId });
    this.broker.close();
  }
}
