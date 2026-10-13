/**
 * Per-tab connection to the SharedWorker broker. Owns: the hello handshake,
 * pong replies + the ping watchdog (a silent broker means the browser killed
 * the SharedWorker; reconnect with a fresh one), visibility reporting, and the
 * pagehide/freeze shutdown notifications. Role decisions and port handling are
 * the coordinator's job; this class only surfaces them as events.
 */
import type { Logger } from '../logger/index';
import {
  PING_INTERVAL_MS,
  PONG_TIMEOUT_MS,
  type BrokerToTabMessage,
  type HeldLeadership,
  type TabId,
  type TabToBrokerMessage,
  type TabVisibility,
} from './protocol';

export interface BrokerClientEvents {
  onBecomeLeader(msg: {
    leadershipId: number;
    forceTakeover: boolean;
    allowMemoryFallback: boolean;
    resumeHeld: boolean;
  }): void;
  onDemote(leadershipId: number): void;
  onLeaderReady(leadershipId: number, leaderTabId: TabId): void;
  /** Leader side: the broker minted ports for a follower. */
  onAttachFollowerPorts(
    followerTabId: TabId,
    leadershipId: number,
    dbPort: MessagePort,
    syncPort: MessagePort
  ): void;
  /** Follower side: our ports into the current leader. */
  onUseFollowerPorts(
    leaderTabId: TabId,
    leadershipId: number,
    dbPort: MessagePort,
    syncPort: MessagePort
  ): void;
  onCloseFollowerPorts(leadershipId: number): void;
  onUnsupported(reason: string): void;
  /** The broker restarted (instance id changed or pings stopped); the client
   *  already re-helloed. Roles survive only via become-leader/use-ports. */
  onBrokerRestarted(): void;
}

/** What the tab reports about itself on (re)connect. */
export interface BrokerHello {
  fingerprint: string;
  bucketId: string;
  heldLeadership: () => HeldLeadership | null;
}

export class TabBrokerClient {
  private worker: SharedWorker | null = null;
  private brokerInstanceId: string | null = null;
  private watchdog: ReturnType<typeof setInterval> | null = null;
  private lastPingAt = 0;
  private closed = false;
  private helloState: BrokerHello | null = null;

  constructor(
    /** Starts (or joins) the broker SharedWorker. */
    private createWorker: () => SharedWorker,
    readonly tabId: TabId,
    private events: BrokerClientEvents,
    private logger: Logger
  ) {}

  /** Connect and hello. Resolves once broker-hello arrives (or rejects on
   *  timeout / SharedWorker error, in which case the caller goes solo). */
  connect(hello: BrokerHello): Promise<void> {
    this.helloState = hello;
    this.installLifecycleListeners();
    return this.openWorker();
  }

  private openWorker(): Promise<void> {
    if (this.closed || !this.helloState) return Promise.reject(new Error('broker client closed'));
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const settle = (fn: () => void) => {
        if (!settled) {
          settled = true;
          fn();
        }
      };
      let worker: SharedWorker;
      try {
        worker = this.createWorker();
      } catch (e) {
        reject(e);
        return;
      }
      this.worker = worker;
      const timeout = setTimeout(() => settle(() => reject(new Error('broker hello timeout'))), 5000);
      worker.onerror = () => {
        clearTimeout(timeout);
        settle(() => reject(new Error('SharedWorker failed to start')));
      };
      worker.port.onmessage = (ev: MessageEvent) => {
        const msg = ev.data as BrokerToTabMessage;
        if (!msg || typeof msg !== 'object') return;
        if (msg.type === 'broker-hello') {
          clearTimeout(timeout);
          this.noteInstance(msg.brokerInstanceId);
          this.lastPingAt = Date.now();
          this.startWatchdog();
          settle(resolve);
          return;
        }
        this.handleMessage(msg, (ev.ports ?? []) as readonly MessagePort[]);
      };
      worker.port.onmessageerror = () => this.reconnect('messageerror');
      worker.port.start?.();
      this.send({
        type: 'hello',
        tabId: this.tabId,
        fingerprint: this.helloState!.fingerprint,
        bucketId: this.helloState!.bucketId,
        visibility: currentVisibility(),
        heldLeadership: this.helloState!.heldLeadership(),
      });
    });
  }

  private noteInstance(id: string): void {
    if (this.brokerInstanceId !== null && this.brokerInstanceId !== id) {
      this.events.onBrokerRestarted();
    }
    this.brokerInstanceId = id;
  }

  private handleMessage(msg: BrokerToTabMessage, ports: readonly MessagePort[]): void {
    // Any message from a NEW instance implies a restart happened while our
    // watchdog had not fired yet.
    if ('brokerInstanceId' in msg) this.noteInstance(msg.brokerInstanceId);
    switch (msg.type) {
      case 'ping':
        this.lastPingAt = Date.now();
        this.send({ type: 'pong', tabId: this.tabId });
        break;
      case 'become-leader':
        this.events.onBecomeLeader({
          leadershipId: msg.leadershipId,
          forceTakeover: msg.forceTakeover,
          allowMemoryFallback: msg.allowMemoryFallback,
          resumeHeld: msg.resumeHeld,
        });
        break;
      case 'demote':
        this.events.onDemote(msg.leadershipId);
        break;
      case 'leader-ready':
        this.events.onLeaderReady(msg.leadershipId, msg.leaderTabId);
        break;
      case 'attach-follower-ports':
        if (ports.length >= 2) {
          this.events.onAttachFollowerPorts(msg.followerTabId, msg.leadershipId, ports[0], ports[1]);
        }
        break;
      case 'use-follower-ports':
        if (ports.length >= 2) {
          this.events.onUseFollowerPorts(msg.leaderTabId, msg.leadershipId, ports[0], ports[1]);
        }
        break;
      case 'close-follower-ports':
        this.events.onCloseFollowerPorts(msg.leadershipId);
        break;
      case 'unsupported':
        this.logger.warn(
          { reason: msg.reason, Category: 'sp00ky-client::BrokerClient' },
          'Broker rejected this tab; running solo'
        );
        this.events.onUnsupported(msg.reason);
        this.close();
        break;
    }
  }

  send(msg: TabToBrokerMessage): void {
    try {
      this.worker?.port.postMessage(msg);
    } catch {
      /* dead port; watchdog reconnects */
    }
  }

  /** The tab moved to a different bucket: re-hello under the new namespace.
   *  The broker treats a re-hello of a known tabId as leave + rejoin. */
  rehello(hello: BrokerHello): Promise<void> {
    this.helloState = hello;
    if (!this.worker) return this.openWorker();
    this.send({
      type: 'hello',
      tabId: this.tabId,
      fingerprint: hello.fingerprint,
      bucketId: hello.bucketId,
      visibility: currentVisibility(),
      heldLeadership: hello.heldLeadership(),
    });
    return Promise.resolve();
  }

  private startWatchdog(): void {
    if (this.watchdog) return;
    this.watchdog = setInterval(() => {
      if (Date.now() - this.lastPingAt > PING_INTERVAL_MS + PONG_TIMEOUT_MS) {
        this.reconnect('broker pings stopped');
      }
    }, PING_INTERVAL_MS);
  }

  private reconnect(reason: string): void {
    if (this.closed) return;
    this.logger.info(
      { reason, Category: 'sp00ky-client::BrokerClient' },
      'Reconnecting to the tabs broker'
    );
    try {
      this.worker?.port.close();
    } catch {
      /* ignore */
    }
    this.worker = null;
    this.brokerInstanceId = null;
    this.events.onBrokerRestarted();
    void this.openWorker().catch((e) => {
      this.logger.warn(
        { err: e, Category: 'sp00ky-client::BrokerClient' },
        'Broker reconnect failed; staying detached until the next watchdog tick'
      );
    });
  }

  private installLifecycleListeners(): void {
    if (typeof window === 'undefined' || typeof document === 'undefined') return;
    document.addEventListener('visibilitychange', () => {
      if (!this.helloState) return;
      this.send({
        type: 'visibility',
        tabId: this.tabId,
        bucketId: this.helloState.bucketId,
        visibility: currentVisibility(),
      });
    });
    // pagehide fires for both close and bfcache entry; treat both as a full
    // departure (a bfcache restore re-hellos via pageshow below). freeze is
    // Chromium's battery-saver signal; leaving proactively hands leadership
    // over in ~0s instead of waiting out the pong timeout.
    const leave = () => {
      if (this.helloState) {
        this.send({ type: 'shutdown', tabId: this.tabId, bucketId: this.helloState.bucketId });
      }
    };
    window.addEventListener('pagehide', leave);
    document.addEventListener('freeze', leave);
    window.addEventListener('pageshow', (e: PageTransitionEvent) => {
      if (e.persisted && this.helloState) void this.rehello(this.helloState);
    });
    document.addEventListener('resume', () => {
      if (this.helloState) void this.rehello(this.helloState);
    });
  }

  close(): void {
    this.closed = true;
    if (this.watchdog) clearInterval(this.watchdog);
    this.watchdog = null;
    try {
      this.worker?.port.close();
    } catch {
      /* ignore */
    }
    this.worker = null;
  }
}

function currentVisibility(): TabVisibility {
  if (typeof document === 'undefined') return 'visible';
  return document.visibilityState === 'hidden' ? 'hidden' : 'visible';
}
