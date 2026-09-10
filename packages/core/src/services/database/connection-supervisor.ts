import type { ConnectionState, ReconnectConfig } from '../../types';
import { withTimeout } from '../../utils/index';
import type { Logger } from '../logger/index';
import type { RemoteDatabaseService } from './remote';

/**
 * Keeps the remote WebSocket alive for the whole life of the page.
 *
 * The SurrealDB SDK reconnects on its own after a socket `close`, but that
 * covers only one of three ways the connection dies:
 *
 * 1. **Socket closes, SDK recovers.** Handled entirely by the SDK. This
 *    supervisor only observes it (to report `reconnecting` upward).
 * 2. **Socket closes, SDK gives up.** With `attempts: -1` this shouldn't happen
 *    from exhaustion — but the SDK also terminates the engine permanently when
 *    its post-reconnect handshake throws (it re-runs `version()`, `use()`,
 *    `authenticate()` on every reconnect and closes the engine on any error).
 *    One transient hiccup there would otherwise kill the page's connection for
 *    good. The revive loop re-opens from scratch.
 * 3. **Socket never closes at all.** A half-open connection: the peer is gone
 *    (NAT timeout, wifi switch, laptop sleep) but no FIN ever arrives, so
 *    `readyState` stays OPEN and the SDK's own 30s ping — fire-and-forget, no
 *    response deadline — never notices. Nothing ever fires a `close` event, so
 *    nothing ever triggers a reconnect. The heartbeat detects this and forces
 *    the teardown that case 2's loop then repairs.
 *
 * Plus wake triggers: coming back `online` or un-hiding the tab probes
 * immediately rather than waiting out a backoff that was scheduled while the
 * network was known-down.
 */
export class ConnectionSupervisor {
  private readonly logger: Logger;
  private readonly config: Required<ReconnectConfig>;

  private state: ConnectionState = 'disconnected';
  private subscribers = new Set<(state: ConnectionState) => void>();

  private started = false;
  private disposed = false;

  private heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatInFlight = false;
  /** Consecutive failed probes. See {@link FAILURES_BEFORE_TEARDOWN}. */
  private heartbeatFailures = 0;

  private reviveTimer: ReturnType<typeof setTimeout> | null = null;
  private reviveAttempts = 0;
  /** Timestamp of the last wake-triggered probe, for rate limiting. */
  private lastWakeProbeAt = 0;
  private reviving = false;
  /**
   * Set while the browser reports itself offline. Retrying a socket against a
   * down interface only burns backoff, so the loop parks until `online` fires.
   */
  private suspended = false;

  private teardown: Array<() => void> = [];

  private static readonly REVIVE_BASE_MS = 1_000;
  /**
   * How many consecutive heartbeat failures it takes to tear the socket down.
   *
   * The probe rides the same serialized queue as every other RPC (deliberately
   * — see {@link beat}), which means it cannot distinguish a WEDGED queue from
   * a merely BUSY one. A single slow window (a large sync burst, one heavy
   * app query) used to be enough to force-close a perfectly healthy socket,
   * and the resulting reconnect re-registered every active query about a
   * second later. That self-inflicted teardown manufactured the very reconnect
   * storms this class exists to survive. A genuinely dead socket still fails
   * every probe, so it is torn down one interval later than before.
   */
  private static readonly FAILURES_BEFORE_TEARDOWN = 2;
  /** Retry delay after an inconclusive (first) heartbeat failure. */
  private static readonly HEARTBEAT_RETRY_MS = 5_000;
  /** Floor between probes triggered by wake events (tab focus, pageshow). */
  private static readonly WAKE_PROBE_MIN_INTERVAL_MS = 10_000;

  constructor(
    private readonly remote: RemoteDatabaseService,
    logger: Logger,
    config?: Required<ReconnectConfig>
  ) {
    this.logger = logger.child({ service: 'ConnectionSupervisor' });
    this.config = config ?? remote.getReconnectConfig();
  }

  /** Latest observed transport state. */
  get connection(): ConnectionState {
    return this.state;
  }

  /**
   * Observe transport state. Fires immediately with the current value and again
   * on every change. Returns an unsubscribe.
   */
  subscribe(cb: (state: ConnectionState) => void): () => void {
    cb(this.state);
    this.subscribers.add(cb);
    return () => {
      this.subscribers.delete(cb);
    };
  }

  /**
   * Begin supervising. Call once, after the initial {@link
   * RemoteDatabaseService.connect}. Idempotent.
   */
  start(): void {
    if (this.started || this.disposed) return;
    this.started = true;

    this.setState(this.remote.getStatus());

    this.teardown.push(
      this.remote.subscribeConnection('connecting', () => this.setState('connecting')),
      this.remote.subscribeConnection('reconnecting', () => {
        this.setState('reconnecting');
        // The SDK owns the retry from here; ours would fight it for the socket.
        this.stopHeartbeat();
      }),
      this.remote.subscribeConnection('connected', () => {
        this.reviveAttempts = 0;
        this.clearReviveTimer();
        this.setState('connected');
        this.startHeartbeat();
      }),
      this.remote.subscribeConnection('disconnected', () => {
        this.setState('disconnected');
        this.stopHeartbeat();
        // The SDK has stopped trying (exhausted, terminated, or handshake
        // failure). From here on, reconnecting is entirely our job.
        this.scheduleRevive();
      }),
      this.remote.subscribeConnection('error', (err) => {
        this.logger.debug(
          { err, Category: 'sp00ky-client::ConnectionSupervisor::error' },
          'Transport error'
        );
      })
    );

    this.installWakeTriggers();

    if (this.state === 'connected') this.startHeartbeat();
    else this.scheduleRevive();
  }

  /** Stop all timers and listeners. Safe to call more than once. */
  dispose(): void {
    this.disposed = true;
    this.started = false;
    this.stopHeartbeat();
    this.clearReviveTimer();
    for (const off of this.teardown) {
      try {
        off();
      } catch {
        /* ignore */
      }
    }
    this.teardown = [];
    this.subscribers.clear();
  }

  private setState(next: ConnectionState): void {
    if (this.state === next) return;
    this.state = next;
    this.logger.info(
      { state: next, Category: 'sp00ky-client::ConnectionSupervisor::state' },
      'Connection state changed'
    );
    for (const cb of this.subscribers) {
      try {
        cb(next);
      } catch (err) {
        this.logger.debug(
          { err, Category: 'sp00ky-client::ConnectionSupervisor::state' },
          'Connection subscriber threw'
        );
      }
    }
  }

  // ---- Revive loop -------------------------------------------------------

  private clearReviveTimer(): void {
    if (this.reviveTimer !== null) {
      clearTimeout(this.reviveTimer);
      this.reviveTimer = null;
    }
  }

  /**
   * Queue the next `connect()` attempt on exponential backoff, capped at
   * `superviseRetryDelayMaxMs`. Never gives up — the page is expected to
   * outlive any outage.
   */
  private scheduleRevive(): void {
    if (this.disposed || this.suspended) return;
    if (this.reviveTimer !== null || this.reviving) return;
    const delay = Math.min(
      this.config.superviseRetryDelayMaxMs,
      ConnectionSupervisor.REVIVE_BASE_MS * 2 ** this.reviveAttempts
    );
    this.reviveTimer = setTimeout(() => {
      this.reviveTimer = null;
      void this.revive();
    }, delay);
  }

  private async revive(): Promise<void> {
    if (this.disposed || this.suspended || this.reviving) return;
    // The SDK may have recovered on its own between scheduling and firing.
    if (this.remote.getStatus() === 'connected') {
      this.reviveAttempts = 0;
      return;
    }
    this.reviving = true;
    this.reviveAttempts++;
    this.setState('reconnecting');
    this.logger.info(
      {
        attempt: this.reviveAttempts,
        Category: 'sp00ky-client::ConnectionSupervisor::revive',
      },
      'Re-opening the remote connection'
    );
    try {
      await this.remote.connect();
      // Don't reset `reviveAttempts` or start the heartbeat here — the
      // `connected` handler does both, and it's the only signal that the
      // handshake (version/use/authenticate) actually completed.
    } catch (err) {
      this.logger.warn(
        {
          err,
          attempt: this.reviveAttempts,
          Category: 'sp00ky-client::ConnectionSupervisor::revive',
        },
        'Reconnect attempt failed; will retry'
      );
    } finally {
      this.reviving = false;
    }
    if (this.remote.getStatus() !== 'connected') this.scheduleRevive();
  }

  // ---- Heartbeat watchdog ------------------------------------------------

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) {
      clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    if (this.disposed || this.suspended) return;
    if (!(this.config.heartbeatIntervalMs > 0)) return;
    this.heartbeatTimer = setTimeout(
      () => void this.beat(),
      this.config.heartbeatIntervalMs
    );
  }

  /**
   * Probe the server end-to-end. Deliberately goes through
   * `remote.query` — the same serialized queue every other remote call uses —
   * so a queue wedged behind a stuck RPC also fails the heartbeat instead of
   * being invisible to it.
   */
  private async beat(): Promise<void> {
    this.heartbeatTimer = null;
    if (this.disposed || this.suspended) return;
    if (this.remote.getStatus() !== 'connected') return;
    if (this.heartbeatInFlight) {
      this.startHeartbeat();
      return;
    }
    this.heartbeatInFlight = true;
    try {
      await withTimeout(
        this.remote.query('RETURN true'),
        this.config.heartbeatTimeoutMs,
        `Heartbeat timed out after ${this.config.heartbeatTimeoutMs}ms`
      );
      this.heartbeatFailures = 0;
      // A round trip is the strongest evidence there is that the socket works,
      // so it also RECONCILES the reported state. The `offline` handler forces
      // `disconnected` without the SDK's agreement; if the SDK never noticed
      // the outage it emits no transition when the network returns, and that
      // forced state would otherwise stick for the life of the page - traffic
      // flowing while every indicator says offline.
      this.setState('connected');
      this.startHeartbeat();
    } catch (err) {
      this.heartbeatFailures++;
      if (this.heartbeatFailures < ConnectionSupervisor.FAILURES_BEFORE_TEARDOWN) {
        // Inconclusive: the probe shares a queue with ordinary traffic, so this
        // may just be a busy window rather than a dead socket. Re-probe soon
        // instead of tearing down a connection that is probably fine.
        this.logger.debug(
          {
            err,
            failures: this.heartbeatFailures,
            Category: 'sp00ky-client::ConnectionSupervisor::heartbeat',
          },
          'Heartbeat failed; re-probing before tearing the socket down'
        );
        this.stopHeartbeat();
        if (!this.disposed && !this.suspended) {
          this.heartbeatTimer = setTimeout(
            () => void this.beat(),
            Math.min(ConnectionSupervisor.HEARTBEAT_RETRY_MS, this.config.heartbeatIntervalMs)
          );
        }
        return;
      }
      this.logger.warn(
        {
          err,
          failures: this.heartbeatFailures,
          Category: 'sp00ky-client::ConnectionSupervisor::heartbeat',
        },
        'Heartbeat failed repeatedly; tearing the socket down to force a reconnect'
      );
      this.heartbeatFailures = 0;
      // Force the `close` the transport never delivered. The resulting
      // `disconnected` event drives the revive loop.
      await this.remote.forceClose();
      if (this.remote.getStatus() !== 'connected') this.scheduleRevive();
    } finally {
      this.heartbeatInFlight = false;
    }
  }

  // ---- Wake triggers -----------------------------------------------------

  /**
   * A restored network or an un-hidden tab is the strongest available hint that
   * a reconnect will now succeed, so probe immediately instead of waiting out a
   * backoff scheduled under worse conditions.
   */
  private installWakeTriggers(): void {
    if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
      const onOnline = () => {
        this.suspended = false;
        this.wake('online');
      };
      const onOffline = () => {
        this.logger.info(
          { Category: 'sp00ky-client::ConnectionSupervisor::offline' },
          'Browser reports offline; parking reconnects until online'
        );
        this.suspended = true;
        this.stopHeartbeat();
        this.clearReviveTimer();
        this.setState('disconnected');
      };
      window.addEventListener('online', onOnline);
      window.addEventListener('offline', onOffline);
      this.teardown.push(
        () => window.removeEventListener('online', onOnline),
        () => window.removeEventListener('offline', onOffline)
      );
      // A tab restored from bfcache keeps its dead socket; `pageshow` is the
      // only event that fires in that path.
      const onPageShow = () => this.wake('pageshow');
      window.addEventListener('pageshow', onPageShow);
      this.teardown.push(() => window.removeEventListener('pageshow', onPageShow));
    }

    if (typeof document !== 'undefined' && typeof document.addEventListener === 'function') {
      const onVisibility = () => {
        if (document.visibilityState !== 'visible') return;
        this.wake('visibilitychange');
      };
      document.addEventListener('visibilitychange', onVisibility);
      this.teardown.push(() => document.removeEventListener('visibilitychange', onVisibility));
    }
  }

  /**
   * Reset the backoff and act on whichever problem is present: reconnect if the
   * socket is gone, otherwise probe it (it may be half-open — which is exactly
   * what a sleep/wake cycle produces).
   */
  private wake(reason: string): void {
    if (this.disposed || this.suspended) return;
    // `visibilitychange` fires on every alt-tab, every window minimise and every
    // focus change. Probing a connected socket each time meant an ordinary
    // afternoon of tab-switching issued a steady stream of extra RPCs, each of
    // which could trip the heartbeat teardown above. A healthy socket does not
    // become unhealthy because the user looked away for four seconds, so probes
    // from wake triggers are rate-limited; a genuinely dead socket is still
    // caught by the regular heartbeat.
    const now = Date.now();
    if (
      this.remote.getStatus() === 'connected' &&
      now - this.lastWakeProbeAt < ConnectionSupervisor.WAKE_PROBE_MIN_INTERVAL_MS
    ) {
      return;
    }
    this.lastWakeProbeAt = now;
    this.logger.debug(
      { reason, state: this.state, Category: 'sp00ky-client::ConnectionSupervisor::wake' },
      'Wake trigger; probing the connection'
    );
    this.reviveAttempts = 0;
    if (this.remote.getStatus() === 'connected') {
      this.stopHeartbeat();
      void this.beat();
      return;
    }
    // 'reconnecting' means the SDK's own loop owns the socket; don't race it.
    if (this.remote.getStatus() === 'disconnected') {
      this.clearReviveTimer();
      void this.revive();
    }
  }
}
