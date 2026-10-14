import type {
  Diagnostic,
  SurrealEvents} from 'surrealdb';
import {
  applyDiagnostics,
  createRemoteEngines,
  Surreal,
} from 'surrealdb';
import type { ConnectionState, ReconnectConfig, Sp00kyConfig } from '../../types';
import type { Logger } from '../logger/index';
import { AbstractDatabaseService } from './database';
import { createDatabaseEventSystem, DatabaseEventTypes } from './events/index';

/** Defaults for {@link ReconnectConfig}. See that type for the rationale. */
export const RECONNECT_DEFAULTS = {
  attempts: -1,
  retryDelayMax: 15_000,
  heartbeatIntervalMs: 20_000,
  heartbeatTimeoutMs: 10_000,
  superviseRetryDelayMaxMs: 15_000,
} as const;

/** Fill in {@link RECONNECT_DEFAULTS} for every field the caller omitted. */
export function resolveReconnectConfig(
  input?: ReconnectConfig
): Required<ReconnectConfig> {
  return { ...RECONNECT_DEFAULTS, ...input };
}

/** Transport events the SDK publishes, mapped 1:1 to {@link ConnectionState}. */
export type RemoteConnectionEvent = ConnectionState | 'error';

/**
 * Statements the remote service keeps in flight at once. Enough for the sync
 * scheduler's `MAX_CONCURRENT_DOWN` registrations plus the list_ref poll and
 * an app's own one-shot reads, small enough to stay polite to the server.
 */
const REMOTE_MAX_CONCURRENT_QUERIES = 6;

/**
 * Session ids a page remembers for its pagehide release and its anonymous LIVE
 * filter. The release only needs the sessions of the last ttl; this many
 * reconnects inside one ttl is a broken network. The LIVE filter would like
 * every session that stamped an edge still held, but the SSP restamps an edge
 * whenever its row changes, so an id that falls off costs at most the LIVE
 * removal of a row untouched since, which the membership poll still catches.
 */
const MAX_SESSION_IDS = 16;

export class RemoteDatabaseService extends AbstractDatabaseService {
  private config: Sp00kyConfig<any>['database'];
  protected eventType = DatabaseEventTypes.RemoteQuery;
  private readonly reconnectConfig: Required<ReconnectConfig>;
  /**
   * In-flight `connect()`, so concurrent callers (boot + supervisor revive +
   * an `online` event landing at the same moment) share one attempt instead of
   * racing two sockets. Cleared on settle, so a later call always reconnects.
   */
  private connecting: Promise<void> | null = null;
  /**
   * The token to re-authenticate a freshly opened socket with, kept current by
   * {@link setAuthToken}.
   *
   * `config.token` is fixed at construction and most apps never set it: they
   * sign in later, which authenticates the socket that happens to be open at
   * the time. That is enough for the SDK's OWN reconnects (it replays
   * `version`/`use`/`authenticate` itself), but not for the supervisor's revive
   * loop, which builds a socket from scratch. Without this the revived socket
   * came back UNAUTHENTICATED and stayed that way for the life of the page,
   * while the client's `currentUserId` — restored from local storage — kept
   * reporting the user as signed in.
   *
   * The visible damage is silent and total: `fn::query::register` sends
   * `<string>($auth.id OR '')`, so every view registered afterwards is stamped
   * with an empty identity, and every `$auth.id` predicate in it resolves
   * false. Public tables keep returning rows while everything owned by the user
   * returns nothing, on a page that still looks signed in.
   */
  private authToken: string | null = null;
  /**
   * Every `session::id()` this page's sockets have had, oldest first.
   *
   * The server stamps a view's `subscribers` entry with the session that
   * registered or heartbeated it, and every socket (an SDK reconnect, a
   * supervisor revive) is a new session. The pagehide release goes over HTTP,
   * whose own session id matches none of them, so it names these instead. Kept
   * for the page, not the socket: a view registered before a reconnect still
   * carries the old id until its next heartbeat.
   *
   * The anonymous LIVE filters on the same ids (see `listRefLive`): its edges
   * name the socket that registered their view, which may be an earlier one.
   * A reload hands its ids to the next page with the salt.
   */
  private sessionIdsSeen: string[] = [];

  constructor(config: Sp00kyConfig<any>['database'], logger: Logger) {
    const events = createDatabaseEventSystem();
    super(
      new Surreal({
        engines: applyDiagnostics(
          createRemoteEngines(),
          ({ key, type, phase, ...other }: Diagnostic) => {
            if (phase === 'progress' || phase === 'after') {
              logger.trace(
                {
                  ...other,
                  key,
                  type,
                  phase,
                  service: 'surrealdb:remote',
                  Category: 'sp00ky-client::RemoteDatabaseService::diagnostics',
                },
                `Remote SurrealDB diagnostics captured ${type}:${phase}`
              );
            }
          }
        ),
      }),
      logger,
      events
    );
    this.config = config;
    this.reconnectConfig = resolveReconnectConfig(config.reconnect);
    this.queryTimeoutMs = Math.max(0, config.queryTimeoutMs ?? 60_000);
    this.maxConcurrentQueries = REMOTE_MAX_CONCURRENT_QUERIES;
    this.client.subscribe('connected', () => void this.recordSessionId());
  }

  /** See {@link sessionIdsSeen}. */
  sessionIds(): readonly string[] {
    return this.sessionIdsSeen;
  }

  /**
   * Take on the session ids of the page this one reloaded from. It kept that
   * page's salt, so the views it re-registers still carry edges stamped by
   * those sockets. Older than anything this page has seen, so they go first.
   */
  adoptSessionIds(ids: ReadonlyArray<string>): void {
    const earlier = ids.filter((sid) => typeof sid === 'string' && sid !== '' && !this.sessionIdsSeen.includes(sid));
    this.sessionIdsSeen = [...earlier, ...this.sessionIdsSeen].slice(-MAX_SESSION_IDS);
  }

  /**
   * Ask the socket that just connected for its session id. Straight to the
   * SDK: it needs no namespace or identity, so it need not wait behind an
   * in-flight connect or take a slot from the query queue.
   */
  private async recordSessionId(): Promise<void> {
    try {
      const [sid] = (await this.client.query('RETURN <string>session::id()')) as unknown[];
      if (typeof sid !== 'string' || this.sessionIdsSeen.includes(sid)) return;
      this.sessionIdsSeen = [...this.sessionIdsSeen, sid].slice(-MAX_SESSION_IDS);
    } catch {
      // Without it this socket's views fall back to the TTL sweep.
    }
  }

  getConfig(): Sp00kyConfig<any>['database'] {
    return this.config;
  }

  /**
   * Send one SurrealQL statement so that it survives the page going away.
   *
   * A WebSocket `send()` during `pagehide` is not guaranteed to flush — the
   * browser may tear the socket down first, and the frame is simply lost.
   * Measured: an unload-time release over the live socket reached the server
   * zero times out of one. `fetch` with `keepalive` is the primitive the
   * platform actually guarantees here, so this goes over SurrealDB's HTTP
   * `/sql` endpoint instead of the RPC socket.
   *
   * Best-effort by design: no await, no retry, errors swallowed. Every caller
   * must have a server-side fallback that makes a lost beacon a non-event.
   *
   * The statement runs in a fresh HTTP session: `session::id()` there is a new
   * id, not this page's socket's, so anything keyed on it must be passed the
   * ids from {@link sessionIds} instead.
   */
  beaconSql(sql: string): void {
    try {
      const { endpoint, namespace, database } = this.getConfig();
      if (!endpoint || typeof fetch !== 'function') return;

      // `ws(s)://host/rpc` is the socket; `http(s)://host/sql` is the same
      // server's statement endpoint.
      const url = new URL(endpoint);
      url.protocol = url.protocol === 'wss:' ? 'https:' : url.protocol === 'ws:' ? 'http:' : url.protocol;
      url.pathname = url.pathname.replace(/\/rpc\/?$/, '') + '/sql';

      const headers: Record<string, string> = {
        Accept: 'application/json',
        'Content-Type': 'text/plain',
      };
      if (namespace) headers['surreal-ns'] = namespace;
      if (database) headers['surreal-db'] = database;
      // Without this the statement runs unauthenticated and `$auth.id` is NONE,
      // which for a per-user release means it matches nothing.
      if (this.authToken) headers.Authorization = `Bearer ${this.authToken}`;

      void fetch(url.toString(), {
        method: 'POST',
        headers,
        body: sql,
        keepalive: true,
        credentials: 'omit',
      }).catch(() => {});
    } catch {
      // Malformed endpoint, no fetch, blocked by CSP: the caller's fallback owns it.
    }
  }

  /**
   * Record the token every future connect should authenticate with, or `null`
   * on sign-out. See {@link authToken}.
   *
   * Does not touch the CURRENT socket: callers authenticate that themselves
   * (sign-in and session restore both already do). This only makes the next
   * from-scratch connect reproduce that state.
   */
  setAuthToken(token: string | null): void {
    this.authToken = token;
  }

  /** Resolved reconnect tunables; the supervisor reads its own knobs here. */
  getReconnectConfig(): Required<ReconnectConfig> {
    return this.reconnectConfig;
  }

  /** Current transport state as reported by the SDK. */
  getStatus(): ConnectionState {
    return this.client.status as ConnectionState;
  }

  /**
   * Observe transport events. Thin passthrough so callers (the supervisor,
   * sync, CRDT) don't have to reach through `getClient()`.
   */
  subscribeConnection<K extends RemoteConnectionEvent>(
    event: K,
    cb: (...payload: SurrealEvents[K]) => void
  ): () => void {
    return this.client.subscribe(event, cb);
  }

  /**
   * Tear the socket down on purpose. Used by the heartbeat watchdog when a
   * socket stops answering but never closes: `close()` makes the SDK publish
   * `disconnected`, which is what drives the supervisor's revive loop.
   */
  async forceClose(): Promise<void> {
    try {
      await this.client.close();
    } catch (err) {
      this.logger.debug(
        { err, Category: 'sp00ky-client::RemoteDatabaseService::forceClose' },
        'forceClose failed; treating the socket as gone anyway'
      );
    }
  }

  /** Resolve once no connect is in flight, for calls that go to the SDK
   *  client directly (sign-in, sign-up) and so skip `beforeQuery`. */
  whenConnected(): Promise<void> {
    return this.beforeQuery();
  }

  /**
   * Hold statements while a connect is in flight.
   *
   * `connect()` opens the socket, then selects the namespace/database, then
   * authenticates - three round trips. The SDK considers the connection ready
   * after the first, so a query queued during the other two ran on a socket
   * with no namespace ("Specify a namespace to use") or, worse, no identity.
   * Seen 2026-09-06 after the supervisor rebuilt a socket: a collection
   * recount failed with exactly that error. Waiting on the in-flight connect
   * costs nothing when no connect is running; a failed connect is not
   * swallowed here - the query then fails on its own, as before.
   */
  protected override async beforeQuery(): Promise<void> {
    const inflight = this.connecting;
    if (!inflight) return;
    try {
      await inflight;
    } catch {
      // The query that follows reports the failure itself.
    }
  }

  /**
   * Open (or re-open) the remote connection.
   *
   * Safe to call repeatedly: concurrent calls share the in-flight attempt, and
   * a call after a `disconnected` builds a fresh socket. `use()` and
   * `authenticate()` are re-applied here for the cold path; the SDK also
   * replays them itself on its own internal reconnects.
   */
  async connect(): Promise<void> {
    if (this.connecting) return this.connecting;
    this.connecting = this.doConnect().finally(() => {
      this.connecting = null;
    });
    return this.connecting;
  }

  private async doConnect(): Promise<void> {
    const { endpoint, namespace, database } = this.getConfig();
    // The live token wins over the constructor-time one: this connect may be
    // the supervisor rebuilding a socket long after sign-in. See `authToken`.
    const token = this.authToken ?? this.getConfig().token;
    if (endpoint) {
      this.logger.info(
        {
          endpoint,
          namespace,
          database,
          Category: 'sp00ky-client::RemoteDatabaseService::connect',
        },
        'Connecting to remote database'
      );
      try {
        // Without explicit options the SDK caps itself at 5 attempts (~62s of
        // outage) and then gives up permanently — nothing would re-open the
        // socket after that. `attempts: -1` keeps it trying; the supervisor
        // still covers the case where the SDK terminates the engine because its
        // post-reconnect handshake threw.
        await this.client.connect(endpoint, {
          reconnect: {
            enabled: true,
            attempts: this.reconnectConfig.attempts,
            retryDelay: 1_000,
            retryDelayMax: this.reconnectConfig.retryDelayMax,
            retryDelayMultiplier: 2,
            retryDelayJitter: 0.1,
          },
        });
        await this.client.use({
          namespace,
          database,
        });

        if (token) {
          this.logger.debug(
            { Category: 'sp00ky-client::RemoteDatabaseService::connect' },
            'Authenticating with token'
          );
          await this.client.authenticate(token);
        }
        this.logger.info(
          { Category: 'sp00ky-client::RemoteDatabaseService::connect' },
          'Connected to remote database'
        );
      } catch (err) {
        this.logger.error(
          { err, Category: 'sp00ky-client::RemoteDatabaseService::connect' },
          'Failed to connect to remote database'
        );
        throw err;
      }
    } else {
      this.logger.warn(
        { Category: 'sp00ky-client::RemoteDatabaseService::connect' },
        'No endpoint configured for remote database'
      );
    }
  }

  async signin(params: any): Promise<any> {
    return this.client.signin(params);
  }

  async signup(params: any): Promise<any> {
    return this.client.signup(params);
  }

  async authenticate(token: string): Promise<any> {
    return this.client.authenticate(token);
  }

  async invalidate(): Promise<void> {
    return this.client.invalidate();
  }
}
