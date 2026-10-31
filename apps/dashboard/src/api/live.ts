/**
 * The dashboard's one connection to a host: `/admin/api/v2/live`.
 *
 * A {@link LiveClient} owns a WebSocket, authenticates it with the session
 * token (as the first message, never in the URL), and multiplexes topic
 * subscriptions over it. It knows nothing about storage: each subscription
 * has one {@link TopicSink} (in practice a TanStack DB collection, see
 * `collections.ts`) that holds the state and its seq.
 *
 * Resilience is the point of this file:
 * - It reconnects with jittered backoff, at once when the browser comes back
 *   online or the tab becomes visible, and resubscribes every topic with the
 *   seq its sink holds, so the host resends only what was missed.
 * - A patch that does not chain onto the sink's seq is a gap: the topic is
 *   resubscribed and the host answers with the missing patches or a snapshot.
 * - A hidden tab gives its socket up after a while, so a forgotten tab costs
 *   the host nothing; it comes back with resume, not a full reload.
 * - `blocked` tells the app the socket never got through (a proxy that drops
 *   upgrades), so it can fall back to polling.
 */

import { createSignal, type Accessor } from 'solid-js';
import { topicKey, type Json, type Shape } from './liveCore';

export type LiveStatus =
  /** Nothing subscribed, or the tab is parked. */
  | 'idle'
  | 'connecting'
  | 'live'
  /** Lost the connection; retrying. */
  | 'offline'
  /** The host refused the session. Needs a new token, then `retry()`. */
  | 'unauthorized'
  /** Several attempts and never authenticated: assume sockets are blocked. */
  | 'blocked';

/** Where one topic's frames go. */
export interface TopicSink {
  /** The seq this sink holds for host process `boot`, if any. */
  since(boot: string): number | undefined;
  snap(boot: string, seq: number, data: Json): void;
  /** Apply a patch. `false` when `from` is not the held seq: a gap. */
  diff(boot: string, from: number, seq: number, patch: Json): boolean;
  /** The host refused this subscription. */
  error?(message: string): void;
  /** The host accepted it; `shape` is the topic's. */
  subscribed?(shape: Shape): void;
}

interface Subscription {
  topic: string;
  args?: Json;
  key: string;
  sink: TopicSink;
}

export interface LiveOptions {
  /** The `ws(s)://…/admin/api/v2/live` URL. */
  url: () => string;
  /** The current bearer, or null when signed out. */
  token: () => string | null;
  /**
   * A fresh token when the host warns the session is expiring (the console
   * mints one from a new grant). Without it the socket closes at expiry, the
   * same moment REST calls start failing.
   */
  renew?: () => Promise<string | null>;
  /** The host refused the session. */
  onUnauthorized?: () => void;
}

/** Close codes, mirrored from `ssp_protocol::admin_live`. */
const CLOSE_UNAUTHORIZED = 4401;
/** How long a hidden tab keeps its socket. */
const PARK_AFTER_MS = 5 * 60_000;
const BACKOFF_MIN_MS = 500;
const BACKOFF_MAX_MS = 30_000;
const BLOCKED_AFTER = 3;

type ServerFrame =
  | { op: 'ready'; boot: string; session: { expires_at: number }; topics: string[] }
  | { op: 'subscribed'; ref: string; key: string; shape: Shape }
  | { op: 'snap'; key: string; seq: number; data: Json }
  | { op: 'diff'; key: string; from: number; seq: number; patch: Json }
  | { op: 'expiring'; at: number }
  | { op: 'error'; ref?: string; code: string; message: string }
  | { op: 'pong' };

export class LiveClient {
  readonly status: Accessor<LiveStatus>;
  /** The host process this client is attached to; changes on host restart. */
  readonly boot: Accessor<string | null>;
  /** Topics the host advertised. */
  readonly topics: Accessor<string[]>;

  private setStatus: (s: LiveStatus) => void;
  private setBoot: (b: string | null) => void;
  private setTopics: (t: string[]) => void;
  private ws: WebSocket | null = null;
  private subs = new Map<string, Subscription>();
  private attempt = 0;
  private everReady = false;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private parkTimer: ReturnType<typeof setTimeout> | undefined;
  private disposed = false;
  private readonly opts: LiveOptions;

  constructor(opts: LiveOptions) {
    this.opts = opts;
    const [status, setStatus] = createSignal<LiveStatus>('idle');
    const [boot, setBoot] = createSignal<string | null>(null);
    const [topics, setTopics] = createSignal<string[]>([]);
    this.status = status;
    this.setStatus = setStatus;
    this.boot = boot;
    this.setBoot = setBoot;
    this.topics = topics;
    this.setTopics = setTopics;
    if (typeof window !== 'undefined') {
      window.addEventListener('online', this.onOnline);
      document.addEventListener('visibilitychange', this.onVisibility);
    }
  }

  /** Subscribe `sink` to `topic`. One sink per topic key; returns the unsubscribe. */
  subscribe(topic: string, args: Json | undefined, sink: TopicSink): () => void {
    const key = topicKey(topic, args);
    if (this.subs.has(key)) throw new Error(`Live topic ${key} already has a reader`);
    const sub: Subscription = { topic, args, key, sink };
    this.subs.set(key, sub);
    if (this.status() === 'live') this.sendSub(sub);
    else this.connect();
    return () => {
      if (this.subs.get(key) !== sub) return;
      this.subs.delete(key);
      this.send({ op: 'unsub', key });
    };
  }

  /** Try again now, e.g. after a sign-in replaced a refused token. */
  retry() {
    this.attempt = 0;
    this.everReady = false;
    this.close();
    this.connect();
  }

  dispose() {
    this.disposed = true;
    this.close();
    this.subs.clear();
    if (typeof window !== 'undefined') {
      window.removeEventListener('online', this.onOnline);
      document.removeEventListener('visibilitychange', this.onVisibility);
    }
  }

  /* -------------------------------------------------------------- */

  private connect() {
    if (this.disposed || this.ws || this.subs.size === 0) return;
    if (typeof document !== 'undefined' && document.hidden && this.status() === 'idle' && this.everReady) return;
    const token = this.opts.token();
    if (!token) {
      this.setStatus('unauthorized');
      return;
    }
    clearTimeout(this.retryTimer);
    this.setStatus(this.everReady || this.attempt < BLOCKED_AFTER ? 'connecting' : 'blocked');
    let ws: WebSocket;
    try {
      ws = new WebSocket(this.opts.url());
    } catch {
      this.scheduleRetry();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      const current = this.opts.token();
      if (!current) {
        ws.close();
        return;
      }
      ws.send(JSON.stringify({ op: 'auth', token: current }));
    };
    ws.onmessage = (ev) => {
      if (typeof ev.data !== 'string') return;
      let frame: ServerFrame;
      try {
        frame = JSON.parse(ev.data) as ServerFrame;
      } catch {
        return;
      }
      this.onFrame(frame);
    };
    ws.onclose = (ev) => {
      if (this.ws !== ws) return;
      this.ws = null;
      if (this.disposed) return;
      if (ev.code === CLOSE_UNAUTHORIZED) {
        this.setStatus('unauthorized');
        this.opts.onUnauthorized?.();
        return;
      }
      if (this.status() === 'idle') return; // parked on purpose
      this.scheduleRetry();
    };
  }

  private onFrame(frame: ServerFrame) {
    switch (frame.op) {
      case 'ready': {
        this.attempt = 0;
        this.everReady = true;
        this.setBoot(frame.boot);
        this.setTopics(frame.topics);
        const wasLive = this.status() === 'live';
        this.setStatus('live');
        // A repeated `ready` is the answer to a renewed token: nothing to redo.
        if (!wasLive) for (const sub of this.subs.values()) this.sendSub(sub);
        return;
      }
      case 'subscribed':
        this.subs.get(frame.key)?.sink.subscribed?.(frame.shape);
        return;
      case 'snap': {
        const boot = this.boot();
        if (boot) this.subs.get(frame.key)?.sink.snap(boot, frame.seq, frame.data);
        return;
      }
      case 'diff': {
        const sub = this.subs.get(frame.key);
        const boot = this.boot();
        if (!sub || !boot) return;
        if (!sub.sink.diff(boot, frame.from, frame.seq, frame.patch)) this.sendSub(sub);
        return;
      }
      case 'expiring':
        void this.renew();
        return;
      case 'error': {
        const sub = frame.ref ? this.subs.get(frame.ref) : undefined;
        sub?.sink.error?.(frame.message);
        return;
      }
      case 'pong':
        return;
    }
  }

  private async renew() {
    const token = await this.opts.renew?.();
    if (token) this.send({ op: 'auth', token });
  }

  /** `ref` is the key itself: one reader per key, so it names the sub. */
  private sendSub(sub: Subscription) {
    const boot = this.boot();
    const since = boot ? sub.sink.since(boot) : undefined;
    this.send({ op: 'sub', ref: sub.key, topic: sub.topic, args: sub.args ?? null, since: since ?? null });
  }

  private send(msg: unknown) {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg));
  }

  private close() {
    clearTimeout(this.retryTimer);
    const ws = this.ws;
    this.ws = null;
    if (ws && ws.readyState <= WebSocket.OPEN) ws.close(1000);
  }

  private scheduleRetry() {
    this.attempt += 1;
    if (!this.everReady && this.attempt >= BLOCKED_AFTER) this.setStatus('blocked');
    else this.setStatus('offline');
    const base = Math.min(BACKOFF_MAX_MS, BACKOFF_MIN_MS * 2 ** (this.attempt - 1));
    // Full jitter in the upper half: a restarted host is not met by every
    // dashboard in the same millisecond.
    const delay = base / 2 + Math.random() * (base / 2);
    clearTimeout(this.retryTimer);
    this.retryTimer = setTimeout(() => this.connect(), delay);
  }

  private onOnline = () => {
    if (this.ws || this.status() === 'unauthorized') return;
    this.attempt = 0;
    this.connect();
  };

  private onVisibility = () => {
    clearTimeout(this.parkTimer);
    if (document.hidden) {
      this.parkTimer = setTimeout(() => {
        if (!document.hidden) return;
        this.setStatus('idle');
        this.close();
      }, PARK_AFTER_MS);
      return;
    }
    if (!this.ws && this.status() !== 'unauthorized') {
      this.attempt = 0;
      this.connect();
    }
  };
}

/** The live URL for an API base (`''` = this page's own origin). */
export function liveUrl(baseUrl: string): string {
  const origin = baseUrl || window.location.origin;
  return `${origin.replace(/^http/i, 'ws')}/admin/api/v2/live`;
}
