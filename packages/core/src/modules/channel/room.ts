import type { Logger } from '../../services/logger/index';
import type { ChannelTransport, ChannelTransportFactory, ChannelWire, TransportHooks } from './index';
import { webrtcTransport, type Signal, type SignalKind } from './webrtc';

/**
 * The link between the browsers of one user.
 *
 * Every browser keeps one socket to the room relay (`sp00ky-relay`, see
 * `apps/relay`). The relay proves the user from the session token, puts the
 * socket into that user's room and forwards what the members send each
 * other without reading it. So a message is there after one hop to the relay
 * and one back, whatever network each device sits in, and nothing waits for
 * a connection to be negotiated.
 *
 * Two browsers alone in a room also open a WebRTC data channel to each other
 * (its offer and answer travel through the relay) and use it while it is up:
 * on one network that is a millisecond instead of a trip to the relay. A
 * third browser joining ends that, and everybody talks through the room.
 *
 * Nothing here is kept or retried: a message lost on the way is one the
 * normal sync delivers a moment later.
 */

/** A browser in the room, by the id it joined with. */
export type RoomPeer = string;

export interface RoomLinkHooks {
  /** Who this browser is, or null while signed out. */
  self(): { user: string; peer: RoomPeer } | null;
  onPeer(peer: RoomPeer, up: boolean): void;
  onMessage(from: RoomPeer, data: unknown): void;
  onStatus(status: 'connecting' | 'connected' | 'failed'): void;
}

export interface RoomLink {
  /** Send to these browsers of the room, or (`null`) to all of them. Fire and forget. */
  send(to: ReadonlyArray<RoomPeer> | null, data: unknown): void;
  close(): void;
}

export type RoomLinkFactory = (hooks: RoomLinkHooks) => RoomLink;

export interface RoomLinkOptions {
  /** The relay's WebSocket address. */
  url: string;
  /** The session token the relay proves the user with; null while there is none. */
  token: () => string | null;
  /** `false`: never open the direct link. */
  direct?: boolean;
  iceServers?: RTCIceServer[];
  logger?: Pick<Logger, 'debug' | 'warn'>;
  /** Injectable for tests and non-browser runtimes. */
  WebSocket?: typeof WebSocket;
  RTCPeerConnection?: typeof RTCPeerConnection;
}

const PROTOCOL = 1;
const RETRY_BASE_MS = 500;
const RETRY_MAX_MS = 30_000;
/** After a refusal that a retry will not fix soon (not signed in at the relay, room full). */
const RETRY_REFUSED_MS = 30_000;
const BEAT_MS = 20_000;

const SIGNAL_KINDS: ReadonlySet<string> = new Set<SignalKind>(['hello', 'offer', 'answer', 'ice']);

/**
 * Two spellings of one record id. The database prints a key the way it would
 * have to be written (`user:⟨a-b⟩`, backticks, or `user:u'0190…'` for a
 * uuid), the client keeps it bare (`user:a-b`): the wrapping is not part of
 * who it is. Verified against SurrealDB 4.0.1.
 */
const bareId = (id: string): string => {
  const at = id.indexOf(':');
  return id.slice(0, at + 1) + id.slice(at + 1).replace(/^u(?=['"])/, '').replace(/[⟨⟩`'"]/g, '');
};

export const sameUser = (a: string, b: string): boolean => bareId(a) === bareId(b);

/** Whether this runtime can hold a room link at all. */
export const roomSupported = (ws?: typeof WebSocket): boolean =>
  (ws ?? (globalThis as { WebSocket?: typeof WebSocket }).WebSocket) !== undefined;

export function roomLink(hooks: RoomLinkHooks, opts: RoomLinkOptions): RoomLink {
  const WS = opts.WebSocket ?? (globalThis as { WebSocket?: typeof WebSocket }).WebSocket;
  const log = opts.logger;
  const peers = new Set<RoomPeer>();
  /** Members the data channel to is open: they get their messages there. */
  const directOpen = new Set<RoomPeer>();
  let socket: WebSocket | null = null;
  let welcomed = false;
  let closed = false;
  let attempts = 0;
  let retry: ReturnType<typeof setTimeout> | null = null;
  let beat: ReturnType<typeof setInterval> | null = null;
  let awaitingPong = false;
  let onSignal: ((s: Signal) => void) | null = null;

  const relay = (to: ReadonlyArray<RoomPeer> | null, d: unknown): void => {
    const ws = socket;
    if (!ws || !welcomed || ws.readyState !== 1) return;
    try {
      ws.send(JSON.stringify(to === null ? { t: 'msg', d } : { t: 'msg', to, d }));
    } catch (error) {
      log?.debug({ error, Category: 'sp00ky-client::channel' }, 'room send failed');
    }
  };

  // The direct link is the WebRTC transport the channels between users use,
  // with the relay as its signaling: both ends are members of one room, which
  // is what proves they are the same user.
  const direct: ChannelTransport | null =
    opts.direct === false
      ? null
      : webrtcTransport(
          {
            self: () => hooks.self(),
            onPeer: (ref, up) => {
              if (up) directOpen.add(ref.peer);
              else directOpen.delete(ref.peer);
            },
            onMessage: (ref, message) => {
              if (peers.has(ref.peer)) hooks.onMessage(ref.peer, message);
            },
            onStatus: () => {},
          },
          {
            signal: {
              send: async (s) => relay(s.toPeer === null ? null : [s.toPeer], { s: { kind: s.kind, payload: s.payload, toPeer: s.toPeer } }),
              listen: (_self, cb) => {
                onSignal = cb;
                return () => {
                  if (onSignal === cb) onSignal = null;
                };
              },
            },
            iceServers: opts.iceServers,
            logger: log,
            RTCPeerConnection: opts.RTCPeerConnection,
          }
        );

  /** The direct link is for two: wanted while there is exactly one other member. */
  const syncDirect = (): void => {
    const self = hooks.self();
    direct?.want(new Set(self && welcomed && peers.size === 1 ? [self.user] : []));
  };

  const stopBeat = (): void => {
    if (beat) clearInterval(beat);
    beat = null;
    awaitingPong = false;
  };

  const scheduleRetry = (delay: number): void => {
    if (closed || retry) return;
    retry = setTimeout(() => {
      retry = null;
      connect();
    }, delay);
  };

  /** The socket is gone: every member with it, and the direct link (it has nobody to prove its peer anymore). */
  const dropped = (delay?: number): void => {
    socket = null;
    stopBeat();
    const was = [...peers];
    peers.clear();
    welcomed = false;
    syncDirect();
    for (const p of was) hooks.onPeer(p, false);
    if (closed) return;
    attempts++;
    hooks.onStatus(attempts > 2 || delay !== undefined ? 'failed' : 'connecting');
    scheduleRetry(delay ?? Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** (attempts - 1)) * (0.75 + Math.random() * 0.5));
  };

  const onFrame = (ws: WebSocket, text: string, self: { user: string; peer: RoomPeer }): void => {
    let f: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(text);
      if (parsed === null || typeof parsed !== 'object') return;
      f = parsed as Record<string, unknown>;
    } catch {
      return;
    }
    switch (f.t) {
      case 'welcome': {
        if (welcomed) return;
        if (typeof f.user !== 'string' || !sameUser(f.user, self.user)) {
          // The token belongs to somebody else than the client thinks is
          // signed in: not a room to speak in under this name.
          log?.warn({ Category: 'sp00ky-client::channel' }, 'the room relay answered for another user; not joining');
          socket = null;
          ws.close();
          dropped(RETRY_REFUSED_MS);
          return;
        }
        welcomed = true;
        attempts = 0;
        awaitingPong = false;
        beat = setInterval(() => {
          // Two beats without an answer in between: the socket is dead, however
          // far apart a hidden tab's timers were allowed to fire.
          if (awaitingPong) return ws.close();
          awaitingPong = true;
          try {
            ws.send('{"t":"ping"}');
          } catch {
            // closing: `onclose` does the rest
          }
        }, BEAT_MS);
        hooks.onStatus('connected');
        for (const p of Array.isArray(f.peers) ? f.peers : []) {
          if (typeof p !== 'string' || p === self.peer || peers.has(p)) continue;
          peers.add(p);
          hooks.onPeer(p, true);
        }
        syncDirect();
        return;
      }
      case 'join': {
        if (!welcomed || typeof f.peer !== 'string' || f.peer === self.peer || peers.has(f.peer)) return;
        peers.add(f.peer);
        hooks.onPeer(f.peer, true);
        syncDirect();
        return;
      }
      case 'leave': {
        if (typeof f.peer !== 'string' || !peers.delete(f.peer)) return;
        hooks.onPeer(f.peer, false);
        syncDirect();
        return;
      }
      case 'msg': {
        if (typeof f.from !== 'string' || !peers.has(f.from) || f.d === null || typeof f.d !== 'object') return;
        const d = f.d as { s?: unknown; m?: unknown };
        if (d.s !== undefined) {
          const s = d.s as { kind?: unknown; payload?: unknown; toPeer?: unknown } | null;
          if (!s || typeof s.kind !== 'string' || !SIGNAL_KINDS.has(s.kind)) return;
          onSignal?.({
            from: self.user,
            fromPeer: f.from,
            toPeer: typeof s.toPeer === 'string' ? s.toPeer : null,
            kind: s.kind as SignalKind,
            payload: typeof s.payload === 'string' ? s.payload : null,
          });
          return;
        }
        if ('m' in d) hooks.onMessage(f.from, d.m);
        return;
      }
      case 'pong':
        awaitingPong = false;
        return;
      case 'error': {
        const code = typeof f.code === 'string' ? f.code : 'unknown';
        log?.debug({ code, Category: 'sp00ky-client::channel' }, 'room relay refused');
        socket = null;
        ws.close();
        // `reauth` is the relay ending a long session: come back with the
        // token of now. Everything else is not fixed by asking again at once.
        if (code === 'reauth') {
          attempts = 0;
          dropped();
        } else dropped(RETRY_REFUSED_MS);
        return;
      }
    }
  };

  function connect(): void {
    if (closed || socket || !WS) return;
    const self = hooks.self();
    if (!self) return;
    const token = opts.token();
    if (!token) {
      // Signed in, and the session has no token yet (it is being restored).
      scheduleRetry(RETRY_BASE_MS);
      return;
    }
    let ws: WebSocket;
    try {
      ws = new WS(opts.url);
    } catch (error) {
      log?.warn({ error, Category: 'sp00ky-client::channel' }, 'the room relay address cannot be opened');
      hooks.onStatus('failed');
      return;
    }
    socket = ws;
    ws.onopen = () => {
      if (socket === ws) ws.send(JSON.stringify({ t: 'hello', v: PROTOCOL, token, peer: self.peer }));
    };
    ws.onmessage = (event: MessageEvent) => {
      if (socket === ws && typeof event.data === 'string') onFrame(ws, event.data, self);
    };
    ws.onclose = () => {
      if (socket === ws) dropped();
    };
    ws.onerror = () => {
      // `onclose` follows and does the work.
    };
  }

  hooks.onStatus('connecting');
  connect();

  return {
    send(to, data) {
      if (closed || !welcomed) return;
      const self = hooks.self();
      const targets = to === null ? [...peers] : to.filter((p) => peers.has(p));
      const viaRelay: RoomPeer[] = [];
      for (const p of targets) {
        if (direct && self && directOpen.has(p)) direct.send({ user: self.user, peer: p }, data as ChannelWire);
        else viaRelay.push(p);
      }
      if (viaRelay.length > 0) relay(viaRelay.length === peers.size ? null : viaRelay, { m: data });
    },
    close() {
      if (closed) return;
      closed = true;
      if (retry) clearTimeout(retry);
      retry = null;
      stopBeat();
      direct?.close();
      const ws = socket;
      socket = null;
      welcomed = false;
      peers.clear();
      directOpen.clear();
      try {
        ws?.close();
      } catch {
        // already gone
      }
    },
  };
}

/**
 * One transport out of two: the user's own devices are reached through
 * `own` (the room), everybody else through `others` (the mesh between
 * users). Without `own`, the user's own devices are simply never reached.
 */
export function splitTransport(own: ChannelTransportFactory | null, others: ChannelTransportFactory): ChannelTransportFactory {
  return (hooks: TransportHooks): ChannelTransport => {
    const mesh = others(hooks);
    const room = own ? own(hooks) : null;
    return {
      supported: mesh.supported || (room?.supported ?? false),
      want(users) {
        const self = hooks.self()?.user ?? null;
        const rest = new Set(users);
        const mine = self !== null && rest.delete(self);
        room?.want(new Set(mine && self !== null ? [self] : []));
        mesh.want(rest);
      },
      send(to, message) {
        (room && to.user === hooks.self()?.user ? room : mesh).send(to, message);
      },
      close() {
        room?.close();
        mesh.close();
      },
    };
  };
}
