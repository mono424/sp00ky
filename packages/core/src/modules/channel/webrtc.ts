import type { Surreal, Uuid } from 'surrealdb';
import type { ConnectionState } from '../../types';
import type { Logger } from '../../services/logger/index';
import { parseRecordIdString } from '../../utils/index';
import type { ChannelMessage, ChannelTransport, PeerRef, TransportHooks } from './index';
import { idKey, peerKey } from './index';

/**
 * The default channel transport: one WebRTC data channel per pair of tabs,
 * set up through signaling rows in the database.
 *
 * Signaling (all rows of `_00_channel_signal`, read through one raw LIVE):
 *
 * 1. A tab that wants user U writes `hello` to U. Every tab of U hears it.
 * 2. Of each pair of tabs, the one with the lower `user#peer` key offers.
 *    A tab that hears a hello from a higher key offers at once; one that
 *    hears it from a lower key answers with a `hello` addressed back to that
 *    tab, which then offers. So both orders of arrival connect, and nobody
 *    loops.
 * 3. `offer` / `answer` carry the SDPs, `ice` batches of candidates
 *    (trickled, so nothing waits for gathering to finish).
 *
 * A tab only ever reacts to signals from users it wants itself, so a
 * connection needs both sides to list each other. The database stamps every
 * row's `sender`, and the offer carries the DTLS fingerprint of the
 * connection, so the user on the other end of a data channel is proven.
 */

export type SignalKind = 'hello' | 'offer' | 'answer' | 'ice';

export interface Signal {
  readonly from: string;
  readonly fromPeer: string;
  readonly toPeer: string | null;
  readonly kind: SignalKind;
  readonly payload: string | null;
}

export interface OutgoingSignal {
  readonly to: string;
  readonly toPeer: string | null;
  readonly fromPeer: string;
  readonly kind: SignalKind;
  readonly payload: string | null;
}

/** Where signals go and come from. The database by default. */
export interface SignalPort {
  send(signal: OutgoingSignal): Promise<void>;
  listen(self: PeerRef, cb: (signal: Signal) => void): () => void;
}

export interface WebRtcOptions {
  signal: SignalPort;
  iceServers?: RTCIceServer[];
  logger?: Pick<Logger, 'debug' | 'warn'>;
  /** Injectable for tests and non-browser runtimes. */
  RTCPeerConnection?: typeof RTCPeerConnection;
  /** Give up on one attempt after this long. Default 15000. */
  connectTimeoutMs?: number;
}

const DEFAULT_ICE: RTCIceServer[] = [{ urls: 'stun:stun.cloudflare.com:3478' }];
const ICE_FLUSH_MS = 20;
const RETRY_BASE_MS = 2000;
const RETRY_MAX_MS = 30_000;

interface Link {
  readonly ref: PeerRef;
  readonly key: string;
  readonly offerer: boolean;
  /** One negotiation: answers and candidates of another are ignored. */
  readonly sid: string;
  readonly pc: RTCPeerConnection;
  dc: RTCDataChannel | null;
  open: boolean;
  remoteSet: boolean;
  pendingIce: RTCIceCandidateInit[];
  outIce: RTCIceCandidateInit[];
  iceTimer: ReturnType<typeof setTimeout> | null;
  connectTimer: ReturnType<typeof setTimeout> | null;
  /** Signals of this link go out in order (offer before its candidates). */
  queue: Promise<void>;
  closed: boolean;
}

const randomSid = (): string => Math.random().toString(36).slice(2, 10);

export function webrtcTransport(hooks: TransportHooks, opts: WebRtcOptions): ChannelTransport {
  const PC = opts.RTCPeerConnection ?? (globalThis as { RTCPeerConnection?: typeof RTCPeerConnection }).RTCPeerConnection;
  const log = opts.logger;
  const connectTimeoutMs = opts.connectTimeoutMs ?? 15_000;
  const links = new Map<string, Link>();
  const retries = new Map<string, { attempts: number; timer: ReturnType<typeof setTimeout> | null }>();
  let wanted = new Set<string>();
  let stopListen: (() => void) | null = null;
  let listeningAs: string | null = null;
  let closed = false;

  const selfKey = (): string | null => {
    const self = hooks.self();
    return self ? peerKey(self) : null;
  };

  const send = (link: Link | null, signal: Omit<OutgoingSignal, 'fromPeer'>): void => {
    const self = hooks.self();
    if (!self) return;
    const out: OutgoingSignal = { ...signal, fromPeer: self.peer };
    const run = () =>
      opts.signal.send(out).catch((error) => {
        log?.debug({ error, kind: signal.kind, Category: 'sp00ky-client::channel' }, 'channel signal failed');
      });
    if (link) link.queue = link.queue.then(run);
    else void run();
  };

  const dropLink = (link: Link, failed: boolean): void => {
    if (link.closed) return;
    link.closed = true;
    if (link.iceTimer) clearTimeout(link.iceTimer);
    if (link.connectTimer) clearTimeout(link.connectTimer);
    try {
      link.dc?.close();
      link.pc.close();
    } catch {
      // already gone
    }
    if (links.get(link.key) === link) links.delete(link.key);
    if (link.open) {
      link.open = false;
      hooks.onPeer(link.ref, false);
    }
    if (failed && wanted.has(link.ref.user) && !closed) {
      const anyOpen = [...links.values()].some((l) => l.ref.user === link.ref.user && l.open);
      if (!anyOpen) hooks.onStatus(link.ref.user, 'failed');
      scheduleRetry(link.ref);
    }
  };

  const scheduleRetry = (ref: PeerRef): void => {
    const key = peerKey(ref);
    const r = retries.get(key) ?? { attempts: 0, timer: null };
    if (r.timer) return;
    const delay = Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** r.attempts);
    r.attempts++;
    r.timer = setTimeout(() => {
      r.timer = null;
      if (closed || !wanted.has(ref.user) || links.has(key)) return;
      const mine = selfKey();
      if (!mine) return;
      if (mine < key) startOffer(ref);
      else send(null, { to: ref.user, toPeer: ref.peer, kind: 'hello', payload: null });
    }, delay);
    retries.set(key, r);
  };

  const attachChannel = (link: Link, dc: RTCDataChannel): void => {
    link.dc = dc;
    dc.onopen = () => {
      if (link.closed) return;
      link.open = true;
      if (link.connectTimer) clearTimeout(link.connectTimer);
      link.connectTimer = null;
      retries.delete(link.key);
      hooks.onPeer(link.ref, true);
      hooks.onStatus(link.ref.user, 'connected');
    };
    dc.onclose = () => dropLink(link, true);
    dc.onmessage = (event) => {
      if (typeof event.data !== 'string') return;
      let message: unknown;
      try {
        message = JSON.parse(event.data);
      } catch {
        return;
      }
      hooks.onMessage(link.ref, message);
    };
  };

  const newLink = (ref: PeerRef, offerer: boolean, sid: string): Link | null => {
    if (!PC) return null;
    const key = peerKey(ref);
    const prev = links.get(key);
    if (prev) dropLink(prev, false);
    const pc = new PC({ iceServers: opts.iceServers ?? DEFAULT_ICE });
    const link: Link = {
      ref,
      key,
      offerer,
      sid,
      pc,
      dc: null,
      open: false,
      remoteSet: false,
      pendingIce: [],
      outIce: [],
      iceTimer: null,
      connectTimer: null,
      queue: Promise.resolve(),
      closed: false,
    };
    links.set(key, link);
    pc.onicecandidate = (event) => {
      if (!event.candidate || link.closed) return;
      link.outIce.push(event.candidate.toJSON());
      link.iceTimer ??= setTimeout(() => {
        link.iceTimer = null;
        const candidates = link.outIce.splice(0);
        if (candidates.length === 0 || link.closed) return;
        send(link, { to: ref.user, toPeer: ref.peer, kind: 'ice', payload: JSON.stringify({ sid, candidates }) });
      }, ICE_FLUSH_MS);
    };
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === 'failed' || pc.connectionState === 'closed') dropLink(link, true);
    };
    if (!offerer) pc.ondatachannel = (event) => attachChannel(link, event.channel);
    link.connectTimer = setTimeout(() => {
      if (!link.open) dropLink(link, true);
    }, connectTimeoutMs);
    hooks.onStatus(ref.user, 'connecting');
    return link;
  };

  const startOffer = (ref: PeerRef): void => {
    const link = newLink(ref, true, randomSid());
    if (!link) return;
    attachChannel(link, link.pc.createDataChannel('sp00ky', { ordered: true }));
    void (async () => {
      try {
        const offer = await link.pc.createOffer();
        await link.pc.setLocalDescription(offer);
        send(link, {
          to: ref.user,
          toPeer: ref.peer,
          kind: 'offer',
          payload: JSON.stringify({ sid: link.sid, sdp: link.pc.localDescription?.sdp ?? offer.sdp }),
        });
      } catch (error) {
        log?.debug({ error, Category: 'sp00ky-client::channel' }, 'channel offer failed');
        dropLink(link, true);
      }
    })();
  };

  const addIce = async (link: Link, candidates: RTCIceCandidateInit[]): Promise<void> => {
    for (const c of candidates) {
      try {
        await link.pc.addIceCandidate(c);
      } catch {
        // a candidate of a torn-down negotiation, or one the browser rejects
      }
    }
  };

  const onSignal = (s: Signal): void => {
    if (closed) return;
    const self = hooks.self();
    if (!self) return;
    const from = idKey(s.from);
    if (!from || !wanted.has(from)) return;
    if (s.toPeer !== null && s.toPeer !== self.peer) return;
    const ref: PeerRef = { user: from, peer: s.fromPeer };
    const key = peerKey(ref);
    const mine = peerKey(self);
    if (key === mine) return;
    const payload = parsePayload(s.payload);
    const link = links.get(key);

    switch (s.kind) {
      case 'hello': {
        if (link && !link.closed) return;
        if (mine < key) startOffer(ref);
        else if (s.toPeer === null) send(null, { to: from, toPeer: ref.peer, kind: 'hello', payload: null });
        return;
      }
      case 'offer': {
        if (mine < key || typeof payload?.sdp !== 'string' || typeof payload.sid !== 'string') return;
        const answerer = newLink(ref, false, payload.sid);
        if (!answerer) return;
        void (async () => {
          try {
            await answerer.pc.setRemoteDescription({ type: 'offer', sdp: payload.sdp as string });
            answerer.remoteSet = true;
            await addIce(answerer, answerer.pendingIce.splice(0));
            const answer = await answerer.pc.createAnswer();
            await answerer.pc.setLocalDescription(answer);
            send(answerer, {
              to: from,
              toPeer: ref.peer,
              kind: 'answer',
              payload: JSON.stringify({ sid: answerer.sid, sdp: answerer.pc.localDescription?.sdp ?? answer.sdp }),
            });
          } catch (error) {
            log?.debug({ error, Category: 'sp00ky-client::channel' }, 'channel answer failed');
            dropLink(answerer, true);
          }
        })();
        return;
      }
      case 'answer': {
        if (!link || !link.offerer || link.remoteSet || payload?.sid !== link.sid || typeof payload.sdp !== 'string') return;
        link.remoteSet = true;
        void link.pc
          .setRemoteDescription({ type: 'answer', sdp: payload.sdp })
          .then(() => addIce(link, link.pendingIce.splice(0)))
          .catch(() => dropLink(link, true));
        return;
      }
      case 'ice': {
        if (!link || payload?.sid !== link.sid || !Array.isArray(payload.candidates)) return;
        const candidates = payload.candidates as RTCIceCandidateInit[];
        if (link.remoteSet) void addIce(link, candidates);
        else link.pendingIce.push(...candidates);
        return;
      }
    }
  };

  const ensureListening = (): void => {
    const self = hooks.self();
    const as = self ? peerKey(self) : null;
    if (as === listeningAs) return;
    stopListen?.();
    stopListen = null;
    listeningAs = as;
    if (self) stopListen = opts.signal.listen(self, onSignal);
  };

  return {
    supported: PC !== undefined,
    want(users) {
      if (closed || !PC) {
        wanted = new Set(users);
        return;
      }
      const before = wanted;
      wanted = new Set(users);
      if (wanted.size > 0) ensureListening();
      for (const link of [...links.values()]) if (!wanted.has(link.ref.user)) dropLink(link, false);
      for (const [key, r] of retries) {
        if (!wanted.has(key.slice(0, key.lastIndexOf('#'))) && r.timer) clearTimeout(r.timer);
      }
      for (const user of wanted) {
        if (before.has(user)) continue;
        send(null, { to: user, toPeer: null, kind: 'hello', payload: null });
        hooks.onStatus(user, 'connecting');
      }
    },
    send(to, message: ChannelMessage) {
      const link = links.get(peerKey(to));
      if (!link?.open || !link.dc || link.dc.readyState !== 'open') return;
      try {
        link.dc.send(JSON.stringify(message));
      } catch (error) {
        log?.debug({ error, Category: 'sp00ky-client::channel' }, 'channel send failed');
      }
    },
    close() {
      closed = true;
      for (const r of retries.values()) if (r.timer) clearTimeout(r.timer);
      retries.clear();
      for (const link of [...links.values()]) dropLink(link, false);
      stopListen?.();
      stopListen = null;
    },
  };
}

function parsePayload(payload: string | null): Record<string, unknown> | null {
  if (!payload) return null;
  try {
    const v = JSON.parse(payload);
    return v !== null && typeof v === 'object' ? (v as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

// ---- signaling over SurrealDB ----------------------------------------------------

/** The bits of the remote service signaling needs. */
export interface SignalRemote {
  query<T extends unknown[]>(sql: string, vars?: Record<string, unknown>): Promise<T>;
  getClient(): Pick<Surreal, 'liveOf'>;
}

export interface SignalConnection {
  subscribe(cb: (state: ConnectionState) => void): () => void;
}

const SIGNAL_TABLE = '_00_channel_signal';
const LISTEN_RETRY_MS = 3000;

/**
 * Signals as rows of `_00_channel_signal` (generated for schemas with a
 * `@channel` table): a CREATE to send, one raw `LIVE SELECT` on rows
 * addressed to this user to receive. Not the outbox and not a synced query:
 * each signal costs one round trip, which is what keeps connection setup
 * short. The LIVE is re-armed whenever the socket reconnects.
 */
export function surrealSignal(
  remote: SignalRemote,
  connection: SignalConnection,
  logger?: Pick<Logger, 'debug' | 'warn'>
): SignalPort {
  return {
    async send(s) {
      const content: Record<string, unknown> = {
        recipient: parseRecordIdString(s.to),
        sender_peer: s.fromPeer,
        kind: s.kind,
      };
      if (s.toPeer !== null) content.recipient_peer = s.toPeer;
      if (s.payload !== null) content.payload = s.payload;
      await remote.query(`CREATE ${SIGNAL_TABLE} CONTENT $c RETURN NONE`, { c: content });
    },
    listen(self, cb) {
      let stopped = false;
      let uuid: Uuid | null = null;
      let retry: ReturnType<typeof setTimeout> | null = null;
      let state: ConnectionState | null = null;
      const arm = async () => {
        if (stopped) return;
        if (retry) clearTimeout(retry);
        retry = null;
        const old = uuid;
        uuid = null;
        if (old) void remote.query('KILL $u', { u: old }).catch(() => {});
        try {
          const [id] = await remote.query<[Uuid]>(`LIVE SELECT * FROM ${SIGNAL_TABLE} WHERE recipient = $me`, {
            me: parseRecordIdString(self.user),
          });
          if (stopped) {
            void remote.query('KILL $u', { u: id }).catch(() => {});
            return;
          }
          uuid = id;
          const live = await remote.getClient().liveOf(id);
          live.subscribe((message) => {
            if (stopped || message.action !== 'CREATE') return;
            const v = message.value as Record<string, unknown>;
            const from = idKey(v.sender);
            if (!from || typeof v.sender_peer !== 'string' || typeof v.kind !== 'string') return;
            cb({
              from,
              fromPeer: v.sender_peer,
              toPeer: typeof v.recipient_peer === 'string' ? v.recipient_peer : null,
              kind: v.kind as SignalKind,
              payload: typeof v.payload === 'string' ? v.payload : null,
            });
          });
        } catch (error) {
          logger?.debug({ error, Category: 'sp00ky-client::channel' }, 'channel signal listen failed; retrying');
          if (!stopped) retry = setTimeout(() => void arm(), LISTEN_RETRY_MS);
        }
      };
      const unsubscribe = connection.subscribe((next) => {
        const was = state;
        state = next;
        if (next === 'connected' && was !== null && was !== 'connected') void arm();
      });
      void arm();
      return () => {
        stopped = true;
        unsubscribe();
        if (retry) clearTimeout(retry);
        if (uuid) void remote.query('KILL $u', { u: uuid }).catch(() => {});
      };
    },
  };
}
