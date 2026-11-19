import type { HubBus, HubLocks } from './hub';
import type { RoomLinkFactory, RoomLinkHooks, RoomPeer } from './room';

/**
 * Stand-ins for what the room code talks to: the relay (speaking protocol v1
 * as `apps/relay` documents it), a browser's Web Locks and BroadcastChannel,
 * and a WebRTC peer connection.
 */

// ---- the relay ---------------------------------------------------------------------

interface Member {
  readonly socket: FakeSocket;
  readonly user: string;
  readonly peer: string;
}

/** `t-<name>` is the session token of `user:<name>`; anything else is refused. */
const userOf = (token: unknown): string | null => (typeof token === 'string' && token.startsWith('t-') ? `user:${token.slice(2)}` : null);

export class FakeSocket {
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  /** Every frame the client sent, parsed. */
  readonly sent: Array<Record<string, unknown>> = [];
  member: Member | null = null;

  constructor(
    readonly relay: FakeRelay,
    readonly url: string
  ) {
    relay.sockets.push(this);
    queueMicrotask(() => {
      if (this.readyState !== 0) return;
      if (relay.down) return this.drop();
      this.readyState = 1;
      this.onopen?.();
    });
  }

  send(text: string): void {
    if (this.readyState !== 1) throw new Error('socket is not open');
    const frame = JSON.parse(text) as Record<string, unknown>;
    this.sent.push(frame);
    queueMicrotask(() => this.relay.receive(this, frame));
  }

  /** The relay's side: a frame for the client. */
  push(frame: unknown): void {
    const text = JSON.stringify(frame);
    queueMicrotask(() => {
      if (this.readyState === 1) this.onmessage?.({ data: text });
    });
  }

  close(): void {
    this.drop();
  }

  /** The connection ends, from either side. */
  drop(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.relay.left(this);
    queueMicrotask(() => this.onclose?.());
  }

  /**
   * The connection dies where only the client can tell (a phone changing
   * networks): the relay still holds the seat, until it times the socket out
   * or the same peer says hello again.
   */
  sever(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    queueMicrotask(() => this.onclose?.());
  }
}

export class FakeRelay {
  readonly sockets: FakeSocket[] = [];
  readonly rooms = new Map<string, Map<string, Member>>();
  /** Refuse every connection, as an unreachable relay does. */
  down = false;
  /** Answer the next hello with this error code instead of a welcome. */
  refuseWith: string | null = null;
  /** Do not answer pings (a dead connection the client has to notice). */
  mute = false;
  /** Frames forwarded between members: how much went through the relay. */
  forwarded = 0;

  /** What `roomLink` takes as its `WebSocket`. */
  get WebSocket(): typeof WebSocket {
    const open = (url: string): FakeSocket => new FakeSocket(this, url);
    // Called with `new`: a function that returns an object hands that object out.
    return function (url: string) {
      return open(url);
    } as unknown as typeof WebSocket;
  }

  receive(socket: FakeSocket, frame: Record<string, unknown>): void {
    if (socket.readyState !== 1) return;
    if (!socket.member) {
      const user = userOf(frame.token);
      const code = this.refuseWith ?? (frame.t !== 'hello' || typeof frame.peer !== 'string' ? 'bad_request' : user ? null : 'unauthorized');
      if (code || !user) {
        this.refuseWith = null;
        socket.push({ t: 'error', code });
        queueMicrotask(() => socket.drop());
        return;
      }
      const room = this.rooms.get(user) ?? new Map<string, Member>();
      this.rooms.set(user, room);
      const peer = frame.peer as string;
      const member: Member = { socket, user, peer };
      socket.member = member;
      socket.push({ t: 'welcome', user, peer, peers: [...room.keys()].filter((p) => p !== peer) });
      // The same peer again takes the seat: the old connection is told, and
      // the others hear it left and came back.
      const old = room.get(peer);
      if (old) {
        old.socket.push({ t: 'error', code: 'replaced' });
        old.socket.member = null;
        if (old.socket.readyState !== 3) queueMicrotask(() => old.socket.drop());
        for (const other of room.values()) if (other.peer !== peer) other.socket.push({ t: 'leave', peer });
      }
      for (const other of room.values()) if (other.peer !== peer) other.socket.push({ t: 'join', peer });
      room.set(peer, member);
      return;
    }
    if (frame.t === 'ping') {
      if (!this.mute) socket.push({ t: 'pong' });
      return;
    }
    if (frame.t !== 'msg') return;
    const { user, peer } = socket.member;
    const to = Array.isArray(frame.to) ? (frame.to as string[]) : null;
    for (const other of this.rooms.get(user)?.values() ?? []) {
      if (other.peer === peer || (to && !to.includes(other.peer))) continue;
      this.forwarded++;
      other.socket.push({ t: 'msg', from: peer, d: frame.d });
    }
  }

  left(socket: FakeSocket): void {
    const member = socket.member;
    if (!member) return;
    socket.member = null;
    const room = this.rooms.get(member.user);
    if (room?.get(member.peer) !== member) return;
    room.delete(member.peer);
    if (room.size === 0) this.rooms.delete(member.user);
    for (const other of room.values()) other.socket.push({ t: 'leave', peer: member.peer });
  }

  /** The relay tells a member something and hangs up (`reauth`, `replaced`, ...). */
  kick(user: string, peer: string, code: string): void {
    const member = this.rooms.get(user)?.get(peer);
    if (!member) return;
    member.socket.push({ t: 'error', code });
    queueMicrotask(() => member.socket.drop());
  }
}

// ---- a room without a relay: links that reach each other at once ------------------------

/** `RoomLink`s of one user's browsers, wired to each other in memory. */
export function memoryRoom() {
  const members = new Map<RoomPeer, RoomLinkHooks>();
  const frames: Array<{ from: RoomPeer; to: ReadonlyArray<RoomPeer> | null; data: unknown }> = [];
  const link: RoomLinkFactory = (hooks) => {
    const self = hooks.self();
    if (!self) throw new Error('a link needs somebody signed in');
    const id = self.peer;
    let open = true;
    queueMicrotask(() => {
      if (!open) return;
      // A member before it hears of the others, as after a relay's welcome:
      // what it sends them on hearing of them goes out.
      const others = [...members];
      members.set(id, hooks);
      hooks.onStatus('connected');
      for (const [peer, other] of others) {
        hooks.onPeer(peer, true);
        other.onPeer(id, true);
      }
    });
    return {
      send(to, data) {
        if (!open || !members.has(id)) return;
        frames.push({ from: id, to, data });
        const copy = JSON.stringify(data);
        for (const [peer, other] of members) {
          if (peer === id || (to && !to.includes(peer))) continue;
          queueMicrotask(() => {
            if (members.get(peer) === other) other.onMessage(id, JSON.parse(copy));
          });
        }
      },
      close() {
        if (!open) return;
        open = false;
        if (!members.delete(id)) return;
        for (const other of members.values()) other.onPeer(id, false);
      },
    };
  };
  return { link, members, frames };
}

// ---- a browser: its Web Locks and its BroadcastChannels ----------------------------------

interface Waiter {
  readonly shared: boolean;
  readonly grant: () => void;
}

/** One browser profile: tabs made from it see each other's locks and broadcasts. */
export function fakeBrowser() {
  const held = new Map<string, { shared: boolean; count: number }>();
  const queues = new Map<string, Waiter[]>();
  const buses = new Map<string, Set<FakeBus>>();

  const pump = (name: string): void => {
    const queue = queues.get(name) ?? [];
    while (queue.length > 0) {
      const lock = held.get(name);
      const next = queue[0];
      if (lock && !(lock.shared && next.shared)) return;
      queue.shift();
      held.set(name, { shared: next.shared, count: (lock?.count ?? 0) + 1 });
      next.grant();
    }
  };

  const locks: HubLocks = {
    request(name: string, a: unknown, b?: unknown): Promise<unknown> {
      const hold = (typeof a === 'function' ? a : b) as () => Promise<void> | void;
      const shared = typeof a === 'object' && a !== null && (a as { mode?: string }).mode === 'shared';
      return new Promise<void>((resolve) => {
        const queue = queues.get(name) ?? [];
        queues.set(name, queue);
        queue.push({
          shared,
          grant: () => {
            void Promise.resolve()
              .then(() => hold())
              .finally(() => {
                const lock = held.get(name);
                if (lock && --lock.count === 0) held.delete(name);
                resolve();
                pump(name);
              });
          },
        });
        pump(name);
      });
    },
  };

  class FakeBus implements HubBus {
    onmessage: ((event: { data: unknown }) => void) | null = null;
    /** A crashed tab: it says nothing anymore, not even goodbye. */
    silent = false;
    closed = false;
    constructor(readonly name: string) {
      const set = buses.get(name) ?? new Set<FakeBus>();
      buses.set(name, set);
      set.add(this);
    }
    postMessage(message: unknown): void {
      if (this.closed || this.silent) return;
      const copy = JSON.stringify(message);
      for (const other of buses.get(this.name) ?? []) {
        if (other === this) continue;
        queueMicrotask(() => {
          if (!other.closed) other.onmessage?.({ data: JSON.parse(copy) });
        });
      }
    }
    close(): void {
      this.closed = true;
      buses.get(this.name)?.delete(this);
    }
  }

  const opened: FakeBus[] = [];
  const bus = (name: string): HubBus => {
    const b = new FakeBus(name);
    opened.push(b);
    return b;
  };
  return { locks, bus, held, opened };
}

// ---- WebRTC ----------------------------------------------------------------------------

class FakeDataChannel {
  readyState = 'connecting';
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((e: { data: unknown }) => void) | null = null;
  other: FakeDataChannel | null = null;
  send(data: string) {
    queueMicrotask(() => this.other?.onmessage?.({ data }));
  }
  close() {
    if (this.readyState === 'closed') return;
    this.readyState = 'closed';
    this.onclose?.();
    this.other?.close();
  }
  open() {
    this.readyState = 'open';
    this.onopen?.();
  }
}

/** Peer connections that find each other through the SDPs they exchange. */
export function fakeWebRtc() {
  const offers = new Map<string, FakePC>();
  const all = new Set<FakePC>();
  let seq = 0;
  /** Data channels that opened: how many direct links there were. */
  const stats = { opened: 0 };

  class FakePC {
    connectionState = 'new';
    localDescription: { type: string; sdp: string } | null = null;
    onicecandidate: ((e: { candidate: { toJSON(): unknown } | null }) => void) | null = null;
    onconnectionstatechange: (() => void) | null = null;
    ondatachannel: ((e: { channel: FakeDataChannel }) => void) | null = null;
    dc: FakeDataChannel | null = null;
    remote: FakePC | null = null;
    constructor() {
      all.add(this);
    }
    createDataChannel() {
      this.dc = new FakeDataChannel();
      return this.dc;
    }
    async createOffer() {
      const sdp = `offer-${++seq}`;
      offers.set(sdp, this);
      return { type: 'offer', sdp };
    }
    async createAnswer() {
      return { type: 'answer', sdp: `answer-${++seq}` };
    }
    async setLocalDescription(d: { type: string; sdp: string }) {
      this.localDescription = d;
      queueMicrotask(() => this.onicecandidate?.({ candidate: { toJSON: () => ({ candidate: 'c' }) } }));
    }
    async setRemoteDescription(d: { type: string; sdp: string }) {
      if (d.type === 'offer') this.remote = offers.get(d.sdp) ?? null;
      if (d.type !== 'answer') return;
      const answerer = [...all].find((p) => p.remote === this);
      if (!answerer || !this.dc) return;
      const theirs = new FakeDataChannel();
      this.dc.other = theirs;
      theirs.other = this.dc;
      answerer.ondatachannel?.({ channel: theirs });
      queueMicrotask(() => {
        stats.opened++;
        this.dc?.open();
        theirs.open();
      });
    }
    async addIceCandidate() {}
    close() {
      this.dc?.close();
    }
  }
  return { RTCPeerConnection: FakePC as unknown as typeof RTCPeerConnection, stats };
}

/** Let queued microtasks and zero-delay timers run. */
export const settle = async (turns = 30): Promise<void> => {
  for (let i = 0; i < turns; i++) await new Promise((r) => setTimeout(r, 0));
};
