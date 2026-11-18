import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ChannelTransport, TransportHooks } from './index';
import { roomLink, roomSupported, sameUser, splitTransport, type RoomLinkOptions } from './room';
import { FakeRelay, fakeWebRtc, settle } from './rooms.fixture';

/** One browser of `user:<name>`: a link to the relay and what it heard on it. */
function browser(relay: FakeRelay, name: string, peer: string, extra: Partial<RoomLinkOptions> = {}) {
  const events: string[] = [];
  const messages: Array<[string, unknown]> = [];
  const status: string[] = [];
  let token: string | null = `t-${name}`;
  const link = roomLink(
    {
      self: () => ({ user: `user:${name}`, peer }),
      onPeer: (p, up) => events.push(`${up ? 'up' : 'down'} ${p}`),
      onMessage: (from, data) => messages.push([from, data]),
      onStatus: (s) => status.push(s),
    },
    { url: 'ws://relay.test/room', token: () => token, WebSocket: relay.WebSocket, direct: false, ...extra }
  );
  return { link, events, messages, status, setToken: (t: string | null) => (token = t) };
}

/** Microtasks, then fake time. */
const advance = async (ms: number) => {
  await vi.advanceTimersByTimeAsync(ms);
};

afterEach(() => {
  vi.useRealTimers();
});

describe('roomLink', () => {
  it('joins the room of its user and learns who is in it', async () => {
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a');
    await settle();
    expect(a.status).toEqual(['connecting', 'connected']);
    expect(relay.sockets[0].sent[0]).toEqual({ t: 'hello', v: 1, token: 't-ann', peer: 'a' });
    const b = browser(relay, 'ann', 'b');
    const c = browser(relay, 'ann', 'c');
    await settle();
    expect(a.events).toEqual(['up b', 'up c']);
    expect(b.events).toEqual(['up a', 'up c']);
    expect(c.events).toEqual(['up a', 'up b']);
  });

  it('sends to everybody or to the browsers named, never to another user', async () => {
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a');
    const b = browser(relay, 'ann', 'b');
    const c = browser(relay, 'ann', 'c');
    const other = browser(relay, 'bob', 'x');
    await settle();
    a.link.send(null, { n: 1 });
    a.link.send(['c'], { n: 2 });
    a.link.send(['nobody', 'b'], { n: 3 });
    await settle();
    expect(b.messages).toEqual([
      ['a', { n: 1 }],
      ['a', { n: 3 }],
    ]);
    expect(c.messages).toEqual([
      ['a', { n: 1 }],
      ['a', { n: 2 }],
    ]);
    expect(a.messages).toEqual([]);
    expect(other.messages).toEqual([]);
    expect(other.events).toEqual([]);
    // To all of them is one frame without a list.
    expect(relay.sockets[0].sent.filter((f) => f.t === 'msg').map((f) => f.to)).toEqual([undefined, ['c'], ['b']]);
  });

  it('reports a browser that left, and ignores what it sends after', async () => {
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a');
    const b = browser(relay, 'ann', 'b');
    await settle();
    b.link.close();
    await settle();
    expect(a.events).toEqual(['up b', 'down b']);
    b.link.send(null, { late: true });
    a.link.send(['b'], { n: 1 });
    await settle();
    expect(a.messages).toEqual([]);
    expect(relay.rooms.get('user:ann')?.size).toBe(1);
  });

  it('comes back after the socket drops, with everybody down in between', async () => {
    vi.useFakeTimers();
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a');
    const b = browser(relay, 'ann', 'b');
    await advance(1);
    expect(a.events).toEqual(['up b']);
    relay.down = true;
    relay.sockets[0].drop();
    await advance(1);
    expect(a.events).toEqual(['up b', 'down b']);
    expect(a.status.at(-1)).toBe('connecting');
    a.link.send(null, { lost: true });
    // Refused while the relay is down, a little later each time.
    await advance(5_000);
    expect(a.status.at(-1)).toBe('failed');
    relay.down = false;
    await advance(40_000);
    expect(a.status.at(-1)).toBe('connected');
    expect(a.events).toEqual(['up b', 'down b', 'up b']);
    expect(b.messages).toEqual([]);
    a.link.close();
    b.link.close();
  });

  it('is refused without a valid token, and asks again later', async () => {
    vi.useFakeTimers();
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a');
    a.setToken('nonsense');
    // The first socket was opened with the good token already.
    relay.sockets[0].drop();
    await advance(1_000);
    expect(a.status.at(-1)).toBe('failed');
    expect(relay.rooms.size).toBe(0);
    const tries = relay.sockets.length;
    await advance(20_000);
    expect(relay.sockets.length).toBe(tries);
    a.setToken('t-ann');
    await advance(15_000);
    expect(a.status.at(-1)).toBe('connected');
    a.link.close();
  });

  it('waits for a token before it opens a socket', async () => {
    vi.useFakeTimers();
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a');
    relay.sockets[0].drop();
    a.setToken(null);
    await advance(3_000);
    const before = relay.sockets.length;
    await advance(3_000);
    expect(relay.sockets.length).toBe(before);
    a.setToken('t-ann');
    await advance(1_000);
    expect(a.status.at(-1)).toBe('connected');
    a.link.close();
  });

  it('signs in again at once when the relay ends a long session', async () => {
    vi.useFakeTimers();
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a');
    await advance(1);
    a.setToken('t-ann');
    relay.kick('user:ann', 'a', 'reauth');
    await advance(1_000);
    expect(a.status.at(-1)).toBe('connected');
    expect(relay.sockets).toHaveLength(2);
    a.link.close();
  });

  it('notices a socket that stopped answering', async () => {
    vi.useFakeTimers();
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a');
    await advance(1);
    await advance(20_000);
    expect(relay.sockets[0].sent.at(-1)).toEqual({ t: 'ping' });
    expect(relay.sockets[0].readyState).toBe(1);
    relay.mute = true;
    await advance(40_000);
    expect(relay.sockets[0].readyState).toBe(3);
    relay.mute = false;
    await advance(5_000);
    expect(a.status.at(-1)).toBe('connected');
    a.link.close();
  });

  it('does not join when the relay answers for another user', async () => {
    const relay = new FakeRelay();
    const warn = vi.fn();
    const events: string[] = [];
    const link = roomLink(
      { self: () => ({ user: 'user:ann', peer: 'a' }), onPeer: (p) => events.push(p), onMessage: () => {}, onStatus: (s) => events.push(s) },
      { url: 'ws://relay.test/room', token: () => 't-bob', WebSocket: relay.WebSocket, direct: false, logger: { warn, debug: () => {} } }
    );
    browser(relay, 'bob', 'x');
    await settle();
    expect(warn).toHaveBeenCalledTimes(1);
    expect(events).toEqual(['connecting', 'failed']);
    link.send(null, { n: 1 });
    link.close();
  });

  it('does nothing while nobody is signed in, or without WebSockets', async () => {
    const relay = new FakeRelay();
    const link = roomLink(
      { self: () => null, onPeer: () => {}, onMessage: () => {}, onStatus: () => {} },
      { url: 'ws://relay.test/room', token: () => 't-ann', WebSocket: relay.WebSocket }
    );
    await settle();
    expect(relay.sockets).toHaveLength(0);
    link.close();
    link.close();
    expect(roomSupported(relay.WebSocket)).toBe(true);
  });

  it('compares record ids whatever escaping the database added', () => {
    expect(sameUser('user:⟨a-b⟩', 'user:a-b')).toBe(true);
    expect(sameUser('user:`a-b`', 'user:⟨a-b⟩')).toBe(true);
    expect(sameUser("user:u'0190f2a4-7c1b-7d3e-9f00-795045881219'", 'user:0190f2a4-7c1b-7d3e-9f00-795045881219')).toBe(true);
    expect(sameUser('user:a', 'user:b')).toBe(false);
    expect(sameUser('user:ua', 'user:a')).toBe(false);
    expect(sameUser('admin:a', 'user:a')).toBe(false);
  });
});

describe('roomLink: the direct link of two browsers', () => {
  it('talks directly while there are two, and through the relay once there are three', async () => {
    const relay = new FakeRelay();
    const rtc = fakeWebRtc();
    const direct = { direct: true, RTCPeerConnection: rtc.RTCPeerConnection };
    const a = browser(relay, 'ann', 'a', direct);
    const b = browser(relay, 'ann', 'b', direct);
    await settle();
    expect(rtc.stats.opened).toBe(1);
    // The link came up over the relay, which now carries nothing more.
    const before = relay.forwarded;
    a.link.send(null, { n: 1 });
    b.link.send(['a'], { n: 2 });
    await settle();
    expect(b.messages).toEqual([['a', { n: 1 }]]);
    expect(a.messages).toEqual([['b', { n: 2 }]]);
    expect(relay.forwarded).toBe(before);
    // Peers are reported once, by the room: the direct link is not a second arrival.
    expect(a.events).toEqual(['up b']);

    const c = browser(relay, 'ann', 'c', direct);
    await settle();
    a.link.send(null, { n: 3 });
    await settle();
    expect(b.messages.at(-1)).toEqual(['a', { n: 3 }]);
    expect(c.messages).toEqual([['a', { n: 3 }]]);
    expect(relay.forwarded).toBe(before + 2);
    expect(rtc.stats.opened).toBe(1);

    // Back to two: direct again.
    c.link.close();
    await settle();
    expect(rtc.stats.opened).toBe(2);
    const again = relay.forwarded;
    b.link.send(null, { n: 4 });
    await settle();
    expect(a.messages.at(-1)).toEqual(['b', { n: 4 }]);
    expect(relay.forwarded).toBe(again);
    a.link.close();
    b.link.close();
  });

  it('stays on the relay when the runtime has no WebRTC', async () => {
    const relay = new FakeRelay();
    const a = browser(relay, 'ann', 'a', { direct: true, RTCPeerConnection: undefined });
    const b = browser(relay, 'ann', 'b', { direct: true, RTCPeerConnection: undefined });
    await settle();
    a.link.send(null, { n: 1 });
    await settle();
    expect(b.messages).toEqual([['a', { n: 1 }]]);
    a.link.close();
    b.link.close();
  });
});

describe('splitTransport', () => {
  const fake = (log: string[], name: string, supported = true): ChannelTransport => ({
    supported,
    want: (users) => log.push(`${name} want ${[...users].join(',')}`),
    send: (to) => log.push(`${name} send ${to.user}`),
    close: () => log.push(`${name} close`),
  });
  const hooks = (user: string | null): TransportHooks => ({
    self: () => (user ? { user, peer: 'p' } : null),
    onMessage: () => {},
    onPeer: () => {},
    onStatus: () => {},
  });

  it('reaches the own user through the room and everybody else through the mesh', () => {
    const log: string[] = [];
    const t = splitTransport(
      () => fake(log, 'room'),
      () => fake(log, 'mesh')
    )(hooks('user:a'));
    t.want(new Set(['user:a', 'user:b']));
    t.send({ user: 'user:a', peer: 'x' }, { t: 'interest', all: true });
    t.send({ user: 'user:b', peer: 'y' }, { t: 'interest', all: true });
    t.want(new Set(['user:b']));
    t.close();
    expect(log).toEqual(['room want user:a', 'mesh want user:b', 'room send user:a', 'mesh send user:b', 'room want ', 'mesh want user:b', 'room close', 'mesh close']);
    expect(t.supported).toBe(true);
  });

  it('is the mesh alone without a room', () => {
    const log: string[] = [];
    const t = splitTransport(null, () => fake(log, 'mesh', false))(hooks('user:a'));
    t.want(new Set(['user:a', 'user:b']));
    t.send({ user: 'user:a', peer: 'x' }, { t: 'interest', all: true });
    t.close();
    expect(log).toEqual(['mesh want user:b', 'mesh send user:a', 'mesh close']);
    expect(t.supported).toBe(false);
    splitTransport(null, () => fake(log, 'mesh'))(hooks(null)).want(new Set(['user:a']));
    expect(log.at(-1)).toBe('mesh want user:a');
  });
});
