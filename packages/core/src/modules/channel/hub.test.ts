import { describe, expect, it } from 'vitest';
import type { ChannelWire, TransportHooks } from './index';
import { hubTransport, type HubOptions } from './hub';
import { roomLink } from './room';
import { FakeRelay, fakeBrowser, memoryRoom, settle } from './rooms.fixture';

type Browser = ReturnType<typeof fakeBrowser>;
type Room = ReturnType<typeof memoryRoom>;

const USER = 'user:ann';
const op = (n: number): ChannelWire => ({ t: 'op', table: 'note', id: `note:${n}`, patch: { n }, hlc: [n, 0, 'x'] });

/** One tab of `USER` in `browser`: the transport and what it told the manager. */
function tab(browser: Browser, room: Room, peer: string, extra: Partial<HubOptions> = {}) {
  const events: string[] = [];
  const messages: Array<[string, unknown]> = [];
  const status: string[] = [];
  let signedIn = true;
  const hooks: TransportHooks = {
    self: () => (signedIn ? { user: USER, peer } : null),
    onMessage: (from, m) => messages.push([from.peer, m]),
    onPeer: (p, up) => events.push(`${up ? 'up' : 'down'} ${p.peer}`),
    onStatus: (_user, s) => status.push(s),
  };
  const t = hubTransport(hooks, { scope: (user) => `ns/db/${user}`, link: room.link, locks: browser.locks, bus: browser.bus, supported: true, ...extra });
  return {
    t,
    events,
    messages,
    status,
    peer,
    join: () => t.want(new Set([USER])),
    to: (...peers: string[]) => peers.map((p) => ({ user: USER, peer: p })),
    signOut: () => (signedIn = false),
  };
}

/** Two browsers, two tabs each, all in the room. */
async function twoByTwo() {
  const room = memoryRoom();
  const left = fakeBrowser();
  const right = fakeBrowser();
  const a1 = tab(left, room, 'a1');
  const a2 = tab(left, room, 'a2');
  const b1 = tab(right, room, 'b1');
  const b2 = tab(right, room, 'b2');
  for (const t of [a1, a2, b1, b2]) t.join();
  await settle();
  return { room, left, right, a1, a2, b1, b2 };
}

describe('hubTransport', () => {
  it('keeps one link per browser, and shows every tab the tabs of the other browsers', async () => {
    const { room, a1, a2, b1, b2 } = await twoByTwo();
    expect([...room.members.keys()].sort()).toEqual(['a1', 'b1']);
    for (const t of [a1, a2]) expect([...t.events].sort()).toEqual(['up b1', 'up b2']);
    for (const t of [b1, b2]) expect([...t.events].sort()).toEqual(['up a1', 'up a2']);
    for (const t of [a1, a2, b1, b2]) expect(t.status.at(-1)).toBe('connected');
  });

  it('carries a message from any tab to the tabs it names, as one frame', async () => {
    const { room, a1, a2, b1, b2 } = await twoByTwo();
    const before = room.frames.length;
    // The manager sends one message to each peer in turn.
    const m = op(1);
    for (const to of a2.to('b1', 'b2')) a2.t.send(to, m);
    await settle();
    expect(b1.messages).toEqual([['a2', m]]);
    expect(b2.messages).toEqual([['a2', m]]);
    expect(a1.messages).toEqual([]);
    expect(room.frames.length - before).toBe(1);

    b1.t.send(b1.to('a2')[0], op(2));
    await settle();
    expect(a2.messages).toEqual([['b1', op(2)]]);
    expect(a1.messages).toEqual([]);

    // Another user, or a tab nobody speaks for: nowhere to go.
    a1.t.send({ user: 'user:bob', peer: 'b1' }, op(3));
    a1.t.send({ user: USER, peer: 'ghost' }, op(4));
    await settle();
    expect(b1.messages).toHaveLength(1);
  });

  it('hands the link to the next tab when the one holding it closes', async () => {
    const { room, a1, a2, b1, b2 } = await twoByTwo();
    a1.t.close();
    await settle();
    expect([...room.members.keys()].sort()).toEqual(['a2', 'b1']);
    // The other browser saw the old link go and the remaining tab return.
    expect(b1.events.slice(2).sort()).toEqual(['down a1', 'down a2', 'up a2']);
    expect(b2.events.slice(2).sort()).toEqual(['down a1', 'down a2', 'up a2']);
    expect(a2.events.filter((e) => e.startsWith('up')).length).toBe(4);
    a2.t.send(a2.to('b2')[0], op(5));
    b1.t.send(b1.to('a2')[0], op(6));
    await settle();
    expect(b2.messages).toEqual([['a2', op(5)]]);
    expect(a2.messages).toEqual([['b1', op(6)]]);
  });

  it('tells the other browsers when a tab is gone, however it went', async () => {
    const { a1, a2, b1, b2 } = await twoByTwo();
    a2.t.close();
    await settle();
    expect(b1.events.at(-1)).toBe('down a2');
    expect(b2.events.at(-1)).toBe('down a2');
    expect(a1.events).toHaveLength(2);

    // A tab that crashes says no goodbye: its lock going free is what tells.
    const { room, left, right } = { room: memoryRoom(), left: fakeBrowser(), right: fakeBrowser() };
    const c1 = tab(left, room, 'c1');
    const c2 = tab(left, room, 'c2');
    const d1 = tab(right, room, 'd1');
    for (const t of [c1, c2, d1]) t.join();
    await settle();
    expect([...d1.events].sort()).toEqual(['up c1', 'up c2']);
    for (const b of left.opened) (b as unknown as { silent: boolean }).silent = true;
    c2.t.close();
    for (const b of left.opened) (b as unknown as { silent: boolean }).silent = false;
    await settle();
    expect(d1.events.at(-1)).toBe('down c2');
  });

  it('brings a tab opened later up to date', async () => {
    const { room, left, a1, b1 } = await twoByTwo();
    const a3 = tab(left, room, 'a3');
    a3.join();
    await settle();
    expect([...a3.events].sort()).toEqual(['up b1', 'up b2']);
    expect(a3.status.at(-1)).toBe('connected');
    expect(b1.events.at(-1)).toBe('up a3');
    expect(a1.events).toHaveLength(2);
    expect(room.members.size).toBe(2);
  });

  it('leaves when the user is no longer wanted, and returns when wanted again', async () => {
    const { room, a1, a2, b1 } = await twoByTwo();
    a1.t.want(new Set());
    await settle();
    expect(a1.events.slice(2).sort()).toEqual(['down b1', 'down b2']);
    expect([...room.members.keys()].sort()).toEqual(['a2', 'b1']);
    a1.t.want(new Set([USER]));
    await settle();
    expect(a1.events.slice(4).sort()).toEqual(['up b1', 'up b2']);
    expect(b1.events.at(-1)).toBe('up a1');
    // Signed out: nothing to join as.
    a2.signOut();
    a2.t.want(new Set([USER]));
    await settle();
    expect(room.members.has('a2')).toBe(false);
    a2.t.send(a2.to('b1')[0], op(7));
    await settle();
    expect(b1.messages).toEqual([]);
  });

  it('gives every tab a link of its own without Web Locks or a BroadcastChannel', async () => {
    const room = memoryRoom();
    const browser = fakeBrowser();
    const a1 = tab(browser, room, 'a1', { locks: null });
    const a2 = tab(browser, room, 'a2', { bus: null });
    a1.join();
    a2.join();
    await settle();
    expect([...room.members.keys()].sort()).toEqual(['a1', 'a2']);
    expect(a1.events).toEqual(['up a2']);
    a1.t.send(a1.to('a2')[0], op(8));
    await settle();
    expect(a2.messages).toEqual([['a1', op(8)]]);
    a1.t.close();
    await settle();
    expect(a2.events).toEqual(['up a1', 'down a1']);
  });

  it('knows the other browser\'s tabs again after its socket was replaced', async () => {
    // Over the real link and a relay: a socket that died where only the
    // client could tell comes back under the same id.
    const relay = new FakeRelay();
    const viaRelay = (hooks: Parameters<typeof roomLink>[0]) =>
      roomLink(hooks, { url: 'ws://relay.test/room', token: () => 't-ann', WebSocket: relay.WebSocket, direct: false });
    const room = { link: viaRelay } as unknown as Room;
    const a1 = tab(fakeBrowser(), room, 'a1');
    const right = fakeBrowser();
    const b1 = tab(right, room, 'b1');
    const b2 = tab(right, room, 'b2');
    for (const t of [a1, b1, b2]) t.join();
    await settle();
    expect([...a1.events].sort()).toEqual(['up b1', 'up b2']);

    relay.sockets[0].sever();
    await new Promise((r) => setTimeout(r, 900));
    await settle();
    expect(a1.status.at(-1)).toBe('connected');
    // Down with the socket, and up again once the other hub said its tabs anew.
    expect(a1.events.slice(2).sort()).toEqual(['down b1', 'down b2', 'up b1', 'up b2']);
    expect(b1.events).toEqual(['up a1', 'down a1', 'up a1']);
    a1.t.send(a1.to('b2')[0], op(11));
    b1.t.send(b1.to('a1')[0], op(12));
    await settle();
    expect(b2.messages).toEqual([['a1', op(11)]]);
    expect(a1.messages).toEqual([['b1', op(12)]]);
    for (const t of [a1, b1, b2]) t.t.close();
  });

  it('takes a message only from a tab its browser speaks for', async () => {
    const { room, b1, b2 } = await twoByTwo();
    const a1Link = room.members.get('a1');
    const forge = (data: unknown) => room.members.get('b1')?.onMessage('a1', data);
    forge({ h: 'm', from: 'zz', to: null, m: op(9) });
    forge({ h: 'm', from: 'a1', to: 'b1', m: op(9) });
    forge({ h: 'tabs', tabs: 'nope' });
    forge({ h: 'what' });
    forge(null);
    room.members.get('b1')?.onMessage('stranger', { h: 'm', from: 'a1', to: null, m: op(9) });
    await settle();
    expect(b1.messages).toEqual([]);
    expect(b2.messages).toEqual([]);
    forge({ h: 'm', from: 'a1', to: null, m: op(10) });
    await settle();
    expect(b1.messages).toEqual([['a1', op(10)]]);
    expect(b2.messages).toEqual([['a1', op(10)]]);
    expect(a1Link).toBeDefined();
  });
});
