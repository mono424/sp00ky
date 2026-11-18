import { describe, expect, it, vi } from 'vitest';
import { RecordId } from 'surrealdb';
import type { QueryPlan, SchemaStructure } from '@spooky-sync/query-builder';
import {
  ChannelManager,
  decodeInterest,
  encodeInterest,
  mergeInterest,
  peerKey,
  queryInterest,
  showsRow,
  type ChannelTransport,
  type ChannelTransportFactory,
  type ChannelsConfig,
  type PeerRef,
  type Row,
  type TransportHooks,
} from './index';
import { hubTransport } from './hub';
import { fakeBrowser, memoryRoom, settle } from './rooms.fixture';

const schema = {
  tables: [
    {
      name: 'note',
      columns: {
        id: { type: 'string', optional: false, recordId: true },
        folder: { type: 'string', optional: false },
        title: { type: 'string', optional: false },
        draft: { type: 'boolean', optional: true },
        author: { type: 'string', optional: true, recordId: true },
      },
      primaryKey: ['id'],
    },
    {
      name: 'tag',
      columns: { id: { type: 'string', optional: false, recordId: true }, note: { type: 'string', optional: false, recordId: true }, label: { type: 'string', optional: false } },
      primaryKey: ['id'],
    },
    {
      name: 'secret',
      columns: { id: { type: 'string', optional: false, recordId: true }, text: { type: 'string', optional: false } },
      primaryKey: ['id'],
    },
    {
      name: 'call',
      columns: { id: { type: 'string', optional: false, recordId: true }, view_id: { type: 'string', optional: true } },
      channel: true,
      primaryKey: ['id'],
    },
  ],
  relationships: [],
  backends: {},
} as unknown as SchemaStructure;

const columnsOf = (table: string) => schema.tables.find((t) => t.name === table)?.columns;
const eq = (field: string, value: unknown) => ({ field, op: '=', value });
const inFolder = (folder: string): QueryPlan => ({ table: 'note', where: [eq('folder', folder)] });

describe('interest', () => {
  it('reads what a query can show from its plan', () => {
    expect(queryInterest(columnsOf, inFolder('a'))).toEqual([{ table: 'note', where: [eq('folder', 'a')] }]);
    expect(queryInterest(columnsOf, { table: 'note' })).toEqual([{ table: 'note', where: [] }]);
    // A parameter is filled in from the query's own.
    expect(queryInterest(columnsOf, { table: 'note', where: [{ field: 'folder', op: '=', value: undefined, paramRef: 'f' }] }, { f: 'x' })).toEqual([
      { table: 'note', where: [eq('folder', 'x')] },
    ]);
    // `_or` groups and swapped operands keep their shape.
    expect(queryInterest(columnsOf, { table: 'note', where: [{ or: [eq('folder', 'a'), { field: 'folder', op: '∈', value: ['b'], swap: true }] }] })).toEqual([
      { table: 'note', where: [{ or: [eq('folder', 'a'), { field: 'folder', op: '∈', value: ['b'], swap: true }] }] },
    ]);
    // What the row alone cannot decide is the whole table.
    for (const where of [
      [{ field: 'title', op: '@@', value: 'x' }],
      [eq('author.name', 'x')],
      [{ field: 'folder', op: '=', value: undefined, paramRef: 'missing' }],
    ]) {
      expect(queryInterest(columnsOf, { table: 'note', where })).toEqual([{ table: 'note', where: null }]);
    }
    expect(queryInterest(columnsOf, { table: 'note', ids: ['note:1'], where: [eq('folder', 'a')] })).toEqual([{ table: 'note', where: null }]);
    // Related tables: every row, however deep.
    const related = queryInterest(columnsOf, {
      table: 'note',
      where: [eq('folder', 'a')],
      relations: [{ alias: 'tags', table: 'tag', cardinality: 'many', foreignKeyField: 'note', relations: [{ alias: 'x', table: 'secret', cardinality: 'one', foreignKeyField: 'y' }] }],
    });
    expect(related.map((q) => [q.table, q.where === null])).toEqual([
      ['note', false],
      ['tag', true],
      ['secret', true],
    ]);
  });

  it('merges the open queries into one claim per shared table', () => {
    const shared = (t: string) => t === 'note' || t === 'tag';
    expect(mergeInterest([], shared)).toEqual({});
    expect(mergeInterest([[{ table: 'secret', where: null }]], shared)).toEqual({});
    expect(
      mergeInterest(
        [
          [{ table: 'note', where: [eq('folder', 'a')] }],
          [{ table: 'note', where: [eq('folder', 'b')] }, { table: 'tag', where: null }],
        ],
        shared
      )
    ).toEqual({ note: [[eq('folder', 'a')], [eq('folder', 'b')]], tag: true });
    // The same filter twice is one, and the order of the queries does not show.
    const twice = [[{ table: 'note', where: [eq('folder', 'b')] }], [{ table: 'note', where: [eq('folder', 'a')] }], [{ table: 'note', where: [eq('folder', 'b')] }]];
    expect(mergeInterest(twice, shared)).toEqual({ note: [[eq('folder', 'a')], [eq('folder', 'b')]] });
    // An unfiltered query, or one nobody described, asks for everything.
    expect(mergeInterest([[{ table: 'note', where: [eq('folder', 'a')] }], [{ table: 'note', where: [] }], [{ table: 'note', where: [eq('folder', 'c')] }]], shared)).toEqual({ note: true });
    expect(mergeInterest([[{ table: 'note', where: [eq('folder', 'a')] }], null], shared)).toBe('all');
    const many = Array.from({ length: 30 }, (_, i) => [{ table: 'note', where: [eq('folder', `f${i}`)] }]);
    expect(mergeInterest(many, shared)).toEqual({ note: true });
  });

  it('survives the wire, record ids and dates included', () => {
    const interest = { note: [[eq('author', new RecordId('user', 'a')), { or: [eq('folder', 'a'), { field: 'at', op: '>', value: new Date(5), swap: true }] }]], tag: true as const };
    const wire = JSON.parse(JSON.stringify(encodeInterest(interest)));
    const back = decodeInterest(wire);
    expect(back).not.toBeNull();
    expect(showsRow(back!, 'note', [{ author: 'user:a', folder: 'a' }])).toBe(true);
    expect(showsRow(back!, 'note', [{ author: 'user:b', folder: 'a' }])).toBe(false);
    expect(showsRow(back!, 'tag', [{}])).toBe(true);
    expect(showsRow(back!, 'secret', [{}])).toBe(false);
    expect(decodeInterest(JSON.parse(JSON.stringify(encodeInterest('all'))))).toBe('all');
    expect(showsRow('all', 'anything', [])).toBe(true);
    // Before or after a write: either is enough.
    expect(showsRow(back!, 'note', [{ author: 'user:b' }, { author: 'user:a', folder: 'a' }])).toBe(true);
  });

  it('reads a filter it cannot evaluate as the whole table, and refuses what is not an interest', () => {
    for (const raw of [null, 'x', { t: 'op' }, { t: 'interest' }, { t: 'interest', v: [] }, { t: 'interest', v: null }]) expect(decodeInterest(raw)).toBeNull();
    const odd = decodeInterest({
      t: 'interest',
      v: {
        a: [[{ field: 'x', op: 'constructor', value: 1 }]],
        b: [[{ field: 1, op: '=', value: 1 }]],
        c: ['nope'],
        d: [[{ or: [] }]],
        e: Array.from({ length: 40 }, () => [eq('x', 1)]),
        f: [[null]],
        g: 'yes',
        h: [[eq('x', 1)]],
      },
    });
    expect(odd).toEqual({ a: true, b: true, c: true, d: true, e: true, f: true, h: [[eq('x', 1)]] });
  });
});

// ---- the manager, sharing with the user's own devices -----------------------------------

/** Tabs connect as soon as both want each other's user, own user included. */
function memoryNet() {
  const nodes = new Map<string, { self: PeerRef; hooks: TransportHooks; wanted: Set<string>; links: Set<string> }>();
  const sent: Array<{ from: string; to: string; message: Record<string, unknown> }> = [];
  const reconcile = () => {
    for (const a of nodes.values()) {
      for (const b of nodes.values()) {
        if (a === b) continue;
        const kb = peerKey(b.self);
        const mutual = a.wanted.has(b.self.user) && b.wanted.has(a.self.user);
        if (mutual && !a.links.has(kb)) {
          a.links.add(kb);
          a.hooks.onPeer(b.self, true);
        } else if (!mutual && a.links.has(kb)) {
          a.links.delete(kb);
          a.hooks.onPeer(b.self, false);
        }
      }
    }
  };
  const factory: ChannelTransportFactory = (hooks): ChannelTransport => {
    const self = hooks.self()!;
    const node = { self, hooks, wanted: new Set<string>(), links: new Set<string>() };
    nodes.set(peerKey(self), node);
    return {
      supported: true,
      want(users) {
        node.wanted = new Set(users);
        reconcile();
      },
      send(to, message) {
        if (!node.links.has(peerKey(to))) return;
        const copy = JSON.parse(JSON.stringify(message)) as Record<string, unknown>;
        sent.push({ from: peerKey(self), to: peerKey(to), message: copy });
        nodes.get(peerKey(to))?.hooks.onMessage(self, copy);
      },
      close() {
        node.wanted = new Set();
        reconcile();
        nodes.delete(peerKey(self));
      },
    };
  };
  return { factory, sent, ops: () => sent.filter((s) => s.message.t === 'op') };
}

const share: ChannelsConfig = { share: { tables: { note: true, tag: true, call: true } } };

/** One device: a manager signed in as `user`, its local rows, and a way to subscribe. */
function device(user: string, transport: ChannelTransportFactory, config: ChannelsConfig = share, held: Row[] = []) {
  const store = new Map<string, Row>(held.map((r) => [String(r.id), r]));
  const calls: string[] = [];
  const m = new ChannelManager(schema, {
    transport,
    config,
    rows: {
      create: async (_t, id, row) => {
        calls.push(`create ${id}`);
        if (store.has(id)) return false;
        store.set(id, { id, ...row });
        return true;
      },
      delete: async (_t, id) => {
        calls.push(`delete ${id}`);
        return store.delete(id);
      },
      read: async (id) => store.get(id) ?? null,
    },
  });
  m.setUser(user);
  /** Subscribe like a mounted query: its rows are what the store holds of its table. */
  const show = (plan?: QueryPlan) => {
    const seen: Row[][] = [];
    const tap = m.tap((rows) => seen.push(rows), plan ? m.interestOf(plan) : null);
    tap.feed([...store.values()].filter((r) => !plan || String(r.id).startsWith(`${plan.table}:`)));
    return { seen, last: () => seen[seen.length - 1], stop: tap.stop };
  };
  return { m, store, calls, show };
}

const note = (id: string, extra: Row = {}): Row => ({ id: `note:${id}`, folder: 'a', title: 't', ...extra });
const flush = async () => {
  for (let i = 0; i < 30; i++) await Promise.resolve();
};

describe('ChannelManager: sharing with the own devices', () => {
  it('sends an update of a shared table to the other device, which shows it at once', async () => {
    const net = memoryNet();
    const phone = device('user:a', net.factory, share, [note('1')]);
    const laptop = device('user:a', net.factory, share, [note('1')]);
    const view = laptop.show(inFolder('a'));
    await flush();
    expect(phone.m.sharing).toBe(true);
    expect(phone.m.enabled).toBe(true);
    expect(phone.m.ownState()).toEqual({ status: 'connected', peers: ['user:a'] });
    phone.m.local('note', 'note:1', { title: 'new' }, null);
    await flush();
    expect(view.last()[0].title).toBe('new');
    // Never in the store: the synced row confirms it, or it expires.
    expect(laptop.store.get('note:1')?.title).toBe('t');
  });

  it('carries creates and deletes too', async () => {
    const net = memoryNet();
    const phone = device('user:a', net.factory, share, [note('1')]);
    const laptop = device('user:a', net.factory, share, [note('1')]);
    laptop.show();
    await flush();
    phone.m.localCreate('note', 'note:2', { folder: 'a', title: 'two' });
    await phone.m.localDelete('note', 'note:1');
    await flush();
    expect(laptop.calls).toEqual(['create note:2', 'delete note:1']);
    expect(laptop.store.get('note:2')).toMatchObject({ folder: 'a', title: 'two' });
  });

  it('shares nothing of a table that was not named, or was turned off', async () => {
    const net = memoryNet();
    const config: ChannelsConfig = { share: { tables: { note: true, secret: false } } };
    const phone = device('user:a', net.factory, config, [{ id: 'secret:1', text: 'x' }, { id: 'tag:1', note: 'note:1', label: 'l' }]);
    const laptop = device('user:a', net.factory, config, [{ id: 'secret:1', text: 'x' }]);
    laptop.show();
    await flush();
    phone.m.local('secret', 'secret:1', { text: 'y' }, null);
    phone.m.local('tag', 'tag:1', { label: 'm' }, null);
    await flush();
    expect(net.ops()).toEqual([]);
    // And a device takes none either, whoever sends it.
    const peer = [...net.sent].find((s) => s.message.t === 'interest')!;
    laptop.m.receive({ user: 'user:a', peer: peer.from.split('#')[1] }, { t: 'op', table: 'secret', id: 'secret:1', patch: { text: 'z' }, hlc: [1, 0, 'x'] });
    await flush();
    expect(laptop.calls).toEqual([]);
  });

  it('warns about a table the schema does not have and shares the rest', () => {
    const warn = vi.fn();
    const m = new ChannelManager(schema, { config: { share: { tables: { nope: true, note: true } } }, logger: { warn, debug: () => {} } });
    expect(warn.mock.calls[0][1]).toMatch(/'nope'/);
    expect(m.sharing).toBe(true);
    expect(new ChannelManager(schema, { config: { share: { tables: { note: false } } } }).sharing).toBe(false);
    expect(new ChannelManager(schema).ownState()).toEqual({ status: 'idle', peers: [] });
  });

  it('applies a rule given as a function on the device that writes and on the one that receives', async () => {
    const net = memoryNet();
    const rule = (row: Row) => row.draft !== true;
    const config: ChannelsConfig = { share: { tables: { note: rule } } };
    const phone = device('user:a', net.factory, config, [note('1', { draft: true }), note('2'), note('5', { draft: true })]);
    const laptop = device('user:a', net.factory, config, [note('1', { draft: true }), note('2'), note('5', { draft: true })]);
    const view = laptop.show();
    await flush();
    phone.m.local('note', 'note:1', { title: 'still a draft' }, null);
    phone.m.local('note', 'note:2', { title: 'public' }, null);
    await flush();
    expect(net.ops().map((s) => s.message.id)).toEqual(['note:2']);
    expect(view.last().map((r) => r.title)).toEqual(['t', 'public', 't']);
    // Publishing a draft: the row after the write passes, so it goes out.
    phone.m.local('note', 'note:1', { draft: false }, null);
    await flush();
    expect(net.ops().map((s) => s.message.id)).toEqual(['note:2', 'note:1']);
    // A device whose own rule says no drops what another build of the app sent.
    laptop.m.receive({ user: 'user:a', peer: 'other' }, { t: 'op', table: 'note', id: 'note:5', patch: { title: 'forced' }, hlc: [9e15, 0, 'x'] });
    await flush();
    expect(view.last()[2].title).toBe('t');
    // A rule that throws shares nothing.
    const bad = device('user:a', net.factory, { share: { tables: { note: () => { throw new Error('boom'); } } } }, [note('3')]);
    bad.m.local('note', 'note:3', { title: 'x' }, null);
    await flush();
    expect(net.ops()).toHaveLength(2);
  });

  it('never shares with another user, and takes nothing shared from one', async () => {
    const net = memoryNet();
    const ann = device('user:a', net.factory, share, [note('1')]);
    const bob = device('user:b', net.factory, share, [note('1')]);
    const view = bob.show();
    await flush();
    ann.m.local('note', 'note:1', { title: 'mine' }, null);
    await flush();
    expect(net.sent).toEqual([]);
    bob.m.receive({ user: 'user:a', peer: 'x' }, { t: 'op', table: 'note', id: 'note:1', patch: { title: 'mine' }, hlc: [1, 0, 'x'] });
    bob.m.receive({ user: 'user:a', peer: 'x' }, { t: 'interest', all: true });
    await flush();
    expect(view.last()[0].title).toBe('t');
  });

  it('follows the signed-in user', async () => {
    const net = memoryNet();
    const phone = device('user:a', net.factory, share, [note('1')]);
    const laptop = device('user:a', net.factory, share, [note('1')]);
    const view = laptop.show();
    await flush();
    laptop.m.setUser('user:b');
    phone.m.local('note', 'note:1', { title: 'for a' }, null);
    await flush();
    expect(net.ops()).toEqual([]);
    expect(laptop.m.ownState().status).toBe('connecting');
    laptop.m.setUser(null);
    expect(laptop.m.ownState().status).toBe('idle');
    laptop.m.setUser('user:a');
    await flush();
    phone.m.local('note', 'note:1', { title: 'for a' }, null);
    await flush();
    expect(view.last()[0].title).toBe('for a');
  });

  it('reports how the own devices are doing to a subscriber', async () => {
    const net = memoryNet();
    const phone = device('user:a', net.factory);
    const states: string[] = [];
    const stop = phone.m.subscribeOwn((s) => states.push(s.status));
    const laptop = device('user:a', net.factory);
    await flush();
    laptop.m.close();
    await flush();
    expect(states).toEqual(['connecting', 'connected', 'connecting']);
    stop();
    expect(new ChannelManager(schema).subscribeOwn((s) => states.push(s.status))()).toBeUndefined();
    expect(states.at(-1)).toBe('idle');
  });

  it('also reaches the own devices when a write goes through a channel to another user', async () => {
    const net = memoryNet();
    const callScope = { table: 'call', where: [] };
    const phone = device('user:a', net.factory, share, [{ id: 'call:1', view_id: '' }]);
    const laptop = device('user:a', net.factory, share, [{ id: 'call:1', view_id: '' }]);
    const bob = device('user:b', net.factory, {}, [{ id: 'call:1', view_id: '' }]);
    const toBob = phone.m.open([callScope], ['user:b']);
    bob.m.open([callScope], ['user:a']);
    const mine = laptop.show();
    const theirs = bob.show();
    await flush();
    phone.m.local('call', 'call:1', { view_id: 'v' }, null, toBob);
    await flush();
    expect(mine.last()[0].view_id).toBe('v');
    expect(theirs.last()[0].view_id).toBe('v');
  });
});

describe('ChannelManager: sending only what the other device shows', () => {
  it('skips a device that shows nothing of the table, and one whose filter the row is outside of', async () => {
    const net = memoryNet();
    const phone = device('user:a', net.factory, share, [note('1'), note('2', { folder: 'b' }), { id: 'tag:1', note: 'note:1', label: 'l' }]);
    const laptop = device('user:a', net.factory, share, [note('1'), note('2', { folder: 'b' })]);
    await flush();
    // Nothing is mounted on the laptop: nothing is sent to it.
    phone.m.local('note', 'note:1', { title: 'x' }, null);
    await flush();
    expect(net.ops()).toEqual([]);

    const view = laptop.show(inFolder('a'));
    await flush();
    phone.m.local('note', 'note:1', { title: 'in a' }, null);
    phone.m.local('note', 'note:2', { title: 'in b' }, null);
    phone.m.local('tag', 'tag:1', { label: 'm' }, null);
    await flush();
    expect(net.ops().map((s) => s.message.id)).toEqual(['note:1']);
    expect(view.last()[0].title).toBe('in a');

    // A row moving into the folder it shows, or out of it, is sent.
    phone.m.local('note', 'note:2', { folder: 'a' }, null);
    phone.m.local('note', 'note:1', { folder: 'z' }, null);
    await flush();
    expect(net.ops().map((s) => s.message.id)).toEqual(['note:1', 'note:2', 'note:1']);
    // So are a create inside it and a delete of a row inside it; outside, neither.
    phone.m.localCreate('note', 'note:3', { folder: 'a', title: 'n' });
    phone.m.localCreate('note', 'note:4', { folder: 'q', title: 'n' });
    await flush();
    expect(net.ops().map((s) => s.message.id)).toEqual(['note:1', 'note:2', 'note:1', 'note:3']);

    // The query goes away: so does the claim.
    view.stop();
    await flush();
    phone.m.localCreate('note', 'note:5', { folder: 'a', title: 'n' });
    await flush();
    expect(net.ops()).toHaveLength(4);
  });

  it('sends everything of a table read through .related(), and everything to a subscription nobody described', async () => {
    const net = memoryNet();
    const phone = device('user:a', net.factory, share, [{ id: 'tag:1', note: 'note:1', label: 'l' }, note('9', { folder: 'zzz' })]);
    const laptop = device('user:a', net.factory, share, [{ id: 'tag:1', note: 'note:1', label: 'l' }, note('9', { folder: 'zzz' })]);
    const withTags = laptop.show({ ...inFolder('a'), relations: [{ alias: 'tags', table: 'tag', cardinality: 'many', foreignKeyField: 'note' }] });
    await flush();
    phone.m.local('tag', 'tag:1', { label: 'm' }, null);
    phone.m.local('note', 'note:9', { title: 'far' }, null);
    await flush();
    expect(net.ops().map((s) => s.message.id)).toEqual(['tag:1']);
    withTags.stop();
    laptop.show();
    await flush();
    phone.m.local('note', 'note:9', { title: 'far' }, null);
    await flush();
    expect(net.ops().map((s) => s.message.id)).toEqual(['tag:1', 'note:9']);
  });

  it('announces once per change, and tells a device that joins later', async () => {
    const net = memoryNet();
    const phone = device('user:a', net.factory, share, [note('1')]);
    const laptop = device('user:a', net.factory, share, [note('1')]);
    await flush();
    const interests = () => net.sent.filter((s) => s.message.t === 'interest');
    // Each told the other on meeting.
    expect(interests()).toHaveLength(2);
    const a = laptop.show(inFolder('a'));
    const b = laptop.show(inFolder('a'));
    laptop.show(inFolder('b'));
    await flush();
    expect(interests()).toHaveLength(3);
    expect(interests().at(-1)!.message.v).toEqual({ note: [[eq('folder', 'a')], [eq('folder', 'b')]] });
    // One of two queries on the folder goes: the claim is the same, and not said again.
    a.stop();
    a.stop();
    await flush();
    expect(interests()).toHaveLength(3);
    b.stop();
    await flush();
    expect(interests()).toHaveLength(4);
    expect(interests().at(-1)!.message.v).toEqual({ note: [[eq('folder', 'b')]] });
    laptop.show(inFolder('a'));
    await flush();
    expect(interests()).toHaveLength(5);
    const tablet = device('user:a', net.factory, share, [note('1')]);
    const view = tablet.show(inFolder('a'));
    await flush();
    phone.m.local('note', 'note:1', { title: 'all three' }, null);
    await flush();
    expect(view.last()[0].title).toBe('all three');
    expect(net.ops().map((s) => s.to).sort()).toEqual([...new Set(net.ops().map((s) => s.to))].sort());
    expect(net.ops()).toHaveLength(2);
  });

  it('forgets what a device showed once it is gone', async () => {
    const net = memoryNet();
    const phone = device('user:a', net.factory, share, [note('1')]);
    const laptop = device('user:a', net.factory, share, [note('1')]);
    laptop.show(inFolder('nothing'));
    await flush();
    phone.m.local('note', 'note:1', { title: 'x' }, null);
    await flush();
    expect(net.ops()).toEqual([]);
    laptop.m.close();
    const again = device('user:a', net.factory, share, [note('1')]);
    again.show();
    await flush();
    phone.m.local('note', 'note:1', { title: 'y' }, null);
    await flush();
    expect(net.ops()).toHaveLength(1);
    // Garbage in place of an interest changes nothing.
    phone.m.receive({ user: 'user:a', peer: 'x' }, { t: 'interest', v: 7 });
  });
});

describe('sharing, end to end: two browsers with two tabs each', () => {
  it('shows a write of one tab in the tabs of the other browser, over one link per browser', async () => {
    const room = memoryRoom();
    const left = fakeBrowser();
    const right = fakeBrowser();
    const viaHub = (b: ReturnType<typeof fakeBrowser>): ChannelTransportFactory => (hooks) =>
      hubTransport(hooks, { scope: (user) => `ns/db/${user}`, link: room.link, locks: b.locks, bus: b.bus, supported: true });
    const a1 = device('user:a', viaHub(left), share, [note('1')]);
    const a2 = device('user:a', viaHub(left), share, [note('1')]);
    const b1 = device('user:a', viaHub(right), share, [note('1')]);
    const b2 = device('user:a', viaHub(right), share, [note('1')]);
    const views = { a2: a2.show(inFolder('a')), b1: b1.show(inFolder('a')), b2: b2.show(inFolder('other')) };
    await settle();
    expect(room.members.size).toBe(2);
    const frames = room.frames.length;
    a1.m.local('note', 'note:1', { title: 'from a1' }, null);
    await settle();
    // The other browser's tab that shows the folder has it; the one that does
    // not was never sent it; the sibling tab is not a peer.
    expect(views.b1.last()[0].title).toBe('from a1');
    expect(views.b2.last()[0].title).toBe('t');
    expect(views.a2.last()[0].title).toBe('t');
    expect(room.frames.length - frames).toBe(1);
    expect(room.frames.at(-1)!.data).toMatchObject({ h: 'm', from: expect.any(String), to: [expect.any(String)] });
    // And back, from the tab that does not hold the link.
    b2.m.local('note', 'note:1', { title: 'from b2' }, null);
    await settle();
    expect(views.a2.last()[0].title).toBe('from b2');
    expect(views.b1.last()[0].title).toBe('from a1');
  });
});
