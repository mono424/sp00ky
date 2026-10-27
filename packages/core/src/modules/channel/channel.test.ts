import { afterEach, describe, expect, it, vi } from 'vitest';
import { RecordId } from 'surrealdb';
import type { SchemaStructure } from '@spooky-sync/query-builder';
import {
  ChannelManager,
  applyOverlay,
  checkPatch,
  compileScope,
  matchesWhere,
  unusedArgs,
  decodeValue,
  encodeValue,
  peerKey,
  peerUsers,
  splitPatch,
  type ChannelTransport,
  type ChannelTransportFactory,
  type OverlayEntry,
  type PeerRef,
  type Row,
  type TransportHooks,
} from './index';
import { webrtcTransport, type Signal, type SignalPort } from './webrtc';

const schema = {
  tables: [
    {
      name: 'call',
      columns: {
        id: { type: 'string', optional: false, recordId: true },
        caller: { type: 'string', optional: false, recordId: true },
        callee: { type: 'string', optional: false, recordId: true },
        view_id: { type: 'string', optional: true },
        view_pos: { type: 'string', optional: true, ephemeral: true },
        count: { type: 'number', optional: true },
      },
      channel: true,
      primaryKey: ['id'],
    },
    {
      name: 'msg',
      columns: {
        id: { type: 'string', optional: false, recordId: true },
        convo: { type: 'string', optional: false },
        text: { type: 'string', optional: false },
        mood: { type: 'string', optional: true, ephemeral: true },
      },
      channel: true,
      primaryKey: ['id'],
    },
    {
      name: 'note',
      columns: { id: { type: 'string', optional: false, recordId: true }, title: { type: 'string', optional: false } },
      primaryKey: ['id'],
    },
  ],
  relationships: [],
  backends: {},
} as unknown as SchemaStructure;

const entry = (value: unknown, ephemeral = true): OverlayEntry => ({ value, hlc: [1, 0, 'p'], from: 'x', ephemeral, expiresAt: null });

describe('pure helpers', () => {
  it('leaves rows untouched when the overlay is empty or misses them', () => {
    const rows: Row[] = [{ id: 'call:1', view_pos: null }];
    expect(applyOverlay(rows, new Map())).toBe(rows);
    expect(applyOverlay(rows, new Map([['call:2', new Map([['view_pos', entry('3')]])]]))).toBe(rows);
  });

  it('folds overlay values into matching rows, nested ones included', () => {
    const child = { id: new RecordId('call', 'c'), view_pos: null };
    const rows: Row[] = [{ id: new RecordId('note', 'n'), title: 't', calls: [child] }, { id: 'call:1', view_pos: null }];
    const out = applyOverlay(
      rows,
      new Map([
        ['call:c', new Map([['view_pos', entry('7')]])],
        ['call:1', new Map([['view_pos', entry('9')]])],
      ])
    );
    expect((out[0].calls as Row[])[0].view_pos).toBe('7');
    expect(out[1].view_pos).toBe('9');
    expect(child.view_pos).toBeNull();
  });

  it('round-trips record ids, dates and bytes', () => {
    const value = { at: new Date('2026-10-26T10:00:00Z'), who: new RecordId('user', 'a'), blob: new Uint8Array([1, 2, 3]), n: [1, 'x'] };
    const back = decodeValue(JSON.parse(JSON.stringify(encodeValue(value)))) as typeof value;
    expect(back.at).toBeInstanceOf(Date);
    expect(back.at.toISOString()).toBe('2026-10-26T10:00:00.000Z');
    expect(String(back.who.table.name)).toBe('user');
    expect(Array.from(back.blob)).toEqual([1, 2, 3]);
    expect(back.n).toEqual([1, 'x']);
  });

  it('checks patches against the columns', () => {
    const cols = schema.tables[0].columns;
    expect(checkPatch(cols, { view_pos: '3' })).toBeNull();
    expect(checkPatch(cols, { view_pos: null })).toBeNull();
    expect(checkPatch(cols, { nope: 1 })).toMatch(/unknown field/);
    expect(checkPatch(cols, { id: 'call:2' })).toMatch(/id/);
    expect(checkPatch(cols, { count: 'x' })).toMatch(/type/);
    expect(checkPatch(cols, {})).toMatch(/empty/);
  });

  it('splits ephemeral fields off a write', () => {
    const cols = schema.tables[0].columns;
    expect(splitPatch(cols, { view_id: 'a', view_pos: '1' })).toEqual({ persistent: { view_id: 'a' }, ephemeral: { view_pos: '1' } });
    expect(splitPatch(cols, { view_id: 'a' })).toEqual({ persistent: { view_id: 'a' }, ephemeral: null });
  });

  it('normalizes peers and skips self', () => {
    const users = peerUsers([new RecordId('user', 'a'), 'user:b', null, 'not-an-id'], 'user:a');
    expect([...users]).toEqual(['user:b']);
  });
});

// ---- managers over an in-memory transport --------------------------------------------

/** Tabs connect as soon as both want each other's user, like the real one. */
function memoryNet() {
  const nodes = new Map<string, { self: PeerRef; hooks: TransportHooks; wanted: Set<string>; links: Set<string> }>();
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
        nodes.get(peerKey(to))?.hooks.onMessage(self, JSON.parse(JSON.stringify(message)));
      },
      close() {
        node.wanted = new Set();
        reconcile();
        nodes.delete(peerKey(self));
      },
    };
  };
  return { factory, nodes };
}

const callRow = (extra: Row = {}): Row => ({
  id: new RecordId('call', '1'),
  caller: new RecordId('user', 'a'),
  callee: new RecordId('user', 'b'),
  view_id: '',
  view_pos: null,
  ...extra,
});

describe('channel filters', () => {
  const eq = (field: string, value: unknown) => ({ field, op: '=', value });

  it('matches equality across record id spellings, dates and nested paths', () => {
    expect(matchesWhere([eq('caller', new RecordId('user', 'a'))], { caller: 'user:a' })).toBe(true);
    expect(matchesWhere([eq('caller', new RecordId('user', 'a'))], { caller: new RecordId('user', 'b') })).toBe(false);
    expect(matchesWhere([eq('at', new Date(5))], { at: new Date(5) })).toBe(true);
    expect(matchesWhere([eq('meta.kind', 'x')], { meta: { kind: 'x' } })).toBe(true);
    expect(matchesWhere([eq('meta.kind', 'x')], { meta: null })).toBe(false);
    expect(matchesWhere([eq('tags', ['a'])], { tags: ['a'] })).toBe(true);
    expect(matchesWhere([eq('missing', null)], {})).toBe(true);
    expect(matchesWhere([], {})).toBe(true);
  });

  it('supports comparisons, membership, _or groups and swapped operands', () => {
    const row = { n: 5, s: 'b', tags: ['x', 'y'], who: 'user:a' };
    expect(matchesWhere([{ field: 'n', op: '>', value: 4 }, { field: 'n', op: '<=', value: 5 }], row)).toBe(true);
    expect(matchesWhere([{ field: 'n', op: '<', value: 5 }], row)).toBe(false);
    expect(matchesWhere([{ field: 'n', op: '>=', value: 'a' }], row)).toBe(false);
    expect(matchesWhere([{ field: 'n', op: '!=', value: 4 }], row)).toBe(true);
    expect(matchesWhere([{ field: 's', op: 'IN', value: ['a', 'b'] }], row)).toBe(true);
    expect(matchesWhere([{ field: 's', op: 'NOT IN', value: ['a', 'b'] }], row)).toBe(false);
    expect(matchesWhere([{ field: 'tags', op: 'CONTAINS', value: 'y' }], row)).toBe(true);
    expect(matchesWhere([{ field: 'tags', op: 'CONTAINSNOT', value: 'y' }], row)).toBe(false);
    expect(matchesWhere([{ field: 'tags', op: '∋', value: 'z', swap: false }], row)).toBe(false);
    expect(matchesWhere([{ field: 'who', op: '∈', value: [new RecordId('user', 'a')] }], row)).toBe(true);
    expect(matchesWhere([{ field: 's', op: '∈', value: ['b'], swap: true }], { s: ['b'] })).toBe(false);
    expect(matchesWhere([{ or: [eq('s', 'z'), eq('n', 5)] }], row)).toBe(true);
    expect(matchesWhere([{ or: [eq('s', 'z'), eq('n', 6)] }], row)).toBe(false);
  });

  it('names the parts of a query a channel ignores', () => {
    expect(unusedArgs({ table: 'msg', where: [] })).toEqual([]);
    expect(unusedArgs({ table: 'msg', orderBy: [['text', 'asc']], limit: 5, offset: 2, select: ['text'] })).toEqual(['orderBy', 'limit', 'offset', 'select']);
    expect(unusedArgs({ table: 'msg', limit: 1 }, true)).toEqual(['.one()']);
    expect(unusedArgs({ table: 'msg', orderBy: [], select: [] })).toEqual([]);
  });

  it('warns once per query shape about ignored parts, never with ignoreUnused', () => {
    const warn = vi.fn();
    const m = new ChannelManager(schema, { logger: { warn, debug: () => {} } });
    const plan = { table: 'msg', where: [], limit: 50, orderBy: [['text', 'asc'] as ['text', 'asc']] };
    m.scope(plan);
    m.scope(plan);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][1]).toMatch(/orderBy, limit.*\.ignoreUnusedArgs\(\)/);
    m.scope({ table: 'msg', limit: 1 }, { isOne: true });
    expect(warn).toHaveBeenCalledTimes(2);
    m.scope({ table: 'msg', offset: 3 }, { ignoreUnused: true });
    m.scope({ table: 'msg', where: [] });
    expect(warn).toHaveBeenCalledTimes(2);
    new ChannelManager(schema).scope(plan);
  });

  it('compiles a plan into a scope, refusing what the row alone cannot decide', () => {
    expect(compileScope(schema, { table: 'msg', where: [eq('convo', 'c1')] })).toEqual({ table: 'msg', where: [eq('convo', 'c1')] });
    expect(compileScope(schema, { table: 'call' })).toEqual({ table: 'call', where: [] });
    expect(() => compileScope(schema, { table: 'note' })).toThrow(/@channel/);
    expect(() => compileScope(schema, { table: 'msg', relations: [{} as never] })).toThrow(/related/);
    expect(() => compileScope(schema, { table: 'msg', where: [{ field: 'text', op: '@@', value: 'x' }] })).toThrow(/operator/);
    expect(() => compileScope(schema, { table: 'msg', where: [{ or: [{ field: 'convo', op: '=', value: undefined, paramRef: 'x' }] }] })).toThrow(/parameter/);
    expect(() => compileScope(schema, { table: 'call', where: [eq('caller.name', 'x')] })).toThrow(/record link/);
  });
});

/** The runtime's peer sagas, as a map: refuses an id it already holds. */
function fakeRows() {
  const store = new Map<string, Row>();
  const calls: string[] = [];
  return {
    store,
    calls,
    rows: {
      create: async (_t: string, id: string, row: Row, ttl: number) => {
        calls.push(`create ${id} ${ttl}`);
        if (store.has(id)) return false;
        store.set(id, { id, ...row });
        return true;
      },
      delete: async (_t: string, id: string, ttl: number) => {
        calls.push(`delete ${id} ${ttl}`);
        return store.delete(id);
      },
      read: async (id: string) => store.get(id) ?? null,
    },
  };
}

type Deps = ConstructorParameters<typeof ChannelManager>[1];
const allCalls = { table: 'call', where: [] };
const convo = (c: string) => ({ table: 'msg', where: [{ field: 'convo', op: '=', value: c }] });

/** One tab: a manager, an open channel and one subscription to watch. */
function client(
  user: string,
  net: ReturnType<typeof memoryNet>,
  deps: Deps = {},
  scopes: Array<{ table: string; where: any[] }> = [allCalls, convo('c1')],
  peers: unknown[] = ['user:a', 'user:b']
) {
  const fr = fakeRows();
  const m = new ChannelManager(schema, { transport: net.factory, rows: fr.rows, ...deps });
  m.setUser(user);
  const ch = m.open(scopes.map((sc) => m.scope(sc)), peers);
  const seen: Row[][] = [];
  const tap = m.tap((rows) => seen.push(rows));
  return { m, ch, tap, fr, seen, last: () => seen[seen.length - 1]?.[0] };
}

/** Let the reads and sends behind a write settle (fake timers too). */
const flush = async () => {
  for (let i = 0; i < 20; i++) await Promise.resolve();
};
const msg = (extra: Row = {}): Row => ({ id: 'msg:1', convo: 'c1', text: 'hi', ...extra });
const hlcNow = () => [Date.now(), 0, 'x'];

afterEach(() => {
  vi.useRealTimers();
});

describe('ChannelManager: opening', () => {
  it('connects the two parties and reports it', () => {
    const net = memoryNet();
    const a = client('user:a', net);
    const states: string[] = [];
    a.ch.subscribe((s) => states.push(s.status));
    expect(a.ch.state().status).toBe('connecting');
    const b = client('user:b', net);
    expect(a.ch.state()).toEqual({ status: 'connected', peers: ['user:b'] });
    expect(b.ch.state().peers).toEqual(['user:a']);
    expect(states).toEqual(['connecting', 'connected']);
    expect(a.ch.tables()).toEqual(['call', 'msg']);
  });

  it('needs at least one query', () => {
    const m = new ChannelManager(schema);
    expect(() => m.open([], [])).toThrow(/at least one query/);
    const ch = m.open([m.scope(allCalls)], []);
    expect(() => ch.setScopes([])).toThrow(/at least one query/);
    ch.setScopes([m.scope(convo('c2'))]);
    expect(ch.tables()).toEqual(['msg']);
  });

  it('is idle with nobody but yourself, and skips you once you sign in', () => {
    const net = memoryNet();
    const m = new ChannelManager(schema, { transport: net.factory });
    const ch = m.open([m.scope(allCalls)], ['user:a', 'user:b']);
    expect(ch.state().status).toBe('connecting');
    m.setUser('user:a');
    expect(net.nodes.size).toBe(1);
    ch.setPeers([new RecordId('user', 'a')]);
    expect(ch.state().status).toBe('idle');
  });

  it('setPeers and close change who it wants', () => {
    const net = memoryNet();
    const a = client('user:a', net);
    const b = client('user:b', net);
    a.ch.setPeers(['user:c']);
    expect(b.ch.state().status).toBe('connecting');
    a.ch.setPeers(['user:b']);
    expect(b.ch.state().status).toBe('connected');
    a.ch.close();
    a.ch.close();
    a.ch.setPeers(['user:b']);
    a.ch.setScopes([]);
    expect(b.ch.state().status).toBe('connecting');
    expect(() => a.m.assertCarries(a.ch, 'call')).toThrow(/closed/);
  });
});

describe('ChannelManager: updates', () => {
  it('sends ephemeral values to the peer and shows them locally, never as a write', async () => {
    const net = memoryNet();
    const a = client('user:a', net);
    const b = client('user:b', net);
    a.tap.feed([callRow()]);
    b.tap.feed([callRow()]);
    const { persistent, ephemeral } = a.m.split('call', { view_pos: '12' });
    expect(persistent).toEqual({});
    a.m.local('call', 'call:1', persistent, ephemeral);
    expect(a.last()?.view_pos).toBe('12');
    await flush();
    expect(b.last()?.view_pos).toBe('12');
  });

  it('shows a persistent peer value until the synced row catches up', async () => {
    const net = memoryNet();
    const a = client('user:a', net);
    const b = client('user:b', net);
    b.tap.feed([callRow()]);
    a.m.local('call', 'call:1', { view_id: 'game:x' }, null);
    await flush();
    expect(b.last()?.view_id).toBe('game:x');
    b.tap.feed([callRow({ view_id: 'game:x' })]);
    b.tap.feed([callRow({ view_id: 'game:y' })]);
    expect(b.last()?.view_id).toBe('game:y');
  });

  it('drops an unconfirmed persistent peer value after the timeout', async () => {
    vi.useFakeTimers();
    let now = 1000;
    const net = memoryNet();
    const a = client('user:a', net, { now: () => now });
    const b = client('user:b', net, { now: () => now, config: { confirmTimeoutMs: 5000 } });
    b.tap.feed([callRow()]);
    a.m.local('call', 'call:1', { view_id: 'forged' }, null);
    await flush();
    expect(b.last()?.view_id).toBe('forged');
    now += 6000;
    vi.advanceTimersByTime(1000);
    expect(b.last()?.view_id).toBe('');
  });

  it('ignores an unlisted user, a non-channel table, a row outside the filter and a row it does not hold', async () => {
    const net = memoryNet();
    const b = client('user:b', net);
    b.tap.feed([callRow(), msg({ id: 'msg:2', convo: 'c2' })]);
    const from = { user: 'user:a', peer: 'x' };
    b.m.receive({ user: 'user:c', peer: 'x' }, { t: 'op', table: 'call', id: 'call:1', patch: { view_pos: '1' }, hlc: hlcNow() });
    b.m.receive(from, { t: 'op', table: 'note', id: 'note:1', patch: { title: 'x' }, hlc: hlcNow() });
    b.m.receive(from, { t: 'op', table: 'msg', id: 'msg:2', patch: { text: 'x' }, hlc: hlcNow() });
    b.m.receive(from, { t: 'op', table: 'msg', id: 'msg:9', patch: { text: 'x' }, hlc: hlcNow() });
    b.m.receive(from, { t: 'op', table: 'call', id: 'note:1', patch: { view_pos: '1' }, hlc: hlcNow() });
    b.m.receive(from, { t: 'op', table: 'call', id: 'call:1', patch: { view_pos: 1 }, hlc: hlcNow() });
    await flush();
    expect(b.seen).toHaveLength(1);
  });

  it('takes a row moving out of the filter (it matched before the patch)', async () => {
    const net = memoryNet();
    const b = client('user:b', net);
    b.fr.store.set('msg:1', msg());
    b.tap.feed([msg()]);
    b.m.receive({ user: 'user:a', peer: 'x' }, { t: 'op', table: 'msg', id: 'msg:1', patch: { convo: 'c2' }, hlc: hlcNow() });
    await flush();
    expect(b.last()?.convo).toBe('c2');
  });

  it('applies the app accept rule', async () => {
    const net = memoryNet();
    const a = client('user:a', net);
    const b = client('user:b', net, { config: { accept: { call: ({ from, row }) => from === String(row?.callee) } } });
    b.tap.feed([callRow()]);
    a.m.local('call', 'call:1', {}, { view_pos: '5' });
    await flush();
    expect(b.last()?.view_pos).toBeNull();
  });

  it('keeps the newest write when two arrive out of order', async () => {
    const net = memoryNet();
    const b = client('user:b', net);
    b.tap.feed([callRow()]);
    const from = { user: 'user:a', peer: 'x' };
    b.m.receive(from, { t: 'op', table: 'call', id: 'call:1', patch: { view_pos: 'new' }, hlc: [2000, 0, 'x'] });
    await flush();
    b.m.receive(from, { t: 'op', table: 'call', id: 'call:1', patch: { view_pos: 'old' }, hlc: [1000, 0, 'x'] });
    await flush();
    expect(b.last()?.view_pos).toBe('new');
  });

  it('sends its ephemeral state to a peer that connects later, and forgets a peer that leaves', async () => {
    const net = memoryNet();
    const a = client('user:a', net);
    a.fr.store.set('call:1', callRow());
    a.m.local('call', 'call:1', {}, { view_pos: '3' });
    a.m.local('msg', 'msg:7', {}, { mood: 'x' });
    const b = client('user:b', net);
    b.tap.feed([callRow()]);
    await flush();
    expect(b.last()?.view_pos).toBe('3');
    a.m.close();
    expect(b.last()?.view_pos).toBeNull();
  });

  it('passes writes on non-channel tables through untouched', () => {
    const m = new ChannelManager(schema);
    expect(m.split('note', { title: 'x' })).toEqual({ persistent: { title: 'x' }, ephemeral: null });
    m.local('note', 'note:1', { title: 'x' }, null);
    m.localCreate('note', 'note:1', {});
  });

  it('an own persistent write supersedes a peer value for the same field', async () => {
    const net = memoryNet();
    const a = client('user:a', net);
    const b = client('user:b', net);
    b.tap.feed([callRow()]);
    a.m.local('call', 'call:1', { view_id: 'game:a' }, null);
    await flush();
    expect(b.last()?.view_id).toBe('game:a');
    b.m.local('call', 'call:1', { view_id: 'game:b' }, null);
    b.tap.feed([callRow({ view_id: 'game:b' })]);
    expect(b.last()?.view_id).toBe('game:b');
  });
});

describe('ChannelManager: routing by filter', () => {
  function threeWay() {
    const net = memoryNet();
    const a = client('user:a', net, {}, [convo('c1')], ['user:b']);
    const toC = a.m.open([a.m.scope(convo('c2'))], ['user:c']);
    const b = client('user:b', net, {}, [convo('c1')], ['user:a']);
    const c = client('user:c', net, {}, [convo('c2')], ['user:a']);
    return { a, b, c, toC };
  }

  it("sends a message only to the participants of its conversation's channel", async () => {
    const { a, b, c } = threeWay();
    a.m.localCreate('msg', 'msg:1', { convo: 'c1', text: 'hi', _00_rv: 1, junk: 2 });
    await flush();
    expect(b.fr.calls).toEqual(['create msg:1 5000']);
    expect(b.fr.store.get('msg:1')).toEqual({ id: 'msg:1', convo: 'c1', text: 'hi' });
    expect(c.fr.calls).toEqual([]);
  });

  it('routes updates and deletes by the stored row; a row changing conversation reaches both', async () => {
    const { a, b, c } = threeWay();
    a.fr.store.set('msg:1', msg());
    for (const t of [b, c]) {
      t.fr.store.set('msg:1', msg());
      t.tap.feed([msg()]);
    }
    a.m.local('msg', 'msg:1', { text: 'edited' }, null);
    await flush();
    expect(b.last()?.text).toBe('edited');
    expect(c.last()?.text).toBe('hi');
    await a.m.localDelete('msg', 'msg:1');
    await flush();
    expect(b.fr.calls).toEqual(['delete msg:1 5000']);
    expect(c.fr.calls).toEqual([]);
    // Moving a row to another conversation: both channels hear it.
    const moved = threeWay();
    moved.a.fr.store.set('msg:1', msg());
    for (const t of [moved.b, moved.c]) {
      t.fr.store.set('msg:1', msg());
      t.tap.feed([msg()]);
    }
    moved.a.m.local('msg', 'msg:1', { convo: 'c2' }, null);
    await flush();
    expect(moved.b.last()?.convo).toBe('c2');
    expect(moved.c.last()?.convo).toBe('c2');
  });

  it('a scoped write goes to that channel only, and only for rows its filter matches', async () => {
    const { a, b, c, toC } = threeWay();
    a.m.localCreate('msg', 'msg:1', { convo: 'c1', text: 'hi' }, toC);
    a.m.localCreate('msg', 'msg:2', { convo: 'c2', text: 'hi' }, toC);
    await flush();
    expect(b.fr.calls).toEqual([]);
    expect(c.fr.calls).toEqual(['create msg:2 5000']);
    expect(() => a.m.assertCarries(toC, 'call')).toThrow(/no query on 'call'/);
    expect(() => a.m.assertCarries(toC, 'msg')).not.toThrow();
  });

  it('an update of a row it does not hold is matched on the patch alone', async () => {
    const { a, b } = threeWay();
    b.fr.store.set('msg:5', msg({ id: 'msg:5' }));
    b.tap.feed([msg({ id: 'msg:5' })]);
    a.m.local('msg', 'msg:5', { convo: 'c1', text: 'x' }, null);
    await flush();
    expect(b.last()?.text).toBe('x');
    await a.m.localDelete('msg', 'msg:404');
  });
});

describe('ChannelManager: creates and deletes', () => {
  it('carries ephemeral fields of a create to the peer overlay, never into its store', async () => {
    const net = memoryNet();
    const a = client('user:a', net);
    const b = client('user:b', net);
    a.tap.feed([msg()]);
    a.m.localCreate('msg', 'msg:1', { convo: 'c1', text: 'hi', mood: 'happy' });
    expect(a.last()?.mood).toBe('happy');
    await flush();
    expect(b.fr.store.get('msg:1')).toEqual({ id: 'msg:1', convo: 'c1', text: 'hi' });
    b.tap.feed([msg()]);
    expect(b.last()?.mood).toBe('happy');
  });

  it('refuses a create from an unlisted user, outside the filter, against the accept rule, of the wrong shape or kind', async () => {
    const net = memoryNet();
    const b = client('user:b', net, { config: { accept: { msg: ({ kind, patch }) => kind !== 'create' || patch.text !== 'spam' } } });
    const send = (user: string, m: Row) => b.m.receive({ user, peer: 'x' }, { t: 'op', hlc: hlcNow(), ...m });
    send('user:c', { kind: 'create', table: 'msg', id: 'msg:1', patch: { convo: 'c1', text: 'hi' } });
    send('user:a', { kind: 'create', table: 'msg', id: 'msg:1', patch: { convo: 'c9', text: 'hi' } });
    send('user:a', { kind: 'create', table: 'msg', id: 'msg:1', patch: { convo: 'c1', text: 'spam' } });
    send('user:a', { kind: 'create', table: 'msg', id: 'msg:1', patch: { convo: 1 } });
    send('user:a', { kind: 'nuke', table: 'msg', id: 'msg:1', patch: {} });
    await flush();
    expect(b.fr.calls).toEqual([]);
    const noRows = new ChannelManager(schema);
    noRows.setUser('user:b');
    noRows.open([noRows.scope(convo('c1'))], ['user:a']);
    noRows.receive({ user: 'user:a', peer: 'x' }, { t: 'op', kind: 'create', table: 'msg', id: 'msg:1', patch: { convo: 'c1' }, hlc: hlcNow() });
    noRows.receive({ user: 'user:a', peer: 'x' }, { t: 'op', kind: 'delete', table: 'msg', id: 'msg:1', patch: {}, hlc: hlcNow() });
    await flush();
  });

  it('sends an own delete; the peer hides it', async () => {
    const net = memoryNet();
    const a = client('user:a', net);
    const b = client('user:b', net);
    a.fr.store.set('msg:1', msg());
    b.fr.store.set('msg:1', msg());
    await a.m.localDelete('msg', 'msg:1');
    await flush();
    expect(b.fr.calls).toEqual(['delete msg:1 5000']);
  });

  it('ignores a delete from an unlisted user, outside the filter, of a row it does not hold, or against the accept rule', async () => {
    const net = memoryNet();
    const b = client('user:b', net, { config: { accept: { call: () => false } } });
    b.fr.store.set('msg:2', msg({ id: 'msg:2', convo: 'c2' }));
    b.fr.store.set('call:1', callRow());
    const del = (user: string, table: string, id: string) =>
      b.m.receive({ user, peer: 'x' }, { t: 'op', kind: 'delete', table, id, patch: {}, hlc: hlcNow() });
    del('user:c', 'msg', 'msg:1');
    del('user:a', 'msg', 'msg:2');
    del('user:a', 'msg', 'msg:404');
    del('user:a', 'call', 'call:1');
    await flush();
    expect(b.fr.calls).toEqual([]);
  });
});

// ---- WebRTC transport against a fake RTCPeerConnection -------------------------------

class FakeChannel {
  readyState = 'connecting';
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((e: { data: unknown }) => void) | null = null;
  other: FakeChannel | null = null;
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

const offers = new Map<string, FakePC>();
let sdpSeq = 0;

class FakePC {
  connectionState = 'new';
  localDescription: { type: string; sdp: string } | null = null;
  onicecandidate: ((e: { candidate: { toJSON(): unknown } | null }) => void) | null = null;
  onconnectionstatechange: (() => void) | null = null;
  ondatachannel: ((e: { channel: FakeChannel }) => void) | null = null;
  dc: FakeChannel | null = null;
  remote: FakePC | null = null;
  createDataChannel() {
    this.dc = new FakeChannel();
    return this.dc;
  }
  async createOffer() {
    const sdp = `offer-${++sdpSeq}`;
    offers.set(sdp, this);
    return { type: 'offer', sdp };
  }
  async createAnswer() {
    return { type: 'answer', sdp: `answer-${++sdpSeq}` };
  }
  async setLocalDescription(d: { type: string; sdp: string }) {
    this.localDescription = d;
    queueMicrotask(() => this.onicecandidate?.({ candidate: { toJSON: () => ({ candidate: 'c' }) } }));
  }
  async setRemoteDescription(d: { type: string; sdp: string }) {
    if (d.type === 'offer') this.remote = offers.get(d.sdp) ?? null;
    if (d.type === 'answer') {
      // The offerer learns the answer: wire both data channels and open them.
      const answerer = [...answerers].find((p) => p.remote === this);
      if (!answerer || !this.dc) return;
      const theirs = new FakeChannel();
      this.dc.other = theirs;
      theirs.other = this.dc;
      answerer.ondatachannel?.({ channel: theirs });
      queueMicrotask(() => {
        this.dc?.open();
        theirs.open();
      });
    }
  }
  async addIceCandidate() {}
  close() {
    this.dc?.close();
  }
  constructor() {
    answerers.add(this);
  }
}
const answerers = new Set<FakePC>();

/** Signals between users, delivered like the LIVE would: to every tab of the recipient. */
function memorySignals() {
  const listeners = new Map<string, Set<(s: Signal) => void>>();
  const sent: Array<{ from: string; kind: string }> = [];
  const port = (user: string): SignalPort => ({
    async send(s) {
      sent.push({ from: user, kind: s.kind });
      const signal: Signal = { from: user, fromPeer: s.fromPeer, toPeer: s.toPeer, kind: s.kind, payload: s.payload };
      queueMicrotask(() => listeners.get(s.to)?.forEach((cb) => cb(signal)));
    },
    listen(self, cb) {
      const set = listeners.get(self.user) ?? new Set();
      set.add(cb);
      listeners.set(self.user, set);
      return () => set.delete(cb);
    },
  });
  return { port, sent };
}

const settle = async () => {
  for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));
};

function tab(user: string, peer: string, signals: ReturnType<typeof memorySignals>) {
  const events: string[] = [];
  const messages: unknown[] = [];
  const hooks: TransportHooks = {
    self: () => ({ user, peer }),
    onMessage: (_from, m) => messages.push(m),
    onPeer: (p, up) => events.push(`${up ? 'up' : 'down'} ${p.user}`),
    onStatus: () => {},
  };
  const t = webrtcTransport(hooks, { signal: signals.port(user), RTCPeerConnection: FakePC as unknown as typeof RTCPeerConnection });
  return { t, events, messages };
}

describe('webrtcTransport', () => {
  it('connects two tabs that want each other, whichever starts first', async () => {
    const signals = memorySignals();
    const a = tab('user:a', 'pa', signals);
    const b = tab('user:b', 'pb', signals);
    a.t.want(new Set(['user:b']));
    await settle();
    expect(a.events).toEqual([]);
    b.t.want(new Set(['user:a']));
    await settle();
    expect(a.events).toEqual(['up user:b']);
    expect(b.events).toEqual(['up user:a']);
    a.t.send({ user: 'user:b', peer: 'pb' }, { t: 'op', table: 'call', id: 'call:1', patch: { view_pos: '1' }, hlc: [1, 0, 'pa'] });
    await settle();
    expect(b.messages).toEqual([{ t: 'op', table: 'call', id: 'call:1', patch: { view_pos: '1' }, hlc: [1, 0, 'pa'] }]);
  });

  it('never connects to a user who does not want it back', async () => {
    const signals = memorySignals();
    const a = tab('user:a', 'pa', signals);
    const b = tab('user:b', 'pb', signals);
    a.t.want(new Set(['user:b']));
    b.t.want(new Set(['user:c']));
    await settle();
    expect(a.events).toEqual([]);
    expect(b.events).toEqual([]);
    expect(signals.sent.filter((s) => s.kind === 'offer')).toHaveLength(0);
  });

  it('closes the link when the user is no longer wanted', async () => {
    const signals = memorySignals();
    const a = tab('user:a', 'pa', signals);
    const b = tab('user:b', 'pb', signals);
    a.t.want(new Set(['user:b']));
    b.t.want(new Set(['user:a']));
    await settle();
    a.t.want(new Set());
    await settle();
    expect(a.events).toEqual(['up user:b', 'down user:b']);
    expect(b.events).toEqual(['up user:a', 'down user:a']);
    a.t.close();
    b.t.close();
  });
});
