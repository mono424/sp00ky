import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as loro from 'loro-crdt';
import { LoroDoc } from 'loro-crdt';
import { check } from './acceptance';
import { asBytes } from './bytes';
import { canonicalState, stateHash } from './hash';
import { CrdtManager, type CrdtPorts } from './index';
import { renderText } from './mirror';

const silent = { child: () => silent, debug: () => {}, info: () => {}, warn: () => {}, error: () => {} } as any;

const schema = {
  tables: [
    { name: 'doc', columns: { body: { type: 'string', crdt: 'text' }, title: { type: 'string' } } },
    { name: 'doc__crdt_log', columns: { doc: {}, field: {}, peer: {}, data: {} } },
  ],
} as any;

type Row = { id: string; doc: string; field: string; peer: string; data: Uint8Array };

/** One fake backend shared by every client: the edit log, the peer claims
 *  and the published documents, plus what each client's local store holds. */
class World {
  log: Row[] = [];
  peers = new Map<string, string>();
  published = new Map<string, { rev: number; snapshot: Uint8Array; upto?: unknown; state_hash?: string }>();
  online = true;
  /** Offline remote calls never answer (queued behind a reconnect) instead of failing. */
  hang = false;
  /** A slow device store: each local write takes this long (real timers). */
  storeDelayMs = 0;
  /** A connected but slow server: reads and the log's first answer wait for `release()`. */
  slow = false;
  private held: Array<() => void> = [];
  release() {
    for (const go of this.held.splice(0)) go();
  }
  private watchers = new Set<{ table: string; where: Record<string, unknown>; cb: (rows: any[]) => void }>();
  /** Every query a client registered, in order. */
  watched: string[] = [];

  client(user: string, store = new Map<string, any>(), logger: any = silent) {
    const local = {
      getById: async (_t: string, id: any) => store.get(String(id)) ?? null,
      upsert: async (_t: string, id: any, data: any) => {
        const copy = structuredClone(data);
        if (this.storeDelayMs) await new Promise((resolve) => setTimeout(resolve, this.storeDelayMs));
        store.set(String(id), copy);
      },
    } as any;
    const remote = {
      subscribeConnection: () => () => {},
      getStatus: () => (this.online ? 'connected' : 'disconnected'),
      query: async (sql: string, vars: any) => {
        if (this.slow) await new Promise<void>((go) => this.held.push(go));
        if (!this.online && this.hang) return new Promise(() => {});
        if (!this.online) throw new Error('not connected');
        if (sql.startsWith('CREATE')) {
          const owner = this.peers.get(vars.peer);
          if (owner) throw new Error(`Database record \`_00_crdt_peer:${vars.peer}\` already exists`);
          this.peers.set(vars.peer, user);
          return [[{ id: `_00_crdt_peer:${vars.peer}` }]];
        }
        if (sql.startsWith('SELECT VALUE author')) return [this.peers.get(vars.peer) === user ? user : null];
        if (sql.includes('_00_crdt_doc')) return [this.published.get(`${vars.doc}|${vars.field}`) ?? null];
        return [[]];
      },
    } as any;
    const manager = new CrdtManager(schema, local, remote, logger);
    const ports: CrdtPorts = {
      watch: async (table, where, cb) => {
        this.watched.push(table);
        const w = { table, where, cb };
        this.watchers.add(w);
        if (this.slow) {
          const settled = new Promise<void>((go) =>
            this.held.push(() => {
              cb(this.rowsFor(table, where));
              go();
            })
          );
          return { stop: () => void this.watchers.delete(w), settled };
        }
        cb(this.rowsFor(table, where));
        return { stop: () => void this.watchers.delete(w), settled: Promise.resolve() };
      },
      create: async (id, data: any) => {
        const owner = this.peers.get(data.peer);
        if (owner !== user) throw new Error('peer not owned');
        this.log.push({ id, doc: data.doc, field: data.field, peer: data.peer, data: data.data });
        queueMicrotask(() => this.notify());
        return data;
      },
    };
    manager.attach(ports);
    return { manager, store };
  }

  rowsFor(table: string, where: Record<string, unknown>) {
    if (!table.endsWith('__crdt_log')) return [];
    return this.log.filter((r) => r.doc === where.doc && r.field === where.field);
  }

  notify() {
    for (const w of this.watchers) w.cb(this.rowsFor(w.table, w.where));
  }

  /** Fold like the SSP: publish `server` as the document and drop the log. */
  async fold(doc: string, field: string, server: LoroDoc, opts: { rev?: number; hash?: string } = {}) {
    this.published.set(`${doc}|${field}`, {
      rev: opts.rev ?? 1,
      snapshot: server.export({ mode: 'snapshot' }),
      upto: { vv: Object.fromEntries(server.oplogVersion().toJSON()), frontiers: server.oplogFrontiers() },
      state_hash: opts.hash ?? (await stateHash(loro, server)),
    });
    this.log = this.log.filter((r) => !(r.doc === doc && r.field === field));
    this.notify();
  }
}

const text = (field: { getDoc(): LoroDoc }) => field.getDoc().getText('body').toString();

/** Let real async work (the WebCrypto digest) finish while timers are fake. */
async function realWait(ms = 50) {
  vi.useRealTimers();
  await new Promise((resolve) => setTimeout(resolve, ms));
  vi.useFakeTimers();
}

describe('CRDT fields', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('send only their own ops as log rows, and concurrent edits converge', async () => {
    const world = new World();
    const a = world.client('user:a');
    const b = world.client('user:b');
    const fa = await a.manager.open('doc', 'doc:1', 'body');
    const fb = await b.manager.open('doc', 'doc:1', 'body');

    fa.getDoc().getText('body').insert(0, 'Hello');
    fa.getDoc().commit();
    fb.getDoc().getText('body').insert(0, 'World');
    fb.getDoc().commit();
    await vi.advanceTimersByTimeAsync(400);

    expect(world.log).toHaveLength(2);
    for (const row of world.log) {
      expect(check(loro, row.data, row.peer).ok).toBe(true);
    }
    expect(text(fa)).toBe(text(fb));
    expect(text(fa)).toHaveLength(10);

    // A's next edit carries only A's new op, not B's imported ones.
    fa.getDoc().getText('body').insert(0, '>');
    fa.getDoc().commit();
    await vi.advanceTimersByTimeAsync(400);
    const last = world.log.at(-1)!;
    const verdict = check(loro, last.data, last.peer);
    expect(verdict.ok && verdict.accepted.peer).toBe(fa.getDoc().peerIdStr);
    expect(text(fb)).toBe(text(fa));
  });

  it('while typing continues, edits go out on a steady cadence instead of waiting for a pause', async () => {
    const world = new World();
    const a = world.client('user:a');
    const f = await a.manager.open('doc', 'doc:1', 'body');
    // A key every 50 ms for a second: a trailing debounce would send nothing
    // until the typing stopped. The first edit goes out after 50 ms, the rest
    // every 200 ms.
    for (let i = 0; i < 20; i++) {
      f.getDoc().getText('body').insert(i, 'x');
      f.getDoc().commit();
      await vi.advanceTimersByTimeAsync(50);
      if (i === 0) expect(world.log).toHaveLength(1);
    }
    expect(world.log.length).toBeGreaterThanOrEqual(4);
    expect(world.log.length).toBeLessThanOrEqual(6);
    await vi.advanceTimersByTimeAsync(300);
    const copy = new LoroDoc();
    for (const row of world.log) copy.import(row.data);
    expect(copy.getText('body').toString()).toBe('x'.repeat(20));
  });

  it('edits made while a send waits on the server go out right after it', async () => {
    const world = new World();
    const a = world.client('user:a');
    const f = await a.manager.open('doc', 'doc:1', 'body');
    world.slow = true;
    // A fresh peer: the next send claims it first, and that claim hangs.
    a.manager.setSessionId('');
    f.getDoc().getText('body').insert(0, 'a');
    f.getDoc().commit();
    await vi.advanceTimersByTimeAsync(100);
    f.getDoc().getText('body').insert(1, 'b');
    f.getDoc().commit();
    await vi.advanceTimersByTimeAsync(300);
    world.slow = false;
    world.release();
    await vi.advanceTimersByTimeAsync(10);
    const copy = new LoroDoc();
    for (const row of world.log) copy.import(row.data);
    expect(copy.getText('body').toString()).toBe('ab');
  });

  it('a first open reads the published document while its log query registers, not before', async () => {
    const world = new World();
    world.slow = true;
    const a = world.client('user:a');
    const opening = a.manager.open('doc', 'doc:1', 'body');
    await vi.advanceTimersByTimeAsync(0);
    // The published-document read is still out, and the log query is already registered.
    expect(world.watched).toEqual(['doc__crdt_log']);
    world.slow = false;
    world.release();
    await vi.advanceTimersByTimeAsync(10);
    expect((await opening).isLoaded()).toBe(true);
  });

  it('send nothing until the peer is claimed, then everything at once', async () => {
    const world = new World();
    world.online = false;
    const a = world.client('user:a');
    const b = world.client('user:b');
    const fa = await a.manager.open('doc', 'doc:1', 'body');
    const fb = await b.manager.open('doc', 'doc:1', 'body');
    fa.getDoc().getText('body').insert(0, 'offline edit');
    fa.getDoc().commit();
    await vi.advanceTimersByTimeAsync(1000);
    expect(world.log).toHaveLength(0);

    world.online = true;
    await vi.advanceTimersByTimeAsync(6000);
    expect(world.log).toHaveLength(1);
    expect(text(fb)).toBe('offline edit');
  });

  it("send a closed session's unsent edits under its own peer on the next open", async () => {
    const world = new World();
    world.online = false;
    const store = new Map<string, any>();
    const first = world.client('user:a', store);
    const f1 = await first.manager.open('doc', 'doc:1', 'body');
    const oldPeer = f1.getDoc().peerIdStr;
    f1.getDoc().getText('body').insert(0, 'kept');
    f1.getDoc().commit();
    first.manager.closeAll({ flush: true });
    await vi.advanceTimersByTimeAsync(1000);
    expect(world.log).toHaveLength(0);

    world.online = true;
    const second = world.client('user:a', store); // a reload: same device store, new peer
    const f2 = await second.manager.open('doc', 'doc:1', 'body');
    expect(text(f2)).toBe('kept');
    expect(f2.getDoc().peerIdStr).not.toBe(oldPeer);
    await vi.advanceTimersByTimeAsync(400);
    expect(world.log.map((r) => r.peer)).toEqual([oldPeer]);
    expect(world.peers.get(oldPeer)).toBe('user:a');
  });

  it('open, adopt a draft and close offline without waiting on the server', async () => {
    const world = new World();
    world.online = false;
    world.hang = true;
    const store = new Map<string, any>();
    const a = world.client('user:a', store);
    const draft = await a.manager.createDraft('doc', 'body');
    draft.getDoc().getText('body').insert(0, 'written offline');
    draft.getDoc().commit();

    let done = false;
    void (async () => {
      const f = await a.manager.open('doc', 'doc:1', 'body', draft);
      expect(text(f)).toBe('written offline');
      await a.manager.close('doc', 'doc:1', 'body');
      done = true;
    })();
    await realWait();
    expect(done).toBe(true);
    expect(world.log).toHaveLength(0);
    // Kept on the device, to go out from the next open.
    expect([...store.values()].some((row) => row.snapshot)).toBe(true);
  });

  it('a field is loaded once the server answers, and at once with a local copy or a draft', async () => {
    const world = new World();
    const server = new LoroDoc();
    server.getText('body').insert(0, 'from the server');
    server.commit();
    await world.fold('doc:1', 'body', server);
    world.slow = true;
    const store = new Map<string, any>();
    const a = world.client('user:a', store);
    const opening = a.manager.open('doc', 'doc:1', 'body');
    await vi.advanceTimersByTimeAsync(9000);
    const f = await opening;
    // Handed out after the bounded wait, but empty only because nothing came.
    expect(text(f)).toBe('');
    expect(f.isLoaded()).toBe(false);
    let loaded = 0;
    f.onLoaded(() => loaded++);
    world.release();
    await realWait();
    expect(text(f)).toBe('from the server');
    expect(f.isLoaded()).toBe(true);
    expect(loaded).toBe(1);
    await a.manager.close('doc', 'doc:1', 'body');

    // This device has it now: loaded before the server says anything.
    const again = await (async () => {
      const opening2 = a.manager.open('doc', 'doc:1', 'body');
      await vi.advanceTimersByTimeAsync(9000);
      return opening2;
    })();
    expect(again.isLoaded()).toBe(true);
    world.release();

    const draft = await a.manager.createDraft('doc', 'body');
    draft.getDoc().getText('body').insert(0, 'new');
    draft.getDoc().commit();
    const fresh = await a.manager.open('doc', 'doc:2', 'body', draft);
    expect(fresh.isLoaded()).toBe(true);
  });

  it('a reopen right after a close starts from what the close saved', async () => {
    vi.useRealTimers();
    const world = new World();
    world.storeDelayMs = 30;
    const a = world.client('user:a');
    const draft = await a.manager.createDraft('doc', 'body');
    draft.getDoc().getText('body').insert(0, 'from the form');
    draft.getDoc().commit();
    await a.manager.open('doc', 'doc:1', 'body', draft);
    void a.manager.close('doc', 'doc:1', 'body');
    const reopened = await a.manager.open('doc', 'doc:1', 'body');
    expect(text(reopened)).toBe('from the form');
  });

  it('never import a row that fails the acceptance rule', async () => {
    const world = new World();
    const b = world.client('user:b');
    const forged = new LoroDoc();
    forged.setPeerId(42);
    forged.getText('body').insert(0, 'forged');
    forged.commit();
    world.log.push({ id: 'doc__crdt_log:x', doc: 'doc:1', field: 'body', peer: '7', data: forged.export({ mode: 'update' }) });
    world.log.push({ id: 'doc__crdt_log:y', doc: 'doc:1', field: 'body', peer: '42', data: forged.export({ mode: 'snapshot' }) });
    const fb = await b.manager.open('doc', 'doc:1', 'body');
    expect(text(fb)).toBe('');
  });

  it('a first open on a device starts from the published document', async () => {
    const world = new World();
    const server = new LoroDoc();
    server.getText('body').insert(0, 'published');
    server.commit();
    world.published.set('doc:1|body', { rev: 3, snapshot: server.export({ mode: 'snapshot' }) });
    const a = world.client('user:a');
    const f = await a.manager.open('doc', 'doc:1', 'body');
    expect(f.hasContent()).toBe(true);
    expect(text(f)).toBe('published');
  });

  it('share one document between opens and stop with the last close', async () => {
    const world = new World();
    const a = world.client('user:a');
    const [x, y] = await Promise.all([a.manager.open('doc', 'doc:1', 'body'), a.manager.open('doc', 'doc:1', 'body')]);
    expect(x).toBe(y);
    a.manager.close('doc', 'doc:1', 'body');
    x.getDoc().getText('body').insert(0, 'still open');
    x.getDoc().commit();
    await vi.advanceTimersByTimeAsync(400);
    expect(world.log).toHaveLength(1);
    a.manager.close('doc', 'doc:1', 'body');
    await vi.advanceTimersByTimeAsync(10);
    const reopened = await a.manager.open('doc', 'doc:1', 'body');
    expect(reopened).not.toBe(x);
  });

  it('adopt a draft made before the record existed and send it under the draft peer', async () => {
    const world = new World();
    const a = world.client('user:a');
    const b = world.client('user:b');
    const draft = await a.manager.createDraft('doc', 'body');
    draft.getDoc().getText('body').insert(0, 'first words');
    draft.getDoc().commit();
    await vi.advanceTimersByTimeAsync(1000);
    expect(world.log).toHaveLength(0);

    const f = await a.manager.open('doc', 'doc:1', 'body', draft);
    expect(text(f)).toBe('first words');
    await vi.advanceTimersByTimeAsync(10);
    expect(world.log.map((r) => r.peer)).toEqual([draft.getDoc().peerIdStr]);
    const fb = await b.manager.open('doc', 'doc:1', 'body');
    expect(text(fb)).toBe('first words');
  });

  it('a device that missed folded rows catches up from the published document', async () => {
    const world = new World();
    const a = world.client('user:a');
    const b = world.client('user:b');
    const fa = await a.manager.open('doc', 'doc:1', 'body');
    fa.getDoc().getText('body').insert(0, 'Hello');
    fa.getDoc().commit();
    await vi.advanceTimersByTimeAsync(400);
    const fb = await b.manager.open('doc', 'doc:1', 'body');
    expect(text(fb)).toBe('Hello');

    // The server folded in an edit whose row B never received.
    const server = LoroDoc.fromSnapshot(fa.getDoc().export({ mode: 'snapshot' }));
    const other = new LoroDoc();
    other.setPeerId(99);
    other.import(server.export({ mode: 'update' }));
    other.getText('body').insert(5, ' world');
    other.commit();
    server.import(other.export({ mode: 'update' }));
    await world.fold('doc:1', 'body', server);
    await vi.advanceTimersByTimeAsync(1100);
    expect(text(fb)).toBe('Hello world');
  });

  it('report a document that differs from the published one at the same version', async () => {
    const world = new World();
    const warn = vi.fn();
    const logger = { ...silent, warn, child: () => logger };
    const a = world.client('user:a', new Map(), logger);
    const fa = await a.manager.open('doc', 'doc:1', 'body');
    fa.getDoc().getText('body').insert(0, 'same ops');
    fa.getDoc().commit();
    await vi.advanceTimersByTimeAsync(400);
    const server = LoroDoc.fromSnapshot(fa.getDoc().export({ mode: 'snapshot' }));

    await world.fold('doc:1', 'body', server, { hash: await stateHash(loro, server) });
    await vi.advanceTimersByTimeAsync(1100);
    await realWait();
    expect(warn).not.toHaveBeenCalled();

    fa.getDoc().getText('body').insert(0, '>');
    fa.getDoc().commit();
    await vi.advanceTimersByTimeAsync(400);
    server.import(fa.getDoc().export({ mode: 'update' }));
    await world.fold('doc:1', 'body', server, { rev: 2, hash: 'f'.repeat(64) });
    await vi.advanceTimersByTimeAsync(1100);
    await realWait();
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it('refuse to open a field that is not @crdt', async () => {
    const world = new World();
    const a = world.client('user:a');
    await expect(a.manager.open('doc', 'doc:1', 'title')).rejects.toThrow(/not a CRDT field/);
  });
});

describe('acceptance rule', () => {
  it('refuses blobs that make the decoder trap, without throwing', async () => {
    const fixtures = await import('../../../../ssp-node/tests/fixtures/crdt_hostile_blobs.json');
    const hostile = (fixtures.default ?? fixtures) as Array<{ peer: string; blob: string }>;
    for (const h of hostile) {
      const bytes = Uint8Array.from(Buffer.from(h.blob, 'base64'));
      expect(check(loro, bytes, h.peer).ok).toBe(false);
    }
  });

  it('normalizes every byte shape a store can hand back', () => {
    const b = new Uint8Array([1, 2, 3]);
    expect(asBytes(b)).toBe(b);
    expect(Array.from(asBytes(b.buffer)!)).toEqual([1, 2, 3]);
    expect(Array.from(asBytes([1, 2, 3])!)).toEqual([1, 2, 3]);
    expect(asBytes('nope')).toBeUndefined();
  });
});

describe('state hash', () => {
  it('matches the server fold on the shared vectors', async () => {
    const fixtures = await import('../../../../ssp-node/tests/fixtures/crdt_state_hash.json');
    const vectors = (fixtures.default ?? fixtures) as Array<{ name: string; snapshot: string; canonical: string; hash: string }>;
    expect(vectors.length).toBeGreaterThan(0);
    for (const v of vectors) {
      const doc = LoroDoc.fromSnapshot(Uint8Array.from(Buffer.from(v.snapshot, 'base64')));
      expect(canonicalState(loro, doc), v.name).toBe(v.canonical);
      expect(await stateHash(loro, doc), v.name).toBe(v.hash);
    }
  });
});

describe('text mirror', () => {
  it('renders text and prosemirror documents like the server fold', () => {
    const doc = new LoroDoc();
    doc.getText('summary').insert(0, 'héllo 🌍');
    expect(renderText(doc, 'summary', 'text')).toBe('héllo 🌍');

    const pm = new LoroDoc();
    const root = pm.getMap('doc');
    root.set('nodeName', 'doc');
    const blocks = root.setContainer('children', new loro.LoroList());
    const para = (parent: InstanceType<typeof loro.LoroList>, name: string) => {
      const m = parent.insertContainer(parent.length, new loro.LoroMap());
      m.set('nodeName', name);
      return m.setContainer('children', new loro.LoroList());
    };
    const run = (parent: InstanceType<typeof loro.LoroList>, s: string) =>
      parent.insertContainer(parent.length, new loro.LoroText()).insert(0, s);
    run(para(blocks, 'heading'), 'Title');
    const p = para(blocks, 'paragraph');
    run(p, 'one ');
    run(p, 'bold');
    p.insertContainer(p.length, new loro.LoroMap()).set('nodeName', 'hard_break');
    run(p, 'two');
    para(blocks, 'paragraph');
    pm.commit();
    // Same fixture and expectation as `mirror.rs`.
    expect(renderText(pm, 'body', 'prosemirror')).toBe('Title\none bold\ntwo\n');
  });
});
