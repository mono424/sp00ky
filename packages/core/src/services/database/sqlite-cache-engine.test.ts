import { describe, it, expect } from 'vitest';
import { RecordId } from 'surrealdb';
import { pureWriteOpResult, SqliteCacheEngine } from './sqlite-cache-engine';
import { stubTransport } from './sqlite-transport.fixture';
import { BrokerPortClosedError } from './sqlite-transport';
import { translateSurql } from './surql-translate';
import type { SqlOp } from './surql-translate';
import { surql } from '../../utils/surql';

function makeLogger(): any {
  const noop = () => {};
  const l: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
  l.child = () => l;
  return l;
}

/**
 * A fake transport handler that models the real SQLite worker's open/closed
 * lifecycle: `open` opens the DB, `close` closes it, and `exec`/`run` REJECT
 * with "sqlite: DB not open" when the DB isn't currently open — exactly the
 * failure the fix targets. Records every message `type` (across transport
 * generations) so tests can also inspect dispatch order.
 */
function lifecycleHandler(log: string[]) {
  let dbOpen = false;
  return (type: string) => {
    log.push(type);
    if (type === 'open') {
      dbOpen = true;
      return { persisted: true };
    }
    if (type === 'close') {
      dbOpen = false;
      return {};
    }
    if (type === 'exec' || type === 'select' || type === 'run' || type === 'batch') {
      if (!dbOpen) throw new Error('sqlite: DB not open');
      if (type === 'exec') return { rows: [] };
      if (type === 'select') return { rows: [], relationFetches: 0 };
    }
    return {};
  };
}

// Regression: switchBucket must run close → reopen as a single serialized
// opQueue entry. Otherwise a read/write enqueued during an auth/bucket change
// dispatches to the just-closed worker → "sqlite: DB not open" (the crash on
// sign-in). The invariant: no `exec` may appear between a `close` and the next
// `open` in the message stream.
describe('SqliteCacheEngine.switchBucket serialization', () => {
  it('never dispatches an op to a closed DB during a bucket switch', async () => {
    const log: string[] = [];
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger());
    stubTransport(engine, lifecycleHandler(log));

    await engine.connect('anon');
    expect(log).toContain('open');

    // Fire a switch and a concurrent read (exactly what the sign-in query
    // re-registration does). With the old non-atomic switch the read's `exec`
    // dispatched to the closed/terminated worker and REJECTED ("sqlite: DB not
    // open" / "not connected") — the uncaught crash. The atomic switch makes the
    // read wait for the reopen and resolve against the new bucket.
    const switching = engine.switchBucket('user:abc');
    const reading = engine.getById('_00_query', 'h1');
    await expect(Promise.all([switching, reading])).resolves.toBeDefined();
    await expect(reading).resolves.toBeNull(); // missing row → null, not a throw

    // And the close→reopen ran with no op wedged between them.
    const closeIdx = log.indexOf('close');
    const openAfter = log.indexOf('open', closeIdx + 1);
    expect(openAfter).toBeGreaterThan(closeIdx);
    expect(log.slice(closeIdx + 1, openAfter)).not.toContain('exec');
    expect(engine.currentBucketId).toBe('user:abc');
  });
});

// Regression: DEFINE is a noop on this engine, so the `_00_*` internal tables
// the migrator DEFINEs are never physically created — a fresh bucket that READS
// one before any write (the sync layer selects `_00_query` at startup) threw
// "no such table: _00_query" and wedged the client on "Loading database". The
// fix seeds them inside `open`; assert the open message carries them.
describe('SqliteCacheEngine system-table seeding', () => {
  it('passes the _00_* system tables to the worker open (fresh-bucket safe)', async () => {
    const msgs: any[] = [];
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger());
    stubTransport(engine, (type, payload) => {
      msgs.push({ type, payload });
      return type === 'open' ? { persisted: true } : {};
    });

    await engine.connect('user:fresh');
    const open = msgs.find((m) => m.type === 'open');
    expect(open?.payload?.systemTables).toContain('_00_view');
    expect(open?.payload?.systemTables).toContain('_00_failed_mutations');
    expect(open?.payload?.systemTables).toContain('_00_pending_mutations');
  });
});

// The worker's `persisted`/`opfsError` reply used to die in a `logger.info`
// line, so a host app running pino at `fatal` (whitepawn does) could not tell a
// disk-backed store from a full-RAM one. It now lands on the engine as
// observable state the app can render.
describe('SqliteCacheEngine storage health', () => {
  /** Engine wired to a worker whose `open` replies with `openReply`. */
  function makeEngine(openReply: Record<string, unknown>, opts?: { useOpfs?: boolean }) {
    const logs: { level: string; msg: string; meta: any }[] = [];
    const logger: any = {};
    for (const level of ['debug', 'info', 'warn', 'error', 'trace']) {
      logger[level] = (meta: any, msg: string) => logs.push({ level, msg, meta });
    }
    logger.child = () => logger;

    const engine = new SqliteCacheEngine(
      { namespace: 'n', database: 'd' } as any,
      logger,
      opts ?? {}
    );
    stubTransport(engine, (type) => (type === 'open' ? openReply : {}));
    return { engine, logs };
  }

  it('publishes a persistent store and logs no error', async () => {
    const { engine, logs } = makeEngine({ persisted: true });
    await engine.connect('user:abc');

    expect(engine.storageHealth).toEqual({
      status: 'persistent',
      fallback: false,
      error: undefined,
    });
    expect(logs.some((l) => l.level === 'error')).toBe(false);
  });

  it('publishes the fallback, its reason, and an error log when OPFS is lost', async () => {
    const { engine, logs } = makeEngine({
      persisted: false,
      opfsError: 'NoModificationAllowedError: locked',
    });
    await engine.connect('user:abc');

    expect(engine.storageHealth).toEqual({
      status: 'memory',
      fallback: true,
      error: 'NoModificationAllowedError: locked',
    });
    const err = logs.find((l) => l.level === 'error');
    expect(err?.msg).toContain('IN MEMORY');
    expect(err?.meta.opfsError).toBe('NoModificationAllowedError: locked');
    // Inspectable from the console without any logging configured.
    expect((globalThis as any).__sqliteStats.persisted).toBe(false);
  });

  // A subscriber almost always attaches AFTER connect() (components mount
  // later), so an immediate fire is the only way it learns about a fallback.
  it('fires a late subscriber with the current snapshot', async () => {
    const { engine } = makeEngine({ persisted: false, opfsError: 'boom' });
    await engine.connect('user:abc');

    const seen: any[] = [];
    const unsub = engine.subscribeToStorageHealth((h) => seen.push(h));
    expect(seen).toEqual([{ status: 'memory', fallback: true, error: 'boom' }]);
    unsub();
  });

  // `store: 'memory'` asked for RAM, so it is not a fallback and must not warn.
  it('does not flag a configured in-memory store as a fallback', async () => {
    const { engine, logs } = makeEngine({ persisted: false }, { useOpfs: false });
    await engine.connect('user:abc');

    expect(engine.storageHealth).toEqual({ status: 'memory', fallback: false, error: undefined });
    expect(logs.some((l) => l.level === 'error')).toBe(false);
  });
});

// Storage numbers for the DevTools Storage tab: DB size via the pragmas, row
// counts on demand, and the configured-vs-effective workerSelect split. Errors
// must land in `error` (the worker may be mid bucket-switch), never throw.
describe('SqliteCacheEngine.getStorageDiagnostics', () => {
  function makeEngine(execRows: (sql: string) => unknown[]) {
    const noop = () => {};
    const logger: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
    logger.child = () => logger;
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, logger);
    stubTransport(engine, (type, payload) =>
      type === 'open' ? { persisted: true } : type === 'exec' ? { rows: execRows(payload.sql) } : {}
    );
    return engine;
  }

  it('reports size, freelist, and per-table counts', async () => {
    const engine = makeEngine((sql) => {
      if (sql.includes('pragma_page_count')) return [{ bytes: 40960, freelist: 4096 }];
      if (sql.includes('sqlite_master')) return [{ name: '_00_query' }, { name: 'game' }];
      if (sql.includes('COUNT(*)'))
        return [
          { t: '_00_query', n: 3 },
          { t: 'game', n: 12 },
        ];
      return [];
    });
    await engine.connect('user:abc');

    const diag = await engine.getStorageDiagnostics({ tableCounts: true });
    expect(diag.engine).toBe('sqlite');
    expect(diag.bucketId).toBe('user:abc');
    expect(diag.dbSizeBytes).toBe(40960);
    expect(diag.freelistBytes).toBe(4096);
    expect(diag.tableCounts).toEqual([
      { table: '_00_query', rows: 3 },
      { table: 'game', rows: 12 },
    ]);
    // Default config: workerSelect on, never downgraded.
    expect(diag.workerSelectConfigured).toBe(true);
    expect(diag.workerSelectEffective).toBe(true);
  });

  it('skips table counts unless asked and never throws on a dead worker', async () => {
    const engine = makeEngine(() => [{ bytes: 8192, freelist: 0 }]);
    await engine.connect('anon');

    const diag = await engine.getStorageDiagnostics();
    expect(diag.tableCounts).toBeUndefined();

    // No worker at all → the failure lands in `error`, not as a throw.
    const cold = makeEngine(() => []);
    const coldDiag = await cold.getStorageDiagnostics();
    expect(coldDiag.error).toContain('not connected');
    expect(coldDiag.bucketId).toBe('anon');
  });
});

// Shared-tabs engine role modes: the engine object survives every role change
// (one monotonic epoch per tab), followers speak the same protocol over a
// MessagePort, and the leaderless window parks ops instead of failing the UI.
describe('SqliteCacheEngine role modes', () => {
  function makeSharedEngine() {
    const log: string[] = [];
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger(), {
      shared: true,
      useOpfs: true,
    });
    stubTransport(engine, lifecycleHandler(log));
    return { engine, log };
  }

  /** A fake follower dbPort: replies like the worker over postMessage. */
  function fakeLeaderPort() {
    const port: any = {
      onmessage: null as null | ((ev: { data: any }) => void),
      onmessageerror: null,
      started: false,
      closed: false,
      start() {
        this.started = true;
      },
      close() {
        this.closed = true;
      },
      postMessage(msg: any) {
        queueMicrotask(() => {
          const rest =
            msg.type === 'exec'
              ? { rows: [] }
              : msg.type === 'select'
                ? { rows: [], relationFetches: 0 }
                : {};
          port.onmessage?.({ data: { id: msg.id, ok: true, wt: 0, ...rest } });
        });
      },
    };
    return port;
  }

  it('adoptOwner opens under the worker lock without wiping anything', async () => {
    const { engine, log } = makeSharedEngine();
    const health = await engine.adoptOwner('anon', {
      workerLockName: 'sp00ky-tabs:fp:anon:worker:1',
      allowMemoryFallback: false,
      resumeHeld: false,
    });
    expect(health.status).toBe('persistent');
    expect(engine.storageHealth.role).toBe('leader');
    expect(log[0]).toBe('open');
    expect(log).not.toContain('run'); // no local _00_query table any more, nothing to wipe
    expect(engine.currentBucketId).toBe('anon');
  });

  it('adoptAttached serves reads over the leader port and reports follower role', async () => {
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger(), {
      shared: true,
    });
    const port = fakeLeaderPort();
    await engine.adoptAttached(
      port,
      { bucketId: 'anon', storageHealth: { status: 'persistent', fallback: false } },
      () => {}
    );
    expect(port.started).toBe(true);
    expect(engine.storageHealth).toMatchObject({ status: 'persistent', role: 'follower' });
    await expect(engine.getById('game', 'game:1')).resolves.toBeNull();
    expect((globalThis as any).__sqliteStats.proxiedOps).toBeGreaterThan(0);
  });

  it('parks ops through a leader loss and releases them on promotion', async () => {
    const { engine } = makeSharedEngine();
    const port = fakeLeaderPort();
    await engine.adoptAttached(
      port,
      { bucketId: 'anon', storageHealth: { status: 'persistent', fallback: false } },
      () => {}
    );
    const epochBefore = engine.epoch;
    engine.onLeaderLost('leader tab closed');
    expect(engine.epoch).toBe(epochBefore + 1);

    // Issued during the leaderless window: must not reject immediately.
    const read = engine.getById('_00_query', 'h1');
    let settled = false;
    void read.finally(() => {
      settled = true;
    });
    await new Promise((r) => setTimeout(r, 10));
    expect(settled).toBe(false);

    await engine.adoptOwner('anon', {
      workerLockName: 'sp00ky-tabs:fp:anon:worker:2',
      allowMemoryFallback: false,
      resumeHeld: false,
    });
    await expect(read).resolves.toBeNull();
    expect(engine.storageHealth.role).toBe('leader');
  });

  /**
   * A transport with the REAL pending-map semantics (the shared fixture never
   * rejects on close, which would make the drain test vacuous): `exec` parks
   * until `flush()`, and `close` rejects whatever is still parked.
   */
  function deferredTransport() {
    const parked: Array<{ resolve: (v: any) => void; reject: (e: unknown) => void }> = [];
    let closed = false;
    const transport: any = {
      kind: 'worker',
      get connected() {
        return !closed;
      },
      call(type: string) {
        if (closed) return Promise.reject(new Error('SQLite worker crashed: transport closed'));
        if (type === 'open') return Promise.resolve({ persisted: true });
        if (type !== 'exec') return Promise.resolve({});
        return new Promise((resolve, reject) => parked.push({ resolve, reject }));
      },
      shutdown: () => Promise.resolve(),
      failAll() {},
      close(reason = 'closed', err?: Error) {
        if (closed) return;
        closed = true;
        const e = err ?? new Error(`SQLite worker crashed: ${reason}`);
        for (const p of parked.splice(0)) p.reject(e);
      },
      flush() {
        for (const p of parked.splice(0)) p.resolve({ rows: [] });
      },
      get parkedCount() {
        return parked.length;
      },
    };
    return transport;
  }

  // Regression (WhitePawn first-login): the bucket switch runs
  // moveToBucket → teardownLeader → releaseOwnership, which lives on the
  // transition chain, NOT the opQueue — so a query's fetch can be sitting at
  // the worker when ownership is released. Tearing the transport down under it
  // rejected that fetch with "SQLite worker crashed: ownership released", which
  // nothing retries (and nobody caught: an unhandled rejection in the console).
  it('releaseOwnership waits for in-flight ops instead of killing them', async () => {
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger(), {
      shared: true,
    });
    const transport = deferredTransport();
    (engine as any).createTransport = () => transport;
    await engine.adoptOwner('anon', {
      workerLockName: 'sp00ky-tabs:fp:anon:worker:1',
      allowMemoryFallback: false,
      resumeHeld: false,
    });

    const read = engine.getById('_00_query', 'h1');
    await new Promise((r) => setTimeout(r, 0));
    expect(transport.parkedCount).toBe(1);

    const released = engine.releaseOwnership();
    await new Promise((r) => setTimeout(r, 0));
    // Still parked: the teardown is waiting on it rather than failing it.
    expect(transport.parkedCount).toBe(1);
    transport.flush();

    await released;
    await expect(read).resolves.toBeNull();
  });

  it('fails an undrainable op as a retryable transport loss, not a crash', async () => {
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger(), {
      shared: true,
    });
    const transport = deferredTransport();
    (engine as any).createTransport = () => transport;
    await engine.adoptOwner('anon', {
      workerLockName: 'sp00ky-tabs:fp:anon:worker:1',
      allowMemoryFallback: false,
      resumeHeld: false,
    });

    const read = engine.getById('_00_query', 'h1');
    const caught = read.catch((e: unknown) => e);
    await new Promise((r) => setTimeout(r, 0));
    // The op never answers: the drain times out and the teardown proceeds.
    (engine as any).drainInFlight = () => Promise.resolve();
    await engine.releaseOwnership();

    const err = await caught;
    expect(err).toBeInstanceOf(BrokerPortClosedError);
    expect((err as Error).message).toContain('ownership released');
  });

  it('bumps the epoch on promotion after having had a store (fences in-flight chains)', async () => {
    const { engine } = makeSharedEngine();
    await engine.adoptOwner('anon', {
      workerLockName: 'sp00ky-tabs:fp:anon:worker:1',
      allowMemoryFallback: false,
      resumeHeld: false,
    });
    const before = engine.epoch;
    await engine.adoptOwner('anon', {
      workerLockName: 'sp00ky-tabs:fp:anon:worker:3',
      allowMemoryFallback: false,
      resumeHeld: false,
    });
    expect(engine.epoch).toBeGreaterThan(before);
  });
});

// `pureWriteOpResult` is the single source of truth for what a pure-write op
// contributes to a query's per-statement results. The batched fast path in
// `query()` and the per-op `execOp` path BOTH route through it, so a caller that
// reads a statement's output (e.g. `create()` reads `resultIndex:0` for the new
// row + its id) sees the same shape either way.
describe('pureWriteOpResult', () => {
  it('echoes the written row (with id) for an upsert — no read-back', () => {
    const op: SqlOp = {
      kind: 'upsert',
      id: 'connection:CONN_abc',
      data: { provider: 'chesscom', username: 'hikaru' },
      mode: 'replace',
    };
    expect(pureWriteOpResult(op)).toEqual({
      provider: 'chesscom',
      username: 'hikaru',
      id: 'connection:CONN_abc',
    });
  });

  it('stringifies a RecordId id via stableKey', () => {
    const op: SqlOp = {
      kind: 'upsert',
      id: new RecordId('connection', 'CONN_abc'),
      data: { provider: 'lichess' },
      mode: 'replace',
    };
    expect(pureWriteOpResult(op)).toEqual({ provider: 'lichess', id: 'connection:CONN_abc' });
  });

  it('yields [] for delete / deleteAll and null for noop', () => {
    expect(pureWriteOpResult({ kind: 'delete', id: 'game:1' })).toEqual([]);
    expect(pureWriteOpResult({ kind: 'deleteAll', table: 'game' })).toEqual([]);
    expect(pureWriteOpResult({ kind: 'noop' })).toBeNull();
  });
});

// Regression: a single `create()` compiles to an all-upsert transaction
// (createSet for the row + createMutation for the pending-mutation log) and
// extracts `resultIndex:0` for the created row. The SQLite fast path must return
// that row (with its id) at that index — returning `[]` there dropped the id and
// crashed the reconcile in `encodeRecordId` ("reading 'table'").
describe('create() tx result shaping (fast path parity)', () => {
  it('resultIndex:0 carries the created row with its id', () => {
    const rid = new RecordId('connection', 'CONN_abc');
    const mid = new RecordId('_00_pending_mutations', '1');
    const vars = {
      id: rid,
      mid,
      data_provider: 'chesscom',
      data_username: 'hikaru',
    };

    // Same statement pair DataModule.create emits.
    const sealed = surql.seal(
      surql.tx([
        surql.createSet('id', [
          { key: 'provider', variable: 'data_provider' },
          { key: 'username', variable: 'data_username' },
        ]),
        surql.createMutation('create', 'mid', 'id', 'data'),
      ]),
      { resultIndex: 0 }
    );

    const { transaction, ops } = translateSurql(sealed.sql, vars);
    expect(transaction).toBe(true);
    // Both statements are upserts → the engine takes the all-write fast path.
    expect(ops.every((o) => o.kind === 'upsert')).toBe(true);

    // Fast-path shaping: [null (BEGIN), ...one result per statement].
    const shaped = [null, ...ops.map(pureWriteOpResult)];
    const created = sealed.extract(shaped) as unknown as { id: unknown; provider: string };

    expect(created.id).toBe('connection:CONN_abc');
    expect(created.provider).toBe('chesscom');
  });

  // The outbox row is the ONLY copy of a pending create once the in-memory
  // UpEvent is gone (reload, or a shared-tabs follower whose row the leader
  // replays). It must carry the payload: `processUpEvent` does
  // `Object.keys(event.data)`, so a row written with `data` unbound produces a
  // create that can never be sent AND blocks every later mutation behind it.
  //
  // Note the vars: the RECORD is written from per-key `data_<field>` vars, so
  // it is easy to emit `data = $data` and never bind `data` at the call site.
  // That is exactly the bug this asserts against.
  it('the outbox row carries the whole payload, not just the record id', () => {
    const payload = { provider: 'chesscom', username: 'hikaru' };
    const vars = {
      id: new RecordId('connection', 'CONN_abc'),
      mid: new RecordId('_00_pending_mutations', '1'),
      data: payload,
      data_provider: 'chesscom',
      data_username: 'hikaru',
    };

    const sealed = surql.seal(
      surql.tx([
        surql.createSet('id', [
          { key: 'provider', variable: 'data_provider' },
          { key: 'username', variable: 'data_username' },
        ]),
        surql.createMutation('create', 'mid', 'id', 'data'),
      ]),
      { resultIndex: 0 }
    );

    const { ops } = translateSurql(sealed.sql, vars);
    const outboxRow = pureWriteOpResult(ops[1]) as Record<string, unknown>;

    expect(outboxRow.mutationType).toBe('create');
    expect(outboxRow.data).toEqual(payload);
  });
});


// The circuit snapshot + version scan the boot-time prime reads through this
// engine. Bytes and meta must round-trip as ONE batch (a reader must never see
// new bytes with old meta) and a table that does not exist yet scans as empty.
describe('SqliteCacheEngine circuit snapshot + scanVersions', () => {
  it('stores bytes and meta in one batch and reads them back', async () => {
    const stored = new Map<string, unknown>();
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger());
    const types: string[] = [];
    stubTransport(engine, (type: string, payload: any) => {
      types.push(type);
      if (type === 'open') return { persisted: true };
      if (type === 'batch') {
        for (const stmt of payload as { sql: string; bind: unknown[] }[]) {
          stored.set(stmt.bind[0] as string, stmt.bind[1]);
        }
        return {};
      }
      if (type === 'exec') {
        const ids = (payload.bind ?? []) as string[];
        return { rows: ids.filter((id) => stored.has(id)).map((id) => ({ id, data: stored.get(id) })) };
      }
      return {};
    });
    await engine.connect('anon');

    expect(await engine.getSnapshot('circuit')).toBeNull();
    const bytes = new Uint8Array([1, 2, 3]);
    await engine.putSnapshot('circuit', bytes, { formatVersion: 1, schemaHash: 'h', savedAt: 5 });
    expect(types.filter((t) => t === 'batch')).toHaveLength(1);
    const snap = await engine.getSnapshot('circuit');
    expect(snap?.bytes).toEqual(bytes);
    expect(snap?.meta).toEqual({ formatVersion: 1, schemaHash: 'h', savedAt: 5 });
  });

  it('scans (id, rv) per table and reads a missing table as empty', async () => {
    const engine = new SqliteCacheEngine({ namespace: 'n', database: 'd' } as any, makeLogger());
    stubTransport(engine, (type: string, payload: any) => {
      if (type === 'open') return { persisted: true };
      if (type === 'exec') {
        if (String(payload.sql).includes('"thing"')) {
          return { rows: [{ id: 'thing:a', rv: 3 }, { id: 'thing:b', rv: null }] };
        }
        throw new Error('no such table');
      }
      return {};
    });
    await engine.connect('anon');
    expect(await engine.scanVersions(['thing', 'ghost'])).toEqual({
      thing: [
        ['thing:a', 3],
        ['thing:b', 0],
      ],
      ghost: [],
    });
  });
});
