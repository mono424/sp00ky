import { describe, it, expect, vi, afterEach } from 'vitest';
import { RecordId } from 'surrealdb';
import { DevToolsService } from './index';
import { initialHealth } from '../../state/client-state';
import { LogTap, createLogger } from '../../services/logger/index';

// Vitest runs in node, where `pino` resolves to its node build and ignores the
// `browser` options the tap hangs off. Apps bundle the browser build.
// The specifier is widened to `string`: pino ships no types for that entry.
vi.mock('pino', async () => ({ default: (await import('pino/browser.js' as string)).default }));

/**
 * The pushed state used to carry every view's full record set (and both
 * membership arrays), deep-cloned once by the serializer and again by
 * postMessage, on every sync event, from inside the ingest call stack. On a
 * client holding a few thousand rows that is what the main thread was doing
 * instead of finishing the write the app was awaiting. Pushes now carry counts
 * and capped ids; rows are pulled per view on demand.
 */
function harness(recordCount = 500) {
  const posted: any[] = [];
  const listeners: ((e: any) => void)[] = [];
  const fakeWindow: any = {
    postMessage: (msg: any) => posted.push(msg),
    addEventListener: (_type: string, cb: (e: any) => void) => listeners.push(cb),
    dispatchEvent: () => true,
  };
  fakeWindow.self = fakeWindow;
  vi.stubGlobal('window', fakeWindow);
  vi.stubGlobal('CustomEvent', class {
    type: string;
    constructor(type: string) {
      this.type = type;
    }
  });
  const noop = () => {};
  const logger: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
  logger.child = () => logger;
  const infoQueries: string[] = [];
  const local: any = {
    query: async (sql: string) => {
      infoQueries.push(sql);
      return [];
    },
    getConfig: () => ({ store: 'memory' }),
    currentBucketId: 'anon',
    storageHealth: { status: 'memory', fallback: false },
  };
  const remote: any = { query: async () => [] };
  const auth: any = { isAuthenticated: false, currentUser: undefined, eventSystem: { subscribe: noop } };
  const records = Array.from({ length: recordCount }, (_, i) => ({ id: `game:${i}`, pgn: 'x' }));
  const localArray = records.map((r) => [r.id, 1] as [string, number]);
  const id = new RecordId('_00_query', 'q1');
  const query = {
    config: { id, params: {}, localArray, remoteArray: localArray, surql: 'SELECT * FROM game' },
    status: 'idle',
    records,
    updateCount: 1,
  };
  const dataManager: any = {
    getActiveQueries: () => [query],
    getQueryById: (rid: RecordId<string>) => (String(rid) === String(id) ? query : undefined),
    phaseTimings: () => ({}),
  };
  const service = new DevToolsService(local, remote, logger, { tables: [] } as any, auth, dataManager);
  for (const cb of listeners) cb({ source: fakeWindow, data: { type: 'SP00KY_DEVTOOLS_CONNECT' } });
  const statePushes = () => posted.filter((m) => m.type === 'SP00KY_STATE_CHANGED');
  return { service, statePushes, posted, fakeWindow, infoQueries, logger };
}

describe('DevTools pushed state shape', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('carries counts and capped ids, never the rows', async () => {
    vi.useFakeTimers();
    const { service, statePushes } = harness(500);
    service.onStateChanged();
    await vi.advanceTimersByTimeAsync(300);
    const push = statePushes().at(-1);
    expect(push).toBeDefined();
    const q: any = Object.values(push.state.activeQueries)[0];
    expect(q.data).toBeUndefined();
    expect(q.localArray).toBeUndefined();
    expect(q.remoteArray).toBeUndefined();
    expect(q.dataSize).toBe(500);
    expect(q.localCount).toBe(500);
    expect(q.remoteCount).toBe(500);
    expect(q.localIds).toHaveLength(200);
    expect(q.idsTruncated).toBe(true);
  });

  it('never pushes synchronously inside the call that requested it', async () => {
    vi.useFakeTimers();
    const { service, statePushes } = harness(5);
    // Connect already queued one push; drain it so the next request is "idle".
    await vi.advanceTimersByTimeAsync(300);
    const before = statePushes().length;
    service.onStreamUpdate({ queryHash: 'q1', localArray: [], op: 'CREATE' });
    expect(statePushes().length).toBe(before);
    await vi.advanceTimersByTimeAsync(0);
    expect(statePushes().length).toBe(before + 1);
  });

  it('serves the rows of one view on demand', () => {
    const { service, fakeWindow } = harness(3);
    void service;
    const state = fakeWindow.__00__.getState();
    const hash = Number(Object.keys(state.activeQueries)[0]);
    const rows = fakeWindow.__00__.getQueryRows(hash);
    expect(rows.data).toHaveLength(3);
    expect(rows.localArray).toHaveLength(3);
    expect(fakeWindow.__00__.getQueryRows(12345)).toBeNull();
  });

  it('logs a stream update as counts and timings, never the membership array', () => {
    const { service, logger } = harness(2);
    const debug = vi.spyOn(logger, 'debug');
    service.onStreamUpdate({ queryHash: 'q1', localArray: [['a', 1], ['b', 1]], op: 'UPDATE', storeApplyMs: 1.5 });
    const [fields, msg] = debug.mock.calls.at(-1) as [any, string];
    expect(msg).toBe('StreamUpdate');
    expect(fields).toMatchObject({ queryHash: 'q1', op: 'UPDATE', localCount: 2, storeApplyMs: 1.5 });
    expect(fields.localArray).toBeUndefined();
  });

  it('no longer keeps an event log, but still sends the field older panels key on', () => {
    const { fakeWindow } = harness(1);
    expect(fakeWindow.__00__.getState().eventsHistory).toEqual([]);
    expect(fakeWindow.__00__.clearHistory).toBeUndefined();
  });

  it('ignores a synthetic re-materialize', async () => {
    vi.useFakeTimers();
    const { service, statePushes } = harness(1);
    await vi.advanceTimersByTimeAsync(300);
    const before = statePushes().length;
    service.onStreamUpdate({ queryHash: 'q1', localArray: [], op: 'UPDATE', synthetic: true });
    await vi.advanceTimersByTimeAsync(300);
    expect(statePushes().length).toBe(before);
  });

  it('refreshes the table list on an explicit pull, not on a push', async () => {
    vi.useFakeTimers();
    const { service, fakeWindow, infoQueries } = harness(1);
    const connectRefreshes = infoQueries.filter((q) => q.includes('INFO FOR DB')).length;
    for (let i = 0; i < 10; i++) {
      service.onStreamUpdate({ queryHash: 'q1', localArray: [], op: 'UPDATE' });
      await vi.advanceTimersByTimeAsync(300);
    }
    expect(infoQueries.filter((q) => q.includes('INFO FOR DB')).length).toBe(connectRefreshes);
    vi.setSystemTime(Date.now() + 60_000);
    fakeWindow.__00__.getState();
    expect(infoQueries.filter((q) => q.includes('INFO FOR DB')).length).toBe(connectRefreshes + 1);
  });
});

describe('DevTools mutations and logs', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  const mutationState = (): any => ({
    outbox: [
      {
        id: '_00_pending_mutations:0000000001000_0001_t',
        type: 'create',
        recordId: 'thread:a',
        table: 'thread',
        status: 'pending',
        ackedAt: null,
        attempts: 0,
      },
    ],
    pendingWrites: new Map(),
    failedCount: 1,
    tabRole: 'solo',
    sync: { health: initialHealth('connected') },
  });

  it('pushes the outbox and what already left it', async () => {
    vi.useFakeTimers();
    const { service, statePushes } = harness(0);
    service.setMutationSource({
      state: mutationState,
      listFailed: async () => [],
      retryFailed: async () => true,
      discardFailed: async () => false,
    });
    service.onMutation([
      {
        type: 'update',
        mutation_id: new RecordId('_00_pending_mutations', '0000000002000_0002_t'),
        record_id: new RecordId('thread', 'b'),
        data: { title: 'a very long payload the push must not carry' },
        tableName: 'thread',
      },
    ]);
    service.onMutationOutcome({
      mutationId: '_00_pending_mutations:0000000002000_0002_t',
      recordId: 'thread:b',
      eventType: 'update',
      status: 'rolled-back',
      error: 'denied',
    });
    await vi.advanceTimersByTimeAsync(300);
    const m = statePushes().at(-1).state.mutations;
    expect(m.counts).toMatchObject({ pending: 1, rolledBack: 1, failed: 1 });
    expect(m.entries[0]).toMatchObject({
      recordId: 'thread:b',
      status: 'rolled-back',
      error: 'denied',
      fields: ['title'],
      queuedAt: 2000,
    });
    expect(JSON.stringify(m)).not.toContain('very long payload');
  });

  it('answers the tray ops, with a reason when the row is gone', async () => {
    const { service, fakeWindow } = harness(0);
    service.setMutationSource({
      state: mutationState,
      listFailed: async () => [],
      retryFailed: async () => true,
      discardFailed: async () => false,
    });
    expect(await fakeWindow.__00__.mutationOp('retry', { id: 'm1' })).toEqual({ success: true });
    expect(await fakeWindow.__00__.mutationOp('discard', { id: 'm1' })).toMatchObject({
      success: false,
      error: expect.stringContaining('not in the failed tray'),
    });
    expect(await fakeWindow.__00__.mutationOp('listFailed')).toEqual({ success: true, failed: [] });
  });

  it('pushes new log lines as deltas and keeps them out of the state', async () => {
    vi.useFakeTimers();
    vi.spyOn(console, 'log').mockImplementation(() => {});
    const { service, posted, fakeWindow } = harness(0);
    const tap = new LogTap('info');
    const logger = createLogger('info', undefined, tap);
    logger.info('before attach');
    service.setLogTap(tap);
    logger.warn('one');
    logger.info('two');
    await vi.advanceTimersByTimeAsync(300);
    const pushes = posted.filter((m) => m.type === 'SP00KY_LOGS');
    expect(pushes).toHaveLength(1);
    expect(pushes[0].entries.map((e: any) => e.msg)).toEqual(['one', 'two']);
    expect(fakeWindow.__00__.getState().logs).toEqual({ head: 3, consoleLevel: 'info', captureLevel: 'info' });
    const backlog = fakeWindow.__00__.logOp('read', {});
    expect(backlog.entries.map((e: any) => e.msg)).toEqual(['before attach', 'one', 'two']);
    expect(fakeWindow.__00__.logOp('setCaptureLevel', { level: 'debug' })).toMatchObject({
      success: true,
      captureLevel: 'debug',
    });
    logger.debug('now captured');
    expect(fakeWindow.__00__.logOp('read', { after: 3 }).entries.map((e: any) => e.msg)).toEqual(['now captured']);
    expect(fakeWindow.__00__.logOp('setCaptureLevel', { level: 'loud' }).success).toBe(false);
  });
});
