import { describe, it, expect, vi, afterEach } from 'vitest';
import { RecordId } from 'surrealdb';
import { DevToolsService } from './index';

/**
 * A state push serializes EVERY active query's full record set and postMessage
 * clones it again, so its cost scales with the whole client dataset. It is
 * requested per event — including one per local DB query — so without
 * coalescing a page load's few hundred local queries turn a few MB of rows into
 * GBs of short-lived large-object garbage and OOM the renderer. These tests pin
 * the coalescing, not the payload. `onStateChanged` stands in for any trigger.
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

  const local: any = {
    query: async () => [],
    getConfig: () => ({ store: 'memory' }),
    currentBucketId: 'anon',
    storageHealth: { status: 'memory', fallback: false },
  };
  const remote: any = { query: async () => [] };
  const auth: any = {
    isAuthenticated: false,
    currentUser: undefined,
    eventSystem: { subscribe: noop },
  };
  // One query holding a lot of rows: the thing whose repeated serialization is
  // what actually blows the heap.
  const records = Array.from({ length: recordCount }, (_, i) => ({ id: `game:${i}`, pgn: 'x' }));
  const dataManager: any = {
    getActiveQueries: () => [
      {
        config: { id: new RecordId('_00_query', 'q1'), params: {} },
        status: 'idle',
        records,
        updateCount: 1,
      },
    ],
    phaseTimings: () => ({}),
  };

  const service = new DevToolsService(local, remote, logger, { tables: [] } as any, auth, dataManager);
  // Whatever the next push should carry; tests move it between requests.
  const mutations: any = {
    outbox: [],
    pendingWrites: new Map(),
    failedCount: 0,
    tabRole: 'solo',
    sync: { health: { status: 'healthy', consecutiveFailures: 0, everConnected: true, connection: 'connected' } },
  };
  service.setMutationSource({
    state: () => mutations,
    listFailed: async () => [],
    retryFailed: async () => false,
    discardFailed: async () => false,
  });
  // Announce a consumer, exactly like the extension's page-script does.
  for (const cb of listeners) {
    cb({ source: fakeWindow, data: { type: 'SP00KY_DEVTOOLS_CONNECT' } });
  }
  const statePushes = () => posted.filter((m) => m.type === 'SP00KY_STATE_CHANGED');
  return { service, statePushes, posted, mutations };
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('DevToolsService state-push coalescing', () => {
  it('pushes once on connect, on the next macrotask', () => {
    vi.useFakeTimers();
    const { statePushes } = harness();
    // Never inline: a push serializes the state, and the request can come
    // from inside an ingest or a mutation the app is awaiting.
    expect(statePushes().length).toBe(0);
    vi.advanceTimersByTime(0);
    expect(statePushes().length).toBe(1);
  });

  it('collapses a burst of change requests into ONE trailing push', () => {
    vi.useFakeTimers();
    const { service, statePushes } = harness();
    const before = statePushes().length;

    // What a page load looks like: hundreds of requests, each of which used to
    // serialize the entire query state.
    for (let i = 0; i < 400; i++) service.onStateChanged();
    // Nothing extra yet — the burst is queued, not serialized 400 times.
    expect(statePushes().length).toBe(before);

    vi.advanceTimersByTime(300);
    expect(statePushes().length).toBe(before + 1);
  });

  it('still pushes again once the window has passed', () => {
    vi.useFakeTimers();
    const { service, statePushes } = harness();
    const before = statePushes().length;

    service.onStateChanged();
    vi.advanceTimersByTime(300);
    expect(statePushes().length).toBe(before + 1);

    // A request arriving right after that flush is still inside the window, so it
    // queues rather than pushing again...
    service.onStateChanged();
    expect(statePushes().length).toBe(before + 1);
    vi.advanceTimersByTime(300);
    expect(statePushes().length).toBe(before + 2);

    // ...but once the tab has been idle past the window, the next request pushes
    // straight away, so the panel never waits on a quiet app.
    vi.advanceTimersByTime(1000);
    service.onStateChanged();
    vi.advanceTimersByTime(0);
    expect(statePushes().length).toBe(before + 3);
  });

  it('drops a queued push when the consumer disconnects mid-window', () => {
    vi.useFakeTimers();
    const { service, statePushes, posted } = harness();
    const before = statePushes().length;

    service.onStateChanged();
    (service as any).enabled = false; // panel closed while the push was queued
    vi.advanceTimersByTime(300);
    expect(statePushes().length).toBe(before);
    expect(posted.some((m) => m.type === 'SP00KY_STATE_CHANGED' && m.state === undefined)).toBe(false);
  });

  it('serializes the LATEST state at flush time, not at request time', () => {
    vi.useFakeTimers();
    const { service, statePushes, mutations } = harness();
    vi.advanceTimersByTime(300);

    service.onStateChanged();
    mutations.failedCount = 1;
    service.onStateChanged();
    mutations.failedCount = 2;
    vi.advanceTimersByTime(300);

    // One push for the window, carrying what was true when it flushed.
    expect(statePushes().at(-1).state.mutations.counts.failed).toBe(2);
  });
});
