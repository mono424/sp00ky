import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ConnectionSupervisor } from './connection-supervisor';
import type { ReconnectConfig } from '../../types';

// The supervisor covers the two failure modes the SDK's own reconnect cannot:
//   - it stopped trying (attempts exhausted, or its post-reconnect handshake
//     threw and it terminated the engine) — nothing else would ever reconnect
//   - the socket never closed at all (half-open: peer gone, readyState OPEN,
//     no `close` event, so no reconnect is ever triggered)

const CONFIG: Required<ReconnectConfig> = {
  attempts: -1,
  retryDelayMax: 8_000,
  heartbeatIntervalMs: 1_000,
  heartbeatTimeoutMs: 500,
  superviseRetryDelayMaxMs: 4_000,
};

function makeRemote() {
  const handlers = new Map<string, Array<(...a: any[]) => void>>();
  const state = { status: 'connected' as string };
  const remote: any = {
    getStatus: () => state.status,
    getReconnectConfig: () => CONFIG,
    subscribeConnection: (event: string, cb: (...a: any[]) => void) => {
      const arr = handlers.get(event) ?? [];
      arr.push(cb);
      handlers.set(event, arr);
      return () => {
        handlers.set(
          event,
          (handlers.get(event) ?? []).filter((h) => h !== cb)
        );
      };
    },
    connect: vi.fn().mockImplementation(async () => {
      state.status = 'connected';
    }),
    forceClose: vi.fn().mockImplementation(async () => {
      state.status = 'disconnected';
      emit('disconnected');
    }),
    query: vi.fn().mockResolvedValue([true]),
  };
  function emit(event: string, ...args: any[]) {
    for (const cb of Array.from(handlers.get(event) ?? [])) cb(...args);
  }
  return { remote, emit, state, handlerCount: () => handlers.size };
}

const silentLogger: any = (() => {
  const l: any = {
    child: () => l,
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
    trace: () => {},
  };
  return l;
})();

function makeSupervisor(remote: any) {
  return new ConnectionSupervisor(remote, silentLogger, CONFIG);
}

describe('ConnectionSupervisor', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('reconnects after the SDK gives up, retrying on backoff until it succeeds', async () => {
    const { remote, emit, state } = makeRemote();
    // Two failures, then success — proves the loop keeps going rather than
    // giving up like the SDK does.
    remote.connect
      .mockRejectedValueOnce(new Error('refused'))
      .mockRejectedValueOnce(new Error('refused'))
      .mockImplementationOnce(async () => {
        state.status = 'connected';
        emit('connected');
      });

    const sup = makeSupervisor(remote);
    sup.start();

    state.status = 'disconnected';
    emit('disconnected');
    expect(sup.connection).toBe('disconnected');

    // 1s, then 2s, then 4s (capped at superviseRetryDelayMaxMs).
    await vi.advanceTimersByTimeAsync(1_000);
    expect(remote.connect).toHaveBeenCalledTimes(1);
    expect(sup.connection).toBe('reconnecting');

    await vi.advanceTimersByTimeAsync(2_000);
    expect(remote.connect).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(4_000);
    expect(remote.connect).toHaveBeenCalledTimes(3);
    expect(sup.connection).toBe('connected');

    // Recovered: no further attempts.
    await vi.advanceTimersByTimeAsync(30_000);
    expect(remote.connect).toHaveBeenCalledTimes(3);

    sup.dispose();
  });

  it('does not reconnect while the SDK is still retrying', async () => {
    const { remote, emit, state } = makeRemote();
    const sup = makeSupervisor(remote);
    sup.start();

    state.status = 'reconnecting';
    emit('reconnecting');
    expect(sup.connection).toBe('reconnecting');

    await vi.advanceTimersByTimeAsync(30_000);
    // The SDK owns the socket during its own retry loop; racing it would open
    // a second connection.
    expect(remote.connect).not.toHaveBeenCalled();

    sup.dispose();
  });

  it('forces a teardown when a heartbeat never answers (half-open socket)', async () => {
    const { remote, emit } = makeRemote();
    // The defining symptom: the RPC never settles and no `close` ever fires.
    remote.query.mockImplementation(() => new Promise(() => {}));

    const sup = makeSupervisor(remote);
    emit('connected');
    sup.start();

    await vi.advanceTimersByTimeAsync(CONFIG.heartbeatIntervalMs);
    expect(remote.query).toHaveBeenCalledWith('RETURN true');
    expect(remote.forceClose).not.toHaveBeenCalled();

    // One failure is inconclusive — the probe shares a queue with ordinary
    // traffic, so it re-probes rather than tearing down a possibly-fine socket.
    await vi.advanceTimersByTimeAsync(CONFIG.heartbeatTimeoutMs);
    expect(remote.forceClose).not.toHaveBeenCalled();

    // The second consecutive failure is the one that tears it down.
    await vi.advanceTimersByTimeAsync(5_000 + CONFIG.heartbeatTimeoutMs);
    expect(remote.forceClose).toHaveBeenCalledTimes(1);

    // The forced close published `disconnected`, so the revive loop takes over.
    await vi.advanceTimersByTimeAsync(1_000);
    expect(remote.connect).toHaveBeenCalled();

    sup.dispose();
  });

  it('does not tear down a healthy socket after a single slow probe', async () => {
    // The regression this guards: the heartbeat rides the same serialized queue
    // as every other RPC, so one busy window (a big sync burst) used to
    // force-close a working connection — and the reconnect then re-registered
    // every active query about a second later.
    const { remote, emit } = makeRemote();
    let calls = 0;
    remote.query.mockImplementation(() => {
      calls++;
      // First probe hangs past its deadline, the next answers normally.
      return calls === 1 ? new Promise(() => {}) : Promise.resolve(true);
    });

    const sup = makeSupervisor(remote);
    emit('connected');
    sup.start();

    await vi.advanceTimersByTimeAsync(CONFIG.heartbeatIntervalMs + CONFIG.heartbeatTimeoutMs);
    expect(remote.forceClose).not.toHaveBeenCalled();

    // The retry succeeds, so the socket survives and heartbeating continues.
    await vi.advanceTimersByTimeAsync(5_000 + CONFIG.heartbeatIntervalMs * 2);
    expect(remote.forceClose).not.toHaveBeenCalled();
    expect(sup.connection).toBe('connected');

    sup.dispose();
  });

  it('keeps heartbeating while the connection is healthy', async () => {
    const { remote, emit } = makeRemote();
    const sup = makeSupervisor(remote);
    emit('connected');
    sup.start();

    await vi.advanceTimersByTimeAsync(CONFIG.heartbeatIntervalMs * 3 + 10);
    expect(remote.query.mock.calls.length).toBeGreaterThanOrEqual(3);
    expect(remote.forceClose).not.toHaveBeenCalled();
    expect(sup.connection).toBe('connected');

    sup.dispose();
  });

  it('parks reconnects while the browser reports offline, and resumes on online', async () => {
    const listeners = new Map<string, Array<() => void>>();
    vi.stubGlobal('window', {
      addEventListener: (e: string, cb: () => void) => {
        listeners.set(e, [...(listeners.get(e) ?? []), cb]);
      },
      removeEventListener: () => {},
    });
    const { remote, emit, state } = makeRemote();
    const sup = makeSupervisor(remote);
    sup.start();

    state.status = 'disconnected';
    emit('disconnected');
    listeners.get('offline')?.forEach((cb) => cb());

    // Retrying a socket against a down interface only burns backoff.
    await vi.advanceTimersByTimeAsync(30_000);
    expect(remote.connect).not.toHaveBeenCalled();

    // `online` is the strongest available hint a reconnect will now work, so it
    // probes immediately rather than waiting out a backoff.
    listeners.get('online')?.forEach((cb) => cb());
    await vi.advanceTimersByTimeAsync(0);
    expect(remote.connect).toHaveBeenCalledTimes(1);

    sup.dispose();
  });

  it('un-sticks the forced offline state when the SDK slept through the outage', async () => {
    const listeners = new Map<string, Array<() => void>>();
    vi.stubGlobal('window', {
      addEventListener: (e: string, cb: () => void) => {
        listeners.set(e, [...(listeners.get(e) ?? []), cb]);
      },
      removeEventListener: () => {},
    });
    const { remote, emit } = makeRemote();
    const sup = makeSupervisor(remote);
    emit('connected');
    sup.start();

    // The browser reports offline, but the SDK's own socket never notices: its
    // status stays `connected` throughout, so it emits no transition at all.
    listeners.get('offline')?.forEach((cb) => cb());
    expect(sup.connection).toBe('disconnected');

    // Back online. Nothing but the probe can correct the forced state here -
    // there is no SDK event coming - and traffic flowing under a permanent
    // "offline" indicator is exactly the bug.
    listeners.get('online')?.forEach((cb) => cb());
    await vi.advanceTimersByTimeAsync(10);
    expect(remote.query).toHaveBeenCalled();
    expect(sup.connection).toBe('connected');

    sup.dispose();
  });

  it('probes immediately when a hidden tab becomes visible', async () => {
    const listeners = new Map<string, Array<() => void>>();
    vi.stubGlobal('document', {
      visibilityState: 'visible',
      addEventListener: (e: string, cb: () => void) => {
        listeners.set(e, [...(listeners.get(e) ?? []), cb]);
      },
      removeEventListener: () => {},
    });
    const { remote, emit } = makeRemote();
    const sup = makeSupervisor(remote);
    emit('connected');
    sup.start();

    listeners.get('visibilitychange')?.forEach((cb) => cb());
    await vi.advanceTimersByTimeAsync(0);
    // Connected-looking socket: probe it rather than reconnect. A sleep/wake
    // cycle is exactly how you get a socket that looks fine and isn't.
    expect(remote.query).toHaveBeenCalledWith('RETURN true');

    sup.dispose();
  });

  it('dispose stops every timer and unsubscribes', async () => {
    const { remote, emit, state } = makeRemote();
    const sup = makeSupervisor(remote);
    emit('connected');
    sup.start();

    sup.dispose();
    state.status = 'disconnected';
    emit('disconnected');

    await vi.advanceTimersByTimeAsync(60_000);
    expect(remote.connect).not.toHaveBeenCalled();
    expect(remote.query).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('reports state changes to subscribers', () => {
    const { remote, emit, state } = makeRemote();
    const sup = makeSupervisor(remote);
    sup.start();

    const seen: string[] = [];
    sup.subscribe((s) => seen.push(s));
    // Fires immediately with the current value.
    expect(seen).toEqual(['connected']);

    state.status = 'reconnecting';
    emit('reconnecting');
    state.status = 'connected';
    emit('connected');
    expect(seen).toEqual(['connected', 'reconnecting', 'connected']);

    sup.dispose();
  });
});
