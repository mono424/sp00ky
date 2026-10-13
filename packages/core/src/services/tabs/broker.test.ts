import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { handleConnect, __resetBrokerForTests } from './tabs-broker-worker';
import { fakeChannel, flush, FakePort } from './fake-ports.fixture';
import type { BrokerToTabMessage, TabToBrokerMessage } from './protocol';

/** A connected fake tab: records broker messages, exposes a send helper. */
interface TestTab {
  tabId: string;
  received: { msg: BrokerToTabMessage; ports: FakePort[] }[];
  send: (msg: TabToBrokerMessage) => void;
  port: FakePort;
  /** Messages of one type, newest last. */
  ofType: <T extends BrokerToTabMessage['type']>(
    t: T
  ) => Extract<BrokerToTabMessage, { type: T }>[];
}

function connectTab(tabId: string, opts: { bucket?: string; fingerprint?: string } = {}): TestTab {
  const { port1, port2 } = fakeChannel();
  handleConnect(port1 as unknown as MessagePort);
  const received: TestTab['received'] = [];
  port2.onmessage = (ev) => received.push({ msg: ev.data, ports: ev.ports });
  const tab: TestTab = {
    tabId,
    received,
    port: port2,
    send: (msg) => port2.postMessage(msg),
    ofType: (t) => received.filter((r) => r.msg.type === t).map((r) => r.msg) as any,
  };
  tab.send({
    type: 'hello',
    tabId,
    fingerprint: opts.fingerprint ?? 'fp-test',
    bucketId: opts.bucket ?? 'anon',
    visibility: 'visible',
    heldLeadership: null,
  });
  return tab;
}

/** Complete the promotion handshake for whichever tab got become-leader. */
async function ackLeadership(tab: TestTab, bucket = 'anon'): Promise<number> {
  await flush();
  const grants = tab.ofType('become-leader');
  expect(grants.length).toBeGreaterThan(0);
  const leadershipId = grants[grants.length - 1].leadershipId;
  tab.send({ type: 'leader-ready', tabId: tab.tabId, bucketId: bucket, leadershipId });
  await flush();
  return leadershipId;
}

beforeEach(() => {
  __resetBrokerForTests();
  vi.useFakeTimers();
});
afterEach(() => {
  __resetBrokerForTests();
  vi.useRealTimers();
});

// The broker is the single arbiter of who owns the OPFS pool. These tests
// drive it with fake ports exactly like real tabs would.
describe('tabs broker: election', () => {
  it('elects exactly one leader for the first tab and announces readiness', async () => {
    const a = connectTab('tab-a');
    await flush();
    expect(a.ofType('broker-hello')).toHaveLength(1);
    const id = await ackLeadership(a);
    expect(a.ofType('leader-ready').map((m) => m.leaderTabId)).toContain('tab-a');
    expect(id).toBeGreaterThan(0);
  });

  it('attaches a second tab as follower with a db+sync port pair', async () => {
    const a = connectTab('tab-a');
    await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();

    // No second become-leader; instead ports flow both ways.
    expect(b.ofType('become-leader')).toHaveLength(0);
    const attach = a.received.filter((r) => r.msg.type === 'attach-follower-ports');
    const use = b.received.filter((r) => r.msg.type === 'use-follower-ports');
    expect(attach).toHaveLength(1);
    expect(use).toHaveLength(1);
    expect(attach[0].ports).toHaveLength(2);
    expect(use[0].ports).toHaveLength(2);
    // The two db ports are entangled ends of ONE channel: a message posted on
    // the leader's end arrives on the follower's end. (The broker mints REAL
    // node MessageChannels here; only the control-plane ports are fakes.)
    const leaderDb = attach[0].ports[0] as unknown as MessagePort;
    const followerDb = use[0].ports[0] as unknown as MessagePort;
    const got = new Promise<unknown>((resolve) => {
      followerDb.onmessage = (ev: MessageEvent) => resolve(ev.data);
    });
    leaderDb.postMessage('entangled?');
    vi.useRealTimers();
    await expect(got).resolves.toBe('entangled?');
    leaderDb.close();
    followerDb.close();
    vi.useFakeTimers();
  });

  it('stops re-minting follower ports once the leader confirms attachment', async () => {
    const a = connectTab('tab-a');
    const leadershipId = await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();
    a.send({
      type: 'follower-port-attached',
      tabId: 'tab-a',
      bucketId: 'anon',
      leadershipId,
      followerTabId: 'tab-b',
    });
    await flush();
    const before = b.ofType('use-follower-ports').length;
    await vi.advanceTimersByTimeAsync(60_000);
    expect(b.ofType('use-follower-ports').length).toBe(before);
  });

  it('re-mints follower ports with backoff while unconfirmed', async () => {
    const a = connectTab('tab-a');
    await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();
    expect(b.ofType('use-follower-ports')).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    await flush();
    expect(b.ofType('use-follower-ports')).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(2000);
    await flush();
    expect(b.ofType('use-follower-ports')).toHaveLength(3);
  });

  it('rejects a tab whose fingerprint differs from the first hello', async () => {
    connectTab('tab-a', { fingerprint: 'fp-1' });
    const b = connectTab('tab-b', { fingerprint: 'fp-2' });
    await flush();
    expect(b.ofType('unsupported').map((m) => m.reason)).toEqual(['fingerprint-mismatch']);
  });

  it('keeps leadership ids monotonic across handoffs', async () => {
    const a = connectTab('tab-a');
    const id1 = await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();
    a.send({ type: 'shutdown', tabId: 'tab-a', bucketId: 'anon' });
    await flush();
    const id2 = await ackLeadership(b);
    expect(id2).toBeGreaterThan(id1);
  });
});

describe('tabs broker: failover', () => {
  it('promotes the follower when the leader shuts down gracefully', async () => {
    const a = connectTab('tab-a');
    await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();
    a.send({ type: 'shutdown', tabId: 'tab-a', bucketId: 'anon' });
    await flush();
    expect(b.ofType('become-leader').length).toBeGreaterThan(0);
  });

  it('evicts a leader that misses pongs and re-elects', async () => {
    const a = connectTab('tab-a');
    await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();
    // b keeps answering pings, a goes silent.
    b.port.onmessage = (ev) => {
      b.received.push({ msg: ev.data, ports: ev.ports });
      if (ev.data.type === 'ping') b.send({ type: 'pong', tabId: 'tab-b' });
    };
    await vi.advanceTimersByTimeAsync(21_000);
    await flush();
    expect(b.ofType('become-leader').length).toBeGreaterThan(0);
  });

  it('applies a failure backoff and grants allowMemoryFallback after 3 opfs cycles', async () => {
    const a = connectTab('tab-a');
    await flush();
    for (let cycle = 0; cycle < 3; cycle++) {
      const grants = a.ofType('become-leader');
      const last = grants[grants.length - 1];
      expect(last.allowMemoryFallback).toBe(false);
      a.send({
        type: 'leader-failed',
        tabId: 'tab-a',
        bucketId: 'anon',
        leadershipId: last.leadershipId,
        reason: 'opfs-unavailable: locked',
      });
      await flush();
      // The 1s candidate backoff must pass before the next nomination.
      await vi.advanceTimersByTimeAsync(1100);
      await flush();
    }
    const grants = a.ofType('become-leader');
    expect(grants[grants.length - 1].allowMemoryFallback).toBe(true);
  });

  it('re-elects when a promoted tab never answers become-leader', async () => {
    // The wedge this guards: a tab that hangs mid-promotion sends neither
    // leader-ready nor leader-failed, and keeps answering pings, so without a
    // deadline the namespace keeps a leader that is never `ready` - no follower
    // ports are minted and no re-election ever runs.
    const a = connectTab('tab-a');
    const b = connectTab('tab-b');
    await flush();
    const grantedTo = a.ofType('become-leader').length > 0 ? a : b;
    const other = grantedTo === a ? b : a;
    expect(grantedTo.ofType('become-leader').length).toBe(1);
    // Both tabs keep answering pings; the promoted one just never reports.
    for (const t of [a, b]) {
      t.port.onmessage = (ev) => {
        t.received.push({ msg: ev.data, ports: ev.ports });
        if (ev.data.type === 'ping') t.send({ type: 'pong', tabId: t.tabId });
      };
    }
    await vi.advanceTimersByTimeAsync(21_000);
    await flush();
    // The other tab gets a shot, under a NEW leadership id.
    const handover = other.ofType('become-leader');
    expect(handover.length).toBeGreaterThan(0);
    expect(handover[handover.length - 1].leadershipId).toBeGreaterThan(
      grantedTo.ofType('become-leader')[0].leadershipId
    );
  });

  it('stops the promotion deadline once leader-ready lands', async () => {
    const a = connectTab('tab-a');
    const id = await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();
    a.port.onmessage = (ev) => {
      a.received.push({ msg: ev.data, ports: ev.ports });
      if (ev.data.type === 'ping') a.send({ type: 'pong', tabId: 'tab-a' });
    };
    b.port.onmessage = (ev) => {
      b.received.push({ msg: ev.data, ports: ev.ports });
      if (ev.data.type === 'ping') b.send({ type: 'pong', tabId: 'tab-b' });
    };
    await vi.advanceTimersByTimeAsync(30_000);
    await flush();
    // A confirmed leader is never torn down by the promotion deadline.
    expect(a.ofType('demote')).toHaveLength(0);
    expect(b.ofType('become-leader')).toHaveLength(0);
    expect(a.ofType('become-leader')[0].leadershipId).toBe(id);
  });

  it('demotes a stale leader-ready from a superseded promotion', async () => {
    const a = connectTab('tab-a');
    const staleId = await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();
    a.send({ type: 'shutdown', tabId: 'tab-a', bucketId: 'anon' });
    await flush();
    await ackLeadership(b);
    // The departed tab reconnects and replays its OLD leader-ready.
    const a2 = connectTab('tab-a');
    await flush();
    a2.send({ type: 'leader-ready', tabId: 'tab-a', bucketId: 'anon', leadershipId: staleId });
    await flush();
    expect(a2.ofType('demote').map((m) => m.leadershipId)).toContain(staleId);
  });

  it('prefers a candidate that still holds a live worker (broker restart path)', async () => {
    // Fresh broker instance, two tabs hello simultaneously; one held leadership
    // from before the restart.
    const a = connectTab('tab-a');
    await flush();
    const idA = await ackLeadership(a);
    void idA;
    __resetBrokerForTests();
    const b2 = connectTab('tab-b');
    void b2;
    const { port1, port2 } = fakeChannel();
    handleConnect(port1 as unknown as MessagePort);
    const received: TestTab['received'] = [];
    port2.onmessage = (ev) => received.push({ msg: ev.data, ports: ev.ports });
    port2.postMessage({
      type: 'hello',
      tabId: 'tab-a',
      fingerprint: 'fp-test',
      bucketId: 'anon',
      visibility: 'hidden',
      heldLeadership: { leadershipId: 7, workerLockName: 'sp00ky-tabs:fp-test:anon:worker:7' },
    });
    await flush();
    // Despite being hidden (b is visible), the holder wins with resumeHeld.
    const grant = received.find((r) => r.msg.type === 'become-leader');
    expect(grant).toBeDefined();
    expect((grant!.msg as { resumeHeld: boolean }).resumeHeld).toBe(true);
  });
});

describe('tabs broker: namespaces', () => {
  it('separates tabs by bucket: one leader per bucket', async () => {
    const a = connectTab('tab-a', { bucket: 'user1' });
    await ackLeadership(a, 'user1');
    const b = connectTab('tab-b', { bucket: 'user2' });
    await flush();
    expect(b.ofType('become-leader').length).toBeGreaterThan(0);
  });

  it('re-elects the old namespace immediately when a leader re-hellos into a new bucket', async () => {
    const a = connectTab('tab-a');
    await ackLeadership(a);
    const b = connectTab('tab-b');
    await flush();
    // a switches buckets: same tabId, new bucket. b must be promoted in anon
    // without waiting for a pong eviction.
    a.send({
      type: 'hello',
      tabId: 'tab-a',
      fingerprint: 'fp-test',
      bucketId: 'user1',
      visibility: 'visible',
      heldLeadership: null,
    });
    await flush();
    expect(b.ofType('become-leader').length).toBeGreaterThan(0);
    // and a is offered leadership of the new bucket.
    expect(a.ofType('become-leader').length).toBeGreaterThan(1);
  });

  it('keeps the port of a leader whose released tab lock means it left the bucket, not that it died', async () => {
    // The broker's crash detector queues a request on the leader's tab lock;
    // here it is granted the moment the tab releases it, as on sign-in.
    const grants: Array<() => void> = [];
    vi.stubGlobal('navigator', {
      locks: {
        request: (_name: string, opts: { ifAvailable?: boolean; steal?: boolean }, cb: (lock: unknown) => unknown) =>
          new Promise((resolve) => {
            if (opts.ifAvailable || opts.steal) resolve(cb({}));
            else grants.push(() => resolve(cb({})));
          }),
      },
    });
    try {
      const a = connectTab('tab-a');
      await ackLeadership(a);
      expect(grants).toHaveLength(1);
      grants[0]();
      await flush();
      a.send({
        type: 'hello',
        tabId: 'tab-a',
        fingerprint: 'fp-test',
        bucketId: 'user1',
        visibility: 'visible',
        heldLeadership: null,
      });
      await flush();
      expect(a.ofType('become-leader').map((m) => m.leadershipId)).toEqual([1, 2]);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('elects the only tab in its new bucket when it signs in alone', async () => {
    const a = connectTab('tab-a');
    await ackLeadership(a);
    a.send({
      type: 'hello',
      tabId: 'tab-a',
      fingerprint: 'fp-test',
      bucketId: 'user1',
      visibility: 'visible',
      heldLeadership: null,
    });
    await flush();
    expect(a.ofType('become-leader').map((m) => m.leadershipId)).toEqual([1, 2]);
    await ackLeadership(a, 'user1');
    // Still pinged in the new bucket, so the client's watchdog stays quiet.
    const pings = a.ofType('ping').length;
    await vi.advanceTimersByTimeAsync(10_000);
    expect(a.ofType('ping').length).toBeGreaterThan(pings);
  });
});
