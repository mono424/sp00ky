import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { handleConnect, __resetBrokerForTests } from './tabs-broker-worker';
import { fakeChannel, flush, installBrokerGlobals, installFakeLocks } from './fake-ports.fixture';
import {
  TabsCoordinator,
  SyncForwarder,
  type CoordinatorHooks,
  type LeaderSyncHub,
} from './coordinator';
import type { StorageHealth } from '../../types';
import type { LeaderToFollowerMessage } from './protocol';

function makeLogger(): any {
  const noop = () => {};
  const l: any = { debug: noop, info: noop, warn: noop, error: noop, trace: noop };
  l.child = () => l;
  return l;
}

interface HookLog {
  adoptOwner: { bucketId: string; workerLockName: string; resumeHeld: boolean }[];
  adoptAttached: { bucketId: string; storageHealth: StorageHealth }[];
  released: number;
  leaderLost: string[];
  exposedPorts: string[];
  hub: LeaderSyncHub | null;
  forwarder: SyncForwarder | null;
  leaderDuties: number;
}

function makeHooks(): { hooks: CoordinatorHooks; log: HookLog } {
  const log: HookLog = {
    adoptOwner: [],
    adoptAttached: [],
    released: 0,
    leaderLost: [],
    exposedPorts: [],
    hub: null,
    forwarder: null,
    leaderDuties: 0,
  };
  const hooks: CoordinatorHooks = {
    async adoptOwner(bucketId, opts) {
      log.adoptOwner.push({ bucketId, workerLockName: opts.workerLockName, resumeHeld: opts.resumeHeld });
      return { status: 'persistent', fallback: false, role: 'leader' };
    },
    async adoptAttached(_dbPort, snapshot) {
      log.adoptAttached.push({ bucketId: snapshot.bucketId, storageHealth: snapshot.storageHealth });
    },
    async releaseOwnership() {
      log.released++;
    },
    onLeaderLost(reason) {
      log.leaderLost.push(reason);
    },
    async exposeClientPort(clientId) {
      log.exposedPorts.push(clientId);
    },
    async removeClientPort() {},
    becomeSyncLeader(hub) {
      log.hub = hub;
    },
    async resumeSyncLeaderDuties() {
      log.leaderDuties++;
    },
    becomeSyncFollower(forwarder) {
      log.forwarder = forwarder;
    },
    becomeSyncSolo() {},
    currentStorageHealth: () => ({ status: 'persistent', fallback: false, role: 'leader' }),
  };
  return { hooks, log };
}

function makeCoordinator(tabId: string, overrides: Partial<CoordinatorHooks> = {}) {
  const { hooks, log } = makeHooks();
  const coordinator = new TabsCoordinator({
    tabId,
    fingerprint: 'fp-coord-test',
    hooks: { ...hooks, ...overrides },
    logger: makeLogger(),
  });
  return { coordinator, log };
}

let restoreGlobals: () => void;
beforeEach(() => {
  __resetBrokerForTests();
  restoreGlobals = installBrokerGlobals(handleConnect);
});
afterEach(() => {
  restoreGlobals();
  __resetBrokerForTests();
});

// End-to-end role assignment: two real coordinators, the real broker module,
// fake SharedWorker/MessageChannel plumbing (node has no SharedWorker).
describe('TabsCoordinator integration', () => {
  it('boots the first tab as leader with a per-leadership worker lock name', async () => {
    const { coordinator, log } = makeCoordinator('tab-1');
    const role = await coordinator.start('anon');
    expect(role).toBe('leader');
    expect(log.adoptOwner).toHaveLength(1);
    expect(log.adoptOwner[0].bucketId).toBe('anon');
    expect(log.adoptOwner[0].workerLockName).toMatch(/^sp00ky-tabs:fp-coord-test:anon:worker:\d+$/);
    expect(log.hub).not.toBeNull();
    await coordinator.stop();
  });

  it('attaches the second tab as follower with the leader-reported snapshot', async () => {
    const a = makeCoordinator('tab-1');
    await a.coordinator.start('anon');
    const b = makeCoordinator('tab-2');
    const role = await b.coordinator.start('anon');

    expect(role).toBe('follower');
    // Leader forwarded the follower's db port into its worker...
    expect(a.log.exposedPorts).toEqual(['tab-2']);
    // ...and the follower adopted the leader's store snapshot.
    expect(b.log.adoptAttached).toHaveLength(1);
    expect(b.log.adoptAttached[0].bucketId).toBe('anon');
    expect(b.log.adoptAttached[0].storageHealth.status).toBe('persistent');
    expect(b.log.forwarder).not.toBeNull();
    await b.coordinator.stop();
    await a.coordinator.stop();
  });

  it('relays hub messages to the follower forwarder (ingest fan-out)', async () => {
    const a = makeCoordinator('tab-1');
    await a.coordinator.start('anon');
    const b = makeCoordinator('tab-2');
    await b.coordinator.start('anon');

    const seen: LeaderToFollowerMessage[] = [];
    b.log.forwarder!.onLeaderMessage = (msg) => seen.push(msg);
    a.log.hub!.relayIngest([{ table: 'game', op: 'CREATE', id: 'game:1', record: { id: 'game:1' } }]);
    await new Promise((r) => setTimeout(r, 20));

    const relay = seen.filter((m) => m.type === 'ingest-relay');
    expect(relay).toHaveLength(1);
    expect((relay[0] as { seq: number }).seq).toBe(1);
    await b.coordinator.stop();
    await a.coordinator.stop();
  });

  it('forwards follower messages to the leader hub (mutation notify)', async () => {
    const a = makeCoordinator('tab-1');
    await a.coordinator.start('anon');
    const b = makeCoordinator('tab-2');
    await b.coordinator.start('anon');

    const seen: { tabId: string; msg: unknown }[] = [];
    a.log.hub!.onFollowerMessage = (tabId, msg) => seen.push({ tabId, msg });
    b.log.forwarder!.mutationEnqueued('_00_pending_mutations:0000000000123_0001_tab2');
    await new Promise((r) => setTimeout(r, 20));

    const notify = seen.filter((s) => (s.msg as { type: string }).type === 'mutation-enqueued');
    expect(notify).toHaveLength(1);
    expect(notify[0].tabId).toBe('tab-2');
    await b.coordinator.stop();
    await a.coordinator.stop();
  });

  // A follower's optimistic write must reach the leader (which ingests it and
  // fans it out) in one hop. Before this, only the mutation id was forwarded
  // and every other tab waited on the server round-trip + LIVE delivery.
  it('forwards a follower ingest to the leader hub tagged with the origin tab', async () => {
    const a = makeCoordinator('tab-1');
    await a.coordinator.start('anon');
    const b = makeCoordinator('tab-2');
    await b.coordinator.start('anon');

    const seen: { tabId: string; msg: unknown }[] = [];
    a.log.hub!.onFollowerMessage = (tabId, msg) => seen.push({ tabId, msg });
    const tuples = [{ table: 'game', op: 'CREATE' as const, id: 'game:9', record: { id: 'game:9' } }];
    b.log.forwarder!.ingest(tuples);
    await new Promise((r) => setTimeout(r, 20));

    const ingest = seen.filter((s) => (s.msg as { type: string }).type === 'ingest');
    expect(ingest).toHaveLength(1);
    expect(ingest[0].tabId).toBe('tab-2');
    expect((ingest[0].msg as { tuples: unknown }).tuples).toEqual(tuples);
    await b.coordinator.stop();
    await a.coordinator.stop();
  });

  it('fan-out with exceptTabId skips the origin follower and reaches the others', async () => {
    const a = makeCoordinator('tab-1');
    await a.coordinator.start('anon');
    const b = makeCoordinator('tab-2');
    await b.coordinator.start('anon');
    const c = makeCoordinator('tab-3');
    await c.coordinator.start('anon');

    const seenB: LeaderToFollowerMessage[] = [];
    const seenC: LeaderToFollowerMessage[] = [];
    b.log.forwarder!.onLeaderMessage = (msg) => seenB.push(msg);
    c.log.forwarder!.onLeaderMessage = (msg) => seenC.push(msg);
    a.log.hub!.relayIngest(
      [{ table: 'game', op: 'CREATE', id: 'game:9', record: { id: 'game:9' } }],
      'tab-2'
    );
    await new Promise((r) => setTimeout(r, 20));

    expect(seenB.filter((m) => m.type === 'ingest-relay')).toHaveLength(0);
    expect(seenC.filter((m) => m.type === 'ingest-relay')).toHaveLength(1);
    await c.coordinator.stop();
    await b.coordinator.stop();
    await a.coordinator.stop();
  });

  // Unlike a mutation notify, an ingest is NOT queued across a leaderless
  // window: the next leader primes its circuit from the shared store, which
  // already holds the row, and a stale replay would put an older `_00_rv` in
  // its version memo.
  it('drops an ingest while detached instead of queueing it', async () => {
    const forwarder = new SyncForwarder('tab-x');
    forwarder.ingest([{ table: 'game', op: 'CREATE', id: 'game:1', record: { id: 'game:1' } }]);

    const { port1, port2 } = fakeChannel();
    const seen: { type: string }[] = [];
    port2.onmessage = (ev) => {
      seen.push(ev.data as { type: string });
    };
    forwarder.rebind(port1 as unknown as MessagePort);
    await flush();

    expect(seen.map((m) => m.type)).toEqual(['sync-hello']);
    forwarder.unbind();
  });

  // Relay traffic that lands between `db-ready` and the sync hooks being
  // installed (the store adopt is async) used to hit a null handler on a
  // first attach and vanish.
  it('buffers leader messages that land before the follower hooks are installed', async () => {
    const a = makeCoordinator('tab-1');
    await a.coordinator.start('anon');

    let adoptStartedResolve!: () => void;
    const adoptStartedP = new Promise<void>((r) => {
      adoptStartedResolve = r;
    });
    let releaseAdopt!: () => void;
    const adoptGate = new Promise<void>((r) => {
      releaseAdopt = r;
    });
    const seen: LeaderToFollowerMessage[] = [];
    const b = makeCoordinator('tab-2', {
      async adoptAttached() {
        adoptStartedResolve();
        await adoptGate;
      },
      becomeSyncFollower(forwarder) {
        forwarder.onLeaderMessage = (msg) => seen.push(msg);
      },
    });
    const started = b.coordinator.start('anon');
    await adoptStartedP;
    a.log.hub!.broadcast({
      type: 'mutation-settled',
      mutationId: '_00_pending_mutations:1_0001_tab-1',
      recordId: 'game:1',
      eventType: 'create',
    });
    await new Promise((r) => setTimeout(r, 20));
    expect(seen).toHaveLength(0);

    releaseAdopt();
    await started;
    expect(seen.map((m) => m.type)).toEqual(['mutation-settled']);
    await b.coordinator.stop();
    await a.coordinator.stop();
  });

  it('promotes the follower when the leader stops (failover)', async () => {
    const a = makeCoordinator('tab-1');
    await a.coordinator.start('anon');
    const b = makeCoordinator('tab-2');
    await b.coordinator.start('anon');

    const roles: string[] = [];
    b.coordinator.onRoleChange((r) => roles.push(r));
    await a.coordinator.stop();
    // Election + promotion are async hops across the fake ports.
    await new Promise((r) => setTimeout(r, 100));

    expect(roles).toContain('leader');
    expect(b.log.adoptOwner).toHaveLength(1);
    // The new leadership got a HIGHER id, so a fresh worker lock name.
    expect(b.log.adoptOwner[0].workerLockName).not.toBe(a.log.adoptOwner[0].workerLockName);
    expect(b.log.hub).not.toBeNull();
    await b.coordinator.stop();
  });

  // A failed promotion must give the leader TAB lock back. The name is shared
  // per namespace, so a leaked one makes every later election fail with
  // 'leader tab lock unavailable': all tabs time out in start(), boot solo, and
  // then contend for the OPFS pool individually — one busy pool wedging the
  // whole app into the pre-shared-tabs behavior it exists to replace.
  describe('with Web Locks', () => {
    let locks: ReturnType<typeof installFakeLocks>;
    beforeEach(() => {
      locks = installFakeLocks();
    });
    afterEach(() => locks.restore());

    /** A tab whose store can never open. Resolves once it has failed a
     *  promotion, so tests don't wait out start()'s 15s timeout. */
    function makeDoomedTab(tabId: string) {
      let failed!: () => void;
      const hasFailed = new Promise<void>((r) => {
        failed = r;
      });
      const tab = makeCoordinator(tabId, {
        async adoptOwner() {
          queueMicrotask(failed);
          throw new Error('opfs-unavailable: NoModificationAllowedError (after 10 attempts)');
        },
      });
      // start() only settles on a role or the timeout; neither is the point.
      void tab.coordinator.start('anon').catch(() => {});
      return { ...tab, hasFailed };
    }

    it('releases the leader tab lock when adoptOwner throws', async () => {
      const a = makeDoomedTab('tab-1');
      await a.hasFailed;
      // Well inside the 1s re-nomination backoff, so a held lock here is a leak
      // and not the next attempt's legitimate acquisition.
      await new Promise((r) => setTimeout(r, 50));
      expect(locks.heldNames()).not.toContain('sp00ky-tabs:fp-coord-test:anon:tab');
      await a.coordinator.stop();
    });

    it('lets a later tab lead after another tab failed to open the store', async () => {
      const a = makeDoomedTab('tab-1');
      await a.hasFailed;

      const b = makeCoordinator('tab-2');
      await expect(b.coordinator.start('anon')).resolves.toBe('leader');
      expect(b.log.adoptOwner).toHaveLength(1);
      await b.coordinator.stop();
      await a.coordinator.stop();
    });
  });

  it('moves buckets by leaving and rejoining: old namespace re-elects', async () => {
    const a = makeCoordinator('tab-1');
    await a.coordinator.start('anon');
    const b = makeCoordinator('tab-2');
    await b.coordinator.start('anon');

    const rolesB: string[] = [];
    b.coordinator.onRoleChange((r) => rolesB.push(r));
    const newRole = await a.coordinator.moveToBucket('user1');

    expect(newRole).toBe('leader');
    expect(a.log.adoptOwner.map((o) => o.bucketId)).toEqual(['anon', 'user1']);
    expect(a.log.released).toBeGreaterThan(0);
    await new Promise((r) => setTimeout(r, 100));
    expect(rolesB).toContain('leader');
    await b.coordinator.stop();
    await a.coordinator.stop();
  });

  it('signs in from an anonymous leader into a user bucket another tab already leads', async () => {
    const signedIn = makeCoordinator('signed-in', {
      // The real hook awaits a worker RPC. FakePort does not buffer messages
      // before onmessage is set, so let the joining tab bind its sync port.
      async exposeClientPort() { await flush(20); },
    });
    const signingIn = makeCoordinator('signing-in');
    try {
      await signedIn.coordinator.start('user1');
      await signingIn.coordinator.start('anon');

      await expect(signingIn.coordinator.moveToBucket('user1')).resolves.toBe('follower');
      expect(signingIn.log.adoptAttached.map((s) => s.bucketId)).toEqual(['user1']);
      expect(signingIn.log.released).toBe(1);
      expect(signingIn.coordinator.leaderTabId).toBe('signed-in');
    } finally {
      await signingIn.coordinator.stop();
      await signedIn.coordinator.stop();
    }
  }, 20_000);
});
