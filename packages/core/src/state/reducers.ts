import type { ConnectionState, QueryHash, RecordVersionArray, SyncHealth } from '../types';
import type {
  ClientState,
  OutboxItem,
  PendingWrite,
  QueryEntry,
  QueryTelemetry,
  Row,
  ServerViewState,
  TabRole,
} from './client-state';
import type { LifecycleEvent } from './lifecycle';
import { transition } from './lifecycle';
import { TELEMETRY_SAMPLE_WINDOW } from '../kernel/constants';

/**
 * Pure state updates. Every reducer returns a new `ClientState`; sagas apply
 * them through `state.update`. Reducers that change a query's render inputs
 * add it to `dirty` themselves, which is what drives materialization.
 */
export type Reducer = (s: ClientState) => ClientState;

const addAll = <T>(set: ReadonlySet<T>, items: Iterable<T>): ReadonlySet<T> => {
  const next = new Set(set);
  for (const item of items) next.add(item);
  return next;
};

const withoutAll = <T>(set: ReadonlySet<T>, items: Iterable<T>): ReadonlySet<T> => {
  const next = new Set(set);
  for (const item of items) next.delete(item);
  return next;
};

function withEntry(s: ClientState, hash: QueryHash, fn: (e: QueryEntry) => QueryEntry): ClientState {
  const entry = s.queries.get(hash);
  if (!entry) return s;
  const next = fn(entry);
  if (next === entry) return s;
  const queries = new Map(s.queries);
  queries.set(hash, next);
  return { ...s, queries };
}

const hashesForTable = (s: ClientState, table: string): QueryHash[] => {
  const out: QueryHash[] = [];
  for (const [hash, entry] of s.queries) if (entry.def.tableName === table) out.push(hash);
  return out;
};

// ---- queries ---------------------------------------------------------------

export const putQuery =
  (entry: QueryEntry): Reducer =>
  (s) => {
    const queries = new Map(s.queries);
    queries.set(entry.def.hash, entry);
    return { ...s, queries, dirty: addAll(s.dirty, [entry.def.hash]) };
  };

export const removeQuery =
  (hash: QueryHash): Reducer =>
  (s) => {
    if (!s.queries.has(hash)) return s;
    const queries = new Map(s.queries);
    queries.delete(hash);
    const membershipReread = new Map(s.membershipReread);
    membershipReread.delete(hash);
    return {
      ...s,
      queries,
      membershipReread,
      dirty: withoutAll(s.dirty, [hash]),
      membershipDirty: withoutAll(s.membershipDirty, [hash]),
    };
  };

export const applyLifecycle =
  (hash: QueryHash, ev: LifecycleEvent): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => ({ ...e, lifecycle: transition(e.lifecycle, ev) }));

export const setServerState =
  (hash: QueryHash, serverState: ServerViewState): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => (e.serverState === serverState ? e : { ...e, serverState }));

/**
 * Accept a server membership set. Also releases every acked outbox item the
 * set names: membership has caught up with the write, the overlay's job is
 * done (invariant I7).
 */
export const commitMembership =
  (hash: QueryHash, remoteArray: RecordVersionArray, present: boolean): Reducer =>
  (s) => {
    const withArray = withEntry(s, hash, (e) => ({
      ...e,
      remoteArray,
      lifecycle: transition(e.lifecycle, { type: 'membership-applied', present }),
    }));
    if (withArray === s) return s;
    const named = new Set(remoteArray.map(([id]) => id));
    const outbox = withArray.outbox.filter((item) => !(item.status === 'acked' && named.has(item.recordId)));
    return { ...withArray, outbox, dirty: addAll(withArray.dirty, [hash]) };
  };

export const setLocalArray =
  (hash: QueryHash, localArray: RecordVersionArray): Reducer =>
  (s) => {
    const next = withEntry(s, hash, (e) => ({ ...e, localArray }));
    return next === s ? s : { ...next, dirty: addAll(next.dirty, [hash]) };
  };

export const setSubqueryRemoteArray =
  (hash: QueryHash, subqueryRemoteArray: RecordVersionArray): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => ({ ...e, subqueryRemoteArray }));

export const setRecords =
  (hash: QueryHash, records: ReadonlyArray<Row>, changed: boolean, materializeMs: number | null): Reducer =>
  (s) => {
    const next = withEntry(s, hash, (e) => {
      const prev = e.telemetry.phaseSamples.localFetch ?? [];
      const telemetry: QueryTelemetry = {
        ...e.telemetry,
        updateCount: changed ? e.telemetry.updateCount + 1 : e.telemetry.updateCount,
        phaseSamples:
          materializeMs === null
            ? e.telemetry.phaseSamples
            : { ...e.telemetry.phaseSamples, localFetch: [...prev, materializeMs].slice(-TELEMETRY_SAMPLE_WINDOW) },
        phaseLast: materializeMs === null ? e.telemetry.phaseLast : { ...e.telemetry.phaseLast, localFetch: materializeMs },
      };
      return {
        ...e,
        records: changed ? records : e.records,
        telemetry,
        lifecycle: transition(e.lifecycle, { type: 'notified' }),
      };
    });
    return next === s ? s : { ...next, dirty: withoutAll(next.dirty, [hash]) };
  };

export const stampUpdated =
  (hash: QueryHash, now: number): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => ({ ...e, telemetry: { ...e.telemetry, lastUpdatedAt: now } }));

export const recordPhase =
  (hash: QueryHash, phase: string, ms: number): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => {
      const prev = e.telemetry.phaseSamples[phase] ?? [];
      return {
        ...e,
        telemetry: {
          ...e.telemetry,
          phaseSamples: { ...e.telemetry.phaseSamples, [phase]: [...prev, ms].slice(-TELEMETRY_SAMPLE_WINDOW) },
          phaseLast: { ...e.telemetry.phaseLast, [phase]: ms },
        },
      };
    });

/** SSP ingest wall time for one update (the `ssp` phase). */
export const recordIngest =
  (hash: QueryHash, ms: number): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => ({
      ...e,
      telemetry: {
        ...e.telemetry,
        lastIngestLatencyMs: ms,
        materializationSamples: [...e.telemetry.materializationSamples, ms].slice(-TELEMETRY_SAMPLE_WINDOW),
      },
    }));

export const recordError =
  (hash: QueryHash): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => ({ ...e, telemetry: { ...e.telemetry, errorCount: e.telemetry.errorCount + 1 } }));

export const setRegistrationTimings =
  (hash: QueryHash, registrationTimings: QueryTelemetry['registrationTimings']): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => ({ ...e, telemetry: { ...e.telemetry, registrationTimings } }));

export const bumpRegisterAttempts =
  (hash: QueryHash): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => ({ ...e, registerAttempts: e.registerAttempts + 1 }));

export const resetRegisterAttempts =
  (hash: QueryHash): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => (e.registerAttempts === 0 ? e : { ...e, registerAttempts: 0 }));

export const stampHeartbeat =
  (hashes: Iterable<QueryHash>, now: number): Reducer =>
  (s) => {
    let next = s;
    for (const hash of hashes) next = withEntry(next, hash, (e) => ({ ...e, lastHeartbeatAt: now }));
    return next;
  };

export const beginRegistering =
  (hash: QueryHash): Reducer =>
  (s) => ({ ...s, registering: addAll(s.registering, [hash]) });

export const endRegistering =
  (hash: QueryHash): Reducer =>
  (s) =>
    s.registering.has(hash) ? { ...s, registering: withoutAll(s.registering, [hash]) } : s;

export const setMembershipReread =
  (hash: QueryHash, attempt: number | null): Reducer =>
  (s) => {
    const membershipReread = new Map(s.membershipReread);
    if (attempt === null) {
      if (!membershipReread.delete(hash)) return s;
    } else {
      membershipReread.set(hash, attempt);
    }
    return { ...s, membershipReread };
  };

export const stampPolled =
  (hashes: Iterable<QueryHash>, now: number): Reducer =>
  (s) => {
    let next = s;
    for (const hash of hashes) next = withEntry(next, hash, (e) => ({ ...e, lastPolledAt: now }));
    return next;
  };

// ---- subscribers -----------------------------------------------------------

export const subscribe =
  (hash: QueryHash): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => ({ ...e, subscribers: e.subscribers + 1, lastSubscriberLeftAt: null }));

export const unsubscribe =
  (hash: QueryHash, now: number): Reducer =>
  (s) =>
    withEntry(s, hash, (e) => {
      const subscribers = Math.max(0, e.subscribers - 1);
      return { ...e, subscribers, lastSubscriberLeftAt: subscribers === 0 ? now : e.lastSubscriberLeftAt };
    });

// ---- dirt ------------------------------------------------------------------

export const markDirty =
  (hashes: Iterable<QueryHash>): Reducer =>
  (s) => {
    const dirty = addAll(s.dirty, hashes);
    return dirty.size === s.dirty.size ? s : { ...s, dirty };
  };

export const markTableDirty =
  (table: string): Reducer =>
  (s) =>
    markDirty(hashesForTable(s, table))(s);

export const clearDirty =
  (hash: QueryHash): Reducer =>
  (s) =>
    s.dirty.has(hash) ? { ...s, dirty: withoutAll(s.dirty, [hash]) } : s;

export const markMembershipDirty =
  (hashes: Iterable<QueryHash>): Reducer =>
  (s) => {
    const live = [...hashes].filter((h) => s.queries.has(h));
    if (live.length === 0) return s;
    return { ...s, membershipDirty: addAll(s.membershipDirty, live) };
  };

export const clearMembershipDirty =
  (hashes: Iterable<QueryHash>): Reducer =>
  (s) => ({ ...s, membershipDirty: withoutAll(s.membershipDirty, hashes) });

// ---- versions --------------------------------------------------------------

/** Record local body versions; dirties every query whose membership names one of them. */
export const setVersions =
  (entries: ReadonlyArray<readonly [string, number]>): Reducer =>
  (s) => {
    if (entries.length === 0) return s;
    const versions = new Map(s.versions);
    const changed = new Set<string>();
    for (const [id, v] of entries) {
      if (versions.get(id) !== v) {
        versions.set(id, v);
        changed.add(id);
      }
    }
    if (changed.size === 0) return s;
    const dirty: QueryHash[] = [];
    for (const [hash, entry] of s.queries) {
      if (entry.remoteArray.some(([id]) => changed.has(id)) || entry.localArray.some(([id]) => changed.has(id))) {
        dirty.push(hash);
      }
    }
    return { ...s, versions, dirty: addAll(s.dirty, dirty) };
  };

export const deleteVersions =
  (ids: Iterable<string>): Reducer =>
  (s) => {
    const versions = new Map(s.versions);
    let touched = false;
    for (const id of ids) touched = versions.delete(id) || touched;
    return touched ? { ...s, versions } : s;
  };

// ---- outbox ----------------------------------------------------------------

export const outboxReplace =
  (items: ReadonlyArray<OutboxItem>): Reducer =>
  (s) => {
    const tables = new Set([...s.outbox, ...items].map((i) => i.table));
    let next: ClientState = { ...s, outbox: [...items] };
    for (const table of tables) next = markTableDirty(table)(next);
    return next;
  };

export const outboxPush =
  (item: OutboxItem): Reducer =>
  (s) =>
    markTableDirty(item.table)({ ...s, outbox: [...s.outbox, item] });

export const outboxAck =
  (id: string, now: number): Reducer =>
  (s) => {
    const idx = s.outbox.findIndex((i) => i.id === id);
    if (idx < 0) return s;
    const outbox = [...s.outbox];
    outbox[idx] = { ...outbox[idx], status: 'acked', ackedAt: now };
    return { ...s, outbox };
  };

export const outboxRemove =
  (id: string): Reducer =>
  (s) => {
    const item = s.outbox.find((i) => i.id === id);
    if (!item) return s;
    return markTableDirty(item.table)({ ...s, outbox: s.outbox.filter((i) => i.id !== id) });
  };

export const outboxBumpAttempts =
  (id: string): Reducer =>
  (s) => {
    const idx = s.outbox.findIndex((i) => i.id === id);
    if (idx < 0) return s;
    const outbox = [...s.outbox];
    outbox[idx] = { ...outbox[idx], attempts: outbox[idx].attempts + 1 };
    return { ...s, outbox };
  };

/** Drop acked items older than `graceMs` (membership never named them). */
export const outboxPruneAcked =
  (now: number, graceMs: number): Reducer =>
  (s) => {
    const stale = s.outbox.filter((i) => i.status === 'acked' && i.ackedAt !== null && now - i.ackedAt >= graceMs);
    if (stale.length === 0) return s;
    let next: ClientState = { ...s, outbox: s.outbox.filter((i) => !stale.includes(i)) };
    for (const table of new Set(stale.map((i) => i.table))) next = markTableDirty(table)(next);
    return next;
  };

/** Merge a debounced update into its pending write (first `before` wins). */
export const mergePendingWrite =
  (write: PendingWrite): Reducer =>
  (s) => {
    const prev = s.pendingWrites.get(write.key);
    const pendingWrites = new Map(s.pendingWrites);
    pendingWrites.set(
      write.key,
      prev ? { ...prev, data: { ...prev.data, ...write.data } } : write
    );
    return { ...s, pendingWrites };
  };

export const clearPendingWrite =
  (key: string): Reducer =>
  (s) => {
    if (!s.pendingWrites.has(key)) return s;
    const pendingWrites = new Map(s.pendingWrites);
    pendingWrites.delete(key);
    return { ...s, pendingWrites };
  };

export const setFailedCount =
  (failedCount: number): Reducer =>
  (s) =>
    s.failedCount === failedCount ? s : { ...s, failedCount };

// ---- identity / connection --------------------------------------------------

export const setIdentity =
  (
    patch: Partial<
      Pick<ClientState, 'sessionId' | 'userId' | 'saltUserId' | 'pendingBucket' | 'bucketId' | 'tabRole' | 'localReady' | 'primed'>
    >
  ): Reducer =>
  (s) => ({ ...s, ...patch });

/** Re-home a query after a bucket switch: new store, same hash, fresh sync state. */
export const rebindQuery =
  (
    hash: QueryHash,
    next: { id: QueryEntry['def']['id']; lifecycle: QueryEntry['lifecycle']; remoteArray: RecordVersionArray; localArray: RecordVersionArray }
  ): Reducer =>
  (s) => {
    const rebound = withEntry(s, hash, (e) => ({
      ...e,
      def: { ...e.def, id: next.id },
      lifecycle: next.lifecycle,
      remoteArray: next.remoteArray,
      localArray: next.localArray,
      subqueryRemoteArray: [],
      records: [],
      serverState: null,
      lastHeartbeatAt: null,
      lastPolledAt: null,
      registerAttempts: 0,
    }));
    return rebound === s ? s : { ...rebound, dirty: addAll(rebound.dirty, [hash]) };
  };

/** Everything a bucket switch invalidates in one go. */
export const clearBucketState = (): Reducer => (s) => ({
  ...s,
  versions: new Map(),
  outbox: [],
  pendingWrites: new Map(),
  dirty: new Set(),
  membershipDirty: new Set(),
  membershipReread: new Map(),
  primed: false,
});

export const setTabRole =
  (tabRole: TabRole): Reducer =>
  (s) =>
    s.tabRole === tabRole ? s : { ...s, tabRole };

export const setConnection =
  (connection: ConnectionState): Reducer =>
  (s) =>
    s.sync.health.connection === connection ? s : { ...s, sync: { ...s.sync, health: { ...s.sync.health, connection } } };

/**
 * Shallow value equality over {@link SyncHealth}. `nextHealth` rebuilds the
 * snapshot on every sync round, so identity alone would report a change each
 * time; the runtime uses identity to decide whether to notify subscribers, and
 * this is what keeps that identity stable while nothing actually moved.
 */
export const sameHealth = (a: SyncHealth, b: SyncHealth): boolean =>
  a.status === b.status &&
  a.consecutiveFailures === b.consecutiveFailures &&
  a.everConnected === b.everConnected &&
  a.connection === b.connection &&
  a.kind === b.kind &&
  a.error === b.error;

export const setHealth =
  (health: SyncHealth): Reducer =>
  (s) => (sameHealth(s.sync.health, health) ? s : { ...s, sync: { ...s.sync, health } });

export const patchSync =
  (patch: Partial<Omit<ClientState['sync'], 'health'>>): Reducer =>
  (s) => ({ ...s, sync: { ...s.sync, ...patch } });

/** Compose reducers left to right. */
export const compose =
  (...reducers: Reducer[]): Reducer =>
  (s) =>
    reducers.reduce((acc, r) => r(acc), s);
