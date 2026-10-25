import type { EdgeDelta, QueryHash, RecordVersionArray, ServerViewMeta } from '../types';
import type { Saga } from '../kernel/saga';
import type { Settled, StatementResult } from '../kernel/effects';
import { fx } from '../kernel/effects';
import {
  MATERIALIZING_REREAD_LADDER_MS,
  MEMBERSHIP_COALESCE_MS,
  MEMBERSHIP_LEADING_MS,
  VIEW_LOST_RETRY_BASE_MS,
  VIEW_LOST_RETRY_MAX_MS,
  backoffMs,
} from '../kernel/constants';
import type { ClientState, QueryEntry } from '../state/client-state';
import * as R from '../state/reducers';
import { encodeRecordId } from '../utils/index';
import { departedIds } from '../state/selectors';
import { forgetDeparted } from './lifecycle.saga';
import { planListRefPollChunks, recordVersionArraysEqual } from '../sync/policy';
import type { SagaEnv } from './env';
import { listRefTable } from './env';
import * as sql from './sql';
import {
  FAILED,
  decideMembershipOutcome,
  rebaseRead,
  withDeltas,
  snapshotFromSingle,
  snapshotsFromBatch,
  suspectHashes,
  type ListRefSnapshot,
} from './membership';

type MembershipOutcome = 'applied' | 'ignored' | 'view-lost';

/**
 * Accept (or refuse) a server id-set for one query. The only writer of
 * `remoteArray`, the `_00_view` row and the cold -> live transition.
 */
export function* applyMembership(
  hash: QueryHash,
  remoteArray: RecordVersionArray,
  meta?: ServerViewMeta,
  verifiedRemoval?: boolean
): Saga<MembershipOutcome> {
  const entry = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
  if (!entry) return 'ignored';
  if (meta) yield fx.state.update(R.setServerState(hash, meta.present ? meta.state : null));
  // The row is there: whatever made it read as gone has passed.
  if (meta?.present && (entry.viewLostCount > 0 || entry.viewLostRetryAt !== null)) {
    yield fx.state.update(R.setViewLost(hash, 0, null));
  }
  const outcome = decideMembershipOutcome({
    phase: entry.lifecycle.phase,
    held: entry.remoteArray.length,
    remoteArray,
    meta,
    verifiedRemoval,
  });
  if (outcome === 'ignored') return outcome;
  if (outcome === 'view-lost') {
    if (entry.lifecycle.phase !== 'view-lost') {
      yield fx.state.update(R.applyLifecycle(hash, { type: 'row-missing' }));
      yield fx.emit({ type: 'query:view-lost', hash });
    }
    // A recovery is already scheduled: every read in the meantime would
    // otherwise re-register again.
    if (entry.viewLostRetryAt !== null) return outcome;
    if (entry.viewLostCount === 0) {
      yield fx.state.update(R.setViewLost(hash, 1, null));
      if (entry.lifecycle.remote !== 'unregistered') {
        yield fx.state.update(R.applyLifecycle(hash, { type: 'remote-dropped' }));
      }
      yield fx.dispatch({ type: 'EnsureRegistered' });
      return outcome;
    }
    // Re-registered already and the row still reads as gone. Under load that
    // repeated every read, each one costing the server a registration while it
    // was still publishing the last, so from here on it is paced.
    const delay = backoffMs(entry.viewLostCount - 1, VIEW_LOST_RETRY_BASE_MS, VIEW_LOST_RETRY_MAX_MS);
    const now = (yield fx.now()) as number;
    yield fx.state.update(R.setViewLost(hash, entry.viewLostCount + 1, now + delay));
    yield fx.emit({
      type: 'log',
      level: 'debug',
      message: 'view still lost; re-registering after a delay',
      data: { hash, attempt: entry.viewLostCount + 1, delayMs: delay },
    });
    yield fx.timer.set(`view-lost:${hash}`, delay, { type: 'RecoverLostView', hash });
    return outcome;
  }
  yield fx.state.update(R.commitMembership(hash, remoteArray, meta?.present ?? true));
  yield* forgetDeparted(hash, departedIds(entry.remoteArray, remoteArray));
  // No authority flip here: the set is known but its rows are not painted
  // yet (bodies may still be on their way). `materialize` flips it once they are.
  const now = (yield fx.now()) as number;
  try {
    yield fx.local.upsert(
      sql.VIEW_TABLE,
      sql.viewRecordId(entry.def.viewKey),
      sql.viewRow(remoteArray, entry.subqueryRemoteArray, true, now),
      'replace'
    );
  } catch (err) {
    yield fx.emit({ type: 'log', level: 'debug', message: 'view row write failed', data: { hash, err } });
  }
  yield fx.dispatch({ type: 'FetchRows' });
  return outcome;
}

/**
 * The paced view-lost re-registration. A no-op when the row has been seen
 * since (the pacing was reset) or the query recovered or went away.
 */
export function* recoverLostView(hash: QueryHash): Saga<void> {
  const entry = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
  if (!entry || entry.viewLostRetryAt === null) return;
  yield fx.state.update(R.setViewLost(hash, entry.viewLostCount, null));
  if (entry.lifecycle.phase !== 'view-lost') return;
  if (entry.lifecycle.remote !== 'unregistered') {
    yield fx.state.update(R.applyLifecycle(hash, { type: 'remote-dropped' }));
  }
  yield fx.dispatch({ type: 'RegisterRemote', hash });
}

/**
 * Replace the subquery child set; bodies follow through the fetch plan. A
 * query with a `_00_view` row (past `cold`) records the children there too,
 * so the GC keeps their bodies and a cold scan may paint them.
 */
export function* applySubqueryChildren(hash: QueryHash, children: RecordVersionArray): Saga<void> {
  const entry = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
  if (!entry || recordVersionArraysEqual(entry.subqueryRemoteArray, children)) return;
  yield fx.state.update(R.setSubqueryRemoteArray(hash, children));
  yield* forgetDeparted(hash, departedIds(entry.subqueryRemoteArray, children));
  if (entry.lifecycle.phase !== 'cold') {
    try {
      yield fx.local.upsert(sql.VIEW_TABLE, sql.viewRecordId(entry.def.viewKey), { children }, 'merge');
    } catch (err) {
      yield fx.emit({ type: 'log', level: 'debug', message: 'view row write failed', data: { hash, err } });
    }
  }
  yield fx.dispatch({ type: 'FetchRows' });
}

const ok = (r: Settled): r is { ok: true; value: unknown } => r.ok;
/** One statement's result, or `FAILED` when it did not answer (see `FAILED`). */
const stmt = (results: unknown, i: number): unknown => {
  if (!Array.isArray(results)) return FAILED;
  const r = (results as StatementResult[])[i];
  return r && r.status === 'OK' ? r.result : FAILED;
};

/**
 * Commit LIVE edge deltas (rows joining a view, or moving to a newer
 * version) straight to membership, without re-reading the edges: the
 * notification already says what the read would. Returns the hashes it
 * settled; any other change still goes through {@link readMembership}.
 *
 * Only for a query whose membership is known and current (`live`, not
 * materializing, no view-lost recovery pending): before that the read decides
 * what the set is. Removals are never applied here, so a view whose edges
 * are being rewritten never loses (and garbage-collects) bodies mid-rewrite.
 * A query with a `LIMIT` takes it only when every delta is a version bump of
 * a row it already holds. A row JOINING it pushes another out, and rendering
 * the joiner before the read removes the leaver would show one row too many
 * (the id-set render drops the `LIMIT`); a bump cannot change the count. A row
 * that an edit moves out of the window arrives as its own `DELETE`, which still
 * marks the query dirty. Windowed lists are most lists, so this spares the
 * coalesce window and a full round trip on the common case of an edit to a
 * row already on screen.
 */
export function* applyEdgeDeltas(deltas: EdgeDelta[]): Saga<Set<QueryHash>> {
  const settled = new Set<QueryHash>();
  const byHash = new Map<QueryHash, EdgeDelta[]>();
  for (const d of deltas) byHash.set(d.hash, [...(byHash.get(d.hash) ?? []), d]);
  for (const [hash, list] of byHash) {
    const entry = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
    if (!entry || entry.lifecycle.phase !== 'live' || entry.serverState === 'materializing' || entry.viewLostRetryAt !== null) continue;
    const limited = entry.def.plan ? entry.def.plan.limit != null : /\bLIMIT\b/i.test(entry.def.surql);
    if (limited) {
      const held = new Set(entry.remoteArray.map(([id]) => id));
      if (!list.every((d) => held.has(d.id))) continue;
    }
    const next = withDeltas(entry.remoteArray, list);
    // Never empty, so always applied.
    if (next !== entry.remoteArray) yield* applyMembership(hash, next);
    settled.add(hash);
  }
  return settled;
}

/**
 * Read the server membership of many queries in as few round trips as the
 * row budget allows, then apply each changed set. Every server id-set enters
 * state here, except the rows LIVE reports joining (see
 * {@link applyEdgeDeltas}): registration, the poll, LIVE dirt and view-lost
 * recovery all come through here.
 */
export function* readMembership(
  env: SagaEnv,
  hashes: QueryHash[],
  opts: { force?: boolean } = {}
): Saga<{ changed: boolean; failed: boolean }> {
  const state = (yield fx.state.read((s) => s)) as ClientState;
  const entries = hashes.map((h) => state.queries.get(h)).filter((e): e is QueryEntry => !!e);
  if (entries.length === 0) return { changed: false, failed: false };
  const table = listRefTable(env, state);
  const now = (yield fx.now()) as number;
  const chunks = planListRefPollChunks(
    entries.map((e) => ({ hash: e.def.hash, rows: e.remoteArray.length, lastPolledAt: opts.force ? 0 : (e.lastPolledAt ?? 0) })),
    { now }
  );
  const byHash = new Map(entries.map((e) => [e.def.hash, e]));
  const single = (e: QueryEntry) =>
    fx.remote.query(sql.singleSnapshotSelect(table), { in: e.def.id }, env.remoteTimeoutMs);
  const requests = chunks.map((chunk) =>
    chunk.length === 1
      ? single(byHash.get(chunk[0])!)
      : fx.remote.query(sql.batchSnapshotSelect(table), { ins: chunk.map((h) => byHash.get(h)!.def.id) }, env.remoteTimeoutMs)
  );
  const results = (yield fx.all(requests)) as Settled[];

  const snapshots = new Map<QueryHash, ListRefSnapshot>();
  let failed = false;
  const rereads: QueryEntry[] = [];
  chunks.forEach((chunk, i) => {
    const r = results[i];
    if (!ok(r)) {
      failed = true;
      return;
    }
    if (chunk.length === 1) {
      const snap = snapshotFromSingle(stmt(r.value, 0) as never, stmt(r.value, 1) as never, stmt(r.value, 2) as never);
      if (snap) snapshots.set(chunk[0], snap);
      else failed = true;
      return;
    }
    const hashById = new Map(chunk.map((h) => [encodeRecordId(byHash.get(h)!.def.id), h]));
    const edges = stmt(r.value, 0) as never[] | null;
    const counts = stmt(r.value, 1);
    if (!Array.isArray(edges) || counts === FAILED) {
      failed = true;
      return;
    }
    const batch = snapshotsFromBatch(edges, counts as never, hashById);
    const held = new Map(chunk.map((h) => [h, byHash.get(h)!.remoteArray.length]));
    const suspect = new Set(suspectHashes(batch, held, edges.length));
    for (const [h, snap] of batch) {
      if (suspect.has(h)) rereads.push(byHash.get(h)!);
      else snapshots.set(h, snap);
    }
  });
  if (rereads.length > 0) {
    const again = (yield fx.all(rereads.map(single))) as Settled[];
    again.forEach((r, i) => {
      if (!ok(r)) {
        failed = true;
        return;
      }
      const snap = snapshotFromSingle(stmt(r.value, 0) as never, stmt(r.value, 1) as never, stmt(r.value, 2) as never);
      if (snap) snapshots.set(rereads[i].def.hash, snap);
      else failed = true;
    });
  }

  let changed = false;
  const stillMaterializing: QueryHash[] = [];
  for (const [hash, snap] of snapshots) {
    const current = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
    if (!current) continue;
    const primary = rebaseRead(snap.primary, byHash.get(hash)!.remoteArray, current.remoteArray);
    const metaChanged = current.serverState !== (snap.meta.present ? snap.meta.state : null);
    if (!recordVersionArraysEqual(primary, current.remoteArray) || metaChanged || current.lifecycle.phase === 'cold' || current.lifecycle.phase === 'cached') {
      const outcome = yield* applyMembership(hash, primary, snap.meta);
      if (outcome === 'applied') changed = true;
      if (outcome === 'ignored' && snap.meta.present && snap.meta.state === 'materializing') stillMaterializing.push(hash);
    }
    yield* applySubqueryChildren(hash, snap.subquery);
  }
  yield fx.state.update(R.compose(R.clearMembershipDirty([...snapshots.keys()]), R.stampPolled(snapshots.keys(), now)));

  // A view whose edges are still in flight: re-read on a short ladder rather
  // than waiting for a backed-off poll tick.
  for (const hash of stillMaterializing) {
    const attempt = (yield fx.state.read((s) => s.membershipReread.get(hash) ?? 0)) as number;
    if (attempt >= MATERIALIZING_REREAD_LADDER_MS.length) {
      yield fx.state.update(R.setMembershipReread(hash, null));
      continue;
    }
    yield fx.state.update(R.compose(R.setMembershipReread(hash, attempt + 1), R.markMembershipDirty([hash])));
    yield fx.timer.set('membership', MATERIALIZING_REREAD_LADDER_MS[attempt], { type: 'ReadDirtyMembership' });
  }
  for (const hash of snapshots.keys()) {
    if (!stillMaterializing.includes(hash)) yield fx.state.update(R.setMembershipReread(hash, null));
  }
  yield fx.dispatch({ type: 'SyncOutcome', ok: !failed, error: failed ? 'membership read failed' : undefined });
  return { changed, failed };
}

/**
 * LIVE handler: mark dirty and coalesce into one batched re-read.
 *
 * The window is armed ONCE per burst, when the dirty set goes from empty to
 * non-empty. `timer.set` replaces a pending timer, so re-arming on every event
 * would push the read out for as long as events keep arriving: on a busy
 * `_00_list_ref` table (registrations, an import, another tab syncing) the
 * events never stop for a whole 50 ms and the read simply never ran, leaving
 * membership frozen until a poll tick happened to cover it.
 *
 * Leading edge: when none of these queries was read within the last
 * {@link MEMBERSHIP_COALESCE_MS}, the read goes out after the short
 * {@link MEMBERSHIP_LEADING_MS} instead of waiting the whole window. A lone
 * remote edit is the common case, and the full window sat in front of every
 * one of them. Dirt arriving while that read is in flight still gets its own
 * trailing read from {@link readDirtyMembership}, so the cap of one read per
 * window holds.
 */
export function* markMembershipDirty(hashes: QueryHash[]): Saga<void> {
  const [armed, lastRead] = (yield fx.state.read((s) => [
    s.membershipDirty.size > 0,
    hashes.reduce((at, h) => Math.max(at, s.queries.get(h)?.lastPolledAt ?? 0), 0),
  ])) as [boolean, number];
  yield fx.state.update(R.markMembershipDirty(hashes));
  if (armed) return;
  const now = (yield fx.now()) as number;
  const delay = Math.max(MEMBERSHIP_LEADING_MS, lastRead + MEMBERSHIP_COALESCE_MS - now);
  yield fx.timer.set('membership', delay, { type: 'ReadDirtyMembership' });
}

export function* readDirtyMembership(env: SagaEnv): Saga<void> {
  const dirty = (yield fx.state.read((s) => [...s.membershipDirty])) as QueryHash[];
  if (dirty.length === 0) return;
  yield* readMembership(env, dirty, { force: true });
  // Dirt that arrived while the read was in flight, or a hash the read could
  // not answer, holds the set non-empty - and with the window armed only on the
  // empty -> non-empty edge, nothing else would arm it again.
  const left = (yield fx.state.read((s) => s.membershipDirty.size)) as number;
  if (left > 0) yield fx.timer.set('membership', MEMBERSHIP_COALESCE_MS, { type: 'ReadDirtyMembership' });
}
