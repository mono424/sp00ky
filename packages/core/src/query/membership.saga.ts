import type { QueryHash, RecordVersionArray, ServerViewMeta } from '../types';
import type { Saga } from '../kernel/saga';
import type { Settled, StatementResult } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { MATERIALIZING_REREAD_LADDER_MS, MEMBERSHIP_COALESCE_MS } from '../kernel/constants';
import type { ClientState, QueryEntry } from '../state/client-state';
import { isAuthoritative } from '../state/lifecycle';
import * as R from '../state/reducers';
import { encodeRecordId } from '../utils/index';
import { planListRefPollChunks, recordVersionArraysEqual } from '../sync/policy';
import type { SagaEnv } from './env';
import { listRefTable } from './env';
import * as sql from './sql';
import {
  decideMembershipOutcome,
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
    if (entry.lifecycle.remote !== 'unregistered') {
      yield fx.state.update(R.applyLifecycle(hash, { type: 'remote-dropped' }));
    }
    yield fx.dispatch({ type: 'EnsureRegistered' });
    return outcome;
  }
  const wasAuthoritative = isAuthoritative(entry.lifecycle);
  yield fx.state.update(R.commitMembership(hash, remoteArray, meta?.present ?? true));
  if (!wasAuthoritative) yield fx.emit({ type: 'query:authority', hash, known: true });
  const now = (yield fx.now()) as number;
  try {
    yield fx.local.upsert(sql.VIEW_TABLE, sql.viewRecordId(entry.def.viewKey), sql.viewRow(remoteArray, true, now), 'replace');
  } catch (err) {
    yield fx.emit({ type: 'log', level: 'debug', message: 'view row write failed', data: { hash, err } });
  }
  yield fx.dispatch({ type: 'FetchRows' });
  return outcome;
}

/** Replace the subquery child set; bodies follow through the fetch plan. */
export function* applySubqueryChildren(hash: QueryHash, children: RecordVersionArray): Saga<void> {
  const entry = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
  if (!entry || recordVersionArraysEqual(entry.subqueryRemoteArray, children)) return;
  yield fx.state.update(R.setSubqueryRemoteArray(hash, children));
  yield fx.dispatch({ type: 'FetchRows' });
}

const ok = (r: Settled): r is { ok: true; value: unknown } => r.ok;
const stmt = (results: unknown, i: number): unknown => {
  if (!Array.isArray(results)) return null;
  const r = (results as StatementResult[])[i];
  return r && r.status === 'OK' ? r.result : null;
};

/**
 * Read the server membership of many queries in as few round trips as the
 * row budget allows, then apply each changed set. This is the ONLY place a
 * server id-set enters state: registration, the poll, LIVE dirt and view-lost
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
      return;
    }
    const hashById = new Map(chunk.map((h) => [encodeRecordId(byHash.get(h)!.def.id), h]));
    const edges = stmt(r.value, 0) as never[] | null;
    const batch = snapshotsFromBatch(edges, stmt(r.value, 1) as never, hashById);
    const held = new Map(chunk.map((h) => [h, byHash.get(h)!.remoteArray.length]));
    const suspect = new Set(suspectHashes(batch, held, Array.isArray(edges) ? edges.length : 0));
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
    });
  }

  let changed = false;
  const stillMaterializing: QueryHash[] = [];
  for (const [hash, snap] of snapshots) {
    const current = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
    if (!current) continue;
    const metaChanged = current.serverState !== (snap.meta.present ? snap.meta.state : null);
    if (!recordVersionArraysEqual(snap.primary, current.remoteArray) || metaChanged || current.lifecycle.phase === 'cold' || current.lifecycle.phase === 'cached') {
      const outcome = yield* applyMembership(hash, snap.primary, snap.meta);
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
 */
export function* markMembershipDirty(hashes: QueryHash[]): Saga<void> {
  const armed = (yield fx.state.read((s) => s.membershipDirty.size > 0)) as boolean;
  yield fx.state.update(R.markMembershipDirty(hashes));
  if (!armed) yield fx.timer.set('membership', MEMBERSHIP_COALESCE_MS, { type: 'ReadDirtyMembership' });
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
