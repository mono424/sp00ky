import type { Saga } from '../kernel/saga';
import type { Settled, StatementResult } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { ACK_GRACE_MS, GC_CHUNK, GC_INTERVAL_MS, TTL_HEARTBEAT_FRACTION, VIEW_RETENTION_MS } from '../kernel/constants';
import type { IngestRecord } from '../services/stream-processor/index';
import type { ClientState, QueryEntry, TabRole } from '../state/client-state';
import * as R from '../state/reducers';
import { evictable, retained, shortestTtlMs } from '../state/selectors';
import type { SagaEnv } from './env';
import { parseViewIndex, type ViewIndexRow } from './membership';
import * as sql from './sql';

/**
 * One tick for every query in state: evict the ones nobody has watched for a
 * ttl, heartbeat the rest in one request, notice reclaimed rows. Reschedules
 * itself at half the shortest ttl.
 */
export function* lifecycleTick(env: SagaEnv): Saga<void> {
  const now = (yield fx.now()) as number;
  const state = (yield fx.state.read((s) => s)) as ClientState;
  for (const hash of evictable(state, now)) yield* evictQuery(hash);
  const remaining = (yield fx.state.read((s) => [...s.queries.values()].filter((e) => e.lifecycle.remote === 'registered'))) as QueryEntry[];
  if (remaining.length > 0) {
    const beat = sql.heartbeatBatch(remaining.map((e) => e.def.id));
    try {
      const results = (yield fx.remote.query(beat.sql, beat.vars, env.remoteTimeoutMs)) as StatementResult[];
      let reclaimed = 0;
      for (let i = 0; i < remaining.length; i++) {
        const r = results[i];
        if (r?.status === 'OK' && sql.heartbeatRowGone(r.result)) {
          reclaimed++;
          yield fx.state.update(R.applyLifecycle(remaining[i].def.hash, { type: 'remote-dropped' }));
        }
      }
      yield fx.state.update(R.stampHeartbeat(remaining.map((e) => e.def.hash), now));
      if (reclaimed > 0) {
        yield fx.emit({ type: 'log', level: 'warn', message: 'query rows reclaimed by the server; re-registering', data: { reclaimed } });
        yield fx.dispatch({ type: 'EnsureRegistered' });
      }
      yield fx.dispatch({ type: 'SyncOutcome', ok: true });
    } catch (error) {
      yield fx.dispatch({ type: 'SyncOutcome', ok: false, error });
    }
  }
  const ttl = (yield fx.state.read((s) => shortestTtlMs(s))) as number | null;
  yield fx.timer.set('lifecycle', Math.floor((ttl ?? env.defaultTtlMs) * TTL_HEARTBEAT_FRACTION), { type: 'LifecycleTick' });
}

/** Free a query's local view and forget it. The server row expires by TTL. */
export function* evictQuery(hash: string): Saga<void> {
  const exists = (yield fx.state.read((s) => s.queries.has(hash))) as boolean;
  if (!exists) return;
  try {
    yield fx.ssp.unregister(hash);
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'debug', message: 'unregister failed', data: { hash, error } });
  }
  yield fx.state.update(R.removeQuery(hash));
  yield fx.emit({ type: 'query:evicted', hash });
}

/** Drop acked outbox items membership never named within the grace window. */
export function* ackPrune(): Saga<void> {
  const now = (yield fx.now()) as number;
  yield fx.state.update(R.outboxPruneAcked(now, ACK_GRACE_MS));
  const stillAcked = (yield fx.state.read((s) => s.outbox.some((i) => i.status === 'acked'))) as boolean;
  if (stillAcked) yield fx.timer.set('ack-prune', ACK_GRACE_MS, { type: 'AckPrune' });
}

/** Every `_00_view` row, parsed. Throws when the read fails. */
function* readViews(): Saga<ViewIndexRow[]> {
  const res = (yield fx.local.query(sql.readViewRows())) as unknown[];
  return parseViewIndex(Array.isArray(res) ? res[0] : undefined);
}

const indexOf = (rows: ViewIndexRow[]): Map<string, string[]> => new Map(rows.map((r) => [r.key, r.ids]));

/**
 * Load the durable view index (boot, bucket switch). A failed read leaves it
 * empty, which fails closed: cold scans paint nothing until the server answers.
 */
export function* loadViews(): Saga<void> {
  try {
    yield fx.state.update(R.reloadViews(indexOf(yield* readViews())));
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'view index read failed', data: { error } });
  }
}

const tableOf = (id: string): string => id.slice(0, id.indexOf(':'));

/**
 * Orphan collection, `GC_BOOT_DELAY_MS` after boot and then every
 * `GC_INTERVAL_MS`. Re-reads every `_00_view` row into the index, retires the
 * rows no query has resolved for `VIEW_RETENTION_MS`, then deletes the bodies
 * nothing retains (see `retained`) from the store and the circuit, a chunk at
 * a time. Only a leader or solo tab deletes, and a leader relays the deletes
 * so followers drop the rows too; a follower only refreshes its index.
 */
export function* gcTick(): Saga<void> {
  yield fx.state.wait((s) => s.primed);
  try {
    const rows = yield* readViews();
    const [role, bucket, held] = (yield fx.state.read((s) => [
      s.tabRole,
      s.bucketId,
      new Set([...s.queries.values()].map((e) => e.def.viewKey)),
    ])) as [TabRole, string | null, Set<string>];
    const now = (yield fx.now()) as number;
    const expired = role === 'follower' ? [] : rows.filter((r) => !held.has(r.key) && now - r.updatedAt >= VIEW_RETENTION_MS);
    const retired = new Set<string>();
    for (let i = 0; i < expired.length; i += GC_CHUNK) {
      const chunk = expired.slice(i, i + GC_CHUNK);
      const settled = (yield fx.all(chunk.map((r) => fx.local.delete(sql.VIEW_TABLE, sql.viewRecordId(r.key))))) as Settled[];
      chunk.forEach((r, j) => settled[j].ok && retired.add(r.key));
    }
    yield fx.state.update(R.reloadViews(indexOf(rows.filter((r) => !retired.has(r.key)))));
    if (role === 'follower') return;
    const removed = yield* collectOrphans(bucket, role === 'leader');
    yield fx.emit({ type: 'log', level: 'info', message: 'orphan gc done', data: { removed, retiredViews: retired.size } });
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'orphan gc failed', data: { error } });
  } finally {
    yield fx.timer.set('gc', GC_INTERVAL_MS, { type: 'GcTick' });
  }
}

/**
 * Delete every body nothing retains, `GC_CHUNK` at a time. Each chunk is
 * re-checked against fresh state (a membership committed meanwhile may name
 * an id again) and the sweep stops if the bucket moved under it.
 */
function* collectOrphans(bucket: string | null, relay: boolean): Saga<number> {
  const candidates = (yield fx.state.read((s) => {
    const keep = retained(s);
    return [...s.versions.keys()].filter((id) => !keep(id));
  })) as string[];
  let removed = 0;
  for (let i = 0; i < candidates.length; i += GC_CHUNK) {
    const chunk = (yield fx.state.read((s) => {
      if (!s.primed || s.bucketId !== bucket) return null;
      const keep = retained(s);
      return candidates.slice(i, i + GC_CHUNK).filter((id) => s.versions.has(id) && !keep(id));
    })) as string[] | null;
    if (chunk === null) break;
    const settled = (yield fx.all(chunk.map((id) => fx.local.delete(tableOf(id), id)))) as Settled[];
    const done = chunk.filter((_, j) => settled[j].ok);
    if (done.length === 0) continue;
    const records: IngestRecord[] = done.map((id) => ({ table: tableOf(id), op: 'DELETE', id, record: {} }));
    yield fx.state.update(R.deleteVersions(done));
    yield fx.ssp.ingest(records);
    if (relay) yield fx.emit({ type: 'tabs:broadcast', message: { type: 'ingest', records } });
    removed += done.length;
  }
  // A query may have committed a deleted id between its chunk's check and the
  // delete; with its version gone, the fetch plan pulls it back.
  if (removed > 0) yield fx.dispatch({ type: 'FetchRows' });
  return removed;
}
