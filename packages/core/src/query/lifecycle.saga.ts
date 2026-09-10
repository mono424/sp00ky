import type { Saga } from '../kernel/saga';
import type { Settled, StatementResult } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { ACK_GRACE_MS, GC_INTERVAL_MS, TTL_HEARTBEAT_FRACTION } from '../kernel/constants';
import type { ClientState, QueryEntry } from '../state/client-state';
import * as R from '../state/reducers';
import { evictable, shortestTtlMs } from '../state/selectors';
import type { SagaEnv } from './env';
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

/**
 * Weekly orphan collection: bodies the store holds that no `_00_view` row
 * names and no outbox item touches are invisible; delete them in chunks.
 */
export function* gcTick(): Saga<void> {
  try {
    const views = (yield fx.local.query(`SELECT ids FROM ${sql.VIEW_TABLE}`)) as unknown[];
    const rows = Array.isArray(views?.[0]) ? (views[0] as Array<{ ids?: unknown }>) : [];
    const keep = new Set<string>();
    for (const row of rows) {
      if (!Array.isArray(row.ids)) continue;
      for (const pair of row.ids as Array<[string, number]>) keep.add(pair[0]);
    }
    const state = (yield fx.state.read((s) => s)) as ClientState;
    for (const item of state.outbox) keep.add(item.recordId);
    const orphans = [...state.versions.keys()].filter((id) => !keep.has(id) && !id.startsWith('_00_'));
    const deletes = orphans.map((id) => {
      const table = id.slice(0, id.indexOf(':'));
      return fx.local.delete(table, id);
    });
    for (let i = 0; i < deletes.length; i += 200) {
      const settled = (yield fx.all(deletes.slice(i, i + 200))) as Settled[];
      const done = orphans.slice(i, i + 200).filter((_, j) => settled[j].ok);
      yield fx.state.update(R.deleteVersions(done));
      if (done.length > 0) {
        yield fx.ssp.ingest(done.map((id) => ({ table: id.slice(0, id.indexOf(':')), op: 'DELETE', id, record: {} })));
      }
    }
    yield fx.emit({ type: 'log', level: 'info', message: 'orphan gc done', data: { removed: orphans.length } });
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'orphan gc failed', data: { error } });
  }
  yield fx.timer.set('gc', GC_INTERVAL_MS, { type: 'GcTick' });
}
