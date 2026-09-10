import type { RecordId } from 'surrealdb';
import type { Saga } from '../kernel/saga';
import type { Settled, StatementResult } from '../kernel/effects';
import { fx } from '../kernel/effects';
import { backoffMs } from '../kernel/constants';
import type { IngestRecord } from '../services/stream-processor/index';
import type { ClientState } from '../state/client-state';
import * as R from '../state/reducers';
import { planFetch } from '../state/selectors';
import { encodeRecordId, parseRecordIdString } from '../utils/index';
import { cleanRecord } from '../utils/parser';
import type { SagaEnv } from './env';
import * as sql from './sql';

/**
 * The unified row fetcher (the poke analogue). Runs on the serial `fetch`
 * lane: computes the cross-query set of bodies missing or stale locally,
 * pulls them in parallel chunks, writes + ingests them, and loops until the
 * plan is empty. A row shared by N queries is fetched once. Bodies are never
 * deleted here; ids that left membership simply stop rendering.
 */
export function* fetchRows(env: SagaEnv): Saga<void> {
  yield fx.state.wait((s) => s.primed);
  for (;;) {
    const state = (yield fx.state.read((s) => s)) as ClientState;
    const plan = planFetch(state);
    if (plan.chunks.length === 0) {
      if (state.sync.fetchAttempts !== 0) yield fx.state.update(R.patchSync({ fetchAttempts: 0 }));
      return;
    }
    // The engine's epoch, not a mirrored counter: it also moves on transport
    // failover, and a stale value makes the store drop every body write.
    const epoch = (yield fx.local.epoch()) as number;
    yield fx.state.update(R.compose(...plan.hashes.map((h) => R.applyLifecycle(h, { type: 'fetch-begin' }))));
    let failed = 0;
    try {
      const results = (yield fx.all(
        plan.chunks.map((ids) => fx.remote.query(sql.bodySelect(), { ids: ids.map(parseRecordIdString) }, env.remoteTimeoutMs))
      )) as Settled[];
      for (let i = 0; i < results.length; i++) {
        const r = results[i];
        const first = r.ok ? (r.value as StatementResult[])[0] : undefined;
        if (!r.ok || !first || first.status === 'ERR' || !Array.isArray(first.result)) {
          failed++;
          continue;
        }
        const landed = yield* landChunk(env, plan.chunks[i], first.result as Array<Record<string, unknown>>, plan.versions, state, epoch);
        if (!landed) failed++;
      }
    } finally {
      yield fx.state.update(R.compose(...plan.hashes.map((h) => R.applyLifecycle(h, { type: 'fetch-end' }))));
    }
    yield fx.dispatch({ type: 'SyncOutcome', ok: failed === 0, error: failed > 0 ? 'body fetch failed' : undefined });
    if (failed > 0) {
      const attempt = (yield fx.state.read((s) => s.sync.fetchAttempts)) as number;
      yield fx.state.update(R.patchSync({ fetchAttempts: attempt + 1 }));
      yield fx.timer.set('fetch', backoffMs(attempt), { type: 'FetchRows' });
      return;
    }
  }
}

/** Write one chunk's bodies to the store and circuit; record their versions. */
function* landChunk(
  env: SagaEnv,
  requested: string[],
  rows: Array<Record<string, unknown>>,
  versions: ReadonlyMap<string, number>,
  state: ClientState,
  epoch: number
): Saga<boolean> {
  const bodies: Array<{ id: unknown; content: Record<string, unknown> }> = [];
  const ingest: IngestRecord[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    const rid = row?.id as RecordId<string> | undefined;
    if (!rid || typeof rid !== 'object') continue;
    const id = encodeRecordId(rid);
    const table = String(rid.table);
    const version = versions.get(id) ?? 0;
    const columns = env.schema.tables.find((t) => t.name === table)?.columns;
    const cleaned = columns ? cleanRecord(columns, row) : row;
    const { id: _id, ...content } = cleaned;
    bodies.push({ id: rid, content: { ...content, _00_rv: version } });
    ingest.push({ table, op: state.versions.has(id) ? 'UPDATE' : 'CREATE', id, record: { ...cleaned, _00_rv: version } });
    seen.add(id);
  }
  if (bodies.length > 0) {
    try {
      const tx = sql.upsertBodiesTx(bodies);
      yield fx.local.execute(tx.query, tx.vars, epoch);
    } catch (error) {
      yield fx.emit({ type: 'log', level: 'warn', message: 'body write failed', data: { error } });
      return false;
    }
    try {
      yield fx.ssp.ingest(ingest);
    } catch (error) {
      yield fx.emit({ type: 'log', level: 'warn', message: 'circuit ingest failed', data: { error } });
    }
    if (state.tabRole === 'leader') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'ingest', records: ingest } });
  }
  // Ids the server did not return (deleted or unreadable upstream) count as
  // known at the requested version, so the plan stops asking for them; their
  // absence from the store is what the render shows.
  const entries: Array<readonly [string, number]> = requested.map((id) => [id, versions.get(id) as number]);
  yield fx.state.update(R.setVersions(entries));
  return true;
}
