import type { QueryHash } from '../types';
import type { StreamUpdate } from '../services/stream-processor/index';
import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import type { QueryEntry } from '../state/client-state';
import * as R from '../state/reducers';
import { needed, overlay, vouched } from '../state/selectors';
import { isAnswered } from '../state/lifecycle';
import type { Overlay } from '../state/selectors';
import { isWindowed, materializeEffect, rowKey, rowsEqual, rowsFromResult } from './materialize';
import { buildRenderIds, paintsCold, resolveMembership } from './render-set';

const NONE: ReadonlySet<string> = new Set();

/**
 * Render one query from state: membership (or a cold scan), the outbox
 * overlay, one local read, then notify subscribers if the rows changed. The
 * only writer of `records`. Runs on the `mat:<hash>` dedupe lane whenever the
 * query is dirty. Before the server has answered (`cold`) only bodies some
 * durable view vouches for paint (see `paintsCold`).
 *
 * Also the only place a query becomes answered (see `QueryLifecycle`): the
 * authority flip is emitted here, after the rows it vouches for.
 */
export function* materialize(hash: QueryHash): Saga<void> {
  const [entry, ov, known, complete] = (yield fx.state.read((s) => {
    const e = s.queries.get(hash);
    return [e, overlay(s), e?.lifecycle.phase === 'cold' ? vouched(s) : NONE, needed(s, hash).length === 0];
  })) as [QueryEntry | undefined, Overlay, ReadonlySet<string>, boolean];
  if (!entry) return;
  const t0 = (yield fx.now()) as number;
  const isWindow = isWindowed(entry.def.surql);
  const paints = (id: string | null) => id === null || paintsCold(id, known, ov);
  const coldWindow = entry.lifecycle.phase === 'cold' && isWindow;
  const membership = resolveMembership({
    phase: entry.lifecycle.phase,
    remoteArray: entry.remoteArray,
    localArray: coldWindow ? entry.localArray.filter(([id]) => paints(id)) : entry.localArray,
    isWindow,
  });
  const ids = membership
    ? buildRenderIds(membership, entry.localArray, ov, {
        hasExplicitOrder: (entry.def.plan?.orderBy?.length ?? 0) > 0,
        isWindow,
      })
    : null;
  const effect = materializeEffect(entry.def, ids);
  let rows: Record<string, unknown>[];
  try {
    rows = rowsFromResult(effect, yield effect);
    // A peer's delete leaves the row in the store until it is confirmed, so
    // the predicate scan must drop it here (an own delete removed it already).
    if (ids === null)
      rows = rows.filter((row) => {
        const key = rowKey(row);
        return paints(key) && !(key !== null && ov.deletes.has(key));
      });
  } catch (error) {
    yield fx.state.update(R.compose(R.recordError(hash), R.clearDirty(hash)));
    yield fx.emit({ type: 'log', level: 'warn', message: 'materialize failed', data: { hash, error } });
    return;
  }
  const current = (yield fx.state.read((s) => s.queries.get(hash))) as QueryEntry | undefined;
  if (!current) return;
  const t1 = (yield fx.now()) as number;
  const changed = !rowsEqual(rows, current.records);
  // These rows answer the query when they drew a known membership with no
  // body of a server set still to download, and that membership is still
  // the one in state. A server set applied, or a bucket switch, while the
  // read was out dirtied the query again: the next render answers instead.
  const answered =
    complete && entry.lifecycle.phase !== 'cold' && current.lifecycle.phase !== 'cold' && current.remoteArray === entry.remoteArray;
  yield fx.state.update(
    R.compose(R.setRecords(hash, rows, changed, t1 - t0, answered), changed ? R.stampUpdated(hash, t1) : (s) => s)
  );
  if (changed) yield fx.emit({ type: 'query:records', hash, records: rows });
  // After the rows: a subscriber told "known" must already hold them.
  if (answered && !isAnswered(current.lifecycle)) yield fx.emit({ type: 'query:authority', hash, known: true });
}

/**
 * A view update from the in-browser SSP: the query's local id-set moved.
 * State takes the array (which dirties the query); the ingest timings feed
 * telemetry. Synthetic updates no longer exist: re-materialization is dirt.
 */
export function* streamUpdate(update: StreamUpdate): Saga<void> {
  const hash = update.queryHash;
  const exists = (yield fx.state.read((s) => s.queries.has(hash))) as boolean;
  if (!exists) return;
  const phases: Array<[string, number | undefined]> = [
    ['sspStoreApply', update.storeApplyMs],
    ['sspCircuitStep', update.circuitStepMs],
    ['sspTransform', update.transformMs],
  ];
  yield fx.state.update(
    R.compose(
      R.setLocalArray(hash, update.localArray),
      typeof update.materializationTimeMs === 'number' ? R.recordIngest(hash, update.materializationTimeMs) : (s) => s,
      ...phases.filter((p): p is [string, number] => typeof p[1] === 'number').map(([phase, ms]) => R.recordPhase(hash, phase, ms))
    )
  );
}
