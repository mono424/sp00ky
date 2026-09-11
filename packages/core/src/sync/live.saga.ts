import type { InlineRow, QueryHash } from '../types';
import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import type { ClientState } from '../state/client-state';
import * as R from '../state/reducers';
import type { SagaEnv } from '../query/env';
import { listRefTable } from '../query/env';
import { markMembershipDirty } from '../query/membership.saga';
import { landChunk } from '../query/fetch.saga';

/**
 * LIVE on the session's `_00_list_ref` table. An event marks its query's
 * membership dirty and the batched re-read decides; followers get the same
 * dirt relayed by the leader.
 *
 * When the subscription was opened with the body joined on
 * (`liveInlineBodies`) the event ALSO carries the changed row. That is an
 * optimisation on top, never a source of truth: the row is landed exactly as a
 * fetched body would be, so a notification the server drops costs nothing but
 * the round trip it would have saved.
 */
export function* liveStart(env: SagaEnv): Saga<void> {
  const state = (yield fx.state.read((s) => s)) as ClientState;
  if (state.tabRole === 'follower') return;
  const table = listRefTable(env, state);
  if (state.sync.liveUuid && state.sync.liveTable === table) return;
  if (state.sync.liveUuid && state.sync.health.connection === 'connected') {
    try {
      yield fx.remote.kill(state.sync.liveUuid);
    } catch (error) {
      yield fx.emit({ type: 'log', level: 'debug', message: 'kill of the previous live query failed', data: { error } });
    }
  }
  yield fx.state.update(R.patchSync({ liveUuid: null, liveTable: null }));
  try {
    const uuid = (yield fx.remote.live(table)) as string;
    yield fx.state.update(R.patchSync({ liveUuid: uuid, liveTable: table }));
  } catch (error) {
    yield fx.emit({ type: 'log', level: 'warn', message: 'live subscription failed; the poll covers membership', data: { table, error } });
  }
}

/** The socket dropped: the server-side live query is gone with it. */
export function* liveInvalidate(): Saga<void> {
  yield fx.state.update(R.patchSync({ liveUuid: null, liveTable: null }));
}

/** One or more edges of these queries changed on the server. */
export function* liveChange(env: SagaEnv, hashes: QueryHash[], rows?: InlineRow[]): Saga<void> {
  const [known, role] = (yield fx.state.read((s) => [hashes.filter((h) => s.queries.has(h)), s.tabRole])) as [QueryHash[], ClientState['tabRole']];
  if (known.length === 0) return;
  yield fx.state.update(R.patchSync({ pollIdleStreak: 0 }));
  // Before marking membership dirty: recording the version here is what makes
  // the re-read's `planFetch` find nothing left to pull for this row.
  if (rows && rows.length > 0) yield* landInlineRows(env, rows);
  yield* markMembershipDirty(known);
  if (role === 'leader') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'membership-dirty', hashes: known } });
}

/**
 * Land bodies that rode in on the notification. Rows already held at this
 * version or newer are dropped: one edit bumps the edge in every view holding
 * the row, so the same body arrives once per view.
 */
function* landInlineRows(env: SagaEnv, rows: InlineRow[]): Saga<void> {
  const state = (yield fx.state.read((s) => s)) as ClientState;
  const fresh = new Map<string, InlineRow>();
  for (const row of rows) {
    if ((state.versions.get(row.id) ?? -1) >= row.version) continue;
    const held = fresh.get(row.id);
    if (!held || held.version < row.version) fresh.set(row.id, row);
  }
  if (fresh.size === 0) return;
  const picked = [...fresh.values()];
  const epoch = (yield fx.local.epoch()) as number;
  const landed = yield* landChunk(
    env,
    picked.map((r) => r.id),
    picked.map((r) => r.record),
    new Map(picked.map((r) => [r.id, r.version] as const)),
    state,
    epoch
  );
  if (!landed) {
    yield fx.emit({ type: 'log', level: 'debug', message: 'inline live body not landed; the fetch path will pull it', data: { ids: picked.map((r) => r.id) } });
  }
}
