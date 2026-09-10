import type { QueryHash } from '../types';
import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import type { ClientState } from '../state/client-state';
import * as R from '../state/reducers';
import type { SagaEnv } from '../query/env';
import { listRefTable } from '../query/env';
import { markMembershipDirty } from '../query/membership.saga';

/**
 * LIVE on the session's `_00_list_ref` table. Events never carry data into
 * state: every change only marks its query's membership dirty, and the
 * batched re-read decides. Followers get the same dirt relayed by the
 * leader.
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
export function* liveChange(hashes: QueryHash[]): Saga<void> {
  const [known, role] = (yield fx.state.read((s) => [hashes.filter((h) => s.queries.has(h)), s.tabRole])) as [QueryHash[], ClientState['tabRole']];
  if (known.length === 0) return;
  yield fx.state.update(R.patchSync({ pollIdleStreak: 0 }));
  yield* markMembershipDirty(known);
  if (role === 'leader') yield fx.emit({ type: 'tabs:broadcast', message: { type: 'membership-dirty', hashes: known } });
}
