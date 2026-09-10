import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import * as R from '../state/reducers';
import { activeHashes, hasAckedWrites } from '../state/selectors';
import type { SagaEnv } from '../query/env';
import { readMembership } from '../query/membership.saga';
import { listRefPollDelayMs } from './policy';

/**
 * The `_00_list_ref` poll: the safety net under LIVE. Reads every active
 * query's membership on a backoff that snaps to the base cadence whenever
 * something changed (or an acked write still waits for membership) and
 * coasts up to the cap while quiet. With no queries it probes connectivity.
 */
export function* pollTick(env: SagaEnv): Saga<void> {
  const [hashes, streak, acked] = (yield fx.state.read((s) => [activeHashes(s), s.sync.pollIdleStreak, hasAckedWrites(s)])) as [
    string[],
    number,
    boolean,
  ];
  let changed = false;
  if (hashes.length === 0) {
    try {
      yield fx.remote.query('RETURN true', undefined, env.remoteTimeoutMs);
      yield fx.dispatch({ type: 'SyncOutcome', ok: true });
    } catch (error) {
      yield fx.dispatch({ type: 'SyncOutcome', ok: false, error });
    }
  } else {
    const result = yield* readMembership(env, hashes);
    changed = result.changed;
  }
  const idleStreak = changed || acked ? 0 : streak + 1;
  yield fx.state.update(R.patchSync({ pollIdleStreak: idleStreak }));
  yield fx.timer.set('poll', listRefPollDelayMs({ idleStreak, baseIntervalMs: env.pollBaseMs }), { type: 'PollTick' });
}
