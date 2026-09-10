import type { ConnectionState } from '../types';
import type { Saga } from '../kernel/saga';
import { fx } from '../kernel/effects';
import { RECONNECT_REFETCH_COOLDOWN_MS } from '../kernel/constants';
import type { ClientState } from '../state/client-state';
import * as R from '../state/reducers';
import { desiredRegistrations, pendingMutationCount } from '../state/selectors';
import type { SagaEnv } from '../query/env';
import { liveInvalidate } from './live.saga';
import { nextHealth, selfHealDelayMs } from './policy';

/**
 * Transport state changes. A drop invalidates the live query and arms a
 * resubscribe; the next `connected` (outside the burst cooldown) drops every
 * remote registration so `ensureRegistered` rebuilds them behind the
 * `$auth.id` gate, and restarts LIVE.
 */
export function* connectionChanged(env: SagaEnv, connection: ConnectionState): Saga<void> {
  const state = (yield fx.state.read((s) => s)) as ClientState;
  yield fx.state.update(R.setConnection(connection));
  if (connection === 'disconnected' || connection === 'reconnecting') {
    yield fx.state.update(R.patchSync({ needsResubscribe: true }));
    yield* liveInvalidate();
    return;
  }
  if (connection !== 'connected' || !state.sync.needsResubscribe) return;
  const now = (yield fx.now()) as number;
  const last = state.sync.lastReconnectRefetchAt;
  if (last !== null && now - last < RECONNECT_REFETCH_COOLDOWN_MS) {
    yield fx.state.update(R.patchSync({ needsResubscribe: false }));
    return;
  }
  yield fx.state.update(
    R.compose(
      R.patchSync({ needsResubscribe: false, lastReconnectRefetchAt: now }),
      ...[...state.queries.values()]
        .filter((e) => e.lifecycle.remote !== 'unregistered')
        .map((e) => R.applyLifecycle(e.def.hash, { type: 'remote-dropped' }))
    )
  );
  yield fx.dispatch({ type: 'EnsureRegistered', requireAuth: true });
  yield fx.dispatch({ type: 'LiveStart' });
  yield fx.dispatch({ type: 'Drain' });
}

/** Fold one sync round's outcome into health; start / stop self-heal. */
export function* syncOutcome(env: SagaEnv, ok: boolean, error?: unknown): Saga<void> {
  const sync = (yield fx.state.read((s) => s.sync)) as ClientState['sync'];
  const next = nextHealth({ health: sync.health, consecutiveFailures: sync.consecutiveFailures, hasSyncedOnce: sync.hasSyncedOnce }, ok, error, env.degradeAfter);
  yield fx.state.update(
    R.compose(R.setHealth(next.health), R.patchSync({ consecutiveFailures: next.consecutiveFailures, hasSyncedOnce: next.hasSyncedOnce }))
  );
  if (next.degradedNow) {
    yield fx.state.update(R.patchSync({ selfHealAttempts: 0 }));
    yield fx.timer.set('heal', selfHealDelayMs(0), { type: 'SelfHealTick' });
  }
  if (next.recoveredNow) yield fx.timer.clear('heal');
}

/**
 * While degraded: re-drive whatever can prove the server is back, on a
 * growing backoff. Retry the outbox first, then the registrations (failed
 * ones included, behind the identity gate), else a bare probe.
 */
export function* selfHealTick(env: SagaEnv): Saga<void> {
  const state = (yield fx.state.read((s) => s)) as ClientState;
  if (state.sync.health.status !== 'degraded') return;
  const attempt = state.sync.selfHealAttempts + 1;
  yield fx.state.update(R.patchSync({ selfHealAttempts: attempt }));
  const failed = [...state.queries.values()].filter((e) => e.lifecycle.remote === 'failed').map((e) => e.def.hash);
  if (pendingMutationCount(state) > 0) {
    yield fx.dispatch({ type: 'Drain' });
  } else if (desiredRegistrations(state).length > 0 || failed.length > 0) {
    if (failed.length > 0) yield fx.state.update(R.compose(...failed.map((h) => R.applyLifecycle(h, { type: 'remote-dropped' }))));
    yield fx.dispatch({ type: 'EnsureRegistered', requireAuth: true });
  } else {
    try {
      yield fx.remote.query('RETURN true', undefined, env.remoteTimeoutMs);
      yield fx.dispatch({ type: 'SyncOutcome', ok: true });
    } catch (error) {
      yield fx.dispatch({ type: 'SyncOutcome', ok: false, error });
    }
  }
  yield fx.timer.set('heal', selfHealDelayMs(attempt), { type: 'SelfHealTick' });
}
