import '../kernel/constants.dart';
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../query/env.dart';
import '../state/lifecycle.dart';
import '../state/reducers.dart' as r;
import '../state/selectors.dart' show desiredRegistrations, pendingMutationCount;
import '../types.dart';
import 'live_saga.dart' show liveInvalidate;
import 'policy.dart';

/// Transport state changes. A drop invalidates the live query and arms a
/// resubscribe. The next `connected` restarts LIVE and drains the outbox;
/// outside the burst cooldown it also drops every remote registration so
/// `ensureRegistered` rebuilds them behind the `$auth.id` gate.
Future<void> connectionChanged(
    Ctx ctx, SagaEnv env, ConnectionState connection) async {
  final state = await ctx(Fx.stateRead((s) => s));
  await ctx(Fx.stateUpdate(r.setConnection(connection)));
  if (connection == ConnectionState.disconnected ||
      connection == ConnectionState.reconnecting) {
    await ctx(Fx.stateUpdate(r.patchSync(needsResubscribe: true)));
    await liveInvalidate(ctx);
    return;
  }
  if (connection != ConnectionState.connected ||
      !state.sync.needsResubscribe) {
    return;
  }
  final now = await ctx(Fx.now());
  final last = state.sync.lastReconnectRefetchAt;
  if (last != null && now - last < reconnectRefetchCooldownMs) {
    // The cooldown only spares the server a re-registration storm. The views
    // the last refetch registered are still on the server, but LIVE died with
    // the socket and nothing else restarts it: skipping it here left the
    // client on the membership poll until a reconnect outside the window.
    await ctx(Fx.stateUpdate(r.patchSync(needsResubscribe: false)));
  } else {
    await ctx(Fx.stateUpdate(r.compose([
      r.patchSync(needsResubscribe: false, lastReconnectRefetchAt: now),
      for (final e in state.queries.values)
        if (e.lifecycle.remote != RemotePhase.unregistered)
          r.applyLifecycle(e.def.hash, const RemoteDroppedEvent()),
    ])));
    await ctx(Fx.dispatch(const EnsureRegistered(requireAuth: true)));
  }
  await ctx(Fx.dispatch(const LiveStart()));
  await ctx(Fx.dispatch(const Drain()));
}

/// Fold one sync round's outcome into health; start or stop self-heal.
Future<void> syncOutcome(
    Ctx ctx, SagaEnv env, bool ok, Object? error) async {
  final sync = await ctx(Fx.stateRead((s) => s.sync));
  final next = nextHealth(
    HealthInput(
      health: sync.health,
      consecutiveFailures: sync.consecutiveFailures,
      hasSyncedOnce: sync.hasSyncedOnce,
    ),
    ok,
    error,
    env.degradeAfter,
  );
  await ctx(Fx.stateUpdate(r.compose([
    r.setHealth(next.health),
    r.patchSync(
      consecutiveFailures: next.consecutiveFailures,
      hasSyncedOnce: next.hasSyncedOnce,
    ),
  ])));
  if (next.degradedNow) {
    await ctx(Fx.stateUpdate(r.patchSync(selfHealAttempts: 0)));
    await ctx(Fx.timerSet('heal', selfHealDelayMs(0), const SelfHealTick()));
  }
  if (next.recoveredNow) await ctx(Fx.timerClear('heal'));
}

/// While degraded: re-drive whatever can prove the server is back, on a growing
/// backoff. Retry the outbox first, then the registrations (failed ones
/// included, behind the identity gate), else a bare probe.
Future<void> selfHealTick(Ctx ctx, SagaEnv env) async {
  final state = await ctx(Fx.stateRead((s) => s));
  if (state.sync.health.status != SyncHealthStatus.degraded) return;
  final attempt = state.sync.selfHealAttempts + 1;
  await ctx(Fx.stateUpdate(r.patchSync(selfHealAttempts: attempt)));
  final failed = [
    for (final e in state.queries.values)
      if (e.lifecycle.remote == RemotePhase.failed) e.def.hash
  ];
  if (pendingMutationCount(state) > 0) {
    await ctx(Fx.dispatch(const Drain()));
  } else if (desiredRegistrations(state).isNotEmpty || failed.isNotEmpty) {
    if (failed.isNotEmpty) {
      await ctx(Fx.stateUpdate(r.compose([
        for (final h in failed) r.applyLifecycle(h, const RemoteDroppedEvent())
      ])));
    }
    await ctx(Fx.dispatch(const EnsureRegistered(requireAuth: true)));
  } else {
    try {
      await ctx(Fx.remoteQuery('RETURN true', timeoutMs: env.remoteTimeoutMs));
      await ctx(Fx.dispatch(const SyncOutcome(true)));
    } catch (error) {
      await ctx(Fx.dispatch(SyncOutcome(false, error)));
    }
  }
  await ctx(
      Fx.timerSet('heal', selfHealDelayMs(attempt), const SelfHealTick()));
}
