import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../query/env.dart';
import '../query/membership_saga.dart';
import '../state/reducers.dart' as r;
import '../state/selectors.dart' show activeHashes, hasAckedWrites;
import 'policy.dart' show listRefPollDelayMs;

/// The `_00_list_ref` poll: the safety net under LIVE. Reads every active
/// query's membership on a backoff that snaps to the base cadence whenever
/// something changed (or an acked write still waits for membership) and coasts
/// up to the cap while quiet. With no queries it probes connectivity, which is
/// also the only health signal an idle client produces.
Future<void> pollTick(Ctx ctx, SagaEnv env) async {
  final hashes = await ctx(Fx.stateRead(activeHashes));
  final streak = await ctx(Fx.stateRead((s) => s.sync.pollIdleStreak));
  final acked = await ctx(Fx.stateRead(hasAckedWrites));
  var changed = false;
  if (hashes.isEmpty) {
    try {
      await ctx(Fx.remoteQuery('RETURN true', timeoutMs: env.remoteTimeoutMs));
      await ctx(Fx.dispatch(const SyncOutcome(true)));
    } catch (error) {
      await ctx(Fx.dispatch(SyncOutcome(false, error)));
    }
  } else {
    final result = await readMembership(ctx, env, hashes);
    changed = result.changed;
  }
  final idleStreak = changed || acked ? 0 : streak + 1;
  await ctx(Fx.stateUpdate(r.patchSync(pollIdleStreak: idleStreak)));
  await ctx(Fx.timerSet(
      'poll',
      listRefPollDelayMs(idleStreak: idleStreak, baseIntervalMs: env.pollBaseMs),
      const PollTick()));
}
