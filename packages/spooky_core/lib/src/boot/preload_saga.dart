import '../kernel/effects.dart';
import '../kernel/saga.dart';
import '../query/env.dart';
import '../query/register_saga.dart';
import '../state/lifecycle.dart' show QueryPhase;
import '../state/selectors.dart' show settleFailed, settled;
import '../types.dart';

class PreloadFailedError implements Exception {
  PreloadFailedError(this.hash);
  final QueryHash hash;
  @override
  String toString() =>
      'PreloadFailedError: preload could not settle, the registration of $hash failed';
}

/// Preload = a registered query nobody subscribes to.
///
/// Resolved before on this device: returns as soon as the entry exists (its
/// rows paint from cache). Never resolved: blocks until the server's membership
/// and every body are local and the first materialization ran. The entry is
/// evicted like any other query a ttl after it was last watched (or
/// registered).
Future<({QueryHash hash, bool waited})> preload(
    Ctx ctx, SagaEnv env, RegisterInput input) async {
  final hash = await registerLocal(ctx, env, input);
  final phase =
      await ctx(Fx.stateRead((s) => s.queries[hash]?.lifecycle.phase));
  if (phase != QueryPhase.cold) return (hash: hash, waited: false);
  await ctx(
      Fx.stateWait((s) => settled(s, hash) || settleFailed(s, hash)));
  final failed = await ctx(Fx.stateRead((s) => settleFailed(s, hash)));
  if (failed) throw PreloadFailedError(hash);
  return (hash: hash, waited: true);
}
