import 'effects.dart';

/// A saga is a function over an effect context. It yields effects by awaiting
/// them and holds no reference to any adapter or to the state, so the same
/// saga runs under the real interpreter or under `testing/run_pure.dart` with
/// canned results.
///
/// The TypeScript core spells this as `Generator<Effect, R, any>` and drives it
/// with `runSaga`. Dart generators are one-way - `yield` evaluates to void and
/// `Iterator` has no `next(value)` - so the effect result comes back through an
/// explicit context instead. Everything else is the same contract: a rejected
/// effect surfaces at the `await` so sagas use ordinary `try`/`catch`, and an
/// error a saga does not catch propagates to the caller.
typedef Saga<R> = Future<R> Function(Ctx ctx);

/// Executes effects. The one seam between a saga and the world.
abstract class Ctx {
  /// Run [effect] and return its result.
  ///
  /// Callable syntax, so a saga reads `await ctx(Fx.now())`.
  Future<R> call<R>(Effect<R> effect);
}

/// Lanes bound concurrency without a queue object per lane:
///
/// - `serial`: runs strictly one at a time per key, in arrival order.
/// - `dedupe`: a request while one run is active joins that run instead of
///   starting another.
///
/// The bookkeeping is a pure value so the policy is unit-testable; the runtime
/// owns the futures.
enum LaneKind { serial, dedupe }

class Lane {
  const Lane.serial(this.key) : kind = LaneKind.serial;
  const Lane.dedupe(this.key) : kind = LaneKind.dedupe;

  final LaneKind kind;
  final String key;
}

class LaneState {
  const LaneState({required this.running, required this.waiting});

  final Set<String> running;
  final Map<String, int> waiting;
}

LaneState emptyLanes() => const LaneState(running: {}, waiting: {});

enum LaneDecision { start, wait, join }

class LaneAcquire {
  const LaneAcquire(this.decision, this.state);
  final LaneDecision decision;
  final LaneState state;
}

LaneAcquire acquire(LaneState state, Lane lane) {
  if (!state.running.contains(lane.key)) {
    return LaneAcquire(
      LaneDecision.start,
      LaneState(
        running: {...state.running, lane.key},
        waiting: state.waiting,
      ),
    );
  }
  if (lane.kind == LaneKind.dedupe) {
    return LaneAcquire(LaneDecision.join, state);
  }
  return LaneAcquire(
    LaneDecision.wait,
    LaneState(
      running: state.running,
      waiting: {...state.waiting, lane.key: (state.waiting[lane.key] ?? 0) + 1},
    ),
  );
}

class LaneRelease {
  const LaneRelease(this.startNext, this.state);
  final bool startNext;
  final LaneState state;
}

LaneRelease release(LaneState state, String key) {
  final pending = state.waiting[key] ?? 0;
  if (pending > 0) {
    final waiting = {...state.waiting};
    if (pending == 1) {
      waiting.remove(key);
    } else {
      waiting[key] = pending - 1;
    }
    return LaneRelease(true, LaneState(running: state.running, waiting: waiting));
  }
  final running = {...state.running}..remove(key);
  return LaneRelease(false, LaneState(running: running, waiting: state.waiting));
}
