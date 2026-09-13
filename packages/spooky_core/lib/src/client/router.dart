import '../boot/auth_flip_saga.dart';
import '../boot/boot_saga.dart';
import '../boot/bucket_switch_saga.dart';
import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../mutation/push_saga.dart';
import '../mutation/write_saga.dart';
import '../query/env.dart';
import '../query/fetch_saga.dart';
import '../query/lifecycle_saga.dart';
import '../query/materialize_saga.dart';
import '../query/membership_saga.dart';
import '../query/register_saga.dart';
import '../sync/connection_saga.dart';
import '../sync/live_saga.dart';
import '../sync/poll_saga.dart';

class RouteTarget {
  const RouteTarget(this.saga, [this.lane]);
  final Saga<void> saga;
  final Lane? lane;
}

Future<void> _materializeDirty(Ctx ctx) async {
  final dirty = await ctx(Fx.stateRead((s) => s.dirty.toList()));
  for (final hash in dirty) {
    await ctx(Fx.dispatch(Materialize(hash)));
  }
}

/// One table: which saga handles an event, and on which lane.
RouteTarget route(SagaEnv env, RuntimeEvent event) => switch (event) {
      EnsureRegistered(:final requireAuth, :final attempt) => RouteTarget(
          (ctx) => ensureRegistered(ctx, env,
              requireAuth: requireAuth, attempt: attempt),
          const Lane.serial('ensure'),
        ),
      RegisterRemote(:final hash, :final retry) => RouteTarget(
          (ctx) => registerRemote(ctx, env, hash, retry: retry),
          Lane.dedupe('register:$hash'),
        ),
      ReadDirtyMembership() => RouteTarget(
          (ctx) => readDirtyMembership(ctx, env),
          const Lane.serial('membership'),
        ),
      ReadMembership(:final hashes) => RouteTarget(
          (ctx) => readMembership(ctx, env, hashes, force: true),
          const Lane.serial('membership'),
        ),
      FetchRows() => RouteTarget(
          (ctx) => fetchRows(ctx, env),
          const Lane.serial('fetch'),
        ),
      Materialize(:final hash) => RouteTarget(
          (ctx) => materialize(ctx, hash),
          Lane.serial('mat:$hash'),
        ),
      MaterializeDirty() => RouteTarget(_materializeDirty),
      StreamUpdateEvent(:final update) => RouteTarget(
          (ctx) => streamUpdate(ctx, update),
          Lane.serial('stream:${update.queryHash}'),
        ),
      LifecycleTick() || HeartbeatNow() => RouteTarget(
          (ctx) => lifecycleTick(ctx, env),
          const Lane.dedupe('lifecycle'),
        ),
      AckPrune() => RouteTarget(ackPrune, const Lane.dedupe('ack-prune')),
      GcTick() => RouteTarget(gcTick, const Lane.dedupe('gc')),
      Drain() => RouteTarget(
          (ctx) => drain(ctx, env),
          const Lane.serial('outbox'),
        ),
      FlushWrite(:final key) => RouteTarget(
          (ctx) => flushWrite(ctx, env, key),
          const Lane.serial('outbox-write'),
        ),
      PollTick() =>
        RouteTarget((ctx) => pollTick(ctx, env), const Lane.dedupe('poll')),
      SelfHealTick() =>
        RouteTarget((ctx) => selfHealTick(ctx, env), const Lane.dedupe('heal')),
      SyncOutcome(:final ok, :final error) => RouteTarget(
          (ctx) => syncOutcome(ctx, env, ok, error),
          const Lane.serial('health'),
        ),
      ConnectionChanged(:final state) => RouteTarget(
          (ctx) => connectionChanged(ctx, env, state),
          const Lane.serial('connection'),
        ),
      LiveStart() =>
        RouteTarget((ctx) => liveStart(ctx, env), const Lane.serial('live')),
      LiveChange(:final hashes, :final rows) => RouteTarget(
          (ctx) => liveChange(ctx, env, hashes, rows),
          const Lane.serial('live-change'),
        ),
      StartRemote() =>
        RouteTarget(startRemote, const Lane.dedupe('start-remote')),
      PrimeCircuit() =>
        RouteTarget(primeCircuit, const Lane.dedupe('prime')),
      VersionsPrimed(:final entries) =>
        RouteTarget((ctx) => versionsPrimed(ctx, entries)),
      AuthFlip(:final userId) => RouteTarget(
          (ctx) => authFlip(ctx, env, userId),
          const Lane.serial('bucket'),
        ),
      BucketSwitch(:final target) => RouteTarget(
          (ctx) => bucketSwitch(ctx, env, target),
          const Lane.serial('bucket'),
        ),
      AppDetached() => RouteTarget(appDetached),
    };
