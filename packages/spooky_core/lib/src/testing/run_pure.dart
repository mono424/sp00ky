import 'dart:async';
import 'dart:convert';

import 'package:crypto/crypto.dart';

import '../kernel/effects.dart';
import '../kernel/events.dart';
import '../kernel/saga.dart';
import '../state/client_state.dart';

typedef EffectHandler = FutureOr<Object?> Function(
    Effect<Object?> effect, PureContext ctx);

class PureContext {
  PureContext({required this.state, required this.now});

  ClientState state;
  int now;
  final List<Effect<Object?>> log = [];
  final Map<String, ({int ms, RuntimeEvent event})> timers = {};
  final List<OutEvent> emitted = [];
  final List<RuntimeEvent> dispatched = [];
  int ids = 0;
}

class RunPureResult<R> {
  RunPureResult(this.context, this.result);
  final PureContext context;
  final R result;

  ClientState get state => context.state;
  List<Effect<Object?>> get log => context.log;
  Map<String, ({int ms, RuntimeEvent event})> get timers => context.timers;
  List<OutEvent> get emitted => context.emitted;
  List<RuntimeEvent> get dispatched => context.dispatched;

  /// Every logged effect of one kind, in order.
  List<Effect<Object?>> ofKind(String kind) =>
      log.where((e) => e.kind == kind).toList();
}

class UnhandledEffectError extends Error {
  UnhandledEffectError(this.effect);
  final Effect<Object?> effect;

  @override
  String toString() => "runPure: no handler for effect '${effect.kind}'";
}

String sha256Hex(String input) =>
    sha256.convert(utf8.encode(input)).toString();

class _PureCtx implements Ctx {
  _PureCtx(this.ctx, this.handlers, this.epoch);

  final PureContext ctx;
  final Map<String, EffectHandler> handlers;
  final int epoch;

  @override
  Future<R> call<R>(Effect<R> effect) async {
    ctx.log.add(effect);
    final custom = handlers[effect.kind];
    if (custom != null) return await custom(effect, ctx) as R;
    switch (effect) {
      case StateRead<R>(:final select):
        return select(ctx.state);
      case StateUpdate(:final fn):
        ctx.state = fn(ctx.state);
        return ctx.state as R;
      case StateWait(:final until):
        if (until(ctx.state)) return null as R;
        throw StateError(
            'runPure: state.wait would block; script a handler or prepare the state');
      case NowEffect():
        return ctx.now as R;
      case LocalEpoch():
        return epoch as R;
      case IdEffect(:final scope):
        ctx.ids += 1;
        return '${scope.name}-${ctx.ids}' as R;
      case HashEffect(:final input):
        return sha256Hex(input) as R;
      case TimerSet(:final key, :final ms, :final event):
        ctx.timers[key] = (ms: ms, event: event);
        return null as R;
      case TimerClear(:final key):
        ctx.timers.remove(key);
        return null as R;
      case EmitEffect(:final event):
        ctx.emitted.add(event);
        return null as R;
      case DispatchEffect(:final event):
        ctx.dispatched.add(event);
        return null as R;
      case AllEffect(:final effects):
        final results = <Settled<Object?>>[];
        for (final inner in effects) {
          try {
            results.add(Settled.ok(await call(inner)));
          } catch (error) {
            results.add(Settled.err(error));
          }
        }
        return results as R;
      default:
        throw UnhandledEffectError(effect);
    }
  }
}

/// Drive a saga with canned effect results.
///
/// `state.*`, `now`, `id`, `hash`, `timer.*`, `emit`, `dispatch` and
/// `local.epoch` are handled here; every other adapter effect (`local.*`,
/// `remote.*`, `ssp.*`, `service`) must have a handler or the run throws, so a
/// test can never pass by an effect silently returning null.
Future<RunPureResult<R>> runPure<R>(
  Saga<R> saga, {
  ClientState? state,
  int now = 1700000000000,
  int epoch = 0,
  Map<String, EffectHandler> handlers = const {},
}) async {
  final ctx = PureContext(
    state: state ?? emptyState(tabId: 'tab-test'),
    now: now,
  );
  final result = await saga(_PureCtx(ctx, handlers, epoch));
  return RunPureResult(ctx, result);
}

/// Handler helper: answer effects by SQL prefix (first match wins).
EffectHandler bySqlPrefix(List<(String, Object? Function(RemoteQuery))> table) {
  return (effect, ctx) {
    final sql = effect is RemoteQuery ? effect.sql : '';
    for (final (prefix, answer) in table) {
      if (sql.startsWith(prefix)) return answer(effect as RemoteQuery);
    }
    throw StateError(
        'no scripted answer for SQL: ${sql.substring(0, sql.length < 80 ? sql.length : 80)}');
  };
}
