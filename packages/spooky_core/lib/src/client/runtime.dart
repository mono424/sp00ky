import 'dart:async';

import '../kernel/events.dart';
import '../kernel/interpreter.dart';
import '../kernel/saga.dart';
import '../query/env.dart';
import '../services/logger/logger.dart';
import '../state/client_state.dart';
import '../state/lifecycle.dart';
import '../state/reducers.dart' as r;
import '../state/selectors.dart';
import '../types.dart';
import 'router.dart';

class _Waiter {
  _Waiter(this.until) : done = Completer<void>();
  final bool Function(ClientState) until;
  final Completer<void> done;
}

/// Wall-clock timers, the only place the engine reaches for one. A test swaps
/// this for a fake through [Adapters.timers].
class RealTimers implements TimerPort {
  final Map<String, Timer> _pending = {};

  @override
  void set(String key, int ms, void Function() fire) {
    _pending.remove(key)?.cancel();
    _pending[key] = Timer(Duration(milliseconds: ms), () {
      _pending.remove(key);
      fire();
    });
  }

  @override
  void clear(String key) => _pending.remove(key)?.cancel();

  void cancelAll() {
    for (final t in _pending.values) {
      t.cancel();
    }
    _pending.clear();
  }
}

/// The one effectful object of the engine: holds the state, runs sagas on
/// lanes, fires timers, fans events out to subscribers and schedules
/// materialization for dirty queries. No decision logic lives here.
class Runtime implements InterpreterHost {
  Runtime({
    required SagaEnv env,
    required Adapters adapters,
    required SpookyLogger logger,
    required String clientId,
    ClientState? initialState,
  })  : _env = env,
        _adapters = adapters,
        _logger = logger.child('Runtime'),
        _state = initialState ?? emptyState(tabId: clientId) {
    _interpret = Interpreter(_adapters, this);
    _lastHealth = _state.sync.health;
  }

  final SagaEnv _env;
  final SpookyLogger _logger;
  final Adapters _adapters;
  late final Interpreter _interpret;

  ClientState _state;

  final Map<String, Future<void>> _serialLanes = {};
  final Map<String, Future<Object?>> _dedupeLanes = {};
  final Set<_Waiter> _waiters = {};
  final Map<QueryHash, Set<QueryUpdateCallback>> _recordSubs = {};
  final Map<QueryHash, Set<QueryStatusCallback>> _statusSubs = {};
  final Map<QueryHash, Set<QueryAuthorityCallback>> _authoritySubs = {};
  final Map<QueryHash, QueryStatus> _lastStatus = {};
  final Map<String, Set<void Function(OutEvent)>> _listeners = {};
  final Set<QueryHash> _scheduledMaterialize = {};
  ({int fetching, int pending}) _lastActivity = (fetching: 0, pending: 0);
  late SyncHealth _lastHealth;
  bool _disposed = false;

  ClientState get state => _state;
  Ctx get ctx => _interpret;

  /// Run a saga, optionally on a lane. Errors propagate to the caller.
  ///
  /// A dedupe lane answers the joiner with the run already in flight, so the
  /// two must have the same result type; a facade call that wants a value back
  /// uses a serial lane or none.
  Future<R> run<R>(Saga<R> saga, {Lane? lane}) {
    Future<R> exec() => saga(_interpret);
    if (lane == null) return exec();
    if (lane.kind == LaneKind.dedupe) {
      final running = _dedupeLanes[lane.key];
      if (running != null) return running.then((v) => v as R);
      late final Future<Object?> p;
      p = exec().whenComplete(() {
        if (identical(_dedupeLanes[lane.key], p)) _dedupeLanes.remove(lane.key);
      });
      _dedupeLanes[lane.key] = p;
      return p.then((v) => v as R);
    }
    final prev = _serialLanes[lane.key] ?? Future<void>.value();
    final p = prev.then((_) => exec(), onError: (_) => exec());
    final tail = p.then<void>((_) {}, onError: (_) {});
    _serialLanes[lane.key] = tail;
    unawaited(tail.then((_) {
      if (identical(_serialLanes[lane.key], tail)) {
        _serialLanes.remove(lane.key);
      }
    }));
    return p;
  }

  /// Route an event to its saga. Never fails: errors are logged.
  @override
  void dispatch(RuntimeEvent event) => unawaited(dispatchAsync(event));

  Future<void> dispatchAsync(RuntimeEvent event) {
    if (_disposed) return Future<void>.value();
    final target = route(_env, event);
    return run(target.saga, lane: target.lane).catchError((Object error) {
      _logger.error('saga failed for ${event.type}', error);
    });
  }

  @override
  ClientState getState() => _state;

  @override
  void setState(ClientState next) {
    final prev = _state;
    if (identical(next, prev)) return;
    _state = next;
    for (final w in [..._waiters]) {
      bool ok;
      try {
        ok = w.until(next);
      } catch (error) {
        _waiters.remove(w);
        w.done.completeError(error);
        continue;
      }
      if (ok) {
        _waiters.remove(w);
        w.done.complete();
      }
    }
    for (final hash in next.dirty) {
      if (_scheduledMaterialize.contains(hash)) continue;
      _scheduledMaterialize.add(hash);
      _adapters.timers.set('mat:$hash', _env.materializeDebounceMs, () {
        _scheduledMaterialize.remove(hash);
        dispatch(Materialize(hash));
      });
    }
    for (final entry in _statusSubs.entries) {
      final query = next.queries[entry.key];
      if (query == null) continue;
      final status = deriveStatus(query.lifecycle);
      if (_lastStatus[entry.key] == status) continue;
      _lastStatus[entry.key] = status;
      for (final cb in entry.value) {
        _safely(() => cb(status));
      }
      _notify(QueryStatusEvent(entry.key, status));
    }
    final activity = (
      fetching: fetchingQueryCount(next),
      pending: pendingMutationCount(next),
    );
    if (activity != _lastActivity) {
      _lastActivity = activity;
      _notify(ActivityChangedEvent(
          fetching: activity.fetching, pending: activity.pending));
    }
    // Health is notified from here, not from the saga that folds a sync round:
    // `connection` moves through `setConnection` on a transport event with no
    // round attached, and a subscriber that only heard the round would show a
    // stale socket state forever. `setHealth` keeps the value stable while
    // nothing moved, so this fires exactly on real transitions.
    if (next.sync.health != _lastHealth) {
      _lastHealth = next.sync.health;
      _notify(HealthChangedEvent(next.sync.health));
    }
  }

  @override
  Future<void> waitFor(bool Function(ClientState) until) {
    try {
      if (until(_state)) return Future<void>.value();
    } catch (error) {
      return Future<void>.error(error);
    }
    final waiter = _Waiter(until);
    _waiters.add(waiter);
    return waiter.done.future;
  }

  /// Deliver an outbound event to its subscribers and listeners.
  @override
  void emit(OutEvent event) {
    switch (event) {
      case QueryRecordsEvent(:final hash, :final records):
        for (final cb in _recordSubs[hash] ?? const <QueryUpdateCallback>{}) {
          _safely(() => cb(records));
        }
      case QueryAuthorityEvent(:final hash, :final known):
        for (final cb
            in _authoritySubs[hash] ?? const <QueryAuthorityCallback>{}) {
          _safely(() => cb(known));
        }
      case LogEvent(:final level, :final message, :final data):
        final text = data == null ? message : '$message $data';
        switch (level) {
          case LogLevel.debug:
            _logger.debug(text);
          case LogLevel.info:
            _logger.info(text);
          case LogLevel.warn:
            _logger.warn(text);
          case LogLevel.error:
            _logger.error(text);
        }
      default:
        break;
    }
    _notify(event);
  }

  void _notify(OutEvent event) {
    for (final cb in _listeners[event.type] ?? const <void Function(OutEvent)>{}) {
      _safely(() => cb(event));
    }
    for (final cb in _listeners['*'] ?? const <void Function(OutEvent)>{}) {
      _safely(() => cb(event));
    }
  }

  void _safely(void Function() fn) {
    try {
      fn();
    } catch (error) {
      _logger.error('subscriber threw', error);
    }
  }

  /// Observe outbound events by type (`'*'` for all).
  void Function() on(String type, void Function(OutEvent) cb) {
    final set = _listeners.putIfAbsent(type, () => {});
    set.add(cb);
    return () => set.remove(cb);
  }

  void Function() subscribe(QueryHash hash, QueryUpdateCallback cb,
      {bool immediate = false}) {
    final set = _recordSubs.putIfAbsent(hash, () => {});
    set.add(cb);
    setState(r.subscribeQuery(hash)(_state));
    if (immediate) {
      final entry = _state.queries[hash];
      if (entry != null) _safely(() => cb(entry.records));
    }
    return () {
      if (!set.remove(cb)) return;
      if (set.isEmpty) _recordSubs.remove(hash);
      setState(r.unsubscribeQuery(hash, _adapters.now())(_state));
    };
  }

  void Function() subscribeStatus(QueryHash hash, QueryStatusCallback cb,
      {bool immediate = false}) {
    final set = _statusSubs.putIfAbsent(hash, () => {});
    set.add(cb);
    final entry = _state.queries[hash];
    if (entry != null) {
      final status = deriveStatus(entry.lifecycle);
      _lastStatus[hash] = status;
      if (immediate) _safely(() => cb(status));
    }
    return () {
      set.remove(cb);
      if (set.isEmpty) {
        _statusSubs.remove(hash);
        _lastStatus.remove(hash);
      }
    };
  }

  void Function() subscribeAuthority(QueryHash hash, QueryAuthorityCallback cb,
      {bool immediate = false}) {
    final set = _authoritySubs.putIfAbsent(hash, () => {});
    set.add(cb);
    if (immediate) {
      final entry = _state.queries[hash];
      if (entry != null) _safely(() => cb(isAuthoritative(entry.lifecycle)));
    }
    return () {
      set.remove(cb);
      if (set.isEmpty) _authoritySubs.remove(hash);
    };
  }

  /// Apply a reducer outside a saga (facade conveniences such as timing
  /// reports).
  void update(ClientState Function(ClientState) reducer) =>
      setState(reducer(_state));

  void dispose() {
    _disposed = true;
    for (final key in _scheduledMaterialize) {
      _adapters.timers.clear('mat:$key');
    }
    _scheduledMaterialize.clear();
    final timers = _adapters.timers;
    if (timers is RealTimers) timers.cancelAll();
    for (final w in _waiters) {
      w.done.completeError(StateError('runtime disposed'));
    }
    _waiters.clear();
  }
}
