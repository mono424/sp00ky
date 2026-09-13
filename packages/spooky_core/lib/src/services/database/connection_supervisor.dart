import 'dart:async';
import 'dart:math' as math;

import '../../types.dart';
import '../logger/logger.dart';

/// Keeps the remote WebSocket alive for the whole life of the process.
///
/// The Dart transport has no reconnect loop of its own - `connect()` is called
/// once and nothing ever re-dials - so unlike the browser client's supervisor,
/// which coordinates with the SurrealDB SDK's own retry, this one owns every
/// recovery path:
///
/// 1. **The socket closes.** `onDisconnected` fires; the revive loop re-opens
///    it on exponential backoff and never gives up.
/// 2. **The socket never closes at all.** A half-open connection: the peer is
///    gone (NAT timeout, wifi switch, the device slept) but no FIN arrives, so
///    nothing fires. The heartbeat detects this and forces the teardown that
///    case 1 then repairs.
/// 3. **A wake signal.** Coming back online or returning to the foreground is
///    the strongest hint a reconnect will now succeed, so [wake] probes
///    immediately rather than waiting out a backoff scheduled under worse
///    conditions. The core is framework-agnostic, so the host calls it: in
///    Flutter, from `AppLifecycleState.resumed`.
class ConnectionSupervisor {
  ConnectionSupervisor({
    required Future<void> Function() reconnect,
    required Future<void> Function() probe,
    required Future<void> Function() forceClose,
    required bool Function() isConnected,
    required Stream<void> onConnected,
    required Stream<void> onDisconnected,
    required SpookyLogger logger,
    ReconnectConfig config = const ReconnectConfig(),
    int Function()? now,
  })  : _reconnect = reconnect,
        _probe = probe,
        _forceClose = forceClose,
        _isConnected = isConnected,
        _onConnected = onConnected,
        _onDisconnected = onDisconnected,
        _logger = logger.child('ConnectionSupervisor'),
        _config = config,
        _now = now ?? _wallClockMs;

  final Future<void> Function() _reconnect;
  final Future<void> Function() _probe;
  final Future<void> Function() _forceClose;
  final bool Function() _isConnected;
  final Stream<void> _onConnected;
  final Stream<void> _onDisconnected;
  final SpookyLogger _logger;
  final ReconnectConfig _config;

  /// Wall clock in ms. Injected so a test can drive the wake rate limit with
  /// the same fake clock it drives the timers with.
  final int Function() _now;

  /// How many consecutive heartbeat failures it takes to tear the socket down.
  ///
  /// The probe rides the same serialized queue as every other RPC
  /// (deliberately - see [_beat]), so it cannot tell a WEDGED queue from a
  /// merely BUSY one. A single slow window (a large sync burst, one heavy app
  /// query) must not force-close a healthy socket, because the resulting
  /// reconnect re-registers every active query a second later: that self-
  /// inflicted teardown manufactures the very reconnect storms this class
  /// exists to survive. A genuinely dead socket still fails every probe and is
  /// torn down one interval later than it would have been.
  static const int failuresBeforeTeardown = 2;

  /// Retry delay after an inconclusive (first) heartbeat failure.
  static const int heartbeatRetryMs = 5000;

  /// Floor between probes triggered by wake events.
  static const int wakeProbeMinIntervalMs = 10000;

  static const int _reviveBaseMs = 1000;

  ConnectionState _state = ConnectionState.disconnected;
  final Set<void Function(ConnectionState)> _subscribers = {};
  final List<StreamSubscription<void>> _subscriptions = [];

  bool _started = false;
  bool _disposed = false;

  Timer? _heartbeatTimer;
  bool _heartbeatInFlight = false;
  int _heartbeatFailures = 0;

  Timer? _reviveTimer;
  int _reviveAttempts = 0;
  /// Null until the first wake probe: the very first one is never rate-limited.
  int? _lastWakeProbeAt;
  bool _reviving = false;

  /// Set while the host reports itself offline. Retrying a socket against a
  /// down interface only burns backoff, so the loop parks until [wake].
  bool _suspended = false;

  ConnectionState get connection => _state;

  /// Observe transport state. Fires immediately with the current value and
  /// again on every change. Returns an unsubscribe.
  void Function() subscribe(void Function(ConnectionState) cb) {
    cb(_state);
    _subscribers.add(cb);
    return () => _subscribers.remove(cb);
  }

  /// Begin supervising. Call once, after the initial connect. Idempotent.
  void start() {
    if (_started || _disposed) return;
    _started = true;
    _setState(_isConnected()
        ? ConnectionState.connected
        : ConnectionState.disconnected);

    _subscriptions.add(_onConnected.listen((_) {
      _reviveAttempts = 0;
      _clearReviveTimer();
      _setState(ConnectionState.connected);
      _startHeartbeat();
    }));
    _subscriptions.add(_onDisconnected.listen((_) {
      _setState(ConnectionState.disconnected);
      _stopHeartbeat();
      _scheduleRevive();
    }));

    if (_state == ConnectionState.connected) {
      _startHeartbeat();
    } else {
      _scheduleRevive();
    }
  }

  /// The host lost its network. Park reconnects rather than burning backoff.
  void suspend() {
    _suspended = true;
    _stopHeartbeat();
    _clearReviveTimer();
    _setState(ConnectionState.disconnected);
  }

  /// Reset the backoff and act on whichever problem is present: reconnect when
  /// the socket is gone, otherwise probe it - a half-open socket is exactly
  /// what a sleep/wake cycle produces.
  ///
  /// Probes from wake triggers are rate-limited: a healthy socket does not
  /// become unhealthy because the app was backgrounded for four seconds, and
  /// an unthrottled probe per resume would trip the teardown above.
  void wake([String reason = 'wake']) {
    _suspended = false;
    if (_disposed) return;
    final now = _now();
    final last = _lastWakeProbeAt;
    if (_isConnected() &&
        last != null &&
        now - last < wakeProbeMinIntervalMs) {
      return;
    }
    _lastWakeProbeAt = now;
    _logger.debug('Wake trigger ($reason); probing the connection');
    _reviveAttempts = 0;
    if (_isConnected()) {
      _stopHeartbeat();
      unawaited(_beat());
      return;
    }
    _clearReviveTimer();
    unawaited(_revive());
  }

  Future<void> dispose() async {
    _disposed = true;
    _started = false;
    _stopHeartbeat();
    _clearReviveTimer();
    for (final sub in _subscriptions) {
      await sub.cancel();
    }
    _subscriptions.clear();
    _subscribers.clear();
  }

  void _setState(ConnectionState next) {
    if (_state == next) return;
    _state = next;
    _logger.info('Connection state changed: ${next.name}');
    for (final cb in [..._subscribers]) {
      try {
        cb(next);
      } catch (e) {
        _logger.debug('Connection subscriber threw: $e');
      }
    }
  }

  // ---- revive loop -----------------------------------------------------------

  void _clearReviveTimer() {
    _reviveTimer?.cancel();
    _reviveTimer = null;
  }

  /// Queue the next reconnect on exponential backoff, capped at
  /// [ReconnectConfig.retryDelayMaxMs]. Never gives up: the process is expected
  /// to outlive any outage.
  void _scheduleRevive() {
    if (_disposed || _suspended) return;
    if (_reviveTimer != null || _reviving) return;
    final delay = math.min(_config.retryDelayMaxMs,
        _reviveBaseMs * (1 << math.min(_reviveAttempts, 30)));
    _reviveTimer = Timer(Duration(milliseconds: delay), () {
      _reviveTimer = null;
      unawaited(_revive());
    });
  }

  Future<void> _revive() async {
    if (_disposed || _suspended || _reviving) return;
    if (_isConnected()) {
      _reviveAttempts = 0;
      return;
    }
    _reviving = true;
    _reviveAttempts++;
    _setState(ConnectionState.reconnecting);
    _logger.info('Re-opening the remote connection (attempt $_reviveAttempts)');
    try {
      await _reconnect();
      // `reviveAttempts` and the heartbeat are reset by the `connected`
      // listener: that is the only signal the handshake actually completed.
    } catch (e) {
      _logger.warn('Reconnect attempt $_reviveAttempts failed; will retry: $e');
    } finally {
      _reviving = false;
    }
    if (!_isConnected()) _scheduleRevive();
  }

  // ---- heartbeat watchdog -----------------------------------------------------

  void _stopHeartbeat() {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = null;
  }

  void _startHeartbeat() {
    _stopHeartbeat();
    if (_disposed || _suspended) return;
    if (_config.heartbeatIntervalMs <= 0) return;
    _heartbeatTimer = Timer(
        Duration(milliseconds: _config.heartbeatIntervalMs), () => _beat());
  }

  /// Probe the server end to end. Deliberately goes through the same
  /// serialized queue every other remote call uses, so a queue wedged behind a
  /// stuck RPC fails the heartbeat instead of being invisible to it.
  Future<void> _beat() async {
    _heartbeatTimer = null;
    if (_disposed || _suspended) return;
    if (!_isConnected()) return;
    if (_heartbeatInFlight) {
      _startHeartbeat();
      return;
    }
    _heartbeatInFlight = true;
    try {
      await _probe().timeout(
          Duration(milliseconds: _config.heartbeatTimeoutMs),
          onTimeout: () => throw TimeoutException(
              'Heartbeat timed out after ${_config.heartbeatTimeoutMs}ms'));
      _heartbeatFailures = 0;
      // A round trip is the strongest evidence there is that the socket works,
      // so it also RECONCILES the reported state: a `suspend()` that forced
      // `disconnected` without the transport's agreement would otherwise stick
      // for the life of the process, traffic flowing while every indicator says
      // offline.
      _setState(ConnectionState.connected);
      _startHeartbeat();
    } catch (e) {
      _heartbeatFailures++;
      if (_heartbeatFailures < failuresBeforeTeardown) {
        // Inconclusive: the probe shares a queue with ordinary traffic, so this
        // may be a busy window rather than a dead socket. Re-probe soon instead
        // of tearing down a connection that is probably fine.
        _logger.debug(
            'Heartbeat failed ($_heartbeatFailures); re-probing before tearing the socket down: $e');
        _stopHeartbeat();
        if (!_disposed && !_suspended) {
          _heartbeatTimer = Timer(
              Duration(
                  milliseconds: math.min(
                      heartbeatRetryMs, _config.heartbeatIntervalMs)),
              () => _beat());
        }
        return;
      }
      _logger.warn(
          'Heartbeat failed $_heartbeatFailures times; tearing the socket down to force a reconnect: $e');
      _heartbeatFailures = 0;
      // Force the close the transport never delivered. The resulting
      // `disconnected` event drives the revive loop.
      await _forceClose();
      if (!_isConnected()) _scheduleRevive();
    } finally {
      _heartbeatInFlight = false;
    }
  }
}

int _wallClockMs() => DateTime.now().millisecondsSinceEpoch;
