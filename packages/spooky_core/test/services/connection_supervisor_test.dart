import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:spooky_core/src/services/database/connection_supervisor.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

class _Transport {
  /// A clock the test advances by hand, so the supervisor's wake rate limit
  /// moves with `async.elapse`.
  int nowMs = 0;

  final connected = StreamController<void>.broadcast(sync: true);
  final disconnected = StreamController<void>.broadcast(sync: true);
  bool open = true;
  int reconnects = 0;
  int probes = 0;
  int forceCloses = 0;

  /// Answers the next probe with a failure when set.
  bool probeFails = false;

  /// Answers the next reconnect with a failure when set.
  bool reconnectFails = false;

  /// Never completes, so the probe's timeout fires instead.
  bool probeHangs = false;

  Future<void> reconnect() async {
    reconnects++;
    if (reconnectFails) throw StateError('nope');
    open = true;
    connected.add(null);
  }

  Future<void> probe() {
    probes++;
    if (probeHangs) return Completer<void>().future;
    if (probeFails) return Future.error(StateError('dead'));
    return Future.value();
  }

  Future<void> forceClose() async {
    forceCloses++;
    open = false;
    disconnected.add(null);
  }

  ConnectionSupervisor supervisor({ReconnectConfig? config}) =>
      ConnectionSupervisor(
        reconnect: reconnect,
        probe: probe,
        forceClose: forceClose,
        isConnected: () => open,
        onConnected: connected.stream,
        onDisconnected: disconnected.stream,
        logger: SpookyLogger.root('test'),
        config: config ?? const ReconnectConfig(),
        now: () => nowMs,
      );
}

void main() {
  test('a dropped socket is re-opened on backoff and never given up on', () {
    fakeAsync((async) {
      final t = _Transport();
      final sup = t.supervisor()..start();
      expect(sup.connection, ConnectionState.connected);

      t.reconnectFails = true;
      t.open = false;
      t.disconnected.add(null);
      expect(sup.connection, ConnectionState.disconnected);

      async.elapse(const Duration(milliseconds: 1000));
      async.flushMicrotasks();
      expect(t.reconnects, 1);
      expect(sup.connection, ConnectionState.reconnecting);

      // Backoff doubles: the second attempt is 2s after the first failed.
      async.elapse(const Duration(milliseconds: 1999));
      expect(t.reconnects, 1);
      async.elapse(const Duration(milliseconds: 1));
      expect(t.reconnects, 2);

      t.reconnectFails = false;
      async.elapse(const Duration(seconds: 10));
      expect(sup.connection, ConnectionState.connected);
      expect(t.open, isTrue);
      unawaited(sup.dispose());
      async.flushTimers();
    });
  });

  test('one failed probe is inconclusive; two tear the socket down', () {
    fakeAsync((async) {
      final t = _Transport();
      final sup = t.supervisor(
          config: const ReconnectConfig(
              heartbeatIntervalMs: 20000, heartbeatTimeoutMs: 1000))
        ..start();

      t.probeFails = true;
      async.elapse(const Duration(milliseconds: 20000));
      async.flushMicrotasks();
      expect(t.probes, 1);
      expect(t.forceCloses, 0, reason: 'a busy window is not a dead socket');

      // The re-probe runs at min(heartbeatRetryMs, interval) = 5s.
      async.elapse(const Duration(milliseconds: 5000));
      async.flushMicrotasks();
      expect(t.probes, 2);
      expect(t.forceCloses, 1);
      expect(sup.connection, ConnectionState.disconnected);
      unawaited(sup.dispose());
      async.flushTimers();
    });
  });

  test('a probe that never answers counts as a failure', () {
    fakeAsync((async) {
      final t = _Transport()..probeHangs = true;
      final sup = t.supervisor(
          config: const ReconnectConfig(
              heartbeatIntervalMs: 1000, heartbeatTimeoutMs: 500))
        ..start();
      async.elapse(const Duration(milliseconds: 1500));
      async.flushMicrotasks();
      expect(t.probes, 1);
      // min(5000, 1000) = 1000 before the second probe, which also times out.
      async.elapse(const Duration(milliseconds: 1500));
      async.flushMicrotasks();
      expect(t.forceCloses, 1);
      unawaited(sup.dispose());
      async.flushTimers();
    });
  });

  test('a healthy probe reconciles a state forced by suspend()', () {
    fakeAsync((async) {
      final t = _Transport();
      final sup = t.supervisor()..start();
      sup.suspend();
      expect(sup.connection, ConnectionState.disconnected);
      // The socket never actually closed, so nothing else would correct this.
      sup.wake('resumed');
      async.flushMicrotasks();
      expect(t.probes, 1);
      expect(sup.connection, ConnectionState.connected);
      unawaited(sup.dispose());
      async.flushTimers();
    });
  });

  test('wake probes are rate-limited while connected', () {
    fakeAsync((async) {
      final t = _Transport();
      final sup = t.supervisor()..start();
      sup.wake();
      async.flushMicrotasks();
      expect(t.probes, 1);
      sup.wake();
      sup.wake();
      async.flushMicrotasks();
      expect(t.probes, 1, reason: 'a resume storm must not probe per event');
      t.nowMs += ConnectionSupervisor.wakeProbeMinIntervalMs;
      sup.wake();
      async.flushMicrotasks();
      expect(t.probes, 2);
      unawaited(sup.dispose());
      async.flushTimers();
    });
  });

  test('wake reconnects immediately when the socket is gone', () {
    fakeAsync((async) {
      final t = _Transport();
      final sup = t.supervisor()..start();
      t.open = false;
      t.disconnected.add(null);
      sup.wake('resumed');
      async.flushMicrotasks();
      expect(t.reconnects, 1, reason: 'not waiting out the scheduled backoff');
      expect(sup.connection, ConnectionState.connected);
      unawaited(sup.dispose());
      async.flushTimers();
    });
  });

  test('subscribers see the current state and every change', () {
    fakeAsync((async) {
      final t = _Transport();
      final sup = t.supervisor();
      final seen = <ConnectionState>[];
      final off = sup.subscribe(seen.add);
      sup.start();
      t.open = false;
      t.disconnected.add(null);
      off();
      t.open = true;
      t.connected.add(null);
      expect(seen, [
        ConnectionState.disconnected,
        ConnectionState.connected,
        ConnectionState.disconnected,
      ]);
      unawaited(sup.dispose());
      async.flushTimers();
    });
  });
}
