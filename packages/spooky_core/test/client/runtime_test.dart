import 'dart:async';

import 'package:spooky_core/src/client/router.dart';
import 'package:spooky_core/src/client/runtime.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/kernel/interpreter.dart';
import 'package:spooky_core/src/kernel/saga.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/fake_adapters.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

({Runtime runtime, FakeAdapters fakes}) build({ClientState? state}) {
  final fakes = FakeAdapters();
  final runtime = Runtime(
    env: env(),
    adapters: Adapters(
      local: fakes.local,
      remote: fakes.remote,
      ssp: fakes.ssp,
      timers: fakes.timers,
      now: () => 1700000000000,
      mutationId: () => '_00_pending_mutations:m1',
      saltId: () => 'salt',
      hash: (s) => 'hash-${s.length}',
      services: fakes.services,
    ),
    logger: SpookyLogger.root('test'),
    clientId: 'client-a',
    initialState: state,
  );
  return (runtime: runtime, fakes: fakes);
}

void main() {
  test('a serial lane runs one saga at a time, in arrival order', () async {
    final rt = build().runtime;
    final order = <String>[];
    Saga<void> step(String name, Completer<void> gate) => (ctx) async {
          order.add('$name:start');
          await gate.future;
          order.add('$name:end');
        };
    final a = Completer<void>();
    final b = Completer<void>();
    final first = rt.run(step('a', a), lane: const Lane.serial('k'));
    final second = rt.run(step('b', b), lane: const Lane.serial('k'));
    await Future<void>.delayed(Duration.zero);
    expect(order, ['a:start']);
    a.complete();
    await first;
    await Future<void>.delayed(Duration.zero);
    expect(order, ['a:start', 'a:end', 'b:start']);
    b.complete();
    await second;
    expect(order, ['a:start', 'a:end', 'b:start', 'b:end']);
  });

  test('a failed serial run does not wedge the lane', () async {
    final rt = build().runtime;
    var ran = false;
    final failing = rt.run((ctx) async => throw StateError('boom'),
        lane: const Lane.serial('k'));
    await expectLater(failing, throwsA(isA<StateError>()));
    await rt.run((ctx) async => ran = true, lane: const Lane.serial('k'));
    expect(ran, isTrue);
  });

  test('a dedupe lane joins the run in flight instead of starting another',
      () async {
    final rt = build().runtime;
    var runs = 0;
    var gate = Completer<void>();
    Future<void> saga(Ctx ctx) async {
      runs++;
      await gate.future;
    }

    final first = rt.run(saga, lane: const Lane.dedupe('k'));
    final joined = rt.run(saga, lane: const Lane.dedupe('k'));
    expect(runs, 1);
    gate.complete();
    await Future.wait([first, joined]);
    expect(runs, 1);
    // Once it finishes, the lane is free again.
    gate = Completer<void>()..complete();
    await rt.run(saga, lane: const Lane.dedupe('k'));
    expect(runs, 2);
  });

  test('a dirty query schedules exactly one debounced materialization', () {
    final built = build(state: buildState([buildEntry()]));
    built.runtime.update(r.markDirty(['h1']));
    built.runtime.update(r.markDirty(['h1']));
    expect(built.fakes.timers.pending.keys, ['mat:h1']);
    expect(built.fakes.timers.pending['mat:h1']!.ms, 50);
  });

  test('status, activity and health are notified off state transitions', () {
    final built = build(state: buildState([buildEntry()]));
    final rt = built.runtime;
    final statuses = <QueryStatus>[];
    final activity = <String>[];
    final health = <SyncHealthStatus>[];
    rt.subscribeStatus('h1', statuses.add);
    rt.on('activity:changed', (e) {
      final a = e as ActivityChangedEvent;
      activity.add('${a.fetching}/${a.pending}');
    });
    rt.on('health:changed',
        (e) => health.add((e as HealthChangedEvent).health.status));

    rt.update(r.applyLifecycle('h1', const FetchBeginEvent()));
    rt.update(r.applyLifecycle('h1', const FetchEndEvent()));
    expect(statuses, [QueryStatus.fetching, QueryStatus.idle]);
    expect(activity, ['1/0', '0/0']);

    const degraded = SyncHealth(
        status: SyncHealthStatus.degraded,
        consecutiveFailures: 3,
        everConnected: true);
    rt.update(r.setHealth(degraded));
    rt.update(r.setHealth(degraded));
    expect(health, [SyncHealthStatus.degraded],
        reason: 'an unchanged health value must not re-notify');
  });

  test('unsynced ids notify when the set moves at an unchanged count', () {
    final built = build(state: buildState([buildEntry()]));
    final rt = built.runtime;
    final activity = <int>[];
    final unsynced = <List<String>>[];
    rt.on('activity:changed',
        (e) => activity.add((e as ActivityChangedEvent).pending));
    rt.on('unsynced:changed', (e) {
      unsynced.add((e as UnsyncedChangedEvent).recordIds.toList()..sort());
    });

    rt.update(r.outboxReplace([buildOutboxItem(id: '1', recordId: 'a:1')]));
    // One write acked while another is queued: the pending count stays 1.
    rt.update(r.outboxReplace([
      buildOutboxItem(
          id: '1',
          recordId: 'a:1',
          status: OutboxStatus.acked,
          ackedAt: 1),
      buildOutboxItem(id: '2', recordId: 'b:2'),
    ]));
    rt.update(r.outboxReplace(const []));

    expect(activity, [1, 0]);
    expect(unsynced, [
      ['a:1'],
      ['b:2'],
      <String>[],
    ]);
  });

  test('record and authority subscriptions fan out and refcount', () {
    final built = build(
        state: buildState([
      buildEntry(records: [
        {'id': 'thing:1'}
      ])
    ]));
    final rt = built.runtime;
    final seen = <List<Map<String, dynamic>>>[];
    final off = rt.subscribe('h1', seen.add, immediate: true);
    expect(seen, hasLength(1));
    expect(rt.state.queries['h1']!.subscribers, 1);

    final known = <bool>[];
    rt.subscribeAuthority('h1', known.add, immediate: true);
    expect(known, [false]);
    rt.emit(const QueryAuthorityEvent('h1', true));
    expect(known, [false, true]);

    off();
    expect(rt.state.queries['h1']!.subscribers, 0);
    expect(rt.state.queries['h1']!.lastSubscriberLeftAt, isNotNull);
    rt.emit(QueryRecordsEvent('h1', const []));
    expect(seen, hasLength(1), reason: 'an unsubscribed callback stays quiet');
  });

  test('a subscriber that throws does not take the runtime down', () {
    final built = build(state: buildState([buildEntry()]));
    built.runtime.subscribe('h1', (_) => throw StateError('ui exploded'));
    built.runtime.emit(QueryRecordsEvent('h1', const []));
  });

  test('waitFor resolves on the update that satisfies it', () async {
    final rt = build().runtime;
    var resumed = false;
    final waiting = rt.waitFor((s) => s.localReady).then((_) => resumed = true);
    await Future<void>.delayed(Duration.zero);
    expect(resumed, isFalse);
    rt.update(r.setIdentity(localReady: true));
    await waiting;
    expect(resumed, isTrue);
  });

  test('dispatch routes to the saga; a disposed runtime routes nothing',
      () async {
    final built = build();
    await built.runtime.dispatchAsync(const LiveStart());
    expect(built.runtime.state.sync.liveUuid, 'live-uuid');

    built.runtime.dispose();
    await built.runtime.dispatchAsync(const PollTick());
    expect(built.fakes.calls.where((c) => c.$1 == 'remote.query'), isEmpty);
  });

  test('dispose cancels every timer and rejects the waiters', () async {
    final built = build();
    final rt = built.runtime;
    final waiting = rt.waitFor((s) => s.localReady);
    rt.update(r.markDirty(['h1']));
    rt.dispose();
    expect(built.fakes.timers.pending, isEmpty);
    await expectLater(waiting, throwsA(isA<StateError>()));
  });

  test('the router has a target for every runtime event', () {
    for (final event in <RuntimeEvent>[
      const EnsureRegistered(),
      const RegisterRemote('h1'),
      const SyncOutcome(true),
      const AckPrune(),
      const ReadDirtyMembership(),
      const ReadMembership(['h1']),
      const FetchRows(),
      const Materialize('h1'),
      const MaterializeDirty(),
      const LifecycleTick(),
      const GcTick(),
      const Drain(),
      const FlushWrite('k'),
      const PollTick(),
      const SelfHealTick(),
      const HeartbeatNow(),
      const StartRemote(),
      const PrimeCircuit(),
      const VersionsPrimed([]),
      const AuthFlip(null),
      const BucketSwitch('u1'),
      const AppDetached(),
      const ConnectionChanged(ConnectionState.connected),
      const LiveStart(),
      const LiveChange([]),
    ]) {
      expect(route(env(), event).saga, isNotNull, reason: event.type);
    }
  });
}
