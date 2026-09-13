import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/sync/connection_saga.dart';
import 'package:spooky_core/src/sync/live_saga.dart';
import 'package:spooky_core/src/sync/poll_saga.dart';
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

const registered = QueryLifecycle(
    phase: QueryPhase.live,
    remote: RemotePhase.registered,
    fetchDepth: 0,
    notified: false);

void main() {
  group('pollTick', () {
    test('with no queries it probes and reports the round', () async {
      final ok = await runPure<void>(
        (ctx) => pollTick(ctx, env()),
        handlers: defaults(),
      );
      expect((ok.ofKind('remote.query').single as RemoteQuery).sql,
          'RETURN true');
      expect(ok.dispatched.whereType<SyncOutcome>().single.ok, isTrue);

      final down = await runPure<void>(
        (ctx) => pollTick(ctx, env()),
        handlers: defaults(over: {
          'remote.query': (_, __) => throw StateError('socket closed'),
        }),
      );
      expect(down.dispatched.whereType<SyncOutcome>().single.ok, isFalse);
    });

    test('a quiet round backs the cadence off; a change snaps it back',
        () async {
      final quiet = await runPure<void>(
        (ctx) => pollTick(ctx, env()),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: registered,
              serverState: ServerViewState.ready,
              remoteArray: [('thing:1', 1)])
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) =>
              snapshot(primary: [('thing:1', 1)], meta: readyMeta(1)),
        }),
      );
      expect(quiet.state.sync.pollIdleStreak, 1);
      expect(quiet.timers['poll']!.ms, 1000);

      final changed = await runPure<void>(
        (ctx) => pollTick(ctx, env()),
        state: r.patchSync(pollIdleStreak: 4)(quiet.state),
        handlers: defaults(over: {
          'remote.query': (_, __) =>
              snapshot(primary: [('thing:2', 1)], meta: readyMeta(1)),
        }),
      );
      expect(changed.state.sync.pollIdleStreak, 0);
      expect(changed.timers['poll']!.ms, 500);
    });

    test('an acked write still waiting for membership holds the base cadence',
        () async {
      final out = await runPure<void>(
        (ctx) => pollTick(ctx, env()),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: registered,
              serverState: ServerViewState.ready)
        ], [
          r.patchSync(pollIdleStreak: 3),
          r.outboxReplace([
            buildOutboxItem(
                id: 'm1', status: OutboxStatus.acked, ackedAt: 1)
          ]),
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) => snapshot(meta: readyMeta(0)),
        }),
      );
      expect(out.state.sync.pollIdleStreak, 0);
      expect(out.timers['poll']!.ms, 500);
    });
  });

  group('liveStart', () {
    test('subscribes on the session table and records the uuid', () async {
      final out = await runPure<void>(
        (ctx) => liveStart(ctx, env()),
        handlers: defaults(),
      );
      expect((out.ofKind('remote.live').single as RemoteLive).table,
          '_00_list_ref');
      expect(out.state.sync.liveUuid, 'live-uuid');
      expect(out.state.sync.liveTable, '_00_list_ref');
    });

    test('already on the right table: no-op', () async {
      final first = await runPure<void>(
        (ctx) => liveStart(ctx, env()),
        handlers: defaults(),
      );
      final again = await runPure<void>(
        (ctx) => liveStart(ctx, env()),
        state: first.state,
        handlers: defaults(),
      );
      expect(again.ofKind('remote.live'), isEmpty);
    });

    test('kills the previous subscription when connected, ignoring errors',
        () async {
      final state = r.setConnection(ConnectionState.connected)(
          r.patchSync(liveUuid: 'old', liveTable: '_00_list_ref_user_u1')(
              buildState()));
      final out = await runPure<void>(
        (ctx) => liveStart(ctx, env()),
        state: state,
        handlers: defaults(over: {
          'remote.kill': (_, __) => throw StateError('already gone'),
        }),
      );
      expect(out.ofKind('remote.kill'), hasLength(1));
      expect(out.state.sync.liveUuid, 'live-uuid');
    });

    test('a failing subscribe leaves the poll to cover membership', () async {
      final out = await runPure<void>(
        (ctx) => liveStart(ctx, env()),
        handlers: defaults(over: {
          'remote.live': (_, __) => throw StateError('no permission'),
        }),
      );
      expect(out.state.sync.liveUuid, isNull);
      expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.warn);
    });

    test('liveInvalidate clears the bookkeeping', () async {
      final out = await runPure<void>(
        liveInvalidate,
        state: r.patchSync(liveUuid: 'u', liveTable: 't')(buildState()),
        handlers: defaults(),
      );
      expect(out.state.sync.liveUuid, isNull);
      expect(out.state.sync.liveTable, isNull);
    });
  });

  group('liveChange', () {
    test('marks known hashes dirty and ignores the rest', () async {
      final out = await runPure<void>(
        (ctx) => liveChange(ctx, env(), ['a', 'nope'], null),
        state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
        handlers: defaults(),
      );
      expect(out.state.membershipDirty, {'a'});
      expect(out.timers['membership']!.ms, 50);
    });

    test('pulls a backed-off poll back to base, and leaves a base one alone',
        () async {
      final backedOff = await runPure<void>(
        (ctx) => liveChange(ctx, env(), ['a'], null),
        state: r.patchSync(pollIdleStreak: 5)(
            buildState([buildEntry(def: buildDefinition(hash: 'a'))])),
        handlers: defaults(),
      );
      expect(backedOff.state.sync.pollIdleStreak, 0);
      expect(backedOff.timers['poll']!.ms, 500);

      final atBase = await runPure<void>(
        (ctx) => liveChange(ctx, env(), ['a'], null),
        state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
        handlers: defaults(),
      );
      expect(atBase.timers.containsKey('poll'), isFalse,
          reason: 're-arming per event would starve the reconciliation poll');
    });

    test('lands a joined body so the fetch plan has nothing left to pull',
        () async {
      final out = await runPure<void>(
        (ctx) => liveChange(ctx, env(), ['a'], const [
              InlineRow(
                  id: 'thing:1',
                  version: 3,
                  record: {'id': 'thing:1', 'title': 'pushed'}),
            ]),
        state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
        handlers: defaults(),
      );
      expect(out.state.versions['thing:1'], 3);
      final tx = out.ofKind('local.tx').single as LocalTx;
      expect((tx.ops.single as PutOp).data['title'], 'pushed');
      expect(out.ofKind('ssp.ingest'), hasLength(1));
    });

    test('ignores a body already held at that version or newer', () async {
      final out = await runPure<void>(
        (ctx) => liveChange(ctx, env(), ['a'], const [
              InlineRow(id: 'thing:1', version: 2, record: {'id': 'thing:1'}),
            ]),
        state: r.setVersions([('thing:1', 3)])(
            buildState([buildEntry(def: buildDefinition(hash: 'a'))])),
        handlers: defaults(),
      );
      expect(out.ofKind('local.tx'), isEmpty);
    });
  });

  group('connectionChanged', () {
    test('a drop arms resubscribe and invalidates LIVE', () async {
      final out = await runPure<void>(
        (ctx) =>
            connectionChanged(ctx, env(), ConnectionState.disconnected),
        state: r.patchSync(liveUuid: 'u', liveTable: 't')(buildState()),
        handlers: defaults(),
      );
      expect(out.state.sync.needsResubscribe, isTrue);
      expect(out.state.sync.liveUuid, isNull);
      expect(out.state.sync.health.connection, ConnectionState.disconnected);
      expect(out.dispatched, isEmpty);
    });

    test('the initial connect does nothing beyond recording the state',
        () async {
      final out = await runPure<void>(
        (ctx) => connectionChanged(ctx, env(), ConnectionState.connected),
        handlers: defaults(),
      );
      expect(out.state.sync.health.connection, ConnectionState.connected);
      expect(out.dispatched, isEmpty);
    });

    test('a reconnect drops every registration and re-drives, once per window',
        () async {
      final dropped = await runPure<void>(
        (ctx) =>
            connectionChanged(ctx, env(), ConnectionState.disconnected),
        state: buildState([
          buildEntry(def: buildDefinition(hash: 'a'), lifecycle: registered)
        ]),
        handlers: defaults(),
      );
      final back = await runPure<void>(
        (ctx) => connectionChanged(ctx, env(), ConnectionState.connected),
        state: dropped.state,
        now: 100000,
        handlers: defaults(),
      );
      expect(back.state.queries['a']!.lifecycle.remote,
          RemotePhase.unregistered);
      expect(back.state.sync.needsResubscribe, isFalse);
      expect(back.dispatched.map((e) => e.type),
          ['EnsureRegistered', 'LiveStart', 'Drain']);
      expect(
          (back.dispatched.first as EnsureRegistered).requireAuth, isTrue);

      // A second reconnect inside the cooldown only clears the flag.
      final burst = await runPure<void>(
        (ctx) => connectionChanged(ctx, env(), ConnectionState.connected),
        state: r.patchSync(needsResubscribe: true)(back.state),
        now: 105000,
        handlers: defaults(),
      );
      expect(burst.dispatched, isEmpty);
      expect(burst.state.sync.needsResubscribe, isFalse);
    });
  });

  group('syncOutcome', () {
    test('degrades after the threshold and arms self-heal; recovery disarms it',
        () async {
      var state = buildState();
      for (var i = 0; i < 2; i++) {
        final out = await runPure<void>(
          (ctx) => syncOutcome(ctx, env(), false, StateError('socket closed')),
          state: state,
          handlers: defaults(),
        );
        state = out.state;
        expect(out.timers, isEmpty);
      }
      final degraded = await runPure<void>(
        (ctx) => syncOutcome(ctx, env(), false, StateError('socket closed')),
        state: state,
        handlers: defaults(),
      );
      expect(degraded.state.sync.health.status, SyncHealthStatus.degraded);
      expect(degraded.timers['heal']!.ms, 2000);

      final recovered = await runPure<void>(
        (ctx) => syncOutcome(ctx, env(), true, null),
        state: degraded.state,
        handlers: defaults(),
      );
      expect(recovered.state.sync.health.status, SyncHealthStatus.healthy);
      expect(recovered.state.sync.consecutiveFailures, 0);
      expect(recovered.timers, isEmpty);
      expect(recovered.log.any((e) => e.kind == 'timer.clear'), isTrue);
    });
  });

  group('selfHealTick', () {
    test('does nothing while healthy', () async {
      final out = await runPure<void>(
        (ctx) => selfHealTick(ctx, env()),
        handlers: defaults(),
      );
      expect(out.timers, isEmpty);
      expect(out.dispatched, isEmpty);
    });

    ClientState degraded([List<r.Reducer> extra = const []]) => r.compose([
          r.setHealth(const SyncHealth(
              status: SyncHealthStatus.degraded,
              consecutiveFailures: 3,
              everConnected: true)),
          ...extra,
        ])(buildState());

    test('drains first', () async {
      final out = await runPure<void>(
        (ctx) => selfHealTick(ctx, env()),
        state: degraded([
          r.outboxReplace([buildOutboxItem()])
        ]),
        handlers: defaults(),
      );
      expect(out.dispatched.single, isA<Drain>());
      expect(out.timers['heal']!.ms, 4000);
    });

    test('then re-registers, failed ones included', () async {
      final out = await runPure<void>(
        (ctx) => selfHealTick(ctx, env()),
        state: r.putQuery(buildEntry(
          def: buildDefinition(hash: 'a'),
          lifecycle: const QueryLifecycle(
              phase: QueryPhase.live,
              remote: RemotePhase.failed,
              fetchDepth: 0,
              notified: false),
        ))(degraded()),
        handlers: defaults(),
      );
      expect(out.state.queries['a']!.lifecycle.remote,
          RemotePhase.unregistered);
      expect((out.dispatched.single as EnsureRegistered).requireAuth, isTrue);
    });

    test('else probes, and always re-arms', () async {
      final out = await runPure<void>(
        (ctx) => selfHealTick(ctx, env()),
        state: degraded(),
        handlers: defaults(),
      );
      expect((out.ofKind('remote.query').single as RemoteQuery).sql,
          'RETURN true');
      expect(out.dispatched.whereType<SyncOutcome>().single.ok, isTrue);
      expect(out.timers['heal'], isNotNull);
    });
  });
}
