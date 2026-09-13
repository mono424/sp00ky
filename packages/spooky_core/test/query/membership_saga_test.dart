import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/query/membership.dart';
import 'package:spooky_core/src/query/membership_saga.dart';
import 'package:spooky_core/src/query/sql.dart' as sql;
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

QueryLifecycle life(QueryPhase phase,
        {RemotePhase remote = RemotePhase.unregistered}) =>
    QueryLifecycle(
        phase: phase, remote: remote, fetchDepth: 0, notified: false);

void main() {
  group('applyMembership', () {
    test('is a no-op for an unknown hash', () async {
      final out = await runPure<MembershipOutcome>(
        (ctx) => applyMembership(ctx, 'nope', const []),
        handlers: defaults(),
      );
      expect(out.result, MembershipOutcome.ignored);
    });

    test('ignored: an unexplained empty set changes nothing', () async {
      final out = await runPure<MembershipOutcome>(
        (ctx) => applyMembership(ctx, 'a', const []),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'), lifecycle: life(QueryPhase.cold))
        ]),
        handlers: defaults(),
      );
      expect(out.result, MembershipOutcome.ignored);
      expect(out.emitted, isEmpty);
    });

    test('view-lost: flips once, drops the registration, re-registers',
        () async {
      final out = await runPure<MembershipOutcome>(
        (ctx) => applyMembership(ctx, 'a', const [],
            meta: const ServerViewMeta(present: false)),
        state: buildState([
          buildEntry(
            def: buildDefinition(hash: 'a'),
            lifecycle: life(QueryPhase.live, remote: RemotePhase.registered),
            remoteArray: [('thing:1', 1)],
          )
        ]),
        handlers: defaults(),
      );
      expect(out.result, MembershipOutcome.viewLost);
      expect(out.state.queries['a']!.lifecycle.phase, QueryPhase.viewLost);
      expect(out.state.queries['a']!.lifecycle.remote,
          RemotePhase.unregistered);
      expect(out.emitted.whereType<QueryViewLostEvent>(), hasLength(1));
      expect(out.dispatched.single, isA<EnsureRegistered>());
      // The rows are KEPT: a lost view is not an empty one.
      expect(out.state.queries['a']!.remoteArray, [('thing:1', 1)]);
    });

    test('applied: commits, writes the view row, flips authority once, fetches',
        () async {
      final state = buildState([
        buildEntry(
            def: buildDefinition(hash: 'a'), lifecycle: life(QueryPhase.cold))
      ], [
        r.outboxReplace([
          buildOutboxItem(
              id: 'm1',
              recordId: 'thing:1',
              status: OutboxStatus.acked,
              ackedAt: 1),
        ]),
      ]);
      final out = await runPure<MembershipOutcome>(
        (ctx) => applyMembership(ctx, 'a', [('thing:1', 2)],
            meta: ServerViewMeta(present: true, rowCount: 1, state: 'ready')),
        state: state,
        handlers: defaults(),
      );
      expect(out.result, MembershipOutcome.applied);
      expect(out.state.queries['a']!.lifecycle.phase, QueryPhase.live);
      expect(out.state.queries['a']!.serverState, ServerViewState.ready);
      expect(out.emitted.whereType<QueryAuthorityEvent>().single.known, isTrue);
      // Membership caught up with the acked write, so the overlay releases it.
      expect(out.state.outbox, isEmpty);
      final put = out.ofKind('local.put').single as LocalPut;
      expect(put.id, sql.viewRecordId('view-a'));
      expect(put.data['ids'], [
        ['thing:1', 2]
      ]);
      expect(out.dispatched.whereType<FetchRows>(), hasLength(1));
    });

    test('a verified removal applies an empty set with no server row', () async {
      final out = await runPure<MembershipOutcome>(
        (ctx) => applyMembership(ctx, 'a', const [], verifiedRemoval: true),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: life(QueryPhase.live),
              remoteArray: [('thing:1', 1)])
        ]),
        handlers: defaults(),
      );
      expect(out.result, MembershipOutcome.applied);
      expect(out.state.queries['a']!.remoteArray, isEmpty);
    });

    test('a failing view-row write is logged, not fatal', () async {
      final out = await runPure<MembershipOutcome>(
        (ctx) => applyMembership(ctx, 'a', [('thing:1', 1)]),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'), lifecycle: life(QueryPhase.cold))
        ]),
        handlers: defaults(over: {
          'local.put': (_, __) => throw StateError('disk full'),
        }),
      );
      expect(out.result, MembershipOutcome.applied);
      expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.debug);
    });
  });

  group('applySubqueryChildren', () {
    test('no-op when equal or unknown; otherwise sets and asks for bodies',
        () async {
      final state = buildState([
        buildEntry(
            def: buildDefinition(hash: 'a'),
            subqueryRemoteArray: [('child:1', 1)])
      ]);
      final same = await runPure<void>(
        (ctx) => applySubqueryChildren(ctx, 'a', [('child:1', 1)]),
        state: state,
        handlers: defaults(),
      );
      expect(same.dispatched, isEmpty);

      final changed = await runPure<void>(
        (ctx) => applySubqueryChildren(ctx, 'a', [('child:2', 1)]),
        state: state,
        handlers: defaults(),
      );
      expect(changed.state.queries['a']!.subqueryRemoteArray,
          [('child:2', 1)]);
      expect(changed.dispatched.single, isA<FetchRows>());
    });
  });

  group('readMembership', () {
    test('nothing to read', () async {
      final out = await runPure<MembershipRead>(
        (ctx) => readMembership(ctx, env(), ['nope']),
        handlers: defaults(),
      );
      expect(out.result.changed, isFalse);
      expect(out.ofKind('remote.query'), isEmpty);
    });

    test('a single query uses the single-shape request and clears its dirt',
        () async {
      final out = await runPure<MembershipRead>(
        (ctx) => readMembership(ctx, env(), ['a'], force: true),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'), lifecycle: life(QueryPhase.cold))
        ], [
          r.markMembershipDirty(['a'])
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) =>
              snapshot(primary: [('thing:1', 1)], meta: readyMeta(1)),
        }),
      );
      expect(out.result.changed, isTrue);
      expect(out.result.failed, isFalse);
      final sent = out.ofKind('remote.query').single as RemoteQuery;
      expect(sent.sql.split(';\n'), hasLength(3));
      expect(out.state.membershipDirty, isEmpty);
      expect(out.state.queries['a']!.lastPolledAt, isNotNull);
      expect(out.dispatched.whereType<SyncOutcome>().last.ok, isTrue);
    });

    test('an equal set on a live query applies nothing; a cached one still flips',
        () async {
      final live = await runPure<MembershipRead>(
        (ctx) => readMembership(ctx, env(), ['a'], force: true),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: life(QueryPhase.live),
              remoteArray: [('thing:1', 1)],
              serverState: ServerViewState.ready)
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) =>
              snapshot(primary: [('thing:1', 1)], meta: readyMeta(1)),
        }),
      );
      expect(live.result.changed, isFalse);
      expect(live.ofKind('local.put'), isEmpty);

      final cached = await runPure<MembershipRead>(
        (ctx) => readMembership(ctx, env(), ['a'], force: true),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: life(QueryPhase.cached),
              remoteArray: [('thing:1', 1)])
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) =>
              snapshot(primary: [('thing:1', 1)], meta: readyMeta(1)),
        }),
      );
      expect(cached.result.changed, isTrue);
      expect(cached.state.queries['a']!.lifecycle.phase, QueryPhase.live);
    });

    test('many queries share one batch request; suspects are re-read singly',
        () async {
      var calls = 0;
      final out = await runPure<MembershipRead>(
        (ctx) => readMembership(ctx, env(), ['a', 'b'], force: true),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: life(QueryPhase.live),
              remoteArray: [('thing:1', 1)]),
          buildEntry(
              def: buildDefinition(hash: 'b'),
              lifecycle: life(QueryPhase.live),
              remoteArray: [('thing:2', 1)]),
        ]),
        handlers: defaults(over: {
          'remote.query': (e, __) {
            calls++;
            final sql = (e as RemoteQuery).sql;
            if (sql.contains('in IN \$ins')) {
              // `b`'s `_00_query` row did not come back: suspect.
              return [
                StatementResult.ok([
                  {
                    'in': '_00_query:a',
                    'out': 'thing:1',
                    'version': 2,
                  }
                ]),
                StatementResult.ok([
                  {'id': '_00_query:a', 'rowCount': 1, 'state': 'ready'}
                ]),
              ];
            }
            return snapshot(primary: [('thing:2', 3)], meta: readyMeta(1));
          },
        }),
      );
      expect(calls, 2, reason: 'one batch plus one single re-read');
      expect(out.state.queries['a']!.remoteArray, [('thing:1', 2)]);
      expect(out.state.queries['b']!.remoteArray, [('thing:2', 3)]);
    });

    test('a failed chunk reports failed', () async {
      final out = await runPure<MembershipRead>(
        (ctx) => readMembership(ctx, env(), ['a'], force: true),
        state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
        handlers: defaults(over: {
          'remote.query': (_, __) => throw StateError('socket closed'),
        }),
      );
      expect(out.result.failed, isTrue);
      expect(out.dispatched.whereType<SyncOutcome>().last.ok, isFalse);
    });

    test('a non-array primary statement is skipped, not believed', () async {
      final out = await runPure<MembershipRead>(
        (ctx) => readMembership(ctx, env(), ['a'], force: true),
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: life(QueryPhase.live),
              remoteArray: [('thing:1', 1)])
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) => [
                const StatementResult.ok(null),
                const StatementResult.ok(null),
                const StatementResult.ok(null),
              ],
        }),
      );
      expect(out.state.queries['a']!.remoteArray, [('thing:1', 1)]);
    });

    test('materializing views walk the re-read ladder then stop', () async {
      Future<RunPureResult<MembershipRead>> tick(int attempt) => runPure(
            (ctx) => readMembership(ctx, env(), ['a'], force: true),
            state: r.setMembershipReread('a', attempt)(buildState([
              buildEntry(
                  def: buildDefinition(hash: 'a'),
                  lifecycle: life(QueryPhase.live),
                  remoteArray: [('thing:1', 1)])
            ])),
            handlers: defaults(over: {
              'remote.query': (_, __) => snapshot(
                  meta: {'rowCount': 1, 'state': 'materializing'}),
            }),
          );
      expect((await tick(0)).timers['membership']!.ms, 150);
      expect((await tick(1)).timers['membership']!.ms, 400);
      expect((await tick(2)).timers['membership']!.ms, 1000);
      final exhausted = await tick(3);
      expect(exhausted.timers.containsKey('membership'), isFalse);
      expect(exhausted.state.membershipReread, isEmpty);
    });
  });

  group('membership dirt', () {
    test('the coalesce window is armed once per burst', () async {
      final state = buildState([buildEntry(def: buildDefinition(hash: 'a'))]);
      final first = await runPure<void>(
        (ctx) => markMembershipDirty(ctx, ['a']),
        state: state,
        handlers: defaults(),
      );
      expect(first.timers['membership']!.ms, 50);

      final second = await runPure<void>(
        (ctx) => markMembershipDirty(ctx, ['a']),
        state: first.state,
        handlers: defaults(),
      );
      expect(second.timers, isEmpty,
          reason: 're-arming per event would push the read out forever');
    });

    test('unknown hashes never arm the window', () async {
      final out = await runPure<void>(
        (ctx) => markMembershipDirty(ctx, ['nope']),
        handlers: defaults(),
      );
      expect(out.state.membershipDirty, isEmpty);
      expect(out.timers['membership'], isNotNull,
          reason: 'the window arms on the empty -> non-empty edge only');
    });

    test('readDirtyMembership is a no-op when clean, and re-arms for leftovers',
        () async {
      final clean = await runPure<void>(
        (ctx) => readDirtyMembership(ctx, env()),
        handlers: defaults(),
      );
      expect(clean.ofKind('remote.query'), isEmpty);

      // 'b' is dirty but unknown, so the read cannot clear it.
      final left = await runPure<void>(
        (ctx) => readDirtyMembership(ctx, env()),
        state: buildState([
          buildEntry(def: buildDefinition(hash: 'a'))
        ], [
          r.markMembershipDirty(['a']),
          (s) => s.copyWith(membershipDirty: {...s.membershipDirty, 'b'}),
        ]),
        handlers: defaults(over: {
          'remote.query': (_, __) => snapshot(meta: readyMeta(0)),
        }),
      );
      expect(left.state.membershipDirty, {'b'});
      expect(left.timers['membership']!.ms, 50);
    });
  });
}
