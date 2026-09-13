import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/query/register_saga.dart';
import 'package:spooky_core/src/query/sql.dart' as sql;
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

const input = RegisterInput(
  tableName: 'thing',
  surql: 'SELECT * FROM thing',
  params: {},
  ttl: '10m',
);

void main() {
  group('registerLocal', () {
    test('cold: no view row -> a cold entry, an SSP view, EnsureRegistered',
        () async {
      final out = await runPure<String>(
        (ctx) => registerLocal(ctx, env(), input),
        handlers: defaults(sspLocalArray: (_) => [('thing:1', 1)]),
      );
      final entry = out.state.queries[out.result]!;
      expect(entry.lifecycle.phase, QueryPhase.cold);
      expect(entry.localArray, [('thing:1', 1)]);
      expect(entry.remoteArray, isEmpty);
      expect(entry.def.tableName, 'thing');
      expect(entry.def.ttlMs, 600000);
      expect(entry.telemetry.registrationTimings.parseMs, 1);
      expect(out.state.registering, isEmpty);
      expect(out.dispatched.single, isA<EnsureRegistered>());
      // The durable row is read by (table, id), not by a SurrealQL string.
      final read = out.ofKind('local.get').single as LocalGet;
      expect(read.table, sql.viewTable);
    });

    test('cached: a durable view row seeds membership; unconfirmed empty stays cold',
        () async {
      final seeded = await runPure<String>(
        (ctx) => registerLocal(ctx, env(), input),
        handlers: defaults(over: {
          'local.get': (_, __) => {
                'ids': [
                  ['thing:1', 2]
                ],
                'confirmed': true,
              },
        }),
      );
      final entry = seeded.state.queries[seeded.result]!;
      expect(entry.lifecycle.phase, QueryPhase.cached);
      expect(entry.remoteArray, [('thing:1', 2)]);

      final unconfirmed = await runPure<String>(
        (ctx) => registerLocal(ctx, env(), input),
        handlers: defaults(over: {
          'local.get': (_, __) => {'ids': <dynamic>[], 'confirmed': false},
        }),
      );
      expect(
          unconfirmed.state.queries[unconfirmed.result]!.lifecycle.phase,
          QueryPhase.cold);

      final failedRead = await runPure<String>(
        (ctx) => registerLocal(ctx, env(), input),
        handlers: defaults(over: {
          'local.get': (_, __) => throw StateError('store is busy'),
        }),
      );
      expect(failedRead.state.queries[failedRead.result]!.lifecycle.phase,
          QueryPhase.cold);
    });

    test('dedupes: an active query returns immediately without registering',
        () async {
      // Pre-seed the entry under the hash this input produces.
      final first = await runPure<String>(
        (ctx) => registerLocal(ctx, env(), input),
        handlers: defaults(),
      );
      final second = await runPure<String>(
        (ctx) => registerLocal(ctx, env(), input),
        state: first.state,
        handlers: defaults(),
      );
      expect(second.result, first.result);
      expect(second.ofKind('ssp.register'), isEmpty);
    });

    test('a failing SSP registration clears the in-flight marker and rethrows',
        () async {
      await expectLater(
        runPure<String>(
          (ctx) => registerLocal(ctx, env(), input),
          handlers: defaults(over: {
            'ssp.register': (_, __) => throw StateError('circuit denied'),
          }),
        ),
        throwsA(isA<StateError>()),
      );
    });
  });

  group('ensureRegistered', () {
    test('dispatches RegisterRemote for unregistered queries only', () async {
      final state = buildState([
        buildEntry(def: buildDefinition(hash: 'a')),
        buildEntry(
            def: buildDefinition(hash: 'b'),
            lifecycle: const QueryLifecycle(
                phase: QueryPhase.live,
                remote: RemotePhase.registered,
                fetchDepth: 0,
                notified: false)),
      ]);
      final out = await runPure<void>(
        (ctx) => ensureRegistered(ctx, env()),
        state: state,
        handlers: defaults(),
      );
      expect(out.dispatched.map((e) => (e as RegisterRemote).hash), ['a']);
    });

    test('with requireAuth: probes \$auth.id, retries on a timer, then gives up',
        () async {
      final state = r.setIdentity(userId: 'user:u1')(
          buildState([buildEntry(def: buildDefinition(hash: 'a'))]));

      final blind = await runPure<void>(
        (ctx) => ensureRegistered(ctx, env(), requireAuth: true),
        state: state,
        handlers: defaults(over: {
          'remote.query': (_, __) => [const StatementResult.ok(null)],
        }),
      );
      expect(blind.dispatched, isEmpty);
      expect(blind.timers['ensure-registered']!.ms, 500);
      expect(
          (blind.timers['ensure-registered']!.event as EnsureRegistered).attempt,
          1);

      final exhausted = await runPure<void>(
        (ctx) => ensureRegistered(ctx, env(), requireAuth: true, attempt: 9),
        state: state,
        handlers: defaults(over: {
          'remote.query': (_, __) => throw StateError('offline'),
        }),
      );
      expect(exhausted.timers, isEmpty);
      expect(exhausted.emitted.whereType<LogEvent>().single.level,
          LogLevel.warn);

      final visible = await runPure<void>(
        (ctx) => ensureRegistered(ctx, env(), requireAuth: true),
        state: state,
        handlers: defaults(over: {
          'remote.query': (_, __) => [const StatementResult.ok('user:u1')],
        }),
      );
      expect(visible.dispatched.single, isA<RegisterRemote>());
    });

    test('an anonymous session skips the identity gate', () async {
      final out = await runPure<void>(
        (ctx) => ensureRegistered(ctx, env(), requireAuth: true),
        state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
        handlers: defaults(),
      );
      expect(out.ofKind('remote.query'), isEmpty);
      expect(out.dispatched.single, isA<RegisterRemote>());
    });
  });

  group('registerRemote', () {
    ClientState withQuery({
      QueryLifecycle? lifecycle,
      List<(String, int)> remoteArray = const [],
    }) =>
        buildState([
          buildEntry(
            def: buildDefinition(hash: 'a'),
            lifecycle: lifecycle,
            remoteArray: remoteArray,
          )
        ]);

    test('registers, applies membership and children, flips registered',
        () async {
      final out = await runPure<void>(
        (ctx) => registerRemote(ctx, env(), 'a'),
        state: withQuery(),
        handlers: defaults(over: {
          'remote.query': (_, __) => registerAnswer(
                primary: [('thing:1', 1)],
                meta: readyMeta(1),
                children: [('child:1', 1)],
              ),
        }),
      );
      final entry = out.state.queries['a']!;
      expect(entry.lifecycle.remote, RemotePhase.registered);
      expect(entry.lifecycle.phase, QueryPhase.live);
      expect(entry.remoteArray, [('thing:1', 1)]);
      expect(entry.subqueryRemoteArray, [('child:1', 1)]);
      expect(entry.lifecycle.fetchDepth, 0, reason: 'begin/end are balanced');
      expect(out.emitted.whereType<QueryAuthorityEvent>().single.known, isTrue);
      expect(out.dispatched.whereType<FetchRows>(), isNotEmpty);
      expect(out.dispatched.whereType<SyncOutcome>().last.ok, isTrue);
      // The durable view row was written.
      final put = out.ofKind('local.put').single as LocalPut;
      expect(put.table, sql.viewTable);
      expect(put.data['confirmed'], isTrue);
    });

    test('guards: missing entry, already registered, registering without retry',
        () async {
      final missing = await runPure<void>(
        (ctx) => registerRemote(ctx, env(), 'nope'),
        state: withQuery(),
        handlers: defaults(),
      );
      expect(missing.ofKind('remote.query'), isEmpty);

      for (final remote in [RemotePhase.registered, RemotePhase.failed]) {
        final out = await runPure<void>(
          (ctx) => registerRemote(ctx, env(), 'a'),
          state: withQuery(
              lifecycle: QueryLifecycle(
                  phase: QueryPhase.live,
                  remote: remote,
                  fetchDepth: 0,
                  notified: false)),
          handlers: defaults(),
        );
        expect(out.ofKind('remote.query'), isEmpty);
      }

      final inFlight = await runPure<void>(
        (ctx) => registerRemote(ctx, env(), 'a'),
        state: withQuery(
            lifecycle: const QueryLifecycle(
                phase: QueryPhase.cold,
                remote: RemotePhase.registering,
                fetchDepth: 0,
                notified: false)),
        handlers: defaults(),
      );
      expect(inFlight.ofKind('remote.query'), isEmpty);
    });

    test('a materializing answer schedules the re-read ladder', () async {
      final out = await runPure<void>(
        (ctx) => registerRemote(ctx, env(), 'a'),
        state: withQuery(),
        handlers: defaults(over: {
          'remote.query': (_, __) => registerAnswer(
                meta: {'rowCount': 3, 'state': 'materializing'},
              ),
        }),
      );
      expect(out.state.membershipDirty, contains('a'));
      expect(out.state.membershipReread['a'], 1);
      expect(out.timers['membership']!.ms, 150);
    });

    test('errors back off, and the budget exhausts to failed', () async {
      var out = await runPure<void>(
        (ctx) => registerRemote(ctx, env(), 'a'),
        state: withQuery(),
        handlers: defaults(over: {
          'remote.query': (_, __) => throw StateError('socket closed'),
        }),
      );
      expect(out.state.queries['a']!.registerAttempts, 1);
      // The phase stays `registering`: the armed retry passes `retry: true`,
      // which is what gets past the in-flight guard.
      expect(out.state.queries['a']!.lifecycle.remote, RemotePhase.registering);
      expect((out.timers['register:a']!.event as RegisterRemote).retry, isTrue);
      expect(out.timers['register:a']!.ms, 1000);
      expect(out.dispatched.whereType<SyncOutcome>().single.ok, isFalse);

      out = await runPure<void>(
        (ctx) => registerRemote(ctx, env(), 'a'),
        state: r.bumpRegisterAttempts('a')(
            r.bumpRegisterAttempts('a')(withQuery())),
        handlers: defaults(over: {
          'remote.query': (_, __) => throw StateError('socket closed'),
        }),
      );
      expect(out.state.queries['a']!.lifecycle.remote, RemotePhase.failed);
      expect(out.timers.containsKey('register:a'), isFalse);
    });

    test('a failed register statement is an error even with OK edges',
        () async {
      final out = await runPure<void>(
        (ctx) => registerRemote(ctx, env(), 'a'),
        state: withQuery(),
        handlers: defaults(over: {
          'remote.query': (_, __) => [
                const StatementResult.err('permission denied'),
                ...snapshot(),
              ],
        }),
      );
      expect(out.state.queries['a']!.registerAttempts, 1);
      expect(out.state.queries['a']!.remoteArray, isEmpty);
    });

    test('tolerates ERR meta and children: the edges still apply', () async {
      final out = await runPure<void>(
        (ctx) => registerRemote(ctx, env(), 'a'),
        state: withQuery(),
        handlers: defaults(over: {
          'remote.query': (_, __) => [
                const StatementResult.ok(null),
                StatementResult.ok(edges([('thing:1', 1)])),
                const StatementResult.err('no permission on _00_query'),
                const StatementResult.err('no children'),
              ],
        }),
      );
      expect(out.state.queries['a']!.remoteArray, [('thing:1', 1)]);
      expect(out.state.queries['a']!.serverState, isNull);
    });
  });
}
