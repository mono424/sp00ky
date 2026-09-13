import 'package:spooky_core/src/boot/auth_flip_saga.dart';
import 'package:spooky_core/src/boot/boot_saga.dart';
import 'package:spooky_core/src/boot/bucket_switch_saga.dart';
import 'package:spooky_core/src/boot/preload_saga.dart';
import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/query/register_saga.dart';
import 'package:spooky_core/src/query/sql.dart' as sql;
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

/// A handler that answers `service` calls by name.
EffectHandler services(Map<ServiceName, Object? Function()> answers) =>
    (e, __) => answers[(e as ServiceEffect).name]?.call();

List<ServiceName> serviceNames(RunPureResult<void> out) =>
    [for (final e in out.ofKind('service')) (e as ServiceEffect).name];

void main() {
  group('boot', () {
    test('opens the store, primes, restores the session, then goes remote',
        () async {
      final out = await runPure<void>(
        (ctx) => boot(ctx, env()),
        handlers: defaults(over: {
          'service': services({
            ServiceName.hintRead: () => 'u1',
            ServiceName.authRestoreSession: () => 'user:u1',
            ServiceName.authSessionAuthId: () => 'user:u1',
            ServiceName.authAccess: () => 'account',
          }),
        }),
      );
      final names = serviceNames(out);
      expect(
        names,
        containsAllInOrder([
          ServiceName.hintRead,
          ServiceName.localConnect,
          ServiceName.migratorProvision,
          ServiceName.sspInit,
          ServiceName.sspSetPermissions,
          ServiceName.authRestoreSession,
        ]),
      );
      expect(out.state.bucketId, 'u1');
      expect(out.state.userId, 'user:u1');
      expect(out.state.sessionId, isNotNull);
      expect(out.state.saltUserId, 'user:u1');
      expect(names, contains(ServiceName.sspSetSessionAuth));
      expect(out.state.localReady, isTrue);
      expect(out.dispatched.map((e) => e.type), ['LifecycleTick', 'GcTick']);
    });

    test('no hint and no session: the anon bucket, no identity seeded',
        () async {
      final out = await runPure<void>(
        (ctx) => boot(ctx, env()),
        handlers: defaults(),
      );
      expect(out.state.bucketId, 'anon');
      expect(out.state.userId, isNull);
      expect(serviceNames(out), isNot(contains(ServiceName.sspSetSessionAuth)));
    });

    test('the legacy window table is copied into _00_view exactly once',
        () async {
      final legacyRow = {
        'id': '_00_window:k1',
        'ids': [
          ['thing:1', 2]
        ],
        'confirmed': true,
        'updatedAt': 7,
      };
      final migrated = await runPure<void>(
        migrateWindowToView,
        handlers: defaults(over: {
          'local.getAll': (e, __) =>
              (e as LocalGetAll).table == sql.legacyViewTable
                  ? [legacyRow]
                  : <Map<String, dynamic>>[],
        }),
      );
      final put = migrated.ofKind('local.put').single as LocalPut;
      expect(put.id, sql.viewRecordId('k1'));
      expect(put.data['confirmed'], isTrue);

      final alreadyDone = await runPure<void>(
        migrateWindowToView,
        handlers: defaults(over: {
          'local.getAll': (_, __) => [
                {'id': '_00_view:k1', 'ids': <dynamic>[]}
              ],
        }),
      );
      expect(alreadyDone.ofKind('local.put'), isEmpty);
    });

    test('a failing migration is skipped, not fatal', () async {
      final out = await runPure<void>(
        migrateWindowToView,
        handlers: defaults(over: {
          'local.getAll': (_, __) => throw StateError('no such table'),
        }),
      );
      expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.debug);
    });
  });

  group('startRemote', () {
    test('every step is best-effort and the engine still starts', () async {
      final out = await runPure<void>(
        startRemote,
        handlers: defaults(over: {
          'service': (e, __) {
            final name = (e as ServiceEffect).name;
            if (name == ServiceName.remoteConnect ||
                name == ServiceName.authInit) {
              throw StateError('offline');
            }
            return null;
          },
        }),
      );
      expect(out.emitted.whereType<LogEvent>(), hasLength(2));
      expect(serviceNames(out), contains(ServiceName.supervisorStart));
      expect(out.dispatched.map((e) => e.type),
          ['EnsureRegistered', 'LiveStart', 'PollTick', 'Drain']);
    });
  });

  group('primeCircuit', () {
    test('passes the pending ids and flips primed even when it fails',
        () async {
      final ok = await runPure<void>(
        primeCircuit,
        state: buildState([], [
          r.outboxReplace([buildOutboxItem(recordId: 'thing:1')])
        ]),
        handlers: defaults(),
      );
      expect(ok.state.primed, isTrue);
      expect((ok.ofKind('service').single as ServiceEffect).args.single,
          ['thing:1']);

      final failed = await runPure<void>(
        primeCircuit,
        handlers: defaults(over: {
          'service': (_, __) => throw StateError('no snapshot'),
        }),
      );
      expect(failed.state.primed, isTrue,
          reason: 'a fetch that waits on `primed` must never hang');
      expect(failed.emitted.whereType<LogEvent>().single.level, LogLevel.warn);
    });
  });

  group('appDetached', () {
    test('hands back only the views the server actually holds', () async {
      final out = await runPure<void>(
        appDetached,
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: const QueryLifecycle(
                  phase: QueryPhase.live,
                  remote: RemotePhase.registered,
                  fetchDepth: 0,
                  notified: false)),
          buildEntry(def: buildDefinition(hash: 'b')),
        ]),
        handlers: defaults(),
      );
      final call = out.ofKind('service').single as ServiceEffect;
      expect(call.name, ServiceName.remoteReleaseViews);
      expect(call.args.single as List, hasLength(1));
    });

    test('nothing registered: no call', () async {
      final out = await runPure<void>(appDetached, handlers: defaults());
      expect(out.ofKind('service'), isEmpty);
    });
  });

  group('authFlip', () {
    test('sets identity and auth first, then switches bucket and rotates salt',
        () async {
      final out = await runPure<void>(
        (ctx) => authFlip(ctx, env(), 'user:u1'),
        handlers: defaults(over: {
          'service': services({
            ServiceName.authSessionAuthId: () => 'user:u1',
            ServiceName.authAccess: () => 'account',
            ServiceName.localCurrentBucketId: () => 'anon',
          }),
        }),
      );
      expect(
        serviceNames(out),
        containsAllInOrder([
          ServiceName.sspSetSessionAuth,
          ServiceName.hintWrite,
          ServiceName.localSwitchStore,
          ServiceName.crdtSetSessionId,
        ]),
      );
      expect(out.state.userId, 'user:u1');
      expect(out.state.bucketId, 'u1');
      expect(out.state.saltUserId, 'user:u1');
      expect(out.state.sessionId, isNotNull);
    });

    test('the same principal on the same bucket switches nothing', () async {
      final out = await runPure<void>(
        (ctx) => authFlip(ctx, env(), 'user:u1'),
        state: r.setIdentity(saltUserId: 'user:u1')(buildState()),
        handlers: defaults(over: {
          'service': services({
            ServiceName.authSessionAuthId: () => 'user:u1',
            ServiceName.localCurrentBucketId: () => 'u1',
          }),
        }),
      );
      expect(serviceNames(out), isNot(contains(ServiceName.localSwitchStore)));
      expect(serviceNames(out), isNot(contains(ServiceName.crdtSetSessionId)));
      expect(out.state.sessionId, isNull);
    });
  });

  group('persistVerifiedUser', () {
    test('writes the server row into the local store and dirties its table',
        () async {
      final out = await runPure<void>(
        (ctx) => persistVerifiedUser(
            ctx, env(schema: {...testSchema, 'user': <String, dynamic>{}})),
        state: buildState(
            [buildEntry(def: buildDefinition(hash: 'a', tableName: 'user'))]),
        handlers: defaults(over: {
          'service': services({
            ServiceName.authCurrentUser: () =>
                {'id': 'user:u1', 'email_verified': true},
          }),
        }),
      );
      final tx = out.ofKind('local.tx').single as LocalTx;
      expect((tx.ops.single as PutOp).table, 'user');
      expect(out.state.versions['user:u1'], 1);
      expect(out.state.dirty, contains('a'));
    });

    test('skips a bare id, a missing row, or an unknown table', () async {
      for (final row in <Map<String, dynamic>?>[
        null,
        {'id': 'user:u1'},
        {'id': 'ghost:1', 'x': 1},
      ]) {
        final out = await runPure<void>(
          (ctx) => persistVerifiedUser(ctx, env()),
          handlers: defaults(over: {
            'service': services({ServiceName.authCurrentUser: () => row}),
          }),
        );
        expect(out.ofKind('local.tx'), isEmpty, reason: '$row');
      }
    });
  });

  group('bucketSwitch', () {
    test('steps aside when a newer target won the race', () async {
      final out = await runPure<void>(
        (ctx) => bucketSwitch(ctx, env(), 'u1'),
        state: r.setIdentity(pendingBucket: 'u2')(buildState()),
        handlers: defaults(over: {
          'service': services({ServiceName.localCurrentBucketId: () => 'anon'}),
        }),
      );
      expect(serviceNames(out), [ServiceName.localCurrentBucketId]);
    });

    test('clears the per-bucket slices, swaps the store and re-drives',
        () async {
      final out = await runPure<void>(
        (ctx) => bucketSwitch(ctx, env(), 'u1'),
        state: r.compose([
          r.setIdentity(pendingBucket: 'u1'),
          r.setVersions([('thing:1', 1)]),
          r.outboxReplace([buildOutboxItem()]),
        ])(buildState()),
        handlers: defaults(over: {
          'service': services({
            ServiceName.localCurrentBucketId: () => 'anon',
            ServiceName.authToken: () => 'jwt',
          }),
        }),
      );
      expect(out.state.versions, isEmpty);
      expect(out.state.bucketId, 'u1');
      expect(
        serviceNames(out),
        containsAllInOrder([
          ServiceName.crdtCloseAll,
          ServiceName.localSwitchStore,
          ServiceName.migratorProvision,
          ServiceName.sspReset,
          ServiceName.sspSetPermissions,
          ServiceName.persistenceSet,
        ]),
      );
      expect(
          out.dispatched.map((e) => e.type),
          containsAllInOrder(
              ['EnsureRegistered', 'LiveStart', 'PollTick', 'Drain']));
      // The poll timer is cleared by the switch, so only the tick re-arms it.
      expect(out.log.where((e) => e.kind == 'timer.clear'), hasLength(5));
    });

    test('rebindQueries re-seeds every query from the new store', () async {
      final out = await runPure<void>(
        rebindQueries,
        state: buildState([
          buildEntry(
              def: buildDefinition(hash: 'a'),
              lifecycle: const QueryLifecycle(
                  phase: QueryPhase.live,
                  remote: RemotePhase.registered,
                  fetchDepth: 0,
                  notified: true),
              remoteArray: [
                ('thing:old', 1)
              ],
              records: [
                {'id': 'thing:old'}
              ]),
        ]),
        handlers: defaults(
          sspLocalArray: (_) => [('thing:new', 1)],
          over: {
            'local.get': (_, __) => {
                  'ids': [
                    ['thing:new', 1]
                  ],
                  'confirmed': true,
                },
          },
        ),
      );
      final entry = out.state.queries['a']!;
      expect(entry.def.hash, 'a', reason: 'the hash is the identity');
      expect(entry.remoteArray, [('thing:new', 1)]);
      expect(entry.localArray, [('thing:new', 1)]);
      expect(entry.records, isEmpty);
      expect(entry.lifecycle.phase, QueryPhase.cached);
      expect(entry.lifecycle.remote, RemotePhase.unregistered);
    });

    test('a failing local view rebuild leaves the query with an empty window',
        () async {
      final out = await runPure<void>(
        rebindQueries,
        state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
        handlers: defaults(over: {
          'ssp.register': (_, __) => throw StateError('circuit denied'),
        }),
      );
      expect(out.state.queries['a']!.localArray, isEmpty);
      expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.warn);
    });
  });

  group('preload', () {
    const input = RegisterInput(
      tableName: 'thing',
      surql: 'SELECT * FROM thing',
      params: {},
      ttl: '10m',
    );

    test('a query resolved before returns as soon as the entry exists',
        () async {
      final out = await runPure<({String hash, bool waited})>(
        (ctx) => preload(ctx, env(), input),
        handlers: defaults(over: {
          'local.get': (_, __) => {'ids': <dynamic>[], 'confirmed': true},
        }),
      );
      expect(out.result.waited, isFalse);
    });

    test('a cold query blocks on the settle gate', () async {
      // `state.wait` cannot block under runPure, so reaching it is the proof.
      await expectLater(
        runPure<({String hash, bool waited})>(
          (ctx) => preload(ctx, env(), input),
          handlers: defaults(),
        ),
        throwsA(isA<StateError>()),
      );
    });
  });
}
