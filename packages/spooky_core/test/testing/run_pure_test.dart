import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/saga.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/reducers.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:test/test.dart';

void main() {
  group('runPure', () {
    test('handles state, clock, ids, hash, timers, emit, dispatch', () async {
      final out = await runPure<Map<String, Object?>>((ctx) async {
        final before = await ctx(Fx.stateRead((s) => s.failedCount));
        await ctx(Fx.stateUpdate(setFailedCount(3)));
        final after = await ctx(Fx.stateRead((s) => s.failedCount));
        final now = await ctx(Fx.now());
        final id1 = await ctx(Fx.id(IdScope.mutation));
        final id2 = await ctx(Fx.id(IdScope.salt));
        final hash = await ctx(Fx.hash('abc'));
        await ctx(Fx.timerSet('k', 10, const Drain()));
        await ctx(Fx.timerSet('k', 20, const Drain()));
        await ctx(Fx.timerSet('j', 5, const FetchRows()));
        await ctx(Fx.timerClear('j'));
        await ctx(Fx.emit(const TrayChangedEvent(1)));
        await ctx(Fx.dispatch(const FetchRows()));
        return {
          'before': before,
          'after': after,
          'now': now,
          'id1': id1,
          'id2': id2,
          'hash': hash,
        };
      }, now: 5);

      expect(out.result, {
        'before': 0,
        'after': 3,
        'now': 5,
        'id1': 'mutation-1',
        'id2': 'salt-2',
        'hash':
            'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
      });
      expect(out.state.failedCount, 3);
      expect(out.timers.keys.toList(), ['k']);
      expect(out.timers['k']!.ms, 20);
      expect(out.emitted.single, isA<TrayChangedEvent>());
      expect(out.dispatched.single, isA<FetchRows>());
      expect(out.log.first.kind, 'state.read');
    });

    test(
        'state.wait passes when the predicate holds and throws when it would block',
        () async {
      final ok = await runPure<String>((ctx) async {
        await ctx(Fx.stateWait((s) => s.failedCount == 0));
        return 'passed';
      });
      expect(ok.result, 'passed');

      expect(
        () => runPure<void>((ctx) async {
          await ctx(Fx.stateWait((s) => s.failedCount == 9));
        }),
        throwsA(isA<StateError>()),
      );
    });

    test('routes adapter effects to handlers and throws on unhandled ones',
        () async {
      Future<List<StatementResult>> saga(Ctx ctx) =>
          ctx(Fx.remoteQuery('RETURN true'));

      final out = await runPure<List<StatementResult>>(saga, handlers: {
        'remote.query': (_, __) => [const StatementResult.ok(true)],
      });
      expect(out.result.single.result, isTrue);

      expect(() => runPure<List<StatementResult>>(saga),
          throwsA(isA<UnhandledEffectError>()));
    });

    test('all returns settled results in order and keeps going after a failure',
        () async {
      final out = await runPure<List<Settled<Object?>>>(
        (ctx) => ctx(Fx.all([
          Fx.remoteQuery('A'),
          Fx.remoteQuery('B'),
          Fx.now(),
        ])),
        now: 1,
        handlers: {
          'remote.query': (e, _) {
            if ((e as RemoteQuery).sql == 'B') throw StateError('nope');
            return 'a';
          },
        },
      );
      expect(out.result, hasLength(3));
      expect(out.result[0].ok, isTrue);
      expect(out.result[0].value, 'a');
      expect(out.result[1].ok, isFalse);
      expect(out.result[1].error, isA<StateError>());
      expect(out.result[2].ok, isTrue);
      expect(out.result[2].value, 1);
    });

    test('bySqlPrefix answers by prefix and rejects unknown SQL', () {
      final handler = bySqlPrefix([
        ('SELECT', (_) => 'rows'),
        ('DELETE', (_) => 'gone'),
      ]);
      final ctx = PureContext(state: emptyState(tabId: 't'), now: 0);
      expect(handler(Fx.remoteQuery('SELECT * FROM x'), ctx), 'rows');
      expect(handler(Fx.remoteQuery('DELETE x'), ctx), 'gone');
      expect(() => handler(Fx.remoteQuery('UPDATE x'), ctx),
          throwsA(isA<StateError>()));
      expect(() => handler(Fx.now(), ctx), throwsA(isA<StateError>()));
    });
  });
}
