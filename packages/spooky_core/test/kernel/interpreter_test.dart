import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/kernel/interpreter.dart';
import 'package:spooky_core/src/state/reducers.dart';
import 'package:spooky_core/src/testing/fake_adapters.dart';
import 'package:test/test.dart';

void main() {
  test('local effects reach the store and read back', () async {
    final fakes = FakeAdapters();
    final host = FakeHost();
    final ctx = Interpreter(fakes.build(), host);

    await ctx(Fx.localPut('thing', 'thing:1', {'a': 1}));
    expect(await ctx(Fx.localGet('thing', 'thing:1')), {'a': 1});
    await ctx(Fx.localPut('thing', 'thing:1', {'b': 2}, mode: WriteMode.merge));
    expect(await ctx(Fx.localGet('thing', 'thing:1')), {'a': 1, 'b': 2});
    expect(await ctx(Fx.localGetAll('thing')), hasLength(1));
    expect(await ctx(Fx.localGetMany('thing', ['thing:1', 'thing:missing'])),
        hasLength(1));
    await ctx(Fx.localDelete('thing', 'thing:1'));
    expect(await ctx(Fx.localGet('thing', 'thing:1')), isNull);
  });

  test('a transaction applies every op in order', () async {
    final fakes = FakeAdapters();
    final ctx = Interpreter(fakes.build(), FakeHost());
    await ctx(Fx.localTx([
      const PutOp('thing', 'thing:1', {'a': 1, '_00_rv': 1}),
      const BumpRvOp('thing', 'thing:1'),
      const PutOp('_00_pending_mutations', 'm1', {'x': 1}),
      const DeleteOp('_00_pending_mutations', 'm1'),
    ]));
    expect((await ctx(Fx.localGet('thing', 'thing:1')))!['_00_rv'], 2);
    expect(await ctx(Fx.localGet('_00_pending_mutations', 'm1')), isNull);
  });

  test('a write fenced with a stale epoch is dropped', () async {
    final fakes = FakeAdapters();
    final ctx = Interpreter(fakes.build(), FakeHost());
    final epoch = await ctx(Fx.localEpoch());
    fakes.local.bumpEpoch();
    await ctx(Fx.localPut('thing', 'thing:1', {'a': 1}, epoch: epoch));
    await ctx(Fx.localTx([const PutOp('thing', 'thing:2', {})], epoch: epoch));
    await ctx(Fx.localDelete('thing', 'thing:3', epoch: epoch));
    expect(await ctx(Fx.localGetAll('thing')), isEmpty);
    // Unfenced, or fenced with the current epoch, still lands.
    await ctx(Fx.localPut('thing', 'thing:1', {'a': 1}));
    await ctx(Fx.localPut('thing', 'thing:2', {'a': 1},
        epoch: fakes.local.epoch));
    expect(await ctx(Fx.localGetAll('thing')), hasLength(2));
  });

  test('a remote request can carry a deadline', () async {
    final fakes = FakeAdapters(
      remote: FakeRemotePort((_, __) {}, answer: (sql, vars) async {
        if (sql == 'SLOW') return Future.delayed(const Duration(seconds: 5), () => const <StatementResult>[]);
        return const [StatementResult.ok(true)];
      }),
    );
    final ctx = Interpreter(fakes.build(), FakeHost());
    expect((await ctx(Fx.remoteQuery('FAST'))).single.result, isTrue);
    expect(() => ctx(Fx.remoteQuery('SLOW', timeoutMs: 10)),
        throwsA(isA<Exception>()));
  });

  test('a live subscription dispatches its changes as events', () async {
    final fakes = FakeAdapters();
    final host = FakeHost();
    final ctx = Interpreter(fakes.build(), host);
    expect(await ctx(Fx.remoteLive('_00_list_ref')), 'live-uuid');
    fakes.remote.onLiveChange!(['h1'], null);
    expect(host.dispatched.single, isA<LiveChange>());
    expect((host.dispatched.single as LiveChange).hashes, ['h1']);
    await ctx(Fx.remoteKill('live-uuid'));
    expect(fakes.remote.killed, ['live-uuid']);
  });

  test('state effects read, update and suspend', () async {
    final fakes = FakeAdapters();
    final host = FakeHost();
    final ctx = Interpreter(fakes.build(), host);
    expect(await ctx(Fx.stateRead((s) => s.failedCount)), 0);
    await ctx(Fx.stateUpdate(setFailedCount(2)));
    expect(host.state.failedCount, 2);

    var resumed = false;
    final waiting = ctx(Fx.stateWait((s) => s.failedCount == 5))
        .then((_) => resumed = true);
    await Future<void>.delayed(Duration.zero);
    expect(resumed, isFalse);
    await ctx(Fx.stateUpdate(setFailedCount(5)));
    await waiting;
    expect(resumed, isTrue);
  });

  test('timers dispatch their event when they fire', () async {
    final fakes = FakeAdapters();
    final host = FakeHost();
    final ctx = Interpreter(fakes.build(), host);
    await ctx(Fx.timerSet('poll', 500, const PollTick()));
    expect(fakes.timers.pending['poll']!.ms, 500);
    await ctx(Fx.timerSet('drop', 1, const Drain()));
    await ctx(Fx.timerClear('drop'));
    fakes.timers.fireAll();
    expect(host.dispatched.single, isA<PollTick>());
  });

  test('all fans out and keeps every outcome, in order', () async {
    final fakes = FakeAdapters(
      remote: FakeRemotePort((_, __) {}, answer: (sql, _) {
        if (sql == 'BAD') throw StateError('nope');
        return const [StatementResult.ok('ok')];
      }),
    );
    final ctx = Interpreter(fakes.build(), FakeHost());
    final out = await ctx(Fx.all([
      Fx.remoteQuery('GOOD'),
      Fx.remoteQuery('BAD'),
      Fx.now(),
    ]));
    expect(out[0].ok, isTrue);
    expect(out[1].ok, isFalse);
    expect(out[1].error, isA<StateError>());
    expect(out[2].value, 1700000000000);
  });

  test('service calls are dispatched by name with their arguments', () async {
    final fakes = FakeAdapters(services: {
      ServiceName.authRestoreSession: (_) => 'user:u1',
      ServiceName.hintRead: (_) => 'bucket-a',
    });
    final ctx = Interpreter(fakes.build(), FakeHost());
    expect(await ctx(Fx.service<String?>(ServiceName.authRestoreSession)),
        'user:u1');
    expect(await ctx(Fx.service<String?>(ServiceName.hintRead)), 'bucket-a');
    await ctx(Fx.service<void>(
        ServiceName.sspSetSessionAuth, ['user:u1', 'account']));
    expect(fakes.names(), contains('service.sspSetSessionAuth'));
    expect(
      fakes.calls.firstWhere((c) => c.$1 == 'service.sspSetSessionAuth').$2,
      ['user:u1', 'account'],
    );
  });

  test('ssp effects reach the circuit', () async {
    final fakes = FakeAdapters(
      ssp: FakeSsp((_, __) {}, localArrayFor: (h) => [('thing:1', 1)]),
    );
    final ctx = Interpreter(fakes.build(), FakeHost());
    final reg = await ctx(Fx.sspRegister(const RegisterPlan(
      queryHash: 'h1',
      surql: 'SELECT * FROM thing',
      params: {},
      ttl: '10m',
      tableName: 'thing',
    )));
    expect(reg.localArray, [('thing:1', 1)]);
    await ctx(Fx.sspUnregister('h1'));
    expect(fakes.ssp.unregistered, ['h1']);
  });

  test('emit reaches the host, hash and ids are deterministic ports', () async {
    final fakes = FakeAdapters();
    final host = FakeHost();
    final ctx = Interpreter(fakes.build(), host);
    await ctx(Fx.emit(const TrayChangedEvent(3)));
    expect((host.emitted.single as TrayChangedEvent).count, 3);
    expect(await ctx(Fx.hash('abc')),
        'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
    expect(await ctx(Fx.id(IdScope.salt)), 'salt-1');
    expect(await ctx(Fx.now()), 1700000000000);
    fakes.advance(5);
    expect(await ctx(Fx.now()), 1700000000005);
  });
}
