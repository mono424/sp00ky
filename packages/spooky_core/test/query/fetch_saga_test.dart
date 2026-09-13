import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/query/fetch_saga.dart';
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

ClientState primed(List<QueryEntry> entries, [List<r.Reducer> extra = const []]) =>
    r.setIdentity(primed: true)(buildState(entries, extra));

QueryEntry liveQuery(String hash, List<(String, int)> remoteArray) => buildEntry(
      def: buildDefinition(hash: hash),
      lifecycle: const QueryLifecycle(
          phase: QueryPhase.live,
          remote: RemotePhase.registered,
          fetchDepth: 0,
          notified: false),
      remoteArray: remoteArray,
    );

Map<String, dynamic> body(String id, {String title = 't', int extra = 0}) =>
    {'id': id, 'title': title, 'serverOnly': extra};

void main() {
  test('waits for the prime and returns on an empty plan', () async {
    final out = await runPure<void>(
      (ctx) => fetchRows(ctx, env()),
      state: primed([], [r.patchSync(fetchAttempts: 3)]),
      handlers: defaults(),
    );
    expect(out.ofKind('remote.query'), isEmpty);
    expect(out.state.sync.fetchAttempts, 0);
  });

  test('fetches the deduped set once, writes and ingests bodies, balances depth',
      () async {
    final out = await runPure<void>(
      (ctx) => fetchRows(ctx, env()),
      state: primed([
        liveQuery('a', [('thing:1', 1), ('thing:2', 1)]),
        liveQuery('b', [('thing:2', 3)]),
      ]),
      handlers: defaults(over: {
        'remote.query': (_, __) => [
              StatementResult.ok([body('thing:1'), body('thing:2')])
            ],
      }),
    );
    expect(out.ofKind('remote.query'), hasLength(1),
        reason: 'a row shared by two queries is fetched once');
    final tx = out.ofKind('local.tx').single as LocalTx;
    expect(tx.ops, hasLength(2));
    // The highest requested version wins and lands on the body.
    final second = tx.ops[1] as PutOp;
    expect(second.data['_00_rv'], 3);
    expect(second.data.containsKey('serverOnly'), isFalse,
        reason: 'the schema strips fields the client does not declare');
    expect(out.state.versions, {'thing:1': 1, 'thing:2': 3});
    expect(out.ofKind('ssp.ingest'), hasLength(1));
    for (final e in out.state.queries.values) {
      expect(e.lifecycle.fetchDepth, 0);
    }
    expect(out.dispatched.whereType<SyncOutcome>().single.ok, isTrue);
  });

  test('a body the server never returns is remembered at the asked version',
      () async {
    final out = await runPure<void>(
      (ctx) => fetchRows(ctx, env()),
      state: primed([
        liveQuery('a', [('thing:1', 1), ('thing:gone', 4)])
      ]),
      handlers: defaults(over: {
        'remote.query': (_, __) => [
              StatementResult.ok([body('thing:1')])
            ],
      }),
    );
    // Without this the plan would ask for `thing:gone` forever.
    expect(out.state.versions['thing:gone'], 4);
    expect(out.ofKind('remote.query'), hasLength(1));
  });

  test('failures back off and stop the loop', () async {
    Future<RunPureResult<void>> run(EffectHandler answer) => runPure(
          (ctx) => fetchRows(ctx, env()),
          state: primed([
            liveQuery('a', [('thing:1', 1)])
          ]),
          handlers: defaults(over: {'remote.query': answer}),
        );

    for (final answer in <EffectHandler>[
      (_, __) => throw StateError('socket closed'),
      (_, __) => [const StatementResult.err('permission denied')],
      (_, __) => [const StatementResult.ok('not a list')],
      (_, __) => <StatementResult>[],
    ]) {
      final out = await run(answer);
      expect(out.state.sync.fetchAttempts, 1);
      expect(out.timers['fetch']!.ms, 500);
      expect(out.dispatched.whereType<SyncOutcome>().single.ok, isFalse);
      expect(out.state.queries['a']!.lifecycle.fetchDepth, 0);
    }
  });

  test('a failing local write is a failure; a failing ingest is only logged',
      () async {
    final writeFailed = await runPure<void>(
      (ctx) => fetchRows(ctx, env()),
      state: primed([
        liveQuery('a', [('thing:1', 1)])
      ]),
      handlers: defaults(over: {
        'remote.query': (_, __) => [
              StatementResult.ok([body('thing:1')])
            ],
        'local.tx': (_, __) => throw StateError('disk full'),
      }),
    );
    expect(writeFailed.timers['fetch'], isNotNull);
    expect(writeFailed.state.versions, isEmpty);

    final ingestFailed = await runPure<void>(
      (ctx) => fetchRows(ctx, env()),
      state: primed([
        liveQuery('a', [('thing:1', 1)])
      ]),
      handlers: defaults(over: {
        'remote.query': (_, __) => [
              StatementResult.ok([body('thing:1')])
            ],
        'ssp.ingest': (_, __) => throw StateError('circuit busy'),
      }),
    );
    expect(ingestFailed.timers['fetch'], isNull);
    expect(ingestFailed.state.versions['thing:1'], 1);
    expect(ingestFailed.emitted.whereType<LogEvent>().single.level,
        LogLevel.warn);
  });

  test('loops until the plan is empty', () async {
    // The first pass lands thing:1, which leaves thing:2 (added meanwhile).
    var round = 0;
    final out = await runPure<void>(
      (ctx) => fetchRows(ctx, env()),
      state: primed([
        liveQuery('a', [('thing:1', 1), ('thing:2', 1)])
      ]),
      handlers: defaults(over: {
        'remote.query': (_, __) {
          round++;
          return [
            StatementResult.ok([body('thing:1'), body('thing:2')])
          ];
        },
      }),
    );
    expect(round, 1, reason: 'one chunk covers both ids');
    expect(out.state.versions.length, 2);
  });
}
