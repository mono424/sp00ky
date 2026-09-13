import 'package:spooky_core/src/ffi/stream_update.dart';
import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/query/materialize_saga.dart';
import 'package:spooky_core/src/state/lifecycle.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:spooky_core/src/types.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

QueryLifecycle life(QueryPhase phase) => QueryLifecycle(
    phase: phase, remote: RemotePhase.unregistered, fetchDepth: 0, notified: false);

void main() {
  test('no entry: nothing happens', () async {
    final out = await runPure<void>(
      (ctx) => materialize(ctx, 'nope'),
      handlers: defaults(),
    );
    expect(out.log, hasLength(1));
  });

  test('live renders membership, dirt clears and subscribers hear once',
      () async {
    final rows = [
      {'id': 'thing:1', 'title': 'a'}
    ];
    final out = await runPure<void>(
      (ctx) => materialize(ctx, 'a'),
      state: r.markDirty(['a'])(buildState([
        buildEntry(
          def: buildDefinition(hash: 'a'),
          lifecycle: life(QueryPhase.live),
          remoteArray: [('thing:1', 1)],
        )
      ])),
      handlers: defaults(over: {
        'local.getMany': (_, __) => rows,
      }),
    );
    final read = out.ofKind('local.getMany').single as LocalGetMany;
    expect(read.table, 'thing');
    expect(read.ids, ['thing:1']);
    expect(out.state.queries['a']!.records, rows);
    expect(out.state.dirty, isEmpty);
    expect(out.state.queries['a']!.lifecycle.notified, isTrue);
    expect(out.emitted.whereType<QueryRecordsEvent>(), hasLength(1));
    expect(out.state.queries['a']!.telemetry.updateCount, 1);
  });

  test('a cold query renders the SSP local window', () async {
    // Divergence from the browser client, which re-runs the query's own
    // predicate against the local store: sqlite cannot run SurrealQL.
    final out = await runPure<void>(
      (ctx) => materialize(ctx, 'a'),
      state: buildState([
        buildEntry(
          def: buildDefinition(hash: 'a'),
          lifecycle: life(QueryPhase.cold),
          remoteArray: [('thing:9', 1)],
          localArray: [('thing:1', 1)],
        )
      ]),
      handlers: defaults(over: {
        'local.getMany': (_, __) => <Map<String, dynamic>>[],
      }),
    );
    expect((out.ofKind('local.getMany').single as LocalGetMany).ids,
        ['thing:1']);
  });

  test('the outbox overlay adds local writes and hides local deletes',
      () async {
    final out = await runPure<void>(
      (ctx) => materialize(ctx, 'a'),
      state: buildState([
        buildEntry(
          def: buildDefinition(hash: 'a'),
          lifecycle: life(QueryPhase.live),
          remoteArray: [('thing:1', 1), ('thing:3', 1)],
          localArray: [('thing:2', 1)],
        )
      ], [
        r.outboxReplace([
          buildOutboxItem(id: 'm1', recordId: 'thing:2'),
          buildOutboxItem(
              id: 'm2',
              recordId: 'thing:3',
              type: MutationEventType.delete),
        ]),
      ]),
      handlers: defaults(over: {
        'local.getMany': (_, __) => <Map<String, dynamic>>[],
      }),
    );
    expect((out.ofKind('local.getMany').single as LocalGetMany).ids,
        ['thing:1', 'thing:2']);
  });

  test('an unchanged read notifies nobody but still clears the dirt', () async {
    final rows = [
      {'id': 'thing:1'}
    ];
    final first = await runPure<void>(
      (ctx) => materialize(ctx, 'a'),
      state: r.markDirty(['a'])(buildState([
        buildEntry(
            def: buildDefinition(hash: 'a'),
            lifecycle: life(QueryPhase.live),
            remoteArray: [('thing:1', 1)])
      ])),
      handlers: defaults(over: {'local.getMany': (_, __) => rows}),
    );
    final again = await runPure<void>(
      (ctx) => materialize(ctx, 'a'),
      state: r.markDirty(['a'])(first.state),
      handlers: defaults(over: {
        'local.getMany': (_, __) => [
              {'id': 'thing:1'}
            ]
      }),
    );
    expect(again.emitted.whereType<QueryRecordsEvent>(), isEmpty);
    expect(again.state.dirty, isEmpty);
    expect(again.state.queries['a']!.telemetry.updateCount, 1);
  });

  test('a failing read counts an error and clears the dirt', () async {
    final out = await runPure<void>(
      (ctx) => materialize(ctx, 'a'),
      state: r.markDirty(['a'])(buildState([
        buildEntry(
            def: buildDefinition(hash: 'a'),
            lifecycle: life(QueryPhase.live),
            remoteArray: [('thing:1', 1)])
      ])),
      handlers: defaults(over: {
        'local.getMany': (_, __) => throw StateError('store closed'),
      }),
    );
    expect(out.state.queries['a']!.telemetry.errorCount, 1);
    expect(out.state.dirty, isEmpty);
    expect(out.emitted.whereType<LogEvent>().single.level, LogLevel.warn);
  });

  test('a windowed query re-applies its own ORDER BY to the id-set read',
      () async {
    final out = await runPure<void>(
      (ctx) => materialize(ctx, 'a'),
      state: buildState([
        buildEntry(
          def: buildDefinition(
              hash: 'a',
              surql: 'SELECT * FROM thing ORDER BY n ASC LIMIT 10 START 10'),
          lifecycle: life(QueryPhase.live),
          remoteArray: [('thing:1', 1), ('thing:2', 1)],
        )
      ]),
      handlers: defaults(over: {
        'local.getMany': (_, __) => [
              {'id': 'thing:1', 'n': 2},
              {'id': 'thing:2', 'n': 1},
            ],
      }),
    );
    expect(out.state.queries['a']!.records.map((r) => r['n']), [1, 2]);
    // A window keeps the server's id order rather than sorting by id.
    expect((out.ofKind('local.getMany').single as LocalGetMany).ids,
        ['thing:1', 'thing:2']);
  });

  group('streamUpdate', () {
    test('takes the local id-set, dirties the query and records timings',
        () async {
      final out = await runPure<void>(
        (ctx) => streamUpdate(
            ctx,
            const StreamUpdate(
              queryHash: 'a',
              localArray: [('thing:1', 1)],
              materializationTimeMs: 4,
              storeApplyMs: 1,
              circuitStepMs: 2,
              transformMs: 3,
            )),
        state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
        handlers: defaults(),
      );
      expect(out.state.queries['a']!.localArray, [('thing:1', 1)]);
      expect(out.state.dirty, contains('a'));
      final t = out.state.queries['a']!.telemetry;
      expect(t.lastIngestLatencyMs, 4);
      expect(t.phaseLast[TimingPhase.sspStoreApply], 1);
      expect(t.phaseLast[TimingPhase.sspCircuitStep], 2);
      expect(t.phaseLast[TimingPhase.sspTransform], 3);
    });

    test('an unknown hash is ignored', () async {
      final out = await runPure<void>(
        (ctx) => streamUpdate(ctx,
            const StreamUpdate(queryHash: 'nope', localArray: [])),
        handlers: defaults(),
      );
      expect(out.state.dirty, isEmpty);
    });
  });
}
