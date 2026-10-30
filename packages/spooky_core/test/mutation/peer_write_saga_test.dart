import 'package:spooky_core/src/kernel/effects.dart';
import 'package:spooky_core/src/kernel/events.dart';
import 'package:spooky_core/src/mutation/write_saga.dart';
import 'package:spooky_core/src/query/render_set.dart';
import 'package:spooky_core/src/services/stream_processor/stream_processor_service.dart'
    show IngestOp;
import 'package:spooky_core/src/state/client_state.dart';
import 'package:spooky_core/src/state/reducers.dart' as r;
import 'package:spooky_core/src/state/selectors.dart';
import 'package:spooky_core/src/surreal/value.dart';
import 'package:spooky_core/src/testing/build.dart';
import 'package:spooky_core/src/testing/run_pure.dart';
import 'package:test/test.dart';

import '../saga_helpers.dart';

/// A local store as a map, so a saga's reads see its own writes.
Map<String, EffectHandler> store(Map<String, Map<String, dynamic>> rows) =>
    defaults(over: {
      'local.get': (e, _) => rows[(e as LocalGet).id],
      'local.tx': (e, _) {
        for (final op in (e as LocalTx).ops) {
          switch (op) {
            case PutOp(:final id, :final data):
              rows[id] = {...data};
            case DeleteOp(:final id):
              rows.remove(id);
            default:
              throw StateError('unexpected $op');
          }
        }
        return null;
      },
    });

void main() {
  test(
      'peerCreate: row without _00_rv and without an outbox entry, circuit, overlay, timer',
      () async {
    final rows = <String, Map<String, dynamic>>{};
    final out = await runPure<bool>(
      (ctx) => peerCreate(
          ctx,
          env(),
          const PeerWriteInput(
              recordId: 'thing:1',
              data: {'title': 'hi', 'owner': 'user:u1'},
              ttlMs: 5000)),
      state: buildState([buildEntry(def: buildDefinition(hash: 'a'))]),
      handlers: store(rows),
    );
    expect(out.result, isTrue);
    final tx = out.ofKind('local.tx').single as LocalTx;
    expect(tx.ops, hasLength(1), reason: 'the peer pushes it, not us');
    final put = tx.ops.single as PutOp;
    expect(put.data.containsKey('_00_rv'), isFalse,
        reason: "the server's body must still download");
    expect(put.data['owner'], isA<RecordId>());
    final ingest = out.ofKind('ssp.ingest').single as SspIngest;
    expect(ingest.records.single.op, IngestOp.create);
    expect(ingest.records.single.record['_00_rv'], 0);
    expect(out.state.versions.containsKey('thing:1'), isFalse);
    expect(out.state.outbox, isEmpty);
    expect(out.state.peerWrites['thing:1']?.kind, PeerWriteKind.create);
    expect(out.state.peerWrites['thing:1']?.expiresAt, 1700000000000 + 5000);
    expect(overlay(out.state).writes, contains('thing:1'));
    expect(out.state.dirty, contains('a'));
    expect(out.timers['peer:thing:1']?.ms, 5000);
    expect(out.timers['peer:thing:1']?.event, isA<PeerWriteExpired>());
  });

  test(
      'peerCreate refuses a confirmed row, an own pending write, a pending peer write and an unknown table',
      () async {
    Future<bool> attempt(ClientState state, {String id = 'thing:1'}) async =>
        (await runPure<bool>(
          (ctx) => peerCreate(ctx, env(),
              PeerWriteInput(recordId: id, data: const {}, ttlMs: 5000)),
          state: state,
          handlers: store({}),
        ))
            .result;

    final base = buildState([]);
    expect(await attempt(r.setVersions([('thing:1', 3)])(base)), isFalse);
    expect(
        await attempt(
            r.outboxPush(buildOutboxItem(id: 'm1', recordId: 'thing:1'))(base)),
        isFalse);
    expect(
        await attempt(r.setPeerWrite(
            'thing:1',
            const PeerWrite(
                kind: PeerWriteKind.delete,
                table: 'thing',
                expiresAt: 0))(base)),
        isFalse);
    expect(await attempt(base, id: 'ghost:1'), isFalse);
  });

  test('peerDelete hides a held row through the overlay and keeps it stored',
      () async {
    final rows = {
      'thing:1': {'id': 'thing:1', 'title': 'hi'}
    };
    final out = await runPure<bool>(
      (ctx) => peerDelete(
          ctx, env(), const PeerWriteInput(recordId: 'thing:1', ttlMs: 5000)),
      handlers: store(rows),
    );
    expect(out.result, isTrue);
    expect(out.ofKind('local.tx'), isEmpty);
    expect(rows, contains('thing:1'));
    expect(overlay(out.state).deletes, contains('thing:1'));
    final ids = buildRenderIds(
        [('thing:1', 1), ('thing:2', 1)],
        const [],
        overlay(out.state),
        const RenderOptions(hasExplicitOrder: false, isWindow: false));
    expect(ids, ['thing:2'], reason: 'the hidden row leaves every query');
    expect(out.timers, contains('peer:thing:1'));

    final missing = await runPure<bool>(
      (ctx) => peerDelete(
          ctx, env(), const PeerWriteInput(recordId: 'thing:9', ttlMs: 5000)),
      handlers: store({}),
    );
    expect(missing.result, isFalse);
  });

  test('peerDelete of an unconfirmed peer create just undoes it', () async {
    final rows = <String, Map<String, dynamic>>{};
    final created = await runPure<bool>(
      (ctx) => peerCreate(
          ctx,
          env(),
          const PeerWriteInput(
              recordId: 'thing:1', data: {'title': 'x'}, ttlMs: 5000)),
      handlers: store(rows),
    );
    final out = await runPure<bool>(
      (ctx) => peerDelete(
          ctx, env(), const PeerWriteInput(recordId: 'thing:1', ttlMs: 5000)),
      state: created.state,
      handlers: store(rows),
    );
    expect(out.result, isTrue);
    expect(rows, isEmpty);
    expect(out.state.peerWrites, isEmpty);
    expect((out.ofKind('ssp.ingest').single as SspIngest).records.single.op,
        IngestOp.delete);
    expect(out.ofKind('timer.clear'), hasLength(1));
  });

  test(
      'expiry undoes an unconfirmed create, keeps a confirmed one, forgets a delete',
      () async {
    final rows = <String, Map<String, dynamic>>{};
    final created = await runPure<bool>(
      (ctx) => peerCreate(
          ctx,
          env(),
          const PeerWriteInput(
              recordId: 'thing:1', data: {'title': 'x'}, ttlMs: 5000)),
      handlers: store(rows),
    );

    final undone = await runPure<void>(
        (ctx) => peerWriteExpired(ctx, 'thing:1'),
        state: created.state,
        handlers: store(rows));
    expect(rows, isEmpty);
    expect(undone.state.peerWrites, isEmpty);

    rows['thing:1'] = {'id': 'thing:1', 'title': 'x'};
    final confirmed = await runPure<void>(
        (ctx) => peerWriteExpired(ctx, 'thing:1'),
        state: r.setVersions([('thing:1', 1)])(created.state),
        handlers: store(rows));
    expect(rows, contains('thing:1'), reason: "the server's body landed");
    expect(confirmed.state.peerWrites, isEmpty);
    expect(confirmed.ofKind('local.tx'), isEmpty);

    final hidden = r.setPeerWrite(
        'thing:2',
        const PeerWrite(
            kind: PeerWriteKind.delete,
            table: 'thing',
            expiresAt: 0))(buildState([]));
    final forgot = await runPure<void>(
        (ctx) => peerWriteExpired(ctx, 'thing:2'),
        state: hidden,
        handlers: store(rows));
    expect(forgot.state.peerWrites, isEmpty);
    expect(forgot.ofKind('local.tx'), isEmpty);

    final none = await runPure<void>((ctx) => peerWriteExpired(ctx, 'thing:3'),
        handlers: store(rows));
    expect(none.log.where((e) => e.kind != 'state.read'), isEmpty);
  });

  test('a bucket switch forgets peer writes', () {
    final s = r.setPeerWrite(
        'thing:1',
        const PeerWrite(
            kind: PeerWriteKind.create,
            table: 'thing',
            expiresAt: 0))(buildState([]));
    expect(r.clearBucketState()(s).peerWrites, isEmpty);
  });

  test('readRow reads the local store', () async {
    final out = await runPure<Map<String, dynamic>?>(
      (ctx) => readRow(ctx, 'thing:1'),
      handlers: store({
        'thing:1': {'id': 'thing:1'}
      }),
    );
    expect(out.result, {'id': 'thing:1'});
  });
}
