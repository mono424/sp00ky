import 'package:spooky_core/src/ffi/stream_update.dart';
import 'package:spooky_core/src/services/logger/logger.dart';
import 'package:spooky_core/src/services/stream_processor/stream_processor_service.dart';
import 'package:test/test.dart';

/// Tests for the batch-coalescing window added to [StreamProcessorService].
///
/// A batched ingest (e.g. sync fetching N missing records) used to emit one
/// stream update per record, making the UI render row-by-row. beginBatch/
/// endBatch collapse those into a single coalesced update per affected query.
void main() {
  final logger = SpookyLogger.root('test');

  group('StreamProcessor batch coalescing', () {
    late StreamProcessorService sp;
    late List<StreamUpdate> received;

    setUp(() async {
      sp = StreamProcessorService(logger);
      await sp.init();
      sp.seedPermissionsFromSchema(
          'DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE true;');
      sp.registerQueryPlan(QueryPlanConfig(
        queryHash: 'q1',
        surql: 'SELECT * FROM user',
        params: {},
        ttl: '10m',
        lastActiveAt: DateTime.utc(2026),
      ));

      received = [];
      sp.addReceiver(_CapturingReceiver(received.add));
    });
    tearDown(() => sp.close());

    void ingestUser(int n) => sp
        .ingest('user', 'CREATE', 'user:$n', {'id': 'user:$n', 'name': 'u$n'});

    test('emits one update per record when NOT batching (baseline / the bug)',
        () {
      // This is the row-by-row behavior the user is seeing: three records, three
      // notifications, so the UI re-renders three times.
      ingestUser(1);
      ingestUser(2);
      ingestUser(3);

      expect(received, hasLength(3));
    });

    test('coalesces a batch into a single update with the final array', () {
      sp.beginBatch();
      try {
        ingestUser(1);
        ingestUser(2);
        ingestUser(3);
        // Nothing dispatched until endBatch.
        expect(received, isEmpty);
      } finally {
        sp.endBatch();
      }

      // Exactly one coalesced update, carrying the full materialized array.
      expect(received, hasLength(1));
      final update = received.single;
      expect(update.queryHash, 'q1');
      expect(update.localArray.map((e) => e.$1),
          containsAll(['user:1', 'user:2', 'user:3']));
      expect(update.localArray, hasLength(3));
      // Coalesced updates take DataModule's immediate (non-debounced) path.
      expect(update.op, 'CREATE');
      expect(update.materializationTimeMs, isNotNull);
    });

    test('batch-ingest of new records matching a FILTERED query emits them',
        () {
      // The app's initial down-sync path: register a record-filtered query, then
      // bulk-fetch the matching rows and ingest them as ONE batch (the live
      // per-record path doesn't fire for rows that pre-existed before the LIVE
      // subscription). The batch must materialize the matching rows into the
      // view — this is the path the live-driven e2e never exercises with NEW rows.
      sp.seedPermissionsFromSchema(
          'DEFINE TABLE game PERMISSIONS FOR select WHERE true;');
      sp.registerQueryPlan(QueryPlanConfig(
        queryHash: 'qg',
        surql:
            r'SELECT * FROM game WHERE database = $db ORDER BY sort_index ASC',
        // Param as the sanitized string form the app's local circuit receives
        // (a RecordId is stringified for the FFI's jsonEncode).
        params: {'db': 'game_database:DBS_x'},
        ttl: '10m',
        lastActiveAt: DateTime.utc(2026),
      ));
      received.clear();

      sp.beginBatch();
      try {
        for (var i = 0; i < 4; i++) {
          sp.ingest('game', 'CREATE', 'game:g$i', {
            'id': 'game:g$i',
            'database': 'game_database:DBS_x',
            'white': 'W',
            'black': 'B',
            'pgn': '1. e4 e5',
            'result': '1-0',
            'status': 'Finished',
            'sort_index': -i,
          });
        }
      } finally {
        sp.endBatch();
      }

      final gameUpdates = received.where((u) => u.queryHash == 'qg').toList();
      expect(gameUpdates, isNotEmpty,
          reason: 'filtered query must emit for a batch of matching rows');
      expect(gameUpdates.last.localArray.map((e) => e.$1),
          containsAll(['game:g0', 'game:g1', 'game:g2', 'game:g3']));
      expect(gameUpdates.last.localArray, hasLength(4));
    });

    test('endBatch is safe with no buffered updates; beginBatch is idempotent',
        () {
      sp.beginBatch();
      sp.beginBatch(); // no-op, must not reset/duplicate
      sp.endBatch();
      expect(received, isEmpty);

      // A subsequent non-batched ingest dispatches normally (window closed).
      ingestUser(1);
      expect(received, hasLength(1));
    });
  });
}

class _CapturingReceiver implements StreamUpdateReceiver {
  _CapturingReceiver(this._onUpdate);
  final void Function(StreamUpdate) _onUpdate;
  @override
  void onStreamUpdate(StreamUpdate update) => _onUpdate(update);
}
