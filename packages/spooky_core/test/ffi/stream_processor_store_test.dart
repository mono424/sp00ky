import 'package:spooky_core/src/ffi/stream_processor.dart';
import 'package:spooky_core/src/ffi/stream_update.dart';
import 'package:test/test.dart';

/// Round-trips the `ssp-ffi` surface the circuit prime needs, against the real
/// native library.
void main() {
  group('StreamProcessor store state', () {
    late StreamProcessor sp;

    Map<String, dynamic> view(String id) => {
          'id': id,
          'surql': 'SELECT * FROM thread',
          'params': <String, dynamic>{},
          'clientId': 'local',
          'ttl': '10m',
          'lastActiveAt': '2026-01-01T00:00:00.000Z',
        };

    Map<String, dynamic> row(String id, {int rv = 1, String title = 't'}) => {
          'table': 'thread',
          'op': 'CREATE',
          'id': 'thread:$id',
          'record': {'id': 'thread:$id', 'title': title, '_00_rv': rv},
        };

    setUp(() {
      sp = StreamProcessor.create();
      sp.setPermission('thread', 'true');
    });
    tearDown(() => sp.dispose());

    test('ingestMany applies a whole batch in one step, in order', () {
      sp.registerView(view('q1'));
      final updates = sp.ingestMany([
        row('a'),
        row('b'),
        {
          ...row('a'),
          'op': 'UPDATE',
          'record': {'id': 'thread:a', 'title': 'last', '_00_rv': 2}
        },
      ]);
      expect(updates, hasLength(1), reason: 'one coalesced update per view');
      final u = updates.single;
      expect(u.queryHash, 'q1');
      expect(u.localArray.map((e) => e.$1).toSet(), {'thread:a', 'thread:b'});
      expect(sp.ingestMany(const []), isEmpty);
    });

    test('a saved store restores under views registered after it', () {
      sp.registerView(view('q1'));
      sp.ingestMany([row('a'), row('b')]);
      final snapshot = sp.saveStoreState();
      expect(snapshot, isNotEmpty);

      // A fresh circuit registers its view against an empty store, then the
      // snapshot lands underneath it and the view catches up.
      final restored = StreamProcessor.create()
        ..setPermission('thread', 'true');
      addTearDown(restored.dispose);
      final reg = restored.registerView(view('q1'))!;
      expect(reg.update.localArray, isEmpty);

      final updates = restored.loadStoreState(snapshot);
      final caught = updates.firstWhere((u) => u.queryHash == 'q1');
      expect(
          caught.localArray.map((e) => e.$1).toSet(), {'thread:a', 'thread:b'});
      expect(restored.loadStoreState(snapshot.sublist(0, 0)), isEmpty);
    });

    test('loading a corrupt snapshot reports an error rather than crashing',
        () {
      expect(() => sp.loadStoreState(sp.saveStoreState()..[0] = 0x00),
          throwsA(isA<SspException>()));
    });

    test(
        'reconcile deletes what the caller no longer has and asks for the rest',
        () {
      sp.registerView(view('q1'));
      sp.ingestMany([row('a'), row('b', rv: 1)]);

      final result = sp.reconcile('thread', [
        ('thread:b', 2), // the caller holds a newer body
        ('thread:c', 1), // the circuit has never seen it
      ]);
      expect(result.fetch.toSet(), {'thread:b', 'thread:c'});
      expect(result.deleted, 1, reason: 'thread:a is not in the list any more');
      final after = result.updates.firstWhere((u) => u.queryHash == 'q1');
      expect(after.localArray.map((e) => e.$1), isNot(contains('thread:a')));
    });

    test('maxRowVersions reports the highest rv folded into each table', () {
      sp.registerView(view('q1'));
      sp.ingestMany([row('a', rv: 3), row('b', rv: 7)]);
      expect(sp.maxRowVersions()['thread'], 7);
    });

    test('projection can be turned on before the first ingest', () {
      sp.setProjection(true);
      sp.registerView(view('q1'));
      final updates = sp.ingestMany([row('a')]);
      expect(updates.single.localArray.map((e) => e.$1), ['thread:a']);
      sp.setProjection(false);
    });

    test('registration timings come back on the register path', () {
      final reg = sp.registerView(view('q1'))!;
      expect(reg.update.parseMs, isNotNull);
      // The ingest side is not measured by a registration.
      expect(reg.update.storeApplyMs, isNull);
    });
  });
}
