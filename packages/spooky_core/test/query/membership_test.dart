import 'package:spooky_core/src/query/membership.dart';
import 'package:spooky_core/src/types.dart';
import 'package:spooky_core/src/state/lifecycle.dart' show QueryPhase;
import 'package:test/test.dart';

void main() {
  group('durable view row', () {
    test('parses and gates resolved-before', () {
      expect(parseViewRow(null), isNull);
      expect(parseViewRow({'ids': 'nope'}), isNull);
      final v = parseViewRow({
        'ids': [
          ['t:1', 1]
        ],
        'confirmed': true
      })!;
      expect(v.ids, [('t:1', 1)]);
      expect(v.confirmed, isTrue);
      final empty = parseViewRow({'ids': <dynamic>[]})!;
      expect(empty.ids, isEmpty);
      expect(empty.confirmed, isFalse);
      expect(isResolvedBefore(null), isFalse);
      expect(isResolvedBefore(const DurableView(ids: [], confirmed: false)),
          isFalse);
      expect(isResolvedBefore(const DurableView(ids: [], confirmed: true)),
          isTrue);
      expect(
          isResolvedBefore(
              const DurableView(ids: [('t:1', 1)], confirmed: false)),
          isTrue);
    });
  });

  group('metaFromRow / dedupe', () {
    test('folds the row-count select', () {
      expect(metaFromRow(null).present, isFalse);
      expect(metaFromRow(null).rowCount, isNull);
      final ready = metaFromRow({'rowCount': 3, 'state': 'ready'});
      expect(ready.present, isTrue);
      expect(ready.rowCount, 3);
      expect(ready.state, 'ready');
      final weird = metaFromRow({'rowCount': null, 'state': 'weird'});
      expect(weird.present, isTrue);
      expect(weird.rowCount, isNull);
      expect(weird.state, isNull);
      final mat = metaFromRow({'state': 'materializing'});
      expect(mat.state, 'materializing');
      expect(mat.rowCount, isNull);
    });

    test('keeps the highest version per id', () {
      const single = [('t:1', 1)];
      expect(identical(dedupeRecordVersions(single), single), isTrue);
      const clean = [('t:1', 1), ('t:2', 1)];
      expect(identical(dedupeRecordVersions(clean), clean), isTrue);
      expect(dedupeRecordVersions([('t:1', 1), ('t:1', 3), ('t:1', 2)]),
          [('t:1', 3)]);
    });
  });

  group('decideMembershipOutcome', () {
    test('non-empty or verified removal is always applied', () {
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.cold, held: 0, remoteArray: [('t:1', 1)])),
        MembershipOutcome.applied,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.live,
            held: 2,
            remoteArray: [],
            verifiedRemoval: true)),
        MembershipOutcome.applied,
      );
    });

    test('empty with no readable row: view-lost when holding, ignored when cold',
        () {
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.cold, held: 0, remoteArray: [])),
        MembershipOutcome.ignored,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.cold,
            held: 0,
            remoteArray: [],
            meta: ServerViewMeta(present: false))),
        MembershipOutcome.ignored,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.cold, held: 2, remoteArray: [])),
        MembershipOutcome.viewLost,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.cached, held: 0, remoteArray: [])),
        MembershipOutcome.viewLost,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.live,
            held: 1,
            remoteArray: [],
            meta: ServerViewMeta(present: false))),
        MembershipOutcome.viewLost,
      );
    });

    test('empty with a present row: applied only when ready and zero rows', () {
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.cold,
            held: 0,
            remoteArray: [],
            meta: ServerViewMeta(present: true, rowCount: 0, state: 'ready'))),
        MembershipOutcome.applied,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.cold,
            held: 0,
            remoteArray: [],
            meta: ServerViewMeta(present: true, rowCount: 0))),
        MembershipOutcome.applied,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.cold,
            held: 0,
            remoteArray: [],
            meta: ServerViewMeta(
                present: true, rowCount: 0, state: 'materializing'))),
        MembershipOutcome.ignored,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.live,
            held: 3,
            remoteArray: [],
            meta: ServerViewMeta(present: true, rowCount: 3, state: 'ready'))),
        MembershipOutcome.ignored,
      );
      expect(
        decideMembershipOutcome(const MembershipDecisionInput(
            phase: QueryPhase.live,
            held: 3,
            remoteArray: [],
            meta: ServerViewMeta(present: true, state: 'ready'))),
        MembershipOutcome.ignored,
      );
    });
  });

  group('snapshots', () {
    Map<String, dynamic> edge(String out, int version, {Object? parent}) =>
        {'out': out, 'version': version, if (parent != null) 'parent': parent};

    test('single: edges, meta, children; null when the primary is not a list',
        () {
      expect(snapshotFromSingle(null, null, null), isNull);
      final snap = snapshotFromSingle(
        [edge('thing:1', 1), edge('thing:1', 2)],
        {'rowCount': 1, 'state': 'ready'},
        [edge('thing:c', 1)],
      )!;
      expect(snap.primary, [('thing:1', 2)]);
      expect(snap.subquery, [('thing:c', 1)]);
      expect(snap.meta.present, isTrue);
      expect(snap.meta.rowCount, 1);
      expect(snapshotFromSingle(const [], null, null)!.subquery, isEmpty);
    });

    test('batch: splits by hash and parent, fills meta, marks missing absent',
        () {
      final hashById = {'_00_query:a': 'ha', '_00_query:b': 'hb'};
      expect(snapshotsFromBatch(null, null, hashById), isEmpty);
      final snaps = snapshotsFromBatch(
        [
          {'in': '_00_query:a', 'out': 'thing:1', 'version': 1},
          {'in': '_00_query:a', 'out': 'thing:1', 'version': 1},
          {
            'in': '_00_query:a',
            'out': 'thing:c',
            'version': 1,
            'parent': '_00_query:a'
          },
          {'in': '_00_query:zz', 'out': 'thing:9', 'version': 1},
        ],
        [
          {'id': '_00_query:a', 'rowCount': 1, 'state': 'ready'},
          null,
          {'rowCount': 5},
          {'id': '_00_query:zz'},
        ],
        hashById,
      );
      expect(snaps['ha']!.primary, [('thing:1', 1)]);
      expect(snaps['ha']!.subquery, [('thing:c', 1)]);
      expect(snaps['ha']!.meta.rowCount, 1);
      expect(snaps['hb']!.primary, isEmpty);
      expect(snaps['hb']!.meta.present, isFalse);
      expect(snapshotsFromBatch(const [], null, hashById)['ha']!.meta.present,
          isFalse);
    });

    test('suspectHashes flags held queries with a missing or silently empty row',
        () {
      final snaps = {
        'missing': ListRefSnapshot(
            primary: [], subquery: [], meta: ServerViewMeta.absent),
        'empty': ListRefSnapshot(
            primary: [],
            subquery: [],
            meta: const ServerViewMeta(
                present: true, rowCount: 4, state: 'ready')),
        'fine': ListRefSnapshot(
            primary: [],
            subquery: [],
            meta: const ServerViewMeta(
                present: true, rowCount: 0, state: 'ready')),
        'unheld': ListRefSnapshot(
            primary: [], subquery: [], meta: ServerViewMeta.absent),
      };
      final held = {'missing': 2, 'empty': 4, 'fine': 1, 'unheld': 0};
      expect(suspectHashes(snaps, held, 0)..sort(), ['empty', 'missing']);
      expect(suspectHashes(snaps, held, 3), ['missing']);
    });
  });
}
